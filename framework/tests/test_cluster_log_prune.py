"""Offline unit tests for pipeline_lib.cluster_log_prune: the allow-listed delete plan the `_cluster logs prune`
job executes. A fake directory tree stands in for dbutils.fs.ls."""
from types import SimpleNamespace

import pytest

from pipeline_lib.cluster_log_prune import CLUSTER_ID, LOG_KINDS, cutoff_ms, plan_prune, validate_log_root
from pipeline_lib.config import PipelineConfigError

ROOT = "/Volumes/cat/sch/vol/cluster_logs"
DAY = 86_400_000
NOW = 1_800_000_000_000
CUTOFF = NOW - 30 * DAY
OLD = CUTOFF - 1        # one millisecond older than the cutoff: deleted
NEW = CUTOFF            # exactly at the cutoff: kept (strictly older is deleted)
CID = "1023-200712-abcd1234"
CID2 = "1023-200713-efgh5678"


def _fs(tree):
    """A dbutils.fs.ls stand-in over {path: [entries]}. A dict value is a directory, an int is a file mtime,
    a (mtime, size) tuple a file with a size, and an Exception value makes listing that directory raise."""
    def ls(path):
        node = tree
        for part in path[len(ROOT):].strip("/").split("/") if path != ROOT else []:
            node = node[part]
        if isinstance(node, Exception):
            raise node
        out = []
        for name, child in node.items():
            if isinstance(child, (dict, Exception)):
                out.append(SimpleNamespace(name=name + "/", size=0, modificationTime=0))
            else:
                mtime, size = child if isinstance(child, tuple) else (child, 10)
                out.append(SimpleNamespace(name=name, size=size, modificationTime=mtime))
        return out
    return ls


def _paths(items):
    return sorted(p for p, *_ in items)


def test_deletes_old_files_and_keeps_new_ones_inside_the_layout():
    tree = {CID: {
        "driver": {"log4j-2026-01-01-00.log.gz": OLD, "log4j-active.log": NEW, "stdout": NEW},
        "executor": {"app-1": {"0": {"stderr": OLD}}},
        "eventlog": {"x": {"y": {"eventlog": OLD}}},
        "init_scripts": {"z": {"a.sh.stdout.log": OLD}},
    }}
    plan = plan_prune(ROOT, _fs(tree), CUTOFF)
    assert _paths(plan.delete_files) == [
        f"{ROOT}/{CID}/driver/log4j-2026-01-01-00.log.gz",
        f"{ROOT}/{CID}/eventlog/x/y/eventlog",
        f"{ROOT}/{CID}/executor/app-1/0/stderr",
        f"{ROOT}/{CID}/init_scripts/z/a.sh.stdout.log",
    ]
    assert plan.kept_files == 2 and plan.kept_bytes == 20
    # driver still holds kept files, so neither it nor the cluster dir is removed; the other kinds empty out.
    assert f"{ROOT}/{CID}/driver" not in plan.delete_dirs
    assert f"{ROOT}/{CID}" not in plan.delete_dirs
    assert f"{ROOT}/{CID}/executor" in plan.delete_dirs
    assert plan.skipped == [] and plan.list_errors == []


def test_cutoff_is_strict():
    plan = plan_prune(ROOT, _fs({CID: {"driver": {"a": OLD, "b": NEW}}}), CUTOFF)
    assert _paths(plan.delete_files) == [f"{ROOT}/{CID}/driver/a"]


def test_fully_old_cluster_is_removed_deepest_first():
    tree = {CID: {"driver": {"stdout": OLD}, "executor": {"app": {"0": {"stderr": OLD}}}}}
    plan = plan_prune(ROOT, _fs(tree), CUTOFF)
    dirs = plan.delete_dirs
    assert set(dirs) == {f"{ROOT}/{CID}/driver", f"{ROOT}/{CID}/executor/app/0", f"{ROOT}/{CID}/executor/app",
                         f"{ROOT}/{CID}/executor", f"{ROOT}/{CID}"}
    # Every directory comes after everything beneath it, so a non-recursive remove can succeed in order.
    for i, d in enumerate(dirs):
        assert not any(later != d and later.startswith(d + "/") for later in dirs[i + 1:])
    assert dirs[-1] == f"{ROOT}/{CID}"


def test_entries_outside_the_layout_are_never_planned():
    # The reason the allow-list exists: a path set to a volume root that also holds checkpoints and the wheel.
    tree = {
        "checkpoints": {"my_feed": {"offsets": {"0": OLD}, "commits": {"0": OLD}}},
        "databricks_es_connector-0.10.0-py3-none-any.whl": OLD,
        "notes.txt": OLD,
        "1023-200712-ABCD1234": {"driver": {"stdout": OLD}},   # uppercase: not a cluster id
        "1023-200712": {"driver": {"stdout": OLD}},            # truncated id
        CID: {"driver": {"stdout": OLD}, "my_backup": {"x": OLD}, "readme": OLD},
    }
    plan = plan_prune(ROOT, _fs(tree), CUTOFF)
    assert _paths(plan.delete_files) == [f"{ROOT}/{CID}/driver/stdout"]
    assert _paths(plan.skipped) == sorted([
        f"{ROOT}/checkpoints", f"{ROOT}/databricks_es_connector-0.10.0-py3-none-any.whl", f"{ROOT}/notes.txt",
        f"{ROOT}/1023-200712-ABCD1234", f"{ROOT}/1023-200712", f"{ROOT}/{CID}/my_backup", f"{ROOT}/{CID}/readme",
    ])
    # Nothing outside the layout is ever a delete candidate, file or directory.
    for path, _ in plan.delete_files:
        assert path.startswith(f"{ROOT}/{CID}/driver/")
    # The cluster dir holds skipped entries, so it is kept; its emptied driver dir goes.
    assert plan.delete_dirs == [f"{ROOT}/{CID}/driver"]


def test_a_log_kind_named_file_is_skipped_not_walked():
    plan = plan_prune(ROOT, _fs({CID: {"driver": OLD}}), CUTOFF)
    assert plan.delete_files == [] and _paths(plan.skipped) == [f"{ROOT}/{CID}/driver"]


@pytest.mark.parametrize("mtime", [0, None, -5, "1700000000000", True])
def test_unknown_age_is_kept(mtime):
    plan = plan_prune(ROOT, _fs({CID: {"driver": {"stdout": (mtime, 7)}}}), CUTOFF)
    assert plan.delete_files == [] and plan.kept_files == 1 and plan.kept_bytes == 7
    assert plan.delete_dirs == []


def test_listing_errors_leave_the_subtree_alone_and_are_reported():
    tree = {
        CID: {"driver": OSError("boom"), "executor": {"0": {"stderr": OLD}}},
        CID2: OSError("denied"),
    }
    plan = plan_prune(ROOT, _fs(tree), CUTOFF)
    assert _paths(plan.list_errors) == [f"{ROOT}/{CID}/driver", f"{ROOT}/{CID2}"]
    assert _paths(plan.delete_files) == [f"{ROOT}/{CID}/executor/0/stderr"]
    assert f"{ROOT}/{CID}" not in plan.delete_dirs   # driver could not be read, so the cluster dir stays
    assert f"{ROOT}/{CID}/driver" not in plan.delete_dirs


def test_unlistable_root_is_a_list_error():
    def ls(path):
        raise FileNotFoundError(path)
    plan = plan_prune(ROOT, ls, CUTOFF)
    assert plan.list_errors and plan.list_errors[0][0] == ROOT and plan.delete_files == []


def test_empty_root_plans_nothing():
    plan = plan_prune(ROOT, _fs({}), CUTOFF)
    assert plan == (([], [], 0, 0, [], []))


def test_constants():
    assert LOG_KINDS == ("driver", "executor", "eventlog", "init_scripts")
    assert CLUSTER_ID.match(CID) and not CLUSTER_ID.match("checkpoints")


def test_cutoff_ms():
    assert cutoff_ms(NOW, 30) == NOW - 30 * DAY
    assert cutoff_ms(NOW, 1) == NOW - DAY
    assert cutoff_ms(NOW, 0) is None
    assert cutoff_ms(NOW, -3) is None


@pytest.mark.parametrize("path,expected", [
    ("", ""), ("   ", ""), (None, ""),
    ("/Volumes/c/s/v", "/Volumes/c/s/v"),
    ("/Volumes/c/s/v/cluster_logs/", "/Volumes/c/s/v/cluster_logs"),
    (" /Volumes/c/s/v/logs ", "/Volumes/c/s/v/logs"),
])
def test_validate_log_root_ok(path, expected):
    assert validate_log_root(path) == expected


@pytest.mark.parametrize("path", [
    "/Volumes/c/s", "/Volumes", "/Volumes/", "dbfs:/cluster_logs", "/tmp/logs", "Volumes/c/s/v",
    "/Volumes/c/s/v/../other", "/Volumes/c/s/v/./logs", "/Volumes/c//v/logs", "/volumes/c/s/v",
])
def test_validate_log_root_rejects(path):
    with pytest.raises(PipelineConfigError, match="must be a UC Volume path"):
        validate_log_root(path)


def test_validate_log_root_rejects_non_string():
    with pytest.raises(PipelineConfigError, match="must be a string"):
        validate_log_root(42)

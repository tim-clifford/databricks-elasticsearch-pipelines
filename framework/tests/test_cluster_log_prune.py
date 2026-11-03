"""Offline unit tests for pipeline_lib.cluster_log_prune: the allow-listed delete plan the `_cluster logs prune`
job executes. A fake directory tree stands in for dbutils.fs.ls."""
import os
from types import SimpleNamespace

import pytest

from pipeline_lib.cluster_log_prune import (
    CLUSTER_ID, LOG_KINDS, PrunePlan, cutoff_ms, execute_plan, parse_retention_days, plan_prune, run_prune,
    validate_log_root,
)
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
    """A dbutils.fs.ls stand-in over {path: [entries]}. A dict value is a directory (its optional "__mtime__"
    key is the directory's own modificationTime; UC Volumes report 0), an int is a file mtime, a
    (mtime, size) tuple a file with a size, and an Exception value makes listing that directory raise."""
    def ls(path):
        node = tree
        for part in path[len(ROOT):].strip("/").split("/") if path != ROOT else []:
            node = node[part]
        if isinstance(node, Exception):
            raise node
        out = []
        for name, child in node.items():
            if name == "__mtime__":
                continue
            if isinstance(child, (dict, Exception)):
                mtime = child.get("__mtime__", 0) if isinstance(child, dict) else 0
                out.append(SimpleNamespace(name=name + "/", size=0, modificationTime=mtime))
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
    # Anything other than a positive "not found" (here, a permission error) is a real failure.
    def ls(path):
        raise PermissionError(f"denied: {path}")
    plan = plan_prune(ROOT, ls, CUTOFF)
    assert plan.list_errors and plan.list_errors[0][0] == ROOT and plan.delete_files == []
    assert plan.root_missing is False


_PARENT = ROOT.rsplit("/", 1)[0]


def _root_ls(root_exc, parent_entries):
    """ls where the root raises root_exc and the parent lists parent_entries (or raises, if an Exception)."""
    def ls(path):
        if path == ROOT:
            raise root_exc
        if path == _PARENT:
            if isinstance(parent_entries, Exception):
                raise parent_entries
            return [SimpleNamespace(name=n, size=0, modificationTime=0) for n in parent_entries]
        raise AssertionError(f"unexpected listing {path}")
    return ls


@pytest.mark.parametrize("exc", [
    FileNotFoundError("/Volumes/cat/sch/vol/cluster_logs"),
    RuntimeError("java.io.FileNotFoundException: /Volumes/cat/sch/vol/cluster_logs does not exist"),
    RuntimeError("ExecutionError: No such file or directory: /Volumes/cat/sch/vol/cluster_logs"),
])
def test_missing_root_is_an_empty_plan_not_a_failure(exc):
    # A configured path no cluster has delivered to yet (the parent lists without it): nothing to prune, so
    # the scheduled run must not fail.
    plan = plan_prune(ROOT, _root_ls(exc, ["checkpoints/", "wheel.whl"]), CUTOFF)
    assert plan.root_missing is True
    assert plan.list_errors == [] and plan.delete_files == [] and plan.delete_dirs == []


def test_missing_volume_is_a_failure_not_an_empty_plan():
    # Log delivery can't create a volume, so a missing volume is a misconfiguration even if the schema
    # positively lacks it.
    vol = "/Volumes/cat/sch/vol"

    def ls(path):
        if path == vol:
            raise FileNotFoundError(path)
        return [SimpleNamespace(name="other_vol/", size=0, modificationTime=0)]
    plan = plan_prune(vol, ls, CUTOFF)
    assert plan.root_missing is False and [p for p, _ in plan.list_errors] == [vol]


def test_missing_file_size_counts_as_zero():
    def ls(path):
        if path == ROOT:
            return [SimpleNamespace(name=f"{CID}/", size=0, modificationTime=0)]
        if path.endswith(CID):
            return [SimpleNamespace(name="driver/", size=0, modificationTime=0)]
        return [SimpleNamespace(name="new", size=None, modificationTime=NEW),
                SimpleNamespace(name="old", modificationTime=OLD)]
    plan = plan_prune(ROOT, ls, CUTOFF)
    assert plan.kept_files == 1 and plan.kept_bytes == 0
    assert plan.delete_files == [(f"{ROOT}/{CID}/driver/old", 0)]


@pytest.mark.parametrize("parent", [["cluster_logs/"], PermissionError("denied")])
def test_not_found_text_without_positive_absence_is_a_failure(parent):
    # The root is present in its parent (or the parent can't be read), so a not-found message must come from
    # something else: fail rather than report a successful empty prune.
    plan = plan_prune(ROOT, _root_ls(RuntimeError("caused by FileNotFoundException: other"), parent), CUTOFF)
    assert plan.root_missing is False and [p for p, _ in plan.list_errors] == [ROOT]


def test_empty_root_plans_nothing():
    plan = plan_prune(ROOT, _fs({}), CUTOFF)
    assert plan == PrunePlan([], [], 0, 0, [], [], root_missing=False)


def test_empty_dirs_with_no_age_evidence_are_kept():
    # A cluster that hasn't delivered yet: an empty (0- or fresh-dated) directory stays; one whose own
    # timestamp is old goes.
    assert plan_prune(ROOT, _fs({CID: {}}), CUTOFF).delete_dirs == []
    assert plan_prune(ROOT, _fs({CID: {"__mtime__": NEW}}), CUTOFF).delete_dirs == []
    assert plan_prune(ROOT, _fs({CID: {"driver": {}}}), CUTOFF).delete_dirs == []
    assert plan_prune(ROOT, _fs({CID: {"__mtime__": OLD}}), CUTOFF).delete_dirs == [f"{ROOT}/{CID}"]


def test_finished_cluster_goes_whole_including_never_used_dirs():
    # Every file under the cluster is old and nothing else is there: the cluster has finished, so its
    # never-used empty directories (executor/app2, eventlog) go with it.
    tree = {CID: {"driver": {"a": OLD}, "executor": {"app": {"0": {"stderr": OLD}}, "app2": {}}, "eventlog": {}}}
    plan = plan_prune(ROOT, _fs(tree), CUTOFF)
    assert set(plan.delete_dirs) == {f"{ROOT}/{CID}/driver", f"{ROOT}/{CID}/executor/app/0",
                                     f"{ROOT}/{CID}/executor/app", f"{ROOT}/{CID}/executor/app2",
                                     f"{ROOT}/{CID}/executor", f"{ROOT}/{CID}/eventlog", f"{ROOT}/{CID}"}
    assert plan.delete_dirs[-1] == f"{ROOT}/{CID}"


def test_active_cluster_keeps_fresh_empty_dirs():
    # A cluster still delivering (driver has a current file): only directories emptied by old-file deletes
    # go; its fresh empty directories stay, and so does the cluster.
    tree = {CID: {"driver": {"log4j-active.log": NEW, "old.gz": OLD},
                  "executor": {"app": {"0": {"stderr": OLD}}, "app2": {}}, "eventlog": {}}}
    plan = plan_prune(ROOT, _fs(tree), CUTOFF)
    assert plan.delete_dirs == [f"{ROOT}/{CID}/executor/app/0", f"{ROOT}/{CID}/executor/app"]


def test_a_skipped_entry_keeps_an_otherwise_finished_cluster():
    plan = plan_prune(ROOT, _fs({CID: {"driver": {"a": OLD}, "executor": {}, "notes": OLD}}), CUTOFF)
    assert plan.delete_dirs == [f"{ROOT}/{CID}/driver"]


def test_the_root_is_never_removed():
    plan = plan_prune(ROOT, _fs({CID: {"driver": {"stdout": OLD}}, CID2: {"__mtime__": OLD}}), CUTOFF)
    assert ROOT not in plan.delete_dirs and f"{ROOT}/{CID}" in plan.delete_dirs


@pytest.mark.parametrize("raw,expected", [("30", 30), (" 30 ", 30), ("0", 0), ("-1", -1), (7, 7)])
def test_parse_retention_days(raw, expected):
    assert parse_retention_days(raw) == expected


@pytest.mark.parametrize("raw", ["", "  ", None, "abc", "3.5", "30d", True, "1_000", 3.5, "3e1", "\u0663\u0660"])
def test_parse_retention_days_fails_closed(raw):
    with pytest.raises(PipelineConfigError, match="must be an integer"):
        parse_retention_days(raw)


def test_cutoff_before_the_epoch_fails_closed():
    with pytest.raises(PipelineConfigError, match="before 1970"):
        cutoff_ms(NOW, 10**9)


def test_constants():
    assert LOG_KINDS == ("driver", "executor", "eventlog", "init_scripts")
    assert CLUSTER_ID.fullmatch(CID) and not CLUSTER_ID.fullmatch("checkpoints")


@pytest.mark.parametrize("name", ["\u0661\u0660\u0662\u0663-200712-abcd1234", CID + "\n", CID + "x!", "x" + CID])
def test_non_ascii_or_padded_cluster_ids_are_skipped(name):
    plan = plan_prune(ROOT, _fs({name: {"driver": {"stdout": OLD}}}), CUTOFF)
    assert plan.delete_files == [] and [p for p, _ in plan.skipped] == [f"{ROOT}/{name}"]


def test_a_root_that_is_a_file_fails():
    # Listing a file returns that file; the parent lists it without a trailing '/'.
    def ls(path):
        return [SimpleNamespace(name="cluster_logs", size=5, modificationTime=OLD)]
    plan = plan_prune(ROOT, ls, CUTOFF)
    assert plan.list_errors == [(ROOT, "the path is a file, not a directory")] and plan.skipped == []


def test_a_directory_holding_one_same_named_file_is_a_directory():
    def ls(path):
        if path == ROOT:
            return [SimpleNamespace(name="cluster_logs", size=5, modificationTime=OLD)]
        return [SimpleNamespace(name="cluster_logs/", size=0, modificationTime=0)]   # the parent: a directory
    plan = plan_prune(ROOT, ls, CUTOFF)
    assert plan.list_errors == [] and [p for p, _ in plan.skipped] == [f"{ROOT}/cluster_logs"]


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


# --------------------------------------------------------------------------- execute_plan


class _Rm:
    """Records rm calls; raises for the paths in `fail`, returns False for those in `false`."""
    def __init__(self, fail=(), false=()):
        self.calls, self.fail, self.false = [], set(fail), set(false)

    def __call__(self, path, recurse=None):
        self.calls.append((path, recurse))
        if path in self.fail:
            raise OSError(f"cannot delete {path}")
        return path not in self.false


def _plan(files, dirs, list_errors=()):
    return PrunePlan([(f, 10) for f in files], list(dirs), 0, 0, [], list(list_errors))


def test_execute_dry_run_deletes_nothing():
    rm = _Rm()
    plan = _plan([f"{ROOT}/{CID}/driver/a"], [f"{ROOT}/{CID}/driver"], [(f"{ROOT}/x", "boom")])
    assert execute_plan(plan, rm, dry_run=True) == (0, 0, 0, [f"list {ROOT}/x: boom"])
    assert rm.calls == []


def test_execute_deletes_files_then_dirs_non_recursively():
    rm = _Rm()
    files = [f"{ROOT}/{CID}/driver/a", f"{ROOT}/{CID}/executor/app/0/b"]
    dirs = [f"{ROOT}/{CID}/executor/app/0", f"{ROOT}/{CID}/executor/app", f"{ROOT}/{CID}/driver"]
    assert execute_plan(_plan(files, dirs), rm, dry_run=False) == (2, 3, 0, [])
    assert rm.calls == [(p, False) for p in files + dirs]


def test_execute_keeps_going_and_skips_dirs_above_a_failed_file():
    bad = f"{ROOT}/{CID}/executor/app/0/b"
    rm = _Rm(fail=[bad])
    files = [bad, f"{ROOT}/{CID}/driver/a"]
    dirs = [f"{ROOT}/{CID}/executor/app/0", f"{ROOT}/{CID}/executor/app", f"{ROOT}/{CID}/executor",
            f"{ROOT}/{CID}/driver", f"{ROOT}/{CID}"]
    deleted_files, deleted_dirs, raced, failures = execute_plan(_plan(files, dirs), rm, dry_run=False)
    # The good file and its directory still go; every ancestor of the failed file is skipped, so the one
    # failure is reported once.
    assert (deleted_files, deleted_dirs, raced) == (1, 1, 0)
    assert failures == [f"delete {bad}: OSError: cannot delete {bad}"]
    assert [p for p, _ in rm.calls] == files + [f"{ROOT}/{CID}/driver"]


def test_execute_reports_a_failed_dir_and_its_ancestors_are_skipped():
    d = f"{ROOT}/{CID}/driver"
    rm = _Rm(fail=[d])
    _, deleted_dirs, raced, failures = execute_plan(_plan([], [d, f"{ROOT}/{CID}"]), rm, dry_run=False,
                                                     ls=lambda p: [])
    assert (deleted_dirs, raced) == (0, 0) and failures == [f"remove dir {d}: OSError: cannot delete {d}"]
    assert [p for p, _ in rm.calls] == [d]


class _NotEmptyRm(_Rm):
    """rm that refuses the `fail` paths the way a non-empty directory does (the exception seen live)."""
    def __call__(self, path, recurse=None):
        self.calls.append((path, recurse))
        if path in self.fail:
            raise RuntimeError(f"org.apache.hadoop.fs.PathIsNotEmptyDirectoryException: `{path}`: Directory is not empty")
        return True


def test_execute_a_dir_that_gained_a_file_is_a_race_not_a_failure():
    # A fresh log landed after the listing: the non-recursive remove refuses (not empty), ls shows it non-empty,
    # so the directory is kept and counted as raced; its ancestors are skipped; nothing fails.
    d = f"{ROOT}/{CID}/driver"
    rm = _NotEmptyRm(fail=[d])
    ls = lambda p: [SimpleNamespace(name="stdout", size=1, modificationTime=NOW)] if p == d else []  # noqa: E731
    assert execute_plan(_plan([], [d, f"{ROOT}/{CID}"]), rm, dry_run=False, ls=ls) == (0, 0, 1, [])
    assert [p for p, _ in rm.calls] == [d]


def test_execute_a_dir_whose_relisting_fails_is_still_a_failure():
    d = f"{ROOT}/{CID}/driver"

    def ls(p):
        raise OSError("gone")
    _, _, raced, failures = execute_plan(_plan([], [d]), _Rm(fail=[d]), dry_run=False, ls=ls)
    assert raced == 0 and len(failures) == 1


def test_execute_rm_returning_false_is_a_failure():
    f = f"{ROOT}/{CID}/driver/a"
    deleted_files, deleted_dirs, _, failures = execute_plan(
        _plan([f], [f"{ROOT}/{CID}/driver"]), _Rm(false=[f]), dry_run=False)
    assert (deleted_files, deleted_dirs) == (0, 0)
    assert failures == [f"delete {f}: OSError: rm returned False (nothing deleted)"]


def test_execute_dir_rm_returning_false_is_a_failure():
    d = f"{ROOT}/{CID}/driver"
    assert execute_plan(_plan([], [d]), _Rm(false=[d]), dry_run=False, ls=lambda p: []) == (
        0, 0, 0, [f"remove dir {d}: OSError: rm returned False (nothing deleted)"])


@pytest.mark.parametrize("not_found", [
    FileNotFoundError("gone"),
    RuntimeError("java.io.FileNotFoundException: /Volumes/cat/sch/vol/cluster_logs/x does not exist"),
])
def test_execute_dir_already_gone_counts_as_removed(not_found):
    # The remove failed but the directory is positively gone (its listing is not-found AND the parent lists
    # without it, e.g. a concurrent run took it): not a failure, and its parent is still tried.
    d, parent = f"{ROOT}/{CID}/driver", f"{ROOT}/{CID}"

    def ls(p):
        if p == d:
            raise not_found
        return []
    rm = _Rm(fail=[d])
    assert execute_plan(_plan([], [d, parent]), rm, dry_run=False, ls=ls) == (0, 2, 0, [])
    assert [p for p, _ in rm.calls] == [d, parent]


def test_execute_not_found_text_without_absence_is_still_a_failure():
    # The relisting says not-found but the parent still shows the directory: not proven gone.
    d = f"{ROOT}/{CID}/driver"

    def ls(p):
        if p == d:
            raise RuntimeError("caused by FileNotFoundException: something else")
        return [SimpleNamespace(name="driver/", size=0, modificationTime=0)]
    _, deleted_dirs, raced, failures = execute_plan(_plan([], [d]), _Rm(fail=[d]), dry_run=False, ls=ls)
    assert (deleted_dirs, raced, len(failures)) == (0, 0, 1)


def test_execute_without_ls_a_failed_dir_is_a_failure():
    d = f"{ROOT}/{CID}/driver"
    _, _, raced, failures = execute_plan(_plan([], [d]), _Rm(fail=[d]), dry_run=False)
    assert raced == 0 and failures == [f"remove dir {d}: OSError: cannot delete {d}"]


def test_active_cluster_removes_a_dir_by_its_own_old_timestamp():
    tree = {CID: {"driver": {"live": NEW}, "executor": {"app": {"__mtime__": OLD}, "app2": {}}}}
    assert plan_prune(ROOT, _fs(tree), CUTOFF).delete_dirs == [f"{ROOT}/{CID}/executor/app"]


def test_missing_root_several_levels_deep_is_an_empty_plan():
    # /Volumes/cat/sch/vol/a/b where a/ doesn't exist either: climb to the volume, which lists without a/.
    root = "/Volumes/cat/sch/vol/a/b"

    def ls(path):
        if path in (root, "/Volumes/cat/sch/vol/a"):
            raise FileNotFoundError(path)
        if path == "/Volumes/cat/sch/vol":
            return [SimpleNamespace(name="checkpoints/", size=0, modificationTime=0)]
        raise AssertionError(f"listed above the volume: {path}")
    assert plan_prune(root, ls, CUTOFF).root_missing is True


def test_missing_volume_never_climbs_above_it():
    # The root's volume is itself missing: the climb stops at the volume, so a schema listing that happens
    # not to contain the volume can't make the root look merely "not created yet".
    root = "/Volumes/cat/sch/vol/a"

    def ls(path):
        if path in (root, "/Volumes/cat/sch/vol"):
            raise FileNotFoundError(path)
        return [SimpleNamespace(name="other_vol/", size=0, modificationTime=0)]
    plan = plan_prune(root, ls, CUTOFF)
    assert plan.root_missing is False and [p for p, _ in plan.list_errors] == [root]


def test_execute_file_already_gone_counts_as_deleted():
    # The file vanished between listing and delete (rm raises or returns False) and the parent lists
    # without it: counted as deleted, not a failure, and its directory is still removed.
    f, d = f"{ROOT}/{CID}/driver/a", f"{ROOT}/{CID}/driver"
    for rm in (_Rm(fail=[f]), _Rm(false=[f])):
        assert execute_plan(_plan([f], [d]), rm, dry_run=False, ls=lambda p: []) == (1, 1, 0, [])


def test_execute_file_rm_failure_with_file_still_listed_is_a_failure():
    f, d = f"{ROOT}/{CID}/driver/a", f"{ROOT}/{CID}/driver"
    ls = lambda p: [SimpleNamespace(name="a", size=1, modificationTime=OLD)]  # noqa: E731
    deleted_files, deleted_dirs, _, failures = execute_plan(_plan([f], [d]), _Rm(fail=[f]), dry_run=False, ls=ls)
    assert (deleted_files, deleted_dirs, len(failures)) == (0, 0, 1)


@pytest.mark.parametrize("parent_exc", [PermissionError("denied"), FileNotFoundError("parent gone")])
def test_execute_file_rm_failure_with_unlistable_parent_is_a_failure(parent_exc):
    # An unreadable parent is not proof the file is gone (and the no-floor check must not climb further).
    f = f"{ROOT}/{CID}/driver/a"

    def ls(p):
        raise parent_exc
    deleted_files, _, _, failures = execute_plan(_plan([f], []), _Rm(fail=[f]), dry_run=False, ls=ls)
    assert deleted_files == 0 and len(failures) == 1


def test_terminated_leftover_with_no_files_is_removed():
    # An earlier run deleted every file but stopped before removing the directories: no files, no readable
    # age, but eventlog/ shows the cluster terminated, so the leftover tree goes.
    tree = {CID: {"driver": {}, "executor": {"app": {}}, "eventlog": {"x": {}}}}
    plan = plan_prune(ROOT, _fs(tree), CUTOFF)
    assert plan.delete_dirs[-1] == f"{ROOT}/{CID}" and len(plan.delete_dirs) == 6
    # Without eventlog/ (a cluster that hasn't delivered yet), the same empty tree stays.
    assert plan_prune(ROOT, _fs({CID: {"driver": {}, "executor": {"app": {}}}}), CUTOFF).delete_dirs == []
    # A file-less cluster holding a current file elsewhere is not a leftover.
    assert plan_prune(ROOT, _fs({CID: {"driver": {"live": NEW}, "eventlog": {}}}), CUTOFF).delete_dirs == []


def test_execute_other_rm_errors_on_a_non_empty_dir_are_failures():
    # A permission or I/O error is not a race even if the directory lists non-empty.
    d = f"{ROOT}/{CID}/driver"
    ls = lambda p: [SimpleNamespace(name="stdout", size=1, modificationTime=NEW)]  # noqa: E731
    _, _, raced, failures = execute_plan(_plan([], [d]), _Rm(fail=[d]), dry_run=False, ls=ls)
    assert raced == 0 and failures == [f"remove dir {d}: OSError: cannot delete {d}"]


def test_execute_not_empty_by_exception_type_is_a_race():
    class PathIsNotEmptyDirectoryException(Exception):
        pass
    d = f"{ROOT}/{CID}/driver"

    def rm(path, recurse=None):
        raise PathIsNotEmptyDirectoryException("refused")
    ls = lambda p: [SimpleNamespace(name="stdout", size=1, modificationTime=NEW)]  # noqa: E731
    assert execute_plan(_plan([], [d]), rm, dry_run=False, ls=ls) == (0, 0, 1, [])


def test_execute_not_empty_refusal_that_relists_empty_is_a_failure():
    d = f"{ROOT}/{CID}/driver"
    _, _, raced, failures = execute_plan(_plan([], [d]), _NotEmptyRm(fail=[d]), dry_run=False, ls=lambda p: [])
    assert raced == 0 and len(failures) == 1


def test_execute_dir_rm_returning_false_on_a_non_empty_dir_is_a_race():
    d = f"{ROOT}/{CID}/driver"
    ls = lambda p: [SimpleNamespace(name="stdout", size=1, modificationTime=NEW)] if p == d else []  # noqa: E731
    assert execute_plan(_plan([], [d]), _Rm(false=[d]), dry_run=False, ls=ls) == (0, 0, 1, [])


def test_execute_file_gone_with_its_parent_climbs_to_the_root():
    # A concurrent cleanup took the file AND its directory: the climb (bounded by root) proves it gone.
    f = f"{ROOT}/{CID}/driver/a"

    def ls(p):
        if p in (f"{ROOT}/{CID}/driver",):
            raise FileNotFoundError(p)
        return []   # the cluster dir lists without driver/
    assert execute_plan(_plan([f], []), _Rm(fail=[f]), dry_run=False, ls=ls, root=ROOT) == (1, 0, 0, [])
    # Without a root the climb doesn't go up, so it is not proven gone.
    assert len(execute_plan(_plan([f], []), _Rm(fail=[f]), dry_run=False, ls=ls)[3]) == 1


def test_execute_dir_gone_with_its_parent_climbs_to_the_root():
    # A concurrent run removed the whole cluster directory: driver/ and the cluster dir are both not found,
    # and the root lists without the cluster, so driver/ counts as removed (root bounds the climb).
    d = f"{ROOT}/{CID}/driver"

    def ls(p):
        if p in (d, f"{ROOT}/{CID}"):
            raise FileNotFoundError(p)
        return []
    assert execute_plan(_plan([], [d]), _Rm(fail=[d]), dry_run=False, ls=ls, root=ROOT) == (0, 1, 0, [])
    assert len(execute_plan(_plan([], [d]), _Rm(fail=[d]), dry_run=False, ls=ls)[3]) == 1


# --------------------------------------------------------------------------- run_prune


def _never(*a, **k):
    raise AssertionError("must not be called")


def test_run_prune_skips_when_unset_or_disabled():
    assert "reason=path unset" in run_prune("", 30, CUTOFF, False, _never, _never, log=_never)
    assert "reason=retention disabled" in run_prune(ROOT, 0, None, False, _never, _never, log=_never)


def test_run_prune_dry_run_deletes_nothing_and_lists_the_plan():
    lines = []
    summary = run_prune(ROOT, 30, CUTOFF, True, _fs({CID: {"driver": {"a": OLD}}}), _never, log=lines.append)
    assert "dry_run=True" in summary and "planned_files=1" in summary and "deleted_files=0" in summary
    assert any(f"would delete {ROOT}/{CID}/driver/a" in l for l in lines)
    assert any(f"would remove directory {ROOT}/{CID}" in l for l in lines)


def test_run_prune_deletes_and_summarizes():
    rm = _Rm()
    summary = run_prune(ROOT, 30, CUTOFF, False, _fs({CID: {"driver": {"a": OLD}}, "x.txt": OLD}), rm, log=lambda l: None)
    assert "deleted_files=1" in summary and "deleted_dirs=2" in summary and "skipped=1" in summary
    assert [p for p, _ in rm.calls] == [f"{ROOT}/{CID}/driver/a", f"{ROOT}/{CID}/driver", f"{ROOT}/{CID}"]


def test_run_prune_raises_after_trying_everything():
    bad = f"{ROOT}/{CID}/driver/a"
    rm = _Rm(fail=[bad])
    tree = {CID: {"driver": {"a": OLD}}, CID2: {"driver": {"b": OLD}}}
    with pytest.raises(RuntimeError, match=r"FAILED \(1 failure"):
        run_prune(ROOT, 30, CUTOFF, False, _fs(tree), rm, log=lambda l: None)
    assert f"{ROOT}/{CID2}/driver/b" in [p for p, _ in rm.calls]   # the other cluster was still pruned


def test_run_prune_warns_on_a_missing_root():
    lines = []
    summary = run_prune(ROOT, 30, CUTOFF, False, _root_ls(FileNotFoundError(ROOT), []), _never, log=lines.append)
    assert "root_missing=True" in summary and any("does not exist yet" in l for l in lines)


def test_run_prune_caps_long_lists():
    tree = {f"stray{i}.txt": OLD for i in range(250)}
    lines = []
    run_prune(ROOT, 30, CUTOFF, False, _fs(tree), _never, log=lines.append)
    assert sum("SKIPPED" in l for l in lines) == 200 and any("50 more skipped" in l for l in lines)


def test_run_prune_a_listing_failure_alone_fails_the_run():
    # Nothing to delete, no delete failure, but one cluster directory can't be read: not green.
    lines = []
    with pytest.raises(RuntimeError, match=r"FAILED \(1 failure"):
        run_prune(ROOT, 30, CUTOFF, False, _fs({CID: PermissionError("denied")}), _Rm(), log=lines.append)
    assert any(f"LIST FAILED (left untouched): {ROOT}/{CID}" in l for l in lines)


def test_a_same_named_file_with_an_unreadable_parent_is_not_called_a_file_root():
    # Can't confirm the root is a file, so it stays a directory: its one entry is skipped, nothing is deleted.
    def ls(path):
        if path == ROOT:
            return [SimpleNamespace(name="cluster_logs", size=5, modificationTime=OLD)]
        raise PermissionError("denied")
    plan = plan_prune(ROOT, ls, CUTOFF)
    assert plan.list_errors == [] and plan.delete_files == [] and [p for p, _ in plan.skipped] == [f"{ROOT}/cluster_logs"]


# --------------------------------------------------------------------------- triage, threads, time budget
from pipeline_lib.cluster_log_prune import (  # noqa: E402
    MAX_MINUTES_LIMIT, MAX_WORKERS, ItemError, _bounded_map, _oldest_first_key, execute_plan_until, parse_max_minutes, parse_workers, triage_cluster,
)


class _Counting:
    """Wraps an ls and records every listed path."""
    def __init__(self, ls):
        self.ls, self.paths = ls, []

    def __call__(self, path):
        self.paths.append(path)
        return self.ls(path)


def _terminated(driver_mtime, **extra):
    """A terminated job cluster: driver/ (newest file at driver_mtime), an old executor file, eventlog/."""
    tree = {"driver": {"stdout": driver_mtime, "log4j-old.log.gz": OLD},
            "executor": {"app-1": {"0": {"stderr": OLD}}}, "eventlog": {"x": {"y": {"eventlog": OLD}}}}
    tree.update(extra)
    return tree


def test_young_terminated_cluster_is_kept_after_two_listings():
    # Terminated (eventlog/) with a driver log inside retention: kept without walking, even though older
    # files sit in it. Only the cluster dir and driver/ are listed.
    ls = _Counting(_fs({CID: _terminated(NEW)}))
    summary = run_prune(ROOT, 30, CUTOFF, False, ls, _never, log=lambda l: None)
    assert ls.paths == [ROOT, f"{ROOT}/{CID}", f"{ROOT}/{CID}/driver"]
    assert "clusters=1 clusters_young=1 clusters_walked=0" in summary and "planned_files=0" in summary


def test_terminated_cluster_past_retention_is_walked_and_removed_whole():
    rm = _Rm()
    summary = run_prune(ROOT, 30, CUTOFF, False, _fs({CID: _terminated(OLD)}), rm, log=lambda l: None)
    assert "clusters_young=0 clusters_walked=1" in summary and "deleted_files=4" in summary
    assert rm.calls[-1] == (f"{ROOT}/{CID}", False)   # removed whole, non-recursively, last


def test_running_cluster_is_always_walked_so_old_rolled_logs_go():
    # No eventlog/: still running (for example a continuous feed's cluster). Its fresh driver log must not
    # stop the walk, or a long-running cluster's old rolled logs would never be deleted.
    tree = {CID: {"driver": {"log4j-active.log": NEW, "log4j-old.log.gz": OLD}}}
    rm = _Rm()
    summary = run_prune(ROOT, 30, CUTOFF, False, _fs(tree), rm, log=lambda l: None)
    assert "clusters_young=0 clusters_walked=1" in summary
    assert [p for p, _ in rm.calls] == [f"{ROOT}/{CID}/driver/log4j-old.log.gz"]


@pytest.mark.parametrize("cluster", [
    {"eventlog": {"x": {"eventlog": OLD}}, "executor": {"app": {"0": {"stderr": OLD}}}},   # no driver/
    {"eventlog": {"x": {"eventlog": OLD}}, "driver": {"stdout": 0}},                       # unreadable age
    {"eventlog": {"x": {"eventlog": OLD}}, "driver": {"sub": {"f": NEW}}},                 # no file in driver/
])
def test_a_terminated_cluster_without_driver_age_evidence_is_walked(cluster):
    summary = run_prune(ROOT, 30, CUTOFF, True, _fs({CID: cluster}), _never, log=lambda l: None)
    assert "clusters_young=0 clusters_walked=1" in summary


def test_an_unreadable_driver_dir_is_walked_and_fails_the_run():
    tree = {CID: {"eventlog": {"x": {"eventlog": OLD}}, "driver": PermissionError("denied")}}
    with pytest.raises(RuntimeError, match=r"FAILED \(1 failure"):
        run_prune(ROOT, 30, CUTOFF, False, _fs(tree), _Rm(), log=lambda l: None)


def test_triage_reports_unknown_entries_of_a_young_cluster():
    tree = {CID: _terminated(NEW, stray={"f": OLD})}
    t = triage_cluster(f"{ROOT}/{CID}", None, _fs(tree), CUTOFF)
    assert t.young and t.skipped == [(f"{ROOT}/{CID}/stray", "not a known log kind")]
    summary = run_prune(ROOT, 30, CUTOFF, False, _fs(tree), _never, log=lambda l: None)
    assert "skipped=1" in summary


def test_triage_records_an_unlistable_cluster():
    t = triage_cluster(f"{ROOT}/{CID}", None, _fs({CID: PermissionError("denied")}), CUTOFF)
    assert not t.young and t.kinds == [] and t.list_errors[0][0] == f"{ROOT}/{CID}"


def test_driver_newest_exactly_at_the_cutoff_is_young():
    # At the cutoff a file is kept (strictly older is deleted), so the cluster can't be removed whole yet.
    assert triage_cluster(f"{ROOT}/{CID}", None, _fs({CID: _terminated(CUTOFF)}), CUTOFF).young
    assert not triage_cluster(f"{ROOT}/{CID}", None, _fs({CID: _terminated(CUTOFF - 1)}), CUTOFF).young


def _many(n, driver_mtime=OLD, month="10"):
    return {f"{month}23-{i:06d}-c{i}": _terminated(driver_mtime) for i in range(n)}


def test_threads_give_the_same_result_as_one_thread():
    tree = {**_many(30), **_many(20, NEW, month="11"), "stray.txt": OLD}
    one = run_prune(ROOT, 30, CUTOFF, False, _fs(tree), _Rm(), log=lambda l: None, workers=1)
    rm8 = _Rm()
    eight = run_prune(ROOT, 30, CUTOFF, False, _fs(tree), rm8, log=lambda l: None, workers=8)
    strip = lambda s: s.replace("workers=1", "").replace("workers=8", "")
    assert strip(one) == strip(eight)
    assert "clusters=50 clusters_young=20 clusters_walked=30" in eight and "deleted_files=120" in eight
    # 4 files and 8 directories per walked cluster (driver, executor, app-1, 0, eventlog, x, y, the cluster)
    assert len(rm8.calls) == 30 * (4 + 8)


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_the_budget_stops_the_prune_green_and_oldest_clusters_go_first():
    # Each delete takes a simulated minute; the budget is 30 minutes, so the run stops partway, green, with
    # the newest cluster ids left for the next run. The cluster in flight at the deadline stops before its
    # next delete and counts as remaining, so at most one remaining cluster has been partly pruned.
    clock = _Clock()

    class _SlowRm(_Rm):
        def __call__(self, path, recurse=None):
            clock.t += 60
            return super().__call__(path, recurse)

    tree = {f"{m:02d}01-000000-c{m}": _terminated(OLD) for m in range(6, 0, -1)}   # listed newest first
    rm, lines = _SlowRm(), []
    summary = run_prune(ROOT, 30, CUTOFF, False, _fs(tree), rm, log=lines.append, workers=1, max_minutes=30,
                        clock=clock, today_mmdd=1231)
    assert "incomplete=True" in summary
    started = sorted({p[len(ROOT) + 1:].split("/")[0] for p, _ in rm.calls})
    remaining = int(summary.split("remaining_clusters=")[1].split()[0])
    assert remaining > 0 and len(started) in (6 - remaining, 6 - remaining + 1)
    assert clock.t <= 30 * 60 + 60   # no delete starts after the deadline: at most the one in progress
    assert started == sorted(tree)[:len(started)]   # oldest first
    assert any("not yet walked" in l for l in lines)


def test_without_running_out_the_run_is_complete():
    summary = run_prune(ROOT, 30, CUTOFF, False, _fs(_many(3)), _Rm(), log=lambda l: None, workers=2,
                        max_minutes=10, clock=_Clock())
    assert "incomplete=False remaining_clusters=0" in summary


def test_triage_that_runs_out_of_time_fails_and_deletes_nothing():
    clock = _Clock()
    base = _fs(_many(20))

    def ls(path):
        clock.t += 120   # every listing takes two simulated minutes
        return base(path)

    with pytest.raises(RuntimeError, match=r"triage classified only \d+ of 20 cluster\(s\) within max_minutes=5"):
        run_prune(ROOT, 30, CUTOFF, False, ls, _never, log=lambda l: None, workers=1, max_minutes=5, clock=clock)


def test_progress_is_logged_about_every_interval():
    clock = _Clock()
    base = _fs(_many(6, NEW))

    def ls(path):
        clock.t += 30
        return base(path)

    lines = []
    run_prune(ROOT, 30, CUTOFF, False, ls, _never, log=lines.append, workers=1, clock=clock, progress_seconds=60,
              poll=0.01)
    progress = [l for l in lines if "progress: triage" in l]
    assert len(progress) >= 2 and all("cluster(s)" in l and "s elapsed" in l for l in progress)


def test_bounded_map_starts_nothing_once_stopped_and_keeps_order():
    results, started = _bounded_map([1, 2, 3], lambda x: x * 10, 2, lambda: False, lambda c: None, poll=0.01)
    assert results == [10, 20, 30] and started == 3
    results, started = _bounded_map([1, 2, 3], _never, 2, lambda: True, lambda c: None, poll=0.01)
    assert results == [None, None, None] and started == 0


def test_bounded_map_returns_a_worker_exception_in_its_slot():
    # One item's unexpected error doesn't discard the others' results.
    def fn(x):
        if x == 2:
            raise ValueError("bug")
        return x * 10
    results, started = _bounded_map([1, 2, 3], fn, 2, lambda: False, lambda c: None, poll=0.01)
    assert started == 3 and results[0] == 10 and results[2] == 30
    assert isinstance(results[1], ItemError) and str(results[1].exc) == "bug"


def test_bounded_map_runs_at_most_workers_at_once():
    import threading
    peak, live, lock = [0], [0], threading.Lock()

    def fn(x):
        with lock:
            live[0] += 1
            peak[0] = max(peak[0], live[0])
        threading.Event().wait(0.005)
        with lock:
            live[0] -= 1
        return x

    results, _ = _bounded_map(list(range(40)), fn, 4, lambda: False, lambda c: None, poll=0.01)
    assert results == list(range(40)) and 1 < peak[0] <= 4


@pytest.mark.parametrize("raw,expected", [(1, 1), ("16", 16), (" 8 ", 8), (MAX_WORKERS, MAX_WORKERS)])
def test_parse_workers_accepts(raw, expected):
    assert parse_workers(raw) == expected


@pytest.mark.parametrize("raw", [0, MAX_WORKERS + 1, "", "1.5", "-1", "+2", True, 2.0, None, "8 threads"])
def test_parse_workers_rejects(raw):
    with pytest.raises(PipelineConfigError, match="cluster_log_prune_workers"):
        parse_workers(raw)


@pytest.mark.parametrize("raw,expected", [(1, 1), ("200", 200), (MAX_MINUTES_LIMIT, MAX_MINUTES_LIMIT)])
def test_parse_max_minutes_accepts(raw, expected):
    assert parse_max_minutes(raw) == expected


@pytest.mark.parametrize("raw", [0, MAX_MINUTES_LIMIT + 1, "240", "", "1e2", False, None])
def test_parse_max_minutes_rejects(raw):
    with pytest.raises(PipelineConfigError, match="cluster_log_prune_max_minutes"):
        parse_max_minutes(raw)


def test_a_cluster_that_cannot_be_listed_is_never_touched():
    # Its own timestamp is old, but with no listing there is no evidence it is empty or finished: no rm at all.
    def ls(path):
        if path == ROOT:
            return [SimpleNamespace(name=CID + "/", size=0, modificationTime=OLD)]
        raise PermissionError("denied")
    rm = _Rm()
    with pytest.raises(RuntimeError, match=r"FAILED \(1 failure"):
        run_prune(ROOT, 30, CUTOFF, False, ls, rm, log=lambda l: None)
    assert rm.calls == []


# --------------------------------------------------------------------------- volume_ls and the triage_ls split
from pipeline_lib.cluster_log_prune import volume_ls  # noqa: E402


def test_volume_ls_has_dbutils_entry_semantics(tmp_path):
    (tmp_path / "driver").mkdir()
    f = tmp_path / "stdout"
    f.write_text("x" * 42)
    os.utime(f, (1_700_000_000.123, 1_700_000_000.123))
    entries = {e.name: e for e in volume_ls(str(tmp_path))}
    assert set(entries) == {"driver/", "stdout"}
    assert entries["driver/"].size == 0 and entries["driver/"].modificationTime == 0
    assert entries["stdout"].size == 42 and entries["stdout"].modificationTime == 1_700_000_000_123


def test_volume_ls_raises_on_a_missing_directory(tmp_path):
    with pytest.raises(FileNotFoundError):
        volume_ls(str(tmp_path / "absent"))


def test_volume_ls_drives_a_real_prune(tmp_path):
    # The adapter's entries work with the planner end to end on a real directory tree.
    root = tmp_path / "cluster_logs"
    for rel in ("driver/stdout", "eventlog/x/y/eventlog", "executor/app/0/stderr"):
        p = root / CID / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    now_ms = int(os.path.getmtime(root / CID / "driver/stdout") * 1000)
    young = triage_cluster(f"{root}/{CID}", None, volume_ls, now_ms)
    old = triage_cluster(f"{root}/{CID}", None, volume_ls, now_ms + DAY)
    assert young.young and not old.young


def test_triage_ls_is_used_for_triage_only_and_walks_relist_with_ls():
    tree = {CID: _terminated(OLD), CID2: _terminated(NEW)}
    fresh = _Counting(_fs(tree))
    # A stale triage view: CID's eventlog/ hasn't shown up yet. CID2 is young in it.
    stale = _Counting(_fs({CID: {"driver": {"stdout": OLD}}, CID2: _terminated(NEW)}))
    rm = _Rm()
    summary = run_prune(ROOT, 30, CUTOFF, False, fresh, rm, log=lambda l: None, triage_ls=stale)
    assert set(stale.paths) == {f"{ROOT}/{CID}", f"{ROOT}/{CID2}", f"{ROOT}/{CID2}/driver"}
    assert fresh.paths[0] == ROOT and f"{ROOT}/{CID2}" not in fresh.paths   # the young cluster isn't walked
    # The walk went by the fresh listing: eventlog/ (missing from the stale view) was walked and deleted too,
    # and the cluster was removed whole.
    assert f"{ROOT}/{CID}/eventlog/x/y/eventlog" in [p for p, _ in rm.calls] and rm.calls[-1][0] == f"{ROOT}/{CID}"
    assert "clusters_young=1 clusters_walked=1" in summary


def test_a_walked_cluster_that_cannot_be_relisted_fails_without_any_delete():
    calls = {"n": 0}
    base = _fs({CID: _terminated(OLD)})

    def ls(path):
        if path == f"{ROOT}/{CID}":
            raise PermissionError("denied on the walk")
        return base(path)

    rm = _Rm()
    with pytest.raises(RuntimeError, match=r"FAILED \(1 failure"):
        run_prune(ROOT, 30, CUTOFF, False, ls, rm, log=lambda l: None, triage_ls=base)
    assert rm.calls == []


# --------------------------------------------------------------------------- the budget counts from the job run start
from pipeline_lib.cluster_log_prune import spent_before_seconds  # noqa: E402


def test_spent_before_reads_the_job_run_start():
    spent, note = spent_before_seconds("1700000000000", 1700000600000)
    assert spent == 600 and "600 s before this attempt" in note


@pytest.mark.parametrize("raw", ["", "{{job.start_time.timestamp_ms}}", " ", "0", "-5", "1.7e12", None, 1700000000000,
                                 "17000000000001"])
def test_spent_before_falls_back_to_this_attempt(raw):
    # Unresolved, empty, non-integer, non-string, or in the future: counted from this attempt, with a note.
    spent, note = spent_before_seconds(raw, 1700000600000)
    assert spent == 0 and "counts from this attempt" in note


def test_spent_before_rejects_a_start_over_a_day_old():
    spent, note = spent_before_seconds("1700000000000", 1700000000000 + 86_401_000)
    assert spent == 0 and "implausible" in note


def _clusters_started(spent_seconds):
    """Clusters a 30-minute run starts when each cluster's deletes take 12 simulated minutes. The fake clock
    advances on the worker thread, so the first refill may race the deadline check: assert ranges, not counts."""
    clock = _Clock()

    class _SlowRm(_Rm):
        def __call__(self, path, recurse=None):
            clock.t += 60
            return super().__call__(path, recurse)

    tree = {f"{m:02d}01-000000-c{m}": _terminated(OLD) for m in range(1, 7)}
    rm = _SlowRm()
    summary = run_prune(ROOT, 30, CUTOFF, False, _fs(tree), rm, log=lambda l: None, workers=1, max_minutes=30,
                        spent_seconds=spent_seconds, clock=clock)
    assert "incomplete=True" in summary and f"spent_before_s={int(spent_seconds)}" in summary
    return len({p[len(ROOT) + 1:].split("/")[0] for p, _ in rm.calls})


def test_a_retry_gets_only_the_rest_of_the_runs_budget():
    # An earlier attempt of the same run used 25 of the 30 minutes: only what was already in flight at the
    # 5-minute mark runs (at most the first two), where a fresh budget starts more.
    assert _clusters_started(25 * 60) <= 2
    assert _clusters_started(0) >= 3


def test_a_retry_with_the_budget_already_used_fails_before_deleting():
    with pytest.raises(RuntimeError, match=r"triage classified only 0 of 3 .* \(1800 s of it used before this attempt\)"):
        run_prune(ROOT, 30, CUTOFF, False, _fs(_many(3)), _never, log=lambda l: None, workers=1, max_minutes=30,
                  spent_seconds=30 * 60, clock=_Clock())


def test_a_queued_item_does_not_start_after_the_deadline():
    # Item 2 is queued (2 x workers in flight) while item 1 runs past the deadline: it must not start then.
    clock = _Clock()

    def fn(x):
        import threading
        threading.Event().wait(0.05)   # let the main thread queue item 2 before the clock passes the deadline
        clock.t += 20
        return x

    results, started = _bounded_map([1, 2, 3], fn, 1, lambda: clock.t >= 10, lambda c: None, poll=0.01)
    assert results == [1, None, None] and started == 1


# --------------------------------------------------------------------------- Isaac round 2 fixes

def test_a_cluster_in_flight_stops_before_its_next_delete_at_the_deadline():
    # One cluster with many old files, and a deadline that passes after its third delete: the rest are left
    # for the next run, nothing is removed after the deadline, and the run stays green with the cluster
    # counted as remaining.
    clock = _Clock()

    class _SlowRm(_Rm):
        def __call__(self, path, recurse=None):
            clock.t += 60
            return super().__call__(path, recurse)

    tree = {CID: _terminated(OLD)}   # four old files, so the deadline (after three deletes) falls inside it
    rm = _SlowRm()
    summary = run_prune(ROOT, 30, CUTOFF, False, _fs(tree), rm, log=lambda l: None, workers=1, max_minutes=3,
                        clock=clock)
    assert len(rm.calls) == 3
    assert "incomplete=True remaining_clusters=1" in summary and "clusters_walked=0" in summary
    assert "failures=0" in summary


def test_execute_plan_until_stops_and_reports_it():
    plan = PrunePlan([(f"{ROOT}/{CID}/driver/f{i}", 1) for i in range(5)], [f"{ROOT}/{CID}/driver", f"{ROOT}/{CID}"],
                     0, 0, [], [])
    rm = _Rm()
    stop = lambda: len(rm.calls) >= 2
    files, dirs, raced, failures, stopped = execute_plan_until(plan, rm, False, None, ROOT, stop=stop)
    assert (files, dirs, raced, failures, stopped) == (2, 0, 0, [], True)
    assert execute_plan_until(plan, _Rm(), False, None, ROOT)[4] is False
    assert len(execute_plan(plan, _Rm(), False, None, ROOT)) == 4


def test_an_unexpected_prune_error_is_recorded_and_the_rest_still_count(monkeypatch):
    # A bug-shaped exception while planning one cluster fails the run, but the other clusters' deletes are
    # still done and reported, not lost.
    import pipeline_lib.cluster_log_prune as m
    real = m._plan_cluster

    def flaky(path, *args):
        if path.endswith("-c2"):
            raise KeyError("bug")
        return real(path, *args)

    monkeypatch.setattr(m, "_plan_cluster", flaky)
    tree = {f"0{i}01-000000-c{i}": _terminated(OLD) for i in (1, 2, 3)}
    rm, lines = _Rm(), []
    with pytest.raises(RuntimeError, match=r"FAILED \(1 failure") as err:
        run_prune(ROOT, 30, CUTOFF, False, _fs(tree), rm, log=lines.append, workers=2)
    pruned = {p[len(ROOT) + 1:].split("/")[0] for p, _ in rm.calls}
    assert pruned == {"0101-000000-c1", "0301-000000-c3"}
    assert "deleted_files=8" in str(err.value)   # the other two clusters' deletes are in the summary
    assert any("KeyError" in l and "-c2" in l for l in lines)


def test_an_unexpected_triage_error_is_that_clusters_failure(monkeypatch):
    import pipeline_lib.cluster_log_prune as m
    real = m.triage_cluster

    def flaky(path, entry, ls, cutoff):
        if path.endswith("-c2"):
            raise KeyError("bug")
        return real(path, entry, ls, cutoff)

    monkeypatch.setattr(m, "triage_cluster", flaky)
    tree = {f"0{i}01-000000-c{i}": _terminated(OLD) for i in (1, 2, 3)}
    rm, lines = _Rm(), []
    with pytest.raises(RuntimeError, match=r"FAILED \(1 failure"):
        run_prune(ROOT, 30, CUTOFF, False, _fs(tree), rm, log=lines.append, workers=2)
    pruned = {p[len(ROOT) + 1:].split("/")[0] for p, _ in rm.calls}
    assert pruned == {"0101-000000-c1", "0301-000000-c3"}   # the other two were still pruned
    assert any("KeyError" in l and "-c2" in l for l in lines)


@pytest.mark.parametrize("today,expected", [
    (1231, ["0101-000000-a", "0601-000000-b", "1201-000000-c"]),   # all this year: plain order
    (115, ["0601-000000-b", "1201-000000-c", "0101-000000-a"]),    # mid January: June and December are last year
    (601, ["1201-000000-c", "0101-000000-a", "0601-000000-b"]),    # June 1: December is last year; today is this year
])
def test_oldest_first_key_across_new_year(today, expected):
    names = ["0101-000000-a", "0601-000000-b", "1201-000000-c"]
    assert sorted(names, key=lambda n: _oldest_first_key(f"{ROOT}/{n}", today)) == expected


@pytest.mark.parametrize("skew_ms,expected_spent", [(-1000, 0), (-299_000, 0), (0, 0), (5000, 5)])
def test_small_clock_skew_counts_from_the_run_start(skew_ms, expected_spent):
    now = 1_800_000_000_000
    spent, note = spent_before_seconds(str(now - skew_ms), now)
    assert spent == expected_spent and "counts from the run start" in note


def test_large_negative_skew_is_still_implausible():
    now = 1_800_000_000_000
    spent, note = spent_before_seconds(str(now + 301_000), now)
    assert spent == 0 and "implausible" in note

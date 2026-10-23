"""Offline unit tests for pipeline_lib.cluster_log_prune: the allow-listed delete plan the `_cluster logs prune`
job executes. A fake directory tree stands in for dbutils.fs.ls."""
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
    def ls(path):
        return [SimpleNamespace(name="cluster_logs", size=5, modificationTime=OLD)]
    plan = plan_prune(ROOT, ls, CUTOFF)
    assert plan.list_errors == [(ROOT, "the path is a file, not a directory")] and plan.skipped == []


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

"""Plan which delivered cluster log files the `_cluster logs prune` job deletes.

Job clusters that opt in deliver their logs to `${var.cluster_log_volume_path}` (a UC Volume) in a fixed
layout: one directory per cluster id, holding the log kinds Databricks writes. Nothing deletes them, so
the prune job removes files older than a retention window.

The danger is the path: it is a free-form bundle variable, so it can point at a volume root that also
holds checkpoints, the connector wheel, or anything else. A plain "delete everything older than N days
under the path" would then delete a stream's checkpoint. So this module is an ALLOW-LIST over the
delivery layout: it only ever plans to delete files inside `<root>/<cluster id>/<log kind>/...`, where
the cluster id matches Databricks' cluster id format and the log kind is one Databricks writes. Every
other entry is reported as skipped and never touched. A file whose age cannot be read is kept.

Pure and dependency-injected (the listing function is passed in, with dbutils.fs.ls semantics) so the
classification is unit-tested off-cluster; the notebook performs the deletes.
"""
import re
from typing import NamedTuple

from pipeline_lib.config import PipelineConfigError

# Databricks cluster ids, e.g. 1023-200712-abcd1234: MMDD-HHMMSS-<lowercase alphanumeric>.
CLUSTER_ID = re.compile(r"^\d{4}-\d{6}-[a-z0-9]+$")

# The log kinds Databricks delivers under each cluster id directory.
LOG_KINDS = ("driver", "executor", "eventlog", "init_scripts")

_MS_PER_DAY = 86_400_000


class PrunePlan(NamedTuple):
    delete_files: list    # (path, size) of files older than the cutoff, inside the allow-listed layout
    delete_dirs: list     # directories that are empty once those files go, deepest first
    kept_files: int       # files inside the layout that are newer than the cutoff (or of unknown age)
    kept_bytes: int
    skipped: list         # (path, reason) of entries outside the layout, never touched
    list_errors: list     # (path, error) of directories that could not be listed (left alone)
    root_missing: bool = False  # the root itself does not exist yet (no cluster has delivered logs)


# How a listing reports a path that does not exist: Python's FileNotFoundError, or the Java
# FileNotFoundException that dbutils.fs.ls surfaces inside its error text. Only these mean "absent"; any
# other listing error is a real failure.
_NOT_FOUND_MARKERS = ("FileNotFoundException", "No such file or directory")


def _is_not_found(exc: Exception) -> bool:
    return isinstance(exc, FileNotFoundError) or any(m in str(exc) for m in _NOT_FOUND_MARKERS)


def validate_log_root(path: object) -> str:
    """The cluster log root, normalized (no trailing slash). "" when unset (the feature is opt-in).

    Fails closed on anything that is not a plain UC Volume path at least as deep as a volume
    (`/Volumes/<catalog>/<schema>/<volume>`), or that has empty, `.` or `..` components."""
    if path is None:
        return ""
    if not isinstance(path, str):
        raise PipelineConfigError(f"cluster_log_volume_path must be a string, got {type(path).__name__}")
    path = path.strip()
    if not path:
        return ""
    trimmed = path.rstrip("/")
    parts = trimmed.split("/")
    # parts[0] is "" (leading slash); then Volumes, catalog, schema, volume, ...
    if not trimmed.startswith("/Volumes/") or len(parts) < 5 or any(p in ("", ".", "..") for p in parts[1:]):
        raise PipelineConfigError(
            f"cluster_log_volume_path must be a UC Volume path like /Volumes/<catalog>/<schema>/<volume>[/...], "
            f"got {path!r}"
        )
    return trimmed


def cutoff_ms(now_ms: int, retention_days: int) -> int | None:
    """Files modified before this epoch-ms are deleted. None when retention is disabled (days <= 0).

    Fails closed on a retention so large the cutoff falls at or before the epoch: no file could ever be
    older, so the job would delete nothing while reporting success."""
    if retention_days <= 0:
        return None
    cutoff = now_ms - retention_days * _MS_PER_DAY
    if cutoff <= 0:
        raise PipelineConfigError(
            f"cluster_log_retention_days={retention_days} puts the cutoff before 1970, so nothing could ever "
            f"be deleted; use a realistic number of days, or 0 to disable the delete"
        )
    return cutoff


def _is_dir(entry) -> bool:
    return entry.name.endswith("/")


def _name(entry) -> str:
    return entry.name.rstrip("/")


def _join(parent: str, entry) -> str:
    return f"{parent}/{_name(entry)}"


def _old(entry, cutoff: int) -> bool:
    """A positive integer modificationTime below the cutoff. Anything else (0, missing, not an int) is not old."""
    mtime = getattr(entry, "modificationTime", None)
    return isinstance(mtime, int) and not isinstance(mtime, bool) and 0 < mtime < cutoff


def _parent(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[0]


def plan_prune(root: str, ls, cutoff: int) -> PrunePlan:
    """Walk `root` and classify every entry. `ls(path)` returns entries with `.name` (a directory's name
    ends in '/'), `.size` and `.modificationTime` (epoch ms), and raises if the path can't be listed.

    Only files under `<root>/<cluster id>/<log kind>/` are candidates; one is deleted when its
    modificationTime is a positive number below `cutoff`, otherwise kept. Directories, per cluster:
    - A FINISHED cluster (nothing kept, skipped or unreadable anywhere under it, and either an old file
      under it or its own timestamp old) is removed whole, empty never-used directories included: UC
      Volumes report directory timestamps as 0, so an empty directory's own age can't be read.
    - Otherwise (a cluster still delivering) only directories that old-file deletes leave empty go (or
      whose own timestamp is old), so a running cluster's fresh empty directories stay.
    The root itself is never removed. A root that doesn't exist yet is an empty plan with root_missing set,
    but only when it is deeper than the volume and the parent's listing positively shows it absent."""
    delete_files, delete_dirs, skipped, list_errors = [], [], [], []
    kept = {"files": 0, "bytes": 0}

    def walk_log_tree(path: str) -> tuple:
        """Classify everything under a log directory. Returns (clean, has_old, dirs): clean when nothing under
        it is kept or unreadable, has_old when an old file under it is being deleted, and dirs the
        descendant directories post-order as (path, clean, has_old, own_old)."""
        try:
            entries = list(ls(path))
        except Exception as exc:  # noqa: BLE001 - recorded; the subtree is left alone
            list_errors.append((path, f"{type(exc).__name__}: {exc}"))
            return False, False, []
        clean, has_old, dirs = True, False, []
        for e in entries:
            child = _join(path, e)
            if _is_dir(e):
                c_clean, c_old, c_dirs = walk_log_tree(child)
                dirs += c_dirs + [(child, c_clean, c_old, _old(e, cutoff))]
                clean, has_old = clean and c_clean, has_old or c_old
                continue
            size = getattr(e, "size", 0) or 0
            if _old(e, cutoff):
                delete_files.append((child, size))
                has_old = True
            else:
                kept["files"] += 1
                kept["bytes"] += size
                clean = False
        return clean, has_old, dirs

    try:
        top = list(ls(root))
    except Exception as exc:  # noqa: BLE001
        # A volume can't be created by log delivery, so a missing volume is a misconfiguration, not "no logs yet".
        deeper_than_volume = len(root.rstrip("/").split("/")) > 5
        volume = "/".join(root.rstrip("/").split("/")[:5])
        if deeper_than_volume and _is_not_found(exc) and _absent_from_parent(root, ls, floor=volume):
            # Configured but not created yet: no cluster has delivered logs, so there is nothing to prune.
            return PrunePlan([], [], 0, 0, [], [], root_missing=True)
        list_errors.append((root, f"{type(exc).__name__}: {exc}"))
        top = []
    for e in top:
        path = _join(root, e)
        if not (_is_dir(e) and CLUSTER_ID.match(_name(e))):
            skipped.append((path, "not a cluster id directory"))
            continue
        try:
            kinds = list(ls(path))
        except Exception as exc:  # noqa: BLE001
            list_errors.append((path, f"{type(exc).__name__}: {exc}"))
            continue
        cluster_clean, cluster_old, cluster_dirs = True, False, []
        for k in kinds:
            kpath = _join(path, k)
            if not (_is_dir(k) and _name(k) in LOG_KINDS):
                skipped.append((kpath, "not a known log kind"))
                cluster_clean = False
                continue
            k_clean, k_old, k_dirs = walk_log_tree(kpath)
            cluster_dirs += k_dirs + [(kpath, k_clean, k_old, _old(k, cutoff))]
            cluster_clean, cluster_old = cluster_clean and k_clean, cluster_old or k_old
        if cluster_clean and (cluster_old or _old(e, cutoff)):
            delete_dirs += [d for d, *_ in cluster_dirs] + [path]
        else:
            # Post-order, so each directory's children are decided first; one is removed only when it is
            # clean, has age evidence, and every child directory is being removed too (a kept child means it
            # won't be empty).
            children = {}
            for c, *_ in cluster_dirs:
                children.setdefault(_parent(c), []).append(c)
            removable = set()
            for d, d_clean, d_old, d_own_old in cluster_dirs:
                if d_clean and (d_old or d_own_old) and all(c in removable for c in children.get(d, ())):
                    removable.add(d)
                    delete_dirs.append(d)
    return PrunePlan(delete_files, delete_dirs, kept["files"], kept["bytes"], skipped, list_errors)


def _absent_from_parent(path: str, ls, floor: str = "") -> bool:
    """True only when the nearest ancestor that lists does not contain the next component toward `path`:
    positive evidence it doesn't exist, so a not-found message from some other cause can't pass for it.
    Climbs past ancestors that are themselves positively not found (a path missing several levels deep),
    but never above `floor` (default: just the parent). Any other listing error => not proven absent."""
    child = path.rstrip("/")
    ancestor = _parent(child)
    while True:
        try:
            listing = list(ls(ancestor))
        except Exception as exc:  # noqa: BLE001
            if not _is_not_found(exc) or not floor or ancestor == floor or len(ancestor) <= len(floor):
                return False
            child, ancestor = ancestor, _parent(ancestor)
            continue
        name = child.rsplit("/", 1)[-1]
        return not any(_name(e) == name for e in listing)


def parse_retention_days(raw: object) -> int:
    """cluster_log_retention_days as an int. Fails closed on anything that isn't a plain integer."""
    text = raw.strip() if isinstance(raw, str) else raw
    if isinstance(text, bool) or text in (None, ""):
        raise PipelineConfigError(f"cluster_log_retention_days must be an integer, got {raw!r}")
    try:
        return int(text)
    except (TypeError, ValueError):
        raise PipelineConfigError(
            f"cluster_log_retention_days must be an integer, got {raw!r} (0 disables the delete)"
        ) from None


def execute_plan(plan: PrunePlan, rm, dry_run: bool, ls=None) -> tuple:
    """Carry out `plan` with `rm(path, recurse=False)` (dbutils.fs.rm, which returns True on success).
    Returns (deleted_files, deleted_dirs, raced_dirs, failures), failures a list of messages.

    - dry_run: no rm call at all; the counts are 0 and failures only carry the plan's listing errors.
    - Files first, then directories in plan order (deepest first). Every rm is non-recursive, so a
      directory that gained a file since the listing fails to delete instead of taking the new log with it.
    - A directory whose remove fails but which `ls` now shows non-empty is a race with a fresh delivery,
      not a failure: it is counted in raced_dirs and kept (so a live cluster doesn't page support).
    - An rm that returns False deleted nothing, so it is a failure, not a success, unless `ls` shows the
      file or directory positively gone (the parent lists without it), which counts as removed.
    - Every item is tried even after a failure; failures are collected for the caller to raise on.
    - A directory above a file or directory that failed (or raced) is skipped (it can't be empty), so one
      problem is reported once rather than once per ancestor directory.
    """
    failures = [f"list {p}: {e}" for p, e in plan.list_errors]
    if dry_run:
        return 0, 0, 0, failures
    deleted_files = deleted_dirs = raced_dirs = 0
    blocked = set()  # every ancestor of a path that is still there: those directories can't be empty

    def _block(path):
        p = _parent(path)
        while p and p not in blocked:
            blocked.add(p)
            p = _parent(p) if "/" in p else ""

    def _rm(path):
        if rm(path, recurse=False) is False:
            raise OSError("rm returned False (nothing deleted)")

    for path, _ in plan.delete_files:
        try:
            _rm(path)
            deleted_files += 1
        except Exception as exc:  # noqa: BLE001 - collected; keep going
            # Already gone (e.g. a concurrent cleanup), on positive evidence: the parent lists without it.
            if ls is not None and _absent_from_parent(path, ls):
                deleted_files += 1
                continue
            failures.append(f"delete {path}: {type(exc).__name__}: {exc}")
            _block(path)
    for path in plan.delete_dirs:
        if path in blocked:
            continue
        try:
            _rm(path)
            deleted_dirs += 1
        except Exception as exc:  # noqa: BLE001
            gone = raced = False
            try:
                raced = ls is not None and len(list(ls(path))) > 0
            except Exception as list_exc:  # noqa: BLE001 - can't tell unless it's positively gone
                # Gone only on positive evidence: the not-found text AND the parent listing without it.
                gone = _is_not_found(list_exc) and _absent_from_parent(path, ls)
            if gone:
                deleted_dirs += 1   # removed after all (e.g. by a concurrent run): nothing left to fail on
                continue
            _block(path)
            if raced:
                raced_dirs += 1
            else:
                failures.append(f"remove dir {path}: {type(exc).__name__}: {exc}")
    return deleted_files, deleted_dirs, raced_dirs, failures

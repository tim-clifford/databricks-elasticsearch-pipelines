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
    """Files modified before this epoch-ms are deleted. None when retention is disabled (days <= 0)."""
    if retention_days <= 0:
        return None
    return now_ms - retention_days * _MS_PER_DAY


def _is_dir(entry) -> bool:
    return entry.name.endswith("/")


def _name(entry) -> str:
    return entry.name.rstrip("/")


def _join(parent: str, entry) -> str:
    return f"{parent}/{_name(entry)}"


def plan_prune(root: str, ls, cutoff: int) -> PrunePlan:
    """Walk `root` and classify every entry. `ls(path)` returns entries with `.name` (a directory's name
    ends in '/'), `.size` and `.modificationTime` (epoch ms), and raises if the path can't be listed.

    Only files under `<root>/<cluster id>/<log kind>/` are candidates; one is deleted when its
    modificationTime is a positive number below `cutoff`, otherwise kept. A directory is removed only
    when everything under it is being deleted (so a directory that still holds a kept or skipped entry,
    or failed to list, stays)."""
    delete_files, delete_dirs, skipped, list_errors = [], [], [], []
    kept = {"files": 0, "bytes": 0}

    def walk_log_tree(path: str) -> bool:
        """Classify everything under an allow-listed log directory. True if it ends up empty."""
        try:
            entries = list(ls(path))
        except Exception as exc:  # noqa: BLE001 - recorded; the subtree is left alone
            list_errors.append((path, f"{type(exc).__name__}: {exc}"))
            return False
        empty = True
        for e in entries:
            child = _join(path, e)
            if _is_dir(e):
                if walk_log_tree(child):
                    delete_dirs.append(child)
                else:
                    empty = False
                continue
            mtime = getattr(e, "modificationTime", None)
            size = getattr(e, "size", 0) or 0
            if isinstance(mtime, int) and not isinstance(mtime, bool) and 0 < mtime < cutoff:
                delete_files.append((child, size))
            else:
                kept["files"] += 1
                kept["bytes"] += size
                empty = False
        return empty

    try:
        top = list(ls(root))
    except Exception as exc:  # noqa: BLE001
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
        cluster_empty = True
        for k in kinds:
            kpath = _join(path, k)
            if not (_is_dir(k) and _name(k) in LOG_KINDS):
                skipped.append((kpath, "not a known log kind"))
                cluster_empty = False
                continue
            if walk_log_tree(kpath):
                delete_dirs.append(kpath)
            else:
                cluster_empty = False
        if cluster_empty:
            delete_dirs.append(path)
    return PrunePlan(delete_files, delete_dirs, kept["files"], kept["bytes"], skipped, list_errors)

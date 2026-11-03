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
import os
import re
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from types import SimpleNamespace
from typing import NamedTuple

from pipeline_lib.config import PipelineConfigError

# Databricks cluster ids, e.g. 1023-200712-abcd1234: MMDD-HHMMSS-<lowercase alphanumeric>.
CLUSTER_ID = re.compile(r"[0-9]{4}-[0-9]{6}-[a-z0-9]+")   # ASCII only; always matched with fullmatch

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


def _walk_log_tree(path: str, ls, cutoff: int, acc: dict) -> tuple:
    """Classify everything under a log directory into `acc` (delete_files, kept counts, list_errors).
    Returns (clean, has_old, dirs): clean when nothing under it is kept or unreadable, has_old when an old
    file under it is being deleted, and dirs the descendant directories post-order as
    (path, clean, has_old, own_old)."""
    try:
        entries = list(ls(path))
    except Exception as exc:  # noqa: BLE001 - recorded; the subtree is left alone
        acc["list_errors"].append((path, f"{type(exc).__name__}: {exc}"))
        return False, False, []
    clean, has_old, dirs = True, False, []
    for e in entries:
        child = _join(path, e)
        if _is_dir(e):
            c_clean, c_old, c_dirs = _walk_log_tree(child, ls, cutoff, acc)
            dirs += c_dirs + [(child, c_clean, c_old, _old(e, cutoff))]
            clean, has_old = clean and c_clean, has_old or c_old
            continue
        size = getattr(e, "size", 0) or 0
        if _old(e, cutoff):
            acc["delete_files"].append((child, size))
            has_old = True
        else:
            acc["kept_files"] += 1
            acc["kept_bytes"] += size
            clean = False
    return clean, has_old, dirs


def _list_root(root: str, ls) -> tuple:
    """List the prune root. Returns (clusters, skipped, list_errors, root_missing): clusters the
    (path, entry) of every cluster id directory directly under it, in listing order; everything else at the
    top level is skipped. A root that doesn't exist yet is root_missing, but only when it is deeper than
    the volume and the parent's listing positively shows it absent; any other listing problem is an error."""
    skipped, list_errors = [], []
    try:
        top = list(ls(root))
    except Exception as exc:  # noqa: BLE001
        # A volume can't be created by log delivery, so a missing volume is a misconfiguration, not "no logs yet".
        deeper_than_volume = len(root.rstrip("/").split("/")) > 5
        volume = "/".join(root.rstrip("/").split("/")[:5])
        if deeper_than_volume and _is_not_found(exc) and _absent_from_parent(root, ls, floor=volume):
            # Configured but not created yet: no cluster has delivered logs, so there is nothing to prune.
            return [], [], [], True
        list_errors.append((root, f"{type(exc).__name__}: {exc}"))
        top = []
    # Listing a FILE returns just that file: a log path that points at a file is a misconfiguration, not an
    # empty volume, so fail instead of reporting a green run that pruned nothing.
    # A single same-named file is ambiguous (a directory can hold a file named like itself), so confirm from the
    # parent's listing that the root itself is a file.
    root_name = root.rstrip("/").rsplit("/", 1)[-1]
    if len(top) == 1 and not _is_dir(top[0]) and _name(top[0]) == root_name and _listed_as_file(root, ls):
        list_errors.append((root, "the path is a file, not a directory"))
        top = []
    clusters = []
    for e in top:
        path = _join(root, e)
        if not (_is_dir(e) and CLUSTER_ID.fullmatch(_name(e))):
            skipped.append((path, "not a cluster id directory"))
            continue
        clusters.append((path, e))
    return clusters, skipped, list_errors, False


def _unknown_kinds(path: str, kinds) -> list:
    """(path, reason) of every entry directly under a cluster directory that isn't a known log kind directory."""
    return [(_join(path, k), "not a known log kind") for k in kinds if not (_is_dir(k) and _name(k) in LOG_KINDS)]


def _plan_cluster(path: str, entry, kinds, ls, cutoff: int) -> PrunePlan:
    """The plan for one cluster directory, given its listing `kinds` (see plan_prune for the rules)."""
    acc = {"delete_files": [], "kept_files": 0, "kept_bytes": 0, "list_errors": []}
    delete_dirs = []
    skipped = _unknown_kinds(path, kinds)
    cluster_clean, cluster_old, cluster_dirs = not skipped, False, []
    for k in kinds:
        if not (_is_dir(k) and _name(k) in LOG_KINDS):
            continue
        kpath = _join(path, k)
        k_clean, k_old, k_dirs = _walk_log_tree(kpath, ls, cutoff, acc)
        cluster_dirs += k_dirs + [(kpath, k_clean, k_old, _old(k, cutoff))]
        cluster_clean, cluster_old = cluster_clean and k_clean, cluster_old or k_old
    # eventlog/ is delivered only at termination, so a clean cluster holding it has terminated. That covers
    # the one case with no age evidence at all: a file-less tree left by an earlier run that stopped partway.
    terminated = any(_is_dir(k) and _name(k) == "eventlog" for k in kinds)
    if cluster_clean and (cluster_old or _old(entry, cutoff) or terminated):
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
    return PrunePlan(acc["delete_files"], delete_dirs, acc["kept_files"], acc["kept_bytes"], skipped,
                     acc["list_errors"])


def _merge(plans, skipped=(), list_errors=(), root_missing=False) -> PrunePlan:
    """One PrunePlan from per-cluster plans (in the given order) plus the root-level skipped and errors."""
    files, dirs, kept_files, kept_bytes, skips, errors = [], [], 0, 0, list(skipped), list(list_errors)
    for p in plans:
        files += p.delete_files
        dirs += p.delete_dirs
        kept_files += p.kept_files
        kept_bytes += p.kept_bytes
        skips += p.skipped
        errors += p.list_errors
    return PrunePlan(files, dirs, kept_files, kept_bytes, skips, errors, root_missing)


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
    but only when it is deeper than the volume and the parent's listing positively shows it absent.

    This is the full, serial walk of every cluster. run_prune uses the same per-cluster rules but first
    triages each cluster cheaply (see triage_cluster) and walks only the ones that can hold old files."""
    clusters, skipped, list_errors, root_missing = _list_root(root, ls)
    if root_missing:
        return PrunePlan([], [], 0, 0, [], [], root_missing=True)
    plans = []
    for path, e in clusters:
        try:
            kinds = list(ls(path))
        except Exception as exc:  # noqa: BLE001
            list_errors.append((path, f"{type(exc).__name__}: {exc}"))
            continue
        plans.append(_plan_cluster(path, e, kinds, ls, cutoff))
    return _merge(plans, skipped, list_errors)


def _listed_as_file(path: str, ls) -> bool:
    """True when the parent lists `path` as a file (no trailing '/'). Unreadable parent => False."""
    name = path.rstrip("/").rsplit("/", 1)[-1]
    try:
        return any(e.name == name for e in ls(_parent(path)))
    except Exception:  # noqa: BLE001
        return False


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
    """cluster_log_retention_days as an int. Fails closed on anything that isn't a plain integer: an int (not a
    bool or float), or a string of optional sign plus digits (no underscores, decimals or units)."""
    if isinstance(raw, int) and not isinstance(raw, bool):
        return raw
    if isinstance(raw, str) and re.fullmatch(r"[+-]?[0-9]+", raw.strip()):
        return int(raw.strip())
    raise PipelineConfigError(
        f"cluster_log_retention_days must be an integer, got {raw!r} (0 disables the delete)"
    )


# Thread pool bounds for the prune. Every listing and delete is one round trip to the Volume, so the run is
# latency-bound and threads overlap the waits; 64 is far past where the measured speedup flattens.
MAX_WORKERS = 64
# The run's time budget must end below the job's 240-minute timeout, so the budget (a green, resumable
# stop) always comes before the timeout (a failed run with no summary).
MAX_MINUTES_LIMIT = 230


def _parse_bounded_int(raw: object, name: str, low: int, high: int) -> int:
    """A plain integer (an int that isn't a bool, or a string of digits) in [low, high]. Fails closed otherwise."""
    value = None
    if isinstance(raw, int) and not isinstance(raw, bool):
        value = raw
    elif isinstance(raw, str) and re.fullmatch(r"[0-9]+", raw.strip()):
        value = int(raw.strip())
    if value is None or not low <= value <= high:
        raise PipelineConfigError(f"{name} must be an integer from {low} to {high}, got {raw!r}")
    return value


def parse_workers(raw: object) -> int:
    """cluster_log_prune_workers: threads listing and deleting in parallel, 1 to MAX_WORKERS."""
    return _parse_bounded_int(raw, "workers (cluster_log_prune_workers)", 1, MAX_WORKERS)


def parse_max_minutes(raw: object) -> int:
    """cluster_log_prune_max_minutes: the run's time budget, 1 to MAX_MINUTES_LIMIT."""
    return _parse_bounded_int(raw, "max_minutes (cluster_log_prune_max_minutes)", 1, MAX_MINUTES_LIMIT)


# How far the driver's clock may lag the Jobs service's before a run start "in the future" is implausible.
_CLOCK_SKEW_TOLERANCE_S = 300


def spent_before_seconds(job_start_ms: object, now_ms: int) -> tuple:
    """Seconds this job run had already used before this attempt started, and a note for the run output.

    The budget is for the whole job run, not one attempt: the job's timeout_seconds bounds every attempt
    together, so a failed attempt that is retried must not hand the retry a fresh budget that the timeout
    would then cut short (ending TIMEDOUT instead of at a clean budget stop). The job passes its run's start
    time as `{{job.start_time.timestamp_ms}}`. A value that isn't a plain positive integer (an interactive
    run, or the reference not resolved) or that is implausible (in the future, or over a day old: no run of
    a 4-hour-timeout job is) falls back to 0, counting the budget from this attempt, with a note saying so."""
    raw = job_start_ms.strip() if isinstance(job_start_ms, str) else ""
    if not re.fullmatch(r"[0-9]+", raw):   # "0" passes here and is rejected below as over a day old
        return 0, f"job run start unknown ({job_start_ms!r}): the budget counts from this attempt"
    spent = (now_ms - int(raw)) / 1000
    if -_CLOCK_SKEW_TOLERANCE_S <= spent < 0:
        # The driver's clock a little behind the Jobs service's: the run started just now.
        return 0, (f"the job run started {spent:.0f} s before this attempt (clock skew); the budget counts "
                   f"from the run start")
    if spent < 0 or spent > 86_400:
        return 0, f"job run start {raw} is implausible ({spent:.0f} s ago): the budget counts from this attempt"
    return spent, f"the job run started {spent:.0f} s before this attempt; the budget counts from the run start"


class ClusterTriage(NamedTuple):
    path: str
    entry: object
    kinds: list         # the cluster directory's listing ([] when it couldn't be listed)
    young: bool         # kept without a walk: terminated, and every driver log is inside retention
    skipped: list       # unknown entries under a young cluster (a walked cluster reports its own)
    list_errors: list   # the cluster directory couldn't be listed


def _all_files_inside_retention(entries, cutoff: int) -> bool:
    """True when the listing holds at least one file and every file's modificationTime is a positive integer
    at or past the cutoff. A file of unknown age, or none at all, is not evidence of youth."""
    times = [getattr(e, "modificationTime", None) for e in entries if not _is_dir(e)]
    return bool(times) and all(isinstance(t, int) and not isinstance(t, bool) and t >= cutoff for t in times)


def triage_cluster(path: str, entry, ls, cutoff: int) -> ClusterTriage:
    """Decide with one or two listings whether a cluster needs the full walk.

    A cluster is YOUNG, and kept without walking it, only when it has terminated (it holds eventlog/, which
    Databricks delivers at termination) and EVERY file directly in its driver/ is at or past the cutoff.
    Those files keep the cluster from being removed whole, so skipping the walk can only keep files, never
    delete one: older executor files in a young cluster stay until its driver logs age out (a job cluster's
    logs then go together). Everything else (still running, no driver/, an unreadable or empty driver/, or
    any driver log past retention) is walked with the full per-file rules of plan_prune. So a running
    cluster, or one restarted after a termination (it keeps its old eventlog/ but its old rolled driver
    logs are past retention), is walked and its old logs are still deleted. One boundary: a cluster
    restarted after a termination whose old driver logs have already been pruned looks young again while it
    runs, so its old executor logs wait until it stops. The framework delivers logs only from job clusters,
    which never restart; telling a restarted cluster apart would mean listing eventlog/'s nested
    directories for every cluster.

    A cluster triage can't list is sent to the walk, never recorded as a failure here: the walk's own
    listing is the one of record."""
    try:
        kinds = list(ls(path))
    except Exception:  # noqa: BLE001 - not young: the walk lists it again and records an error if that fails
        # Triage's listing is never the one of record (it may go through the FUSE mount, whose view can be
        # stale): a cluster it can't list goes to the walk, which lists it through ls and fails only then.
        return ClusterTriage(path, entry, [], False, [], [])
    dirs = {_name(k) for k in kinds if _is_dir(k)}
    if "eventlog" in dirs and "driver" in dirs:
        try:
            driver = list(ls(f"{path}/driver"))
        except Exception:  # noqa: BLE001 - not young; the walk lists driver/ again and records the error
            driver = None
        # Young only when EVERY driver file is inside retention. eventlog/ proves the cluster terminated once,
        # not that it is still down: a restarted cluster keeps its old eventlog/ and writes fresh driver logs,
        # and its old rolled driver logs (older than the cutoff) send it to the walk, so they still go.
        if driver is not None and _all_files_inside_retention(driver, cutoff):
            return ClusterTriage(path, entry, kinds, True, _unknown_kinds(path, kinds), [])
    return ClusterTriage(path, entry, kinds, False, [], [])


def _today_mmdd() -> int:
    t = time.gmtime()
    return t.tm_mon * 100 + t.tm_mday


def _oldest_first_key(path: str, today_mmdd: int) -> tuple:
    """Sort key for the prune order: a cluster id's MMDD-HHMMSS prefix, with a month-day after today's
    counted as last year's (the id has no year), so December's clusters still come before January's after New
    Year. Ordering only: it never decides a delete."""
    name = path.rstrip("/").rsplit("/", 1)[-1]
    mmdd = int(name[:4]) if name[:4].isdigit() else 0
    return (0 if mmdd > today_mmdd else 1, name)


def volume_ls(path: str) -> list:
    """List a UC Volume directory through the /Volumes FUSE mount (os.scandir), with dbutils.fs.ls entry
    semantics: `.name` (a directory's ends in '/'), `.size`, `.modificationTime` (epoch ms). Measured on a
    serverless notebook (2026-11-03) at 21.5 ms per call against 270 ms for dbutils.fs.ls, with identical
    names, sizes and file times. Used for triage only: FUSE can show a stale view of changes made through
    dbutils (seen live after deletes). A stale view never causes a delete: it can send a cluster to the
    walk (which lists it again through dbutils), or, if it misses a fresh driver file, keep a cluster for
    one more run, which only delays a delete. Walks and deletes go through dbutils.fs. A directory's time is reported as 0, as the prune
    already assumes for directories."""
    out = []
    with os.scandir(path) as it:
        for e in it:
            try:
                is_dir = e.is_dir(follow_symlinks=False)
                st = e.stat(follow_symlinks=False)
            except FileNotFoundError:
                # Removed or renamed after scandir yielded it (rotation, a concurrent prune). Skipping it can only
                # make triage send the cluster to the walk, which lists it again through dbutils.
                continue
            out.append(SimpleNamespace(name=e.name + ("/" if is_dir else ""), size=0 if is_dir else st.st_size,
                                       modificationTime=0 if is_dir else int(st.st_mtime * 1000)))
    return out


class _DeadlineReached(BaseException):
    """Raised by a planning listing once the deadline has passed. A BaseException so the walk's own
    per-directory error handling (which records an Exception as a listing error) lets it through."""


class ItemError(NamedTuple):
    """An unexpected exception from one item of _bounded_map, returned in place of its result."""
    exc: Exception


def _bounded_map(items, fn, workers: int, stop, tick, poll: float = 1.0) -> tuple:
    """Run fn over items on `workers` threads with at most 2 * workers in flight. `stop()` is checked
    before each item is queued and again when it actually starts; once it is true nothing new starts and the
    items already running finish. Returns (results, started): results in item order (None for an item never
    started) and how many items ran. `tick(completed)` is
    called at least every `poll` seconds while items are in flight. An exception in fn doesn't stop the
    map: it is returned in that item's slot as an ItemError, so the caller can record it as a failure and
    still report every other item's result."""
    results, pending, submitted, completed = [None] * len(items), {}, 0, 0
    skipped = object()

    def run(item):
        # Checked again when the item actually starts: one queued behind busy threads must not start after the
        # deadline (measured live: without this, a run overshot a 4-minute budget by about 45 s).
        if stop():
            return skipped
        try:
            return fn(item)
        except Exception as exc:  # noqa: BLE001 - returned for the caller to record; the run still fails
            return ItemError(exc)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        def fill():
            nonlocal submitted
            while submitted < len(items) and len(pending) < 2 * workers and not stop():
                pending[ex.submit(run, items[submitted])] = submitted
                submitted += 1
        fill()
        while pending:
            done, _ = wait(pending, timeout=poll, return_when=FIRST_COMPLETED)
            for f in done:
                result = f.result()
                index = pending.pop(f)
                if result is not skipped:
                    results[index] = result
                    completed += 1
            tick(completed)
            fill()
    return results, completed


def execute_plan(plan: PrunePlan, rm, dry_run: bool, ls=None, root: str = "") -> tuple:
    """execute_plan_until with no deadline: returns (deleted_files, deleted_dirs, raced_dirs, failures)."""
    return execute_plan_until(plan, rm, dry_run, ls, root)[:4]


def execute_plan_until(plan: PrunePlan, rm, dry_run: bool, ls=None, root: str = "", stop=None) -> tuple:
    """Carry out `plan` with `rm(path, recurse=False)` (dbutils.fs.rm, which returns True on success).
    Returns (deleted_files, deleted_dirs, raced_dirs, failures, stopped), failures a list of messages.

    - stop (optional): checked before every rm. Once it is true nothing more is removed and `stopped` is
      True: the files already deleted were old, every directory is left in place (some may not be empty),
      and the next run walks the cluster again from a fresh listing. So a cluster with a long backlog can't
      carry the run past its deadline.

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
        return 0, 0, 0, failures, False
    deleted_files = deleted_dirs = raced_dirs = 0
    stopping = (lambda: False) if stop is None else stop
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
        if stopping():
            return deleted_files, deleted_dirs, raced_dirs, failures, True
        try:
            _rm(path)
            deleted_files += 1
        except Exception as exc:  # noqa: BLE001 - collected; keep going
            # Already gone (e.g. a concurrent cleanup), on positive evidence: the parent lists without it.
            # The climb stops at `root` (the prune root), so a parent removed concurrently still counts.
            if ls is not None and _absent_from_parent(path, ls, floor=root):
                deleted_files += 1
                continue
            failures.append(f"delete {path}: {type(exc).__name__}: {exc}")
            _block(path)
    for path in plan.delete_dirs:
        if path in blocked:
            continue
        if stopping():
            return deleted_files, deleted_dirs, raced_dirs, failures, True
        try:
            _rm(path)
            deleted_dirs += 1
        except Exception as exc:  # noqa: BLE001
            gone = raced = False
            # A race is only the "directory not empty" refusal (seen live as PathIsNotEmptyDirectoryException)
            # on a directory that now lists non-empty; any other remove error (permissions, I/O) is a failure.
            not_empty = ("NotEmpty" in type(exc).__name__ or "PathIsNotEmptyDirectory" in str(exc)
                         or "rm returned False" in str(exc))
            try:
                raced = ls is not None and len(list(ls(path))) > 0 and not_empty
            except Exception as list_exc:  # noqa: BLE001 - can't tell unless it's positively gone
                # Gone only on positive evidence: the not-found text AND the parent listing without it.
                gone = _is_not_found(list_exc) and _absent_from_parent(path, ls, floor=root)
            if gone:
                deleted_dirs += 1   # removed after all (e.g. by a concurrent run): nothing left to fail on
                continue
            _block(path)
            if raced:
                raced_dirs += 1
            else:
                failures.append(f"remove dir {path}: {type(exc).__name__}: {exc}")
    return deleted_files, deleted_dirs, raced_dirs, failures, False


def _print_capped(log, items, fmt, what, cap=200):
    for item in items[:cap]:
        log(fmt(item))
    if len(items) > cap:
        log(f"  ... and {len(items) - cap} more {what}")


def run_prune(root: str, retention_days: int, cutoff, dry_run: bool, ls, rm, log=print, *, workers: int = 1,
              max_minutes=None, spent_seconds: float = 0, triage_ls=None, clock=time.monotonic,
              progress_seconds: float = 60, poll: float = 1.0, today_mmdd: int | None = None) -> str:
    """The whole prune run: returns the summary line, or raises RuntimeError when anything failed.

    - root unset ("") or retention disabled (cutoff None): nothing to do; a skipped-run summary.
    - Otherwise, on `workers` threads:
      1. TRIAGE every cluster directory (triage_cluster: one or two listings each). A young cluster is kept
         without a walk. Triage must finish within `max_minutes`: if it can't, nothing is deleted and the
         run fails, because a run that can't even classify the clusters can't keep up.
      2. PRUNE the rest, oldest cluster id first (the id's MMDD-HHMMSS prefix; ordering only, it never
         decides a delete): walk each with the plan_prune rules and delete its old files and emptied
         directories (execute_plan, every rm non-recursive; no rm at all on a dry run). Once `max_minutes`
         has passed, no new cluster starts, and a cluster in flight stops before its next delete (it counts
         as remaining); the run then reports incomplete=True and remaining_clusters, and stays green: the
         next run carries on from a fresh listing. A cluster whose walk is still listing at the deadline is
         abandoned with nothing deleted, and counts as remaining too. The order treats a month-day after
         `today_mmdd` (default: today, UTC) as last year's, so it stays oldest first across New Year (a
         cluster id over a year old can still sort late: ordering only, never a delete decision).
    - An unexpected exception for one cluster (in triage or the prune) is recorded as that cluster's
      failure; every other cluster's result is still counted and reported, and the run then fails.
    - `spent_seconds` (see spent_before_seconds) is the part of the budget an earlier attempt of the same job
      run already used: the deadline is `max_minutes` after the RUN started, not after this attempt did.
    - `triage_ls` (default `ls`) does the triage listings only; the notebook passes volume_ls, which is much
      faster than dbutils.fs.ls. The root listing, every walk and every delete use `ls` and `rm`, and a
      walked cluster is listed again with `ls`, so triage_ls never decides a delete.
    - Progress is logged about every `progress_seconds`. Any listing or delete failure raises after every
      started item has been tried, so a partial prune never reports green.
    """
    if not root or cutoff is None:
        return (f"cluster_logs_prune path={root!r} retention_days={retention_days} skipped_run=True "
                f"reason={'path unset' if not root else 'retention disabled'}")
    start = clock()
    deadline = None if max_minutes is None else start + max_minutes * 60 - max(0, spent_seconds)
    over = (lambda: False) if deadline is None else (lambda: clock() >= deadline)
    if over():
        # An earlier attempt of this job run (or the original run, for a repair) already used the budget, so
        # there is nothing this attempt can do. Say so plainly, so this second failure email reads as expected.
        raise RuntimeError(
            f"cluster_logs_prune FAILED: the time budget (max_minutes={max_minutes}) was already used by an "
            f"earlier attempt of this job run ({int(spent_seconds)} s since the run started), so nothing was "
            f"listed or deleted. Expected right after an attempt that ran out of budget; start a new run.")
    last_progress = [start]

    def ticker(phase: str, total: int):
        def tick(completed: int):
            now = clock()
            if now - last_progress[0] >= progress_seconds:
                last_progress[0] = now
                log(f"  progress: {phase} {completed}/{total} cluster(s), {int(now - start)} s elapsed")
        return tick

    clusters, root_skipped, root_errors, root_missing = _list_root(root, ls)
    log(f"  {len(clusters)} cluster directory(ies) under the path; triaging on {workers} thread(s)")
    tls = ls if triage_ls is None else triage_ls
    triaged, started = _bounded_map(clusters, lambda c: triage_cluster(c[0], c[1], tls, cutoff), workers, over,
                                    ticker("triage", len(clusters)), poll)
    # Triage must FINISH within the budget, not just start: one that ran past it leaves no time to prune,
    # so the run would end green with nothing done.
    if started < len(clusters) or over():
        raise RuntimeError(
            f"cluster_logs_prune FAILED: triage classified {started} of {len(clusters)} cluster(s) and did not finish within "
            f"max_minutes={max_minutes} ({int(spent_seconds)} s of it used before this attempt), so nothing was "
            f"deleted; raise cluster_log_prune_workers or cluster_log_prune_max_minutes")
    # An unexpected triage error is that cluster's listing error: it is left alone and fails the run.
    triaged = [ClusterTriage(c[0], c[1], [], False, [], [(c[0], f"{type(t.exc).__name__}: {t.exc}")])
               if isinstance(t, ItemError) else t for c, t in zip(clusters, triaged)]
    young = [t for t in triaged if t.young]
    md = _today_mmdd() if today_mmdd is None else today_mmdd
    to_walk = sorted((t for t in triaged if not t.young and not t.list_errors),
                     key=lambda t: _oldest_first_key(t.path, md))
    log(f"  triage: {len(young)} cluster(s) kept without a walk (every driver log inside retention), "
        f"{len(to_walk)} to walk, {int(clock() - start)} s elapsed")

    def prune_one(t: ClusterTriage):
        try:
            kinds = list(ls(t.path))   # fresh, through the same ls the walk and deletes use
        except Exception as exc:  # noqa: BLE001 - recorded; the cluster is left alone
            error = (t.path, f"{type(exc).__name__}: {exc}")
            return PrunePlan([], [], 0, 0, [], [error]), (0, 0, 0, [f"list {error[0]}: {error[1]}"]), "failed"
        def planning_ls(path):
            if over():
                raise _DeadlineReached()
            return ls(path)

        try:
            plan = _plan_cluster(t.path, t.entry, kinds, planning_ls, cutoff)
        except _DeadlineReached:
            # Abandoned mid-walk at the deadline: nothing deleted from a partial plan; the cluster is remaining.
            return PrunePlan([], [], 0, 0, [], []), (0, 0, 0, []), True
        f_count, d_count, r_count, fails, stopped = execute_plan_until(plan, rm, dry_run, ls, root, stop=over)
        return plan, (f_count, d_count, r_count, fails), stopped

    results, started_walks = _bounded_map(to_walk, prune_one, workers, over, ticker("prune", len(to_walk)), poll)
    errors = [(t, r) for t, r in zip(to_walk, results) if isinstance(r, ItemError)]
    finished = [r for r in results if r is not None and not isinstance(r, ItemError)]
    done = [(p, c) for p, c, _ in finished]
    # r[2]: False = walked to the end, True = stopped at the deadline, "failed" = its relisting failed.
    stopped_walks = sum(1 for r in finished if r[2] is True)
    failed_walks = sum(1 for r in finished if r[2] == "failed")
    # Walked = pruned to the end: a cluster stopped at the deadline counts as remaining, and one that hit an
    # unexpected error is a failure, neither as walked.
    walked = started_walks - stopped_walks - failed_walks - len(errors)
    plan = _merge([p for p, _ in done], root_skipped + [s for t in young for s in t.skipped],
                  root_errors + [e for t in triaged for e in t.list_errors], root_missing)
    failures = [f"list {p}: {e}" for p, e in root_errors + [e for t in triaged for e in t.list_errors]]
    deleted_files = deleted_dirs = raced_dirs = 0
    for _, (f_count, d_count, r_count, fails) in done:
        deleted_files, deleted_dirs, raced_dirs = deleted_files + f_count, deleted_dirs + d_count, raced_dirs + r_count
        failures += fails
    failures += [f"prune {t.path}: {type(r.exc).__name__}: {r.exc}" for t, r in errors]
    remaining = len(to_walk) - walked - failed_walks - len(errors)
    delete_bytes = sum(size for _, size in plan.delete_files)
    log(f"  plan: delete {len(plan.delete_files)} file(s) ({delete_bytes} bytes) and {len(plan.delete_dirs)} "
        f"empty dir(s); keep {plan.kept_files} file(s) ({plan.kept_bytes} bytes) in the walked clusters")
    _print_capped(log, plan.skipped, lambda x: f"  SKIPPED (left untouched, {x[1]}): {x[0]}", "skipped")
    _print_capped(log, plan.list_errors, lambda x: f"  LIST FAILED (left untouched): {x[0]}: {x[1]}",
                  "listing failures")
    if plan.root_missing:
        log(f"  WARNING: {root!r} does not exist yet (no cluster has delivered logs there); nothing to prune")
    if dry_run:
        _print_capped(log, plan.delete_files, lambda x: f"  would delete {x[0]} ({x[1]} bytes)", "file(s)")
        _print_capped(log, plan.delete_dirs, lambda x: f"  would remove directory {x}", "directories")
    if remaining:
        log(f"  WARNING: max_minutes={max_minutes} reached with {remaining} cluster(s) not yet walked; the next "
            f"run carries on")
    summary = (
        f"cluster_logs_prune path={root!r} retention_days={retention_days} dry_run={dry_run} "
        f"planned_files={len(plan.delete_files)} planned_bytes={delete_bytes} planned_dirs={len(plan.delete_dirs)} "
        f"deleted_files={deleted_files} deleted_dirs={deleted_dirs} raced_dirs={raced_dirs} "
        f"kept_files={plan.kept_files} skipped={len(plan.skipped)} root_missing={plan.root_missing} "
        f"clusters={len(clusters)} clusters_young={len(young)} clusters_walked={walked} "
        f"incomplete={remaining > 0} remaining_clusters={remaining} clusters_stopped={stopped_walks} "
        f"workers={workers} "
        f"elapsed_s={int(clock() - start)} spent_before_s={int(spent_seconds)} failures={len(failures)}"
    )
    if failures:
        for f in failures[:50]:
            log(f"  FAILED: {f}")
        raise RuntimeError(f"cluster_logs_prune FAILED ({len(failures)} failure(s)): {summary}")
    return summary

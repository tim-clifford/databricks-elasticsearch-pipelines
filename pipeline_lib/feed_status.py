"""Pure logic for the `_feed status` job: decide, per pipeline config ("feed"), whether its export is
caught up, and own the status table's schema and SQL.

One row per feed, refreshed every few minutes, carrying one of STATUSES:
- CAUGHT_UP:   nothing left to send (streaming: no unsent source commit; batch: the last expected run
               succeeded and nothing is running).
- IN_PROGRESS: a send is running and has been for less than the threshold (default 1 hour).
- PENDING:     (streaming) new source data is waiting, under the threshold old, and nothing is sending it
               yet (the normal state between runs of a scheduled stream).
- BEHIND:      data has waited past the threshold, a send has run past it, the last run failed, or an
               expected scheduled run never happened.
- UNKNOWN:     the job could not establish the answer (no checkpoint, unreadable source, no logged runs,
               an unsupported cron, ...). Never shown as caught up; status_reason says why.

Where the signals come from:
- STREAMING position comes from the stream's own checkpoint (offsets/N + commits/N). It is the exact
  record of which source Delta version has been sent, including a first-run seed that is never logged.
  Live-verified Delta source offset convention (DBR serverless, 2026-10-05): after fully consuming version
  V the committed offset is (reservoirVersion=V+1, index=-1); part-way through a multi-file commit it is
  (V, i). Either way every version strictly BELOW reservoirVersion is fully sent. Uncommitted offsets past
  the last commit are the in-flight batch(es) (there can be more than one); the first one's file mtime is
  when that send began.
- STREAMING arrival time comes from DESCRIBE HISTORY on the source: the oldest unsent commit that can carry
  new rows. Only the newest commits are read (LIMIT k, widened only when needed), and the read proves its
  own coverage (history_covers), so a commit landing between reads can never silently fall out of the
  window. A Delta stream advances its offset past maintenance commits on its next run (live-verified for
  OPTIMIZE, UPDATE under skipChangeCommits, SET TBLPROPERTIES), so IGNORED_OPERATIONS only matters
  between runs.
- RUN state (both modes) comes from the monitoring log table's run_start / run_end / batch_start rows: a
  batch run's outcome, and whether a stream's leftover in-flight offsets are being worked on right now
  (the latest run has no run_end) or are debris from a run that already ended.

Like pipeline_lib/monitoring_sink.py this module is PURE (no Spark, no dbutils): every function is
unit-testable off-cluster. notebooks/feed_status.py owns the I/O and passes plain values in.
"""
import json
import re
from datetime import datetime, timedelta, timezone

from pipeline_lib.monitoring_sink import RECORD_TYPES, STATUSES as LOG_STATUSES, validate_table_name

# ---------------------------------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------------------------------

CAUGHT_UP = "CAUGHT_UP"
IN_PROGRESS = "IN_PROGRESS"
PENDING = "PENDING"
BEHIND = "BEHIND"
UNKNOWN = "UNKNOWN"
STATUSES = (CAUGHT_UP, IN_PROGRESS, PENDING, BEHIND, UNKNOWN)

# The closed set of status_reason codes, keyed by the status each may accompany. Every result is checked
# against this (see _result), so a reason can never be paired with the wrong status.
REASONS = {
    CAUGHT_UP: ("no_unsent_data", "last_run_succeeded"),
    IN_PROGRESS: ("sending", "run_in_progress"),
    PENDING: ("unsent_within_threshold",),
    BEHIND: ("unsent_over_threshold", "send_over_threshold", "run_over_threshold", "last_run_failed",
             "missed_schedule"),
    UNKNOWN: ("no_checkpoint", "no_committed_batch", "bad_checkpoint", "source_unreadable",
              "history_window_exceeded", "history_retention_exceeded", "offset_ahead_of_table", "no_runs_logged", "unknown_run_status",
              "unsupported_cron", "no_job_for_config", "unsupported_trigger", "evaluation_error"),
}

DEFAULT_BEHIND_THRESHOLD_MINUTES = 60
# A scheduled run starts a little after its fire time (serverless startup, then the run_start append). The
# status job judges a batch feed against the latest fire at least this long ago, so a run that is still
# starting is not reported as missed.
DEFAULT_SCHEDULE_GRACE_MINUTES = 10
# How far back the run-state query reads the monitoring log. Must exceed the longest schedule interval in
# use, or a feed on a slower schedule reads as no_runs_logged.
DEFAULT_LOG_LOOKBACK_DAYS = 35
# DESCRIBE HISTORY window: start small (one ~0.8 s read covers a feed in steady state) and double up to
# the cap when the window does not reach the sent position.
DEFAULT_HISTORY_LIMIT = 20
DEFAULT_HISTORY_LIMIT_CAP = 1000

# The monitoring log record_type / status values this job reads, from the run/batch row model in
# pipeline_lib/monitoring_sink.py. Named here for readable SQL, and checked against that module's
# RECORD_TYPES / STATUSES at import, so a rename there fails loudly here instead of silently matching
# no rows.
LOG_RUN_START = "run_start"
LOG_RUN_END = "run_end"
LOG_BATCH_START = "batch_start"
RUN_SUCCESS = "success"
RUN_ERROR = "error"
RUN_STOPPED = "stopped"
_missing = ({LOG_RUN_START, LOG_RUN_END, LOG_BATCH_START} - set(RECORD_TYPES)) | \
    ({RUN_SUCCESS, RUN_ERROR, RUN_STOPPED} - set(LOG_STATUSES))
if _missing:
    raise ImportError(f"feed_status reads log values monitoring_sink no longer defines: {sorted(_missing)}")

# DESCRIBE HISTORY `operation` values that can NOT carry new rows for a skipChangeCommits Delta stream, so
# a commit with one of these is never "unsent data". An ALLOW-LIST of the safe-to-ignore set: any
# operation not named here (WRITE, STREAMING UPDATE, CREATE TABLE AS SELECT, COPY INTO, anything new) is
# treated as possibly carrying rows, so an unanticipated operation fails toward PENDING/BEHIND, never
# toward a false CAUGHT_UP. Every name is the exact string recorded on DBR serverless (2026-10-05 probe).
# MERGE is ignored by decision for v1 (no source table uses it); an insert-only MERGE does add rows the
# stream sends, so revisit this if a merged-into source appears.
IGNORED_OPERATIONS = frozenset({
    "CREATE TABLE",          # empty create (a CTAS records CREATE TABLE AS SELECT, which is NOT ignored)
    "OPTIMIZE",              # compaction / reclustering; also runs automatically (auto-compaction)
    "VACUUM START",
    "VACUUM END",
    "UPDATE",                # change commits: skipChangeCommits skips them
    "DELETE",
    "TRUNCATE",
    "MERGE",                 # ignored by v1 decision (see above)
    "SET TBLPROPERTIES",     # metadata-only commits
    "UNSET TBLPROPERTIES",
    "ADD COLUMNS",
    "CHANGE COLUMN",
    "CLUSTER BY",
})

# ---------------------------------------------------------------------------------------------------
# Status table
# ---------------------------------------------------------------------------------------------------

# The status table's columns, in order, as (name, sql_type): the single source for the CREATE, the MERGE
# and the row shape (RESULT_FIELDS).
STATUS_TABLE_COLUMNS = (
    ("config_name", "STRING"),           # the feed: one pipeline config, the MERGE key
    ("pipeline_mode", "STRING"),         # batch | streaming, from the config
    ("trigger", "STRING"),               # schedule | continuous | on_demand, from the generated job
    ("trigger_paused", "BOOLEAN"),       # the deployed trigger's effective pause state (NULL on_demand)
    ("status", "STRING"),                # one of STATUSES
    ("status_reason", "STRING"),         # one of REASONS[status]
    ("source_table", "STRING"),          # streaming: the source Delta table (catalog.schema.table)
    ("source_version", "BIGINT"),        # streaming: newest source version seen
    ("sent_through_version", "BIGINT"),  # streaming: every version <= this is fully sent
    ("oldest_unsent_ts", "TIMESTAMP"),   # streaming: commit time of the oldest unsent data commit
    ("lag_minutes", "DOUBLE"),           # streaming: minutes since oldest_unsent_ts (NULL when none)
    ("in_flight_since", "TIMESTAMP"),    # streaming: when the active send began
    ("last_run_id", "STRING"),           # the latest logged run's job_run_id
    ("last_run_status", "STRING"),       # its run_end status, or NULL when it has not ended
    ("last_run_start_ts", "TIMESTAMP"),
    ("last_run_end_ts", "TIMESTAMP"),
    ("expected_run_ts", "TIMESTAMP"),    # batch on an active schedule: the fire time a run must cover
    ("detail", "STRING"),                # free text for UNKNOWN / errors
    ("evaluated_at", "TIMESTAMP"),       # when this row was computed
)
RESULT_FIELDS = tuple(name for name, _type in STATUS_TABLE_COLUMNS)
_TS_FIELDS = frozenset(name for name, sql_type in STATUS_TABLE_COLUMNS if sql_type == "TIMESTAMP")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def create_status_table_sql(table_name):
    """Idempotent `CREATE TABLE IF NOT EXISTS` for the status table, from STATUS_TABLE_COLUMNS. The name
    is validated with the same strict allow-list as the monitoring table (it is interpolated into SQL)."""
    canonical = validate_table_name(table_name, "feed_status_table")
    cols = ",\n  ".join(f"{name} {sql_type}" for name, sql_type in STATUS_TABLE_COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {canonical} (\n  {cols}\n) USING DELTA"


def merge_status_sql(table_name, source_view):
    """MERGE the freshly computed rows (`source_view`, a temp view with RESULT_FIELDS) into the status
    table: update matched feeds, insert new ones, and DELETE rows for feeds no longer in the configs, so
    the table always holds exactly one row per current feed."""
    canonical = validate_table_name(table_name, "feed_status_table")
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", source_view or ""):
        raise ValueError(f"source_view must be a bare identifier, got {source_view!r}")
    sets = ", ".join(f"t.{f} = s.{f}" for f in RESULT_FIELDS if f != "config_name")
    cols = ", ".join(RESULT_FIELDS)
    vals = ", ".join(f"s.{f}" for f in RESULT_FIELDS)
    return (
        f"MERGE INTO {canonical} t USING {source_view} s ON t.config_name = s.config_name\n"
        f"WHEN MATCHED THEN UPDATE SET {sets}\n"
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})\n"
        f"WHEN NOT MATCHED BY SOURCE THEN DELETE"
    )


def run_state_sql(log_table, lookback_days=DEFAULT_LOG_LOOKBACK_DAYS):
    """One row per config_name with its LATEST run ATTEMPT in the lookback window (job_run_id,
    run_started_at, run_ended_at, run_end_status) and its newest batch_start (last_batch_started_at).

    An attempt is keyed by (config_name, job_run_id, batch_start_ts): run_start and run_end both carry the
    attempt's own start wall clock in batch_start_ts. job_run_id alone is NOT enough: it is {{job.run_id}},
    shared by every task retry inside one job run (ON_FAILURE retries, a continuous stream's restarts), so
    grouping by it would pair a failed attempt's run_end with the retry that is running now.

    last_batch_started_at (outer-joined) keeps a long-lived continuous attempt visible after its run_start
    has aged out of the window. The event_ts predicates let the clustered (config_name, event_ts)
    monitoring table skip old files."""
    canonical = validate_table_name(log_table, "monitoring_log_table")
    days = int(lookback_days)
    if days <= 0:
        raise ValueError(f"lookback_days must be positive, got {lookback_days!r}")
    window = f"event_ts >= current_timestamp() - INTERVAL {days} DAYS"
    return f"""
WITH attempts AS (
  SELECT config_name, job_run_id, batch_start_ts AS run_started_at,
    max(CASE WHEN record_type = '{LOG_RUN_END}' THEN coalesce(batch_end_ts, event_ts) END) AS run_ended_at,
    max(CASE WHEN record_type = '{LOG_RUN_END}' THEN status END) AS run_end_status
  FROM {canonical}
  WHERE {window} AND record_type IN ('{LOG_RUN_START}', '{LOG_RUN_END}') AND batch_start_ts IS NOT NULL
  GROUP BY config_name, job_run_id, batch_start_ts
),
latest AS (
  SELECT config_name,
    max_by(named_struct('job_run_id', job_run_id, 'run_started_at', run_started_at,
                        'run_ended_at', run_ended_at, 'run_end_status', run_end_status),
           run_started_at) AS a
  FROM attempts GROUP BY config_name
),
batches AS (
  SELECT config_name, max(coalesce(batch_start_ts, event_ts)) AS last_batch_started_at
  FROM {canonical}
  WHERE {window} AND record_type = '{LOG_BATCH_START}'
  GROUP BY config_name
)
SELECT coalesce(l.config_name, b.config_name) AS config_name,
  l.a.job_run_id AS job_run_id, l.a.run_started_at AS run_started_at, l.a.run_ended_at AS run_ended_at,
  l.a.run_end_status AS run_end_status, b.last_batch_started_at AS last_batch_started_at
FROM latest l FULL OUTER JOIN batches b ON l.config_name = b.config_name""".strip()


# ---------------------------------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------------------------------

def _result(status, reason, **fields):
    """A result dict keyed by RESULT_FIELDS (unset fields None). Fail-closed: status must be in STATUSES
    and reason in REASONS[status], and every keyword must be a known field."""
    if status not in STATUSES:
        raise ValueError(f"unknown status {status!r}")
    if reason not in REASONS[status]:
        raise ValueError(f"reason {reason!r} is not allowed with status {status}")
    unknown = set(fields) - set(RESULT_FIELDS)
    if unknown:
        raise ValueError(f"unknown result field(s): {', '.join(sorted(unknown))}")
    out = dict.fromkeys(RESULT_FIELDS)
    out.update(fields)
    out["status"] = status
    out["status_reason"] = reason
    return out


def unknown_result(reason, detail=None, **fields):
    """An UNKNOWN result with a reason code and optional detail text."""
    return _result(UNKNOWN, reason, detail=detail, **fields)


def _minutes(delta):
    return delta.total_seconds() / 60.0


# ---------------------------------------------------------------------------------------------------
# Streaming: checkpoint
# ---------------------------------------------------------------------------------------------------

def parse_delta_offset(text):
    """Parse a streaming checkpoint offsets/N file for a single Delta source into
    {"reservoir_version": int, "index": int, "reservoir_id": str}.

    The file is `v1`, a metadata JSON line, then one JSON line per source; these pipelines read exactly
    one source, so the last non-empty line is the Delta source offset. Fail-closed: raises ValueError
    unless that line is a JSON object with integer reservoirVersion and index (the caller reports
    bad_checkpoint)."""
    if not isinstance(text, str):
        raise ValueError(f"offset file content must be text, got {type(text).__name__}")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 3 or not lines[0].startswith("v"):
        raise ValueError("offset file is not `v<N>` + metadata + a source offset line")
    try:
        offset = json.loads(lines[-1])
    except json.JSONDecodeError as e:
        raise ValueError(f"source offset line is not JSON: {e}") from None
    if not isinstance(offset, dict):
        raise ValueError("source offset line is not a JSON object")
    rv, idx = offset.get("reservoirVersion"), offset.get("index")
    for name, val in (("reservoirVersion", rv), ("index", idx)):
        if isinstance(val, bool) or not isinstance(val, int):
            raise ValueError(f"source offset {name} must be an integer, got {val!r}")
    if rv < 0 or idx < -1:
        raise ValueError(f"source offset out of range: reservoirVersion={rv}, index={idx}")
    return {"reservoir_version": rv, "index": idx, "reservoir_id": offset.get("reservoirId")}


def _batch_ids(names):
    """Integer batch ids from a directory listing (names may carry a trailing '/'; non-numeric names such
    as temp files are ignored)."""
    out = set()
    for n in names or ():
        n = str(n).rstrip("/")
        if n.isdigit():
            out.add(int(n))
    return out


def summarize_checkpoint(offset_names, commit_names, read_offset, offset_mtime):
    """Summarize a stream checkpoint into its sent position and in-flight batches.

    offset_names / commit_names: the entries of <checkpoint>/offsets and <checkpoint>/commits, or None
    when the directory does not exist. read_offset(batch_id) returns that offsets file's text;
    offset_mtime(batch_id) returns its modification time as an aware datetime. (Injected so this stays
    pure; the notebook passes FUSE file reads.)

    Returns {"committed": offset|None, "in_flight": offset|None, "in_flight_since": datetime|None,
    "state": "ok" | "no_checkpoint" | "no_committed_batch"}. `committed` is the offset of the newest
    committed batch; `in_flight` the newest offset past it (None when everything planned is committed).
    Raises ValueError on an unparseable offset (the caller reports bad_checkpoint)."""
    offsets = _batch_ids(offset_names) if offset_names is not None else set()
    commits = _batch_ids(commit_names) if commit_names is not None else set()
    if not offsets:
        return {"state": "no_checkpoint", "committed": None, "in_flight": None, "in_flight_since": None}
    if not commits:
        return {"state": "no_committed_batch", "committed": None, "in_flight": None, "in_flight_since": None}
    last_commit = max(commits)
    if last_commit not in offsets:
        raise ValueError(f"commits/{last_commit} has no matching offsets/{last_commit}")
    committed = parse_delta_offset(read_offset(last_commit))
    pending_ids = sorted(b for b in offsets if b > last_commit)
    in_flight = since = None
    if pending_ids:
        in_flight = parse_delta_offset(read_offset(pending_ids[-1]))
        since = offset_mtime(pending_ids[0])
    return {"state": "ok", "committed": committed, "in_flight": in_flight, "in_flight_since": since}


# ---------------------------------------------------------------------------------------------------
# Streaming: source history
# ---------------------------------------------------------------------------------------------------

def carries_rows(operation):
    """Can a commit with this DESCRIBE HISTORY operation carry rows the stream would send? True for
    everything not in the IGNORED_OPERATIONS allow-list (fail toward 'yes')."""
    return operation not in IGNORED_OPERATIONS


def history_covers(history, limit, from_version):
    """Does a `DESCRIBE HISTORY ... LIMIT limit` result include every version >= from_version? Only when
    it reaches down to from_version: LIMIT returns the newest rows, contiguous by version, so reaching
    from_version means nothing above it is missing, however many commits landed between reads. A result
    shorter than the limit is the whole RETAINED history, which still does not cover versions that Delta
    log cleanup has already dropped (see history_exhausted)."""
    if not history:
        return False
    return min(h["version"] for h in history) <= from_version


def history_exhausted(history, limit):
    """Is this the source's entire retained history (fewer rows than asked for)? Widening the window
    cannot then reveal anything older."""
    return len(history) < limit


def next_history_limit(limit, cap=DEFAULT_HISTORY_LIMIT_CAP):
    """The next, wider history window (doubling), or None once the cap has been read."""
    if limit >= cap:
        return None
    return min(limit * 2, cap)


def classify_streaming(checkpoint, history, history_limit, run, now,
                       threshold_minutes=DEFAULT_BEHIND_THRESHOLD_MINUTES):
    """Classify a streaming feed. Returns a result dict, or None when the history window does not reach
    far enough to decide (the caller widens it with next_history_limit and calls again).

    checkpoint: summarize_checkpoint(...) output. history: DESCRIBE HISTORY rows as dicts with integer
    `version`, aware datetime `timestamp`, and `operation`, read AFTER the checkpoint (so a send that
    finishes in between can only make a feed look more behind for one cycle, never caught up early).
    history_limit: the LIMIT used. run: the latest logged run (see latest_run) or None.

    A send counts as ACTIVE only when the checkpoint has in-flight offsets AND the latest logged run has
    not ended. Leftover offsets from a run that ended (a crash, a failure) are not an active send."""
    threshold = timedelta(minutes=threshold_minutes)
    state = checkpoint.get("state")
    if state == "no_checkpoint":
        return unknown_result("no_checkpoint")
    if state == "no_committed_batch":
        return unknown_result("no_committed_batch")
    if state != "ok":
        return unknown_result("bad_checkpoint", detail=f"checkpoint state {state!r}")
    if not history:
        return unknown_result("source_unreadable", detail="DESCRIBE HISTORY returned no rows")

    committed = checkpoint["committed"]
    sent_below = committed["reservoir_version"]  # every version < this is fully sent
    newest = max(h["version"] for h in history)
    common = {"source_version": newest, "sent_through_version": sent_below - 1, **_run_fields(run)}
    if sent_below > newest + 1:
        return unknown_result("offset_ahead_of_table", **common,
                              detail=f"checkpoint at version {sent_below} but the source's newest is {newest}")

    in_flight = checkpoint.get("in_flight")
    active = in_flight is not None and run is not None and run.get("run_ended_at") is None
    since = checkpoint.get("in_flight_since") if active else None
    # With an active send, versions below the in-flight offset are being sent now; only later ones (and a
    # partially planned version itself) are waiting. Without one, everything from the committed offset is.
    waiting_from = in_flight["reservoir_version"] if active else sent_below

    unsent = sorted((h for h in history if h["version"] >= sent_below and carries_rows(h["operation"])),
                    key=lambda h: h["version"])
    waiting = [h for h in unsent if h["version"] >= waiting_from]
    oldest = unsent[0]["timestamp"] if unsent else None
    # Clamped at 0: a commit newer than `now` (it landed after evaluation began, or writer clock skew) is
    # simply fresh, never a negative age.
    common.update(oldest_unsent_ts=oldest, in_flight_since=since,
                  lag_minutes=max(0.0, _minutes(now - oldest)) if oldest else None)

    # Decisive regardless of window coverage: anything older than these is older still.
    if active and since is not None and now - since >= threshold:
        return _result(BEHIND, "send_over_threshold", **common)
    if waiting and now - waiting[0]["timestamp"] >= threshold:
        return _result(BEHIND, "unsent_over_threshold", **common)
    if not history_covers(history, history_limit, sent_below):
        if history_exhausted(history, history_limit):
            return unknown_result("history_retention_exceeded", **common,
                                  detail=f"sent position {sent_below} predates the oldest retained commit "
                                         f"{min(h['version'] for h in history)}")
        return None
    if not unsent:
        return _result(CAUGHT_UP, "no_unsent_data", **common)
    if active:
        return _result(IN_PROGRESS, "sending", **common)
    return _result(PENDING, "unsent_within_threshold", **common)


# ---------------------------------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------------------------------

def latest_run(row):
    """Normalize one run_state_sql row (a dict) into {job_run_id, started_at, run_ended_at,
    run_end_status}, or None for a feed with no logged run.

    A batch_start newer than the latest attempt's end (or with no attempt in the window at all) means an
    attempt is running whose run_start is not visible (a long-lived continuous run that started before the
    lookback window): it is reported as an OPEN run starting at that batch, never as the older ended one."""
    if not row:
        return None
    started, ended = row.get("run_started_at"), row.get("run_ended_at")
    last_batch = row.get("last_batch_started_at")
    if last_batch is not None and (started is None or (ended is not None and last_batch > ended)):
        return {"job_run_id": None, "started_at": last_batch, "run_ended_at": None, "run_end_status": None}
    return {"job_run_id": row.get("job_run_id"), "started_at": started, "run_ended_at": ended,
            "run_end_status": row.get("run_end_status")}


def _run_fields(run):
    if run is None:
        return {}
    return {"last_run_id": run["job_run_id"], "last_run_status": run["run_end_status"],
            "last_run_start_ts": run["started_at"], "last_run_end_ts": run["run_ended_at"]}


def classify_batch(run, trigger, now, threshold_minutes=DEFAULT_BEHIND_THRESHOLD_MINUTES,
                   grace_minutes=DEFAULT_SCHEDULE_GRACE_MINUTES):
    """Classify a batch feed from its latest logged run and its deployed trigger (see feed_triggers).

    - no logged run                         => UNKNOWN no_runs_logged
    - running (no run_end) < threshold      => IN_PROGRESS run_in_progress; >= threshold => BEHIND
    - ended error                           => BEHIND last_run_failed
    - ended success, active schedule, but the latest fire at least grace_minutes ago came AFTER that run
      ended (so nothing ran for it)  => BEHIND missed_schedule. A fire that landed while the run was still
      going was skipped for overlap (max_concurrent_runs 1, queue off), not missed, so a run longer than
      the schedule interval does not read as behind.
    - ended success otherwise               => CAUGHT_UP last_run_succeeded
    - any other run_end status              => UNKNOWN unknown_run_status (allow-list)
    A paused or on-demand trigger has no expected fire time, so only the last run's outcome counts."""
    threshold = timedelta(minutes=threshold_minutes)
    if run is None or run.get("started_at") is None:
        return unknown_result("no_runs_logged")
    fields = _run_fields(run)
    status = run["run_end_status"]
    if run["run_ended_at"] is None and status is None:
        if now - run["started_at"] >= threshold:
            return _result(BEHIND, "run_over_threshold", **fields)
        return _result(IN_PROGRESS, "run_in_progress", **fields)
    if status == RUN_ERROR:
        return _result(BEHIND, "last_run_failed", **fields)
    if status != RUN_SUCCESS:
        return unknown_result("unknown_run_status", detail=f"run_end status {status!r}", **fields)
    if trigger.get("kind") == "schedule" and not trigger.get("paused"):
        try:
            expected = previous_fire(trigger["cron"], now - timedelta(minutes=grace_minutes))
        except UnsupportedCron as e:
            return unknown_result("unsupported_cron", detail=str(e), **fields)
        fields["expected_run_ts"] = expected
        if expected > (run["run_ended_at"] or run["started_at"]):
            return _result(BEHIND, "missed_schedule", **fields)
    return _result(CAUGHT_UP, "last_run_succeeded", **fields)


# ---------------------------------------------------------------------------------------------------
# Deployed triggers
# ---------------------------------------------------------------------------------------------------

_RUNNER_NOTEBOOK = "run_index_pipeline.py"
_PAUSE_GLOBAL_REF = "${var.schedule_pause_status}"


def feed_triggers(job_docs, schedule_pause_status):
    """Map config_name -> its deployed trigger, read from the generated resources/*.job.yml documents
    (what gen_jobs emitted, so grouped configs get their group job's trigger). `job_docs` is an iterable
    of parsed YAML dicts; `schedule_pause_status` is the target's resolved ${var.schedule_pause_status}.

    Each value is {"kind": "schedule"|"continuous"|"on_demand", "cron": str|None, "paused": bool|None},
    or {"kind": "unsupported", "detail": ...} when the trigger's pause state is neither a literal nor the
    global reference, or when one config is run by more than one job (fail closed: which trigger applies
    is ambiguous). Jobs whose tasks do not run the index-pipeline notebook are skipped."""
    out, seen = {}, set()
    for doc in job_docs:
        jobs = ((doc or {}).get("resources") or {}).get("jobs") or {}
        for job in jobs.values():
            kind, cron, raw_pause = "on_demand", None, None
            if job.get("continuous") is not None:
                kind, raw_pause = "continuous", (job["continuous"] or {}).get("pause_status")
            elif job.get("schedule") is not None:
                kind = "schedule"
                cron = (job["schedule"] or {}).get("quartz_cron_expression")
                raw_pause = (job["schedule"] or {}).get("pause_status")
            trig = {"kind": kind, "cron": cron, "paused": None}
            if kind != "on_demand":
                pause = schedule_pause_status if raw_pause == _PAUSE_GLOBAL_REF else raw_pause
                if pause not in ("PAUSED", "UNPAUSED"):
                    trig = {"kind": "unsupported", "detail": f"pause_status {raw_pause!r} resolves to {pause!r}"}
                else:
                    trig["paused"] = pause == "PAUSED"
            for task in job.get("tasks") or []:
                nb = task.get("notebook_task") or {}
                if not str(nb.get("notebook_path", "")).endswith(_RUNNER_NOTEBOOK):
                    continue
                name = (nb.get("base_parameters") or {}).get("config_name")
                if not name:
                    continue
                if name in seen:
                    out[name] = {"kind": "unsupported", "detail": f"config {name!r} is run by more than one job"}
                else:
                    out[name] = trig
                seen.add(name)
    return out


def to_row(config_name, pipeline_mode, trigger, result, evaluated_at, source_table=None):
    """Finish a result into a status-table row: identity, trigger columns, and every TIMESTAMP field as
    integer microseconds since the Unix epoch (the notebook converts with timestamp_micros(), so the stored
    value never depends on the Spark session time zone). A naive datetime is taken as UTC."""
    row = dict(result)
    row.update(config_name=config_name, pipeline_mode=pipeline_mode, evaluated_at=evaluated_at,
               trigger=(trigger or {}).get("kind"), trigger_paused=(trigger or {}).get("paused"),
               source_table=source_table if pipeline_mode == "streaming" else None)
    for f in _TS_FIELDS:
        v = row.get(f)
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            row[f] = (v - _EPOCH) // timedelta(microseconds=1)
    return {f: row.get(f) for f in RESULT_FIELDS}


# ---------------------------------------------------------------------------------------------------
# Quartz cron: previous fire time
# ---------------------------------------------------------------------------------------------------

class UnsupportedCron(ValueError):
    """A Quartz cron uses syntax previous_fire does not implement (L, W, #, C, or a malformed field).
    The caller reports the feed UNKNOWN rather than guessing a fire time."""


# Quartz month and day-of-week names. Quartz numbers days of the week 1=SUN .. 7=SAT.
_MONTH_NAMES = {n: i + 1 for i, n in enumerate(
    ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"))}
_DOW_NAMES = {n: i + 1 for i, n in enumerate(("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"))}

# One comma-separated term: `*`, `N`, `A-B`, each optionally followed by `/STEP`. Anything else (L, W, #,
# C, stray characters) is rejected: an allow-list, so an unrecognized token can never be misread.
_CRON_TERM = re.compile(r"^(\*|[0-9A-Z]+(?:-[0-9A-Z]+)?)(?:/([0-9]+))?$")

# How far back previous_fire searches for a matching day. Eight years covers a Feb 29 schedule across
# any leap-year gap; a cron that matches nothing in that window is treated as unsupported.
_MAX_LOOKBACK_DAYS = 366 * 8


def _parse_field(text, lo, hi, names=None):
    """Expand one cron field to the set of integers it matches within [lo, hi]. Raises UnsupportedCron
    on any token outside the supported grammar (see _CRON_TERM) or out of range."""
    out = set()
    for term in text.upper().split(","):
        m = _CRON_TERM.match(term)
        if not m:
            raise UnsupportedCron(f"unsupported cron term {term!r}")
        base, step = m.group(1), m.group(2)

        def num(tok):
            if names and tok in names:
                return names[tok]
            if not tok.isdigit():
                raise UnsupportedCron(f"unsupported cron value {tok!r}")
            return int(tok)

        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-")
            start, end = num(a), num(b)
        else:
            start = num(base)
            # Quartz `N/STEP` means "from N to the top of the range, every STEP".
            end = hi if step else start
        if not (lo <= start <= hi and lo <= end <= hi and start <= end):
            raise UnsupportedCron(f"cron term {term!r} out of range {lo}-{hi}")
        stride = int(step) if step else 1
        if stride <= 0:
            raise UnsupportedCron(f"cron step must be positive in {term!r}")
        out.update(range(start, end + 1, stride))
    return out


def parse_quartz_cron(expr):
    """Parse a Quartz cron (`sec min hour day-of-month month day-of-week [year]`) into a dict of
    allowed-value sets. Exactly one of day-of-month / day-of-week must be `?` (the Quartz rule); the other
    is the active day filter. Raises UnsupportedCron on anything outside the supported subset."""
    if not isinstance(expr, str):
        raise UnsupportedCron(f"cron must be a string, got {type(expr).__name__}")
    fields = expr.split()
    if len(fields) not in (6, 7):
        raise UnsupportedCron(f"Quartz cron needs 6 or 7 fields, got {len(fields)} in {expr!r}")
    sec, minute, hour, dom, month, dow = fields[:6]
    year = fields[6] if len(fields) == 7 else "*"
    if (dom == "?") == (dow == "?"):
        raise UnsupportedCron(f"exactly one of day-of-month / day-of-week must be '?' in {expr!r}")
    return {
        "second": _parse_field(sec, 0, 59),
        "minute": _parse_field(minute, 0, 59),
        "hour": _parse_field(hour, 0, 23),
        "dom": None if dom == "?" else _parse_field(dom, 1, 31),
        "month": _parse_field(month, 1, 12, _MONTH_NAMES),
        "dow": None if dow == "?" else _parse_field(dow, 1, 7, _DOW_NAMES),
        "year": _parse_field(year, 1970, 2199),
    }


def _day_matches(spec, d):
    if d.year not in spec["year"] or d.month not in spec["month"]:
        return False
    if spec["dom"] is not None:
        return d.day in spec["dom"]
    # Python weekday(): Monday=0 .. Sunday=6. Quartz: SUN=1 .. SAT=7.
    return ((d.weekday() + 1) % 7) + 1 in spec["dow"]


def previous_fire(expr, now):
    """The latest UTC instant <= `now` at which the Quartz cron `expr` fires (generated jobs always use
    timezone UTC). `now` must be timezone-aware. Raises UnsupportedCron for syntax outside the supported
    subset or a cron with no fire time in the lookback window."""
    spec = parse_quartz_cron(expr)
    now = now.astimezone(timezone.utc).replace(microsecond=0)
    hours = sorted(spec["hour"], reverse=True)
    minutes = sorted(spec["minute"], reverse=True)
    seconds = sorted(spec["second"], reverse=True)
    for offset in range(_MAX_LOOKBACK_DAYS):
        day = (now - timedelta(days=offset)).date()
        if not _day_matches(spec, day):
            continue
        today = offset == 0
        for h in hours:
            if today and h > now.hour:
                continue
            for mi in minutes:
                if today and h == now.hour and mi > now.minute:
                    continue
                for s in seconds:
                    if today and h == now.hour and mi == now.minute and s > now.second:
                        continue
                    return datetime(day.year, day.month, day.day, h, mi, s, tzinfo=timezone.utc)
    raise UnsupportedCron(f"cron {expr!r} has no fire time in the last {_MAX_LOOKBACK_DAYS} days")

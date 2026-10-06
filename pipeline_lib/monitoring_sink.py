"""Pure logic for the OPTIONAL durable monitoring log: turn what run_index_pipeline.py knows about a run
and its batches into rows for one shared UC Delta table, and own that table's schema and its SQL.

Why this module exists, and what it deliberately is NOT:
- It is the SINGLE SOURCE OF TRUTH for the monitoring table's columns (MONITORING_TABLE_COLUMNS) and its
  closed row vocabulary (RECORD_TYPES x STATUSES). Both the `_log table create` job (which runs the
  CREATE / additive migration) and the writer (run_index_pipeline.py, which appends) import that one
  definition, so the DDL and the rows can never drift apart.
- It is PURE: no Spark, no dbutils, no Databricks. Every function here is unit-testable off-cluster
  (plain pytest), exactly like pipeline_lib/observability.py. The notebooks own all Spark I/O.

THE ROW MODEL. Two levels, each with a start and an end, plus one diagnostics row per batch:
- run_start / run_end: one pair per job run (run_end is absent only when the run was killed outright,
  which is itself the signal: a run_start with no run_end is a run that died).
- batch_start / batch_end / batch_summary: one set per batch. A batch-mode run is exactly ONE batch
  (batch_id BATCH_MODE_BATCH_ID); a streaming run has one per micro-batch (the micro-batch id). A
  batch_start with no batch_end is a batch that never finished.
- batch_end is the OUTCOME (status success|error, end time, ES counts or the error); batch_summary is the
  DIAGNOSTICS (the ES write rollup and, for streaming, Spark's progress report for that batch). They are
  separate because in streaming Spark only publishes a batch's progress AFTER the batch has committed,
  later than batch_end is written.

DESIGN INVARIANT - when the log is ON it is part of the contract, not best-effort. The builders RAISE on
unusable input (a builder that silently produced no row would be exactly the missing log entry this
model exists to prevent), and the notebook's writer raises on a failed append, so a log fault fails the
task. Only the CONTENT of diagnostics is fail-soft (observability's rollups yield None for a figure they
cannot compute), and payload serialization never raises (default=str).
"""
import json
import re
from datetime import datetime, timedelta, timezone

from pipeline_lib.observability import bulk_stats_overall, bulk_stats_tail

# The monitoring table's columns, in order, as (name, sql_type). This ONE tuple drives both the CREATE
# TABLE statement (create_table_sql) and the row shape (ROW_FIELDS), so the schema is defined exactly
# once. payload is VARIANT (semi-structured, queryable with `payload:field` paths in SQL); it needs DBR
# 15.3+ / recent serverless, which every target here runs. event_ts is the batch/emit wall-clock the
# row is about; ingest_ts is when the row was actually written (a WRITER-SUPPLIED default, see
# ROW_FIELDS), so a delayed relay is still distinguishable from batch time.
MONITORING_TABLE_COLUMNS = (
    ("config_name", "STRING"),     # which pipeline config emitted the row
    ("job_run_id", "STRING"),      # the Databricks job run id, to group rows of one run across batches
    ("record_type", "STRING"),     # discriminator; one of RECORD_TYPES
    ("status", "STRING"),          # one of STATUSES (started | success | error | stopped); see _ALLOWED_STATUS
    ("batch_id", "BIGINT"),        # batch id (micro-batch id; BATCH_MODE_BATCH_ID in batch mode); NULL on run rows
    ("event_ts", "TIMESTAMP"),     # UTC wall-clock the row is ABOUT (batch/emit time)
    ("batch_start_ts", "TIMESTAMP"),  # UTC wall-clock the batch/run STARTED; NULL where not applicable
    ("batch_end_ts", "TIMESTAMP"),    # UTC wall-clock the batch/run ENDED; NULL where not applicable
    ("payload", "VARIANT"),        # the type-specific fields, verbatim from the source dict
    ("ingest_ts", "TIMESTAMP"),    # UTC wall-clock the row was WRITTEN (writer supplies via current_timestamp())
)

# The allow-list of record_type values (see THE ROW MODEL above). A row builder only ever emits one of
# these; the writer and any reader can trust the set is closed. (Adding a type is a deliberate change.)
RECORD_TYPES = (
    "run_start",      # one per run, first row written: identity + effective settings
    "run_end",        # one per run that ended in-process: success | error | stopped, totals or the error
    "batch_start",    # one per batch, written immediately BEFORE the batch's data is sent to ES
    "batch_end",      # one per batch that ended: success (ES counts) | error (the exception)
    "batch_summary",  # one per batch with diagnostics: ES write rollup (+ Spark progress for streaming)
)

# The allow-list of status values, and which statuses each record_type may carry. `stopped` is a
# continuous stream that ended WITHOUT an error (a job cancel, redeploy, or cluster shutdown), which is
# neither a success nor a failure of the export.
STATUSES = ("started", "success", "error", "stopped")
_ALLOWED_STATUS = {
    "run_start": ("started",),
    "run_end": ("success", "error", "stopped"),
    "batch_start": ("started",),
    "batch_end": ("success", "error"),
    "batch_summary": ("success", "error"),
}

# A batch-mode run is exactly ONE batch; its batch rows carry this id so batch and streaming rows join
# and pair the same way (streaming micro-batch ids start at 0 too, but config_name + job_run_id keep the
# two apart: one run is one mode).
BATCH_MODE_BATCH_ID = 0

# An exception message is stored on error rows, capped so a pathological message (a Delta schema-change
# error embeds both full schemas, tens of KB) cannot bloat the table. The cap is generous: the head of
# the message names the error class and the cause.
MAX_ERROR_MESSAGE_CHARS = 16000

# The fields a row builder emits, in order. This is MONITORING_TABLE_COLUMNS MINUS the writer-supplied
# ingest_ts (the writer stamps ingest_ts with the Spark current_timestamp() at append time, so a pure
# builder never invents it). assert_columns_consistent() enforces the relationship so the two lists
# cannot drift.
ROW_FIELDS = ("config_name", "job_run_id", "record_type", "status", "batch_id", "event_ts",
              "batch_start_ts", "batch_end_ts", "payload")

# Liquid-clustering columns for the monitoring table. Every monitoring query filters by WHICH pipeline
# (config_name) and a TIME window (event_ts), so clustering on these two gives data skipping as the table
# grows, without the small-partition skew hive-partitioning this high-ingest/low-row-width table would
# cause. Liquid clustering (CLUSTER BY) needs DBR 13.3+ (all targets run 15.3+/serverless for VARIANT).
CLUSTER_BY_COLUMNS = ("config_name", "event_ts")

# A UC name part (catalog / schema / table): a leading letter or underscore, then letters/digits/
# underscores. Strict ALLOW-LIST (not a deny-list): the table name is interpolated into a CREATE TABLE
# string, so anything not matching this is rejected rather than escaped, closing off SQL injection and
# malformed identifiers. Backtick-quoted names with exotic characters are intentionally NOT supported;
# the monitoring table is ours to name plainly.
_NAME_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def assert_columns_consistent():
    """Guard the invariant that ROW_FIELDS is exactly the table columns minus the writer-supplied
    ingest_ts, so the DDL (MONITORING_TABLE_COLUMNS) and the row shape (ROW_FIELDS) can never drift.
    Called by tests; cheap enough to be a plain assert."""
    col_names = [name for name, _type in MONITORING_TABLE_COLUMNS]
    assert col_names[-1] == "ingest_ts", "ingest_ts must be the last (writer-supplied) column"
    assert tuple(col_names[:-1]) == ROW_FIELDS, "ROW_FIELDS must equal table columns minus ingest_ts"


def _fmt_ts(dt):
    """Format a datetime to the stored UTC string `YYYY-MM-DDTHH:MM:SS.ffffff`. A tz-aware dt is converted
    to UTC; a naive one is assumed already-UTC (the same contract observability._ts_token uses). Fail-soft:
    returns None on any error, so a builder never raises on a timestamp."""
    try:
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    except Exception:
        return None


def _event_ts(now=None):
    """The event_ts string a builder stores. `now=None` reads the current UTC time (so event_ts is always
    populated); tests pass a fixed datetime. Delegates formatting to _fmt_ts."""
    return _fmt_ts(now if now is not None else datetime.now(timezone.utc))


def _opt_ts(dt):
    """Format an OPTIONAL timing (batch_start_ts / batch_end_ts). Unlike _event_ts, `None` means NULL (not
    'now'): a row that has no meaningful start/end stores NULL rather than inventing the current time."""
    return None if dt is None else _fmt_ts(dt)


def _json(payload):
    """Serialize a payload dict to a compact, stable JSON string for the VARIANT column. sort_keys keeps
    it deterministic (so tests and diffs are stable); default=str means an odd value (a datetime, a
    Decimal) is stringified rather than raising - fail-soft, a diagnostic row must never break a write."""
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


def _row(config_name, job_run_id, record_type, status, batch_id, payload, now=None,
         start=None, end=None):
    """Assemble one row dict keyed by ROW_FIELDS. FAIL-CLOSED: record_type must be in RECORD_TYPES, status
    must be one _ALLOWED_STATUS permits for it, payload must be a dict, and a batch row needs an integer
    batch_id (a run row must have none). Any violation raises ValueError: a malformed row is a bug, and a
    silently dropped row is the missing log entry this module exists to prevent. start/end are OPTIONAL
    datetimes (None => NULL) stored as batch_start_ts / batch_end_ts (the run's or batch's wall clock)."""
    if record_type not in RECORD_TYPES:
        raise ValueError(f"unknown record_type {record_type!r}; allowed: {', '.join(RECORD_TYPES)}")
    if status not in _ALLOWED_STATUS[record_type]:
        raise ValueError(f"status {status!r} not allowed for {record_type}; allowed: "
                         f"{', '.join(_ALLOWED_STATUS[record_type])}")
    if not isinstance(payload, dict):
        raise ValueError(f"{record_type} payload must be a dict, got {type(payload).__name__}")
    if record_type.startswith("batch_"):
        if isinstance(batch_id, bool) or not isinstance(batch_id, int) or batch_id < 0:
            raise ValueError(f"{record_type} needs a non-negative integer batch_id, got {batch_id!r}")
    elif batch_id is not None:
        raise ValueError(f"{record_type} is a run row and carries no batch_id, got {batch_id!r}")
    return {
        "config_name": config_name,
        "job_run_id": job_run_id,
        "record_type": record_type,
        "status": status,
        "batch_id": batch_id,
        "event_ts": _event_ts(now),
        "batch_start_ts": _opt_ts(start),
        "batch_end_ts": _opt_ts(end),
        "payload": _json(payload),
    }


def _progress_bounds(progress):
    """Derive (start_dt, end_dt) for a StreamingQueryProgress dict: start = its `timestamp` (the batch
    trigger time, an ISO-8601 string), end = start + `batchDuration` ms. Returns (None, None) if the
    timestamp is missing/unparseable (fail-soft; the columns are then NULL). batchDuration missing => end
    equals start (zero-length), which still records when the batch ran."""
    ts = progress.get("timestamp")
    if not isinstance(ts, str):
        return None, None
    try:
        # StreamingQueryProgress timestamps end in 'Z' (UTC); fromisoformat handles the offset forms.
        start = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None, None
    dur = progress.get("batchDuration")
    try:
        end = start + timedelta(milliseconds=float(dur)) if dur is not None else start
    except (TypeError, ValueError):
        end = start
    return start, end


def error_facts(exc):
    """The payload fields describing an exception: its type name and its message, capped at
    MAX_ERROR_MESSAGE_CHARS (with message_truncated set when it was cut). Shared by every error row so they
    all describe a failure the same way."""
    message = str(exc)
    facts = {"exception_type": type(exc).__name__, "message": message[:MAX_ERROR_MESSAGE_CHARS]}
    if len(message) > MAX_ERROR_MESSAGE_CHARS:
        facts["message_truncated"] = True
    return facts


def run_start_row(config_name, job_run_id, facts, start, now=None):
    """The `run_start` row: the run's identity and effective settings (`facts`: mode, es_index, view,
    source, trigger, connector version, ...), stored verbatim. `start` is the run's start wall clock."""
    return _row(config_name, job_run_id, "run_start", "started", None, facts, now, start=start)


def run_end_row(config_name, job_run_id, status, facts, start, end, now=None):
    """The `run_end` row: how the run ended (status success | error | stopped) plus `facts` (totals on
    success, error_facts on error), with the run's start and end wall clocks so its duration is direct."""
    return _row(config_name, job_run_id, "run_end", status, None, facts, now, start=start, end=end)


def batch_start_row(config_name, job_run_id, batch_id, facts, start, now=None):
    """The `batch_start` row, written immediately BEFORE the batch's data is sent to ES. `facts` carries
    whatever is known up front (at least the mode). With the log on, failing to write this row fails the
    batch before any data is sent."""
    return _row(config_name, job_run_id, "batch_start", "started", batch_id, facts, now, start=start)


def batch_end_row(config_name, job_run_id, batch_id, status, facts, start, end, now=None):
    """The `batch_end` row: the batch's OUTCOME. status success (facts = es_counts) or error (facts =
    error_facts), with the batch's start and end wall clocks."""
    return _row(config_name, job_run_id, "batch_end", status, batch_id, facts, now, start=start, end=end)


def batch_summary_row(config_name, job_run_id, batch_id, es=None, progress=None, status="success", now=None):
    """The `batch_summary` row: the batch's DIAGNOSTICS. `es` is es_write_summary(...) (the ES write
    rollup); `progress` is Spark's StreamingQueryProgress dict for the batch (streaming only; stored
    whole, so nothing the runtime reports is dropped). At least one must be present. status mirrors the
    batch's outcome: a streaming summary is only ever written for a committed batch (success), while a
    batch-mode write that returned diagnostics but then failed reconciliation is summarized as error.
    batch_start_ts /
    batch_end_ts come from the progress (trigger timestamp + batchDuration) when there is one, else from
    the es summary's own start/end."""
    if es is None and progress is None:
        raise ValueError("batch_summary needs an es summary, a progress report, or both")
    if es is not None and not isinstance(es, dict):
        raise ValueError(f"batch_summary es must be a dict, got {type(es).__name__}")
    if progress is not None and not isinstance(progress, dict):
        raise ValueError(f"batch_summary progress must be a dict, got {type(progress).__name__}")
    payload = {}
    start = end = None
    if es is not None:
        payload["es"] = es
    if progress is not None:
        payload["progress"] = progress
        start, end = _progress_bounds(progress)
    return _row(config_name, job_run_id, "batch_summary", status, batch_id, payload, now,
                start=start, end=end)


def es_counts(result):
    """The ES write COUNTS from a bulk_write result (what batch_end carries on success): written, deleted,
    errors, ignored, total_input. Raises ValueError on a non-dict result (a bulk_write that returned
    nothing usable is not a success to record)."""
    if not isinstance(result, dict):
        raise ValueError(f"bulk_write result must be a dict, got {type(result).__name__}")
    return {k: result.get(k) for k in ("written", "deleted", "errors", "ignored", "total_input")}


def es_write_summary(result, wall_ms=None):
    """The ES write DIAGNOSTICS for batch_summary, from a bulk_write result: the driver-side facts
    (collect_ms, merge_ms, num_partitions, and the driver-measured bulk_write wall time when given) plus,
    when bulk_stats is on, the cluster-wide `overall` rollup and the straggler `tail` facts. The rollups
    come from observability's bulk_stats_overall / bulk_stats_tail, the SAME computation behind the
    BULK_STATS log lines, so the table and the console agree. Per-partition detail is deliberately NOT
    stored (too granular for this log; it still prints when bulk_stats is on). Raises ValueError on a
    non-dict result."""
    if not isinstance(result, dict):
        raise ValueError(f"bulk_write result must be a dict, got {type(result).__name__}")
    parts = result.get("bulk_stats")
    out = {
        "collect_ms": result.get("collect_ms"),
        "merge_ms": result.get("merge_ms"),
        "num_partitions": len(parts) if isinstance(parts, list) else None,
        "bulk_write_wall_ms": wall_ms,
    }
    if isinstance(parts, list) and parts:
        out["overall"] = bulk_stats_overall(parts)
        tail = bulk_stats_tail(result)
        # collect_ms / merge_ms are already top-level above; keep only the partition facts in `tail`.
        out["tail"] = {k: v for k, v in tail.items() if k not in ("collect_ms", "merge_ms")}
    return out


def progress_batch_ids(progresses, last_batch_id):
    """The progress reports from `progresses` (StreamingQueryProgress dicts, as polled from
    query.recentProgress) that describe a batch which actually EXECUTED and is newer than
    `last_batch_id` (the highest batch id already recorded; None = none yet), returned in ascending batch
    order with one report per batch id.

    A high-water mark rather than a set of seen ids: within one query, Spark's batch ids only increase, and
    a mark stays constant-size on a stream that runs for months. A report counts only when it has an
    integer batchId above the mark and its durationMs carries addBatch: Spark also posts progress for idle
    triggers (no new data, so no batch ran), and those carry no addBatch. If one batch id appears twice,
    the first executed report wins. Non-dict entries are skipped."""
    by_id = {}
    for p in progresses or []:
        if not isinstance(p, dict):
            continue
        bid = p.get("batchId")
        if isinstance(bid, bool) or not isinstance(bid, int) or bid in by_id:
            continue
        if last_batch_id is not None and bid <= last_batch_id:
            continue
        duration = p.get("durationMs")
        if not isinstance(duration, dict) or "addBatch" not in duration:
            continue
        by_id[bid] = p
    return [by_id[b] for b in sorted(by_id)]


def leftover_relay_ids(names, below=None):
    """The batch ids among relay directory entry `names` (as listed; a trailing "/" is ignored) that are
    below `below` (None = all), ascending. Non-numeric names are skipped. The streaming runner writes one
    relay directory per batch that ended successfully and deletes it once that batch's batch_summary is
    written, so whatever is left below the batch being recorded is a batch whose progress report was never
    seen (evicted from query.recentProgress's bounded buffer, or a read that kept failing). Those still owe
    a batch_summary, written from the relayed ES diagnostics alone, so no batch is silently skipped."""
    out = []
    for name in names or []:
        n = str(name).rstrip("/")
        if not n.isdigit():
            continue
        bid = int(n)
        if below is None or bid < below:
            out.append(bid)
    return sorted(out)


def validate_table_name(name, where="monitoring_log_table"):
    """Canonicalize and fail-closed-validate a monitoring table name. Returns the stripped
    `catalog.schema.table` when valid; raises ValueError otherwise. Requires EXACTLY three dot-separated
    parts, each matching the strict identifier allow-list (_NAME_PART), because the name is interpolated
    into a CREATE TABLE / INSERT statement - a two-part or oddly-charactered name is rejected, never
    escaped. `where` labels the error for the caller (a widget name or config source)."""
    if not isinstance(name, str):
        raise ValueError(f"{where}: must be a string, got {type(name).__name__}")
    stripped = name.strip()
    if not stripped:
        raise ValueError(f"{where}: must not be blank")
    parts = stripped.split(".")
    if len(parts) != 3 or not all(_NAME_PART.match(p) for p in parts):
        raise ValueError(
            f"{where}: must be a three-part catalog.schema.table of simple identifiers "
            f"([A-Za-z_][A-Za-z0-9_]*), got {name!r}"
        )
    return stripped


def create_table_sql(table_name):
    """The idempotent CREATE TABLE statement the `_log table create` job runs. Validates the name
    (fail-closed via validate_table_name), then builds `CREATE TABLE IF NOT EXISTS <name> (...) USING
    DELTA CLUSTER BY (...)` from MONITORING_TABLE_COLUMNS (the single schema source) plus auto-optimize
    table properties (the sink appends one small file per micro-batch, so predictive/auto compaction keeps
    the table tidy). CLUSTER BY (CLUSTER_BY_COLUMNS) adds data skipping for the time/config queries this
    table serves as it grows. IF NOT EXISTS makes re-running the job a safe no-op. (CLUSTER BY is a
    table_clause that precedes TBLPROPERTIES in the Databricks SQL grammar.)"""
    canonical = validate_table_name(table_name)
    cols = ",\n  ".join(f"{name} {sql_type}" for name, sql_type in MONITORING_TABLE_COLUMNS)
    cluster_by = ", ".join(CLUSTER_BY_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {canonical} (\n  {cols}\n) USING DELTA\n"
        f"CLUSTER BY ({cluster_by})\n"
        "TBLPROPERTIES (\n"
        "  'delta.autoOptimize.optimizeWrite' = 'true',\n"
        "  'delta.autoOptimize.autoCompact' = 'true'\n"
        ")"
    )


def missing_columns(existing_names):
    """The MONITORING_TABLE_COLUMNS entries whose column name is NOT already present, as a list of
    (name, sql_type). ADDITIVE allow-list and order-insensitive: it only ever reports columns to ADD, never
    considers dropping/renaming an existing one, so applying its result can never lose data. `existing_names`
    is the current table's column names (any iterable). Drives the re-runnable schema migration in the
    `_log table create` job: add exactly the columns a newer schema introduced."""
    have = set(existing_names)
    return [(name, sql_type) for name, sql_type in MONITORING_TABLE_COLUMNS if name not in have]


def alter_add_columns_sql(table_name, cols):
    """`ALTER TABLE <name> ADD COLUMNS (name type, ...)` for the given (name, sql_type) list (typically the
    output of missing_columns). Validates the name fail-closed. Returns None when `cols` is empty (nothing
    to add => the caller skips). ADD COLUMNS is purely additive (existing rows get NULL for the new
    columns); it never rewrites or replaces data."""
    canonical = validate_table_name(table_name)
    if not cols:
        return None
    added = ", ".join(f"{name} {sql_type}" for name, sql_type in cols)
    return f"ALTER TABLE {canonical} ADD COLUMNS ({added})"


def alter_cluster_by_sql(table_name):
    """`ALTER TABLE <name> CLUSTER BY (...)` to (idempotently) set liquid-clustering columns on an existing
    table that predates clustering. Validates the name fail-closed. Setting clustering does NOT rewrite
    existing files; a later OPTIMIZE (see optimize_sql, run by the prune job) reclusters them."""
    canonical = validate_table_name(table_name)
    cluster_by = ", ".join(CLUSTER_BY_COLUMNS)
    return f"ALTER TABLE {canonical} CLUSTER BY ({cluster_by})"


def prune_sql(table_name, retention_days):
    """`DELETE FROM <name> WHERE ingest_ts < current_timestamp() - INTERVAL <n> DAYS` to enforce retention
    on the monitoring table (it grows unbounded otherwise: several rows per batch, and an always-on stream
    runs a batch every trigger). Validates the name fail-closed. Returns None when retention_days <= 0 (retention
    DISABLED => keep all rows; the caller skips the DELETE). retention_days is coerced to a non-negative
    int; a non-numeric value raises (fail-closed, since it is interpolated into SQL).

    Retention is on ingest_ts (write time), deliberately NOT the clustered event_ts. ingest_ts is ALWAYS
    set (the writer stamps current_timestamp() at append), whereas event_ts can be NULL from a fail-soft
    builder - and a NULL-event_ts row would then never age out, leaking forever; "age since written" is
    also the correct retention semantic. The cost is that this DELETE predicate is not a clustering key, so
    it does not get event_ts clustered-file skipping; that is acceptable for a once-a-day prune of a
    retention-bounded table (ingest_ts and event_ts are near-identical for rows this sink writes, since
    builders stamp event_ts at write time, so any skipping would be approximate anyway)."""
    canonical = validate_table_name(table_name)
    days = int(retention_days)
    if days <= 0:
        return None
    return f"DELETE FROM {canonical} WHERE ingest_ts < current_timestamp() - INTERVAL {days} DAYS"


def optimize_sql(table_name):
    """`OPTIMIZE <name>`: compacts small files and (on a liquid-clustered table) reclusters data. Run by
    the prune job after a retention DELETE, and to recluster a table that only just had CLUSTER BY set.
    Validates the name fail-closed."""
    canonical = validate_table_name(table_name)
    return f"OPTIMIZE {canonical}"


def vacuum_sql(table_name):
    """`VACUUM <name>`: reclaims storage from files removed by DELETE/OPTIMIZE past the Delta retention
    threshold (default 7 days). Run by the prune job after OPTIMIZE. Validates the name fail-closed."""
    canonical = validate_table_name(table_name)
    return f"VACUUM {canonical}"

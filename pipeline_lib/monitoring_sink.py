"""Pure logic for the OPTIONAL durable monitoring log: turn what run_index_pipeline.py knows about a run
and its batches into rows for one shared UC Delta table, and own that table's schema and its SQL.

Why this module exists, and what it deliberately is NOT:
- It is the SINGLE SOURCE OF TRUTH for the monitoring table's columns (MONITORING_TABLE_COLUMNS) and its
  closed row vocabulary (RECORD_TYPES x STATUSES). Both the `_log table create` job (which runs the
  CREATE, or adds a newly introduced column) and the writer (run_index_pipeline.py, which appends) import that one
  definition, so the DDL and the rows can never drift apart.
- It is PURE: no Spark, no dbutils, no Databricks. Every function here is unit-testable off-cluster
  (plain pytest), exactly like pipeline_lib/observability.py. The notebooks own all Spark I/O.

THE ROW MODEL. Two levels, each with a start and an end, plus Spark's report for streaming batches:
- run_start / run_end: one pair per job run (run_end is absent when the run was killed or cancelled,
  which is itself the signal: a run_start with no run_end is a run that did not end in-process).
- batch_start / batch_end: one pair per batch. A batch-mode run is exactly ONE batch (batch_id
  BATCH_MODE_BATCH_ID); a streaming run has one per micro-batch (the micro-batch id). batch_end is the
  OUTCOME and the ES DIAGNOSTICS, written inline as the batch finishes, in both modes: status success
  (ES counts + `es`, the write rollup from es_write_summary) or error (the error, plus counts/diagnostics
  when the write returned). A batch_start with no batch_end is a batch that never finished.
- batch_summary (streaming only): Spark's StreamingQueryProgress for the batch, recorded when the notebook's
  wait loop sees it, plus the newest source version its batch_start read (source_latest), so caught up versus
  behind is answerable from this one row (see source_latest_facts and the source_* columns). Spark publishes it only after the batch commits, asynchronously, and keeps only the
  last few in memory, so it is best-effort by nature: a batch_end with no batch_summary is a batch whose
  report was lost (e.g. a cancel moments after it committed). Batch mode has no Spark progress, so no
  batch_summary.

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

# The monitoring table's columns, in order, as (name, sql_type, comment). This ONE tuple drives the CREATE TABLE
# statement (create_table_sql), the column comments (set at create, and by ADD COLUMNS for a column added later),
# the row shape (ROW_FIELDS) and the writer's DataFrame schema, so the schema is defined exactly once. payload is
# VARIANT (semi-structured, queryable with `payload:field` paths in SQL); it needs DBR 15.3+ / recent serverless,
# which every target here runs. The comments are stored on the table, so they are written for someone reading the
# table in Catalog Explorer: what the column holds and on which rows (see _COMMENT for the allowed characters).
MONITORING_TABLE_COLUMNS = (
    ("config_name", "STRING", "Pipeline config that wrote the row."),
    ("job_run_id", "STRING",
     "Databricks job run id ({{job.run_id}}). Shared by every retry of the task, so it groups all attempts of "
     "one job run."),
    ("task_run_id", "STRING",
     "Databricks task run id ({{task.run_id}}). One per task attempt, so it tells the retries of a job run "
     "apart. NULL on an interactive run."),
    ("record_type", "STRING", "Row kind: run_start, run_end, batch_start, batch_end or batch_summary."),
    ("status", "STRING",
     "started (run_start, batch_start), success or error (run_end, batch_end), stopped (run_end of a "
     "continuous stream that ended without error), success (batch_summary)."),
    ("batch_id", "BIGINT", "Batch id: the streaming micro-batch id, or 0 for a batch-mode run. NULL on run rows."),
    ("event_ts", "TIMESTAMP",
     "UTC time the event this row records happened (the start, the end, or when the progress report was "
     "seen). Clustering key."),
    ("start_ts", "TIMESTAMP",
     "UTC start of the run (run rows) or batch (batch rows) this row describes. On batch_summary, the Spark "
     "trigger time."),
    ("end_ts", "TIMESTAMP",
     "UTC end of the run or batch this row describes. On batch_summary, trigger time plus batchDuration. NULL "
     "on run_start and batch_start."),
    ("docs_written", "BIGINT",
     "Documents written to Elasticsearch: by the batch (batch_end) or by the whole run (run_end). NULL on "
     "other rows and when not known."),
    ("error_type", "STRING", "Exception class name on status error rows (run_end, batch_end). NULL otherwise."),
    ("error_message", "STRING",
     "Exception message on status error rows, capped at 16000 characters (payload message_truncated marks a "
     "cut). NULL otherwise."),
    ("files_outstanding", "BIGINT",
     "batch_summary only: source files not yet processed after this batch (Spark numFilesOutstanding). The "
     "streaming backlog."),
    ("bytes_outstanding", "BIGINT",
     "batch_summary only: source bytes not yet processed after this batch (Spark numBytesOutstanding). The "
     "streaming backlog."),
    ("source_latest_version", "BIGINT",
     "batch_start and batch_summary: newest commit version of the source table, read at batch start. Behind when "
     "this is at or past source_end_version and files_outstanding is above 0. NULL when the read failed."),
    ("source_latest_ts", "TIMESTAMP",
     "batch_start and batch_summary: commit time (UTC) of source_latest_version, from the source table history."),
    ("source_end_version", "BIGINT",
     "batch_summary only: reservoirVersion of the end offset Spark reports for the batch. With source_end_index "
     "-1 every source version below this was sent; otherwise this version was sent through that file index."),
    ("source_end_index", "BIGINT",
     "batch_summary only: index of the end offset. -1 means the batch ended on a version boundary; 0 or more "
     "means it stopped inside version source_end_version, after that file."),
    ("payload", "VARIANT", "Everything else the row records, by record_type (query with payload:field paths)."),
    ("logged_ts", "TIMESTAMP",
     "UTC time the row was committed to this table (set by the writer). Retention (the prune job) is on this "
     "column."),
)

# Comments are interpolated into DDL as '...' literals, so they are an ALLOW-LIST of plain characters: no quote
# or backslash can reach the SQL. The comments are our own constants; a violation is a bug and fails closed.
_COMMENT = re.compile(r"^[A-Za-z0-9 .,:;()_{}+/-]+$")



class _ParseJson:
    """A payload path step: the value is a JSON string (Spark reports source offsets that way) to parse before
    the next step. An already-parsed object passes through; anything else is a miss (NULL)."""

    def __repr__(self):
        return "PARSE_JSON"


PARSE_JSON = _ParseJson()

# Columns surfaced from the payload, so common questions do not need payload paths, as (column, record_types it
# applies to (None = any), status it applies to (None = any), payload paths tried in order (a str key, an int
# list index, or PARSE_JSON), cast). The second docs_written path is the streaming run_end's key.
_END_OFFSET = ("progress", "sources", 0, "endOffset", PARSE_JSON)
SURFACED_COLUMNS = (
    ("docs_written", ("batch_end", "run_end"), None, (("written",), ("rows_pushed",)), "bigint"),
    ("error_type", None, "error", (("exception_type",),), "string"),
    ("error_message", None, "error", (("message",),), "string"),
    ("files_outstanding", ("batch_summary",), None,
     (("progress", "sources", 0, "metrics", "numFilesOutstanding"),), "bigint"),
    ("bytes_outstanding", ("batch_summary",), None,
     (("progress", "sources", 0, "metrics", "numBytesOutstanding"),), "bigint"),
    ("source_latest_version", ("batch_start", "batch_summary"), None, (("source_latest", "version"),), "bigint"),
    ("source_latest_ts", ("batch_start", "batch_summary"), None, (("source_latest", "timestamp"),), "timestamp"),
    ("source_end_version", ("batch_summary",), None, ((*_END_OFFSET, "reservoirVersion"),), "bigint"),
    ("source_end_index", ("batch_summary",), None, ((*_END_OFFSET, "index"),), "bigint"),
)

# The allow-list of record_type values (see THE ROW MODEL above). A row builder only ever emits one of
# these; the writer and any reader can trust the set is closed. (Adding a type is a deliberate change.)
RECORD_TYPES = (
    "run_start",      # one per run, first row written: identity + effective settings
    "run_end",        # one per run that ended in-process: success | error | stopped, totals or the error
    "batch_start",    # one per batch, written immediately BEFORE the batch's data is sent to ES
    "batch_end",      # one per batch that ended: success (ES counts) | error (the exception)
    "batch_summary",  # streaming only: Spark's progress report for a batch, when it was seen
)

# The allow-list of status values, and which statuses each record_type may carry. `stopped` is a
# continuous stream whose query ended WITHOUT an error while the notebook kept running, which is
# neither a success nor a failure of the export.
STATUSES = ("started", "success", "error", "stopped")
_ALLOWED_STATUS = {
    "run_start": ("started",),
    "run_end": ("success", "error", "stopped"),
    "batch_start": ("started",),
    "batch_end": ("success", "error"),
    "batch_summary": ("success",),
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
# logged_ts (the writer stamps logged_ts with the Spark current_timestamp() at append time, so a pure
# builder never invents it). assert_columns_consistent() enforces the relationship so the two lists
# cannot drift.
ROW_FIELDS = ("config_name", "job_run_id", "task_run_id", "record_type", "status", "batch_id", "event_ts",
              "start_ts", "end_ts", "docs_written", "error_type", "error_message", "files_outstanding",
              "bytes_outstanding", "source_latest_version", "source_latest_ts", "source_end_version",
              "source_end_index", "payload")

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
    logged_ts, so the DDL (MONITORING_TABLE_COLUMNS) and the row shape (ROW_FIELDS) can never drift, and that
    every surfaced column names a real column. Called by tests; cheap enough to be a plain assert."""
    col_names = [name for name, _type, _comment in MONITORING_TABLE_COLUMNS]
    assert col_names[-1] == "logged_ts", "logged_ts must be the last (writer-supplied) column"
    assert tuple(col_names[:-1]) == ROW_FIELDS, "ROW_FIELDS must equal table columns minus logged_ts"
    assert all(c in col_names for c, *_rest in SURFACED_COLUMNS), "every surfaced column must be a table column"


def _fmt_ts(dt):
    """Format a datetime to the stored UTC string `YYYY-MM-DDTHH:MM:SS.ffffff`. A tz-aware dt is converted
    to UTC; a naive one is assumed already-UTC (the same contract observability._ts_token uses). Fail-soft:
    returns None on any error, so a builder never raises on a timestamp."""
    try:
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        # The year is formatted by hand: strftime's %Y is not zero-padded below 1000 on every platform (glibc).
        return f"{dt.year:04d}" + dt.strftime(_TS_FORMAT_AFTER_YEAR)
    except Exception:
        return None


def _event_ts(now=None):
    """The event_ts string a builder stores. `now=None` reads the current UTC time (so event_ts is always
    populated); tests pass a fixed datetime. Delegates formatting to _fmt_ts."""
    return _fmt_ts(now if now is not None else datetime.now(timezone.utc))


def _opt_ts(dt):
    """Format an OPTIONAL timing (start_ts / end_ts). Unlike _event_ts, `None` means NULL (not
    'now'): a row that has no meaningful start/end stores NULL rather than inventing the current time."""
    return None if dt is None else _fmt_ts(dt)


def _json(payload):
    """Serialize a payload dict to a compact, stable JSON string for the VARIANT column. sort_keys keeps
    it deterministic (so tests and diffs are stable); default=str means an odd value (a datetime, a
    Decimal) is stringified rather than raising - fail-soft, a diagnostic row must never break a write."""
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


# The strict cast rule for the surfaced columns: "bigint" takes a JSON integer, or a string of plain digits (Spark
# reports its metrics as strings), that fits in a signed 64-bit integer; "string" takes a JSON string;
# "timestamp" takes a string in the stored format _fmt_ts writes (a real date and time). Anything else (a
# boolean, a fraction, an object, an out-of-range number, another date format) is NULL rather than a guess.
_INTEGER_STRING = r"^-?[0-9]+$"
_BIGINT_MIN, _BIGINT_MAX = -(2 ** 63), 2 ** 63 - 1
_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
_TS_FORMAT_AFTER_YEAR = "-%m-%dT%H:%M:%S.%f"  # _fmt_ts writes the year itself, then this
_TS_STRING = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}$"


def _cast_value(value, cast):
    """`value` under the strict cast rule above, or None."""
    if cast == "string":
        return value if isinstance(value, str) else None
    if cast == "timestamp":
        if not isinstance(value, str) or not re.match(_TS_STRING, value):
            return None
        try:
            datetime.strptime(value, _TS_FORMAT)
        except ValueError:
            return None
        return value
    if isinstance(value, str) and re.match(_INTEGER_STRING, value):
        value = int(value)
    if isinstance(value, int) and not isinstance(value, bool) and _BIGINT_MIN <= value <= _BIGINT_MAX:
        return value
    return None


def _payload_value(payload, paths, cast):
    """A surfaced column's value: the first of `paths` whose value in `payload` passes the strict cast rule.
    FAIL-SOFT like every diagnostic: a missing path or a value that does
    not cast is None (NULL), never an error."""
    for path in paths:
        value = payload
        for key in path:
            if key is PARSE_JSON:
                value = _parse_json(value)
            elif isinstance(key, int):
                value = value[key] if isinstance(value, list) and len(value) > key else None
            else:
                value = value.get(key) if isinstance(value, dict) else None
        value = _cast_value(value, cast)
        if value is not None:
            return value
    return None


def _parse_json(value):
    """A PARSE_JSON step: a JSON string parsed, an already-parsed dict or list as is, anything else (or a string
    that is not JSON) None. FAIL-SOFT like every surfaced value."""
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    try:
        return json.loads(value)
    except (ValueError, RecursionError):  # not JSON, or nested too deep to parse
        return None


def _surfaced(record_type, status, payload):
    """The SURFACED_COLUMNS values for one row: each column's payload value on the rows it applies to, else None."""
    out = {}
    for column, record_types, only_status, paths, cast in SURFACED_COLUMNS:
        applies = ((record_types is None or record_type in record_types)
                   and (only_status is None or status == only_status))
        out[column] = _payload_value(payload, paths, cast) if applies else None
    return out


def _row(config_name, job_run_id, task_run_id, record_type, status, batch_id, payload, now=None,
         start=None, end=None):
    """Assemble one row dict keyed by ROW_FIELDS. FAIL-CLOSED: record_type must be in RECORD_TYPES, status
    must be one _ALLOWED_STATUS permits for it, payload must be a dict, and a batch row needs an integer
    batch_id (a run row must have none). Any violation raises ValueError: a malformed row is a bug, and a
    silently dropped row is the missing log entry this module exists to prevent. start/end are OPTIONAL
    datetimes (None => NULL) stored as start_ts / end_ts (the run's or batch's wall clock). task_run_id ""
    (an interactive run) is stored as NULL. The SURFACED_COLUMNS are copied from the payload (_surfaced)."""
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
        "task_run_id": task_run_id or None,
        "record_type": record_type,
        "status": status,
        "batch_id": batch_id,
        "event_ts": _event_ts(now),
        "start_ts": _opt_ts(start),
        "end_ts": _opt_ts(end),
        **_surfaced(record_type, status, payload),
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


def run_start_row(config_name, job_run_id, task_run_id, facts, start, now=None):
    """The `run_start` row: the run's identity and effective settings (`facts`: mode, es_index, view,
    source, trigger, connector version, ...), stored verbatim. `start` is the run's start wall clock."""
    return _row(config_name, job_run_id, task_run_id, "run_start", "started", None, facts, now, start=start)


def run_end_row(config_name, job_run_id, task_run_id, status, facts, start, end, now=None):
    """The `run_end` row: how the run ended (status success | error | stopped) plus `facts` (totals on
    success, error_facts on error), with the run's start and end wall clocks so its duration is direct."""
    return _row(config_name, job_run_id, task_run_id, "run_end", status, None, facts, now, start=start, end=end)


def batch_start_row(config_name, job_run_id, task_run_id, batch_id, facts, start, now=None):
    """The `batch_start` row, written immediately BEFORE the batch's data is sent to ES. `facts` carries
    whatever is known up front (at least the mode). With the log on, failing to write this row fails the
    batch before any data is sent."""
    return _row(config_name, job_run_id, task_run_id, "batch_start", "started", batch_id, facts, now, start=start)


def batch_end_row(config_name, job_run_id, task_run_id, batch_id, status, facts, start, end, now=None):
    """The `batch_end` row: the batch's OUTCOME and ES DIAGNOSTICS. status success (facts =
    batch_success_facts(...): ES counts + the `es` write rollup) or error (facts = error_facts, plus the
    counts/diagnostics when the write returned), with the batch's start and end wall clocks."""
    return _row(config_name, job_run_id, task_run_id, "batch_end", status, batch_id, facts, now, start=start, end=end)


def batch_summary_row(config_name, job_run_id, task_run_id, batch_id, progress, now=None, source_latest=None):
    """The `batch_summary` row (streaming only): Spark's StreamingQueryProgress dict for the batch, stored
    WHOLE (backlog, step durations, rates, offsets, nothing the runtime reports is dropped).
    start_ts / end_ts are Spark's trigger timestamp and trigger + batchDuration (Spark's view of the batch;
    the batch's own wall clock is on its batch_start / batch_end rows). `source_latest` is the batch's
    source_latest facts relayed from its batch_start (None when the relay had nothing), stored beside the
    progress so the newest version at batch start and the end offset sit on one row."""
    if not isinstance(progress, dict):
        raise ValueError(f"batch_summary progress must be a dict, got {type(progress).__name__}")
    start, end = _progress_bounds(progress)
    payload = {"progress": progress}
    if source_latest is not None:
        payload["source_latest"] = source_latest
    return _row(config_name, job_run_id, task_run_id, "batch_summary", "success", batch_id, payload, now,
                start=start, end=end)


def source_latest_facts(version, timestamp):
    """The `source_latest` payload field: the source table's newest commit `version` (a non-negative int) and that
    commit's `timestamp` (a datetime; naive means UTC), read at batch start. Raises ValueError on anything else, so
    a caller that cannot read a usable version records the error instead of a wrong number."""
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise ValueError(f"source version must be a non-negative int, got {version!r}")
    if not isinstance(timestamp, datetime):
        raise ValueError(f"source commit timestamp must be a datetime, got {type(timestamp).__name__}")
    return {"version": version, "timestamp": _fmt_ts(timestamp)}


def batch_success_facts(result, wall_ms=None):
    """The `batch_end` payload for a successful batch: the ES counts (es_counts) plus `es`, the ES write
    rollup (es_write_summary). One helper so batch and streaming batches carry identical batch_end rows."""
    return {**es_counts(result), "es": es_write_summary(result, wall_ms=wall_ms)}


def es_counts(result):
    """The ES write COUNTS from a bulk_write result (what batch_end carries on success): written, deleted,
    errors, ignored, total_input. Raises ValueError on a non-dict result (a bulk_write that returned
    nothing usable is not a success to record)."""
    if not isinstance(result, dict):
        raise ValueError(f"bulk_write result must be a dict, got {type(result).__name__}")
    return {k: result.get(k) for k in ("written", "deleted", "errors", "ignored", "total_input")}


def es_write_summary(result, wall_ms=None):
    """The ES write DIAGNOSTICS (batch_end's `es` field), from a bulk_write result: the driver-side facts
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
    DELTA CLUSTER BY (...)` from MONITORING_TABLE_COLUMNS (the single schema source, each column with its
    COMMENT) plus auto-optimize
    table properties (the sink appends one small file per micro-batch, so predictive/auto compaction keeps
    the table tidy). CLUSTER BY (CLUSTER_BY_COLUMNS) adds data skipping for the time/config queries this
    table serves as it grows. IF NOT EXISTS makes re-running the job a safe no-op. (CLUSTER BY is a
    table_clause that precedes TBLPROPERTIES in the Databricks SQL grammar.)"""
    canonical = validate_table_name(table_name)
    cols = ",\n  ".join(_column_sql(*col) for col in MONITORING_TABLE_COLUMNS)
    cluster_by = ", ".join(CLUSTER_BY_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {canonical} (\n  {cols}\n) USING DELTA\n"
        f"CLUSTER BY ({cluster_by})\n"
        "TBLPROPERTIES (\n"
        "  'delta.autoOptimize.optimizeWrite' = 'true',\n"
        "  'delta.autoOptimize.autoCompact' = 'true'\n"
        ")"
    )


def _sql_comment(comment):
    """`comment` as a SQL string literal. FAIL-CLOSED allow-list (_COMMENT): a quote or backslash can never
    reach the DDL."""
    if not isinstance(comment, str) or not _COMMENT.match(comment):
        raise ValueError(f"column comment must match {_COMMENT.pattern}, got {comment!r}")
    return f"'{comment}'"


def _column_sql(name, sql_type, comment):
    """One column definition: `name TYPE COMMENT '...'`."""
    return f"{name} {sql_type} COMMENT {_sql_comment(comment)}"


def missing_columns(existing_names):
    """The MONITORING_TABLE_COLUMNS entries whose column name is NOT already present, as a list of
    (name, sql_type, comment). ADDITIVE allow-list and order-insensitive: it only ever reports columns to ADD, never
    considers dropping/renaming an existing one, so applying its result can never lose data. `existing_names`
    is the current table's column names (any iterable). Drives the `_log table create` job's add-missing-columns
    step: when a future build adds a column to MONITORING_TABLE_COLUMNS, a re-run adds exactly that column."""
    have = set(existing_names)
    return [col for col in MONITORING_TABLE_COLUMNS if col[0] not in have]


def alter_add_columns_sql(table_name, cols):
    """`ALTER TABLE <name> ADD COLUMNS (name type COMMENT '...', ...)` for the given (name, sql_type, comment)
    list (typically the output of missing_columns). Validates the name fail-closed. Returns None when `cols` is
    empty (nothing to add => the caller skips). ADD COLUMNS is purely additive (existing rows get NULL for the new
    columns); it never rewrites or replaces data."""
    canonical = validate_table_name(table_name)
    if not cols:
        return None
    added = ", ".join(_column_sql(name, sql_type, comment) for name, sql_type, comment in cols)
    return f"ALTER TABLE {canonical} ADD COLUMNS ({added})"


def prune_sql(table_name, retention_days):
    """`DELETE FROM <name> WHERE logged_ts < current_timestamp() - INTERVAL <n> DAYS` to enforce retention
    on the monitoring table (it grows unbounded otherwise: several rows per batch, and an always-on stream
    runs a batch every trigger). Validates the name fail-closed. Returns None when retention_days <= 0 (retention
    DISABLED => keep all rows; the caller skips the DELETE). retention_days is coerced to a non-negative
    int; a non-numeric value raises (fail-closed, since it is interpolated into SQL).

    Retention is on logged_ts (write time), deliberately NOT the clustered event_ts. logged_ts is ALWAYS
    set (the writer stamps current_timestamp() at append), whereas event_ts can be NULL from a fail-soft
    builder - and a NULL-event_ts row would then never age out, leaking forever; "age since written" is
    also the correct retention semantic. The cost is that this DELETE predicate is not a clustering key, so
    it does not get event_ts clustered-file skipping; that is acceptable for a once-a-day prune of a
    retention-bounded table (logged_ts and event_ts are near-identical for rows this sink writes, since
    builders stamp event_ts at write time, so any skipping would be approximate anyway)."""
    canonical = validate_table_name(table_name)
    days = int(retention_days)
    if days <= 0:
        return None
    return f"DELETE FROM {canonical} WHERE logged_ts < current_timestamp() - INTERVAL {days} DAYS"


def optimize_sql(table_name):
    """`OPTIMIZE <name>`: compacts small files and (on a liquid-clustered table) reclusters data. Run by
    the prune job after a retention DELETE.
    Validates the name fail-closed."""
    canonical = validate_table_name(table_name)
    return f"OPTIMIZE {canonical}"


def vacuum_sql(table_name):
    """`VACUUM <name>`: reclaims storage from files removed by DELETE/OPTIMIZE past the Delta retention
    threshold (default 7 days). Run by the prune job after OPTIMIZE. Validates the name fail-closed."""
    canonical = validate_table_name(table_name)
    return f"VACUUM {canonical}"

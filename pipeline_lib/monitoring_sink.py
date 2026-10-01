"""Pure logic for the OPTIONAL durable monitoring sink: turn the metrics run_index_pipeline.py already
computes (StreamingQueryProgress + the connector's per-partition bulk_stats + per-run summary facts)
into rows for one shared UC Delta table, and own that table's schema and its CREATE statement.

Why this module exists, and what it deliberately is NOT:
- It is the SINGLE SOURCE OF TRUTH for the monitoring table's columns (MONITORING_TABLE_COLUMNS). Both
  the `_log table create` job (notebooks/log_table_create.py, which runs the CREATE) and the writer
  (notebooks/run_index_pipeline.py, which appends) import that one definition, so the DDL and the rows
  can never drift apart.
- It is PURE: no Spark, no dbutils, no Databricks. Every function here is unit-testable off-cluster
  (plain pytest), exactly like pipeline_lib/observability.py. The notebooks own all Spark I/O.
- It stores ATOMIC rows, not the console rollups. observability.format_bulk_stats computes an `overall`
  line (weighted-mean rtt, cluster totals) and format_tail_summary computes a straggler line; those are
  aggregations OVER the per-partition dicts. Re-computing them here would re-type values from their
  source (the per-partition dicts), so instead each partition dict is stored raw as its own row and the
  overall/tail views are derived in SQL. Nothing is lost: the rollups are exactly recoverable from the
  bulk_stats_partition rows.

DESIGN INVARIANT - the sink is OBSERVABILITY ONLY, like observability.py. The row builders are called
from the export path, so they are FAIL-SOFT: an unusable (non-dict) input yields None (the caller skips
it) rather than raising, and payload serialization never raises (default=str). A monitoring fault must
never disturb a write.
"""
import json
import re
from datetime import datetime, timedelta, timezone

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
    ("batch_id", "BIGINT"),        # streaming micro-batch id; NULL for batch runs and run_summary rows
    ("event_ts", "TIMESTAMP"),     # UTC wall-clock the row is ABOUT (batch/emit time)
    ("batch_start_ts", "TIMESTAMP"),  # UTC wall-clock the batch/run STARTED; NULL where not applicable
    ("batch_end_ts", "TIMESTAMP"),    # UTC wall-clock the batch/run ENDED; NULL where not applicable
    ("payload", "VARIANT"),        # the type-specific fields, verbatim from the source dict
    ("ingest_ts", "TIMESTAMP"),    # UTC wall-clock the row was WRITTEN (writer supplies via current_timestamp())
)

# The allow-list of record_type values. A row builder only ever emits one of these; the writer and any
# reader can trust the set is closed. (Adding a type is a deliberate change here, never an accident.)
RECORD_TYPES = (
    "stream_progress",       # one StreamingQueryProgress (streaming only), payload = the progress dict
    "bulk_stats_partition",  # one per DataFrame partition, payload = that partition's raw bulk_stats dict
    "bulk_stats_batch",      # driver-side facts not in any partition (collect_ms/merge_ms/written)
    "run_summary",           # one per run: streaming_start, es_index, versions, totals
    "run_error",             # one per FAILED run: the exception that ended it (timeouts included)
)

# The fields a row builder emits, in order. This is MONITORING_TABLE_COLUMNS MINUS the writer-supplied
# ingest_ts (the writer stamps ingest_ts with the Spark current_timestamp() at append time, so a pure
# builder never invents it). assert_columns_consistent() enforces the relationship so the two lists
# cannot drift.
ROW_FIELDS = ("config_name", "job_run_id", "record_type", "batch_id", "event_ts",
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


def _row(config_name, job_run_id, record_type, batch_id, payload, now=None,
         batch_start=None, batch_end=None):
    """Assemble one row dict keyed by ROW_FIELDS. record_type is asserted to be in the allow-list (a
    programming error if not, so it raises here in tests, but callers only ever pass a literal). batch_id
    is coerced to int or None. payload is JSON-serialized. batch_start/batch_end are OPTIONAL datetimes
    (None => NULL) for the batch/run start and end wall clocks."""
    if record_type not in RECORD_TYPES:
        raise ValueError(f"unknown record_type {record_type!r}; allowed: {', '.join(RECORD_TYPES)}")
    try:
        bid = int(batch_id) if batch_id is not None else None
    except (TypeError, ValueError):
        bid = None
    return {
        "config_name": config_name,
        "job_run_id": job_run_id,
        "record_type": record_type,
        "batch_id": bid,
        "event_ts": _event_ts(now),
        "batch_start_ts": _opt_ts(batch_start),
        "batch_end_ts": _opt_ts(batch_end),
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


def progress_row(progress, config_name, job_run_id, now=None):
    """One `stream_progress` row from a StreamingQueryProgress dict (as parsed in the listener). payload
    is the WHOLE progress dict (full fidelity - durationMs breakdown, per-source backlog, offsets), so
    nothing the runtime emits is dropped. batch_id is taken from progress['batchId']; batch_start_ts/
    batch_end_ts are derived from the progress timestamp + batchDuration. Returns None for a non-dict input
    (fail-soft; the caller skips it)."""
    if not isinstance(progress, dict):
        return None
    start, end = _progress_bounds(progress)
    return _row(config_name, job_run_id, "stream_progress", progress.get("batchId"), progress, now,
                batch_start=start, batch_end=end)


def bulk_stats_partition_rows(result, config_name, job_run_id, batch_id, now=None):
    """One `bulk_stats_partition` row PER partition, payload = that partition's raw bulk_stats dict with
    a `partition` index added. `result` is the connector's bulk_write return dict (carries
    result['bulk_stats'], a list of per-partition dicts, when EsWriteConfig bulk_stats is on). Stores the
    per-partition dicts verbatim so every field the connector emits is preserved without re-typing;
    overall/tail rollups are derived from these rows in SQL. Returns [] when no bulk_stats are present
    (diagnostics off, or an empty batch) or the input is unusable (fail-soft)."""
    if not isinstance(result, dict):
        return []
    parts = result.get("bulk_stats")
    if not isinstance(parts, list):
        return []
    rows = []
    for i, part in enumerate(parts):
        payload = dict(part) if isinstance(part, dict) else {"unparseable": type(part).__name__}
        payload["partition"] = i
        rows.append(_row(config_name, job_run_id, "bulk_stats_partition", batch_id, payload, now))
    return rows


def bulk_stats_batch_row(result, config_name, job_run_id, batch_id, now=None,
                         batch_start=None, batch_end=None):
    """One `bulk_stats_batch` row carrying the DRIVER-side facts that are NOT in any partition:
    collect_ms and merge_ms (Spark result finalization / driver rollup), written (rows the batch shipped)
    and num_partitions. These are read verbatim from the top level of the bulk_write result, not
    re-aggregated from the partition dicts. batch_start/batch_end are the driver wall-clock around the
    batch's bulk_write (so beginning-to-end duration per batch is directly queryable). Returns None for a
    non-dict input (fail-soft)."""
    if not isinstance(result, dict):
        return None
    parts = result.get("bulk_stats")
    payload = {
        "collect_ms": result.get("collect_ms"),
        "merge_ms": result.get("merge_ms"),
        "written": result.get("written"),
        "num_partitions": len(parts) if isinstance(parts, list) else None,
    }
    return _row(config_name, job_run_id, "bulk_stats_batch", batch_id, payload, now,
                batch_start=batch_start, batch_end=batch_end)


def run_summary_row(summary, config_name, job_run_id, now=None):
    """One `run_summary` row (batch_id NULL) from a plain dict of run-level facts the notebook assembles
    at the end of a run (e.g. streaming_start, es_index, connector_version, wheel_path, environment,
    batches, rows_pushed, mode). Stored verbatim as the payload. Returns None for a non-dict input
    (fail-soft)."""
    if not isinstance(summary, dict):
        return None
    return _row(config_name, job_run_id, "run_summary", None, summary, now)


def run_error_row(error, config_name, job_run_id, now=None, batch_start=None, batch_end=None):
    """One `run_error` row (batch_id NULL) recording the exception that ENDED a run. `error` is a dict of
    failure facts the notebook assembles in its except handler (e.g. exception_type, message, mode,
    es_index, elapsed_ms); stored verbatim as the payload. batch_start/batch_end carry the run's start and
    the failure time so a timed-out run shows HOW LONG it ran before failing. This is the durable
    breadcrumb a hard failure (a timeout that exhausts retries and raises) otherwise never leaves in the
    table. Returns None for a non-dict input (fail-soft)."""
    if not isinstance(error, dict):
        return None
    return _row(config_name, job_run_id, "run_error", None, error, now,
                batch_start=batch_start, batch_end=batch_end)


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
    on the monitoring table (it grows unbounded otherwise - one bulk_stats_partition row per partition per
    micro-batch). Validates the name fail-closed. Returns None when retention_days <= 0 (retention
    DISABLED => keep all rows; the caller skips the DELETE). retention_days is coerced to a non-negative
    int; a non-numeric value raises (fail-closed, since it is interpolated into SQL)."""
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

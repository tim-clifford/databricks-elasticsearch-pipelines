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
from datetime import datetime, timezone

# The monitoring table's columns, in order, as (name, sql_type). This ONE tuple drives both the CREATE
# TABLE statement (create_table_sql) and the row shape (ROW_FIELDS), so the schema is defined exactly
# once. payload is VARIANT (semi-structured, queryable with `payload:field` paths in SQL); it needs DBR
# 15.3+ / recent serverless, which every target here runs. event_ts is the batch/emit wall-clock the
# row is about; ingest_ts is when the row was actually written (a WRITER-SUPPLIED default, see
# ROW_FIELDS), so a delayed relay is still distinguishable from batch time.
MONITORING_TABLE_COLUMNS = (
    ("config_name", "STRING"),   # which pipeline config emitted the row
    ("job_run_id", "STRING"),    # the Databricks job run id, to group rows of one run across batches
    ("record_type", "STRING"),   # discriminator; one of RECORD_TYPES
    ("batch_id", "BIGINT"),      # streaming micro-batch id; NULL for batch runs and run_summary rows
    ("event_ts", "TIMESTAMP"),   # UTC wall-clock the row is ABOUT (batch/emit time)
    ("payload", "VARIANT"),      # the type-specific fields, verbatim from the source dict
    ("ingest_ts", "TIMESTAMP"),  # UTC wall-clock the row was WRITTEN (writer supplies via current_timestamp())
)

# The allow-list of record_type values. A row builder only ever emits one of these; the writer and any
# reader can trust the set is closed. (Adding a type is a deliberate change here, never an accident.)
RECORD_TYPES = (
    "stream_progress",       # one StreamingQueryProgress (streaming only), payload = the progress dict
    "bulk_stats_partition",  # one per DataFrame partition, payload = that partition's raw bulk_stats dict
    "bulk_stats_batch",      # driver-side facts not in any partition (collect_ms/merge_ms/written)
    "run_summary",           # one per run: streaming_start, es_index, versions, totals
)

# The fields a row builder emits, in order. This is MONITORING_TABLE_COLUMNS MINUS the writer-supplied
# ingest_ts (the writer stamps ingest_ts with the Spark current_timestamp() at append time, so a pure
# builder never invents it). assert_columns_consistent() enforces the relationship so the two lists
# cannot drift.
ROW_FIELDS = ("config_name", "job_run_id", "record_type", "batch_id", "event_ts", "payload")

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


def _event_ts(now=None):
    """The event_ts string a builder stores: UTC, `YYYY-MM-DDTHH:MM:SS.ffffff`. `now=None` reads the
    current UTC time; tests pass a fixed datetime. A tz-aware `now` is converted to UTC; a naive one is
    assumed already-UTC (the same contract observability._ts_token uses). Fail-soft: returns None if the
    clock read fails, so a builder never raises on the timestamp."""
    try:
        dt = now if now is not None else datetime.now(timezone.utc)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    except Exception:
        return None


def _json(payload):
    """Serialize a payload dict to a compact, stable JSON string for the VARIANT column. sort_keys keeps
    it deterministic (so tests and diffs are stable); default=str means an odd value (a datetime, a
    Decimal) is stringified rather than raising - fail-soft, a diagnostic row must never break a write."""
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


def _row(config_name, job_run_id, record_type, batch_id, payload, now=None):
    """Assemble one row dict keyed by ROW_FIELDS. record_type is asserted to be in the allow-list (a
    programming error if not, so it raises here in tests, but callers only ever pass a literal). batch_id
    is coerced to int or None. payload is JSON-serialized."""
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
        "payload": _json(payload),
    }


def progress_row(progress, config_name, job_run_id, now=None):
    """One `stream_progress` row from a StreamingQueryProgress dict (as parsed in the listener). payload
    is the WHOLE progress dict (full fidelity - durationMs breakdown, per-source backlog, offsets), so
    nothing the runtime emits is dropped. batch_id is taken from progress['batchId']. Returns None for a
    non-dict input (fail-soft; the caller skips it)."""
    if not isinstance(progress, dict):
        return None
    return _row(config_name, job_run_id, "stream_progress", progress.get("batchId"), progress, now)


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


def bulk_stats_batch_row(result, config_name, job_run_id, batch_id, now=None):
    """One `bulk_stats_batch` row carrying the DRIVER-side facts that are NOT in any partition:
    collect_ms and merge_ms (Spark result finalization / driver rollup), written (rows the batch shipped)
    and num_partitions. These are read verbatim from the top level of the bulk_write result, not
    re-aggregated from the partition dicts. Returns None for a non-dict input (fail-soft)."""
    if not isinstance(result, dict):
        return None
    parts = result.get("bulk_stats")
    payload = {
        "collect_ms": result.get("collect_ms"),
        "merge_ms": result.get("merge_ms"),
        "written": result.get("written"),
        "num_partitions": len(parts) if isinstance(parts, list) else None,
    }
    return _row(config_name, job_run_id, "bulk_stats_batch", batch_id, payload, now)


def run_summary_row(summary, config_name, job_run_id, now=None):
    """One `run_summary` row (batch_id NULL) from a plain dict of run-level facts the notebook assembles
    at the end of a run (e.g. streaming_start, es_index, connector_version, wheel_path, environment,
    batches, rows_pushed, mode). Stored verbatim as the payload. Returns None for a non-dict input
    (fail-soft)."""
    if not isinstance(summary, dict):
        return None
    return _row(config_name, job_run_id, "run_summary", None, summary, now)


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
    DELTA` from MONITORING_TABLE_COLUMNS (the single schema source) plus auto-optimize table properties
    (the sink appends one small file per micro-batch, so predictive/auto compaction keeps the table
    tidy). IF NOT EXISTS makes re-running the job a safe no-op."""
    canonical = validate_table_name(table_name)
    cols = ",\n  ".join(f"{name} {sql_type}" for name, sql_type in MONITORING_TABLE_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {canonical} (\n  {cols}\n) USING DELTA\n"
        "TBLPROPERTIES (\n"
        "  'delta.autoOptimize.optimizeWrite' = 'true',\n"
        "  'delta.autoOptimize.autoCompact' = 'true'\n"
        ")"
    )

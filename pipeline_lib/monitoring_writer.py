"""The monitoring log's WRITE side: whether the log is on, and appending rows to it fail-closed.

pipeline_lib.monitoring_sink owns the schema and builds the rows; this module decides whether they are written
and how a failed write is surfaced. The Spark write itself is a separate function (spark_append) handed to
MonitoringLog, so the policy here is unit-tested off-cluster with a fake writer.

The contract when the log is ON: it is part of the export, not best-effort. A misconfiguration fails the run
before any data moves (resolve_log_table), and a failed append raises MonitoringLogError, which fails the batch
and with it the task, so support is notified and no further data is sent unlogged. Rows that record a failure
are written with append_failure, which never hides the failure it records. When the log is OFF every call is a
no-op.

No object here holds a Spark session: the session is passed to every append (the micro-batch's own session
inside foreachBatch, the notebook's `spark` elsewhere), because a MonitoringLog is captured by the foreachBatch
function, which Spark Connect ships to the cluster.
"""
from pipeline_lib.monitoring_sink import MONITORING_TABLE_COLUMNS, ROW_FIELDS, validate_table_name


class MonitoringLogError(RuntimeError):
    """A monitoring log append failed while the log is ON. Raised, never swallowed, so the batch and the task
    fail: support is notified and no further data is sent without a log record."""


def resolve_log_table(enabled, table):
    """The table to log to, or "" when the log is off. `enabled` is the canonical flag ("true" / "false" / ""
    from require_es_flag); `table` is the configured catalog.schema.table. FAIL-CLOSED: the log switched on
    with the table unset or malformed raises ValueError, so the run fails before any data moves."""
    if enabled != "true":
        return ""
    if not table:
        raise ValueError("monitoring_log_enabled=true but monitoring_log_table is unset "
                         "(${var.monitoring_log_table}): set the bundle variable and run the `_log table "
                         "create` job, or turn monitoring_log_enabled off")
    return validate_table_name(table, "monitoring_log_table")


class MonitoringLog:
    """Appends monitoring rows (dicts keyed by ROW_FIELDS, from monitoring_sink's builders) to `table` through
    `write_rows(table, rows, session)` (spark_append on a cluster, a fake in tests). `table` "" means the log is
    off and every call is a no-op."""

    def __init__(self, table, write_rows):
        self.table = table
        self._write_rows = write_rows

    @property
    def active(self):
        return bool(self.table)

    def append(self, rows, session):
        """Append `rows`. No-op when the log is off or `rows` is empty. FAIL-CLOSED: any write error raises
        MonitoringLogError (chained to the cause)."""
        if not self.active or not rows:
            return
        try:
            self._write_rows(self.table, rows, session)
        except Exception as exc:
            kinds = ", ".join(sorted({r.get("record_type", "?") for r in rows}))
            raise MonitoringLogError(
                f"monitoring log append to {self.table!r} failed ({type(exc).__name__}: {exc}); {len(rows)} "
                f"row(s) ({kinds}) not persisted. Failing the task so no further data is sent without a log "
                f"record.") from exc

    def append_failure(self, rows, exc, session, log=print):
        """Append the rows that record a failure (`exc`) WITHOUT ever masking it: the caller re-raises `exc`.
        If this append fails too (often the same outage), that is printed and attached to `exc` as a note, so
        the original error stays the one the run fails with."""
        try:
            self.append(rows, session)
        except Exception as log_exc:
            note = f"monitoring log: the failure row(s) could not be written either: {log_exc}"
            log(f"WARNING: {note}")
            if hasattr(exc, "add_note"):  # Python 3.11+; serverless environment versions are not pinned here
                exc.add_note(note)


def spark_append(table, rows, session):
    """The Spark write behind MonitoringLog on a cluster: build a DataFrame from `rows` (payload and timestamps
    as strings), parse the payload to VARIANT, cast the timestamps, stamp logged_ts = current_timestamp() (the
    write time), and append BY NAME via writeTo().append(), so the table's physical column order does not
    matter (a migrated table has its newer columns last, after the DEPRECATED ones, which new rows leave NULL).
    The DataFrame schema is derived from MONITORING_TABLE_COLUMNS (spark_row_schema), never re-typed. Raises on
    any failure; MonitoringLog turns that into MonitoringLogError. Not unit-tested off-cluster (it is Spark
    I/O); proven live."""
    from pyspark.sql import functions as F

    df = session.createDataFrame([tuple(r[f] for f in ROW_FIELDS) for r in rows], spark_row_schema())
    for name, sql_type, _comment in MONITORING_TABLE_COLUMNS:
        if name not in ROW_FIELDS:
            continue
        if sql_type == "TIMESTAMP":
            df = df.withColumn(name, F.col(name).cast("timestamp"))
        elif sql_type == "VARIANT":
            df = df.withColumn(name, F.expr(f"parse_json({name})"))
    df = df.withColumn("logged_ts", F.current_timestamp())
    df.writeTo(table).append()


def spark_row_schema():
    """The DDL schema string for a DataFrame of builder rows: every ROW_FIELDS column with its table type,
    except TIMESTAMP and VARIANT, which the builders hand over as strings (spark_append casts them)."""
    types = {name: sql_type for name, sql_type, _comment in MONITORING_TABLE_COLUMNS}
    return ", ".join(f"{name} {'string' if types[name] in ('TIMESTAMP', 'VARIANT') else types[name].lower()}"
                     for name in ROW_FIELDS)

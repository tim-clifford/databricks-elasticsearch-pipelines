# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: log table prune
# MAGIC
# MAGIC A recurring maintenance notebook that bounds the growth of the shared monitoring Delta table (the
# MAGIC one the pipelines append to when `monitoring_log_enabled` is on). The sink appends one small row per
# MAGIC DataFrame partition per micro-batch, so on an always-on stream the table grows without limit; this
# MAGIC job enforces retention and keeps the table tidy and query-fast.
# MAGIC
# MAGIC What it does, in order:
# MAGIC 1. DELETE rows older than `monitoring_log_retention_days` (by `logged_ts`, the write time). `0` DISABLES the delete
# MAGIC    (keep ALL rows) - the job then only optimizes/vacuums.
# MAGIC 2. OPTIMIZE the table (compacts small files and reclusters the liquid-clustered data, including a
# MAGIC    table that only just had CLUSTER BY set by the `_log table create` migration).
# MAGIC 3. VACUUM the table (reclaims storage from the files the DELETE/OPTIMIZE removed).
# MAGIC
# MAGIC Run identity needs `MODIFY` on the table (DELETE/OPTIMIZE/VACUUM), not `CREATE`. The job ships on a
# MAGIC daily schedule that is PAUSED by default (`${var.schedule_pause_status}`); unpause it per target once
# MAGIC the table exists and the sink is on. It is also runnable on demand: `databricks bundle run
# MAGIC log_table_prune`.
# MAGIC
# MAGIC Parameters (both deploy-time base_parameters):
# MAGIC - `monitoring_log_table` (from `${var.monitoring_log_table}`): the fully-qualified
# MAGIC   `catalog.schema.table` to prune. A blank or malformed value fails closed (the SAME allow-list
# MAGIC   `pipeline_lib.monitoring_sink.validate_table_name` the writer and the create job use).
# MAGIC - `monitoring_log_retention_days` (from `${var.monitoring_log_retention_days}`): days of history to
# MAGIC   keep. A non-integer value fails closed; `0` (or negative) disables the DELETE.

# COMMAND ----------
# Cell 1 - RESOLVE + VALIDATE. Read the parameters, make pipeline_lib importable (the shared SQL builders),
# and fail closed on a blank/malformed name or a non-integer retention BEFORE composing any SQL.
# validate_table_name is a strict allow-list (three-part catalog.schema.table of simple identifiers), so
# the name cannot carry anything that is not a plain identifier into the DELETE/OPTIMIZE/VACUUM statements.
import os
import sys

dbutils.widgets.text("monitoring_log_table", "", "Fully-qualified catalog.schema.table of the shared monitoring Delta table to prune")
dbutils.widgets.text("monitoring_log_retention_days", "90", "Days of history to keep (DELETE older rows); 0 disables the delete (keep all)")
MONITORING_LOG_TABLE = dbutils.widgets.get("monitoring_log_table").strip()
MONITORING_LOG_RETENTION_DAYS = dbutils.widgets.get("monitoring_log_retention_days").strip()

# Resolve the synced bundle root and add it to sys.path so pipeline_lib imports, the same way
# log_table_create.py / run_index_pipeline.py do. This notebook is synced to <bundle files>/notebooks/.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
FILES_ROOT = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files
if FILES_ROOT not in sys.path:
    sys.path.insert(0, FILES_ROOT)

from pipeline_lib.monitoring_sink import (  # noqa: E402
    optimize_sql,
    prune_sql,
    validate_table_name,
    vacuum_sql,
)

if not MONITORING_LOG_TABLE:
    # monitoring_log_table is empty on main and set per target; with no name there is nothing to prune, so
    # fail closed rather than do something surprising.
    raise ValueError(
        "missing required parameter: monitoring_log_table (set the ${var.monitoring_log_table} bundle "
        "variable at deploy); a log table prune needs a fully-qualified catalog.schema.table"
    )
# Fail closed on a malformed name (single source of the rule, shared with the writer / create job).
CANONICAL_TABLE = validate_table_name(MONITORING_LOG_TABLE)
# Fail closed on a non-integer retention; prune_sql coerces and treats <= 0 as "retention disabled".
try:
    RETENTION_DAYS = int(MONITORING_LOG_RETENTION_DAYS)
except (TypeError, ValueError):
    raise ValueError(
        f"monitoring_log_retention_days must be an integer, got {MONITORING_LOG_RETENTION_DAYS!r} "
        f"(set the ${{var.monitoring_log_retention_days}} bundle variable; 0 disables the delete)"
    )

PRUNE_SQL = prune_sql(CANONICAL_TABLE, RETENTION_DAYS)  # None when retention disabled (<= 0)
OPTIMIZE_SQL = optimize_sql(CANONICAL_TABLE)
VACUUM_SQL = vacuum_sql(CANONICAL_TABLE)

print("log table prune - parameters:")
print(f"  monitoring_log_table = {CANONICAL_TABLE!r}")
print(f"  monitoring_log_retention_days = {RETENTION_DAYS} ({'DELETE older rows' if PRUNE_SQL else 'retention DISABLED (keep all)'})")

# COMMAND ----------
# Cell 2 - PRUNE + OPTIMIZE + VACUUM + VERIFY. The table must exist (this is a maintenance job for an
# existing monitoring table; run `_log table create` first). Fail closed if it is absent rather than
# silently doing nothing, so a prune scheduled against a mistyped/uncreated table surfaces as a failure.
if not spark.catalog.tableExists(CANONICAL_TABLE):
    raise RuntimeError(
        f"log_table_prune FAILED: table {CANONICAL_TABLE!r} does not exist; run the `_log table create` "
        f"job first (this job only prunes/optimizes an existing monitoring table)"
    )

DELETED_ROWS = None
if PRUNE_SQL:
    print(f"  {PRUNE_SQL}")
    # A Delta DELETE returns one row with num_affected_rows; read it so the log states how many rows the
    # retention cutoff removed. Best-effort on the count read (the DELETE itself must succeed); a count-read
    # fault must not fail a prune that actually ran.
    _del_df = spark.sql(PRUNE_SQL)
    try:
        DELETED_ROWS = _del_df.collect()[0]["num_affected_rows"]
    except Exception as _e:  # noqa: BLE001 - the DELETE ran; only the count read is best-effort
        print(f"  WARNING: could not read deleted row count ({type(_e).__name__}: {_e})")
    print(f"  deleted rows (older than {RETENTION_DAYS} days): {DELETED_ROWS}")
else:
    print("  retention disabled (monitoring_log_retention_days <= 0); skipping DELETE")

# OPTIMIZE compacts small files and reclusters the liquid-clustered data; VACUUM reclaims the removed
# files. Both must succeed (a failed maintenance op is a real failure, not swallowed).
print(f"  {OPTIMIZE_SQL}")
spark.sql(OPTIMIZE_SQL)
print(f"  {VACUUM_SQL}")
spark.sql(VACUUM_SQL)

SUMMARY = (
    f"log_table_prune table={CANONICAL_TABLE!r} retention_days={RETENTION_DAYS} "
    f"deleted_rows={DELETED_ROWS} optimized=True vacuumed=True"
)
print(f"LOG TABLE PRUNE COMPLETE: {SUMMARY}")

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output. Reached only on success (prune/optimize/vacuum all ran against a verified-present table).
dbutils.notebook.exit(SUMMARY)

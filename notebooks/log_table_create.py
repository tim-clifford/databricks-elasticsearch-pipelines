# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: log table create
# MAGIC
# MAGIC A one-shot maintenance notebook that CREATES the shared monitoring Delta table the pipelines
# MAGIC append to when `monitoring_log_enabled` is on. It is the sole place the table's DDL lives at
# MAGIC deploy time; the schema itself comes from `pipeline_lib.monitoring_sink.MONITORING_TABLE_COLUMNS`
# MAGIC (the SINGLE source of truth shared with the writer), so the created table and the appended rows
# MAGIC can never drift.
# MAGIC
# MAGIC Run this ONCE per target before turning the monitoring sink on. It is idempotent and RE-RUNNABLE:
# MAGIC `CREATE TABLE IF NOT EXISTS` makes a first run create the table and a later run a no-op, and when a
# MAGIC newer build adds a column to the schema, re-running ADDITIVELY applies it (`ALTER TABLE ADD COLUMNS`
# MAGIC for the missing columns only) and ensures liquid clustering, WITHOUT dropping or replacing the table,
# MAGIC so existing rows are preserved. It NEVER drops or renames a column (an extra/renamed column is warned,
# MAGIC not touched). It also keeps every column COMMENT current (one metadata commit, and none when nothing
# MAGIC changed), marks a renamed column (`DEPRECATED_COLUMNS`) DEPRECATED in its comment, and backfills the
# MAGIC new columns on rows an older build wrote (`backfill_sql`, idempotent). After an upgrade, re-run this job
# MAGIC once every pipeline job is running the new build, to backfill the rows old jobs wrote in between. The export jobs only ever APPEND to the table, so their run identity needs only
# MAGIC `MODIFY`; the identity that runs THIS job needs `CREATE TABLE` / `ALTER` (and `USE CATALOG`/
# MAGIC `USE SCHEMA`) on the target schema.
# MAGIC
# MAGIC Parameters:
# MAGIC - `monitoring_log_table` (deploy-time base_parameter, from the `${var.monitoring_log_table}` bundle
# MAGIC   variable): the fully-qualified `catalog.schema.table` to create. A blank or malformed value
# MAGIC   (not a three-part name of simple identifiers) fails closed - the SAME allow-list
# MAGIC   `pipeline_lib.monitoring_sink.validate_table_name` applies at write time, so a name accepted here
# MAGIC   is exactly a name the writer will accept.

# COMMAND ----------
# Cell 1 - RESOLVE + VALIDATE. Read the table name, make pipeline_lib importable (the shared schema), and
# fail closed on a blank/malformed name BEFORE composing any SQL. validate_table_name is a strict
# allow-list (three-part catalog.schema.table of [A-Za-z_][A-Za-z0-9_]* identifiers), so the name cannot
# carry anything that is not a plain identifier into the CREATE statement.
import os
import sys

dbutils.widgets.text("monitoring_log_table", "", "Fully-qualified catalog.schema.table of the shared monitoring Delta table to create")
MONITORING_LOG_TABLE = dbutils.widgets.get("monitoring_log_table").strip()

# Resolve the synced bundle root and add it to sys.path so pipeline_lib imports, the same way
# deploy_views.py / run_index_pipeline.py do. This notebook is synced to <bundle files>/notebooks/.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
FILES_ROOT = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files
if FILES_ROOT not in sys.path:
    sys.path.insert(0, FILES_ROOT)

from pipeline_lib.log_table_migration import create_or_migrate  # noqa: E402
from pipeline_lib.monitoring_sink import create_table_sql, validate_table_name  # noqa: E402

if not MONITORING_LOG_TABLE:
    # monitoring_log_table is empty on main and set per target; with no name there is nothing to create,
    # so fail closed rather than do something surprising.
    raise ValueError(
        "missing required parameter: monitoring_log_table (set the ${var.monitoring_log_table} bundle "
        "variable at deploy); a log table create needs a fully-qualified catalog.schema.table"
    )
# Fail closed on a malformed name (single source of the rule, shared with the writer). Raises ValueError
# with a descriptive message on anything that is not a three-part name of simple identifiers.
CANONICAL_TABLE = validate_table_name(MONITORING_LOG_TABLE)
CREATE_SQL = create_table_sql(CANONICAL_TABLE)

print("log table create - parameters:")
print(f"  monitoring_log_table = {CANONICAL_TABLE!r}")
print("  DDL:")
print(CREATE_SQL)

# COMMAND ----------
# Cell 2 - CREATE + MIGRATE + VERIFY (pipeline_lib.log_table_migration.create_or_migrate, unit-tested with a fake
# session). Creates the table, or brings an existing one to the current schema: adds missing columns, sets the
# column comments (deprecated columns marked DEPRECATED), ensures liquid clustering, and backfills the new columns
# on rows an older build wrote. Never drops a column. Success is the VERIFIED end state (the table exists
# afterwards); anything else raises, so the run fails closed.
SUMMARY = create_or_migrate(spark, CANONICAL_TABLE)

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output. Reached only on success (CREATED, MIGRATED or ALREADY_EXISTS, table verified present).
dbutils.notebook.exit(SUMMARY)

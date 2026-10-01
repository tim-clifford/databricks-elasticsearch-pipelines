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
# MAGIC Run this ONCE per target before turning the monitoring sink on. It is idempotent
# MAGIC (`CREATE TABLE IF NOT EXISTS`), so re-running is a safe no-op. The export jobs only ever APPEND to
# MAGIC the table, so their run identity needs only `MODIFY`; the identity that runs THIS job needs
# MAGIC `CREATE TABLE` (and `USE CATALOG`/`USE SCHEMA`) on the target schema.
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

from pipeline_lib.monitoring_sink import (  # noqa: E402
    MONITORING_TABLE_COLUMNS,
    create_table_sql,
    validate_table_name,
)

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
# Cell 2 - CREATE + VERIFY. Probe existence before and after so the run log states plainly whether this
# job created the table or found it already there, then confirm the end state. Success is the VERIFIED end
# state (the table exists), not merely that the CREATE statement ran; if the table is somehow still absent
# afterwards, or a real error occurs, raise so the run fails closed. A pre-existing table whose columns do
# not match the current schema is WARNED (not failed): correcting a drifted table is a deliberate act, not
# this job's role, and IF NOT EXISTS leaves it untouched.
EXISTED_BEFORE = spark.catalog.tableExists(CANONICAL_TABLE)

spark.sql(CREATE_SQL)

EXISTS_AFTER = spark.catalog.tableExists(CANONICAL_TABLE)
if not EXISTS_AFTER:
    raise RuntimeError(
        f"log_table_create FAILED: table {CANONICAL_TABLE!r} does not exist after CREATE TABLE IF NOT "
        f"EXISTS ran (no exception was raised); check catalog/schema existence and CREATE grants"
    )

# Advisory schema check when the table pre-existed: warn if its columns differ from the schema this
# build expects, so an operator notices a table created by an older version. Best-effort: a failure to
# introspect must not turn a verified-present table into a false failure.
if EXISTED_BEFORE:
    try:
        existing_cols = [c.name for c in spark.table(CANONICAL_TABLE).schema]
        expected_cols = [name for name, _type in MONITORING_TABLE_COLUMNS]
        if existing_cols != expected_cols:
            print(
                f"WARNING: existing table {CANONICAL_TABLE!r} columns {existing_cols} differ from the "
                f"expected schema {expected_cols}. IF NOT EXISTS left it as-is; if appends fail on schema "
                f"mismatch, migrate or recreate the table deliberately."
            )
    except Exception as exc:  # noqa: BLE001 - advisory only; the table is verified present above
        print(f"  WARNING: could not introspect existing table columns ({type(exc).__name__}: {exc})")

OUTCOME = "ALREADY_EXISTS" if EXISTED_BEFORE else "CREATED"
SUMMARY = f"log_table_create outcome={OUTCOME} table={CANONICAL_TABLE!r} exists_after={EXISTS_AFTER}"
print(f"LOG TABLE CREATE COMPLETE: {SUMMARY}")

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output. Reached only on success (CREATED or ALREADY_EXISTS, table verified present).
dbutils.notebook.exit(SUMMARY)

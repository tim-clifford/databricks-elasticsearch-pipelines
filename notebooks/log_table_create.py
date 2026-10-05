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
# MAGIC not touched). The export jobs only ever APPEND to the table, so their run identity needs only
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

from pipeline_lib.monitoring_sink import (  # noqa: E402
    MONITORING_TABLE_COLUMNS,
    alter_add_columns_sql,
    alter_cluster_by_sql,
    create_table_sql,
    missing_columns,
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
# Cell 2 - CREATE + MIGRATE + VERIFY. Probe existence before and after so the run log states plainly whether
# this job created the table or found it already there, then confirm the end state. Success is the VERIFIED
# end state (the table exists AND its columns are a superset of the expected schema), not merely that the
# CREATE statement ran; if the table is still absent afterwards, or a real error occurs, raise so the run
# fails closed.
#
# RE-RUNNABLE additive migration: CREATE TABLE IF NOT EXISTS leaves a pre-existing table untouched, so when
# a newer build adds a column it would NOT reach an existing table on its own. So after the create, if the
# table pre-existed, compute the columns the expected schema has that the table lacks (missing_columns -
# an ADDITIVE allow-list, never a drop) and apply them with ALTER TABLE ADD COLUMNS, and (idempotently)
# ensure liquid clustering. This preserves all existing rows (ADD COLUMNS backfills NULL). Columns the
# table has but the schema does not (an older/renamed column) are WARNED, never dropped - removing data is a
# deliberate act, not this job's role. Reclustering existing files (OPTIMIZE) is intentionally left to the
# `_log table prune` maintenance job, since it can be heavy on a large table.
EXISTED_BEFORE = spark.catalog.tableExists(CANONICAL_TABLE)

spark.sql(CREATE_SQL)

EXISTS_AFTER = spark.catalog.tableExists(CANONICAL_TABLE)
if not EXISTS_AFTER:
    raise RuntimeError(
        f"log_table_create FAILED: table {CANONICAL_TABLE!r} does not exist after CREATE TABLE IF NOT "
        f"EXISTS ran (no exception was raised); check catalog/schema existence and CREATE grants"
    )

MIGRATED_COLS = []
if EXISTED_BEFORE:
    # Introspect the existing columns and additively reconcile to the expected schema. Everything here
    # FAILS CLOSED: the schema READ and the ADD COLUMNS / CLUSTER BY statements must all succeed. A
    # transient fault fails the job (and a re-run retries) rather than being swallowed - correct for an
    # on-demand maintenance job, since silently skipping migration would leave the table unmigrated and
    # later appends failing. The table itself is already verified-present above, so this only governs the
    # additive migration, never whether the create succeeded.
    existing_cols = [c.name for c in spark.table(CANONICAL_TABLE).schema]
    expected_cols = [name for name, _type in MONITORING_TABLE_COLUMNS]
    to_add = missing_columns(existing_cols)
    add_sql = alter_add_columns_sql(CANONICAL_TABLE, to_add)
    if add_sql:
        print(f"migrating: adding missing columns {[n for n, _t in to_add]} to existing table")
        print(f"  {add_sql}")
        spark.sql(add_sql)
        MIGRATED_COLS = [n for n, _t in to_add]
    # Ensure liquid clustering on a table that predates it (idempotent to set; no-op if already clustered).
    spark.sql(alter_cluster_by_sql(CANONICAL_TABLE))
    # Any column the table has that the expected schema does not: warn, never drop.
    extra_cols = [c for c in existing_cols if c not in expected_cols]
    if extra_cols:
        print(
            f"WARNING: existing table {CANONICAL_TABLE!r} has columns {extra_cols} not in the expected "
            f"schema. Left as-is (never dropped); remove them deliberately if they are obsolete."
        )

if EXISTED_BEFORE:
    OUTCOME = f"MIGRATED(added={MIGRATED_COLS})" if MIGRATED_COLS else "ALREADY_EXISTS"
else:
    OUTCOME = "CREATED"
SUMMARY = f"log_table_create outcome={OUTCOME} table={CANONICAL_TABLE!r} exists_after={EXISTS_AFTER}"
print(f"LOG TABLE CREATE COMPLETE: {SUMMARY}")

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output. Reached only on success (CREATED or ALREADY_EXISTS, table verified present).
dbutils.notebook.exit(SUMMARY)

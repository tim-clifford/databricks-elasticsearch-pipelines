"""The `_log table create` job's work: create the monitoring table, or bring an existing one up to the current
column set. The notebook (notebooks/log_table_create.py) resolves the table name and calls create_or_migrate; the
logic lives here, with the Spark session passed in, so it is unit-tested with a fake session.

RE-RUNNABLE and ADDITIVE: CREATE TABLE IF NOT EXISTS creates the table with every column, its comment, and the
liquid clustering, and is a no-op on an existing table. Clustering is not re-applied to an existing table: every
earlier build's create job set it (the previous one re-applied CLUSTER BY on each run), so every table already
has it. On an existing table this then adds the columns the schema has and the table lacks (missing_columns: ADD
COLUMNS, comments included; existing rows get NULL). That is the path for a column added to
MONITORING_TABLE_COLUMNS later: re-run this job BEFORE deploying the build that writes it, because a writer fails
closed on a table that lacks one of its columns. It never drops a column it does not know (an unknown column is
warned about). Everything FAILS CLOSED: a transient fault fails the job and a re-run retries.

TEMPORARY: a table that still has a RETIRED_COLUMNS column is DROPPED instead (see _drop_retired_table), and the
next run creates it fresh.
"""
from pipeline_lib.monitoring_sink import (
    MONITORING_TABLE_COLUMNS,
    alter_add_columns_sql,
    create_table_sql,
    missing_columns,
    validate_table_name,
)

# TEMPORARY (remove this constant, _drop_retired_table and its call once deployed to stg and the table there has
# been re-created): the timestamp columns an earlier build wrote under other names (now start_ts / end_ts /
# logged_ts). Only the dev and stg tables have them; a table created by this build never does, so the drop can
# never reach a table this build created.
RETIRED_COLUMNS = ("batch_start_ts", "batch_end_ts", "ingest_ts")


def create_or_migrate(spark, table_name, printer=print):
    """Create `table_name`, or bring an existing one up to the current column set (see the module docstring), and
    return the one-line summary the job exits with. Raises if the table does not exist afterwards, or on any
    failed statement."""
    table = validate_table_name(table_name)
    existed_before = spark.catalog.tableExists(table)
    if existed_before and _drop_retired_table(spark, table, printer):
        summary = f"log_table_create outcome=DROPPED table={table!r} exists_after=False"
        printer(f"LOG TABLE CREATE COMPLETE: {summary}. Re-run this job to create the table.")
        return summary
    spark.sql(create_table_sql(table))
    if not spark.catalog.tableExists(table):
        raise RuntimeError(
            f"log_table_create FAILED: table {table!r} does not exist after CREATE TABLE IF NOT EXISTS ran (no "
            f"exception was raised); check catalog/schema existence and CREATE grants")
    outcome = _migrate(spark, table, printer) if existed_before else "CREATED"
    summary = f"log_table_create outcome={outcome} table={table!r} exists_after=True"
    printer(f"LOG TABLE CREATE COMPLETE: {summary}")
    return summary


def _migrate(spark, table, printer):
    """Bring an existing table up to the current column set; returns the outcome token for the summary."""
    existing_cols = [f.name for f in spark.table(table).schema]

    to_add = missing_columns(existing_cols)
    added = [name for name, _type, _comment in to_add]
    add_sql = alter_add_columns_sql(table, to_add)
    if add_sql:
        printer(f"adding missing columns {added} to the existing table")
        printer(f"  {add_sql}")
        spark.sql(add_sql)

    expected = [name for name, _type, _comment in MONITORING_TABLE_COLUMNS]
    extra = [c for c in existing_cols if c not in expected]
    if extra:
        printer(f"WARNING: existing table {table!r} has columns {extra} not in the expected schema. Left as-is "
                f"(never dropped); remove them deliberately if they are obsolete.")

    return f"MIGRATED(added={added})" if added else "ALREADY_EXISTS"


def _drop_retired_table(spark, table, printer):
    """TEMPORARY: DROP the table when it still has a RETIRED_COLUMNS column, and return True; otherwise False.

    Dropping the columns in place needs column mapping on the table, which the stg table does not have, so the
    whole table goes instead and the next run of this job creates it exactly as on a new environment. Accepted by
    Tim (2026-10-07): back the table up first; while it is gone, jobs with the log on fail their next append
    (fail-closed), and rows appended between the backup and the drop are lost. A Unity Catalog managed table can be
    restored with UNDROP (7 days by default). A failed DROP raises (fails the job): a destructive step must not fail
    quietly."""
    present = [f.name for f in spark.table(table).schema if f.name in RETIRED_COLUMNS]
    if not present:
        return False
    drop_sql = f"DROP TABLE {table}"
    printer(f"table {table!r} still has the retired columns {present}: dropping it so the next run creates it fresh")
    printer(f"  {drop_sql}")
    spark.sql(drop_sql)
    if spark.catalog.tableExists(table):
        raise RuntimeError(f"log_table_create FAILED: table {table!r} still exists after {drop_sql} (no exception "
                           f"was raised); check DROP grants")
    return True

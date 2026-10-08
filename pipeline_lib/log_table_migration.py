"""The `_log table create` job's work: create the monitoring table, or bring an existing one up to the current
column set. The notebook (notebooks/log_table_create.py) resolves the table name and calls create_or_migrate; the
logic lives here, with the Spark session passed in, so it is unit-tested with a fake session.

RE-RUNNABLE and ADDITIVE: CREATE TABLE IF NOT EXISTS creates the table with every column, its comment, and the
liquid clustering, and is a no-op on an existing table (clustering is set at create, never re-applied). On an
existing table this then adds the columns the schema has and the table lacks (missing_columns: ADD COLUMNS,
comments included; existing rows get NULL). That is the path for a column added to MONITORING_TABLE_COLUMNS
later: re-run this job BEFORE deploying the build that writes it, because a writer fails closed on a table that
lacks one of its columns. It never drops a column it does not know (an unknown column is warned about).
Everything FAILS CLOSED: a transient fault fails the job and a re-run retries.
"""
from pipeline_lib.monitoring_sink import (
    MONITORING_TABLE_COLUMNS,
    alter_add_columns_sql,
    create_table_sql,
    missing_columns,
    validate_table_name,
)


def create_or_migrate(spark, table_name, printer=print):
    """Create `table_name`, or bring an existing one up to the current column set (see the module docstring), and
    return the one-line summary the job exits with. Raises if the table does not exist afterwards, or on any
    failed statement."""
    table = validate_table_name(table_name)
    existed_before = spark.catalog.tableExists(table)
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


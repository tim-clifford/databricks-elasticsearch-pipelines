"""The `_log table create` job's work: create the monitoring table, or migrate an existing one to the current
schema. The notebook (notebooks/log_table_create.py) resolves the table name and calls create_or_migrate; the
logic lives here, with the Spark session passed in, so it is unit-tested with a fake session.

RE-RUNNABLE and ADDITIVE: CREATE TABLE IF NOT EXISTS leaves an existing table untouched, so on an existing table
this also:
- adds the columns the schema has and the table lacks (missing_columns: ADD COLUMNS, comments included; existing
  rows get NULL);
- sets every column comment that differs, in ONE ALTER (comment_changes: deprecated columns get a DEPRECATED
  comment), so a re-run with nothing to change makes no metadata commit;
- ensures liquid clustering;
- backfills the new columns on rows an older build wrote (backfill_sql: selects only rows not yet backfilled, so
  a re-run is a no-op). After an upgrade, re-run the job once every pipeline job runs the new build, to catch the
  rows old jobs wrote in between.
It NEVER drops or renames a column: deprecated columns are kept, any other unknown column is warned about.
Everything FAILS CLOSED (a transient fault fails the job and a re-run retries), except reading the backfilled row
count, which is best-effort because the UPDATE itself already ran. Reclustering existing files (OPTIMIZE) is left
to the `_log table prune` job, since it can be heavy on a large table.
"""
from pipeline_lib.monitoring_sink import (
    DEPRECATED_COLUMNS,
    MONITORING_TABLE_COLUMNS,
    alter_add_columns_sql,
    alter_cluster_by_sql,
    alter_column_comments_sql,
    backfill_sql,
    comment_changes,
    create_table_sql,
    missing_columns,
    validate_table_name,
)


def create_or_migrate(spark, table_name, printer=print):
    """Create or migrate `table_name` (see the module docstring) and return the one-line summary the job exits
    with. Raises if the table does not exist afterwards, or on any failed statement."""
    table = validate_table_name(table_name)
    existed_before = spark.catalog.tableExists(table)
    spark.sql(create_table_sql(table))
    if not spark.catalog.tableExists(table):
        raise RuntimeError(
            f"log_table_create FAILED: table {table!r} does not exist after CREATE TABLE IF NOT EXISTS ran (no "
            f"exception was raised); check catalog/schema existence and CREATE grants")
    if not existed_before:
        outcome = "CREATED"
    else:
        outcome = _migrate(spark, table, printer)
    summary = f"log_table_create outcome={outcome} table={table!r} exists_after=True"
    printer(f"LOG TABLE CREATE COMPLETE: {summary}")
    return summary


def _migrate(spark, table, printer):
    """Bring an existing table to the current schema; returns the outcome token for the summary."""
    existing_cols = [f.name for f in spark.table(table).schema]

    to_add = missing_columns(existing_cols)
    added = [name for name, _type, _comment in to_add]
    add_sql = alter_add_columns_sql(table, to_add)
    if add_sql:
        printer(f"migrating: adding missing columns {added} to existing table")
        printer(f"  {add_sql}")
        spark.sql(add_sql)

    # Read the comments back AFTER the ADD COLUMNS (the added columns already carry theirs).
    changes = comment_changes({f.name: f.metadata.get("comment") for f in spark.table(table).schema})
    commented = [name for name, _comment in changes]
    comments_sql = alter_column_comments_sql(table, changes)
    if comments_sql:
        printer(f"setting column comments (one ALTER): {commented}")
        spark.sql(comments_sql)

    # Ensure liquid clustering on a table that predates it (idempotent to set).
    spark.sql(alter_cluster_by_sql(table))

    backfilled = None
    fill_sql = backfill_sql(table, existing_cols)
    if fill_sql:
        printer("backfilling the new columns on rows written by an older build:")
        printer(f"  {fill_sql}")
        result = spark.sql(fill_sql)
        # A Delta UPDATE returns one row with num_affected_rows.
        try:
            backfilled = result.collect()[0]["num_affected_rows"]
        except Exception as exc:  # noqa: BLE001 - the UPDATE ran; only the count read is best-effort
            printer(f"  WARNING: could not read the backfilled row count ({type(exc).__name__}: {exc})")
            backfilled = "unknown"  # the UPDATE ran, so this is a migration, not ALREADY_EXISTS
        printer(f"  backfilled rows: {backfilled}")

    expected = [name for name, _type, _comment in MONITORING_TABLE_COLUMNS]
    deprecated = [old for old, _new in DEPRECATED_COLUMNS if old in existing_cols]
    if deprecated:
        printer(f"deprecated columns kept (no longer written; dropped in a later release): {deprecated}")
    extra = [c for c in existing_cols if c not in expected and c not in deprecated]
    if extra:
        printer(f"WARNING: existing table {table!r} has columns {extra} not in the expected schema. Left as-is "
                f"(never dropped); remove them deliberately if they are obsolete.")

    if added or commented or backfilled:
        return f"MIGRATED(added={added}, commented={commented}, backfilled_rows={backfilled})"
    return "ALREADY_EXISTS"

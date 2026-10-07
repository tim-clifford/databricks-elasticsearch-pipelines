"""The `_log table create` job's work: create the monitoring table, or bring an existing one up to the current
column set. The notebook (notebooks/log_table_create.py) resolves the table name and calls create_or_migrate; the
logic lives here, with the Spark session passed in, so it is unit-tested with a fake session.

RE-RUNNABLE and ADDITIVE: CREATE TABLE IF NOT EXISTS creates the table with every column, its comment, and the
liquid clustering, and is a no-op on an existing table. On an existing table this then adds the columns the
schema has and the table lacks (missing_columns: ADD COLUMNS, comments included; existing rows get NULL). That is
the path for a column added to MONITORING_TABLE_COLUMNS later: re-run this job BEFORE deploying the build that
writes it, because a writer fails closed on a table that lacks one of its columns. It never drops a column it
does not know (an unknown column is warned about), except the temporary block below.
Everything FAILS CLOSED (a transient fault fails the job and a re-run retries), except that temporary block.
"""
from pipeline_lib.monitoring_sink import (
    MONITORING_TABLE_COLUMNS,
    alter_add_columns_sql,
    create_table_sql,
    missing_columns,
    validate_table_name,
)

# TEMPORARY (remove this constant, _drop_retired_columns and its call once deployed to stg and these columns are
# gone): the timestamp columns an earlier build wrote under other names, as (retired name, current name). Only the
# dev and stg tables have them, already copied into the current columns by that build's backfill; a table created
# by this build never does.
RETIRED_COLUMNS = (("batch_start_ts", "start_ts"), ("batch_end_ts", "end_ts"), ("ingest_ts", "logged_ts"))


def create_or_migrate(spark, table_name, printer=print):
    """Create `table_name`, or bring an existing one up to the current column set (see the module docstring), and
    return the one-line summary the job exits with. Raises if the table does not exist afterwards, or on any
    failed statement (other than the temporary retired-column drop)."""
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

    dropped = _drop_retired_columns(spark, table, existing_cols, printer)

    expected = [name for name, _type, _comment in MONITORING_TABLE_COLUMNS]
    retired = [old for old, _new in RETIRED_COLUMNS]
    extra = [c for c in existing_cols if c not in expected and c not in retired]
    if extra:
        printer(f"WARNING: existing table {table!r} has columns {extra} not in the expected schema. Left as-is "
                f"(never dropped); remove them deliberately if they are obsolete.")

    if added or dropped:
        return f"MIGRATED(added={added}, dropped={dropped})"
    return "ALREADY_EXISTS"


def _drop_retired_columns(spark, table, existing_cols, printer):
    """TEMPORARY: drop the RETIRED_COLUMNS the table still has, in one statement, and return the names dropped.

    It drops only when no row would lose data: first it counts the rows where a retired column holds a value its
    current column does not hold exactly (a table the earlier build's backfill never ran on). Any such row, or a
    failed count, keeps the columns. The check and the drop are two statements, so run this only when no job on a build from
    before the column rename is still appending (only those write the retired columns; in dev and stg every job
    already runs a later build). Delta allows DROP COLUMNS only with column mapping enabled on the table; this never
    enables it (that is an irreversible table-protocol upgrade). Every failure here only WARNS: the retired
    columns are NULL on every new row and harmless, so they are left for a deliberate manual drop."""
    pairs = [(old, new) for old, new in RETIRED_COLUMNS if old in existing_cols]
    if not pairs:
        return []
    present = [old for old, _new in pairs]
    # A retired value the current column does not hold exactly (NULL there, or different) would be lost.
    unbackfilled = " OR ".join(f"({old} IS NOT NULL AND NOT ({new} <=> {old}))" for old, new in pairs)
    check_sql = f"SELECT count(*) AS n FROM {table} WHERE {unbackfilled}"
    drop_sql = f"ALTER TABLE {table} DROP COLUMNS ({', '.join(present)})"
    try:
        n = spark.sql(check_sql).collect()[0]["n"]
        if n:
            printer(f"WARNING: not dropping retired columns {present}: {n} row(s) hold values the current columns "
                    f"do not (the earlier build's backfill has not run on this table). Dropping them would lose "
                    f"those timestamps, so they were left in place. Rows with NULL logged_ts are not pruned by "
                    f"retention until it is set.")
            return []
        printer(f"dropping retired columns {present}")
        printer(f"  {drop_sql}")
        spark.sql(drop_sql)
    except Exception as exc:  # noqa: BLE001 - deliberate safety net; see the docstring
        # Only the first line: a Spark AnalysisException message carries the whole JVM stack trace (about 200
        # lines, seen live); its first line names the error class.
        reason = (str(exc).strip().splitlines() or [""])[0][:500]
        printer(f"WARNING: could not drop retired columns {present} ({type(exc).__name__}: {reason}). They are "
                f"harmless (NULL on new rows) and were left in place; drop them manually once the table has column "
                f"mapping enabled (delta.columnMapping.mode = 'name').")
        return []
    return present

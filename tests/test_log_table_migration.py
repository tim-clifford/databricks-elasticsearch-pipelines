"""Offline unit tests for pipeline_lib.log_table_migration.create_or_migrate, driven by a fake Spark session that
applies the DDL it is sent to an in-memory table (columns and comments). No Spark.

The load-bearing contracts:
- A fresh table is created with every column and comment, and nothing else runs.
- An existing table gets exactly the columns it lacks (with comments), in one ADD COLUMNS; a current table gets
  no ALTER at all.
- TEMPORARY: the retired timestamp columns are dropped in one statement; a failed drop only warns.
- Fail closed: any other failed statement raises; a table missing after the create raises.
"""
import re

import pytest

from pipeline_lib.log_table_migration import RETIRED_COLUMNS, create_or_migrate

RETIRED = [old for old, _new in RETIRED_COLUMNS]
from pipeline_lib.monitoring_sink import MONITORING_TABLE_COLUMNS

TABLE = "cat.sch.mon"


class Field:
    def __init__(self, name, comment):
        self.name = name
        self.metadata = {"comment": comment} if comment is not None else {}


class Result:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class FakeSpark:
    """`columns`: the existing table's {name: comment}, or None when the table does not exist. `fail_on`: a
    statement prefix that raises. `unbackfilled`: what the retired-column data check counts."""

    def __init__(self, columns, fail_on=None, create_noop=False, unbackfilled=0):
        self.columns = None if columns is None else dict(columns)
        self.fail_on = fail_on
        self.unbackfilled = unbackfilled
        self.create_noop = create_noop
        self.statements = []
        self.catalog = self

    def tableExists(self, name):
        assert name == TABLE
        return self.columns is not None

    def table(self, name):
        assert name == TABLE
        return type("T", (), {"schema": [Field(n, c) for n, c in self.columns.items()]})()

    def sql(self, stmt):
        self.statements.append(stmt)
        if self.fail_on and stmt.startswith(self.fail_on):
            raise RuntimeError(f"{self.fail_on} failed")
        if stmt.startswith("CREATE TABLE IF NOT EXISTS"):
            if self.columns is None and not self.create_noop:
                self.columns = dict(re.findall(r"^  (\w+) \w+ COMMENT '([^']*)'", stmt, re.M))
        elif " ADD COLUMNS " in stmt:
            self.columns.update(re.findall(r"(\w+) \w+ COMMENT '([^']*)'", stmt))
        elif " DROP COLUMNS " in stmt:
            for name in re.search(r"DROP COLUMNS \(([^)]*)\)", stmt).group(1).split(", "):
                del self.columns[name]
        elif stmt.startswith("SELECT count(*)"):
            return Result([{"n": self.unbackfilled}])
        return Result([])

    def kinds(self):
        return ["CREATE" if s.startswith("CREATE") else "ADD" if " ADD COLUMNS " in s else
                "DROP" if " DROP COLUMNS " in s else "CHECK" if s.startswith("SELECT count(*)") else s
                for s in self.statements]


def _current():
    return {n: c for n, _t, c in MONITORING_TABLE_COLUMNS}


def _quiet():
    return lambda *_: None


def test_fresh_table_is_created_with_every_column_and_comment():
    spark = FakeSpark(None)
    summary = create_or_migrate(spark, TABLE, printer=_quiet())
    assert spark.kinds() == ["CREATE"]
    assert spark.columns == _current()
    assert summary == f"log_table_create outcome=CREATED table={TABLE!r} exists_after=True"


def test_current_table_gets_no_alter():
    spark = FakeSpark(_current())
    summary = create_or_migrate(spark, TABLE, printer=_quiet())
    assert spark.kinds() == ["CREATE"]  # the no-op CREATE IF NOT EXISTS only
    assert "outcome=ALREADY_EXISTS" in summary


def test_a_table_missing_a_future_column_gets_it_with_its_comment():
    current = _current()
    spark = FakeSpark({n: c for n, c in current.items() if n != "docs_written"})
    summary = create_or_migrate(spark, TABLE, printer=_quiet())
    assert spark.kinds() == ["CREATE", "ADD"]
    assert spark.columns["docs_written"] == current["docs_written"]
    assert "outcome=MIGRATED(added=['docs_written'], dropped=[])" in summary


def test_unknown_extra_column_is_warned_and_kept():
    spark = FakeSpark({**_current(), "mystery": None})
    printed = []
    create_or_migrate(spark, TABLE, printer=printed.append)
    assert "mystery" in spark.columns
    assert any(p.startswith("WARNING") and "mystery" in p for p in printed)
    assert spark.kinds() == ["CREATE"]  # never dropped


# --- TEMPORARY: the retired timestamp columns (remove with RETIRED_COLUMNS) ---------------------

def test_retired_columns_are_dropped_in_one_statement():
    spark = FakeSpark({**_current(), **{c: None for c in RETIRED}})
    printed = []
    summary = create_or_migrate(spark, TABLE, printer=printed.append)
    assert spark.kinds() == ["CREATE", "CHECK", "DROP"]
    assert spark.statements[1] == (
        f"SELECT count(*) AS n FROM {TABLE} WHERE (start_ts IS NULL AND batch_start_ts IS NOT NULL) OR "
        f"(end_ts IS NULL AND batch_end_ts IS NOT NULL) OR (logged_ts IS NULL AND ingest_ts IS NOT NULL)")
    assert spark.statements[-1] == f"ALTER TABLE {TABLE} DROP COLUMNS (batch_start_ts, batch_end_ts, ingest_ts)"
    assert spark.columns == _current()
    assert f"outcome=MIGRATED(added=[], dropped={RETIRED})" in summary
    assert not any(p.startswith("WARNING") for p in printed)  # retired columns are not "unknown"


def test_only_the_retired_columns_present_are_dropped():
    spark = FakeSpark({**_current(), "ingest_ts": None})
    create_or_migrate(spark, TABLE, printer=_quiet())
    assert spark.statements[1] == (
        f"SELECT count(*) AS n FROM {TABLE} WHERE (logged_ts IS NULL AND ingest_ts IS NOT NULL)")
    assert spark.statements[-1] == f"ALTER TABLE {TABLE} DROP COLUMNS (ingest_ts)"


def test_retired_columns_holding_unbackfilled_data_are_kept():
    # A table the earlier build's backfill never ran on: dropping would lose those timestamps.
    spark = FakeSpark({**_current(), **{c: None for c in RETIRED}}, unbackfilled=7)
    printed = []
    summary = create_or_migrate(spark, TABLE, printer=printed.append)
    assert spark.kinds() == ["CREATE", "CHECK"]  # no DROP
    assert all(c in spark.columns for c in RETIRED)
    assert "outcome=ALREADY_EXISTS" in summary
    warnings = [p for p in printed if p.startswith("WARNING")]
    assert len(warnings) == 1 and "7 row(s)" in warnings[0] and "not dropping" in warnings[0]


def test_a_failed_data_check_keeps_the_columns():
    spark = FakeSpark({**_current(), **{c: None for c in RETIRED}}, fail_on="SELECT count(*)")
    printed = []
    summary = create_or_migrate(spark, TABLE, printer=printed.append)
    assert "DROP" not in spark.kinds() and all(c in spark.columns for c in RETIRED)
    assert "outcome=ALREADY_EXISTS" in summary
    assert sum(p.startswith("WARNING") for p in printed) == 1


def test_a_failed_drop_only_warns_and_keeps_the_columns():
    # As on a table without column mapping, where Delta rejects DROP COLUMNS.
    spark = FakeSpark({**_current(), **{c: None for c in RETIRED}}, fail_on=f"ALTER TABLE {TABLE} DROP")
    printed = []
    summary = create_or_migrate(spark, TABLE, printer=printed.append)
    assert all(c in spark.columns for c in RETIRED)
    assert "outcome=ALREADY_EXISTS" in summary
    warnings = [p for p in printed if p.startswith("WARNING")]
    assert len(warnings) == 1 and "could not drop retired columns" in warnings[0]
    assert "delta.columnMapping.mode" in warnings[0]


def test_a_failed_drop_warns_with_only_the_first_line_of_the_error():
    # Seen live: Delta's AnalysisException message carries the whole JVM stack trace.
    class MultiLine(FakeSpark):
        def sql(self, stmt):
            if " DROP COLUMNS " in stmt:
                self.statements.append(stmt)
                head = "[DELTA_UNSUPPORTED_DROP_COLUMN.ENABLE_COLUMN_MAPPING] DROP COLUMN is not supported"
                raise RuntimeError(head + "\n" + "\n".join(f"\tat org.apache.spark.Frame{i}" for i in range(200)))
            return super().sql(stmt)
    printed = []
    create_or_migrate(MultiLine({**_current(), **{c: None for c in RETIRED}}), TABLE, printer=printed.append)
    warning, = [p for p in printed if p.startswith("WARNING")]
    assert "\n" not in warning and "Frame" not in warning
    assert "DELTA_UNSUPPORTED_DROP_COLUMN.ENABLE_COLUMN_MAPPING" in warning


def test_drop_never_enables_column_mapping():
    spark = FakeSpark({**_current(), **{c: None for c in RETIRED}}, fail_on=f"ALTER TABLE {TABLE} DROP")
    create_or_migrate(spark, TABLE, printer=_quiet())
    assert not any("columnMapping" in s or "TBLPROPERTIES" in s for s in spark.statements[1:])


def test_added_columns_and_the_drop_run_in_one_migration():
    current = _current()
    old = {n: c for n, c in current.items() if n != "docs_written"}
    spark = FakeSpark({**old, "ingest_ts": None})
    summary = create_or_migrate(spark, TABLE, printer=_quiet())
    assert spark.kinds() == ["CREATE", "ADD", "CHECK", "DROP"]
    assert spark.columns == current
    assert "outcome=MIGRATED(added=['docs_written'], dropped=['ingest_ts'])" in summary


# --- fail closed --------------------------------------------------------------------------------

@pytest.mark.parametrize("stage", ["CREATE", f"ALTER TABLE {TABLE} ADD"])
def test_any_other_failed_statement_fails_closed(stage):
    spark = FakeSpark({n: c for n, c in _current().items() if n != "docs_written"}, fail_on=stage)
    with pytest.raises(RuntimeError, match="failed"):
        create_or_migrate(spark, TABLE, printer=_quiet())


def test_table_missing_after_create_fails_closed():
    with pytest.raises(RuntimeError, match="does not exist after CREATE"):
        create_or_migrate(FakeSpark(None, create_noop=True), TABLE, printer=_quiet())


def test_bad_table_name_fails_before_any_sql():
    spark = FakeSpark(None)
    with pytest.raises(ValueError):
        create_or_migrate(spark, "bad name", printer=_quiet())
    assert spark.statements == []

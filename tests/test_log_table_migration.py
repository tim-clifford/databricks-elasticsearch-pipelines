"""Offline unit tests for pipeline_lib.log_table_migration.create_or_migrate, driven by a fake Spark session that
applies the DDL it is sent to an in-memory table (columns and comments). No Spark.

The load-bearing contracts:
- A fresh table is created with every column and comment, and nothing else runs.
- An existing table gets exactly the columns it lacks (with comments), in one ADD COLUMNS; a current table gets
  no ALTER at all.
- TEMPORARY: a table with any retired timestamp column is DROPPED (and nothing else runs); the next run creates
  it fresh. A table without one is never dropped.
- Fail closed: any failed statement raises; a table missing after the create, or still there after the drop,
  raises.
"""
import re

import pytest

from pipeline_lib.log_table_migration import RETIRED_COLUMNS, create_or_migrate
from pipeline_lib.monitoring_sink import MONITORING_TABLE_COLUMNS

TABLE = "cat.sch.mon"


class Field:
    def __init__(self, name, comment):
        self.name = name
        self.metadata = {"comment": comment} if comment is not None else {}


class FakeSpark:
    """`columns`: the existing table's {name: comment}, or None when the table does not exist. `fail_on`: a
    statement prefix that raises. `create_noop` / `drop_noop`: the statement succeeds but changes nothing."""

    def __init__(self, columns, fail_on=None, create_noop=False, drop_noop=False):
        self.columns = None if columns is None else dict(columns)
        self.fail_on = fail_on
        self.create_noop = create_noop
        self.drop_noop = drop_noop
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
        elif stmt.startswith("DROP TABLE "):
            if not self.drop_noop:
                self.columns = None

    def kinds(self):
        return ["CREATE" if s.startswith("CREATE") else "ADD" if " ADD COLUMNS " in s else
                "DROP TABLE" if s.startswith("DROP TABLE ") else s for s in self.statements]


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
    assert "outcome=MIGRATED(added=['docs_written'])" in summary


def test_unknown_extra_column_is_warned_and_kept():
    spark = FakeSpark({**_current(), "mystery": None})
    printed = []
    create_or_migrate(spark, TABLE, printer=printed.append)
    assert "mystery" in spark.columns
    assert any(p.startswith("WARNING") and "mystery" in p for p in printed)
    assert spark.kinds() == ["CREATE"]  # never dropped


# --- TEMPORARY: a table with the retired columns is dropped (remove with RETIRED_COLUMNS) -------

@pytest.mark.parametrize("present", [list(RETIRED_COLUMNS), ["ingest_ts"], ["batch_end_ts"]])
def test_a_table_with_any_retired_column_is_dropped_and_nothing_else_runs(present):
    spark = FakeSpark({**_current(), **{c: None for c in present}})
    printed = []
    summary = create_or_migrate(spark, TABLE, printer=printed.append)
    assert spark.statements == [f"DROP TABLE {TABLE}"]  # no CREATE, no ALTER in the same run
    assert spark.columns is None
    assert summary == f"log_table_create outcome=DROPPED table={TABLE!r} exists_after=False"
    assert any("Re-run this job to create the table" in p for p in printed)


def test_the_next_run_after_the_drop_creates_the_table_fresh():
    spark = FakeSpark({**_current(), **{c: None for c in RETIRED_COLUMNS}})
    create_or_migrate(spark, TABLE, printer=_quiet())
    summary = create_or_migrate(spark, TABLE, printer=_quiet())
    assert "outcome=CREATED" in summary
    assert spark.columns == _current()  # exactly a new environment's table, no retired columns


@pytest.mark.parametrize("columns", [
    _current(),                                                   # a table this build created
    {**_current(), "mystery": None},                               # an unknown extra column
    {n: c for n, c in _current().items() if n != "docs_written"},  # a table missing a future column
])
def test_a_table_without_retired_columns_is_never_dropped(columns):
    spark = FakeSpark(columns)
    create_or_migrate(spark, TABLE, printer=_quiet())
    assert not any(s.startswith("DROP") for s in spark.statements)
    assert spark.columns is not None


@pytest.mark.parametrize("columns", [
    {"id": None, "ingest_ts": None},                                          # an unrelated table
    {**{n: c for n, c in _current().items() if n != "payload"}, "ingest_ts": None},  # missing a log column
    {**_current(), "ingest_ts": None, "mystery": None},                       # an unknown extra column
])
def test_a_table_not_shaped_like_the_log_is_refused_and_untouched(columns):
    spark = FakeSpark(columns)
    with pytest.raises(RuntimeError, match="refusing to drop it"):
        create_or_migrate(spark, TABLE, printer=_quiet())
    assert spark.statements == []  # no DROP, no CREATE, no ALTER
    assert spark.columns == columns


def test_a_failed_drop_fails_the_job():
    spark = FakeSpark({**_current(), "ingest_ts": None}, fail_on="DROP TABLE")
    with pytest.raises(RuntimeError, match="DROP TABLE failed"):
        create_or_migrate(spark, TABLE, printer=_quiet())
    assert spark.kinds() == ["DROP TABLE"]


def test_a_table_still_there_after_the_drop_fails_the_job():
    spark = FakeSpark({**_current(), "ingest_ts": None}, drop_noop=True)
    with pytest.raises(RuntimeError, match="still exists after DROP TABLE"):
        create_or_migrate(spark, TABLE, printer=_quiet())


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

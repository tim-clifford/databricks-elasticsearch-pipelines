"""Offline unit tests for pipeline_lib.log_table_migration.create_or_migrate, driven by a fake Spark session that
applies the DDL it is sent to an in-memory table (columns and comments). No Spark.

The load-bearing contracts:
- A fresh table is created with every column and comment, and nothing else runs.
- The previous build's table is migrated in this order: ADD COLUMNS, ONE comment ALTER, CLUSTER BY, the backfill;
  afterwards every column comment is current and the deprecated columns are marked DEPRECATED.
- A re-run changes no column or comment, and reports ALREADY_EXISTS when the backfill finds nothing.
- Fail closed: a failed statement raises; a table missing after the create raises. Only the backfill count read
  is best-effort.
"""
import re

import pytest

from pipeline_lib.log_table_migration import create_or_migrate
from pipeline_lib.monitoring_sink import DEPRECATED_COLUMNS, MONITORING_TABLE_COLUMNS, deprecated_comment

TABLE = "cat.sch.mon"
PREVIOUS_BUILD = {n: None for n in ("config_name", "job_run_id", "record_type", "batch_id", "event_ts",
                                    "batch_start_ts", "batch_end_ts", "payload", "ingest_ts", "status")}


class Field:
    def __init__(self, name, comment):
        self.name = name
        self.metadata = {"comment": comment} if comment is not None else {}


class Result:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


class FakeSpark:
    """`columns`: the existing table's {name: comment}, or None when the table does not exist."""

    def __init__(self, columns, updated=7, fail_on=None, create_noop=False):
        self.columns = None if columns is None else dict(columns)
        self.updated = updated
        self.fail_on = fail_on
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
        elif " ALTER COLUMN " in stmt:
            for name, comment in re.findall(r"(\w+) COMMENT '([^']*)'", stmt.split(" ALTER COLUMN ", 1)[1]):
                assert name in self.columns
                self.columns[name] = comment
        elif stmt.startswith("UPDATE "):
            return Result(self.updated if isinstance(self.updated, Exception) else [{"num_affected_rows": self.updated}])
        return Result([])

    def kinds(self):
        out = []
        for s in self.statements:
            out.append("CREATE" if s.startswith("CREATE") else "ADD" if " ADD COLUMNS " in s else
                       "COMMENT" if " ALTER COLUMN " in s else "CLUSTER" if " CLUSTER BY " in s else
                       "UPDATE" if s.startswith("UPDATE") else s)
        return out


def _current():
    return {n: c for n, _t, c in MONITORING_TABLE_COLUMNS}


def test_fresh_table_is_created_with_every_column_and_comment():
    spark = FakeSpark(None)
    summary = create_or_migrate(spark, TABLE, printer=lambda *_: None)
    assert spark.kinds() == ["CREATE"]
    assert spark.columns == _current()
    assert summary == f"log_table_create outcome=CREATED table={TABLE!r} exists_after=True"


def test_previous_build_table_is_migrated_in_order_with_comments_and_backfill():
    spark = FakeSpark(PREVIOUS_BUILD, updated=42)
    printed = []
    summary = create_or_migrate(spark, TABLE, printer=printed.append)
    assert spark.kinds() == ["CREATE", "ADD", "COMMENT", "CLUSTER", "UPDATE"]
    assert spark.statements[-1].endswith("WHERE logged_ts IS NULL AND ingest_ts IS NOT NULL")
    expected = {**_current(), **{old: deprecated_comment(new) for old, new in DEPRECATED_COLUMNS}}
    assert spark.columns == expected
    added = ["task_run_id", "start_ts", "end_ts", "docs_written", "error_type", "error_message",
             "files_outstanding", "bytes_outstanding", "logged_ts"]
    assert f"added={added}" in summary and "backfilled_rows=42" in summary and "outcome=MIGRATED(" in summary
    assert any("deprecated columns kept" in p and "ingest_ts" in p for p in printed)
    assert not any(p.startswith("WARNING") for p in printed)  # deprecated columns are not "unknown"


def test_rerun_after_migration_changes_nothing():
    spark = FakeSpark(PREVIOUS_BUILD, updated=42)
    create_or_migrate(spark, TABLE, printer=lambda *_: None)
    again = FakeSpark(spark.columns, updated=0)
    summary = create_or_migrate(again, TABLE, printer=lambda *_: None)
    assert again.kinds() == ["CREATE", "CLUSTER", "UPDATE"]  # no ADD, no comment ALTER
    assert "outcome=ALREADY_EXISTS" in summary


def test_rerun_after_restart_reports_the_rows_old_jobs_wrote_in_between():
    spark = FakeSpark(PREVIOUS_BUILD)
    create_or_migrate(spark, TABLE, printer=lambda *_: None)
    summary = create_or_migrate(FakeSpark(spark.columns, updated=3), TABLE, printer=lambda *_: None)
    assert "outcome=MIGRATED(added=[], commented=[], backfilled_rows=3)" in summary


def test_current_table_created_by_this_build_runs_no_backfill():
    spark = FakeSpark(_current())
    summary = create_or_migrate(spark, TABLE, printer=lambda *_: None)
    assert spark.kinds() == ["CREATE", "CLUSTER"]
    assert "outcome=ALREADY_EXISTS" in summary


def test_unknown_extra_column_is_warned_and_kept():
    spark = FakeSpark({**_current(), "mystery": None})
    printed = []
    create_or_migrate(spark, TABLE, printer=printed.append)
    assert "mystery" in spark.columns
    assert any(p.startswith("WARNING") and "mystery" in p for p in printed)


@pytest.mark.parametrize("stage", ["CREATE", "ALTER TABLE cat.sch.mon ADD", "ALTER TABLE cat.sch.mon ALTER",
                                   "ALTER TABLE cat.sch.mon CLUSTER", "UPDATE"])
def test_any_failed_statement_fails_closed(stage):
    with pytest.raises(RuntimeError, match="failed"):
        create_or_migrate(FakeSpark(PREVIOUS_BUILD, fail_on=stage), TABLE, printer=lambda *_: None)


def test_table_missing_after_create_fails_closed():
    with pytest.raises(RuntimeError, match="does not exist after CREATE"):
        create_or_migrate(FakeSpark(None, create_noop=True), TABLE, printer=lambda *_: None)


def test_backfill_count_read_failure_only_warns():
    printed = []
    summary = create_or_migrate(FakeSpark(PREVIOUS_BUILD, updated=OSError("no rows")), TABLE, printer=printed.append)
    assert "backfilled_rows=None" in summary
    assert any("could not read the backfilled row count" in p for p in printed)


def test_bad_table_name_fails_before_any_sql():
    spark = FakeSpark(None)
    with pytest.raises(ValueError):
        create_or_migrate(spark, "bad name", printer=lambda *_: None)
    assert spark.statements == []

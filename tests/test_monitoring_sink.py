"""Offline unit tests for pipeline_lib.monitoring_sink. No Spark, no cluster: plain pytest.

The load-bearing contracts:
- The DDL (MONITORING_TABLE_COLUMNS) and the row shape (ROW_FIELDS) cannot drift: ROW_FIELDS is exactly
  the columns minus the writer-supplied ingest_ts.
- Every row builder is FAIL-SOFT (a non-dict input yields None / [], never an exception) and stores the
  source verbatim in the payload (full fidelity, no re-typing of the connector's per-partition stats).
- validate_table_name is a strict ALLOW-LIST that fails closed, because the name is interpolated into a
  CREATE TABLE / INSERT string (injection guard).
"""
from datetime import datetime, timezone
import json

import pytest

from pipeline_lib.monitoring_sink import (
    CLUSTER_BY_COLUMNS,
    MONITORING_TABLE_COLUMNS,
    RECORD_TYPES,
    ROW_FIELDS,
    alter_add_columns_sql,
    alter_cluster_by_sql,
    assert_columns_consistent,
    bulk_stats_batch_row,
    bulk_stats_partition_rows,
    create_table_sql,
    missing_columns,
    optimize_sql,
    progress_row,
    prune_sql,
    run_error_row,
    run_summary_row,
    validate_table_name,
    vacuum_sql,
)

FIXED = datetime(2026, 9, 29, 21, 18, 32, 458000, tzinfo=timezone.utc)
START = datetime(2026, 9, 29, 21, 18, 30, 0, tzinfo=timezone.utc)
END = datetime(2026, 9, 29, 21, 18, 35, 500000, tzinfo=timezone.utc)


# --- schema invariants -------------------------------------------------------------------------

def test_columns_consistent_row_fields_are_columns_minus_ingest_ts():
    assert_columns_consistent()
    col_names = [name for name, _t in MONITORING_TABLE_COLUMNS]
    assert col_names[-1] == "ingest_ts"
    assert tuple(col_names[:-1]) == ROW_FIELDS


def test_table_has_expected_columns_and_types():
    cols = dict(MONITORING_TABLE_COLUMNS)
    assert cols["payload"] == "VARIANT"
    assert cols["batch_id"] == "BIGINT"
    assert cols["event_ts"] == "TIMESTAMP"
    assert cols["ingest_ts"] == "TIMESTAMP"


def test_record_types_are_the_closed_set():
    assert RECORD_TYPES == (
        "stream_progress",
        "bulk_stats_partition",
        "bulk_stats_batch",
        "run_summary",
        "run_error",
    )


def test_timing_columns_present_and_between_event_ts_and_payload():
    col_names = [name for name, _t in MONITORING_TABLE_COLUMNS]
    cols = dict(MONITORING_TABLE_COLUMNS)
    assert cols["batch_start_ts"] == "TIMESTAMP"
    assert cols["batch_end_ts"] == "TIMESTAMP"
    # Order: event_ts, batch_start_ts, batch_end_ts, payload (ingest_ts stays last).
    assert col_names.index("event_ts") < col_names.index("batch_start_ts")
    assert col_names.index("batch_start_ts") < col_names.index("batch_end_ts")
    assert col_names.index("batch_end_ts") < col_names.index("payload")


# --- progress_row ------------------------------------------------------------------------------

def test_progress_row_stores_whole_progress_and_promotes_batch_id():
    progress = {
        "name": "my_index",
        "batchId": 7,
        "numInputRows": 25,
        "durationMs": {"addBatch": 10, "commitOffsets": 2},
        "sources": [{"metrics": {"numFilesOutstanding": "3"}}],
    }
    row = progress_row(progress, "my_index", "run-123", now=FIXED)
    assert row["record_type"] == "stream_progress"
    assert row["config_name"] == "my_index"
    assert row["job_run_id"] == "run-123"
    assert row["batch_id"] == 7
    assert row["event_ts"] == "2026-09-29T21:18:32.458000"
    # Full fidelity: the entire progress dict round-trips through the payload.
    assert json.loads(row["payload"]) == progress
    assert set(row) == set(ROW_FIELDS)


def test_progress_row_non_dict_is_none():
    assert progress_row("not a dict", "c", "r") is None
    assert progress_row(None, "c", "r") is None


def test_progress_row_missing_batch_id_is_null():
    row = progress_row({"name": "x"}, "x", "r", now=FIXED)
    assert row["batch_id"] is None


# --- bulk_stats_partition_rows -----------------------------------------------------------------

def test_bulk_stats_partition_rows_one_per_partition_with_index_and_raw_fields():
    result = {
        "bulk_stats": [
            {"n_sends": 40, "docs_sent": 400000, "rejected_429": 5, "rtt_ms_max": 89.0},
            {"n_sends": 60, "docs_sent": 600000, "rejected_429": 3, "rtt_ms_max": 91.0},
        ],
        "collect_ms": 12345.6,
    }
    rows = bulk_stats_partition_rows(result, "cfg", "run-1", batch_id=4, now=FIXED)
    assert len(rows) == 2
    assert all(r["record_type"] == "bulk_stats_partition" for r in rows)
    assert all(r["batch_id"] == 4 for r in rows)
    p0 = json.loads(rows[0]["payload"])
    p1 = json.loads(rows[1]["payload"])
    assert p0["partition"] == 0 and p1["partition"] == 1
    # Raw per-partition fields preserved verbatim (no re-typing / rollup).
    assert p0["n_sends"] == 40 and p0["rejected_429"] == 5 and p0["rtt_ms_max"] == 89.0
    assert p1["docs_sent"] == 600000


def test_bulk_stats_partition_rows_no_stats_is_empty():
    assert bulk_stats_partition_rows({"written": 10}, "c", "r", 0) == []
    assert bulk_stats_partition_rows({"bulk_stats": "nope"}, "c", "r", 0) == []
    assert bulk_stats_partition_rows("not a dict", "c", "r", 0) == []


def test_bulk_stats_partition_rows_non_dict_entry_marked_not_raised():
    rows = bulk_stats_partition_rows({"bulk_stats": [None, 5]}, "c", "r", 1, now=FIXED)
    assert len(rows) == 2
    p0 = json.loads(rows[0]["payload"])
    assert p0["unparseable"] == "NoneType" and p0["partition"] == 0


# --- bulk_stats_batch_row ----------------------------------------------------------------------

def test_bulk_stats_batch_row_captures_driver_facts_and_partition_count():
    result = {
        "bulk_stats": [{"n_sends": 1}, {"n_sends": 2}, {"n_sends": 3}],
        "collect_ms": 100.5,
        "merge_ms": 7.6,
        "written": 999,
    }
    row = bulk_stats_batch_row(result, "cfg", "run-9", batch_id=2, now=FIXED)
    assert row["record_type"] == "bulk_stats_batch"
    assert row["batch_id"] == 2
    payload = json.loads(row["payload"])
    assert payload["collect_ms"] == 100.5
    assert payload["merge_ms"] == 7.6
    assert payload["written"] == 999
    assert payload["num_partitions"] == 3


def test_bulk_stats_batch_row_missing_stats_num_partitions_null():
    row = bulk_stats_batch_row({"written": 5}, "c", "r", None, now=FIXED)
    payload = json.loads(row["payload"])
    assert payload["num_partitions"] is None
    assert row["batch_id"] is None


def test_bulk_stats_batch_row_non_dict_is_none():
    assert bulk_stats_batch_row("x", "c", "r", 0) is None


# --- run_summary_row ---------------------------------------------------------------------------

def test_run_summary_row_batch_id_null_payload_verbatim():
    summary = {"streaming_start": "new", "es_index": "my_index", "batches": 42, "rows_pushed": 1000000}
    row = run_summary_row(summary, "cfg", "run-5", now=FIXED)
    assert row["record_type"] == "run_summary"
    assert row["batch_id"] is None
    assert json.loads(row["payload"]) == summary


def test_run_summary_row_non_dict_is_none():
    assert run_summary_row(None, "c", "r") is None


# --- event_ts / payload serialization ----------------------------------------------------------

def test_event_ts_naive_now_treated_as_utc():
    naive = datetime(2026, 1, 2, 3, 4, 5, 6000)
    row = run_summary_row({}, "c", "r", now=naive)
    assert row["event_ts"] == "2026-01-02T03:04:05.006000"


def test_event_ts_tzaware_converted_to_utc():
    # +02:00 wall clock 05:00 is 03:00 UTC.
    from datetime import timedelta
    tz = timezone(timedelta(hours=2))
    aware = datetime(2026, 1, 2, 5, 0, 0, tzinfo=tz)
    row = run_summary_row({}, "c", "r", now=aware)
    assert row["event_ts"].startswith("2026-01-02T03:00:00")


def test_payload_is_deterministic_sorted_json():
    row = run_summary_row({"b": 1, "a": 2}, "c", "r", now=FIXED)
    assert row["payload"] == '{"a":2,"b":1}'


def test_payload_serialization_is_fail_soft_on_odd_types():
    # A datetime is not JSON-serializable by default; default=str must stringify it, not raise.
    row = run_summary_row({"when": FIXED}, "c", "r", now=FIXED)
    payload = json.loads(row["payload"])
    assert "2026-09-29" in payload["when"]


# --- validate_table_name (allow-list, fail closed) --------------------------------------------

def test_validate_table_name_accepts_three_part_and_strips():
    assert validate_table_name("  cat.sch.tbl  ") == "cat.sch.tbl"
    assert validate_table_name("c1._s.t_2") == "c1._s.t_2"


@pytest.mark.parametrize("bad", [
    "",
    "   ",
    "onlyone",
    "two.parts",
    "a.b.c.d",
    "cat.sch.tbl; DROP TABLE x",
    "cat.sch.tbl--comment",
    "cat.sch.`weird name`",
    "1cat.sch.tbl",          # part may not start with a digit
    "cat.sch.tbl WHERE 1=1",
])
def test_validate_table_name_rejects_bad(bad):
    with pytest.raises(ValueError):
        validate_table_name(bad)


def test_validate_table_name_non_string_rejected():
    with pytest.raises(ValueError):
        validate_table_name(None)


# --- create_table_sql --------------------------------------------------------------------------

def test_create_table_sql_is_idempotent_and_has_every_column():
    sql = create_table_sql("cat.sch.monitoring")
    assert "CREATE TABLE IF NOT EXISTS cat.sch.monitoring" in sql
    assert "USING DELTA" in sql
    for name, sql_type in MONITORING_TABLE_COLUMNS:
        assert f"{name} {sql_type}" in sql


def test_create_table_sql_validates_name():
    with pytest.raises(ValueError):
        create_table_sql("bad name")


# --- batch_start_ts / batch_end_ts timing columns ---------------------------------------------

def test_optional_timing_defaults_to_null():
    # A builder with no start/end stores NULL (NOT 'now') for the timing columns, unlike event_ts.
    row = run_summary_row({"a": 1}, "c", "r", now=FIXED)
    assert row["event_ts"] == "2026-09-29T21:18:32.458000"
    assert row["batch_start_ts"] is None
    assert row["batch_end_ts"] is None


def test_bulk_stats_batch_row_carries_passed_start_end():
    row = bulk_stats_batch_row({"written": 5}, "cfg", "run-1", batch_id=3,
                               now=FIXED, batch_start=START, batch_end=END)
    assert row["batch_start_ts"] == "2026-09-29T21:18:30.000000"
    assert row["batch_end_ts"] == "2026-09-29T21:18:35.500000"


def test_progress_row_derives_start_end_from_timestamp_and_duration():
    progress = {"name": "x", "batchId": 2, "timestamp": "2026-09-29T21:18:30.000Z", "batchDuration": 5500}
    row = progress_row(progress, "x", "r", now=FIXED)
    # start = the progress timestamp (UTC); end = start + batchDuration ms.
    assert row["batch_start_ts"] == "2026-09-29T21:18:30.000000"
    assert row["batch_end_ts"] == "2026-09-29T21:18:35.500000"


def test_progress_row_bad_timestamp_leaves_timing_null():
    assert progress_row({"timestamp": "not-a-date"}, "x", "r", now=FIXED)["batch_start_ts"] is None
    # Missing timestamp => both NULL (but the row still exists with its payload).
    row = progress_row({"batchId": 1}, "x", "r", now=FIXED)
    assert row["batch_start_ts"] is None and row["batch_end_ts"] is None


def test_progress_row_missing_duration_end_equals_start():
    progress = {"timestamp": "2026-09-29T21:18:30.000Z"}
    row = progress_row(progress, "x", "r", now=FIXED)
    assert row["batch_start_ts"] == row["batch_end_ts"] == "2026-09-29T21:18:30.000000"


# --- run_error_row -----------------------------------------------------------------------------

def test_run_error_row_records_exception_facts_and_timing():
    err = {"exception_type": "ConnectionTimeout", "message": "timed out", "mode": "batch", "elapsed_ms": 60000}
    row = run_error_row(err, "cfg", "run-7", now=FIXED, batch_start=START, batch_end=END)
    assert row["record_type"] == "run_error"
    assert row["batch_id"] is None
    assert json.loads(row["payload"]) == err
    assert row["batch_start_ts"] == "2026-09-29T21:18:30.000000"
    assert row["batch_end_ts"] == "2026-09-29T21:18:35.500000"
    assert set(row) == set(ROW_FIELDS)


def test_run_error_row_non_dict_is_none():
    assert run_error_row(None, "c", "r") is None
    assert run_error_row("boom", "c", "r") is None


# --- liquid clustering in CREATE ---------------------------------------------------------------

def test_create_table_sql_has_cluster_by_before_tblproperties():
    sql = create_table_sql("cat.sch.monitoring")
    assert f"CLUSTER BY ({', '.join(CLUSTER_BY_COLUMNS)})" in sql
    assert sql.index("CLUSTER BY") < sql.index("TBLPROPERTIES")
    assert sql.index("USING DELTA") < sql.index("CLUSTER BY")


# --- additive migration: missing_columns / alter_add_columns_sql / alter_cluster_by_sql --------

def test_missing_columns_additive_only():
    all_names = [name for name, _t in MONITORING_TABLE_COLUMNS]
    # Nothing missing when every column is present (order-insensitive).
    assert missing_columns(list(reversed(all_names))) == []
    # An old table lacking the two timing columns => exactly those two are reported, with types.
    old = [n for n in all_names if n not in ("batch_start_ts", "batch_end_ts")]
    assert missing_columns(old) == [("batch_start_ts", "TIMESTAMP"), ("batch_end_ts", "TIMESTAMP")]
    # Extra columns in the table are NEVER reported for dropping (additive allow-list).
    assert missing_columns(all_names + ["some_future_col"]) == []


def test_alter_add_columns_sql_additive_and_none_when_empty():
    sql = alter_add_columns_sql("cat.sch.t", [("batch_start_ts", "TIMESTAMP"), ("batch_end_ts", "TIMESTAMP")])
    assert sql == "ALTER TABLE cat.sch.t ADD COLUMNS (batch_start_ts TIMESTAMP, batch_end_ts TIMESTAMP)"
    assert alter_add_columns_sql("cat.sch.t", []) is None


def test_alter_cluster_by_sql():
    assert alter_cluster_by_sql("cat.sch.t") == f"ALTER TABLE cat.sch.t CLUSTER BY ({', '.join(CLUSTER_BY_COLUMNS)})"


@pytest.mark.parametrize("fn", [alter_cluster_by_sql, optimize_sql, vacuum_sql])
def test_maintenance_sql_fail_closed_on_bad_name(fn):
    with pytest.raises(ValueError):
        fn("bad name")


def test_alter_add_columns_sql_fail_closed_on_bad_name():
    with pytest.raises(ValueError):
        alter_add_columns_sql("bad name", [("x", "STRING")])


# --- retention / maintenance: prune_sql / optimize_sql / vacuum_sql ----------------------------

def test_prune_sql_builds_delete_with_interval():
    assert prune_sql("cat.sch.t", 90) == (
        "DELETE FROM cat.sch.t WHERE ingest_ts < current_timestamp() - INTERVAL 90 DAYS"
    )


def test_prune_sql_disabled_when_retention_non_positive():
    assert prune_sql("cat.sch.t", 0) is None
    assert prune_sql("cat.sch.t", -5) is None


def test_prune_sql_fail_closed_on_bad_name_and_bad_days():
    with pytest.raises(ValueError):
        prune_sql("bad name", 90)
    with pytest.raises(ValueError):
        prune_sql("cat.sch.t", "ninety")


def test_optimize_and_vacuum_sql():
    assert optimize_sql("cat.sch.t") == "OPTIMIZE cat.sch.t"
    assert vacuum_sql("cat.sch.t") == "VACUUM cat.sch.t"

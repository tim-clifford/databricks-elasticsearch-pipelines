"""Offline unit tests for pipeline_lib.monitoring_sink. No Spark, no cluster: plain pytest.

The load-bearing contracts:
- The DDL (MONITORING_TABLE_COLUMNS) and the row shape (ROW_FIELDS) cannot drift: ROW_FIELDS is exactly
  the columns minus the writer-supplied ingest_ts.
- The row vocabulary is CLOSED: RECORD_TYPES x STATUSES, with each record_type allowed only its own
  statuses, and batch rows (and only batch rows) carrying a batch id.
- Builders FAIL CLOSED: a malformed row raises rather than being silently dropped, because with the log on
  a missing row is exactly the gap the row model exists to prevent.
- validate_table_name is a strict ALLOW-LIST that fails closed, because the name is interpolated into a
  CREATE TABLE / INSERT string (injection guard).
"""
from datetime import datetime, timedelta, timezone
import json

import pytest

from pipeline_lib.monitoring_sink import (
    BATCH_MODE_BATCH_ID,
    CLUSTER_BY_COLUMNS,
    MAX_ERROR_MESSAGE_CHARS,
    MONITORING_TABLE_COLUMNS,
    RECORD_TYPES,
    ROW_FIELDS,
    STATUSES,
    alter_add_columns_sql,
    alter_cluster_by_sql,
    assert_columns_consistent,
    batch_end_row,
    batch_start_row,
    batch_success_facts,
    batch_summary_row,
    create_table_sql,
    error_facts,
    es_counts,
    es_write_summary,
    missing_columns,
    optimize_sql,
    progress_batch_ids,
    prune_sql,
    run_end_row,
    run_start_row,
    validate_table_name,
    vacuum_sql,
)
from pipeline_lib.observability import bulk_stats_overall, bulk_stats_tail

FIXED = datetime(2026, 9, 29, 21, 18, 32, 458000, tzinfo=timezone.utc)
START = datetime(2026, 9, 29, 21, 18, 30, 0, tzinfo=timezone.utc)
END = datetime(2026, 9, 29, 21, 18, 35, 500000, tzinfo=timezone.utc)

PART = {"n_sends": 4, "docs_sent": 400, "bytes_sent": 40000, "send_busy_ms": 900.0,
        "partition_wall_ms": 1000.0, "rtt_ms_mean": 20.0, "rtt_ms_max": 50.0, "took_ms_mean": 10.0,
        "took_ms_max": 30.0}
RESULT = {"written": 800, "deleted": 0, "errors": 0, "ignored": 0, "total_input": 800,
          "collect_ms": 1200.5, "merge_ms": 0.4, "bulk_stats": [PART, dict(PART, partition_wall_ms=3000.0)]}
PROGRESS = {"id": "q", "runId": "r", "name": "cfg-1234", "batchId": 7, "numInputRows": 800,
            "timestamp": "2026-09-29T21:18:30.000Z", "batchDuration": 5500,
            "durationMs": {"addBatch": 5000, "triggerExecution": 5500},
            "sources": [{"metrics": {"numFilesOutstanding": "3"}}]}


# --- schema invariants -------------------------------------------------------------------------

def test_columns_consistent_row_fields_are_columns_minus_ingest_ts():
    assert_columns_consistent()
    col_names = [name for name, _t in MONITORING_TABLE_COLUMNS]
    assert col_names[-1] == "ingest_ts"
    assert tuple(col_names[:-1]) == ROW_FIELDS


def test_table_has_expected_columns_and_types():
    cols = dict(MONITORING_TABLE_COLUMNS)
    assert cols["payload"] == "VARIANT"
    assert cols["status"] == "STRING"
    assert cols["batch_id"] == "BIGINT"
    assert cols["event_ts"] == "TIMESTAMP"
    assert cols["batch_start_ts"] == "TIMESTAMP"
    assert cols["batch_end_ts"] == "TIMESTAMP"
    assert cols["ingest_ts"] == "TIMESTAMP"


def test_record_types_and_statuses_are_the_closed_sets():
    assert RECORD_TYPES == ("run_start", "run_end", "batch_start", "batch_end", "batch_summary")
    assert STATUSES == ("started", "success", "error", "stopped")


def test_batch_mode_batch_id_is_zero():
    assert BATCH_MODE_BATCH_ID == 0


# --- run rows ----------------------------------------------------------------------------------

def test_run_start_row_shape():
    row = run_start_row("cfg", "run1", {"mode": "batch", "es_index": "idx"}, START, now=FIXED)
    assert set(row) == set(ROW_FIELDS)
    assert (row["record_type"], row["status"], row["batch_id"]) == ("run_start", "started", None)
    assert row["batch_start_ts"] == "2026-09-29T21:18:30.000000"
    assert row["batch_end_ts"] is None
    assert json.loads(row["payload"]) == {"mode": "batch", "es_index": "idx"}
    assert row["event_ts"] == "2026-09-29T21:18:32.458000"


@pytest.mark.parametrize("status", ["success", "error", "stopped"])
def test_run_end_row_allows_terminal_statuses(status):
    row = run_end_row("cfg", "run1", status, {"x": 1}, START, END, now=FIXED)
    assert (row["record_type"], row["status"], row["batch_id"]) == ("run_end", status, None)
    assert row["batch_start_ts"] == "2026-09-29T21:18:30.000000"
    assert row["batch_end_ts"] == "2026-09-29T21:18:35.500000"


def test_run_end_row_rejects_started():
    with pytest.raises(ValueError, match="not allowed for run_end"):
        run_end_row("cfg", "run1", "started", {}, START, END)


def test_run_rows_reject_a_batch_id():
    from pipeline_lib import monitoring_sink as ms
    with pytest.raises(ValueError, match="run row"):
        ms._row("cfg", "r", "run_start", "started", 3, {})


# --- batch rows --------------------------------------------------------------------------------

def test_batch_start_row_shape():
    row = batch_start_row("cfg", "run1", 5, {"mode": "streaming"}, START, now=FIXED)
    assert (row["record_type"], row["status"], row["batch_id"]) == ("batch_start", "started", 5)
    assert row["batch_start_ts"] == "2026-09-29T21:18:30.000000"
    assert row["batch_end_ts"] is None


@pytest.mark.parametrize("status", ["success", "error"])
def test_batch_end_row_allows_success_and_error(status):
    row = batch_end_row("cfg", "run1", 0, status, {"written": 1}, START, END, now=FIXED)
    assert (row["record_type"], row["status"], row["batch_id"]) == ("batch_end", status, 0)
    assert row["batch_end_ts"] == "2026-09-29T21:18:35.500000"


def test_batch_end_row_rejects_stopped():
    with pytest.raises(ValueError, match="not allowed for batch_end"):
        batch_end_row("cfg", "run1", 0, "stopped", {}, START, END)


@pytest.mark.parametrize("bad", [None, -1, True, "3", 2.0])
def test_batch_rows_require_non_negative_int_batch_id(bad):
    with pytest.raises(ValueError, match="batch_id"):
        batch_start_row("cfg", "run1", bad, {}, START)


@pytest.mark.parametrize("bad", [None, [], "x", 3])
def test_rows_reject_non_dict_payload(bad):
    with pytest.raises(ValueError, match="payload must be a dict"):
        batch_start_row("cfg", "run1", 0, bad, START)


def test_unknown_record_type_rejected():
    from pipeline_lib import monitoring_sink as ms
    with pytest.raises(ValueError, match="unknown record_type"):
        ms._row("cfg", "r", "stream_progress", "success", 1, {})


# --- batch_summary (streaming only: Spark's progress) ------------------------------------------

def test_batch_summary_stores_whole_progress_with_progress_timing():
    row = batch_summary_row("cfg", "run1", 7, PROGRESS, now=FIXED)
    assert (row["record_type"], row["status"], row["batch_id"]) == ("batch_summary", "success", 7)
    assert json.loads(row["payload"]) == {"progress": PROGRESS}
    # start = progress timestamp, end = start + batchDuration (5.5 s).
    assert row["batch_start_ts"] == "2026-09-29T21:18:30.000000"
    assert row["batch_end_ts"] == "2026-09-29T21:18:35.500000"


@pytest.mark.parametrize("bad", [None, "x", ["p"], 3])
def test_batch_summary_requires_a_progress_dict(bad):
    with pytest.raises(ValueError, match="progress must be a dict"):
        batch_summary_row("cfg", "run1", 7, bad)


def test_batch_summary_only_success_status():
    from pipeline_lib import monitoring_sink as ms
    with pytest.raises(ValueError, match="not allowed for batch_summary"):
        ms._row("cfg", "r", "batch_summary", "error", 1, {})


def test_batch_summary_bad_progress_timestamp_leaves_timing_null():
    row = batch_summary_row("cfg", "run1", 7, dict(PROGRESS, timestamp="nope"), now=FIXED)
    assert row["batch_start_ts"] is None and row["batch_end_ts"] is None


def test_batch_summary_missing_duration_end_equals_start():
    p = {k: v for k, v in PROGRESS.items() if k != "batchDuration"}
    row = batch_summary_row("cfg", "run1", 7, p, now=FIXED)
    assert row["batch_start_ts"] == row["batch_end_ts"] == "2026-09-29T21:18:30.000000"


def test_batch_success_facts_are_counts_plus_es_rollup():
    facts = batch_success_facts(RESULT, wall_ms=1300.0)
    assert facts == {**es_counts(RESULT), "es": es_write_summary(RESULT, wall_ms=1300.0)}
    assert facts["written"] == 800 and facts["es"]["bulk_write_wall_ms"] == 1300.0


# --- es_counts / es_write_summary / error_facts ------------------------------------------------

def test_es_counts_takes_exactly_the_counts():
    assert es_counts(RESULT) == {"written": 800, "deleted": 0, "errors": 0, "ignored": 0, "total_input": 800}


def test_es_counts_rejects_non_dict():
    with pytest.raises(ValueError):
        es_counts(None)


def test_es_write_summary_uses_the_shared_rollups_and_drops_per_partition_detail():
    s = es_write_summary(RESULT, wall_ms=1300.0)
    assert s["collect_ms"] == 1200.5 and s["merge_ms"] == 0.4
    assert s["num_partitions"] == 2 and s["bulk_write_wall_ms"] == 1300.0
    # The SAME computation as the BULK_STATS log lines (single source of truth).
    assert s["overall"] == bulk_stats_overall(RESULT["bulk_stats"])
    tail = bulk_stats_tail(RESULT)
    assert s["tail"] == {k: v for k, v in tail.items() if k not in ("collect_ms", "merge_ms")}
    assert s["tail"]["slowest_partition"] == 1
    # No raw per-partition list in the payload.
    assert "bulk_stats" not in s and "partitions" not in s


def test_es_write_summary_without_bulk_stats_has_driver_facts_only():
    s = es_write_summary({"written": 3, "collect_ms": 5, "merge_ms": 1})
    assert s == {"collect_ms": 5, "merge_ms": 1, "num_partitions": None, "bulk_write_wall_ms": None}


def test_es_write_summary_empty_bulk_stats_list_has_no_rollups():
    s = es_write_summary({"written": 0, "collect_ms": 2, "merge_ms": 0, "bulk_stats": []})
    assert s == {"collect_ms": 2, "merge_ms": 0, "num_partitions": 0, "bulk_write_wall_ms": None}


def test_es_write_summary_rejects_non_dict():
    with pytest.raises(ValueError):
        es_write_summary("x")


def test_error_facts_type_and_message():
    assert error_facts(ValueError("boom")) == {"exception_type": "ValueError", "message": "boom"}


def test_error_facts_caps_long_messages():
    facts = error_facts(RuntimeError("x" * (MAX_ERROR_MESSAGE_CHARS + 5)))
    assert len(facts["message"]) == MAX_ERROR_MESSAGE_CHARS
    assert facts["message_truncated"] is True


# --- progress_batch_ids (the wait loop's dedup) ------------------------------------------------

def _p(bid, executed=True):
    d = {"batchId": bid, "durationMs": {"triggerExecution": 1}}
    if executed:
        d["durationMs"]["addBatch"] = 1
    return d


def test_progress_batch_ids_above_the_mark_in_ascending_order():
    got = progress_batch_ids([_p(3), _p(1), _p(2)], last_batch_id=1)
    assert [p["batchId"] for p in got] == [2, 3]


def test_progress_batch_ids_none_mark_takes_everything_executed():
    got = progress_batch_ids([_p(0), _p(1)], last_batch_id=None)
    assert [p["batchId"] for p in got] == [0, 1]


def test_progress_batch_ids_skips_idle_reports_without_addbatch():
    got = progress_batch_ids([_p(4, executed=False), _p(4), _p(5, executed=False)], last_batch_id=None)
    assert [p["batchId"] for p in got] == [4]


def test_progress_batch_ids_first_report_for_an_id_wins():
    first, dup = _p(9), _p(9)
    dup["marker"] = "dup"
    assert progress_batch_ids([first, dup], last_batch_id=None) == [first]


@pytest.mark.parametrize("junk", [None, "x", {"batchId": "1", "durationMs": {"addBatch": 1}},
                                  {"batchId": True, "durationMs": {"addBatch": 1}}, {"batchId": 1}])
def test_progress_batch_ids_skips_unusable_entries(junk):
    assert progress_batch_ids([junk], last_batch_id=None) == []


def test_progress_batch_ids_empty_input():
    assert progress_batch_ids(None, last_batch_id=None) == []
    assert progress_batch_ids([], last_batch_id=3) == []


# --- event_ts / payload serialization ----------------------------------------------------------

def test_event_ts_naive_now_treated_as_utc():
    naive = datetime(2026, 1, 2, 3, 4, 5, 6000)
    row = run_start_row("c", "r", {}, None, now=naive)
    assert row["event_ts"] == "2026-01-02T03:04:05.006000"


def test_event_ts_tzaware_converted_to_utc():
    # +02:00 wall clock 05:00 is 03:00 UTC.
    aware = datetime(2026, 1, 2, 5, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    row = run_start_row("c", "r", {}, None, now=aware)
    assert row["event_ts"].startswith("2026-01-02T03:00:00")


def test_payload_is_deterministic_sorted_json():
    row = run_start_row("c", "r", {"b": 1, "a": 2}, None, now=FIXED)
    assert row["payload"] == '{"a":2,"b":1}'


def test_payload_serialization_is_fail_soft_on_odd_types():
    # A datetime is not JSON-serializable by default; default=str must stringify it, not raise.
    row = run_start_row("c", "r", {"when": FIXED}, None, now=FIXED)
    assert "2026-09-29" in json.loads(row["payload"])["when"]


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

"""Offline unit tests for pipeline_lib.feed_status. No Spark, no cluster: plain pytest.

The load-bearing contracts:
- previous_fire returns the latest Quartz fire time <= now for the supported cron subset, and rejects
  (UnsupportedCron) anything outside it rather than guessing.
- classify_streaming never reports CAUGHT_UP / IN_PROGRESS / PENDING from a history window that does not
  provably reach the sent position (it returns None to widen instead), so a commit that lands between
  reads cannot hide the oldest unsent one.
- Leftover in-flight offsets only count as a send while the latest logged run is still open.
- Checkpoint offset parsing and the ignorable-operation set are allow-lists that fail closed.
"""
import json
from datetime import datetime, timezone

import pytest

from pipeline_lib.feed_status import UnsupportedCron, parse_quartz_cron, previous_fire


def utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------------------------------
# previous_fire / parse_quartz_cron
# ---------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("expr,now,expected", [
    # every 10 minutes (the ecs_dns_activity schedule)
    ("0 */10 * * * ?", utc(2026, 10, 5, 12, 34, 56), utc(2026, 10, 5, 12, 30, 0)),
    # exactly on a fire time => that fire time (<= now)
    ("0 */10 * * * ?", utc(2026, 10, 5, 12, 30, 0), utc(2026, 10, 5, 12, 30, 0)),
    # one second before a fire time => the previous one
    ("0 */10 * * * ?", utc(2026, 10, 5, 12, 29, 59), utc(2026, 10, 5, 12, 20, 0)),
    # daily 03:00, before today's fire => yesterday's
    ("0 0 3 * * ?", utc(2026, 10, 5, 2, 59, 59), utc(2026, 10, 4, 3, 0, 0)),
    # daily 03:00, after today's fire => today's
    ("0 0 3 * * ?", utc(2026, 10, 5, 3, 0, 1), utc(2026, 10, 5, 3, 0, 0)),
    # crossing midnight and a month boundary
    ("0 0 23 * * ?", utc(2026, 11, 1, 0, 5, 0), utc(2026, 10, 31, 23, 0, 0)),
    # day-of-week (Quartz 2=MON); 2026-10-05 is a Monday
    ("0 0 9 ? * MON", utc(2026, 10, 5, 8, 0, 0), utc(2026, 9, 28, 9, 0, 0)),
    ("0 0 9 ? * 2", utc(2026, 10, 5, 9, 30, 0), utc(2026, 10, 5, 9, 0, 0)),
    # Sunday is Quartz 1; 2026-10-04 is a Sunday
    ("0 0 9 ? * SUN", utc(2026, 10, 5, 8, 0, 0), utc(2026, 10, 4, 9, 0, 0)),
    # day-of-month + month names + lists + ranges
    ("0 15 6 1 JAN,JUL ?", utc(2026, 10, 5, 0, 0, 0), utc(2026, 7, 1, 6, 15, 0)),
    ("0 0 8-10 * * ?", utc(2026, 10, 5, 12, 0, 0), utc(2026, 10, 5, 10, 0, 0)),
    # N/STEP from a start value; seconds field honored
    ("30 5/15 * * * ?", utc(2026, 10, 5, 12, 21, 0), utc(2026, 10, 5, 12, 20, 30)),
    # explicit 7th year field
    ("0 0 0 1 1 ? 2025", utc(2026, 10, 5, 0, 0, 0), utc(2025, 1, 1, 0, 0, 0)),
    # Feb 29 only exists in leap years
    ("0 0 0 29 2 ?", utc(2026, 10, 5, 0, 0, 0), utc(2024, 2, 29, 0, 0, 0)),
])
def test_previous_fire(expr, now, expected):
    assert previous_fire(expr, now) == expected


def test_previous_fire_converts_non_utc_now():
    from datetime import timedelta
    plus2 = timezone(timedelta(hours=2))
    # 14:34 at +02:00 is 12:34 UTC
    assert previous_fire("0 */10 * * * ?", datetime(2026, 10, 5, 14, 34, tzinfo=plus2)) == utc(2026, 10, 5, 12, 30)


@pytest.mark.parametrize("expr", [
    "0 0 12 L * ?",        # last day of month
    "0 0 12 15W * ?",      # nearest weekday
    "0 0 12 ? * 6#3",      # third Friday
    "0 0 12 * * *",        # neither day field is '?'
    "0 0 12 ? * ?",        # both day fields are '?'
    "0 0 12 * *",          # 5-field Unix cron
    "0 60 * * * ?",        # out of range
    "0 0 12 * FOO ?",      # unknown name
])
def test_unsupported_or_malformed_cron_raises(expr):
    with pytest.raises(UnsupportedCron):
        previous_fire(expr, utc(2026, 10, 5))


def test_cron_with_no_fire_in_lookback_window_raises():
    # 2020 is inside the 8-year lookback from 2026, so it fires; 2000 is outside it, so it raises.
    assert previous_fire("0 0 0 1 1 ? 2020", utc(2026, 10, 5)) == utc(2020, 1, 1)
    with pytest.raises(UnsupportedCron):
        previous_fire("0 0 0 1 1 ? 2000", utc(2026, 10, 5))


def test_parse_rejects_non_string():
    with pytest.raises(UnsupportedCron):
        parse_quartz_cron(None)


# ---------------------------------------------------------------------------------------------------
# Classifier fixtures
# ---------------------------------------------------------------------------------------------------

from datetime import timedelta  # noqa: E402

from pipeline_lib.feed_status import (  # noqa: E402
    BEHIND,
    CAUGHT_UP,
    IGNORED_OPERATIONS,
    IN_PROGRESS,
    PENDING,
    REASONS,
    RESULT_FIELDS,
    STATUS_TABLE_COLUMNS,
    STATUSES,
    UNKNOWN,
    _result,
    carries_rows,
    classify_batch,
    classify_streaming,
    create_status_table_sql,
    feed_triggers,
    history_covers,
    latest_run,
    merge_status_sql,
    next_history_limit,
    parse_delta_offset,
    run_state_sql,
    summarize_checkpoint,
    to_row,
)

NOW = utc(2026, 10, 5, 12, 0, 0)


def offset_text(version, index=-1, rid="rid-1"):
    meta = json.dumps({"batchWatermarkMs": 0, "batchTimestampMs": 0, "conf": {}})
    src = json.dumps({"sourceVersion": 1, "reservoirId": rid, "reservoirVersion": version,
                      "index": index, "isStartingVersion": False})
    return f"v1\n{meta}\n{src}"


def ckpt(committed_version, committed_index=-1, in_flight=None, since=None):
    """A summarize_checkpoint-shaped dict. in_flight = (version, index) or None."""
    return {
        "state": "ok",
        "committed": {"reservoir_version": committed_version, "index": committed_index, "reservoir_id": "r"},
        "in_flight": None if in_flight is None else
        {"reservoir_version": in_flight[0], "index": in_flight[1], "reservoir_id": "r"},
        "in_flight_since": since,
    }


def hist(*commits):
    """DESCRIBE HISTORY rows, newest first like the real output: (version, minutes_ago, operation)."""
    rows = [{"version": v, "timestamp": NOW - timedelta(minutes=m), "operation": op} for v, m, op in commits]
    return sorted(rows, key=lambda r: -r["version"])


RUNNING = {"job_run_id": "r9", "started_at": NOW - timedelta(minutes=5), "run_ended_at": None, "run_end_status": None}
ENDED = {"job_run_id": "r8", "started_at": NOW - timedelta(minutes=30), "run_ended_at": NOW - timedelta(minutes=20),
         "run_end_status": "success"}


# ---------------------------------------------------------------------------------------------------
# parse_delta_offset / summarize_checkpoint
# ---------------------------------------------------------------------------------------------------

def test_parse_delta_offset_reads_last_line():
    assert parse_delta_offset(offset_text(9, -1)) == {"reservoir_version": 9, "index": -1, "reservoir_id": "rid-1"}
    assert parse_delta_offset(offset_text(8, 2))["index"] == 2


@pytest.mark.parametrize("text", [
    None, "", "v1\n{}", "v1\n{}\nnot json", "v1\n{}\n[1]",
    'v1\n{}\n{"reservoirVersion": "9", "index": -1}',
    'v1\n{}\n{"reservoirVersion": 9}',
    'v1\n{}\n{"reservoirVersion": true, "index": -1}',
    'v1\n{}\n{"reservoirVersion": -1, "index": -1}',
    'v1\n{}\n{"reservoirVersion": 3, "index": -2}',
    'x1\n{}\n{"reservoirVersion": 3, "index": -1}',
])
def test_parse_delta_offset_fails_closed(text):
    with pytest.raises(ValueError):
        parse_delta_offset(text)


def _reader(texts, mtimes=None):
    return (lambda b: texts[b]), (lambda b: (mtimes or {})[b])


def test_summarize_checkpoint_states():
    r, m = _reader({})
    assert summarize_checkpoint(None, None, r, m)["state"] == "no_checkpoint"
    assert summarize_checkpoint([], ["0"], r, m)["state"] == "no_checkpoint"
    assert summarize_checkpoint(["0"], None, r, m)["state"] == "no_committed_batch"


def test_summarize_checkpoint_committed_only():
    r, m = _reader({4: offset_text(8)})
    out = summarize_checkpoint(["3", "4", ".tmp"], ["3", "4/"], r, m)
    assert out["state"] == "ok"
    assert out["committed"]["reservoir_version"] == 8
    assert out["in_flight"] is None and out["in_flight_since"] is None


def test_summarize_checkpoint_multiple_in_flight_uses_newest_offset_and_first_mtime():
    # The live probe left offsets 6 and 7 uncommitted after a failure (commits through 5).
    t6, t7 = NOW - timedelta(minutes=3), NOW - timedelta(minutes=2)
    r, m = _reader({5: offset_text(8, 0), 6: offset_text(8, 1), 7: offset_text(8, 2)}, {6: t6, 7: t7})
    out = summarize_checkpoint([str(i) for i in range(8)], [str(i) for i in range(6)], r, m)
    assert out["committed"] == {"reservoir_version": 8, "index": 0, "reservoir_id": "rid-1"}
    assert out["in_flight"]["index"] == 2
    assert out["in_flight_since"] == t6


def test_summarize_checkpoint_commit_without_offset_raises():
    r, m = _reader({})
    with pytest.raises(ValueError):
        summarize_checkpoint(["1"], ["2"], r, m)


# ---------------------------------------------------------------------------------------------------
# history helpers
# ---------------------------------------------------------------------------------------------------

def test_carries_rows_is_an_allow_list_of_ignorable_operations():
    for op in ("WRITE", "STREAMING UPDATE", "CREATE TABLE AS SELECT", "COPY INTO", "SOMETHING NEW"):
        assert carries_rows(op)
    for op in IGNORED_OPERATIONS:
        assert not carries_rows(op)
    assert "WRITE" not in IGNORED_OPERATIONS


def test_history_covers():
    h = hist((12, 1, "WRITE"), (11, 2, "WRITE"), (10, 3, "WRITE"))
    assert history_covers(h, limit=3, from_version=10)
    assert history_covers(h, limit=3, from_version=11)
    assert not history_covers(h, limit=3, from_version=9)   # version 9 may exist below the window
    # fewer rows than the limit is the whole RETAINED history: covers only if it reaches the version
    assert history_covers(h, limit=20, from_version=10)
    assert not history_covers(h, limit=20, from_version=0)
    assert not history_covers([], limit=20, from_version=0)


def test_next_history_limit_doubles_to_cap():
    assert next_history_limit(20, cap=1000) == 40
    assert next_history_limit(640, cap=1000) == 1000
    assert next_history_limit(1000, cap=1000) is None


# ---------------------------------------------------------------------------------------------------
# classify_streaming
# ---------------------------------------------------------------------------------------------------

def s(checkpoint, history, run=None, limit=20):
    return classify_streaming(checkpoint, history, limit, run, NOW)


def test_streaming_caught_up_when_nothing_after_sent_position():
    out = s(ckpt(9), hist((8, 30, "WRITE"), (7, 40, "WRITE")))
    assert (out["status"], out["status_reason"]) == (CAUGHT_UP, "no_unsent_data")
    assert out["sent_through_version"] == 8 and out["source_version"] == 8
    assert out["oldest_unsent_ts"] is None and out["lag_minutes"] is None


def test_streaming_ignored_operations_after_sent_position_are_caught_up():
    out = s(ckpt(9), hist((11, 1, "OPTIMIZE"), (10, 2, "SET TBLPROPERTIES"), (9, 3, "UPDATE"), (8, 30, "WRITE")))
    assert out["status"] == CAUGHT_UP


def test_streaming_pending_when_unsent_and_young_and_no_send():
    out = s(ckpt(9), hist((9, 10, "WRITE"), (8, 30, "WRITE")), run=ENDED)
    assert (out["status"], out["status_reason"]) == (PENDING, "unsent_within_threshold")
    assert out["oldest_unsent_ts"] == NOW - timedelta(minutes=10)
    assert out["lag_minutes"] == pytest.approx(10.0)


def test_streaming_behind_when_unsent_past_threshold():
    out = s(ckpt(9), hist((10, 5, "WRITE"), (9, 61, "WRITE"), (8, 90, "WRITE")), run=ENDED)
    assert (out["status"], out["status_reason"]) == (BEHIND, "unsent_over_threshold")
    assert out["oldest_unsent_ts"] == NOW - timedelta(minutes=61)


def test_streaming_threshold_boundary_is_behind_at_exactly_one_hour():
    assert s(ckpt(9), hist((9, 60, "WRITE")), run=ENDED)["status"] == BEHIND
    assert s(ckpt(9), hist((9, 59, "WRITE")), run=ENDED)["status"] == PENDING


def test_streaming_unknown_operation_counts_as_data():
    assert s(ckpt(9), hist((9, 5, "BRAND NEW OP")), run=ENDED)["status"] == PENDING


def test_streaming_partial_commit_is_still_unsent():
    # committed (8, 0): file 0 of version 8 sent, the rest not.
    out = s(ckpt(8, 0), hist((8, 70, "WRITE"), (7, 90, "WRITE")), run=ENDED)
    assert out["status"] == BEHIND and out["sent_through_version"] == 7


def test_streaming_in_progress_when_active_send_covers_unsent():
    since = NOW - timedelta(minutes=2)
    out = s(ckpt(9, in_flight=(11, -1), since=since), hist((10, 3, "WRITE"), (9, 4, "WRITE")), run=RUNNING)
    assert (out["status"], out["status_reason"]) == (IN_PROGRESS, "sending")
    assert out["in_flight_since"] == since


def test_streaming_in_progress_even_with_old_data_inside_the_active_send():
    # data waited 70 min, but it is being sent now (send started 2 min ago): the send's age decides.
    out = s(ckpt(9, in_flight=(10, -1), since=NOW - timedelta(minutes=2)), hist((9, 70, "WRITE")), run=RUNNING)
    assert out["status"] == IN_PROGRESS


def test_streaming_behind_when_active_send_past_threshold():
    out = s(ckpt(9, in_flight=(10, -1), since=NOW - timedelta(minutes=61)), hist((9, 62, "WRITE")), run=RUNNING)
    assert (out["status"], out["status_reason"]) == (BEHIND, "send_over_threshold")


def test_streaming_behind_when_data_beyond_active_send_is_old():
    # sending versions < 10; version 10 arrived 65 min ago and is not part of this send.
    out = s(ckpt(9, in_flight=(10, -1), since=NOW - timedelta(minutes=1)),
            hist((10, 65, "WRITE"), (9, 66, "WRITE")), run=RUNNING)
    assert (out["status"], out["status_reason"]) == (BEHIND, "unsent_over_threshold")


def test_streaming_leftover_in_flight_offsets_of_an_ended_run_are_not_a_send():
    # crash debris: offsets past the commit, but the latest run has a run_end. Not IN_PROGRESS.
    c = ckpt(9, in_flight=(10, -1), since=NOW - timedelta(minutes=5))
    out = s(c, hist((9, 10, "WRITE")), run=ENDED)
    assert out["status"] == PENDING and out["in_flight_since"] is None
    assert s(c, hist((9, 10, "WRITE")), run=None)["status"] == PENDING


def test_streaming_running_run_without_in_flight_offsets_is_not_a_send():
    out = s(ckpt(9), hist((9, 10, "WRITE")), run=RUNNING)
    assert out["status"] == PENDING


def test_streaming_needs_more_history_when_window_does_not_reach_sent_position():
    # sent below 5; window holds 10..12 only (limit 3), all young => cannot decide yet.
    assert s(ckpt(5), hist((12, 1, "WRITE"), (11, 2, "WRITE"), (10, 3, "WRITE")), limit=3) is None
    # all-ignorable window is not proof of caught up either
    assert s(ckpt(5), hist((12, 1, "OPTIMIZE"), (11, 2, "OPTIMIZE"), (10, 3, "OPTIMIZE")), limit=3) is None


def test_streaming_old_unsent_commit_in_a_partial_window_is_decisive():
    out = s(ckpt(5), hist((12, 1, "WRITE"), (11, 2, "WRITE"), (10, 80, "WRITE")), limit=3, run=ENDED)
    assert out["status"] == BEHIND


def test_streaming_late_commit_cannot_hide_the_oldest_unsent():
    # The race the coverage rule exists for: sent below 10, one unsent commit (10) plus a commit (11) that
    # landed after we would have computed LIMIT = newest - sent = 1. The LIMIT 1 read sees only 11 and must
    # NOT conclude anything about 10.
    assert s(ckpt(10), hist((11, 0, "WRITE")), limit=1) is None
    out = s(ckpt(10), hist((11, 0, "WRITE"), (10, 30, "WRITE")), limit=2, run=ENDED)
    assert out["status"] == PENDING and out["oldest_unsent_ts"] == NOW - timedelta(minutes=30)


@pytest.mark.parametrize("state,reason", [("no_checkpoint", "no_checkpoint"),
                                          ("no_committed_batch", "no_committed_batch"),
                                          ("weird", "bad_checkpoint")])
def test_streaming_checkpoint_states_are_unknown(state, reason):
    out = s({"state": state}, hist((1, 1, "WRITE")))
    assert (out["status"], out["status_reason"]) == (UNKNOWN, reason)


def test_streaming_empty_history_is_unknown():
    assert s(ckpt(1), [])["status_reason"] == "source_unreadable"


def test_streaming_offset_ahead_of_table_is_unknown():
    # checkpoint says versions < 50 are sent, but the table's newest is 3 (recreated table).
    out = s(ckpt(50), hist((3, 1, "WRITE")))
    assert (out["status"], out["status_reason"]) == (UNKNOWN, "offset_ahead_of_table")


# ---------------------------------------------------------------------------------------------------
# classify_batch
# ---------------------------------------------------------------------------------------------------

SCHED = {"kind": "schedule", "cron": "0 */10 * * * ?", "paused": False}


def run_(started_min_ago, ended_min_ago=None, status=None):
    return {"job_run_id": "j1", "started_at": NOW - timedelta(minutes=started_min_ago),
            "run_ended_at": None if ended_min_ago is None else NOW - timedelta(minutes=ended_min_ago),
            "run_end_status": status}


def b(run, trigger=SCHED):
    return classify_batch(run, trigger, NOW)


def test_batch_no_runs_is_unknown():
    assert b(None)["status_reason"] == "no_runs_logged"


def test_batch_running_under_and_over_threshold():
    assert (b(run_(5))["status"], b(run_(5))["status_reason"]) == (IN_PROGRESS, "run_in_progress")
    assert (b(run_(60))["status"], b(run_(60))["status_reason"]) == (BEHIND, "run_over_threshold")


def test_batch_last_run_failed_is_behind():
    out = b(run_(5, 1, "error"))
    assert (out["status"], out["status_reason"]) == (BEHIND, "last_run_failed")
    assert out["last_run_status"] == "error" and out["last_run_id"] == "j1"


def test_batch_success_covering_expected_fire_is_caught_up():
    # NOW 12:00, grace 10 => expected fire is 11:50; a run that started 11:50:30 covers it.
    r = {"job_run_id": "j1", "started_at": utc(2026, 10, 5, 11, 50, 30), "run_ended_at": utc(2026, 10, 5, 11, 52),
         "run_end_status": "success"}
    out = b(r)
    assert (out["status"], out["status_reason"]) == (CAUGHT_UP, "last_run_succeeded")
    assert out["expected_run_ts"] == utc(2026, 10, 5, 11, 50)


def test_batch_success_before_expected_fire_is_missed_schedule():
    r = {"job_run_id": "j1", "started_at": utc(2026, 10, 5, 11, 40, 30), "run_ended_at": utc(2026, 10, 5, 11, 42),
         "run_end_status": "success"}
    out = b(r)
    assert (out["status"], out["status_reason"]) == (BEHIND, "missed_schedule")


def test_batch_grace_lets_a_starting_run_not_count_as_missed():
    # NOW 12:05: the 12:00 fire is inside the 10 min grace, so the 11:50 run still covers expectations.
    r = {"job_run_id": "j1", "started_at": utc(2026, 10, 5, 11, 50, 30), "run_ended_at": utc(2026, 10, 5, 11, 52),
         "run_end_status": "success"}
    assert classify_batch(r, SCHED, utc(2026, 10, 5, 12, 5))["status"] == CAUGHT_UP


def test_batch_paused_or_on_demand_ignores_schedule():
    old = run_(600, 590, "success")
    assert b(old, {"kind": "schedule", "cron": "0 */10 * * * ?", "paused": True})["status"] == CAUGHT_UP
    assert b(old, {"kind": "on_demand", "cron": None, "paused": None})["status"] == CAUGHT_UP
    assert b(old)["status"] == BEHIND


def test_batch_unknown_run_end_status_is_unknown():
    assert b(run_(5, 1, "stopped"))["status_reason"] == "unknown_run_status"
    assert b(run_(5, 1, "weird"))["status_reason"] == "unknown_run_status"


def test_batch_unsupported_cron_is_unknown():
    out = b(run_(5, 1, "success"), {"kind": "schedule", "cron": "0 0 12 L * ?", "paused": False})
    assert out["status_reason"] == "unsupported_cron"


def test_latest_run_normalizes_and_falls_back_to_batch_start():
    t = NOW - timedelta(minutes=3)
    assert latest_run(None) is None
    r = latest_run({"job_run_id": None, "run_started_at": None, "last_batch_started_at": t,
                    "run_ended_at": None, "run_end_status": None})
    assert r["started_at"] == t and r["run_ended_at"] is None


def test_latest_run_newer_batch_than_the_ended_attempt_is_an_open_run():
    # The visible attempt ended at -30; a batch started at -2 belongs to a newer attempt whose run_start is
    # outside the window: report that one, open.
    r = latest_run({"job_run_id": "j", "run_started_at": NOW - timedelta(minutes=40),
                    "run_ended_at": NOW - timedelta(minutes=30), "run_end_status": "error",
                    "last_batch_started_at": NOW - timedelta(minutes=2)})
    assert r["run_ended_at"] is None and r["started_at"] == NOW - timedelta(minutes=2)
    # A batch from INSIDE the ended attempt changes nothing.
    r = latest_run({"job_run_id": "j", "run_started_at": NOW - timedelta(minutes=40),
                    "run_ended_at": NOW - timedelta(minutes=30), "run_end_status": "error",
                    "last_batch_started_at": NOW - timedelta(minutes=35)})
    assert r["run_end_status"] == "error" and r["started_at"] == NOW - timedelta(minutes=40)


# ---------------------------------------------------------------------------------------------------
# feed_triggers
# ---------------------------------------------------------------------------------------------------

def _job(trigger=None, configs=("c1",), notebook="../notebooks/run_index_pipeline.py"):
    job = {"tasks": [{"task_key": f"t_{c}", "notebook_task": {"notebook_path": notebook,
                                                             "base_parameters": {"config_name": c}}}
                     for c in configs]}
    if trigger:
        job.update(trigger)
    return {"resources": {"jobs": {"k": job}}}


def test_feed_triggers_kinds_and_pause_resolution():
    docs = [
        _job({"schedule": {"quartz_cron_expression": "0 */10 * * * ?", "pause_status": "${var.schedule_pause_status}"}}, ("a",)),
        _job({"schedule": {"quartz_cron_expression": "0 0 3 * * ?", "pause_status": "UNPAUSED"}}, ("b",)),
        _job({"continuous": {"pause_status": "${var.schedule_pause_status}"}}, ("c",)),
        _job(None, ("g1", "g2")),
        _job({"schedule": {"quartz_cron_expression": "0 0 3 * * ?"}}, ("x",), notebook="../notebooks/_other.py"),
        _job({"schedule": {"quartz_cron_expression": "0 0 3 * * ?", "pause_status": "${var.other}"}}, ("u",)),
    ]
    t = feed_triggers(docs, "PAUSED")
    assert t["a"] == {"kind": "schedule", "cron": "0 */10 * * * ?", "paused": True}
    assert t["b"]["paused"] is False
    assert t["c"] == {"kind": "continuous", "cron": None, "paused": True}
    assert t["g1"] == t["g2"] == {"kind": "on_demand", "cron": None, "paused": None}
    assert "x" not in t
    assert t["u"]["kind"] == "unsupported"
    assert feed_triggers(docs, "UNPAUSED")["a"]["paused"] is False


def test_feed_triggers_against_the_committed_generated_jobs():
    import glob, os, yaml
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    docs = [yaml.safe_load(open(p)) for p in glob.glob(os.path.join(root, "resources", "*.yml"))]
    t = feed_triggers(docs, "UNPAUSED")
    configs = {os.path.splitext(os.path.basename(p))[0]
               for p in glob.glob(os.path.join(root, "_pipelines", "pipeline_configs", "*.yml"))}
    assert set(t) == configs  # every config maps to exactly one deployed trigger
    assert t["ecs_dns_activity"] == {"kind": "schedule", "cron": "0 */10 * * * ?", "paused": False}
    assert t["ecs_dns_activity_continuous"]["kind"] == "continuous"


# ---------------------------------------------------------------------------------------------------
# results, rows and SQL
# ---------------------------------------------------------------------------------------------------

def test_every_status_has_reasons_and_result_rejects_mismatches():
    assert set(REASONS) == set(STATUSES)
    with pytest.raises(ValueError):
        _result(CAUGHT_UP, "last_run_failed")
    with pytest.raises(ValueError):
        _result("GREEN", "no_unsent_data")
    with pytest.raises(ValueError):
        _result(CAUGHT_UP, "no_unsent_data", bogus=1)


def test_to_row_shapes_and_formats_timestamps():
    res = s(ckpt(9), hist((9, 10, "WRITE")), run=ENDED)
    row = to_row("cfg", "streaming", {"kind": "schedule", "paused": False}, res, NOW, source_table="c.s.t")
    assert tuple(row) == RESULT_FIELDS
    assert row["config_name"] == "cfg" and row["trigger"] == "schedule" and row["trigger_paused"] is False
    # timestamps are epoch microseconds (2026-10-05 11:50:00 UTC / 12:00:00 UTC)
    assert row["oldest_unsent_ts"] == 1791201000000000
    assert row["source_table"] == "c.s.t"
    assert row["evaluated_at"] == 1791201600000000
    assert row["last_run_end_ts"] == 1791200400000000  # ENDED ended 11:40 UTC
    assert to_row("b", "batch", None, b(None), NOW, source_table="c.s.t")["source_table"] is None


def test_status_table_sql():
    ddl = create_status_table_sql("c.s.feed_status")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS c.s.feed_status (")
    for name, sql_type in STATUS_TABLE_COLUMNS:
        assert f"{name} {sql_type}" in ddl
    m = merge_status_sql("c.s.feed_status", "_feed_status_rows")
    assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in m
    assert "t.config_name = s.config_name" in m and "t.config_name = s.config_name," not in m
    for bad in ("c.s", "c.s.t; DROP", ""):
        with pytest.raises(ValueError):
            create_status_table_sql(bad)
    with pytest.raises(ValueError):
        merge_status_sql("c.s.t", "v; DROP")


def test_run_state_sql():
    q = run_state_sql("c.s.log", 7)
    assert "FROM c.s.log" in q and "INTERVAL 7 DAYS" in q
    assert "'run_start'" in q and "'run_end'" in q and "'batch_start'" in q
    with pytest.raises(ValueError):
        run_state_sql("c.s.log", 0)
    with pytest.raises(ValueError):
        run_state_sql("bad", 7)


def test_streaming_offset_ahead_boundary():
    # newest version 3: sent-below 4 means fully caught up; sent-below 5 points past the table.
    assert s(ckpt(4), hist((3, 1, "WRITE")))["status"] == CAUGHT_UP
    assert s(ckpt(5), hist((3, 1, "WRITE")))["status_reason"] == "offset_ahead_of_table"


def test_streaming_rows_carry_the_latest_run():
    out = s(ckpt(9, in_flight=(10, -1), since=NOW - timedelta(minutes=5)), hist((9, 10, "WRITE")),
            run={**ENDED, "run_end_status": "error"})
    assert out["last_run_id"] == "r8" and out["last_run_status"] == "error"
    assert out["last_run_start_ts"] == ENDED["started_at"] and out["last_run_end_ts"] == ENDED["run_ended_at"]
    assert s(ckpt(9), hist((8, 10, "WRITE")))["last_run_id"] is None


def test_streaming_sent_position_before_retained_history_is_unknown_not_caught_up():
    # Delta log cleanup kept only versions 40-41 (2 rows < limit 20); the stream is at 10. Versions 10-39
    # are invisible and may hold unsent rows, so this must not read as CAUGHT_UP (and widening cannot help).
    out = s(ckpt(10), hist((41, 1, "OPTIMIZE"), (40, 2, "OPTIMIZE")), run=ENDED)
    assert (out["status"], out["status_reason"]) == (UNKNOWN, "history_retention_exceeded")
    # ...but an old unsent commit still visible is decisive.
    assert s(ckpt(10), hist((41, 90, "WRITE")), run=ENDED)["status"] == BEHIND


def test_new_table_whole_history_still_covers():
    # A young table: history back to version 0 is whole and reaches any sent position.
    assert s(ckpt(0), hist((1, 2, "WRITE"), (0, 3, "CREATE TABLE")), run=ENDED)["status"] == PENDING


def test_batch_run_longer_than_interval_is_not_missed_schedule():
    # 10-min cron, run 11:30:10-11:47 (17 min, so the 11:40 fire was skipped for overlap). At 11:58 the
    # latest fire >= grace ago is 11:40, which landed while the run was going: caught up, not missed.
    r = {"job_run_id": "j1", "started_at": utc(2026, 10, 5, 11, 30, 10), "run_ended_at": utc(2026, 10, 5, 11, 47),
         "run_end_status": "success"}
    assert classify_batch(r, SCHED, utc(2026, 10, 5, 11, 58))["status"] == CAUGHT_UP
    # At 12:01 the latest fire >= grace ago is 11:50, after the run ended, and no run is logged for it.
    out = classify_batch(r, SCHED, utc(2026, 10, 5, 12, 1))
    assert (out["status"], out["status_reason"]) == (BEHIND, "missed_schedule")


def test_batch_run_without_any_start_time_is_no_runs_logged():
    r = {"job_run_id": "j", "started_at": None, "run_ended_at": None, "run_end_status": None}
    assert classify_batch(r, SCHED, NOW)["status_reason"] == "no_runs_logged"


def test_to_row_treats_naive_timestamps_as_utc_and_offsets_correctly():
    from datetime import timedelta as _td
    res = _result(PENDING, "unsent_within_threshold", oldest_unsent_ts=datetime(2026, 10, 5, 11, 50, 7),
                  in_flight_since=datetime(2026, 10, 5, 13, 50, 7, tzinfo=timezone(_td(hours=2))))
    row = to_row("cfg", "streaming", None, res, NOW)
    assert row["oldest_unsent_ts"] == 1791201007000000
    assert row["in_flight_since"] == 1791201007000000  # 13:50:07+02:00 is the same instant


def test_zero_step_cron_is_unsupported():
    for expr in ("0 */0 * * * ?", "0 5/0 * * * ?"):
        with pytest.raises(UnsupportedCron):
            previous_fire(expr, NOW)


def test_lag_minutes_never_negative_for_a_commit_newer_than_now():
    out = s(ckpt(9), hist((9, -2, "WRITE")), run=ENDED)  # committed 2 min "after" now
    assert out["status"] == PENDING and out["lag_minutes"] == 0.0


def test_run_state_sql_pairs_by_attempt_not_job_run():
    q = run_state_sql("c.s.log", 7)
    assert "GROUP BY config_name, job_run_id, batch_start_ts" in q
    assert "FULL OUTER JOIN batches" in q
    # the LATEST attempt per config, ordered by the attempt's own start
    assert "max_by(named_struct(" in q and "run_started_at) AS a" in q


def test_feed_triggers_config_in_two_jobs_is_unsupported():
    docs = [_job({"schedule": {"quartz_cron_expression": "0 */10 * * * ?", "pause_status": "UNPAUSED"}}, ("dup",)),
            _job(None, ("dup",))]
    assert feed_triggers(docs, "PAUSED")["dup"]["kind"] == "unsupported"


def test_log_vocabulary_comes_from_monitoring_sink():
    from pipeline_lib import feed_status as fs
    from pipeline_lib.monitoring_sink import RECORD_TYPES as RT, STATUSES as ST
    assert {fs.LOG_RUN_START, fs.LOG_RUN_END, fs.LOG_BATCH_START} <= set(RT)
    assert {fs.RUN_SUCCESS, fs.RUN_ERROR, fs.RUN_STOPPED} <= set(ST)


def test_missing_log_vocabulary_names_what_was_removed():
    from pipeline_lib.feed_status import missing_log_vocabulary
    from pipeline_lib.monitoring_sink import RECORD_TYPES as RT, STATUSES as ST
    assert missing_log_vocabulary(RT, ST) == []
    assert missing_log_vocabulary([t for t in RT if t != "run_end"], ST) == ["run_end"]
    assert missing_log_vocabulary(RT, [x for x in ST if x != "stopped"]) == ["stopped"]


def test_effective_pipeline_mode_inherits_the_global_and_fails_closed():
    from pipeline_lib.feed_status import effective_pipeline_mode
    assert effective_pipeline_mode("streaming", "batch") == "streaming"
    assert effective_pipeline_mode("", "batch") == "batch"          # omitted => inherits the global
    assert effective_pipeline_mode("", "streaming") == "streaming"
    assert effective_pipeline_mode("", "") is None
    assert effective_pipeline_mode("weird", "batch") is None
    assert "unsupported_pipeline_mode" in REASONS[UNKNOWN]


def test_effective_mode_matches_the_generated_jobs_default():
    # The generated job's pipeline_mode parameter default is the config's own value or ${var.pipeline_mode};
    # effective_pipeline_mode must agree for every committed config.
    import glob, os, yaml
    from pipeline_lib.config import load_config
    from pipeline_lib.feed_status import effective_pipeline_mode
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    defaults = {}
    for p in glob.glob(os.path.join(root, "resources", "*.yml")):
        for job in ((yaml.safe_load(open(p)) or {}).get("resources") or {}).get("jobs", {}).values():
            params = {x["name"]: x.get("default") for x in job.get("parameters") or []}
            for t in job.get("tasks") or []:
                c = ((t.get("notebook_task") or {}).get("base_parameters") or {}).get("config_name")
                if c and "pipeline_mode" in params:
                    defaults[c] = params["pipeline_mode"]
    assert defaults
    for c, default in defaults.items():
        cfg = load_config(os.path.join(root, "_pipelines", "pipeline_configs", f"{c}.yml"))
        expect = default if default != "${var.pipeline_mode}" else "batch"
        assert effective_pipeline_mode(cfg["pipeline_mode"], "batch") == expect, c


def test_import_fails_when_monitoring_sink_drops_a_name_read_here():
    # In a fresh interpreter (so no other test sees a reloaded module): drop one record type from
    # monitoring_sink, then importing feed_status must raise ImportError naming it.
    import os, subprocess, sys
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    code = ("import pipeline_lib.monitoring_sink as ms\n"
            "ms.RECORD_TYPES = tuple(t for t in ms.RECORD_TYPES if t != 'batch_start')\n"
            "try:\n    import pipeline_lib.feed_status\nexcept ImportError as e:\n    print('RAISED', e)\n")
    out = subprocess.run([sys.executable, "-c", code], cwd=root, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "RAISED" in out.stdout and "batch_start" in out.stdout


def test_feed_triggers_skips_a_runner_task_without_config_name():
    doc = {"resources": {"jobs": {"k": {"tasks": [
        {"task_key": "t", "notebook_task": {"notebook_path": "../notebooks/run_index_pipeline.py",
                                            "base_parameters": {"environment": "x"}}}]}}}}
    assert feed_triggers([doc], "PAUSED") == {}

"""Offline unit tests for the monitoring log's write policy and the run / batch / stream steps that use it:
pipeline_lib.monitoring_writer, run_record, batch_export, stream_batch, stream_progress. No Spark: the Spark
write, the connector, and the query are fakes.

The load-bearing contracts:
- Fail-closed when the log is on: a misconfigured table fails before anything runs; a failed append raises
  MonitoringLogError; rows recording a failure never mask that failure. Off => nothing is written.
- ORDER: batch_start is written before any data is sent; batch_end (with ES diagnostics) after; a failure from
  the write onward records batch_end status error and re-raises.
- Streaming batch_summary rows come from Spark's progress reports, appended fail-closed.
"""
from datetime import datetime, timedelta, timezone
import json

import pytest

from pipeline_lib.batch_export import run_batch_export
from pipeline_lib.monitoring_writer import MonitoringLog, MonitoringLogError, resolve_log_table
from pipeline_lib.run_record import RunRecorder
from pipeline_lib.stream_batch import make_foreach_batch
from pipeline_lib.stream_progress import ProgressRecorder

T0 = datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc)
RESULT = {"written": 3, "deleted": 0, "errors": 0, "ignored": 0, "total_input": 3, "collect_ms": 10.0,
          "merge_ms": 0.1}


class Clock:
    def __init__(self):
        self.t = T0

    def __call__(self):
        self.t += timedelta(seconds=1)
        return self.t


class Events:
    """One shared, ordered record of what happened (log writes and data sends), to assert ORDER."""

    def __init__(self):
        self.items = []

    def types(self):
        return [e[0] if e[0] != "log" else e[1] for e in self.items]


def make_log(events, fail_on=(), table="cat.sch.mon"):
    """A real MonitoringLog over a fake writer that records rows into `events` and fails for record types in
    `fail_on`."""
    def write_rows(tbl, rows, session):
        for r in rows:
            if r["record_type"] in fail_on:
                raise OSError(f"table down ({r['record_type']})")
        for r in rows:
            events.items.append(("log", r["record_type"], r))
    return MonitoringLog(table, write_rows)


def rows_of(events, record_type):
    return [e[2] for e in events.items if e[0] == "log" and e[1] == record_type]


class WriteConfig:
    index = "idx"


# --- monitoring_writer -------------------------------------------------------------------------

@pytest.mark.parametrize("flag", ["", "false"])
def test_resolve_log_table_off(flag):
    assert resolve_log_table(flag, "cat.sch.tbl") == ""


def test_resolve_log_table_on_requires_a_table():
    with pytest.raises(ValueError, match="monitoring_log_table is unset"):
        resolve_log_table("true", "")


def test_resolve_log_table_on_validates_the_name():
    with pytest.raises(ValueError):
        resolve_log_table("true", "not-a-three-part")
    assert resolve_log_table("true", " cat.sch.tbl ") == "cat.sch.tbl"


def test_log_off_writes_nothing():
    calls = []
    log = MonitoringLog("", lambda *a: calls.append(a))
    log.append([{"record_type": "run_start"}], session=None)
    assert not log.active and calls == []


def test_log_append_failure_raises_monitoring_log_error_chained():
    def boom(table, rows, session):
        raise OSError("disk")
    log = MonitoringLog("c.s.t", boom)
    with pytest.raises(MonitoringLogError, match="not persisted") as info:
        log.append([{"record_type": "batch_start"}], session=None)
    assert isinstance(info.value.__cause__, OSError)


def test_log_empty_rows_is_a_noop():
    calls = []
    MonitoringLog("c.s.t", lambda *a: calls.append(a)).append([], session=None)
    assert calls == []


def test_append_failure_never_raises_and_notes_the_original():
    def boom(table, rows, session):
        raise OSError("disk")
    original = RuntimeError("the real error")
    printed = []
    MonitoringLog("c.s.t", boom).append_failure([{"record_type": "run_end"}], original, None, log=printed.append)
    assert any("could not be written" in n for n in getattr(original, "__notes__", []))
    assert printed


# --- run_record --------------------------------------------------------------------------------

def test_run_start_and_end_rows():
    ev = Events()
    rec = RunRecorder(make_log(ev), "cfg", "run1", {"mode": "batch", "es_index": "idx"}, clock=Clock())
    rec.start({"view": "v"}, None)
    rec.end("success", {"batches": 1}, None)
    start, = rows_of(ev, "run_start")
    end, = rows_of(ev, "run_end")
    assert json.loads(start["payload"]) == {"mode": "batch", "es_index": "idx", "view": "v"}
    assert end["status"] == "success" and json.loads(end["payload"])["batches"] == 1
    assert end["start_ts"] == start["start_ts"]


def test_guard_records_run_end_error_once_and_reraises():
    ev = Events()
    rec = RunRecorder(make_log(ev), "cfg", "run1", {"mode": "batch"}, clock=Clock())
    rec.start({}, None)
    with pytest.raises(ValueError, match="boom"):
        with rec.guard(None):
            raise ValueError("boom")
    with pytest.raises(KeyError):
        with rec.guard(None):
            raise KeyError("second")
    ends = rows_of(ev, "run_end")
    assert len(ends) == 1 and ends[0]["status"] == "error"
    assert json.loads(ends[0]["payload"])["exception_type"] == "ValueError"


def test_guard_after_a_clean_end_writes_nothing():
    ev = Events()
    rec = RunRecorder(make_log(ev), "cfg", "run1", {}, clock=Clock())
    rec.start({}, None)
    rec.end("success", {}, None)
    with pytest.raises(ValueError):
        with rec.guard(None):
            raise ValueError("late")
    assert len(rows_of(ev, "run_end")) == 1


def test_guard_keeps_the_original_error_when_the_log_is_down():
    ev = Events()
    rec = RunRecorder(make_log(ev, fail_on=("run_end",)), "cfg", "run1", {}, clock=Clock())
    rec.start({}, None)
    with pytest.raises(ValueError, match="real") as info:
        with rec.guard(None):
            raise ValueError("real")
    assert getattr(info.value, "__notes__", [])


def test_run_start_failure_raises():
    rec = RunRecorder(make_log(Events(), fail_on=("run_start",)), "cfg", "run1", {}, clock=Clock())
    with pytest.raises(MonitoringLogError):
        rec.start({}, None)


# --- batch_export ------------------------------------------------------------------------------

def _export(ev, bulk_write=None, reconcile=None, fail_on=()):
    def default_write(df, cfg):
        ev.items.append(("send", df))
        return dict(RESULT)
    return run_batch_export("DF", WriteConfig(), bulk_write=bulk_write or default_write,
                            reconcile=reconcile or (lambda result, index: None), log=make_log(ev, fail_on),
                            config_name="cfg", job_run_id="run1", session=None, printer=lambda *_: None,
                            clock=Clock())


def test_batch_export_order_and_success_rows():
    ev = Events()
    result = _export(ev)
    assert ev.types() == ["batch_start", "send", "batch_end"]
    end, = rows_of(ev, "batch_end")
    payload = json.loads(end["payload"])
    assert end["status"] == "success" and end["batch_id"] == 0
    assert payload["written"] == 3 and payload["es"]["collect_ms"] == 10.0
    assert result["written"] == 3
    assert rows_of(ev, "batch_summary") == []  # batch mode has no Spark progress


def test_batch_export_start_failure_sends_nothing():
    ev = Events()
    with pytest.raises(MonitoringLogError):
        _export(ev, fail_on=("batch_start",))
    assert "send" not in ev.types()


def test_batch_export_write_failure_records_batch_end_error_and_reraises():
    ev = Events()

    def failing_write(df, cfg):
        raise TimeoutError("ES timed out")
    with pytest.raises(TimeoutError):
        _export(ev, bulk_write=failing_write)
    end, = rows_of(ev, "batch_end")
    payload = json.loads(end["payload"])
    assert end["status"] == "error" and payload["exception_type"] == "TimeoutError"
    assert "written" not in payload


def test_batch_export_reconcile_failure_includes_counts_and_diagnostics():
    ev = Events()

    def bad_reconcile(result, index):
        raise ValueError("2 docs rejected")
    with pytest.raises(ValueError):
        _export(ev, reconcile=bad_reconcile)
    end, = rows_of(ev, "batch_end")
    payload = json.loads(end["payload"])
    assert end["status"] == "error" and payload["written"] == 3 and "es" in payload


def test_batch_export_end_append_failure_raises_after_the_send():
    ev = Events()
    with pytest.raises(MonitoringLogError):
        _export(ev, fail_on=("batch_end",))
    assert ev.types() == ["batch_start", "send"]


# --- stream_batch ------------------------------------------------------------------------------

class FakeBatchDF:
    sparkSession = "SESSION"


def _fb(ev, bulk_write=None, write_metrics=None, relay=True, fail_on=(), result=None):
    res = dict(result or RESULT)

    def default_write(df, cfg, raise_on_error):
        assert raise_on_error is True
        ev.items.append(("send", df))
        return res

    def default_metrics(session, batch_id, written):
        assert session == "SESSION"
        ev.items.append(("metrics", batch_id, written))

    def relay_fn(session, batch_id, text):
        ev.items.append(("relay", batch_id, text))

    return make_foreach_batch(
        transform=lambda batch_df, session: ("T", session), bulk_write=bulk_write or default_write,
        write_config=WriteConfig(), log=make_log(ev, fail_on), config_name="cfg", job_run_id="run1",
        write_metrics=write_metrics or default_metrics, write_print_relay=relay_fn if relay else None,
        printer=lambda *_: None, clock=Clock())


def test_foreach_batch_order_and_success_rows():
    ev = Events()
    _fb(ev)(FakeBatchDF(), 4)
    assert ev.types() == ["batch_start", "send", "metrics", "batch_end"]
    assert ev.items[1][1] == ("T", "SESSION")  # transform ran on the batch's own session
    end, = rows_of(ev, "batch_end")
    assert end["batch_id"] == 4 and end["status"] == "success"
    assert json.loads(end["payload"])["es"]["merge_ms"] == 0.1


def test_foreach_batch_relays_the_bulk_stats_line_last_when_present():
    ev = Events()
    part = {"n_sends": 1, "docs_sent": 3, "partition_wall_ms": 5.0}
    _fb(ev, result=dict(RESULT, bulk_stats=[part]))(FakeBatchDF(), 2)
    assert ev.types() == ["batch_start", "send", "metrics", "batch_end", "relay"]
    assert "BULK_STATS" in ev.items[-1][2]


def test_foreach_batch_no_relay_without_bulk_stats_or_when_off():
    ev = Events()
    _fb(ev)(FakeBatchDF(), 1)  # no bulk_stats in the result
    _fb(ev, relay=False, result=dict(RESULT, bulk_stats=[{}]))(FakeBatchDF(), 2)
    assert "relay" not in ev.types()


def test_foreach_batch_relay_failure_is_swallowed():
    ev = Events()

    def bad_relay(session, batch_id, text):
        raise OSError("volume blip")
    fb = make_foreach_batch(
        transform=lambda b, s: "T", bulk_write=lambda df, cfg, raise_on_error: dict(RESULT, bulk_stats=[{}]),
        write_config=WriteConfig(), log=make_log(ev), config_name="cfg", job_run_id="run1",
        write_metrics=lambda s, b, w: None, write_print_relay=bad_relay, printer=lambda *_: None, clock=Clock())
    fb(FakeBatchDF(), 3)  # does not raise
    assert rows_of(ev, "batch_end")[0]["status"] == "success"


def test_foreach_batch_start_failure_sends_nothing():
    ev = Events()
    with pytest.raises(MonitoringLogError):
        _fb(ev, fail_on=("batch_start",))(FakeBatchDF(), 1)
    assert "send" not in ev.types()


def test_foreach_batch_write_failure_records_batch_end_error_and_reraises():
    ev = Events()

    def failing_write(df, cfg, raise_on_error):
        raise ConnectionError("ES down")
    with pytest.raises(ConnectionError):
        _fb(ev, bulk_write=failing_write)(FakeBatchDF(), 5)
    end, = rows_of(ev, "batch_end")
    assert end["status"] == "error" and json.loads(end["payload"])["exception_type"] == "ConnectionError"
    assert "metrics" not in ev.types()


def test_foreach_batch_metrics_failure_records_batch_end_error():
    ev = Events()

    def bad_metrics(session, batch_id, written):
        raise OSError("checkpoint volume")
    with pytest.raises(OSError):
        _fb(ev, write_metrics=bad_metrics)(FakeBatchDF(), 6)
    assert [r["status"] for r in rows_of(ev, "batch_end")] == ["error"]


def test_foreach_batch_end_append_failure_raises():
    ev = Events()
    with pytest.raises(MonitoringLogError):
        _fb(ev, fail_on=("batch_end",))(FakeBatchDF(), 7)
    assert ev.types() == ["batch_start", "send", "metrics"]


def test_foreach_batch_with_log_off_still_writes_and_records_metrics():
    ev = Events()
    fb = make_foreach_batch(
        transform=lambda b, s: "T", bulk_write=lambda df, cfg, raise_on_error: dict(RESULT),
        write_config=WriteConfig(), log=MonitoringLog("", lambda *a: None), config_name="cfg", job_run_id="",
        write_metrics=lambda s, b, w: ev.items.append(("metrics", b, w)), printer=lambda *_: None, clock=Clock())
    fb(FakeBatchDF(), 8)
    assert ev.types() == ["metrics"]


# --- stream_progress ---------------------------------------------------------------------------

class P:
    def __init__(self, d):
        self.json = json.dumps(d)


def report(bid, executed=True):
    d = {"batchId": bid, "timestamp": "2026-10-06T12:00:00.000Z", "batchDuration": 1000,
         "durationMs": {"triggerExecution": 1000}, "name": "q", "sources": []}
    if executed:
        d["durationMs"]["addBatch"] = 900
    return d


class FakeQuery:
    def __init__(self, polls):
        self.polls = list(polls)
        self.calls = 0

    @property
    def recentProgress(self):
        item = self.polls[min(self.calls, len(self.polls) - 1)]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        return [P(d) for d in item]


def test_progress_recorder_appends_one_summary_per_new_executed_batch():
    ev = Events()
    printed = []
    rec = ProgressRecorder(make_log(ev), "cfg", "run1", session=None, printer=printed.append)
    q = FakeQuery([[report(0), report(1, executed=False)], [report(0), report(1), report(2)]])
    rec(q)
    rec(q)
    assert [r["batch_id"] for r in rows_of(ev, "batch_summary")] == [0, 1, 2]
    assert rec.last_batch_id == 2
    assert sum("STREAM_PROGRESS" in line for line in printed) == 3


def test_progress_recorder_prints_the_relayed_line():
    printed = []
    rec = ProgressRecorder(make_log(Events()), "cfg", "run1", session=None,
                           read_print_relay=lambda bid: f"BULK_STATS overall batch_id={bid}", printer=printed.append)
    rec(FakeQuery([[report(3)]]))
    assert "BULK_STATS overall batch_id=3" in printed


def test_progress_recorder_read_failure_only_warns():
    printed = []
    rec = ProgressRecorder(make_log(Events()), "cfg", "run1", session=None, printer=printed.append)
    rec(FakeQuery([RuntimeError("rpc")]))
    assert rec.last_batch_id is None and any("could not read query progress" in p for p in printed)


def test_progress_recorder_append_failure_raises_and_does_not_advance():
    rec = ProgressRecorder(make_log(Events(), fail_on=("batch_summary",)), "cfg", "run1", session=None,
                           printer=lambda *_: None)
    with pytest.raises(MonitoringLogError):
        rec(FakeQuery([[report(0)]]))
    assert rec.last_batch_id is None


def test_progress_recorder_log_off_records_nothing_but_still_prints():
    printed = []
    rec = ProgressRecorder(MonitoringLog("", lambda *a: None), "cfg", "", session=None, printer=printed.append)
    rec(FakeQuery([[report(0)]]))
    assert rec.last_batch_id == 0 and printed


def test_catch_up_polls_until_the_last_batch_is_recorded():
    ev = Events()
    rec = ProgressRecorder(make_log(ev), "cfg", "run1", session=None, printer=lambda *_: None)
    q = FakeQuery([[report(0)], [report(0)], [report(0), report(1)]])
    rec(q)
    slept = []
    rec.catch_up(q, through_batch_id=1, sleep=slept.append)
    assert rec.last_batch_id == 1 and len(slept) == 2


def test_catch_up_gives_up_after_its_attempts():
    rec = ProgressRecorder(make_log(Events()), "cfg", "run1", session=None, printer=lambda *_: None)
    q = FakeQuery([[]])
    slept = []
    rec.catch_up(q, through_batch_id=4, attempts=3, sleep=slept.append)
    assert rec.last_batch_id is None and len(slept) == 3


# --- task_run_id: every step stamps its attempt's id on every row it writes ----------------------

def test_run_recorder_stamps_task_run_id_on_start_end_and_guarded_error():
    ev = Events()
    rec = RunRecorder(make_log(ev), "cfg", "run1", {"mode": "batch"}, task_run_id="t9", clock=Clock())
    rec.start({}, None)
    with pytest.raises(ValueError):
        with rec.guard(None):
            raise ValueError("boom")
    start, = rows_of(ev, "run_start")
    end, = rows_of(ev, "run_end")
    assert (start["task_run_id"], end["task_run_id"]) == ("t9", "t9")
    assert (end["error_type"], end["error_message"]) == ("ValueError", "boom")
    assert "task_run_id" not in json.loads(start["payload"])  # a column now, not a payload field


def test_batch_export_stamps_task_run_id_on_success_and_error_rows():
    for fail in (False, True):
        ev = Events()

        def write(df, cfg):
            if fail:
                raise OSError("es down")
            return dict(RESULT)
        call = lambda: run_batch_export(  # noqa: E731
            "DF", WriteConfig(), bulk_write=write, reconcile=lambda result, index: None, log=make_log(ev),
            config_name="cfg", job_run_id="run1", task_run_id="t9", session=None, printer=lambda *_: None,
            clock=Clock())
        if fail:
            with pytest.raises(OSError):
                call()
        else:
            call()
        rows = rows_of(ev, "batch_start") + rows_of(ev, "batch_end")
        assert len(rows) == 2 and {r["task_run_id"] for r in rows} == {"t9"}
        assert rows[1]["docs_written"] == (None if fail else 3)


def test_foreach_batch_stamps_task_run_id_on_success_and_error_rows():
    for fail in (False, True):
        ev = Events()

        def write(df, cfg, raise_on_error):
            if fail:
                raise OSError("es down")
            return dict(RESULT)
        fb = make_foreach_batch(
            transform=lambda b, s: "T", bulk_write=write, write_config=WriteConfig(), log=make_log(ev),
            config_name="cfg", job_run_id="run1", task_run_id="t9", write_metrics=lambda s, b, w: None,
            printer=lambda *_: None, clock=Clock())
        if fail:
            with pytest.raises(OSError):
                fb(FakeBatchDF(), 5)
        else:
            fb(FakeBatchDF(), 5)
        rows = rows_of(ev, "batch_start") + rows_of(ev, "batch_end")
        assert len(rows) == 2 and {r["task_run_id"] for r in rows} == {"t9"}
        assert rows[1]["error_type"] == ("OSError" if fail else None)


def test_progress_recorder_stamps_task_run_id_and_backlog():
    ev = Events()
    rep = dict(report(0), sources=[{"metrics": {"numFilesOutstanding": "4", "numBytesOutstanding": "400"}}])
    rec = ProgressRecorder(make_log(ev), "cfg", "run1", session=None, printer=lambda *_: None, task_run_id="t9")
    rec(FakeQuery([[rep]]))
    row, = rows_of(ev, "batch_summary")
    assert (row["task_run_id"], row["files_outstanding"], row["bytes_outstanding"]) == ("t9", 4, 400)


# --- spark_row_schema: the writer's DataFrame schema comes from the table definition -------------

def test_spark_row_schema_matches_row_fields_and_types():
    from pipeline_lib.monitoring_sink import ROW_FIELDS
    from pipeline_lib.monitoring_writer import spark_row_schema
    fields = [f.split(" ") for f in spark_row_schema().split(", ")]
    assert [name for name, _t in fields] == list(ROW_FIELDS)
    types = dict(fields)
    assert types["batch_id"] == types["docs_written"] == types["files_outstanding"] == "bigint"
    assert types["start_ts"] == types["event_ts"] == types["payload"] == "string"  # cast by spark_append
    assert "logged_ts" not in types  # stamped by the writer


# --- source position: newest source version read at batch start, relayed to batch_summary ---------

LATEST = (9, datetime(2026, 10, 9, 18, 46, 31, tzinfo=timezone.utc))
LATEST_FACTS = {"version": 9, "timestamp": "2026-10-09T18:46:31.000000"}


def _fb_src(ev, read=None, relay=None, log=None, bulk_write=None, printed=None):
    def default_read(session):
        assert session == "SESSION"
        ev.items.append(("read_latest",))
        return LATEST

    def default_relay(session, batch_id, facts):
        ev.items.append(("source_relay", batch_id, facts))

    def default_write(df, cfg, raise_on_error):
        ev.items.append(("send", df))
        return dict(RESULT)

    return make_foreach_batch(
        transform=lambda batch_df, session: ev.items.append(("transform",)) or "T",
        bulk_write=bulk_write or default_write, write_config=WriteConfig(), log=log or make_log(ev),
        config_name="cfg", job_run_id="run1", write_metrics=lambda s, b, w: ev.items.append(("metrics",)),
        read_source_latest=default_read if read is None else read,
        write_source_relay=default_relay if relay is None else relay,
        printer=(printed.append if printed is not None else (lambda *_: None)), clock=Clock())


def test_foreach_batch_reads_the_newest_version_first_and_records_it_on_batch_start():
    ev = Events()
    _fb_src(ev)(FakeBatchDF(), 4)
    assert ev.types() == ["read_latest", "transform", "batch_start", "send", "metrics", "batch_end", "source_relay"]
    start, = rows_of(ev, "batch_start")
    assert json.loads(start["payload"]) == {"mode": "streaming", "source_latest": LATEST_FACTS}
    assert start["source_latest_version"] == 9
    assert ev.items[-1] == ("source_relay", 4, LATEST_FACTS)


def test_foreach_batch_read_failure_is_recorded_not_raised():
    ev = Events()
    printed = []

    def bad_read(session):
        raise OSError("history unavailable")
    _fb_src(ev, read=bad_read, printed=printed)(FakeBatchDF(), 2)
    start, = rows_of(ev, "batch_start")
    assert json.loads(start["payload"])["source_latest"] == {"error": "OSError: history unavailable"}
    assert start["source_latest_version"] is None
    assert rows_of(ev, "batch_end")[0]["status"] == "success"
    assert ev.items[-1] == ("source_relay", 2, {"error": "OSError: history unavailable"})
    assert any("could not read the newest source version" in p for p in printed)


def test_foreach_batch_unusable_version_is_recorded_as_an_error():
    ev = Events()
    _fb_src(ev, read=lambda session: (None, LATEST[1]))(FakeBatchDF(), 2)
    facts = json.loads(rows_of(ev, "batch_start")[0]["payload"])["source_latest"]
    assert set(facts) == {"error"} and facts["error"].startswith("ValueError: source version")


def test_foreach_batch_read_error_message_is_capped():
    ev = Events()

    def bad_read(session):
        raise RuntimeError("x" * 5000)
    _fb_src(ev, read=bad_read)(FakeBatchDF(), 2)
    facts = json.loads(rows_of(ev, "batch_start")[0]["payload"])["source_latest"]
    assert len(facts["error"]) <= 1000


def test_foreach_batch_source_relay_failure_is_swallowed():
    ev = Events()

    def bad_relay(session, batch_id, facts):
        raise OSError("volume blip")
    _fb_src(ev, relay=bad_relay)(FakeBatchDF(), 3)
    assert rows_of(ev, "batch_end")[0]["status"] == "success"


def test_foreach_batch_no_source_relay_when_the_batch_fails():
    ev = Events()

    def failing_write(df, cfg, raise_on_error):
        raise ConnectionError("ES down")
    with pytest.raises(ConnectionError):
        _fb_src(ev, bulk_write=failing_write)(FakeBatchDF(), 5)
    assert "source_relay" not in ev.types()


def test_foreach_batch_skips_the_read_and_relay_when_the_log_is_off():
    ev = Events()
    _fb_src(ev, log=MonitoringLog("", lambda *a: None))(FakeBatchDF(), 8)
    assert "read_latest" not in ev.types() and "source_relay" not in ev.types()


def test_foreach_batch_without_a_reader_keeps_the_old_batch_start_payload():
    ev = Events()
    _fb(ev)(FakeBatchDF(), 1)
    assert json.loads(rows_of(ev, "batch_start")[0]["payload"]) == {"mode": "streaming"}


def test_progress_recorder_adds_the_relayed_source_version_to_batch_summary():
    ev = Events()
    asked = []
    rep = dict(report(5), sources=[{"endOffset": json.dumps({"reservoirVersion": 10, "index": -1})}])
    rec = ProgressRecorder(make_log(ev), "cfg", "run1", session=None, printer=lambda *_: None,
                           read_source_relay=lambda bid: asked.append(bid) or LATEST_FACTS)
    rec(FakeQuery([[rep]]))
    row, = rows_of(ev, "batch_summary")
    assert asked == [5]
    assert (row["source_latest_version"], row["source_end_version"], row["source_end_index"]) == (9, 10, -1)


def test_progress_recorder_without_a_relayed_version_still_records_the_summary():
    ev = Events()
    rec = ProgressRecorder(make_log(ev), "cfg", "run1", session=None, printer=lambda *_: None,
                           read_source_relay=lambda bid: None)
    rec(FakeQuery([[report(0)]]))
    row, = rows_of(ev, "batch_summary")
    assert row["source_latest_version"] is None and "source_latest" not in json.loads(row["payload"])


def test_progress_recorder_does_not_read_the_source_relay_when_the_log_is_off():
    asked = []
    rec = ProgressRecorder(MonitoringLog("", lambda *a: None), "cfg", "", session=None, printer=lambda *_: None,
                           read_source_relay=lambda bid: asked.append(bid))
    rec(FakeQuery([[report(0)]]))
    assert asked == [] and rec.last_batch_id == 0


def test_progress_recorder_drains_the_relays_of_batches_whose_report_was_lost():
    ev = Events()
    asked_src, asked_print, printed = [], [], []
    rec = ProgressRecorder(make_log(ev), "cfg", "run1", session=None, printer=printed.append,
                           read_print_relay=lambda bid: asked_print.append(bid) or f"BULK_STATS batch_id={bid}",
                           read_source_relay=lambda bid: asked_src.append(bid) or LATEST_FACTS)
    q = FakeQuery([[report(0)], [report(3)]])  # the reports of batches 1 and 2 were lost
    rec(q)
    rec(q)
    assert [r["batch_id"] for r in rows_of(ev, "batch_summary")] == [0, 3]
    assert asked_src == [0, 1, 2, 3] and asked_print == [0, 1, 2, 3]
    assert not any("batch_id=1" in p or "batch_id=2" in p for p in printed)  # drained, not printed


def test_progress_recorder_gap_drain_is_bounded():
    asked = []
    rec = ProgressRecorder(make_log(Events()), "cfg", "run1", session=None, printer=lambda *_: None,
                           read_source_relay=lambda bid: asked.append(bid))
    rec(FakeQuery([[report(0)]]))
    rec(FakeQuery([[report(10_000)]]))
    from pipeline_lib.stream_progress import MAX_RELAY_GAP_DRAIN
    assert len(asked) == 1 + MAX_RELAY_GAP_DRAIN + 1 and asked[-1] == 10_000
    assert asked[1] == 10_000 - MAX_RELAY_GAP_DRAIN


def test_progress_recorder_first_report_drains_no_gap():
    asked = []
    rec = ProgressRecorder(make_log(Events()), "cfg", "run1", session=None, printer=lambda *_: None,
                           read_source_relay=lambda bid: asked.append(bid))
    rec(FakeQuery([[report(500)]]))  # a resumed stream: nothing below 500 belongs to this run
    assert asked == [500]


def test_progress_recorder_gap_drain_respects_log_off_and_absent_print_relay():
    asked_src, asked_print = [], []
    rec = ProgressRecorder(MonitoringLog("", lambda *a: None), "cfg", "", session=None, printer=lambda *_: None,
                           read_print_relay=lambda bid: asked_print.append(bid),
                           read_source_relay=lambda bid: asked_src.append(bid))
    q = FakeQuery([[report(0)], [report(3)]])
    rec(q)
    rec(q)
    assert asked_print == [0, 1, 2, 3] and asked_src == []
    ev = Events()
    rec = ProgressRecorder(make_log(ev), "cfg", "run1", session=None, printer=lambda *_: None,
                           read_source_relay=lambda bid: asked_src.append(bid))
    q = FakeQuery([[report(0)], [report(3)]])
    rec(q)
    rec(q)
    assert [r["batch_id"] for r in rows_of(ev, "batch_summary")] == [0, 3] and asked_src == [0, 1, 2, 3]

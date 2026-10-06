"""Offline unit tests for pipeline_lib.stream_wait.await_stream, driven by a fake query. No Spark.

The load-bearing contracts:
- A query whose wait call never reports the end (the reproduced hang: FAILED query, awaitTermination never
  returning) is still noticed through isActive, and its failure is raised, so the task fails and restarts.
- A normal end returns; a failed query raises its own exception; a slow query just keeps waiting.
- A recording failure stops a running query and raises; a final recording failure after the query failed
  never masks the query's error.
"""
import pytest

from pipeline_lib.stream_wait import await_stream


class QueryFailed(Exception):
    pass


class FakeQuery:
    """Scripted query. `waits` is a list of per-slice outcomes for awaitTermination: True (ended), False
    (still running), or an exception instance (raised). After the script runs out, the last outcome
    repeats. `active` likewise scripts isActive (a bool or an exception instance per slice)."""

    def __init__(self, waits, active=None, failure=None):
        self.waits = list(waits)
        self.active = list(active) if active is not None else None
        self.failure = failure
        self.wait_calls = 0
        self.active_calls = 0
        self.stopped = False

    @staticmethod
    def _next(script, i):
        return script[min(i, len(script) - 1)]

    def awaitTermination(self, timeout):
        assert timeout == 10
        # A loop that never notices the end would spin forever against a hung script: fail it instead.
        if self.wait_calls > 2000:
            raise AssertionError("the wait loop never noticed the query ended (hung)")
        out = self._next(self.waits, self.wait_calls)
        self.wait_calls += 1
        if isinstance(out, Exception):
            raise out
        return out

    @property
    def isActive(self):
        if self.active is None:
            return True
        out = self._next(self.active, self.active_calls)
        self.active_calls += 1
        if isinstance(out, Exception):
            raise out
        return out

    def exception(self):
        return self.failure

    def stop(self):
        self.stopped = True


def _run(q, record=None):
    logs = []
    await_stream(q, 10, record=record, log=logs.append)
    return logs


def test_normal_end_returns_after_waiting():
    q = FakeQuery([False, False, True])
    _run(q)
    assert q.wait_calls == 3


def test_failed_query_raises_its_exception_from_the_wait_call():
    q = FakeQuery([False, QueryFailed("schema changed")])
    with pytest.raises(QueryFailed, match="schema changed"):
        _run(q)


def test_hung_wait_call_is_caught_by_isactive_and_the_failure_is_raised():
    # The reproduced hang: the wait call keeps saying "still running" although the query has FAILED.
    q = FakeQuery([False], active=[True, True, False], failure=QueryFailed("DELTA_SCHEMA_CHANGED_WITH_VERSION"))
    with pytest.raises(QueryFailed, match="DELTA_SCHEMA_CHANGED_WITH_VERSION"):
        _run(q)
    assert q.wait_calls == 3


def test_hung_wait_call_on_a_cleanly_stopped_query_returns():
    q = FakeQuery([False], active=[False], failure=None)
    logs = _run(q)
    assert any("no longer active" in m for m in logs)


def test_slow_query_keeps_waiting_through_many_slices():
    # A long micro-batch: many "still running" slices, the query active throughout, then a clean end.
    q = FakeQuery([False] * 500 + [True], active=[True])
    _run(q)
    assert q.wait_calls == 501 and not q.stopped


def test_isactive_error_is_not_fatal_and_waiting_continues():
    q = FakeQuery([False, False, True], active=[RuntimeError("rpc blip"), True])
    logs = _run(q)
    assert q.wait_calls == 3
    assert any("could not check whether the query is active" in m for m in logs)


def test_record_runs_every_slice():
    calls = []
    q = FakeQuery([False, False, True])
    _run(q, record=lambda query: calls.append(query))
    assert len(calls) == 3


def test_record_failure_stops_the_running_query_and_raises():
    q = FakeQuery([False])

    def record(_query):
        raise RuntimeError("monitoring log append failed")

    with pytest.raises(RuntimeError, match="monitoring log append failed"):
        _run(q, record=record)
    assert q.stopped


def test_record_failure_after_a_clean_end_raises_without_stopping():
    q = FakeQuery([True])

    def record(_query):
        raise RuntimeError("append failed")

    with pytest.raises(RuntimeError, match="append failed"):
        _run(q, record=record)
    assert not q.stopped


def test_query_failure_still_records_once_and_record_failure_never_masks_it():
    calls = []

    def record(_query):
        calls.append(1)
        raise RuntimeError("append failed too")

    q = FakeQuery([QueryFailed("the real error")])
    with pytest.raises(QueryFailed, match="the real error") as info:
        _run(q, record=record)
    assert calls == [1]
    notes = getattr(info.value, "__notes__", [])
    assert any("final progress not recorded" in n for n in notes)


def test_query_failure_records_final_progress_when_record_works():
    calls = []
    q = FakeQuery([False, QueryFailed("boom")])
    with pytest.raises(QueryFailed):
        _run(q, record=lambda _q: calls.append(1))
    assert calls == [1, 1]

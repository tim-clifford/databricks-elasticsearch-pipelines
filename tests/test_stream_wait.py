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
    # The failure becomes visible only once isActive reports False, so this exercises the isActive path.
    class FailsOnceInactive(FakeQuery):
        def exception(self):
            return self.failure if self.active_calls >= 3 else None

    q = FailsOnceInactive([False], active=[True, True, False],
                          failure=QueryFailed("DELTA_SCHEMA_CHANGED_WITH_VERSION"))
    with pytest.raises(QueryFailed, match="DELTA_SCHEMA_CHANGED_WITH_VERSION"):
        _run(q)
    assert q.wait_calls == 3


def test_slow_query_keeps_waiting_through_many_slices():
    # A long micro-batch: many "still running" slices, the query active throughout, then a clean end.
    q = FakeQuery([False] * 500 + [True], active=[True])
    _run(q)
    assert q.wait_calls == 501 and not q.stopped


def test_isactive_error_is_not_fatal_and_waiting_continues():
    q = FakeQuery([False, False, True], active=[RuntimeError("rpc blip"), True])
    logs = _run(q)
    assert q.wait_calls == 3
    assert any("could not check the query's state" in m for m in logs)


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


def test_record_failure_after_liveness_detected_failure_raises_the_query_error():
    # Isaac finding: on the isActive-end path a failed monitoring append must not mask the stream's error.
    q = FakeQuery([False], active=[False], failure=QueryFailed("the stream error"))

    def record(_query):
        raise RuntimeError("append failed")

    with pytest.raises(QueryFailed, match="the stream error") as info:
        _run(q, record=record)
    assert any("append failed" in n for n in getattr(info.value, "__notes__", []))


def test_isactive_error_streak_resets_on_a_good_check():
    # 29 errors, one good check, 29 errors, then a clean end: never 30 in a row, so no failure.
    errs = [RuntimeError("blip")] * 29
    q = FakeQuery([False] * 60 + [True], active=errs + [True] + errs + [True])
    _run(q)
    assert q.wait_calls == 61


def test_failure_recorded_while_isactive_stays_stale_true_is_still_raised():
    # Isaac finding: if both the wait call AND isActive kept reporting "running" for a failed query, the
    # loop would spin forever. query.exception() is checked every slice as an independent signal.
    q = FakeQuery([False], active=[True], failure=QueryFailed("failed but isActive is stale"))
    with pytest.raises(QueryFailed, match="isActive is stale"):
        _run(q)
    assert q.wait_calls == 1


def test_exception_check_error_is_not_fatal():
    class FlakyException(FakeQuery):
        def exception(self):
            self.exc_calls = getattr(self, "exc_calls", 0) + 1
            if self.exc_calls == 1:
                raise RuntimeError("rpc blip")
            return None

    q = FlakyException([False, False, True], active=[True])
    _run(q)
    assert q.wait_calls == 3


def test_isactive_errors_with_a_readable_exception_check_do_not_fail_a_healthy_stream():
    # Isaac finding: if only isActive errors while the query's recorded state is still readable (no
    # failure), the stream is healthy and must not be failed after MAX_ISACTIVE_ERRORS slices.
    q = FakeQuery([False] * 40 + [True], active=[RuntimeError("isActive rpc broken")])
    _run(q)
    assert q.wait_calls == 41


def test_unreadable_state_on_both_checks_still_fails_after_the_limit():
    class Unreadable(FakeQuery):
        def exception(self):
            raise RuntimeError("exception rpc broken")

    q = Unreadable([False], active=[RuntimeError("isActive rpc broken")])
    with pytest.raises(RuntimeError, match="could not determine whether the query is active"):
        _run(q)
    assert q.wait_calls == 30


def test_liveness_end_with_late_failure_visibility_raises_via_confirming_wait():
    # Isaac finding: isActive flips False before exception() is populated. The end must be confirmed by a
    # final wait call, which raises the failure, instead of being reported as a clean stop.
    class LateFailure(FakeQuery):
        def awaitTermination(self, timeout):
            self.wait_calls += 1
            if self.wait_calls == 1:
                return False  # the wait call missed the end
            raise QueryFailed("failure visible only to the confirming wait")

        def exception(self):
            return None  # not yet populated

    q = LateFailure([False], active=[False])
    with pytest.raises(QueryFailed, match="confirming wait"):
        _run(q)


def test_liveness_end_that_cannot_be_confirmed_fails_closed():
    # isActive says ended, no failure recorded, but the confirming wait still says "running": the outcome is
    # unknown, so fail (a restart is safe) rather than report a clean stop.
    q = FakeQuery([False], active=[False], failure=None)
    with pytest.raises(RuntimeError, match="could not confirm how the query ended"):
        _run(q)


def test_liveness_end_confirmed_clean_returns():
    q = FakeQuery([False, True], active=[False], failure=None)
    _run(q)
    assert q.wait_calls == 2

"""The wait loop run_index_pipeline.py uses to wait on a running Structured Streaming query: wait in short
slices, record progress between slices, and notice the query ending even when the wait call does not.

Why not a plain blocking `query.awaitTermination()`: under Spark Connect (serverless, and classic clusters in
STANDARD access mode) that call is one long-lived server operation, and it was reproduced on FEVM (DBR 17.3
classic STANDARD, 2026-10-05) NEVER returning after its query FAILED. The Spark UI showed the query FAILED,
but the task sat RUNNING for over an hour, so the Jobs continuous trigger never saw a failure and never
restarted the task. The same family of long-lived Connect calls carries the StreamingQueryListener event
stream, which the server can abandon (INVALID_HANDLE.OPERATION_ABANDONED), after which PySpark drops every
client-side listener for the rest of the run.

How this works instead: `awaitTermination(timeout)` returns as soon as the query ends (it is not a sleep),
so a finished or failed query is seen immediately; between slices the loop asks the query directly whether
it is still active, which catches an end the wait call missed within one slice. Waiting this way does not
touch the query, which runs on the cluster on its own: a micro-batch of any length simply spans many slices.

PURE: the query is duck-typed (awaitTermination(timeout), isActive, exception(), stop()), so this module has
no Spark import and is unit-tested off-cluster with a fake query.
"""

# How many CONSECUTIVE slices may fail to read the query's state (BOTH isActive and exception()) before the
# loop gives up and raises. A
# transient blip is tolerated (the next slice decides), but a state that can never be read must not leave
# the task waiting forever, which is the hang this module exists to prevent. 30 slices of 10 s = 5 min.
MAX_ISACTIVE_ERRORS = 30


def await_stream(query, poll_seconds, record=None, log=print):
    """Block until `query` ends, waiting in `poll_seconds` slices. Returns normally ONLY when the query ended
    without an error (an availableNow drain finishing, or a graceful stop); raises the query's exception
    when it failed, exactly as a blocking awaitTermination() would.

    `record` (optional) is called with the query after every slice, to record progress (the notebook prints
    each new batch's STREAM_PROGRESS line and appends its batch_summary row). Its contract:
    - It handles its OWN transient read problems (a failed progress read warns and is retried next slice).
    - Whatever it RAISES is a real failure (a monitoring log append that failed): the loop stops the query,
      if it is still running, and re-raises, so the task fails instead of streaming on unlogged.
    - When the query FAILED, `record` still runs once (best effort) so batches that committed since the last
      slice get their summaries; a failure there never masks the query's own error (it is attached as a
      note when the runtime supports exception notes).

    Between slices the loop checks two independent signals: `query.isActive` and `query.exception()` (a
    failure the server has recorded). Either one ending the wait is enough. An `isActive` check that itself
    errors is treated as "cannot tell this slice": the loop keeps waiting
    and the next wait call (which raises on a broken connection or failed query) decides. After
    MAX_ISACTIVE_ERRORS consecutive failed checks it raises instead, so a query whose state can never be
    read fails the task rather than hanging it. If the query is found ended AND has failed, its own error
    is what propagates, even when recording progress then fails too (that is attached as a note). `log`
    receives one-line status messages."""
    isactive_errors = 0
    ended_by_liveness = False  # the end was seen by a state check, not by the wait call
    while True:
        try:
            ended = query.awaitTermination(poll_seconds)  # True once terminated; RAISES if it failed
        except Exception as query_exc:
            if record is not None:
                try:
                    record(query)
                except Exception as record_exc:
                    log(f"WARNING: could not record the final progress ({type(record_exc).__name__}: "
                        f"{record_exc})")
                    if hasattr(query_exc, "add_note"):
                        query_exc.add_note(f"monitoring log: final progress not recorded: {record_exc}")
            raise
        if not ended:
            # Two independent liveness signals, each a fresh request to the server: isActive, and the failure
            # the server has RECORDED for the query (query.exception()). Either one ending the wait is enough,
            # so the hang guard does not depend on isActive alone (the reproduced hang was a long-held wait
            # call; the server itself knew the query had FAILED). A check that errors means "cannot tell this
            # slice"; only when BOTH fail does the slice count toward MAX_ISACTIVE_ERRORS, so one broken call
            # cannot fail a stream whose state is still readable through the other.
            state_read = False
            check_errors = []
            try:
                ended = not query.isActive
                state_read = True
                if ended:
                    log("query is no longer active although the wait call had not returned; "
                        "treating it as ended")
            except Exception as check_exc:
                check_errors.append(check_exc)
            if not ended:
                try:
                    ended = query.exception() is not None
                    state_read = True
                    if ended:
                        log("query has a recorded failure although the wait call had not returned; "
                            "treating it as ended")
                except Exception as check_exc:
                    check_errors.append(check_exc)
            if ended:
                ended_by_liveness = True
            if state_read:
                isactive_errors = 0
            else:
                isactive_errors += 1
                last = check_errors[-1]
                if isactive_errors >= MAX_ISACTIVE_ERRORS:
                    raise RuntimeError(
                        f"could not determine whether the query is active for {isactive_errors} consecutive "
                        f"polls ({poll_seconds}s apart); failing rather than waiting blind "
                        f"(last error: {type(last).__name__}: {last})") from last
            for check_exc in check_errors:
                log(f"WARNING: could not check the query's state ({type(check_exc).__name__}: {check_exc}); "
                    f"checking again on the next poll")
        if record is not None:
            try:
                record(query)
            except Exception as record_exc:
                if not ended:
                    try:
                        query.stop()
                    except Exception as stop_exc:
                        log(f"WARNING: could not stop the query after a recording failure "
                            f"({type(stop_exc).__name__}: {stop_exc})")
                    raise
                # The query already ended: if it FAILED, its error is the one to report (the recording
                # failure rides along as a note), matching the wait-call-raised branch above.
                failure = query.exception()
                if failure is None:
                    raise
                if hasattr(failure, "add_note"):
                    failure.add_note(f"monitoring log: final progress not recorded: {record_exc}")
                raise failure
        if ended:
            failure = query.exception()
            if failure is not None:
                raise failure
            if ended_by_liveness:
                # The wait call never reported this end, so CONFIRM the outcome before calling it clean: the
                # failure may not be visible to exception() yet (isActive can flip first). For an ended query
                # the wait call returns at once: True for a clean end, or it RAISES the failure. If it still
                # says "running", the outcome is unknown, so fail (the task's retry restarts the stream,
                # which is always safe) rather than report a clean stop that would never be restarted.
                confirmed = query.awaitTermination(poll_seconds)
                failure = query.exception()
                if failure is not None:
                    raise failure
                if not confirmed:
                    raise RuntimeError("the query reported itself ended, but the wait call could not confirm "
                                       "how the query ended; failing so the stream is restarted")
            return

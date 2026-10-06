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

    An `isActive` check that itself errors is treated as "cannot tell this slice": the loop keeps waiting
    and the next wait call (which raises on a broken connection or failed query) decides. `log` receives
    one-line status messages."""
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
            try:
                ended = not query.isActive
            except Exception as check_exc:
                log(f"WARNING: could not check whether the query is active ({type(check_exc).__name__}: "
                    f"{check_exc}); checking again on the next poll")
            else:
                if ended:
                    log("query is no longer active although the wait call had not returned; "
                        "treating it as ended")
        if record is not None:
            try:
                record(query)
            except Exception:
                if not ended:
                    try:
                        query.stop()
                    except Exception as stop_exc:
                        log(f"WARNING: could not stop the query after a recording failure "
                            f"({type(stop_exc).__name__}: {stop_exc})")
                raise
        if ended:
            failure = query.exception()
            if failure is not None:
                raise failure
            return

"""The streaming per-micro-batch step: the function handed to foreachBatch.

make_foreach_batch builds it from injected pieces, so its ORDER and failure handling are unit-tested with fakes:
  transform -> batch_start -> bulk_write -> metrics file -> batch_end -> (best-effort) BULK_STATS print relay.
- batch_start is written BEFORE any of the batch's data is sent, so a log outage fails the micro-batch (and the
  task) before the data moves.
- Any failure from the write onward (the write itself, or the metrics file) records batch_end status error,
  never masking the original exception, and re-raises: the micro-batch fails, the checkpoint does not advance,
  and Spark reprocesses the batch on the task's retry.
- batch_end status success comes last, so success means everything the batch had to do is done. It carries the
  ES counts and the ES diagnostics (monitoring_sink.batch_success_facts).
- The print relay only copies the per-batch BULK_STATS line to the notebook cell (under Spark Connect this
  function runs on the cluster, so its prints reach only the driver log). It is FAIL-SOFT and carries no logging
  guarantees.

Spark Connect ships this function to the cluster, so it must not capture a Spark session: every Spark operation
uses the micro-batch's own session (batch_df.sparkSession), and the injected writers take it as an argument.
"""
import time
from datetime import datetime, timezone

from pipeline_lib.monitoring_sink import batch_end_row, batch_start_row, batch_success_facts, error_facts
from pipeline_lib.observability import bulk_stats_relay_line, format_bulk_stats


def make_foreach_batch(*, transform, bulk_write, write_config, log, config_name, job_run_id, write_metrics,
                       write_print_relay=None, printer=print, clock=None, timer=time.time):
    """Build the foreachBatch function.

    - transform(batch_df, session) -> the DataFrame to write (the view's SELECT over the batch, filtered,
      optionally repartitioned).
    - bulk_write(df, write_config, raise_on_error=True) -> the connector's result dict.
    - log: a MonitoringLog.
    - write_metrics(session, batch_id, written): persist the batch's written count (the drain summary reads it).
    - write_print_relay(session, batch_id, text) or None: hand the BULK_STATS line to the notebook's progress
      recorder; None when bulk_stats is off.
    """
    clock = clock or (lambda: datetime.now(timezone.utc))

    def foreach_batch(batch_df, batch_id):
        batch_id = int(batch_id)
        session = batch_df.sparkSession
        transformed = transform(batch_df, session)
        start = clock()
        log.append([batch_start_row(config_name, job_run_id, batch_id, {"mode": "streaming"}, start)], session)
        try:
            # raise_on_error=True: bulk_write itself raises on any rejected/unaccounted row, so a batch that does
            # not FULLY succeed fails here and is reprocessed (idempotent only with es_id_field set).
            t0 = timer()
            result = bulk_write(transformed, write_config, raise_on_error=True)
            wall_ms = (timer() - t0) * 1000.0
            write_metrics(session, batch_id, int(result.get("written", 0) or 0))
        except Exception as exc:
            end = clock()
            log.append_failure([batch_end_row(config_name, job_run_id, batch_id, "error", {
                **error_facts(exc), "elapsed_ms": (end - start).total_seconds() * 1000.0,
            }, start, end)], exc, session, log=printer)
            raise
        if "bulk_stats" in result:
            # A compact per-batch rollup in the driver log (this runs on the cluster).
            printer(format_bulk_stats(result["bulk_stats"], oneline=True) + f" batch_id={batch_id}")
        log.append([batch_end_row(config_name, job_run_id, batch_id, "success",
                                  batch_success_facts(result, wall_ms=wall_ms), start, clock())], session)
        if write_print_relay is not None:
            try:
                line = bulk_stats_relay_line(result, batch_id)
                if line is not None:
                    write_print_relay(session, batch_id, line)
            except Exception as exc:  # best-effort: only the cell print is lost
                printer(f"WARNING: could not relay batch {batch_id}'s BULK_STATS line to the notebook "
                        f"({type(exc).__name__}: {exc})")

    return foreach_batch

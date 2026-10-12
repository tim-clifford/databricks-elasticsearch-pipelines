"""The batch-mode export step: one batch, recorded as batch_start -> write -> reconcile -> batch_end.

run_index_pipeline.py's batch cell builds the DataFrame and calls run_batch_export. The order is the contract:
batch_start is written BEFORE any data is sent (a log outage stops the export first); batch_end is written after
the write and its reconciliation, as status success with the ES counts and diagnostics, or status error with the
exception (plus the counts and diagnostics when the write returned, i.e. a reconcile failure). The connector's
bulk_write and reconcile_or_raise are passed in, so the step is unit-tested with fakes.
"""
import time
from datetime import datetime, timezone

from pipeline_lib.monitoring_sink import (
    BATCH_MODE_BATCH_ID,
    batch_end_row,
    batch_start_row,
    batch_success_facts,
    error_facts,
    es_counts,
    es_write_summary,
)
from pipeline_lib.observability import BULK_STATS_TAG, format_bulk_stats, format_tail_summary


def run_batch_export(df, write_config, *, bulk_write, reconcile, log, config_name, job_run_id, session,
                     task_run_id="", printer=print, clock=None, timer=time.time):
    """Export `df` as this run's one batch and return the bulk_write result. Raises whatever the write or the
    reconciliation raises, after recording it as batch_end status error; raises MonitoringLogError if the log
    is on and an append fails (batch_start failing means nothing was sent)."""
    clock = clock or (lambda: datetime.now(timezone.utc))
    start = clock()
    log.append([batch_start_row(config_name, job_run_id, task_run_id, BATCH_MODE_BATCH_ID, {"mode": "batch"},
                                start)], session)
    result = None
    wall_ms = None
    try:
        # Driver wall clock around the write, to LOCATE a tail that persists after the Spark UI shows every task
        # complete: bulk_write's own collect_ms is the time INSIDE Spark's collect, so wall ~ collect_ms puts the
        # tail inside the write (typically a straggler partition, named by the BULK_STATS tail line), while wall
        # well above collect_ms is work after the collect.
        t0 = timer()
        result = bulk_write(df, write_config)
        wall_ms = (timer() - t0) * 1000.0
        printer(f"batch bulk_write result: { {k: v for k, v in result.items() if k != 'bulk_stats'} }")
        printer(f"{BULK_STATS_TAG} driver: bulk_write_wall_ms={wall_ms:.1f}")
        if "bulk_stats" in result:
            # The tail/straggler summary first (where did the wall time go), then the per-partition breakdown.
            printer(format_tail_summary(result))
            printer(format_bulk_stats(result["bulk_stats"]))
        reconcile(result, index=write_config.index)
    except Exception as exc:
        end = clock()
        facts = {**error_facts(exc), "elapsed_ms": (end - start).total_seconds() * 1000.0}
        if isinstance(result, dict):
            # A reconcile failure: the write returned, so say HOW it failed reconciliation.
            facts.update(es_counts(result))
            facts["es"] = es_write_summary(result, wall_ms=wall_ms)
        log.append_failure([batch_end_row(config_name, job_run_id, task_run_id, BATCH_MODE_BATCH_ID, "error", facts,
                                          start, end)],
                           exc, session, log=printer)
        raise
    log.append([batch_end_row(config_name, job_run_id, task_run_id, BATCH_MODE_BATCH_ID, "success",
                              batch_success_facts(result, wall_ms=wall_ms), start, clock())], session)
    return result

"""The RUN level of the monitoring log: run_start, run_end, and the guard that records a failure in any later step.

run_index_pipeline.py creates one RunRecorder per run. start() writes run_start (which also proves the log table
is writable before any data moves). end() writes run_end for an in-process ending (success, or a graceful
`stopped`). guard() wraps each notebook cell after run_start: an exception raised inside is recorded as run_end
status error (at most once per run, never masking the exception) and then re-raised, so the task still fails and
its retry policy applies. A run that is killed or cancelled cannot run any of this; its run_start without a
run_end is the record.
"""
import contextlib
from datetime import datetime, timezone

from pipeline_lib.monitoring_sink import error_facts, run_end_row, run_start_row


class RunRecorder:
    """Writes this run's run-level rows through a MonitoringLog. `identity` (mode, es_index, trigger) is
    repeated on run_end so either row alone says what ran. `clock` returns the current UTC datetime (injected
    for tests)."""

    def __init__(self, log, config_name, job_run_id, identity, clock=None):
        self.log = log
        self.config_name = config_name
        self.job_run_id = job_run_id
        self.identity = dict(identity)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.started_at = None
        self.ended = False

    def start(self, facts, session):
        """Write run_start: the identity plus `facts` (the run's effective settings). Raises on a log failure."""
        self.started_at = self._clock()
        self.log.append([run_start_row(self.config_name, self.job_run_id, {**self.identity, **facts},
                                       self.started_at)], session)

    def end(self, status, facts, session):
        """Write run_end for an in-process ending (status success | stopped). Raises on a log failure."""
        self.log.append([run_end_row(self.config_name, self.job_run_id, status, {**self.identity, **facts},
                                     self.started_at, self._clock())], session)
        self.ended = True

    @contextlib.contextmanager
    def guard(self, session):
        """Record an exception raised inside the block as run_end status error, then re-raise it. Written at most
        once per run (the first failure is the one that ended it) and never masks the exception."""
        try:
            yield
        except Exception as exc:
            if not self.ended:
                self.ended = True
                end = self._clock()
                elapsed = ((end - self.started_at).total_seconds() * 1000.0) if self.started_at else None
                self.log.append_failure([run_end_row(self.config_name, self.job_run_id, "error", {
                    **self.identity, **error_facts(exc), "elapsed_ms": elapsed,
                }, self.started_at, end)], exc, session)
            raise

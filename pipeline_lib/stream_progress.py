"""Recording each streaming batch's Spark progress report: the `record` callback of stream_wait.await_stream.

Between wait slices, ProgressRecorder reads query.recentProgress (a short request; Spark keeps the last
spark.sql.streaming.numRecentProgressUpdates reports, default 100) and, for every batch newer than the last one
recorded (monitoring_sink.progress_batch_ids: a high-water mark, idle triggers skipped):
- prints its STREAM_PROGRESS line, plus the batch's relayed BULK_STATS line when bulk_stats is on;
- appends its batch_summary (Spark's whole progress report, plus the batch's `source_latest` relayed from
  foreachBatch when read_source_relay returns it). FAIL-CLOSED like every log append: a failure raises, and
  await_stream stops the query and fails the task. A missing relay only leaves `source_latest` off the row.
A failed progress READ only warns: the report stays in Spark's buffer and the next slice reads it again.
When a recorded batch skips over batch ids (batches whose report was lost), prune_relays deletes every relay
below it, ONE call per gap (best-effort), so a long-running stream does not accumulate relay files. A last
batch whose report is lost leaves its relays until the next run start clears them.

Spark's progress is best-effort by nature (published asynchronously after the batch commits, kept only in
memory), so a batch whose report is lost (a cancel moments after it committed) has batch_start / batch_end but
no batch_summary. Nothing here tries to reconstruct it.
"""
import json
import time

from pipeline_lib.monitoring_sink import batch_summary_row, progress_batch_ids
from pipeline_lib.observability import PROGRESS_TAG, format_progress


class ProgressRecorder:
    """Callable(query): record newly executed batches (see the module docstring).

    - log: a MonitoringLog; session: the notebook's Spark session (used for the appends).
    - task_run_id: this attempt's {{task.run_id}} ("" on an interactive run).
    - read_print_relay(batch_id) -> str | None, or None when bulk_stats is off: return and delete the batch's
      relayed BULK_STATS line (best-effort; never raises).
    - read_source_relay(batch_id) -> dict | None, or None: return and delete the batch's relayed source_latest
      facts (best-effort; never raises). Read only when the log is on.
    - prune_relays(below_batch_id), or None: delete every relay for a batch id below `below_batch_id`. Called once
      when a recorded batch skipped over ids (their reports were lost). A failure only warns.
    """

    def __init__(self, log, config_name, job_run_id, session, read_print_relay=None, printer=print,
                 task_run_id="", read_source_relay=None, prune_relays=None):
        self.log = log
        self.config_name = config_name
        self.job_run_id = job_run_id
        self.task_run_id = task_run_id
        self.session = session
        self.read_print_relay = read_print_relay
        self.read_source_relay = read_source_relay
        self.prune_relays = prune_relays
        self.printer = printer
        self.last_batch_id = None

    def __call__(self, query):
        try:
            reports = [json.loads(p.json) for p in query.recentProgress]
        except Exception as exc:
            self.printer(f"WARNING: {PROGRESS_TAG} could not read query progress ({type(exc).__name__}: {exc}); "
                         f"retrying on the next poll")
            return
        for report in progress_batch_ids(reports, self.last_batch_id):
            batch_id = report["batchId"]
            if self.prune_relays is not None and self.last_batch_id is not None and batch_id > self.last_batch_id + 1:
                try:
                    self.prune_relays(batch_id)
                except Exception as exc:
                    self.printer(f"WARNING: {PROGRESS_TAG} could not prune relays below batch {batch_id} "
                                 f"({type(exc).__name__}: {exc})")
            self.printer(format_progress(report))
            if self.read_print_relay is not None:
                line = self.read_print_relay(batch_id)
                if line:
                    self.printer(line)
            latest = (self.read_source_relay(batch_id)
                      if self.log.active and self.read_source_relay is not None else None)
            self.log.append([batch_summary_row(self.config_name, self.job_run_id, self.task_run_id, batch_id,
                                                report, source_latest=latest)], self.session)
            self.last_batch_id = batch_id

    def catch_up(self, query, through_batch_id, attempts=5, sleep=time.sleep, pause_seconds=2):
        """After a drain-and-stop run ends, wait briefly for the report of its last batch (`through_batch_id`):
        Spark can publish it just after the query terminates. Polls up to `attempts` times; whatever is still
        missing afterwards keeps batch_start / batch_end without a batch_summary (Spark's report was lost)."""
        for _ in range(attempts):
            if self.last_batch_id is not None and self.last_batch_id >= through_batch_id:
                return
            sleep(pause_seconds)
            self(query)


def prune_relay_dirs(relay_dirs, below_batch_id, ls, rm):
    """Delete every per-batch relay entry (a directory named by its batch id) below `below_batch_id`, with ONE
    listing per relay dir: the notebook's prune_relays. ls(dir) -> entries with .name and .path (dbutils.fs.ls);
    rm(path) deletes one. Best-effort: a missing dir, an entry that is not a batch id, or a failed delete is
    skipped, never raised."""
    for relay_dir in relay_dirs:
        try:
            entries = ls(relay_dir)
        except Exception:
            continue
        for entry in entries:
            name = entry.name.rstrip("/")
            if name.isdigit() and int(name) < below_batch_id:
                try:
                    rm(entry.path)
                except Exception:
                    pass

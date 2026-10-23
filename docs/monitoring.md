# Logging and Monitoring

Where a feed's runs are recorded and how to read them: the run output, failure emails, the bulk-write statistics, the monitoring log, always-on feeds, and cluster logs. For anyone operating feeds. When something has gone wrong, [Troubleshooting](troubleshooting.md) starts from the symptom.

- [Run output](#run-output)
- [Failure emails](#failure-emails)
- [Reading the BULK_STATS output](#reading-the-bulk_stats-output)
- [Monitoring log](#monitoring-log)
- [Watching an always-on feed](#watching-an-always-on-feed)
- [Cluster log delivery](#cluster-log-delivery)

## Run output

Every feed run prints a summary line at the end of its output, in the Jobs UI:

- **Batch:** `BATCH EXPORT COMPLETE: written=<N> deleted=<N> errors=<N> ignored=<N> total_input=<N>`.
  `written` should match `total_input` with `errors=0`. `ignored` counts documents Elasticsearch didn't
  need to apply, such as 409s on already-indexed `_id`s under `op_type: create`.
- **Streaming:** `STREAMING EXPORT COMPLETE: streaming_start=<new|full> batches=<N> rows_pushed=<N> ...`,
  or `STREAMING EXPORT COMPLETE: 0 rows pushed (no new source data since the last run)` when there was
  nothing new to send.

An always-on (continuous) run never prints a summary line, because it never ends: see
[Watching an always-on feed](#watching-an-always-on-feed).

## Failure emails

Every job, generated or fixed, emails the [`support_email`](parameters.md#support_email) list when a job
or task run fails. Skipped and canceled runs don't email. Set the list per target; an empty list sends
nothing.

## Reading the `BULK_STATS` output

With [`bulk_stats`](parameters.md#bulk_stats) on, each batch logs lines tagged `BULK_STATS`:

- **`overall`:** totals for the batch: `docs/send` (real documents per request), the round-trip time the
  client saw (`rtt_ms`) and the time ES reports spending (`took_ms`), CPU and GIL-wait totals
  ([what GIL wait means](throughput.md#python-threads-and-the-gil)), and rejection counts.
- **One line per partition** (batch mode), with median, 95th percentile and maximum for the same timings.
- **`tail:`**, a summary of the slowest partition against the median one: its wall time, sends and
  documents, and the spread (`wall_p95`, `wall_max/median`, `stragglers>2x`, `docs_max/median`).
  Streaming feeds log `overall` and `tail:` for each micro-batch.

`rtt_ms` minus `took_ms` is time spent outside ES: the network, queuing, and decoding on the client.
Every line carries a `ts=` timestamp, which is when the batch actually ran; for streaming, the line can
appear in the log later than that. ([Why](technical-details.md#bulk-stats-relay-timing).)

What each signal says about the write, and which lever to move, is in
[Troubleshooting: Reading the signals](troubleshooting.md#reading-the-signals). For trends across runs,
the monitoring log below keeps each batch's write diagnostics.

## Monitoring log

To keep a durable record of every run and every batch for support, dashboards, alerting, and capacity trends, turn on the monitoring log. The run then appends rows to a shared Delta table. It is off by default. When on, it is part of the export's contract, not best-effort: see [Fail-closed](#fail-closed-when-on) below.

**One-time setup: create the table.** Table creation is a separate, deliberate step, not something an export job does (so export jobs never need `CREATE` permission). Point `monitoring_log_table` at a `catalog.schema.table` (per target, or `--var`), then run `log_table_create` once:

```bash
databricks bundle run log_table_create -t <target> -p <profile>
```

It runs `CREATE TABLE IF NOT EXISTS` with the log's schema (from `framework/pipeline_lib/monitoring_sink.py`), then `ALTER TABLE ADD COLUMNS` for any column an existing table lacks, and is idempotent. The identity that runs this job needs `CREATE TABLE` (and `USE CATALOG` / `USE SCHEMA`) on the target schema. That identity owns the table it creates, so it can always re-run the job; a different identity re-running it on an existing table also needs `SELECT` and `MODIFY` on the table, which adding columns requires. Export jobs only append, so their identity needs only `MODIFY` on the table. If a table name is blank or not a three-part identifier it fails closed.

Set up the monitoring table once per environment in [Environment Setup](setup.md#monitoring-log-table).

**Turn the log on.** Set `monitoring_log_enabled` globally per target in `databricks.yml`, per feed in its config, or per run with `--params monitoring_log_enabled=true`. See [Parameters Reference](parameters.md#run-time-knobs-one-precedence-rule) for the precedence order.

**What lands in the table: the row model.** One append-only table. Every column has a `COMMENT` describing it (visible in `DESCRIBE TABLE` or Catalog Explorer), defined in `framework/pipeline_lib/monitoring_sink.py`.

| Column | Holds |
|---|---|
| `config_name` | the feed config that wrote the row |
| `job_run_id` | `{{job.run_id}}`: shared by every retry of the task, so it groups all attempts of one job run |
| `task_run_id` | `{{task.run_id}}`: one per task attempt, so it tells retries apart (NULL on an interactive run) |
| `record_type`, `status` | the row kind and its outcome (table below) |
| `batch_id` | the micro-batch id, or `0` for a batch-mode run; NULL on run rows |
| `event_ts` | when the event this row records happened (clustering key) |
| `start_ts`, `end_ts` | the span of the run or batch the row describes (`end_ts` NULL on `run_start` / `batch_start`) |
| `docs_written` | documents written by the batch (`batch_end`) or the whole run (`run_end`) |
| `error_type`, `error_message` | the exception on status `error` rows |
| `files_outstanding`, `bytes_outstanding` | `batch_summary` only: the streaming backlog left after the batch (Spark's `numFilesOutstanding` / `numBytesOutstanding`) |
| `source_latest_version`, `source_latest_ts` | streaming `batch_start` and `batch_summary`: the source table's newest commit version made at least [`log_monitor_source_lookback_seconds`](parameters.md#log_monitor_source_lookback_seconds) (default 60) before the read at batch start, and its commit time |
| `source_end_version`, `source_end_index` | `batch_summary` only: where the batch ended in the source, from Spark's end offset |
| `payload` | `VARIANT`: everything else the row records (queryable with `payload:field` paths in SQL) |
| `logged_ts` | when the row was committed to the table (retention is on this) |

The table has two levels, each with a start and an end:

| `record_type` | When it is written | `status` | What `payload` holds |
|---|---|---|---|
| `run_start` | once per run, before anything else | `started` | mode, es_index, view, source, trigger, connector version, effective write settings |
| `batch_start` | immediately before a batch's data is sent to ES | `started` | mode; streaming with the log on also `source_latest` (version, timestamp, or error when the read failed) |
| `batch_end` | when the batch's write finishes | `success` / `error` | ES counts (written, errors, ...) plus `es`, the write diagnostics (`collect_ms`, `bulk_write_wall_ms`, and with `bulk_stats` on the per-partition rollup totals and tail straggler facts); on error `exception_type` / `message` / `elapsed_ms` |
| `batch_summary` | streaming only: when the notebook sees Spark's progress report for the batch | `success` | `progress`: Spark's whole `StreamingQueryProgress` (backlog, step durations, rates, source offsets); `source_latest` relayed from the batch's `batch_start` (absent if the relay was lost) |
| `run_end` | when the run ends in-process | `success` / `error` / `stopped` | totals (batches, rows) or the error |

A batch-mode run is exactly one batch (`batch_id` 0); a streaming run has one set of batch rows per micro-batch. `batch_start` and `batch_end` are written inline from the batch and carry everything about the batch (outcome, counts, ES diagnostics) in both modes. `batch_summary` is Spark's side, streaming only. A `batch_end` with no `batch_summary` is a streaming batch whose progress report was lost (typically a cancel shortly after it committed); its outcome and ES diagnostics are still in `batch_end`. `stopped` is a continuous stream whose query ended without error while the notebook kept running. A job cancel interrupts the notebook itself, so a cancelled run leaves `run_start` with no `run_end`, just like a killed run.

**Reading the gaps.** A `batch_start` with no `batch_end` is a batch that never finished (the task was killed or the driver died mid-write). A `run_start` with no `run_end` is a run that was killed or cancelled. A batch retried by the task's retry policy shows a second `batch_start` for the same `batch_id` under the retry's own `task_run_id`. Per-partition bulk-send detail is not stored (too granular for this log); it still prints in the run log when `bulk_stats` is on.

<a id="fail-closed-when-on"></a>**Fail-closed when on.** With `monitoring_log_enabled` on, the log is required: `monitoring_log_table` unset or malformed, the table missing or unwritable, or any failed append fails the task. So support receives the failure notification and no further data is sent without a log record. Because `batch_start` is written before a batch's data is sent, a log outage stops the export before the next batch reaches Elasticsearch. A failure while recording a failure never hides the original error (the log fault is attached as a note). A failed `batch_summary` append fails the task too. The trade-off: a log write that fails after a batch's data reached Elasticsearch (in `batch_end`) fails the micro-batch before the checkpoint advances, so the task's retry re-sends that batch. With `es_id_field` set that re-send is idempotent; without it, Elasticsearch assigns new ids and the batch's rows are duplicated. See [technical details](technical-details.md#fail-closed-log-trade-off) for the full trade-off.

**Example: every batch of the last day, with outcome and duration:**

```sql
SELECT config_name, job_run_id, task_run_id, batch_id, status, start_ts, end_ts,
       timestampdiff(MILLISECOND, start_ts, end_ts) AS batch_ms,
       docs_written, error_type, error_message
FROM <catalog>.<schema>.<table>
WHERE record_type = 'batch_end' AND event_ts > current_timestamp() - INTERVAL 1 DAY
ORDER BY start_ts DESC;
```

**Example: batches that started but never finished (killed mid-write):**

```sql
SELECT s.config_name, s.job_run_id, s.task_run_id, s.batch_id, s.start_ts
FROM <catalog>.<schema>.<table> s
LEFT ANTI JOIN <catalog>.<schema>.<table> e
  ON e.record_type = 'batch_end' AND e.config_name = s.config_name AND e.job_run_id = s.job_run_id
 AND e.task_run_id <=> s.task_run_id AND e.batch_id = s.batch_id AND e.event_ts >= s.event_ts
WHERE s.record_type = 'batch_start'
ORDER BY s.start_ts DESC;
```

**Example: streaming backlog and 429 pressure per micro-batch:**

```sql
SELECT e.config_name, e.batch_id, e.start_ts, s.files_outstanding, s.bytes_outstanding,
       e.payload:es.overall.rejected_429::bigint AS docs_429,
       e.payload:es.overall.docs_per_send::double AS docs_per_send
FROM <catalog>.<schema>.<table> e
LEFT JOIN <catalog>.<schema>.<table> s
  ON s.record_type = 'batch_summary' AND s.config_name = e.config_name AND s.job_run_id = e.job_run_id
 AND s.batch_id = e.batch_id
WHERE e.record_type = 'batch_end' AND e.status = 'success'
ORDER BY e.start_ts DESC;
```

<a id="caught-up-or-behind"></a>**Caught up or behind.** `files_outstanding` at zero means caught up. A non-zero backlog is ambiguous because Spark measures it after the batch, against the table as it then is, so files that landed while the batch ran count too. Each streaming batch records the source's newest commit made at least [`log_monitor_source_lookback_seconds`](parameters.md#log_monitor_source_lookback_seconds) (default 60) before its start (`source_latest_version`), and `batch_summary` carries both that and where the batch ended (`source_end_version`, `source_end_index`). A batch was **behind** when

```text
source_latest_version >= source_end_version AND files_outstanding > 0
```

and otherwise it was caught up (anything outstanding arrived while it ran). With `source_end_index = -1` every source version below `source_end_version` was sent; with an index of 0 or more the batch stopped inside version `source_end_version` after that file. A fully sent version is always reported as the next version with index -1. See [technical details](technical-details.md#caught-up-or-behind-offset-semantics) for the offset semantics and misread cases.

**Example: caught up or behind per streaming batch:**

```sql
SELECT config_name, batch_id, start_ts, files_outstanding, source_latest_version, source_end_version,
       source_end_index,
       CASE WHEN files_outstanding = 0 THEN 'caught_up'
            WHEN files_outstanding IS NULL OR source_latest_version IS NULL OR source_end_version IS NULL
              THEN 'unknown'
            WHEN source_latest_version >= source_end_version THEN 'behind'
            ELSE 'caught_up' END AS backlog_state
FROM <catalog>.<schema>.<table>
WHERE record_type = 'batch_summary' AND event_ts > current_timestamp() - INTERVAL 1 DAY
ORDER BY config_name, batch_id DESC;
```

**Retention and the prune job.** The table grows without limit otherwise. The `log_table_prune` job enforces retention: it deletes rows older than `monitoring_log_retention_days` (default 90; `0` disables the delete), then optimizes (reclusters and compacts) and vacuums. It runs daily at 03:00 UTC, paused or unpaused by the target's [`schedule_pause_status`](parameters.md#schedule_pause_status) like every other trigger. It fails when `monitoring_log_table` is empty, so in any target where triggers are unpaused but the log isn't used, pause `log_table_prune` (or it fails, and emails `support_email`, every day). Run it on demand with:

```bash
databricks bundle run log_table_prune -t <target> -p <profile>
```

The table is liquid-clustered on `(config_name, event_ts)` (the columns monitoring queries filter by), so data skipping holds up as the table grows. The `log_table_create` job also sets `delta.autoOptimize.optimizeWrite` and `autoCompact` to keep the table compact.

## Watching an always-on feed

An always-on (continuous) run never reaches an end-of-run summary because it never terminates. Health comes from the Databricks Jobs continuous-run state (RUNNING, restart count, failure notifications) plus, with the monitoring log on, the per-batch rows the runner writes as each batch runs. Bound a first-run backfill or large post-restart catch-up with `max_files_per_trigger` and `max_bytes_per_trigger` so one micro-batch does not try to read the whole table.

## Cluster log delivery

Optionally, classic **job clusters** can deliver their driver, executor, and init-script logs to a Unity Catalog Volume, so the full Spark and driver logs survive after the cluster terminates. This is useful for a post-mortem on a continuous stream. It is **opt-in per job cluster config**, in two steps.

**Step 1: set the volume path.** Set the `cluster_log_volume_path` variable to the destination in every target the config deploys to (a simple string, so per target or `--var`):

```
/Volumes/<catalog>/<schema>/<volume>/cluster_logs
```

**Step 2: opt in the cluster config.** In the job cluster config (`_pipelines/job_cluster_configs/<key>.yml`), add the block (the example in `standard_batch.yml` is commented out), then regenerate:

```yaml
cluster_log_conf:
  volumes:
    destination: ${var.cluster_log_volume_path}
```

Then regenerate and redeploy:

```bash
python framework/scripts/gen_jobs.py
databricks bundle deploy -t <target> -p <profile>
```

The generator inlines the spec verbatim, so every job using that config gets it.

**Classic job clusters only.** Serverless compute does not support cluster log delivery, so serverless feeds and the serverless maintenance jobs are unaffected; their output stays in the run log. `existing_cluster` feeds use whatever the existing cluster is configured with. The `es_diagnostics` job's optional job cluster takes its spec from `diagnostics_job_clusters`: add a `cluster_log_conf` block to that spec to deliver its logs too.

**Where logs land.** Databricks writes under `<destination>/<cluster-id>/` (`driver/`, `executor/`, `init_scripts/`), delivering periodically (about every 5 minutes) and at termination.

**Permissions.** The identity the job cluster runs as needs `READ VOLUME` and `WRITE VOLUME` on the volume (plus `USE CATALOG` and `USE SCHEMA`).

**Retention.** Every cluster (every continuous restart, every scheduled run) adds a directory. The `cluster_log_prune` job deletes files older than [`cluster_log_retention_days`](parameters.md#cluster_log_retention_days) (default 30) and the directories that leaves empty, daily, paused or unpaused by `schedule_pause_status`. It only touches the `<destination>/<cluster-id>/<log kind>/` layout and leaves anything else on the volume alone ([Pruning cluster logs](maintenance-jobs.md#pruning-cluster-logs)). See why delivery is opt-in in [technical details](technical-details.md#cluster-log-delivery-opt-in).

**Sensitive contents.** The logs hold everything the notebook prints and the driver and executors log: config, host URLs, error messages and stack traces. Secret values read through `dbutils.secrets` are redacted in notebook output, but don't assume the same for the delivered files: treat the volume as sensitive and restrict who can read it.

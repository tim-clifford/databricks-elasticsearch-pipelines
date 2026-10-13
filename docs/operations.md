# Operations and Monitoring

Running feeds day to day: deploying and running, the maintenance jobs, checkpoints, checking an Elasticsearch host, and the monitoring log. For anyone operating feeds.

- [Deploy and run](#deploy-and-run)
- [Maintenance jobs at a glance](#maintenance-jobs-at-a-glance)
- [Checkpoints](#checkpoints)
- [Resetting a checkpoint](#resetting-a-checkpoint)
- [Diagnosing an Elasticsearch host](#diagnosing-an-elasticsearch-host)
- [Monitoring log](#monitoring-log)
- [Watching an always-on feed](#watching-an-always-on-feed)
- [Cluster log delivery](#cluster-log-delivery)

## Deploy and run

The workflow is generate, deploy, and run. Bundle variables come from the last deploy, so you fill in each target's `variables` block in `databricks.yml` once and need no `--var` on routine deploys.

Generate job definitions from your configs:

```bash
python framework/scripts/gen_jobs.py
```

Deploy to your target environment (variables from `databricks.yml` or `--var`):

```bash
databricks bundle deploy -t dev -p <profile>
```

Create or update the views:

```bash
databricks bundle run deploy_views -t dev -p <profile>
```

Run a feed with the defaults from its config and the last deploy:

```bash
databricks bundle run index_pipeline_<config_name> -t dev -p <profile>
```

Override run-time settings for a single run:

```bash
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> \
  --params filter_condition="action = 'allowed'",chunk_size=1000,max_partition_bytes=8m
```

Stream a full table backfill (instead of the default new commits only):

```bash
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> \
  --params pipeline_mode=streaming,streaming_start=full
```

You can also override bundle variables at deploy, for a one-off wheel path for example:

```bash
databricks bundle deploy -t dev -p <profile> \
  --var="wheel_path=/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"
```

Bundle variables persist from one deploy to the next, so fill in each target's `variables` block in your fork's `databricks.yml` once. Prefer not to commit paths? Set per-target values in the git-ignored `.databricks/bundle/<target>/variable-overrides.json` instead; the bundle resolves them the same way.

Every job grants `CAN_MANAGE_RUN` to the `users` group, so anyone in the workspace can trigger, stop or repair a run without being the job's owner. A run executes as the job's run-as identity, and a parameter override applies to that run only. See [Parameters Reference](parameters.md#how-values-are-set) for the full precedence order.

## Maintenance jobs at a glance

Six fixed jobs handle setup and upkeep. Run any with `databricks bundle run <resource_key> -t <target> -p <profile>`, with optional `--params` as noted.

| Resource key | Display name | What it does | When to run it |
|---|---|---|---|
| `deploy_views` | Deploy views | Creates or updates every feed's view in the workspace. Fails closed if the schema or join logic is invalid. | Once per deploy, before the first index run. Re-run if you modify a view's SQL. |
| `build_wheel` | Build wheel | Builds the `databricks-es-connector` wheel from a checked-out copy of the connector repo (with `--params repo_workspace_path=<path>,wheel_version=<x.x.x>`). Publishes it to the UC Volume next to your existing wheels. Does not switch your feeds to it. | On demand when you want to update the connector version. |
| `checkpoint_clear` | Checkpoint clear | Deletes a feed's checkpoint directory (with `--params config_name=<config_name>`). The next streaming run starts fresh. Safe to run while the stream is paused. | When you wipe an index and want to backfill it, or when you change `streaming_start`. |
| `es_diagnostics` | Diagnostics | Samples an Elasticsearch cluster twice (5 seconds apart by default) and reports a verdict on its health (with optional `--params index_name=<index>,es_host_config=<name>`). Reports load-bearing metrics: write thread-pool saturation, rejections, indexing pressure, circuit breakers, node heap/GC, and cluster state. | When writes are slow or timing out, to see if the host is saturated. |
| `log_table_create` | Log table create | Creates the monitoring Delta table if it does not exist, or adds any new columns to an existing table. Idempotent. (See [Monitoring log](#monitoring-log) below.) | Once, before turning the monitoring log on. Re-run before deploying a build that adds a new monitoring column. |
| `log_table_prune` | Log table prune | Deletes rows older than `monitoring_log_retention_days` (default 90), then optimizes and vacuums. Runs daily on the shared pause setting: live in `prd`, paused elsewhere. | Leave it scheduled where the log is in use; pause it in `prd` if you don't use the log there (it fails without a table). |

Every maintenance job parameter is in [Parameters Reference](parameters.md#maintenance-job-parameters).

## Checkpoints

Each streaming feed keeps its checkpoint at `<checkpoint_base_path>/<config_name>` in the UC Volume. The checkpoint holds the stream's last read position in the source table. If you reset an index in Elasticsearch and want to backfill it from the Delta table, you must clear that checkpoint first, otherwise the stream considers those records already exported and writes nothing. Deterministic document `_id`s make a re-send an idempotent upsert, not a duplicate.

Clear checkpoints with the `checkpoint_clear` job rather than by hand (see [Maintenance jobs at a glance](#maintenance-jobs-at-a-glance) above).

In `dev` environments, use `mode: development` in `databricks.yml` and append `${workspace.current_user.short_name}` to `checkpoint_base_path` so each developer gets their own isolated checkpoint tree. See [Environment Setup](setup.md#per-developer-checkpoints).

## Resetting a checkpoint

When a stream's checkpoint is stale (the index was wiped, or you want to start from a fresh `streaming_start`), clear it with the `checkpoint_clear` job:

```bash
databricks bundle run checkpoint_clear -t <target> -p <profile> --params config_name=<config_name>
```

This deletes the whole checkpoint directory `<checkpoint_base_path>/<config_name>` and does nothing else. It never reads the Elasticsearch secret or writes a document. The next `streaming` run of that feed then behaves like a first run: `streaming_start=new` reseeds at the source's current version, `streaming_start=full` backfills the whole table. Clearing an already-absent checkpoint is a harmless no-op. `config_name` is required and validated (a blank or path-bearing value fails), so the job can only delete the one checkpoint that feed would use, never a shared base or another feed's checkpoint.

For a **continuous** (always-on) feed, pause the continuous trigger first (or cancel the running job), run the clear, then resume, so the always-on stream does not re-establish a checkpoint while you clear it.

Concurrency: unlike every other job, `checkpoint_clear` allows up to 20 concurrent runs, so you can reset several feeds' checkpoints at once. Two runs for the same config are harmless (the second finds nothing to delete). Concurrency does not make it safe to clear a checkpoint while that feed's stream is running: stop the stream first. See [technical details](technical-details.md#checkpoint-clear-concurrency) for the concurrency design.

## Diagnosing an Elasticsearch host

When a host looks backed up (writes slowing, requests timing out), run the read-only `es_diagnostics` job to collect an outside view of what the cluster is doing:

```bash
databricks bundle run es_diagnostics -t <target> -p <profile>                              # default host, cluster/node level
databricks bundle run es_diagnostics -t <target> -p <profile> --params index_name=<index>  # + deep-dive on one index
databricks bundle run es_diagnostics -t <target> -p <profile> --params es_host_config=es_host_secondary  # hit a different host
```

The job only issues `GET` requests (it never writes a document or misuses the ES secret). It connects to the host config named by `es_host_config` (default: the bundle's `default_es_host_config`), using the same endpoints and secrets the feeds use. The host is picked only by name, from the `es_host_configs_json` variable in `databricks.yml`, so someone who can run the job can't point it at another endpoint ([why](technical-details.md#diagnostics-host-by-name-security)).

It gathers: write thread-pool saturation and rejections, indexing pressure, circuit breakers, node heap/GC, cluster health and pending tasks, in-flight bulk tasks, hot threads, and (when `index_name` is set) that index's `_stats`, `_settings`, `_count`, and shard placement. The job samples the counter endpoints twice (`sample_interval_secs` apart, default 5s) so rejection and GC rates reflect the window rather than lifetime totals, then prints a one-line **verdict**:

- `REJECTING`: Elasticsearch is actively shedding load with 429s (write-pool / indexing-pressure / circuit-breaker rejections in the window). This is the idiomatic backpressure signal. Reduce bulk size and write concurrency, and back off on 429s.
- `SATURATED`: the write queue is building but not yet rejecting (near capacity).
- `PRESSURED`: indexing-pressure memory is a high fraction of its limit (rejections imminent).
- `HEAP_GC_PRESSURE`: high heap and/or heavy GC in the window: the node is processing slowly rather than queueing. A slow `_bulk` that times out client-side (rather than a fast 429) points here or at the network.
- `HEALTHY`: every load-bearing signal was collected and is clear.
- `INCONCLUSIVE`: a load-bearing signal (write rejections/queue, indexing pressure) could not be collected, so the host cannot be cleared as healthy. The failed endpoints are listed.

The run **fails only** when it could collect nothing at all (bad host, api_key, TLS, or egress). A verdict of "found congestion" is a successful run; reporting congestion is the job's purpose.

Run parameters: `index_name` (blank checks only cluster and nodes), `es_host_config` (the named host config to check; unknown names fail), `verify_certs` (`false` for self-signed endpoints; ignored when `ca_certs` is set), `sample_interval_secs` (`0` for one snapshot: gauges only, so it can never report `HEALTHY`; use the default two-sample window to clear a host), and `request_timeout_secs` (timeout per request).

By default the job runs serverless. To run it on a cluster instead (for example to match the runtime or the network path a feed's writes take), set one of the `es_diagnostics` compute variables in `databricks.yml` and redeploy. Compute is a deploy-time property. Set `diagnostics_existing_cluster_id` to run on an already-running cluster by id, or `diagnostics_job_clusters` + `diagnostics_job_cluster_key` to run on a new job cluster (copy a `_pipelines/job_cluster_configs/<key>.yml` `new_cluster` spec into the list). Set at most one path. See [Parameters Reference](parameters.md#diagnostics-compute) for details.

## Monitoring log

To keep a durable record of every run and every batch for support, dashboards, alerting, and capacity trends, turn on the monitoring log. The run then appends rows to a shared Delta table. It is off by default. When on, it is part of the export's contract, not best-effort: see [Fail-closed](#fail-closed-when-on) below.

**One-time setup: create the table.** Table creation is a separate, deliberate step, not something an export job does (so export jobs never need `CREATE` permission). Point `monitoring_log_table` at a `catalog.schema.table` (per target, or `--var`), then run `log_table_create` once:

```bash
databricks bundle run log_table_create -t <target> -p <profile>
```

It runs `CREATE TABLE IF NOT EXISTS` with the log's schema (from `framework/pipeline_lib/monitoring_sink.py`) and is idempotent. The identity that runs this job needs `CREATE TABLE` (and `USE CATALOG` / `USE SCHEMA`) on the target schema. Export jobs only append, so their identity needs only `MODIFY` on the table. If a table name is blank or not a three-part identifier it fails closed.

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
| `source_latest_version`, `source_latest_ts` | streaming `batch_start` and `batch_summary`: the source table's newest commit version and commit time, read at batch start |
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

<a id="caught-up-or-behind"></a>**Caught up or behind.** `files_outstanding` at zero means caught up. A non-zero backlog is ambiguous because Spark measures it after the batch, against the table as it then is, so files that landed while the batch ran count too. Each streaming batch reads the source's newest commit at its start (`source_latest_version`), and `batch_summary` carries both that and where the batch ended (`source_end_version`, `source_end_index`). A batch was **behind** when

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

**Retention and the prune job.** The table grows without limit otherwise. The `log_table_prune` job enforces retention: it deletes rows older than `monitoring_log_retention_days` (default 90; `0` disables the delete), then optimizes (reclusters and compacts) and vacuums. It runs daily at 03:00 UTC on the same pause setting as every scheduled job ([`schedule_pause_status`](parameters.md#schedule_pause_status)), so it fires in `prd` and is paused in `dev` and `stg` until you unpause it. It fails when `monitoring_log_table` is empty, so pause it in any target where the schedule is live but the log isn't used. Run it on demand with:

```bash
databricks bundle run log_table_prune -t <target> -p <profile>
```

The table is liquid-clustered on `(config_name, event_ts)` (the columns monitoring queries filter by), so data skipping holds up as the table grows. The `log_table_create` job also sets `delta.autoOptimize.optimizeWrite` and `autoCompact` to keep the table compact.

## Watching an always-on feed

An always-on (continuous) run never reaches an end-of-run summary because it never terminates. Health comes from the Databricks Jobs continuous-run state (RUNNING, restart count, failure notifications) plus, with the monitoring log on, the per-batch rows the runner writes as each batch runs. Bound a first-run backfill or large post-restart catch-up with `max_files_per_trigger` and `max_bytes_per_trigger` so one micro-batch does not try to read the whole table.

See the worked example in [`ecs_dns_activity_continuous.yml`](../_pipelines/pipeline_configs/ecs_dns_activity_continuous.yml).

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

**They accumulate.** There is no automatic retention: every cluster (every continuous restart, every scheduled run) adds a directory. Prune the volume yourself (or add a cleanup job) if storage matters. See why this is opt-in in [technical details](technical-details.md#cluster-log-delivery-opt-in).

**Sensitive contents.** The logs hold everything the notebook prints and the driver and executors log: config, host URLs, error messages and stack traces. Secret values read through `dbutils.secrets` are redacted in notebook output, but don't assume the same for the delivered files: treat the volume as sensitive and restrict who can read it.

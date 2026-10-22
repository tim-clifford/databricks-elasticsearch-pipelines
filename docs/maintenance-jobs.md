# Maintenance Jobs

The fixed jobs that set up and look after the feeds: views, checkpoints, the connector wheel, the Elasticsearch host check, and the monitoring log table. For anyone operating feeds.

- [Maintenance jobs at a glance](#maintenance-jobs-at-a-glance)
- [Checkpoints](#checkpoints)
- [Resetting a checkpoint](#resetting-a-checkpoint)
- [Renaming a feed](#renaming-a-feed)
- [Diagnosing an Elasticsearch host](#diagnosing-an-elasticsearch-host)

## Maintenance jobs at a glance

Six fixed jobs handle setup and upkeep. Run any with `databricks bundle run <resource_key> -t <target> -p <profile>`, with optional `--params` as noted.

| Resource key | Display name | What it does | When to run it |
|---|---|---|---|
| `deploy_views` | Deploy views | Creates or updates every feed's view in the workspace. Fails closed if the schema or join logic is invalid. | Once per deploy, before the first index run. Re-run if you modify a view's SQL. |
| `build_wheel` | Build wheel | Builds the `databricks-es-connector` wheel from a checked-out copy of the connector repo (with `--params repo_workspace_path=<path>,wheel_version=<x.x.x>`). Publishes it to the UC Volume next to your existing wheels. Does not switch your feeds to it. | On demand when you want to update the connector version. |
| `checkpoint_clear` | Checkpoint clear | Deletes a feed's checkpoint directory (with `--params config_name=<config_name>`). The next streaming run starts fresh. Safe to run while the stream is paused. | When you wipe an index and want to backfill it, or when you change `streaming_start`. |
| `es_diagnostics` | Diagnostics | A work in progress ([see below](#diagnosing-an-elasticsearch-host)). Samples an Elasticsearch cluster twice (5 seconds apart by default) and reports a verdict on its health (with optional `--params index_name=<index>,es_host_config=<name>`). Reports write thread-pool saturation, rejections, indexing pressure, circuit breakers, node heap/GC, and cluster state. | When writes are slow or timing out, as one input on whether the host is saturated. |
| `log_table_create` | Log table create | Creates the monitoring Delta table if it does not exist, or adds any new columns to an existing table. Idempotent. (See [Monitoring log](monitoring.md#monitoring-log) below.) | Once, before turning the monitoring log on. Re-run after deploying a build that adds a monitoring column, before its feeds run again ([how](framework-maintenance.md#upgrading-an-existing-log-table)). |
| `log_table_prune` | Log table prune | Deletes rows older than `monitoring_log_retention_days` (default 90), then optimizes and vacuums. Runs daily, paused or unpaused by the target's `schedule_pause_status`. | Leave it scheduled where the log is in use; pause it in any target where triggers are unpaused but the log isn't used (it fails without a table). |

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

## Renaming a feed

A feed's config file name is its `config_name`, and the framework keys the feed by it: its job
(`index_pipeline_<config_name>`), its checkpoint (`<checkpoint_base_path>/<config_name>`) and its rows in
the [monitoring log](monitoring.md#monitoring-log). So renaming the file creates a new feed rather than
renaming the old one. At the next generate and deploy the old job is removed and a new job takes its
place (for a [job group](feed-configuration.md#job-groups) member, the group's job stays and only the
feed's task, `index_pipeline_<config_name>`, is replaced), and the monitoring log has the feed's history under the old name and its new runs under the new
one. Renaming a `job_group` or setting `job_name_postfix` doesn't change `config_name`, so neither
affects the checkpoint.

A batch feed loses nothing: every run re-exports the whole view.

**A streaming feed's position does not carry over.** The new name has no checkpoint, so its first run is
a first run and [`streaming_start`](parameters.md#streaming_start) decides where it begins. The old
checkpoint is left where it was, unused.

- **`new`** (the default) starts at the source's version as of the start of that first run. Everything
  after the point the old feed had reached, up to then, is never sent, leaving a **gap** in the index
  that only a backfill fills. The gap starts where the old stream ended, not when it last ran: if the old
  feed was behind, the rows it hadn't reached yet are in the gap too.
- **`full`** re-sends the whole table. That leaves no gap, but it is a full
  [backfill](backfill.md) in size, and without [`es_id_field`](parameters.md#es_id_field) every document
  already in the index is written again as a duplicate.

To rename a streaming feed:

1. **Stop the old feed once it is caught up.** Check that its last batch was caught up, not behind
   ([Caught up or behind](monitoring.md#caught-up-or-behind), if the monitoring log is on), so the gap
   starts at its last run. Then, if it has a schedule or continuous trigger, pause it
   (`pause_status: PAUSED` in its config, then generate and deploy; see
   [Pausing triggers](feed-configuration.md#pausing-triggers)), and let any running run finish. A
   continuous feed's run doesn't finish on its own: once the trigger is paused, cancel the run if it is
   still going, and check that it has stopped.
   - A job group shares one trigger, declared in one or more members' configs (if the member you're
     renaming is the only one, the trigger moves with the file). If the group's job has one, pausing it pauses the
     whole group, so every member stops until step 4 (streaming members then catch up from their
     checkpoints); members must agree on `pause_status`, so change it on any other member that sets it
     too. If the group's job has no trigger, there is nothing to pause: just don't start it until step 4.
2. **Note where it stopped:** the start time of its last successful run (for a continuous feed, of its
   last successful batch: that `batch_end` row's `start_ts` in the monitoring log) and, if the monitoring log is on, its
   last `batch_summary` row's `source_end_version` and `source_end_index`
   ([how to read them](monitoring.md#caught-up-or-behind)). If it was behind after all, the gap starts
   at that version (its commit time is in `DESCRIBE HISTORY` on the source), not at the last run.
3. **Choose how to close the gap** (see [How to Backfill](backfill.md)):
   - **Re-send the whole table**, if that volume is acceptable: set `streaming_start: full` in the
     renamed config, so the new feed's first run (scheduled or not) starts at the beginning of the table
     ([Method 1](backfill.md#method-1-streaming-from-the-start-of-the-table-streaming-feeds-only)).
   - **Backfill only the gap:** leave `streaming_start` at `new`, and fill the gap after the new feed's
     first run (step 5).
4. **Rename the config file**, put `pause_status` back to what it was before step 1 (on every config you
   changed), regenerate, and deploy. Then run the new feed, or let its trigger start it.
5. **Once the new feed has completed its first batch** (a successful run, or a successful `batch_end`
   row in the monitoring log; for a continuous feed, its first successful batch):
   - If you set `streaming_start: full`, remove it again, then regenerate and deploy, so a later
     checkpoint reset doesn't re-send the whole table. Removing it before a batch has completed would
     let a failed first run restart with `new` and skip the re-send.
   - If you're backfilling the gap, send the rows in it with batch runs filtered to that range, from a
     separate [backfill config](backfill.md#option-2-a-parallel-job-for-the-backfill) so they don't
     collide with the renamed feed's own runs
     ([Method 2](backfill.md#method-2-batch-runs-in-sized-pieces)). That needs a column recording when
     each row landed, and that time can be earlier than its commit, so err wide: start well before the
     point noted in step 2 and end well after the start of the new feed's first run. With `es_id_field`
     set the overlap is harmless. Without it, every row in the overlap is written again as a duplicate
     (rows already in the index keep their random ids), so keep the range as tight as the data allows and
     expect some duplicates at its edges.
6. **Clear the old checkpoint** with `checkpoint_clear` and `config_name=<old name>`. First check that no
   current config has the old name: the job deletes that name's checkpoint either way, and if a feed uses
   the name, that checkpoint is live. (With no such config, the job warns and clears it anyway.) Left in place, it would be picked up by any future feed
   given the old name, which would resume from the old position and send everything since.

## Diagnosing an Elasticsearch host

> **Work in progress.** `es_diagnostics` is still being developed. Treat its checks and verdicts as a
> starting point for looking into a slow host, not as a production monitoring tool, and confirm what it
> reports with the Elasticsearch cluster's own monitoring.

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
- `HEALTHY`: every key signal was collected and is clear.
- `INCONCLUSIVE`: a key signal (write rejections/queue, indexing pressure) could not be collected, so the host cannot be cleared as healthy. The failed endpoints are listed.

The run **fails only** when it could collect nothing at all (bad host, api_key, TLS, or egress). A verdict of "found congestion" is a successful run; reporting congestion is the job's purpose.

Run parameters: `index_name` (blank checks only cluster and nodes), `es_host_config` (the named host config to check; unknown names fail), `verify_certs` (`false` for self-signed endpoints; ignored when `ca_certs` is set), `sample_interval_secs` (`0` for one snapshot: gauges only, so it can never report `HEALTHY`; use the default two-sample window to clear a host), and `request_timeout_secs` (timeout per request).

By default the job runs serverless. To run it on a cluster instead (for example to match the runtime or the network path a feed's writes take), set one of the `es_diagnostics` compute variables in `databricks.yml` and redeploy. Compute is a deploy-time property. Set `diagnostics_existing_cluster_id` to run on an already-running cluster by id, or `diagnostics_job_clusters` + `diagnostics_job_cluster_key` to run on a new job cluster (copy a `_pipelines/job_cluster_configs/<key>.yml` `new_cluster` spec into the list). Set at most one path. See [Parameters Reference](parameters.md#diagnostics-compute) for details.

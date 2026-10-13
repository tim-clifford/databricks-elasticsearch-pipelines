# Maintenance Jobs

The fixed jobs that set up and look after the feeds: views, checkpoints, the connector wheel, the Elasticsearch host check, and the monitoring log table. For anyone operating feeds.

- [Maintenance jobs at a glance](#maintenance-jobs-at-a-glance)
- [Checkpoints](#checkpoints)
- [Resetting a checkpoint](#resetting-a-checkpoint)
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

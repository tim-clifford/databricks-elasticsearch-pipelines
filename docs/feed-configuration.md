# Feed Configuration Guide

How to shape a feed: its view, mode, compute, schedule and grouping. For anyone who has a first feed working ([New Feed Quick Start](quick-start.md)) and wants to change how it runs. Each config key is defined in the [Parameters Reference](parameters.md); this guide explains how to use them together.

- [Views](#views)
- [Reference tables](#reference-tables)
- [Sharing a view across feeds](#sharing-a-view-across-feeds)
- [Batch or streaming](#batch-or-streaming)
- [Compute](#compute)
- [Job tags](#job-tags)
- [Scheduling](#scheduling)
- [Continuous streaming](#continuous-streaming)
- [Job groups](#job-groups)
- [Job naming](#job-naming)

## Views

Each `.sql` file in `_pipelines/pipeline_views/` defines one view that a feed exports. The filename matches the view name it creates (for example, `ecs_dns_activity.sql` creates the `ecs_dns_activity` view, typically feeding an `ecs-dns-activity` index). View names use underscores because Databricks view names cannot contain unquoted hyphens.

Within the view SQL, use three `${...}` parameters resolved per-feed from its config at deploy time:

| Parameter | Resolves to |
|---|---|
| `${view}` | the fully-qualified view to create: `catalog.schema.name` |
| `${source}` | the fully-qualified source table: `catalog.schema.table` |
| `${ref_<alias>}` | a reference (join) table, aliased: `catalog.schema.table <alias>` |

Each parameter is assembled from the feed config, with any `${environment}` component already folded in. An unknown `${...}` parameter in the SQL is a hard error (fails closed at deploy), so a typo cannot create a view pointing at the wrong location. Do not write literal `${...}` in SQL comments unless it is one of the parameters above.

## Reference tables

A view has exactly one source table, but may join reference tables such as dimension or lookup tables. Declare each under `reference_tables` in the feed config; the key is the join alias you use in the SQL as `${ref_<alias>}`:

```sql
SELECT
    base.dsl_id,
    (validation.dsl_id IS NOT NULL) AS validation_row_exists
FROM ${source} base
LEFT JOIN ${ref_validation} ON base.dsl_id = validation.dsl_id
```

The feed config specifies where each reference table lives; the SQL specifies the join itself (type, `ON` clause, surfaced columns, and any tuning hints like `/*+ BROADCAST(alias) */`).

## Sharing a view across feeds

More than one feed may point at the same view (same `view:` block and `.sql` file). This is how you fan one transformed view out to multiple destinations: give each feed its own config with a different `es_index_name`, `es_host_config`, and/or `filter_condition` (a subset), all reading the one view. The `deploy_views` job creates the view once and prints a `WARNING` naming all sharing feeds (so an accidental duplicate view name is still visible).

The sharing feeds must agree on the view definition: the `view:` target, the `source:`, and every `reference_tables` entry. They may differ only on the Elasticsearch side: `es_index_name`, `es_host_config`, `es_id_field`, `filter_condition`, and `pipeline_mode`. If two sharers would render different definitions for one view name, `deploy_views` fails that view closed and rejects the deploy. The shared view must contain every sharer's `es_id_field` column (each feed checks its own `_id` column against the view's resolved schema).

## Batch or streaming

A feed exports data in one of two modes, set by [`pipeline_mode`](parameters.md#pipeline_mode) (global default `batch`):

- **Batch mode** (`pipeline_mode: batch`): Each job run re-exports the entire view. Schedule it to run periodically, or run it on demand.
- **Streaming mode** (`pipeline_mode: streaming`): Each job run drains new commits from the source table since the last run, applies the view logic to those commits, and exports them. Schedule it to run periodically (it stops after draining available commits), or run it always-on with a `continuous` block for lower latency.

### Per-micro-batch view execution

In streaming mode, the view logic runs over each micro-batch, not by reading the deployed view. The runner takes the view's `SELECT` statement, binds the micro-batch as the source table in place of the real table, and executes the projection, joins, and hints against batch-sized data. Reference (join) tables are read as their real tables. This means the exact same projection, joins, and SQL logic from the view apply, but to batch-sized data rather than the full stream. See [technical details](technical-details.md#streaming-view-per-micro-batch) for the implementation.

### Row-wise views only for streaming

Streaming mode supports only row-wise views: projection, filters, scalar expressions, and 1:1 reference joins where each output row depends on a single source row. A view that aggregates across source rows (e.g., `GROUP BY`, `DISTINCT`, window functions, `PIVOT`) would be computed per micro-batch, not over the whole stream, so its streamed results would silently differ from batch mode. Use `batch` mode for aggregating views.

### Append-only assumption

The stream assumes the source is append-only. The runner uses `skipChangeCommits`, so non-append commits (manual `UPDATE`, `DELETE`, or `MERGE` on the source) are skipped rather than failing the stream. If you make such a change and need it reflected in the index, re-send the affected records with a `batch` run.

To manage stream position, see [Maintenance Jobs](maintenance-jobs.md#resetting-a-checkpoint).

## Compute

By default each feed job runs as a serverless notebook task (no cluster). Set an optional `compute` block per feed to run it elsewhere; the choice is per feed, so different feeds can run on different compute.

| Type | Extra key | Runs on |
|---|---|---|
| `serverless` (default) | none | serverless notebook task |
| `existing_cluster` | `cluster_config` | an existing all-purpose/interactive cluster, named by a per-target bundle variable |
| `job_cluster` | `job_cluster_config` | a job cluster created fresh per run from a reusable spec |

The whole `compute` block is validated fail-closed: an unrecognized `type`, a missing required key, or a stray key for the chosen type is rejected at config load (and by `gen_jobs.py --check`).

### Existing cluster

Attach to an existing interactive cluster. A cluster id is workspace-specific, so you name a per-target bundle variable (never a literal id), allowing one config to attach to a different cluster per environment (dev/stg/prd):

```yaml
compute:
  type: existing_cluster
  cluster_config: interactive_primary   # -> existing_cluster_id: ${var.interactive_primary.cluster_id}
```

The `cluster_config` names a `databricks.yml` cluster config: a `type: complex` bundle variable with a single `cluster_id` field, set per target with empty placeholders on `main`. The generator emits `existing_cluster_id: ${var.<name>.cluster_id}` and the bundle resolves the right cluster id per target at deploy. This mirrors `es_host_config`: a workspace-specific value is never a literal, always a per-target variable. A `cluster_config` that names no declared cluster config fails at generation (`framework/scripts/gen_jobs.py`), before deploy.

### Job cluster

Run on a job cluster created per run from a reusable spec:

```yaml
compute:
  type: job_cluster
  job_cluster_config: standard_batch    # -> _pipelines/job_cluster_configs/standard_batch.yml
```

### Job-cluster specs

Reusable job-cluster specs live in `_pipelines/job_cluster_configs/<key>.yml`. Each file is a Databricks [`new_cluster`](https://docs.databricks.com/api/workspace/jobs/create) spec (`spark_version`, `node_type_id`, `num_workers` or `autoscale`, `spark_conf`, etc.), defined once and referenced by its filename stem from any number of feeds' `compute.job_cluster_config`. The generator inlines the spec into each referencing job's `job_clusters` block, so there is no copy-paste; `databricks bundle validate` checks the spec's own fields at deploy. A job cluster is created fresh for each run and torn down after; for a single always-on cluster shared across jobs, use `existing_cluster` instead. A `job_cluster_config` that names no existing file fails the generator, so a bad reference never deploys.

### Cluster policy and tags

The generator injects the target's [`cluster_policy_id`](parameters.md#cluster_policy_id) into every job cluster, so all job-cluster feeds run under that workspace's cluster policy; set it in every target a job-cluster feed deploys to. Serverless and `existing_cluster` feeds have no job cluster and are unaffected.

Hardcoded `custom_tags` in a job-cluster spec pass straight through onto the cluster (e.g., `project: elastic`).

## Job tags

### Global job tags

The `global_job_tags` bundle variable (a `type: complex` map, empty `{}` on `main`) is applied to every job the bundle deploys: the generated feed/group jobs and the six fixed maintenance jobs in `framework/resources/`. Set it in `databricks.yml` by replacing `default: {}` with a `key: value` mapping:

```yaml
global_job_tags:
  type: complex
  default:
    environment: "${var.environment}"   # per-target value (resolves at deploy)
    team: "search-platform"
    cost_center: "CC-1234"
```

A value may be a bundle reference such as `${var.environment}`, so the `environment` tag can carry the same per-target value the cluster `custom_tags` use. Keep the key set at the top level: generated jobs take their tag keys from the top-level default at generation, and only values written as `${var...}` references vary per target ([why](technical-details.md#global-job-tags-key-set)). At most 24 global tags are allowed, and a `dev` (`mode: development`) target adds its own `dev` tag at deploy, so keep the set one lower there. Limits and quoting rules are under [`global_job_tags`](parameters.md#global_job_tags).

### `es_index_list` tag

In addition to the global tags, `gen_jobs.py` adds one generated tag to each generated job: `es_index_list`, a distinct, sorted, space-separated list of the `es_index_name`(s) that job writes (one index for a standalone feed, the distinct set for a job group). It is reserved; `global_job_tags` must not define it. This is the one case where a generated job's `tags:` is never empty even when `global_job_tags` is `{}`. See [technical details](technical-details.md#es-index-list-tag) for why the separator is a space, not a comma.

### Tags on clusters and serverless

Databricks forwards job tags to the job cluster as cluster tags, so on a `job_cluster` feed the global tags also land on the cluster alongside the spec's `custom_tags`. Serverless jobs have no cluster, so job-level tags are the only tag lever. A tag whose key or value conflicts with a value fixed by the workspace cluster policy fails at deploy, the same as any policy conflict. Tag value length is bounded by the cloud provider (AWS 255, Azure 256, GCP 63) once a tag reaches a cluster. To keep a large job group deployable, `gen_jobs.py` truncates a long `es_index_list` to a conservative cap (255), keeping whole index names in sorted order and appending a ` ...+N` marker for the count dropped (see [technical details](technical-details.md#es-index-list-tag) for why truncation matters). The global tags are independent and are never truncated.

## Scheduling

By default each feed job is on-demand (run it with `bundle run` or the API). Add an optional `schedule` block per feed to run it on a [Quartz cron](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html):

```yaml
schedule:
  quartz_cron_expression: "0 0 8 * * ?"   # 08:00 every day
```

The timezone is always UTC (not a config field). Quartz cron has 6 or 7 fields (seconds first, optional trailing year), so a 5-field Unix cron like `0 8 * * *` is rejected at config load with a clear message rather than failing at deploy. Omitting `schedule` leaves the job on-demand.

The schedule pairs naturally with either export mode: a `batch` job re-exports the view on each tick, and a `streaming` job drains new source commits since its last run on each tick (it uses `Trigger.availableNow`, so a scheduled run processes the delta and stops).

### Overlapping runs skipped

Every job except `checkpoint_clear` (which allows 20 concurrent runs, one per feed) sets `max_concurrent_runs: 1` and `queue: {enabled: false}`, so a scheduled tick that fires while the previous run is still going is skipped (`MAX_CONCURRENT_RUNS_EXCEEDED`) rather than overlapping or queueing up. Skipping loses no work: a streaming job's next tick drains the full backlog since its last checkpoint, and a batch job's next tick re-exports the whole view. The `queue: {enabled: false}` is load-bearing (see [technical details](technical-details.md#queue-disabled)).

### Where schedules fire

Every generated schedule's `pause_status` is bound to the `schedule_pause_status` variable, which defaults to `PAUSED` (fail-safe), so a target controls firing without touching feed configs: `dev` and `stg` inherit the paused default, so schedules are deployed but dormant in both; only `prd` binds `UNPAUSED` and actually fires them. Unpause a single job in the UI/API for a one-off test, or set `--var=schedule_pause_status=UNPAUSED` at deploy to override.

### Per-feed pause override

A single feed can opt out of the target-wide default with its own top-level `pause_status: PAUSED | UNPAUSED` config key, which overrides `${var.schedule_pause_status}` for that one job's trigger. A feed that omits `pause_status` inherits the target-wide default, so this is fully backward-compatible. It applies to the job's `schedule` or `continuous` trigger, so it is only valid on a job that has one: an on-demand feed (no `schedule`, no `continuous`, and not in a `job_group`) that sets `pause_status` is rejected at config load, and a `job_group` with no trigger at all that sets it is rejected by `gen_jobs.py`.

```yaml
pause_status: UNPAUSED          # optional; overrides the target-wide schedule_pause_status default
schedule:
  quartz_cron_expression: "0 */10 * * * ?"
```

## Continuous streaming

A scheduled `streaming` job drains new commits and stops on each tick. For lower latency you can instead run a stream as always-on with a `continuous` block (mutually exclusive with `schedule`):

```yaml
pipeline_mode: streaming
compute:
  type: job_cluster            # classic compute REQUIRED
  job_cluster_config: standard_batch
continuous:
  trigger_interval: 30 seconds  # Spark ProcessingTime cadence
```

This emits a Databricks Jobs continuous trigger: the orchestrator keeps exactly one run perpetually active and restarts it on completion or failure, and the runner drives the stream with `Trigger.ProcessingTime(interval)` instead of `Trigger.availableNow`, so it never terminates. `trigger_interval` is the target gap between the start of one micro-batch and the next; batches never overlap. A shorter interval trades throughput (more, smaller bulk writes and index refreshes) for lower latency.

### Classic compute only

Serverless notebooks support only `Trigger.availableNow`, not the `ProcessingTime` trigger an always-on stream needs. So `continuous` requires `compute.type` to be `job_cluster` or `existing_cluster`; `continuous` on serverless, `continuous` with `pipeline_mode: batch`, and `continuous` together with `schedule` are each rejected at config load (and by `gen_jobs.py --check`), before deploy. See [technical details](technical-details.md#serverless-and-continuous) for the specifics.

### Pausing

Like a schedule, the continuous trigger's `pause_status` is bound to `schedule_pause_status` (default `PAUSED`), so `dev`/`stg` deploy the always-on job dormant and only `prd` (or an explicit `--var=schedule_pause_status=UNPAUSED`) actually runs it. A single feed can override this with its own top-level `pause_status` key, which applies to the continuous trigger the same way. One always-on run holds its job cluster for as long as it is unpaused, so treat it as a running-cost commitment.

### Failure recovery

A continuous job's per-task recovery is governed by the continuous trigger's `task_retry_mode: ON_FAILURE`. This is pinned by the generator and is load-bearing: when a task's stream fails, Databricks retries that task in place (with exponential backoff) as long as at least one other task in the run is still on its first attempt, so one stream can recover without disturbing the others. When that no longer holds, or the retry limit is reached, the whole run is cancelled and a fresh one started. This matters most for a job group of continuous streams (multiple tasks in one run): see [technical details](technical-details.md#task-retry-mode) for why.

### Schema changes

An added source column fails the stream once with `DELTA_SCHEMA_CHANGED_WITH_VERSION` ("Please try restarting the query"); the task retry restarts it with the new schema and it continues. The views list their columns explicitly, so a new column does not reach Elasticsearch until the view is changed to include it. A renamed or dropped column fails on every attempt with `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG` and needs an operator (deliberately: no `schemaTrackingLocation` is set). See [technical details](technical-details.md#schema-changes-in-a-stream) for how this interacts with the task retry limit.

### Worked example

See [`_pipelines/pipeline_configs/ecs_dns_activity_continuous.yml`](../_pipelines/pipeline_configs/ecs_dns_activity_continuous.yml) for a continuous streaming feed example.

## Job groups

By default each feed config becomes its own Databricks job (`index_pipeline_<config_name>`). Set an optional `job_group` on two or more configs to merge them into one job instead, with each member config as an independent task (no inter-task dependencies):

```yaml
# in each member config
job_group: ecs_streams
```

The generator emits a single `_pipelines/resources/group_<job_group>.job.yml` (resource key `index_pipeline_group_<job_group>`) with one task per member (`index_pipeline_<config_name>`, unchanged). The main reason to group is compute sharing: members that name the same `job_cluster_config` land on one physical job cluster (the generator deduplicates the `job_clusters` block), so N feeds run on one cluster instead of N. Members that name different cluster configs, or a mix of serverless/`existing_cluster`, still coexist as tasks in the one job. Size a shared cluster for the sum of its concurrent tasks (they run at once, and continuous members run perpetually).

### What members must agree on

All validated fail-closed at generation by `gen_jobs.py --check`:

- **Trigger** (define-once): a Databricks job has exactly one trigger, so members must not declare conflicting ones. Any member may set a `schedule` or `continuous` block (or omit it); the group is on-demand if none declare one, adopts the single declared trigger if one or more agree, and is rejected if members declare different triggers (two different crons, or `schedule` vs `continuous`). A continuous group propagates its `trigger_interval` to every member (so a member that omitted `continuous` still runs always-on rather than draining under `availableNow`) and requires every member to be `streaming` on classic compute.
- **`job_name_postfix`** (define-once): same rule (omit on most members and set once); conflicting values are rejected.

### Members writing one index

Members may target the same `es_index_name` (e.g., one task per disjoint `filter_condition` subset feeding a single index), which is allowed but deliberate. Grouped members run as concurrent tasks in one run (`max_concurrent_runs: 1` serializes job runs, not tasks within a run), so two tasks write that index at the same time. That is safe for disjoint rows; overlapping rows either duplicate (Elasticsearch auto-ids) or race on upserts (a shared `es_id_field`). Give each member a distinct `filter_condition` and set `es_id_field` for idempotency.

### Run-time parameters baked per task

A standalone job exposes the run-time knobs (`pipeline_mode`, `filter_condition`, `chunk_size`, etc.) as job-level parameters, overridable per run with `--params`. A job cannot hold per-member parameter defaults, so a grouped job instead bakes each member's knobs into that task's `base_parameters` at deploy. The runner reads the same widgets either way (no behavior change), but on a grouped job these are not `--params`-overridable per member: change a member's value in its config and redeploy. A run-time `notebook_params` override still applies job-wide to every task. If you need per-member run-time overrides, keep those configs standalone.

## Job naming

A job's display name is `[<target>] <prefix>: <postfix>`.

- **`prefix`** is the `job_name_prefix` bundle variable (`databricks.yml`), default `databricks-elasticsearch-pipelines`. The generator emits `${var.job_name_prefix}`, so you can rebrand every job name per target (or with `--var=job_name_prefix=<name>`) without regenerating. The hand-authored `deploy_views` job references the same variable, so it rebrands alongside the generated jobs.
- **`postfix`** defaults to the config name (standalone) or the group name (group). Set an optional `job_name_postfix` on a config to override just the trailing display segment (e.g., `"ECS DNS (serverless)"`). It is purely cosmetic (it never changes any resource key or task key), so it may contain spaces and punctuation. In a group it follows the define-once rule.


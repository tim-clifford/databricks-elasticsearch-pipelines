# Feed Configuration Guide

The full guide to configuring a feed: its view, mode, compute, triggers and grouping. Use it to set up a feed from the start or to change how an existing one runs. [New Feed Quick Start](quick-start.md) is the short path to a first working feed, but doesn't contain all the available options that are documented here. Each config key is defined in the [Parameters Reference](parameters.md); this guide explains how to use them together. Several choices here (compute, partitions, micro-batch size) also impact how much data a feed sends to Elasticsearch at a time: [Throughput Tuning](throughput.md) covers how to size them.

- [Feeds, tables and indices](#feeds-tables-and-indices)
- [Views](#views)
- [Reference tables](#reference-tables)
- [Sharing a view across feeds](#sharing-a-view-across-feeds)
- [Batch or streaming](#batch-or-streaming)
- [Compute](#compute)
- [Job tags](#job-tags)
- [When a feed runs](#when-a-feed-runs)
  - [On demand](#on-demand)
  - [CRON scheduling](#cron-scheduling)
  - [Continuous streaming](#continuous-streaming)
  - [Pausing triggers](#pausing-triggers)
  - [Overlapping runs skipped](#overlapping-runs-skipped)
- [Job groups](#job-groups)
- [Job naming](#job-naming)

## Feeds, tables and indices

Each feed config maps one source table, through its view, to one Elasticsearch index. Beyond that one-to-one link, feeds can overlap on either side:

- **Several feeds can read the same source table**, through one shared view or each through its own view, for example to send different columns or different subsets of rows to different indices or hosts.
- **Several feeds can write the same index**, for example one feed per source table going to the same index, or one per `filter_condition` subset of a table.

Both are allowed, and neither is checked for overlap. When feeds share a source or an index, configure them so that each document comes from exactly one feed: give feeds that write one index disjoint rows (separate tables, or non-overlapping `filter_condition`s), and set [`es_id_field`](parameters.md#es_id_field) so a row that is sent twice overwrites itself instead of becoming a second document. Overlapping feeds either duplicate documents (with Elasticsearch-assigned ids) or overwrite each other's documents (with a shared `es_id_field`), and the result depends on which feed ran last. Grouped feeds writing one index add one more consideration: see [Members writing one index](#members-writing-one-index).

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

A feed exports data in one of two modes, set by [`pipeline_mode`](parameters.md#pipeline_mode):

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

By default each feed job runs as a serverless notebook task (no cluster). Set an optional `compute` block per feed to run it elsewhere; the choice is per feed, so different feeds can run on different compute. The compute a feed runs on sets how many cores it writes with, which is one of the main throughput levers: see [Throughput Tuning: Worker cores](throughput.md#worker-cores).

| Type | Extra key | Runs on |
|---|---|---|
| `serverless` (default) | none | serverless notebook task |
| `job_cluster` | `job_cluster_config` | a job cluster created fresh per run from a reusable spec |
| `existing_cluster` | `cluster_config` | an existing all-purpose (interactive) cluster, named by a per-target bundle variable |

The whole `compute` block is validated fail-closed: an unrecognized `type`, a missing required key, or a stray key for the chosen type is rejected at config load (and by `gen_jobs.py --check`).

### Job cluster

Run on a job cluster created per run from a reusable spec:

```yaml
compute:
  type: job_cluster
  job_cluster_config: standard_batch    # -> _pipelines/job_cluster_configs/standard_batch.yml
```

### Job-cluster specs

Reusable job-cluster specs live in `_pipelines/job_cluster_configs/<key>.yml`. Each file is a Databricks [`new_cluster`](https://docs.databricks.com/api/workspace/jobs/create) spec (`spark_version`, `node_type_id`, `num_workers` or `autoscale`, `spark_conf`, etc.), defined once and referenced by its filename stem from any number of feeds' `compute.job_cluster_config`. The generator inlines the spec into each referencing job's `job_clusters` block, so there is no copy-paste; `databricks bundle validate` checks the spec's own fields at deploy. A job cluster is created fresh for each run and torn down after. A `job_cluster_config` that names no existing file fails the generator, so a bad reference never deploys.

The node type and worker count set the cluster's total worker cores. To choose them for a feed's volume, see [Throughput Tuning](throughput.md#worker-cores).

### Existing interactive cluster

Attach to an existing interactive cluster. A cluster id is workspace-specific, so you name a per-target bundle variable (never a literal id), allowing one config to attach to a different cluster per environment (dev/stg/prd):

```yaml
compute:
  type: existing_cluster
  cluster_config: interactive_primary   # -> existing_cluster_id: ${var.interactive_primary.cluster_id}
```

The `cluster_config` names a `databricks.yml` cluster config: a `type: complex` bundle variable with a single `cluster_id` field, set per target. The generator emits `existing_cluster_id: ${var.<name>.cluster_id}` and the bundle resolves the right cluster id per target at deploy. This mirrors `es_host_config`: a workspace-specific value is never a literal, always a per-target variable. A `cluster_config` that names no declared cluster config fails at generation (`framework/scripts/gen_jobs.py`), before deploy.

### Cluster policy

The generator injects the target's [`cluster_policy_id`](parameters.md#cluster_policy_id) into every job cluster, so all job-cluster feeds run under that workspace's cluster policy; set it in every target a job-cluster feed deploys to. Serverless and `existing_cluster` feeds have no job cluster and are unaffected.

## Job tags

### Global job tags

The `global_job_tags` bundle variable (a `type: complex` map) is applied to every job the bundle deploys: the generated feed/group jobs and the six fixed maintenance jobs in `framework/resources/`. Set it in `databricks.yml` as a `key: value` mapping under `default:`:

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

Hardcoded `custom_tags` in a job-cluster spec pass straight through onto the cluster (e.g., `project: elastic`). Databricks also forwards job tags to the job cluster as cluster tags, so on a `job_cluster` feed the global tags land on the cluster alongside the spec's `custom_tags`. Serverless jobs have no cluster, so job-level tags are the only tag lever. A tag whose key or value conflicts with a value fixed by the workspace cluster policy fails at deploy, the same as any policy conflict. Tag value length is bounded by the cloud provider (AWS 255, Azure 256, GCP 63) once a tag reaches a cluster. To keep a large job group deployable, `gen_jobs.py` truncates a long `es_index_list` to a conservative cap (255), keeping whole index names in sorted order and appending a ` ...+N` marker for the count dropped (see [technical details](technical-details.md#es-index-list-tag) for why truncation matters). The global tags are independent and are never truncated.

## When a feed runs

A feed job runs in one of three ways, set in its config:

| Trigger | Config | How it runs |
|---|---|---|
| On demand | neither `schedule` nor `continuous` | only when started (`bundle run`, the Jobs UI or the API) |
| CRON schedule | a `schedule` block | at each tick of a Quartz cron |
| Continuous | a `continuous` block (streaming only) | always on: one run that never ends, restarted if it stops |

`schedule` and `continuous` are mutually exclusive. A scheduled feed can also be started by hand between ticks. The pause state and the skipping of overlapping runs apply to both scheduled and continuous triggers: see [Pausing triggers](#pausing-triggers) and [Overlapping runs skipped](#overlapping-runs-skipped). A failed task is retried in both, by different settings: see [Retrying a failed task](#retrying-a-failed-task).

### On demand

A feed with neither a `schedule` nor a `continuous` block is on-demand: it runs only when started. A batch feed re-exports the view each time; a streaming feed sends the commits since its last run, then stops.

### CRON scheduling

Add a `schedule` block to run a feed on a [Quartz cron](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html):

```yaml
schedule:
  quartz_cron_expression: "0 0 8 * * ?"   # 08:00 every day
```

The timezone is always UTC (not a config field). Quartz cron has 6 or 7 fields (seconds first, optional trailing year), so a 5-field Unix cron like `0 8 * * *` is rejected at config load with a clear message rather than failing at deploy.

The schedule pairs with either export mode: a `batch` job re-exports the view on each tick, and a `streaming` job drains new source commits since its last run on each tick (it uses `Trigger.availableNow`, so a scheduled run processes the delta and stops).

### Continuous streaming

A `continuous` block runs a streaming feed always-on, so new source commits reach Elasticsearch at the cadence of its `trigger_interval` instead of waiting for the next scheduled run:

```yaml
pipeline_mode: streaming
compute:
  type: job_cluster            # classic compute REQUIRED
  job_cluster_config: standard_batch
continuous:
  trigger_interval: 30 seconds  # Spark ProcessingTime cadence
```

This emits a Databricks Jobs continuous trigger: the orchestrator keeps exactly one run perpetually active and restarts it on completion or failure, and the runner drives the stream with `Trigger.ProcessingTime(interval)` instead of `Trigger.availableNow`, so it never terminates. `trigger_interval` is the target gap between the start of one micro-batch and the next; batches never overlap. A shorter interval trades throughput (more, smaller bulk writes and index refreshes) for lower latency; to size each micro-batch, see [Throughput Tuning: Streaming micro-batch size](throughput.md#streaming-micro-batch-size). One always-on run holds its job cluster for as long as it is unpaused, so treat it as a running-cost commitment. The run keeps the settings it started with, so after deploying a change, restart it ([Continuous jobs need a restart](deployment.md#continuous-jobs-need-a-restart)).

#### Classic compute only

Serverless notebooks support only `Trigger.availableNow`, not the `ProcessingTime` trigger an always-on stream needs. So `continuous` requires `compute.type` to be `job_cluster` or `existing_cluster`; `continuous` on serverless, `continuous` with `pipeline_mode: batch`, and `continuous` together with `schedule` are each rejected at config load (and by `gen_jobs.py --check`), before deploy. See [technical details](technical-details.md#serverless-and-continuous) for the specifics.

#### Failure recovery

A continuous job's per-task recovery is governed by the continuous trigger's `task_retry_mode: ON_FAILURE`, which the generator pins because recovery depends on it: when a task's stream fails, Databricks retries that task in place (with exponential backoff) as long as at least one other task in the run is still on its first attempt, so one stream can recover without disturbing the others; while that holds, the retries are not capped and the run is never cancelled. Otherwise (a single-task job, or no other task still on its first attempt), the whole run is cancelled at the retry limit and a fresh one started. This matters most for a job group of continuous streams (multiple tasks in one run): see [technical details](technical-details.md#task-retry-mode) for why.

#### Schema changes

An added source column fails the stream once with `DELTA_SCHEMA_CHANGED_WITH_VERSION` ("Please try restarting the query"); the task retry restarts it with the new schema and it continues. A view that names its columns sends the new column only once the view is changed to include it; a view that uses `SELECT *` sends it from the restarted stream on. A renamed or dropped column fails on every attempt with `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG`, even when the view doesn't use it, and needs an operator (deliberately: no `schemaTrackingLocation` is set): a standalone feed loops through failed runs, and a job group member is retried indefinitely while the other members keep sending. See [Troubleshooting](troubleshooting.md#a-renamed-or-dropped-source-column) for the recovery steps and [technical details](technical-details.md#schema-changes-in-a-stream) for how this interacts with the task retry limit.

### Pausing triggers

Every schedule and continuous trigger has a pause state, `PAUSED` or `UNPAUSED`. A paused trigger is deployed but starts no runs. The state is set in two layers:

- **Per target:** every generated trigger is bound to the [`schedule_pause_status`](parameters.md#schedule_pause_status) bundle variable, which you set per target in `databricks.yml`. That is how one set of feed configs deploys with triggers live in some targets (usually only production) and dormant in the rest.
- **Per feed:** a feed's own top-level [`pause_status`](parameters.md#pause_status) overrides the target value for that one job. A feed that omits it inherits the target value. It is valid only on a job that has a trigger: an on-demand feed (no `schedule`, no `continuous`, and not in a `job_group`) that sets it is rejected at config load, and a `job_group` with no trigger at all that sets it is rejected by `gen_jobs.py`.

```yaml
pause_status: UNPAUSED          # optional; overrides the target's schedule_pause_status for this job
schedule:
  quartz_cron_expression: "0 */10 * * * ?"
```

For a one-off test, unpause a single job in the Jobs UI or API, or deploy with `--var=schedule_pause_status=UNPAUSED` to unpause every trigger in the target that doesn't set its own `pause_status`. The next routine deploy puts back the values from `databricks.yml` and the feed configs.

### Overlapping runs skipped

Every job except `checkpoint_clear` (which allows up to 20 concurrent runs, so several feeds' checkpoints can be cleared at once) sets `max_concurrent_runs: 1` and `queue: {enabled: false}`, so a scheduled tick that comes while the previous run is still going is skipped (`MAX_CONCURRENT_RUNS_EXCEEDED`) rather than overlapping or queueing up. Skipping loses no work: a streaming job's next tick drains the full backlog since its last checkpoint, and a batch job's next tick re-exports the whole view. Without `queue: {enabled: false}` the tick would wait in a queue instead (see [technical details](technical-details.md#queue-disabled)).

### Retrying a failed task

A feed job without a continuous trigger (scheduled, on demand, or started by hand) re-runs a failed task within the same run, up to [`triggered_task_max_retries`](parameters.md#triggered_task_max_retries) times (shipped `2`), each retry starting at least [`triggered_task_min_retry_interval_millis`](parameters.md#triggered_task_min_retry_interval_millis) after the failed attempt **started** (shipped `60000`, one minute). Set either in `databricks.yml` for every feed in a target, or in a feed's config for that feed:

```yaml
triggered_task_max_retries: 3                   # 0 turns retries off for this feed
triggered_task_min_retry_interval_millis: 120000
```

What a retry does:

- **Batch:** re-exports the whole view. With [`es_id_field`](parameters.md#es_id_field) set this overwrites the same documents; without it, documents the failed attempt already sent are written again as duplicates.
- **Streaming:** resumes from the checkpoint, so it re-sends at most the micro-batch that was in flight and then carries on. The same `es_id_field` point applies.
- **Job group:** each task retries on its own; the other members are not stopped or re-run. The run fails if any member's last attempt fails, and succeeds if every member ends successful.

Every failed attempt emails [`support_email`](parameters.md#support_email), including one that a later retry recovers. A run whose task failed and then succeeded on retry ends `SUCCESS`, so an alert is not the same as a failed run: look at the task's attempts in the run.

Size the two settings against the schedule. A run that is still retrying when the next tick comes makes that tick skip ([Overlapping runs skipped](#overlapping-runs-skipped)), so keep `(retries + 1) x (attempt time, or the interval if longer)` under the schedule's interval.

A continuous feed doesn't use these settings: it recovers through the pinned `task_retry_mode` ([Failure recovery](#failure-recovery)), and setting either key on a continuous feed, or on a member of a continuous job group, fails generation (the Jobs API rejects a task retry policy on a continuous job). The [maintenance jobs](maintenance-jobs.md) never retry.

The connector also retries inside a task, before the task fails. The layers, from innermost:

| Layer | Setting | Retries |
|---|---|---|
| Document | [`max_retries_per_doc`](parameters.md#max_retries_per_doc) | one document ES rejects with a 429 |
| Request | [`max_retries_per_request`](parameters.md#max_retries_per_request) | a whole bulk request that timed out, lost its connection, or got a 429/502/503/504 |
| Task (triggered jobs) | `triggered_task_max_retries` | the whole task, after the connector's retries are used up or anything else fails it |
| Task (continuous jobs) | `task_retry_mode: ON_FAILURE` (pinned) | the whole task, or the whole run at the retry limit |

See [technical details](technical-details.md#triggered-task-retries) for what was measured.

## Job groups

By default each feed config becomes its own Databricks job (`index_pipeline_<config_name>`). Set an optional `job_group` on two or more configs to merge them into one job instead, with each member config as an independent task (no inter-task dependencies):

```yaml
# in each member config
job_group: ecs_streams
```

The generator emits a single `_pipelines/resources/group_<job_group>.job.yml` (resource key `index_pipeline_group_<job_group>`) with one task per member (`index_pipeline_<config_name>`, unchanged). The main reason to group is compute sharing: members that name the same `job_cluster_config` land on one physical job cluster (the generator deduplicates the `job_clusters` block), so N feeds run on one cluster instead of N. Members that name different cluster configs, or a mix of serverless/`existing_cluster`, still coexist as tasks in the one job. Size a shared cluster for the sum of its concurrent tasks (they run at once, and continuous members run perpetually); see [Throughput Tuning](throughput.md#worker-cores).

### What members must agree on

All validated fail-closed at generation by `gen_jobs.py --check`. For each of these, set the value on one member, or the same value on several or all of them; the members that omit it inherit it. Only different values are rejected.

- **Trigger:** a Databricks job has exactly one trigger. The group is on-demand if no member declares a `schedule` or `continuous` block, adopts the declared trigger if every member that declares one agrees, and is rejected if members declare different triggers (two different crons, or `schedule` vs `continuous`). A continuous group propagates its `trigger_interval` to every member (so a member that omitted `continuous` still runs always-on rather than draining under `availableNow`) and requires every member to be `streaming` on classic compute.
- **`pause_status`:** the same rule.
- **`job_name_postfix`:** the same rule.

### Members writing one index

Members may target the same `es_index_name` (e.g., one task per disjoint `filter_condition` subset feeding a single index), which is allowed but deliberate ([Feeds, tables and indices](#feeds-tables-and-indices) covers the general rule). Grouped members run as concurrent tasks in one run (`max_concurrent_runs: 1` serializes job runs, not tasks within a run), so two tasks write that index at the same time. That is safe for disjoint rows; overlapping rows either duplicate (Elasticsearch auto-ids) or race on upserts (a shared `es_id_field`). Give each member a distinct `filter_condition` and set `es_id_field` for idempotency.

### Run-time parameters baked per task

A standalone job exposes the run-time knobs (`pipeline_mode`, `filter_condition`, `chunk_size`, etc.) as job-level parameters, overridable per run with `--params`. A job cannot hold per-member parameter defaults, so a grouped job instead bakes each member's knobs into that task's `base_parameters` at deploy. The runner reads the same widgets either way (no behavior change), but on a grouped job these are not `--params`-overridable per member: change a member's value in its config and redeploy. A run-time `notebook_params` override still applies job-wide to every task. If you need per-member run-time overrides, keep those configs standalone.

## Job naming

A job's display name is `[<target>] <prefix>: <postfix>`.

- **`prefix`** is the [`job_name_prefix`](parameters.md#job_name_prefix) bundle variable in `databricks.yml`.
- **`postfix`** defaults to the config name (standalone) or the group name (group). Set an optional `job_name_postfix` on a config to override just the trailing display segment (e.g., `"ECS DNS (serverless)"`). It is purely cosmetic (it never changes any resource key or task key), so it may contain spaces and punctuation. In a group, set it on one member or the same value on several ([What members must agree on](#what-members-must-agree-on)).

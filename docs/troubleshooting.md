# Troubleshooting

What to do when a feed fails, falls behind, writes slowly, sends the wrong data, or doesn't run when it
should, organized by what you see. For operators. Each entry gives the symptom, the error text where
there is one (`...` marks the parts that vary), the cause, and the fix.

## First steps

1. **Read the failed run's output.** Open the run in the Jobs UI. The framework fails closed with a
   specific message: almost every failure names the setting or file at fault.
2. **Check the monitoring log**, if it's on: the run's `run_end` and `batch_end` rows carry the error type
   and message, and once the run is over, a missing end row says where it stopped (while a batch or run
   is still in progress, its end row isn't written yet). For a streaming feed, the
   `batch_summary` rows show the backlog after each batch and whether it was caught up or behind
   ([Logging and Monitoring](monitoring.md#monitoring-log)).

## Symptom index

| Where it fails | Entries |
|---|---|
| [Generating jobs](#generation-fails) | unknown key, undeclared host config, `es_host_configs_json`, cluster configs, tags, job groups, root `resources/` folder, `--check` |
| [Deploying](#deploy-fails) | cluster policy, cluster id, cluster log path, policy conflicts |
| [Starting a run](#run-fails-at-start) | wheel, host config, `${environment}`, checkpoint path, TLS settings, monitoring table, missing index |
| [Writing](#writes-fail-or-are-slow) | timeouts, 429s, slow writes, reading the signals |
| [Falling behind](#falling-behind) | growing backlog, skipped runs, stale index |
| [Data issues](#data-issues) | fewer documents, duplicates, a stream that writes nothing, source updates missing |
| [Streaming](#streaming-failures) | schema changes, continuous retry limit |
| [Schedules](#a-scheduled-job-didnt-run) | paused, skipped |
| [Settings](#a-committed-value-seems-ignored) | a committed value seems ignored |
| [The monitoring log](#gaps-in-the-monitoring-log) | start rows with no end row |

## Generation fails

`python framework/scripts/gen_jobs.py` validates every config and `databricks.yml` before writing anything,
and stops at the first problem.

### Unknown config key

`...: unknown key(s): ...; allowed: ...`

A key in a feed config is misspelled or doesn't exist. Compare it with the
[feed definition keys](parameters.md#feed-definition-keys) and [run-time knobs](parameters.md#run-time-knobs).

### Undeclared host config

`es_host_config '...' is not declared in databricks.yml (declared host configs: ...)`, or
`default_es_host_config (databricks.yml) names '...', which is not a declared host config`

A feed (or the default) names a host config `databricks.yml` doesn't declare. Declare it, or fix the name
([Elasticsearch host configs](setup.md#elasticsearch-host-configs)).

### es_host_configs_json doesn't match

`es_host_configs_json (databricks.yml) must list exactly the declared host configs (...); missing: ...; not
declared: ...`, or `... entry '...' must be exactly ...`, or `... default is not valid JSON: ...`

The map must have one entry per declared host config, each referencing its own host's three fields. Add or
remove entries to match ([`es_host_configs_json`](parameters.md#es_host_configs_json)). A message ending
`is overridden in target(s) ...` means a target sets it; remove that, since it's set once at the top level.

### Cluster config or job cluster spec not found

`cluster_config '...' is not declared as a cluster config in databricks.yml`, or
`compute.job_cluster_config '...' not found: expected _pipelines/job_cluster_configs/....yml (or .yaml)`

Declare the [cluster config](parameters.md#cluster-configs), or create the
[job cluster spec](feed-configuration.md#job-cluster-specs) file the feed names.

### Global job tags rejected

`global_job_tags (databricks.yml) has ... tags; Databricks caps a job at 25 tags ...`, or
`global_job_tags (databricks.yml): tag ... contains a character Databricks rejects in a tag (allowed: ASCII
letters, digits, space, and + - = . : / @ _) ...`, or `... 'es_index_list' is reserved ...`

Keep to 24 tags, use only the allowed characters (no commas), and don't set `es_index_list`
([`global_job_tags`](parameters.md#global_job_tags)).

### Job group conflicts

`job_group '...' has conflicting triggers: ...`, or `... has conflicting pause_status values: ...`, or
`... has conflicting job_name_postfix values: ...`, or `job_group '...' is continuous (always-on), so every
member must be a streaming pipeline on classic compute ...`

Members of a group share one job, so they must agree. Set the trigger, `pause_status` and
`job_name_postfix` on one member (or identically on several) and let the rest inherit
([Job groups](feed-configuration.md#job-groups)).

### Continuous feed on the wrong compute or mode

`...: continuous requires pipeline_mode: streaming ...`, `...: continuous requires classic compute ...`, or
`...: continuous and schedule are mutually exclusive ...`

A continuous feed must set `pipeline_mode: streaming` in its own config, run on `job_cluster` or
`existing_cluster` compute, and have no `schedule`
([Continuous streaming](feed-configuration.md#continuous-streaming)).

### Bad config name

`...: config name '...' is not a valid resource key; use only letters, digits, '_' and '-' ...`, or
`... must not start with '_' ...`

Rename the config file. The name becomes the job's resource key. Renaming a streaming feed that has
already run starts it from a new checkpoint: see [Renaming a feed](maintenance-jobs.md#renaming-a-feed)
first.

### A root resources/ folder or a stale include

`the old layout's root resources/ folder (or a file of that name) still exists; delete it ...`, or
`include (databricks.yml) must list ...`

The copy still has parts of an earlier repo layout, which this framework no longer supports. Job
resources live in `framework/resources/` (the fixed jobs) and `_pipelines/resources/` (the generated
ones), and `databricks.yml` must include exactly those two folders. Move any job of your own from a root
`resources/` folder to `_pipelines/resources/` (the generator leaves files it didn't write alone), delete
the root folder, set `include` to `framework/resources/*.yml` and `_pipelines/resources/*.yml`, and run
the generator again.

### --check fails

`stale/missing generated job file(s): ...` or `orphaned generated job file(s) with no matching config/group: ...`

A config was added, changed, renamed or deleted without regenerating. Run
`python framework/scripts/gen_jobs.py`: it rewrites stale files and removes orphans. Then commit the
result. If `--check` still reports stale files right after regenerating, check that the pinned pyyaml
from `requirements.txt` is installed ([why](technical-details.md#pyyaml-pin)).

## Deploy fails

### Job-cluster feed with no cluster policy

A feed uses `job_cluster` compute and the target's [`cluster_policy_id`](parameters.md#cluster_policy_id)
is empty; the Jobs API rejects an empty policy id. Set it for the target.

### Existing-cluster feed with no cluster id

A feed's [cluster config](parameters.md#cluster-configs) has an empty `cluster_id` in this target. Set it.

### Cluster log delivery with an empty path

`Invalid cluster log storage info` (a 400 from the Jobs API)

A job cluster config opts in to log delivery, but
[`cluster_log_volume_path`](parameters.md#cluster_log_volume_path) is empty in this target. Set it, or
remove the `cluster_log_conf` block ([Cluster log delivery](monitoring.md#cluster-log-delivery)).

### A tag conflicts with the cluster policy

A job tag's key or value conflicts with one the workspace cluster policy fixes. Change the tag in
[`global_job_tags`](parameters.md#global_job_tags) or the job cluster spec's `custom_tags`.

## Run fails at start

### No connector wheel

`wheel_path is required: the UC Volume path to the databricks_es_connector wheel, ...`

Set [`wheel_path`](parameters.md#wheel_path) for the target and redeploy. If the path is set but the
install or import fails, check the file exists at that path, keeps its standard wheel filename, and that
the job's identity can read the volume ([Environment Setup](setup.md#connector-wheel)).

### Connector wheel too old

`wheel_path: databricks-es-connector 0.9.7 is older than the framework's minimum 0.10.0; ...`

`wheel_path` points at a connector release the framework doesn't support. Point it at a 0.10.0 or later
wheel and redeploy ([Environment Setup](setup.md#connector-wheel)).

### Host config empty for the target

`missing required parameter: es_host_url (fill in this pipeline's es_host_config values for this target in
databricks.yml)` (or the same for `secret_scope_name` or `secret_key_name`)

The feed's host config has no values in this target. Fill them in
([Elasticsearch host configs](setup.md#elasticsearch-host-configs)).

### ${environment} empty or illegal

`... uses ${environment} but no environment was provided`, or `... resolved to '...' (from '...' with
environment='...'), which is not a legal SQL identifier`

Set [`environment`](parameters.md#environment) for the target, to a value that makes a legal identifier
(letters, digits and underscores; no hyphens).

### No checkpoint path for a streaming run

`missing required parameter: checkpoint_base_path (set the bundle variable at deploy); a streaming run needs
a UC Volume checkpoint location`

Set [`checkpoint_base_path`](parameters.md#checkpoint_base_path) for the target and redeploy.

### CA bundle and verify_certs conflict

`ca_certs is set but verify_certs is False, pick one`

[`ca_certs`](parameters.md#ca_certs) is set and [`verify_certs`](parameters.md#verify_certs) is `false`.
To verify against your CA, leave `verify_certs` unset or `true`; to skip verification, clear `ca_certs`.

### Monitoring log table not usable

`monitoring_log_enabled=true but monitoring_log_table is unset ...`, a `monitoring_log_table` name that
isn't a three-part `catalog.schema.table`, or a table-not-found or permission error on the first write

The log is on and must be writable. Set
[`monitoring_log_table`](parameters.md#monitoring_log_table), create the table with `log_table_create`,
and give the job's identity `MODIFY` on it ([setup](setup.md#monitoring-log-table)). Or turn
[`monitoring_log_enabled`](parameters.md#monitoring_log_enabled) off.

### Index doesn't exist

`index '...' does not exist. Elasticsearch would auto-create it with a ...`

[`require_existing_index`](parameters.md#require_existing_index) is on (the connector default) and the index isn't
there. Create the index with its mapping, or check `es_index_name` for a typo.

### No config for the job

`no pipeline definition found for '...' (.yml/.yaml) in ...`

The job's config file is missing from the deployed files: usually a config was deleted or renamed without
regenerating and redeploying.

## Writes fail or are slow

### Timeouts

`EsWriteError: ... ConnectionTimeout ... The write operation timed out`

A bulk request took longer than [`request_timeout`](parameters.md#request_timeout). The request never
returned per-document results, so its documents are counted as rejected (one sample with `_id: None`).
Find out why before changing anything: an overloaded ES host needs less pressure (lower
`write_concurrency` or the partition count), and requests that are too heavy need a smaller
[`chunk_size`](parameters.md#chunk_size). Raise `request_timeout` only when the host is healthy and
requests legitimately take longer. With
[`retry_transport_timeout`](parameters.md#retry_transport_timeout) on (recommended), `bulk_stats` shows
how many sends timed out. See [Throughput Tuning](throughput.md#timeouts-and-retries).

### 429 rejections

`es_rejected_execution_exception`, or `rejected(429=...)` in the `BULK_STATS` output

ES's write queue is full. [`max_retries_per_doc`](parameters.md#max_retries_per_doc) retries those
documents with backoff, which absorbs brief bursts. If 429s are steady, lower the pressure:
`write_concurrency`, the partition count, or cores ([Reading the signals](#reading-the-signals)).

### Slow writes with no errors

Turn on [`bulk_stats`](parameters.md#bulk_stats), if it isn't already on, and use [Reading the signals](#reading-the-signals)
below to tell whether ES or the client side is the limit.

### Reading the signals

Turn on [`bulk_stats`](parameters.md#bulk_stats), if it isn't already on, to see what the write is doing (what each part of the
output means is in [Logging and Monitoring](monitoring.md#reading-the-bulk_stats-output)). These
signals tell you which side of the balance a feed is on; the levers are explained in
[Throughput Tuning](throughput.md#the-levers):

| You see | It means | Try |
|---|---|---|
| 429 rejections (`rejected(429=...)`), or `took_ms` rising run over run | ES is under too much pressure | lower `write_concurrency` or the partition count |
| `rtt_ms` far above `took_ms`, no 429s, executors idle | the write is waiting on round trips | raise `write_concurrency`, or add partitions or cores |
| `gil_wait_ms_total` a large share of `wall_ms` on the `tail:` line, with `rtt_ms` rising while `took_ms` doesn't on the `overall` line | a partition's threads are waiting for the GIL, not for ES ([why](throughput.md#python-threads-and-the-gil)) | lower `write_concurrency` and get the parallelism from more cores (with enough partitions to use them) instead |
| a high `wall_max/median` or `stragglers>2x` in the `tail:` line | uneven partitions; the slowest sets the pace | `write_repartition` for evenly sized partitions, or a smaller `max_partition_bytes` |
| a high `docs_max/median` in the `tail:` line | rows are unevenly spread across partitions | `write_repartition` |
| `docs/send` well below `chunk_size` | requests are smaller than you asked for | `chunk_size` is above the Arrow batch size or some other limit, so raising it does nothing; look at the other levers instead |
| `timeout_sends` climbing (with `retry_transport_timeout` on) | requests are timing out and being retried | find out whether ES is overloaded (lower the pressure) or requests are too big (smaller `chunk_size`); raise `request_timeout` only for a healthy host |

## Falling behind

A feed that runs without errors can still fall behind: data reaches the index later and later, because
the feed sends less per interval than the source receives.

### How to tell

- **Streaming, with the monitoring log on:** the `batch_summary` rows say whether each batch was caught
  up or behind (`files_outstanding`, `source_latest_version`, `source_end_version`). A run of behind
  batches, or `files_outstanding` growing batch over batch, is a feed falling behind
  ([the query](monitoring.md#caught-up-or-behind)).
- **Scheduled feeds:** runs that take longer than the schedule interval, so the next tick is skipped
  (`MAX_CONCURRENT_RUNS_EXCEEDED`; see [Skipped](#skipped)).
- **Either mode:** the newest documents in the index are older than the newest rows in the source.

### Why, and what to do

- **The write is slower than the arrivals.** Measure the arrival rate and set a target
  ([Throughput Tuning: know the source](throughput.md#before-you-tune-know-the-source)), then tune the
  write until it meets the target with headroom.
- **The micro-batch caps are below the arrival rate.** If `max_files_per_trigger` or
  `max_bytes_per_trigger` lets each batch read less than arrives between batches, the backlog grows
  however fast each batch is. Raise the caps ([Streaming micro-batch size](throughput.md#streaming-micro-batch-size)).
- **Elasticsearch is the limit.** 429s or rising `took_ms` mean more client-side parallelism won't help:
  see [Reading the signals](#reading-the-signals).
- **A spike, not a trend.** A source that lands a large batch hourly or daily can fall behind during the
  spike and catch up after. That is expected if it catches up before the next one; size for the spike
  if it doesn't.
- **A batch feed re-exports too much.** A batch run re-sends the whole view each time, so its run time
  grows with the table. Move a large, append-only feed to streaming, or narrow it with
  `filter_condition`.

## Data issues

### Fewer documents than source rows

No error. Rows that share an [`es_id_field`](parameters.md#es_id_field) value become one document. Check
the column is unique across the view's output. (A `filter_condition` also removes rows by design.)

### Duplicate documents

No error. With no `es_id_field`, ES assigns a new random id every time a row is sent, and a retried batch
or a restarted stream sends some rows again. Set `es_id_field` to a unique column
([why](technical-details.md#delivery-is-at-least-once)). Grouped feeds writing one index with overlapping
rows can also duplicate ([Job groups](feed-configuration.md#job-groups)).

### A stream writes nothing after the index was rebuilt

The feed runs and reports 0 rows. Its checkpoint still records everything already sent, so it only sends
new commits. To re-send the table, [reset the checkpoint](maintenance-jobs.md#resetting-a-checkpoint) and run
with `streaming_start=full`, or run the feed once in batch mode.

### A streaming_start change did nothing

[`streaming_start`](parameters.md#streaming_start) applies only to a stream's first run. Once a checkpoint
exists, it decides the position. [Reset the checkpoint](maintenance-jobs.md#resetting-a-checkpoint) to start
over from the new setting.

### Updates or deletes on the source are missing

Streaming feeds are append-only: `UPDATE`, `DELETE` and `MERGE` commits on the source are skipped
([Batch or streaming](feed-configuration.md#batch-or-streaming)). Re-send the affected rows with a batch
run. A batch run overwrites documents by `_id`; it does not remove documents for deleted rows.

## Streaming failures

### An added source column

`DELTA_SCHEMA_CHANGED_WITH_VERSION` ("Please try restarting the query")

Expected once per added column: the task retry restarts the stream and it continues. A view that names
its columns sends the new column only once you add it to the view and run `deploy_views`; a view that
uses `SELECT *` sends it from the restarted stream on.

<a id="a-renamed-or-dropped-source-column"></a>

### A renamed or dropped source column

`DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG`

Every attempt fails with this, even when the view doesn't use the column, so the feed sends nothing
until someone acts. Retries never clear it ([why](technical-details.md#schema-changes-in-a-stream)).
What it looks like on a continuous feed:

- **A standalone feed** fails four attempts about a minute apart, the run ends ("The continuous job run
  was cancelled because a task reached its retry limit"), and the trigger starts a new run about a minute
  later on a new cluster. That repeats indefinitely.
- **A job group member** is retried in place with a growing wait (about one minute, then two), and the
  run stays `RUNNING` and is never cancelled. The other members keep sending, so only that task's
  attempt number and its failure emails show the problem
  ([Spotting a stuck feed](monitoring.md#spotting-a-stuck-feed)).

To recover, reset the feed's checkpoint and [backfill](backfill.md) the rows it missed:

1. **Confirm the change.** `DESCRIBE HISTORY <source table>` shows a `RENAME COLUMN` or `DROP COLUMNS`
   commit. Note its version and time.
2. **Note where the feed stopped**, the same way as step 2 of
   [renaming a feed](maintenance-jobs.md#renaming-a-feed): the `start_ts` of its last successful
   `batch_end` row in the monitoring log, and that batch's `batch_summary` `source_end_version`. Without
   the log, go by the commit time from step 1 and widen the backfill window in step 7.
3. **Update the view** if it uses the renamed or dropped column. Deploy (the stream reads the view's
   `.sql` from the deployed files) and run `deploy_views` (batch runs, including the backfill, read the
   view itself).
4. **Stop the feed.** Set `pause_status: PAUSED` in its config, generate and deploy, cancel the running
   run, and check that it has stopped ([Pausing triggers](feed-configuration.md#pausing-triggers)). In a
   job group this stops every member; the others resume from their own checkpoints afterwards, with
   nothing lost.
5. **Clear the checkpoint** of the failing feed only:
   `databricks bundle run checkpoint_clear -t <target> --params config_name=<config_name>`.
6. **Restart.** Choose how the new stream starts, put `pause_status` back, generate and deploy:
   - **Re-send the whole table**, if that volume is acceptable: set `streaming_start: full` in the config
     ([Method 1](backfill.md#method-1-streaming-from-the-start-of-the-table-streaming-feeds-only)). Remove
     it again once the first batch has succeeded, so a later reset doesn't re-send the table again. Skip
     step 7.
   - **Backfill only the gap** (`streaming_start: new`): the restarted stream begins at the
     source's current version, so it never sends the rows committed between the point from step 2 and
     the restart, including rows that landed while the feed was paused. Step 7 sends them.
7. **Backfill the gap** once the restarted stream is up, following [How to Backfill](backfill.md) with
   [Option 2](backfill.md#option-2-a-parallel-job-for-the-backfill) and
   [Method 2](backfill.md#method-2-batch-runs-in-sized-pieces): filter on a column that records when each
   row landed, from before the point in step 2 to after the restart.

### A continuous run cancelled at its retry limit

`The continuous job run was cancelled because a task reached its retry limit`

The task retry limit counts every failure in a long-lived run (added columns, transient ES errors). The
continuous trigger starts a fresh run. Look at the earlier failures in the run to find what keeps failing
([Continuous streaming](feed-configuration.md#continuous-streaming)). If the same error ends run after run,
the failure is permanent, for example [a renamed or dropped source column](#a-renamed-or-dropped-source-column).
A job group member that keeps failing usually doesn't end the run at all: see the same section.

## A scheduled job didn't run

### Paused

Schedules and continuous triggers are paused unless the target's
[`schedule_pause_status`](parameters.md#schedule_pause_status) in `databricks.yml` is `UNPAUSED` or the
feed sets its own [`pause_status`](parameters.md#pause_status) to `UNPAUSED`. Unpause it for the target
or the feed and redeploy, or unpause the job in the UI
([Pausing triggers](feed-configuration.md#pausing-triggers)).

### Skipped

`MAX_CONCURRENT_RUNS_EXCEEDED`

The previous run was still going, and overlapping runs are skipped rather than queued. No work is lost:
the next run catches up. If it happens often, the feed takes longer than its schedule interval: lengthen
the interval or make the feed faster ([Overlapping runs skipped](feed-configuration.md#overlapping-runs-skipped)).

## A committed value seems ignored

You changed a value in `databricks.yml`, deployed, and the job still uses the old one. Something with
higher precedence is setting it ([precedence](parameters.md#bundle-variables)):

- a `.databricks/bundle/<target>/variable-overrides.json` file (git-ignored, so easy to forget),
- an exported `BUNDLE_VAR_<name>` environment variable (`env | grep BUNDLE_VAR_`),
- a `--var` on the deploy command.

Also check you redeployed: `--var` on `bundle run` has no effect, and values come from the last deploy.
For a feed config change, regenerate before deploying. And for a continuous feed, check the job has been
restarted since the deploy: its running run keeps the settings it started with
([Continuous jobs need a restart](deployment.md#continuous-jobs-need-a-restart)).

<a id="gaps-in-the-monitoring-log"></a>

## Gaps in the monitoring log

A missing end row first means the batch or run may still be in progress: `batch_end` is written when the
batch's write finishes, and `run_end` when the run ends (never, for a continuous run that is still
going). `batch_summary` is written after the batch is over, so it can land shortly after `batch_end`.
Check the run's state in the Jobs UI first. Once the batch or run is over:

- **`batch_start` with no `batch_end`:** the batch never finished. The task was killed or the driver died
  mid-write.
- **`run_start` with no `run_end`:** the run was killed or cancelled. A cancel interrupts the notebook
  before it can write `run_end`.
- **A second `batch_start` for the same `batch_id`:** a retry, under the retry's own `task_run_id`.

See [Logging and Monitoring](monitoring.md#monitoring-log) for the row model and queries.

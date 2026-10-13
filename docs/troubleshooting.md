# Troubleshooting

What to do when a feed fails or misbehaves, organized by what you see. For operators. Each entry gives the
symptom, the error text where there is one (`...` marks the parts that vary), the cause, and the fix.

## First steps

1. **Read the failed run's output.** Open the run in the Jobs UI. The framework fails closed with a
   specific message: almost every failure names the setting or file at fault.
2. **Check the monitoring log**, if it's on: the run's `run_end` and `batch_end` rows carry the error type
   and message, and a missing end row says where the run stopped ([Logging and Monitoring](monitoring.md#monitoring-log)).
3. **For slow or failing writes, check the ES host** with
   [`es_diagnostics`](maintenance-jobs.md#diagnosing-an-elasticsearch-host).

## Symptom index

| Where it fails | Entries |
|---|---|
| [Generating jobs](#generation-fails) | unknown key, undeclared host config, `es_host_configs_json`, cluster configs, tags, job groups, old layout, `--check` |
| [Deploying](#deploy-fails) | cluster policy, cluster id, cluster log path, policy conflicts |
| [Starting a run](#run-fails-at-start) | wheel, host config, `${environment}`, checkpoint path, TLS settings, monitoring table, missing index |
| [Writing](#writes-fail-or-are-slow) | timeouts, 429s, slow ES |
| [The data](#data-looks-wrong) | fewer documents, duplicates, a stream that writes nothing, source updates missing |
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

Rename the config file. The name becomes the job's resource key.

### Old layout still present

`the old layout's root resources/ folder (or a file of that name) still exists; delete it ...`, or
`include (databricks.yml) must list ...`

The repo was upgraded from an earlier build but not finished. Follow
[Upgrading from earlier builds](framework-maintenance.md#upgrading-from-earlier-builds).

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

[`require_existing_index`](parameters.md#require_existing_index) is on (the default) and the index isn't
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
Lower [`chunk_size`](parameters.md#chunk_size), raise `request_timeout`, and consider
[`retry_transport_timeout`](parameters.md#retry_transport_timeout) so retries are visible. If it keeps
happening, ES is likely overloaded: see [Throughput Tuning](throughput.md#timeouts-and-retries) and run
`es_diagnostics`.

### 429 rejections

`es_rejected_execution_exception`, or `rejected(429=...)` in the `BULK_STATS` output

ES's write queue is full. [`max_retries_per_doc`](parameters.md#max_retries_per_doc) retries those
documents with backoff, which absorbs brief bursts. If 429s are steady, lower the pressure:
`write_concurrency`, the partition count, or cores ([Throughput Tuning](throughput.md#reading-the-signals)).

### Slow writes with no errors

Turn on [`bulk_stats`](parameters.md#bulk_stats) and use
[Reading the signals](throughput.md#reading-the-signals) to tell whether ES or the client side is the
limit. `es_diagnostics` reports the host's state; its verdicts are explained in
[Maintenance Jobs](maintenance-jobs.md#diagnosing-an-elasticsearch-host).

## Data looks wrong

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

Expected once per added column: the task retry restarts the stream and it continues. The new column
reaches ES only once you add it to the view and run `deploy_views`.

### A renamed or dropped source column

`DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG`

This fails on every attempt and needs an operator: the stream can't continue across that change
([why](technical-details.md#schema-changes-in-a-stream)). Update the view for the new schema, then decide
how the stream should resume, for example by [resetting its checkpoint](maintenance-jobs.md#resetting-a-checkpoint)
and choosing `streaming_start`.

### A continuous run cancelled at its retry limit

`The continuous job run was cancelled because a task reached its retry limit`

The task retry limit counts every failure in a long-lived run (added columns, transient ES errors). The
continuous trigger starts a fresh run. Look at the earlier failures in the run to find what keeps failing
([Continuous streaming](feed-configuration.md#continuous-streaming)).

## A scheduled job didn't run

### Paused

Schedules and continuous triggers are paused unless [`schedule_pause_status`](parameters.md#schedule_pause_status)
is `UNPAUSED` (only `prd` sets it) or the feed sets its own
[`pause_status`](parameters.md#pause_status). Unpause it for the target or the feed and redeploy, or
unpause the job in the UI.

### Skipped

`MAX_CONCURRENT_RUNS_EXCEEDED`

The previous run was still going, and overlapping runs are skipped rather than queued. No work is lost:
the next run catches up. If it happens often, the feed takes longer than its schedule interval: lengthen
the interval or make the feed faster ([Scheduling](feed-configuration.md#scheduling)).

## A committed value seems ignored

You changed a value in `databricks.yml`, deployed, and the job still uses the old one. Something with
higher precedence is setting it ([precedence](parameters.md#bundle-variables)):

- a `.databricks/bundle/<target>/variable-overrides.json` file (git-ignored, so easy to forget),
- an exported `BUNDLE_VAR_<name>` environment variable (`env | grep BUNDLE_VAR_`),
- a `--var` on the deploy command.

Also check you redeployed: `--var` on `bundle run` has no effect, and values come from the last deploy.
For a feed config change, regenerate before deploying.

<a id="gaps-in-the-monitoring-log"></a>

## Gaps in the monitoring log

- **`batch_start` with no `batch_end`:** the batch never finished. The task was killed or the driver died
  mid-write.
- **`run_start` with no `run_end`:** the run was killed or cancelled. A cancel interrupts the notebook
  before it can write `run_end`.
- **A second `batch_start` for the same `batch_id`:** a retry, under the retry's own `task_run_id`.

See [Logging and Monitoring](monitoring.md#monitoring-log) for the row model and queries.

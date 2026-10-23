# Technical Details

This document explains the rationale behind the framework's design decisions, edge-case behaviors, and the live evidence supporting them. It is a reference for "why" the framework is built as it is and how to interpret unusual scenarios. It is advanced material for a deeper understanding, and most day-to-day operation never needs it. Readers should be familiar with [Feed Configuration Guide](feed-configuration.md) and [Logging and Monitoring](monitoring.md).

## Contents

- [Feed behavior](#feed-behavior)
  - [Source primary key](#source-primary-key)
  - [Delivery is at least once](#delivery-is-at-least-once)
  - [Streaming runs the view per micro-batch](#streaming-view-per-micro-batch)
  - [Where a new stream starts](#inclusive-start-version)
  - [Schema changes in a stream](#schema-changes-in-a-stream)
- [Scheduling and recovery](#scheduling-and-recovery)
  - [Overlapping runs are skipped, not queued](#queue-disabled)
  - [Continuous jobs pin task_retry_mode](#task-retry-mode)
  - [Continuous needs classic compute](#serverless-and-continuous)
- [Streaming mechanics](#streaming-mechanics)
  - [Waiting on a stream](#stream-wait)
- [Operations and monitoring](#operations-and-monitoring)
  - [Checkpoint clear runs concurrently](#checkpoint-clear-concurrency)
  - [Fail-closed log trade-off](#fail-closed-log-trade-off)
  - [Caught up or behind: offset semantics](#caught-up-or-behind-offset-semantics)
  - [Cost of the source version read](#source-version-read-cost)
  - [Diagnostics picks a host only by name](#diagnostics-host-by-name-security)
  - [Why BULK_STATS lines arrive late](#bulk-stats-relay-timing)
  - [Monitoring table layout](#liquid-clustering)
  - [Why cluster log delivery is opt-in](#cluster-log-delivery-opt-in)
- [Writing to Elasticsearch](#writing-to-elasticsearch)
  - [Why retry_transport_timeout exists](#retry-transport-timeout)
- [Generation and deployment](#generation-and-deployment)
  - [Generation reads only top-level defaults](#generation-reads-only-top-level-defaults)
  - [op_type create miscount](#op_type-create-miscount)
  - [Why the generated tag key set is fixed at generation](#global-job-tags-key-set)
  - [The es_index_list tag](#es-index-list-tag)
- [Permissions and tooling](#permissions-and-tooling)
  - [All users can run every job](#can-manage-run)
  - [Why pyyaml is pinned](#pyyaml-pin)

## Feed behavior

### Source primary key

The `primary_key` field in a feed's `source` block is **informational only**: no code reads it at runtime. Every streaming feed assumes an append-only stream (updates and deletes are not processed), so there is nothing to deduplicate against a key. It is recorded so a config documents the source's real unique-row column. If the repository ever gains full CDC (change data capture) support, `primary_key` would become **required** for any feed that must apply updates or deletes; until then you can set it or omit it freely.

### Delivery is at least once

Each export attempt is at-least-once: a retried batch or restarted stream can resend the same source rows. This is fundamental to distributed fault recovery, and the write is either idempotent (via `es_id_field` upserts) or deduplicates benignly (via `op_type: create`).

When `es_id_field` is omitted, the feed passes no id_field to the connector, so Elasticsearch assigns a random `_id` to every document. This is zero-config, but it is **not** idempotent: because both modes are at-least-once, the same source rows can be written again as **new** documents, leaving **duplicates** in the index. This is especially pronounced for `pipeline_mode: streaming`, where restarts and micro-batch retries are routine rather than exceptional, so an omitted `es_id_field` will typically accumulate duplicates over the life of the stream. Omit `es_id_field` only when duplicates are acceptable (or the source guarantees no replay); set it whenever you need a stable 1:1 row-to-document mapping.

Conversely, if `es_id_field` *is* set but two input rows share its value, they collapse to a single document (they share one `_id`), so Elasticsearch ends up with *fewer* documents than rows sent. Which of the duplicates survives is **not** guaranteed: the write is partitioned and runs in parallel, so there is no defined "last" row. Ensure the column is unique across the input when you need a 1:1 mapping.

<a id="streaming-view-per-micro-batch"></a>
### Streaming runs the view per micro-batch

The view logic runs over each micro-batch, not by reading the deployed view. The framework takes the view's `SELECT` and runs it with the micro-batch bound in place of the source table, so the exact same projection, joins, and hints the view defines apply, but only to batch-sized data (never a join back to the full view). Reference (join) tables are read as their real tables.

This design choice is why streaming supports only **row-wise** views: projection, filters, scalar expressions, and 1:1 reference joins, where each output row depends on a single source row. A view that aggregates **across** source rows (`GROUP BY`, `DISTINCT`, window/`OVER`, `PIVOT`) would be computed per batch, not over the whole stream, so its streamed results would silently differ from batch mode. This is a limitation of streaming mode; use `batch` mode for aggregating views.

<a id="inclusive-start-version"></a>
### Where a new stream starts

When a stream starts for the first time, `streaming_start` controls whether it begins with `new` (the source's current Delta version, so existing history is not re-exported) or `full` (backfill the whole existing table). This choice is honored **only on the first run**, before a checkpoint exists: once a stream has a checkpoint, that checkpoint is the position of record and `streaming_start` is ignored.

A `new` start reads the source's current version with a separate no-op stream first: it starts at that version (Delta's `startingVersion` is inclusive), reads through it without sending anything to Elasticsearch, and commits the checkpoint after it. The real stream then resumes from that checkpoint, so it sends only commits made after the start. On a large table this no-op pass is also much cheaper than having the real stream position itself.

Two fallbacks pin `startingVersion` on the real stream instead. If the no-op pass commits nothing (the current version has no data to read), nothing is re-sent either. If the runner can't tell whether a checkpoint already exists (a listing error), it starts the real stream at the current version, which is safe on a resume and on a true first run but, on a first run, sends that one commit's rows once. With `es_id_field` set that is an idempotent overwrite. The logic is in `framework/notebooks/run_index_pipeline.py`, before the stream starts.

### Schema changes in a stream

An **added** source column fails the stream once with `DELTA_SCHEMA_CHANGED_WITH_VERSION` ("Please try restarting the query"); the task retry restarts it with the new schema and it continues (verified live). Whether the new column reaches Elasticsearch depends on the view: a view that names its columns sends it only once the view is changed to include it, while a view that uses `SELECT *` sends it from the restarted stream on, because the stream runs the view's SQL against each micro-batch.

A **renamed or dropped** column fails on every attempt with `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG` and needs an operator (deliberately: no `schemaTrackingLocation` is set).

Note the task retry limit is **per run and cumulative**: a long-lived run's earlier failures (added columns, transient Elasticsearch errors) count toward it. A single-task continuous job that hits the limit is cancelled and restarted by the trigger ("The continuous job run was cancelled because a task reached its retry limit"); in a job group, tasks were seen retried past that limit while siblings ran.

## Scheduling and recovery

<a id="queue-disabled"></a>
### Overlapping runs are skipped, not queued

Every job (except `_checkpoint clear`) sets `max_concurrent_runs: 1` **and** `queue: {enabled: false}`, so a scheduled tick that comes while the previous run is still going is **skipped** (`MAX_CONCURRENT_RUNS_EXCEEDED`) rather than overlapping or piling up.

The `queue` disable is what makes overlapping runs skip: the Jobs API defaults `queue.enabled` to `true`, which would otherwise *queue* the overlapping tick (waiting up to 48 hours for the slot) instead of dropping it. Skipping loses no work: a streaming job's next tick drains the full backlog since its last checkpoint, and a batch job's next tick re-exports the whole view.

<a id="task-retry-mode"></a>
### Continuous jobs pin task_retry_mode

A continuous job's per-task recovery is governed by the continuous trigger's `task_retry_mode`, which the generator pins to **`ON_FAILURE`** for every continuous job. This is deliberate, and recovery depends on it: the field's API/bundle default when omitted is **`NEVER`** (a failed task is never retried), even though the Jobs UI defaults it to `ON_FAILURE`.

Under `ON_FAILURE`:

- If a task's stream fails, Databricks **retries that task in place** (with exponential backoff) as long as at least one other task in the run is still on its first attempt, so one stream can recover without disturbing the others.
- When that no longer holds, or the retry limit is reached, the **whole run is cancelled and a fresh one started** (the job-level restart, itself exponential-backoff throttled).

This matters most for a **job group** of continuous streams (multiple tasks in one run): under the `NEVER` default a failed task would sit `FAILED` while the sibling streams keep running, so the run never reaches a terminal state and the continuous trigger never restarts it either. The failed task would never recover. `ON_FAILURE` gives the intended "retry the task, else restart the job" behavior. A single-task continuous job recovers under either value (its failure ends the run, which the trigger restarts), but it is pinned the same way for consistency.

<a id="serverless-and-continuous"></a>
### Continuous needs classic compute

Serverless notebooks/jobs support only `Trigger.availableNow`, not the `ProcessingTime` trigger an always-on stream needs. So `continuous` requires `compute.type` of `job_cluster` or `existing_cluster`; `continuous` on serverless, `continuous` with `pipeline_mode: batch`, and `continuous` together with `schedule` are each rejected at config load, before deploy.

## Streaming mechanics

<a id="stream-wait"></a>
### Waiting on a stream

**A failed stream always fails its task.** When a blocking `awaitTermination()` was reproduced (FEVM, DBR 17.3 classic STANDARD, a two-task continuous group) **never returning** after its query failed, the Spark UI showed the query FAILED, yet the task sat `RUNNING` with no work for over an hour, so the job never saw a failure and never retried it, while the sibling task ran on.

The runner therefore waits on a stream in short slices (see `framework/pipeline_lib/stream_wait.py`) and, between them, asks the query directly whether it is still active. A stream that has ended is noticed within about 10 seconds even if the wait call misses it, and its error fails the task, so the retry above applies. The implementation waits with a timeout (returns as soon as the query ends, not a sleep) and periodically checks `query.isActive` and `query.exception()` directly; either one can detect the end, and both checks must fail consistently before giving up (fail-closed after 30 consecutive failed checks, about 5 minutes).

## Operations and monitoring

<a id="checkpoint-clear-concurrency"></a>
### Checkpoint clear runs concurrently

Unlike every other job, `_checkpoint clear` allows up to **20 concurrent runs**, so you can reset several feeds' checkpoints at once (one run per `config_name`). Two runs for the same config are harmless (the second finds nothing to delete). Concurrency does not make it safe to clear a checkpoint while that feed's stream is running: stop the stream first.

### Fail-closed log trade-off

With `monitoring_log_enabled` on, the log is required: `monitoring_log_table` unset or malformed, the table missing or not writable, or **any** failed append fails the task, so support receives the failure notification and no further data is sent without a log record. Because `batch_start` is written before a batch's data is sent, a log outage stops the export before the next batch reaches Elasticsearch.

The trade-off: a log write that fails **after** a batch's data reached Elasticsearch (its `batch_end`) fails the micro-batch before the checkpoint advances, so the task's retry re-sends that batch. With `es_id_field` set that re-send is an idempotent overwrite; without it, Elasticsearch assigns new ids and the batch's rows are **duplicated**, the same as for any other mid-batch failure. With the log **off**, nothing is written and nothing about the export changes.

A failure while recording a failure never hides the original error (the log fault is attached to it as a note).

### Caught up or behind: offset semantics

`files_outstanding` / `bytes_outstanding` at zero means caught up, but a non-zero backlog is ambiguous: Spark measures it **after** the batch, against the table as it is then, so files that landed while the batch ran count too. Spark's own `latestOffset` would settle it, but the Delta source always reports it as null (seen live on DBR 17.3).

Each streaming batch records the source's newest commit made at least `log_monitor_source_lookback_seconds` (default 60) before its start (`source_latest_version`, from the newest `log_monitor_source_history_rows` history rows, default 100), and `batch_summary` carries both that and where the batch ended (`source_end_version`, `source_end_index`). A batch was **behind** (it left files that were already there for the next batch) when

```
source_latest_version >= source_end_version AND files_outstanding > 0
```

and otherwise it was caught up (anything outstanding arrived while it ran).

**How to read the end offset:** with `source_end_index = -1` every source version **below** `source_end_version` was sent (five commits v1 to v5 end at version 6, index -1); with an index of 0 or more the batch stopped inside version `source_end_version` after that file (a cap that split one large commit), which the `>=` also counts as behind. A fully sent version is always reported as the next version with index -1, including an initial snapshot (seen in every case on DBR 17.3; Delta never writes its internal end-of-version index into an offset). Commits the reader sends nothing for (table maintenance such as OPTIMIZE, VACUUM and property changes, and the UPDATE / DELETE commits `skipChangeCommits` skips) do not read as behind: the Delta source moves its end offset past them, before or during a batch.

**Why the look-back:** Spark plans a batch (fixing its end offset) before the batch starts running and reads the history. A commit landing in that gap is newer than anything the batch could send, so with no look-back it counts as available: if it carries data, or carries none while other data arrives during the batch (`files_outstanding > 0`), a caught-up batch reads as behind. The gap is the offset log write, the call into `foreachBatch`, and the history read itself, usually one to three seconds. The look-back skips every commit newer than the cutoff, so commits in the gap don't count.

**Where it misreads:**

- **Toward caught up:** data committed inside the look-back just before Spark planned the batch doesn't count. If a cap left only such data behind, the batch reads as caught up; the next batch reads as behind if the feed stays over capacity. Keep the look-back short next to how far behind matters to you.
- **Toward behind, when the source is busier than the fetch:** if more than `log_monitor_source_history_rows` commits land within the look-back, none of the fetched rows is old enough and the oldest one is used. That commit can be newer than the planning point, so a caught-up batch can read as behind. Raise the row count if this happens.
- **Either way, by clock skew:** the cutoff compares the cluster's clock with the commit times the source's writers recorded, so skew between them shifts the effective look-back by the skew (normally milliseconds, which matters only for a commit right at the cutoff).
- **Toward behind, for a batch replayed after a restart:** Spark re-runs the last planned, uncommitted batch with its original end offset, but the read happens at replay time, so anything committed during downtime longer than the look-back counts. A replay shows as a second `batch_start` for the same `batch_id` (under the new attempt's `task_run_id`). A NULL `source_latest_version` (the read failed, or the relay to `batch_summary` was lost; `batch_start` still has it) leaves the batch unclassified.

<a id="source-version-read-cost"></a>
### Cost of the source version read

Two steps per batch, both inside the batch before its checkpoint commits, add about 3 seconds per batch at the median with the default 100 history rows (live probes on DBR 17.3, standard-mode job cluster):

- The `DESCRIBE HISTORY <source> LIMIT <log_monitor_source_history_rows>` read before the data is sent. Its median grows with the row count (live probe, source with 150 commits, 12 reads each):

  | Rows | Serverless | DBR 17.3 job cluster |
  |---|---|---|
  | 1 | 0.60 s | 0.75 s |
  | 10 | 0.67 s | 0.87 s |
  | 50 | 0.94 s | 1.39 s |
  | 100 (default) | 1.32 s | 2.17 s |

  The first call on a cluster is slower (about 3.7 seconds at one row).
- The relay write that hands the value to `batch_summary`, one small text write to the checkpoint Volume after `batch_end` (median 0.9 seconds, p90 1.1 seconds, first write about 10 seconds).

Both run only for streaming with the log on. There is no knob to skip them with the log on; one could be added by gating `read_source_latest` in `run_index_pipeline.py` (the code already handles a batch without it, and then writes no relay).

<a id="diagnostics-host-by-name-security"></a>
### Diagnostics picks a host only by name

The diagnostics job connects to the host config named by the `es_host_config` parameter (default: the bundle's `default_es_host_config`). The host is picked **only by name**: the job takes no run-time URL or secret, so someone who can run it can't point it, and the `api_key`, at another endpoint. This design choice trades away the flexibility of a run-time URL for the security that the diagnostics endpoint and its credentials are vetted at deployment and cannot be redirected at runtime.

<a id="bulk-stats-relay-timing"></a>
### Why BULK_STATS lines arrive late

Batch runs emit bulk statistics under the `BULK_STATS` tag with an `overall` rollup (docs/send, send-weighted mean/max round-trip `rtt_ms` and Elasticsearch-reported `took_ms`, plus `cpu_ms` and `gil_wait_ms` totals), one line per write partition with real p50/p95/max, and a `tail:` summary naming the slowest partition. Streaming runs emit the compact `overall` line plus that `tail:` summary per micro-batch.

Every line carries a `ts=<UTC>` wall-clock token so a batch can be placed in real time. This matters because a micro-batch's write runs server-side (its log output does not reach the notebook cell), so each batch's line is written to a small per-batch file and re-emitted by the notebook's progress recorder (best-effort), appearing alongside the `STREAM_PROGRESS` line (in the notebook cell for an interactive run, in the driver log for a job run) **LATER** than the batch actually ran, so the ambient log timestamp is the relay time and the embedded `ts=` is the true batch time.

The `rtt_ms - took_ms` is the network/queue overhead and `docs/send` is the real docs-per-bulk, so this is the tool for diagnosing whether more `write_concurrency` or cores would help; `gil_wait_ms` (from the connector's GIL-acquisition probe, on connectors that emit it, else `n/a`) separates a slow round trip that is a genuine socket/Elasticsearch wait from one inflated by GIL starvation under a high `write_concurrency` ([Python threads and the GIL](throughput.md#python-threads-and-the-gil)).

<a id="liquid-clustering"></a>
### Monitoring table layout

The monitoring table is **liquid-clustered** on `(config_name, event_ts)` (the columns monitoring queries filter by), so data skipping holds up as it grows. The `_log table create` job also sets `delta.autoOptimize.optimizeWrite` and `autoCompact` to keep the table compact. `VARIANT` and liquid clustering require DBR 15.3+ or recent serverless (every target here qualifies).

<a id="cluster-log-delivery-opt-in"></a>
### Why cluster log delivery is opt-in

Cluster log delivery (`cluster_log_conf`) is deliberately not injected into every job cluster automatically: the Jobs API rejects an empty `cluster_log_conf` (`400 Invalid cluster log storage info`, seen live), and a bundle cannot drop a nested block at deploy, so an opted-in config whose target leaves the path empty **fails that deploy** (loudly, nothing is half-applied).

**Classic job clusters only.** Serverless compute does not support cluster log delivery, so serverless feeds and the serverless maintenance jobs are unaffected; their output stays in the run log. `existing_cluster` feeds use whatever the existing cluster is configured with.

## Writing to Elasticsearch

<a id="retry-transport-timeout"></a>
### Why retry_transport_timeout exists

When a bulk request takes longer than `request_timeout`, the outcome is unknown: ES may or may not have applied it. By default the Elasticsearch Python client handles that inside its transport layer. It re-sends the same request right away, up to `transport_max_retries` times, each attempt waiting a full `request_timeout`, and the connector sees nothing until the last attempt fails. A run of timeouts therefore shows up only as a slow batch, with no record of how many sends timed out or how long the retries took.

With `retry_transport_timeout` on, the connector builds its client with the transport's timeout retry turned off and does the retry itself: on a timeout only, it waits with exponential backoff (2 seconds, doubling, at most 30) and re-sends the chunk, within the same `transport_max_retries` budget. Every attempt, failed ones included, is recorded in `bulk_stats` (`timeout_sends`, `timeout_wait_ms`), so the cost of timeouts is visible, and the backoff gives a busy ES host time to recover instead of receiving the same request again at once. Connection resets and 429 or 503 responses are still retried by the transport, as before. A re-sent chunk is applied again, which is an overwrite with `es_id_field` set and a duplicate without it, the same as any other retry.

The logic is in the connector (`retry_transport_timeout` in `EsWriteConfig`, and the bulk sender in `bulk.py`).

## Generation and deployment

### Generation reads only top-level defaults

The generator (`framework/scripts/gen_jobs.py`) runs on your machine before any deploy and reads `databricks.yml` as a plain YAML file. It sees each variable's top-level `default:` and the `targets` blocks, but nothing the bundle resolves at deploy: not `--var`, `BUNDLE_VAR_` environment variables or `variable-overrides.json`. Three values it acts on therefore have to be fixed at the top level: `default_es_host_config` (which host a feed without `es_host_config` writes to), the key set of `global_job_tags`, and the shape of `es_host_configs_json` (checked against the declared host configs). Their per-environment values still vary per target: the host config's own fields, tag values written as `${var...}` references, and the references inside `es_host_configs_json`.

A pipeline referencing a host config that isn't declared in `databricks.yml` fails **at generation** (`framework/scripts/gen_jobs.py`), before deploy. So does a `default_es_host_config` that is missing, names an undeclared host config, or is overridden in a target: both the feeds that omit `es_host_config` and the `es_diagnostics` job default to it, and the generator reads only its top-level `default:`, so it can't vary per target (a target override in `databricks.yml` fails generation).

### op_type create miscount

Under the default fast path, a chunk that mixes a **NEW** `_id` with already-indexed ones can **miscount** when using `op_type: create`: the probe creates the new doc, then the whole-chunk re-ship self-409s it, so it is tallied as `docs_deduped` rather than `written` (the data is still correct: one copy, no overwrite; only the attribution is off). All-new and all-existing chunks count exactly.

Set `bypass_fast_path` to make the counts exact, or accept the possible miscount. The fast path is an optimization that skips per-item decoding on clean chunks; disabling it gives up that throughput optimization in exchange for exact per-item counts on the first try.

<a id="global-job-tags-key-set"></a>
### Why the generated tag key set is fixed at generation

The `global_job_tags` bundle variable's **key set** is baked at generation time for generated jobs. A per-target override that changes the *keys* reaches only the six fixed jobs in `framework/resources/`, exactly like `default_es_host_config`, while per-target **values** still work through `${var...}` references in the tag values themselves.

The reason is when each part is resolved. The generator writes each generated job's `tags:` block as literal keys, read from the top-level `default:` (`framework/scripts/gen_jobs.py`), and appends its own `es_index_list` tag. A value that is a `${var...}` reference is copied in verbatim and resolves per target at deploy, but the keys themselves are fixed text in the generated file by then. The fixed jobs instead reference the whole variable (`tags: ${var.global_job_tags}`), which the bundle resolves per target. So keep the key set at the top level and vary values with references.

<a id="es-index-list-tag"></a>
### The es_index_list tag

In addition to the global tags, the generator adds one generated tag to each generated job: `es_index_list`, a distinct, sorted, **space-separated** list of the `es_index_name`(s) that job writes (one index for a singleton, the distinct set for a job group).

The separator is a **space**, not a comma: the Jobs API restricts a tag value to `^[\d \w+\-=.:/@]*$`, which excludes commas; Elasticsearch index names never contain spaces, so a space is unambiguous.

**Truncation:** Tag value length is bounded by the cloud provider (AWS 255, Azure 256, GCP labels 63) once a tag reaches a cluster. To keep a large job group deployable, the generator **truncates** a long `es_index_list` to a conservative cap (255 characters), keeping whole index names in sorted order and appending a ` ...+N` marker for the count dropped (with a generation warning). The global tags are independent tag values and are never truncated. GCP's 63-character label limit is tighter than the cap, so a GCP deployment with many indices per group may still need fewer. Databricks allows at most 25 tags per job; because the generator adds `es_index_list` to every generated job, at most **24** global tags are allowed (`gen_jobs.py` fails closed on more).

## Permissions and tooling

<a id="can-manage-run"></a>
### All users can run every job

Every job grants `CAN_MANAGE_RUN` to the `users` group, so anyone in the workspace can trigger, stop or repair a run without being the job's owner. A run executes as the job's run-as identity, not as the person who started it, and `CAN_MANAGE_RUN` lets that person change the run's parameters. So run-time parameters are kept narrow where a wider one would hand that identity's reach to whoever starts the run: `es_diagnostics` picks a host only by name ([above](#diagnostics-picks-a-host-only-by-name)), so it can't be pointed, with its api_key, at another endpoint, and `checkpoint_clear` accepts only a feed name, never a path, so it can only delete that feed's checkpoint.

<a id="pyyaml-pin"></a>
### Why pyyaml is pinned

Running the generator needs `pyyaml`, pinned in `requirements.txt` (`pip install -r requirements.txt`). The pin matters because `--check` byte-compares against `yaml.safe_dump` output, whose formatting can drift across pyyaml versions. A version mismatch surfaces as a false "generated files are stale" failure from `--check` even though the YAML is semantically identical.

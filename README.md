# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/). It uses the
[**databricks-es-connector**](https://github.com/tim-clifford/es-databricks-connector) library for the
transfer.

## Overview

Every Elasticsearch index is fed by its own pipeline, and each pipeline is described by two files:

- a view `_pipelines/pipeline_views/<view_name>.sql` defining what gets exported, and
- a config file `_pipelines/pipeline_configs/<config_name>.yml` that points a pipeline at that view and says
  where its view, source table, and any reference (join) tables live.

The `_pipelines/` folder is where you configure your pipelines; everything else in the repo is shared
framework code you don't normally edit.

The bundle deploys:

- **One `deploy_views` job**: creates or replaces one Databricks view per `.sql` file. Each view is a
  `.sql` file in [`_pipelines/pipeline_views/`](_pipelines/pipeline_views/). The job renders the
  catalog/schema parameters and runs every file with `spark.sql`. A view **can be shared** by more than
  one pipeline (e.g. to send different subsets to different indices or hosts; see
  [Sharing a view across pipelines](#sharing-a-view-across-pipelines)) - it is created once, and the
  sharing pipelines must agree on the view definition.
- **One job per index** (`index_pipeline_<config_name>`): all run the same shared notebook
  [`notebooks/run_index_pipeline.py`](notebooks/run_index_pipeline.py) with that index's config. These
  job resources are **generated** by [`scripts/gen_jobs.py`](scripts/gen_jobs.py) from the config
  files (see [Adding a new pipeline for an ES index](#adding-a-new-pipeline-for-an-es-index)).
  Configs can instead be merged into a single multi-task job via [Job groups](#job-groups) (e.g. to
  share one cluster).

## Adding a new pipeline for an ES index

1. Add `_pipelines/pipeline_views/<view_name>.sql`.
2. Add `_pipelines/pipeline_configs/<config_name>.yml` (see the schema under [Configuration](#configuration)).
   The view's filename must match the config's `view.name`.
3. Regenerate the job resources: `python scripts/gen_jobs.py`.
4. Deploy.

The config's filename stem becomes the job's resource key (`index_pipeline_<stem>`), so it must
contain only letters, digits, `_`, and `-` (the generator rejects anything else, e.g. a dotted name).

Step 3 writes one `resources/<config_name>.job.yml` per config, keeping the generated jobs in sync. If you
run it, you're covered. `python scripts/gen_jobs.py --check` is the separate verification that step 3
was actually run: it makes no changes and fails if any generated file is missing, stale, or orphaned
(left behind by a deleted or renamed config). Run it in CI to catch a commit that edited a config but
forgot to regenerate.

## Views

Each `.sql` file in `_pipelines/pipeline_views/` defines one view. The filename matches the view it creates
(`ecs_dns_activity.sql` creates the `ecs_dns_activity` view, feeding the `ecs-dns-activity` ES index;
the view name uses underscores because a Databricks view name can't contain unquoted hyphens). Object
names use `${...}` parameters resolved per-view from the config at deploy time:

| Parameter | Resolves to |
|---|---|
| `${view}` | the fully-qualified view to create: `catalog.schema.name` |
| `${source}` | the fully-qualified source table: `catalog.schema.table` |
| `${ref_<alias>}` | a reference (join) table, aliased: `catalog.schema.table <alias>` |

Each of those is assembled from the config, with any `${environment}` component already folded in
(see [Configuration](#configuration)). An unknown `${...}` parameter in a file is a hard error (fail
closed), so a typo can't create a view pointing at the wrong place. (For the same reason, don't write
a literal `${...}` in a view's SQL comments unless it's one of the parameters above.)

### Reference (join) tables

A view has exactly one source table, but may join **reference tables** (e.g. dimension or lookup
tables). Declare each under `reference_tables` in the config; the key is the join alias you reference
in the SQL as `${ref_<alias>}`:

```sql
SELECT
    base.dsl_id,
    (validation.dsl_id IS NOT NULL) AS validation_row_exists
FROM ${source} base
LEFT JOIN ${ref_validation} ON base.dsl_id = validation.dsl_id
```

The config owns *where* each table is; the SQL owns the join itself (type, `ON` clause, surfaced
columns, and any tuning such as a `/*+ BROADCAST(alias) */` hint, written directly in the SQL).

### Sharing a view across pipelines

More than one pipeline may point at the **same** view (same `view:` block, so the same `.sql`). This is
how you fan one transformed view out to several destinations: give each pipeline its own config with a
different `es_index_name`, `es_host_config`, and/or `filter_condition` (a subset), all reading the one
view. `deploy_views` creates the view **once** and prints a `WARNING` naming the sharing pipelines (so an
*accidental* duplicate view name is still visible).

The sharing configs must agree on the **view definition** - the `view:` target, the `source:`, and every
`reference_tables` entry (i.e. everything that renders into the `CREATE OR REPLACE VIEW` statement). They
may differ **only** on the ES-write side: `es_index_name`, `es_host_config`, `es_id_field`,
`filter_condition`, and `pipeline_mode`. If two sharers would render *different* definitions for one view
name, `deploy_views` **fails that view closed** (you can't deploy two definitions to one object).
`es_id_field` is checked per pipeline: the shared view must contain every sharer's `_id` column.

## Configuration

The **workspace** is not a bundle variable: it comes from your Databricks CLI profile (`-p <profile>`)
or `DATABRICKS_HOST`. The environment-specific connection, path, and policy variables (`environment`,
`wheel_path`, `checkpoint_base_path`, `cluster_policy_id`, `ca_certs`, and the ES host configs) are set **per
target** in `databricks.yml` (`targets.<env>.variables`), shipping **empty** on `main` for you to fill
in for the environments you deploy to, so a routine deploy needs no `--var`. (`schedule_pause_status` is
also per-environment but is the exception: it defaults to `PAUSED` globally and only `prd` overrides it,
and it is `--var`-settable too; see [Scheduling](#scheduling).) The simple per-target string
variables (`environment`, `wheel_path`, `checkpoint_base_path`, `cluster_policy_id`, `ca_certs`, and the
per-target global default for every run-time knob) can still
be overridden at deploy with `--var=<name>=<value>`; the `type: complex` variables (the ES host configs, and any `cluster_config`)
**cannot** be set via `--var` at all (the CLI rejects it: *"setting variables of complex type via --var
flag is not supported"*), so override those through the git-ignored `variable-overrides.json` (see
[Configuring Elasticsearch host connections](#configuring-elasticsearch-host-connections)). Precedence,
highest first: `--var`, then a `BUNDLE_VAR_<name>` environment variable, then the git-ignored
`variable-overrides.json`, then the per-target `variables` value, then the top-level `default`. A stale
`variable-overrides.json` (or an exported `BUNDLE_VAR_<name>`) therefore silently outranks a value you
committed to a target block (this repo's dev workflow keeps an overrides file for exactly these
variables, so if a committed per-target value looks ignored, check for a local overrides file or a stray
`BUNDLE_VAR_` env var). An empty
value fails closed wherever the value is required. The bundle variables are:

| Variable | What it sets |
|---|---|
| `environment` | folded into any config name containing `${environment}` (e.g. `ocsf_${environment}` -> `ocsf_prod`); may be empty when no name uses the token. Set per target |
| `wheel_path` | UC Volume path to the `databricks-es-connector` wheel each **index job** installs (the connector version lives here, in the wheel filename); a global prerequisite, not created by this bundle (see [the connector repo](https://github.com/tim-clifford/es-databricks-connector) for building/uploading it). Set per target (empty on `main`). An index job deployed with an empty `wheel_path` fails closed at run; `deploy_views` doesn't need it |
| `checkpoint_base_path` | UC Volume base path for **streaming** checkpoints; the runner appends `/<config_name>` so each stream gets its own subfolder. Set per target (empty on `main`). Required for a streaming run (fails closed if empty); unused by batch and `deploy_views`. The `dev` target shows how to append `${workspace.current_user.short_name}` to isolate each developer's checkpoints (see [Streaming](#streaming)) |
| `cluster_policy_id` | workspace-specific cluster policy id injected into every job cluster (see [Compute](#compute)). Set per target (empty on `main`); required only when a pipeline uses `job_cluster` compute |
| `ca_certs` | UC Volume path to a CA bundle (PEM) the connector uses to verify the ES server's TLS certificate. One global bundle shared by every host config. Set per target (empty on `main`); empty means fall back to the system CA store. Incompatible with `verify_certs: false` (the connector rejects that combination at run). Per-endpoint CA pinning is not supported (would need `ca_certs` moved onto the `es_host_*` complex variables) |
| `default_es_host_config` | name of the ES **host config** a pipeline writes to when it omits `es_host_config` (out of the box `es_host_primary`); must name one of the `es_host_*` complex variables. Read at **generation** time, so it is a repo-level choice: per-target and `--var` overrides are not seen by the generator (its per-environment values live on the host config itself). See [Configuring Elasticsearch host connections](#configuring-elasticsearch-host-connections) |
| `schedule_pause_status` | target-wide default `PAUSED` or `UNPAUSED` applied to every scheduled **and** continuous job (default `PAUSED`, fail-safe). `dev` and `stg` inherit the paused default so they deploy the trigger without firing it; only `prd` binds `UNPAUSED` to actually run it. A pipeline config's own top-level `pause_status` overrides this for that one job's trigger. Affects jobs that declare a `schedule` or a `continuous` block (see [Scheduling](#scheduling) and [Continuous streaming](#continuous-always-on-streaming)) |
| `support_email` | email recipients (a **list**) notified on any job/task run **failure** in the target. Every job (generated index/group jobs plus the hand-authored `deploy_views`, `checkpoint_clear`, `build_wheel`) emits `email_notifications.on_failure: ${var.support_email}` with `notification_settings` that suppress SKIPPED and CANCELED runs, so the address is paged only on a genuine failure. A `type: complex` LIST var: the empty list `[]` turns notifications **off** (an unambiguous "no recipients", unlike a single empty string). Set per target (empty `[]` on `main`, `dev`, `stg`; set a real address, e.g. `["es-oncall@yourco.com"]`, only in `prd`). Because it is complex it **cannot** be set via `--var`; override `dev` through the git-ignored `.databricks/bundle/<target>/variable-overrides.json` |
| `bulk_stats` | global default for the connector's `bulk_stats` diagnostics (per-partition ES bulk-send stats in the run log). Empty default (off). The generator bakes `${var.bulk_stats}` as the `bulk_stats` job-parameter default for any pipeline that omits it, so setting this per target (or `--var=bulk_stats=true`) turns diagnostics on for a whole environment. A pipeline's own `bulk_stats:` and a per-run `--params bulk_stats=<v>` override it (see [Configuration](#configuration)) |
| `retry_transport_timeout` | global default for the connector's `retry_transport_timeout` reliability toggle: when on, the connector OWNS whole-request timeout retries (re-sends a timed-out bulk with backoff instead of letting the transport re-send it invisibly and then failing the batch). Empty default (off). Threaded exactly like `bulk_stats`: the generator bakes `${var.retry_transport_timeout}` as the job-parameter default for any pipeline that omits it, so setting this per target (or `--var=retry_transport_timeout=true`) turns it on for a whole environment, and a pipeline's own `retry_transport_timeout:` or a per-run `--params retry_transport_timeout=<v>` override it (see [Configuration](#configuration)) |
| `bypass_fast_path` | global default for the connector's `bypass_fast_path` write-path toggle: when on, the connector skips its `filter_path="errors"` probe and classifies every chunk per-item, which makes the `docs_deduped` / `written` counts EXACT for `op_type=create` on chunks mixing new and existing `_id`s (and avoids the auto-id re-ship duplication), at the cost of the fast path's throughput on clean chunks. Empty default (off; the fast path is used). Threaded exactly like `bulk_stats`: the generator bakes `${var.bypass_fast_path}` as the job-parameter default for any pipeline that omits it, so setting this per target (or `--var=bypass_fast_path=true`) turns it on for a whole environment, and a pipeline's own `bypass_fast_path:` or a per-run `--params bypass_fast_path=<v>` override it (see [Configuration](#configuration)) |
| `pipeline_mode`, `filter_condition`, `chunk_size`, `write_concurrency`, `op_type`, `request_timeout`, `transport_max_retries`, `require_existing_index`, `verify_certs`, `streaming_start`, `write_repartition`, `max_partition_bytes`, `max_files_per_trigger`, `max_bytes_per_trigger` | the **remaining run-time knobs**, each threaded exactly like `bulk_stats`: the generator bakes `${var.<name>}` as that knob's job-parameter default for any pipeline that omits it, so **every** run-time knob follows one uniform pattern (this global default < a per-pipeline config value < a per-run `--params <name>=<value>`). All ship at a default that reproduces the prior behavior: `pipeline_mode` **`batch`** and `streaming_start` **`new`** (concrete, since their validators reject `""`), the rest empty (`""` = defer to the connector/Spark/built-in default; `op_type` empty defers to the connector's default write action). Leaving them unset changes nothing; set one per target (or `--var=<name>=<value>`) to move a whole environment. Note: setting a non-empty global for an empty-sentinel knob applies to every omitting pipeline with no per-config opt-out (watch `filter_condition`). Validated at generation and at run (see [Configuration](#configuration)) |

The **Elasticsearch connection** is not a single global setting: it is a named **host config** that each
pipeline selects, with values that differ per environment. See
[Configuring Elasticsearch host connections](#configuring-elasticsearch-host-connections) below.
`wheel_path` ships empty and is baked in at deploy; an index job run with an empty `wheel_path` fails
closed, and `deploy_views` doesn't need it. `checkpoint_base_path` is the same shape (global, per-target,
empty default) but only a **streaming** run requires it; it must be a UC Volume path (serverless
streaming checkpoints can't live on `dbfs:/tmp`).

Everything else is per-pipeline and lives in `_pipelines/pipeline_configs/<config_name>.yml`. Each object is fully
qualified (`catalog`, `schema`, and a name/table). Only `catalog` and `schema` may embed
`${environment}`; the view name and table names are plain identifiers (so a view's name always equals
its `.sql` filename):

```yaml
es_index_name: ecs-dns-activity   # target ES index (hyphens allowed)
es_id_field: dsl_id               # OPTIONAL: view output column passed to the connector as the ES document _id (idempotent upserts). Omit to let ES auto-generate _ids (replays may duplicate; see below)
es_host_config: es_host_primary   # OPTIONAL: which ES host config to write to; declared in databricks.yml (see below). Omit to use the bundle default
pipeline_mode: batch              # OPTIONAL default export mode: batch | streaming. Omit to defer to the global ${var.pipeline_mode} default (batch), or set here; override per run. A continuous pipeline must set streaming explicitly (can't inherit the global)
filter_condition: "action = 'allowed'"  # OPTIONAL default row filter (Spark SQL); omit for no filter
chunk_size: 1000                  # OPTIONAL EsWriteConfig tuning (docs per bulk request); omit for connector default
require_existing_index: true      # OPTIONAL EsWriteConfig tuning (require the index to exist); omit for connector default
verify_certs: true                # OPTIONAL EsWriteConfig tuning (verify the ES TLS cert); omit for connector default
write_concurrency: 4              # OPTIONAL EsWriteConfig tuning (parallel bulk streams per partition); omit for connector default
request_timeout: 120              # OPTIONAL EsWriteConfig tuning (per-request ES client timeout, seconds); omit for connector default. Raise it (with a smaller chunk_size) when a bulk send times out mid-write
transport_max_retries: 5          # OPTIONAL EsWriteConfig tuning (whole-request retries on a transport failure: connection reset/timeout, 429/503 on the bulk call); omit for connector default. 0 disables them
bulk_stats: true                  # OPTIONAL EsWriteConfig diagnostics (per-partition ES bulk-send stats in the run log). Behaves like verify_certs: omit to defer to the global ${var.bulk_stats} default (off), or set true|false here to override it for this pipeline
retry_transport_timeout: true     # OPTIONAL EsWriteConfig reliability toggle: connector OWNS whole-request timeout retries (re-send a timed-out bulk with backoff instead of failing the batch). Behaves like bulk_stats: omit to defer to the global ${var.retry_transport_timeout} default (off), or set true|false here to override it for this pipeline
op_type: create                   # OPTIONAL EsWriteConfig write action: index (default, upsert by _id) | create (append-only; a resend of an existing _id is a benign 409 dedup, not overwritten or duplicated). Behaves like bulk_stats: omit to defer to the global ${var.op_type} (empty default defers to the connector's default write action), or set here to override it for this pipeline. create needs es_id_field to dedup a resend (else replays duplicate; the runner warns if create runs without one)
bypass_fast_path: true            # OPTIONAL EsWriteConfig write-path toggle: skip the errors-probe fast path and classify every chunk per-item. Makes docs_deduped/written counts EXACT for op_type=create at the cost of fast-path throughput. Behaves like bulk_stats: omit to defer to the global ${var.bypass_fast_path} default (off), or set true|false here to override it for this pipeline
streaming_start: new              # OPTIONAL first-run stream position: new (only new commits) | full (backfill whole table). Omit to defer to the global ${var.streaming_start} default (new), or set here; override per run. Streaming only; honored on a first run before a checkpoint exists
max_partition_bytes: 2m           # OPTIONAL: spark.sql.files.maxPartitionBytes for the source read (read parallelism); 0 leaves it unset; omit to defer to the global ${var.max_partition_bytes} (built-in default 2m)
write_repartition: 0              # OPTIONAL: repartition the write input to N partitions before bulk_write (0 = off); set > 0 only when the view shuffles; omit to defer to the global ${var.write_repartition} (built-in default 0)
max_files_per_trigger: 1000       # OPTIONAL streaming read rate-limit: max Delta files per micro-batch; omit for Spark default 1000. Useful to throttle a full backfill / post-restart catch-up
max_bytes_per_trigger: 128m       # OPTIONAL streaming read rate-limit: max bytes per micro-batch; omit for no cap
view:                             # the view this pipeline uses
  catalog: acme_${environment}
  schema: es_poc
  name: ecs_dns_activity
source:                           # the single source table the pipeline reads from
  catalog: acme_${environment}
  schema: ocsf
  table: dns_activity
  primary_key: dsl_id             # OPTIONAL, informational only: the source's unique-row column (not used at runtime today)
reference_tables:                 # OPTIONAL: holds one alias entry per joined table (add as many
                                  # alias entries below as you have reference tables)
  validation:                     # 'validation' is an EXAMPLE alias you choose; it is the
                                  # ${ref_validation} join alias used in the SQL
    catalog: acme_${environment}
    schema: ocsf_validation_${environment}
    table: dns_activity
# compute:                        # OPTIONAL: where this job runs. Omit for serverless (see Compute)
#   type: existing_cluster
#   cluster_config: interactive_primary   # names a per-target databricks.yml cluster config (complex var with a cluster_id field)
# schedule:                        # OPTIONAL: when this job runs. Omit for on-demand (see Scheduling)
#   quartz_cron_expression: "0 0 8 * * ?"   # 08:00 UTC daily
# continuous:                      # OPTIONAL: run always-on instead of scheduled (see Continuous streaming).
#   trigger_interval: 30 seconds   #   streaming + classic compute only; mutually exclusive with schedule
# pause_status: UNPAUSED           # OPTIONAL: override the target-wide schedule_pause_status for THIS job's trigger (needs a schedule/continuous; see Scheduling)
# job_group: ecs_streams           # OPTIONAL: merge every config sharing this name into ONE job, one task each (see Job groups)
# job_name_postfix: "ECS streams"  # OPTIONAL: cosmetic display-name segment (default: config name, or group name in a group)
```

A `catalog`/`schema` without an `${environment}` token is used verbatim. One that *uses* the token
fails closed at **run** (the runner folds the token in when the job runs; `bundle deploy`/`validate`
don't resolve it) if `environment` is empty or would produce an illegal identifier (e.g. one containing
a hyphen).

`es_id_field` and `source.primary_key` are two distinct keys for two distinct contexts, and **both are
optional**: `es_id_field` is a column of the **view's** output, handed to the connector as the ES
document `_id`; `primary_key` names the **source table's** unique-row column. They often share a value
but need not, and neither defaults to the other. When `es_id_field` is set and `deploy_views` creates a
view it verifies `es_id_field` is actually one of that view's output columns (against Spark's resolved
schema), so a typo fails the deploy rather than surfacing later at export time.

`source.primary_key` is **informational only**: no code reads it at runtime. Every streaming pipeline
assumes an **append-only** stream (updates and deletes are not processed), so there is nothing to
de-duplicate against a key. It's recorded so a config documents the source's real key. If the repo ever
gains full CDC support, `primary_key` would become **required** for any pipeline that has to apply
updates or deletes; until then you can set it or omit it freely.

`es_id_field` is likewise **optional**. Set it and each
document's `_id` is that column's value, so a re-run **upserts** over the same documents: the write is
idempotent and a retried batch or restarted stream converges to one document per id. **Omit it and the
pipeline passes no id_field to the connector, so ES assigns a random `_id` to every document.** That is
zero-config, but it is *not* idempotent: because both modes are at-least-once (a retried batch, a
restarted stream that reprocesses its last micro-batch), the same source rows can be written again as
**new** documents, leaving **duplicates** in the index. This is especially pronounced for
`pipeline_mode: streaming`, where restarts and micro-batch retries are routine rather than
exceptional, so an omitted `es_id_field` will typically accumulate duplicates over the life of the
stream. Omit `es_id_field` only when duplicates are acceptable (or the source guarantees no replay);
set it whenever you need a stable 1:1 row→document mapping. To get auto-ids you must **omit the key
entirely** or comment it out; a present-but-blank `es_id_field:` or `es_id_field: null` is treated as
an invalid value and fails the config, not as a request for auto-ids.

Conversely, if `es_id_field` *is* set but two input rows share its value, they collapse to a single
document (they share one `_id`), so ES ends up with *fewer* documents than rows sent. Which of the
duplicates survives is **not** guaranteed: the write is partitioned and runs in parallel, so there is no
defined "last" row. Ensure the column is unique across the input when you need a 1:1 mapping.

### Configuring Elasticsearch host connections

Each pipeline writes to one **host config**: a named group of the three connection settings the
connector needs, declared once in `databricks.yml` and referenced by name from the pipeline
(`es_host_config: <name>`). `es_host_config` is **optional**: a pipeline that omits it falls back to
the bundle's `default_es_host_config` (a `databricks.yml` variable, `es_host_primary` out of the box —
rename it or point it at your own default). A host config is:

| Field | What it is |
|---|---|
| `es_host_url` | the Elasticsearch endpoint, e.g. `https://<host>:9200` |
| `secret_scope_name` | the Databricks [secret scope](https://docs.databricks.com/security/secrets/) holding the ES **api_key** |
| `secret_key_name` | the key within that scope whose value is the ES **api_key** the connector authenticates with |

Because each environment writes to its own Elasticsearch cluster, a host config's values are set
**per target** (`dev`/`stg`/`prd`), so deploying to a target automatically uses that environment's host
— no `--var` needed. Only the api_key **value** is a secret (it lives in the referenced Databricks
secret scope); the endpoint and the scope/key **names** are not, so they are committed per target.

A host config is a `type: complex` bundle variable. On `main` the per-target values ship **empty**
(placeholders) — fill in the environments you deploy to:

```yaml
# databricks.yml
variables:
  es_host_primary:
    type: complex
    default: {es_host_url: "", secret_scope_name: "", secret_key_name: ""}   # empty = fail-closed

targets:
  dev:
    variables:
      es_host_primary:
        es_host_url: "https://your-dev-es-host:9200"
        secret_scope_name: "es_dev"
        secret_key_name: "api_key"
  prd:
    variables:
      es_host_primary:
        es_host_url: "https://your-prd-es-host:9200"
        secret_scope_name: "es_prd"
        secret_key_name: "api_key"
```

A pipeline whose host config is left empty for the target it deploys to **fails closed** at run
(`missing required parameter: es_host_url`) rather than writing nowhere.

**To add another host config** (e.g. to route some pipelines to a second cluster): declare a second
complex variable (`es_host_secondary`, same three fields), give it per-target values, and point a
pipeline at it with `es_host_config: es_host_secondary`. A pipeline referencing a host config that
isn't declared in `databricks.yml` fails **at generation** (`scripts/gen_jobs.py`), before deploy — as
does a pipeline that omits `es_host_config` when no `default_es_host_config` is declared. (The default
is read from the variable's `default:` at generation time, so it can't vary per target or by `--var`;
the default host config's *values* are still per target.)

(Don't want to commit even placeholder endpoints? Put the per-target maps in the git-ignored
`.databricks/bundle/<target>/variable-overrides.json` instead; the rest works identically.)

### Compute

By default each index job runs as a **serverless** notebook task (no cluster block). Set an optional
`compute` block per index to run it elsewhere; the choice is per index, so different pipelines can run
on different compute. `type` is one of:

| `type` | Extra key | Runs on |
|---|---|---|
| `serverless` (default; also when `compute` is omitted) | none | serverless notebook task |
| `existing_cluster` | `cluster_config` | an existing all-purpose/interactive cluster, named by a per-target bundle variable |
| `job_cluster` | `job_cluster_config` | a job cluster created per run from a reusable spec (see below) |

```yaml
# attach to an existing interactive cluster. A cluster id is workspace-specific, so you name a
# per-target bundle variable (never a literal id), so one config attaches to a different cluster per
# environment (dev/stg/prd):
compute:
  type: existing_cluster
  cluster_config: interactive_primary   # -> existing_cluster_id: ${var.interactive_primary.cluster_id}

# or run on a job cluster defined once and referenced by key:
compute:
  type: job_cluster
  job_cluster_config: standard_batch    # -> _pipelines/job_cluster_configs/standard_batch.yml
```

`cluster_config` names a `databricks.yml` **cluster config**: a `type: complex` bundle variable with a
single `cluster_id` field (per-target values, empty placeholders on `main` like the other environment
values). The generator emits `existing_cluster_id: ${var.<name>.cluster_id}` and the bundle resolves the
right cluster id per target at deploy. This mirrors `es_host_config` exactly: a workspace-specific value
is a shape-tagged per-target variable, never a literal baked into the config. A `cluster_config` that
doesn't name a declared cluster config (a complex variable with a `cluster_id` field) fails **at
generation** (`scripts/gen_jobs.py`), before deploy, so a typo or a reference to some other variable
(e.g. `wheel_path`) is caught up front. See the commented `interactive_primary` example in
`databricks.yml`.

**Reusable job-cluster specs** live in `_pipelines/job_cluster_configs/<key>.yml`. Each file is a
Databricks [`new_cluster`](https://docs.databricks.com/api/workspace/jobs/create) spec
(`spark_version`, `node_type_id`, `num_workers` or `autoscale`, `spark_conf`, ...), defined once and
referenced by its filename stem from any number of pipelines' `compute.job_cluster_config`. The
generator inlines the spec into each referencing job's `job_clusters` block, so there's no copy-paste;
`databricks bundle validate` checks the spec's own fields at deploy. See
[`_pipelines/job_cluster_configs/example.yml`](_pipelines/job_cluster_configs/example.yml) for the
format. (A job cluster is created fresh for each run and torn down after; for a single always-on
cluster shared across jobs, use `existing_cluster` instead.) A `job_cluster_config` that names no
existing file fails the generator, so a bad reference never deploys. `deploy_views` always runs
serverless.

**Cluster policy and tags.** The generator injects `policy_id: ${var.cluster_policy_id}` (plus
`apply_policy_default_values: true`) into every job cluster, so all job-cluster pipelines run under the
target workspace's cluster policy. A policy id is workspace-specific: set it **per target** in
`databricks.yml` (empty on `main`), the same way as `wheel_path` and the other environment values, or
override at deploy with `--var=cluster_policy_id=<id>`. Under `dev`'s `development` mode each engineer
deploys to their own workspace, so a committed `dev` value fits only one workspace; others override it
with `--var`. An empty value fails closed at deploy when a job-cluster pipeline is present
(the Jobs API rejects `policy_id: ""`), so provide it whenever any pipeline uses `job_cluster` compute.
Hardcoded `custom_tags` in a job-cluster spec pass straight through onto the cluster (e.g.
`project: elastic`). Serverless and `existing_cluster` pipelines have no job cluster and are unaffected
by either.

The whole `compute` block is validated fail-closed: an unrecognized `type`, a missing required key,
or a stray key for the chosen type is rejected at config load (and by `gen_jobs.py --check`).

### Scheduling

By default each index job is **on-demand** (run it with `bundle run` or the API). Add an optional
`schedule` block per index to run it on a [Quartz cron](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html):

```yaml
schedule:
  quartz_cron_expression: "0 0 8 * * ?"   # 08:00 every day
```

The timezone is always **UTC** (not a config field). Quartz cron has **6 or 7** fields (seconds first,
optional trailing year), so a 5-field Unix cron like `0 8 * * *` is rejected at config load with a
clear message rather than failing at deploy. Omitting `schedule` leaves the job on-demand.

The schedule pairs naturally with either export mode: a `batch` job re-exports the view on each tick,
and a `streaming` job drains new source commits since its last run on each tick (it uses
`Trigger.availableNow`, so a scheduled run processes the delta and stops). Every job sets
`max_concurrent_runs: 1` **and** `queue: {enabled: false}`, so a scheduled tick that fires while the
previous run is still going is **skipped** (`MAX_CONCURRENT_RUNS_EXCEEDED`) rather than overlapping or
piling up. The `queue` disable is load-bearing: the Jobs API defaults `queue.enabled` to `true`, which
would otherwise *queue* the overlapping tick (waiting up to 48h for the slot) instead of dropping it.
Skipping loses no work: a streaming job's next tick drains the full backlog since its last checkpoint,
and a batch job's next tick re-exports the whole view.

**Where schedules actually fire.** Every generated schedule's `pause_status` is bound to the
`schedule_pause_status` variable, which defaults to `PAUSED` (fail-safe), so a target controls firing
without touching configs: `dev` and `stg` inherit the paused default, so schedules are deployed but
**dormant** in both; only `prd` binds `UNPAUSED` and actually fires them. Unpause a single job in the
UI/API for a one-off test, or set `--var=schedule_pause_status=UNPAUSED` at deploy to override.

**Per-pipeline pause override.** A single pipeline can opt out of the target-wide default with its own
top-level `pause_status: PAUSED | UNPAUSED` config key, which overrides `${var.schedule_pause_status}`
for that one job's trigger (two-layer pattern: target-wide default `<` per-pipeline value). A pipeline
that omits `pause_status` inherits the target-wide default, so this is fully backward-compatible. It
applies to the job's `schedule` **or** `continuous` trigger, so it is only valid on a job that has one:
an on-demand pipeline (no `schedule`, no `continuous`, and not in a `job_group`) that sets `pause_status`
is rejected at config load, and a `job_group` with no trigger at all that sets it is rejected by
`gen_jobs.py`. In a `job_group` (one job, one trigger), members follow define-once/conflict-fails: at
most one distinct `pause_status` across members, others inherit.

```yaml
pause_status: UNPAUSED          # optional; overrides the target-wide schedule_pause_status default
schedule:
  quartz_cron_expression: "0 */10 * * * ?"
```

### Continuous (always-on) streaming

A scheduled `streaming` job drains the new commits and stops on each tick. For lower latency you can
instead run a stream **always-on** with a `continuous` block (mutually exclusive with `schedule`):

```yaml
pipeline_mode: streaming
compute:
  type: job_cluster            # classic compute REQUIRED (see below)
  job_cluster_config: standard_batch
continuous:
  trigger_interval: 30 seconds  # Spark ProcessingTime cadence
```

This emits a Databricks Jobs **continuous** trigger: the orchestrator keeps exactly one run perpetually
active and restarts it on completion or failure (see **Failure recovery** below), and the runner drives
the stream with `Trigger.ProcessingTime(interval)` instead of `Trigger.availableNow`, so it never terminates.
`trigger_interval` is the target gap between the **start** of one micro-batch and the next (batches
never overlap); a shorter interval trades ES/Delta efficiency (more, smaller bulk writes and index
refreshes) for lower latency.

**Classic compute only.** Serverless notebooks/jobs support only `Trigger.availableNow`, not the
`ProcessingTime` trigger an always-on stream needs. So `continuous` requires `compute.type`
`job_cluster` or `existing_cluster`; `continuous` on serverless, `continuous` with `pipeline_mode:
batch`, and `continuous` together with `schedule` are each rejected at config load (and by
`gen_jobs.py --check`), before deploy. Each restart resumes from the checkpoint, so `streaming_start`
matters only on the very first run.

**Pausing.** Like a schedule, the continuous trigger's `pause_status` is bound to
`schedule_pause_status` (default `PAUSED`), so `dev`/`stg` deploy the always-on job **dormant** and
only `prd` (or an explicit `--var=schedule_pause_status=UNPAUSED`) actually runs it. A single pipeline
can override this with its own top-level `pause_status` key (see **Per-pipeline pause override** above),
which applies to the continuous trigger the same way. One always-on run holds its job cluster for as
long as it is unpaused, so treat it as a running-cost commitment.

**Failure recovery.** A continuous job's per-task recovery is governed by the continuous trigger's
`task_retry_mode`, which the generator pins to **`ON_FAILURE`** for every continuous job. This is
deliberate and load-bearing: the field's API/bundle default when omitted is **`NEVER`** (a failed task
is never retried), even though the Jobs UI defaults it to `ON_FAILURE`. Per-task `max_retries` cannot be
used in a continuous job, so `task_retry_mode` is the only lever. Under `ON_FAILURE`:

- If a task's stream fails, Databricks **retries that task in place** (with exponential backoff) as long
  as at least one other task in the run is still on its first attempt, so one stream can recover without
  disturbing the others.
- When that no longer holds, or the retry limit is reached, the **whole run is cancelled and a fresh one
  started** (the job-level restart, itself exponential-backoff throttled).

This matters most for a **job group** of continuous streams (multiple tasks in one run): under the
`NEVER` default a failed task would sit `FAILED` while the sibling streams keep running, so the run never
reaches a terminal state and the continuous trigger never restarts it either - the failed task would
never recover. `ON_FAILURE` gives the intended "retry the task, else restart the job" behavior. A
single-task continuous job recovers under either value (its failure ends the run, which the trigger
restarts), but it is pinned the same way for consistency. Continuous jobs deployed before this change
carry the `NEVER` default until redeployed.

**Observability.** An always-on run never reaches the end-of-run summary (it never terminates), so
health comes from the Databricks Jobs continuous-run state (RUNNING / restart count / failure
notifications) plus the per-batch metrics the runner writes as each batch commits. Bound a first-run
backfill or a large post-restart catch-up with `max_files_per_trigger` / `max_bytes_per_trigger` so one
micro-batch does not try to read the whole table.

See [`_pipelines/pipeline_configs/ecs_dns_activity_continuous.yml`](_pipelines/pipeline_configs/ecs_dns_activity_continuous.yml)
for a worked example.

### Job groups

By default each config becomes its own Databricks job (`index_pipeline_<config_name>`). Set an
optional **`job_group`** on two or more configs to merge them into **one** job instead, with each
member config as an independent task (no inter-task dependencies):

```yaml
# in each member config
job_group: ecs_streams
```

The generator emits a single `resources/group_<job_group>.job.yml` (resource key
`index_pipeline_group_<job_group>`) with one task per member (`index_pipeline_<config_name>`, unchanged).
The main reason to group is **compute sharing**: members that name the **same** `job_cluster_config`
land on **one** physical job cluster (the generator deduplicates the `job_clusters` block), so N
pipelines run on one cluster instead of N. Members that name *different* cluster configs, or a mix of
serverless / `existing_cluster`, still coexist as tasks in the one job. Size a shared cluster for the
**sum** of its concurrent tasks (they run at once, and continuous members run perpetually).

What the group agrees on, all fail-closed at generation (`gen_jobs.py --check`):

- **Trigger** (define-once): a Databricks job has exactly one trigger, so members must not declare
  *conflicting* ones. Any member may set a `schedule` or `continuous` block (or omit it and inherit);
  the group is on-demand if none declare one, adopts the single declared trigger if one or more agree,
  and is **rejected** if members declare different triggers (two different crons, or `schedule` vs
  `continuous`). A **continuous** group propagates its `trigger_interval` to every member (so a member
  that omitted `continuous` still runs always-on rather than draining under `availableNow`) and
  requires **every** member to be `streaming` on classic compute. A failed member task recovers per the
  continuous trigger's `task_retry_mode: ON_FAILURE` (retry that task, else restart the whole run) - see
  [Continuous (always-on) streaming](#continuous-always-on-streaming) **Failure recovery**.
- **`job_name_postfix`** (define-once): same rule (omit on most members and set once); conflicting
  values are rejected (see [Job naming](#job-naming)).

**Members may target the same `es_index_name`.** This is allowed (e.g. one task per disjoint
`filter_condition` subset feeding a single index), so the generator only **warns** rather than failing.
Be deliberate: grouped members run as **concurrent** tasks in one run (`max_concurrent_runs: 1` serializes
job *runs*, not the tasks within a run), so two tasks write that index at the same time. That is safe for
**disjoint** rows; overlapping rows either duplicate (ES auto-ids) or race on upserts (a shared
`es_id_field`). Give each member a distinct `filter_condition` and set `es_id_field` for idempotency.
(Separate jobs writing one index have always been allowed and are serialized only within a single job's
runs, not across jobs.)

**Run-time parameters differ in a group.** A standalone job exposes the run-time knobs (`pipeline_mode`,
`filter_condition`, `chunk_size`, …) as job-level **parameters**, overridable per run with `--params`.
A job cannot hold *per-member* parameter defaults, so a grouped job instead bakes each member's knobs
into that **task's** `base_parameters` at deploy. The runner reads the same widgets either way (no
behavior change), but on a grouped job these are **not** `--params`-overridable per member: change a
member's value in its config and redeploy. (A run-time `notebook_params` override still applies, but
job-wide to every task.) If you need per-member run-time overrides, keep those configs standalone.

### Job naming

A job's display name is `[<target>] <prefix>: <postfix>`.

- **`prefix`** is the `job_name_prefix` bundle variable (`databricks.yml`), default
  `databricks-elasticsearch-pipelines`. The generator emits `${var.job_name_prefix}`, so you can
  rebrand every job name per target (or with `--var=job_name_prefix=<name>`) without regenerating.
  The hand-authored `deploy_views` job references the same variable, so it rebrands alongside the
  generated jobs.
- **`postfix`** defaults to the config name (standalone) or the group name (group). Set an optional
  **`job_name_postfix`** on a config to override just the trailing display segment (e.g.
  `"ECS DNS (serverless)"`). It is purely cosmetic (it never changes any resource key or task key), so
  it may contain spaces and punctuation. In a group it follows the define-once rule above.

## Deploy and run

The bundle defines three **targets**, selected with `-t`: `dev` (the default), `stg`, and `prd`.
`dev` uses DAB `development` mode (deploys are isolated to the deploying user and schedules are
paused); `stg` and `prd` use `production` mode with a shared, non-user deploy path. All three take the
workspace host from your CLI profile (`-p <profile>`) or `DATABRICKS_HOST`, so the same target can
point at any workspace.

Two different mechanisms carry values into a job, and they resolve at different times:

- **Bundle variables** (`environment`, `wheel_path`, `checkpoint_base_path`, `cluster_policy_id`,
  `ca_certs`, a global default for **every run-time knob** (`pipeline_mode`, `op_type`, `streaming_start`,
  `bulk_stats`, and the rest; see the variables table), and the
  ES host configs) are resolved into the job at
  **deploy** time. Each is set **per
  target** in `databricks.yml` (`targets.<env>.variables`), so a routine deploy takes no `--var` at all.
  The simple string variables can still be overridden at deploy with `--var=<name>=<value>`, which wins over
  the per-target value; the `type: complex` variables (the ES host configs, and any `cluster_config`)
  cannot be set via `--var` at all (the CLI rejects it: *"setting variables of complex type via --var
  flag is not supported"*), so override those through the git-ignored `variable-overrides.json`. A
  `--var` on `bundle run` is ignored: only what was set at the last `bundle deploy` applies. They ship
  empty on `main`, so a deploy without filling them in still succeeds and `deploy_views` runs fine (it
  needs no connector or ES); an index job needs a real `wheel_path`, a streaming run also needs
  `checkpoint_base_path`, a job-cluster pipeline needs `cluster_policy_id`, and an index job whose host
  config is empty for its target fails closed at run. The ES host configs have their own section:
  [Configuring Elasticsearch host connections](#configuring-elasticsearch-host-connections).
- **Job parameters** are `--params` values applied at **run** time, overridable per run without
  redeploying (an invalid value fails the run closed). This is the standalone-job model; a
  [job group](#job-groups) instead bakes these per member at deploy (not `--params`-overridable):
  - `pipeline_mode` (`batch` | `streaming`), `filter_condition` (a Spark SQL predicate), and the
    connector-write tuning knobs `chunk_size`, `write_concurrency`, `request_timeout`,
    `transport_max_retries`, `require_existing_index`, `verify_certs` (each an optional config key; see
    [Configuration](#configuration)). Clearing a stale streaming checkpoint is a separate operation, not
    a `pipeline_mode` (see [Resetting a checkpoint](#resetting-a-checkpoint)).
  - **Every run-time knob follows one uniform three-layer pattern**: a per-run `--params <name>=<value>`
    (highest) > the pipeline's own config value > the target-wide `${var.<name>}` global default in
    `databricks.yml` > the connector/Spark/built-in default. So a config that omits a knob (and a run that
    doesn't override it) inherits that target's global, and when the global is left at its shipped default
    the connector's/Spark's own default applies, exactly as before. `pipeline_mode` and `streaming_start`
    ship concrete globals (`batch` / `new`, since their validators reject `""`); the rest ship empty
    (`op_type` included, so its connector default applies). This is why a knob can be moved for a whole environment from
    `databricks.yml` alone, without editing any pipeline.
  - `write_concurrency` (a positive integer) runs that many bulk request
    streams in parallel *within each write partition*. Raise it when
    the write is latency-bound on ES round-trips (executors idle, CPU and network both under-used)
    rather than CPU/bandwidth-bound; it multiplies with the partition count, so raise it gradually and
    watch for 429s. Applies to **both** modes.
  - `request_timeout` (a positive integer, **seconds**) and
    `transport_max_retries` (a non-negative integer; `0` disables) tune a write
    that fails at the transport layer: the classic symptom is `EsWriteError: ... ConnectionTimeout ...
    The write operation timed out`, where a whole bulk request exceeded the socket timeout. Because the
    request never returned per-document statuses, the connector fails those documents closed (counted as
    rejected, with a single `{_id: None, op_type: bulk, status: None}` sample) rather than listing them
    individually. Raise `request_timeout` so a large/slow bulk send has longer to complete, and/or lower
    `chunk_size` so each request is smaller; `transport_max_retries` is how many times the ES client
    re-sends the *whole* request on such a failure (it is **not** the per-document 429 retry, which is a
    separate connector knob not surfaced here). Both apply to **both** modes.
  - `bulk_stats` (`true` | `false`) collects
    per-partition ES bulk-send diagnostics and logs them under the `BULK_STATS` tag. It behaves like the
    other bool knobs, with one extra layer: a **global** default. Precedence, highest first: a per-run
    `--params bulk_stats=<v>` > a pipeline's own `bulk_stats:` config value > the target-wide
    `${var.bulk_stats}` databricks.yml variable > the connector's own default (**off**). So set
    `bulk_stats` in databricks.yml (per target, or `--var=bulk_stats=true`) to turn the diagnostic on for
    a whole environment without editing each config, and set/clear it in one pipeline's config to
    override that. Batch runs emit
    an `overall` rollup (`docs/send`, and send-weighted mean / max round-trip `rtt_ms` and ES-reported
    `took_ms`, plus `cpu_ms` and `gil_wait_ms` totals), one line per write partition with real
    p50/p95/max, and a `tail:` summary that names the slowest partition (its `wall_ms`, `sends`,
    `rtt_ms_max` vs `took_ms_max`, `gil_wait_ms_total`, concurrency, and docs) and
    the spread behind a tail: `wall_p95`, `wall_max/median`, a `stragglers>2x` count, and the
    `docs_max/median` skew (alongside a driver `bulk_write_wall_ms`, so an in-write straggler is
    distinguishable from time spent after the write returns). Streaming runs emit the compact `overall`
    line plus that `tail:` summary per micro-batch. Every line carries a `ts=<UTC>` wall-clock token so a
    batch can be placed in real time; this matters because a micro-batch's write runs server-side (its
    log output does not reach the notebook cell), so each batch's line is written to a small per-batch
    file and re-emitted by the progress listener, appearing alongside the `STREAM_PROGRESS` line (in the
    notebook cell for an interactive run, in the driver log for a job run) LATER than the batch actually
    ran, so the ambient log timestamp is the relay time and the embedded `ts=` is the true batch time.
    `rtt_ms - took_ms`
    is the network/queue overhead and
    `docs/send` is the real docs-per-bulk, so this is the tool for diagnosing whether more
    `write_concurrency` or cores would help; `gil_wait_ms` (from the connector's GIL-acquisition probe,
    on connectors that emit it, else `n/a`) separates a slow round trip that is a genuine socket/ES wait
    from one inflated by GIL starvation under a high `write_concurrency`. Applies to **both**
    modes.
  - `retry_transport_timeout` (`true` | `false`) moves whole-request
    **timeout** retries into the connector: a bulk send that exceeds the connector's `request_timeout`
    is re-sent with bounded exponential backoff instead of being re-sent invisibly by the ES transport
    and then failing the batch (which makes Spark re-run the whole task, so a latency-bound pipeline
    falls further behind). It has the **same layering as `bulk_stats`**, including the global default.
    Precedence, highest first: a per-run `--params retry_transport_timeout=<v>` > a pipeline's own
    `retry_transport_timeout:` config value > the target-wide `${var.retry_transport_timeout}`
    databricks.yml variable > the connector's own default (**off**). So set `retry_transport_timeout` in
    databricks.yml (per target, or `--var=retry_transport_timeout=true`) to turn it on for a whole
    environment, and set/clear it in one pipeline's config to override that. When on, the connector does
    **not** shorten `request_timeout` and only retries `ConnectionTimeout` (connection resets and
    429/503 on the bulk call itself still retry at the transport as before); with `bulk_stats` also on,
    the retry cost is visible as `timeout_sends` / `timeout_wait_ms` in the `BULK_STATS` output. Applies to **both** modes.
  - `op_type` (`index` | `create`, default `index`) picks the `_bulk`
    action. `index` upserts by `_id` (a resend overwrites). `create` is **append-only**: a resend of an
    existing `_id` returns a 409 the connector treats as a benign dedup (the doc is neither overwritten
    nor duplicated), which lets ES take its cheaper append path. Unlike `bulk_stats` /
    `retry_transport_timeout`, this is **per-feed only** (no global `${var.*}` default): set it in a
    pipeline's config, or `--params op_type=create` for one run. It requires `es_id_field` (a create
    without an explicit `_id` never conflicts) and must be used **only** for feeds that never legitimately
    update an existing `_id` (a real update is silently absorbed as a 409). Under the default fast path,
    a chunk that mixes a NEW `_id` with already-indexed ones can MISCOUNT: the probe creates the new doc,
    then the whole-chunk re-ship self-409s it, so it is tallied as `docs_deduped` rather than `written`
    (the data is still correct: one copy, no overwrite; only the attribution is off). All-new and
    all-existing chunks count exactly. Set `bypass_fast_path` (below) to make the counts exact, or accept
    the possible miscount. Applies to **both** modes.
  - `bypass_fast_path` (`true` | `false`) turns off the connector's
    `filter_path="errors"` fast path so every chunk is classified per-item on the first send (no
    whole-chunk re-ship). This makes the `docs_deduped` / `written` counts **exact** for `op_type=create`
    on chunks that mix new and already-indexed `_id`s (the miscount described above), and it also avoids
    the auto-id re-ship duplication, at the cost of a per-item response decode on clean chunks (it gives
    up the GIL-avoidance throughput of the fast path). It is useful beyond `op_type=create` too: any run
    that wants a full per-item check on the first try. It has the **same layering as `bulk_stats`**,
    including the global default. Precedence, highest first: a per-run `--params bypass_fast_path=<v>` >
    a pipeline's own `bypass_fast_path:` config value > the target-wide `${var.bypass_fast_path}`
    databricks.yml variable > the connector's own default (**off**; the fast path is used). So set
    `bypass_fast_path` in databricks.yml (per target, or `--var=bypass_fast_path=true`) to turn it on for
    a whole environment. Applies to **both** modes.
  - `streaming_start` (`new` | `full`) sets where a **streaming** run begins on its
    first run: `new` streams only commits after the stream starts (batch mode owns the history);
    `full` backfills the whole existing table first. Now an optional config key on the same three-layer
    pattern (a `${var.streaming_start}` global default of `new` < a pipeline's config value < `--params`).
    See [Streaming](#streaming).
  - `max_files_per_trigger` / `max_bytes_per_trigger` (a count and a Spark byte-size; default unset)
    bound each **streaming** micro-batch (Spark defaults: 1000 files, no byte cap). Most useful to
    throttle a `streaming_start=full` backfill or a large post-restart catch-up so one micro-batch does
    not read the whole table. Ignored by batch.
  - `max_partition_bytes` (a Spark byte-size such as `2m`, default `2m`) sets
    `spark.sql.files.maxPartitionBytes` for the source read. Smaller values produce more, smaller file
    splits, so the scan and the view transform fan out across more cores. This is the primary
    parallelism lever, since those partitions carry through the (shuffle-free) view to the write. Aim
    for a partition count of **~2-3x total worker cores** (the same target as `write_repartition`
    below), i.e. set it to about `data_size / (2-3 × cores)`. `0` leaves the cluster/engine default
    untouched. Applies to **both** modes.
  - `write_repartition` (a non-negative integer, default `0` = off) repartitions the write input to N
    partitions before the ES write (`bulk_write` runs one bulk stream per partition). It is off by
    default because `max_partition_bytes` already parallelizes the read and that partitioning flows
    through to the write. Set it `> 0` (a good target is ~2-3x total worker cores) only when the write
    needs parallelism the read does not supply, e.g. a view that **shuffles** (a non-broadcast join,
    `GROUP BY`, `DISTINCT`, window) resets the post-shuffle partition count, or **large source files**
    that read into too few partitions to spread the write across the cluster (a smaller
    `max_partition_bytes` splits them further, but repartitioning also fixes it). Applies to **both** modes.

```bash
python scripts/gen_jobs.py   # regenerate resources/<config_name>.job.yml from _pipelines/pipeline_configs/*.yml

# Environment-specific values (environment, wheel_path, checkpoint_base_path, cluster_policy_id, ca_certs,
# the per-target global default for every run-time knob, and the ES host config) come from this target's variables block in databricks.yml. Fill in the target you
# deploy to BEFORE running an index pipeline: the shipped configs embed ${environment} and install the
# connector wheel, so an index run with those still empty fails closed (deploy itself always succeeds).
# Filled in, the deploy needs no --var:
databricks bundle deploy -t dev -p <profile>

# The five simple string vars can still be overridden ad hoc, e.g. a one-off wheel:
databricks bundle deploy -t dev -p <profile> \
  --var="wheel_path=/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"

databricks bundle run deploy_views                 -t dev -p <profile>
databricks bundle run index_pipeline_<config_name> -t dev -p <profile>

# override run-time settings for a single run (each defaults to its config/connector value otherwise):
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> \
  --params filter_condition="action = 'allowed'",chunk_size=1000,max_partition_bytes=8m

# stream a one-off full backfill of the whole table (default is new-commits-only):
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> \
  --params pipeline_mode=streaming,streaming_start=full
```

(Bundle variables come from the last `deploy`, so they are not repeated on `run`. Fill in each target's
`variables` block in your fork's `databricks.yml` once so no `--var` is needed per deploy. Prefer not to
commit even placeholder paths? Put the per-target values in the git-ignored
`.databricks/bundle/<target>/variable-overrides.json` instead; the rest works identically.)

## Streaming

A `pipeline_mode=streaming` run reads the **raw source table** as a Delta stream, applies the view's own
transform to each micro-batch, and bulk-writes it. By default it uses `Trigger.availableNow`: it drains
all currently-available new commits, then stops, so each job run exports what's arrived since the last
run. Schedule the job (or run it on demand) to keep an index current, or make it **always-on** with a
[`continuous`](#continuous-always-on-streaming) block, which drives the same stream with a
never-terminating `Trigger.ProcessingTime` micro-batch trigger on classic compute instead.

Key behaviors:

- **The view logic runs over each micro-batch, not by reading the deployed view.** The framework
  takes the view's `SELECT` and runs it with the micro-batch bound in place of the source table, so
  the exact same projection/joins/hints the view defines apply, but only to batch-sized data (never a
  join back to the full view). Reference (join) tables are read as their real tables.
- **Row-wise views only (streaming).** Because the view runs per micro-batch, streaming supports only
  **row-wise** views: projection, filters, scalar expressions, and 1:1 reference joins, where each
  output row depends on a single source row. A view that aggregates **across** source rows (`GROUP
  BY`, `DISTINCT`, window/`OVER`, `PIVOT`) would be computed per batch, not over the whole stream, so
  its streamed results would silently differ from batch mode. This is a limitation of streaming mode;
  use `batch` mode for aggregating views.
- **Append-only assumption.** The stream uses `skipChangeCommits`, so a non-append commit (a manual
  `UPDATE`/`DELETE`/`MERGE` on the source) is skipped rather than failing the stream. If you make such
  a change and need it reflected in the index, re-send the affected records with a `batch` run.
- **Where the stream starts (`streaming_start`).** Default `new` starts at the source's current
  Delta version, so existing history is not re-exported and later runs pick up only new commits;
  `full` backfills the whole existing table on the first run. This choice is honored **only on the
  first run**, before a checkpoint exists: once a stream has a checkpoint, that checkpoint is the
  position of record and `streaming_start` is ignored. (The start version is inclusive, so if the
  source's current commit is itself an append, that one commit's rows are re-sent on the first `new`
  run; deterministic `_id`s make this a harmless idempotent upsert bounded to a single commit.)
- **Checkpoints.** Each stream keeps its checkpoint at `<checkpoint_base_path>/<config_name>`. If an
  index is reset and you want to resend its records from the Delta table, clear that stream's
  checkpoint first, otherwise the stream considers those records already exported and writes nothing.
  Deterministic document `_id`s make a re-send an idempotent upsert, not a duplicate. Clear it with the
  dedicated `_checkpoint clear` job below rather than by hand.
  - **Per-developer checkpoints in `dev`.** `mode: development` isolates workspace files and resource
    names per user but **not** UC Volume data paths, so two developers streaming the same pipeline would
    share one checkpoint. The `dev` target in `databricks.yml` documents appending
    `${workspace.current_user.short_name}` to `checkpoint_base_path` (DAB resolves it to the deploying
    user at deploy time) so each engineer gets an isolated checkpoint tree.

### Resetting a checkpoint

When a stream's checkpoint is stale (say the index was wiped, or you want to re-stream from a fresh
`streaming_start`), clear it with the dedicated `_checkpoint clear` job (a serverless maintenance job,
`resources/checkpoint_clear.job.yml`):

```bash
databricks bundle run checkpoint_clear -t <target> -p <profile> --params config_name=<config_name>
```

This deletes the whole checkpoint directory `<checkpoint_base_path>/<config_name>` and does nothing else
(it never reads the ES secret or writes a document). The next `streaming` run of that pipeline then
behaves like a first run: `streaming_start=new` reseeds at the source's current version,
`streaming_start=full` backfills the whole table. Clearing an already-absent checkpoint is a harmless
no-op. `config_name` is required and validated closed (a blank or path-bearing value fails), so the job
can only ever clear the one checkpoint that pipeline would resume from, never the shared base or another
pipeline's checkpoint.

For a **continuous** (always-on) pipeline, pause the continuous trigger first (or cancel the running
job), run the clear, then resume, so the always-on stream is not re-establishing a checkpoint while you
clear it.

### Diagnosing a congested Elasticsearch host

When a host looks backed up (writes slowing down, requests timing out), run the read-only `es_diagnostics`
job (a serverless maintenance job, `resources/es_diagnostics.job.yml`) to collect an outside view of what
the cluster is doing:

```bash
databricks bundle run es_diagnostics -t <target> -p <profile>                              # es_host_primary, cluster/node level
databricks bundle run es_diagnostics -t <target> -p <profile> --params index_name=<index>  # + deep-dive one index
databricks bundle run es_diagnostics -t <target> -p <profile> --params es_host_config=es_host_secondary  # hit a different host config
```

It only issues `GET` requests (it never writes a document or reads the ES secret for anything but the
`Authorization` header) and connects to the host config named by the `es_host_config` parameter (default
`es_host_primary`), the same endpoints and `api_key` secrets the pipelines use — so testing a different
configured host is a `--params es_host_config=<name>`, not a YAML edit (the job carries every configured
host's url + secret scope/key, and the notebook resolves the chosen name, failing closed on an unknown
one). It gathers: write thread-pool saturation and rejections, indexing pressure,
circuit breakers, node heap/GC, cluster health and pending tasks, in-flight bulk tasks, hot threads, and
(when `index_name` is set) that index's `_stats`/`_settings`/`_count`/shard placement. It samples the
counter endpoints **twice** (`sample_interval_secs` apart, default 5s) so rejection/GC rates reflect the
window rather than lifetime totals, then prints a one-line **verdict** classifying the signature:

- `REJECTING` — ES is actively shedding load with 429s (write-pool / indexing-pressure / breaker
  rejections in the window). This is the idiomatic backpressure signal; reduce bulk size / write
  concurrency and back off on 429.
- `SATURATED` — the write queue is building but not yet rejecting (near capacity).
- `PRESSURED` — indexing-pressure memory is a high fraction of its limit (rejections imminent).
- `HEAP_GC_PRESSURE` — high heap and/or heavy GC in the window: the box is processing slowly rather than
  queueing. A slow `_bulk` that times out client-side (rather than a fast 429) points here or at the
  network, not at a full queue.
- `HEALTHY` — every load-bearing signal was collected and is clear.
- `INCONCLUSIVE` — a load-bearing signal (write rejections/queue, indexing pressure) could not be
  collected, so the host cannot be cleared as healthy (fail closed). The failed endpoints are listed.

Run parameters (all `--params`-overridable): `index_name` (blank => cluster/node level only),
`verify_certs` (`false` for a self-signed endpoint; ignored when `ca_certs` is set), `sample_interval_secs`
(`0` => single snapshot: gauges only, no rejection rate, so it can surface `SATURATED`/`PRESSURED` but
never clears a host as `HEALTHY` — use the default two-sample window for that), and `request_timeout_secs`.
The run FAILS only when it could
collect nothing at all (bad host / api_key / TLS / egress); a "found congestion" verdict is a successful
run, since reporting congestion is the job's purpose.

By default the job runs **serverless**. To run it on a specific cluster instead (e.g. to match the
client's DBR runtime or the network/egress path the real pipeline writes over), set one of the
`es_diagnostics COMPUTE` variables in `databricks.yml` and re-deploy (compute is a deploy-time property;
Databricks has no run-time cluster swap): `diagnostics_existing_cluster_id` to run on an already-running
cluster by id (a plain string, so `--var="diagnostics_existing_cluster_id=<id>"` works), or
`diagnostics_job_clusters` + `diagnostics_job_cluster_key` to run on a new job cluster — copy a
`_pipelines/job_cluster_configs/<key>.yml` `new_cluster` spec into the `diagnostics_job_clusters` list
(complex var; set per target or in `variable-overrides.json`). Set at most one path.

The workspace deployed to is whichever one `-p <profile>` (or `DATABRICKS_HOST`) points at.
All jobs are granted `CAN_MANAGE_RUN` to the `users` group, so teammates can trigger them on demand.

Running the generator needs `pyyaml`, pinned in `requirements.txt` (`pip install -r requirements.txt`).
The pin matters because `--check` byte-compares against `yaml.safe_dump` output, whose formatting can
drift across pyyaml versions.

## Layout

```
databricks.yml                  Bundle definition: variables + targets
requirements.txt                Off-cluster tooling deps (pinned pyyaml for the generator)

You edit these, one pair per pipeline (all under _pipelines/, the pipeline-configuration folder):
  _pipelines/
    pipeline_views/
      <view_name>.sql           The view: what gets exported (filename == view.name)
    pipeline_configs/
      <config_name>.yml         The config: view/source/reference locations + es_index_name,
                                pipeline_mode, optional es_id_field/source.primary_key/compute
                                + schedule (see Configuration)
    job_cluster_configs/
      <key>.yml                 OPTIONAL reusable new_cluster specs, referenced by key from a
                                config's compute.job_cluster_config (see Compute)

Shared notebooks (run by the jobs, not edited per pipeline):
  notebooks/
    deploy_views.py             Renders each view's parameters from its config (folding in the
                                environment), then runs CREATE OR REPLACE
    run_index_pipeline.py       Run by every per-index job: installs the connector wheel (verifying
                                the import), loads its config by name, resolves the environment, and
                                exports to Elasticsearch via the connector - batch (bulk_write over the
                                deployed view) or streaming (view SELECT over each source micro-batch)

Shared library + tests (the config schema, used by the generator and both notebooks):
  pipeline_lib/
    config.py                   Loads/validates a pipeline definition; resolves ${environment} and
                                derives view substitutions + job parameters (single source of truth)
  tests/
    test_config.py              Offline unit tests for pipeline_lib.config (plain pytest)

Generated / tooling (do not hand-edit the generated jobs):
  scripts/
    gen_jobs.py                 Generates resources/<config_name>.job.yml from the configs (--check guards drift)
  resources/
    deploy_views.job.yml        The deploy_views job (hand-authored)
    <config_name>.job.yml       GENERATED per-index job (one per pipeline_configs config)
```

## License & Attribution

**Copyright © Databricks, Inc.** — Developed and maintained by Databricks Forward Deployed Engineering. Available to support customers and the broader community in building Elasticsearch export pipelines on Databricks. For production support and customization, contact your Databricks account team.

---

**Built with 💜 by Databricks Forward Deployed Engineering**

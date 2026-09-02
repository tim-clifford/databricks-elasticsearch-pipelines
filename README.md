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

- **One `deploy_views` job**: creates or replaces one Databricks view per index. Each view is a
  `.sql` file in [`_pipelines/pipeline_views/`](_pipelines/pipeline_views/). The job renders the
  catalog/schema parameters and runs every file with `spark.sql`.
- **One job per index** (`index_pipeline_<config_name>`): all run the same shared notebook
  [`notebooks/run_index_pipeline.py`](notebooks/run_index_pipeline.py) with that index's config. These
  job resources are **generated** by [`scripts/gen_jobs.py`](scripts/gen_jobs.py) from the config
  files (see [Adding a new pipeline for an ES index](#adding-a-new-pipeline-for-an-es-index)).

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

## Configuration

The **workspace** is not a bundle variable: it comes from your Databricks CLI profile (`-p <profile>`)
or `DATABRICKS_HOST`. The environment-specific connection, path, and policy variables (`environment`,
`wheel_path`, `checkpoint_base_path`, `cluster_policy_id`, `ca_certs`, and the ES host configs) are set **per
target** in `databricks.yml` (`targets.<env>.variables`), shipping **empty** on `main` for you to fill
in for the environments you deploy to, so a routine deploy needs no `--var`. (`schedule_pause_status` is
also per-environment but is the exception: it defaults to `PAUSED` globally and only `prd` overrides it,
and it is `--var`-settable too; see [Scheduling](#scheduling).) The five simple per-target string
variables (`environment`, `wheel_path`, `checkpoint_base_path`, `cluster_policy_id`, `ca_certs`) can still
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
| `schedule_pause_status` | `PAUSED` or `UNPAUSED` applied to every scheduled job (default `PAUSED`, fail-safe). `dev` and `stg` inherit the paused default so they deploy schedules without firing them; only `prd` binds `UNPAUSED` to actually run them. Only affects jobs that declare a `schedule` (see [Scheduling](#scheduling)) |

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
pipeline_mode: batch              # default export mode: batch | streaming (required; can override per run)
filter_condition: "action = 'allowed'"  # OPTIONAL default row filter (Spark SQL); omit for no filter
chunk_size: 1000                  # OPTIONAL EsWriteConfig tuning (docs per bulk request); omit for connector default
require_existing_index: true      # OPTIONAL EsWriteConfig tuning (require the index to exist); omit for connector default
verify_certs: true                # OPTIONAL EsWriteConfig tuning (verify the ES TLS cert); omit for connector default
write_concurrency: 4              # OPTIONAL EsWriteConfig tuning (parallel bulk streams per partition; connector >= 0.7.0); omit for connector default 1
max_partition_bytes: 2m           # OPTIONAL: spark.sql.files.maxPartitionBytes for the source read (read parallelism); 0 leaves it unset; omit for default 2m
write_repartition: 0              # OPTIONAL: repartition the write input to N partitions before bulk_write (0 = off, the default); set > 0 only when the view shuffles
view:                             # the view this pipeline uses
  catalog: acme_${environment}
  schema: es_poc
  name: ecs_dns_activity
source:                           # the single source table the pipeline reads from
  catalog: acme_${environment}
  schema: ocsf
  table: dns_activity
  primary_key: dsl_id             # source-table column identifying a unique row (for the streaming read)
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
```

A `catalog`/`schema` without an `${environment}` token is used verbatim. One that *uses* the token
fails closed at **run** (the runner folds the token in when the job runs; `bundle deploy`/`validate`
don't resolve it) if `environment` is empty or would produce an illegal identifier (e.g. one containing
a hyphen).

`es_id_field` and `source.primary_key` are two distinct keys for two distinct contexts: `es_id_field`
is a column of the **view's** output, handed to the connector as the ES document `_id`; `primary_key`
is a column of the **source table**, used by the streaming read to identify a unique row. They often
share a value but need not, and neither defaults to the other. When `es_id_field` is set and
`deploy_views` creates a view it verifies `es_id_field` is actually one of that view's output columns
(against Spark's resolved schema), so a typo fails the deploy rather than surfacing later at export time.

`es_id_field` is **optional** (unlike `source.primary_key`, which is required). Set it and each
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
`Trigger.availableNow`, so a scheduled run processes the delta and stops). Because every job sets
`max_concurrent_runs: 1`, a scheduled run that fires while the previous one is still going is skipped
rather than overlapping.

**Where schedules actually fire.** Every generated schedule's `pause_status` is bound to the
`schedule_pause_status` variable, which defaults to `PAUSED` (fail-safe), so a target controls firing
without touching configs: `dev` and `stg` inherit the paused default, so schedules are deployed but
**dormant** in both; only `prd` binds `UNPAUSED` and actually fires them. Unpause a single job in the
UI/API for a one-off test, or set `--var=schedule_pause_status=UNPAUSED` at deploy to override.

## Deploy and run

The bundle defines three **targets**, selected with `-t`: `dev` (the default), `stg`, and `prd`.
`dev` uses DAB `development` mode (deploys are isolated to the deploying user and schedules are
paused); `stg` and `prd` use `production` mode with a shared, non-user deploy path. All three take the
workspace host from your CLI profile (`-p <profile>`) or `DATABRICKS_HOST`, so the same target can
point at any workspace.

Two different mechanisms carry values into a job, and they resolve at different times:

- **Bundle variables** (`environment`, `wheel_path`, `checkpoint_base_path`, `cluster_policy_id`,
  `ca_certs`, and the ES host configs) are resolved into the job at **deploy** time. Each is set **per
  target** in `databricks.yml` (`targets.<env>.variables`), so a routine deploy takes no `--var` at all.
  The five simple string variables can still be overridden at deploy with `--var=<name>=<value>`, which wins over
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
  redeploying (an invalid value fails the run closed):
  - `pipeline_mode` (`batch` | `streaming`), `filter_condition` (a Spark SQL predicate), and the
    connector-write tuning knobs `chunk_size`, `write_concurrency`, `require_existing_index`,
    `verify_certs` all default to their config values (each is an optional config key; see
    [Configuration](#configuration)).
  - For the tuning knobs, a config that omits a knob (and a run that doesn't override it) leaves the
    connector's own default in force.
  - `write_concurrency` (a positive integer, default the connector's `1`) runs that many bulk request
    streams in parallel *within each write partition* (requires connector **>= 0.7.0**). Raise it when
    the write is latency-bound on ES round-trips (executors idle, CPU and network both under-used)
    rather than CPU/bandwidth-bound; it multiplies with the partition count, so raise it gradually and
    watch for 429s. Applies to **both** modes.
  - `streaming_start` (`new` | `full`, default `new`) sets where a **streaming** run begins on its
    first run: `new` streams only commits after the stream starts (batch mode owns the history);
    `full` backfills the whole existing table first. See [Streaming](#streaming).
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
# and the ES host config) come from this target's variables block in databricks.yml. Fill in the target you
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

A `pipeline_mode=streaming` run reads the **raw source table** as a Delta stream
(`Trigger.availableNow`: it drains all currently-available new commits, then stops, so each job run
exports what's arrived since the last run), applies the view's own transform to each micro-batch, and
bulk-writes it. Schedule the job (or run it on demand) to keep an index current.

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
  Deterministic document `_id`s make a re-send an idempotent upsert, not a duplicate.
  - **Per-developer checkpoints in `dev`.** `mode: development` isolates workspace files and resource
    names per user but **not** UC Volume data paths, so two developers streaming the same pipeline would
    share one checkpoint. The `dev` target in `databricks.yml` documents appending
    `${workspace.current_user.short_name}` to `checkpoint_base_path` (DAB resolves it to the deploying
    user at deploy time) so each engineer gets an isolated checkpoint tree.

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
                                pipeline_mode, source.primary_key, optional es_id_field/compute
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

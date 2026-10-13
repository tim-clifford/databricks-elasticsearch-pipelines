# Parameters Reference

Every value you can set, in one place: what it does, where you set it, when it takes effect, its
default, and what to weigh when you change it. Other docs link here instead of repeating these
definitions.

- [How values are set](#how-values-are-set)
- [Environment variables](#environment-variables) (`databricks.yml`)
- [Elasticsearch host configs](#elasticsearch-host-configs) (`databricks.yml`)
- [Cluster configs](#cluster-configs) (`databricks.yml`)
- [Diagnostics compute](#diagnostics-compute) (`databricks.yml`)
- [Feed definition keys](#feed-definition-keys) (feed config)
- [Run-time knobs](#run-time-knobs) (`databricks.yml`, feed config and `--params`)
- [Maintenance job parameters](#maintenance-job-parameters) (`--params`)

## How values are set

Values reach a job in three ways, and they take effect at different times.

| Kind | Where you set it | Takes effect |
|---|---|---|
| Bundle variable | `databricks.yml` (top-level `variables` and `targets.<env>.variables`) | at `bundle deploy` (a few are also read by the generator; noted per entry) |
| Feed config key | `_pipelines/pipeline_configs/<config_name>.yml` | when you regenerate (`python framework/scripts/gen_jobs.py`) and deploy |
| Job parameter | `--params <name>=<value>` on `bundle run` (or the Jobs UI/API) | for that one run |

The workspace itself is not a variable: it comes from your Databricks CLI profile (`-p <profile>`) or
`DATABRICKS_HOST`.

### Bundle variables

Each environment-specific variable is set per target in `databricks.yml` and ships empty on `main`, so
you fill in the targets you deploy to once and a routine deploy needs no `--var`. An empty value fails
closed wherever the value is required.

When the same variable is set in more than one place, the highest of these wins:

1. `--var=<name>=<value>` on `bundle deploy`
2. a `BUNDLE_VAR_<name>` environment variable
3. the git-ignored `.databricks/bundle/<target>/variable-overrides.json`
4. the target's `variables` block in `databricks.yml`
5. the top-level `default` in `databricks.yml`

So a stale `variable-overrides.json` or an exported `BUNDLE_VAR_` variable silently outranks a value
committed to a target block.

Variables declared `type: complex` (the ES host configs, cluster configs, `global_job_tags`,
`support_email`, `diagnostics_job_clusters`) can't be set with `--var`: the CLI rejects it ("setting
variables of complex type via --var flag is not supported"). Set them in `databricks.yml` or override
them in `variable-overrides.json`.

A `--var` on `bundle run` has no effect: a job uses the values from the last `bundle deploy`.

Declare variables and targets in `databricks.yml` only, not in an included resource file. The generator
reads `databricks.yml` alone, so its checks would not see a variable declared elsewhere.

**Read at generation.** A few variables are also read by the generator, which sees only the top-level
`default:` in `databricks.yml`: never a target override, `--var`, `BUNDLE_VAR_` variable or
`variable-overrides.json`. Those entries say so. ([Why](technical-details.md#generation-reads-only-top-level-defaults).)

### Run-time knobs: one precedence rule

Every [run-time knob](#run-time-knobs) resolves the same way, highest first:

1. a per-run `--params <name>=<value>`
2. the feed's own config value
3. the target-wide global `${var.<name>}` in `databricks.yml`
4. the connector, Spark or built-in default

So a knob can be moved for a whole environment from `databricks.yml` alone, overridden for one feed in
its config, and overridden again for a single run. Every value is validated at generation and again at
run, so an invalid one fails closed before any data moves.

Two exceptions:

- In a [job group](feed-configuration.md#job-groups), each member's knobs are baked into its task at
  deploy, so `--params` can't override one member. A run-time override applies job-wide to every task.
- Most globals ship empty, meaning "no target-wide default". Setting a non-empty global applies it to
  every feed that omits the knob, with no way for one feed to opt back out to the connector default.
  This matters most for [`filter_condition`](#filter_condition).

## Environment variables

### `environment`

A value folded into any `catalog` or `schema` in a feed config that contains `${environment}` (for
example `acme_${environment}` becomes `acme_prod`).

- **Set:** per target, or `--var`. Default empty.
- **Takes effect:** when the job runs: the runner folds it in. `bundle deploy` and `validate` don't
  resolve it.
- **Fails closed:** a feed whose names use the token fails at run if the value is empty or produces an
  illegal identifier (one containing a hyphen, for example). It may stay empty if no name uses the token.

### `wheel_path`

The UC Volume path to the `databricks-es-connector` wheel every index job installs. The connector
version lives in the filename.

- **Set:** per target, or `--var` (for example a one-off wheel). Default empty.
- **Takes effect:** at deploy.
- **Required by:** every index job (an empty value fails the run). `deploy_views` doesn't need it.
- **Note:** the wheel is a prerequisite this bundle does not create. The `build_wheel` job can build and
  upload one next to it (see [Environment Setup](setup.md#connector-wheel)); switching to a new version
  means changing `wheel_path` and redeploying.

### `checkpoint_base_path`

The UC Volume base path for streaming checkpoints. The runner appends `/<config_name>`, so each feed
gets its own checkpoint folder.

- **Set:** per target, or `--var`. Default empty.
- **Takes effect:** at deploy.
- **Required by:** streaming runs only (an empty value fails a streaming run). Batch runs and
  `deploy_views` don't use it.
- **Values:** a UC Volume path. Serverless streaming checkpoints can't live on `dbfs:/tmp`.
- **Recommendation:** in `dev`, append `${workspace.current_user.short_name}` so each developer gets
  their own checkpoints (see [Environment Setup](setup.md#per-developer-checkpoints)).

### `cluster_policy_id`

The workspace's cluster policy id, injected into every job cluster (with
`apply_policy_default_values: true`).

- **Set:** per target, or `--var`. Default empty.
- **Required by:** any feed with `job_cluster` compute. An empty value fails the deploy, since the Jobs
  API rejects `policy_id: ""`. Serverless and `existing_cluster` feeds don't use it.
- **Note:** a policy id is workspace-specific. Under `dev`'s development mode each engineer deploys to
  their own workspace, so a committed `dev` value fits one workspace; others pass `--var`.

### `ca_certs`

The UC Volume path to a CA bundle (PEM) the connector uses to verify the ES server's TLS certificate.
One bundle is shared by every host config.

- **Set:** per target, or `--var`. Default empty, meaning use the system CA store.
- **Fails closed:** combined with `verify_certs: false` the connector rejects the run. Pick one.
- **Limitation:** a different CA bundle per host config is not supported.

### `cluster_log_volume_path`

The UC Volume destination for job-cluster log delivery, used only by a job cluster config that opts in
with a `cluster_log_conf` block (see [Operations](operations.md#cluster-log-delivery)).

- **Set:** per target, or `--var`. Default empty (unused).
- **Fails closed:** a config that opts in fails the deploy in any target where this is empty.

### `job_name_prefix`

The prefix in every job's display name, `[<target>] <prefix>: <postfix>` (see
[Job naming](feed-configuration.md#job-naming)).

- **Set:** top level, per target, or `--var`. Default `databricks-elasticsearch-pipelines`.
- **Takes effect:** at deploy, with no regeneration.

### `global_job_tags`

Tags (a map of key to string value) applied to every job the bundle deploys. Job tags are forwarded to
job clusters as cluster tags. See [Job tags](feed-configuration.md#job-tags).

- **Set:** `type: complex`, so in `databricks.yml` or `variable-overrides.json`, not `--var`. Default
  `{}` (no tags).
- **Takes effect:** the fixed maintenance jobs resolve it per target at deploy. Generated jobs bake
  the top-level default **at generation**, so the tag key set for generated jobs comes from the top
  level only; per-target values still work through `${var...}` references.
- **Values:** at most 24 keys (each generated job also gets an `es_index_list` tag, and a job may
  have 25). `es_index_list` is reserved. Quote any value with `${...}`, a colon, or that YAML would read
  as a non-string.

### `schedule_pause_status`

The target-wide pause state for every scheduled and continuous trigger (see
[Scheduling](feed-configuration.md#scheduling)).

- **Set:** top level, per target, or `--var`. Default `PAUSED`; only `prd` sets `UNPAUSED`.
- **Values:** `PAUSED` or `UNPAUSED`.
- **Override:** a feed's own [`pause_status`](#pause_status) wins for that job.

### `support_email`

The email recipients (a list) notified when any job or task run fails. Skipped and canceled runs don't
notify.

- **Set:** `type: complex`, so in `databricks.yml` or `variable-overrides.json`. Default `[]`.
- **Values:** a list such as `["es-oncall@yourco.com"]`. The empty list turns notifications off. Ships
  `[]` everywhere; set a real address in `prd`.

### `monitoring_log_table`

The fully-qualified `catalog.schema.table` of the shared monitoring Delta table (see
[Operations](operations.md#monitoring-log)).

- **Set:** per target, or `--var`. Default empty.
- **Takes effect:** at deploy (a notebook base parameter, not a per-run parameter).
- **Required by:** any run with [`monitoring_log_enabled`](#monitoring_log_enabled) on. Unset,
  malformed, missing or unwritable fails the run.
- **Note:** create the table first with the `log_table_create` job. Export jobs only append, so their
  identity needs `MODIFY` on the table, not `CREATE`.

### `monitoring_log_retention_days`

Days of history the `log_table_prune` job keeps in the monitoring table. It deletes older rows, then
runs `OPTIMIZE` and `VACUUM`.

- **Set:** per target, or `--var`. Default `90`.
- **Values:** a non-negative integer; `0` disables the delete (keep everything).

## Elasticsearch host configs

A host config is a named group of three connection settings, declared once in `databricks.yml` and
selected by name from a feed's [`es_host_config`](#es_host_config). Setup steps, including adding a
second host config, are in [Environment Setup](setup.md#elasticsearch-host-configs).

### `es_host_default`

The host config shipped out of the box. Add more under any name (for example `es_host_secondary`), each
a `type: complex` variable with exactly these three fields:

| Field | What it is |
|---|---|
| `es_host_url` | the Elasticsearch endpoint, for example `https://<host>:9200` |
| `secret_scope_name` | the Databricks secret scope holding the ES api_key |
| `secret_key_name` | the key in that scope whose value is the api_key |

- **Set:** per target (each environment writes to its own cluster). `type: complex`, so not `--var`;
  `variable-overrides.json` works if you'd rather not commit even the endpoints.
- **Fails closed:** a host config left empty for the target fails the run (`missing required
  parameter: es_host_url`).
- **Note:** only the api_key value is secret, and it stays in the secret scope. The endpoint and the
  scope and key names are safe to commit.

### `es_host_configs_json`

Every host config as one JSON object, which the `es_diagnostics` job carries so a run can pick any host
by name. Each entry's fields are `${var.<name>.<field>}` references, which resolve per target at deploy.

- **Set:** once, at the top level of `databricks.yml`. Never per target.
- **Read at generation:** the generator fails closed unless the map lists exactly the declared host
  configs, each entry referencing its own host's three fields, and no target overrides it.
- **Note:** don't override it in `variable-overrides.json`, with `--var` or with `BUNDLE_VAR_`: the
  generator can't see those. A host value containing a quote or backslash breaks the JSON and the
  diagnostics run fails closed.

### `default_es_host_config`

The host config a feed uses when it omits `es_host_config`, and the default host for `es_diagnostics`.

- **Set:** the top-level `default:` only. Default `es_host_default`.
- **Read at generation:** must name a declared host config, and a target override fails generation.
  The default host's values still vary per target, on the host config itself.

## Cluster configs

A cluster config is a `type: complex` variable with a single `cluster_id` field, named by a feed's
`compute.cluster_config` to attach it to an existing cluster. `databricks.yml` ships a commented
`interactive_primary` example; declare as many as you need, with per-target values (a cluster id is
workspace-specific). An empty `cluster_id` fails the deploy, and a `cluster_config` that names no
declared cluster config fails generation. See [Compute](feed-configuration.md#compute).

## Diagnostics compute

The `es_diagnostics` job runs serverless unless one of these is set. Compute is a deploy-time
property, so redeploy after changing them. Set at most one path; setting both fails the deploy.

### `diagnostics_existing_cluster_id`

Run `es_diagnostics` on an already-running cluster, by id. A plain string, so `--var` works. Default
empty (serverless).

### `diagnostics_job_cluster_key`

The `job_cluster_key` (from `diagnostics_job_clusters`) to run `es_diagnostics` on. Default empty.

### `diagnostics_job_clusters`

The whole `job_clusters` list for `es_diagnostics`: a `job_cluster_key` plus a `new_cluster` spec, for
example copied from a `_pipelines/job_cluster_configs/<key>.yml` file (add
`apply_policy_default_values: true` if a policy is enforced). `type: complex`, so set it per target or
in `variable-overrides.json`. Default `[]`.

## Feed definition keys

These live only in `_pipelines/pipeline_configs/<config_name>.yml`. The file's stem becomes the job's
resource key (`index_pipeline_<config_name>`), so it may contain only letters, digits, `_` and `-`, and
may not start with `_`. An unknown key anywhere in the file fails at load. Changes take effect when you
regenerate and deploy.

Object names are fully qualified (`catalog`, `schema`, and a name or table). Only `catalog` and `schema`
may contain `${environment}`; view and table names are plain identifiers.

### `es_index_name`

**Required.** The target Elasticsearch index. Must be a valid ES index name: lowercase, starting with a
letter or digit, then letters, digits, `.`, `-` or `_`, at most 255 bytes, not ending in `.`.

### `es_id_field`

Optional. A column of the view's output, handed to the connector as each document's `_id`.

- **Set it** and a re-run overwrites the same documents: writes are idempotent, and a retried batch or
  restarted stream converges to one document per id.
- **Omit it** and Elasticsearch assigns a random `_id` per document. That needs no setup but is not
  idempotent: both modes are at least once, so a retry or restart can write the same rows again as new
  documents. Duplicates are especially likely for streaming feeds, where restarts and retries are
  routine. ([Why](technical-details.md#delivery-is-at-least-once).)
- **Duplicate values collapse:** if two input rows share an `es_id_field` value they become one
  document, and which one survives is not defined. Make the column unique when you need one document
  per row.
- **Values:** a column identifier. To get random ids you must omit the key; a present but blank or
  `null` value fails the config.
- **Checked:** when `deploy_views` creates the view, it verifies the column is in the view's output.
- Required by [`op_type: create`](#op_type).

### `es_host_config`

Optional. The name of the [host config](#elasticsearch-host-configs) this feed writes to. Omit it to use
[`default_es_host_config`](#default_es_host_config). A name that isn't declared in `databricks.yml`
fails generation.

### `view`

**Required.** The view this feed exports: `catalog`, `schema` and `name`. The view's SQL lives in
`_pipelines/pipeline_views/<name>.sql`, so `name` must equal that file's stem. Several feeds may share
one view. See [Views](feed-configuration.md#views).

### `source`

**Required.** The single source table the view reads: `catalog`, `schema`, `table`, and an optional
`primary_key`.

`primary_key` is informational only: no code reads it. It records the source's unique-row column, and is
separate from `es_id_field` (a column of the view's output), though they often hold the same value.
Streaming feeds are append-only, so there is nothing to deduplicate against a key.
([More](technical-details.md#source-primary-key)).

### `reference_tables`

Optional. One entry per joined (lookup) table, keyed by the alias you use in the SQL as
`${ref_<alias>}`; each entry has `catalog`, `schema` and `table`. See
[Reference tables](feed-configuration.md#reference-tables).

### `compute`

Optional. Where the job runs; omit for serverless. A `type` plus its one key:

| `type` | Key | Runs on |
|---|---|---|
| `serverless` (default) | none | a serverless notebook task |
| `existing_cluster` | `cluster_config` | an existing cluster, named by a [cluster config](#cluster-configs) |
| `job_cluster` | `job_cluster_config` | a job cluster built per run from `_pipelines/job_cluster_configs/<key>.yml` |

An unknown type, a missing key, or a key that doesn't belong to the type fails at load. See
[Compute](feed-configuration.md#compute).

### `schedule`

Optional. A Quartz cron (`quartz_cron_expression`), always in UTC. Omit for an on-demand job. Quartz has
6 or 7 fields (seconds first), so a 5-field Unix cron is rejected. Can't be combined with `continuous`.
See [Scheduling](feed-configuration.md#scheduling).

### `continuous`

Optional. Run the feed always-on instead of on a schedule, with a `trigger_interval` (a Spark interval
with a full-word unit, for example `30 seconds` or `1 minute`). Requires `pipeline_mode: streaming` set
explicitly in the config (not inherited from the global) and classic compute (`job_cluster` or
`existing_cluster`). See [Continuous streaming](feed-configuration.md#continuous-streaming).

### `pause_status`

Optional. `PAUSED` or `UNPAUSED`, overriding [`schedule_pause_status`](#schedule_pause_status) for this
job's trigger. Only valid on a job that has a trigger: a standalone on-demand feed that sets it fails at
load, and so does a job group with no trigger. Within a group, at most one distinct value is allowed.

### `job_group`

Optional. Configs that share a `job_group` value merge into one job, `index_pipeline_group_<job_group>`,
one task per feed. Letters, digits, `_` and `-`, not starting with `_`. See
[Job groups](feed-configuration.md#job-groups).

### `job_name_postfix`

Optional. Replaces the trailing segment of the job's display name (default: the config name, or the
group name). Cosmetic only: it never changes a resource or task key. In a group, set it on at most one
member (conflicting values fail generation). See [Job naming](feed-configuration.md#job-naming).

## Run-time knobs

Each knob below can be set three ways, by the [precedence rule](#run-time-knobs-one-precedence-rule):
the `${var.<name>}` global in `databricks.yml`, the feed config key of the same name, and
`--params <name>=<value>`. "Default" is what applies when all three are unset. For how the write knobs
interact, see [Throughput Tuning](throughput.md).

### `pipeline_mode`

`batch` re-exports the whole view each run; `streaming` reads only new source commits since the last run.
Global default `batch`. A continuous feed must set `streaming` in its own config. See
[Batch or streaming](feed-configuration.md#batch-or-streaming).

### `filter_condition`

A Spark SQL predicate applied to the view's rows before writing, for example `"action = 'allowed'"`.
Default empty (no filter). A malformed predicate fails the run when Spark applies it.

Leave the global empty unless every feed that omits the key really should carry that filter.

### `streaming_start`

Where a streaming feed starts on its first run: `new` (only commits after the stream starts; batch mode
owns the history) or `full` (backfill the whole table first). Global default `new`.

It is honored only on the first run. Once a checkpoint exists, the checkpoint is the position of record,
so changing this has no effect until the checkpoint is
[cleared](operations.md#resetting-a-checkpoint). On a `new` start the source's current commit is
included, so if it is an append its rows are sent once more; with `es_id_field` set that overwrites the
same documents.

### `chunk_size`

Documents per bulk request. A positive integer. Connector default `500`.

Larger chunks mean fewer, larger requests: each takes longer and a failure re-sends more. A request
also can't hold more rows than one Spark Arrow batch, so beyond that size raising it changes nothing.
See [Throughput Tuning](throughput.md#chunk_size).

### `write_concurrency`

Bulk requests in flight at once within each write partition. A positive integer. Connector default `1`.

Raise it when the write is waiting on round trips (executors idle, ES not busy). It multiplies with the
number of partitions running at once, so raise it gradually and watch for 429s. The connection pool is
sized to it automatically. See [Throughput Tuning](throughput.md#write_concurrency).

### `request_timeout`

The per-request ES client timeout, in seconds. A positive integer. Connector default `60`.

Raise it (together with a smaller `chunk_size`) when bulk sends time out (`ConnectionTimeout ... The
write operation timed out`).

### `transport_max_retries`

How many times the ES client re-sends a whole request after a transport failure (a connection reset or
timeout, or a 429 or 503 on the bulk call itself). A non-negative integer; `0` disables. Connector
default `3`.

It never sees a single document rejected inside a successful response; that is
[`max_retries_per_doc`](#max_retries_per_doc).

### `max_retries_per_doc`

How many times the connector re-sends an individual document that ES rejects with a 429 (the write
queue is full), with exponential backoff. A non-negative integer; `0` disables. Connector default `3`.

Reach for this, not `transport_max_retries`, when a feed sees 429s under load. Raising it far
multiplies backoff time within the partition; the real ceiling is ES's write capacity. The connector
warns if this is `0` while `transport_max_retries` is above 3, since a 429'd document would then get no
retry at either layer.

### `retry_transport_timeout`

`true` makes the connector own whole-request timeout retries: a bulk send that exceeds `request_timeout`
is re-sent with backoff (up to `transport_max_retries` times) instead of the ES client re-sending it
silently and then failing the batch. Default off.

It only changes timeout handling; connection resets and 429 or 503 responses are retried as before.
With `bulk_stats` on, the cost shows as `timeout_sends` and `timeout_wait_ms`. Requires connector
0.9.7 or later (ignored with a warning on older wheels).

### `op_type`

The bulk action: `index` (overwrite by `_id`) or `create` (append only). The global `${var.op_type}`
ships empty, which means the connector default, `index`, so a feed opts into `create` in its own config
unless a target sets the global.

With `create`, re-sending an existing `_id` returns a 409 the connector treats as already written, so
the document is neither overwritten nor duplicated. Use it only for feeds that never legitimately update
a document, since a real update is silently dropped. It requires [`es_id_field`](#es_id_field): a config
that sets `create` without one fails at load, and `create` arriving through the global or `--params` on
a feed without one fails the run before any document is sent (connector 0.10.0 or later rejects it with
`op_type='create' requires id_field ...`; an older wheel ignores `op_type` altogether).

Under the default fast path, a chunk mixing new and already-indexed `_id`s can count a new document as
deduplicated instead of written. The data is correct; only the count is off.
[`bypass_fast_path`](#bypass_fast_path) makes the counts exact.
([Why](technical-details.md#op_type-create-miscount).) Requires connector 0.10.0 or later.

### `bypass_fast_path`

`true` skips the connector's fast path (an `errors`-only response probe) and checks every document's
result on the first send. Counts become exact for `op_type: create`, at some cost to throughput on clean
chunks. Default off. Requires connector 0.10.0 or later.

### `require_existing_index`

`true` checks that the ES index exists before writing and fails if it doesn't; `false` skips the check,
so ES auto-creates a missing index on first write (unless the cluster disables auto-creation).
Connector default `true`.

Keep it on unless the feed deliberately relies on auto-creation or an index template: with it off, a
mistyped `es_index_name` silently creates a new, dynamically mapped index and the run still reports
every document written.

### `verify_certs`

`true` verifies the ES server's TLS certificate; `false` skips verification (for a self-signed test
endpoint, for example). Connector default `true`. `false` can't be combined with
[`ca_certs`](#ca_certs).

### `bulk_stats`

`true` logs per-partition bulk-send diagnostics under the `BULK_STATS` tag: docs per request, round-trip
and ES time, straggler partitions. Default off. Turn it on when tuning or investigating a slow feed; see
[Throughput Tuning](throughput.md#reading-the-signals) for how to read it. Requires connector 0.9.3 or
later.

### `monitoring_log_enabled`

`true` also records every run and batch in the [monitoring table](#monitoring_log_table). Default off.

When on, the log is required: a missing table or any failed append fails the task, so no data is sent
without a record. See [Operations](operations.md#monitoring-log).

### `max_partition_bytes`

Sets `spark.sql.files.maxPartitionBytes` for the source read: smaller values split the read into more,
smaller partitions, which carry through the view to the write. A Spark byte size (`2m`, `32m`) or a
byte count; `0` leaves the engine default. Built-in default `2m`.

A good target is about 2 to 3 times total worker cores in partitions. See
[Throughput Tuning](throughput.md#partitions).

### `write_repartition`

Repartitions the write input into this many partitions before the ES write. A non-negative integer;
`0` (built-in default) turns it off.

Usually unnecessary, because `max_partition_bytes` already sets the partitioning that reaches the
write. Set it when the view shuffles (a non-broadcast join, `GROUP BY`, `DISTINCT`, a window), which
resets the partition count, or when large source files read into too few partitions. It adds a shuffle.

### `max_files_per_trigger`

Streaming only: the most Delta files one micro-batch reads. A positive integer. Spark default 1000.
Use it to bound a backfill or a large catch-up so one micro-batch doesn't read the whole table.

### `max_bytes_per_trigger`

Streaming only: a soft cap on bytes per micro-batch (Delta always admits at least one file). A Spark
byte size such as `128m`. Default: no cap.

## Maintenance job parameters

These are `--params` on the fixed maintenance jobs. Each job also receives some of the bundle variables
above at deploy.

**`checkpoint_clear`** ([Resetting a checkpoint](operations.md#resetting-a-checkpoint))

| Parameter | Default | What it does |
|---|---|---|
| `config_name` | none (required) | the feed whose checkpoint (`<checkpoint_base_path>/<config_name>`) to delete. A blank or path-bearing value fails |

**`es_diagnostics`** ([Diagnosing an Elasticsearch host](operations.md#diagnosing-an-elasticsearch-host))

| Parameter | Default | What it does |
|---|---|---|
| `es_host_config` | `default_es_host_config` | which declared host config to check, by name. An unknown name fails |
| `index_name` | blank | also report on this one index; blank checks only the cluster and nodes |
| `verify_certs` | `true` | `false` for a self-signed endpoint; ignored when `ca_certs` is set |
| `sample_interval_secs` | `5` | seconds between the two samples; `0` takes one snapshot (it can't report `HEALTHY` then) |
| `request_timeout_secs` | `15` | per-request timeout |

**`build_wheel`** ([Environment Setup](setup.md#connector-wheel))

| Parameter | Default | What it does |
|---|---|---|
| `repo_workspace_path` | none (required) | the `/Workspace/...` path of the connector repo to build (the folder holding its `pyproject.toml`) |
| `wheel_version` | none (required) | the version to build; must match the repo's `pyproject.toml` |

`deploy_views`, `log_table_create` and `log_table_prune` take no run-time parameters.

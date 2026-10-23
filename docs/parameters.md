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
- [Summary table](#summary-table)

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

Each environment-specific variable is set per target in `databricks.yml`. Fill in the targets you
deploy to once, and a routine deploy needs no `--var`. An empty value fails closed wherever the value
is required.

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
`variable-overrides.json`. Each of those entries below has a **Read at generation** line
([why](technical-details.md#generation-reads-only-top-level-defaults)).

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
- An empty global means "no target-wide default". Setting a non-empty global applies it to every feed
  that omits the knob, with no way for one feed to opt back out to the connector default. This matters
  most for [`filter_condition`](#filter_condition).

## Environment variables

### `environment`

A value folded into any `catalog` or `schema` in a feed config that contains `${environment}` (for
example `acme_${environment}` becomes `acme_prod`).

- **Set:** per target, or `--var`. Default empty.
- **Takes effect:** when the job runs: the runner folds it in. `bundle deploy` and `validate` don't
  resolve it.
- **Fails closed:** a feed whose names use the token fails at run if the value is empty or produces an
  illegal identifier (one containing a hyphen, for example). It may stay empty if no name uses the token.

### `view_schema_per_user`

`true` puts every view in a per-developer schema, `<view schema>_<deploying user's short name>` (for
example `es_poc` becomes `es_poc_jane_doe`), so developers sharing a workspace don't overwrite each
other's views (see [Environment Setup](setup.md#per-developer-views)). `deploy_views` creates the schema
if it's missing and every feed job reads its view from there. Source and reference tables are unchanged.

- **Set:** top level, per target, or `--var`. Default `false`; the `dev` target sets `true`.
- **Takes effect:** at deploy. `--var view_schema_per_user=false` turns it off for one deploy, to test
  against the real view schema.
- **Values:** `true` or `false`. Anything else fails the run.
- **Fails closed:** with it on, an empty or illegal user name fails `deploy_views` and feed runs instead
  of falling back to the shared schema.
- **Needs:** `CREATE SCHEMA` on each view catalog for the `deploy_views` identity.

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

The workspace's cluster policy id, injected into every feed's job cluster (with
`apply_policy_default_values: true`).

- **Set:** per target, or `--var`. Default empty.
- **Required by:** any feed with `job_cluster` compute. An empty value fails the deploy, since the Jobs
  API rejects `policy_id: ""`. Serverless and `existing_cluster` feeds don't use it.
- **Note:** a policy id is workspace-specific, so set it in each target for that target's workspace.

### `ca_certs`

The UC Volume path to a CA bundle (PEM) the connector uses to verify the ES server's TLS certificate.
One bundle is shared by every host config.

- **Set:** per target, or `--var`. Default empty, meaning use the system CA store.
- **Fails closed:** combined with `verify_certs: false` the connector rejects the run. Pick one.
- **Limitation:** a different CA bundle per host config is not supported.

### `cluster_log_volume_path`

The UC Volume destination for job-cluster log delivery, used only by a job cluster config that opts in
with a `cluster_log_conf` block (see [Logging and Monitoring](monitoring.md#cluster-log-delivery)).

- **Set:** per target, or `--var`. Default empty (unused).
- **Fails closed:** a config that opts in fails the deploy in any target where this is empty.
- **Cleanup:** the `cluster_log_prune` job deletes logs older than
  [`cluster_log_retention_days`](#cluster_log_retention_days) from this path.

### `cluster_log_retention_days`

Days of delivered cluster logs the `cluster_log_prune` job keeps under
[`cluster_log_volume_path`](#cluster_log_volume_path). It deletes older files, then the directories that
leaves empty ([Pruning cluster logs](maintenance-jobs.md#pruning-cluster-logs)).

- **Set:** per target, or `--var`. Default `30`.
- **Takes effect:** at deploy.
- **Values:** an integer; `0` (or negative) disables the delete. A non-integer, or a number of days that
  reaches back before 1970 (so nothing could ever be deleted), fails the run.

### `job_name_prefix`

The prefix in every job's display name, `[<target>] <prefix>: <postfix>` (see
[Job naming](feed-configuration.md#job-naming)).

- **Set:** top level, per target, or `--var`.
- **Takes effect:** at deploy, with no regeneration.
- **Recommendation:** set it once at the top level to a name that identifies these jobs in your
  workspace (a team or product name), so every job sorts and filters together in the Jobs UI.

### `global_job_tags`

Tags (a map of key to string value) applied to every job the bundle deploys. Job tags are forwarded to
job clusters as cluster tags. See [Job tags](feed-configuration.md#job-tags).

- **Set:** `type: complex`, so in `databricks.yml` or `variable-overrides.json`, not `--var`. Default
  `{}` (no tags).
- **Read at generation:** the fixed maintenance jobs resolve it per target at deploy. Generated jobs bake
  the top-level default **at generation**, so the tag key set for generated jobs comes from the top
  level only; per-target values still work through `${var...}` references.
- **Values:** at most 24 keys (each generated job also gets an `es_index_list` tag, and a job may
  have 25). `es_index_list` is reserved. Quote any value with `${...}`, a colon, or that YAML would read
  as a non-string.

### `schedule_pause_status`

The target-wide pause state for every scheduled and continuous trigger (see
[Pausing triggers](feed-configuration.md#pausing-triggers)).

- **Set:** top level, per target, or `--var`.
- **Values:** `PAUSED` or `UNPAUSED`.
- **Recommendation:** `PAUSED` at the top level, so a target runs its triggers only when it opts in,
  and `UNPAUSED` in the targets where scheduled and continuous jobs should run (usually only
  production).
- **Override:** a feed's own [`pause_status`](#pause_status) wins for that job.

### `support_email`

The email recipients (a list) notified when any job or task run fails. Skipped and canceled runs don't
notify.

- **Set:** per target. `type: complex`, so in `databricks.yml` or `variable-overrides.json`.
- **Values:** a list such as `["es-oncall@yourco.com"]`. The empty list `[]` turns notifications off.
- **Recommendation:** a real address in the targets that should alert someone (usually production), and
  `[]` in the rest.

### `monitoring_log_table`

The fully-qualified `catalog.schema.table` of the shared monitoring Delta table (see
[Logging and Monitoring](monitoring.md#monitoring-log)).

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
`compute.cluster_config` to attach it to an existing cluster. Declare as many as you need, for example
`interactive_primary`, with per-target values (a cluster id is workspace-specific). An empty `cluster_id` fails the deploy, and a `cluster_config` that names no
declared cluster config fails generation. See [Compute](feed-configuration.md#compute).

## Diagnostics compute

The `es_diagnostics` job (a work in progress; see
[Diagnosing an Elasticsearch host](maintenance-jobs.md#diagnosing-an-elasticsearch-host)) runs
serverless unless one of these is set. Compute is a deploy-time
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
- **With [`op_type: create`](#op_type):** required. A document whose `_id` is already in the index
  returns a 409. The connector counts it as `ignored` (and as `docs_deduped` in `bulk_stats`), not as an
  error, and leaves the existing document unchanged. Expect 409s on every re-send under `create`: a
  retried batch, a restarted stream, or a backfill over rows already sent.

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
See [CRON scheduling](feed-configuration.md#cron-scheduling).

### `continuous`

Optional. Run the feed always-on instead of on a schedule, with a `trigger_interval` (a Spark interval
with a full-word unit, for example `30 seconds` or `1 minute`). Requires `pipeline_mode: streaming` set
explicitly in the config (not inherited from the global) and classic compute (`job_cluster` or
`existing_cluster`). See [Continuous streaming](feed-configuration.md#continuous-streaming).

### `pause_status`

Optional. `PAUSED` or `UNPAUSED`, overriding [`schedule_pause_status`](#schedule_pause_status) for this
job's trigger. Only valid on a job that has a trigger: a standalone on-demand feed that sets it fails at
load, and so does a job group with no trigger. Within a group, set it on one member or the same value on
several; different values fail generation.

### `job_group`

Optional. Configs that share a `job_group` value merge into one job, `index_pipeline_group_<job_group>`,
one task per feed. Letters, digits, `_` and `-`, not starting with `_`. See
[Job groups](feed-configuration.md#job-groups).

### `job_name_postfix`

Optional. Replaces the trailing segment of the job's display name (default: the config name, or the
group name). Cosmetic only: it never changes a resource or task key. In a group, set it on one member
or the same value on several; different values fail generation. See
[Job naming](feed-configuration.md#job-naming).

## Run-time knobs

Each knob below can be set three ways, by the [precedence rule](#run-time-knobs-one-precedence-rule):
the `${var.<name>}` global in `databricks.yml`, the feed config key of the same name, and
`--params <name>=<value>`. "Default" is what applies when all three are unset. For how the write knobs
interact, see [Throughput Tuning](throughput.md).

### `pipeline_mode`

`batch` re-exports the whole view each run; `streaming` reads only new source commits since the last run.
There is no built-in default: if the global, the feed and `--params` all leave it empty, the run fails.
Set the global to the mode most of your feeds use. A continuous feed must set `streaming` in its own
config. See
[Batch or streaming](feed-configuration.md#batch-or-streaming).

### `filter_condition`

A Spark SQL predicate applied to the view's rows before writing, for example `"action = 'allowed'"`.
Default empty (no filter). A malformed predicate fails the run when Spark applies it.

Leave the global empty unless every feed that omits the key really should carry that filter.

### `streaming_start`

Where a streaming feed starts on its first run: `new` (only commits after the stream starts; batch mode
owns the history) or `full` (backfill the whole table first). Built-in default `new`.

It is honored only on the first run. Once a checkpoint exists, the checkpoint is the position of record,
so changing this has no effect unless the checkpoint is
[cleared](maintenance-jobs.md#resetting-a-checkpoint). A `new` start normally sends nothing that is
already in the table: it records the source's current version in the checkpoint without exporting it.
In one rare fallback (the runner can't tell whether a checkpoint exists) it sends the current commit's
rows once ([how](technical-details.md#inclusive-start-version)).

### `chunk_size`

Documents per bulk request. A positive integer. Connector default `500`.

Larger chunks mean fewer, larger requests: each takes longer and a failure re-sends more. A request
also can't hold more rows than one Spark Arrow batch, so beyond that size raising it changes nothing.
See [Throughput Tuning](throughput.md#chunk_size).

### `write_concurrency`

Bulk requests in flight at once within each write partition. A positive integer. Connector default `1`.

Raise it when the write is waiting on round trips (executors idle, ES not busy). It multiplies with the
number of partitions running at once, so raise it gradually and watch for 429s. The threads share one
Python GIL per partition, so past a point more threads wait instead of sending
([Python threads and the GIL](throughput.md#python-threads-and-the-gil)). The connection pool is
sized to it automatically. See [Throughput Tuning](throughput.md#write_concurrency).

### `request_timeout`

The per-request ES client timeout, in seconds. A positive integer. Connector default `60`.

When bulk sends time out (`ConnectionTimeout ... The write operation timed out`), first find out why
before raising it: an overloaded ES host needs less pressure, not a longer wait, and a longer timeout
holds a thread on a request that may never return. Raise it when ES is healthy but large requests
legitimately take longer. See [Throughput Tuning](throughput.md#timeouts-and-retries).

### `transport_max_retries`

How many times the ES client re-sends a whole request after a transport failure (a connection reset or
timeout, or a 429 or 503 on the bulk call itself). A non-negative integer; `0` disables. Connector
default `3`.

It retries only whole requests that fail. When a request succeeds but ES rejects some of the documents
in it (per-document 429s in the response), those documents are retried by
[`max_retries_per_doc`](#max_retries_per_doc) instead.

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
With `bulk_stats` on, the cost shows as `timeout_sends` and `timeout_wait_ms`.

**Recommendation:** turn it on for every feed (set the global `${var.retry_transport_timeout}` to
`true`). It changes nothing until a request times out, and then makes the timeouts and the time spent
retrying them visible. ([Why it exists](technical-details.md#retry-transport-timeout).)

### `op_type`

The bulk action: `index` (overwrite by `_id`) or `create` (append only). An empty global
`${var.op_type}` means the connector default, `index`, so each feed that should append only sets
`create` in its own config.

With `create`, re-sending an existing `_id` returns a 409 the connector treats as already written, so
the document is neither overwritten nor duplicated. Use it only for feeds that never legitimately update
a document, since a real update is silently dropped. It requires [`es_id_field`](#es_id_field): a config
that sets `create` without one fails at load, and `create` arriving through the global or `--params` on
a feed without one fails the run before any document is sent (`op_type='create' requires id_field
...`).

Under the default fast path, a chunk mixing new and already-indexed `_id`s can count a new document as
deduplicated instead of written. The data is correct; only the count is off.
[`bypass_fast_path`](#bypass_fast_path) makes the counts exact.
([Why](technical-details.md#op_type-create-miscount).)

### `bypass_fast_path`

`true` skips the connector's fast path and checks every document's result on the first send. Default
off.

Under the fast path, each chunk is sent with a request for only the response's `errors` flag. A clean
chunk is done in one send with very little to decode. A chunk with any document-level error (a 409, a
429, a mapping error) is then sent again in full to get each document's result. So:

- **It helps** a feed where document-level errors are routine, above all `op_type: create` with
  re-sends, where most chunks carry 409s: the fast path sends each of those chunks twice, and bypassing
  it sends them once. It also makes the `create` counts exact, and avoids the second send duplicating a
  chunk's good documents when the feed has no `es_id_field`.
- **It costs** on a feed whose chunks are almost always clean: every response then carries and decodes
  a result per document, which takes more time for no benefit. That decoding is Python work under the
  GIL, so it also lowers the `write_concurrency` a partition can use
  ([Python threads and the GIL](throughput.md#python-threads-and-the-gil)).

Turn on [`bulk_stats`](#bulk_stats) to see which case a feed is in.

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
[Logging and Monitoring](monitoring.md#reading-the-bulk_stats-output) for how to read it.

### `monitoring_log_enabled`

`true` also records every run and batch in the [monitoring table](#monitoring_log_table). Default off.

When on, the log is required: a missing table or any failed append fails the task, so no data is sent
without a record. See [Logging and Monitoring](monitoring.md#monitoring-log).

### `log_monitor_source_lookback_seconds`

Streaming with the monitoring log on: how far back, in seconds, each batch's source version read looks. A
integer from 0 to 86400 (a day); built-in default `60`. Each batch records the newest source commit made at least
this long before the read (`source_latest_version`), so a commit that lands between Spark planning the
batch and the read doesn't make a caught-up batch look [behind](monitoring.md#caught-up-or-behind).
The trade-off: data committed within this window just before a batch doesn't count as left behind, so
keep it short next to how far behind matters to you. `0` records the newest commit and reads only one
history row, whatever [`log_monitor_source_history_rows`](#log_monitor_source_history_rows) is set to.

### `log_monitor_source_history_rows`

Streaming with the monitoring log on: how many of the source's newest history rows that read fetches. A
integer from 1 to 10000; built-in default `100`. If none of them is older than the look-back (the source
committed more often than this within the window), the oldest one fetched is used, so a caught-up
batch can again read as behind. Larger values make the read slower
([cost](technical-details.md#source-version-read-cost)).

### `max_partition_bytes`

Sets `spark.sql.files.maxPartitionBytes` for the source read: smaller values split the read into more,
smaller partitions, which carry through the view to the write. A Spark byte size (`2m`, `32m`) or a
byte count; `0` leaves the engine default. Built-in default `2m`.

A good starting target is about 1x to 3x the total worker cores in partitions. See
[Throughput Tuning](throughput.md#partitions).

### `write_repartition`

Repartitions the write input into this many partitions before the ES write. A non-negative integer;
`0` (built-in default) turns it off.

Set it per feed: the right value depends on the feed's data and its cluster. It adds a shuffle, and in
return gives the write a set number of evenly sized partitions. See
[Throughput Tuning](throughput.md#partitions) for how to choose it.

### `max_files_per_trigger`

Streaming only: the most Delta files one micro-batch reads. A positive integer. Spark default 1000.
Use it to bound a backfill or a large catch-up so one micro-batch doesn't read the whole table.

### `max_bytes_per_trigger`

Streaming only: a soft cap on bytes per micro-batch (Delta always admits at least one file). A Spark
byte size such as `128m`. Default: no cap.

## Maintenance job parameters

These are `--params` on the fixed maintenance jobs. Each job also receives some of the bundle variables
above at deploy.

**`checkpoint_clear`** ([Resetting a checkpoint](maintenance-jobs.md#resetting-a-checkpoint))

| Parameter | Default | What it does |
|---|---|---|
| `config_name` | none (required) | the feed whose checkpoint (`<checkpoint_base_path>/<config_name>`) to delete. A blank or path-bearing value fails |

**`es_diagnostics`** ([Diagnosing an Elasticsearch host](maintenance-jobs.md#diagnosing-an-elasticsearch-host))

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

**`deploy_views`** ([Deploying some of the views](maintenance-jobs.md#deploying-some-of-the-views))

| Parameter | Default | What it does |
|---|---|---|
| `views` | blank (every view) | deploy only these views: a comma-separated list of view names (the `.sql` file names without the extension). An unknown name or an empty entry fails before anything deploys. At most 9,988 characters, or the run fails |

The length limit comes from the Jobs API, which caps the JSON form of all the job parameters passed to
one run at 10,000 characters and refuses to start a longer run ("The provided job parameters (in json
representation) are too long"). `{"views":""}` uses 12 of those. So "every view" is the blank default,
never a typed-out list; a list long enough to hit the limit has to be split across runs.

**`cluster_log_prune`** ([Pruning cluster logs](maintenance-jobs.md#pruning-cluster-logs))

| Parameter | Default | What it does |
|---|---|---|
| `dry_run` | `false` | `true` lists what the run would delete (files, bytes, directories) and deletes nothing |

`log_table_create` and `log_table_prune` take no run-time parameters.

## Summary table

Every parameter on this page, with where it is set and what it does. Follow a parameter's link for its
default, what happens when it is left empty, and what to weigh when you change it. "Bundle"
means a `databricks.yml` variable, "Config" a feed config key, "Knob" a run-time knob (bundle global,
feed config, or `--params`), and "Job" a maintenance job's `--params`.

| Parameter | Kind | Set in | Description |
|---|---|---|---|
| [`environment`](#environment) | Bundle | per target | the value substituted for `${environment}` in feed catalog and schema names |
| [`view_schema_per_user`](#view_schema_per_user) | Bundle | top level, per target, `--var` | puts each developer's views in `<view schema>_<short name>` (on in `dev`) |
| [`wheel_path`](#wheel_path) | Bundle | per target | the UC Volume path of the connector wheel every index job installs |
| [`checkpoint_base_path`](#checkpoint_base_path) | Bundle | per target | the UC Volume base path for streaming checkpoints, one folder per feed |
| [`cluster_policy_id`](#cluster_policy_id) | Bundle | per target | the cluster policy applied to every feed's job cluster |
| [`ca_certs`](#ca_certs) | Bundle | per target | a CA bundle for verifying the ES server's TLS certificate |
| [`cluster_log_volume_path`](#cluster_log_volume_path) | Bundle | per target | where job-cluster logs are delivered, for cluster configs that opt in |
| [`cluster_log_retention_days`](#cluster_log_retention_days) | Bundle | per target | how many days of delivered cluster logs `cluster_log_prune` keeps |
| [`job_name_prefix`](#job_name_prefix) | Bundle | top level or per target | the prefix in every job's display name |
| [`global_job_tags`](#global_job_tags) | Bundle | top level (keys), values may vary | tags applied to every job and its job clusters |
| [`schedule_pause_status`](#schedule_pause_status) | Bundle | top level and per target | the target-wide pause state of scheduled and continuous triggers |
| [`support_email`](#support_email) | Bundle | per target | who is emailed when a job or task run fails |
| [`monitoring_log_table`](#monitoring_log_table) | Bundle | per target | the shared monitoring Delta table that runs and batches are logged to |
| [`monitoring_log_retention_days`](#monitoring_log_retention_days) | Bundle | per target | how many days of history `log_table_prune` keeps |
| [`es_host_default`](#es_host_default) and other host configs | Bundle | per target | an ES endpoint plus the secret scope and key holding its api_key |
| [`es_host_configs_json`](#es_host_configs_json) | Bundle | top level only | every host config in one JSON object, so `es_diagnostics` can check any host |
| [`default_es_host_config`](#default_es_host_config) | Bundle | top level only | the host config a feed uses when it omits `es_host_config` |
| [Cluster configs](#cluster-configs) | Bundle | per target | an existing cluster's id, which a feed's `compute` can name |
| [`diagnostics_existing_cluster_id`](#diagnostics_existing_cluster_id) | Bundle | per target | runs `es_diagnostics` on an existing cluster |
| [`diagnostics_job_cluster_key`](#diagnostics_job_cluster_key) | Bundle | per target | runs `es_diagnostics` on this job cluster from `diagnostics_job_clusters` |
| [`diagnostics_job_clusters`](#diagnostics_job_clusters) | Bundle | per target | the job cluster specs `es_diagnostics` can run on |
| [`es_index_name`](#es_index_name) | Config | feed config | the Elasticsearch index the feed writes to |
| [`es_id_field`](#es_id_field) | Config | feed config | the view column used as each document's `_id` |
| [`es_host_config`](#es_host_config) | Config | feed config | which host config the feed writes to |
| [`view`](#view) | Config | feed config | the view the feed exports |
| [`source`](#source) | Config | feed config | the source table the view reads |
| [`reference_tables`](#reference_tables) | Config | feed config | lookup tables the view joins |
| [`compute`](#compute) | Config | feed config | where the job runs: serverless, an existing cluster, or a job cluster |
| [`schedule`](#schedule) | Config | feed config | a Quartz cron schedule, in UTC |
| [`continuous`](#continuous) | Config | feed config | runs the feed always-on at a trigger interval |
| [`pause_status`](#pause_status) | Config | feed config | this job's pause state, overriding `schedule_pause_status` |
| [`job_group`](#job_group) | Config | feed config | merges feeds into one job, with a task per feed |
| [`job_name_postfix`](#job_name_postfix) | Config | feed config | replaces the last part of the job's display name |
| [`pipeline_mode`](#pipeline_mode) | Knob | global, config, `--params` | `batch` (re-export the whole view) or `streaming` (only new source commits) |
| [`filter_condition`](#filter_condition) | Knob | global, config, `--params` | a SQL predicate that filters rows before they are written |
| [`streaming_start`](#streaming_start) | Knob | global, config, `--params` | where a streaming feed's first run starts: `new` or `full` |
| [`chunk_size`](#chunk_size) | Knob | global, config, `--params` | documents per bulk request |
| [`write_concurrency`](#write_concurrency) | Knob | global, config, `--params` | bulk requests in flight at once per write partition |
| [`request_timeout`](#request_timeout) | Knob | global, config, `--params` | the per-request ES client timeout, in seconds |
| [`transport_max_retries`](#transport_max_retries) | Knob | global, config, `--params` | retries of a whole bulk request after a transport failure |
| [`max_retries_per_doc`](#max_retries_per_doc) | Knob | global, config, `--params` | retries of a single document ES rejects with a 429 |
| [`retry_transport_timeout`](#retry_transport_timeout) | Knob | global, config, `--params` | has the connector retry timed-out bulk requests itself, with backoff |
| [`op_type`](#op_type) | Knob | global, config, `--params` | `index` (overwrite by `_id`) or `create` (append only) |
| [`bypass_fast_path`](#bypass_fast_path) | Knob | global, config, `--params` | checks every document's result on the first send |
| [`require_existing_index`](#require_existing_index) | Knob | global, config, `--params` | fails the run if the ES index doesn't exist, instead of letting ES create it |
| [`verify_certs`](#verify_certs) | Knob | global, config, `--params` | verifies the ES server's TLS certificate |
| [`bulk_stats`](#bulk_stats) | Knob | global, config, `--params` | logs per-partition bulk-send diagnostics |
| [`monitoring_log_enabled`](#monitoring_log_enabled) | Knob | global, config, `--params` | records every run and batch in the monitoring table |
| [`log_monitor_source_lookback_seconds`](#log_monitor_source_lookback_seconds) | Knob | global, config, `--params` | how far back each streaming batch's source version read looks |
| [`log_monitor_source_history_rows`](#log_monitor_source_history_rows) | Knob | global, config, `--params` | how many source history rows that read fetches |
| [`max_partition_bytes`](#max_partition_bytes) | Knob | global, config, `--params` | the source read's partition size, which sets how many partitions the write gets |
| [`write_repartition`](#write_repartition) | Knob | global, config, `--params` | repartitions the write input into this many partitions |
| [`max_files_per_trigger`](#max_files_per_trigger) | Knob | global, config, `--params` | the most Delta files one streaming micro-batch reads |
| [`max_bytes_per_trigger`](#max_bytes_per_trigger) | Knob | global, config, `--params` | a soft cap on bytes per streaming micro-batch |
| [`config_name`](#maintenance-job-parameters) | Job | `checkpoint_clear` | the feed whose checkpoint to delete |
| [`es_host_config`, `index_name`, `verify_certs`, `sample_interval_secs`, `request_timeout_secs`](#maintenance-job-parameters) | Job | `es_diagnostics` | which host (and optionally index) to check, and how to sample it |
| [`repo_workspace_path`, `wheel_version`](#maintenance-job-parameters) | Job | `build_wheel` | the connector repo to build and the version to build |
| [`views`](#maintenance-job-parameters) | Job | `deploy_views` | deploy only these views (blank deploys every view) |
| [`dry_run`](#maintenance-job-parameters) | Job | `cluster_log_prune` | list what would be deleted without deleting |

# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/). It uses the
[**databricks-es-connector**](https://github.com/tim-clifford/es-databricks-connector) library for the
transfer.

## Overview

Every Elasticsearch index is fed by its own pipeline, and each pipeline is described by two files:

- a view `views/<view_name>.sql` defining what gets exported, and
- a config file `pipeline_definitions/<config_name>.yml` that points a pipeline at that view and says where
  its view, source table, and any reference (join) tables live.

The bundle deploys:

- **One `deploy_views` job**: creates or replaces one Databricks view per index. Each view is a
  `.sql` file in [`views/`](views/). The job renders the catalog/schema parameters and runs every
  file with `spark.sql`.
- **One job per index** (`index_pipeline_<config_name>`): all run the same shared notebook
  [`notebooks/run_index_pipeline.py`](notebooks/run_index_pipeline.py) with that index's config. These
  job resources are **generated** by [`scripts/gen_jobs.py`](scripts/gen_jobs.py) from the config
  files (see [Adding a new pipeline for an ES index](#adding-a-new-pipeline-for-an-es-index)).

## Adding a new pipeline for an ES index

1. Add `views/<view_name>.sql`.
2. Add `pipeline_definitions/<config_name>.yml` (see the schema under [Configuration](#configuration)). The
   view's filename must match the config's `view.name`.
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

Each `.sql` file in `views/` defines one view. The filename matches the view it creates
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
or `DATABRICKS_HOST`. The bundle variables are:

| Variable | What it sets |
|---|---|
| `environment` | folded into any config name containing `${environment}` (e.g. `ocsf_${environment}` -> `ocsf_prod`); may be empty when no name uses the token |
| `wheel_path` | UC Volume path to the `databricks-es-connector` wheel each **index job** installs (the connector version lives here, in the wheel filename); a global prerequisite, not created by this bundle (see [the connector repo](https://github.com/tim-clifford/es-databricks-connector) for building/uploading it). Defaults to empty; supply it on `bundle deploy` (or set a real default in your fork). An index job deployed with an empty `wheel_path` fails closed at run; `deploy_views` doesn't need it |
| `es_host_url` | the Elasticsearch endpoint every index job writes to, e.g. `https://<host>:9200` |
| `secret_scope_name` | the Databricks [secret scope](https://docs.databricks.com/security/secrets/) holding the ES **api_key** |
| `secret_key_name` | the key within that scope whose value is the ES **api_key** the connector authenticates with |
| `checkpoint_base_path` | UC Volume base path for **streaming** checkpoints; the runner appends `/<config_name>` so each stream gets its own subfolder. Required for a streaming run (fails closed if empty); unused by batch and `deploy_views` |

`es_host_url`, `secret_scope_name`, and `secret_key_name` are the global ES connection settings,
shared by every index job (the auth secret is an api_key, not a username/password). Like
`wheel_path`, they default to empty and are baked in at deploy; an index job run with any of them
empty fails closed, and `deploy_views` doesn't need them. `checkpoint_base_path` is the same shape
(global, deploy-time, empty default) but only a **streaming** run requires it; it must be a UC Volume
path (serverless streaming checkpoints can't live on `dbfs:/tmp`).

Everything else is per-pipeline and lives in `pipeline_definitions/<config_name>.yml`. Each object is fully
qualified (`catalog`, `schema`, and a name/table). Only `catalog` and `schema` may embed
`${environment}`; the view name and table names are plain identifiers (so a view's name always equals
its `.sql` filename):

```yaml
es_index_name: ecs-dns-activity   # target ES index (hyphens allowed)
es_id_field: dsl_id               # view output column passed to the connector as the ES document _id
pipeline_mode: batch              # default export mode: batch | streaming (required; can override per run)
filter_condition: "action = 'allowed'"  # OPTIONAL default row filter (Spark SQL); omit for no filter
chunk_size: 1000                  # OPTIONAL EsWriteConfig tuning (docs per bulk request); omit for connector default
require_existing_index: true      # OPTIONAL EsWriteConfig tuning (require the index to exist); omit for connector default
verify_certs: true                # OPTIONAL EsWriteConfig tuning (verify the ES TLS cert); omit for connector default
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
```

A `catalog`/`schema` without an `${environment}` token is used verbatim. One that *uses* the token
but is deployed with an empty `environment` fails closed at deploy time, as does an environment value
that would produce an illegal identifier (e.g. one containing a hyphen).

`es_id_field` and `source.primary_key` are two distinct keys for two distinct contexts: `es_id_field`
is a column of the **view's** output, handed to the connector as the ES document `_id`; `primary_key`
is a column of the **source table**, used by the streaming read to identify a unique row. They often
share a value but need not, and neither defaults to the other. When `deploy_views` creates a view it
verifies `es_id_field` is actually one of that view's output columns (against Spark's resolved schema),
so a typo fails the deploy rather than surfacing later at export time.

## Deploy and run

Two different mechanisms carry values into a job, and they resolve at different times:

- **Bundle variables** (`environment`, `wheel_path`, `es_host_url`, `secret_scope_name`,
  `secret_key_name`, `checkpoint_base_path`) are `--var` values resolved into the job at **deploy**
  time. A `--var` on `bundle run` is ignored: only what was set at the last `bundle deploy` applies.
  They default to empty, so a deploy without them still succeeds and `deploy_views` runs fine (it
  needs no connector or ES); an index job needs a real `wheel_path` and the three ES connection
  settings, a streaming run also needs `checkpoint_base_path`, and one deployed with a required one
  empty fails closed.
- **Job parameters** are `--params` values applied at **run** time, overridable per run without
  redeploying (an invalid value fails the run closed):
  - `pipeline_mode` (`batch` | `streaming`), `filter_condition` (a Spark SQL predicate), and the
    connector-write tuning knobs `chunk_size`, `require_existing_index`, `verify_certs` all default to
    their config values (each is an optional config key; see [Configuration](#configuration)).
  - For the tuning knobs, a config that omits a knob (and a run that doesn't override it) leaves the
    connector's own default in force.
  - `streaming_start` (`new` | `full`, default `new`) sets where a **streaming** run begins on its
    first run: `new` streams only commits after the stream starts (batch mode owns the history);
    `full` backfills the whole existing table first. See [Streaming](#streaming).

```bash
python scripts/gen_jobs.py   # regenerate resources/<config_name>.job.yml from pipeline_definitions/*.yml

WHEEL="/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"
databricks bundle deploy -t dev -p <profile> \
  --var="environment=<env>" --var="wheel_path=$WHEEL" \
  --var="es_host_url=https://<host>:9200" \
  --var="secret_scope_name=<scope>" --var="secret_key_name=<key>" \
  --var="checkpoint_base_path=/Volumes/<catalog>/<schema>/<volume>/checkpoints"

databricks bundle run deploy_views                 -t dev -p <profile>
databricks bundle run index_pipeline_<config_name> -t dev -p <profile>

# override run-time settings for a single run (each defaults to its config/connector value otherwise):
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> \
  --params filter_condition="action = 'allowed'",chunk_size=1000

# stream a one-off full backfill of the whole table (default is new-commits-only):
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> \
  --params pipeline_mode=streaming,streaming_start=full
```

(Bundle variables come from the last `deploy`, so they are not repeated on `run`. Set real defaults
in your fork's `databricks.yml` to avoid passing them each deploy.)

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

The workspace deployed to is whichever one `-p <profile>` (or `DATABRICKS_HOST`) points at.
All jobs are granted `CAN_MANAGE_RUN` to the `users` group, so teammates can trigger them on demand.

Running the generator needs `pyyaml`, pinned in `requirements.txt` (`pip install -r requirements.txt`).
The pin matters because `--check` byte-compares against `yaml.safe_dump` output, whose formatting can
drift across pyyaml versions.

## Layout

```
databricks.yml                  Bundle definition: variables + targets
requirements.txt                Off-cluster tooling deps (pinned pyyaml for the generator)

You edit these, one pair per pipeline:
  views/
    <view_name>.sql             The view: what gets exported (filename == view.name)
  pipeline_definitions/
    <config_name>.yml           The config: view/source/reference locations + es_index_name,
                                es_id_field, pipeline_mode, source.primary_key (see Configuration)

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
    <config_name>.job.yml       GENERATED per-index job (one per pipeline_definitions config)
```

## License & Attribution

**Copyright © Databricks, Inc.** — Developed and maintained by Databricks Forward Deployed Engineering. Available to support customers and the broader community in building Elasticsearch export pipelines on Databricks. For production support and customization, contact your Databricks account team.

---

**Built with 💜 by Databricks Forward Deployed Engineering**

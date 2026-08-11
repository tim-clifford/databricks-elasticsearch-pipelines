# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/). It uses the
[**databricks-es-connector**](https://github.com/tim-clifford/es-databricks-connector) library for the
transfer.

## Overview

Every Elasticsearch index is fed by its own pipeline, and each pipeline is described by two files:

- a view `views/<view_name>.sql` defining what gets exported, and
- a config file `pipeline_definitions/<name>.yml` that points a pipeline at that view and says where
  its view, source table, and any reference (join) tables live.

The bundle deploys:

- **One `deploy_views` job**: creates or replaces one Databricks view per index. Each view is a
  `.sql` file in [`views/`](views/). The job renders the catalog/schema parameters and runs every
  file with `spark.sql`.
- **One job per index** (`index_pipeline_<name>`): all run the same shared notebook
  [`notebooks/run_index_pipeline.py`](notebooks/run_index_pipeline.py) with that index's config. These
  job resources are **generated** by [`scripts/gen_jobs.py`](scripts/gen_jobs.py) from the config
  files (see [Adding a new pipeline for an ES index](#adding-a-new-pipeline-for-an-es-index)).

## Adding a new pipeline for an ES index

1. Add `views/<view_name>.sql`.
2. Add `pipeline_definitions/<name>.yml` (see the schema under [Configuration](#configuration)). The
   view's filename must match the config's `view.name`.
3. Regenerate the job resources: `python scripts/gen_jobs.py`.
4. Deploy.

The config's filename stem becomes the job's resource key (`index_pipeline_<stem>`), so it must
contain only letters, digits, `_`, and `-` (the generator rejects anything else, e.g. a dotted name).

Step 3 writes one `resources/<name>.job.yml` per config, keeping the generated jobs in sync. If you
run it, you're covered. `python scripts/gen_jobs.py --check` is the separate verification that step 3
was actually run: it makes no changes and fails if any generated file is missing, stale, or orphaned
(left behind by a deleted or renamed config). Run it in CI to catch a commit that edited a config but
forgot to regenerate.

## Views

Each `.sql` file in `views/` defines one view. The filename matches the view it creates
(`ecs_dns_activity.sql` creates the `ecs_dns_activity` view, feeding the `ecs-dns-activity` ES index;
the view name uses underscores because a Databricks view name can't contain unquoted hyphens). Object
names use `${...}` parameters resolved per-view from the config plus the shared `catalog` at deploy
time:

| Parameter | Resolves to |
|---|---|
| `${catalog}` | the shared catalog (bundle variable) |
| `${view_schema}` / `${view_name}` | where the view is created, and its name |
| `${source_schema}` / `${source_table}` | the source table the view reads from |
| `${ref_<alias>}` | a reference (join) table, aliased: `catalog.schema.table <alias>` |
| `${broadcast_hint}` | a Spark `/*+ BROADCAST(...) */` hint (empty unless a reference table sets `broadcast: true`); place it right after the top-level `SELECT` |

An unknown `${...}` parameter in a file is a hard error (fail closed), so a typo can't create a view
pointing at the wrong place.

### Reference (join) tables

A view has exactly one source table, but may join **reference tables** (e.g. dimension or lookup
tables). Declare each under `reference_tables` in the config; the key is the join alias you reference
in the SQL as `${ref_<alias>}`:

```sql
SELECT ${broadcast_hint}
    base.dsl_id,
    (validation.dsl_id IS NOT NULL) AS validation_row_exists
FROM ${catalog}.${source_schema}.${source_table} base
LEFT JOIN ${ref_validation} ON base.dsl_id = validation.dsl_id
```

The config owns *where* each table is and whether to broadcast it; the SQL owns the join itself (type,
`ON` clause, surfaced columns). Set `broadcast: true` on a reference table to have the framework add a
broadcast hint naming that join.

**Known limitation:** all tables a view reads (source + reference) must live in the shared `catalog`.
A cross-catalog join is not supported.

## Configuration

The **workspace** is not a bundle variable: it comes from your Databricks CLI profile (`-p <profile>`)
or `DATABRICKS_HOST`. The only bundle variable is:

| Variable | What it sets |
|---|---|
| `catalog` | the one shared catalog every view and source/reference table lives in (required, no default) |

Everything else is per-pipeline and lives in `pipeline_definitions/<name>.yml`:

```yaml
es_index_name: ecs-dns-activity   # target ES index (hyphens allowed)
primary_key: dsl_id               # view column used as the ES document _id
view:                             # the view this pipeline creates
  schema: es_poc
  name: ecs_dns_activity
source:                           # the single source table the view reads from
  schema: ocsf
  table: dns_activity
reference_tables:                 # OPTIONAL: extra tables the view joins (see Views)
  validation:                     # key = the ${ref_validation} join alias in the SQL
    schema: ocsf_validation
    table: dns_activity
    broadcast: false              # true adds a Spark broadcast hint for this join
```

`catalog` is deliberately not per-pipeline: it is assumed shared across every pipeline in an
environment. (Supporting different catalogs per pipeline would be a future change.)

## Deploy and run

```bash
python scripts/gen_jobs.py   # regenerate resources/<name>.job.yml from pipeline_definitions/*.yml

databricks bundle deploy -t dev -p <profile> --var="catalog=<catalog>"

databricks bundle run deploy_views                -t dev -p <profile> --var="catalog=<catalog>"
databricks bundle run index_pipeline_<name>       -t dev -p <profile> --var="catalog=<catalog>"
```

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
    <name>.yml                  The config: view/source/reference locations + es_index_name,
                                primary_key (see Configuration for the schema)

Shared notebooks (run by the jobs, not edited per pipeline):
  notebooks/
    deploy_views.py             Renders each view's parameters from its config + the shared catalog,
                                then runs CREATE OR REPLACE
    run_index_pipeline.py       Run by every per-index job (reads/validates/prints its config)

Shared library + tests (the config schema, used by the generator and deploy_views):
  pipeline_lib/
    config.py                   Loads/validates a pipeline definition; derives view substitutions
                                and job parameters (single source of truth for the schema)
  tests/
    test_config.py              Offline unit tests for pipeline_lib.config (plain pytest)

Generated / tooling (do not hand-edit the generated jobs):
  scripts/
    gen_jobs.py                 Generates resources/<name>.job.yml from the configs (--check guards drift)
  resources/
    deploy_views.job.yml        The deploy_views job (hand-authored)
    <name>.job.yml              GENERATED per-index job (one per pipeline_definitions config)
```

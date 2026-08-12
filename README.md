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
| `wheel_path` | UC Volume path to the `databricks-es-connector` wheel each **index job** installs (the connector version lives here, in the wheel filename); a global prerequisite, not created by this bundle (see [the connector repo](https://github.com/tim-clifford/es-databricks-connector) for building/uploading it). Defaults to empty; supply it on `bundle deploy` (or set a real default in your fork). An index-job deployed with an empty `wheel_path` fails closed at run; `deploy_views` and `deploy` don't need it |

Everything else is per-pipeline and lives in `pipeline_definitions/<config_name>.yml`. Each object is fully
qualified (`catalog`, `schema`, and a name/table). Only `catalog` and `schema` may embed
`${environment}`; the view name and table names are plain identifiers (so a view's name always equals
its `.sql` filename):

```yaml
es_index_name: ecs-dns-activity   # target ES index (hyphens allowed)
es_id_field: dsl_id               # view output column passed to the connector as the ES document _id
pipeline_mode: batch              # export mode for this index's job: batch | streaming (required)
view:                             # the view this pipeline creates
  catalog: acme_${environment}
  schema: es_poc
  name: ecs_dns_activity
source:                           # the single source table the view reads from
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

`wheel_path` is baked into each job's parameters at **deploy** time (that is when `${var.wheel_path}`
resolves), so supply it on `bundle deploy`, not on `bundle run` (a `--var` on `run` does not override
the deployed value). It defaults to empty, so a deploy without it still succeeds and `deploy_views`
runs fine (it needs no connector); only an index-job run needs a real wheel, and one deployed with an
empty `wheel_path` fails closed:

```bash
python scripts/gen_jobs.py   # regenerate resources/<config_name>.job.yml from pipeline_definitions/*.yml

WHEEL="/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"
databricks bundle deploy -t dev -p <profile> --var="environment=<env>" --var="wheel_path=$WHEEL"

databricks bundle run deploy_views                 -t dev -p <profile> --var="environment=<env>"
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> --var="environment=<env>"
```

(Omit `--var="environment=..."` if none of your config names use `${environment}`. An index-job run
needs a `wheel_path` supplied at deploy; set a real default in your fork's `databricks.yml` to avoid
passing it each time.)

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
                                prints the resolved config (export logic lands here later)

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

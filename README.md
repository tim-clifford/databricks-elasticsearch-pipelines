# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) so the whole thing
deploys to a fresh workspace with one command. It builds on the
[**databricks-es-connector**](https://github.com/tim-clifford/es-databricks-connector) library
(serverless-safe bulk write/read, gzip, idempotent IDs) rather than re-implementing the transfer.

## Overview

Every Elasticsearch index is fed by its own pipeline, and each pipeline is described by two files:

- a view `views/<view_name>.sql` defining what gets exported, and
- a config file `index_pipelines/<name>.yml` (`es_index_name`, `source_table`, `view_name`,
  `primary_key`) that points a pipeline at that view.

The bundle deploys:

- **`deploy_views`** — creates or replaces one Databricks view per index. Each view is a `.sql` file
  in [`views/`](views/); the job renders the catalog/schema parameters and runs every file with
  `spark.sql`.
- **one job per index** (`index_pipeline_<name>`) — all run the same shared notebook
  [`notebooks/run_index_pipeline.py`](notebooks/run_index_pipeline.py) with that index's config. These
  job resources are **generated** by [`scripts/gen_jobs.py`](scripts/gen_jobs.py) from the config
  files (see [Adding a new pipeline for an ES index](#adding-a-new-pipeline-for-an-es-index)).

## Adding a new pipeline for an ES index

1. Add `views/<view_name>.sql`.
2. Add `index_pipelines/<name>.yml` with `es_index_name`, `source_table`, `view_name`, `primary_key`.
3. Regenerate the job resources: `python scripts/gen_jobs.py`.
4. Deploy.

Step 3 is what keeps the generated `resources/<name>.job.yml` files in sync with the configs; the
generator writes one per config. To catch a config that was changed but not regenerated, run
`python scripts/gen_jobs.py --check` (e.g. in CI): it fails if any generated file is missing, stale,
or orphaned (left behind by a deleted or renamed config).

## Views

Each `.sql` file in `views/` defines one view. The filename matches the view it creates
(`ecs_dns_activity.sql` creates the `ecs_dns_activity` view, feeding the `ecs-dns-activity` ES index;
the view name uses underscores because a Databricks view name can't contain unquoted hyphens). Object
names use `${...}` parameters substituted from job parameters at deploy time:

| Parameter | Meaning |
|---|---|
| `${view_catalog}` / `${view_schema}` | where the view is created |
| `${source_catalog}` / `${source_schema}` | where the view's source table(s) are read from |

An unknown `${...}` parameter in a file is a hard error (fail closed), so a typo can't create a view
pointing at the wrong place.

**Known limitation:** all tables referenced by a single view must share one
`source_catalog.source_schema`. A view joining tables across different schemas is not supported.

## Configuration

The bundle carries no environment-specific values; every one is supplied at deploy time and has no
default, because catalog/schema names and the target workspace differ per environment.

The **workspace** is not a bundle variable — it comes from your Databricks CLI profile (`-p <profile>`)
or `DATABRICKS_HOST`. The bundle variables, all passed with `--var`, are:

| Variable | What it sets |
|---|---|
| `view_catalog` | catalog where `deploy_views` creates the views |
| `view_schema` | schema where `deploy_views` creates the views |
| `source_catalog` | catalog the views read their source tables from |
| `source_schema` | schema the views read their source tables from |

The per-pipeline values (`es_index_name`, `source_table`, `view_name`, `primary_key`) are not bundle
variables — each comes from its `index_pipelines/<name>.yml`.

## Deploy and run

```bash
python scripts/gen_jobs.py   # regenerate resources/<name>.job.yml from index_pipelines/*.yml

databricks bundle deploy -t dev -p <profile> \
  --var="view_catalog=<catalog>" \
  --var="view_schema=<schema>" \
  --var="source_catalog=<catalog>" \
  --var="source_schema=<schema>"

databricks bundle run deploy_views                -t dev -p <profile>   # plus the same --var flags
databricks bundle run index_pipeline_<name>       -t dev -p <profile>   # plus the same --var flags
```

The workspace deployed to is whichever one `-p <profile>` (or `DATABRICKS_HOST`) points at.
All jobs are granted `CAN_MANAGE_RUN` to the `users` group, so teammates can trigger them on demand.

The generator needs `pyyaml`, pinned in `requirements.txt` (`pip install -r requirements.txt`).
The pin matters: `--check` byte-compares against `yaml.safe_dump` output, whose formatting can drift
across pyyaml versions. A config's filename stem becomes the job's resource key
(`index_pipeline_<stem>`), so it must contain only letters, digits, `_`, and `-` (the generator
rejects anything else, e.g. a dotted name, at generation time).

## Layout

```
databricks.yml                  Bundle definition: variables + targets (host comes from the CLI profile)
requirements.txt                Off-cluster tooling deps (pinned pyyaml for the generator)

You edit these — one pair per pipeline:
  views/
    <view_name>.sql             The view: what gets exported (filename == view name)
  index_pipelines/
    <name>.yml                  The config: points a pipeline at a view (es_index_name,
                                source_table, view_name, primary_key)

Shared notebooks (run by the jobs, not edited per pipeline):
  notebooks/
    deploy_views.py             Renders view parameters + CREATE OR REPLACE each view
    run_index_pipeline.py       Run by every per-index job (reads/validates/prints its config)

Generated / tooling — do not hand-edit the generated jobs:
  scripts/
    gen_jobs.py                 Generates resources/<name>.job.yml from the configs (--check guards drift)
  resources/
    deploy_views.job.yml        The deploy_views job (hand-authored)
    <name>.job.yml              GENERATED per-index job (one per index_pipelines config)
```

# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) so the whole thing
deploys to a fresh workspace with one command. It builds on the
[**databricks-es-connector**](https://github.com/tim-clifford/es-databricks-connector) library
(serverless-safe bulk write/read, gzip, idempotent IDs) rather than re-implementing the transfer.

## Overview

The unit of scale is **one Elasticsearch index**. Each index has:

- a config file `index_pipelines/<name>.yml` (`es_index_name`, `source_table`, `view_name`,
  `primary_key`), and
- a view `views/<view_name>.sql` defining what gets exported.

The bundle deploys:

- **one job per index** (`index_pipeline_<name>`) — all run the same shared notebook
  [`src/run_index_pipeline.py`](src/run_index_pipeline.py) with that index's config. These job
  resources are **generated** by [`scripts/gen_jobs.py`](scripts/gen_jobs.py) from the config files
  (see [Adding an index](#adding-an-index)).
- **`deploy_views`** — creates or replaces one Databricks view per index. Each view is a `.sql` file
  in [`views/`](views/); the job renders the catalog/schema placeholders and runs every file with
  `spark.sql`.

## Adding an index

1. Add `index_pipelines/<name>.yml` with `es_index_name`, `source_table`, `view_name`, `primary_key`.
2. Add `views/<view_name>.sql`.
3. Regenerate the job resources: `python scripts/gen_jobs.py`.
4. Deploy.

`scripts/gen_jobs.py --check` fails if any generated `resources/<name>.job.yml` is missing, stale, or
orphaned (left behind by a deleted/renamed config), so run it in CI to catch a config change that
wasn't regenerated. Generated files carry a `DO NOT EDIT` header; edits belong in the config or the
generator template. Config files may use `.yml` or `.yaml`, and each required value must be a
non-empty string (a null or non-string value is rejected, not silently rendered).

## Views

Each `.sql` file in `views/` defines one view. The filename matches the view it creates
(`ecs_dns_activity.sql` creates the `ecs_dns_activity` view, feeding the `ecs-dns-activity` ES index;
the view name uses underscores because a Databricks view name can't contain unquoted hyphens). Object
names use `${...}` placeholders substituted from job parameters at deploy time:

| Placeholder | Meaning |
|---|---|
| `${view_catalog}` / `${view_schema}` | where the view is created |
| `${source_catalog}` / `${source_schema}` | where the view's source table(s) are read from |

An unknown `${placeholder}` in a file is a hard error (fail closed), so a typo can't create a view
pointing at the wrong place.

**Known limitation:** all tables referenced by a single view must share one
`source_catalog.source_schema`. A view joining tables across different schemas is not supported.

## Configuration

The bundle carries no environment-specific values. Supply them at deploy time:

| What | How | Required? |
|---|---|---|
| **Workspace host** | your Databricks CLI profile (`-p <profile>`) or `DATABRICKS_HOST` | yes |
| `view_catalog`, `view_schema` | where `deploy_views` creates the views | yes, no default |
| `source_catalog`, `source_schema` | where the views read their source tables | yes, no default |

The required variables have no defaults on purpose: catalog/schema names are environment-specific,
so each deployment must state them. The per-index values (`es_index_name`, `source_table`,
`view_name`, `primary_key`) come from each `index_pipelines/<name>.yml`, not from `--var`.

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

The generator needs `pyyaml`, pinned in `requirements-dev.txt` (`pip install -r requirements-dev.txt`).
The pin matters: `--check` byte-compares against `yaml.safe_dump` output, whose formatting can drift
across pyyaml versions. A config's filename stem becomes the job's resource key
(`index_pipeline_<stem>`), so it must contain only letters, digits, `_`, and `-` (the generator
rejects anything else, e.g. a dotted name, at generation time).

## Layout

```
databricks.yml                    bundle: variables + targets (host comes from the CLI profile)
index_pipelines/<name>.yml        per-index config (es_index_name, source_table, view_name, primary_key)
scripts/gen_jobs.py               generates resources/<name>.job.yml from the configs (--check guards drift)
resources/deploy_views.job.yml    the deploy_views job
resources/<name>.job.yml          GENERATED per-index job (one per index_pipelines config)
src/run_index_pipeline.py         shared notebook run by every per-index job (reads/validates/prints config)
src/deploy_views.py               notebook: render placeholders + CREATE OR REPLACE each view
views/<view_name>.sql             one .sql file per view (filename == view name)
```

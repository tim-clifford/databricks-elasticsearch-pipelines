# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) so the whole thing
deploys to a fresh workspace with one command. It builds on the
[**databricks-es-connector**](https://github.com/tim-clifford/es-databricks-connector) library
(serverless-safe bulk write/read, gzip, idempotent IDs) rather than re-implementing the transfer.

## Overview

Two serverless jobs:

- **`elasticsearch_pipeline`** — installs the `databricks-es-connector` wheel from a **required,
  configurable** UC Volume path, **imports it** to prove the install is usable (not merely that pip
  exited 0), and validates the export **mode** (`batch` or `streaming`) it was launched with.
- **`deploy_views`** — creates or replaces one Databricks view per Elasticsearch index. Each view is
  a `.sql` file in [`views/`](views/); the job renders the catalog/schema placeholders and runs every
  file with `spark.sql`. Adding an index is adding a `.sql` file.

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
| `wheel_path` | UC Volume path to the connector `.whl` (`--var`, target override, or `DATABRICKS_BUNDLE_VAR_wheel_path`) | yes, no default |
| `view_catalog`, `view_schema` | where `deploy_views` creates the views | yes, no default |
| `source_catalog`, `source_schema` | where the views read their source tables | yes, no default |
| `pipeline_mode` | `batch` or `streaming` | no (defaults to `batch`) |

The required variables have no defaults on purpose: wheel location and catalog/schema names are
environment-specific, so each deployment must state them. The wheel path shape is
`/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl`.

### Environment prerequisites (not created by this bundle)

- The `databricks-es-connector` wheel already present on the `wheel_path` UC Volume. Build and
  upload it from the [connector repo](https://github.com/tim-clifford/es-databricks-connector).

## Deploy and run

Every required `--var` is passed as its own flag:

```bash
databricks bundle deploy -t dev -p <profile> \
  --var="wheel_path=/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl" \
  --var="view_catalog=<catalog>" \
  --var="view_schema=<schema>" \
  --var="source_catalog=<catalog>" \
  --var="source_schema=<schema>"

databricks bundle run deploy_views          -t dev -p <profile>   # plus the same --var flags
databricks bundle run elasticsearch_pipeline -t dev -p <profile>   # plus the same --var flags
```

The workspace deployed to is whichever one `-p <profile>` (or `DATABRICKS_HOST`) points at.
Both jobs are granted `CAN_MANAGE_RUN` to the `users` group, so teammates can trigger them on demand.

## Layout

```
databricks.yml                    bundle: variables + targets (host comes from the CLI profile)
resources/pipeline.job.yml        the elasticsearch_pipeline job
resources/deploy_views.job.yml    the deploy_views job
src/run_pipeline.py               notebook: install wheel, prove import, validate mode
src/deploy_views.py               notebook: render placeholders + CREATE OR REPLACE each view
views/                            one .sql file per view (filename == view name)
```

# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) so the whole thing
deploys to a fresh workspace with one command. It builds on the
[**databricks-es-connector**](https://github.com/tim-clifford/es-databricks-connector) library
(serverless-safe bulk write/read, gzip, idempotent IDs) rather than re-implementing the transfer.

Built **bottom-up**: small, verifiable pieces, one at a time.

## Status: v1

A single serverless job (`elasticsearch_pipeline`) with one notebook task that:

- installs the `databricks-es-connector` wheel (v0.6.1) from a **configurable** UC Volume path, and
- validates the export **mode** (`batch` or `streaming`) the job was launched with.

Batch/streaming routing and the actual Delta -> Elasticsearch export come in later steps.

## Configuration

Set per environment as bundle variables (override with `--var` or in `databricks.yml`):

| Variable | What it is | Default |
|---|---|---|
| `wheel_path` | UC Volume path to the connector `.whl` | the FEVM `es_poc` volume path |
| `pipeline_mode` | `batch` or `streaming` | `batch` |

The **workspace host** is set per target in `databricks.yml` (`dev` / `prd`).

### Environment prerequisites (not created by this bundle)

- The `databricks-es-connector` wheel already present on the `wheel_path` UC Volume.

## Deploy and run

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run elasticsearch_pipeline -t dev

# override a variable at deploy time, e.g. a different wheel location:
databricks bundle deploy -t dev --var="wheel_path=/Volumes/my_cat/my_schema/vol/databricks_es_connector-0.6.1-py3-none-any.whl"
```

Add `-p <profile>` if the target workspace is not your default `~/.databrickscfg` profile
(e.g. `-p fe-vm-tim-clifford-classic-dsl-lite`).

Currently deployed to `https://fevm-tim-clifford-classic-dsl-lite.cloud.databricks.com`
(job `elasticsearch_pipeline`), verified end to end: a valid run installs the wheel and
succeeds; an invalid `pipeline_mode` fails the run on the validation guard.

## Layout

```
databricks.yml                 bundle: variables + targets
resources/pipeline.job.yml     the elasticsearch_pipeline job (one serverless notebook task)
src/run_pipeline.py            the notebook (v1: installs the wheel, validates mode)
```

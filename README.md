# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) so the whole thing
deploys to a fresh workspace with one command. It builds on the
[**databricks-es-connector**](https://github.com/tim-clifford/es-databricks-connector) library
(serverless-safe bulk write/read, gzip, idempotent IDs) rather than re-implementing the transfer.

Built **bottom-up**: small, verifiable pieces, one at a time.

## Status: v1

A single serverless job (`elasticsearch_pipeline`) with one notebook task that:

- installs the `databricks-es-connector` wheel from a **required, configurable** UC Volume path,
- **imports it** to prove the install is usable (not merely that pip exited 0), and
- validates the export **mode** (`batch` or `streaming`) the job was launched with.

Batch/streaming routing and the actual Delta -> Elasticsearch export come in later steps.

## Configuration

The bundle carries no environment-specific values. Supply them at deploy time:

| What | How | Required? |
|---|---|---|
| **Workspace host** | your Databricks CLI profile (`-p <profile>`) or `DATABRICKS_HOST` | yes |
| `wheel_path` | UC Volume path to the connector `.whl` (`--var`, target override, or `DATABRICKS_BUNDLE_VAR_wheel_path`) | yes, no default |
| `pipeline_mode` | `batch` or `streaming` | no (defaults to `batch`) |

`wheel_path` has no default on purpose: the wheel's location is environment-specific, so each
deployment must state it. The example path shape is
`/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl`.

### Environment prerequisites (not created by this bundle)

- The `databricks-es-connector` wheel already present on the `wheel_path` UC Volume. Build and
  upload it from the [connector repo](https://github.com/tim-clifford/es-databricks-connector).

## Deploy and run

```bash
databricks bundle validate -t dev -p <profile>

databricks bundle deploy -t dev -p <profile> \
  --var="wheel_path=/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"

databricks bundle run elasticsearch_pipeline -t dev -p <profile>
```

The workspace deployed to is whichever one `-p <profile>` (or `DATABRICKS_HOST`) points at.
The job is granted `CAN_MANAGE_RUN` to the `users` group, so teammates can trigger it on demand.

## Layout

```
databricks.yml                 bundle: variables + targets (host comes from the CLI profile)
resources/pipeline.job.yml     the elasticsearch_pipeline job (one serverless notebook task)
src/run_pipeline.py            the notebook (v1: install wheel, prove import, validate mode)
```

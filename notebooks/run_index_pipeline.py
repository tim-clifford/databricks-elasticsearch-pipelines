# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: per-index pipeline runner
# MAGIC
# MAGIC The shared notebook run by every per-index job. Each index has a config file in
# MAGIC `pipeline_definitions/<name>.yml`; the generated job passes that file's values in as widgets. For
# MAGIC now the notebook only reads, validates, and prints them; the actual export is added later.
# MAGIC
# MAGIC Parameters (set by the generated per-index job as widgets, from `job_base_parameters`):
# MAGIC - `es_index_name`: target Elasticsearch index.
# MAGIC - `primary_key`: the view column used as the Elasticsearch document `_id`.
# MAGIC - `view_schema`, `view_name`: the Databricks view that defines what gets exported.
# MAGIC - `source_schema`, `source_table`: the Delta table the view reads from.

# COMMAND ----------
# Read and validate the job parameters. Every one is required with no default: they come from the
# index's config YAML via the generated job, so a missing value means the job was wired up wrong and
# must fail loudly (fail closed) rather than run against a blank. This list must match the keys the
# generator emits (pipeline_lib.config.job_base_parameters).
PARAM_NAMES = ("es_index_name", "primary_key", "view_schema", "view_name", "source_schema", "source_table")
for name in PARAM_NAMES:
    dbutils.widgets.text(name, "", name)

params = {name: dbutils.widgets.get(name).strip() for name in PARAM_NAMES}

missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"missing required parameter(s): {', '.join(missing)}")

# COMMAND ----------
# For now, just print what this job was configured with. This is the placeholder for the export
# logic; keeping it a pure echo makes the generated-job wiring verifiable on its own.
print("per-index pipeline configuration:")
for name in PARAM_NAMES:
    print(f"  {name} = {params[name]}")

dbutils.notebook.exit("; ".join(f"{name}={params[name]}" for name in PARAM_NAMES))

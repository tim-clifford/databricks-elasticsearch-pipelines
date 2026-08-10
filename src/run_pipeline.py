# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: pipeline runner (v1)
# MAGIC
# MAGIC Bottom-up scaffold. **v1 does one thing:** install the `databricks-es-connector` wheel
# MAGIC (v0.6.1) from a configurable UC Volume path, and validate the export mode the job was
# MAGIC launched with. Batch/streaming routing and the actual export land in later steps.
# MAGIC
# MAGIC Parameters (set by the DAB job as widgets):
# MAGIC - `wheel_path`: UC Volume path to the connector `.whl` (required).
# MAGIC - `pipeline_mode`: `batch` or `streaming`.

# COMMAND ----------
# Install the connector wheel from a configurable UC Volume path, and validate the export mode.
#
# %pip cannot expand a widget/parameter inside a literal `%pip install <path>` line, so we read the
# parameter in Python and invoke the pip magic programmatically, then restart Python so the freshly
# installed package is importable. restartPython() ends this cell, so it is the last statement.
dbutils.widgets.text("wheel_path", "", "Connector wheel path (UC Volume .whl)")
dbutils.widgets.dropdown("pipeline_mode", "batch", ["batch", "streaming"], "Export mode")

wheel_path = dbutils.widgets.get("wheel_path").strip()
pipeline_mode = dbutils.widgets.get("pipeline_mode").strip()

# Allow-list, not deny-list: reject anything that is not an explicitly supported mode (fail closed).
if pipeline_mode not in ("batch", "streaming"):
    raise ValueError(f"pipeline_mode must be 'batch' or 'streaming', got {pipeline_mode!r}")
if not wheel_path:
    raise ValueError(
        "wheel_path is required: the UC Volume path to the databricks_es_connector wheel, e.g. "
        "/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-0.6.1-py3-none-any.whl"
    )

print(f"pipeline_mode={pipeline_mode}; installing connector wheel from {wheel_path}")
get_ipython().run_line_magic("pip", f"install {wheel_path}")
dbutils.library.restartPython()

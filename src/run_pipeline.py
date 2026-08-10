# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: pipeline runner
# MAGIC
# MAGIC Installs the `databricks-es-connector` wheel from a configurable UC Volume path, proves it is
# MAGIC importable, and validates the export mode the job was launched with.
# MAGIC
# MAGIC Parameters (set by the DAB job as widgets):
# MAGIC - `wheel_path`: UC Volume path to the connector `.whl` (required).
# MAGIC - `pipeline_mode`: `batch` or `streaming`.

# COMMAND ----------
# Read and validate the job parameters.
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
        "/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"
    )

# COMMAND ----------
# Install the connector wheel and restart Python so the freshly installed package is importable.
#
# %pip cannot expand a widget/parameter inside a literal `%pip install <path>` line, so we invoke the
# pip magic programmatically with the validated path. shlex.quote so a path with spaces (or a value
# that could otherwise look like extra pip options) is passed to pip as a single literal argument,
# not split or parsed as flags. restartPython() ends this cell, so the import-proof check lives in
# the next cell (it can only run after the restart).
import shlex

print(f"pipeline_mode={pipeline_mode}; installing connector wheel from {wheel_path}")
get_ipython().run_line_magic("pip", f"install {shlex.quote(wheel_path)}")
dbutils.library.restartPython()

# COMMAND ----------
# Prove the install is actually USABLE, not merely that pip exited 0: a wheel can install yet fail to
# import (wrong Python/arch, broken deps). This runs after restartPython(), so it exercises the fresh
# interpreter. A failed import raises here and fails the job, which is the point.
import databricks_es_connector
from databricks_es_connector import EsConfig, bulk_write, make_foreach_batch, read_index  # noqa: F401

print(f"databricks_es_connector import OK: {databricks_es_connector.__file__}")

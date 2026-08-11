# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: per-index pipeline runner
# MAGIC
# MAGIC The shared notebook run by every per-index job. The generated job passes the config's NAME plus
# MAGIC the deploy-time `environment`; this notebook loads `pipeline_definitions/<config_name>.yml`,
# MAGIC resolves `${environment}` into the object names, and (for now) prints the resolved config. The
# MAGIC actual export is added later.
# MAGIC
# MAGIC Why load the config here rather than receive resolved values: the job resources are generated
# MAGIC offline by scripts/gen_jobs.py, which cannot know the deploy-time environment, so it cannot bake
# MAGIC resolved catalog/schema names into the job. The notebook resolves them at runtime instead.
# MAGIC
# MAGIC Parameters (set by the generated per-index job as widgets):
# MAGIC - `config_name`: the pipeline definition to load (`pipeline_definitions/<config_name>.yml`).
# MAGIC - `environment`: folded into any `${environment}` in the config's object names (may be empty).

# COMMAND ----------
# Read the parameters. config_name is required; environment may be empty (a config that uses no
# ${environment} token needs none, and one that does fails closed later in resolve_config).
dbutils.widgets.text("config_name", "", "Pipeline definition name (pipeline_definitions/<name>.yml)")
dbutils.widgets.text("environment", "", "Environment folded into ${environment} in config names")
CONFIG_NAME = dbutils.widgets.get("config_name").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip()
if not CONFIG_NAME:
    raise ValueError("missing required parameter: config_name")

# COMMAND ----------
# Resolve the synced bundle root and make pipeline_lib importable. This notebook is synced to
# <bundle files>/notebooks/run_index_pipeline.py; pipeline_definitions/ is a sibling of notebooks/.
import os
import sys

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
FILES_ROOT = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files
if FILES_ROOT not in sys.path:
    sys.path.insert(0, FILES_ROOT)

from pipeline_lib.config import load_config, resolve_config  # noqa: E402

# Resolve the config file, accepting either extension: gen_jobs.py and deploy_views.py both discover
# .yml AND .yaml, so the runner must too, or a .yaml-defined pipeline would deploy fine and then fail
# here at runtime. Fail closed if neither exists.
CONFIG_DIR = os.path.join(FILES_ROOT, "pipeline_definitions")
config_path = next(
    (p for ext in (".yml", ".yaml") if os.path.exists(p := os.path.join(CONFIG_DIR, f"{CONFIG_NAME}{ext}"))),
    None,
)
if config_path is None:
    raise ValueError(f"no pipeline definition found for {CONFIG_NAME!r} (.yml/.yaml) in {CONFIG_DIR}")

# load_config validates the schema; resolve_config folds ${environment} in and validates the result
# (both fail closed). After this, every catalog/schema/name is a concrete identifier.
cfg = resolve_config(load_config(config_path), ENVIRONMENT)

# COMMAND ----------
# For now, just print the resolved configuration. This is the placeholder for the export logic;
# keeping it a pure echo makes the generated-job wiring + environment resolution verifiable on its own.
view = cfg["view"]
source = cfg["source"]
print(f"config_name        = {CONFIG_NAME}")
print(f"environment        = {ENVIRONMENT!r}")
print(f"es_index_name      = {cfg['es_index_name']}")
print(f"primary_key        = {cfg['primary_key']}")
print(f"view               = {view['catalog']}.{view['schema']}.{view['name']}")
print(f"source             = {source['catalog']}.{source['schema']}.{source['table']}")
for alias, spec in cfg["reference_tables"].items():
    print(f"reference[{alias}]  = {spec['catalog']}.{spec['schema']}.{spec['table']} (broadcast={spec['broadcast']})")

dbutils.notebook.exit(
    f"config_name={CONFIG_NAME}; es_index_name={cfg['es_index_name']}; "
    f"view={view['catalog']}.{view['schema']}.{view['name']}; "
    f"source={source['catalog']}.{source['schema']}.{source['table']}"
)

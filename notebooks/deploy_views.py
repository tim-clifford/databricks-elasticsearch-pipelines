# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: deploy views
# MAGIC
# MAGIC Creates or replaces one Databricks view per Elasticsearch index. Each view is a `.sql` file in
# MAGIC `views/`, paired with a `pipeline_definitions/<name>.yml` that says where its view and source
# MAGIC live and which reference tables it joins. This notebook renders each view's `${...}` parameters
# MAGIC from its config and the shared `catalog`, then runs it with `spark.sql`.
# MAGIC
# MAGIC Parameter (set by the DAB job as a widget):
# MAGIC - `catalog`: the one shared catalog every view and source table lives in (required).
# MAGIC
# MAGIC Everything else (view schema/name, source schema/table, reference tables) comes from each
# MAGIC pipeline definition, so adding an index is adding a `.sql` file plus its config.

# COMMAND ----------
# Read the one shared parameter. catalog has no default: it is environment-specific, so a missing
# value must fail loudly (fail closed) rather than deploy views into a blank catalog.
dbutils.widgets.text("catalog", "", "Shared catalog (where views and source tables live)")
CATALOG = dbutils.widgets.get("catalog").strip()
if not CATALOG:
    raise ValueError("missing required parameter: catalog")

# COMMAND ----------
# Resolve the synced bundle root so we can read both views/ and pipeline_definitions/, and add it to
# sys.path so pipeline_lib (the shared config schema) is importable. This notebook is synced to
# <bundle files>/notebooks/deploy_views.py; both dirs are siblings of notebooks/.
import os
import sys

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
FILES_ROOT = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files
if FILES_ROOT not in sys.path:
    sys.path.insert(0, FILES_ROOT)

VIEWS_DIR = os.path.join(FILES_ROOT, "views")
CONFIG_DIR = os.path.join(FILES_ROOT, "pipeline_definitions")
print("files root:", FILES_ROOT)

from pipeline_lib.config import load_config, view_substitutions  # noqa: E402

# COMMAND ----------
# Load every pipeline definition and key it by the view name it declares, so a view .sql file can be
# matched to its config. A duplicate view name across configs is an error (two pipelines can't own
# the same view).
import glob

configs_by_view = {}
for cfg_path in sorted(glob.glob(os.path.join(CONFIG_DIR, "*.yml")) + glob.glob(os.path.join(CONFIG_DIR, "*.yaml"))):
    cfg = load_config(cfg_path)  # validates; raises on any invalid config (fail closed)
    view_name = cfg["view"]["name"]
    if view_name in configs_by_view:
        raise ValueError(f"two pipeline definitions declare view '{view_name}'; view names must be unique")
    configs_by_view[view_name] = cfg
print(f"loaded {len(configs_by_view)} pipeline definition(s)")

sql_files = sorted(f for f in os.listdir(VIEWS_DIR) if f.endswith(".sql"))
if not sql_files:
    raise ValueError(f"no .sql view files found in {VIEWS_DIR}")

# Every view .sql must have a matching config, and vice versa: an unpaired file on either side is a
# wiring mistake that would otherwise deploy a view with unresolved parameters, or silently skip one.
sql_view_names = {os.path.splitext(f)[0] for f in sql_files}
missing_config = sorted(sql_view_names - set(configs_by_view))
missing_sql = sorted(set(configs_by_view) - sql_view_names)
if missing_config:
    raise ValueError(f"view .sql file(s) with no matching pipeline definition: {', '.join(missing_config)}")
if missing_sql:
    raise ValueError(f"pipeline definition(s) with no matching view .sql file: {', '.join(missing_sql)}")
print(f"found {len(sql_files)} view file(s): {', '.join(sql_files)}")

# COMMAND ----------
# Render each view's ${parameter} tokens from its config and run it. Substitution is explicit (not
# str.format, which would choke on any literal braces in SQL) and fail-closed: an unknown ${token}
# in a file is an error, not a silently-unsubstituted string that would create a broken view.
import re

_TOKEN = re.compile(r"\$\{(\w+)\}")


def render(sql: str, filename: str, subs: dict) -> str:
    def _sub(m: "re.Match") -> str:
        key = m.group(1)
        if key not in subs:
            raise ValueError(
                f"{filename}: unknown parameter ${{{key}}}; available: {sorted(subs)}"
            )
        return subs[key]

    return _TOKEN.sub(_sub, sql)


created = []
for filename in sql_files:
    view_name = os.path.splitext(filename)[0]
    subs = view_substitutions(configs_by_view[view_name], CATALOG)
    with open(os.path.join(VIEWS_DIR, filename)) as fh:
        rendered = render(fh.read(), filename, subs)
    print(f"--- {filename} ---")
    # A view file holds exactly one CREATE OR REPLACE VIEW statement; run it as one statement.
    spark.sql(rendered)
    created.append(filename)

print(f"deployed {len(created)} view(s): {', '.join(created)}")
dbutils.notebook.exit(f"deployed {len(created)} view(s): {', '.join(created)}")

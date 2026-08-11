# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: deploy views
# MAGIC
# MAGIC Creates or replaces one Databricks view per Elasticsearch index. Each view is a `.sql` file in
# MAGIC `views/`, paired with a `pipeline_definitions/<name>.yml` that says where its view, source, and
# MAGIC reference tables live (fully qualified, with an optional `${environment}` component). This
# MAGIC notebook renders each view's `${...}` parameters from its config and runs it with `spark.sql`.
# MAGIC
# MAGIC Parameter (set by the DAB job as a widget):
# MAGIC - `environment`: folded into any config name that contains `${environment}` (e.g.
# MAGIC   `ocsf_${environment}` -> `ocsf_prod`). May be empty when no config name uses the token.

# COMMAND ----------
# Read the environment. Unlike catalog/schema (which now live in each config), environment MAY be
# empty: not every deployment has one, and a config whose names use no ${environment} token needs
# none. A config that DOES use the token but gets an empty environment fails closed later, in
# resolve_name -- so we do not reject empty here.
dbutils.widgets.text("environment", "", "Environment folded into ${environment} in config names")
ENVIRONMENT = dbutils.widgets.get("environment").strip()
print(f"environment = {ENVIRONMENT!r}")

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

# Caps for the debug print of rendered SQL. A view is a single CREATE OR REPLACE VIEW statement, so
# these are never hit in normal use; they exist only so that a pathologically large view can't make
# this diagnostic logging itself blow the notebook's output size limit and fail an otherwise-good
# deploy. spark.sql still runs the FULL SQL regardless -- only the printed copy is capped.
_SQL_PRINT_MAX_LINES = 200
_SQL_PRINT_MAX_CHARS = 20_000


def render(sql: str, filename: str, subs: dict) -> str:
    def _sub(m: "re.Match") -> str:
        key = m.group(1)
        if key not in subs:
            raise ValueError(
                f"{filename}: unknown parameter ${{{key}}}; available: {sorted(subs)}"
            )
        return subs[key]

    return _TOKEN.sub(_sub, sql)


def print_sql(sql: str) -> None:
    """Print the rendered SQL for debugging, capped so a huge view can't flood notebook output.

    Truncates by whichever limit hits first (lines or characters) and prints a clear notice with the
    full size, so it's obvious the display was clipped and by how much.
    """
    lines = sql.splitlines()
    clipped = lines[:_SQL_PRINT_MAX_LINES]
    shown, chars = [], 0
    for line in clipped:
        if chars + len(line) + 1 > _SQL_PRINT_MAX_CHARS:
            break
        shown.append(line)
        chars += len(line) + 1
    for line in shown:
        print(f"      {line}")
    if len(shown) < len(lines):
        print(
            f"      ... [truncated for display: showing {len(shown)} of {len(lines)} lines, "
            f"{len(sql)} chars total; full SQL is still executed]"
        )


# Best-effort per view: each view is an independent CREATE OR REPLACE, so one view's failure
# (a bad environment resolution, a missing source table, a SQL error) must NOT stop the others.
# We attempt every view, collect failures, then FAIL the job at the end if any view failed -- a
# partial deploy must never report green (fail closed).
created = []
failed = []
for filename in sql_files:
    view_name = os.path.splitext(filename)[0]
    print(f"--- {filename} ---")
    try:
        subs = view_substitutions(configs_by_view[view_name], ENVIRONMENT)
        with open(os.path.join(VIEWS_DIR, filename)) as fh:
            rendered = render(fh.read(), filename, subs)
        # Print the fully-rendered SQL that is about to run (all ${...} substituted, ${environment}
        # folded in) so the exact CREATE OR REPLACE is visible in the job output for debugging.
        print(f"    substitutions: {subs}")
        print("    running SQL:")
        print_sql(rendered)
        # A view file holds exactly one CREATE OR REPLACE VIEW statement; run it as one statement.
        spark.sql(rendered)
        created.append(filename)
        print(f"    created {view_name}")
    except Exception as exc:  # noqa: BLE001 -- deliberately continue to the next view
        failed.append((filename, exc))
        print(f"    FAILED {view_name}: {type(exc).__name__}: {exc}")

print(f"deployed {len(created)} view(s): {', '.join(created) or '(none)'}")
if failed:
    summary = "; ".join(f"{f}: {type(e).__name__}: {e}" for f, e in failed)
    # Raise so the job run fails: the successful views are already deployed, but a partial run is
    # not a green run.
    raise RuntimeError(f"{len(failed)} view(s) failed to deploy: {summary}")

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output, visually replacing any print() output from the same cell. Keeping it separate
# leaves the per-view SQL / substitution prints above visible in their own completed cell.
dbutils.notebook.exit(f"deployed {len(created)} view(s): {', '.join(created)}")

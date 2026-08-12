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
# resolve_name - so we do not reject empty here.
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

from pipeline_lib.config import column_present, load_config, view_substitutions  # noqa: E402

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

# Caps for the debug print of rendered SQL. These are deliberately set FAR above any realistic view
# (even a view listing 1000+ columns one-per-line is well under both) and exist only as a backstop so
# a pathologically large view can't make this diagnostic logging itself blow the notebook's output
# size limit (10 MB/cell) and fail an otherwise-good deploy. The char cap is the real guard (bytes are
# what the notebook limit measures); the line cap is a secondary proxy. At ~200 KB the printed SQL is
# still ~50x under the cell limit. spark.sql always runs the FULL SQL regardless - only the printed
# copy is capped.
_SQL_PRINT_MAX_LINES = 2_000
_SQL_PRINT_MAX_CHARS = 200_000

# Cumulative cap across ALL views in this run. deploy_views prints every view's SQL into ONE cell, so
# the per-view cap above bounds a single runaway view but not the sum: e.g. 100 views each near the
# per-view cap would be ~20 MB, over the 10 MB cell limit (and toward the 30 MB job-notebook limit
# that fails the run). Once total printed SQL crosses this budget, later views' SQL is suppressed (a
# one-line notice prints instead); every view still deploys. Set well under the 10 MB cell limit.
_SQL_PRINT_TOTAL_BUDGET = 4_000_000


def render(sql: str, filename: str, subs: dict) -> str:
    def _sub(m: "re.Match") -> str:
        key = m.group(1)
        if key not in subs:
            raise ValueError(
                f"{filename}: unknown parameter ${{{key}}}; available: {sorted(subs)}"
            )
        return subs[key]

    return _TOKEN.sub(_sub, sql)


_SQL_PRINT_INDENT = "      "  # each printed SQL line is indented by this


def print_sql(sql: str) -> int:
    """Print the rendered SQL for debugging, capped so a huge view can't flood notebook output.

    Truncates by whichever limit hits first (lines or characters) and prints a clear notice with the
    full size, so it's obvious the display was clipped and by how much. The character budget counts
    the printed form (indent included), since it exists to bound actual notebook output. At least the
    first line is always shown - itself hard-truncated if that single line exceeds the budget - so
    there is never a case where only the truncation notice prints with no SQL content.

    Returns the ACTUAL number of characters emitted (every printed line, including the indent, the
    hard-truncated first line, and the truncation notice), so the caller's cumulative budget reflects
    real output. Returning an undercount here would let the cumulative cap be defeated by exactly the
    pathological input it exists for.
    """
    emitted = 0

    def emit(text: str) -> None:
        nonlocal emitted
        print(text)
        emitted += len(text) + 1  # + newline

    lines = sql.splitlines()
    per_line_overhead = len(_SQL_PRINT_INDENT) + 1  # indent + newline
    shown, chars, truncated_line = [], 0, False
    for line in lines[:_SQL_PRINT_MAX_LINES]:
        cost = len(line) + per_line_overhead
        if chars + cost > _SQL_PRINT_MAX_CHARS:
            if not shown:
                # A single first line larger than the whole budget: show it, hard-truncated, so some
                # SQL is always visible rather than only the notice.
                budget = max(0, _SQL_PRINT_MAX_CHARS - per_line_overhead)
                shown.append(line[:budget])
                truncated_line = True
            break
        shown.append(line)
        chars += cost
    for line in shown:
        emit(f"{_SQL_PRINT_INDENT}{line}")
    if truncated_line:
        # The one shown line was itself hard-truncated (a single line bigger than the whole budget),
        # so a "1 of 1 lines" count would misleadingly imply nothing was clipped.
        emit(
            f"{_SQL_PRINT_INDENT}... [truncated for display: first line hard-truncated; "
            f"{len(sql)} chars total; full SQL is still executed]"
        )
    elif len(shown) < len(lines):
        emit(
            f"{_SQL_PRINT_INDENT}... [truncated for display: showing "
            f"{len(shown)} of {len(lines)} line(s), {len(sql)} chars total; full SQL is still executed]"
        )
    return emitted


# Best-effort per view: each view is an independent CREATE OR REPLACE, so one view's failure
# (a bad environment resolution, a missing source table, a SQL error) must NOT stop the others.
# We attempt every view, collect failures, then FAIL the job at the end if any view failed - a
# partial deploy must never report green (fail closed).
created = []
failed = []
printed_chars = 0  # cumulative SQL chars printed so far, to bound total cell output across all views
for filename in sql_files:
    view_name = os.path.splitext(filename)[0]
    print(f"--- {filename} ---")
    try:
        cfg = configs_by_view[view_name]
        subs = view_substitutions(cfg, ENVIRONMENT)
        with open(os.path.join(VIEWS_DIR, filename)) as fh:
            rendered = render(fh.read(), filename, subs)
        # Print the substitutions and the fully-rendered SQL that is about to run (all ${...}
        # substituted, ${environment} folded in) so the exact CREATE OR REPLACE is visible in the job
        # output for debugging - but only until the cumulative budget is reached, so many views can't
        # flood the cell's output. Both prints are gated and counted against the same budget.
        if printed_chars < _SQL_PRINT_TOTAL_BUDGET:
            subs_line = f"    substitutions: {subs}"
            print(subs_line)
            printed_chars += len(subs_line) + 1
            print("    running SQL:")
            printed_chars += print_sql(rendered)
        else:
            print("    [debug print suppressed: cumulative SQL-print budget reached; view still deploys]")
        # A view file holds exactly one CREATE OR REPLACE VIEW statement; run it as one statement.
        spark.sql(rendered)
        # Verify the config's es_id_field is an actual output column of the view just created. This
        # is the ground-truth check (Spark's own resolved schema, not a parse of the .sql), and it
        # runs here rather than in the offline generator because the generator has no Spark. A typo'd
        # es_id_field or a view that renamed the column would otherwise only surface much later, when
        # the connector is handed a nonexistent _id column. Fail closed per-view (collected below).
        fqn = subs["view"]  # catalog.schema.name, ${environment} already folded in
        es_id_field = cfg["es_id_field"]
        view_columns = spark.table(fqn).columns
        # column_present matches Spark's default (case-INSENSITIVE) column resolution, so a view
        # emitting e.g. `DSL_ID` for a config `dsl_id` is not false-rejected. Original casing is kept
        # in the error text. See pipeline_lib.config.column_present (unit-tested there).
        if not column_present(es_id_field, view_columns):
            raise ValueError(
                f"es_id_field '{es_id_field}' is not an output column of view {fqn}; "
                f"available columns: {view_columns}"
            )
        created.append(filename)
        print(f"    created {view_name} (es_id_field '{es_id_field}' present)")
    except Exception as exc:  # noqa: BLE001 - deliberately continue to the next view
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

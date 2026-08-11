# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: deploy views
# MAGIC
# MAGIC Creates or replaces one Databricks view per Elasticsearch index. Each view is a `.sql` file in
# MAGIC the repo's `views/` folder; this notebook renders the catalog/schema placeholders from job
# MAGIC parameters and runs every file with `spark.sql`. Adding an index is adding a `.sql` file.
# MAGIC
# MAGIC Parameters (set by the DAB job as widgets):
# MAGIC - `view_catalog`, `view_schema`: where the views are created (required).
# MAGIC - `source_catalog`, `source_schema`: where the source tables are read from (required).
# MAGIC
# MAGIC Known limitation: all tables referenced by one view must share a single
# MAGIC `source_catalog.source_schema`; a view joining tables across schemas is not supported.

# COMMAND ----------
# Read and validate the job parameters. Every one is required with no default: catalog/schema names
# are environment-specific, so each deployment must supply them (fail closed on a missing value).
import re

dbutils.widgets.text("view_catalog", "", "View catalog (where views are created)")
dbutils.widgets.text("view_schema", "", "View schema (where views are created)")
dbutils.widgets.text("source_catalog", "", "Source catalog (where source tables live)")
dbutils.widgets.text("source_schema", "", "Source schema (where source tables live)")

# The placeholders a view .sql file may reference. Used to substitute AND to validate: a file may use
# no unknown placeholder, and every supplied value must be non-empty. Allow-list, not deny-list.
PARAMS = {
    "view_catalog": dbutils.widgets.get("view_catalog").strip(),
    "view_schema": dbutils.widgets.get("view_schema").strip(),
    "source_catalog": dbutils.widgets.get("source_catalog").strip(),
    "source_schema": dbutils.widgets.get("source_schema").strip(),
}
missing = [k for k, v in PARAMS.items() if not v]
if missing:
    raise ValueError(f"missing required parameter(s): {', '.join(missing)}")

# These values are substituted verbatim into the view DDL as bare SQL identifiers (catalog.schema.name),
# so restrict them to a legal unquoted identifier: a letter or underscore first, then letters, digits,
# and underscores. A leading digit is rejected too, because an unquoted identifier cannot start with
# one. Rejecting anything else (a hyphen, space, dot, quote, or SQL-reserved punctuation) fails closed
# at deploy time instead of producing invalid SQL, or worse, binding to the wrong object. Allow-list,
# not deny-list. A name that legitimately needs backtick-quoting is out of scope and must be rejected
# here rather than silently mishandled.
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
illegal = {k: v for k, v in PARAMS.items() if not _VALID_IDENTIFIER.match(v)}
if illegal:
    detail = "; ".join(f"{k}={v!r}" for k, v in illegal.items())
    raise ValueError(
        "parameter(s) are not a legal SQL identifier (letters, digits, underscore only): " + detail
    )

# COMMAND ----------
# Locate the views/ folder. This notebook is synced to <bundle files>/notebooks/deploy_views.py; the SQL
# lives in the sibling <bundle files>/views/. Resolve it from the notebook's own workspace path so
# it works when run as a DAB job (no hardcoded workspace path).
import os

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_files_root = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files
VIEWS_DIR = os.path.join(_files_root, "views")
print("views dir:", VIEWS_DIR)

sql_files = sorted(f for f in os.listdir(VIEWS_DIR) if f.endswith(".sql"))
if not sql_files:
    raise ValueError(f"no .sql view files found in {VIEWS_DIR}")
print(f"found {len(sql_files)} view file(s): {', '.join(sql_files)}")

# COMMAND ----------
# Render each file's ${placeholder} tokens and run it. Substitution is explicit (not str.format,
# which would choke on any literal braces in SQL) and fail-closed: an unknown ${token} in a file is
# an error, not a silently-unsubstituted string that would create a broken view.
_TOKEN = re.compile(r"\$\{(\w+)\}")


def render(sql: str, filename: str) -> str:
    def _sub(m: "re.Match") -> str:
        key = m.group(1)
        if key not in PARAMS:
            raise ValueError(
                f"{filename}: unknown placeholder ${{{key}}}; allowed: {sorted(PARAMS)}"
            )
        return PARAMS[key]

    return _TOKEN.sub(_sub, sql)


created = []
for filename in sql_files:
    with open(os.path.join(VIEWS_DIR, filename)) as fh:
        rendered = render(fh.read(), filename)
    print(f"--- {filename} ---")
    # A view file is expected to hold exactly one CREATE OR REPLACE VIEW statement; splitting on ';'
    # is unnecessary and error-prone, so run the file as one statement.
    spark.sql(rendered)
    created.append(filename)

print(f"deployed {len(created)} view(s): {', '.join(created)}")
dbutils.notebook.exit(f"deployed {len(created)} view(s): {', '.join(created)}")

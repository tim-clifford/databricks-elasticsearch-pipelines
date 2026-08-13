# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: per-index pipeline runner
# MAGIC
# MAGIC The shared notebook run by every per-index job. It installs the connector wheel (verifying the
# MAGIC import), loads `pipeline_definitions/<config_name>.yml`, resolves `${environment}` into the object
# MAGIC names, and exports the config's view to Elasticsearch via the connector's `bulk_write`.
# MAGIC
# MAGIC Currently `pipeline_mode=batch` is implemented (read the whole view, optionally filter, bulk write);
# MAGIC `pipeline_mode=streaming` is a stub that raises until the streaming phase lands.
# MAGIC
# MAGIC Why load the config here rather than receive resolved values: the job resources are generated
# MAGIC offline by scripts/gen_jobs.py, which cannot know the deploy-time environment, so it cannot bake
# MAGIC resolved catalog/schema names into the job. The notebook resolves them at runtime instead.
# MAGIC
# MAGIC Deploy-time parameters (base_parameters; from bundle variables, fixed at deploy):
# MAGIC - `config_name`: the pipeline definition to load (`pipeline_definitions/<config_name>.yml`).
# MAGIC - `environment`: folded into any `${environment}` in the config's object names (may be empty).
# MAGIC - `wheel_path`: UC Volume path to the connector `.whl` to install (required).
# MAGIC - `es_host_url`, `secret_scope_name`, `secret_key_name`: the ES endpoint, and the Databricks
# MAGIC   secret scope/key holding the ES api_key (all required for an index-job run).
# MAGIC
# MAGIC Run-time parameters (job parameters; overridable per run with `--params <name>=<value>`):
# MAGIC - `pipeline_mode`: `batch` | `streaming` (default from config).
# MAGIC - `filter_condition`: optional Spark SQL predicate applied before the write (default from config).
# MAGIC - `chunk_size`, `require_existing_index`, `verify_certs`: EsWriteConfig tuning; empty => connector default.

# COMMAND ----------
# FIRST, install the connector wheel and restart Python. This cell handles ONLY the wheel, because
# restartPython() discards all Python interpreter state (including any widget values read into
# variables), so any work done before it would just have to be redone. Reading config_name/environment
# is therefore deferred to after the restart. %pip can't expand a widget inside a literal
# `%pip install <path>`, so we read wheel_path in Python and invoke the pip magic programmatically.
# restartPython() MUST be the last statement in the cell (it ends the cell).
#
# FOLLOW-UP (not this PR): on serverless, the wheel could instead be declared as a task-level
# `environment` dependency (job `environments[].spec.dependencies`, referenced via `environment_key`),
# resolved once at environment setup rather than reinstalled per run. That is a separate refactor of
# the install mechanism; this in-notebook %pip approach is intentional and verified for now.
import shlex

dbutils.widgets.text("wheel_path", "", "Connector wheel path (UC Volume .whl)")
WHEEL_PATH = dbutils.widgets.get("wheel_path").strip()
if not WHEEL_PATH:
    raise ValueError(
        "wheel_path is required: the UC Volume path to the databricks_es_connector wheel, e.g. "
        "/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"
    )
print(f"installing connector wheel from {WHEEL_PATH}")
# shlex.quote the path so a UC Volume filename containing a space (or any pip-meaningful token) is
# passed to pip as ONE argument, not split or interpreted as extra pip options. Verified live: a wheel
# at a path containing a space installs and imports fine, so Databricks' %pip honors the quoting.
get_ipython().run_line_magic("pip", f"install {shlex.quote(WHEEL_PATH)}")
dbutils.library.restartPython()

# COMMAND ----------
# Verify the connector is importable and report its version, before any export work. What each step
# proves: a nonexistent or broken wheel_path already failed the %pip install above (seen live). This
# import then catches the case where the install ran but the package still isn't importable. It does
# NOT prove THIS wheel_path's build is the one loaded (a connector already present on the runtime would
# also satisfy the import), so it is an importability check, not a version-match assertion.
import databricks_es_connector  # noqa: E402

# getattr fallback: the successful import above is the real install-succeeds signal; a build that
# happens not to expose __version__ shouldn't turn a good install into an AttributeError here.
_connector_version = getattr(databricks_es_connector, "__version__", "unknown")
print(f"connector installed: databricks_es_connector {_connector_version}")

# The write surface used by the export cell below. Imported here (after the restart) so the export
# cell reads as pure orchestration. bulk_write does the mapInPandas export; reconcile_or_raise turns
# the result dict into an exception when any document was rejected or any row went unaccounted for.
from databricks_es_connector import EsWriteConfig, bulk_write, reconcile_or_raise  # noqa: E402

# COMMAND ----------
# Now read the remaining parameters (the restart above cleared any earlier Python state, so this is
# their first and only read). config_name is required; environment may be empty (a config that uses no
# ${environment} token needs none, and one that does fails closed later in resolve_config).
#
# Two kinds of parameter arrive as widgets (see the header): deploy-time base_parameters and run-time
# job parameters. pipeline_mode / filter_condition / the tuning knobs are JOB PARAMETERS: the
# generated job sets their defaults (from the config, or "" for the tuning knobs), and each is
# overridable per run with `--params <name>=<value>`. We read the EFFECTIVE value here (default or
# override) and validate below, so the widget, not the config value, is the source of truth at run
# time. Empty defaults fail closed at validation rather than silently assuming a value.
dbutils.widgets.text("config_name", "", "Pipeline definition name (pipeline_definitions/<config_name>.yml)")
dbutils.widgets.text("environment", "", "Environment folded into ${environment} in config names")
dbutils.widgets.text("es_host_url", "", "Elasticsearch endpoint, e.g. https://<host>:9200")
dbutils.widgets.text("secret_scope_name", "", "Databricks secret scope holding the ES api_key")
dbutils.widgets.text("secret_key_name", "", "Key in the scope whose value is the ES api_key")
dbutils.widgets.text("pipeline_mode", "", "Export mode: batch | streaming (job parameter; overridable per run)")
dbutils.widgets.text("filter_condition", "", "Optional row filter, a Spark SQL predicate (overridable per run)")
dbutils.widgets.text("chunk_size", "", "EsWriteConfig chunk_size override (empty => connector default)")
dbutils.widgets.text("require_existing_index", "", "EsWriteConfig require_existing_index: true|false (empty => default)")
dbutils.widgets.text("verify_certs", "", "EsWriteConfig verify_certs: true|false (empty => default)")
CONFIG_NAME = dbutils.widgets.get("config_name").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip()
ES_HOST_URL = dbutils.widgets.get("es_host_url").strip()
SECRET_SCOPE_NAME = dbutils.widgets.get("secret_scope_name").strip()
SECRET_KEY_NAME = dbutils.widgets.get("secret_key_name").strip()
PIPELINE_MODE = dbutils.widgets.get("pipeline_mode").strip()
FILTER_CONDITION = dbutils.widgets.get("filter_condition").strip()
CHUNK_SIZE = dbutils.widgets.get("chunk_size").strip()
REQUIRE_EXISTING_INDEX = dbutils.widgets.get("require_existing_index").strip()
VERIFY_CERTS = dbutils.widgets.get("verify_certs").strip()
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

from pipeline_lib.config import (  # noqa: E402
    load_config,
    require_filter_condition,
    require_pipeline_mode,
    resolve_config,
    write_config_overrides,
)

# Validate the run-time job-parameter values FIRST, before the config file I/O below, so a bad
# override fails closed immediately without wasting the config load/resolve on a run that can't
# proceed. Each uses the same shared validator the config schema uses (single source of truth):
# - pipeline_mode: allow-list (batch|streaming); a bad value (e.g. --params pipeline_mode=turbo) fails.
# - filter_condition: must be a string (the SQL expression itself is validated by Spark at df.filter).
# - write_overrides: EsWriteConfig tuning knobs, parsed from their string widgets; an unset knob is
#   omitted so the connector default stands, and a bad value (chunk_size=abc, verify_certs=maybe) fails.
PIPELINE_MODE = require_pipeline_mode(PIPELINE_MODE, "pipeline_mode job parameter")
FILTER_CONDITION = require_filter_condition(FILTER_CONDITION, "filter_condition job parameter")
write_overrides = write_config_overrides(CHUNK_SIZE, REQUIRE_EXISTING_INDEX, VERIFY_CERTS)

# The global ES connection settings are required for any index-job run: fail closed on an empty one
# (e.g. a deploy that forgot --var=es_host_url) rather than constructing a broken EsWriteConfig.
for _param, _value in (
    ("es_host_url", ES_HOST_URL),
    ("secret_scope_name", SECRET_SCOPE_NAME),
    ("secret_key_name", SECRET_KEY_NAME),
):
    if not _value:
        raise ValueError(f"missing required parameter: {_param} (set the bundle variable at deploy)")

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
# Echo the resolved configuration + effective run-time settings before the export, so a run's log
# shows exactly what it is about to do (which view, which index, which mode/filter/overrides).
view = cfg["view"]
source = cfg["source"]
VIEW_FQN = f"{view['catalog']}.{view['schema']}.{view['name']}"
print(f"config_name        = {CONFIG_NAME}")
print(f"environment        = {ENVIRONMENT!r}")
print(f"es_index_name      = {cfg['es_index_name']}")
print(f"es_id_field        = {cfg['es_id_field']}")
print(f"pipeline_mode      = {PIPELINE_MODE}")
print(f"filter_condition   = {FILTER_CONDITION!r}")
print(f"write_overrides    = {write_overrides}")
print(f"view               = {VIEW_FQN}")
print(f"source             = {source['catalog']}.{source['schema']}.{source['table']}")
print(f"es_host_url        = {ES_HOST_URL}")

# COMMAND ----------
# Build the connector write config. Everything here is MODE-INDEPENDENT (batch and streaming write
# through the same EsWriteConfig), so it lives above the mode branch and streaming reuses it as-is.
#
# api_key auth: the secret's value is passed straight to the connector as api_key. dbutils.secrets
# reads it on the DRIVER; EsWriteConfig is a plain frozen dataclass that carries the string into the
# executor closure (the connector builds the ES client per-partition from it). Redaction: the value
# is a Databricks secret, so it is automatically redacted from notebook output if printed.
#
# write_overrides splats in only the tuning knobs that were set this run (chunk_size /
# require_existing_index / verify_certs); an unset knob is absent, leaving the connector's own
# default in force. index and id_field come from the (validated, resolved) config.
es_write_config = EsWriteConfig(
    hosts=ES_HOST_URL,
    api_key=dbutils.secrets.get(SECRET_SCOPE_NAME, SECRET_KEY_NAME),
    index=cfg["es_index_name"],
    id_field=cfg["es_id_field"],
    **write_overrides,
)


def read_view_df():
    """The rows to export: the whole view, with the optional filter_condition applied.

    Shared by both modes so the source-of-rows logic is written once. Batch reads it as a static
    DataFrame; the streaming phase will read the same VIEW_FQN with spark.readStream and reuse this
    same filter step, so the filter is applied here rather than duplicated per mode.
    """
    df = spark.table(VIEW_FQN)
    if FILTER_CONDITION:
        df = df.filter(FILTER_CONDITION)
    return df


# COMMAND ----------
# Export. The mode branch is the ONE place batch and streaming differ; both share es_write_config and
# read_view_df above. Keeping the branch this thin is deliberate, so adding streaming is additive
# (fill in the elif) rather than a rewrite.
if PIPELINE_MODE == "batch":
    # Batch: read the whole (optionally filtered) view and bulk_write it. bulk_write returns the
    # count dict; reconcile_or_raise then FAILS the run if any document was rejected (errors > 0) or
    # any row went unaccounted for, so a partial export surfaces as a job failure, not a silent
    # success. (raise_on_error=False here so the result is printed for the log before we reconcile.)
    export_df = read_view_df()
    result = bulk_write(export_df, es_write_config)
    print(f"bulk_write result: {result}")
    reconcile_or_raise(result, index=es_write_config.index)
    RUN_SUMMARY = (
        f"written={result['written']} deleted={result['deleted']} errors={result['errors']} "
        f"ignored={result['ignored']} total_input={result['total_input']}"
    )
elif PIPELINE_MODE == "streaming":
    # STREAMING IS NOT IMPLEMENTED YET (lands in the next phase). It will reuse es_write_config and
    # read_view_df above: spark.readStream.table(VIEW_FQN) through the same filter, written via the
    # connector's make_foreach_batch(es_write_config). Fail closed rather than silently no-op.
    raise NotImplementedError(
        "pipeline_mode=streaming is not implemented yet; use pipeline_mode=batch. Streaming export "
        "lands in a later phase."
    )
else:
    # Unreachable: require_pipeline_mode already allow-list validated PIPELINE_MODE above. Kept as a
    # fail-closed backstop so a future mode added to the allow-list without a branch here cannot
    # silently do nothing.
    raise ValueError(f"unhandled pipeline_mode {PIPELINE_MODE!r}")

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output, visually replacing any print() output from the same cell. Keeping it separate
# leaves the prints above visible in their own completed cells.
dbutils.notebook.exit(
    f"config_name={CONFIG_NAME}; es_index_name={cfg['es_index_name']}; pipeline_mode={PIPELINE_MODE}; "
    f"view={VIEW_FQN}; {RUN_SUMMARY}"
)

# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: per-index pipeline runner
# MAGIC
# MAGIC The shared notebook run by every per-index job. It installs the connector wheel (verifying the
# MAGIC import), loads `pipeline_definitions/<config_name>.yml`, resolves `${environment}` into the object
# MAGIC names, and exports the config's view to Elasticsearch via the connector's `bulk_write`.
# MAGIC
# MAGIC Both modes are implemented. `pipeline_mode=batch` reads the whole deployed view, optionally
# MAGIC filters, and bulk-writes it. `pipeline_mode=streaming` reads the RAW source table as a stream
# MAGIC (Trigger.availableNow), renders the view's OWN SELECT over each micro-batch (so the view logic
# MAGIC runs against batch-sized data, never a join back to the full view), and bulk-writes per batch
# MAGIC through the connector's foreachBatch writer with a checkpoint.
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
# MAGIC - `checkpoint_base_path`: UC Volume base for streaming checkpoints; the runner appends
# MAGIC   `/<config_name>` (required for a streaming run; unused by batch).
# MAGIC
# MAGIC Run-time parameters (job parameters; overridable per run with `--params <name>=<value>`):
# MAGIC - `pipeline_mode`: `batch` | `streaming` (default from config).
# MAGIC - `filter_condition`: optional Spark SQL predicate applied before the write (default from config).
# MAGIC - `chunk_size`, `require_existing_index`, `verify_certs`: EsWriteConfig tuning; empty => connector default.
# MAGIC - `streaming_start`: `new` (default; only new commits) | `full` (backfill the whole table);
# MAGIC   streaming only, honored on the first run before a checkpoint exists.

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
# cell reads as pure orchestration. bulk_write does the batch mapInPandas export; reconcile_or_raise
# turns the result dict into an exception when any document was rejected or any row went unaccounted
# for; make_foreach_batch wraps that same write+reconcile into a foreachBatch fn for the streaming path.
from databricks_es_connector import (  # noqa: E402
    EsWriteConfig,
    bulk_write,
    make_foreach_batch,
    reconcile_or_raise,
)

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
# Streaming-only widgets. checkpoint_base_path is a deploy-time base_parameter (bundle variable);
# streaming_start is a run-time job parameter (default "new"). Both are ignored by a batch run.
dbutils.widgets.text("checkpoint_base_path", "", "UC Volume base for streaming checkpoints (runner appends /<config_name>)")
dbutils.widgets.text("streaming_start", "", "Streaming start: new (only new commits) | full (backfill whole table)")
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
CHECKPOINT_BASE_PATH = dbutils.widgets.get("checkpoint_base_path").strip()
STREAMING_START = dbutils.widgets.get("streaming_start").strip()
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
    render_view_sql,
    require_filter_condition,
    require_pipeline_mode,
    require_streaming_start,
    resolve_config,
    view_select_body,
    view_substitutions,
    write_config_overrides,
)

# Validate the run-time job-parameter values FIRST, before the config file I/O below, so a bad
# override fails closed immediately without wasting the config load/resolve on a run that can't
# proceed. Each uses the same shared validator the config schema uses (single source of truth):
# - pipeline_mode: allow-list (batch|streaming); a bad value (e.g. --params pipeline_mode=turbo) fails.
# - filter_condition: must be a string (the SQL expression itself is validated by Spark at df.filter).
# - write_overrides: EsWriteConfig tuning knobs, parsed from their string widgets; an unset knob is
#   omitted so the connector default stands, and a bad value (chunk_size=abc, verify_certs=maybe) fails.
# - streaming_start: allow-list (new|full). Validated unconditionally (cheap, fails a bad --params
#   value regardless of mode); only actually USED by the streaming branch. Empty widget -> "new"
#   default, so an unset value takes the intended default rather than failing.
PIPELINE_MODE = require_pipeline_mode(PIPELINE_MODE, "pipeline_mode job parameter")
FILTER_CONDITION = require_filter_condition(FILTER_CONDITION, "filter_condition job parameter")
write_overrides = write_config_overrides(CHUNK_SIZE, REQUIRE_EXISTING_INDEX, VERIFY_CERTS)
STREAMING_START = require_streaming_start(STREAMING_START or "new", "streaming_start job parameter")

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
SOURCE_FQN = f"{source['catalog']}.{source['schema']}.{source['table']}"
print(f"config_name        = {CONFIG_NAME}")
print(f"environment        = {ENVIRONMENT!r}")
print(f"es_index_name      = {cfg['es_index_name']}")
print(f"es_id_field        = {cfg['es_id_field']}")
print(f"pipeline_mode      = {PIPELINE_MODE}")
print(f"filter_condition   = {FILTER_CONDITION!r}")
print(f"write_overrides    = {write_overrides}")
print(f"view               = {VIEW_FQN}")
print(f"source             = {SOURCE_FQN}")
print(f"es_host_url        = {ES_HOST_URL}")
if PIPELINE_MODE == "streaming":
    print(f"streaming_start    = {STREAMING_START}")

# COMMAND ----------
# Build the connector write config. Everything here is MODE-INDEPENDENT (batch and streaming write
# through the same EsWriteConfig), so it lives above the mode branch and both modes reuse it as-is.
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


def apply_filter(df):
    """Apply the optional filter_condition to a DataFrame. Shared by both modes so the filter step is
    written once and applied identically to a batch DataFrame or a streaming micro-batch."""
    return df.filter(FILTER_CONDITION) if FILTER_CONDITION else df


# COMMAND ----------
# Export. The mode branch is the ONE place batch and streaming differ; both share es_write_config and
# apply_filter above, and both run the SAME view transform (batch reads the deployed view; streaming
# renders that view's SELECT over each micro-batch - see the streaming branch).
if PIPELINE_MODE == "batch":
    # Batch: read the whole (optionally filtered) deployed view and bulk_write it. bulk_write returns
    # the count dict; reconcile_or_raise then FAILS the run if any document was rejected (errors > 0)
    # or any row went unaccounted for, so a partial export surfaces as a job failure, not a silent
    # success. (raise_on_error=False here so the result is printed for the log before we reconcile.)
    export_df = apply_filter(spark.table(VIEW_FQN))
    result = bulk_write(export_df, es_write_config)
    print(f"bulk_write result: {result}")
    reconcile_or_raise(result, index=es_write_config.index)
    RUN_SUMMARY = (
        f"written={result['written']} deleted={result['deleted']} errors={result['errors']} "
        f"ignored={result['ignored']} total_input={result['total_input']}"
    )
elif PIPELINE_MODE == "streaming":
    # Streaming export. The design constraint: we must NOT read the deployed VIEW and join it back to
    # each micro-batch (that would scan the huge source side of the view every trigger). Instead we
    # take the view's OWN SELECT and run it with ${source} bound to a temp view over just the
    # micro-batch, so the identical transform the deployed view applies runs against batch-sized data.
    # Reference tables (${ref_*}) stay their real FQNs - a reference join is small-batch-to-dimension.

    # checkpoint_base_path is required ONLY for a streaming run (batch/deploy_views never stream), so
    # it is validated here rather than up top. Per-stream subfolder keyed by config_name (stable +
    # unique + filesystem-safe), so each stream's checkpoint is isolated and survives across runs.
    if not CHECKPOINT_BASE_PATH:
        raise ValueError(
            "missing required parameter: checkpoint_base_path (set the bundle variable at deploy); "
            "a streaming run needs a UC Volume checkpoint location"
        )
    checkpoint_location = f"{CHECKPOINT_BASE_PATH.rstrip('/')}/{CONFIG_NAME}"

    # The view's SELECT body, with ${source} bound to the per-batch temp view and ${ref_*} left as the
    # real reference tables. Extracted + rendered from the SAME .sql the deployed view uses (shared
    # renderer), so streaming and batch provably apply identical transform logic. Rendered ONCE here
    # (the SQL text is constant across micro-batches); only the temp view's contents change per batch.
    _view_file = os.path.join(FILES_ROOT, "views", f"{view['name']}.sql")
    with open(_view_file) as _fh:
        _view_sql = _fh.read()
    # The per-batch temp view name is substituted UNQUOTED into the rendered SELECT's FROM (via
    # source_override), so it must be a bare SQL identifier. config_name is only [A-Za-z0-9_-]+ (a
    # bundle resource key), which permits hyphens - and a hyphen is not a legal bare identifier, so a
    # hyphenated config would produce `FROM _stream_src_my-index`, a parse error failing every
    # streaming run for that config. Sanitize non-identifier chars to '_'. The name only has to be
    # valid + stable within THIS run's Spark session (a temp view is session-scoped, and each job run
    # is a single config), so collapsing e.g. '-' to '_' cannot collide with another config's view.
    # The `_stream_src_` prefix also guarantees a letter/underscore start regardless of config_name.
    _safe_config = "".join(c if (c.isalnum() or c == "_") else "_" for c in CONFIG_NAME)
    BATCH_SOURCE_VIEW = f"_stream_src_{_safe_config}"
    stream_subs = view_substitutions(cfg, ENVIRONMENT, source_override=BATCH_SOURCE_VIEW)
    RENDERED_SELECT = render_view_sql(view_select_body(_view_sql, _view_file), stream_subs, _view_file)
    print(f"checkpoint_location = {checkpoint_location}")
    print(f"rendered micro-batch SELECT (source bound to {BATCH_SOURCE_VIEW}):\n{RENDERED_SELECT}")

    def transform_micro_batch(batch_df, batch_id: int):
        """Run the view's SELECT over one raw-source micro-batch, returning the transformed rows.

        The batch is registered as the ${source} temp view, then the rendered view SELECT runs
        against it (so joins/projections/hints all execute exactly as the deployed view defines them,
        but only over this batch). The optional filter_condition is applied to the result. Kept as a
        pure DataFrame->DataFrame transform so the connector's make_foreach_batch can wrap it.
        """
        batch_df.createOrReplaceTempView(BATCH_SOURCE_VIEW)
        return apply_filter(spark.sql(RENDERED_SELECT))

    # Wrap the connector's writer so each micro-batch is transformed (raw source -> view logic ->
    # filter) BEFORE it is bulk-written. make_foreach_batch handles the write + reconcile: it raises
    # on a failed batch so the checkpoint does not advance past lost rows (at-least-once + deterministic
    # _id => effectively exactly-once on retry).
    _write_batch = make_foreach_batch(es_write_config)

    def foreach_batch(batch_df, batch_id: int):
        _write_batch(transform_micro_batch(batch_df, batch_id), batch_id)

    # Read the RAW source table as a stream (not the view: we apply the view logic per-batch above).
    # skipChangeCommits=true: tolerate non-append commits (a manual UPDATE/DELETE upstream) by skipping
    # them rather than failing the stream; corrections are handled out-of-band via a batch backfill.
    #
    # streaming_start controls where a FIRST run (no checkpoint yet) begins; once a checkpoint exists
    # it is the position of record and startingVersion is ignored (Spark resumes from the checkpoint).
    # - "new": start at the source's CURRENT Delta version, so existing history is NOT re-exported and
    #   subsequent runs pick up only new commits. We resolve the concrete current version NUMBER rather
    #   than startingVersion="latest" because of a Trigger.availableNow interaction proven live: a
    #   "latest" first run finds no new rows, runs zero micro-batches, and persists NO checkpoint
    #   offset, so it never establishes a resume point and later runs keep skipping new data. A numeric
    #   startingVersion seeds a real, persisted offset even on a zero-row first batch, so the next run
    #   resumes correctly (verified: seed -> append -> resume exports exactly the appended rows).
    #   startingVersion is INCLUSIVE and must be an EXISTING version, so we use the current version
    #   (current+1 is rejected when it does not exist yet). One consequence: if the current commit is
    #   an append, the first "new" run re-exports that single commit's rows. With deterministic _id
    #   this is an idempotent upsert bounded to one commit (not the whole table), so it is harmless.
    # - "full": omit startingVersion, so the first micro-batches backfill the whole existing table.
    reader = spark.readStream.option("skipChangeCommits", "true")
    if STREAMING_START == "new":
        current_version = (
            spark.sql(f"DESCRIBE HISTORY {SOURCE_FQN}")
            .agg({"version": "max"})
            .collect()[0][0]
        )
        print(f"streaming_start=new: seeding at current source version {current_version} (history skipped)")
        reader = reader.option("startingVersion", str(current_version))
    stream_df = reader.table(SOURCE_FQN)

    # Trigger.availableNow: drain every currently-available source commit in one or more micro-batches,
    # then stop. This is the supported serverless streaming trigger (processingTime is rejected) and
    # fits the DAB job model - each job RUN exports the new data since the last run and terminates,
    # rather than holding an always-on cluster. Schedule the job (or run on demand) to pick up new data.
    query = (
        stream_df.writeStream
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .foreachBatch(foreach_batch)
        .start()
    )
    query.awaitTermination()
    _progress = query.lastProgress
    _rows = (_progress or {}).get("numInputRows", "unknown")
    print(f"stream drained (availableNow); last-batch numInputRows={_rows}")
    RUN_SUMMARY = f"streaming_start={STREAMING_START}; checkpoint={checkpoint_location}; last_numInputRows={_rows}"
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

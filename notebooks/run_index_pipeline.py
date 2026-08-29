# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: per-index pipeline runner
# MAGIC
# MAGIC The shared notebook run by every per-index job. It installs the connector wheel (verifying the
# MAGIC import), loads `_pipelines/pipeline_configs/<config_name>.yml`, resolves `${environment}` into the object
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
# MAGIC - `config_name`: the pipeline definition to load (`_pipelines/pipeline_configs/<config_name>.yml`).
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
# MAGIC - `chunk_size`, `require_existing_index`, `verify_certs`: EsWriteConfig tuning (default from config;
# MAGIC   omitted there and unset per run => connector default).
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

# The write surface used by the export cells below. Imported here (after the restart) so the export
# cells read as pure orchestration. bulk_write does the mapInPandas export (returns the count dict);
# reconcile_or_raise turns that dict into an exception when any document was rejected or any row went
# unaccounted for. Batch calls bulk_write then reconcile_or_raise; streaming calls
# bulk_write(..., raise_on_error=True) per micro-batch (same write+reconcile in one call) so a failed
# batch fails the trigger and the checkpoint holds.
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
dbutils.widgets.text("config_name", "", "Pipeline definition name (_pipelines/pipeline_configs/<config_name>.yml)")
dbutils.widgets.text("environment", "", "Environment folded into ${environment} in config names")
dbutils.widgets.text("es_host_url", "", "Elasticsearch endpoint, e.g. https://<host>:9200")
dbutils.widgets.text("secret_scope_name", "", "Databricks secret scope holding the ES api_key")
dbutils.widgets.text("secret_key_name", "", "Key in the scope whose value is the ES api_key")
dbutils.widgets.text("pipeline_mode", "", "Export mode: batch | streaming (job parameter; overridable per run)")
dbutils.widgets.text("filter_condition", "", "Optional row filter, a Spark SQL predicate (overridable per run)")
dbutils.widgets.text("chunk_size", "", "EsWriteConfig chunk_size override (empty => connector default)")
dbutils.widgets.text("require_existing_index", "", "EsWriteConfig require_existing_index: true|false (empty => default)")
dbutils.widgets.text("verify_certs", "", "EsWriteConfig verify_certs: true|false (empty => default)")
dbutils.widgets.text("write_repartition", "", "Repartition the write input to N partitions before bulk_write (0 disables; empty => default)")
dbutils.widgets.text("max_partition_bytes", "", "spark.sql.files.maxPartitionBytes for the source read, e.g. 32m (0 leaves it unset; empty => default)")
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
WRITE_REPARTITION = dbutils.widgets.get("write_repartition").strip()
MAX_PARTITION_BYTES = dbutils.widgets.get("max_partition_bytes").strip()
CHECKPOINT_BASE_PATH = dbutils.widgets.get("checkpoint_base_path").strip()
STREAMING_START = dbutils.widgets.get("streaming_start").strip()
if not CONFIG_NAME:
    raise ValueError("missing required parameter: config_name")

# COMMAND ----------
# Resolve the synced bundle root and make pipeline_lib importable. This notebook is synced to
# <bundle files>/notebooks/run_index_pipeline.py; the _pipelines/ tree is a sibling of notebooks/.
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
    require_max_partition_bytes,
    require_pipeline_mode,
    require_streaming_start,
    require_write_repartition,
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
# - write_repartition: non-negative int (0 disables). Validated unconditionally and used by BOTH modes
#   (the batch export and each streaming micro-batch). Empty widget -> the built-in default (the
#   validator turns "" into _DEFAULT_WRITE_REPARTITION), so a standalone run still parallelizes. Parsed
#   to int here since it feeds df.repartition(N).
PIPELINE_MODE = require_pipeline_mode(PIPELINE_MODE, "pipeline_mode job parameter")
FILTER_CONDITION = require_filter_condition(FILTER_CONDITION, "filter_condition job parameter")
write_overrides = write_config_overrides(CHUNK_SIZE, REQUIRE_EXISTING_INDEX, VERIFY_CERTS)
STREAMING_START = require_streaming_start(STREAMING_START or "new", "streaming_start job parameter")
WRITE_REPARTITION = int(require_write_repartition(WRITE_REPARTITION, "write_repartition job parameter"))
# - max_partition_bytes: Spark byte-size (or "0" = leave unset). Validated unconditionally; applied to
#   the source read below (both modes). Empty widget -> the built-in default via the validator.
MAX_PARTITION_BYTES = require_max_partition_bytes(MAX_PARTITION_BYTES, "max_partition_bytes job parameter")

# The ES connection settings are required for any index-job run: fail closed on an empty one rather
# than constructing a broken EsWriteConfig. These come from this pipeline's es_host_config (a complex
# bundle variable in databricks.yml, resolved per target); an empty value means that host config's
# fields were never filled in for the target being deployed - the common cause on a fresh checkout.
for _param, _value in (
    ("es_host_url", ES_HOST_URL),
    ("secret_scope_name", SECRET_SCOPE_NAME),
    ("secret_key_name", SECRET_KEY_NAME),
):
    if not _value:
        raise ValueError(
            f"missing required parameter: {_param} (fill in this pipeline's es_host_config values "
            f"for this target in databricks.yml)"
        )

# checkpoint_base_path is required for a STREAMING run only (batch and deploy_views never stream, so
# their runs leave it empty). Validated here, at the validation stage, so a streaming run with no
# checkpoint location fails closed immediately rather than after the config load and stream setup.
if PIPELINE_MODE == "streaming" and not CHECKPOINT_BASE_PATH:
    raise ValueError(
        "missing required parameter: checkpoint_base_path (set the bundle variable at deploy); "
        "a streaming run needs a UC Volume checkpoint location"
    )

# Resolve the config file, accepting either extension: gen_jobs.py and deploy_views.py both discover
# .yml AND .yaml, so the runner must too, or a .yaml-defined pipeline would deploy fine and then fail
# here at runtime. Fail closed if neither exists.
CONFIG_DIR = os.path.join(FILES_ROOT, "_pipelines", "pipeline_configs")
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
print("es_id_field        = " + (cfg["es_id_field"] or "<unset> (ES auto-generates _id)"))
print(f"pipeline_mode      = {PIPELINE_MODE}")
print(f"filter_condition   = {FILTER_CONDITION!r}")
print(f"write_overrides    = {write_overrides}")
print(f"write_repartition  = {WRITE_REPARTITION}" + (" (disabled: natural partitioning)" if WRITE_REPARTITION == 0 else ""))
print(f"max_partition_bytes= {MAX_PARTITION_BYTES}" + (" (leave engine default)" if MAX_PARTITION_BYTES == "0" else ""))
print(f"view               = {VIEW_FQN}")
print(f"source             = {SOURCE_FQN}")
print(f"es_host_url        = {ES_HOST_URL}")
if PIPELINE_MODE == "streaming":
    print(f"streaming_start    = {STREAMING_START}")
# Loud warning for omitting es_id_field. BOTH modes are at-least-once, so a replay re-writes the same
# source rows with fresh auto-generated _ids and the index accumulates DUPLICATES: for batch, a
# failed-then-retried run re-exports the whole view; for streaming, micro-batch retries, stream
# restarts, and the "new"-mode last-commit re-export below are ROUTINE, so duplication is far more
# likely there. This is ALLOWED (duplicates may be fine for an append-only sink), so it is a warning,
# not a failure - but it must be loud, because idempotency is the safe default.
if not cfg["es_id_field"]:
    _stream_note = (" Streaming replays (micro-batch retries, restarts) are routine, so this is "
                    "especially likely." if PIPELINE_MODE == "streaming" else "")
    print(f"WARNING: {PIPELINE_MODE} pipeline with no es_id_field - ES auto-generates _ids, so a replay "
          f"(a retry or restart) re-inserts rows as NEW documents and accumulates DUPLICATES.{_stream_note} "
          f"Set es_id_field for idempotent upserts; leave it unset only if duplicates are acceptable.")

# Tune read/scan parallelism for BOTH modes by setting spark.sql.files.maxPartitionBytes before any
# read below (smaller => more, smaller source-file splits => the scan+view-transform fans out across
# more cores). "0" means leave the cluster/engine default untouched, so we skip the set. Guarded: this
# is a performance conf, not a correctness one, and some runtimes (e.g. serverless, which auto-tunes
# parallelism) may reject setting it - so a failure to set it warns and continues on the engine default
# rather than failing the run.
if MAX_PARTITION_BYTES != "0":
    try:
        spark.conf.set("spark.sql.files.maxPartitionBytes", MAX_PARTITION_BYTES)
        print(f"set spark.sql.files.maxPartitionBytes = {MAX_PARTITION_BYTES}")
    except Exception as _e:
        print(f"WARNING: could not set spark.sql.files.maxPartitionBytes={MAX_PARTITION_BYTES} "
              f"({type(_e).__name__}: {_e}); continuing on the engine default")

# COMMAND ----------
# Build the connector write config + the shared filter helper. Both are MODE-INDEPENDENT (batch and
# streaming write through the same EsWriteConfig and apply the same filter), so they are prepared once
# here, above the per-mode cells below.
#
# api_key auth: the secret's value is passed straight to the connector as api_key. dbutils.secrets
# reads it on the DRIVER; EsWriteConfig is a plain frozen dataclass that carries the string into the
# executor closure (the connector builds the ES client per-partition from it). Redaction: the value
# is a Databricks secret, so it is automatically redacted from notebook output if printed.
#
# write_overrides splats in only the tuning knobs that were set this run (chunk_size /
# require_existing_index / verify_certs); an unset knob is absent, leaving the connector's own
# default in force. index and id_field come from the (validated, resolved) config. es_id_field is
# OPTIONAL: an omitted one resolves to None, which is exactly the connector's "no id_field" default
# (id_field: Optional[str] = None) - ES then assigns a random _id per doc, so at-least-once replays
# can duplicate documents. A set es_id_field gives deterministic _ids => idempotent upserts.
es_write_config = EsWriteConfig(
    hosts=ES_HOST_URL,
    api_key=dbutils.secrets.get(SECRET_SCOPE_NAME, SECRET_KEY_NAME),
    index=cfg["es_index_name"],
    id_field=cfg["es_id_field"],  # None when unset == connector default (auto _id)
    **write_overrides,
)


def apply_filter(df):
    """Apply the optional filter_condition to a DataFrame. Shared by both modes so the filter step is
    written once and applied identically to a batch DataFrame or a streaming micro-batch."""
    return df.filter(FILTER_CONDITION) if FILTER_CONDITION else df


# Set by whichever mode cell below runs, and read by the summary/exit cell. Initialized to None so the
# backstop cell can fail closed if NO mode handled the run (e.g. a mode added to the allow-list without
# an export cell here) rather than exiting on a stale/empty summary.
RUN_SUMMARY = None

# COMMAND ----------
# BATCH export. Read the whole (optionally filtered) deployed view and bulk_write it in one shot.
# bulk_write returns the count dict; reconcile_or_raise then FAILS the run if any document was rejected
# (errors > 0) or any row went unaccounted for, so a partial export surfaces as a job failure, not a
# silent success. (raise_on_error=False so the result is printed for the log before we reconcile.)
if PIPELINE_MODE == "batch":
    export_df = apply_filter(spark.table(VIEW_FQN))
    # bulk_write runs one ES bulk stream per DataFrame partition (mapInPandas), so write parallelism ==
    # partition count. Read parallelism (max_partition_bytes, set above) is the primary lever: the scan
    # and this narrow, shuffle-free transform preserve that partition count through to the write, so the
    # write already fans out and WRITE_REPARTITION defaults to 0 (off). Set WRITE_REPARTITION > 0 only to
    # override the write's partition count independently of the read (e.g. a view that shuffles resets it
    # to spark.sql.shuffle.partitions); the target is the same either way, ~2-3x total worker cores.
    # Repartition AFTER the filter so the surviving rows spread evenly.
    if WRITE_REPARTITION > 0:
        export_df = export_df.repartition(WRITE_REPARTITION)
    result = bulk_write(export_df, es_write_config)
    print(f"batch bulk_write result: {result}")
    reconcile_or_raise(result, index=es_write_config.index)
    RUN_SUMMARY = (
        f"written={result['written']} deleted={result['deleted']} errors={result['errors']} "
        f"ignored={result['ignored']} total_input={result['total_input']}"
    )
    print(f"BATCH EXPORT COMPLETE: {RUN_SUMMARY}")

# COMMAND ----------
# STREAMING setup (streaming mode only). Prepare everything the stream needs BEFORE starting it, so a
# problem here surfaces in this cell rather than mid-stream: the checkpoint location, the rendered
# per-micro-batch SELECT, and the foreachBatch writer (with a driver-side totals accumulator).
#
# The design constraint: we must NOT read the deployed VIEW and join it back to each micro-batch (that
# would scan the huge source side of the view every trigger). Instead we take the view's OWN SELECT
# and run it with ${source} bound to a temp view over just the micro-batch, so the identical transform
# the deployed view applies runs against batch-sized data. Reference tables (${ref_*}) stay their real
# FQNs - a reference join is small-batch-to-dimension.
#
# ROW-WISE VIEWS ONLY. Running the view SELECT per micro-batch is correct only for row-wise logic:
# projection, filters, scalar expressions, and 1:1 reference joins - each output row depends on a
# single source row. A view that aggregates ACROSS source rows (GROUP BY, DISTINCT, window/OVER,
# PIVOT, ...) would be computed PER BATCH here, not over the whole stream, so streaming would silently
# emit different results than batch (which scans the full view). This is a limitation of streaming
# mode: do not point a streaming pipeline at an aggregating view; use batch mode for those.
if PIPELINE_MODE == "streaming":
    # checkpoint_base_path was validated non-empty at the validation stage above (streaming only).
    # Per-stream subfolder keyed by config_name (stable + unique + filesystem-safe), so each stream's
    # checkpoint is isolated and survives across runs.
    checkpoint_location = f"{CHECKPOINT_BASE_PATH.rstrip('/')}/{CONFIG_NAME}"

    # The view's SELECT body, with ${source} bound to the per-batch temp view and ${ref_*} left as the
    # real reference tables. Extracted + rendered from the SAME .sql the deployed view uses (shared
    # renderer), so streaming and batch provably apply identical transform logic. Rendered ONCE here
    # (the SQL text is constant across micro-batches); only the temp view's contents change per batch.
    #
    # Opening by view['name'] (the RESOLVED name) matches the on-disk .sql filename because a view NAME
    # cannot contain ${environment}: config.py validates view.name with _require_identifier (a plain
    # identifier, token rejected at load), so resolve_config leaves it byte-for-byte unchanged. Thus
    # resolved == unresolved == filename for the view name, and deploy_views keys files the same way.
    _view_file = os.path.join(FILES_ROOT, "_pipelines", "pipeline_views", f"{view['name']}.sql")
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

    # How-many-rows-did-this-run-push must be recorded DURABLY, not in a Python variable: on serverless
    # the foreachBatch body runs server-side, so a client-side counter never sees the mutation (verified
    # live: rows landed in ES while a client-side dict read 0), and query.recentProgress is delivered
    # asynchronously so reading it right after awaitTermination is racy (verified: it reported 0 for a
    # batch that really moved rows). So each batch appends its count to a METRICS DIRECTORY of small
    # JSON files, written server-side from foreachBatch and read back by the summary cell. It lives
    # UNDER the checkpoint location (a UC Volume path we already require for streaming), so it creates
    # NO catalog object in the customer's namespace. Cleared at the start of THIS run so the directory
    # only ever holds this run's files; retries within the run are deduped by batch_id when summing.
    metrics_dir = f"{checkpoint_location}/_run_metrics"
    dbutils.fs.rm(metrics_dir, recurse=True)

    def foreach_batch(batch_df, batch_id: int):
        # Register the batch as the ${source} temp view and run the rendered view SELECT over it, so
        # the deployed view's projection/joins/hints apply to exactly this batch. Both the register and
        # the query go through batch_df.sparkSession, NOT the notebook's global `spark`: inside
        # foreachBatch the micro-batch can carry a cloned session, and a temp view is session-scoped, so
        # binding both to the batch's own session keeps the view visible to the query in every runtime.
        # filter_condition is applied to the transformed rows.
        session = batch_df.sparkSession
        batch_df.createOrReplaceTempView(BATCH_SOURCE_VIEW)
        transformed = apply_filter(session.sql(RENDERED_SELECT))
        # Optional per-micro-batch repartition, same knob and rationale as the batch path: read
        # parallelism (max_partition_bytes) is the primary lever and its partition count carries
        # through this shuffle-free transform to the write, so WRITE_REPARTITION defaults to 0 (off).
        # Set it > 0 only to override the write's partition count independently (e.g. a view that
        # shuffles), targeting ~2-3x worker cores, the same target as the batch path.
        if WRITE_REPARTITION > 0:
            transformed = transformed.repartition(WRITE_REPARTITION)
        # Write via the connector, capturing its AUTHORITATIVE result (not our own .count() of the
        # input, which would over-report a partially-failed batch). raise_on_error=True makes bulk_write
        # itself raise on any rejected/unaccounted row, so a batch that does not FULLY succeed fails the
        # micro-batch here: the checkpoint does not advance and Spark reprocesses the batch. That retry
        # is an idempotent upsert ONLY when es_id_field is set (deterministic _id); with es_id_field
        # OMITTED, ES assigns fresh random _ids, so the reprocessed rows land as NEW documents and the
        # retry DUPLICATES them - streaming replays are routine, so omit es_id_field only for a stream
        # where duplicates are acceptable. If it never recovers the run fails with no summary. So the
        # record step below is only reached for a batch that wrote every row cleanly, and
        # result['written'] is the true count.
        result = bulk_write(transformed, es_write_config, raise_on_error=True)
        # Persist this clean batch's authoritative written count as one JSON file, keyed by batch_id so
        # the summary can dedup a retried batch (write mode append; each batch is its own small file).
        session.createDataFrame(
            [(int(batch_id), int(result.get("written", 0) or 0))], "batch_id bigint, written bigint"
        ).coalesce(1).write.mode("append").json(metrics_dir)

# COMMAND ----------
# STREAMING run (streaming mode only). Read the RAW source as a Delta stream and drain it once.
# skipChangeCommits=true: tolerate non-append commits (a manual UPDATE/DELETE upstream) by skipping
# them rather than failing the stream; corrections are handled out-of-band via a batch backfill.
#
# streaming_start controls where a FIRST run (no checkpoint yet) begins; once a checkpoint exists it is
# the position of record and startingVersion is ignored (Spark resumes from the checkpoint).
# - "new": start at the source's CURRENT Delta version, so existing history is NOT re-exported and
#   subsequent runs pick up only new commits. We resolve the concrete current version NUMBER rather
#   than startingVersion="latest" because of a Trigger.availableNow interaction proven live: a "latest"
#   first run finds no new rows, runs zero micro-batches, and persists NO checkpoint offset, so it never
#   establishes a resume point and later runs keep skipping new data. A numeric startingVersion seeds a
#   real, persisted offset even on a zero-row first batch, so the next run resumes correctly.
#   startingVersion is INCLUSIVE and must be an EXISTING version, so we use the current version
#   (current+1 is rejected when it does not exist yet). One consequence: if the current commit is an
#   append, the first "new" run re-exports that single commit's rows. That is a harmless idempotent
#   upsert (bounded to one commit) ONLY when es_id_field is set; with es_id_field OMITTED those rows get
#   fresh random _ids and are re-inserted as NEW documents (duplicates).
# - "full": omit startingVersion, so the first micro-batches backfill the whole existing table.
if PIPELINE_MODE == "streaming":
    reader = spark.readStream.option("skipChangeCommits", "true")
    if STREAMING_START == "new":
        current_version = spark.sql(f"DESCRIBE HISTORY {SOURCE_FQN}").agg({"version": "max"}).collect()[0][0]
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

    # Report how many rows this run pushed, read back from the per-batch JSON metrics foreachBatch
    # wrote under metrics_dir (see above). This is the reliable driver-side total: it survives the
    # server-side foreachBatch boundary and the async delivery of query.recentProgress, both of which
    # under-reported in testing. DEDUP by batch_id first (max written per batch_id), so a batch that was
    # retried within this run is counted once, not summed twice - then total. A run with no new source
    # data wrote no metric files (empty dir), which reads as 0 batches / 0 rows: a valid outcome, not a
    # failure. Each recorded batch used bulk_write(raise_on_error=True), so any batch that did not fully
    # succeed failed the run instead of recording, and a TERMINATED-SUCCESS total is exact.
    from pyspark.sql import functions as _F  # noqa: E402

    def _metrics_dir_missing():
        # Existence probe that FAILS CLOSED: return True (treat as "no metric files, 0 batches ran")
        # ONLY when dbutils.fs.ls positively reports the path does not exist. dbutils wraps that as an
        # error whose text contains FileNotFoundException; any OTHER error (permission/403, transient
        # IO, etc.) is re-raised so it fails the run rather than being misread as "0 rows" - masking a
        # run that already pushed rows is exactly the fail-open bug this must avoid.
        try:
            dbutils.fs.ls(metrics_dir)
            return False  # path exists
        except Exception as _e:
            # Verified on this runtime: a missing path raises ExecutionError wrapping
            # CloudFileNotFoundException with text "No such file or directory". Match the not-found
            # signal explicitly; re-raise everything else.
            _msg = str(_e)
            if "FileNotFoundException" in _msg or "No such file or directory" in _msg or "does not exist" in _msg:
                return True  # positively not-found: no batches wrote metrics
            raise  # anything else is a real failure - do not swallow it

    if _metrics_dir_missing():
        # No metric files => the stream drained zero micro-batches (no new source data since the last
        # run). A valid outcome, reported as 0, not a failure.
        num_batches, rows_pushed = 0, 0
    else:
        # Dir exists: read it WITHOUT catching, so any genuine read failure propagates and fails the run
        # rather than being silently reported as 0.
        _per_batch = spark.read.json(metrics_dir).groupBy("batch_id").agg(_F.max("written").alias("written"))
        _agg = _per_batch.agg(_F.count("*").alias("batches"), _F.coalesce(_F.sum("written"), _F.lit(0)).alias("rows")).collect()[0]
        num_batches, rows_pushed = int(_agg["batches"]), int(_agg["rows"])
    RUN_SUMMARY = (
        f"streaming_start={STREAMING_START} batches={num_batches} rows_pushed={rows_pushed} "
        f"checkpoint={checkpoint_location}"
    )
    if rows_pushed == 0:
        print("STREAMING EXPORT COMPLETE: 0 rows pushed (no new source data since the last run)")
    print(f"STREAMING EXPORT COMPLETE: {RUN_SUMMARY}")

# COMMAND ----------
# Fail-closed backstop: every supported mode's cell above sets RUN_SUMMARY. If it is still None, the
# effective PIPELINE_MODE passed allow-list validation but no export cell handled it (e.g. a new mode
# added to the allow-list without a corresponding cell). Raise rather than exit on an empty summary.
if RUN_SUMMARY is None:
    raise ValueError(f"no export ran for pipeline_mode {PIPELINE_MODE!r} (allow-listed but unhandled)")

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output, visually replacing any print() output from the same cell. Keeping it separate leaves
# the prints above visible in their own completed cells.
dbutils.notebook.exit(
    f"config_name={CONFIG_NAME}; es_index_name={cfg['es_index_name']}; pipeline_mode={PIPELINE_MODE}; "
    f"view={VIEW_FQN}; {RUN_SUMMARY}"
)

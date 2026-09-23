# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: per-index pipeline runner
# MAGIC
# MAGIC The shared notebook run by every per-index job. It installs the connector wheel (verifying the
# MAGIC import), loads `_pipelines/pipeline_configs/<config_name>.yml`, resolves `${environment}` into the object
# MAGIC names, and exports the config's view to Elasticsearch via the connector's `bulk_write`.
# MAGIC
# MAGIC Both modes are implemented. `pipeline_mode=batch` reads the whole deployed view, optionally
# MAGIC filters, and bulk-writes it. `pipeline_mode=streaming` reads the RAW source table as a stream,
# MAGIC renders the view's OWN SELECT over each micro-batch (so the view logic runs against batch-sized
# MAGIC data, never a join back to the full view), and bulk-writes per batch through the connector's
# MAGIC foreachBatch writer with a checkpoint. The streaming trigger is set by `streaming_trigger_interval`
# MAGIC (below): empty => `Trigger.availableNow` (drain the backlog and STOP, for scheduled/on-demand
# MAGIC runs); non-empty => `Trigger.ProcessingTime(interval)`, an ALWAYS-ON stream that never terminates
# MAGIC (for a `continuous` pipeline on classic compute, wrapped by a Databricks Jobs continuous trigger).
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
# MAGIC - `streaming_trigger_interval`: the continuous ProcessingTime cadence (e.g. `30 seconds`) from the
# MAGIC   config's `continuous` block, or empty for availableNow (drain-and-stop). Deploy-time, not per-run.
# MAGIC
# MAGIC Run-time parameters (job parameters; overridable per run with `--params <name>=<value>`):
# MAGIC - `pipeline_mode`: `batch` | `streaming` (default from config). Clearing a stale streaming
# MAGIC   checkpoint is handled by the dedicated `_checkpoint clear` job, not a pipeline_mode.
# MAGIC - `filter_condition`: optional Spark SQL predicate applied before the write (default from config).
# MAGIC - `chunk_size`, `write_concurrency`, `request_timeout`, `transport_max_retries`, `require_existing_index`,
# MAGIC   `verify_certs`: EsWriteConfig tuning (default from config; omitted there and unset per run => connector
# MAGIC   default). `request_timeout` (seconds) and `transport_max_retries` (0 disables) tune a write that
# MAGIC   times out mid-send.
# MAGIC - `streaming_start`: `new` (default; only new commits) | `full` (backfill the whole table);
# MAGIC   streaming only, honored on the first run before a checkpoint exists. `new` establishes the
# MAGIC   checkpoint at the current source position via a no-op availableNow seed (drains the initial
# MAGIC   snapshot without exporting to ES), so history is skipped without stalling on a large source.
# MAGIC - `max_files_per_trigger`, `max_bytes_per_trigger`: streaming read rate-limits that bound each
# MAGIC   micro-batch (default from config; empty => Spark defaults). Streaming only; useful for a backfill.

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
dbutils.widgets.text("ca_certs", "", "UC Volume path to a CA bundle (PEM) verifying the ES TLS cert (empty => system CAs)")
dbutils.widgets.text("pipeline_mode", "", "Export mode: batch | streaming (job parameter; overridable per run)")
dbutils.widgets.text("filter_condition", "", "Optional row filter, a Spark SQL predicate (overridable per run)")
dbutils.widgets.text("chunk_size", "", "EsWriteConfig chunk_size override (empty => connector default)")
dbutils.widgets.text("write_concurrency", "", "EsWriteConfig write_concurrency: parallel bulk streams per partition (empty => connector default 1)")
dbutils.widgets.text("request_timeout", "", "EsWriteConfig request_timeout: per-request ES client timeout in seconds (empty => connector default 60)")
dbutils.widgets.text("transport_max_retries", "", "EsWriteConfig transport_max_retries: whole-request retries on a transport failure; 0 disables (empty => connector default 3)")
dbutils.widgets.text("require_existing_index", "", "EsWriteConfig require_existing_index: true|false (empty => default)")
dbutils.widgets.text("verify_certs", "", "EsWriteConfig verify_certs: true|false (empty => default)")
dbutils.widgets.text("bulk_stats", "", "EsWriteConfig bulk_stats: true|false; per-partition ES bulk-send diagnostics in the run log (default from ${var.bulk_stats}/config; empty => connector default off; needs connector 0.9.3+)")
dbutils.widgets.text("retry_transport_timeout", "", "EsWriteConfig retry_transport_timeout: true|false; connector OWNS whole-request timeout retry (re-send with backoff instead of failing the batch) (default from ${var.retry_transport_timeout}/config; empty => connector default off; needs connector 0.9.7+)")
dbutils.widgets.text("op_type", "", "EsWriteConfig op_type: index (default) | create; 'create' is append-only (a resend of an already-indexed doc is a 409 no-op, deduped not overwritten) (default from config; empty => connector default 'index'; needs connector 0.10.0+)")
dbutils.widgets.text("bypass_fast_path", "", "EsWriteConfig bypass_fast_path: true|false; skip the errors-probe fast path and classify every chunk per-item (exact docs_deduped/written counts for op_type=create, at the cost of fast-path throughput) (default from ${var.bypass_fast_path}/config; empty => connector default off; needs connector 0.10.0+)")
dbutils.widgets.text("write_repartition", "", "Repartition the write input to N partitions before bulk_write (0 disables; empty => default)")
dbutils.widgets.text("max_partition_bytes", "", "spark.sql.files.maxPartitionBytes for the source read, e.g. 32m (0 leaves it unset; empty => default)")
# Streaming-only widgets. checkpoint_base_path is a deploy-time base_parameter (bundle variable);
# streaming_start is a run-time job parameter (default "new"). Both are ignored by a batch run.
dbutils.widgets.text("checkpoint_base_path", "", "UC Volume base for streaming checkpoints (runner appends /<config_name>)")
dbutils.widgets.text("streaming_start", "", "Streaming start: new (only new commits) | full (backfill whole table)")
# streaming_trigger_interval is a DEPLOY-TIME base_parameter (from the config's continuous block), not a
# per-run job parameter: empty => Trigger.availableNow (drain-and-stop); non-empty => an always-on
# ProcessingTime stream at that cadence. max_files/max_bytes_per_trigger are per-run streaming read
# rate-limits (empty => Spark defaults).
dbutils.widgets.text("streaming_trigger_interval", "", "Continuous ProcessingTime cadence, e.g. '30 seconds' (empty => availableNow drain-and-stop)")
dbutils.widgets.text("max_files_per_trigger", "", "Streaming: max Delta files per micro-batch (empty => Spark default 1000)")
dbutils.widgets.text("max_bytes_per_trigger", "", "Streaming: max bytes per micro-batch, e.g. 128m (empty => no cap)")
CONFIG_NAME = dbutils.widgets.get("config_name").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip()
ES_HOST_URL = dbutils.widgets.get("es_host_url").strip()
SECRET_SCOPE_NAME = dbutils.widgets.get("secret_scope_name").strip()
SECRET_KEY_NAME = dbutils.widgets.get("secret_key_name").strip()
CA_CERTS = dbutils.widgets.get("ca_certs").strip()
PIPELINE_MODE = dbutils.widgets.get("pipeline_mode").strip()
FILTER_CONDITION = dbutils.widgets.get("filter_condition").strip()
CHUNK_SIZE = dbutils.widgets.get("chunk_size").strip()
WRITE_CONCURRENCY = dbutils.widgets.get("write_concurrency").strip()
REQUEST_TIMEOUT = dbutils.widgets.get("request_timeout").strip()
TRANSPORT_MAX_RETRIES = dbutils.widgets.get("transport_max_retries").strip()
REQUIRE_EXISTING_INDEX = dbutils.widgets.get("require_existing_index").strip()
VERIFY_CERTS = dbutils.widgets.get("verify_certs").strip()
BULK_STATS = dbutils.widgets.get("bulk_stats").strip()
RETRY_TRANSPORT_TIMEOUT = dbutils.widgets.get("retry_transport_timeout").strip()
OP_TYPE = dbutils.widgets.get("op_type").strip()
BYPASS_FAST_PATH = dbutils.widgets.get("bypass_fast_path").strip()
WRITE_REPARTITION = dbutils.widgets.get("write_repartition").strip()
MAX_PARTITION_BYTES = dbutils.widgets.get("max_partition_bytes").strip()
CHECKPOINT_BASE_PATH = dbutils.widgets.get("checkpoint_base_path").strip()
STREAMING_START = dbutils.widgets.get("streaming_start").strip()
STREAMING_TRIGGER_INTERVAL = dbutils.widgets.get("streaming_trigger_interval").strip()
MAX_FILES_PER_TRIGGER = dbutils.widgets.get("max_files_per_trigger").strip()
MAX_BYTES_PER_TRIGGER = dbutils.widgets.get("max_bytes_per_trigger").strip()
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
    require_max_bytes_per_trigger,
    require_max_files_per_trigger,
    require_max_partition_bytes,
    require_pipeline_mode,
    require_streaming_start,
    require_trigger_interval,
    require_write_repartition,
    resolve_config,
    view_select_body,
    view_substitutions,
    write_config_overrides,
)
# Ephemeral per-batch streaming observability (pure Python, no Spark): the STREAM_PROGRESS log-line
# formatter. Kept in pipeline_lib so it is unit-tested off-cluster.
import json  # noqa: E402
import time  # noqa: E402
import uuid  # noqa: E402
from pipeline_lib.observability import (  # noqa: E402
    BULK_STATS_TAG,
    PROGRESS_TAG,
    bulk_stats_relay_line,
    format_bulk_stats,
    format_progress,
    format_tail_summary,
)
# Streaming checkpoint offsets-state classifier (pure Python, dependency-injected ls; unit-tested
# off-cluster). Decides seed-vs-resume for streaming_start=new; fail-closed so an existing checkpoint is
# never misread as a first run (which would drain over an un-exported backlog).
from pipeline_lib.checkpoint import EMPTY, HAS_OFFSET, checkpoint_offsets_state  # noqa: E402

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
# A continuous (always-on) job carries a Databricks Jobs continuous trigger and hands the notebook a
# non-empty streaming_trigger_interval (a deploy-time base_parameter). pipeline_mode stays run-time
# overridable, so guard the one override that would misbehave: a batch (or any non-streaming) run under
# a continuous trigger is a TERMINATING export, which the continuous trigger then auto-restarts - an
# endless loop of full re-exports to ES. Fail closed so the mismatch surfaces as one clear run failure.
# (validate_config already forbids continuous + non-streaming at DEPLOY; this closes the RUN-TIME
# override gap that a deploy-time config check cannot see.)
if STREAMING_TRIGGER_INTERVAL and PIPELINE_MODE != "streaming":
    raise ValueError(
        f"continuous job (streaming_trigger_interval={STREAMING_TRIGGER_INTERVAL!r}) requires "
        f"pipeline_mode=streaming, got {PIPELINE_MODE!r}: a terminating {PIPELINE_MODE} run under a "
        f"continuous trigger would auto-restart in an endless loop. Remove the pipeline_mode override, "
        f"or run this config's batch export as a separate, non-continuous job."
    )
# Re-validate the continuous ProcessingTime cadence against the SAME grammar the config schema uses,
# BEFORE it reaches Trigger.ProcessingTime. streaming_trigger_interval is a deploy-time base_parameter
# baked by the generator, so a stale-generated (from before the grammar was tightened) or hand-edited
# value would otherwise slip through to .start() and, under the Jobs continuous trigger, loop instead of
# failing once. Empty => availableNow, nothing to validate.
if STREAMING_TRIGGER_INTERVAL:
    STREAMING_TRIGGER_INTERVAL = require_trigger_interval(
        STREAMING_TRIGGER_INTERVAL, "streaming_trigger_interval base parameter"
    )
FILTER_CONDITION = require_filter_condition(FILTER_CONDITION, "filter_condition job parameter")
write_overrides = write_config_overrides(CHUNK_SIZE, REQUIRE_EXISTING_INDEX, VERIFY_CERTS, WRITE_CONCURRENCY, BULK_STATS,
                                         request_timeout=REQUEST_TIMEOUT, transport_max_retries=TRANSPORT_MAX_RETRIES,
                                         retry_transport_timeout=RETRY_TRANSPORT_TIMEOUT, op_type=OP_TYPE,
                                         bypass_fast_path=BYPASS_FAST_PATH)
# Four knobs were added to EsWriteConfig in specific connector releases: bulk_stats (0.9.3+),
# retry_transport_timeout (0.9.7+), and op_type + bypass_fast_path (both 0.10.0+). Each is only present in write_overrides when
# its effective value (the global ${var.*} default where one exists, a per-pipeline config value, or a
# --params override) resolves to a non-empty setting. On an OLDER wheel, a present-but-unknown kwarg would
# make EsWriteConfig(**write_overrides) raise TypeError and fail EVERY such run. All are OPTIONAL
# enhancements (diagnostics; a reliability retry; an append-only bulk action), so they must never break
# the export: if the installed EsWriteConfig lacks a field, drop it from the overrides and warn rather
# than failing. Detected against the live dataclass's own field set (the installed wheel is the source of
# truth), so this is correct whatever version is deployed. Dropping op_type means the feed runs as the
# connector's default op_type ("index"): benign for append-only data (a resend re-writes identical
# content instead of deduping), but the warning makes the downgrade visible. The other tuning knobs have
# existed since well before this framework, so only these version-gated ones are guarded.
_VERSION_GATED_KNOBS = {"bulk_stats": "0.9.3+", "retry_transport_timeout": "0.9.7+", "op_type": "0.10.0+", "bypass_fast_path": "0.10.0+"}
if any(k in write_overrides for k in _VERSION_GATED_KNOBS):
    import dataclasses  # noqa: E402
    _es_fields = {f.name for f in dataclasses.fields(EsWriteConfig)}
    for _knob, _min_ver in _VERSION_GATED_KNOBS.items():
        if _knob in write_overrides and _knob not in _es_fields:
            _dropped = write_overrides.pop(_knob)
            print(f"WARNING: installed connector (databricks_es_connector {_connector_version}) has no "
                  f"EsWriteConfig.{_knob} field; dropping {_knob}={_dropped} (requires {_min_ver}). "
                  f"The export proceeds WITHOUT it.")
STREAMING_START = require_streaming_start(STREAMING_START or "new", "streaming_start job parameter")
WRITE_REPARTITION = int(require_write_repartition(WRITE_REPARTITION, "write_repartition job parameter"))
# - max_partition_bytes: Spark byte-size (or "0" = leave unset). Validated unconditionally; applied to
#   the source read below (both modes). Empty widget -> the built-in default via the validator.
MAX_PARTITION_BYTES = require_max_partition_bytes(MAX_PARTITION_BYTES, "max_partition_bytes job parameter")
# - max_files_per_trigger / max_bytes_per_trigger: streaming read rate-limits, validated unconditionally
#   (a bad --params value fails closed regardless of mode) but only APPLIED by the streaming reader
#   below. Empty widget -> "" (leave Spark's default), so an unset knob is simply omitted from the read.
MAX_FILES_PER_TRIGGER = require_max_files_per_trigger(MAX_FILES_PER_TRIGGER, "max_files_per_trigger job parameter")
MAX_BYTES_PER_TRIGGER = require_max_bytes_per_trigger(MAX_BYTES_PER_TRIGGER, "max_bytes_per_trigger job parameter")

# The ES connection settings are required for any run that WRITES to ES: fail closed on an empty one
# rather than constructing a broken EsWriteConfig. These come from this pipeline's es_host_config (a
# complex bundle variable in databricks.yml, resolved per target); an empty value means that host
# config's fields were never filled in for the target being deployed - the common cause on a fresh
# checkout. Both export modes (batch, streaming) write to ES, so this always applies.
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

# checkpoint_base_path is required for a STREAMING run (batch and deploy_views never touch a checkpoint,
# so their runs leave it empty). Validated here so a run that needs a checkpoint location but has none
# fails closed immediately rather than after the config load.
if PIPELINE_MODE == "streaming" and not CHECKPOINT_BASE_PATH:
    raise ValueError(
        f"missing required parameter: checkpoint_base_path (set the bundle variable at deploy); "
        f"a {PIPELINE_MODE} run needs a UC Volume checkpoint location"
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
print(f"es_id_field        = {cfg['es_id_field'] or '<unset> (ES auto-generates _id)'}")
print(f"pipeline_mode      = {PIPELINE_MODE}")
print(f"filter_condition   = {FILTER_CONDITION!r}")
print(f"write_overrides    = {write_overrides}")
print(f"write_repartition  = {WRITE_REPARTITION}" + (" (disabled: natural partitioning)" if WRITE_REPARTITION == 0 else ""))
print(f"max_partition_bytes= {MAX_PARTITION_BYTES}" + (" (leave engine default)" if MAX_PARTITION_BYTES == "0" else ""))
print(f"view               = {VIEW_FQN}")
print(f"source             = {SOURCE_FQN}")
print(f"es_host_url        = {ES_HOST_URL}")
print(f"ca_certs           = {CA_CERTS or '<unset> (system CA store)'}")
if PIPELINE_MODE == "streaming":
    print(f"streaming_start    = {STREAMING_START}")
    print("streaming_trigger  = " + (
        f"continuous / ProcessingTime {STREAMING_TRIGGER_INTERVAL!r} (always-on)"
        if STREAMING_TRIGGER_INTERVAL else "availableNow (drain-and-stop)"))
    print(f"max_files_per_trigger = {MAX_FILES_PER_TRIGGER or '<unset> (Spark default)'}")
    print(f"max_bytes_per_trigger = {MAX_BYTES_PER_TRIGGER or '<unset> (no cap)'}")
# Loud warning for omitting es_id_field. BOTH modes are at-least-once, so a replay re-writes the same
# source rows with fresh auto-generated _ids and the index accumulates DUPLICATES: for batch, a
# failed-then-retried run re-exports the whole view; for streaming, micro-batch retries, stream
# restarts, and the "new"-mode last-commit re-export below are ROUTINE, so duplication is far more
# likely there. This is ALLOWED (duplicates may be fine for an append-only sink), so it is a warning,
# not a failure - but it must be loud, because idempotency is the safe default.
if cfg["es_id_field"] is None:
    _stream_note = (" Streaming replays (micro-batch retries, restarts) are routine, so this is "
                    "especially likely." if PIPELINE_MODE == "streaming" else "")
    print(f"WARNING: {PIPELINE_MODE} pipeline with no es_id_field - ES auto-generates _ids, so a replay "
          f"(a retry or restart) re-inserts rows as NEW documents and accumulates DUPLICATES.{_stream_note} "
          f"Set es_id_field for idempotent upserts; leave it unset only if duplicates are acceptable.")
    # op_type=create escalates the above: create needs a DETERMINISTIC _id to dedup a resend (the
    # connector's create-409-is-a-no-op). With no es_id_field, ES assigns a random _id per doc, so a
    # create can NEVER 409-conflict and the append-only dedup the mode promises silently never fires - a
    # replay just accumulates duplicates, exactly as op_type=index would. That is a footgun (the intended
    # effect is lost), not a data-integrity failure, so WARN rather than fail; the run still writes. The
    # config-load guard (validate_config) already fails a config that STATICALLY sets op_type: create with
    # no es_id_field; this covers the path that guard cannot see - op_type=create arriving via the
    # ${var.op_type} global default or a --params override, known only here at run time. Keyed on the
    # EFFECTIVE op_type in write_overrides (post version-gate), so on an older wheel where op_type was
    # dropped fail-soft there is no create in effect and nothing to warn about.
    if write_overrides.get("op_type") == "create":
        print("WARNING: op_type=create with no es_id_field - creates get ES-assigned random _ids, so they "
              "never 409-conflict and the append-only dedup op_type=create promises NEVER fires; the run "
              "behaves like op_type=index (replays DUPLICATE). Set es_id_field so create dedups resends, "
              "or drop op_type=create.")

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
#
# ca_certs is the global CA-bundle variable (a UC Volume PEM path): pass it straight through as
# an EsConnection field. Empty widget => None => the connector omits it (client_kwargs only injects
# ca_certs when non-None) and falls back to the system CA store. The connector loads it as a local file
# on the driver and every executor. verify_certs=false together with a set ca_certs is a contradiction
# the connector rejects in EsWriteConfig.__post_init__ (raised on the driver here), so we don't
# re-check it pipeline-side - the connector is the single source of truth for that rule.
es_write_config = EsWriteConfig(
    hosts=ES_HOST_URL,
    api_key=dbutils.secrets.get(SECRET_SCOPE_NAME, SECRET_KEY_NAME),
    index=cfg["es_index_name"],
    id_field=cfg["es_id_field"],  # None when unset == connector default (auto _id)
    ca_certs=CA_CERTS or None,    # "" => None => connector falls back to system CAs
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
    # Driver wall clock around the write, to LOCATE a tail that persists after the Spark UI shows every
    # write task complete. bulk_write's own collect_ms (under bulk_stats) is the time INSIDE Spark's
    # collect (the write job PLUS Spark's result finalization), so if this driver-measured wall is
    # ~collect_ms the tail is inside the write itself - typically a straggler partition, which the
    # BULK_STATS tail line below then names - whereas wall well above collect_ms would be work between
    # the collect and this return. Two time.time() calls on the driver; nothing touches the write path.
    _bw_t0 = time.time()
    result = bulk_write(export_df, es_write_config)
    _bw_wall_ms = (time.time() - _bw_t0) * 1000.0
    # Print the core count dict on one line; when bulk_stats is on, result also carries a per-partition
    # 'bulk_stats' list, which we render SEPARATELY (below) as readable BULK_STATS lines rather than
    # dumping the raw list into the result line. reconcile_or_raise reads only the counts, so the extra
    # key is ignored there.
    _core_result = {k: v for k, v in result.items() if k != "bulk_stats"}
    print(f"batch bulk_write result: {_core_result}")
    print(f"{BULK_STATS_TAG} driver: bulk_write_wall_ms={_bw_wall_ms:.1f}")
    if "bulk_stats" in result:
        # The tail/straggler summary FIRST (the one-line answer to "where did the wall time go"), then
        # the full per-partition breakdown. Both fail-soft.
        print(format_tail_summary(result))
        print(format_bulk_stats(result["bulk_stats"]))
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

    # Per-batch bulk-stats RELAY directory, a SIBLING of _run_metrics (kept out of it so the summary's
    # spark.read.json(metrics_dir) never sees these files). Under Spark Connect foreachBatch runs
    # server-side and its BULK_STATS print reaches only the driver log, while the StreamingQueryListener
    # runs client-side and its print reaches the notebook cell (that is why STREAM_PROGRESS shows there).
    # An in-memory handoff can't cross that process split, so foreachBatch writes each batch's
    # overall+tail line to `{relay_dir}/{batch_id}` (via the SAME Spark write it uses for the row count -
    # the one file mechanism proven to work server-side here), and onQueryProgress reads it for the batch
    # it is reporting and prints it into the cell. Cleared at run start so it only holds this run.
    relay_dir = f"{checkpoint_location}/_bulk_stats_relay"
    dbutils.fs.rm(relay_dir, recurse=True)

    # The relay READ below is a driver-side os/open read, which needs relay_dir on a FUSE-mounted path
    # (/Volumes or /dbfs). Streaming checkpoints are UC Volumes in practice, but if a deployment points
    # checkpoint_base_path at a dbfs:/ or cloud URI the read would fail-soft to None and the per-batch
    # relay-to-cell would vanish with no signal. So when diagnostics are on AND the path is not
    # FUSE-readable, warn ONCE here (the BULK_STATS oneline still reaches the driver log from
    # foreachBatch). Non-fatal: diagnostics never affect the export.
    _relay_readable = relay_dir.startswith("/Volumes/") or relay_dir.startswith("/dbfs/")
    if BULK_STATS.strip().lower() == "true" and not _relay_readable:
        print(f"WARNING: {BULK_STATS_TAG} per-batch relay-to-cell disabled: checkpoint path "
              f"{checkpoint_location!r} is not a FUSE-mounted /Volumes or /dbfs path; the per-batch "
              f"BULK_STATS/tail line still appears in the driver log.")

    def read_relay_line(batch_id):
        """Client-side read of the per-batch relay file foreachBatch wrote to `{relay_dir}/{batch_id}`
        (a Spark `.text()` output directory containing one `part-*.txt`). Called from the progress
        listener, which runs on the DRIVER where the UC Volume is FUSE-mounted, so a plain os/open read
        works with no Spark job on the listener thread. After reading, PRUNE this and any earlier batch's
        relay directory (the listener only ever reads the batch it is reporting, so anything <= batch_id
        is spent) - otherwise an always-on stream would accumulate one directory per batch. FAIL-SOFT: a
        missing directory (empty batch, or not yet written) or any read/prune error yields None / is
        ignored, so nothing extra is printed and the export is never affected."""
        content = None
        try:
            d = f"{relay_dir}/{int(batch_id)}"
            for name in sorted(os.listdir(d)):
                if name.startswith("part-"):
                    with open(os.path.join(d, name)) as fh:
                        content = fh.read().rstrip("\n")
                    break
        except Exception:
            content = None
        # Prune spent relay dirs (<= this batch). Best-effort; bounds growth even if a batch's progress
        # event was missed (its dir is reclaimed when a later batch is read).
        try:
            _n = int(batch_id)
            for name in os.listdir(relay_dir):
                if name.isdigit() and int(name) <= _n:
                    dbutils.fs.rm(f"{relay_dir}/{name}", recurse=True)
        except Exception:
            pass
        return content

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
        # When bulk_stats is on, log a COMPACT one-line rollup of this micro-batch's ES bulk-send
        # diagnostics (overall docs/send, rtt, took) so an always-on run has per-batch visibility
        # without the full per-partition breakdown flooding the log. format_bulk_stats is fail-soft,
        # so a diagnostic-formatting fault can never disturb the write. This print runs server-side
        # (micro-batch), so its stdout lands in the driver LOG (surfacing as stderr), not the cell.
        if "bulk_stats" in result:
            print(format_bulk_stats(result["bulk_stats"], oneline=True) + f" batch_id={batch_id}")
            # ALSO relay the overall+tail line to the cell, via a small per-batch FILE the
            # client-side listener reads (foreachBatch's own stdout can't reach the cell under Spark
            # Connect - see relay_dir above). Written with the SAME nested Spark write used for the
            # row count below, so it uses only a mechanism proven to work server-side here. Gated on
            # _relay_readable: when the checkpoint path isn't FUSE-readable the listener can neither
            # read NOR prune these files, so skip the write entirely (no leak; the operator was warned
            # once at run start, and the oneline above still reaches the driver log). FAIL-SOFT: a
            # relay/format fault must never disturb the write. Only data-carrying batches produce a
            # line (bulk_stats_relay_line returns None otherwise), so empty batches write nothing.
            try:
                _relay_line = bulk_stats_relay_line(result, batch_id) if _relay_readable else None
                if _relay_line is not None:
                    session.createDataFrame([(_relay_line,)], "line string") \
                        .coalesce(1).write.mode("overwrite").text(f"{relay_dir}/{int(batch_id)}")
            except Exception as _e:
                print(f"WARNING: {BULK_STATS_TAG} could not relay batch {batch_id} to the cell "
                      f"({type(_e).__name__}: {_e}); continuing")
        # Persist this clean batch's authoritative written count as one JSON file, keyed by batch_id so
        # the summary can dedup a retried batch (write mode append; each batch is its own small file).
        session.createDataFrame(
            [(int(batch_id), int(result.get("written", 0) or 0))], "batch_id bigint, written bigint"
        ).coalesce(1).write.mode("append").json(metrics_dir)

# COMMAND ----------
# STREAMING run - STEP 1 of 2: PREPARE the checkpoint (streaming mode only). Build the Delta stream
# reader and, on a first run, position the checkpoint; the NEXT cell runs the stream from it. Nothing
# here writes to ES. skipChangeCommits=true: tolerate non-append commits (a manual UPDATE/DELETE
# upstream) by skipping them rather than failing the stream; corrections are handled out-of-band via a
# batch backfill.
#
# streaming_start controls where a FIRST run (no checkpoint yet) begins; once a checkpoint exists it is
# the position of record (Spark resumes from the checkpoint), so we POSITIVELY classify the checkpoint's
# offsets and skip all first-run seeding on a resume (see checkpoint_offsets_state in pipeline_lib).
# - "new": establish the checkpoint at the source's CURRENT position WITHOUT exporting existing data,
#   then subsequent runs pick up only new commits. On a genuine first run we do this with a dedicated
#   NO-OP SEED: a Trigger.availableNow stream (startingVersion = current version, a bounded
#   maxFilesPerTrigger, skipChangeCommits matching the main reader) whose foreachBatch does NOTHING,
#   writing the REAL checkpoint. It drains the source's initial snapshot as empty-effect micro-batches -
#   committing offsets but sending nothing to ES - then stops, and the MAIN stream below resumes from
#   that checkpoint incrementally. Why a separate no-op drain rather than seeding startingVersion on the
#   main reader: on a large-history source, first-run positioning enumerates a task per active file and
#   can run for many hours before the main stream makes progress; doing it as a no-op (no transform, no
#   ES write) bounded by maxFilesPerTrigger lets it complete and commit a resume point cheaply. We use a
#   numeric startingVersion (not "latest") to pin a deterministic boundary, and after the seed we VERIFY
#   an offset was actually committed: if it was not (nothing to drain at that version, so availableNow
#   ran zero batches), we pin startingVersion on the main reader instead, so existing history is never
#   re-exported. If the checkpoint state cannot be positively classified (a listing error, neither
#   clearly resume nor clearly first-run), we likewise fall back to seeding startingVersion on the main
#   reader instead of the no-op drain - safe on both a resume and a true first run, and it never skips a
#   backlog.
# - "full": omit startingVersion and use the REAL foreachBatch, so the first micro-batches backfill the
#   whole existing table to ES (intentional history export).
if PIPELINE_MODE == "streaming":
    # The seed drain's batch bound (used by the "new" first-run path below). maxFilesPerTrigger MUST be
    # set on the no-op seed so the source's initial snapshot is consumed as bounded, resumable
    # micro-batches instead of one unbounded batch that can stall on a large-history source. Reuse the
    # operator's max_files_per_trigger when set; otherwise this default. TUNABLE: since the seed's batches
    # are no-ops (no read/transform/write), a larger value means fewer passes to drain; the right value is
    # confirmed against the real source during rollout.
    _SEED_MAX_FILES_PER_TRIGGER_DEFAULT = "10000"

    reader = spark.readStream.option("skipChangeCommits", "true")
    # Optional read rate-limits: bound how much each micro-batch pulls from the source. Applied to BOTH
    # triggers (availableNow splits the backlog into multiple batches of this size; ProcessingTime caps
    # each interval's batch), but most valuable for throttling a first-run backfill (streaming_start=full)
    # or a large post-restart catch-up so one micro-batch does not read the whole table. Empty => omitted,
    # so Spark's own defaults stand (maxFilesPerTrigger 1000, no byte cap). Validated above.
    if MAX_FILES_PER_TRIGGER:
        reader = reader.option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    if MAX_BYTES_PER_TRIGGER:
        reader = reader.option("maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)
    if STREAMING_START == "new":
        # Choose the first-run seeding strategy from the checkpoint's POSITIVELY classified state, so a
        # misread never silently drops data (see checkpoint_offsets_state).
        _cp_state = checkpoint_offsets_state(checkpoint_location, dbutils.fs.ls)
        if _cp_state == HAS_OFFSET:
            # A prior run persisted an offset: Spark resumes from the checkpoint (startingVersion is
            # ignored), so no seed is needed. A committed offset ALWAYS means "resume" - we never re-run
            # the no-op drain when offsets exist. Consequences:
            #   - A pipeline whose checkpoint predates this seed logic resumes normally: it is never
            #     no-op-drained, so no un-sent backlog is skipped (re-draining an existing checkpoint is
            #     exactly what would lose data, which is why has_offset never triggers a drain).
            #   - If a previous run's no-op seed FAILED partway, it left a partial offset; this run's main
            #     stream resumes from there and SENDS the un-drained tail of the initial snapshot to ES
            #     (slower, and partial history reaches ES). That is the SAFE direction - it over-sends,
            #     never drops - and is self-limiting to the tail; a clean seed avoids it entirely.
            print("streaming_start=new: resuming from existing checkpoint (no seed needed)")
        elif _cp_state == EMPTY:
            # GENUINE first run (offsets dir positively absent): establish the checkpoint at the source's
            # current position WITHOUT exporting existing data, via a no-op Trigger.availableNow seed
            # drain, then let the main stream below resume from it incrementally. On a large-history
            # source, seeding startingVersion directly on the main reader enumerates a task per active
            # file and can take many hours before the main stream progresses; running that as a no-op (no
            # transform, no ES write), bounded by maxFilesPerTrigger, lets it complete and commit a resume
            # point cheaply.
            #
            # Resolve the current Delta version to pin the seed's startingVersion via
            # `DESCRIBE HISTORY <table> LIMIT 1` (commits are newest-first, so the LIMIT 1 row's `version`
            # is the current snapshot version). We do NOT use DeltaTable.forName(...).history(1): that
            # Python API errors on some managed source types (Lakeflow/SDP streaming tables and
            # materialized views) the client hit in production, while the SQL command works across them.
            # LIMIT 1 + a plain `.select("version").collect()` is a driver-local CollectLimit (no .agg, no
            # executor aggregation), so it avoids the spark.rpc.message.maxSize task abort the original
            # full-history `.agg(max("version"))` form hit on a long transaction log.
            current_version = spark.sql(f"DESCRIBE HISTORY {SOURCE_FQN} LIMIT 1").select("version").collect()[0][0]
            seed_max_files = MAX_FILES_PER_TRIGGER or _SEED_MAX_FILES_PER_TRIGGER_DEFAULT
            print(f"streaming_start=new: first run, draining initial snapshot as a NO-OP seed "
                  f"(startingVersion={current_version}, maxFilesPerTrigger={seed_max_files}); existing data "
                  f"is NOT sent to ES, only a resume point is established at {checkpoint_location}")
            # skipChangeCommits matches the main reader (a source-identity option, must agree on resume);
            # startingVersion is numeric so the seed persists an offset even with no new data ("latest"
            # would run zero batches and persist none, so the next run would re-seed and could skip data).
            seed_reader = (
                spark.readStream
                .option("skipChangeCommits", "true")
                .option("startingVersion", str(current_version))
                .option("maxFilesPerTrigger", seed_max_files)
            )
            if MAX_BYTES_PER_TRIGGER:
                seed_reader = seed_reader.option("maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)
            # foreachBatch does NOTHING: the batch DataFrame is never acted on, so no source data is read,
            # transformed, or written - the engine merely advances and commits the streaming offset for
            # each (empty-effect) micro-batch, which is precisely the resume point we want. availableNow
            # drains all currently-available data this way, then stops. A UNIQUE seed query name avoids
            # colliding with the main query on a reused SparkSession.
            #
            # START BOUNDARY (intentional): availableNow snapshots its END offset at query LAUNCH (Delta's
            # lastOffsetForTriggerAvailableNow) and drains only up to that snapshot, so the resume point is
            # the source's latest version AT SEED LAUNCH. Two consequences, both intended for
            # streaming_start=new ("start from ~now", never backfill history):
            #   - Commits that land WHILE the drain runs (which can be many hours on a large-history
            #     source) are NOT lost: they are past the seed's snapshot, so the main stream below resumes
            #     from the committed offset and exports everything after it.
            #   - The only commits the seed skips are any that land in the sub-second window between the
            #     `DESCRIBE HISTORY` version resolution above and this .start() (i.e. strictly-after the
            #     pinned startingVersion but at/under the launch snapshot). That near-instant startup
            #     boundary is deliberately treated as part of "now" and not exported - consistent with the
            #     feature's contract of starting fresh rather than replaying recent history.
            seed_query = (
                seed_reader.table(SOURCE_FQN).writeStream
                .queryName(f"{CONFIG_NAME}-seed-{uuid.uuid4().hex[:8]}")
                .option("checkpointLocation", checkpoint_location)
                .foreachBatch(lambda _batch_df, _batch_id: None)
                .trigger(availableNow=True)
                .start()
            )
            seed_query.awaitTermination()
            # The seed exists to persist a resume offset. A numeric startingVersion under availableNow
            # normally commits at least one offset even with no new data, but we do NOT rely on that: if
            # there is nothing to drain at/after the pinned version (e.g. the current version is a
            # metadata-only commit with no data files and no later commits), availableNow can run ZERO
            # micro-batches and persist NO offset. The main reader below carries no startingVersion, so
            # against an empty checkpoint it would backfill the ENTIRE table to ES - exactly what this
            # feature prevents. So VERIFY an offset was actually committed; if not, pin startingVersion on
            # the main reader as a fallback (it reads only from the current version forward, never
            # re-exporting history, and is a no-op in the normal case where the seed did commit).
            if checkpoint_offsets_state(checkpoint_location, dbutils.fs.ls) == HAS_OFFSET:
                print(f"streaming_start=new: seed drain complete; the main stream resumes incrementally "
                      f"from {checkpoint_location}")
            else:
                print(f"streaming_start=new: seed committed no offset (nothing to drain at "
                      f"startingVersion={current_version}); pinning startingVersion={current_version} on "
                      f"the main reader so existing history is NOT re-exported")
                reader = reader.option("startingVersion", str(current_version))
        else:  # "unknown"
            # The offsets listing FAILED for a reason other than a clean not-found, so we cannot tell a
            # resume from a first run. Fall back to the PROVEN-SAFE original behavior: seed startingVersion
            # on the MAIN reader. It is a no-op on a real resume (Spark ignores it once an offset exists)
            # and a correct first-run seed otherwise - and, unlike the no-op drain, it NEVER skips a
            # backlog on a misclassified resume. (Slower on a true first run against a huge table, but this
            # path is rare and correctness comes first.)
            current_version = spark.sql(f"DESCRIBE HISTORY {SOURCE_FQN} LIMIT 1").select("version").collect()[0][0]
            print(f"streaming_start=new: checkpoint offsets state UNKNOWN (listing error); falling back to "
                  f"startingVersion={current_version} on the main reader (safe on resume and first run)")
            reader = reader.option("startingVersion", str(current_version))
    # `reader` is now fully prepared: skipChangeCommits, any rate limits, and - for a first-run new-mode
    # seed - either a persisted checkpoint offset (from the no-op drain) or a pinned startingVersion. The
    # next cell runs the actual stream from it.

# COMMAND ----------
# STREAMING run - STEP 2 of 2: RUN the stream (streaming mode only). Read the RAW source as a Delta
# stream using the `reader` prepared in the previous cell, and export each micro-batch via foreach_batch.
# The trigger is chosen below: availableNow (drain-and-stop, the scheduled/serverless default) or
# ProcessingTime (an always-on continuous job). `reader` carries over from the previous cell via the
# shared notebook session; this cell re-opens the same `if PIPELINE_MODE == "streaming":` guard.
if PIPELINE_MODE == "streaming":
    stream_df = reader.table(SOURCE_FQN)

    # The Spark-UI query name: CONFIG_NAME (readable identifier of THIS pipeline) plus a short unique
    # per-run suffix. The suffix keeps the name UNIQUE among a session's active queries so a re-run on a
    # REUSED/interactive SparkSession cannot collide with a still-active prior query (Spark rejects a
    # duplicate active query name at .start()). It also scopes the listener's filter below: a leftover
    # listener from a prior run carries that run's name, so it can never match this run's progress and
    # emit duplicate lines. The CONFIG_NAME prefix keeps the streaming tab legible.
    _QUERY_NAME = f"{CONFIG_NAME}-{uuid.uuid4().hex[:8]}"

    # Ephemeral per-batch observability. Register a StreamingQueryListener BEFORE .start() (so it also
    # catches the query-started event) that logs one STREAM_PROGRESS line per micro-batch - backlog
    # (numFilesOutstanding/numBytesOutstanding), the durationMs breakdown, rates, and Delta offset
    # progress - via the shared, unit-tested format_progress. This is the per-batch visibility an
    # always-on continuous run otherwise lacks; the lines land in the driver log and complement the
    # Spark UI Structured Streaming tab. Registered for BOTH triggers (availableNow too). OBSERVABILITY
    # ONLY and FAIL-SOFT: every callback swallows its own errors, so a logging fault can never fail or
    # slow the export - a listener exception must not touch the stream.
    from pyspark.sql.streaming import StreamingQueryListener  # noqa: E402

    class _ProgressLogger(StreamingQueryListener):
        # Filter every callback to THIS run's query, so a shared/reused SparkSession running other
        # StreamingQueries never gets logged under this config's trail. onQueryStarted/onQueryProgress
        # match on the query NAME (we set queryName=CONFIG_NAME and it rides on both events); the
        # terminated event carries no name, so it matches on the runId captured after .start() (until
        # that is set - only this query's own startup window - it does not filter, which is harmless).
        our_run_id = None  # set on the instance to this run's query.runId once it has started

        def onQueryStarted(self, event):
            try:
                if event.name != _QUERY_NAME:
                    return
                print(f"{PROGRESS_TAG} query started: name={event.name!r} id={event.id} runId={event.runId}")
            except Exception as _e:  # never let observability disturb the stream
                print(f"WARNING: {PROGRESS_TAG} onQueryStarted logging failed ({type(_e).__name__}: {_e})")

        def onQueryProgress(self, event):
            try:
                progress = json.loads(event.progress.json)
                if progress.get("name") != _QUERY_NAME:
                    return
                print(format_progress(progress))
                # Surface this batch's bulk/tail diagnostics in the CELL, if foreachBatch wrote them for
                # this batch (bulk_stats on, non-empty batch). Read the per-batch relay file written
                # server-side; this runs on the driver so a plain file read reaches it. Own try so a
                # relay/read fault cannot suppress the STREAM_PROGRESS line just printed; only this run's
                # batches reach here (name-filtered above).
                try:
                    _relayed = read_relay_line(progress.get("batchId"))
                    if _relayed:
                        print(_relayed)
                except Exception as _re:
                    print(f"WARNING: {BULK_STATS_TAG} onQueryProgress relay failed "
                          f"({type(_re).__name__}: {_re})")
            except Exception as _e:
                print(f"WARNING: {PROGRESS_TAG} onQueryProgress logging failed ({type(_e).__name__}: {_e})")

        def onQueryTerminated(self, event):
            try:
                if self.our_run_id is not None and str(event.runId) != str(self.our_run_id):
                    return
                _exc = getattr(event, "exception", None)
                print(f"{PROGRESS_TAG} query terminated: id={event.id} runId={event.runId}"
                      + (f" exception={_exc}" if _exc else ""))
            except Exception as _e:
                print(f"WARNING: {PROGRESS_TAG} onQueryTerminated logging failed ({type(_e).__name__}: {_e})")

    # Register FAIL-SOFT: on a cluster access mode where the streaming-listener API is unsupported or
    # restricted, addListener itself could raise - which must NOT fail the export. Warn and continue
    # WITHOUT progress logging. Track whether registration actually succeeded so the finally below only
    # removes a listener that was added.
    _progress_listener = _ProgressLogger()
    _listener_registered = False
    try:
        spark.streams.addListener(_progress_listener)
        _listener_registered = True
        print(f"{PROGRESS_TAG} listener registered (per-batch progress logging)")
    except Exception as _e:
        print(f"WARNING: {PROGRESS_TAG} could not register progress listener "
              f"({type(_e).__name__}: {_e}); continuing WITHOUT per-batch progress logging")

    # The trigger is chosen by streaming_trigger_interval (a deploy-time base_parameter from the config's
    # `continuous` block), which is the SINGLE signal that keeps the job's shape and this trigger in step:
    # - EMPTY => Trigger.availableNow: drain every currently-available source commit in one or more
    #   micro-batches, then STOP. The supported serverless trigger, and it fits the scheduled/on-demand
    #   DAB job model - each RUN exports the new data since the last run and terminates. availableNow
    #   still honors the rate-limits above (multiple batches) and startingVersion (first-run seed).
    # - NON-EMPTY => Trigger.ProcessingTime(interval): an ALWAYS-ON micro-batch stream that never
    #   terminates. Set only by a continuous pipeline, which config restricts to CLASSIC compute
    #   (serverless rejects ProcessingTime). The generated job carries a Databricks Jobs `continuous`
    #   trigger that keeps this run perpetually alive (auto-restarting on failure); each restart resumes
    #   from the checkpoint (the startingVersion seed above is skipped once an offset exists).
    try:
        writer = (
            stream_df.writeStream
            .queryName(_QUERY_NAME)  # unique per-run name (CONFIG_NAME + suffix): legible + collision-free
            .option("checkpointLocation", checkpoint_location)
            .foreachBatch(foreach_batch)
        )
        if STREAMING_TRIGGER_INTERVAL:
            print(f"continuous streaming: ProcessingTime trigger every {STREAMING_TRIGGER_INTERVAL!r} "
                  f"(always-on; this run does not self-terminate)")
            query = writer.trigger(processingTime=STREAMING_TRIGGER_INTERVAL).start()
            _progress_listener.our_run_id = query.runId  # scope the terminated-event filter to this run
            # awaitTermination BLOCKS for the life of an always-on run, returning ONLY if the stream stops:
            # on a FAILURE it re-raises (the run fails and the Jobs continuous trigger auto-restarts it, so a
            # lost batch never passes silently), and on a GRACEFUL stop (job cancel, redeploy, cluster
            # shutdown) it returns normally. Either way there is NO drain-and-stop reconciliation for an
            # always-on run - observability is the per-batch metrics foreachBatch writes as each batch commits
            # plus the Databricks Jobs continuous-run state (RUNNING / restart count / failure notifications).
            # So set a summary noting the stop and do NOT run the availableNow summary below (this branch owns
            # its own RUN_SUMMARY; the drain-and-stop reconciliation is the else branch's, for availableNow).
            query.awaitTermination()
            RUN_SUMMARY = (
                f"streaming_trigger=continuous({STREAMING_TRIGGER_INTERVAL}) stopped; "
                f"checkpoint={checkpoint_location}"
            )
            print(f"CONTINUOUS STREAM STOPPED: {RUN_SUMMARY}")
        else:
            # availableNow (drain-and-stop): start, drain to completion, then summarize THIS run.
            query = writer.trigger(availableNow=True).start()
            _progress_listener.our_run_id = query.runId  # scope the terminated-event filter to this run
            query.awaitTermination()

            # Report how many rows this run pushed, read back from the per-batch JSON metrics foreachBatch
            # wrote under metrics_dir (see above). This is the reliable driver-side total: it survives the
            # server-side foreachBatch boundary and the async delivery of query.recentProgress, both of which
            # under-reported in testing. DEDUP by batch_id first (max written per batch_id), so a batch that
            # was retried within this run is counted once, not summed twice - then total. A run with no new
            # source data wrote no metric files (empty dir), which reads as 0 batches / 0 rows: a valid
            # outcome, not a failure. Each recorded batch used bulk_write(raise_on_error=True), so any batch
            # that did not fully succeed failed the run instead of recording, and the total is exact.
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
                # Dir exists: read it WITHOUT catching, so any genuine read failure propagates and fails the
                # run rather than being silently reported as 0.
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
    finally:
        # Remove our listener when the run ends (availableNow drain-and-stop, graceful continuous
        # stop, OR failure) so it does not survive on a reused SparkSession and keep logging unrelated
        # queries, and so repeated runs do not accumulate listeners. Removing on termination (rather
        # than stashing a session-global handle) also means one config's cleanup can never touch
        # another config's live listener. Guarded by _listener_registered so we never try to remove a
        # listener that was never added (registration is fail-soft above). Fail-soft: cleanup must not
        # mask a real run error.
        if _listener_registered:
            try:
                spark.streams.removeListener(_progress_listener)
                print(f"{PROGRESS_TAG} listener removed")
            except Exception as _e:
                print(f"WARNING: {PROGRESS_TAG} could not remove listener ({type(_e).__name__}: {_e})")

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

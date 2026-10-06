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
# MAGIC - `monitoring_log_table`: the shared monitoring Delta table (`catalog.schema.table`) for the durable
# MAGIC   log, from `${var.monitoring_log_table}`. Deploy-time, not per-run; create it once with the
# MAGIC   `_log table create` job. Only used when `monitoring_log_enabled` is true.
# MAGIC
# MAGIC Run-time parameters (job parameters; overridable per run with `--params <name>=<value>`):
# MAGIC - `pipeline_mode`: `batch` | `streaming` (default from config). Clearing a stale streaming
# MAGIC   checkpoint is handled by the dedicated `_checkpoint clear` job, not a pipeline_mode.
# MAGIC - `filter_condition`: optional Spark SQL predicate applied before the write (default from config).
# MAGIC - `chunk_size`, `write_concurrency`, `request_timeout`, `transport_max_retries`, `max_retries_per_doc`,
# MAGIC   `require_existing_index`, `verify_certs`: EsWriteConfig tuning (default from config; omitted there and
# MAGIC   unset per run => connector default). `request_timeout` (seconds) and `transport_max_retries` (0 disables)
# MAGIC   tune a write that times out mid-send; `max_retries_per_doc` (0 disables) is the PER-DOCUMENT retry count
# MAGIC   for rows ES rejects with a 429 (write queue full), distinct from the whole-request `transport_max_retries`.
# MAGIC - `streaming_start`: `new` (default; only new commits) | `full` (backfill the whole table);
# MAGIC   streaming only, honored on the first run before a checkpoint exists. `new` establishes the
# MAGIC   checkpoint at the current source position via a no-op availableNow seed (drains the initial
# MAGIC   snapshot without exporting to ES), so history is skipped without stalling on a large source.
# MAGIC - `max_files_per_trigger`, `max_bytes_per_trigger`: streaming read rate-limits that bound each
# MAGIC   micro-batch (default from config; empty => Spark defaults). Streaming only; useful for a backfill.
# MAGIC - `monitoring_log_enabled`: `true` | `false` (default from `${var.monitoring_log_enabled}`/config;
# MAGIC   empty => off). When true, the run ALSO appends rows to `monitoring_log_table`: `run_start` and
# MAGIC   `run_end` for the run, and `batch_start` / `batch_end` / `batch_summary` for every batch (one batch in
# MAGIC   batch mode, one per micro-batch in streaming). See pipeline_lib/monitoring_sink.py for the row model.
# MAGIC   When ON the log is part of the contract, not best-effort: an unset/malformed/unwritable table or
# MAGIC   ANY failed append FAILS the task (so support is notified and no further data is sent unlogged).
# MAGIC   A batch's `batch_start` row is written before its data is sent, so a log outage stops the export
# MAGIC   before the next batch reaches ES. When off, nothing is written and nothing about the export changes.

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
dbutils.widgets.text("max_retries_per_doc", "", "EsWriteConfig max_retries_per_doc: PER-DOCUMENT retries for rows ES rejects with a 429 (write queue full); 0 disables (empty => connector default 3)")
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
# Durable monitoring log. monitoring_log_enabled is a run-time job parameter (true|false; default from
# ${var.monitoring_log_enabled}/config); monitoring_log_table is a deploy-time base_parameter (the
# ${var.monitoring_log_table} bundle variable, the shared catalog.schema.table). When enabled, the run
# also APPENDS its run and batch rows to that table (in addition to the log lines), and the table is then
# REQUIRED: unset, malformed, missing, or unwritable fails the run (see the setup below).
dbutils.widgets.text("monitoring_log_enabled", "", "Durable monitoring sink: true|false; also append run metrics to ${var.monitoring_log_table} (empty => off)")
# job_run_id / task_run_id: deploy-time base_parameters bound to the Jobs dynamic value references
# {{job.run_id}} / {{task.run_id}} (resolved per run by the Jobs service; empty on an interactive run).
dbutils.widgets.text("job_run_id", "", "Databricks job run id ({{job.run_id}}; set by the generated job)")
dbutils.widgets.text("task_run_id", "", "Databricks task run id ({{task.run_id}}; set by the generated job)")
dbutils.widgets.text("monitoring_log_table", "", "Fully-qualified catalog.schema.table for the monitoring sink (deploy-time; empty => sink skipped). Created by the `_log table create` job.")
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
MAX_RETRIES_PER_DOC = dbutils.widgets.get("max_retries_per_doc").strip()
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
MONITORING_LOG_ENABLED = dbutils.widgets.get("monitoring_log_enabled").strip()
MONITORING_LOG_TABLE = dbutils.widgets.get("monitoring_log_table").strip()
JOB_RUN_ID_PARAM = dbutils.widgets.get("job_run_id").strip()
TASK_RUN_ID = dbutils.widgets.get("task_run_id").strip()
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
    require_es_flag,
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
# Durable monitoring log (optional): pure row builders + the table schema/name validator, shared with
# the `_log table create` job. Kept in pipeline_lib so it is unit-tested off-cluster; this notebook owns
# only the Spark append (append_monitoring_rows below).
from pipeline_lib.monitoring_sink import (  # noqa: E402
    BATCH_MODE_BATCH_ID,
    ROW_FIELDS,
    batch_end_row,
    batch_start_row,
    batch_summary_row,
    error_facts,
    es_counts,
    es_write_summary,
    leftover_relay_ids,
    progress_batch_ids,
    run_end_row,
    run_start_row,
    validate_table_name,
)
# Ephemeral per-batch observability (pure Python, no Spark): the STREAM_PROGRESS / BULK_STATS log-line
# formatters. Kept in pipeline_lib so they are unit-tested off-cluster.
import json  # noqa: E402
import time  # noqa: E402
import uuid  # noqa: E402
from datetime import datetime, timezone  # noqa: E402
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
# The streaming wait loop (pure, duck-typed query; unit-tested off-cluster with a fake query).
from pipeline_lib.stream_wait import await_stream as _await_stream  # noqa: E402

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
                                         max_retries_per_doc=MAX_RETRIES_PER_DOC,
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

# Durable monitoring log setup (OPTIONAL; when ON it is part of the contract, FAIL-CLOSED). Canonicalize
# the enable flag with the SAME validator the config/registry use (a bad --params value fails closed here).
# The log is ACTIVE when it resolves true; it then REQUIRES a valid table name, so the log switched on with
# the table unset or malformed fails the run here, before any data moves (an export that silently runs
# unlogged is exactly what this guards against). Off => every append below is a no-op and nothing about the
# export changes.
MONITORING_LOG_ENABLED = require_es_flag(MONITORING_LOG_ENABLED, "monitoring_log_enabled job parameter")
_MONITORING_TABLE = ""
if MONITORING_LOG_ENABLED == "true":
    if not MONITORING_LOG_TABLE:
        raise ValueError("monitoring_log_enabled=true but monitoring_log_table is unset "
                         "(${var.monitoring_log_table}): set the bundle variable and run the `_log table "
                         "create` job, or turn monitoring_log_enabled off")
    _MONITORING_TABLE = validate_table_name(MONITORING_LOG_TABLE, "monitoring_log_table")
MONITORING_ACTIVE = bool(_MONITORING_TABLE)


def _resolve_job_run_id():
    """The Databricks job run id, used to group a run's monitoring rows. Preferred source: the job_run_id
    base_parameter ({{job.run_id}}, resolved by the Jobs service; a literal "{{" means it was not
    substituted, so it is ignored). Fallbacks, for interactive/legacy runs: the notebook context tags, then
    a per-process uuid (so a run's rows still share an id, just not the platform run id)."""
    if JOB_RUN_ID_PARAM and "{{" not in JOB_RUN_ID_PARAM:
        return JOB_RUN_ID_PARAM
    try:
        _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        _tags = json.loads(_ctx.toJson()).get("tags", {}) or {}
        for _k in ("jobRunId", "multitaskParentRunId", "rootRunId", "runId"):
            if _tags.get(_k):
                return str(_tags[_k])
    except Exception:
        pass
    return f"local-{uuid.uuid4().hex[:12]}"


JOB_RUN_ID = _resolve_job_run_id() if MONITORING_ACTIVE else ""


class MonitoringLogError(RuntimeError):
    """A monitoring log append failed while the log is ON. Raised (never swallowed) so the batch, and with it
    the task, fails: support is notified and no further data is sent without a log record."""


def append_monitoring_rows(rows, session=None):
    """Append monitoring rows (dicts keyed by ROW_FIELDS, from pipeline_lib.monitoring_sink builders) to the
    shared Delta table. No-op when the log is off. When ON, FAIL-CLOSED: any error raises
    MonitoringLogError (chained to the cause). Builds a DataFrame with the payload + timestamps as strings,
    then parse_json -> VARIANT and cast -> TIMESTAMP, stamps ingest_ts = current_timestamp() (the write
    time), and appends BY NAME via writeTo().append() (so the table's physical column order does not matter,
    e.g. a migrated table with `status` added last). A `session` is passed from foreachBatch (its micro-batch
    session); elsewhere the global spark is used."""
    if not MONITORING_ACTIVE or not rows:
        return
    sess = session if session is not None else spark
    try:
        from pyspark.sql import functions as _F  # noqa: E402
        _schema = ("config_name string, job_run_id string, record_type string, status string, "
                   "batch_id bigint, event_ts string, batch_start_ts string, batch_end_ts string, "
                   "payload string")
        _df = sess.createDataFrame([tuple(r[f] for f in ROW_FIELDS) for r in rows], _schema)
        _df = (_df
               .withColumn("event_ts", _F.col("event_ts").cast("timestamp"))
               .withColumn("batch_start_ts", _F.col("batch_start_ts").cast("timestamp"))
               .withColumn("batch_end_ts", _F.col("batch_end_ts").cast("timestamp"))
               .withColumn("payload", _F.expr("parse_json(payload)"))
               .withColumn("ingest_ts", _F.current_timestamp()))
        _df.writeTo(_MONITORING_TABLE).append()
    except Exception as _e:
        raise MonitoringLogError(
            f"monitoring log append to {_MONITORING_TABLE!r} failed ({type(_e).__name__}: {_e}); "
            f"{len(rows)} row(s) ({', '.join(sorted({r['record_type'] for r in rows}))}) not persisted. "
            f"Failing the task so no further data is sent without a log record.") from _e


def append_failure_rows(rows, exc, session=None):
    """Append the rows that record a failure (`exc`) WITHOUT ever masking it: the caller re-raises `exc`
    afterwards. If the append itself fails (often the same outage that caused `exc`), that is attached to
    `exc` as a note, so the original error stays the one the run fails with and the log fault is still
    visible next to it."""
    try:
        append_monitoring_rows(rows, session=session)
    except Exception as _log_exc:
        _note = f"monitoring log: the failure row(s) could not be written either: {_log_exc}"
        print(f"WARNING: {_note}")
        if hasattr(exc, "add_note"):  # Python 3.11+; serverless environment versions are not pinned here
            exc.add_note(_note)


if MONITORING_ACTIVE:
    print(f"monitoring log ACTIVE -> {_MONITORING_TABLE} (job_run_id={JOB_RUN_ID})")

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
# RUN START. From here on every outcome of the run is recorded in the monitoring log (when it is on):
# `run_start` now, and `run_end` when the run ends - written by the export cells on success / graceful stop,
# and by run_guard (below) on ANY exception raised in a guarded cell, so a failure while preparing the
# export (reading the secret, the view, the checkpoint) is recorded too, not only one inside the ES write.
# A run killed outright (cancel, driver loss) cannot write run_end: its run_start with no run_end IS the
# record. Writing run_start is also the log's startup check: a missing or unwritable table fails the run
# here, before any data moves.
import contextlib  # noqa: E402

_RUN_START_DT = datetime.now(timezone.utc)
_RUN_TRIGGER = ((f"continuous({STREAMING_TRIGGER_INTERVAL})" if STREAMING_TRIGGER_INTERVAL else "availableNow")
                if PIPELINE_MODE == "streaming" else None)
# The identity a run's rows share; repeated on run_end so either row alone says what ran.
_RUN_IDENTITY = {"mode": PIPELINE_MODE, "es_index": cfg["es_index_name"], "trigger": _RUN_TRIGGER}
append_monitoring_rows([run_start_row(CONFIG_NAME, JOB_RUN_ID, {
    **_RUN_IDENTITY,
    "view": VIEW_FQN, "source": SOURCE_FQN, "environment": ENVIRONMENT,
    "connector_version": _connector_version, "filter_condition": FILTER_CONDITION,
    "task_run_id": TASK_RUN_ID if TASK_RUN_ID and "{{" not in TASK_RUN_ID else None,
    "write_overrides": write_overrides, "write_repartition": WRITE_REPARTITION,
    "streaming_start": STREAMING_START if PIPELINE_MODE == "streaming" else None,
    "checkpoint": f"{CHECKPOINT_BASE_PATH.rstrip('/')}/{CONFIG_NAME}" if PIPELINE_MODE == "streaming" else None,
}, _RUN_START_DT)])
_RUN_END_WRITTEN = False


def record_run_end(status, facts):
    """Write this run's run_end row (status success | stopped; error goes through run_guard). Raises on a
    log failure like every other append: a run whose end cannot be recorded fails."""
    global _RUN_END_WRITTEN
    append_monitoring_rows([run_end_row(CONFIG_NAME, JOB_RUN_ID, status, {**_RUN_IDENTITY, **facts},
                                        _RUN_START_DT, datetime.now(timezone.utc))])
    _RUN_END_WRITTEN = True


@contextlib.contextmanager
def run_guard():
    """Wrap a cell's body so an exception it raises is recorded as this run's run_end (status error) before
    it propagates. Written at most once per run, never masks the exception (append_failure_rows), and the
    exception is always re-raised, so failure semantics are unchanged: the task still fails and its retry
    policy applies."""
    global _RUN_END_WRITTEN
    try:
        yield
    except Exception as _exc:
        if not _RUN_END_WRITTEN:
            _RUN_END_WRITTEN = True
            _end = datetime.now(timezone.utc)
            append_failure_rows([run_end_row(CONFIG_NAME, JOB_RUN_ID, "error", {
                **_RUN_IDENTITY, **error_facts(_exc),
                "elapsed_ms": (_end - _RUN_START_DT).total_seconds() * 1000.0,
            }, _RUN_START_DT, _end)], _exc)
        raise


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
with run_guard():
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
#
# Monitoring log (when on): a batch-mode run is exactly ONE batch (batch_id BATCH_MODE_BATCH_ID), recorded
# like a streaming micro-batch: batch_start immediately before the write, then batch_end (the outcome) and
# batch_summary (the ES write diagnostics), then the run's run_end. A failure (a timeout that exhausts
# retries RAISES from bulk_write, or a reconcile mismatch) records batch_end status=error (plus the
# diagnostics when the write returned some) before it propagates, and run_guard records run_end.
if PIPELINE_MODE == "batch":
    with run_guard():
        export_df = apply_filter(spark.table(VIEW_FQN))
        # bulk_write runs one ES bulk stream per DataFrame partition (mapInPandas), so write parallelism ==
        # partition count. Read parallelism (max_partition_bytes, set above) is the primary lever: the scan
        # and this narrow, shuffle-free transform preserve that partition count through to the write, so
        # the write already fans out and WRITE_REPARTITION defaults to 0 (off). Set WRITE_REPARTITION > 0
        # only to override the write's partition count independently of the read (e.g. a view that
        # shuffles resets it to spark.sql.shuffle.partitions); the target is the same either way, ~2-3x
        # total worker cores. Repartition AFTER the filter so the surviving rows spread evenly.
        if WRITE_REPARTITION > 0:
            export_df = export_df.repartition(WRITE_REPARTITION)
        # Beginning-of-batch wall clock (UTC). In batch mode the whole export IS the batch.
        _export_start_dt = datetime.now(timezone.utc)
        append_monitoring_rows([batch_start_row(CONFIG_NAME, JOB_RUN_ID, BATCH_MODE_BATCH_ID,
                                                {"mode": "batch"}, _export_start_dt)])
        result = None
        try:
            # Driver wall clock around the write, to LOCATE a tail that persists after the Spark UI shows
            # every write task complete. bulk_write's own collect_ms (under bulk_stats) is the time INSIDE
            # Spark's collect (the write job PLUS Spark's result finalization), so if this driver-measured
            # wall is ~collect_ms the tail is inside the write itself - typically a straggler partition,
            # which the BULK_STATS tail line below then names - whereas wall well above collect_ms would be
            # work between the collect and this return. Two time.time() calls on the driver; nothing
            # touches the write path.
            _bw_t0 = time.time()
            result = bulk_write(export_df, es_write_config)
            _bw_wall_ms = (time.time() - _bw_t0) * 1000.0
            # Print the core count dict on one line; when bulk_stats is on, result also carries a
            # per-partition 'bulk_stats' list, which we render SEPARATELY (below) as readable BULK_STATS
            # lines rather than dumping the raw list into the result line. reconcile_or_raise reads only
            # the counts, so the extra key is ignored there.
            _core_result = {k: v for k, v in result.items() if k != "bulk_stats"}
            print(f"batch bulk_write result: {_core_result}")
            print(f"{BULK_STATS_TAG} driver: bulk_write_wall_ms={_bw_wall_ms:.1f}")
            if "bulk_stats" in result:
                # The tail/straggler summary FIRST (the one-line answer to "where did the wall time go"),
                # then the full per-partition breakdown. Both fail-soft.
                print(format_tail_summary(result))
                print(format_bulk_stats(result["bulk_stats"]))
            reconcile_or_raise(result, index=es_write_config.index)
        except Exception as _exc:
            _fail_dt = datetime.now(timezone.utc)
            _rows = [batch_end_row(CONFIG_NAME, JOB_RUN_ID, BATCH_MODE_BATCH_ID, "error", {
                **error_facts(_exc),
                # The counts too when the write returned (a reconcile failure), so the table says HOW the
                # batch failed reconciliation, not only that it did.
                **(es_counts(result) if isinstance(result, dict) else {}),
                "elapsed_ms": (_fail_dt - _export_start_dt).total_seconds() * 1000.0,
            }, _export_start_dt, _fail_dt)]
            if isinstance(result, dict):
                _rows.append(batch_summary_row(CONFIG_NAME, JOB_RUN_ID, BATCH_MODE_BATCH_ID,
                                               es=es_write_summary(result, wall_ms=_bw_wall_ms), status="error"))
            append_failure_rows(_rows, _exc)
            raise
        _bw_end_dt = datetime.now(timezone.utc)
        append_monitoring_rows([
            batch_end_row(CONFIG_NAME, JOB_RUN_ID, BATCH_MODE_BATCH_ID, "success", es_counts(result),
                          _export_start_dt, _bw_end_dt),
            batch_summary_row(CONFIG_NAME, JOB_RUN_ID, BATCH_MODE_BATCH_ID,
                              es=es_write_summary(result, wall_ms=_bw_wall_ms)),
        ])
        RUN_SUMMARY = (
            f"written={result['written']} deleted={result['deleted']} errors={result['errors']} "
            f"ignored={result['ignored']} total_input={result['total_input']}"
        )
        print(f"BATCH EXPORT COMPLETE: {RUN_SUMMARY}")
        record_run_end("success", {"batches": 1, **es_counts(result)})

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
    with run_guard():
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

        # Per-batch RELAY directory, a SIBLING of _run_metrics (kept out of it so the summary's
        # spark.read.json(metrics_dir) never sees these files). Under Spark Connect foreachBatch runs
        # server-side (its prints reach only the driver log, and its Python memory is a different process),
        # while the wait loop in the run cell runs in this notebook process. An in-memory handoff cannot cross
        # that split, so foreachBatch writes each batch's diagnostics to `{relay_dir}/{batch_id}` (via the SAME
        # Spark write it uses for the row count - the one file mechanism proven to work server-side here):
        # one JSON line with the ES write summary (for the batch_summary row) and the BULK_STATS overall+tail
        # text (to print into the cell). The wait loop reads it once Spark has published that batch's progress
        # and joins the two into the batch's batch_summary. A relay is deleted only AFTER its batch_summary is
        # written, and the directory is NOT cleared at run start: an attempt that died after a batch committed
        # but before its summary was written leaves that relay behind, and the sweep at the end of this cell
        # summarizes it first (with the relay off, leftovers are simply deleted).
        relay_dir = f"{checkpoint_location}/_batch_relay"
        # The relay is needed only when something consumes it: the monitoring log (batch_summary.es) or the
        # bulk_stats relay-to-cell print.
        _RELAY_ON = MONITORING_ACTIVE or BULK_STATS.strip().lower() == "true"

        def read_relay(batch_id):
            """Read the relay foreachBatch wrote for `batch_id` (a Spark `.text()` output directory holding one
            `part-*` file with one JSON line) and return it as a dict, or None when it is absent or unreadable.
            Does NOT delete it: the caller drops it (drop_relay) only once the batch's summary is safely
            written, so a failed read or a failed summary append leaves it for a later sweep instead of losing
            the batch's diagnostics. Runs in the notebook process via dbutils.fs, so any checkpoint URI works
            (no FUSE mount needed). FAIL-SOFT: returns None on any problem."""
            try:
                for f in dbutils.fs.ls(f"{relay_dir}/{int(batch_id)}"):
                    if f.name.startswith("part-"):
                        return json.loads(dbutils.fs.head(f.path, 1024 * 1024))
            except Exception:
                pass
            return None

        def drop_relay(batch_id):
            """Delete `batch_id`'s spent relay directory (best-effort), so an always-on stream does not
            accumulate one directory per batch."""
            try:
                dbutils.fs.rm(f"{relay_dir}/{int(batch_id)}", recurse=True)
            except Exception:
                pass

        def _summary_already_written(job_run_id, batch_id):
            """True when the monitoring table already holds a batch_summary for this (config, run, batch).
            Consulted only by the START-UP sweep, whose leftovers may include a batch the previous process
            summarized just before its best-effort relay delete failed (the in-process high-water mark, which
            guards the later sweeps, starts empty after a restart). Raises on a read failure (fail-closed)."""
            return spark.sql(
                f"SELECT 1 FROM {_MONITORING_TABLE} WHERE record_type = 'batch_summary' "
                f"AND config_name = :c AND job_run_id = :r AND batch_id = :b LIMIT 1",
                args={"c": CONFIG_NAME, "r": str(job_run_id), "b": int(batch_id)},
            ).count() > 0

        def summarize_leftover_relays(below=None, final=False):
            """Write a batch_summary for every batch that ended successfully (it left a relay directory) but
            whose progress report was never recorded, for ids below `below` (None = all): its report was
            evicted from query.recentProgress's bounded buffer before a poll saw it, reads kept failing, or an
            earlier attempt died before summarizing it. The summary carries the relayed ES diagnostics alone,
            with a warning, so no batch is silently skipped (pipeline_lib.monitoring_sink.leftover_relay_ids).
            A relay is dropped only after its summary is written; an unreadable one is left (and warned) for
            the next sweep. With the log off, leftovers are just dropped. A listing failure only warns, except on
            the `final` sweep of a run (there is no later sweep to catch up), where it raises so a committed
            batch can never be left silently unsummarized; the same goes for an unreadable relay there."""
            if not _RELAY_ON:
                return
            try:
                _names = [f.name for f in dbutils.fs.ls(relay_dir)]
            except Exception as _e:
                if "FileNotFound" in str(_e) or "No such file" in str(_e) or "does not exist" in str(_e):
                    return
                if final and MONITORING_ACTIVE:
                    raise MonitoringLogError(f"could not list {relay_dir} to summarize unrecorded batches "
                                             f"({type(_e).__name__}: {_e})") from _e
                print(f"WARNING: could not list {relay_dir} for unrecorded batches ({type(_e).__name__}: {_e})")
                return
            for _bid in leftover_relay_ids(_names, below):
                # At or below this run's high-water mark the batch was already recorded (its relay outlived a
                # failed best-effort delete): drop it, never summarize it twice. The start-up sweep runs before
                # any batch is recorded (mark None), so a previous attempt's leftovers are never skipped here.
                _recorded = _progress_mark["last"] is not None and _bid <= _progress_mark["last"]
                if not MONITORING_ACTIVE or _recorded:
                    drop_relay(_bid)
                    continue
                _relay = read_relay(_bid)
                if not (_relay and _relay.get("es")):
                    if final:
                        raise MonitoringLogError(f"could not read the relayed diagnostics for unrecorded batch "
                                                 f"{_bid} ({relay_dir}/{_bid}); its batch_summary cannot be "
                                                 f"written. If that directory is damaged, delete it to proceed "
                                                 f"(the batch keeps its batch_start/batch_end rows).")
                    print(f"WARNING: could not read the relayed diagnostics for unrecorded batch {_bid}; "
                          f"retrying on the next sweep")
                    continue
                _owner = _relay.get("job_run_id") or JOB_RUN_ID
                if _progress_mark["last"] is None and _summary_already_written(_owner, _bid):
                    drop_relay(_bid)  # summarized before a restart; only its cleanup was lost
                    continue
                print(f"WARNING: {PROGRESS_TAG} no progress report seen for batch {_bid}; writing its "
                      f"batch_summary from the relayed ES diagnostics only")
                # Filed under the run that executed the batch (it may be an earlier, dead run).
                append_monitoring_rows([batch_summary_row(CONFIG_NAME, _owner, _bid, es=_relay["es"])])
                drop_relay(_bid)

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
            # Monitoring log (when on): batch_start goes in BEFORE any of this batch's data is sent, so a log
            # outage fails the micro-batch (and the task) before the data moves rather than after. Appended
            # from the micro-batch's OWN session (this runs server-side). FAIL-CLOSED like every append.
            _fb_start_dt = datetime.now(timezone.utc)
            append_monitoring_rows([batch_start_row(CONFIG_NAME, JOB_RUN_ID, int(batch_id), {"mode": "streaming"},
                                                    _fb_start_dt)], session=session)
            # Write via the connector, capturing its AUTHORITATIVE result (not our own .count() of the
            # input, which would over-report a partially-failed batch). raise_on_error=True makes bulk_write
            # itself raise on any rejected/unaccounted row, so a batch that does not FULLY succeed fails the
            # micro-batch here: the checkpoint does not advance and Spark reprocesses the batch. That retry
            # is an idempotent upsert ONLY when es_id_field is set (deterministic _id); with es_id_field
            # OMITTED, ES assigns fresh random _ids, so the reprocessed rows land as NEW documents and the
            # retry DUPLICATES them - streaming replays are routine, so omit es_id_field only for a stream
            # where duplicates are acceptable. If it never recovers the run fails with no summary. A failed
            # write records batch_end status=error (never masking the write's own error) before re-raising.
            _bw_t0 = time.time()
            try:
                result = bulk_write(transformed, es_write_config, raise_on_error=True)
            except Exception as _exc:
                _fail_dt = datetime.now(timezone.utc)
                append_failure_rows([batch_end_row(CONFIG_NAME, JOB_RUN_ID, int(batch_id), "error", {
                    **error_facts(_exc), "elapsed_ms": (_fail_dt - _fb_start_dt).total_seconds() * 1000.0,
                }, _fb_start_dt, _fail_dt)], _exc, session=session)
                raise
            _bw_wall_ms = (time.time() - _bw_t0) * 1000.0
            _fb_end_dt = datetime.now(timezone.utc)
            # When bulk_stats is on, log a COMPACT one-line rollup of this micro-batch's ES bulk-send
            # diagnostics (overall docs/send, rtt, took) so an always-on run has per-batch visibility
            # without the full per-partition breakdown flooding the log. format_bulk_stats is fail-soft,
            # so a diagnostic-formatting fault can never disturb the write. This print runs server-side
            # (micro-batch), so its stdout lands in the driver LOG (surfacing as stderr), not the cell.
            if "bulk_stats" in result:
                print(format_bulk_stats(result["bulk_stats"], oneline=True) + f" batch_id={batch_id}")
            # Persist this clean batch's authoritative written count as one JSON file, keyed by batch_id so
            # the summary can dedup a retried batch (write mode append; each batch is its own small file).
            session.createDataFrame(
                [(int(batch_id), int(result.get("written", 0) or 0))], "batch_id bigint, written bigint"
            ).coalesce(1).write.mode("append").json(metrics_dir)
            # The batch's OUTCOME, last, so status=success means everything this batch had to do is done.
            # (Its diagnostics follow as batch_summary from the wait loop, once Spark publishes the progress.)
            append_monitoring_rows([batch_end_row(CONFIG_NAME, JOB_RUN_ID, int(batch_id), "success",
                                                  es_counts(result), _fb_start_dt, _fb_end_dt)], session=session)
            # Relay this batch's diagnostics to the wait loop (see relay_dir above), AFTER batch_end, so a relay
            # directory always means the batch ended successfully (summarize_leftover_relays relies on that).
            # With the monitoring log ON the relay is part of the log (it is how this batch's batch_summary is
            # guaranteed even if its progress report is lost), so a failed relay write FAILS the batch like any
            # log write. With the log off it only feeds the BULK_STATS cell print, so a fault just warns.
            if _RELAY_ON:
                try:
                    # job_run_id travels with the relay so a summary written later by a DIFFERENT run (the
                    # start-up sweep) is still filed under the run whose batch_start/batch_end it completes.
                    _relay = json.dumps({"es": es_write_summary(result, wall_ms=_bw_wall_ms),
                                         "line": bulk_stats_relay_line(result, batch_id),
                                         "job_run_id": JOB_RUN_ID}, default=str)
                    session.createDataFrame([(_relay,)], "line string") \
                        .coalesce(1).write.mode("overwrite").text(f"{relay_dir}/{int(batch_id)}")
                except Exception as _e:
                    if MONITORING_ACTIVE:
                        raise MonitoringLogError(f"could not write batch {batch_id}'s relay for its batch_summary "
                                                 f"({type(_e).__name__}: {_e})") from _e
                    print(f"WARNING: could not relay batch {batch_id} diagnostics to the notebook "
                          f"({type(_e).__name__}: {_e}); continuing")

        # THE WAIT LOOP: how this notebook waits on a running stream, records each batch's progress, and notices
        # the stream ending: pipeline_lib.stream_wait.await_stream (see its docstring for why a blocking
        # awaitTermination() plus a StreamingQueryListener was replaced: both ride long-lived Spark Connect
        # calls, and a blocking awaitTermination() was reproduced never returning after its query FAILED,
        # leaving the task RUNNING and never restarted). It waits in short slices and checks query.isActive
        # between them, so an ended stream is noticed within one slice. It never touches the stream itself:
        # a micro-batch of any length simply spans many slices, and batch_start / batch_end are written inline
        # from foreachBatch, not from here.
        #
        # Progress: each slice reads query.recentProgress (a short request/response; the server keeps the last
        # spark.sql.streaming.numRecentProgressUpdates reports, default 100) and records every batch newer than
        # the last one recorded (progress_batch_ids: a high-water mark, idle triggers skipped). A missed or
        # failed read is filled in by the next one, which a dropped listener event never was. For each new
        # batch: print its STREAM_PROGRESS line (and the relayed BULK_STATS line), and, when the monitoring log
        # is on, append its batch_summary. A failed read only warns (retried next slice); a failed APPEND raises
        # (the log's fail-closed contract), and the caller stops the query and fails the task.
        _POLL_SECONDS = 10
        _progress_mark = {"last": None}  # highest batch id recorded (a dict so the helpers can update it)

        def record_progress(query):
            """Record every newly executed batch in query.recentProgress (see THE WAIT LOOP above)."""
            try:
                _reports = [json.loads(p.json) for p in query.recentProgress]
            except Exception as _e:
                print(f"WARNING: {PROGRESS_TAG} could not read query progress ({type(_e).__name__}: {_e}); "
                      f"retrying on the next poll")
                return
            for _p in progress_batch_ids(_reports, _progress_mark["last"]):
                _bid = _p["batchId"]
                summarize_leftover_relays(below=_bid)  # earlier batches whose report was never seen
                print(format_progress(_p))
                _relay = read_relay(_bid) if _RELAY_ON else None
                # A missing relay here is normally a momentary read blip (with the log on, every committed batch
                # has one: a failed relay write fails the batch). Retry briefly before falling back to a
                # progress-only summary, so a blip does not cost the batch its ES diagnostics.
                for _retry in range(2):
                    if _relay is not None or not (_RELAY_ON and MONITORING_ACTIVE):
                        break
                    time.sleep(1)
                    _relay = read_relay(_bid)
                if _relay and _relay.get("line"):
                    print(_relay["line"])
                if MONITORING_ACTIVE:
                    append_monitoring_rows([batch_summary_row(CONFIG_NAME, JOB_RUN_ID, _bid,
                                                              es=(_relay or {}).get("es"), progress=_p)])
                if _RELAY_ON:
                    drop_relay(_bid)  # only once its summary is written (a failed append raised above)
                _progress_mark["last"] = _bid

        def await_stream(query, record=True):
            """Block until `query` ends (pipeline_lib.stream_wait.await_stream: _POLL_SECONDS slices plus the
            isActive liveness check), recording progress between slices when `record` is set. The no-op
            seed drain passes False: its batches send nothing to ES, so there is nothing to record. Returns
            only on a clean end; raises the query's exception when it failed, or a monitoring log failure
            (after stopping the query)."""
            _await_stream(query, _POLL_SECONDS, record=record_progress if record else None,
                          log=lambda m: print(f"{PROGRESS_TAG} {m}"))

        # Settle what a previous attempt left behind (see relay_dir above): summarize committed batches it
        # never summarized, or, with the relay off, just clear the leftovers.
        if _RELAY_ON:
            summarize_leftover_relays()  # not final: the stream's own sweeps and end-of-run pass follow
        else:
            dbutils.fs.rm(relay_dir, recurse=True)
        # The previous runner's relay directory name; nothing reads it any more, so remove it if present.
        dbutils.fs.rm(f"{checkpoint_location}/_bulk_stats_relay", recurse=True)

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
    with run_guard():
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
                await_stream(seed_query, record=False)
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
    with run_guard():
        stream_df = reader.table(SOURCE_FQN)

        # The Spark-UI query name: CONFIG_NAME (readable identifier of THIS pipeline) plus a short unique
        # per-run suffix. The suffix keeps the name UNIQUE among a session's active queries so a re-run on a
        # REUSED/interactive SparkSession cannot collide with a still-active prior query (Spark rejects a
        # duplicate active query name at .start()). The CONFIG_NAME prefix keeps the streaming tab legible.
        _QUERY_NAME = f"{CONFIG_NAME}-{uuid.uuid4().hex[:8]}"

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
        # Either way the run then waits in await_stream (the wait loop, see the setup cell), which records each
        # batch's progress and fails the task promptly if the stream fails, even if the wait call misses it.
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
            print(f"{PROGRESS_TAG} query started: name={query.name!r} id={query.id} runId={query.runId}")
            # await_stream returns ONLY if the stream stops: on a FAILURE it raises (the run fails and the Jobs
            # continuous trigger restarts it, so a lost batch never passes silently), and if the query stops
            # without an error it returns normally (run_end status stopped). A JOB CANCEL is not that: it
            # interrupts this notebook (seen live), so nothing below runs and the run keeps a run_start with no
            # run_end; an interrupted batch_summary is written by the next run's start-up sweep. There is NO
            # drain-and-stop reconciliation for an always-on run - observability is the per-batch rows written
            # as each batch commits plus the Databricks Jobs continuous-run state (RUNNING / restart count /
            # failure notifications). So set a summary noting the stop and do NOT run the availableNow summary
            # below (this branch owns its own RUN_SUMMARY).
            await_stream(query)
            # Batches that committed just before the stop may not have had their report polled yet: record
            # them, then summarize any whose report never arrived, as the availableNow branch does.
            record_progress(query)
            summarize_leftover_relays(final=True)
            RUN_SUMMARY = (
                f"streaming_trigger=continuous({STREAMING_TRIGGER_INTERVAL}) stopped; "
                f"checkpoint={checkpoint_location}"
            )
            print(f"CONTINUOUS STREAM STOPPED: {RUN_SUMMARY}")
            record_run_end("stopped", {"checkpoint": checkpoint_location,
                                       "last_batch_recorded": _progress_mark["last"]})
        else:
            # availableNow (drain-and-stop): start, drain to completion, then summarize THIS run.
            query = writer.trigger(availableNow=True).start()
            print(f"{PROGRESS_TAG} query started: name={query.name!r} id={query.id} runId={query.runId}")
            await_stream(query)

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

            _batch_ids = []
            if _metrics_dir_missing():
                # No metric files => the stream drained zero micro-batches (no new source data since the last
                # run). A valid outcome, reported as 0, not a failure.
                num_batches, rows_pushed = 0, 0
            else:
                # Dir exists: read it WITHOUT catching, so any genuine read failure propagates and fails the
                # run rather than being silently reported as 0.
                _per_batch = spark.read.json(metrics_dir).groupBy("batch_id").agg(_F.max("written").alias("written"))
                _rows = _per_batch.collect()
                _batch_ids = sorted(int(r["batch_id"]) for r in _rows)
                num_batches, rows_pushed = len(_rows), sum(int(r["written"] or 0) for r in _rows)

            # Every batch this run executed owes a batch_summary. Spark publishes a batch's progress
            # asynchronously, so the LAST batch's report can still be in flight when the query terminates (seen
            # in testing: recentProgress right after the end missed a batch that moved rows). Poll briefly for
            # the stragglers; any batch whose progress never arrives still gets its summary from the relayed ES
            # diagnostics alone, so the record is complete either way.
            if MONITORING_ACTIVE and _batch_ids:
                for _attempt in range(5):
                    if _progress_mark["last"] is not None and _progress_mark["last"] >= _batch_ids[-1]:
                        break
                    time.sleep(2)
                    record_progress(query)
                # Whatever is still unrecorded (its report never arrived) is summarized from its relayed ES
                # diagnostics alone, so every executed batch ends up with a batch_summary.
                summarize_leftover_relays(final=True)

            RUN_SUMMARY = (
                f"streaming_start={STREAMING_START} batches={num_batches} rows_pushed={rows_pushed} "
                f"checkpoint={checkpoint_location}"
            )
            if rows_pushed == 0:
                print("STREAMING EXPORT COMPLETE: 0 rows pushed (no new source data since the last run)")
            print(f"STREAMING EXPORT COMPLETE: {RUN_SUMMARY}")
            record_run_end("success", {"streaming_start": STREAMING_START, "batches": num_batches,
                                       "rows_pushed": rows_pushed, "checkpoint": checkpoint_location})

# COMMAND ----------
# Fail-closed backstop: every supported mode's cell above sets RUN_SUMMARY. If it is still None, the
# effective PIPELINE_MODE passed allow-list validation but no export cell handled it (e.g. a new mode
# added to the allow-list without a corresponding cell). Raise rather than exit on an empty summary.
with run_guard():
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

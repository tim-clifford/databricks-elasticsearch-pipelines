# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: feed status
# MAGIC
# MAGIC Refreshes the feed status table: ONE row per pipeline config ("feed") saying whether its export is
# MAGIC `CAUGHT_UP`, `IN_PROGRESS`, `PENDING`, `BEHIND` or `UNKNOWN`, with a `status_reason` code and the facts
# MAGIC behind it. Scheduled every 5 minutes by the `_feed status` job; the table is the source for a
# MAGIC caught-up/behind dashboard. All decision logic lives in `pipeline_lib/feed_status.py` (pure,
# MAGIC unit-tested); this notebook only gathers the inputs and writes the result.
# MAGIC
# MAGIC Inputs it reads (it never touches Elasticsearch or a pipeline's data):
# MAGIC - the pipeline configs (`_pipelines/pipeline_configs/`) and the generated job definitions
# MAGIC   (`resources/*.yml`, for each feed's deployed trigger);
# MAGIC - each streaming feed's checkpoint (`<checkpoint_base_path>/<config_name>/offsets` + `commits`);
# MAGIC - `DESCRIBE HISTORY ... LIMIT k` on each DISTINCT streaming source table (shared sources are read once);
# MAGIC - the monitoring log table's run_start / run_end / batch_start rows.
# MAGIC
# MAGIC Parameters (deploy-time base_parameters):
# MAGIC - `feed_status_table`: catalog.schema.table to MERGE into (created IF NOT EXISTS on first run).
# MAGIC - `monitoring_log_table`: the monitoring log table the pipelines write (required).
# MAGIC - `checkpoint_base_path`: the same base path the export jobs use for streaming checkpoints.
# MAGIC - `environment`: folded into `${environment}` in config names, as for the export jobs.
# MAGIC - `schedule_pause_status`: the target's `${var.schedule_pause_status}`, to resolve each generated job's
# MAGIC   pause state (a paused schedule is not expected to fire, so it cannot be "missed").
# MAGIC - `pipeline_mode`: the target's `${var.pipeline_mode}` global, the mode of a config that omits its own
# MAGIC   (the same `config or global` resolution as the generated jobs' pipeline_mode default).
# MAGIC - `behind_threshold_minutes` (default 60), `schedule_grace_minutes` (default 10),
# MAGIC   `log_lookback_days` (default 35), `max_workers` (default 8: live measurement showed DESCRIBE HISTORY
# MAGIC   throughput flattening at about 8 concurrent calls).
# MAGIC
# MAGIC One feed's failure to evaluate never stops the others: it is recorded as `UNKNOWN` /
# MAGIC `evaluation_error` with the exception in `detail`. A failure to read the configs, query the log table or
# MAGIC write the status table fails the run (so the job's failure notification fires).

# COMMAND ----------
import glob
import os
import sys
import time

_RUN_T0 = time.time()
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

dbutils.widgets.text("feed_status_table", "", "catalog.schema.table of the feed status table")
dbutils.widgets.text("monitoring_log_table", "", "catalog.schema.table of the monitoring log table")
dbutils.widgets.text("checkpoint_base_path", "", "Streaming checkpoint base path (as the export jobs use)")
dbutils.widgets.text("environment", "", "Environment folded into ${environment} in config names")
dbutils.widgets.text("schedule_pause_status", "", "The target's schedule_pause_status (PAUSED|UNPAUSED)")
dbutils.widgets.text("pipeline_mode", "", "The target's pipeline_mode global, for configs that omit pipeline_mode")
dbutils.widgets.text("behind_threshold_minutes", "", "Minutes after which waiting data / a running send is BEHIND")
dbutils.widgets.text("schedule_grace_minutes", "", "Minutes a scheduled run may take to start before it counts as missed")
dbutils.widgets.text("log_lookback_days", "", "Days of monitoring log read for run state")
dbutils.widgets.text("max_workers", "", "Concurrent checkpoint / history reads")

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
FILES_ROOT = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files
if FILES_ROOT not in sys.path:
    sys.path.insert(0, FILES_ROOT)

import yaml  # noqa: E402

from pipeline_lib import feed_status as fs  # noqa: E402
from pipeline_lib.checkpoint import checkpoint_location  # noqa: E402
from pipeline_lib.config import load_config, require_pause_status, require_pipeline_mode, resolve_config  # noqa: E402
from pipeline_lib.monitoring_sink import validate_table_name  # noqa: E402


def _int_widget(name, default, minimum=1):
    raw = dbutils.widgets.get(name).strip()
    if not raw:
        return default
    value = int(raw)  # a non-integer raises: fail closed
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


STATUS_TABLE = validate_table_name(dbutils.widgets.get("feed_status_table"), "feed_status_table")
LOG_TABLE = validate_table_name(dbutils.widgets.get("monitoring_log_table"), "monitoring_log_table")
CHECKPOINT_BASE_PATH = dbutils.widgets.get("checkpoint_base_path").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip()
SCHEDULE_PAUSE_STATUS = require_pause_status(dbutils.widgets.get("schedule_pause_status").strip(),
                                             "schedule_pause_status")
GLOBAL_PIPELINE_MODE = require_pipeline_mode(dbutils.widgets.get("pipeline_mode").strip(), "pipeline_mode")
THRESHOLD_MIN = _int_widget("behind_threshold_minutes", fs.DEFAULT_BEHIND_THRESHOLD_MINUTES)
GRACE_MIN = _int_widget("schedule_grace_minutes", fs.DEFAULT_SCHEDULE_GRACE_MINUTES, minimum=0)
LOOKBACK_DAYS = _int_widget("log_lookback_days", fs.DEFAULT_LOG_LOOKBACK_DAYS)
MAX_WORKERS = _int_widget("max_workers", 8)

print(f"feed_status_table={STATUS_TABLE} monitoring_log_table={LOG_TABLE}")
print(f"checkpoint_base_path={CHECKPOINT_BASE_PATH!r} environment={ENVIRONMENT!r} "
      f"schedule_pause_status={SCHEDULE_PAUSE_STATUS}")
print(f"threshold={THRESHOLD_MIN}m grace={GRACE_MIN}m lookback={LOOKBACK_DAYS}d workers={MAX_WORKERS}")

# COMMAND ----------
# Feeds and their deployed triggers. A config that fails to load fails the run (the export jobs would
# fail on it too). config_name is the file stem, exactly as gen_jobs names jobs and the runner names
# checkpoints.
CONFIG_DIR = os.path.join(FILES_ROOT, "_pipelines", "pipeline_configs")
FEEDS = {}
for path in sorted(glob.glob(os.path.join(CONFIG_DIR, "*.yml")) + glob.glob(os.path.join(CONFIG_DIR, "*.yaml"))):
    name = os.path.splitext(os.path.basename(path))[0]
    FEEDS[name] = resolve_config(load_config(path), ENVIRONMENT)

job_docs = []
for path in sorted(glob.glob(os.path.join(FILES_ROOT, "resources", "*.yml"))
                   + glob.glob(os.path.join(FILES_ROOT, "resources", "*.yaml"))):
    with open(path) as fh:
        job_docs.append(yaml.safe_load(fh))
TRIGGERS = fs.feed_triggers(job_docs, SCHEDULE_PAUSE_STATUS)

# No configs means a wrong path or an empty deployment, never "no feeds": fail before the MERGE, whose
# NOT MATCHED BY SOURCE DELETE would otherwise empty the status table while the run reports success.
if not FEEDS:
    raise RuntimeError(f"no pipeline configs found under {CONFIG_DIR}; refusing to MERGE an empty status set")

# Each feed's EFFECTIVE mode (its own pipeline_mode, else the target global). An allow-list: a mode that is
# neither batch nor streaming makes just that feed UNKNOWN, never the whole refresh fail.
MODES = {n: fs.effective_pipeline_mode(c["pipeline_mode"], GLOBAL_PIPELINE_MODE) for n, c in FEEDS.items()}
STREAMING = sorted(n for n, m in MODES.items() if m == "streaming")
BATCH = sorted(n for n, m in MODES.items() if m == "batch")
UNSUPPORTED_MODE = sorted(n for n, m in MODES.items() if m is None)
if STREAMING and not CHECKPOINT_BASE_PATH:
    raise ValueError("checkpoint_base_path is required when any config is pipeline_mode: streaming")
print(f"{len(FEEDS)} feed(s): {len(STREAMING)} streaming, {len(BATCH)} batch")

# COMMAND ----------
# Gather inputs. ORDER MATTERS for streaming: every checkpoint is read BEFORE any source history, so a
# send that finishes in between can only make a feed look more behind for one cycle, never caught up
# early (see classify_streaming). Run state is read first of all: a run that ends while this notebook is
# still evaluating then reads as still running for one cycle, which is the conservative direction.
from pyspark.sql import functions as F  # noqa: E402

t0 = time.time()
_run_df = spark.sql(fs.run_state_sql(LOG_TABLE, LOOKBACK_DAYS))
RUN_ROWS = {}
for r in _run_df.select(
        "config_name", "job_run_id", "run_end_status",
        F.expr("unix_micros(run_started_at)").alias("run_started_at"),
        F.expr("unix_micros(run_ended_at)").alias("run_ended_at"),
        F.expr("unix_micros(last_batch_started_at)").alias("last_batch_started_at")).collect():
    d = r.asDict()
    for k in ("run_started_at", "run_ended_at", "last_batch_started_at"):
        d[k] = None if d[k] is None else datetime.fromtimestamp(d[k] / 1e6, timezone.utc)
    RUN_ROWS[d["config_name"]] = d
print(f"run state: {len(RUN_ROWS)} config(s) with logged runs ({time.time() - t0:.1f}s)")


def _read_checkpoint(name):
    cp = checkpoint_location(CHECKPOINT_BASE_PATH, name)

    def ls(sub):
        p = f"{cp}/{sub}"
        return os.listdir(p) if os.path.isdir(p) else None

    def read_offset(batch_id):
        with open(f"{cp}/offsets/{batch_id}") as fh:
            return fh.read()

    def offset_mtime(batch_id):
        return datetime.fromtimestamp(os.stat(f"{cp}/offsets/{batch_id}").st_mtime, timezone.utc)

    return fs.summarize_checkpoint(ls("offsets"), ls("commits"), read_offset, offset_mtime)


def _guard(fn, *args):
    """Run fn, returning (value, None) or (None, exception) so one feed's error never stops the rest."""
    try:
        return fn(*args), None
    except Exception as e:  # noqa: BLE001 - recorded per feed as UNKNOWN evaluation_error
        return None, e


t0 = time.time()
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    CHECKPOINTS = dict(zip(STREAMING, ex.map(lambda n: _guard(_read_checkpoint, n), STREAMING)))
print(f"checkpoints: read {len(CHECKPOINTS)} ({time.time() - t0:.1f}s)")


def _source_fqn(cfg):
    s = cfg["source"]
    return f"{s['catalog']}.{s['schema']}.{s['table']}"


def _read_history(source, limit):
    rows = (spark.sql(f"DESCRIBE HISTORY {source} LIMIT {int(limit)}")
            .select("version", "operation", F.expr("unix_micros(timestamp)").alias("ts_us")).collect())
    return [{"version": int(r["version"]), "operation": r["operation"],
             "timestamp": datetime.fromtimestamp(r["ts_us"] / 1e6, timezone.utc)} for r in rows]


# COMMAND ----------
# Evaluate. Streaming: per DISTINCT source, read the newest DEFAULT_HISTORY_LIMIT commits, classify every
# feed on that source, and widen the window (doubling to the cap) only for sources with a feed that could
# not be decided. Batch: classify from the run state and the deployed trigger.
NOW = datetime.now(timezone.utc)
RESULTS = {n: fs.unknown_result("unsupported_pipeline_mode",
                                detail=f"pipeline_mode {FEEDS[n]['pipeline_mode']!r} / global {GLOBAL_PIPELINE_MODE!r}")
           for n in UNSUPPORTED_MODE}

by_source = {}
for name in STREAMING:
    ckpt, err = CHECKPOINTS[name]
    if err is not None:
        RESULTS[name] = fs.unknown_result("bad_checkpoint", detail=f"{type(err).__name__}: {err}")
    elif ckpt["state"] != "ok":
        RESULTS[name] = fs.classify_streaming(ckpt, [], fs.DEFAULT_HISTORY_LIMIT, None, NOW, THRESHOLD_MIN)
    else:
        by_source.setdefault(_source_fqn(FEEDS[name]), []).append(name)


def _evaluate_source(source):
    """Classify every feed reading `source`; returns {config_name: result}."""
    out, undecided, limit = {}, list(by_source[source]), fs.DEFAULT_HISTORY_LIMIT
    while undecided:
        history = _read_history(source, limit)
        still = []
        for name in undecided:
            res = fs.classify_streaming(CHECKPOINTS[name][0], history, limit,
                                        fs.latest_run(RUN_ROWS.get(name)), NOW, THRESHOLD_MIN)
            if res is None:
                still.append(name)
            else:
                out[name] = res
        undecided = still
        nxt = fs.next_history_limit(limit) if undecided else None
        if undecided and nxt is None:
            for name in undecided:
                out[name] = fs.unknown_result("history_window_exceeded",
                                              detail=f"{limit} newest commits do not reach the sent position")
            break
        limit = nxt or limit
    return out


t0 = time.time()
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    for source, (res, err) in zip(by_source, ex.map(lambda s: _guard(_evaluate_source, s), list(by_source))):
        if err is not None:
            reason = "source_unreadable"
            for name in by_source[source]:
                RESULTS[name] = fs.unknown_result(reason, detail=f"{type(err).__name__}: {err}")
        else:
            RESULTS.update(res)
print(f"streaming: {len(by_source)} distinct source(s) ({time.time() - t0:.1f}s)")

for name in BATCH:
    trigger = TRIGGERS.get(name)
    if trigger is None:
        RESULTS[name] = fs.unknown_result("no_job_for_config", detail="no generated job runs this config")
    elif trigger["kind"] == "unsupported":
        RESULTS[name] = fs.unknown_result("unsupported_trigger", detail=trigger.get("detail"))
    else:
        res, err = _guard(fs.classify_batch, fs.latest_run(RUN_ROWS.get(name)), trigger, NOW,
                          THRESHOLD_MIN, GRACE_MIN)
        RESULTS[name] = res if err is None else fs.unknown_result(
            "evaluation_error", detail=f"{type(err).__name__}: {err}")

# Every feed gets exactly one row; a feed that slipped through every branch is a bug, so fail closed.
missing = sorted(set(FEEDS) - set(RESULTS))
if missing:
    raise RuntimeError(f"no status computed for: {', '.join(missing)}")

# COMMAND ----------
# Write: one row per feed, MERGEd so the table always holds exactly the current feeds.
ROWS = [fs.to_row(name, MODES[name], TRIGGERS.get(name), RESULTS[name], NOW,
                  source_table=_source_fqn(FEEDS[name]))
        for name in sorted(FEEDS)]

# to_row carries timestamps as epoch microseconds; timestamp_micros() makes them TIMESTAMPs independent of
# the session time zone.
_schema = ", ".join(f"{n} {'BIGINT' if t == 'TIMESTAMP' else t}" for n, t in fs.STATUS_TABLE_COLUMNS)
_cast = [F.expr(f"timestamp_micros({n})").alias(n) if t == "TIMESTAMP" else F.col(n)
         for n, t in fs.STATUS_TABLE_COLUMNS]
_TEMP_VIEW = "_feed_status_rows"
spark.createDataFrame([tuple(r[f] for f in fs.RESULT_FIELDS) for r in ROWS], _schema) \
    .select(*_cast).createOrReplaceTempView(_TEMP_VIEW)

spark.sql(fs.create_status_table_sql(STATUS_TABLE))
spark.sql(fs.merge_status_sql(STATUS_TABLE, _TEMP_VIEW))

COUNTS = {s: sum(1 for r in ROWS if r["status"] == s) for s in fs.STATUSES}
for r in ROWS:
    print(f"{r['config_name']}: {r['status']} ({r['status_reason']})" + (f" - {r['detail']}" if r["detail"] else ""))
SUMMARY = (f"feed_status table={STATUS_TABLE} feeds={len(ROWS)} sources={len(by_source)} "
           + " ".join(f"{k}={v}" for k, v in COUNTS.items()) + f" elapsed_s={time.time() - _RUN_T0:.1f}")
print(SUMMARY)

# COMMAND ----------
dbutils.notebook.exit(SUMMARY)

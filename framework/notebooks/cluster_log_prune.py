# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: cluster logs prune
# MAGIC
# MAGIC A recurring maintenance notebook that bounds the growth of the delivered job-cluster logs (the UC Volume
# MAGIC at `cluster_log_volume_path`, used by job cluster configs that opt in with a `cluster_log_conf` block).
# MAGIC Every cluster adds a directory and nothing else removes them.
# MAGIC
# MAGIC What it does: deletes log files older than `cluster_log_retention_days`, then removes the directories
# MAGIC that leaves empty. It only touches the delivery layout, `<path>/<cluster id>/<driver|executor|eventlog|
# MAGIC init_scripts>/...`: any other file or directory under the path (a checkpoint, a wheel) is reported as
# MAGIC skipped and never deleted (see `pipeline_lib.cluster_log_prune`).
# MAGIC
# MAGIC Parameters:
# MAGIC - `cluster_log_volume_path` (deploy-time, from `${var.cluster_log_volume_path}`): the delivery path.
# MAGIC   Empty => nothing to prune (cluster log delivery is opt-in): the run succeeds without doing anything.
# MAGIC - `cluster_log_retention_days` (deploy-time, from `${var.cluster_log_retention_days}`): days to keep.
# MAGIC   A non-integer value fails closed; `0` (or negative) disables the delete.
# MAGIC - `dry_run` (job parameter, per run): `true` lists what would be deleted (counts and bytes) without
# MAGIC   deleting anything. Default `false`.
# MAGIC - `workers` (job parameter, default `${var.cluster_log_prune_workers}`): threads listing and deleting
# MAGIC   in parallel, 1 to 64.
# MAGIC - `max_minutes` (job parameter, default `${var.cluster_log_prune_max_minutes}`): the run's time budget,
# MAGIC   1 to 230. Every cluster must be triaged within it or the run fails; the delete phase stops starting
# MAGIC   clusters at the budget and the run ends green with `incomplete=True` (the next run carries on).
# MAGIC
# MAGIC How it scales: a terminated cluster whose newest driver log is inside retention is kept after two
# MAGIC listings, without walking its files; every other cluster is walked with the per-file rules, oldest first.
# MAGIC
# MAGIC Run identity needs READ VOLUME and WRITE VOLUME on the volume.

# COMMAND ----------
# Cell 1 - RESOLVE + VALIDATE. Read the parameters, make pipeline_lib importable, and fail closed on a
# malformed path, a non-integer retention, or a dry_run that is not true/false, before listing anything.
import os
import sys
import time

dbutils.widgets.text("cluster_log_volume_path", "", "UC Volume path cluster logs are delivered to (empty => nothing to prune)")
dbutils.widgets.text("cluster_log_retention_days", "30", "Days of cluster logs to keep; 0 disables the delete")
dbutils.widgets.text("dry_run", "false", "true => list what would be deleted, delete nothing")
dbutils.widgets.text("workers", "16", "Threads listing and deleting in parallel (1 to 64)")
dbutils.widgets.text("max_minutes", "200", "Time budget in minutes (1 to 230)")
dbutils.widgets.text("job_start_ms", "", "The job run's start in epoch ms (the job passes {{job.start_time.timestamp_ms}})")
CLUSTER_LOG_VOLUME_PATH = dbutils.widgets.get("cluster_log_volume_path")
CLUSTER_LOG_RETENTION_DAYS = dbutils.widgets.get("cluster_log_retention_days").strip()
DRY_RUN_RAW = dbutils.widgets.get("dry_run")
WORKERS_RAW = dbutils.widgets.get("workers")
MAX_MINUTES_RAW = dbutils.widgets.get("max_minutes")
JOB_START_MS_RAW = dbutils.widgets.get("job_start_ms")

# Resolve the synced framework root and add it to sys.path so pipeline_lib imports, the same way
# log_table_prune.py does. This notebook is synced to <bundle files>/framework/notebooks/.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
FRAMEWORK_ROOT = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files/framework
if FRAMEWORK_ROOT not in sys.path:
    sys.path.insert(0, FRAMEWORK_ROOT)

from pipeline_lib.cluster_log_prune import (  # noqa: E402
    cutoff_ms,
    parse_max_minutes,
    parse_retention_days,
    parse_workers,
    run_prune,
    spent_before_seconds,
    validate_log_root,
    volume_ls,
)
from pipeline_lib.config import require_es_flag  # noqa: E402

ROOT = validate_log_root(CLUSTER_LOG_VOLUME_PATH)
RETENTION_DAYS = parse_retention_days(CLUSTER_LOG_RETENTION_DAYS)
# require_es_flag is the shared true/false allow-list; "" (unset) means the default, false.
DRY_RUN = require_es_flag(DRY_RUN_RAW, "dry_run") == "true"
CUTOFF_MS = cutoff_ms(int(time.time() * 1000), RETENTION_DAYS)
WORKERS = parse_workers(WORKERS_RAW)
MAX_MINUTES = parse_max_minutes(MAX_MINUTES_RAW)
# The budget is the job run's, not this attempt's: an attempt retried after a failure gets only what is left.
SPENT_SECONDS, SPENT_NOTE = spent_before_seconds(JOB_START_MS_RAW, int(time.time() * 1000))

print("cluster logs prune - parameters:")
print(f"  cluster_log_volume_path = {ROOT!r}" + ("" if ROOT else " (unset: nothing to prune)"))
print(f"  cluster_log_retention_days = {RETENTION_DAYS}" + ("" if CUTOFF_MS else " (retention DISABLED: delete nothing)"))
print(f"  dry_run = {DRY_RUN}")
print(f"  workers = {WORKERS}")
print(f"  max_minutes = {MAX_MINUTES} ({SPENT_NOTE})")

# COMMAND ----------
# Cell 2 - TRIAGE + PRUNE + VERIFY. Triage every cluster directory (a terminated cluster whose newest driver
# log is inside retention is kept after two listings), then walk the rest oldest first with the per-file
# rules (allow-listed to the delivery layout) and delete their old files and the directories that leaves
# empty, deepest first, on WORKERS threads within MAX_MINUTES. A failed delete or an unlistable directory is
# collected and fails the run at the end, after every started item has been tried, so a partial prune never
# reports green; running out of budget in the delete phase ends green with incomplete=True.
# See pipeline_lib.cluster_log_prune.run_prune.
# Triage lists through the /Volumes FUSE mount (volume_ls, about 12x faster per call); the walks and deletes
# use dbutils.fs, whose non-recursive remove the race handling relies on.
SUMMARY = run_prune(ROOT, RETENTION_DAYS, CUTOFF_MS, DRY_RUN, dbutils.fs.ls, dbutils.fs.rm, workers=WORKERS,
                    max_minutes=MAX_MINUTES, spent_seconds=SPENT_SECONDS, triage_ls=volume_ls)
print(f"CLUSTER LOGS PRUNE COMPLETE: {SUMMARY}")

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output. Reached only on success.
dbutils.notebook.exit(SUMMARY)

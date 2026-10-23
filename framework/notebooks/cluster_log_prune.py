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
CLUSTER_LOG_VOLUME_PATH = dbutils.widgets.get("cluster_log_volume_path")
CLUSTER_LOG_RETENTION_DAYS = dbutils.widgets.get("cluster_log_retention_days").strip()
DRY_RUN_RAW = dbutils.widgets.get("dry_run")

# Resolve the synced framework root and add it to sys.path so pipeline_lib imports, the same way
# log_table_prune.py does. This notebook is synced to <bundle files>/framework/notebooks/.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
FRAMEWORK_ROOT = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files/framework
if FRAMEWORK_ROOT not in sys.path:
    sys.path.insert(0, FRAMEWORK_ROOT)

from pipeline_lib.cluster_log_prune import cutoff_ms, plan_prune, validate_log_root  # noqa: E402
from pipeline_lib.config import require_es_flag  # noqa: E402

ROOT = validate_log_root(CLUSTER_LOG_VOLUME_PATH)
try:
    RETENTION_DAYS = int(CLUSTER_LOG_RETENTION_DAYS)
except (TypeError, ValueError):
    raise ValueError(
        f"cluster_log_retention_days must be an integer, got {CLUSTER_LOG_RETENTION_DAYS!r} "
        f"(set the ${{var.cluster_log_retention_days}} bundle variable; 0 disables the delete)"
    )
# require_es_flag is the shared true/false allow-list; "" (unset) means the default, false.
DRY_RUN = require_es_flag(DRY_RUN_RAW, "dry_run") == "true"
CUTOFF_MS = cutoff_ms(int(time.time() * 1000), RETENTION_DAYS)

print("cluster logs prune - parameters:")
print(f"  cluster_log_volume_path = {ROOT!r}" + ("" if ROOT else " (unset: nothing to prune)"))
print(f"  cluster_log_retention_days = {RETENTION_DAYS}" + ("" if CUTOFF_MS else " (retention DISABLED: delete nothing)"))
print(f"  dry_run = {DRY_RUN}")

# COMMAND ----------
# Cell 2 - PLAN + DELETE + VERIFY. Plan from a full listing first (pure classification, allow-listed to the
# delivery layout), then delete the planned files and the directories that leaves empty, deepest first.
# A failed delete or an unlistable directory is collected and fails the run at the end, after every other
# item has been tried, so a partial prune never reports green.
SUMMARY = None
if not ROOT or CUTOFF_MS is None:
    SUMMARY = (
        f"cluster_logs_prune path={ROOT!r} retention_days={RETENTION_DAYS} "
        f"skipped_run=True reason={'path unset' if not ROOT else 'retention disabled'}"
    )
else:
    plan = plan_prune(ROOT, dbutils.fs.ls, CUTOFF_MS)
    delete_bytes = sum(size for _, size in plan.delete_files)
    print(f"  plan: delete {len(plan.delete_files)} file(s) ({delete_bytes} bytes) and "
          f"{len(plan.delete_dirs)} empty dir(s); keep {plan.kept_files} file(s) ({plan.kept_bytes} bytes)")
    for path, reason in plan.skipped:
        print(f"  SKIPPED (left untouched, {reason}): {path}")
    for path, err in plan.list_errors:
        print(f"  LIST FAILED (left untouched): {path}: {err}")

    failures = [f"list {p}: {e}" for p, e in plan.list_errors]
    deleted_files = deleted_dirs = 0
    if DRY_RUN:
        for path, size in plan.delete_files[:200]:
            print(f"  would delete {path} ({size} bytes)")
        if len(plan.delete_files) > 200:
            print(f"  ... and {len(plan.delete_files) - 200} more file(s)")
    else:
        for path, _ in plan.delete_files:
            try:
                dbutils.fs.rm(path)
                deleted_files += 1
            except Exception as exc:  # noqa: BLE001 - collected; keep going
                failures.append(f"delete {path}: {type(exc).__name__}: {exc}")
        for path in plan.delete_dirs:
            try:
                # Non-recursive: the directory was planned empty, so a new file landing in it since the
                # listing makes this fail rather than deleting a fresh log.
                dbutils.fs.rm(path, recurse=False)
                deleted_dirs += 1
            except Exception as exc:  # noqa: BLE001
                failures.append(f"remove dir {path}: {type(exc).__name__}: {exc}")
    SUMMARY = (
        f"cluster_logs_prune path={ROOT!r} retention_days={RETENTION_DAYS} dry_run={DRY_RUN} "
        f"planned_files={len(plan.delete_files)} planned_bytes={delete_bytes} planned_dirs={len(plan.delete_dirs)} "
        f"deleted_files={deleted_files} deleted_dirs={deleted_dirs} kept_files={plan.kept_files} "
        f"skipped={len(plan.skipped)} failures={len(failures)}"
    )
    if failures:
        for f in failures[:50]:
            print(f"  FAILED: {f}")
        raise RuntimeError(f"cluster_logs_prune FAILED ({len(failures)} failure(s)): {SUMMARY}")

print(f"CLUSTER LOGS PRUNE COMPLETE: {SUMMARY}")

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output. Reached only on success.
dbutils.notebook.exit(SUMMARY)

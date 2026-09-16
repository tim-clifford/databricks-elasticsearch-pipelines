# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: checkpoint clear
# MAGIC
# MAGIC A maintenance notebook that DELETES the Structured Streaming checkpoint directory for a single
# MAGIC pipeline config, so the next streaming run of that pipeline starts fresh (its `streaming_start`
# MAGIC then governs where it begins), exactly as if the pipeline were brand new.
# MAGIC
# MAGIC This is the standalone counterpart to the runner's in-line `pipeline_mode=reset_checkpoint`
# MAGIC (`notebooks/run_index_pipeline.py`): same target path, but a dedicated `_checkpoint clear` job you
# MAGIC invoke on demand with just the `config_name`, without a full pipeline run and without touching any
# MAGIC Elasticsearch connection setting.
# MAGIC
# MAGIC Parameters:
# MAGIC - `config_name` (job parameter, REQUIRED): the pipeline definition whose checkpoint to clear
# MAGIC   (`_pipelines/pipeline_configs/<config_name>.yml`). Default is blank; a blank value fails closed.
# MAGIC - `checkpoint_base_path` (deploy-time base_parameter, from the `${var.checkpoint_base_path}` bundle
# MAGIC   variable): the UC Volume base under which every stream keeps its checkpoint. The target folder is
# MAGIC   `{checkpoint_base_path}/{config_name}`, composed the IDENTICAL way the runner composes
# MAGIC   `checkpoint_location`, so this job can only ever clear the checkpoint that pipeline would resume
# MAGIC   from - never the shared base, never another pipeline's checkpoint.

# COMMAND ----------
# Cell 1 - DEBUG INFO. Read the two parameters, fail closed on anything missing, compose the target
# checkpoint path, and print everything relevant before we touch the filesystem.
#
# config_name is a per-run JOB PARAMETER (default "" in the job resource, overridable with
# `--params config_name=<name>`); checkpoint_base_path is a DEPLOY-TIME base_parameter (the bundle
# variable). Both arrive as notebook widgets. We read the effective value, strip it, and validate: a
# blank config_name or an empty checkpoint_base_path fails closed here (never a silent no-target delete),
# mirroring the runner's own required-parameter checks.
dbutils.widgets.text("config_name", "", "Pipeline definition whose checkpoint to clear (_pipelines/pipeline_configs/<config_name>.yml)")
dbutils.widgets.text("checkpoint_base_path", "", "UC Volume base for streaming checkpoints (this job appends /<config_name>)")
CONFIG_NAME = dbutils.widgets.get("config_name").strip()
CHECKPOINT_BASE_PATH = dbutils.widgets.get("checkpoint_base_path").strip()

if not CONFIG_NAME:
    raise ValueError("missing required parameter: config_name")
if not CHECKPOINT_BASE_PATH:
    # checkpoint_base_path is empty on main and set per target; a clear with no base path has no
    # location to act on, so fail closed rather than compose a meaningless path.
    raise ValueError(
        "missing required parameter: checkpoint_base_path (set the bundle variable at deploy); "
        "a checkpoint clear needs a UC Volume checkpoint base"
    )

# SINGLE SOURCE OF TRUTH for the target path: composed the IDENTICAL way the runner builds
# checkpoint_location (run_index_pipeline.py streaming branch and reset_checkpoint mode), so this job
# clears exactly the checkpoint the stream would resume from - and only that one, never the shared base.
CHECKPOINT_LOCATION = f"{CHECKPOINT_BASE_PATH.rstrip('/')}/{CONFIG_NAME}"

print("checkpoint clear - parameters:")
print(f"  config_name          = {CONFIG_NAME!r}")
print(f"  checkpoint_base_path  = {CHECKPOINT_BASE_PATH!r}")
print(f"  target checkpoint dir = {CHECKPOINT_LOCATION!r}")


def _path_exists(path: str) -> bool:
    """True if `path` exists, False ONLY when the filesystem positively reports not-found.

    Uses dbutils.fs.ls, which lists a directory or a single file and raises on a missing path.
    Fails closed on ambiguity: a missing path (any of the runtime's not-found signals) returns
    False; ANY other error (permission/403, transient IO) is re-raised so it surfaces as a real
    failure rather than being misread as "not there". This is the same not-found matching the runner
    uses for its metrics-dir probe (run_index_pipeline.py), an allow-list of not-found signals with
    everything else re-raised.
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:  # noqa: BLE001 - narrowed below; non-not-found is re-raised
        msg = str(exc)
        if "FileNotFoundException" in msg or "No such file or directory" in msg or "does not exist" in msg:
            return False
        raise

# COMMAND ----------
# Cell 2 - BEFORE listing. Show every checkpoint that currently exists under the base path, so it is
# visible in the run log which checkpoints are present (and that the target is among them) before we
# delete anything. If the base path itself does not exist yet (no stream has ever checkpointed here),
# that is not an error: report it and carry on - the target folder cannot exist either, handled next.
print(f"checkpoints under {CHECKPOINT_BASE_PATH!r} (before):")
if _path_exists(CHECKPOINT_BASE_PATH):
    _before = dbutils.fs.ls(CHECKPOINT_BASE_PATH)
    if _before:
        for _entry in sorted(_before, key=lambda e: e.name):
            print(f"  {_entry.name}")
    else:
        print("  (base path exists but is empty - no checkpoints)")
else:
    print(f"  (base path does not exist yet - nothing to list)")

# COMMAND ----------
# Cell 3 - DELETE. Remove the target checkpoint folder if it exists; if it does not, WARN (a no-op, not
# a failure). Any REAL error (e.g. a permission failure on the delete) is captured into CLEAR_ERROR and
# reported by the final cell, which then fails the run - a maintenance job that could not do its one job
# must not report green.
EXISTED_BEFORE = None   # True/False once probed; None means the probe itself errored
DELETED = False         # True only if rm actually removed the folder
CLEAR_ERROR = None      # str of any real (non-not-found) failure

try:
    EXISTED_BEFORE = _path_exists(CHECKPOINT_LOCATION)
    if EXISTED_BEFORE:
        print(f"deleting checkpoint directory {CHECKPOINT_LOCATION!r}")
        # dbutils.fs.rm(recurse=True) removes the directory and everything under it (offsets, commits,
        # sources, metrics). It returns True when it removed a path. We report that boolean verbatim.
        DELETED = bool(dbutils.fs.rm(CHECKPOINT_LOCATION, recurse=True))
        print(f"deleted (rm returned {DELETED})")
    else:
        print(f"WARNING: checkpoint directory {CHECKPOINT_LOCATION!r} does not exist - nothing to delete "
              f"(config_name={CONFIG_NAME!r}). No action taken.")
except Exception as exc:  # noqa: BLE001 - captured and re-raised in the final cell so the run still fails
    CLEAR_ERROR = f"{type(exc).__name__}: {exc}"
    print(f"ERROR while clearing checkpoint: {CLEAR_ERROR}")

# COMMAND ----------
# Cell 4 - AFTER listing. List the base path again so the run log shows the target folder is now gone
# (or, if the base path is empty/absent, that nothing remains). Also probe the exact target so the final
# cell can state definitively whether it is gone.
print(f"checkpoints under {CHECKPOINT_BASE_PATH!r} (after):")
if _path_exists(CHECKPOINT_BASE_PATH):
    _after = dbutils.fs.ls(CHECKPOINT_BASE_PATH)
    if _after:
        for _entry in sorted(_after, key=lambda e: e.name):
            print(f"  {_entry.name}")
    else:
        print("  (base path exists but is empty - no checkpoints)")
else:
    print(f"  (base path does not exist - nothing remains)")

# Ground-truth re-check of the exact target. Only meaningful when the delete did not itself error.
GONE_AFTER = None
if CLEAR_ERROR is None:
    GONE_AFTER = not _path_exists(CHECKPOINT_LOCATION)
    print(f"target {CHECKPOINT_LOCATION!r} present after: {not GONE_AFTER}")

# COMMAND ----------
# Cell 5 - RESULTS. Summarize what happened: whether the folder existed, whether it was deleted, whether
# it is verified gone, and any error. If a real error occurred, RAISE after reporting so the job run
# fails closed (a missing folder is a benign warning and succeeds; a failed delete does not).
if CLEAR_ERROR is not None:
    outcome = "ERROR"
elif EXISTED_BEFORE:
    outcome = "DELETED" if (DELETED and GONE_AFTER) else "DELETE_INCOMPLETE"
else:
    outcome = "NOT_PRESENT"

SUMMARY = (
    f"checkpoint_clear outcome={outcome} config_name={CONFIG_NAME!r} "
    f"checkpoint={CHECKPOINT_LOCATION!r} existed_before={EXISTED_BEFORE} deleted={DELETED} "
    f"gone_after={GONE_AFTER} error={CLEAR_ERROR!r}"
)
print(f"CHECKPOINT CLEAR COMPLETE: {SUMMARY}")

# DELETE_INCOMPLETE is also a failure: the folder existed and we tried, but it is not verified gone.
if CLEAR_ERROR is not None or outcome == "DELETE_INCOMPLETE":
    raise RuntimeError(SUMMARY)

# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output. It is reached only on success (DELETED or NOT_PRESENT).
dbutils.notebook.exit(SUMMARY)

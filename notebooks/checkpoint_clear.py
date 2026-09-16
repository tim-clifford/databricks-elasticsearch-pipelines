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
# MAGIC   (`_pipelines/pipeline_configs/<config_name>.yml`). Default is blank; a blank, malformed, or
# MAGIC   unknown value fails closed.
# MAGIC - `checkpoint_base_path` (deploy-time base_parameter, from the `${var.checkpoint_base_path}` bundle
# MAGIC   variable): the UC Volume base under which every stream keeps its checkpoint. The target folder is
# MAGIC   `{checkpoint_base_path}/{config_name}`, composed the IDENTICAL way the runner composes
# MAGIC   `checkpoint_location`, so this job can only ever clear the checkpoint that pipeline would resume
# MAGIC   from - never the shared base, never another pipeline's checkpoint.

# COMMAND ----------
# Cell 1 - DEBUG INFO. Read the two parameters, fail closed on anything missing or unsafe, tie config_name
# to a real pipeline definition, compose the target checkpoint path, and print everything relevant before
# we touch the filesystem.
#
# config_name is a per-run JOB PARAMETER (default "" in the job resource, overridable with
# `--params config_name=<name>`); checkpoint_base_path is a DEPLOY-TIME base_parameter (the bundle
# variable). Both arrive as notebook widgets. We read the effective value, strip it, and validate:
#   1. config_name non-empty (never a no-target run),
#   2. config_name is a bare config stem - an allow-list of [A-Za-z0-9_-] (the config-stem charset used
#      throughout pipeline_lib, e.g. the job-group/cluster-key validators). This rejects path separators
#      and `..` BEFORE the name is ever interpolated into a path we recursively delete, so a value like
#      '../other' can't escape the base and delete another pipeline's checkpoint (or a parent dir),
#   3. checkpoint_base_path non-empty,
#   4. a matching _pipelines/pipeline_configs/<config_name>.yml (or .yaml) EXISTS - resolved the same way
#      run_index_pipeline.py does before its reset_checkpoint delete. This ties the clear to a real
#      pipeline: a typo'd/unknown config_name fails closed here instead of composing a nonexistent path
#      and reporting a benign "nothing to delete" success.
import os
import re

dbutils.widgets.text("config_name", "", "Pipeline definition whose checkpoint to clear (_pipelines/pipeline_configs/<config_name>.yml)")
dbutils.widgets.text("checkpoint_base_path", "", "UC Volume base for streaming checkpoints (this job appends /<config_name>)")
CONFIG_NAME = dbutils.widgets.get("config_name").strip()
CHECKPOINT_BASE_PATH = dbutils.widgets.get("checkpoint_base_path").strip()

if not CONFIG_NAME:
    raise ValueError("missing required parameter: config_name")
# Allow-list the config-stem charset (letters, digits, underscore, hyphen). This is the SAME charset the
# generator holds every config stem to (scripts/gen_jobs.py _VALID_STEM = ^[A-Za-z0-9_-]+$; mirrored by
# pipeline_lib.config._VALID_JOB_GROUP / _VALID_JOB_CLUSTER_KEY), so a name valid here is exactly a name
# that could have produced a deployed pipeline. Fails closed on anything else - crucially any '/' or '.' -
# so the name cannot carry a path separator or `..` into the composed, recursively-deleted path.
if not re.fullmatch(r"[A-Za-z0-9_-]+", CONFIG_NAME):
    raise ValueError(
        f"invalid config_name {CONFIG_NAME!r}: must match [A-Za-z0-9_-]+ (a bare pipeline config stem, "
        f"no path separators)"
    )
if not CHECKPOINT_BASE_PATH:
    # checkpoint_base_path is empty on main and set per target; a clear with no base path has no
    # location to act on, so fail closed rather than compose a meaningless path.
    raise ValueError(
        "missing required parameter: checkpoint_base_path (set the bundle variable at deploy); "
        "a checkpoint clear needs a UC Volume checkpoint base"
    )

# Resolve the synced bundle root so we can confirm the pipeline definition exists. This notebook is synced
# to <bundle files>/notebooks/checkpoint_clear.py; the _pipelines/ tree is a sibling of notebooks/. Same
# resolution deploy_views.py and run_index_pipeline.py use.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
FILES_ROOT = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))  # .../files
CONFIG_DIR = os.path.join(FILES_ROOT, "_pipelines", "pipeline_configs")

# Accept either extension (gen_jobs.py and deploy_views.py both discover .yml AND .yaml), matching
# run_index_pipeline.py's config resolution. Fail closed if neither exists: a clear must name a real
# pipeline, so a typo raises here rather than silently "succeeding" against a path no pipeline owns.
CONFIG_PATH = next(
    (p for ext in (".yml", ".yaml") if os.path.exists(p := os.path.join(CONFIG_DIR, f"{CONFIG_NAME}{ext}"))),
    None,
)
if CONFIG_PATH is None:
    raise ValueError(f"no pipeline definition found for {CONFIG_NAME!r} (.yml/.yaml) in {CONFIG_DIR}")

# SINGLE SOURCE OF TRUTH for the target path: composed the IDENTICAL way the runner builds
# checkpoint_location (run_index_pipeline.py streaming branch and reset_checkpoint mode), so this job
# clears exactly the checkpoint the stream would resume from - and only that one, never the shared base.
CHECKPOINT_LOCATION = f"{CHECKPOINT_BASE_PATH.rstrip('/')}/{CONFIG_NAME}"

print("checkpoint clear - parameters:")
print(f"  config_name          = {CONFIG_NAME!r}")
print(f"  pipeline definition   = {CONFIG_PATH!r}")
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


def _list_checkpoints(base: str):
    """List the entries directly under `base`, returning (exists, entries).

    Collapses the existence probe and the listing into ONE dbutils.fs.ls call so there is no
    time-of-check/time-of-use window: a concurrent removal of the base path between a separate probe
    and ls would otherwise raise uncaught. Not-found (any of the runtime's signals) degrades to
    (False, []) - the benign "base path does not exist" branch; ANY other error re-raises (fail
    closed), matching _path_exists.
    """
    try:
        return True, dbutils.fs.ls(base)
    except Exception as exc:  # noqa: BLE001 - narrowed below; non-not-found is re-raised
        msg = str(exc)
        if "FileNotFoundException" in msg or "No such file or directory" in msg or "does not exist" in msg:
            return False, []
        raise


def _print_checkpoint_listing(base: str) -> None:
    """Print every checkpoint directly under `base`, or a benign notice if it is absent/empty."""
    exists, entries = _list_checkpoints(base)
    if not exists:
        print("  (base path does not exist - nothing to list)")
    elif entries:
        for entry in sorted(entries, key=lambda e: e.name):
            print(f"  {entry.name}")
    else:
        print("  (base path exists but is empty - no checkpoints)")

# COMMAND ----------
# Cell 2 - BEFORE listing. Show every checkpoint that currently exists under the base path, so it is
# visible in the run log which checkpoints are present (and that the target is among them) before we
# delete anything. If the base path itself does not exist yet (no stream has ever checkpointed here),
# that is not an error: the helper reports it and we carry on - the target folder cannot exist either,
# handled next.
print(f"checkpoints under {CHECKPOINT_BASE_PATH!r} (before):")
_print_checkpoint_listing(CHECKPOINT_BASE_PATH)

# COMMAND ----------
# Cell 3 - DELETE. Remove the target checkpoint folder if it exists; if it does not, WARN (a no-op, not
# a failure). Any REAL error (e.g. a permission failure on the delete) is captured into CLEAR_ERROR and
# reported by the final cell, which then fails the run - a maintenance job that could not do its one job
# must not report green.
EXISTED_BEFORE = None   # True/False once probed; None means the probe itself errored
RM_RESULT = None        # bool dbutils.fs.rm returned (True removed, False path already absent)
CLEAR_ERROR = None      # str of any real (non-not-found) failure

try:
    EXISTED_BEFORE = _path_exists(CHECKPOINT_LOCATION)
    if EXISTED_BEFORE:
        print(f"deleting checkpoint directory {CHECKPOINT_LOCATION!r}")
        # dbutils.fs.rm(recurse=True) removes the directory and everything under it (offsets, commits,
        # sources, metrics). It returns True when it removed a path and False when the path was already
        # absent. We report that boolean verbatim, but base success on the verified end state (cell 5),
        # not on this boolean: if another actor removes the folder between the probe and this rm, rm
        # returns False yet the goal (folder gone) is still met.
        RM_RESULT = bool(dbutils.fs.rm(CHECKPOINT_LOCATION, recurse=True))
        print(f"delete issued (rm returned {RM_RESULT})")
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
# Best-effort: this after-listing is DIAGNOSTIC only. The authoritative success signal is the guarded
# GONE_AFTER re-check below, which fails closed on a real error against the target itself. So a failure to
# re-list the base here (a transient IO / permission blip on the base dir) must neither bypass the RESULTS
# cell with a bare traceback nor turn a verified-successful clear into a false failure: warn and continue,
# and let GONE_AFTER decide the outcome. (Cell 2's before-listing is deliberately NOT wrapped: it runs
# before any delete, so a hard failure there is a clean fail-closed with nothing done.)
try:
    _print_checkpoint_listing(CHECKPOINT_BASE_PATH)
except Exception as exc:  # noqa: BLE001 - diagnostic listing only; outcome is decided by GONE_AFTER
    print(f"  WARNING: could not list base path after delete ({type(exc).__name__}: {exc}); "
          f"the clear result is decided by the target re-check below")

# Ground-truth re-check of the exact target. Only meaningful when the delete did not itself error.
# Guarded like the delete: a non-not-found error here (permission/403, transient IO) is captured into
# CLEAR_ERROR rather than propagating, so the RESULTS cell still runs, reports a SUMMARY, and raises -
# the structured fail-closed reporting is never bypassed by a bare traceback.
GONE_AFTER = None
if CLEAR_ERROR is None:
    try:
        GONE_AFTER = not _path_exists(CHECKPOINT_LOCATION)
        print(f"target {CHECKPOINT_LOCATION!r} present after: {not GONE_AFTER}")
    except Exception as exc:  # noqa: BLE001 - captured so Cell 5 still reports and raises
        CLEAR_ERROR = f"{type(exc).__name__}: {exc}"
        print(f"ERROR while re-checking target after delete: {CLEAR_ERROR}")

# COMMAND ----------
# Cell 5 - RESULTS. Summarize what happened: whether the folder existed, whether it is verified gone, and
# any error. Success is defined by the VERIFIED END STATE, not by rm's boolean: if the folder existed and
# is now gone (whether this job's rm removed it or a concurrent actor did), that is DELETED. If a real
# error occurred, or the folder existed and is still present, RAISE after reporting so the job run fails
# closed (a missing folder is a benign warning and succeeds).
if CLEAR_ERROR is not None:
    outcome = "ERROR"
elif EXISTED_BEFORE:
    # GONE_AFTER, not RM_RESULT, decides success - so a probe/rm race that reaches the goal is not a
    # false failure.
    outcome = "DELETED" if GONE_AFTER else "DELETE_INCOMPLETE"
else:
    outcome = "NOT_PRESENT"

SUMMARY = (
    f"checkpoint_clear outcome={outcome} config_name={CONFIG_NAME!r} "
    f"checkpoint={CHECKPOINT_LOCATION!r} existed_before={EXISTED_BEFORE} rm_result={RM_RESULT} "
    f"gone_after={GONE_AFTER} error={CLEAR_ERROR!r}"
)
print(f"CHECKPOINT CLEAR COMPLETE: {SUMMARY}")

# DELETE_INCOMPLETE is also a failure: the folder existed and we tried, but it is not verified gone.
if CLEAR_ERROR is not None or outcome == "DELETE_INCOMPLETE":
    raise RuntimeError(SUMMARY)

# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output. It is reached only on success (DELETED or NOT_PRESENT).
dbutils.notebook.exit(SUMMARY)

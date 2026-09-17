# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: build wheel
# MAGIC
# MAGIC A maintenance notebook that BUILDS the `databricks-es-connector` wheel from a copy of the connector
# MAGIC repo synced into the workspace and UPLOADS it to the UC Volume directory this bundle's `wheel_path`
# MAGIC points at, so the per-index jobs install exactly the wheel this job just built.
# MAGIC
# MAGIC Parameters:
# MAGIC - `repo_workspace_path` (job parameter, REQUIRED): the `/Workspace/...` path of the checked-out
# MAGIC   connector repo to build (the directory that contains its `pyproject.toml`). Default is blank; a
# MAGIC   blank or non-absolute value, or a path that does not exist / is not a buildable project root, fails
# MAGIC   closed.
# MAGIC - `wheel_version` (job parameter, REQUIRED): the connector version to build, `x.x.x`. Used to build
# MAGIC   the EXPECTED wheel filename (`databricks_es_connector-<wheel_version>-py3-none-any.whl`) and to
# MAGIC   verify the build produced it. `python -m build` reads the actual version from the repo's
# MAGIC   `pyproject.toml`, so `wheel_version` must MATCH that version - a mismatch is caught in cell 6 (the
# MAGIC   expected filename will not be present) and fails the run.
# MAGIC - `wheel_path` (deploy-time base_parameter, from the `${var.wheel_path}` bundle variable): the UC
# MAGIC   Volume path an index job installs the connector from. This job uses only its PARENT DIRECTORY as
# MAGIC   the upload destination; the filename is rebuilt from `wheel_version`. Empty fails closed.

# COMMAND ----------
# Cell 1 - INSTALL the build frontend and restart Python. `build` is the PyPA build frontend
# (`python -m build`). This cell handles ONLY the install, because restartPython() discards all Python
# interpreter state, so any work done before it would just have to be redone; every parameter is read
# AFTER the restart (cell 2). `build` is a static package name (no widget to expand), but we invoke the
# pip magic programmatically - the same mechanism run_index_pipeline.py uses - to keep this a normal
# Python cell whose last statement is restartPython() (which ends the cell).
#
# NOTE on isolation: the build in cell 5 uses `--no-isolation` (required per client-system testing), which
# means `build` does NOT create a fresh venv and instead expects the repo's build backend (hatchling) to
# already be importable in this environment. If a live run fails there with a "backend not available"
# error, the fix is to also install the backend here (e.g. `pip install build hatchling`).
get_ipython().run_line_magic("pip", "install build")
dbutils.library.restartPython()

# COMMAND ----------
# Cell 2 - PARAMETERS + validation + DEBUG. Read the three widgets, fail closed on anything missing or
# unsafe, derive the upload directory and the expected wheel filename, and print every resolved value
# before we touch the filesystem.
#
# repo_workspace_path and wheel_version are per-run JOB PARAMETERS (default "" in the job resource,
# overridable with --params); wheel_path is a DEPLOY-TIME base_parameter (the ${var.wheel_path} bundle
# variable). All arrive as notebook widgets. We validate:
#   1. repo_workspace_path non-empty and absolute (existence is checked in cell 3),
#   2. wheel_version non-empty and a bare PEP 440 version (an allow-list that admits only already-normalized
#      forms - x.x.x with an optional .devN/.postN or aN/bN/rcN suffix - so the value drops verbatim into
#      both a wheel filename and an fs path with no separators or `..` to escape the volume dir),
#   3. wheel_path non-empty (empty on main; set per target at deploy),
#   4. the derived upload directory is under /Volumes/ (a UC Volume, where index jobs read the wheel from).
import os
import re

dbutils.widgets.text("repo_workspace_path", "", "Workspace path of the connector repo to build (contains pyproject.toml)")
dbutils.widgets.text("wheel_version", "", "Connector version to build, x.x.x (must match the repo's pyproject.toml)")
dbutils.widgets.text("wheel_path", "", "Deploy-time UC Volume connector wheel path; this job uploads to its PARENT dir")
REPO_WORKSPACE_PATH = dbutils.widgets.get("repo_workspace_path").strip()
WHEEL_VERSION = dbutils.widgets.get("wheel_version").strip()
WHEEL_PATH = dbutils.widgets.get("wheel_path").strip()

if not REPO_WORKSPACE_PATH:
    raise ValueError("missing required parameter: repo_workspace_path (the /Workspace path of the connector repo to build)")
if not REPO_WORKSPACE_PATH.startswith("/"):
    raise ValueError(
        f"repo_workspace_path must be an absolute path (e.g. /Workspace/Users/<you>/es-databricks-connector): "
        f"{REPO_WORKSPACE_PATH!r}"
    )

if not WHEEL_VERSION:
    raise ValueError("missing required parameter: wheel_version (the x.x.x connector version to build)")
# Allow-list the version: a bare, already-normalized PEP 440 version. This is what appears VERBATIM in a
# wheel filename, so accepting only these forms keeps the expected filename (cell 6) an exact match, and -
# crucially - rejects anything with a '/' or '.' sequence that could carry a path separator or `..` into
# the composed upload path. Fails closed on anything else.
_WHEEL_VERSION_RE = re.compile(r"\d+\.\d+\.\d+(?:\.(?:dev|post)\d+|(?:a|b|rc)\d+)?")
if not _WHEEL_VERSION_RE.fullmatch(WHEEL_VERSION):
    raise ValueError(
        f"invalid wheel_version {WHEEL_VERSION!r}: expected a version like 'x.x.x' (optionally with a "
        f".devN/.postN or aN/bN/rcN suffix), no path separators"
    )

if not WHEEL_PATH:
    # wheel_path is empty on main and set per target; without it there is no volume directory to derive.
    raise ValueError(
        "missing required parameter: wheel_path (set the bundle variable at deploy); a build/upload needs "
        "a UC Volume wheel path to derive its destination directory from"
    )

# Upload destination = the PARENT directory of wheel_path (drop the filename). rstrip a trailing slash
# first so a path that (wrongly) ends in '/' still yields its containing dir, not itself.
VOLUME_DEST_DIR = os.path.dirname(WHEEL_PATH.rstrip("/"))
if not VOLUME_DEST_DIR.startswith("/Volumes/"):
    raise ValueError(
        f"derived upload directory {VOLUME_DEST_DIR!r} (parent of wheel_path {WHEEL_PATH!r}) is not under "
        f"/Volumes/; this job uploads the connector wheel to a UC Volume"
    )

# HARDCODED wheel filename FORMAT, matching what the connector build produces on main (src-layout, dist
# name 'databricks-es-connector' -> normalized 'databricks_es_connector', pure-Python 'py3-none-any').
# Only the version varies, from wheel_version.
WHEEL_FILENAME = f"databricks_es_connector-{WHEEL_VERSION}-py3-none-any.whl"
DEST_WHEEL_PATH = f"{VOLUME_DEST_DIR}/{WHEEL_FILENAME}"

print("build wheel - parameters:")
print(f"  repo_workspace_path = {REPO_WORKSPACE_PATH!r}")
print(f"  wheel_version       = {WHEEL_VERSION!r}")
print(f"  wheel_path (var)    = {WHEEL_PATH!r}")
print(f"  volume dest dir     = {VOLUME_DEST_DIR!r}  (parent of wheel_path)")
print(f"  wheel filename      = {WHEEL_FILENAME!r}  (hardcoded format, version from wheel_version)")
print(f"  dest wheel path     = {DEST_WHEEL_PATH!r}")

# COMMAND ----------
# Cell 3 - VALIDATE the repo path. It must exist, be a directory, and be a buildable project root (contain
# a pyproject.toml). /Workspace is FUSE-mounted on the driver, so os.path works against it directly. This
# turns a mistyped repo_workspace_path into a clear failure here, before we invoke the builder on it.
if not os.path.isdir(REPO_WORKSPACE_PATH):
    raise ValueError(f"repo_workspace_path does not exist or is not a directory: {REPO_WORKSPACE_PATH!r}")
_PYPROJECT = os.path.join(REPO_WORKSPACE_PATH, "pyproject.toml")
if not os.path.isfile(_PYPROJECT):
    raise ValueError(
        f"no pyproject.toml under repo_workspace_path {REPO_WORKSPACE_PATH!r} - not a buildable project root"
    )
print(f"repo path OK: {REPO_WORKSPACE_PATH} (found {_PYPROJECT})")

# COMMAND ----------
# Cell 4 - ENSURE the upload directory exists, creating it if not. dbutils.fs understands /Volumes paths.
# The existence probe fails closed on ambiguity: a positively-reported not-found returns False; ANY other
# error (permission/403, transient IO) re-raises rather than being misread as "not there" (the same
# not-found matching checkpoint_clear.py / run_index_pipeline.py use). After a create we re-probe to
# confirm, so a silently-failed mkdirs cannot pass as success.
def _fs_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:  # noqa: BLE001 - narrowed below; non-not-found is re-raised
        msg = str(exc)
        if "FileNotFoundException" in msg or "No such file or directory" in msg or "does not exist" in msg:
            return False
        raise


if _fs_exists(VOLUME_DEST_DIR):
    print(f"volume destination directory already exists: {VOLUME_DEST_DIR}")
else:
    print(f"volume destination directory does not exist - creating: {VOLUME_DEST_DIR}")
    dbutils.fs.mkdirs(VOLUME_DEST_DIR)
    if not _fs_exists(VOLUME_DEST_DIR):
        raise RuntimeError(f"failed to create volume destination directory {VOLUME_DEST_DIR}")
    print(f"created and verified: {VOLUME_DEST_DIR}")

# COMMAND ----------
# Cell 5 - BUILD the wheel into a fresh temp directory. We build only the wheel (--wheel) with
# --no-isolation (required per client-system testing: build uses the current environment's backend rather
# than provisioning an isolated one). Output goes to a per-run temp outdir on the driver's local disk, kept
# separate from the source tree and from the upload target so nothing stale is picked up. We capture and
# print the builder's stdout/stderr, then fail closed on a non-zero exit or a build that produced no wheel.
import subprocess
import sys
import tempfile

BUILD_OUTDIR = tempfile.mkdtemp(prefix="wheel_build_")
_cmd = [sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", BUILD_OUTDIR, REPO_WORKSPACE_PATH]
print(f"building wheel from {REPO_WORKSPACE_PATH}")
print(f"  outdir : {BUILD_OUTDIR}")
print(f"  command: {' '.join(_cmd)}")
_proc = subprocess.run(_cmd, capture_output=True, text=True)
print("--- build stdout ---")
print(_proc.stdout)
print("--- build stderr ---")
print(_proc.stderr)
if _proc.returncode != 0:
    raise RuntimeError(f"wheel build failed (exit code {_proc.returncode}); see the build output above")
_built = sorted(f for f in os.listdir(BUILD_OUTDIR) if f.endswith(".whl"))
if not _built:
    raise RuntimeError(f"build reported success (exit 0) but produced no .whl file in {BUILD_OUTDIR}")
print(f"build succeeded; produced: {_built}")

# COMMAND ----------
# Cell 6 - VERIFY the built wheel. Two checks:
#   (a) the EXPECTED wheel (databricks_es_connector-<wheel_version>-py3-none-any.whl) is present in the
#       build outdir. This is also where a wheel_version that disagrees with the repo's pyproject version
#       is caught: build names the file from pyproject, so a mismatch means the expected name is absent.
#   (b) the wheel's CONTENTS include the connector library package (databricks_es_connector/__init__.py).
#       The connector wheel is library-only by design (src-layout, packages=[src/databricks_es_connector]);
#       tests/ and integration_tests/ are top-level repo dirs excluded from the wheel, so we assert only the
#       library package is present and make NO assertion about test directories. The full entry list is
#       printed for debugging.
import zipfile

LOCAL_WHEEL_PATH = os.path.join(BUILD_OUTDIR, WHEEL_FILENAME)
if not os.path.isfile(LOCAL_WHEEL_PATH):
    raise RuntimeError(
        f"expected built wheel {WHEEL_FILENAME!r} not found in {BUILD_OUTDIR}; build produced {_built}. "
        f"The wheel_version parameter ({WHEEL_VERSION!r}) must match the version in the repo's pyproject.toml."
    )
print(f"verified built wheel present: {LOCAL_WHEEL_PATH}")

with zipfile.ZipFile(LOCAL_WHEEL_PATH) as zf:
    _names = zf.namelist()
print(f"wheel contains {len(_names)} entries:")
for _n in sorted(_names):
    print(f"  {_n}")

_LIB_PKG = "databricks_es_connector"
_lib_modules = [n for n in _names if n.startswith(_LIB_PKG + "/")]
if f"{_LIB_PKG}/__init__.py" not in _names:
    raise RuntimeError(
        f"built wheel is missing the {_LIB_PKG} library package (no {_LIB_PKG}/__init__.py); "
        f"contents: {sorted(_names)}"
    )
print(f"content check OK: library package present ({len(_lib_modules)} {_LIB_PKG}/ module file(s))")

# COMMAND ----------
# Cell 7 - UPLOAD the verified wheel to the UC Volume and confirm it landed. dbutils.fs.cp copies from the
# driver-local build outdir (file: scheme) to the volume path. We then re-list the destination directory
# and assert the file is present AND its size matches the local wheel, so a truncated or failed copy cannot
# report green.
print(f"uploading {LOCAL_WHEEL_PATH} -> {DEST_WHEEL_PATH}")
dbutils.fs.cp(f"file:{LOCAL_WHEEL_PATH}", DEST_WHEEL_PATH)

_local_size = os.path.getsize(LOCAL_WHEEL_PATH)
_dest_entries = [e for e in dbutils.fs.ls(VOLUME_DEST_DIR) if e.name == WHEEL_FILENAME]
if not _dest_entries:
    raise RuntimeError(f"upload verify failed: {WHEEL_FILENAME!r} not found under {VOLUME_DEST_DIR} after copy")
_dest_size = _dest_entries[0].size
print(f"upload complete: dest={DEST_WHEEL_PATH} local_size={_local_size} dest_size={_dest_size}")
if _dest_size != _local_size:
    raise RuntimeError(
        f"upload verify failed: size mismatch (local={_local_size}, dest={_dest_size}) for {DEST_WHEEL_PATH}"
    )
print(f"upload verified: {DEST_WHEEL_PATH} ({_dest_size} bytes)")

# COMMAND ----------
# Cell 8 - LIST the volume directory so the run log shows every file now in the upload destination
# (including the wheel just uploaded, and any prior versions still present).
print(f"contents of {VOLUME_DEST_DIR}:")
for _e in sorted(dbutils.fs.ls(VOLUME_DEST_DIR), key=lambda e: e.name):
    _kind = "dir " if _e.isDir() else "file"
    print(f"  [{_kind}] {_e.name}\t{_e.size} bytes")

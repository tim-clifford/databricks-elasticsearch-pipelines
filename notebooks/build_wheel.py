# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: build wheel
# MAGIC
# MAGIC A maintenance notebook that BUILDS the `databricks-es-connector` wheel from a copy of the connector
# MAGIC repo synced into the workspace and PUBLISHES it into the UC Volume directory this bundle's
# MAGIC `wheel_path` lives in (the parent dir), under a filename built from `wheel_version`.
# MAGIC
# MAGIC This job does NOT put the new wheel into use. Index jobs install the exact `${var.wheel_path}`
# MAGIC string (`run_index_pipeline.py` runs `%pip install <wheel_path>`), so the freshly built wheel is only
# MAGIC ADDED alongside the existing ones - `wheel_path` is not overwritten or repointed. Adopting the new
# MAGIC version is a separate, deliberate step: update `wheel_path` in `databricks.yml` to the new filename
# MAGIC and redeploy, when ready. So `wheel_version` and the version baked into `wheel_path` may legitimately
# MAGIC differ (that is the point of building a new version).
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
# MAGIC   the upload destination (the filename is rebuilt from `wheel_version`); it does not read or overwrite
# MAGIC   the file at `wheel_path` itself. Empty fails closed.

# COMMAND ----------
# Cell 1 - INSTALL the build frontend + backend, and restart Python. `build` is the PyPA build frontend
# (`python -m build`); `hatchling` is the connector's build backend (pyproject [build-system] requires it).
# We install BOTH because cell 5 builds with `--no-isolation` (required per client-system testing): build
# does NOT provision a fresh isolated env, so the backend must already be importable here - installing only
# `build` fails the build with "Backend 'hatchling.build' is not available" (confirmed on a serverless run).
# This cell handles ONLY the install, because restartPython() discards all Python interpreter state, so any
# work done before it would just have to be redone; every parameter is read AFTER the restart (cell 2).
# `build`/`hatchling` are static package names (no widget to expand), but we invoke the pip magic
# programmatically - the same mechanism run_index_pipeline.py uses - to keep this a normal Python cell whose
# last statement is restartPython() (which ends the cell).
get_ipython().run_line_magic("pip", "install build hatchling")
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
#   2. wheel_version a valid PEP 440 version, which we NORMALIZE (via packaging, the library build itself
#      uses) and use in normalized form everywhere below. build writes the normalized version into the
#      wheel filename, so normalizing here is what keeps cell 6's exact-name check aligned with what build
#      emits (a non-canonical but valid input like '01.2.3', or a combined '1.0.0rc1.dev1', is accepted and
#      normalized rather than passing here only to fail the name match later). Local-version and epoch
#      segments are rejected below, after which a normalized PEP 440 version is only digits, dots and
#      lowercase tokens, so it drops into the filename/fs path with no separators or `..` to escape the
#      volume dir,
#   3. wheel_path non-empty (empty on main; set per target at deploy),
#   4. the derived upload directory is under /Volumes/ (a UC Volume, where index jobs read the wheel from).
import os

from packaging.version import InvalidVersion, Version

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
# Parse and NORMALIZE the version with packaging (the same library build uses). `python -m build` writes the
# PEP 440-normalized version into the wheel filename, so we build the expected filename (cell 6) from the
# NORMALIZED form rather than matching the raw input: a valid-but-non-canonical input like '01.2.3', or a
# combined suffix like '1.0.0rc1.dev1', is accepted and normalized to exactly what build emits, instead of
# passing here only to fail cell 6's name match. An unparseable value fails closed (InvalidVersion).
try:
    _parsed_version = Version(WHEEL_VERSION)
except InvalidVersion as exc:
    raise ValueError(f"invalid wheel_version {WHEEL_VERSION!r}: not a PEP 440 version ({exc})")
# Reject local-version ('1.2.3+abc') and epoch ('2!1.0.0') segments: we never publish either, and each keeps
# a character ('+', '!') that str(Version) preserves but the wheel filename escapes to '_' per PEP 427 - so
# the composed name would never match what build emits, failing cell 6 late on an otherwise-good build. Once
# both are excluded, a normalized PEP 440 version is only digits, dots and lowercase tokens (a/b/rc/dev/post):
# no '/' or '..', and nothing build would escape, so it drops into the composed filename and fs path safely
# AND equals exactly what build writes.
if _parsed_version.local is not None:
    raise ValueError(f"invalid wheel_version {WHEEL_VERSION!r}: local version segments (+...) are not supported")
if _parsed_version.epoch != 0:
    raise ValueError(f"invalid wheel_version {WHEEL_VERSION!r}: epoch version segments (N!...) are not supported")
NORMALIZED_VERSION = str(_parsed_version)

if not WHEEL_PATH:
    # wheel_path is empty on main and set per target; without it there is no volume directory to derive.
    raise ValueError(
        "missing required parameter: wheel_path (set the bundle variable at deploy); a build/upload needs "
        "a UC Volume wheel path to derive its destination directory from"
    )

# wheel_path must be a full .whl FILE path: index jobs %pip install it directly, and we derive the upload
# dir as its PARENT. Reject a directory-valued wheel_path (e.g. '.../wheels' or a trailing-slash dir) - its
# os.path.dirname would strip a real path component and silently publish one level too high (the /Volumes
# check below would still pass). Requiring a .whl basename makes the parent-dir derivation correct by
# construction (and, since it can't end in '/', no rstrip is needed).
if not WHEEL_PATH.endswith(".whl") or os.path.isdir(WHEEL_PATH):
    # endswith() rejects a trailing-slash or non-wheel path; the isdir() check additionally rejects an
    # existing directory whose name happens to end in '.whl', which the suffix test alone would accept and
    # then dirname one level too high. (A not-yet-existing dest is isdir()==False and allowed.)
    raise ValueError(
        f"wheel_path {WHEEL_PATH!r} must be a full .whl file path (this job uploads to its parent directory)"
    )

# Upload destination = the PARENT directory of the wheel_path file (drop the filename).
VOLUME_DEST_DIR = os.path.dirname(WHEEL_PATH)
if not VOLUME_DEST_DIR.startswith("/Volumes/"):
    raise ValueError(
        f"derived upload directory {VOLUME_DEST_DIR!r} (parent of wheel_path {WHEEL_PATH!r}) is not under "
        f"/Volumes/; this job uploads the connector wheel to a UC Volume"
    )

# HARDCODED wheel filename FORMAT, matching what the connector build produces on main (src-layout, dist
# name 'databricks-es-connector' -> normalized 'databricks_es_connector', pure-Python 'py3-none-any').
# Only the version varies, from the NORMALIZED wheel_version (so this equals what build writes).
WHEEL_FILENAME = f"databricks_es_connector-{NORMALIZED_VERSION}-py3-none-any.whl"
DEST_WHEEL_PATH = f"{VOLUME_DEST_DIR}/{WHEEL_FILENAME}"

print("build wheel - parameters:")
print(f"  repo_workspace_path = {REPO_WORKSPACE_PATH!r}")
print(f"  wheel_version       = {WHEEL_VERSION!r}  (normalized: {NORMALIZED_VERSION!r})")
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
# Cell 4 - ENSURE the upload directory exists, creating it if not. We use the LOCAL FILE API (os) against
# the FUSE-mounted UC Volume, NOT dbutils.fs. This whole build/upload path deliberately avoids dbutils.fs:
# on serverless shared UC, dbutils.fs.cp refuses to read a driver-local file: source (it raises
# LocalFilesystemAccessDeniedException for anything outside /Workspace), which is exactly the copy we need in
# cell 7. os/shutil against /Volumes is the supported way to read/write Volume files on serverless, so we use
# it uniformly here too. After a create we re-probe to confirm, so a silently-failed mkdir cannot pass as
# success.
if os.path.isdir(VOLUME_DEST_DIR):
    print(f"volume destination directory already exists: {VOLUME_DEST_DIR}")
else:
    print(f"volume destination directory does not exist - creating: {VOLUME_DEST_DIR}")
    os.makedirs(VOLUME_DEST_DIR, exist_ok=True)
    if not os.path.isdir(VOLUME_DEST_DIR):
        raise RuntimeError(f"failed to create volume destination directory {VOLUME_DEST_DIR}")
    print(f"created and verified: {VOLUME_DEST_DIR}")

# IMMUTABLE PUBLISH: refuse to overwrite an already-published wheel of this version. The file at
# DEST_WHEEL_PATH may be the one index jobs currently install (whichever version wheel_path names), and
# silently swapping its bytes under an unchanged version string breaks the "published alongside; adoption is
# a separate step" contract. Fail closed BEFORE the build so republishing a version is a deliberate act:
# bump wheel_version, or remove the existing file first. Single-flight (max_concurrent_runs=1, queue
# disabled) means no concurrent run can create it between here and the copy in cell 7.
if os.path.exists(DEST_WHEEL_PATH):
    raise FileExistsError(
        f"target wheel already exists: {DEST_WHEEL_PATH}. Refusing to overwrite it (it may be in use by "
        f"index jobs). Build a different wheel_version, or remove the existing file first to republish."
    )
print(f"target wheel not yet present (safe to publish): {DEST_WHEEL_PATH}")

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
    # Embed the tail of stderr (falling back to stdout) in the exception itself, not just "see above": when
    # this notebook runs as a job, the cell's printed output is not returned by the run-output API, so a
    # bare "see above" would leave the failure undiagnosable without opening the run UI. Capped so a verbose
    # build log can't blow the notebook's output/error size limits.
    _tail = (_proc.stderr or _proc.stdout or "").strip()[-3000:]
    raise RuntimeError(f"wheel build failed (exit code {_proc.returncode}); build output tail:\n{_tail}")
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
# Cell 7 - UPLOAD the verified wheel to the UC Volume and confirm it landed. We use the local file API
# against the FUSE-mounted /Volumes (see cell 4 for why not dbutils.fs.cp), with a stage-then-atomic-rename
# publish so neither failure mode leaves a bad file at DEST_WHEEL_PATH:
#   - Copy into a TEMP file in the SAME directory (so the publish is a rename within one filesystem), then
#     size-verify the staged copy. A copy that raises or truncates only ever affects the temp file, which the
#     finally-block removes - never a partial/corrupt wheel at the destination that index jobs might install.
#   - Publish with os.rename, which is atomic (readers see either no file or the whole file, never a partial).
#     Guard no-overwrite: cell 4 already failed fast on a pre-existing target; we re-assert absence here right
#     before the rename. Single-flight (max_concurrent_runs=1, queue disabled) rules out a concurrent run of
#     THIS job creating it in between; an external writer to the same volume path in that sub-second window is
#     the only residual race and is out of scope.
import shutil
import tempfile

print(f"uploading {LOCAL_WHEEL_PATH} -> {DEST_WHEEL_PATH}")
_local_size = os.path.getsize(LOCAL_WHEEL_PATH)
_fd, _staged = tempfile.mkstemp(dir=VOLUME_DEST_DIR, prefix=".build_wheel.", suffix=".whl.partial")
os.close(_fd)
try:
    shutil.copyfile(LOCAL_WHEEL_PATH, _staged)
    _staged_size = os.path.getsize(_staged)
    if _staged_size != _local_size:
        raise RuntimeError(f"staged copy size mismatch (local={_local_size}, staged={_staged_size})")
    if os.path.exists(DEST_WHEEL_PATH):
        raise FileExistsError(
            f"target wheel already exists: {DEST_WHEEL_PATH}. Refusing to overwrite it (it may be in use by "
            f"index jobs). Build a different wheel_version, or remove the existing file first to republish."
        )
    os.rename(_staged, DEST_WHEEL_PATH)
    _staged = None  # published: the temp path no longer exists, nothing to clean up
finally:
    if _staged is not None and os.path.exists(_staged):
        os.remove(_staged)

if not os.path.isfile(DEST_WHEEL_PATH):
    raise RuntimeError(f"upload verify failed: {DEST_WHEEL_PATH} not present after publish")
_dest_size = os.path.getsize(DEST_WHEEL_PATH)
print(f"upload complete: dest={DEST_WHEEL_PATH} local_size={_local_size} dest_size={_dest_size}")
if _dest_size != _local_size:
    raise RuntimeError(
        f"upload verify failed: size mismatch (local={_local_size}, dest={_dest_size}) for {DEST_WHEEL_PATH}"
    )
print(f"upload verified: {DEST_WHEEL_PATH} ({_dest_size} bytes)")
# The wheel is now PUBLISHED, not yet in use. Index jobs install the exact ${var.wheel_path}; this job only
# added a file to that path's directory. Adopting the new build is a separate, deliberate step, so state
# that here to keep the run log unambiguous. We phrase it as "ensure wheel_path references this filename"
# (a no-op if it already does) rather than trying to detect a match: wheel_path's basename may encode the
# same version in a non-canonical form, so a raw string compare against our normalized name could mislead.
print(
    f"NOTE: published only - index jobs install the exact wheel_path ({WHEEL_PATH!r}). To put THIS build "
    f"into use, ensure wheel_path references {WHEEL_FILENAME!r} in databricks.yml and redeploy."
)

# COMMAND ----------
# Cell 8 - LIST the volume directory so the run log shows every file now in the upload destination
# (including the wheel just uploaded, and any prior versions still present). Local file API against the
# FUSE-mounted volume, consistent with cells 4 and 7.
print(f"contents of {VOLUME_DEST_DIR}:")
for _name in sorted(os.listdir(VOLUME_DEST_DIR)):
    _full = os.path.join(VOLUME_DEST_DIR, _name)
    if os.path.isdir(_full):
        print(f"  [dir ] {_name}")
    else:
        print(f"  [file] {_name}\t{os.path.getsize(_full)} bytes")

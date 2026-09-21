"""Streaming checkpoint offsets-state classification.

The runner notebook (notebooks/run_index_pipeline.py) uses this on a streaming_start=new run to decide
whether a checkpoint already exists (RESUME, skip seeding) or not (genuine FIRST run, safe to run the
no-op seed drain). Getting it wrong in the "first run" direction is a silent-data-loss bug: running the
seed against an existing checkpoint resumes from its committed offset and advances past an un-exported
backlog, sending nothing. So this classifier is FAIL CLOSED and decides existence ONLY from SUCCESSFUL
directory listings - it never interprets a listing EXCEPTION. Consequently an existing checkpoint can
never be misread as absent: any ancestor that lists shows the child toward the checkpoint present, which
yields "unknown" (the caller's safe fallback), not "empty".

Pure and dependency-injected (the directory-listing function is passed in) so it can be unit-tested
off-cluster; the notebook passes dbutils.fs.ls.
"""

# Classification results. The notebook imports these so its branches never re-type the string values.
HAS_OFFSET = "has_offset"  # a committed batch offset exists => Spark resumes; NO first-run seeding
EMPTY = "empty"            # offsets dir POSITIVELY absent => genuine first run => safe to seed
UNKNOWN = "unknown"        # cannot positively confirm absence => caller uses the safe startingVersion path

# How many ancestor levels to try when the offsets dir itself will not list. The UC Volume root lists
# well within this, so a genuine first run is still positively classified; if even that many levels all
# fail to list (every one a transient/permission error), we fail closed to UNKNOWN.
_DEFAULT_MAX_CLIMB = 8


def _child_name(path):
    """Basename of a path (no trailing slash), e.g. '/a/b/offsets' -> 'offsets'."""
    return path.rstrip("/").rsplit("/", 1)[-1]


def _parent(path):
    """Parent path (no trailing slash), e.g. '/a/b/offsets' -> '/a/b'."""
    return path.rstrip("/").rsplit("/", 1)[0]


def checkpoint_offsets_state(cp_location, ls, max_climb=_DEFAULT_MAX_CLIMB):
    """Classify a stream's checkpoint offsets directory as HAS_OFFSET | EMPTY | UNKNOWN.

    ls(path) returns an iterable of entries, each with a ``.name`` attribute (dbutils.fs.ls semantics: a
    directory entry's name may carry a trailing '/'), and RAISES if the path cannot be listed for ANY
    reason (not-found, transient, permission). This function never inspects the raised exception - see the
    module docstring for why.

    - HAS_OFFSET: the offsets dir lists and holds at least one integer-named batch offset file (Spark will
      RESUME from the checkpoint; startingVersion is ignored and a first-run seed must be skipped).
    - EMPTY: the offsets dir is POSITIVELY absent - the nearest ancestor that lists shows the path toward
      it is not present - so this is a genuine first run and seeding is safe.
    - UNKNOWN: existence cannot be positively confirmed - an ancestor lists but the child toward the
      checkpoint IS present while a lower level could not be read, or no ancestor up to max_climb could be
      listed at all. The caller falls back to startingVersion on the main reader, which is a no-op on a
      resume and a correct first-run seed, and never skips a backlog.
    """
    base = cp_location.rstrip("/")
    offsets_dir = base + "/offsets"

    # Fast path: the offsets dir itself lists. A resume normally lands here. Classify by contents; an
    # integer-named entry is a committed batch offset (digit-filtering ignores temp/hidden/non-batch
    # markers). dbutils.fs.ls returns a directory entry's children, so these are the offset files.
    try:
        entries = ls(offsets_dir)
    except Exception:
        entries = None
    if entries is not None:
        return HAS_OFFSET if any(e.name.rstrip("/").isdigit() for e in entries) else EMPTY

    # The offsets dir did not list. Decide POSITIVELY by walking UP: list the nearest ancestor that will
    # list, and read off whether the child on the path toward the offsets dir is present there.
    #   child present  -> something exists toward the checkpoint that we could not fully read => UNKNOWN
    #                     (never seed over it - this is the branch that protects an existing backlog).
    #   child absent   -> the whole subtree toward the checkpoint is gone => genuine first run => EMPTY.
    # We invariantly keep ancestor == parent(child): each miss climbs one level. A successful listing is
    # the ONLY thing that produces a verdict, so an existing checkpoint (any listable ancestor shows the
    # child present) can never be classified EMPTY.
    child, ancestor = offsets_dir, base
    for _ in range(max(1, max_climb)):
        try:
            listing = ls(ancestor)
        except Exception:
            listing = None
        if listing is not None:
            name = _child_name(child)
            present = any(e.name.rstrip("/") == name for e in listing)
            return UNKNOWN if present else EMPTY
        parent = _parent(ancestor)
        if parent == ancestor:  # reached a root that cannot climb further
            break
        child, ancestor = ancestor, parent
    return UNKNOWN

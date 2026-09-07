"""Ephemeral per-batch observability for streaming runs: pure formatters, no Spark.

run_index_pipeline.py registers a StreamingQueryListener that, on each micro-batch, parses the
StreamingQueryProgress JSON and calls format_progress() to emit one greppable log line; foreachBatch
calls batch_job_description() to label the batch's Spark jobs in the Jobs/Stages UI. Both helpers live
here, apart from the notebook, so they are unit-testable off-cluster (plain pytest, no Spark session).

DESIGN INVARIANT - these are OBSERVABILITY ONLY. They never change what is written, the checkpoint, or
reconciliation. format_progress in particular is FAIL-SOFT: it is driven from a listener callback whose
failure must not destabilize the export, so any missing/renamed/oddly-typed field is omitted, never
raised. It reads values exactly as the runtime emits them - notably the Delta source's backlog metrics
(numFilesOutstanding / numBytesOutstanding) arrive as STRINGS - and surfaces every metric key present,
so a metric this runtime does not emit today (e.g. numNewListedFiles) simply appears when it does.
"""
import json

# Log-line prefix. Stable and distinctive so a run's driver log can be grepped for the per-batch trail
# (e.g. `grep STREAM_PROGRESS`). Do not change it casually: it is the documented handle for the trail.
PROGRESS_TAG = "STREAM_PROGRESS"

# The backlog metrics we call out by name (the "am I caught up or behind" signal). Any OTHER metric key
# the runtime emits is still surfaced generically after these, so this is a highlight list, not a filter.
_HEADLINE_SOURCE_METRICS = ("numFilesOutstanding", "numBytesOutstanding")


def batch_job_description(config_name, index, batch_id):
    """The Spark job description set in foreachBatch before bulk_write, so each micro-batch's write
    jobs are self-labeling in the Jobs/Stages UI. Built only from values already in hand (no count() /
    no extra scan). Kept on one short line - the UI truncates long descriptions."""
    return f"{PROGRESS_TAG} {config_name} -> {index} | batch {batch_id}"


def _offset_version(offset):
    """Extract a compact 'v<reservoirVersion>/i<index>' from a Delta source offset (a JSON string like
    {"reservoirVersion":4,"index":0,...}). Returns None if absent or unparseable (fail-soft), so a
    non-Delta or future offset shape just drops the token rather than breaking the line."""
    if not offset:
        return None
    try:
        d = json.loads(offset) if isinstance(offset, str) else offset
        if not isinstance(d, dict):
            return None
        ver = d.get("reservoirVersion")
        if ver is None:
            return None
        idx = d.get("index")
        return f"v{ver}/i{idx}" if idx is not None else f"v{ver}"
    except Exception:
        return None


def _source_tokens(idx, source):
    """Tokens for one source entry: the backlog headline metrics first, then rates and offset progress,
    then any remaining metric keys the runtime emitted. Every access is guarded (fail-soft)."""
    if not isinstance(source, dict):
        return [f"src{idx}=<unparseable>"]
    tokens = [f"src{idx}"]
    metrics = source.get("metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    # Headline backlog metrics by name (values arrive as strings; emit verbatim). Only when present.
    for key in _HEADLINE_SOURCE_METRICS:
        if key in metrics:
            tokens.append(f"{key}={metrics[key]}")
    # Rates and per-source input count.
    for key in ("numInputRows", "inputRowsPerSecond", "processedRowsPerSecond"):
        val = source.get(key)
        if val is not None:
            tokens.append(f"{key}={val}")
    # Delta version progress: how far this batch reached vs the latest available (latest may be null).
    end_v = _offset_version(source.get("endOffset"))
    if end_v is not None:
        tokens.append(f"endOffset={end_v}")
    latest_v = _offset_version(source.get("latestOffset"))
    if latest_v is not None:
        tokens.append(f"latestOffset={latest_v}")
    # Any OTHER metric keys the runtime emits (e.g. numNewListedFiles, backlogEndOffset when present),
    # so a metric not in the headline list is surfaced rather than silently dropped.
    for key in sorted(metrics):
        if key not in _HEADLINE_SOURCE_METRICS:
            tokens.append(f"{key}={metrics[key]}")
    return tokens


def format_progress(progress):
    """Render one StreamingQueryProgress (parsed to a dict) as a single greppable STREAM_PROGRESS line.

    FAIL-SOFT by contract (see module docstring): returns a best-effort string for any input and never
    raises, so a listener callback built on it cannot destabilize the stream. A non-dict input yields a
    marker line rather than an exception.
    """
    if not isinstance(progress, dict):
        return f"{PROGRESS_TAG} <unparseable progress: {type(progress).__name__}>"
    tokens = [PROGRESS_TAG]
    # Query/batch identity and the top-level counters. name identifies WHICH pipeline (we set
    # queryName=config_name); batchId + numInputRows + batchDuration frame the batch.
    for key, label in (
        ("name", "name"),
        ("batchId", "batchId"),
        ("numInputRows", "numInputRows"),
        ("batchDuration", "batchDuration_ms"),
    ):
        val = progress.get(key)
        if val is not None:
            tokens.append(f"{label}={val}")
    # The full durationMs breakdown (triggerExecution/latestOffset/addBatch/...): WHERE the batch spent
    # its time. Emitted compactly as-is; keys vary by runtime, so we do not hard-code them.
    duration = progress.get("durationMs")
    if isinstance(duration, dict) and duration:
        tokens.append("durationMs=" + json.dumps(duration, separators=(",", ":"), sort_keys=True))
    # Per-source blocks (our pipelines read one source, but handle the list generically).
    sources = progress.get("sources")
    if isinstance(sources, list):
        for i, source in enumerate(sources):
            tokens.extend(_source_tokens(i, source))
    return " ".join(tokens)

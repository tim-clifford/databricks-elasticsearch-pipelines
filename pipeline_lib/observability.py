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

# Log-line prefix for the per-partition ES bulk-send diagnostics (the connector's `bulk_stats`, on by
# default; see pipeline_lib.config). Distinct from PROGRESS_TAG so the two trails grep independently
# (`grep BULK_STATS`). Stable: it is the documented handle for the bulk-send trail.
BULK_STATS_TAG = "BULK_STATS"

# The per-partition percentile keys the connector emits under each `bulk_stats` entry. Named here (not
# re-typed inline) so the formatter and its tests read the same set; a key the connector renames simply
# renders as n/a rather than raising (fail-soft).
_RTT_KEYS = ("rtt_ms_p50", "rtt_ms_p95", "rtt_ms_max")
_TOOK_KEYS = ("took_ms_p50", "took_ms_p95", "took_ms_max")

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


def _num(value):
    """Format a numeric stat compactly: an int stays an int, a float rounds to 2 decimals, None (a
    took value the connector could not record) renders as 'n/a'. Any other/oddly-typed value is
    stringified as-is (fail-soft - never raise from a formatter)."""
    if value is None:
        return "n/a"
    if isinstance(value, bool):  # bool is an int subclass; show it literally, don't format as a number
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def format_bulk_stats(bulk_stats, oneline=False):
    """Render the connector's per-partition `bulk_stats` as greppable BULK_STATS log line(s).

    `bulk_stats` is the list the connector returns under result['bulk_stats'] when EsWriteConfig
    bulk_stats is on: one dict per DataFrame partition, each with n_sends, docs_sent, bytes_sent,
    send_busy_ms, partition_wall_ms, and the rtt_ms_*/took_ms_* percentiles (see pipeline_lib.config /
    the connector). rtt_ms is the full client-observed round trip per bulk send; took_ms is
    Elasticsearch's own reported service time, so rtt - took is the network/queue overhead and
    docs_sent/n_sends the real docs-per-bulk. bytes_sent is the uncompressed NDJSON size, so
    bytes_sent/docs_sent is the real per-document size (no ES query) and bytes_sent/n_sends the
    per-request size - comparing rtt against these tells a fixed per-request latency apart from a
    transfer-bound write. send_busy_ms/partition_wall_ms is the effective in-flight concurrency the
    partition reached (~1 == sends ran serially, ~write_concurrency == fully overlapped).

    Returns a multi-line string: an `overall` line (cluster-wide rollup) followed by one line per
    partition, OR just the overall line when oneline=True (used by the streaming per-batch log to avoid
    flooding). The overall rollup reports only figures that recombine EXACTLY across partitions: total
    sends/docs/bytes, docs-per-send, bytes-per-doc/send, the wall-weighted concurrency, the
    send-weighted MEAN rtt/took, and the MAX rtt/took. It deliberately does NOT synthesize a global
    p50/p95 (percentiles cannot be exactly recombined from per-partition percentiles) - the real
    per-partition percentiles are in the per-partition lines below.

    FAIL-SOFT by contract, like format_progress: this is observability only and is called from the
    export path, so any missing/renamed/oddly-typed field is tolerated (rendered n/a) and a non-list or
    empty input yields a marker line rather than an exception."""
    try:
        if not isinstance(bulk_stats, list) or not bulk_stats:
            return f"{BULK_STATS_TAG} <no bulk stats: {type(bulk_stats).__name__}>"

        parts = [p if isinstance(p, dict) else {} for p in bulk_stats]

        def _sum(key):
            total = 0
            for p in parts:
                v = p.get(key)
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    total += v
            return total

        total_sends = _sum("n_sends")
        total_docs = _sum("docs_sent")
        total_bytes = _sum("bytes_sent")
        total_busy = _sum("send_busy_ms")
        total_wall = _sum("partition_wall_ms")
        docs_per_send = (total_docs / total_sends) if total_sends else 0.0

        def _pair_ratio(num, den):
            """num/den as a float, or None when either is non-numeric or den is falsy (renders n/a)."""
            ok = (isinstance(num, (int, float)) and not isinstance(num, bool)
                  and isinstance(den, (int, float)) and not isinstance(den, bool) and den)
            return (num / den) if ok else None

        def _rounded(value, ndigits):
            """None-safe round for _num: keep None as None so it renders n/a, else round."""
            return round(value, ndigits) if value is not None else None

        # Send-weighted mean of a per-partition mean: sum(mean_i * n_i) / sum(n_i), which is the exact
        # overall mean when every send is counted. Partitions with a non-numeric mean or zero sends are
        # skipped. Returns None when nothing weighed in (so it renders n/a).
        def _weighted_mean(mean_key):
            num = 0.0
            den = 0
            for p in parts:
                m = p.get(mean_key)
                n = p.get("n_sends")
                if (isinstance(m, (int, float)) and not isinstance(m, bool)
                        and isinstance(n, int) and not isinstance(n, bool) and n > 0):
                    num += m * n
                    den += n
            return (num / den) if den else None

        def _max(max_key):
            vals = [p.get(max_key) for p in parts]
            vals = [v for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool)]
            return max(vals) if vals else None

        overall = (
            f"{BULK_STATS_TAG} overall: partitions={len(parts)} sends={total_sends} docs={total_docs} "
            f"docs/send={_num(round(docs_per_send, 2))} "
            f"bytes/doc={_num(_rounded(_pair_ratio(total_bytes, total_docs), 1))} "
            f"bytes/send={_num(_rounded(_pair_ratio(total_bytes, total_sends), 1))} "
            f"conc(busy/wall)={_num(_rounded(_pair_ratio(total_busy, total_wall), 2))} "
            f"rtt_ms(mean={_num(_weighted_mean('rtt_ms_mean'))} max={_num(_max('rtt_ms_max'))}) "
            f"took_ms(mean={_num(_weighted_mean('took_ms_mean'))} max={_num(_max('took_ms_max'))})"
        )
        if oneline:
            return overall

        lines = [overall]
        for i, p in enumerate(parts):
            rtt = " ".join(f"{k.rsplit('_', 1)[1]}={_num(p.get(k))}" for k in _RTT_KEYS)
            took = " ".join(f"{k.rsplit('_', 1)[1]}={_num(p.get(k))}" for k in _TOOK_KEYS)
            bytes_per_doc = _pair_ratio(p.get("bytes_sent"), p.get("docs_sent"))
            conc = _pair_ratio(p.get("send_busy_ms"), p.get("partition_wall_ms"))
            lines.append(
                f"{BULK_STATS_TAG}   part{i}: sends={_num(p.get('n_sends'))} "
                f"docs={_num(p.get('docs_sent'))} bytes/doc={_num(_rounded(bytes_per_doc, 1))} "
                f"conc={_num(_rounded(conc, 2))} rtt_ms({rtt}) took_ms({took})"
            )
        return "\n".join(lines)
    except Exception as _e:  # never let a diagnostic formatter disturb the export
        return f"{BULK_STATS_TAG} <formatting failed: {type(_e).__name__}: {_e}>"


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

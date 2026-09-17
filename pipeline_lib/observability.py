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
from datetime import datetime, timezone

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
# The GIL-wait probe keys the connector emits (total stall over the partition plus its distribution).
# A high gil_wait beside a high rtt means the round trip is inflated by GIL starvation (client-side,
# under a high write_concurrency), not a real socket/ES wait. Absent on pre-gil-probe connectors -> n/a.
_GIL_KEYS = ("gil_wait_ms_total", "gil_wait_ms_p95", "gil_wait_ms_max")


def _ts_token(now=None):
    """Wall-clock `ts=<ISO-8601 UTC, ms>Z` token appended to each emitted line so the trail can be
    placed in real time and correlated with ES/cluster metrics. It matters most for the streaming
    BULK_STATS relay: `foreachBatch` BUILDS the line server-side, but the listener PRINTS it client-side
    later, so the driver log's own line timestamp is the relay time, not the batch time -- this token
    captures the batch time at the moment the line is built. `now=None` reads the current UTC time; tests
    pass a fixed datetime. Fail-soft: returns '' if the clock read fails, so a formatter never raises."""
    try:
        dt = now if now is not None else datetime.now(timezone.utc)
        return f"ts={dt.strftime('%Y-%m-%dT%H:%M:%S')}.{dt.microsecond // 1000:03d}Z"
    except Exception:
        return ""


def _with_ts(line, now=None):
    """Append the `ts=` token to a finished line (at the END, so every existing tag/keyword prefix and
    its `startswith` contract is preserved). Drops the token silently if the clock read failed."""
    ts = _ts_token(now)
    return f"{line} {ts}" if ts else line

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


def format_bulk_stats(bulk_stats, oneline=False, now=None):
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
    partition reached (~1 == sends ran serially, ~write_concurrency == fully overlapped). send_cpu_ms is
    the worker CPU burned inside es.bulk (so send_busy_ms - send_cpu_ms is off-CPU send time) and the
    gil_wait_ms_* come from the connector's GIL-acquisition probe: a large gil_wait beside a high rtt
    means the round trip is inflated by GIL starvation (client-side, under a high write_concurrency),
    not a genuine socket/ES wait. Both are n/a on connectors that predate those fields. A `ts=<UTC>`
    wall-clock token is appended to the overall line (see _ts_token: it is the batch time even when the
    relay prints the line later); `now` is injectable for tests.

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

        def _numeric(p, key):
            v = p.get(key)
            return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None

        def _all_present(*keys):
            """True only when EVERY partition carries a numeric value for EVERY key. Used to gate the
            optional cpu/gil rollups: a field only SOME partitions emit (a connector-version mix) must
            NOT be summed into a total that looks cluster-wide but understates contention, and a pair
            like gil (total, max) must be sourced together so it never renders total=n/a max=<value>."""
            return all(_numeric(p, k) is not None for p in parts for k in keys)

        def _sum_opt(key):
            """Sum `key` across partitions, or None (renders n/a) unless every partition carries it."""
            return _sum(key) if _all_present(key) else None

        total_sends = _sum("n_sends")
        total_docs = _sum("docs_sent")
        total_bytes = _sum("bytes_sent")
        total_busy = _sum("send_busy_ms")
        total_wall = _sum("partition_wall_ms")
        total_cpu = _sum_opt("send_cpu_ms")            # off-CPU send time = busy - cpu (n/a on old wheels)
        # gil total and max are sourced together (both n/a unless every partition has both), so the
        # rollup never shows an inconsistent total/max pair. (max is read below, where _max is defined.)
        gil_ok = _all_present("gil_wait_ms_total", "gil_wait_ms_max")
        total_gil = _sum("gil_wait_ms_total") if gil_ok else None
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
            f"took_ms(mean={_num(_weighted_mean('took_ms_mean'))} max={_num(_max('took_ms_max'))}) "
            f"cpu_ms(total={_num(_rounded(total_cpu, 1))}) "
            f"gil_wait_ms(total={_num(_rounded(total_gil, 1))} "
            f"max={_num(_max('gil_wait_ms_max') if gil_ok else None)})"
        )
        if oneline:
            return _with_ts(overall, now)

        lines = [_with_ts(overall, now)]
        for i, p in enumerate(parts):
            rtt = " ".join(f"{k.rsplit('_', 1)[1]}={_num(p.get(k))}" for k in _RTT_KEYS)
            took = " ".join(f"{k.rsplit('_', 1)[1]}={_num(p.get(k))}" for k in _TOOK_KEYS)
            gil = " ".join(f"{k.rsplit('_', 1)[1]}={_num(p.get(k))}" for k in _GIL_KEYS)
            bytes_per_doc = _pair_ratio(p.get("bytes_sent"), p.get("docs_sent"))
            conc = _pair_ratio(p.get("send_busy_ms"), p.get("partition_wall_ms"))
            lines.append(
                f"{BULK_STATS_TAG}   part{i}: sends={_num(p.get('n_sends'))} "
                f"docs={_num(p.get('docs_sent'))} bytes/doc={_num(_rounded(bytes_per_doc, 1))} "
                f"conc={_num(_rounded(conc, 2))} cpu_ms={_num(p.get('send_cpu_ms'))} "
                f"rtt_ms({rtt}) took_ms({took}) gil_wait_ms({gil})"
            )
        return "\n".join(lines)
    except Exception as _e:  # never let a diagnostic formatter disturb the export
        return f"{BULK_STATS_TAG} <formatting failed: {type(_e).__name__}: {_e}>"


def format_progress(progress, now=None):
    """Render one StreamingQueryProgress (parsed to a dict) as a single greppable STREAM_PROGRESS line.

    A `ts=<UTC>` wall-clock token is appended so the batch can be placed in real time. FAIL-SOFT by
    contract (see module docstring): returns a best-effort string for any input and never raises, so a
    listener callback built on it cannot destabilize the stream. A non-dict input yields a marker line
    rather than an exception. `now` is injectable for tests; None uses the current time.
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
    return _with_ts(" ".join(tokens), now)


def _ratio(num, den):
    """num/den as a float, or None when either isn't a real number or den is falsy (renders n/a).
    Module-level twin of the nested _pair_ratio in format_bulk_stats, reused by format_tail_summary."""
    ok = (isinstance(num, (int, float)) and not isinstance(num, bool)
          and isinstance(den, (int, float)) and not isinstance(den, bool) and den)
    return (num / den) if ok else None


def _median(vals):
    """Median of the numeric values in `vals` (non-numeric/bool entries ignored), or None if none are
    numeric. Even counts average the two middle values. Fail-soft: never raises on odd input."""
    nums = sorted(v for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool))
    if not nums:
        return None
    n = len(nums)
    mid = n // 2
    return nums[mid] if n % 2 else (nums[mid - 1] + nums[mid]) / 2


def _percentile(vals, q):
    """Nearest-rank percentile (q in [0, 1]) of the numeric values in `vals`, or None if none are
    numeric. Nearest-rank (not interpolated) is fine for the small partition counts here and never
    fabricates a value between observed ones. Fail-soft."""
    import math
    nums = sorted(v for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool))
    if not nums:
        return None
    idx = max(0, min(len(nums) - 1, math.ceil(q * len(nums)) - 1))
    return nums[idx]


def format_tail_summary(result, now=None):
    """Render a one-line `BULK_STATS tail:` summary from a bulk_write result dict, calling out the
    STRAGGLER and SKEW behind a wall-time tail that persists after "all tasks complete".

    `result` is what the connector's bulk_write returns; under EsWriteConfig bulk_stats it carries
    result['bulk_stats'] (one dict per partition) plus top-level collect_ms / merge_ms. A write's wall
    time is set by its SLOWEST partition, because Spark's collect returns only once the last task does,
    so a long tail after the stage view shows every task complete is almost always one partition still
    draining. This line names that partition by index with its wall clock and the figures that say WHY
    it lagged: rtt_ms_max vs took_ms_max (a large rtt with a small took is time waiting on the
    network / ES bulk queue, not ES indexing), its busy/wall concurrency, and its doc count. It then
    contrasts the slowest partition against the MEDIAN one - wall_max/median >> 1 is a straggler,
    docs_max/median >> 1 is data skew feeding one fat partition - and echoes collect_ms/merge_ms so a
    tail that is NOT inside a partition (Spark result finalization, or the driver rollup) is visible
    too: merge_ms is O(partitions) pure Python and should be milliseconds.

    A `ts=<UTC>` wall-clock token is appended (the batch time even when the relay prints it later);
    `now` is injectable for tests. FAIL-SOFT by contract, like format_bulk_stats / format_progress: this
    is observability only and is called from the export path, so any missing/renamed/oddly-typed field
    renders n/a and a non-dict or stat-less input yields a marker line rather than an exception."""
    try:
        if not isinstance(result, dict):
            return f"{BULK_STATS_TAG} tail: <no result: {type(result).__name__}>"
        counts = (f"collect_ms={_num(result.get('collect_ms'))} "
                  f"merge_ms={_num(result.get('merge_ms'))}")
        parts = result.get("bulk_stats")
        if not isinstance(parts, list) or not parts:
            return _with_ts(f"{BULK_STATS_TAG} tail: <no bulk stats> {counts}", now)
        parts = [p if isinstance(p, dict) else {} for p in parts]

        def _val(p, key):
            v = p.get(key)
            return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None

        # Slowest partition by wall clock: it sets the write's tail. Only partitions carrying a numeric
        # partition_wall_ms are eligible; if none do, the wall figures render n/a but skew/counts still
        # report.
        walls = [(_val(p, "partition_wall_ms"), i) for i, p in enumerate(parts)]
        walls = [(w, i) for (w, i) in walls if w is not None]
        if walls:
            slow_wall, slow_i = max(walls, key=lambda t: t[0])
            sp = parts[slow_i]
            conc = _ratio(_val(sp, "send_busy_ms"), _val(sp, "partition_wall_ms"))
            # The slowest partition's shape: n_sends (few big sends vs many small), rtt_ms_max vs
            # took_ms_max (a big rtt with a small took = time waiting on the network / ES bulk queue,
            # not indexing), busy/wall concurrency, and docs.
            # gil_wait_ms_total on the slowest partition is a first-class "why it lagged" signal: a large
            # value beside a large rtt_ms_max says the tail is GIL starvation (the worker could not read
            # the ES response), not the network / ES queue. n/a on connectors without the probe.
            slow = (f"slowest=part{slow_i} wall_ms={_num(slow_wall)} "
                    f"sends={_num(_val(sp, 'n_sends'))} "
                    f"rtt_ms_max={_num(_val(sp, 'rtt_ms_max'))} "
                    f"took_ms_max={_num(_val(sp, 'took_ms_max'))} "
                    f"gil_wait_ms_total={_num(_val(sp, 'gil_wait_ms_total'))} "
                    f"conc={_num(conc)} docs={_num(_val(sp, 'docs_sent'))}")
            wall_vals = [w for (w, _i) in walls]
            wall_med = _median(wall_vals)
            # How the slow tail is shaped across partitions: p95 vs median vs max says whether it is one
            # outlier (max >> p95 ~ median) or a broad slow tail (p95 >> median), and stragglers>2x counts
            # how many partitions ran past 2x the median wall (1 == a lone straggler; many == systemic).
            # Require a POSITIVE median: with a zero median (e.g. empty/near-empty partitions) `w > 2*0`
            # collapses to `w > 0` and would flag every non-empty partition, a meaningless count.
            straggler_ct = (sum(1 for w in wall_vals if w > 2 * wall_med)
                            if isinstance(wall_med, (int, float)) and wall_med > 0 else 0)
            wall_tokens = (f"median_wall_ms={_num(wall_med)} "
                           f"wall_p95={_num(_percentile(wall_vals, 0.95))} "
                           f"wall_max/median={_num(_ratio(slow_wall, wall_med))} "
                           f"stragglers>2x={straggler_ct}")
        else:
            slow = "slowest=n/a"
            wall_tokens = "median_wall_ms=n/a wall_p95=n/a wall_max/median=n/a stragglers>2x=0"

        docs = [d for d in (_val(p, "docs_sent") for p in parts) if d is not None]
        if docs:
            docs_tokens = f"docs_max/median={_num(_ratio(max(docs), _median(docs)))}"
        else:
            docs_tokens = "docs_max/median=n/a"

        return _with_ts(f"{BULK_STATS_TAG} tail: {slow} | {wall_tokens} {docs_tokens} {counts}", now)
    except Exception as _e:  # never let a diagnostic formatter disturb the export
        return f"{BULK_STATS_TAG} tail: <formatting failed: {type(_e).__name__}: {_e}>"


def bulk_stats_relay_line(result, batch_id, now=None):
    """The text to relay for one micro-batch: the compact `BULK_STATS overall` rollup (with batch_id)
    plus the `BULK_STATS tail:` summary, or None when the result carries no bulk_stats (diagnostics off,
    or an empty batch that shipped nothing). Both lines carry the SAME `ts=` batch timestamp, captured
    here when the line is built (server-side in foreachBatch) so it survives the later client-side print.

    The streaming path cannot print the connector's bulk_stats into the notebook cell directly: under
    Spark Connect `foreachBatch` runs SERVER-side (its stdout goes to the driver log, and its Python
    memory is a different process), while the StreamingQueryListener callback runs CLIENT-side (its
    stdout reaches the cell - that is why STREAM_PROGRESS shows there). An in-memory handoff cannot
    bridge the two. So `foreachBatch` writes this text to a small per-batch file (via the same Spark
    write it already uses for the per-batch row count) and the listener reads it for the batch it is
    reporting - a file is the one channel both sides share. This function is the PURE part (the content);
    the notebook owns the file I/O so this module stays Spark/Databricks-free and unit-testable.

    FAIL-SOFT: never raises (format_* are fail-soft), so a diagnostic fault cannot disturb the write."""
    try:
        if not isinstance(result, dict) or "bulk_stats" not in result:
            return None
        # One timestamp for the whole batch: capture it once so the overall and tail lines agree.
        if now is None:
            now = datetime.now(timezone.utc)
        overall = format_bulk_stats(result["bulk_stats"], oneline=True, now=now) + f" batch_id={batch_id}"
        return overall + "\n" + format_tail_summary(result, now=now)
    except Exception:  # never let a diagnostic helper disturb the export
        return None

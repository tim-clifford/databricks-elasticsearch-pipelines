"""Pure parsing, delta, and verdict logic for the _es_diagnostics maintenance notebook.

The notebook (notebooks/_es_diagnostics.py) collects read-only Elasticsearch diagnostics when a host or
index looks congested: it fetches a handful of `_cat` / `_nodes/stats` / `_cluster` endpoints TWICE a few
seconds apart, then asks this module to (a) parse each raw response into a normalized shape, (b) diff the
counter-bearing fields between the two samples (rejections, GC, completions are cumulative-since-boot, so a
rate over the window is what actually tells you anything), and (c) classify the congestion signature into a
single top-line verdict.

Why a separate pure module: the HTTP fetch is impure and lives in the notebook, but the parse/delta/verdict
logic is where the reasoning is - and a wrong verdict ("healthy" during an incident) is the failure this job
exists to prevent. So that logic is pulled out here, dependency-free, and unit-tested off-cluster
(tests/test_es_diagnostics.py) with fixture payloads.

The verdict is FAIL CLOSED (an allow-list, per the repo's classification convention): it names HEALTHY only
when the load-bearing signals were positively collected AND positively clear. A missing signal (an endpoint
that failed to respond) can never be read as healthy - it degrades the verdict to INCONCLUSIVE, so an
un-collected signal is never mistaken for an absent problem.
"""

# --------------------------------------------------------------------------- verdict codes
# The notebook imports these so its branches never re-type the string values.
VERDICT_REJECTING = "REJECTING"        # ES is actively shedding load (429s): write-pool / indexing-pressure
                                       # / circuit-breaker rejections seen. The idiomatic backpressure signal.
VERDICT_SATURATED = "SATURATED"        # write thread-pool queue is building but not yet rejecting: near capacity.
VERDICT_PRESSURED = "PRESSURED"        # indexing-pressure memory is a high fraction of its limit (rejects soon).
VERDICT_HEAP_GC = "HEAP_GC_PRESSURE"   # JVM heap high and/or heavy GC in the window: slow processing, not a queue.
VERDICT_HEALTHY = "HEALTHY"            # every load-bearing signal collected and clear.
VERDICT_INCONCLUSIVE = "INCONCLUSIVE"  # a load-bearing signal could not be collected: cannot clear the host.

# --------------------------------------------------------------------------- verdict thresholds
# Named so the tests and the notebook read off the same numbers, and so tuning is one edit.
_QUEUE_WARN_FRACTION = 0.10      # write queue >= 10% of its (bounded) queue_size => genuine build-up.
_QUEUE_WARN_DEPTH = 100          # absolute backstop for SATURATED when queue_size is unknown/unbounded.
_PRESSURE_WARN_FRACTION = 0.70   # indexing-pressure current >= 70% of its limit => PRESSURED.
_HEAP_WARN_PERCENT = 85          # any node's JVM heap >= 85% => heap pressure.
_GC_WARN_WINDOW_FRACTION = 0.30  # GC collection time in the window >= 30% of wall-clock => GC pressure.

# The rate signals HEALTHY depends on having ACTUALLY MEASURED. Each is a window delta the reducers return
# as None unless its counter endpoint was collected in BOTH samples over a real window (window_secs>0). A
# None here therefore means the rate could not be measured - single snapshot, or a partial/total endpoint
# failure in either sample - so a rejection burst cannot be ruled out and the verdict fails closed to
# INCONCLUSIVE. Keying HEALTHY on the measured RATE (not on a point-in-time collection flag) is what closes
# the partial-sample-failure fail-open: a sample-B failure yields a None delta here, never a spurious 0.
# It also subsumes endpoint collection (a delta is non-None only if the endpoint answered in both samples),
# while a present-but-limitless indexing-pressure reading still measures its rejection delta fine (finding
# 2), because the delta comes from the rejection counters, not from the limit-derived pct.
_REQUIRED_MEASURED_SIGNALS = (
    ("write_rejected_delta", "write thread-pool rejection rate over the window"),
    ("indexing_pressure_rejected_delta", "indexing-pressure rejection rate over the window"),
)


# --------------------------------------------------------------------------- small parsing helpers
def _to_int(value, default=None):
    """Best-effort int of a value that `_cat` APIs return as a string (or ES returns as a number).

    Returns `default` on anything unparseable (None, "", "-", a non-numeric string), so a missing or
    placeholder field never crashes a parse and never counts as a real number.
    """
    if value is None:
        return default
    if isinstance(value, bool):  # bool is an int subclass; a stray bool is not a count
        return default
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default


def _to_float(value, default=None):
    """Best-effort float, same contract as _to_int (used for load averages / cpu that may be fractional)."""
    if value is None:
        return default
    if isinstance(value, bool):
        return default
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return default


def _dig(mapping, *keys, default=None):
    """Walk nested dicts by `keys`, returning `default` the moment any level is missing or not a dict.

    Lets a parser read a deep ES stats path (e.g. memory.current.combined..._in_bytes) without a tower of
    `.get(...)` calls or a KeyError when a node omits a section.
    """
    cur = mapping
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


# --------------------------------------------------------------------------- per-endpoint parsers
def parse_cat_thread_pool(rows):
    """Parse `_cat/thread_pool/write?format=json` rows into a list of per-node write-pool dicts.

    Each input row is a dict with string values, e.g.
        {"node_id": "aBc", "node_name": "n1", "active": "3", "queue": "120", "queue_size": "10000",
         "rejected": "42", "completed": "99999"}
    Returns [{"node_id": str|None, "node": str, "active": int|None, "queue": int|None,
              "queue_size": int|None, "rejected": int|None, "completed": int|None}, ...]. Non-list => [].
    `node_id` is the stable, always-unique match key for the delta reducers (`node` is the display name,
    which could in principle be blank/duplicated); a row missing both is still parsed but unmatchable.
    """
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append({
            "node_id": row.get("node_id") or None,
            "node": row.get("node_name") or row.get("node") or row.get("name") or "",
            "active": _to_int(row.get("active")),
            "queue": _to_int(row.get("queue")),
            "queue_size": _to_int(row.get("queue_size")),
            "rejected": _to_int(row.get("rejected")),
            "completed": _to_int(row.get("completed")),
        })
    return out


def parse_cat_nodes(rows):
    """Parse `_cat/nodes?format=json` rows into per-node health dicts.

    Expected columns (requested via h=): name, heap.percent, cpu, load_1m, disk.used_percent, node.role.
    Returns [{"node","heap_percent","cpu","load_1m","disk_used_percent","roles"}]. Missing columns => None.
    """
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append({
            "node": row.get("name") or "",
            "heap_percent": _to_int(row.get("heap.percent")),
            "cpu": _to_int(row.get("cpu")),
            "load_1m": _to_float(row.get("load_1m")),
            "disk_used_percent": _to_float(row.get("disk.used_percent")),
            "roles": row.get("node.role") or "",
        })
    return out


def _nodes_map(nodes_stats):
    """Return the `nodes` dict of a `_nodes/stats` response, or {} if the shape is unexpected."""
    nodes = _dig(nodes_stats, "nodes", default={})
    return nodes if isinstance(nodes, dict) else {}


def parse_indexing_pressure(nodes_stats):
    """Parse `_nodes/stats/indexing_pressure` into per-node indexing-pressure dicts.

    Path per node: indexing_pressure.memory.{current.combined_coordinating_and_primary_in_bytes,
    limit_in_bytes, total.{coordinating_rejections, primary_rejections, replica_rejections}}.
    Returns {node_name: {"current_bytes","limit_bytes","pct" (current/limit or None),
    "coordinating_rejections","primary_rejections","replica_rejections","rejections_total"}}.
    """
    result = {}
    for node_id, node in _nodes_map(nodes_stats).items():
        if not isinstance(node, dict):
            continue
        name = node.get("name") or node_id
        mem = _dig(node, "indexing_pressure", "memory", default={})
        current = _to_int(_dig(mem, "current", "combined_coordinating_and_primary_in_bytes"))
        limit = _to_int(_dig(mem, "limit_in_bytes"))
        coord = _to_int(_dig(mem, "total", "coordinating_rejections"), 0)
        primary = _to_int(_dig(mem, "total", "primary_rejections"), 0)
        replica = _to_int(_dig(mem, "total", "replica_rejections"), 0)
        pct = (current / limit) if (current is not None and limit not in (None, 0)) else None
        result[name] = {
            "current_bytes": current,
            "limit_bytes": limit,
            "pct": pct,
            "coordinating_rejections": coord,
            "primary_rejections": primary,
            "replica_rejections": replica,
            "rejections_total": coord + primary + replica,
        }
    return result


def parse_jvm(nodes_stats):
    """Parse `_nodes/stats/jvm` into per-node {heap_used_percent, gc_collection_count, gc_time_ms}.

    GC is summed across young+old collectors (jvm.gc.collectors.<name>.collection_count /
    collection_time_in_millis). Both counters are cumulative-since-boot => diff them across samples.
    """
    result = {}
    for node_id, node in _nodes_map(nodes_stats).items():
        if not isinstance(node, dict):
            continue
        name = node.get("name") or node_id
        heap_pct = _to_int(_dig(node, "jvm", "mem", "heap_used_percent"))
        collectors = _dig(node, "jvm", "gc", "collectors", default={})
        gc_count = 0
        gc_time = 0
        if isinstance(collectors, dict):
            for coll in collectors.values():
                gc_count += _to_int(_dig(coll, "collection_count"), 0)
                gc_time += _to_int(_dig(coll, "collection_time_in_millis"), 0)
        result[name] = {
            "heap_used_percent": heap_pct,
            "gc_collection_count": gc_count,
            "gc_time_ms": gc_time,
        }
    return result


def parse_breakers(nodes_stats):
    """Parse `_nodes/stats/breaker` into {node: {breaker_name: {tripped, estimated_bytes, limit_bytes}}}.

    `tripped` is cumulative-since-boot => diff across samples to see trips DURING the window.
    """
    result = {}
    for node_id, node in _nodes_map(nodes_stats).items():
        if not isinstance(node, dict):
            continue
        name = node.get("name") or node_id
        breakers = _dig(node, "breakers", default={})
        node_breakers = {}
        if isinstance(breakers, dict):
            for bname, b in breakers.items():
                node_breakers[bname] = {
                    "tripped": _to_int(_dig(b, "tripped"), 0),
                    "estimated_bytes": _to_int(_dig(b, "estimated_size_in_bytes")),
                    "limit_bytes": _to_int(_dig(b, "limit_size_in_bytes")),
                }
        result[name] = node_breakers
    return result


def parse_index_stats(stats):
    """Parse a `<index>/_stats` response's `_all.total` block into the write-relevant counters.

    Returns a flat dict of the fields that matter for a congested index: in-flight and cumulative indexing,
    in-flight/total merges, refresh, flush, translog size/backlog, and segment count/memory. Counters that
    are cumulative (index_total, *_time_in_millis, merges.total, refresh.total) are diffed across samples;
    the *_current and translog/segments gauges are read as point-in-time state.
    """
    total = _dig(stats, "_all", "total", default={})
    indexing = _dig(total, "indexing", default={})
    merges = _dig(total, "merges", default={})
    refresh = _dig(total, "refresh", default={})
    flush = _dig(total, "flush", default={})
    translog = _dig(total, "translog", default={})
    segments = _dig(total, "segments", default={})
    return {
        "index_total": _to_int(indexing.get("index_total")),
        "index_time_ms": _to_int(indexing.get("index_time_in_millis")),
        "index_current": _to_int(indexing.get("index_current")),
        "index_failed": _to_int(indexing.get("index_failed")),
        "merges_current": _to_int(merges.get("current")),
        "merges_total": _to_int(merges.get("total")),
        "merges_time_ms": _to_int(merges.get("total_time_in_millis")),
        "refresh_total": _to_int(refresh.get("total")),
        "refresh_time_ms": _to_int(refresh.get("total_time_in_millis")),
        "flush_total": _to_int(flush.get("total")),
        "flush_time_ms": _to_int(flush.get("total_time_in_millis")),
        "translog_operations": _to_int(translog.get("operations")),
        "translog_size_bytes": _to_int(translog.get("size_in_bytes")),
        "translog_uncommitted_operations": _to_int(translog.get("uncommitted_operations")),
        "translog_uncommitted_size_bytes": _to_int(translog.get("uncommitted_size_in_bytes")),
        "segments_count": _to_int(segments.get("count")),
        "segments_memory_bytes": _to_int(segments.get("memory_in_bytes")),
    }


# --------------------------------------------------------------------------- reducers (across nodes)
def _tp_key(row):
    """Stable match key for a write-pool row: the always-unique node_id, falling back to the display name."""
    return row.get("node_id") or row.get("node")


def total_rejected_delta(tp_before, tp_after):
    """Sum of (after - before) write-pool `rejected` across nodes, matched by node id.

    Returns None unless BOTH samples carried usable rejected counts: an empty side means the endpoint was
    not collected in that sample, so no trustworthy window rate can be computed and the verdict must fail
    closed (this is what stops a sample-B failure from reading as a spurious 0). Otherwise an int >= 0. When
    both sides are populated, a node present in only one of them contributes 0 (it came/went, not a spike).
    Keyed on node_id so two nodes never collapse to one entry even if their display names are blank/equal.
    """
    before = {_tp_key(r): r["rejected"] for r in tp_before if r.get("rejected") is not None}
    after = {_tp_key(r): r["rejected"] for r in tp_after if r.get("rejected") is not None}
    if not before or not after:
        return None
    delta = 0
    for node, aft in after.items():
        bef = before.get(node)
        if bef is not None and aft >= bef:
            delta += aft - bef
    return delta


def max_write_queue(tp_sample):
    """Max write-pool `queue` depth across nodes in one sample, or None if no node reported a queue."""
    queues = [r["queue"] for r in tp_sample if r.get("queue") is not None]
    return max(queues) if queues else None


def max_write_queue_fill(tp_sample):
    """Max write-pool queue FILL (queue / queue_size) across nodes with a positive bounded queue_size.

    Returns a fraction in [0, 1+] or None when no node reported a usable (queue, queue_size) pair (e.g. an
    unbounded/resizable pool reports queue_size -1). Fill, not raw depth, is what says "near capacity": a
    transient depth of a few is nothing against a 10000-deep bound, so the SATURATED verdict gates on this.
    """
    fills = []
    for r in tp_sample:
        q, qs = r.get("queue"), r.get("queue_size")
        if q is not None and qs is not None and qs > 0:
            fills.append(q / qs)
    return max(fills) if fills else None


def max_indexing_pressure_pct(ip_sample):
    """Max indexing-pressure fraction-of-limit across nodes, or None if none reported a pct."""
    pcts = [v["pct"] for v in ip_sample.values() if v.get("pct") is not None]
    return max(pcts) if pcts else None


def total_indexing_pressure_rejected_delta(ip_before, ip_after):
    """Sum of (after - before) indexing-pressure rejections across nodes. None unless BOTH samples were
    collected (an empty side => rate unmeasurable => fail closed, never a spurious 0)."""
    if not ip_before or not ip_after:
        return None
    delta = 0
    for node, aft in ip_after.items():
        bef = ip_before.get(node)
        if bef is None:
            continue
        a_total = aft.get("rejections_total")
        b_total = bef.get("rejections_total")
        if a_total is not None and b_total is not None and a_total >= b_total:
            delta += a_total - b_total
    return delta


def total_breaker_tripped_delta(br_before, br_after):
    """Sum of (after - before) circuit-breaker `tripped` across nodes+breakers. None unless BOTH samples
    were collected (an empty side => rate unmeasurable => fail closed, never a spurious 0)."""
    if not br_before or not br_after:
        return None
    delta = 0
    for node, breakers in br_after.items():
        bef_node = br_before.get(node, {})
        for bname, b in breakers.items():
            aft = b.get("tripped")
            bef = _dig(bef_node, bname, "tripped")
            if aft is not None and bef is not None and aft >= bef:
                delta += aft - bef
    return delta


def max_heap_percent(jvm_sample):
    """Max JVM heap-used percent across nodes, or None if none reported it."""
    heaps = [v["heap_used_percent"] for v in jvm_sample.values() if v.get("heap_used_percent") is not None]
    return max(heaps) if heaps else None


def total_gc_time_delta_ms(jvm_before, jvm_after):
    """Sum of (after - before) GC collection time (ms) across nodes. None unless BOTH samples were
    collected (an empty side => rate unmeasurable => fail closed, never a spurious 0)."""
    if not jvm_before or not jvm_after:
        return None
    delta = 0
    for node, aft in jvm_after.items():
        bef = jvm_before.get(node)
        if bef is None:
            continue
        a_ms = aft.get("gc_time_ms")
        b_ms = bef.get("gc_time_ms")
        if a_ms is not None and b_ms is not None and a_ms >= b_ms:
            delta += a_ms - b_ms
    return delta


# --------------------------------------------------------------------------- the verdict
def classify_verdict(signals, window_secs):
    """Classify the congestion signature from reduced scalar `signals` into (code, [reason lines]).

    `signals` is a flat dict the notebook assembles from the reducers above; any key may be None meaning
    "not measured this run". Recognized keys:
        write_rejected_delta, write_queue_max, write_queue_fill_max, indexing_pressure_pct_max,
        indexing_pressure_rejected_delta, breaker_tripped_delta, heap_percent_max, gc_time_delta_ms
    `window_secs` is the wall-clock gap between the two samples (0 for a single snapshot), used only to
    scale the GC-pressure test.

    FAIL CLOSED: HEALTHY is returned ONLY when the two backpressure RATE signals (write-pool and
    indexing-pressure rejection deltas) were actually measured over the window AND no acute signature
    fired. A rejection delta is non-None only if its endpoint answered in BOTH samples over a real window,
    so a single snapshot, or a partial/total endpoint failure, forces INCONCLUSIVE - a rate we could not
    measure is never treated as "no rejections". The severity order (rejecting > saturated > pressured >
    heap/GC) reports the most acute signature first; an acute signal visible in even one sample still fires.
    """
    def sig(key):
        return signals.get(key)

    reasons = []

    # 1. REJECTING - ES is actively shedding load. The clearest, most acute signal; report it first.
    rej_reasons = []
    if (sig("write_rejected_delta") or 0) > 0:
        rej_reasons.append(f"write thread-pool rejected +{sig('write_rejected_delta')} in the window")
    if (sig("indexing_pressure_rejected_delta") or 0) > 0:
        rej_reasons.append(f"indexing-pressure rejections +{sig('indexing_pressure_rejected_delta')} in the window")
    if (sig("breaker_tripped_delta") or 0) > 0:
        rej_reasons.append(f"circuit breaker(s) tripped +{sig('breaker_tripped_delta')} in the window")
    if rej_reasons:
        rej_reasons.append("=> ES is shedding load with 429s; reduce bulk size / write concurrency and back off on 429.")
        return VERDICT_REJECTING, rej_reasons

    # 2. SATURATED - write queue building toward its bound but not yet rejecting. Gate on FILL (a fraction
    #    of the pool's queue_size), so a transient depth of a few against a 10000-deep bound is not flagged;
    #    fall back to an absolute depth only when queue_size is unknown (an unbounded/resizable pool).
    fill = sig("write_queue_fill_max")
    q = sig("write_queue_max")
    if (fill is not None and fill >= _QUEUE_WARN_FRACTION) or \
            (fill is None and q is not None and q >= _QUEUE_WARN_DEPTH):
        depth = f"depth {q}" if q is not None else "depth n/a"
        pct = f" ({fill*100:.0f}% of the queue bound)" if fill is not None else ""
        reasons.append(f"write thread-pool queue {depth}{pct} building toward its bound (not yet rejecting) "
                       f"=> near write capacity.")
        return VERDICT_SATURATED, reasons

    # 3. PRESSURED - indexing-pressure memory a high fraction of its limit (will reject soon).
    ip_pct = sig("indexing_pressure_pct_max")
    if ip_pct is not None and ip_pct >= _PRESSURE_WARN_FRACTION:
        reasons.append(
            f"indexing-pressure memory at {ip_pct*100:.0f}% of the limit "
            f"(>= {_PRESSURE_WARN_FRACTION*100:.0f}%) => approaching rejection."
        )
        return VERDICT_PRESSURED, reasons

    # 4. HEAP_GC_PRESSURE - slow processing rather than a full queue: high heap and/or heavy GC in the window.
    heap = sig("heap_percent_max")
    gc_ms = sig("gc_time_delta_ms")
    heap_hot = heap is not None and heap >= _HEAP_WARN_PERCENT
    gc_hot = (
        gc_ms is not None and window_secs and window_secs > 0
        and gc_ms >= _GC_WARN_WINDOW_FRACTION * window_secs * 1000.0
    )
    if heap_hot or gc_hot:
        if heap_hot:
            reasons.append(f"JVM heap at {heap}% (>= {_HEAP_WARN_PERCENT}%).")
        if gc_hot:
            reasons.append(
                f"GC ran {gc_ms} ms of the {window_secs:g}s window "
                f"(>= {_GC_WARN_WINDOW_FRACTION*100:.0f}%) => slow processing, not a full queue."
            )
        return VERDICT_HEAP_GC, reasons

    # 5. Nothing acute observed. Only call HEALTHY if BOTH backpressure rejection RATES were measured over
    #    the window; a None means single-snapshot or a partial/total endpoint failure, so a rejection burst
    #    can't be ruled out - fail closed to INCONCLUSIVE naming the rate(s) we could not measure.
    missing = [label for key, label in _REQUIRED_MEASURED_SIGNALS if sig(key) is None]
    if missing:
        reasons.append(
            "no acute congestion in the signals we DID read, but these backpressure rates could not be "
            f"measured: {'; '.join(missing)} => cannot clear the host as healthy (a single snapshot cannot "
            f"measure a rate; otherwise check the endpoint errors above)."
        )
        return VERDICT_INCONCLUSIVE, reasons

    reasons.append("write queue not building, no rejections over the window, indexing pressure and heap/GC "
                   "within limits.")
    return VERDICT_HEALTHY, reasons


# --------------------------------------------------------------------------- formatting helpers
def human_bytes(n):
    """Human-readable bytes for report lines. None/negative-safe: None -> 'n/a'."""
    if n is None:
        return "n/a"
    try:
        n = float(n)
    except (ValueError, TypeError):
        return "n/a"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024.0

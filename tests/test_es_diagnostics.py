"""Offline unit tests for pipeline_lib.es_diagnostics. No Spark, no cluster, no ES: plain pytest with
fixture payloads shaped like real `_cat` / `_nodes/stats` / `<index>/_stats` responses.

The load-bearing contract is the verdict: it must (1) report the most acute congestion signature first,
(2) diff cumulative counters over the sampling window rather than reading lifetime totals, and (3) FAIL
CLOSED - never return HEALTHY when a load-bearing signal (write rejections, queue depth, indexing pressure)
was not collected. The `*_missing_signal_*` and delta tests are the regressions for that contract.
"""
import pytest

from pipeline_lib.es_diagnostics import (
    VERDICT_HEALTHY,
    VERDICT_HEAP_GC,
    VERDICT_INCONCLUSIVE,
    VERDICT_PRESSURED,
    VERDICT_REJECTING,
    VERDICT_SATURATED,
    classify_verdict,
    human_bytes,
    max_heap_percent,
    max_indexing_pressure_pct,
    max_write_queue,
    max_write_queue_fill,
    parse_breakers,
    parse_cat_nodes,
    parse_cat_thread_pool,
    parse_index_stats,
    parse_indexing_pressure,
    parse_jvm,
    total_breaker_tripped_delta,
    total_gc_time_delta_ms,
    total_indexing_pressure_rejected_delta,
    total_rejected_delta,
)


# ============================================================ parse_cat_thread_pool
def test_parse_cat_thread_pool_string_values():
    rows = [{"node_name": "n1", "active": "3", "queue": "120", "queue_size": "10000",
             "rejected": "42", "completed": "99999"}]
    parsed = parse_cat_thread_pool(rows)
    assert parsed == [{"node": "n1", "active": 3, "queue": 120, "queue_size": 10000,
                       "rejected": 42, "completed": 99999}]


def test_parse_cat_thread_pool_missing_fields_are_none():
    parsed = parse_cat_thread_pool([{"node_name": "n1"}])
    assert parsed[0]["queue"] is None and parsed[0]["rejected"] is None


def test_parse_cat_thread_pool_non_list_is_empty():
    assert parse_cat_thread_pool({"not": "a list"}) == []
    assert parse_cat_thread_pool(None) == []


# ============================================================ parse_cat_nodes
def test_parse_cat_nodes():
    rows = [{"name": "n1", "heap.percent": "72", "cpu": "40", "load_1m": "3.5",
             "disk.used_percent": "55.2", "node.role": "dim"}]
    parsed = parse_cat_nodes(rows)
    assert parsed[0] == {"node": "n1", "heap_percent": 72, "cpu": 40, "load_1m": 3.5,
                         "disk_used_percent": 55.2, "roles": "dim"}


# ============================================================ parse_indexing_pressure
def _ip_node(name, current, limit, coord=0, primary=0, replica=0):
    return {
        "name": name,
        "indexing_pressure": {"memory": {
            "current": {"combined_coordinating_and_primary_in_bytes": current},
            "limit_in_bytes": limit,
            "total": {"coordinating_rejections": coord, "primary_rejections": primary,
                      "replica_rejections": replica},
        }},
    }


def test_parse_indexing_pressure_pct_and_rejections():
    stats = {"nodes": {"abc": _ip_node("n1", current=200, limit=1000, coord=1, primary=2, replica=3)}}
    parsed = parse_indexing_pressure(stats)
    assert parsed["n1"]["pct"] == pytest.approx(0.2)
    assert parsed["n1"]["rejections_total"] == 6


def test_parse_indexing_pressure_zero_limit_yields_none_pct():
    # A zero (or missing) limit must not divide-by-zero; pct is None, not a crash or a bogus number.
    stats = {"nodes": {"abc": _ip_node("n1", current=200, limit=0)}}
    assert parse_indexing_pressure(stats)["n1"]["pct"] is None


def test_parse_indexing_pressure_empty_nodes():
    assert parse_indexing_pressure({"nodes": {}}) == {}
    assert parse_indexing_pressure({}) == {}


# ============================================================ parse_jvm
def test_parse_jvm_sums_collectors():
    stats = {"nodes": {"abc": {"name": "n1", "jvm": {
        "mem": {"heap_used_percent": 88},
        "gc": {"collectors": {
            "young": {"collection_count": 100, "collection_time_in_millis": 2000},
            "old": {"collection_count": 5, "collection_time_in_millis": 500},
        }},
    }}}}
    parsed = parse_jvm(stats)
    assert parsed["n1"] == {"heap_used_percent": 88, "gc_collection_count": 105, "gc_time_ms": 2500}


# ============================================================ parse_breakers
def test_parse_breakers():
    stats = {"nodes": {"abc": {"name": "n1", "breakers": {
        "parent": {"tripped": 7, "estimated_size_in_bytes": 500, "limit_size_in_bytes": 1000},
    }}}}
    parsed = parse_breakers(stats)
    assert parsed["n1"]["parent"]["tripped"] == 7


# ============================================================ parse_index_stats
def test_parse_index_stats_reads_all_total():
    stats = {"_all": {"total": {
        "indexing": {"index_total": 1000, "index_time_in_millis": 5000, "index_current": 3, "index_failed": 1},
        "merges": {"current": 2, "total": 40, "total_time_in_millis": 8000},
        "translog": {"operations": 500, "size_in_bytes": 123456, "uncommitted_operations": 10,
                     "uncommitted_size_in_bytes": 2048},
        "segments": {"count": 55, "memory_in_bytes": 999},
    }}}
    parsed = parse_index_stats(stats)
    assert parsed["index_total"] == 1000
    assert parsed["merges_current"] == 2
    assert parsed["translog_operations"] == 500
    assert parsed["segments_count"] == 55
    # A section ES omitted (refresh/flush) comes back as None, not a crash.
    assert parsed["refresh_total"] is None


# ============================================================ reducers: deltas over the window
def test_total_rejected_delta_counts_only_the_window_increment():
    # THE cumulative-counter regression: rejected is since-boot; the delta must be after-before (3), not
    # the lifetime total (103).
    before = parse_cat_thread_pool([{"node_name": "n1", "rejected": "100"}])
    after = parse_cat_thread_pool([{"node_name": "n1", "rejected": "103"}])
    assert total_rejected_delta(before, after) == 3


def test_total_rejected_delta_none_when_not_collected():
    # Neither sample carried a rejected count => signal absent => None (not 0), so the verdict can fail closed.
    before = parse_cat_thread_pool([{"node_name": "n1"}])
    after = parse_cat_thread_pool([{"node_name": "n1"}])
    assert total_rejected_delta(before, after) is None


def test_total_rejected_delta_counter_reset_contributes_zero():
    # If a node restarted mid-window (after < before), don't report a negative/huge spike; contribute 0.
    before = parse_cat_thread_pool([{"node_name": "n1", "rejected": "100"}])
    after = parse_cat_thread_pool([{"node_name": "n1", "rejected": "2"}])
    assert total_rejected_delta(before, after) == 0


def test_total_rejected_delta_node_only_in_one_sample_contributes_zero():
    before = parse_cat_thread_pool([{"node_name": "n1", "rejected": "100"}])
    after = parse_cat_thread_pool([{"node_name": "n2", "rejected": "5"}])
    assert total_rejected_delta(before, after) == 0


def test_max_write_queue():
    sample = parse_cat_thread_pool([{"node_name": "n1", "queue": "10"},
                                    {"node_name": "n2", "queue": "250"}])
    assert max_write_queue(sample) == 250


def test_max_write_queue_none_when_absent():
    assert max_write_queue(parse_cat_thread_pool([{"node_name": "n1"}])) is None


def test_max_write_queue_fill_is_fraction_of_queue_size():
    sample = parse_cat_thread_pool([{"node_name": "n1", "queue": "1000", "queue_size": "10000"},
                                    {"node_name": "n2", "queue": "50", "queue_size": "10000"}])
    assert max_write_queue_fill(sample) == pytest.approx(0.1)


def test_max_write_queue_fill_none_without_bounded_queue_size():
    # An unbounded/resizable pool reports queue_size -1 (or omits it); no usable fill => None.
    sample = parse_cat_thread_pool([{"node_name": "n1", "queue": "5", "queue_size": "-1"}])
    assert max_write_queue_fill(sample) is None


def test_max_indexing_pressure_pct():
    stats = {"nodes": {"a": _ip_node("n1", 700, 1000), "b": _ip_node("n2", 100, 1000)}}
    assert max_indexing_pressure_pct(parse_indexing_pressure(stats)) == pytest.approx(0.7)


def test_total_indexing_pressure_rejected_delta():
    before = parse_indexing_pressure({"nodes": {"a": _ip_node("n1", 0, 1000, coord=5)}})
    after = parse_indexing_pressure({"nodes": {"a": _ip_node("n1", 0, 1000, coord=9, primary=1)}})
    assert total_indexing_pressure_rejected_delta(before, after) == 5


def test_total_breaker_tripped_delta():
    def br(tripped):
        return parse_breakers({"nodes": {"a": {"name": "n1", "breakers": {"parent": {"tripped": tripped}}}}})
    assert total_breaker_tripped_delta(br(2), br(6)) == 4


def test_max_heap_percent():
    stats = {"nodes": {
        "a": {"name": "n1", "jvm": {"mem": {"heap_used_percent": 60}}},
        "b": {"name": "n2", "jvm": {"mem": {"heap_used_percent": 91}}},
    }}
    assert max_heap_percent(parse_jvm(stats)) == 91


def test_total_gc_time_delta_ms():
    def jvm(ms):
        return parse_jvm({"nodes": {"a": {"name": "n1", "jvm": {"gc": {"collectors": {
            "young": {"collection_time_in_millis": ms}}}}}}})
    assert total_gc_time_delta_ms(jvm(1000), jvm(4200)) == 3200


# ============================================================ classify_verdict (the fail-closed core)
def _clear_signals():
    """A fully-collected, fully-clear signal set (baseline HEALTHY)."""
    return {
        "write_rejected_delta": 0,
        "write_queue_max": 0,
        "write_queue_fill_max": 0.0,
        "indexing_pressure_pct_max": 0.1,
        "indexing_pressure_rejected_delta": 0,
        "breaker_tripped_delta": 0,
        "heap_percent_max": 40,
        "gc_time_delta_ms": 50,
        "write_pool_collected": True,
        "indexing_pressure_collected": True,
    }


def test_verdict_healthy_when_all_collected_and_clear():
    code, _ = classify_verdict(_clear_signals(), window_secs=5)
    assert code == VERDICT_HEALTHY


def test_verdict_rejecting_on_write_rejections():
    s = _clear_signals()
    s["write_rejected_delta"] = 4
    code, reasons = classify_verdict(s, window_secs=5)
    assert code == VERDICT_REJECTING
    assert any("rejected +4" in r for r in reasons)


def test_verdict_rejecting_on_indexing_pressure_rejections():
    s = _clear_signals()
    s["indexing_pressure_rejected_delta"] = 2
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_REJECTING


def test_verdict_rejecting_on_breaker_trip():
    s = _clear_signals()
    s["breaker_tripped_delta"] = 1
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_REJECTING


def test_verdict_rejecting_takes_precedence_over_saturation():
    # Both a full queue AND rejections: the more acute REJECTING must win.
    s = _clear_signals()
    s["write_rejected_delta"] = 1
    s["write_queue_fill_max"] = 0.5
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_REJECTING


def test_verdict_saturated_when_queue_fill_over_threshold():
    s = _clear_signals()
    s["write_queue_fill_max"] = 0.10  # at the 10% bound => building
    s["write_queue_max"] = 1000
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_SATURATED


def test_verdict_not_saturated_on_transient_nonzero_queue_below_threshold():
    # THE finding-1 regression: a small transient queue on a healthy, actively-writing cluster (well under
    # 10% of the bound) must NOT be flagged SATURATED.
    s = _clear_signals()
    s["write_queue_max"] = 30
    s["write_queue_fill_max"] = 0.003  # 30 / 10000
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_HEALTHY


def test_verdict_saturated_absolute_backstop_when_queue_size_unknown():
    # No bounded queue_size (fill None), but a large absolute depth => SATURATED via the backstop.
    s = _clear_signals()
    s["write_queue_fill_max"] = None
    s["write_queue_max"] = 500
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_SATURATED


def test_verdict_pressured_at_threshold():
    s = _clear_signals()
    s["indexing_pressure_pct_max"] = 0.70
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_PRESSURED


def test_verdict_not_pressured_just_below_threshold():
    s = _clear_signals()
    s["indexing_pressure_pct_max"] = 0.69
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_HEALTHY


def test_verdict_heap_pressure():
    s = _clear_signals()
    s["heap_percent_max"] = 85
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_HEAP_GC


def test_verdict_gc_pressure_scaled_to_window():
    # GC ran 2000ms of a 5s window = 40% >= 30% threshold => HEAP_GC.
    s = _clear_signals()
    s["gc_time_delta_ms"] = 2000
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_HEAP_GC


def test_verdict_gc_not_flagged_below_window_fraction():
    # 1000ms of a 5s window = 20% < 30% => not flagged.
    s = _clear_signals()
    s["gc_time_delta_ms"] = 1000
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_HEALTHY


def test_verdict_gc_ignored_on_single_snapshot_window_zero():
    # window_secs=0 (single snapshot): the GC-rate test can't be scaled, so it must not fire.
    s = _clear_signals()
    s["gc_time_delta_ms"] = 100000
    assert classify_verdict(s, window_secs=0)[0] == VERDICT_HEALTHY


def test_verdict_inconclusive_when_a_backpressure_endpoint_missing():
    # THE fail-closed regression: no acute problem in what we read, but the write-pool endpoint did not
    # respond => must be INCONCLUSIVE, never HEALTHY, and the reason names the endpoint.
    s = _clear_signals()
    s["write_pool_collected"] = False
    code, reasons = classify_verdict(s, window_secs=5)
    assert code == VERDICT_INCONCLUSIVE
    assert any("write thread-pool" in r for r in reasons)


def test_verdict_healthy_when_ip_limit_absent_but_endpoint_collected():
    # THE finding-2 regression: indexing pressure responded but a node reported no limit (pct None). That
    # is a COLLECTED-but-limitless reading, not a missing endpoint, so a clear host must still be HEALTHY.
    s = _clear_signals()
    s["indexing_pressure_pct_max"] = None
    assert classify_verdict(s, window_secs=5)[0] == VERDICT_HEALTHY


def test_verdict_healthy_on_single_snapshot_when_endpoints_collected():
    # Single snapshot (window 0): rate deltas are legitimately None, but the endpoints were collected and
    # the gauges are clear => HEALTHY, not falsely INCONCLUSIVE for the None deltas.
    s = _clear_signals()
    s["write_rejected_delta"] = None
    s["indexing_pressure_rejected_delta"] = None
    s["gc_time_delta_ms"] = None
    assert classify_verdict(s, window_secs=0)[0] == VERDICT_HEALTHY


def test_verdict_inconclusive_when_all_signals_missing():
    code, _ = classify_verdict({}, window_secs=5)
    assert code == VERDICT_INCONCLUSIVE


def test_verdict_rejecting_even_when_other_signals_missing():
    # A positively-observed rejection must still classify REJECTING even if other signals are absent -
    # fail-closed applies to CLEARING, not to a real problem we can see.
    code, _ = classify_verdict({"write_rejected_delta": 3}, window_secs=5)
    assert code == VERDICT_REJECTING


# ============================================================ human_bytes
@pytest.mark.parametrize("n,expected", [
    (None, "n/a"), (512, "512 B"), (2048, "2.0 KB"), (5 * 1024 * 1024, "5.0 MB"),
])
def test_human_bytes(n, expected):
    assert human_bytes(n) == expected

# Databricks notebook source
# MAGIC %md
# MAGIC # databricks-elasticsearch-pipelines: ES diagnostics
# MAGIC
# MAGIC A read-only maintenance notebook that gathers as much congestion-relevant information about an
# MAGIC Elasticsearch host (and, optionally, one index) as it can in a single run, so you can trigger it
# MAGIC when a host looks backed up and collect an outside view without a full pipeline run and without
# MAGIC writing anything to ES. It only ever issues `GET` requests to `_cat` / `_nodes` / `_cluster` /
# MAGIC `<index>` endpoints.
# MAGIC
# MAGIC It samples the counter-bearing endpoints TWICE, `sample_interval_secs` apart, so rejection / GC /
# MAGIC completion counters (all cumulative-since-boot) can be reported as a RATE over the window rather
# MAGIC than as meaningless lifetime totals. It then classifies the signature into one top-line verdict
# MAGIC (`pipeline_lib.es_diagnostics.classify_verdict`, unit-tested), fail-closed: it only reports HEALTHY
# MAGIC when the load-bearing signals (write-pool rejections/queue, indexing pressure) were positively
# MAGIC collected AND clear; a signal it could not read degrades the verdict to INCONCLUSIVE.
# MAGIC
# MAGIC Parameters:
# MAGIC - `es_host_url`, `secret_scope_name`, `secret_key_name` (deploy-time base_parameters, from the same
# MAGIC   `es_host_config` complex var the pipeline uses): where to connect and which secret holds the
# MAGIC   `api_key`. Required; a blank value fails closed.
# MAGIC - `ca_certs` (deploy-time base_parameter, from `${var.ca_certs}`): UC Volume path to a CA bundle
# MAGIC   verifying the ES TLS cert. Empty => system CAs (unless `verify_certs=false`).
# MAGIC - `verify_certs` (job parameter, default "true"): set "false" to skip TLS verification (self-signed
# MAGIC   endpoints). Ignored when `ca_certs` is set (a CA bundle implies verification).
# MAGIC - `index_name` (job parameter, OPTIONAL): an index to deep-dive (`_stats`/`_settings`/`_count`/
# MAGIC   shards). Blank => cluster/node level only. A malformed value fails closed.
# MAGIC - `sample_interval_secs` (job parameter, default "5"): gap between the two counter samples; "0"
# MAGIC   takes a single snapshot (no window => rate counters are reported as "not collected", fail closed).
# MAGIC - `request_timeout_secs` (job parameter, default "15"): per-request client timeout.
# MAGIC
# MAGIC Invoke on demand, e.g.
# MAGIC `databricks bundle run _es_diagnostics -t <target> -p <profile> --params index_name=<idx>`.

# COMMAND ----------
# Cell 1 - PARAMETERS + HELPERS. Read and validate parameters (fail closed on anything missing/unsafe),
# fetch the api_key from the secret scope, and define a single guarded es_get() that every collection
# step below goes through. Each GET is fail-SOFT: a failing endpoint records an error and returns no data
# rather than aborting the run, so one dead endpoint never costs us the rest of the picture. (A run that
# collects NOTHING is failed at the end - see the RESULTS cell.)
import json
import re
import time

import requests
import urllib3

from pipeline_lib.es_diagnostics import (
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

dbutils.widgets.text("es_host_url", "", "Elasticsearch endpoint, e.g. https://<host>:9200")
dbutils.widgets.text("secret_scope_name", "", "Databricks secret scope holding the ES api_key")
dbutils.widgets.text("secret_key_name", "", "Key in the scope whose value is the ES api_key")
dbutils.widgets.text("ca_certs", "", "UC Volume path to a CA bundle (PEM) verifying the ES TLS cert (empty => system CAs)")
dbutils.widgets.text("verify_certs", "true", "Verify TLS certs: true|false (false for self-signed; ignored when ca_certs set)")
dbutils.widgets.text("index_name", "", "Optional index to deep-dive (empty => cluster/node level only)")
dbutils.widgets.text("sample_interval_secs", "5", "Seconds between the two counter samples (0 => single snapshot)")
dbutils.widgets.text("request_timeout_secs", "15", "Per-request client timeout in seconds")

ES_HOST_URL = dbutils.widgets.get("es_host_url").strip()
SECRET_SCOPE_NAME = dbutils.widgets.get("secret_scope_name").strip()
SECRET_KEY_NAME = dbutils.widgets.get("secret_key_name").strip()
CA_CERTS = dbutils.widgets.get("ca_certs").strip()
VERIFY_CERTS = dbutils.widgets.get("verify_certs").strip().lower()
INDEX_NAME = dbutils.widgets.get("index_name").strip()
SAMPLE_INTERVAL_SECS = dbutils.widgets.get("sample_interval_secs").strip()
REQUEST_TIMEOUT_SECS = dbutils.widgets.get("request_timeout_secs").strip()

# --- validate, fail closed ---------------------------------------------------------------------------
if not ES_HOST_URL:
    raise ValueError("missing required parameter: es_host_url")
if not SECRET_SCOPE_NAME or not SECRET_KEY_NAME:
    raise ValueError("missing required parameter: secret_scope_name and/or secret_key_name")

# index_name is optional, but if given it is interpolated into a request PATH, so allow-list a safe
# ES index-name charset (lowercase alnum plus . _ -, not starting with - _ +) and reject everything else -
# no '/', wildcards, or '..' can reach the URL. This is the same fail-closed stance the checkpoint_clear
# job takes on config_name.
if INDEX_NAME and not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", INDEX_NAME):
    raise ValueError(
        f"invalid index_name {INDEX_NAME!r}: must match [a-z0-9][a-z0-9._-]* (a single concrete index, "
        f"no path separators or wildcards)"
    )


def _int_param(raw, default, lo, hi, name):
    """Parse an int job parameter, clamped to [lo, hi]; blank => default. Fails closed on non-numeric."""
    if raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"invalid {name} {raw!r}: must be an integer")
    return max(lo, min(hi, value))


SAMPLE_INTERVAL = _int_param(SAMPLE_INTERVAL_SECS, 5, 0, 60, "sample_interval_secs")
REQUEST_TIMEOUT = _int_param(REQUEST_TIMEOUT_SECS, 15, 1, 60, "request_timeout_secs")

# TLS verification target for requests: a CA bundle path (implies verify) wins; else the verify_certs
# boolean; default verify with system CAs. Suppress urllib3's InsecureRequestWarning only when we
# deliberately turn verification off, so the run log isn't spammed for an intentional self-signed target.
if CA_CERTS:
    _VERIFY = CA_CERTS
elif VERIFY_CERTS in ("false", "0", "no"):
    _VERIFY = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
else:
    _VERIFY = True

# api_key is read on the DRIVER (a Databricks secret => auto-redacted from output). The stored value is
# the base64 api_key the connector passes straight to elasticsearch-py, so the REST header is
# "Authorization: ApiKey <value>".
_API_KEY = dbutils.secrets.get(SECRET_SCOPE_NAME, SECRET_KEY_NAME)
_HEADERS = {"Authorization": f"ApiKey {_API_KEY}", "Accept": "application/json"}
_BASE = ES_HOST_URL.rstrip("/")

# Every endpoint we attempt and every one that failed, so the RESULTS cell can (a) show what we could not
# read and (b) fail the run only if we collected NOTHING at all.
ENDPOINTS_ATTEMPTED = []
ENDPOINTS_FAILED = []


def es_get(path, as_json=True):
    """GET {host}/{path}. Returns (ok, data, err). Fail-soft: never raises; records failures.

    On a non-2xx or a transport error, ok=False and err carries a short reason (status + body snippet, or
    the exception). data is the parsed JSON (as_json=True) or raw text (as_json=False) on success.
    """
    url = f"{_BASE}/{path}"
    ENDPOINTS_ATTEMPTED.append(path)
    try:
        resp = requests.get(url, headers=_HEADERS, verify=_VERIFY, timeout=REQUEST_TIMEOUT)
    except Exception as exc:  # noqa: BLE001 - transport failure is a recorded miss, not a crash
        err = f"{type(exc).__name__}: {exc}"
        ENDPOINTS_FAILED.append(f"{path} ({err})")
        return False, None, err
    if not (200 <= resp.status_code < 300):
        err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        ENDPOINTS_FAILED.append(f"{path} ({err})")
        return False, None, err
    try:
        data = resp.json() if as_json else resp.text
    except ValueError as exc:
        err = f"non-JSON response: {exc}"
        ENDPOINTS_FAILED.append(f"{path} ({err})")
        return False, None, err
    return True, data, None


print("es_diagnostics - parameters:")
print(f"  es_host_url          = {ES_HOST_URL!r}")
print(f"  secret               = scope {SECRET_SCOPE_NAME!r} key {SECRET_KEY_NAME!r} (value redacted)")
print(f"  ca_certs             = {CA_CERTS!r}")
print(f"  verify_certs         = {_VERIFY!r}")
print(f"  index_name           = {INDEX_NAME!r}" + ("" if INDEX_NAME else "  (cluster/node level only)"))
print(f"  sample_interval_secs = {SAMPLE_INTERVAL}" + ("  (single snapshot: no rate counters)" if SAMPLE_INTERVAL == 0 else ""))
print(f"  request_timeout_secs = {REQUEST_TIMEOUT}")

# COMMAND ----------
# Cell 2 - TWO-SAMPLE COUNTER COLLECTION. Snapshot the counter-bearing endpoints (write thread pool,
# indexing pressure, JVM/GC, breakers, and the index's _stats if requested), wait sample_interval_secs,
# and snapshot again. The reducers then diff the cumulative counters over the window; the point-in-time
# gauges (queue depth, indexing-pressure fraction, heap %) are read from the samples directly.


def _nodes_stats():
    """One _nodes/stats call covering the three sections we parse, trimmed with filter_path."""
    ok, data, _ = es_get(
        "_nodes/stats/indexing_pressure,jvm,breaker"
        "?filter_path=nodes.*.name,nodes.*.indexing_pressure,nodes.*.jvm.mem,nodes.*.jvm.gc,nodes.*.breakers"
    )
    return data if ok else {}


def snapshot():
    """Collect one sample of every counter/gauge endpoint. Missing endpoints degrade to empty structures
    (which the reducers read as 'not collected'), never to a crash."""
    ok_tp, tp_raw, _ = es_get("_cat/thread_pool/write?format=json"
                              "&h=node_id,node_name,active,queue,queue_size,rejected,completed")
    ns = _nodes_stats()
    snap = {
        "tp": parse_cat_thread_pool(tp_raw) if ok_tp else [],
        "ip": parse_indexing_pressure(ns),
        "jvm": parse_jvm(ns),
        "breakers": parse_breakers(ns),
    }
    if INDEX_NAME:
        ok_is, is_raw, _ = es_get(f"{INDEX_NAME}/_stats"
                                  "?filter_path=_all.total.indexing,_all.total.merges,_all.total.refresh,"
                                  "_all.total.flush,_all.total.translog,_all.total.segments")
        snap["index"] = parse_index_stats(is_raw) if ok_is else None
    return snap


print(f"sampling counters (interval {SAMPLE_INTERVAL}s)...")
# Measure the TRUE window with a monotonic clock around the two snapshots, not the configured interval:
# each snapshot() issues several sequential GETs on top of the sleep, so the real read-to-read gap is
# longer than SAMPLE_INTERVAL. classify_verdict scales the GC-pressure test by this window, so an
# understated window would overstate the GC fraction and over-fire HEAP_GC.
_t_a = time.monotonic()
SAMPLE_A = snapshot()
if SAMPLE_INTERVAL > 0:
    time.sleep(SAMPLE_INTERVAL)
    _t_b = time.monotonic()
    SAMPLE_B = snapshot()
    WINDOW_SECS = _t_b - _t_a  # start-of-A to start-of-B (sleep + A's GET durations)
else:
    SAMPLE_B = SAMPLE_A  # single snapshot: gauges only, no window => rate counters reported as absent
    WINDOW_SECS = 0


def _max_opt(*vals):
    """Max of the non-None values, or None if all are None (so a missing gauge stays 'not collected')."""
    present = [v for v in vals if v is not None]
    return max(present) if present else None


# Rate counters are only meaningful over a window; with a single snapshot they are 'not collected' (None),
# which makes the verdict fail closed rather than reading a quiet lifetime total as 'no problem now'.
if WINDOW_SECS > 0:
    write_rejected_delta = total_rejected_delta(SAMPLE_A["tp"], SAMPLE_B["tp"])
    ip_rejected_delta = total_indexing_pressure_rejected_delta(SAMPLE_A["ip"], SAMPLE_B["ip"])
    breaker_tripped_delta = total_breaker_tripped_delta(SAMPLE_A["breakers"], SAMPLE_B["breakers"])
    gc_time_delta_ms = total_gc_time_delta_ms(SAMPLE_A["jvm"], SAMPLE_B["jvm"])
else:
    write_rejected_delta = ip_rejected_delta = breaker_tripped_delta = gc_time_delta_ms = None

# The verdict gates HEALTHY on the rejection RATE deltas being non-None (actually measured over the window
# in BOTH samples), so we do not pass separate collection flags: a partial/total endpoint failure or a
# single snapshot leaves the relevant delta None, which fails the verdict closed to INCONCLUSIVE. The
# point-in-time gauges take the max over whichever samples produced a reading.
SIGNALS = {
    "write_rejected_delta": write_rejected_delta,
    "write_queue_max": _max_opt(max_write_queue(SAMPLE_A["tp"]), max_write_queue(SAMPLE_B["tp"])),
    "write_queue_fill_max": _max_opt(max_write_queue_fill(SAMPLE_A["tp"]), max_write_queue_fill(SAMPLE_B["tp"])),
    "indexing_pressure_pct_max": _max_opt(max_indexing_pressure_pct(SAMPLE_A["ip"]),
                                          max_indexing_pressure_pct(SAMPLE_B["ip"])),
    "indexing_pressure_rejected_delta": ip_rejected_delta,
    "breaker_tripped_delta": breaker_tripped_delta,
    "heap_percent_max": _max_opt(max_heap_percent(SAMPLE_A["jvm"]), max_heap_percent(SAMPLE_B["jvm"])),
    "gc_time_delta_ms": gc_time_delta_ms,
}

VERDICT, VERDICT_REASONS = classify_verdict(SIGNALS, WINDOW_SECS)

# --- print the write-path picture (the direct backpressure signals) ----------------------------------
print("\n=== WRITE THREAD POOL (per node, latest sample) ===")
for row in SAMPLE_B["tp"]:
    print(f"  {row['node']}: active={row['active']} queue={row['queue']}/{row['queue_size']} "
          f"rejected(lifetime)={row['rejected']} completed(lifetime)={row['completed']}")
if not SAMPLE_B["tp"]:
    print("  (write thread-pool not collected)")

print("\n=== INDEXING PRESSURE (per node, latest sample) ===")
for v in SAMPLE_B["ip"].values():
    pct = f"{v['pct']*100:.0f}%" if v["pct"] is not None else "n/a"
    print(f"  {v['name']}: current={human_bytes(v['current_bytes'])} limit={human_bytes(v['limit_bytes'])} "
          f"({pct} of limit) rejections(lifetime)={v['rejections_total']}")
if not SAMPLE_B["ip"]:
    print("  (indexing pressure not collected)")

print("\n=== JVM HEAP / GC + BREAKERS (per node, latest sample) ===")
for v in SAMPLE_B["jvm"].values():
    print(f"  {v['name']}: heap_used={v['heap_used_percent']}% gc_time(lifetime)={v['gc_time_ms']}ms")
for node, breakers in SAMPLE_B["breakers"].items():
    for bname, b in breakers.items():
        if b["tripped"]:
            print(f"  {node} breaker {bname}: tripped(lifetime)={b['tripped']} "
                  f"est={human_bytes(b['estimated_bytes'])}/{human_bytes(b['limit_bytes'])}")

print(f"\n=== WINDOW DELTAS (over {WINDOW_SECS:.1f}s) ===")
print(f"  write rejected delta        = {write_rejected_delta}")
print(f"  indexing-pressure rej delta = {ip_rejected_delta}")
print(f"  breaker tripped delta       = {breaker_tripped_delta}")
print(f"  GC time delta               = {gc_time_delta_ms} ms")

# COMMAND ----------
# Cell 3 - CLUSTER / NODE STATE (single-shot, point-in-time). Node health, cluster health + pending
# tasks, in-flight bulk tasks, and hot_threads - the "what is the box doing right now" picture that
# complements the write-path counters above.
print("=== NODE HEALTH (_cat/nodes) ===")
ok, nodes_rows, _ = es_get("_cat/nodes?format=json&h=name,heap.percent,cpu,load_1m,disk.used_percent,node.role")
if ok:
    for n in parse_cat_nodes(nodes_rows):
        print(f"  {n['node']} [{n['roles']}]: heap={n['heap_percent']}% cpu={n['cpu']}% "
              f"load_1m={n['load_1m']} disk_used={n['disk_used_percent']}%")
else:
    print("  (not collected)")

print("\n=== CLUSTER HEALTH (_cluster/health) ===")
ok, health, _ = es_get("_cluster/health")
if ok and isinstance(health, dict):
    print(f"  status={health.get('status')} nodes={health.get('number_of_nodes')} "
          f"active_shards%={health.get('active_shards_percent_as_number')}")
    print(f"  unassigned={health.get('unassigned_shards')} initializing={health.get('initializing_shards')} "
          f"relocating={health.get('relocating_shards')}")
    print(f"  pending_tasks={health.get('number_of_pending_tasks')} "
          f"task_max_waiting={health.get('task_max_waiting_in_queue_millis')}ms")
else:
    print("  (not collected)")

print("\n=== PENDING CLUSTER TASKS (_cat/pending_tasks) ===")
ok, pending, _ = es_get("_cat/pending_tasks?format=json&h=insertOrder,timeInQueue,priority,source")
if ok and isinstance(pending, list):
    if pending:
        for t in pending[:20]:
            print(f"  order={t.get('insertOrder')} waited={t.get('timeInQueue')} "
                  f"priority={t.get('priority')} source={t.get('source')}")
    else:
        print("  (none - cluster state queue is clear)")
else:
    print("  (not collected)")

print("\n=== IN-FLIGHT BULK/WRITE TASKS (_cat/tasks) ===")
ok, tasks, _ = es_get("_cat/tasks?format=json&detailed=true&h=action,running_time,node")
if ok and isinstance(tasks, list):
    write_tasks = [t for t in tasks if "bulk" in (t.get("action") or "") or "write" in (t.get("action") or "")]
    if write_tasks:
        for t in write_tasks[:20]:
            print(f"  {t.get('action')} running_time={t.get('running_time')} node={t.get('node')}")
    else:
        print("  (no bulk/write tasks in flight right now)")
else:
    print("  (not collected)")

print("\n=== HOT THREADS (_nodes/hot_threads, top 3 per node) ===")
ok, hot, _ = es_get("_nodes/hot_threads?threads=3", as_json=False)
print(hot if ok else "  (not collected)")

# COMMAND ----------
# Cell 4 - PER-INDEX DEEP DIVE (only when index_name is set). Indexing/merge/refresh/translog/segment
# state and its window delta, settings that shape write cost (refresh_interval, shards/replicas, translog
# durability), doc count, and per-shard placement/size.
if not INDEX_NAME:
    print("index_name not set - skipping per-index deep dive.")
else:
    print(f"=== INDEX {INDEX_NAME!r} STATS (latest sample + window delta) ===")
    idx_b = SAMPLE_B.get("index")
    idx_a = SAMPLE_A.get("index")
    if idx_b:
        print(f"  index_current(in-flight)={idx_b['index_current']} index_failed={idx_b['index_failed']}")
        print(f"  merges_current={idx_b['merges_current']} segments_count={idx_b['segments_count']} "
              f"segments_mem={human_bytes(idx_b['segments_memory_bytes'])}")
        print(f"  translog: ops={idx_b['translog_operations']} size={human_bytes(idx_b['translog_size_bytes'])} "
              f"uncommitted_ops={idx_b['translog_uncommitted_operations']} "
              f"uncommitted_size={human_bytes(idx_b['translog_uncommitted_size_bytes'])}")
        if WINDOW_SECS > 0 and idx_a and idx_b["index_total"] is not None and idx_a["index_total"] is not None:
            docs = idx_b["index_total"] - idx_a["index_total"]
            merges = (idx_b["merges_total"] or 0) - (idx_a["merges_total"] or 0)
            refreshes = (idx_b["refresh_total"] or 0) - (idx_a["refresh_total"] or 0)
            print(f"  window delta ({WINDOW_SECS:.1f}s): docs_indexed={docs} merges={merges} refreshes={refreshes}")
    else:
        print("  (index _stats not collected)")

    print(f"\n=== INDEX {INDEX_NAME!r} SETTINGS ===")
    ok, settings, _ = es_get(f"{INDEX_NAME}/_settings")
    if ok and isinstance(settings, dict):
        # Settings are keyed by the concrete (possibly aliased) index name; read the first entry's index block.
        for concrete, body in settings.items():
            idx = (body.get("settings", {}) or {}).get("index", {}) if isinstance(body, dict) else {}
            translog = idx.get("translog", {}) if isinstance(idx, dict) else {}
            print(f"  {concrete}: refresh_interval={idx.get('refresh_interval')} "
                  f"shards={idx.get('number_of_shards')} replicas={idx.get('number_of_replicas')} "
                  f"translog.durability={translog.get('durability')}")
    else:
        print("  (not collected)")

    print(f"\n=== INDEX {INDEX_NAME!r} DOC COUNT ===")
    ok, count, _ = es_get(f"{INDEX_NAME}/_count")
    print(f"  count={count.get('count')}" if ok and isinstance(count, dict) else "  (not collected)")

    print(f"\n=== INDEX {INDEX_NAME!r} SHARDS (_cat/shards) ===")
    ok, shards, _ = es_get(f"_cat/shards/{INDEX_NAME}"
                           "?format=json&h=shard,prirep,state,docs,store,node,unassigned.reason")
    if ok and isinstance(shards, list):
        for s in shards:
            unassigned = s.get("unassigned.reason")
            print(f"  shard={s.get('shard')} {s.get('prirep')} state={s.get('state')} docs={s.get('docs')} "
                  f"store={s.get('store')} node={s.get('node')}"
                  + (f" unassigned={unassigned}" if unassigned else ""))
    else:
        print("  (not collected)")

# COMMAND ----------
# Cell 5 - RESULTS. Print the fail-closed verdict and its reasoning, list any endpoints we could not read,
# and exit with a compact machine-readable summary. The run is FAILED (raise) only when we collected
# NOTHING at all (bad host/auth/total outage) - a diagnostic that gathered data succeeds even when the
# finding is "congested", because congestion is exactly what it is meant to report.
print("=" * 90)
print(f"VERDICT: {VERDICT}")
for reason in VERDICT_REASONS:
    print(f"  - {reason}")

if ENDPOINTS_FAILED:
    print(f"\nendpoints not collected ({len(ENDPOINTS_FAILED)}/{len(ENDPOINTS_ATTEMPTED)}):")
    for f in ENDPOINTS_FAILED:
        print(f"  - {f}")

_ip_pct = SIGNALS["indexing_pressure_pct_max"]
SUMMARY = (
    f"es_diagnostics verdict={VERDICT} host={ES_HOST_URL!r} index={INDEX_NAME or '-'!r} "
    f"window_secs={WINDOW_SECS:.1f} write_queue_max={SIGNALS['write_queue_max']} "
    f"write_rejected_delta={SIGNALS['write_rejected_delta']} "
    f"indexing_pressure_pct_max={f'{_ip_pct:.2f}' if _ip_pct is not None else None} "
    f"indexing_pressure_rejected_delta={SIGNALS['indexing_pressure_rejected_delta']} "
    f"heap_percent_max={SIGNALS['heap_percent_max']} "
    f"endpoints_failed={len(ENDPOINTS_FAILED)}/{len(ENDPOINTS_ATTEMPTED)}"
)
print(f"\nES DIAGNOSTICS COMPLETE: {SUMMARY}")

# Collected nothing at all => the diagnostic could not run (not merely 'found congestion'); fail the run.
if ENDPOINTS_ATTEMPTED and len(ENDPOINTS_FAILED) == len(ENDPOINTS_ATTEMPTED):
    raise RuntimeError(
        "es_diagnostics collected NO data - every endpoint failed (check es_host_url, api_key, TLS, and "
        f"network egress to the ES host). {SUMMARY}"
    )

# COMMAND ----------
# dbutils.notebook.exit() must be the ONLY statement in its cell: its return value becomes the cell's
# rendered output and the run-output value. Reached only when at least one endpoint was collected.
dbutils.notebook.exit(SUMMARY)

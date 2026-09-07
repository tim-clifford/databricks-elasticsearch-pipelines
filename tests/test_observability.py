"""Offline unit tests for pipeline_lib.observability. No Spark, no cluster: plain pytest.

The REAL_PROGRESS fixture is a verbatim StreamingQueryProgress captured from the live DBR 17.3 / Spark 4
runtime (the Phase-0 probe), so these tests pin the formatter to real runtime output, not to docs. The
rest exercise the fail-soft contract: format_progress must never raise, whatever the input.
"""
import json

from pipeline_lib.observability import (
    PROGRESS_TAG,
    batch_job_description,
    format_progress,
)

# Captured verbatim from a real micro-batch on the target runtime (Phase-0 probe). Note the Delta
# backlog metric values are STRINGS, sink.numOutputRows is -1 (ForeachBatchSink), and source-level
# latestOffset is null - all real runtime behavior this formatter must handle.
_END_OFFSET = ('{"sourceVersion":1,"reservoirId":"63336fa6-8d15-48fd-ab9f-ea93b85a52ee",'
               '"reservoirVersion":4,"index":0,"isStartingVersion":true}')
REAL_PROGRESS = {
    "id": "331ab813-97de-4915-8151-26f94c880214",
    "runId": "e6542e09-e87c-4a82-8c98-e358d6164f03",
    "name": "streaming_progress_probe",
    "timestamp": "2026-09-06T21:18:32.458Z",
    "batchId": 0,
    "batchDuration": 16116,
    "durationMs": {
        "triggerExecution": 16110, "queryPlanning": 301, "collectSourceMetrics": 179,
        "getBatch": 72, "commitOffsets": 99, "addBatch": 3323, "latestOffset": 11988, "walCommit": 104,
    },
    "eventTime": {},
    "stateOperators": [],
    "sources": [{
        "description": "DeltaSource[dbfs:/Volumes/x/y/z/_probe/tbl]",
        "startOffset": None,
        "endOffset": _END_OFFSET,
        "latestOffset": None,
        "numInputRows": 25,
        "inputRowsPerSecond": 0.0,
        "processedRowsPerSecond": 1.5512534127575082,
        "metrics": {"numFilesOutstanding": "19", "numBytesOutstanding": "19442"},
    }],
    "sink": {"description": "ForeachBatchSink", "numOutputRows": -1, "metrics": {}},
    "observedMetrics": {},
    "rtmMetrics": None,
}


def test_real_progress_line_carries_identity_and_counters():
    line = format_progress(REAL_PROGRESS)
    assert line.startswith(PROGRESS_TAG + " ")
    assert "name=streaming_progress_probe" in line
    assert "batchId=0" in line
    assert "numInputRows=25" in line
    assert "batchDuration_ms=16116" in line


def test_real_progress_surfaces_backlog_metrics():
    # The headline "caught up or behind" signal. This is the line that must never silently vanish.
    line = format_progress(REAL_PROGRESS)
    assert "numFilesOutstanding=19" in line
    assert "numBytesOutstanding=19442" in line


def test_real_progress_duration_breakdown_and_offset_and_rates():
    line = format_progress(REAL_PROGRESS)
    # durationMs breakdown is emitted as a compact JSON blob carrying the real keys.
    assert "durationMs=" in line
    dm = line.split("durationMs=", 1)[1]
    assert '"addBatch":3323' in dm
    assert '"latestOffset":11988' in dm  # file-listing time, inside the durationMs blob
    # Delta version progress from the offset; source-level latestOffset is null => no latestOffset token.
    assert "endOffset=v4/i0" in line
    assert "latestOffset=" not in line
    assert "processedRowsPerSecond=1.5512534127575082" in line


def test_format_progress_never_raises_on_bad_input():
    # Fail-soft contract: any input yields a string, never an exception.
    for bad in (None, "garbage", 42, [], {"sources": "not-a-list"}):
        out = format_progress(bad)
        assert isinstance(out, str)
        assert out.startswith(PROGRESS_TAG)


def test_missing_metrics_is_failsoft():
    prog = {"name": "p", "batchId": 3, "sources": [{"numInputRows": 5}]}  # no metrics key
    line = format_progress(prog)
    assert "batchId=3" in line
    assert "numFilesOutstanding" not in line  # absent, not fabricated
    assert "src0" in line


def test_missing_duration_and_sources_omitted_not_faked():
    line = format_progress({"name": "p", "batchId": 1})
    assert "durationMs=" not in line
    assert "src0" not in line
    assert "batchId=1" in line


def test_unparseable_offset_dropped_without_raising():
    prog = {"sources": [{"endOffset": "not-json", "metrics": {"numFilesOutstanding": "2"}}]}
    line = format_progress(prog)
    assert "endOffset=" not in line       # unparseable => token dropped
    assert "numFilesOutstanding=2" in line  # rest of the source still rendered


def test_extra_metric_keys_are_surfaced_not_dropped():
    # A future/conditional metric this runtime did not emit in the probe (e.g. numNewListedFiles,
    # backlogEndOffset) must appear when present, not be filtered out by the headline allow-list.
    prog = {"sources": [{"metrics": {
        "numFilesOutstanding": "7", "numBytesOutstanding": "800",
        "numNewListedFiles": "3", "backlogEndOffset": "42",
    }}]}
    line = format_progress(prog)
    assert "numFilesOutstanding=7" in line
    assert "numNewListedFiles=3" in line
    assert "backlogEndOffset=42" in line


def test_multiple_sources_indexed():
    prog = {"sources": [
        {"metrics": {"numFilesOutstanding": "1"}},
        {"metrics": {"numFilesOutstanding": "2"}},
    ]}
    line = format_progress(prog)
    assert "src0" in line and "src1" in line


def test_batch_job_description_shape():
    desc = batch_job_description("ecs_dns_activity_continuous", "ecs-dns-activity-continuous", 42)
    assert PROGRESS_TAG in desc
    assert "ecs_dns_activity_continuous" in desc
    assert "ecs-dns-activity-continuous" in desc
    assert "batch 42" in desc

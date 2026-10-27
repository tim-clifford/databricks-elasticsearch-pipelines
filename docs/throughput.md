# Throughput Tuning

How to send as much data to Elasticsearch as a feed needs without overloading it, on clusters no bigger
than the job requires. For anyone sizing a cluster or tuning a feed. The goal has three sides: keep each
feed caught up with its source, keep Elasticsearch healthy, and keep cluster cost down. Each knob is
defined in the [Parameters Reference](parameters.md); this doc is about how they interact.

- [Before you tune: know the source](#before-you-tune-know-the-source)
- [How the write works](#how-the-write-works)
- [Python threads and the GIL](#python-threads-and-the-gil)
- [The levers](#the-levers)
- [Reading the signals](#reading-the-signals)
- [A tuning procedure](#a-tuning-procedure)
- [Target throughput](#target-throughput)

## Before you tune: know the source

Start from how much data the feed has to move, not from the cluster. Work out, for each source table:

- **When it changes:** how often new data lands (continuously, every few minutes, hourly, in a daily
  load) and how predictable that is.
- **How much each change carries:** the typical rows and bytes per update, and the largest.
- **The shape over a day:** steady arrivals through the day, or large intermittent spikes (an hourly or
  daily batch); and, if it applies, the difference between peak and off-peak hours.

From that, set a target the feed must meet under normal conditions, over an interval that suits how
fresh the index needs to be. For example: "this feed must send X rows (Y bytes) to Elasticsearch every 15
minutes to stay caught up". Size for the peak interval, not the daily average, and leave headroom for
catching up after a pause or a failure. A spiky source either needs capacity for the spike or has to
accept falling behind during it and catching up after.

The source's Delta history gives the numbers. This lists the rows and bytes added per 15 minutes, from a
notebook. It counts every commit that reports rows written (appends, streaming writes, `COPY INTO`,
`CREATE` or `REPLACE TABLE AS SELECT`) and the rows a `MERGE` inserts, and leaves out commits with no
row count. The metrics differ by operation, so check the result against a few commits in the table's
history before relying on it. It can also overstate what a feed sends: a `REPLACE TABLE AS SELECT`
counts the whole table, a `MERGE`'s bytes include files rewritten for updates, and a streaming feed
skips change commits (`MERGE`, `UPDATE`, `DELETE`) altogether, so for a streaming feed count only the
append-type commits:

```python
from pyspark.sql import functions as F

m = F.col("operationMetrics")
history = spark.sql("DESCRIBE HISTORY <catalog>.<schema>.<table>")
(history
    .select("timestamp",
            F.coalesce(m["numTargetRowsInserted"], m["numOutputRows"]).cast("long").alias("rows"),
            F.coalesce(m["numTargetBytesAdded"], m["numOutputBytes"]).cast("long").alias("bytes"))
    .where(F.col("rows").isNotNull())
    .groupBy(F.window("timestamp", "15 minutes").alias("interval"))
    .agg(F.count("*").alias("commits"), F.sum("rows").alias("rows"), F.sum("bytes").alias("bytes"))
    .orderBy("interval")
    .display())
```

The history covers the table's log retention (30 days by default). The bytes are compressed Delta file
bytes, not the size of the JSON documents Elasticsearch receives, and the view can add or drop columns,
so treat them as a relative measure. Rows map one to one to documents for a row-wise view.

## How the write works

A feed's write moves through these steps:

1. Spark reads the source into partitions. [`max_partition_bytes`](parameters.md#max_partition_bytes)
   sets how finely the files are split.
2. The view transforms each partition. A view that only projects, filters and joins small lookup tables
   keeps the partition count; one that shuffles (a large join, `GROUP BY`, `DISTINCT`, a window) resets it.
3. Optionally, [`write_repartition`](parameters.md#write_repartition) reshuffles the rows into a set
   number of partitions.
4. Each partition is sent to Elasticsearch as bulk requests of up to
   [`chunk_size`](parameters.md#chunk_size) documents, with up to
   [`write_concurrency`](parameters.md#write_concurrency) requests in flight at once.

One idea explains most of the tuning:

> Requests in flight at ES at once ≈ (partitions writing at the same time) × `write_concurrency`

The partitions writing at the same time are capped by the total worker cores: each core runs one
partition at a time. Every request in flight is one HTTP connection doing a bulk write.

That number is the balance you're tuning. Too few requests in flight and the cluster spends most of its
time waiting on round trips, so batches take longer than they need to. Too many and Elasticsearch's write
queue fills: it starts rejecting documents with 429s, or slows down so requests time out. Most levers
below move that number, the size of each request, or both.

## Python threads and the GIL

The other limit is on the Spark side, in Python. Each partition's write runs in its own Python worker
process: Spark starts one for each task it runs at the same time. Every process has its own global
interpreter lock (GIL), which lets only one of its threads run Python code at a time. A partition's
threads are its `write_concurrency` senders plus the thread that hands them chunks, and they all share
that one GIL.

Most of the write doesn't need the GIL:

- **Building the documents:** Spark serializes each row to its JSON action line in the JVM, before the
  rows reach Python, and the senders pass those lines through without re-encoding them.
- **Compressing the request** and **waiting on the network and ES:** a sender waiting for its response
  doesn't hold the GIL, so the other senders can run.

What does need it is the Python work around each request: handing the lines to the senders, the ES
client's work to send each request, and reading each response. Under the [fast path](#fast-path) a clean
chunk's response is just its `errors` flag, which is cheap. A chunk that takes the full path (any
document-level error retry, such as a 409 or 429, or every chunk with `bypass_fast_path` on) has its response
decoded and checked document by document, which is the heaviest GIL work in the write.

What follows from that:

- **More `write_concurrency` stops helping once the GIL is busy.** While the senders mostly wait on ES,
  the GIL is free and each extra sender adds requests in flight. Once the senders' Python work keeps the
  GIL busy, a sender whose response has arrived has to wait for the GIL before it can read it. That wait
  counts in the request's round trip (`rtt_ms`), so the client looks slow in the same way a slow ES
  does, while ES's own `took_ms` stays flat. Past that point, more `write_concurrency` adds waiting, not
  throughput, and a longer `request_timeout` or more retries don't help, because the delay is in the
  client.
- **Cores add GILs.** Every partition running at the same time has its own process and GIL, so the way
  to more parallelism past the GIL limit is more partitions running at once: more
  [worker cores](#worker-cores), with [enough partitions](#partitions) to use them.
- **Node shape doesn't change it.** The same total cores as many small workers or fewer large ones runs
  the same number of GILs, each with the same contention. What node shape does change is what a node's
  partitions share: its network bandwidth and memory.
- **The full path costs GIL time.** A feed whose chunks often take the full path (routine 409s or
  429s, or `bypass_fast_path` on) does more Python work per request, so it reaches the GIL limit at a
  lower `write_concurrency`.

**How to tell.** With [`bulk_stats`](parameters.md#bulk_stats) on, each partition's process runs a small
probe thread that sleeps 5 ms at a time and records how late it wakes, which is almost all time spent
waiting for the GIL; `gil_wait_ms` is that delay (`n/a` from a connector without the probe). Compare `gil_wait_ms_total` with `wall_ms` on the `tail:` line, which reports both for the slowest
partition. A small share means the
round trips are a real wait on the network or ES. A large share, together with `rtt_ms` rising while
`took_ms` doesn't, means the partition is limited by its GIL: lower `write_concurrency` and get the
parallelism from more cores instead. The [monitoring log](monitoring.md#monitoring-log) keeps these
figures for each batch (in `batch_end`'s `es` field), so you can follow them across runs.

## The levers

### Worker cores

The cluster configuration sets the worker cores: the worker node type times the number of workers. For
example, a job cluster of 4 `m7g.2xlarge` workers (8 vCPUs each) has 32 worker cores. The driver's
cores don't run partitions and don't count. A job cluster's node type and worker count live in its
[job-cluster spec](feed-configuration.md#job-cluster-specs); serverless sizes itself.

More cores let more partitions write at the same time, which raises requests in flight and speeds up
the read and the view transform. Each of those partitions also has its own GIL, so cores, not
`write_concurrency`, are the way past the [GIL limit](#python-threads-and-the-gil). How the cores are
split into nodes doesn't change that; it changes only what each node's partitions share (network and
memory).

- **Too few:** partitions wait their turn and batches take longer.
- **Too many:** more pressure on ES (the same 429 and timeout risk as raising `write_concurrency`), and
  once ES or the network is the limit, extra cores sit idle and still cost money.

Adding cores only helps if there are enough partitions to use them; see [Partitions](#partitions).

### Driver sizing

The driver doesn't send documents, so it usually doesn't set the feed's throughput directly. It plans
each batch, runs the notebook, tracks the stream and its checkpoint, gathers each partition's results and
writes the monitoring log. That only matters when the driver is too small to keep up: then batches start
late, the gaps between them grow, and a driver short of memory can fail the run. Choose a driver node
with enough memory and CPU for that overhead. A [job group](feed-configuration.md#job-groups) with
several tasks on one cluster runs each task's notebook and stream on the same driver, so it may need a
larger driver than a single feed.

### `write_concurrency`

The number of bulk requests each partition keeps in flight. While one request waits for ES, others are
already on the wire, so the partition isn't idle during the round trip. It raises requests in flight
without adding cores. The connector sizes its connection pool to match.

- **Too low:** cores sit idle waiting on each round trip, and the write takes longer than ES needs.
- **Too high:** more pressure on ES (429s, slower responses). The threads in one partition also share
  one GIL, so past a point extra threads spend their time waiting for it instead of sending
  ([Python threads and the GIL](#python-threads-and-the-gil)). Each thread also holds a chunk of
  documents in memory.

Raise it in small steps and watch for 429s and `gil_wait_ms`.

### Partitions

The partition count decides how many cores can work at once. A good starting target is about 1x to 3x
the total worker cores, which keeps every core busy.

- **Too few:** cores sit idle, and a few large partitions become stragglers that set the pace for the
  whole batch.
- **Too many:** per-task overhead adds up, and more partitions writing at once (capped at the number of
  cores) could mean more requests in flight.

Two settings control the count:

- [`max_partition_bytes`](parameters.md#max_partition_bytes) splits the read: a smaller value gives more
  partitions, and that count flows through a non-shuffling view to the write with no extra cost. It
  can't even out partitions when the source files are uneven, and a shuffling view resets it.
- [`write_repartition`](parameters.md#write_repartition) sets the write's partition count directly and
  spreads the rows evenly across them, whatever the source files or the view do. It adds a shuffle, so
  it costs time of its own, but evenly sized partitions remove stragglers.

### `chunk_size`

The number of documents in each bulk request.

- **Larger:** fewer requests, so less per-request overhead. But each request takes longer, is more likely
  to hit `request_timeout`, and a failed request re-sends more documents.
- **Smaller:** each request is quick and cheap to retry, but there are more of them, and per-request
  overhead grows.

Large documents make each request heavier at the same `chunk_size`, so a feed with big documents usually
wants a smaller value. A request also can't hold more rows than Spark hands the writer in one Arrow batch, so beyond that size
raising `chunk_size` changes nothing. An Arrow batch ends at a row limit,
`spark.sql.execution.arrow.maxRecordsPerBatch`, and on Spark 4.0 and later (Databricks Runtime 17 and
later) also at a byte limit, `spark.sql.execution.arrow.maxBytesPerBatch`, whichever it reaches first. A
feed with large documents can hit the byte limit first, so its requests hold fewer than `chunk_size`
documents.

### Timeouts and retries

When requests are slow rather than rejected, these decide what happens:

- [`request_timeout`](parameters.md#request_timeout) is how long one request may take. Too short fails
  requests a slow-but-working ES would have finished; too long holds a thread on a request that won't
  come back. It and `chunk_size` are separate levers: change each for its own reason.
- [`max_retries_per_request`](parameters.md#max_retries_per_request) is how many times the connector
  re-sends a whole request after it fails (a timeout, a connection error, or a 429, 502, 503 or 504 on
  the request). Each timed-out attempt waits a full `request_timeout`, and every retry also waits the
  backoff, so a higher value lets a batch ride out a brief slowdown but can stretch a batch a long way
  before it fails and Spark retries the task.
- [`retry_backoff_initial`](parameters.md#retry_backoff_initial) and
  [`retry_backoff_max`](parameters.md#retry_backoff_max) set the wait between retries (2s doubling to
  30s by default). The wait is what gives an ES cluster under indexing pressure time to recover, and
  `bulk_stats` shows its total as `backoff_ms`.
- [`max_retries_per_doc`](parameters.md#max_retries_per_doc) is the separate retry for documents ES
  rejects with a 429. It buys time for a briefly full queue, with the same backoff. It is not a capacity
  fix: if 429s are steady, lower the pressure instead.

### Fast path

By default the connector checks each response with a quick errors-only probe and decodes per-document
results only when something failed. That keeps clean chunks cheap. But a chunk with any document-level
error is then sent a second time to get those results.
[`bypass_fast_path`](parameters.md#bypass_fast_path) decodes every response in full on the first send.
Turn it on when:

- **409s are expected regularly**, as with re-sends with `op_type: create`: almost every such chunk fails
  the probe and goes out twice, so bypassing the fast path halves those sends.
- **You need exact counts** for `op_type: create`.

Leave `bypass_fast_path` off (so the fast path stays on) for feeds whose chunks are almost always clean: decoding every response in full is
Python work under the GIL, so it lowers the `write_concurrency` a partition can use
([Python threads and the GIL](#python-threads-and-the-gil)).

### Streaming micro-batch size

A streaming feed processes new data in micro-batches.
[`max_files_per_trigger`](parameters.md#max_files_per_trigger) and
[`max_bytes_per_trigger`](parameters.md#max_bytes_per_trigger) cap how much each one reads.

- **Larger batches:** fewer fixed per-batch costs (planning, checkpoint commits and, with the
  [monitoring log](monitoring.md#monitoring-log) on, its writes), so better throughput, but each batch takes
  longer and a retried batch re-sends more.
- **Smaller batches:** steadier, shorter batches, at more total overhead. Don't set the caps so low that
  a micro-batch holds less than typically arrives in the time one micro-batch takes to process: each
  batch then leaves more behind than it sent, and the feed falls further behind however fast it runs.

The caps matter most during a backfill or a catch-up after downtime, where an uncapped first batch can
try to read the whole backlog at once. As a rough target, set one or both so a micro-batch holds no more
than what normally arrives in about an hour (or another period that suits the feed), from the
[source analysis](#before-you-tune-know-the-source). A feed that falls behind then catches up in
batches of a size the cluster and ES are known to handle, instead of one batch of everything it missed.
Inside each micro-batch, the write is tuned with the same levers as a batch run.

## Reading the signals

Turn on [`bulk_stats`](parameters.md#bulk_stats), if it isn't already on, to see what the write is doing. Which signals point to
which lever is in [Troubleshooting: Reading the signals](troubleshooting.md#reading-the-signals), and
what each part of the output means is in
[Logging and Monitoring: Reading the BULK_STATS output](monitoring.md#reading-the-bulk_stats-output). For
trends across runs, the [monitoring log](monitoring.md#monitoring-log) keeps each batch's counts and
write diagnostics, including 429s per batch.

## A tuning procedure

1. **Measure first.** Make sure `bulk_stats` is on for the feed (if it isn't already on for the target,
   turn it on in its config, or with `--params bulk_stats=true` for one run) and run a representative batch: one the size of the target from the
   [source analysis](#before-you-tune-know-the-source).
2. **Find the symptom** in [the signals table](troubleshooting.md#reading-the-signals).
3. **Change one lever** that matches it, by a modest step, and run again. Changing two at once hides
   which one helped.
4. **Stop raising parallelism** (cores, `write_concurrency`, partitions) as soon as 429s appear or
   `took_ms` starts climbing: ES is now the limit, and more pressure only makes batches slower and less
   reliable. Stop raising `write_concurrency` alone when `gil_wait_ms` grows and `rtt_ms` rises without
   `took_ms`: the partitions are limited by their GILs, and more cores are the lever.
5. **Weigh the cost.** If more cores no longer shorten the run, they're waiting on ES or the network. A
   smaller cluster that finishes in about the same time, and still meets the target, is the better
   choice.

When nothing on the client side moves the numbers, the limit is downstream: ES capacity (nodes, write
threads, shards and refresh settings) or the network between the cluster and ES.

## Target throughput

What to aim for, in short:

- **Stay caught up with the normal volume.** Under normal conditions the feed sends at least what the
  source receives over each interval ([know the source](#before-you-tune-know-the-source)), so it doesn't
  fall behind.
- **Leave room for spikes and recoveries.** Keep enough headroom to absorb the source's peaks and to
  catch up in a reasonable time after an outage or a pause, without overloading Elasticsearch.
- **Use the fewest cores that do that.** Once a feed meets both, extra cores only add cost. Pick the
  smallest cluster that still meets the target with that headroom.

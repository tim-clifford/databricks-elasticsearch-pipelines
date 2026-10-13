# Throughput Tuning

How to make a feed write faster without overloading Elasticsearch. For anyone sizing a cluster or
tuning a feed that is slower than it needs to be. Each knob is defined in the
[Parameters Reference](parameters.md); this doc is about how they interact.

- [How the write works](#how-the-write-works)
- [The levers](#the-levers)
- [Reading the signals](#reading-the-signals)
- [A tuning procedure](#a-tuning-procedure)

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

## The levers

### Worker cores

More cores let more partitions write at the same time, which raises requests in flight and speeds up
the read and the view transform.

- **Too few:** partitions wait their turn and batches take longer.
- **Too many:** more pressure on ES (the same 429 and timeout risk as raising `write_concurrency`), and
  once ES or the network is the limit, extra cores sit idle and still cost money.

Adding cores only helps if there are enough partitions to use them; see [Partitions](#partitions).

### `write_concurrency`

The number of bulk requests each partition keeps in flight. While one request waits for ES, others are
already on the wire, so the partition isn't idle during the round trip. It raises requests in flight
without adding cores. The connector sizes its connection pool to match.

- **Too low:** cores sit idle waiting on each round trip, and the write takes longer than ES needs.
- **Too high:** more pressure on ES (429s, slower responses). The threads in one partition also share a
  single Python interpreter, so past some point they spend time waiting for the GIL instead of sending,
  and each thread holds a chunk of documents in memory.

Raise it in small steps and watch for 429s.

### Partitions

The partition count decides how many cores can work at once. A good starting target is about 2 to 3
times the total worker cores, which keeps every core busy and lets a fast partition's core pick up
another one.

- **Too few:** cores sit idle, and a few large partitions become stragglers that set the pace for the
  whole batch.
- **Too many:** per-task overhead adds up, and more partitions writing at once means more requests in
  flight.

Set the count with [`max_partition_bytes`](parameters.md#max_partition_bytes) first: a smaller value
splits the read into more partitions, and that count flows through a non-shuffling view to the write
with no extra cost. Reach for [`write_repartition`](parameters.md#write_repartition) when that isn't
enough: the view shuffles and resets the count, or a few large source files read into too few
partitions. It adds a shuffle, so it costs time of its own.

### `chunk_size`

The number of documents in each bulk request.

- **Larger:** fewer requests, so less per-request overhead. But each request takes longer, is more likely
  to hit `request_timeout`, and a failed request re-sends more documents.
- **Smaller:** each request is quick and cheap to retry, but there are more of them, and per-request
  overhead grows.

Large documents make each request heavier at the same `chunk_size`, so a feed with big documents usually
wants a smaller value. A request also can't hold more rows than Spark hands the writer in one Arrow batch
(`spark.sql.execution.arrow.maxRecordsPerBatch`), so beyond that size raising `chunk_size` changes nothing.

### Timeouts and retries

When requests are slow rather than rejected, these decide what happens:

- [`request_timeout`](parameters.md#request_timeout) is how long one request may take. Too short fails
  requests a slow-but-working ES would have finished; too long holds a thread on a request that won't
  come back. Raising it pairs well with a smaller `chunk_size`.
- [`transport_max_retries`](parameters.md#transport_max_retries) re-sends a whole request after a
  connection failure or timeout. By default the ES client re-sends a timed-out request quietly, and each
  re-send waits a full `request_timeout`, so a run of timeouts can stretch a batch a long way before it
  fails and Spark retries the task.
- [`retry_transport_timeout`](parameters.md#retry_transport_timeout) moves timeout retries into the
  connector, with backoff, and makes them visible in `bulk_stats` (`timeout_sends`, `timeout_wait_ms`).
  Turn it on for a feed that sees timeouts, so you can see how much time retries cost.
- [`max_retries_per_doc`](parameters.md#max_retries_per_doc) is the separate retry for documents ES
  rejects with a 429. It buys time for a briefly full queue, with backoff. It is not a capacity fix: if
  429s are steady, lower the pressure instead.

### Fast path

By default the connector checks each response with a quick errors-only probe and decodes per-document
results only when something failed. That keeps clean chunks cheap, since decoding holds the GIL.
[`bypass_fast_path`](parameters.md#bypass_fast_path) decodes every response in full, which makes counts
exact for `op_type: create`, at some throughput cost. Leave it off unless you need those exact counts.

### Streaming micro-batch size

A streaming feed processes new data in micro-batches.
[`max_files_per_trigger`](parameters.md#max_files_per_trigger) and
[`max_bytes_per_trigger`](parameters.md#max_bytes_per_trigger) cap how much each one reads.

- **Larger batches:** fewer fixed per-batch costs (planning, checkpoint commits and, with the
  [monitoring log](monitoring.md#monitoring-log) on, its writes), so better throughput, but each batch takes
  longer and a retried batch re-sends more.
- **Smaller batches:** steadier, shorter batches, at more total overhead.

The caps matter most during a backfill or a catch-up after downtime, where an uncapped first batch can
try to read the whole backlog at once. The write itself inside each batch tunes the same way as batch
mode.

## Reading the signals

Turn on [`bulk_stats`](parameters.md#bulk_stats) to see what the write is doing. These signals tell you
which side of the balance a feed is on:

| You see | It means | Try |
|---|---|---|
| 429 rejections (`rejected(429=...)`), or `took_ms` rising run over run | ES is under too much pressure | lower `write_concurrency` or the partition count; check the host with [`es_diagnostics`](maintenance-jobs.md#diagnosing-an-elasticsearch-host) |
| `es_diagnostics` reports `REJECTING`, `SATURATED`, `PRESSURED` or `HEAP_GC_PRESSURE` | the ES host is the limit | lower the pressure, or add ES capacity |
| `rtt_ms` far above `took_ms`, no 429s, executors idle | the write is waiting on round trips | raise `write_concurrency`, or add partitions or cores |
| `gil_wait_ms` a large share of the partition's time | a partition's threads are waiting for the GIL | lower `write_concurrency` and get parallelism from more partitions instead |
| a high `wall_max/median` or `stragglers>2x` in the `tail:` line | uneven partitions; the slowest sets the pace | more, smaller partitions (`max_partition_bytes`, or `write_repartition` after a shuffling view) |
| a high `docs_max/median` in the `tail:` line | rows are unevenly spread across partitions | `write_repartition` |
| `docs/send` well below `chunk_size` | requests are smaller than you asked for | `chunk_size` is above the Arrow batch size, so raising it does nothing |
| `timeout_sends` climbing (with `retry_transport_timeout` on) | requests are timing out and being retried | smaller `chunk_size`, higher `request_timeout`, or less pressure |

### Reading the `BULK_STATS` output

With `bulk_stats` on, each batch logs lines tagged `BULK_STATS`:

- **`overall`:** totals for the batch: `docs/send` (real documents per request), the round-trip time the
  client saw (`rtt_ms`) and the time ES reports spending (`took_ms`), CPU and GIL-wait totals, and
  rejection counts.
- **One line per partition** (batch mode), with median, 95th percentile and maximum for the same timings.
- **`tail:`**, a summary of the slowest partition against the median one: its wall time, sends and
  documents, and the spread (`wall_p95`, `wall_max/median`, `stragglers>2x`, `docs_max/median`).
  Streaming feeds log `overall` and `tail:` for each micro-batch.

`rtt_ms` minus `took_ms` is time spent outside ES: the network, queuing, and decoding on the client.
Every line carries a `ts=` timestamp, which is when the batch actually ran; for streaming, the line can
appear in the log later than that. ([Why](technical-details.md#bulk-stats-relay-timing).)

For trends across runs, the [monitoring log](monitoring.md#monitoring-log) keeps each batch's counts and
write diagnostics, including 429s per batch.

## A tuning procedure

1. **Measure first.** Turn on `bulk_stats` for the feed (in its config, or `--params bulk_stats=true` for
   one run) and run a representative batch.
2. **Find the symptom** in the table above.
3. **Change one lever** that matches it, by a modest step, and run again. Changing two at once hides
   which one helped.
4. **Stop raising parallelism** (cores, `write_concurrency`, partitions) as soon as 429s appear or
   `took_ms` starts climbing: ES is now the limit, and more pressure only makes batches slower and less
   reliable.
5. **Weigh the cost.** If more cores no longer shorten the run, they're waiting on ES or the network. A
   smaller cluster that finishes in about the same time is the better choice.

When nothing on the client side moves the numbers, the limit is downstream: ES capacity (nodes, write
threads, shards and refresh settings) or the network between the cluster and ES. `es_diagnostics` is the
place to start.

# How to Backfill

A guide to re-sending a feed's existing data to Elasticsearch, for example after an index is rebuilt, a
new feed starts on a table that already has history, a feed recovers from a sustained outage, or a
streaming feed is [renamed](maintenance-jobs.md#renaming-a-feed).

- [Plan the volume first](#plan-the-volume-first)
- [Where the backfill runs](#where-the-backfill-runs)
- [How to send the data](#how-to-send-the-data)

## Plan the volume first

A backfill often sends far more data than a feed's normal runs, so size it before starting:

- **Know how much there is:** the rows and bytes to re-send, from the source's history or a count over
  the range ([Throughput Tuning: know the source](throughput.md#before-you-tune-know-the-source)).
- **Know what ES can handle:** a backfill that runs alongside the feed's normal runs adds to their load.
  Elasticsearch has to keep up with the regular feed **plus** the backfill, so budget the backfill
  within what the host can take on top of normal traffic, not within what it can take alone.
- **Break it into pieces:** split the backfill into batches small enough for Spark and ES to handle
  comfortably, and run them over a period of time with delays between them, rather than in one large
  run. Watch the [signals](troubleshooting.md#reading-the-signals) on the first pieces, and adjust the
  size before continuing.

What a re-sent row does depends on whether the feed sets [`es_id_field`](parameters.md#es_id_field),
which follows from its data (whether it has a unique column to use as the `_id`), not from the backfill:

- **With `es_id_field`:** a re-sent row overwrites its own document, so overlapping or repeated pieces
  are harmless, and a piece that fails can simply be run again. Rows already in the index are
  overwritten in place. (Under [`op_type: create`](parameters.md#op_type) they are left unchanged and
  counted as `ignored` instead.)
- **Without it:** Elasticsearch gives every document sent a new random `_id`, so a row sent twice
  becomes two documents. That includes rows the index already holds, rows a re-run piece sends again,
  and rows the regular feed sends too. Keep the pieces from overlapping, keep track of which ones
  finished, and if the index already holds part of the range, remove those documents first (or rebuild
  the index), or accept the duplicates.

## Where the backfill runs

There are two ways to set up the job that runs it.

### Option 1: the feed's own job, taken out of bundle control

Use the feed's existing job for the backfill, then return it to normal. Two things decide whether this
fits:

- **How long the backfill will take.** The feed's regular runs are paused for the whole backfill, so it
  sends no new data until the restart. Estimate the duration from the volume ([above](#plan-the-volume-first))
  and check that the feed can be offline that long. If not, use Option 2, which leaves the regular feed
  running.
- **A standalone feed or a group member.** A [job group](feed-configuration.md#job-groups) member shares
  its job and trigger with the other members, so pausing or changing that job stops or changes all of
  them: use Option 2 for a group member.

1. **Take the job out of bundle control.** In the Jobs UI, use **Disconnect from source** on the job, so
   it can be edited there (a bundle-deployed job is locked to its bundle definition until then).
2. **Pause its regular runs.** Pause the job's schedule or continuous trigger, and wait for a running
   run to finish (or cancel it). Keep it paused until the backfill has started, so no scheduled run
   can start in between. To backfill a continuous feed with batch runs (Method 2), also take the job out
   of continuous mode: remove its continuous trigger, and clear the task's `streaming_trigger_interval`
   parameter, since a batch run fails while it is set.
3. **Backfill**, with one of the [methods below](#how-to-send-the-data).
4. **Restart.** Deploy the bundle to the target again. The deploy puts the job back to its bundle
   definition, including its trigger and pause state, and the feed resumes its normal runs. If you
   backfilled a streaming feed with batch runs (Method 2), its checkpoint was left where the stream was
   paused, so the stream still has to catch up on everything that arrived from then until the restart.
   (Method 1 already streamed through to the present.)

Don't deploy the target during the backfill: the deploy would also undo the job's UI edits.

### Option 2: a parallel job for the backfill

Create a second feed config for the backfill, pointing at the same view and index as the regular feed.
Its file name is its own config name, for example `my_feed_backfill.yml` (config name
`my_feed_backfill`) next to a regular feed `my_feed.yml`; below it is `<backfill_config_name>`.

- It gets its own job (`index_pipeline_<backfill_config_name>`) and, if streaming, its own checkpoint,
  so it can't disturb the regular feed's job or position.
- Leave it on demand (no `schedule`, `continuous` or `job_group`), so it runs only when you start it.
- Set its `pipeline_mode` to the method you'll use: `streaming` for Method 1, `batch` for Method 2.
- Generate, deploy, run it as often as the backfill needs. When it's done, clear its checkpoint if it
  streamed (`checkpoint_clear` with `config_name=<backfill_config_name>`; the checkpoint outlives the
  job, and a later backfill under the same name would resume from it), then delete the config,
  regenerate and deploy to remove the job.

This option works for any feed, including continuous feeds and job group members. The regular feed
keeps running throughout, so the ES load of both has to fit together
([above](#plan-the-volume-first)). Be mindful of any potential overlap between the backfill job and the
regular feed.

## How to send the data

### Method 1: streaming from the start of the table (streaming feeds only)

Start a stream with no checkpoint and `streaming_start=full`, so it begins at the start of the table and
works forward. The whole table is sent, so check the total is reasonable for Spark streaming and for ES
before starting, and bound each micro-batch with
[`max_files_per_trigger`](parameters.md#max_files_per_trigger) or
[`max_bytes_per_trigger`](parameters.md#max_bytes_per_trigger) so the stream works through the table in
batches of a known size instead of reading it in one
([Streaming micro-batch size](throughput.md#streaming-micro-batch-size)).

**With an Option 2 backfill job:** the new job has no checkpoint yet, so there is nothing to clear and the
regular feed is left alone. Run it with `streaming_start=full`:

```bash
databricks bundle run index_pipeline_<backfill_config_name> -t <target> \
  --params pipeline_mode=streaming,streaming_start=full
```

**With the feed's own job (Option 1):** with its trigger paused (step 2), clear this feed's checkpoint
([Resetting a checkpoint](maintenance-jobs.md#resetting-a-checkpoint)), then start it from the
beginning:

```bash
databricks bundle run checkpoint_clear -t <target> --params config_name=<config_name>
databricks bundle run index_pipeline_<config_name> -t <target> \
  --params pipeline_mode=streaming,streaming_start=full
```

For a **continuous** feed, the always-on run is the job's only run, so instead of the second command,
set `streaming_start` to `full` in the job's parameters in the Jobs UI and unpause the trigger: the
always-on run then starts from the beginning of the table. `streaming_start` applies only while there is
no checkpoint, so it has no further effect once the stream has started. Once caught up, the stream
carries on as a normal feed from its new checkpoint.

### Method 2: batch runs in sized pieces

Start the job by hand in batch mode, one piece at a time, each limited by a
[`filter_condition`](parameters.md#filter_condition), for example a date range:

```bash
databricks bundle run index_pipeline_<config_name> -t <target> \
  --params pipeline_mode=batch,filter_condition="event_date >= '2026-01-01' AND event_date < '2026-02-01'"
```

A `--params filter_condition` replaces the feed's own `filter_condition`; the two are not combined. If the
feed has its own filter (for example one of several feeds writing one index, each with its own
subset), repeat it in each piece: `"(<feed filter>) AND event_date >= ..."`.

Choose a range size the cluster and ES handle comfortably, run the pieces in order with a pause between
them, and check each run's summary line before starting the next
([Run output](monitoring.md#run-output)). A batch run doesn't touch a streaming feed's checkpoint, so
the regular stream's position is unaffected.

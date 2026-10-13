# How to Backfill

A guide to re-sending a feed's existing data to Elasticsearch, for example after an index is rebuilt or a
new feed starts on a table that already has history.

This guide is a placeholder and will be filled in. Until then, these references cover the pieces involved:

- [`streaming_start`](parameters.md#streaming_start): where a streaming feed starts on its first run
  (`full` backfills the whole table).
- [Resetting a checkpoint](maintenance-jobs.md#resetting-a-checkpoint): making a streaming feed start over.
- [`max_files_per_trigger`](parameters.md#max_files_per_trigger) and
  [`max_bytes_per_trigger`](parameters.md#max_bytes_per_trigger): bounding each micro-batch during a
  backfill.
- [Batch or streaming](feed-configuration.md#batch-or-streaming): a batch run re-exports the whole view.

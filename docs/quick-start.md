# New Feed Quick Start

This guide gets one new feed working end to end, as a first proof that data flows from a Delta table into an Elasticsearch index: one view, one config, deploy, run, check. It is the shortest path, not the full set of options; the [Feed Configuration Guide](feed-configuration.md) covers everything a feed can do.

## Before you start

- **Your environment is already set up**: the workspace, targets, host configs and connector wheel ([Environment Setup](setup.md)).
- **The target index exists** in Elasticsearch, with its mapping.

This is a walkthrough, not a reference. For the details of each config key, parameter, or behavior, follow the links to [Parameters Reference](parameters.md) and other docs mentioned here.

## Steps

### Write the view

Create `_pipelines/pipeline_views/<view_name>.sql` (the filename becomes the view name). Use three parameters resolved from the config at deploy:

- `${view}` the fully-qualified view to create: `catalog.schema.name`
- `${source}` the fully-qualified source table: `catalog.schema.table`
- `${ref_<alias>}` any joined reference tables (optional)

```sql
-- _pipelines/pipeline_views/my_view.sql
CREATE OR REPLACE VIEW ${view} AS
SELECT
    id,
    timestamp,
    status,
    details
FROM ${source}
WHERE status = 'active'
```

An unknown `${...}` parameter fails `deploy_views`, so a typo can't create a view in the wrong place.

### Write the config

Create `_pipelines/pipeline_configs/<config_name>.yml` (the filename becomes the job name `index_pipeline_<config_name>`). The filename can contain only letters, digits, `_`, and `-`, not dots or other characters, and can't start with `_`. Set three required keys: `es_index_name`, `view`, and `source`. Add `es_id_field` (recommended for idempotent writes):

```yaml
# _pipelines/pipeline_configs/my_data.yml
es_index_name: my-data-index
es_id_field: id                    # column from the view to use as the ES _id
es_host_config: es_host_default    # optional; omit to use the bundle default

pipeline_mode: batch               # optional; batch (default) or streaming

view:
  catalog: my_workspace
  schema: my_schema
  name: my_view

source:
  catalog: my_workspace
  schema: my_schema
  table: my_table
```

A `catalog` or `schema` may include `${environment}` for per-environment names (for example `acme_${environment}`); view and table names may not. Every parameter shown here is defined in [Parameters Reference](parameters.md). To add a join, see [Feed Configuration: Reference tables](feed-configuration.md#reference-tables).

For a first test against an index or host that can't take the whole table yet, add a [`filter_condition`](parameters.md#filter_condition) that limits the feed to a volume the target environment can handle, for example a recent date range, and remove it once the test is done:

```yaml
filter_condition: "timestamp >= current_date() - INTERVAL 1 DAY"   # optional; for initial testing only
```

### Regenerate job resources

```bash
python framework/scripts/gen_jobs.py
```

This creates `_pipelines/resources/<config_name>.job.yml`. Run it whenever you add, change, or remove a config file. Use `python framework/scripts/gen_jobs.py --check` (in CI or before deploy) to verify that every config has been regenerated; it fails if any generated job file is missing, stale, or orphaned.

### Deploy the bundle

```bash
databricks bundle validate -t <target> -p <profile>
databricks bundle deploy -t <target> -p <profile>
```

Replace `<target>` with your environment (e.g. `dev`, `prd`) and `<profile>` with your Databricks CLI profile. The bundle resolves variables from that target's block in `databricks.yml`. Check the status in the Databricks workspace.

### Deploy the views

```bash
databricks bundle run deploy_views -t <target> -p <profile>
```

This creates the Databricks views from the `.sql` files. Watch for `created my_view` in the output (or a `FAILED` message if something went wrong).

### Run the feed

```bash
databricks bundle run index_pipeline_<config_name> -t <target> -p <profile>
```

For example, if your config is `my_data.yml`:

```bash
databricks bundle run index_pipeline_my_data -t dev -p my-profile
```

Batch mode runs once and stops. Streaming mode drains all available data, then stops (or runs continuously with a `continuous` block; see [Feed Configuration: Continuous streaming](feed-configuration.md#continuous-streaming)).

### Verify

In the Databricks workspace, open the job's run output and look for the summary line:

- **Batch:** `BATCH EXPORT COMPLETE: written=<N> deleted=<N> errors=<N> ignored=<N> total_input=<N>`. `written` should match `total_input`, with `errors=0`.
- **Streaming:** `STREAMING EXPORT COMPLETE: streaming_start=<new|full> batches=<N> rows_pushed=<N> ...`. With `streaming_start: new` (the usual setting), the first run only records where the stream starts and sends nothing that is already in the table, so 0 rows pushed is expected. Run the feed again after new data has landed in the source table: that run should push the new rows.

If monitoring is on, query the log table:

```sql
SELECT job_run_id, status, start_ts, end_ts, docs_written, error_message
FROM <catalog>.<schema>.<table>          -- your monitoring_log_table
WHERE config_name = '<config_name>' AND record_type = 'run_end'
ORDER BY start_ts DESC
LIMIT 5;
```

The latest row should show `status = 'success'`. See [Logging and Monitoring: Monitoring log](monitoring.md#monitoring-log) for more.

## Next steps

Explore these to shape your feed:

- **Batch or streaming**: [Feed Configuration: Batch or streaming](feed-configuration.md#batch-or-streaming)
- **Scheduling**: [Feed Configuration: CRON scheduling](feed-configuration.md#cron-scheduling)
- **Compute**: [Feed Configuration: Compute](feed-configuration.md#compute)
- **Tuning for throughput**: [Throughput Tuning](throughput.md)
- **Sharing a view**: [Feed Configuration: Sharing a view across feeds](feed-configuration.md#sharing-a-view-across-feeds)
- **All parameters**: [Parameters Reference](parameters.md)

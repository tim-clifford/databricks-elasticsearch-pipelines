# Deployment

Deploying the bundle and running feeds. For anyone deploying or running feeds.

## Deploy and run

The workflow is generate, deploy, and run. Bundle variables come from the last deploy, so you fill in each target's `variables` block in `databricks.yml` once and need no `--var` on routine deploys.

Generate job definitions from your configs:

```bash
python framework/scripts/gen_jobs.py
```

Deploy to your target environment (variables from `databricks.yml` or `--var`):

```bash
databricks bundle deploy -t dev -p <profile>
```

Create or update the views:

```bash
databricks bundle run deploy_views -t dev -p <profile>
```

Run a feed with the defaults from its config and the last deploy:

```bash
databricks bundle run index_pipeline_<config_name> -t dev -p <profile>
```

Override run-time settings for a single run:

```bash
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> \
  --params filter_condition="action = 'allowed'",chunk_size=1000,max_partition_bytes=8m
```

Stream a full table backfill (instead of the default new commits only):

```bash
databricks bundle run index_pipeline_<config_name> -t dev -p <profile> \
  --params pipeline_mode=streaming,streaming_start=full
```

You can also override bundle variables at deploy, for a one-off wheel path for example:

```bash
databricks bundle deploy -t dev -p <profile> \
  --var="wheel_path=/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"
```

Bundle variables persist from one deploy to the next, so fill in each target's `variables` block in your fork's `databricks.yml` once. Prefer not to commit paths? Set per-target values in the git-ignored `.databricks/bundle/<target>/variable-overrides.json` instead; the bundle resolves them the same way.

Every job grants `CAN_MANAGE_RUN` to the `users` group, so anyone in the workspace can trigger, stop or repair a run without being the job's owner. A run executes as the job's run-as identity, and a parameter override applies to that run only. See [Parameters Reference](parameters.md#how-values-are-set) for the full precedence order.

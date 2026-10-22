# Deployment

Deploying the bundle and running feeds. For anyone deploying or running feeds. Setting up a new
workspace or target comes first: see [Environment Setup](setup.md).

- [The deploy sequence](#the-deploy-sequence)
- [What to re-run after a change](#what-to-re-run-after-a-change)
- [Running a feed](#running-a-feed)
- [Overriding a bundle variable at deploy](#overriding-a-bundle-variable-at-deploy)
- [Who can run the jobs](#who-can-run-the-jobs)

## The deploy sequence

The full sequence, from the repo root. Each step is explained below; later docs refer back to this one.

```bash
pip install -r requirements.txt pytest                      # once: the generator's pinned dependency, and pytest
python -m pytest framework/tests -q                         # after changing framework code or taking a release
python framework/scripts/gen_jobs.py                        # generate a job per feed config
python framework/scripts/gen_jobs.py --check                # confirm the generated jobs are current
databricks bundle validate -t <target> -p <profile>
databricks bundle deploy -t <target> -p <profile>
databricks bundle run deploy_views -t <target> -p <profile>
databricks bundle run log_table_create -t <target> -p <profile>   # optional: only if you use the monitoring log
databricks bundle run index_pipeline_<config_name> -t <target> -p <profile>
```

1. **Install the tooling** once per machine. `requirements.txt` pins the generator's one dependency,
   pyyaml ([why it's pinned](framework-maintenance.md#the-pyyaml-pin)); pytest is only for the tests.
2. **Run the tests** when the framework itself changed: a code change under `framework/` or a new
   release ([Framework Maintenance](framework-maintenance.md#running-the-tests)). A feed change doesn't
   need them.
3. **Generate** the job files under `_pipelines/resources/` from the feed configs, and commit them. Run
   it after adding, changing or removing any config.
4. **Check** that the generated files match the configs: `--check` changes nothing and fails on a stale,
   missing or orphaned file. Run it in CI too.
5. **Validate** the bundle for the target. It resolves the target's variables and checks the job
   definitions without deploying anything.
6. **Deploy.** The target's variables come from `databricks.yml` ([precedence](parameters.md#bundle-variables)).
   A deploy creates or updates the feed jobs and the six maintenance jobs.
7. **Deploy the views.** `deploy_views` creates or replaces every feed's view from its `.sql` file.
8. **Create the monitoring log table** the first time you use the log in a target
   ([Logging and Monitoring](monitoring.md#monitoring-log)). Re-running it is safe.
9. **Run a feed** and check its output ([Run output](monitoring.md#run-output)).

## What to re-run after a change

| You changed | Run |
|---|---|
| a feed config | generate, check, deploy; also `deploy_views` if the change touches the view, source or reference tables |
| a feed config's file name | the same, but a streaming feed starts over from a new checkpoint: see [Renaming a feed](maintenance-jobs.md#renaming-a-feed) first |
| a view's `.sql` file | deploy, then `deploy_views` |
| `databricks.yml` | deploy (generate first if the change is to a value the generator reads: see [Read at generation](parameters.md#bundle-variables)) |
| a job-cluster spec | generate, check, deploy |
| framework code, or a new release | the whole sequence ([Taking a new release](framework-maintenance.md#taking-a-new-release)) |

## Running a feed

Run a feed with the defaults from its config and the last deploy:

```bash
databricks bundle run index_pipeline_<config_name> -t <target> -p <profile>
```

Override run-time settings for a single run:

```bash
databricks bundle run index_pipeline_<config_name> -t <target> -p <profile> \
  --params filter_condition="action = 'allowed'",chunk_size=1000,max_partition_bytes=8m
```

A `--params` override applies to that run only ([precedence](parameters.md#run-time-knobs-one-precedence-rule)).
It doesn't work per member on a [job group](feed-configuration.md#run-time-parameters-baked-per-task).

Stream a whole table on a feed's first streaming run (instead of only new commits):

```bash
databricks bundle run index_pipeline_<config_name> -t <target> -p <profile> \
  --params pipeline_mode=streaming,streaming_start=full
```

`streaming_start` applies only when the feed has no checkpoint yet; see [How to Backfill](backfill.md).

## Overriding a bundle variable at deploy

A `--var` overrides a bundle variable for one deploy, for a one-off wheel path for example:

```bash
databricks bundle deploy -t <target> -p <profile> \
  --var="wheel_path=/Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<version>-py3-none-any.whl"
```

The deployed jobs keep that value until the next deploy, which uses `databricks.yml` again unless it
also passes the `--var`. For values you set every time, fill in the target's `variables` block in
`databricks.yml` instead. Prefer not to commit them? Set per-target values in the git-ignored
`.databricks/bundle/<target>/variable-overrides.json`; the bundle reads it on every deploy. A `--var` on
`bundle run` has no effect.

## Who can run the jobs

Every job grants `CAN_MANAGE_RUN` to the workspace's `users` group, so anyone in the workspace can
start, stop or repair a run without being the job's owner. The grant is written into each job
definition: the generator adds it to every generated feed and group job (`framework/scripts/gen_jobs.py`),
and each fixed job in `framework/resources/` has the same `permissions` block. A run still executes as
the job's run-as identity, not as the person who started it ([why that matters](technical-details.md#can-manage-run)).

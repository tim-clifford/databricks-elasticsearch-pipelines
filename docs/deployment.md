# Deployment

Deploying the bundle and running feeds. For anyone deploying or running feeds. Setting up a new
workspace or target comes first: see [Environment Setup](setup.md).

- [The deploy sequence](#the-deploy-sequence)
- [What to re-run after a change](#what-to-re-run-after-a-change)
- [Continuous jobs need a restart](#continuous-jobs-need-a-restart)
- [Running a feed](#running-a-feed)
- [Overriding a bundle variable at deploy](#overriding-a-bundle-variable-at-deploy)
- [Who can run the jobs](#who-can-run-the-jobs)

## The deploy sequence

The full sequence, from the repo root. Each step is explained below; later docs refer back to this one.
The `databricks` commands here and in the other docs don't name a workspace; add `-p <profile>` if you
use a named CLI profile ([Local tools](setup.md#local-tools)).

```bash
pip install -r requirements.txt pytest               # once: the generator's pinned dependency, and pytest
python -m pytest framework/tests -q                  # after changing framework code or taking a release
python framework/scripts/gen_jobs.py                 # generate a job per feed config
python framework/scripts/gen_jobs.py --check         # confirm the generated jobs are current
databricks bundle validate -t <target>
databricks bundle deploy -t <target>
databricks bundle run deploy_views -t <target>
databricks bundle run log_table_create -t <target>   # optional: only if you use the monitoring log
databricks bundle run index_pipeline_<config_name> -t <target>
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
   A deploy creates or updates the feed jobs and the seven maintenance jobs. A continuous feed's run
   that is already going keeps its old settings until it is restarted
   ([Continuous jobs need a restart](#continuous-jobs-need-a-restart)).
7. **Deploy the views.** `deploy_views` creates or replaces every feed's view from its `.sql` file. When
   only a few views changed, `--params views=<name>,<name>` deploys just those
   ([Deploying some of the views](maintenance-jobs.md#deploying-some-of-the-views)).
8. **Create the monitoring log table** the first time you use the log in a target
   ([Logging and Monitoring](monitoring.md#monitoring-log)). Re-running it is safe.
9. **Run a feed** and check its output ([Run output](monitoring.md#run-output)).

## What to re-run after a change

| You changed | Run |
|---|---|
| a feed config | generate, check, deploy; also `deploy_views` if the change touches the view, source or reference tables (`--params views=<view name>` deploys only that view) |
| a feed config's file name | the same, but a streaming feed starts over from a new checkpoint: see [Renaming a feed](maintenance-jobs.md#renaming-a-feed) first |
| a view's `.sql` file | deploy, then `deploy_views` (`--params views=<view name>,...` for just the changed views) |
| `databricks.yml` | deploy (generate first if the change is to a value the generator reads: see [Read at generation](parameters.md#bundle-variables)) |
| a job-cluster spec | generate, check, deploy |
| framework code, or a new release | the whole sequence ([Taking a new release](framework-maintenance.md#taking-a-new-release)) |

After any of these, restart each running continuous feed the change affects, or it carries on with its
old settings ([below](#continuous-jobs-need-a-restart)).

## Continuous jobs need a restart

A continuous (always-on) feed has one run that never ends, and a run keeps the job settings, parameters
and deployed files it started with. So a deploy updates the job, but the run already going doesn't pick
the change up until it restarts. This applies to every change in the table above, for example:

- a feed config value, such as a run-time knob, `trigger_interval` or `compute`;
- a `databricks.yml` value the job uses, such as `wheel_path` (a new connector version), a host config,
  or a run-time knob's global;
- the view's `.sql` file, since a stream reads the view's `SELECT` from the deployed files when it
  starts;
- framework code, from a change or a new release.

To apply the change, open the job in the Jobs UI and click **Restart run**, or start it with the Jobs
API's `run-now` request, as in the Databricks
[continuous jobs docs](https://docs.databricks.com/aws/en/jobs/continuous). For a
[job group](feed-configuration.md#job-groups) that restarts every member, since they share one run.
Each restarted stream resumes from its checkpoint.

A feed whose continuous trigger was paused needs no separate restart: unpausing it starts a fresh run,
which picks up the current settings.

## Running a feed

Run a feed with the defaults from its config and the last deploy:

```bash
databricks bundle run index_pipeline_<config_name> -t <target>
```

Override run-time settings for a single run:

```bash
databricks bundle run index_pipeline_<config_name> -t <target> \
  --params filter_condition="action = 'allowed'",chunk_size=1000,max_partition_bytes=8m
```

A `--params` override applies to that run only ([precedence](parameters.md#run-time-knobs-one-precedence-rule)).
It doesn't work per member on a [job group](feed-configuration.md#run-time-parameters-baked-per-task).

Stream a whole table on a feed's first streaming run (instead of only new commits):

```bash
databricks bundle run index_pipeline_<config_name> -t <target> \
  --params pipeline_mode=streaming,streaming_start=full
```

`streaming_start` applies only when the feed has no checkpoint yet; see [How to Backfill](backfill.md).

## Overriding a bundle variable at deploy

A `--var` overrides a bundle variable for one deploy, for a one-off wheel path for example:

```bash
databricks bundle deploy -t <target> \
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

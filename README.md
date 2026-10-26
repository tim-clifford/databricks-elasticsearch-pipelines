# databricks-elasticsearch-pipelines

A framework for exporting data from Databricks Delta tables into Elasticsearch, packaged as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/). It uses the
[**databricks-es-connector**](/../../../databricks-es-connector) library for the
transfer.

## Overview

A **feed** exports one Delta source table, through a view, to one Elasticsearch index. A feed is two
files you write:

- a view, `_pipelines/pipeline_views/<view_name>.sql`, defining what gets exported, and
- a config, `_pipelines/pipeline_configs/<config_name>.yml`, saying where the view, its source table and any
  lookup tables live, and which index and Elasticsearch host the data goes to.

A generator, [`framework/scripts/gen_jobs.py`](framework/scripts/gen_jobs.py), turns the configs into
Databricks jobs, and `databricks bundle deploy` deploys them. Feeds are configured in the `_pipelines/`
folder and deploy as `index_pipeline_<config_name>` jobs.

The bundle deploys:

- **Feed jobs.** By default each feed gets its own job, `index_pipeline_<config_name>`, with one task.
  Feeds can instead share one job as a **job group** (`index_pipeline_group_<job_group>`), with one task
  per feed, so they can share a cluster. Every feed task runs the same notebook,
  [`framework/notebooks/run_index_pipeline.py`](framework/notebooks/run_index_pipeline.py), with its own
  config, in `batch` mode (re-export the view) or `streaming` mode (send only new source commits).
- **Maintenance jobs**: creating or replacing the views (`deploy_views`), clearing a streaming
  checkpoint, checking an Elasticsearch host (a work in progress), building the connector wheel,
  creating and pruning the optional monitoring log, a Delta table that records every run and batch, and
  pruning optional delivered cluster logs.

## Documentation

| If you want to | Read |
|---|---|
| Set up the framework in a workspace, or add an environment | [Environment Setup](docs/setup.md) |
| Add a new feed - initial POC | [New Feed Quick Start](docs/quick-start.md) |
| Configure a feed fully: streaming, compute, schedule, job groups, shared views, etc. | [Feed Configuration Guide](docs/feed-configuration.md) |
| Look up any setting: what it does, where it's set, what to set it to | [Parameters Reference](docs/parameters.md) |
| Deploy the bundle and run feeds | [Deployment](docs/deployment.md) |
| Our internal documentation about our own feeds, environments, infrastructure, and processes. (We maintain these docs.) | [Internal Documentation](docs/_pipelines/README.md) |
| Run a maintenance job: views, checkpoints, the connector wheel, the ES host check, the log table | [Maintenance Jobs](docs/maintenance-jobs.md) |
| Read a feed's run output, the monitoring log, or cluster logs | [Logging and Monitoring](docs/monitoring.md) |
| Keep feeds caught up without overloading Elasticsearch, at the lowest cluster cost | [Throughput Tuning](docs/throughput.md) |
| Re-send existing data to Elasticsearch | [How to Backfill](docs/backfill.md) |
| Fix a feed that fails, falls behind, or sends the wrong data | [Troubleshooting](docs/troubleshooting.md) |
| Take a framework update, run the tests, or change framework code | [Framework Maintenance and Upgrades](docs/framework-maintenance.md) |
| Understand why something works the way it does | [Technical Details](docs/technical-details.md) |

## Layout

The repo has two halves. `_pipelines/` holds what you edit for your own feeds, plus the job resources
generated from it; `framework/` holds the shared code that runs them and is not edited per feed.
`databricks.yml` at the root carries your environment's variables and targets. The docs split the same
way: `docs/_pipelines/` holds your own documentation, and the rest of `docs/` documents the framework.

```
databricks.yml                  Bundle definition: variables + targets (edit for your environment)
requirements.txt                Off-cluster tooling deps (pinned pyyaml for the generator)
docs/                           The framework documentation above, plus your own in _pipelines/
  _pipelines/                   YOU EDIT THIS: your own internal documentation (feeds, environments,
    README.md                   infrastructure, processes), starting from this index

You edit these, one pair per feed:
  _pipelines/
    pipeline_views/
      <view_name>.sql           The view: what gets exported (filename == view.name)
    pipeline_configs/
      <config_name>.yml         The config: view/source/reference locations, es_index_name, and
                                optional settings (mode, compute, schedule, ...)
    job_cluster_configs/
      <key>.yml                 OPTIONAL reusable new_cluster specs, referenced by key from a
                                config's compute.job_cluster_config
    resources/
      <config_name>.job.yml     GENERATED per-feed job (do not hand-edit; rerun the generator)
      group_<job_group>.job.yml GENERATED job for a job_group

Shared framework (not edited per feed):
  framework/
    notebooks/
      run_index_pipeline.py     Run by every feed job: installs the connector wheel, loads the feed's
                                config, and exports to Elasticsearch (batch or streaming)
      deploy_views.py           Renders each view's parameters from its config, then runs
                                CREATE OR REPLACE VIEW
      ...                       plus the maintenance notebooks (build_wheel, checkpoint_clear,
                                es_diagnostics, log_table_create, log_table_prune,
                                cluster_log_prune)
    pipeline_lib/               The shared library (used by the generator and the notebooks)
      config.py                 Loads and validates a feed config; the single source of truth for
                                config keys and run-time knobs
      monitoring_sink.py        Schema and row builders for the monitoring log
      log_table_migration.py    The log table's create-or-add-columns step
      stream_wait.py            The streaming wait loop, so a failed stream always fails its task
      cluster_log_prune.py      The allow-listed delete plan for the cluster logs prune job
    scripts/
      gen_jobs.py               Generates _pipelines/resources/*.job.yml from the configs
                                (--check guards drift)
    resources/                  The fixed maintenance jobs (hand-authored), by resource key:
      _build_wheel.job.yml        build_wheel
      _checkpoint_clear.job.yml   checkpoint_clear
      _deploy_views.job.yml       deploy_views
      _es_diagnostics.job.yml     es_diagnostics
      _log_table_create.job.yml   log_table_create
      _log_table_prune.job.yml    log_table_prune (daily; follows schedule_pause_status)
      _cluster_log_prune.job.yml  cluster_log_prune (daily; follows schedule_pause_status)
    tests/                      Offline unit tests: python -m pytest framework/tests

Config names and job_group names may not start with `_` (checked at generation), so they never collide
with the `_`-prefixed fixed jobs.
```

## License & Attribution

**Copyright © Databricks, Inc.** Developed and maintained by Databricks Forward Deployed Engineering. Available to support customers and the broader community in building Elasticsearch export pipelines on Databricks. For production support and customization, contact your Databricks account team.

---

**Built with 💜 by Databricks Forward Deployed Engineering**

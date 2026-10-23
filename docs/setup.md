# Environment Setup

How to set up the framework in a Databricks workspace, or add a new environment. For whoever installs and
configures it; once this is done, [New Feed Quick Start](quick-start.md) adds feeds. Follow the sections
in order. Each setting mentioned here is defined in the [Parameters Reference](parameters.md).

- [Prerequisites](#prerequisites)
- [Targets](#targets)
- [Fill in databricks.yml](#fill-in-databricksyml)
- [Defaults for the run-time knobs](#defaults-for-the-run-time-knobs)
- [Elasticsearch host configs](#elasticsearch-host-configs)
- [Per-developer checkpoints](#per-developer-checkpoints)
- [Per-developer views](#per-developer-views)
- [Optional: the monitoring log table](#monitoring-log-table)
- [Optional: failure emails, job clusters, cluster logs](#optional-failure-emails-job-clusters-cluster-logs)
- [First deploy](#first-deploy)

## Prerequisites

### Unity Catalog objects

- The catalogs and schemas your feeds use: the source and reference tables to read, and a schema to
  create the views in (each feed config names them).
- A UC Volume for the connector wheel, and one for streaming checkpoints if any feed streams (one volume
  can hold both).
- Optionally, a UC Volume path for a CA bundle (if ES uses a private CA) and a schema for the monitoring
  log table.

<a id="connector-wheel"></a>

### The connector wheel

Every feed job installs the [databricks-es-connector](/../../../databricks-es-connector)
wheel from the UC Volume path in [`wheel_path`](parameters.md#wheel_path). Keep the wheel's standard
filename (`databricks_es_connector-<version>-py3-none-any.whl`), which carries the version. The framework
requires connector **0.10.0 or later**: every feed run checks the installed version first and fails with
a clear message, before reading or sending anything, on an older wheel. Two ways to get it there:

- **Upload a wheel** you built or downloaded, with the Catalog Explorer or the Databricks CLI.
- **Build it in the workspace** with the `build_wheel` job. Put a copy of the connector repo in the
  workspace, set `wheel_path` for the target (the job writes to that path's folder), deploy, then run:

  ```bash
  databricks bundle run build_wheel -t <target> -p <profile> \
    --params repo_workspace_path=/Workspace/<path-to-connector-repo>,wheel_version=<version>
  ```

  `repo_workspace_path` is the folder holding the connector's `pyproject.toml`, and `wheel_version` must
  match the version in it. The job builds the wheel and adds it to the folder `wheel_path` points into. It
  never overwrites or repoints `wheel_path`: to use the new version, change `wheel_path` and redeploy.

### Secret scopes for the ES api_keys

Each Elasticsearch host config has its own api_key, so there is one api_key per host the feeds write to.
Each lives in a Databricks secret scope; the bundle stores only the scope and key names. Several keys can
share one scope, or each host can have its own. With the Databricks CLI, for each host:

```bash
databricks secrets create-scope <scope> -p <profile>            # once per scope
databricks secrets put-secret <scope> <key> -p <profile>        # prompts for that host's api_key value
```

The identity the jobs run as needs `READ` on every scope that holds one of the keys.

### Elasticsearch

- The target indices, created with their mappings before a feed first writes. With
  [`require_existing_index`](parameters.md#require_existing_index) on (the connector default), a feed
  fails if its index doesn't exist; with it off, Elasticsearch creates a missing index on the first write
  (unless the cluster disables auto-creation).

### Network access

The compute that runs the jobs (serverless, or your clusters) needs network access to each ES endpoint.

### Optional: a service principal to run the jobs

By default every job runs as the identity that deploys the bundle. For shared and production targets, a
service principal is the usual choice instead, so the jobs don't depend on one person's account or
access. Either deploy as the service principal (for example from CI), or set `run_as` for the target in
`databricks.yml`:

```yaml
targets:
  prd:
    run_as:
      service_principal_name: "<service-principal-application-id>"
```

With `run_as`, the identity that deploys needs the Service Principal User role on that service
principal. Whichever identity the jobs run as needs every grant in [Permissions](#permissions) below.

### Optional prerequisites

Only if you use the feature:

- **A cluster policy** for job-cluster feeds: its id goes in [`cluster_policy_id`](parameters.md#cluster_policy_id).
- **A support email address** or list for failure emails: [`support_email`](parameters.md#support_email).
- **A UC Volume for cluster logs**, if job clusters should keep their logs:
  [`cluster_log_volume_path`](parameters.md#cluster_log_volume_path).
- **A UC Volume for a CA bundle**, if ES uses a certificate the system CA store doesn't trust:
  [`ca_certs`](parameters.md#ca_certs).
- **A schema for the monitoring log table**: [`monitoring_log_table`](parameters.md#monitoring_log_table).

### Permissions

Jobs run as the identity that deploys the bundle, or as the `run_as` service principal if you set one.
That identity needs, in Unity Catalog terms:

| For | Needs |
|---|---|
| `deploy_views` | `USE CATALOG` and `USE SCHEMA` on the view schema, `CREATE TABLE` there (creating a view requires it), and `SELECT` on each source and reference table. With [per-developer views](#per-developer-views) on, also `CREATE SCHEMA` on the view catalog |
| Feed jobs | `SELECT` on each view, source and reference table; `READ VOLUME` on the wheel's volume (and the CA bundle's, if set); `READ VOLUME` and `WRITE VOLUME` on the checkpoint volume (streaming); `READ` on the secret scopes; `MODIFY` on the monitoring table (if the log is on) |
| `checkpoint_clear` | `READ VOLUME` and `WRITE VOLUME` on the checkpoint volume |
| `es_diagnostics` (a work in progress) | `READ` on the secret scopes (and `READ VOLUME` for the CA bundle, if set) |
| `build_wheel` | `CAN_READ` on the connector repo folder in the workspace, and `WRITE VOLUME` on the wheel's volume |
| `log_table_create` | `USE CATALOG`, `USE SCHEMA` and `CREATE TABLE` on the monitoring table's schema; to re-run it on a table another identity created, also `SELECT` and `MODIFY` on the table |
| `log_table_prune` | `MODIFY` on the monitoring table |
| `cluster_log_prune` | `READ VOLUME` and `WRITE VOLUME` on the cluster log volume |
| Cluster log delivery | `READ VOLUME` and `WRITE VOLUME` on the log volume, for the identity the job cluster runs as |

Every job also grants `CAN_MANAGE_RUN` to the workspace's `users` group, so anyone in the workspace can
start a run. The grant is part of each job definition, written by the generator and in the fixed job
files ([Who can run the jobs](deployment.md#who-can-run-the-jobs)). A run still executes as the job's
identity ([why that shapes the job parameters](technical-details.md#can-manage-run)).

### Local tools

- The [Databricks CLI](https://docs.databricks.com/dev-tools/cli/), with a profile for each workspace.
- Python with the generator's pinned dependency, from the repo root:

  ```bash
  pip install -r requirements.txt
  ```

<a id="targets"></a>

## Targets

`databricks.yml` defines three targets, chosen with `-t`:

- **`dev`** (the default) uses development mode: each deploying user gets their own copy, with resource
  names prefixed by the user name.
- **`stg`** and **`prd`** use production mode, with a shared deploy path per target.

All three take the workspace from your CLI profile (`-p <profile>`) or `DATABRICKS_HOST`. Whether
scheduled and continuous jobs run in a target is set per target in `databricks.yml` by
[`schedule_pause_status`](parameters.md#schedule_pause_status). The usual setup is paused in `dev` and
`stg`, so their triggers are deployed but don't start runs, and unpaused in `prd`, where the triggers
run.

## Fill in databricks.yml

Each target has a `variables` block for its environment-specific values. Fill in the targets you deploy
to; after that a routine deploy needs no `--var`. An empty value fails closed wherever it's required.

| Variable | Set it when |
|---|---|
| [`environment`](parameters.md#environment) | any feed config uses `${environment}` in a catalog or schema |
| [`wheel_path`](parameters.md#wheel_path) | always (every feed job installs the connector) |
| [`checkpoint_base_path`](parameters.md#checkpoint_base_path) | any feed streams |
| [`es_host_default`](parameters.md#es_host_default) | always: see the next section |
| [`ca_certs`](parameters.md#ca_certs) | ES uses a certificate the system CA store doesn't trust |
| [`cluster_policy_id`](parameters.md#cluster_policy_id) | any feed uses `job_cluster` compute |
| [`support_email`](parameters.md#support_email) | you want failure emails (usually `prd` only) |
| [`monitoring_log_table`](parameters.md#monitoring_log_table) | you use the monitoring log |

Two things to know:

- Values set elsewhere outrank the target block: `--var`, a `BUNDLE_VAR_<name>` environment variable, and
  the git-ignored `.databricks/bundle/<target>/variable-overrides.json`. If a committed value seems
  ignored, look for one of those ([precedence](parameters.md#bundle-variables)).
- Declare variables and targets in `databricks.yml` only, not in an included resource file. The generator
  reads `databricks.yml` alone.

## Defaults for the run-time knobs

The [run-time knobs](parameters.md#run-time-knobs) (`pipeline_mode`, `request_timeout`, `op_type`,
`bulk_stats`, `max_partition_bytes` and the rest) each have a bundle variable of the same name in
`databricks.yml`. That variable is the target-wide default: it applies to every feed in the target that
doesn't set the knob in its own config, and a `--params` override still beats both
([precedence](parameters.md#run-time-knobs-one-precedence-rule)). An empty value means the connector's or
Spark's own default.

Set a knob here when most feeds in a target should share a value, and leave it empty otherwise. Set it
at the top level for every target, or in one target's `variables` block:

```yaml
# databricks.yml
variables:
  pipeline_mode:
    default: batch                 # the mode most feeds use; the run fails if nothing sets it
  retry_transport_timeout:
    default: "true"                # recommended for every feed
targets:
  prd:
    variables:
      monitoring_log_enabled: "true"
```

Each knob's entry in the [Parameters Reference](parameters.md#run-time-knobs) says what to weigh, and
[Throughput Tuning](throughput.md) covers the write knobs together. Leave `filter_condition` empty: a
global filter applies to every feed that doesn't set its own.

<a id="elasticsearch-host-configs"></a>

## Elasticsearch host configs

A host config names an ES endpoint and where its api_key lives. The shipped one is
[`es_host_default`](parameters.md#es_host_default); fill in its three fields for each target:

```yaml
# databricks.yml
targets:
  dev:
    variables:
      es_host_default:
        es_host_url: "https://your-dev-es-host:9200"
        secret_scope_name: "es_dev"
        secret_key_name: "api_key"
  prd:
    variables:
      es_host_default:
        es_host_url: "https://your-prd-es-host:9200"
        secret_scope_name: "es_prd"
        secret_key_name: "api_key"
```

Feeds that don't set `es_host_config` write to the host named by
[`default_es_host_config`](parameters.md#default_es_host_config) (`es_host_default` out of the box). Only
the api_key value is secret, and it stays in the secret scope.

### Adding additional host configs

To send some feeds to other ES clusters, add a host config for each, all in `databricks.yml`:

1. Declare another complex variable with the same three fields, and give it values in each target. The
   file has a commented `es_host_secondary` example:

   ```yaml
   variables:
     es_host_secondary:
       type: complex
       default: {es_host_url: "", secret_scope_name: "", secret_key_name: ""}
   ```

2. Add its entry to [`es_host_configs_json`](parameters.md#es_host_configs_json), just below the host
   configs, so the `es_diagnostics` job can reach it. Set this once at the top level; the references pick
   up each target's values at deploy:

   ```yaml
   es_host_configs_json:
     default: >-
       {"es_host_default": {"es_host_url": "${var.es_host_default.es_host_url}",
       "secret_scope_name": "${var.es_host_default.secret_scope_name}",
       "secret_key_name": "${var.es_host_default.secret_key_name}"},
       "es_host_secondary": {"es_host_url": "${var.es_host_secondary.es_host_url}",
       "secret_scope_name": "${var.es_host_secondary.secret_scope_name}",
       "secret_key_name": "${var.es_host_secondary.secret_key_name}"}}
   ```

   The generator fails unless the map lists exactly the declared host configs, each referencing its own
   fields.

3. Point a feed at it with `es_host_config: es_host_secondary`.

Repeat the three steps for each further host config, and add a secret for each host's api_key
([Secret scopes](#secret-scopes-for-the-es-api_keys)).

### Keeping endpoints out of git

To avoid committing even the endpoints, put the per-target host values in the git-ignored
`.databricks/bundle/<target>/variable-overrides.json` instead:

```json
{
  "es_host_default": {
    "es_host_url": "https://your-dev-es-host:9200",
    "secret_scope_name": "es_dev",
    "secret_key_name": "api_key"
  }
}
```

The bundle applies it at deploy; the generator never reads it. So put only the host configs' values
there, never `default_es_host_config` or `es_host_configs_json`, which the generator checks in
`databricks.yml`.

<a id="per-developer-checkpoints"></a>

## Per-developer checkpoints

Development mode keeps each developer's files and jobs separate, but not data in UC Volumes, so two
developers streaming the same feed in `dev` would share one checkpoint. Give each their own by appending
the deploying user's name in the `dev` target:

```yaml
targets:
  dev:
    variables:
      checkpoint_base_path: /Volumes/<catalog>/<schema>/<volume>/checkpoints/${workspace.current_user.short_name}
```

The bundle resolves `${workspace.current_user.short_name}` at deploy, and the runner adds `/<config_name>`
for each feed.

<a id="per-developer-views"></a>

## Per-developer views

Views live in Unity Catalog, so without this two developers deploying to one `dev` workspace would
overwrite each other's views, and one could fail to replace a view the other owns. With
[`view_schema_per_user`](parameters.md#view_schema_per_user) set to `true` (the `dev` target's
setting), `deploy_views` creates every view in `<view schema>_<your short name>`, lowercased (for example
`es_poc_jane_doe`), creating the schema if it's missing, and every feed job reads its view from there.
Each developer owns their own copy.

- Only the view moves. Source and reference tables keep their configured names, so every developer
  reads the same inputs.
- It needs `CREATE SCHEMA` on the view catalog (see [Permissions](#permissions)).
- To test against the real view schema for one deploy, turn it off:
  `databricks bundle deploy -t dev -p <profile> --var view_schema_per_user=false`, then run
  `deploy_views`. The next deploy without the flag turns it back on.
- With it on, a missing or illegal user name fails `deploy_views` and the feed runs, rather than
  falling back to the shared schema.
- If a per-user schema would be one of the configured schemas (a view, source or reference table schema,
  for example view schema `es` and user `poc` against a feed whose view schema is `es_poc`),
  `deploy_views` fails before deploying. It also fails when a config outside a `views` subset run can't
  be resolved, since its schemas can't be checked; a broken config inside the subset fails only its own
  view.
- If the per-user schema already exists, it must be owned by you. One owned by anyone else (a shared
  schema with a matching name, or another developer's whose name happens to concatenate the same, such as
  `es` + `poc_jane` and `es_poc` + `jane`) fails that view instead of deploying into it.
- It assumes the jobs run as the developer who deployed them (the `dev` default; no `run_as`), since the
  schema is named after the deployer and the ownership check uses the identity the job runs as.
- The collision check covers schemas the feed configs name, and the ownership check covers schemas owned
  by anyone else. A schema you own that no config names (for example the monitoring log table's) isn't
  detected, so don't give it a name of the form `<view schema>_<your short name>`.
- A service principal's short name (a CI deploy) is usually not a legal schema name, so deploy to `dev`
  as a service principal with `--var view_schema_per_user=false`.

Developers still share the Elasticsearch indices their feeds write and the
[monitoring log table](#monitoring-log-table). The log is safe to share (every row carries its job and
task run ids); two developers writing one index are not, so agree who runs which feed, or point dev
feeds at separate indices.

<a id="monitoring-log-table"></a>

## Optional: the monitoring log table

The monitoring log records every run and batch in a Delta table. To use it:

1. Set [`monitoring_log_table`](parameters.md#monitoring_log_table) to a `catalog.schema.table` name in
   each target that uses it, and deploy.
2. Create the table once (re-running is safe):

   ```bash
   databricks bundle run log_table_create -t <target> -p <profile>
   ```

3. Turn it on with [`monitoring_log_enabled`](parameters.md#monitoring_log_enabled), for the whole target,
   for one feed, or for a single run. When it's on, a missing or unwritable table fails the run.
4. The `log_table_prune` job keeps the table to
   [`monitoring_log_retention_days`](parameters.md#monitoring_log_retention_days). It runs daily, paused
   or unpaused by the target's `schedule_pause_status` like every other trigger.

The prune job fails when `monitoring_log_table` is empty. So in any target where triggers are unpaused but
the monitoring log isn't used, pause `log_table_prune`; otherwise it fails, and emails `support_email`,
every day.

How to read the table is in [Logging and Monitoring](monitoring.md#monitoring-log).

## Optional: failure emails, job clusters, cluster logs

- **Failure emails.** Set [`support_email`](parameters.md#support_email) (a list) in the targets that
  should page someone, usually only production. Skipped and canceled runs don't email.
- **Job clusters.** If any feed uses `job_cluster` compute, set
  [`cluster_policy_id`](parameters.md#cluster_policy_id) in each target it deploys to.
- **Cluster log delivery.** To keep job-cluster logs after the cluster ends, see
  [Cluster log delivery](monitoring.md#cluster-log-delivery). The `cluster_log_prune` job deletes them
  after [`cluster_log_retention_days`](parameters.md#cluster_log_retention_days) (default 30).

## First deploy

Run the [deploy sequence](deployment.md#the-deploy-sequence) from the repo root, the same one every
later deploy uses:

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

The deploy creates the feed jobs and the seven maintenance jobs (`build_wheel`, `deploy_views`,
`checkpoint_clear`, `es_diagnostics`, `log_table_create`, `log_table_prune`, `cluster_log_prune`). Commit the files the
generator writes under `_pipelines/resources/`. Check the feed's output as in the
[quick start](quick-start.md#verify).

If you have CI, run `python framework/scripts/gen_jobs.py --check` there: it changes nothing and fails if
a config was edited without regenerating its job.

Next: [add a feed](quick-start.md), or see [Deployment](deployment.md) for running feeds day to day.

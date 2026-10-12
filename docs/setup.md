# Environment Setup

How to set up the framework in a Databricks workspace, or add a new environment. For whoever installs and
configures it; once this is done, [New Feed Quick Start](quick-start.md) adds feeds. Follow the sections
in order. Each setting mentioned here is defined in the [Parameters Reference](parameters.md).

- [Prerequisites](#prerequisites)
- [Targets](#targets)
- [Fill in databricks.yml](#fill-in-databricksyml)
- [Elasticsearch host configs](#elasticsearch-host-configs)
- [Per-developer checkpoints](#per-developer-checkpoints)
- [Optional: the monitoring log table](#monitoring-log-table)
- [Optional: failure emails, job clusters, cluster logs](#optional-failure-emails-job-clusters-cluster-logs)
- [First deploy](#first-deploy)

## Prerequisites

### Unity Catalog objects

- The catalogs and schemas your feeds use: the source and reference tables to read, and a schema to
  create the views in (each feed config names them).
- A UC Volume for the connector wheel, and one for streaming checkpoints if any feed streams (one volume
  can hold both).
- Optionally, a UC Volume path for a CA bundle (if ES uses a private CA), a volume for cluster logs, and a
  schema for the monitoring log table.

<a id="connector-wheel"></a>

### The connector wheel

Every feed job installs the [databricks-es-connector](https://github.com/tim-clifford/es-databricks-connector)
wheel from the UC Volume path in [`wheel_path`](parameters.md#wheel_path). Keep the wheel's standard
filename (`databricks_es_connector-<version>-py3-none-any.whl`), which carries the version. Two ways to get
it there:

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

Each Elasticsearch host's api_key lives in a Databricks secret scope; the bundle stores only the scope and
key names. With the Databricks CLI:

```bash
databricks secrets create-scope <scope> -p <profile>
databricks secrets put-secret <scope> <key> -p <profile>        # prompts for the api_key value
```

The identity the jobs run as needs `READ` on the scope.

### Elasticsearch

- The target indices, created with their mappings before a feed first writes. By default a feed fails if
  its index doesn't exist ([`require_existing_index`](parameters.md#require_existing_index)).
- Network access from the compute that runs the jobs (serverless, or your clusters) to each ES endpoint.

### Permissions

Jobs run as the identity that deploys the bundle (no job sets a different run-as). That identity needs,
in Unity Catalog terms:

| For | Needs |
|---|---|
| `deploy_views` | `USE CATALOG` and `USE SCHEMA` on the view schema, `CREATE TABLE` there (creating a view requires it), and `SELECT` on each source and reference table |
| Feed jobs | `SELECT` on each view, source and reference table; `READ VOLUME` on the wheel's volume (and the CA bundle's, if set); `READ VOLUME` and `WRITE VOLUME` on the checkpoint volume (streaming); `READ` on the secret scopes; `MODIFY` on the monitoring table (if the log is on) |
| `checkpoint_clear` | `READ VOLUME` and `WRITE VOLUME` on the checkpoint volume |
| `es_diagnostics` | `READ` on the secret scopes (and `READ VOLUME` for the CA bundle, if set) |
| `build_wheel` | read access to the connector repo folder in the workspace, and `WRITE VOLUME` on the wheel's volume |
| `log_table_create` | `USE CATALOG`, `USE SCHEMA` and `CREATE TABLE` on the monitoring table's schema |
| `log_table_prune` | `MODIFY` on the monitoring table |
| Cluster log delivery | `READ VOLUME` and `WRITE VOLUME` on the log volume, for the identity the job cluster runs as |

Every job grants `CAN_MANAGE_RUN` to the `users` group, so anyone in the workspace can start a run. A run
still executes as the job's identity ([why that shapes the job parameters](technical-details.md#can-manage-run)).

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

All three take the workspace from your CLI profile (`-p <profile>`) or `DATABRICKS_HOST`, so a target can
point at any workspace. Scheduled and continuous jobs deploy paused in `dev` and `stg` and fire only in
`prd` ([`schedule_pause_status`](parameters.md#schedule_pause_status)).

## Fill in databricks.yml

Each target's `variables` block ships with empty placeholders. Fill in the targets you deploy to; after
that a routine deploy needs no `--var`. An empty value fails closed wherever it's required.

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

### Adding another host config

To send some feeds to a second ES cluster, all in `databricks.yml`:

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
   [`monitoring_log_retention_days`](parameters.md#monitoring_log_retention_days). It runs daily, on the
   same pause setting as every scheduled job, so it fires in `prd` and stays paused in `dev` and `stg`
   unless you unpause it.

The prune job fails when `monitoring_log_table` is empty, and in `prd` its schedule is live. So if you
deploy to `prd` without the monitoring log, pause `log_table_prune` there; otherwise it fails, and emails
`support_email`, every day.

How to read the table is in [Operations](operations.md#monitoring-log).

## Optional: failure emails, job clusters, cluster logs

- **Failure emails.** Set [`support_email`](parameters.md#support_email) (a list) in the targets that
  should page someone, usually only `prd`. Skipped and canceled runs don't email.
- **Job clusters.** If any feed uses `job_cluster` compute, set
  [`cluster_policy_id`](parameters.md#cluster_policy_id) in each target it deploys to.
- **Cluster log delivery.** To keep job-cluster logs after the cluster ends, see
  [Cluster log delivery](operations.md#cluster-log-delivery).

## First deploy

From the repo root:

```bash
python framework/scripts/gen_jobs.py                       # generate a job per feed config
databricks bundle validate -t <target> -p <profile>
databricks bundle deploy -t <target> -p <profile>
databricks bundle run deploy_views -t <target> -p <profile>
```

The deploy creates the feed jobs and the six maintenance jobs (`build_wheel`, `deploy_views`,
`checkpoint_clear`, `es_diagnostics`, `log_table_create`, `log_table_prune`). Commit the files the
generator writes under `_pipelines/resources/`.

Then run one feed and check its output, as in the [quick start](quick-start.md#verify).

If you have CI, run `python framework/scripts/gen_jobs.py --check` there: it changes nothing and fails if
a config was edited without regenerating its job.

Next: [add a feed](quick-start.md), or see [Operations](operations.md) for running feeds day to day.

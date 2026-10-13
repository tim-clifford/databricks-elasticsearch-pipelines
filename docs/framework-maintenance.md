# Framework Maintenance and Upgrades

How to take a new framework release, change code under `framework/`, run the tests, and move to a new connector version. For whoever owns the deployed copy of this repo.

## What's in framework/ vs _pipelines/

`_pipelines/` holds your feeds (views, configs, job cluster specs, and the job resources generated from them); `framework/` holds the shared code that runs them and is not edited per feed. `databricks.yml` at the root is yours too: it carries your environment's values. For the complete directory layout, see [README.md#layout](../README.md#layout).

## Taking a new release

When a new framework release is published, upgrade your deployed copy in these steps:

1. **Replace `framework/`.** Download the new release and replace the entire `framework/` directory with the one from the release. Keep your `_pipelines/`, `databricks.yml`, and any `.databricks/bundle/<target>/variable-overrides.json` edits intact.
2. **Check the new `databricks.yml`.** The release ships a new `databricks.yml` with its own variable declarations and targets. Compare it against your current one: look for new variables (especially runtime knobs) and add them to your targets, or to the top-level `default` if you want all targets to inherit the same value.
3. **Reinstall dependencies.** Run `pip install -r requirements.txt` to pin the new pyyaml version (see [Checking generated jobs](#checking-generated-jobs) for why the pin matters).
4. **Regenerate job resources.** Run `python framework/scripts/gen_jobs.py` to regenerate `_pipelines/resources/*.job.yml` with any new framework changes.
5. **Verify generation.** Run `python framework/scripts/gen_jobs.py --check` and confirm it reports no stale or orphaned files.
6. **Run the test suite.** Execute `python -m pytest framework/tests` and verify all tests pass (see [Running the tests](#running-the-tests) for details).
7. **Validate the bundle.** Run `databricks bundle validate -t <target>` for each target you deploy to.
8. **Deploy to dev first.** Deploy to your dev target (`databricks bundle deploy -t dev`), run a few feeds, and confirm they export data as expected.
9. **Deploy to production.** Once dev validates, deploy to your other targets. Job resource keys don't change between releases unless a release note says so, so each job is updated in place and keeps its run history.

### Release notes

Each release adds a note here. Read them before upgrading to understand what changed and whether your deployment needs any follow-up work.

#### Upgrading from earlier builds

Earlier builds kept `notebooks/`, `pipeline_lib/`, `scripts/`, `tests/` and every job resource (fixed and generated) at the repo root, named the shipped host config `es_host_primary`, and listed the hosts for the `es_diagnostics` job inside its job file. To move an existing copy to this build:

1. Replace those root folders with `framework/` from this build. `_pipelines/pipeline_configs/`, `_pipelines/pipeline_views/` and `_pipelines/job_cluster_configs/` carry over unchanged.
2. In `databricks.yml`, replace the `resources/*.yml` include with the two lines this build ships: `framework/resources/*.yml` and `_pipelines/resources/*.yml` (the generator fails closed until both are listed and the old one is gone).
3. Rename the host config `es_host_primary` to `es_host_default` everywhere you set it: its declaration and each target's values in `databricks.yml`, the top-level `default_es_host_config` value, the host config's key in any `.databricks/bundle/<target>/variable-overrides.json` (where only the host values belong, never `default_es_host_config` or `es_host_configs_json`), and every feed config with `es_host_config: es_host_primary`. (Or keep your own name: the shipped one is only a default.) Then add the `es_host_configs_json` variable this build ships, with one entry per host config you declare (see [Parameters Reference](parameters.md#elasticsearch-host-configs)).
   Within `databricks.yml` the generator fails closed on a feed naming an undeclared host, a stale `default_es_host_config` and a map that doesn't match the declared hosts; it doesn't read `variable-overrides.json`, so check that file yourself.
4. Delete the old root `resources/` folder (the generator refuses to run while it exists; move a job of your own to `_pipelines/resources/`, where the generator leaves it alone) and run `python framework/scripts/gen_jobs.py`, which writes the per-index jobs into `_pipelines/resources/`, creating the folder if needed. (Generated files you moved there yourself are recognized and regenerated.) `--check` should then report every generated file up to date.
5. Deploy. Job resource keys are unchanged, so each job is updated in place (same job id, same run history) and only its notebook path changes. Streaming checkpoints live under `checkpoint_base_path`, not in the repo, so a streaming feed resumes where it left off.

## Monitoring log table changes

When the monitoring log schema evolves (new columns added to track additional state), existing deployments must be upgraded before deploying a build that writes those columns.

### Upgrading an existing log table

When a new framework build adds columns to the monitoring log, re-run the `log_table_create` job before deploying the build. The job is re-runnable: it adds any column the new schema has and the table lacks, without dropping or replacing the table, so existing rows are preserved and receive `NULL` for new columns.

Run the job with:

```bash
databricks bundle run log_table_create -t <target> -p <profile>
```

A build that writes a column the table lacks will fail its first append (`run_start` before any data moves); this upgrade step prevents that failure.

### Adding a column later

The `log_table_create` job uses re-runnable migration logic (in `framework/pipeline_lib/log_table_migration.py`). To add a field:

1. Add the column definition to `MONITORING_TABLE_COLUMNS` in `framework/pipeline_lib/monitoring_sink.py`.
2. Re-run `log_table_create` before deploying the build that writes the new column.
3. The job adds the column via `ALTER TABLE ADD COLUMNS` (a Delta metadata commit), preserving all existing rows.

Note: `ALTER TABLE ADD COLUMNS` can transiently conflict with a log append in flight; such a task fails once (fail-closed) and the task's retry policy restarts it. The job never drops a column it does not know; an unknown column is warned about and left alone.

## Running the tests

The framework includes an offline unit test suite. Run it to verify a code change or new release does not break existing behavior.

### Running the test command

From the repo root, run:

```bash
python -m pytest framework/tests -q
```

The `-q` flag shows a summary (pass count) rather than per-test output. All tests must pass; do not deploy if any test fails.

### Prerequisites

A virtualenv with the generator's pinned dependency and pytest (`requirements.txt` doesn't include pytest):

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest
```

### Expected output

A successful run reports all tests passed with no failures. The count changes as the framework evolves; do not hard-code it into expectations. The full test run completes in a few seconds.

### Python version compatibility

Every Python file in the repo must parse on Python 3.11, the Python of Databricks Runtime 15.3, the oldest runtime the framework supports. `framework/tests/test_python_compat.py` fails on syntax newer Pythons accept but 3.11 rejects, so the suite catches it even when you run it on a newer Python.

## Checking generated jobs

The generator (`framework/scripts/gen_jobs.py`) creates the per-feed job resources from your feed configs. Running it regenerates those files; the `--check` flag verifies they are up to date.

### What `--check` verifies

`--check` exits with status 0 if:

- Every config in `_pipelines/pipeline_configs/` has a corresponding generated file in `_pipelines/resources/<config_name>.job.yml`.
- Every generated file matches the current output of the generator (no stale files from config edits that were not regenerated).
- No orphaned generated files exist (files left behind by deleted or renamed configs).

If any check fails, `--check` exits non-zero and lists the problems.

### Never hand-edit generated files

The files `_pipelines/resources/<config_name>.job.yml` and `_pipelines/resources/group_<job_group>.job.yml` are generated. Do not hand-edit them; changes will be overwritten on the next regeneration. Always edit the source config in `_pipelines/pipeline_configs/<config_name>.yml` and regenerate.

### The pyyaml pin

The generator depends on `pyyaml`, pinned in `requirements.txt`. The pin is load-bearing: `--check` byte-compares the committed generated files against fresh `yaml.safe_dump` output, and formatting can differ across pyyaml versions. A version mismatch surfaces as a false "stale files" failure even though the YAML is semantically identical. For details on why this matters, see [Technical Details](technical-details.md#pyyaml-pin).

When you upgrade the framework, always reinstall dependencies (`pip install -r requirements.txt`) to pin the correct pyyaml version.

### Recommending `--check` in CI

Add `python framework/scripts/gen_jobs.py --check` to your CI pipeline to catch config edits that were not regenerated before they reach production.

## Changing framework code

If you modify code under `framework/`, follow this procedure to ensure your changes do not break existing behavior and integrate cleanly with the deployed framework.

1. **Run the tests.** Execute `python -m pytest framework/tests` and verify all tests pass (see [Running the tests](#running-the-tests) for the exact command).
2. **Run `--check`.** Execute `python framework/scripts/gen_jobs.py --check` to verify no generated files are stale.
3. **Validate the bundle.** Run `databricks bundle validate -t <target>` for each target.
4. **Deploy to dev first.** Deploy to your dev target and run a few feeds to confirm the change works as expected.
5. **Deploy to production.** Once dev validates, deploy to production.

### Config names and job_group names

Config names and `job_group` names may not start with `_` (checked at generation), so they never collide with the `_`-prefixed fixed jobs in `framework/resources/` (for example `_checkpoint_clear.job.yml`, shown as `_checkpoint clear`). Those fixed jobs change only with framework releases.

## Updating the connector

The connector wheel is versioned separately from the framework. To deploy a new connector version, build or obtain the new wheel and update your deployment configuration.

### Building or uploading the wheel

See [setup.md#connector-wheel](setup.md#connector-wheel) for instructions on building the connector from source or uploading a pre-built wheel to your workspace.

### Updating wheel_path per target

In `databricks.yml`, set [`wheel_path`](parameters.md#wheel_path) for each target to the new wheel. Keep the wheel's standard filename, which carries the version. Targets can run different versions, for example to try a new one in `dev` first:

```yaml
targets:
  dev:
    variables:
      wheel_path: /Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<new-version>-py3-none-any.whl
  prd:
    variables:
      wheel_path: /Volumes/<catalog>/<schema>/<volume>/databricks_es_connector-<current-version>-py3-none-any.whl
```

### Redeploying with the new wheel

Run `databricks bundle deploy -t <target>` to deploy the bundle with the new `wheel_path`. The next run of any job will use the new wheel.

### Version-gated knobs

Some knobs need a minimum connector version (`bulk_stats`, `retry_transport_timeout`, `op_type`, `bypass_fast_path`; see each in the [Parameters Reference](parameters.md)). On an older wheel the run doesn't fail: it drops the knob, logs `WARNING: installed connector (databricks_es_connector <version>) has no EsWriteConfig.<knob> field; dropping <knob>=<value> (requires <version>). The export proceeds WITHOUT it.`, and continues. If you see that warning, move to a newer wheel or stop setting the knob. The list lives in `_VERSION_GATED_KNOBS` in `framework/notebooks/run_index_pipeline.py`.

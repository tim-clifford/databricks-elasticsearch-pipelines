# Framework Maintenance and Upgrades

How to take a new framework release, change code under `framework/`, run the tests, and move to a new connector version. For whoever owns the deployed copy of this repo.

In normal operation nobody changes the code under `framework/`: feeds are added and changed entirely in `_pipelines/` and `databricks.yml`. If you need a change to the framework itself, the recommended route is to ask Databricks, who maintain it. If you do make framework changes yourself, this guide is the process to follow, and it is the same process Databricks follows for its own changes.

## The upstream repo and your copy

The framework is maintained by Databricks in an upstream source repo. Your deployment runs from your own copy of it, with your feeds in `_pipelines/` and your environments in `databricks.yml`.

- **Make framework changes upstream first.** Change and test them in the upstream repo, then bring the new release into your copy with [Taking a new release](#taking-a-new-release), following your organization's own policies for changes and deployments.
- **Track any change made only in your copy.** A change to `framework/` that is not also made upstream is overwritten the next time you replace `framework/` with a release. Keep a record of each one (a commit you can find again, or a patch), compare it against every new release before you take it, and re-apply or retire it. Until a local change is upstream, every release has to be checked for conflicts with it.

## What's in framework/ vs _pipelines/

`_pipelines/` holds your feeds (views, configs, job cluster specs, and the job resources generated from them); `framework/` holds the shared code that runs them and is not edited per feed. `databricks.yml` at the root is yours too: it carries your environment's values. The docs follow the same split: [`docs/_pipelines/`](_pipelines/README.md) holds your own documentation (your feeds, environments, and infrastructure), and the rest of `docs/` documents the framework. For the complete directory layout, see [README.md#layout](../README.md#layout).

## Taking a new release

When a new framework release is published, upgrade your deployed copy in these steps:

1. **Check for local changes.** If your copy has framework changes that aren't upstream ([above](#the-upstream-repo-and-your-copy)), compare them against the release and decide how each carries over.
2. **Replace `framework/` and the framework docs.** Replace the entire `framework/` directory with the one from the release, and the framework docs (`README.md` and everything in `docs/` except `docs/_pipelines/`) with the release's. Keep your `_pipelines/`, `docs/_pipelines/`, `databricks.yml`, and any `.databricks/bundle/<target>/variable-overrides.json` edits intact.
3. **Check the new `databricks.yml`.** The release comes with its own `databricks.yml`, with its variable declarations and targets. Compare it against your current one: look for new variables (especially run-time knobs) and add them to your targets, or to the top-level `default` if you want all targets to inherit the same value.
4. **Reinstall dependencies.** Run `pip install -r requirements.txt` to pin the new pyyaml version (see [The pyyaml pin](#the-pyyaml-pin) for why the pin matters).
5. **Regenerate job resources.** Run `python framework/scripts/gen_jobs.py` to regenerate `_pipelines/resources/*.job.yml` with any new framework changes.
6. **Verify generation.** Run `python framework/scripts/gen_jobs.py --check` and confirm it reports no stale or orphaned files.
7. **Run the test suite.** Execute `python -m pytest framework/tests` and verify all tests pass (see [Running the tests](#running-the-tests) for details).
8. **Validate the bundle.** Run `databricks bundle validate -t <target>` for each target you deploy to.
9. **Deploy to dev first.** Deploy to your dev target (`databricks bundle deploy -t dev`). If the release adds monitoring log columns and the log is on, [upgrade the log table](#upgrading-an-existing-log-table) right after this deploy. Run a few feeds and confirm they export data as expected.
10. **Deploy to production.** Once dev validates, deploy to your other targets, upgrading the log table in each the same way. Job resource keys don't change between releases unless the release says so, so each job is updated in place and keeps its run history.

## Monitoring log table changes

When the monitoring log schema evolves (new columns added to track additional state), the table in each target has to gain those columns before the new build's feeds write to it.

### Upgrading an existing log table

The `log_table_create` job takes the schema from the deployed framework code, so it can only add a new build's columns after that build is deployed. Upgrade each target in this order:

1. **Deploy the build.**
2. **Run `log_table_create` straight away**, before the feeds run again:

   ```bash
   databricks bundle run log_table_create -t <target> -p <profile>
   ```

   The job is re-runnable: it adds any column the new schema has and the table lacks, without dropping or replacing the table, so existing rows are preserved and receive `NULL` for new columns.

A feed run that starts between the two steps writes a column the table lacks and fails at its first log write (`run_start`, before any data is sent); the next scheduled run, or a continuous job's retry, then succeeds. To avoid even those failures, deploy the build with its triggers paused (`databricks bundle deploy -t <target> -p <profile> --var=schedule_pause_status=PAUSED`), run `log_table_create`, then deploy again without the `--var` to restore the target's pause state. A feed that sets its own `pause_status: UNPAUSED` stays unpaused through the first deploy, so check for those first. Between the two deploys every inherited trigger is paused, so continuous feeds stop and `log_table_prune` doesn't run, and a run started by hand in that window still fails at its first log write.

### Adding a column later

The `log_table_create` job uses re-runnable migration logic (in `framework/pipeline_lib/log_table_migration.py`). To add a field:

1. Add the column definition to `MONITORING_TABLE_COLUMNS` in `framework/pipeline_lib/monitoring_sink.py`.
2. Deploy the build, then re-run `log_table_create` before the feeds run again ([above](#upgrading-an-existing-log-table)).
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

Python with the generator's pinned dependency and pytest (`requirements.txt` doesn't include pytest):

```bash
pip install -r requirements.txt pytest
```

A virtual environment is optional. It keeps the pinned pyyaml from changing other Python tools on the machine, for example:

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

The generator depends on `pyyaml`, pinned in `requirements.txt`. `--check` depends on the pin: it byte-compares the committed generated files against fresh `yaml.safe_dump` output, and formatting can differ across pyyaml versions. A version mismatch surfaces as a false "stale files" failure even though the YAML is semantically identical. For details on why this matters, see [Technical Details](technical-details.md#pyyaml-pin).

When you upgrade the framework, always reinstall dependencies (`pip install -r requirements.txt`) to pin the correct pyyaml version.

### Recommending `--check` in CI

Add `python framework/scripts/gen_jobs.py --check` to your CI pipeline to catch config edits that were not regenerated before they reach production.

## Changing framework code

If you modify code under `framework/` (upstream first, where you can: see [The upstream repo and your copy](#the-upstream-repo-and-your-copy)), follow this procedure to ensure your changes do not break existing behavior and integrate cleanly with the deployed framework.

1. **Run the tests.** Execute `python -m pytest framework/tests` and verify all tests pass (see [Running the tests](#running-the-tests) for the exact command).
2. **Run `--check`.** Execute `python framework/scripts/gen_jobs.py --check` to verify no generated files are stale.
3. **Validate the bundle.** Run `databricks bundle validate -t <target>` for each target.
4. **Deploy to dev first.** Deploy to your dev target and run a few feeds to confirm the change works as expected.
5. **Deploy to production.** Once dev validates, deploy to production.

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

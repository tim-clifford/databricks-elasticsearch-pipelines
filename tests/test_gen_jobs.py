"""Offline unit tests for scripts/gen_jobs.py: the compute-aware job rendering and the reusable
job-cluster spec loader. No cluster, no bundle: render to YAML text and assert the parsed structure.
"""
import os
import sys

import pytest
import yaml

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO_ROOT, "scripts"))

import gen_jobs  # noqa: E402
from pipeline_lib.config import validate_config  # noqa: E402


def _cfg(compute=None, schedule=None):
    """A minimal validated config, optionally with a compute and/or schedule block."""
    raw = {
        "es_index_name": "ecs-dns-activity",
        "es_id_field": "dsl_id",
        "es_host_config": "es_host_primary",
        "pipeline_mode": "batch",
        "view": {"catalog": "cat", "schema": "es_poc", "name": "ecs_dns_activity"},
        "source": {"catalog": "cat", "schema": "ocsf", "table": "dns_activity", "primary_key": "dsl_id"},
    }
    if compute is not None:
        raw["compute"] = compute
    if schedule is not None:
        raw["schedule"] = schedule
    return validate_config(raw)


def _render_job(cfg, spec=None):
    """Render and parse a job resource; return the single job dict under resources.jobs."""
    text = gen_jobs.render_job_yaml("ecs_dns_activity.yml", "ecs_dns_activity", cfg, spec)
    assert text.startswith(gen_jobs._GENERATED_MARKER)  # header preserved
    parsed = yaml.safe_load(text)
    jobs = parsed["resources"]["jobs"]
    assert list(jobs) == ["index_pipeline_ecs_dns_activity"]
    return jobs["index_pipeline_ecs_dns_activity"]


# --------------------------------------------------------------------------- render: serverless


def test_render_job_name_uses_config_name_not_es_index():
    # The job display name is keyed on the config NAME (the resource-key stem), not es_index_name, so
    # a job lines up with the config you edit/deploy. es_index_name differs here ("ecs-dns-activity")
    # to prove the name follows the config name; the ES index still appears in the description.
    job = _render_job(_cfg())
    assert job["name"] == "[${bundle.target}] databricks-elasticsearch-pipelines: ecs_dns_activity"
    assert "ecs-dns-activity" in job["description"]  # es_index_name still named in the description


def test_render_serverless_has_no_cluster_block():
    job = _render_job(_cfg())  # default serverless
    task = job["tasks"][0]
    assert "existing_cluster_id" not in task
    assert "job_cluster_key" not in task
    assert "job_clusters" not in job
    assert "notebook_task" in task


def test_render_wires_es_host_config_fields():
    # The generated notebook task references the pipeline's es_host_config as complex-variable subfields
    # (${var.<name>.es_host_url} etc.), so the bundle resolves the right host per target at deploy. Use a
    # non-default host-config name to prove the ref follows the config value, not a hardcoded literal.
    cfg = validate_config({
        "es_index_name": "ecs-dns-activity", "es_id_field": "dsl_id", "es_host_config": "es_host_secondary",
        "pipeline_mode": "batch", "view": {"catalog": "c", "schema": "s", "name": "v"},
        "source": {"catalog": "c", "schema": "s", "table": "t", "primary_key": "dsl_id"},
    })
    bp = _render_job(cfg)["tasks"][0]["notebook_task"]["base_parameters"]
    assert bp["es_host_url"] == "${var.es_host_secondary.es_host_url}"
    assert bp["secret_scope_name"] == "${var.es_host_secondary.secret_scope_name}"
    assert bp["secret_key_name"] == "${var.es_host_secondary.secret_key_name}"


def test_render_all_jobs_max_concurrent_runs_1():
    for compute, spec in (
        (None, None),
        ({"type": "existing_cluster", "existing_cluster_id": "0123-x"}, None),
        ({"type": "job_cluster", "job_cluster_config": "std"}, {"spark_version": "15.4.x-scala2.12", "num_workers": 1}),
    ):
        assert _render_job(_cfg(compute), spec)["max_concurrent_runs"] == 1


# --------------------------------------------------------------------------- render: existing_cluster


def test_render_existing_cluster():
    job = _render_job(_cfg({"type": "existing_cluster", "existing_cluster_id": "0123-456789-abcde"}))
    task = job["tasks"][0]
    assert task["existing_cluster_id"] == "0123-456789-abcde"
    assert "job_clusters" not in job
    assert "job_cluster_key" not in task
    # the cluster ref precedes notebook_task in the task (deterministic key order)
    assert list(task) == ["task_key", "existing_cluster_id", "notebook_task"]


# --------------------------------------------------------------------------- render: job_cluster


def test_render_job_cluster_inlines_spec():
    spec = {"spark_version": "15.4.x-scala2.12", "node_type_id": "m5d.large", "num_workers": 2}
    job = _render_job(_cfg({"type": "job_cluster", "job_cluster_config": "standard_batch"}), spec)
    # The spec is inlined verbatim, PLUS the injected per-environment policy_id bundle-variable ref and
    # apply_policy_default_values (so the policy's own defaults fill omitted cluster attrs at deploy).
    expected_new_cluster = {**spec, "policy_id": "${var.cluster_policy_id}", "apply_policy_default_values": True}
    assert job["job_clusters"] == [{"job_cluster_key": "standard_batch", "new_cluster": expected_new_cluster}]
    # The caller's loaded spec dict must NOT be mutated (render builds a copy); policy_id is not added to it.
    assert "policy_id" not in spec
    task = job["tasks"][0]
    assert task["job_cluster_key"] == "standard_batch"
    assert "existing_cluster_id" not in task
    # job_clusters is emitted before tasks (deterministic key order)
    keys = list(job)
    assert keys.index("job_clusters") < keys.index("tasks")


def test_render_job_cluster_injects_policy_id_var():
    # Every job cluster gets policy_id bound to the cluster_policy_id bundle variable, so the target's
    # single cluster policy is applied at deploy (not hardcoded per config).
    spec = {"spark_version": "15.4.x-scala2.12", "num_workers": 1}
    job = _render_job(_cfg({"type": "job_cluster", "job_cluster_config": "std"}), spec)
    nc = job["job_clusters"][0]["new_cluster"]
    assert nc["policy_id"] == "${var.cluster_policy_id}"
    # apply_policy_default_values lets the policy's own defaults fill attrs the spec omits.
    assert nc["apply_policy_default_values"] is True


def test_render_job_cluster_policy_var_overrides_spec_policy_id():
    # The injected variable is authoritative: a policy_id in the spec file is overridden (policy is an
    # environment property, bound per target, not part of the reusable spec).
    spec = {"spark_version": "15.4.x-scala2.12", "num_workers": 1, "policy_id": "HARDCODED_SHOULD_LOSE"}
    job = _render_job(_cfg({"type": "job_cluster", "job_cluster_config": "std"}), spec)
    assert job["job_clusters"][0]["new_cluster"]["policy_id"] == "${var.cluster_policy_id}"


def test_render_job_cluster_passes_custom_tags_verbatim():
    # Hardcoded custom_tags in the spec ride the verbatim passthrough onto the cluster, no special handling.
    spec = {"spark_version": "15.4.x-scala2.12", "num_workers": 1, "custom_tags": {"project": "elastic"}}
    job = _render_job(_cfg({"type": "job_cluster", "job_cluster_config": "std"}), spec)
    assert job["job_clusters"][0]["new_cluster"]["custom_tags"] == {"project": "elastic"}


def test_render_job_cluster_without_spec_fails_closed():
    # A job_cluster compute with no loaded spec is a caller bug; render must raise, not emit a blank cluster.
    with pytest.raises(ValueError, match="requires a loaded new_cluster spec"):
        gen_jobs.render_job_yaml(
            "x.yml", "x", _cfg({"type": "job_cluster", "job_cluster_config": "std"}), None
        )


# --------------------------------------------------------------------------- load_job_cluster_spec


# --------------------------------------------------------------------------- render: schedule


def test_render_no_schedule_omits_block():
    job = _render_job(_cfg())  # default: on-demand
    assert "schedule" not in job


def test_render_schedule_emits_utc_block_with_pause_var():
    # timezone is always UTC; pause_status is bound to the schedule_pause_status bundle variable so a
    # target (stg) can pause all schedules at deploy without editing configs.
    job = _render_job(_cfg(schedule={"quartz_cron_expression": "0 0 8 * * ?"}))
    assert job["schedule"] == {
        "quartz_cron_expression": "0 0 8 * * ?",
        "timezone_id": "UTC",
        "pause_status": "${var.schedule_pause_status}",
    }


def test_render_schedule_composes_with_compute():
    # schedule and compute are independent; both render on the same job.
    job = _render_job(
        _cfg(compute={"type": "existing_cluster", "existing_cluster_id": "0123-x"},
             schedule={"quartz_cron_expression": "0 0 8 * * ?"}),
    )
    assert job["schedule"]["timezone_id"] == "UTC"
    assert job["tasks"][0]["existing_cluster_id"] == "0123-x"


def test_load_job_cluster_spec_missing_fails_closed():
    with pytest.raises(ValueError, match="not found"):
        gen_jobs.load_job_cluster_spec("definitely_no_such_cluster_config_key")


def test_example_job_cluster_spec_loads():
    # The shipped example must be a valid non-empty mapping (documents the format + guards it).
    spec = gen_jobs.load_job_cluster_spec("example")
    assert isinstance(spec, dict) and spec
    assert "spark_version" in spec


def test_standard_batch_spec_loads_with_tags_and_no_policy_id():
    # The referenced spec (used by ecs_dns_activity_jobcluster) carries hardcoded custom_tags and must
    # NOT hardcode a policy_id (the generator injects the per-environment cluster_policy_id variable).
    spec = gen_jobs.load_job_cluster_spec("standard_batch")
    assert spec.get("custom_tags") == {"project": "elastic"}
    assert "policy_id" not in spec


# --------------------------------------------------------------------------- es_host_config


def test_load_es_host_configs_reads_databricks_yml():
    # The shipped databricks.yml declares es_host_primary (a complex var with the three connection
    # fields). The scan must find it; the commented es_host_secondary example must NOT appear.
    declared = gen_jobs.load_es_host_configs()
    assert "es_host_primary" in declared
    assert "es_host_secondary" not in declared


def test_load_es_host_configs_ignores_non_host_complex_vars(tmp_path):
    # Only a complex var whose default keys are EXACTLY the three connection fields is a host config; a
    # complex var with a different shape (e.g. a cluster spec) must be ignored, not misread as one.
    yml = tmp_path / "databricks.yml"
    yml.write_text(
        "variables:\n"
        "  es_host_primary:\n    type: complex\n    default:\n"
        "      es_host_url: ''\n      secret_scope_name: ''\n      secret_key_name: ''\n"
        "  some_cluster:\n    type: complex\n    default:\n      spark_version: '15.4.x'\n"
        "  a_plain_var:\n    default: ''\n"
    )
    assert gen_jobs.load_es_host_configs(str(yml)) == {"es_host_primary"}


def test_require_es_host_config_unknown_fails_closed():
    with pytest.raises(ValueError, match="not declared in databricks.yml"):
        gen_jobs.require_es_host_config("es_host_typo", {"es_host_primary"})


def test_require_es_host_config_known_passes():
    gen_jobs.require_es_host_config("es_host_primary", {"es_host_primary"})  # no raise

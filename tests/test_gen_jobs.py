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
from pipeline_lib.config import job_parameters, validate_config  # noqa: E402


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
    # The prefix is the ${var.job_name_prefix} bundle variable (resolved per target at deploy); the
    # trailing segment defaults to the config name.
    assert job["name"] == "[${bundle.target}] ${var.job_name_prefix}: ecs_dns_activity"
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


def test_render_wires_global_deploy_vars():
    # wheel_path, checkpoint_base_path and ca_certs are GLOBAL bundle variables (one value for every
    # job), so every generated task references them verbatim, independent of the pipeline's host config.
    bp = _render_job(_cfg())["tasks"][0]["notebook_task"]["base_parameters"]
    assert bp["wheel_path"] == "${var.wheel_path}"
    assert bp["checkpoint_base_path"] == "${var.checkpoint_base_path}"
    assert bp["ca_certs"] == "${var.ca_certs}"


def test_render_all_jobs_max_concurrent_runs_1():
    for compute, spec in (
        (None, None),
        ({"type": "existing_cluster", "cluster_config": "interactive_primary"}, None),
        ({"type": "job_cluster", "job_cluster_config": "std"}, {"spark_version": "15.4.x-scala2.12", "num_workers": 1}),
    ):
        assert _render_job(_cfg(compute), spec)["max_concurrent_runs"] == 1


def test_render_all_jobs_disable_queue():
    # queue.enabled=false pairs with max_concurrent_runs=1 so a trigger firing on a still-running job is
    # SKIPPED, not queued (the Jobs API defaults queue.enabled to true, which would pile runs up). Holds
    # for every compute; independent of schedule/continuous since it is a job-level, always-on guard.
    for compute, spec in (
        (None, None),
        ({"type": "existing_cluster", "cluster_config": "interactive_primary"}, None),
        ({"type": "job_cluster", "job_cluster_config": "std"}, {"spark_version": "15.4.x-scala2.12", "num_workers": 1}),
    ):
        assert _render_job(_cfg(compute), spec)["queue"] == {"enabled": False}


def test_render_scheduled_job_disables_queue():
    # The pile-up scenario in the wild: a cron job whose run outlasts the interval. queue off => the
    # overlapping tick is dropped rather than queued behind the running drain.
    job = _render_job(_cfg(schedule={"quartz_cron_expression": "0 0 8 * * ?"}))
    assert "schedule" in job and job["queue"] == {"enabled": False}


# --------------------------------------------------------------------------- render: existing_cluster


def test_render_existing_cluster():
    # existing_cluster names a cluster_config bundle variable; the task's existing_cluster_id is rendered
    # as a ${var.<name>} reference, so the bundle resolves the workspace-specific cluster id per target.
    job = _render_job(_cfg({"type": "existing_cluster", "cluster_config": "interactive_primary"}))
    task = job["tasks"][0]
    assert task["existing_cluster_id"] == "${var.interactive_primary.cluster_id}"
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
        _cfg(compute={"type": "existing_cluster", "cluster_config": "interactive_primary"},
             schedule={"quartz_cron_expression": "0 0 8 * * ?"}),
    )
    assert job["schedule"]["timezone_id"] == "UTC"
    assert job["tasks"][0]["existing_cluster_id"] == "${var.interactive_primary.cluster_id}"


# --------------------------------------------------------------------------- render: continuous

_JC_SPEC = {"spark_version": "17.3.x-scala2.13", "node_type_id": "i3.xlarge", "num_workers": 1}


def _continuous_cfg(interval="30 seconds"):
    """A validated continuous config (streaming + job_cluster + a continuous block)."""
    raw = {
        "es_index_name": "ecs-dns-activity",
        "es_id_field": "dsl_id",
        "es_host_config": "es_host_primary",
        "pipeline_mode": "streaming",
        "view": {"catalog": "cat", "schema": "es_poc", "name": "ecs_dns_activity"},
        "source": {"catalog": "cat", "schema": "ocsf", "table": "dns_activity"},
        "compute": {"type": "job_cluster", "job_cluster_config": "standard_batch"},
        "continuous": {"trigger_interval": interval},
    }
    return validate_config(raw)


def test_render_no_continuous_omits_block_and_empties_interval():
    # A non-continuous job has no continuous block, and its streaming_trigger_interval base param is ""
    # (so the notebook uses Trigger.availableNow).
    job = _render_job(_cfg())
    assert "continuous" not in job
    assert job["tasks"][0]["notebook_task"]["base_parameters"]["streaming_trigger_interval"] == ""


def test_render_continuous_emits_trigger_and_no_schedule():
    # A continuous config emits a Databricks Jobs continuous trigger (pause bound to the shared
    # schedule_pause_status var) INSTEAD of a schedule.
    job = _render_job(_continuous_cfg(), _JC_SPEC)
    assert job["continuous"] == {"pause_status": "${var.schedule_pause_status}"}
    assert "schedule" not in job


def test_render_continuous_wires_trigger_interval_base_param():
    # The ProcessingTime cadence reaches the notebook as the streaming_trigger_interval base parameter.
    job = _render_job(_continuous_cfg("1 minute"), _JC_SPEC)
    assert job["tasks"][0]["notebook_task"]["base_parameters"]["streaming_trigger_interval"] == "1 minute"


def test_render_continuous_existing_cluster():
    # Continuous is valid on existing_cluster too (not just job_cluster): the continuous trigger and the
    # trigger-interval base param render alongside the existing_cluster_id, with no job_clusters block.
    raw = {
        "es_index_name": "ecs-dns-activity",
        "es_id_field": "dsl_id",
        "es_host_config": "es_host_primary",
        "pipeline_mode": "streaming",
        "view": {"catalog": "cat", "schema": "es_poc", "name": "ecs_dns_activity"},
        "source": {"catalog": "cat", "schema": "ocsf", "table": "dns_activity"},
        "compute": {"type": "existing_cluster", "cluster_config": "interactive_primary"},
        "continuous": {"trigger_interval": "30 seconds"},
    }
    job = _render_job(validate_config(raw))  # existing_cluster needs no new_cluster spec
    assert job["continuous"] == {"pause_status": "${var.schedule_pause_status}"}
    assert "job_clusters" not in job
    assert job["tasks"][0]["existing_cluster_id"] == "${var.interactive_primary.cluster_id}"
    assert job["tasks"][0]["notebook_task"]["base_parameters"]["streaming_trigger_interval"] == "30 seconds"


def test_render_continuous_keeps_max_concurrent_runs_1():
    # Databricks continuous jobs require exactly one active run; the framework fixes this at 1 for all jobs.
    assert _render_job(_continuous_cfg(), _JC_SPEC)["max_concurrent_runs"] == 1


def test_render_continuous_on_serverless_fails_closed():
    # Defense in depth: even if a continuous+serverless config reached the generator (config rejects it
    # first), render must refuse rather than emit a continuous job on serverless.
    cfg = _cfg()  # batch/serverless
    cfg["continuous"] = {"trigger_interval": "30 seconds"}  # hand-set, bypassing validate_config's guard
    with pytest.raises(ValueError, match="continuous.*requires classic compute"):
        gen_jobs.render_job_yaml("x.yml", "x", cfg, None)


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


# --------------------------------------------------------------------------- cluster_config (existing_cluster)


def test_load_cluster_configs_ignores_non_cluster_complex_vars(tmp_path):
    # A cluster config is a complex var whose default keys are EXACTLY {cluster_id}. A host-shaped complex
    # var and a plain string var are ignored, so cluster_config can only ever name a real cluster-id
    # variable - never es_host_primary, wheel_path, etc.
    yml = tmp_path / "databricks.yml"
    yml.write_text(
        "variables:\n"
        "  interactive_primary:\n    type: complex\n    default:\n      cluster_id: ''\n"
        "  es_host_primary:\n    type: complex\n    default:\n"
        "      es_host_url: ''\n      secret_scope_name: ''\n      secret_key_name: ''\n"
        "  wheel_path:\n    default: ''\n"
    )
    assert gen_jobs.load_cluster_configs(str(yml)) == {"interactive_primary"}


def test_load_cluster_configs_none_shipped_on_databricks_yml():
    # interactive_primary ships COMMENTED, so no cluster config is declared out of the box.
    assert gen_jobs.load_cluster_configs() == set()


def test_require_cluster_config_unknown_fails_closed():
    # A name that is not a declared cluster config - a typo, or a real but non-cluster variable like
    # es_host_primary - fails closed at generation (not a deny-list: only cluster-shaped configs pass).
    with pytest.raises(ValueError, match="not declared as a cluster config"):
        gen_jobs.require_cluster_config("es_host_primary", {"interactive_primary"})


def test_require_cluster_config_known_passes():
    gen_jobs.require_cluster_config("interactive_primary", {"interactive_primary"})  # no raise


def test_load_default_es_host_config_reads_databricks_yml():
    # The shipped databricks.yml sets default_es_host_config to es_host_primary; that is what a pipeline
    # that omits es_host_config falls back to.
    assert gen_jobs.load_default_es_host_config() == "es_host_primary"


def test_load_default_es_host_config_absent_is_none(tmp_path):
    yml = tmp_path / "databricks.yml"
    yml.write_text("variables:\n  wheel_path:\n    default: ''\n")  # no default_es_host_config declared
    assert gen_jobs.load_default_es_host_config(str(yml)) is None


@pytest.mark.parametrize("bad", ["bad-name", "a.b", "1leading", "has space"])
def test_load_default_es_host_config_rejects_bad_identifier(tmp_path, bad):
    # The default name is held to the SAME identifier rule as a pipeline's own es_host_config, so a
    # malformed default fails closed with a clear message (not a confusing "not declared" downstream).
    yml = tmp_path / "databricks.yml"
    yml.write_text(f"variables:\n  default_es_host_config:\n    default: {bad!r}\n")
    with pytest.raises(ValueError, match="identifier"):
        gen_jobs.load_default_es_host_config(str(yml))


def test_omitted_es_host_config_resolves_to_bundle_default():
    # A pipeline that omits es_host_config (validate returns None) must render the BUNDLE DEFAULT's refs,
    # mirroring how main() resolves it (cfg["es_host_config"] or the default) before rendering.
    default = gen_jobs.load_default_es_host_config()  # es_host_primary
    cfg = validate_config({
        "es_index_name": "ecs-dns-activity", "es_id_field": "dsl_id",  # no es_host_config
        "pipeline_mode": "batch", "view": {"catalog": "c", "schema": "s", "name": "v"},
        "source": {"catalog": "c", "schema": "s", "table": "t", "primary_key": "dsl_id"},
    })
    assert cfg["es_host_config"] is None
    cfg["es_host_config"] = cfg["es_host_config"] or default  # what main() does
    bp = _render_job(cfg)["tasks"][0]["notebook_task"]["base_parameters"]
    assert bp["es_host_url"] == "${var.es_host_primary.es_host_url}"


def test_render_unresolved_es_host_config_fails_closed():
    # render must never emit a ${var.None.*} ref: an unresolved (None) es_host_config is a caller bug
    # (main resolves the default first), so rendering it fails closed.
    cfg = validate_config({
        "es_index_name": "ecs-dns-activity", "es_id_field": "dsl_id",  # no es_host_config -> None
        "pipeline_mode": "batch", "view": {"catalog": "c", "schema": "s", "name": "v"},
        "source": {"catalog": "c", "schema": "s", "table": "t", "primary_key": "dsl_id"},
    })
    with pytest.raises(ValueError, match="es_host_config for .* is unset"):
        gen_jobs.render_job_yaml("x.yml", "x", cfg, None)


# --------------------------------------------------------------------------- job_name_prefix / postfix


def test_render_job_name_prefix_is_bundle_variable():
    # Every job name embeds ${var.job_name_prefix} (resolved per target at deploy), not a hardcoded literal.
    assert "${var.job_name_prefix}" in _render_job(_cfg())["name"]


def test_render_job_name_postfix_overrides_config_name():
    # A singleton's optional job_name_postfix replaces only the trailing display segment; the resource
    # key and task key stay identifier-safe (index_pipeline_<config name>), untouched by the postfix.
    cfg = _cfg()
    cfg["job_name_postfix"] = "ECS DNS (serverless)"
    text = gen_jobs.render_job_yaml("ecs_dns_activity.yml", "ecs_dns_activity", cfg, None)
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_ecs_dns_activity"]
    assert job["name"] == "[${bundle.target}] ${var.job_name_prefix}: ECS DNS (serverless)"
    assert job["tasks"][0]["task_key"] == "index_pipeline_ecs_dns_activity"  # key unaffected


def test_require_job_name_prefix_declared_present_passes(tmp_path):
    yml = tmp_path / "databricks.yml"
    yml.write_text("variables:\n  job_name_prefix:\n    default: acme-pipelines\n")
    gen_jobs.require_job_name_prefix_declared(str(yml))  # no raise


def test_require_job_name_prefix_declared_missing_fails_closed(tmp_path):
    # The generated names reference ${var.job_name_prefix}; if the variable is not declared, fail closed
    # at generation rather than let the reference break confusingly at deploy.
    yml = tmp_path / "databricks.yml"
    yml.write_text("variables:\n  wheel_path:\n    default: ''\n")
    with pytest.raises(ValueError, match="job_name_prefix is not declared"):
        gen_jobs.require_job_name_prefix_declared(str(yml))


def test_shipped_databricks_yml_declares_job_name_prefix():
    gen_jobs.require_job_name_prefix_declared()  # the repo's databricks.yml declares it


def test_shipped_deploy_views_job_disables_queue():
    # deploy_views is hand-authored (not generated), so guard its skip-not-queue setting against drift
    # back to the queuing default, matching every generated job.
    path = os.path.join(_REPO_ROOT, "resources", "deploy_views.job.yml")
    with open(path) as fh:
        job = yaml.safe_load(fh)["resources"]["jobs"]["deploy_views"]
    assert job["max_concurrent_runs"] == 1
    assert job["queue"] == {"enabled": False}


# --------------------------------------------------------------------------- job groups


def _member(config_filename, name, es_index, *, group="g1", postfix=None, mode="streaming",
            continuous=None, schedule=None, job_cluster_config=None, cluster_config=None):
    """One (config_filename, name, cfg, spec) group member. spec is a job-cluster new_cluster spec when
    job_cluster_config is set (mirrors what main() loads), else None."""
    raw = {
        "es_index_name": es_index, "es_id_field": "dsl_id", "es_host_config": "es_host_primary",
        "pipeline_mode": mode, "job_group": group,
        "view": {"catalog": "c", "schema": "s", "name": "v"},
        "source": {"catalog": "c", "schema": "s", "table": "t"},
    }
    if postfix is not None:
        raw["job_name_postfix"] = postfix
    if continuous is not None:
        raw["continuous"] = {"trigger_interval": continuous}
    if schedule is not None:
        raw["schedule"] = {"quartz_cron_expression": schedule}
    spec = None
    if job_cluster_config is not None:
        raw["compute"] = {"type": "job_cluster", "job_cluster_config": job_cluster_config}
        spec = {"spark_version": "17.3.x-scala2.13", "num_workers": 1}
    elif cluster_config is not None:
        raw["compute"] = {"type": "existing_cluster", "cluster_config": cluster_config}
    return (config_filename, name, validate_config(raw), spec)


def _render_group(group, members):
    """Render and parse a group job; return the single job dict under resources.jobs."""
    text = gen_jobs.render_group_job_yaml(group, members)
    assert text.startswith(gen_jobs._GENERATED_MARKER)  # group header carries the generated marker
    jobs = yaml.safe_load(text)["resources"]["jobs"]
    assert list(jobs) == [f"index_pipeline_group_{group}"]
    return jobs[f"index_pipeline_group_{group}"]


def test_group_one_job_one_task_per_member():
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch"),
        _member("b.yml", "b", "idx-b", mode="batch"),
    ])
    # Tasks emitted sorted by config name; each keyed index_pipeline_<member>.
    assert [t["task_key"] for t in job["tasks"]] == ["index_pipeline_a", "index_pipeline_b"]


def test_group_disables_queue():
    # A grouped job is one Databricks job with one trigger, so it needs the same skip-not-queue guard.
    job = _render_group("g1", [_member("a.yml", "a", "idx-a", mode="batch")])
    assert job["queue"] == {"enabled": False}


def test_group_has_no_job_level_parameters_block():
    # Option A: a grouped job carries NO job-level parameters (they can't hold per-member defaults).
    job = _render_group("g1", [_member("a.yml", "a", "idx-a", mode="batch")])
    assert "parameters" not in job


def test_group_run_time_knobs_move_into_task_base_parameters():
    # The 12 run-time knobs (from job_parameters) become each task's base_parameters, with per-member
    # defaults; the notebook reads the same widget names, so no notebook change.
    cfg = validate_config({
        "es_index_name": "idx-a", "es_id_field": "dsl_id", "es_host_config": "es_host_primary",
        "pipeline_mode": "batch", "job_group": "g1", "chunk_size": 500, "write_concurrency": 4,
        "view": {"catalog": "c", "schema": "s", "name": "v"},
        "source": {"catalog": "c", "schema": "s", "table": "t"},
    })
    job = _render_group("g1", [("a.yml", "a", cfg, None)])
    bp = job["tasks"][0]["notebook_task"]["base_parameters"]
    for p in job_parameters(cfg):
        assert bp[p["name"]] == p["default"]
    assert bp["chunk_size"] == "500" and bp["write_concurrency"] == "4"  # per-member defaults carried


def test_group_shares_one_job_cluster_when_same_config():
    # Two members naming the SAME job_cluster_config get ONE job_clusters entry; both tasks reference it.
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch", job_cluster_config="shared"),
        _member("b.yml", "b", "idx-b", mode="batch", job_cluster_config="shared"),
    ])
    assert [c["job_cluster_key"] for c in job["job_clusters"]] == ["shared"]
    assert {t["job_cluster_key"] for t in job["tasks"]} == {"shared"}


def test_group_distinct_job_clusters_stay_separate():
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch", job_cluster_config="big"),
        _member("b.yml", "b", "idx-b", mode="batch", job_cluster_config="small"),
    ])
    assert [c["job_cluster_key"] for c in job["job_clusters"]] == ["big", "small"]  # sorted, both present


def test_group_on_demand_has_no_trigger_and_postfix_defaults_to_group_name():
    job = _render_group("mygrp", [_member("a.yml", "a", "idx-a", group="mygrp", mode="batch")])
    assert "schedule" not in job and "continuous" not in job
    assert job["name"] == "[${bundle.target}] ${var.job_name_prefix}: mygrp"


def test_group_postfix_adopted_from_single_declaring_member():
    # One member declares job_name_postfix, the other omits it: the declared one is adopted (lenient).
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch", postfix="ECS Group"),
        _member("b.yml", "b", "idx-b", mode="batch"),
    ])
    assert job["name"] == "[${bundle.target}] ${var.job_name_prefix}: ECS Group"


def test_group_conflicting_postfix_fails_closed():
    with pytest.raises(ValueError, match="conflicting job_name_postfix"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", mode="batch", postfix="X"),
            _member("b.yml", "b", "idx-b", mode="batch", postfix="Y"),
        ])


def test_group_scheduled_shares_one_cron():
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch", schedule="0 0 8 * * ?"),
        _member("b.yml", "b", "idx-b", mode="batch", schedule="0 0 8 * * ?"),
    ])
    assert job["schedule"]["quartz_cron_expression"] == "0 0 8 * * ?"
    assert job["schedule"]["timezone_id"] == "UTC"


def test_group_conflicting_cron_fails_closed():
    with pytest.raises(ValueError, match="conflicting triggers"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", mode="batch", schedule="0 0 8 * * ?"),
            _member("b.yml", "b", "idx-b", mode="batch", schedule="0 0 9 * * ?"),
        ])


def test_group_schedule_and_continuous_conflict_fails_closed():
    with pytest.raises(ValueError, match="conflicting triggers"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", continuous="30 seconds", job_cluster_config="s"),
            _member("b.yml", "b", "idx-b", mode="batch", schedule="0 0 8 * * ?"),
        ])


def test_group_continuous_emits_trigger_and_propagates_interval_to_all():
    # A continuous group: one job-level continuous trigger, and the single interval propagates to EVERY
    # task's streaming_trigger_interval (including the member that omitted its own continuous block, so it
    # runs always-on rather than draining under availableNow).
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", continuous="30 seconds", job_cluster_config="shared"),
        _member("b.yml", "b", "idx-b", job_cluster_config="shared"),  # no continuous block: inherits
    ])
    assert job["continuous"] == {"pause_status": "${var.schedule_pause_status}"}
    for t in job["tasks"]:
        assert t["notebook_task"]["base_parameters"]["streaming_trigger_interval"] == "30 seconds"


def test_group_continuous_conflicting_interval_fails_closed():
    with pytest.raises(ValueError, match="conflicting triggers"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", continuous="30 seconds", job_cluster_config="s"),
            _member("b.yml", "b", "idx-b", continuous="1 minute", job_cluster_config="s"),
        ])


def test_group_continuous_nonstreaming_member_fails_closed():
    # A member that inherits continuous must itself be a streaming pipeline on classic compute; a batch
    # member (which would drain-and-stop) fails closed.
    with pytest.raises(ValueError, match="continuous.*every member must be a streaming"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", continuous="30 seconds", job_cluster_config="s"),
            _member("b.yml", "b", "idx-b", mode="batch", job_cluster_config="s"),
        ])


def test_group_continuous_serverless_member_fails_closed():
    # A serverless member inheriting continuous is rejected (serverless has no ProcessingTime trigger).
    with pytest.raises(ValueError, match="continuous.*every member must be a streaming"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", continuous="30 seconds", job_cluster_config="s"),
            _member("b.yml", "b", "idx-b"),  # streaming but serverless (no compute) -> classic required
        ])


def test_group_duplicate_es_index_name_fails_closed():
    # Two members writing the SAME es_index_name would run as concurrent tasks and double-write that
    # index (max_concurrent_runs=1 guards concurrent job runs, not tasks). Fail closed at generation.
    with pytest.raises(ValueError, match="writing the SAME es_index_name"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "shared-idx", mode="batch"),
            _member("b.yml", "b", "shared-idx", mode="batch"),
        ])


def test_group_distinct_es_index_names_ok():
    # Distinct indices are the normal case: no raise.
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch"),
        _member("b.yml", "b", "idx-b", mode="batch"),
    ])
    assert len(job["tasks"]) == 2


def test_group_generated_path_uses_group_prefix():
    assert gen_jobs.group_generated_path("g1").endswith("/resources/group_g1.job.yml")

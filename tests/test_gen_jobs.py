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
from pipeline_lib.config import (  # noqa: E402
    RUNTIME_KNOB_NAMES,
    job_parameters,
    runtime_knob_global_refs,
    validate_config,
)


# Legal global defaults for building a databricks.yml the run-time-knob gate accepts: pipeline_mode and
# streaming_start need a concrete value (their validators reject ""), every other knob (op_type included)
# accepts "".
_LEGAL_KNOB_DEFAULTS = {"pipeline_mode": "batch", "streaming_start": "new"}


def _write_runtime_knob_yml(tmp_path, overrides=None, drop=()):
    """Write a databricks.yml declaring EVERY run-time knob with a legal default, applying `overrides`
    (name -> default) and dropping `drop` names, so a test can break or remove exactly one knob and assert
    require_runtime_knobs_declared fails closed on it."""
    overrides = overrides or {}
    lines = ["variables:"]
    for name in RUNTIME_KNOB_NAMES:
        if name in drop:
            continue
        default = overrides.get(name, _LEGAL_KNOB_DEFAULTS.get(name, ""))
        lines.append(f"  {name}:")
        lines.append(f"    default: '{default}'")
    yml = tmp_path / "databricks.yml"
    yml.write_text("\n".join(lines) + "\n")
    return yml


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


def test_render_all_jobs_notify_support_email_on_failure():
    # Every generated job emails the ${var.support_email} recipients on a run failure. on_failure
    # references the whole complex LIST variable (NOT [${var.support_email}]) so an empty-list target
    # resolves to [] (no recipients = off), never [""]. The variable is empty in dev/stg and set only in
    # prd, so the same emitted block gives per-target control. Holds for every compute.
    for compute, spec in (
        (None, None),
        ({"type": "existing_cluster", "cluster_config": "interactive_primary"}, None),
        ({"type": "job_cluster", "job_cluster_config": "std"}, {"spark_version": "15.4.x-scala2.12", "num_workers": 1}),
    ):
        assert _render_job(_cfg(compute), spec)["email_notifications"] == {
            "on_failure": "${var.support_email}"
        }


def test_render_all_jobs_suppress_skipped_and_canceled_alerts():
    # Paired with email_notifications: suppress SKIPPED runs (queue disabled => an overlapping trigger is
    # skipped, not failed) and CANCELED runs (a continuous job's ON_FAILURE retry cancels-and-restarts),
    # so the support address is paged only on a genuine failure, not on the framework's own churn.
    assert _render_job(_cfg())["notification_settings"] == {
        "no_alert_for_skipped_runs": True,
        "no_alert_for_canceled_runs": True,
    }


def test_render_group_job_notifies_support_email_on_failure():
    # A grouped (multi-task) job carries the same failure-email block at the JOB level, so one failed
    # member fails the run and pages the address, exactly like a singleton.
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch"),
        _member("b.yml", "b", "idx-b", mode="batch"),
    ])
    assert job["email_notifications"] == {"on_failure": "${var.support_email}"}
    assert job["notification_settings"] == {
        "no_alert_for_skipped_runs": True,
        "no_alert_for_canceled_runs": True,
    }


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


def test_render_schedule_pause_status_override():
    # A per-pipeline pause_status overrides the target-wide global for THIS job's schedule: the literal
    # PAUSED|UNPAUSED is emitted instead of ${var.schedule_pause_status}.
    cfg = _cfg(schedule={"quartz_cron_expression": "0 0 8 * * ?"})
    cfg["pause_status"] = "UNPAUSED"
    job = _render_job(cfg)
    assert job["schedule"]["pause_status"] == "UNPAUSED"
    # The rest of the schedule block is unchanged.
    assert job["schedule"]["quartz_cron_expression"] == "0 0 8 * * ?"
    assert job["schedule"]["timezone_id"] == "UTC"


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
    # schedule_pause_status var, task_retry_mode ON_FAILURE) INSTEAD of a schedule.
    job = _render_job(_continuous_cfg(), _JC_SPEC)
    assert job["continuous"] == {
        "pause_status": "${var.schedule_pause_status}",
        "task_retry_mode": "ON_FAILURE",
    }
    assert "schedule" not in job


def test_render_continuous_pause_status_override():
    # A per-pipeline pause_status overrides the target-wide global on a continuous trigger too; the
    # task_retry_mode pin is unaffected.
    cfg = _continuous_cfg()
    cfg["pause_status"] = "UNPAUSED"
    job = _render_job(cfg, _JC_SPEC)
    assert job["continuous"] == {
        "pause_status": "UNPAUSED",
        "task_retry_mode": "ON_FAILURE",
    }


def test_render_continuous_sets_task_retry_mode_on_failure():
    # Continuous jobs MUST set task_retry_mode: ON_FAILURE. This is the recovery knob for a continuous
    # job (per-task max_retries is not usable in continuous). Its API/bundle default when OMITTED is
    # NEVER (a failed task is never retried); in a multi-task continuous group a failed task then sits
    # FAILED forever while the sibling streams never terminate, so the run never restarts either.
    # ON_FAILURE retries the failed task while a sibling is still running, else cancels and restarts the
    # whole run. So the value must be present and exactly ON_FAILURE, not merely "not NEVER".
    singleton = _render_job(_continuous_cfg(), _JC_SPEC)
    assert singleton["continuous"]["task_retry_mode"] == "ON_FAILURE"
    group = _render_group("g", [
        _member("a.yml", "a", "idx-a", continuous="30 seconds", job_cluster_config="shared"),
        _member("b.yml", "b", "idx-b", job_cluster_config="shared"),
    ])
    assert group["continuous"]["task_retry_mode"] == "ON_FAILURE"


def test_render_non_continuous_never_carries_task_retry_mode():
    # task_retry_mode is a CONTINUOUS-only field (it lives on the continuous trigger). A non-continuous
    # job has no continuous block at all, so it can never carry the knob - guard against it leaking onto
    # scheduled/on-demand jobs.
    job = _render_job(_cfg())
    assert "continuous" not in job
    assert "task_retry_mode" not in yaml.safe_dump(job)


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
    assert job["continuous"] == {
        "pause_status": "${var.schedule_pause_status}",
        "task_retry_mode": "ON_FAILURE",
    }
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


def test_require_support_email_declared_present_passes(tmp_path):
    yml = tmp_path / "databricks.yml"
    yml.write_text("variables:\n  support_email:\n    type: complex\n    default: []\n")
    gen_jobs.require_support_email_declared(str(yml))  # no raise


def test_require_support_email_declared_missing_fails_closed(tmp_path):
    # Every job emits email_notifications.on_failure: [${var.support_email}]; if the variable is not
    # declared, fail closed at generation rather than let the reference break confusingly at deploy.
    yml = tmp_path / "databricks.yml"
    yml.write_text("variables:\n  wheel_path:\n    default: ''\n")
    with pytest.raises(ValueError, match="support_email is not declared"):
        gen_jobs.require_support_email_declared(str(yml))


def test_shipped_databricks_yml_declares_support_email():
    gen_jobs.require_support_email_declared()  # the repo's databricks.yml declares it


def test_require_runtime_knobs_declared_shipped_passes():
    # The repo's databricks.yml declares EVERY run-time knob with a legal default.
    gen_jobs.require_runtime_knobs_declared()  # no raise


def test_require_runtime_knobs_declared_full_legal_passes(tmp_path):
    gen_jobs.require_runtime_knobs_declared(str(_write_runtime_knob_yml(tmp_path)))  # no raise


@pytest.mark.parametrize("missing", ["pipeline_mode", "bulk_stats", "streaming_start", "max_bytes_per_trigger"])
def test_require_runtime_knobs_declared_missing_fails_closed(tmp_path, missing):
    # An omitted-knob config bakes ${var.<name>}; if the variable is not declared, fail closed at
    # generation rather than let the reference break confusingly at deploy. Checked for every knob (the
    # gate loops the registry), so dropping ANY one is caught, naming that knob.
    yml = _write_runtime_knob_yml(tmp_path, drop=(missing,))
    with pytest.raises(ValueError, match=f"{missing} is not declared"):
        gen_jobs.require_runtime_knobs_declared(str(yml))


@pytest.mark.parametrize("name,bad", [
    ("bulk_stats", "on"), ("retry_transport_timeout", "yes"), ("bypass_fast_path", "1"),
    ("verify_certs", "maybe"), ("require_existing_index", "nope"),
    ("request_timeout", "abc"), ("transport_max_retries", "-1"), ("max_retries_per_doc", "-1"),
    ("pipeline_mode", "turbo"), ("op_type", "has space"), ("streaming_start", "sideways"),
    ("write_repartition", "-5"), ("max_partition_bytes", "32x"), ("chunk_size", "0"),
])
def test_require_runtime_knobs_declared_bad_default_fails_closed(tmp_path, name, bad):
    # The registry wires each knob's OWN validator (the same require_* helper the runner applies to the
    # effective value), so an illegal global default for ANY knob fails closed at generation, not at run.
    yml = _write_runtime_knob_yml(tmp_path, overrides={name: bad})
    with pytest.raises(ValueError, match=name):
        gen_jobs.require_runtime_knobs_declared(str(yml))


def test_version_gated_knob_globals_ship_empty():
    # Regression: a VERSION-GATED connector knob (see _VERSION_GATED_KNOBS in
    # notebooks/run_index_pipeline.py: bulk_stats, retry_transport_timeout, op_type, bypass_fast_path) MUST
    # ship an EMPTY global default in databricks.yml. A concrete non-empty global would make a config that
    # OMITS the knob bake that value, so on a connector older than the knob's min version the runner's
    # version-gate drops it with a spurious "dropping <knob>=<v>" warning every run (export unchanged, but
    # misleading). An empty global bakes "" => the knob is omitted from write_overrides => the gate never
    # fires unless a target/config explicitly set it. op_type's empty also defers to the connector default
    # (index), so nothing is lost. (op_type shipped a literal 'index' once; this guards against regressing.)
    doc = yaml.safe_load(open(os.path.join(_REPO_ROOT, "databricks.yml")))
    variables = doc["variables"]
    for knob in ("bulk_stats", "retry_transport_timeout", "op_type", "bypass_fast_path"):
        spec = variables[knob]
        default = spec.get("default") if isinstance(spec, dict) else spec
        assert (default or "") == "", (
            f"version-gated knob {knob!r} must ship an EMPTY global default (got {default!r}); a non-empty "
            f"global spuriously trips the runner version-gate on a pre-min-version wheel"
        )


def test_require_runtime_knobs_declared_accepts_scalar_shorthand(tmp_path):
    # DAB's scalar shorthand (<name>: <v>, no `default:` key) IS the default; a legal shorthand for every
    # knob passes (the gate reads the default from whichever shape was used).
    lines = ["variables:"]
    for name in RUNTIME_KNOB_NAMES:
        lines.append(f"  {name}: '{_LEGAL_KNOB_DEFAULTS.get(name, '')}'")
    yml = tmp_path / "databricks.yml"
    yml.write_text("\n".join(lines) + "\n")
    gen_jobs.require_runtime_knobs_declared(str(yml))  # no raise


# --- render guards: an omitted knob bakes its ${var.<name>} global ref, a set knob bakes its literal ---


def test_render_singleton_omitted_retry_transport_timeout_bakes_global_ref():
    cfg = _cfg()
    text = gen_jobs.render_job_yaml("ecs_dns_activity.yml", "ecs_dns_activity", cfg, None)
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_ecs_dns_activity"]
    assert {"name": "retry_transport_timeout", "default": "${var.retry_transport_timeout}"} in job["parameters"]


def test_render_singleton_set_retry_transport_timeout_bakes_literal():
    cfg = _cfg()
    cfg["retry_transport_timeout"] = "true"
    text = gen_jobs.render_job_yaml("ecs_dns_activity.yml", "ecs_dns_activity", cfg, None)
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_ecs_dns_activity"]
    assert {"name": "retry_transport_timeout", "default": "true"} in job["parameters"]


def test_render_singleton_omitted_bypass_fast_path_bakes_global_ref():
    cfg = _cfg()
    text = gen_jobs.render_job_yaml("ecs_dns_activity.yml", "ecs_dns_activity", cfg, None)
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_ecs_dns_activity"]
    assert {"name": "bypass_fast_path", "default": "${var.bypass_fast_path}"} in job["parameters"]


def test_render_singleton_set_bypass_fast_path_bakes_literal():
    cfg = _cfg()
    cfg["bypass_fast_path"] = "true"
    text = gen_jobs.render_job_yaml("ecs_dns_activity.yml", "ecs_dns_activity", cfg, None)
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_ecs_dns_activity"]
    assert {"name": "bypass_fast_path", "default": "true"} in job["parameters"]


def test_render_singleton_omitted_bulk_stats_bakes_global_ref():
    # A singleton config that omits bulk_stats gets ${var.bulk_stats} as the bulk_stats job-parameter
    # default, so it defers to the target-wide global default at deploy.
    cfg = _cfg()
    text = gen_jobs.render_job_yaml("ecs_dns_activity.yml", "ecs_dns_activity", cfg, None)
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_ecs_dns_activity"]
    assert {"name": "bulk_stats", "default": "${var.bulk_stats}"} in job["parameters"]


def test_render_singleton_set_bulk_stats_bakes_literal():
    # A singleton that SETS bulk_stats bakes its literal value, overriding the global ref.
    cfg = _cfg()
    cfg["bulk_stats"] = "false"
    text = gen_jobs.render_job_yaml("ecs_dns_activity.yml", "ecs_dns_activity", cfg, None)
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_ecs_dns_activity"]
    assert {"name": "bulk_stats", "default": "false"} in job["parameters"]


def test_shipped_deploy_views_job_disables_queue():
    # deploy_views is hand-authored (not generated), so guard its skip-not-queue setting against drift
    # back to the queuing default, matching every generated job.
    path = os.path.join(_REPO_ROOT, "resources", "deploy_views.job.yml")
    with open(path) as fh:
        job = yaml.safe_load(fh)["resources"]["jobs"]["deploy_views"]
    assert job["max_concurrent_runs"] == 1
    assert job["queue"] == {"enabled": False}


@pytest.mark.parametrize("filename,job_key", [
    ("deploy_views.job.yml", "deploy_views"),
    ("checkpoint_clear.job.yml", "checkpoint_clear"),
    ("build_wheel.job.yml", "build_wheel"),
])
def test_shipped_hand_authored_jobs_notify_support_email(filename, job_key):
    # The hand-authored jobs are NOT emitted by gen_jobs, so guard their failure-email block against drift:
    # each must carry the same email_notifications.on_failure: [${var.support_email}] and the skipped/
    # canceled suppression that every generated job gets. Without this a hand-authored job could silently
    # go un-monitored while the generated ones page prd on failure.
    path = os.path.join(_REPO_ROOT, "resources", filename)
    with open(path) as fh:
        job = yaml.safe_load(fh)["resources"]["jobs"][job_key]
    assert job["email_notifications"] == {"on_failure": "${var.support_email}"}
    assert job["notification_settings"] == {
        "no_alert_for_skipped_runs": True,
        "no_alert_for_canceled_runs": True,
    }


def test_shipped_build_wheel_job_shape():
    # build_wheel is hand-authored (not generated), so guard its shape against drift: single-flight +
    # skip-not-queue like every other job, its two required run parameters (blank defaults, so a bare run
    # fails closed in the notebook), and the wheel_path base_parameter the notebook derives the upload dir
    # from. The notebook's build/upload behavior is proven by a live run, not here.
    path = os.path.join(_REPO_ROOT, "resources", "build_wheel.job.yml")
    with open(path) as fh:
        job = yaml.safe_load(fh)["resources"]["jobs"]["build_wheel"]
    assert job["max_concurrent_runs"] == 1
    assert job["queue"] == {"enabled": False}
    # Both parameters are declared with blank defaults (the notebook fails closed on blank).
    assert {"name": "repo_workspace_path", "default": ""} in job["parameters"]
    assert {"name": "wheel_version", "default": ""} in job["parameters"]
    (task,) = job["tasks"]
    assert task["notebook_task"]["notebook_path"] == "../notebooks/build_wheel.py"
    # wheel_path is wired from the bundle variable; the notebook takes its parent dir as the upload target.
    assert task["notebook_task"]["base_parameters"] == {"wheel_path": "${var.wheel_path}"}


# --------------------------------------------------------------------------- job groups


def _member(config_filename, name, es_index, *, group="g1", postfix=None, mode="streaming",
            continuous=None, schedule=None, pause_status=None, job_cluster_config=None, cluster_config=None):
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
    if pause_status is not None:
        raw["pause_status"] = pause_status
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
    # The 17 run-time knobs (from job_parameters) become each task's base_parameters, with per-member
    # defaults; the notebook reads the same widget names, so no notebook change.
    cfg = validate_config({
        "es_index_name": "idx-a", "es_id_field": "dsl_id", "es_host_config": "es_host_primary",
        "pipeline_mode": "batch", "job_group": "g1", "chunk_size": 500, "write_concurrency": 4,
        "view": {"catalog": "c", "schema": "s", "name": "v"},
        "source": {"catalog": "c", "schema": "s", "table": "t"},
    })
    job = _render_group("g1", [("a.yml", "a", cfg, None)])
    bp = job["tasks"][0]["notebook_task"]["base_parameters"]
    # The grouped task bakes the SAME defaults the generator uses (job_parameters with the full
    # runtime_knob_global_refs()), so every knob the member OMITS carries its ${var.<name>} global ref and
    # grouped tasks defer to the target-wide defaults too; a knob the member SETS carries its literal.
    for p in job_parameters(cfg, runtime_knob_global_refs()):
        assert bp[p["name"]] == p["default"]
    assert bp["chunk_size"] == "500" and bp["write_concurrency"] == "4"  # per-member defaults carried
    assert bp["bulk_stats"] == "${var.bulk_stats}"        # omitted => defers to the global default
    assert bp["request_timeout"] == "${var.request_timeout}"      # omitted => global default
    assert bp["streaming_start"] == "${var.streaming_start}"      # now globally-defaulted too
    assert bp["pipeline_mode"] == "batch"                 # set on the member => literal


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
    assert job["continuous"] == {
        "pause_status": "${var.schedule_pause_status}",
        "task_retry_mode": "ON_FAILURE",
    }
    for t in job["tasks"]:
        assert t["notebook_task"]["base_parameters"]["streaming_trigger_interval"] == "30 seconds"


def test_group_continuous_conflicting_interval_fails_closed():
    with pytest.raises(ValueError, match="conflicting triggers"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", continuous="30 seconds", job_cluster_config="s"),
            _member("b.yml", "b", "idx-b", continuous="1 minute", job_cluster_config="s"),
        ])


def test_group_schedule_omitted_pause_status_inherits_global():
    # No member sets pause_status => the group schedule inherits the target-wide global (unchanged).
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch", schedule="0 0 8 * * ?"),
        _member("b.yml", "b", "idx-b", mode="batch", schedule="0 0 8 * * ?"),
    ])
    assert job["schedule"]["pause_status"] == "${var.schedule_pause_status}"


def test_group_pause_status_adopted_from_single_declaring_member():
    # One member sets pause_status, the other omits it: the declared one is adopted for the group's
    # single trigger (lenient define-once, like job_name_postfix and the cron).
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch", schedule="0 0 8 * * ?", pause_status="UNPAUSED"),
        _member("b.yml", "b", "idx-b", mode="batch", schedule="0 0 8 * * ?"),
    ])
    assert job["schedule"]["pause_status"] == "UNPAUSED"


def test_group_continuous_pause_status_override():
    # A continuous group adopts a member's pause_status onto its continuous trigger.
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", continuous="30 seconds", job_cluster_config="shared", pause_status="UNPAUSED"),
        _member("b.yml", "b", "idx-b", job_cluster_config="shared"),
    ])
    assert job["continuous"]["pause_status"] == "UNPAUSED"


def test_group_conflicting_pause_status_fails_closed():
    # A group is one job with one trigger, so one pause state: two members setting DIFFERENT values fail.
    with pytest.raises(ValueError, match="conflicting pause_status"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", mode="batch", schedule="0 0 8 * * ?", pause_status="PAUSED"),
            _member("b.yml", "b", "idx-b", mode="batch", schedule="0 0 8 * * ?", pause_status="UNPAUSED"),
        ])


def test_group_pause_status_on_demand_fails_closed():
    # A member setting pause_status while the GROUP has no trigger at all (all on-demand) fails closed:
    # there is nothing for pause_status to apply to. (The config layer allows it because the member is
    # grouped; the group resolver is where the whole-group triggerless case is caught.)
    with pytest.raises(ValueError, match="pause_status.*no trigger|no effect"):
        gen_jobs.render_group_job_yaml("g1", [
            _member("a.yml", "a", "idx-a", mode="batch", pause_status="UNPAUSED"),
            _member("b.yml", "b", "idx-b", mode="batch"),
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


def test_group_duplicate_es_index_name_warns_but_renders(capsys):
    # Two members writing the SAME es_index_name is now ALLOWED (e.g. disjoint filter_condition subsets):
    # generation proceeds and emits both tasks, but WARNS about the concurrent-write hazard on stderr.
    job = _render_group("g1", [
        _member("a.yml", "a", "shared-idx", mode="batch"),
        _member("b.yml", "b", "shared-idx", mode="batch"),
    ])
    assert len(job["tasks"]) == 2  # both tasks emitted, not rejected
    warning = capsys.readouterr().err
    assert "writing the SAME es_index_name" in warning
    assert "'shared-idx'" in warning and "disjoint" in warning.lower()


def test_group_distinct_es_index_names_ok():
    # Distinct indices are the normal case: no raise.
    job = _render_group("g1", [
        _member("a.yml", "a", "idx-a", mode="batch"),
        _member("b.yml", "b", "idx-b", mode="batch"),
    ])
    assert len(job["tasks"]) == 2


def test_group_generated_path_uses_group_prefix():
    assert gen_jobs.group_generated_path("g1").endswith("/resources/group_g1.job.yml")


# --------------------------------------------------------------------------- global job tags + es_index_list

def _render_job_tags(cfg, global_job_tags, spec=None, name="ecs_dns_activity"):
    """Render a singleton job with the given global tags; return its parsed `tags` map (or None)."""
    text = gen_jobs.render_job_yaml(f"{name}.yml", name, cfg, spec, global_job_tags)
    job = yaml.safe_load(text)["resources"]["jobs"][f"index_pipeline_{name}"]
    return job.get("tags")


def _render_group_tags(group, members, global_job_tags):
    """Render a group job with the given global tags; return its parsed `tags` map (or None)."""
    text = gen_jobs.render_group_job_yaml(group, members, global_job_tags)
    return yaml.safe_load(text)["resources"]["jobs"][f"index_pipeline_group_{group}"].get("tags")


def test_generated_singleton_carries_es_index_list_of_its_one_index():
    # A singleton job writes exactly one index; es_index_list is that name. With no global tags the job's
    # tags map is JUST the generator-owned es_index_list (never empty - a job always writes an index).
    tags = _render_job_tags(_cfg(), {})
    assert tags == {"es_index_list": "ecs-dns-activity"}


def test_generated_group_es_index_list_is_distinct_sorted_space_separated():
    # A group's es_index_list is the DISTINCT es_index_name(s) of its members, sorted, SPACE-joined -
    # deduped so two members writing the same index collapse to one entry, sorted for deterministic output.
    # Space (not comma): the Jobs API tag-value regex rejects a comma (verified live at deploy).
    members = [
        _member("b.yml", "b", "idx-b", mode="batch"),
        _member("a.yml", "a", "idx-a", mode="batch"),
        _member("c.yml", "c", "idx-a", mode="batch"),  # duplicate index -> deduped
    ]
    tags = _render_group_tags("g1", members, {})
    assert tags == {"es_index_list": "idx-a idx-b"}


def test_es_index_list_value_has_no_comma_and_matches_jobs_api_regex():
    # Guard the API constraint that made comma fail live: the es_index_list value must match the Jobs API
    # tag-value regex ^[\d \w+\-=.:/@]*$ (no comma). Assert on a multi-index group value.
    import re
    tags = _render_group_tags("g1", [
        _member("a.yml", "a", "idx-a", mode="batch"),
        _member("b.yml", "b", "idx-b", mode="batch"),
    ], {})
    value = tags["es_index_list"]
    assert "," not in value
    assert re.fullmatch(r"[\d \w+\-=.:/@]*", value)


def test_global_tags_baked_before_es_index_list_in_order():
    # Global tags are baked in FIRST (in declared order), es_index_list LAST, on both singleton and group.
    gtags = {"environment": "${var.environment}", "team": "search-platform"}
    stags = _render_job_tags(_cfg(), gtags)
    assert stags == {"environment": "${var.environment}", "team": "search-platform",
                     "es_index_list": "ecs-dns-activity"}
    assert list(stags) == ["environment", "team", "es_index_list"]
    gtags2 = _render_group_tags("g1", [_member("a.yml", "a", "idx-a", mode="batch")], gtags)
    assert list(gtags2) == ["environment", "team", "es_index_list"]


def test_global_tag_var_reference_value_preserved_verbatim():
    # A tag VALUE that is a bundle reference is baked in verbatim (resolves per target at deploy), not
    # touched by the generator - the whole point of per-target values on generated jobs.
    tags = _render_job_tags(_cfg(), {"environment": "${var.environment}"})
    assert tags["environment"] == "${var.environment}"


def test_empty_global_tags_still_emits_es_index_list_only():
    # Empty global tags => the job still carries es_index_list (generator-owned), nothing else.
    assert _render_job_tags(_cfg(), {}) == {"es_index_list": "ecs-dns-activity"}


def test_global_tags_baked_across_all_computes():
    # Global tags reach every generated job regardless of compute (serverless / existing / job_cluster).
    for compute, spec in (
        (None, None),
        ({"type": "existing_cluster", "cluster_config": "interactive_primary"}, None),
        ({"type": "job_cluster", "job_cluster_config": "std"}, {"spark_version": "17.3.x-scala2.13", "num_workers": 1}),
    ):
        tags = _render_job_tags(_cfg(compute), {"environment": "${var.environment}"}, spec)
        assert tags["environment"] == "${var.environment}"
        assert tags["es_index_list"] == "ecs-dns-activity"


def test_job_cluster_job_keeps_cluster_custom_tags_and_gets_job_tags():
    # Job-level tags and the cluster spec's own custom_tags coexist: the generator does NOT push global
    # tags into custom_tags, nor does it drop the spec's custom_tags. (Databricks forwards job tags onto
    # the cluster at deploy; the generator leaves the spec untouched.)
    spec = {"spark_version": "17.3.x-scala2.13", "num_workers": 1, "custom_tags": {"project": "elastic"}}
    text = gen_jobs.render_job_yaml("x.yml", "x", _cfg({"type": "job_cluster", "job_cluster_config": "std"}),
                                    spec, {"environment": "${var.environment}"})
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_x"]
    assert job["tags"] == {"environment": "${var.environment}", "es_index_list": "ecs-dns-activity"}
    assert job["job_clusters"][0]["new_cluster"]["custom_tags"] == {"project": "elastic"}


def test_tags_emitted_after_notification_settings():
    # Deterministic key order: tags sits right after notification_settings (keeps --check byte-stable).
    text = gen_jobs.render_job_yaml("x.yml", "x", _cfg(), None, {"environment": "e"})
    job = yaml.safe_load(text)["resources"]["jobs"]["index_pipeline_x"]
    keys = list(job)
    assert keys.index("tags") == keys.index("notification_settings") + 1


def test_es_index_list_over_cap_is_truncated_to_fit_and_warns(capsys):
    # A very long es_index_list is TRUNCATED to the tag-value cap (so the job stays deployable) and warns.
    # The value fits the cap, ends with a ` ...+N` marker, and keeps WHOLE names (truncation on a space
    # boundary), so no partial index name is emitted.
    many = [_member(f"{i}.yml", f"m{i}", f"index-{i:03d}-longname", mode="batch") for i in range(60)]
    tags = _render_group_tags("g1", many, {})
    value = tags["es_index_list"]
    err = capsys.readouterr().err
    assert "truncated" in err and "index name(s) dropped" in err
    assert len(value) <= gen_jobs._TAG_VALUE_MAX_LEN
    import re
    m = re.search(r" \.\.\.\+(\d+)$", value)
    assert m, f"expected a ' ...+N' marker, got {value!r}"
    dropped = int(m.group(1))
    kept = value[: m.start()].split()
    assert len(kept) + dropped == 60           # every name accounted for
    assert all(k.startswith("index-") and k.endswith("-longname") for k in kept)  # whole names only


def test_es_index_list_truncation_leaves_global_tags_intact(capsys):
    # Truncation only affects es_index_list; the independent global tag values are emitted unchanged.
    many = [_member(f"{i}.yml", f"m{i}", f"index-{i:03d}-longname", mode="batch") for i in range(60)]
    tags = _render_group_tags("g1", many, {"environment": "${var.environment}", "team": "search-platform"})
    assert tags["environment"] == "${var.environment}"
    assert tags["team"] == "search-platform"


def test_es_index_list_within_cap_not_truncated_and_silent(capsys):
    # A short es_index_list is emitted whole, with no marker and no warning (truncation is not spurious).
    tags = _render_job_tags(_cfg(), {})
    assert tags["es_index_list"] == "ecs-dns-activity"
    assert "truncated" not in capsys.readouterr().err


# --------------------------------------------------------------------------- load_global_job_tags / require

def _write_tags_yml(tmp_path, body):
    yml = tmp_path / "databricks.yml"
    yml.write_text(body)
    return yml


def test_load_global_job_tags_reads_map(tmp_path):
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n"
                                    "      environment: '${var.environment}'\n      team: search\n")
    assert gen_jobs.load_global_job_tags(str(yml)) == {"environment": "${var.environment}", "team": "search"}


@pytest.mark.parametrize("body", [
    "variables: {}\n",                                                  # var absent
    "variables:\n  global_job_tags:\n    type: complex\n    default: {}\n",  # empty map
    "variables:\n  global_job_tags:\n    type: complex\n",              # no default
])
def test_load_global_job_tags_absent_or_empty_is_empty(tmp_path, body):
    assert gen_jobs.load_global_job_tags(str(_write_tags_yml(tmp_path, body))) == {}


def test_load_global_job_tags_non_mapping_fails_closed(tmp_path):
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      - a\n      - b\n")
    with pytest.raises(ValueError, match="must be a mapping"):
        gen_jobs.load_global_job_tags(str(yml))


def test_load_global_job_tags_non_string_value_fails_closed(tmp_path):
    # YAML reads a bare 2026 / true as int/bool; tags are string->string, so require the operator to quote.
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      year: 2026\n")
    with pytest.raises(ValueError, match="must be a string"):
        gen_jobs.load_global_job_tags(str(yml))


def test_load_global_job_tags_reserved_es_index_list_fails_closed(tmp_path):
    # es_index_list is generator-owned; a user defining it in global_job_tags fails closed.
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      es_index_list: nope\n")
    with pytest.raises(ValueError, match="reserved"):
        gen_jobs.load_global_job_tags(str(yml))


def test_require_global_job_tags_declared_present_passes(tmp_path):
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default: {}\n")
    gen_jobs.require_global_job_tags_declared(str(yml))  # no raise


def test_require_global_job_tags_declared_missing_fails_closed(tmp_path):
    yml = _write_tags_yml(tmp_path, "variables:\n  other: {default: x}\n")
    with pytest.raises(ValueError, match="global_job_tags is not declared"):
        gen_jobs.require_global_job_tags_declared(str(yml))


def test_shipped_databricks_yml_declares_global_job_tags():
    gen_jobs.require_global_job_tags_declared()  # the repo's databricks.yml declares it
    assert gen_jobs.load_global_job_tags() == {}  # ships empty on main


@pytest.mark.parametrize("filename,job_key", [
    ("deploy_views.job.yml", "deploy_views"),
    ("checkpoint_clear.job.yml", "checkpoint_clear"),
    ("build_wheel.job.yml", "build_wheel"),
    ("es_diagnostics.job.yml", "es_diagnostics"),
])
def test_shipped_fixed_jobs_reference_global_job_tags(filename, job_key):
    # The 4 hand-authored jobs are NOT emitted by gen_jobs, so guard against drift: each must reference the
    # whole global_job_tags var so it gets the same global tags every generated job carries.
    path = os.path.join(_REPO_ROOT, "resources", filename)
    with open(path) as fh:
        job = yaml.safe_load(fh)["resources"]["jobs"][job_key]
    assert job["tags"] == "${var.global_job_tags}"


def test_load_global_job_tags_comma_in_value_fails_closed(tmp_path):
    # A literal tag VALUE with a comma is rejected by the Jobs API at deploy; catch it at generation
    # (fail-closed-at-generation, same contract es_index_list follows) rather than letting it deploy-fail.
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      owners: 'a,b'\n")
    with pytest.raises(ValueError, match="comma is NOT allowed"):
        gen_jobs.load_global_job_tags(str(yml))


def test_load_global_job_tags_bad_char_in_key_fails_closed(tmp_path):
    # A disallowed character in a tag KEY is caught at generation too.
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      'a,b': x\n")
    with pytest.raises(ValueError, match="rejects in a tag"):
        gen_jobs.load_global_job_tags(str(yml))


def test_load_global_job_tags_var_reference_value_allowed(tmp_path):
    # A ${...} bundle reference is NOT validated against the tag regex (it contains { } which the regex
    # forbids, but its DEPLOY-resolved form is what matters). So environment: ${var.environment} is fine.
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      environment: '${var.environment}'\n")
    assert gen_jobs.load_global_job_tags(str(yml)) == {"environment": "${var.environment}"}


def test_load_global_job_tags_over_tag_cap_fails_closed(tmp_path):
    # 25 global tags + the generator-owned es_index_list = 26 > Databricks' 25-tag cap; fail closed at
    # generation (leave room for es_index_list: at most 24 global tags).
    body = ["variables:", "  global_job_tags:", "    type: complex", "    default:"]
    body += [f"      k{i}: v{i}" for i in range(25)]
    yml = _write_tags_yml(tmp_path, "\n".join(body) + "\n")
    with pytest.raises(ValueError, match="at most 24 global tags"):
        gen_jobs.load_global_job_tags(str(yml))


def test_load_global_job_tags_at_tag_cap_boundary_passes(tmp_path):
    # 24 global tags is the boundary: 24 + es_index_list = 25 = the cap, so it is allowed.
    body = ["variables:", "  global_job_tags:", "    type: complex", "    default:"]
    body += [f"      k{i}: v{i}" for i in range(24)]
    yml = _write_tags_yml(tmp_path, "\n".join(body) + "\n")
    assert len(gen_jobs.load_global_job_tags(str(yml))) == 24


def test_load_global_job_tags_comma_beside_reference_fails_closed(tmp_path):
    # A value MIXING a literal bad char with a ${...} reference must still fail: only the ${...} span is
    # stripped, the literal remainder ("a,b") is validated, so the comma is caught at generation.
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      owners: 'a,b${var.x}'\n")
    with pytest.raises(ValueError, match="comma is NOT allowed"):
        gen_jobs.load_global_job_tags(str(yml))


def test_load_global_job_tags_literal_around_reference_allowed(tmp_path):
    # A clean literal around a reference passes: "prod-${var.environment}" -> literal "prod-" is legal.
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      environment: 'prod-${var.environment}'\n")
    assert gen_jobs.load_global_job_tags(str(yml)) == {"environment": "prod-${var.environment}"}


def test_load_global_job_tags_non_ascii_char_fails_closed(tmp_path):
    # \w is matched ASCII-only, so a non-ASCII letter (which the server regex rejects) fails at generation
    # rather than passing here and being rejected at deploy.
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      team: café\n")
    with pytest.raises(ValueError, match="rejects in a tag"):
        gen_jobs.load_global_job_tags(str(yml))


def test_load_global_job_tags_reserved_key_error_wins_over_count(tmp_path):
    # With es_index_list plus 24 other keys (25 total), the per-key reserved check runs BEFORE the count
    # check, so the operator sees the accurate 'reserved' message, not the generic 'at most 24' one.
    body = ["variables:", "  global_job_tags:", "    type: complex", "    default:", "      es_index_list: x"]
    body += [f"      k{i}: v{i}" for i in range(24)]
    yml = _write_tags_yml(tmp_path, "\n".join(body) + "\n")
    with pytest.raises(ValueError, match="reserved"):
        gen_jobs.load_global_job_tags(str(yml))


def test_load_global_job_tags_reference_shaped_key_fails_closed(tmp_path):
    # A KEY is a literal (references belong in values); a ${...}-shaped key must fail closed at generation,
    # not be reduced to '' by the ref-strip and baked verbatim. ($ { } are all outside the tag regex.)
    yml = _write_tags_yml(tmp_path, "variables:\n  global_job_tags:\n    type: complex\n    default:\n      '${var.foo}': x\n")
    with pytest.raises(ValueError, match="key must be a literal"):
        gen_jobs.load_global_job_tags(str(yml))


def test_es_index_list_multidigit_dropped_count_stays_within_cap():
    # Marker width is derived from the max possible count, so even a 3-digit dropped count keeps the value
    # within the cap and the count digits intact (guards the reserved-budget math without needing 10^11).
    import re
    names = [f"ix-{i:04d}" for i in range(200)]  # 200 short names -> dropped is 3 digits
    value, dropped = gen_jobs._es_index_list_value(names)
    assert len(value) <= gen_jobs._TAG_VALUE_MAX_LEN
    m = re.search(r"\.\.\.\+(\d+)$", value)
    assert m and int(m.group(1)) == dropped        # the full count survives (not sliced)
    assert len(value.split()[:-1]) + dropped == 200  # every name kept-or-counted

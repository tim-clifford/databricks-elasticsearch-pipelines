"""Offline unit tests for pipeline_lib.config. No Spark, no cluster, no live ES: plain pytest.

Covers the validation contract (every fail-closed branch), the ${environment} template + resolution
logic, and the derivations (view_substitutions, job_base_parameters).
"""
import copy

import pytest

from pipeline_lib.config import (
    _RUNTIME_KNOBS,
    RUNTIME_KNOB_NAMES,
    PipelineConfigError,
    column_present,
    job_base_parameters,
    job_parameters,
    render_view_sql,
    runtime_knob_global_refs,
    require_chunk_size,
    require_es_flag,
    require_filter_condition,
    require_max_bytes_per_trigger,
    require_max_files_per_trigger,
    require_max_partition_bytes,
    require_pause_status,
    require_pipeline_mode,
    require_request_timeout,
    require_streaming_start,
    require_transport_max_retries,
    require_trigger_interval,
    require_write_concurrency,
    require_write_repartition,
    resolve_config,
    resolve_name,
    shared_view_conflict,
    validate_config,
    view_select_body,
    view_substitutions,
    write_config_overrides,
)


# job_base_parameters grew connection + checkpoint refs; a tiny helper keeps the call sites readable.
def _job_base_parameters(config_name):
    return job_base_parameters(
        config_name,
        environment_ref="${var.environment}",
        wheel_path_ref="${var.wheel_path}",
        es_host_url_ref="${var.es_host_primary.es_host_url}",
        secret_scope_name_ref="${var.es_host_primary.secret_scope_name}",
        secret_key_name_ref="${var.es_host_primary.secret_key_name}",
        checkpoint_base_path_ref="${var.checkpoint_base_path}",
        ca_certs_ref="${var.ca_certs}",
    )


def _base():
    """A minimal valid config (no reference tables, no environment tokens)."""
    return {
        "es_index_name": "ecs-dns-activity",
        "es_id_field": "dsl_id",
        "es_host_config": "es_host_primary",
        "pipeline_mode": "batch",
        "view": {"catalog": "cat", "schema": "es_poc", "name": "ecs_dns_activity"},
        "source": {"catalog": "cat", "schema": "ocsf", "table": "dns_activity", "primary_key": "dsl_id"},
    }


def _with_env():
    """A config using ${environment} in catalog and a reference schema."""
    cfg = _base()
    cfg["view"]["catalog"] = "acme_${environment}"
    cfg["source"]["catalog"] = "acme_${environment}"
    cfg["reference_tables"] = {
        "validation": {
            "catalog": "acme_${environment}",
            "schema": "ocsf_validation_${environment}",
            "table": "dns_activity",
        },
        "geo": {"catalog": "acme_${environment}", "schema": "ref", "table": "geoip"},
    }
    return cfg


# --------------------------------------------------------------------------- valid configs


def test_minimal_valid():
    out = validate_config(_base())
    assert out["view"] == {"catalog": "cat", "schema": "es_poc", "name": "ecs_dns_activity"}
    assert out["reference_tables"] == {}
    # The optional tuning knobs, omitted here, default to "" (canonical "unset" => connector default).
    assert out["chunk_size"] == ""
    assert out["write_concurrency"] == ""
    assert out["require_existing_index"] == ""
    assert out["verify_certs"] == ""
    # bulk_stats now behaves like the other bool knobs: omitted => "" (unset at the config level), which
    # defers to the global ${var.bulk_stats} default (baked by the generator) and ultimately the
    # connector default (off). It is no longer forced "true" here.
    assert out["bulk_stats"] == ""
    # write_repartition and max_partition_bytes now store "" when omitted (like the other knobs), so the
    # job-parameter default can inherit the target-wide global ${var.*}. Their built-in defaults (0 / 2m)
    # are applied by the RUNNER as the final fallback when the effective value is empty, so an omitted
    # config with an empty global still gets the same effective default; see the runner-fallback tests.
    assert out["write_repartition"] == ""
    assert out["max_partition_bytes"] == ""
    # pipeline_mode and streaming_start are OPTIONAL too: omitted => "" (inherit the ${var.*} global,
    # defaulting to batch / new). This config sets pipeline_mode: batch, so it is not "".
    assert out["pipeline_mode"] == "batch"
    assert out["streaming_start"] == ""


def test_write_concurrency_config_default_parsed_and_validated():
    cfg = _base()
    cfg["write_concurrency"] = 8
    assert validate_config(cfg)["write_concurrency"] == "8"   # YAML int -> canonical string form
    cfg["write_concurrency"] = 0                              # not a positive int
    with pytest.raises(PipelineConfigError, match="write_concurrency"):
        validate_config(cfg)


def test_bulk_stats_defaults_unset_when_omitted():
    # bulk_stats now matches verify_certs: an omitted value is "" (unset at the config level), deferring
    # to the global ${var.bulk_stats} default / connector default (off), NOT forced "true".
    assert validate_config(_base())["bulk_stats"] == ""


@pytest.mark.parametrize("blank", ["", "  ", None])
def test_bulk_stats_explicit_blank_is_unset(blank):
    # An explicit empty/blank/null bulk_stats is treated as unset ("") - the same "defer to the global
    # default" state as omission (only an explicit true/false is a per-pipeline override).
    cfg = _base()
    cfg["bulk_stats"] = blank
    assert validate_config(cfg)["bulk_stats"] == ""


@pytest.mark.parametrize("value,expected", [(True, "true"), (False, "false"), ("true", "true"), ("FALSE", "false")])
def test_bulk_stats_explicit_value_canonicalized(value, expected):
    cfg = _base()
    cfg["bulk_stats"] = value
    assert validate_config(cfg)["bulk_stats"] == expected   # YAML bool / string -> canonical form


@pytest.mark.parametrize("bad", ["maybe", "1", 0, "yes"])
def test_bulk_stats_invalid_rejected(bad):
    cfg = _base()
    cfg["bulk_stats"] = bad
    with pytest.raises(PipelineConfigError, match="bulk_stats"):
        validate_config(cfg)


def test_bulk_stats_carried_through_resolve():
    # bulk_stats is a connector setting, not an object name: resolve_config passes it through verbatim.
    cfg = _base()
    cfg["bulk_stats"] = False
    assert resolve_config(validate_config(cfg), "")["bulk_stats"] == "false"


def test_environment_token_accepted_as_template():
    # validate_config accepts the template; it does NOT resolve it.
    out = validate_config(_with_env())
    assert out["source"]["catalog"] == "acme_${environment}"
    assert out["reference_tables"]["validation"]["schema"] == "ocsf_validation_${environment}"


# --------------------------------------------------------------------------- fail-closed: structure


@pytest.mark.parametrize("missing", ["es_index_name", "view", "source"])
def test_missing_required_key(missing):
    cfg = _base()
    del cfg[missing]
    with pytest.raises(PipelineConfigError, match="missing required key"):
        validate_config(cfg)


def test_pipeline_mode_optional_defaults_empty_when_omitted():
    # pipeline_mode is no longer required: omit it and it stores "" (inherit the ${var.pipeline_mode}
    # global, which databricks.yml defaults to batch), rather than failing closed. A PRESENT value is
    # still allow-list validated.
    cfg = {k: v for k, v in _base().items() if k != "pipeline_mode"}
    assert validate_config(cfg)["pipeline_mode"] == ""
    assert validate_config({**cfg, "pipeline_mode": "streaming"})["pipeline_mode"] == "streaming"
    with pytest.raises(PipelineConfigError, match="pipeline_mode"):
        validate_config({**cfg, "pipeline_mode": "turbo"})


def test_source_primary_key_optional():
    # primary_key is OPTIONAL (informational only, not read at runtime): a source that omits it
    # validates fine and stores None, rather than failing closed.
    cfg = _base()
    del cfg["source"]["primary_key"]
    out = validate_config(cfg)
    assert out["source"]["primary_key"] is None


def test_source_primary_key_rejects_environment_token():
    # A PRESENT primary_key is a column identifier, not an object name: no ${environment} token.
    cfg = _base()
    cfg["source"]["primary_key"] = "id_${environment}"
    with pytest.raises(PipelineConfigError, match="source.primary_key"):
        validate_config(cfg)


def test_es_id_field_and_primary_key_independent():
    # The two keys are distinct contexts and need not share a value.
    cfg = _base()
    cfg["es_id_field"] = "event_id"
    cfg["source"]["primary_key"] = "row_key"
    out = validate_config(cfg)
    assert out["es_id_field"] == "event_id"
    assert out["source"]["primary_key"] == "row_key"


@pytest.mark.parametrize("bad", ["has-hyphen", "has space", "1leading", "", None, 5])
def test_illegal_es_id_field_rejected(bad):
    # A PRESENT es_id_field must be a legal identifier. An explicit empty/null/invalid value still fails
    # closed (only OMISSION - the key absent entirely - defers to ES auto-ids; see the next test).
    cfg = _base()
    cfg["es_id_field"] = bad
    with pytest.raises(PipelineConfigError, match="es_id_field"):
        validate_config(cfg)


def test_es_id_field_optional_when_omitted():
    # es_id_field is optional: OMITTING the key validates and returns None, which the runner passes to
    # the connector as its "no id_field" default (ES auto-generates each _id). Distinct from an explicit
    # empty/null value, which fails closed above.
    cfg = _base()
    del cfg["es_id_field"]
    assert validate_config(cfg)["es_id_field"] is None


def test_es_id_field_none_survives_resolve():
    # An omitted es_id_field (None) must pass through resolve_config unchanged (it is a passthrough, not
    # an object name), so the resolved config the notebook reads still carries None.
    cfg = _with_env()
    del cfg["es_id_field"]
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["es_id_field"] is None


@pytest.mark.parametrize("mode", ["batch", "streaming"])
def test_pipeline_mode_allowed_values(mode):
    cfg = _base()
    cfg["pipeline_mode"] = mode
    assert validate_config(cfg)["pipeline_mode"] == mode


def test_pipeline_mode_reset_checkpoint_removed():
    # reset_checkpoint is no longer a pipeline_mode in ANY context (clearing a checkpoint is the
    # dedicated `_checkpoint clear` job). It must fail closed both as a config default (validate_config)
    # and on the run-time-override path (require_pipeline_mode), where it was previously accepted -
    # this pins the removal so it cannot silently return.
    cfg = _base()
    cfg["pipeline_mode"] = "reset_checkpoint"
    with pytest.raises(PipelineConfigError, match="pipeline_mode"):
        validate_config(cfg)
    with pytest.raises(PipelineConfigError, match="pipeline_mode"):
        require_pipeline_mode("reset_checkpoint", "pipeline_mode job parameter")


@pytest.mark.parametrize(
    "bad", ["Batch", "BATCH", "stream", "micro-batch", "reset_checkpoint", "", None, 5, True]
)
def test_pipeline_mode_rejects_non_allowlisted(bad):
    # Allow-list: only exactly 'batch'/'streaming'. A near-miss, wrong case, empty, non-string, or the
    # removed reset_checkpoint value must fail closed - never silently defaulted.
    cfg = _base()
    cfg["pipeline_mode"] = bad
    with pytest.raises(PipelineConfigError, match="pipeline_mode"):
        validate_config(cfg)


def test_pipeline_mode_carried_through_resolve():
    # pipeline_mode is a passthrough (not an object name): resolve must keep it verbatim.
    out = resolve_config(validate_config(_with_env()), environment="prod")
    assert out["pipeline_mode"] == "batch"


# --------------------------------------------------------------------------- es_host_config


def test_es_host_config_round_trips():
    cfg = _base()
    cfg["es_host_config"] = "es_host_secondary"
    assert validate_config(cfg)["es_host_config"] == "es_host_secondary"


def test_es_host_config_optional_when_omitted():
    # es_host_config is optional: an omitted key validates and returns None, so the generator can fall
    # back to the bundle's default_es_host_config (databricks.yml). Only OMISSION defers to the default.
    cfg = _base()
    del cfg["es_host_config"]
    assert validate_config(cfg)["es_host_config"] is None


@pytest.mark.parametrize("bad", ["has-hyphen", "has space", "1leading", "a.b", "", None, 5])
def test_illegal_es_host_config_rejected(bad):
    # es_host_config becomes part of a ${var.<name>.field} reference, so it must be a bare identifier:
    # a hyphen/dot/space or non-string fails closed (it would otherwise emit a broken variable ref).
    cfg = _base()
    cfg["es_host_config"] = bad
    with pytest.raises(PipelineConfigError, match="es_host_config"):
        validate_config(cfg)


def test_es_host_config_carried_through_resolve():
    # es_host_config names a bundle variable, not an object name: no ${environment} folding; resolve
    # must keep it verbatim so the generated ${var.<name>.*} refs stay intact.
    out = resolve_config(validate_config(_with_env()), environment="prod")
    assert out["es_host_config"] == "es_host_primary"


def test_unknown_top_level_key():
    cfg = _base()
    cfg["source_table"] = "oops"  # a plausible legacy key from the old flat schema
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


def test_object_missing_catalog():
    cfg = _base()
    del cfg["view"]["catalog"]
    with pytest.raises(PipelineConfigError, match="view.catalog"):
        validate_config(cfg)


def test_unknown_nested_key():
    cfg = _base()
    cfg["source"]["tabel"] = "typo"
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


# --------------------------------------------------------------------------- fail-closed: templates


@pytest.mark.parametrize("bad", ["my-schema", "my schema", "cat.schema", "", "${env}", "a${environ}b", None, 5])
def test_illegal_name_template_rejected(bad):
    cfg = _base()
    cfg["source"]["schema"] = bad
    with pytest.raises(PipelineConfigError):
        validate_config(cfg)


def test_leading_digit_template_rejected_at_resolve():
    # "1abc" matches the template char class but is not a legal identifier; caught at resolve.
    cfg = _base()
    cfg["source"]["schema"] = "1abc"
    validated = validate_config(cfg)  # template chars are legal
    with pytest.raises(PipelineConfigError, match="not a legal SQL identifier"):
        resolve_config(validated, environment="")


@pytest.mark.parametrize("bad", ["Has-Caps", "UPPER", "has space", ".leading", "-leading", "_leading", "+leading", "", "bad/name", "trailing."])
def test_illegal_es_index_rejected(bad):
    cfg = _base()
    cfg["es_index_name"] = bad
    with pytest.raises(PipelineConfigError, match="es_index_name"):
        validate_config(cfg)


def test_es_index_length_bound():
    cfg = _base()
    cfg["es_index_name"] = "a" * 255
    assert validate_config(cfg)["es_index_name"] == "a" * 255  # 255 bytes OK
    cfg["es_index_name"] = "a" * 256
    with pytest.raises(PipelineConfigError, match="255 bytes"):
        validate_config(cfg)


def test_reference_broadcast_key_now_unknown():
    # broadcast was removed: join tuning is the view author's job, written in SQL. A leftover
    # `broadcast` key must be rejected as unknown rather than silently accepted.
    cfg = _base()
    cfg["reference_tables"] = {"v": {"catalog": "c", "schema": "s", "table": "t", "broadcast": True}}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


def test_reference_alias_rejects_environment_token():
    # An alias is internal; it must be a bare identifier, not a template.
    cfg = _base()
    cfg["reference_tables"] = {"a_${environment}": {"catalog": "c", "schema": "s", "table": "t"}}
    with pytest.raises(PipelineConfigError):
        validate_config(cfg)


# --------------------------------------------------------------------------- resolve_name


def test_resolve_name_no_token_passthrough():
    assert resolve_name("ocsf", environment="", where="x") == "ocsf"
    assert resolve_name("ocsf", environment="prod", where="x") == "ocsf"


def test_resolve_name_folds_environment():
    assert resolve_name("ocsf_${environment}", environment="prod", where="x") == "ocsf_prod"
    assert resolve_name("acme_${environment}", environment="catalog", where="x") == "acme_catalog"


def test_resolve_name_missing_environment_fails():
    with pytest.raises(PipelineConfigError, match="no environment"):
        resolve_name("ocsf_${environment}", environment="", where="x")


@pytest.mark.parametrize("env", ["has-hyphen", "has space", "has.dot"])
def test_resolve_name_illegal_environment_fails(env):
    with pytest.raises(PipelineConfigError, match="not a legal SQL identifier"):
        resolve_name("ocsf_${environment}", environment=env, where="x")


@pytest.mark.parametrize("env", ["back\\slash", "\\1", "a\\g<0>b"])
def test_resolve_name_backslash_environment_fails_closed(env):
    # str.replace (not re.sub): a backslash/group-ref in the env value must raise PipelineConfigError
    # (illegal identifier), never an uncaught re.error.
    with pytest.raises(PipelineConfigError, match="not a legal SQL identifier"):
        resolve_name("ocsf_${environment}", environment=env, where="x")


def test_name_and_table_reject_environment_token():
    # ${environment} belongs only in catalog/schema, never in a view name or table name.
    for key, obj in (("name", "view"), ("table", "source")):
        cfg = _base()
        cfg[obj][key] = "thing_${environment}"
        with pytest.raises(PipelineConfigError, match=f"{obj}.{key}"):
            validate_config(cfg)


def test_reference_table_rejects_environment_token():
    cfg = _base()
    cfg["reference_tables"] = {"v": {"catalog": "c", "schema": "s_${environment}", "table": "t_${environment}"}}
    with pytest.raises(PipelineConfigError, match="table"):
        validate_config(cfg)


# --------------------------------------------------------------------------- resolve_config


def test_resolve_config_folds_everywhere():
    out = resolve_config(validate_config(_with_env()), environment="prod")
    assert out["view"]["catalog"] == "acme_prod"
    assert out["source"]["catalog"] == "acme_prod"
    assert out["reference_tables"]["validation"]["schema"] == "ocsf_validation_prod"
    assert out["reference_tables"]["geo"]["schema"] == "ref"  # no token -> unchanged


def test_resolve_config_carries_source_primary_key():
    # primary_key is a column identifier: resolve must pass it through unchanged, not drop it or
    # try to fold ${environment} into it.
    out = resolve_config(validate_config(_with_env()), environment="prod")
    assert out["source"]["primary_key"] == "dsl_id"
    assert out["es_id_field"] == "dsl_id"


def test_resolve_config_carries_omitted_primary_key_as_none():
    # An omitted primary_key stays None through resolve (it is informational only, never resolved).
    cfg = _with_env()
    del cfg["source"]["primary_key"]
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["source"]["primary_key"] is None


# --------------------------------------------------------------------------- view_substitutions


def test_view_substitutions_fqn_and_no_refs():
    subs = view_substitutions(validate_config(_base()), environment="")
    assert subs["view"] == "cat.es_poc.ecs_dns_activity"
    assert subs["source"] == "cat.ocsf.dns_activity"
    assert not any(k.startswith("ref_") for k in subs)
    assert "broadcast_hint" not in subs  # broadcast is no longer a framework concern


def test_view_substitutions_env_ref_alias():
    subs = view_substitutions(validate_config(_with_env()), environment="catalog")
    assert subs["view"] == "acme_catalog.es_poc.ecs_dns_activity"
    assert subs["source"] == "acme_catalog.ocsf.dns_activity"
    # ref_<alias> is the aliased FQN, with environment folded into catalog + schema
    assert subs["ref_validation"] == "acme_catalog.ocsf_validation_catalog.dns_activity validation"
    assert subs["ref_geo"] == "acme_catalog.ref.geoip geo"


def test_view_substitutions_missing_env_fails():
    with pytest.raises(PipelineConfigError, match="no environment"):
        view_substitutions(validate_config(_with_env()), environment="")


def test_view_substitutions_source_override_only_changes_source():
    # The streaming seam: source_override replaces ${source} with the given name (a micro-batch temp
    # view), while ${view} and every ${ref_*} keep their real, fully-qualified values.
    subs = view_substitutions(validate_config(_with_env()), environment="catalog", source_override="__batch_src")
    assert subs["source"] == "__batch_src"
    assert subs["view"] == "acme_catalog.es_poc.ecs_dns_activity"
    assert subs["ref_validation"] == "acme_catalog.ocsf_validation_catalog.dns_activity validation"


def test_view_substitutions_source_override_none_is_real_source():
    # None (the default, what deploy_views uses) keeps the real source FQN.
    subs = view_substitutions(validate_config(_base()), environment="", source_override=None)
    assert subs["source"] == "cat.ocsf.dns_activity"


# --------------------------------------------------------------------------- shared_view_conflict


def test_shared_view_conflict_single_config_is_none():
    # The trivial case (one pipeline owns the view) is never a conflict.
    assert shared_view_conflict([("a.yml", validate_config(_base()))], "") is None


def test_shared_view_conflict_identical_definition_is_none():
    # Two pipelines sharing a view but differing ONLY on the ES-write side (index, host, id, filter) is
    # the supported shared-view case: same view definition => no conflict.
    a = _base()
    b = _base()
    b["es_index_name"] = "ecs-dns-activity-secondary"
    b["es_host_config"] = "es_host_secondary"
    b["es_id_field"] = "event_id"
    b["filter_condition"] = "action = 'blocked'"
    assert shared_view_conflict(
        [("a.yml", validate_config(a)), ("b.yml", validate_config(b))], ""
    ) is None


def test_shared_view_conflict_different_source_reported():
    # Same view NAME but a different source => different CREATE statement => conflict (fail closed).
    a = _base()
    b = _base()
    b["source"]["table"] = "dns_activity_backup"
    msg = shared_view_conflict([("a.yml", validate_config(a)), ("b.yml", validate_config(b))], "")
    assert msg is not None
    assert "CONFLICTING" in msg and "a.yml" in msg and "b.yml" in msg


def test_shared_view_conflict_different_reference_tables_reported():
    # A different reference-table join changes the rendered view definition => conflict.
    a = _base()
    b = _base()
    b["reference_tables"] = {"geo": {"catalog": "cat", "schema": "ref", "table": "geoip"}}
    msg = shared_view_conflict([("a.yml", validate_config(a)), ("b.yml", validate_config(b))], "")
    assert msg is not None and "CONFLICTING" in msg


def test_shared_view_conflict_env_resolves_identically_is_none():
    # ${environment} templates that resolve to the same FQNs under the given environment are NOT a
    # conflict (the comparison is on the resolved substitutions, not the raw templates).
    a = _with_env()
    b = _with_env()
    b["es_index_name"] = "ecs-dns-activity-2"
    assert shared_view_conflict(
        [("a.yml", validate_config(a)), ("b.yml", validate_config(b))], "prod"
    ) is None


# --------------------------------------------------------------------------- render_view_sql


def test_render_view_sql_substitutes_known_tokens():
    sql = "SELECT * FROM ${source} base LEFT JOIN ${ref_v} ON base.id = v.id -- creates ${view}"
    subs = {"source": "c.s.t", "ref_v": "c.s.ref v", "view": "c.s.myview"}
    out = render_view_sql(sql, subs, "f.sql")
    assert "FROM c.s.t base" in out
    assert "LEFT JOIN c.s.ref v ON" in out
    assert out.endswith("c.s.myview")  # the token inside the comment is substituted too


def test_render_view_sql_unknown_token_fails_closed():
    with pytest.raises(PipelineConfigError, match=r"unknown parameter \$\{nope\}"):
        render_view_sql("SELECT * FROM ${nope}", {"source": "c.s.t"}, "f.sql")


def test_render_view_sql_no_tokens_is_identity():
    assert render_view_sql("SELECT 1", {"source": "c.s.t"}, "f.sql") == "SELECT 1"


# --------------------------------------------------------------------------- view_select_body


def test_view_select_body_strips_ddl_prefix():
    sql = (
        "-- a leading comment mentioning ${view}\n"
        "CREATE OR REPLACE VIEW ${view} AS\n"
        "SELECT a, b FROM ${source} base"
    )
    assert view_select_body(sql, "f.sql") == "SELECT a, b FROM ${source} base"


def test_view_select_body_case_insensitive_and_whitespace():
    sql = "create   or  replace   view   ${view}   as\n  SELECT 1 FROM ${source}\n"
    assert view_select_body(sql, "f.sql") == "SELECT 1 FROM ${source}"


@pytest.mark.parametrize("sql", [
    "SELECT a FROM ${source}",                       # no CREATE VIEW prefix at all
    "CREATE OR REPLACE VIEW real.view.name AS SELECT 1",  # not keyed off the ${view} token
    "CREATE VIEW ${view} AS SELECT 1",               # missing 'OR REPLACE'
])
def test_view_select_body_rejects_non_framework_shape(sql):
    with pytest.raises(PipelineConfigError, match="CREATE OR REPLACE VIEW"):
        view_select_body(sql, "f.sql")


def test_view_select_body_rejects_empty_body():
    with pytest.raises(PipelineConfigError, match="no SELECT body"):
        view_select_body("CREATE OR REPLACE VIEW ${view} AS   \n  ", "f.sql")


# --------------------------------------------------------------------------- require_streaming_start


@pytest.mark.parametrize("value", ["new", "full"])
def test_require_streaming_start_accepts_allowed(value):
    assert require_streaming_start(value, "streaming_start job parameter") == value


@pytest.mark.parametrize("bad", ["New", "FULL", "latest", "", None, "backfill", 5])
def test_require_streaming_start_rejects_bad(bad):
    with pytest.raises(PipelineConfigError, match="streaming_start"):
        require_streaming_start(bad, "streaming_start job parameter")


# --------------------------------------------------------------------------- job_base_parameters


def test_job_base_parameters():
    params = _job_base_parameters("ecs_dns_activity")
    assert params == {
        "config_name": "ecs_dns_activity",
        "environment": "${var.environment}",
        "wheel_path": "${var.wheel_path}",
        "es_host_url": "${var.es_host_primary.es_host_url}",
        "secret_scope_name": "${var.es_host_primary.secret_scope_name}",
        "secret_key_name": "${var.es_host_primary.secret_key_name}",
        "checkpoint_base_path": "${var.checkpoint_base_path}",
        "ca_certs": "${var.ca_certs}",
        "streaming_trigger_interval": "",
    }


def test_job_base_parameters_streaming_trigger_interval():
    # streaming_trigger_interval is a base_parameter (deploy-time, from the continuous block): it takes
    # the passed literal, and defaults to "" (availableNow) when omitted.
    assert _job_base_parameters("x")["streaming_trigger_interval"] == ""
    params = job_base_parameters(
        "x", "${var.environment}", "${var.wheel_path}", "${var.es_host_primary.es_host_url}",
        "${var.es_host_primary.secret_scope_name}", "${var.es_host_primary.secret_key_name}",
        "${var.checkpoint_base_path}", "${var.ca_certs}", "30 seconds",
    )
    assert params["streaming_trigger_interval"] == "30 seconds"


def test_job_base_parameters_excludes_run_time_params():
    # Run-time job parameters (pipeline_mode, filter_condition, streaming_start, and the EsWriteConfig
    # tuning knobs) must NOT leak into base_parameters, which would re-fix them at deploy and defeat
    # per-run override.
    params = _job_base_parameters("x")
    for run_time in ("pipeline_mode", "filter_condition", "chunk_size", "write_concurrency",
                     "request_timeout", "transport_max_retries", "require_existing_index",
                     "verify_certs", "bulk_stats", "streaming_start", "write_repartition", "max_partition_bytes"):
        assert run_time not in params


# --------------------------------------------------------------------------- job_parameters


@pytest.mark.parametrize("mode", ["batch", "streaming"])
def test_job_parameters_pipeline_mode_default_from_config(mode):
    # The pipeline_mode job parameter's default is the config's pipeline_mode (the per-index choice).
    cfg = _base()
    cfg["pipeline_mode"] = mode
    params = job_parameters(validate_config(cfg))
    assert {"name": "pipeline_mode", "default": mode} in params


def test_job_parameters_full_shape_and_order():
    # The generated job exposes exactly these run-time parameters, in this order (the _RUNTIME_KNOBS
    # registry order). With NO global_refs supplied (the pure/unit-test call), every default is the
    # config value or "" - pipeline_mode comes from the config here (batch), filter_condition is set, and
    # every omitted knob (including streaming_start / write_repartition / max_partition_bytes, which now
    # store "" when omitted) defaults to "".
    cfg = _base()
    cfg["filter_condition"] = "action = 'allowed'"
    assert job_parameters(validate_config(cfg)) == [
        {"name": "pipeline_mode", "default": "batch"},
        {"name": "filter_condition", "default": "action = 'allowed'"},
        {"name": "chunk_size", "default": ""},
        {"name": "write_concurrency", "default": ""},
        {"name": "op_type", "default": ""},
        {"name": "request_timeout", "default": ""},
        {"name": "transport_max_retries", "default": ""},
        {"name": "require_existing_index", "default": ""},
        {"name": "verify_certs", "default": ""},
        {"name": "bulk_stats", "default": ""},
        {"name": "retry_transport_timeout", "default": ""},
        {"name": "bypass_fast_path", "default": ""},
        {"name": "streaming_start", "default": ""},
        {"name": "write_repartition", "default": ""},
        {"name": "max_partition_bytes", "default": ""},
        {"name": "max_files_per_trigger", "default": ""},
        {"name": "max_bytes_per_trigger", "default": ""},
    ]


def test_job_parameters_full_shape_with_global_refs():
    # With runtime_knob_global_refs() (what the generator passes), every knob the config OMITS bakes its
    # ${var.<name>} global reference as the default; a knob the config SETS bakes its literal value. Here
    # only pipeline_mode is set (batch), so it stays literal and every other knob inherits its global.
    refs = runtime_knob_global_refs()
    params = job_parameters(validate_config(_base()), refs)
    by_name = {p["name"]: p["default"] for p in params}
    assert by_name["pipeline_mode"] == "batch"                     # set in the config -> literal
    for name in RUNTIME_KNOB_NAMES:
        if name != "pipeline_mode":
            assert by_name[name] == "${var." + name + "}"          # omitted -> global ref
    # Every registry knob appears exactly once, in registry order.
    assert [p["name"] for p in params] == list(RUNTIME_KNOB_NAMES)


def test_job_parameters_bulk_stats_defaults_empty_without_ref():
    # With no ref supplied (the pure/unit-test call), an omitted bulk_stats stays "" - the connector's
    # own default (off) stands.
    params = job_parameters(validate_config(_base()))
    assert {"name": "bulk_stats", "default": ""} in params


def test_job_parameters_bulk_stats_omitted_uses_global_ref():
    # When the config OMITS bulk_stats, the generator's ref (runtime_knob_global_refs) becomes the
    # job-parameter default, so an omitted pipeline defers to the target-wide ${var.bulk_stats} global.
    params = job_parameters(validate_config(_base()), runtime_knob_global_refs())
    assert {"name": "bulk_stats", "default": "${var.bulk_stats}"} in params


@pytest.mark.parametrize("value,expected", [(True, "true"), (False, "false")])
def test_job_parameters_bulk_stats_config_value_overrides_ref(value, expected):
    # A config that SETS bulk_stats bakes its literal value, overriding the global ref (per-pipeline
    # wins over the target-wide default).
    cfg = _base()
    cfg["bulk_stats"] = value
    params = job_parameters(validate_config(cfg), runtime_knob_global_refs())
    assert {"name": "bulk_stats", "default": expected} in params


def test_job_parameters_reliability_knobs_default_empty_without_ref():
    # With no refs supplied (the pure/unit-test call), omitted request_timeout / transport_max_retries
    # stay "" - the connector's own defaults stand.
    params = job_parameters(validate_config(_base()))
    assert {"name": "request_timeout", "default": ""} in params
    assert {"name": "transport_max_retries", "default": ""} in params


def test_job_parameters_retry_transport_timeout_defaults_empty_without_ref():
    # With no ref supplied (the pure/unit-test call), an omitted retry_transport_timeout stays "" - the
    # connector's own default (off) stands.
    params = job_parameters(validate_config(_base()))
    assert {"name": "retry_transport_timeout", "default": ""} in params


def test_job_parameters_retry_transport_timeout_omitted_uses_global_ref():
    # When the config OMITS retry_transport_timeout, the generator's ref becomes the job-parameter
    # default, so an omitted pipeline defers to ${var.retry_transport_timeout}.
    params = job_parameters(validate_config(_base()), runtime_knob_global_refs())
    assert {"name": "retry_transport_timeout", "default": "${var.retry_transport_timeout}"} in params


@pytest.mark.parametrize("value,expected", [(True, "true"), (False, "false")])
def test_job_parameters_retry_transport_timeout_config_value_overrides_ref(value, expected):
    # A config that SETS retry_transport_timeout bakes its literal value, overriding the global ref.
    cfg = _base()
    cfg["retry_transport_timeout"] = value
    params = job_parameters(validate_config(cfg), runtime_knob_global_refs())
    assert {"name": "retry_transport_timeout", "default": expected} in params


def test_validate_config_retry_transport_timeout_canonicalized_and_rejected():
    # Stored canonical ("true"/"false"/"") like the other bool flags; a bad value fails closed.
    assert validate_config({**_base(), "retry_transport_timeout": True})["retry_transport_timeout"] == "true"
    assert validate_config({**_base(), "retry_transport_timeout": False})["retry_transport_timeout"] == "false"
    assert validate_config(_base())["retry_transport_timeout"] == ""    # omitted => unset
    with pytest.raises(PipelineConfigError, match="retry_transport_timeout"):
        validate_config({**_base(), "retry_transport_timeout": "maybe"})


def test_write_config_overrides_includes_retry_transport_timeout():
    # The runner's override parser converts the effective flag to a typed bool kwarg, and omits it when
    # unset (so the connector default stands).
    assert write_config_overrides("", "", "", retry_transport_timeout="true")["retry_transport_timeout"] is True
    assert write_config_overrides("", "", "", retry_transport_timeout="false")["retry_transport_timeout"] is False
    assert "retry_transport_timeout" not in write_config_overrides("", "", "", retry_transport_timeout="")


def test_validate_config_bypass_fast_path_canonicalized_and_rejected():
    # Stored canonical ("true"/"false"/"") like the other bool flags; a bad value fails closed.
    assert validate_config({**_base(), "bypass_fast_path": True})["bypass_fast_path"] == "true"
    assert validate_config({**_base(), "bypass_fast_path": False})["bypass_fast_path"] == "false"
    assert validate_config(_base())["bypass_fast_path"] == ""    # omitted => unset
    with pytest.raises(PipelineConfigError, match="bypass_fast_path"):
        validate_config({**_base(), "bypass_fast_path": "maybe"})


def test_bypass_fast_path_carried_through_resolve():
    # bypass_fast_path is a connector setting, not an object name: resolve_config passes it through verbatim.
    cfg = _base()
    cfg["bypass_fast_path"] = True
    assert resolve_config(validate_config(cfg), "")["bypass_fast_path"] == "true"


def test_op_type_carried_through_resolve():
    # op_type is a connector setting, not an object name: resolve_config passes it through verbatim.
    # (Cleanup from the op_type PR, which added the field everywhere else but missed this passthrough.)
    cfg = _base()
    cfg["op_type"] = "create"
    assert resolve_config(validate_config(cfg), "")["op_type"] == "create"


def test_job_parameters_bypass_fast_path_defaults_empty_without_ref():
    # With no ref supplied (the pure/unit-test call), an omitted bypass_fast_path stays "" - the
    # connector's own default (off, fast path used) stands.
    params = job_parameters(validate_config(_base()))
    assert {"name": "bypass_fast_path", "default": ""} in params


def test_job_parameters_bypass_fast_path_omitted_uses_global_ref():
    # When the config OMITS bypass_fast_path, the generator's ref becomes the job-parameter default, so an
    # omitted pipeline defers to ${var.bypass_fast_path}.
    params = job_parameters(validate_config(_base()), runtime_knob_global_refs())
    assert {"name": "bypass_fast_path", "default": "${var.bypass_fast_path}"} in params


@pytest.mark.parametrize("value,expected", [(True, "true"), (False, "false")])
def test_job_parameters_bypass_fast_path_config_value_overrides_ref(value, expected):
    # A config that SETS bypass_fast_path bakes its literal value, overriding the global ref.
    cfg = _base()
    cfg["bypass_fast_path"] = value
    params = job_parameters(validate_config(cfg), runtime_knob_global_refs())
    assert {"name": "bypass_fast_path", "default": expected} in params


def test_write_config_overrides_includes_bypass_fast_path():
    # The runner's override parser converts the effective flag to a typed bool kwarg, and omits it when
    # unset (so the connector default stands).
    assert write_config_overrides("", "", "", bypass_fast_path="true")["bypass_fast_path"] is True
    assert write_config_overrides("", "", "", bypass_fast_path="false")["bypass_fast_path"] is False
    assert "bypass_fast_path" not in write_config_overrides("", "", "", bypass_fast_path="")


# --- op_type (connector 0.10.0+): bulk action, "index" (default) | "create" ------------------------
# op_type now follows the same three-layer pattern as the other run-time knobs: a target-wide
# ${var.op_type} global (databricks.yml default "index") < a per-pipeline config value < a --params
# override. The allowed set (index|create) is NOT re-enumerated here - the connector's EsWriteConfig is
# the single source of truth and validates the value; this layer only passes a bare token through / fails
# closed on a non-string or a malformed one. create needs a deterministic es_id_field to dedup a resend
# (a config that statically sets create without one fails closed at validate_config; the dynamic
# global/--params path is warned at run time in the notebook).


def test_job_parameters_op_type_defaults_empty_without_ref():
    # With NO global_refs supplied (the pure/unit-test call), an omitted op_type stays "" - the
    # connector's own default op_type ("index") stands.
    params = job_parameters(validate_config(_base()))
    assert {"name": "op_type", "default": ""} in params


def test_job_parameters_op_type_omitted_uses_global_ref():
    # When the config OMITS op_type, the generator's ref becomes the job-parameter default, so an omitted
    # pipeline defers to the target-wide ${var.op_type} global (databricks.yml default "index").
    params = job_parameters(validate_config(_base()), runtime_knob_global_refs())
    assert {"name": "op_type", "default": "${var.op_type}"} in params


@pytest.mark.parametrize("value", ["index", "create"])
def test_job_parameters_op_type_config_value_overrides_ref(value):
    # A config that SETS op_type bakes its literal value, overriding the global ref (per-pipeline wins).
    cfg = _base()
    cfg["op_type"] = value
    params = job_parameters(validate_config(cfg), runtime_knob_global_refs())
    assert {"name": "op_type", "default": value} in params


def test_validate_config_op_type_passthrough_and_type_checked():
    # A present op_type is passed THROUGH unchanged (the connector validates index|create); omitted => ""
    # (unset). A non-string or a non-bare-token value fails closed here, but the value SET itself
    # (e.g. an unknown op) is deliberately NOT enumerated at this layer.
    assert validate_config({**_base(), "op_type": "create"})["op_type"] == "create"
    assert validate_config({**_base(), "op_type": "index"})["op_type"] == "index"
    assert validate_config({**_base(), "op_type": " create "})["op_type"] == "create"  # stripped
    assert validate_config(_base())["op_type"] == ""            # omitted => unset
    with pytest.raises(PipelineConfigError, match="op_type"):
        validate_config({**_base(), "op_type": 7})              # non-string
    with pytest.raises(PipelineConfigError, match="op_type"):
        validate_config({**_base(), "op_type": "has space"})    # not a bare token


def test_validate_config_op_type_create_requires_es_id_field():
    # Cross-field consistency (like continuous-requires-streaming): op_type: create is append-only and
    # needs an explicit _id to dedup a resend, so create WITHOUT es_id_field is rejected fail-closed at
    # config time rather than left to fail at the connector (0.10.0+) or silently duplicate on an older
    # wheel. create WITH es_id_field, and index without it, are both fine.
    no_id = {k: v for k, v in _base().items() if k != "es_id_field"}
    with pytest.raises(PipelineConfigError, match="op_type: create requires es_id_field"):
        validate_config({**no_id, "op_type": "create"})
    assert validate_config({**no_id, "op_type": "index"})["op_type"] == "index"   # index needs no id
    assert validate_config({**_base(), "op_type": "create"})["op_type"] == "create"  # create + id ok


def test_write_config_overrides_includes_op_type():
    # The runner's override parser passes a set op_type through as a string kwarg, and OMITS it when unset
    # (so the connector's own default op_type stands). The value is not enumerated here.
    assert write_config_overrides("", "", "", op_type="create")["op_type"] == "create"
    assert write_config_overrides("", "", "", op_type="index")["op_type"] == "index"
    assert "op_type" not in write_config_overrides("", "", "", op_type="")


def test_job_parameters_reliability_knobs_omitted_use_global_ref():
    # When the config OMITS them, the generator's refs become the job-parameter defaults, so an omitted
    # pipeline defers to the target-wide ${var.request_timeout} / ${var.transport_max_retries} globals.
    params = job_parameters(validate_config(_base()), runtime_knob_global_refs())
    assert {"name": "request_timeout", "default": "${var.request_timeout}"} in params
    assert {"name": "transport_max_retries", "default": "${var.transport_max_retries}"} in params


def test_job_parameters_reliability_knobs_config_value_overrides_ref():
    # A config that SETS the knob bakes its literal value, overriding the global ref (per-pipeline wins).
    # transport_max_retries=0 is meaningful and stored canonical "0" (truthy), so it must beat the ref too.
    cfg = _base()
    cfg["request_timeout"] = 120
    cfg["transport_max_retries"] = 0
    params = job_parameters(validate_config(cfg), runtime_knob_global_refs())
    assert {"name": "request_timeout", "default": "120"} in params
    assert {"name": "transport_max_retries", "default": "0"} in params


def test_job_parameters_streaming_start_defaults_empty_without_ref():
    # streaming_start is now a config key on the same three-layer pattern. With NO global_refs and the
    # config omitting it, the default is "" (the runner turns "" into "new" as the final fallback).
    params = job_parameters(validate_config(_base()))
    assert {"name": "streaming_start", "default": ""} in params


def test_job_parameters_streaming_start_omitted_uses_global_ref():
    # Omitted config => the ${var.streaming_start} global ref (databricks.yml default "new").
    params = job_parameters(validate_config(_base()), runtime_knob_global_refs())
    assert {"name": "streaming_start", "default": "${var.streaming_start}"} in params


@pytest.mark.parametrize("value", ["new", "full"])
def test_job_parameters_streaming_start_config_value_overrides_ref(value):
    # A config that SETS streaming_start bakes its literal value, overriding the global ref.
    cfg = {**_base(), "streaming_start": value}
    params = job_parameters(validate_config(cfg), runtime_knob_global_refs())
    assert {"name": "streaming_start", "default": value} in params


def test_validate_config_streaming_start_optional_and_validated():
    # OPTIONAL: omitted => "" (inherit the global). A PRESENT value is allow-list validated (new|full);
    # a bad value fails closed. Carried through resolve_config verbatim (a run behavior, not a name).
    assert validate_config(_base())["streaming_start"] == ""                       # omitted
    assert validate_config({**_base(), "streaming_start": "full"})["streaming_start"] == "full"
    with pytest.raises(PipelineConfigError, match="streaming_start"):
        validate_config({**_base(), "streaming_start": "sideways"})
    assert resolve_config(validate_config({**_base(), "streaming_start": "full"}), "")["streaming_start"] == "full"


def test_job_parameters_pipeline_mode_omitted_uses_global_ref():
    # When the config OMITS pipeline_mode, the generator's ref becomes the job-parameter default, so an
    # omitted pipeline defers to the target-wide ${var.pipeline_mode} global (databricks.yml default batch).
    cfg = {k: v for k, v in _base().items() if k != "pipeline_mode"}
    params = job_parameters(validate_config(cfg), runtime_knob_global_refs())
    assert {"name": "pipeline_mode", "default": "${var.pipeline_mode}"} in params


def test_runtime_knob_registry_shape():
    # The registry is the single source of truth for the run-time job parameters: exactly these knobs, in
    # this order. job_parameters, the generation gate, and the shape tests all derive from it, so this
    # asserts the canonical set/order in one place.
    assert RUNTIME_KNOB_NAMES == (
        "pipeline_mode", "filter_condition", "chunk_size", "write_concurrency", "op_type",
        "request_timeout", "transport_max_retries", "require_existing_index", "verify_certs",
        "bulk_stats", "retry_transport_timeout", "bypass_fast_path", "streaming_start",
        "write_repartition", "max_partition_bytes", "max_files_per_trigger", "max_bytes_per_trigger",
    )
    # Every knob carries a callable validator (the same require_* helper used at parse + run time).
    for knob in _RUNTIME_KNOBS:
        assert callable(knob.validator)
    # runtime_knob_global_refs() maps each knob to its ${var.<name>} reference, one per knob.
    refs = runtime_knob_global_refs()
    assert refs == {name: "${var." + name + "}" for name in RUNTIME_KNOB_NAMES}


def test_job_parameters_filter_condition_defaults_empty_when_absent():
    # A config that omits filter_condition yields a "" default for that job parameter.
    params = job_parameters(validate_config(_base()))
    assert {"name": "filter_condition", "default": ""} in params


# --------------------------------------------------------------------------- filter_condition


def test_filter_condition_absent_defaults_empty():
    assert validate_config(_base())["filter_condition"] == ""


def test_filter_condition_present_kept_verbatim():
    cfg = _base()
    cfg["filter_condition"] = "action = 'allowed' AND rcode <> 0"
    assert validate_config(cfg)["filter_condition"] == "action = 'allowed' AND rcode <> 0"


@pytest.mark.parametrize("bad", [5, True, ["a"], {"x": 1}])
def test_filter_condition_non_string_rejected(bad):
    # df.filter expects a string expression; a YAML number/bool/list must fail closed at validation.
    cfg = _base()
    cfg["filter_condition"] = bad
    with pytest.raises(PipelineConfigError, match="filter_condition"):
        validate_config(cfg)


def test_filter_condition_carried_through_resolve():
    # A SQL predicate, not an object name: resolve passes it through unchanged (no ${environment}).
    cfg = _with_env()
    cfg["filter_condition"] = "catalog_name = 'x'"
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["filter_condition"] == "catalog_name = 'x'"


@pytest.mark.parametrize("mode", ["", "col = 'v'"])
def test_require_filter_condition_accepts_strings(mode):
    assert require_filter_condition(mode, "filter_condition job parameter") == mode


@pytest.mark.parametrize("bad", [5, True, None, ["a"]])
def test_require_filter_condition_rejects_non_string(bad):
    with pytest.raises(PipelineConfigError, match="filter_condition"):
        require_filter_condition(bad, "filter_condition job parameter")


# --------------------------------------------------------------------------- write_config_overrides


def test_write_config_overrides_all_empty_is_empty():
    # Every knob unset => omit all, so the connector's own defaults stand untouched.
    assert write_config_overrides("", "", "") == {}
    assert write_config_overrides(None, None, None) == {}


def test_write_config_overrides_chunk_size_parsed():
    assert write_config_overrides("1000", "", "") == {"chunk_size": 1000}
    assert write_config_overrides(" 250 ", "", "") == {"chunk_size": 250}


@pytest.mark.parametrize("bad", ["abc", "12.5", "0", "-5", "1e3"])
def test_write_config_overrides_bad_chunk_size_fails_closed(bad):
    with pytest.raises(PipelineConfigError, match="chunk_size"):
        write_config_overrides(bad, "", "")


def test_write_config_overrides_write_concurrency_parsed():
    assert write_config_overrides("", "", "", "4") == {"write_concurrency": 4}
    assert write_config_overrides("", "", "", " 8 ") == {"write_concurrency": 8}


@pytest.mark.parametrize("bad", ["abc", "2.5", "0", "-1", "1e2"])
def test_write_config_overrides_bad_write_concurrency_fails_closed(bad):
    with pytest.raises(PipelineConfigError, match="write_concurrency"):
        write_config_overrides("", "", "", bad)


@pytest.mark.parametrize("value,expected", [("true", True), ("false", False), ("True", True), ("FALSE", False), (" true ", True)])
def test_write_config_overrides_booleans_parsed(value, expected):
    assert write_config_overrides("", value, "") == {"require_existing_index": expected}
    assert write_config_overrides("", "", value) == {"verify_certs": expected}


@pytest.mark.parametrize("bad", ["maybe", "1", "0", "yes", "no", "T"])
def test_write_config_overrides_bad_boolean_fails_closed(bad):
    # Allow-list 'true'/'false' only: never fall back to Python truthiness (bool('false') is True).
    with pytest.raises(PipelineConfigError, match="require_existing_index"):
        write_config_overrides("", bad, "")
    with pytest.raises(PipelineConfigError, match="verify_certs"):
        write_config_overrides("", "", bad)


@pytest.mark.parametrize("value,expected", [("true", True), ("false", False), ("True", True), ("FALSE", False), (" true ", True)])
def test_write_config_overrides_bulk_stats_parsed(value, expected):
    # bulk_stats is the 5th positional arg; parsed to a bool like the other flags.
    assert write_config_overrides("", "", "", "", value) == {"bulk_stats": expected}


def test_write_config_overrides_bulk_stats_empty_omitted():
    # An empty bulk_stats (widget cleared) is omitted, so the connector's own default stands.
    assert write_config_overrides("", "", "", "", "") == {}


@pytest.mark.parametrize("bad", ["maybe", "1", "0", "yes", "T"])
def test_write_config_overrides_bad_bulk_stats_fails_closed(bad):
    with pytest.raises(PipelineConfigError, match="bulk_stats"):
        write_config_overrides("", "", "", "", bad)


def test_write_config_overrides_combined():
    assert write_config_overrides("500", "false", "false", "4", "true") == {
        "chunk_size": 500,
        "write_concurrency": 4,
        "require_existing_index": False,
        "verify_certs": False,
        "bulk_stats": True,
    }


def test_write_config_overrides_request_timeout_parsed():
    # request_timeout is a keyword-only override (a positive int, seconds).
    assert write_config_overrides("", "", "", request_timeout="120") == {"request_timeout": 120}
    assert write_config_overrides("", "", "", request_timeout=" 90 ") == {"request_timeout": 90}


@pytest.mark.parametrize("bad", ["abc", "2.5", "0", "-5", "1e3"])
def test_write_config_overrides_bad_request_timeout_fails_closed(bad):
    with pytest.raises(PipelineConfigError, match="request_timeout"):
        write_config_overrides("", "", "", request_timeout=bad)


def test_write_config_overrides_transport_max_retries_parsed():
    # transport_max_retries is a keyword-only override (a non-negative int).
    assert write_config_overrides("", "", "", transport_max_retries="5") == {"transport_max_retries": 5}
    assert write_config_overrides("", "", "", transport_max_retries=" 8 ") == {"transport_max_retries": 8}


def test_write_config_overrides_transport_max_retries_zero_included():
    # 0 is a MEANINGFUL value (disable transport retries), so it must be passed through, not dropped as
    # if unset - the distinction between "0 retries" and "leave the connector default (3)".
    assert write_config_overrides("", "", "", transport_max_retries="0") == {"transport_max_retries": 0}
    assert write_config_overrides("", "", "", transport_max_retries=0) == {"transport_max_retries": 0}


def test_write_config_overrides_transport_max_retries_empty_omitted():
    # Unset (empty) omits the knob, so the connector's own default (3) stands.
    assert write_config_overrides("", "", "", transport_max_retries="") == {}


@pytest.mark.parametrize("bad", ["abc", "2.5", "-1", "1e2"])
def test_write_config_overrides_bad_transport_max_retries_fails_closed(bad):
    with pytest.raises(PipelineConfigError, match="transport_max_retries"):
        write_config_overrides("", "", "", transport_max_retries=bad)


def test_write_config_overrides_combined_with_reliability_knobs():
    # All knobs together, including the two new reliability knobs (transport_max_retries=0 kept). The
    # positional args are (chunk_size, require_existing_index, verify_certs, write_concurrency,
    # bulk_stats), so bulk_stats=False is included here too.
    assert write_config_overrides("500", "true", "true", "4", "false",
                                  request_timeout="120", transport_max_retries="0") == {
        "chunk_size": 500,
        "write_concurrency": 4,
        "request_timeout": 120,
        "transport_max_retries": 0,
        "require_existing_index": True,
        "verify_certs": True,
        "bulk_stats": False,
    }


# ------------------------------------------------- require_chunk_size / require_es_flag (shared validators)


@pytest.mark.parametrize("value,expected", [
    ("", ""), (None, ""), ("  ", ""),          # unset -> canonical ""
    (500, "500"), ("500", "500"), (" 250 ", "250"),  # YAML int OR string -> canonical string
])
def test_require_chunk_size_canonical(value, expected):
    assert require_chunk_size(value) == expected


@pytest.mark.parametrize("bad", ["abc", "12.5", "0", "-5", "1e3", 0, -1, 12.5, True, False])
def test_require_chunk_size_fails_closed(bad):
    # A non-positive-int, a float, a non-numeric string, or a bool (int subclass) must fail closed.
    with pytest.raises(PipelineConfigError, match="chunk_size"):
        require_chunk_size(bad)


@pytest.mark.parametrize("value,expected", [
    ("", ""), (None, ""), ("  ", ""),                  # unset -> canonical ""
    (True, "true"), (False, "false"),                  # YAML bool -> canonical string
    ("true", "true"), ("false", "false"),              # string passthrough
    ("True", "true"), ("FALSE", "false"), (" true ", "true"),  # case-insensitive + trimmed
])
def test_require_es_flag_canonical(value, expected):
    assert require_es_flag(value, "verify_certs") == expected


@pytest.mark.parametrize("bad", ["maybe", "1", "0", "yes", "no", "T", 5, 1.0, ["a"]])
def test_require_es_flag_fails_closed(bad):
    # Allow-list 'true'/'false'/bool only; never fall back to Python truthiness.
    with pytest.raises(PipelineConfigError, match="verify_certs"):
        require_es_flag(bad, "verify_certs")


@pytest.mark.parametrize("value,expected", [
    ("", ""), (None, ""), ("  ", ""),                # unset -> canonical ""
    (60, "60"), ("120", "120"), (" 90 ", "90"),      # YAML int OR string -> canonical string
])
def test_require_request_timeout_canonical(value, expected):
    # request_timeout is a positive int (seconds); shares the require_chunk_size positive-int rule.
    assert require_request_timeout(value) == expected


@pytest.mark.parametrize("bad", ["abc", "12.5", "0", "-5", "1e3", 0, -1, 12.5, True, False])
def test_require_request_timeout_fails_closed(bad):
    # A non-positive-int, a float, a non-numeric string, or a bool (int subclass) must fail closed.
    with pytest.raises(PipelineConfigError, match="request_timeout"):
        require_request_timeout(bad)


@pytest.mark.parametrize("value,expected", [
    ("", ""), (None, ""), ("  ", ""),                # unset -> canonical "" (NOT a default)
    (0, "0"), ("0", "0"),                            # 0 is valid (disable transport retries)
    (3, "3"), ("5", "5"), (" 8 ", "8"),              # YAML int OR string -> canonical string
])
def test_require_transport_max_retries_canonical(value, expected):
    # transport_max_retries is a NON-negative int: 0 is accepted, and unset stays "" (defer to connector
    # default) rather than being coerced to a value like require_write_repartition does.
    assert require_transport_max_retries(value) == expected


@pytest.mark.parametrize("bad", ["abc", "2.5", "-1", "1e2", -5, 2.5, True, False])
def test_require_transport_max_retries_fails_closed(bad):
    # A negative int, a float, a non-numeric string, or a bool (int subclass) must fail closed.
    with pytest.raises(PipelineConfigError, match="transport_max_retries"):
        require_transport_max_retries(bad)


# ------------------------------------------------- tuning knobs as config keys


def test_tuning_knobs_from_yaml_native_types_canonicalized():
    # A config may set the knobs with YAML-native types (int, bool); they are stored canonical strings.
    cfg = _base()
    cfg["chunk_size"] = 1000
    cfg["require_existing_index"] = False
    cfg["verify_certs"] = True
    out = validate_config(cfg)
    assert out["chunk_size"] == "1000"
    assert out["require_existing_index"] == "false"
    assert out["verify_certs"] == "true"


def test_tuning_knobs_from_yaml_string_values():
    # Strings are equally accepted (and canonicalized) in the config.
    cfg = _base()
    cfg["chunk_size"] = "250"
    cfg["require_existing_index"] = "TRUE"
    out = validate_config(cfg)
    assert out["chunk_size"] == "250"
    assert out["require_existing_index"] == "true"


@pytest.mark.parametrize("key,bad", [
    ("chunk_size", "abc"), ("chunk_size", 0), ("chunk_size", -5), ("chunk_size", 12.5),
    ("request_timeout", "abc"), ("request_timeout", 0), ("request_timeout", -5), ("request_timeout", 12.5),
    ("transport_max_retries", "abc"), ("transport_max_retries", -1), ("transport_max_retries", 2.5),
    ("require_existing_index", "maybe"), ("require_existing_index", 1),
    ("verify_certs", "yes"),
])
def test_tuning_knobs_bad_config_value_fails_closed(key, bad):
    cfg = _base()
    cfg[key] = bad
    with pytest.raises(PipelineConfigError, match=key):
        validate_config(cfg)


def test_reliability_knobs_from_config_canonicalized():
    # request_timeout / transport_max_retries accept YAML int or string; stored as canonical strings.
    # transport_max_retries=0 is a valid config value (disable transport retries), kept as "0".
    cfg = _base()
    cfg["request_timeout"] = 120
    cfg["transport_max_retries"] = 0
    out = validate_config(cfg)
    assert out["request_timeout"] == "120"
    assert out["transport_max_retries"] == "0"


def test_tuning_knobs_carried_through_resolve():
    # Connector settings, not object names: resolve passes the canonical strings through unchanged.
    cfg = _with_env()
    cfg["chunk_size"] = 800
    cfg["verify_certs"] = False
    cfg["request_timeout"] = 90
    cfg["transport_max_retries"] = 5
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["chunk_size"] == "800"
    assert out["verify_certs"] == "false"
    assert out["require_existing_index"] == ""  # omitted -> unset
    assert out["request_timeout"] == "90"
    assert out["transport_max_retries"] == "5"


def test_reliability_knobs_omitted_carry_through_resolve_as_unset():
    # Omitted reliability knobs stay "" through resolve (defer to the connector's own defaults).
    out = resolve_config(validate_config(_with_env()), environment="prod")
    assert out["request_timeout"] == ""
    assert out["transport_max_retries"] == ""


def test_job_parameters_tuning_defaults_from_config():
    # The tuning job parameters' defaults come from the config (the whole point of this change), in
    # canonical string form; an omitted knob defaults to "".
    cfg = _base()
    cfg["chunk_size"] = 1000
    cfg["verify_certs"] = False
    cfg["request_timeout"] = 120
    cfg["transport_max_retries"] = 0
    params = job_parameters(validate_config(cfg))
    assert {"name": "chunk_size", "default": "1000"} in params
    assert {"name": "verify_certs", "default": "false"} in params
    assert {"name": "require_existing_index", "default": ""} in params
    assert {"name": "request_timeout", "default": "120"} in params
    assert {"name": "transport_max_retries", "default": "0"} in params


# --------------------------------------------------------------------------- write_repartition


@pytest.mark.parametrize("value,expected", [
    (500, "500"), ("500", "500"), (" 250 ", "250"),  # YAML int OR string -> canonical string
    (0, "0"), ("0", "0"),                             # 0 is allowed: "do not repartition"
])
def test_require_write_repartition_canonical(value, expected):
    assert require_write_repartition(value) == expected


@pytest.mark.parametrize("value", ["", None, "  "])
def test_require_write_repartition_empty_takes_builtin_default(value):
    # Unlike the tuning knobs (empty -> ""), an unset write_repartition falls back to the built-in
    # default, which is 0 = off (read parallelism via max_partition_bytes is the primary lever).
    assert require_write_repartition(value) == "0"


@pytest.mark.parametrize("bad", ["abc", "12.5", "-5", "1e3", -1, -100, 12.5, True, False])
def test_require_write_repartition_fails_closed(bad):
    # A negative int, a float, a non-numeric string, or a bool (int subclass) must fail closed. 0 is
    # NOT here: it is a valid value meaning "disable repartitioning".
    with pytest.raises(PipelineConfigError, match="write_repartition"):
        require_write_repartition(bad)


def test_write_repartition_absent_stores_empty_in_config():
    # A config that OMITS write_repartition now stores "" (so the job-parameter default can inherit the
    # ${var.write_repartition} global). The built-in default (0 = off) is applied by the RUNNER as the
    # final fallback: require_write_repartition("") == "0" (asserted in
    # test_require_write_repartition_empty_takes_builtin_default), so an omitted config with an empty
    # global still effectively repartitions by 0.
    assert validate_config(_base())["write_repartition"] == ""
    assert require_write_repartition("") == "0"   # runner still resolves the built-in default


@pytest.mark.parametrize("value,expected", [(256, "256"), ("256", "256"), (0, "0")])
def test_write_repartition_from_config(value, expected):
    # A config value (int or string, including 0 to disable) is accepted and canonicalized.
    cfg = _base()
    cfg["write_repartition"] = value
    assert validate_config(cfg)["write_repartition"] == expected


@pytest.mark.parametrize("bad", ["abc", -1, 12.5, True])
def test_write_repartition_bad_config_value_fails_closed(bad):
    cfg = _base()
    cfg["write_repartition"] = bad
    with pytest.raises(PipelineConfigError, match="write_repartition"):
        validate_config(cfg)


def test_write_repartition_carried_through_resolve():
    # A run behavior, not an object name: resolve passes the canonical string through unchanged.
    cfg = _with_env()
    cfg["write_repartition"] = 200
    assert resolve_config(validate_config(cfg), environment="prod")["write_repartition"] == "200"


def test_job_parameters_write_repartition_default_from_config():
    cfg = _base()
    cfg["write_repartition"] = 256
    assert {"name": "write_repartition", "default": "256"} in job_parameters(validate_config(cfg))


# --------------------------------------------------------------------------- max_partition_bytes


@pytest.mark.parametrize("value,expected", [
    ("32m", "32m"), ("16M", "16m"), ("128m", "128m"), ("512mb", "512mb"), ("1g", "1g"),  # byte-size strings
    (" 8m ", "8m"),                                   # trimmed + lowercased
    (33554432, "33554432"), ("33554432", "33554432"), # raw byte count (int or string)
    (0, "0"), ("0", "0"), ("0m", "0"),                # 0 sentinel: "do not set", normalized to "0"
])
def test_require_max_partition_bytes_canonical(value, expected):
    assert require_max_partition_bytes(value) == expected


@pytest.mark.parametrize("value", ["", None, "  "])
def test_require_max_partition_bytes_empty_takes_builtin_default(value):
    # Unset falls back to the built-in scan-parallelism default (not "unset"/engine default).
    assert require_max_partition_bytes(value) == "2m"


@pytest.mark.parametrize("bad", ["abc", "32.5m", "32x", "m", "-5", -1, 12.5, True, False, "32 m"])
def test_require_max_partition_bytes_fails_closed(bad):
    # A malformed size, a bad/space-separated unit, a float, a bool (int subclass), or a negative value
    # must fail closed rather than reach spark.conf.set as a value it would choke on at read time.
    with pytest.raises(PipelineConfigError, match="max_partition_bytes"):
        require_max_partition_bytes(bad)


def test_max_partition_bytes_absent_stores_empty_in_config():
    # A config that OMITS max_partition_bytes now stores "" (so the job-parameter default can inherit the
    # ${var.max_partition_bytes} global). The built-in default (2m) is applied by the RUNNER as the final
    # fallback: require_max_partition_bytes("") == "2m", so an omitted config with an empty global still
    # gets the built-in scan-parallelism size.
    assert validate_config(_base())["max_partition_bytes"] == ""
    assert require_max_partition_bytes("") == "2m"   # runner still resolves the built-in default


@pytest.mark.parametrize("value,expected", [("16m", "16m"), (67108864, "67108864"), ("0", "0")])
def test_max_partition_bytes_from_config(value, expected):
    cfg = _base()
    cfg["max_partition_bytes"] = value
    assert validate_config(cfg)["max_partition_bytes"] == expected


@pytest.mark.parametrize("bad", ["nope", "32.5m", -1, 12.5, True])
def test_max_partition_bytes_bad_config_value_fails_closed(bad):
    cfg = _base()
    cfg["max_partition_bytes"] = bad
    with pytest.raises(PipelineConfigError, match="max_partition_bytes"):
        validate_config(cfg)


def test_max_partition_bytes_carried_through_resolve():
    cfg = _with_env()
    cfg["max_partition_bytes"] = "16m"
    assert resolve_config(validate_config(cfg), environment="prod")["max_partition_bytes"] == "16m"


def test_job_parameters_max_partition_bytes_default_from_config():
    cfg = _base()
    cfg["max_partition_bytes"] = "16m"
    assert {"name": "max_partition_bytes", "default": "16m"} in job_parameters(validate_config(cfg))


# --------------------------------------------------------------------------- require_pipeline_mode


@pytest.mark.parametrize("mode", ["batch", "streaming"])
def test_require_pipeline_mode_accepts_allowed(mode):
    # batch/streaming are the only valid modes, in BOTH contexts (the config default and the run-time
    # override). The validator returns them unchanged.
    assert require_pipeline_mode(mode, "pipeline_mode job parameter") == mode


@pytest.mark.parametrize("bad", ["turbo", "Batch", "", None, "streaming ", 5])
def test_require_pipeline_mode_rejects_bad_override(bad):
    # A bad --params pipeline_mode=... override must fail closed, not silently run an unknown mode.
    with pytest.raises(PipelineConfigError, match="pipeline_mode"):
        require_pipeline_mode(bad, "pipeline_mode job parameter")


def test_validate_does_not_mutate_input():
    cfg = _with_env()
    before = copy.deepcopy(cfg)
    validate_config(cfg)
    assert cfg == before


# --------------------------------------------------------------------------- column_present


def test_column_present_exact_match():
    assert column_present("dsl_id", ["dsl_id", "time", "action"])
    assert not column_present("missing", ["dsl_id", "time", "action"])


@pytest.mark.parametrize(
    "field,columns",
    [
        ("dsl_id", ["DSL_ID", "time"]),        # view column upper-cased
        ("DSL_ID", ["dsl_id", "time"]),        # config value upper-cased
        ("Dsl_Id", ["dSL_id", "time"]),        # mixed casing on both sides
    ],
)
def test_column_present_case_insensitive(field, columns):
    # Spark resolves column names case-insensitively by default, so the es_id_field check must too:
    # a case-only difference is a real, resolvable column, not a missing one.
    assert column_present(field, columns)


def test_column_present_empty_columns():
    assert not column_present("dsl_id", [])


# --------------------------------------------------------------------------- compute


def test_compute_absent_defaults_serverless():
    # No compute block => serverless (the framework default: no cluster block, serverless notebook task).
    assert validate_config(_base())["compute"] == {"type": "serverless"}


def test_compute_explicit_serverless():
    cfg = _base()
    cfg["compute"] = {"type": "serverless"}
    assert validate_config(cfg)["compute"] == {"type": "serverless"}


def test_compute_serverless_rejects_extra_keys():
    # serverless takes no other keys; a stray key (e.g. a cluster id) must fail closed, not be dropped.
    cfg = _base()
    cfg["compute"] = {"type": "serverless", "existing_cluster_id": "x"}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


def test_compute_existing_cluster_config_valid():
    # existing_cluster names a bundle variable (cluster_config) whose per-target value is the cluster id;
    # a cluster id is workspace-specific, so it is never a literal here. The generator turns the name
    # into a ${var.<name>} reference resolved per target.
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", "cluster_config": "interactive_primary"}
    assert validate_config(cfg)["compute"] == {
        "type": "existing_cluster",
        "cluster_config": "interactive_primary",
    }


@pytest.mark.parametrize("missing", [{}, {"cluster_config": None}])
def test_compute_existing_cluster_requires_cluster_config(missing):
    # existing_cluster with no cluster_config fails closed (there is no cluster named to attach to).
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", **missing}
    with pytest.raises(PipelineConfigError, match="cluster_config"):
        validate_config(cfg)


def test_compute_existing_cluster_rejects_literal_id():
    # The literal existing_cluster_id was removed (a cluster id is workspace-specific, so it is always
    # per-target via cluster_config); passing it now fails closed as an unknown key.
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", "existing_cluster_id": "0123-456789-abcde"}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


def test_compute_existing_cluster_rejects_unknown_key():
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", "cluster_config": "x", "job_cluster_config": "y"}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


@pytest.mark.parametrize("bad", ["bad-name", "with.dot", "with space", "1leading", "", 5, True, ["x"]])
def test_compute_existing_cluster_config_must_be_identifier(bad):
    # cluster_config names a bundle variable, so it is held to the identifier rule (letter/underscore,
    # then letters/digits/underscore): a hyphen/dot/space/non-string would make a broken ${var.<name>}.
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", "cluster_config": bad}
    with pytest.raises(PipelineConfigError, match="identifier"):
        validate_config(cfg)


def test_compute_job_cluster_valid():
    cfg = _base()
    cfg["compute"] = {"type": "job_cluster", "job_cluster_config": "standard_batch"}
    assert validate_config(cfg)["compute"] == {
        "type": "job_cluster",
        "job_cluster_config": "standard_batch",
    }


@pytest.mark.parametrize("bad", [None, "", "has space", "with.dot", "with/slash", 5, True])
def test_compute_job_cluster_requires_valid_key(bad):
    # job_cluster_config must be present and a safe filename stem (letters/digits/_/-): it maps to a
    # file, so dots/slashes (path traversal) and non-strings fail closed.
    cfg = _base()
    compute = {"type": "job_cluster"}
    if bad is not None:
        compute["job_cluster_config"] = bad
    cfg["compute"] = compute
    with pytest.raises(PipelineConfigError, match="job_cluster_config"):
        validate_config(cfg)


def test_compute_job_cluster_rejects_unknown_key():
    cfg = _base()
    cfg["compute"] = {"type": "job_cluster", "job_cluster_config": "x", "existing_cluster_id": "y"}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


@pytest.mark.parametrize("bad", ["Serverless", "cluster", "", None, 5, "new_cluster"])
def test_compute_unknown_type_rejected(bad):
    # Allow-list on type: a near-miss, wrong case, empty, or non-string must fail closed.
    cfg = _base()
    cfg["compute"] = {"type": bad}
    with pytest.raises(PipelineConfigError, match="compute.type"):
        validate_config(cfg)


@pytest.mark.parametrize("bad", ["serverless", 5, ["type"]])
def test_compute_non_mapping_rejected(bad):
    # compute must be a mapping (not a bare string/list/number).
    cfg = _base()
    cfg["compute"] = bad
    with pytest.raises(PipelineConfigError, match="compute"):
        validate_config(cfg)


def test_compute_carried_through_resolve():
    # compute is a deploy-time job property, not an object name: resolve passes it through unchanged.
    cfg = _with_env()
    cfg["compute"] = {"type": "job_cluster", "job_cluster_config": "standard_batch"}
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["compute"] == {"type": "job_cluster", "job_cluster_config": "standard_batch"}


# --------------------------------------------------------------------------- schedule


def test_schedule_absent_defaults_none():
    # No schedule block => None (on-demand, the default: no schedule emitted on the job).
    assert validate_config(_base())["schedule"] is None


@pytest.mark.parametrize("cron", [
    "0 0 8 * * ?",        # 6 fields: 08:00 daily
    "0 0 8 * * ? 2027",   # 7 fields: with year
    "0 */15 * * * ?",     # every 15 minutes
])
def test_schedule_valid_cron_accepted(cron):
    cfg = _base()
    cfg["schedule"] = {"quartz_cron_expression": cron}
    assert validate_config(cfg)["schedule"] == {"quartz_cron_expression": cron}


def test_schedule_cron_trimmed():
    cfg = _base()
    cfg["schedule"] = {"quartz_cron_expression": "  0 0 8 * * ?  "}
    assert validate_config(cfg)["schedule"]["quartz_cron_expression"] == "0 0 8 * * ?"


@pytest.mark.parametrize("bad", ["0 0 8 * * ?", 5, ["cron"]])
def test_schedule_non_mapping_rejected(bad):
    # schedule must be a mapping with quartz_cron_expression, not a bare string/list/number.
    cfg = _base()
    cfg["schedule"] = bad
    with pytest.raises(PipelineConfigError, match="schedule"):
        validate_config(cfg)


def test_schedule_unknown_key_rejected():
    cfg = _base()
    cfg["schedule"] = {"quartz_cron_expression": "0 0 8 * * ?", "timezone_id": "UTC"}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


@pytest.mark.parametrize("bad", [None, "", "   ", 5, True, ["x"]])
def test_schedule_missing_or_bad_cron_rejected(bad):
    cfg = _base()
    schedule = {}
    if bad is not None:
        schedule["quartz_cron_expression"] = bad
    cfg["schedule"] = schedule
    with pytest.raises(PipelineConfigError, match="quartz_cron_expression"):
        validate_config(cfg)


@pytest.mark.parametrize("cron", ["0 8 * * *", "* * * * *", "0 0 8 * * ? 2027 extra"])
def test_schedule_wrong_field_count_rejected(cron):
    # 5-field Unix cron (or an 8-field typo) must fail closed: Quartz needs 6 or 7 fields.
    cfg = _base()
    cfg["schedule"] = {"quartz_cron_expression": cron}
    with pytest.raises(PipelineConfigError, match="6 or 7 fields"):
        validate_config(cfg)


def test_schedule_carried_through_resolve():
    # schedule is a deploy-time job property, not an object name: resolve passes it through unchanged.
    cfg = _with_env()
    cfg["schedule"] = {"quartz_cron_expression": "0 0 8 * * ?"}
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["schedule"] == {"quartz_cron_expression": "0 0 8 * * ?"}


def test_schedule_none_carried_through_resolve():
    out = resolve_config(validate_config(_base()), environment="")
    assert out["schedule"] is None


# --------------------------------------------------------------------------- pause_status (per-pipeline)


def test_pause_status_absent_defaults_empty():
    # No pause_status key => "" (inherit the target-wide ${var.schedule_pause_status} global).
    assert validate_config(_base())["pause_status"] == ""


@pytest.mark.parametrize("value", ["PAUSED", "UNPAUSED"])
def test_pause_status_valid_accepted_with_schedule(value):
    # A pipeline with a trigger may set its own pause_status (allow-list PAUSED|UNPAUSED).
    cfg = _base()
    cfg["schedule"] = {"quartz_cron_expression": "0 0 8 * * ?"}
    cfg["pause_status"] = value
    assert validate_config(cfg)["pause_status"] == value


@pytest.mark.parametrize("bad", ["paused", "unpaused", "UNPAUSE", "", "PAUSE", True, 1, None])
def test_pause_status_invalid_rejected(bad):
    # Allow-list: anything but exactly PAUSED/UNPAUSED fails closed (a typo would deploy a pause_status
    # the Jobs API rejects, or silently mean nothing). Paired with a schedule so the failure is the
    # allow-list check, not the on-demand cross-field guard.
    cfg = _base()
    cfg["schedule"] = {"quartz_cron_expression": "0 0 8 * * ?"}
    cfg["pause_status"] = bad
    with pytest.raises(PipelineConfigError, match="pause_status"):
        validate_config(cfg)


def test_pause_status_on_demand_standalone_rejected():
    # A standalone on-demand job (no schedule/continuous/job_group) can't use pause_status: it would
    # have no effect, so fail closed rather than silently ignore it.
    cfg = _base()
    cfg["pause_status"] = "UNPAUSED"
    with pytest.raises(PipelineConfigError, match="pause_status.*no effect|on-demand"):
        validate_config(cfg)


def test_pause_status_allowed_on_continuous():
    cfg = _continuous_ready()
    cfg["continuous"] = {"trigger_interval": "30 seconds"}
    cfg["pause_status"] = "UNPAUSED"
    assert validate_config(cfg)["pause_status"] == "UNPAUSED"


def test_pause_status_allowed_on_grouped_member_without_own_trigger():
    # A grouped member may set pause_status while inheriting the group's trigger (job_group present), so
    # it is NOT rejected at the config layer even though this member declares no schedule/continuous.
    # (The generator rejects a job_group that has no trigger at all.)
    cfg = _base()
    cfg["job_group"] = "grp"
    cfg["pause_status"] = "UNPAUSED"
    assert validate_config(cfg)["pause_status"] == "UNPAUSED"


def test_pause_status_carried_through_resolve():
    # pause_status is a deploy-time job property, not an object name: resolve passes it through unchanged.
    cfg = _with_env()
    cfg["schedule"] = {"quartz_cron_expression": "0 0 8 * * ?"}
    cfg["pause_status"] = "UNPAUSED"
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["pause_status"] == "UNPAUSED"


def test_pause_status_absent_carried_through_resolve_as_empty():
    out = resolve_config(validate_config(_base()), environment="")
    assert out["pause_status"] == ""


@pytest.mark.parametrize("value", ["PAUSED", "UNPAUSED"])
def test_require_pause_status_accepts_allowed(value):
    assert require_pause_status(value) == value


@pytest.mark.parametrize("bad", ["paused", "", "UNPAUSE", None, 0, True])
def test_require_pause_status_rejects_others(bad):
    with pytest.raises(PipelineConfigError, match="pause_status"):
        require_pause_status(bad)


# --------------------------------------------------------------------------- continuous (always-on)


def _continuous_ready():
    """A config that satisfies the continuous cross-field rules (streaming + classic compute), so a
    `continuous` block can be added without tripping an unrelated rule."""
    cfg = _base()
    cfg["pipeline_mode"] = "streaming"
    cfg["compute"] = {"type": "job_cluster", "job_cluster_config": "standard_batch"}
    return cfg


def test_continuous_absent_defaults_none():
    # No continuous block => None (not always-on, the default: scheduled/on-demand availableNow).
    assert validate_config(_base())["continuous"] is None


def test_continuous_valid():
    cfg = _continuous_ready()
    cfg["continuous"] = {"trigger_interval": "30 seconds"}
    assert validate_config(cfg)["continuous"] == {"trigger_interval": "30 seconds"}


def test_continuous_trigger_interval_trimmed():
    cfg = _continuous_ready()
    cfg["continuous"] = {"trigger_interval": "  1 minute  "}
    assert validate_config(cfg)["continuous"] == {"trigger_interval": "1 minute"}


@pytest.mark.parametrize("bad", [None, "", "   ", 30, 1.5, True])
def test_continuous_trigger_interval_required(bad):
    # trigger_interval is required within the block and must be a non-empty string.
    cfg = _continuous_ready()
    cfg["continuous"] = {} if bad is None else {"trigger_interval": bad}
    with pytest.raises(PipelineConfigError, match="trigger_interval"):
        validate_config(cfg)


@pytest.mark.parametrize("good", [
    "30 seconds", "1 minute", "500 milliseconds", "2 hours", "0 seconds", "1.5 seconds",
    "1 minute 30 seconds", "1 day", "1 week",
])
def test_continuous_trigger_interval_allowed_forms(good):
    # The allow-list accepts one-or-more "<number> <full-word-unit>" terms (Spark's stringToInterval
    # grammar): compound ("1 minute 30 seconds"), decimal, and 0 (as-fast-as-possible) are all valid.
    cfg = _continuous_ready()
    cfg["continuous"] = {"trigger_interval": good}
    assert validate_config(cfg)["continuous"] == {"trigger_interval": good}


@pytest.mark.parametrize("bad", [
    "30", "0", "seconds", "minute",                     # missing a number or a unit
    "5s", "2h", "250us", "10 mins", "500 ms", "30seconds",  # abbreviated / space-less: Spark-invalid, would loop
    "1.5 minutes", "1.5 hours", "1.5 days",             # fractional on a non-second unit: Spark-invalid
    "1.5 milliseconds",                                 # fractional allowed ONLY for 'second' (Spark: b=='s')
    "250 microseconds", "1 microsecond",                # sub-millisecond: below ProcessingTime granularity
    "999999999999 weeks", "12345678 seconds", "1.1234567 seconds",  # unbounded quantity/precision: overflows Spark
    "30 fortnights", "30 secondz", "5 blah",            # unknown / malformed unit
    "-5 seconds", "-1 minute", "1 minute -30 seconds",  # negative duration
])
def test_continuous_trigger_interval_rejects_malformed(bad):
    # Anything that is not a full-word, whitespace-separated "<number> <unit>" term is rejected at config
    # load - including abbreviated/space-less forms that Spark's ProcessingTime parser rejects. This
    # closes the gap where such a value would pass validation, fail only at .start(), and (on a
    # continuous run) restart in an endless loop.
    cfg = _continuous_ready()
    cfg["continuous"] = {"trigger_interval": bad}
    with pytest.raises(PipelineConfigError, match="trigger_interval"):
        validate_config(cfg)


def test_require_trigger_interval_returns_stripped():
    # The shared helper (also called by the runner notebook on its base_parameter) returns the trimmed
    # value on a valid interval.
    assert require_trigger_interval("  30 seconds  ", "streaming_trigger_interval") == "30 seconds"


@pytest.mark.parametrize("bad", ["", "5s", "30seconds", "1.5 minutes", "10 mins", None, 30])
def test_require_trigger_interval_fails_closed(bad):
    # The notebook re-validates the deploy-time base_parameter through this same helper, so a stale or
    # hand-edited value fails closed here rather than looping at Trigger.ProcessingTime.
    with pytest.raises(PipelineConfigError, match="streaming_trigger_interval"):
        require_trigger_interval(bad, "streaming_trigger_interval")


@pytest.mark.parametrize("bad", ["nope", 5, ["30 seconds"]])
def test_continuous_non_mapping_rejected(bad):
    cfg = _continuous_ready()
    cfg["continuous"] = bad
    with pytest.raises(PipelineConfigError, match="continuous"):
        validate_config(cfg)


def test_continuous_unknown_key_rejected():
    cfg = _continuous_ready()
    cfg["continuous"] = {"trigger_interval": "30 seconds", "pause_status": "UNPAUSED"}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


def test_continuous_requires_streaming():
    # An always-on run only applies to a stream; continuous + batch is a config error, not a downgrade.
    cfg = _continuous_ready()
    cfg["pipeline_mode"] = "batch"
    cfg["continuous"] = {"trigger_interval": "30 seconds"}
    with pytest.raises(PipelineConfigError, match="continuous requires pipeline_mode: streaming"):
        validate_config(cfg)


def test_continuous_omitting_pipeline_mode_fails_closed():
    # pipeline_mode is now optional (omit => "" => inherit the ${var.pipeline_mode} global), BUT a
    # continuous pipeline must declare streaming EXPLICITLY: generation resolves this cross-field guard
    # before the bundle resolves the variable, so an omitted mode ("") can't be seen as streaming and
    # fails closed. The message points the author at declaring it explicitly.
    cfg = _continuous_ready()
    del cfg["pipeline_mode"]
    cfg["continuous"] = {"trigger_interval": "30 seconds"}
    with pytest.raises(PipelineConfigError, match="cannot inherit"):
        validate_config(cfg)


def test_continuous_rejects_serverless():
    # Serverless supports only Trigger.availableNow, not the ProcessingTime trigger an always-on stream
    # needs, so continuous on serverless (the default compute) fails closed at config load.
    cfg = _base()
    cfg["pipeline_mode"] = "streaming"  # serverless default compute
    cfg["continuous"] = {"trigger_interval": "30 seconds"}
    with pytest.raises(PipelineConfigError, match="continuous requires classic compute"):
        validate_config(cfg)


@pytest.mark.parametrize("compute", [
    {"type": "job_cluster", "job_cluster_config": "standard_batch"},
    {"type": "existing_cluster", "cluster_config": "interactive_primary"},
])
def test_continuous_accepts_classic_compute(compute):
    cfg = _base()
    cfg["pipeline_mode"] = "streaming"
    cfg["compute"] = compute
    cfg["continuous"] = {"trigger_interval": "30 seconds"}
    assert validate_config(cfg)["continuous"] == {"trigger_interval": "30 seconds"}


def test_continuous_and_schedule_mutually_exclusive():
    # A job has EITHER a continuous trigger OR a schedule, never both.
    cfg = _continuous_ready()
    cfg["continuous"] = {"trigger_interval": "30 seconds"}
    cfg["schedule"] = {"quartz_cron_expression": "0 0 8 * * ?"}
    with pytest.raises(PipelineConfigError, match="mutually exclusive"):
        validate_config(cfg)


def test_continuous_carried_through_resolve():
    # continuous is a deploy-time job property, not an object name: resolve passes it through unchanged.
    cfg = _continuous_ready()
    cfg["view"]["catalog"] = "acme_${environment}"
    cfg["source"]["catalog"] = "acme_${environment}"
    cfg["continuous"] = {"trigger_interval": "30 seconds"}
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["continuous"] == {"trigger_interval": "30 seconds"}


def test_continuous_none_carried_through_resolve():
    assert resolve_config(validate_config(_base()), environment="")["continuous"] is None


# ------------------------------------------------- max_files_per_trigger / max_bytes_per_trigger


@pytest.mark.parametrize("value,expected", [(500, "500"), ("250", "250"), ("", ""), (None, "")])
def test_require_max_files_per_trigger_canonical(value, expected):
    assert require_max_files_per_trigger(value) == expected


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, "abc", "12.5"])
def test_require_max_files_per_trigger_fails_closed(bad):
    with pytest.raises(PipelineConfigError, match="max_files_per_trigger"):
        require_max_files_per_trigger(bad)


@pytest.mark.parametrize("value,expected", [
    ("128m", "128m"), ("512MB", "512mb"), ("1g", "1g"), (1048576, "1048576"),
    ("", ""), (None, ""), ("0", ""), ("0m", ""), (0, ""),
])
def test_require_max_bytes_per_trigger_canonical(value, expected):
    # A byte-size is canonicalized (lowercased); empty OR a zero size => "" (unset, no cap).
    assert require_max_bytes_per_trigger(value) == expected


@pytest.mark.parametrize("bad", [-1, 1.5, True, "128x", "abc"])
def test_require_max_bytes_per_trigger_fails_closed(bad):
    with pytest.raises(PipelineConfigError, match="max_bytes_per_trigger"):
        require_max_bytes_per_trigger(bad)


def test_rate_limits_absent_default_empty_in_config():
    out = validate_config(_base())
    assert out["max_files_per_trigger"] == "" and out["max_bytes_per_trigger"] == ""


def test_rate_limits_from_config():
    cfg = _base()
    cfg["max_files_per_trigger"] = 200
    cfg["max_bytes_per_trigger"] = "256m"
    out = validate_config(cfg)
    assert out["max_files_per_trigger"] == "200" and out["max_bytes_per_trigger"] == "256m"


@pytest.mark.parametrize("field,bad", [
    ("max_files_per_trigger", 0), ("max_files_per_trigger", "abc"),
    ("max_bytes_per_trigger", "128x"), ("max_bytes_per_trigger", -1),
])
def test_rate_limits_bad_config_value_fails_closed(field, bad):
    cfg = _base()
    cfg[field] = bad
    with pytest.raises(PipelineConfigError, match=field):
        validate_config(cfg)


def test_rate_limits_carried_through_resolve():
    cfg = _with_env()
    cfg["max_files_per_trigger"] = 200
    cfg["max_bytes_per_trigger"] = "256m"
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["max_files_per_trigger"] == "200" and out["max_bytes_per_trigger"] == "256m"


def test_job_parameters_rate_limits_default_from_config():
    cfg = _base()
    cfg["max_files_per_trigger"] = 200
    cfg["max_bytes_per_trigger"] = "256m"
    params = job_parameters(validate_config(cfg))
    assert {"name": "max_files_per_trigger", "default": "200"} in params
    assert {"name": "max_bytes_per_trigger", "default": "256m"} in params


# --------------------------------------------------------------------------- job_group / job_name_postfix


def test_job_group_and_postfix_default_none():
    cfg = validate_config(_base())
    assert cfg["job_group"] is None
    assert cfg["job_name_postfix"] is None


def test_job_group_valid_identifier_accepted():
    cfg = _base()
    cfg["job_group"] = "ecs_streams-1"
    assert validate_config(cfg)["job_group"] == "ecs_streams-1"


@pytest.mark.parametrize("bad", ["has space", "a.b", "grp/sub", ""])
def test_job_group_bad_identifier_fails_closed(bad):
    cfg = _base()
    cfg["job_group"] = bad
    with pytest.raises(PipelineConfigError, match="job_group"):
        validate_config(cfg)


def test_job_name_postfix_string_accepted_and_stripped():
    cfg = _base()
    cfg["job_name_postfix"] = "  ECS DNS + Auth  "
    assert validate_config(cfg)["job_name_postfix"] == "ECS DNS + Auth"


@pytest.mark.parametrize("bad", ["", "   ", "line1\nline2", 5, True])
def test_job_name_postfix_bad_value_fails_closed(bad):
    cfg = _base()
    cfg["job_name_postfix"] = bad
    with pytest.raises(PipelineConfigError, match="job_name_postfix"):
        validate_config(cfg)


def test_job_group_and_postfix_carried_through_resolve():
    cfg = _base()
    cfg["job_group"] = "g1"
    cfg["job_name_postfix"] = "My Group"
    out = resolve_config(validate_config(cfg), environment="")
    assert out["job_group"] == "g1" and out["job_name_postfix"] == "My Group"

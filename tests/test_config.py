"""Offline unit tests for pipeline_lib.config. No Spark, no cluster, no live ES: plain pytest.

Covers the validation contract (every fail-closed branch), the ${environment} template + resolution
logic, and the derivations (view_substitutions, job_base_parameters).
"""
import copy

import pytest

from pipeline_lib.config import (
    PipelineConfigError,
    column_present,
    job_base_parameters,
    job_parameters,
    render_view_sql,
    require_chunk_size,
    require_es_flag,
    require_filter_condition,
    require_max_partition_bytes,
    require_pipeline_mode,
    require_streaming_start,
    require_write_repartition,
    resolve_config,
    resolve_name,
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
    assert out["require_existing_index"] == ""
    assert out["verify_certs"] == ""
    # write_repartition and max_partition_bytes are the exceptions: omitted, they take a built-in
    # default (NOT "unset"). write_repartition defaults to 0 (off - read parallelism is the primary
    # lever); max_partition_bytes defaults to the built-in scan-parallelism size.
    assert out["write_repartition"] == "0"
    assert out["max_partition_bytes"] == "2m"


def test_environment_token_accepted_as_template():
    # validate_config accepts the template; it does NOT resolve it.
    out = validate_config(_with_env())
    assert out["source"]["catalog"] == "acme_${environment}"
    assert out["reference_tables"]["validation"]["schema"] == "ocsf_validation_${environment}"


# --------------------------------------------------------------------------- fail-closed: structure


@pytest.mark.parametrize("missing", ["es_index_name", "es_id_field", "pipeline_mode", "view", "source"])
def test_missing_required_key(missing):
    cfg = _base()
    del cfg[missing]
    with pytest.raises(PipelineConfigError, match="missing required key"):
        validate_config(cfg)


def test_missing_source_primary_key():
    # primary_key now lives inside source; a source without it must fail closed.
    cfg = _base()
    del cfg["source"]["primary_key"]
    with pytest.raises(PipelineConfigError, match="source.primary_key"):
        validate_config(cfg)


def test_source_primary_key_rejects_environment_token():
    # primary_key is a column identifier, not an object name: no ${environment} token.
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
    cfg = _base()
    cfg["es_id_field"] = bad
    with pytest.raises(PipelineConfigError, match="es_id_field"):
        validate_config(cfg)


@pytest.mark.parametrize("mode", ["batch", "streaming"])
def test_pipeline_mode_allowed_values(mode):
    cfg = _base()
    cfg["pipeline_mode"] = mode
    assert validate_config(cfg)["pipeline_mode"] == mode


@pytest.mark.parametrize("bad", ["Batch", "BATCH", "stream", "micro-batch", "", None, 5, True])
def test_pipeline_mode_rejects_non_allowlisted(bad):
    # Allow-list: only exactly 'batch'/'streaming'. A near-miss, wrong case, empty, or non-string
    # must fail closed - never silently defaulted.
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
    }


def test_job_base_parameters_excludes_run_time_params():
    # Run-time job parameters (pipeline_mode, filter_condition, streaming_start, and the EsWriteConfig
    # tuning knobs) must NOT leak into base_parameters, which would re-fix them at deploy and defeat
    # per-run override.
    params = _job_base_parameters("x")
    for run_time in ("pipeline_mode", "filter_condition", "chunk_size", "require_existing_index",
                     "verify_certs", "streaming_start", "write_repartition", "max_partition_bytes"):
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
    # The generated job exposes exactly these run-time parameters, in this order. filter_condition's
    # default comes from the config; the tuning knobs default to "" (meaning "use connector default").
    cfg = _base()
    cfg["filter_condition"] = "action = 'allowed'"
    assert job_parameters(validate_config(cfg)) == [
        {"name": "pipeline_mode", "default": "batch"},
        {"name": "filter_condition", "default": "action = 'allowed'"},
        {"name": "chunk_size", "default": ""},
        {"name": "require_existing_index", "default": ""},
        {"name": "verify_certs", "default": ""},
        {"name": "streaming_start", "default": "new"},
        {"name": "write_repartition", "default": "0"},
        {"name": "max_partition_bytes", "default": "2m"},
    ]


def test_job_parameters_streaming_start_defaults_new():
    # streaming_start is a literal default (not a config key), always "new" regardless of the config.
    params = job_parameters(validate_config(_base()))
    assert {"name": "streaming_start", "default": "new"} in params


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


def test_write_config_overrides_combined():
    assert write_config_overrides("500", "false", "false") == {
        "chunk_size": 500,
        "require_existing_index": False,
        "verify_certs": False,
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
    ("require_existing_index", "maybe"), ("require_existing_index", 1),
    ("verify_certs", "yes"),
])
def test_tuning_knobs_bad_config_value_fails_closed(key, bad):
    cfg = _base()
    cfg[key] = bad
    with pytest.raises(PipelineConfigError, match=key):
        validate_config(cfg)


def test_tuning_knobs_carried_through_resolve():
    # Connector settings, not object names: resolve passes the canonical strings through unchanged.
    cfg = _with_env()
    cfg["chunk_size"] = 800
    cfg["verify_certs"] = False
    out = resolve_config(validate_config(cfg), environment="prod")
    assert out["chunk_size"] == "800"
    assert out["verify_certs"] == "false"
    assert out["require_existing_index"] == ""  # omitted -> unset


def test_job_parameters_tuning_defaults_from_config():
    # The tuning job parameters' defaults come from the config (the whole point of this change), in
    # canonical string form; an omitted knob defaults to "".
    cfg = _base()
    cfg["chunk_size"] = 1000
    cfg["verify_certs"] = False
    params = job_parameters(validate_config(cfg))
    assert {"name": "chunk_size", "default": "1000"} in params
    assert {"name": "verify_certs", "default": "false"} in params
    assert {"name": "require_existing_index", "default": ""} in params


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


def test_write_repartition_absent_defaults_builtin_in_config():
    # A config that omits write_repartition stores the built-in default (canonical string), which then
    # becomes the job-parameter default. Default is 0 = off.
    assert validate_config(_base())["write_repartition"] == "0"


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


def test_max_partition_bytes_absent_defaults_builtin_in_config():
    assert validate_config(_base())["max_partition_bytes"] == "2m"


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
    # The run-time override validator (used by the notebook on the job-parameter value) accepts the
    # allow-listed modes and returns them unchanged.
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


def test_compute_existing_cluster_valid():
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", "existing_cluster_id": "0123-456789-abcde"}
    assert validate_config(cfg)["compute"] == {
        "type": "existing_cluster",
        "existing_cluster_id": "0123-456789-abcde",
    }


def test_compute_existing_cluster_id_trimmed():
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", "existing_cluster_id": "  0123-456789-abcde  "}
    assert validate_config(cfg)["compute"]["existing_cluster_id"] == "0123-456789-abcde"


@pytest.mark.parametrize("bad", [None, "", "   ", 5, True, ["x"]])
def test_compute_existing_cluster_requires_id(bad):
    # existing_cluster with a missing/empty/non-string id must fail closed.
    cfg = _base()
    compute = {"type": "existing_cluster"}
    if bad is not None:
        compute["existing_cluster_id"] = bad
    cfg["compute"] = compute
    with pytest.raises(PipelineConfigError, match="existing_cluster_id"):
        validate_config(cfg)


def test_compute_existing_cluster_rejects_unknown_key():
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", "existing_cluster_id": "x", "job_cluster_config": "y"}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


def test_compute_existing_cluster_config_valid():
    # existing_cluster may name a bundle variable (cluster_config) instead of a literal id, for a
    # per-target (workspace-specific) cluster. The generator turns it into a ${var.<name>} reference.
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster", "cluster_config": "interactive_primary"}
    assert validate_config(cfg)["compute"] == {
        "type": "existing_cluster",
        "cluster_config": "interactive_primary",
    }


def test_compute_existing_cluster_rejects_both_id_and_config():
    # Exactly one of existing_cluster_id / cluster_config: naming both is ambiguous and fails closed.
    cfg = _base()
    cfg["compute"] = {
        "type": "existing_cluster", "existing_cluster_id": "0123-x", "cluster_config": "interactive_primary",
    }
    with pytest.raises(PipelineConfigError, match="exactly one"):
        validate_config(cfg)


def test_compute_existing_cluster_rejects_neither_id_nor_config():
    cfg = _base()
    cfg["compute"] = {"type": "existing_cluster"}
    with pytest.raises(PipelineConfigError, match="exactly one"):
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

"""Offline unit tests for pipeline_lib.config. No Spark, no cluster, no live ES: plain pytest.

Covers the validation contract (every fail-closed branch), the ${environment} template + resolution
logic, and the derivations (view_substitutions, job_base_parameters).
"""
import copy

import pytest

from pipeline_lib.config import (
    PipelineConfigError,
    job_base_parameters,
    resolve_config,
    resolve_name,
    validate_config,
    view_substitutions,
)


def _base():
    """A minimal valid config (no reference tables, no environment tokens)."""
    return {
        "es_index_name": "ecs-dns-activity",
        "primary_key": "dsl_id",
        "view": {"catalog": "cat", "schema": "es_poc", "name": "ecs_dns_activity"},
        "source": {"catalog": "cat", "schema": "ocsf", "table": "dns_activity"},
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


def test_environment_token_accepted_as_template():
    # validate_config accepts the template; it does NOT resolve it.
    out = validate_config(_with_env())
    assert out["source"]["catalog"] == "acme_${environment}"
    assert out["reference_tables"]["validation"]["schema"] == "ocsf_validation_${environment}"


# --------------------------------------------------------------------------- fail-closed: structure


@pytest.mark.parametrize("missing", ["es_index_name", "primary_key", "view", "source"])
def test_missing_required_key(missing):
    cfg = _base()
    del cfg[missing]
    with pytest.raises(PipelineConfigError, match="missing required key"):
        validate_config(cfg)


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


# --------------------------------------------------------------------------- job_base_parameters


def test_job_base_parameters():
    params = job_base_parameters("ecs_dns_activity", environment_ref="${var.environment}")
    assert params == {"config_name": "ecs_dns_activity", "environment": "${var.environment}"}


def test_validate_does_not_mutate_input():
    cfg = _with_env()
    before = copy.deepcopy(cfg)
    validate_config(cfg)
    assert cfg == before

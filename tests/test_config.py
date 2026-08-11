"""Offline unit tests for pipeline_lib.config. No Spark, no cluster, no live ES: plain pytest.

Covers the validation contract (every fail-closed branch) and the two derivation functions
(view_substitutions, job_base_parameters), including the reference-table + broadcast-hint logic.
"""
import copy

import pytest

from pipeline_lib.config import (
    PipelineConfigError,
    job_base_parameters,
    validate_config,
    view_substitutions,
)


def _base():
    """A minimal valid config (no reference tables)."""
    return {
        "es_index_name": "ecs-dns-activity",
        "primary_key": "dsl_id",
        "view": {"schema": "es_poc", "name": "ecs_dns_activity"},
        "source": {"schema": "ocsf", "table": "dns_activity"},
    }


def _with_refs():
    cfg = _base()
    cfg["reference_tables"] = {
        "validation": {"schema": "ocsf_validation", "table": "dns_activity", "broadcast": False},
        "geo": {"schema": "ref", "table": "geoip", "broadcast": True},
    }
    return cfg


# --------------------------------------------------------------------------- valid configs


def test_minimal_valid():
    out = validate_config(_base())
    assert out["view"] == {"schema": "es_poc", "name": "ecs_dns_activity"}
    assert out["source"] == {"schema": "ocsf", "table": "dns_activity"}
    assert out["reference_tables"] == {}  # defaulted


def test_reference_tables_defaults_broadcast_false():
    cfg = _base()
    cfg["reference_tables"] = {"validation": {"schema": "ocsf_validation", "table": "dns_activity"}}
    out = validate_config(cfg)
    assert out["reference_tables"]["validation"]["broadcast"] is False


# --------------------------------------------------------------------------- fail-closed branches


@pytest.mark.parametrize("missing", ["es_index_name", "primary_key", "view", "source"])
def test_missing_required_key(missing):
    cfg = _base()
    del cfg[missing]
    with pytest.raises(PipelineConfigError, match="missing required key"):
        validate_config(cfg)


def test_unknown_top_level_key():
    cfg = _base()
    cfg["source_table"] = "oops"  # a plausible legacy/typo key
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


def test_unknown_nested_key():
    cfg = _base()
    cfg["source"]["tabel"] = "typo"
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


@pytest.mark.parametrize("bad", ["my-schema", "my schema", "cat.schema", "1abc", "", "select", None, 5])
def test_illegal_identifier_rejected(bad):
    # 'select' is a legal identifier lexically (the DDL would fail loudly at spark.sql, not here),
    # so it is intentionally NOT in this list except to note it: it passes the regex. Everything else
    # here must be rejected.
    if bad == "select":
        return
    cfg = _base()
    cfg["source"]["schema"] = bad
    with pytest.raises(PipelineConfigError):
        validate_config(cfg)


@pytest.mark.parametrize("bad", ["Has-Caps", "UPPER", "has space", ".leading", "-leading", ""])
def test_illegal_es_index_rejected(bad):
    cfg = _base()
    cfg["es_index_name"] = bad
    with pytest.raises(PipelineConfigError, match="es_index_name"):
        validate_config(cfg)


def test_es_index_allows_hyphen():
    cfg = _base()
    cfg["es_index_name"] = "ecs-dns-activity"
    assert validate_config(cfg)["es_index_name"] == "ecs-dns-activity"


def test_reference_broadcast_must_be_bool():
    cfg = _base()
    cfg["reference_tables"] = {"v": {"schema": "s", "table": "t", "broadcast": "yes"}}
    with pytest.raises(PipelineConfigError, match="broadcast"):
        validate_config(cfg)


def test_reference_alias_must_be_identifier():
    cfg = _base()
    cfg["reference_tables"] = {"bad-alias": {"schema": "s", "table": "t"}}
    with pytest.raises(PipelineConfigError):
        validate_config(cfg)


def test_reference_unknown_key():
    cfg = _base()
    cfg["reference_tables"] = {"v": {"schema": "s", "table": "t", "brodcast": True}}
    with pytest.raises(PipelineConfigError, match="unknown key"):
        validate_config(cfg)


# --------------------------------------------------------------------------- view_substitutions


def test_view_substitutions_no_refs():
    subs = view_substitutions(validate_config(_base()), catalog="mycat")
    assert subs["catalog"] == "mycat"
    assert subs["view_schema"] == "es_poc"
    assert subs["source_table"] == "dns_activity"
    assert subs["broadcast_hint"] == ""  # no broadcast refs
    assert not any(k.startswith("ref_") for k in subs)


def test_view_substitutions_ref_alias_and_broadcast():
    subs = view_substitutions(validate_config(_with_refs()), catalog="mycat")
    # ref_<alias> expands to an aliased fully-qualified table
    assert subs["ref_validation"] == "mycat.ocsf_validation.dns_activity validation"
    assert subs["ref_geo"] == "mycat.ref.geoip geo"
    # only the broadcast=true ref lands in the hint, naming its alias
    assert subs["broadcast_hint"] == "/*+ BROADCAST(geo) */"


def test_view_substitutions_multiple_broadcast():
    cfg = _with_refs()
    cfg["reference_tables"]["validation"]["broadcast"] = True
    subs = view_substitutions(validate_config(cfg), catalog="c")
    # both aliases, order-stable (insertion order)
    assert subs["broadcast_hint"] == "/*+ BROADCAST(validation, geo) */"


def test_view_substitutions_rejects_bad_catalog():
    with pytest.raises(PipelineConfigError):
        view_substitutions(validate_config(_base()), catalog="bad-catalog")


# --------------------------------------------------------------------------- job_base_parameters


def test_job_base_parameters_excludes_reference_tables():
    params = job_base_parameters(validate_config(_with_refs()))
    assert params == {
        "es_index_name": "ecs-dns-activity",
        "primary_key": "dsl_id",
        "view_schema": "es_poc",
        "view_name": "ecs_dns_activity",
        "source_schema": "ocsf",
        "source_table": "dns_activity",
    }


def test_validate_does_not_mutate_input():
    cfg = _with_refs()
    before = copy.deepcopy(cfg)
    validate_config(cfg)
    assert cfg == before

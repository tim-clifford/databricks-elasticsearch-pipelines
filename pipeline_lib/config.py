"""Load, validate, and derive substitutions from a pipeline definition config.

One pipeline_definitions/<name>.yml describes one Elasticsearch index's pipeline. This module is the
single source of truth for that schema: both the offline job generator (scripts/gen_jobs.py) and the
on-cluster deploy_views notebook import it, so validation can never drift between them.

Schema (see pipeline_definitions/*.yml for a commented example):

    es_index_name: <es index>          # ES index name (hyphens allowed; NOT a SQL identifier)
    primary_key:   <column>            # view column used as the ES document _id
    view:   { schema: <s>, name:  <n> }   # where the view is created, and its name
    source: { schema: <s>, table: <t> }   # the one source table the view reads from
    reference_tables:                  # OPTIONAL: extra tables the view joins
      <alias>:                         # key is caller-chosen; must match ${ref_<alias>} in the SQL
        schema: <s>
        table:  <t>
        broadcast: <bool>              # optional, default false; adds a Spark BROADCAST hint

`catalog` is intentionally NOT part of the config: it is one shared value per environment, supplied
as a bundle variable at deploy time.
"""
from __future__ import annotations

import re

# A value substituted into SQL as a bare (unquoted) identifier: a letter/underscore, then letters,
# digits, underscores. Rejects a hyphen, space, dot, quote, or reserved punctuation, so a bad value
# fails closed at deploy time instead of producing invalid SQL or binding to the wrong object.
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ES index names are not SQL identifiers: lowercase, may contain hyphens/dots/underscores, must start
# with an alphanumeric. Conservative subset of Elasticsearch's own rules, enough to reject the values
# that would break (uppercase, spaces, leading punctuation).
_VALID_ES_INDEX = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


class PipelineConfigError(ValueError):
    """A pipeline definition is invalid. Raised at load time, never at row time (fail closed)."""


def _require_identifier(value: object, where: str) -> str:
    if not isinstance(value, str) or not _VALID_IDENTIFIER.match(value):
        raise PipelineConfigError(
            f"{where} must be a legal SQL identifier (letter/underscore, then letters/digits/"
            f"underscores), got {value!r}"
        )
    return value


def _require_table_map(node: object, where: str) -> dict:
    """Validate a {schema, table} (or {schema, name}) mapping and return it normalized."""
    if not isinstance(node, dict):
        raise PipelineConfigError(f"{where} must be a mapping with schema + object name, got {type(node).__name__}")
    return node


def load_config(path: str) -> dict:
    """Load one config file, validate it, and return a normalized dict. Fail closed on any problem.

    The returned dict mirrors the file with values stripped and defaults applied
    (reference_tables -> {}, each reference table's broadcast -> False).
    """
    import yaml

    with open(path) as fh:
        raw = yaml.safe_load(fh)
    return validate_config(raw, source=path)


def validate_config(raw: object, source: str = "<config>") -> dict:
    """Validate an already-parsed config mapping. Separated from load_config so it is unit-testable
    without a file. `source` only labels error messages."""
    if not isinstance(raw, dict):
        raise PipelineConfigError(f"{source}: expected a YAML mapping, got {type(raw).__name__}")

    allowed_top = {"es_index_name", "primary_key", "view", "source", "reference_tables"}
    unknown = sorted(set(raw) - allowed_top)
    if unknown:
        raise PipelineConfigError(f"{source}: unknown key(s): {', '.join(unknown)}; allowed: {', '.join(sorted(allowed_top))}")

    for key in ("es_index_name", "primary_key", "view", "source"):
        if key not in raw:
            raise PipelineConfigError(f"{source}: missing required key '{key}'")

    es_index_name = raw["es_index_name"]
    if not isinstance(es_index_name, str) or not _VALID_ES_INDEX.match(es_index_name):
        raise PipelineConfigError(
            f"{source}: es_index_name must be a valid ES index name (lowercase; letters, digits, "
            f"'.', '-', '_'; leading alphanumeric), got {es_index_name!r}"
        )

    primary_key = _require_identifier(raw["primary_key"], f"{source}: primary_key")

    view = _require_table_map(raw["view"], f"{source}: view")
    view_unknown = sorted(set(view) - {"schema", "name"})
    if view_unknown:
        raise PipelineConfigError(f"{source}: view has unknown key(s): {', '.join(view_unknown)}; allowed: name, schema")
    view_schema = _require_identifier(view.get("schema"), f"{source}: view.schema")
    view_name = _require_identifier(view.get("name"), f"{source}: view.name")

    source_map = _require_table_map(raw["source"], f"{source}: source")
    source_unknown = sorted(set(source_map) - {"schema", "table"})
    if source_unknown:
        raise PipelineConfigError(f"{source}: source has unknown key(s): {', '.join(source_unknown)}; allowed: schema, table")
    source_schema = _require_identifier(source_map.get("schema"), f"{source}: source.schema")
    source_table = _require_identifier(source_map.get("table"), f"{source}: source.table")

    reference_tables = _validate_reference_tables(raw.get("reference_tables"), source)

    return {
        "es_index_name": es_index_name,
        "primary_key": primary_key,
        "view": {"schema": view_schema, "name": view_name},
        "source": {"schema": source_schema, "table": source_table},
        "reference_tables": reference_tables,
    }


def _validate_reference_tables(node: object, source: str) -> dict:
    if node is None:
        return {}
    if not isinstance(node, dict):
        raise PipelineConfigError(f"{source}: reference_tables must be a mapping of alias -> table, got {type(node).__name__}")

    result: dict[str, dict] = {}
    for alias, spec in node.items():
        where = f"{source}: reference_tables.{alias}"
        # The alias is used both as the ${ref_<alias>} substitution key and as the SQL join alias,
        # so it must itself be a legal identifier.
        _require_identifier(alias, f"{source}: reference_tables key {alias!r}")
        if not isinstance(spec, dict):
            raise PipelineConfigError(f"{where} must be a mapping with schema + table, got {type(spec).__name__}")
        spec_unknown = sorted(set(spec) - {"schema", "table", "broadcast"})
        if spec_unknown:
            raise PipelineConfigError(f"{where} has unknown key(s): {', '.join(spec_unknown)}; allowed: schema, table, broadcast")
        schema = _require_identifier(spec.get("schema"), f"{where}.schema")
        table = _require_identifier(spec.get("table"), f"{where}.table")
        broadcast = spec.get("broadcast", False)
        if not isinstance(broadcast, bool):
            raise PipelineConfigError(f"{where}.broadcast must be true or false, got {broadcast!r}")
        result[alias] = {"schema": schema, "table": table, "broadcast": broadcast}
    return result


def view_substitutions(cfg: dict, catalog: str) -> dict:
    """The ${...} tokens a view .sql may reference, resolved for a given catalog.

    - view_schema / view_name / source_schema / source_table: bare identifiers.
    - ref_<alias>: the aliased, fully-qualified reference table, e.g. `cat.schema.table alias`, so
      the SQL writes `LEFT JOIN ${ref_alias} ON ...` and refers to columns via the alias.
    - broadcast_hint: a Spark hint naming every reference table with broadcast=true (or '' if none).
      A Spark broadcast hint must sit immediately after the top-level SELECT and name the join alias;
      the framework owns the alias, so the hint always resolves to a real relation.
    """
    _require_identifier(catalog, "catalog")
    subs = {
        "catalog": catalog,
        "view_schema": cfg["view"]["schema"],
        "view_name": cfg["view"]["name"],
        "source_schema": cfg["source"]["schema"],
        "source_table": cfg["source"]["table"],
    }
    broadcast_aliases = []
    for alias, spec in cfg["reference_tables"].items():
        subs[f"ref_{alias}"] = f"{catalog}.{spec['schema']}.{spec['table']} {alias}"
        if spec["broadcast"]:
            broadcast_aliases.append(alias)
    subs["broadcast_hint"] = f"/*+ BROADCAST({', '.join(broadcast_aliases)}) */" if broadcast_aliases else ""
    return subs


def job_base_parameters(cfg: dict) -> dict:
    """The scalar values the generated per-index job passes to run_index_pipeline.py as widgets.

    Only the values that notebook needs to read the view and write to ES: it reads the deployed
    view, not the raw source, so reference tables (a view-build concern) are deliberately excluded.
    All values are strings, as job base_parameters must be.
    """
    return {
        "es_index_name": cfg["es_index_name"],
        "primary_key": cfg["primary_key"],
        "view_schema": cfg["view"]["schema"],
        "view_name": cfg["view"]["name"],
        "source_schema": cfg["source"]["schema"],
        "source_table": cfg["source"]["table"],
    }

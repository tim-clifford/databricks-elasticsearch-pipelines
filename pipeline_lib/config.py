"""Load, validate, and derive substitutions from a pipeline definition config.

One pipeline_definitions/<name>.yml describes one Elasticsearch index's pipeline. This module is the
single source of truth for that schema: both the offline job generator (scripts/gen_jobs.py) and the
on-cluster notebooks (deploy_views.py, run_index_pipeline.py) import it, so validation can never drift.

Schema (see pipeline_definitions/*.yml for a commented example):

    es_index_name: <es index>            # ES index name (hyphens allowed; NOT a SQL identifier)
    primary_key:   <column>              # view column used as the ES document _id
    view:   { catalog: <c>, schema: <s>, name:  <n> }   # where the view is created, and its name
    source: { catalog: <c>, schema: <s>, table: <t> }   # the one source table the view reads from
    reference_tables:                    # OPTIONAL: extra tables the view joins
      <alias>:                           # key is caller-chosen; matches ${ref_<alias>} in the SQL
        catalog: <c>
        schema:  <s>
        table:   <t>

ENVIRONMENT SUBSTITUTION
A `catalog` or `schema` value may embed the token `${environment}`, folded in at deploy time from the
`environment` bundle variable, e.g. `ocsf_${environment}` -> `ocsf_prod`. A value with no token is used
as-is. The environment component only ever belongs at the catalog/schema level, so table/view NAMES
are plain identifiers and may NOT contain the token (this also guarantees a view's name always equals
its `.sql` filename). Validated in two phases: at load, a catalog/schema is a legal identifier
*template* (identifier characters plus optional ${environment} tokens) and a name/table is a plain
identifier; at resolve, the token is substituted and the RESULT must be a legal bare identifier, so a
bad environment value (a hyphen, say) fails closed at resolve time rather than producing invalid SQL.
"""
from __future__ import annotations

import re

# A resolved value substituted into SQL as a bare (unquoted) identifier: a letter/underscore, then
# letters, digits, underscores. Rejects a hyphen, space, dot, quote, or reserved punctuation.
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# The ${environment} substitution token, the only token allowed inside a catalog/schema template.
_ENV_TOKEN = "${environment}"

# A catalog/schema TEMPLATE (pre-resolution): identifier characters and/or ${environment} tokens,
# nothing else. The token may appear anywhere (prefix/infix/suffix); after the tokens, only identifier
# characters may remain, and the template must be non-empty. Rejects stray `${...}`, dots, spaces, and
# leading hyphens, so a malformed template fails at load, not at SQL time. (Table/view NAMES do not
# use this: they must be plain identifiers, so a view name always equals its .sql filename.)
_VALID_NAME_TEMPLATE = re.compile(r"^([A-Za-z0-9_]|\$\{environment\})+$")

# ES index names are not SQL identifiers. Per Elasticsearch's rules (verified against the docs):
# lowercase; the chars \ / * ? " < > | space , # : are forbidden; cannot start with -, _, + (or the
# deprecated leading .); cannot be "." or ".."; max 255 BYTES. This char class (alphanumeric first,
# then [a-z0-9._-]) already enforces the lowercase / allowed-char / no-bad-leading-char rules; the
# length and trailing-dot checks are applied separately in _require_es_index (a regex can't do bytes).
_VALID_ES_INDEX = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_ES_INDEX_MAX_BYTES = 255


class PipelineConfigError(ValueError):
    """A pipeline definition is invalid. Raised at load/resolve time, never at row time (fail closed)."""


def _require_name_template(value: object, where: str) -> str:
    """A catalog/schema/table/name value: a legal identifier template (may contain ${environment})."""
    if not isinstance(value, str) or not _VALID_NAME_TEMPLATE.match(value):
        raise PipelineConfigError(
            f"{where} must be an identifier, optionally containing '${{environment}}' "
            f"(letters, digits, underscore, and ${{environment}} tokens only), got {value!r}"
        )
    return value


def _require_identifier(value: object, where: str) -> str:
    """A value that must ALREADY be a bare identifier (no template tokens), e.g. primary_key."""
    if not isinstance(value, str) or not _VALID_IDENTIFIER.match(value):
        raise PipelineConfigError(
            f"{where} must be a legal SQL identifier (letter/underscore, then letters/digits/"
            f"underscores), got {value!r}"
        )
    return value


def _require_es_index(value: object, where: str) -> str:
    """A valid Elasticsearch index name. Enforces the char/leading-char rules via the regex, plus the
    255-BYTE length bound and no-trailing-dot rule (which a single regex can't express well)."""
    if not isinstance(value, str) or not _VALID_ES_INDEX.match(value):
        raise PipelineConfigError(
            f"{where} must be a valid ES index name (lowercase; letters, digits, '.', '-', '_'; "
            f"leading alphanumeric), got {value!r}"
        )
    if value.endswith("."):
        raise PipelineConfigError(f"{where} must not end with '.', got {value!r}")
    if len(value.encode("utf-8")) > _ES_INDEX_MAX_BYTES:
        raise PipelineConfigError(
            f"{where} must be at most {_ES_INDEX_MAX_BYTES} bytes, got {len(value.encode('utf-8'))}"
        )
    return value


def resolve_name(template: str, environment: str, where: str) -> str:
    """Fold `environment` into a name template and validate the result is a bare identifier.

    A template with no ${environment} token needs no environment and is returned once validated. A
    template that DOES use the token requires a non-empty environment, and the substituted result
    must itself be a legal identifier (this is where a bad environment value fails closed)."""
    if _ENV_TOKEN in template and not environment:
        raise PipelineConfigError(
            f"{where} uses ${{environment}} but no environment was provided"
        )
    # str.replace, NOT re.sub: re.sub treats `environment` as a replacement template, so a value
    # containing a backslash or group reference would raise re.error instead of failing closed here.
    resolved = template.replace(_ENV_TOKEN, environment)
    if not _VALID_IDENTIFIER.match(resolved):
        raise PipelineConfigError(
            f"{where} resolved to {resolved!r} (from {template!r} with environment={environment!r}), "
            f"which is not a legal SQL identifier"
        )
    return resolved


def load_config(path: str) -> dict:
    """Load one config file, validate its structure, and return a normalized dict. Fail closed.

    Names are validated as identifier templates here; ${environment} is folded in later by
    resolve_config. Defaults applied: reference_tables -> {}.
    """
    import yaml

    with open(path) as fh:
        raw = yaml.safe_load(fh)
    return validate_config(raw, source=path)


def validate_config(raw: object, source: str = "<config>") -> dict:
    """Validate an already-parsed config mapping (structure + name templates). Unit-testable without
    a file. `source` only labels error messages. Does NOT resolve ${environment} (see resolve_config)."""
    if not isinstance(raw, dict):
        raise PipelineConfigError(f"{source}: expected a YAML mapping, got {type(raw).__name__}")

    allowed_top = {"es_index_name", "primary_key", "view", "source", "reference_tables"}
    unknown = sorted(set(raw) - allowed_top)
    if unknown:
        raise PipelineConfigError(f"{source}: unknown key(s): {', '.join(unknown)}; allowed: {', '.join(sorted(allowed_top))}")

    for key in ("es_index_name", "primary_key", "view", "source"):
        if key not in raw:
            raise PipelineConfigError(f"{source}: missing required key '{key}'")

    es_index_name = _require_es_index(raw["es_index_name"], f"{source}: es_index_name")
    primary_key = _require_identifier(raw["primary_key"], f"{source}: primary_key")
    view = _validate_object(raw["view"], f"{source}: view", name_key="name", allowed={"catalog", "schema", "name"})
    source_map = _validate_object(raw["source"], f"{source}: source", name_key="table", allowed={"catalog", "schema", "table"})
    reference_tables = _validate_reference_tables(raw.get("reference_tables"), source)

    return {
        "es_index_name": es_index_name,
        "primary_key": primary_key,
        "view": view,
        "source": source_map,
        "reference_tables": reference_tables,
    }


def _validate_object(node: object, where: str, name_key: str, allowed: set) -> dict:
    """Validate a {catalog, schema, <name_key>} object.

    catalog and schema are name TEMPLATES (may contain ${environment}); the name/table is a plain
    identifier (no token), so an object's name is fixed and, for a view, always equals its filename.
    """
    if not isinstance(node, dict):
        raise PipelineConfigError(f"{where} must be a mapping with catalog, schema, and {name_key}, got {type(node).__name__}")
    node_unknown = sorted(set(node) - allowed)
    if node_unknown:
        raise PipelineConfigError(f"{where} has unknown key(s): {', '.join(node_unknown)}; allowed: {', '.join(sorted(allowed))}")
    return {
        "catalog": _require_name_template(node.get("catalog"), f"{where}.catalog"),
        "schema": _require_name_template(node.get("schema"), f"{where}.schema"),
        name_key: _require_identifier(node.get(name_key), f"{where}.{name_key}"),
    }


def _validate_reference_tables(node: object, source: str) -> dict:
    if node is None:
        return {}
    if not isinstance(node, dict):
        raise PipelineConfigError(f"{source}: reference_tables must be a mapping of alias -> table, got {type(node).__name__}")

    result: dict[str, dict] = {}
    for alias, spec in node.items():
        where = f"{source}: reference_tables.{alias}"
        # The alias is the ${ref_<alias>} substitution key AND the SQL join alias, so it must be a
        # bare identifier (no environment token: an alias is internal, not an object name).
        _require_identifier(alias, f"{source}: reference_tables key {alias!r}")
        if not isinstance(spec, dict):
            raise PipelineConfigError(f"{where} must be a mapping with catalog, schema, table, got {type(spec).__name__}")
        spec_unknown = sorted(set(spec) - {"catalog", "schema", "table"})
        if spec_unknown:
            raise PipelineConfigError(f"{where} has unknown key(s): {', '.join(spec_unknown)}; allowed: catalog, schema, table")
        result[alias] = {
            "catalog": _require_name_template(spec.get("catalog"), f"{where}.catalog"),
            "schema": _require_name_template(spec.get("schema"), f"{where}.schema"),
            "table": _require_identifier(spec.get("table"), f"{where}.table"),
        }
    return result


def resolve_config(cfg: dict, environment: str) -> dict:
    """Fold `environment` into every name template in a validated config, returning resolved names.

    Every catalog/schema/table/name becomes a concrete bare identifier. Raises if any resolves to an
    illegal identifier, or if a template needs an environment that was not supplied (fail closed)."""
    def obj(o: dict, name_key: str, where: str) -> dict:
        return {
            "catalog": resolve_name(o["catalog"], environment, f"{where}.catalog"),
            "schema": resolve_name(o["schema"], environment, f"{where}.schema"),
            name_key: resolve_name(o[name_key], environment, f"{where}.{name_key}"),
        }

    return {
        "es_index_name": cfg["es_index_name"],
        "primary_key": cfg["primary_key"],
        "view": obj(cfg["view"], "name", "view"),
        "source": obj(cfg["source"], "table", "source"),
        "reference_tables": {
            alias: obj(spec, "table", f"reference_tables.{alias}")
            for alias, spec in cfg["reference_tables"].items()
        },
    }


def _fqn(obj: dict, name_key: str) -> str:
    return f"{obj['catalog']}.{obj['schema']}.{obj[name_key]}"


def view_substitutions(cfg: dict, environment: str) -> dict:
    """The ${...} tokens a view .sql may reference, with ${environment} folded in.

    - view / source: the fully-qualified object, e.g. `catalog.schema.name`.
    - ref_<alias>: the aliased, fully-qualified reference table, e.g. `catalog.schema.table alias`, so
      the SQL writes `LEFT JOIN ${ref_alias} ON ...` and refers to columns via the alias.

    Join tuning (a broadcast hint, etc.) is the view author's responsibility, written directly in the
    SQL like the rest of the join -- the framework only resolves table locations.
    """
    resolved = resolve_config(cfg, environment)
    subs = {
        "view": _fqn(resolved["view"], "name"),
        "source": _fqn(resolved["source"], "table"),
    }
    for alias, spec in resolved["reference_tables"].items():
        subs[f"ref_{alias}"] = f"{_fqn(spec, 'table')} {alias}"
    return subs


def job_base_parameters(config_name: str, environment_ref: str) -> dict:
    """The values the generated per-index job passes to run_index_pipeline.py as widgets.

    The generator runs OFFLINE and cannot know `environment` (a deploy-time value), so it cannot bake
    resolved names into the job. Instead it passes the config's NAME, and the notebook loads and
    resolves that config itself at runtime. `environment_ref` is threaded through unchanged: the
    generator passes the DAB variable reference (e.g. "${var.environment}"), which the bundle resolves
    at deploy. All values are strings, as job base_parameters must be.
    """
    return {
        "config_name": config_name,
        "environment": environment_ref,
    }

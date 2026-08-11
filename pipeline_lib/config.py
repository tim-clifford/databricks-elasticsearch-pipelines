"""Load, validate, and derive substitutions from a pipeline definition config.

One pipeline_definitions/<name>.yml describes one Elasticsearch index's pipeline. This module is the
single source of truth for that schema: both the offline job generator (scripts/gen_jobs.py) and the
on-cluster notebooks (deploy_views.py, run_index_pipeline.py) import it, so validation can never drift.

Schema (see pipeline_definitions/*.yml for a commented example):

    es_index_name: <es index>            # ES index name (hyphens allowed; NOT a SQL identifier)
    es_id_field:   <column>              # view output column passed to the connector as the ES _id
    pipeline_mode: batch | streaming     # export mode for THIS index's job (required; no default)
    view:   { catalog: <c>, schema: <s>, name:  <n> }   # where the view is created, and its name
    source:                              # the one source table the view reads from
      catalog: <c>
      schema:  <s>
      table:   <t>
      primary_key: <column>              # source-table column identifying a unique row (streaming read)
    reference_tables:                    # OPTIONAL: extra tables the view joins
      <alias>:                           # key is caller-chosen; matches ${ref_<alias>} in the SQL
        catalog: <c>
        schema:  <s>
        table:   <t>

TWO DISTINCT KEYS, TWO CONTEXTS
`es_id_field` and `source.primary_key` are deliberately separate. es_id_field is a column of the
VIEW's output, handed to the ES connector as the document _id. primary_key is a column of the SOURCE
table, used by the streaming read to identify a unique row. They often share a name but need not, and
neither defaults to the other. Both are plain column identifiers (no ${environment} token).

PIPELINE MODE (per index) vs WHEEL PATH (global)
`pipeline_mode` (batch|streaming) is per-index: it lives in each config because different indices
may export differently, and the runner branches on it. It is required with no default (an
unrecognized/absent mode fails closed). The connector `wheel_path` is NOT here: it is a single
global bundle variable (one wheel serves every index), threaded to each generated job by
job_base_parameters and installed by the runner notebook.

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

# Export modes the runner supports. Allow-list: an unrecognized/absent mode is rejected (fail closed),
# never silently defaulted. Threaded to the runner notebook, which branches on it.
_VALID_PIPELINE_MODES = ("batch", "streaming")


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


def _require_pipeline_mode(value: object, where: str) -> str:
    """An export mode, restricted to the allow-list (batch|streaming). No default: absent/unknown fails."""
    if value not in _VALID_PIPELINE_MODES:
        raise PipelineConfigError(
            f"{where} must be one of {', '.join(_VALID_PIPELINE_MODES)}, got {value!r}"
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

    allowed_top = {"es_index_name", "es_id_field", "pipeline_mode", "view", "source", "reference_tables"}
    unknown = sorted(set(raw) - allowed_top)
    if unknown:
        raise PipelineConfigError(f"{source}: unknown key(s): {', '.join(unknown)}; allowed: {', '.join(sorted(allowed_top))}")

    for key in ("es_index_name", "es_id_field", "pipeline_mode", "view", "source"):
        if key not in raw:
            raise PipelineConfigError(f"{source}: missing required key '{key}'")

    es_index_name = _require_es_index(raw["es_index_name"], f"{source}: es_index_name")
    es_id_field = _require_identifier(raw["es_id_field"], f"{source}: es_id_field")
    pipeline_mode = _require_pipeline_mode(raw["pipeline_mode"], f"{source}: pipeline_mode")
    view = _validate_object(raw["view"], f"{source}: view", name_key="name", allowed={"catalog", "schema", "name"})
    # source carries primary_key in addition to catalog/schema/table: it is a column of the SOURCE
    # table (unique-row identity for the streaming read), so it lives with the source, not at top level.
    source_map = _validate_object(
        raw["source"], f"{source}: source", name_key="table",
        allowed={"catalog", "schema", "table", "primary_key"},
        extra_identifiers=("primary_key",),
    )
    reference_tables = _validate_reference_tables(raw.get("reference_tables"), source)

    return {
        "es_index_name": es_index_name,
        "es_id_field": es_id_field,
        "pipeline_mode": pipeline_mode,
        "view": view,
        "source": source_map,
        "reference_tables": reference_tables,
    }


def _validate_object(node: object, where: str, name_key: str, allowed: set, extra_identifiers: tuple = ()) -> dict:
    """Validate a {catalog, schema, <name_key>} object, plus any `extra_identifiers` columns.

    catalog and schema are name TEMPLATES (may contain ${environment}); the name/table is a plain
    identifier (no token), so an object's name is fixed and, for a view, always equals its filename.
    `extra_identifiers` are additional REQUIRED plain-identifier keys (e.g. source.primary_key): they
    are column names, not object names, so they carry no ${environment} token.
    """
    if not isinstance(node, dict):
        raise PipelineConfigError(f"{where} must be a mapping with catalog, schema, and {name_key}, got {type(node).__name__}")
    node_unknown = sorted(set(node) - allowed)
    if node_unknown:
        raise PipelineConfigError(f"{where} has unknown key(s): {', '.join(node_unknown)}; allowed: {', '.join(sorted(allowed))}")
    result = {
        "catalog": _require_name_template(node.get("catalog"), f"{where}.catalog"),
        "schema": _require_name_template(node.get("schema"), f"{where}.schema"),
        name_key: _require_identifier(node.get(name_key), f"{where}.{name_key}"),
    }
    for key in extra_identifiers:
        result[key] = _require_identifier(node.get(key), f"{where}.{key}")
    return result


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
    def obj(o: dict, name_key: str, where: str, passthrough: tuple = ()) -> dict:
        resolved = {
            "catalog": resolve_name(o["catalog"], environment, f"{where}.catalog"),
            "schema": resolve_name(o["schema"], environment, f"{where}.schema"),
            name_key: resolve_name(o[name_key], environment, f"{where}.{name_key}"),
        }
        # `passthrough` keys (e.g. source.primary_key) are plain column identifiers, not object names:
        # they carry no ${environment} token, so they are copied through unchanged, not resolved.
        for key in passthrough:
            resolved[key] = o[key]
        return resolved

    return {
        "es_index_name": cfg["es_index_name"],
        "es_id_field": cfg["es_id_field"],
        "pipeline_mode": cfg["pipeline_mode"],
        "view": obj(cfg["view"], "name", "view"),
        "source": obj(cfg["source"], "table", "source", passthrough=("primary_key",)),
        "reference_tables": {
            alias: obj(spec, "table", f"reference_tables.{alias}")
            for alias, spec in cfg["reference_tables"].items()
        },
    }


def _fqn(obj: dict, name_key: str) -> str:
    return f"{obj['catalog']}.{obj['schema']}.{obj[name_key]}"


def column_present(column: str, columns: list) -> bool:
    """Is `column` among `columns`, matching Spark's default column resolution?

    Spark/Databricks resolves column names case-INSENSITIVELY by default
    (spark.sql.caseSensitive=false), so a view emitting `DSL_ID` genuinely satisfies a config value
    of `dsl_id` and the connector resolves _id fine. A case-sensitive membership test would
    false-reject that and fail an otherwise-good deploy. deploy_views uses this to check es_id_field
    against a created view's actual output columns; it lives here so the semantics have a unit test
    (the notebook has no offline test harness of its own).
    """
    return column.lower() in {c.lower() for c in columns}


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


def job_base_parameters(config_name: str, environment_ref: str, wheel_path_ref: str) -> dict:
    """The values the generated per-index job passes to run_index_pipeline.py as widgets.

    The generator runs OFFLINE and cannot know `environment` or `wheel_path` (deploy-time values), so
    it cannot bake them into the job. Instead it passes the config's NAME (the notebook loads and
    resolves that config itself at runtime, so pipeline_mode and object names come from the config,
    not from widgets) plus two DAB variable references threaded through unchanged:
    - `environment_ref` (e.g. "${var.environment}"): folded into ${environment} in the config names.
    - `wheel_path_ref` (e.g. "${var.wheel_path}"): the ONE global connector wheel every job installs.
    The bundle resolves both at deploy. All values are strings, as job base_parameters must be.
    """
    return {
        "config_name": config_name,
        "environment": environment_ref,
        "wheel_path": wheel_path_ref,
    }

"""Load, validate, and derive substitutions from a pipeline definition config.

One _pipelines/pipeline_configs/<name>.yml describes one Elasticsearch index's pipeline. This module is
the single source of truth for that schema: both the offline job generator (scripts/gen_jobs.py) and
the on-cluster notebooks (deploy_views.py, run_index_pipeline.py) import it, so validation can never drift.

Schema (see _pipelines/pipeline_configs/*.yml for a commented example):

    es_index_name: <es index>            # ES index name (hyphens allowed; NOT a SQL identifier)
    es_id_field:   <column>              # view output column passed to the connector as the ES _id
    pipeline_mode: batch | streaming     # THIS index's DEFAULT export mode (required); a job parameter,
                                         #   so it is overridable per run with --params pipeline_mode=...
    filter_condition: <sql predicate>    # OPTIONAL default row filter (a Spark SQL boolean expr);
                                         #   also a job parameter, overridable per run. Empty => no filter.
    chunk_size: <positive int>           # OPTIONAL EsWriteConfig tuning: docs per bulk request.
    require_existing_index: true | false # OPTIONAL EsWriteConfig tuning: require the index to exist.
    verify_certs: true | false           # OPTIONAL EsWriteConfig tuning: verify the ES TLS certificate.
                                         #   All three: config DEFAULT, also a job parameter overridable
                                         #   per run. Omitted => the connector's own default stands.
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
# never silently defaulted.
_VALID_PIPELINE_MODES = ("batch", "streaming")

# Compute options for a per-index job: WHERE its notebook task runs. Allow-list, fail closed: an
# unrecognized type is rejected, never silently treated as serverless.
# - serverless (DEFAULT, also when `compute` is omitted): no cluster block => serverless notebook task.
# - existing_cluster: attach to an existing all-purpose/interactive cluster by id.
# - job_cluster: run on a job cluster whose new_cluster spec is defined once under
#   _pipelines/job_cluster_configs/<key>.yml and referenced here by that key (the generator inlines it).
# Only the generator (scripts/gen_jobs.py) acts on compute (it wires the job's cluster); the on-cluster
# notebooks ignore it, since compute affects WHERE a job runs, not what it does.
_VALID_COMPUTE_TYPES = ("serverless", "existing_cluster", "job_cluster")

# A job_cluster_config reference is a filename stem under _pipelines/job_cluster_configs/, so restrict
# it to safe filename characters (no dots or slashes) - this also blocks path traversal when the
# generator resolves the key to a file.
_VALID_JOB_CLUSTER_KEY = re.compile(r"^[A-Za-z0-9_-]+$")

# Streaming start positions (a run-time job parameter, streaming mode only). Allow-list, fail closed.
# - "new"  (DEFAULT): the stream starts at the source table's CURRENT version (readStream
#   startingVersion=latest), so nothing already in the table is exported; only commits after the
#   stream first starts are sent. This is the common case: batch mode owns the history, streaming
#   carries the ongoing delta.
# - "full": omit startingVersion, so the stream's first micro-batch(es) are the full existing table
#   snapshot, then it continues with new commits. Deterministic _id makes any overlap with a prior
#   batch export an idempotent upsert, not a duplicate.
# FIRST-RUN-ONLY: startingVersion is honored only when the stream has no checkpoint yet. Once a
# checkpoint exists it is the position of record and this knob is ignored, so flipping it on an
# already-running stream is a no-op until that stream's checkpoint is cleared.
_VALID_STREAMING_STARTS = ("new", "full")

# Read/scan parallelism is governed by spark.sql.files.maxPartitionBytes (the size of each source file
# split): smaller => more, smaller splits => the scan and the view transform fan out across more cores.
# This is the PRIMARY parallelism lever, because the scan+transform (projections, casts, broadcast
# reference joins, filter) are narrow operations that preserve the partition count all the way to the
# write - so parallelizing the read parallelizes the whole pipeline in ONE stage, with no extra shuffle.
# _DEFAULT_MAX_PARTITION_BYTES lowers Spark's own 128m default so a modest read (e.g. ~360MB/day, which
# 128m scans in only ~3 splits) spreads across the cluster instead of running on ~3 cores. Tune it to
# roughly data_size / (target partitions); see the README. "0" means "do not set it" (defer to the
# cluster/engine default). max_partition_bytes accepts a Spark byte-size string ("32m", "16m", "128m")
# or a raw byte count.
_DEFAULT_MAX_PARTITION_BYTES = "32m"
# A Spark byte-size: digits, optional unit (k/m/g/t/p, optionally with a trailing 'b'), case-insensitive.
_MAX_PARTITION_BYTES_RE = re.compile(r"^(\d+)([kmgtp]b?)?$", re.IGNORECASE)

# write_repartition explicitly repartitions the write input (a df.repartition(N) before bulk_write). It
# defaults to 0 = OFF, because maxPartitionBytes above already parallelizes the read AND the partition
# count flows through the (shuffle-free) view to the write, so an explicit repartition is normally
# redundant and would only add a shuffle. Set it > 0 (a good target is ~2-3x total worker cores) when
# the write needs parallelism the read does not supply: a view that SHUFFLES (a non-broadcast join, a
# GROUP BY/DISTINCT/window) resets the post-shuffle partition count to spark.sql.shuffle.partitions, so
# there the read parallelism does not reach the write and this knob restores it. Applied in BOTH modes
# (the whole export in batch, each micro-batch in streaming).
_DEFAULT_WRITE_REPARTITION = 0


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


def require_pipeline_mode(value: object, where: str = "pipeline_mode") -> str:
    """An export mode, restricted to the allow-list (batch|streaming). No default: absent/unknown fails.

    Public because it validates two things: the config's pipeline_mode (the per-index DEFAULT, checked
    at config load) AND the run-time job-parameter override the runner notebook receives (a bad
    `--params pipeline_mode=...` must fail closed, not silently run the wrong mode)."""
    if value not in _VALID_PIPELINE_MODES:
        raise PipelineConfigError(
            f"{where} must be one of {', '.join(_VALID_PIPELINE_MODES)}, got {value!r}"
        )
    return value


def require_streaming_start(value: object, where: str = "streaming_start") -> str:
    """The streaming start position, restricted to the allow-list (new|full). Fail closed.

    Validated by the runner notebook on the streaming_start job-parameter value (streaming mode
    only), so a bad `--params streaming_start=...` fails closed rather than silently picking a
    backfill behavior. See _VALID_STREAMING_STARTS for what each value does."""
    if value not in _VALID_STREAMING_STARTS:
        raise PipelineConfigError(
            f"{where} must be one of {', '.join(_VALID_STREAMING_STARTS)}, got {value!r}"
        )
    return value


def require_filter_condition(value: object, where: str = "filter_condition") -> str:
    """An OPTIONAL row filter: a Spark SQL boolean expression, or "" for no filter.

    Public because it validates two things (like require_pipeline_mode): the config's
    filter_condition (the per-index DEFAULT) AND the run-time job-parameter override the runner
    notebook receives. We only enforce that it is a string here; the expression itself is validated
    by Spark when the notebook applies df.filter(...), so a malformed predicate fails closed there
    (on the actual DataFrame schema) rather than being second-guessed by a partial parser here. A
    non-string (e.g. a YAML number/bool) is rejected, since df.filter expects a string expression."""
    if not isinstance(value, str):
        raise PipelineConfigError(f"{where} must be a string SQL predicate (or empty), got {value!r}")
    return value


# The EsWriteConfig tuning knobs (chunk_size, require_existing_index, verify_certs) follow the SAME
# pattern as filter_condition: an OPTIONAL config key that sets the per-index DEFAULT, AND a run-time
# job parameter overridable per run. Each validator below accepts EITHER the YAML-native value (the
# config default: a YAML int/bool) OR a string (the run-time job-parameter widget value), and returns
# the CANONICAL STRING form - "" meaning "unset, leave the connector's own default alone". A string is
# the canonical form because a job-parameter default must be a string; the string is converted back to
# the typed value (int/bool) only at the point it is splatted into EsWriteConfig, in
# write_config_overrides. Validating once, here, means config-load and run-time override apply exactly
# the same fail-closed rule (single source of truth), never two subtly different parses.


def require_chunk_size(value: object, where: str = "chunk_size") -> str:
    """OPTIONAL EsWriteConfig chunk_size (docs per bulk request): a positive integer, or "" for unset.

    Accepts a YAML int (config default) or a string (run-time override) and returns the canonical
    string form ("" when unset). A float, non-numeric string, zero/negative, or bool is rejected
    (fail closed): bool is an int subclass, so a YAML true/false must be refused explicitly rather
    than silently read as 1/0."""
    if isinstance(value, str):
        value = value.strip()
    if value is None or value == "":
        return ""
    if isinstance(value, bool):
        raise PipelineConfigError(f"{where} must be a positive integer, got {value!r}")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = int(value)  # rejects "12.5"/"1e3"/"abc" (no silent float truncation)
        except ValueError:
            raise PipelineConfigError(f"{where} must be a positive integer, got {value!r}")
    else:
        raise PipelineConfigError(f"{where} must be a positive integer, got {value!r}")
    if parsed <= 0:
        raise PipelineConfigError(f"{where} must be a positive integer, got {value!r}")
    return str(parsed)


def require_es_flag(value: object, where: str) -> str:
    """OPTIONAL EsWriteConfig boolean knob (require_existing_index / verify_certs): the canonical
    string "true"/"false", or "" for unset.

    Accepts a YAML bool (config default) or a case-insensitive 'true'/'false' string (run-time
    override). Anything else is rejected via an allow-list rather than falling to Python truthiness
    (bool('false') is True), the exact trap a bool-ish knob must avoid. bool is checked BEFORE any
    other type since it is an int subclass."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return ""
        lowered = value.lower()
        if lowered in ("true", "false"):
            return lowered
    raise PipelineConfigError(f"{where} must be 'true' or 'false', got {value!r}")


def write_config_overrides(chunk_size: object, require_existing_index: object, verify_certs: object) -> dict:
    """Convert the effective EsWriteConfig tuning values into a typed kwargs dict, fail closed.

    Called by the runner on the effective (config-default or --params override) widget values. Each
    knob's empty/unset value means "leave the connector default alone", so it is simply omitted from
    the returned dict (never coerced to a value that overrides the connector's own default). Validation
    is delegated to the shared require_chunk_size / require_es_flag helpers (the same ones the config
    schema uses), so a bad value (chunk_size=abc, verify_certs=maybe) fails closed BEFORE any write.
    The set knobs are converted from their canonical string form to the typed value EsWriteConfig
    expects (int / bool) and splatted into EsWriteConfig(**...)."""
    overrides: dict = {}

    canonical_chunk_size = require_chunk_size(chunk_size)
    if canonical_chunk_size:
        overrides["chunk_size"] = int(canonical_chunk_size)

    for name, value in (("require_existing_index", require_existing_index), ("verify_certs", verify_certs)):
        flag = require_es_flag(value, name)
        if flag:
            overrides[name] = flag == "true"

    return overrides


def require_write_repartition(value: object, where: str = "write_repartition") -> str:
    """OPTIONAL write repartition count: a NON-NEGATIVE integer, returned as a canonical string.

    Follows the same config-default-plus-run-time-override pattern as require_chunk_size (one validator
    shared by the config schema AND the runner's --params value, so both apply the identical fail-closed
    rule). It is NOT an EsWriteConfig knob: it drives a Spark `df.repartition(N)` on the write input
    BEFORE bulk_write (the whole export in batch mode, each transformed micro-batch in streaming mode).
    0 means "do not repartition" (keep the read's natural partitioning) and is the DEFAULT
    (_DEFAULT_WRITE_REPARTITION): read parallelism (max_partition_bytes) is the primary lever and its
    partition count flows through a shuffle-free view to the write, so an explicit repartition is
    normally redundant. Set it > 0 (a good target is ~2-3x total worker cores) when the write needs
    parallelism the read does not supply - e.g. a view that shuffles resets the post-shuffle partition
    count to spark.sql.shuffle.partitions. Empty/absent returns the default. Accepts a YAML int (config
    default) or a string (run-time override). A bool (int subclass), a float, a non-numeric string, or a
    negative value is rejected (fail closed)."""
    if isinstance(value, str):
        value = value.strip()
    if value is None or value == "":
        return str(_DEFAULT_WRITE_REPARTITION)
    if isinstance(value, bool):
        raise PipelineConfigError(f"{where} must be a non-negative integer (0 disables repartitioning), got {value!r}")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = int(value)  # rejects "12.5"/"1e3"/"abc" (no silent float truncation)
        except ValueError:
            raise PipelineConfigError(f"{where} must be a non-negative integer (0 disables repartitioning), got {value!r}")
    else:
        raise PipelineConfigError(f"{where} must be a non-negative integer (0 disables repartitioning), got {value!r}")
    if parsed < 0:
        raise PipelineConfigError(f"{where} must be a non-negative integer (0 disables repartitioning), got {value!r}")
    return str(parsed)


def require_max_partition_bytes(value: object, where: str = "max_partition_bytes") -> str:
    """OPTIONAL spark.sql.files.maxPartitionBytes for the source read: a Spark byte-size, canonical str.

    Governs read/scan parallelism (the size of each source file split): a smaller value yields more,
    smaller splits, so the scan and the (narrow) view transform fan out across more cores - the primary
    parallelism lever for the whole pipeline. Same shared-validator pattern as the other tuning knobs.
    Accepts a Spark byte-size string ("32m", "16m", "128m", "512mb"; unit k/m/g/t/p, optional trailing
    'b', case-insensitive) OR a raw positive byte count (int or its string). Returned canonical
    (lowercased). "0" (or 0, or "0m") is the escape hatch meaning "do NOT set it" - defer to the
    cluster/engine default; the runner skips the spark.conf.set in that case. Empty/absent returns the
    built-in default (_DEFAULT_MAX_PARTITION_BYTES). A bool (int subclass), a float, a negative value, or
    a malformed size string is rejected (fail closed)."""
    if isinstance(value, bool):
        raise PipelineConfigError(f"{where} must be a Spark byte-size like '32m' or a byte count (0 to leave unset), got {value!r}")
    if isinstance(value, str):
        value = value.strip()
    if value is None or value == "":
        return _DEFAULT_MAX_PARTITION_BYTES
    if isinstance(value, int):
        if value < 0:
            raise PipelineConfigError(f"{where} must be non-negative (0 to leave unset), got {value!r}")
        return str(value)  # raw bytes; 0 -> "0" meaning "do not set"
    if isinstance(value, str):
        m = _MAX_PARTITION_BYTES_RE.match(value)
        if not m:
            raise PipelineConfigError(
                f"{where} must be a Spark byte-size like '32m'/'16m'/'128m' or a raw byte count "
                f"(0 to leave unset), got {value!r}"
            )
        if int(m.group(1)) == 0:
            return "0"  # normalize "0"/"0m"/"0b" to the "do not set" sentinel
        return value.lower()
    raise PipelineConfigError(f"{where} must be a Spark byte-size like '32m' or a byte count (0 to leave unset), got {value!r}")


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

    allowed_top = {
        "es_index_name", "es_id_field", "pipeline_mode", "filter_condition",
        "chunk_size", "require_existing_index", "verify_certs", "write_repartition", "max_partition_bytes",
        "view", "source", "reference_tables", "compute", "schedule",
    }
    unknown = sorted(set(raw) - allowed_top)
    if unknown:
        raise PipelineConfigError(f"{source}: unknown key(s): {', '.join(unknown)}; allowed: {', '.join(sorted(allowed_top))}")

    for required in ("es_index_name", "es_id_field", "pipeline_mode", "view", "source"):
        if required not in raw:
            raise PipelineConfigError(f"{source}: missing required key '{required}'")

    es_index_name = _require_es_index(raw["es_index_name"], f"{source}: es_index_name")
    es_id_field = _require_identifier(raw["es_id_field"], f"{source}: es_id_field")
    pipeline_mode = require_pipeline_mode(raw["pipeline_mode"], f"{source}: pipeline_mode")
    # filter_condition is OPTIONAL: absent -> "" (no filter). It is a SQL predicate, not an object
    # name, so it is not an identifier and carries no ${environment} token.
    filter_condition = require_filter_condition(raw.get("filter_condition", ""), f"{source}: filter_condition")
    # The three EsWriteConfig tuning knobs are OPTIONAL: absent -> "" (leave the connector default). Each
    # is stored in its CANONICAL STRING form (the require_* helpers accept the YAML-native int/bool and
    # return a string), because it becomes a string-valued job-parameter default in job_parameters.
    chunk_size = require_chunk_size(raw.get("chunk_size", ""), f"{source}: chunk_size")
    require_existing_index = require_es_flag(raw.get("require_existing_index", ""), f"{source}: require_existing_index")
    verify_certs = require_es_flag(raw.get("verify_certs", ""), f"{source}: verify_certs")
    # write_repartition is OPTIONAL but, unlike the tuning knobs above, an absent value does NOT mean
    # "unset": it falls back to the built-in default (require_write_repartition turns "" into
    # _DEFAULT_WRITE_REPARTITION), so a config that omits it still parallelizes the write instead of
    # inheriting the read's ~few partitions. Stored in canonical string form (a job-parameter default).
    write_repartition = require_write_repartition(raw.get("write_repartition", ""), f"{source}: write_repartition")
    # max_partition_bytes is OPTIONAL: absent -> the built-in default (require_max_partition_bytes turns
    # "" into _DEFAULT_MAX_PARTITION_BYTES). It governs read/scan parallelism; "0" means leave it unset.
    max_partition_bytes = require_max_partition_bytes(raw.get("max_partition_bytes", ""), f"{source}: max_partition_bytes")
    view = _validate_object(raw["view"], f"{source}: view", name_key="name", allowed={"catalog", "schema", "name"})
    # source carries primary_key in addition to catalog/schema/table: it is a column of the SOURCE
    # table (unique-row identity for the streaming read), so it lives with the source, not at top level.
    source_map = _validate_object(
        raw["source"], f"{source}: source", name_key="table",
        allowed={"catalog", "schema", "table", "primary_key"},
        extra_identifiers=("primary_key",),
    )
    reference_tables = _validate_reference_tables(raw.get("reference_tables"), source)
    # compute is OPTIONAL: absent -> serverless (the default). It says WHERE the job runs; carries no
    # object names or ${environment} tokens, so it is validated here and passed through resolve unchanged.
    compute = _validate_compute(raw.get("compute"), f"{source}: compute")
    # schedule is OPTIONAL: absent -> None (on-demand, the default). It says WHEN the job runs (a Quartz
    # cron), carries no object names or ${environment}, so it is validated here and passed through resolve.
    schedule = _validate_schedule(raw.get("schedule"), f"{source}: schedule")

    return {
        "es_index_name": es_index_name,
        "es_id_field": es_id_field,
        "pipeline_mode": pipeline_mode,
        "filter_condition": filter_condition,
        "chunk_size": chunk_size,
        "require_existing_index": require_existing_index,
        "verify_certs": verify_certs,
        "write_repartition": write_repartition,
        "max_partition_bytes": max_partition_bytes,
        "view": view,
        "source": source_map,
        "reference_tables": reference_tables,
        "compute": compute,
        "schedule": schedule,
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
    for field_name in extra_identifiers:
        result[field_name] = _require_identifier(node.get(field_name), f"{where}.{field_name}")
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


def _validate_compute(node: object, where: str = "compute") -> dict:
    """Validate the OPTIONAL per-index `compute` block; return a normalized dict. Fail closed.

    Absent (node is None) => {"type": "serverless"}: the job runs as a serverless notebook task with no
    cluster block, the framework default. Otherwise `type` is allow-listed and each type carries exactly
    its own required key (unknown keys for that type are rejected, so a typo can't be silently ignored):
      - serverless:       no other keys.
      - existing_cluster: existing_cluster_id (non-empty string) - attach to an existing cluster by id.
      - job_cluster:      job_cluster_config (a job_cluster_configs/<key> stem) - the generator inlines
                          that reusable new_cluster spec into the job's job_clusters block.
    """
    if node is None:
        return {"type": "serverless"}
    if not isinstance(node, dict):
        raise PipelineConfigError(f"{where} must be a mapping, got {type(node).__name__}")

    ctype = node.get("type")
    if ctype not in _VALID_COMPUTE_TYPES:
        raise PipelineConfigError(
            f"{where}.type must be one of {', '.join(_VALID_COMPUTE_TYPES)}, got {ctype!r}"
        )

    # Per-type allow-list of keys: reject anything else so a misplaced/typo'd key fails closed rather
    # than being silently dropped (e.g. an existing_cluster_id under a job_cluster compute).
    allowed = {
        "serverless": {"type"},
        "existing_cluster": {"type", "existing_cluster_id"},
        "job_cluster": {"type", "job_cluster_config"},
    }[ctype]
    unknown = sorted(set(node) - allowed)
    if unknown:
        raise PipelineConfigError(
            f"{where} has unknown key(s) for type '{ctype}': {', '.join(unknown)}; "
            f"allowed: {', '.join(sorted(allowed))}"
        )

    result = {"type": ctype}
    if ctype == "existing_cluster":
        cid = node.get("existing_cluster_id")
        if not isinstance(cid, str) or not cid.strip():
            raise PipelineConfigError(
                f"{where}.existing_cluster_id is required for type 'existing_cluster' and must be a "
                f"non-empty string, got {cid!r}"
            )
        result["existing_cluster_id"] = cid.strip()
    elif ctype == "job_cluster":
        key = node.get("job_cluster_config")
        if not isinstance(key, str) or not _VALID_JOB_CLUSTER_KEY.match(key):
            raise PipelineConfigError(
                f"{where}.job_cluster_config is required for type 'job_cluster' and must name a "
                f"_pipelines/job_cluster_configs/ entry (letters, digits, '_', '-'), got {key!r}"
            )
        result["job_cluster_config"] = key
    return result


def _validate_schedule(node: object, where: str = "schedule") -> dict | None:
    """Validate the OPTIONAL per-index `schedule` block; return a normalized dict or None. Fail closed.

    Absent (node is None) => None: the job stays on-demand (no schedule block), the default. Otherwise
    it is a mapping carrying a single required `quartz_cron_expression`. The timezone is not a config
    key: the generator always emits UTC (see gen_jobs.render_job_yaml). Only the generator acts on the
    schedule; the on-cluster notebooks ignore it.

    The cron string itself is a Quartz expression validated at deploy by the bundle/Jobs API (like a
    new_cluster spec), so here we only enforce it is a non-empty string with 6 or 7 whitespace-separated
    fields. That field-count check exists to catch the common mistake of pasting a 5-field Unix cron
    (`0 8 * * *`): Quartz requires a leading seconds field (6 fields, optional 7th for year), so a
    5-field value is always wrong and fails closed here with a clear message instead of at deploy.
    """
    if node is None:
        return None
    if not isinstance(node, dict):
        raise PipelineConfigError(f"{where} must be a mapping with quartz_cron_expression, got {type(node).__name__}")

    unknown = sorted(set(node) - {"quartz_cron_expression"})
    if unknown:
        raise PipelineConfigError(
            f"{where} has unknown key(s): {', '.join(unknown)}; allowed: quartz_cron_expression"
        )

    cron = node.get("quartz_cron_expression")
    if not isinstance(cron, str) or not cron.strip():
        raise PipelineConfigError(
            f"{where}.quartz_cron_expression is required and must be a non-empty string, got {cron!r}"
        )
    fields = cron.split()
    if len(fields) not in (6, 7):
        raise PipelineConfigError(
            f"{where}.quartz_cron_expression must be a Quartz cron of 6 or 7 fields "
            f"(seconds minutes hours day-of-month month day-of-week [year]), got {len(fields)} "
            f"field(s) in {cron!r}; note Quartz needs a leading seconds field, unlike 5-field Unix cron"
        )
    return {"quartz_cron_expression": cron.strip()}


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
        # `passthrough` fields (e.g. source.primary_key) are plain column identifiers, not object
        # names: they carry no ${environment} token, so they are copied through unchanged, not resolved.
        for field_name in passthrough:
            resolved[field_name] = o[field_name]
        return resolved

    return {
        "es_index_name": cfg["es_index_name"],
        "es_id_field": cfg["es_id_field"],
        "pipeline_mode": cfg["pipeline_mode"],
        # filter_condition is a SQL predicate, not an object name: passed through verbatim (no
        # ${environment} folding), like pipeline_mode.
        "filter_condition": cfg["filter_condition"],
        # The EsWriteConfig tuning knobs are connector settings, not object names: passed through
        # verbatim (canonical string form), like filter_condition.
        "chunk_size": cfg["chunk_size"],
        "require_existing_index": cfg["require_existing_index"],
        "verify_certs": cfg["verify_certs"],
        # write_repartition (partitions for the pre-write repartition) and max_partition_bytes (read
        # scan parallelism) are run behaviors, not object names: passed through verbatim (canonical
        # string form), like the tuning knobs.
        "write_repartition": cfg["write_repartition"],
        "max_partition_bytes": cfg["max_partition_bytes"],
        "view": obj(cfg["view"], "name", "view"),
        "source": obj(cfg["source"], "table", "source", passthrough=("primary_key",)),
        "reference_tables": {
            alias: obj(spec, "table", f"reference_tables.{alias}")
            for alias, spec in cfg["reference_tables"].items()
        },
        # compute is a deploy-time job property (where the job runs), not an object name: passed
        # through verbatim, like the connector tuning knobs.
        "compute": cfg["compute"],
        # schedule (when the job runs) is likewise a deploy-time job property: passed through verbatim.
        "schedule": cfg["schedule"],
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


def view_substitutions(cfg: dict, environment: str, source_override: str | None = None) -> dict:
    """The ${...} tokens a view .sql may reference, with ${environment} folded in.

    - view / source: the fully-qualified object, e.g. `catalog.schema.name`.
    - ref_<alias>: the aliased, fully-qualified reference table, e.g. `catalog.schema.table alias`, so
      the SQL writes `LEFT JOIN ${ref_alias} ON ...` and refers to columns via the alias.

    `source_override`, when given, replaces the `${source}` value with that string (leaving `view`
    and every `ref_<alias>` untouched). This is the seam the streaming runner uses: it binds
    `${source}` to a temp view over the current micro-batch, so the SAME view SQL that deploy_views
    ran against the full source table instead runs against just the batch. The reference tables stay
    their real, fully-qualified selves in both modes, because a reference join is small-batch-to-
    dimension and we do want it against the real table. None (the default) => the real source FQN,
    which is what deploy_views uses.

    Join tuning (a broadcast hint, etc.) is the view author's responsibility, written directly in the
    SQL like the rest of the join - the framework only resolves table locations.
    """
    resolved = resolve_config(cfg, environment)
    subs = {
        "view": _fqn(resolved["view"], "name"),
        "source": source_override if source_override is not None else _fqn(resolved["source"], "table"),
    }
    for alias, spec in resolved["reference_tables"].items():
        subs[f"ref_{alias}"] = f"{_fqn(spec, 'table')} {alias}"
    return subs


# The ${token} pattern a view .sql may reference. Shared by every renderer so the substitution rule
# (which characters form a token) is defined in exactly one place.
_VIEW_TOKEN = re.compile(r"\$\{(\w+)\}")


def render_view_sql(sql: str, subs: dict, filename: str = "<view>") -> str:
    """Substitute every ${token} in a view .sql body from `subs`, fail closed on an unknown token.

    Explicit regex substitution (NOT str.format, which would choke on literal braces in SQL). An
    unknown ${token} is a hard error rather than a silently-unsubstituted string, so a typo can't
    produce SQL that points at the wrong place or references a nonexistent relation. Shared by
    deploy_views (renders CREATE OR REPLACE VIEW against the real source) and the streaming runner
    (renders the same file with ${source} bound to a micro-batch temp view), so both apply provably
    identical transform logic from one code path.
    """
    def _sub(m: "re.Match") -> str:
        token = m.group(1)
        if token not in subs:
            raise PipelineConfigError(
                f"{filename}: unknown parameter ${{{token}}}; available: {sorted(subs)}"
            )
        return subs[token]

    return _VIEW_TOKEN.sub(_sub, sql)


# The mandatory opening of every view .sql: `CREATE OR REPLACE VIEW ${view} AS <select>`. Matched
# against the RAW (pre-substitution) file, keying off the literal ${view} token, so leading comments
# are skipped and the match can't be confused by a resolved name that happens to contain SQL words.
_VIEW_DDL_PREFIX = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+VIEW\s+\$\{view\}\s+AS\b(.*)", re.IGNORECASE | re.DOTALL
)


def view_select_body(sql: str, filename: str = "<view>") -> str:
    """Return just the SELECT body of a view .sql, stripping the `CREATE OR REPLACE VIEW ${view} AS`.

    The streaming runner needs the view's SELECT (with ${source}/${ref_*} tokens still intact) so it
    can render that SELECT against a micro-batch temp view instead of creating a persistent view. The
    body is extracted from the RAW template text by matching the literal ${view} token in the
    framework's mandatory DDL prefix, so it is robust to leading comments and to whatever the resolved
    object names turn out to be. Fail closed if the file does not follow that shape - streaming can't
    reinterpret an arbitrary statement, and a silent mis-parse would export the wrong rows.
    """
    m = _VIEW_DDL_PREFIX.search(sql)
    if not m:
        raise PipelineConfigError(
            f"{filename}: expected a 'CREATE OR REPLACE VIEW ${{view}} AS <select>' statement "
            f"(required for streaming to render the view's SELECT over a micro-batch)"
        )
    body = m.group(1).strip()
    if not body:
        raise PipelineConfigError(f"{filename}: no SELECT body after 'CREATE OR REPLACE VIEW ${{view}} AS'")
    return body


def job_base_parameters(
    config_name: str,
    environment_ref: str,
    wheel_path_ref: str,
    es_host_url_ref: str,
    secret_scope_name_ref: str,
    secret_key_name_ref: str,
    checkpoint_base_path_ref: str,
) -> dict:
    """The DEPLOY-TIME values the generated per-index job passes to run_index_pipeline.py as widgets.

    These are notebook-task base_parameters: fixed at deploy, not meant to change per run. The
    generator runs OFFLINE and cannot know the deploy-time values, so it passes the config's NAME
    (the notebook loads/resolves that config at runtime, so object names come from the config) plus
    DAB variable references threaded through unchanged, all resolved by the bundle at deploy:
    - `environment_ref` (e.g. "${var.environment}"): folded into ${environment} in the config names.
    - `wheel_path_ref` (e.g. "${var.wheel_path}"): the ONE global connector wheel every job installs.
    - `es_host_url_ref`, `secret_scope_name_ref`, `secret_key_name_ref`: the global ES connection
      settings (endpoint, and the secret scope/key holding the ES api_key), shared by every index job.
    - `checkpoint_base_path_ref` (e.g. "${var.checkpoint_base_path}"): the global base path (a UC
      Volume) under which each STREAMING job keeps its checkpoint; the runner appends /<config_name>
      so each stream gets a stable, unique subfolder. Unused by batch runs.
    All values are strings, as job base_parameters must be.

    Run-time-overridable knobs (pipeline_mode, filter_condition, streaming_start, the EsWriteConfig
    tuning params) are deliberately NOT here: they are job parameters (see job_parameters),
    toggleable per run.
    """
    return {
        "config_name": config_name,
        "environment": environment_ref,
        "wheel_path": wheel_path_ref,
        "es_host_url": es_host_url_ref,
        "secret_scope_name": secret_scope_name_ref,
        "secret_key_name": secret_key_name_ref,
        "checkpoint_base_path": checkpoint_base_path_ref,
    }


def job_parameters(cfg: dict) -> list:
    """The RUN-TIME-overridable job-level parameters for a per-index job, as JobParameterDefinitions.

    Unlike base_parameters (fixed at deploy), a job parameter can be overridden per run with
    `--params <name>=<value>` and surfaces to the notebook as a widget of the same name. The runner
    re-validates each effective value, so a bad --params override fails closed.

    - pipeline_mode: DEFAULT from the config (already allow-list validated); flip batch<->streaming
      for one run without redeploying.
    - filter_condition: DEFAULT from the config ("" if the config omits it); an optional row filter
      applied before the write, overridable per run.
    - chunk_size / require_existing_index / verify_certs: EsWriteConfig tuning knobs. DEFAULT from the
      config ("" if the config omits it, meaning "use the connector's own default"); overridable per
      run. The config stores each in canonical string form (see validate_config), which is exactly the
      string a job-parameter default must be. Parsed + validated by write_config_overrides at run time
      (an unset one leaves the connector default untouched).
    - streaming_start: new|full, DEFAULT "new" (start the stream at the source's current version, so
      only new commits are exported; batch mode owns the history). Set to "full" for a one-off first
      run that backfills the whole existing table. Streaming mode only; ignored by batch. A literal
      default rather than a config key: it is a per-rollout operator choice, not a per-index property.
    - max_partition_bytes: spark.sql.files.maxPartitionBytes for the source read (read/scan
      parallelism); DEFAULT from the config, which defaults to _DEFAULT_MAX_PARTITION_BYTES when the
      config omits it. "0" leaves it unset. The primary parallelism lever; applies to both modes.
    - write_repartition: how many partitions to repartition the write input into before bulk_write; 0
      (the default) leaves the read's partitioning in place (see max_partition_bytes). DEFAULT from the
      config, which defaults to _DEFAULT_WRITE_REPARTITION. Applies to both modes.

    Returns the list shape DAB expects under a job's `parameters:` key.
    """
    return [
        {"name": "pipeline_mode", "default": cfg["pipeline_mode"]},
        {"name": "filter_condition", "default": cfg["filter_condition"]},
        {"name": "chunk_size", "default": cfg["chunk_size"]},
        {"name": "require_existing_index", "default": cfg["require_existing_index"]},
        {"name": "verify_certs", "default": cfg["verify_certs"]},
        {"name": "streaming_start", "default": "new"},
        {"name": "write_repartition", "default": cfg["write_repartition"]},
        {"name": "max_partition_bytes", "default": cfg["max_partition_bytes"]},
    ]

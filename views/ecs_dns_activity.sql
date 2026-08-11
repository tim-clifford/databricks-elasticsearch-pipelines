-- View feeding the ecs-dns-activity Elasticsearch index.
--
-- A wrapper over the source OCSF dns_activity table (every column listed explicitly rather than
-- SELECT *, so the view's output schema is an explicit, reviewable contract), left-joined to a
-- reference table to surface a validation flag. This is the slot where the OCSF -> ECS projection
-- will live later.
--
-- Parameters (${...}) are substituted by the deploy_views notebook from the pipeline definition
-- (with any environment component already folded into each object name):
--   view                           the fully-qualified view to create (catalog.schema.name)
--   source                         the fully-qualified source table (catalog.schema.table)
--   ref_<alias>                    a reference table, aliased, e.g. `catalog.schema.table alias`
-- The ref_<alias> name matches the reference_tables key in pipeline_definitions/ecs_dns_activity.yml.
-- Join tuning (a /*+ BROADCAST(alias) */ hint, etc.) is written directly here by the view author,
-- like the rest of the join.
--
-- Known limitation: the single source table plus any reference tables are what this view reads;
-- there is exactly one source table per pipeline.
CREATE OR REPLACE VIEW ${view} AS
SELECT
    base.dsl_id,
    base.time,
    base.action,
    base.action_id,
    base.activity,
    base.activity_id,
    base.activity_name,
    base.answers,
    base.app_name,
    base.category_name,
    base.category_uid,
    base.class_name,
    base.class_uid,
    base.connection_info,
    base.disposition,
    base.disposition_id,
    base.dst_endpoint,
    base.enrichments,
    base.message,
    base.metadata,
    base.observables,
    base.policy,
    base.query,
    base.osint,
    base.raw_data,
    base.rcode,
    base.rcode_id,
    base.severity,
    base.severity_id,
    base.src_endpoint,
    base.status,
    base.status_code,
    base.status_detail,
    base.status_id,
    base.timezone_offset,
    base.traffic,
    base.type_name,
    base.type_uid,
    base.unmapped,
    -- Surfaced from the reference join: does a validation row exist for this event?
    (validation.dsl_id IS NOT NULL) AS validation_row_exists
FROM ${source} base
-- dsl_id is a unique key in BOTH tables, so this join is 1:1 and cannot fan out (no duplicate
-- primary_key / ES _id). A reference join that is not 1:1 on the key would need de-duplication.
LEFT JOIN ${ref_validation} ON base.dsl_id = validation.dsl_id

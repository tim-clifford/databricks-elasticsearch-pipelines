-- View feeding the ecs-dns-activity Elasticsearch index.
--
-- Currently a passthrough wrapper over the source OCSF dns_activity table (every column listed
-- explicitly rather than SELECT *, so the view's output schema is an explicit, reviewable contract).
-- This is the slot where the OCSF -> ECS projection will live later.
--
-- Placeholders (${...}) are substituted by the deploy_views notebook from job parameters:
--   view_catalog / view_schema     where this view is created
--   source_catalog / source_schema where the source table is read from
-- Known limitation: all tables referenced by a single view must share one catalog.schema
-- (source_catalog.source_schema); a view joining tables across schemas is not supported.
CREATE OR REPLACE VIEW ${view_catalog}.${view_schema}.ecs_dns_activity AS
SELECT
    dsl_id,
    time,
    action,
    action_id,
    activity,
    activity_id,
    activity_name,
    answers,
    app_name,
    category_name,
    category_uid,
    class_name,
    class_uid,
    connection_info,
    disposition,
    disposition_id,
    dst_endpoint,
    enrichments,
    message,
    metadata,
    observables,
    policy,
    query,
    osint,
    raw_data,
    rcode,
    rcode_id,
    severity,
    severity_id,
    src_endpoint,
    status,
    status_code,
    status_detail,
    status_id,
    timezone_offset,
    traffic,
    type_name,
    type_uid,
    unmapped
FROM ${source_catalog}.${source_schema}.dns_activity

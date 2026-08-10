-- View feeding the ecs-authentication Elasticsearch index.
--
-- Currently a passthrough wrapper over the source OCSF authentication table (every column listed
-- explicitly rather than SELECT *, so the view's output schema is an explicit, reviewable contract).
-- This is the slot where the OCSF -> ECS projection will live later.
--
-- Placeholders (${...}) are substituted by the deploy_views notebook from job parameters:
--   view_catalog / view_schema     where this view is created
--   source_catalog / source_schema where the source table is read from
-- Known limitation: all tables referenced by a single view must share one catalog.schema
-- (source_catalog.source_schema); a view joining tables across schemas is not supported.
CREATE OR REPLACE VIEW ${view_catalog}.${view_schema}.ecs_authentication AS
SELECT
    dsl_id,
    time,
    action,
    action_id,
    activity,
    activity_id,
    activity_name,
    actor,
    auth_factors,
    auth_protocol,
    auth_protocol_id,
    category_name,
    category_uid,
    class_name,
    class_uid,
    cloud,
    disposition,
    disposition_id,
    dst_endpoint,
    enrichments,
    is_mfa,
    is_remote,
    logon_type,
    logon_type_id,
    message,
    metadata,
    observables,
    osint,
    raw_data,
    service,
    session,
    severity,
    severity_id,
    src_endpoint,
    status,
    status_code,
    status_detail,
    status_id,
    timezone_offset,
    type_name,
    type_uid,
    unmapped,
    user
FROM ${source_catalog}.${source_schema}.authentication

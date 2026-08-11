-- View feeding the ecs-authentication Elasticsearch index.
--
-- A passthrough wrapper over the source OCSF authentication table (every column listed explicitly
-- rather than SELECT *, so the view's output schema is an explicit, reviewable contract). This is
-- the slot where the OCSF -> ECS projection will live later. No reference-table joins here.
--
-- Parameters (${...}) are substituted by the deploy_views notebook from the pipeline definition:
--   catalog                        the shared catalog (bundle variable)
--   view_schema / view_name        where this view is created, and its name
--   source_schema / source_table   where the source table is read from
CREATE OR REPLACE VIEW ${catalog}.${view_schema}.${view_name} AS
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
FROM ${catalog}.${source_schema}.${source_table}

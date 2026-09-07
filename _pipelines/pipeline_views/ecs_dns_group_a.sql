-- View feeding the ecs-dns-group-a Elasticsearch index (job-group example, member A).
--
-- A minimal wrapper over the source OCSF dns_activity table (columns listed explicitly, not SELECT *,
-- so the output schema is an explicit contract). This member exists to demonstrate job_group merging +
-- shared-cluster compute alongside ecs_dns_group_b; the view itself is a straight passthrough.
--
-- Parameters (${...}) are substituted by the deploy_views notebook from the pipeline definition:
--   view     the fully-qualified view to create (catalog.schema.name)
--   source   the fully-qualified source table (catalog.schema.table)
CREATE OR REPLACE VIEW ${view} AS
SELECT
    base.dsl_id,
    base.time,
    base.action,
    base.activity_name,
    base.query,
    base.rcode,
    base.severity,
    base.src_endpoint,
    base.dst_endpoint
FROM ${source} base

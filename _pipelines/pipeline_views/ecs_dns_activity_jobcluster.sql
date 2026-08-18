-- View feeding the ecs-dns-activity-jobcluster Elasticsearch index (the job-cluster example pipeline).
--
-- A minimal wrapper over the source OCSF dns_activity table: columns listed explicitly (not SELECT *)
-- so the output schema is an explicit contract. No reference join here - this example exists to prove
-- out job-cluster compute + cluster policy + tags, so the view itself is kept simple.
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

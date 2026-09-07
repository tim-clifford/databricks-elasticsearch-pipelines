-- View feeding the ecs-dns-group-b Elasticsearch index (job-group example, member B).
--
-- A deliberately SMALLER projection than member A's (ecs_dns_group_a.sql) over the same OCSF
-- dns_activity source, to show that each group member carries its own view feeding its own index even
-- though the two run as tasks in one shared-cluster job.
--
-- Parameters (${...}) are substituted by the deploy_views notebook from the pipeline definition:
--   view     the fully-qualified view to create (catalog.schema.name)
--   source   the fully-qualified source table (catalog.schema.table)
CREATE OR REPLACE VIEW ${view} AS
SELECT
    base.dsl_id,
    base.time,
    base.query,
    base.rcode
FROM ${source} base

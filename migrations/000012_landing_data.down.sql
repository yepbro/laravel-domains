-- T-120: rollback of the landing data.
--
-- web_checks is a log with no references from the working tables, dropped
-- entirely; the zone index and column are removed. IF [NOT] EXISTS — a repeated
-- run against a clean schema does not fail (a full rollback after a partial one).

DROP TABLE IF EXISTS web_checks CASCADE;
DROP INDEX IF EXISTS laravel_domains_updated_at_idx;
ALTER TABLE zones DROP COLUMN IF EXISTS domains_laravel;

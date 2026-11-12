-- T-171: rollback of the monitoring window indexes.
--
-- DROP INDEX on the partitioned parents cascades to the child
-- indexes of all partitions; the data is untouched. The monitoring aggregates
-- will keep working with full seq scans (as before the migration) — the
-- indexes do not affect correctness.

DROP INDEX IF EXISTS subdomain_results_checked_idx;
DROP INDEX IF EXISTS domains_last_checked_idx;

-- T-104: rollback of the partial indexes of the scheduler's background passes.
--
-- DROP INDEX on a partitioned parent cascades to the child
-- indexes of all partitions; the data is untouched. The passes themselves
-- (lease/recheck) will keep working with a seq scan — as before the migration.

DROP INDEX IF EXISTS domains_recheck_idx;
DROP INDEX IF EXISTS domains_lease_idx;

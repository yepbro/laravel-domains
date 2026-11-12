-- T-170: rollback of the partial index of deleted domains.
--
-- DROP INDEX on a partitioned parent cascades to the child
-- indexes of all partitions; the data is untouched. The ingest cleanup
-- will keep working with a seq scan (the "from results" plan, as before the
-- migration) — correctness does not depend on the index.

DROP INDEX IF EXISTS domains_deleted_idx;

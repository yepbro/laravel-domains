-- T-004: fillfactor = 85 of the subdomain_results leaf partition.
--
-- Check history is not kept (spec 3.5): a domain recheck rewrites the
-- result rows with the same primary key. That is an UPDATE, and for it to
-- stay HOT the new row version needs room on the same page —
-- spec 6.3 explicitly classifies subdomain_results as a frequently updated
-- table (tuned autovacuum for the domains and subdomain_results partitions).
-- The value 85 — same as the domains partitions (migration 000001).
--
-- Storage parameters cannot be set on a partitioned parent (PG16),
-- so the parameter goes to the leaf partitions; dedicated zone
-- partitions (T-005) are created with fillfactor = 85 right away — the contract is pinned by the test
-- test/integration/schema_test.go (TestSchema_ResultsTablesPartitioning).
--
-- ALTER ... SET affects only future pages: on an already loaded
-- DB the rewrite is done by VACUUM FULL outside the migration (it cannot run
-- inside a transaction; there is no data in the table at this point).

ALTER TABLE subdomain_results_default SET (fillfactor = 85);

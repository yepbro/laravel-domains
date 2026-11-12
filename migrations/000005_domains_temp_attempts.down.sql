-- T-031: rollback of the temp_error retry counter.
--
-- The counter is the scheduler's retry state; the migration introduces
-- no other data: dropping the column merely stops tracking the retry
-- history, the domain rows lose no check results.
-- DROP COLUMN on a partitioned table is a catalog change,
-- with no row relocation.

ALTER TABLE domains DROP COLUMN temp_attempts;

-- T-096: rollback of the contents (pack_id, off) compaction index.
--
-- The index stores no data: dropping it returns to the migration 000001 set
-- (only the PK on hash). IF EXISTS — a repeated run against a clean
-- schema does not fail (the rollback style of 000006).

DROP INDEX IF EXISTS contents_pack_off_idx;

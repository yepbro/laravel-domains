-- T-113: rollback to the global unique key ingest_files.sha256.
--
-- Restores the 000001 uniqueness constraint (global by
-- content) and drops the service ingest_files_sha256_idx. The rollback
-- requires that the journal contain no rows with the same sha256 in different
-- (zone_id, action) — created by the T-113 contract (undo rows delete,
-- redeliveries to other zones): if such rows exist, ADD CONSTRAINT
-- will fail, the conflicting rows are resolved manually before the rollback.
-- The constraint returns in the original 000001 form (ADD CONSTRAINT …
-- UNIQUE), not as a bare index: the name and the uniqueness are the same, the pg_dump view
-- of the schema matches the pre-schema one.

DROP INDEX IF EXISTS ingest_files_sha256_idx;

DROP INDEX IF EXISTS ingest_files_zone_id_action_sha256_key;

ALTER TABLE ingest_files ADD CONSTRAINT ingest_files_sha256_key UNIQUE (sha256);

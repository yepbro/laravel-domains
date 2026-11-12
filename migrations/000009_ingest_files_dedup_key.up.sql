-- T-113: weakening the ingest file dedup to exact redelivery.
--
-- The unique key ingest_files.sha256 (global by content, 000001)
-- is replaced with (zone_id, action, sha256): a duplicate is only an exact
-- redelivery (the same zone, the same action, the same content).
-- The global key skipped two legitimate scenarios (the product
-- decision of 2026-10-18, variant D):
--
--   - an undo by the same list: a file from add/, then the same list in delete/
--     (eef80a7) — a redelivery with a different action must
--     be processed, domains get deleted;
--   - a redelivery after the world changed: the same content in a different
--     zone — the first delivery did not touch that zone's domains (the file's rows
--     must belong to the path's zone), processing is legitimate.
--
-- The harmlessness of re-applying is proven by the idempotency of the domain
-- operations (T-097: ON CONFLICT by (zone_id, label), the revive guard on
-- status = deleted, an UPDATE delete without a guard) — the file dedup is merely
-- an optimization, correctness is guaranteed at the domain level.
--
-- The replacement is safe for the data: the new key is stricter than the old one only on
-- non-empty zone_id/action (the old key's whole set plus two more columns),
-- existing rows with pairwise distinct sha256 cannot violate it.
-- zone_id/action are nullable per the §6.1 sketch (room for manual
-- inserts); the pipeline always writes them, and a NULL per the SQL standard does not conflict in a
-- unique key — for manual rows the key remains
-- advisory. The journal is one row per file, the indexes build instantly, without
-- CONCURRENTLY (the 000008 style: journal indexing is not moved to a
-- maintenance window).
--
-- The 000001 key is in constraint form (a UNIQUE column in CREATE TABLE):
-- it is dropped together with the constraint, DROP INDEX is rejected by PostgreSQL
-- (the index backs the constraint).
--
-- ingest_files_sha256_idx (sha256) — a plain index replacing the index part
-- of the dropped unique one: the pipeline checks the duplicate AFTER loading
-- staging with the query "all journal rows with this content"
-- (WHERE sha256 = $1, zone/action are filtered in code — per the
-- T-113 contract there may be several of them). The unique key (zone_id, action,
-- sha256) does not cover such a prefix (sha256 is the third column); without
-- an index the check cost would grow linearly with the journal on every file.
-- A deliberate addition to "the minimum of indexes" (spec §6) modeled on
-- contents_pack_off_idx (000007): covers the actual query, the composition
-- is pinned by the schema_test.go contract.

ALTER TABLE ingest_files DROP CONSTRAINT ingest_files_sha256_key;

CREATE UNIQUE INDEX ingest_files_zone_id_action_sha256_key
    ON ingest_files (zone_id, action, sha256);

CREATE INDEX ingest_files_sha256_idx ON ingest_files (sha256);

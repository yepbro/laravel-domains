-- T-017: the staging table for ingesting input files (spec 5.1, 3.5).
--
-- Validated rows of a file land in staging in chunks of
-- ingest.batch_size (50,000 rows by default, spec 5.1) via COPY
-- (internal/ingest.Stage → db.CopyFromStream: each chunk is a separate
-- transaction, memory is bounded by the chunk size). Applying to domains
-- (add: INSERT ... ON CONFLICT; delete: moving to deleted) — the tasks
-- T-018/T-019; after applying, the batch is removed from staging
-- (ingest.ClearBatch) and before a re-ingest it is wiped by the
-- ingest itself (idempotency, spec 3.4).
--
-- Columns:
--   batch_id — the ingestion batch id: 16 bytes (128 bits), one per
--     file ingest: the high 8 bytes are the Unix microseconds of the batch
--     creation time (big-endian), the low 8 are random bytes (crypto/rand;
--     T-098). It ties together all COPY chunks of one ingest and addresses
--     the cleanup: a deletion/rewrite touches only its own batch and does not
--     disturb parallel files with a different batch_id; the byte order
--     of the ids matches the creation chronology, so the background cleanup of orphaned
--     batches (kill -9, a DB crash) cuts the range
--     by prefix without a separate time column;
--   zone_id  — the file's zone (zones.id): all rows of a file belong to one zone
--     (spec 5.1: a row outside the file's zone is rejected by the validator, T-016);
--   label    — the domain label without the zone, punycode (spec 5.2: inside the system
--     everything is in ASCII form);
--   action   — the file operation: 1 add, 2 delete — the model.Action codes, the same
--     as ingest_files.action (spec 6.1); denormalized into every row,
--     so that applying (T-018/T-019) reads everything from staging without reference tables;
--   line_no  — the row number in the source file (one-based): the row position
--     for the per-file report and matching against the rejected log (spec 5.1).
--
-- Primary key (batch_id, line_no): the rows of a batch are addressed by their position
-- in the file; the leading batch_id serves both the batch cleanup
-- (DELETE ... WHERE batch_id = $1 — a key prefix scan) and the
-- protection against duplicates on re-insert. Duplicate labels inside a file are allowed:
-- a file row may repeat, deduplication at apply time is done by
-- ON CONFLICT (T-018).
--
-- UNLOGGED: staging is a draft living from the file ingest to the apply;
-- with no WAL writes COPY does not churn the log — minimal I/O (spec 3.5).
-- The price is the table being cleared when PostgreSQL recovers from an OS crash:
-- unapplied batches disappear, but the input file remains in incoming
-- and will be reprocessed from scratch (at-least-once delivery, spec 3.4), and
-- apply idempotency prevents duplicates.
--
-- No partitioning or extra indexes (the spec 6 principle of "a minimum
-- of indexes"): there is no domain_id in staging, partitioning per spec 6 is
-- not needed; the volume is bounded by one file's batch, and the primary key is enough
-- for the cleanup and the apply's selection.

CREATE UNLOGGED TABLE ingest_stage (
    batch_id bytea    NOT NULL,                -- the batch id (16 bytes)
    zone_id  smallint NOT NULL,                -- the file's zone (zones.id)
    label    text     NOT NULL,                -- the domain label without the zone, punycode
    action   smallint NOT NULL,                -- 1 add, 2 delete (model.Action)
    line_no  bigint   NOT NULL,                -- the row number in the file (one-based)
    PRIMARY KEY (batch_id, line_no)
);

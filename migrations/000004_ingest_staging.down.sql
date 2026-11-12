-- Rollback of T-017: the staging table for input file ingestion is dropped entirely.
--
-- No long-lived data lives in it: a batch lives from the file ingest to the
-- apply and is cleaned up (ingest.ClearBatch). Losing unapplied
-- batches is safe — files are reprocessed from scratch (spec 3.4, at-least-once).

DROP TABLE IF EXISTS ingest_stage;

-- T-295: rollback of the debt-observed marker. Losing the column loses only
-- the debt-era memory, not data: a restart degrades to the pre-T-295 behavior
-- (the T-274 position stays — the old binary does not know the column), and a
-- debt era that ends in a restart window is caught up by the fallback day, a
-- retry-list overflow or a re-analysis recheck — slower, but without loss.

ALTER TABLE retention_drain_cursor DROP COLUMN IF EXISTS debt_seen;

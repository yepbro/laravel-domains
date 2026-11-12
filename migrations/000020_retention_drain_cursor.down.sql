-- T-274: rollback of the persistent drain cursor. Losing the row is not data
-- loss: the cursor is merely a cost optimization (a restart without it makes
-- one full catch-up pass from the zero cursor — the pre-T-274 behavior),
-- retention purges the data itself idempotently (spec 3.4).
--
-- Strictly aligning the rollback with the http-worker version is not required:
-- the old binary does not know the table and does not touch it; the new binary on a DB without
-- the table starts without persistence (Info, the render debt counter
-- T-175 style) — a degradation to the T-273 behavior, not a breakage.

DROP TABLE IF EXISTS retention_drain_cursor;

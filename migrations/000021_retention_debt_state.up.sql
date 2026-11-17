-- T-295: the debt-observed marker of the retention drain — the debt_seen
-- column of retention_drain_cursor (migration 000020).
--
-- The hole of 09#P2-2 (the bulk review): debtSeen lived only in the http-worker
-- process memory. After T-274 a restart loads the saved cursor and continues from
-- it — if the render-debt era (crawler_counters['render_pending'] > 0, migration
-- 000015) ENDED in the restart window, the new process reads the counter as
-- already zero, never sees the ">0 → 0" transition and plans no catch-up circle:
-- the records filtered out by the render_pending flag during the era stay behind
-- the cursor indefinitely (the fallback-day anchor is refreshed by every
-- successful counter read, so the fallback never fires either).
--
-- The marker is written by the drain machine at the moment the era is observed
-- (retentionMarkDebtSeen — a separate short transaction) and by every batch cursor
-- upsert (the drain's own transaction, the T-274 atomicity); a forced circle plan
-- deletes the row together with the position (the circle closes the era). NULL —
-- the state is unknown (a row written by a binary before T-295): the safe reading
-- is a catch-up circle from zero, not a guess.
--
-- Nullable deliberately (no DEFAULT): ADD COLUMN with a default would stamp
-- existing rows with a false "no debt seen" — exactly the guess the column
-- exists to avoid. A full rewrite of the row by the new binary always writes an
-- explicit value, so NULL survives only until the first restart after the deploy.

ALTER TABLE retention_drain_cursor ADD COLUMN debt_seen boolean;

COMMENT ON COLUMN retention_drain_cursor.debt_seen IS
    'The render-debt era marker of the drain (T-295): true — the debt counter was observed > 0 after the last catch-up circle; NULL — unknown (a row from a pre-T-295 binary) — a restart plans a full catch-up circle';

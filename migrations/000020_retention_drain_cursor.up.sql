-- T-274: the persistent retention drain cursor — the
-- retention_drain_cursor table (one row, upsert).
--
-- The time-based drain cursor (checked_at, PK; T-185) before T-274 lived in the
-- http-worker process memory (retentionState): every service restart — a deploy,
-- an external power-cycle of the VM (the Nov 11 11:03 precedent), a potential
-- OOM kill — reset it to zero, and the first pass had to traverse
-- the "clean prefix" of the subdomain_results history again (the "something to purge"
-- predicate is outside the index — a heap visit of every row): the measurements of
-- Nov 10–11, 2026 — ~60 min when idle, >60 min under the full load; three
-- passes did not fit into the 3600 s cap (T-273), the drain continued only
-- in the quiet window (a day of incident analysis).
--
-- Storage — a separate compact state table, NOT
-- crawler_counters (T-175): the cursor is composite (a timestamp + four
-- PK fields), it does not fit into a numeric counter value. One row:
-- a surrogate PK with CHECK (id = 1) — a second row does not exist by
-- construction, the upsert is always addressed to it. One writer — the
-- RetentionLoop cycle of the http-worker (a parallel run is safe, but
-- useless — guards, like in the T-045 compaction), no conflicts.
--
-- Atomicity with the batch: the upsert runs in the TRANSACTION of the batch purge
-- (retentionPurge) — the saved position cannot run ahead of the committed
-- batch; a transaction failure leaves the previous cursor, a pass repeat
-- idempotently re-reads the already purged rows (spec 3.4). The frequency —
-- hundreds of upserts per pass (~155 batches per 16 h of draining Nov 10–11):
-- a point PK update of one row, no load; the table is not
-- on the hot front path (writing results by the http workers) at all.
-- updated_at — diagnostics of the cursor's last movement.
--
-- Forced catch-up circles (clearing the render debt, overflowing the
-- retry list, a fallback day — retentiondrain.go catchUp) purge the
-- row with a DELETE: a restart in the middle of a circle must continue the circle from zero,
-- not from the saved cursor — the records behind it are purged by the circle itself.

CREATE TABLE retention_drain_cursor (
    id           smallint    PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    checked_at   timestamptz NOT NULL,
    zone_id      smallint    NOT NULL,
    domain_id    bigint      NOT NULL,
    subdomain_id smallint    NOT NULL,
    variant      smallint    NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE retention_drain_cursor IS
    'The persistent retention drain cursor (T-274): the last visited row (checked_at, PK); one row (id = 1), upsert in the retentionPurge batch transaction, DELETE on forced catch-up circles';

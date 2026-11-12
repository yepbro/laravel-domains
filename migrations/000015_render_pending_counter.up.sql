-- T-175: the incremental render debt counter — the crawler_counters table.
--
-- The render debt (the domains with the render_pending bit in domains.flags, bit 1,
-- T-066/T-008, + the unconsumed remainder of the q:render stream) before T-175
-- was visible only via manual SQL: a count over the bitmask is a full seq scan
-- of 41.8M rows (tens of seconds of DB CPU), unacceptable for periodic
-- monitoring polling (T-171 closed the same cost class of windows with
-- indexes; an index is impossible here — see below).
--
-- Why NOT a partial index (flags & 2) <> 0 (modeled on T-104/T-170):
-- the HOT invariant of the schema (docs/schema.md "HOT updates", pinned by the
-- TestSchema_DomainsHOTColumns test) requires flags to enter no
-- index — neither the key nor the predicate. The premise "bit transitions are rare"
-- is wrong at the COLUMN level: flags changes twice per episode
-- of every domain (the dns worker sets the http_published bit — bit 2 — after
-- publishing to q:http, the result record clears it, T-108; the result record
-- sets/clears the dead_host_cutoff bit, T-164) — millions of updates
-- per day. A partial index with flags in the predicate would make EVERY
-- such update non-HOT (rewriting row versions and index entries over
-- the full tree height), not only the rare render_pending transitions.
-- Hence the counter is incremental: +1/−1 in the same transaction as
-- the render_pending bit transition (internal/httpworker/renderq.go, T-175),
-- reading — a point Lookup by PK (microseconds) once per
-- monitor.render_debt_interval.
--
-- A general-purpose counter table (name/value), not a separate
-- render_pending_count table: future counters of this class (rare transitions,
-- frequent reads) do not spawn new tables; the row name is an SQL
-- literal ('render_pending'), the value is a bigint (a domain counter).
--
-- The initial backfill: a ONE-OFF seq scan over domains (41.8M rows —
-- tens of seconds, the same cost class as one zone recount pass
-- T-078), runs once when the migration is applied on deploy;
-- afterwards the bit transitions maintain the row. Manual operations with the
-- render_pending bit must adjust the counter (docs/runbooks.md §6).

CREATE TABLE crawler_counters (
    name  text   PRIMARY KEY,
    value bigint NOT NULL
);

INSERT INTO crawler_counters (name, value)
SELECT 'render_pending', count(*)
FROM domains
WHERE (flags & 2) <> 0;

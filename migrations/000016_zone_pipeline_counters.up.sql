-- T-178: incremental pipeline state counters (the incremental-counters spec,
-- tasks/TZ_incremental_counters.md): full passes over 47M rows of domains
-- leave the read paths — the counters are maintained by deltas in the transition
-- transactions, accuracy is restored by the periodic monitor reconciliation
-- (the combined ZoneStatsOnce pass, the incremental-counters spec §7).
--
-- 1) zones.domains_pending / domains_queued — the live distribution
--    of check states across zones (the incremental-counters spec §4.1). The base set of all
--    zone columns is unified — status <> deleted (like the T-078
--    reconciliation): a deleted domain is neither work nor progress.
--    Readers: crawlerctl status / the dashboard (no domains JOIN, the incremental-counters spec §8.1),
--    BacklogLoop (a sum over the reference table, the incremental-counters spec §8.4), the dispatcher's
--    selection (the domains_pending > 0 filter — task T-183). Writers: transition
--    deltas (scheduler/ingest/httpworker, the incremental-counters spec §6) and the reconciliation.
--
-- 2) crawler_counters.updated_at — the moment of the row's last write:
--    the analyzer increments (the history cumulatives, the incremental-counters spec §6.5) and
--    the reconciliation write the name/value/moment; row age is shown by
--    crawlerctl counters list (the incremental-counters spec §9). The table was created by migration 000015
--    (T-175, render_pending): new rows (status_*, the appeared/lost
--    cumulatives) land in it too — "do not spawn new tables"
--    (the T-175 decision, confirmed by the incremental-counters spec of T-178: the pipeline_counters
--    table is not created, the single point is crawler_counters).
--
-- Population — the first reconciliation pass at monitor start (the
-- 000012/domains_laravel precedent, the incremental-counters spec §5): between the migration and the pass (seconds)
-- the readers see zeros; the crawler_counters rows are created by the reconciliation
-- with an upsert. There is no separate backfill SQL — a duplicate of the aggregate text
-- in migration/code would be a source of divergence.
--
-- Rollback (see down): (1) must be accompanied by rolling the services back to
-- the pre-T-178 versions — the transition code writes deltas into the missing columns;
-- (2) DROP COLUMN touches only the new zone columns; crawler_counters is not
-- dropped (the owner is T-175): the 000015 rollback remains separate.

ALTER TABLE zones
    ADD COLUMN domains_pending bigint NOT NULL DEFAULT 0,
    ADD COLUMN domains_queued  bigint NOT NULL DEFAULT 0;

ALTER TABLE crawler_counters
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

COMMENT ON COLUMN zones.domains_pending IS 'Live zone domains in check_state = pending (status <> deleted): transition deltas + monitor reconciliation, T-178';
COMMENT ON COLUMN zones.domains_queued IS 'Live zone domains in check_state = queued (status <> deleted): transition deltas + monitor reconciliation, T-178';
COMMENT ON COLUMN crawler_counters.updated_at IS 'Time of the last write to the counter row (increments/reconciliation, T-178)';

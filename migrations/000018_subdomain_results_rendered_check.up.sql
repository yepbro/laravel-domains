-- T-259 (the TZ-fsm spec §3 FR-5.2): the render-state invariant of a
-- subdomain_results row — the CHECK "rendered = true ⇒ render_failed_at IS NULL".
--
-- The invariant has always been kept by the writers' SQL guards (internal/renderworker:
-- renderUpdateSQL writes rendered = true and render_failed_at = NULL in one
-- statement; the renderFailMarkerSQL marker is set only with rendered =
-- false; the FSM specification — the doc comment in internal/renderworker/
-- worker.go, the TZ-fsm spec §3 FR-5, the I1 invariant): a column mismatch is not
-- produced by the code at all. The CHECK moves the invariant from procedural to
-- schema level: manual SQL, future writers and experiments with the table will
-- not be able to commit "rendered AND impossible" — a meaningless
-- combination breaking the pass selections (renderPendingRowsSQL
-- filters render_failed_at IS NULL, and the late re-publisher
-- internal/httpworker/renderlate.go — the same predicate: a rendered row
-- with a marker would fall out of both semantics at once).
--
-- NOT VALID — per the migration rules (the TZ-fsm spec §4): subdomain_results is
-- 40+ million rows, scanning the table on deploy is forbidden; a NOT VALID constraint
-- checks ONLY new rows (existing data it does not touch — there are
-- no violators among them, the guards keep the invariant).
-- VALIDATE CONSTRAINT — a separate step per the rules after the soak
-- (docs/schema.md "CHECK invariants"): on a partitioned parent
-- PG16 validates recursively into all partitions with one statement.
--
-- Partitioning (migrations 000001/000003): ADD CONSTRAINT ... NOT
-- VALID on the subdomain_results parent creates the constraint on the parent
-- and recursively on all existing partitions (verified on live
-- PostgreSQL 16: convalidated = f on all of them); the new partitions of
-- create_zone_partitions inherit it already validated; enforcement
-- for new/changed rows applies from the moment of application.
--
-- HOT: the constraint columns (rendered, render_failed_at) enter no
-- index (the minimum of indexes, spec 6) — adds no index entries,
-- does not change the HOT update picture (docs/schema.md "HOT updates").

ALTER TABLE subdomain_results
    ADD CONSTRAINT subdomain_results_rendered_check
    CHECK (rendered = false OR render_failed_at IS NULL) NOT VALID;

COMMENT ON CONSTRAINT subdomain_results_rendered_check ON subdomain_results IS
    'The render-state invariant (T-259): a rendered row carries no "render impossible" marker (render_failed_at); added NOT VALID, VALIDATE — per the docs/schema.md rules';

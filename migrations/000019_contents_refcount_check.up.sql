-- T-265 (the TZ-fsm spec §3 FR-11): the non-negativity of the content refcount —
-- the CHECK contents.refcount >= 0.
--
-- The invariant has always been kept by the writers' SQL guards (the refcount FSM
-- specification — the doc comment in internal/content/pack.go, the I1 invariant):
-- point statements release a reference only from a live row (the guard
-- predicate refcount > 0: UPDATE 0 → ErrNotFound at Delete, ErrRefIntegrity
-- at the T-044 swap), the bulk passes (the T-166 retention, the cleanup of
-- deleted domains T-020) clamp GREATEST(refcount − n, 0) — the code
-- produces no negative refcount at all. The CHECK moves the invariant
-- from procedural to schema level: manual SQL, future writers and experiments
-- will not be able to commit a negative counter. A negative refcount
-- would break the "0 = dead" semantics (Get and dedup filter
-- refcount > 0 — a "−1" would look dead) and would forever mask
-- a double reference debit: a desync would stop being observable.
--
-- NOT VALID — per the migration rules (the TZ-fsm spec §4): contents is millions
-- of rows and a constant inflow (content growth ≈20 GiB/day at ≈16 KiB per pack,
-- docs/load-measurements.md §7/§12), scanning the table on deploy
-- is forbidden; a constraint in the NOT VALID mode checks ONLY new and
-- changed rows (existing data untouched — there are no violators among
-- them, the guards kept the invariant).
-- VALIDATE CONSTRAINT — a separate step per the rules after the soak
-- (docs/schema.md "CHECK invariants"); contents is not partitioned
-- (migration 000001) — the constraint is one, validated by one statement.
--
-- HOT: refcount enters no index (the PK on hash,
-- contents_pack_off_idx on (pack_id, off), migration 000007 — point
-- counter updates stay HOT) — adds no index entries,
-- does not change the HOT update picture (docs/schema.md "HOT updates").

ALTER TABLE contents
    ADD CONSTRAINT contents_refcount_check
    CHECK (refcount >= 0) NOT VALID;

COMMENT ON CONSTRAINT contents_refcount_check ON contents IS
    'Non-negativity of the content refcount (T-265): refcount >= 0, the "0 = dead" invariant; added NOT VALID, VALIDATE — per the docs/schema.md rules';

-- T-175: rollback of the render debt counter. The table is small (one row per
-- counter), the data is not restored — the backfill on a repeated
-- up apply will recount the current flag state.
--
-- ATTENTION (the T-175 review, P3-7/P3-5): (1) the table is declared general-purpose
-- (future transition counters land in it too) — the rollback will delete their
-- rows as well; while there is one counter (render_pending) there is no loss, when new ones appear
-- the rollback must be reconsidered; (2) apply down only together with
-- rolling the http-worker back to the previous version (or after draining
-- render_pending to zero in advance): the new http-worker does ±1 of the counter on every
-- bit transition — without the table every flagging result-write
-- transaction rolls back with the "0 rows affected" error, and the
-- flag rework would not be able to clear either.

DROP TABLE IF EXISTS crawler_counters;

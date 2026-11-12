-- T-118: rollback of the appeared/lost detection history.
--
-- An append-only journal with no references from the working tables: dropping it
-- loses no pipeline data. IF EXISTS — a repeated run against a clean
-- schema does not fail (a full golang-migrate rollback after a partial one).

DROP TABLE IF EXISTS laravel_history CASCADE;

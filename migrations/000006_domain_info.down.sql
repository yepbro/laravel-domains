-- T-082: rollback of the SQL function domain_info (spec 5.11).
--
-- The function only reads the tables and changes neither the schema nor the data:
-- dropping it loses nothing. IF EXISTS — a repeated run against a clean
-- schema does not fail (a full golang-migrate --all rollback after a partial one).

DROP FUNCTION IF EXISTS domain_info(text);

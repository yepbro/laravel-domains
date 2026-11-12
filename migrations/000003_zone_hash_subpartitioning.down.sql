-- Rollback of T-005: the zone carve-out procedure is dropped.
--
-- Zone partitions created by procedure calls are not dropped: they are data,
-- not part of the procedure definition (the tables and their partitions live in
-- migrations 000001/000002). If the zone's partitions need to be returned to the DEFAULT —
-- that is a separate manual operation (DETACH the zone's partitions, move the rows
-- into the DEFAULT, ATTACH the DEFAULT).

DROP PROCEDURE IF EXISTS create_zone_partitions(smallint, integer);

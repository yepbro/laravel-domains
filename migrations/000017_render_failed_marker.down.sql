-- T-210: rollback of the "render impossible" marker.
--
-- Roll back only together with rolling the render-worker and http-worker back
-- to the pre-T-210 versions: both will stop writing/filtering the column, the stamps
-- are not restored and are not needed — re-applying up returns
-- a NULL column, finalizations will set the markers again. The mixed
-- version window is harmless: the old services do not touch the column, the new ones apply
-- the migration at start before the workers launch (migrations/embed.go).

ALTER TABLE subdomain_results DROP COLUMN render_failed_at;

-- T-259: rollback of the render-state CHECK invariant.
--
-- Roll back only together with rolling the render-worker and http-worker back
-- to the pre-T-259 versions: only the new paths will stop writing the columns, the column pair
-- is still maintained by the old writers' guards (renderUpdateSQL /
-- renderFailMarkerSQL). The mixed version window is harmless: the old services
-- do not check the constraint, the new ones apply the migration at start before
-- the workers launch (migrations/embed.go).

ALTER TABLE subdomain_results
    DROP CONSTRAINT IF EXISTS subdomain_results_rendered_check;

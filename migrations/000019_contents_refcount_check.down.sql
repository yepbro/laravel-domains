-- T-265: rollback of the refcount non-negativity CHECK.
--
-- The rollback is trivial and needs no coordination with the services: T-265 changed no
-- writers — negativity was excluded before the task by the guards (the
-- refcount > 0 predicates of the point statements, the GREATEST clamp of the bulk ones —
-- the internal/content/pack.go specification), the constraint only duplicates them
-- at the schema level. The mixed version window is harmless: the old services
-- do not check the constraint, the new ones apply the migration at start before
-- the workers launch (migrations/embed.go).
ALTER TABLE contents
    DROP CONSTRAINT IF EXISTS contents_refcount_check;

-- T-178: rollback of the incremental zone counter columns. The rollback must
-- go together with rolling the services back to the pre-T-178 versions: scheduler/ingest/
-- httpworker/dnsworker write deltas into these columns — without a column every
-- flapping transition transaction fails with an error. The crawler_counters table
-- is preserved (the owner is migration 000015/T-175); only
-- the updated_at column added by this release is dropped — the old T-175 code does not
-- read it, the loss is harmless. The values are not restored: re-applying up
-- will populate the columns with the first reconciliation pass
-- (the 000012 precedent).

ALTER TABLE zones
    DROP COLUMN domains_pending,
    DROP COLUMN domains_queued;

ALTER TABLE crawler_counters
    DROP COLUMN updated_at;

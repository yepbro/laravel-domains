-- T-120: public landing data (stage 13, moved from larasites).
--
-- 1) zones.domains_laravel — a cache of the zone's count of Laravel domains: the cards and
--    the zone cloud/table of the landing read sums over zones without aggregates over
--    laravel_domains (hundreds of millions of rows on the production corpus).
--    Updated by the same ZoneStatsLoop pass (T-078, every 30–60 s):
--    the extra aggregate SELECT zone_id, count(*) FROM laravel_domains
--    GROUP BY zone_id — a table row has already passed the analyzer threshold
--    (laravel_domains only records confident hits, §13.1),
--    a separate threshold is not needed here. NOT NULL DEFAULT 0: a zone without
--    finds — zero, not NULL (the pass's LEFT JOIN gives COALESCE).
--
-- 2) laravel_domains_updated_at_idx — the landing's "latest finds" are sorted
--    by updated_at (ORDER BY updated_at DESC LIMIT 5), the table's PK
--    is (zone_id, domain_id): without an index — sorting the whole
--    table on every view. On the partitioned parent
--    (the parent is partitioned by zone_id — the index is created on the parent, PG creates the leaves
--    on the partitions; new zone partitions inherit the index
--    automatically, the 000003 procedure touches only its own tables).
--
-- 3) web_checks — the log of manual checks from the landing form (an analog of
--    the larasites form_histories; only cmd/web writes it, T-123): who checked what,
--    when and from where — anti-spam triage and form statistics.
--    domain — as entered by the operator (before normalization), punycode_domain
--    — after idn.Normalize: the raw input is needed for typo triage,
--    punycode — for matching against pipeline data. No rotation —
--    the volume of manual checks is small; growth is controlled by the form's
--    rate limit (T-123).

ALTER TABLE zones ADD COLUMN domains_laravel bigint NOT NULL DEFAULT 0;

CREATE INDEX laravel_domains_updated_at_idx ON laravel_domains (updated_at);

CREATE TABLE web_checks (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    domain          text        NOT NULL,
    punycode_domain text        NOT NULL,
    ip_address      inet        NOT NULL,
    found           boolean     NOT NULL,
    score           real,
    duration_ms     integer,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX web_checks_ip_created_idx ON web_checks (ip_address, created_at);
CREATE INDEX web_checks_created_idx ON web_checks (created_at);

COMMENT ON COLUMN zones.domains_laravel IS 'Cached count of Laravel domains in the zone (recomputed by ZoneStatsLoop, T-120)';
COMMENT ON TABLE web_checks IS 'Log of manual checks from the landing form (written by cmd/web, T-120/T-123)';

-- Initial schema of Domain Crawler (DDL sketch from the main spec, item 6.1).
-- Partitioning by LIST (zone_id); DEFAULT partitions are created right away,
-- dedicated partitions of large zones are added later as needed.
--
-- fillfactor = 85 is set per partition separately: PostgreSQL 16 forbids
-- storage parameters on a partitioned parent (the "cannot specify
-- storage parameters for a partitioned table" error), and partition reloptions are not
-- inherited from the parent. New partitions (T-005) are created with the same
-- fillfactor — the contract is pinned by the test test/integration/schema_test.go.
-- Details: docs/schema.md.

CREATE TABLE zones (
    id              smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            text     NOT NULL UNIQUE,      -- punycode
    name_unicode    text,                          -- for display only
    is_active       boolean  NOT NULL DEFAULT true,
    domains_total   bigint   NOT NULL DEFAULT 0,
    domains_checked bigint   NOT NULL DEFAULT 0,
    updated_at      timestamptz
);

CREATE TABLE subdomains (
    id       smallint PRIMARY KEY,                 -- bit number in dns_mask (0..62)
    label    text     NOT NULL UNIQUE,             -- '' = apex
    enabled  boolean  NOT NULL DEFAULT true
);

-- Initial set of subdomains (spec 5.10).
INSERT INTO subdomains (id, label) VALUES
    (0, ''),
    (1, 'www'),
    (2, 'admin'),
    (3, 'crm'),
    (4, 'api'),
    (5, 'app');

CREATE SEQUENCE domain_id_seq;

CREATE TABLE domains (
    zone_id         smallint NOT NULL,
    id              bigint   NOT NULL DEFAULT nextval('domain_id_seq'),
    label           text     NOT NULL,             -- label without the zone, punycode
    status          smallint NOT NULL DEFAULT 0,   -- 6.2 of the main spec
    check_state     smallint NOT NULL DEFAULT 0,   -- 0 pending, 1 queued, 2 done
    flags           smallint NOT NULL DEFAULT 0,   -- bit 0: is_idn, bit 1: render_pending
    dns_mask        bigint   NOT NULL DEFAULT 0,   -- resolving subdomains
    queued_at       timestamptz,
    last_checked_at date,
    PRIMARY KEY (zone_id, id),
    UNIQUE (zone_id, label)
) PARTITION BY LIST (zone_id);

-- Partial index for the scheduler (the pending selection).
CREATE INDEX domains_pending_idx
    ON domains (zone_id, id) WHERE check_state = 0;

-- DEFAULT partition: takes all zones until dedicated partitions are carved out (T-005).
-- fillfactor = 85 — page headroom for row versions under frequent status
-- updates (spec 6, HOT updates; see the comment at the top of the file).
CREATE TABLE domains_default PARTITION OF domains DEFAULT WITH (fillfactor = 85);

CREATE TABLE subdomain_results (
    zone_id            smallint NOT NULL,
    domain_id          bigint   NOT NULL,
    subdomain_id       smallint NOT NULL,
    variant            smallint NOT NULL DEFAULT 0, -- 0 primary, 1 http (if different)
    scheme             smallint NOT NULL,           -- 1 http, 2 https
    http_status        smallint,
    final_url          text,
    alias_kind         smallint NOT NULL DEFAULT 0, -- 0 none, 1 identical, 2 redirect
    alias_subdomain_id smallint,
    addrs              bytea,                       -- packed A/AAAA
    cname              text,
    tls                jsonb,
    redirects          jsonb,
    headers            jsonb,
    cookies            jsonb,
    rendered           boolean  NOT NULL DEFAULT false,
    content_hash       bytea,                       -- 16 bytes, reference into contents
    error              smallint,
    response_ms        integer,
    checked_at         timestamptz NOT NULL,
    rules_version      integer,
    PRIMARY KEY (zone_id, domain_id, subdomain_id, variant)
) PARTITION BY LIST (zone_id);

CREATE TABLE subdomain_results_default PARTITION OF subdomain_results DEFAULT;

CREATE TABLE contents (
    hash     bytea    PRIMARY KEY,                  -- 16 bytes
    pack_id  integer  NOT NULL,
    off      bigint   NOT NULL,
    len      integer  NOT NULL,
    raw_len  integer  NOT NULL,
    refcount integer  NOT NULL DEFAULT 1
);

CREATE TABLE laravel_domains (
    zone_id        smallint NOT NULL,
    domain_id      bigint   NOT NULL,
    score          real     NOT NULL,
    ver_min        text,
    ver_max        text,
    found_on_mask  bigint   NOT NULL,               -- subdomains where Laravel was found
    rules_version  integer  NOT NULL,
    updated_at     timestamptz NOT NULL,
    PRIMARY KEY (zone_id, domain_id)
) PARTITION BY LIST (zone_id);

CREATE TABLE laravel_domains_default PARTITION OF laravel_domains DEFAULT;

CREATE TABLE laravel_findings (
    zone_id       smallint NOT NULL,
    domain_id     bigint   NOT NULL,
    subdomain_id  smallint NOT NULL,
    variant       smallint NOT NULL DEFAULT 0,
    score         real     NOT NULL,
    ver_min       text,
    ver_max       text,
    signals       jsonb    NOT NULL,                -- [{rule, weight, evidence}, …]
    is_laravel    boolean  NOT NULL,                -- score >= the config threshold
    rules_version integer  NOT NULL,                -- hash of the rule set (internal/rules, T-068)
    PRIMARY KEY (zone_id, domain_id, subdomain_id, variant)
) PARTITION BY LIST (zone_id);

CREATE TABLE laravel_findings_default PARTITION OF laravel_findings DEFAULT;

CREATE TABLE ingest_files (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    zone_id     smallint,
    action      smallint,                           -- 1 add, 2 delete
    path        text,
    sha256      bytea UNIQUE,
    lines_total bigint,
    added       bigint,
    existed     bigint,
    deleted     bigint,
    rejected    bigint,
    processed_at timestamptz
);

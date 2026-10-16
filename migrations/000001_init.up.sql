-- Начальная схема Domain Crawler (эскиз DDL из основного ТЗ, п. 6.1).
-- Партиционирование по LIST (zone_id); DEFAULT-партиции создаются сразу,
-- выделенные партиции крупных зон добавляются позже по мере надобности.
--
-- fillfactor = 85 задаётся каждой партиции отдельно: PostgreSQL 16 запрещает
-- storage parameters на партиционированном родителе (ошибка «cannot specify
-- storage parameters for a partitioned table»), и reloptions партиций от
-- родителя не наследуются. Новые партиции (T-005) создаются с тем же
-- fillfactor — контракт закреплён тестом test/integration/schema_test.go.
-- Подробности: docs/schema.md.

CREATE TABLE zones (
    id              smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            text     NOT NULL UNIQUE,      -- punycode
    name_unicode    text,                          -- только для отображения
    is_active       boolean  NOT NULL DEFAULT true,
    domains_total   bigint   NOT NULL DEFAULT 0,
    domains_checked bigint   NOT NULL DEFAULT 0,
    updated_at      timestamptz
);

CREATE TABLE subdomains (
    id       smallint PRIMARY KEY,                 -- номер бита в dns_mask (0..62)
    label    text     NOT NULL UNIQUE,             -- '' = апекс
    enabled  boolean  NOT NULL DEFAULT true
);

-- Начальный набор субдоменов (ТЗ 5.10).
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
    label           text     NOT NULL,             -- без зоны, punycode
    status          smallint NOT NULL DEFAULT 0,   -- 6.2 основного ТЗ
    check_state     smallint NOT NULL DEFAULT 0,   -- 0 pending, 1 queued, 2 done
    flags           smallint NOT NULL DEFAULT 0,   -- бит0: is_idn, бит1: render_pending
    dns_mask        bigint   NOT NULL DEFAULT 0,   -- резолвящиеся субдомены
    queued_at       timestamptz,
    last_checked_at date,
    PRIMARY KEY (zone_id, id),
    UNIQUE (zone_id, label)
) PARTITION BY LIST (zone_id);

-- Частичный индекс для планировщика (выборка pending).
CREATE INDEX domains_pending_idx
    ON domains (zone_id, id) WHERE check_state = 0;

-- DEFAULT-партиция: приём всех зон до выделения отдельных партиций (T-005).
-- fillfactor = 85 — запас на странице под версии строк при частых апдейтах
-- статусов (ТЗ 6, HOT-апдейты; см. комментарий в шапке файла).
CREATE TABLE domains_default PARTITION OF domains DEFAULT WITH (fillfactor = 85);

CREATE TABLE subdomain_results (
    zone_id            smallint NOT NULL,
    domain_id          bigint   NOT NULL,
    subdomain_id       smallint NOT NULL,
    variant            smallint NOT NULL DEFAULT 0, -- 0 основной, 1 http (если отличается)
    scheme             smallint NOT NULL,           -- 1 http, 2 https
    http_status        smallint,
    final_url          text,
    alias_kind         smallint NOT NULL DEFAULT 0, -- 0 нет, 1 идентичен, 2 редирект
    alias_subdomain_id smallint,
    addrs              bytea,                       -- упакованные A/AAAA
    cname              text,
    tls                jsonb,
    redirects          jsonb,
    headers            jsonb,
    cookies            jsonb,
    rendered           boolean  NOT NULL DEFAULT false,
    content_hash       bytea,                       -- 16 байт, ссылка на contents
    error              smallint,
    response_ms        integer,
    checked_at         timestamptz NOT NULL,
    rules_version      integer,
    PRIMARY KEY (zone_id, domain_id, subdomain_id, variant)
) PARTITION BY LIST (zone_id);

CREATE TABLE subdomain_results_default PARTITION OF subdomain_results DEFAULT;

CREATE TABLE contents (
    hash     bytea    PRIMARY KEY,                  -- 16 байт
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
    found_on_mask  bigint   NOT NULL,               -- субдомены, где найден Laravel
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
    is_laravel    boolean  NOT NULL,                -- score >= порога из конфига
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

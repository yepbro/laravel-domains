-- T-082: the SQL function domain_info(text) — a domain summary by name for
-- a direct query from psql (spec 5.11: "the SQL function domain_info(text) for
-- a direct query from the DB"; the CLI wrapper crawlerctl domain — T-081).
--
-- The response format is jsonb, not a TABLE column set: the result is composite
-- (a zone + the domain row + arrays decoding dns_mask, the per-subdomain/variant
-- results and the Laravel signs), of variable depth;
-- jsonb is read from psql as one value and gives stable field paths
-- for the tests. A missing domain or zone — NULL, not an error (spec 5.11:
-- the "understandable message" is produced by the CLI, the SQL function only answers with the fact).
--
-- Input normalization — the SQL-side minimum (spec 5.2 item 1): lower case,
-- trimming edge spaces and dots. The full IDN pipeline
-- (spaces inside, punycode, length checks) — the Go side (internal/idn):
-- in the DB names are stored punycode; unicode input is addressed by the CLI T-081,
-- which normalizes the name before calling the function. The zone is split off by matching
-- suffixes against the zones reference table (the longest matching suffix):
-- the public suffix list is unavailable in SQL, and zones is the actual
-- zone set of the catalog, covering composite zones like co.uk;
-- the behavior matches idn.Normalize (the zone by PSL, composite domain
-- labels stay in the label).
--
-- The domain lookup — by the unique key (zone_id, label), the index
-- domains_zone_id_label_key, with no full scan (spec 5.11).
--
-- Security: the function only reads — STABLE, PARALLEL SAFE,
-- SECURITY INVOKER (explicit); the body references the tables by
-- unqualified names, so search_path is pinned to the
-- function (ALTER after CREATE: the schema name is unknown during the migration —
-- the set is applied both to the production schema and to the isolated schemas
-- of the integration tests via the search_path DSN).
--
-- Response composition (jsonb keys, snake_case names matching the columns):
--   zone            {id, name, name_unicode}
--   domain          {id, label, name, status, status_name, check_state,
--                    check_state_name, is_idn, render_pending, dns_mask,
--                    temp_attempts, queued_at, last_checked_at}
--   dns_alive       [{id, label}] — dns_mask decoded against the
--                    subdomains reference table (spec 5.11)
--   results         [{subdomain_id, subdomain, variant, scheme,
--                    scheme_name, http_status, final_url, alias_kind,
--                    alias_kind_name, alias, rendered, error,
--                    content_hash, checked_at}] — by (subdomain, variant),
--                    as in spec 5.11; the content is not disclosed — only
--                    the content_hash reference (unpacking from the store — the CLI)
--   laravel         {score, ver_min, ver_max, found_on_mask, found_on,
--                    rules_version, updated_at, findings} — the aggregate
--                    of laravel_domains (json null when there are no signs);
--                    findings — all laravel_findings hits
--                    of the domain by descending score (including weak ones below
--                    the threshold marked with is_laravel, spec 5.11); signals
--                    are passed as is
-- The *_name names duplicate the value codes from spec 6.2 and internal/model
-- (Status/CheckState/Scheme/AliasKind) so that psql can read them without a reference table.

CREATE OR REPLACE FUNCTION domain_info(p_name text)
RETURNS jsonb
LANGUAGE sql
STABLE
PARALLEL SAFE
SECURITY INVOKER
AS $fn$
WITH input AS (
    -- Spec 5.2 item 1 with the SQL minimum: case, edge spaces and dots.
    SELECT NULLIF(lower(btrim(p_name, E' \t\n\r\f\v.')), '') AS name
),
labels AS (
    SELECT string_to_array(name, '.') AS arr
    FROM input
    WHERE name IS NOT NULL
),
cand AS (
    -- The zone — the longest suffix of the name present in zones
    -- (co.uk is preferred over uk); label — the remaining labels as a whole.
    -- k is the index of the first zone label; k = 2 leaves the label at least one
    -- label: a single-label name is a zone, not a domain.
    SELECT z.id                                        AS zone_id,
           z.name                                      AS zone_name,
           z.name_unicode                              AS zone_name_unicode,
           array_to_string(l.arr[1:k - 1], '.')        AS label
    FROM labels l
    CROSS JOIN LATERAL generate_series(2, array_length(l.arr, 1)) AS k
    JOIN zones z
      ON z.name = array_to_string(l.arr[k:array_length(l.arr, 1)], '.')
    ORDER BY k
    LIMIT 1
),
dom AS (
    -- The domain row by the unique key (spec 5.11); an empty dom —
    -- the function returns NULL (no result row).
    SELECT c.zone_id, c.zone_name, c.zone_name_unicode, c.label,
           d.id, d.status, d.check_state, d.flags, d.dns_mask,
           d.queued_at, d.last_checked_at, d.temp_attempts
    FROM cand c
    JOIN domains d
      ON d.zone_id = c.zone_id AND d.label = c.label
)
SELECT jsonb_build_object(
    'zone', jsonb_build_object(
        'id',           dom.zone_id,
        'name',         dom.zone_name,
        'name_unicode', dom.zone_name_unicode
    ),
    'domain', jsonb_build_object(
        'id',               dom.id,
        'label',            dom.label,
        'name',             dom.label || '.' || dom.zone_name,
        'status',           dom.status,
        'status_name',      CASE dom.status
                                WHEN 0 THEN 'new'
                                WHEN 1 THEN 'not_delegated'
                                WHEN 2 THEN 'delegated_no_address'
                                WHEN 3 THEN 'no_http'
                                WHEN 4 THEN 'alive'
                                WHEN 5 THEN 'temp_error'
                                WHEN 6 THEN 'deleted'
                            END,
        'check_state',      dom.check_state,
        'check_state_name', CASE dom.check_state
                                WHEN 0 THEN 'pending'
                                WHEN 1 THEN 'queued'
                                WHEN 2 THEN 'done'
                            END,
        'is_idn',           (dom.flags & 1) <> 0,
        'render_pending',   (dom.flags & 2) <> 0,
        'dns_mask',         dom.dns_mask,
        'temp_attempts',    dom.temp_attempts,
        'queued_at',        dom.queued_at,
        'last_checked_at',  dom.last_checked_at
    ),
    'dns_alive', COALESCE((
        SELECT jsonb_agg(jsonb_build_object('id', s.id, 'label', s.label)
                         ORDER BY s.id)
        FROM subdomains s
        WHERE (dom.dns_mask >> s.id) & 1 <> 0
    ), '[]'::jsonb),
    'results', COALESCE((
        SELECT jsonb_agg(
                   jsonb_build_object(
                       'subdomain_id',    r.subdomain_id,
                       'subdomain',       s.label,
                       'variant',         r.variant,
                       'scheme',          r.scheme,
                       'scheme_name',     CASE r.scheme
                                              WHEN 1 THEN 'http'
                                              WHEN 2 THEN 'https'
                                          END,
                       'http_status',     r.http_status,
                       'final_url',       r.final_url,
                       'alias_kind',      r.alias_kind,
                       'alias_kind_name', CASE r.alias_kind
                                              WHEN 0 THEN 'none'
                                              WHEN 1 THEN 'identical'
                                              WHEN 2 THEN 'redirect'
                                          END,
                       'alias',           s2.label,
                       'rendered',        r.rendered,
                       'error',           r.error,
                       'content_hash',    encode(r.content_hash, 'hex'),
                       'checked_at',      r.checked_at
                   )
                   ORDER BY r.subdomain_id, r.variant
               )
        FROM subdomain_results r
        JOIN subdomains s  ON s.id  = r.subdomain_id
        LEFT JOIN subdomains s2 ON s2.id = r.alias_subdomain_id
        WHERE r.zone_id = dom.zone_id AND r.domain_id = dom.id
    ), '[]'::jsonb),
    'laravel', (
        SELECT jsonb_build_object(
            'score',          ld.score,
            'ver_min',        ld.ver_min,
            'ver_max',        ld.ver_max,
            'found_on_mask',  ld.found_on_mask,
            'found_on', COALESCE((
                SELECT jsonb_agg(jsonb_build_object('id', s.id, 'label', s.label)
                                 ORDER BY s.id)
                FROM subdomains s
                WHERE (ld.found_on_mask >> s.id) & 1 <> 0
            ), '[]'::jsonb),
            'rules_version',  ld.rules_version,
            'updated_at',     ld.updated_at,
            'findings', COALESCE((
                SELECT jsonb_agg(
                           jsonb_build_object(
                               'subdomain_id', lf.subdomain_id,
                               'subdomain',    s.label,
                               'variant',      lf.variant,
                               'score',        lf.score,
                               'ver_min',      lf.ver_min,
                               'ver_max',      lf.ver_max,
                               'is_laravel',   lf.is_laravel,
                               'signals',      lf.signals
                           )
                           ORDER BY lf.score DESC, lf.subdomain_id, lf.variant
                       )
                FROM laravel_findings lf
                JOIN subdomains s ON s.id = lf.subdomain_id
                WHERE lf.zone_id = ld.zone_id AND lf.domain_id = ld.domain_id
            ), '[]'::jsonb)
        )
        FROM laravel_domains ld
        WHERE ld.zone_id = dom.zone_id AND ld.domain_id = dom.id
    )
)
FROM dom;
$fn$;

-- Pinning search_path to the function (see the security comment):
-- pg_catalog first — the function names in the body, — then the schema
-- the migration was applied to (the tables). %I escapes the name when needed.
DO $fix$
BEGIN
    EXECUTE format(
        'ALTER FUNCTION domain_info(text) SET search_path = pg_catalog, %I',
        current_schema());
END
$fix$;

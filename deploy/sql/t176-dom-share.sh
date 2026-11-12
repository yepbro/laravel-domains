#!/bin/bash
# The T-176 measurement: the share of Laravel finds owed exclusively to the
# render's DOM signals, by the mixture classes (parking .рф against the other
# zones). The methodology and the results — docs/render.md, the section
# "The DOM signal contribution by mixture classes (T-176)".
#
# STRICTLY READ-ONLY: SELECT and EXPLAIN only. No changes to the data, the
# queues or the services. Queries 1–3, 6, 7 go through laravel_findings
# (≈91k rows) with a JOIN of subdomain_results BY PK — a nested loop forced
# (enable_hashjoin/mergejoin/seqscan = off for the session): the planner's
# hash join would build a seq scan over subdomain_results (≈4.5M rows,
# 4.3 GB) — on the production DB under CPU starvation (PSI some ~90%, load
# ~25 on 4 vCPU) that is forbidden. The hints are a cost penalty, not a ban:
# the control EXPLAIN (query 0) records the actual plan in the measurement
# output. Query 4 is a sampling pass of TABLESAMPLE SYSTEM (0.5%): it reads
# ~0.5% of the table's blocks (a full count(*) over rendered — a seq scan,
# not done; a ×200 estimate with a binomial spread of ~±5%). The 0.6
# threshold in query 3 is the production value of analyzer.laravel_min_score;
# when changing the config, change the number in the script along with it.
# statement_timeout cuts off a stuck query.
#
# The measurement contents (output to stdout, psql -At):
#   0. a control EXPLAIN of the main join (the plan must be a nested loop
#      with an index scan by PK, no seq scans of the large tables);
#   1. the rule versions in laravel_findings (the measurement's uniformity);
#   2. a table by zones: findings / is_laravel / rendered — the input for
#      picking the decomposition zones (rendered > 0);
#   3. the decomposition for the zones with rendered finds: dom_only (not a
#      single header/cookie signal) and needs_dom (the noisy-OR score of the
#      H-signals below the 0.6 threshold — content signals are mandatory);
#   4. an estimate of the rendered records by zones — TABLESAMPLE SYSTEM
#      (0.5%), the counter × 200 (a full count = a seq scan, not done);
#   5. the rule frequencies by the rendered status (a validation of the H/C
#      classification);
#   6. the share of a zone's raw (rendered=false) is_laravel finds with the
#      cookie pair cookie_xsrf_token + cookie_encrypted_payload — the
#      reproducible part of the proof that "the cookies are available even
#      without a render";
#   7. freshness: rendered finds whose row has not been re-analyzed after
#      the render yet (rules_version IS NULL) — the "rendered, but
#      q:analyze not delivered" window.
#
# The PSI gates as in t167: before each query, some avg10 above the
# threshold — wait (the chronic starvation of 91–95% keeps the 95 threshold
# passable).
#
# Run on the production server (runbook §2.5):
#   ssh root@155.212.174.88 'bash -s' -- < deploy/sql/t176-dom-share.sh
# The DSN comes from CRAWLER_PG_DSN (or the standard PG* — like runbook §2.5).
set -euo pipefail

dsn="${CRAWLER_PG_DSN:-}"
psi_max="${T176_PSI_MAX:-95}"     # gates: PSI CPU some avg10, %
timeout_s="${T176_TIMEOUT:-300}"  # the statement_timeout of one query, s
pause="${T176_PAUSE:-5}"          # the pause between queries, s

PSQL=(psql -X --no-psqlrc -v ON_ERROR_STOP=1 -Atq)
[ -n "$dsn" ] && PSQL+=(-d "$dsn")

fail() { echo "ОШИБКА: $*" >&2; exit 2; }
[[ "$psi_max" =~ ^[0-9.]+$ ]] || fail "T176_PSI_MAX: ожидается число: $psi_max"
[[ "$timeout_s" =~ ^[0-9]+$ ]] || fail "T176_TIMEOUT: ожидается целое число: $timeout_s"
[[ "$pause" =~ ^[0-9]+$ ]] || fail "T176_PAUSE: ожидается целое число: $pause"
command -v psql >/dev/null 2>&1 || fail "psql не найден"
"${PSQL[@]}" -c "SELECT 1" >/dev/null || fail "нет соединения с БД (CRAWLER_PG_DSN / PG*)"

# The shared session settings: the timeout and the plan protection against
# seq scans of the large tables (the planner hints apply only to this session).
COMMON="SET statement_timeout = '${timeout_s}s';
SET enable_seqscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;"

psi_missing=0
psi_wait() {
    local f avg
    f=/proc/pressure/cpu
    if [[ ! -r $f ]]; then
        (( psi_missing )) || { echo "WARN: $f недоступен — ворота PSI пропущены" >&2; psi_missing=1; }
        return 0
    fi
    avg=$(sed -n 's/^some avg10=\([0-9.]*\).*/\1/p' "$f")
    [[ "$avg" =~ ^[0-9.]+$ ]] || return 0
    if awk -v a="$avg" -v b="$psi_max" 'BEGIN { exit !(a > b) }'; then
        echo "WAIT: PSI CPU some avg10=${avg}% > ${psi_max}% — пауза" >&2
        return 1
    fi
    return 0
}

gates() {
    local waited=0
    until psi_wait; do
        (( waited += 10 ))
        if (( waited >= 600 )); then
            echo "ОСТАНОВКА: PSI не отпускает 600 с — замер прерван" >&2
            return 1
        fi
        sleep 10
    done
}

# H-rules (the header/cookie targets of the internal/rules engine): the
# classification by target in rules/laravel.yaml —
# cookie_name/cookie_value/header_name.
H_RULES="'cookie_xsrf_token','cookie_laravel_session','cookie_cms_session','cookie_session_suffix','cookie_encrypted_payload','cookie_remember_guard','header_php_cli_server'"

echo "T-176: замер DOM-вклада (read-only); timeout=${timeout_s}s pause=${pause}s psi_max=${psi_max}%"
echo "время: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

gates
echo "=== 0. контрольный EXPLAIN основного join (план: nested loop + index scan по PK)"
"${PSQL[@]}" -c "$COMMON
EXPLAIN (COSTS OFF)
SELECT count(*) FROM laravel_findings f
JOIN subdomain_results r
  ON r.zone_id = f.zone_id AND r.domain_id = f.domain_id
 AND r.subdomain_id = f.subdomain_id AND r.variant = f.variant
WHERE f.is_laravel;"
sleep "$pause"

gates
echo "=== 1. версии правил в laravel_findings"
"${PSQL[@]}" -c "$COMMON
SELECT rules_version, count(*) FROM laravel_findings GROUP BY 1 ORDER BY 2 DESC;"
sleep "$pause"

gates
echo "=== 2. зоны: findings / is_laravel / rendered (JOIN по PK)"
"${PSQL[@]}" -c "$COMMON
SELECT z.id, z.name, count(*) AS findings_all,
       count(*) FILTER (WHERE f.is_laravel) AS is_laravel,
       count(*) FILTER (WHERE f.is_laravel AND r.rendered) AS rendered
FROM laravel_findings f
JOIN subdomain_results r
  ON r.zone_id = f.zone_id AND r.domain_id = f.domain_id
 AND r.subdomain_id = f.subdomain_id AND r.variant = f.variant
JOIN zones z ON z.id = f.zone_id
GROUP BY 1,2 ORDER BY 4 DESC;" | tee /tmp/t176_zones.$$
sleep "$pause"

# The decomposition zones — those with rendered > 0 (the 2026-10-24
# measurement: the bulk is zones 5/6, .su and .рф; the rest join as the
# render debt drains).
zones=$(awk -F'|' '$5 > 0 { print $1 }' /tmp/t176_zones.$$ | paste -sd, -)
rm -f /tmp/t176_zones.$$
[ -n "$zones" ] || { echo "rendered-находок нет — декомпозиция не нужна"; exit 0; }
echo "зоны декомпозиции (rendered > 0): $zones"
sleep "$pause"

gates
echo "=== 3. декомпозиция зон $zones: dom_only / needs_dom"
# First the rendered filter (hundreds of rows), then the jsonb parsing — an
# order of magnitude cheaper than parsing all of the zone's is_laravel finds
# under CPU starvation.
"${PSQL[@]}" -c "$COMMON
WITH rend AS (
  SELECT f.zone_id, f.domain_id, f.subdomain_id, f.variant, f.signals
  FROM laravel_findings f
  JOIN subdomain_results r
    ON r.zone_id = f.zone_id AND r.domain_id = f.domain_id
   AND r.subdomain_id = f.subdomain_id AND r.variant = f.variant
  WHERE f.is_laravel AND r.rendered AND f.zone_id IN ($zones)
), h AS (
  SELECT g.zone_id, g.domain_id, g.subdomain_id, g.variant,
         (1 - exp(sum(ln(1 - least((s->>'weight')::float8, 0.999999)))))::float4 AS h_score
  FROM rend g
  CROSS JOIN LATERAL jsonb_array_elements(g.signals) s
  WHERE s->>'rule' IN ($H_RULES)
  GROUP BY 1,2,3,4
)
SELECT z.name,
       count(*) AS rendered,
       count(*) FILTER (WHERE NOT EXISTS (
           SELECT 1 FROM jsonb_array_elements(f.signals) s
           WHERE s->>'rule' IN ($H_RULES)
       )) AS dom_only,
       count(*) FILTER (WHERE coalesce(h.h_score,0) < 0.6) AS needs_dom
FROM rend f
LEFT JOIN h ON h.zone_id = f.zone_id AND h.domain_id = f.domain_id
           AND h.subdomain_id = f.subdomain_id AND h.variant = f.variant
JOIN zones z ON z.id = f.zone_id
GROUP BY 1;"
sleep "$pause"

gates
echo "=== 4. rendered-записи subdomain_results по зонам (TABLESAMPLE 0.5%, оценка ×200)"
"${PSQL[@]}" -c "$COMMON
SELECT zone_id, rendered, count(*) AS sample_n, (count(*)*200)::bigint AS est_rows
FROM subdomain_results TABLESAMPLE SYSTEM (0.5)
GROUP BY 1,2 ORDER BY 4 DESC LIMIT 15;"
sleep "$pause"

gates
echo "=== 5. частоты правил в is_laravel-находках по rendered-статусу (все зоны)"
# The global count — the measurement's heaviest query (a jsonb parse of all
# the is_laravel finds); under the chronic starvation it takes 1–3 min.
# It tolerates a timeout — rerunning is idempotent (read-only).
"${PSQL[@]}" -c "$COMMON
SELECT s->>'rule' AS rule,
       count(*) FILTER (WHERE NOT r.rendered) AS raw_cnt,
       count(*) FILTER (WHERE r.rendered) AS rendered_cnt
FROM laravel_findings f
JOIN subdomain_results r
  ON r.zone_id = f.zone_id AND r.domain_id = f.domain_id
 AND r.subdomain_id = f.subdomain_id AND r.variant = f.variant
CROSS JOIN LATERAL jsonb_array_elements(f.signals) s
WHERE f.is_laravel
GROUP BY 1 ORDER BY 3 DESC;"
sleep "$pause"

gates
echo "=== 6. сырые (rendered=false) is_laravel-находки зон $zones с парой кук XSRF+encrypted"
"${PSQL[@]}" -c "$COMMON
SELECT z.name,
       count(*) AS raw_is_laravel,
       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(f.signals) s WHERE s->>'rule' = 'cookie_xsrf_token')
                    AND EXISTS (SELECT 1 FROM jsonb_array_elements(f.signals) s WHERE s->>'rule' = 'cookie_encrypted_payload'))
         AS with_pair
FROM laravel_findings f
JOIN subdomain_results r
  ON r.zone_id = f.zone_id AND r.domain_id = f.domain_id
 AND r.subdomain_id = f.subdomain_id AND r.variant = f.variant
JOIN zones z ON z.id = f.zone_id
WHERE f.is_laravel AND NOT r.rendered AND f.zone_id IN ($zones)
GROUP BY 1;"
sleep "$pause"

gates
echo "=== 7. свежесть: rendered-находки без переанализа после рендера (rules_version IS NULL)"
"${PSQL[@]}" -c "$COMMON
SELECT count(*) AS rendered_stale
FROM laravel_findings f
JOIN subdomain_results r
  ON r.zone_id = f.zone_id AND r.domain_id = f.domain_id
 AND r.subdomain_id = f.subdomain_id AND r.variant = f.variant
WHERE f.is_laravel AND r.rendered AND r.rules_version IS NULL;"

echo "T-176: замер завершён $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

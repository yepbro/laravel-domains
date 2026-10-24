#!/bin/bash
# Замер T-176: доля Laravel-находок, обязанных исключительно DOM-сигналам
# рендера, в разрезе классов смеси (парковочные .рф против прочих зон).
# Методика и результаты — docs/render.md, раздел «Вклад DOM-сигналов по
# классам смеси (T-176)».
#
# СТРОГО READ-ONLY: только SELECT и EXPLAIN. Никаких изменений данных,
# очередей и сервисов. Запросы 1–3, 6, 7 идут через laravel_findings
# (≈91 тыс. строк) с JOIN subdomain_results ПО PK — nested loop
# принудительно (enable_hashjoin/mergejoin/seqscan = off на сессию):
# hash join планировщика построил бы seq scan по subdomain_results
# (≈4,5 млн строк, 4,3 ГБ) — на боевой БД под CPU-голодом (PSI some
# ~90%, load ~25 на 4 vCPU) это запрещено. Хинты — стоимостный штраф,
# не запрет: контрольный EXPLAIN (запрос 0) фиксирует факт план в выводе
# замера. Запрос 4 — выборочный проход TABLESAMPLE SYSTEM (0.5%): читает
# ~0,5% блоков таблицы (полный count(*) по rendered — seq scan, не
# делаем; оценка ×200 с биномиальным разбросом ~±5%). Порог 0.6 в
# запросе 3 — боевое значение analyzer.laravel_min_score; при изменении
# конфига число в скрипте менять вместе с ним.
# statement_timeout режет застрявший запрос.
#
# Состав замера (вывод в stdout, psql -At):
#   0. контрольный EXPLAIN основного join (план обязан быть nested loop
#      с index scan по PK, без seq scan больших таблиц);
#   1. версии правил в laravel_findings (однородность замера);
#   2. таблица по зонам: findings / is_laravel / rendered — вход для
#      выбора зон декомпозиции (rendered > 0);
#   3. декомпозиция для зон с rendered-находками: dom_only (нет ни одного
#      header/cookie-сигнала) и needs_dom (noisy-OR score H-сигналов
#      ниже порога 0.6 — контентные сигналы обязательны);
#   4. оценка rendered-записей по зонам — TABLESAMPLE SYSTEM (0.5%),
#      счётчик × 200 (полный count = seq scan, не делаем);
#   5. частоты правил по rendered-статусу (валидация классификации H/C);
#   6. доля сырых (rendered=false) is_laravel-находок зоны с парой кук
#      cookie_xsrf_token + cookie_encrypted_payload — воспроизводимая
#      часть доказательства «куки доступны и без рендера»;
#   7. свежесть: rendered-находки, чья строка ещё не переанализирована
#      после рендера (rules_version IS NULL) — окно «отрендерено, но
#      q:analyze не доставлен».
#
# Ворота PSI как у t167: перед каждым запросом some avg10 выше порога —
# подождать (хронический голод 91–95% держит порог 95 проходимым).
#
# Запуск на боевом сервере (runbook §2.5):
#   ssh root@155.212.174.88 'bash -s' -- < deploy/sql/t176-dom-share.sh
# DSN берётся из CRAWLER_PG_DSN (или стандартных PG* — как runbook §2.5).
set -euo pipefail

dsn="${CRAWLER_PG_DSN:-}"
psi_max="${T176_PSI_MAX:-95}"     # ворота: PSI CPU some avg10, %
timeout_s="${T176_TIMEOUT:-300}"  # statement_timeout одного запроса, с
pause="${T176_PAUSE:-5}"          # пауза между запросами, с

PSQL=(psql -X --no-psqlrc -v ON_ERROR_STOP=1 -Atq)
[ -n "$dsn" ] && PSQL+=(-d "$dsn")

fail() { echo "ОШИБКА: $*" >&2; exit 2; }
[[ "$psi_max" =~ ^[0-9.]+$ ]] || fail "T176_PSI_MAX: ожидается число: $psi_max"
[[ "$timeout_s" =~ ^[0-9]+$ ]] || fail "T176_TIMEOUT: ожидается целое число: $timeout_s"
[[ "$pause" =~ ^[0-9]+$ ]] || fail "T176_PAUSE: ожидается целое число: $pause"
command -v psql >/dev/null 2>&1 || fail "psql не найден"
"${PSQL[@]}" -c "SELECT 1" >/dev/null || fail "нет соединения с БД (CRAWLER_PG_DSN / PG*)"

# Общие настройки сессии: таймаут и защита планов от seq scan больших
# таблиц (хинты планировщика действуют только на эту сессию).
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

# H-правила (header/cookie-цели движка internal/rules): классификация по
# target в rules/laravel.yaml — cookie_name/cookie_value/header_name.
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

# Зоны декомпозиции — те, где rendered > 0 (замер 2026-10-24: основная
# масса — зоны 5/6, .su и .рф; прочие включаются по мере дренажа долга).
zones=$(awk -F'|' '$5 > 0 { print $1 }' /tmp/t176_zones.$$ | paste -sd, -)
rm -f /tmp/t176_zones.$$
[ -n "$zones" ] || { echo "rendered-находок нет — декомпозиция не нужна"; exit 0; }
echo "зоны декомпозиции (rendered > 0): $zones"
sleep "$pause"

gates
echo "=== 3. декомпозиция зон $zones: dom_only / needs_dom"
# Сначала фильтр rendered (сотни строк), затем jsonb-разбор — на порядок
# легче разбора всех is_laravel-находок зоны под CPU-голодом.
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
# Глобальный счёт — самый тяжёлый запрос замера (jsonb-разбор всех
# is_laravel-находок); под хроническим голодом занимает 1–3 мин.
# Терпит таймаут — повторный запуск идемпотентен (read-only).
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

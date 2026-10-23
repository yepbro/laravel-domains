#!/bin/bash
# Кампания T-167, путь Б: операционный дренаж накопленного контента и
# трим jsonb не-находок батчами (runbook docs/runbooks.md §5.3).
#
# Семантика — та же, что у цикла T-166 (internal/content/retention.go),
# без изменений кода: критерии отбора (checked_at старше content.retention,
# не Laravel-находка, не render_pending либо rendered, домен не deleted),
# охраны UPDATE (повторная проверка возраста + content_hash IS NOT
# DISTINCT FROM прочитанного), GREATEST(refcount - n, 0), DELETE contents
# при refcount <= 0. Каждая пачка — отдельная транзакция BEGIN…COMMIT:
# прерывание (Ctrl-C, рестарт) в любой момент безопасно, незавершённая
# пачка откатывается целиком; повторный запуск идемпотентен (очищенные
# строки выпадают из отбора). Параллельно со штатным циклом T-166
# безопасен (охраны отсекают двойной decref), но бессмыслен — CPU-голод
# общий, выберите один путь.
#
# Отличия кампании от штатного цикла (постановка T-167): батчи уменьшены
# под CPU-голодом (T-158 не закрыт), между пачками пауза, перед пачкой —
# ворота: PSI CPU some avg10 (по умолчанию 95%) и глубины q:render/q:analyze
# (XLEN как грубая верхняя граница; точная глубина — сводка crawlerctl
# status). Ворота ждут, а не роняют скрипт.
#
# Запуск на боевом сервере (runbook §5.3): из чекаута репозитория либо
#   ssh root@155.212.174.88 'bash -s' -- < deploy/sql/t167-retention-backfill.sh
# DSN берётся из CRAWLER_PG_DSN (или стандартных PG* — как runbook §2.5),
# счётчики — в stdout, итог дублируется в journald (тег crawler-t167).
set -euo pipefail

dsn="${CRAWLER_PG_DSN:-}"
batch="${T167_BATCH:-2000}"            # пачка-транзакция (кампания: меньше дефолта цикла)
retention="${T167_RETENTION:-6 hours}" # окно жизни, как content.retention
pause="${T167_PAUSE:-5}"               # пауза между пачками, с
max_batches="${T167_MAX_BATCHES:-1000}" # страховка: 1000 x 2000 = 2 млн строк
psi_max="${T167_PSI_MAX:-95}"          # ворота: PSI CPU some avg10, %
queue_max="${T167_QUEUE_MAX:-100000}"  # ворота: XLEN q:render/q:analyze
                                      # (как scheduler.queue_high_watermark)
gate_max_wait="${T167_GATE_MAX_WAIT:-3600}" # столько ворота ждут до остановки, с

PSQL=(psql -X --no-psqlrc -v ON_ERROR_STOP=1 -Atq)
[ -n "$dsn" ] && PSQL+=(-d "$dsn")

fail() { echo "ОШИБКА: $*" >&2; exit 2; }

# Валидация подстановок: значения попадают в SQL литералами, поэтому
# алфавиты закрыты заранее (интервал — буквы/цифры/разделители, без
# кавычек; остальное — только цифры).
[[ "$retention" =~ ^[0-9A-Za-z_[:space:]:.,-]+$ ]] || fail "T167_RETENTION: недопустимые символы: $retention"
for v in "$batch" "$pause" "$max_batches" "$psi_max" "$queue_max" "$gate_max_wait"; do
    [[ "$v" =~ ^[0-9]+$ ]] || fail "ожидается целое число: $v"
done

command -v psql >/dev/null 2>&1 || fail "psql не найден"
"${PSQL[@]}" -c "SELECT '$retention'::interval + interval '0'" >/dev/null \
    || fail "T167_RETENTION не разбирается как interval: $retention"
"${PSQL[@]}" -c "SELECT 1" >/dev/null || fail "нет соединения с БД (CRAWLER_PG_DSN / PG*)"

# Один запуск: повторный параллельный экземпляр бессмыслен (идемпотентен,
# но делит CPU) — мягкая блокировка файлом.
exec 9>/tmp/crawler-t167-backfill.lock
flock -n 9 || fail "уже запущен другой экземпляр (flock /tmp/crawler-t167-backfill.lock)"

# psi_wait — ворота CPU-голода: some avg10 выше порога (замер §12 держал
# 91–95%) — подождать, пока не отпустит. /proc/pressure нет (старое ядро,
# контейнер) — предупредить один раз и не блокировать.
psi_missing=0
psi_wait() {
    local f avg
    f=/proc/pressure/cpu
    if [[ ! -r $f ]]; then
        (( psi_missing )) || { echo "WARN: $f недоступен — ворота PSI пропущены"; psi_missing=1; }
        return 0
    fi
    avg=$(sed -n 's/^some avg10=\([0-9.]*\).*/\1/p' "$f")
    [[ "$avg" =~ ^[0-9.]+$ ]] || return 0
    if awk -v a="$avg" -v b="$psi_max" 'BEGIN { exit !(a > b) }'; then
        echo "WAIT: PSI CPU some avg10=${avg}% > ${psi_max}% — пауза"
        return 1
    fi
    return 0
}

# queue_wait — ворота лагов стримов: рост q:render/q:analyze сверх порога
# значит «конвейер захлёбывается» (для q:analyze это тот же интерлок, что
# у цикла T-166). redis-cli недоступен — ворота пропущены (сводка глубин
# остаётся на операторе: crawlerctl status).
redis_missing=0
queue_wait() {
    local s n
    if ! command -v redis-cli >/dev/null 2>&1; then
        (( redis_missing )) || { echo "WARN: redis-cli не найден — ворота очередей пропущены"; redis_missing=1; }
        return 0
    fi
    for s in q:render q:analyze; do
        n=$(redis-cli --raw XLEN "$s" 2>/dev/null || echo 0)
        [[ "$n" =~ ^[0-9]+$ ]] || continue
        if (( n >= queue_max )); then
            echo "WAIT: XLEN $s = $n >= $queue_max — пауза"
            return 1
        fi
    done
    return 0
}

gates() {
    local waited=0
    until psi_wait && queue_wait; do
        (( waited += 10 ))
        if (( waited >= gate_max_wait )); then
            echo "ОСТАНОВКА: ворота не отпускают ${gate_max_wait} с — кампания прервана (итог ниже)"
            return 1
        fi
        sleep 10
    done
}

# run_batch — одна пачка-транзакция: курсорная пагинация по PK (стиль
# retentionSelectSQL), срезание ссылки и jsonb одним UPDATE, уменьшение
# refcount по агрегированным хэшам, удаление contents без ссылок. Тексты
# операторов — ручное зеркало retention.go (T-166); отличия только в
# переносе строк между командами через временные таблицы ON COMMIT DROP
# (без следов после транзакции) вместо массивов Go.
run_batch() {
    # $1..$4 — курсор (zone, domain, sub, variant)
    "${PSQL[@]}" -f - <<SQL
BEGIN;

-- Пачка кандидатов: критерии и порядок — retentionSelectSQL (T-166).
CREATE TEMP TABLE _t167_batch ON COMMIT DROP AS
SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.content_hash
FROM subdomain_results AS r
JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id
WHERE r.checked_at < now() - interval '$retention'
  AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL
       OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL)
  AND (r.rendered OR (d.flags & 2) = 0)
  AND d.status <> 6
  AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld
                  WHERE ld.zone_id = r.zone_id AND ld.domain_id = r.domain_id)
  AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant)
      > ($1::int2, $2::int8, $3::int2, $4::int2)
ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant
LIMIT $batch;

-- Срезание ссылки и jsonb одной командой (одна версия строки — один
-- HOT-апдейт); охраны гонок с перезаписью — retentionPurgeSQL (T-166).
CREATE TEMP TABLE _t167_cut ON COMMIT DROP AS
WITH purged AS (
    UPDATE subdomain_results AS r
    SET content_hash = NULL, headers = NULL, cookies = NULL,
        tls = NULL, redirects = NULL, addrs = NULL, cname = NULL
    FROM _t167_batch AS k
    WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id
      AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant
      AND r.checked_at < now() - interval '$retention'
      AND r.content_hash IS NOT DISTINCT FROM k.content_hash
    RETURNING k.content_hash AS hash
)
SELECT hash FROM purged;

-- Уменьшение refcount по фактически срезанным корректным хэшам
-- (агрегированно; GREATEST держит неотрицательным) — retentionDecrefSQL.
CREATE TEMP TABLE _t167_dec ON COMMIT DROP AS
WITH cut AS (
    SELECT hash FROM _t167_cut
    WHERE hash IS NOT NULL AND length(hash) = 16
), agg AS (
    SELECT hash, count(*)::int4 AS n FROM cut GROUP BY hash
    -- детерминированный порядок захвата строчных блокировок contents —
    -- как сортировка хэшей в retentionDecrease (T-166, стиль T-020)
    ORDER BY hash
), decref AS (
    UPDATE contents AS c
    SET refcount = GREATEST(c.refcount - a.n, 0)
    FROM agg AS a
    WHERE c.hash = a.hash
    RETURNING c.hash, c.refcount, c.len
)
SELECT hash, refcount, len FROM decref;

-- Удаление contents, лишившихся последней ссылки (охрана refcount <= 0
-- видит уже новые значения: отдельный оператор после decref). Итоги
-- пачки и курсор следующей (последний кандидат по порядку PK — как
-- RetentionOnce T-166).
WITH removed AS (
    DELETE FROM contents AS c
    WHERE c.hash IN (SELECT hash FROM _t167_dec WHERE refcount <= 0)
      AND c.refcount <= 0
    RETURNING c.len
)
SELECT
    (SELECT count(*) FROM _t167_batch)                      AS candidates,
    (SELECT count(*) FROM _t167_cut)                        AS rows_trimmed,
    (SELECT count(*) FROM _t167_cut WHERE hash IS NOT NULL) AS refs_cut,
    (SELECT count(*) FROM _t167_cut
      WHERE hash IS NOT NULL AND length(hash) <> 16)        AS bad_hashes,
    (SELECT count(*) FROM _t167_dec)                        AS decrefs,
    (SELECT count(DISTINCT hash) FROM _t167_cut
      WHERE hash IS NOT NULL AND length(hash) = 16)
      - (SELECT count(*) FROM _t167_dec)                    AS missing_refs,
    (SELECT count(*) FROM removed)                          AS contents_deleted,
    (SELECT coalesce(sum(len), 0) FROM removed)             AS freed_bytes,
    (SELECT zone_id FROM _t167_batch
       ORDER BY zone_id DESC, domain_id DESC, subdomain_id DESC, variant DESC LIMIT 1) AS next_zone,
    (SELECT domain_id FROM _t167_batch
       ORDER BY zone_id DESC, domain_id DESC, subdomain_id DESC, variant DESC LIMIT 1) AS next_domain,
    (SELECT subdomain_id FROM _t167_batch
       ORDER BY zone_id DESC, domain_id DESC, subdomain_id DESC, variant DESC LIMIT 1) AS next_sub,
    (SELECT variant FROM _t167_batch
       ORDER BY zone_id DESC, domain_id DESC, subdomain_id DESC, variant DESC LIMIT 1) AS next_variant;

COMMIT;
SQL
}

echo "T-167 путь Б: batch=$batch retention='$retention' pause=${pause}s max_batches=$max_batches"
echo "ворота: PSI some avg10 <= ${psi_max}%, XLEN q:render/q:analyze < $queue_max, ожидание ворот <= ${gate_max_wait}s"

cz=0 cd=0 cs=0 cv=0
batches=0
t_rows=0; t_refs=0; t_bad=0; t_missing=0; t_deleted=0; t_freed=0
# Код возврата: аварийные остановки (ошибка пачки, таймаут ворот, аномалии
# учёта) не должны маскироваться нулём — дренаж не завершён, оператор/скрипт
# цепочки обязан это видеть. Штатные исходы (исчерпание дренажа, потолок
# max_batches с продолжением повторным запуском) — успех.
exit_code=0
while :; do
    if ! gates; then
        exit_code=1
        break
    fi
    t0=$SECONDS
    if ! line=$(run_batch "$cz" "$cd" "$cs" "$cv" | tail -n 1); then
        echo "ОШИБКА пачки — прервано (незавершённая пачка откатилась целиком; повторный запуск идемпотентен)"
        exit_code=1
        break
    fi
    IFS='|' read -r cand trimmed refs bad dec missing deleted freed nz nd ns nv <<<"$line"
    for v in cand trimmed refs bad dec missing deleted freed; do
        [[ "${!v}" =~ ^[0-9]+$ ]] || fail "неожидаемый вывод пачки: $line"
    done
    batches=$((batches + 1))
    t_rows=$((t_rows + trimmed)); t_refs=$((t_refs + refs))
    t_bad=$((t_bad + bad)); t_missing=$((t_missing + missing))
    t_deleted=$((t_deleted + deleted)); t_freed=$((t_freed + freed))
    echo "пачка $batches: кандидатов=$cand срезано=$trimmed ссылок=$refs удалено contents=$deleted"
    echo "  (${trimmed} строк за $((SECONDS - t0)) с, курсор → ($nz,$nd,$ns,$nv))"

    # Аномалии учёта — стоп-сигнал (аналог retention.go BadHashes/Missing):
    # refcount по ним не менялся, но рост означает ошибку писателя/учёта.
    if (( bad > 0 || missing > 0 )); then
        echo "ОСТАНОВКА: аномалии учёта (bad_hashes=$bad, missing_refs=$missing) — разберитесь до продолжения (runbook §5.5)"
        exit_code=1
        break
    fi
    # Неполная пачка — дренаж исчерпан (как RetentionOnce T-166).
    if (( cand < batch )); then
        echo "дренаж исчерпан (кандидатов $cand < $batch)"
        break
    fi
    # Курсор следующей пачки — тоже числа (при cand >= batch ключ не NULL,
    # проверка — защита от неожиданного вывода, как у счётчиков выше).
    for v in nz nd ns nv; do
        [[ "${!v}" =~ ^[0-9]+$ ]] || fail "неожидаемый курсор пачки: $line"
    done
    cz=$nz; cd=$nd; cs=$ns; cv=$nv
    if (( batches >= max_batches )); then
        echo "достигнут потолок max_batches=$max_batches — продолжите повторным запуском (идемпотентно)"
        break
    fi
    sleep "$pause"
done

summary="T-167 путь Б: пачек=$batches строк=$t_rows ссылок=$t_refs contents=$t_deleted освобождено байт=$t_freed аномалии: bad=$t_bad missing=$t_missing"
echo "$summary"
command -v logger >/dev/null 2>&1 && logger -t crawler-t167 -- "$summary" || true
exit "$exit_code"

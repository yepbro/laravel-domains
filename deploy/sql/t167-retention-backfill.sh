#!/bin/bash
# The T-167 campaign, path B: operational drainage of the accumulated
# content and trimming the jsonb of non-finds in batches (runbook
# docs/runbooks.md §5.3).
#
# The semantics are the same as the T-166 loop (internal/content/retention.go),
# with no code changes: the selection criteria (checked_at older than
# content.retention, not a Laravel find, not render_pending or rendered, the
# domain not deleted), the UPDATE guards (an age re-check + content_hash IS
# NOT DISTINCT FROM the value read), GREATEST(refcount - n, 0), DELETE of
# contents at refcount <= 0. Each chunk is a separate BEGIN…COMMIT
# transaction: an interruption (Ctrl-C, a restart) is safe at any moment, an
# incomplete chunk is rolled back in full; rerunning is idempotent (the
# trimmed rows drop out of the selection). Running in parallel with the
# regular T-166 loop is safe (the guards cut off a double decref) but
# pointless — the CPU starvation is shared; pick one path.
#
# Differences of the campaign from the regular loop (the T-167 assignment):
# the batches are reduced under CPU starvation (T-158 not closed), a pause
# between chunks, and gates before a chunk: PSI CPU some avg10 (95% by
# default), the q:render/q:analyze depths on two levels — a coarse XLEN (an
# upper bound, 100k by default) and the exact lag of the consumer groups
# (XINFO GROUPS → lag, Redis 7+; the undelivered tail — it is exactly what
# keeps the records from being cleaned by the T-166 loop, the q:render/
# q:analyze window fixation). The gates wait instead of crashing the script.
#
# With q:render jammed (the lag far above the threshold — like 23.10,
# ≈99k) the lag gates will not let through: the script stops honestly on the
# gate timeout — the stream drain (T-159) comes first; cleaning the records
# waiting for render is not allowed.
#
# Run on the production server (runbook §5.3): from a repository checkout or
#   ssh root@155.212.174.88 'bash -s' -- < deploy/sql/t167-retention-backfill.sh
# The DSN comes from CRAWLER_PG_DSN (or the standard PG* — like runbook §2.5);
# the counters go to stdout, the summary is duplicated to journald (the
# crawler-t167 tag).
set -euo pipefail

dsn="${CRAWLER_PG_DSN:-}"
batch="${T167_BATCH:-2000}"            # a chunk-transaction (campaign: smaller than the loop default)
retention="${T167_RETENTION:-6 hours}" # the lifetime window, like content.retention
pause="${T167_PAUSE:-5}"               # the pause between chunks, s
max_batches="${T167_MAX_BATCHES:-1000}" # a guard: 1000 x 2000 = 2M rows
psi_max="${T167_PSI_MAX:-95}"          # gates: PSI CPU some avg10, %
queue_max="${T167_QUEUE_MAX:-100000}"  # gates: XLEN q:render/q:analyze
                                      # (a coarse upper bound of the depth)
render_lag_max="${T167_RENDER_LAG:-1000}"   # gates: the q:render group lag
analyze_lag_max="${T167_ANALYZE_LAG:-1000}" # gates: the q:analyze group lag
gate_max_wait="${T167_GATE_MAX_WAIT:-3600}" # how long the gates wait before stopping, s

PSQL=(psql -X --no-psqlrc -v ON_ERROR_STOP=1 -Atq)
[ -n "$dsn" ] && PSQL+=(-d "$dsn")

fail() { echo "ОШИБКА: $*" >&2; exit 2; }

# Validation of the substitutions: the values reach the SQL as literals, so
# the alphabets are closed in advance (the interval — letters/digits/
# separators, no quotes; everything else — digits only).
[[ "$retention" =~ ^[0-9A-Za-z_[:space:]:.,-]+$ ]] || fail "T167_RETENTION: недопустимые символы: $retention"
for v in "$batch" "$pause" "$max_batches" "$psi_max" "$queue_max" "$render_lag_max" "$analyze_lag_max" "$gate_max_wait"; do
    [[ "$v" =~ ^[0-9]+$ ]] || fail "ожидается целое число: $v"
done

command -v psql >/dev/null 2>&1 || fail "psql не найден"
"${PSQL[@]}" -c "SELECT '$retention'::interval + interval '0'" >/dev/null \
    || fail "T167_RETENTION не разбирается как interval: $retention"
"${PSQL[@]}" -c "SELECT 1" >/dev/null || fail "нет соединения с БД (CRAWLER_PG_DSN / PG*)"

# One instance: a second parallel instance is pointless (idempotent, but it
# shares the CPU) — a soft file lock.
exec 9>/tmp/crawler-t167-backfill.lock
flock -n 9 || fail "уже запущен другой экземпляр (flock /tmp/crawler-t167-backfill.lock)"

# psi_wait — the CPU-starvation gates: some avg10 above the threshold (the
# §12 measurement held 91–95%) — wait until it releases. No /proc/pressure
# (an old kernel, a container) — warn once and do not block.
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

# lag_of — the stream lag: the maximum across the consumer groups
# (XINFO GROUPS → the lag field, Redis 7+; in production a working stream has
# one group). lag = nil (the trim cut off the undelivered part, or Redis < 7)
# — the lag is unavailable, the answer is "unknown": exact gates on it are
# impossible, XLEN remains. No groups on the stream — everything is
# undelivered: the lag = XLEN (the same semantics as the depth of the T-026
# gate — BackpressureGate.Len).
lag_of() {
    local out
    out=$(redis-cli --raw XINFO GROUPS "$1" 2>/dev/null) || return 1
    if [[ -z "$out" ]]; then
        redis-cli --raw XLEN "$1" 2>/dev/null || echo 0
        return
    fi
    # m == "" — the lag has not yet been seen as a number (the initialization
    # is a separate check: in awk an uninitialized m in the comparison
    # $2+0 > m equals zero, and a lag of 0 — a healthy queue — would not have
    # been assigned).
    paste -d' ' - - <<<"$out" \
        | awk '$1 == "lag" && $2 ~ /^[0-9]+$/ { if (m == "" || $2+0 > m) m = $2+0 }
               END { print (m == "" ? "unknown" : m) }'
}

# queue_wait — the stream gates, two levels: XLEN (a coarse upper bound of
# the depth) and the group lag (the exact undelivered tail — it is exactly
# what keeps the records from being cleaned by the T-166 loop: the q:render/
# q:analyze window fixation). The default lag thresholds of 1000: the
# q:analyze inflow is ≈35 msg/s (the §12 measurement, 3M/day) — 1000 is about
# half a minute, "the consumers almost keep up"; deeper — a campaign is out
# of place, let them triage the tail first. No redis-cli — the gates are
# skipped (the depth summary stays with the operator: crawlerctl status).
redis_missing=0
lag_missing=0
queue_wait() {
    local s n lag max
    if ! command -v redis-cli >/dev/null 2>&1; then
        (( redis_missing )) || { echo "WARN: redis-cli не найден — ворота очередей пропущены"; redis_missing=1; }
        return 0
    fi
    for s in q:render q:analyze; do
        n=$(redis-cli --raw XLEN "$s" 2>/dev/null || echo 0)
        if [[ "$n" =~ ^[0-9]+$ ]] && (( n >= queue_max )); then
            echo "WAIT: XLEN $s = $n >= $queue_max — пауза"
            return 1
        fi
        max=$render_lag_max
        [[ "$s" == q:analyze ]] && max=$analyze_lag_max
        lag=$(lag_of "$s") || continue
        if [[ "$lag" == unknown ]]; then
            (( lag_missing )) || { echo "WARN: лаг $s недоступен (nil либо Redis < 7) — лаг-ворота пропущены, действует XLEN"; lag_missing=1; }
            continue
        fi
        [[ "$lag" =~ ^[0-9]+$ ]] || continue
        if (( lag >= max )); then
            echo "WAIT: лаг $s = $lag >= $max — пауза (записи, ждущие потребителя, не трогаем)"
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

# run_batch — one chunk-transaction: cursor pagination by PK (the
# retentionSelectSQL style), cutting the reference and the jsonb with one
# UPDATE, decreasing the refcount over the aggregated hashes, deleting
# contents without references. The statement texts are a manual mirror of
# retention.go (T-166); the only differences are splitting the work between
# statements via ON COMMIT DROP temp tables (no traces after the transaction)
# instead of Go arrays.
run_batch() {
    # $1..$4 — the cursor (zone, domain, sub, variant)
    "${PSQL[@]}" -f - <<SQL
BEGIN;

-- The candidate chunk: the criteria and the order — retentionSelectSQL (T-166).
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

-- Cutting the reference and the jsonb with one statement (one row version —
-- one HOT update); the overwrite race guards — retentionPurgeSQL (T-166).
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

-- Decreasing the refcount over the actually cut, valid hashes
-- (aggregated; GREATEST keeps it non-negative) — retentionDecrefSQL.
CREATE TEMP TABLE _t167_dec ON COMMIT DROP AS
WITH cut AS (
    SELECT hash FROM _t167_cut
    WHERE hash IS NOT NULL AND length(hash) = 16
), agg AS (
    SELECT hash, count(*)::int4 AS n FROM cut GROUP BY hash
    -- a deterministic order of taking the contents row locks —
    -- like the hash sorting in retentionDecrease (T-166, the T-020 style)
    ORDER BY hash
), decref AS (
    UPDATE contents AS c
    SET refcount = GREATEST(c.refcount - a.n, 0)
    FROM agg AS a
    WHERE c.hash = a.hash
    RETURNING c.hash, c.refcount, c.len
)
SELECT hash, refcount, len FROM decref;

-- Deleting the contents that lost their last reference (the refcount <= 0
-- guard sees the already-new values: a separate statement after the decref).
-- The chunk totals and the next chunk's cursor (the last candidate in PK
-- order — like RetentionOnce of T-166).
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
echo "ворота: PSI some avg10 <= ${psi_max}%, XLEN q:render/q:analyze < $queue_max, лаг q:render < $render_lag_max / q:analyze < $analyze_lag_max, ожидание ворот <= ${gate_max_wait}s"

cz=0 cd=0 cs=0 cv=0
batches=0
t_rows=0; t_refs=0; t_bad=0; t_missing=0; t_deleted=0; t_freed=0
# The return code: emergency stops (a chunk error, a gate timeout, accounting
# anomalies) must not be masked with zero — the drainage is not finished; the
# operator/the chain script must see it. Regular outcomes (the drainage
# exhausted, the max_batches cap with a continuation by rerunning) — success.
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

    # Accounting anomalies — a stop signal (analogous to retention.go
    # BadHashes/Missing): the refcount was not changed for them, but growth
    # means a writer/accounting error.
    if (( bad > 0 || missing > 0 )); then
        echo "ОСТАНОВКА: аномалии учёта (bad_hashes=$bad, missing_refs=$missing) — разберитесь до продолжения (runbook §5.5)"
        exit_code=1
        break
    fi
    # An incomplete chunk — the drainage is exhausted (like RetentionOnce of T-166).
    if (( cand < batch )); then
        echo "дренаж исчерпан (кандидатов $cand < $batch)"
        break
    fi
    # The next chunk's cursor is numbers too (with cand >= batch the keys are
    # not NULL; the check protects against unexpected output, like the
    # counters above).
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

#!/usr/bin/env bash
# Снимок состояния прода Domain Crawler (larasites). Строго read-only.
# Использование: collect.sh [--interval SEC] [--host HOST]
set -uo pipefail

INTERVAL=20
HOST="${PROD_HOST:-larasites}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval) INTERVAL="${2:?}"; shift 2 ;;
    --host)     HOST="${2:?}"; shift 2 ;;
    -h|--help)  sed -n '2,3p' "$0"; exit 0 ;;
    *) echo "неизвестный аргумент: $1 (см. --help)" >&2; exit 2 ;;
  esac
done

ssh -o ConnectTimeout=10 -o BatchMode=yes "$HOST" bash -s -- "$INTERVAL" <<'REMOTE'
set -uo pipefail
IV="$1"
TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"

echo "# Прод-снимок: $(hostname) — $TS  (срезы очередей: ${IV} c)"
echo

echo "## Система"
uptime
echo "vCPU: $(nproc)"
free -h | sed -n '1,3p'
df -h / | tail -1
echo

echo "## Юниты"
systemctl list-units 'crawler-*' --all --type=service --no-legend --plain 2>/dev/null \
  | awk '{printf "%-44s %-9s %s\n", $1, $3, $4}'
for u in redis-server postgresql@16-main unbound nginx fail2ban; do
  printf "%-44s %-9s\n" "$u" "$(systemctl is-active "$u" 2>/dev/null)"
done
echo

echo "## Топ CPU"
ps -eo pcpu,pmem,rss,comm --sort=-pcpu | awk 'NR==1 || $4!="ps"' | head -10
echo

echo "## Очереди (два среза, ${IV} c)"
snap() {
  local s xlen g pend rd lag
  for s in q:dns q:http q:render q:analyze q:dead; do
    xlen=$(redis-cli XLEN "$s" 2>/dev/null)
    g=$(redis-cli --raw XINFO GROUPS "$s" 2>/dev/null)
    if [[ -z "${g:-}" ]]; then
      echo "$(date +%s) $s ${xlen:-0} - - -"   # q:dead: группы нет — только XLEN
    else
      pend=$(sed -n 6p <<<"$g"); rd=$(sed -n 10p <<<"$g"); lag=$(sed -n 12p <<<"$g")
      echo "$(date +%s) $s ${xlen:-0} ${pend:-0} ${rd:-0} ${lag:-0}"
    fi
  done
}
S1=$(snap); sleep "$IV"; S2=$(snap)
printf '%-9s %9s %9s %8s %11s %11s\n' поток xlen lag pending потребл/с приток/с
paste <(printf '%s\n' "$S1") <(printf '%s\n' "$S2") | awk -v iv="$IV" '{
  if ($5 ~ /^[0-9]+$/ && $11 ~ /^[0-9]+$/) {
    c = ($11 - $5) / iv; p = ($11 - $5 + $12 - $6) / iv
    printf "%-9s %9s %9s %8s %11.1f %11.1f\n", $2, $9, $12, $10, c, p
  } else {
    printf "%-9s %9s %9s %8s %11s %11s\n", $2, $9, "-", "-", "-", "-"
  }
}'
echo

echo "## Redis"
redis-cli INFO memory 2>/dev/null | grep -E '^(used_memory_human|maxmemory_human)'
redis-cli INFO clients 2>/dev/null | grep '^connected_clients'
echo

echo "## Конвейер (crawlerctl status, хвост)"
set -a; . /etc/crawler/crawler.env 2>/dev/null; set +a
/opt/crawler/bin/crawlerctl status 2>/dev/null | tail -10
echo

echo "## PostgreSQL (db=crawler)"
psql() { sudo -u postgres psql -d crawler -At -F'|' -c "$1" 2>/dev/null; }
echo "[размер БД] $(psql "SELECT pg_size_pretty(pg_database_size('crawler'))")"
echo "[топ таблиц]"
psql "SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) FROM pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 6"
echo "[живость ключевых таблиц] relname|live|dead"
psql "SELECT relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname IN ('contents','subdomain_results_default','laravel_domains')"
echo "[счётчики crawler_counters — свежие, без шума reanalyze:*/rendered:*]"
psql "SELECT * FROM crawler_counters ORDER BY 3 DESC LIMIT 60" | grep -vE '^(reanalyze:|rendered:)' | head -12
echo

echo "## Контент-хранилище"
du -sh /var/lib/crawler/content 2>/dev/null
echo "паков: $(ls /var/lib/crawler/content 2>/dev/null | grep -c '^pack-')"
echo "старейший/новейший пак:"
ls -lh --time-style=long-iso /var/lib/crawler/content 2>/dev/null | awk '/pack-/ {print $6, $7, $8}' | sed -n '1p;$p'
echo "[компактификация/retention за 24ч]"
journalctl -u crawler-http-worker --since -24h -q 2>/dev/null | grep -E "компактификации паков|retention" | tail -5
echo

echo "## Метрики http-worker:9104"
curl -s -m 3 http://127.0.0.1:9104/metrics 2>/dev/null \
  | grep -E '^crawler_queue_depth|^crawler_content_' | head -15
echo

echo "## Логи"
journalctl --disk-usage 2>/dev/null
du -sh /var/log/* 2>/dev/null | sort -rh | head -5
echo
echo "# Конец снимка"
REMOTE

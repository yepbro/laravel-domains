#!/usr/bin/env bash
# A snapshot of the Domain Crawler production state (larasites). Strictly read-only.
# Usage: collect.sh [--interval SEC] [--host HOST]
set -uo pipefail

INTERVAL=20
HOST="${PROD_HOST:-larasites}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval) INTERVAL="${2:?}"; shift 2 ;;
    --host)     HOST="${2:?}"; shift 2 ;;
    -h|--help)  sed -n '2,3p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1 (see --help)" >&2; exit 2 ;;
  esac
done

ssh -o ConnectTimeout=10 -o BatchMode=yes "$HOST" bash -s -- "$INTERVAL" <<'REMOTE'
set -uo pipefail
IV="$1"
TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"

echo "# Prod snapshot: $(hostname) — $TS  (queue snapshots: ${IV} s)"
echo

echo "## System"
uptime
echo "vCPU: $(nproc)"
free -h | sed -n '1,3p'
df -h / | tail -1
echo

echo "## Units"
systemctl list-units 'crawler-*' --all --type=service --no-legend --plain 2>/dev/null \
  | awk '{printf "%-44s %-9s %s\n", $1, $3, $4}'
for u in redis-server postgresql@16-main unbound nginx fail2ban; do
  printf "%-44s %-9s\n" "$u" "$(systemctl is-active "$u" 2>/dev/null)"
done
echo

echo "## Top CPU"
ps -eo pcpu,pmem,rss,comm --sort=-pcpu | awk 'NR==1 || $4!="ps"' | head -10
echo

echo "## Queues (two snapshots, ${IV} s)"
snap() {
  local s xlen g pend rd lag
  for s in q:dns q:http q:render q:render-fallback q:analyze q:dead; do
    xlen=$(redis-cli XLEN "$s" 2>/dev/null)
    g=$(redis-cli --raw XINFO GROUPS "$s" 2>/dev/null)
    if [[ -z "${g:-}" ]]; then
      echo "$(date +%s) $s ${xlen:-0} - - -"   # no groups — XLEN only
    else
      pend=$(sed -n 6p <<<"$g"); rd=$(sed -n 10p <<<"$g"); lag=$(sed -n 12p <<<"$g")
      echo "$(date +%s) $s ${xlen:-0} ${pend:-0} ${rd:-0} ${lag:-0}"
    fi
  done
}
S1=$(snap); sleep "$IV"; S2=$(snap)
printf '%-9s %9s %9s %8s %11s %11s\n' stream xlen lag pending cons/s inflow/s
paste <(printf '%s\n' "$S1") <(printf '%s\n' "$S2") | awk -v iv="$IV" '{
  if ($5 ~ /^[0-9]+$/ && $11 ~ /^[0-9]+$/) {
    c = ($11 - $5) / iv; p = ($11 - $5 + $12 - $6) / iv
    printf "%-9s %9s %9s %8s %11.1f %11.1f\n", $2, $9, $12, $10, c, p
  } else {
    printf "%-9s %9s %9s %8s %11s %11s\n", $2, $9, "-", "-", "-", "-"
  }
}'
if [[ "$(redis-cli --raw EXISTS q:render-fallback 2>/dev/null)" != "1" ]]; then
  echo "q:render-fallback: the stream is not in Redis — normal before the stage 35/36 deploy"
fi
echo

echo "## Redis"
redis-cli INFO memory 2>/dev/null | grep -E '^(used_memory_human|maxmemory_human)'
redis-cli INFO clients 2>/dev/null | grep '^connected_clients'
echo

echo "## Pipeline (crawlerctl status, tail)"
set -a; . /etc/crawler/crawler.env 2>/dev/null; set +a
/opt/crawler/bin/crawlerctl status 2>/dev/null | tail -10
echo

echo "## PostgreSQL (db=crawler)"
psql() { sudo -u postgres psql -d crawler -At -F'|' -c "$1" 2>/dev/null; }
echo "[DB size] $(psql "SELECT pg_size_pretty(pg_database_size('crawler'))")"
echo "[top tables]"
psql "SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) FROM pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 6"
echo "[liveness of the key tables] relname|live|dead"
psql "SELECT relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname IN ('contents','subdomain_results_default','laravel_domains')"
echo "[crawler_counters counters — fresh, without the reanalyze:*/rendered:* noise]"
psql "SELECT * FROM crawler_counters ORDER BY 3 DESC LIMIT 60" | grep -vE '^(reanalyze:|rendered:)' | head -12
echo

echo "## Content"
du -sh /var/lib/crawler/content 2>/dev/null
echo "packs: $(ls /var/lib/crawler/content 2>/dev/null | grep -c '^pack-')"
echo "oldest/newest pack:"
ls -lh --time-style=long-iso /var/lib/crawler/content 2>/dev/null | awk '/pack-/ {print $6, $7, $8}' | sed -n '1p;$p'
echo "[compaction/retention in 24h]"
journalctl -u crawler-http-worker --since -24h -q 2>/dev/null | grep -E "pack compaction pass|retention" | tail -5
echo

echo "## Metrics http-worker:9104"
curl -s -m 3 http://127.0.0.1:9104/metrics 2>/dev/null \
  | grep -E '^crawler_queue_depth|^crawler_content_' | head -15
echo

# Stage 35/36 (render engines + the fallback queue). Before the stage is
# deployed to prod these entities do not exist — every field tolerates the
# absence and prints a placeholder marker, not emptiness: a pre-deploy
# marker is normal, not an anomaly.
echo "## Render: engines and fallback (stage 35/36)"
if systemctl cat crawler-render-fallback-worker.service >/dev/null 2>&1; then
  echo "[worker B unit] $(systemctl is-active crawler-render-fallback-worker 2>/dev/null)"
else
  echo "[worker B unit] no unit — normal before the stage 35/36 deploy"
fi
echo "[worker A :9105 — fallback counters]"
out=$(curl -s -m 3 http://127.0.0.1:9105/metrics 2>/dev/null | grep '^crawler_render_fallback')
echo "${out:-none (before the stage 35/36 deploy)}"
echo "[worker A :9105 — records by engine, the engine label]"
out=$(curl -s -m 3 http://127.0.0.1:9105/metrics 2>/dev/null \
  | grep '^crawler_render_records_total' | grep 'engine=')
echo "${out:-no engine label (before the stage 35/36 deploy)}"
echo "[worker B :9120 — fallback counters, the port listens after the deploy]"
out=$(curl -s -m 3 http://127.0.0.1:9120/metrics 2>/dev/null | grep '^crawler_render_fallback')
echo "${out:-none (before the stage 35/36 deploy / the port does not listen)}"
echo

echo "## Logs"
journalctl --disk-usage 2>/dev/null
du -sh /var/log/* 2>/dev/null | sort -rh | head -5
echo
echo "# End of snapshot"
REMOTE

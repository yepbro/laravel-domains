#!/bin/bash
# Light diff-grade snapshot for the standing-watch mode (~20–30 s,
# no double queue cuts — do NOT use per-tick instead of collect.sh).
# Covers: time/load/mem + swap si/so thrash, disk/content/packs
# + /var/log/postgresql, PG restarts + OOM per hour + longest active
# query, http-worker WARN/err series per hour, retention metrics
# + persisted cursor + journal tail, today's crawler_counters (checks /
# temp_error / retry_mature / render_pending), queue depths, scheduler
# state, q:dead.
ssh larasites '
echo "=== time / load / mem ==="; date +"%d.%m %H:%M:%S"; cat /proc/loadavg
free -m | sed -n "2,3p" | awk "{print \$2\"/\"\$3\"/\"\$7}"
vmstat 1 2 | tail -1 | awk "{print \"runq=\"\$1\" si/so=\"\$7\"/\"\$8\" kB/s\"}"
echo "=== disk / content / pg-log ==="
df -h / | tail -1 | tr -s " " | cut -d" " -f3,4,5; du -sh /var/lib/crawler/content | cut -f1; ls /var/lib/crawler/content/ | grep -c "^pack-"
du -sh /var/log/postgresql | cut -f1
echo "=== PG: restarts / OOM / longest active query ==="
systemctl show postgresql@16-main -p NRestarts
journalctl -k --since "-65min" -q 2>/dev/null | grep -ci "out of memory"
sudo -u postgres psql -d crawler -tAc "SELECT coalesce(round(max(extract(epoch from now()-query_start)))::int,0) FROM pg_stat_activity WHERE state='"'"'active'"'"';" 2>/dev/null
echo "=== http-worker: WARN/err series (last hour) ==="
journalctl -u crawler-http-worker --since "-65min" -q | grep -cE "level=(WARN|ERROR)"
journalctl -u crawler-http-worker --since "-65min" -q | grep -E "level=(WARN|ERROR)" | grep -oE "msg=\"[^\"]+\"" | sort | uniq -c | sort -rn | head -3
echo "=== retention: metrics / cursor / journal ==="
curl -s 127.0.0.1:9104/metrics | grep -E "^crawler_content_retention_(errors_total|pass_seconds_count|rows_total|contents_total|freed_bytes_total)" | sed "s/{service=\"http-worker\"}//"
sudo -u postgres psql -d crawler -tAc "SELECT checked_at, updated_at FROM retention_drain_cursor;" 2>&1
journalctl -u crawler-http-worker --since "-65min" -q | grep -iE "retention pass|cursor" | tail -3
echo "=== counters today: checks / temp_error / retry_mature / render_pending ==="
sudo -u postgres psql -d crawler -tAc "SELECT name||'"'"'='"'"'||value FROM crawler_counters WHERE name IN ('"'"'checks:'"'"'||current_date,'"'"'temp_error:'"'"'||current_date,'"'"'retry_mature:'"'"'||current_date,'"'"'render_pending'"'"');" 2>/dev/null
echo "=== queues / rates ==="
systemctl is-active crawler-scheduler
set -a; . /etc/crawler/crawler.env 2>/dev/null; set +a
/opt/crawler/bin/crawlerctl status 2>/dev/null | grep -A6 "queues (depth)"
redis-cli XLEN q:dead
'

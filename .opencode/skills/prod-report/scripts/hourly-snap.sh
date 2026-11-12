#!/bin/bash
# Light diff-grade snapshot for the standing-watch mode (~15–20 s,
# no double queue cuts — do NOT use per-tick instead of collect.sh).
# Covers: time/load/mem, disk/content/packs, PG restarts+OOM per hour,
# retention metrics + persisted cursor + journal tail, queue depths/rates,
# scheduler state, q:dead.
ssh larasites '
echo "=== time / load / mem ==="; date +"%d.%m %H:%M:%S"; cat /proc/loadavg
free -m | sed -n "2,3p" | awk "{print \$2\"/\"\$3\"/\"\$7}"
echo "=== disk / content ==="
df -h / | tail -1 | tr -s " " | cut -d" " -f3,4,5; du -sh /var/lib/crawler/content | cut -f1; ls /var/lib/crawler/content/ | grep -c "^pack-"
echo "=== PG: restarts / OOM in the last hour ==="
systemctl show postgresql@16-main -p NRestarts
journalctl -k --since "-65min" -q 2>/dev/null | grep -ci "out of memory"
echo "=== retention: metrics / cursor / journal ==="
curl -s 127.0.0.1:9104/metrics | grep -E "^crawler_content_retention_(errors_total|pass_seconds_count|rows_total|contents_total|freed_bytes_total)" | sed "s/{service=\"http-worker\"}//"
sudo -u postgres psql -d crawler -tAc "SELECT checked_at, updated_at FROM retention_drain_cursor;" 2>&1
journalctl -u crawler-http-worker --since "-65min" -q | grep -iE "проход retention|курсор" | tail -3
echo "=== очереди / скорость ==="
systemctl is-active crawler-scheduler
set -a; . /etc/crawler/crawler.env 2>/dev/null; set +a
/opt/crawler/bin/crawlerctl status 2>/dev/null | grep -A6 "очереди (глубина)"
redis-cli XLEN q:dead
'

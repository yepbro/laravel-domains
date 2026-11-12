#!/bin/bash
# drain-guard.sh — guard of a "calm window" (docs/operations.md §4.6):
# watches for the completion of the current retention pass (an increase
# of crawler_content_retention_pass_seconds_count) and STARTS the
# scheduler itself on completion OR on the time cap — the stop window
# must never hang forever.
#
# Usage: drain-guard.sh [cap-min]     (default cap: 90 minutes)
# Poll interval: 5 minutes. Prints a tick line per poll; the final
# state after the loop is for the agent's report.
#
# Note: the guard runs on the agent side and dies with the agent's
# server restart — the standing-watch recovery rule (SKILL.md) covers
# that: on resume take an immediate snapshot and decide per the
# autonomy criteria whether to start the front.

CAP_MIN="${1:-90}"
BASE=$(ssh larasites 'curl -s 127.0.0.1:9104/metrics | grep -E "^crawler_content_retention_pass_seconds_count" | grep -oE "[0-9]+$"' 2>/dev/null | tr -d " ")
BASE="${BASE:-0}"
TARGET=$((BASE + 1))
echo "guard: pass_seconds_count baseline=$BASE, target>=$TARGET, cap=${CAP_MIN}min"

ST=""
for i in $(seq 1 $((CAP_MIN / 5))); do
  sleep 300
  ST=$(ssh larasites '
    systemctl is-active crawler-scheduler
    curl -s 127.0.0.1:9104/metrics | grep -E "^crawler_content_retention_(errors_total|pass_seconds_count|rows_total)" | sed "s/{service=\"http-worker\"}//" | tr "\n" " "
    sudo -u postgres psql -d crawler -tAc "SELECT checked_at, updated_at FROM retention_drain_cursor;" 2>/dev/null
    cat /proc/loadavg' 2>/dev/null)
  echo "[$(date +%H:%M:%S)] $ST"
  if echo "$ST" | grep -q "pass_seconds_count $TARGET"; then
    ssh larasites "systemctl start crawler-scheduler" \
      && echo "PASS_DONE -> SCHEDULER_STARTED at $(date +%H:%M:%S)"
    exit 0
  fi
done
ssh larasites "systemctl start crawler-scheduler" \
  && echo "CAP_${CAP_MIN}MIN -> SCHEDULER_STARTED at $(date +%H:%M:%S)"

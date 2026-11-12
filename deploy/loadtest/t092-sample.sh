#!/bin/bash
# T-092/T-093: a continuous metric sampler of a load run on the production
# server. Launch (root on the server):
#
#   mkdir -p /var/lib/crawler/t092
#   cp t092-sample.sh /var/lib/crawler/t092/ && chmod +x $_
#   systemd-run --unit=t092-sampler /var/lib/crawler/t092/t092-sample.sh [interval_s]
#   systemctl stop t092-sampler          # stop
#
# Output: /var/lib/crawler/t092/metrics.tsv — one row per sample; the counters
# are cumulative (Prometheus counters), the rates are computed as deltas at
# analysis time. Unit memory — MemoryCurrent (bytes), sizes — bytes.
#
# The stage 35/36 columns (render engines and the fallback: fb_*/rend_*/q_render_fb/
# m_fbworker) hold zeros until the stage is deployed to prod: the counters in
# /metrics, the q:render-fallback depth and the worker B unit do not exist
# yet — that is normal, not degradation (hold/holds keep the previous value,
# mem returns 0).
set -u
OUT=/var/lib/crawler/t092/metrics.tsv
INTERVAL=${1:-15}
UNITS="crawler-ingest crawler-scheduler crawler-dns-worker crawler-http-worker crawler-render-worker crawler-analyzer crawler-monitor crawler-web crawler-dashboard crawler-chromium crawler-render-fallback-worker.service postgresql@16-main.service redis-server.service unbound.service"

# a metric from the Prometheus exposition: m <port> <name> <label filter>.
# An empty response (the service is down/restarting) is not returned — the
# caller keeps the counter's previous value (hold-last), so a curl failure
# does not break the monotonicity of the deltas.
m() { curl -fsS --max-time 3 "127.0.0.1:$1/metrics" 2>/dev/null \
      | awk -v name="$2" -v lbl="$3" '
        index($0, name) == 1 {
          rest = substr($0, length(name) + 1)
          lbl_ok = 0
          if (substr(rest, 1, 1) == "{") {
            if (match(rest, /\{[^}]*\}/)) {
              h = substr(rest, RSTART, RLENGTH); lbl_ok = (index(h, lbl) > 0)
            }
          } else if (substr(rest, 1, 1) == " ") {
            # a space or "{" must follow the name — prefix look-alikes
            # (…_total_extra) do not count as a match
            lbl_ok = (lbl == "")
          }
          if (lbl_ok) { print $NF; exit }
        }'
      }

# mem — a unit's MemoryCurrent in bytes; "[not set]" (the unit is restarting
# or does not exist) and an empty response — 0, so the sample row is not lost.
mem() { local v; v=$(systemctl show -p MemoryCurrent --value "$1" 2>/dev/null); \
      case "$v" in ""|"[not set]"|"0") echo 0 ;; *) echo "$v" ;; esac; }

# hold <variable name> <port> <metric> <label>: updates the variable with
# the metric's fresh value; an empty response (the service is down/restarting)
# keeps the previous one — the counter is monotonic, a curl failure does not
# break the deltas. Call without command substitution (it holds variable state).
hold() { # $1 — the variable name, rest — the arguments of m
  local __v
  __v=$(m "$2" "$3" "$4")
  [ -n "$__v" ] && printf -v "$1" '%s' "$__v"
  return 0
}

# msum — like m, but the SUM of all the metric's series: the stage 35/36
# fallback counters are multi-label (published{reason}, consumed{result}) —
# a sum over the label dictionary is needed, not the first series. An empty
# label filter matches all series (for m it matches only the label-less one).
# An empty response is not returned — the caller keeps the previous value
# (hold-last), like m.
msum() { curl -fsS --max-time 3 "127.0.0.1:$1/metrics" 2>/dev/null \
      | awk -v name="$2" -v lbl="$3" '
        index($0, name) == 1 {
          rest = substr($0, length(name) + 1)
          lbl_ok = 0
          if (substr(rest, 1, 1) == "{") {
            if (match(rest, /\{[^}]*\}/)) {
              h = substr(rest, RSTART, RLENGTH); lbl_ok = (lbl == "" || index(h, lbl) > 0)
            }
          } else if (substr(rest, 1, 1) == " ") {
            lbl_ok = (lbl == "")
          }
          if (lbl_ok) { s += $NF; found = 1 }
        }
        END { if (found) print s }'
      }

# holds — hold for msum (the sum of series).
holds() { # $1 — the variable name, rest — the arguments of msum
  local __v
  __v=$(msum "$2" "$3" "$4")
  [ -n "$__v" ] && printf -v "$1" '%s' "$__v"
  return 0
}

mkdir -p "$(dirname "$OUT")"
[ -s "$OUT" ] || printf '# ts load1 memavail_b psi_cpu psi_mem psi_io dns_tasks http_tasks render_tasks analyze_tasks fb_pub fb_cons fb_susp_gate fb_susp_budget rend_chromium rend_moli rend_lightpanda q_dns q_http q_render q_render_fb q_analyze backlog_pending backlog_queued db_size content_size disk_avail m_ingest m_sched m_dns m_http m_render m_analyzer m_monitor m_web m_dash m_chromium m_fbworker m_pg m_redis m_unbound\n' >> "$OUT"

while :; do
  ts=$(date +%s)
  read -r load1 _ < /proc/loadavg
  memavail=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
  psi_cpu=$(awk '/^some/{sub(/^some avg10=/,""); print $1}' /proc/pressure/cpu 2>/dev/null)
  psi_mem=$(awk '/^full/{sub(/^full avg10=/,""); print $1}' /proc/pressure/memory 2>/dev/null)
  psi_io=$(awk '/^full/{sub(/^full avg10=/,""); print $1}' /proc/pressure/io 2>/dev/null)

  hold dns_t    9103 crawler_stage_tasks_total 'stage="dns"'
  hold http_t   9104 crawler_stage_tasks_total 'stage="http"'
  hold render_t 9105 crawler_stage_tasks_total 'stage="render"'
  hold analyze_t 9106 crawler_stage_tasks_total 'stage="analyze"'
  hold q_dns    9103 crawler_queue_depth 'queue="q:dns"'
  hold q_http   9104 crawler_queue_depth 'queue="q:http"'
  hold q_render 9105 crawler_queue_depth 'queue="q:render"'
  hold q_analyze 9106 crawler_queue_depth 'queue="q:analyze"'
  hold bl_pend  9102 crawler_backlog_domains 'state="pending"'
  hold bl_que   9102 crawler_backlog_domains 'state="queued"'

  # Stage 35/36 — render engines and the fallback (before the prod deploy
  # the entities do not exist: hold/holds keep zeros). Publications/
  # suspensions are exposed by worker A (:9105); consumption and the
  # q:render-fallback depth — the worker B build (:9120); engines — the
  # engine label on worker A's records.
  holds fb_pub        9105 crawler_render_fallback_published_total ''  # the sum over reason
  holds fb_cons       9120 crawler_render_fallback_consumed_total ''   # the sum over result
  hold  fb_susp_gate   9105 crawler_render_fallback_suspended_total 'reason="gate"'
  hold  fb_susp_budget 9105 crawler_render_fallback_suspended_total 'reason="budget"'
  hold  rend_chromium  9105 crawler_render_records_total 'engine="chromium"'
  hold  rend_moli      9105 crawler_render_records_total 'engine="moli"'
  hold  rend_lightpanda 9105 crawler_render_records_total 'engine="lightpanda"'
  hold  q_render_fb    9120 crawler_queue_depth 'queue="q:render-fallback"'

  db_size=$(runuser -u postgres -- psql -d crawler -tAc "SELECT pg_database_size('crawler')" 2>/dev/null)
  content_size=$(du -sb /var/lib/crawler/content 2>/dev/null | awk '{print $1}')
  disk_avail=$(df --output=avail -B1 /var/lib/crawler/content | tail -1 | tr -d ' ')

  vals=("$ts" "$load1" "$memavail" "${psi_cpu:-0}" "${psi_mem:-0}" "${psi_io:-0}"
        "${dns_t:-0}" "${http_t:-0}" "${render_t:-0}" "${analyze_t:-0}"
        "${fb_pub:-0}" "${fb_cons:-0}" "${fb_susp_gate:-0}" "${fb_susp_budget:-0}"
        "${rend_chromium:-0}" "${rend_moli:-0}" "${rend_lightpanda:-0}"
        "${q_dns:-0}" "${q_http:-0}" "${q_render:-0}" "${q_render_fb:-0}" "${q_analyze:-0}"
        "${bl_pend:-0}" "${bl_que:-0}" "${db_size:-0}" "${content_size:-0}" "${disk_avail:-0}")
  for u in $UNITS; do vals+=("$(mem "$u")"); done

  line="$(IFS=$'\t'; printf '%s' "${vals[*]}")"
  printf '%s\n' "$line" >> "$OUT"
  sleep "$INTERVAL"
done

#!/bin/bash
# T-092/T-093: непрерывный сэмплер метрик нагрузочного прогона на боевом
# сервере. Запуск (root на сервере):
#
#   mkdir -p /var/lib/crawler/t092
#   cp t092-sample.sh /var/lib/crawler/t092/ && chmod +x $_
#   systemd-run --unit=t092-sampler /var/lib/crawler/t092/t092-sample.sh [интервал_с]
#   systemctl stop t092-sampler          # останов
#
# Выход: /var/lib/crawler/t092/metrics.tsv — по строке на замер; счётчики
# кумулятивные (counter'ы Prometheus), скорости считаются дельтами при
# анализе. Память юнитов — MemoryCurrent (байты), размеры — байты.
set -u
OUT=/var/lib/crawler/t092/metrics.tsv
INTERVAL=${1:-15}
UNITS="crawler-ingest crawler-scheduler crawler-dns-worker crawler-http-worker crawler-render-worker crawler-analyzer crawler-monitor crawler-web crawler-dashboard crawler-chromium postgresql@16-main.service redis-server.service unbound.service"

# метрика из экспозиции Prometheus: m <порт> <имя> <лейбл-фильтр>.
# Пустой ответ (сервис лежит/рестартует) не возвращается — вызывающая
# сторона держит прошлое значение счётчика (hold-last), чтобы сбой
#curl не рвал монотонность дельт.
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
            # за именем обязателен пробел или «{» — префиксные созвучия
            # (…_total_extra) матчем не считаются
            lbl_ok = (lbl == "")
          }
          if (lbl_ok) { print $NF; exit }
        }'
      }

# mem — MemoryCurrent юнита в байтах; «[not set]» (юнит перезапускается
# или не существует) и пустой ответ — 0, чтобы строка замера не терялась.
mem() { local v; v=$(systemctl show -p MemoryCurrent --value "$1" 2>/dev/null); \
      case "$v" in ""|"[not set]"|"0") echo 0 ;; *) echo "$v" ;; esac; }

# hold <имя_переменной> <порт> <метрика> <лейбл>: обновляет переменную
# свежим значением метрики; пустой ответ (сервис лежит/рестартует)
# оставляет прошлое — счётчик монотонен, сбой curl не рвёт дельты.
# Вызывать без подстановки команд (держит состояние переменной).
hold() { # $1 — имя переменной, rest — аргументы m
  local __v
  __v=$(m "$2" "$3" "$4")
  [ -n "$__v" ] && printf -v "$1" '%s' "$__v"
  return 0
}

mkdir -p "$(dirname "$OUT")"
[ -s "$OUT" ] || printf '# ts load1 memavail_b psi_cpu psi_mem psi_io dns_tasks http_tasks render_tasks analyze_tasks q_dns q_http q_render q_analyze backlog_pending backlog_queued db_size content_size disk_avail m_ingest m_sched m_dns m_http m_render m_analyzer m_monitor m_web m_dash m_chromium m_pg m_redis m_unbound\n' >> "$OUT"

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

  db_size=$(runuser -u postgres -- psql -d crawler -tAc "SELECT pg_database_size('crawler')" 2>/dev/null)
  content_size=$(du -sb /var/lib/crawler/content 2>/dev/null | awk '{print $1}')
  disk_avail=$(df --output=avail -B1 /var/lib/crawler/content | tail -1 | tr -d ' ')

  vals=("$ts" "$load1" "$memavail" "${psi_cpu:-0}" "${psi_mem:-0}" "${psi_io:-0}"
        "${dns_t:-0}" "${http_t:-0}" "${render_t:-0}" "${analyze_t:-0}"
        "${q_dns:-0}" "${q_http:-0}" "${q_render:-0}" "${q_analyze:-0}"
        "${bl_pend:-0}" "${bl_que:-0}" "${db_size:-0}" "${content_size:-0}" "${disk_avail:-0}")
  for u in $UNITS; do vals+=("$(mem "$u")"); done

  line="$(IFS=$'\t'; printf '%s' "${vals[*]}")"
  printf '%s\n' "$line" >> "$OUT"
  sleep "$INTERVAL"
done

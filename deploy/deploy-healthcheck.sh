#!/bin/bash
# Проверка боевого набора после деплоя (T-135): все юниты active, healthz
# зелёный на всех портах метрик (9100…9108), crawlerctl отвечает.
# Запускается CI (.github/workflows/deploy.yml) по SSH; exit 0 — всё зелёное.
#
# Повторы вместо одиночной пробы (инциденты деплоя 23.10, утро и вечер:
# оба ранна откатывались на «healthz 9104 FAIL» при живом воркере —
# readiness пингует БД с 1-секундным таймаутом (service.go, healthzHandler)
# и не успевал за один 5-секундный curl через 8 секунд после рестарта:
# стартовый шторм соединений к БД + загрузка слотов воркера под
# MemoryHigh-троттлингом. Юнит при этом работоспособен — откат не требовался).
# Каждая проба повторяется до успеха в общем бюджете READY_DEADLINE.

set -euo pipefail

READY_DEADLINE=${READY_DEADLINE:-300} # бюджет готовности healthz, с
PROBE_TIMEOUT=5                       # curl -m, с
PROBE_PAUSE=5                         # пауза между попытками, с

# Юниты с их портами healthz (docs/deploy.md §5; chromium — только
# активность юнита, порта метрик нет). Юнит, отключённый на сервере
# оператором (systemctl disable/mask — например, crawler-scheduler при
# приостановке постановки задач), исключается из проверки целиком:
# планово выключенный сервис — не сбой деплоя, healthcheck не должен
# авто-откатывать релиз из-за него.
checked=()
ports=()
for spec in crawler-chromium:- crawler-ingest:9101 crawler-scheduler:9102 \
            crawler-dns-worker:9103 crawler-http-worker:9104 \
            crawler-render-worker:9105 crawler-analyzer:9106 \
            crawler-web:9107 crawler-dashboard:9108; do
    u=${spec%%:*}
    p=${spec#*:}
    enabled=$(systemctl is-enabled "$u" 2>/dev/null || true)
    if [ "$enabled" = disabled ] || [ "$enabled" = masked ]; then
        echo "юнит $u: $enabled — исключён из проверки (отключён оператором)"
        continue
    fi
    checked+=("$u")
    if [ "$p" != - ]; then
        ports+=("$p")
    fi
done

# Юниты: до 60 секунд на активность (рестарт набора не мгновенный)
units_deadline=$((SECONDS + 60))
fail=0
while :; do
    fail=0
    for u in "${checked[@]}"; do
        state=$(systemctl is-active "$u" 2>/dev/null || true)
        if [ "$state" != active ]; then
            echo "юнит $u: $state"
            fail=1
        fi
    done
    [ "$fail" -eq 0 ] && break
    if [ "$SECONDS" -ge "$units_deadline" ]; then
        break
    fi
    sleep "$PROBE_PAUSE"
done

deadline=$((SECONDS + READY_DEADLINE))
for p in "${ports[@]}"; do
    until curl -fsS -m "$PROBE_TIMEOUT" "127.0.0.1:$p/healthz" >/dev/null 2>&1; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "healthz 127.0.0.1:$p: FAIL (нет ответа за ${READY_DEADLINE}s)"
            fail=1
            break
        fi
        sleep "$PROBE_PAUSE"
    done
done

# crawlerctl status на загруженной БД отвечает небыстро (агрегаты по
# ~47 млн строк — T-161): таймаут вдвое больше бюджета проб, чтобы шаг
# не вис бесконечно, но не рвал деплой из-за известной медлительности
if ! timeout $((READY_DEADLINE * 2)) env CRAWLER_CONFIG=/etc/crawler/config.yaml \
    /opt/crawler/bin/crawlerctl status >/dev/null 2>&1; then
    echo "crawlerctl status: FAIL"
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    echo "ЕСТЬ КРАСНЫЕ ПРОВЕРКИ" >&2
    exit 1
fi
echo "все проверки зелёные: ${#checked[@]} юнитов active, healthz ${ports[*]} ok, crawlerctl ok (за ${SECONDS}s)"

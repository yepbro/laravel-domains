#!/bin/bash
# Проверка боевого набора после деплоя (T-135): все юниты active, healthz
# зелёный на всех портах метрик (9100…9108), crawlerctl отвечает.
# Запускается CI (.github/workflows/deploy.yml) по SSH; exit 0 — всё зелёное.
set -euo pipefail

# Прогрев: первый старт открывает пулы БД/Redis, применяет миграции
sleep 8

fail=0

for u in crawler-chromium crawler-ingest crawler-scheduler crawler-dns-worker \
         crawler-http-worker crawler-render-worker crawler-analyzer \
         crawler-monitor crawler-web crawler-dashboard; do
    state=$(systemctl is-active "$u" 2>/dev/null || true)
    if [ "$state" != active ]; then
        echo "юнит $u: $state"
        fail=1
    fi
done

# monitor 9100, ingest 9101, scheduler 9102, dns 9103, http 9104,
# render 9105, analyzer 9106, web 9107, dashboard 9108 (docs/deploy.md §5)
for p in 9100 9101 9102 9103 9104 9105 9106 9107 9108; do
    if ! curl -fsS -m 5 "127.0.0.1:$p/healthz" >/dev/null 2>&1; then
        echo "healthz 127.0.0.1:$p: FAIL"
        fail=1
    fi
done

if ! CRAWLER_CONFIG=/etc/crawler/config.yaml /opt/crawler/bin/crawlerctl status >/dev/null 2>&1; then
    echo "crawlerctl status: FAIL"
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    echo "ЕСТЬ КРАСНЫЕ ПРОВЕРКИ" >&2
    exit 1
fi
echo "все проверки зелёные: 10 юнитов active, healthz 9100…9108 ok, crawlerctl ok"

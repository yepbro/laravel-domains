#!/bin/bash
# Автоматический откат боевого деплоя (T-137, ТЗ §10.4 «healthcheck
# с автоматическим откатом»): восстановление предыдущей версии
# /opt/crawler/bin.prev в /opt/crawler/bin и перезапуск набора — по
# механике rollback() из deploy-install.sh. Вызывается CI
# (.github/workflows/deploy.yml, шаг rollback после провала healthcheck)
# по SSH; пригоден и для ручного запуска дежурным (docs/deploy.md §5) —
# заменяет ручную процедуру stop/копирование/start.
#
# Коды выхода — шаг извещения Telegram переводит их в точный текст:
#   0 — откат выполнен, набор перезапущен, все юниты active;
#   1 — откат выполнен, но не все юниты active (требуется оператор);
#   3 — откат не удался (временный каталог/копирование/stop/замена);
#   4 — откатывать нечего: нет $BACKUP (первый деплой), набор не тронут.
# 2 и 255 сам скрипт не возвращает (2 — сбой разбора у bash, 255 —
# транспорт ssh): конвейер трактует оба как «состояние сервера неизвестно».
#
# Пути переопределяются окружением при боевых дефолтах (тестируемость
# локальным harness без root и /opt): BIN_DIR=... BACKUP=... скрипт
set -euo pipefail

BIN_DIR=${BIN_DIR:-/opt/crawler/bin}
BACKUP=${BACKUP:-/opt/crawler/bin.prev}

if [ ! -d "$BACKUP" ]; then
    echo "ОТКАТ: откатывать нечего — нет $BACKUP (первый деплой); набор не тронут, работает новая версия" >&2
    exit 4
fi

# Копия бэкапа — ДО остановки набора и во временный соседний каталог
# (mktemp — уникальное имя, та же ФС): сбой копирования не трогает живой
# каталог (сервер остаётся на новой версии), а замена через mv в пределах
# одной ФС атомарна — без окна частично скопированного bin. rmdir: cp
# должен создать каталог сам, иначе копия вложится внутрь готового.
if ! tmp="$(mktemp -d "${BIN_DIR}.rollback.XXXXXX")"; then
    echo "ОТКАТ: не удался — не создать временный каталог рядом с $BIN_DIR; набор не тронут, работает новая версия" >&2
    exit 3
fi
rmdir "$tmp" || { echo "ОТКАТ: не удался — не очистить временный $tmp; набор не тронут, работает новая версия" >&2; exit 3; }
trap 'rm -rf "$tmp"' EXIT
if ! cp -a "$BACKUP" "$tmp"; then
    echo "ОТКАТ: не удался — копирование $BACKUP; набор не тронут, работает новая версия" >&2
    exit 3
fi

if ! (systemctl stop crawler.target &&
      rm -rf "$BIN_DIR" &&
      mv "$tmp" "$BIN_DIR"); then
    echo "ОТКАТ: не удался (stop/замена) — проверьте $BIN_DIR и systemctl status crawler.target" >&2
    exit 3
fi

# Старт — отдельно от цепочки замены: его сбой не значит «откат не
# удался» (бинарники уже восстановлены), итог покажет проверка юнитов
# ниже. reset-failed обязателен: юниты упавшей новой версии могли
# упереться в start-limit (StartLimitBurst=10, docs/deploy.md §5) —
# без сброса start откажет и откат не поднимет набор.
systemctl reset-failed 'crawler-*' 2>/dev/null || true
if ! systemctl start crawler.target; then
    echo "старт набора после отката вернул ошибку — состояние юнитов см. ниже" >&2
fi

# Лёгкая проверка после отката: юниты active (список — как в
# deploy-healthcheck.sh; полный healthz-цикл не повторяем — подробную
# диагностику делает следующий шаг конвейера). 3 с — окно на первый
# крах: Type=simple даёт active сразу, сломанный бинарник за это время
# успевает упасть и виден как activating/failed.
sleep 3
degraded=0
for u in crawler-chromium crawler-ingest crawler-scheduler crawler-dns-worker \
         crawler-http-worker crawler-render-worker crawler-analyzer \
         crawler-monitor crawler-web crawler-dashboard; do
    state=$(systemctl is-active "$u" 2>/dev/null || true)
    if [ "$state" != active ]; then
        echo "юнит $u после отката: $state" >&2
        degraded=1
    fi
done

if [ "$degraded" -ne 0 ]; then
    echo "ОТКАТ: $BACKUP -> $BIN_DIR выполнен, набор перезапущен, но не все юниты active — требуется оператор" >&2
    exit 1
fi

echo "ОТКАТ: $BACKUP -> $BIN_DIR выполнен, набор перезапущен, все юниты active"

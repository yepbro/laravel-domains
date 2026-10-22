#!/bin/bash
# Установка бинарников Domain Crawler на боевой сервер (docs/deploy.md §4.2,
# T-135). Вызывается CI (.github/workflows/deploy.yml) по SSH после загрузки
# /tmp/crawler-deploy.tar.gz; пригоден и для ручного запуска: положить архив
# и выполнить этот скрипт от root.
#
# Атомарность: текущий /opt/crawler/bin сохраняется в /opt/crawler/bin.prev
# (копия во временный каталог + mv — старый бэкап жив, пока новый не готов
# целиком), набор останавливается, бинарники заменяются, набор стартует.
# Любая ошибка после успешной остановки — откат на bin.prev и рестарт (trap).
#
# Коды выхода — шаг извещения Telegram в deploy.yml различает их:
#   0 — установлено;
#   1/2 и прочие, кроме перечисленных — сбой внутри установки, trap-откат
#       на bin.prev выполнен, набор перезапущен и active (2 возвращают и
#       bash при сбое разбора, и tar при битом архиве — в обоих реальных
#       случаях откат уже выполнен trap'ом);
#   3 — не остановился набор (до trap: bin не тронут, но состояние юнитов
#       требует проверки);
#   4 — нет архива (до trap: сервер не тронут, откат не требовался);
#   5 — сбой внутри установки, откат на bin.prev выполнен, но набор не
#       поднялся/поднялся не весь — требуется оператор;
#   6 — сбой внутри установки, откат сам не удался — состояние требует
#       оператора;
#   7 — сбой при подготовке бэкапа: bin не тронут (работает версия до
#       деплоя), откат не требовался, набор перезапущен;
#   8 — то же, но старт набора не удался — состояние требует оператора.
# 255 — транспорт ssh: состояние сервера неизвестно.
set -euo pipefail

# Пути переопределяются окружением при боевых дефолтах — тестируемость
# локальным harness без root и /opt (как в deploy-rollback.sh, T-137).
BIN_DIR=${BIN_DIR:-/opt/crawler/bin}
BACKUP=${BACKUP:-/opt/crawler/bin.prev}
ARCHIVE=${ARCHIVE:-/tmp/crawler-deploy.tar.gz}

# Свежий бэкап построен (rollback имеет смысл); результат trap-отката
# (0 — выполнен и набор active, 5 — выполнен, набор не весь, 6 — не удался).
fresh=0
rollback_rc=0

# Откат — по образцу deploy-rollback.sh (T-137): копия бэкапа во временный
# соседний каталог ДО rm (сбой cp не уничтожает текущий bin), затем
# атомарный mv; итог (рестарт + юниты) проверяется и отражается в коде
# выхода. reset-failed сбрасывает start-limit юнитов, упершихся в него на
# краш-лупе упавшей версии, — без этого рестарт может отказать.
rollback() {
    echo "ОТКАТ: восстанавливаю $BACKUP -> $BIN_DIR" >&2
    ok=0
    if rb="$(mktemp -d "${BIN_DIR}.rollback.XXXXXX")"; then
        if rmdir "$rb" && cp -a "$BACKUP" "$rb" && rm -rf "$BIN_DIR" && mv "$rb" "$BIN_DIR"; then
            ok=1
        else
            rm -rf "$rb" || true
            echo "ОТКАТ: восстановление не удалось — проверьте $BIN_DIR" >&2
        fi
    else
        echo "ОТКАТ: не создать временный каталог рядом с $BIN_DIR — $BIN_DIR не тронут" >&2
    fi
    systemctl reset-failed 'crawler-*' 2>/dev/null || true
    if ! systemctl restart crawler.target; then
        echo "ОТКАТ: рестарт набора вернул ошибку — требуется оператор" >&2
        if [ "$ok" = 1 ]; then rollback_rc=5; else rollback_rc=6; fi
        return 0
    fi
    if [ "$ok" = 0 ]; then
        rollback_rc=6
        return 0
    fi
    # Лёгкая проверка (как в deploy-rollback.sh): Type=simple даёт active
    # сразу, сломанный бинарник виден за 3 с как activating/failed.
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
        echo "ОТКАТ: выполнен, но не все юниты active — требуется оператор" >&2
        rollback_rc=5
    fi
    return 0
}

on_exit() {
    rc=$?
    if [ "$rc" -ne 0 ]; then
        if [ "$fresh" = 1 ]; then
            rollback
            if [ "$rollback_rc" -ne 0 ]; then
                rc=$rollback_rc
            fi
        else
            # Сбой до построения свежего бэкапа: bin не тронут — откатывать
            # нечего (bin.prev хранит версию N-1, откат даунгрейдил бы
            # живую N) — вернуть набор на той же версии.
            echo "СБОЙ до построения бэкапа — $BIN_DIR не тронут, стартую набор" >&2
            systemctl reset-failed 'crawler-*' 2>/dev/null || true
            if systemctl start crawler.target; then
                rc=7
            else
                echo "старт набора не удался — состояние сервера требует оператора" >&2
                rc=8
            fi
        fi
    fi
    exit "$rc"
}

[ -f "$ARCHIVE" ] || { echo "ОШИБКА: нет архива $ARCHIVE" >&2; exit 4; }

# До trap'а (по коду видно, что откат не выполнялся и не требовался):
# сбой остановки = bin не тронут, но набор мог остановиться частично.
systemctl stop crawler.target || { echo "ОШИБКА: не удалось остановить crawler.target" >&2; exit 3; }
trap on_exit EXIT
# Выход по сигналу (обрыв ssh при отмене/таймауте рана, ручное прерывание):
# без этого $? в EXIT-обработнике равен 0 и откат не запускается.
trap 'exit 130' INT TERM HUP

# Свежий бэкап через временный каталог и mv: старый bin.prev жив, пока
# новый не скопирован целиком (сбой cp не оставляет сервер без бэкапа).
bu="$(mktemp -d "${BACKUP}.new.XXXXXX")"
rmdir "$bu"
cp -a "$BIN_DIR" "$bu"
rm -rf "$BACKUP"
mv "$bu" "$BACKUP"
fresh=1

rm -f "$BIN_DIR"/*
tar -xzf "$ARCHIVE" -C "$BIN_DIR"
chmod 0755 "$BIN_DIR"/*

systemctl reset-failed 'crawler-*' 2>/dev/null || true
systemctl start crawler.target
rm -f "$ARCHIVE"
trap - EXIT
trap - INT TERM HUP

echo "установлено в $BIN_DIR; предыдущие бинарники — в $BACKUP"

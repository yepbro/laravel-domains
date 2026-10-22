#!/bin/bash
# Установка бинарников Domain Crawler на боевой сервер (docs/deploy.md §4.2,
# T-135). Вызывается CI (.github/workflows/deploy.yml) по SSH после загрузки
# /tmp/crawler-deploy.tar.gz; пригоден и для ручного запуска: положить архив
# и выполнить этот скрипт от root.
#
# Атомарность: текущий /opt/crawler/bin сохраняется в /opt/crawler/bin.prev,
# набор останавливается, бинарники заменяются, набор стартует. Любая ошибка
# до успешного старта — откат на bin.prev и рестарт (trap).
set -euo pipefail

BIN_DIR=/opt/crawler/bin
BACKUP=/opt/crawler/bin.prev
ARCHIVE=/tmp/crawler-deploy.tar.gz

[ -f "$ARCHIVE" ] || { echo "ОШИБКА: нет архива $ARCHIVE" >&2; exit 1; }

rollback() {
    echo "ОТКАТ: восстанавливаю $BACKUP -> $BIN_DIR" >&2
    rm -rf "$BIN_DIR"
    cp -a "$BACKUP" "$BIN_DIR"
    systemctl restart crawler.target || true
}

systemctl stop crawler.target
trap 'rc=$?; if [ $rc -ne 0 ]; then rollback; fi; exit $rc' EXIT

rm -rf "$BACKUP"
cp -a "$BIN_DIR" "$BACKUP"
rm -f "$BIN_DIR"/*
tar -xzf "$ARCHIVE" -C "$BIN_DIR"
chmod 0755 "$BIN_DIR"/*

systemctl start crawler.target
rm -f "$ARCHIVE"
trap - EXIT

echo "установлено в $BIN_DIR; предыдущие бинарники — в $BACKUP"

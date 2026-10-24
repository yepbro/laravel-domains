#!/bin/bash
# Доставка Telegram-секретов в EnvironmentFile monitor (T-173; запуск —
# ops-workflow ops-telegram-env.yml вручную workflow_dispatch: значения
# живут в секретах GitHub и больше нигде не хранятся, прочитать их из
# GitHub нельзя — CI единственное место, откуда их можно доставить).
# Аргументы (передаются по SSH из шага workflow, в логи не печатаются):
#   $1 — TELEGRAM_BOT_TOKEN
#   $2 — TELEGRAM_CHAT_ID
#
# Идемпотентен: существующие ключи заменяются по месту (sed), отсутствующие
# дописываются; права файла сохраняются (root:crawler 640 — читает только
# юнит monitor, EnvironmentFile). После записи — рестарт crawler-monitor:
# EnvironmentFile читается systemd при старте юнита. Результат проверяется
# по журналу monitor (строка «telegram: доставка алертов включена» из
# NewTelegramNotifier) — секреты в вывод не попадают.
set -euo pipefail

ENV_FILE=/etc/crawler/crawler.env
UNIT=crawler-monitor.service

if [ $# -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
    echo "telegram-env: ожидаются два аргумента (токен, chat id)" >&2
    exit 4
fi
token="$1"
chat="$2"

if [ ! -f "$ENV_FILE" ]; then
    echo "telegram-env: $ENV_FILE не найден" >&2
    exit 4
fi

# Замена по месту (ключ есть) или дописывание (ключа нет); разделитель |
# в sed — значения секрета разделителей и перевода строки не содержат.
upsert() {
    local key="$1" value="$2"
    if grep -q "^${key}=" "$ENV_FILE"; then
        sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
    else
        printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
    fi
}
upsert TELEGRAM_BOT_TOKEN "$token"
upsert TELEGRAM_CHAT_ID "$chat"

# Права: владелец/группа и режим файла обязан сохраниться (root:crawler
# 640) — sed -i пересоздаёт файл в tmp той же группы; восстанавливаем
# явно, случайная замена (umask раннера не действует — локальный root)
# исключена приписыванием.
chown root:crawler "$ENV_FILE"
chmod 640 "$ENV_FILE"

# Секреты вступают в силу рестартом юнита (EnvironmentFile читается при
# старте); monitor — служебный сервис без состояния, рестарт безопасен.
systemctl restart "$UNIT"

# Проверка: journal монитора после рестарта содержит строку включения
# доставки (NewTelegramNotifier, notify.go). 5 с — каркас поднимает
# рабочую логику почти сразу; без строки не падаем — ран жёлтый по
# проверке ниже (текст для оператора), состояние видно в journalctl.
sleep 5
if journalctl -u "$UNIT" --since '-2 min' --no-pager | grep -q 'telegram: доставка алертов включена'; then
    echo "telegram-env: секреты доставлены, доставка включена (журнал monitor)"
else
    echo "telegram-env: секреты записаны, строка включения в журнале не найдена — проверьте journalctl -u $UNIT" >&2
    exit 3
fi

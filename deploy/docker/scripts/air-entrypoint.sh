#!/bin/sh
# Точка входа Go-сервисов в dev-режиме.
# Берёт общий .air.toml из корня репозитория, подставляет имя сервиса
# из переменной окружения SERVICE и запускает air.
set -eu

: "${SERVICE:?SERVICE env is required}"

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root_dir=$(CDPATH= cd -- "${script_dir}/../../.." && pwd)

cfg="/tmp/air.${SERVICE}.toml"
sed "s|__SERVICE__|${SERVICE}|g" "${root_dir}/.air.toml" > "${cfg}"

cd "${root_dir}"
exec air -c "${cfg}"

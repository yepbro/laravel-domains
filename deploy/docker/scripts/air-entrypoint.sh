#!/bin/sh
# The entrypoint of the Go services in dev mode.
# Takes the shared .air.toml from the repository root, substitutes the
# service name from the SERVICE environment variable and starts air.
set -eu

: "${SERVICE:?SERVICE env is required}"

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root_dir=$(CDPATH= cd -- "${script_dir}/../../.." && pwd)

cfg="/tmp/air.${SERVICE}.toml"
sed "s|__SERVICE__|${SERVICE}|g" "${root_dir}/.air.toml" > "${cfg}"

cd "${root_dir}"
exec air -c "${cfg}"

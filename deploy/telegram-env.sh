#!/bin/bash
# Delivering the Telegram secrets into the monitor's EnvironmentFile (T-173;
# launched by the ops workflow ops-telegram-env.yml manually via
# workflow_dispatch: the values live in GitHub secrets and are stored nowhere
# else, they cannot be read back from GitHub — CI is the only place they can
# be delivered from). The values arrive in the environment variables
# TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID of this process (T-303, 19#P2-3): the
# workflow's remote command reads them from the ssh stdin stream (two lines
# ahead of the script text) and exports them for the script. Formerly they
# were positional arguments — but /proc/<pid>/cmdline is world-readable
# (hidepid=0) for the script's lifetime, while the environment of a root
# process (/proc/<pid>/environ) is root-only. The values are single-line
# (a Telegram token and a numeric chat id; a newline inside a secret would
# break the line-based transport).
#
# Idempotent: existing keys are replaced in place (sed), missing ones are
# appended; the file permissions are preserved (root:crawler 640 — only the
# monitor unit reads it, the EnvironmentFile). After writing — a restart of
# crawler-monitor: systemd reads the EnvironmentFile at unit start. The
# result is verified against the monitor's journal (the alert-delivery-
# enabled line from NewTelegramNotifier) — the secrets never reach the output.
set -euo pipefail

# The paths are overridden by the environment with production defaults —
# testable by a local harness with PATH stubs for chown/chmod/systemctl/
# journalctl and ENV_FILE pointed at a temp file (the chown of root:crawler
# and the systemctl restart still need the stubs; like deploy-install.sh's
# BIN_DIR/BACKUP/ARCHIVE).
ENV_FILE=${ENV_FILE:-/etc/crawler/crawler.env}
UNIT=crawler-monitor.service

if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
    echo "telegram-env: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID (the environment) are required" >&2
    exit 4
fi
token=${TELEGRAM_BOT_TOKEN}
chat=${TELEGRAM_CHAT_ID}

if [ ! -f "$ENV_FILE" ]; then
    echo "telegram-env: $ENV_FILE not found" >&2
    exit 4
fi

# Replace in place (the key exists) or append (no key). The value reaches
# awk through its ENVIRONMENT (ENVIRON), not through argv — /proc/<pid>/
# cmdline is world-readable at hidepid=0 (T-303, review P2-1; sed would
# take the value in its argument list). cp -p seeds the temp copy with the
# original's mode/owner — the secrets never sit in a world-readable file,
# even between the redirect and the mv (the property sed -i had); the
# final chown/chmod below re-asserts both anyway.
upsert() {
    local key="$1" value="$2"
    if grep -q "^${key}=" "$ENV_FILE"; then
        cp -p "$ENV_FILE" "$ENV_FILE.upsert.tmp"
        if ! KEY="$key" VAL="$value" awk '
                index($0, ENVIRON["KEY"] "=") == 1 { print ENVIRON["KEY"] "=" ENVIRON["VAL"]; next }
                { print }
            ' "$ENV_FILE" > "$ENV_FILE.upsert.tmp"; then
            rm -f "$ENV_FILE.upsert.tmp"
            echo "telegram-env: the $key upsert failed" >&2
            exit 1
        fi
        mv "$ENV_FILE.upsert.tmp" "$ENV_FILE"
    else
        printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
    fi
}
upsert TELEGRAM_BOT_TOKEN "$token"
upsert TELEGRAM_CHAT_ID "$chat"

# Permissions: the file's owner/group and mode must be preserved
# (root:crawler 640) — sed -i recreates the file in tmp with the same group;
# we restore them explicitly, so an accidental change (the runner's umask
# does not apply — a local root) is ruled out.
chown root:crawler "$ENV_FILE"
chmod 640 "$ENV_FILE"

# The secrets take effect on the unit restart (the EnvironmentFile is read
# at start); monitor is a stateless utility service, a restart is safe.
systemctl restart "$UNIT"

# The check: after the restart the monitor's journal contains the
# delivery-enabled line (NewTelegramNotifier, notify.go). 5 s — the service
# framework brings up the working logic almost immediately; without the line
# we do not fail — the run is yellow on the check below (the text is for the
# operator), the state is visible in journalctl.
sleep 5
if journalctl -u "$UNIT" --since '-2 min' --no-pager | grep -q 'telegram: alert delivery on'; then
    echo "telegram-env: secrets delivered, delivery enabled (see the monitor journal)"
else
    echo "telegram-env: secrets written, the enable line not found in the journal — check journalctl -u $UNIT" >&2
    exit 3
fi

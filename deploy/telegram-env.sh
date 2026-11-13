#!/bin/bash
# Delivering the Telegram secrets into the monitor's EnvironmentFile (T-173;
# launched by the ops workflow ops-telegram-env.yml manually via
# workflow_dispatch: the values live in GitHub secrets and are stored nowhere
# else, they cannot be read back from GitHub — CI is the only place they can
# be delivered from). Arguments (passed over SSH from the workflow step, not
# printed to the logs):
#   $1 — TELEGRAM_BOT_TOKEN
#   $2 — TELEGRAM_CHAT_ID
#
# Idempotent: existing keys are replaced in place (sed), missing ones are
# appended; the file permissions are preserved (root:crawler 640 — only the
# monitor unit reads it, the EnvironmentFile). After writing — a restart of
# crawler-monitor: systemd reads the EnvironmentFile at unit start. The
# result is verified against the monitor's journal (the alert-delivery-
# enabled line from NewTelegramNotifier) — the secrets never reach the output.
set -euo pipefail

ENV_FILE=/etc/crawler/crawler.env
UNIT=crawler-monitor.service

if [ $# -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
    echo "telegram-env: two arguments expected (token, chat id)"; >&2
    exit 4
fi
token="$1"
chat="$2"

if [ ! -f "$ENV_FILE" ]; then
    echo "telegram-env: $ENV_FILE not found" >&2
    exit 4
fi

# Replace in place (the key exists) or append (no key); the | separator in
# sed — the secret values contain no separators or newlines.
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

#!/bin/sh
# The smartd warning handler for Domain Crawler (T-089, spec §10.3).
#
# Invoked by smartd via /usr/share/smartmontools/smartd_warning.sh when any
# checked directive fires (-H, -l error, -W; the config —
# deploy/smartd/smartd.conf). There are no arguments and no stdin
# (-m <nomailer>); the data comes in the SMARTD_* environment variables
# (man smartd.conf(5), "-M exec").
#
# The task is one: write the warning to syslog/journald — the same tape all
# the crawler services write to (docs/deploy.md, T-085); the operator reads
# it with:
#   journalctl -t smartd -p warning
# Print nothing to stdout/stderr: smartd treats output as a handler failure
# and duplicates the fragments into syslog.

set -u

# The priority by the event type (SMARTD_FAILTYPE): health/temperature —
# crit, a test record — info, the rest (error log growth and the like) — warning.
prio=daemon.warning
case "${SMARTD_FAILTYPE:-}" in
Health | Temperature) prio=daemon.crit ;;
EmailTest) prio=daemon.info ;;
esac

logger -t smartd -p "$prio" \
    "SMART: ${SMARTD_DEVICESTRING:-${SMARTD_DEVICE:-device}}:" \
    "${SMARTD_MESSAGE:-cause unknown};" \
    "first occurrence: ${SMARTD_TFIRST:-?};" \
    "previous messages: ${SMARTD_PREVCNT:-0}"

exit 0

#!/bin/bash
# Installing the Domain Crawler binaries on the production server
# (docs/deploy.md §4.2, T-135). Invoked by CI (.github/workflows/deploy.yml)
# over SSH after /tmp/crawler-deploy.tar.gz is uploaded; also usable
# manually: put the archive in place and run this script as root.
#
# Atomicity: the current /opt/crawler/bin is saved to /opt/crawler/bin.prev
# (a copy into a temporary directory + mv — the old backup lives until the
# new one is ready in full), the set is stopped, the binaries are replaced,
# the set starts. Any error after a successful stop — a rollback to bin.prev
# and a restart (trap).
#
# Exit codes — the Telegram notification step in deploy.yml distinguishes them:
#   0 — installed;
#   1/2 and any others not listed — a failure inside the installation, the
#       trap rollback to bin.prev has run, the set is restarted and active
#       (2 is returned both by bash on a parse error and by tar on a broken
#       archive — in both real cases the rollback has already run via the trap);
#   3 — the set did not stop (before the trap: bin untouched, but the unit
#       states need checking);
#   4 — no archive (before the trap: the server untouched, no rollback needed);
#   5 — a failure inside the installation, the rollback to bin.prev has run,
#       but the set did not come up / came up only partly — an operator is needed;
#   6 — a failure inside the installation, the rollback itself failed — the
#       state needs an operator;
#   7 — a failure while preparing the backup: bin untouched (the pre-deploy
#       version is running), no rollback needed, the set restarted;
#   8 — the same, but starting the set failed — the state needs an operator.
# 255 — the ssh transport: the server state is unknown.
set -euo pipefail

# The paths are overridden by the environment with production defaults —
# testable by a local harness without root and /opt (like deploy-rollback.sh, T-137).
BIN_DIR=${BIN_DIR:-/opt/crawler/bin}
BACKUP=${BACKUP:-/opt/crawler/bin.prev}
ARCHIVE=${ARCHIVE:-/tmp/crawler-deploy.tar.gz}

# A fresh backup has been built (a rollback makes sense); the trap rollback
# result (0 — done and the set active, 5 — done, the set is not fully up, 6 — failed).
fresh=0
rollback_rc=0

# The rollback follows deploy-rollback.sh (T-137): a copy of the backup into
# a temporary neighboring directory BEFORE the rm (a cp failure does not
# destroy the current bin), then an atomic mv; the outcome (restart + units)
# is checked and reflected in the exit code. reset-failed clears the
# start-limit of units that hit it in the crash loop of the broken version —
# without it the restart may be refused.
rollback() {
    echo "ROLLBACK: restoring $BACKUP -> $BIN_DIR" >&2
    ok=0
    if rb="$(mktemp -d "${BIN_DIR}.rollback.XXXXXX")"; then
        if rmdir "$rb" && cp -a "$BACKUP" "$rb" && rm -rf "$BIN_DIR" && mv "$rb" "$BIN_DIR"; then
            ok=1
        else
            rm -rf "$rb" || true
            echo "ROLLBACK: the restore failed — check $BIN_DIR" >&2
        fi
    else
        echo "ROLLBACK: cannot create a temp directory next to $BIN_DIR — $BIN_DIR is untouched" >&2
    fi
    systemctl reset-failed 'crawler-*' 2>/dev/null || true
    if ! systemctl restart crawler.target; then
        echo "ROLLBACK: the set restart returned an error — an operator is required" >&2
        if [ "$ok" = 1 ]; then rollback_rc=5; else rollback_rc=6; fi
        return 0
    fi
    if [ "$ok" = 0 ]; then
        rollback_rc=6
        return 0
    fi
    # A light check (like deploy-rollback.sh): Type=simple reports active
    # right away; a broken binary shows within 3 s as activating/failed.
    sleep 3
    degraded=0
    for u in crawler-chromium crawler-ingest crawler-scheduler crawler-dns-worker \
             crawler-http-worker crawler-render-worker crawler-analyzer \
             crawler-monitor crawler-web crawler-dashboard; do
        # A unit disabled by the operator (disable/mask — like in
        # deploy-healthcheck.sh) is deliberately stopped, not "failed to
        # come up": it is excluded from the check, otherwise the rollback
        # would falsely report degraded.
        enabled=$(systemctl is-enabled "$u" 2>/dev/null || true)
        if [ "$enabled" = disabled ] || [ "$enabled" = masked ]; then
            continue
        fi
        state=$(systemctl is-active "$u" 2>/dev/null || true)
        if [ "$state" != active ]; then
            echo "unit $u after the rollback: $state" >&2
            degraded=1
        fi
    done
    if [ "$degraded" -ne 0 ]; then
        echo "ROLLBACK: performed, but not all units are active — an operator is required" >&2
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
            # A failure before the fresh backup was built: bin is untouched —
            # there is nothing to roll back to (bin.prev holds version N-1;
            # a rollback would downgrade the live N) — bring the set back
            # on the same version.
            echo "FAILURE before the backup was built — $BIN_DIR is untouched, starting the set" >&2
            systemctl reset-failed 'crawler-*' 2>/dev/null || true
            if systemctl start crawler.target; then
                rc=7
            else
                echo "the set start failed — the server state needs an operator" >&2
                rc=8
            fi
        fi
    fi
    exit "$rc"
}

[ -f "$ARCHIVE" ] || { echo "ERROR: no archive $ARCHIVE" >&2; exit 4; }

# Before the trap (the code shows no rollback ran and none was needed):
# a stop failure = bin untouched, but the set may have stopped only partly.
systemctl stop crawler.target || { echo "ERROR: failed to stop crawler.target" >&2; exit 3; }
trap on_exit EXIT
# Exit on a signal (ssh dropped on a run cancel/timeout, a manual interrupt):
# without this $? is 0 in the EXIT handler and the rollback never starts.
trap 'exit 130' INT TERM HUP

# A fresh backup via a temporary directory and mv: the old bin.prev lives
# until the new one is copied in full (a cp failure never leaves the server
# without a backup).
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

echo "installed into $BIN_DIR; the previous binaries are in $BACKUP"

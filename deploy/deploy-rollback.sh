#!/bin/bash
# Automatic rollback of a production deploy (T-137, spec §10.4 "healthcheck
# with automatic rollback"): restoring the previous version
# /opt/crawler/bin.prev into /opt/crawler/bin and restarting the set —
# following the rollback() mechanics from deploy-install.sh. Invoked by CI
# (.github/workflows/deploy.yml, the rollback step after a healthcheck
# failure) over SSH; also usable manually by the on-call (docs/deploy.md §5) —
# it replaces the manual stop/copy/start procedure.
#
# Exit codes — the Telegram notification step turns them into exact text:
#   0 — the rollback is done, the set restarted, all units active;
#   1 — the rollback is done, but not all units are active (an operator needed);
#   3 — the rollback failed (temporary directory/copying/stop/replacement);
#   4 — nothing to roll back: no $BACKUP (the first deploy), the set untouched.
# 2 and 255 are never returned by the script itself (2 — a bash parse error,
# 255 — the ssh transport): the pipeline treats both as "the server state
# is unknown".
#
# The paths are overridden by the environment with production defaults
# (testable by a local harness without root and /opt): BIN_DIR=... BACKUP=... script
set -euo pipefail

BIN_DIR=${BIN_DIR:-/opt/crawler/bin}
BACKUP=${BACKUP:-/opt/crawler/bin.prev}

if [ ! -d "$BACKUP" ]; then
    echo "ROLLBACK: nothing to roll back — no $BACKUP (the first deploy); the set is untouched, the new version is running" >&2
    exit 4
fi

# The backup copy — BEFORE stopping the set, into a temporary neighboring
# directory (mktemp — a unique name, the same filesystem): a copying failure
# does not touch the live directory (the server stays on the new version),
# and the replacement via mv within one filesystem is atomic — no window of
# a partly copied bin. rmdir: cp must create the directory itself, otherwise
# the copy would land inside the ready one.
if ! tmp="$(mktemp -d "${BIN_DIR}.rollback.XXXXXX")"; then
    echo "ROLLBACK: failed — cannot create a temp directory next to $BIN_DIR; the set is untouched, the new version is running" >&2
    exit 3
fi
rmdir "$tmp" || { echo "ROLLBACK: failed — cannot clean the temp $tmp; the set is untouched, the new version is running" >&2; exit 3; }
trap 'rm -rf "$tmp"' EXIT
if ! cp -a "$BACKUP" "$tmp"; then
    echo "ROLLBACK: failed — copying $BACKUP; the set is untouched, the new version is running" >&2
    exit 3
fi

if ! (systemctl stop crawler.target &&
      rm -rf "$BIN_DIR" &&
      mv "$tmp" "$BIN_DIR"); then
    echo "ROLLBACK: failed (stop/replacement) — check $BIN_DIR and systemctl status crawler.target" >&2
    exit 3
fi

# The start — separate from the replacement chain: its failure does not mean
# "the rollback failed" (the binaries are already restored); the unit check
# below shows the outcome. reset-failed is mandatory: the units of the broken
# new version may have hit the start-limit (StartLimitBurst=10,
# docs/deploy.md §5) — without the reset the start is refused and the
# rollback never brings the set up.
systemctl reset-failed 'crawler-*' 2>/dev/null || true
if ! systemctl start crawler.target; then
    echo "the set start after the rollback returned an error — see the unit states below" >&2
fi

# A light check after the rollback: the units active (the list — like in
# deploy-healthcheck.sh; the full healthz cycle is not repeated — the next
# pipeline step does the deep diagnostics). 3 s — the window for the first
# crash: Type=simple reports active right away, and a broken binary manages
# to die within it and is visible as activating/failed.
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
    echo "ROLLBACK: $BACKUP -> $BIN_DIR performed, the set restarted, but not all units are active — an operator is required" >&2
    exit 1
fi

echo "ROLLBACK: $BACKUP -> $BIN_DIR performed, the set restarted, all units are active"

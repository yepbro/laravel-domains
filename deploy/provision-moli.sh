#!/bin/bash
# Installing the Moli render engine on the production server (the moli
# primary rollout of stage 35, T-337; docs/deploy.md §2.4). Invoked by
# the operator as root from a repository checkout over SSH
# (`ssh root@<server> 'bash -s' -- < deploy/provision-moli.sh`) or
# directly on the server; idempotent — a re-run replaces the binary and
# refreshes the user/unit state; against a RUNNING unit a re-run applies
# the new binary at once (try-restart).
#
# What it does: installs the runtime libraries (the set of
# deploy/docker/Dockerfile.moli), downloads the pinned release asset,
# verifies sha256, installs the binary into /opt/crawler/moli/, creates
# the crawler-moli user/group (docs/deploy.md §4.4), daemon-reloads and
# ENABLES the unit WITHOUT STARTING it — the start is a deliberate later
# step of the rollout order (the safety net first: worker B, then the
# @9109 chromium pin, only then the engine and the worker A switch —
# docs/deploy.md §2.4).
#
# The version pin (FR-17): the canonical source is the
# MOLI_VERSION/MOLI_SHA256 pair of deploy/docker/.env.example — keep the
# defaults below in sync with it; the pair travels as one. The defaults
# are the canonical pin v1.1.21 — for the ROLLOUT the operator passes
# the tested v1.1.26 pair explicitly (docs/deploy.md §2.4): v1.1.21/1.1.22
# reject our Fetch.enable pattern set (upstream #2470, fixed in v1.1.23)
# — every render task would fail; v1.1.26 is the build measured by
# T-331/T-332/T-335 (the #3047 memory burst fixed; the remaining quality
# gap is the operator's accepted trade-off, D-075).
#
# --dry-run: print the plan (the URL, the SHA256, the target paths, the
# user/unit actions) and touch nothing — a rehearsal on the server.
#
# Exit codes: 0 — done (or the plan printed); 1 — a bad invocation;
# 2 — a missing prerequisite (the unit file); the curl/tar/apt failures
# return their own codes through set -e.
set -euo pipefail

usage() {
    echo "usage: $0 [--dry-run]   (env overrides: MOLI_VERSION MOLI_SHA256 MOLI_DIR UNIT_FILE)" >&2
}

DRY_RUN=0
case "${1:-}" in
    "") ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 1 ;;
esac

# Overridable with the environment — testable without root and /opt
# (the pattern of deploy-install.sh). The defaults are the canonical pin
# (deploy/docker/.env.example).
MOLI_VERSION=${MOLI_VERSION:-v1.1.21}
MOLI_SHA256=${MOLI_SHA256:-048d9e482b6c618af08b17eb19200ea65b1b2fafd39975e5b9de19a60653ed8f}
MOLI_DIR=${MOLI_DIR:-/opt/crawler/moli}
UNIT_FILE=${UNIT_FILE:-/etc/systemd/system/crawler-moli.service}

# 64 HEX characters, not just 64 of anything (a bad value must die here,
# not in a confusing sha256sum -c failure later).
[[ $MOLI_SHA256 =~ ^[0-9a-fA-F]{64}$ ]] || {
    echo "ERROR: MOLI_SHA256 must be a 64-hex sha256 (got \"$MOLI_SHA256\")" >&2
    exit 1
}

# The release asset target per the machine architecture (the Dockerfile
# mapping; release assets exist for both).
case "$(uname -m)" in
    x86_64) moli_target=x86_64-unknown-linux-gnu ;;
    aarch64) moli_target=aarch64-unknown-linux-gnu ;;
    *)
        echo "ERROR: unsupported architecture $(uname -m)" >&2
        exit 1
        ;;
esac
url="https://github.com/lexmount/moli/releases/download/${MOLI_VERSION}/moli-${moli_target}.tar.gz"

# The plan header (always printed — the --dry-run rehearsal material).
cat <<EOF
moli provisioning plan
  version:      ${MOLI_VERSION}
  sha256:       ${MOLI_SHA256}
  asset:        ${url}
  binary:       ${MOLI_DIR}/moli
  user/group:   crawler-moli (crawler)
  unit:         ${UNIT_FILE} (daemon-reload + enable; NOT started on the
                first run — a re-run against a running unit try-restarts it)
  packages:     ca-certificates curl libstdc++6 libfontconfig1 libfreetype6
EOF
if [ "$DRY_RUN" -eq 1 ]; then
    # Read-only rehearsal findings — the checks most likely to stop the
    # real run, without any side effect.
    if [ -f "$UNIT_FILE" ]; then
        echo "  unit file:    present ($UNIT_FILE)"
    else
        echo "  unit file:    MISSING — install it first (the real run exits 2)"
    fi
    if getent passwd crawler-moli >/dev/null; then
        echo "  user:         crawler-moli exists"
    else
        echo "  user:         crawler-moli will be created"
    fi
    echo "DRY-RUN: nothing touched"
    exit 0
fi

# The runtime libraries of the release binary (the Dockerfile.moli set):
# the binary is dynamically linked — libstdc++/libgcc and the font stack
# libfontconfig/libfreetype (+their deps). The host font files themselves
# (installed for chromium, docs/deploy.md §4.3) stay visible to the
# engine — a benign difference from the (fontless) dev image. curl is
# also the downloader below; ca-certificates — TLS of the rendered pages.
apt-get update -yq
apt-get install -yq --no-install-recommends \
    ca-certificates curl libstdc++6 libfontconfig1 libfreetype6

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
# The download is resilient to GitHub release-CDN flapping (the incident
# of Nov 8, 2026 — the Dockerfile set): 504/503/timeout are transient,
# --retry repeats the attempt inside this run, --max-time bounds a hung
# one. A 404 (a broken version pin) is NOT a retry — an immediate fail.
curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 15 --max-time 90 \
    -o "$tmp/moli.tar.gz" "$url"
# Exactly two spaces — the sha256sum -c format (a substituted/corrupted
# download dies here).
echo "${MOLI_SHA256}  $tmp/moli.tar.gz" | sha256sum -c -
# The tarball contains a top-level directory with the binary (the layout
# of the official moli-installer.sh).
tar -xzf "$tmp/moli.tar.gz" -C "$tmp" --strip-components=1

# The user/group (docs/deploy.md §4.4): the engine renders untrusted
# pages under its own account; the group is shared with the set
# (idempotent — what exists is kept as is).
getent group crawler >/dev/null || groupadd -r crawler
getent passwd crawler-moli >/dev/null || \
    useradd -r -g crawler -d /var/lib/crawler -s /usr/sbin/nologin crawler-moli

# The binary: /opt/crawler/moli/ — the same "a separate package
# directory, not an OS package" decision as chromium (docs/deploy.md
# §4.3); the unit's ExecStart points here.
install -d -o root -g root -m 0755 "$MOLI_DIR"
install -m 0755 "$tmp/moli" "$MOLI_DIR/moli"
"$MOLI_DIR/moli" --version

# The unit travels separately (the pipeline does not ship units; the
# runbook installs it from the checkout first — docs/deploy.md §2.4).
[ -f "$UNIT_FILE" ] || {
    echo "ERROR: no unit ${UNIT_FILE} — install it first:" >&2
    echo "  ssh root@<server> 'install -D -m 0644 /dev/stdin ${UNIT_FILE}' < deploy/systemd/crawler-moli.service" >&2
    exit 2
}
systemctl daemon-reload
# Enable WITHOUT start: the start is a later, deliberate step of the
# rollout order (docs/deploy.md §2.4) — the fallback safety net comes up
# first. Idempotent.
systemctl enable crawler-moli.service
# A re-provision against a RUNNING unit applies the new binary at once
# (try-restart: restarts only if active — on the first run the unit is
# inactive and this is a no-op). The worker waits the restart out within
# render.engine_wait (T-283), so a version bump does not burn task
# attempts; the live version — `curl -s 127.0.0.1:9224/json/version`.
state="$(systemctl is-active crawler-moli.service 2>/dev/null || true)"
if [ "$state" = active ]; then
    systemctl try-restart crawler-moli.service
    echo "provisioned moli ${MOLI_VERSION} into ${MOLI_DIR} (unit enabled; the running engine was restarted onto the new binary)"
else
    echo "provisioned moli ${MOLI_VERSION} into ${MOLI_DIR} (unit enabled, NOT started — start it per docs/deploy.md §2.4 step 4)"
fi

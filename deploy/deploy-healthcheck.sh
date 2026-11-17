#!/bin/bash
# Post-deploy check of the production set (T-135): all units active, healthz
# green on all metric ports (9100…9108), crawlerctl responding.
# Run by CI (.github/workflows/deploy.yml) over SSH; exit 0 — everything green.
#
# Retries instead of a single probe (deploy incidents of 23.10, morning and
# evening: both runs rolled back on "healthz 9104 FAIL" with the worker alive —
# readiness pings the DB with a 1-second timeout (service.go, healthzHandler)
# and did not make it within one 5-second curl 8 seconds after the restart:
# the initial connection storm to the DB + worker slot loading under
# MemoryHigh throttling. The unit was otherwise operational — no rollback was
# needed). Each probe repeats until success within the shared READY_DEADLINE budget.

set -euo pipefail

READY_DEADLINE=${READY_DEADLINE:-300} # the healthz readiness budget, s
PROBE_TIMEOUT=5                       # curl -m, s
PROBE_PAUSE=5                         # the pause between attempts, s

# Units with their healthz ports (docs/deploy.md §5; chromium — unit
# activity only, no metric port). A unit disabled on the server by the
# operator (systemctl disable/mask — e.g., crawler-scheduler when enqueueing
# is suspended) is excluded from the check entirely: a deliberately stopped
# service is not a deploy failure, and the healthcheck must not auto-rollback
# the release because of it.
#
# The list must stay in sync with the unit lists in deploy-install.sh (the
# rollback check) and deploy-rollback.sh (T-303, 19#P2-2: monitor was
# missing here while install/rollback checked it — a crash-looping monitor
# passed the post-deploy check). A shared list file cannot be sourced: the
# scripts travel to the server over ssh stdin (`bash -s < script`) and the
# repository is not on the server.
checked=()
ports=()
for spec in crawler-chromium:- crawler-monitor:9100 crawler-ingest:9101 \
            crawler-scheduler:9102 crawler-dns-worker:9103 \
            crawler-http-worker:9104 crawler-render-worker:9105 \
            crawler-analyzer:9106 crawler-web:9107 crawler-dashboard:9108; do
    u=${spec%%:*}
    p=${spec#*:}
    enabled=$(systemctl is-enabled "$u" 2>/dev/null || true)
    if [ "$enabled" = disabled ] || [ "$enabled" = masked ]; then
        echo "unit $u: $enabled — excluded from the check (disabled by the operator)"
        continue
    fi
    checked+=("$u")
    if [ "$p" != - ]; then
        ports+=("$p")
    fi
done

# Units: up to 60 seconds to become active (the set restart is not instantaneous)
units_deadline=$((SECONDS + 60))
fail=0
while :; do
    fail=0
    for u in "${checked[@]}"; do
        state=$(systemctl is-active "$u" 2>/dev/null || true)
        if [ "$state" != active ]; then
            echo "unit $u: $state"
            fail=1
        fi
    done
    [ "$fail" -eq 0 ] && break
    if [ "$SECONDS" -ge "$units_deadline" ]; then
        break
    fi
    sleep "$PROBE_PAUSE"
done

# The healthz budget is SHARED across the ports: one deadline for the whole
# list. That is deliberate — it bounds the entire check well within the
# deploy job's timeout-minutes, so a hung service still yields a definite
# rc=1 and the rollback step runs, instead of a cancelled job with the
# state "unknown". The asymmetry (T-303, 19#P3-3): a port that consumes the
# whole budget leaves every following port a single -m 5 attempt (the
# SECONDS >= deadline check is true at once) — the message then names the
# first port that timed out. Per-port full budgets (9 × 300 s) would exceed
# the job timeout, so the shared budget with the documented asymmetry is
# the lesser evil. The ports are probed in list order.
deadline=$((SECONDS + READY_DEADLINE))
for p in "${ports[@]}"; do
    until curl -fsS -m "$PROBE_TIMEOUT" "127.0.0.1:$p/healthz" >/dev/null 2>&1; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "healthz 127.0.0.1:$p: FAIL (no response within ${READY_DEADLINE}s)"
            fail=1
            break
        fi
        sleep "$PROBE_PAUSE"
    done
done

# crawlerctl runs outside the units, but the DSN may live ONLY in the
# EnvironmentFile (crawler.env.example: "The secret (DSN) is kept here, not
# in YAML") — the units get the file via EnvironmentFile=, the ssh session
# has none of it, and `crawlerctl status` would fail to connect → rc=1 → a
# false rollback of every deploy in that configuration (T-303, 19#P2-1).
# The file is NOT sourced with `.`: a systemd EnvironmentFile permits values
# with shell metacharacters (the example's CRAWLER_HTTP_USER_AGENT has
# spaces, parentheses and a semicolon) — a shell read would die on a syntax
# error. The two keys the CLI needs are taken as whole lines and exported
# into the environment (not argv: /proc/<pid>/cmdline is world-readable at
# hidepid=0, /proc/<pid>/environ of a root process is root-only). A missing
# file or key — the value must then live in the YAML (the former behavior).
ENV_FILE=${CRAWLER_ENV_FILE:-/etc/crawler/crawler.env}
env_line() {
    local val
    val=$(sed -n "s/^$1=//p" "$ENV_FILE" 2>/dev/null | tail -n 1 || true)
    # The systemd EnvironmentFile grammar also allows one pair of matching
    # quotes around the value (systemd strips them) — strip them too, or a
    # quoted DSN would be exported with the quotes and the CLI would fail
    # (review P3-1). Later duplicates win (tail -n 1), like in systemd.
    case "$val" in
        \"*\") val=${val#\"}; val=${val%\"} ;;
        \'*\') val=${val#\'}; val=${val%\'} ;;
    esac
    printf '%s' "$val"
}
v=$(env_line CRAWLER_POSTGRES_DSN)
if [ -n "$v" ]; then export CRAWLER_POSTGRES_DSN="$v"; fi
v=$(env_line CRAWLER_REDIS_ADDR)
if [ -n "$v" ]; then export CRAWLER_REDIS_ADDR="$v"; fi

# crawlerctl status answers slowly on a loaded DB (aggregates over
# ~47M rows — T-161): the timeout is twice the probe budget, so the step
# neither hangs forever nor breaks the deploy over a known slowness
if ! timeout $((READY_DEADLINE * 2)) env CRAWLER_CONFIG=/etc/crawler/config.yaml \
    /opt/crawler/bin/crawlerctl status >/dev/null 2>&1; then
    echo "crawlerctl status: FAIL"
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    echo "SOME CHECKS ARE RED" >&2
    exit 1
fi
echo "all checks green: ${#checked[@]} units active, healthz ${ports[*]} ok, crawlerctl ok (in ${SECONDS}s)"

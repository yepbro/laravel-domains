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
checked=()
ports=()
for spec in crawler-chromium:- crawler-ingest:9101 crawler-scheduler:9102 \
            crawler-dns-worker:9103 crawler-http-worker:9104 \
            crawler-render-worker:9105 crawler-analyzer:9106 \
            crawler-web:9107 crawler-dashboard:9108; do
    u=${spec%%:*}
    p=${spec#*:}
    enabled=$(systemctl is-enabled "$u" 2>/dev/null || true)
    if [ "$enabled" = disabled ] || [ "$enabled" = masked ]; then
        echo "юнит $u: $enabled — исключён из проверки (отключён оператором)"
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
            echo "юнит $u: $state"
            fail=1
        fi
    done
    [ "$fail" -eq 0 ] && break
    if [ "$SECONDS" -ge "$units_deadline" ]; then
        break
    fi
    sleep "$PROBE_PAUSE"
done

deadline=$((SECONDS + READY_DEADLINE))
for p in "${ports[@]}"; do
    until curl -fsS -m "$PROBE_TIMEOUT" "127.0.0.1:$p/healthz" >/dev/null 2>&1; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "healthz 127.0.0.1:$p: FAIL (нет ответа за ${READY_DEADLINE}s)"
            fail=1
            break
        fi
        sleep "$PROBE_PAUSE"
    done
done

# crawlerctl status answers slowly on a loaded DB (aggregates over
# ~47M rows — T-161): the timeout is twice the probe budget, so the step
# neither hangs forever nor breaks the deploy over a known slowness
if ! timeout $((READY_DEADLINE * 2)) env CRAWLER_CONFIG=/etc/crawler/config.yaml \
    /opt/crawler/bin/crawlerctl status >/dev/null 2>&1; then
    echo "crawlerctl status: FAIL"
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    echo "ЕСТЬ КРАСНЫЕ ПРОВЕРКИ" >&2
    exit 1
fi
echo "все проверки зелёные: ${#checked[@]} юнитов active, healthz ${ports[*]} ok, crawlerctl ok (за ${SECONDS}s)"

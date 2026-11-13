#!/usr/bin/env bash
# Telegram notifications about the CI/CD pipeline events (spec §10.4, T-136)
# and runtime alerts (T-162, spec §5.9 — delivered by monitor).
#
# One invocation — one message into the dedicated group: the outcome marker
# at the start, the event type, the workflow and the step, the offending
# commit, a link to the run; for a deploy — the version (git describe) and
# the rollback fact. There is no separate service: CI calls the script as the
# last step from .github/workflows/ci.yml (failures) and deploy.yml (success
# and failure); monitor — via exec from the alert loop
# (internal/monitor/notify.go) in the --message mode: the alert text in full
# (monitor puts the 🚨/✅ marker itself), without the CI wrapper (commit/run).
#
# Reliability (§10.4): a sending failure does not affect the pipeline
# outcome — the notification step in the workflow runs with
# continue-on-error, and missing secrets (a PR from a fork, a repository
# without configured secrets, a monitor host without TELEGRAM_* in its
# EnvironmentFile) — a regular skip with the reason in the log. The
# TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID values are not printed: the token
# never reaches the output (curl without -v, the API response with chat.id
# is discarded), xtrace is not enabled.
#
# Usage (CI/CD):
#   deploy/telegram-notify.sh --status ok|fail --event "<event type>" \
#       --workflow deploy [--step "<step>"] \
#       [--version "$(git describe --tags --always)"] \
#       [--rollback performed|performed-degraded|nothing-to-rollback|failed|unknown] \
#       [--sha <SHA>] [--dry-run]
# Usage (monitor alerts, T-162; the text is passed as is):
#   deploy/telegram-notify.sh --message "<alert text>" [--dry-run]
#
# The commit and the run link are taken from the checkout and the runner's
# GITHUB_* variables (--sha overrides the commit — for PR runs, where HEAD is
# a merge commit); locally with --dry-run the missing lines are simply
# omitted.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: telegram-notify.sh --status ok|fail --event TYPE --workflow NAME
                         [--step STEP] [--version VERSION] [--sha SHA]
                         [--rollback performed|performed-degraded|nothing-to-rollback|failed|unknown]
                         [--dry-run]
        telegram-notify.sh --message TEXT [--dry-run]
EOF
    exit 2
}

status='' event='' workflow='' step='' version='' rollback='' sha='' message='' dry_run=0
while [ $# -gt 0 ]; do
    case "$1" in
        --status)   [ $# -ge 2 ] || usage; status=$2;   shift 2 ;;
        --event)    [ $# -ge 2 ] || usage; event=$2;    shift 2 ;;
        --workflow) [ $# -ge 2 ] || usage; workflow=$2; shift 2 ;;
        --step)     [ $# -ge 2 ] || usage; step=$2;     shift 2 ;;
        --version)  [ $# -ge 2 ] || usage; version=$2;  shift 2 ;;
        --rollback) [ $# -ge 2 ] || usage; rollback=$2; shift 2 ;;
        --sha)      [ $# -ge 2 ] || usage; sha=$2;      shift 2 ;;
        --message)  [ $# -ge 2 ] || usage; message=$2;  shift 2 ;;
        --dry-run)  dry_run=1; shift ;;
        -h|--help)  usage ;;
        *)          echo "telegram-notify: unknown argument: $1" >&2; usage ;;
    esac
done

# The --message mode (monitor alerts, T-162): arbitrary text in full — the
# markers and the details are already in it; the CI arguments are not mixed
# with it (an error, not a silent reinterpretation: sending alerts as a
# deploy message is worse than the invocation failing).
if [ -n "$message" ]; then
    if [ -n "$status" ] || [ -n "$event" ] || [ -n "$workflow" ] || \
       [ -n "$step" ] || [ -n "$version" ] || [ -n "$rollback" ] || [ -n "$sha" ]; then
        echo 'telegram-notify: --message does not combine with the CI/CD arguments (--status/--event/--workflow/...)' >&2
        exit 2
    fi
else
    if [ -z "$status" ] || [ -z "$event" ] || [ -z "$workflow" ]; then
        usage
    fi
    case "$status" in
        ok|fail) ;;
        *) echo "telegram-notify: --status must be ok or fail" >&2; exit 2 ;;
    esac
    case "$rollback" in
        performed|performed-degraded|nothing-to-rollback|failed|unknown|'') ;;
        *) echo "telegram-notify: --rollback must be performed, performed-degraded, nothing-to-rollback, failed or unknown" >&2; exit 2 ;;
    esac
fi

# The secrets guard: without a token/chat there are no notifications —
# exit 0, the run is not colored red (dry-run works without secrets too,
# substituting placeholders).
if [ "$dry_run" -eq 0 ]; then
    if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
        echo 'telegram: notification skipped — TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID is not set (a PR from a fork or the repository secrets are not configured)'
        exit 0
    fi
fi

# The message text: in the --message mode (monitor alerts) — the text passed
# in full; in the CI/CD mode — an assembly of the event marker, the workflow
# header, the rollback, the commit and the run link.
if [ -n "$message" ]; then
    text="$message"
else
    # The offending commit: --sha (a PR run: the PR head, not the merge
    # commit), otherwise the checkout's HEAD (in deploy it is the SHA tested
    # by ci); outside git — GITHUB_SHA.
    short_sha=''
    subject=''
    if [ -n "$sha" ]; then
        short_sha="$(git rev-parse --short "$sha" 2>/dev/null || printf '%s' "$sha" | cut -c1-7)"
        subject="$(git log -1 --pretty=%s "$sha" 2>/dev/null || true)"
    elif short_sha="$(git rev-parse --short HEAD 2>/dev/null)"; then
        subject="$(git log -1 --pretty=%s)"
    else
        short_sha="${GITHUB_SHA:-unknown}"
        short_sha="${short_sha:0:7}"
    fi

    run_url=''
    if [ -n "${GITHUB_SERVER_URL:-}" ] && [ -n "${GITHUB_REPOSITORY:-}" ] && [ -n "${GITHUB_RUN_ID:-}" ]; then
        run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
    fi

    # The map of markers at the start of the message — the owner's choice by
    # a live Telegram render (T-136): different events get different markers.
    # ⚠️ for the fallback "CI pipeline error" — the main session's decision
    # (it matches none of the owner's four; the semantics is
    # "infrastructure/attention").
    marker=''
    case "$event" in
        'Code error'*)          marker='🟥 ❌' ;;
        'Tests error'*)         marker='🛑' ;;
        'CI pipeline error'*)   marker='⚠️' ;;
        'Deploy succeeded'*)    marker='✅' ;;
        'Deploy failed'*)       marker='⛔️' ;;
        *) # unknown event: the previous status marker
           if [ "$status" = ok ]; then marker='✅'; else marker='❌'; fi ;;
    esac

    head="$workflow"
    if [ -n "$step" ]; then
        head="$head · step: $step"
    fi
    if [ -n "$version" ]; then
        head="$head · deploy version: $version"
    fi

    rollback_line=''
    case "$rollback" in
        performed)           rollback_line='Rollback: performed to the previous version (bin.prev)' ;;
        performed-degraded)  rollback_line='Rollback: performed to the previous version (bin.prev), but not all units are active — an operator is required' ;;
        nothing-to-rollback) rollback_line='Rollback: not performed — there is no previous version (bin.prev is missing, the first deploy); the new version remains on the server' ;;
        failed)              rollback_line='Rollback: failed — the server state needs an operator check (diagnostics in the run log)' ;;
        unknown)             rollback_line='Rollback: unknown — the work on the server was not carried to the end (a lost connection, a run cancellation, or a set-stop failure); check /opt/crawler/bin and systemctl status crawler.target' ;;
    esac

    commit_line="Commit: $short_sha"
    if [ -n "$subject" ]; then
        commit_line="$commit_line — $subject"
    fi

    text="$marker $event"$'\n'"$head"
    if [ -n "$rollback_line" ]; then
        text="$text"$'\n'"$rollback_line"
    fi
    text="$text"$'\n'"$commit_line"
    if [ -n "$run_url" ]; then
        text="$text"$'\n'"$run_url"
    fi
fi

# dry-run: a separate masked payload (the real chat id is not printed on any
# path) and an exit BEFORE the production payload is built — the masked value
# never reaches curl.
if [ "$dry_run" -eq 1 ]; then
    masked_payload="$(jq -cn --arg chat_id '<TELEGRAM_CHAT_ID>' --arg text "$text" \
        '{chat_id: $chat_id, text: $text}')"
    echo 'telegram: dry-run — nothing is sent'
    echo 'telegram: endpoint: https://api.telegram.org/bot<TOKEN>/sendMessage'
    printf 'telegram: payload: %s\n' "$masked_payload"
    exit 0
fi

# The production payload: the real chat id from the secret (it never reaches
# the output).
payload="$(jq -cn --arg chat_id "$TELEGRAM_CHAT_ID" --arg text "$text" \
    '{chat_id: $chat_id, text: $text}')"

# The API response with chat.id (a secret) is not printed; on an error only
# the error_code and description from the body are shown (chat.id is not part
# of error responses). curl without -v, so the token from the URL never
# appears in its diagnostics.
resp="$(mktemp)"
trap 'rm -f "$resp"' EXIT
if ! http_code="$(curl -sS -o "$resp" -w '%{http_code}' \
        --connect-timeout 10 --max-time 30 \
        -H 'Content-Type: application/json' \
        -d "$payload" \
        "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage")"; then
    # A transport error: curl itself prints 000 via -w, and its error
    # message has already gone to stderr.
    [ -n "$http_code" ] || http_code=000
fi
if [ "$http_code" = 200 ]; then
    echo 'telegram: notification sent'
else
    echo "telegram: send error (HTTP $http_code); the run is not failed by it — the step has continue-on-error" >&2
    api_err="$(jq -r '"\(.error_code // "?"): \(.description // "no description")"' \
        "$resp" 2>/dev/null || true)"
    if [ -n "$api_err" ]; then
        echo "telegram: API: $api_err" >&2
    fi
    exit 1
fi

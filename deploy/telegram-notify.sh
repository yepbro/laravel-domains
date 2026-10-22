#!/usr/bin/env bash
# Извещения Telegram о событиях конвейера CI/CD (ТЗ §10.4, T-136).
#
# Один вызов — одно сообщение в выделенную группу: маркер результата в
# начале, тип события, workflow и шаг, виновный коммит, ссылка на ран;
# для деплоя — версия (git describe) и факт отката. Отдельного сервиса
# нет, отправляет сам конвейер: скрипт вызывается последним шагом из
# .github/workflows/ci.yml (провалы) и deploy.yml (успех и провал).
#
# Надёжность (§10.4): сбой отправки не влияет на результат конвейера —
# шаг уведомления в workflow идёт с continue-on-error, а отсутствие
# секретов (PR из форка, репозиторий без настроенных секретов) — штатный
# пропуск с причиной в логе. Значения TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID
# не печатаются: токен не попадает в вывод (curl без -v, ответ API с
# chat.id отбрасывается), xtrace не включается.
#
# Использование:
#   deploy/telegram-notify.sh --status ok|fail --event "Деплой успешен" \
#       --workflow deploy [--step "Установить на сервер"] \
#       [--version "$(git describe --tags --always)"] \
#       [--rollback performed|not-performed|unknown] [--sha <SHA>] \
#       [--dry-run]
#
# Коммит и ссылка на ран берутся из чекаута и GITHUB_*-переменных раннера
# (--sha переопределяет коммит — для PR-ранов, где HEAD это merge-commit);
# локально с --dry-run недостающие строки просто опускаются.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
использование: telegram-notify.sh --status ok|fail --event ТИП --workflow ИМЯ
                         [--step ШАГ] [--version ВЕРСИЯ] [--sha SHA]
                         [--rollback performed|not-performed|unknown] [--dry-run]
EOF
    exit 2
}

status='' event='' workflow='' step='' version='' rollback='' sha='' dry_run=0
while [ $# -gt 0 ]; do
    case "$1" in
        --status)   [ $# -ge 2 ] || usage; status=$2;   shift 2 ;;
        --event)    [ $# -ge 2 ] || usage; event=$2;    shift 2 ;;
        --workflow) [ $# -ge 2 ] || usage; workflow=$2; shift 2 ;;
        --step)     [ $# -ge 2 ] || usage; step=$2;     shift 2 ;;
        --version)  [ $# -ge 2 ] || usage; version=$2;  shift 2 ;;
        --rollback) [ $# -ge 2 ] || usage; rollback=$2; shift 2 ;;
        --sha)      [ $# -ge 2 ] || usage; sha=$2;      shift 2 ;;
        --dry-run)  dry_run=1; shift ;;
        -h|--help)  usage ;;
        *)          echo "telegram-notify: неизвестный аргумент: $1" >&2; usage ;;
    esac
done

if [ -z "$status" ] || [ -z "$event" ] || [ -z "$workflow" ]; then
    usage
fi
case "$status" in
    ok|fail) ;;
    *) echo "telegram-notify: --status должен быть ok или fail" >&2; exit 2 ;;
esac
case "$rollback" in
    performed|not-performed|unknown|'') ;;
    *) echo "telegram-notify: --rollback должен быть performed, not-performed или unknown" >&2; exit 2 ;;
esac

# Guard секретов: без токена/чата извещений нет — выход 0, ран не красится
# (dry-run работает и без секретов, подставляя заглушки).
if [ "$dry_run" -eq 0 ]; then
    if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
        echo 'telegram: извещение пропущено — не задан TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID (PR из форка или секреты репозитория не настроены)'
        exit 0
    fi
fi

# Виновный коммит: --sha (PR-ран: head PR, а не merge-commit), иначе HEAD
# чекаута (в deploy это SHA, протестированный ci); вне git — GITHUB_SHA.
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

# Карта маркеров в начале сообщения — выбор заказчика по живому рендеру
# Telegram (T-136): разным событиям разные маркеры. ⚠️ для фолбэка
# «Ошибка конвейера CI» — решение основной сессии (не совпадает ни с одним
# из четырёх заказчика, семантика «инфраструктура/внимание»).
marker=''
case "$event" in
    'Ошибка в коде'*)       marker='🟥 ❌' ;;
    'Ошибка тестов'*)       marker='🛑' ;;
    'Ошибка конвейера CI'*) marker='⚠️' ;;
    'Деплой успешен'*)      marker='✅' ;;
    'Деплой не успешен'*)   marker='⛔️' ;;
    *) # неизвестное событие: прежний статусный маркер
       if [ "$status" = ok ]; then marker='✅'; else marker='❌'; fi ;;
esac

head="$workflow"
if [ -n "$step" ]; then
    head="$head · шаг: $step"
fi
if [ -n "$version" ]; then
    head="$head · версия деплоя: $version"
fi

rollback_line=''
case "$rollback" in
    performed)     rollback_line='Откат: выполнен на предыдущую версию (bin.prev)' ;;
    not-performed) rollback_line='Откат: не выполнен — на сервере осталась новая версия' ;;
    unknown)       rollback_line='Откат: неизвестен — установка прервана (связь потеряна или ран отменён); проверьте /opt/crawler/bin и systemctl status crawler.target' ;;
esac

commit_line="Коммит: $short_sha"
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

# dry-run: отдельный маскированный payload (реальный chat id не печатается
# ни в одном пути) и выход ДО построения боевого payload — маскированное
# значение никогда не доходит до curl.
if [ "$dry_run" -eq 1 ]; then
    masked_payload="$(jq -cn --arg chat_id '<TELEGRAM_CHAT_ID>' --arg text "$text" \
        '{chat_id: $chat_id, text: $text}')"
    echo 'telegram: dry-run — отправки нет'
    echo 'telegram: эндпоинт: https://api.telegram.org/bot<TOKEN>/sendMessage'
    printf 'telegram: payload: %s\n' "$masked_payload"
    exit 0
fi

# Боевой payload: реальный chat id из секрета (в вывод не попадает).
payload="$(jq -cn --arg chat_id "$TELEGRAM_CHAT_ID" --arg text "$text" \
    '{chat_id: $chat_id, text: $text}')"

# Ответ API с chat.id (секрет) не печатаем; при ошибке показываем только
# error_code и description из тела (chat.id в ошибочные ответы не входит).
# curl без -v, поэтому токен из URL в его диагностике не появляется.
resp="$(mktemp)"
trap 'rm -f "$resp"' EXIT
if ! http_code="$(curl -sS -o "$resp" -w '%{http_code}' \
        --connect-timeout 10 --max-time 30 \
        -H 'Content-Type: application/json' \
        -d "$payload" \
        "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage")"; then
    # Транспортная ошибка: curl сам выводит 000 через -w, его сообщение
    # об ошибке уже ушло в stderr.
    [ -n "$http_code" ] || http_code=000
fi
if [ "$http_code" = 200 ]; then
    echo 'telegram: извещение отправлено'
else
    echo "telegram: ошибка отправки (HTTP $http_code); ран это не красит — шаг с continue-on-error" >&2
    api_err="$(jq -r '"\(.error_code // "?"): \(.description // "нет описания")"' \
        "$resp" 2>/dev/null || true)"
    if [ -n "$api_err" ]; then
        echo "telegram: API: $api_err" >&2
    fi
    exit 1
fi

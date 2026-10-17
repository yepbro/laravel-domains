#!/bin/sh
# Обработчик предупреждений smartd для Domain Crawler (T-089, ТЗ §10.3).
#
# Вызывается smartd через /usr/share/smartmontools/smartd_warning.sh при
# срабатывании любой проверяемой директивы (-H, -l error, -W; конфиг —
# deploy/smartd/smartd.conf). Аргументов и stdin нет (-m <nomailer>),
# данные — в переменных окружения SMARTD_* (man smartd.conf(5), «-M exec»).
#
# Задача одна: записать предупреждение в syslog/journald — в ту же ленту,
# куда пишут все сервисы краулера (docs/deploy.md, T-085); читает оператор:
#   journalctl -t smartd -p warning
# Ничего не печатать в stdout/stderr: smartd трактует вывод как сбой
# обработчика и дублирует обрывки в syslog.

set -u

# Приоритет по типу события (SMARTD_FAILTYPE): здоровье/температура — crit,
# тестовая запись — info, остальное (рост журнала ошибок и т.п.) — warning.
prio=daemon.warning
case "${SMARTD_FAILTYPE:-}" in
Health | Temperature) prio=daemon.crit ;;
EmailTest) prio=daemon.info ;;
esac

logger -t smartd -p "$prio" \
    "SMART: ${SMARTD_DEVICESTRING:-${SMARTD_DEVICE:-устройство}}:" \
    "${SMARTD_MESSAGE:-причина неизвестна};" \
    "первое срабатывание: ${SMARTD_TFIRST:-?};" \
    "предыдущих сообщений: ${SMARTD_PREVCNT:-0}"

exit 0

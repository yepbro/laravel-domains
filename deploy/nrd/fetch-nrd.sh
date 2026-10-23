#!/bin/bash
# Ежедневная выгрузка списка новых доменов (NRD) smet.cz в наблюдаемую
# папку ingest: deploy/nrd/README.md, docs/deploy.md §4.9.
#
# Источник https://smet.cz/nrd/data/today.txt обновляется ежедневно
# (~20:19 UTC); запуск в 19:17 UTC забирает срез предыдущих суток —
# между двумя запусками ровно один срез, дублей по sha256 не бывает.
#
# Файл зона-less (домены разных зон в одном списке): кладётся в
# <incoming>/add/, зоны строк выводятся из самих доменов
# (docs/incoming-files.md §1). Доставка — по контракту «Готовность
# файла» (§2): запись под временным именем *.part (наблюдатель его
# игнорирует) + атомарное переименование в *.txt.
#
# Вызов: cron /etc/cron.d/crawler-nrd (root) или вручную. Пути
# переопределяются окружением — тестируемость без root и /var/lib.
# Логи: journalctl -t crawler-nrd-fetch (плюс stderr — для ручного
# запуска и писем cron).
set -euo pipefail

url="https://smet.cz/nrd/data/today.txt"
incoming="${CRAWLER_NRD_INCOMING:-/var/lib/crawler/incoming}"
owner="${CRAWLER_NRD_OWNER:-crawler-ingest}"
group="${CRAWLER_NRD_GROUP:-crawler}"

tag="crawler-nrd-fetch"
log() {
    logger -t "$tag" -- "$*" 2>/dev/null || true
    echo "$tag: $*" >&2
}

dest_dir="$incoming/add"
dest="$dest_dir/nrd-$(date -u +%F).txt"
part="$dest.part"

mkdir -p "$dest_dir"
# прибраться за прерванными загрузками прошлых суток (старше 24 ч)
find "$dest_dir" -maxdepth 1 -type f -name 'nrd-*.txt.part' -mmin +1440 -print -delete || true

# --fail: HTTP 4xx/5xx — ошибка, а не HTML-страница в файл;
# --retry: повторные попытки на обрывы и 5xx Cloudflare.
if ! curl -fsS --connect-timeout 30 --max-time 300 --retry 3 --retry-delay 15 \
        -o "$part" "$url"; then
    rm -f "$part"
    log "СБОЙ: выгрузка $url не удалась (сеть/HTTP)"
    exit 1
fi

if [ ! -s "$part" ]; then
    rm -f "$part"
    log "СБОЙ: $url вернул пустой файл"
    exit 1
fi

# владелец/права — как у ручной доставки оператора; при ручном запуске
# не-root chown/chmod могут не пройти — файл и тогда читаем сервису
# (0644 от umask), доставка не срывается
chown "$owner:$group" "$part" 2>/dev/null || true
chmod 0640 "$part" 2>/dev/null || true

# атомарно: наблюдатель видит только завершённый *.txt
mv -f "$part" "$dest"

size="$(wc -c <"$dest")"
lines="$(wc -l <"$dest")"
sha="$(sha256sum "$dest" | cut -d' ' -f1)"
log "OK: $url -> $dest ($size байт, $lines строк, sha256 $sha); обработает crawler-ingest"

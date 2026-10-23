#!/bin/bash
# Ежедневная выгрузка списка новых доменов (NRD) smet.cz в наблюдаемую
# папку ingest: deploy/nrd/README.md, docs/deploy.md §4.9.
#
# Источник публикует дни immutable-файлами /nrd/data/daily/<дата>.txt[.gz]
# («файл дня растёт до закрытия дня, затем не меняется»; справка —
# https://smet.cz/nrd/). Берём **закрытый** день — файл вчерашней (UTC)
# даты: на момент запуска 20:33 UTC следующего дня он закрыт (генерация
# источника ~20:10–20:19 UTC, запас > 20 ч) и больше не изменится. Файл
# текущего дня до ~20:19 UTC отсутствует (404), а сразу после публикации
# ещё растёт — гарантию полного дня не даёт, не используется. Справка
# источника сама рекомендует забирать только закрытые дни.
#
# Формат .txt.gz (~2,6 МиБ вместо ~8 МиБ): ingest принимает .gz-архивы
# (docs/incoming-files.md §9: .gz — один член). Список зона-less —
# кладётся в <incoming>/add/, зоны строк выводятся из самих доменов
# (§1). Доставка — по контракту «Готовность файла» (§2): запись под
# временным именем *.part (наблюдатель его игнорирует) + атомарное
# переименование в *.txt.gz.
#
# Вызов: cron /etc/cron.d/crawler-nrd (root) или вручную. Пути
# переопределяются окружением — тестируемость без root и /var/lib.
# Логи: journalctl -t crawler-nrd-fetch (плюс stderr — для ручного
# запуска и писем cron).
set -euo pipefail

base="${CRAWLER_NRD_BASE:-https://smet.cz/nrd/data/daily}"
day="${CRAWLER_NRD_DAY:-$(date -u -d yesterday +%F)}"
incoming="${CRAWLER_NRD_INCOMING:-/var/lib/crawler/incoming}"
owner="${CRAWLER_NRD_OWNER:-crawler-ingest}"
group="${CRAWLER_NRD_GROUP:-crawler}"

url="$base/$day.txt.gz"
tag="crawler-nrd-fetch"
log() {
    logger -t "$tag" -- "$*" 2>/dev/null || true
    echo "$tag: $*" >&2
}

dest_dir="$incoming/add"
dest="$dest_dir/nrd-$day.txt.gz"
part="$dest.part"

mkdir -p "$dest_dir"
# прибраться за прерванными загрузками прошлых суток (старше 24 ч)
find "$dest_dir" -maxdepth 1 -type f -name 'nrd-*.part' -mmin +1440 -print -delete || true

# --fail: HTTP 4xx/5xx — ошибка, а не HTML-страница в файл;
# --retry: повторные попытки на обрывы и 5xx CDN.
if ! curl -fsS --connect-timeout 30 --max-time 300 --retry 3 --retry-delay 15 \
        -o "$part" "$url"; then
    rm -f "$part"
    log "СБОЙ: выгрузка $url не удалась (сеть/HTTP)"
    exit 1
fi

# санитарные проверки: непустой файл и gzip-магика 1f 8b — отсекают
# HTML-заглушку с HTTP 200 и пустой/обрезанный ответ
if [ ! -s "$part" ] || [ "$(od -An -N2 -tx1 "$part" | tr -d ' \n')" != "1f8b" ]; then
    rm -f "$part"
    log "СБОЙ: $url вернул не gzip-файл (пустой/битый)"
    exit 1
fi

# владелец/права — как у ручной доставки оператора; при ручном запуске
# не-root chown/chmod могут не пройти — файл и тогда читаем сервису
# (0644 от umask), доставка не срывается
chown "$owner:$group" "$part" 2>/dev/null || true
chmod 0640 "$part" 2>/dev/null || true

# атомарно: наблюдатель видит только завершённый *.txt.gz
mv -f "$part" "$dest"

size="$(wc -c <"$dest")"
sha="$(sha256sum "$dest" | cut -d' ' -f1)"
log "OK: день $day закрыт, $url -> $dest ($size байт, sha256 $sha); обработает crawler-ingest"

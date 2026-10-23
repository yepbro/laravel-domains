#!/bin/bash
# Бэкфилл исторических закрытых дней NRD smet.cz в наблюдаемую папку
# ingest: deploy/nrd/README.md, docs/deploy.md §4.9.
#
# Читает /nrd/data/manifest.json и доставляет в <ingest.dir>/add/ файлы
# всех дней строго раньше NRD_BEFORE (YYYY-MM-DD) тем же контрактом, что
# ежедневный fetch-nrd.sh: закрытый день /daily/<дата>.txt.gz, запись под
# временным именем *.part (наблюдатель игнорирует) + атомарное
# переименование, санитарная проверка gzip-магики, владелец
# crawler-ingest:crawler 0640. Дни у источника immutable и хранятся 90
# дней; между запросами пауза 2 с — рекомендация справки источника для
# исторических файлов.
#
# Идемпотентен: день, уже доставлявшийся (файл есть в add/ или в
# processed/add/), пропускается — повторный запуск безопасен и доливает
# только недостающее. Обработка — фоновым порядком ingest (~1 мин/день),
# скрипт только доставляет.
#
# Запуск на сервере под root (chown); на сервер не устанавливается:
#   ssh root@<сервер> 'NRD_BEFORE=2026-09-21 bash -s' -- < deploy/nrd/backfill-nrd.sh
# либо scp + запуск + удаление. Итог — в journald, тег crawler-nrd-backfill.
set -euo pipefail

before="${NRD_BEFORE:-}"
[ -n "$before" ] || { echo "укажите границу: NRD_BEFORE=YYYY-MM-DD — дни строго раньше неё" >&2; exit 2; }
[[ "$before" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "NRD_BEFORE должна быть датой YYYY-MM-DD" >&2; exit 2; }
[ "$(id -u)" -eq 0 ] || { echo "запускайте от root (доставка требует chown на crawler-ingest)" >&2; exit 2; }

base="${NRD_BASE:-https://smet.cz/nrd/data/daily}"
incoming="${CRAWLER_NRD_INCOMING:-/var/lib/crawler/incoming}"
owner="${CRAWLER_NRD_OWNER:-crawler-ingest}"
group="${CRAWLER_NRD_GROUP:-crawler}"

manifest="$(mktemp /tmp/nrd-manifest.XXXXXX)"
trap 'rm -f "$manifest"' EXIT

curl -fsS --connect-timeout 30 --max-time 120 --retry 3 -o "$manifest" \
    https://smet.cz/nrd/data/manifest.json

# дни строго раньше $before, по возрастанию
mapfile -t days < <(grep -oE '"date":"[0-9]{4}-[0-9]{2}-[0-9]{2}"' "$manifest" \
    | cut -d'"' -f4 | sort -u | awk -v b="$before" '$0 < b')
if [ "${#days[@]}" -eq 0 ]; then
    echo "дней раньше $before в manifest нет"
    exit 0
fi

echo "дней к доставке: ${#days[@]} (${days[0]} … ${days[-1]})"

ok=0; skip=0; fail=0
for day in "${days[@]}"; do
    url="$base/$day.txt.gz"
    dest="$incoming/add/nrd-$day.txt.gz"
    part="$dest.part"
    # идемпотентный перезапуск: день уже доставлялся — пропустить
    if [ -e "$dest" ] || [ -e "$incoming/processed/add/nrd-$day.txt.gz" ]; then
        echo "SKIP $day (уже доставлялся)"
        skip=$((skip+1))
        continue
    fi
    # --fail: HTTP 4xx/5xx — ошибка, а не HTML-страница в файл;
    # магика 1f 8b отсекает не-gzip ответ с HTTP 200 и пустой файл
    if ! curl -fsS --connect-timeout 30 --max-time 300 --retry 3 --retry-delay 15 \
            -o "$part" "$url" \
        || [ ! -s "$part" ] \
        || [ "$(od -An -N2 -tx1 "$part" | tr -d ' \n')" != "1f8b" ]; then
        rm -f "$part"
        echo "FAIL $day ($url)"
        fail=$((fail+1))
        continue
    fi
    chown "$owner:$group" "$part"
    chmod 0640 "$part"
    mv -f "$part" "$dest"   # атомарно: наблюдатель видит завершённый файл
    echo "OK $day ($(wc -c <"$dest") байт)"
    ok=$((ok+1))
    sleep 2
done

echo "итог: ok=$ok skip=$skip fail=$fail из ${#days[@]}"
logger -t crawler-nrd-backfill -- "бэкфилл NRD (<$before): ok=$ok skip=$skip fail=$fail из ${#days[@]}"
[ "$fail" -eq 0 ]

#!/bin/bash
# Backfill of historical closed NRD days of smet.cz into the watched ingest
# directory: deploy/nrd/README.md, docs/deploy.md §4.9.
#
# Reads /nrd/data/manifest.json and delivers the files of all days strictly
# earlier than NRD_BEFORE (YYYY-MM-DD) to <ingest.dir>/add/ under the same
# contract as the daily fetch-nrd.sh: a closed day /daily/<date>.txt.gz,
# written under the temporary *.part name (ignored by the watcher) + an
# atomic rename, a gzip-magic sanity check, the owner
# crawler-ingest:crawler 0640. Days at the source are immutable and kept for
# 90 days; a 2 s pause between requests — the source documentation's
# recommendation for historical files.
#
# Idempotent: a day already delivered (the file exists in add/ or in
# processed/add/) is skipped — rerunning is safe and only fills in what is
# missing. Processing happens in the background ingest order (~1 min/day);
# the script only delivers.
#
# Run on the server as root (chown); not installed on the server:
#   ssh root@<server> 'NRD_BEFORE=2026-09-21 bash -s' -- < deploy/nrd/backfill-nrd.sh
# or scp + run + delete. The result goes to journald, the crawler-nrd-backfill tag.
set -euo pipefail

before="${NRD_BEFORE:-}"
[ -n "$before" ] || { echo "specify the boundary: NRD_BEFORE=YYYY-MM-DD — days strictly earlier than it" >&2; exit 2; }
[[ "$before" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "NRD_BEFORE must be a YYYY-MM-DD date" >&2; exit 2; }
[ "$(id -u)" -eq 0 ] || { echo "run as root (delivery requires chown to crawler-ingest)" >&2; exit 2; }

base="${NRD_BASE:-https://smet.cz/nrd/data/daily}"
incoming="${CRAWLER_NRD_INCOMING:-/var/lib/crawler/incoming}"
owner="${CRAWLER_NRD_OWNER:-crawler-ingest}"
group="${CRAWLER_NRD_GROUP:-crawler}"

manifest="$(mktemp /tmp/nrd-manifest.XXXXXX)"
trap 'rm -f "$manifest"' EXIT

curl -fsS --connect-timeout 30 --max-time 120 --retry 3 -o "$manifest" \
    https://smet.cz/nrd/data/manifest.json

# days strictly earlier than $before, ascending
mapfile -t days < <(grep -oE '"date":"[0-9]{4}-[0-9]{2}-[0-9]{2}"' "$manifest" \
    | cut -d'"' -f4 | sort -u | awk -v b="$before" '$0 < b')
if [ "${#days[@]}" -eq 0 ]; then
    echo "no days earlier than $before in the manifest"
    exit 0
fi

echo "days to deliver: ${#days[@]} (${days[0]} … ${days[-1]})"

ok=0; skip=0; fail=0
for day in "${days[@]}"; do
    url="$base/$day.txt.gz"
    dest="$incoming/add/nrd-$day.txt.gz"
    part="$dest.part"
    # idempotent rerun: the day was already delivered — skip
    if [ -e "$dest" ] || [ -e "$incoming/processed/add/nrd-$day.txt.gz" ]; then
        echo "SKIP $day (already delivered)"
        skip=$((skip+1))
        continue
    fi
    # --fail: HTTP 4xx/5xx is an error, not an HTML page in the file;
    # the 1f 8b magic cuts off a non-gzip response with HTTP 200 and an empty file
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
    mv -f "$part" "$dest"   # atomic: the watcher sees a complete file
    echo "OK $day ($(wc -c <"$dest") bytes)"
    ok=$((ok+1))
    sleep 2
done

echo "summary: ok=$ok skip=$skip fail=$fail out of ${#days[@]}"
logger -t crawler-nrd-backfill -- "NRD backfill (<$before): ok=$ok skip=$skip fail=$fail out of ${#days[@]}"
[ "$fail" -eq 0 ]

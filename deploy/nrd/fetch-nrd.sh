#!/bin/bash
# Daily fetch of the new-domains list (NRD) of smet.cz into the watched
# ingest directory: deploy/nrd/README.md, docs/deploy.md §4.9.
#
# The source publishes days as immutable /nrd/data/daily/<date>.txt[.gz]
# files ("a day's file grows until the day closes, then never changes";
# docs — https://smet.cz/nrd/). We take the **closed** day — the file of
# yesterday's (UTC) date: at the 05:33 UTC run time of the next day it is
# closed (the source generates ~20:10–20:19 UTC, a > 9 h margin) and will
# not change again. The current day's file is missing until ~20:19 UTC
# (404), and right after publication it still grows — it gives no full-day
# guarantee and is not used. The source documentation itself recommends
# fetching only closed days.
#
# The .txt.gz format (~2.6 MiB instead of ~8 MiB): ingest accepts .gz
# archives (docs/incoming-files.md §9: .gz — a single member). The list is
# zone-less — it goes to <incoming>/add/, the zones of the rows are derived
# from the domains themselves (§1). Delivery follows the "file readiness"
# contract (§2): writing under the temporary *.part name (ignored by the
# watcher) + an atomic rename to *.txt.gz.
#
# Invoked by cron /etc/cron.d/crawler-nrd (root) or manually. The paths are
# overridden by the environment — testable without root and /var/lib.
# Logs: journalctl -t crawler-nrd-fetch (plus stderr — for manual runs
# and cron mail).
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
# clean up downloads interrupted over the past day (older than 24 h)
find "$dest_dir" -maxdepth 1 -type f -name 'nrd-*.part' -mmin +1440 -print -delete || true

# --fail: HTTP 4xx/5xx is an error, not an HTML page in the file;
# --retry: retries on disconnects and CDN 5xx.
if ! curl -fsS --connect-timeout 30 --max-time 300 --retry 3 --retry-delay 15 \
        -o "$part" "$url"; then
    rm -f "$part"
    log "FAILURE: fetch of $url failed (network/HTTP)"
    exit 1
fi

# sanity checks: a non-empty file and the 1f 8b gzip magic — they cut off
# an HTML stub with HTTP 200 and an empty/truncated response
if [ ! -s "$part" ] || [ "$(od -An -N2 -tx1 "$part" | tr -d ' \n')" != "1f8b" ]; then
    rm -f "$part"
    log "FAILURE: $url returned a non-gzip file (empty/broken)"
    exit 1
fi

# owner/permissions — like the operator's manual delivery; on a manual
# non-root run chown/chmod may fail — the file stays readable to the
# service anyway (0644 from the umask), the delivery does not break
chown "$owner:$group" "$part" 2>/dev/null || true
chmod 0640 "$part" 2>/dev/null || true

# atomic: the watcher sees only a complete *.txt.gz
mv -f "$part" "$dest"

size="$(wc -c <"$dest")"
sha="$(sha256sum "$dest" | cut -d' ' -f1)"
log "OK: day $day closed, $url -> $dest ($size bytes, sha256 $sha); crawler-ingest will process it"

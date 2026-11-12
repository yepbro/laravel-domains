# The daily incoming NRD feed from smet.cz (cron)

Automatic feeding of the new-domain list into the pipeline: once a day,
cron downloads the smet.cz new-domain list into the watched ingest
directory — the file appears in `<ingest.dir>/add/`, and the
`crawler-ingest` service then processes it as a regular delivery
([`docs/incoming-files.md`](../../docs/incoming-files.md)).

The list is **zone-less**: domains of different zones (1807 zones per the
source's documentation) in one file; the zone of each line is the last
label of the name, the TLD (`internal/idn`, the tld-only-zones spec);
missing zones are created automatically (docs/incoming-files.md §1) —
there is no need to split by zone directories.

| File | Installed to | Purpose |
|---|---|---|
| `fetch-nrd.sh` | `/usr/local/bin/crawler-fetch-nrd` (0755) | the fetch script: curl → `add/nrd-<date>.txt.gz.part` → an atomic rename to `.txt.gz` |
| `crawler-nrd.cron` | `/etc/cron.d/crawler-nrd` (0644) | runs daily at **05:33 UTC** |
| `backfill-nrd.sh` | — (run from a checkout, not installed on the server) | history backfill: all closed days strictly before `NRD_BEFORE`, per manifest.json |

## A closed day instead of today.txt

The source (its documentation — <https://smet.cz/nrd/>) publishes each day
as a separate immutable file `/nrd/data/daily/<date>.txt[.gz]`: a day's
file "grows until the day closes, then never changes", kept for 90 days;
`today.txt` is just a mutating alias of the current day. The script takes
the file of **yesterday's (UTC) date** — a closed day:

- the source generates a day at ~20:10–20:19 UTC; the current day's file
  is absent (404) until then, and right after that it is still growing
  and does not give the complete day;
- at the 05:33 UTC run time, yesterday's (UTC) file has been closed for
  more than 9 hours (generation at 20:10–20:19 UTC of the previous
  calendar day) and will not change again — which day was downloaded is
  known from the name, and the content is guaranteed complete (the
  source's own documentation recommends taking only closed days);
- a day is dated in UTC; the file in the `processed/` archive documents
  itself: `nrd-2026-10-22.txt.gz` = the registrations of the day
  2026-10-22.

The format is `.txt.gz` (~2.6 MiB instead of ~8 MiB): ingest accepts
`.gz` (docs/incoming-files.md §9; a `.gz` — a single member);
performance on this production is already proven by `30d.txt.gz`
(12.3M lines, Oct 22).

## Installation

```bash
install -m 0755 fetch-nrd.sh /usr/local/bin/crawler-fetch-nrd
install -m 0644 crawler-nrd.cron /etc/cron.d/crawler-nrd
```

The dependencies — `curl` and `cron` (OS packages, present on the
production server). Root permissions: the file is placed by the owner
`crawler-ingest:crawler` with 0640, like the operator's manual delivery.

Check it right now (without waiting for the schedule):

```bash
/usr/local/bin/crawler-fetch-nrd
journalctl -t crawler-nrd-fetch --since -1d    # the line "OK: day … closed, … sha256 …"
```

From there it is the usual ingest lifecycle: the file goes to
`incoming/processed/add/`, the domains go to the DB, and a row goes into
the `ingest_files` journal (the sha256 from the log = the sha256 of the
`.gz` bytes, as for all archives of §9); the zones are visible in
`crawlerctl zones list`.

## Behavior and failures

- **The schedule is 05:33 UTC** (moved from 20:33 UTC on Oct 26, T-205:
  applying the daily file — a single INSERT…SELECT over ~470k domains —
  landed in the evening CPU-starvation window (PSI ~80%) and the midnight
  storm of the daily re-check; the morning is outside the starvation
  hours, the source's day has been closed for >9 h, and the closed-day
  contract is preserved). Cron cannot express a time zone in its line: in
  `crawler-nrd.cron` the hours are converted to the server's local time
  (Europe/Moscow, UTC+3 → `33 8 * * *`); when the server's zone changes
  (`timedatectl`), convert them back to 05:33 UTC.
- **Network/HTTP failure** — `curl --fail --retry 3`; the file is not
  placed at all (a partially downloaded `.part` is removed), a
  `FAILURE: …` entry goes to `journalctl -t crawler-nrd-fetch`, exit
  code 1 — no further attempts on the same day. Unlike `today.txt`, a day
  in `/daily/` is kept for 90 days: a missed run is recovered by
  re-delivery — `/usr/local/bin/crawler-fetch-nrd` by hand (the same
  closed day) or with a date specified:
  `CRAWLER_NRD_DAY=2026-10-22 /usr/local/bin/crawler-fetch-nrd`.
- **A non-gzip response** (an HTML stub with HTTP 200, an empty/truncated
  file) is rejected by the sanity check of the gzip magic — the file is
  not placed, `FAILURE` to the log.
- **A re-run on the same day** is harmless: the closed day's URL and
  content are unchanged → the same `sha256` → the `ingest_files` dedup,
  no re-processing (docs/incoming-files.md §5).
- Junk lines inside the list are filtered by ingest itself (the
  `ingest.max_error_ratio` threshold, docs/incoming-files.md §4).

## History backfill (backfill-nrd.sh)

A bulk top-up of history or missed runs: it reads `manifest.json` and
delivers to `add/` all closed days **strictly before `NRD_BEFORE`**
(the source's days are immutable, kept for 90 days). It is idempotent —
days already delivered (a file in `add/` or `processed/add/`) are
skipped; a re-run tops up only the missing ones. The delivery contract is
the same as `fetch-nrd.sh`'s; a 2 s pause between requests (the source's
recommendation for historical files); processing happens in ingest's
background order (~1 min/day), the script only delivers. The result —
`journalctl -t crawler-nrd-backfill`.

Run on the server as root; not installed on the server:

```bash
ssh root@<server> 'NRD_BEFORE=2026-09-21 bash -s' -- < deploy/nrd/backfill-nrd.sh
# a single day, point-wise — via the regular feed, without the root script:
CRAWLER_NRD_DAY=2026-09-21 /usr/local/bin/crawler-fetch-nrd
```

Applied 2026-10-23: `NRD_BEFORE=2026-09-21` (59 days,
2026-07-24…Sep 20) and `NRD_BEFORE=2026-09-23` (topping up the hole of
Sep 21–22, the other 59 SKIP) — the history is continuous from
2026-07-24.

## Removal

```bash
rm /etc/cron.d/crawler-nrd /usr/local/bin/crawler-fetch-nrd
```

Already processed files/domains remain in the DB — if needed, purge the
list by delivering to `delete/` (docs/incoming-files.md §6).

# Incoming domain files: the contract (spec 5.1–5.2)

A single description of incoming files for the operator and integrations:
the structure of the watched folder, the file format, the line validation
rules, the reports and the lifecycle. The implementation is the
`internal/ingest` pipeline (T-013…T-023); the parameters are the `ingest`
section of the configuration
([`configuration.md`](configuration.md)).

## 1. The watched folder structure

```
<ingest.dir>/
  <tld>/                   — files of one zone (the zone is set by the directory;
    add/       *.txt, §9 archives — add or update domains
    delete/    *.txt, §9 archives — remove domains (ones registered earlier)
  add/         *.txt, §9 archives — a zone-less file: domains of any zones
  delete/                   in one file, the zone is derived from the name
  processed/            — the archive of successfully processed files
  failed/               — bad files with a *.report log report nearby
```

The zone directory is a TLD (`incoming/<tld>/`); a testbed test zone may be
multi-level — only from the allowlist, §1.1.

- The directory is `ingest.dir` (the default `/data/incoming`;
  the dev environment — `var/incoming/` at the repository root, production —
  `/var/lib/crawler/incoming`, permissions — [`deploy.md`](deploy.md) §3–4).
  A missing directory is created by the service at startup.
- A **zone-less file** (`add/<file>` without a zone directory) may mix
  zones: the zone of each line is the name's last label, the TLD
  (`internal/idn`, the tld-only-zones spec §4); zones are created
  automatically from the file's first lines. Convenient for "as is" lists
  (manual checks, exports without splitting). A subtlety: a full TLD dump
  is accepted as a whole — composite zone names become a composite label
  (`example.5g.in` in `add/` → the zone `in`, the label `example.5g`);
  for testbed test zones (`example.crawl.test`) a zone-less submission
  would give the zone `test` — use the zone directory
  (`crawl.test/add/…`, the allowlist §1.1). The daily NRD feed of smet.cz
  is delivered the same way — automatically, by a cron export into
  `add/` ([`deploy/nrd/`](../deploy/nrd/), [`deploy.md`](deploy.md) §4.9).
- The **zone** is the directory name: a TLD (`com`, `uk`, `xn--p1ai`);
  unicode or punycode is allowed (`москва` and `xn--80adxhks` are one
  zone, there will be no duplicates in the reference table). A directory
  whose name has a dot (a multi-level zone) — only a test zone from the
  allowlist `ingest.allowed_zone_paths` (§1.1); other multi-level
  directories are a file-level error (`failed/`, lines are not parsed).
  A new zone is created in `zones` automatically at the first file
  (`is_active = true`); the name is stored in punycode, the unicode form
  is for display only.
- The **operation** is strictly a lowercase `add` or `delete` directory;
  extra nesting (`<zone>/add/sub/x.txt`, `add/sub/x.txt`) is a path
  structure error.
- The extension is `*.txt` or an archive of a supported format (§9),
  case-insensitive (`.TXT` is taken); the `processed/` and `failed/`
  directories are not scanned by the watcher.

### 1.1. The allowlist of multi-level directories (`ingest.allowed_zone_paths`)

A zone's working directory is a TLD: `incoming/<tld>/add|delete/`. A full
TLD dump is accepted as one file without splitting by registration
levels: `example.5g.in` and `example.co.uk` in `incoming/in/` and
`incoming/uk/` are regular lines with a composite label
(`example.5g`, `example.co`).

A multi-level directory — a name with a dot (`incoming/co.uk/`) — is
allowed only for zones from the `ingest.allowed_zone_paths` allowlist
([`configuration.md`](configuration.md), the `ingest` section). Its
purpose is testbed test zones (`crawl.test`) that TLD semantics forbids
from being a "path zone", while the bench must check them as separate
zones: `example.crawl.test` in `incoming/crawl.test/` → the zone
`crawl.test`, the label `example`. The production value is empty. A file
in a multi-level directory outside the list is a file-level error: the
watcher moves it to `failed/` with the reason in the log report (the
T-021 semantics); lines are not parsed.

## 2. The file format

| What | Rule |
|---|---|
| Encoding | UTF-8; a line with invalid UTF-8 is rejected (`invalid_utf8`), parsing continues |
| Line | a **full domain name** (`example.com`) in any form: case, edge spaces, a trailing dot, unicode/punycode are normalized (spec 5.2, `internal/idn`); or an arbitrary record among whose extra data a domain is extracted (T-132 — see below) |
| Line endings | `\n`, `\r\n`, `\r` — mixing within one file is allowed |
| Empty lines | skipped (not a rejection) |
| Comments | a line whose first significant character is `#` (after edge spaces) is skipped |
| BOM | stripped at the start of the file; in the middle it stays in the data and is removed by name normalization |
| Line length | up to 1024 bytes; longer — a per-line `line_len` rejection |
| File size | unlimited: parsing is streaming, memory does not depend on the size; loading into the DB in batches of `ingest.batch_size` (50000) |

### Extracting a domain from an arbitrary line (T-132)

Real registrar exports carry extra data in a line:
`xxx.com 2029-11-11 TIMEWEB`, CSV, URLs. A line **without separators**
(whitespace characters, comma, semicolon) and without an explicit
`http(s)://` scheme is still one full domain name: it is validated by
the usual rules (§3), the rejection reasons of single lines have not
changed. Otherwise the line is split into tokens:

- the separators are whitespace characters (`unicode.IsSpace`), `,`, `;`;
- quotes and punctuation are stripped from the token's edges:
  `"xxx.com",` → `xxx.com`;
- a token with an explicit `http(s)://` scheme (case does not matter) is
  replaced by the host from `net/url` — the port and path are cut off:
  `https://xxx.com/page` → `xxx.com`; other schemes (`ftp://…`) are not
  replaced;
- the candidates are tokens containing an ASCII dot `.`: dates
  `2029-11-11` and dotless words are not candidates. Ideographic dots
  (`。`/`．`) are understood by normalization in single lines; in mixed
  ones only an ASCII dot makes a candidate — a known limitation;
- the candidates are scanned left to right through full validation
  (spec 5.2 normalization + a check against the path's zone): the first
  one that passes becomes the line's record; if the next valid candidate
  turns out to be from a foreign zone — the next one is taken
  (`yyy.org xxx.com` in the `com` zone directory → `xxx.com`);
- none passed — a `no_domain` rejection (§3) into the `rejected` log and
  the report's reason breakdown.

A consequence: a space inside a line is now a field separator. Typos
like "при мер.рф" no longer glue into one name: the token with a dot
(`мер.рф`) is extracted from such a line. Name normalization
(spec 5.2 items 1–6) has not changed — extraction only chooses which
token of the line enters the pipeline. There is no configuration flag:
the old format is a subset of the new one, ingest only widens; the line
limit (1024 bytes) is the same — dump lines are short.

**File readiness.** Only a file whose writing is complete is processed:
its size and mtime stay stable for `ingest.stable_seconds` seconds
(10 by default), plus a periodic full folder walk every
`ingest.scan_interval` (a safety net for fsnotify; it picks up files
that existed before the start). The right way to deliver is to write
under a temporary name (`*.tmp`, `*.part` — the watcher ignores them)
and rename to `*.txt`.

## 3. Line validation and rejection reasons

A line (for mixed lines — the extracted candidate token, T-132, §2) must
be a full domain name from the zone given in the file's path.
Normalization per spec 5.2: lowercase, stripping spaces and the trailing
dot, punycode (the UTS-46 Lookup profile, invalid `xn--` labels are
rejected), length checks (a label ≤ 63, a name ≤ 253 octets), zone
separation — the name's last label (the TLD; the tld-only-zones spec
§4). A zone cannot consist of digits only — such TLDs do not exist;
export dates and IPv4 literals are rejected (`numeric_zone`). The check
against the path's zone: the TLD matched — a composite label is accepted
(`example.5g.in` in `incoming/in/` → the label `example.5g`); a name
deeper than the path's zone — only an allowlisted test zone
(`example.crawl.test` in `incoming/crawl.test/` → the label `example`),
otherwise `wrong_zone`. For a zone-less file (`add/<file>`) the line's
zone is the separated TLD — no check against the path's zone is
performed, `wrong_zone` is impossible.

Rejected lines **do not fail the file**: they are written to the
`rejected` log (records with the `service=rejected` attribute on the
ingest service's stderr: the file, the line number, the truncated line,
the reason) and into the report counters.

| Reason | Meaning | Example |
|---|---|---|
| `invalid_utf8` | the line is not UTF-8 | the bytes `\xff\xfe` |
| `line_len` | the line is longer than 1024 bytes | a junk line |
| `empty` | after normalization the name is empty | a line with a single dot |
| `invalid` | an invalid name / broken punycode | `ex_ample.com`, `xn--e` |
| `label_len` | a label longer than 63 octets | a 64-character label |
| `name_len` | the full name is longer than 253 octets | a chain of labels |
| `no_label` | no domain label | a bare zone `com` |
| `numeric_zone` | the name's zone (the last label) consists of digits only — numeric TLDs do not exist | an export date `18.06.1999`, an IPv4 literal `1.2.3.4` |
| `wrong_zone` | the domain is not from the path's zone (only files with a zone directory) | `example.com` in a `ru/add/` file; `shop.co.test` with the zone `crawl.test` (does not end with the path's zone); composite TLD names are accepted — `example.co.uk` in `uk/add/` gives the label `example.co` |
| `no_domain` | no domain found in a line with extra data (T-132): there are no candidates or none passed validation | `2029-11-11 TIMEWEB`; `yyy.org registrar` in a `com/add/` file |

Duplicates within a file (including the unicode/punycode forms of one
IDN name) are not rejected — the DB deduplicates them by the unique key
`(zone_id, label)`.

## 4. The error-share threshold

`ingest.max_error_ratio` (0.1 by default) — the share of rejected lines
above which the file is considered bad **as a whole** (no line is
applied):

- the rejection share is **strictly above** the threshold → the file is
  bad → `failed/`;
- equal to the threshold or below — the file is applied; the rejected
  lines are visible in the report and the `rejected` log.

The meaning (spec 5.1): single junk does not harm a file, while a "file
in the wrong zone" or a broken dump gives a share close to 1 and ends up
in `failed/`.

## 5. The file lifecycle

| Outcome | Where | What remains |
|---|---|---|
| applied | `processed/<zone>/<operation>/…` | an `ingest_files` row (sha256, counters) |
| a duplicate — an exact repeated delivery (the same zone + the same action + the same sha256) | `processed/…` | no new report — the re-processing is not performed |
| bad (the path structure, the zone, the rejection share above the threshold) | `failed/…` | a `<file>.report` log report nearby; there is no `ingest_files` row — the fixed file can be placed again |
| an infrastructure failure (the DB, reading) | stays in `incoming/` | reprocessed after a restart/the next walk (at-least-once, spec 3.4) |

The zone/operation structure is kept in the archive; a name collision in
the archive is resolved with a `-2`, `-3`… suffix. Dedup is by content
(sha256), not by the file name: a changed file at the same path is
processed again. A duplicate is only an **exact repeated delivery**: a
match of the zone, the action and the sha256 (the unique key
`ingest_files (zone_id, action, sha256)`, T-113). The same content
**with another action** (an undo: the same list from `delete/` after
`add/`) and **in another zone** is processed again — see §6. File dedup
is an optimization; the correctness of repeated delivery is guaranteed
by the operations on domains (§6, "Idempotency of repeated delivery").

**Emergency death of the service.** Processing interrupted by `kill -9`
or a DBMS crash leaves the file's batch in the `ingest_stage` staging
table orphaned: its `batch_id` will never be seen again (a re-processing
takes a fresh one), while the UNLOGGED table survives a service restart
(it is truncated only by an emergency PostgreSQL recovery — crash
recovery; a regular DBMS restart keeps the rows). Such batches are
removed by the same background cleanup cycle as the deleted domains'
results (a pass once a minute): the rows of batches older than a day are
cleaned by a range over `batch_id` — the creation time is baked into the
identifier's high bytes, staging has no dedicated time column — with a
Warn written to the service log. The threshold is well above the
processing time of any real file (files are processed one at a time,
COPY batches give ~10⁵–10⁶ rows/s), so the batch of an in-progress
processing is not touched. The contract's boundary is the system clock
(the batch age is wall time): time synchronization of the nodes is
mandatory (NTP/chrony in slew mode); a clock jump forward beyond the
threshold in the middle of a live processing is outside the contract.

## 6. The add and delete semantics

**add** (`INSERT … ON CONFLICT DO NOTHING` + the revival of deleted
domains):

- a new domain is inserted (`status = new`, `check_state = pending`);
- an existing live domain is **not changed** at all (the results, flags
  and deadlines are kept);
- a domain in the `deleted` status returns to circulation:
  `new`/`pending`, `queued_at` is reset (the "revived" counter — in the
  log report, part of "added").

**delete**:

- the domain moves to `status = deleted` and **stays in the DB**
  (spec 13.1); its results and content are cleaned up by the background
  cleanup (a pass once a minute in batches of up to 1000 domains,
  T-020);
- lines without a domain in the DB — "not found" in the log report
  (there is no column for this in `ingest_files`);
- deleted domains are not checked and not selected by the scheduler; a
  repeated delete file is a no-op.

### Idempotency of repeated delivery (T-097)

Repeated delivery of a file is safe at two levels:

- the **file level (an optimization)** — the unique triple (zone,
  action, `sha256`) in `ingest_files` (T-113): an exact repeated
  delivery is not applied again, the file simply goes to `processed/`
  without a second journal row; the same content with another action or
  in another zone is processed again (see "The dedup contract" below);
- the **domain level (correctness)** — even if the same content reaches
  applying again (a failure after applying but before the report is
  written, a lost journal row, the same list in another file), the
  operations on domains converge to the same DB state:

| Re-application path | Guarantee |
|---|---|
| `add` of an existing live domain | a no-op: `INSERT … ON CONFLICT DO NOTHING` by the unique `(zone_id, label)`; the status, flags, results and deadlines are not changed |
| `add` of a domain in the `deleted` status | a revival (`new`/`pending`, the lease is reset); a repeated `add` of an already revived domain — a no-op (the `status = deleted` guard) |
| `delete` of a missing domain | a no-op, the line goes into "not found" |
| `delete` of an already deleted domain | stays `deleted`, the "deleted" counter is stable |
| file loading | a fresh `batch_id` per attempt: the rows of an interrupted batch in `ingest_stage` are never applied again, an applied batch leaves the staging |
| service death during processing (`kill -9`, a DB crash) | the file stays in `incoming/`, the batch in `ingest_stage` is orphaned — removed by the background cleanup by `batch_id` age (T-098); a re-processing takes a fresh identifier |
| the background cleanup | cleaned-up domains are not selected again; `contents.refcount` is decremented strictly by the actually deleted references |

The bottom line: **the same file processed twice does not change the
domains' state** (fixed by the unit and integration idempotency tests,
T-097). The report counters reflect the last attempt ("already existed"
instead of "added") — this does not affect the DB state.

### The dedup contract (T-113)

An exact repeated delivery = a match of the **zone, action and sha256**
(the unique key `ingest_files (zone_id, action, sha256)`, migration
000009) — only it is skipped as a duplicate. Hence two regular scenarios
of processing the same content again:

- an **undo with the same list** — a file from `add/`, then the same
  list in `delete/`: the action is different, there is no duplicate, the
  domains are deleted (a second row in the journal);
- a **repeated delivery in another zone** — the first delivery did not
  touch that zone's domains (the file's lines must belong to the path's
  zone, §3), the processing is legitimate; a Warn is highlighted for the
  operator in the service log (it is useful to see the reuse of one
  list).

An earlier restriction — a global dedup by a single `sha256`, which
skipped both scenarios — was lifted by T-113 (the product decision of
2026-10-18); the harmlessness of re-application is guaranteed by the
domain level above (T-097, `TestIngest_ReapplySameFileConverges`).

### Operator scenario: undoing an add with the same list

A mistakenly delivered list is undone by delivering it again into
`delete/` — byte for byte:

```
var/incoming/crawl.test/add/2026-10-18.txt      # delivered
example.crawl.test
shop.crawl.test

var/incoming/crawl.test/delete/2026-10-18.txt   # the same list — an undo
example.crawl.test
shop.crawl.test
```

A match of the zone and the `sha256` with a different action is not a
duplicate: the file is applied, the list's domains move to `deleted`
(they stay in the DB, spec 13.1), and the journal gains a second row.
Preparing a separate "correct" file for the undo is not needed — and
should not be done: an exact copy guarantees that exactly the list that
was delivered is undone. The pipeline highlights the content reuse with
an Info record "the same content was already delivered with another
action — processing it (an undo)" with the path of the first delivery.
The reverse direction works the same way: reviving a deleted domain
accepts the same list from `delete/` into `add/`. An exact repeated
delivery — a match of the zone, the action and the content (`sha256`) —
is still a duplicate and is not applied.

## 7. The reports

The **`ingest_files` journal** (SQL, `make psql`): the zone, the action,
the path, the `sha256`, the `lines_total / added / existed / deleted /
rejected` counters, the processing time.

The **`failed/<file>.report` log report** — the text next to the bad
file:

```
# ingest log report (spec 5.1)
file: /var/lib/crawler/incoming/ru/add/dump.txt
zone: ru (id 42)
action: add
sha256: …
verdict: bad — the rejected-line share, 620 of 1000, is above ingest.max_error_ratio (0.1)
lines: 1000
  skipped (empty and comments): 12
  rejected: 620
    wrong_zone: 611
    invalid: 9
  valid: 368
```

The same text is the basis of the ingest service's log records about
processing a file.

The **`rejected` log** — the stream of per-line rejections on the
service's stderr (the `service=rejected` attribute): the line number,
the line truncated to 255 runes, the reason.

## 8. An example

```
var/incoming/crawl.test/add/2026-10-18.txt   # crawl.test — allowlisted
                                              # testbed test zone (§1.1)

# fresh domains (extra data in a line is handled by extraction, T-132)
example.crawl.test
пример.crawl.test          # IDN: normalized to xn--e1afmkfd.crawl.test
www2.example.crawl.test    # ok: composite labels are allowed (the idn contract)

# junk — single lines, the usual reasons (do not fail the file at a share ≤ 10%)
example.com
под_чёркивание.crawl.test
```

The first block is mixed lines: extraction takes the first candidate
that passes (an inline comment after the domain does not interfere — it
is just the line's following tokens). The second block is single lines
the usual way: `example.com` → `wrong_zone` (not from the `crawl.test`
zone), `под_чёркивание.crawl.test` → `invalid`.

After processing: the file in
`var/incoming/processed/crawl.test/add/`, the domains in the DB (a
punycode label, the `is_idn` bit for IDNs), a row in `ingest_files`, and
the `crawl.test` zone visible in `crawlerctl zones list`.

## 9. Archives (.zip, .tar, .tar.gz, .tgz, .gz)

Real registrar exports and public zone dumps arrive packed. Besides
`*.txt`, the `<zone>/add|delete/` directory accepts archives: `.zip`,
`.tar`, `.tar.gz`, `.tgz`, `.gz` (case-insensitive). The formats are the
Go stdlib; `rar`/`7z` are **not supported** (considered and rejected:
not stdlib) — the watcher does not surface such files, and ingest parses
only what is surfaced.

- **Readiness** — the same as for txt (§2): the `ingest.stable_seconds`
  size/mtime stability, delivery under a temporary name (`*.tmp`,
  `*.part`) and a rename.
- **Streaming processing** (spec 3.5): the members are not materialized
  on disk, each is read as a stream and goes through the usual §2–§3
  pipeline — the same line format, the same normalization (spec 5.2),
  the same rejection reasons. Memory — as with a txt file: the parser's
  window plus a COPY batch.
- **The zone and the action come only from the archive's directory**
  (`com/add/x.zip` → the zone `com`, the action `add`). The members'
  internal paths **do not affect** the zone: the members are domain
  lists of the directory's zone; choosing the zone independently from a
  member's path is unsafe (the member `ru/list.txt` in the
  `com/add/x.zip` archive is a domain list of the zone `com`, not `ru`).
  The internal path is used only in rejection addressing.
- **Skipped with a Warn** in the service log: directories, hidden files
  and directories (a path component starting with a dot — including the
  macOS junk `._*`, `.DS_Store` and hidden trees as a whole), the
  resource directory `__MACOSX` and its contents. Skipped entries do not
  fail the archive.
- **Nested archives are not unpacked** — one level: an archive member is
  read as text, the binary junk gives an error share above the threshold
  — the whole archive goes to `failed/`.
- `.gz` — **a single member** named after the archive without the
  extension (`zone.gz` → the member `zone`); a multi-segment gzip (a
  concatenation of streams) is read as one member. `.tar.gz`/`.tgz` —
  gzip over tar.

### The accounting unit and the report

The accounting unit is the **archive**: one `ingest_files` row — the
`sha256` of the archive's bytes (the file's compressed form),
`lines_total / added / existed / deleted / rejected` are the sums over
the members, the zone and the action come from the directory. Rejected
lines are addressed as "archive!member:line" — in the `rejected` log the
`file` attribute for line 42 of the member `ru/list.txt` of the
`com/add/x.zip` archive is `com/add/x.zip!ru/list.txt` with `line=42`;
the log report next to the bad archive additionally shows the number of
members. Idempotency is the same as for txt (§5–§6): an exact repeated
delivery of an archive is a duplicate (the processing is not repeated),
a resubmission after a failure converges to the same domain state
(`ON CONFLICT DO NOTHING`, spec 5.1).

### Limits and the verdict

| Parameter | Meaning | Default |
|---|---|---|
| `ingest.archive_max_uncompressed` | the total uncompressed volume of the stream (for zip — of the members) | `8GiB` |
| `ingest.archive_max_files` | the number of the archive's entries (directories and junk included) | `1000` |

The archive goes **as a whole** to `failed/` with a log report nearby,
and its batch is discarded (the lines are not applied), if:

- the share of rejected lines of **any member** is above
  `ingest.max_error_ratio` (the threshold applies to a member — as to a
  txt file; the reason in the report addresses the member);
- the archive is **broken**: it cannot be read (junk bytes, an empty
  `.gz`) or is damaged while reading (a truncated body, a bad CRC/tar
  header);
- any of the limits is exceeded: for zip the uncompressed volume is
  checked **before reading a member** by the `UncompressedSize64`
  header, for tar/gz — by a counter over the whole unpacked stream (the
  tar headers and the members' contents); the actual zip reading is
  counted too — the header may lie. The number of entries: for zip — by
  the central directory before reading the members, for tar — each
  header, including directories and skipped junk (a metadata bomb —
  thousands of empty directories — compresses by orders of magnitude and
  is caught by both limits). The cost of the zip central directory
  itself is bounded by the input file's size (incoming is the operator's
  responsibility, spec 5.1); the limit bounds the subsequent processing.

An infrastructure failure (the DB, the disk) does not fail the archive:
it stays in `incoming/` and will be reprocessed (at-least-once,
spec 3.4); the lines that had been applied before the failure are
harmless for a resubmission (§6).

A subtlety of full reading: a tar stream ends with zero blocks and
padding (GNU tar pads up to 10 KiB), gzip checks the CRC only by the
trailer — the pipeline reads the rest of the stream after the last
member, so the `sha256` in `ingest_files` equals the `sha256sum` of the
whole file, and a broken trailer is caught (bytes after the gzip stream
that do not form a correct stream concatenation are also counted as
damage). Bad archives are not read to the end — the hash is not printed
in their log report. An empty (entry-less) tar/zip is a valid empty
archive (applied with an empty batch, like an empty txt); a zero-length
file is a broken archive of any format (zip/gz have no valid container).

### An example

```
var/incoming/com/add/2026-10-19.zip     # the zone will be created automatically

  sub/one.txt        # internal paths are arbitrary — the zone is not derived from them
    example.com
  ru/two.txt         # the member's directory name is NOT the zone: the lines must be com
    shop.com
  __MACOSX/junk      # skipped with a Warn
```

After processing: the archive as a whole in
`var/incoming/processed/com/add/`, the domains of both members in the
DB, one `ingest_files` row with the sums over the members.

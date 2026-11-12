# Operational runbooks

Step-by-step instructions for the operator: retraining the compression
dictionary, triage of the `q:dead` dead-letter queue, re-analysis of
results under new rules, triage of a DNS resolver failure, a one-off
cleanup of accumulated content and the jsonb trim (the T-167 campaign),
manual operations on the `render_pending` bit (the T-175 render-debt
counter), drift of incremental counters (T-178), the zone collapse
window `crawlerctl zones collapse` (the switch to TLD zone semantics),
growth of the queued-without-queued_at guard counter (T-255). General
operations (startup, logs, metrics, updates) —
[`operations.md`](operations.md); the CLI reference —
[`dev-environment.md`](dev-environment.md) §6.

## 1. Retraining the zstd compression dictionary

Compressing small HTML pages (parking pages, hosting stubs) is markedly
more efficient with a trained dictionary: the typical template is
encoded once (spec 5.7; mechanics — [`content.md`](content.md),
parameters — [`configuration.md`](configuration.md), the `content`
section).

**When to retrain.** The best time is **before bulk content loading
starts** (an empty store): all packs are compressed with one
dictionary. Retraining on a live store does not harm the readability of
old packs — the previous-dictionary registry, step 5.

### Steps

1. **Collect samples** — HTML pages (`*.html`/`*.htm`) typical of your
   workload: one or two directories, a recursive crawl. The starter set
   is `testdata/laravel-samples`; the corpus is read up to 64 MiB (the
   training limit), no more is needed.
2. **Train the dictionary** — `crawlerctl dict train <output-file>
   [--samples PATH]…` (the flag repeats):

   ```bash
   # dev environment (the dictionary goes into the data directory
   # mounted into the services)
   docker compose --project-directory . -f deploy/docker/compose.yaml \
       run --rm -v "$(pwd)/var/content:/out" tools \
       go run ./cmd/crawlerctl dict train /out/dict.zst \
       --samples testdata/laravel-samples

   # production server
   /opt/crawler/bin/crawlerctl dict train \
       /var/lib/crawler/content/dict.zst --samples /opt/crawler/samples
   ```

   The command prints the sample count, the corpus size, the dictionary
   ID, and the corpus compression estimate ("without dictionary / with
   dictionary" — a reference point; the real effect is measured on new
   pages of the same template). The file is written atomically (a temp
   file + rename) — a half-written dictionary is impossible. Retraining
   is the same command run again: the file is overwritten, the
   dictionary ID changes (a deterministic function of the content), and
   **the previous dictionary is automatically archived into the
   previous-dictionary registry** (T-103): the file `dict-<id>.zstd`
   (id — the previous dictionary's ID) next to the new one; the command
   prints the archive path. An archiving failure is never silent — the
   command fails before writing the new file, and the previous
   dictionary stays active.
3. **Hook it up** — the path is already set by the
   `content.zstd_dictionary` configuration (dev: `var/content/dict.zst`;
   production: `/var/lib/crawler/content/dict.zst`), and it also sets
   the previous-dictionary registry directory. A missing file is not an
   error: content is compressed without a dictionary (a log entry at
   startup), and the registry stays connected.
4. **Restart the storage services** — the dictionary is read at process
   start: http-worker, render-worker (they write packs), analyzer, and
   `crawlerctl domain --content` (they read):

   ```bash
   systemctl restart crawler-http-worker crawler-render-worker crawler-analyzer
   # check: the log line "zstd dictionary loaded" with the same ID that
   # dict train printed
   journalctl -t crawler-http-worker | grep dictionary
   ```

   In dev — `make up` (the tools container and the services see one
   bind-mount of `var/content`).
5. **The previous-dictionary registry — readability of old packs.** A
   zstd frame carries the dictionary ID in its header; when reading a
   block whose ID is unknown to the process, the dictionary is lazily
   registered from the registry (`dict-<id>.zstd` in the
   `content.zstd_dictionary` directory) and memoized: **retraining does
   not make recorded content unreadable** — old packs are read with
   their previous dictionary from the registry, new ones are written
   with the new active one. The registry needs no restart when refilled:
   a file dropped into the directory on a live service is picked up on
   the next read.

   Readability is lost only if the previous dictionary's file is absent
   from the registry (trained before T-103 and the file was replaced,
   or the file was deleted by hand): `Get` answers with the error
   "dictionary … is missing from the registry", and at startup the
   services show a Warn (`WarnDictMismatch`; a dictionary present in
   the registry — Info). Recovery — restore the `dict-<id>.zstd` file
   from a backup or retrain on the same sample corpus (determinism
   gives the same ID). Space: ~100 KB per dictionary — not critical by
   hand; the archive may be deleted only when no packs compressed with
   that dictionary remain (mechanics — [`content.md`](content.md)
   "Dictionary and the previous-dictionary registry").
6. **Check the effect** — the size of new pack records (the
   `content.dir` directory) and the log; a manual read check —
   `crawlerctl domain --content <a domain with a recent check>`.

The command needs no database or Redis — it works with the stack
stopped.

## 2. Triage of the q:dead dead-letter queue

`q:dead` is a stream of tasks that exhausted their delivery count
(`redis.max_attempts`, 3 by default): an entry read by the group
multiple times and never acknowledged (XACK is set only after the
result is recorded into the database), when claimed back from a hang
via `XAUTOCLAIM`, is published to `q:dead` and acknowledged in the
source stream (spec 4.2, T-025). The queue has no consumer — triage is
manual.

**Important note on self-healing:** the `XAUTOCLAIM` claim is built
into every worker (RecoverLoop, T-025) — an entry stuck in pending
after a worker crash or a poison error returns to work, and one that
exhausted its attempts goes to `q:dead`. The scheduler's lease pass
(T-029) recovers only `q:dns` tasks (the domain in `queued`): for the
other stages the domain is already `done`, and redelivery is the
claim's job. Growth of `q:dead` in the summary is a signal to
investigate, not an outage: isolated entries are traces of tasks that
were published validly but keep failing in processing (see the typical
causes below); messages outside the package formats (without
`task_id`) also end up here — with the synthetic identifier
`unknown:<stream>:<entry_id>` and without entity fields
(T-260: before it they spun in pending in an endless claim loop), their
flow is counted by `crawler_queue_dead_unidentified_total`; mass growth
is a symptom of a database outage, a stuck stage, or Redis OOM (§2.6).

**q:dns batch retry and the `http_published` bit (T-108):** a batch
redelivery (claim or queue) processes only unfinished domains
(`check_state <> done`), and domains whose `q:http` task has already
been published are recognized by the `http_published` bit in
`domains.flags` — the worker carries them through to result recording
without republishing (a duplicate `q:http` — a full repeat HTTP check —
is not created). The bit lives only within an episode: it is cleared by
result recording (`done`) and by a scheduler enqueue
(`pending → queued` — including a lease return, resurrection, and a
recheck). For the operator: a domain in `queued` with the
`http_published` bit is a normal intermediate state "published, being
recorded"; the bit on a `done` row is a cosmetic residue of a failure,
cleared by the next enqueue — no manual cleanup needed. To see the bit:
`SELECT label, flags FROM domains WHERE
flags & 4 <> 0;` (the value 4).

### 2.1. See what has accumulated

```bash
# operator summary: the q:dead line
crawlerctl status            # dev: make status

# Redis directly (dev: make redis-cli)
XLEN q:dead                  # depth
XINFO STREAM q:dead          # metadata: length, first/last entry
XRANGE q:dead - + COUNT 10   # the first 10 entries
XREVRANGE q:dead + - COUNT 10 # the newest entries
```

Entry format (`internal/queue`, the `data` envelope): the task and
entity identifiers, without the original message payload (T-105 —
byte-level protection, see [`redis.md`](redis.md) §3):

```json
{"task_id": "9f2b…", "stream": "q:http", "entry_id": "1770…-0",
 "attempts": 4, "reason": "attempt limit 3 exhausted: 4 deliveries without ack",
 "zone_id": 3, "domain_id": 42, "name": "example.com"}
```

- `stream` — the source: `q:dns` (a domain batch), `q:http` (a domain),
  `q:render`/`q:analyze` (the `subdomain_results` row key);
- entity identifiers: `name`/`zone_id`/`domain_id` — the domain of a
  single-domain task; for `q:render`/`q:analyze` the row key is
  extended with `subdomain_id` and `variant` (omitted — 0, the primary
  one); for `q:dns`, `domains` (the batch size) instead of the domain
  list: the list is not stored — up to ~250 KB per batch with the
  10,000-entry cap would give a worst case of ~2.5 GB against the 0.5 GB
  Redis budget; the batch's domains themselves are republished by the
  lease pass (see §2.3);
- a `task_id` of the form `unknown:<stream>:<entry_id>` — a synthetic
  identifier for an entry without a `task_id` (T-260): a body outside
  the package formats, such an entry has no entity fields — the source
  is tracked down via `entry_id` (step 2 below:
  `XRANGE <stream> <entry_id> <entry_id>` shows the body) and via the
  journals around the entry's time; the flow of such entries is counted
  by `crawler_queue_dead_unidentified_total` (zero in steady state —
  growth means a publication outside the package contract: a manual
  XADD or a writer bug);
- entries carry no original message payload (T-105); a `payload` field
  occurs only in entries migrated before T-105 — it is readable but no
  longer published by new entries;
- the stream MAXLEN is 10,000 (history is bounded; entries without
  payload — a cap of ~2–3 MB, see docs/redis.md §3).

### 2.2. Find the cause

1. Identify the domain from the entry's `name`/`domain_id` —
   `crawlerctl domain <name>` shows the status, the results, and the
   check errors; for `q:render`/`q:analyze` the exact row is keyed by
   `(zone_id, domain_id, subdomain_id, variant)`. A `q:dns` batch
   carries no names in the entry (only the size): the lease pass will
   bring back the specific domains, and the cause of the batch's death
   is visible in the dns-worker logs around the entry's time.
2. The original message itself is sometimes still readable: XACK does
   not remove the entry from the source stream immediately, but the
   acknowledged prefix frees it for the scheduler's XTRIM MINID loop
   (T-141, up to `redis.trim_interval` after acknowledgement), and
   `entry_id` is present in the q:dead entry —

   ```bash
   XRANGE <stream> <entry_id> <entry_id>   # the task's full payload
   ```

   (`stream` and `entry_id` are fields of the q:dead entry itself;
   once the stream has trimmed the entry away, what remains is the
   worker logs and lease recovery).
3. Logs of the worker of the corresponding stage around the hang time
   (the `service` attribute): dev — `make logs SERVICE=http-worker`,
   production — `journalctl -t crawler-http-worker --since …`.
4. Typical permanent causes (the task will fail again):
   - a DOM bloated by JS beyond `content.max_store_bytes`
     (`ErrTooLarge`, render) — raise the limit via config;
   - content compressed with a dictionary whose file is lost from the
     previous-dictionary registry (runbook 1, step 5) — put the
     `dict-<id>.zstd` file back into the dictionary directory; reads
     recover without a restart;
   - a database/storage failure — visible in logs more broadly (not
     only this task);
   - a render page timeout (tarpit) exhausted attempts here before
     T-148; after T-148 such entries are finalized without render and
     never reach `q:dead` — §2.5 covers the backlog accumulated before
     the fix;
   - a permanent render-host certificate error
     (`net::ERR_CERT_AUTHORITY_INVALID`/`ERR_CERT_COMMON_NAME_INVALID`,
     plus `net::ERR_CERT_DATE_INVALID` since T-172) exhausted attempts
     here before T-159; after T-159 the cert class is finalized without
     render on the first delivery and never reaches `q:dead` — §2.5
     covers the backlog accumulated before the fix;
   - a task-class render error (connection refused, load error) on the
     last delivery died here before T-172 (the 23.10 measurement: 88%
     of first `ERR_CONNECTION_REFUSED` failures exhausted attempts);
     after T-172 such entries are finalized without render on the last
     delivery and never reach `q:dead` — §2.5 covers the backlog.

### 2.3. Requeue or discard

- **Manual requeueing is unnecessary and not recommended** (manual
  republishing via XADD creates a new entry without attempt history —
  a source of duplicates, not a fix):
  - domains from `q:dns`/`q:http` tasks are recovered by the
    scheduler's lease pass: `queued` without a result older than
    `scheduler.lease` (30 min) returns to `pending` and goes through
    the pipeline again. Important: the lease pass only sees `queued` —
    if the DNS result is already recorded (`done`, the status still
    new), a lost q:http is NOT returned by the lease pass: the row is
    recovered by the "stuck status" compensator T-270 (an hourly pass,
    new∧done older than 24 h — §2.10);
  - SPA entries (`q:render`) are finished by the HTTP stage's
    background pass (`render_pending`, once a minute);
  - analysis (`q:analyze`) is retried by the hourly re-analysis cycle
    (ReanalyzeLoop).
  A point requeue of a single domain — `crawlerctl recheck <domain>`
  (moves it to `pending`; the regular pipeline takes over).
- **Discard** the triaged entries:

  ```bash
  XDEL q:dead <entry_id>          # point-wise
  XTRIM q:dead MAXLEN 0           # everything (after triage)
  ```

  After upgrading to a version where q:dead entries no longer carry
  payload (T-105), old payload-bearing entries can be flushed once with
  the same `XTRIM q:dead MAXLEN 0` after triage — otherwise they stay
  in the stream until evicted by the 10,000-entry cap.

- Redis loss/restart clears `q:dead` (there is no persistence) — this
  is the expected scenario ([`redis.md`](redis.md) §5): the tasks of
  `queued` domains are not lost, the lease pass brings the domains
  back; an undelivered `q:http` with a recorded `done` is returned by
  the "stuck status" compensator (§2.10).

### 2.4. Poison domain rows: the invalid status

A domain with an empty/whitespace name in a `q:dns` batch (a junk or
corrupted `domains` row: manual edits, database corruption — the
regular ingest never writes such rows) receives the terminal diagnosis
`status = invalid` (T-109): `check_state = done`, the lease is
completed, a Warn in the dns-worker logs. The status is not a fact
about the domain but the diagnosis "checking is impossible": the
recheck (T-030) and the lease pass do not return such rows, and the
pipeline does not loop them endlessly.

Find them:

```sql
SELECT zone_id, id, label, last_checked_at
FROM domains WHERE status = 7;        -- invalid
```

Triage is manual — two paths:

- **the row is junk** (broken for good): delete it with
  `DELETE FROM domains WHERE zone_id = $1 AND id = $2;` — the zone may
  then become empty and drop out of the summary; this is expected;
- **the name is fixable** (a typo in `label`): fix it with
  `UPDATE domains SET label = $1 WHERE zone_id = $2 AND id = $3;`
  (label uniqueness within a zone applies), then return it to the
  pipeline: `UPDATE domains SET status = 0, check_state = 0 WHERE …;`
  — the row goes through the full cycle again.

Keeping invalid rows is harmless (they are excluded from the cycles),
but the `crawler_domain_status_total{type="invalid"}` counter and the
backlog gauge show them honestly — growth means corrupted data, not a
pipeline failure.

### 2.5. Bulk triage of q:render entries without render (after T-148/T-159)

Before T-148 a render page timeout burned the common delivery limit as
a random error: a heavy SPA (not fitting into the 25 s `render.timeout`)
exhausted `redis.max_attempts` 3 and was sent to `q:dead` — the work
dropped out of the pipeline until manual triage. The 2026-10-23
incident: 5507 entries over hours with the reason "attempt limit 3
exhausted"; the initial attribution "all of them are q:render tarpits"
was refuted by the post-mortem (composition: 2786 `q:http`, 2685
`q:render`, 36 `q:dns` batches; the root — a Redis OOM window; the
signature and the analysis — §2.6 and
[`load-measurements.md`](load-measurements.md) §11.1). After the T-148
deploy, new tarpits never reach `q:dead` (the policy —
[`configuration.md`](configuration.md) §render, `render.timeout_attempts`),
after T-159 — cert hosts too: a permanent certificate error
(`ERR_CERT_AUTHORITY_INVALID`/`ERR_CERT_COMMON_NAME_INVALID`, plus
`ERR_CERT_DATE_INVALID` since T-172; per the measurement: 0 successful
retries out of 321 per day) is finalized without render on the first
delivery (a host property, retries are pointless —
[`render.md`](render.md), "Queue cycle"); after T-211 —
DNS-dead hosts too (`ERR_NAME_NOT_RESOLVED`: the entry reached `q:render`
through a live HTTP episode and the name stopped resolving — DNS
recovery on a scale of days does not fit the retry window, the T-207
measurement), and after
T-172 — entries whose last delivery failed with a task-class render
error (connection refused, load error): up to `redis.max_attempts` the
error is repeatable (host recovery is observed — 6.6% of retries
`ERR_CONNECTION_REFUSED` succeed), on the last one the entry is
finalized without render and never reaches `q:dead` (the 23.10
21:20–21:31 incident: a wave of 459 deaths on mixed timeout→refused
series). A heavy page,
cert host, and a host that exhausted attempts leave the raw HTML of
the HTTP phase as the entry's outcome (the entry goes to
`q:analyze`); `q:dead` grows only from
infrastructure errors (database, storage, publishing, browser).

Accumulated entries are triaged manually, in this order:

1. **Deploy the fix first** — requeueing entries before it is
   pointless: the tarpit would exhaust attempts again and return to
   `q:dead`.
2. **Requeue criterion**: a `q:render` entry with the reason "attempt
   limit exhausted" and page timeouts in the render-worker logs around
   the time of death (`journalctl -t crawler-render-worker --since … |
   grep "page timeout"`) — the page gets another chance: after T-148
   the cost of a repeated tarpit is bounded (`render.timeout_attempts` ×
   25 s of slot time per task); afterwards the entry finalizes itself
   without render. The same after T-159 for cert hosts (in the logs —
   `page load error net::ERR_CERT_AUTHORITY_INVALID` /
   `ERR_CERT_COMMON_NAME_INVALID` / plus `ERR_CERT_DATE_INVALID` since
   T-172): the retry costs one attempt — if the certificate did not
   change, the entry finalizes itself without render; if it did, the
   render goes through. The same
   after T-172 for deaths with task-class errors on early
   deliveries (in the logs — `net::ERR_CONNECTION_REFUSED` and other
   `net::ERR_*` without cert codes): the host was given retries, and
   after the fix deploy such an entry finalizes itself on the last
   delivery (before the fix, mixed series died up to T-172). Entries
   with a different cause (a DOM beyond
   `content.max_store_bytes`, database/Redis failures) are triaged per
   §2.2 and are not requeued by the script.
3. **Requeue into q:render** — not via a manual XADD (§2.3) but via
   the `render_pending` flag (bit 1, value 2): the HTTP stage's rework
   pass (every minute, with a `q:render` capacity check) will itself
   republish the domain's SPA entries (a deterministic second verdict
   over the stored content: `rendered = false AND final_url IS NOT NULL AND
   render_failed_at IS NULL`, T-210) and clear the flag. Entries
   already marked `render_failed_at` (the T-148/T-159/T-172
   finalizations have already declared render impossible until the
   content changes) will NOT be republished by the pass — the flag is
   cleared by a "blind" clearing without publications; dead entries of
   infrastructure errors carry no stamp (finalizations XACK them; they
   never reach q:dead), but mixed domains and a forced re-render after
   a host fix require resetting the marker with the same domain list
   (T-210; caveat: for aged-out rows the content is already cleaned by
   retention — `content_hash` NULL,
   the pass skips them even without the marker; only a new episode of
   `crawlerctl recheck` reworks them):

   ```bash
   # reset the "render impossible" marker for the domains of q:render
   # dead entries (before setting the flag; RETURNING — a row-count
   # check; zone_id IN — partition pruning of subdomain_results, T-005)
   PAIRS=$(redis-cli --raw XRANGE q:dead - + \
     | grep '^{' \
     | jq -r 'select(.stream == "q:render") | "(\(.zone_id),\(.domain_id))"' \
     | sort -u)
   [ -n "$PAIRS" ] || echo "no q:render dead entries — marker reset skipped"
   if [ -n "$PAIRS" ]; then
     ZONES=$(printf '%s\n' "$PAIRS" | tr -d '()' | cut -d, -f1 | sort -u | paste -sd,)
     printf '%s\n' "$PAIRS" \
       | paste -sd, \
       | sed "s#^#UPDATE subdomain_results AS r SET render_failed_at = NULL FROM (VALUES #; s#\$#) AS k(zone_id, domain_id) WHERE r.zone_id IN ($ZONES) AND (r.zone_id, r.domain_id) = (k.zone_id, k.domain_id) RETURNING 1#" \
       > /tmp/opencode/unmark-render.sql
     psql "$CRAWLER_PG_DSN" -f /tmp/opencode/unmark-render.sql
   fi
   ```

   The domain list comes from the dead entries' identifiers. The flag
   is set atomically with the render-debt counter (the §6 invariant:
   the rework pass clears the bit with a decrement — setting it without
   an increment would drive the counter negative):

   ```bash
   # SQL to set the render_pending flag (+counter) for all q:render dead entries
   redis-cli --raw XRANGE q:dead - + \
     | grep '^{' \
     | jq -r 'select(.stream == "q:render") | "(\(.zone_id),\(.domain_id))"' \
     | sort -u \
     | paste -sd, \
     | sed 's#^#WITH flagged AS (UPDATE domains AS d SET flags = (d.flags | 2)::int2 FROM (VALUES #; s#$#) AS k(zone_id, domain_id) WHERE (d.zone_id, d.id) = (k.zone_id, k.domain_id) AND (d.flags \& 2) = 0 RETURNING 1)#' \
     > /tmp/opencode/requeue-render.sql
   printf "UPDATE crawler_counters SET value = value + (SELECT count(*) FROM flagged) WHERE name = 'render_pending';\n" \
     >> /tmp/opencode/requeue-render.sql
   psql "$CRAWLER_PG_DSN" -1 -f /tmp/opencode/requeue-render.sql
   ```

   (PostgreSQL casts the VALUES types itself; `psql -1` — both
   statements in one transaction; `UPDATE 0` from the second statement
   means the counter row is missing, see the §6 symptom; dev —
   `make psql`; a single domain — point-wise, with the same §6
   invariant:

   ```sql
   WITH flagged AS (
       UPDATE domains SET flags = (flags | 2)::int2
       WHERE zone_id = $1 AND id = $2 AND (flags & 2) = 0
       RETURNING 1
   )
   UPDATE crawler_counters
   SET value = value + (SELECT count(*) FROM flagged)
   WHERE name = 'render_pending';
   ```

   or a full new episode of `crawlerctl recheck <domain>`)
4. **Finalize without render** (an alternative for entries that do not
   need a render, or when the queue is triaged without a render wave):
   the rows already hold raw HTML (`rendered = false`); only the
   `q:analyze` publication is missing — the pipeline is completed by
   analyzing the raw HTML:

   ```bash
   redis-cli --raw XRANGE q:dead - + \
     | grep '^{' \
     | jq -r 'select(.stream == "q:render") |
         "redis-cli XADD q:analyze \\* data " +
         ({task_id: .task_id, zone_id: .zone_id, domain_id: .domain_id,
           subdomain_id: .subdomain_id, variant: (.variant // 0),
           name: (.name // ""), fresh: true} | tostring)' \
     > /tmp/opencode/finalize-render.sh
   # run in portions (watching the q:analyze depth; analysis duplicates
   # are bounded and idempotent — the same contract as repeated render
   # deliveries):
   #   sh -x /tmp/opencode/finalize-render.sh
   ```

5. **Discard the triaged entries** — as in §2.3 (`XDEL` point-wise,
   `XTRIM q:dead MAXLEN 0` after triaging the whole batch).

Post-requeue monitoring: the `q:render`/`q:analyze` depths (the
`crawlerctl status` summary), the metrics
`crawler_render_tarpit_finalized_total`,
`crawler_render_cert_finalized_total`,
`crawler_render_dns_finalized_total`, and
`crawler_render_task_finalized_total{reason}` (the share of pages that
did not render even after the retry, and the breakdown of reasons),
and the growth of `q:dead` — after the T-148/T-159/T-211/T-172 deploys
it no longer means tarpits, certificates, DNS-dead hosts, or task-class
errors on the last attempt, but the causes
from §2.2 (mass growth is the Redis OOM signature, §2.6).

### 2.6. Mass q:dead growth under Redis OOM

Worked example — the 22–23.10.2026 incident: 5507 deaths with fully
healthy processing (the post-mortem —
[`load-measurements.md`](load-measurements.md) §11.1; the work log —
[`operations.md`](operations.md) §5, 22–23.10).
A Redis OOM window (22.10 ≈16:00–18:15 MSK: legacy `q:dns` batches of
the batch_size 5000 era ≈620 MiB against a temporarily raised
maxmemory of 900 MiB, `noeviction` — [`redis.md`](redis.md) §4)
rejected result publications (`XADD: OOM command not allowed`: 16,953
rejections from http-worker, 1,953 from render-worker, 392 from
dns-worker, 243 from the scheduler), and the publication error was
handled as an error of the task itself: delivery without XACK,
RecoverLoop claimed it in a circle, attempts burned down. Code fixes
of the consequences — T-152 (OOM does not burn attempts:
  a publication InfraError — XADD retries, a worker consumption pause,
  and a claim gate probing the entry — `redis.md` §4.1), T-153
(the RecoverLoop pace: at the time of the incident 16 claims/30 s ≈
2.6 h to walk a PEL of ~5K entries — deaths stretch over hours), T-154
(death observability); summary — load-measurements §11.1.

**The signature** — distinguishes an infrastructure event from poison
tasks (§2.2) and render tarpits (§2.5):

- the reason is the same for all — "attempt limit 3 exhausted" — but
  `attempts` ranges 4–11 with peaks at 9 and 11: the transfers into
  `q:dead` themselves hit the same OOM, and the delivery counter kept
  growing until the first successful XADD;

  ```bash
  redis-cli --raw XRANGE q:dead - + | grep '^{' \
    | jq -r .attempts | sort -n | uniq -c
  ```

- **the originals were published in a narrow window**: the
  milliseconds of a `q:dead` entry's `entry_id` are the publication
  time of the original task. Compare the publication histograms of the
  originals and of the deaths themselves: masses of deaths leading back
  to entries from a single source window (in the example all originals
  fall into 15:00–19:00 MSK, with the 2786 `q:http` inside one hour) —
  an infrastructure event, not domain quality; for tarpits and poison
  tasks the originals are spread out together with the inflow;

  ```bash
  # minutes of original publication (the q:dead entry's entry_id)
  redis-cli --raw XRANGE q:dead - + | grep '^{' \
    | jq -r '(.entry_id | split("-")[0] | tonumber / 1000 | floor)
             | strftime("%Y-%m-%d %H:%M")' | sort | uniq -c
  # minutes of deaths (the q:dead entries' own ids)
  redis-cli --raw XRANGE q:dead - + | grep -E '^[0-9]+-[0-9]+$' \
    | cut -d- -f1 \
    | jq -Rr '(tonumber / 1000 | floor) | strftime("%Y-%m-%d %H:%M")' \
    | sort | uniq -c
  ```

- deaths come in "waves" for hours after the window (12 h in the
  example): RecoverLoop mechanically burns through the same PEL cohort,
  and there are no new processing errors in the worker logs.

**Diagnosing the cause:**

```bash
# OOM publication rejections in the service journals (production;
# dev — make logs SERVICE=…)
journalctl -t crawler-http-worker --since "2026-10-22 15:00" --until "2026-10-23 07:00" \
  | grep -c "OOM command not allowed"
# the same for crawler-render-worker / crawler-dns-worker / crawler-scheduler

redis-cli INFO memory \
  | grep -E 'used_memory_human|maxmemory_human|maxmemory_policy'

for s in q:dns q:http q:render q:analyze q:dead; do printf '%-9s ' "$s"; redis-cli XLEN "$s"; done
```

**The remedy:**

1. **Remove the cause first** — Redis memory (in the example: draining
   the legacy `q:dns` — `XTRIM q:dns MAXLEN 125` freed 575 MiB, Redis
   683→104 MiB, the 512 MiB/640 MiB budgets restored, T-149). While
   the OOM is alive, triage is pointless: repeated publications fail
   and deaths keep accumulating.
2. **Do not requeue** — the pipeline recovers the subjects itself
   (§2.3): domains of `q:dns`/`q:http` — the lease pass (`queued`
   without a result older than `scheduler.lease` 30 min → `pending`),
   `q:render` SPA entries — the `render_pending` background pass (once
   a minute), `q:analyze` — the hourly ReanalyzeLoop; the flag from
   §2.5 item 3 is not needed here — render is not at fault. The
   pre-discard check — SQL over the dead entries' identifiers: a
   predominance of `check_state=2` (done) confirms "processing
   completed, the publication died":

   ```bash
   redis-cli --raw XRANGE q:dead - + | grep '^{' \
     | jq -r 'select(.domain_id != null) | "(\(.zone_id),\(.domain_id))"' \
     | sort -u | paste -sd, \
     | sed -e 's/^/SELECT d.check_state, count(*) FROM domains AS d JOIN (VALUES /' \
           -e 's/$/) AS k(zone_id, domain_id) ON (d.zone_id, d.id) = (k.zone_id, k.domain_id) GROUP BY 1 ORDER BY 1;/' \
     > /tmp/opencode/check-dead.sql
   psql "$CRAWLER_PG_DSN" -f /tmp/opencode/check-dead.sql
   ```

   (`q:dns` batches carry no names in the entry — their domains are
   brought back by the lease; `check_state`: 0 pending, 1 queued,
   2 done)
3. **Discard the triaged entries** — `XTRIM q:dead MAXLEN 0` (§2.3).
4. **PEL tail monitoring**: the groups' pending still holds entries
   that have already exhausted the delivery counter (~940 entries with
   a counter of 3 in the example, some on dead consumers): further
   claims will keep adding deaths by the hundred. Monitor the growth of
   `XLEN q:dead` and of the groups' PELs; a repeated XTRIM after things
   stabilize (deaths stopped, the PEL empty):

   ```bash
   redis-cli XINFO GROUPS q:http   # the group's pending (same for q:render)
   redis-cli XPENDING q:http http-worker - + 20   # the PEL with delivery counters
   ```

   (default groups — `dns-worker`/`http-worker`/`render-worker`/
   `analyzer`; exact names — in the XINFO GROUPS output)

### 2.7. Scheduler freeze from a phantom q:dns lag (XTRIM of undelivered entries)

Worked example — the 22–23.10.2026 incident: the `q:dns` publication
gate stayed closed from 22.10 23:29 to 23.10 15:24 MSK (~15.5 h) with
a live Redis and 40.3M pending domains — the pipeline stood still down
the chain ("empty" `q:http`/`q:analyze`), the dns-worker idled
(inflight = 0, XLEN `q:dns` = 1). The phantom's source — the T-149
drain of legacy batches after the §2.6 OOM incident; the operational
fix — T-168 (the same task added the clamp in code, see
"Prevention").

**The signature** — differs from the overflow case (§2.6) and from
tarpits (§2.5):

- exactly **one** WARN in the scheduler journal (the gate writes it
  once on entering the wait, no repeats — the freeze itself is
  invisible in the journal):

  ```
  publication paused: queue depth at threshold stream=q:dns depth=1828 watermark=125
  ```

- no XADD into `q:dns`, the dns-worker idles (the group's consumer
  idle for hours), while pending domains > 0 and `q:http`/`q:analyze`
  are empty;
- an everlasting `queue_overflow` alert on `q:dns` (the depth at the
  threshold, it will not clear by itself — in the example it hung from
  09:04 until the fix);
- the decisive sign — the group's lag is a multiple of the stream
  length:

  ```bash
  redis-cli XINFO STREAM q:dns   # length, entries-added
  redis-cli XINFO GROUPS q:dns   # the dns-worker group's lag, entries-read
  ```

  In the example: `length=1`, `entries-added=2140`; the `dns-worker`
  group: `entries-read=387`, `lag=1753` — the lag counts entries long
  gone from the stream.

**Mechanics.** A group's `lag` = `entries_added − entries_read` and
does not subtract entries trimmed away while undelivered. The T-149
drain (`XTRIM q:dns MAXLEN 125`) trimmed ~1753 undelivered legacy
batches of the batch_size 5000 era — the group's `entries-read`
counter does not subtract them, and Redis 7.0.x (prod runs 7.0.15)
holds a positive phantom lag forever (Redis 7.4+ corrects the lag on
trim by itself; the dev stack runs 7.4 — hence it does not reproduce
on dev). The effective `q:dns` gate threshold =
min(`queue_high_watermark` 100,000, MAXLEN 250 / 2) = 125 — 1753 > 125
forever, `XADD` never resumes on its own. Diagnostic masking: the
day's progress before the freeze is the drain of the stream's last
entries, which looks like a healthy DNS-stage rate.

**The remedy:**

1. Reset the group's read position to the end of the stream — the lag
   zeroes out, the gate opens:

   ```bash
   redis-cli XGROUP SETID q:dns dns-worker '$'
   redis-cli XINFO GROUPS q:dns   # check: lag 0, entries-read NULL
   ```

   The group name — from XINFO GROUPS (the defaults are `dns-worker`/
   `http-worker`/`render-worker`/`analyzer`). Before the fix, make sure
   the group's pending is empty (`pending=0` in the same output):
   SETID discards unacknowledged deliveries — with a live PEL, first
   let the worker finish its work or triage the PEL (§2.3).

2. **Do not restart Redis and do not flush** — live tasks sit in the
   streams and PELs: at the moment of the incident `q:render` held
   ~107K SPA tasks with neither `render_pending` flags nor dead
   entries — there would be no way to recover them (the loss family of
   the OOM incident — §2.6).

3. Resumption monitoring (the first minutes): XADD into `q:dns`
   flowing again, XLEN/entries-read growing, dns-worker inflight > 0,
   pending domains decreasing; the `queue_overflow` alert on `q:dns`
   cleared and not returning after the TrimLoop (XTRIM MINID).

   ```bash
   redis-cli XINFO STREAM q:dns   # length, entries-added growing
   redis-cli XINFO GROUPS q:dns   # entries-read/last-delivered-id growing, lag ≈ live depth
   ```

   In the example: lag 1753 → 0, after 90 s XLEN 44, entries-read 2144,
   inflight 32, ~33 domains/s; `q:dead` flat (366).

**Prevention.** Code fix T-168: the gate depth (and the metrics/alerts
built on it) is computed as `min(lag, XLEN) + pending` — undelivered
entries cannot exceed the stream's entries, the phantom is clipped
(min(1753, 1) = 1), and the real backlog is not distorted (while the
undelivered entries are still in the stream, lag ≤ XLEN and the clamp
is inactive). A related family — the TrimLoop WARN "acknowledged
stream prefix at the cap, the trim point is not advancing": the same
frozen group position (the `last-delivered-id` of an orphaned group
holds the minimum across groups in XTRIM MINID). The diagnostics are
the same (the stream's XINFO GROUPS); actions: a live consumer will
advance `last-delivered-id` itself; a dead group with an empty
pending — `XGROUP DESTROY` (non-empty pending — the entries will come
back via the worker's claim, do not touch the group; DESTROY loses the
PEL).

### 2.8. Counter deadlocks 40P01 (q:http redeliveries)

**Sign.** In the http-worker journal, errors of the form "daily checks
counter: ERROR: deadlock detected (SQLSTATE 40P01)" on the
`checks:YYYY-MM-DD` upsert (`crawler_counters`) inside the
result-recording transaction; the `q:http` task is not acknowledged
and goes to redelivery — every retry costs a full HTTP episode. The
PostgreSQL journal (`log_lock_waits = on`) shows a pair of
`process N still waiting …` / `deadlock found` with the texts of both
statements.

**Cause.** A lock-ordering inversion in one of the `crawler_counters`
writers: the convention is "domains → zones → counters, counters
last" (the invariant and the writer list — [`schema.md`](schema.md),
the `crawler_counters` section). Two incidents, two mechanisms:

- the retry wave (9 cases in 3 h) — `scheduler.RetryOnce`: two
  statements of a batch in one transaction grabbed the domains rows of
  the second half while already holding the zones and `retry_mature:`
  buckets — the "counters → domains" edge against the http-worker's
  result recording closed the cycle. The fix — separate transactions
  for the batch decisions (`retryDecide`).
- 07.11.2026 (~3–4 per hour; the 15ff3a3 fix did not help — the edge
  was a different one) — the unreferenced DML-CTEs `zc`/`rm` of ONE
  statement of the dns-worker's result recording
  (`resultUpdateStatusSQL`, autocommit): PostgreSQL does not guarantee
  their execution order; in production they executed as `rm → zc` —
  the statement held a counter row and waited for a zone (the
  "counters → zones" edge) against the http-worker `RecordAndPublish`,
  which holds zones until Commit and waits for `retry_mature:today`;
  the same edge came from the scheduler's `retryRequeueSQL`. The
  T-267 fix moves the counters out of the zones statement into
  separate statements (the DNS-stage increment — autocommit after the
  recording; the requeue decrement — a second statement of the
  decision transaction; the price — a window of bucket non-atomicity
  ±1 until the nearest reconciliation, ≤ 30 min — safe; the monitor is
  the authority for the family).

**Actions.** No data repair is needed: recording is idempotent
(at-least-once, spec 3.4), redeliveries are safe but costly. If the
errors return on fresh code — look for a NEW writer with an inversion:
a statement or transaction that grabs `domains`/`zones` rows after
`crawler_counters`/`zones` (grep for `crawler_counters` in internal/,
cross-check the ordering — schema.md), or zones and counters mixed as
unreferenced DML-CTEs of one statement. The deadlock is detected
automatically (`deadlock_timeout` 1 s); the victim rolls back and
retries.

**The T-268 guard (application-level retry).** The pipeline's hot
spots run under the `db.RetryDeadlock` wrapper
(`internal/db/deadlock.go`): up to two retries with jitter and a pause
cap, only for 40P01 and only for idempotent operations — the
http-worker's result-recording transaction (`RecordAndPublish`), the
scheduler's retry decision transaction (`retryDecide`), the
autocommit statements of DNS-episode recording (`RecordEpisode` — the
command retries without a second DNS episode). The scheduler's
temp_error drain survives a batch error (up to `DefaultRetryDrainFails`
= 3 consecutive failures — a persistent failure waits for the next
tick). Therefore isolated 40P01s in the service journals after T-268
are the guard working as designed: watch the
`crawler_db_deadlock_retried_total{service}` counter (docs/services.md)
— the rate of absorbed deadlocks. Sustained metric growth with
non-zero losses is that same new lock cycle: an ordering inversion
somewhere in the code — fix the cause (cross-check — schema.md);
retrying only masks it.

### 2.9. Growth of `crawler_scheduler_lease_null_returned_total` (queued rows without queued_at)

**Sign.** The `crawler_scheduler_lease_null_returned_total` counter
(scheduler) is non-zero and growing; the planner journal shows the
Warn "repair pass: queued rows without queued_at returned to pending".

**Cause.** The T-028 invariant "`check_state = queued` ⇒
`queued_at IS NOT NULL`" is violated (the state machine spec —
`internal/scheduler/checkstate.go`, invariant I1). The regular writers
maintain the invariant: the transition to queued writes
`queued_at = now()` in the same statement as `check_state`
(`internal/scheduler/enqueue.go`). Sources of the violation:

- manual SQL: `UPDATE domains SET check_state = 1 …` without
  `queued_at` — e.g. "let's move the domains to checking" bypassing
  `crawlerctl recheck` (the proper command moves them to pending and
  resets `queued_at`);
- a writer bug: new code setting queued outside the enqueue statement.

Before T-255 such rows hung in queued forever: the lease pass selects
by the age of `queued_at`, and NULL is filtered out by the comparison.
Now the repair pass of the LeaseWatch cycle
(`internal/scheduler/leasenull.go`) returns them to pending in batches
— the dispatcher republishes the check (the retry is idempotent,
spec 3.4). The event by itself is not an outage: the data heals
automatically, and the domain loses at most the lease window until the
next check.

**Actions.** A non-zero steady state — find the writer that sets
queued without the stamp; otherwise the guard will return the same
rows endlessly (each return is a repeated domain check, wasted
pipeline work):

```sql
-- who is currently queued without the stamp (until the nearest guard pass):
SELECT zone_id, id, label, status FROM domains
WHERE check_state = 1 AND queued_at IS NULL;
```

Check the database operations log around the counter's first rise:
manual edits (who/what), a deploy of new code. If the source is code,
verify the writer against the transition spec (checkstate.go — the
only transition into queued is the scheduler's enqueue R1). If the
source is a one-off manual edit, nothing needs doing: the guard has
already returned the rows; the counter is monotonic, it never "returns
to zero" — the health criterion is a zero growth rate (`rate(...) ≈ 0`),
not a zero value.

### 2.10. Domains with a stuck status (new ∧ done): the compensator and manual operations

**Sign.** The `crawler_domains_new_done_stale` gauge > 0 (the
`crawler_counters['new_done_stale']` row, written by the
reconciliation pass every `monitor.zone_stats_interval`) and/or the
`domains_new_done_stale` alert (a streak of 2 snapshots); the
`crawler_scheduler_stale_requeued_total` counter is growing — the
compensator is already draining.

**What it is.** The FR-2.2 anomaly (TZ-fsm §3): the DNS stage recorded
the "has addresses" branch (`check_state = done`, the status remains
new — the HTTP stage decides the outcome), and the `q:http` task was
lost AFTER the XADD — the delivery will never happen. Causes: a Redis
restart without persistence ([`redis.md`](redis.md) §5, the
`redis_restart` alert — a signal at the moment of the event), entry
deaths in an OOM window
([`load-measurements.md`](load-measurements.md) §11.1), a stream-trim
failure. The lease pass does NOT see such a row (outside queued), and
the age-based recheck matures only after `recheck_interval` (~half a
year) — hence the compensator.

**The compensator (T-270).** The scheduler's hourly cycle
`StaleRequeueLoop` (`internal/scheduler/stalerequeue.go`, the R15
transition of the check_state spec) returns
`status = new ∧ check_state = done ∧ last_checked_at` older than a day
to `pending` in batches of ≤1000 — the domain goes through the
pipeline again. The threshold is the gauge's window (24 h over the
date column ≡ `last_checked_at < current_date`; the cohort matures at
midnight — like the retry windows of T-031/T-202); today's in-flight
tasks are invisible to the compensator altogether. The price of the
date granularity is the midnight boundary effect: a q:http backlog
that survived midnight is drained once, with a duplicate episode (the
duplicate is resolved by the A3 asymmetry — a repeat HTTP-stage
delivery overwrites the result and republishes q:analyze); a counter
spike in the first hour after midnight with a non-empty q:http is the
normal boundary effect, not losses. The cycle's guard is a point read
of the `new_done_stale` row: in steady state (the cohort healed) the
hourly tick costs a single PK lookup, domains is not scanned; the
guard's freshness is the reconciliation interval (30 min), the total
repair delay ≤ ~1.5 h. Manual treatment of the cohort
(`crawlerctl recheck`) is NOT needed and not recommended: the code
heals itself, and the repeat episode is idempotent.

**Actions.** When the alert fires, action is usually not required:

1. Watch that the compensator drains: the
   `crawler_scheduler_stale_requeued_total` counter grows and the
   gauge falls to 0 within an hour after a non-zero reconciliation
   (the level's freshness — `zone_stats_interval`).
2. Cross-check the cause: `redis_restart` in parallel — the restart
   loss class (q:dns was returned by the lease at the same time);
   without a restart — the http-worker journal for the loss window
   (an OOM window, a crash) and §2.5/§2.6.
3. The cohort is not draining (the gauge is stuck, the counter is not
   growing):
   - is the scheduler alive? (`systemctl status crawler-scheduler`,
     the Warn "compensator drain" in the journal);
   - is the monitor alive? The guard reads the reconciliation row — a
     stalled monitor keeps the level frozen and does NOT open the
     compensator (check `zones.updated_at`, the `progress_stall`
     alert);
   - rows are not transitioning while the guard is open — capture the
     EXPLAIN of the selection (the pinned seq scan with early stop,
     T-191) and check the journal: a batch error repeating hourly
     (each next tick) — a Warn in the scheduler journal;
   - requeue a domain point-wise outside the compensator —
     `crawlerctl recheck <domain>` (a regular manual operation, §2.3):
     the guard is closed (e.g. the monitor is under repair) but the
     check is needed now.
4. Future cohorts are closed by the same cycle automatically: after a
   Redis restart expect a counter spike and the gauge draining to
   zero; a sustained non-zero rate of
   `crawler_scheduler_stale_requeued_total` in steady state (without
   restarts/OOM windows) — losses are happening CONSTANTLY:
   investigate via the http-worker journal and the `redis_restart`
   alert; precedent — the 22–29.10.2026 incident week (1865 domains).

Find the cohort by hand (guard closed / for cross-checking):

```sql
SELECT zone_id, id, label, last_checked_at FROM domains
WHERE status = 0 AND check_state = 2
  AND last_checked_at < current_date;   -- the 24 h window at date precision
```

## 3. Re-analysis of results under new rules

Changing the Laravel rule set does not require re-downloading sites:
the analyzer reads the stored content from storage (spec 5.8 "passive
analysis"). Mechanics — [`analyzer.md`](analyzer.md) "Re-analysis";
the rule format — [`configuration.md`](configuration.md), the
`analyzer` section.

### Steps

1. **Edit the rules** — YAML in `rules/` (dev) or `/etc/crawler/rules`
   (production). The rule set version (`rules_version`) is the hash of
   the detection (`target`/`match`/`regex`/`weight`/version bounds);
   editing `description` and comments does **not** change the version —
   no re-analysis is needed.
2. **An offline check** — `make test-rules`: a run over the
   `testdata/laravel-samples` corpus, cross-checking `is_laravel`/the
   version range/the matched-rule set against `meta.yaml`.
3. **Apply without a restart** — SIGHUP the analyzer:

   ```bash
   make rules-reload                              # dev
   systemctl kill -s HUP crawler-analyzer         # production
   ```

   A broken rule set does not stop the service — the previous one keeps
   working (a Warn in the log); the retry happens on the next SIGHUP.
4. **Automatic re-analysis** starts on its own: a rule set version
   change triggers an out-of-turn drain of the analyzer's re-analysis
   cycle (enqueues into `q:analyze` in batches of 5000, under the
   backpressure gate). Progress is visible in the `q:analyze` depth
   (`crawlerctl status`) and in the share of rows with the new
   `rules_version` in the database:

   ```sql
   -- make psql
   SELECT rules_version, count(*) FROM subdomain_results GROUP BY 1 ORDER BY 2 DESC;
   ```

5. **Manual run / control** — the same drain with one command:

   ```bash
   make reanalyze                          # dev; production: crawlerctl reanalyze
   crawlerctl reanalyze [--zone NAME]       # limit to a zone (the name in any form)
   ```

   The command prints the rule set version ("ruleset vN") and the
   enqueue counter; "no stale results — re-analysis not required"
   means everything is current. Lost MAXLEN ~ enqueues will be picked
   up by the analyzer's hourly cycle.
6. **Check the result** — `crawlerctl domain <name>` (the rule
   version, score, range, hits) or SQL:

   ```sql
   SELECT count(*) FROM laravel_domains;             -- confident (score ≥ threshold)
   SELECT count(*) FROM laravel_findings WHERE NOT is_laravel;  -- weak
   ```

### The threshold caveat

Changing only `analyzer.laravel_min_score` does **not** change
`rules_version` (the version is the rule set hash, the threshold is
configuration): accumulated finds are not re-evaluated, and
`is_laravel` is recomputed only for newly analyzed entries (fresh
checks, renders, re-analysis after rule edits). To change the
threshold "retroactively" over all data — edit any rule weight (it
shifts the version and triggers re-analysis) or wait for natural
rechecks.

## 4. DNS resolver failure (switching to the fallbacks)

The DNS stage's resolvers form a chain: the local unbound
(`dns.resolvers`) and the fallbacks (`dns.fallback_resolvers`,
spec 5.4). An exchange that exhausted its attempts on a network
failure of the local resolver is retried against the fallbacks until
the first answer (T-107); a server answer with any RCODE (including
SERVFAIL) is not a switch. Mechanics and configuration —
[`configuration.md`](configuration.md), the `dns` section.

### 4.1. Signs

- `rate(crawler_dns_fallback_switches_total) > 0` — the local one is
  not answering at the transport level and a fallback is working: the
  pipeline is alive, but the latency of every DNS exchange grew by the
  failed resolver's retry cycle (`dns.timeout` × (`dns.retries`+1),
  ~6.4 s by default);
- `rate(crawler_dns_fallback_exhausted_total) > 0` — nobody answered
  (the local one and the fallbacks): these domains get `temp_error`
  and a retry after `scheduler.temp_error_retry_after`;
- the Warn "resolver startup probe: no response" at dns-worker start —
  part of the chain is dead (the service kept working);
- the startup error "startup probe: no resolver in the chain responds"
  — all are dead; systemd restarts the service until it is fixed.

### 4.2. Actions

1. Check the local unbound: `journalctl -t unbound` (production) or
   `make logs SERVICE=unbound` (dev); typical causes — a process
   crash/restart, memory exhaustion (the budget ≈ 0.7 GB, spec 3.2).
2. Check the network reachability of the fallbacks from item 4.1: if
   `crawler_dns_fallback_exhausted_total` grows while the local one is
   silent, the public resolvers are unreachable too (outbound UDP/53
   filtering?) — no exchanges until either side recovers.
3. Recovery of the local resolver is picked up automatically: the
   switch does not "remember" a dead resolver — the next exchange
   after it comes back is served by the local one (the switches metric
   returns to a zero rate).
4. If the local resolver is dead for a long time and the fallbacks are
   not configured — stop the dns-worker
   (`systemctl stop crawler-dns-worker`) until it is fixed: every
   episode without resolvers puts domains into `temp_error` and burns
   recheck attempts (`scheduler.temp_error_max_retries`).

A resolver failure needs no q:dead triage (section 2): the domains
come back through the regular temp_error cycle (T-031).

## 5. One-off cleanup of accumulated content and the jsonb trim (the T-167 campaign)

Data checked **before** retention was introduced (T-166) does not fall
under the same criteria automatically, purely because of its date: its
age already exceeds the window, and the mass of history (the
2026-10-23 evening measurement: content **20 GB / ≈3.1M** `contents`
blocks, a 261 MB index; `subdomain_results` **≈3.7 GB** over ≈1.14M
live checked rows) is drained by an explicit campaign — once; after
that the window is held by the regular cycle. The selection criteria
and the cleanup mechanics — [`content.md`](content.md) "Content
retention and the non-find trim (T-166)": not repeated here. The
campaign's differences from the background cycle: reduced batches under
CPU starvation (T-158 still open) with pauses, VACUUM (ANALYZE) of the
affected partitions after the drain, monitoring of pack compaction
under mass orphan blocks, and the untouchability of the
`render_pending` cohort until the `q:render` drain (T-159: a lag of
≈99K against a 100K watermark) — those entries are excluded by the
T-166 selection itself, and entries with still-unconsumed
`q:render`/`q:analyze` messages are excluded by the exact protection
of the T-166/T-167 fix (a snapshot of unconsumed stream keys,
[`content.md`](content.md) "Exact protection by unconsumed messages"):
the regular cycle reworks them after the flags are cleared and the
queues are triaged.

Run after deploying the wave containing T-166. The `domains` registry
is not touched.

### 5.1. Baseline measurement (before any action)

All acceptance checkpoints (§5.5) are verified against this
measurement — record the findings in the work log
([`operations.md`](operations.md) §5).

```bash
# the data volume and the content directory (disk_avail — the main criterion)
df -h /var/lib/crawler
du -sh /var/lib/crawler/content
# queues (the q:render/q:analyze depths — interlock/gates; group lags —
# the exact protection of the T-166/T-167 fix; q:dead — acceptance)
for s in q:dns q:http q:render q:analyze q:dead; do printf '%-9s ' "$s"; redis-cli XLEN "$s"; done
for s in q:render q:analyze; do printf 'lag %-9s ' "$s"; redis-cli --raw XINFO GROUPS "$s" | paste - - | awk '$1=="lag"{print $2}'; done
# PSI CPU (the §12 measurement steady state — 91–95% some)
grep -E '^(some|full)' /proc/pressure/cpu
```

```sql
-- psql "$CRAWLER_PG_DSN" (as in §2.5); dev — make psql

-- content accounting: live/dead rows and bytes (what will die from
-- ref clearing, and what is already dead — pack compaction territory)
SELECT count(*) FILTER (WHERE refcount > 0)      AS live_rows,
       count(*) FILTER (WHERE refcount = 0)      AS dead_rows,
       pg_size_pretty(sum(len)::bigint)          AS block_bytes,
       pg_size_pretty(pg_total_relation_size('contents')) AS contents_size
FROM contents;

-- the results journal: the sum of the leaf partitions (the parent is
-- partitioned — it holds no storage; pg_total_relation_size on it
-- would return 0; migration 000003: subdomain_results_z<zone>_h<k>,
-- fresh zones land in subdomain_results_default)
SELECT pg_size_pretty(sum(pg_total_relation_size(relid))::bigint) AS journal_size
FROM pg_stat_user_tables
WHERE relname LIKE 'subdomain\_results\_%';
SELECT relname, n_live_tup, n_dead_tup,
       pg_size_pretty(pg_total_relation_size(relid)) AS size
FROM pg_stat_user_tables
WHERE relname LIKE 'subdomain\_results\_%'
ORDER BY pg_total_relation_size(relid) DESC;

-- inviolable data (the counts before/after — acceptance §5.5)
SELECT count(*) FROM laravel_domains;   -- confident finds
SELECT count(*) FROM laravel_findings;  -- all rule hits

-- the finds' content anchor: find rows with a content reference
-- (the number must not change; find content is always stored)
SELECT count(*) FROM laravel_domains AS ld
JOIN subdomain_results AS r
  ON r.zone_id = ld.zone_id AND r.domain_id = ld.domain_id
WHERE r.content_hash IS NOT NULL;

-- the render_pending cohort (do NOT touch until the q:render drain, T-159)
SELECT count(*) FROM domains WHERE flags & 2 <> 0;

-- the campaign volume estimate (an upper bound: without excluding
-- finds and render_pending; heavy — run it in a window; the result is
-- a progress reference, the drain itself gives the exact number)
SELECT count(*) FROM subdomain_results
WHERE checked_at < now() - interval '6 hours'
  AND (content_hash IS NOT NULL OR headers IS NOT NULL OR cookies IS NOT NULL
       OR tls IS NOT NULL OR redirects IS NOT NULL OR addrs IS NOT NULL
       OR cname IS NOT NULL);
```

### 5.2. Path A (primary): the regular T-166 cycle

The history's age already exceeds the `content.retention` window
(6 h) — the very first `RetentionLoop` pass (it starts right away with
the http-worker, without waiting for the hourly tick) drains the
volume in `content.retention_batch` batches until the set is exhausted
or the `q:analyze` interlock trips. The expected scale: ≈1.14M rows ÷
10,000 = **≈114 batches** for the first pass; seconds per batch —
minutes for the drain (the `pass_seconds` histogram has the 300 s
bucket exactly for this backfill). Tens of minutes per HOURLY
steady-state pass is a sign of CPU starvation (see "Slowdown" below).
Since T-185 the first pass is a catch-up circle with a Sort of the
slice (the slice's candidates are sorted, not the visited heap): for
the STARTING circle, tens of minutes is the upper estimate — no
reaction needed; the trigger is a systematic (not one-off) excess of
the 300 s bucket by the starting circle per `pass_seconds`/the T-177
journal: then enable the composite index `(checked_at, zone_id,
domain_id, subdomain_id, variant)` (a DDL candidate, spec T-185).
Subsequent hourly passes process only the inflow (≈3M rows/day ≈ 13
batches per pass) — literally so since T-185: the drain cursor
survives passes, and the hourly full circles are gone. The interlock
is the `q:analyze` depth ≥ `scheduler.queue_high_watermark` (100,000):
the pass is skipped entirely, `skips_total` grows — this is the
regular protection of bodies from being cleared, and the campaign
continues with the next pass.

The exact protection by unconsumed messages (the T-166/T-167 fix; the
wave deploy is mandatory before the campaign starts): entries whose
Result messages are not yet consumed by `q:render`/`q:analyze` are
excluded from the batches by a snapshot of the streams — path A
**never touches** entries waiting for their queues (the
`render_pending` flag only covers unpublished ones, the interlock only
the depth at the threshold). With a stalled `q:render` (the lag is now
≈99K) the pass will keep excluding those entries in batches:
`crawler_content_retention_deferred_total{stream="q:render"}` grows —
this is the protection working, not an error; after the stream drains
(T-159) the exclusions stop and the drain resumes. A stream read error
— a conservative skip of the pass
(`passes{result="skipped_pending"}`, `pending_errors_total`): the
queue entries are untouched; fix the Redis access.

Behavior after a database failure (T-271 after the 08–09.11.2026
incident, refined by T-273 after the 09–10.11 regression and T-274):
the retention batch selection runs inside a plan-pinning tx wrapper
(`SET LOCAL
enable_seqscan/hashjoin/mergejoin/bitmapscan = off` + a
`statement_timeout` of 7200 s and a client ctx deadline — a hung query
is impossible); the 7200 s cap (T-274; it was 3600 under T-273 and 600
under T-271; configurable via `content.retention_select_timeout`
without a deploy) must cover the first catch-up pass from the zero
cursor after a retention outage (a clean-prefix skip of pure rows,
30–60 min at production volume, over an hour under front load) — a
deadline smaller than the pass cost killed the pass before the commit:
the cursor did not move, and every retry ran the same doomed scan (an
endless loop). An errored pass puts the ticker into a **15-minute
pause from the end of the pass** (T-273: a reset after every error,
the pause ≥ the pass duration + 15 min): a Warn "content retention
pass" in the journal once per error, no immediate restarts against a
just-raised PG; the first successful pass restores the hourly
interval, and the `crawler_content_retention_errors_total` counter
(T-273) grows on every failed pass — a series growing with zero
`passes_total{result="ok"}` means the drain is stalled; read the Warn
heading. What the operator sees after a PG restart: one Warn, then
either a successful pass (`passes_total{result="ok"}`) or another Warn
no earlier than 15 minutes later — this is the regular backoff; there
is no need to restart the http-worker (the in-memory cursor survives
the pause; since T-274 the cursor is persistent in the database after
all — the `retention_drain_cursor` table, migration 000020: an
http-worker restart continues the drain from the saved position, and a
full catch-up from zero remains only for the first pass after an
outage or a forced catch-up circle).
The heavy scans of retention and of the late render republisher are
serialized by the shared `db.ScanGate` gate: the republisher's minute
passes may wait for the retention selection (≤7200 s) — imperceptible
on the monthly loss-guard cycle.

**Progress monitoring** (after the deploy, on the server):

```bash
# cycle metrics (the http-worker listens on :9104)
curl -s 127.0.0.1:9104/metrics | grep crawler_content_retention
# the journal of passes and anomalies
journalctl -t crawler-http-worker -f | grep -E 'retention|compaction'
# queue depths and stage rates
crawlerctl status
```

Expected readings: `crawler_content_retention_rows_total` grows toward
the §5.1 estimate (the first pass is the main jump, after that — the
inflow); `contents_total` — deleted `contents` rows;
`freed_bytes_total` — the compressed bytes of dead blocks (the claim
on the space; the space is physically returned by pack compaction,
§5.4); `passes_total{result="ok"}` — one pass per hour;
`skips_total` — only when the `q:analyze` depth is at the watermark;
`deferred_total{stream}` — entries excluded by the exact protection
(grows when the queues are stalled — expected until the `q:render`
drain, T-159); `pending_errors_total` — **always 0** (growth — stop:
the protection cannot read the streams); `bad_hashes_total` and
`missing_refs_total` — **always 0** (growth — stop, see §5.5).

**Slowdown/pause** — on a sustained (minutes-long) violation of any of
the conditions: PSI CPU some avg10 > 95% (`grep '^some'
/proc/pressure/cpu`), stage rates below the §12 measurement (HTTP < 20
domains/s etc.), the `q:render`/`q:analyze` depths growing toward the
watermark:

1. Reduce the batch in `/etc/crawler/config.yaml`:

   ```yaml
   content:
     retention_batch: 2000     # was 10000; the campaign's range under CPU starvation
   ```

2. `systemctl restart crawler-http-worker` — safe for the data: every
   batch is a short atomic transaction, a restart between batches drops
   nothing, an interrupted batch rolls back entirely; the cleaned data
   is not scanned again (cleaned rows drop out of the selection —
   idempotency, spec 3.4). But the in-memory drain cursor is lost this
   way (T-185): the first pass after the restart is the starting
   catch-up circle from the zero cursor — the most expensive scenario
   (a Sort of the slice over the whole window, see the estimate in
   §5.2). During a campaign under CPU starvation, restart only when
   necessary and outside the peak hour.
2a. (a soft alternative) pause the campaign with the interlock: raise
   `content.retention_analyze_watermark` to the current `q:analyze`
   depth — passes start being skipped (`skips_total` grows) without a
   restart.
3. After the campaign, restore `retention_batch: 10000` the same way.

**What NOT to do:**

- do not touch the `render_pending` cohort until the `q:render` drain
  (T-159): do not clear the flags by hand, do not republish `q:render`
  in batches — a render wave on a starved CPU would collapse on its
  own; the excluded entries are reworked by the regular T-166 cycle
  after the flags are cleared by the T-066 rework pass;
- do not run path B (§5.3) in parallel with a running path A drain:
  the UPDATE guards make it safe but pointless — the CPU starvation is
  shared, pick one path;
- do not raise `retention_batch` above the working range of 10–50K:
  long transactions hold row locks and cause WAL spikes.

### 5.3. Path B (accelerated): operational SQL

When time is short or the interlocks eat up path A's passes — a drain
via the `deploy/sql/t167-retention-backfill.sh` script: a manual
mirror of the T-166 cycle's SQL (`internal/content/retention.go`) —
the same criteria, the same guards (a re-check of age +
`content_hash IS NOT DISTINCT FROM` against races with overwriting,
`GREATEST(refcount − n, 0)`, `DELETE … refcount <= 0`), cursor
pagination by PK, every batch a separate `BEGIN…COMMIT` transaction.
Interrupting (Ctrl-C) is safe at any moment, a re-run is idempotent,
and it does not conflict with the regular cycle (but is not needed —
see §5.2).

The differences from the cycle — the campaign mode: a default batch of
2000 (under CPU starvation), a 5 s pause between batches, and before
every batch the gates — PSI CPU some avg10 (≤ 95% by default) and the
`q:render`/`q:analyze` depths on two levels: the coarse `XLEN`
(< 100,000 by default; the exact depth — `crawlerctl status`) and the
exact **lag of the consumer groups** (`XINFO GROUPS` → `lag`,
Redis 7+; `T167_RENDER_LAG`/`T167_ANALYZE_LAG`, 1000 by default —
≈half a minute of the `q:analyze` inflow, "the consumers almost keep
up"). The lag is the stream's undelivered tail: it is exactly what
holds entries back from cleanup at the exact protection of the T-166
cycle, and path B's gates align the script with the same guarantee: a
pause while the lag is ≥ the threshold, entries waiting for a consumer
are not touched. No groups on the stream — the lag is computed as
XLEN (everything in it is undelivered); a `nil` lag (a trim of
undelivered entries, or Redis < 7) — the lag gates are skipped with a
WARN, and XLEN applies. The gates **wait** (up to
`T167_GATE_MAX_WAIT`, an hour by default) — they do not crash the
script. Accounting anomalies (`bad_hashes`/`missing_refs` — analogs of
the T-166 counters) stop the script: refcount was not changed for
them, but growth is a writer/accounting error — investigate before
continuing. Exit code: regular outcomes (the drain exhausted, the
`max_batches` cap) — 0; emergency ones (a batch error, a gate timeout,
accounting anomalies) — 1: command chains and automation see an
unfinished drain.

**With a stalled `q:render`, do not run path B until the stream
drains (T-159):** a lag of ≈99K against a threshold of 1000 will not
release the gates — the script will honestly stop on the timeout; this
is the protection working as expected (entries waiting for a render
must not be cleared), first bring the render-worker back or drain the
stream, then repeat the run (it is idempotent).

```bash
# from a repository checkout on the server, or via ssh without copying:
ssh root@155.212.174.88 'CRAWLER_PG_DSN=<dsn> bash -s' -- \
    < deploy/sql/t167-retention-backfill.sh

# tuning variables (the default values):
#   T167_BATCH=2000          rows per batch transaction
#   T167_RETENTION='6 hours' the window, same as content.retention
#   T167_PAUSE=5             the pause between batches, s
#   T167_MAX_BATCHES=1000    a guard against an endless loop (2M rows)
#   T167_PSI_MAX=95          gates: PSI CPU some avg10, %
#   T167_QUEUE_MAX=100000    gates: the XLEN of q:render/q:analyze (coarse)
#   T167_RENDER_LAG=1000     gates: the q:render group's lag (Redis 7+)
#   T167_ANALYZE_LAG=1000    gates: the q:analyze group's lag
#   T167_GATE_MAX_WAIT=3600  how long the gates wait before stopping, s
```

The script prints the result of every batch
(candidates/cleared/refs/contents deleted/cursor) and the total result
(duplicated into journald, the `crawler-t167` tag). Monitoring in
parallel — as in §5.2 (`crawlerctl status`, PSI, metrics: path B does
not move `crawler_content_retention_*` — its results are only in the
script's stdout/journald; cross-check the remaining candidates with
the §5.1 estimate query). At `bad_hashes`/`missing_refs` > 0 — stop
and investigate; at near-watermark depths the gates hold the pace
themselves.

### 5.4. Final steps (after the drain, both paths)

1. **VACUUM (ANALYZE) the affected `subdomain_results` partitions.**
   The trim NULLs out jsonb and refs — dead row versions; a plain
   VACUUM removes the dead versions and returns pages, and ANALYZE
   refreshes the statistics after a strong change in row width. Plain,
   **not FULL**: FULL takes an AccessExclusiveLock and rewrites the
   tables — the space does not need it; autovacuum would return the
   dead HOT versions anyway, the campaign merely forces the moment.
   VACUUM (ANALYZE) locks only against concurrent vacuums
   (ShareUpdateExclusive), the pipeline does not stop; run one or two
   partitions at a time, watching the PSI:

   ```bash
   # affected partitions with dead rows, heaviest first
   psql "$CRAWLER_PG_DSN" -Atc "
     SELECT format('VACUUM (ANALYZE) %I;', relname)
     FROM pg_stat_user_tables
     WHERE relname LIKE 'subdomain\_results\_%' AND n_dead_tup > 0
     ORDER BY n_dead_tup DESC" \
     | psql "$CRAWLER_PG_DSN" -v ON_ERROR_STOP=1
   ```

   (VACUUM cannot run inside a transaction — one command per line; the
   partitions `subdomain_results_z<zone>_h<hash>` and `…_default` —
   migration 000003. A re-run is safe.)

2. **Bloat check.** If `pgstattuple` is available —
   `CREATE EXTENSION IF NOT EXISTS pgstattuple;` and
   `pgstattuple_approx('<partition>')`: after VACUUM the
   `dead_tuple_percent` of the affected partitions is ≈0. Without the
   extension — before/after via the `pg_total_relation_size` sizes
   from the §5.1 measurement: a partition's size after VACUUM drops in
   proportion to the cleared jsonb (the acceptance bar — the whole
   results journal ≤ 1.5 GB, §5.5); the residual bloat is held down by
   autovacuum (`docs/db.md` — autovacuum for partitions).

3. **Pack compaction monitoring.** After the refs are cleared, the
   live-data share of almost every pack falls below the 0.5 threshold
   (`DefaultCompactThreshold` T-045, not changeable via configuration)
   — the hourly `CompactLoop` pass (http-worker) will rewrite almost
   all packs: live blocks are appended into new ones and old files are
   deleted. Watch:

   ```bash
   journalctl -t crawler-http-worker -f | grep 'compaction'
   # "pack compaction pass packs=N freed_bytes=…" — progress
   du -sh /var/lib/crawler/content      # the directory shrinks as packs are unlinked
   df -h /var/lib/crawler               # a temporary double footprint: live
                                        # blocks are rewritten before the old
                                        # pack is deleted
   ```

   The double footprint is small with a mass of dead blocks (a few GB
   of live bytes out of 20), but when `disk_avail` is near the
   `content.disk_free_percent` threshold, compaction itself defers
   packs with live blocks (C-17): packs without live blocks are only
   deleted — that is exactly the space being freed, and the drain is
   not blocked. Rebuilding ~20 GB of reads+writes on NVMe takes hours;
   the space returns gradually — do not expect an instant `du` to
   zero.

4. **Spot-check inviolability** (all numbers — against §5.1):

   ```sql
   SELECT count(*) FROM laravel_domains;    -- = the baseline
   SELECT count(*) FROM laravel_findings;   -- = the baseline
   SELECT count(*) FROM laravel_domains AS ld              -- the anchor = the baseline
   JOIN subdomain_results AS r
     ON r.zone_id = ld.zone_id AND r.domain_id = ld.domain_id
   WHERE r.content_hash IS NOT NULL;
   ```

   ```bash
   crawlerctl domain --content <the name of a find from laravel_domains>   # the content is readable
   redis-cli XLEN q:dead                                          # = the baseline
   ```

### 5.5. Acceptance criteria

| Criterion | Target | How to measure |
|---|---|---|
| Volume free space | **+≥15 GB** over the baseline | `df -h` (§5.1 ↔ after §5.4) |
| Content directory | **≤3–5 GB** (finds, render_pending, 6 h freshness) | `du -sh /var/lib/crawler/content` |
| `subdomain_results` after VACUUM | **≤1.5 GB** | the sum of `pg_total_relation_size` over leaf partitions (§5.1) |
| Finds | the `laravel_domains`/`laravel_findings` counters and the anchor JOIN equal the baseline; find content is readable | §5.4 item 4 |
| `q:dead` | **0 growth** over the campaign | `XLEN` (§5.1 ↔ the final check) |
| Pipeline | stage rates within the §12 measurement: HTTP 21–23 domains/s, DNS 26–28, render 0.20–0.23 pages/s; PSI CPU some ≤95% | `crawlerctl status`, the t092 sampler, `/proc/pressure/cpu` |
| Content accounting | `bad_hashes_total` = `missing_refs_total` = 0 (path A — metrics; path B — the script's summary) | §5.2/§5.3 |

The numbers do not match — typical causes:

- **the disk does not grow despite a completed drain** (`rows_total`
  near the estimate, `contents_total` grown): compaction has not
  caught up yet or defers packs per C-17 — check the journal for the
  "pack compaction pass" and §5.4 item 3; the residual of dead
  `contents` rows (§5.1, `dead_rows`) is its input;
- **`skips_total` grows** — the `q:analyze` depth is at the watermark:
  the analyzer cannot keep up, the bodies are still needed; the
  campaign honestly waits (or path B — the gates), forcing is not
  allowed;
- **`bad_hashes_total`/`missing_refs_total` > 0** — stop and
  investigate against the T-044/T-056 invariants (a writer/accounting
  error); resume the drain after the cause is fixed;
- **rates/PSI outside §12** — slow down (§5.2: reduce
  `retention_batch`; §5.3: raise `T167_PAUSE`/reduce `T167_BATCH`) or
  stop the script — the drain resumes from where it stopped
  (idempotency).

After the campaign: restore `content.retention_batch` if it was
changed (§5.2) and record the work in the log
([`operations.md`](operations.md) §5).

## 6. Manual operations on the render_pending bit (T-175)

The `render_pending` bit (bit 1 of `domains.flags`) and the render-debt
counter `crawler_counters['render_pending']` (migration 000015) are
linked by an invariant: **the counter = the number of rows with the
bit** (no status filter). Bit transitions in code move the pair
atomically (±1 in the same transaction, internal/httpworker/renderq.go),
so **any manual edit of the bit must edit the counter in the same
transaction** — otherwise the `crawler_render_pending_domains` gauge,
the "Render debt" row of the load report, and the `render_debt` alert
start lying (and a missing counter row stops the recording of results
with the flag — the worker fails honestly on the missing row; that is
the protection against silent lying).

A manual edit — the "bit + counter" pair in ONE statement, the shift
tied to the FACT of the transition (an unconditional ±1 when the bit is
already set/cleared — the guard returned 0 rows — would desync the
counter). Clearing the bit (example: a domain's content is
irrecoverably lost):

```sql
WITH cleared AS (
    UPDATE domains SET flags = flags & -3
    WHERE zone_id = $1 AND id = $2 AND (flags & 2) <> 0
    RETURNING 1
)
UPDATE crawler_counters
SET value = value - (SELECT count(*) FROM cleared)
WHERE name = 'render_pending'
RETURNING value;  -- empty = the counter row is missing (symptom below)
```

Setting the bit (returning domains to rework — a ready-made template
for the list from the §2.5 dead entries; the status guard protects
against a race with delete application: a transition to `deleted`
clears the bit, and re-setting it for a deleted domain without the
guard would leave an orphan bit, T-269):

```sql
WITH flagged AS (
    UPDATE domains SET flags = (flags | 2)::int2
    WHERE zone_id = $1 AND id = $2 AND (flags & 2) = 0 AND status <> 6
    RETURNING 1
)
UPDATE crawler_counters
SET value = value + (SELECT count(*) FROM flagged)
WHERE name = 'render_pending'
RETURNING value;
```

`UPDATE ... RETURNING` shows the counter's actual value; an empty
result of the second UPDATE (0 rows) means the
`crawler_counters['render_pending']` row is missing, see the symptom
below: the flag has by then already been set/cleared (the first CTE
always executes) while the counter was left behind — restore the row
and verify.

Symptom of a missing/broken counter row: a burst of result-recording
errors "render debt counter: 0 rows affected" from the http-worker
with `q:render` overflowing (every flagging transaction rolls back),
the `render_debt` alert in the "not measurable" state and — since
T-269 — errors of applying delete files with flagged domains ("counter
row 'render_pending' is missing"; the file stays in staging until the
row is restored) — restore the row (migration
000015 or a manual `INSERT ... VALUES ('render_pending', <the actual
count>)`), then verify below.

Symptom of the "bit without counter" desync (the rework pass guard,
T-181): with `value = 0` and live flags (manual SQL set the bit past
the counter) the `render_pending` drain stands still — the pass checks
the debt with a point read of the counter and never runs the heavy
selection; the symptom shows as "flags present, the rework pass
publishes 0" with a zero debt gauge. The reverse desync (`value > 0`,
no selectable flags) — the heavy selection runs on every tick without
the LIMIT early stop: a full pass over the corpus (tens of seconds,
like the verification below), the Warn "debt counter > 0, the
selection is empty" in the http-worker journal. The treatment in both
cases — the verification below and aligning the counter to the actual
`count(*)` (a negative `value` — the same remedy: drift <> 0).

Orphan bits — deleted domains carrying the flag. The regular path is
closed by T-186: applying a delete file clears the bit in the same
transaction with a counter decrement (apply_delete; the "not yet
deleted" guard does not decrement twice — at-least-once idempotency).
Sources of the residual: domains deleted BEFORE the T-186 deploy with
a live bit (a one-off tail), and manual-SQL drift. The symptom is the
old one: the counter sees them (the invariant has no status filter),
the pass's selection does not → `value` is > 0 forever, the
render_debt gauge/alert show phantom debt, and the pass pays a full
seq scan every tick (the Warn "debt counter > 0, the selection is
empty"). A one-off tail cleanup — the "bit + counter" pair per the
template above:

```sql
WITH cleared AS (
    UPDATE domains SET flags = flags & -3
    WHERE (flags & 2) <> 0 AND status = 6
    RETURNING 1
)
UPDATE crawler_counters
SET value = value - (SELECT count(*) FROM cleared)
WHERE name = 'render_pending'
RETURNING value;  -- empty = the counter row is missing (symptom above)
```

Verification after any questionable operations (a single seq scan — do
it in a window; on a corpus of 41.8M rows it takes tens of seconds):

```sql
-- one row: drift = 0 — the numbers match; -1 — the counter row is missing
-- (NULL without COALESCE does not compare equal to 0 and the
-- missing-row case would be invisible)
SELECT COALESCE((SELECT value FROM crawler_counters WHERE name = 'render_pending'), -1)
     - (SELECT count(*) FROM domains WHERE (flags & 2) <> 0) AS drift;
```

`drift <> 0` — align it by hand (UPDATE the counter to the actual
`count(*)`). Deleting or renaming the `'render_pending'` row is
forbidden: the migration creates it once, and result writers expect it
on every bit transition.

Returning the accumulated debt to work — the regular rework pass
(`RequeueRenderPending` of the http-worker cycle): it clears the flags
with the counter by itself and needs no manual involvement;
freezing/unfreezing the scheduler inflow for the `q:render` drain does
not touch the counter (the bit is set only by HTTP-episode results
when the render queue overflows).

### Manual edits alongside a live reconciliation (T-269, the outcome of the 08.11.2026 incident)

An incident during q:dead triage (prod-reports/2026-11-08-010800.md):
18 flags were set manually per the template above, the rework pass
cleared them with a pair of −18 — and the counter went to −18 (a shift
of −36 in one pass). The investigation (T-269) refuted the suspicion
of the monitor's reconciliation and found the culprit in delete-file
application: the decrement was counted over a SNAPSHOT of the bit
count while the bit was cleared against the current row version — a
concurrent clearing by the rework pass between the snapshot and the
lock decremented the counter twice. Fixed (the "bit × counter" pair
now follows the fact of the transition with a guard on the current
version); the reproduction — `test/integration/renderbit_test.go`
(TestRenderPendingBit_DeleteClearRace). Operational consequences for
manual edits:

- **The clearing and verification templates of this section are safe
  against races with the code** — the `(flags & 2)` guard and the
  delta over the `count(*)` from RETURNING count the actual
  transitions; racing with the rework pass or a delete application
  yields 0 rows and does not move the counter. The setting template
  needs the status guard (`status <> 6`, added by T-269): a delete
  application clears the bit in the same transaction, and re-setting
  it for a deleted domain without the guard would leave an orphan bit.
- **The monitor's reconciliation writes the aggregate's absolute value
  and always advances `updated_at`** — it cannot leave a "bit" of a
  second decrement behind. Its own window (the aggregate computed
  earlier, the upsert executed later) can overwrite concurrent
  transitions with the stale absolute: a false debt or a false zero
  for up to `monitor.zone_stats_interval`, self-healing on the next
  pass. When investigating a desync, check the row's `updated_at`:
  the reconciliation pass's time is its trace; if `updated_at` did not
  move but the value shifted, a delta writer wrote it (the rework
  pass / delete application) or manual SQL bypassed the code.
- **The pass's drift metric (`crawler_counters_drift`) does not
  measure pairing** (the counter's old value against the new one):
  only the verification below / `crawlerctl counters verify` can see
  the "counter vs the number of bits" desync.

## 7. Drift of the pipeline's incremental counters (T-178)

The pipeline state counters (the zone columns `domains_total` /
`domains_pending` / `domains_queued` / `domains_checked` /
`domains_laravel` and the `crawler_counters` rows: `status_*`, the
cumulatives `laravel_appeared_total` / `laravel_lost_total`,
`render_pending`, the daily `checks:YYYY-MM-DD` and
`temp_error:YYYY-MM-DD` — T-179/T-182, the retry maturity buckets
`retry_mature:YYYY-MM-DD` — T-202, the hourly rate buckets
`results:`/`rendered:YYYY-MM-DD"T"HH24` — T-203, the unanalyzed
reanalysis-tail buckets `reanalyze:YYYY-MM-DD"T"HH24` — T-204) keep
deltas in the transition transactions; the authority is the monitor's
combined reconciliation every `monitor.zone_stats_interval`. Non-zero
drift (`crawler_counters_drift`, the Warn "reconciliation: counter
drift" in the monitor journal) — a sign of a skipped transition, a
failure between service deploys, or manual SQL bypassing the code; the
pass fixes it itself. An operational consequence of `domains_pending`
drifting downward (T-183): a zone with a counter of 0 is invisible to
the dispatcher's selection (the "only zones with work" filter) — its
pending domains wait for the nearest reconciliation (≤
`monitor.zone_stats_interval`, 30 min by default) or a `recount`; an
inflated counter yields only an idle probe, without degradation. A
similar trust boundary at the retry maturity buckets (T-202): an
understated bucket sum keeps the guard of the heavy retry selection
closed — mature temp_error candidates wait for the nearest
reconciliation or a `recount` (the drain is merely postponed, ≤ one
interval); an inflated one yields idle seq scans per tick until the
pass. For the reanalysis tail buckets (T-204) the asymmetry is harsher
because of the closed guard's cursor jumping to the hour boundary:
understated (a negative drift of the decrements masks a positive
prev-bucket) — the tick neither reads out the window NOR keeps the
cursor in place, and the mature tail invisible to the buckets ends up
behind the cursor — a restart (the starting drain + the 24 h anchor)
or a full rule-change drain (the residual class of T-193 §4.2) will
pick it up; waiting for one reconciliation is not enough; an inflated
one yields one idle window pass per tick until the pass. The daily
`daily:checks` / `daily:temp_error` positions are the exception: they
are streaming (the increments count HTTP result-recording events, the
live window is the day's domains across all branches), a non-zero
value under live load is normal, no Warn is raised for them, they are
observed via the gauge; the `retry_mature:past` / `retry_mature:today`
buckets (T-202), the hourly rate buckets
`rendered:prev`/`rendered:cur`/`results:prev`/`results:cur` (T-203),
and the tail buckets `reanalyze:prev`/`reanalyze:cur` (T-204) have the
same streaming semantics (row-insertion, render, and stamp events
against the live rows of the window; the hourly buckets' drift only
slightly distorts the dashboard's rate between passes, the tail
buckets' drift — the guard's freshness — the pass realigns the
absolutes).

**When to act by hand.** The cycle's drift tends to zero in steady
state, but the reconciliation pass is three statements under READ
COMMITTED (read the stale ones → scan domains → write), so a one-off
transient drift under live load is possible even without a skipped
transition: the delta committed between the reads — the next pass
zeroes it. Act on a sustained drift (a repeat pass did not zero it; a
skipped transition — code bypassing the transition table, the
incremental-counters spec §6.2, or manual SQL — or a failure between
service deploys), or when the counters are needed "right now" (after
migrations, a restore from backup, manual edits of `domains` /
`zones` / `crawler_counters`). A note on `--tolerance`: the tolerance
is only needed for `counters verify` — its reads of the stored and
live aggregates are not atomic, and under live load a tolerance
absorbs isolated rate-of-transition discrepancies; the `status_*`
rows take no part in drift or verify at all (written only by the
reconciliation, the incremental-counters spec §13.3 — check their
freshness via `updated_at` in `counters list`); the daily `checks:`
row is also outside verify — its increments count HTTP
result-recording events, while the live aggregate is the day's domains
across all pipeline branches (DNS-terminal completions have no HTTP
episode): the regular discrepancy is the difference of the stages'
flows since the last pass (T-179, an accepted decision; observed via
the `daily:checks` position, not included in the Warn).

### Steps

1. **Read-only verification** — `crawlerctl counters verify` (exit
   code 0 — no discrepancies, 1 — drift present; it prints a "stored /
   aggregate" table; under live load `--tolerance N` absorbs isolated
   rate-of-transition discrepancies); for a single zone —
   `--zone NAME` (the global counters are always verified).
2. **Repair** — `crawlerctl counters recount` (the same pass as the
   monitor's: it overwrites the counters with aggregates; prints
   "before → after" for the drifting positions). `--zone NAME` limits
   the column overwrite to one zone — the global counters are always
   recomputed.
3. **Re-verify** — `crawlerctl counters verify` must return code 0; a
   repeated non-zero drift of the same positions is a systematic cause
   (see below), not a one-off failure.

### Systematic causes

- **A sustained drift of `zone:*:pending`/`queued`/`checked` for one
  zone** — transitions bypassing the code: a manual
  `UPDATE domains ... SET check_state`, pipeline-crawl scripts. Every
  manual change of `check_state`/`status` must be accompanied by the
  zone counters' delta (a sample — `internal/db/zoneshift.go`), or be
  performed before a reconciliation pass and covered by it.
- **Drift over `status_*` is not observed** — the status distribution
  is written only by the reconciliation (the incremental-counters spec
  §13.3); these positions are not part of the gauge/Warn/verify; their
  freshness — the rows' `updated_at` in `counters list`. Manual SQL
  over `domains.status` breaks the rows' consistency between passes —
  status edits only via the pipeline.
- **Drift of `global:laravel_*` and `zone:*:laravel`** — manual edits
  of `laravel_domains` / `laravel_history` bypassing the analyzer:
  only it writes those tables (T-072/T-118); edits go through
  re-analysis.
- **A non-zero `daily:checks` is normal under load** (T-179): the
  position is streaming — the difference between the "HTTP
  result-recording events" and the "day's domains across all branches"
  since the last pass: DNS-terminal completions (`not_delegated`,
  `invalid` etc.) push it up, the day's repeat episodes do too (an
  increment without the live window growing), mutually compensating.
  This is an observable quantity, not an anomaly; the pass has already
  reconciled the row to the live window, and the position is not part
  of the aggregated Warn (only the gauge). Do not build alerting on it
  — a real missed increment is masked by the flow; check a suspicion
  by hand (the row does not match the live window after
  `counters recount` with the pipeline stopped):
  `SELECT value FROM crawler_counters
  WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')`
  against `SELECT count(*) FROM domains WHERE last_checked_at =
  current_date` — on a stopped pipeline the equality must hold right
  after a pass.
- **"Render debt counter: 0 rows affected" from the http-worker** —
  the `'render_pending'` row is missing (section 6): restore it and
  recount.

`crawlerctl counters list` shows the current values and the age of the
last write (milliseconds on the reference table — safe to poll
freely); a cron-driven `counters recount` for small corpora without a
permanent monitor is a legitimate mode (the incremental-counters spec
§9).

## 8. The zone collapse window: `crawlerctl zones collapse` (T-196)

A one-off maintenance window for switching to TLD zone semantics
(the spec `tasks/TZ_tld_only_zones.md`: a zone is the last label of a
name, not a PSL public suffix). The command moves the rows of 2LD
zones (`5g.in`, `co.uk`, `net.in`… — public-suffix zones created by
ingest before the switch) into their TLD: `zone_id` → the TLD,
`label` → `label || '.' || prefix` (`example` in `5g.in` →
`example.5g` in `in`); the 2LD zone's partitions are dropped, the zone
row is deleted, and the counters are recomputed by the general
reconciliation. Idempotent and resumable: a re-run continues from
where it stopped (only rows still sitting in a 2LD zone are moved); no
2LD zones — a no-op. Zones in the `ingest.allowed_zone_paths` allowlist
are not collapsed — the testbed's test zones live on as zones.

**The order is mandatory.** Mixing old/new code with half-moved data
produces duplicates and re-creation of 2LD zones: the old code treats
the public suffix as the zone and re-creates the `5g.in` zone; the new
code, before the move, writes duplicates into the TLD zone; queue
messages carry a stale `zone_id`.

### Steps

1. **Ship the build with TLD semantics without starting it** — the
   images are built, the stack is stopped: the update does not start
   services until the operator's command (systemd: build and place the
   binaries, `crawler.target` stopped; [`ci-cd.md`](ci-cd.md) — with a
   live deploy pipeline, stop `crawler.target` first; the deploy script
   does not start it itself).
2. **Back up the database** (`pg_dump` or a volume snapshot) — the only
   rollback path (the "Rollback" step below): the move is irreversible,
   a partial rollback is impossible.
3. **Record the inventory** — a dry run:

   ```bash
   # dry run: the table of 2LD zones and the move plan; changes nothing
   /opt/crawler/bin/crawlerctl zones collapse
   ```

4. **Stop the sources of work** — ingest (the watcher) and the
   scheduler: new files and tasks are no longer published. The other
   services keep running for now.
5. **Wait for the streams to empty out**: `q:dns`, `q:http`,
   `q:analyze`, `q:render` (the workers finish the tail; monitoring —
   `crawlerctl status` or `redis-cli XLEN q:dns` …). Messages carry a
   `zone_id` — a task of a deleted zone executed after the move would
   record a result with a non-existent `zone_id` into the DEFAULT
   partition. The emergency option (the tail is not draining): `DEL`
   the streams and return the stuck tasks to pending —
   `UPDATE domains SET check_state = 0, queued_at = NULL
   WHERE check_state = 1;` (pipeline idempotency, T-097, guarantees
   the correctness of the re-selection).
6. **Stop the workers, analyzer, monitor, render-worker, web** — the
   stack is fully down.
7. **The move**:

   ```bash
   # precondition: no domains with check_state = 1 (queued) in the
   # affected zones — otherwise it fails with the list of zones; after
   # the emergency step 5, deliberately: --force
   /opt/crawler/bin/crawlerctl zones collapse --apply
   ```

   The command prints the move per zone and the counter reconciliation
   result; `--skip-reconcile` leaves the recomputation to the monitor's
   first pass. An interrupted run (Ctrl-C, a failure) is resumed by
   repeating the command — completed batches stay in place.
8. **Post-checks** — the command runs them itself (no 2LD zones left
   outside the allowlist; no orphan `zone_id`s in the domain tables;
   counters = the aggregate). A manual spot check:

   ```sql
   SELECT domain_info('example.5g.in');  -- zone in, label example.5g
   ```

9. **VACUUM after the row movement** (bloat from the move between
   partitions):

   ```sql
   VACUUM (ANALYZE) domains, subdomain_results, laravel_domains,
                  laravel_findings;
   ```

10. **Start the stack on the new build** — the services start on the
    new data schema; the monitor confirms the counters with its first
    reconciliation.
11. **Clean up the `var/incoming/<2LD>/` directories** (if dumps were
    stored under the old directory scheme): with a TLD allowlist a file
    from there would fall into `failed/`, but cleaning up is still
    better. Optionally — `crawlerctl zones sync` (refresh the reference
    table from the IANA snapshot).

**A warning about `zones sync --deactivate-missing`.** The flag
disables database zones missing from the IANA snapshot — along with
the junk 2LD zones it also deactivates the allowlist's test zones
(`crawl.test` in the dev/testbed configs; the production allowlist is
empty — production records have no such problem). In dev, after
`--deactivate-missing`, restore the test zones one per call
(`zones enable` takes a single zone):
`crawlerctl zones enable crawl.test`, then the remaining scenario
zones from `ingest.allowed_zone_paths`.

### Rollback

Restore the step-2 backup in full. A partial rollback (returning one
zone) is impossible: a moved compound label `example.5g` is
indistinguishable from an original compound label (the domain
`foo.bar.in` could always live in the `in` zone with the label
`foo.bar`).

### Dev environment

The window on the dev stack is not required: re-creating the data by
re-ingesting the test dumps instead of the move is acceptable
(spec §11). The command works on dev too (`docker compose … run --rm
tools go run ./cmd/crawlerctl zones collapse`); the procedure's
integration test — `test/integration/zones_collapse_test.go`.

# Technical Specification: TLD-Only Zones

Version: 1.0 (a supplement to the main spec [`TZ_domain_crawler.md`](./TZ_domain_crawler.md);
**it replaces the semantics of spec §5.2 item 5** "a zone is a public suffix
from the publicsuffix list". The main spec is not edited — this supplement
governs, following the precedent of [`TZ_incremental_counters.md`](./TZ_incremental_counters.md)).

---

## 1. Purpose and Overview

A domain's zone in the system is the **last label of the name (TLD)**, not a
public suffix per the Mozilla PSL. `example.5g.in` → zone `in`, label `example.5g`.
The existing public-suffix zones (`5g.in`, `co.uk`, `net.in`, …)
are collapsed into their TLDs by moving the data; no new ones appear.

### 1.1. Current state (what is being replaced)

The zone is determined by `internal/idn` (`idn.go`, `Normalize`) calling
`publicsuffix.PublicSuffix` from `golang.org/x/net/publicsuffix` — a bundled
snapshot of the Mozilla PSL. The PSL is a list for cookie isolation, to which
**registries themselves add second levels**: `.in` alone (the NIXI registry)
has ~44 of them — `5g.in`, `6g.in`, `ai.in`, `co.in`, `net.in`, `org.in`, …

Consequences:

| # | Problem | Where |
|---|---|---|
| 1 | **A full `.in` dump via `incoming/in/` is impossible**: for `example.5g.in` the suffix `5g.in` ≠ the path zone `in`, `domainLabel` answers `wrong_zone` on every line, and the file fails on `ingest.max_error_ratio`. The dump has to be manually sorted into the `incoming/5g.in/`, `incoming/co.in/`, … directories | `internal/ingest/validate.go`, `domainLabel` |
| 2 | Each 2LD zone is a `zones` row and potentially its own set of partitions (4 tables × `hash_parts`): thousands of junk micro-zones absent from the IANA snapshot | `zones`, `create_zone_partitions` |
| 3 | The zone set drifts with dependency updates: the PSL is embedded in `golang.org/x/net` (currently v0.61.0); registries add suffixes — new "zones" appear outside the operator's control | `go.mod` |
| 4 | The `zones` reference diverges from the IANA snapshot: `crawlerctl zones sync` (T-125) loads only TLDs; `zones --deactivate-missing` is meaningless while the table has 2LDs | `cmd/crawlerctl/zonessync.go` |

### 1.2. Target state

1. `idn.Normalize`: the zone is the **last label** of the ASCII form; the label is
   everything before it (a composite label is the norm; the contract already
   allowed composite labels).
2. The `zones` reference = TLDs (+ exception test zones from the allowlist,
   §5). `zones sync` is the canonical source of the reference.
3. Ingest directories: `incoming/<tld>/add|delete/`. Multi-level directories —
   only from the `ingest.allowed_zone_paths` allowlist (testbed test zones).
4. The data of the existing 2LD zones is moved by the console command
   **`crawlerctl zones collapse`** (§7) in a maintenance window per runbook §8.
5. The dependency on `golang.org/x/net/publicsuffix` is removed (`idna` remains).

### 1.3. Key decisions (rationale — §12)

| Decision | Essence |
|---|---|
| Bare public suffixes in the data (`co.in` in an `.in` dump) | **Accepted as junk** — they become domains (`in`, `co`). Filtering would require a suffix list — a return to the PSL we are moving away from (§12.6) |
| Selective collapsing ("collapse `*.in`, keep `co.uk`") | Rejected: two zone semantics, an endless config chase after registries (§12.3) |
| The PSL ICANN section only | Does not solve the problem: `5g.in`/`co.in` are in the ICANN section (§12.2) |
| Data move via CLI, not a migration file | Migrations at service start are schema-only, "pure SQL without code" (project style); a heavy row move is a CLI with batches, the `zones sync` precedent (§12.4) |

---

## 2. Terms

| Term | Meaning |
|---|---|
| The spec | The project's main spec (`TZ_domain_crawler.md`), unless stated otherwise |
| This spec | The present specification |
| TLD | The last label of a domain name (the IANA snapshot `tlds-alpha-by-domain.txt`) |
| 2LD zone | A multi-label zone in `zones` that matches a PSL public suffix (`5g.in`, `co.uk`) — the kind being collapsed |
| Zone prefix | A 2LD zone without its TLD tail: for `5g.in` it is `5g`, for `city.kawasaki.jp` it is `city.kawasaki` |
| Collapse | Moving the rows of a 2LD zone into the TLD zone: `zone_id` → TLD, `label` → `label || '.' || prefix` |
| Zone directory allowlist | `ingest.allowed_zone_paths` — multi-level zone names allowed to be `incoming/<zone>/` directories (testbed test zones) |

---

## 3. Constraints and Scope

### 3.1. Scope (what is not included)

1. **No bare-suffix filtering is introduced** (decision §1.3): `co.in`
   in a dump becomes the domain `(in, co)` and shares the common fate
   (checks, statuses). The volume is a handful of lines per dump.
2. **2LD granularity is not preserved**: `zone_stats` alerts
   (the DNS error share per zone), `zone_weights`,
   `requeue_deprioritized_zones` priorities become per-TLD. A broken
   `co.in` registry smears across the `in` zone.
3. The DB schema does not change: LIST partitioning by `zone_id`, keys
   `(zone_id, …)`, `UNIQUE (zone_id, label)` — as is. No new
   SQL migrations.
4. The semantics of `zones enable|disable`, `zones sync`, ingest
   idempotency (T-097), file dedup (T-113) — unchanged.
5. A down-move (TLD → 2LD) is not designed: the migration is irreversible
   (§12.5); rollback is a restore from backup (§8.5).

### 3.2. Technical constraints

1. No new dependencies; `go.mod` is unchanged: only the `publicsuffix`
   usage leaves `golang.org/x/net`, `idna` remains.
2. The data move is performed by the CLI command `crawlerctl zones collapse`
   (batches, its own pace), not by an automatic migration at service start
   ("migrations are pure SQL", the incremental counters spec §3.2).
3. PostgreSQL 16: moving rows between partitions is native row movement
   via an `UPDATE` of the partition key.
4. A maintenance window is mandatory: during the move the services are stopped
   (§8) — otherwise the old code recreates 2LD zones, the new code writes
   duplicates before the move, and queue messages carry a stale `zone_id`.

---

## 4. `internal/idn` Normalization Contract

### 4.1. Zone splitting rule

`Normalize` after `normalizeASCII` (pipeline steps 1–4 — unchanged):

```
i     := LastIndexByte(ascii, '.')
zone  := ascii[i+1:]        // last label — the TLD
label := ascii[:i]          // everything before it; may be composite
```

- `i < 0` (no dots, the name is itself a TLD) → `ErrNoLabel` — as today for `com`.
- `isNumericZone(zone)` → `ErrNumericZone` — as today: a last label
  made of digits only cuts off IPv4 literals (`1.2.3.4` → zone `4`) and
  dump dates (`18.06.1999` → zone `1999`). The function is unchanged
  (iterating over the labels of a single label — the same result).
- `zone == ""` is unreachable (normalizeASCII guarantees non-empty ASCII
  without empty labels) — the existing guard branch is kept.
- The `publicsuffix.PublicSuffix` call and the `golang.org/x/net/publicsuffix`
  import are removed.

`NormalizeZone` (the zone name of a directory) and `HostToASCII` — unchanged.

### 4.2. Examples (before → after)

| Input | Today (PSL) | After (TLD-only) |
|---|---|---|
| `example.com` | zone `com`, label `example` | unchanged |
| `example.5g.in` | zone `5g.in`, label `example` | zone `in`, label `example.5g` |
| `example.co.uk` | zone `co.uk`, label `example` | zone `uk`, label `example.co` |
| `example.city.kawasaki.jp` | zone `kawasaki.jp` | zone `jp`, label `example.city.kawasaki` |
| `example.github.io` | zone `github.io` (a private suffix) | zone `io`, label `example.github` |
| `магазин.рф` | zone `xn--p1ai`, label `xn--80adddb…` | unchanged |
| `example.crawl.test` | zone `test`, label `example.crawl` (the `*` rule) | unchanged (now always) |
| `github.io` (a bare suffix) | `ErrNoLabel` | **valid**: zone `io`, label `github` |
| `co.in` (a bare suffix) | `ErrNoLabel` | **valid**: zone `in`, label `co` (decision §12.6) |
| `com` | `ErrNoLabel` | `ErrNoLabel` (unchanged) |
| `1.2.3.4`, `18.06.1999` | `ErrNumericZone` | `ErrNumericZone` (unchanged) |

### 4.3. Package changes

| Place | Change |
|---|---|
| `Normalize` | the §4.1 formula instead of `publicsuffix.PublicSuffix` |
| the package docstring (lines 12–13) | "the zone is split off by the last label (TLD), item 5 as amended by this spec" |
| `Result.Zone` (docstring) | "the TLD — the last label; composite PSL zones are not detected" |
| `Result.Label` (docstring) | "a composite label is the norm for zones with 2LD registrations" |
| `ErrNumericZone`, `ErrNoLabel` (docstrings) | wording without a reference to the PSL `*` rule |
| the `golang.org/x/net/publicsuffix` import | remove |

The `IsIDN` semantics is unchanged: `HasXNLabel(label)` over a composite label
gives the same result as before over the domain's label (any A-label to the left
of the zone sets the bit).

---

## 5. Ingest: Zone Directories and Validation

### 5.1. Zone directories

- `incoming/<tld>/add|delete/` — the working layout; a full TLD dump
  (`incoming/in/` with `example.5g.in` lines) is accepted as a whole.
- Zone-less files (`add|delete/<file>`) — the mechanics are unchanged:
  a line's zone is derived from the name (`idn.Result.Zone`), which is now the TLD.
- **A multi-level directory** (`incoming/<dotted zone>/`) — only if the
  zone is listed in `ingest.allowed_zone_paths` (§6). Otherwise `NewValidator`
  returns a file-level error and the watcher moves the file to `failed/`
  with the reason (T-021 semantics); lines are not parsed.
- The allowlist exists for testbed test zones (`crawl.test`) which
  TLD semantics forbids from being a "path zone". The production config is empty.

### 5.2. `domainLabel`

The structural logic is unchanged; the inputs change (`res.Zone` — a TLD):

1. `res.Zone == zone` (the path is the name's TLD) → label `res.Label`
   (a composite one is the norm): `example.5g.in` in `incoming/in/` → `example.5g`.
2. `res.ASCII == zone` → `ReasonNoLabel` (the name itself is the path zone;
   for TLD paths `idn.Normalize` catches it earlier, the branch is a guard).
3. The name ends with `.zone` **and** `zone` ends with `.`+TLD
   (the path is deeper than the TLD — an allowlisted test zone) → the label without the tail:
   `example.crawl.test` in `incoming/crawl.test/` → label `example`.
   Otherwise — `ReasonWrongZone`.

Branch 3 for allowlisted zones is the same one that today served non-PSL
path zones: the `domainLabel` code is unchanged, the allowlist check happens
earlier (in `NewValidator`).

### 5.3. Errors

No new line reasons are introduced. The `wrong_zone` coverage changes:
- `example.co.uk` in `incoming/uk/`: today reject → after: accept.
- `example.5g.in` in `incoming/in/`: today reject → after: accept.

---

## 6. Configuration

### 6.1. New field

```yaml
ingest:
  # Multi-level zones allowed to be incoming/<zone>/ directories
  # (test zones; example — crawl.test in the testbed).
  # The production value is empty: working directories are TLDs only.
  allowed_zone_paths: []
```

- `Ingest.AllowedZonePaths []string` (`yaml:"allowed_zone_paths"`),
  default `nil` (empty).
- Validation: every entry is a valid zone name per `idn.NormalizeZone`,
  lowercase punycode, **at least one dot** (a TLD zone has no business
  in the allowlist). Normalization and dedup happen at config load
  (the `ZoneWeights` style, config.go `Validate`).
- The validation error message cites the example (`crawl.test`).

### 6.2. Related text edits

- `config.go`, the `scheduler.zone_weights` validation (line ~1109) and
  `render.requeue_deprioritized_zones` (~1302): remove "composite — co.uk"
  from the texts — the keys are now TLDs (`in`, `uk`, `xn--p1ai`).
  The `zoneNameRe` regex itself is unchanged (allowlist names with dots
  are validated separately).
- `config.example.yaml`: the `zone_weights` comment — TLD names;
  the new §6.1 block.

---

## 7. CLI: `crawlerctl zones collapse` (data migration)

### 7.1. Semantics

```
crawlerctl zones collapse              # dry-run: inventory and plan, changes nothing
crawlerctl zones collapse --apply      # performs the move
crawlerctl zones collapse --apply --skip-reconcile   # without counter reconciliation at the end
crawlerctl zones collapse --force      # ignore the check_state = 1 warning
```

- A command of the `zones` group (precedent — `zones sync`, T-125); the DSN comes
  from the configuration, the allowlist from the same config (its zones are
  **not collapsed** — `crawl.test` lives on as a zone).
- **Idempotent and resumable**: a repeated run continues
  from where it stopped — only rows still sitting in the 2LD zone
  are moved (a §7.3 batch addresses `zone_id = <2LD>`; already moved rows do not
  match the predicate). No 2LD zones — a no-op.
- Precondition: no domains with `check_state = 1`
  (queued) in the affected zones. If there are — refuse with the list of zones (or `--force` after
  the operator's review, §8.2).

### 7.2. Algorithm (per 2LD zone Z → TLD T)

Inventory (the list for the dry-run and the loop):

```sql
SELECT z.id, z.name, z.domains_total,
       (SELECT count(*) FROM domains d WHERE d.zone_id = z.id) AS domains_now
FROM zones z
WHERE z.name LIKE '%.%'
  AND z.name <> ALL($1)      -- allowlist from the config
ORDER BY z.name;
```

For each zone `Z` (id `zid`, name `zname`): `T` = the last label
of `zname` (Go: `LastIndexByte`), `prefix` = `zname` without `.{T}`
(`5g.in` → `T=in`, `prefix=5g`; `city.kawasaki.jp` → `T=jp`,
`prefix=city.kawasaki`).

**Step 1 — the T zone.** Look up `zones` by name; if absent — create it (the unicode form
for XN zones — `idn.ToUnicode`, ASCII — NULL; `is_active = true`, the
`zones sync` style):

```sql
SELECT id FROM zones WHERE name = $t;
INSERT INTO zones (name, name_unicode, is_active)
VALUES ($t, NULLIF($unicode, ''), true) RETURNING id;
```

**Step 2 — the T partitions** (before the move, so that rows land directly in the
dedicated partitions; idempotent — existing ones are skipped with a NOTICE):

```sql
CALL create_zone_partitions($tid);
```

**Step 3 — the set of ids to move** (a temp table for the command's session):

```sql
CREATE TEMP TABLE moving_ids AS SELECT id FROM domains WHERE zone_id = $zid;
CREATE INDEX ON moving_ids (id);
```

**Step 4 — batched move** (§7.3): per table — an `UPDATE` with
row movement (PG16 moves rows into the T partitions automatically):

```sql
UPDATE domains
SET zone_id = $tid, label = label || '.' || $prefix
WHERE zone_id = $zid AND id >= $lo AND id < $hi;

UPDATE subdomain_results SET zone_id = $tid
WHERE zone_id = $zid AND domain_id >= $lo AND domain_id < $hi;

UPDATE laravel_domains   SET zone_id = $tid
WHERE zone_id = $zid AND domain_id >= $lo AND domain_id < $hi;

UPDATE laravel_findings  SET zone_id = $tid
WHERE zone_id = $zid AND domain_id >= $lo AND domain_id < $hi;
```

`laravel_history` (no partitions, an order of magnitude smaller) — in one
statement per zone:

```sql
UPDATE laravel_history SET zone_id = $tid WHERE zone_id = $zid;
```

**Step 5 — the `ingest_files` dedup journal** (rebinding to T keeps the dedup
of exact redelivery accurate, T-113; a conflict of the key
`(zone_id, action, sha256)` is possible if the same content was already
delivered to T — the duplicate is deleted, the owning row remains):

```sql
DELETE FROM ingest_files a
USING ingest_files b
WHERE a.zone_id = $zid AND b.zone_id = $tid
  AND a.action IS NOT DISTINCT FROM b.action
  AND a.sha256 = b.sha256;

UPDATE ingest_files SET zone_id = $tid WHERE zone_id = $zid;
```

**Step 6 — drop the Z partitions** (a selection from the catalog by the
`FOR VALUES IN (zid)` bound — the same technique as in `create_zone_partitions`;
`DROP TABLE` of the zone's parent partition takes its hash leaves with it):

```sql
SELECT p.relname AS parent, c.relname AS part
FROM pg_inherits i
JOIN pg_class c        ON c.oid = i.inhrelid
JOIN pg_class p        ON p.oid = i.inhparent
JOIN pg_namespace n    ON n.oid = p.relnamespace
WHERE n.nspname = current_schema()
  AND p.relname IN ('domains','subdomain_results','laravel_domains','laravel_findings')
  AND pg_get_expr(c.relpartbound, c.oid) IN (
      format('FOR VALUES IN (%s)', $zid), format('FOR VALUES IN (%L)', $zid::text));
-- DROP TABLE <part> for each result row
```

**Step 7 — the zone row:**

```sql
DELETE FROM zones WHERE id = $zid;
```

**Step 8 — counters.** The `zones` columns (`domains_total`, `domains_checked`,
`domains_pending`, `domains_queued`) are not merged manually: after all zones
the command runs the general counter reconciliation (the same aggregate as
`monitor` `ZoneStatsOnce` / `crawlerctl counters`, T-178) — it
authoritatively recomputes both T and the global cumulatives. `--skip-reconcile`
leaves this to the first monitor pass after startup.

`DROP TABLE moving_ids` — at the end of the command.

### 7.3. Batching

- Go iterates over `moving_ids` in ascending `id` in windows of
  `batch = 50000` ids (a constant, not config): `lo`/`hi` are the window bounds.
- Each batch is a **separate transaction** (the 4 `UPDATE`s of step 4 for one
  range together: domains and their children move atomically per batch).
- A `UNIQUE (zone_id, label)` violation within a batch is impossible (proof in
  §12.5); if anomalous data still produce a conflict — the batch transaction
  rolls back and the command stops with an error and the zone name
  (a natural safeguard; a separate pre-check is not needed).
- No throttling pause between batches (a maintenance window,
  no concurrent writes); WAL/bloat are handled by `VACUUM (ANALYZE)`
  of the affected tables at the end (the post step, §8.4).

### 7.4. Report

Dry-run and `--apply` print a table (the `zones sync` style):

```
zone        TLD   domains  results  findings  history  files  partitions
5g.in       in       12345       456789    12345     1234      12  yes
co.in       in      456789      1234567   456789    45678      3  default
…           …           …           …        …        …       …  …
total: 44 2LD zones, 1,234,567 domains → zone in
```

After `--apply` — a per-zone summary of what was moved and the reconciliation result.

### 7.5. Post-checks (the command, always)

```sql
-- no 2LD zones outside the allowlist remain
SELECT count(*) FROM zones WHERE name LIKE '%.%' AND name <> ALL($1);            -- 0

-- no orphan zone_id in the domain tables
SELECT count(*) FROM domains d
WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = d.zone_id);                 -- 0
-- (same for subdomain_results, laravel_domains, laravel_findings, laravel_history)

-- zone counters = the aggregate (after reconciliation)
```

Plus a spot check of one moved name: `domain_info('<domain>.<Z>)`
returns the zone `T`, a label with the appended prefix.

---

## 8. Maintenance Window Runbook

The order is mandatory: mixing old/new code with half-moved
data produces duplicates and recreated zones.

### 8.1. Preparation

1. Ship the build with waves 1–4 (code + collapse) **without starting it**
   (the images are built, the stack is stopped — the update does not start the services
   until the operator's command).
2. A DB backup (`pg_dump` or a volume snapshot) — the only rollback path (§8.5).
3. Record the inventory: `crawlerctl zones collapse` (dry-run).

### 8.2. Quiesce

4. Stop the sources of work: ingest (the watcher) and the scheduler — no new
   tasks are published.
5. Wait for the `q:dns`, `q:http`, `q:analyze`,
   `q:render` streams to drain (the workers finish the tail; the check — `crawlerctl status`
   / `redis-cli XLEN`). Messages carry a `zone_id` (queue/message.go) —
   tasks of removed zones executed after the move would write
   results with a non-existent `zone_id` into the DEFAULT partitions.
6. Stop the workers, analyzer, monitor, render-worker, web.
   The emergency option (the tail does not drain): `DEL` the streams and return
   stuck tasks to pending — `UPDATE domains SET check_state = 0,
   queued_at = NULL WHERE check_state = 1`; the pipeline idempotency
   (T-097) guarantees the correctness of the re-selection.

### 8.3. Move

7. `crawlerctl zones collapse --apply` (the §7.1 precondition will check
   `check_state = 1`; with `--force` from step 6 — deliberate).
8. The §7.5 post-checks, the spot check `domain_info('example.5g.in')` → zone
   `in`, label `example.5g`.

### 8.4. Finale

9. `VACUUM (ANALYZE) domains, subdomain_results, laravel_domains,
   laravel_findings;` (bloat after row movement).
10. The stack with the step 1 build — the services start on the new data schema;
    monitor confirms the counters with its first reconciliation.
11. Clean up the `var/incoming/<2LD>/` directories (if dumps were sorted there
    under the old scheme) — with the §5.1 allowlist a file from there would fall into `failed/`,
    but cleanliness is better; optionally — `crawlerctl zones sync`.

### 8.5. Rollback

Restore the step 2 backup in full. A partial rollback (bringing back one
zone) is impossible: a moved composite label cannot be distinguished from a
native one (`foo.bar.in` always lived in the `in` zone with the label `foo.bar`) (§12.5).

---

## 9. Tests

| Layer | What is verified |
|---|---|
| `internal/idn` unit + corpus (T-010) | the §4.2 table in full: `co.uk`/`5g.in`/`github.io`/`kawasaki.jp` → TLD zones and composite labels; bare `github.io`/`co.in` are valid; `com` — `ErrNoLabel`; `1.2.3.4`/dates — `ErrNumericZone`; `IsIDN` on composite labels |
| `internal/ingest` | `incoming/in/` + `example.5g.in` → accept, label `example.5g` (the former `wrong_zone`); a multi-label directory without the allowlist — a file error; with the allowlist `crawl.test` — accept via branch 3 of §5.2; `wrong_zone` on a foreign TLD; a zone-less file with mixed TLDs |
| `internal/config` | `allowed_zone_paths`: empty by default, valid/invalid entries (no dot, not a zone), dedup; the error texts of `zone_weights`/`requeue_deprioritized_zones` |
| `crawlerctl zones collapse` integration (`test/integration`) | on a seeded schema: a 2LD zone with domains/results/findings/history/ingest_files (with and without partitions) → `--apply` moves everything, appends the label suffix, drops the partitions, deletes the zone, rebinds ingest_files (including the duplicate conflict); a repeated run — a no-op; an interrupted run (after N batches) — continues; the allowlist zone is untouched; the §7.5 post-checks are green |
| testbed | the testbed config with `allowed_zone_paths: [crawl.test]`; the e2e ingest→check scenarios in the `crawl.test` zone pass |

General criteria: `make test`, `make lint`, `make test-integration` — green.

---

## 10. Documentation

| File | Change |
|---|---|
| `docs/schema.md` | the "Name processing" section (`domain_info`): the zone is a TLD, "the longest suffix from `zones`" remains true (the reference is TLDs + the allowlist); the `site.co.uk` example → zone `uk` |
| `docs/incoming-files.md` | the `incoming/<tld>/` directories, the allowlist, the prohibition of other multi-level directories |
| `docs/configuration.md` + `config.example.yaml` | `ingest.allowed_zone_paths`; the `zone_weights`/`requeue_deprioritized_zones` comments — TLD names |
| `docs/runbooks.md` | runbook §8 (the zone collapse window) |
| `tasks.md` | the index entry added: stage 30, T-194…T-198 |

---

## 11. Implementation Stages

Tasks are filed in [`tasks.md`](./tasks.md) as stage 30: T-194…T-198,
one task per wave.

| Wave | Task | Content | Acceptance criterion |
|---|---|---|---|
| 1 | T-194 | `internal/idn`: TLD splitting, removal of `publicsuffix`, docstrings; corpus/unit tests §9 | `make test` green; the §4.2 table pinned by tests |
| 2 | T-195 | `internal/ingest` + `internal/config`: the directory allowlist, the texts; tests §9 | `make test` green |
| 3 | T-196 | `crawlerctl zones collapse` + the integration test §9 | `make test-integration` green; the dry-run on the dev stack is readable |
| 4 | T-197 | Documentation §10, the testbed config | the doc review; the testbed scenarios pass |
| 5 | T-198 | The maintenance window per §8 on production data | the §7.5 post-checks green; an `.in` dump is accepted via `incoming/in/` as a single file |

Waves 1–4 — one PR/release (the code is in the repository before the window, but the services
keep running the old build until step §8.1.1 — the switch is atomic).
Sequential testing on the dev stack with re-ingesting
test data instead of the move is acceptable.

---

## 12. Decisions and Alternatives

### 12.1. PSL → the last label

The PSL solves the cookie isolation problem (who may set a cookie on `*.in`),
not the "administrative bucket" problem of a crawler zone. Properties of TLD-only:
the zone is determined by the name (no external lists), it is stable
(the TLD set changes by a handful per year), and it matches the IANA snapshot —
the canon of `zones sync`. The price is 2LD granularity (§3.1.2).

### 12.2. The PSL ICANN section — not a solution

`5g.in`, `co.in`, etc. are in the ICANN section (submitted by the NIXI registry);
filtering the PRIVATE section would remove only `github.io`-like entries. It does not
solve the problem and keeps the dependency.

### 12.3. Selective collapsing rejected

"Collapse `*.in`, keep `co.uk`" requires a config list and maintaining it
after the registries; it yields two zone semantics (statistics, weights,
ingest) and returns to the original problem in a reduced form. TLD-only is
one simple invariant semantics.

### 12.4. Data move via CLI, not a migration at start

The project's migrations are pure SQL schema, applied automatically at the start
of all services (docs/migrations.md). Moving millions of rows — hours,
batches, a report, a dry-run, preconditions — is a maintenance CLI command
(the `zones sync` precedent, T-125) executed in a quiet window; an automatic migration
of such a volume in a single implicit transaction of a starting service is a risk
of long transactions and cascading restarts.

### 12.5. Irreversibility and the absence of collisions

- **No `UNIQUE (zone_id, label)` collisions**: the pair `(T, label||prefix)`
  maps bijectively to the full name, and under the old code the name
  `example.5g.in` went **only** to the `5g.in` zone — the pair `(in,
  example.5g)` could not exist in the DB. `domains.id` is a global
  sequence (`domain_id_seq`); there are no PK collisions between zones.
- **The reverse move is impossible**: a moved label `example.5g` is indistinguishable
  from the native composite `foo.bar` (the domain `foo.bar.in` always lived in
  the `in` zone). Hence there is no down mechanism; rollback is the backup (§8.5).

### 12.6. Bare suffixes — junk without a filter

Telling `co.in` (a suffix) from `nic.in` (a domain) is only possible with a suffix
list — a return to the PSL/a manual list. The noise volume: ~44 lines per
`.in` dump out of millions; the lines really exist in DNS, go through
checks, and skew the statistics by fractions of a percent. A targeted
`bare_suffixes` list can be added later, if ever needed.

### 12.7. A composite label is not new

The composite label contract existed since T-010 (non-PSL zones:
`example.crawl.test` → label `example.crawl`; `магазин.рф.co.uk` in the
`co.uk` zone). TLD-only merely makes composite labels a regular case;
neither the schema, nor the indexes, nor ingest idempotency change.

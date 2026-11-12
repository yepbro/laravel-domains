# Completed Tasks Archive

Completed tasks (`[done]`) have been moved here from [`tasks.md`](tasks.md),
which contains only active work. The line format is preserved: the status,
a detailed description of the result with decisions and references to the
spec sections — this remains the primary implementation log (what was
done and why).

Transfers: 2026-10-18 — 93 tasks (stages 0–10, excluding blocked
T-092/T-093); 2026-10-20 — 39 more (T-096, stages 11–16, T-134);
2026-10-22 — T-135 (stage 17), T-136 (stage 18) and T-137 (stage 19);
later — T-092 (stage 10) and waves of stages 21–23 (T-141…T-156, 2026-10-23);
2026-10-24 — T-093 (stage 10, closing the acceptance of spec §11.4 item 7);
2026-10-26 — the full statement of T-149 (stage 23), waves of stages 24–29
(T-157…T-193), closing notes of the waves of stages 10 and 20–22, and the
table-of-contents lines of completed stages; 2026-10-29 — stages 30 (T-194…T-198,
TLD-only zones) and 31 (T-199…T-206, slow queries);
2026-11-01 — stage 32 (T-207…T-214, optimizations of the render-debt
drain: research, deploy/ units, marker, DNS finalization,
A/B contexts/timeout/locks);
2026-11-02 — stage 33 (T-215…T-218, tails of the render-debt drain:
PartOf for chromium recycle units, live render tests, the
zones collapse deadlock);
2026-11-03 — stage 34 (T-219…T-232, findings of the full code review
by three models: SSRF perimeter, form resources, queue, service
shutdown, minor trust issues); 2026-11-08 — the code part of stage 35
(T-233…T-246, render engines and the fallback queue), stage 36
(T-250…T-254, extra-round fixes), stage 37 (T-255…T-266,
formalization of implicit finite-state machines), stage 38 (T-267…T-268,
zone × counter deadlocks) and stage 39 (T-269, desync of the
render_pending counter).
Total tasks: 266.

## Stage Table of Contents

| Stage | Name | Tasks |
|---|---|---|
| 0 | Project foundation | T-001…T-012 |
| 1 | Ingest — file ingest | T-013…T-023 |
| 2 | Queues and scheduler | T-024…T-032 |
| 3 | DNS stage | T-033…T-040 |
| 4 | Content storage | T-041…T-046 |
| 5 | HTTP stage | T-047…T-060 |
| 6 | Render | T-061…T-067 |
| 7 | Laravel analyzer | T-068…T-075 |
| 8 | Monitoring and search | T-076…T-084 |
| 9 | Deployment and OS tuning | T-085…T-089 |
| 10 | Testing and acceptance | T-090…T-096 |
| 11 | Improvements from the code review (2026-10-18) | T-097…T-114 |
| 12 | Improvements from the larasites analysis (2026-10-19) | T-115…T-119, T-134 |
| 13 | Public landing page — migration from larasites (2026-10-19) | T-120…T-124 |
| 14 | Initial population of the zone catalog (IANA) (2026-10-19) | T-125 |
| 15 | Operator web dashboard of the pipeline (2026-10-19) | T-126…T-131 |
| 16 | Input file ingest extensions (2026-10-19) | T-132…T-133 |
| 17 | Production server deployment — physical deploy (2026-10-22) | T-135 |
| 18 | Telegram notifications for CI/CD (2026-10-22) | T-136 |
| 19 | CD: automatic rollback on healthcheck failure (2026-10-22) | T-137 |
| 20 | Improvements from load measurements (2026-10-22) | T-138…T-140 |
| 21 | Improvements from the re-measurement after T-139 (2026-10-23) | T-141…T-143 |
| 22 | Improvements from wave 21 saturation (2026-10-23) | T-144…T-156 |
| 23 | Investigating the q:dead dead-letter queue — Redis OOM (2026-10-23) | T-149 (full statement; wave results — stage 22) |
| 24 | Stabilization from the control measurement (2026-10-23) | T-157…T-173 |
| 25 | Render debt drain — equilibrium at the watermark (2026-10-24) | T-174…T-177, T-184 |
| 26 | Incremental pipeline counters (2026-10-24) | T-178 |
| 27 | Slow PostgreSQL queries — production log analysis (2026-10-24) | T-179, T-181…T-183, T-185…T-187, T-191…T-193 |
| 28 | Dev environment: deterministic docker network addresses (2026-10-24) | T-180 |
| 29 | Consistency of observability numbers — load report vs crawlerctl status (2026-10-25) | T-188…T-190 |
| 30 | Top-level-only zones (TLD-only) (2026-10-26) | T-194…T-198 |
| 31 | Slow PostgreSQL queries — window after the 3bffc62 deploy (2026-10-26) | T-199…T-206 |
| 32 | Render debt drain optimizations (2026-10-29) | T-207…T-214 |
| 33 | Tails from the render debt drain (2026-11-01) | T-215…T-218 |
| 34 | Full code review findings (2026-11-02) | T-219…T-232 |

## Stage 0. Project foundation

- **T-001** [done] — Monorepo skeleton: the `cmd/` directories (8 services: ingest, scheduler, dns-worker, http-worker, render-worker, analyzer, monitor, crawlerctl), `internal/`, `migrations/`, `rules/`, `deploy/`, `docs/`, `testdata/`; `go.mod` with dependencies. Spec: §4.1, Annex A, Annex B.
- **T-002** [done] — PostgreSQL migration mechanism (up/down), the DB schema stored in the repository, applied at service startup. Spec: §6.3, §6.1.
- **T-003** [done] — Migration of the `zones`, `subdomains`, `domains` tables: LIST partitioning by `zone_id`, `fillfactor = 85`, the partial index `domains_pending_idx` (`check_state = 0`), the `domain_id_seq` sequence, `last_checked_at` without an index (HOT updates). Spec: §6.1, §6 (principles).
- **T-004** [done] — Migration of the `subdomain_results`, `contents`, `laravel_domains`, `laravel_findings`, `ingest_files` tables: schema 000001 matched the §6.1 sketch; refinement — `000002_subdomain_results_fillfactor` (fillfactor = 85 for the `subdomain_results` leaves, frequent result rewrites); `contents`/`ingest_files` without partitioning (no `domain_id`); the minimum of indexes (only PK + the unique `ingest_files.sha256`); the contract is pinned in `schema_test.go`. Spec: §6.1, §6, §6.3.
- **T-005** [done] — Subpartitioning of large zones: migration `000003_zone_hash_subpartitioning` — the `create_zone_partitions(zone_id, hash_parts)` procedure carves a zone out of the DEFAULT partitions of all tables with `zone_id` (idempotent, advisory lock, row move in a single DETACH→CREATE→MOVE→ATTACH transaction); `subdomain_results`/`laravel_*` — LIST→HASH by `domain_id` (power-of-two number of parts), `domains` — without a hash: the UNIQUE `(zone_id, label)` from T-003 is incompatible with subpartitioning in PG16 (a unique key must include the key columns of every level); contract — `test/integration/zone_partitions_test.go`. Spec: §6 (principles), §6.1.
- **T-006** [done] — Package `internal/config`: a single YAML + overrides via environment variables; the full set from the spec (postgres, redis, ingest, scheduler, dns, http, render, content, analyzer, subdomains, monitor), default values, validation. Naming scheme `CRAWLER_<SECTION>_<FIELD>` (37 variables, the `envSpecs` table); config file path — `CRAWLER_CONFIG` (legacy `APP_CONFIG`), auto-discovery of `./config.yaml`; duration/size/list types; validation collects all problems at once; priority env > YAML > default, legacy `APP_PG_DSN` supported; the service framework takes the DSN from the config (`Options.Config`), monitor — `monitor.listen`; example — `config.example.yaml`, reference — `docs/configuration.md`; dev environment contract — `test/integration/config_test.go`. Spec: §5.10.
- **T-007** [done] — Package `internal/db`: a `pgxpool` pool from the `postgres` section (`db.Open`/`NewOptions`; `pool_size` overrides `pool_max_conns` from the DSN, lazy connections, lifetime 30m/idle 5m/health-check 1m, fail-fast Ping; the 1..100 limit — the spec budget is 20–50 across all services, `pool_size` is per service); `CopyFrom` and the streaming `CopyFromStream` — the `RowIter` iterator, a batch = a separate transaction, memory bounded by `batchSize`, ctx cancellation between batches (committed batches remain — a retry is safe per §3.4); the `Update` constructor — a batched `UPDATE … FROM unnest($n::type[])` with keys/`Set`/`SetExpr`/`Where` guards/`Returning` (in SET a column is not qualified by the alias — a PG requirement); an array column = one parameter → no 65535 limit (contract — a test with 70,000 rows); the service framework — the `Options.OnDBPool` option (the pool opens after migrations, closes at shutdown, no-DB stubs stay unchanged); unit tests without a DB (SQL, batches, validation, cancellation) + integration tests in an isolated `db_test_*` schema; documentation — `docs/db.md`. Spec: §6.3, §3.5, §6.
- **T-008** [done] — Package `internal/model`: domain typing per the schema — `Status` (all 7 codes of §6.2, names, `ParseStatus`), `CheckState` (0–2), `Flags` (bit 0 `is_idn` §5.2 item 9, bit 1 `render_pending`; Set/Clear/Test), `DNSMask` (bit number = `subdomains.id`, bits 0..62 — max 63 subdomains §5.10; bit 63 is the sign bit, Set/Unset atomically reject out-of-range numbers, Test returns false for an invalid one, `ValidateSubdomainID` for crawlerctl), `Variant`/`Scheme`/`AliasKind`/`Action`; row structs of all tables with `db` tags for pgx (NULL — pointers, jsonb — `[]byte`); conformance to the migration 000001 columns and DEFAULTs is pinned by tests; documentation — docs/schema.md §"The model in Go". Spec: §6.1, §6.2, §5.2 (item 9), §5.5 (items 5–6), §5.10.
- **T-009** [done] — Package `internal/idn`: name normalization per §5.2 — the `Normalize` pipeline (lowercase, stripping all whitespace characters and trailing dots → `idna.Lookup.ToASCII` (UTS-46: punycode, checking `xn--` labels by decoding) → length checks: label ≤ 63, name ≤ 253 — `idna.Lookup` does not check them and lets empty labels through (`xn--` decodes to an empty one), so the package validates them itself → zone separation via `publicsuffix`, composite zones `co.uk`, private suffixes and unknown TLDs — the last label); `Result{ASCII, Unicode, Zone, Label, IsIDN}` — `is_idn` is judged by the domain label, the zone does not count (consistent with `model.FlagIDN`); `NormalizeZone` (the incoming catalog, no PSL membership required — test zones like `crawl.test`), `HostToASCII` (hosts from `Location`/meta refresh, §5.2 item 8; the interception itself — T-047/T-048), `ToUnicode` (display, item 7, best-effort), `HasXNLabel`; errors are typed (`*idn.Error`: class `empty/invalid/label_len/name_len/no_label` + the original name truncated to 255 runes + the reason) — the basis of the T-016 rejected log; core unit tests (case, whitespace/dots, valid/invalid `xn--`, lengths, `co.uk`, unicode↔punycode; the exhaustive corpus — T-010). Spec: §5.2.
- **T-010** [done] — Name normalization unit tests: the `internal/idn/corpus_test.go` corpus per spec 11.1/§5.2 — mixed case (Bulgaria.RF, Πόλης.Γρ, fullwidth, uppercase-punycode canonicalization), UTS-46 nontransitional deviation characters (ß→xn--strae-oqa ≠ strasse, ς≠σ — the actual behavior of x/net v0.61 Lookup is pinned: transitionalLookup=false), emoji (single ones encode to punycode, ZWJ sequences — a CONTEXTJ rejection), zero-width characters (ZWSP/SHY/BOM/WJ — ignored; ZWNJ/ZWJ outside context, U+180E — disallowed; ZWNJ in Arabic passes), valid xn-- (the homoglyph xn--80ak6aa92e = a lookalike of apple.com) and broken ones (decoding into disallowed characters/non-canonical ASCII/hyphens), length boundaries (a Cyrillic letter ×57 → a label of exactly 63 punycode octets, ×58 → label_len), composite PSL zones (com.au, appspot.com, IP literals → no_label via netip.ParseAddr in publicsuffix), unicode zone catalogs (moskva→xn--80adxhks, 日本。jp U+3002), HostToASCII (IPv4, empty), ToUnicode (emoji/deviation round-trip); ~60 cases, 130 subtests in the package; no defects found in T-009 — behavior agreed with spec 5.2 item 2 (delegation to the Lookup profile). Spec: §11.1, §5.2.
- **T-011** [done] — CLI skeleton of `crawlerctl`: a command table with exit codes (0 success/`--help`, 1 error and "not implemented", 2 usage) and `--help` for every command; stubs for `status` (T-079), `domain <name> [--json] [--content]` — normalization already via internal/idn (zone/label, a JSON skeleton), `subdomains list|add|disable` — label validation (punycode, a-z/0-9/hyphen inside, 1..63, '' — the apex with id 0), `zones list|enable|disable` — NormalizeZone, `reanalyze [--zone]` (T-074), `recheck <domain>…` — manual scheduling for a check: the name was chosen as a verb alongside reanalyze, "tasks enqueue" was rejected as implementation jargon; seed/test-rules moved without behavior changes (make targets work as before); per-command code in `cmd/crawlerctl/{main,seed,testrules,domain,manage}.go`, routing/flags/exit-codes/help tests — main_test.go; CLI reference — docs/dev-environment.md §6. Spec: §4.1, §5.10, §5.11, §12 (stage 1).
- **T-012** [done] — The common Go service framework (audit: config — T-006, migrations — T-002, pool/`OnDBPool` — T-007, `/healthz`+`/metrics` with the `service` label — T-001, all reused); added: structured logging with stdlib `log/slog` — `service.NewLogger(name, cfg)` (text format on stderr, the `service` attribute in every record, the level from the new `log.level`/`CRAWLER_LOG_LEVEL` section, default `info` — an extension to the minimal spec 5.10 set, with validation), `Run` makes the logger the default one (`slog.SetDefault`), migrations switched to slog — they log with the service context; no bare `log.Printf` in the framework, `Run` lifecycle errors are logged by itself and returned (`os.Exit(1)` in main is enough), `net.Listen` before the working logic starts; graceful shutdown per §3.4 — the strict order OnRun (context cancellation + waiting within `ShutdownTimeout`, 10 s by default; a second signal — a forced exit) → the metrics HTTP server (waits for scrapes already started) → the DB pool, shutdown errors go to the log, no panic; the `OnRun(ctx) error` hook for the stage 2+ workers (returning without a signal stops the service: `nil` — clean, an error — exit code 1); uptime — from the `process_start_time_seconds` process collector, no separate metric needed; `cmd/*/main.go` — a single template, stubs stay healthy; unit tests: the logger (level/attributes), shutdown order/timeout/second signal, SIGHUP, OnRun/HTTP errors; documentation — `docs/services.md`, configuration.md, db.md. Spec: §3.4, §4.1, §5.9.

## Stage 1. Ingest — file ingest

- **T-013** [done] — Package `internal/watch`: watching `incoming/` — fsnotify (create/write; recursively: a watch on new `<zone>/add|delete` directories is added on the fly) + a periodic full crawl as a safety net (the new option `ingest.scan_interval` 10ms..1h, 30 s by default, env `CRAWLER_INGEST_SCAN_INTERVAL`, T-006 style); readiness — the stability of size and mtime `stable_seconds` (10 s from §5.10), a write event restarts the window even with an unchanged signature (the FS mtime is coarse); only regular `*.txt` files are emitted — `*.tmp`/`*.part`, hidden files/directories and the top-level `processed/`/`failed/` archive are ignored (a rename to `*.txt` = a creation event of a ready name); a buffered channel of ready files — backpressure by blocking without losses, re-emission is excluded in-memory by path+size+mtime (a new file at the same path is emitted again; permanent idempotency — ingest_files, T-021); a missing directory is created, a disappeared one is restored by a scan; the stability logic — a separate tracker with mockable time; tests: the tracker on fake clocks + a fast watcher on real fsnotify (file growth, tmp/part/hidden, the safety-net poll, new directories, backpressure, cancellation, deletion before stability); `cmd/ingest` wired via `OnRun` — logs ready files (the integration point for T-014+). Spec: §5.1, §5.10.
- **T-014** [done] — Package `internal/ingest`: `ParsePath` — parsing the path of a ready file relative to incoming `<zone>/add|delete/*.txt` → `{Zone (punycode), ZoneUnicode, Action, Path}`: exactly three components (extra nesting/no add|delete/an absolute path/escaping incoming — an error), the operation directory strictly `add|delete` in lowercase (the spec 5.1 structure is fixed, the Linux FS case is exact), the `*.txt` name without case sensitivity — the same as the watch readiness filter, so that ingest parses everything emitted; the zone — `idn.NormalizeZone` (composite `co.uk` allowed, no PSL required), a unicode and a punycode directory give one zone without duplicates (§11.4 item 1); errors are typed `*ingest.PathError` (structure/not_txt/zone, the zone — a `*idn.Error` wrapper with the class for the failed report). Zone catalog: a small `ZoneStore` interface (ZoneByName/CreateZone, a duplicate name is not an error) + `EnsureZone` — find by the punycode name, otherwise create automatically on the first file: `is_active = true` (explicitly in SQL), `name` — punycode, `name_unicode` — the unicode form for display, NULL for ASCII zones (§5.2 item 7); the `SQLZoneStore` SQL implementation on top of `internal/db` (`INSERT … ON CONFLICT (name) DO NOTHING RETURNING …`, no row — a repeated SELECT: concurrent creations of one zone converge into one row), unit tests: EnsureZone/ParsePath on fakes, SQL — exact text and parameters on a fake pgx pool without a DB; assembling the ingest pipeline (watch→ParsePath→EnsureZone→parser) — a later task. Spec: §5.1, §5.2 (items 6–7), §11.4 (item 1).
- **T-015** [done] — A streaming line-by-line file parser (the new package `internal/ingest`, parser.go): `Parse(io.Reader, Options{OnLine, OnReject}) → (Stats, error)` — Line with the physical line number (needed by the T-016 rejected log and the T-021 report) and the Total/Skipped/Rejected/Data counters (the invariant Total = Skipped+Rejected+Data); line endings `\n`/`\r`/`\r\n` (a CRLF pair is not split, a CR at the window edge is read to completion), a BOM is stripped only at the start of the file (in the middle it stays in the data — the UTS-46 mapping in idn.Normalize removes it), empty lines and comments by the first significant character `#`, leading/trailing spaces stripped (unicode.IsSpace — the same predicate as idn.cleanup; inner ones remain — normalization removes them, spec 5.2 item 1); invalid UTF-8 and a line longer than `MaxLineLen` = 1024 bytes (derived from spec 5.2 item 4: 253 punycode octets ≤ ~1012 UTF-8 bytes) — per-line rejections (the Reason `invalid_utf8`/`line_len`, the raw text ≤255 bytes), parsing continues, the file is not considered failed; memory is constant (spec 3.5): a 64 KiB read window + an accumulator of one line ≤ MaxLineLen, long garbage is not buffered; our own scanner instead of bufio.Scanner (its ErrTooLong is unrecoverable); tests per §11.1 (empty, comments, CRLF, BOM, empty lines, a long line, broken UTF-8, no trailing newline) + white-box window boundaries (windows of 3–64 bytes) + short reads + a memory test on an io.Reader generator of ~40 MiB (the live heap stays under 24 MiB, the counters converge). Spec: §5.1, §3.5, §11.1.
- **T-016** [done] — Line validation: a line must be a full domain name from the zone specified in the path; invalid and overlong names are rejected; entries are written to the `rejected` log with the reason; the per-file error share threshold from the config. Package `internal/ingest` (`validate.go`): the file validator — `NewValidator` normalizes the path zone (unicode/punycode — one zone), `Line(number, line)` returns the domain `{zone, label, is_idn}` for loading by T-017 or the rejection reason, `Finish` — the total/accepted/rejected counters (by reason) and the "file is failed" verdict; a line is normalized by `idn.Normalize` (§5.2), the result zone is checked against the path zone (item 6): a match with a public suffix (composite `co.uk`, private ones) or a zone deeper than the suffix (test zones like `crawl.test`, T-014); a foreign zone and a domain of a nested public zone (`example.co.uk` with the zone `uk`) — `wrong_zone`, the `idn.Error` classes — `empty/invalid/label_len/name_len/no_label`; compound labels (`magazin.rf.co.uk`) are accepted — the idn contract (the T-010 corpus); the `rejected` log — a separate slog logger `NewRejectedLogger` (text, T-012 style, the `service=rejected` attribute; a record has the file, the line number, the line truncated to 255 runes, the reason; the format is pinned by tests); the new option `ingest.max_error_ratio` (a share (0, 1], default 0.1 — a single garbage file does not hurt, while a wrong zone/a broken dump gives a share of ~1; env `CRAWLER_INGEST_MAX_ERROR_RATIO`, T-006 style): a rejection share strictly above the threshold — the file is failed as a whole (the `failed/` signal for T-021), equal to the threshold and below — not. Spec: §5.1, §5.2.
- **T-017** [done] — The `ingest_stage` staging table (migration 000004, UNLOGGED — staging without WAL, spec §3.5; no partitioning or extra indexes — the minimum of indexes §6): columns `batch_id bytea` (16 random bytes, the batch identifier — all COPY batches of one file load, it addresses the cleanup of only its own batch), `zone_id`, `label` (punycode, without the zone), `action` (1 add/2 delete — the `model.Action` codes, like `ingest_files.action`), `line_no` (the line position in the file for the §5.1 report), PK `(batch_id, line_no)`; package `internal/ingest`: `ingest.Stage` — streaming load of validated lines via `db.CopyFromStream` in batches of `ingest.batch_size` (the field has existed since T-006: default 50000, range 1..1000000, `CRAWLER_INGEST_BATCH_SIZE`), before copying a batch is wiped — reloading the same batch overwrites it cleanly (§3.4 idempotency), ctx cancellation between batches is safe (committed batches remain); `ingest.ClearBatch` — batch cleanup after applying (called by T-018/T-019); row invariants (a punycode label ≤ 63 without dots, `line_no` grows); the table contract against the migration SQL — a unit test (the integration schema_test.go — T-023 territory); applying add/delete — T-018/T-019. Spec: §5.1, §3.5.
- **T-018** [done] — Applying **add** (`internal/ingest/apply_add.go`): `ApplyAdd(ctx, pool, batch)` applies a batch from staging to domains in the fixed "apply → cleanup" order — `INSERT … SELECT FROM ingest_stage WHERE batch_id AND action=add ON CONFLICT DO NOTHING` (the conflict — the unique `(zone_id, label)` of migration 000001; zone_id/label are taken from the staging rows — the zone was created by EnsureZone; duplicate labels within a file are deduplicated by the conflict), then a single `UPDATE … FROM staging` reviving deleted rows: `status → new`, `check_state → pending`, `queued_at → NULL` (back into the check cycle; the guard `status = deleted` — other existing rows are not touched at all, flags/dns_mask/last_checked_at are untouched; the batched db.Update from unnest was rejected: the keys are already in the DB, selecting into memory contradicts the streaming of §3.5), `is_idn` is computed in the INSERT from the label (`ILIKE 'xn--%'`, the idn.HasXNLabel semantics — staging does not store the flag); the `AddResult{Inserted, Existing, Revived}` counters for the T-021 report: Existing = the unique (zone_id, label) pairs of the batch before cleanup − inserted − revived (the invariant is checked); the finale — `ClearBatch`; a failure of any step — without cleanup, re-applying is a no-op (§3.4); unit tests without a DB: exact SQL and parameters, operation order, counters, an empty batch, a retry, step errors, ctx cancellation, the SQL contract against the model codes and the UNIQUE of migration 000001. Spec: §5.1, §5.2 (item 9), §6.2, §3.4, §3.5.
- **T-019** [done] — Applying **delete** (`internal/ingest/apply_delete.go`): `ApplyDelete(ctx, pool, Batch)` — moving the status of the domains of a delete batch from staging to `deleted` with a single SQL statement (the `applied` CTE: `UPDATE domains AS d SET status=$2 FROM ingest_stage AS s WHERE batch_id=$1 AND action=$3 AND (zone_id,label)` + `RETURNING` of the keys; the outer SELECT without FROM — a counters row): both sets are already in the DB, so a set-based UPDATE instead of `db.Update` with unnest arrays — the data is not hauled through the application (§3.5), the counters are computed by the same statement and cannot diverge from the application; the domain stays in the DB (§13.1 — the status only, results/content are cleaned by the background task T-020); the transition has no guard on the current status — already `deleted` stays `deleted` and counts toward the deleted counter (a decision per §5.1/§13.1: the final status is recorded, not the path to it; re-applying is a state no-op, §3.4); `DeleteResult{Deleted, NotFound}` — counters over the file lines for the T-021 report (label duplicates each count their own line; NotFound — lines with no domain in the DB); the order: apply → `ClearBatch` (a cleanup failure returns the counters together with the error — the application did happen; a failure between the statements is harmless, a retry finishes the cleanup); batch validation — a non-delete operation is an error (otherwise the final cleanup would silently destroy an unapplied add batch); unit tests on a fake pgx pool: exact SQL/parameters (the `model.StatusDeleted`/`ActionDelete` codes — as parameters), call order, counters, an empty batch, an idempotent retry, apply/cleanup errors, ctx cancellation, the SQL contract. Spec: §5.1, §13.1, §3.4.
- **T-020** [done] — Background cleanup of deleted domains (internal/ingest/cleanup.go): `CleanupOnce` — one pass with a batch of up to `DefaultCleanupBatch`=1000 domains (comparable to the spec 5.3 scheduler batches, a short transaction): candidates — deleted domains with remaining subdomain_results (EXISTS — the rows are "marked" by the data itself: cleaned rows are not re-selected, the schema is unchanged, a failure loses nothing), in a single transaction (the `CleanupDB` interface = db.DB + Begin, satisfied by pgxpool) DELETE subdomain_results in unnest batches with a guard re-checking status (a domain resurrected by an add file keeps its results) and a batched UPDATE contents `refcount = refcount - h.n` over aggregated content_hash values (NULL — no content, length 16 — an invariant); refcount→0 does NOT physically delete the content — packs/GC is stage 4 (T-042/T-045); laravel_* is outside the task scope (the plan). `CleanupDeleted` — a ticker loop (the interval `DefaultCleanupInterval`=1 min, the first pass right after startup; the launch moment is by schedule, not the ApplyDelete event: T-019 is not API-coupled, repeated passes also pick up results written after the deletion, spec 8; pass errors are logged as Warn and do not stop the service, ctx cancellation — a clean exit under the T-012 OnRun); wiring into the service — T-021; unit tests without a DB on a fake pool with transactions (exact SQL, batching, hash aggregation, repeated passes, an empty set, errors/cancellation, the loop; clean under -race). Spec: §5.1, §13.1, §8.
- **T-021** [done] — Accounting and idempotency + assembling the ingest pipeline: the `ingest_files` journal (report.go) — the `FileStore` interface + `SQLFileStore` (FileBySHA256/ErrFileNotFound, RecordFile; INSERT without ON CONFLICT — a concurrent report of the same file is rejected by the unique sha256); a log report by renderReport next to the file in failed/ (the format is pinned by a test: the reason, the line counters by reason, the application; Revived is included in Added — a row revived from deleted is also an effect of the file "added", there is no NotFound column in §6.1 — it is visible in the log report); the end-to-end Pipeline (pipeline.go): ParsePath → EnsureZone → a single pass over the file (io.TeeReader: sha256 and the parser at once, no second pass — §3.5; the pushing parser ↔ the pulling Stage via a bounded-buffer channel) → validation with the rejected log (T-016) → Stage (T-017) → ApplyAdd/ApplyDelete (T-018/T-019) → the report → the move; a duplicate sha256 — the batch is discarded, no application, the file → processed/ without a new report (a decision: a duplicate is not an error); a file is failed (the path structure/zone/the T-016 rejection share threshold) → failed/ with a log report `<file>.report`, no journal row is written — a fixed file can be uploaded again; a DB/read failure → the file stays in incoming (at-least-once, §3.4), the dead batch is cleaned best effort (every attempt gets a new batch_id); the processed/failed archive — the top level of incoming (as the T-013 watch ignores them), the zone/operation structure is preserved, a name collision — a -2/-3… suffix; cmd/ingest — a real service: config → pool (OnDBPool) → watch (T-013) → the pipeline, CleanupDeleted (T-020) as a separate goroutine in the same OnRun; the rejected log — the shared stderr with the service=rejected attribute; no new config options (§5.10 does not parameterize the directories or reports); unit tests on fakes (a pool, ZoneStore/FileStore, the t.TempDir FS): add/delete/duplicate/error threshold/broken path/DB failures at every step/ctx cancellation/archive collision/empty file, clean under -race. Spec: §5.1, §6.1, §3.4, §3.5.
- **T-022** [done] — An end-to-end corpus of unit tests for file parsing (a new `internal/ingest/corpus_test.go`, the T-010 corpus style): the "file → lines → valid entries/rejections" cases run the file as a whole through both stages — the T-015 parser + the T-016 validator, connected as in T-017 (significant lines and per-line parser rejections — `invalid_utf8`/`line_len` — go to OnLine/OnReject, Entry records/reasons — from `Validator.Line`), the rejected logger writes to io.Discard (the format is pinned by the T-016 tests). Composition: an empty file, a file of comments only (including with leading whitespace), a file of empty lines, CRLF, a BOM at the start (stripped by the parser) and in the middle of the file (stays in the data, removed by the UTS-46 normalization — separating the roles of the stages), duplicate lines and the unicode/punycode forms of one IDN domain — the contract is pinned: every occurrence is valid and yields its own record, dedup is the DB's job (T-018), the pipeline does not drop repeats; lines not from the path zone (`wrong_zone`, including a nested public zone `example.co.uk` with the zone `uk` and a composite path zone `co.uk`), broken punycode (`invalid`), long names (`label_len` 64 octets, `name_len` 254), spaces around (stripped by the parser) and inside the line (removed by normalization); a mixed file with everything at once and end-to-end physical line numbering; per case — the Stats/ValidatorStats counters (plus the coupling `Stats.Data = ValidatorStats.Total` — significant parser lines reach the validator, parser-rejected ones do not), the records and reject reasons, the Reasons distribution as a derivative of the rejections; a separate test — the `max_error_ratio` threshold at the boundary end-to-end through a file (a share equal to the threshold does not fail the file — 2/4 at 0.5 and 1/10 at the default 0.1, above the threshold fails it — 3/4 and 2/10; `Failed` — the T-021 signal). No parser/validator defects found: all corpus expectations matched the actual behavior on the first run. Identifiers with the corpus* prefix against collisions with existing package names. Spec: §11.1, §5.1, §5.2.
- **T-023** [done] — Integration tests of the ingest pipeline against live PostgreSQL (a new `test/integration/ingest_test.go`, the neighbors' style: skipUnlessIntegration without CRAWLER_INTEGRATION=1, an isolated schema with migrations via migratedSchema per test — run idempotency, incoming = t.TempDir): the level — the real T-021 Pipeline (SQLZoneStore/SQLFileStore, COPY into staging with BatchSize 4, applying add/delete) along the mandatory path "a file on disk → the pipeline → the DB + the move" (the fsnotify loop — T-013 unit territory). Scenarios: an add file — automatic zones creation (name punycode, name_unicode NULL for ASCII/unicode for IDN, is_active), domains new/pending/queued_at NULL, the is_idn bit for IDN labels (the label without the path zone — idn.Result.Label for a composite non-PSL zone, the expectation is computed by cutting off the zone suffix), the ingest_files report (the sha256 of the content, counters), the file → processed with the structure, staging empty; a unicode and a punycode directory of one zone — a single zones row (§11.4 item 1); a duplicate sha256 — not applied, no second journal record, both copies in processed (the collision — the -2 suffix); a modified file at the same path — existing (alive/done with a lease) rows unchanged, new ones added; a delete file — the deleted status, the domain stays in the DB (§13.1), NotFound in the pipeline report (no column in §6.1); reviving a deleted one with an add file — new/pending, queued_at reset; invalid lines (wrong_zone, broken punycode) within the threshold — rejected in the counters/Reasons, the file applied and in processed; a rejection share above the threshold — the file is failed as a whole (not a single line is applied), failed/ with a log report of the reason counters, no ingest_files record; no production-code defects found. Spec: §11.2, §5.1, §5.2.

## Stage 2. Queues and scheduler

- **T-024** [done] — Package `internal/queue`: the `Queue` interface (stream.go — Publish/Consume/Ack, the transport is replaceable: the methods do not expose Redis specifics, spec 4.2), `Delivery` (the record ID for XACK/XAUTOCLAIM + the raw payload) and the `Decode[T]` generic; the `RedisQueue` implementation (redisq.go): XADD into the single `data` field (the MAXLEN ~ parameter in `RedisOptions`, the policy — T-025), XREADGROUP ">" in batches with Block (2 s by default), XACK separate from delivery (after writing to the DB — T-025), lazy group creation XGROUP CREATE MKSTREAM starting at "0" (the first consumers also receive what was published earlier), BUSYGROUP is not an error, a cache of created groups; message formats (message.go, JSON in the `data` field: the messages are tiny, readability in redis-cli and evolution by adding fields matter more — unknown fields are ignored): an envelope with `task_id` (16 random hex bytes, generated at publish time, survives the move to q:dead) + `DNSBatch` (q:dns — a batch {zone_id, domain_id, name punycode}), `HTTPTask` (q:http — the domain + DNS-alive subdomains with addresses and CNAME — carried in the task per spec 5.4 item 4), `Result` (q:render/q:analyze — the subdomain_results key: domain, subdomain, variant), `Dead` (q:dead — the format pinned in advance: task_id, the original stream/record, attempts, the payload unchanged; the producer — T-025); `Message` is sealed by a non-exported method, Publish validates the stream and values before writing; unit tests without live Redis: the wire formats of all streams as exact JSON, round-trip, malformed data, command mapping on a fake of the narrow `redisStreams` interface (satisfied by *redis.Client; no new dependencies, including miniredis, were introduced). Spec: §4.2, §3.4, §5.4 (item 4).
- **T-025** [done] — Queue reliability (new `internal/queue/reliab.go` + `reliab_test.go`; targeted edits of redisq.go/redisq_test.go/queue.go — applying the MAXLEN default, doc comments, justified below). MAXLEN: T-024 already applied `RedisOptions.MaxLen` in the single `RedisQueue.Publish` (all XADD go through it) — there was no gap; T-025 added the defaults policy `DefaultStreamMaxLen` (0 in the options → the per-stream limit, an explicit value overrides all streams; "do not trim" is not provided — spec 4.2 requires MAXLEN on every stream, and the source of truth is PostgreSQL: a trimmed unprocessed task is recoverable by the scheduler via lease, spec 3.4/5.3): q:dns 10,000 (records — batches of up to scheduler.batch_size=5000 domains ≈ up to ~250 KB; the depth is realistically tens, 10,000 batches = 50 million domains "in flight", memory bounded in GB), q:http/q:render/q:analyze 1,000,000 (single items ~100–200 B ≈ 150–200 MB per stream, an order of magnitude above the 100,000 backpressure threshold), q:dead 100,000 (the flow is rare, manual investigation needs history); MAXLEN ~ trimming cuts primarily the acknowledged history — without it the streams grow unboundedly (350 million domains × several streams). The XACK-after-DB order is formalized by the `Processing(ctx, q, group, delivery, process)` helper: Ack is unreachable before process finishes without error; a processing error — no XACK (the record returns via redelivery), an XACK error — a return (the result is already in the DB, repeated processing is idempotent, spec 3.4). Returning the stuck ones — `Reliab` (NewReliab(rdb, q, ReliabOptions)) `.Claim(ctx, stream, group, consumer, minIdle, n)` via XAUTOCLAIM: spec 4.2 names XAUTOCLAIM directly, go-redis v9.23 supports it fully; one atomic command filters the PEL by idle time, re-claims and evicts from the PEL the MAXLEN-trimmed records (they arrive with an empty body — skipped), whereas XPENDING+XCLAIM is two rounds with a race window and XCLAIM without an idle filter. The attempt counter — the record's own PEL deliveries counter (the RetryCount field of the XPENDING EXTENDED response, read after claiming by the id..id range exactly over the returned records — XAUTOCLAIM does not return the counter; reading has no side effects, races with other recoverers are safe): the stream record body is immutable (a counter in the body = republications and record duplicates), separate counter keys are unnecessary, the counter survives worker restarts; a record = a task (task_id in the body) → the counter is per task_id; the MaxAttempts boundary (default `DefaultMaxAttempts`=3, spec 4.2): counter == MaxAttempts — back to work (3 processings in total: the first delivery + claims), counter > MaxAttempts — Claim itself moves the record to q:dead: publishing Dead (task_id from the body, the payload unchanged, the reason — the limit exhausted: workers do not report queue processing errors) before the XACK of the original record (an XACK failure leaves the record pending and yields a duplicate Dead on the next pass — a duplicate is preferable to a loss), after the XACK the message leaves pending; records that left the PEL between the commands (acknowledged) are skipped; a record outside the package formats (no task_id) — an error for the operator, not a silent move; `DefaultClaimMinIdle`=5m (a margin over the stage timeouts of spec 5.5/5.6), minIdle 0 in Claim → the option. Config (T-006 style): the redis section `maxlen` (0 — per-stream defaults, otherwise 1000..100000000; env CRAWLER_REDIS_MAXLEN), `max_attempts` (3, 1..100 — the minimum is 1: zero deliveries would mean q:dead before the first processing; env CRAWLER_REDIS_MAX_ATTEMPTS), `claim_min_idle` (5m, >0; env CRAWLER_REDIS_CLAIM_MIN_IDLE) + config.example.yaml + docs/configuration.md. Unit tests without live Redis (go-redis has no New*Result factories for XAUTOCLAIM/XPENDING — values are set via SetVal/SetErr of prepared commands): the XADD MAXLEN ~ arguments across all streams (defaults and an explicit limit), XAUTOCLAIM (stream/group/consumer/MINIDLE/Start 0-0/Count) and XPENDING (the id..id range, Count 1), the counter at the boundary (deliveries 1/3 — back to work, 4 — q:dead), the Dead format + the Dead→XACK order of the original stream, a Dead publish failure — no XACK, an XACK failure — Dead published, Processing (ack only after a successful "DB write", no ack on a processing error, an XACK error, a nil handler), skips of trimmed/PEL-left records, XAUTOCLAIM/XPENDING errors, bad arguments; the reliab/Reliab/Claim/Processing/MaxAttempts identifiers without collisions with T-024/T-026. Spec: §4.2, §3.4, §5.3.
- **T-026** [done] — Backpressure (internal/queue/backpressure.go): publishing new tasks only while the stream length is below the `scheduler.queue_high_watermark` threshold (spec 4.2 "strictly below the threshold": len == watermark — waiting). `BackpressureGate` — a capacity gate: `Len` (XLEN) and `AwaitCapacity` — when exceeded, a pause of `Interval` (1 s by default) and a retry, ctx cancellation — a clean exit with the context error, an XLEN error — immediately (the retry decision belongs to the calling loop), a single Warn record on entering the wait; `WatermarkPublisher` — a Queue decorator (for the T-028 scheduler and the workers): Publish waits for capacity and delegates, Consume/Ack — transparent, the gate is not in RedisQueue.Publish — XLEN is Redis-specific, and Queue is transport-replaceable (spec 4.2); the threshold is shared by the working streams q:dns/q:http/q:render/q:analyze (a decision per spec 4.2 "new tasks": a q:dns batch spawns up to batch_size q:http tasks and a q:analyze task per subdomain — the worker streams without a gate grow multiplically faster and threaten the Redis budget ≤0.5 GB of spec 3.2), q:dead is not gated (the T-025 transfer of attempts-exhausted records is not blocked by overflow), Watermark ≤ 0 disables the gate (like MaxLen in RedisOptions); XLEN counts all records, including those not yet XACKed (records disappear only by MAXLEN trimming ~ T-025) — the measure is conservative, the gate is soft regulation, the hard cap is MAXLEN; the config was not changed (the default 100000 and the ≥1 validation were correct), the threshold stays in the scheduler section (the legacy place per spec 5.10), the semantics are fixed in docs/configuration.md; unit tests without live Redis (fakeBackpressure = fakeRedis+XLEN): XLEN mapping, the threshold below/equal/above, waiting with a retry via the interval, a capacity check before every batch, ctx cancellation, XLEN errors, q:dead/disabled/foreign stream, end-to-end publishing and passthrough Consume/Ack. Spec: §4.2, §5.3.
- **T-027** [done] — Scheduler, batch selection: the `internal/scheduler` package (dispatch.go): `DispatchBatch(ctx, pool, DispatchQuery)` — a single SQL with zone interleaving (spec 5.3: "round-robin… so that a large zone does not hold up the others"): `row_number() PARTITION BY zone_id ORDER BY id` numbers the zone queues exactly over the partial `domains_pending_idx` (migration 000001), the sort `(rn − 0.5)/weight, zone_id, rn` yields one domain per zone per round — instead of N queries per zone and instead of ORDER BY zone_id (a large zone would fill the whole batch); optional zone weights (≥ 1, default 1) — the `ZoneWeights` argument in the unnest arrays `$n::int2[]/int4[]`, a proportional stride schedule; there is no weight column in the schema (spec 6.1 does not require one) — no migrations; the result — a batch of `DispatchRow{domain_id, zone_id, name}` in emission order, name = `label || '.' || zones.name` (punycode, spec 5.2) — ready q:dns message data for T-028; the guard `status <> deleted` (the T-019 delete application does not touch check_state — without the guard, deleted candidates would be selected forever and their result would never be recorded per spec §8; those revived by T-018 come back on their own) and the inactive-zone filter `zones.is_active`; pagination of repeated selections — OFFSET in the deterministic sort (the selection does not change state until T-028; after moving to queued the rows leave the selection — Offset 0; concurrent pending returns with smaller ids may give repeats at page boundaries — safe, at-least-once delivery per spec 3.4); memory — streaming reads, the batch is bounded by the limit (§3.5); the `scheduler.batch_size` validation was tightened to 1000..10000 (§5.3 "in batches (default 1000–10 000)" — normative), docs/configuration.md; unit tests without a DB on a fake pgx pool: the SQL contract and parameters (limit/offset/weight arrays), the row order, an empty result, validation, read errors, ctx cancellation; dispatch/Dispatch-prefixed identifiers against the parallel T-030/T-031. Spec: §5.3, §4.2, §3.5.
- **T-028** [done] — Scheduler: moving the selected domains to queued with a lease and publishing the batches to q:dns (a new enqueue.go in internal/scheduler): `EnqueueBatch` — the input is a batch of DispatchRow (T-027), with a single batched UPDATE `db.Update` unnest-style (the keys zone_id+id — the domains PK, each array is one parameter; check_state/queued_at — SetExpr: the queued code as a model literal, `queued_at = now()` — the DB server, a single lease start moment for the batch) moves to queued ONLY the rows still pending (the guard `t.check_state = pending`: between the T-027 selection and the move the state could change — a concurrent T-029 lease return, a T-018 resurrection, a second scheduler), RETURNING the keys of the rows actually moved — the publication is built exactly over them, the race losers are excluded and picked up by the next cycle (at-least-once §3.4); the operation order "move → publish" (§3.4: the source of truth is PostgreSQL — a failure after the UPDATE leaves queued rows without a task, the T-029 lease return brings them back: bounded delay without duplicates; the reverse order on failure would produce q:dns tasks for pending rows with endless re-enqueueing; a publish failure does NOT roll back the move — the XADD might have succeeded with a lost response, a rollback would yield duplicates in work, there is one recovery path — the lease; stuck queue messages are returned by the T-025 XAUTOCLAIM); publication — one DNSBatch{zone_id, domain_id, name} message to q:dns from the moved rows in selection order (zone interleaving preserved), a duplicate key is published once, pre-validation of the rows before the UPDATE does not put values into queued that the queue would reject; the `EnqueueLoop` cycle for OnRun: DispatchBatch → EnqueueBatch → a pause (DefaultEnqueueInterval 1 s), an empty batch/a step error — waiting for replenishment (DefaultEnqueueIdleWait 30 s, failed rounds do not rush batches into queued in vain), errors — Warn without stopping, ctx cancellation — a clean nil; the T-026 backpressure is absent from the tree (a branch off master predating it) — a direct Publish, the rate is limited by the pause, a TODO(T-026) in the code; assembling cmd/scheduler (like T-021 for ingest): config → pool (OnDBPool) → queue.NewClient/RedisQueue → EnqueueLoop + background RecheckDaily (T-030) and RetryTempErrors (T-031) goroutines in one OnRun; unit tests in enqueue_test.go on a fake pool/queue: the SQL contract/parameters, the pending guard, the race (not all moved/all lost), duplicate keys, DNSBatch grouping, an empty batch, validation, UPDATE/read RETURNING/publication errors (the move is fixed — Queued in the end), ctx cancellation, an EnqueueLoop smoke test (the selection→move→publish order, waiting on an empty batch, continuing after a publish failure), clean under -race; docs/services.md — the scheduler as the not-a-stub exception. Spec: §5.3, §4.2, §3.4.
- **T-029** [done] — Scheduler: returning expired leases (internal/scheduler, lease.go): a `LeaseOnce` pass — a single `UPDATE … FROM (SELECT … ORDER BY zone_id, id LIMIT)` statement with a batch of up to `DefaultLeaseBatch`=5000 (the T-030 style): domains `check_state = queued` with `queued_at` older than `now() − scheduler.lease` → `check_state = pending`, `queued_at = NULL` (the pending invariant from T-018/T-030); the `scheduler.lease` field has existed since T-006 (the default 30 min per §5.3, validation > 0, env `CRAWLER_SCHEDULER_LEASE`) — no config edits were needed, the semantics clarified in docs/configuration.md; "no result" is built into the condition: a worker that wrote a result moves the domain to done — the row is no longer queued; a NULL queued_at is filtered out by the comparison (a broken T-028 invariant is not moved by age); deleted is excluded in the subquery and the outer guard (spec 13.1, like T-030: deleted ones are outside the check cycle, a T-018 resurrection sets pending itself; the filter is exactly in the subquery — otherwise dead rows would fill the batch limit and starve live returns), the outer WHERE re-checks the selection against the current row version (a race with a result write between the selection and the lock), repeated passes are idempotent (§3.4); the `LeaseWatch` loop under OnRun (T-012): an initial drain with all batches at once (picking up a hung one during idle — §3.4, 11.4 item 9), then a ticker with the lease/2 interval — a balance of "return speed vs load": there is no index over queued (the partial one is pending-only), the frequency must scale with the lease term, an expired lease is detected no later than half the term after expiry (a hang ≤ 1.5×lease), a fixed interval would oversleep a short configurable lease; a batch error — Warn and the loop does not stop, ctx cancellation — a clean nil; the wiring into cmd/scheduler — T-028; unit tests on a fake pgx pool (lease_test.go): the SQL contract and parameters (the now()−lease boundary, the queued guard, the queued_at reset, LIMIT), the defaults (including Interval = Timeout/2), an empty set, repeated passes, errors/ctx cancellation, the loop (an immediate drain, surviving an error, a clean exit). Spec: §5.3, §3.4.
- **T-030** [done] — Daily recheck (a new `internal/scheduler` package, recheck.go): `RecheckOnce` — one pass with a single `UPDATE … FROM (SELECT … ORDER BY zone_id, id LIMIT)` statement, batches of up to `DefaultRecheckBatch`=5000 (the spec 5.3 batch range): domains `check_state = done` with `last_checked_at` older than the interval and `status <> deleted` → `check_state = pending` (a decision per §5.3/§6.1/§6.2: done/pending are `check_state` states, the result status does not change — it keeps the outcome of the previous check until the new one, i.e. status=alive + check_state=pending = "awaiting recheck"; deleted is excluded per §13.1; queued_at → NULL — the pending invariant from T-018); the autocommit of a single statement — short locks (the T-020 style), the outer WHERE re-checks the selection against the current row version (a race with a T-019 delete file), repeated passes are idempotent (§3.4); `RecheckDaily` — a loop for OnRun (T-012): an initial drain with all batches until exhaustion (picking up a matured one during idle, like CleanupDeleted T-020), then the ticker `DefaultRecheckSchedule`=24 h (the daily cadence — spec 5.3, not parameterized in the config), a batch error — Warn and continuing with the next drain, ctx cancellation — a clean nil; `RecheckOptions{Interval, BatchSize, Schedule, Now, Log}` (Interval — from `scheduler.recheck_interval`, the default 4380 h ≈ 6 months per §3.3 was already correct, the value is used as an age threshold — "no more often than the interval" §11.4 item 8, no config edits were needed), the semantics clarified in docs/configuration.md; the wiring into cmd/scheduler — a later task; unit tests on a fake pgx pool: exact SQL/parameters (the check_state/status codes, the now()−interval boundary, LIMIT), the defaults, an empty set, repeated passes, errors/cancellation, the loop (an immediate drain, surviving an error, a clean exit). Spec: §5.3, §3.3, §11.4 (item 8).
- **T-031** [done] — Scheduler: rechecking `temp_error` (a new `internal/scheduler` package, retry.go): a `RetryOnce` pass with batches of up to 1000 domains (status=temp_error, check_state=done — the increment point "one retry assignment per check episode", last_checked_at older than `temp_error_retry_after` — the option has existed since T-006) in a single transaction splits the batch by the `domains.temp_attempts` counter: attempts < N → check_state=pending (the T-027/T-028 dispatcher will re-select it) + counter+1; N or more → the final status delegated_no_address + a counter reset; the final status is justified by the spec: the last check SERVFAIL/timeout = "no addresses" — the terminal branch of the §7 algorithm, not_delegated was rejected (it encodes exactly NXDOMAIN, RFC 8020), the status set is not extended (§13.1), a classification error self-corrects via the T-030 recheck loop; guards in the UPDATE (status+check_state at write time — races with T-018/T-029/T-030 do not double-increment), RETURNING — counters only over the actually affected rows; an attempt counter is inexpressible without a schema change (check_state is an enum, flags are bit booleans, dns_mask is a subdomain mask, timestamps do not count, there is no result history §3.5) → migration 000005 `domains.temp_attempts smallint NOT NULL DEFAULT 0` (the last resort per the plan; metadata-only on a partitioned table, no index — HOT updates preserved, the schema_test.go contract updated, the Domain model extended), the reset on a successful (non-temp_error) check — the DNS stage contract T-036+ (until then the counter may stay non-zero between episodes — the effect is conservative); the new option `scheduler.temp_error_max_retries` (default 3 — like the T-025 queue delivery attempts, the spec sets no number; env `CRAWLER_SCHEDULER_TEMP_ERROR_MAX_RETRIES`, validation 1..100); the `RetryTempErrors` loop (the first pass immediately, an error — Warn and no stop, ctx cancellation — a clean nil under OnRun; the interval min(retry_after, 1 h) — the loop does not oversleep its own window), the wiring into cmd/scheduler — a later task; unit tests on a fake pgx pool: SQL/parameters, the retry_after window boundaries, the counter below N/at N/past N, an empty set, the guards, errors/ctx cancellation, the loop; retry-prefixed identifiers against collisions with T-027/T-030. Spec: §5.3, §6.2.
- **T-032** [done] — Integration resilience tests against live Redis/PostgreSQL (a new `test/integration/resilience_test.go`, the neighbors' style: skipUnlessIntegration, migratedSchema; Redis isolation — dedicated DB 15 from the dev config databases 16, the services run in db 0: the production stream names `q:*` — only transport constants (knownStream), keys are cleaned before/after each test, the running stack does not interfere with the tests). Scenarios: (1) publish → consume → write the result to the DB → XACK (Processing): two batches via DispatchBatch/EnqueueBatch, the history published before the group creation arrives too (XGROUP starting at "0"), a processing failure — no XACK, a new consumer with ">" re-delivers nothing, a claim returns the record (attempts = 2), the repeated processing completes the cycle — all domains done, the PEL empty, q:dead empty; (2) stopping a worker in the middle of a q:http batch (6 tasks, 2 acknowledged): XAUTOCLAIM returns the unfinished remainder with the original body (the payload byte-for-byte), finishing the work restores all task_id, no losses and no q:dead; (3) attempts exhaustion (q:render): claims grow the PEL deliveries counter 2→3 (still in work), the next pass — the move to q:dead (Dead: task_id/stream/entry_id/attempts=4/payload unchanged) and the XACK of the originals, the PEL empty, all task_id alive in q:dead; (4) Redis loss (DEL of the streams together with the groups): domains hang in queued → aging queued_at with direct SQL (deterministically, without 30 min) → LeaseOnce returns 10 to pending (queued_at NULL) → DispatchBatch/EnqueueBatch re-publish the batch, a live worker (the same queue instance) consumes the republished batch after NOGROUP; the id set and the domain count unchanged (the source of truth is PostgreSQL, spec 3.4); (5) backpressure: publishing below the threshold goes through, at the threshold WatermarkPublisher blocks, freeing capacity (XDEL) lets the publication through, ctx cancellation — a clean context.Canceled; (6) MAXLEN ~: 300 publications with a limit of 40 — the length is bounded by the threshold + one list macronode (~100 records), the first published record trimmed. A production-code defect was found and minimally fixed: RedisQueue.ensureGroup cached the group creation forever — after losing Redis with live workers every XREADGROUP failed with NOGROUP until the process restarted; the fix: on NOGROUP the group cache is reset, the group is recreated (XGROUP CREATE MKSTREAM), the read is retried (the unit test TestRedisQueueConsumeGroupLost without live Redis + scenario 4 integration-wise); docs/dev-environment.md — a note about DB 15. make test / make lint / make test-integration green. Spec: §11.2, §3.4, §11.4 (item 9).

## Stage 3. DNS stage

- **T-033** [done] — unbound configuration per spec 5.4/3.2: `unbound.internet.conf` (the `make up-internet` mode) — pure recursion from the root servers (no forward-zone, built-in root hints; verified functionally in a throwaway container: example.com resolves from the root, a repeated query — from the cache), `prefetch: yes` (a record requested with a remaining TTL <10 % is refreshed before expiry — "popular records do not fall out of the cache"), `num-threads: 2` for the dns.concurrency 2000 / max_qps 10000 load; caches in the §5.4 window of 512–768 MB: msg 128m + rrset 384m (≈3× msg — the NLnet Labs 2–4× recommendation) + neg 16m (the crawler's mass NXDOMAIN) + key 4m (the DNSSEC validator is disabled — module-config "iterator", as it was) + infra-cache-numhosts 100000 (~10 MB of NS RTT/timeout statistics) ≈ 542 MB; memory: `mem_limit ${MEM_UNBOUND:-700m}` — the §3.2 budget "Unbound ≈ 0.7 GB", the docker analog of MemoryMax §10.1, .env.example MEM_UNBOUND 256m→700m (the testbed mode has enough with 256m — the limit is not a reservation); measured: the starting RSS ~16–20 MB, the cache tables grow lazily, the limit must fit a filled cache + ~160 MB of margin. A fallback to public resolvers in unbound is deliberately NOT configured — unbound semantics: forward-zone "." intercepts all queries and makes the forwarders primary, forward-first: yes only changes the failure direction (a forwarder SERVFAIL → its own recursion), the direction "recursion → forwarder on SERVFAIL" does not exist (the unbound 1.26 documentation; such a fallback is done on the client); the list consumer — the dns-worker DNS client via `dns.fallback_resolvers` (internal/dnsclient, T-034; the field + env CRAWLER_DNS_FALLBACK_RESOLVERS + the §5.10 defaults 1.1.1.1:53/9.9.9.9:53 have existed since T-006 — no config edits were needed), the list is not passed to the unbound container. The testbed mode without behavioral changes (forwarding everything to coredns, internet isolation for T-040; the conf has an updated cache comment and infra-cache-numhosts explicitly = the default). Validation: unbound-checkconf of both configs mounted into a throwaway crawler/unbound:local container — no errors, the control values (-o) matched; make test/lint/fmt green, docker compose config OK. Documentation: docs/docker.md (section 9 "Unbound": modes, memory, the fallback rationale), docs/configuration.md (the fallback_resolvers consumer), config.example.yaml (a comment). Production systemd units with MemoryMax (§10.1, Annex A /deploy) — outside the docker stage, take the same values when moving to deploy. Spec: §5.4, §3.2, §10.1, deploy (Annex A /deploy).
- **T-034** [done] — The `internal/dnsclient` package (client.go + pool.go): a DNS client over `miekg/dns` (the dependency was listed in go.mod since T-001 — none added); a pool of connected (connect(2)) UDP sockets — an eager fail-fast set, Get/Put/Discard (retirement → a lazy set by the nearest Get, a refusal does not block the pool), sending via a channel under a Close mutex (no "socket stuck in a drained channel" race), the size — a parameter, the default 2000 = the spec 5.10 dns.concurrency — the pool must not cut the planned parallelism (one socket = one query in flight, the waiting ones — a Get queue: the pool is itself the natural concurrency bound); TCP-fallback on TC=1 — repeating the same question over TCP with its own timeout, a connection per query without pooling (TC is a rare branch, keeping a pool of TCP connections — costs without gain; a TCP error fails the attempt, the cycle starts over with UDP); retries — repeats beyond the first on timeout/network error with a pause, ctx cancellation — before an attempt, after every failure and during the pause; the contract for T-036…T-039: `Exchange(ctx, Question{Name,Type})` → `Result{RCode, Answer/Ns — raw []dns.RR, Elapsed, Attempts, TCP}` — an RCODE (the NOERROR/NXDOMAIN/SERVFAIL/REFUSED/… constants, miekg mnemonics) is not an error (NXDOMAIN→not_delegated, SERVFAIL/timeout→temp_error — the consumer's decision), an error — only the network after the attempts are exhausted; `RecordsOf` — a filter by type, CNAME chains are behind the consumer (T-037); `IsTimeout` — the distinction for the T-035 adaptive reduction; the T-035 extension points — the `Resolver` interface (a decorator of concurrency/QPS limits and the AAAA filter) and the pool boundary; late answers/garbage datagrams on a reused socket are skipped (a fresh Id per exchange, reading until our own deadline); config — only the missing fields of the dns section (timeout 2s, tcp_timeout 5s, retries 2 — the system's single retry policy, retry_delay 200ms, udp_pool_size 2000; env `CRAWLER_DNS_*`, validation in the T-006 style) + config.example.yaml + docs/configuration.md; unit tests on two levels: the transport seam exchangeFunc (retries, TC→TCP, RCODE mapping, ctx cancellation, attempt counters, malformed questions — no network) and a local miekg/dns DNS server on 127.0.0.1:0 udp+tcp (the real wire: an exchange, TC→TCP, RCODE, timeouts, skipping garbage and foreign Ids), the pool — concurrency invariants under -race. Spec: §5.4, §5.10.
- **T-035** [done] — Configurable DNS limits (new `internal/dnsclient/limits.go` + `limits_test.go`; the T-034 client.go/pool.go untouched): the `LimitResolver` decorator over the `Resolver` interface — the Exchange contract unchanged, the T-036+ consumer depends on the interface. Concurrency — a `limitSemaphore` semaphore with a mutable cap (the adaptive lowers the cap for new arrivals without taking slots from the in-flight ones — a natural drain; broadcast wakeup — the semaphore protects, it is not a queue of thousands of goroutines). QPS — a `limitBucket` token bucket with lazy refilling and no background goroutines (against a fixed/sliding window: the average rate max_qps + short bursts up to burst; the default burst = concurrency — more cannot fly at once anyway). The wait order semaphore→token: the token is taken right before sending — tokens pre-collected in the slot queue would give a burst bypassing the reduced QPS. ctx cancellation waits in both queues, the slot is returned by defer ("do not lose a slot forever"). The adaptive `limitAdaptive` — an O(1) ring window of outcomes (only IsTimeout errors count: spec 5.4 ties the reduction exactly to the timeout share, protocol refusals are a different semantics), decisions after the window fills (the first timeout at startup does not cut the limits), hysteresis: a share ≥10% → ×0.5, ≤2% → ×1.25, a floor of base/100 but at least 1; the reduction is decisive/the recovery cautious (the AIMD argument: while the resolver times out, an exchange burns the attempt budget and holds a slot — you cannot pull; a fast return gives oscillation); the parameters — LimitOptions constants/options with defaults (the spec sets no numbers), a window of 200 exchanges. Disabling AAAA — dns.ipv6=false (the field since T-006, spec 5.10; a separate disable_aaaa was NOT introduced — a semantics duplicate; T-034 left the "AAAA filter (dns.ipv6)" extension point): the limiter answers with a local empty NOERROR (NODATA) without going to the network, without spending the limits and without diluting the adaptive window. Config: concurrency/max_qps/ipv6 existed since T-006 — no field/envSpecs/validation edits were needed; config.example.yaml (comments) and docs/configuration.md (the limiter and adaptive mechanics) updated. A metrics hook — `Stats() LimitStats` (the effective/configured limits, InFlight, the window, counters) for T-077, metrics export not implemented. Unit tests without a network (a fake Resolver, mockable limitClock clocks): the semaphore (the cap, cancellation, dynamic shrink), the bucket (the rate, burst, a rate change), the adaptive (a reduction to the floor, recovery to base, non-timeout errors ignored, a reduction applies to the live semaphore), AAAA without calling the lower resolver, cancellation in both limit queues, the end-to-end pass of results/errors; `-race` green. Spec: §5.4, §5.10.
- **T-036** [done] — The `internal/dnsworker` package (apex.go): `CheckApex(ctx, resolver, domain)` — the first step of the DNS stage (§7): an A query for the apex (the name from q:dns punycode, converted to an FQDN with a trailing dot), with an empty answer — a confirming NS ("and NS when needed" §5.4 item 1: while A answers with addresses, NS is not needed; NOERROR on NS = the name/zone exists — the "there is an NS/answer, but no A/AAAA" branch §5.4 item 5); the classification — `ApexStatus` (4 branches): NXDOMAIN → `ApexNotDelegated` + `Stop()` — an explicit "do not go further" sign (RFC 8020: a negative answer covers the subtree, the T-037 subdomains and the T-038 wildcard are not checked; NXDOMAIN is the only source of not_delegated), NOERROR with A → `ApexDelegated`, the addresses `[]netip.Addr` (the answer order) — the input of the HTTP stage §5.5 item 2 (the q:http format already validates the netip addresses, T-024), NOERROR without A (NODATA) → `ApexDelegatedNoAddress` — the distinguishable §7 "no addresses" branch, but the final status is decided by T-039 (addresses may be found at the subdomains), SERVFAIL/REFUSED/other codes/network after the dnsclient retries → `ApexTempError` (REFUSED — the resolver's statement about itself (ACL/limits), not about the absence of delegation: a false terminal not_delegated is excluded, the episode repeat — T-031); `DomainStatus()` — a model.Status code only for the terminal branches, false for the delegation branches; NXDOMAIN on the confirming NS (a race of a vanished registration between queries) — not_delegated by the last answer; ctx cancellation/deadline — an error to the caller (the episode is not finished, not a domain outcome), an Exchange error with a live ctx — temp_error; the apex AAAA is not queried (§5.4 item 2 — T-037); the DB write/dns_mask/q:http — T-039; unit tests on a scenario fake Resolver (no network): all classification branches (15 cases, incl. a CNAME answer with A and CNAME-only), Stop/NSQueried/the exact number of calls (NXDOMAIN and SERVFAIL — one query, NS only on NODATA), the FQDN format and question types (A/NS, punycode without re-encoding), the composition/order of addresses (a junk A and AAAA are filtered out), the Stop/DomainStatus/String contract, ctx cancellation (before the query / an expired deadline / at the NS step / an error without cancellation — temp_error), an empty name without resolver calls. Spec: §5.4 (item 1), §6.2, §7.
- **T-037** [done] — The `internal/dnsworker` package (subdom.go): `CheckSubdomains(ctx, resolver, domain, []model.Subdomain, askAAAA)` — checking the subdomains of a domain (spec 5.4 item 2): for every enabled subdomain (the label '' — the apex with id 0) an A query with unwinding the CNAME chain + an AAAA query for the terminal name when askAAAA (dns.ipv6 is decided by the T-035/T-039 consumer; disabled — zero AAAA questions); a separate CNAME-type query is not issued — RFC 1035 §3.3.1 requires attaching the CNAME record to the answer to any query to an alias name, the records of all three types are collected from the answers; the chain is unwound from the answer section of the already obtained exchange (the recursors drive it themselves), chasing the terminal — with our own exchange, CNAME takes priority over neighboring A records (RFC 2181 §10.1); the depth limit of 10 hops (Go net, recursors — the same order; a completion guarantee against ladders) + a set of visited names against A→B→A cycles; CNAME→NXDOMAIN of the target is not an error: the subdomain exists, there are no addresses, the sign is in the RCode (RFC 8020, a contribution to delegated_no_address, not temp_error); the outcome `SubdomResult{ID, Label, A, AAAA, Chain (the unwinding: Chain[0]=the direct alias, the last=the final target; the CNAME/Target methods under the q:http field), RCode, Err}` — transport failures, SERVFAIL, NXDOMAIN, a cycle/depth stay in the subdomain's result and do not interrupt the batch (a partial outcome), only ctx cancellation interrupts (a nil report, at-least-once §3.4); the `SubdomReport.Mask` aggregate — the DNSMask with the bits of subdomains having addresses (spec 5.4 item 4), RCode/Err — the raw temp_error signs for T-039; the traversal is sequential without a semaphore — the concurrency/QPS limits are the Resolver decorator (T-035), our own would duplicate it; input validation (id 0..62, the label, duplicates) before the first exchange; disabled rows are skipped; unit tests on a fake Resolver (13 tests + subtests): A with deduplication, a chain in one answer and via exchanges, a long chain, a cycle, depth, CNAME→NXDOMAIN, the AAAA flag on/off, v6-only, an AAAA transport failure with A records found, mask aggregation and error isolation, the apex id 0, validation, ctx cancellation; all identifiers with the subdom prefix against the parallel T-036/T-038. Spec: §5.4 (item 2), §5.10, §7.
- **T-038** [done] — Wildcard detection (a new `internal/dnsworker` package, wild.go + wild_test.go — files strictly with the wild* prefix, in parallel with T-036/T-037 in the adjacent apex*/subdom*): `DetectWildcard(ctx, dnsclient.Resolver, domain)` — the detection query of spec 5.4 item 3: an A for `<random>.<domain>`; the label crypto/rand 16 chars a-z0-9 ≈ 82.7 bits of entropy (a collision with an existing record ≈10⁻¹⁹ even in a zone with a million names; no hyphen → the `xn--` prefix is impossible, the hyphen position rules do not apply; crypto/rand, not math/rand: no seed management and no global mutex, one getrandom(2) per domain is negligible against a DNS exchange); the outcome: NOERROR with A records → the zone is under a wildcard, `WildcardInfo{Detected, Addrs, Label}`; NXDOMAIN → a detection denial (all subdomains are honest); NOERROR without A (NODATA/a wildcard without addresses) → also without a wildcard — an empty set matches nothing; other RCODEs (SERVFAIL/…) and network/ctx errors — a "detection failed" error, it does NOT fail the domain check: the consumer (T-039) continues without filtering — a false negative costs extra HTTP requests to a stub, a false positive — missed sites; a wildcard CNAME wrapper (random → CNAME → A) — the final A addresses from the same answer are compared (the recursive Unbound §5.4 returns the unwound chain RFC 1034 §4.3.2; chasing the target with our own queries — an extra exchange per wildcard domain with no new information); `MarkWildcardSubdomains(wildcard addresses, []WildSubdomain{ID,Label,Addrs})` — the input is generalized, it does NOT depend on the T-037 types; the criterion — a subset: a subdomain is a wildcard ⇔ there are addresses and all of them ⊆ the wildcard addresses (equality — a special case; a truncated round-robin answer of a resolver still counts as coverage; any address outside the set proves an explicit record → HTTP); an empty subdomain set is not marked ("∅ ⊆ everything" would mark everyone); the apex (Label "") and www (Label "www") are never marked — the direct §5.4 item 3 rule "except the apex and www", identified by the label (the subdomains contract, migration 000001: bit 0 — the apex, bit 1 — www), even if they formally answer with a wildcard; the output `WildcardResult{Mask model.DNSMask}` — the same bit semantics as dns_mask (T-039 excludes them from q:http); address comparison net.IP.Equal (the 4-/16-byte forms of one point); an IPv6 address outside the detection A set leaves the dual stack honest (the safe direction); an error — only an ID outside 0..62 (the mask model, a caller bug); unit tests on a fake Resolver (no network): a zone with a wildcard, without one (NXDOMAIN), NODATA, a wildcard-CNAME (with A and without), partially matching addresses, label uniqueness/stability within a call, ctx cancellation, detection errors, an invalid ID, an end-to-end parking scenario. Spec: §5.4 (item 3), §8.
- **T-039** [done] — Writing the DNS result (new `internal/dnsworker/result.go` + `result_test.go`; assembling `cmd/dns-worker/main.go` — not a stub): a domain episode `ProcessDomain` = CheckApex (T-036) → the subdomains CheckSubdomains + DetectWildcard/MarkWildcardSubdomains (T-037/T-038) → the DecideEpisode decision → the q:http publication → the write; statuses per §7: NXDOMAIN → not_delegated, SERVFAIL/timeout → temp_error (the episode is done, T-031 will schedule the repeat), no addresses (including "only wildcard names resolve" — there are no HTTP-checkable ones, an empty q:http is forbidden by the T-024 contract) → delegated_no_address, addresses present → a q:http task (the T-024 HTTPTask: DNS-alive subdomains minus the wildcard bits, the apex/www always) without a terminal status — alive/no_http is set by the HTTP stage (§6.2), the domain keeps its previous/new status until then; dns_mask — always the resolving bits (§5.4 item 4, a wildcard excludes from HTTP, not from the mask); a resolver failure on a subdomain (Err/SERVFAIL/other codes, except NOERROR/NXDOMAIN — RFC 8020) with addresses — a partial T-037 outcome, without addresses — temp_error (a false terminal delegated_no_address is excluded); the order "publish q:http → UPDATE-done" (§3.4, in the spirit of T-028): a write failure → a delivery without XACK repeats the episode (a bounded q:http duplicate — delivery attempts end in q:dead), the reverse order would lose the task forever (done domains are cut off by the filter, only the T-030 recheck could fix it); the write with a single statement and the guards `status<>deleted` (§13.1/§8 — a domain deleted during the check is not overwritten) and `check_state<>done` (the idempotency of redelivery, T-025), SET dns_mask/status (only the terminal branches)/check_state=done/queued_at=NULL (the lease is finished, the T-018/T-029/T-030 invariant)/last_checked_at=now()/temp_attempts=CASE (a reset to 0 on a non-temp_error outcome — the T-031 contract on the DNS stage); the freshness filter of the PendingDomains batch (a single SELECT unnest WITH ORDINALITY, the T-027 batch order) — a redelivery finishes only the unfinished domains, with no repeated DNS exchanges and no q:http duplicates; LoadSubdomains re-reads the catalog per batch (crawlerctl changes without a restart); the Run loop: Consume q:dns (the dns-worker group, the consumer dns-worker-<pid>) → Processing (T-025) → episodes in parallel up to dns.concurrency (a semaphore+WaitGroup, no errgroup — x/sync an indirect dependency), errors — Warn without stopping, ctx cancellation — a clean nil (the T-012 framework OnRun); assembling cmd/dns-worker: config → pool (OnDBPool) → dnsclient.Client (a fail-fast socket pool) + LimitResolver (T-035, AAAA=dns.ipv6) → the q:http publication under the backpressure gate (WatermarkPublisher, T-026); unit tests on a fake pool/queue/resolver (a shared event protocol of the pool and the queue — checking the "publish → write" order): the SQL contracts (schema codes as literals), 11 DecideEpisode cases, RecordEpisode (statements/parameters/guards/cancellation), ProcessDomain (the order, the HTTPTask content incl. AAAA and CNAME, the wildcard incl. a detection failure, publication/write errors, cancellation before/mid-way), PendingDomains/LoadSubdomains, the defaults, a Run loop smoke test (an end-to-end batch → q:http → the write → XACK; an idempotent redelivery — no exchanges and no duplicates; surviving errors — a failed delivery without XACK, the next one acknowledged), clean under -race; documentation — docs/services.md. Spec: §5.4 (items 4–5), §6.1, §6.2, §7, §3.4.
- **T-040** [done] — Integration tests of the DNS stage on the testbed (a new `test/integration/dns_test.go`, the neighbors' style: skipUnlessIntegration, an isolated migratedSchema schema, Redis DB 15 with q:* cleanup): the level — the stage 3 components against live unbound→coredns, the raw TestDNS_TestbedScenarios protocol (env_test.go) is not duplicated — the expectations here are on the classification branches and the T-034…T-039 contracts. dnsclient (T-034): a regular A with one attempt, no TCP; NXDOMAIN/NODATA/SERVFAIL — response codes without an error (an SOA in authority on NXDOMAIN); TCP-fallback — a new testbed scenario `tc-fallback` (dns.a_many: 60 distinct A records 192.0.2.0/24 in scenarios.yaml + an a_many field in testbedgen, the SOA serial 2, the zones regenerated by make testbed-gen — the diff only the new records, the existing fixtures untouched): UDP without EDNS0 → unbound truncates with TC=1, the client repeats over TCP — Result{TCP=true, Attempts=2} with all 60 addresses; a transport timeout with retries and IsTimeout — deterministic against the testbed blackhole resolver 172.28.100.250:53 (via unbound it is nondeterministic: a dead-upstream SERVFAIL ≈0.5 s races the client deadline — the outcome depends on jitter, pinned in the test comment). CheckApex (T-036): delegated (addresses) and a wildcard zone (an explicit apex A), NXDOMAIN → ApexNotDelegated+Stop+not_delegated, SERVFAIL and the dead upstream of the dns-timeout zone (a timeout or a SERVFAIL — both temp_error) → ApexTempError, NODATA+a confirming NS → ApexDelegatedNoAddress with NSQueried. CheckSubdomains (T-037): the apex+www with addresses and admin NXDOMAIN (the mask bits 0,1), a CNAME chain unwinds to the final addresses (Chain/CNAME/Target, mask 0), a wildcard zone synthesizes all labels (mask 0,1,2), a looping CNAME — unbound itself answers SERVFAIL: a subdomain without addresses and without Err (a contribution to temp_error — T-039). DetectWildcard (T-038): a random label of the wildcard zone gets addresses (a 16-char Label), a regular zone — a detection denial without an error; MarkWildcardSubdomains on live subdomain outcomes — admin/crm/api/app marked, the apex and www never. End-to-end Run (T-039, minimal — delivery resilience is not duplicated, T-032): a q:dns batch (same-site + nxdomain-case) via live Redis/PostgreSQL — both records done in the isolated schema (the delegated one: the new status kept — the outcome belongs to the HTTP stage, dns_mask bits 0,1, queued_at NULL, last_checked_at; NXDOMAIN: not_delegated, mask 0), q:http exactly one task with the apex and www addresses (the NXDOMAIN domain — no task), the q:dns PEL empty (XACK after the write), ctx cancellation — Run returns nil. No production-code defects found; docs/dev-environment.md §4 — a_many in the list of DNS scenario fields. Spec: §11.2, §5.4, §7.

## Stage 4. Content storage

- **T-041** [done] — The `ContentStore` interface (`Put`/`Get`/`Delete`) in `internal/content` (new store.go + store_test.go) for later replacement of the implementation, e.g. with S3 (spec 5.7): `Hash` — 16 bytes of SHA-256 of the normalized content, truncated to 128 bits (§5.7/§6.1; the algorithm is pinned by `HashOf` — Put must return `HashOf(data)`; "normalization" = UTF-8 and the final page of the §5.5 item 4/8 pipeline — the store receives ready bytes, otherwise the round-trip and dedup are undefined; HashOf also lets T-054/T-055 compare content without going to the store); `Put → PutResult{Hash, Created}` — the "new/dedup" fact for the dedup share metrics (§11.4 item 3) in one call, without reading metadata; the interface/implementation boundary: the contract is a thin bytes layer (the T-042/T-043 compression and refcount are implementation details; the pair "Put added a reference — Delete removed it" is the method semantics, the counter is kept by the implementation in contents.refcount T-044, rewriting a result with a content change — the behavior of the results layer over the DB, not a store method; physical freeing — the T-045 compaction); sentinel errors: `ErrNotFound` (Get/Delete, including a repeated Delete — a symptom of an accounting error under at-least-once §3.4), `ErrStoreFull` (the store/disk limit §8: a write pause + an alert, T-046/T-080), `ErrTooLarge` (the max_store_bytes limit §5.5/§5.10, T-056 stores the result without content); the contract: Put idempotency, Get — an independent copy, empty data — valid content, ctx cancellation — the operation did not happen (a reference is not added/removed); the contract suite `StoreContract(t, newStore)` (round-trip, dedup, Put×N/Delete×N reference pairing, errors, cancellation, parallel Puts of one content) is run against the reference in-memory implementation and is reused by the T-042/T-046 implementation tests (ErrTooLarge/ErrStoreFull depend on the implementation configuration — its own tests' territory); the file name/identifiers — the store* prefix against the parallel T-043 (dict*), collisions checked with grep. Spec: §5.7.
- **T-042** [done] — The ContentStore implementation on pack files (`internal/content/pack.go` + `pack_test.go`): the `content.dir` directory (a config since T-006, no new fields needed), `pack-NNNNNN.pack` append-only files, the v1 format pinned in `docs/content.md` — a 23-byte header (the magic "CDPK", the version, hdrLen for evolution, pack_id, the rotation threshold, the dictID of the T-043 dictionary) + records `[u32 LE len][a zstd CompressWithDict frame]`; `contents.off` — the offset of the compressed data (a read with a single ReadAt), `raw_len` is verified after decompression (corruption detection); pack_id — the ordinal number of the file name, the pack registry — the set of files + the header (there is no packs table in §6.1, no migration introduced), the newest pack = the max id; rotation by `content.pack_size` (512 MB default, §5.7 512 MB–1 GB validated by the config), the first block of a pack is always written (rotation advances); multi-process writing (http-worker + render-worker): `flock .packs.lock` for the duration of appending — the size stat and the id allocation of a new pack under the lock, `O_APPEND`, a mutex in-process, Get without locks (append-only, files are opened for reading on demand); metadata via `internal/db` with four statements (dedup UPDATE-first — live content writes no bytes, Created=false per T-041; INSERT ... ON CONFLICT with EXCLUDED placement; SELECT with `refcount > 0`; Delete refcount−1), the order "spool the block → commit the metadata" (§3.4): a crash between the steps — a garbage block without a row, invisible, cleaned by the T-045 compaction; refcount 0 — a "dead" row stays (Get/Delete do not see it), a resurrection by a new Put — a fresh block + EXCLUDED placement, Created=false (indistinguishable from a conflict by tag, pinned); errors: ErrTooLarge per max_store_bytes, ENOSPC → ErrStoreFull (the proactive threshold — T-046), ErrNotFound; tests on a fake pgx pool with live refcount semantics and exact SQL (the ingest style) + real pack files in t.TempDir: the T-041 StoreContract (the main check), format/header/offsets, rotation with a small threshold, dedup without writing bytes, the crash window, dead rows, the limit, reopening the directory, the dictionary on/off (the dictID in the header), concurrency (clean under -race), closing, the ENOSPC mapping; documentation: a new `docs/content.md`, configuration.md, config.example.yaml, README. Spec: §5.7, §6.1, §13.2, §3.4.
- **T-043** [done] — zstd compression with a trainable dictionary (internal/content/dict.go): training `TrainDict` — the pure Go `zstd.BuildDict` of the installed klauspost/compress v1.20.3 (it exports a trainer — the external `zstd --train` binary and new dependencies were not needed); the dictionary content (History) — a greedy selection of 16-byte segments by frequency (≥2 occurrences, frequencies over a corpus prefix of up to 4 MiB, Contents — up to 64 MiB) without intersections within a sample — a long repeat is collected by neighboring segments without duplicates; determinism — a full sort of the candidates, the dictionary ID = FNV-1a(32) of the content (an ID of 0 is forbidden by the format, at 0 → 1); `Dict` — the `WithEncoderDict`/`WithDecoderDicts` codecs (thread-safe, frames without a dictionary are also readable — the choice by the frame's dictID), `Save` atomically (tmp+rename), `LoadDict`; the package-level active dictionary: `SetDict`/`SetDictFile` (a missing file is not an error, a log + the framework fallback Compress/Decompress; a broken one — an error) and `CompressWithDict`/`DecompressWithDict` (without a dictionary = Compress) — the T-042 hook point; a measurement on a deterministic corpus of parking pages: 2048 B → without a dictionary 1764 B, with one 1179 B (−33%, the test asserts only the fact of improvement, without brittle percentages). The `crawlerctl dict train <file> [--samples PATH]…` command (retraining = another run; the name follows the verb style, like reanalyze): samples — *.html/*.htm files/directories recursively (the default testdata/laravel-samples, like test-rules), reading up to the corpus limit of 64 MiB, printing the size/ID/compression estimate, flags are allowed after the positional argument (reordering — stdlib flag stops at the first positional); exit codes per T-011 (0/1/2), routing/flags/end-to-end run tests; the `content.zstd_dictionary` config field existed since T-006 — unchanged; documentation: dev-environment.md §6, configuration.md (a note on the field), config.example.yaml (a comment). Spec: §5.7, §13.2.
- **T-044** [done] — Swapping the content reference when a result is rewritten (internal/content/refcount.go): `SwapRef(ctx, pool, oldHash, newHash)` — an atomic swap in one DB transaction: `+1` to the new hash, then `−1` to the old one — the same statements/contents table as Put/Delete T-042; the order "first +new, then −old" is pinned per §3.4: a crash between the separate steps leaves an extra reference on the old content (safe — the block stays alive, a space leak), the reverse order would lose a live reference (refcount→0 for content referenced by a result, the T-045 compaction would free the block — re-applying the §5.7 rules would be impossible); scenarios: `old == new` — a no-op without a transaction (the redelivery idempotency §3.4), a zero Hash — "no reference" (NULL content_hash §6.1): an old zero → only +, a new zero → only −, both — a no-op; refcount 0 after − — the row is dead, stays (the T-042 contract), the space is cleaned by T-045; the guard refcount > 0 in both statements: RowsAffected 0 — ErrRefIntegrity (the new one was not written by a Put/is dead, the old one has no live reference) with a rollback — better to fail loudly than silently go negative; `SwapRefTx` — a primitive for the caller's transaction (T-058 writes subdomain_results and swaps references with ONE transaction — separate ones are dangerous in both directions), the `(*PackStore).SwapRef` method over the same pool (RefDB = db.DB + Begin, a runtime check — OpenPackStore accepts db.DB); writing subdomain_results is not implemented — T-058 territory, pinned in a doc comment; unit tests on a fake pgx pool with transactions and live refcount semantics (an overlay + a rollback): exact SQL/parameters/statement order, a single transaction, all scenarios, integrity (including a repeated swap of a pair — a double removal is rejected), Begin/statement/Commit failures, ctx cancellation, the PackStore method (a pool without Begin is rejected); docs/content.md — the "Reference counter" item. Spec: §5.7, §8.
- **T-045** [done] — Background pack compaction (new internal/content/compact.go + compact_test.go; targeted pack.go edits, justified in the code and docs): CompactScan — a pack analysis: the contents rows (pack_id, refcount) + a walk of the file over length prefixes (the T-042 format) → live/dead/garbage bytes and blocks, the live share = LiveBytes/DataBytes (an unfinished tail — garbage bytes, not a block; a row past a whole record — a corruption error, the pack is not compacted); CompactPack — editing one pack per §3.4: live blocks are rewritten into the NEWEST pack by the regular appendBlock path (the old file is untouched, the blocks verbatim — len/raw_len unchanged), transactions in batches of 1000 rows move the placements (UPDATE … WHERE pack_id = N AND refcount > 0) and delete dead rows (DELETE … refcount = 0 — together with the blocks, the T-041/T-042 contract), then the file deletion after a control check (0 live rows, the same size; unlink under mu+flock); crash windows are safe: before the commit the old pack is the source of truth (the copies are garbage), after the commit a garbage file has a live share of 0, the next pass will delete it; concurrency: only packs except the newest are compacted — writers moved to the strict invariant "an append always goes to the newest pack" (syncing the active pack with the registry under flock, the registry cache keyed by the directory mtime: creating/deleting packs changes mtime, an append does not) and hold flock until the metadata row is committed (Put → appendAndMeta: the block and the row are atomic with respect to compaction) → the set of rows of a non-newest pack is frozen, the control before deletion — a safety net; a consequence: writers and compaction must be the same software version (a rolling deploy mixing semantics is not supported — pinned in docs); the background CompactLoop/CompactOnce cycle (a ticker, the first pass immediately, a pack error is collected into the pass error — Warn and continue, ctx cancellation — a clean nil, the RecheckDaily/CleanupDeleted style); the CompactOptions.Threshold threshold — strictly below 0.5 by default (§5.7), the interval DefaultCompactInterval = 1 h (§13.2 — the pace at the developers' discretion, not exposed in the config — like T-020); max_store_bytes/the used space: there is no counter — the sum of the pack sizes, honestly decreasing after deletion (CompactResult.FreedBytes / CompactStats.FreedBytes; the proactive threshold — T-046); tests on a fake pool with transactions + real pack files in t.TempDir: the scan (live/dead/a garbage block/an unfinished tail/a row corruption), the threshold 49 % — yes / 51 % — no, the rewriting (the offsets and length prefixes in the metadata are correct, Get, dead rows deleted, writing after compaction), crash windows (Begin/Exec/Commit + "after the commit before the unlink" — the state recreated, picked up by a retry), a concurrent writer into the newest pack does not interfere (a -race run is clean), a "lagging" writer always syncs with the newest (before and after deleting the old pack), the loop (a DB failure of the first pass — a retry on a tick, cancellation — nil), the SQL contract of the four statements; documentation — docs/content.md (the "Compaction" section, the "Concurrent writing" section clarified). Spec: §5.7, §13.2, §3.4, §8.
- **T-046** [done] — Storage tests + the reaction to disk overflow (spec §5.7, §8, §11.4 item 3). Integration tests against a live DB (a new `test/integration/content_test.go`, the neighbors' style: skipUnlessIntegration, an isolated migratedSchema schema with contents from migration 000001, pack files in t.TempDir): the T-041 `StoreContract` on PackStore with live PostgreSQL/files (for this the suite was moved from store_test.go into a regular file `store_contract.go` — an external package cannot import from a _test file; the reference run on storeFake stayed), a round-trip with a dictionary (TrainDict on parking pages, a block with a dictionary ≤ half the raw bytes) and without one (len<raw_len on compressible content; for small pages a zstd frame may be bigger than the raw bytes — the comparison only covers the compressible case, a defect of our own draft test fixed), reopening the directory by a second "process"; dedup (Created=false, refcount grows in the DB, the block is not rewritten), Delete × refcount (the last reference → a dead row, Get → ErrNotFound, a resurrection by a fresh block); SwapRef scenarios of result rewriting (a content change, the first content 0→X, a result without content X→0, a no-op old==new, ErrRefIntegrity with a rollback); rotation with a small pack_size (≥3 packs, reading from all of them, writing into the newest); compaction (a live share <50% → CompactOnce rewrites and deletes the pack, FreedBytes>0, Get of the live rows works after the move, dead rows cleaned, writing continues; ≥50% — untouched); the max_store_bytes limit (above → ErrTooLarge without a DB row, exactly at the limit — passes; the unit coverage of a small limit — pack_test T-042). Disk overflow (new internal/content/disk.go + disk_test.go): free-space control — statfs of the content directory (the SpaceChecker interface, OSStatfs on Bavail/Blocks — available to an unprivileged service) before writing a new Put block and periodically; the `content.disk_free_percent` threshold (a new field of the content section in the T-006 style: env CRAWLER_CONTENT_DISK_FREE_PERCENT, default 5, validation (0,95]; the spec sets no number — a share of free space was chosen, %); below the threshold — Put → ErrStoreFull (a content write pause: no block and no row appear), dedup references and reads keep working — no bytes are spent; recovery is automatic after freeing (there is no separate used-space counter — free space is taken from live statfs, so rotation/compaction are honestly reflected, the T-045 note "the proactive threshold — T-046" removed); the alert signal — a typed StoreFullEvent event (the put/enospc/watch sources, a space snapshot, Recovered) into the PackOptions.OnStoreFull hook (a counter for T-080, the alert itself not implemented) + Warn/Info logs of `DiskWatchLoop` (a periodic check, DefaultDiskWatchInterval 30 s, the first pass immediately, an edge-triggered signal on transitions, ctx cancellation — nil per the OnRun contract); ENOSPC remains the last line of defense and also yields an event. Unit tests: a mockable statfs (fakeSpace), the threshold/pause/dedup-during-pause/read-during-pause, recovery, disabling with zero, a statfs error ≠ ErrStoreFull, Total=0, the loop (transitions, Warn/Info, cancellation), without a hook. Documentation: docs/content.md (the "Limits and disk overflow" section, "Errors" and "Used space" updated), docs/configuration.md and config.example.yaml (disk_free_percent). Checks: make test, make lint, make fmt, -race (content), make test-integration — green. Spec: §5.7, §8, §11.4 (item 3).

## Stage 5. HTTP stage

- **T-047** [done] — The `internal/httpfetch` package (transport.go + resolve.go + tests): a transport over net/http with connecting to a pre-resolved IP (spec 5.5 item 2) — the DNS stage addresses are put into the Fetch request ctx and verified against the host in DialContext (a mismatch = a redirect to another host), the Host header and SNI stay by the URL host (the transport sets ServerName: connecting to an IP does not change the target address); with several addresses — a happy-eyeballs-like race with a 300 ms step (RFC 8305, like the net.Dialer FallbackDelay), generalized over the whole list: the first connection wins, the losing attempts are cancelled and closed, and when all started ones fail the next address starts immediately (a sequential fallback — a special case); against strict sequencing — a silent address would wait out the whole 5 s connect, a domain with 4 A records would spend 20 s of the total budget; timeouts layer by layer from the §5.5 limits table: connect 5 s (per dial attempt), TLS handshake 5 s (Transport.TLSHandshakeTimeout; the new field http.tls_handshake_timeout — missing from the minimal spec 5.10 set, present in the limits table; env CRAWLER_HTTP_TLS_HANDSHAKE_TIMEOUT, default/validation/example/reference), headers 10 s (ResponseHeaderTimeout), total 20 s (the request ctx, alive until the Body is closed — the cancelBody wrapper); keep-alive disabled (§5.5: one page per site, a pool by "host" would survive an address change on re-resolution; a short idle — T-049); redirect-host resolution — the Resolver interface (LookupNetIP) + the ResolverFunc adapter + the SystemResolver default (net.DefaultResolver): a query without DNS-stage addresses (an http.Client over the RoundTripper interface or a manual walk of the T-048 chain) resolves "via the same resolver" (the dnsclient adapter — the T-053 worker wiring), Location IP literals — without a resolver; the cache — a separate CachingResolver decorator layer (a TTL of 60 s, a limit of 4096 entries; on overflow the expired ones are evicted first, then an arbitrary victim — addresses are cheap, LRU is overkill; errors are not cached; the parameters are not exposed in the config — they are absent from the limits table), separate from the transport so that T-048 resolves meta refresh hosts with the same one (the content level, bypassing the dial); FetchResult is minimal (status/line/headers/Body/FinalURL/Elapsed/TLS state — T-048…T-051 will extend it), the body limit — Options.MaxBodyBytes for T-049; IsTimeout for the consumer's temp_error classification; ctx cancellation everywhere, no leaks (the caller closes the Body, cancelBody extinguishes the total timeout, the attempt goroutines finish by the derived ctx cancellation); unit tests: the dialFunc seam (the address race, an instant fallback, a connect timeout, boundaries), httptest on 127.0.0.1 (a basic GET, Host+SNI by the pre-resolved IP via ClientHelloInfo, a resolve hook with the cache, an IP literal, header and body tar pits, ctx cancellation, a connection refusal on a closed port), a TLS tar pit on a raw listener (a handshake with no answer), the cache on injected clocks (hit/miss/TTL/eviction/non-cacheable errors/concurrency), a run with -race clean. Spec: §5.5 (items 1–2, the limits table), §8.
- **T-048** [done] — Redirect handling (new `internal/httpfetch/redirect.go` + `redirect_test.go`; transport.go/resolve.go untouched — the f.lookup seam already existed): `FetchChain(ctx, transport, the start request)` — walking the chain to the final page "at the end of the chain" (spec 5.5 item 4) with the limit `DefaultMaxRedirects`=8 (`FetchChain` for empty options; an explicit value — `FetchChainOpts`+`ChainOptions.MaxRedirects` with the config semantics of `http.max_redirects` T-006: 0 — do not follow, the outcome is the first response); transitions: HTTP 301/302/303/307/308 with a Location (303→GET — all chain requests are GET anyway; 300/304 and a Location without a value — not transitions; an absolute/relative/protocol-relative Location — `base.ResolveReference`) and `<meta http-equiv="refresh">` of 200 HTML responses (a Content-Type with "html"; a minimal string parser without dependencies: case, quotes `'…'/"…"/bare values`, attribute order, a self-closing tag, the "metadata" trap; the content "N; url=X": 0 — an immediate transition, N>0 — the same final address without waiting; without url — a refresh of itself, ignored; forms without a number/with a broken number are invalid per HTML — ignored; a streaming scan with overlapping chunks, a limit of 256 KiB and a stop at `</head` — HTML 4.2.5, §9 finite parsers; the read prefix is returned in the outcome body by the redirectBody wrapper — the round-trip loses no bytes); HTTP+meta count against ONE max_redirects and one cycle detection (the key — a normalized URL without the fragment, the query counts: counter `?i=1,2,…` — not a cycle, an exact repetition — a cycle, parametric "infinite" ones are stopped by the limit); the excess and a cycle — the unified `ErrTooManyRedirects` classification (errors.Is), NOT temp_error: a permanent site property (spec 6.2 temp_error — SERVFAIL/timeouts), per §8 the error code is recorded, the final domain status is decided by the T-058 aggregator; Location/meta refresh hosts — `idn.HostToASCII`→punycode+lowercase before resolving and requesting (§5.2 item 8; IPv6 literals as is), broken punycode/a foreign scheme/a broken URL — the transition is impossible, the outcome is the response itself (a browser without the transition); the DNS stage addresses apply only to the start host (its repetition in the chain reuses the addresses, http↔https — a regular step), new hosts — `f.lookup`→the T-047 Resolver/CachingResolver (spec 5.5 item 2); a step transport error — as is (IsTimeout→temp_error on the consumer's side); the outcome: `ChainResult{Result *FetchResult (a reader body, the T-049 limits), Chain []RedirectStep{URL, Status}}` — the format of the `subdomain_results.redirects` record (spec 5.5 item 7), on error Result nil, Chain — the completed part; the config unchanged (`http.max_redirects` exists since T-006, the default 8 = the limits table); unit tests on httptest: a 3-step chain, all the codes+the GET method after 303, a relative/protocol-relative Location, the cycles A→B→A/self/query-based, the limit explicit (requests = transitions+1)/default 8/0-and-negative, meta refresh (15 content cases, CT types, 404/3xx priority, chunk boundaries of 35 KiB, the round-trip of the full body, meta-to-itself), a punycode host in Location (the resolver and Host see ASCII), http→https same-host without resolving, another host via the resolver, broken Locations (URL/scheme/punycode), ctx cancellation in the middle of the chain; identifiers with the redirect* prefix, collisions checked with grep. Spec: §5.5 (items 1, 4), §5.2 (item 8), §8, §5.5 item 7.
- **T-049** [done] — The limits and protections of `internal/httpfetch` (limits.go + tests): the body cap MaxBodyBytes 2 MiB — BodyLimiter in FetchResult.Body: exceeding is not a network error, a truncation + the `BodyLimiter.Truncated` sign (a trial byte after the limit is exhausted decides whether the body is longer; an immediate EOF — exactly at the limit; works with both Content-Length and chunked; the decision on data partiality belongs to the T-058 consumer, spec §8); the anti-tar-pit "a minimum speed of 2 KB/s after the first byte" — a time.AfterFunc watchdog without goroutines: before each read the deadline for reading the remainder is extended by `now + remain/speed` (the invariant: the average speed to the end ≥ the minimum), the window floor 500 ms (scheduler jitter, the RTT of the last segment); a violation — cancelling the request context (the connection closes, a blocked read returns) and `limitBodySpeedError` with net.Error.Timeout=true → the IsTimeout time classification (temp_error, not "no site"); connections per IP 3–4 — a limitIPMap semaphore map by the dialed IP (the DNS stage/redirect resolution address, not the URL host): an entry is created on demand, deleted at zero interested parties (slot holders and waiters), a channel FIFO queue with ctx cancellation; the global limits — a connections semaphore (= http.concurrency) + an RPS token bucket (like dnsclient/limits.go, the same style: an average rate + a burst up to burst = concurrency), the token is taken in Fetch and RoundTrip (the http.Client redirect walks — the same requests), the connection slots — in dialLimited before the connect timeout starts (queue waiting is not part of establishing the connection), released by closing the connection (limitConn); the integration via `Options.Limits *LimitOptions` — nil leaves the transport without limits (the isolated T-047 layer tests keep their behavior), NewOptions always enables the limits from the http section; keep-alive stays disabled (DisableKeepAlives, the choice of "disabled" from the spec 5.5 "disabled or a very short idle" is pinned: one page per site, a pool by host would survive a re-resolution and hold thousands of connections at concurrency 2000; IdleConnTimeout has no effect with DisableKeepAlives — not configured); new config fields: http.min_body_speed (2048 bytes/s, env CRAWLER_HTTP_MIN_BODY_SPEED), http.max_rps (1000, env CRAWLER_HTTP_MAX_RPS; the spec sets no numbers — the order of the stage's natural pace, the per-IP tuning — spec 10.2), defaults/validation/example/reference; unit and httptest tests: truncation (exactly/over/chunked), a body tar pit (a speed violation after the first byte, the time classification), the rate without false positives, the per-IP queue (a third request waits, passes after freeing), cancellation in the queue, the global semaphore, the RPS rate in RoundTrip, keep-alive (two requests — two connections, Connection: close), a Reader after Close, the behavior without limits. Spec: §5.5 (the limits table), §8.
- **T-050** [done] — TLS (tlsinfo.go + tests): certificate parsing and self-verification with InsecureSkipVerify=true (spec 5.5 item 3) — NewTLSInfo(tls.ConnectionState, host) over PeerCertificates (the leaf is first, several server certificates/mTLS are not distinguished); TLSInfo: valid + typed TLSProblem reasons (expired/not_yet_valid/hostname_mismatch/self_signed, string codes for the jsonb subdomain_results.tls — spec 6.1), the issuer/subject CN+O, the serial number hex, NotBefore/NotAfter ("days left" is computed by the consumer — time derivatives are not stored), SelfSigned, SAN DNS+IP (netip), the chain (the length, the RFC 2253 subjects); the verification mechanics: the validity window on mockable clocks (newTLSInfoAt — the expired/not_yet_valid tests), the host (SNI/Host) in the SANs via VerifyHostname (wildcards, IP literals) with a CN fallback for certificates without a dNSName SAN — modern crypto/x509 ignores the CN, while the spec requires SAN/CN (RFC 6125 item 6.4.4), self-signedness — leaf.CheckSignature over its own signature (CheckSignatureFrom requires IsCA on the parent and would reject the leaf before the signature check); the trust chain to the system roots is deliberately not verified: the result does not depend on the container's root store, an incorrect certificate does not block the check (§8), the chain is stored informationally; the integration — FetchResult.TLSInfo is filled by Fetch (the URL host is known only there; a targeted transport.go edit: the field + the NewTLSInfo call), RoundTrip users (an http.Client over a Fetcher, the T-048 chains) assemble it themselves from resp.TLS and the final URL host; http/without certificates — nil, not an error (§8); IsTLSCertError — the classification of handshake errors "certificate/protocol vs network" only by typed errors (tls.CertificateVerificationError, x509.HostnameError/CertificateInvalidError/UnknownAuthorityError/ConstraintViolationError/SystemRootsError, tls.RecordHeaderError, tls.AlertError; the network — IsTimeout and the rest), without string matching; snake_case JSON tags for the jsonb; the config not extended — InsecureSkipVerify is unconditional per the spec, not "configurable"; unit tests: mockable clocks (before/inside/after the window), SAN match/mismatch (a wildcard, an IP literal, the CN fallback), self-signedness, the fields+chain+JSON, an end-to-end Fetch on httptest TLS servers with certificates of our own mini-CA (crypto/x509.CreateCertificate, ed25519): self-signed (invalid + self_signed), a leaf+CA chain (valid, the issuer, the length 2, a host mismatch), http (empty), IsTLSCertError by a table; -race clean. Spec: §5.5 (item 3), §8.
- **T-051** [done] — Charsets and compression (new internal/httpfetch/charset.go + charset_test.go; targeted transport.go edits — the RoundTrip hook): `ToUTF8(Header, body) → CharsetResult{UTF8, Charset, Source}` with the spec 5.5 priority: a BOM (the UTF-8 BOM we strip ourselves — the x/text decoder does not remove it; UTF-16LE/BE — the decoder with UseBOM; UTF-32 is not detected, like WHATWG/browsers) → the Content-Type charset (mime.ParseMediaType + htmlindex: label aliases and case) → a meta charset (the x/net/html/charset prescan over the first 1024 bytes: both `<meta charset>` and `<meta http-equiv>` kinds, the utf-16→utf-8 normalization; a battle-tested tokenizer instead of our own parsing) → detection: valid UTF-8 over the whole body and a frequency estimate of Cyrillic (the sum of letter frequency logarithms over the koi8-r/windows-1251/iso-8859-5 candidates, thresholds of ≥32 letters and an average geometric frequency ≥0.02 — x/net has no content heuristic, its 1252 default would give mojibake; it also saves a mixed-up `meta charset="windows-1252"` over a 1251 body) → the WHATWG windows-1252 default (a superset of latin-1); an unknown header label — a "best effort" (a pass to meta/detection); invalid sequences — U+FFFD, not an error; the conversion to UTF-8 — a consumer call over the already read body (the dedup hash and storage buffer it anyway, a streaming reader replacement saves no double reading); compression: `Accept-Encoding: gzip, br` is set in RoundTrip on a clone of the request (the RoundTrip contract; the http.Client/T-048 path gets the same, a foreign value is not overwritten), Fetch goes through its own RoundTrip; an explicit header disables the transparent net/http decompression (without a cap); decompression by Content-Encoding (gzip/br/identity; unknown or compound — an error, not compressed bytes as the body) with a cap = MaxBodyBytes from the limits table (the decompressed stream is the body — otherwise reading to the end would inflate a bomb with gigabytes of decompression; if unset — defaultDecompressLimit 2 MB, the name does not argue with the T-049 constant), exceeding — ErrDecompressTooLarge, exactly-at-cap — an honest EOF (a trial byte beyond the limit); the read chain: the network → gunzip/brotli (the cap) → the T-049 body limit → the consumer; Content-Encoding/Content-Length after decompression are removed (the body in hand is decompressed, like net/http's transparent decompression), identity — the headers are truthful and stay; brotli — a deliberate new dependency github.com/andybalholm/brotli v1.0.4: spec 5.5 explicitly requires br, it is absent from the stdlib, this is the reference Go port without transitive dependencies (it was already in go.sum via the golang-migrate graph); tests: the priorities of all steps, label aliases, BOM UTF-8/16LE/16BE, koi8-r/cp1251 without hints, saving a mixed-up meta, U+FFFD replacement, an empty body, gzip/br/identity via an end-to-end Fetch, a bomb (ErrDecompressTooLarge, bytes up to the cap are readable), exactly-at-cap, closing the source, the gzip+windows-1251+ToUTF8 chain, a foreign Accept-Encoding is not overwritten. Spec: §5.5.
- **T-052** [done] — An honest `User-Agent` with contacts from the config and the "minimum requests" contract: the `http.user_agent` default moved to `config.DefaultUserAgent` = `DomainCrawler/1.0 (+https://example.example/contact; admin@example.example)` — the accepted "name/version (+URL; e-mail)" form, stub contacts on a reserved domain/address (RFC 2606, the .example zone): real owners are substituted by the operator via config/YAML/env (the default merely keeps the service from being anonymous), the config example is synchronized; the field validation extended (non-emptiness since T-006): ≤512 bytes and no newlines/control characters (the value goes into the header as one field, `\r\n` — header injection); `httpfetch.NewOptions` substitutes the default when the field is empty — the production transport always identifies itself (`Options.UserAgent` empty remains a loophole only of the isolated layer tests, the Go UA); the UA installation point — Fetch (T-047), all the T-048 redirect chain walks also go through it (FetchChain calls Fetch) — pinned by a test; the "only the front page /" convention is pinned by package doc comments and `FetchRequest.URL`: the URL is built from scheme+host without a path or with "/" (equivalent, RequestURI gives "/"), `robots.txt` is not requested by code (grep — only doc comments), other paths in the chain are set by the server (Location/meta refresh — its initiative, §9, not the crawler's); "one request per (subdomain, variant) without repeats" — the T-053 worker's final-URL cache (documented, not implemented here), the transport itself does not retry; tests `internal/httpfetch/useragent_test.go`: the UA from the config reaches the server (custom and default), the NewOptions fallback, exactly one GET "/" per Fetch per the full httptest request log (host-only and an explicit "/" — the same request, no robots), the UA and paths on both walks of the redirect chain, validation (empty/`\n`/`\x00`/513 bytes — an error, 512 — the boundary is acceptable); docs/configuration.md (user_agent: the §9 ethics, where to specify contacts, value bounds; a paragraph on the minimum of requests of the http section), config.example.yaml; transport.go — only doc comments (the package/Options/UserAgent/FetchRequest.URL) and the UA fallback in NewOptions, the transport behavior unchanged. Spec: §5.5 (items 4–5 of the list after the limits table), §9.
- **T-053** [done] — http-worker (new `internal/httpworker/worker.go` + `worker_test.go`; assembling `cmd/http-worker/main.go` — not a stub): a q:http task — a whole domain (T-024): each subdomain × (https, http) — a walk by the `httpfetch.FetchChain` chain (T-048) through the FetchFunc seam (production `ChainFetch`, tests — a scenario without a network), in parallel within the task by the DefaultPerDomain=8 pool (subdomains ≤ 63 live on the same IPs — the real connection parallelism is already limited by the per-IP semaphore of 3–4 from T-049, a bigger pool merely accumulates waiting goroutines; 8 = twice the per-IP cap for domains with several addresses); the shared "final URL → result" cache per task (§5.5 item 6): the outcome of each check is stored, the start URL of each next one is checked — bare→www does not download the page twice; the key is scheme+host+path, the query/port/fragment are excluded (consistent with the T-054 variant criterion — counter ?i=N does not break the dedup; the scheme is included — substituting an http check with an https outcome would fabricate protocol availability); a cache outcome is marked Cached, ResponseMS=0, the chain synthetic; the limitation pinned: dedup inside the chain (an http→https redirect to an already downloaded URL) is not caught — it would require a hook in FetchChain (httpfetch closed for edits, in parallel with T-059), the saving is exactly the start matches; the body is read with a cap (the T-049 BodyLimiter + the worker's safety limit), UTF-8 (T-051), from it — the content.HashOf hash (the T-054/T-055 criteria, storage T-056), the T-057 SPA verdict, the headers+cookies of the final response; bytes in memory only within processing a check; the result — DomainResult{SubdomainResult{both SchemeChecks, the Record after DecideVariants/DetectAliases}}: the record = the subdomain_results columns (variant/scheme/final URL/status/headers/cookies/TLS/chain/hash/time/aliases/StoreContent); errors are classified ErrorClass (timeout/redirects/tls/body (truncation with Available=true)/network, IsTemp for T-058) and do not fail the domain, all silent → HasRecords=false+AnyTimeout (the no_http/temp_error status will be assigned by T-058); ctx cancellation — a task error (there will be no false "no HTTP"); the Run loop: a read window of 16, domain tasks up to DefaultConcurrency=64 (the global connection/RPS cap — the T-049 transport, 64×8 ≈ a third of the http.concurrency budget, the result memory — a few MB), each delivery — Processing (XACK after the full cycle; no DB write until T-058 — the Options.OnResult hook is called BEFORE the XACK, T-058 will insert the write and publication there without editing the loop), a processing error — no XACK (at-least-once §3.4); cmd/http-worker: config → pool (OnDBPool, held by the assembly for T-058) → dnsclient+LimitResolver → the NewDNSLookup adapter (redirect hosts "via the same resolver", A+AAAA per dns.ipv6, IP literals without an exchange) → the CachingResolver → the Fetcher NewOptions (the T-049 limits, the T-052 UA) + InsecureSkipVerify (the T-050 TLSInfo); unit tests: an end-to-end task of 3 subdomains (variants/aliases/cache — download counts by hosts), the cache key, query+per-task, errors classified and not failing the domain, all-errors→no records, SPA, cookies/charset, ctx cancellation, task validation, a Run loop smoke test (OnResult before Ack), an OnResult error — no XACK, the DNS adapter (7 subtests), -race clean. Spec: §5.5 (items 1, 6), §4.2, §7.
- **T-054** [done] — The http/https variant logic (new `internal/httpfetch/variant.go` + `variant_test.go`; transport/redirect untouched): the pure `DecideVariants(outcome, outcome) ([]VariantRecord, error)` — both unavailable → 0 records (the no_http status is decided by the T-058 aggregator per §7); one available → one record `variant=0` with the scheme of its check; both available → `VariantSameSite` (the OR criterion §5.5 item 5: the same final host+path OR both hashes non-nil and equal) → one record `variant=0` `scheme=https` (https canonical: the spec explicitly "variant = 0 (https)"; the record scheme = the data source — variant 0 takes the https check data), otherwise two: `variant=0` (https), `variant=1` (http), the order deterministic; the input `VariantInput{Scheme, Available, Host, Path, *content.Hash}` is generalized from FetchResult (the wiring — T-053; SameSite/the input are reused by the T-055 aliases and the T-059 corpus), `NewVariantInput` pins the final URL representation in one place: the host without a port, the path without query/fragment (the query outside the criterion — the spec names "host and path", counter `?i=1` does not split a site, a content difference at the same path is caught by the hash), the host compared case-insensitively (punycode is already lowercase — T-048; EqualFold — a safety net), the path — byte-exact in the decoded form; a nil hash (content not stored — the T-056 limit/an error) degenerates the criterion into host+path (two nils are not enough), the hash of an empty body — a valid value; the arguments are distinguished by the Scheme field (any order, a non-http+https pair — an error, nil records); unit tests per §11.1/§11.4 item 2 and the T-059 list: http→https one site, another host → two variants, different paths, parking by hash, nil hashes (both/one), empty content, one protocol (the check scheme ≠ the final URL scheme), host case, query/port/percent-encoding/nil-URL in NewVariantInput, argument order, pair validation. Spec: §5.5 (item 5), §7.
- **T-055** [done] — A pure alias-detection function (new internal/httpfetch/alias.go + alias_test.go): `DetectAliases(domain, []AliasInput) []AliasResult` — labels the subdomain records of one domain after the variant logic (spec 5.5 item 6); the input is generalized {subdomain_id, label, the final host+path, scheme, variant, content_hash (may be nil)} — no dependency on the T-054 types, the output parallels the input; the match criteria: the same normalized final host+path (lowercase, the FQDN dot trimmed, an empty path = "/"; query/fragment are not compared — counter ?i=N does not break the bare ≡ www alias) or the same non-nil content hash (parkings/stubs, spec 5.7); related records — an equivalence class (union-find, the transitivity a=b=c); the kinds per model.AliasKind (0 none / 1 identical / 2 redirect — pinned: 1 = the same final URL or hash at its OWN address, 2 = the record's final host equals the original subdomain's own address, the classic bare → www; verified against the testbed expectations ApexRedirect/WWWRedirect/WWWSame); the class original rule (who stores the content): a record "at home" — the final host equals its own address (label+domain), with several — the minimal (subdomain_id, variant); nobody "at home" (everyone moved to a common external URL) — the minimal (subdomain_id, variant) of the class; the minimal subdomain_id automatically prioritizes the apex (id 0), then www (id 1, migration 000001) and does not depend on the completion order of the parallel subdomain downloads (T-053); alias_subdomain_id — only to another subdomain (the variants of one subdomain cannot be aliases of each other, a transitive match leaves the record independent); records without a final URL (no page) are independent; StoreContent=true only for the original and only with a non-nil hash — the class content is stored once; wildcard subdomains do not participate (they do not reach the HTTP stage, T-038); unit tests: bare→www and www→bare, parking by hash at different URLs, the chains a=b=c (redirect- and hash-variants — one original), a nil hash → the URL criterion, no matches → kind 0, the original choice (WWWSame → the apex, independence from the input order, a common external URL), host/path normalization, degenerate cases (an empty input, a record without a final URL, the variants of one subdomain); the alias*/Alias* identifiers — no collisions with the parallel tasks (grep). Spec: §5.5 (item 6), §7, §11.1.
- **T-056** [done] — Storing content via the `ContentStore` with the `max_store_bytes` limit; `content_hash` (16 bytes) in `subdomain_results`; deduplication of parkings/stubs. Done (new `internal/httpworker/store.go` + `store_test.go`; targeted `worker.go` edits — `Record.Body`/`SchemeCheck.Body`/`workerPage.body` and releasing the bodies after labeling; the `cmd/http-worker` wiring): `ContentSaver.Save` is called by the assembly in `Options.OnResult` — inside task processing, before the XACK and before the future T-058 write (spec 4.2); the dedup is two-level per §8 — the task level (T-053/T-055: the final-URL cache + the alias classes, `Put` only for the `StoreContent` originals, aliases get no reference of their own — their `content_hash` is normalized to NULL, the class content is reachable via `alias_subdomain_id`) and the store level (T-041/T-042: the same hash of different domains — one block, `Created=false`, refcount++); the hash — one source (the worker and `Put` compute `HashOf` over the same UTF-8 bytes, the match is verified after every `Put`); ErrStoreFull (T-046) and ErrTooLarge — the content is not stored, the result is NOT failed (§8 "a content write pause"): a record with `content_hash=nil` + the `Record.ContentNotStored` sign (dedup references to live content go through even under overflow), a Warn log, a recheck (the T-030 recheck) will store it; a body truncated by `max_body_bytes` is not stored and not hashed for storage (a hash of truncated bytes would lie under dedup; the partiality — `Truncated`/ErrBody from T-049/T-053); other Put errors and ctx cancellation — a Save error → a delivery without XACK (at-least-once, a repeated Put adds an extra refcount without new bytes — a space leak in the safe direction, like SwapRef T-044); memory: the bodies are held until the end of the domain task (the checks phase — one instance per downloaded page, after labeling — only the `Record.Body` of originals ≤ 2 MiB × originals ≤ records ≤ 126), after `Put` the bytes are dropped; the wiring: `SetDictFile` (T-043) → `OpenPackStore` (content.dir/pack_size/max_store_bytes/disk_free_percent) → `DiskWatchLoop` as a goroutine → OnResult = Save → (T-058); unit tests on a fake ContentStore with live dedup semantics: only originals, dedup across domains (Created=false, refcount 2), ErrStoreFull (the result alive, Warn, a dedup reference goes through), ErrTooLarge, truncation (Put is not called), ctx cancellation/foreign errors/a foreign hash, the OnResult→store integration in Run; documentation — docs/content.md §"Writing from the http-worker", docs/services.md. Spec: §5.5, §5.7, §8.
- **T-057** [done] — The SPA shell heuristic: little visible text + root containers `#root`/`#app`/`#__next`/`#__nuxt`, a `<noscript>` demanding JS, a JS redirect, a `meta refresh` → publishing a task to `q:render`; otherwise the raw HTML is final. Done (new `internal/httpfetch/spa.go` + `spa_test.go`, no foreign files touched): the pure `DetectSPA(html []byte) SPAVerdict{SPA, Reasons []SPAReason, VisibleText}` — the verdict + all the triggered signs (publishing to q:render is the T-053 consumer's job; the URL context is not needed: the signs do not depend on the address, the transitions were already consumed by T-048); the signs: "little visible text" — `SPAVisibleTextMax`=300 visible bytes after a coarse removal of tags and the content of script/style/noscript/comments (the React/Next/Nuxt/Vue shells carry 0–~200 bytes — the text is in noscript and the bundles; live front pages — from hundreds; bytes, not runes: Cyrillic counts double — the conservative side; `<` of the threshold, exactly 300 — already a lot); a container — id=root/app/__next/__nuxt in any quotes and case at any tag (a mini attribute parser: `data-id`, `rootx`, spaces around "=" per HTML5 — handled); a noscript demanding JS — keywords ("javascript" is spelled the same in any language; enable/js required, including the Russian-language equivalents), case-insensitive including Cyrillic (bytes.EqualFold), a 4096-byte window; a JS redirect — a string scan of the whole document (catches inline onclick too): the word location (case-sensitive, like JS) with no name/URL-context byte before it (`?location=` — not a redirect) followed by `=` (not `==`) / `.replace(` / `.assign(` / `.href =` (not `==`, not pathname), without execution (§9); a meta refresh — http-equiv=refresh in the final HTML: T-048 took the transitions, what remains is "self to self" (including a loop cut short by detection) — a sign of the page's intent, caught over the whole document, not only the head; the decision table (§13.2 — the developers' discretion, pinned in the file's doc comment): a JS redirect and a meta refresh are sufficient alone (a trampoline/self-refresh — raw HTML is not the outcome), the pair "little text + a container" — the spec's wording, "little text + noscript" — for custom containers ("and the like"), little text/a container/noscript alone and with rich text (SSR) — not SPA; the motivation — the error cost asymmetry: a false positive — one render in the queue (T-066), a miss — empty content in the store until the recheck; a single linear pass without body copies, zero allocations on "quiet" pages (pinned by a test), the T-048 primitives reused (redirectTagEnd/redirectIndexFold/redirectIsSpace), no HTML parser introduced (byte markers, §9); unit tests: the CRA/Next/Nuxt/Vue shells + the spa-shell testbed fixture, the classics and SSR with a large SEO text → not SPA, the threshold 299/300/301, the id attribute forms, 14 JS-redirect forms (positives and negatives: ==/===/pathname/?location=/ /location//mylocations), meta refresh (self-to-self, a loop, case, in the body, name≠http-equiv), degenerate inputs (empty, whitespace, binary garbage, an unclosed script, a single "<"), visible-text counting, 9 rows of the decision table, allocations. Spec: §5.5 (item 8), §13.2.
- **T-058** [done] — Writing the HTTP episode result (new `internal/httpworker/record.go` + `record_test.go`; the `cmd/http-worker/main.go` wiring; targeted comment edits of worker.go/store.go): `Recorder.RecordAndPublish` in the `Options.OnResult` chain after ContentSaver (T-056) and before the XACK — the order "a DB write (one transaction) → publishing q:analyze → XACK" (§5.5 item 9, spec 4.2; the publication after the commit — Result points at the row, a publication failure → a delivery repeat will rewrite and re-publish, the duplicates are bounded by the delivery attempts); the transaction: UPDATE domains (the status alive with HasRecords / temp_error with AnyTimeout / no_http, §7; check_state=done — the domain is finished after DNS/HTTP, render/analyze are asynchronous; queued_at=NULL, last_checked_at=now(), temp_attempts CASE — the T-031 contract; the guard status<>deleted §8/§13.1, the UPDATE as the first step locks the row) → DELETE of all the domain's old rows (§3.5 — a rewrite, including vanished keys) with RETURNING content_hash → −1 refcount of every old hash by the SwapRefTx(old, zero) primitive of T-044 (the new hashes already have +1 from the T-056 Put — the accounting is balanced, a delivery repeat keeps the balance: a repeated Put +1 and a DELETE of the same row −1; going negative — ErrRefIntegrity with a rollback) → INSERT of the new rows with one unnest-batch statement (18 array columns, ≤126 rows; COPY rejected — the unnest-INSERT gives an exact SQL contract in unit tests at a batch ≤126); all the fields of §5.5 item 7: addrs — a packing of 16 octets/address, IPv4 v4-mapped (the format pinned in docs/schema.md), CNAME, the redirects chain jsonb, the final URL, TLS/headers/cookies jsonb (cookies — a snake_case structure with attributes), http_status, the response time, error by ErrorClass (NULL at ErrNone), alias_kind/alias_subdomain_id, rendered=false (T-067), rules_version=NULL (T-068+), checked_at=now() — one transaction_timestamp with last_checked_at; q:analyze — a Result message (T-024) per each subdomain×variant record (analysis over all records — render T-067 will replace the content and publish again; q:render — T-066/T-067, the boundary pinned); the redelivery idempotency — a rewrite without the check_state<>done guard (the choice against the T-039 filter: the "written, not published" window must be closed by a redelivery, and a race with a fresh recheck is practically unreachable — the delivery attempts exhaust into q:dead within minutes at a recheck interval of months); validation before the transaction (the model codes, the alias contract, PK duplicates, ContentNotStored⇒hash NULL); the service queue is wrapped in WatermarkPublisher (T-026, like dns-worker); unit tests on a fake pool (snapshot transactions, live semantics of three tables)/queue: the field completeness and the SQL contracts, the statuses (alive/no_http/temp_error, temp_attempts), the deleted guard (no rows/no publication), the rewrite+SwapRef (the old to 0, vanished keys deleted, the statement order), an idempotent repeat without going negative, transactionality (Begin/UPDATE/DELETE/INSERT/Commit failures, ErrRefIntegrity with a rollback of the old rows, ctx cancellation), the publication after the write (a failure — the result written, the delivery returns), a Run smoke test with the OnResult chain (the publication before the ack) and no XACK on a publication failure; `make test`/`make lint`/`make test-integration` green. Spec: §5.5 (items 7, 9), §6.1, §7, §8, §3.4.
- **T-059** [done] — An end-to-end corpus of unit tests for the variant and alias logic (a new `internal/httpfetch/variants_corpus_test.go`, the production code unchanged): the §11.4 item 2 acceptance cases — whole domain stories "subdomains × http/https checks → DecideVariants per each subdomain independently → DetectAliases over all records", the stage joining as in the T-053 worker (the record scheme = the data-source check, AliasInput assembled from the same VariantInput, the DecideVariants argument order alternates — check outcomes arrive in completion order); composition: `bare → www` (the apex final URL = the www address → an alias kind 2 to the www original), `www → bare` (mirrored, the original apex), `http → https` (both checks converge on one page, the counter query outside the criterion — one record variant 0 scheme https), `http leads to another site` (a foreign final host, :80 in the URL does not hurt — two records variant 0/1), parking by an equal hash at different URLs (kind 1, the original — the minimal subdomain_id), three-member chains — redirect- and hash-variants (one original, the transitivity a=b=c), nil hashes (for all — the URL criterion and content not stored even for the original; one nil — the hash branch unprovable, two variants at the apex), the independence of subdomain checks (the apex did not answer at all — no records, no_http is decided by T-058), a mixed story (the apex https converges on www — an alias, http goes to a parking — variant 1 outside the class), only one protocol works (one record each with its own check scheme, incl. scheme http); every record is run through the model validation (model.Variant/Scheme/AliasKind.Valid) and the alias contracts (alias_subdomain_id nil ⟷ kind 0, a reference only to ANOTHER subdomain, StoreContent only with a non-nil hash); helper names with the corpus prefix against collisions; no T-054/T-055 defects found — all corpus expectations matched the actual behavior on the first run. Spec: §11.1, §11.4 (item 2), §5.5 (items 5–6).
- **T-060** [done] — Integration tests of the HTTP stage on the testbed (a new `test/integration/http_test.go`, the component level against live testweb/mockserver; the raw TestHTTP_TestwebScenarios/MockserverEvil protocol from env_test.go is not duplicated — the expectations here are on the classification branches and the pipeline contracts). The httpfetch transport: a tar pit — the T-049 anti-tar-pit (mockserver 512 B/s against the tightened limits 8192 B/s and a cap of 4 KiB: a "tar pit" error with the IsTimeout time classification in ~1 s, the watchdog, not the total timeout, the connection closed by cancellation); a giant body — Content-Length 1 GiB and a real delivery beyond the limit: a truncation exactly at 2 MiB + BodyLimiter.Truncated, reading without a network error; a zip bomb — an infinite gzip of zeros → ErrDecompressTooLarge (T-051; no br scenario: testweb does not encode responses, mockserver — gzip only); the T-048 redirects — infinite → ErrTooManyRedirects "limit", a chain of a start+8 transitions; a ring → "cycle"; both reasons are NOT time-class (not temp_error); an incorrect certificate T-050 — the connect passes (InsecureSkipVerify), TLSInfo.Valid=false with the typed reasons self_signed / expired+self_signed / hostname_mismatch+self_signed (the order validity→host→self-signedness). The httpworker pipeline: the end-to-end minimum TestHTTPWorker_E2E (same-site — the only record variant 0 https, www without a TLS site — a record with its own check scheme; bare→www — alias_kind 2 to the www original, the content stored once, a round-trip from the store; the subdomain_results rows with all the fields; the domains alive/done, the lease reset, temp_attempts 0; the q:analyze publication — a Result per subdomain×variant record, decoding and key verification; a redelivery — rewriting the rows without duplicates and the contents refcount 1→2→1: the balance of the repeated Put (+1) and the DELETE of the old row (−1)); TestHTTPWorker_EvilScenarios — "evil" responses via ProcessDomain+ContentSaver+Recorder: a tar pit → ErrTimeout of both checks → the temp_error status (§7/§8), closed ports (a black hole, EHOSTUNREACH ~3 s) → no_http, a giant body → a 200/Truncated/ErrBody record with content_hash NULL and ContentNotStored (T-056 — truncated content is not stored and not hashed), a zip bomb → ErrBody without records, an infinite redirect → ErrRedirects without a temp class, an SPA shell #root+noscript → the low_text+root_container+noscript_js verdict and stored content, tls-* → records with TLSInfo and the reasons in jsonb. The testbed: mockserver — the scenario virtual hosts (the "/" routing by Host to mock_path from scenarios.yaml — the "evil" answer arrives at the worker's front page, which did not happen before; the direct paths kept) and SNI certificates for tls-self-signed/tls-expired/tls-wrong-san (the debug bad-tls names untouched); Dockerfile.go — a separate mockserver-build stage (a stdlib-only source, edits of the rest of the module do not invalidate the layer), make test-integration rebuilds the image (--build) — the source and the image do not diverge; isolation — migratedSchema + Redis DB 15 with q:* cleanup, the new tests' time ~5 s (the slow branches — classification expectations: a tar pit ~0.5 s, a black hole ~3 s); no production-code defects found. Spec: §11.2, §8.

## Stage 6. Render

- **T-061** [done] — The `internal/render` package (new browser.go/session.go/pool.go/errors.go, render.go rewritten from the framework stub; live tests browser_live_test.go behind CRAWLER_RENDER_TEST=1): one Chromium per worker (spec 5.6) — a remote connection by `render.remote_url` (`remote.NewAllocator`, the dev stack: the compose chromium service ws://172.28.100.14:9222) or its own process when the field is empty (`NewExecAllocator` + `--no-sandbox`, the production render image Dockerfile.render — the modes pinned by the config/spec 5.10 and compose); isolated contexts — CDP `Target.createBrowserContext` (chromedp `WithNewBrowserContext`): an incognito profile (its own cookies/cache/storages) + a tab per session; the state between pages is not preserved by recreation: the pool closes the session (`disposeBrowserContext`) after every task — dispose drops the storages atomically without enumerating origins (a Storage cleanup by origins does not cover the cache), cheaper than a page timeout (the isolation confirmed by a live test: a cookie does not survive a context change); the `NewPool` pool — up to `render.contexts` tasks in parallel (a slots semaphore), `Pool.Run(ctx, RenderTask{URL}) → (RenderOutput{FinalURL,Title}, error)` — the API for T-062 (blocking — the stage before the transition), T-063 (waiting — after), T-064 (DOM/cookies — the RenderOutput fields, access via `Session.Context()`); errors — `*render.Error` with the classes task (timeout/cancellation/net::ERR_*)/context (the tab died — one retry on a new context)/browser (unavailable — T-065), the classification by the context states and `LostConnection` without parsing texts + a live tab probe (chromedp does not cancel its context when it falls); the pages' User-Agent — the single `http.user_agent` (spec §9, Emulation.setUserAgentOverride); the `Browser.Pages`/`NeedsRestart` counter for restarts every N pages (T-065); two critical chromedp v0.21 rules pinned in the code after diagnostics on the live stack: the browser and the tab's event pump live as long as the context of the first call lives (a timeout child of the first Run drains the commands — that is why the first call goes on the context itself, the wait is bounded by an overtaking race with the session cancellation), and the task's actions context must be derived from the tab's context (the caller's cancellation — `context.AfterFunc`); `cmd/render-worker` — a stub until T-066/T-067; unit tests without a browser (the classification, the pool on fakes: retries, the slot limit, cancellation, Close, a -race stress; starting against an unreachable Chromium) + live (a pool of 4 tasks on 2 contexts, isolation, the UA); documentation — docs/render.md, services.md, configuration.md, README. Spec: §5.6, §9.
- **T-062** [done] — Blocking images, fonts and media in Chromium (new block.go/block_test.go/block_live_test.go, a targeted session.Run edit). Mechanics: intercepting the tab's requests with the CDP `Fetch` domain — `Network.setRequestInterception` was removed from the protocol, in chromedp v0.21 interception is the `fetch.RequestPaused` events (cdp.Events); `Session.Run` enables the blocking BEFORE the transition in the order "subscribe to events → `Fetch.enable` → `Navigate`" — an event arriving before the subscription would be left unanswered and would hold the request until the page timeout. `Fetch.enable` gets patterns only for the blockable types — `Image`/`Media`/`Font`, the `Request` stage: only what will be rejected is paused (`Fetch.failRequest` with `BlockedByClient` — the request does not leave the browser, outbound traffic is saved too), the other types (Document/Stylesheet/Script/XHR/Fetch/WebSocket/…) the browser does not pause at all: scripts and XHR must load (SPA shells finish drawing — render exists for JS's sake), and a listener failure cannot hang the page (fail-open: the blocking is a saving, not a correctness condition); continueRequest in the listener — a safety net against classification divergence. Favicon is classed as Image: one requested by the page (link rel=icon) arrives as the Image type and is blocked, the browser's /favicon.ico arrives as Other and passes — in headless-shell without UI there are no such requests (confirmed by a live test), a URL heuristic for one small file is not justified. The listener — a goroutine on the task's context (blockPump): the event iterator is fabricated on the listener's internal context, stop (defer in Run) cancels it and waits for the goroutine to exit — no leaks (verified with `-race`, unit and live); on a sudden close of the event channel (the tab died) the listener itself does `Fetch.disable` (fail-open, so that blockable requests do not hang), at the end of the task disable is done by stop (best-effort, without cancelling the context). No new options/RenderTask fields added: spec 5.6 makes the blocking unconditional for render-worker, the spec 5.10 render section has no field for it; enable errors are classified by the usual Run wrap (task/context/browser). Unit tests without a browser (a fake cdp.Session): the list of blocked/skipped types over the protocol's full ResourceType list (new/unknown types pass), the enable patterns, failRequest/continueRequest by types with RequestID, the pump lifecycle (stop/context cancellation/channel close/a response error does not kill the pump); live (CRAWLER_RENDER_TEST=1) — an HTTP server with counters inside the test listens on 0.0.0.0 (httptest is no good: 127.0.0.1 is unreachable from the chromium container), the page is given the address of the tools run container in crawler_net (the first non-loopback IPv4, no DNS needed): the image and the font were not requested, CSS/script/XHR were requested, favicon — 0 (diagnostics); the existing T-061 live tests unaffected (data: pages make no requests). Documentation — docs/render.md as its own section. Spec: §5.6.
- **T-063** [done] — Waiting for page readiness after the transition (new `internal/render/wait.go` + `wait_test.go` + `wait_live_test.go`; a targeted `session.go` edit — the stages in Run, no foreign files touched): the combined algorithm `Page.navigate` (starting the transition without waiting — chromedp.Navigate itself waits for the end of loading without a separate cap, a hanging page would eat the budget) → the load event `Page.loadEventFired` with a cap of 1/2 of the total timeout (soft: stuck on a subresource — we go on, DOMContentLoaded has usually happened already; for an SPA load comes before the finish drawing — the next phase waits for it) → a network "quiet": a 2 s window without NEW `Network.requestWillBeSent` (every request restarts the window; the loading-phase signals merge — the window is counted from entering the phase; Network.enable and the chromedp.Events subscriptions are set BEFORE the transition — buffering from the moment of subscription, no events lost) with a cap of 1/5 of the timeout (soft: a continuous analytics/polling background — the outcome is taken as is; the spec's "bounded wait" is bounded both below and above) → taking the outcome. The values: a 2 s window — the top of the plan's 1.5–2 s range (an SPA XHR chain ≈1 s, the cost is one-time per page), a constant of the site's request pace, but ≤ half of the phase cap — with `render.timeout` < 10 s the window compresses proportionally (DefaultWaitPlan), otherwise it would never be confirmed; the phase shares 1/2+1/5 leave ~30 % of the budget for taking the outcome; all contexts — derived from the tab (the T-061 lesson). The total timeout `render.timeout` (25 s default, verified by the T-006 config) — the wrapper of the whole visit; expiry — a ClassTask task error, NOT a partial result: §8 requires recording the error code on interruption by limits, there is no "partial" DOM in the §6.1/§5.6 schema (T-067 would replace the full raw HTML with an unfinished DOM without a marker), §7 — render is an asynchronous supplement, the raw HTML is already stored by the HTTP stage, a failure — a queue redelivery (T-066); no Timeout field added to RenderOutput. Extensibility kept: T-062 — the stage before the transition (before waitSubscribe), T-064 — next to Title/Location (commented in Run). Tests: unit on fake clocks/events (the plan and shares, load arrived/did not arrive, requests move the window, merging old signals, a cap shorter than the window, ctx cancellation in both phases, the total deadline; -race clean); live behind CRAWLER_RENDER_TEST=1 — a quiet data: page ends by the window (≈2 s), a "noisy" one (a fetch to TEST-NET every 300 ms) — by the quiet cap (≈4 s), a page with an eternal parser-blocking script — the total timeout with ClassTask/DeadlineExceeded and a page counter of 0; `make test`/`make lint` green. Spec: §5.6, §7, §8.
- **T-064** [done] — Obtaining the render result (new `internal/render/extract.go` + `extract_test.go` + `extract_live_test.go` behind CRAWLER_RENDER_TEST=1; targeted session.go/render.go edits). DOM — the `chromedp.Evaluate` expression `document.documentElement.outerHTML`: serializing the live tree after the JS finish-drawing by the same Chrome HTML serializer ("save the page") — the format is identical to raw HTML, the result goes to the store (T-067) and the HTML/meta rules (T-068) without transformations; the alternative DOM.getFlattenedDocument (a node tree with nodeId, would require a reverse per-node serialization via DOM.getOuterHTML) rejected. Cookies — CDP `Network.getCookies` with the final page's URL: reads the isolated context's store, not document.cookie — httpOnly and JS-set ones are visible (spec 5.6, §11.4 item 3); the `Network.getAllCookies` mentioned in the decomposition was removed from the current protocol (there is no such command in cdproto v0.157.8 — pinned in a doc comment); the `ExtractCookie` format repeats the HTTP stage's record (recordCookie, T-058: name/value/path/domain/expires RFC 3339 UTC/secure/http_only/same_site lowercase lax/strict/none) — the single subdomain_results.cookies contract, max_age is not filled (the browser store keeps only the absolute term), the order is deterministic (name, domain, path), for schemes without a cookie store (data:, about:) the step is skipped. The response headers of the main navigation (spec 5.8 — rules over headers, the HTTP stage sees only the raw response): the transition replaced with `chromedp.NavigateResponse` (the same load wait as Navigate; chromedp itself leads the loader/request chain through redirects, no event subscription needed), the headers converted to http.Header — a single contract with T-058; for data: navigations Chromium gives a pseudo-response (the Content-Type from the URL). The integration — the final stage of Session.Run: NavigateResponse → Title/Location → extractResult (DOM+cookies into RenderOutput{DOM, Cookies, ResponseHeaders}); FinalURL already existed (document.location, JS redirects visible). Unit tests: the CDP cookie conversion (terms/samesite/session), a golden-JSON contract with T-058, headers (canonicalization/numbers/merging), orchestration on fakes of extractCDP (a DOM string, an empty page, an evaluate error, a cookies error, data: without a cookies request); live: the exact outerHTML of a data: page after executing a script, the JS cookie js_session on spa-shell.crawl.test + the response headers; the earlier BrowserLive tests without regressions. Spec: §5.6, §11.4 (item 3), §5.8.
- **T-065** [done] — Chromium resource management: restart every N pages and by a memory threshold (new `internal/render/manager.go`+`proc.go`+`manager_test.go`+`proc_test.go`; edits of browser.go/pool.go/render.go and their tests; config, deploy, documentation). Manager — the "browser + pool" generations: `NewManager(ctx, opts)`/`Manager.Run(ctx, task)` — the task entry point (T-067 will call it instead of a direct Pool.Run), before a task acquire (a mutex) guarantees a live generation; the change triggers: the page threshold (`Browser.Pages ≥ render.restart_every_pages`, spec 5.6), the memory threshold (`Browser.RSS ≥ render.restart_memory_bytes`, spec 5.6/3.2), the browser's death (`Alive`/LostConnection) and the "pool already closed" safety net (the acquire→Run race). A generation change is instant for new tasks — Pool.retire synchronously (ErrClosed), finishing the pages of the old pool (each ≤ render.timeout, spec 3.4) and stopping its browser — a background goroutine, the new Chromium starts immediately: a memory restart does not wait for the "heavy" pages that ate the budget. A task with ErrClosed (it did not start in the pool being closed) or ClassBrowser (the browser died mid-task) — a generation change and a retry, at most 2 per task (a crash loop does not multiply page executions, beyond that — a queue redelivery); Close — graceful for the whole chain. RSS: CDP does not give the process memory — the total RSS of the process tree from /proc/<pid>/stat (the rss field × page size, a walk by PPid down to the leaves: the main + renderers + GPU; comm with brackets — parsed by the last ")"), the root — the PID chromedp gives explicitly (Browser.Process(), the method is declared for collecting process metrics); the remote mode: there is no process nearby → RSS=-1, the threshold does not trigger, the cap is held by the chromium container's mem_limit (pinned by a comment in compose). The default threshold 1536MiB = 75 % of the 2 GB budget (spec 3.2): a restart — before cgroup-OOM, a margin for growth between checks (once per task) and tmpfs /dev/shm (counted in cgroup memory, but not in process RSS). The new option render.restart_memory_bytes (ByteSize, default 1536MiB, 0 — disabled, range 256MiB..16GiB; env CRAWLER_RENDER_RESTART_MEMORY_BYTES; the T-006 style: envSpecs, config.example.yaml, docs/configuration.md, default/YAML/env/validation tests). An unprivileged user and container memory (§9/§10.1): the Dockerfile.render image under USER nobody (since T-061, the rationale pinned), Chromium with --no-sandbox in an unprivileged container; a new compose profile runtime (the render service, the crawler/render:local image per Dockerfile.render, the config deploy/docker/config.render.yaml with an empty remote_url — the production mode): mem_limit ${MEM_RENDER:-2048m} ≈ the spec 3.2 budget (the docker analog of the cgroup MemoryMax from spec 10.1 — OOM will not touch PostgreSQL), shm_size 1gb, init, cap_drop ALL, no-new-privileges; systemd units with MemoryMax/User= — stage 9 (spec 10.1). Unit tests: parseProcStat/treeRSS (brackets in comm, broken lines, cycles in ppid, foreign subtrees), the RSS of a live process and a fresh child, OverMemory (exactly the threshold/below/disabled/unknown), the manager — a single start, changes by pages/memory/death before a task, a retry after a death mid-task and into a closed pool (a deterministic race imitation), the restart limit (an error upward), a failed start (the next task tries again), Close (ErrClosed, finishing tasks in flight), a parallel stress (20 tasks at a threshold of 1 page — constant generation changes, all successful, the invariant starts = restarts+1); -race clean. Documentation: docs/render.md (the "Browser restart and the memory threshold" section, the error class table), docs/docker.md (the runtime profile, the 172.28.100.15 address), docs/configuration.md, config.example.yaml, .env.example (MEM_RENDER). Spec: §5.6, §3.2, §9, §10.1.
- **T-066** [done] — Render queue overflow (new `internal/httpworker/renderq.go` + `renderq_test.go`; targeted comment-call edits of record.go/worker.go and the `cmd/http-worker` wiring; docs/services.md): per spec §5.6, domains under q:render overflow get the `render_pending` flag and are finished later, render does not delay the other stages; §7 — the domain is done after DNS/HTTP, render supplements asynchronously. The regular path: the SPA records of the result (the T-057 verdicts in Record.SPA) are published to q:render by a T-024 Result message per subdomain×variant record (agreed with T-067: the consumer will replace the content by the key), after the result transaction is committed — like q:analyze (T-058), the message points at the row. Backpressure does NOT block: WatermarkPublisher (T-026) waits for capacity — the wait would delay the XACK of the q:http task (OnResult before the acknowledgment, spec 4.2), which is forbidden by §5.6; instead a non-blocking check of the stream length (XLEN via BackpressureGate.Len) BEFORE the transaction — free (len < watermark, "strictly below the threshold" of spec 4.2; the threshold is the shared scheduler.queue_high_watermark: spec 5.10 introduces no separate render threshold) → the publication after the commit through the "raw" queue bypassing the gate; an overflow or an XLEN error (conservatively — the flag saves tasks, a Redis unavailability would return the delivery to q:analyze anyway) → no publication, the domain gets the model.FlagRenderPending bit (T-008) by the renderSetPendingSQL statement in the SAME result transaction (§3.4: the decision is atomic with the rows; OR preserves is_idn) and finishes as usual — check_state=done, XACK without delay. The "publish q:render → clear the flag" order of the pass — at-least-once (§3.4, in the spirit of T-039): a failure after the publication leaves the flag — a retry will publish again (the duplicates are bounded and idempotent by key), the reverse order would lose the tasks forever. The background RequeueRenderPending pass (the recheck T-030/CleanupDeleted T-020 style: an initial drain + the ticker DefaultRenderRequeueInterval=1 min — like the T-020 intervals, not added to the config: the §5.10 set is fixed; batches DefaultRenderRequeueBatch=100 — the pass reads the records' content; with a full queue — one XLEN without touching the DB): SELECT a batch of domains with the flag (status<>deleted §13.1, the name label‖zone for Result.Name) → a repeated SPA verdict over the stored content (DetectSPA is deterministic over the same bytes — the verdict matches the record; records with content_hash are read from the ContentStore, aliases — hash NULL, alias_subdomain_id NOT NULL — inherit the original subdomain's verdict: the T-055 equivalence class is one; records without content and without an alias — T-056 — are skipped, the T-030 recheck will finish them; rendered=false and final_url NOT NULL — filters) → the capacity is checked before the batch and each domain → the publication → clearing the flag with a bit guard (RowsAffected 0 — a competitor already decided; races with redelivery are bounded by the T-025 attempts and self-heal via T-030); pass errors — Warn without stopping the loop, ctx cancellation — a clean nil; the wiring — a goroutine in OnRun of cmd/http-worker (the T-021 sample), the result writing — NewRenderRecorder (the NewRecorder signature of T-058 unchanged — its tests untouched; render=nil keeps the old behavior). Unit tests on our own fakes with the renderq prefix (record_test.go of T-058 is not edited — the task boundary): publication with a free queue after commit; an overflow/an XLEN error → the flag in domains by the same transaction (the event order UPDATE→setflag→commit), no q:render publications, the XACK not delayed (ok=true, nil), q:analyze published; a flag-statement failure — a transaction rollback; a q:render publication failure — an error with the result committed (replayed by delivery); no SPA — capacity not checked, a draft flag is not erased by the write (the pass clears it); NewRecorder without render — the T-058 behavior (backward compatibility); a deleted domain — no flag, no publications (§8/§13.1); the pass: publishing SPA+alias keys, excluding non-SPA/without content/rendered, clearing the flag preserving is_idn, a full queue at the start/between domains (Full, the flags kept, the DB not read), content unavailable — the record skipped without a pass error, the bit guard in a clearing race, selection/clearing errors, ctx cancellation, the loop (an initial drain, a clean exit), the SQL contract by literals. `COMPOSE_PROJECT_NAME=crawler make test`/`make lint` green (23 new tests, -race clean). Spec: §5.6, §7 (also §5.5 item 8, §3.4, §4.2, §13.1).
- **T-067** [done] — Replacing the raw HTML with the final DOM in the store, `rendered = true` in the result, publishing to `q:analyze` (a new `internal/renderworker/worker.go` package + `worker_test.go`; assembling `cmd/render-worker/main.go` from the stub; targeted comment edits of `internal/render/{render,pool}.go`; documentation docs/{services,render,content}.md, README): the worker consumes q:render (Result — the subdomain_results key, T-024; published by the HTTP stage T-066 and the renderq.go finishing pass) with the `render-worker` group, a window of 16, 4 deliveries in parallel (render is additionally limited by the render.contexts pool slots), each — the Processing pattern (T-025: XACK after the DB and the publication). Processing a delivery: SELECT the row's final_url+content_hash (the message carries only identifiers — spec 4.2; no row/final_url NULL — success without a render and a publication, XACK) → Pool.Run (T-061…T-064) → the T-064 contract safety net (an empty DOM/FinalURL — a task error, §8: there is no partial result) → ContentStore.Put(DOM) +1 (T-056/T-042, the T-058 order: the reference before the swap; the DOM is byte-equal to the stored one — Put and the swap are skipped, no extra +1) → ONE transaction (spec 3.4): UPDATE the row rendered=true (§5.6), content_hash=DOM, the render's final_url/cookies/headers (JS redirects are visible only to render — T-061; cookies — all, including httpOnly/JS, §11.4 item 3 — the single jsonb contract T-064/T-058; the final navigation's headers — spec 5.8), rules_version=NULL (the content changed — the analyzer's previous verdict does not reflect the new one, a repeated publication will re-analyze and set a fresh one, T-068+) + SwapRefTx(old, zero) −1 of the old reference (spec 5.7, T-044; a COMMIT failure/an error → a rollback, the +1 from Put — a leak in the safe direction) → the q:analyze publication with the same key AFTER the commit (§7; under WatermarkPublisher, T-026) → XACK. The redelivery idempotency (§3.4, the T-058 style): a rewrite without the rendered guard — a repeated render rewrites the content and the refcount; the race with a fresh recheck (the T-030 recheck rewrites the rows DELETE+INSERT) is closed by the optimistic UPDATE guard `content_hash IS NOT DISTINCT FROM $9` (NULL is correct — "no content" T-056): a changed row is not rewritten, 0 rows → the Put reference is removed by Delete (ErrNotFound — Warn, not an error), the delivery is acknowledged — the task was about the previous episode. Decisions per the spec: alias rows (content_hash NULL, T-055) are rendered by their own final URL and get their own reference to the DOM (the profiles are isolated, the equivalence class — the HTTP stage's optimization "do not download twice", not a ban on the record owning content; the render_pending flag is not touched — the http-worker T-066 is the owner, render changes only the row, §7 the domain is already done); Put errors: ErrStoreFull — temporary (a delivery retry will wait for space), ErrTooLarge (a DOM inflated by JS) — a permanent page property, the attempts will exhaust → q:dead (T-025) for the operator to investigate (a signal to raise content.max_store_bytes); an unavailable Chromium (the browser.Alive probe in Options.Alive) — a consumption pause by the loop without reading the queue (the deliveries stay unread in the stream, not in the PEL — without wired claims the PEL would hang for a long time), the restart — T-065; a browser start error — an OnRun error (exit code 1, Restart=on-failure); a q:analyze publication failure with the row committed — (written=true, err) without XACK, a redelivery replays it idempotently (at-least-once); a poisonous record — no XACK → q:dead (T-025); the cmd/render-worker wiring: StartBrowser (render.NewOptions from the config) → NewPool → the same content store as http-worker (SetDictFile → OpenPackStore → DiskWatchLoop as a goroutine, T-043/T-046) → WatermarkPublisher over RedisQueue (consumption/XACK transparent) → renderworker.New/Run; graceful shutdown — Pool.Close finishes the pages (each ≤ render.timeout) and redeliveries (§3.4). Unit tests on fakes of the pgx pool (snapshot transactions, live semantics of SELECT/the UPDATE guard/contents-refcount), the store (Put/Delete are tied to the pool's contents table — the reference balance end to end), render and the queue: the happy path (the order render→Put→begin→update→swap→commit→publish, the UPDATE field completeness, rendered=true, the refcount raw−1/DOM+1, q:analyze with the same key after Commit), the SQL contracts by literals, no row/no final_url (render does not start), the same content (without Put and the swap), an alias (without the swap, its own reference), the recheck race (0 rows, the Delete of the reference, the foreign content intact, no publication/Commit), a redelivery (the balance without leaks, 2 publications), render errors (no Put/Begin), an empty output, ErrTooLarge (no transaction), UPDATE/Commit errors (a rollback, the row unchanged, the Put +1 — a leak in the safe direction), ErrRefIntegrity (a rollback), a publication failure (written=true), a SELECT error, nil validation of New/Run, a Run smoke test (2 XACK after the publications, a poisonous record without XACK, a clean exit on cancellation), a dead browser (Consume not called — a busy-loop found and fixed: the Alive branch did not check ctx after the pause); `make test`/`make lint` green. Spec: §5.6, §5.7 (also §5.5 item 9, §7, §8, §3.4, §4.2, §11.4 item 3).

## Stage 7. Laravel analyzer

- **T-068** [done] — The `internal/rules` package: loading/validation/hash/reloader (rules.go appended; new reload.go + reload_test.go, rules_test.go extended; targeted cmd/crawlerctl/testrules.go, migration 000001 + the model/schema-test contract, docs). Loading: all `*.yaml`/`*.yml` of the `analyzer.rules_dir` directory in name order, strict YAML parsing (KnownFields — a typo in a field is an error, as in the configuration; a file of comments only is skipped), canonicalization — sorting the rules by id; validating the set as a whole (applied or not): non-empty/unique id, a target of the six types, exactly one of match/regex (for cookie_value/header_value/html/meta — regex only: the match engine does not apply to values/the body, an exact match is meaningful only for names), the regex compiles (broken expressions no longer pass silently — the engine.go comment "filtered out at load" is now ensured), weight — a number in (0,1] (0 and NaN rejected: a zero contribution to noisy-OR), ver_min/ver_max parse and ver_min ≤ ver_max (a typo in a boundary no longer silently disables the range). `rules_version` — FNV-32a of the canonical form (every field with a length prefix — a shift of field boundaries does not give the same byte stream) → a signed int32 for the integer column; equal sets — the same hash regardless of the file split and order; description/comments are not in the hash: the version reflects the detection behavior, editing descriptions does not require a re-analysis (pinned by a test). Reloader (reload.go): NewReloader(dir, logger) — the start set (an error → the service does not start: an analyzer without rules would silently find no Laravel), Set() — an immutable snapshot, Reload() on error keeps the old set (Warn) and returns the error, a successful reload with the same version — a no-op without notifications; Subscribe() — a buffer-1 channel replacing an unsent notification with a fresh one (a slow subscriber does not block Reload) + cancel; HandleSIGHUP — the entry point for service.Options.OnSIGHUP (the framework T-012 catches the signal, the package itself does not depend on signals; the analyzer will wire it in T-071), the load seam for tests. Schema: `laravel_findings.rules_version integer NOT NULL` (a refinement of the §6.1 sketch in the spirit of §5.8 "rules_version is stored in the results" — the T-074 re-analysis searches for stale records in findings too; model.LaravelFinding and the model/integration contract tests synchronized). Tests: a table of invalid sets (17 cases), hash stability (split/order/weight/description), disambiguation of the canonical form, the reloader (a set replacement, an error keeps the old one, a no-op, subscribe/cancel/a slow subscriber, the seam, concurrent access — a `-race` run); `make test-rules` on the current rules/laravel.yaml green. Documentation: configuration.md (the rule format, validation, reload, rules_version), schema.md (the rules_version section), dev-environment.md (a pointer). The weight decision: 0 forbidden (a no-contribution signal), NaN rejected. Spec: §5.8, §5.10.
- **T-069** [done] — The matching engine over targets with size/time limits on untrusted content and match evidence (refining `internal/rules/engine.go`: Evaluate/EvaluateCtx/matchRule; new `limits.go` + `evidence.go` + tests `engine_test.go`/`match_test.go`; documentation `docs/analyzer.md`, references from README): matching over all spec 5.8 targets — cookie_name/cookie_value/header_name/header_value — an exact match (match) OR a regex, html/meta — regex only ("a regular expression over HTML"); the first match fires, the traversal is deterministic (cookies in record order, header names sorted — a map traversal in Go is random, and the re-analysis must be reproducible; in the old skeleton the header evidences jumped from run to run); the `Set.Evaluate(Page) Result` signature kept (the crawlerctl test-rules contract), `EvaluateCtx(ctx, page, Limits)` added — a time budget via context.WithTimeout (the EvalTimeout field), ctx is checked at the matchRule input, between rules and between cookie/header values, on cancellation the result contains what fired before it; the evidence — formats for the jsonb laravel_findings.signals `[{rule, weight, evidence}]`: "cookie NAME", "cookie NAME=VALUE…", "header Name: value…", the matched regex fragment (html), the whole meta tag (meta); untrusted content is sanitized — invalid UTF-8 → U+FFFD (valid already in memory, not only after json.Marshal), control C0/DEL → "·" (one line, no terminal escapes), truncation to 64 bytes AT A RUNE BOUNDARY (a skeleton bug: s[:64] tore multibyte runes) + "…", the name inside the evidence is squeezed to 32 bytes so that a hostilely long name does not push the value out of the window; the spec §9 limits ("all parsers have size and time limits") — defense in depth over the stage limits (the HTTP body/headers, content.max_store_bytes): Limits{MaxHTMLBytes 2 MiB — the upper bound of the spec's 1–2 MB recommendation, Laravel signs in the head part; MaxValueBytes 64 KiB per cookie/header value — all value rules are anchored ^, a documented trade-off: a $-anchor on an over-limit value will not fire; MaxPatternBytes 4 KiB; MaxSnippetBytes 64; EvalTimeout 5 s}, a wholly zero Limits = DefaultLimits (forgetting the protection is impossible), HTML/meta are truncated BEFORE matching; the ReDoS decision: Go regexp = RE2, matching is linear, no catastrophic backtracking — the remaining vectors are a bloated input and a bloated pattern, both closed by the size limits, the timeout insures the total budget (a single match cannot be interrupted, but it is linear over a bounded input — cancellation takes effect within one match); patterns are compiled once and cached (a sync.Map by the pattern string + a compile-error cache) — without the cache every page would pay the compilation of every rule, SIGHUP-reload (T-068) and parallel goroutines are safe; an uncompilable/over-limit pattern and an unknown target are silently skipped (validation at load is T-068's job, the engine double-checks, a page is not failed); meta tags are searched by a sliding FindIndex without materializing all matches (the old FindAllString copied the whole HTML into a string on every rule) with an exit on the first match; the scoring tail (noisyOR/versions/Contains) untouched — moving it to score.go is T-070's job; unit tests: all targets × match/regex, evidences as literal strings, header determinism (50 runs), ctx cancellation before the start/between rules (a deterministic countingCtx double)/inside the cookie enumeration, the limit boundaries (HTML exactly at the boundary and beyond, a $-anchor beyond the value limit, meta beyond the limit), a pattern over the limit, malformed data (an empty page, an unknown target, an empty rule), evidence: truncation/ellipsis/a rune boundary/control characters/broken UTF-8, RE2 immunity: (a+)+b over 8 MiB of "a" finishes fast + a match within a 2 MiB cap not found, the DefaultLimits guard (changing the protective values breaks the test). Spec: §5.8, §9.
- **T-070** [done] — The score/versions calculation moved from engine.go into a new `internal/rules/score.go` + `score_test.go` (the engine.go diff is minimal: Evaluate collects the fired rules into a slice and calls ScoreRules in one line — matching/matchRule untouched, T-069). `ScoreRules(matched)` — a pure function: the "noisy-OR" score `1 − Π(1 − wᵢ)` (accumulating in rule order — bit-for-bit the same as the old inline calculation; ScoreFixed by weights kept); the version range — the intersection of the `[ver_min, ver_max]` of the fired rules: the lower bound = max(ver_min), the upper = min(ver_max), a rule without a boundary does not narrow the range, the boundary strings are kept from the rules as is ("9.x" does not become "9"); versions — numeric components (absent = 0, "x"/"*" = +∞ "until the end of the major branch", a numeric rather than string comparison "9.19" < "11"); an empty intersection (`ver_max < ver_min`) — Conflict=true and a reset of both boundaries — a storage decision: an undefined range = ver_min/ver_max NULL in laravel_findings/laravel_domains, the conflict fact is visible by both empty boundaries; equal boundaries — not a conflict (a single-version range), including equality in different notations "9.19" = "9.19.0"; invalid version strings are ignored. The `is_laravel` threshold — `Result.IsLaravel(analyzer.laravel_min_score)`: a non-strict `score ≥ threshold`, a non-positive threshold is replaced by `DefaultMinScore = 0.6` (the config validation allows only (0, 1] — protecting direct calls with zero). Unit tests per §11.1: noisy-OR (an empty set, weight 1, the accumulation of identical ones), the intersection table (no boundaries / min only / max only / an intersection / equal boundaries / equal in different notations / a branch conflict [10,∞)∩(−∞,9.x] / a point one [10]∩(−∞,9.19.1] / ignoring invalid ones), the threshold (equality at the boundary, the default at 0/negative, a threshold of 1), parseVersion (components, "x"/"*", spaces, errors), less (9 vs 9.1, 9.x vs 10, 9.19 vs 9.19.1, 10.x vs 10.1), Contains (extended: an unbounded range, an invalid expectation); the old scoring-tail tests (TestNoisyOR/TestVersionCompare/TestResultContains) moved from rules_test.go, the Evaluate tests untouched. Documentation: docs/configuration.md §analyzer — the "Score and version range calculation" subsection (the formula, the intersection, a conflict→NULL, is_laravel/the threshold, all matches in laravel_findings regardless of the threshold §13.1). `COMPOSE_PROJECT_NAME=crawler make test`/`make lint` green. Spec: §5.8, §11.1, §5.10.
- **T-071** [done] — Writing `laravel_findings` (a new `internal/analyzer` package — worker.go + worker_test.go; assembling `cmd/analyzer/main.go` from the stub; documentation docs/{analyzer,services}.md, README): the worker consumes q:analyze (Result — the subdomain_results key, T-024; published by the HTTP stage T-058 and render-worker T-067) with the `analyzer` group, a window of 16, 8 deliveries in parallel (analysis is light — RE2 over a bounded input + a couple of DB statements; scaling — by instances), each — Processing (T-025: XACK only after the DB write). Processing a delivery: SELECT the row (the headers/cookies jsonb — after render these are the final navigation's data T-067/T-058, content_hash; no row — success without a write, the renderworker style) → the content via ContentStore.Get (a NULL hash / ErrNotFound / an empty DOM / a truncated hash — analysis over headers and cookies: the rules over the six spec 5.8 targets, html/meta — part of the set) → a set snapshot per delivery (the SetSource seam — satisfied by the T-068 Reloader; subscribing to changes is not needed — SIGHUP is picked up by the next delivery), EvaluateCtx with DefaultLimits (a zero Limits → the T-069 default, the cancellation budget — the delivery context) → ONE transaction (spec 3.4): UPDATE subdomain_results.rules_version = the set's version — the T-074 re-analysis stamp (T-067 resets it to NULL on a content change — "a repeated publication will re-analyze and set a fresh one" — this stage's duty), the UPDATE as the first statement — the freshness guard: content_hash IS NOT DISTINCT FROM the read one (the T-058 recheck DELETE+INSERT or the T-067 render rewrote it — 0 rows → success without a write: the new episode has its own publication) + NOT EXISTS of a deleted domain (§8/§13.1) → laravel_findings by the same key: ALL matches of any rule regardless of the threshold (§13.1), is_laravel = IsLaravel(analyzer.laravel_min_score) (a non-strict ≥, a non-positive threshold → DefaultMinScore 0.6, T-070), score → float4, ver_min/ver_max NULL without a boundary and on a range conflict (T-070), signals jsonb [{rule,weight,evidence}] — lowercase tags per the §6.1 sketch (rules.Evidence has no tags, the format is introduced by the writing stage), rules_version from the current set; INSERT … ON CONFLICT DO UPDATE — a redelivery and a re-analysis overwrite (§3.5); no matches — no row is written, an existing one is deleted (a previous analysis found signs that are gone), the stamp is set on an "empty" outcome too. Assembling cmd/analyzer: the Reloader before service.Run (a set error — exit code 1: an analyzer without rules would silently find no Laravel), OnSIGHUP = HandleSIGHUP, OnDBPool — PackStore in read mode (the same zstd dictionary as the writing stages, otherwise decompression is impossible; DiskWatchLoop not needed) + RedisQueue without WatermarkPublisher (the analyzer does not publish — the end of the §7 pipeline). The laravel_domains aggregation — T-072, in the transaction only findings (pinned in the package doc comment). Unit tests on fakes (a pool of subdomain_results + domains + laravel_findings with snapshot transactions and a rewrite hook, ContentStore, a SetSource with a set change, a queue with a protocol): the SQL contracts (the stamp/guards/the PK upsert/the §6.1 columns), the end-to-end step order per the journal (Get → Begin → the stamp → findings → Commit, XACK after the DB), the branches — matches below the threshold are written with is_laravel=false, the equality boundary, the default threshold at 0, a version conflict → both boundaries NULL, no content (4 variants), a vanished row, a row rewritten by another episode, a deleted domain, an idempotent redelivery, a SIGHUP reload (the new version in the stamp and findings), an empty outcome with the deletion of the previous row, failures of all steps with a state rollback, a broken jsonb, ctx cancellation, a poisonous record, a Run loop smoke test (both deliveries acknowledged, an error does not stop the loop). make test/lint green. Spec: §5.8, §6.1, §13.1, §4.2, §7, §8, §3.4, §3.5.
- **T-072** [done] — Domain-level aggregation into `laravel_domains` (a new `internal/analyzer/aggregate.go` + `aggregate_test.go`; a MINIMAL edit of `internal/analyzer/worker.go` — one call in the transaction + doc comments; `internal/rules/score.go` exported `CompareVersions` + tests; the fake pool of `worker_test.go` extended with laravel_domains live semantics and docs/{analyzer,schema,services}.md, README). The recalculation runs in the same T-071 worker transaction right after writing the findings — every committed state is consistent (spec 3.4). The aggregate composition — over CONFIDENT matches (`laravel_findings` with `is_laravel`): the §13.1 laravel_domains entry threshold "score ≥ 0.6" is the definition of confidence, weak ones stay only in findings; therefore score = the maximum (only the maximum could reach the threshold — equal to the maximum over all findings), found_on_mask — the bits of subdomains with an is_laravel match of any variant (the variants collapse into a bit; the model.DNSMask semantics: bit № = subdomains.id, 0..62, like dns_mask §5.8). The version range — the UNIFIED covering [min(ver_min), max(ver_max)], not the intersection (the spec 5.8 word "unified"; the intersection would treat each findings by the rules of one page): comparison by numeric components via the new `rules.CompareVersions` (a textual MIN/MAX in SQL would order "10" before "5.3"; "x"/"*" = +∞, "9.19" = "9.19.0"), a NULL boundary of any match (no boundary or a T-070 conflict) propagates to the domain — the union is unbounded from that side, an inversion is impossible (ver_min ≤ ver_max for every match), equal in different notations keep the first one (reading by PK is deterministic). rules_version = the version of the recalculating set (in the transition period of the T-074 re-analysis, findings of mixed versions — the row is rewritten by every delivery, after the last update a fresh one), updated_at = now() of the server (the transaction time, the record.go style). A domain without is_laravel rows — no laravel_domains row: the recalculation deletes an existing one (the §13.1 threshold + the §3.5 rewrite). An upsert by PK (zone_id, domain_id) §3.5/§6.1. Coordinating parallel deliveries of a domain (the HTTP stage publishes q:analyze in a batch of rows, the worker is parallel 8 + instances): the recalculation takes pg_advisory_xact_lock(domain_id) (globally unique, domain_id_seq) BEFORE reading the findings — a second recalculation waits for the first's commit and reads complete data; advisory, not FOR UPDATE domains: the worker already holds the subdomain_results row lock (the stamp), the "stamp → domains" chain would deadlock with the T-058 recheck transaction (UPDATE domains → DELETE subdomain_results), only analyzer recalculations take the advisory key. Unit tests on a fake pool (live laravel_domains semantics: SELECT of the domain's is_laravel rows in PK order, upsert/DELETE by PK, a snapshot/rollback of the transaction): the SQL contracts and parameter types, the end-to-end path through ProcessResult with the order "findings → lock → SELECT → upsert → COMMIT", the score maximum, the range union (the numeric order min(10,5.3)=5.3 / max(9.x,11)=11, NULL boundaries and a conflict of a separate match, equal in different notations), the bit mask (the variants collapse, bit 62, subdomain_id 63 — an error), a foreign domain and weak matches outside the aggregate, "no confident ones — the row deleted" (from a weak one and from an empty outcome), an idempotent redelivery, a deleted domain (the stamp guard — no aggregation, the row untouched), failures of every step (lock/select/upsert/delete/commit — a rollback of the findings and the row), ctx cancellation. Spec: §5.8, §6.1, §13.1, §3.4, §3.5.
- **T-073** [done] — A working set of Laravel YAML rules from documentation and samples (`rules/laravel.yaml` rewritten — 22 rules with weight/boundary rationales in comments; the `testdata/laravel-samples` corpus extended 4→13 samples; `internal/rules/samples.go` — a `rules` field in meta.yaml; `cmd/crawlerctl/testrules.go` — verifying the rules composition; a new test `internal/rules/laravel_test.go`): rules by groups — cookies: XSRF-TOKEN 0.25 (other frameworks use the name too), laravel_session exact 0.35 (the default Str::slug(APP_NAME)+'_session', Laravel 4+), the *_session suffix 0.15 (lowered from 0.2: falsely matches ci_session of CodeIgniter and _<app>_session of Rails), the encrypted value ^eyJpdiI6 0.45 (EncryptCookies: base64 JSON {\"iv\":…}), remember_<guard>_<sha1> 0.35 (SessionGuard, not seen outside Laravel); headers: Host in the response 0.2 (the PHP built-in server / `php artisan serve`; X-Powered-By and Server: PHP/… deliberately not taken — a sign of PHP in general, noise on every WordPress); meta: csrf-token 0.2 (the same metas are generated by Rails/Symfony; the spec's target=meta used); HTML: the hidden field _token 0.25 (csrf_field(); Symfony writes form[_token], CakePHP — _csrfToken); build: Vite /build/assets/+an 8-char hash 0.2 ver_min 9.19 (laravel-vite-plugin — the default since Laravel 9.19.0, June 2022), @vite/client dev mode 0.1 ver_min 9.19, Mix ?id=20hex 0.15 ver_max 9.x (the default until 9.19, since Laravel 5.4; the boundaries are indicative — Mix/Vite are installed on other versions too); the stack: Inertia data-page 0.2 (non-Laravel adapters exist), Livewire wire:* 0.5 (works only over Laravel) + livewire*.js 0.25, Ziggy 0.35, Sanctum /sanctum/csrf-cookie 0.3, Laravel Debugbar 0.3, window.Laravel 0.4 (the default layouts 5.3+), JSON {\"message\":\"Unauthenticated.\"} 0.3 (the Authenticate middleware API/Sanctum); default pages: the classic welcome (position-ref/title m-b-md) 0.45 with the boundaries [5.3, 7.x] (replaced by the Tailwind version in 8.0), the laravel.com/docs link 0.3, pagination aria-label=\"Pagination Navigation\" 0.25. Weight = the rarity of the sign outside Laravel; noisy-OR accumulation (T-070), the 0.6 threshold requires 2–3 independent signs (cookies alone ≈0.77–0.79; the Rails negative — 0.32). The corpus (synthetic until the customer hands over real samples, §13.3): the positives laravel-8-mix and laravel-11-vite (existing) + laravel-6-welcome (the classic welcome, Mix, artisan serve with Host in the response), laravel-5-8-legacy (window.Laravel, remember_web_, Mix), laravel-10-breeze-login (Breeze: the csrf_field _token, Vite), laravel-12-vite-livewire (Livewire 3, Ziggy, pagination, the custom session acme_session), laravel-9-json-api (no HTML signs: cookies + a JSON 401 only), laravel-11-dev-spa (the Vite dev server, Sanctum, Debugbar); the negatives django/symfony (existing) + rails (a csrf-meta + _shop_session → weak 0.32 below the threshold — a false-positive control), wordpress (?ver= assets, X-Powered-By), parking (a parking page). Each sample's meta.yaml got a `rules` — the full composition of fired rules (`[]` — none; yaml.v3 distinguishes nil/empty — verified); the verification of is_laravel by the 0.6 threshold, the covering version range (Contains) and the rules composition — in crawlerctl test-rules (make test-rules) and the laravel_test.go unit test (a corpus ≥5 positives/≥4 negatives, no conflicts, no false positives on an empty page); all 22 rules covered by the corpus, a run of 13/13 OK; `make test`/`make lint`/`make fmt` green. Spec: §5.8, §11.1, §13.1, §13.3.
- **T-074** [done] — Re-analysis (a new `internal/analyzer` package — reanalyze.go + reanalyze_test.go; the command in cmd/crawlerctl/manage.go finished, main_test.go tests; a MINIMAL edit of cmd/analyzer/main.go — one OnRun goroutine). The staleness criterion — `subdomain_results.rules_version IS DISTINCT FROM <the rules.Set.Version of the set>` (T-068): IS DISTINCT FROM, not `<>` — NULL ("never analyzed": the rows before T-071 and the new T-058 episodes) does not drop out of the comparison; the §13.1 guard `status <> deleted` (a JOIN domains, the scheduler selection style); disabled zones are deliberately not filtered — the re-analysis does not touch the network. Batches: a selection of keys+name (label || '.' || zone, the T-027 style) → publishing a Result (T-024) to q:analyze per each — re-downloading is excluded, the worker takes the content by the key; backpressure — WatermarkPublisher (T-026, the shared threshold scheduler.queue_high_watermark), ctx cancellation — a clean exit. The key pagination of the drain (a row-value `>` by PK): the publication does not remove a row from the selection (the worker T-071 will pin the version asynchronously) — a repeated selection "from the start" would duplicate an in-flight batch, so the drain goes by a cursor until the first incomplete batch; duplicates between passes are bounded and extinguished by the consumer's idempotency (like T-066). The contract with T-071 pinned: the worker sets rules_version per every processed message, including rows without content. `ReanalyzeLoop` (the RecheckDaily T-030/RequeueRenderPending T-066 style): an initial drain → the ticker DefaultReanalyzeInterval=1h (a safety net: MAXLEN ~ lost records, failures; a pass without an index over rules_version — not a minute) → an off-schedule drain on a set change (Reloader.Subscribe, SIGHUP); a pass error — Warn, the loop lives (the OnRun contract); the rules source — the narrow RulesSource interface (*rules.Reloader). Constants: a batch of 5000 (= DefaultRecheckBatch). `crawlerctl reanalyze [--zone NAME]` (the T-011 skeleton brought to working state): zone normalization (idn.NormalizeZone), config → a one-shot LoadDir → ZoneIDByName (new, zones.name UNIQUE) → a drain by zone/all with printing the enqueue counter; the CLI pool pool_size=2 (the §6.3 budget), Ctrl-C interrupts, exit codes per T-011; make reanalyze matches. Unit tests on fakes (a pool with live SQL semantics, a queue, RulesSource): exact SQL/parameters, the filters (NULL/an old/the current version, deleted, a zone, the JOIN), batching and the cursor, a batch-multiple set, an empty set, idempotency (after a "worker commit" a repeat — 0), selection/publication errors/cancellation, the loop (an initial drain, the ticker, the rules-change trigger, Warn with survival, a clean exit), ZoneIDByName, the CLI (routing/flags/codes, the path without infrastructure — exit code 1 on a missing config, zone validation before connections); -race clean (a CGO_ENABLED=1 run). Documentation: docs/analyzer.md (the "Re-analysis" section), docs/dev-environment.md §5/§6. Spec: §5.8, §11.4 (item 4), §4.2, §13.1.
- **T-075** [done] — Unit tests of the rules engine: score, range intersections, conflicts; a run over the corpus of Laravel samples (with expected `is_laravel` and ranges) and negative samples (non-Laravel). A new end-to-end `internal/rules/corpus_test.go` (the T-022/T-059 corpus style, corpus*-prefixed names): a case — the full story "headers.json+cookies.json+index.html → Page → EvaluateCtx(DefaultLimits) → ScoreRules → IsLaravel(0.6)" per each of the 13 samples: the exact composition of fired rules, the score, the exact strings of the computed range boundaries ("" — no boundary, NULL in the DB), a conflict, IsLaravel + the default threshold at zero; the ScoreRules link is verified by a direct call on the fired rules; the corpus integrity — ≥5 positive, ≥4 negative, ≥1 negative with weak matches, exact expectations mandatory in every meta.yaml. The approach to the score expectation: in meta.yaml — the noisy-OR of the weights of the rules from `rules`, rounded to 3 decimals, the test allows ±0.001 (not brittle to the float accumulation order and to edits of the set's weights). Separate groups: the JSON-API sample with empty HTML — passive analysis over cookies works without a body (0.772 ≥ 0.6, json_unauthenticated does not fire); the negative samples — is_laravel=false and a full breakdown of the weak matches (no weight reaches the threshold, the accumulation is lower; rails: 0.32 = noisy-OR(0.2 csrf-meta, 0.15 *_session) — the §13.3 reference of a "similar" framework); the score mathematics end to end via synthetic pages (single weights, the accumulation 1−0.8ⁿ with the threshold reached only by the 5th weak sign, a weight of exactly 0.6 — the score bit-exact at the threshold and the non-strict ≥ gives true, the real pair 0.25+0.45 → 0.5875 below the threshold, a weight of 1 closes the score); the range intersections via a preassembled Set (the lower = max(ver_min), the upper = min(ver_max) over many rules, equal boundaries in different notations 9.19=9.19.0 — not a conflict, the boundary strings kept as is — "9.x" not "9", a conflicting set on a synthetic page [10,∞)∩(−∞,9.x] — the boundaries reset, all signals kept, the score/threshold in force, plus a conflict of the working set vite≥9.19 + welcome_classic≤7.x on a synthetic page "Laravel 12 with the classic welcome"); the limits (the laravel-11-vite sample with 2 MiB of garbage before the document — the HTML signs in the tail do not match, the cookies untouched by the limit and holding the threshold; a marker byte-for-byte at the MaxHTMLBytes boundary — the last byte exactly at the limit is visible, a byte further is not, is_laravel flips). The expectations of `testdata/laravel-samples/*/meta.yaml` extended (score/calc_ver_min/calc_ver_max/conflict — the computed values pinned from the actual engine run) and targeted `internal/rules/samples.go` (the SampleExpect fields + loading; backward compatible — crawlerctl test-rules unchanged); documentation — docs/dev-environment.md §5. No engine defects (the T-069/T-070 territory) found: the actual composition/score/ranges of all 13 samples matched the expectations on the first run; control mutations of the expectations (the composition, the score, a range boundary) are caught by the corpus. Spec: §11.1, §11.4 (items 4, 11), §13.3.

## Stage 8. Monitoring and search

- **T-076** [done] — The `internal/metrics` package as the common registration point of all services' metrics (metrics.go rewritten + metrics_test.go extended; a comment in internal/service/service.go; documentation docs/{services,configuration}.md). `New(name)` now returns a `*metrics.Registry` factory — an embedded `*prometheus.Registry` (Register/Gather promoted for non-standard collectors) + the service name; inside, the same framework base metrics `crawler_service_up`/`crawler_build_info` (now via the factory) and the Go/runtime/process collectors (uptime — from `process_start_time_seconds`, no separate metric). The factory methods `Counter`/`Gauge`/`Histogram` (+ `Must` variants; Histogram returns a `prometheus.ObserverVec` — that is what `MustCurryWith` of a HistogramVec is) create a vector with a single first label `service` and return it curried (`prometheus.MustCurryWith`): the caller passes values of only the other labels (`tasks.WithLabelValues("dns").Inc()`), the service value is substituted automatically and cannot diverge from the registry; HELP/TYPE — from the Opts, the instrument's documentation is written at the place of creation. The common labels for the T-077 instruments — the constants `LabelService`/`LabelStage` (dns/http/render/analyze)/`LabelQueue` (q:dns…q:dead)/`LabelType` (the error type). A duplicate registration of an identical metric does not panic: `AlreadyRegisteredError` with the same descriptor (a `Desc.String()` comparison) → the already registered vector is returned (duplicates share one collector) — instrument constructors may be called repeatedly in tests; conflicts (the same name, another help/labels — caught by prometheus's dimHash protection; another collector type — a type check after AlreadyRegisteredError) — an error, `Must` panics. `Handler` accepts any `prometheus.Gatherer` (both a `*Registry` and a bare registry). The HTTP endpoint `/metrics` on every service was already brought up by the framework T-001/T-012 (`service.Run`: `metrics.New` + `metrics.Handler` on `Options.MetricsAddr`, the calls unchanged) — beyond the framework added: the factory/registry, the common labels, duplicate tolerance and unit tests: the service autometric for all three instrument types + exporting HELP/TYPE/bucket/sum/count, an instrument without extra labels, duplicate registration (all three types, a shared collector: 1+2=3), help/label/type conflicts (an error, a Must panic), the independence of same-named metrics in different services' registries, a Handler with a bare registry. The `monitor.listen` semantics (spec 5.10) pinned in the documentation: it is the address of monitor's own HTTP endpoint (`/healthz`+`/metrics`, `METRICS_ADDR` takes precedence), not a single scrape aggregator — spec 5.9 requires an endpoint on every service, Prometheus scrapes them individually (the dev config deploy/docker/prometheus/prometheus.yml, the monitoring profile, the address table :9100…:9106 — in docs/services.md). The stage instruments (throughput, queues, errors, the render share, the response time, backlog, memory) — T-077 (the registry from service.Run is passed into the working logic by T-077 — in T-076 the framework works with it itself). Code-review edits (round 1, agy/gemini-3.8-flash-high): label validation in withServiceLabel — LabelService/duplicates return an error, not a prometheus.New*Vec panic; in the error methods CurryWith instead of MustCurryWith; descOf without a deadlock — Describe in a separate goroutine with draining the rest and ok=false for collectors without descriptors (register → descEqual); Histogram buckets are not part of the prometheus descriptor — re-registering with other Buckets returns the vector of the first call (pinned by a doc and a test, the buckets of the registered vector cannot be compared); tests extended — concurrent registration of 16 goroutines (a CGO_ENABLED=1 -race run clean), histogram conflicts/Must panics of all three types, invalid labels. `COMPOSE_PROJECT_NAME=crawler make test`/`make lint`/`make fmt` green. Spec: §5.9, §5.10.
- **T-077** [done] — The working-logic metrics of all services per spec 5.9 on the T-076 factory. Passing the registry through: `service.Options.Metrics *metrics.Registry` — main creates the registry in advance (the instruments/collectors get it before OnRun starts), nil — the framework builds it itself (`newRegistry`); the OnRun/factory signatures unchanged, the framework tests intact. The stage instruments — `NewMetrics(reg)` constructors in the worker packages (nil-safe, Options.Metrics; idempotent — the T-076 duplicate registration shares collectors): `crawler_stage_tasks_total{stage}` (a counter of finished tasks: dns/http — domains, render/analyze — subdomain×variant records) and `crawler_stage_seconds{stage}` (duration histograms, buckets per stage) incremented at the episode completion points — dnsworker.processBatch (around ProcessDomain), httpworker/renderworker/analyzer processDelivery (before the XACK, spec 4.2); `crawler_stage_errors_total{stage,type}` — http-worker by the ErrorClass classes (timeout/redirects/tls/body/network, §8) from the finished result (cache hits — without a response time), render-worker by render.ClassOf (task/context/browser, T-061/T-063); `crawler_domain_status_total{type}` — the final domain statuses: the DNS stage — the terminal branches (not_delegated/delegated_no_address/temp_error) after the write, the HTTP stage — recordStatus (alive/no_http/temp_error); `crawler_http_response_seconds` — the response time of available non-cached checks (ResponseMS, spec 5.5 item 7); the render share — `crawler_render_records_total{rendered}`: false incremented by http-worker over the result records, true — render-worker when a DOM is written, the share = the rate ratio. Queue lengths: `crawler_queue_depth{queue}` — a periodic XLEN collector `metrics.CollectQueueLoop` (collect.go, 15 s, the first pass immediately, an error — Warn and the gauge keeps the last one; the LenFunc source = the T-026 gate.Len, no Redis dependency in the package) at the stream consumers: dns-worker q:dns, http-worker q:http, render-worker q:render, analyzer q:analyze; q:dead has no consumer (manual investigation, spec 4.2) — crawlerctl status shows the depth (T-079), the metric will appear with a consumer. The backlog depth: `crawler_backlog_domains{state=pending|queued}` — `scheduler.BacklogOnce/BacklogLoop` (backlog.go, 30 s, an SQL `GROUP BY check_state` without deleted §13.1, both series written explicitly — an empty backlog does not vanish) — lives in the scheduler as the owner of the domains states (internal/monitor untouched — the spec 5.9 zone counters/alerts are T-078/T-080; a TODO in zonestats.go left); the metric is a level (a gauge), not a counter: a backlog is a level, not an event. Worker memory: no separate metrics added — go_memstats_* (heap) and process_resident_memory_bytes (RSS — what limits the spec 3.2 MemoryMax) are already collected by the T-076 collector, a runtime.MemStats duplicate is meaningless. Pinned: the status/record counters — per a finished episode (after a successful DB write before the XACK; a race with §13.1 deletion is not distinguished, at-least-once repeats — honest work); the DNS stage does not type exchange errors — temp_error is the outcome (T-036); the analyzer is passive — findings/is_laravel are DB data, not metrics. The LabelStage stage values — the metrics.StageDNS/HTTP/Render/Analyze constants. Wiring: cmd/{scheduler,dns-worker,http-worker,render-worker,analyzer}/main.go create the registry, pass Options.Metrics + the workers' Options and bring up the collectors as goroutines (the scheduler — a 4th background cycle in bg). Unit tests on fakes: nil-safety, exporting values via Gather, increments by the typical paths (dnsworker — an end-to-end Run on fakes of result_test: 2 episodes/1 status; httpworker — Episode + a Run smoke test: 2 response-time observations, rendered=false, alive; renderworker — a processDelivery success/error-class; analyzer — processDelivery); the queue collector (export, independent stream errors, a no-op, a loop surviving an error and recovering), the backlog (exact SQL/parameters, zero states, errors, defaults, the loop), service (the newRegistry registry choice, Run with Options.Metrics); `CGO_ENABLED=1 -race` over the affected packages green. Documentation: docs/services.md — a metrics table by services with increment points and pinned decisions, Options.Metrics. `COMPOSE_PROJECT_NAME=crawler make test`/`make lint`/`make fmt` green. Spec: §5.9, §3.2, §4.2, §3.4, §8.
- **T-078** [done] — The `monitor` service: a periodic batched recalculation of the caching zone progress counters `zones.domains_total`/`domains_checked` (a new `internal/monitor` package — zonestats.go + zonestats_test.go; cmd/monitor/main.go from the stub — OnDBPool/OnRun, the cmd/scheduler style). The columns are already in migration 000001 (spec 6.1) — no migration needed. The semantics (§5.9, §11.4 item 6 "total/checked", pinned in a doc comment and docs): `domains_total` — the live domains of the zone `status <> deleted` ("live" — present in the registry, not status=alive: alive is learned only by a check and cannot be the progress denominator; deleted in the denominator — the progress would never be 100 %, §13.1 the deleted are not checked); `domains_checked` — `check_state = done` (checked in the current cycle): the §5.3 recheck moves done → pending, the counter naturally tracks the new cycle, while `last_checked_at IS NOT NULL` ("ever") would freeze at 100 % after the first; both counters over one base set ⇒ `checked ≤ total`. One SQL statement per pass: `UPDATE zones … FROM (zones LEFT JOIN (SELECT zone_id, count(*), count(*) FILTER (WHERE check_state = done) FROM domains WHERE status <> deleted GROUP BY zone_id))` — all zones batched, without row-per-zone (§5.9); GROUP BY by the partitioning key → partitionwise aggregation over partitions; LEFT JOIN from zones zeroes the zones without live domains (INNER would leave frozen counters); a FILTER count — both values in one scan; a single-statement snapshot — consistent total/checked; updated_at — the pass moment (the "progress stopped" observation, T-080). The period — `monitor.zone_stats_interval` (a field since T-006, default 60 s — the top of the §5.9 "30–60 s" corridor: less load with the same freshness guarantees; the >0 validation already existed, the 30–60 s range is not hard-wired — a recommendation in docs, dev/tests use short intervals); the batch default DefaultZoneStatsInterval mirrors the config (the DefaultRecheckInterval style). `ZoneStatsLoop` (the RecheckDaily T-030/RetryTempErrors T-031 style): the first pass right at startup (fresh counters after idle/restart), the ticker, a pass error — Warn and the loop lives (the T-012 OnRun contract; a pass is idempotent, the next one will repeat over the actual state), the pass outcome — Debug (Info every 30–60 s — noise), ctx cancellation — a clean nil (§3.4); `ZoneStatsOnce` — a one-shot pass with the outcome in ZoneStatsResult. The progress metrics — a TODO(T-077) in the doc comment (internal/metrics untouched, the T-076/T-077 territory). Unit tests on a fake pgx pool (the recheck_test.go style): the exact SQL text and parameters (the check_state/status codes, updated_at with fixed clocks), the defaults (time.Now within the corridor), an empty set, repeated passes, an error/cancellation (errors.Is), a nil pool, withDefaults, the loop (an immediate first pass without a tick, a pass on a tick after a Warn with survival — a Debug-level handler, cancellation before start without a Warn, a nil pool). Documentation: docs/services.md (monitor — not a stub, the counter semantics), docs/schema.md (the domains_total semantics clarified against count(*) over a partition), docs/configuration.md (the purpose of the monitor section fields). `make test`/`make lint` green. Spec: §5.9, §11.4 (item 6), §6.1, §13.1, §3.4.
- **T-079** [done] — `crawlerctl status`: a summary of zones, queues, the speed and the ETA of the first cycle's completion (a new `cmd/crawlerctl/status.go` + `status_test.go`; the T-011 skeleton removed from `manage.go`; the routing cases of `main_test.go` updated — the command table in main.go unchanged). Zones — with one query: the `zones.domains_total/domains_checked` counters (monitor updates them, spec 5.9) + a `domains` aggregate over `check_state` via `count FILTER` with a LEFT JOIN (a zone without domains stays in the summary); pending excludes `status = deleted` — the T-028 scheduler does not select a deleted domain, there is nothing to wait for in its check; queued/done without a status filter (a domain in work/finished); a human-readable monospace table (text left, numbers right, a "total" row) and `--json` in the `domain` style (spec 5.11): the same data, "n/a" — null. Queues — XLEN of all spec 4.2 streams (`q:dns/http/render/analyze/dead`) via `BackpressureGate.Len` (T-026, a narrow API over go-redis; the gate threshold does not participate); an unavailable Redis does not hide the DB summary — the section is printed with "n/a", the command exits with code 1 (informativeness + a non-zero code for scripts). The speed per stage is consistent with the available data, without invention: domains/s — the average since the start of the day over `last_checked_at` (the date column, spec 6.1 — there is no hourly window; both intervals — `now()`/`current_date` of one transaction, the DB server's timezone); with a window < 10 minutes the estimate is not shown (a spike right after midnight); HTTP results/s and renders/s — over the last hour by `subdomain_results.checked_at` (render has no separate timestamp — the `rendered` share in the window); the DNS stage pace separately and the analysis are not measurable from the DB (no timestamps; the DNS+HTTP completion is the "domains/s") — honestly not shown, pinned in help/docs, the stage metrics — Prometheus (T-076/T-077). The ETA of the first cycle = `(pending+queued)/speed` with division-by-zero protection (a speed of 0 or unknown → "n/a"), an empty cycle — "nothing to check" regardless of the speed, the ETA is rounded and at least 1 s with a non-zero remainder; durations — rounded human-readable (s/m/h+m/d+h). The SQL contracts (the exact text of the two queries, the check_state/status constants from model, the scheduler/dispatch.go style) and the aggregate semantics — on a fake db.DB pool; a fake XLEN (the polling order, failures); computeRates — a short window, a zero speed, an empty cycle, rounding/the minimum; the table format and JSON (null for "n/a", degradation without Redis); routing/codes — exit code 1 without configuration (the TestRunReanalyze style), an extra argument/flag — code 2. The CLI pool_size=2 (the spec 6.3 budget, the manage.go T-074 style). Documentation: docs/dev-environment.md §6; the Makefile `status` target already matched. Spec: §5.9, §4.1, §5.3, §3.3.
- **T-080** [done] — The alerts of the minimal spec 5.9 set in the monitor service (new `internal/monitor/alerts.go` + `alerts_test.go`; an edit of `cmd/monitor/main.go` — a second cycle in OnRun next to the T-078 ZoneStatsLoop: the zone recalculation in the background, the alerts in the foreground, collecting both results on ctx cancellation — the cmd/scheduler style). The evaluation — an `Alerts` type with state memory between passes, `Pass` evaluates all alerts at once; the `AlertsLoop` cycle in the ZoneStatsLoop style (the first pass immediately, a source error — Warn and the loop lives, ctx cancellation — a clean nil). The alerts: growth of the DNS error share — the temp_error share among the current day's checks by one SQL aggregate (last_checked_at has date precision, spec 6.1 — there are no minute windows, the same choice as the crawlerctl status T-079 pace; current_date — the DB server's timezone, like the T-058 record's now(); NXDOMAIN→not_delegated — a valid outcome, not an error), turning on at ≥30 % / clearing at <20 % (hysteresis; the T-035 adaptive keeps timeouts ≤10 % — 30 % is a refusal of resolvers, not a dip), with <100 checks in the window the share is empty-window noise, "not measured"; queue overflow — XLEN of q:dns/q:http/q:render/q:analyze via `BackpressureGate.Len` (T-026 — the same narrow API as the scheduler's and crawlerctl's; q:dead is not limited by the gate — not checked) against `scheduler.queue_high_watermark`, firing at len ≥ the threshold (simultaneously with closing the publication gate), clearing below 90 % of the threshold; a progress stop — max(zones.updated_at) (written by ZoneStatsLoop) older than 5 × zone_stats_interval (a pass over 350 million rows — minutes, a single miss is not an accident; a self-control caveat: a full monitor death is seen by the framework's crawler_service_up metric, not this alert), NULL — the zone catalog is empty; a space shortage — statfs of content.dir against `content.disk_free_percent` by the same formula as the Put pause (T-046): the alert fires exactly when content writing is stopped, without a hysteresis gap (a desync with the real pause is worse than flapping); NVMe wear (SMART, spec 10.3) — percentage_used of the health log (byte 8) + TBW (bytes 48–63, the NVMe unit of 512,000 bytes) via NVME_IOCTL_ADMIN_CMD directly (without nvme-cli and new dependencies; the size of struct nvme_admin_cmd — a compile-time control, the ioctl number verified against _IOWR('N',0x41,72)), devices from sysfs, the alert at ≥90 % of the resource (values >100 are valid — the resource is exhausted), resource monotonicity — hysteresis is not needed; in a Docker container sysfs sees the host's devices, but /dev/nvme* is not passed through — an honest degradation "not measured" (a Debug transition, without Warn and a false firing), full SMART monitoring on a systemd host — stage 9 (T-089). The signal is edge-triggered (the DiskWatchLoop style): a Warn "ALERT: …" on firing, Info on clearing, Debug "not measured"; source errors (DB/Redis/statfs) — a Warn of every pass (ctx cancellation — without a Warn, part of the shutdown). State export — the "log + field" decision: an `AlertsResult` snapshot (all 5 alerts, the ok/firing/not_measured statuses + detail, the `Firing()` selection) into the `AlertsOptions.OnPass` hook; monitor does not introduce its own Prometheus-gauge registry — the alerts metrics will be introduced by the T-077 layer over internal/metrics (do not duplicate registries and the LabelQueue/LabelStage labels, the hook — a hook-up point without editing the loop). Thresholds: existing fields reused (queue_high_watermark; disk_free_percent + content.dir), the rest — `internal/monitor` constants with rationales in the code and docs (the spec sets no numbers; the cycle period = monitor.zone_stats_interval from cmd — one pair of monitoring cycles in the spec 5.9 corridor, no new config fields introduced); OSStatfs/OSWear — own copies of the content/disk.go approaches (T-046): the storage package is not pulled into the monitor, the sources are behind the SpaceChecker/WearSource/StreamLener interfaces for substitution in tests. Unit tests on fakes (a pgx pool with routing of two SQLs, XLEN, statfs, Wear): the SQL and argument contracts (temp_error), the thresholds and hysteresis (30/25/20 %; 1000/950/899), the selection minimum, the boundaries (exactly at the disk threshold — ok, strictly below — an alert), the degradations (total=0, disabled control, without Redis/a threshold, empty zones, the directory unset), source errors (Warn + the next pass alive), the quiet SMART degradation (without a Warn), the edge records (a repeated quiet pass without Warn/Info), OnPass/the pinned snapshot order/Firing(), withDefaults and the OS implementation substitution in NewAlerts, OSStatfs on a live t.TempDir, OSWear on fake sysfs/devfs (an empty sysfs, an inaccessible /dev node, ctx cancellation), the loop (an immediate first pass, survival after a source error, cancellation during a pass without a Warn, a nil pool); CGO_ENABLED=1 -race green. Documentation: docs/services.md (the monitor alerts section), docs/configuration.md (the purpose of zone_stats_interval; no new fields), config.example.yaml (a comment). `COMPOSE_PROJECT_NAME=crawler make test`/`make lint`/`make fmt` green. Code-review edits (round 1, agy/gemini-3.8-flash-high — Sonnet exhausted its limits): the percentage_used offset — byte 5, not 8 (per uapi/linux/nvme.h struct nvme_smart_log: critical_warning 0, temperature 1–2, spare 3, thresh 4; byte 8 is in reserved — the alert would read an eternal zero), decoding moved into a pure parseHealthLog with a table test of offsets (including >100 % and a short buffer); the page buffer aligned (a zeroed uint64 field in the wrapper) + runtime.KeepAlive after the syscall (the address in cmd.addr hidden from GC); evalWear — worst starts from the first device (zero wears left an empty name in the detail, the 0 % test); the firing→not_measured transition (a source failure of an active alert) — Info "a fired alert is no longer measured", not a quiet Debug (a test); cmd/monitor run — the derived ctx cancellation (an early exit of its own cycle stops the background one, without an eternal wait <-bg) and error joining (a ZoneStatsLoop failure is not hidden behind nil); a typo in the OnPass comment. Spec: §5.9, §10.3, §8, §6.1, §3.4.
- **T-081** [done] — `crawlerctl domain <name> [--json] [--content]`: input normalization in any form (unicode/punycode, case, dot, spaces — internal/idn per spec 5.2, before touching the DB; an incorrect name — exit code 2); the single data source — the `domain_info(text)` SQL function (T-082, a single point of truth with psql), the CLI decorates the response and unpacks the content; the human-readable summary: the zone (id/unicode), the status and check_state with the names from spec 6.2, last_checked_at, queued_at/temp_attempts/flags, the dns_mask decoding by catalog labels, the results table by subdomains×variants (the scheme, the HTTP status, the final URL, an alias ≡/→, the content DOM/HTML, the error with the internal/httpworker class, the check time) and Laravel: the score with the threshold from the config, the version range, found_on, the rules version and all findings with rules and evidence fragments, weak ones (is_laravel=false) — in a separate section (domains with only weak matches have no laravel_domains row and are not revealed by the function — the T-082 contract, reflected in help); `--json` — the raw jsonb pretty-printed, for "not found" — `{"found":false,…}`+exit code 1; `--content` — stdout gets only the bytes of the chosen record as is (the rendered DOM has priority, otherwise the first record with content in subdomain×variant order — the decision pinned), the summary and the record note — to stderr (a redirection to a file), the reader — PackStore+SetDictFile without writing (the analyzer T-071 style), `--json`+`--content` incompatible (exit code 2); "what exactly was not found" — a second light query to zones (the longest suffix of the name, as the function cuts it): "in zone X there is no domain with label Y" / "zone Z is absent from the catalog"; compose: tools has var/content mounted (for --content via make domain); the domain_test.go tests — the SQL contracts (domain_info + zones), a fake pool/store, form normalization, the content choice, the summary/JSON format, not-found, routing/codes; verified by a live run in the isolated t081_smoke schema (migrations + data + a real PackStore round-trip). Spec: §5.11, §11.4 (item 10), §5.2.
- **T-082** [done] — The `domain_info(text)` SQL function for querying directly from the DB (migration 000006 + a down `DROP FUNCTION IF EXISTS`): a domain summary as one jsonb — `zone` {id, name, name_unicode}, `domain` {status/status_name and check_state/check_state_name — the codes + names from spec 6.2/internal/model, last_checked_at, dns_mask, temp_attempts, is_idn/render_pending, queued_at}, `dns_alive` — the dns_mask decoding over subdomains, `results` per (subdomain, variant): scheme/http_status/final_url/alias_kind(+name, the alias label)/rendered/error/the content_hash hex/checked_at (content is not revealed — only the hash reference, unpacking is the CLI's job), `laravel`: the laravel_domains aggregate (score, ver_min/ver_max, found_on_mask + the found_on decoding, rules_version, updated_at; json null without signs) and all laravel_findings by descending score — including weak ones below the threshold with the is_laravel mark, signals as is; jsonb instead of TABLE — a composite result of variable depth is read from psql as one value; input normalization — the SQL minimum (lowercase, edge spaces and dots, §5.2 item 1), the full IDN pipeline (punycode, lengths, inner spaces) — the Go side (internal/idn, crawlerctl T-081), the function expects punycode-normalized input; the zone — the longest suffix of the name from the zones catalog (an analog of PSL separation, composite co.uk preferred over uk), the domain — by UNIQUE (zone_id, label) without a full scan; a domain/zone not found — NULL, not an error; the function only reads: STABLE, PARALLEL SAFE, SECURITY INVOKER, search_path pinned to the function (pg_catalog + the migration schema; an ALTER after CREATE — the schema name at migration time is unknown, the tests apply the set into isolated schemas); the contract — test/integration/schema_test.go TestSchema_DomainInfoFunction (the pg_proc attributes, the JSON composition over test data, the findings sorting, the co.uk suffix against uk, the NULL cases, empty domains), the SQL additionally verified by a live run of the migrations and calls in a throwaway isolated schema; documentation — docs/schema.md §"The domain_info function". Spec: §5.11, §5.2 (item 1), §6.2.
- **T-083** [done] — Subdomain management `crawlerctl subdomains` (spec 5.10, 13.1): `list` — a "bit/label/enabled" table ("''" shown as "(apex)", the total "N of 63, M enabled"); `add <label>` — inserting an enabled=true row with the free bit `max(id)+1` (there is no sequence in migration 000001 — the CLI assigns the bit, PK/UNIQUE guard integrity, a race of two adds = an insert error), the bit range 0..62 is checked before the insert with `model.ValidateSubdomainID` (a maximum of 63 subdomains, bit 63 is the sign bit), a duplicate label — an error with the bit and state, no data migration needed (spec 13.1); `disable <label|bit>` — `enabled=false` with one UPDATE by id (the argument is first dereferenced as a label — digits can be labels, then as a bit number), the bit and the accumulated masks/results untouched, a repeat — an idempotent "already disabled", the apex (bit 0) is protected — it is the basis of the domain check (spec 5.4 item 1: the NXDOMAIN branch and the wildcard detection); the NEW `recheck <label> [--zone NAME]` — starting a check of a subdomain over already checked domains (spec 13.1): the mechanism — moving `check_state done → pending` in batches of 5000 with one UPDATE … FROM (SELECT … LIMIT) in the style of the daily recheck T-030 (the `status <> deleted` guard in the subquery and the outer WHERE, `queued_at = NULL`, ORDER BY by PK), the batches repeat until a partial one (a drain, the affected counter); publishing to q:dns is done by the scheduler (backpressure + lease — the pipeline is not duplicated), and the DNS stage takes the subdomains from the catalog itself (T-039) — domains in pending/queued are checked with the new subdomain anyway; the subdomain must be in the catalog and enabled, the zone — `analyzer.ZoneIDByName` (not found — an error), Ctrl-C interrupts the drain with a partial outcome in the error, repeating the command continues (the move is idempotent); the config not extended (the batch size — a CLI-level constant, like the T-030 defaults); tests — cmd/crawlerctl/subdomains_test.go (the SQL contracts, the semantics on a fake pool: bit assignment, the 63 maximum, a duplicate label, the digital-label priority, the apex guard, the move with the deleted guard/zone filter/batches of 5000+a partial one, errors, the codes via run) + the main_test.go cases; docs/dev-environment.md §6, docs/services.md (the mechanism at dns-worker). Spec: §5.10, §13.1, §5.3, §5.4.
- **T-084** [done] — Zone management and manual tasks of `crawlerctl` (spec 4.1, 5.3, 13.1): `zones list` — an "id/zone punycode/unicode/enabled/total/checked" table (ORDER BY name, coalesce name_unicode → "—" for ASCII zones, the domains_total/domains_checked counters updated by monitor T-078 — list only displays; the total row "N zones, M enabled, domains …"); `zones enable|disable <zone>` — the `zones.is_active` flag by name (`idn.NormalizeZone` before touching the DB; a lookup by UNIQUE name → an UPDATE by id with RETURNING of the row; a repeat — an idempotent "already enabled/disabled", the subdomains disable T-083 style; an unknown zone — exit code 1 with a zones list hint): the T-027 DispatchBatch joins zones with the `z.is_active` condition, so enable returns the zone into the scheduler selection automatically (without a datafix — the domain states were not changed), disable pauses the check (domains stay pending) and does NOT touch domains/statuses/results (spec 13.1), ingest file intake works as before, updated_at — monitor's ownership (untouched); `recheck <domain> [domain …]` — forced scheduling for a check: the names are normalized by `idn.Normalize` (the domain T-081 style), the zone — the longest suffix of the name from the zones catalog (a LATERAL LIMIT 1 by descending length — like domain_info T-082: PSL does not know test zones like crawl.test, co.uk preferred over uk), the label — the name's prefix `substr(name, 1, length(name)−length(zone)−1)`; the mechanism — moving `check_state → pending, queued_at = NULL` (the decision pinned per the T-028/T-030 semantics, consistent with subdomains recheck T-083: publishing to q:dns is done by the scheduler — it owns the backpressure and the lease, the CLI does not duplicate the pipeline; pending will be picked up by the T-027 DispatchBatch); the resolution and the move — one batched query each (unnest of names with WITH ORDINALITY + LEFT JOIN of zones/domains — the not-found ones are not lost, the argument order kept; UPDATE … FROM unnest of the PK keys with the `status <> deleted` guard at lock time and RETURNING of the actually moved ones — a race with a T-019 delete file lands in a refusal); the semantics by states: done → pending — the main case, pending → pending — an idempotent no-op, queued → pending — a manual lease return (T-029: the task may have been published — the domain will be checked twice, at-least-once/idempotent records allow it, spec 3.4); deleted — a refusal with a message (spec 13.1), not-found ones (no zone / no label in the zone — the "what exactly" in the T-081 style) — a per-line refusal, exit code 1 with at least one not scheduled (the scheduled ones are moved — a partial outcome in the output, duplicate arguments converge into one move), a disabled zone — a move with the note "the domain awaits enabling (zones enable …)"; tests — cmd/crawlerctl/zones_test.go + recheck_test.go (the SQL contracts, the semantics on fake pools: the catalog/toggling/idempotency, the deleted guard and the lookup/UPDATE race, the co.uk>uk suffix, not-found, a disabled zone, duplicates, the codes/routing via run), the SQL verified by a live run in a throwaway isolated t084_smoke schema (minimal zones/domains: the suffix lookup, co.uk against uk, the move with RETURNING, the queued_at reset — a substr-label defect caught and fixed); the command pool — the shared openManagePool (formerly openSubdomainsPool, pool_size=2 — the spec 6.3 budget); the framework notImplemented/summarizeNames removed (all commands implemented), main_test.go moved from stubs to functional cases; documentation — docs/dev-environment.md §6 (zones/recheck fully, the subdomains style). The AGENT.md review scheme was NOT run: agy claude-sonnet-4-6 — RESOURCE_EXHAUSTED (quota), gemini-3.8-flash-high — timeouts, grok-4.7 — the Grok Build limit, cursor-agent — a usage limit; the fact did not make it into the commit (the branch was merged by the main session), the compensation — a live SQL run and self-verification; make test/lint green, the main session's control test/lint/test-integration in master — green. Spec: §4.1, 5.3, 13.1.

## Stage 9. Deployment and OS tuning

- **T-085** [done] — The systemd unit set of the production deploy — a new `deploy/systemd/` directory (8 service units + `crawler.target` + OS package drop-ins + `crawler.env.example`; the instructions — a new `docs/deploy.md`). The deployment method pinned per spec §10.1: **native** — PostgreSQL/Redis/Unbound as OS packages (the budget via `postgresql@.service.d`/`redis-server.service.d`/`unbound.service.d` drop-ins: MemoryMax 2560M/512M/700M + Restart=on-failure), the Go services — static binaries (CGO_ENABLED=0 -trimpath -s -w, like Dockerfile.render) in `/opt/crawler/bin`, Chromium — the headless-shell distribution in `/opt/crawler/chromium` (the same build as CHROMEDP_HEADLESS_IMAGE; the Ubuntu package is a snap, unusable); docker compose remains the dev environment, the T-065 docker-runtime profile — the docker variant of render for Docker hosts (the link pinned in docs/docker.md §6). Per service: MemoryMax per the spec 3.2 budget — the breakdown of the "Go services ≈1.3 GB" line over seven units: http-worker 384M (up to 64 tasks × bodies ≤2 MiB until the end of the domain task, the final-URL cache, the writing store; parity of the app dev profile), dns-worker 256M (a pool of 2000 UDP sockets, a TTL cache), ingest/scheduler/render-worker/analyzer/monitor 128M each (batched DB work without heavy buffers; render-worker — only the Go part, Chromium separately), the sum 1280M, the remainder of the line ≈50M — the crawlerctl CLI (it has no unit per spec 4.1, episodic runs by the operator); chromium 2048M; in total 7100M of 8192, the remainder — the OS/page cache. Restart=on-failure + RestartSec=5s + StartLimitIntervalSec=300/StartLimitBurst=10 (exhaustion — failed until `systemctl reset-failed`: protection against an infinite crash loop, a one-off crash restart does not hit the limit). Separate users — static system `crawler-<service>` (nologin) in the shared `crawler` group; the decision against DynamicUser is justified (docs/deploy.md §3): the content store is written by two units (http/render-worker) and read by analyzer/crawlerctl — stable UID/GID are needed + a setgid directory 02775, the incoming/ archive readable to the operator between runs. LimitNOFILE=65535 — parity with the dev-compose ulimits, the sysctl caps — T-088; WorkingDirectory=/var/lib/crawler; EnvironmentFile=/etc/crawler/crawler.env (0640 root:crawler: CRAWLER_CONFIG, the DSN secret, redis, resolvers, render.remote_url, the production UA with real contacts — spec §9); METRICS_ADDR=127.0.0.1:9101…9106 in the units (monitor — monitor.listen in the config); a hardening set in every unit (NoNewPrivileges, ProtectSystem=strict + ReadWritePaths by roles (incoming — ingest, content — http/render), PrivateTmp, CapabilityBoundingSet=, MemoryDenyWriteExecute/SystemCallArchitectures=native at the Go units; chromium softer — V8 JIT and AF_NETLINK, MemoryDenyWriteExecute would break the browser). Chromium as a separate `crawler-chromium.service` unit (a decision): the §3.2 budget line is separate — a cgroup-OOM kills a tab within the unit and it restarts, without touching the worker and PG; the worker in the remote mode `ws://127.0.0.1:9222` (the dev app profile contract: a consumption pause at a dead browser, docs/render.md); the remote-mode compensation — restarts by pages/RSS from inside (T-065) are impossible for a foreign process, the cap and the restart are held by the unit (an analog of the dev chromium service's mem_limit). Dependencies: After+Wants on postgresql/redis-server/unbound (best-effort package meta-units) without a Requires link — stopping the infrastructure does not stop the workers (the pgx pool reconnects, a Redis loss is recovered by the scheduler — spec 3.4), DB readiness at start closes the restart loop; migrations at start are applied by the first service that comes up (T-002, an advisory lock) — there is no separate step. docs/deploy.md: the ratio of the methods (a table), the budget with rationale, the users/directories and permissions, the step-by-step installation (packages+drop-ins, the role/DB/pg_stat_statements with psql commands — an analog of deploy/docker/postgres/init, unbound.internet.conf with interface 127.0.0.1, building the binaries, headless-shell, users/directories, config.yaml edits: the /var/lib/crawler/* paths, remote_url, monitor.listen 127.0.0.1:9100, dns.ipv6, the UA; resolv.conf → 127.0.0.1 so that render goes through unbound), enabling with enable --now, operations (journalctl -t, healthz, the analyzer SIGHUP, reset-failed, memory show -p MemoryMax, the §11.4 item 7 criterion — the daily run of T-093, the 9100…9106 metrics), the T-086…T-089 boundaries (the PG/redis/sysctl configs, the monitor's SMART access). README (the structure: deploy/systemd) and docs/docker.md (a link to deploy.md). Checks: `systemd-analyze verify` on the dev host — the syntax/directives clean, with stub binaries in /opt/crawler a full exit 0 (without them — the expected "not executable" warnings on a non-target host); COMPOSE_PROJECT_NAME=crawler make test / make lint green (no Go code changes). The AGENT.md review scheme was NOT run — all agents unavailable: agy claude-sonnet-4-6 — RESOURCE_EXHAUSTED (quota, a reset in ~132 h), agy gemini-3.8-flash-high — two 240 s timeouts, grok-4.7 — the Grok Build limit, cursor-agent (auto) — a usage limit; the compensation — a detailed self-check: the systemd-analyze verification of the units (exit 0), cross-checking the units against docs/configuration.md (the CRAWLER_*/METRICS_ADDR variables, monitor.listen), config.example.yaml and deploy/docker/compose.yaml (ports, paths, limits), a self-read of docs/deploy.md with edits (the sudo rights of the installation commands, the wording, typos). Spec: §10.1, §3.2, §11.4 (item 7), §4.1, §3.4.
- **T-086** [done] — Tuning the production PostgreSQL for NVMe and the crawler's load: a new `deploy/postgres/postgresql.conf` (PostgreSQL 16 from OS packages, spec §10.1 — a systemd host, not a container; connecting via `config_file=` to the cluster, the resource cap — `MemoryMax` ≈ 2.5 GB from T-085) + a docs/db.md section "Production PostgreSQL tuning" with the rationale and trade-offs of every parameter. Planner/NVMe (§6.3): `random_page_cost=1.1` (NVMe random reads are almost no more expensive than sequential, the 4.0 default unjustly penalizes index scans under the point-indexed load), `effective_io_concurrency=200` (spec ≥200, read-ahead of partition bitmap scans), `maintenance_io_concurrency=200` (VACUUM/index build prefetch), `default_statistics_target=100`. Memory (§3.2: shared_buffers ≈ 2 GB + backends, the whole instance ≈ 2.5 GB): `shared_buffers=2GB` (25 % of RAM, the rest — the OS cache), `work_mem=16MB` (a plan node × query; worst case ~30 active × 16 MB ≈ 0.5 GB — the backends budget; bulk work — COPY/unnest batches without work_mem), `maintenance_work_mem=256MB` (CREATE INDEX/migrations over partitions of 30–50 million rows, one-off), `autovacuum_work_mem=128MB` (a separate cap for the vacuum workers: 4 × 128 MB and it does not compete with migrations for maintenance_work_mem; 128 MB of a TID array ≈ 20 million dead rows — with the 0.02 trigger realistically an order of magnitude less, on overflow the vacuum degrades to a page-by-page pass, memory does not grow), `effective_cache_size=4GB` (a planner hint: buffers + page cache, not allocated). Connections: `listen_addresses=localhost` (all services on one server §3.1, not exposed outside §9), `max_connections=50` — the top of the §6.3 "20–50" budget: 7 services × a pool of 5 + crawlerctl (2) + an admin ≈ 40–45, 3 slots — superuser_reserved_connections. WAL/reliability (§6, §3.4): `synchronous_commit=off` — the §6 prescription for crawler data, the trade-off pinned: on server loss the loss window is ≤ ~3×wal_writer_delay ≈ 0.3 s, all writes are idempotent and recoverable by re-scheduling/re-checking (§3.4; §6.3 backups are not required), losing a result = an extra check, not data loss; migrations — the DDL and the version in one commit, a lost commit rolls back both the marker and the schema, a repeated start will apply again; `wal_writer_delay=100ms` (a window half the default, 10 background fsyncs/s — nothing for NVMe), `wal_compression=on` (compressing full-page images → fewer writes and wear §10.3), `checkpoint_timeout=15min` + `max_wal_size=8GB`/`min_wal_size=2GB` + `checkpoint_completion_target=0.9` (fewer checkpoints → fewer full-page writes and NVMe TBW, "plenty of free space" §3.1; the trade-off — crash recovery replays ≤ 8 GB of WAL, minutes on NVMe, acceptable for recoverable data). Autovacuum for the domains/subdomain_results partitions (30–50 million rows, frequent UPDATEs of check_state/last_checked_at — HOT at fillfactor 85, the initial ingest of 350 million via COPY, mass done→pending moves): the decision — config-level settings only, per-table ALTERs are NOT used in migrations (partitions are created dynamically by migration 000003 per zones/hash — the values would have to be duplicated in each; the config covers all existing and future ones at once and is portable between servers): `naptime=30s` (thousands of leaf partitions, with the default 60 s the distant ones wait for a worker for cycles), `max_workers=4` (the default 3 — a job queue; memory bounded by autovacuum_work_mem), `vacuum_scale_factor=0.02` (a trigger of 0.6–1 million dead rows per partition against 6–10 million at the 0.2 default — bloat squeezed to ~2 %, vacuums short, 4 workers keep up; with fillfactor 85 pages quickly return to circulation for HOT), `insert_scale_factor=0.02` (PG13+: COPY creates no dead rows, but without a vacuum the visibility map is not set — the index-only scans of the partial domains(check_state=0) index degrade and freezing does not happen), `analyze_scale_factor=0.01` (the check_state distribution flows pending→queued→done, the planner's partial-index plan depends on it; ANALYZE is cheap — a sample of ~30 thousand rows), `vacuum_cost_limit=2000` (the default 200 credits, divisible among workers with a 2 ms tick, throttles the vacuum over thousands of partitions — it cannot keep up with the update pace; 2000 ≈ 125 thousand pages/s/worker — NVMe digests it). Monitoring §5.9: `pg_stat_statements` (monitor queries), `track_io_timing=on` (I/O — assessing the tuning effect), `track_functions=pl` (domain_info T-082); logs from warning, UTC. The dev stack untouched: compose.yaml unchanged, the Docker environment (incl. the production runtime profile) keeps using `postgresql.dev.conf` — the planner/WAL/autovacuum semantics match, the volumes are dev-scale (mem_limit 1536m, max_connections 60); the production PG — an OS package §10.1; the integration tests work with the same dev server in isolated schemas and are unrelated to the production settings. Checks: the config's format and contract values are guarded by a new unit test `deploy/postgres/postgresql_conf_test.go` (the GUC syntax "name = value", duplicates, quote stripping/trailing comments, parsing sizes kB/MB/GB; a table of the mandatory spec values + ranges: io_concurrency ≥ 200, the autovacuum factors below the PostgreSQL defaults, the §3.2 budget corridors, max_wal_size ≥ 4 GB) — the package is included in GO_PKGS and the go/fmt Makefile path as `./deploy/postgres`, not `./deploy/...` (deploy/docker/Dockerfile.go — a Dockerfile with a .go extension, not parsed by the go toolchain); the config verified by a live launch of PostgreSQL 16.15 in a one-off container (initdb + `postgres -c config_file=` + SHOW of all parameters — the values matched, no errors; the main docker stack untouched); a static cross-check: 31 parameters, no duplicates, the config/test/doc consistent. The AGENT.md review scheme was NOT run — all agents unavailable: agy claude-sonnet-4-6 — RESOURCE_EXHAUSTED (quota, a reset in ~132 h), agy gemini-3.8-flash-high — two timeouts (interrupted), grok-4.7 — the Grok Build limit, cursor-agent — a usage limit; the compensation — a live SHOW verification, a unit test of the value contract and a self-check. `COMPOSE_PROJECT_NAME=crawler make test`/`make lint`/`make fmt` green. Spec: §6.3, §6, §3.2, §3.4, §10.1, §10.3, §5.9.
- **T-087** [done] — The Redis configuration: `maxmemory` ≤ 0.5 GB, an eviction policy accounting for queue recoverability: the production config `deploy/redis/redis.conf` (an OS package under systemd, spec 10.1; the units — T-085) — `maxmemory 512mb` (the §3.2 budget) + `maxmemory-policy noeviction`, the choice pinned with trade-offs: volatile-* degenerates into noeviction (there are no keys with TTL), allkeys-lru silently deletes stream keys entirely — together with the groups and the PEL (an empirical check on redis:7.4, 30,000 XADD, maxmemory 2mb: all records accepted without errors, but on reaching the limit the key was deleted entirely — the first ~17,000 records and the group created before the flow disappeared, the next XADDs recreated the stream without a group); noeviction accepted 17,211, it rejected the rest with an OOM error — the accepted ones intact, the group in place; a loud write refusal agrees with the "Redis — an expendable transport" architecture (a loud OOM → Warn loops live, recovery via the T-029 lease, the T-032 integration test), silent corruption — no, trimming is already guarded by MAXLEN (T-025), overflow — by the backpressure gate (T-026) long before the cap; persistence off (`save ""`, `appendonly no`): losing Redis — a regular spec 3.4 scenario, AOF gives no "in flight" guarantees (everysec loses a second) and costs fsync/NVMe wear (§10.3), the q:dead history after a restart is re-published by the lease pass; network: `bind 127.0.0.1 -::1` + `protected-mode yes` (the services on the same server, no password in the services' config), `tcp-backlog 511`, `timeout 0` + `tcp-keepalive 300` (long-lived XREADGROUP connections), `io-threads 1` — the load is hundreds of commands/s, enable per the §11.3 measurements, `supervised auto`/`daemonize no`/logs to journald; the dev stack unchanged (redis.dev.conf 224mb < mem_limit 256m — consistent, there is no production redis profile in compose — a systemd deploy); documentation — a new `docs/redis.md` (the data in Redis, three levels of memory protection, the eviction trade-offs, persistence, the MemoryMax≈640M recommendation with a margin over maxmemory for T-085, the verification method with a one-off container) + a link in README. Spec: §3.2, §3.4, §4.2, §10.1.
- **T-088** [done] — Tuning the production server's Linux kernel for one IP (spec §10.2): `deploy/sysctl/99-crawler.conf` — a drop-in for `/etc/sysctl.d` (applying `sysctl --system`, persistence via systemd-sysctl; the instructions, the check and the rollback — `deploy/sysctl/README.md`), the calculations — `docs/os-tuning.md`; the calculation from the config's planned load (§5.10/5.4/5.5): dns.concurrency=udp_pool_size=2000 (the internal/dnsclient UDP socket pool, one query in flight per socket, no EDNS0 → datagrams ≤512 B, a TCP fallback on TC=1 ≤2000 concurrent to the local unbound), http.concurrency=GlobalConns=2000 + max_rps=1000 + DisableKeepAlives (spec 5.5 "keep-alive disabled") → a connection per request, churn up to ~1000/s; the parameters: ip_local_port_range=10240 65535 (55,296 ports against the default 28,232: 2000 active + up to ~60,000 TIME_WAIT (churn×2×MSL 60 s); the 4-tuple is unique — the hard per-destination requirement is lower; the lower bound 10240 moves all local listeners — sshd/unbound/postgres/redis/the metrics 9090+/DevTools 9222 — out of the conflict zone with outgoing connections at service restarts), tcp_tw_reuse=1 (kernels ≥4.12: the default 2="loopback only", the HTTP worker goes to remote hosts; tcp_timestamps=1 pinned explicitly — the PAWS condition; tcp_tw_recycle removed from the kernel — not applied), tcp_max_tw_buckets=262144 (a peak of ~60k ×4, an overflow makes noise in dmesg), somaxconn/tcp_max_syn_backlog=4096 (listeners: unbound TCP 53 accepts a TCP fallback ≤2000, the metrics, postgres), netdev_max_backlog=4096 (RX peaks of downloading bodies up to 2 MiB × 1000 RPS, drops visible in softnet_stat), fs.file-max=2097152 (a peak of ~11,600 fds per the services table in docs: dns-worker ~4100 = a pool of 2000 + a TCP fallback 2000 + pools, http-worker ~2200, unbound ~2100, Chromium ~1500, DB/system; the value — a final guard cap against a descriptor leak two orders above the peak, not a "budget": file-max is a system-wide kill-switch, ENFILE takes down sshd/postgres together with the crawler, lowering it to ~the peak is impossible; on new kernels the default is LONG_MAX, memory — only for live struct file), nf_conntrack_max=1048576 + nf_conntrack_buckets=262144=max/4 (a churn of 1000/s × the conntrack TIME_WAIT timeout of 120 s ≈ 120,000 entries steady state + active ones + the pool's UDP flows; the default on an 8 GB machine is 262,144 without a margin, an overflow drops the packets of all connections; buckets writable since kernel 5.15, on older ones — hashsize in modprobe.d or a hot write into /sys/module/nf_conntrack/parameters/hashsize (unloading the module on a live server is impossible); loading the module via /etc/modules-load.d is mandatory — systemd-modules-load runs before systemd-sysctl, otherwise the net.netfilter.* keys will be lost at boot; the keys require the loaded module — systemd-sysctl skips them with a warning; the spec's "exclude the traffic" alternative — NOTRACK by --uid-owner http-worker: described with caveats — it breaks a stateful firewall and NAT/Docker, the main path is the table), rmem_max/wmem_max=16 MiB (the setsockopt cap without CAP_NET_ADMIN — primarily for unbound so-rcvbuf at high QPS and a future pool SetReadBuffer), rmem_default/wmem_default deliberately not set — commented out in the config with rationale (one query in flight per pool socket × datagrams ≤512 B — a ×400 margin; explicitly pinning the kernel default would override the distribution's/admin's tuning — sysctl.d is applied in lexicographic order); the TCP fallback 64 KiB and the HTTP bodies 2 MiB — to the tcp_rmem/tcp_wmem autotuning, the default max of 4–6 MiB is enough; deliberately not set (a table in docs): tcp_tw_recycle (removed), tcp_mem/udp_mem (an autotune by RAM, bodies are read streaming), ip_local_reserved_ports (the lower bound 10240 decides), tcp_fin_timeout, tcp_syncookies; nofile per process — not a sysctl: the calculation (a maximum of dns-worker ~4100 → a ×16 margin) and LimitNOFILE=65535 are pinned in docs/os-tuning.md, set by T-085 in systemd (already present in Docker in the compose ulimits), fs.nr_open untouched; monitoring after applying (ss time-wait, conntrack_count, file-nr, dmesg, softnet_stat) — in docs and README; docs/dev-environment.md §9 moved from duplicating ad-hoc values to the canonical file; README — a link to docs/os-tuning.md and the deploy/sysctl line in the structure. The AGENT.md review scheme: round 1 agy claude-sonnet-4-6 — RESOURCE_EXHAUSTED (quota, a reset in ~132 h), the fallback round agy gemini-3.8-flash-high was run — 6 remarks, all accepted and fixed (the port range arithmetic 53,296→55,296 — 65535−10240+1; fs.file-max 262144→2097152 — a final guard cap, not a "budget": lowering a system-wide kill-switch is dangerous, on new kernels the default is LONG_MAX; rmem/wmem_default commented out — explicitly pinning the kernel default would override the distro tuning; loading nf_conntrack via /etc/modules-load.d — otherwise the early systemd-sysctl at boot would skip the keys and the module would come up with defaults; a hot hashsize on kernels <5.15 — a write into /sys/module/nf_conntrack/parameters/hashsize instead of an unrealistic module unload; the rollback reworked — sysctl -w primary, reboot as a guarantee, a kernel defaults table added to README); round 2 grok-4.7 — the Grok Build limit, cursor-agent (auto) — a usage limit: both unavailable, the round was not run (the fact here and in the commit), the compensation — a final read-through of all files and cross-checking the key syntax against /proc/sys of the live kernel (reading). make test/lint green (no code changed: a configuration and documentation set). Spec: §10.2, §5.4, §5.5, §5.10, §3.1.
- **T-089** [done] — Disks and NVMe wear monitoring (spec §10.3): a new `docs/disks.md` + a `deploy/smartd/` set (smartd.conf, a smartd-warning.sh warning handler → syslog/journald, a 99-crawler-nvme.rules udev rule + a crawler-monitor.service.d/90-nvme-smart.conf drop-in, a README of installation/check/rollback). The layout: two LVM logical volumes on the single NVMe (§3.1) — lv_pg → /var/lib/postgresql (the packaged PG's PGDATA+pg_wal; WAL not separated: on one device wear-leveling wear is not shared, the volume is bounded by the max_wal_size 8 GB from T-086) and lv_content → /var/lib/crawler/content (pack files); the LVM-against-partitions choices pinned (online-resize for unpredictable content growth, pvmove/adding a PV at a second NVMe, the Ubuntu installer's standard) and ext4-against-XFS (packs are written strictly sequentially by a single writer under flock — docs/content.md, XFS parallelism not exercised; ext4 can online-shrink, the distribution default with a packaged PG), noatime on both volumes, periodic fstrim kept; an honest pinning: on one NVMe separate volumes do NOT reduce wear (LBA ranges do not affect wear-leveling) — the gain is operational: fill isolation (filling the content volume → the regular Put pause per content.disk_free_percent/spec §8 with the source of truth alive, not a PostgreSQL crash on a shared volume), an exact statfs for the Put pause and the disk_free alert (T-080), independent options, readiness for a second NVMe; the permissions consistent with T-085 (postgres:postgres 0750 on the PGDATA root, root:crawler 02775 setgid on content), the installation order — volumes before apt install postgresql (the package cluster is created at the mount point); the size calculation from the planned volumes: PG ≈ 0.2–0.3 TB (domains/subdomain_results/contents/laravel_* with indexes, fillfactor and bloat) → a 0.5–1 TB volume, content ≈ 7–20 TB of live blocks for the first cycle → the rest of the disk, the 4/8 TB and 2×4 TB layout examples. The SMART/TBW monitoring — two layers with a single journald channel: smartd from the package (the config: DEVICESCAN -d nvme — the Debian/Ubuntu builds exclude NVMe from the regular scan; -H critical warning — including available spare below the threshold and read-only; -l error an error log growth; -W 4,45,60 Composite Temperature; warnings without mail (-m <nomailer>: there is no MTA, §5.9 does not require mail) — the /usr/local/lib/crawler/smartd-warning.sh handler writes a structured record tagged smartd; smartd itself duplicates into syslog LOG_CRIT/INFO — "what smartd sends"), picked up by the operator: journalctl -t smartd (documented in deploy.md §5); layer 2 — the monitor's nvme_wear alert (T-080, percentage_used ≥ 90 % + TBW in the details, a 60 s period) is enabled by the crawler-monitor unit's access to /dev/nvme*: udev gives the controllers to the dedicated nvme-smart group 0660 (a separate group instead of disk — access exactly to the NVMe nodes; write permission on the node — the kernel's requirement for admin ioctls, in fact read-only), the drop-in adds SupplementaryGroups; the version mismatch pinned: smartmontools 7.4 (Ubuntu 24.04 LTS) has no percentage_used threshold for NVMe (-f/-p — experimental 7.5 features, attached as a commented line), therefore on 7.4 the monitor layer is mandatory for spec §10.3, on 7.5+ it duplicates; the layers' role separation: smartd as root catches the drive's death regardless of the crawler, monitor ties the wear to the other §5.9 alerts. The TBW estimate under the load (docs/disks.md §5): the PG volume's writes ≈ 0.5–1 TB for the first cycle (the ingest WAL 45–70 GB + 2–3 status updates per domain ~100–160 GB + results ~100–170 GB, FPI ×1.3–1.5, the double write of WAL+checkpoint; synchronous_commit=off cuts fsyncs, not bytes) and ~3–6 GB/day in steady state (a 6-month cycle), content: filling 5–15 TB + the recheck churn of 2–8 TB/half a year (unchanged pages are not written — the Put dedup) + compaction ×1.5; in total ~10–30 TB/year against 600–2400 TBW consumer NVMe — a margin of 1–2 orders, the control left mandatory per §10.3 (real pages are fatter, OEM drives with a small TBW, actual wear ≠ calculated). Documentation: a new docs/disks.md (the layout/installation fresh+migration/check, the TBW estimate, the SMART layers), references from deploy.md (§4 the disk order + smartmontools in the packages + §4.8 SMART + §5 journalctl -t smartd + §6 T-089), db.md (the §10.3 WAL lines + a summary toward the TBW estimate), redis.md (persistence/wear), os-tuning.md §8, services.md and the monitor unit's comment and the internal/monitor/alerts.go header (updating "stage 9 T-089" → ready layers), README (the docs list + the deploy/smartd line). Checks: the smartd config validated by a live smartd 7.5 on the dev machine with NVMe (smartd -d -q onecheck -c smartd.conf: the basic line and the 7.5 variant with -f/-p — the device registers, exit 0; the host machine's smartd service stopped/disabled after the check), the handler — a run with real SMARTD_* variables (a journald record, exit 0, silence in stdout), sh -n; the udev rule — udevadm verify (clean with the existing nvme-smart group, the syntax valid); COMPOSE_PROJECT_NAME=crawler make test/lint — green. The AGENT.md review scheme: all agents unavailable — agy claude-sonnet-4-6 (RESOURCE_EXHAUSTED), agy gemini-3.8-flash-high (a timeout), grok-4.7 (a limit), cursor-agent (a usage limit); the compensation — a self-check.

## Stage 10. Testing and acceptance

- **T-092** [done] — Load measurements on the target server per spec §11.3/§13.2/§3.3. Done (2026-10-22): the real ru/rf/su lists of 7,157,975 domains (the customer) fed to production as .gz archives (the T-133 ingest, the T-132 extraction; ru 34 thousand rows/s, rf 30, su 22; a rejection share of ≤0.05% — almost all 52 of su's "rejections" are PSL geographic suffixes like nalchik.su — correct behavior). Measured (docs/load-measurements.md + the deploy/loadtest/{t092-sample.sh,t092-analyze.py} kit, the sampler under systemd-run): DNS ≈210 domains/s ≈760 QPS unbound (a mean of 9.5 s/task, p50 0.8 s; 3.6 requests/domain; max_qps=10000 not reached); HTTP — the pipeline's bottleneck ≈1.9 domains/s ≈6 requests/s at max_rps=1000: the consumption window barrier (Consume=16, a wg.Wait over the whole window) + the hard-wired task parallelism (Concurrency=64/PerDomain=8, not overridden by the config — http.concurrency acts only on the transport) + head-of-line parking IPs at per_ip_limit=4; the backpressure couples the whole pipeline to the HTTP pace (q:http at the watermark of 100K → dns-worker does not publish). Render 5.1 s/page, ≈0.5 pages/s (contexts=3, the same barrier); the analyzer 12.9 ms/record. Memory within the spec 3.2 budget: the Go services ≈440 MiB avg/550 peak, Redis max 465 MiB, unbound 459 MiB, PG at the MemoryMax cap of 3 GiB (the file cache), no OOM either by the kernel or the cgroup. Writing: the DB 285 KiB/s in steady state (≈150 KiB/domain), content 36 KiB/s (a pack of 52 MiB/3.2K blocks ≈16 KiB zstd). The first cycle of 350 million estimated: the HTTP stage ≈4.6 years at the current defaults (≈200–450 domains/s needed toward the spec's "1–2 weeks" target) → reworking the q:http consumption as task T-139; DNS ≈19 days; ingest hours. The run's findings: T-138 (an empty Content-Length:0 body loses content_hash — ToUTF8 nil on an empty body, a Warn "the record's body unavailable", ~4% of tasks), T-140 (headless-shell without fontconfig — a Skia FATAL crash loop, fixed operationally with apt install fontconfig+fonts-liberation+fonts-dejavu-core), a q:dead relic (yandex.ru/admin of the T-135 era) — runbooks §2. The run continues as the daily T-093 (the sampler writes metrics.tsv continuously). The deepseek review "acceptable with edits" — a P1 (losing TSV rows at [not set]/a curl failure: hold-last counters, a tolerant parse) and a P2 (a prefix match of metrics, the content volume's df, the numbers) fixed in the same commit; rejected — TSV rotation/versioning (the run is finite). Spec: §11.3, §13.2, §3.3.
- **T-093** [done] — The daily run on the target server within the memory budget without triggering the OOM killer (spec §11.4 item 7, §3.2; unblocked by T-135, the units' budget T-085, the load T-092). Done (2026-10-24), commit e9fed1c. The window 23.10 09:17:40 → 24.10 10:26:37 MSK ≈ 25.1 h of continuous full load — the daily interval covered with a margin; the boundaries: the end of the T-157 incident (right after it the 256→192 slots deploy) → the operational scheduler freeze for the q:render drain (docs/operations.md §5, 24.10); inside the window 7 regular deploys (graceful) and apt-daily-upgrade 06:08 (a scheduled OS maintenance, not a reboot). The result: `journalctl -k --grep oom` over the window empty — the only kernel OOM records are the boot's (the cgroup OOM of http-worker 23.10 05:48, CONSTRAINT_MEMCG) belonging to T-157 and lying before the window; the mandatory `--since` window pinned in the criterion commands of deploy.md §5 and operations.md (without it the journal shows the T-157 records). oom/oom_kill/NRestarts = 0 for all 14 units of the set (per-life counters — deploys recreate the cgroup; the through-proof over the window — an empty kernel journal + the peaks of all finished lives + the absence of OOM records in the units' journalctl -u, including pg/unbound/redis/chromium). The peaks within the budget: http-worker 851.2M/900M (MemoryHigh 850M), unbound 769M/800M (T-160), dns-worker 256.1M at the old cap of 256M before the 18:32 deploy (no kill — confirming the T-160 finding), analyzer 128M at the cap (page cache), postgres ≈3072M at the cap (page cache, regular §6), chromium 1.6G/2048M, redis 65M; the spec §3.2 cross-check held (Redis ≤0.5 GB; unbound ≈0.7 GB at the upper bound; PG ≈2.5 GB; Chromium ≈2 GB; the Go services: the cgroup sum avg ≈1.16 GiB + render@9109 ≈1.3 GiB against the "≈1.3 GB" reference, a one-off sum peak of 1.41 GiB — page cache and coinciding peaks, every unit within its MemoryMax). Observations outside the criterion: http-worker's MemoryHigh throttling episodic — 0.44% of samples above 850M (24/5434), the memory_high alert in bursts with clearings; the T-157 criterion "episodic, not continuous" met, after the T-163 deploy (a GC limit of 768 MiB, 24.10 00:02) the heap avg ≈747 MiB against 860–893 of the wave 21 saturation; PSI CPU some avg 88% (max 98.7%) — a chronic CPU starvation (T-158); MemAvailable ≥ ≈1.7 GiB over the window. The report — docs/load-measurements.md §13 (the methodology, the units table, the §3.2 cross-check, the observations); the closing pointers — §6/§8 there, deploy.md §5, dev-environment.md (the acceptance map item 7 — done), operations.md §4.4, os-tuning.md §8. The deepseek review (1 round): no P0/P1; the P1 "tasks.md not updated" rejected (statuses — the main session only, AGENT.md); the P2/P3s accepted (the per-life caveat of the counters, the --since window, the T-163 profile step, the alerts timeline, the sum's addends, the units) — added via amend.
- **T-090** [done] — An audit of the testbed against spec 11.2 and closing the gap (not duplicating what exists). The §11.2 audit table: NXDOMAIN ✓ (nxdomain-case — a zone without records, an authoritative NXDOMAIN with an SOA), wildcard ✓ (*.wildcard + the T-038/T-040 detection), a tar pit ✓ (mockserver 512 B/s, the T-049/T-060 anti-tar-pit), an infinite redirect ✓ (+a cycle), a giant body ✓ (Content-Length 1 GiB + the 2 MiB truncation), SPA ✓ (#root+noscript+a JS cookie), Laravel pages — PARTIAL: the generator wired testdata/laravel-samples with cookies/headers from headers.json (analysis over cookies is possible in e2e — a separate mockserver scenario is not needed), but the committed generator became stale with T-073: the corpus grew 4→13 samples without regeneration — 9 of 13 (including all versions except 8/11) did not resolve and were not served. Closed: serial 2→3; regenerating the zones and the Caddyfile with a one-off tools container in the worktree (--root /src, without restarting services — make testbed-gen in the worktree is forbidden): db.crawl.test +9 A records, the Caddyfile +9 snippets/18 vhosts (Set-Cookie XSRF-TOKEN/laravel_session/acme_session/remember_web_, Content-Type: application/json at laravel-9-json-api, Host/X-Powered-By at artisan serve) — the web/sites fixtures byte-identical (generator determinism); AFTER THE MERGE the main session must run make testbed-gen (restarting the live stack's coredns/testweb — the zones in the commit are already updated). The SiteCfg schema extended with mock_path (the scenarios.yaml field was previously silently dropped) — the "scenario→mockserver handler" contract is verified by a test. New unit tests cmd/testbedgen/main_test.go: TestScenariosCoverTZ — the §11.2 table over the real scenarios.yaml (+ a mock without mock_path is forbidden); TestSamplesCoverLaravelVersions — ≥7 positives, the major versions 5–12, each with a Set-Cookie of Laravel cookies; TestGeneratedArtifactsInSync — regeneration into an isolated root (a copy of scenarios.yaml + a symlink of the samples) and a byte-wise cross-check of the zones/zones.conf/Caddyfile/fixtures against the commit — insurance against repeating the T-073 desync (the CI of the zones depends on the committed generator); TestAddSamples — a mini corpus: a snippet with several Set-Cookies, a sample without headers.json, skipping a directory without meta.yaml. docs/dev-environment.md §4 — the "spec 11.2 scenario → where" table, the list beyond the spec, the generator's and the sync test's contract. make test/lint/fmt green; make test-integration in the worktree is forbidden — the main session will run it after the merge together with make testbed-gen. The AGENT.md review scheme was NOT run: all agents unavailable — agy claude-sonnet-4-6 RESOURCE_EXHAUSTED (an individual quota, a reset in ~131 h), agy gemini-3.8-flash-high — a 240 s timeout (2 attempts), grok — the Grok Build limit, cursor-agent — a usage limit. Spec: §11.2, §5.4, §5.5.
- **T-091** [done] — The end-to-end integration test `test/integration/e2e_test.go` (TestE2E_FileToLaravel; the neighbors' style: skipUnlessIntegration, an isolated migratedSchema-schema sample with the application_name label, Redis DB 15 with q:* cleanup before/after, incoming/content — t.TempDir; e2e*-prefixed helpers, the neighbors' common helpers reused): the full pipeline with library calls of the components, without launching service processes — (1) a real add file in incoming via ingest.Pipeline (T-021): the zone/domains new+pending, the ingest_files report (6 physical lines: a comment, an empty one, 4 domains), staging empty, the file in processed; (2) the scheduler DispatchBatch/EnqueueBatch (T-027/T-028): 4 domains queued, one q:dns batch; (3) consuming q:dns with the worker's processBatch pattern (LoadSubdomains → PendingDomains → dnsworker.ProcessDomain T-039, XACK after the write): all done, the lease reset, dns_mask = bit 0 (the samples have no www/admin records), the status new (the outcome belongs to the HTTP stage, §7), a q:http task per domain with the apex address; (4) RESTARTING PostgreSQL mid-pipeline (§11.2, the essence of 11.4 item 9) — restarting the shared stack's container from the test is impossible, an in-essence emulation: pg_terminate_backend of all the test's connections (the pool is labeled application_name in the DSN, a one-off connect-"killer" without the label; the role terminates its own backends — verified on the dev stack, foreign services untouched) after a guaranteed warm-up of ≥2 connections by parallel pg_sleep → ≥2 interrupted, the data/queues survived (the domain count, done, the q:http depth unchanged), the pool reconnects with a ping loop (dead connections are released by failed calls, pgxpool creates new ones lazily); (5) the HTTP stage after the failure (ProcessDomain + ContentSaver + Recorder, T-053/T-056/T-058): the domains alive/done/temp_attempts 0, one subdomain_results row each (the apex, variant 0, scheme https — the samples answer identically over http/https, spec 5.5 item 5), the content in PackStore reads back (a round-trip by title), q:analyze per record per domain, q:render empty (non-SPA); (6) analyzer.ProcessResult (T-071/T-072) per each q:analyze delivery; the §11.4 item 5 verification against the testdata/laravel-samples expectations (meta.yaml, a score tolerance of ±0.001 under float4): the list of domains with Laravel = {laravel-11-vite, laravel-8-mix} by a selection from laravel_domains (a JOIN domains), findings — the full composition of matches (7/6 rules: the XSRF/laravel_session/suffix/encrypted cookies, the csrf-meta, vite/mix, inertia), the scores 0.883/0.845, the ranges [9.19, ∞) and (−∞, 9.x] with exact boundary strings (NULL — no boundary), found_on_mask = the apex bit, rules_version = the rules/ set's version (a Reloader, APP_RULES_DIR/a relative path); the negatives: symfony — one weak meta_csrf_token match (score 0.2, is_laravel false, no laravel_domains row), django — zero findings; the rules_version stamp on all subdomain_results rows (the T-074 re-analysis contract). Documentation — docs/dev-environment.md §7. A live run — the main session after the merge (integration runs are forbidden in the worktree); the branch compiles, make test/lint/fmt green. Spec: §11.2, §11.4 (item 5).
- **T-094** [done] — Acceptance tests over all the spec criteria (IDN zones, subdomains/variants/aliases, the DOM and cookies, the re-analysis, the `laravel_*` selections, zone progress, the recheck interval, restart resilience, search by name, the customer's samples). Spec: §11.4 (items 1–11). A new `test/integration/acceptance_test.go` (T-094): an explicit acceptance map "one §11.4 criterion — one TestAcceptNN_* case" (the item's wording — a comment above the case), the library level like the T-091 e2e (live PostgreSQL/Redis/unbound/testweb/mockserver/Chromium, an isolated schema + Redis DB 15, without launching service processes), the neighboring tests not duplicated — the cases assemble the criterion as a whole and close the gaps. The map: item 1 → TestAccept01 (ingest.Pipeline: an add in the unicode directory of an IDN zone + the punycode directory of the same zone — one zones row with a punycode name and a unicode display, is_idn labels; delete → deleted with the residue in the DB; a revival by an add file → new/pending/queued_at NULL); item 2 → TestAccept02 (4 scenarios via the dispatcher + the DNS/HTTP stages: same-site — the apex variant 0 https and www independently scheme http, dns_mask bits 0+1; different-sites — the variants 0/1 with different final URLs; equal-pair — alias_kind 1 → the apex original, one content; www-to-bare — alias_kind 2 → the apex; alive/done); item 3 → TestAccept03 (RenderRecorder → q:render → renderworker.ProcessResult on a live Chromium: rendered=true, a DOM with JS-drawn content reads from PackStore, a cookies jsonb with js_session, the raw HTML dead (the reference removed), q:analyze +1; parkings — one content_hash for the two, refcount 2, len<raw_len (zstd), a round-trip; the https→http navigation adapter — the internal Caddy CA is not trusted by the dev stack's Chromium, the T-067 pipeline is real in full); item 4 → TestAccept04 (sample analysis → a copy of the rules set with a never-firing probe (the Version changes, the verdicts do not) → ReanalyzeOnce publishes exactly the stale rows → the second set's worker rewrites the findings with the new rules_version; without re-downloading — checked_at/content_hash/the number of contents blocks and the q:dns/q:http/q:render depths unchanged); item 5 → TestAccept05 (the SQL selections: the list of domains with Laravel (confident only), the subdomains found_on_mask ↔ the catalog, the version range and the full sign composition of a confident one, the weak rails — only is_laravel=false findings, a clean django — no rows at all); item 6 → TestAccept06 (ZoneStatsOnce: total=4/checked=2 with deleted outside the denominator and queued unchecked, an empty zone 0/0, updated_at; the cache = the direct aggregate of crawlerctl status; ZoneStatsLoop with a short interval picks up a state change itself — updated automatically; the metrics — the /metrics exposure of the metrics.New("monitor") registry with crawler_service_up — what Prometheus scrapes); item 7 — the target server 24 h, outside the dev stand: T-093 (blocked), reflected in the suite doc comment and docs; item 8 → TestAccept08 (a live DB, domains of different ages and states + a fixed Now: the 6-month default moves only the matured one (the result status untouched), a repeated pass empty — "no more often than", a 10-year interval picks up the fresh one, NULL-last_checked/pending/deleted not selected; the config default = the scheduler constant, CRAWLER_SCHEDULER_RECHECK_INTERVAL overrides); item 9 → TestAccept09 (the edge beyond e2e/resilience: a live lease is not taken away, an expired one — LeaseOnce returns queued → pending, the dispatcher re-selects the set without losses and re-publishes the batch (q:dns 2), a deleted one outside the cycle — the §13.1 guard); item 10 → TestAccept10 (search via idn.Normalize + the SQL domain_info: the unicode/punycode forms (case, dot, spaces) give one answer — the status alive, the results by subdomains, the zone with name_unicode, laravel null for a non-Laravel; for a Laravel domain findings with is_laravel; a non-existent domain in an existing zone — NULL (the CLI distinguishes "no domain"/"no zone"), an unknown zone — NULL); item 11 → TestAccept11 (the whole customer sample corpus (13) through the full pipeline on the testbed: the expectations from meta.yaml via rules.LoadSamples (without copies of the references) — the composition of matches, the score ±0.001, the computed range boundaries, is_laravel, rules_version; laravel_findings — all matches including the negatives' weak ones, laravel_domains — only ≥0.6; the corpus integrity ≥8 positives/≥4 negatives/≥2 weak). Helpers with the acc* prefix (the accEnv environment — the components of all stages like the cmd/* assemblies; accPipeline/accPipelineZone — the dispatcher → DNS → HTTP stages; accAnalyze; readers of rows/findings/aggregates; the rules — APP_RULES_DIR with the ../../rules fallback (the T-091 lesson), the samples — ../../testdata/laravel-samples). Documentation — docs/dev-environment.md §7: the "§11.4 criterion → case" table and the specifics (a probe version of the set, the render's http adapter). Runs: make test / make lint / go vet green; a live test-integration run — the main session after the merge (forbidden in the worktree).
- **T-095** [done] — The final documentation of stage 10: consolidating "from zero to operations" (spec §12 stage 10, Annex A — `/docs`). New documents: (1) `docs/operations.md` — the operator's entry point: a documentation map, the first-run scenarios of dev and the production deploy (step by step, references to the ready deploy/disks/os-tuning/dev-environment sections), maintenance — software updates and migrations (applied by the services at start, a `schema_migrations` control, a `crawler.target` restart of all units at once — the single-version limitation of the pack protocol, docs/content.md), logs (dev `make logs` / production `journalctl -t crawler-*`), metrics and alerts (the key `crawler_*`, Prometheus dev/production, the monitor warnings in journald), a regular-control checklist (`crawlerctl status`, `q:dead`, the volumes, memory/OOM, SMART) and a graceful stop; (2) `docs/incoming-files.md` — the unified contract of the `incoming/<zone>/add|delete/*.txt` input files (spec §5.1–5.2): the directory structure (unicode/punycode — one zone, auto-creation), the format (UTF-8, full names, comments/BOM/CRLF/CR, the 1024 B line limit, readiness by size/mtime stability and delivery via `*.tmp`→rename), a table of all eight rejection reasons (`invalid_utf8`/`line_len`/`empty`/`invalid`/`label_len`/`name_len`/`no_label`/`wrong_zone`) with examples, the `ingest.max_error_ratio` threshold (strictly above → `failed/` as a whole), the lifecycle table (processed/a sha256 duplicate/failed+`.report`/an infrastructure failure — stays in incoming), the add/delete semantics (a revival from deleted, NotFound in the log report), the reports (the `ingest_files` journal, the log report format by renderReport, the rejected log) and an end-to-end example; (3) `docs/runbooks.md` — procedural instructions: retraining the zstd dictionary (`crawlerctl dict train`, `--samples`/the 64 MiB corpus limit, wiring `content.zstd_dictionary` + restarting the writing/reading services, the readability limitation honestly pinned — the active dictionary is one per process, after replacing the file the old frames are not readable until the content is overwritten by new episodes), investigating `q:dead` (`crawlerctl status`, XLEN/XINFO/XRANGE/XREVRANGE, the Dead format `{task_id, stream, entry_id, attempts, reason, payload}`, finding the cause by logs and `crawlerctl domain`, do not re-publish manually — the domains are returned by the lease pass, SPA — RequeueRenderPending, analysis — the hourly ReanalyzeLoop, a targeted return `crawlerctl recheck`, the XDEL/XTRIM cleanup, losing Redis is normal; the current state reflected: the XAUTOCLAIM claim loop is not wired into the services) and the re-analysis (editing rules → `make test-rules` → `rules-reload`/SIGHUP → the automatic drain on a `rules_version` change, a manual `make reanalyze`/`crawlerctl reanalyze --zone`, a progress control by SQL over `rules_version`, a nuance: changing one `laravel_min_score` does not change the set version — no re-application happens). README: the prose list of links replaced by a documentation-map table (all 17 documents), the `internal/` composition updated (ingest/scheduler/dnsclient/dnsworker/httpfetch/httpworker/monitor; removed "dnsclient, httpfetch — in their tasks"). Cleaning up the stale: docker.md §6 ("the render queue loop is wired by T-067; now healthz" → implemented, a link to render.md), migrations.md ("commands with DB access will appear later" → the active ones listed), services.md (the "Stub behavior" section → "The services' working logic"; the T-029 lease return wired — LeaseWatch in cmd/scheduler; "the T-080 alerts territory" → done), render.md ("integration — later tasks" → test/integration); dev-environment.md — references to the new documents in the intro, §3, §5 and §6 (the testbed section §4 untouched — T-090 territory). configuration.md completeness verified against internal/config (12 sections, all fields and envSpecs) and config.example.yaml — nothing missing, the reference unchanged; the relative-link integrity check of README+docs — by a script, none broken. Spec: §12 (stage 10), Annex A.
- **T-096** [done] — The `contents (pack_id, off)` index: the content metadata table has only a PK over `hash` (migration 000001), and the compaction (internal/content, CompactLoop) selects a pack's rows `WHERE pack_id = $1 ORDER BY off` and `WHERE pack_id = $1 AND refcount > 0` every hour — without an index this is a seq scan of the whole table, the cost grows linearly with the corpus and competes with working queries (the stage 12 review, INFRA-05). Solution: a migration `CREATE INDEX … ON contents (pack_id, off)` (covers the filter and the sort), an EXPLAIN plan check after applying. Done (2026-10-18): migration 000007 `contents_pack_off_idx` (pack_id, off) + a down `DROP INDEX IF EXISTS`, the TestSchema_ResultsTablesIndexes contract, docs/schema.md; the EXPLAIN check — a seq scan → an Index Scan (without Sort) on an isolated PG 16 with the project's real migrations (the main stack's dev PG was stopped).

- The stage 10 wave is finished (2026-10-24): T-090/T-091/T-092/T-094/T-095 —
  in the archive (stage 10 of this file); T-093 closed by the daily run
  23.10 09:17 → 24.10 10:26 (≈25 h of full load: `journalctl -k
  --grep oom` over the window empty, oom/oom_kill/NRestarts = 0 for all units,
  the peaks within MemoryMax — the docs/load-measurements.md §13 report), the line
  moved to the archive. The spec §11.4 acceptance is finished.

## Stage 11. Improvements from the code review (2026-10-18)

Candidates from the final reports of the fix groups; review findings not closed
by code due to a contract change, migrations, or the level of new functionality.

### Ingest

- **T-097** [done] — Domain-level ingest idempotency, an audit (variant D of the ING-P2-5 core; the product decision of 2026-10-18). The audit of all paths confirmed the guarantees: add of an existing one — ON CONFLICT over (zone_id, label); delete of an absent/already deleted one — an UPDATE without a guard, the counters stable; revive — the guard status = deleted (a repeat — a no-op); re-processing a file — a fresh batch_id, an applied batch leaves staging; CleanupDeleted/refcount — strictly over the actually deleted references. A small hole fixed: a batch cleanup failure in the error-threshold and duplicate branches was not retried by the discard (a ctx cancellation at graceful shutdown left rows in staging). Tests: the unit internal/ingest/idempotency_test.go (a repeat after losing the report → a no-op and one journal record; the discard retries), the integration test/integration/ingest_idempotency_test.go (the same file twice — the DB state snapshots unchanged; the revive cycle; the dedup contract boundary until T-113; CleanupOnce × 2 + refcount over the full delete→cleanup→revive→delete cycle). Documentation: docs/incoming-files.md §6 "Redelivery idempotency". The input for T-113: the global UNIQUE(sha256) lets an undo with the same list through (pinned by the TestIngest_SameContentCrossActionSkipped test before the contract change); orphaned staging batches after kill -9 — T-098.
- **T-113** [done] — Relaxing the file dedup to exact redelivery: the migration `UNIQUE (ingest_files.sha256)` → `UNIQUE (zone_id, action, sha256)` (safe: the new key is weaker — existing rows cannot violate it), the pipeline skips a file only on a match of the hash, the zone and the action; a duplicate with another action (the undo scenario: the same list from `add/` in `delete/`) is processed. Depends on T-097 — re-processing must be provably harmless.
- **T-114** [done] — Acceptance tests and documentation for the new semantics: the T-094 case "a sha256 duplicate of a delete file is legitimate" is redefined (the undo with the same list now works and deletes domains), docs/incoming-files.md §5, docs/schema.md. Depends on T-113.
- **T-098** [done] — Background cleanup of orphaned `ingest_stage` batches by the batch_id age (the ING-P2-3 safety net against kill -9/a DB crash; an UNLOGGED table survives a service restart, but not a PostgreSQL restart). The place — together with CleanupDeleted.
- **T-099** [done] — A metric/alert over `CleanupResult.BadHashes` (garbage `content_hash`): currently only a Warn in the log; on a spike — a signal of a result-writer error (T-056).

### HTTP

- **T-100** [done] — Compound `Content-Encoding` ("gzip, br" — the H-10 remainder): multi-layer decompression in RFC 9110 order (the last encoding in the list — the outer layer) with a decompression cap per layer (a single `bodyCeiling`, not a product); Accept-Encoding does not advertise compound values — a value from a broken server is accepted defensively, as a single deflate. The tail classification (Truncated/ErrDecompressTooLarge) — over the final decoded stream, the T-051/H-2 semantics kept.
- **T-101** [done] — Parallel polling of A/AAAA of redirect hosts (the HW-07 remainder): `DNSLookup.LookupNetIP` (internal/httpworker/worker.go) asks A and AAAA with two goroutines sharing a ctx — the resolve latency is max(A, AAAA), not the sum; ipv6=false — one exchange without goroutines. The HW-07 semantics kept: a failure/refusal of one type with a valid other — the resolve succeeds with the live one's addresses; no addresses at all — the first-by-type-order exchange error (the determinism A before AAAA); ctx cancellation — a cancellation; the address order stable A then AAAA regardless of completion (assembled in types order, the transport sees the old order). Tests: overlapping exchange intervals + the total time < the sum (a slow A + a fast AAAA, a fake sleeping outside the mutex), the first-error determinism, ctx cancellation, the question order by a set (the arrival at the resolver is nondeterministic); docs/services.md synchronized.

### Content

- **T-102** [done] — Group commit of content (C-15): a batch fsync + a metadata INSERT outside flock — removes the fsync-per-block cost (C-1) from the hot Put path. Requires revisiting the T-042/T-045 invariant and profiling on the production disk.
- **T-103** [done] — A zstd dictionary registry `dictID → *Dict` (variant 2 of C-5): pre-registering previous dictionaries when reading old frames — retraining does not make written content unreadable (`ErrUnknownDictionary`).

### Queues and scheduler

- **T-104** [done] — Partial indexes of the scheduler's background passes (S-04): migration 000008 — `domains_lease_idx (queued_at, zone_id, id) WHERE check_state = 1` and `domains_recheck_idx (last_checked_at, zone_id, id) WHERE check_state = 2`; the code literals + an ORDER BY over the index key in lease/recheck (a linear drain, an early LIMIT stop). Retry (a status in the predicate would break HOT on the delete paths) and backlog (an aggregate over the whole table) — deliberately without an index, documented.
- **T-105** [done] — The byte guard of q:dead (the Q-05 remainder): Dead records are published without the original tasks' payload — instead of the body, a squeeze of the subject's identifiers (`deadSubjectOf` in bury, best-effort: q:dns — the size of the `domains` batch (a list of up to ~250 KB is not carried, the domains will be re-published by the lease pass; multi-zone batches — a zone identifier is impossible), q:http — `zone_id`/`domain_id`/`name`, q:render/q:analyze — + `subdomain_id`/`variant` (the subdomain_results row key)); a squeeze that made the record invalid (a foreign body: a variant outside 0/1, a name longer than the limit) falls back to a record without identifiers — Publish will not reject, the XACK happens, the "poisonous" task goes to quarantine instead of getting stuck in pending (a P2 review); the q:dead worst case reduced from ~2.5 GB (10,000 × 250 KB) to ~2–3 MB at the same cap of 10,000 records — two orders below the Redis 0.5 GB budget (noeviction: an overflow = the whole transport refuses). The XTRIM-by-byte-budget variant (MEMORY USAGE on a schedule) rejected as an extra loop/complexity: the records are uniformly small, the MAXLEN cap already holds the bytes; lowering the record cap is not needed — manual investigation needs history. The `payload` field is kept in the format (omitempty) only for reading old records — Decode of history does not break, validation: a present payload must be JSON, the `subdomain_id` range (model.ValidateSubdomainID). The at-least-once/q:dead-delivery semantics unchanged (the record form and the budget). Tests: TestReliabClaimDeadByteGuard (a batch of 2000 domains ≈ 120 KB → a record ≤ 512 B, the identifiers and the batch size kept, no payload), TestReliabClaimDeadPoisonBody (a foreign body → a record without identifiers, the carry and the XACK go), TestDeadSubjectOf (all streams + a foreign format/garbage), the Dead wire formats/round-trip/validation updated, TestDecodeDeadLegacyPayload (old records readable), the T-032 integration scenario switched from a byte-wise payload to identifiers (compiles by a unit run of ./test/...). Documentation: docs/redis.md §3 (the q:dead budget in bytes — the record form instead of trimming), docs/runbooks.md §2 (what the operator sees: the subject identifiers, the batch's `domains` size, the absence of payload in new records, reading the original message with XRANGE by entry_id, cleaning old records with XTRIM), docs/configuration.md (2 places), config.example.yaml. Review: opencode/deepseek-v4.1-flash-free#high — no P0/P1; the P2 (a foreign body getting stuck in pending) and the P3s (ValidateSubdomainID, XRANGE/XTRIM in runbooks, a clarification about non-format messages) fixed, the P3 about forbidding payload at publication rejected (a double read/write contract of the format for a hypothetical producer; the only producer bury is guarded by a test).
- **T-106** [done] — The source of zone weights from the configuration (the S-08 remainder): `scheduler.zone_weights` — a map "a zone name (punycode, as in the zones catalog) → a weight of 1..1000" inside the scheduler section (the spec 5.10 set of sections not extended with new top-level sections); the default — no map: all weights 1, an equal round-robin order (the pre-task behavior); the env `CRAWLER_SCHEDULER_ZONE_WEIGHTS="com=5,org=2"` replaces the map entirely, the keys normalized to lowercase (a deterministic collision resolution), validation — a weight of 1..1000 and the syntax of a punycode zone name (fail-fast; a unicode name is rejected — the catalog does not contain it). Resolving names into zone_id — `scheduler.ResolveZoneWeights` (one query `SELECT id, name FROM zones`: all zones, not only active ones — an inactive zone's weight takes effect on enabling without a restart; the caller's map is not mutated); cmd/scheduler resolves at start, names without a catalog — a Warn, not an error (ingest creates the zone with the first file, spec 5.1 — the scheduler starting before ingest is legitimate), the zone is checked with a weight of 1 until a restart; the cycle's start log shows the number of weighted zones. Tests: the config (YAML/env parsing and the full-replacement priority, the default, weight/name validation, case normalization), the scheduler (resolution by the catalog, unknown ones ascending, errors/ctx, weights from the configuration changing the batch merge order, the SQL contract + the UNIQUE zones.name of migration 000001). Documentation: docs/configuration.md (the field, the variable, the "map" type, the Warn/inactive-zone semantics), config.example.yaml (an example with a comment). Spec: §5.3, §5.10.

### DNS

- **T-107** [done] — Implementing `dns.fallback_resolvers` and the start resolver probe (DNS-12, the T-035 policy): `internal/dnsclient/fallback.go` — the `FallbackResolver` decorator over the chain "the main one (`resolvers[0]`, T-034) + the reserves in config order"; the switch condition — a transport failure of the lower resolver (an error after its own retries = "the local one's attempts exhausted" and "unreachable" — in the T-034 contract these are the same thing), any RCODE — a server answer, the classification semantics (NXDOMAIN→not_delegated, SERVFAIL→temp_error) unchanged, ctx cancellation interrupts the chain without polling the reserves; the switch — per exchange, without remembering a "dead" state (the local one's recovery is picked up immediately, without a re-probe; the cost with a dead local — its retry cycle in the latency of every exchange); the place in the stack — under LimitResolver (one slot/token for the whole chain cycle, the adaptive sees the outcome: a success via a reserve does not drop the limits); the exhaustion error enumerates the chain's addresses and wraps the last one's cause (errors.Is/As/IsTimeout work); `Stats()` — Switches/Exhausted. The start probe `probe.go` — `Probe` (one root NS exchange, the life criterion — any RCODE) and `ProbeChain` (a success — Info, a failure — Warn, all dead — a start error: the fail-fast DNS-12, a pipeline without resolvers would sort domains into temp_error; ctx cancellation — not a verdict), it goes into the clients directly bypassing the limiter; assembling the chain and the probe — cmd/dns-worker (the reserves' pools with the same `udp_pool_size` — the planned parallelism at a switch, fail-fast on a malformed reserve address). The `internal/dnsworker/fallbackmetrics.go` metrics per the DNS-06 limiter sample: `crawler_dns_fallback_switches_total`/`crawler_dns_fallback_exhausted_total`, counters by the pass deltas `CollectFallbackLoop`. Tests: fallback_test.go (the chain order, an RCODE — not a switch, the error aggregate, cancellation before/mid-chain, concurrent counters), probe_test.go (the probe question, life on any RCODE, the ProbeChain levels via a recording slog handler, all dead, cancellation), fallbackmetrics_test.go (export, deltas, restarting the source). Documentation: configuration.md (the switch and probe semantics), config.example.yaml, runbooks.md §4 (investigating a resolver degradation), services.md.
- **T-108** [done] — Narrowing the q:dns retry unit to a domain (DNS-04): a staging mark "q:http published" (T-004, a migration) or a group acknowledgment — a batch retry today costs up to `max_attempts × |batch|` DNS episodes and q:http duplicates.
- **T-109** [done] — A terminal/diagnostic status for "poisonous" domains (the DNS-03 remainder). Done (2026-10-19): the diagnostic status `invalid` (code 7 — an extension of the §6.2 table, directly allowed by spec 6.2; the precedent "seven statuses confirmed" from spec 13.1/retry.go deliberately overridden: temp_error would generate false retries with a finalization into delegated_no_address — a lie about the domain, not diagnostics). The model: model.StatusInvalid + Statuses/statusNames (String/ParseStatus/Valid automatically). The DNS stage: a domain with an empty/whitespace name (ErrEmptyDomain) in processBatch is pinned by the resultInvalidSQL statement (RecordInvalid: status=invalid, temp_attempts=0, clearing the http_published bit, check_state=done, queued_at=NULL, last_checked_at; the guards status<>deleted AND check_state<>done — idempotency) + Warn + the crawler_domain_status_total{type=invalid} metric; a diagnosis-write failure — an episode error (no XACK, a redelivery will finish it); the batch's other domains are processed, the batch is not failed, q:dead does not grow. The exception from re-publishing — only the `status <> invalid` filter in recheckUpdateSQL (the only regular path of returning a done row into the pipeline; lease looks only at queued, retry — only at temp_error); dispatch/lease are deliberately not filtered: a recheck↔diagnosis-write race can leave invalid+pending — without the dispatch filter the row is re-published once and the diagnosis is pinned anew (self-healing), with the filter it would hang in pending forever; the filter does not break the domains_recheck_idx suitability and HOT (status is not in the indexes). The subdomains catalog (empty/broken/disabled) — a configuration, domains are NOT marked: after fixing the catalog they must be processed, a terminal status would freeze them forever (pinned in the processBatch comment). Migration 000010: domain_info CASE status_name + a WHEN 7 branch (a full CREATE OR REPLACE of the 000006 body, for 0..6 unchanged; down — restoring the old body), otherwise crawlerctl domain answered status_name=null. Tests: model (8 statuses, code 7, ParseStatus; unknown ones are now 8+), dnsworker (TestResultProcessBatchPoisonDomain — 3 records over domains, the order nondeterministic; TestResultProcessBatchPoisonDomainRecordError — a write failure fails the batch; TestResultRecordInvalid — the SQL contract/guards/errors), the scheduler recheck (wantRecheckUpdate with two status filters), the integration TestDNSWorker_InvalidDomainStatus (a live loop: an empty name → invalid+done, queued_at reset, PEL/q:dead empty; RecheckOnce with aged rows moves only the control temp_error — invalid stays done). Documentation: docs/schema.md (the index map, model.Status, a note on domain_info), docs/services.md (the dns-worker branch, the metric, RecheckDaily), docs/runbooks.md §2.4 "Poisonous domain rows: the invalid status" (a search SQL, the breakdown: delete the garbage or fix the label + a manual return into the pipeline).

### Core/infrastructure

- **T-110** [done] — Separate liveness/readiness for the services (`/livez` + `/healthz`): C-03 made a minimal readiness for the existing compose healthcheck; the full variant — separate probes (livez — the process is alive, healthz — the dependencies). Done (2026-10-18): the internal/service framework registers both probes on the metrics port (registerProbes) — `/livez` — liveness: a trivial 200 without going to the dependencies (a DB/Redis failure is not a process death, a worker restart would treat the symptom); the "background loop did not panic" sign was not added, with argumentation: a panic/an error/an OnRun exit stops the whole process (serve → Run → main exit), the state "the loop died, the process lives" is not allowed by the framework longer than the shutdown budget. `/healthz` — readiness unchanged from C-03 (a PG pool ping, 503; a nil pool — 200); Redis is deliberately not included in the probe: the Redis clients live inside the workers' OnRun (the framework does not see them), there is no light check — a new dependency in the probe path is forbidden by the task. The compose healthchecks of the application services moved from /healthz to /livez: docker health — per-container liveness (docker ps, up --wait), there are no "admit traffic/tasks" consumers (the workers pull the streams themselves), and an unhealthy wall of workers at a DB down would only duplicate the postgres status; the tools' eternally red healthcheck (sleep infinity without HTTP) disabled (healthcheck.disable), render without a healthcheck stays (the headless-shell image has no HTTP client). The metrics listener untouched. Tests: TestLivezHandler (always 200), TestRegisterProbes (routing: a dead DB → livez 200 / healthz 503, foreign paths 404), the TestHealthzHandler from C-03 kept. Documentation: docs/services.md (the "Liveness and readiness probes" subsection: what each checks, who consumes), docs/docker.md (§2 "What the healthchecks check"), operations/configuration/deploy — targeted edits.
- **T-111** [done] — The `version`/`revision` labels in `crawler_build_info` via `-ldflags -X` (C-11 introduced only `mode=prod|dev-stub`): the values' source — the git tag/hash in the Dockerfile build.
- **T-112** [done] — Interrupting a migration in progress by a signal: golang-migrate `Up` does not accept a context — today a migration finishes (pinned in C-04); full interruption requires a fork/a driver change.

## Stage 12. Improvements from the larasites analysis (2026-10-19)

The source — an analysis of the old PHP implementation of the same purpose
(`~/projects/old/larasites`, Laravel 12): a boolean "at least 2 of 5" detector
with an active 404 JSON probe (`GET /<random path>` +
`Accept: application/json`), a version by the structure of the cookie encryption
envelope, a "near matches" log (one.log), the `laravel_histories` history and
a cross-table of external detectors (external_domains). Detection-wise the current
engine already covers all the passive signs of the old detector
(the XSRF-TOKEN/laravel_session/_session cookies, the `eyJpdiI6` envelope —
rules/laravel.yaml); what is portable — the processes around the detector: calibration,
history, tools. The transfer limit — the crawler's passive ethics
(spec §5.8/§9: analyzing only what is already downloaded, no invented paths).

- **T-115** [done] — The "near matches" tool for rule calibration (an analog of the old implementation's one.log: a log of hosts with a single sign for manual investigation). Done (2026-10-19): the `crawlerctl near [--zone NAME] [--min-score F]` command (cmd/crawlerctl/near.go) — a selection of laravel_findings of the gray zone `[min_score/2, min_score)` with one SQL (a JOIN domains/zones for the name, a LEFT JOIN subdomains for the label; the zone — a NULL-filter parameter; an ORDER BY score DESC with the domain key in the tail — the closest to the threshold first), the output domain/subdomain/variant, the score, the version range, the fired rules with evidence (the T-071 signals format and the domainSignal type from domain.go reused); the threshold — the analyzer.laravel_min_score from the configuration by default (the flag takes precedence, without configuration 0.6 — the test-rules/CLI-06 recipe), a validation of (0, 1) and idn.NormalizeZone for --zone. A metric in internal/monitor/nearstats.go per the zonestats sample: the `crawler_laravel_near_findings` gauge (NewNearGauge in the service registry), NearStatsOnce (a count aggregate over the same range — one SQL contract for the gauge and the CLI), the NearStatsLoop cycle in cmd/monitor (a third OnRun cycle next to ZoneStatsLoop; the period DefaultNearStatsInterval = 1 h — justified: a pass is a seq scan of the laravel_findings partitions, calibration material does not need minute freshness; the first pass immediately, an error — Warn and the loop lives, a nil registry/pool — a no-op); cmd/monitor got a metrics registry (metrics.New, Options.Metrics — the cmd/analyzer precedent). Tests: near_test.go (the SQL contract, the fake semantics — range/zone/sort/signals parsing, the output, run() without a config — exit code 1 before connections, usage — code 2; statusRows extended with float32/[]byte types), nearstats_test.go (the SQL contract and real parameters, threshold validation, defaults, the gauge after the first pass, a no-op, surviving an error). Documentation: docs/services.md (monitor's third cycle + a line in the metrics table), docs/operations.md (the metric + a checklist line), the crawlerctl usageText.
- **T-116** [done] — Calibration: passing the stateless API. Done (2026-10-19): the cookie_encrypted_payload weight 0.45 → 0.5 (rules/laravel.yaml; the eyJpdiI6 prefix — the Laravel encryption format, not seen outside the framework — the weight justified), the analyzer.laravel_min_score threshold 0.6 NOT changed (a minimal coverage: a threshold change would touch all rules at once). The pair cookie_xsrf_token (0.25) + cookie_encrypted_payload (0.5) → noisy-OR 0.625 ≥ 0.6 — a stateless API (an api subdomain without a session on the front page) is detected, like the "XSRF-TOKEN + encryption envelope" pair (rules 2+3) was detected in larasites; the calibration rationale pinned in the rules set's header. A new sample laravel-11-stateless-api (an XSRF-TOKEN with an encrypted envelope, no session, the body — a 404 JSON without Laravel keys until T-117): is_laravel=true exactly by the cookie pair, score 0.625. The T-116 gray-zone check (the relaxation did not bring noisy pairs in): the CodeIgniter triple ci_session + csrf-meta + a _token form = 0.49 < 0.6 — a new corpus_test case; the Rails pair 0.32 (an existing test) — both below the threshold; all new pairs ≥ 0.6 must contain the encrypted cookie. The scores of samples with the enc cookie recalculated (meta.yaml: 0.845–0.959 → …, a ±0.001 tolerance), make test-rules green (14 samples, 0 errors). The affected tests: corpus_test (the xsrf+enc pair — must now be detected; the noisy triple; ScoreFixed 0.45→0.5 in two places with recalculating the comments 0.772→0.793).
- **T-117** [done] — A passive analog of larasites' strongest sign. Done (2026-10-19): json_unauthenticated (the exact "Unauthenticated." form, 0.3) replaced by the wide `json_message_key` — the regex `\{"message":"[^"]{1,120}"\}`, a weight of 0.25 (a deliberate lowering: NestJS/Express write the pattern `{"message":"Cannot GET /"}` — a single rule is not enough for the threshold; a replacement, not a neighborhood: the exact rule nearby would double-count one answer — "Unauthenticated." matches both). larasites' active 404 probe stays rejected (the passive ethics of spec §5.8/§9) — the rule reads the already downloaded response. A new sample laravel-12-api-404 (an api subdomain, a 404 `{"message":"Not Found"}` + XSRF/Sanctum cookies): is_laravel=true by three signs (0.25+0.5+0.25 → 0.719, calibrated together with T-116). Tests: TestCorpusJSONMessageKey (both forms + a long/empty/non-string/a foreign key — the pattern boundaries; a NestJS singleton below the threshold; fired exactly once — no duplicates), laravel-9-json-api meta.yaml recalculated (json_message_key 0.25 → score 0.845), make test-rules green (15 samples). Changing the rule id changes rules_version — the stale findings will be recalculated by the regular reanalyze (T-074).
- **T-118** [done] — The appeared/lost detection history. Done (2026-10-19): migration 000011 `laravel_history` (an id IDENTITY PK — a domain has many transitions, zone_id, domain_id, an appeared bool, changed_at now(); a changed_at index — closes "findings in 24 h" of T-120 and the daily dynamics; no partitioning deliberately — only transitions are written, the volume orders of magnitude below findings; down — DROP IF EXISTS). The write — in the aggregateDomain transaction: a SELECT EXISTS of the row's presence (under the advisory lock — a concurrent recalculation will not insert between the check and the write) → an upsert/delete → on a transition INSERT appeared=true (there was no row → it appeared) / appeared=false (the DELETE affected a row). The freshness semantics — the `queue.Result.Fresh` flag (omitempty, old records read false): set by httpworker (newRecordBatch) and renderworker (the publication after render — finishing the current episode), ReanalyzeLoop does not set it — a re-analysis of the same content by new rules is not a fact about the site, the flapping of borderline scores of T-116 does not generate false appeared/lost; pinned: the semantics is tied to the laravel_domains row ("left during a re-analysis, returned fresh" will leave one appeared without a paired lost — the price of protection against rules noise, the trend over a set is correct). The crawler_laravel_appeared_total/crawler_laravel_lost_total metrics — internal/monitor/historystats.go: cumulative gauges from the append-only journal (monotonic — rate()/increase() correct), a FILTER aggregate in one scan, a 1 h period, monitor's fourth OnRun cycle. Tests: the aggregate_test TestAggregate_History (an appearance/a repeat/a disappearance fresh, a re-analysis appearance and disappearance without a record, a DELETE 0 without a record), the anFakePool fake extended (a history snapshot in Rollback, a QueryRow EXISTS, a journal INSERT, the agg_exists/agg_hist events), historystats_test (the SQL contract, the gauge after the first pass, a no-op, surviving an error), the e2e test checks laravel_history appeared over all the cycle's Laravel domains, TestMigrate_InterruptedMidApply extended with the 000011 down effect (a DROP TABLE) per the test's convention; the e2e score expectations synchronized with T-116. Documentation: docs/schema.md (the laravel_history section, a line in the tables table), docs/services.md (analyzer: the journal; monitor: the fourth cycle; the metric lines), docs/operations.md.
- **T-119** [done] — `crawlerctl check <domain>`. Done (2026-10-19): cmd/crawlerctl/check.go — the pipeline of one name as a library of stages: the subdomains catalog from the DB (the only read, LoadSubdomains) → the DNS resolver (the T-034 client + the T-035 limits, a reserve chain for one name is overkill) → CheckApex (the terminal branches — a status and an exit) → CheckSubdomains + DetectWildcard/MarkWildcardSubdomains (the T-038 policy: a detection failure — continuing without filtering) → DecideEpisode → the HTTP transport like the production http-worker's (httpfetch.NewOptions(cfg.HTTP): the UA/timeouts/limits; CachingResolver+DNSLookup; InsecureSkipVerify+TLSInfo) → httpworker.ProcessDomain (without OnResult/Recover/Metrics — writes and publishes nothing; the task identifiers synthetic, the name and subdomains pass the production workerPrepareTask) → an SPA render with render.remote_url configured (render.Manager+Start, an error/not configured — an honest mark and an analysis over the original HTML; the browser's cookies/headers added to the T-067 passive set) → the analysis rules.LoadDir+EvaluateCtx with the analyzer.laravel_min_score threshold. The per-subdomain output: the record (variant/scheme/status/final URL), the redirect chain, the TLS summary (validity/problems/NotAfter/CN/the chain), the aliases, the laravel outcome (score, ver, is_laravel, the HTML/DOM source) and the signals with evidence; the header — the normalized name/zone/unicode/threshold. No active probes (the §5.8/§9 ethics). Tests: check_test.go (a record output with redirects/TLS/signals, TLS problems, an error class, a summary of silent schemes, conversions, run without a config — exit code 1, invalid input — code 2), a live run on the dev stand: laravel-11-vite.crawl.test → a DNS apex → a main/https 200 record, a TLS summary, score 0.894 (matched the corpus meta.yaml), 7 rules with evidence; the render on the self-signed stand degrades with a mark (production sites with valid certificates render), read-only confirmed by the final line. usageText extended.
- **T-134** [done] — Known Laravel-CMS session cookie names. Done (2026-10-19): the cookie_cms_session rule (a weight of 0.35, target cookie_name, the regex `^(?:__Secure-|__Host-)?(?:october|winter|bagisto)_session$`) — a CMS cookie with an envelope without XSRF gives 0.35+0.5 → 0.724 ≥ 0.6 (the T-116 gap closed); the rules/laravel.yaml header extended with a T-116-style rationale. The composition — only october|winter|bagisto (the names from the task text, "verified against the sources"); botble/pyrocms/asgard NOT added (no internal confidence in the default names; pinned by the commit and negative tests — they must not match). cookie_session_suffix extended with prefixes `^(?:__Secure-|__Host-)?[a-z0-9_\-]+_session$` (October ≥1.1 automatically sets __Secure- on https — a real class of names lost even the weak signal; the prefix group is case-sensitive per RFC 6265bis; zero regression — there are no cookies with prefixes in the corpus, no existing meta.yaml recalculated, test-rules all old samples OK). The remember bonus admin_auth/user_auth NOT done: the crawler analyzes only the Set-Cookie of a passive GET (httpworker), October sets them only via Cookie::forever in the response to a login POST — a detection value of ~0, generic names → near noise. The samples october-cms (__Secure-october_session, an https episode) and winter-cms (winter_session), both "envelope + no XSRF" — exactly 3 rules (cms+the suffix duplicate+enc, the double counting consistent with laravel_session) → 0.724. Tests: TestCorpusCMSSession (fixtures + a table of names/prefixes/boundaries, the gray zone: ci_session+an envelope 0.575 < 0.6, a CMS name without an envelope 0.4475, the CodeIgniter triple 0.49, a NestJS singleton 0.25), TestCorpusScoreMath synchronized (+a __Secure-ci_session case). Testbed: the db.crawl.test zones + the Caddyfile regenerated compute-only (--root /src, the T-090 style), TestGeneratedArtifactsInSync green. docs/dev-environment.md: the sample counter 13→17 (stale since T-116/T-117) + the CMS cookies. Review (deepseek unavailable — Insufficient funds → agy gemini-3.8-flash-high, /tmp/opencode/review/134-t134-cms-cookies.md): no P0–P2; P3-1 non-capturing groups and P3-2 %3D artifacts accepted; P3-3 (prefixes in cookie_laravel_session — Laravel does not set it automatically, a manual SESSION_COOKIE) postponed as a candidate for future calibrations. make test 30 ok, lint 0 issues, test-rules 17 samples/0 errors. After the merge: make testbed-gen in the main tree — postponed until the dev stack is freed (the user's mutate-calib experiment). The rules_version change will arrive via the regular reanalyze.

### Considered and rejected (the larasites analysis, 2026-10-19)

- The active 404 probe (`GET /<random path>` + `Accept: application/json`): a strong sign, but it violates the passive ethics of spec §5.8/§9 and doubles the load on foreign sites; the passive alternative — T-117.
- A version by the structure of the cookie encryption envelope (in larasites: `{value,mac}` ≤5.3 / `+iv` 5.4 / `+tag` ≥5.5) and a strict envelope check mode: a cross-check against the framework sources (the tags 4.2, 5.3–5.7, 11.x) showed the table is wrong — iv goes first in all versions since 4.2 (the eyJpdiI6 prefix is universal, carries no version information), tag appeared later than 5.7 and depends on the cipher in the application's config, not on the framework version (in 11.x the tag key is always present, empty for CBC), all 5.8–12.x are indistinguishable as one mass; plus it would require base64+JSON parsing in an engine designed regex-only (RE2). The current ranges (Vite ≥9.19 / Mix ≤9.x / welcome 5.3–7.x) are more informative.
- The "the host starts with api." sign (larasites' fifth rule): not a Laravel indicator; an api subdomain is already checked content-wise (subdomains, dns_mask).
- The boolean "2 of 5" count: noisy-OR with weights — a generalization accounting for the signs' strength; returning to equal votes — a step back.
- larasites' priority re-check, the www→bare fallback, separate redirect modes and the 3-month cycle are covered: crawlerctl recheck, resolving all subdomains by the DNS stage, the redirect budget + aliases/variants, recheck_interval.

## Stage 13. Public landing page — migration from larasites (2026-10-19)

The source — the public part of the old implementation (`~/projects/old/larasites`):
the `GET /{locale}` landing page (HomeController + home.blade.php, 12 Blade components,
~600 lines of templates) over precomputed counters, with a zone cloud (top-50),
a table of featured zones, the last 5 findings and a manual check form
(`POST /api/check-domain`, a 4/min throttle, IDN/punycode); the front — vanilla JS
(landing.js, 897 lines: count-up, an animated cloud with collisions,
table sorting, the AJAX form) + Tailwind v4; i18n ru/en (lang/landing.php).
A product extension outside the main spec (marked in T-119 as "not done in that
task" — done here). Pinned decisions (2026-10-19): the form's semantics —
a hybrid (first the stored `domain_info` data, then a live check by the T-119
pipeline), the manual checks history — the web_checks table,
i18n — ru+en. The transfer limits: the active 404 probe rejected earlier (Stage 12 —
the passive ethics of spec §5.8/§9), CSRF/PHP sessions not needed (a JSON API + a rate limit),
Tailwind is built at build time (a css stage in Dockerfile.go before go build,
make web-css for dev; the standalone tailwindcss v4 CLI, node appears neither
in the project build nor at runtime), the built bundle is not stored in git,
the zones' is_featured is not carried over (there is no admin panel —
"featured" zones = the top-N by the counter), the "items/hour speed" from
daily_domain_statistics is not carried over (the table does not exist; a dynamics analog —
"findings in 24 h" will appear with T-118, before that the card is not shown).

- **T-120** [done] — Migration 000010, the landing data (the number shifted: 000010/000011 are taken by T-109/T-118, the actual one — 000012_landing_data). Done (2026-10-19): the `zones.domains_laravel bigint NOT NULL DEFAULT 0` column — a cache of the zone's Laravel domain count, updated by the same ZoneStatsOnce pass (zoneStatsSQL extended with a second LEFT JOIN of the aggregate `SELECT zone_id, count(*) FROM laravel_domains GROUP BY zone_id` — the table's rows already passed the analyzer §13.1 threshold, no separate threshold needed; the LEFT JOIN zeroes zones without findings); the partitioned index `laravel_domains_updated_at_idx (updated_at)` — the landing's "latest findings" are sorted by updated_at without sorting the table, new zone partitions inherit the index (000003 touched only by its own tables); the `web_checks` table (an id bigint IDENTITY PK, domain text — as entered, punycode_domain text, ip_address inet, found bool, score real, duration_ms int, created_at timestamptz DEFAULT now(); the indexes (ip_address, created_at) and created_at; only cmd/web writes — T-123, no rotation — the manual checks volume is small, growth is held by the form's rate limit). Down — DROPs with IF EXISTS. Tests: schema_test (the zones/web_checks columns in TestSchema_TableColumns, the indexes in TestSchema_ResultsTablesIndexes, web_checks in the exclusion of regular tables of leaf partitions), TestAccept06 extended (a laravel_domains row → domains_laravel=1 by the same pass, an empty zone — 0), TestMigrate_InterruptedMidApply extended with the 000012 down effects, wantZoneStatsSQL. Documentation: docs/schema.md (the tables table, the cache paragraph, the laravel_domains/laravel_history indexes).
- **T-121** [done] — The skeleton of the public service `cmd/web` + `internal/web`. Done (2026-10-19): cmd/web per the cmd/monitor sample (config.Load → service.Run, OnDBPool, METRICS_ADDR 127.0.0.1:9107 — the next after analyzer); the public listener — its own http.Server inside OnRun with a graceful stop on ctx cancellation (a Shutdown within the 8 s window < the framework's 10 s budget; Serve → ErrServerClosed — a regular exit), ReadHeaderTimeout 10 s (slowloris, §9). internal/web: Server (New: a nil Pinger — an error), Handler() — Go-1.22 method routes: GET /{$} → 302 /ru (the default locale), GET /{locale} — the landing.html template (a stub until T-122, the ru/en locales, foreign prefixes — 404), POST /api/check-domain — an honest 501 stub until T-123 (+an explicit GET handler 405: the landing's catch-all would return a misleading 404), GET /healthz — a PG ping with a 1 s timeout (503, a body without details), methods other than GET/HEAD — 405. The html/template templates + go:embed internal/web/templates (the migrations/embed.go precedent), no dependencies. The web config section per the KnownFields recipe: listen (127.0.0.1:8080 — publishing behind a reverse proxy with TLS in T-124), rate_limit (4/min — the stage 13 decision), timeout (60 s — the full check pipeline: 2×http.total_timeout + render.timeout); Default/Validate (host:port, 1..1000, 1s..10m)/envSpecs (CRAWLER_WEB_LISTEN/RATE_LIMIT/TIMEOUT) + config.example.yaml + docs/configuration.md. Tests: internal/web/web_test.go (routing with a table, the root Location, healthz 200/503, the 405 methods, a graceful stop of Run on a live listener, a nil Pinger), config_test (the web defaults/env/validation). Documentation: docs/services.md (the web service, a metrics table line :9107, the dev port range), docs/configuration.md.
- **T-122** [done] — The landing page: migrating the Blade markup → html/template. Done (2026-10-19): internal/web/templates/landing.html — a full migration of the larasites components (the header with navigation and the locale switcher, the hero with an animated SVG radar, statistics — 5 cards with count-up counters (active zones, the sums of domains_total/checked/laravel + "findings in 24 h" from laravel_history T-118 — larasites' items/hour speed card deliberately not carried over by the stage's decision), a zone cloud top-50 by domains_laravel with a log size scale (0.875–1.5rem, log1p — .com does not squeeze the others into unreadability) and a tooltip, a zone table top-10 with the laravel/checked share and client-side sorting, how-it-works (rewritten for the passive pipeline: lists → DNS+subdomains → passive analysis — larasites' active 404 probe rejected by the §5.8/§9 ethics), the check form + catalog-teaser, latest-domains (subdomain badges from found_on_mask with https://{sub}.{domain} links, the ver_min–ver_max range, the date), contacts+telegram; the layout — canonical/hreflang ru+en/OG/Twitter, favicon.svg/site.webmanifest (go:embed), the Yandex.Metrika counter kept). i18n — internal/web/i18n.go (ru/en dictionaries, the larasites keys carried over, the locale in landingVM.Msg data — the template is parsed once without FuncMap). The data — internal/web/data.go: 6 SQLs over the T-120/T-118 caches (statistics by an aggregate over zones, last24h by laravel_history_changed_at_idx, the cloud/table LIMIT 50/10, findings ORDER BY updated_at DESC LIMIT 5 by laravel_domains_updated_at_idx, the subdomains catalog for badges); a DB error — 500 (a public page does not degrade halfway), punycode→Unicode via x/net/idna. The front — internal/web/static/landing.js (a count-up IntersectionObserver, a cloud with rectangle collisions and an appearance animation + a re-layout on resize, table sorting, the AJAX form: data-endpoint, no CSRF header, rendering the T-123 contract, 429/422/501 — localized messages via a JSON script); app.src.css (@import tailwindcss + @source of the template + the radar animations/cloud styles — carrying over the hero/zone-cloud styles), a placeholder app.css in git (the build — make web-css/T-124). Static under /static/ from go:embed (StripPrefix). Tests: web_test (a ru/en render with fake data, a dictionary in the body, the root redirect, 404, static 200, a 500 on a DB failure, healthz, 405, a graceful stop), data_test (the SQL contracts, statistics/the log scale of the rank/percentages/mask badges with an apex without a badge/versions/the date, a nil pool, errors, displayUnicode). make test 27 ok, lint 0.
- **T-123** [done] — `POST /api/check-domain`, the hybrid. Done (2026-10-19): the T-119 pipeline refactored into the internal/checker library package (Run: DNS → HTTP → an SPA render → analysis; read-only; the Result/SubRecord signatures for the CLI and the web; the Logger/LiveFunc seams; cmd/crawlerctl/check.go switched to the package — printing from Result, a live smoke of the stand confirms score 0.845 = the corpus). Input validation per the larasites CheckDomainRequest sample (validateCheckInput): trim, cutting the trailing dot, an http(s) prefix → host (the port cut), lowercase, garbage (spaces/@/:) — a refusal, a cap of 2048, idn.Normalize → punycode. The stored section — SELECT domain_info($1) (T-082) best-effort: status_name/last_checked_at/the laravel aggregate; a failure/an unknown domain — the section omitted. The live section — s.liveCheck (a seam, production checker.Run) with the web.timeout cap; a live error does not destroy the response: stored stays, an empty outcome without stored — 502. Writing web_checks (T-120) best-effort (domain/punycode/ip/found/score/duration_ms). The rate limit — an in-memory token bucket 4/min per IP (web.rate_limit, a lazy cleanup of buckets >4096), 429 + Retry-After: 60. The response — a single JSON (found, score, ver_min/ver_max, stored, subdomains[{subdomain, variant, status, final_url, redirects, tls_valid, score, ver_*, signals[{rule,weight,evidence}], rendered}], duration_ms) — the carried-over landing.js contract; the 422/429/502 errors in the {"error":...} format. Tests: a validator table (URL/port/IDN punycode/garbage), the token bucket (4 passes, the 5th refused, a foreign IP, recovery through a quarter of the window), an e2e handler with a fake liveCheck (the JSON fields, the stored section, the DB calls), the 422 format, 429+Retry-After at a limit of 1; the fakeDB fake extended with a domain_info case and a []byte sink. make test 27 ok, lint 0. Dependencies: T-119/T-120/T-121 — done.
- **T-124** [done] — Deployment and documentation: the css stage in Dockerfile.go. Done (2026-10-19): a css stage in Dockerfile.go (alpine + the standalone tailwindcss v4.3.3, a musl binary by TARGETARCH, the ARG TAILWINDCSS_VERSION like the other versions; the binary is dynamically linked against the C++ runtime — libstdc++/libgcc only in the css stage; a COPY --from=css replaces the placeholder app.css in the build stage before go build → go:embed embeds the bundle, only the placeholder stays in git); web added to the cmd build cycle (before the task the production image was built without /app/web — a T-121 gap); make web-css for dev (a one-off docker run, chown DEV_UID/DEV_GID), WEB_PORT=8081 in .env (8080 is taken by Adminer). A compose web service in the app profile (a go-common clone: SERVICE=web, METRICS_PORT=9107, METRICS_ADDR=:9107 in the docker network, outward 127.0.0.1:${WEB_PORT:-8081}:8080), a web section in config.dev.yaml, a web job in prometheus. systemd crawler-web.service (User=crawler-web, metrics loopback 127.0.0.1:9107, MemoryMax=256M — live form checks run the checker pipeline with page bodies; a RuntimeDirectory under a unix socket), crawler.target extended, the deploy.md budget recalculated (8 units, 1536M of services, ≈8.0/8.2 GB). The first sample of the public entrance deploy/nginx/crawler.example.conf (HTTP→HTTPS, ACME, HSTS, X-Forwarded-*; a unix socket variant with permissions). Routes: GET /robots.txt (embed static: Disallow /api/, /healthz) and GET /sitemap.xml (generation: an absolute <loc> is mandatory, the scheme/host from the request — X-Forwarded-Proto only from a loopback peer, no domain URLs — a deviation from the letter of "static" pinned); web.listen accepts a unix socket path (a validation of ≤100 bytes, Run distinguishes tcp/unix, an orphaned file is removed). Review (deepseek, /tmp/opencode/review/124-t124-deploy.md): P1 clientIP ignored the proxy headers (behind nginx one 4/min bucket for everyone + 127.0.0.1 in web_checks) — X-Real-IP trusted only from a loopback peer and only a valid IP, the nginx sample rewrites the header; P2 (an orphaned socket, the http2 directive under Ubuntu LTS, the socket permissions in README) and P3 closed. Tests: TestRobotsSitemap, TestRunUnixSocket, TestClientIP, config cases; make test 27 ok, lint 0 issues; docker build --target css / build-runtime / web-css verified pointwise. Documentation: services/deploy/configuration/docker/dev-environment/db/migrations/operations + README + config.example.yaml.

## Stage 14. Initial population of the zone catalog (IANA) (2026-10-19)

The zones catalog is currently populated only automatically — by ingest at
the zone's first file (spec 5.1), there is no prefill. The full catalog of delegated
TLDs (the IANA root zone registry, ~1460 names) — the operational base: the catalog's
completeness for the landing page (T-122, the zone cloud/table), the name control of incoming
incoming directories, a fast `zones enable` without waiting for the first file.
The source — a machine-readable snapshot of the registry
`https://data.iana.org/TLD/tlds-alpha-by-domain.txt` (the same content as
the iana.org/domains/root/db page, without HTML parsing): one zone per line,
IDN zones already in punycode — matches the zones.name contract (spec 5.2).
The limitation: this is a catalog of TLD names, NOT a source of domains — domains
still only via ingest (var/incoming); the snapshot contains no metadata (type, manager),
the zones schema does not need it anyway.

- **T-125** [done] — `crawlerctl zones sync` — adding/updating the zone catalog from the IANA registry (the fourth zones action next to list/enable/disable). Done (2026-10-19): cmd/crawlerctl/zonessync.go — the `--file` (offline) / `--url` flags (the default — the canonical data.iana.org/TLD/tlds-alpha-by-domain.txt, stdlib net/http, a 30 s timeout, a 1 MiB body cap); specifying both — exit code 2; `--deactivate-missing` — an UPDATE of the DB zones outside the list with an is_active guard (idempotency, RETURNING — an honest list of the disabled into the report). The parser: `#` comments skipped, the header comment until the first zone — the version into the report; every line — idn.NormalizeZone (garbage — an error with the line number, not a silent skip), a dedup after normalization; an empty list — a refusal before the DB (the --deactivate-missing safety net against a total shutdown); reading/parsing the snapshot before the configuration and the pool — a source refusal does not touch the DB. The insert — `INSERT … SELECT … FROM unnest($1::text[], $2::text[]) WHERE NOT EXISTS … ON CONFLICT (name) DO NOTHING` (NOT EXISTS is mandatory: the IDENTITY default executes for conflicting candidates too — on live PG 16 a conflict without the filter moved the sequence, repeated syncs of ~1460 rows would exhaust the smallint in ~20 runs; ON CONFLICT — a race guard with EnsureZone); new ones is_active=true (like ingest's auto-creation, the zone pause contract untouched), existing ones not changed at all, name_unicode — nullif(ToUnicode) for XN--, NULL for ASCII. The report: the source, the version, read/duplicates/added/already there, with the flag — the disabled. Tests (zonessync_test.go, 12 functions): the parser, an httptest download (200/404/the cap/a refusal), the SQL contracts and exact parameters on a fake pool (the zones_test.go style), sync on a fake (idempotency, existing unchanged), --deactivate-missing, an e2e run(). Documentation: zonesHelp, usageText, docs/dev-environment.md §6 (+an example of offline sync with curl; README does not contain the crawlerctl command list — untouched, the decision in the commit). Review (deepseek, /tmp/opencode/review/125-t125-zones-sync.md): P1 RETURNING 3 columns against 4 Scan receivers (a live DB would fail, the fake masked it) — fixed, the fake turned into mirroring the SQL; the P3s accepted (a cap on --file, the version only from the header), 2 P3s rejected with argumentation (disabling non-IANA zones — the intended behavior of the explicit flag with a report; the UA/transaction — operators are idempotent). The SQL additionally verified on the live PG 16 of the dev stack in ROLLBACK transactions. make test 27 ok, lint 0 issues.

## Stage 15. The operator's web dashboard of the pipeline (2026-10-19)

The source — [`TZ_dashboard.md`](./TZ_dashboard.md) (below — TZD): an internal
web dashboard — an at-a-glance summary (progress, speed, counters,
queues, findings, service health) with password access. The key
decisions are pinned in TZD §13: our own Go service instead of Grafana (without
new containers and dependencies), separate from the public landing page of stage
13 (a different audience and data), Basic Auth + PBKDF2-SHA256 (stdlib
crypto/pbkdf2) with a cookie session, read-only access to the same sources
as crawlerctl status, a snapshot cache by a background cycle. Ports: public
127.0.0.1:8090, metrics — loopback :9108.

- **T-126** [done] — Extracting the internal/status package — the common summary library (TZD §4.5): moving from cmd/crawlerctl/status.go without behavior changes. Done (2026-10-19): internal/status (status.go + status_test.go) — the zoneStatsSQL/throughputSQL SQL texts moved byte-for-byte (verified by a diff against master), the types/selections/calculations — verbatim. Exported: FetchZoneStats, FetchThroughput, FetchQueueLens, ZoneStats, Throughput, Totals, Rates, Streams (the order of the 5 spec 4.2 streams), ComputeTotals, ComputeRates, FormatDuration, MinSpeedWindowSec, the StreamLener interface (*queue.BackpressureGate satisfies); the SQL not exported (the monitor style) — duplicating the SQL between the CLI and the dashboard is excluded by construction. The CLI status.go — only the output (writeStatusTable/writeStatusJSON, actMark, percentMark) and the glue with the infrastructure; the SQL and the model import removed from the CLI. Tests: 9 status tests moved with a package change, 6 output/routing tests stayed in the CLI (15 → 9+6, no losses; the statusRows fake stayed in the CLI — near/zones/zonessync/subdomains/recheck use it, a documented copy in the package). Review (deepseek, /tmp/opencode/review/126-t126-status-pkg.md): no P0–P2; a P3 accepted (the wording of the doc comment about snapshot caching — groundwork for TZD §4.4), 3 P3s rejected with argumentation (slice mutability — the project style; a JSON DTO in the CLI — a wire-contract question of T-129). Acceptance: make test 28 ok (the internal/status coverage ≈92%), lint 0 issues; a manual cross-check of make status on the dev stand before/after — the output identical (the diff only the container ID and the live elapsed), status --json — a valid JSON of the same shape. For T-129: consider extracting the canonical JSON DTO into internal/status so that the CLI's and the dashboard's wire contract do not diverge (TZD §6.2).
- **T-127** [done] — The dashboard configuration and secret generation (TZD §7.2–7.3, §7.7, §8): a section per the KnownFields recipe (Default/Validate/envSpecs + config.example.yaml + docs/configuration.md). Done (2026-10-19): the dashboard section in internal/config — listen (127.0.0.1:8090), refresh (5s, the 1s–5m corridor), username, password_hash, probes (a map name→metrics base URL); env CRAWLER_DASHBOARD_LISTEN/REFRESH_INTERVAL/USERNAME/PASSWORD_HASH/PROBES (probes — "name=url,name2=url2" comma-separated, a new setStringMap setter per the zone_weights T-106 sample: case normalization, deterministic collisions, replacing the map entirely); Validate — listen/refresh/probes (§8.3: host:port, the corridor, absolute http(s) URLs, non-empty names), username/password_hash deliberately NOT validated (§7.7 — the mandatory check is done by cmd/dashboard at start). The internal/dashboard/auth package: the format `pbkdf2-sha256$<iterations>$<salt-b64>$<key-b64>` — Generate (a 16-byte crypto/rand salt, a 32-byte key, 600,000 iterations), Parse (strict: the scheme, ≥1 iteration, a salt ≥16 bytes, a key of exactly 32, std base64), Derive (the KDF-derived key — groundwork for the §7.5 HMAC cookie for T-128: a password change invalidates sessions), Verify (subtle.ConstantTimeCompare); PBKDF2 — stdlib crypto/pbkdf2 (Go 1.24+, the project is on go 1.26 — availability verified in the tools image), go.mod unchanged. The crawlerctl dashboard-secret command (cmd/crawlerctl/dashboardsecret.go): the password from an argument or stdin (the first line, CRLF stripped, the argument takes precedence), empty input/extra arguments — exit code 2, a read error — code 1, the help warns about the shell history; registration in commands/usageText. Tests: config (defaults/YAML/env/priority/normalize/validation/boundaries), auth (a round-trip, a wrong password, strict parsing, incompatible iterations/length, salt randomness), the command (an argument, stdin ×4 forms, empty input ×3, precedence, a read error, help). Review (deepseek, /tmp/opencode/review/127-t127-dashboard-config.md): no P0–P1; P2 (a stdin test without a full KDF verification) and 3 P3s closed by fixes, the re-runs green. Documentation: docs/configuration.md, config.example.yaml, dev-environment.md §6, the CLI help. make test 29 ok, lint 0 issues. For T-128: auth.Parse+Verify (Basic) and auth.Derive (the HMAC cookie key); the credentials mandatory check — in cmd/dashboard.
- **T-128** [done] — The cmd/dashboard skeleton + internal/dashboard and authorization (TZD §4.1–4.2, §6.1–6.3, §7): cmd/dashboard per the cmd/monitor sample (config.Load → service.Run, OnDBPool; the framework metrics — METRICS_ADDR 127.0.0.1:9108, TZD §4.2). Done (2026-10-19): cmd/dashboard/main.go — config.Load → the §7.7 start credentials check BEFORE service.Run (empty username/password_hash, an unparseable hash, a ":" in the username — inseparable in Basic) → service.Run (OnDBPool); the public http.Server on dashboard.listen inside OnRun with a graceful stop on ctx cancellation (the T-121 precedent), the §6.3 timeouts (ReadHeader 5s/Read 10s/Write 15s/Idle 60s). internal/dashboard (dashboard.go): Server/New/ParseCredentials/Handler/Run, the §6.1 routes — /, /assets/*, /api/status (stubs until T-129/T-130: / — a 200 HTML placeholder, /assets/* — a 404 behind authorization, /api/status — a 503 {"error":"snapshot_not_ready"} + no-store — the §5.9 semantics until the first snapshot), /healthz (a PG ping, 503), /livez, robots.txt (disallow all) — without authorization; non-GET/HEAD — 405; security headers (nosniff + the CSP verbatim §6.3) on all responses. authmw.go: Basic — the name subtle.ConstantTimeCompare, the password auth.Verify (both comparisons without short-circuiting); the cookie session `<unix-expiry-decimal>.<hex HMAC-SHA256 (expiry, key)>` (the key = the Key of the parsed hash; HttpOnly, SameSite=Lax, 24 h; hmac.Equal; a password_hash change invalidates sessions — a test on two servers); a valid cookie is checked before the limiter (a live session is not locked out by others' failures); a per-IP brute-force protection of 5 failures/min → 429 + Retry-After (a token bucket per the T-123 sample; a success resets the window — pinned; a 429 does not extend the window); a Warn on a failed verification with the IP, without credentials a 429 is not logged, the password/hash/Basic do not get into the log (a test). Tests: 13 functions of dashboard_test.go (401+WWW-Authenticate, wrong credentials, blocking and expiry, a cookie valid/tampered/expired, 405/404/headers, healthz/livez/robots without authorization, a graceful stop, clientIP). Review (deepseek, /tmp/opencode/review/128-t128-dashboard-skeleton.md): no P0/P1; P2 (the clientIP comment) and 2 P3s accepted (a clientIP test, refusing a username with ":"); 6 P3s rejected with argumentation (CSP frame-ancestors/Secure-cookie — extending the verbatim TZD; a DoS bucket/an O(n²) cleanup/a ping in healthz — consequences of the TZD and precedents; a unix socket — §9.1, a T-131 question). make test 30 ok, lint 0 issues. Documentation: docs/services.md (the service + a metrics line :9108), README (cmd/, internal/), docs/configuration.md (refusing to start on empty credentials).
- **T-129** [done] — The snapshot collector and `GET /api/status` (TZD §4.3–4.4, §5, §6.2, §10): a background cycle with the dashboard.refresh period and an atomic snapshot replacement; until the first successful collection /api/status — 503 (§5.9). Done (2026-10-19): internal/dashboard/collector.go + snapshot.go — the Collector.Loop cycle in OnRun of cmd/dashboard (the cmd/monitor pattern), the first collection immediately, a ticker with the dashboard.refresh period, publishing by an atomic replacement (atomic.Pointer), /api/status reports readiness via an atomic.Bool latch; the T-128 stub replaced by a live delivery (Options.Snapshots SnapshotSource), 503 — only until the first successful collection. A "successful collection" = a live summary skeleton (Totals ≠ nil): a Redis down from the very start is served (the queues "n/a", the §12.3 acceptance works at a restart), a down DB holds 503 + result=error. Sections: zones/totals/pace/speeds/ETA — internal/status (T-126, the SQL not duplicated — §4.5); new selections added to internal/status: the domains distribution by status (model.Statuses, a legend with zeros), Laravel (total + per 24 h in one pass, the last 10 with a JOIN of zones ORDER BY updated_at DESC — the T-120 index), the last 10 ingest_files (NULLS LAST, id DESC); the stream depths — BackpressureGate.Len like monitor (a Redis client by cfg.redis in cmd/dashboard, NewCollector accepts a StreamLener — the tests' seam, nil → an honest degradation); the /livez probes in parallel by goroutines+WaitGroup, a 2 s timeout, url.JoinPath, without redirects (plus an "HTTP 301" test). The §5.9 degradation: section errors in an errors map, null/"n/a" values; the probes — up/down data (not an error), empty probes → health null. The JSON DTO extracted into internal/status/json.go (SummaryJSON — the T-126 note): the CLI status --json switched to it, the output byte-for-byte (a review empirically: 0 divergences); a snapshot = SummaryJSON + statuses/laravel/ingest/health/errors/built_at + queue_watermark (the T-130 front reads only the API). The crawler_dashboard_snapshots_total{result}/crawler_dashboard_snapshot_seconds metrics (the internal/metrics factory, Options.Metrics). Tests: the SQL contracts of 4 selections + the fake's live semantics (status_test), assembling sections/degradation (a DB/Redis/nil-lenner — null sections, the foreign ones alive), the JSON shape (keys/null/order), httptest probes, the cycle (the period/replacement/the latch/Redis-down-from-start), /api/status 200/503, -race clean; the integration TestDashboard_Snapshot on the testbed (skipUnlessIntegration) — the main session's control run green. Review (deepseek, 2 rounds, /tmp/opencode/review/129-t129-snapshot.md): the P2 latch (an eternal 503 at Redis-down from start) fixed; the P2 of holding a connection at pool_size=1 — closing the set before the second query; the P2 queue_watermark in the integration test; P3 fixes (JoinPath, CheckRedirect+a test, rows.Err, the docs "Pass cost": the regulator of full aggregates — the refresh 5s TZD §8.1, there is no ingest_files.processed_at index — deliberate); the P3 status.Streams mutability rejected per the T-126 precedent. make test 30 ok, lint 0 issues, test-integration PASS (80 s, all tests). Documentation: docs/services.md (the collector, readiness, degradation, the pass cost, the metrics), README. The live §12 acceptance (up → 8090 after login matches crawlerctl status; Redis down → the queues "n/a") — after T-131 (the compose service).
- **T-130** [done] — The dashboard front (TZD §5, §6.1, §6.4–6.5): assets/ (page.html, app.js, style.css) via go:embed (the migrations/embed.go precedent), without external CDNs (TZD §3.2.2). Done (2026-10-19): internal/dashboard/assets.go — //go:embed assets/, the GET / (page.html) and GET /assets/* (app.js, style.css) handlers; the T-128 stubs removed, the routes/authorization unchanged. Content-Type — an explicit assetTypes map (not a FileServer: the system /etc/mime.types gives a nondeterministic .js, and listing a name outside the map — an honest 404, incl. ".."), a no-cache (names without hashes). page.html — a single ru page: 8 cards (§5.1–5.2), the queues of 5 streams with watermark highlighting (the sum of the WORKING streams without q:dead — the gate does not control it; ≥90% warn, ≥watermark over; a non-zero q:dead — its own highlight), the zones table with pending/queued/done/activity progress bars (§5.3), the status distribution as a legend strip (§5.5), the latest findings (§5.6), the ingest files (§5.7), the health chips (§5.8), "n/a" placeholders (§5.9), meta[name=dashboard-refresh]. app.js — vanilla (the landing.js style): fetching /api/status every ~5 s (the period from meta; the client shows the last ready snapshot — allowed by the TZD), SVG polyline sparklines from a ~120-point history (~10 min at 5 s) for 3 speeds and each stream, a 401 — stopping the polling + location.reload() (fetch does not show the browser's Basic dialog; a repeated 401 on the document will make the browser offer a login), 503 → "the snapshot is being prepared…", a network failure — a status line, the polling lives; everything via textContent/CSSOM — CSP-clean (verified: no innerHTML/eval/document.write). style.css — a dark theme on system fonts, no CDN/external fonts. The fields — strictly the T-129 snapshot (the structure unchanged). Tests: assets_test.go (a render: the sections/ru/meta/polyline/no external sources; static: Content-Type/no-cache/non-zero bodies/404 outside the map) + TestRoutes/TestSessionCookie/TestSecurityHeaders updated for the real routes. Documentation: docs/services.md (the dashboard section: the front, the sections, the polling, the sparklines, the 401), the cmd/dashboard comment. Review: a fallback chain (deepseek/opencode — Insufficient account funds, agy — RESOURCE_EXHAUSTED, grok — a free limit, cursor-agent — a usage limit) → the gemini CLI (gemini-3.5-flash-lite, read-only, the report /tmp/opencode/review/130-t130-frontend.md): the field names verified against the snapshot, 401/503/network, the history limit, CSP/XSS, embed/routes — no P0–P3; after the review one wording edit, the tests green again; the main session's control self-check (grep innerHTML/eval, 401) — clean. make test 30 ok, lint 0 issues, node --check app.js OK. The live acceptance in a browser — T-131.
- **T-131** [done] — Deployment and documentation (TZD §9.1, §11, §12). Done (2026-10-19): a compose dashboard service in the app profile (a go-common clone per the web T-124 sample: SERVICE=dashboard, METRICS_PORT=9108, METRICS_ADDR=:9108 in the docker network, the port 127.0.0.1:${DASHBOARD_PORT:-8090}:8090, depends_on postgres/redis healthy, mem_limit 256m §11.1, app-dns, a /livez healthcheck); dashboard added to the build cycle of the build stage of Dockerfile.go; .env.example DASHBOARD_PORT; prometheus.yml a dashboard job :9108 (§11.3); systemd crawler-dashboard.service (User=crawler-dashboard, Group=crawler, CRAWLER_DASHBOARD_LISTEN=127.0.0.1:8090, METRICS_ADDR loopback :9108, MemoryMax=128M, the §11.2 isolation) + Wants in crawler.target. config.dev.yaml — a dashboard section (listen ":8090", username admin, password_hash from the dev password admin via crawlerctl dashboard-secret, probes by the service names ingest/scheduler/dns-worker/http-worker/render-worker/analyzer/monitor with their METRICS_PORT). Key decisions: the dashboard's DB pool = 2, not 5 (a ninth service of 5 each does not fit into max_connections=50: 9×5+crawlerctl 2+an admin ≈ 48 > 47 available; snapshot building — one sequential cycle, the second slot — a /healthz ping; the budget recalculated in 5 places: 9 units/1664M/total 8124M); the EnvironmentFile > Environment= precedence per man systemd.exec — the wording "pins the unit" replaced by "sets the unit", a safe global maximum CRAWLER_POSTGRES_POOL_SIZE in crawler.env — 4; the probes example errors from T-127 fixed (dns-worker 9102→9103, http-worker 9103→9104) in config.example.yaml/configuration.md, the examples moved to the loopback form of the systemd deploy; "the only public entrance" updated (the landing — to visitors, the dashboard — a separate server block of the same nginx, TLS mandatory because of the Basic credentials, X-Real-IP for per-IP brute-force protection); the production credentials — config.yaml via stdin, not duplicated in crawler.env. Test: test/integration/config_test.go — the dashboard section contract in config.dev.yaml (listen, username, ParseCredentials+Verify("admin"), the exact probes map). Documentation: deploy.md (the unit, the user, the budget, password generation, loopback/reverse-proxy+TLS §9.1), services.md, dev-environment.md (the dev credentials admin/admin + regeneration), configuration.md, docker.md, operations.md, migrations.md, redis.md, README. Review (deepseek-v4.1-flash-free#high via opencode run, 3 rounds, /tmp/opencode/review/131-t131-deploy.md): P2-1 the connections budget, P2-2 the EnvironmentFile precedence (verified per man systemd.exec), P2-3 the global pool consistency + 9 P3s — all closed by fixes with repeated green runs. Checks: make test 30 ok, lint 0 issues, docker compose config valid, TestConfigDevEnvironment with CRAWLER_INTEGRATION=1 PASS, systemd-analyze verify clean. The live TZD §12 acceptance (make up → 8090 Basic admin/admin, cross-checking the numbers with crawlerctl status, stopping Redis → the queues "n/a") — postponed: the dev stack is busy with the user's experiment (mutate-calib, postgres down with an address conflict); the execution commands pinned and will be run when the stack is freed.

## Stage 16. Input file ingest extensions (2026-10-19)

Real registrar exports and public zone dumps do not arrive in the "clean"
form of spec 5.1 (one full domain name per line): lines carry extraneous
data ("xxx.com 2029-11-11 TIMEWEB", a registrar's CSV/TSV), and the files themselves
are packed (.zip/.gz). Today such lines are rejected by the validator
(ReasonInvalid/wrong_zone — internal/ingest/validate.go), a file with an error
share above ingest.max_error_ratio falls into failed/, and archives are ignored
by the watcher (internal/watch watch.wantFile tracks only *.txt).
The stage extends the input format — an extension of spec 5.1 (the items about "one
domain per line" and "lines that are not a full domain name
are rejected") with the name normalization (spec 5.2) and streaming
(spec 3.5) unchanged. The tasks are technically independent and run in parallel.

- **T-132** [done] — Extracting a domain from an arbitrary line (an extension of spec 5.1; the name normalization — spec 5.2 items 1–6 unchanged). Done (2026-10-19): internal/ingest/extract.go — extractCandidates (the separators unicode.IsSpace/,/;, trimToken strips the edge quotes and punctuation, urlHost — the host from net/url: the port/userinfo/path cut, the scheme's case irrelevant; the filter "a token with an ASCII dot"); the integration into Validator.Line — extraction turns on only with a separator in the line or an explicit http(s) scheme of a single token, single lines pass the old path unchanged (the empty/invalid/label_len/name_len/no_label/wrong_zone reasons kept, pinned by tests); the candidates left to right through the full pipeline (checkName — the extracted common path of idn.Normalize+domainLabel, bitwise equivalent to the old one); a valid one from a foreign zone is skipped; none passed — the new no_domain reason (the rejected log, the report's Reasons breakdown, counted in the max_error_ratio threshold; it was not written into the ingest_files Reasons table before either). A deliberate behavior change: an inner space — a field separator, gluing a split domain name ("pri mer.rf") is gone (otherwise xxx.com yyy.com would fabricate xxx.comyyy.com — dictated by the foreign-zone test); documented, the corpus pins it. The config flag is absent, MaxLineLen=1024 untouched, internal/idn unchanged. Tests: extract_test.go (a candidates table: the customer's example "xxx.com 2029-11-11 TIMEWEB", a URL with a port/case/userinfo/a broken port/a url.Parse error, IPv4/IPv6, quotes/commas/;, foreign schemes, edge -/_, dates/words without dots), TestLineExtract (pinning the old reasons + no_domain in the counters/Reasons/log), corpus_test.go (mixed lines, an IDN in a mixed line and a URL, the space semantics), pipeline_test.go TestPipelineExtractLines (staging/the application, copy:3, processed/). Review (deepseek, /tmp/opencode/review/132-t132-extract.md): no P0/P1; P2-1 (unicode.IsPunct stripped the edge -/_ — substituting the name with another domain, _dmarc.example.com → dmarc.example.com) — the predicate narrowed, tests added; P2-2 (stale whitespace comments) and P3-1 (ToLower→EqualFold in the hot path), P3-2/P3-5/P3-6 — fixed; P3-3 (ideographic dots as a candidate sign) and P3-4 (a BOM before a URL — a pre-existing limitation) rejected with argumentation. Documentation: docs/incoming-files.md §2/§3/§8. make test 30 ok, lint 0 issues; the control test-integration postponed — the dev stack is busy with the user's parallel experiment (mutate-calib: postgres down with an address conflict), the run will be done when the stack is freed.
- **T-133** [done] — Accepting archives in incoming (an extension of spec 5.1 "*.txt"; sha256 idempotency — spec 5.1, streaming — spec 3.5). Done (2026-10-19): internal/watch wantFile additionally tracks .zip/.tar/.tar.gz/.tgz/.gz (case-insensitively; .tar.gz is covered by the .gz suffix) — the same StableAfter readiness rules, hidden/temporary names, processed/failed ignored. internal/ingest/archive.go (~560 lines) — streaming unpacking: members are read as io.Reader and pass the existing Parse/Validator/Stage, without materializing on disk; the stdlib formats (a zip ReaderAt+fstat of the size, tar, gzip; .gz — one member with the archive's name without the extension; rar/7z rejected as non-stdlib); the zone/action — only from the archive's directory (member paths have no effect, addressing the rejected log "archive!member:line"); directories/hidden files/macOS garbage (__MACOSX/, ._*, .DS_Store) — a Warn skip; nested archives are not unpacked (binary garbage → an error share → failed). The accounting unit — the archive: one ingest_files row (the archive's sha256, sums over members, one staging batch with end-to-end line_no numbering); a duplicate — the T-113 semantics; failed → the batch discarded, "partially accepted rows" covered by the domain-level idempotency (T-097); the error classification: a broken archive → failed/, infrastructure → the file stays in incoming. The zip-bomb limits: ingest.archive_max_uncompressed (ByteSize, default 8GiB; zip — a pre-check of UncompressedSize64 before reading + a counter of the fact — the header may lie; tar/gz — a counter over the whole decompressed stream, headers included) and ingest.archive_max_files (default 1000; entries counted including directories/garbage — a metadata bomb is caught); the KnownFields recipe + env + config.example.yaml. sha256: tar/gz — a single pass + reading the remainder to the end on a clean outcome (the hash = the file's sha256sum, verified over the gzip trailer), zip — a separate ReaderAt pass; broken archives honestly without a hash. The sticky io.ReadAtLeast budget (a limit error is given by the next call, a media error is not masked). pipeline.go: branching by extension in process(), the staging producer/consumer extracted into the shared stageStream — the txt behavior unchanged; ParsePath accepts archive names; report.go Members + a line in the log report. Tests: archive_test.go (18 scenarios: streaming processing, a zone from the directory, garbage, the volume/count limits, a broken one → failed, the repeat idempotency, an empty archive), watch/path/config. Review (deepseek, 2 rounds, /tmp/opencode/review/133-t133-archives.md + -r2.md): round 1 "requires fixes" — P1 R1/R2 (the tar limits did not count headers/directories — the bypass confirmed by runs), P2 R3/R4 (the .tar sha256 prefix, an unchecked CRC of .tar.gz) + R6/R7–R13 — fixed (+ a ReadAtLeast error swallowing found and fixed); R5/R13 rejected with argumentation; round 2 "acceptable", the residual P3s added afterwards with repeated green runs. Rejected outside the task: renaming the stale ErrPathNotTXT (a public contract change). Documentation: docs/incoming-files.md §9 (a new section; the line format — T-132 untouched), configuration.md, dev-environment.md. make test 30 ok, lint 0 issues; the control test-integration postponed until the dev stack is freed (the mutate-calib experiment; the SQL layer unchanged — full unit coverage on fakes).

## Stage 17. Production server deployment — the physical deploy (2026-10-22)

- **T-135** [done] — The physical deploy of the production server per spec §10.1 (a native systemd deploy, Docker — dev only): installing and starting the T-085…T-089 set on the target server + the customer's additions (domains/dashboard, certbot, CI/CD). Done (2026-10-22).

  **The hardware journal** (VDS 18561438, IXcellerate Moscow, 155.212.174.88, pulpirzvk.fvds.ru): Ubuntu 24.04.5 LTS, kernel 6.8.0-142-generic, 4 vCPU x86_64, RAM 7.7 GiB, zram swap 3.8G (swappiness 30). Disk — a single virtio /dev/vda 80 GB, GPT: vda1 2M + vda2 EFI 100M + vda3 root ext4 79.9G (the whole disk). No global IPv6 → `dns.ipv6: false`. Ports 80/443/5432/6379/9222 free before installation.

  **Deviations from the scenario** (2): (1) disks — the LVM volumes `lv_pg`/`lv_content` (disks.md §4, the scenario's step 1) not created: the provider image's root partition takes the whole disk, there are no free extents, an online shrink of ext4 is impossible (a rescue mode outside the task's scope); `/var/lib/postgresql` and `/var/lib/crawler/content` live on the root ext4, `noatime` applied to the whole root (fstab vda3), TRIM — the regular fstrim.timer. The risk of a single FS is mitigated by `disk_free_percent` (a content write pause + an alert); a full cycle of 350 million domains on 80 GB is not designed for anyway (disks.md §3 requires 8 TB+) — the scale of the first batches does not contradict the place. (2) SMART/TBW — the deploy/smartd set (the scenario's step 8) not installed: there are no NVMe devices on the VDS, SMART via virtio is unavailable (smartctl: Unable to detect device type; -d scsi too), smartd stopped and disabled, the monitor's nvme_wear alert honestly "not measured".

  **A set defect found and fixed during the deploy** (code edits in this task despite "contains no development" — without them a multi-user systemd deploy is inoperative): internal/content/pack.go created `.packs.lock` and pack files 0o644 (with the units' UMask=0007 → 0640), and they are opened for writing by SEVERAL units with different uids in the shared crawler group (http-worker, render-worker — appending the active pack; analyzer — the same flock via O_RDWR) → EACCES, http-worker and analyzer in a restart loop ("permission denied .packs.lock"); in the docker deploy the defect is invisible — all services under one uid. The fix: the mode 0o660 (exactly what deploy.md §3 promises "pack file permissions 0660, the crawler group") for the lock and the packs + ReadWritePaths=/var/lib/crawler/content added to crawler-analyzer.service (the flock opens O_RDWR, and ProtectSystem=strict without an RW path cuts the write). The content/web unit tests green.

  **Done by the scenario's steps** (operations.md §3): (2) the packages postgresql-16.15/redis-server 7.0.15/unbound 1.19.2/smartmontools/nginx/certbot + the memory budget drop-ins, the crawler role/DB + pg_stat_statements, the production PG configs (T-086, an include in postgresql.conf) and Redis (T-087, /etc/redis/redis.conf), unbound — the internet mode from deploy/docker/unbound/unbound.internet.conf with the edits interface 127.0.0.1 + access-control loopback (unbound-checkconf clean; systemd-resolved pointed at 127.0.0.1 via resolved.conf.d, the fallback resolvers kept); (3) sysctl 99-crawler.conf + nf_conntrack (the module + modules-load.d); (4) the static binaries of 9 services + crawlerctl (CGO_ENABLED=0 -trimpath -s -w, buildMode=prod, the version/revision from git; the landing css T-124 via make web-css) into /opt/crawler/bin; Chromium headless-shell 156.0.8078.17 — the distribution from the CHROMEDP_HEADLESS_IMAGE image into /opt/crawler/chromium + the OS packages libnspr4/libnss3 (the rootfs variant dropped: the unit expects the binary in the directory's root, and the host loader does not see the chroot's libraries); (5) the crawler-<service> users in the crawler group + working directories per §4.4; (6) /etc/crawler/config.yaml (the DSN, paths, render.remote_url=ws://127.0.0.1:9222, http.user_agent=LaraCityBot/1.0 (+https://laracity[cut]

  **The customer's additions**: DNS — laravel.city and laracity.ru already pointed at 155.212.174.88 (reg.ru), the sites.laravel.city (A) and sites.laracity.ru (CNAME) records also existed; only the dashboard.laracity.ru CNAME→laracity.ru record added (the reg.ru MCP); the public entrance (the customer's correction of 2026-10-22, the same day: the landing pages moved from the main domains to the sites subdomains) — nginx: https://sites.laravel.city (the en locale: location = / → /en), https://sites.laracity.ru (ru: / → /ru), the main and www names (laravel.city, www.laravel.city, laracity.ru, www.laracity.ru) → 301 to the corresponding sites names (one canonical landing host), https://dashboard.laracity.ru → 127.0.0.1:8090 (X-Real-IP for per-IP brute-force protection, the admin credentials, HTTP→HTTPS, HSTS/nosniff, an ACME webroot); one Let's Encrypt certificate for 7 names (laravel.city + www + sites, laracity.ru + www + sites, dashboard.laracity.ru; certbot webroot + certbot.timer + a renewal hook reloading nginx, `certbot renew --dry-run` success); the landing's sitemap.xml assembles absolute URLs of the sites host from the Host header. CI/CD — .github/workflows/deploy.yml (a workflow_run after a green ci on master, deploying the same head_sha; a build with setup-go + make web-css + the production go build cycle; scp over ed2[cut]

  The readiness criterion met: the pipeline works on production hardware, all checks green. T-092/T-093 unblocked (the text in tasks.md updated).

## Stage 18. Telegram notifications for CI/CD (2026-10-22)

- **T-136** [done] — Notifications by a Telegram bot into the group per spec §10.4. Done (2026-10-22), commit 6d7df85. **deploy/telegram-notify.sh** — the common sending script of both workflows: JSON via jq, curl `--connect-timeout 10 --max-time 30`, plain text without parse_mode; a secrets guard (empty `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` → the reason into the log, exit 0 — PRs from forks), `--dry-run` with secret masking; secrets are not printed (the API response with chat.id is discarded, on error only `error_code`/`description`). **ci.yml**: one notification step `if: failure() && event != workflow_dispatch` + `continue-on-error` (manual ci runs are silent — §10.4), the category by the first failed step: build/lint/production images → "A code error", tests → "A test error", the rest (diagnostics, the environment) → the "A CI pipeline error" fallback (the event is absent from the §10.4 table, but staying silent about a red run is worse — the decision pinned); a checkout depth 2 + `--sha` of the head branch: in PR runs the guilty commit is the PR head, not the merge commit. **deploy.yml**: one step `if: always()` (success/failure/cancellation; workflow_dispatch — a regular deploy, notified), the "Upload and install" step split into scp and installation for the honest rollback fact — per the actual semantics of deploy-install.sh the trap is active from `systemctl stop` until a successful start and is removed (`trap - EXIT`): an installation failure → "the rollback performed (bin.prev)", an ssh rc=255/cancellation → "the rollback is unknown", a healthcheck failure → "the rollback NOT performed — the new version stays on the server" (the healthcheck does not roll back — the §10.4 divergence with the T-135 code found in this task, the stale claims fixed in docs; the rollback implementation moved to T-137); the deploy version — `git describe` from the build step (`GITHUB_OUTPUT`) with a recalculation per the checkout (the deploy checkout: `fetch-depth 0` + `fetch-tags`); a cancelled run — "The deploy is not successful (the run cancelled)". **The marker map** — the customer's choice by a live Telegram render: 🟥 ❌ a code error, 🛑 a test error, ✅ a deploy success, ⛔️ a failure (with FE0F), ⚠️ the fallback (the main session's decision — matches none of the customer's four). The secrets created (the bot @yepbro_logger_bot → the "YepBro Log" group, a control message delivered). **The main session's verification** found a P1: the production payload was built with the `<TELEGRAM_CHAT_ID>` stub → an HTTP 400 "chat not found" forever silently under continue-on-error (the subagent's E2E with a fake token did not catch it — Telegram validates the token before chat_id); fixed by the subagent (a masked payload only in the dry-run branch), confirmed by a live send. The deepseek review — 3 rounds, the verdict OK (r2: 5×P2 + 4×P3 closed; P2-5 "mute PR notifications" rejected — master is pushed only after a green PR, a push-only restriction would deprive the group of almost all notifications). **Acceptance on the live pipeline**: a push to master → "✅ The deploy succeeded" (deploy 39744306697); a temporary PR #3 (closed, the branch deleted): a deliberate lint error → "🟥 ❌ A code error" (ci 39743036453), fixed by lint + a failing test → "🛑 A test error" (ci 39744723691, the category confirmed by the substitution in the run's log); the failed deploy path — by dry-run scenarios and review (production was not broken deliberately). Documentation: docs/ci-cd.md (the secrets + the notifications section), docs/operations.md, README. make test 31 ok, lint 0 issues, shellcheck 0.

## Stage 19. CD: automatic rollback on a healthcheck failure (2026-10-22)

- **T-137** [done] — Automatic rollback of the production deploy on a healthcheck failure (spec §10.4 "a healthcheck with automatic rollback"). Done (2026-10-22), commit 6884d85. **deploy/deploy-rollback.sh** (new): copying `bin.prev` into an mktemp directory BEFORE stopping (a copy failure does not touch the live bin) → stop → rm → an atomic mv within the FS → `reset-failed 'crawler-*'` (resetting the start limit of the units that hit it on the failed version's crash loop) → start → a light is-active check of 10 units after 3 s (the full healthz cycle is not repeated — chained rollbacks and tens of seconds of readiness). Codes: 0 — performed/all active; 1 — performed/not all active; 3 — failed; 4 — nothing to roll back (no bin.prev, the first deploy, the set untouched); 2/255 are not returned — the pipeline treats them as "the state is unknown". Usable by the duty operator manually (replaces the docs/deploy.md §5 procedure). **deploy.yml**: a rollback step between the healthcheck and diagnostics, `if: failure() && healthcheck.outcome == failure && healthcheck.rc == 1` — only a confirmed check failure (rc=255/2 → the state unknown, a destructive rollback is not started); the order "rollback first, then diagnostics" — returning a working version matters more than collecting logs (`journalctl -u --since '-30 min' -n 200` keeps the failed version's logs); the step paints the run red on an incomplete rollback success, pick() in the notification names the healthcheck the failure's root. **deploy-install.sh** reworked: the codes 3–8 detail the outcome (4 — no archive; 3 — the set did not stop; 7/8 — a failure before the backup: bin untouched, the set starts back up — a fresh backup does not downgrade the live version; 5/6 — a trap rollback with a result check), the INT/TERM/HUP signal traps → exit 130 (before, `$?`=0 in the EXIT trap cancelled the rollback at an ssh cut), a backup via mktemp+mv (the old bin.prev lives until the new one is ready), the env paths BIN_DIR/BACKUP/ARCHIVE at production defaults — testability by the harness without root (CI does not set these names — the prod behavior unchanged). **Notifications**: `--rollback performed | performed-degraded | nothing-to-rollback | failed | unknown` (not-performed removed — its only consumer was the T-136 CI path); the wording reflects the server's state exactly; the "rollback by healthcheck is not implemented" caveat left the docs. **Tests**: a local harness var/tmp/t137 (a systemctl shim + env paths) — 66/66 scenarios (success, no bin.prev, mktemp/cp/stop/start failures, degradation, the whole pipeline install→healthcheck fail→rollback→notification, all install codes); shellcheck 0; yaml.safe_load; make test 31 ok, lint 0 issues. The deepseek review — 3 rounds, all verdicts OK, no P0/P1 (r1: honest install codes, reset-failed, tmp→mv; r2: checking the trap rollback's result, the signals, the fresh flag, the healthcheck rc in GITHUB_OUTPUT; r3: starting at fresh=0, `journalctl -u`; rejected with argumentation: keeping tmp on an accident, healthcheck retries — a false failure must roll back to a known-working version). **Acceptance**: a regular master deploy (39773947043) — install success by the new codes, the healthcheck green, the rollback step correctly skipped, "✅ The deploy succeeded" delivered; the rollback path covered by harness emulation and three review rounds (production was not broken deliberately). Docs: ci-cd.md (the pipeline diagram, the principles, the notifications section), deploy.md §5 (a manual rollback → deploy-rollback.sh), operations.md (the duty operator's runbook).

## Stage 20. Improvements from the load measurements (2026-10-22)

The source: T-092 (docs/load-measurements.md, "Findings for tasks").

- **T-138** [done] — An empty response body lost the content reference (the T-092 measurement, §9): `httpfetch.ToUTF8` on an empty body returned `UTF8=nil` with the worker's valid hash of the empty body → `Record.Body=nil` → the ContentSaver guard extinguished `content_hash` (a Warn "the record's body unavailable", ~4% of tasks — parkings and 400/404/500s with `Content-Length: 0`). Done (2026-10-22), commit e6d3879. The fix: ToUTF8 on an empty body returns a non-nil empty slice; the nil/empty semantics separated — nil "no body at all" (protecting against a layer desync in store.go), an empty slice — a valid empty body, reaching Put with the hash of empty bytes (`HashOf(nil)==HashOf([]byte{})`, the T-041 contract "empty data — valid content"); the dedup/equivalence classes of empty pages, the final-URL cache and the renderq pass work like for any content; `emptyIfNil` in decodeAs — the contract does not depend on the x/text implementation details (a BOM-only body decodes into 0 bytes). httpworker untouched (the parallel T-139 was rewriting worker.go), the httpworker tests — in store_test.go. Tests: empty 200/400/404/500 via ProcessDomain+Save by both saver paths (the per-row Put and the production PutBatch) — the hash alive, one dedup block for all empty pages; the final-URL cache path; BOM-only (UTF-8/UTF-16LE/BE). The deepseek review (1 round): no P0/P1, 2×P3 closed via amend (BOM-only nil normalization, parameterizing the saver test). Documentation: content.md, services.md. Spec: §5.5 item 6, §5.7.
- **T-139** [done] — The HTTP stage's throughput (the T-092 measurement: a cap of ≈1.9 domains/s because of the window barrier, the spec §3.3 target ≥100 domains/s, the first cycle of 350 million ≈4.6 years). Done (2026-10-22), commit be3a57f. The window barrier of 16 deliveries (a wg.Wait over the whole window with a total_timeout tail of 20–25 s) replaced by sliding consumption of q:http: the "reader + batch buffer + dispatcher" scheme — the reader keeps ahead no deeper than a couple of Consume batches, the dispatcher occupies a semaphore slot per delivery and takes the next one when the slot frees; the stage cap is now `Concurrency / the average duration` (≈150 domains/s at the default 256 against 1.9), not `Consume / the tail`. The guard rethought for the sliding mode: the threshold — Consume deliveries in a row without an acknowledgment → a pause with doubling (workerFailWaitBase, a 30 s cap) + a Warn failed/streak/wait (a ctx check against a false Warn at shutdown); a success resets the counter. The parallelism configurable: http.task_concurrency=256 / task_consume=16 / task_per_domain=8 (env CRAWLER_HTTP_TASK_*, validation 1..10000/1..1000/1..126 — 126=2×63 subdomains, passed into cmd/http-worker, the worker's DefaultConcurrency synchronized); the default rationale per T-092 (an average task of 1.67 s → ≈150 domains/s with a ×1.5 margin for per-IP degradation: head-of-line parkings of per_ip_limit=4 absorbed by slots; 256×8 ≈ http.concurrency 2000; bodies in flight ≤ max_body_bytes, ≈tens of MiB — MemoryMax=384M was not raised until the measurement, the rollout order 64–128 → measurement → 256 and the acceptance criterion — load-measurements §3); the q:http backpressure (a watermark of 100K) unchanged; the render cycle untouched (Chromium dictates the cap). Tests: sliding consumption (tail deliveries acknowledged with one blocking in flight — impossible at the barrier), cancellation (buffered ones do not start at occupied slots, without XACK, the slot cap), a poisonous queue (pauses throttle the read pace with doubling), the worker/config defaults' sync, YAML/env/validation; stability -count=3/10. Review 3 rounds (deepseek ×2 + agy/gemini-3.8-flash-high at a timeout/quota): no P0/P1; P2/P3 closed (the stale "64 tasks" in the unit/deploy.md, the rollout order, an honest prefetch, a cap test, a pause Warn, docs); rejected with justification: a nondeterministic streak-reset test and a sequential RecoverLoop (a candidate for a separate task). A re-measurement with the T-092 kit — an operational step after the deploy. Spec: §3.3, §13.2.
- **T-140** [done] — OS packages for headless-shell (the T-092 measurement §4 incident: at T-135 only the binary was taken from the docker image → a Skia `SkFontMgr_FontConfigInterface: Not implemented` FATAL (SIGTRAP) on some pages → a crash loop until the start limit of 10 restarts → the unit failed, q:render accumulates attempts; invisible in dev — fonts inside the image). Done (2026-10-22), commit 703a07a (operationally fixed on the server earlier: apt install fontconfig fonts-liberation fonts-dejavu-core + reset-failed/start). Pinned in docs/deploy.md §4.3: the headless-shell dependency package list (libnspr4 libnss3 — mandatory, installed at T-135 outside the docs; fontconfig fonts-liberation fonts-dejavu-core — Skia's font stack) with the incident explanation and the failed-unit recovery step (reset-failed + start), loud ldd checks (LIBS MISSING/libs ok, including ldd's own failure branch) and fc-list (FONTS MISSING/fonts ok); a pointer from §4.1. docs/operations.md §3: the package list in the deploy scenario's steps 2/4, the fc-list font check in step 9 "Check", the stale healthz range fixed 9100…9106 → 9100…9108 (the metrics of all nine services). The deepseek review (2 rounds): no P0/P1, the accepted P2/P3 closed via amend (an explicit fc-list, reset-failed+start, the FATAL wording per load-measurements §4, the order "after unpacking", the ldd check and its failure branch); rejected and recognized as justified: the font check in deploy-healthcheck.sh (code — outside a docs-only task, a candidate for a separate one) and the statuses in tasks/ (only the main session).

- The stage 20 wave finished (2026-10-22): T-138/T-139/T-140 moved
  to the archive (stage 20 of this file). A re-measurement of the throughput
  with the T-092 kit after the T-139 deploy — an operational step (the rollout
  order — load-measurements §3).

## Stage 21. Improvements from the re-measurement after T-139 (2026-10-23)

The source: a re-measurement with the T-092 kit after the T-139 deploy (docs/load-measurements.md §11: 47.6 domains/s on 256 slots, ×25 against the barrier; the ≥100 criterion not reached — a mixture of ≈5.3 s/task; Redis hit OOM even before the deploy because of the streams' history).

- **T-141** [done] — The Redis streams' memory: XACK does not delete records — the acknowledged history accumulated until the T-025 MAXLEN (single 1M ≈ 350–700 MiB/stream; q:dns 10K batches) and overflowed the 512 MiB budget (the T-093 daily run sat on "OOM command not allowed" even before the T-139 deploy). Done (2026-10-23), commit 2f78f26. The mechanism — a periodic XTRIM MINID (internal/queue/trim.go, TrimOnce/TrimLoop; the cycle in the scheduler, redis.trim_interval default 1 min): the trim point — the oldest mandatory record (min over the groups: the oldest pending, with an empty PEL — last-delivered-id), a numeric id comparison, atomicity and monotonicity make the race safe; the choice against XDEL-after-Ack — ~12 commands/min against +1 per record; a Warn about a "stuck prefix" (XLEN ≥ ¾ MAXLEN with live groups). The constants per the measurement on live Redis 7.4 (a batch of 1000 domains ≈ 64 KiB — "237 KiB" was the era of batch_size 5000): scheduler.batch_size 5000→1000, defaultMaxLenDNS 10,000→250, defaultMaxLenUnit 1M→150,000; the cap arithmetic of 327 MiB ≤ 512 (the worst fork case ≈467 ≤ 480) pinned by the queue.TestStreamPolicyFitsBudget and config.TestSchedulerBatchFitsStreamBudget pin tests. The integration TestResilience_TrimConfirmedPrefix: 5 rounds of publish→consume→XACK→TrimOnce — XLEN to the boundary record, MEMORY USAGE does not grow (2,000 records), a hung PEL and an undelivered tail intact, q:dead without a group does not break. Docs: redis.md §3 (the budget table + rolling back the T-093 temporary measures), configuration.md, runbooks §2.2, config.example/dev.yaml, deploy/redis/redis.conf. Review 3 rounds deepseek: no P0/P1; rejected with justification P2-4 (dashboard thresholds — a candidate for a separate task) and P3-2/P3-7. Spec §4.2 (a 2026-10-23 supplement), §3.2.
- **T-142** [done] — Render — the next bottleneck (0.59 pages/s at an inflow of 5.4% × HTTP). Done (2026-10-23), commit 90b4af9. renderworker.Run — sliding consumption per the T-139 sample (a reader: the Alive gate → the guard → Consume → the buffer; a dispatcher on the Concurrency semaphore), the window barrier removed; the renderFailWaitBase/Backoff guard — Consume deliveries in a row without an XACK → a pause with doubling up to 30 s, panics counted; read-ahead without an XACK returns via XAUTOCLAIM (at-least-once). contexts 3→4 (+33% ≈0.78 pages/s; the Chromium budget ≈2 GB: a peak of 1024 MiB at 3, the restart threshold 1536 MiB — a margin of ≥0.5 GB): the config default, config.example.yaml, deploy/docker/config.render.yaml; the dev stack left at 3 with a comment; cmd/render-worker passes Concurrency = max(Default, contexts). The spec §5.6 render_pending contract verified, unchanged (the T-066 mechanism: HasCapacity before the transaction, the flag by the same transaction, RequeueRenderPending — http-worker is not blocked). The PEL holding window recalculated (claim_min_idle: a tail ≤ ~3.8 min against the 10 min threshold). Tests: RunSlidingConsumption, RunCancelKeepsBufferUnacked, RunBackoffOnFailures, RunPanicBackoff, RunCancelWhileWaitingSlot, the defaults' couplings; -race stable. Review 2 rounds: no P0/P1, 4×P2 of round 1 fixed; rejected with argumentation (a token channel, Alive in the dispatcher, generalizing the pattern, the acceptance measurement — operational after the deploy). Spec §3.3, §5.6.
- **T-143** [done] — The http-worker's per-task memory and the unit budget (the §11 measurement: ≈3.34 MiB anon/slot, 256 → 834–873 MiB, oom=0). Done (2026-10-23), commit fb18f8f. deploy/systemd/crawler-http-worker.service: MemoryMax 384M→900M with rationale; docs/deploy.md §2 — the budget table (the sum of caps 2180M/8640M at 8192M RAM with an explanation), the Go services line ≈1.0–1.1 GiB < 1.3 GiB of spec 3.2, the daily T-093 — the closing condition; docs/load-measurements.md §3 — a note on the completed rollout (the step of 128 ≈380 MiB barely fits; 256 — 47.6 domains/s, avg ≈855 MiB; the old criterion marked historical, the active one: oom=0, NRestarts=0, anon ≤900 MiB), the order of removing the server drop-in. The follow-up (the stale calculated 150 domains/s and "tens of KiB" in configuration.md §http) closed by a separate commit 2787707. Review 2 rounds: no P0/P1; rejected P2-1b (MemoryHigh/alerts — only with a measurement), P2-2 (configuration.md — coordination with T-142), P3-6 (a foreign PG drop-in). Marked for the future: the dev stack's compose mem_limit 384m at 256 slots (parity removed deliberately), MemoryHigh≈850M/alerts over memory.events — with a measurement.

- The stage 21 wave finished (2026-10-23): T-141/T-142/T-143 moved
  to the archive (stage 21 of this file). After the deploy — remove the temporary
  T-093 measures on the server (the instructions — docs/redis.md §3.2).

## Stage 22. Improvements from wave 21 saturation (2026-10-23)

The source: the wave 21 saturation + the q:dead 5507 investigation (the root — the Redis OOM window of 22.10 16:00–18:15, the post-mortem load-measurements §11.1; not render tar pits). The code tasks — by subagents, the operational ones (T-149/T-150/T-151) — by the main session; review: the reviewer pools exhausted all day, 6 tasks without an external review (the facts in the commits, the compensation — full test+lint+integration runs and the main session's conflict analysis).

- **T-144** [done] — The dashboard thresholds = the effective ones via queue.StreamWatermark (75K single / 125 q:dns batches), a queue_watermarks map in the snapshot, per-stream highlighting instead of the sum (TZD §5.4 — by intent, the deviation from the letter pinned), the coupling tests with the monitor threshold. Commit f5ba43d.
- **T-145** [done] — MemoryHigh=850M of the http-worker unit; monitor: the unit_oom alert + the crawler_unit_oom_events{unit,kind}/crawler_unit_restarts{unit} metrics (the source — systemctl show + /sys/fs/cgroup memory.events, one Warn per episode); operations.md §4.3/4.4. Commit 44ac334.
- **T-146** [done] — The dev stack's http-worker mem_limit 384m→900m (parity with production), the stale default rationales in config.example/config.go brought to the §11 facts (47.6 domains/s, 3.34 MiB/slot). Commit ef9a2ad.
- **T-147** [done] — An early Put of page bodies into the content store + a reference check in ContentSaver (originals inherit, extras — Delete): an http-worker slot without holding bodies, calculated ≈1–1.5 MiB/slot (the goal ≤450 MiB on 256 slots); the refcount invariant puts≥claims confirmed by E2E; workerReadBody — a preallocation by Content-Length. The pack protocol unchanged. Acceptance — a slot memory measurement in production. Commit eb28315.
- **T-148** [done] — Render tar pits (a render.timeout timeout by DeadlineExceeded) are finalized without a render (the outcome — raw HTML, rendered=false, a q:analyze publication, XACK) instead of burning attempts in q:dead; render.timeout_attempts=2 (≤ redis.max_attempts, a cross-validation), the counter by the PEL delivery number (ProcessingAttempt — additively in queue); runbooks §2.5 (the dead investigation procedure), render.md. Commit 35bed67.
- **T-149** [done, operational] — XTRIM q:dns MAXLEN 125: 1754 legacy batch_size 5000 batches trimmed (617 MiB; safe — PG is the source of truth, a 30 min lease); Redis 683→104 MiB; maxmemory 512mb and the 640M cgroup returned; t092-trimhist stopped (replaced by the T-141 TrimLoop). The used<150 MiB criterion met.
- **T-150** [done, operational] — q:dead 5507: the subjects check_state=done without a manual requeue (an SQL check), XTRIM MAXLEN 0; the post-mortem §11.1 (2786 q:http/2685 q:render/36 q:dns, attempts spikes of 9/11); a PEL tail of ~940 with RC=3 keeps adding hundreds — a repeated XTRIM after stabilization (279 on 23.10 — under control).
- **T-151** [done, operational] — the zstd dictionary trained (crawlerctl dict train, the testdata/laravel-samples samples → /opt/crawler/samples; ID f2ddc163, 5233 B, −32% on the samples), placed /var/lib/crawler/content/dict.zst; the writers pick it up at the next restart (the wave deploy), the control — the load log.
- **T-152** [done] — A Redis OOM does not burn attempts: queue.InfraError/IsInfra (a typed classification of go-redis OOM/loading/readonly/transport, Canceled/DeadlineExcluded cut off); an XADD retry (2×, a backoff of 250ms×2) in Publish; a fuse pause of reading in the http/render workers on a persistent InfraError (a Reliab.Writable probe — SET crawler:queue:write-probe TTL 2h before every claim pass: zero claims with unavailable writes, RetryCount stable); the integration TestResilience_OOMDoesNotBurnAttempts (a live maxmemory scenario: the attempts do not grow, after freeing — XACK without q:dead). Commit 41b7a47.
- **T-153** [done] — redis.recover_batch=64 / recover_interval=30s (+ DefaultRecoverBatch 16→64, wired into 4 workers), the invariant "a batch walk < claim_min_idle" (64×5s ≈ 5.3min < 10min, ×1.9) — the self-recovery of a ~5K PEL ≈ 39 min instead of 2.6 h; the formula in docs. Commit a42eab8.
- **T-154** [done] — The crawler_queue_dead_buried_total{queue} counter in Reliab.bury (silent deaths visible); monitor: an alert of a persistent q:dead and Redis used/maxmemory > 80% (crawler_redis_memory_ratio/bytes, hold-last), one Warn per episode; the dashboard q:dead highlighting aligned. The subagent stopped by a limit after the commit — verified and merged by the main session. Commit 3f5d562.
- **T-155** [done] — Consumer hygiene: a periodic (redis.trim_interval) cleanup of strictly empty ones (pending=0, idle ≥ 2h) via XGROUP DELCONSUMER — a TrimLoop sibling cycle in the scheduler, queue.WorkingGroups (stream→group) pinned by an external test package; a PEL re-check before deletion (a revival race). The subagent stopped by a limit after the commit — verified and merged by the main session. Commit 6062a00.
- **T-156** [done] — Runbooks §2.6 "Massive q:dead growth at a Redis OOM": the signature (attempts 4–11, the originals in a narrow window, deaths in claim waves), the diagnostics (journalctl OOM, INFO memory), the recipe (no requeue — an SQL check, XTRIM, a PEL tail control); the §2.5 fact fix (the 5507 breakdown: 2786 q:http/2685 q:render/36 q:dns); redis.md §4 — a noeviction example note. Commit cc7d29f.

- The stage 22 wave finished (2026-10-23): T-144…T-156 moved
  to the archive (stage 22 of this file). The wave's review: the reviewer pools exhausted
  all day — 6 tasks without an external review (the fact in the commits), the compensation
  by full runs; a candidate for a post-factum review of the wave's diff.

## Stage 23. Investigating the q:dead dead-letter queue — Redis OOM (2026-10-23)

The source: an investigation of 5507 q:dead records in production (a full XRANGE dump +
logs, 2026-10-23 ~06:20 MSK). The root cause — not render tar pits
(the T-148 premise about the death causes refuted, the production part
remains): **the Redis OOM window of 22.10 16:00–18:15 MSK** — `q:dns` accumulated
1887 legacy batches of the batch_size 5000 era (~330 KiB, ≈620 MiB of
the temporary maxmemory 900M, T-093), `noeviction` refused all XADDs
(http-worker 16,953 OOM publication errors, render 1953, dns 392,
scheduler 243) → processings finished with a result publication error →
deliveries without XACK accumulated attempts by claims → q:dead. All the dead tasks
were published in the 15:00–19:00 MSK window (2786 q:http — all within the hour of 15:00,
2685 q:render, 36 q:dns batches), the deaths stretched over 12 h (18:00–05:24)
by the slow PEL walk of RecoverLoop (16 claims/30 s ≈ 2.6 h/walk);
attempts 9–11 at a limit of 3 — OOM also hit the publications into q:dead itself.
At night the workers gave no processing errors — the "death waves" are
the mechanical burn-down of the same cohort. Agreed with the operator 2026-10-23.

- **T-149** [done] — Operationally, P0: a q:dns drain — free the 620 MiB of legacy batches. The T-141 caps (250 batches×64 KiB) cannot trim the stream now: the gate is closed (a group lag of 1886 batches > the watermark 125 — the scheduler does not publish, XADD MAXLEN does not fire), TrimLoop frees only the confirmed prefix, and dns-worker stands at the q:http publication gate (depth 75016/75000) — a drain of ~1 batch/min ≈ 30 h of a standing jam at a memory of 650/900 MiB. Actions: `XTRIM q:dns MAXLEN 125` (safe: PostgreSQL is the source of truth, `queued` domains without a result will be returned by the 30 min lease, re-scheduling in batches of batch_size 1000×64 KiB; runbooks §2.3), a control of memory/queues and of the absence of q:dead growth after; then remove the remaining T-093 temporary measure (docs/redis.md §3.2: return `maxmemory 512mb`, the redis unit `MemoryMax=640M`, a restart; the external t092-trimhist already removed). Criterion: the Redis used_memory < 150 MiB, the pipeline keeps draining without q:dead growth. Spec: §3.2, §5.3.

## Stage 24. Stabilization from the control measurement (2026-10-23)

The source: the control measurement with the T-092 kit 2026-10-23 ~11:00 MSK
(the 1 h / 6 h windows from metrics.tsv — the sampler has not been stopped since 22.10;
the summary and the table — docs/load-measurements.md §12). The conditions against §11
changed: ≈47 million domains in the base across dozens of zones (com 13.87 million,
ru 6.27, xyz 4.32, top 1.53, shop 0.93…; the population went outside
ingest files — ingest_files has only ru/rf/su), a backlog of 40.3 million
pending. Speeds: HTTP 21–23 domains/s on 256 slots (a mean task of 11.5 s,
p50 ≈ 13.6 s, 60% of tasks in the 10–30 s bucket; the transport mean 0.54 s/request
at 3.1 requests/task — the bottleneck is not the network), DNS 26–28 domains/s (throttled
by the q:http gate=75,000, not by its own cap), render 0.2 pages/s.
Problems: http-worker — two fatals per day (an OOM kill 05:48, an exit-code
09:17 at a peak of 900.5 MiB = MemoryMax) and MemoryHigh throttling (120,095
events); PSI CPU some 91–95% at a load of 25/4 vCPU (postgres 177% —
the monitoring aggregates over 47 million rows, chromium 140% — render cores,
analyzer 66%); q:render 103,868 at a watermark of 100K, last-delivered-id
frozen since ≈08:59, a lag of 99 thousand — permanent certificate errors
retry in a circle, q:dead +366; unbound 733.9/734 MiB (1.19 million
reclaim events), dns-worker pressed against 256 MiB. The disk 72.3 → 40 GB over
20 h (≈36 GiB/day: content 20.3 + DB 15.5) — P0: horizontal
scaling (a server/disk) unavailable by the operator's decision of 2026-10-23,
S3 offload and archiving postponed — the decision "content
retention + a DB trim" taken (T-166; discussed with the operator: subdomain_results
rows are written only for live domains, the passports + the registry at
a backlog of 40 million ≈ 25–30 GB — fits into the free ~40 GB),
a one-off cleanup of what already accumulated — T-167, disk observability —
T-162.
The "50 domains/s" goal postponed by the same decision: of its groundwork only the tasks
solvable by code are included in the stage (T-163…T-165).
A supplement (2026-10-23 ~15:00 MSK, cross-checking the plan against production after
scheduling the wave): a scheduler freeze by a phantom q:dns lag —
the publication gate closed since 22.10 23:29, the pipeline takes no new domains
at 40.3 million pending; the §12 measurement did not see it (the drain of the stream's remainder
masked the closed gate until ~13:00, "DNS 26–28 domains/s") — T-168 (P0).

- **T-157** [done] — P0-2, the http-worker's memory stability: two fatals
  per day on 256 slots (the §12 measurement: 05:48 killed by OOM killer,
  09:17 an exit-code at a peak of 900.5 MiB = MemoryMax; MemoryHigh=850M
  fired 120,095 times — reclaim storms and swap slow the worker, at
  256 slots the cycle will repeat). Operationally: `http.task_concurrency`
  256 → 192 on the server (the stage 22 measurement: 192 slots — 860–893 MiB,
  oom=0, NRestarts=0; the speed will dip ≈22 → ≈17–19 domains/s — the
  50 domains/s goal postponed, stability matters more), synchronously update the
  default comment in config.go and docs/configuration.md §http (256 → 192 until
  the T-163 decision). The systemic solution — lowering the slot's memory (T-163:
  body streaming), after it return 256 per a measurement. Criterion: a day without
  unit fatals (memory.events oom=0, NRestarts=0), anon ≤ 850 MiB,
  the MemoryHigh events episodic, not continuous. Spec: §3.2, §5.5.
- **T-158** [done] — P0-3, the units' CPU priorities: the pipeline against
  the harness (the §12 measurement: PSI CPU some 91–95%, a load of 25/4 vCPU; postgres
  177% + chromium 140% + analyzer 66% CPU — http-worker gets ~90%
  of one core). Scaling is unavailable — redistribute the existing
  4 vCPU via CPUWeight (systemd, the 1–10000 range, the default 100):
  http-worker and dns-worker 1000 (the pipeline's critical path),
  render-worker 300, analyzer 200 + CPUQuota=150% (background analysis must not
  take CPU from checking), monitor/web/dashboard 50. Write into
  the deploy/systemd/ units (an update — deploy.md §4.6), on the server before
  the deploy — `systemctl set-property` (the drop-in removal order — the
  T-143 sample). A measurement before/after with the T-092 kit (PSI, the stage speeds).
  Criterion: PSI CPU some < 80% in steady state, the units' NRestarts 0,
  the HTTP speed does not fall during the analyzer's background passes and
  zone recalculations. Spec: §3.2, §10.
- **T-159** [done] — P1-4, render cores: permanent certificate errors
  burn attempts and hold the queue (the §12 measurement: q:render 103,868 at
  a watermark of 100K, last-delivered-id frozen since ≈08:59, a lag of 99 thousand,
  0.2 pages/s at a demand of ≈1.2; chromium 140% CPU; q:dead +366 "the attempt
  limit of 3 exhausted" — cert errors and 25 s tar pits). `ERR_CERT_
  AUTHORITY_INVALID`/`ERR_CERT_COMMON_NAME_INVALID` — permanent
  host properties, a retry is pointless: classify as
  permanent per the ErrTooLarge sample (renderworker — "a permanent
  page property", T-142): XACK with recording the record's outcome "render
  impossible: certificate" (the domain's HTTP result is already stored;
  a partial result — spec §8), publishing q:analyze without a DOM. After
  the deploy the queue drains by itself (the watermark will move away from 100K);
  sort out the 366 q:dead records per runbooks §2 (a check + XTRIM,
  the T-150 sample). Criterion: q:render's last-delivered-id moves (the lag
  falls to the background level), the daily q:dead growth from the cert class
  = 0, chromium's CPU falls in steady state. Spec: §7, §8, 3.4
  (at-least-once).
  The q:dead investigation done (2026-10-23 ~19:10 MSK, after the 66b5d41 deploy):
  the composition of 710 q:render records "the attempt limit of 3 exhausted" — 366
  incident + 344 restart burn-downs of the PEL at the deploys of 09:03/18:32/18:57
  (the attempts burned in the cert era before the fix); check_state of all 415 domains
  = done → finalization by analyzing the raw HTML (runbook §2.5 item 4: 710 XADD
  q:analyze fresh), XTRIM q:dead MAXLEN 0. The residual death source:
  ERR_CONNECTION_REFUSED (~5/min) and ERR_CERT_DATE_INVALID — outside
  the cert class of the task, a candidate for extending the permanent list.
- **T-160** [done] — P1-5, the memory caps of the resolver and dns-worker
  (the §12 measurement: unbound 733.9/734.0 MiB at a MemoryMax of 700M — 1.19 million
  reclaim events, the cache grew from 452 MiB (§6) at ≈47 million names;
  dns-worker periodically at 256 MiB; MemAvailable 2.6 GiB — there is a margin). Raise MemoryMax: unbound 700 → 800M (deploy/systemd/
  unbound.service.d/90-crawler-budget.conf), dns-worker 256 → 384M
  (deploy/systemd/crawler-dns-worker.service; the budget comment
  "Go services ≈1.3 GB" recalculate); on the server before the deploy —
  set-property. Control: memory.events oom=0 for both, the 9103 histograms
  — the median of DNS task durations does not degrade. Criterion:
  unbound's MemoryCurrent < MemoryMax with a margin of ≥ 50 MiB,
  recursion.time.median does not grow (unbound-control stats). Spec: §3.2,
  §5.4.
- **T-161** [done] — P1-6, the monitoring aggregates over 47 million rows
  (the §12 measurement: `GROUP BY check_state` — scheduler.BacklogLoop every
  30 s, a pass of 17+ s with parallel workers; `UPDATE zones` — monitor.
  ZoneStatsLoop; `SELECT z.id…count` — internal/status for
  the landing/dashboard; together — most of postgres's 177% CPU).
  Three parts: (1) single-flight — a new aggregate pass does not start
  until the previous one finished (BacklogLoop and ZoneStatsLoop;
  insurance against the "a pass longer than the interval" picture as the table grows),
  a Warn when a pass's duration > the interval; (2) the BacklogLoop interval
  moved into the `scheduler.backlog_interval` config, defaults 30 s → 300 s
  (the dashboard's gauge tolerates five-minute freshness;
  zone_stats_interval is already configurable — raise the default); (3) a cache
  of the status response (zones with counters) TTL 1–5 min. The systemic
  alternative if insufficient — incremental zone counters on
  status transitions (a separate wave). Criterion: in steady state
  pg_stat_activity does not show full-scan aggregates more often than the interval,
  postgres's CPU (top) lower by at least half a core against the §12 measurement.
  Spec: §5.9, §9.
- **T-162** [done] — P2-7, delivering alerts to the operator + the missing
  thresholds (the §12 measurement: all the P0s found manually; the monitor/alerts.go
  framework (T-080/T-145/T-154) already covers the disk in percent
  (content.disk_free_percent), queues at the watermark, OOM/restarts
  (memory.events
  oom/oom_kill, NRestarts), q:dead, Redis memory — but the signal
  stays a Warn in the journal, the operator does not read it). To do: (1)
  sending fired/cleared alerts to Telegram — monitor calls
  deploy/telegram-notify.sh (T-136) with a debounce, secrets via
  the unit's EnvironmentFile; (2) the missing thresholds: an absolute minimum
  of the disk in GB (on an 79 GB disk a percentage threshold is late: 10% = 8 GB
  ≈ 5 h until filling at ≈36 GiB/day — a minimum of 15 GB, an escalation at
  10 GB), MemoryHigh throttling (the high counter — a harbinger of the T-157 fatals), a frozen queue (a group's last-delivered-id not moving
  ≥ 15 min at depth > 0 — the T-159 poison signature), a scheduler
  stop (no queue publications at pending domains > 0
  ≥ 30 min — the T-168 freeze signature; the scheduler's WARN "publication
  suspended" is written once, journal silence is not counted as work), PSI CPU some > 90% steadily for 30 min. Criterion:
  a test firing of every threshold
  arrives in Telegram, clearing — a separate message, no duplicates
  (a debounce); monitor's journal does not flood. Spec: §10.4, §5.9, §10.3.
- **T-163** [done] — The postponed 50 domains/s goal (code): slot memory
  of http — bodies are held in memory until the end of the task (the §11/§12 measurements:
  ≈3.3–4.4 MiB/slot, 256 slots ≈ 855 MiB — the cause of the T-157 fatals;
  the direction named in §3). Variants: streaming bodies into the content-store as
  they download (a block written on buffer fill, the body not
  held entirely) or buffering only the final body
  (redirect chains not accumulated). Requires revising the alias deduplication
  (several records per one body hash — T-041; a block
  written before knowing the final URL — a revision of the store.go contracts).
  Criterion: ≤ 1.5 MiB/slot on the production mixture (a sampler measurement), 256 slots
  in a MemoryMax of 900M without MemoryHigh throttling, the dedup/equivalence-class
  contracts and the store_test.go tests green; after — return
  task_concurrency 256 (T-157). Spec: §5.5, §5.7.
- **T-164** [done] — The postponed 50 domains/s goal (code): the timeout tail
  — 60% of tasks in the 10–30 s bucket (the §12 measurement: a mean of 11.5 s with a transport
  of 0.54 s/request; the timeout class 42% of tasks, network 31% — dead hosts
  of the com/xyz mixture burn slots until total_timeout). A dead-host cutoff in
  httpworker: apex and www both finished with connect-timeout/connection
  refused — do not check the task's other subdomains, finish with
  the available result (the subdomain records — no_http by the §8
  classification rules; a cutoff sign in the record — observability and
  a control cross-check). The `http.dead_host_cutoff` config (on/off,
  the default — by the control sample's outcome). Criterion: a mean task on
  the mixture ≤ 7 s (the 9104 histograms), a share of false cutoffs < 1% —
  a control sample of cutoff domains by a repeated full check.
  Spec: §5.5, §8.
- **T-165** [done] — The postponed 50 domains/s goal (code): head-of-line
  parking IPs — per_ip_limit=4 with the FIFO consumption order of q:http
  (the T-092 §3 finding, not confirmed instrumentally; in the com/xyz mixture
  millions of domains per parker IP — waiting for the semaphore stretches
  tasks to 20 s). Two parts: (1) observability — a histogram of waiting
  for the per-IP semaphore in /metrics of http-worker (9104): the share of tasks that waited
  for a per-IP slot > 2 s; (2) by the numbers — IP diversity of consumption
  (the slot dispatcher prefers a delivery with an IP free under the limit
  — a heuristic over the read buffer's head) and/or a raised per_ip_limit
  for parker prefixes (a list in the config). Criterion: the waiting metric
  in 9104; the share of tasks with a per-IP wait > 2 s < 5% on the
  production mixture; the HTTP speed does not fall (a measurement before/after with the T-092 kit).
  Spec: §5.5, §5.9.
- **T-166** [done] — P0-1, the disk without new capacity: retention
  of file content + a trim of subdomain_results (the operator's
  2026-10-23 decision: scaling unavailable, S3 and archiving postponed).
  The measurement rationale: a growth of ≈36 GiB/day (content 20.3 + DB 15.5);
  subdomain_results rows are written only for live domains (the su cut: 70,981
  alive → 140,944 rows ≈ 2/domain, not_delegated/no_http/temp_error —
  0 rows), the heavy jsonb — only at answering sites (a Laravel domain
  ≈ 6–7 KiB, a parking ≈ 0.5–1 KiB). The outcome calculation: the passports of the live +
  the registry over the whole backlog of 40 million ≈ 25–30 GB — fits into the current ~40 GB
  free. Part 1 — a content purge: records with `checked_at` older than
  `content.retention` (a default of 6 h; the stationary content volume =
  the daily inflow × the window — at 20 GiB/day a window of 48 h would hold ≈40 GB
  and eat the disk, 6 h ≈ 5 GB), a domain NOT a Laravel finding, not
  render_pending (or rendered) → a transaction `content_hash → NULL` +
  `contents.refcount − 1`, the contents row deleted at 0; the space
  is returned by the existing hourly pack compaction and autovacuum;
  an hourly cycle, in batches (the samples — BadHashes/compaction);
  an interlock — at a q:analyze depth above the threshold (the analyzer does not keep up,
  the bodies are still needed) the pass is skipped.
  Part 2 — a trim of the jsonb of non-findings by the same criterion: an UPDATE
  of headers/cookies/tls/redirects/addrs/cname → NULL; the passport
  (http_status, final_url, error, response_ms, checked_at, scheme,
  subdomain_id, variant) always stays — domain_info and the dashboard alive;
  non-indexed columns → HOT (fillfactor 85 — migration 000002),
  batches of 10–50 thousand rows, a bloat control. The timing must wait for
  consumers: the analyzer reads the bodies and headers/cookies
  (analyzeSelectSQL; the findings' body signs — e.g. vite_assets 0.2 of
  0.7 at belajarceria.site), render replaces the content of SPA records —
  earlier than retention and until rendered do not delete. The pinned deviations
  (agree with the customer): re-analyzing non-findings by new rules
  impossible (the application — at the 6-month recheck); a recheck loses
  the "did the site change" comparison (it will overwrite); findings keep content
  always (the proof + the T-074 re-analysis). Criterion: the total growth
  ≤ 5 GiB/day on the production mixture (the sampler), content_size reaches a plateau
  after draining the backlog, the findings' content not deleted (a sample cross-check),
  q:analyze/q:render do not fail on a NULL hash (the T-056 base), refcount
  invariants under a failure between UPDATE and decref — tests (the T-044 sample).
  Spec: §5.5, §5.7, §6.1, §3.2.
- **T-167** [done] — P0-1, a one-off cleanup of the accumulated (a backfill
  of the T-166 retention): data already checked before retention's introduction
  will not fall under the same criteria automatically just because of the date —
  an explicit campaign over the historical array is needed. The tooling ready
  (2026-10-23): runbooks §5 (path A — the regular T-166 cycle, the records' age
  exceeded — it drains by itself; path B —
  deploy/sql/t167-retention-backfill.sh with PSI/lag gates);
  executing the campaign — right after the deploy (the T-166 code needed), acceptance
  per runbooks §5.5. The starting sizes
  (2026-10-23 evening): content 20 GB / ≈3.1 million blocks (the
  contents index 261 MB), subdomain_results ≈3.7 GB over 1,138,077 live
  checked (status=4, done), the domains registry (future work) not
  touched. The mechanics — the T-166 criteria (not a Laravel finding, not
  render_pending, older than retention) over the historical records: either
  the regular T-166 cycle (the records' age already exceeded — it drains by itself),
  or when time is short — operational SQL per the runbook (a transaction
  `content_hash → NULL` + decref + `DELETE refcount ≤ 0` + a jsonb trim
  in batches; the operational drain sample — T-149, the cleanup — T-099).
  The differences from the regular cycle (a campaign, not background): (1) reduced batches
  under CPU starvation (P0-3, T-158 not closed) with pauses when the lags of
  q:render/q:analyze grow and PSI CPU > 95%; (2) after the pass — VACUUM
  (ANALYZE) of the affected subdomain_results partitions, a bloat control;
  (3) a compaction control: the 0.5 threshold exceeded at a mass
  orphan block, the hourly cycle will rewrite almost all packs —
  watch the disk during the rebuild (a temporary double footprint) and
  the duration; (4) the render cohort: q:render stands (a lag of 99 thousand,
  T-159) — live SPA records render_pending excluded until the drain, the
  regular T-166 cycle will finish them after. Criterion: disk_avail grows by
  ≥ 15 GB (content 20 → ≤ 3–5 GB: findings, render_pending, retention freshness),
  subdomain_results after VACUUM ≤ 1.5 GB, laravel_findings
  and the findings' content untouched (a sample cross-check), a q:dead growth of 0,
  the pipeline without degradation (the stage speeds and PSI within the §12
  measurement). Spec: §5.7, §6.1, §3.2. Samples: T-149, T-099, T-044.
- **T-168** [done] — P0-8, a scheduler freeze by a phantom q:dns lag
  (a finding of cross-checking the plan against production 2026-10-23 ~15:00 MSK, after the §12 measurement
  and scheduling the wave): the q:dns publication gate closed since 22.10 23:29 MSK —
  the scheduler WARN "publication suspended: the queue depth at the threshold"
  stream=q:dns depth=1828 watermark=125 (09:04, once — after that
  silence, the freeze is invisible in the journal); no XADD, dns-worker
  idle (inflight=0, the consumer idle ~2 h, XLEN=1, entries-read=387)
  at 40.3 million pending — the day's progress was the drain of 387 batches
  remaining in the stream until 22.10 23:29 (the masking: §12 at ~11:00 still saw
  "DNS 26–28 domains/s"), from ~13:00 new domains are not taken; the visible symptom —
  "empty" q:http/q:analyze. The mechanics: Redis's group lag
  (entries_added − entries_read) does not subtract records cut by XTRIM
  as undelivered — ~1753 legacy batch_size 5000 batches from the
  T-149 drain stayed in the lag forever; the q:dns gate threshold =
  min(queue_high_watermark 100000, DefaultStreamMaxLen 250/2) = 125 —
  tiny; the BackpressureGate.Len protection is designed for Lag = −1/NULL
  ("a trim of undelivered"), but Redis 7.0.15 in this scenario returns
  a positive number; the same phantom holds the eternal queue_overflow alert
  of q:dns (1753 > 125, not cleared since 09:04). Actions: (1) operationally,
  immediately — XGROUP SETID q:dns dns-worker $ (there are no undelivered in
  the stream: the only record is already delivered; even when skipping
  a record the 30 min lease will return the domains to pending — PostgreSQL is the source of truth;
  do NOT restart Redis: it would kill ~107 thousand SPA tasks
  of q:render that have neither the render_pending flag nor dead records —
  a loss until the 6-month recheck); the control after: q:dns publications
  resumed, dns-worker's inflight > 0, q:dead not growing; (2) a code fix
  of the gate — do not count the trimmed undelivered as lag (the direction:
  the undelivered are no more than XLEN — gate by min(lag, XLEN) + pending,
  or reset the group's trace at an XTRIM below last-delivered-id); a test
  of the "XTRIM of undelivered → a positive lag" scenario on live
  Redis 7 semantics (the fake in backpressure_test.go does not catch it —
  the server semantics); along the way a WARN of TrimLoop q:http "the trim point does not
  move — an orphaned group with a frozen last-delivered-id
  holds the trim" (09:03/09:26) — the same family of phantom group traces;
  (3) observability — the alert "no queue publications at pending
  domains > 0 ≥ 30 min" (added in T-162); (4) a runbook — a section
  "a scheduler freeze by a phantom lag": diagnostics (a one-off
  scheduler WARN + XINFO GROUPS lag against XLEN), the SETID fix.
  Criterion: a day of live q:dns publications (entries-read and
  last-delivered-id grow, dns-worker's inflight > 0, pending falls),
  the q:dns lag no higher than the stream's live depth, the queue_overflow alert for
  q:dns cleared and not returning after TrimLoop/XTRIM; the phantom-lag test
  green. Spec: §5.3, §4.2, §3.4.
  The operational part (1) done (2026-10-23 15:24 MSK, the main
  session): XGROUP SETID q:dns dns-worker $ — the lag 1753 → 0 (entries-read
  reset to NULL), the gate open; a 90 s control: XLEN q:dns 1 → 44
  batches, entries-read 387 → 2144, dns-worker's inflight 32 (~33 domains/s),
  q:dead without growth (366), no new scheduler WARNs. Left: (2)
  the gate's code fix + tests, (4) the runbook; (3) — T-162 territory.
- **T-169** [done] — P1-9, render capacity — draining q:render and unblocking
  the disk (the 23.10 monitoring after T-159: a drain of 29–34 tasks/min = the inflow, the lag
  of 98.9 thousand at a watermark of 100K not melting since the evening of 22.10; the demand at a free
  gate ≈1.2 pages/s — the §12 measurement; the disk coupled to the drain: ~99 thousand
  unconsumed q:render messages hold their cohort's content under
  retentionpending protection (the render-window fix, 1f48425) — the historical
  ~20 GB are cleaned only as the messages are consumed, T-167's path B
  will not pass the lag gates; by 24:00 of 23.10 the disk settled into a plateau of ~34 GB, but
  the cohort is not unloaded). Steps with a measurement after each (with the
  T-092 kit, 30–60 min of steady state): (1) operationally `render.contexts 4 → 8`
  on the server (the T-142 precedent: 3→4; the Chromium budget ≈2 GB, an RSS of ~550 M
  at the restart threshold of restart_memory_bytes 1536 MiB — watch),
  synchronously the render-worker unit's MemoryMax 128M → 256M (117–128 M + swap
  already now — a slot-time deficit, the T-160 style) and the defaults of
  config.go/config.example.yaml/configuration.md; (2) if insufficient —
  `render.timeout 25 → 15–20 s` ONLY by the 9105 histogram: the share of successful
  renders in the cut tail must not exceed ~20% (the 10–25 s bucket —
  the most massive, DOM coverage must not be cut), the gain — the T-148 tar-pit finalization
  in 2×timeout instead of 2×25 s; (3) with the balance kept — a second
  render-worker instance (a systemd template against the same Chromium
  ws://127.0.0.1:9222: the stream group supports many consumers,
  the unit's memory budget separate; at one Chromium's saturation — a second
  chromium instance on 9223 in the second worker's config). Interaction with
  T-158: after its CPUWeight (render 300) repeat the measurement — capacity against
  priority. Control at every step: the q:render lag's fall rate,
  PSI CPU some < 95%, the 9105 histograms and memory (chromium's RSS below the
  restart threshold), the DNS/HTTP speeds within the §12 measurement, q:dead without growth.
  Criterion: the q:render lag falls monotonically ≥1.5 thousand/h (draining 99 thousand
  ≤3 days), the gate moves away from 100K, the 75K alert goes out, content_size falls as
  the retention cohort frees, the units' NRestarts 0, a daily
  q:dead growth of 0. Spec: §7, §3.2, §3.3.
- **T-170** [done] — P1, PostgreSQL CPU: a partial index of deleted
  domains for ingest cleanup (the diagnostics of the 100 % CPU incident of 23–24.10:
  4 cores, a load of 21–23; the T-020 cleanup candidates selection — a mean of 17.4 s,
  2056 calls ≈ 10 h of DB time over 2.7 days at zero deleted domains —
  the planner without a status index drove the "from subdomain_results" plan:
  a full scan of 2.6 million rows + 2.6 million PK probes into the 41.8-million domains, once a
  minute in vain). Migration 0013 `domains_deleted_idx (zone_id, id)
  WHERE status = 6`: the key = the PK prefix (the pass's ORDER BY, an early LIMIT stop), empty at zero deleted; the status literal sewn into the cleanup SQL
  (cleanupSelectSQL/cleanupDeleteSQL via fmt.Sprintf(model.StatusDeleted),
  the dispatch.go style — the planner cannot prove a parameter in a partial index's predicate). HOT: status only in the predicate — non-HOT only transitions
  involving deleted (the T-019 delete files, the T-018 revival), mass
  result-status updates stay HOT; TestSchema_DomainsHOTColumns
  extended with the compromise. Criterion: the candidates selection plan — an Index Scan
  domains_deleted_idx (an EXPLAIN in production), the cleanup pass time ≤ single
  ms at zero deleted. Spec: §6, §13.1, docs/schema.md "HOT updates".
- **T-171** [done] — P1, PostgreSQL CPU: the monitoring windows and the aggregates' parallelism
  (the 23–24.10 incident — the second DB CPU consumer: the daily window
  done_today mean 3.3 s × 10,094 ≈ 9.3 h, the temp_error share mean 7.4 s ×
  2157 ≈ 4.4 h, the hourly windows of subdomain_results — a seq scan of 2.6 million rows
  ×2 per snapshot; the aggregates' parallel workers took 3 of 4 cores
  from the pipeline). Parts: (1) migration 0014 `domains_last_checked_idx
  (last_checked_at)` + `subdomain_results_checked_idx (checked_at)` —
  a range scan of the window instead of a full pass; HOT untouched (both
  columns change only together with check_state / only on INSERT, the
  T-104 class), TestSchema_DomainsHOTColumns extended; (2) `db.Options.
  AfterConnect` + the `db.NoParallelGather` hook (SET
  max_parallel_workers_per_gather = 0 on the pool's connections) — the wiring
  `service.Options.DBAfterConnect`, enabled at monitor, dashboard,
  scheduler (the batch scheduling path — index scans without parallel
  plans, only the background aggregates slow down: a monitoring freshness of
  tens of seconds is enough, the progress-stop alert threshold —
  5 zone recalculation periods). Complements T-161 (single-flight/the
  300 s intervals/the summary cache): the frequency already lowered, here — the query's
  cost and the core division. Criterion: the windows' EXPLAIN — an Index (Only) Scan over the new
  indexes; in pg_stat_activity the monitor/dashboard/scheduler connections have
  max_parallel_workers_per_gather = 0 (no parallel worker backends
  from their sessions); the windows' mean_exec_time in pg_stat_statements ≤ single ms.
  Spec: §5.9, §6, docs/schema.md "Monitoring window indexes".
- **T-172** [done] — P2, render mortality outside the cert class: cheap
  finalization of permanent and attempts-exhausted pages (the incident
  of 23.10 21:20–21:31: a wave of 459 q:render deaths in 11 min — the worker reached FIFO into a fresh inflow region (originals 19:24–19:57, an hour of queue standing
  at the watermark) with a toxic mixture of xn--p1ai parkings: per 20 min of logs
  timeouts 37, ERR_CONNECTION_REFUSED 34, ERR_CERT_DATE_INVALID 16;
  the T-159 cert class finalizes correctly meanwhile (+31/20 min),
  the T-148 tar pits too (+21) — the out-of-list codes and MIXED
  sequences die: timeout(1) → refused(2–4): the T-148 finalization
  requires a tarpitError on the current delivery, a mixed series does not
  reach it and burns all 4 deliveries → q:dead; every death ≈ 3 extra
  render slot-times and manual investigation). Three parts: (1) a measurement over the logs
  of a live run of the share of successful retries of ERR_CERT_DATE_INVALID and
  ERR_CONNECTION_REFUSED (a first entry failure → a subsequent success);
  (2) by the numbers — extend certErrorCodes with ERR_CERT_DATE_
  INVALID (an expired/inactive parking certificate is stable at the
  scale of days; excluded in T-159 as "time-dependent" — reconsider the decision by the measurement); (3) the "last attempt × page class" policy: at a delivery number = redis.max_attempts an error of the
  task class (a timeout/load error — a host property, not infrastructure)
  is finalized per the finalizeSkipRender sample instead of burial —
  q:render deaths remain only to infrastructure failures (DB/publication/
  browser), in the spirit of T-148 "q:dead — to real infrastructure errors";
  refused stays retryable at early attempts (recovery
  observed), an attempts-exhausted host gets an honest raw HTML.
  Tests per the cert_test.go/tarpit_test.go samples: a mixed series →
  finalization at the last attempt; DATE_INVALID from the first delivery;
  an infra error still dies; a finalization metric with a reason label.
  Criterion: the daily q:dead growth from q:render = 0 on the toxic mixture
  (the control — waves like 23.10), DOM coverage no lower than the measurement (the share
  of successful renders does not fall), slot-time per dying record ≥ −75%.
  Spec: §7, §8, 3.4.
- **T-173** [done] — P2, load monitoring for the optimization wave without
  hardware scaling (the operator's 24.10 decision: the server parameters do not
  grow — we optimize the code; a "before/after" diary of every change):
  a periodic system load report to the operator every 10 minutes
  (monitor.load_report_interval, 0 — off, a 1m..1h corridor) into
  Telegram by the same delivery as the T-162 alerts (no secrets/script —
  only an Info journal line and the crawler_os_* gauges). The snapshot's composition:
  the system (load, CPU utilization over the window by a /proc/stat delta, PSI
  CPU/memory, RAM/swap from meminfo); a top of processes "htop"-style — the
  utime+stime ticks and the RSS of /proc aggregated by comm (12 chromia — the
  headless-shell line, PG backends — postgres), CPU over the window in core fractions;
  the content directory's free space (statfs, the alerts' SpaceChecker);
  the q:* depth and the consumption pace — the entries-read delta of XINFO GROUPS over
  the window (Redis 7+). The first pass — the base ("n/a" paces, honest sizes);
  degradations quiet (no PSI/Redis/disk — an "n/a" section, not an accident).
  parsePSI generalized to parsePSIKind(some/full); TelegramNotifier.Send —
  an exported point of sending an arbitrary message. Criterion: every
  10 minutes six lines in the operator's group (CPU/PSI/load, top CPU, top
  RSS, RAM/swap, disk, queues with ±depth and pace); monitor's journal
  duplicates; the crawler_os_* metrics in /metrics. Spec: §5.9, spec 8.

## Stage 25. Render debt drain — equilibrium at the watermark (2026-10-24)

The source: the saturation after the wave 24 deploy (24.10, the T-169 steps all applied:
contexts 8, render.timeout 15 s by the 9105 histogram, a second worker
crawler-render-worker@9109, CPUWeight chromium 100 → 300). The fact
of equilibrium: the q:render consumption ≈ 5100/h (2 workers × 8 contexts,
the CPU cap 4 vCPU, PSI some 89–91%) equals the inflow ≈ 5100/h — the lag
of 98.4 thousand does not fall (a drain of −280…+60/h — noise). The equilibrium's cause:
the publication gate open — the shared `scheduler.queue_high_watermark`
(100 thousand) at the T-168 clamp (`min(lag, XLEN)+pending` ≈ a lag of 98.4 thousand)
gives HasCapacity=true, and the inflow (the http-worker publications + the pass
RequeueRenderPending from the accumulated) lands exactly in the appearing
capacity. The render debt in total ≈ 443 thousand: 98.4 thousand unconsumed
stream messages + **344.5 thousand domains with the render_pending flag**
(accumulated over days of the queue standing at the watermark; found by manual SQL —
no observability). The disk meanwhile stable (retention ≈ the inflow:
474 thousand rows / 3.8 GB over ~5 h; content 16 → 14 GB as
compaction progresses; 41 GB free), but the T-167 goal "content ≤ 3–5 GB"
is blocked exactly by the debt: the protected content is held by the flag predicate
(retentionpending 508 thousand deferrals — the render-window queue protection
works as designed). The operator's 24.10 decision before this wave's tasks —
accept the plateau (variant A): the pipeline in priority (T-158), the debt melts
over weeks; no horizontal scaling.

- **T-174** [done] — P0, the q:render gates: a per-queue watermark. The shared
  `scheduler.queue_high_watermark` (spec 5.10, T-026) at the T-168 clamp
  keeps the q:render gates open at a lag of 98.4 thousand of 100 thousand —
  the inflow equals the consumption, the debt drain is zero (the 24.10 equilibrium,
  stage 25 above). The solution: a separate render capacity threshold
  `render.queue_high_watermark` (config+env, 0 = shared; justify the
  default — the reference point 10 thousand: closes the gates at the current debt and
  returns a useful "draining — admitting" hysteresis), the HasCapacity check
  in renderq.go goes by it; exceeding — the regular T-066 path:
  no publication, the domain gets the render_pending flag
  by the same transaction (finished later by the pass — tasks are not
  lost). RequeueRenderPending publishes only with a free queue — closed gates stop the flag re-submission too:
  consumption (~5100/h) drains the stream (98.4 thousand ≈ 19 h), then at the
  same pace the flags (344.5 thousand ≈ 67 h) — a full drain ≈ 3.6 days against
  "never". Criterion: with the gates closed the lag falls monotonically
  ≥ 1.5 thousand/h (the T-169 criterion finally achievable), new SPA domains
  are flagged and not lost (after the drain the requeue starts — the publication
  metric), PSI some < 95%, the HTTP speed does not fall (a T-092 measurement before/
  after). Spec: §5.5, §5.10, §3.3, §3.4.
  Done (2026-10-24), commit a6acc02: `render.queue_high_watermark`
  (config+env, a default of 10 thousand ≈ 2 h of consumption buffer, 0 = shared,
  validation 0 or 1..10⁸); the cmd/http-worker wiring — the "0 → shared" fallback
  and a clamp of `queue.StreamWatermark` by q:render's maxLen (a threshold of 75 thousand
  invariantly above the default, a test; the old T-066 wiring compared the raw
  value); the gates close both paths of one RenderQueue instance —
  the T-066 publication and RequeueRenderPending, the flag mechanism unchanged;
  the overflow tests unaffected (the threshold explicit in the tests), the
  queue_overflow alert untouched (firing later than the gates — the signal "the drain does not
  keep up", documented). make test + test-integration green;
  the deepseek review: no P0–P2, 4×P3 added via amend. The operational control —
  after the deploy by the operator: the lag falls ≥ 1.5 thousand/h, PSI < 95%, the HTTP
  speed.
- **T-175** [done] — P1, render debt observability. The debt of 443 thousand
  (the stream + flags) was found on 24.10 by manual SQL: neither a metric nor an alert;
  the T-162 "frozen queue" alert catches a delivery stop, not an
  accumulation. To do: (1) a render debt gauge in monitor —
  `crawler_render_pending_domains` (the flags) + the stream lag (already present
  as queue_depth/q:render) in one summary; decide the flag counter's source
  by cost: a count over the bit mask — a full scan of 41.8 million rows
  every 5 min is unacceptable; variants — a partial index `(flags & 2)
  <> 0` (bit transitions rare — the non-HOT cost acceptable, justify per the
  T-104/T-170 sample), or an incremental counter on transitions
  (set/clear in renderq/requeue — the T-161 systemic alternative);
  (2) a threshold alert: debt > N thousand and growing (Warn, the journal + Telegram
  when secrets present — the T-162 delivery). Criterion: the debt visible without
  manual SQL (a metric + a T-173 summary line), the alert fires at a
  test threshold, monitor's journal does not flood.
  Done (2026-10-24), commit 708aa81: an incremental counter
  `crawler_counters` (migration 000015 + a backfill count at apply) —
  a partial index over (flags & 2) rejected: it breaks the HOT invariant
  of schema.md (the http_published bit changes twice per domain episode —
  non-HOT on millions of updates/day); the bit transitions — a guard
  `AND (flags & 2) = 0` at the SET (RowsAffected = the transition fact) and a shift
  of the counter ±1 by the same transaction (renderq/record); the gauge
  `crawler_render_pending_domains` + a "Render debt" summary in the report
  of T-173 (renderdebt.go, a default of 5m); the render_debt alert — firing on
  growth (a series of ≥2 passes, a threshold of 50 thousand) + a firing latch hold
  (survives not_measured, a clearing hysteresis of 90 % ceil — review round 2);
  runbooks §6 — manual operations with the bit via an atomic CTE
  (UPDATE…RETURNING + count(*)); migration 000015, docs
  schema/services/operations/configuration/migrations/render. make test
  + test-integration green; the deepseek review 2 rounds: no P0/P1,
  P2×2+P3×12 — all closed (round 1's P3-3 "cut the scan by a zero counter"
  rejected with justification — the pipeline's progress does not depend on the
  observability counter). A desync at manual SQL without the counter — the
  "UPDATE 0" symptom documented.
- **T-176** [done] — P2, shaping render demand by the DOM contribution. A measurement
  (by data, not by eye): the share of Laravel findings owed
  exclusively to the render's DOM signals (without the header/body signals
  of the HTTP phase — analyzeSelectSQL/rules), by the mixture's classes
  (parking xn--p1ai against other zones; the §12 measurement: render demand ≈1.2
  pages/s with a live pipeline). By the numbers — a decision with the operator:
  (a) debt prioritization — requeue/publications first to zones with
  a high DOM contribution, parking classes — by the residual principle
  (a change of RequeueRenderPending's order, the debt queue loses no
  records); (b) at a class's DOM contribution ≈ 0 — do not render at all
  (the verdict by HTTP signals, a product decision on coverage — agree,
  pinning the deviation per the T-166 sample). Criterion: the measurement
  pinned (docs/render.md, the shares by classes); with the accepted
  shaping — the share of render slot-time on classes with a DOM contribution ~0
  lowered ≥ by an order, the find rate does not fall (a control sample
  by re-analysis), the documentation reflects the decision.
  The measurement done (2026-10-24), commit afb72bb: docs/render.md (the section
  "The DOM signals' contribution by mixture classes", 207 lines) + a read-only script
  deploy/sql/t176-dom-share.sh. The rules classification (23): H=7
  (headers/cookies), C=16 (content) — no disputable ones. The cut of 24.10 ~19:20
  (findings 91,565 / is_laravel 33,807): xn--p1ai — 4,819 findings, 440
  rendered, dom_only 2 (0.45% of rendered), needs_dom 11 (2.5%); other
  zones — dom_only 0; in total dom_only 0.28%, needs_dom 2.0% of rendered.
  97.5% of xn--p1ai's rendered findings pass the threshold by H signals (the encrypted
  cookie — 673/675); renders: ≈88 thousand of ≈89 thousand — xn--p1ai (98–99%), an exit
  of 0.5% findings per render; q:render's PEL — xn--p1ai 97.2% (evil retries).
  The recommendation: (b) for xn--p1ai — do not render at all (an observable loss
  of 0.04–0.23% of the zone's findings, all — real Livewire/Inertia applications;
  the deviation per the T-166 sample), (a) for the rest of the debt not needed
  (the tail is already "non-xn--p1ai first", after the drain repeat the ru/com measurement
  with the script). Limitations: dom_only may overstate (a false SPA verdict),
  needs_dom — understate (render cookies); for classes without completed
  renders (ru/com — 95.5% of the undelivered debt) the DOM contribution is not measurable
  until renders. **The operator's decision (2026-10-24, evening): variant (a)** —
  debt prioritization by zones, no findings loss: zones with a high
  DOM contribution first, parking xn--p1ai — by the residual principle;
  the implementation — T-184 (coordination with T-181 — both rebuild the
  renderSelectPending selection); after the ru/com drain repeat the measurement with the script.
  The deepseek review: P1×1 (the estimates' framing) + P2×4 + P3×6 — all accepted
  and added via amend.
- **T-184** [done] — P2, prioritizing the render debt by zones —
  implementing the T-176 decision (a) (the operator 24.10: we render everything, we lose
  nothing; zones with a high DOM contribution — first, parking xn--p1ai — by
  the residual principle). The mechanism — the selection order of RequeueRenderPending
  (renderq.go, renderSelectPending): the config
  `render.requeue_deprioritized_zones` (a list of zone names to the end of the order,
  an empty default; production — xn--p1ai), a two-stage selection WITHOUT sorting
  (coordination with T-181, which removes the PK order and forces a seq
  scan with an early LIMIT stop): first a batch of domains of zones NOT in the
  list, on exhaustion (Selected = 0) — a batch of the list's zones; the physical
  locality of the flagged xn--p1ai debt at the heap's start plays into the second
  stage's hands. The q:render gates (T-174) ration the publications — the priority
  disposes of the freed capacity; the debt queue loses no
  records (everything is finished, xn--p1ai last). The publication
  of live check SPA results (renderSPAResults) is not prioritized
  (the gates bound it); the undelivered stream tail (95 thousand, xn--p1ai
  0.2%) drains FIFO as is. Observability: the pass's publications by
  stages in the log (an order control without manual SQL); a control sample by re-analysis — the dom_only/needs_dom candidates of the T-176 measurement,
  including the Livewire site xn--80afc7aid1a.xn--p1ai (a direct test of a false
  SPA verdict). After the ru/com drain — a repeated measurement
  of deploy/sql/t176-dom-share.sh (close the unknown DOM contribution
  of non-parking classes). Criterion: in the drain window the requeue publications
  go by non-listed zones first (the pass log), the ru/com findings
  arrive earlier than the FIFO order (a comparison on a control sample),
  q:render/q:dead without growth, make test + test-integration green.
  Dependencies: the T-174/T-175 deploy (the gates, the counter); T-181 — both tasks
  rebuild one selection, do them consistently (T-181 on top or
  as one change — by the actual order).
- **T-177** [done] — P2, slow PostgreSQL queries: a log + a notification
  to the operator (the operator's 24.10 request: production queries — point
  index ones, >3 s — a pathology; today you can learn about it only
  by manual SQL). Two surfaces: (1) the production config of T-086 —
  `log_min_duration_statement = 3000` (a reload, the cost only of writing
  those over the threshold) + the same threshold in the dev config; a full posthumous
  trace of every statement longer than 3 s in the server's journal; (2) the
  monitor `SlowQueryLoop` cycle (internal/monitor/slowqueries.go): every
  `monitor.slow_query_interval` (a default of 10 s) it polls
  `pg_stat_activity` — client statements running longer than
  `monitor.slow_query_min_duration` (a default of 3 s = the log threshold)
  in one Telegram message by the same delivery
  as the T-162 alerts and the T-173 load report: the header (new + "in total
  running"), the three longest lines "duration [service] pid:
  text" (a 160-char cut, the full text — in the server's journal);
  a Warn duplicate into the journal and the crawler_pg_slow_queries /
  crawler_pg_slow_queries_total{app} metrics. A pid+query_start debounce —
  the operator is informed once, a finished one leaves the DB (tens of
  records), a source failure does not fail the base; monitor's own aggregates
  (an UPDATE over 47 million rows every 5 min, the §12 measurement — a regular
  load, not a pathology) excluded by the pool's application_name: the
  services' pools are labeled with the service name (db.Options.AppName, the DSN takes precedence),
  the labels in the message and the metric's app label — from there too. The snapshot
  is instantaneous: episodes shorter than the interval are caught partially (3–10 s at
  10 s), longer ones — always; the honesty reflected in docs. The required deploy:
  `GRANT pg_read_all_stats TO crawler` (deploy.md §4.1) — without the grant
  the foreign sessions' text is hidden (a message with a note). Tests:
  slowqueries_test.go (the format, numeral agreement, the debounce,
  a source failure, disabling with an interval of 0), pool_test.go
  (application_name and the DSN priority), config (the defaults/env/the
  5s..10m and 1s..1h corridors), postgresql_conf_test.go (the threshold contract).
  Along the way: the env mapping CRAWLER_MONITOR_LOAD_REPORT_INTERVAL
  missed at T-173 (docs declared it, load.go did not have it). Criterion: every statement longer than 3 s — a line in the server's
  journal; a steadily running one — one Telegram message with
  the service label and pid; monitor's journal does not flood. Spec: §5.9, §6.3.

## Stage 26. Incremental pipeline counters (2026-10-24)

The source: an analysis of the service/landing/dashboard's aggregate queries
(2026-10-24) — implementing the "systemic alternative" from the T-161 statement
("incremental zone counters on status transitions, a separate wave").
The stage's spec: [`TZ_incremental_counters.md`](./TZ_incremental_counters.md).

- **T-178** [done] — P1, incremental counters of the pipeline states
  (the full passes of 47 million `domains` rows removed from the read paths: the
  §12 measurement — `zoneStatsSQL`/`domainStatusSQL` of internal/status, `backlogSQL`
  of the scheduler 17+ s every 5 min, `historyStatsSQL` of monitor — most
  of postgres's CPU; after T-161 softened by the cache/intervals, the passes
  remained). The solution (the details and rationales — TZI): (1) zones get
  `domains_pending`/`domains_queued` (the base set of all zone columns unified — `status <> deleted`, TZI §4.1), the global
  scalars (the status distribution, the appeared/lost cumulatives, render_pending)
  — the `pipeline_counters` table (migration 000015, filled by the first
  reconciliation pass per the 000012 precedent); (2) increments/decrements
  in the same transactions as the transitions (dispatch/lease/retry/recheck/
  ingest — `UPDATE … FROM (VALUES…)` batches over `RETURNING zone_id`;
  record.go — a point `SELECT … FOR UPDATE` of the old values; renderq —
  a bit guard + RowsAffected, closing the T-175 counter source;
  analyzer — the history cumulatives and `domains_laravel`), Redis not
  used (TZI §13.1); (3) reconciliation: ZoneStatsOnce extended
  into one combined pass (zones + statuses + history + the render
  bit), the drift |old − new| — a `crawler_counters_drift` gauge +
  a Warn; (4) the readers switch to the counters: status.zoneStatsSQL without
  a JOIN domains, domainStatusSQL/laravelTotals/history — from the counters,
  BacklogOnce — `sum(zones.*)` (backlogSQL deleted), the landing and the
  status.Cache cache unchanged; (5) the CLI `crawlerctl counters list|recount
  [--zone]|verify [--zone]` — recount reuses monitor's Once functions
  (one SQL point of truth), verify — a check without writing, exit
  codes 0/1/2. Criterion: in steady state the only full domains pass is
  the reconciliation once per `monitor.zone_stats_interval`; `crawlerctl
  counters verify` green after the e2e; the dashboard numbers match
  crawlerctl status; postgres's CPU below the §12 measurement by the amount of the removed
  passes; an artificial drift is caught by verify (exit code 1) and fixed by
  recount. Spec: TZI in full; the main spec §5.9, §6.3, §13.1.

## Stage 27. Slow PostgreSQL queries — production log analysis (2026-10-24)

The source: the production slow-query log (T-177,
`log_min_duration_statement = 3000`, `/var/log/postgresql` on the server;
a local copy `tmp/pg_slow.log`, outside git): 669 firings over
09:39–12:20 UTC, 29 unique queries. The analysis one by one in sequence
(the order driven by the operator); the queries covered by TZI's
incremental counters (zoneStatsSQL, domainStatusSQL,
the reconciliation UPDATE zones, backlogSQL — TZI §1.1/§8) and the handled
[cut]

Continuation (2026-10-25): the window strictly after the last deploy
(12:37 MSK) — 09:38–13:15 UTC, 773 firings, 18 unique;
the deploy-handled forms left the log. Investigated and taken into
work: retrySelectSQL (T-191), the lock convoy of 10:22–12:44
(T-192), the reanalyze scan (T-193). A pass with a checkpoint: the new
throughputSQL of T-179 (a median of 6.1 s — the index-only scan of the hourly window
degrades with heap fetches on fresh pages; a measurement after T-191:
at >3 s — hourly bucket counters, a TZI extension of T-179/T-182);
the echo of the T-191 I/O storm — the enqueue UPDATE (a median of 4.2 s) and dispatchZone
(a median of 3.7 s against 5.2 s before T-183) — a measurement after T-191; the zone
reconciliation pass (a median of 38.9 s every 5 min) — covered by TZI T-178
(the only full pass by design), not investigated.

- **T-179** [done] — P1, the daily checked counter `done_today` —
  query #1 of the production log (96 firings >3 s over 09:39–12:20 UTC,
  a median of 15 s, max 51 s): the throughputSQL subquery `count(*) … WHERE
  last_checked_at = current_date`. The plan is indexed (migration 0014/T-171),
  but the index-only scan degrades with heap fetches of fresh pages — 676
  thousand per 815 thousand rows of the window, ≈1.1 GB of reads per execution — plus
  serial execution in the dashboard pool (`NoParallelGather`, T-171);
  the cost grows toward evening with the day window. The solution — an incremental
  daily counter (the analysis's variant "a"): a `checks:YYYY-MM-DD` row in
  `crawler_counters` (T-175, 708aa81 — "do not spawn new tables"; no new
  migration, the dependency — the T-175/000015 deploy), the increment in
  the `httpworker/record.go` transaction over RowsAffected, reconciliation —
  an hourly monitor pass (after T-178 — by a FILTER position of its combined
  pass) with a drift metric; the throughputSQL reader — a point read
  of the counter, the results_hour/rendered_hour hourly windows stay alive
  (0.07 ms, healthy). Revisiting the TZI §13.7 rejection based on the live
  measurement. Criterion: firings of the 3 s threshold over done_today in a day of
  an active pipeline — 0 (the T-177 journal); the EXPLAIN of the new throughputSQL —
  point plans without a domains scan; `crawlerctl status` shows the pace;
  make test + make test-integration green. Spec:
  [`TZ_slow_done_today.md`](./TZ_slow_done_today.md); the link — TZI §13.7.
- **T-181** [done] — P1, draining the render debt without PK order — query #2
  of the production log, the most frequent and heavy (137 firings >3 s over
  09:39–12:21 UTC, a median of 44 s, max 143 s): `renderPendingDomainsSQL`
  (renderq.go) — selecting a batch of 100 domains with the render_pending bit
  every 60 s. The EXPLAIN diagnosis: an index scan over the PK tramples the drained
  "empty" prefix — 14.1 million Rows Removed, ≈4.5 GB of buffers per call;
  without ORDER BY the plan is the same (no statistics for the `flags & 2` expression,
  the selectivity estimated as 41.6/41.6 million), but a forced seq scan —
  43 ms (700×): the flagged debt (xn--p1ai) is physically at the heap's start, an early
  LIMIT stop. The solution (the operator 24.10: the domain order does not matter) —
  the analysis's variant "a": (1) SQL without ORDER BY; (2) pinning the plan
  `SET LOCAL enable_indexscan/bitmapscan = off` in the selection transaction
  (one point, renderSelectPending); (3) a guard by a point read of
  `crawler_counters['render_pending']` (T-175): 0 → without the heavy SELECT
  (a steady state without debt — one row read per tick); a missing row —
  an error. Risks: the physical locality — a fact, not a contract — a control
  by the T-177 journal, the systemic alternative (a keyset cursor) postponed to the spec.
  Dependency: the T-175 deploy (migration 000015). Criterion: the
  `renderPendingDomainsSQL` records disappear from the slow log; the debt drain
  pace is bounded by the q:render gates (T-174), not the selection; at
  counter = 0 the tick does not touch domains; make test + test-integration
  green. Spec: [`TZ_slow_render_requeue.md`](./TZ_slow_render_requeue.md).
- **T-182** [done] — P1, the temp_error share on the daily counters —
  query #6 of the production log (33 firings >3 s over 09:43–12:19 UTC,
  a median of 28 s, max 111 s): `dnsShareSQL` (monitor/alerts.go) — the
  T-080 temp_error share alert, a 5 min cycle. The EXPLAIN diagnosis: the T-171 index
  works (an Index Scan over domains_last_checked_at_idx), but the status
  column requires a heap visit of every window row — 308 thousand
  buffers (≈2.4 GB), 170 thousand reads; serially in the monitor pool
  (`NoParallelGather`, T-171) ≈14–15 s; the "the pages are hot in
  shared_buffers" premise did not survive the 1–2 GB window. The solution — a continuation
  of T-179 (TZ_slow_done_today): the numerator `temp_error:YYYY-MM-DD` in
  `crawler_counters` incremented by the same record.go transaction
  (the denominator — the T-179 `checks:` row), reconciling both rows
  by one aggregate of the T-179 pass; `evalDNS` — two point reads,
  `dnsShareSQL` deleted; the share's semantics — the record events per day
  (the share is robust to duplicates, the absolutes are fixed by reconciliation). An index with
  status rejected by the HOT invariant (the flags/T-175 class). Criterion:
  the dnsShareSQL records disappear from the slow log; a test
  firing of the alert arrives as before (the T-162 thresholds); make test +
  test-integration green. Spec:
  [`TZ_slow_dns_share.md`](./TZ_slow_dns_share.md); the dependencies —
  T-175, T-179.
- **T-183** [done] — P1, the dispatcher's selection — only zones with work —
  query #9 of the production log, the pipeline's critical path (26 firings
  >3 s, the window 09:50–10:09, a median of 5.2 s, max 132 s): `dispatchZoneSQL`
  (dispatch.go, T-027) — LATERAL prefixes of pending queues by zones.
  The EXPLAIN diagnosis: the plans are correct (the T-104 partial pending index),
  the wasted volume — round 1 probes all 2204 active zones of the
  IANA catalog (T-125; ~2000 almost empty by pending — the average returned 3
  rows), round 2 "topping up" re-reads the hungry zones (at
  a round-1 quota of 1 a hungry one = any zone with pending) with a Limit quota —
  up to ~200 thousand row reads per batch. The solution: (1) `dispatchZonesSQL`
  a filter `AND domains_pending > 0` — the T-178/TZI §4.1 column with the same
  semantics as the dispatcher's predicate (the transition increments keep it
  alive, apply_add opens a new zone immediately); the dependency —
  the T-178 deploy; (2) a cap of round 2's quota by the batch remainder (remaining) —
  a zone's contribution to the batch does not exceed the remainder, reads beyond do not reach the merge;
  implemented independently of (1). Criterion: the
  dispatcher's selection records disappear from the slow log; the publication
  pace of q:dns does not fall (a T-092 measurement); make test + test-integration green.
  Spec: [`TZ_slow_dispatch_zones.md`](./TZ_slow_dispatch_zones.md);
  the dependency — T-178.

- **T-185** [done] — P1, the time-based retention drain — query #13
  of the production log, the heaviest by total time (50 firings >3 s
  over 09:51–12:15, a median of 76 s, max 262 s ≈ 63 min of DB time over 2.4 h):
  `retentionSelectSQL` (retention.go, T-166) — selecting cleanup batches
  of non-findings' content. The EXPLAIN diagnosis: the PK row-value cursor
  forces an Index Scan over the PK — the T-171 age index is not used;
  every hourly pass starts from zero and re-reads the heap of all
  the traversed history (cleaned/not-old filtered after the visit) +
  a JOIN domains per candidate; the track grows with the table (an inflow of ≈3 million
  rows/day). The solution — a time cursor: (1) the selection by
  (checked_at, PK) > the cursor, the same ORDER BY — the aging window
  is served by subdomain_results_checked_idx (T-171), an Incremental Sort
  for determinism, a composite index only per the actual plan;
  (2) the cycle's cursor survives passes (a catch-up round at start);
  (3) the queue-protection retries — a bounded key list with point
  offers (the full rounds were a retry and a track at once);
  (4) a catch-up round on the render_pending counter reset (T-175;
  a fallback — a day). The interlock/snapshots/purge semantics of T-166 unchanged. Criterion: the retentionSelectSQL records disappear from the slow
  log; the hourly pass — seconds (OnPass); the T-166 disk
  plateau holds; make test + test-integration green. Spec:
  [`TZ_slow_retention_drain.md`](./TZ_slow_retention_drain.md);
  the dependencies — T-171 (the index), T-175 (optionally).

- **T-186** [done] — P2, the T-181 review tail (a deepseek P1, round 1) —
  clearing the render_pending bit at domain deletion: the orphan bits
  of deleted flagged ones hold `crawler_counters['render_pending']` > 0
  forever — the T-181 guard does not sleep, renderSelectPending runs a full
  seq scan every tick (60 s) without a result. The current protection — a Warn
  "orphan bits/desync" + a cleanup SQL (runbooks §6, T-181);
  the systemic fix — clearing the bit and decrementing the counter in apply_delete
  (ingest) or excluding the deleted from the selection. Coordination:
  T-178 (the ingest counters, apply_delete) and T-184 (the
  renderSelectPending selection) rebuild the same places — do it after
  their merges. Criterion: deleting a domain with render_pending clears the
  bit and decreases the counter; at counter = 0 the tick does not touch
  domains (T-181); make test + test-integration green.

- **T-187** [done] — P3, the T-185 review tail (the reviewer's R3 condition) —
  a point drain at a non-fresh re-analysis: a finding removal
  by a re-analysis (non-fresh, `aggregateDomain` internal/analyzer/
  aggregate.go — the RowsAffected > 0 ∧ ¬fresh ∧ existed event)
  leaves the domain's old records in subdomain_results behind the retention
  cursor (a residual class, documented in docs/content.md;
  fresh losses are not lost — a fresh rewrite ahead-of-cursor,
  deleted domains are cleaned by T-030/T-020). The solution: a signal from
  `aggregateDomain` + a point drain of the domain by the PK prefix
  `(zone_id, domain_id)`; the minimum — a log/metric of the event. Coordination:
  T-178 rebuilds the analyzer cumulatives — do it after its
  merge. Criterion: re-analyzing a non-fresh finding leaves no
  records behind the cursor (a control scenario); make test +
  test-integration green.

- **T-191** [done] — P1, temp_error rechecks — query #1
  of the post-deploy window of 25.10 (150 firings >3 s over 09:38–13:15 UTC,
  a median of 74.8 s, max 427 s, a sum of ≈3.6 h of DB time — the heaviest
  unhandled one): `retrySelectSQL` (`internal/scheduler/retry.go`,
  T-031) — selecting a batch of retry candidates. The EXPLAIN diagnosis in production:
  the planner riding the partial `domains_recheck_idx` (T-104,
  the predicate check_state = 2 provable) scans the `last_checked_at` window
  older than a day — 3.36 million rows (94 % of the checked; the status requires
  a heap visit of each — the T-182 class on a window 4× the daily one), the estimate
  of ~2.5 thousand rows — the histogram does not see the check wave (92 % NULL),
  a parameter cast does not fix it; the multiplier: the DNS incident cohort of 24.10
  (491 thousand temp_error, 157 thousand mature) keeps the drain at full
  batches — a scan every ~90 s, plus ~940 dirty pages (hint bits)
  per pass. The solution per the T-181 precedent: SQL without ORDER BY +
  a transaction `SET LOCAL enable_indexscan/bitmapscan/indexonlyscan =
  off` (a seq scan with an early stop; a production measurement of 229 ms against
  44.7 s, buffers 1819 against 2.11 million) + a guard by a point read of
  `crawler_counters['status_temp_error']` (T-178): 0 → without the heavy
  SELECT; a missing row — an error. The residual case (temp_error
  > 0, none mature — a day-granularity one matures at midnight) — a full
  seq scan of ~15–20 s once per hour, accepted consciously. Criterion: the
  retrySelectSQL records disappear from the slow log; the cohort's storm
  drains in minutes (the drain log); at status_temp_error = 0
  the hourly tick — one row read; make test + test-integration
  green. Spec:
  [`TZ_slow_temp_error_retry.md`](./TZ_slow_temp_error_retry.md);
  the dependency — T-178 in production (done by the 25.10 deploy).

- **T-192** [done] — P1, the `domains` row-lock convoy —
  records #2–#4/#6/#11 of the post-deploy window of 25.10 (point SELECT … FOR
  UPDATE / UPDATEs by PK with waits up to 39 min, a sum of ≈6.1 h of
  DB time over 3.6 h; the second group after T-191): the victims — the write path
  (the dns/http result writes, the publish bit, the lease return). The diagnosis:
  the plans are perfect (an Index Scan by PK, cost 2.8) — the time = waiting
  for someone else's lock; a reconstruction from the journal: a mass holder
  10:22–10:38 (9 victims across zones, a simultaneous release),
  a single convoy 10:40–11:17 on row (560, 25398110) — 5 sessions
  of three services, a release within one second 11:17:27.9, the holder
  ≈37 min; the operator excluded (no ssh logins); the holder's name not
  recoverable — `log_lock_waits = off` (0 records per day).
  The consequence: the pipeline stood ≈55 min (q:dns at the watermark 125,
  publications suspended; the T-162 "frozen queue" alert
  silent — the deliveries went slowly, they did not stand). The solution — the class, not
  the point: (1) operationally `log_lock_waits = on` (a reload, the holder's
  name in the journal on a recurrence); (2) `lock_timeout = 30 s`
  by the AfterConnect hook (next to NoParallelGather, T-171) at
  dns-worker/http-worker/scheduler — the convoy is cut, a rollback
  is finished by a delivery repeat (at-least-once §3.4), q:dead —
  a visible alert signal of T-162; (3) a point fix of the holder — by
  the recurrence's data. Rejected: statement_timeout (cuts slow
  successes), idle_in_transaction_session_timeout (the main path —
  the application), "do nothing" (55 min of silent standing already
  paid). Criterion: `SHOW log_lock_waits` = on in production; on
  recurrence the journal names the holder's pid/statement; waits in
  the journal outside the holder ≤ 30 s; make test + test-integration green.
  Spec: [`TZ_slow_lock_convoy.md`](./TZ_slow_lock_convoy.md);
  coordination with T-191 (the scheduler pool's common wiring).

- **T-193** [done] — P2, the re-analysis safety pass — a time
  cursor instead of a full scan — record #12 of the post-deploy window
  of 25.10 (3 firings per tick hours, 18.2–24.9 s, batches of 56/206
  rows): `reanalyzeSelectSQL` (analyzer/reanalyze.go, T-068/T-074) —
  the hourly tick of `ReanalyzeOnce` walks with a zero PK cursor, and
  `rules_version` is not indexable (the HOT invariant, the T-104/T-175 class) —
  for a handful of fresh tails the pass does a Parallel Seq Scan of all
  subdomain_results (an EXPLAIN in production: 19.9 s, 4.8 million rows, 423 thousand
  reads ≈3.3 GB, 156 found), the cost grows linearly with the table
  (an inflow of ≈3 million/day). The solution per the T-185 precedent (retention):
  a time cursor (checked_at, PK) leaning on subdomain_results_checked_
  idx (T-171) — the tick reads only the fresh cut (~0.3–1 s, independent
  of the table size), the start anchor now−24 h; the full drain at
  a rules change (Subscribe) and crawlerctl reanalyze stay on the
  PK form from zero (rare, deliberately full); an index over
  rules_version rejected (HOT), an EXISTS probe rejected (the proof
  of emptiness = the same scan). Criterion: the tick's records disappear from the
  slow log (the drain at a rules change stays); "the re-analysis pass"
  — the old numbers; make test + test-integration green. Spec:
  [`TZ_slow_reanalyze_tick.md`](./TZ_slow_reanalyze_tick.md);
  the dependency — T-171 (the index, in production).

## Stage 28. Dev environment: deterministic docker network addresses (2026-10-24)

The source: the third recurrence of the crawler_net static-IP race
(172.28.100.0/24): services without an `ipv4_address` get addresses from Docker's dynamic
allocator, which does not reserve the addresses of STOPPED static
services — a starting dynamic service takes someone else's address, and the owner
crashes at start with "Address already in use" (Exited 128) and does not come up
until a manual fix. Incidents: 2026-10-23 — twice (postgres→web,
dashboard→testweb; the operations journal operations.md §5, the fix: stop
the usurper → start the IP's owner → return the usurper); 2026-10-24
14:41 (+07) — analyzer→.3 (redis) and dashboard→.12 (testweb), the victims
stood 4.5 h and blocked `make test-integration` of the stage 25 wave
(fixed manually by the same procedure); 2026-10-24 ~21:05 (+07) —
  the fourth recurrence: render-worker took .11 from the stopped
  coredns (Exited 128), the `make test-integration` of the T-181 merge
  blocked; along the way it surfaced: unbound hangs at a long coredns downtime
  (263 healthcheck failures, fixed by a container restart).
  The static addresses now (the
deploy/docker/compose.yaml header): postgres .2, redis .3, unbound .10, coredns
.11, testweb .12, mockserver .13, chromium .14, render (the runtime profile)
.15, .250 — the "black hole" of closed-port scenarios; dynamic:
ingest, scheduler, dns-worker, http-worker, render-worker, analyzer,
monitor, web, dashboard, prometheus, adminer, redis-insight, tools.

- **T-180** [done] — P2, static `ipv4_address` for all long-lived
  crawler_net services in deploy/docker/compose.yaml: an address plan by
  classes (infrastructure/testbed — as is, the application and
  observation/utilities — compact ranges, e.g. .20–.29 and .30+)
  with the mandatory preservation of the existing addresses — they are pinned in
  the compose.yaml header and at the consumers: testbedgen bakes .12 into the DNS zones
  (cmd/testbedgen/main.go Network.TestwebIP), the render live tests go
  to ws://172.28.100.14:9222, the migration tests — the .2 DSN; a grep cross-check
  of `172.28.100` over the repo is mandatory, do not take .250. The exception —
  the tools run container: do NOT set an address (one-off runs
  of `docker compose run` go in parallel from worktrees with
  `COMPOSE_PROJECT_NAME=crawler` — a shared static address would conflict
  between them), pin the exception and the residual risk (a one-off run container
  may take a stopped static service's address; the window bounded by
  the run container's lifetime and self-heals with `--rm`)
  by a comment in compose and in docs. The address plan as a table
  "service → address → profile" — in docs/dev-environment.md (or docker.md —
  wherever the network map lives). Criterion: a full stack recreation
  (`make down && make up` — all healthy) and the incident scenario (stopping
  redis+testweb → restarting the dynamic services → starting the owners)
  pass without "Address already in use"; `make test` +
  `make test-integration` green; a parallel
  `COMPOSE_PROJECT_NAME=crawler make test` in a worktree does not conflict
  with the main stack.

## Stage 29. The consistency of observability numbers — the load report vs crawlerctl status (2026-10-25)

The source: a divergence of the load report's "📬 Queues" line (T-173) from
the `crawlerctl status` summary and the `crawler_queue_depth` metric, found
by the operator 25.10 in production (155.212.174.88, revision c791ef5): the line
prints the raw XLEN (the stream's whole record history until the XTRIM trim, T-141)
and in production overstated q:render ~5× (136.5 thousand against a backlog of 27.5
thousand — lag 27,409 + pending 120), and the q:analyze growth (+654 over the window at
lag 0) was phantom: the analyzer keeps up with the flow, the acknowledged untrimmed history accumulated (the 24.10 freeze stopped
`crawler-scheduler` together with TrimLoop — T-141 is in the deploy but was not
running for ~17 h). The right depth — the consumption groups' backlog
`min(lag, XLEN) + pending` (the backpressure gate formula T-026/T-168 —
`crawlerctl status`, `crawler_queue_depth` and the T-162 alert print it).
Along the way the 25.10 report exposed an underflow of the top-CPU ticks (postgres
3.1·10¹⁴ "cores" after a restart of the DB processes). The stage's specs:
[`task_one_load_report_queue_backlog.md`](./task_one_load_report_queue_backlog.md),
[`task_four_topcpu_ticks_underflow.md`](./task_four_topcpu_ticks_underflow.md).

- **T-188** [done] — P1, the load report's "📬 Queues" line — the
  groups' backlog instead of the raw XLEN (`internal/monitor/load.go`,
  `RedisQueues.Queues`): `min(lag, XLEN) + pending` over all groups —
  the same formula as `BackpressureGate.Len` and `RedisStreamGroups`
  (the T-162 alert); extract the common calculation, formula duplication is not
  allowed; a stream without groups and q:dead — XLEN as now; the delivery
  paces (entries-read) kept, the ±N delta becomes
  a backlog growth/drain. After the fix the report == `crawlerctl status`
  == `crawler_queue_depth`. Criterion: on the dev stack the report's line
  matches `make status`; independent of the trim's operation;
  `make test`/`make lint`/`make test-integration` green. Spec:
  [`task_one_load_report_queue_backlog.md`](./task_one_load_report_queue_backlog.md).

- **T-189** [done] — P2, the load report's top-CPU: unsigned
  tick subtraction (`ProcAgg.Ticks uint64`, load.go, the window delta in
  `LoadReportLoop`) underflows at a restart of same-named processes between
  passes — 25.10 "postgres 310613997334638.2 cores" (18 → 16 processes,
  the tick sum by name fell); the fix — a comparison before subtracting,
  non-monotonic names skipped for the window. Criterion: a unit test
  of a restart (a tick-sum drop) gives no garbage top entries;
  `make test`/`make lint` green. Spec:
  [`task_four_topcpu_ticks_underflow.md`](./task_four_topcpu_ticks_underflow.md).

- **T-190** [done] — P3, the slowquery tests' races under `-race` (a
  T-189 finding): a non-standard run of `go test -race ./internal/monitor`
  exposes 8 WARNINGs in `TestSlowQueryLoop_*` (T-177) — races in the
  tests themselves (the fake/the cycle), existing since T-177; the regular `make test` does not
  catch them (no -race). Bring the slowquery tests to cleanliness under `-race`,
  pin the rule (a local -race for monitor or a CI step).
  Criterion: `go test -race ./internal/monitor` — 0 WARNINGs; `make
  test`/`make lint` green.

## Stage Table of Contents| Stage | Name | Spec §12 mapping | Tasks |
|---|---|---|---|
| 30 | Top-level-only zones (TLD-only) (2026-10-26) | replaces spec 5.2 item 5 | T-194…T-198 |
| 31 | Slow PostgreSQL queries — the window after the 3bffc62 deploy (2026-10-26) | — | T-199…T-205 |

Stages 0–29 finished: 194 done tasks moved to the archive
[`tasks_done.md`](./tasks_done.md) together with the table-of-contents
lines (the last transfer, 2026-10-26: the full statement of T-149 — stage 23,
the waves of stages 24–29 — T-157…T-193 — and the closing notes of the waves of stages
10 and 20–22). Only stage 30 remains here (T-194…T-198, not started).

## Stage 30. Top-level-only zones (TLD-only) (2026-10-26)

The source: 2LD zones from the Mozilla PSL list (`5g.in`, `co.in`, … — the registries
add them themselves, the PSL is embedded in golang.org/x/net) shatter the
`zones` catalog, the statistics and the partitions, and a full `.in` dump cannot be ingested
via `incoming/in/` — every line under a 2LD suffix is rejected
`wrong_zone` (`domainLabel`, internal/ingest/validate.go). The solution
(the 26.10 discussion): the zone — the name's last label (the TLD), the label — everything
else (a compound label — the norm); the `incoming/<tld>/` directories;
multi-level directories — only the `ingest.allowed_zone_paths` allowlist
(the testbed's test zones); the data of the existing 2LD zones is collapsed
by the `crawlerctl zones collapse` command in a maintenance window. It cancels
the semantics of spec 5.2 item 5 (the public suffix per PSL) — governed by the supplement
[`TZ_tld_only_zones.md`](./TZ_tld_only_zones.md).

- **T-194** [done] — P1, `internal/idn`: the zone — the name's last label
  (TLD-only): `Normalize` cuts an ASCII name by the last dot (the zone =
  the TLD, the label = the prefix), remove the call and import of
  `golang.org/x/net/publicsuffix`; keep `ErrNumericZone` (a last label of digits — dump dates, IPv4 literals) and `ErrNoLabel` (a bare
  TLD); the package/`Result` docstrings — without PSL references.
  Deliberate contract changes: bare suffixes (`github.io`, `co.in`)
  become valid domains (garbage without a filter — spec §12.6),
  `example.co.uk` → the zone `uk`, the label `example.co`. Criterion: the
  T-010 corpus and the unit tests keep the spec's §4.2 table in full; `make test`/
  `make lint` green. Spec:
  [`TZ_tld_only_zones.md`](./TZ_tld_only_zones.md).

- **T-195** [done] — P1, `internal/ingest` + `internal/config`:
  TLD-only zone directories + an allowlist: `incoming/<tld>/add|delete/` —
  the working path (a full `.in` dump as one file); a multi-level directory
  — a file-level error (`failed/`, T-021), except the new
  `ingest.allowed_zone_paths` (an empty default; a `NormalizeZone`
  validation + at least one dot, a dedup at load); `domainLabel` structurally
  unchanged; the validation texts of `zone_weights`/
  `requeue_deprioritized_zones` — TLD names (remove "compound —
  co.uk"). Criterion: `example.5g.in` in `incoming/in/` is accepted
  (the label `example.5g`, formerly `wrong_zone`); `crawl.test` from the allowlist
  works, without it — `failed/`; the ingest/config tests green. Spec:
  [`TZ_tld_only_zones.md`](./TZ_tld_only_zones.md).

- **T-196** [done] — P1, `crawlerctl zones collapse` — carrying the data
  of 2LD→TLD: dry-run by default, `--apply`/`--skip-reconcile`/
  `--force`; per each zone (§7 of the spec): ensure the TLD zone and partitions
  (`create_zone_partitions`), `UPDATE` batches with row movement (domains with
  re-gluing `label || '.' || prefix`, subdomain_results,
  laravel_domains, laravel_findings, laravel_history), re-binding `ingest_files`
  to the TLD with dedup resolution of `(zone_id, action,
  sha256)`, dropping the 2LD partitions, `DELETE zones`, a counter reconciliation;
  idempotent and resumable; allowlist zones are not collapsed;
  a precondition — no `check_state = 1`. Criterion: an integration test on
  a seeded schema (carrying all six tables, a dedup conflict, a no-op
  by a repeat, continuing after a mid-way failure, the §7.5 post-checks);
  `make test-integration` green. Spec:
  [`TZ_tld_only_zones.md`](./TZ_tld_only_zones.md).

- **T-197** [done] — P2, the documentation and testbed of the TLD-only wave:
  docs/schema.md (§"Name processing" — a TLD zone), docs/incoming-files.md
  (the `incoming/<tld>/` directories, the allowlist), docs/configuration.md and
  config.example.yaml (`allowed_zone_paths`, weight comments — TLD),
  docs/runbooks.md (the zone collapse window §8 of the spec), the testbed config —
  `allowed_zone_paths: [crawl.test]`. Criterion: a documentation review;
  the testbed scenarios (ingest→check in the `crawl.test` zone) pass. Spec:
  [`TZ_tld_only_zones.md`](./TZ_tld_only_zones.md).

- **T-198** [done] — P1, the maintenance window: collapsing the productive
  DB's 2LD zones per the spec's runbook §8 (29.10): the deploy of the T-194…T-197 wave
  (the healthcheck green) → stopping scheduler/ingest → draining the streams (XLEN
  was history; PEL residues 219 — DEL of the streams, 1008 domains returned
  to pending) → stopping the workers → a dry-run inventory (1290 2LD zones) →
  `zones collapse --apply`. The backup skipped deliberately (no space, the
  operator's decision). The carry: a batching defect fixed on the fly (269f6a6 — id-space
  windows gave 726 empty batches for 26 domains, an estimate of 40+ h;
  batches of actual ids — carrying 1290 zones ≈1 h 20 m, 1.9 million domains,
  3.3 million results, ~2.9 million rows of children+history, 3665 ingest_files,
  ~250 TLD extractions from DEFAULT), the reconciliation rewrote 935 zones.
  The §7.5 post-checks — manually (the process hung in a deadlock after
  the reconciliation — a follow-up below): 0 2LD zones, 0 orphans in all six
  tables, a spot check of 26=26 domains `*.5g` in the `in` zone; the TLD
  counters converged by the reconciliation. VACUUM (ANALYZE) — 0 dead tuples. The stack brought up
  by the 269f6a6 deploy, the scheduler stopped (the operator's decision: draining
  the render debt at wm=10000 without new dispatch). Window follow-ups: (1)
  P2 — a PostChecks collapse deadlock on the production pool (a futex, sessions in
  ClientRead after the reconciliation) — reproduce and fix; (2)
  max_locks_per_transaction 64 → 512 (an ALTER SYSTEM, a PG restart in the window):
  retention/count/domain_info failed with `out of shared memory` on ~2700
  relations of the partitioned schema (680 LIST partitions × hash leaves
  × indexes > 64×50 slots); (3) the carry pace: dominated by first-time
  TLD extraction from DEFAULT (~20–60 s × 4 tables) — acceptable for
  a one-off operation. Criterion: the post-checks green, the `zones`
  catalog without multi-level zones (935 TLDs), a redelivery of the `.in` dump to
  `incoming/in/` accepted as one file (the T-194/T-195 contract). Spec:
  [`TZ_tld_only_zones.md`](./TZ_tld_only_zones.md).

## Stage 31. Slow PostgreSQL queries — the window after the 3bffc62 deploy (2026-10-26)

The source: the production server's slow-query log (T-177,
`log_min_duration_statement = 3000`), the window strictly after the last
`3bffc62` deploy — 2026-10-25 18:16 → 10-26 03:05 UTC (8.8 h; the
T-191/T-192/T-193 wave deployed exactly in it): 1053 statements, 25 kinds,
≈4.9 h of total DB time; a local copy
`tmp/pg_slow_after_3bffc62.log` (outside git). Context: CPU starvation
(PSI some avg300 ≈ 80 %), the midnight storm of 00:00–02:30 UTC (the daily
recheck + the maturing of the 24.10 temp_error cohort — the 21:20 UTC drain
moved 104,297 domains). The T-191…T-193 wave's outcomes confirmed:
retrySelect 150 → 10 events, the reanalyze tick 18–25 → 5–12 s,
renderRequeue 137 → 1, the lock convoy did not recur (459 waits,
all ≤1.1 s — T-192 works). The stage closes the leftovers: the
checkpoints of stage 27 (throughputSQL, enqueue/dispatch — "a measurement after
T-191") fired, the new items — the lease return and applying the NRD feed.

- **T-199** [done] — P2, zone reconciliation less often —
  `monitor.zone_stats_interval` 5 → 30 min: the operator's decision per TZI
  §7.1 (to space out after the wave's run-in by the drift measurement); the
  window's measurement — the reconciliation is the only full `domains` pass, 48 %
  of the slow DB time (100 events, med 39 s, max 501 s at a pass
  every 5 min). The code default (`DefaultZoneStatsInterval`) + a comment,
  `config.example.yaml`, docs (configuration.md, deploy.md §4.5 —
  the deployment instruction, mentions in operations.md/disks.md/
  runbooks.md), a default test. Production — operationally 26.10 (the operations
  journal operations.md §5): config.yaml + a monitor restart. Pin the
  consequences in docs: the alert cycle is now 30 min (the spec 5.9 corridor),
  the progress-stop threshold `AlertStallFactor × interval` grows
  25 min → 2.5 h. Criterion: the reconciliation records in the slow log
  ≤ 2/h; the counter drift in steady state 0 (the TZI §11 metric).
- **T-200** [done] — P1, returning expired leases — query #3 of the
  window (75 events, med 8.1 s, max 111 s, 7 % of the time). Diagnosis: the
  UPDATE side's plan scans the whole expired range of the lease index without
  a LIMIT (a Nested Loop, a double read of the range per batch; AS MATERIALIZED
  does not change the plan) + a dead prefix of `domains_lease_idx` between vacuums
  (273 MB of index at ~1 thousand live records; a pass with zero rows —
  10 thousand reads; autovacuum 0.02 × 42.3 million ≈ 846 thousand — the prefix
  accumulates up to ~a million records). The solution: a SELECT of the batch + an UPDATE … FROM
  unnest by PK probes with guard re-checks in one transaction + a drain cursor
  (queued_at, zone_id, id) in LeaseWatch's state (the
  T-185/T-193 style). Spec: [`TZ_slow_lease_return.md`](./TZ_slow_lease_return.md).
- **T-201** [done] — P2, retention — the residual slowness of the T-185 time form
  (SELECT 38 events med 13.8 s / the UPDATE cleanup 37 med 12.1 s).
  The diagnosis (EXPLAIN ANALYZE): an aging window of 84 thousand rows/h (the T-185
  spec's estimate was 15 thousand), ~70 thousand heap visits + 22 thousand PK probes into domains
  per batch — the necessary minimum of the form under the HOT invariant; the main
  factor — I/O starvation; the pass gathers 2 parallel workers on
  the same service's starved CPU. The solution: `db.NoParallelGather` for the
  http-worker pool (extending T-171 to the fourth service) + a control
  measurement after the render's hardware scaling; the SQL/indexes unchanged.
  Spec: [`TZ_slow_retention_residual.md`](./TZ_slow_retention_residual.md).
- **T-202** [done] — P1, the temp_error retry — the case "a big counter,
  few mature ones" (10 events med 51 s, max 239 s after the T-191 fix):
  the guard over `status_temp_error` (543 thousand) does not distinguish mature candidates
  (`check_state = 2 AND last_checked_at < now − 24 h`) from immature ones —
  at a low density of mature ones the seq scan without an early stop reads out
  all 42.3 million rows by the hourly tick. The solution (the TZI T-178 family /
  T-179/T-182): a counter of the mature ones — hourly buckets ("matures in 24 h":
  an increment of the now+24h bucket when writing a temp_error result,
  a decrement at a retry), the guard lets the heavy SELECT through only with
  non-zero mature buckets; reconciliation — by a FILTER position of the
  reconciliation pass. Criterion: the retrySelectSQL records in the log — only in
  the hours of live mature draining (full batches, an early stop).
- **T-203** [done] — P1, throughputSQL — the stage 27 checkpoint
  fired: after T-191 a median of 6.0 s > 3 s (108 events; the hourly
  subqueries `results_hour`/`rendered_hour` — index-only over fresh
  pages degrades with heap fetches). The solution prescribed there:
  hourly `crawler_counters` buckets — the TZI extension of T-179/T-182
  (an increment in the result-write transaction of `httpworker/record.go`,
  the reader — point rows, reconciliation by a FILTER position of the
  reconciliation pass). Criterion: throughputSQL leaves the slow log;
  the bucket values converge with the live count (reconciliation).
- **T-204** [done] — P2, the reanalyze tick — the T-193 time form is more expensive
  than the spec's estimate (5–12 s steadily, 86–94 s in the midnight storm against
  0.3–1 s): the bitmap+heap pass of the hour (84 thousand rows) + a JOIN
  of domains/zones per every window row — the T-193 acceptance criterion
  ("the tick's records disappear from the log") not reached. Candidates: a guard
  over an unanalyzed-tails counter (an increment when writing
  a result without analysis / a decrement at an analysis publication) or
  a JOIN lightening (the zone name multiplied only to a non-empty batch);
  choose by an EXPLAIN measurement. Criterion: the tick < 3 s in steady state.
- **T-205** [done] — P1, applying the NRD feed — one statement
  INSERT…SELECT of ~470 thousand rows (63 s on 24.10, 200 s on 25.10; the previous
  analysis windows of 20:33 UTC did not cover it — a new finding). Both variants:
  (a) chunking `apply_add` — batches of `ingest.batch_size` (50 thousand)
  by a cursor over the staging PK `(batch_id, line_no)` in one transaction,
  the `zones` deltas — one final statement over the accumulated
  batch counters (the T-018 atomicity and idempotency kept);
  (b) a cron 20:33 → 05:33 UTC — outside the evening CPU starvation and the midnight
  recheck storm (the source day closes >9 h earlier — the
  deploy/nrd/README contract kept); the production part operationally 26.10 (the
  journal), the repo — `deploy/nrd/crawler-nrd.cron` + README + deploy.md
  §4.9. Criterion: applying a day — batches <3 s each in the slow
  log, the final zones statement — point; the next NRD day
  applies without >3 s records.
- **T-206** [done] — P2, decoupling the alert cycle from the reconciliation period —
  its own interval at the former frequency (the operator's 26.10 decision).
  Today monitor's alert cycle follows `monitor.zone_stats_interval`
  (T-161, "one pair of cycles"), which T-199 spaced out to 30 min — the
  detection windows set in passes stretched: `queue_dead` 15 → 90 min,
  `memory_high`/`render_debt` 10 → 60 min, the progress-stop threshold
  `AlertStallFactor × interval` 25 min → 2.5 h; the PSI sample (a
  30 min window) became equal to the pass period. The solution: a separate field
  `monitor.alerts_interval` (a default of 5 min — the former frequency, a corridor
  per the zone_stats_interval sample), the alert cycle ticks by it,
  the reconciliation stays 30 min; update the comments/docs (deploy.md
  §4.5 — the T-199 consequence is removed, services.md — the alert windows
  back to the former minutes, configuration.md — the new field).
  Criterion: alert evaluations every 5 min at a 30-minute
  reconciliation; the detection windows returned to the former values.
  Not part of the current stage 31 wave (T-199/T-205 close it):
  picked up by the next task cycle per the common AGENT.md rules.

## Stage 32. Render debt drain after the T-198 window (2026-10-29)

Context: the T-198 window left the stack without a scheduler (the operator's decision)
with an open `render.queue_high_watermark: 10000` — q:render drains the
`render_pending` debt (~966 thousand domains on the evening of 29.10). The speedup
measured: 2 workers × 6 contexts — the optimum (144/min maximum);
8 contexts and a second chromium (a unit on :9223, rolled back) worse — the
bottleneck 4 vCPU. The chromium unit copy requires a `ReadWritePaths` edit
(found when wiring it up). A crawler-chromium recycle timer installed
every 2 h (the remote mode does not restart the browser itself — a memory
degradation of 400→950+ MiB and ×2 timeouts; a measurement: a recycle halved the timeouts).

- **T-207** [done] — P2 (narrowed by the operator: research only,
  no fixes), the efficiency of the render debt drain. Verdicts: "losing
  half" — a units error: 1 domain ≈ 1.85–1.92 deliveries
  (published/domains = 1026/535…1265/667 per the pass logs),
  128 deliveries/min ≈ 67–69 domains/min; the average 44/min understated
  by the day's slow phases (the wm=1 freeze until 14:07, restarts, contexts
  tuning), the evening's steady state — ~60/min (−49…−71/min). The PEL clean
  (102–166 records, idle < 10 min, zero hangs) — H1 claims
  ~5–10 %, not half. H2/H3 refuted: the flag lifecycle correct
  (the result write sets it at a full q:render, the pass clears it at the
  publication; 8 domains from the WARN logs — the flag cleared, the pass's clearings
  match the counter exactly). H4 refuted: the pass is not the bottleneck
  (10 batches/10 min, stopping on full=true watermark). There is no eternal core in
  the debt; there is an eternal tail of unrendered rows ~10–12 % of episodes
  (timeouts/cert/dead hosts ≈ 180–220 thousand records) — re-published
  at every future episode. The real overheads: finalizations without
  render ~10 % of deliveries (a 15 s tar pit per slot), task-class retries
  5–10 %. The ETA of the remaining ~954 thousand: ~12 days. Continuations — T-210…T-212.

- **T-210** [done] — P2, a "render impossible" marker in the
  `subdomain_results` rows (per the T-207 outcomes): finalized-without-render
  rows stay `rendered = false` forever and are indistinguishable from "not
  tried" — the renderq pass re-publishes them at every new episode
  of the domain (~180–220 thousand records, ~2–3 deliveries + slot time per broken
  record per episode; the cert class and NAME_NOT_RESOLVED — permanent
  host properties, retries pointless: the T-172 measurement — 0 successful).
  The solution: a stamp/marker column (or a rules_version pattern) on the row,
  set by the tar pit/cert/task-class finalizations; the pass's selection
  (`renderPendingRowsSQL`: rendered = false AND final_url IS
  NOT NULL) excludes the marked records until a content change.
  Criterion: a repeated episode of a broken host does not re-publish
  the marked records; their share in the pass's selection → 0;
  test/lint/integration green.

- **T-211** [done] — P3, NAME_NOT_RESOLVED → finalization from the first
  delivery (per the T-207 outcomes): extend the list of "finalization
  without retries" classes (the sample — ERR_CERT_DATE_INVALID, T-159/T-172) with the
  DNS-refusal error at render: a dead host now eats 3 deliveries and ~20 min
  in the PEL (the 29.10 measurement: 252 NAME_NOT_RESOLVED + 186
  CONNECTION_REFUSED). The risk small (a DNS refusal at render with a live
  HTTP episode — a rarity). Criterion: the class finalized by the first
  delivery, the window's metrics show −2 deliveries on a dead host;
  test/lint green.

- **T-212** [done] — P3, operational: re-evaluate `render.contexts
  6 → 8` with the chromium recycle timer running. An A/B of 30.10 (the base —
  a fresh T-210/T-211 build, the domains/min metric over the renderq passes):
  6 contexts = 85/min; 8 = 73/min (a clean 30-min window) at a load of 16
  and q:dead 1→5 — the conclusion as on 29.10: the CPU limit 4 vCPU, the added
  tabs only intensify the competition; the recycle timer removes
  the memory degradation but adds no CPU. The decision: **keep 6**,
  the config commented with the measurement (6=85, 8=73), the final render pace
  144 deliveries/min, the timeouts 49. q:dead 5 — an observation (not a trend).
  The next meaningful speedup — only scaling CPU/memory
  (a second chromium per T-209) or lowering render.timeout (the
  T-207 measurement: 13% of successes >15 s — they would go to the tar pit, the operator's call).

  The operational decision of 31.10: `render.timeout` 15 → 10 s in production
  (the debt's tar-pit cohort — timeouts 340/10 min with the CPU idle,
  the tar pits' slot time −a third; the cost ~13 % of slow successful renders,
  the T-207 measurement). Applied in /etc/crawler/config.yaml with a comment,
  introduced by the deploy from this commit.

  An A/B #2 of contexts after lowering the timeout (31.10, the T-212 scheme):
  6 = 41 domains/min, 8 = **106 domains/min** (×2.6; the timeouts down 662→384,
  chromium 226 %/400, MemAvailable 3.0 GiB, load ~9) — **8 kept**:
  T-212's "6" verdict was the 15 s mode, where tar pits held the slots and
  saturated chromium; at 10 s the slots free quickly and 16 tabs
  parallelize with CPU to spare. The config commented with the measurement.

- **T-213** [done] — P3 (an A/B in production 31.10: PSI CPU 66–71 → 47–59 %,
  0 detection divergences — the laravel+ findings and laravel_domains identical
  before/after ~50 min of re-renders, the pace within the noise band) — **enabled true in
  production** with the verdict in the config; the `render.block_stylesheet` option — blocking
  CSS in the render context (per the 31.10 research: block.go of T-062 cuts
  Image/Media/Font = 91 % of a page's weight, Stylesheet — the remaining
  5–10 % of bytes + style/layout CPU; the research sources give
  another −a third to loading with CSS+images blocked, but the risk: rare sites
  whose JS reads computed styles/media queries). The solution: a config flag
  (a false default), the Fetch.enable pattern extended with the ResourceType
  Stylesheet when enabled; an A/B in production with a control of the detection-change share
  (a diff of the Laravel signatures before/after on the same sample) and
  the pace. Criterion: the option in code+docs, unit/live tests green; when
  enabled in production — the pace no worse, the detection divergence share < 1 %.

- **T-214** [done] — P3, blocking tracker domains in render: block.go
  cuts by the resource TYPE, and analytics arrives as the Script type (by type
  is impossible — SPA scripts are needed); Fetch.enable accepts urlPattern —
  add an updatable list of known-tracker patterns
  (google-analytics, googletagmanager, doubleclick, facebook,
  hotjar, yandex-metrica etc., ~15 patterns) by the config option
  `render.block_tracker_domains` (a true default — detection does not need
  trackers). The effect: −traffic and −CPU of the trackers' timers on every page
  (the sources' measurements: analytics — 5–15 requests/100–500 KB per
  page). Criterion: the patterns in the config, pattern-matching tests
  (our own domain untouched), a live test; the render metrics in production do not
  degrade.

## Stage 33. Tails from the render debt drain (2026-11-01)

Follow-up tasks from the reviews and findings of waves 30–32, not included in their
acceptance criteria.

- **T-215** [done] — P3, `PartOf=crawler.target` for the
  `crawler-chromium-recycle.{service,timer}` pair (a candidate from the T-208 review):
  now after `systemctl stop crawler.target` the next timer tick
  starts the stopped chromium (documented by a caveat in
  docs/deploy.md §2.2 and operations.md). The fix: the directive in the oneshot service
  (PartOf extinguishes the unit with the set) — first on the server
  (the source of truth), then synchronously in `deploy/systemd/` + editing
  the caveats in docs. Criterion: on the server and in the repo the directives match;
  `systemctl stop crawler.target` stops it and a timer restart does not
  raise chromium until the set starts.

- **T-216** [done] — P3, failing render live tests on master:
  `WaitLive`/`ExtractLiveDataURL`/`TestBrowserLivePoolRender`
  (internal/render, the CRAWLER_RENDER_TEST=1 contour) break on `data:`
  URLs — `validateTaskURL` began rejecting them after the tests were written
  (a pre-existing breakage, found at T-213; CI is not painted red — the contour
  is opt-in). Fix it (allow data: in test tasks deliberately or
  move the tests to http fixtures per the block_live_test.go sample).
  Criterion: `CRAWLER_RENDER_TEST=1 go test ./internal/render` — green
  in full; make test/lint untouched.

- **T-217** [done] — P2, the deadlock of the `crawlerctl zones
  collapse` post-checks (a P2 finding of the T-198 window, 29.10): after a successful
  counter reconciliation the process hung (futex_wait, ~10 threads),
  the DB sessions idle in ClientRead — CheckZonesLeftSQL executed by the server,
  the result not consumed by the client; the transfer connection (Acquire)
  held `DELETE FROM zones`; the window closed manually (the process killed,
  the post-checks done by hand — all green). Reproduce
  (the likely vector: unconsumed rows of a single-row aggregate at
  competition for the pool connections of pool_size 5; an integration test with
  a small pool), find and fix. Criterion: a test detecting
  the hang (a timeout instead of an eternal wait), a fix, a green
  `make test-integration`.

- **T-218** [done] — P1, a CDP connection leak of render-worker after
  chromium restarts (the 02.11 incident): after a browser recycle
  (the T-208 timer, a tick at ~01:35) the workers began accumulating websockets to :9222
  (284 by the moment of detection), `Target.createBrowserContext` goes
  into a 15-s timeout ("the render context died, a task retry on a new
  context" ×11,520 over 6 h); the system degraded ~5.5 h (the pace
  fell), by 07:25 it stood up entirely (0 domains/min, the read guard
  streak=1241, publication passes stopped on full); a fresh chromium
  after the next recycle healthy — the workers' state breaks. The incident's
  treatment — a worker restart (the connections 284→8, the pace
  110/min). Fix: closing old websockets/recreating the
  browser session at a CDP break (the reconnect path of render.Manager),
  auto-recovery without a worker restart; reproduce
  kill-the-browser in a test. Criterion: a test "a browser restart → the worker
  recovers ≤ a minute, the connections do not grow"; in production the recycles
  pass without a pace degradation (a control by the journals).

- **T-208** [done] — P3, pinning the Chromium recycle timer in the repository: the units
  `crawler-chromium-recycle.{service,timer}` (a restart of `crawler-chromium` every 2 h)
  now live only on the production server
  (`/etc/systemd/`, installed 29.10 in the T-198 window) — at a host reinstall
  they will be lost. Move into `deploy/systemd/`, extend
  `docs/deploy.md` §2.2 (the remote mode does not let the worker restart
  a foreign browser — T-065; without a recycle chromium accumulates memory 400→950+
  MiB over ~2 h and the render timeouts double, the 29.10 measurement: a recycle
  halved the timeouts 227→110). The content — from the server (155.212.174.88,
  `/etc/systemd/system/crawler-chromium-recycle.*`); the production installation
  already done — after the merge cross-check the files. Criterion: the units in
  `deploy/systemd/`, docs updated, `make test`/`make lint` green.

- **T-209** [done] — P3, the unit template of a second Chromium with a
  `ReadWritePaths` fix: the 29.10 experiment (a second chromium :9223 for
  `render-worker@9109`, rolled back — the bottleneck 4 vCPU) left a working
  `crawler-chromium2.service` unit only on the server. Commit it into
  `deploy/systemd/` as a template for the case of returning to two browsers on
  scaled-up hardware, pinning the found defect of the unit copy:
  `ReadWritePaths` must point at this instance's profile directory
  (`chromium2`), otherwise `ProtectSystem=strict` blocks the write and
  headless-shell falls into the start limit; mention in `docs/deploy.md`
  §2.2 next to the per-instance env recipe. In production the unit disabled —
  after the merge the server's state does not change. Criterion: the unit in
  `deploy/systemd/`, the defect and the fix described in the unit's comment and
  docs/deploy.md §2.2, `make lint` green.

## Stage 34. Full code review findings (2026-11-02)

A full audit of the codebase per "Bulk code review" (AGENT.md):
the baseline `make test` + `make lint` green; the reviewers —
`opencode-go/deepseek-v4.1-flash` (13 findings) and
`opencode-go/minimax-m3.1-flash` (10 findings), both reports complete;
`kimi-code/k3.1-pro` failed twice on 429 (a 5-hour quota), from
a partial transcript 1 finding verified (T-228, latent) and
the IPv4-in-6 filter bypass independently confirmed (went into T-219).
03.11 — a consolidated review by `grok-4.7` (`tmp/code-review.md`): duplicates
mapped to T-222…T-229 (RecoverLoop already closed by T-222), new after
verification — T-230, T-231 and the T-227 extension. After the dedup and
consolidation of three sources — 18 unique findings, each verified against
the code (callers/consumers), none rejected. The reviewers' reports:
`/tmp/opencode/review/34-full-audit-{deepseek,minimax}.md` (temporary,
the essence duplicated in the tasks).

- **T-219** [done] — P0, the SSRF perimeter: the internal-address filter
  covers only the redirect path. The DNS stage's addresses go into
  `dialAddrs` unchecked (`internal/httpfetch/transport.go:562-575`,
  the comment "they do not get into the funnel"), `BlockInternalAddrs` lives
  only in `lookup()` (:598-609); docs/configuration.md:501 admits
  this, requiring the "mandatory" flag for Internet operation. Render
  with no address check at all: `validateTaskURL`
  (`internal/render/render.go:251-259`) looks only at scheme+host,
  renderworker opens `final_url` in Chromium
  (`internal/renderworker/worker.go:674`). Plus the bypass of the filter itself
  via IPv4-in-6: `addrIsInternal` (`transport.go:617-620`) without
  `Unmap()` — `::ffff:127.0.0.1` / `::ffff:169.254.169.254` (cloud
  metadata) pass as "external" (the neighboring code in `limits.go:330`
  uses Unmap). The vector is reachable via the public form too
  `POST /api/check-domain` (`internal/web/web.go:160`, `api.go:96`):
  the full pipeline on an arbitrary name, the response carries the statuses,
  the redirect chain and the Evidence of fired rules (`api.go:136-157`)
  — a channel of exfiltrating internal content. The fix: a single "the address is allowed" funnel
  with `Unmap`, applied to `FetchRequest.Addrs`,
  the render host's addresses and the `Location` literals; for the Internet profile —
  a strict mode (internal addresses forbidden from any source);
  for the form — a restriction of the Evidence reflection. Criterion: tests on all
  three paths (a start A→internal, a redirect `[::ffff:127.0.0.1]`,
  a render of an internal URL) at `block_internal_addrs: true` give a connection/render
  refusal; docs updated; `make test`/`make lint` green.

- **T-220** [done] — P1, the public live check rebuilds the
  pipeline per every request: `checker.Run` builds a new resolver
  chain — 1+2 clients × an eager pool of `udp_pool_size: 2000`
  (`internal/checker/resolve.go:98-124`,
  `internal/dnsclient/pool.go:73-80`, `internal/config/config.go:735,748`)
  = 6000 UDP sockets per one form request at a `LimitNOFILE` of 65535,
  the crawler-web unit's `MemoryMax=256M` (≈10 parallel checks
  exhaust the fds, including `/healthz`); there also its own `httpfetch.Fetcher`
  with local `limitState` (no service-wide cap),
  a `rules.LoadDir` from disk and `render.Manager`+`Start()` at
  `render.remote_url` (`internal/checker/checker.go:156-193`);
  no semaphore of concurrent live checks — only the per-IP 4/min.
  The fix: a long-lived contour in `web.New` — one resolver chain with
  a small UDPPoolSize for live (8–16), a shared Fetcher with shared limits,
  a rules cache with reloading (SIGHUP/a timer), one render.Manager
  or disabling render for the form; a live concurrency semaphore.
  Criterion: a load test — 10 parallel form checks do not
  grow fds linearly and do not fail `/healthz`; `make test`/`make lint`
  green.

- **T-221** [done] — P1, the landing's unix socket breaks the rate limit and
  the journal: when deployed on a unix socket (a documented variant,
  deploy/nginx/README.md §"The unix socket variant") `clientIP`
  (`internal/web/api.go:263-274`) does not recognize the peer as trusted
  (a RemoteAddr "@" — not a loopback IP) → X-Real-IP ignored: (1)
  a token bucket one for all visitors — 4 checks/min total for
  the site; (2) `recordCheck` inserts "@" into `web_checks.ip_address
  inet NOT NULL` (migration 000012) — the INSERT fails; (3) `recordCheck`
  (:245-253) executes the INSERT via `Query`, `rows.Close()` only
  at `err == nil`, errors swallowed — the T-120 journal silently empty;
  (4) no form metrics at all (a grep of `metrics.` in internal/web empty).
  The fix: a unix peer = a trusted proxy → X-Real-IP/X-Forwarded-For with
  validation; without a header — a separate bucket key without writing
  a non-IP into inet (a skip with an explicit Warn, not silently); `recordCheck` →
  `Exec` + a Warn/refusal metric; the checks/errors/duration counters
  of the form in internal/metrics on :9107. Criterion: a test on a unix socket —
  two different X-Real-IPs get independent buckets; a journal error
  is logged; the form metrics are served; `make test`/`make lint` green.

- **T-222** [done] — P1, RecoverLoop burns the delivery attempts of a
  claimed but unprocessed batch: `XAUTOCLAIM` with
  `Count: Batch` (64 by default, `internal/queue/recover.go:61`,
  `reliab.go:350-357`) immediately increments the delivery counter of the whole
  batch, and the cancellation check stands inside the processing loop
  (`recover.go:164-172`); the graceful shutdown budget of 10 s
  (`internal/service/service.go:42`) is less than a batch's processing time
  (render streams up to 25 s per write) — at every restart during
  a PEL walk the batch's tail loses an attempt, after `max_attempts=3`
  it goes to q:dead without a single processing (the scenario
  T-152/T-153 protected against). The fix: finishing the claimed batch under
  a separate short ctx on cancellation (per the ingest discard sample),
  or returning the unprocessed `XCLAIM … JUSTID RETRYCOUNT <attempts-1>`,
  or a small batch (1–8) for slow streams; at least —
  document the XAUTOCLAIM semantics. Criterion: a test "a cancellation
  mid-batch — the unprocessed records' attempts do not grow";
  `make test` green, `make test-integration` green.

- **T-223** [done] — P2, cmd/http-worker: CompactLoop and
  RetentionLoop do not join before the resource closures: the invariant's comment
  requires "background = we wait in bg until the defer-closures of store/rdb",
  but only three cycles write into `bg` (cap 3) (main.go:207,269,290),
  CompactLoop (:215) and RetentionLoop (:317-328) — fire-and-forget;
  after `run` three `<-bg` (:372-374), then defer store.Close/rdb.Close
  — the cycles may touch closed resources; adding a fourth
  sender would block on the capacity-3 channel. The fix: all background cycles
  via errgroup/WaitGroup (or bg with a margin), wait before
  the defer-closures. Criterion: all cycles join before the closures (a check
  by a test hook or review), `make test`/`make lint` green.

- **T-224** [done] — P2, the scheduler's EnqueueBatch holds a pool
  connection and the cursor during the Redis publication: `defer rows.Close()`
  (`internal/scheduler/enqueue.go:172-176`) is removed only at
  exit, and between the reading and `q.Publish` (:214, retries/pauses up to
  ~0.75 s at Redis failures — redisq.go:145-158) the DB connection
  is held needlessly — pressure on the pool at a slow Redis.
  The fix: `rows.Close()` right after the read/`rows.Err()`, before
  building the message and publishing. Criterion: the connection is not held
  during the network I/O in Redis (a review/a test with a small pool);
  `make test` green.

- **T-225** [done] — P2, renderworker's `ProcessResult` returns
  `true` together with an error: the path "the transaction committed, the q:analyze
  publication failed" (`internal/renderworker/worker.go:817`) —
  `return true, err`, all other error paths — `false, err`;
  the calling `processDelivery` (:1260) ignores the value on error
  (no observable defect), but the contract "true = the record processed"
  broken — the next consumer will consider the delivery successful.
  The fix: `false, err`, the DOM-write fact — a separate metric/log.
  Criterion: a contract test of `true` only at `err == nil`;
  `make test` green.

- **T-226** [done] — P2, the CDP probe `probeConn` changes the browser's generation
  by a single timeout: any `Target.getTargets` error within
  `probeTimeout = 2s` (including the DeadlineExceeded of an overloaded
  Chromium) sets a sticky `lost` (`internal/render/browser.go:242-265`)
  → `Alive()` false → Manager changes the whole generation (a pool retire,
  task interruptions, a cold restart); the probe is called on every
  task and every page timeout (`internal/render/session.go:43,228-236`,
  `browser.go:359-370`) — a false death on a loaded worker is more expensive
  than an extra probe (this is the flip side of the T-218 countermeasure, not the leak itself).
  The fix: require 2 failures in a row (or a series with a backoff), distinguish
  a timeout from a transport error of a closed socket, raise
  `probeTimeout` for a loaded browser. Criterion: a unit test —
  a single probe timeout does not extinguish the generation, a double one does;
  `make test` green.
- **T-227** [done] — P3, web/dashboard small trust-to-request items:
  (1) the session cookie `dashboard_session` without `Secure`
  (`internal/dashboard/authmw.go:80-88`) — at direct access bypassing
  nginx it would leave over plaintext; set it when working behind TLS (a flag/
  X-Forwarded-Proto from a trusted proxy). (2) sitemap.xml assembles
  `<loc>` from the visitor's `r.Host` (`internal/web/web.go:425-451`) —
  host-header poisoning of caches/crawlers at direct access or
  a wrong proxy; take the canonical host from the configuration or
  an allowlist. (3, grok) the sitemap scheme: `X-Forwarded-Proto`
  (`web.go:440`) is trusted from any peer (unlike `clientIP` —
  only loopback/unix) — trust only a trusted peer, otherwise
  the default by the actual TLS. (4, grok) the form errors 429/503/502 (`api.go:125,
  240,247`) — a JSON body via `http.Error` with a `Content-Type:
  text/plain` (422 via `checkWriteError` sets it correctly) —
  unify via `checkWriteError` without breaking landing.js (it
  looks at the status). Criterion: Secure in TLS mode; the sitemap independent
  of the Host/headers of untrusted peers; the form errors — JSON with
  the correct Content-Type; `make test`/`make lint` green.

- **T-228** [done] — P3, content: (1) `store_contract.go` without
  a build tag imports `testing` and lands in the production binary
  (`internal/content/store_contract.go:11-17`; the comment
  pins the deliberateness — needed by the external integration tests) —
  extract into a `contenttest` package imported only by tests.
  (2) a latent PutBatch contract defect (found by kimi before the
  quota cut-off, verified): `sqlMetaAddRefBatch` gives `refcount + 1`
  per hash regardless of the number of in-batch duplicates of live
  content (`internal/content/batch.go:47,220-242`), although the contract is
  "every Put of an element adds a reference"; the INSERT path of new rows
  writes the counter correctly (counts → sqlMetaPutBatch); there are no
  production PutBatch calls now — the defect is latent. The fix: an UPDATE with
  an increment of `+ count` (a VALUES-join), a contract test
  "a duplicate × a live row". Criterion: the refcount test green;
  `testing` is not imported outside test packages (a build check);
  `make test` green.

- **T-229** [done] — P3, collapse/render small items: (1) cleaning
  `moving_ids` after carrying the zones — `conn.Exec(context.Background(),
  …)` without a timeout (`internal/collapse/collapse.go:778-784`): at
  a hung PostgreSQL the CLI hangs unboundedly, holding a connection;
  give `WithTimeout(5s)` per the ingest discardTimeout sample, the error —
  into the log. (2) `resolveRemoteWSURL` takes only the first DNS address
  (`internal/render/remotealloc.go:130-139`) — at an IPv6-first order
  Chromium (listening on IPv4 only) the generation will not come up with a live
  browser; iterate the addresses or prefer IPv4. (3) the RSS threshold
  is read by a full /proc walk on every task
  (`internal/render/manager.go:208` → `procTreeRSS`) — only
  the local chromium mode (remote returns −1, does not
  fire in production); a cache with a TTL of 5–15 s is enough. Criterion: a DROP with a timeout
  does not block the CLI's exit; WS connects by the second address (a test);
  `/proc` is read no more often than the TTL; `make test`/`make lint` green.

- **T-230** [done] — P1 (grok, verified), SSRF: the contour
  enabled by code but disabled in the deploy examples, and render does not catch
  DNS rebinding: (1) `config.example.yaml:128` —
  `block_internal_addrs: false` and in `deploy/systemd/
  crawler.env.example` there is no `CRAWLER_HTTP_BLOCK_INTERNAL_ADDRS` at all,
  although docs/configuration.md require the "mandatory" flag for
  Internet operation — the repository does not pin the safe
  value, a fresh deploy by the example is vulnerable (T-219 closed the code, not
  the configuration); (2) `requestURLInternal`
  (`internal/render/block.go:289+`) checks only literals/
  numeric-like WHATWG forms/localhost — an arbitrary name goes into
  Chromium and is resolved by the browser: classic rebinding (a name
  public at the HTTP stage, internal — 169.254.169.254/RFC1918 by
  the render moment; a JS redirect/a subresource takes the response, the DOM — into
  the store) documented as a residual risk
  (docs/render.md:82-86) leaning on a network egress policy
  of the container that does not exist in the repo. The fix: enable the flag in
  config.example.yaml (a comment: dev/testbed switch off deliberately)
  and add the variable to crawler.env.example; in the render interception
  with the flag on, resolve the request's host name (the render resolver,
  a per-session cache, to avoid multiplying exchanges) and reject
  `httpfetch.AddrIsInternal` addresses — the rebinding window between the resolve and
  the load narrows to the milliseconds of CDP; update docs
  (configuration.md, render.md — remove the "residual risk" or
  reword). Criterion: the env example contains
  `CRAWLER_HTTP_BLOCK_INTERNAL_ADDRS=1`; a rebinding-scenario test —
  a name resolving to an internal address at the render moment, with
  the flag on, rejected by the interception (a fake resolver), with
  it off — passes; `make test`/`make lint` green.

- **T-231** [done] — P2 (grok, verified), dashboard: the "5
  failures per minute" window is not atomic — `internal/dashboard/authmw.go:61-73`:
  `limiter.check(ip)` and `limiter.record(ip)` (the counter write —
  `:185-205`) take the mutex separately, between them the expensive
  PBKDF2 verification (`verifyCredentials`): a parallel batch of Basic
  requests from one IP all see `fails < 5` before the first `record` and
  all reach the KDF — the limit holds only sequential attempts;
  along the way a CPU DoS: "a blocked address must not spend KDF"
  (`authmw.go:41-43`) only takes effect after the failure is recorded. The form's
  limiter (`internal/web/api.go`) does not suffer this — the spend and
  the check under one mutex. The fix: reserve a failure slot in
  the same critical section as the check (an increment before the KDF,
  a reset on success), so that a failed attempt over the limit does not call `PasswordHash.Verify` at all; if needed — a semaphore
  of concurrent verifications per IP and a global in-flight KDF cap.
  Criterion: a test — 10 parallel failed Basic requests from one
  IP: the KDF called ≤5 times, the rest — 429 without a verification (before the fix
  the test red — all 10 call Verify); the sequential behavior and
  the window reset on success not broken; `make test`/`make lint` green.

- **T-232** [done] — P2 (out-of-frame of T-223, verified by the main
  session), the same fire-and-forget class in the other services:
  `cmd/render-worker/main.go:126` (DiskWatchLoop — touches the store,
  closed at shutdown) and `:158` (CollectQueueLoop — rdb);
  `cmd/analyzer/main.go:148` (CollectQueueLoop); `cmd/dns-worker/
  main.go:168` (CollectQueueLoop) — started `go func(){ _ = … }()`
  without a join: after run returns the defer-closures of rdb/store may
  fire under live cycles (the invariant restored in
  http-worker by task T-223). The fix: the same T-223 pattern —
  a derived cancellation + `sync.WaitGroup` over all background cycles,
  a join-defer before the resource closures; an AST test per the sample
  `TestRunBackgroundJoin` for each main (or a common helper if
  the duplication is justified). Criterion: in all three services the cycles
  join before the closures (the AST test green, a mutation check
  of the regression classes as in T-223); `make test`/`make lint` green.

## Stage 35: choosing the render engine and the fallback queue (T-233…T-249)

The source: [`TZ_render_engines.md`](./TZ_render_engines.md) (version 0.1).
The "§" and "FT" references in the tasks — to the sections and functional requirements
of this spec. The parallelism waves: A (T-233/236/237/241/242 — independent,
launched simultaneously in separate worktrees), B (T-234/238/243),
C (T-235/239/240/244), D (T-245/246), then the operational T-247…T-249
(sequential). Each task is atomic: one mergeable block,
green `make test`/`make lint` before handing to review.

*(the code part of T-233…T-246 — waves A–D — moved here 2026-11-08; the operational T-247…T-249 stayed in the active plan, the "Operational" section below was not moved)*

### Wave A (parallel)

- **T-233** [done] — P1, config: the `render.engine` field and the whole
  `render.fallback` subsection — the feature's configuration contract in one
  block (spec §4.1 items 1–2, FT-1/2/3/14). In `internal/config`: the enumeration
  `chromium|moli|lightpanda` with a `chromium` default (the default reproduces
  the current behavior — a test invariant), the fallback subsection: enabling
  (a false default), the worker B pace cap (records per window), the share budget
  (a percent over a sliding window, 0 — no cap), the health-gate
  streak threshold; validating the "engine × remote_url ×
  restart_memory_bytes" coupling: for `moli|lightpanda` remote_url is mandatory and
  restart_memory_bytes — 0 (the error wording sample — the rangeMsg of the
  render section), the new fields' ranges — per the style of the existing checks
  (config.go, error strings in the unit tests). Field documentation:
  `config.example.yaml`, `docs/configuration.md`. Criterion: a validation
  matrix in the config tests (all the coupling's combinations + the range boundaries),
  the default snapshot of the configuration without the new fields unchanged;
  `make test`/`make lint` green.

- **T-236** [done] — P1, renderworker: the consumption stream — a
  worker assembly parameter (spec §4.1 item 3, FT-12). Today `queue.StreamRender`
  is hard-wired at the consumption and metric collection points (`internal/renderworker/
  worker.go:1028,1137`, `cmd/render-worker/main.go:197`): a Stream field
  in the worker's Options (the default — `q:render`, zero behavior change),
  wiring all the points through the field; the group/consumer — already Options
  parameters (DefaultGroup/DefaultConsume nearby). Criterion: a unit test
  of Options with a stream override; an integration test of consuming
  an alternative stream by a separate group (per the sample of the existing
  queue integration tests); the default behavior unchanged
  (the existing tests without edits); `make test`/`make lint` green.

- **T-237** [done] — P1, queue: the `q:render-fallback` stream (spec §4.3,
  FT-6). `internal/queue/queue.go`: a StreamRenderFallback constant next to
  the existing ones, inclusion into allStreams (trim/claim/reliability
  pick it up automatically); a MAXLEN cap per the q:render sample
  (T-025) — a value from the same mechanism as the existing streams
  (trim.go); the message format — the existing queue.Result contract
  unchanged. Criterion: the trim/claim/PEL tests on the new stream
  (per the sample of the existing tests), an allStreams invariant in a unit test;
  `make test`/`make lint` green.

- **T-241** [done] — P2, the dev stack: compose services of both alternative
  engines (spec §4.7 item 1, FT-16/17/18). `deploy/docker/compose.yaml`:
  the `lightpanda` service (the official image, the telemetry-disabling
  environment variable — mandatory, FT-18) and `moli` (our own
  minimal image from the release tarball — `deploy/docker/Dockerfile.moli`,
  there is no official image; without `--layout`, without `--resource`,
  `--obey-robots` passed to no one); the addresses in the app network per the addressing plan
  (§7 item 8 of the spec — choose the name/address here and pin it),
  a `/json/version` healthcheck per the chromium service sample, a mem_limit from
  spec §4.7 item 2 (initially ≤1 GB), pinned versions — the stack's environment variables
  (an analog of CHROMEDP_HEADLESS_IMAGE, FT-17). `deploy/docker/
  config.dev.yaml` + `.env.example`: the render.remote_url addresses
  of both engines' profiles; `docs/dev-environment.md`, `docs/docker.md` —
  switching the engine locally. Criterion: the stack comes up, both engines' healthchecks
  green, an address/variable snapshot in the doc; a live
  CDP smoke per the env_test TestChromium_CDP sample for both engines
  (optionally behind a flag); `make test`/`make lint` green.
- **T-242** [done] — P2, a fixture export tool from production
  (spec §6.1 items 1–2). A one-off tool (the home — choose by conventions:
  a crawlerctl subcommand or scripts/): an SQL selection of 3000 records
  of subdomain_results — real q:render tasks (the T-057 SPA verdict,
  http_published), a stratification of ~2000 random chromium-successful /
  ~500 problematic (the T-172 tar pits/timeouts/finalizations of the last weeks)
  / ~500 SPA-heavy (a large DOM), proportional to zones; plus
  a smoke subsample of 300; exporting zones/domains/subdomain_results without
  the content store, the dump format documented (a contract for T-243).
  Criterion: on a production copy (a pg_dump stand) the export gives a dump of the expected
  structure and stratification (the tool's self-check by composition);
  format documentation; a repeated export idempotent.

### Wave B (after A)

- **T-234** [done] — P1, render: the engine in Options and the allocator
  (spec §4.2, FT-4; depends on T-233). `internal/render/render.go`:
  an Engine field in Options + carrying into NewOptions + an Options validation
  repeating the spec's coupling (engine × RemoteURL × MemoryThreshold);
  `internal/render/browser.go`/`remotealloc.go`: for moli|lightpanda —
  a remote allocator with `chromedp.NoModifyURL` (spec §4.2 item 1: the flag is required
  by both engines), the exec mode stays chromium-only.
  The FT-4 invariant: Session/Pool/Manager/extract/block/wait without branchings
  by engine — checked by review. Criterion: unit tests of the coupling's validation
  and the NewOptions wiring; the existing core tests without edits;
  `make test`/`make lint` green.

- **T-238** [done] — P1, publishing the fallback from the finalization paths without
  render (spec §4.4 items 1–2/5, FT-7/8/10/11; depends on T-233, T-237).
  `internal/renderworker/worker.go`: on the T-148 tar-pit paths and the
  task class of T-172 (the last attempt) — instead of setting
  render_failed_at, publishing a task to q:render-fallback in the same
  DB transaction (per the sample of the finalization's coupling with the q:analyze publication);
  the T-207 DNS class and the T-159 cert class finalize as today — without
  a fallback; the publication's idempotency — a redelivery of a record gives no
  second record in the stream (a filter by the result record's identifier,
  the sample — the parking dedup); the publication decision — via a dedicated
  pure decision function (candidate × gate × budget) with a stub
  always-publish for this task (the gate's implementation — T-239, a parallel
  extension point); the crawler_render_fallback_published_total
  {reason} counter in metrics.go (reason — the taskFinalizeReason dictionary, spec §4.6
  item 2). The acceptance map: a fallback scenario in test/integration/
  acceptance_test.go. Criterion: unit tests of the three paths (a tar pit →
  a fallback; a task class last attempt → a fallback; dns/cert → a marker without
  a fallback), a transactional test (a publication failure — no marker,
  the record stays for redelivery), a duplicate delivery does not duplicate the stream;
  `make test`/`make lint` green.

- **T-243** [done] — P2, applying the fixture and stage-local publication
  (spec §6.1 items 3–4; depends on T-242). The tool (the same home as
  T-242): an idempotent apply of the dump into the dev stack's test DB (the
  dev stack's migrations + the import, a repeated apply does not duplicate) and publishing the
  fixture's tasks straight into q:render (queue.Publish, without the dns/http stages) —
  the render stage tested in isolation. Criterion: a repeated apply —
  idempotent (a key-duplicate test); a render-worker run on the fixture
  reproducible (two runs — a control of live pages' variability);
  the run instruction in docs/dev-environment.md.

### Wave C (after B)

- **T-235** [done] — P2, the engine label in the stage metrics (spec §4.6 item 1,
  FT-15; depends on T-234). `internal/renderworker/metrics.go`: an
  engine label on crawler_render_records_total and
  crawler_render_task_finalized_total (a cardinality of 3: chromium/moli/
  lightpanda; the engine comes from the worker's Options — wiring in
  the initialization), the existing dashboard queries grouping without
  engine do not break (a new label does not change the existing aggregations).
  Criterion: metric tests with the label per the metrics_test sample;
  `make test`/`make lint` green.

- **T-239** [done] — P1, the health gate and the fallback share budget (spec §4.4
  items 3–4, FT-9; depends on T-238). `internal/renderworker`: implementing
  the decision function from T-238 — a gate over a streak of ClassBrowser infra errors
  (the fallback publication suspended until the engine recovers; the
  streak-logic sample — renderFailBackoff) and a sliding share budget (a share
  of publications over the window above the cap from the T-233 config → a suspension until
  the window's end, tasks finalized regularly with the render_failed_at marker);
  a suspension event metric {reason: gate|budget} in metrics.go.
  Criterion: unit tests of the states (healthy/streak/the budget window/recovery);
  an integration scenario "the engine fell → no fallback publications, tasks
  in redelivery"; after the budget is exhausted — the marker set (a test);
  `make test`/`make lint` green.

- **T-240** [done] — P2, worker B: the consumption pace and the
  deployment profile (spec §4.5, FT-13; depends on T-236, T-237).
  `internal/renderworker`: a consumption pace limit (records per
  window from the T-233 config — the spec FT-13 batch mode, per the sample of the existing
  worker windows/ticks); the fallback worker's configuration profile: engine=
  chromium, stream=q:render-fallback, the group render-fallback-worker,
  the pace from the cap; deployment — a unit/profile per the sample of the existing
  render workers (deploy/, docs/deploy.md §2.2): a local
  headless-shell unchanged (today's render image), the T-208 recycle pair
  applies as is. Criterion: a pace test (over the window no more than
  the cap, idle at an empty queue); the profile passes config
  validation; `make test`/`make lint` green.

- **T-244** [done] — P2, the engine comparison report (spec §5.1 item 3, §6.1
  item 5; depends on T-243). The report tool (the T-242 home): the input —
  the run results (the test DB's tables + the Prom endpoints' metrics),
  the output — the §5.1 comparison metrics: the share of successful renders, the
  finalization reasons (the net::ERR_* dictionary), the pace (pages/s, mean/p95), the engine's RSS
  memory, the Laravel detection diff (the analyzer's verdicts against the
  chromium baseline, the T-213 protocol), the DOM's structural metrics (length, nodes,
  scripts) — a normalized comparison, a byte-wise diff not applied
  (live DOM nondeterministic). Criterion: on two runs of one
  configuration the detection diff ≈0 (a sanity); on an artificially spoiled
  run the report catches the divergence (a test); the §5.1 item 4 threshold metrics
  output as explicit verdict lines.

### Wave D (after C)

- **T-245** [done] — P2, the fallback consumption metrics (spec §4.6 item 2,
  FT-15; depends on T-238, T-240). `internal/renderworker/metrics.go`:
  crawler_render_fallback_consumed_total (worker B's consumption) with
  a success/finalized-with-marker split; a tooling balance check:
  published (T-238) vs consumed + the PEL remainder — a metric/endpoint for
  cross-checking step 4 of the spec §6.2 acceptance. Criterion: counter tests per the sample
  metrics_test; the balance cross-check on an integration fallback run
  converges (0 divergences); `make test`/`make lint` green.

- **T-246** [done] — P2, the monitor and dashboard of engines/fallback (spec §4.6
  items 3–4, FT-15; depends on T-237, T-235, T-245). `internal/monitor`:
  the depth of q:render-fallback (XLEN/lag) per the q:render monitoring sample,
  alerts: a depth growth (worker B does not keep up), exceeding the share
  budget (the T-239 events), an engine unit's fall; `internal/dashboard`:
  an engines panel — pace/success/memory (the unit's RSS from system
  metrics)/the fallback rate, per the sample of the existing render stage panels;
  docs/operations.md — the alerts and thresholds. Criterion: monitor-selection tests
  per the existing sample; dashboard tests per the existing sample;
  `make test`/`make lint` green.

## Stage 36: the fixes of stage 35's extra review round + per-engine tests (T-250…T-254)

The source: the extra-round review of all of stage 35 (16 findings: 4×P1, 6×P2,
6×P3; the report — `/tmp/opencode/review/stage35-full-review.md`, the reviewer
deepseek-v4.1-flash) + the operator's decisions on the disputed points (2026-11-06:
pre-creating series; the streak "≥ the threshold" without requiring growth; the tar-pit rate
a separate column; a rate alert of browser errors; accepting MAXLEN with
a control in acceptance — added to the spec v0.2; `--force` for fixtures import).
The mode — AGENTS.md's "Bulk code review and fixes": a domain group = one
worktree `fix-s35-<slug>`, **each finding — a separate commit**
`fix: <description> (S-PN-N)`, before each commit
`COMPOSE_PROJECT_NAME=crawler make test` + `make lint` in the worktree,
the subagents' model `zai-coding-plan/glm-5.3#max`. An invalid finding is not
fixed but analyzed in the report. A window of 1/1, the groups' order
sequential: T-250 → T-251 → T-252 → T-253 → T-254 (the groups' files
barely overlap; sequential because of the window; T-254 — after
all fix tasks, the operator's 2026-11-06 decision).

- **T-250** [done] — fix-s35-worker (internal/renderworker +
  acceptance): (1) S-P1-1 — the engine guard of the fallback publication:
  `fallbackArmed()` does not publish at engine=chromium (step 1 of the
  spec §6.2 acceptance, the §6.4 rollback "it dies out by itself", FT-14); acceptance
  TestAccept03_RenderFallback — the primary worker with a candidate engine;
  a unit test "chromium+enabled → no publications"; the render.md/deploy.md docs.
  (2) S-P1-4 (the decision: pre-creating series) — zero children
  `suspended{gate,budget}` and `consumed{rendered,finalized,skipped}` in
  NewMetrics (+ for consistency — evaluate `fallback{reason}`/
  `tasksFin{reason,engine}`: the dictionary open — pre-create only
  the closed ones, argue the opening); the test "the first event 0→1 →
  the alert firing" on the monitor side lives in T-252, here — the metrics.
  (3) S-P2-4 (the core) — the engine label on
  `crawler_render_tarpit_finalized_total` (we do NOT change the panel's success
  denominator — the operator's decision); services.md. Criterion: the
  three items' tests green, the default behavior (chromium without a fallback)
  unchanged.
- **T-251** [done] — fix-s35-infra (internal/config + deploy):
  (1) S-P1-2 — `restart_memory_bytes: 0` in deploy/docker/config.dev.yaml
  + an explanation in the commented engine samples + a dev profile test
  with engine=moli/lightpanda passing Validate + dev-environment.md
  (a switch = engine+remote_url, the threshold already 0). (2) S-P1-3 —
  pinning chromium to worker B in systemd: EnvironmentFile
  -/etc/crawler/render-fallback-worker.env AFTER the common one
  (CRAWLER_RENDER_ENGINE=chromium, remote_url chromium), the installation in
  deploy.md §2.3/§4.4. (3) S-P2-2 — implement
  CRAWLER_MONITOR_RENDER_METRICS_URL and CRAWLER_DASHBOARD_RENDER_METRICS_URLS
  in envSpecs + TestEnvOverrides tests. (4) S-P2-3 — worker B's metrics port
  9110→9120 (the unit + docs/deploy.md + docs/docker.md; the @-range
  9109/9111+ untouched). (5) S-P2-5/S-P3-3 — the default
  dashboard.render_metrics_urls=[127.0.0.1:9105/metrics], a deploy step
  of B's address (9110→9120) in docs/deploy.md §2.3 and the T-248 checklist, a
  render-fallback job in deploy/docker/prometheus/prometheus.yml.
  (6) S-P3-4 — docs/configuration.md: where worker_rate lives in docker
  (the B profile) vs systemd (the common config). Criterion: profile tests
  green, docker compose config valid, env tests green.
- **T-252** [done] — fix-s35-monodash (monitor + dashboard + status):
  (1) S-P2-1 (the decision: a streak without growth) — render_fallback_depth:
  a second trigger "n ≥ the threshold 2 passes in a row" without requiring growth;
  tests: a steady state of 9000 → firing, pressing against a MAXLEN of 15000 → firing+holding,
  a single spike between passes → silent. (2) S-P2-6 (the decision:
  a rate detector) — the render_engine_browser_errors alert (or extending
  the existing one) on the growth of `crawler_stage_errors_total{stage="render",
  type="browser"}` from the render-worker probe with a streak of 2-3 passes;
  the gate event stays a counter/log; operations.md/services.md.
  (3) S-P1-4 (the monitor part) — the tests "the budget's first event →
  firing", "a missing series = 0, not not_measured" (after
  the T-250 pre-creation; the parser — per the T-250 decision, the test here).
  (4) S-P3-1 — q:render-fallback in internal/status Streams,
  monitor/load reportStreams, crawlerctl status's help; the comments
  "five streams" → current. (5) S-P3-2 (the remainder) — the panel: a column
  "tar-pit rate" (from the tar-pit counter with engine from T-250), an RSS column
  (process_resident_memory_bytes of the exposure), a line
  "the fallback rate = published/pages", `by.* || 0` on the front;
  docs/services.md — the render section JSON+front. Criterion: the alert/panel tests
  per the existing sample green.
- **T-253** [done] — fix-s35-tools (crawlerctl fixtures): (1) S-P3-5
  (the decision: --force per the zones collapse sample) — `fixtures import`
  requires an explicit `--force` to execute the apply, prints the target before starting
  (the host/database DSN, the row counters by tables) and the affected
  tables; without --force — a dry-run output of the plan; update the cycle in dev-environment.md.
  Criterion: the flag/dry-run tests; the help in sync.
- **T-254** [done] — per-engine live render tests (two-layer;
  depends on T-251 — the fixed dev switch profile and the engines'
  addresses; done after all the stage's fix tasks). Layer 1 —
  a testbed regression: live tests of the full CDP cut of each engine
  (chromium/moli/lightpanda) against the dev stack's `*.crawl.test` zones per
  navigate + taking the DOM/cookies/headers, the Fetch interception of blocking
  (T-062/213/214 — the block_live_test sample), the SSRF contour (T-219/T-230),
  the UA override; the wiring — environment variables per the
  CHROMIUM_WS_URL sample (the addresses of the render-engines profile's services from T-241);
  the profile not up — SKIP without errors; the tests in the regular make test behind
  the flags do not fire. Layer 2 — a real smoke per FT-17: an optional
  (a separate env flag + internet, never in CI) run of each
  engine over the T-242 fixture's smoke subsample (300 real domains):
  fixtures import --publish smoke (T-243) → a render-worker in the
  engine's profile → snapshot/report (T-244) → a comparison report; formalizes
  the FT-17 requirement (a smoke at an engine's pinned-version change) — document
  the procedure (docs/render.md or dev-environment.md).
  Criterion: layer 1 green on the raised profile for all three engines,
  SKIP without a profile; layer 2 reproducible on the smoke subsample, the report
  generated, without the flag — skipped; make test/make lint green.

## Stage 37: formalizing the pipeline's implicit finite-state machines (T-255…T-266)

The source: [`TZ-fsm.md`](./TZ-fsm.md) (version 0.1; the primary source — the
codebase audit of 2026-11-06, the machines' registry — the spec §2). The "FT" references — the
spec's functional requirements; the formalization principles — the spec §4 (the
specification format, invariant tests, the allowed behavior changes).
The project rule — `AGENTS.md` "Rules": behavioral logic — an explicit
machine. One machine — one task. Waves: A (T-255/257/259/260/263/265 —
non-overlapping packages), B (T-256/258/261/264/266), C (T-262 — after
T-258/T-261/T-266: both workers' readers).

### Wave A (parallel)

- **T-255** [done] — P0, scheduler: the domain's `check_state` machine (the spec
  §3 FT-1). Specifying the transitions (pending → queued → done; the lease return,
  the daily recheck, the temp_error retry, a manual recheck, a resurrection)
  as a "state × event × guards × target × side effects" table in
  the package's doc comment (the single point of truth, the workers' writers
  reference it). The FT-1.2 fix: a terminal insurance of `queued` rows
  with a NULL `queued_at` (today an eternal hang — lease does not
  see them) — a repair pass in `LeaseWatch` by a separate statement (without changing
  the main lease query's form and `domains_lease_idx`'s suitability),
  a `crawler_scheduler_lease_null_returned_total` metric, runbooks.
  Criterion: integration tests of all the specification's transitions + a test
  of the NULL insurance (inserting queued/NULL → a return to pending, the counter);
  `make test`/`make lint` green.

- **T-257** [done] — P1, dnsworker: the `http_published` bit machine (the spec
  §3 FT-3). Specifying the staging bit's lifecycle (4 transitions:
  setting after the XADD of q:http, clearing by a result write, clearing
  by the scheduler's setting, skipping the publication at a redelivery) in
  the doc comment of `internal/dnsworker/result.go` next to
  `resultMarkPublishedSQL`; a scenario integration test covers all
  four transitions by one chain, including the "XADD without the bit" window (a duplicate
  publication allowed, documented). The behavior unchanged.
  Criterion: the test chain green, the specification in the doc; `make test`/
  `make lint` green.

- **T-259** [done] — P0, renderworker + httpworker/renderq: the
  render-state machine of a subdomain_results row (the spec §3 FT-5). The
  states' specification (`a new episode → queued → rendered | impossible |
  awaits fallback | slipped from under the task`; the columnless "awaits fallback" —
  explicitly, with an observation mechanism) in the doc comment
  of `internal/renderworker/worker.go`. FT-5.2: a CHECK
  `rendered ⇒ render_failed_at IS NULL` by a `NOT VALID` migration (a VALIDATE —
  per the regulations), a paired test invariant. FT-5.3 (a fix): a "late
  re-publisher" — a background pass re-publishing to q:render the records
  `rendered=false AND final_url IS NOT NULL AND render_failed_at IS NULL`
  older than 6 h without the domain's `render_pending` bit and without unconsumed
  q:render/q:render-fallback messages (an interlock per the
  `content/retentionpending.go` sample), a batch/an invalidation per the finishing pass's
  sample, a `crawler_render_late_republish_total` metric — it cures the loss
  of MAXLEN-trimmed fallback tasks and lost publications. Criterion:
  the specification; the re-publisher's integration tests (a lost record →
  a publication; a record in the stream/under the bit → untouched); a migration
  `make test-integration`; `make test`/`make lint` green.

- **T-260** [done] — P0, queue: the stream record's delivery machine (the spec §3
  FT-6). The FT-6.1 fix: a terminal branch for PEL records without a `task_id`
  (today an eternal claim loop — `reliab.go:593-597`): `bury` publishes to
  q:dead a record with a synthetic `task_id` `unknown:<stream>:<entry-id>`
  (without subject fields), the original's XACK, the
  `crawler_queue_dead_unidentified_total` counter. The FT-6.2 fix: records with an
  XPENDING classification error after a claim included in `Refund` (a RETRYCOUNT
  rollback) — the T-222 window extends from "a ctx cancellation" to "any unfinished
  classification". Criterion: unit/integration tests of both branches
  (a record without a task_id leaves the PEL and is visible in q:dead; the attempt does not burn
  at a failing XPENDING); `make test`/`make lint` green.

- **T-263** [done] — P2, render: the browser generation machine (the spec §3
  FT-9). The generation's liveness — one predicate method with an enumeration
  of the states (absent | starting | active | draining | dead |
  closed) instead of five independent checks (`manager.go:173-225`);
  `swapReasonLocked`/`Alive` use it; a doc transition table
  (the replacement reasons, the side effects). The behavior unchanged. Criterion: unit tests
  of all the states on cdpfake browsers; `make test`/`make lint` green.

- **T-265** [done] — P2, content: the content refcount machine (the spec §3
  FT-11). A CHECK `contents.refcount >= 0` by a `NOT VALID` migration (a VALIDATE —
  per the regulations after a soak); the machine's specification (absent → live(n)
  → dead(tombstone|deleted) → deleted, a resurrection `ON CONFLICT EXCLUDED`,
  a `GREATEST` clamp) in the doc comment of `internal/content/pack.go`; a table
  of the two "dead" representations in `docs/content.md`. Criterion:
  invariant tests (reviving a dead row, a clamp at a desync,
  a double Delete → ErrNotFound); `make test`/`make lint` green.

### Wave B (after A)

- **T-256** [done] — P0, model + dnsworker/httpworker + monitor:
  the domain's `status` machine (the spec §3 FT-2). Specifying the transitions (the
  8 states of spec 6.2): a decision table "stage × condition → status" — the common
  part in `internal/model/enums.go`, the stage branches in the packages' docs of
  dnsworker/httpworker; the deliberate asymmetries (temp_error→
  delegated_no_address at attempts exhaustion; connect-refused→no_http
  against a connect timeout→temp_error; the absence of a done guard at
  the HTTP stage) pinned as decisions, NOT changed. FT-2.2: the observability
  of "the status hung" — a `crawler_domains_new_done_stale` gauge (domains
  status=new ∧ check_state=done ∧ last_checked_at older than 24 h) in the
  `monitor/zonestats.go` pass and an alert (a streak of 2, normally zero in steady state);
  no self-healing — a signal to the operator. Criterion: the decision table's tests
  (a unit over pure functions) + an alert test per the existing sample;
  `make test`/`make lint` green.

- **T-258** [done] — P1, httpworker/renderq: the `render_pending` bit machine
  × the debt counter (the spec §3 FT-4). The specification (no-debt
  → debt → published → cleared + a draft flag/keep/orphan bits; the invariant
  "the bit and the counter — one transaction", RowsAffected — the indicator of a real
  transition) in the doc comment of `internal/httpworker/renderq.go`;
  integration tests of the ±1 pairing on all transitions (including the clearing at a
  delete file and a redelivery with the bit already set). The behavior unchanged.
  Criterion: the pairing tests green; `make test`/`make lint`
  green.

- **T-261** [done] — P1, renderworker: the render-outcome policy machine
  (the spec §3 FT-7). The fix: the tar-pit finalization decision — by the number
  of tar-pit-classified deliveries of the episode (a Redis set
  `render:tarpit:{task_id}`, an SADD by delivery id — idempotent, a
  48 h TTL, SCARD ≥ `render.timeout_attempts`), not by the total
  PEL delivery number — mixed series stop finalizing an attempt
  earlier (`worker.go:1805-1808`); the common `redis.max_attempts` limit
  works as before; a Redis loss — a degradation to the current behavior.
  Criterion: the mixed-series tests (an infra failure between tar pits —
  no finalization until the tar-pit threshold), the redelivery idempotency
  (an SADD of the same id), the TTL degradation; `make test`/`make lint`
  green.

- **T-264** [done] — P2, monitor/alerts: the alerts machine (the spec §3 FT-10).
  Every alert — a value of the unified "{name, status, track memory}" structure
  with one `transition`; the track types (a hysteresis, a series, a level latch,
  a ring) typed; the eval functions return an observation, the state
  written only via a transition. The 18 alerts' behavior unchanged:
  the existing tests migrate to the "a sequence of observations →
  a sequence of statuses" format without changing the expectations. Criterion:
  table tests per each machine; a behavior snapshot before/after the refactor
  identical; `make test`/`make lint` green.

- **T-266** [done] — P2, httpworker: the scheme check's state machine
  (the spec §3 FT-12). The allowed combinations of `SchemeCheck` (7 fields) and
  the content part of `Record` (5 fields) — centralized
  validator methods, called at the stage boundaries (before `ContentSaver`, before
  the write); the invariants `ContentNotStored ⇒ hash=nil`,
  `AliasNone ⇔ alias_id=nil` no longer checked disparately; a unit test
  — a matrix of all the legal combinations (including `Available=true ∧
  ErrClass=ErrBody`, a cache hit, DeadDial); distorting `final_url`
  at a cache hit pinned in the doc as deliberate. The behavior unchanged.
  Criterion: the matrix test green; `make test`/`make lint`
  green.

### Wave C (after B)

- **T-262** [done] — P1, httpworker + renderworker: the workers' reader
  modes machine (the spec §3 FT-8). One mode enum (reading |
  a browser pause | an infra pause | a guard pause | a pace pause) in
  each worker: the transitions — one function over the observations (`Alive`,
  `Writable`, `failed`, allowance), the transition table — in the doc and
  a unit test; the semantics kept (a browser/infra pause does not read —
  attempts do not burn; the guard reads). A panic parity: recover accounting
  in the http-worker delivery goroutine (the sample — renderworker) instead of
  a process fall. Criterion: a table test of both workers' transitions,
  a recover-panic test; `make test`/`make lint` green.

## Stage 38: zone × counter deadlocks (T-267…T-268)

The source: the production 40P01 incidents of 07.11.2026 (a code audit per the PG deadlock
DETAIL; the lock-order invariant — `docs/schema.md`, the
crawler_counters section, and `docs/runbooks.md` §2.8). After the 15ff3a3 fix
(splitting the RetryOnce tx) the deadlocks continued (~3–4/h): the proven
edge "holds crawler_counters → waits for zones" is created by the unreferenced
DML CTEs `zc`/`rm` of one statement at dns-worker
(`internal/dnsworker/result.go:155-178`, autocommit; in production on 07.11
they executed in the order `rm → zc` — an inversion against the convention, PostgreSQL does not guarantee
the order of unreferenced CTEs) and scheduler-requeue
(`internal/scheduler/retry.go:159-176`); the cycle closes through the
http-worker `RecordAndPublish`, holding the zones until Commit and waiting for
`retry_mature:today`. The tasks sequential (shared files): first
the structural fix, then the insurance.

- **T-267** [done] — P0, dnsworker + scheduler: remove the
  `retry_mature:` increment from the hot CTE (the counter and the zones — different
  statements). (1) `internal/dnsworker/result.go`: the `rm` CTE leaves
  `resultUpdateStatusSQL` — the statement becomes strictly
  "domains → zones"; after a successful temp_error outcome write
  (recorded > 0 ∧ SetStatus ∧ status=temp_error) a separate
  autocommit upsert of `+1` of the `retry_mature:today` bucket in
  `RecordEpisode` (result.go:402-426). (2)
  `internal/scheduler/retry.go`: in the `retryDecide` tx for a requeue —
  two statements instead of one: (upd+zc), then an rm decrement by
  the first one's keys (the zones held until Commit — the order convention;
  the "counters → domains" edge removed by 15ff3a3 does not return);
  `retryFinalizeSQL` untouched (rm depends on upd, no zones).
  Semantics: a ±1 non-atomicity window until the nearest reconciliation (≤30
  min) — safe: the family's authority is monitor
  (f_retry_mature_today/past), the retry's guard reads only the past days' buckets
  (retry.go:235-237); pin in
  `docs/schema.md`. Criterion: updated SQL contract tests
  (dnsworker/result_test.go, scheduler/retry_test.go) and
  integration ones (test/integration/counters_test.go,
  test/integration/retry_test.go); new tests: the increment by a separate
  statement after the write; a failure between the statements — the transition recorded,
  the bucket caught up by reconciliation; `make test`/`make lint` green
  (DB contracts affected — the main `make test-integration` run
  after the merge).

- **T-268** [done] — P1, db + dnsworker + httpworker + scheduler:
  an application-level 40P01 retry at the hot spots (insurance against residual
  and future cycles; after T-267). An SQLSTATE
  40P01 recognition helper (pgconn.PgError) + a retry wrapper (1–2 attempts, jitter,
  a cap) in `internal/db`; the points: http-worker `RecordAndPublish`
  (a whole-transaction repeat — idempotent), dns-worker
  `RecordEpisode`/`resultCount` (an autocommit command repeat, without
  a repeated DNS episode), the scheduler's `retryDecide` and the temp_error drain
  cycle — one batch's error no longer wrecks the hourly pass
  (today a WARN of 07.11 16:04 wrecked the drain of 118 thousand domains until
  the next tick). A `crawler_db_deadlock_retried_total` metric by
  services. Criterion: a 40P01 fault injection (a fake pool) — the retry
  succeeds, the XACK passes, after the attempts are exhausted the error goes out;
  the drain continues after a batch's fall; `make test`/`make lint`
  green.

## Stage 39: the render_pending counter desync (T-269)

The source: the production incident of 08.11.2026 while investigating `q:dead` per the runbook
(`docs/runbooks.md` §2.5/§6; the operational note
`prod-reports/2026-11-08-010800.md` — the chronology and artifacts).
The chronology: 11:48:04 MSK a manual setting of the `render_pending` flag
on 18 domains atomically with the counter (+18, the §6 template); 11:50:39.982 the
T-066 finishing pass cleared 18 flags with a pair of −18 each
(`renderClearPendingTx`, httpworker) — the expected value 0;
11:50:39.996 (14 ms later) the next pass read the counter already
−18 — a total shift of −36 in one pass with the only observable
set of −18. The counter row's `updated_at` stayed 11:48:04:
the pass's writes do not move it (`renderCounterDeltaSQL` does not touch
`updated_at`) — that is, between 11:50:39.0 and .996 someone else was writing the counter
leaving no trace in `updated_at`. All the bit transitions in
httpworker are paired with guards (D7 — a concurrent clearing does not
decrement; the T-258 specification and the renderbit_test pairing tests
confirm the behavior). The §6 alignment (value = the count of bits)
restored the invariant, the "negative render debt counter" Warn
stopped. The main suspect — monitor's periodic reconciliation
(`internal/monitor/zonestats.go`, T-178: `render_pending` in
the reconciliation list, the passes write `updated_at` too — 07:51:02 UTC
of 07.11/08.11): the hypothesis — applying a delta from a stale expectation
(the expectation snapshot taken before the T-066 pass, the fact — after) gives a second
−18. The hypothesis unproven — the task starts with a reproduction.

- **T-269** [done] — P1, monitor: investigate and fix the zonestats
  reconciliation's `render_pending` write path. (1) An audit of all the writers
  of `crawler_counters['render_pending']` (httpworker's
  `renderBumpDebtCounter`/`renderSetPendingSQL` — the paired invariant
  of T-175/T-258; monitor/zonestats.go — the reconciliation) and the reconciliation's
  semantics: an absolute value against a delta from the expectation.
  (2) Reproduce the race with a scenario test: in parallel a clearing of the flags
  by the finishing pass × a monitor reconciliation with a stale
  expectation snapshot — confirm or refute the double delta application;
  if the hypothesis is not confirmed — find the real mechanism from the
  incident's facts (−36 per pass, `updated_at` mute) and reproduce it.
  (3) The fix: the `render_pending` reconciliation writes the absolute
  `count(bit)` (the source of truth — `domains.flags`, the same predicate
  as migration 000015's backfill), not a delta; pointwise for
  `render_pending`, without breaking the reconstructible families
  (`retry_mature` — monitor's authority, T-267). (4) Documentation:
  `docs/runbooks.md` §6 — a note on manual edits with a live
  reconciliation; on a semantics change — `docs/operations.md` (zonestats)
  and `docs/schema.md` (crawler_counters). Criterion: a reproduction test
  of the 08.11 scenario — after the fix the invariant holds at every step
  (|counter − count(bit)| = 0); the existing zonestats/renderbit tests
  without behavior edits; `make test`/`make lint` green.

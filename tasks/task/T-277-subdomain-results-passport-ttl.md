# Task T-277: TTL for subdomain_results passports of non-findings (10 days)

Version: 1.0. Stage 47. Priority: P2.
Sources: disk capacity study (12.11.2026, ~1.0–1.5 GB per 1M checked domains,
subdomain_results is the main permanent grower) + architecture research
(subagent, 12.11.2026, facts embedded below) + the owner's product decision
(12.11.2026): "passports of non-findings are not needed" — the TTL is approved.

## 1. Context and verified facts

- `subdomain_results` today: ~14.8M rows / 10 GB (heap 7.7 GB + indexes
  1.6 GB), growing ~192k rows/day at the current pace; no cleanup exists —
  rows live forever.
- Row lifecycle (verified in code): rows are created ONLY by the HTTP stage
  for alive episodes (`internal/httpworker/record.go`, `recordInsertSQL`;
  temp_error/no_http episodes do DELETE without INSERT; DNS-stage deaths
  never reach the HTTP stage). 100% of current rows belong to alive domains;
  **~98.7% are retention-trimmed "light passports" of non-findings**
  (`content_hash IS NULL`, older than 6 h): final_url, statuses,
  checked_at — the page content itself is already gone.
- Consumers map (verified): analyzer / render-worker / render-late /
  reanalyze / retention treat "row missing" as a normal outcome; retry and
  recheck work off the `domains` table, not rows; value-based cursors
  (retention T-185/T-274, reanalyze) tolerate holes; Laravel loss detection
  works off `laravel_domains`. The only visible change: `domain_info` of an
  expired non-finding returns `results: []` — accepted by the owner.
- Future ghost class (handled by this TTL automatically): the first
  6-month recheck wave (~April 2027) will kill previously alive domains at
  the DNS stage, which does not delete their rows.

## 2. Requirements

### R1. TTL pass (background drain)
- Periodic pass modeled on `ingest/cleanup.go` (T-020 precedent) and the
  retention drain toolkit (T-185/T-274): batched
  `DELETE FROM subdomain_results WHERE checked_at < now() - ttl
  AND content_hash IS NULL
  AND NOT EXISTS (laravel_domains by key)` with LIMIT-batches.
- Guards (mandatory): Laravel findings and their rows are never touched
  (protected forever — reanalyze material, T-074); rows with a live
  `content_hash` are skipped (fresh or finding); if a hash-bearing row is
  ever selected, the content refcount swap MUST happen in the same
  transaction (`SwapRefTx` pattern) — keep the invariant explicit even
  though trimmed rows have hash = NULL.
- Cycle semantics: per the T-273/T-274 toolkit — per-batch select timeout,
  honest error backoff ("pass + backoff" cadence), `scangate` for heavy
  scans, error metric. First run is a catch-up drain over ~14M rows
  (~10 GB): reuse the drain toolkit's batch/backoff/gate machinery so the
  first pass cannot turn into a T-271-style doomed scan.
- Metrics: `rows_deleted_total`, pass duration histogram, `errors_total`.

### R2. Config key
- TTL in config.yaml (e.g. `content.results_passport_ttl`), **default
  240h (10 days)**, validation corridor (e.g. 24h..180d), env override;
  wired like the other retention keys (`internal/config/config.go`,
  cmd of the owning service). Owner service — executor's choice
  (http-worker hosts the neighboring passes).

### R3. FSM compatibility
- The pass is NOT a domain-status event (same as T-020): no transitions
  in `internal/model/enums.go` change. If the TZ-fsm registry needs a
  note about row lifecycle vs domain FSM — add it there.

### R4. Tests
- Unit: selection predicate (ttl boundary, hash-bearing skipped, findings
  guarded), batch idempotency, metrics.
- Integration (live PG): the full lifecycle — check → retention trim →
  TTL delete → `domain_info` returns `results: []` → a recheck of the
  expired domain recreates the passport (record DELETE+INSERT unaffected);
  finding rows survive; fresh rows survive; counters consistent.
- `make test-race` mandatory (content/httpworker ∈ RACE_PKGS);
  `make test-integration` mandatory (behavior + possible new index).

### R5. Candidate-selection performance
- Verify the candidate select plan with EXPLAIN on prod-order data; if a
  partial index is needed (precedent: `domains_deleted_idx`, migration
  0013), reason the write amplification (T-201/T-104 rejected an index
  over hot columns — the TTL predicate is over `checked_at` + nullable
  hash, decide with numbers). A migration is allowed.

### R6. Documentation
- `docs/content.md`: the light-passport lifecycle (created → trimmed at
  6 h → deleted at TTL), the config key, the amended T-166 contract
  ("passport stays forever" → "stays for findings; non-findings expire").
- `docs/schema.md` / `docs/configuration.md` / config example if they
  cover the table or the key; a journal note in `docs/operations.md`.

## 3. Acceptance criteria

1. `make test` + `make test-race` + `make lint` + `make test-integration`
   green.
2. The lifecycle integration test (R4) is green.
3. Findings protection proven by a test.
4. The config key works; default 10 days.
5. Docs updated.

## 4. Out of scope

- The separate "dead-status drain" (research variant A) — the TTL covers
  that class automatically (ghost rows are older than the TTL).
- Domain status changes, recheck policy, the laravel tables contract.
- Physical file shrink (`pg_repack`/`VACUUM FULL`) — space returns to FSM
  pages for reuse; a separate ops decision if ever needed.

## 5. Deployment plan (for the main session/operator)

- Deploy any time (a background pass); the FIRST pass is a large catch-up
  (~14M rows, ~10 GB) — expect hours of batched draining; watch
  `rows_deleted_total` and table size; steady state after: ~1–1.5 GB
  (10 days × ~192k rows/day × ~700 B) instead of endless growth; disk
  frees ~8–9 GB.
- Verification: metrics grow, `pg_total_relation_size` drops, `domain_info`
  of an old non-finding → `results: []`, findings intact, no new WARN
  series, no OOM pattern (scangate + batches).

## 6. References

- Research facts: subagent report 12.11.2026 (this task file embeds the
  essentials); disk capacity study — chat/prod-reports 12.11.2026.
- Code: `internal/httpworker/record.go` (row lifecycle),
  `internal/ingest/cleanup.go` (T-020 pattern), `internal/content/
  retention*.go` + `retentiondrain.go` + `retentioncursor` (T-274 drain
  toolkit: batches, cursors, backoff, scangate, metrics),
  `migrations/0013` (partial index precedent), `internal/model/enums.go`.
- Related: T-166 (retention contract — amended here for non-findings by
  the owner's decision), T-273/T-274 (drain-resilience lessons).

# Task T-274: retention restart resilience — a select cap config key + a persistent drain cursor

Version: 1.0. Stage 44. Priority: P1 (a direct consequence of the P0 hotfix
T-273). Sources: the T-273 investigation (options B and C —
[`prod-reports/2026-11-10-005208-t271-research.md`](../../prod-reports/2026-11-10-005208-t271-research.md)),
the production facts of 10–11.11.2026 ([the drain outcome](../../prod-reports/2026-11-11-000550-final-drain.md),
the runbook [`docs/operations.md`](../../docs/operations.md) §4.6 and the §5 log).

## 1. Problem

- The retention drain cursor lives in the http-worker process memory; every
  restart resets it to zero — “catch-up circle: process start…”
  (`internal/content/retention.go:540-545`).
- The first pass after a restart must walk the “clean prefix” of the
  subdomain_results history (the “anything to purge” predicate is not in the
  index — a heap visit of every row): production measurements —
  **~60 min when idle** (pass 4, the night of 10–11.11, right at the cap),
  **>60 min under full load** (passes 1–3 did not fit into 3600 s,
  `retention_errors_total` = 3).
- `SelectTimeout` is a code constant of 3600 s (`DefaultRetentionSelectTimeout`,
  `internal/content/retention.go:152`); there is no config knob — the cap
  cannot be changed without a deploy.
- Restarts happen more often than it seemed: every deploy, plus unplanned
  ones (an external power-cycle of the VM on 11.11 11:03 — the hypervisor/hosting
  panel, ACPI-poweroff + start; a potential process OOM kill, hosting
  maintenance). After each one — an hour-long walk over the prefix with a
  timeout lottery; losing it means retention stands still until a quiet
  window (the incident, a day of triage on 10–11.11).

## 2. Solution (option B + the C cursor from the investigation; NO partial index)

### R1. Select cap config keys

- `content.retention_select_timeout` — a new key in the `retention_*`
  family (`internal/config/config.go:554-569`), **default 7200 s**
  (double the headroom for walking the prefix under load; currently a
  3600 s constant).
- `render.late_select_timeout` — a similar knob for the late republisher
  (`internal/httpworker/renderlate.go:109-121`), the default stays 300 s
  (the republisher's select is adaptive and fast — the knob is just in case).
- Wiring in `cmd/http-worker/main.go`: the config values into
  `RetentionOptions.SelectTimeout` / `LateOptions.SelectTimeout`; an empty
  or zero value — the default (the current `withDefaults` semantics is
  preserved).

### R2. The persistent retention drain cursor

- The cursor (`checked_at` + `retentionKey{zone_id, domain_id, subdomain_id,
  variant}`) is saved to the DB **in the transaction committing each batch**;
  the frequency — hundreds of writes per pass (155 per 16 h) — the load is
  negligible.
- Storage: a **migration** — a compact state table (one row,
  upsert). NOT `crawler_counters` (a composite key does not fit into the
  numeric value). The concrete design is up to the implementer; criteria:
  atomicity with the batch commit, no locks on the hot path.
- Process start: the cursor is read from the DB; absence/invalidity
  (for example, a timestamp in the future) — the current behavior (zero →
  a full catch-up).
- **Important**: the existing causes of a forced reset to a catch-up circle
  (clearing the render debt, overflowing the retry list, the fallback day —
  the comment at `retention.go:540-545`) must also clean the persistent
  state, otherwise the reset will not work.
- The semantics of the interlocks and skips is unchanged.

### R3. Tests

- Config wiring: the key is parsed and applied; absence — the default.
- Cursor: a round-trip (a batch commit → state saved); **the main test is
  “restart”** (a new loop reads the state) → the drain continues from the
  cursor, not from zero; a forced reset cleans the state; a malformed state →
  start from zero.
- The T-273 test “a catch-up from the zero cursor fits into SelectTimeout”
  stays green (behavior without saved state is unchanged).

### R4. Documentation

- `docs/content.md`: the new keys, the persistent cursor, restart behavior
  (the penalty is lifted).
- `docs/operations.md` §4.6: remove the warning “every http-worker restart
  = a new catch-up” (after T-274 a restart continues from the cursor); the
  §5 log — a note on closing the risk.
- `deploy.md` §5 — if config keys are listed there, extend it.

## 3. Acceptance criteria

1. `make test` + `make test-race` (content and httpworker are in RACE_PKGS)
   + `make lint` — green; `make test-integration` — **mandatory**
   (a migration is added).
2. The “restart continues from the persistent cursor” test — green.
3. The config keys work (a wiring test); the retention default — 7200 s.
4. Documentation updated.

## 4. Out of scope

- A partial index on the “anything to purge” predicate (the C index) — a
  separate decision after measuring the write amplification (breaks HOT,
  rejected in T-201/T-104). If the persistent cursor + 7200 s proves
  enough — not needed at all.
- F2 (the PG memory budget), q:dead triage — separate tasks.

## 5. Deploy plan (for the main session/operator)

- The first T-274 deploy itself performs one last full catch-up (there is
  no saved cursor yet): deploy in a quiet window OR under supervision —
  the 7200 s cap gives good odds even under load (the idle walk ~60 min,
  up to ~2 h under full load).
- Verification: the first pass completes (INFO “content retention pass”
  with the totals); the **next** http-worker restart (a regular deploy) —
  the drain continues from the cursor: passes are short, the clean prefix
  is not walked, `retention_errors_total` does not grow.

## 6. References

- Investigation and options: `prod-reports/2026-11-10-005208-t271-research.md`
- Window precedent: `docs/operations.md` §4.6, the §5 log;
  `prod-reports/2026-11-11-000550-final-drain.md`
- Code: `internal/content/retention.go` (the constant :152, options :407–464,
  the catch-up reset :540–545, retentionSelect :605+), `internal/content/retentiondrain.go`,
  `internal/httpworker/renderlate.go` (:109–121, :296), `internal/config/config.go`
  (:554–569), wiring `cmd/http-worker/main.go`
- History: T-271 (`15a4e41`, the regression), T-273 (`05e2ed3`, the hotfix)

## 7. Implementation (implementer notes)

- **State table** — `retention_drain_cursor` (migration 000020):
  one row (`id smallint PK CHECK (id = 1)`), the cursor columns
  `(checked_at, zone_id, domain_id, subdomain_id, variant)` + `updated_at`
  for diagnostics. Not `crawler_counters` (a composite key does not fit into
  a bigint); a separate table instead of generic key-value — typed
  columns without jsonb.
- **Atomicity with the batch**: the upsert runs in the `retentionPurge` tx
  as the last statement before Commit — the position never runs ahead of the
  committed purge; a batch failure (including a failure of the upsert
  itself) rolls everything back. A batch with rows deferred by the
  protection does NOT move the DB position (`len(batch)==len(selected)`,
  the P1 review): the retry list is non-persistent, its loss on a restart is
  compensated by re-reading the protection window; the in-memory cursor
  moves as before (the probe returns keys within the process lifetime).
  The retry probe and the targeted drain of a single domain (T-187) do not
  move the cursor (`retentionPurge(..., nil)`).
- **Forced circles** (clearing the render debt, overflowing the retry list,
  the fallback day) delete the row with a DELETE at PLAN TIME
  (`planCircle`, the P2 review): a restart in the window between the plan
  and its application (≤ the interval) falls into a full catch-up from
  zero; position saving is suppressed until the circle is applied (otherwise
  the batches of the same pass would resurrect the row). A failure to
  reflect the plan — Warn without a pass error (a degradation to continuing
  from the old position — costlier, nothing lost).
- **Loop start** (`RetentionLoop`): reading `retentionLoadCursor`; a missing
  row/a zero timestamp — a zero cursor (the old behavior); a timestamp in
  the future (a 1 min tolerance for clock skew) — Warn + start from zero;
  a read failure — Warn + start from zero. No table (42P01, a DB before
  000020) — Info once per process, persistence switches off (including on
  the first upsert failure mid-run) — a degradation to the T-273 behavior,
  not a breakage (the errRetentionNoCounterTable style).
- **RetentionOnce** (the runbooks T-167 path A) neither reads nor writes the
  persistent state: the state owner is the http-worker loop; an isolated
  drain must not move/roll back its position (an idempotent purge from zero
  is safe even without state).
- **Config**: `content.retention_select_timeout` (a default of 2 h in
  `Default()` and in the `DefaultRetentionSelectTimeout` constant
  3600→7200 s; duplicating the default — an established pattern for the
  retention keys, config does not import service packages),
  `render.late_select_timeout` (default 300 s). Both: 0 — the code default
  (`withDefaults`), the validation corridor 1m..24h; env
  `CRAWLER_CONTENT_RETENTION_SELECT_TIMEOUT` /
  `CRAWLER_RENDER_LATE_SELECT_TIMEOUT`.
- **Tests**: unit — a round-trip in the batch tx, an upsert failure rolls
  back the batch, the position does not move past the deferred rows (a
  full/partial deferral + recovery “by restart”), a circle plan cleans
  the row at plan time and suppresses saves (a list overflow), a failure to
  reflect the plan — Warn, a circle resets the row and rewrites it with
  circle batches, THE MAIN “restart continues from the cursor”
  (RetentionLoop #2 starts its first select from the saved position),
  a malformed state → zero, 42P01 → running without persistence
  (at start and mid-run), RetentionOnce does not touch the state;
  the integration `TestContent_RetentionCursorRestart` (live PG: loop 1
  → the cursor row → “restart” → a record behind the cursor is not visited,
  ahead of it — purged; RetentionOnce picks up what is behind — no loss) +
  the one-row contract (CHECK id=1).

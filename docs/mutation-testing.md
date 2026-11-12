# Mutation testing

Mutation testing checks the **quality of the tests**, not the code:
small errors are introduced into the source one at a time (mutations —
inverted conditions, cut-out branches, changed operators and constants),
and after each one the package's tests are run. A mutant the tests
**did not notice** ("survived") is a piece of code not pinned down by
assertions. The share of killed mutants — the mutation score — is the
metric of the test suite's rigor.

In this project it is an auxiliary, heavy check: it is not part of the
CI of every push; it is run by the weekly workflow and manually.

## 1. The tool

[`go-mutesting`](https://github.com/zimmski/go-mutesting) in the live
fork [avito-tech/go-mutesting](https://github.com/avito-tech/go-mutesting):
the zimmski upstream has not been updated since 2021 and panics in
`go/types` under modern toolchains; the fork is the same tool (the same
mutators and the built-in exec via `go test`), extended with YAML config,
mutation disabling annotations, JSON/HTML reports and an extra set of
mutators (arithmetic/loop/numbers on top of branch/expression/statement).

The version is pinned in `.env` (`GO_MUTESTING_VERSION`, installed into
the dev image `deploy/docker/Dockerfile.go`); the chosen revision is the
latest one that does not require a toolchain newer than the project's
(`go 1.25.5` with the project Go 1.26.x: `go install` does not pull an
extra toolchain).

## 2. Running

```bash
make mutate                                    # the default scope
make mutate PKGS='./internal/httpfetch/...'    # a targeted package
make mutate PKGS='./internal/queue/... ./internal/idn/...'
make mutate MUTATION_MIN_SCORE=0.7             # fail if the score < 0.7
make mutate MUTATE_TIMEOUT=120                 # the go test timeout per mutant, s
```

Everything runs in the `tools` container (like `make test`); Go is not
needed on the host. The variables:

| Variable | Meaning | Default |
|---|---|---|
| `PKGS` | the scope — local `./` package patterns separated by spaces | the "logical core", see below |
| `MUTATION_MIN_SCORE` | the score threshold 0..1, below it — exit 1 | `0` (the report only) |
| `MUTATE_TIMEOUT` | the `go test` timeout for a single mutant, s | `60` |

Important: `go-mutesting` resolves targets with GOPATH-era logic — the
patterns must be local (`./internal/...`); the module's import paths
(`crawler/internal/...`) do not work.

### The default scope

The `PKGS` default is the business-logic packages with hermetic and fast
unit tests: `./internal/{analyzer,dashboard,idn,model,monitor,queue,
render,rules,scheduler,status}/...`. A full-run measurement (2026-10,
c62f4cd): **~2.5 h, 2632 mutants, score 1.000** (0 survivors); per
package:

| Package | Killed | Survived | Score |
|---|---:|---:|---:|
| `analyzer` | 279 | 0 | 1.00 |
| `dashboard` | 49 | 0 | 1.00 |
| `dashboard/auth` | 37 | 0 | 1.00 |
| `idn` | 38 | 0 | 1.00 |
| `model` | 164 | 0 | 1.00 |
| `monitor` | 394 | 0 | 1.00 |
| `queue` | 392 | 0 | 1.00 |
| `render` | 390 | 0 | 1.00 |
| `rules` | 260 | 0 | 1.00 |
| `scheduler` | 460 | 0 | 1.00 |
| `status` | 169 | 0 | 1.00 |
| **total** | **2632** | **0** | **1.000** |

The path to 1.000: the baseline 71e1da3 (2877 mutants, 941 survivors,
score 0.673) was worked through by the tasks T-M01…T-M13 — 714 killed by
new tests, 227 suppressed by annotations (another 18 disappeared along
the way — annotation granularity, §4). The T-M14 control full run found
two survivors (score 0.9992), both closed: `idn.go.8` — a direct
contract test of `isNumericZone`; `wait.go.6` — a deterministic subtest
of the event decoding error (the kill via ctx cancellation is in
principle a chromedp teardown race; the negative-check window was
widened to 1 s as a guard).

Not in the default scope; checked spot-wise as needed:

- `config`, `web` — hundreds of mutants over yaml binding and i18n
  plumbing at low signal;
- heavy in run time (`httpfetch`, `content`, `watch`, `dnsclient`,
  `httpworker`, `dnsworker`, `ingest`, `migrate`, `db`, `service`,
  `renderworker`, `metrics`) — a mutation run multiplies their tests'
  time by the number of mutants (for `httpfetch` that is hours).

### How it works

`deploy/docker/scripts/mutate.sh` (called by the make target):

1. Copies the sources into an isolated directory inside the container
   (`/tmp/mutation-src`, without `.git`/`var`/`tmp`). The copy is needed
   because go-mutesting substitutes mutant files **right in the tree**,
   and the working tree is mounted into the service containers with
   hot-reload.
2. Runs `go-mutesting` with the `test/mutation/go-mutesting.yml` config
   and the built-in exec: for each mutant — a `go test` of its package
   with the `MUTATE_TIMEOUT` timeout (the `gomod`/`gocache` caches are
   shared — dependency compilation is not repeated).
3. Unloads the reports into `var/mutation/` and compares the final score
   with `MUTATION_MIN_SCORE` (go-mutesting itself always exits with code
   0; the threshold is gated by the wrapper).

## 3. The reports

`make mutate` writes to `var/mutation/` (the directory does not get into
git):

- `mutate.log` — the full output: per mutant a `PASS` line (killed) /
  `FAIL` (survived, with a diff patch) / `SKIP` (did not compile) and
  the final line `The mutation score is ...`;
- `go-mutesting-report.html` — an interactive report on the surviving
  mutants (diff, file, mutator) — the main triage tool;
- `report.json` — the same data in a machine-readable form.

The output terminology is counter-intuitive: `PASS` — good (the tests
killed the mutant); `FAIL` — a surviving mutant, a hole in the tests.

## 4. False positives

Some mutants are meaningless (optimizations, logging, format minutiae).
They are not suppressed by tests but excluded — in priority order:

1. **Annotations in the code** (the recommended way; lives next to the
   code):

   ```go
   // mutator-disable-func            — do not mutate the whole function
   // mutator-disable-next-line *     — do not mutate the next line
   // mutator-disable-next-line branch/if, expression/comparison
   // mutator-disable-regexp `^log\.` *  — lines by a regular expression
   ```

2. The **checksum blacklist** — `test/mutation/blacklist.txt` (the MD5
   from the `FAIL "..." with checksum <md5>` line in `mutate.log`, one
   per line; the fork's parser requires exactly 32 hex characters —
   comments in the file are not allowed, the group justifications are in
   this file, below); it is connected automatically if the file is
   non-empty. The way is fragile: a source edit changes the checksums —
   hence only for mutants that cannot be closed by an annotation.
   Blacklisted mutants count as duplicates and enter neither the
   numerator nor the denominator of the score
   (`Msi = (killed + error + skipped) / (killed + escaped + error +
   skipped)`, the fork's internal/models) — suppression raises the score
   exactly like a test kill, so a group must be provably
   equivalent/unobservable, not "could not find how to kill it".

   The blacklist contents (the triage of the survivors of the
   2026-11-09 CI run, half A — 96 checksums), by group:

   | Group | Where | Justification |
   |---|---|---|
   | `parsefloat-bitsize` | load.go, psi.go, renders.go | `strconv.ParseFloat` with bitSize ≠ 32 behaves as 64 (the 32/other branchings in strconv) — the bitSize 63/65/9 mutants are equivalent |
   | `log:side-effect`, `*-log` (loop/drain/info/warn/debug) | monitor, analyzer | the log calls (log.Info/Warn/Debug and their `ctx.Err() == nil` guards): the only effect is a journal line, the behavior contract does not change; suppressed in the same sense as the `mutator-disable-regexp log\.…` annotations in nearstats/historystats |
   | `make-cap` | zonestats.go, renders.go | the `make(...)` capacity — a performance hint: the length values/the contents do not change |
   | `load:guest-break` | load.go L261 | `break → continue` in the ticks loop: after 8 fields both forms write nothing (continue just adds effect-less iterations) |
   | `load:closeq-open` | load.go | `close_ <= open → <`: close_ == open is unreachable (different bracket characters) |
   | `load:read-max-eq` | load.go | the maximum over groups with an equal entries-read — the same value |
   | `load:busy-clamp-eq`, `load:round-neg-eq` | load.go | zero clamps: at 0 both branches give 0; for `v < -1` — the int(x+0.5) truncation to 0 on [-1, 0.5) |
   | `load:procwin-guard` | load.go | the relaxations of `d > 0`/`perCore > 0`/`agg.Ticks <= prev` are covered by the internal `perCore > 0` guard and the topByCPU noise threshold: a 0 share is filtered out, the report is the same |
   | `load:rate-window-eq` | load.go | `window == 0` between passes is unreachable (the ticker ≥ the interval); an equal/fallen Read gives a 0/negative rate — the rate line is not printed in any of the variants |
   | `load:top-trunc-eq` | load.go | `len(items) >= topProcs` at equality — a slice of the same length |
   | `zonestats:init-guard` | zonestats.go init() | a guard against the legend's desync under future edits: on consistent code it is unexecutable (see the neighboring precedent annotation) |
   | `zonestats:drift-int`, `zonestats:absdiff-eq` | zonestats.go | `d < 0 → <= 0` and `a > b → >= b` on int64: at the equality point both branches give 0 |
   | `zonestats:sort-uniq` | zonestats.go | the `<=` comparator: the zoneIDs keys are unique (a map), equality is unreachable |
   | `zonestats:took-eq` | zonestats.go | `took >= interval` differs only at the exact equality of the measured duration to the interval — non-deterministic/unreachable in a test |
   | `zonestats:globals-prezero` | zonestats.go | the pre-zeroing of statuses before `+=`: += over a missing key reads 0 — the map is indistinguishable |
   | `zonestats:returning-missing` | zonestats.go L1123 | the "RETURNING did not return a zone" branch is unreachable: writeZonePositions requires len(out) == len(ids) and fails the pass earlier |
   | `slowqueries:phrase-eq` | slowqueries.go | `n%100 <= 10`/`> 20` differ only at n%100==10/20, where n%10==0 — the first `n%10 >= 2` clause already cuts those off; `n%10 >= 1` is covered by case 1 (`n%10 == 1 ∧ n%100 != 11`) — proven by enumeration over 0..300 |
   | `renders:trunc-hash` | renders.go | the `#`-tail truncation: the value is the first token of rest in both variants (defensive code) |
   | `renders:labels-end-neg` | renders.go | `end <= 0`: end ≥ start+1 ≥ 1, zero is unreachable |
   | `renders:escape-boundary-eq` | renders.go | `i+1 < len(s) → i+0/i-1/true` (the last character's boundary): both forms end the loop without finding a bracket |
   | `renders:rateof-eq` | renders.go | `cur <= prev → <`: at equality both give 0 |
   | `renders:dt-clamp-eq` | renders.go | the dt clamp is covered by rateOf's own guard (`dt <= 0 → 0`) — a negative window gives 0 in both paths |
   | `renders:body-close`, `renders:ctx-cancel` | renders.go | the body Close/the context cancellation after reading — without an observable effect in the unit surface |
   | `idn:unreachable-guard` | idn.go | `i == 0` (a name with a leading dot) is excluded by normalizeASCII; the neighboring guard branch is already suppressed by an annotation with the same justification |
   | `worker:drain-log` | worker.go | `stats.Rows > 0 → >= 0/> -1/> 1`: the branch's body is only log.Info |

   The blacklist contents of part B (the triage of the survivors of the
   2026-11-10 CI run, the `mutate-b` job queue+rules+scheduler+status:
   286 survivors → 153 closed by the packages' `*_triage_test.go`
   tests, 133 checksums below), by group:

   | Group | Where | Justification |
   |---|---|---|
   | `consumers:loop-log` | consumers.go | the cleanup loop's logging (Info/Warn, the gathering of group pairs, the `err != nil && ctx.Err() == nil` and `removed > 0` guards) — the only effect is a journal line (the A precedent `log:side-effect`) |
   | `redisq:publish-retry-pause-timing` | redisq.go | the exponential pause of the XADD retries (`<< min(attempt-1, 5)` and the boundary shifts) — a timing contract; the fact/the number of retries and the stop on success/another's error are closed by tests |
   | `redisq:publish-retry-guard-eq` | redisq.go | the equivalent guards of the retry loop: `err != nil && infraCause` (infraCause(nil) = false), the header's `ctx.Err()` at body-break, `break → continue` on a non-infra error — the exit by the same path without an extra XADD |
   | `recover:refund-timeout-timing`, `reliab:classify-timeout-timing` | recover.go, reliab.go | the ctx budgets of refunds/classification (5 s) — timing constants: the unit surface's fakes are not sensitive to the deadline, the commands' behavior is closed by tests |
   | `recover:refund-log` | recover.go | the log branches of attempt refunds (Info/Warn and the claim's `ctx.Err() == nil` guard) |
   | `recover:unclassified-guard-eq` | recover.go | `len(unclassified) > 0 → >= 0/> -1`: an empty tail — only the journal (a Refund* with an empty batch is transparent, there are no commands) |
   | `recover:minidle-boundary-eq` | recover.go | `elapsed < MinIdle → <=`: the equality of the measured interval to the threshold is deterministically unreachable (the A precedent `zonestats:took-eq`) |
   | `recover:claim-minidle-eq` | recover.go | minIdle 0 → −1 is indistinguishable — Claim normalizes non-positive values (the T-M04 precedent comment in the code itself) |
   | `reliab:refund-clamp-eq` | reliab.go | the clamp `n < 0 → <= 0/< 1`: at the point 0 both branches give 0 |
   | `reliab:processing-attempt-arg-eq` | reliab.go | the Processing attempt argument is consumed by a wrapper closure with `_` — the value is not observable from the public surface |
   | `reliab:probe-value-eq` | reliab.go | the record's SET probe value is not read — the fact of writing the key is what matters |
   | `trim:make-cap` | trim.go | the `make(...)-1` capacity — a performance hint (the A group `make-cap`) |
   | `trim:loop-log-bookkeeping` | trim.go | the loop's journal and the warned map ("a Warn once per episode", stuckSet, the delete walk) — only warnings |
   | `trim:cand-empty-guard-eq` | trim.go | `continue` on an empty cand: `streamIDGreater(safe, "")` = false (splitStreamID of an empty string crashes) — the safe assignment does not change the point |
   | `infra:nil-guard-eq`, `infra:ctx-canceled-eq` | infra.go | a nil error and context.Canceled are not matched by any subsequent classifier check — the early exit is equivalent to the full pass |
   | `scheduler:err-count-unobs` | stalerequeue, retry, lease, leasenull | `return ±1, err` on errors: the value is read by no one — all callers check err first and discard the result |
   | `scheduler:drain-total-log` | stalerequeue, lease | the drains' totals (`total +=/-=/=`, `total > 0`) — only the journal |
   | `stalerequeue:guard-wrap-eq`, `retry:guard-wrap-eq` | stalerequeue.go, retry.go | suppressing the `rows.Err()` return after `!Next` is caught by the next guard return of the same step (a Scan after the rows are exhausted must fail / "an empty aggregate answer") — the only difference is the wrapper's text |
   | `retry:finalize-keys-eq` | retry.go | the finalization's RETURNING keys are not read — only the len (the episode days are consumed only by the retry assignment branch) |
   | `retry:empty-candidates-eq` | retry.go | an empty candidate selection: the early exit is equivalent to the full pass (both decision transactions are skipped with empty batches) |
   | `backlog:took-boundary-eq` | backlog.go | `took > Interval → >=`: the equality of the measured duration to the interval is non-deterministic |
   | `enqueue:panic-guard` | enqueue.go | the defensive panic on an enqueueUpdate construction error is unreachable without a construction bug (the A precedent `zonestats:init-guard`) |
   | `cache:valid-guard-eq` | cache.go | `e.valid &&` — a dead guard: valid=false only for an unwritten record with a zero expiresAt, and `now.Before(zero)` = false |
   Continuation — the part C triage (render, the 2026-11-10 CI run,
   score 0.916: 54 survivors — 43 closed by the
   `internal/render/triage_test.go` tests and by extending the
   TestWhatwgParseIPv4/TestOptionsValidate tables in render_test.go;
   11 equivalent ones — the blacklist):

   | Group | Where | Justification |
   |---|---|---|
   | `render:dns-internal-break` | block.go, hostInternal | `break → continue` in the AddrIsInternal loop: internal is already true, the remaining iterations are effect-less reads, the verdict is the same |
   | `render:chan-cap-init` | browser.go, initBrowserSession | the `make(chan result, 1)` capacity: 2 — strictly without effect (a single send); 0 — blocks only the goroutine of the abandoned timer path, no observable behavior |
   | `render:case-identical` | render.go, whatwgIPv4Number | the branch/case mutation yields a byte-for-byte identical source (an empty diff, a degenerate mutant) |
   | `render:whatwg-redundant-guard` | render.go, whatwgParseIPv4 | the early return of "does not end with a number" is duplicated by the check of the same part in the loop below — both paths give false |
   | `render:whatwg-empty-default` | render.go, whatwgIPv4Number | `return 0/1,false` for an empty string: ParseUint("") already returns (0,false), and v at ok=false is not read by any call |
   | `render:validate-host-guard` | render.go, validateTaskURLInternal | `u.Host == "" → false`: Hostname() of an empty Host is empty too, renderHostInternal("") = false — both paths give nil |
   | `render:whatwg-split-nonempty` | render.go, whatwgParseIPv4 | `len(parts) == 0` is unreachable (strings.Split always yields ≥ 1 part): the operand mutants to false and −1 change dead code |
   | `render:octal-leading-zero` | render.go, whatwgIPv4Number | `s[1:] → s[0:]` in the octal ParseUint: the leading zero adds 0·8^k — the value and the set of valid digits are the same |

   The post-triage of the remaining survivors of the A/B jobs of CI run
   42842802147 (2026-11-10, A 0.993/B 0.993 at the 0.98 threshold — the
   tails after the T-270/T-271/T-273 edits and the first wave's misses:
   35 survivors — 19 closed by the `*_triage_test.go` tests
   (load/renders/zonestats/redisuptime/renderdebt/consumers/lease/
   leasenull), 16 checksums below), by group:

   | Group | Where | Justification |
   |---|---|---|
   | `worker:drain-disabled-log` | analyzer/worker.go, drainUnfound | the cutting of the "the drain is disabled" Info line (retention 0 / a nil seam): the event is already observable via the `met.Unfound()` metric earlier in the code; the cut statement is only the journal (the A precedent `log:side-effect`/`worker:drain-log`) |
   | `renders:dt-clamp-zero-eq` | dashboard/renders.go, observe | `dt < 0 → dt < 1`: Duration is integer nanoseconds; at the new point 0 the clamp 0→0 is a no-op (negatives and >0 are the same in both forms) |
   | `load:readdir-fail-unreach` | load.go, Load | the failure of the root listing while its files are read successfully (loadavg/stat/meminfo read) is reachable only via EACCES/EMFILE — the unit surface never reaches it (the tests run in a root container, the fds are opened sequentially); the production behavior — a pass error like the neighboring sources |
   | `notify:disabled-guard-eq` | notify.go, OnPass | the early exit at script=="": the state records are unobservable (script does not change after the constructor, there are no other map readers); the other effects — the journal (the A group `log:side-effect`) |
   | `notify:notmeasured-guard-eq` | notify.go, OnPass | `a.Status == AlertStatusNotMeasured &&`: there are exactly three statuses (ok/firing/not_measured); the first two are closed by the switch branches above — only NotMeasured reaches the third branch, the operand is always true |
   | `unitevents:ctx-guard-eq` | unitevents.go, UnitEvents | the first ctx.Err() check is duplicated by the second right after context.WithTimeout: the parent's cancellation is visible in the derived ctx too — both forms return the same err |
   | `unitevents:nrestarts-break-eq` | unitevents.go, parseSystemctlShow | `continue → break` inside a switch case branch: break interrupts the switch, not the loop, and the loop body after the switch is empty — both forms move on to the next line |
   | `zonestats:globals-prezero-retri` | zonestats.go, globalCounters | again (the T-271/T-273 line shifts changed the `zonestats:globals-prezero` group's checksums): the pre-zeroing of statuses before `+=` — the consumers read only the fixed keys (CounterNames/driftCounterNames), a missing key = 0, the key's presence is unobservable |
   | `trim:err-count-unobs` | trim.go, trimStream (4 sites × ±1) | `return 0 → ±1` on the error paths (an unknown stream, groups, pending, XTRIM): TrimOnce discards n at err (continue), TrimLoop reads only stuck — the value is read by no one (the B precedent `scheduler:err-count-unobs`) |

The `test/mutation/go-mutesting.yml` config additionally cuts off files
without a companion `*_test.go` and files whose tests sit under a build
tag — a mutant cannot be killed without executable tests.

The annotation granularity is the whole line: `next-line` and `regexp`
act on all mutations of the listed mutators on the line, not on a
specific operand. On a line with several mutations of different value,
an annotation of an equivalent one can incidentally suppress killable
ones too (the example — T-M02: `if a || b` with the equivalent mutant
`b → false` also suppresses the killable `a → false`; an index literal
next to an indivisible `bitSize`). The complete elimination — splitting
the expressions across lines — is a production code edit: it is not done
without a bug; the trade-off is recorded in the task's commit and
removed at the next substantive edit of the file.

## 5. CI

[`.github/workflows/mutation.yml`](../.github/workflows/mutation.yml):
a weekly run over `master` (Sunday 20:00 MSK) + a manual run (inputs:
`pkgs`, `min_score`, `timeout`).

The full scope does not fit into the hosted job's platform cap of 6 h:
the local ~2.5 h slows down severalfold on a CI runner; the sequential
full distance per the measurements is ~9.5–11.5 h. The split history: a
single job hit the timeout twice (240 min; a manual stop at ~4.6 h in
the 7th package of 10); the next split into two halves by weight
(LOC 52/48) was refuted by the fact of run 42595221462 of 2026-11-09:
half A passed the mutation step in ~2h15m (the quality gate fell though
— score 0.758; the survivors' triage is a separate task), while half B
(then queue+render+rules+scheduler+status) was cut by the step timeout
of 340 min — queue took ~77 min (770 mutants, ~6 s/mutant), but render
goes at ~29 s/mutant (in 265 min — only ~70–85% of the package, i.e.
the full package ~5.2–6.3 h); rules/scheduler/status did not start; the
full B set is ≥7 h (queue + render + the unstarted packages). The
conclusion: balancing must be by time, not by the number of mutants or
LOC — the time is proportional to "mutants × the package's test time",
and the pace is set by the package's test suite, which differs
severalfold (queue ~6 s/mutant vs render's ~29 s/mutant; the mutant
numbers in the §2 table are as of 2026-10, the code has grown since:
queue is already 770 per run count vs 392). Therefore the full scope is
split into three parallel jobs — the parts:

| Job | Packages | Time on CI |
|---|---|---|
| `mutate-a` | analyzer, dashboard (incl. auth), idn, model, monitor | the 2026-11-09 measurement: ~2h15m |
| `mutate-b` | queue, rules, scheduler, status | an estimate of ~2–3 h: queue is measured (77 min); rules/scheduler/status were not run in CI — presumably fast suites, to be refined by the first run |
| `mutate-c` | render | ~5.2–6.3 h per the pace measurement of ~29 s/mutant (544 mutants in 265 min ≈ 70–85% of the package) |

Render is separated into its own job not by the number of mutants but by
time: the slow test suite gives ~29 s/mutant — five times slower than
queue — and in one bundle with queue it did not fit into the 6 h cap
(the measurement above: a cut at ~70–85% of render with
rules/scheduler/status not started). The pessimistic edge of the C
estimate (~6.3 h) touches the step cap of 340 min (a cut at ~90% of the
package) — even then the run is more informative than the former half B,
and the always() upload will keep the reports; the further lever is
speeding up the render test suite (outside the job-balancing task).

Each job is independent: it builds the tools image, runs its part and
unloads the reports into its own artifact
(`mutation-report-{a,b,c}-<run_id>`); `timeout-minutes: 360` per job is
the platform maximum, while the mutation step itself has a cap of
340 min: when cut by the step cap, the always() upload runs as normal; a
job killed by the job cap does not guarantee the upload.

A manual run: an empty `pkgs` — all three parts (the full scope); a
non-empty one — a targeted run of the single job `mutate-a` with that
set (do not put the full list into it — a single job will be cut by the
6 h cap; the set is validated against a character whitelist — the value
lands in the make recipe inside single quotes); `mutate-b` and
`mutate-c` are skipped. `min_score`/`timeout` apply to all three jobs.

The default threshold is `0.98`, gating **each part separately**: this
is stricter than a global weighted threshold — the latter would allow a
skew (a perfect part would compensate a failing one below 0.98), while a
per-part gate localizes the holes in the part's packages; a deliberate
decision. The margin of ~0.02 (tens of mutants per part) covers
timing-sensitive tests (the T-M14 control run once showed a flaky
survivor `wait.go.6`: the kill via ctx cancellation — a chromedp
teardown race; it was closed by a deterministic subtest, but the
threshold margin was left deliberately); below the threshold the weekly
check turns red but does not block development (it is not invoked in the
PR workflow).

The threshold is raised deliberately: surviving mutants are fixed by new
tests or explicitly suppressed (§4), and only then is the bar raised to
the achieved level minus the margin (~0.02).

## 6. When to run

- before substantial edits of a package in the default scope —
  targeted: `make mutate PKGS='./internal/<pkg>/...'`;
- when the weekly run fails — triage the HTML reports of all the parts
  (the artifacts `mutation-report-{a,b,c}-*`), close the holes with
  tests or suppress the false ones, raise the threshold;
- with a full run — occasionally, to verify the overall picture.

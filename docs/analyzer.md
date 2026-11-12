# The Laravel analyzer: the rules engine, the worker, and re-analysis (spec 5.8, §9)

The `internal/rules` package — the passive analysis of
already-downloaded content (spec 5.8: "only over the already
downloaded content, cookies, and headers; no additional requests to
the sites are made"). The YAML ruleset loading, the `SIGHUP` reload,
and `rules_version` — T-068 (`rules.go`/`reload.go`); the Laravel
ruleset — T-073 (`rules/laravel.yaml`); the score and version-range
computation (noisy-OR, the intersection of `[ver_min, ver_max]`,
a conflict) — T-070 (`score.go`). This document — the matching engine
over targets and the hit evidence (T-069), the stage worker and the
writing of `laravel_findings` (`internal/analyzer`, T-071), the
domain-level aggregation into `laravel_domains` (T-072,
`aggregate.go`), the point drain of a domain's content when a find is
removed by re-analysis (T-187, same place), and the re-analysis under
a stale rules version (T-074, `internal/analyzer/reanalyze.go`).

## The engine's input

`rules.Page` — what is stored about a page in `subdomain_results` and
the content storage (spec 5.7):

| Field | Source | Contract |
|---|---|---|
| `HTML []byte` | the content storage: the raw HTTP response or the final DOM after the render (T-067) | the DOM — Chrome's textual HTML serializer, no transformations needed (T-064) |
| `Cookies []Cookie` | the jsonb `subdomain_results.cookies` (the HTTP stage T-058 / the render T-064) | `{name, value}` — the rule targets; the attributes are not needed by the engine |
| `Headers map[string][]string` | the jsonb `subdomain_results.headers` | canonical MIME header names (`http.Header`), the order of the values is preserved |

## Targets and matching modes

A rule's target (`rule.target`) sets where the trait is searched for;
the mode — the `match` field (exact match) or `regex` (a regular
expression):

| target | match | regex | Traversal |
|---|---|---|---|
| `cookie_name` | yes | yes | the cookies in recording order |
| `cookie_value` | yes | yes | the cookies in recording order |
| `header_name` | yes | yes | the header names **sorted** |
| `header_value` | yes | yes | the names sorted, the values in slice order |
| `html` | — | yes | the first matched fragment |
| `meta` | — | yes | the meta tags in HTML order |

`html`/`meta` accept only `regex` (spec 5.8: "a regular expression
over the HTML"; an exact match against the whole document makes no
sense) — a rule with `match` over these targets never fires. The
**first** match fires — one piece of evidence per rule.

The traversal order is deterministic: cookies — the recording order
(the HTTP stage preserves the response order, the render sorts by
name), header names are sorted lexicographically (a Go map traversal
is random). The re-analysis result must be reproducible byte for byte.

Patterns are compiled once and cached (a `sync.Map` keyed by the
pattern string): rulesets reload on SIGHUP without a restart,
`Evaluate` is called by the worker's parallel goroutines; the cache
lives together with the ruleset — a `rules_version` change clears it.
A non-compilable or over-limit (`MaxPatternBytes`) pattern is rejected
by the validation at load (T-068); the engine protects itself as well
and silently skips such a rule — it must not fail a page's analysis.
The validation also rejects a pattern matching the empty string (`^`,
`x*`, `(?:)`): such a rule would fire on any input, with empty
evidence.

## Evidence

`laravel_findings.signals` (jsonb, migration 000001) — an array
`[{rule, weight, evidence}, …]` for every hit. The evidence — a brief
string of "what exactly was found":

| target | Format | Example |
|---|---|---|
| `cookie_name` | `cookie <name>` | `cookie XSRF-TOKEN` |
| `cookie_value` | `cookie <name>=<value prefix>` | `cookie laravel_session=eyJpdiI6…` |
| `header_name` | `header <name>` | `header X-Laravel` |
| `header_value` | `header <name>: <value>` | `header X-Powered-By: PHP/8.2.8` |
| `html` | the regex-matched fragment | `/build/assets/app-DJIrOSj4.js` |
| `meta` | the whole meta tag | `<meta name="csrf-token" content="eyJ…` |

The content is untrusted (spec §9); the evidence is brought to its
storable form (`finalizeEvidence`):

- **trimming** to `max_snippet_bytes` (64 by default) bytes **at a
  UTF-8 rune boundary**, adding `…` when trimmed; a cookie/header name
  inside the evidence is additionally compressed to 32 bytes so that a
  hostile long name does not push the value out of the trim window;
- **invalid UTF-8** → U+FFFD (the evidence is valid already in
  memory, not only after `json.Marshal`);
- **control characters** (C0, DEL, including tabs and line breaks) →
  `·` — a single line without terminal escape sequences when read by
  an operator.

## Limits on untrusted content (spec §9)

Spec §9: "page content is considered untrusted: all parsers have size
and time limits". The inputs are bounded even before the engine (HTTP:
body/header limits; the storage: `content.max_store_bytes`, the 1–2 MB
recommendation) — the engine limits are defense in depth: `Evaluate`
terminates in finite time on any input.

`rules.Limits` (the zero value fully enables `DefaultLimits`;
`Set.Evaluate` always calls with the defaults, `Set.EvaluateCtx` takes
explicit ones):

| Limit | Value | What it bounds |
|---|---|---|
| `MaxHTMLBytes` | 2 MiB | the inspected HTML/DOM: trimmed **before** matching (the html and meta targets) |
| `MaxValueBytes` | 64 KiB | one cookie/header value (and the header names) |
| `MaxPatternBytes` | 4 KiB | the length of one rule's regex pattern |
| `MaxSnippetBytes` | 64 | the evidence (see above) |
| `EvalTimeout` | 5 s | the budget for the whole page (through the context) |

Decisions on protection against ReDoS-like hangs:

- Go `regexp` — an **RE2** implementation: matching is linear in the
  input length, there is no catastrophic backtracking like `(a+)+$`
  against `aaaa…b`. Two vectors remain — a bloated input (linear time
  still grows) and a bloated pattern (the compilation cost and the
  number of states). Both are closed by the size limits above, not by
  per-match timers.
- `EvalTimeout` is implemented via `context.WithTimeout` inside
  `EvaluateCtx`: the context is checked between rules and between
  cookie/header values (and at the input of `matchRule`). A single
  match cannot be interrupted, but it is linear over the already
  bounded input — the cancellation takes effect within one match. On
  cancellation the scan stops, the result contains what fired before
  the cancellation and is marked `Result.Partial`; the worker neither
  records nor stamps an incomplete result — the row stays stale, and
  the re-analysis (T-074) will return to it.
- The price of `MaxValueBytes`: a rule with an end anchor (`…$`) will
  not fire on a value longer than the limit — a deliberate trade of
  completeness for a time guarantee; the Laravel rules are anchored at
  the start over values (`^eyJ…`).
- The Laravel traits (the meta csrf-token, the vite/mix build paths,
  `data-page`, `wire:*`) live in the head part of the document — the
  first tens of kilobytes; trimming the tail beyond 2 MiB loses no
  signals on real pages.

## The analyzer worker and the writing of laravel_findings (T-071)

`internal/analyzer` — the `cmd/analyzer` service (stage 8 of spec
§12): it consumes `q:analyze` with the `analyzer` consumer group (a
read window of 16, 8 deliveries in parallel), every delivery — with
the `queue.Processing` pattern (T-025: XACK only after the database
write). The HTTP stage (T-058, after recording the result) and the
render-worker (T-067, after replacing the content with the final DOM)
publish the stream — the `Result` message carries only the
`subdomain_results` row key (spec 4.2); the analysis input is read
from the database.

Processing one delivery:

1. **SELECT of the row** — the `headers`/`cookies` jsonb (after the
   render — the data of the final navigation, T-067/T-058) and
   `content_hash`. No row — success without writing (0 rows →
   success, the render-worker style): the domain was rechecked without
   this key or cleaned up.
2. **The content** — `ContentStore.Get` by `content_hash` (the same
   PackStore and zstd dictionary as the writing stages): the HTML/DOM —
   the material of the `html`/`meta` targets. No hash, lost content
   (`ErrNotFound`), or an empty DOM — the analysis continues over the
   headers and cookies (the rules cover six targets, spec 5.8;
   html/meta are only part of the set).
3. **The matching** — `Set.EvaluateCtx` with `DefaultLimits` (the
   cancellation budget — the delivery's context): a ruleset snapshot
   is taken for every delivery from `Reloader.Set()` (T-068); SIGHUP
   changes the ruleset without a restart — the
   `service.Options.OnSIGHUP` hook, no subscription to changes is
   needed.
4. **One transaction** (spec 3.4):
   - `UPDATE subdomain_results SET rules_version = <the ruleset
     version>` — the stamp "the row was analyzed by ruleset N": the
     re-analysis reads it (T-074 finds records with a stale version;
     T-067 resets it to NULL on a content change). The UPDATE as the
     first statement — the freshness guard: `content_hash IS NOT
     DISTINCT FROM` the read value (another episode rewrote the row —
     the T-058 recheck DELETE+INSERT, the render T-067) and the domain
     is not `deleted` (§8/§13.1). 0 rows — the row slipped out from
     under the task: success without writing; the new episode has its
     own `q:analyze` publication;
   - `laravel_findings` under the same key: **all hits of any rule
     regardless of the threshold**,
     `is_laravel = score ≥ threshold` (`analyzer.laravel_min_score`,
     0.6 by default, §13.1); `ver_min`/`ver_max` — the intersection of
     the fired rules' ranges (NULL when a bound is absent; a range
     conflict — both bounds NULL, T-070); `signals` —
     `[{rule, weight, evidence}, …]`; `rules_version` — the ruleset
     version. An upsert by PK — redeliveries and re-analysis overwrite
     (§3.5);
   - no rule fired — the row is not written (spec 5.8) and an existing
     one is deleted: the previous analysis found traits that the
     current episode does not have (§3.5 result overwriting); the
     `rules_version` stamp is placed on the "empty" outcome too;
   - the recomputation of the `laravel_domains` row — the domain's
     aggregate over its findings (T-072, the next section): in the
     same transaction, right after writing the findings.
5. **XACK** after the transaction commit.

Redelivery idempotency (spec 3.4): the upsert + a repeated stamp by
the same ruleset do not change the state; a failure between the steps
rolls back the transaction entirely — the redelivery goes through
again.

Throughput: the analysis is light (RE2 matching over bounded input —
microseconds; a few database statements per delivery); scaling — by
service instances (a consumer group, spec 4.2).

## Domain-level aggregation: laravel_domains (T-072)

Spec 5.8: "at the domain level — an aggregate (the maximum score, the
union range, the subdomain mask)" — the `laravel_domains` row (§6.1:
the PK `(zone_id, domain_id)`, `score`, `ver_min`/`ver_max`,
`found_on_mask`, `rules_version`, `updated_at`). The recomputation
runs in the worker's transaction right after writing the findings:
every committed state is consistent — the findings and the aggregate
never diverge (spec 3.4). Implementation —
`internal/analyzer/aggregate.go`.

**Which findings enter.** Only confident hits — `laravel_findings`
with `is_laravel` (§13.1: the "score ≥ 0.6" threshold for a domain to
enter `laravel_domains` is the definition of confidence; weak hits
stay in the findings). Therefore:

- `score` — the maximum: only the maximum could reach the threshold,
  and the value coincides with the maximum over all the domain's
  findings;
- `found_on_mask` — the bits of the subdomains with an `is_laravel`
  hit (any variant — the variants of one subdomain collapse into a
  bit); the semantics are the same as `domains.dns_mask`: bit # =
  `subdomains.id`, bits 0..62 (`model.DNSMask`, at most 63 subdomains,
  spec 5.10).

**The union version range.** The lower bound — `min(ver_min)`, the
upper — `max(ver_max)` over the confident hits — a spanning range, not
an intersection (an intersection computes each finding by the rules of
one page; the spec's word is "union"). The comparison — by numeric
components (`rules.CompareVersions`, T-070): a textual MIN/MAX in SQL
would order "10" before "5.3". A NULL bound at any hit (no bound or a
T-070 conflict) propagates to the domain: the union from that side is
unbounded, the domain's bound is NULL; a bound inversion is impossible
— every hit has `ver_min ≤ ver_max` (T-070). Equal bounds in different
notations ("9.19" and "9.19.0") keep the first one — the read order by
PK is deterministic.

**`rules_version`** — the version of the ruleset the recomputation was
performed with (the delivery's `rules.Set.Version`): in the transition
period of re-analysis (T-074) a domain's findings are of mixed
versions, the row is rewritten by every delivery — after the last
update of the domain's findings the version is fresh.

**No confident hits — no row** (the §13.1 threshold): the
recomputation deletes the existing row — the previous traits were not
confirmed by the current episode (§3.5: only the last result is
stored). The removal of a find **by re-analysis** (a delivery without
fresh) — the `unfound` signal from `aggregateDomain`
(`RowsAffected > 0 ∧ ¬fresh ∧ existed`): the domain's
`subdomain_results` records were not rewritten and returned to the
retention candidates behind the T-185 drain cursor — right after
committing the transaction the worker calls the point drain
`content.DrainDomain` (T-187, the `Options.DrainDomain` seam, the
window `Options.Retention` = `content.retention`): the cleanup by the
PK prefix `(zone_id, domain_id)` with the regular T-166 purge
mechanics; a drain failure — a Warn and a metric, the delivery is not
failed. The details and the deliberate residual windows —
docs/content.md ("Content retention").

**Reconciling parallel deliveries.** The HTTP stage publishes
`q:analyze` in a batch over a domain's rows, and the worker processes
deliveries in parallel (8 at a time, plus other instances of the
group). Two recomputations reading the findings before each other's
commit would produce an aggregate without the neighboring delivery's
contribution. Therefore the recomputation first takes
`pg_advisory_xact_lock(domain_id)` (the identifier is globally unique
— the `domain_id_seq` sequence): the domain's second recomputation
waits for the first one's commit and reads the already complete
findings. An advisory lock, not a lock of the `domains` row: by this
moment the worker already holds the `subdomain_results` row lock (the
T-071 stamp), and the "stamp → domains" chain would close into a cycle
with the T-058 recheck transaction (UPDATE domains → DELETE
subdomain_results); the advisory key of this class is taken only by
the analyzer's recomputations — there are no waiting cycles.

`updated_at` — the database server's `now()`: the transaction time (in
the style of `httpworker/record.go`).

## Re-analysis (T-074)

Spec 5.8 "Re-analysis": a background task finds records with a stale
`rules_version` and enqueues them into `q:analyze`; re-downloading is
not required. Implementation — `internal/analyzer/reanalyze.go`.

**The staleness criterion.** `subdomain_results.rules_version IS
DISTINCT FROM <the current ruleset version>` (`rules.Set.Version`,
T-068). `IS DISTINCT FROM`, not `<>`: NULL — "never analyzed" (rows
written before T-071 and the new check episodes of T-058) — does not
drop out of the comparison. `deleted` domains are not re-analyzed
(§13.1, the `status <> deleted` guard as in the scheduler's
selections); their results are cleaned by T-020. Disabled zones are
not filtered: re-analysis does not touch the network and violates none
of the §5.10 bans.

**The pass.** Batches of 5000 rows (`DefaultReanalyzeBatch`): a
selection of keys (domain, subdomain, variant + the domain name) →
the publication of `Result` messages (T-024) into `q:analyze`. The
worker takes the content from the storage by key — no requests to the
sites (§5.8 "passive analysis"). Backpressure — `WatermarkPublisher`
(T-026): the publication waits for the `q:analyze` capacity; a ctx
cancellation — a clean exit.

**A cursor instead of re-selection — two shapes (T-193).** The
publication does not change the row — the T-071 worker will stamp the
version, asynchronously. Re-selecting "from the beginning" would take
the same in-flight batch, so the passes use cursor pagination, and
there are two shapes:

- **the PK shape** (`ReanalyzeDrain`/`ReanalyzeOnce`, a row-value `>`
  over the PK) — the full drain: a rules change (Subscribe, T-068) and
  `crawlerctl reanalyze`. Rare; it must traverse the whole stale set
  from zero; `rules_version` is deliberately not indexed (the HOT
  invariant: analysis updates — millions/day, the T-104/T-175 class),
  and the seq scan here is deliberately complete — one full pass per
  rules change.
- **the time shape** (`ReanalyzeTick`, a row-value `>` over
  `(checked_at, PK)`, an `ORDER BY` over the same key) — the hourly
  tick: the tick's candidate set — a fresh slice (the new T-058
  episodes with a fresh `checked_at` and a NULL version; the tails
  whose publication lagged/was lost). The composite cursor is served
  by the ready `subdomain_results_checked_idx` index (T-171) — the
  tick reads only the window ahead of the cursor, the cost ∝ the
  inflow since the previous tick, not the table size (before T-193 a
  zero PK cursor ran a Parallel Seq Scan of the whole
  `subdomain_results` — 4.8M rows, 19.9 s — for the sake of a handful
  of rows; the decision's precedent — the T-185 retention cursor).
  The cursor lives in the loop state between passes and advances past
  the last row of the published batch; an empty batch — the window is
  exhausted, the cursor waits for the next tick. A publication error
  does not move the cursor past the failed batch — the next tick will
  publish its rows again (at-least-once, §3.4; the duplicates are
  absorbed by the worker's key idempotency — like T-066).

**The tail-bucket guard — the tick does not enter the window without
mature tails (T-204).** Even an hour's window — 84K rows (the inflow
2–3M/day, TZ T-201 §2.1) with a heap visit for each because of the
unindexable `rules_version` — cost 5–12 s at steady state and 86–94 s
in the midnight storm (a multiplier of CPU/IO starvation,
PSI ~80%) — the T-193 acceptance criterion "the tick disappears from
the slow journal" was not met. As with the retry (T-191 → T-202), a
naive "recorded without analysis" counter does not distinguish tails
from the immature in-flight flow (record → `q:analyze` → stamp takes
seconds, and an inflow of 23–35 rows/s keeps it non-zero always) —
maturity is provided by the HOURLY buckets
`crawler_counters['reanalyze:YYYY-MM-DD"T"HH24']` (the T-202/T-203
family): the guard (`reanalyzeGuardSQL`) sums the buckets older than
the current hour BEFORE the heavy selection. A sum ≤ 0 — the window is
not read at all, and the cursor jumps to the current hour's boundary
(the date from the same guard row): without the jump the cursor would
freeze at the last publication, and the first tail after quiet hours
would cost a multi-hour pass — the jump keeps every guard-opened
window within ~2 h of inflow; the first tick after the loop start,
with the guard closed, does not read the 24 h anchor window either
(the starting drain has already traversed everything stale). The
buckets take increments from result recording (`record.go`, the hour
of the transaction's `now()` = the rows' `checked_at`) and from
content replacement by the render (`renderworker`, the episode's hour
from `RETURNING`); the decrement — the analyzer's stamp of stale rows
only (the version guard in `analyzeMarkSQL`: repeated deliveries are a
no-op, the delta is not doubled); the authority — the monitor's
reconciliation (the `prev_reanalyze`/`cur_reanalyze` FILTER positions
of the two-live-hours window aggregate, `docs/schema.md`). The
writers' lock order is shared with the pass: "… → `retry_mature:` →
`reanalyze:` → `rendered:` → `results:`". The residual classes (an
order of the same kind as the §4.2 anchor): a tail not picked up
within two hours (failed ticks) — the bucket was deleted by the pass;
a restart with the anchor or the full drain of a rules change will
pick it up; a mature tail masked by a negative bucket drift (a closed
guard jumps the cursor past it) — the same rescue: a restart/drain,
waiting for one reconciliation is not enough (docs/runbooks.md §7);
stale NON-NULL rows (a stamp of the old version shortly before a rules
change + a lost drain publication) are invisible to the buckets
(`IS NULL` against the `IS DISTINCT FROM` of the tick's selection) —
their territory is the next full drain/restart; the decrements of
old-version stamps (drains) produce a negative drift before the pass —
the guard is silent, and during a drain the tick's own work intersects
only in duplicates; a render lateness > 1 h resurrects an old hour's
bucket — the guard opens in vain (the tail is behind the cursor), and
the pass deletes the bucket.

**The cursor anchor — a 24 h window, not the epoch**
(`DefaultReanalyzeLookback`, T-193 §4.2): the tick's cursor starts at
`now() − 24 h`, not at the beginning of time — the starting full drain
has already covered everything stale at start-up; the tick is left
with only the fresh tails, and the first tick must not read the whole
history. The tick's safety net serves fresh losses (the main path —
the immediate publication of `record.go` and the drain on Subscribe);
everything older is either analyzed (`rules_version` changes only with
an analysis write by the T-071 worker or a rules change; a new episode
gives a fresh `checked_at`) or is a **residual class** — a loss older
than the window without a rules change: unreachable in the model,
insured by the q:dead/q:analyze observability (T-162), and picked up
by the next full drain at a rules change (or by hand —
`crawlerctl reanalyze`). Cursor persistence in the database was
rejected deliberately: the anchor makes cursor loss cheap (one 24 h
window, ∝ the inflow).

**The `ReanalyzeLoop` loop** (wired in `cmd/analyzer`, a single
`OnRun` goroutine): a starting full drain right at start-up (the stale
data from the downtime, including the rules that changed during the
downtime — Subscribe notifies only a running process) → a ticker once
an hour (`DefaultReanalyzeInterval` — the insurance: messages lost to
the MAXLEN ~, a worker failure, new episodes) in the time shape under
the tail-bucket guard (T-204, above): the cursor is anchored at the
loop start to `now − 24 h` and survives passes, every tick continues
from where it stopped (or jumps to the hour boundary with the guard
closed) → an off-schedule full drain on a ruleset change
(`Reloader.Subscribe`, SIGHUP — `make rules-reload`; the PK shape from
zero; the guard does not concern the drain — it must traverse
everything). The tick and the drain run in the loop's single goroutine
— an intersection of the sets (a rules change during a tick) is safe:
both are idempotent at the "selection + publication" level. A pass
error — a Warn, the loop lives on (the OnRun contract, T-012).

**The contract with the T-071 worker:** for every processed `Result`
the worker stamps `subdomain_results.rules_version = the ruleset
version` — including rows without stored content ("nothing to
analyze" is also a result with a version), otherwise the background
pass would return to them endlessly. The stamp is placed only on stale
rows (the version freshness guard, T-204): a redelivery of the same
version is a no-op (the same version × the same content
deterministically give the same findings); the stamp decrements the
tail bucket of the row's episode hour in the same transaction — the
next tick's guard closes.

**Manual run** — `crawlerctl reanalyze [--zone NAME]` (see
`docs/dev-environment.md` §6): the same drain for one zone or all
zones, with a one-time load of the rules directory; prints the
enqueueing counter.

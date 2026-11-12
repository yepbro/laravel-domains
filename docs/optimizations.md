# Production-verified render pipeline optimization methods

A report on the methods **verified on the production server** (4 vCPU / 8 GB RAM,
155.212.174.88) during the render debt drain after the zone collapse window
(T-198, 2026-10-29–11-01). Context: ~980k domains with the `render_pending`
flag, the scheduler stopped, q:render at the watermark of 10000,
2 render-worker instances (9105, 9109) on a single chromium :9222.

Each method was verified with an A/B measurement on production (the metric —
domains/min over renderq passes, deliveries/min over XINFO, timeouts/10 min
over the journal, chromium CPU/MemoryCurrent, PSI). The week's final dynamics:
**37 → 96–106 domains/min (×2.7)**, the debt ETA from ~17 down to ~6 days.

Related tasks: T-198, T-207…T-214; the measurement log —
[`load-measurements.md`](load-measurements.md).

## Successful (applied on production)

### 1. `render.timeout` 15 s → 10 s (Oct 31)

The tarpit cohort of the debt: 340–660 timeouts/10 min with **idle** CPU
(chromium 74–190 %/400) — slots were sitting in 15 s waits. Lowering the
timeout cut the slot time of tarpits by a third (the price — ~13 % of slow
successful renders go into tarpit finalization, the T-207 measurement).

Effect: opened the way for the contexts speed-up (method 3); on its own the
rate stayed in the band, but the slots were freed. Config `/etc/crawler/config.yaml`,
introduced by the deploy.

### 2. `render.contexts` 4 → 6 (Oct 29)

A/B: 80 → 112–144 deliveries/min. With the 15 s timeout — the cap for a single
chromium (6 tabs × 2 workers = 12). Pinned as the intermediate optimum of the
"15 s" era.

### 3. `render.contexts` 6 → 8 with timeout=10 s (Oct 31, A/B #2)

An A/B repeat after the timeout reduction: **41 → 106 domains/min (×2.6)**,
timeouts down 662 → 384, chromium 226 %/400, memory within limits. At 15 s the
same configuration failed (see "Unsuccessful" #1) — slots were freed too
slowly, and 16 tabs choked the single process.

Conclusion: the "short timeout + more slots" pair is synergistic; the final
production configuration: `timeout: 10s`, `contexts: 8`.

### 4. Chromium recycling timer every 2 h (Oct 30, T-208)

In remote mode a worker cannot restart someone else's browser (T-065):
without recycling chromium accumulates memory 400 → 950+ MiB over ~2 h and
the timeouts double. The recycling (the systemd timer `crawler-chromium-recycle`)
in the same hour **halved the timeouts** (227 → 110). The price — up to ~10
orphaned in-flight deliveries per restart (they come back as claims;
attempts accumulating over frequent restarts give a single-step growth of
q:dead — observable, acceptable). The units are in `deploy/systemd/` (T-208).

### 5. Chromium flags (Oct 31)

`--disable-features=SitePerProcess,Translate,BlinkGenPropertyTrees`
(the feature name is exactly `SitePerProcess` since Chrome 96 — chromedp#1605 sends
the outdated `site-per-process`), `--disable-crash-reporter
--disable-breakpad --no-crash-upload`, `--disable-background-networking
--disable-sync --disable-default-apps
--disable-client-side-phishing-detection --metrics-recording-only`,
`--disable-background-timer-throttling
--disable-backgrounding-occluded-windows --disable-renderer-backgrounding`
(do not throttle the timers of invisible tabs — SPA hydration in full swing).

A/B: chromium memory 528 → 458 MiB (−13 %, site-isolation-off — fewer
renderer processes), the rate unchanged, 0 crashes. Kept.

### 6. CSS blocking — `render.block_stylesheet: true` (Oct 31, T-213)

A/B: **PSI CPU 66–71 → 47–59 %** (−15–20 pp — style/layout is cut),
the kernel-CPU share afterwards 47 → 23 %, detection **without
discrepancies** (the laravel+ and laravel_domains findings identical before/after ~50 min
of re-renders: 78,957 / 44,291), the rate within the noise band. No rate gain
(the limit — 16 slots and the page's network/JS), the value — CPU headroom
for the other services. The mechanism — extending the T-062 Fetch-interception
list with the ResourceType Stylesheet.

### 7. Tracker domain blocking — `render.block_tracker_domains: true` (Oct 31, T-214)

Analytics arrives as the Script/XHR type (it cannot be cut by type — SPA scripts
are needed), so 15 specialized domains
(google-analytics/googletagmanager/doubleclick/mc.yandex/…) are blocked
by the pair "a broad CDP pattern `*domain*` → an exact host verdict" in
the T-062 mechanics, the fail-open invariant preserved. Source measurements: 5–15
requests / 100–500 KB of analytics per page. Contributed to the timeout minima
(195–217/10 min after enabling).

### 8. The "render impossible" marker — `render_failed_at` (Oct 31, T-210, migration 000017)

Rows finalized without a render (tarpit/cert/DNS class;
~180–220k per the T-207 measurement) were previously re-published by a pass
at every re-flag. The stamp in `subdomain_results` excludes them from the
selection until the content changes (a reset — a new episode: a repaired host
will get rendered). 15k+ rows stamped over the first day.

### 9. DNS finalization from the first delivery (Oct 31, T-211)

`net::ERR_NAME_NOT_RESOLVED` — a permanent property of the host: finalization
without retries per the cert-class mechanics (the T-207 measurement: a dead host ate
3 deliveries and ~20 min in PEL). The counter
`crawler_render_dns_finalized_total`.

### 10. PostgreSQL: `max_locks_per_transaction` 64 → 512 (Oct 29)

Infrastructure: after the T-198 window (~700 partitions × hash leaves ×
indexes ≈ 2.7k relations) retention/count/`domain_info` fell with
`out of shared memory` — without this fix the content cleanup did not
start at all. ALTER SYSTEM + a PG restart in the window.

## Unsuccessful (verified and rejected)

### 1. `render.contexts` 6 → 8 with timeout=15 s (Oct 29, A/B #1)

**85 versus 73 domains/min** — worse than the baseline: 16 tabs saturated the single
chromium (381 %/400), the timeouts ×3 (83 → 264), browser memory up to 1.4 GB.
Rolled back the same day. The failure cause was understood later (see "Successful" #3):
with a long timeout the slots are busy with waits, and adding tabs only
increases the competition. Lesson: **contexts and timeout are a coupled pair,
an A/B of contexts without the timeout context is invalid**.

### 2. A second chromium (:9223) with `render-worker@9109` moved onto it (Oct 29)

**96 versus 112–144 domains/min** for a single browser: the two chromium
processes together reached 400 %/400 (all cores), the rate did not grow, MemAvailable
fell to 2.1 GiB. Rolled back (the env file removed, the unit disabled; the unit itself
with the `ReadWritePaths` fix kept in the set — T-209). Diagnosis:
the bottleneck is the 4 vCPU, not the browser process; the second browser shares
the same cores. Makes sense only on extended hardware.

### 3. A third render-worker instance (Oct 29)

Rejected without a run: both workers share one chromium (already saturated at
12 tabs), the second chromium did not fit in memory (MemAvailable
1.9 GiB < the ~2 GB budget). The reconsideration condition — extended RAM.

### 4. `redis.max_attempts` 3 → 2 (Oct 31, rejected by analysis)

Half of the "losses" did not exist (units: 1 domain ≈ 1.9 deliveries —
T-207), retries are 5–10 % of deliveries; timeouts and DNS classes are finalized
from the first delivery, attempts do not participate here. Cons: the recycling timer
orphans deliveries for sure (with 2 attempts less margin for a
claim), 6.6 % of successful retries of transient classes are lost
(CONNECTION_REFUSED, T-172), the setting is global across all queues
(a DNS batch = 5000 domains). Rejected without an A/B.

### 5. `ERR_CONNECTION_REFUSED` into the finalize-from-first-delivery class (Oct 31, T-211)

Deliberately NOT enabled: per the T-172 measurement the class has **6.6 % successful
retries** (0/321 for cert codes) — it is transient (a port/filter/service
restart), retries yield a real DOM.

### 6. Lightpanda (a CDP browser without rendering, 3–5× faster — per research)

Rejected on fidelity grounds: Laravel detection depends on the real SPA hydration
in a real Blink; the project is young. The research alternative is pinned
as "exotic" (sources: lightpanda.io, CDP client comparisons).

## Summary table

| # | Method | Result | Status |
|---|---|---|---|
| 1 | timeout 15→10 s | slots freed, tarpits −a third | ✅ prod |
| 2 | contexts 4→6 (at 15 s) | 80→112–144 deliveries/min | ✅ prod (the 15 s era) |
| 3 | contexts 6→8 (at 10 s) | 41→106 domains/min (×2.6) | ✅ prod |
| 4 | chromium recycling 2 h | timeouts ×0.5, memory stable | ✅ prod |
| 5 | chromium flags (SitePerProcess-off etc.) | memory −13 % | ✅ prod |
| 6 | block_stylesheet | PSI CPU −15–20 pp, detection 0 discrepancies | ✅ prod |
| 7 | block_tracker_domains | timeouts at the minima | ✅ prod |
| 8 | render_failed_at (T-210) | re-publishing stopped | ✅ prod |
| 9 | DNS finalization from the 1st delivery (T-211) | −2 deliveries/−20 min PEL per host | ✅ prod |
| 10 | max_locks_per_transaction 512 | retention works | ✅ prod |
| 11 | contexts 8 at 15 s | 73 versus 85 — worse | ❌ rollback |
| 12 | second chromium :9223 | 96 versus 112–144 — worse | ❌ rollback |
| 13 | third render-worker | no CPU/RAM headroom | ❌ rejected |
| 14 | max_attempts 3→2 | ~0 effect, work-loss risks | ❌ rejected |
| 15 | CONNECTION_REFUSED → 1st delivery | 6.6 % successful retries | ❌ rejected |
| 16 | Lightpanda | detection fidelity | ❌ rejected |

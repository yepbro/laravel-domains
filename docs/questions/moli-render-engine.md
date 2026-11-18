# Moli: the candidate render engine — the full history

Settled 2026-11-16, updated 2026-11-18 (T-331, the upstream confirmation;
T-332, the quality-gap decomposition; T-333, the fresh-window
reproducibility re-check). The short version:
**Moli is our second candidate render engine (stage 35). Functionally it
reached full CDP parity with Chromium in v1.1.25 — the release that
finally carried all the interception fixes we waited for — but the same
release introduced a memory regression: a single heavy streaming page
(hh.ru) bursts the process to any memory ceiling in seconds. Upstream
confirmed our issue
([#3047](https://github.com/lexmount/moli/issues/3047)); the fix
([PR #3060](https://github.com/lexmount/moli/pull/3060)) shipped in
**v1.1.26** and is **verified fixed by the T-331 retest** (the canary
holds hh.ru at 177.7 MiB; the smoke peak 312 MiB < ⅓ of the chromium
budget; 0 crashes; the T-283 battle check clean). The pin, however,
stays at v1.1.21: the §5.1 quality thresholds are still red (success
87/100 vs the baseline 92/100 — the tarpit/ERR_FAILED class that
predates the memory regression; detection 2.38% — both divergences are
the baseline's own channel losses, in moli's favor). Meanwhile our side
is fully compensated: page-killer URLs are routed around the engine
(T-285) and the worker survives engine restarts (T-283).**
The follow-up ran as **T-332** (2026-11-18): fresh paired runs
(chromium → moli back-to-back, one channel window) with the per-domain
divergence table — the gap is decomposed (see "Current state and
retrigger" item 2 and the T-332 artifact); the pin decision stands at
v1.1.21. The #3047 fix confirmation is posted upstream.
**T-333** (2026-11-18, a fresh day window) re-checked the
engine-attributable findings for reproducibility before any upstream
post: the core HOLDS with amendments (heroku/netlify/fastly solid,
cloudflare reframed from a ">60 s hang" to the 2–3× page-time
regression class + a new ~30 s internal `net::ERR_TIMED_OUT` timer;
ovh demoted as channel-fragile, rust-lang resolved as a flake) —
item 2 below.

## Why Moli at all

- Stage 35/36 of the render track: candidate render engines against the
  chromium baseline — [Moli](https://github.com/lexmount/moli) and
  Lightpanda. The goal is a smaller memory footprint for bulk rendering
  (budget ≤ 1 GB per engine container vs the heavy `headless-shell`),
  with production canary windows as the end state.
- Dev stack: the `render-engines` compose profile, CDP at
  `ws://172.28.100.17:9222`, our own minimal image built from the release
  tarball (`deploy/docker/Dockerfile.moli`); the version pinned via
  `MOLI_VERSION` + `MOLI_SHA256` in `.env` (FR-17). Moli speaks the same
  `/json/version` + CDP as Chromium, so the engine switch is two config
  keys. Details — [`../dev-environment.md`](../dev-environment.md),
  "Render engines: lightpanda and moli (stage 35)".
- The operator is a moli contributor (PRs #2470, #2704 are ours), so the
  upstream feedback loop is direct.

## The timeline: what broke and what fixed it

### v1.1.21/v1.1.22 — Fetch.enable rejected the patterns (2026-11-06)

The first bench (the seed hundred, the dump 242 baseline
`chromium-1`): moli answered `InvalidParams` to `Fetch.enable` with the
Media/Font resource types in the patterns — the engine distinguished a
trimmed set of `resourceType`. Every task failed at `startBlock`:
**0/100** (the artifacts `var/fixtures-render/moli-1.snapshot.json`,
`report-moli-1.txt`). The CDP slice confirmed: everything except the
Fetch interception was green already (contexts, navigation, DOM, cookies,
UA override).

**Fixed upstream by our own PR**
[#2470](https://github.com/lexmount/moli/pull/2470) (the `yepbro/moli`
fork) + the paired [#2476](https://github.com/lexmount/moli/pull/2476) —
shipped in **v1.1.23**.

### v1.1.23 — the patterns accepted, the interception itself broken (T-272, 2026-11-10)

The retest (T-272): `Fetch.enable` accepts the base set, and the slice
was green except the interception-dependent items. Three defects:

1. **a synchronous XHR under interception fails on the client**
   (`Uncaught NetworkError … 'send' on 'XMLHttpRequest'`) — even when the
   request does not match any block pattern;
2. **`Fetch.failRequest` of the main navigation leaves the tab
   unresponsive** — no load error arrives, the SSRF cut-off cannot
   finalize the task;
3. **Stylesheet is absent from `supports_fetch_enable()`** — the
   Stylesheet pattern brings the whole `Fetch.enable` down with
   `InvalidParams`, so `BlockStylesheet` (the T-213 CSS mode) is
   unavailable (not a blocker: the option defaults to false and the base
   slice does not enable it).

**Fixed in upstream main** (after the v1.1.24 release cut of 2026-11-12,
which therefore lacked them):

- ① [#2704](https://github.com/lexmount/moli/pull/2704) — derive
  interception admission from shared rules (the sync-XHR class-set fix;
  ours);
- ② [#2696](https://github.com/lexmount/moli/pull/2696) — `failRequest`
  of a blocked navigation finishes with a load error (the SSRF path);
- ④ [#2729](https://github.com/lexmount/moli/pull/2729) +
  [#2883](https://github.com/lexmount/moli/pull/2883) — interception
  stays live during synchronous requests: a matching sync request is
  paused at the Request stage and the client's verdict decides — full
  parity with Chromium (the final maintainer line `9e357b95e`, after the
  exact-URL drafts `8ff18ca02`/`e8ce829d8`).

The evolving semantics forced two cleanups on our side: **T-275** moved
the tracker probes to the async switch (real analytics is async) and
**T-276** unified the sync-probe expectations across the engines (both
expect the block now).

### main `f857b6651` (reports 1.1.24) — the integration-build retest (T-247, 2026-11-13)

The first build with ①②④: the slice **6/6** (after T-276; the first run
was 5/6 with the only red being our own stale expectations). Layer 2
(the smoke hundred): **NOT READY on quality, not on CDP** — success
69/100 vs the baseline 92/100 (tarpit finalizations 27 vs 4, 4×
`net::ERR_FAILED`), the Laravel detection divergences 2.94% > 1%, the
engine RSS 736 MiB monotonic (above ⅓ of the chromium budget), 0
crashes. The pace and memory were "close, but not yet". The pin was NOT
raised (a main build, not a release); the retest was scheduled with the
first release carrying ①④.

### v1.1.25 — full functional parity, but the memory regression (T-247, 2026-11-15/16)

The release we waited for (①④ in a release): the CDP slice **6/6 PASS**
(and the chromium control 6/6) — the first time a *released* moli passed
the whole stage invariants set. But both smoke runs (a flapping-VPN
channel at the 1 GiB limit, then a clean channel at 2 GiB) ended in
**cgroup OOM kills ×3**: the process bursts from a plateau of tens of
MiB to whatever ceiling exists (2 GiB in 6–21 s), at a strict
~10-minute rhythm, single process, silent logs. The §5.1 report:
**4×FAILED** (detection 3.75%, success 83/100, memory, crashes) + pace
NO DATA. Page time and DOM structure were *better* than chromium
(avg 10.37 s vs 14.69 s, structure ratios ×1.00) — the engine is good,
except the memory.

The repro ladder (a subagent driver = the exact worker CDP pattern)
localized the trigger: **ONE page, `https://hh.ru/`** — a single
navigation (even raw, without interception) kills the engine 6/6; the
body-size flag does not help; chromium 156 holds the same page at
~519 MiB. Upstream: our issue
[#3047](https://github.com/lexmount/moli/issues/3047) — confirmed by the
maintainer the next day; the fix
[PR #3060](https://github.com/lexmount/moli/pull/3060) "bound Inspector
parser text mutation buffering": **`DOM.getDocument` during a streaming
navigation enables Inspector DOM mutation recording, and the parser's
fragment-by-fragment appends buffer the old text + new text + a snapshot
of every growing prefix — quadratic memory** (Closes #3047). This also
explains our whole picture: the small testbed pages of the slice stayed
green, the heavy streaming DOM of hh.ru burst, and it fires exactly on
our DOM-capture path. Not in any release yet (open as of 2026-11-16).

### v1.1.26 — the memory fix verified, the quality gate still red (T-331, 2026-11-18)

The first release carrying #3060 (the tag `c8061786…`). Layer 0: the
asset checksum `b57b1b7a…93bb5`, the compose image `crawler/moli:v1.1.26`,
provenance checked at the gate (`Moli-Version: 1.1.26`,
`WebKit-Version: @c8061786…`). The **canary (D-026) GREEN**: hh.ru ×3
through the repro driver under the 1 GiB limit — 3/3 LOADED (DOM
2.25–2.32 MB, 19–23 s), peak RSS **177.7 MiB**, 0 kills (v1.1.25: 3/3
OOM in 8–18 s; chromium holds the same page at ~519 MiB). The CDP
slice **6/6** (+ the chromium control 6/6 — after re-running both in
the testbed DNS mode; in internet mode unbound NXDOMAINs `*.crawl.test`
by design and any engine goes red on spa-shell/ssrf, see D-069). The
smoke (the seed hundred, baseline chromium-1): **NOT READY — quality,
not memory**: success **87/100** vs 92/100 (tarpit 9 vs 4, ERR_FAILED
4 vs HTTP2 1; 8 candidate-only fails are internationals over the
night's channel, 3 baseline-only — the baseline's losses), the Laravel
detection divergences **2.38%** — both are laravel-com/forge-laravel-com
where the baseline failed to render and moli detected them correctly;
**memory 312.0 MiB PASSED** (< ⅓ of 2 GiB); **0 crashes PASSED** (0
memcg events in dmesg); pace NO DATA (the q:analyze trim shape;
indicatively the drain took 26 min 16 s ≈ 0.063 rows/s vs the baseline
0.07). The **T-283 battle check** (a deliberate `docker kill` mid-drain,
~48 s of engine downtime — the explicit kill is not policy-restarted,
D-069): the claim gate held ("delivery attempts are not spent"),
`crawler_render_browser_restarts_total{engine="moli"}` 0→1, all
delivery counts stayed 1 (0 burned attempts), the drain completed. The
pin reverted to v1.1.21; the artifacts `var/fixtures-l2/moli-v1.1.26-*`.

## What each version means for us

| Version | State against our stage | What appeared |
|---|---|---|
| v1.1.21/1.1.22 | broken: every task fails at `Fetch.enable` | — |
| v1.1.23 | base CDP green; interception broken (sync XHR, failRequest, no Stylesheet) | #2470 (ours) + #2476: `Fetch.enable` accepts the pattern set |
| v1.1.24 | not tested — the release cut lacked ①④ | — |
| main `f857b6651` ("1.1.24") | slice 6/6; smoke: not ready (quality) | ①#2704 ②#2696 ④#2729+#2883 |
| **v1.1.25** | **slice 6/6 — full functional parity; smoke: NOT READY (the hh.ru memory burst)** | ①④ in a release; **the regression**: quadratic Inspector mutation buffering (#3047/#3060) |
| v1.1.26 | slice 6/6; canary hh.ru GREEN (3/3, peak 177.7 MiB); smoke: **NOT READY (quality, not memory)** — memory 312 MiB PASSED, 0 crashes, T-283 battle check clean; success 87/100 vs 92, detection 2.38% (both divergences favor moli — the baseline's own channel losses) | #3060 in a release: **the #3047 memory burst is fixed**; the tarpit/ERR_FAILED quality gap persists |
| v1.1.26 (T-332, fresh pair) | 85/100 vs fresh chromium 89/100; the gap decomposed per-domain (D-071): the ERR_FAILED quartet is channel-side, the engine core = the cloudflare.com **hang** + the ovh/heroku/sport-express slowness (39–44 s), moli wins forge (12.7 s vs ~21 s) | the quality residue is now a named reproducible domain set — the upstream follow-up material |
| v1.1.26 (T-333, fresh-window re-check) | the engine core re-confirmed in a day window with amendments (D-072): heroku 0/2 @60 s vs chromium 26–28 s (the cleanest exhibit), netlify 35–60 s, fastly upgraded, sport-express ratio-only; cloudflare reframed: 55.95 s LOADED + ERR_TIMED_OUT @39 s — a 2–3× page-time regression, NOT a deterministic >60 s hang; NEW: moli's ~30 s internal net timer (7 attempts aborted before the budget); ovh demoted (both engines fail that window), rust-lang resolved (identical times); moli's forge win holds in direction | the upstream draft: needs changes (reframe to the slowness class + the 30 s timer, drop ovh/rust-lang), then ready — still not posted |

## Our side: tasks and compensations

- **T-241** — the engine on the dev stack (the render-engines profile,
  `Dockerfile.moli`, the static address, the pin pair).
- **T-254** — the pinned-version procedure (FR-17: pin → slice → smoke)
  and the per-engine live slice (`internal/render/engines_live_test.go`).
- **T-247** — the bench comparison + every retest above (the journal
  entries 2026-11-13/15/16 in [`../operations.md`](../operations.md); the
  artifacts in `var/fixtures-l2/`, the research doc
  `moli-v1.1.25-memory-research.md` and `moli-repro-summary.md`).
  **T-331** — the v1.1.26 retest (the journal entry 2026-11-18; the
  artifacts `moli-v1.1.26-*`).
- **T-275/T-276** — the probe expectations aligned with the final #2729
  semantics.
- **T-285** — `render.engine_skip_patterns`: a per-engine URL skip-list;
  a page-killer URL (hh.ru × moli) is routed to the fallback worker
  (chromium) *before* the navigation, reason `engine-skip`. The sample
  pattern lives commented in `config.example.yaml`.
- **T-283** — the worker survives an engine restart: the
  `render.engine_wait` start-retry budget (a short restart costs 0 task
  attempts), the claim-loop gate while the engine is down, the
  `crawler_render_browser_restarts_total{engine}` metric.
- Mitigation note: `restart_every_pages: 50` in the moli specimen of
  `config.dev.yaml` bounds the per-context retention floor — it does
  **not** protect against the burst (one page = one kill).
- Decisions: D-020 (the pin stays v1.1.21, v1.1.25 not ready), D-022
  (the regression confirmed by the clean-channel run), D-026 (hh.ru is
  the deterministic killer; every moli retest includes it as a canary
  URL), D-028 (the engine-restart semantics), D-069 (v1.1.26: the
  memory fix verified; the pin stays v1.1.21 on the §5.1 quality gate).

## Upstream links (issues & PRs)

- [#2470](https://github.com/lexmount/moli/pull/2470) — **ours**: the
  `Fetch.enable` resourceType class-set fix (shipped in v1.1.23, with
  the paired #2476).
- [#2696](https://github.com/lexmount/moli/pull/2696) — `failRequest`
  of a blocked navigation → a load error (the SSRF path).
- [#2704](https://github.com/lexmount/moli/pull/2704) — **ours**:
  interception admission from shared rules (the sync-XHR class-set fix).
- [#2729](https://github.com/lexmount/moli/pull/2729) +
  [#2883](https://github.com/lexmount/moli/pull/2883) — interception
  live during synchronous requests (Chromium parity).
- [#3047](https://github.com/lexmount/moli/issues/3047) — **ours**: the
  hh.ru memory burst report (the minimal repro, the controls, the
  regression window `f857b6651..v1.1.25`); confirmed by the maintainer.
- [#3060](https://github.com/lexmount/moli/pull/3060) — the fix: bounded
  Inspector parser text mutation buffering (Closes #3047); **shipped in
  v1.1.26 and verified fixed by the T-331 retest**. Our confirming
  comment (the canary + smoke memory numbers) is posted:
  [#3047 (comment)](https://github.com/lexmount/moli/issues/3047#issuecomment-6974108962).
  The remaining quality observations (ERR_FAILED/tarpits) are
  deliberately NOT upstream yet — first our own T-332 divergence
  analysis (the operator's decision); a separate issue with per-URL
  repros follows only if the engine side confirms.
   **T-333 confirmed the engine side** (2026-11-18, a fresh day
   window, D-072): the slowness class is reproducible (heroku 0/2
   @60 s vs chromium 26-28 s; netlify 35-60 s; fastly 36.4 s + a
   net error; the cloudflare gap real at 2-3x - but 55.95 s LOADED,
   not a deterministic >60 s hang) and a second engine-side behavior
   surfaced: moli aborts page loads on an internal ~30 s network
   timer (`net::ERR_TIMED_OUT`, 7/24 attempts) that chromium never
   has (it waits the budget out). The draft issue needs a reframe
   before posting - the slowness class + the 30 s timer as the
   headline, heroku as the lead exhibit, ovh/rust-lang dropped -
   see the T-333 section of the T-332 artifact.

## Current state and retrigger

1. The pin: `.env.example` default **v1.1.21**, the local `.env` holds
   the same pair (reverted after the T-331 retest per the FR-17 red
   path); the images `crawler/moli:v1.1.23`/`v1.1.25`/`v1.1.26` stay in
   the local cache (a retest is a two-line flip).
2. The v1.1.26 retest (T-331, 2026-11-18) closed the memory question —
   see the table row above and [`../operations.md`](../operations.md),
   2026-11-18. The remaining (and only) blocker was the §5.1 quality
   gap — **decomposed per-domain by T-332** (2026-11-18, the fresh
   paired runs; the full divergence table + the 25 s/60 s driver repros
   — `var/fixtures-l2/moli-v1.1.26-divergence-analysis.md`, D-071):
   the ERR_FAILED quartet is **channel-side** (3 dead resolutions + 1
   HTTP/2-broken origin — both engines fail them, moli just collapses
   the errors into `ERR_FAILED`); the baseline itself drifts (fresh
   chromium 89/100 vs the 06.11 92 — the old 2.38% detection headline
   was the old base's own losses); the engine-attributable core is a
   named reproducible set: the **cloudflare.com hang** (>60 s on moli
   vs 15–17 s on chromium — the upstream-report candidate) + the
   slowness class ovh/heroku/sport-express (39–44 s, would pass at
   timeout 45 s) ± borderline netlify/rust-lang, partially offset by
   forge.laravel.com where moli wins (12.7 s vs chromium's ~21 s with
   no headroom). The pin stays v1.1.21 (success 85<89, pace 0.13<0.14
   marginal, detection 1.22%>1% on the fresh pair); the upstream
   follow-up drafts (the hang + the ERR_FAILED mapping) live in the
   T-332 artifact, not posted. **T-333** (2026-11-18, a fresh day
    window, same driver @60 s, moli ×2 → chromium ×2 per domain,
    D-072) re-checked the core for reproducibility: heroku/netlify/
    fastly HOLD as engine-attributable (heroku harder: 0/2 @60 s),
    sport-express holds as a 4-5x ratio (the absolute passed this
    window), cloudflare is reframed - a real 2-3x gap (55.95 s
    LOADED + a 39 s net error vs chromium 28 s) but NOT a
    deterministic >60 s hang; ovh demoted (both engines failed that
    window - channel-fragile) and rust-lang resolved (identical
    times - a night flake); moli's forge win holds in direction.
    NEW: moli aborts loads on an internal ~30 s network timer
    (`net::ERR_TIMED_OUT`, 7/24 attempts; chromium waits the budget).
    The upstream draft: reframe (the slowness class + the 30 s
    timer, heroku as the lead exhibit, ovh/rust-lang dropped), then
    ready - the full T-333 table is in the divergence-analysis
    artifact. The pin decision is unchanged.
3. The canary protocol stands (D-026): every moli retest starts with
   hh.ru through the repro driver; with T-283 in place the engine may
   be killed mid-drain on purpose (note: an explicit `docker kill` is
   NOT restarted by the `unless-stopped` policy — only kernel-side
   kills are; use `docker restart` or an in-container kill to simulate,
   D-069).
4. A green retest (slice 6/6 + the smoke thresholds §5.1) → empty the
   `engine_skip_patterns` sample, raise the pin, and moli becomes a
   canary candidate again.

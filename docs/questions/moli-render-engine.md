# Moli: the candidate render engine — the full history

Settled 2026-11-16. The short version: **Moli is our second candidate render
engine (stage 35). Functionally it reached full CDP parity with Chromium in
v1.1.25 — the release that finally carried all the interception fixes we
waited for — but the same release introduced a memory regression: a single
heavy streaming page (hh.ru) bursts the process to any memory ceiling in
seconds. Upstream confirmed our issue
([#3047](https://github.com/lexmount/moli/issues/3047)) and the fix
([PR #3060](https://github.com/lexmount/moli/pull/3060)) is not in a release
yet. The pin stays at v1.1.21; the retest target is the first release
carrying #3060. Meanwhile our side is fully compensated: page-killer URLs
are routed around the engine (T-285) and the worker survives engine
restarts (T-283).**

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

## What each version means for us

| Version | State against our stage | What appeared |
|---|---|---|
| v1.1.21/1.1.22 | broken: every task fails at `Fetch.enable` | — |
| v1.1.23 | base CDP green; interception broken (sync XHR, failRequest, no Stylesheet) | #2470 (ours) + #2476: `Fetch.enable` accepts the pattern set |
| v1.1.24 | not tested — the release cut lacked ①④ | — |
| main `f857b6651` ("1.1.24") | slice 6/6; smoke: not ready (quality) | ①#2704 ②#2696 ④#2729+#2883 |
| **v1.1.25** | **slice 6/6 — full functional parity; smoke: NOT READY (the hh.ru memory burst)** | ①④ in a release; **the regression**: quadratic Inspector mutation buffering (#3047/#3060) |
| next (TBD) | the retest target | #3060: bounded parser text mutation buffering |

## Our side: tasks and compensations

- **T-241** — the engine on the dev stack (the render-engines profile,
  `Dockerfile.moli`, the static address, the pin pair).
- **T-254** — the pinned-version procedure (FR-17: pin → slice → smoke)
  and the per-engine live slice (`internal/render/engines_live_test.go`).
- **T-247** — the bench comparison + every retest above (the journal
  entries 2026-11-13/15/16 in [`../operations.md`](../operations.md); the
  artifacts in `var/fixtures-l2/`, the research doc
  `moli-v1.1.25-memory-research.md` and `moli-repro-summary.md`).
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
  URL), D-028 (the engine-restart semantics).

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
  Inspector parser text mutation buffering (Closes #3047); watch until
  merge + release.

## Current state and retrigger

1. The pin: `.env.example` default **v1.1.21**, the local `.env` holds
   the same pair; the images `crawler/moli:v1.1.23`/`v1.1.25` stay in
   the local cache (a retest is a two-line flip).
2. Watch [PR #3060](https://github.com/lexmount/moli/pull/3060) until
   merge + release; then bump `MOLI_VERSION`/`MOLI_SHA256` via the
   FR-17 procedure (T-254 — [`../dev-environment.md`](../dev-environment.md),
   "Switching the pinned engine version").
3. The retest must include **hh.ru as a canary URL** (D-026) — with the
   T-283 resilience in place the engine may even be killed mid-drain on
   purpose to verify the worker survives it end-to-end.
4. A green retest (slice 6/6 + the smoke thresholds §5.1) → empty the
   `engine_skip_patterns` sample, raise the pin, and moli becomes a
   canary candidate again; the tarpit/ERR_FAILED quality gap of the
   13.11 run is the second thing to watch in the report.

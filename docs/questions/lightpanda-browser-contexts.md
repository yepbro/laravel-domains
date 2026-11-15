# Lightpanda: why we need it and why it is still red (browser contexts)

Settled 2026-11-15. The short version: **Lightpanda is our candidate low-memory
render engine (stage 35), but it allows only one browser context per CDP
connection, which makes our per-task isolation model impossible. Release 1.0.4
did not change this — multi-page ≠ multi-context. The fix is upstream PR
[#4151](https://github.com/lightpanda-io/browser/pull/4151), still open. Until
it lands in a release, the engine stays pinned red on the live slice; there is
nothing to re-test on our side.**

## Why Lightpanda at all

- Stage 35/36 of the render track: candidate render engines against the
  chromium baseline — [Lightpanda](https://github.com/lightpanda-io/browser)
  (official image) and Moli. The goal is a smaller memory footprint for bulk
  rendering (budget ≤ 1 GB per engine container vs the heavy `headless-shell`),
  with production canary windows as the end state.
- Dev stack: the `render-engines` compose profile, CDP at
  `ws://172.28.100.16:9222`, the image pinned via `LIGHTPANDA_IMAGE` in `.env`
  (FR-17). Details — [`../dev-environment.md`](../dev-environment.md),
  "Render engines: lightpanda and moli (stage 35)".

## The requirement that blocks it

Our render model (spec 5.6/§9, FR-5 of the engines spec): **one task = one
isolated incognito browser context** — `Target.createBrowserContext`
(`chromedp.WithNewBrowserContext`), a single tab, own cookies/cache/storages,
`Target.disposeBrowserContext` after the task. No state survives between pages.
Implementation: `internal/render/session.go` (`NewSession`); the package doc in
`internal/render/render.go`. The per-engine live slice checks exactly this
invariant — `internal/render/engines_live_test.go`, item (b) "an isolated
context per task: a cookie does not survive a context change".

## The blocker

Lightpanda (1.0.2/1.0.3) allows **at most one browser context per CDP
connection**: a second `Target.createBrowserContext` answers
`-32000: Cannot have more than one browser context at a time`. The starting
(service) tab already occupies the only slot, so the session's
`createBrowserContext` fails unconditionally — the "context per task" model is
impossible on the pinned versions. Recorded in:

- `internal/render/engines_live_test.go` — the doc comment, the lightpanda
  bullet (the slice is red until the engine is fixed);
- `.opencode/skills/prod-report/references/prod-topology.md` — the 2026-11-06
  bench row ("one browser context per connection, upstream #4151, watch").

Note: sequential create→dispose→create does not help either —
`Target.createTarget` auto-creates the (only) context, so a client that first
opens a helper tab (as our `Browser` does) can never create a context
explicitly. This nuance is stated in our comment in the upstream issue
(2026-11-06).

## Upstream state (as of 2026-11-15)

- The umbrella issue:
  [lightpanda-io/browser#882](https://github.com/lightpanda-io/browser/issues/882)
  "multi contexts and multi pages is not supported".
- [PR #3945](https://github.com/lightpanda-io/browser/pull/3945) — multiple
  **pages/targets within one context**. Landed; shipped in release 1.0.4.
- [PR #4151](https://github.com/lightpanda-io/browser/pull/4151) — concurrent
  **browser contexts** over CDP (each with its own session, HTTP client,
  storage, permissions, viewport). **Open**: marked ready for review
  2026-11-12, sits on the chain #4150 → #4149; the maintainer's stance on the
  chain (2026-10-23) was "we're not ready to do this just yet". Not in any
  release.
- Independent measurement of the **released 1.0.4** binary over raw CDP
  ([issue comment, 2026-11-15](https://github.com/lightpanda-io/browser/issues/882#issuecomment-6890350964)):
  two `Target.createTarget` calls in one context succeed; a second
  `Target.createBrowserContext` still returns
  `-32000: Cannot have more than one browser context at a time`.

## Verdict: 1.0.4 does not solve our problem

What 1.0.4 fixed (multi-page in one context) is irrelevant to us — we use one
tab per task; what we need is isolation **between** tasks, i.e. many contexts
on one connection (or at least an explicit context that a helper tab does not
block). That is exactly what is still missing, confirmed on the released
binary. Consequences:

- keep `LIGHTPANDA_IMAGE` at 1.0.3 — 1.0.4 changes nothing for us, a bump is
  pointless;
- the `EnginesLive` slice stays red for lightpanda — this is the engine's
  state, not a bench regression;
- no re-testing until the real fix lands.

## Retrigger (when to revisit)

1. Watch [PR #4151](https://github.com/lightpanda-io/browser/pull/4151)
   (and its deps [#4150](https://github.com/lightpanda-io/browser/pull/4150),
   [#4149](https://github.com/lightpanda-io/browser/pull/4149)) until merge +
   release.
2. Then: bump `LIGHTPANDA_IMAGE` via the pinned-version procedure (FR-17/T-254 —
   [`../dev-environment.md`](../dev-environment.md), "Switching the pinned
   engine version") and re-run the isolation smoke:
   `go test ./internal/render -run EnginesLive` against the raised
   `render-engines` profile (the full command — the doc comment of
   `internal/render/engines_live_test.go`).
3. Green slice (all six items, including (b) context isolation) — update the
   lightpanda bullet in `engines_live_test.go` and the bench row in
   `prod-topology.md`, then the engine becomes a canary candidate again.

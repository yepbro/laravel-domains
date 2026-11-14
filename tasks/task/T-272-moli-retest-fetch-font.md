# T-272: the moli v1.1.23 re-test — the upstream fix for Fetch.enable Font

- **Priority:** P1
- **Status:** [done]
- **Stage:** 42 (operational, not code)
- **Procedure source:** the re-test plan from T-247 (operator decision
  2026-11-06, spec v0.3 “watch + smoke per release”); steps —
  docs/dev-environment.md, “Changing a pinned engine version” (smoke
  FR-17/T-254)

## Context

Source: the moli release
[v1.1.23](https://github.com/lexmount/moli/releases/tag/v1.1.23)
(2026-11-09, commit `ee0652d`) carrying our PR
[lexmount/moli#2470](https://github.com/lexmount/moli/pull/2470)
“fix(fetch): accept the Font resourceType in Fetch.enable patterns” (fork
`yepbro/moli`, branch `fix/fetch-enable-font-resource-type`, commit
`4729723`; submitted 2026-11-06, the maintainers tagged it for release
1.1.23) and the paired maintainer #2476 “intercept font and media loads
with shared admission”. Reason for the fix: on pinned v1.1.21/1.1.22 the
CDP slice `TestEnginesLiveCDPSlice/moli` is red — `Fetch.enable` with the
patterns of the T-062 base blocking set (Image+Media+Font) answers
InvalidParams → every task fails at startBlock, engine=moli is
unusable (live run 2026-11-06: 0/100 because of the Font pattern;
precedents — the doc comment of `internal/render/engines_live_test.go`,
the log `.opencode/skills/prod-report/references/prod-topology.md` dated
06.11). An operational task (not code), like T-247…T-249; lightpanda is out
of scope (waiting for upstream #4151 — one browser context per connection).

## Task

P1, operational: bump the moli pin to v1.1.23, the CDP
slice, a smoke run on the seed hundred, a candidate verdict (input for
T-247/T-249; the pin infrastructure and smoke procedures — T-241/T-254). Steps:

1. Bump the pin with the pair `MOLI_VERSION=v1.1.23` + `MOLI_SHA256` (release
   asset `moli-x86_64-unknown-linux-gnu.tar.gz`, sha256
   `df360c04e567ce2e56a8391a03dc677d794465848b5821f944d2de10f3f1f430`)
   in every default location: `deploy/docker/.env.example`,
   `deploy/docker/compose.yaml` (two literals — ARG and the image tag),
   `deploy/docker/Dockerfile.moli` (ARG defaults); on the bench — the pair in
   `.env` (the pin is currently set by the compose defaults; no pair in `.env`).
2. Rebuild/re-up: `dc --profile render-engines up -d moli` →
   the `crawler/moli:v1.1.23` image (a version tag), the healthcheck
   `/json/version` green, the version in the response matches the pin (the
   slice gate prints it itself).
3. Layer 1 — the CDP slice: `dc run --use-aliases -e CRAWLER_RENDER_TEST=1
   --rm tools go test ./internal/render
   -run 'TestEnginesLiveCDPSlice/moli' -v -count=1`. Expectation:
   it goes green (InvalidParams on Fetch.enable goes away with
   #2470+#2476; the stage invariants — spec 5.6/FR-5). Additionally run the set with
   `BlockStylesheet` (T-213): if Stylesheet was still not accepted in moli —
   NOT a blocker for the base profile (the option is optional, default false),
   record it as a separate line for the CSS mode. A red slice is a
   stop signal: revert the variable to v1.1.21, analyze the cause; if
   engine-side — an upstream issue.
4. Layer 2 (with layer 1 green) — a smoke run on the seed hundred per the
   docs/dev-environment.md procedure (steps 3–4 “Real smoke”): `fixtures
   import --force --publish smoke` → the bench render-worker in the
   moli profile (engine: moli + ws://172.28.100.17:9222) → drain
   q:render → `fixtures snapshot --label moli-v1.1.23 --engine moli` →
   `fixtures report` against baseline-chromium. The §5.1 item 4 thresholds
   of the render engines spec (indicative on a hundred; the full 3000×2
   comparison — T-247); the smoke criterion: success rate/throughput without
   degradation against baseline, 0 crashes of the engine core, memory fine;
   between baseline and candidate — restart render-worker and re-run
   `import --force` + `--publish`.
5. Verdict and recording: “ready/not ready now” against the thresholds — the
   input for T-247 (the full bench comparison) and T-249 (the canary);
   update the precedent doc comment of `internal/render/
   engines_live_test.go` (moli v1.1.23 — green/red), the prod-topology.md
   log line (06.11 “waiting for 1.1.23” → the fact), the
   docs/operations.md log.

## Acceptance criterion

The v1.1.23 pin committed in every default location, the slice
and smoke recorded (snapshots/reports in var/fixtures-render), the verdict and
doc edits committed; on red — a revert to v1.1.21 and the next
re-test plan written down (on the next engine release).

## References

- Upstream: release
  [v1.1.23](https://github.com/lexmount/moli/releases/tag/v1.1.23),
  our PR
  [#2470](https://github.com/lexmount/moli/pull/2470),
  the paired maintainer #2476.
- Procedure: docs/dev-environment.md (“Changing a pinned engine version”),
  the re-test plan from T-247; smoke FR-17/T-254.
- Related: T-247/T-249 (the verdict consumers), T-241/T-254 (the pin
  infrastructure and smoke procedures).

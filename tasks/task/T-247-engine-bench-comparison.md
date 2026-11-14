# T-247: bench comparison of the engines and canary-readiness verdicts

- **Priority:** P1
- **Status:** [new] (returned by the operator 2026-11-13 — see “Status history”)
- **Stage:** 35, operational (sequential)
- **Source:** [`TZ_render_engines.md`](../TZ_render_engines.md) §5.1

## Task

P1, bench comparison of the engines and canary-readiness verdicts
(spec §5.1; depends on T-234, T-241, T-243,
T-244; T-254 — engine smoke preliminaries). Not code: runs of the three engines ×
a 3000-domain sample twice (variability control), baseline — a fresh
chromium run on the same sample; the T-244 reports against the §5.1
item 4 thresholds (detection <1%, “engine+fallback” success ≥ baseline minus
engine-independent losses, throughput no worse, memory <⅓ of the chromium
budget, 0 crashes); per candidate — a “ready/not ready now” verdict with
metrics (prod-reports/ or docs). Candidates are NOT cut (operator decision
2026-11-06, spec v0.3): both engines stay permanent candidates,
unready ones get a re-test plan with new pinned versions (smoke
FR-17/T-254), the cadence follows engine releases.

## Acceptance criterion

The reports are saved, a verdict per candidate is explicitly
recorded against the thresholds; for the unready ones — a re-test plan written down.

## Status history

- Returned to [new] on 2026-11-13 (operator): the moli re-test from
  upstream/main f857b6651 — layer 1 green (after T-276), layer 2 NOT
  READY (success 69% vs 92%, detection divergences 2.94%, RSS 736 MiB
  above ⅓ of the chromium budget; 0 crashes) → the retest is scheduled
  with the first moli release carrying ①④. The 13.11 run: the journal
  docs/operations.md 2026-11-13, the artifacts in var/fixtures-l2/
  (moli-main-f857b6651.snapshot.json, report-moli-main-f857b6651.txt).

## References

- Spec: [`TZ_render_engines.md`](../TZ_render_engines.md) §5.1 (item 4 —
  the thresholds).
- Dependencies: T-234, T-241, T-243, T-244; smoke preliminaries — T-254.
- Related: T-248 (the rollout waits for this verdict), T-249 (the canary),
  T-272 (the moli re-test precedent).

# T-248: production rollout of both engine containers and worker B

- **Priority:** P1
- **Status:** [new]
- **Stage:** 35, operational (sequential)
- **Source:** [`TZ_render_engines.md`](../TZ_render_engines.md) §4.7 items 2–3 (v0.3), §6.2 item 1

## Task

P1, production rollout of BOTH engine containers and
worker B (spec §4.7 items 2–3 v0.3, §6.2 item 1; depends on T-247,
T-239, T-240, T-245, T-246). Not code: moli and lightpanda containers with
pinned versions from T-241 (both — regardless of which candidate goes to
canary first; deployed for windows and re-tests without
new deploys), the egress policy of both containers (mandatory — spec
§4.7 item 2), mem_limit from the T-247 measurements × headroom (initially ≤1 GB
each); worker B on q:render-fallback (the T-240 profile) runs against an
empty queue — the wiring check (step 1 of spec §6.2); the T-246 alerts
live in production; the stage baseline metric window recorded (≥48 h); the
spec §6.4 rollback rehearsed on the bench.

## Acceptance criterion

Healthchecks of both engines and worker B green, alerts
active, the baseline window recorded.

## References

- Spec: [`TZ_render_engines.md`](../TZ_render_engines.md) §4.7 (items 2–3),
  §6.2 (item 1 — the wiring check), §6.4 (the rollback).
- Dependencies: T-247, T-239, T-240, T-245, T-246.
- Related: T-249 (the canary follows this rollout).

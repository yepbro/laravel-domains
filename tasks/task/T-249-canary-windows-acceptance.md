# T-249: canary windows, acceptance and locking in the switch

- **Priority:** P1
- **Status:** [new]
- **Stage:** 35, operational (sequential)
- **Source:** [`TZ_render_engines.md`](../TZ_render_engines.md) §5.2, §5.3 (v0.3), §6.2–§6.4

## Task

P1, canary windows, acceptance and locking in the switch
(spec §5.2, §5.3 v0.3, §6.2–§6.4; depends on T-248). The canary is switching
the main worker to a candidate found ready by T-247; if neither is
ready now — T-249 does not start, a T-247 re-test is scheduled
(both engines stay candidates, no cut). Not code: an operator
run following the steps of spec §6.2: enabling fallback without an engine
switch (empty queue — the wiring check) → window 1 (one worker on the candidate,
checkpoints every 4 h: the fallback rate, the share of Laravel verdicts
against the baseline window, the q:render-fallback depth, engine
RSS/stability, the PEL of both streams) → window 2 (100% of workers, 48 h) →
§6.3 acceptance: the fallback balance reconciliation (published = consumed + PEL,
0 mismatches), the §5.3 thresholds, the T-177 log without new pathologies →
locking in: the production profile config (engine, caps from the actual
rate), docs/render.md (profile, rollback runbook, engine version change
procedure), docs/operations.md (the log), pinned versions.

## Acceptance criterion

The §6.2 checklist passed in full, a repeated 48 h confirms
stability, the decision recorded in the docs; on a threshold failure —
the §6.4 rollback executed and documented (a valid outcome is “stay on
chromium”, spec §5.3 item 2).

## References

- Spec: [`TZ_render_engines.md`](../TZ_render_engines.md) §5.2, §5.3,
  §6.2–§6.4.
- Dependencies: T-248; the ready-candidate verdict — T-247.
- Related: T-177 (the pathology log), docs/render.md, docs/operations.md.

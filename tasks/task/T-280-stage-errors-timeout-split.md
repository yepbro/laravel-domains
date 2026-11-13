# T-280: Split the HTTP-stage timeout metric (connect/tls/tarpit/ctx-in-queue)

- **Priority:** P2
- **Status:** [new]

## Context and symptoms

- 92.7% of the HTTP-stage errors are a single counter
  `crawler_stage_errors_total{type="timeout"}`: it mixes
  - the remote failures (a connect timeout, a tarpit — a property of the
    dead corpus),
  - the self-inflicted ones (the ctx expired while waiting for the per-IP
    semaphore: the code path ctx.Err → DeadlineExceeded → ErrTimeout is
    confirmed).
- The 13.11 investigation: per-IP parking — 30% of the waits >2 s (the
  T-165 norm <5%), 21% >30 s. Per-check errors are not written to the
  journal — only the aggregate, so the share of the local loop (A) against
  the dead corpus (B) cannot be measured.

## Root cause

The timeout-cause classification does not reach the metrics: one series for
all the mechanisms whose owners are different (the network/corpus vs our
per-IP queue).

## Requirements

1. Introduce a reason label (or separate series):
   connect_timeout / tls_timeout / tarpit / ctx_expired_in_queue.
2. Classify at the point where the error arises (not post-factum by a
   string).
3. Read compatibility: the old `type="timeout"` series either stays the sum
   or is marked deprecated in the docs.
4. Update the metrics documentation (the dictionary) and, if needed, the
   dashboard panel of the HTTP stage.

## Acceptance criteria

- /metrics exposes the split counters; after the deploy the real cause
  breakdown is visible on production (expected: tarpit/connect — the
  majority, ctx-in-queue — single-digit percents outside the storms).
- The watch reports can compute the A/B balance by the counter diffs.
- `make test` + `make lint` green (+ `make test-race` for
  render/httpworker per the Makefile rules).

## References

- The temp_error investigation of 13.11.2026 (a subagent report, the main
  session's session; the digest in T-278)
- T-165 (the per-IP semaphores and their norm of <5% of the waits >2 s),
  T-148 (tarpit), T-278 (the retry backoff — the metric's consumer)

# T-278: Progressive temp_error retry backoff (24 h → 72 h at attempts≥2)

- **Priority:** P1
- **Status:** [new]

## Context and symptoms

- 12–13.11.2026 the temp_error share is anomalous: 63–69% of the checks
  against the normal 23–40% (09–11.11: 40/23/36%). The production
  investigation of 13.11 (a subagent: the journal + metrics + curl/dig
  probes): 92.7% of the HTTP-stage errors are timeouts, and the provable
  part of them are properties of the remote sides (SYN silence, a tarpit
  after the handshake with a healthy local network); ⅓ of temp_error is
  produced by the DNS stage (broken upstream zones, SERVFAIL).
- 67% of the temp_error domains failed ≥2 times in a row (temp_attempts:
  1 — 297k, 2 — 266k, 3 — 140k) — the failures are persistent, not
  transient.
- retry_mature 462–464k/day: the fixed retry_after (24 h, at most 3
  retries) drives the dead corpus in a circle up to 4 times — up to ⅔ of
  the repeated load on q:http and PG.
- The background: the series of PG memcg OOM kills and quiet windows of
  11–13.11 (see T-279) — reducing the repeated load also weakens this
  pattern.

## Root cause

A fixed retry_after does not distinguish transient and persistent failures:
dead domains are re-enqueued at the same frequency as live ones, and per-IP
parking (T-165) adds self-inflicted timeouts to the same stream.

## Requirements

1. A progressive retry_after backoff: attempts=1 → 24 h (the current
   behavior), attempts≥2 → 72 h; the parameter — a config key.
2. Successful checks reset the attempt counter (the behavior for live
   domains does not change).
3. Consider extending per_ip_overrides for the parking prefixes (the T-165
   mechanism already exists) — it cuts the per-IP waits over dead IPs.

## Acceptance criteria

- The marginal retry_mature/checks share drops ≥2× (by the crawler_counters
  counters over a day after the deploy).
- The temp_attempts distribution shifts toward 1; the q:http inflow
  (domains/s) at the same check pace does not grow.
- `make test` + `make lint` green (+ `make test-race` if the scheduler is
  touched); `make test-integration` if the queues/DB are touched.
- A day on production without new PG OOM kills (all other things equal,
  together with T-279 — not the only factor).

## References

- prod-reports/2026-11-12-110746.md (the 12.11 report, the temp_error
  anomaly)
- prod-reports/2026-11-12-165812.md (the 13.11 window, the OOM recurrence)
- docs/operations.md §4.6; T-165 (per-IP), T-277 (the passport TTL —
  a related dead-data cleanup)

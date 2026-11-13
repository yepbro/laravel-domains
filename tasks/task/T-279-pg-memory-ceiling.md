# T-279: A PostgreSQL memory cap against the OOM kills

- **Priority:** P1
- **Status:** [new]

## Context and symptoms

- A series of memcg OOM kills of the postgres backend at full front
  (CONSTRAINT_MEMCG, the postgresql@16-main cgroup): 11.11 21:10,
  12.11 03:14, 13.11 02:32 — a ~4–5 h interval. Each time the treatment is
  the §4.6 quiet window (symptomatic; the 13.11 window went through
  normally, the front was re-raised by the watchdog after 39 min).
- PG is the server's main RAM consumer (4 vCPU / 8 GiB): backends at
  250–890 MB RSS, the top swap consumers are postgres too; available drops
  to ~1.2 GiB, swap 2–3 GiB, swapping up to 76 MB/s.
- The current PG config: shared_buffers = 2 GB, work_mem = 16 MB
  (× the sorts in every backend), maintenance_work_mem = 256 MB,
  30+ connections at the peaks.
- The kill pattern: “a long retention/analytics SELECT + a full front”
  (see the red flags of the prod-report skill).

## Root cause

The total PG footprint (shared_buffers + the backends × work_mem +
maintenance_work_mem) hits the unit's cgroup limit under peak queries; the
limit/config are not balanced against the VM budget
(http-worker ~0.7 GiB, unbound ~0.7 GiB, chromium + the engines ~1 GiB,
the OS and the caches — the rest).

## Requirements

1. An audit: the actual cgroup limit of the unit, the observed PG RSS peaks,
   the number/types of the connections and heavy queries at the peaks.
2. Choose and carry out one path (or a combination):
   - tighten work_mem / maintenance_work_mem / cap max_connections and the
     applications' connection pools;
   - revisit shared_buffers for the real working set;
   - revisit the memcg limit with an explicit VM budget.
3. The changes — via config/deploy, with a pipeline-speed measurement
   (domains/s) before and after; the rollback — the same change.
4. Update docs/db.md and the memory budget in docs/load-measurements.md.

## Acceptance criteria

- 24–48 h of continuous full front without PG OOM kills (watch ticks:
  “OOM 0”), without manual quiet windows.
- available at the peaks not below ~1.2 GiB; the swapping is not of a
  permanent nature (vmstat si/so).
- The pipeline pace (domains/s) no worse than −10% of the baseline.
- `make test` + `make lint` green; `make test-integration` if the testbed/
  environment configs are touched.

## References

- docs/db.md, docs/operations.md §4.6, docs/load-measurements.md
- prod-reports/2026-11-11-051557.md, 2026-11-12-110746.md,
  2026-11-12-165812.md (the chronology of the kills and windows)
- T-278 (reducing the repeated load — a related lever)

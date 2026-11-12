# Technical specification: the load report “Queues” line — queue group backlog instead of raw XLEN

Version: 1.0. Task T-188, stage 29. Source — a mismatch of numbers
found by the operator on 2026-10-25 (the incident analysis in
`tasks.md`, stage 29).

---

## 1. Current behavior

The “📬 Queues” line of the load report (T-173) prints the **raw XLEN**
of each stream — the total number of entries in the Redis Stream, including
entries already delivered and acknowledged (XACK) but not yet trimmed by
XTRIM MINID (T-141). The source — `RedisQueues.Queues`
(`internal/monitor/load.go`, the `QueueStat{Len, Read}` type):

```go
length, err := s.RDB.XLen(ctx, stream).Result()   // Len = XLEN
...
if groups, err := s.RDB.XInfoGroups(ctx, stream).Result(); err == nil {
    for _, g := range groups {                    // Read = max entries-read
        if g.EntriesRead > st.Read { st.Read = g.EntriesRead }
    }
}
```

The line format — `formatLoadReport` (same file): the current `Len` +
the `Len` delta over the window (±N) + the delivery rate from the
`Read` delta (N/s).

The other consumers of the same concept compute it differently — the
**consumer-group backlog** `min(lag, XLEN) + pending` over all groups (the
formula of the `BackpressureGate.Len` gate, `internal/queue/backpressure.go`):

| Consumer | Formula |
|---|---|
| `crawlerctl status` (T-079, `internal/status`) | gate: min(lag, XLEN) + pending |
| the `crawler_queue_depth` metric (workers, T-077) | gate (the same source) |
| the T-162 frozen-queue alert (`RedisStreamGroups`) | gate |
| the “Render debt … lag” line (T-175) | gate |
| **the “📬 Queues” line of the load report** | **XLEN — diverges** |

## 2. Diagnosis (a live measurement of the incident 2026-10-25 ~13:25 MSK)

Production 155.212.174.88, revision c791ef5. The 24.10 freeze stopped
`crawler-scheduler`, and with it TrimLoop (T-141) — acknowledged history
stopped being trimmed, XLEN accumulated ~17 h of untrimmed tail:

| Stream | Report (XLEN) | XINFO GROUPS: lag + pending | `crawlerctl status` |
|---|---|---|---|
| q:dns | 2 | 0 + 0 | 0 |
| q:http | 952 | 0 + 0 | 0 |
| q:render | 136 500 | 27 409 + 120 | 27 580 |
| q:analyze | 71 784 (+654 over the window) | 0 + 1 | 0 |
| q:dead | 1 441 | no groups | 1 441 |

Consequences for the operator:

- q:render is inflated ~5× (136.5K against a backlog of 27.5K);
- q:analyze shows **phantom growth** (+654 over the window, 1.1/s
  deliveries): the analyzer keeps up with the stream (lag 0), acknowledged
  history accumulates — “the queue grows” with an empty backlog;
- the “Render debt: … lag 27 893” line in the same report contradicts the
  “Queues” line (136 500) for the same stream.

Note: normally (scheduler up) XLEN converges to the backlog within the
`redis.trim_interval` window, but the report's consistency must not
depend on trimming working.

## 3. Target semantics

`QueueStat.Len` becomes the stream backlog depth — exactly like
`BackpressureGate.Len`:

1. **Groups exist**: `Len = Σ over groups (min(lag, XLEN) + pending)`;
   `lag = −1` (undefined, Redis < 7.0 after trimming undelivered entries,
   the T-168 clamp) — only `pending` contributes.
2. **Stream exists, no groups** — `Len = XLEN` (like the gate: no entry
   has been delivered, accumulation without a consumer must be visible;
   the “territory” of the T-162 alert).
3. **No stream** (no such key) — `{0, 0}` (as now).
4. **q:dead** — always XLEN: the quarantine has no groups by construction,
   the depth is the operator's inventory (the same semantics in
   `DeadDepthMetrics` T-154 and `crawlerctl status`).
5. `Read` (the delivery rate) — max `entries-read` over groups, unchanged.

The ±N window delta becomes the growth/drain of the **real backlog**
(today — a garbage stream-length delta). Example after the fix:
`q:analyze 0 (±0, 1.1/s)` — deliveries flow, the queue is empty.

## 4. Code changes

`internal/monitor/load.go`, `RedisQueues.Queues` (≈ lines 402–448):

- the method already calls both `XLen` and `XInfoGroups` — no new Redis
  calls: the backlog is computed in the same loop over the already-read
  `groups` and `xlen`;
- for item 3 of the semantics XLEN is read when there are no groups too
  (currently it is always read — keep);
- **do not duplicate the formula**: extract a shared `depth(groups, xlen)`
  calculation and use it here and in
  `RedisStreamGroups.StreamGroups` (`streamgroups.go`) — the formulas of
  the gate, the alerts and the report are not allowed to diverge (the
  invariant from the streamgroups.go header: “the source must agree…”);
- update the `RedisQueues` doc comment (currently “on top of XLEN + XINFO
  GROUPS”) and the comment of the “Queues” block in `LoadReportLoop`
  (≈ 653).

`formatLoadReport` is unchanged (prints `Len` as is).

## 5. Tests

`internal/monitor/load_test.go` (package unit style — the
`redisGroupsCmd` fakes):

- `TestRedisQueues_Backlog` (new): XLEN 136 500, a group with
  lag 27 409 + pending 120 → `Len = 27 529` — history in XLEN does not
  inflate the depth;
- several groups → the sum of contributions; `lag = −1` → pending only;
- no groups, stream exists → XLEN; no key → `{0, 0}`; q:dead → XLEN;
- `TestFormatLoadReport_Deltas` / `TestLoadReportLoop_OnePass` —
  update the line fixtures to the new `Len` semantics (the fixture
  numbers simply become backlogs, the format is unchanged).

## 6. Documentation

- `docs/services.md` — the T-173 report line description (the block about
  the “raw XLEN” near current lines 224–236) and the metric table: state
  that the “Queues” line = backlog, the source shared with
  `crawler_queue_depth`;
- `docs/operations.md` — the notification section (≈ 265): an example of
  reading the line (the delta = backlog growth/drain, the rate = deliveries);
- `docs/redis.md` — where XLEN vs the backlog is mentioned, add a
  reference to consumer consistency (in place, at the implementer's
  discretion).

## 7. Acceptance criteria

1. Dev stack (`make up` + `make seed`): the “Queues” line of the monitor
   log matches `make status` (both — backlog; q:dead — XLEN).
2. In production it does not depend on the trim state: with the scheduler
   stopped the line shows the backlog (verified on the live numbers of §2).
3. `make test`, `make lint`, `make test-integration` — green.

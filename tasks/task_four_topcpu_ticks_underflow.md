# Technical specification: the load report top-CPU — tick underflow on process restart

Version: 1.0. Task T-189, stage 29. Source — the production load report
of 2026-10-25 (production 155.212.174.88, revision c791ef5).

---

## 1. Symptom

The “🧮 Top CPU” line of the load report (T-173):

```
🧮 Top CPU: postgres 310613997334638.2c, headless-shell 0.8c, render-worker 0.1c
```

The postgres “cores” — 3.1·10¹⁴ on a 4-core host. The adjacent report
(~10 min away) was correct: `postgres 0.6c`. In the same window the
postgres process count changed: 18 → 16 (the per-name aggregate in “Top
memory”) — the process was restarting (connection recycling/a restart of
part of the DB processes).

## 2. Diagnosis

`internal/monitor/load.go`:

- `ProcAgg.Ticks uint64` (≈ 93–95) — the sum of the CPU ticks
  (utime+stime) of all processes of a **name**; the `Load` snapshot sums
  by name (≈ 342);
- the window delta in `LoadReportLoop` (≈ 643–651):

```go
if dt := agg.Ticks - prev.ticks[name]; dt > 0 {   // uint64!
    procCPU[name] = float64(dt) / perCore
}
```

`agg.Ticks − prev.ticks[name]` — **unsigned subtraction**: after a
restart of a name's processes the per-name tick sum drops (new processes
start with small utime+stime), `dt` underflows to ≈ 1.8·10¹⁹, the
`dt > 0` guard lets the garbage through, and the division by `perCore`
(the ticks of one core over the window, ~10⁴–10⁵) yields ~10¹⁴ “cores”.
The symptom numbers match in order of magnitude.

The same problem class is impossible for `cpuDelta` (/proc/stat, a
monotonic kernel counter) — only the per-process-name aggregate suffers.

## 3. Solution

Compare before subtracting; compute the delta only for names that grew
monotonically — a name restarted within the window is honestly skipped
(neither 0, nor garbage):

```go
for name, agg := range st.Procs {
    if agg.Ticks <= prev.ticks[name] {
        continue // the name's processes restarted/disappeared: ticks are non-monotonic — the window is skipped
    }
    procCPU[name] = float64(agg.Ticks-prev.ticks[name]) / perCore
}
```

Rejected: clamping the value from above by the core count — treats the
symptom and hides the cause; storing individual PIDs — extra complexity
with no benefit to the operator (the top is by names).

## 4. Tests

`internal/monitor/load_test.go`:

- an extension of `TestLoadReportLoop_OnePass` (or a separate case
  `TestLoadReportLoop_ProcRestart`): the first pass — postgres Ticks
  100 000; the second — postgres Ticks 1 000 (a restart) with the other
  names alive: postgres is **absent** from top-CPU, the other names are
  correct, no garbage values (> the core count × a reasonable margin);
- a format regression: the top value must not reach 10¹⁴ (a fixture
  sample — `%s %.1fc` from the symptom).

## 5. Acceptance criteria

1. `make test`, `make lint` — green.
2. On the dev stack, a postgres restart between report passes produces no
   garbage top lines (the name simply disappears from the top for one
   window).

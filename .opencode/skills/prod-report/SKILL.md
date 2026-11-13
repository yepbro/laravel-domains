---
name: Prod report (larasites)
description: A summary report on the state of the Domain Crawler production — the larasites server (155.212.174.88): CPU/RAM/disk load, depth and lag of the q:dns/q:http/q:render/q:render-fallback/q:analyze/q:dead queue groups (the consumer pending, not XLEN), pipeline speed and progress, how many domains are checked, content retention/compaction, systemd unit status (running/failed), render engines and fallback (after the stage 35/36 deploy). Run it when asked to assess the production health: "give a load report", "what's up with prod/the server", "what is the queue speed/depth", "why are disk/memory/lags growing", "how many domains are checked", a mention of larasites, dashboard.laracity.ru or 155.212.174.88 in a status/metrics context. The skill shows only aggregated metrics and is NOT suitable for log work: "show the last log lines", journal tail/grep, "unit logs for yesterday", searching for errors in logs — journal-reading tools are needed for that, not this skill. The standing-watch mode is covered too: "keep an eye on prod", "check once an hour / every 20 minutes", "monitor continuously, you may stop/start the queue yourself" — periodic light snapshots (scripts/hourly-snap.sh), compact diff reports, and queue stop/start autonomy per the skill criteria.
---

# Prod report: larasites (Domain Crawler)

The skill collects a one-off snapshot of the production state and formats
it as a report.
Prod is the project's only production server; the deploy is native systemd
(NOT docker — do not look for containers).

## Safety rules

1. **Data collection is read-only.** No unit `restart`/`stop`,
   no `DEL`/`XTRIM` in Redis, no vacuum, no `--vacuum-size`, no psql
   writes — without an explicit user command. The report ends with
   recommendations and a question, not actions.
2. **`crawler-scheduler` may be stopped intentionally by the operator**
   (this happened on 03.11.2026). On finding it `inactive` — report it
   and ask, do not "fix" it.
3. SSH — only via the `larasites` alias (root, key). Do not use direct
   IPs or passwords.
4. All heavy queries (psql, two queue snapshots) are gentle anyway: one
   `collect.sh` run ≈ 25–30 s. Do not run it more often than necessary.
5. **The standing watch mode (section below) may extend these rules by an
   explicit user grant** to stop/start `crawler-scheduler` — and only it,
   only within the criteria of that section. Rule 2 ("ask, do not fix")
   applies unless the grant covers the situation.

## Procedure

1. Run the collector:
   ```bash
   <base-dir>/scripts/collect.sh              # snapshot interval 20 s
   collect.sh --interval 45                   # more precise queue speed
   ```
   The script goes over ssh to `larasites` and prints a markdown snapshot
   in sections: System, Units, Top CPU, Queues (with rate calculation),
   Redis, Pipeline (crawlerctl), PostgreSQL, Content, Metrics,
   Render (engines and fallback, after the stage 35/36 deploy), Logs.
2. Read the snapshot and interpret it: what is normal and what is not —
   using the red flags table below.
3. On anomalies, open `references/prod-topology.md` — it has the prod
   topology, targeted commands for going deeper on each block, and known
   incidents. Go deeper only on the suspicious sections; do not run all
   over the server.
4. Format the report using the template below.
5. **Output the report to the chat in full AND save the same text to a file**
   `prod-reports/YYYY-MM-DD-HHMMSS.md` — the path is from the repo root; the
   name is the report generation date-time in the local timezone, seconds
   are mandatory (example: `prod-reports/2026-11-03-193215.md`). After the
   report, state the path to the saved file on a separate line. The
   `prod-reports/` directory already exists in the repo root and is
   entirely ignored by git (it has its own `.gitignore`)
   — the reports are local and never reach commits.
6. Finish with a question about further actions.

## Standing watch (continuous monitoring)

Launched when the user asks to watch prod with a period ("check once an
hour", "every 20 minutes") — usually with a grant to decide on stopping/
starting the queue (`crawler-scheduler`). One-off report rules stay for
one-off reports; this section adds the loop and the autonomy.

### Procedure

1. Baseline snapshot right away:
   `<base-dir>/scripts/hourly-snap.sh` — a light diff-grade snapshot
   (~15–20 s, no double queue cuts; collect.sh stays for one-off reports).
2. Background timer per the requested period:
   `sleep <N> && <base-dir>/scripts/hourly-snap.sh`. On each tick:
   analyze → decide (criteria below) → post a compact diff-report →
   relaunch the timer.
3. Quiet-hour format: a 5–6 row diff table (load / PG OOM / checks rate /
   retention cursor / disk+content / queue depths) + "no action
   required". Do NOT write hourly files into `prod-reports/` — full
   reports and files only on anomalies, actions taken, and the final
   summary of the watch.
4. Any prod action — with a one-line justification in the report.

### Autonomy criteria (only under an explicit user grant)

| Decision | Criterion | Action |
|---|---|---|
| STOP the front | ≥1 PG cgroup OOM kill per hour with the front active (the "long retention select + full front" pattern) | `systemctl stop crawler-scheduler`, open a drain window: `<base-dir>/scripts/drain-guard.sh [cap-min]` — the guard restarts the front itself on pass completion or the cap |
| STOP the front | disk ≥ 80% or content growing >1 GB/h with retention stalled | same: a calm window per `docs/operations.md` §4.6 + drain-guard |
| START the front | the stop criterion is gone (the retention cursor advances / a pass completed, OOM kills 0, the disk shrinking) | `systemctl start crawler-scheduler` |
| START the front | the scheduler is inactive with no active reason while the system is healthy (e.g. left after a deploy) | start it, report the fact |

Safeguards: every stop window gets a self-restart cap (drain-guard,
default 90 min) — the front must never stay down forever; nothing beyond
`crawler-scheduler` is stopped/started without a separate user command.

### Recovery after an agent-server restart

Background timers and the guard die on opencode server restarts
(a recurring environment fact). On resuming a session with an open
standing watch: take an immediate `hourly-snap.sh`, then relaunch the
timer; if a stop window was open — decide per the criteria above whether
to start the front (healthy system → start; the drain criterion still
holding → re-arm the guard).

## Key formulas (do not mix up!)

- **Queue backlog = group `lag` + `pending`** (in-flight without ACK).
  `XLEN` is NOT the backlog: the stream keeps the history of processed
  entries until XTRIM. Incident 03.11.2026: XLEN q:render = 150k, the real
  backlog (lag) = 9.9k — an error of 15x. `crawler_queue_depth` in the
  metrics is computed correctly: `min(lag, XLEN) + pending`.
- **Consumption rate** = group Δ`entries-read` / Δt (two snapshots).
- **Inflow** = (Δ`entries-read` + Δ`lag`) / Δt — a negative Δlag means
  the backlog is draining.
- **Compare q:render between snapshots ONLY taking the render debt into
  account.** While the q:render depth is below `render.queue_high_watermark`
  (T-174, 10k in prod), the `RequeueRenderPending` rework pass in the
  http-worker (tick ~1 min, a batch of up to 100 domains, T-066)
  replenishes the queue from domains with the `render_pending` flag —
  the lag stays near the threshold and barely changes until the debt is
  exhausted: "the lag is not melting" ≠ a jam. The full render debt =
  lag(q:render) + `render_pending` (a crawler_counters counter; the
  monitor metric `crawler_render_pending_domains`). Compute the trend and
  the ETA ("the backlog will clear in N") from the sum; a forecast from
  the lag alone is correct only at render_pending = 0.
- `q:dead` is a graveyard without a consumer: only XLEN applies to it,
  there is no lag.
- **q:render-fallback is the sixth stream (after the stage 35/36 deploy —
  see the "Render engines and fallback" section below).** There is a
  consumer (the `render-fallback-worker` group, worker B) — lag/rate/
  inflow are computed as for the working streams; this is not q:dead. An
  empty queue at engine=chromium is the engine guard: the fallback is
  published only by candidate engines (moli/lightpanda), chromium does
  not fall back to itself; an empty queue with B alive is normal while
  there are no canaries. With a canary the target fallback rate is
  ≤15% of the stage flow; the depth must be ≪ 15,000 (the stream MAXLEN):
  at the cap XADD truncates the undelivered entries — task loss.
- **Fallback balance check (acceptance reconciliation):**
  `increase(crawler_render_fallback_published_total)` (worker A)
  = `increase(crawler_render_fallback_consumed_total)` (worker B)
  + depth(q:render-fallback) + `increase(buried)` — see the section below.

## Red flags

| Flag | Threshold / sign | Where to dig |
|---|---|---|
| Load | ≥ 2× vCPU (8) sustained | Top CPU: `headless-shell`/`postgres` in rendering — normal (baseline measurements — docs/load-measurements.md); after the 35/36 deploy — also the moli/lightpanda engines (budget ≤1 GB each) and worker B (+256M, a second headless-shell — see the "Render engines and fallback" section) — also normal; an anomaly is an unfamiliar process |
| Memory | swap occupied and growing | `postgres` with a large RSS, chromium copies; unit MemoryHigh |
| Disk `/` | ≥ 85% or < 10 GB free | retention/compaction: the http-worker journal (the "pack compaction pass" line, retention skips), `/var/log` |
| Queue | lag growing (inflow > consumption) | Is the worker alive (`systemctl`), the render-worker WARN "render tarpit" |
| q:render | the lag near ~10k (`render.queue_high_watermark`) is not melting | Normal at render_pending > 0: the T-066/T-174 rework pass replenishes the queue from the debt — a jam only at zero consumption; watch the trend via lag + render_pending (see the formulas) |
| q:render-fallback (after the 35/36 deploy) | depth growing toward 7,500 (the `render_fallback_depth` alert, the MAXLEN cap 15,000, cleared < 6,750) | Worker B rate (`render.fallback.worker_rate`, the crawler-render-fallback-worker unit, metrics :9120) against the publication flow; at the 15,000 cap XADD truncates the undelivered entries — task loss |
| suspended{gate\|budget} (after the 35/36 deploy) | growth of `crawler_render_fallback_suspended_total` | gate — the main engine (moli/lightpanda) is unavailable: a browser-error streak closed publication until recovery; budget — the share of publications per window exceeded the `render.fallback.share_percent` cap; decide on the engine (fix/roll back to chromium), tasks are not lost meanwhile |
| render_browser_errors (after the 35/36 deploy) | a streak of growing render browser errors (alert) | A CDP engine hang with the engine container/unit alive: the engine status, the worker Warn logs; if it does not clear quickly — roll back to chromium |
| Entries in q:render-fallback at engine=chromium | XLEN > 0 with the build engine chromium | Engine guard: chromium does not fall back to itself — there must be no publications; verify `render.engine` and the worker A build |
| Pipeline | rate 0 domains/s but pending > 0 | `systemctl is-active crawler-scheduler` — if it is stopped, see rule 2 above |
| q:dead | growing | Worker journals: retries exhausted (`redis.max_attempts`) |
| Counters | `checks:<today>` = 0 with the front active | scheduler → dns-worker journal, the enqueue chain |
| Logs | journal > 1 GB or syslog growing | Is the spam back? Is the `crawler-chromium.service.d/10-logfilter.conf` drop-in in place? |
| PG | `n_dead_tup` of contents/subdomain_results — millions | Autovacuum cannot keep up (docs/db.md) |
| PG OOM kills | ≥1 cgroup OOM kill of postgres per hour with the front active (often a long retention select + full front) | The tight PG memory ceiling (F2, the operations.md journal); a calm window per docs/operations.md §4.6 + scripts/drain-guard.sh; with the persisted cursor (T-274) restarts no longer reset the drain |
| scheduler active unexpectedly | active although the operator had stopped it | Deploys restart crawler.target (install) — re-open the stop window after a deploy if it must survive it (docs/operations.md §4.6) |

## Render engines and fallback (stage 35/36)

**A single caveat: before the stage 35/36 deploy the new entities DO NOT
exist in prod** —
q:render-fallback is absent from Redis, there is no crawler-render-fallback-worker
unit, nobody listens on port 9120, there are no moli/lightpanda engines; the
engine/fallback metrics in /metrics are absent or sit at zero without growth
(the pre-created S-P1-4 series; the published series — an open dictionary —
is never born). This is NORMAL, not an anomaly (the code is merged into
master, prod awaits the push to production); all the rules of this section
apply after the deploy —
no need to repeat the caveat in every line.

- **Acceptance balance check** (step 4 of the render-fallback spec §6.2):
  `increase(crawler_render_fallback_published_total)` (worker A, :9105)
  = `increase(crawler_render_fallback_consumed_total)` (worker B, :9120)
  + `crawler_queue_depth{queue="q:render-fallback"}` (the depth — the backlog
  of group B: lag + pending) + `increase(crawler_queue_dead_buried_total
  {queue="q:render-fallback"})`. Compare by increments between restarts
  (the counters are per-process); with the queue drained the remainder is
  zero; the equality holds while the depth is below the MAXLEN (15,000).
- **`/metrics` sources of the render workers with the `engine` label**
  (chromium|moli|lightpanda): `crawler_render_records_total
  {rendered,engine}` — engine shares (the rendered="true" branch — render
  workers, "false" — the HTTP stage without the label), tarpit
  finalizations —
  `crawler_render_tarpit_finalized_total{engine}`, the fallback —
  `crawler_render_fallback_{published,consumed,suspended}_total`.
  Publications/suspensions are exposed by worker A (:9105), consumption —
  only by the B build.
- **Worker B metrics port — 9120** (outside the render-worker@ instance
  range). If 9110 shows up in old notes — OUTDATED after the S-P2-3 fix;
  the correct port is 9120.
- **The "Render engines" dashboard panel** (site): rate/success/
  tarpit rate/RSS per engine; the fallback row — the fallback rate (the
  share of publications from stage pages), the publication/consumption
  rates, the consumed outcomes, the q:render-fallback depth.
- **Load block:** after the deploy the moli/lightpanda engines
  (budget ≤1 GB each) and worker B (+256M; a second
  headless-shell — in docker-runtime the render-fallback container with
  its own browser, natively the shared crawler-chromium) appear in the top
  CPU/RAM — include them in the normal-state assessment.

## Report template

Exact section headings (a comparison with the previous report — a plus,
if available):

```markdown
# Report: prod larasites (155.212.174.88), DD.MM.YYYY HH:MM MSK

## 1. Server load — a one-sentence verdict
| Metric | Value | Assessment |
|---|---|---|
(load / vCPU, RAM + swap, disk, the key CPU consumers; after the
stage 35/36 deploy the moli/lightpanda engines and worker B in the top are
normal, see the Load flag)

## 2. Queues
| Queue | Backlog (lag) | Pending | Consumption | Inflow | Assessment |
|---|---|---|---|---|---|
(q:dns/q:http/q:render/q:render-fallback/q:analyze/q:dead; the lag trend —
growing/declining; for q:render — always taking render_pending into
account, see the formulas; q:render-fallback — after the 35/36 deploy,
the rules are in the "Render engines and fallback" section)

## 3. Pipeline progress
X of Y million checked (Z%), the domain rate over 24 hours (the checks:
counter), the largest zones not yet crawled

## 4. Anomalies and root causes
a numbered list; each anomaly — with evidence from the snapshot
(a metric value, a log line, a counter)

## 5. Recommendations
a numbered list; what can be done and what it will give;
production actions — only after user confirmation
```

If the user asked narrowly ("queues only" / "disk only") —
run the collector in full (it is cheap), but put the requested block
first in the report; the rest — briefly, in one or two lines.

## References

- `references/prod-topology.md` — the server, the pipeline, the metrics
  ports, targeted deep-dive commands, known incidents and their signs.
- In the repo: `docs/runbooks.md`, `docs/operations.md` (§4.6 — the
  "calm window" runbook), `docs/redis.md`,
  `docs/content.md` (retention/compaction), `docs/db.md`,
  `docs/load-measurements.md` (baseline performance measurements).

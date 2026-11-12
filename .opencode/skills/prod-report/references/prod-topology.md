# larasites prod topology and deep-dive commands

Server: `ssh larasites` → root@155.212.174.88 (pulpirzvk.fvds.ru, IXcellerate).
4 vCPU / 8 GB RAM / NVMe 79 GB, Ubuntu 24.04.5, kernel 6.8.
Load 12–17 during active rendering is a known working mode (the project
deliberately overloads the CPU with renders; docs/load-measurements.md).

## Deploy (native systemd, NOT docker)

- Binaries: `/opt/crawler/bin/` (ingest, scheduler, dns-worker, http-worker,
  render-worker, analyzer, monitor, web, dashboard, **crawlerctl**).
- Service working directory: `/var/lib/crawler` (content — `content/`,
  incoming — `incoming/`).
- Environment: `/etc/crawler/crawler.env`; config: `/etc/crawler/config.yaml`.
- Units: `crawler-{ingest,scheduler,dns-worker,http-worker,render-worker,
  render-worker@9109,render-fallback-worker,analyzer,monitor,web,dashboard,
  chromium}` (render-fallback-worker — worker B, appears after the stage
  35/36 deploy; before the deploy there is no unit — normal), plus
  `redis-server`, `postgresql@16-main`, `unbound`, `nginx`, `fail2ban`.

### The crawler.env pitfall

The line `CRAWLER_HTTP_USER_AGENT=... (+https://...; ...)` contains parentheses —
`source` in bash fails on it with a syntax error, and the variables after it
are not loaded. For `crawlerctl status` the early lines are enough (the DSN
is on lines 2–3), so the working invocation is:

```bash
set -a; . /etc/crawler/crawler.env 2>/dev/null; set +a
/opt/crawler/bin/crawlerctl status          # zones… totals, queues, speed, cycle
```

## Pipeline and queues (Redis Streams)

**Before the stage 35/36 deploy there is no fallback branch in prod** (no
q:render-fallback stream in Redis, no worker B unit, no port 9120; the
engine/fallback metrics in /metrics — absent or zero series without growth,
including the pre-created suspended/consumed zeros): this is normal, not
an anomaly — the code is in master, prod awaits the push to production;
the rules of this block apply after the deploy.

```
scheduler ──> q:dns ──> dns-worker ──> q:http ──> http-worker ─┬─> q:analyze ──> analyzer ──> PostgreSQL
                                          │                    └─> q:render ──> render-worker ─┬─> q:analyze
                                          │                                        │ engine-dependent render failures
                                          │                                        │ (tarpit T-148 / task class T-172
                                          │                                        │  on the last delivery, after the
                                          │                                        │  stage 35/36 deploy)
                                          │                                        └─> q:render-fallback ──> render-fallback-worker
                                          │                                                                     (worker B, chromium, rate cap
                                          │                                                                      render.fallback.worker_rate)
                                          │                                                                        └─> q:analyze
                           (retries exhausted ──> q:dead — no consumer, manual triage)
```

Groups: `dns-worker`, `http-worker`, `render-worker`,
`render-fallback-worker` (after the 35/36 deploy), `analyzer` — one
per stream. The message field — `subdomain_results` (DNSBatch/HTTPTask/Result).

The fallback branch (after the deploy): with `render.fallback.enabled` on,
engine-dependent render candidates are not finalized but are published
by worker A to q:render-fallback; worker B (the same binary, chromium)
reworks them at the `render.fallback.worker_rate` rate: success —
`rendered = true` + q:analyze, exhaustion — the `render_failed_at`
marker + q:analyze. **The health gate/share budget** — protection against
a publication storm: a browser-error streak (the gate — the main engine
is unavailable) or exceeding `render.fallback.share_percent` for a window
(the budget) suspends publications — tasks are NOT lost in the process:
they are finalized with the marker as usual, the browser class waits in
the q:render redelivery. At engine=chromium there are no publications at
all — the engine guard
(chromium does not fall back to itself, an empty queue is normal).

- Depth: `min(lag, XLEN) + pending` — `crawler_queue_depth` computes it
  the same way.
- Two `XINFO GROUPS` snapshots (Δentries-read) → the rate. See the
  formulas in SKILL.md.
- XINFO GROUPS --raw, one group, line positions (Redis 7.4):
  2=name, 4=consumers, 6=pending, 8=last-delivered-id, 10=entries-read, 12=lag.
- Backpressure: the common `scheduler.queue_high_watermark` (100k), while
  q:render has a separate `render.queue_high_watermark` (T-174, 10k in
  prod, the `render:` section of `/etc/crawler/config.yaml`). Above the
  threshold SPA entries are not published, and the domain gets the flag
  `render_pending`
  (bit 1 of `domains.flags`).
- Replenishing q:render from the debt (T-066): while the stream depth is
  below
  `render.queue_high_watermark`, the `RequeueRenderPending` rework pass
  in the http-worker (tick ~1 min, a batch of up to 100 domains) publishes
  the SPA tasks of flagged domains and clears the bit — bit transitions
  feed the counter
  `crawler_counters['render_pending']` (T-175). Consequence: the q:render
  lag stays near the threshold until the debt is exhausted. Between
  snapshots compare only the sum **lag(q:render) + render_pending** (a
  ready monitor metric:
  `crawler_render_pending_domains`); the drain is alive if the
  http-worker journal shows the "render rework pass" line (full=true —
  the batch hit the threshold, this is normal).

## Metrics ports (Prometheus, 127.0.0.1, from the server)

| Port | Service |
|---|---|
| 9100 | monitor (aggregates + alerts: frozen queues, redis, PG slow, OOM) |
| 9104 | http-worker (retention, compaction, HTTP stage) |
| 9105 / 9109 | render-worker (main + the second instance @9109) |
| 9106 | analyzer |
| 9120 | render-fallback-worker, worker B — after the stage 35/36 deploy (the port is fixed: 9109+ is the render-worker@ instance range; 9110 in old notes is outdated, the S-P2-3 fix) |

```bash
curl -s 127.0.0.1:9104/metrics | grep -E '^crawler_queue_depth|^crawler_content_'
curl -s 127.0.0.1:9100/metrics | grep -E '^crawler_(queue|unit|redis)'
```

### Render metrics dictionary: engines and fallback (after the 35/36 deploy)

- `crawler_render_fallback_published_total{reason}` — tasks
  published to q:render-fallback from the finalize-without-render paths
  (engine-dependent reasons: tarpit, task class on the last
  delivery); reason — the code `net::ERR_*` / `timeout` / `other`;
  the numerator of the fallback rate. Exposed by worker A (:9105).
- `crawler_render_fallback_consumed_total{result}` — consumption of
  q:render-fallback by worker B by delivery outcome: `rendered` —
  the rework succeeded (the DOM is written); `finalized` — the fallback
  is exhausted, the entry is closed with the `render_failed_at` marker;
  `skipped` — the entry slipped out from under the task (no entry / no
  final URL), without side
  effects. Only the B build counts it (:9120); failed deliveries live
  in the PEL until claimed or buried to q:dead.
- `crawler_render_fallback_suspended_total{reason}` — events (not
  state!) of fallback publication suspension: `gate` — the health gate,
  a streak of browser-class infra errors (the main engine is unavailable)
  reached `render.fallback.health_gate_streak`; `budget` — the share
  of publications per 1000-delivery window exceeded the cap
  `render.fallback.share_percent`. While the suspension holds,
  candidates are finalized as usual with the marker — tasks are not lost.
- `crawler_render_records_total{rendered,engine}` — entries by the
  rendered flag (`false` — the HTTP stage, without engine; `true` —
  render workers);
  the `engine` label (chromium|moli|lightpanda) — the build engine of the
  worker, the material for the engine shares. Tarpit finalizations are
  attributed to the engine —
  `crawler_render_tarpit_finalized_total{engine}`.

## PostgreSQL (sudo -u postgres psql -d crawler)

- Sizes: `SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
  FROM pg_statio_user_tables ORDER BY 2 DESC LIMIT 8;`
  As of 03.11.2026: domains_default ~10 GB, subdomain_results_default
  ~7.5 GB, contents ~0.26 GB (the content itself lives in packs on disk,
  not in the DB).
- Table liveness: `n_live_tup`/`n_dead_tup` of `contents`,
  `subdomain_results_default`, `laravel_domains`.
- The `crawler_counters` counters (3 columns: name, value, time):
  `checks:YYYY-MM-DD` — domain checks per 24 hours (0 when the front is
  stopped);
  `laravel_appeared_total` / `laravel_lost_total` — finds;
  `render_pending` — the render debt (retention protection, T-166/T-185);
  the full render debt = the q:render lag + this value — compare q:render
  between snapshots only together with it (the metric
  `crawler_render_pending_domains`);
  `reanalyze:*` — noise, filter it out.

## Content storage and disk

- Packs: `/var/lib/crawler/content/pack-NNNNNN.pack` (~489 MB each).
  Growth/reclamation: retention (6 h, clears the references of old
  non-finds) +
  hourly compaction — BOTH cycles in the http-worker. Check:
  ```bash
  journalctl -u crawler-http-worker --since -24h | grep -E "compaction|retention"
  ```
  Normal: a pass once per hour, `freed_bytes` > 0. Retention skips at a
  q:analyze depth ≥ watermark — occasionally normal, constant — blocks
  cleanup.
- Logs: journal ≤ 1 GB (SystemMaxUse), ForwardToSyslog=no; chromium spam
  is suppressed by the `crawler-chromium.service.d/10-logfilter.conf`
  drop-in
  (repo: `deploy/systemd/crawler-chromium.service.d/`).
- `/var/log/postgresql` ~0.3 GB — normal; if growing — the PG log level.

## Dashboard and entry points

- Operator: `https://dashboard.laracity.ru` (HTTP Basic, the JSON snapshot
  `GET /api/status` — the same queues/rates as in crawlerctl).
- Landing pages: `https://sites.laravel.city`, `https://sites.laracity.ru`.

## Known incidents (what to look for by signs)

| Date | Sign | Cause |
|---|---|---|
| 03.11.2026 | the "queue backlog" is 15x above the truth | XLEN was taken for the depth; the truth is lag |
| 03.11.2026 | rate 0 domains/s for days, q:dns/q:http empty | crawler-scheduler stopped manually by the operator |
| 23.10.2026 | q:render lag ≈99k, retention idle | render debt: protection of entries from deletion (T-166/T-167) |
| 03.11.2026 | syslog +2.5 GB in 2 days | consoles of foreign pages from chromium (see the drop-in above) |
| 04.11.2026 | q:render lag ~9.9k "not melting" for days, the hours-scale drain forecast failed | equilibrium at `render.queue_high_watermark` (10k): the T-066/T-174 rework pass replenishes the queue from render_pending (~350k) — this is normal; the metric is lag + render_pending, the ETA from the sum |
| 06.11.2026 | candidate engines red on the per-engine snapshot (runs on the dev bench, NOT prod) | bench comparison of engines (stage 35/36): moli 1.1.21/1.1.22 — `Fetch.enable` InvalidParams (upstream PR #2470, waiting for 1.1.23), lightpanda 1.0.2/1.0.3 — one browser context per connection (upstream #4151, watch), chromium — the baseline ≈92% success; context for future canary windows |
| 09–11.11.2026 | WARN "content retention pass … candidate selection: timeout" every 15 min, :9104 metrics at zero; since T-273 — `retention_errors_total` growing in bursts once per "pass + 15 min"; render-late "selection: timeout" | T-271 regression: the client timeout is shorter than the cost of the first catch-up pass with a zero cursor (the cursor lives in the process memory, it does not move without a commit) + the unpinning flipped the render-late join away from domains; the T-273 fix (3600/300 s, unpinned, ticker reset, error metrics), backlog drain — the "quiet window" docs/operations.md §4.6 (the deploy always brings the scheduler up itself — resume the window manually; zero counters with a live pass are normal, the total is by pass_seconds_count) |
| 10.11.2026 | moli v1.1.23 still red on the snapshot (dev bench, NOT prod; T-272) | The InvalidParams on `Fetch.enable` is gone (our upstream PR #2470 + the paired #2476 made it into 1.1.23): navigation/DOM/cookies/isolation/UA are green, the chromium control is 6/6 PASS on the same bench. The snapshot is kept red by the Fetch interception defects: (1) with interception on, a synchronous XHR from the page JS fails with NetworkError on the client — the request never reaches the server (the moli log: Uncaught NetworkError … XMLHttpRequest); (2) `fetch.failRequest` of the main navigation does not finish the navigation with a load error — the tab is unresponsive, ERR_BLOCKED_BY_CLIENT outside the loadFailed channel (class context, no load code); (3) Stylesheet is still not accepted in v1.1.23 (supports_fetch_enable) — the CSS mode `BlockStylesheet` fails the whole call with InvalidParams, the base profile is unaffected (the default false). The pin is rolled back to v1.1.21 (the container is stopped, as before); the crawler/moli:v1.1.23 tag remains local — the next retest starts with restoring the pair in .env, without a build. Plan: an upstream issue + a retest with the next release (T-247) |

## Targeted commands per block

```bash
# the unit and its restarts/environment
systemctl status crawler-render-worker --no-pager -l
systemctl show crawler-scheduler -p ActiveState,NRestarts,MainPID,ActiveEnterTimestamp

# the service journal (errors for the last hour)
journalctl -u crawler-http-worker --since -1h -q | grep -iE "err|warn|fatal|panic" | tail -20

# stuck queue messages (the group pending)
redis-cli XPENDING q:render render-worker

# render fallback (after the stage 35/36 deploy; before the deploy there is no
# stream/unit —
# this is normal): the worker B group pending
redis-cli XPENDING q:render-fallback render-fallback-worker

# fallback consumption by worker B — its :9120 exposition
curl -s 127.0.0.1:9120/metrics | grep fallback

# publication suspensions (gate/budget) — the primary worker A exposition
curl -s 127.0.0.1:9105/metrics | grep suspended

# render debt drain: replenishing q:render from render_pending (T-066)
journalctl -u crawler-http-worker --since -30m -q | grep "render rework pass" | tail -5

# domain state
set -a; . /etc/crawler/crawler.env 2>/dev/null; set +a
/opt/crawler/bin/crawlerctl domain example.com --json

# slow PG queries
sudo -u postgres psql -d crawler -c "SELECT calls, round(mean_exec_time::numeric,0) ms, left(query,80) FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;"
```

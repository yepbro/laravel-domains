# Production deployment: systemd

Instructions for running Domain Crawler on the target server (Ubuntu Server LTS,
8 GB RAM, a single IP — spec 3.1) with systemd units. The Docker dev
environment is a separate way to run the system ([`docker.md`](docker.md),
[`dev-environment.md`](dev-environment.md)); how the two relate is covered below.

Unit set: [`deploy/systemd/`](../deploy/systemd/) (stage 9, T-085).
Specs: §10.1 (environment), §3.2 (memory budget), §4.1 (service composition),
§11.4 item 7 (a 24-hour run without OOM).

## 1. Deployment method and its relation to Docker

Spec §10.1 defines the production method explicitly: **a systemd unit per service**,
PostgreSQL/Redis/Unbound — OS packages, Go services — static binaries,
Chromium — a separate package/directory. Docker Compose in the repository is
the dev environment (hot reload, testbed, tools) and is not the production
deployment. The decision is fixed:

| Layer | Production (systemd) | Dev (docker compose) |
|---|---|---|
| PostgreSQL, Redis, Unbound | OS packages + budget drop-ins (`deploy/systemd/*.service.d`) | compose containers |
| Go services (9 total: 7 pipeline + web + dashboard) | static binaries `/opt/crawler/bin`, units `crawler-*.service` | image `crawler/go-dev:local` + air |
| Chromium | headless-shell distribution in `/opt/crawler/chromium`, unit `crawler-chromium.service` | image `chromedp/headless-shell` (dev app profile) |
| `crawlerctl` | CLI in `/opt/crawler/bin`, no unit | tools container |

The Docker `runtime` profile (the production render container, T-065) remains
the docker variant of rendering (worker + its own Chromium in one container,
`mem_limit` 2048m) — for hosts with Docker; its relation to the systemd set:
`crawler-chromium.service` + `crawler-render-worker.service` provide the same
pair natively, but in two cgroups (see below).

## 2. Memory budget: MemoryMax per unit

The spec 3.2 budget is enforced with per-cgroup `MemoryMax` (spec 10.1: the
OOM killer does not touch PostgreSQL — a tripped cap kills the process inside
its own cgroup, systemd restarts the unit, the other services are unaffected).

| Unit | MemoryMax | Spec 3.2 budget line |
|---|---|---|
| `postgresql@.service` + drop-in | **3072M** (budget ≈ 2.5 GB + headroom for maintenance peaks) | PostgreSQL ≈ 2.5 GB |
| `redis-server.service` + drop-in | **640M** (maxmemory 512mb + ~128 MB of overhead, docs/redis.md §7) | Redis ≤ 0.5 GB |
| `unbound.service` + drop-in | **800M** (budget ≈ 0.7 GB + cache growth at ≈47M names: 733.9 MiB at the old 700M cap, §12; +100M taken from MemAvailable 2.6 GiB) | Unbound ≈ 0.7 GB |
| `crawler-*.service` (9 units) | **2436M total** (a second render-worker@ instance adds its own 256M — §2.2; the fallback worker B adds another +256M, §2.3) | Go services ≈ 1.3 GB (7 pipeline) + web of stage 13 + dashboard of stage 15 |
| `crawler-chromium.service` | **2048M** | Chromium ≈ 2 GB (the chromium2 second-instance template — another +2048M when enabled, §2.2/T-209) |

Total caps of 8996M with 8192M of RAM — above the physical memory in sum, and that
is acceptable: a cap is not a reservation, all caps are never touched at the same
time (per the averages of measurements §6/§11 the whole set is ≈ 5.1–5.2 GiB, with a Chromium
peak — ≈ 6 GiB; the T-160 increase — unbound +100M, dns-worker +128M —
was taken from the actual MemAvailable headroom of 2.6 GiB of measurement §12;
PostgreSQL at its 3 GiB cap holds page cache that it yields under pressure;
per-cgroup OOM isolation — the intro paragraph of this
section, spec 10.1, — is preserved). The spec line "Go services ≈ 1.3 GB"
covered seven pipeline units; the public landing `web` — a product
extension of stage 13 outside the main spec — adds its own 256M line;
the operator `dashboard` — an extension of stage 15 (dashboard spec) — 128M. MemoryMax sits above the planned budget line
where process overhead or maintenance peaks live on top of the internal
limit — Redis (maxmemory 512mb + ~128 MB, eviction before OOM —
docs/redis.md §7), PostgreSQL (work_mem/maintenance/autovacuum on top of
shared_buffers — docs/db.md, "Memory"), http-worker (bodies in flight
across 192 slots — T-143/T-157, below). In normal operation the instances stay within the budget lines:
the planned lines total ≈ 7.4 GB, the "Go services" line measured in fact
(§11) is ≈ 1.0–1.1 GiB; the remaining ≈ 0.6 GB and the unfilled caps are the OS
and page cache (spec 3.2).

The caps of the nine Go service units (the "Go services ≈ 1.3 GB" line
is met by actual consumption of ≈ 1.0–1.15 GiB, not by the sum of
the 2436M caps):

| Unit | MemoryMax |
|---|---|
| `crawler-http-worker.service` | 900M |
| `crawler-dns-worker.service` | 384M |
| `crawler-web.service` | 256M |
| `crawler-render-worker.service` | 256M |
| `crawler-ingest.service` | 128M |
| `crawler-scheduler.service` | 128M |
| `crawler-analyzer.service` | 128M |
| `crawler-monitor.service` | 128M |
| `crawler-dashboard.service` | 128M |

- **http-worker — 900M**, the heaviest one: up to `http.task_concurrency`
  domain tasks per instance (192 by default — T-157, returning to 256 —
  based on a measurement after T-163; T-139 — the sliding consumption of q:http);
  page bodies are not held in the slot (the early Put of T-147 — every
  page goes straight to hash-addressed storage), a task carries metadata and
  ≤ `http.task_per_domain` transit buffers of bodies being read, plus a cache
  of final URLs and the content write storage; the soft Go heap cap is
  `http.mem_limit` (T-163, default 768 MiB: GC at the threshold returns
  pages to the OS on its own, keeping anon below MemoryHigh/MemoryMax — the reclaim
  storms and fatals of T-157); the criterion is measured via the slot metrics on :9104. The cap
  was raised 384M → 900M after a repeat measurement following
  T-139 (load-measurements.md §11, T-143): 256 slots held anon
  avg ≈ 855 MiB (834–873, oom=0 under the 900 MiB cap set on the
  server with `systemctl set-property` — a persistent drop-in, now
  pinned in the unit), i.e. ≈ 3.34 MiB per slot — a measurement of the code before T-147
  (back then the bodies of original records, each ≤ 2 MiB, were held until the end
  of the task; a typical mix, not an upper bound: a burst of large pages
  can go higher — the measurement window is 12 min).
  The 24-hour run confirmed it (§12, T-157): two fatals per day at 256 —
  05:48 killed by OOM killer and 09:17 hitting the peak of 900.5 MiB =
  MemoryMax, MemoryHigh 850M tripped 120,095 times (reclaim storms,
  swap) — the default was lowered to 192, stable through the soak of wave 21
  (860–893 MiB, oom=0, NRestarts=0; the rate ≈17–19 domains/s).
  384M was already a tight fit at 128 slots (anon ≈ 380 of 384 MiB).
  The "Go services ≈ 1.3 GB" line holds: http 900M + the other Go services
  ≈ 130–160 MiB per measurement §6/§11 → ≈ 1.0–1.1 GiB in total (with
  render-worker per the §6 avg — ≈ 1.13 GiB); even by the §6 maxima —
  ≈ 1.2 GiB. Returning to 256 — based on a measurement after T-163 (the slot metrics
  `crawler_http_*` on :9104: unit anon / busy slots ≤ 1.5 MiB);
- **dns-worker — 384M**: a pool of 2000 UDP sockets (`dns.udp_pool_size`) and
  exchange buffers, a TTL cache of redirect resolutions (T-047). The cap was raised
  256M → 384M per measurement §12 (T-160): at ≈47M domains and a deep q:dns
  the worker periodically hit 256 MiB (in §6 on a ru/rf/su mix the max was
  141 MiB); the headroom came from MemAvailable 2.6 GiB;
- **web — 256M** (stage 13, outside the spec line): besides light DB work, live
  form checks run the full checker pipeline (T-123: DNS → HTTP with page
  bodies up to `http.max_body_bytes` → SPA render → analysis) — the network buffers
  of a single domain episode, parity with the dev app profile;
- **render-worker — 256M** (T-169, was 128M): only the Go part (Chromium is
  in a separate cgroup), but DOM in flight is ≤ `render.contexts` ×
  `content.max_store_bytes` (8 × 2 MiB + transient serialization copies), plus
  pgx/queue buffers: measurement T-092 §6 hit the 128 MiB cap
  (23/116/128) already at 4 contexts, by 23.10 — 117–128 MB + swap
  (a slot-time deficit, an increase in the T-160 style); the default `render.contexts`
  8 (T-169) doubles the DOM window — cap 256M; control after
  the deploy: `memory.events oom=0`, NRestarts 0;
- **ingest / scheduler / analyzer / monitor / dashboard — 128M each**:
  batch DB work without large buffers (streaming parsing
  of files, SQL batches, page-by-page parsing of already compressed content, counters);
  dashboard — a read-only reader of DB/Redis (aggregates are computed by the SQL engine,
  in memory — only the snapshot and the embedded HTML/JSON artifacts);
- `crawlerctl` — a CLI without a unit (spec 4.1), run by the operator
  episodically (`crawlerctl status/domain/zones/...`); its memory is not pinned
  by a cap — the command is short-lived; episodic tens of MiB fit
  into the headroom of the "Go services" line per the measurement averages (the pipeline fact
  is ≈ 1.0–1.15 GiB against ≈ 1.3 GB).

Chromium as a separate unit, not a worker process (a decision): the budget
line gets its own cgroup — a browser tab killed by OOM dies inside
`crawler-chromium.service` and is restarted without touching the worker;
a browser stop/restart does not stop q:render consumption (the worker
survives — it pauses consumption while the browser is dead, docs/render.md);
the profile and `--no-sandbox` run under the dedicated `crawler-chromium` user.
The worker runs in remote mode (`render.remote_url = ws://127.0.0.1:9222`)
— the same contract as the dev app profile. The compensation for remote mode:
a restart by page count/RSS from inside the worker (T-065) is impossible for
a foreign process — the unit holds the cap and the restart (the analogy is the mem_limit
of the dev chromium service, docs/docker.md §6); the scheduled recycle every 2 h is
the `crawler-chromium-recycle.timer` timer (§2.2 below, T-208).

Applying T-160 on the live server (unbound 700 → 800M, dns-worker
256 → 384M) — **before the deploy**, following the §2.1 pattern (T-158): the values are the same
as in the drop-in/unit, they take effect immediately, and the persistent drop-in is written to
`/etc/systemd/system.control/`:

```bash
systemctl set-property unbound.service MemoryMax=800M
systemctl set-property crawler-dns-worker.service MemoryMax=384M
```

After the deploy — sync the drop-in/unit per §5 (item 1) and remove
the persistent set-property drop-ins (`systemctl revert <unit>` +
`daemon-reload` + `restart` — as in §2.1, the T-143 sample,
load-measurements.md §3): the set's unit files are the single source of truth. The T-160 criterion: `memory.events oom=0` for
both units, unbound MemoryCurrent < MemoryMax with a margin of ≥ 50 MiB,
the median duration of DNS tasks in the 9103 histograms does not degrade,
`recursion.time.median` (`unbound-control stats`) does not grow.

Applying T-169 on the live server (render-worker 128 → 256M) — together
with step 1 (operationally `render.contexts 4 → 8` in
`/etc/crawler/config.yaml` — an explicit value in the config overrides
the code defaults, §5 item 2; measured with the T-092 kit before/after, 30–60 min
of steady state):

```bash
systemctl set-property crawler-render-worker.service MemoryMax=256M
```

After the deploy — sync the unit per §5 (item 1) and remove the drop-in
(`systemctl revert` + `daemon-reload` + `restart`). The criterion of step 1
(T-169): the q:render backlog decreases by ≥1,500/h, PSI CPU some < 95%,
chromium RSS below the `render.restart_memory_bytes` threshold (1536 MiB —
the chromium unit's MemoryCurrent), the 9105 histograms and memory, `q:dead`
not growing, NRestarts 0; a repeat measurement after the render CPUWeight (T-158) —
capacity versus priority. Steps 2–3 — configuration.md (`render`,
`timeout`) and §2.2 below.

### 2.1. CPU priorities: CPUWeight/CPUQuota (T-158)

There is a single server (4 vCPU, spec 3.1), scaling out is unavailable (measurement §12), so
CPU is redistributed between units with the relative `CPUWeight`
weights (cgroup v2, range 1–10000, default 100). A weight is not a cap: it acts
only under CPU contention, applies on the fly, and requires no unit
restart. The trigger was the control measurement of 2026-10-23
([`load-measurements.md`](load-measurements.md) §12): PSI CPU some
91–95%, load 25/4 vCPU; in steady state CPU was distributed against the
checking — postgres 177%, chromium 140%, analyzer 66% versus ~90% of one
core at http-worker (monitoring aggregates, render cores, and background analysis
were taking CPU away from the pipeline). Specs: §3.2, §10.

| Unit | CPUWeight | CPUQuota | Rationale |
|---|---|---|---|
| `crawler-http-worker.service` | **1000** | — | critical path: the HTTP stage is the pipeline bottleneck (load-measurements §3) |
| `crawler-dns-worker.service` | **1000** | — | critical path: feeds q:http, must not lag behind HTTP (backpressure) |
| `crawler-render-worker.service` | **300** | — | a pipeline stage, not the bottleneck; the CPU-heavy rendering lives in chromium (see below) |
| `crawler-analyzer.service` | **200** | **150%** | background analysis must not take CPU from checking: the quota does not cut the current 66% (§12) and limits bursts of background passes |
| `crawler-monitor.service` | **50** | — | auxiliary: aggregates over 47M rows (BacklogLoop/ZoneStats, §12) — no competition for the pipeline |
| `crawler-web.service` | **50** | — | auxiliary: the landing page, live form checks — one-off episodes |
| `crawler-dashboard.service` | **50** | — | auxiliary: a read-only reader of DB/Redis (aggregates are the SQL engine's job) |
| `crawler-ingest.service`, `crawler-scheduler.service` | 100 (default) | — | CPU-light enqueuers: their CPU was negligible in measurement §12 |
| `crawler-chromium.service` | 100 (default) | — | the render pair's priority is set by render-worker (300); the default 100 with http/dns at 1000 already lowers the browser's relative priority tenfold (140% CPU, §12) |

Infrastructure (postgresql/redis/unbound) is not prioritized: its CPU
serves the pipeline itself (writing results, queues, resolutions) —
lowering its weight would strangle the stages; the pipeline weights of 1000
against their default of 100 resolve contention in favor of checking
without editing foreign units.

Applying on the live server — **before the deploy** (the values are the same
as in the units; they take effect immediately, the persistent drop-in goes to
`/etc/systemd/system.control/`):

```bash
systemctl set-property crawler-http-worker.service CPUWeight=1000
systemctl set-property crawler-dns-worker.service CPUWeight=1000
systemctl set-property crawler-render-worker.service CPUWeight=300
systemctl set-property crawler-analyzer.service CPUWeight=200 CPUQuota=150%
systemctl set-property crawler-monitor.service CPUWeight=50
systemctl set-property crawler-web.service CPUWeight=50
systemctl set-property crawler-dashboard.service CPUWeight=50
```

After the deploy (the units carry the same values): sync the unit files
per §5 ("Version updates", item 1) and remove the persistent drop-ins following the
T-143 pattern (load-measurements.md §3): `systemctl revert <unit>` +
`daemon-reload` + `restart` — otherwise the drop-in keeps overriding the
unit file; the unit file is the single source of truth. Verification:

```bash
systemctl show crawler-http-worker -p CPUWeight        # 1000
systemctl show crawler-analyzer -p CPUQuotaPerSecUSec  # 1s 500ms (= 150%)
```

Acceptance criterion (measured with the T-092 kit before/after —
load-measurements.md §10): PSI CPU some in steady state < 80%, unit
NRestarts 0, the HTTP rate does not drop during the analyzer's background passes
and zone recomputations.

### 2.2. The second render-worker instance (step 3 of T-169)

The unit template [`deploy/systemd/crawler-render-worker@.service`](../deploy/systemd/crawler-render-worker@.service)
brings up the second and further render-worker instances — step 3
of speeding up q:render drainage (introduced by T-169), **after** step 1
(`render.contexts 8` + MemoryMax 256M) and when step 2 is not enough
(`render.timeout` 25 → 15–20 s, only per the histogram —
configuration.md, `render`): a second worker is justified when the q:render backlog
does not decrease by ≥1,500/h at PSI CPU some < 95% and is gated at the watermark —
meaning the slots/CPU of the first worker and its Chromium are exhausted.

Mechanics: the same binary, the same consumer group `render-worker`
(spec 4.2 — consumer groups scale workers horizontally);
the consumer name is per-process (`render-worker-<pid>`) — the instances do not
conflict either in XREADGROUP (deliveries are split between consumers) or
in XAUTOCLAIM claims (a live worker acknowledges before
`redis.claim_min_idle`), and they write to the shared content storage (coordination
via the DB — like http-worker with render-worker, spec 5.7).

- **Metrics**: the instance name = the port: `crawler-render-worker@9109` →
  `METRICS_ADDR=127.0.0.1:9109` (9101–9108 are taken by the set; free are
  9109+ except 9120 — the fixed port of the fallback worker B, moved out of
  the instance growth range, §2.3).
  `METRICS_ADDR` must not be set in the shared
  `/etc/crawler/crawler.env` — EnvironmentFile overrides the unit's
  `Environment` and would collide the ports of all instances;
- **memory**: its own 256M cap per instance (a cgroup separate from
  the first one); the §2 total grows by 256M; control — `memory.events oom=0`,
  NRestarts 0, the 9105/9109 histograms;
- **the second Chromium (:9223)** — only at saturation of the first one
  (the chromium unit's MemoryCurrent near 2048M / the steady-state rate cap
  with free slots): a per-instance file
  `/etc/crawler/render-worker@9109.env` with
  `CRAWLER_RENDER_REMOTE_URL=ws://127.0.0.1:9223` (the template connects
  it after the shared crawler.env — it overrides), the unit is in the set:
  [`crawler-chromium2.service`](../deploy/systemd/crawler-chromium2.service)
  (port 9223, a separate profile `/var/lib/crawler/chromium2/profile`;
  installation and the ReadWritePaths defect — the subsection below, T-209); its
  cgroup of ≈2 GB is a separate spec 3.2 budget line; on an 8 GB server —
  only per a free-memory measurement (MemAvailable, §12); recycling with the
  `crawler-chromium-recycle.timer` timer (see below) does not cover it — the unit name
  is hard-coded in the `ExecStart=` of the recycle service; the second one needs its own timer
  modeled on it.

Startup and control:

```bash
systemctl enable --now crawler-render-worker@9109.service
curl -fsS 127.0.0.1:9109/livez              # liveness of the second instance
journalctl -t crawler-render-worker@9109 -f # instance logs
```

Rollback — `systemctl disable --now crawler-render-worker@9109`: the instance's
deliveries return via claims to the first one (the hold window ≪
`redis.claim_min_idle`). The template is copied by the standard installation
procedure (§4.6, `cp deploy/systemd/crawler*.service` — the mask captures the
template too).

#### The second Chromium: the crawler-chromium2 unit (T-209)

[`crawler-chromium2.service`](../deploy/systemd/crawler-chromium2.service)
is the second headless-shell instance (CDP `127.0.0.1:9223`, a separate profile
`/var/lib/crawler/chromium2/profile`, its own cgroup with MemoryMax 2048M) for
the second worker of step 3 (the bullet above). It was carried over from the 29.10 experiment
(the drainage window of T-198), where it was produced as a sed-copy of `crawler-chromium.service`
(port 9222→9223, user-data-dir, Description; the other directives are as in
the first one, except `ReadWritePaths` — the fix below); the directives were verified against the server
byte by byte. This is a concrete unit, not an `@.service` template: systemd has long expanded
specifiers in isolation paths (`ReadWritePaths=`) (since v233;
Ubuntu 24.04 on the server has systemd 255 — it works), but the pair "port 9223 +
chromium2 profile" cannot be derived from a single `%i` (9223 and 2 are different
numbers) — a template would require renaming the port or the directory on the
server; the concrete unit keeps the server directives literally
and is verified by md5. On the server
after the experiment the unit is **disabled** (the worker env file was removed): in the set
the file is a template for a possible return to two browsers; installing the set does not change the server state through it — §4.6 copies it with the
`crawler*.service` mask, but it is not in the `enable --now` list.

Returning to two browsers (saturation of the first + free memory — the criteria
of the bullet above; the §2 cap total of 8996M grows by 2048M to ≈11 GB with
8 GB of RAM — only per a MemAvailable measurement, §12):

```bash
install -d -o crawler-chromium -g crawler -m 0770 /var/lib/crawler/chromium2
cp deploy/systemd/crawler-chromium2.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now crawler-chromium2.service
curl -fsS 127.0.0.1:9223/json/version # the second browser's CDP responds
journalctl -u crawler-chromium2 -f    # logs: the SyslogIdentifier is shared with the first one; tell them apart by unit
# worker → second browser: per-instance env (the bullet above). Create the file
# with this single line; if other overrides appear, append
# (`>>`) instead of truncating:
echo 'CRAWLER_RENDER_REMOTE_URL=ws://127.0.0.1:9223' \
  > /etc/crawler/render-worker@9109.env
systemctl restart crawler-render-worker@9109
```

Rollback — `systemctl disable --now crawler-chromium2.service`, delete
`/etc/crawler/render-worker@9109.env`, and restart the worker instance
(it returns to :9222 from the shared crawler.env) — the server state as of 30.10.

**Manual-copy defect** (found 29.10): when copying
`crawler-chromium.service` with sed, `ReadWritePaths` must be replaced with
this instance's profile directory — `/var/lib/crawler/chromium2`; replaced, not
appended to (the first instance's directory is not needed by the second one).
The `ReadWritePaths=/var/lib/crawler/chromium` left over from the original,
paired with `ProtectSystem=strict`, forbids writing to `chromium2` —
headless-shell cannot create the profile, exits → a restart loop until start-limit
(10 starts in 5 min, the unit failed), while the worker on `:9223` at that time
crash-loops with "dial tcp 127.0.0.1:9223: connection refused". The symptom
is misleading — it looks like a browser crash; the cause is a path in the unit's
isolation (details in the unit header). The cure: fix `ReadWritePaths`, then
`systemctl daemon-reload && systemctl reset-failed crawler-chromium2.service`
(start-limit does not release on its own) and start.

**Outcome of the 29.10 experiment**: on 4 vCPU the second browser did not speed up the pipeline
— 96 domains/min versus 112–144 for one; the two browsers totaled ≈400% CPU:
the bottleneck is the processor, not the render slots. Returning to two browsers
is justified only on scaled-up hardware (more vCPU; memory — the second
2048M budget line, spec 3.2); the second one's recycle — its own timer modeled on T-208 (the unit name
is hard-coded in the `ExecStart=` of the recycle service, not the timer).

#### Chromium recycle: the crawler-chromium-recycle timer (T-208)

The pair [`crawler-chromium-recycle.service`](../deploy/systemd/crawler-chromium-recycle.service)
and [`crawler-chromium-recycle.timer`](../deploy/systemd/crawler-chromium-recycle.timer)
restarts `crawler-chromium` every 2 h — anti-degradation for
the remote render mode. **When it is needed**: the production deploy by default
runs in remote mode (§2, `render.remote_url = ws://127.0.0.1:9222`),
where the worker cannot restart a foreign browser by page count/RSS
(T-065) — systemd holds the recycle, not the worker. Without it chromium accumulates
memory 400→950+ MiB over ~2 h and render timeouts double; measurement 29.10
(the drainage window of T-198): after a recycle the timeouts are twice as low, 227→110. In
local mode (`render.remote_url` empty — the browser as a worker process)
the timer is not needed: the worker restarts the browser itself (T-065,
[`render.md`](render.md)).

The cost of a restart — interrupted in-flight renders (<1 min each): deliveries
return via an XAUTOCLAIM claim (idempotency, spec 3.4), and the worker survives
the consumption pause while the browser is dead (docs/render.md). The period follows the
degradation measurement (~2 h, `OnUnitActiveSec`); the first run is 30 min
after the timer is activated (`OnActiveSec`) — right after startup the browser is fresh.

Installation — the standard §4.6 procedure (the `cp` mask captures `crawler*.timer`,
the timer is in the `enable --now` list); on an already deployed host (the pair
was installed manually on 29.10, outside the set) this is enough:

```bash
cp deploy/systemd/crawler-chromium-recycle.{service,timer} /etc/systemd/system/
systemctl daemon-reload
systemctl reenable --now crawler-chromium-recycle.timer # before T-215 it was WantedBy=timers.target — the symlink moves to crawler.target.wants/
systemctl list-timers 'crawler-chromium-recycle*' # schedule check
```

The pair's lifecycle — together with the set (T-215): `PartOf=crawler.target`
in both units shuts down both the service and the timer on `systemctl stop
crawler.target` — the next tick (every 2 h) will not start the stopped
chromium; `WantedBy=crawler.target` brings the timer back when the set starts
(`PartOf=` propagates only stop/restart, not start — hence the pair of
two directives). Stopping the timer separately when the set stops is no longer
needed; `systemctl disable --now crawler-chromium-recycle.timer` — only
if recycling is not needed at all (local mode; re-enabling — §4.6).
A set restart propagates to the pair the same way as stop (`PartOf=` passes
restart through too): on `systemctl restart crawler.target` the recycle also runs —
an extra chromium restart in the same transaction (harmless: renders
return via claims; a possible transient `failed` of the recycle when jobs collapse
is cleared by the next tick). After a manual restart of the set it is useful to check
`systemctl --failed`.

### 2.3. The render-fallback worker B (T-240)

[`crawler-render-fallback-worker.service`](../deploy/systemd/crawler-render-fallback-worker.service)
is a Chromium rework pass for tasks that failed to render on the main engine
(render spec, stage 35 §4.5, the `q:render-fallback` queue): the same binary as
`crawler-render-worker.service`, a different build. Build differences:

- **consumption stream** — `RENDER_WORKER_STREAM=q:render-fallback` in
  the unit (a launch parameter, like `METRICS_ADDR`; the consumption stream is
  a worker build parameter, not a configuration field — render spec §4.1 item 3). The group
  is derived from the stream's canonical pair (`queue.WorkingGroups`):
  `render-fallback-worker`, separate from the main `render-worker` —
  fallback deliveries are not shared with the main pipeline;
- **rate** (FR-13) — `render.fallback.worker_rate` records per
  a 1 min sliding window (`renderworker.RateWindow`) from the shared
  `/etc/crawler/config.yaml`: a batch mode of "collect and rework" without
  scaling up chromium capacity. It also applies when
  `render.fallback.enabled` is off — what has accumulated in the queue is reworked until
  empty (FR-14). Worker B itself does not publish fallbacks — recursion is forbidden
  (FR-10);
- **engine** — chromium is pinned to build B by the unit-local
  `EnvironmentFile=-/etc/crawler/render-fallback-worker.env` (the unit reads
  it **after** the shared `crawler.env` — same-named variables win, like
  the per-instance file of `render-worker@`, §2.2). The content of the production file
  (it lives on the host; there is no separate file in `deploy/`):

  ```
  CRAWLER_RENDER_ENGINE=chromium
  CRAWLER_RENDER_REMOTE_URL=ws://127.0.0.1:9222
  ```

  The second line is not always needed: while the main worker is on chromium,
  the value comes from the shared `crawler.env` anyway; after the main
  worker is switched to the candidate engine (editing `render.engine`/`render.remote_url`
  in the shared config or `crawler.env`, T-248/T-249) it returns worker B
  to the chromium of the main unit (`:9222`; the second Chromium — `:9223`, §2.2).
  Without the pin, worker B would drift to the candidate together with the shared config —
  it would rework the fallback with the same light engine that failed the tasks
  (recursive fallback is forbidden, FR-10/§4.5 item 1), and the builds' metrics
  would get mixed in `crawler_render_records_total{engine}`. The file
  is optional (`-`): until it is created the unit starts with the engine from the shared
  config (without the canary that is the same chromium);
- **metrics** — `127.0.0.1:9120`: 9101–9108 are taken by the set; 9120 is
  the fixed port of worker B, moved out of the natural growth range of
  `render-worker@` instances (§2.2: 9109+ are free, except 9120).

Chromium is shared with the main worker (`render.remote_url` from
`crawler.env` → `ws://127.0.0.1:9222`, the `crawler-chromium.service` unit):
the CDP browser holds several consumers, and the unit's memory budget is one for
all (§2.2). **The T-208 recycle pair applies as is, no new files are
needed**: the `ExecStart` of the recycle service restarts exactly
`crawler-chromium.service` — the browser shared by both workers; worker B
does not need its own pair. A local headless-shell (without an external unit) is
the docker-runtime mode: the `render-fallback` service of the `runtime` profile
(`deploy/docker/compose.yaml`, the image is the current `Dockerfile.render`
unchanged, the configuration is
[`deploy/docker/config.render-fallback.yaml`](../deploy/docker/config.render-fallback.yaml));
browser restarts there are done by the worker itself (T-065), as with `render`.

The unit is copied by the `crawler*.service` mask (§4.6), but is **not in
the set's enable list**: enabling it is a deliberate step of rolling out the fallback
(the §2 budget line +256M; +5 DB pool connections — recalculate the
`postgres.pool_size` budget, §4.5). Until the canary is ready (render spec §4.5 item 3)
worker B may run on an empty queue — wiring verification; on the
main chromium there are no publications at all — the engine guard (S-P1-1):
only candidate engines publish fallbacks, chromium does not
fall back to itself. Step 1 of render spec §6.2 acceptance ("enabling the fallback without changing the
engine"): the q:render-fallback queue stays empty even with
`render.fallback.enabled` **on**; with the flag off there are no
publications either (FR-14). The rollback of render spec §6.4 item 1 — returning workers to
chromium stops the publication by itself (without changing the flag); worker B reworks
what has accumulated until empty.

```bash
# On an already deployed host — the user first (§4.4 creates it on
# fresh installs; worker B is separate, not a template instance):
useradd -r -g crawler -d /var/lib/crawler -s /usr/sbin/nologin crawler-render-fallback-worker
# Pinning chromium to build B (the file content is above, permissions as for
# crawler.env, §4.5; the unit reads it after the shared crawler.env):
printf '%s\n' 'CRAWLER_RENDER_ENGINE=chromium' \
  'CRAWLER_RENDER_REMOTE_URL=ws://127.0.0.1:9222' \
  > /etc/crawler/render-fallback-worker.env
chown root:crawler /etc/crawler/render-fallback-worker.env
chmod 0640 /etc/crawler/render-fallback-worker.env
systemctl enable --now crawler-render-fallback-worker.service
curl -fsS 127.0.0.1:9120/livez               # worker B liveness
journalctl -t crawler-render-fallback-worker -f
# the rate is in effect: reads in batches of ≤ render.fallback.worker_rate per minute
```

Rollback (FR-14): `systemctl disable --now
crawler-render-fallback-worker.service` stops consumption
(deliveries stay in the stream, not in the PEL); or, conversely, the worker is left
until the queue is empty with the publication flag off in the main
worker's config. There are no separate migrations/cleanups (render spec §4.8).

Worker B observability — a step toward the canary (T-246, S-P2-5/S-P3-3):
its exposition address `127.0.0.1:9120/metrics` is added to
`dashboard.render_metrics_urls` (the engines panel: consumption and depth of
`q:render-fallback`; the key's default is only the main build
`127.0.0.1:9105`, consistent with `monitor.render_metrics_url`) and to the Prometheus
scrape — the production config lives outside the repository; the target `127.0.0.1:9120` follows the
pattern of the `render-fallback` job of the dev config
([`deploy/docker/prometheus/prometheus.yml`](../deploy/docker/prometheus/prometheus.yml)):
`crawler_service_up`, the published/consumed balance ("working-logic
metrics" in [`services.md`](services.md)). The `monitor.render_metrics_url` probe
stays on the main build (9105) — the budget/fallback gate alerts read the
publishing worker's exposition.

## 3. Users: static, one per service

Spec §10.1 requires a separate user per service. The solution is **static
system users** `crawler-<service>` (no shell, home
`/var/lib/crawler`) in the shared `crawler` group, rather than `DynamicUser=yes`:

- the content storage is written by **three** units (http-worker, render-worker,
  render-fallback-worker — §2.3) and
  read by analyzer and `crawlerctl`: a stable group ownership of files is needed —
  a setgid directory + fixed UID/GID; DynamicUser would rotate
  the owner and require re-chmodding on every start;
- the ingest archive (`incoming/processed|failed`) must stay readable
  by the operator between runs;
- pack file permissions (0660, group `crawler`) survive restarts without
  the systemd StateDirectory machinery.

Directories:

```
/var/lib/crawler/            root:crawler 0755   — work zone (WorkingDirectory)
/var/lib/crawler/incoming/   crawler-ingest:crawler 0770 — the ingest watched folder (spec 5.1)
/var/lib/crawler/content/    root:crawler 02775  — content pack files (setgid: group crawler)
/var/lib/crawler/chromium/   crawler-chromium:crawler 0770 — the browser service profile
/var/lib/crawler/chromium2/  crawler-chromium:crawler 0770 — the second Chromium's profile (created when it is enabled, §2.2/T-209)
/etc/crawler/                root:crawler 0750 — config.yaml, crawler.env, rules/
/opt/crawler/bin/            root:root 0755 — binaries; /opt/crawler/chromium/ — headless-shell
```

An operator in the `crawler` group drops files into `incoming/` without sudo.

## 4. Installation

The commands below run as root (`sudo -i` or with the `sudo` prefix);
the exception is building the binaries (4.2): as a regular user, installing
into `/opt` — with `sudo`.

Before packages and users, **disk layout** is done
([`disks.md`](disks.md) §4): the `/var/lib/postgresql` and
`/var/lib/crawler/content` volumes are mounted before `apt install postgresql` —
the package cluster is created right at the mount point, not on the root filesystem.

### 4.1. OS packages

```bash
apt install postgresql redis-server unbound smartmontools jq
```

`jq` is needed by monitor's Telegram delivery script (`/opt/crawler/bin/
telegram-notify.sh`, T-162/T-173: alerts and load reports — building the
JSON message and masking the secret in the log; without it sending silently
fails with exit 127 — measured 24.10 on the production setup).

The Chromium headless-shell dependencies (`libnspr4 libnss3 fontconfig` +
fonts) are a separate list at its installation: §4.3 below.

Budget drop-ins (copied as directories — the directory name = the unit name):

```bash
cp -r deploy/systemd/postgresql@.service.d /etc/systemd/system/
cp -r deploy/systemd/redis-server.service.d /etc/systemd/system/
cp -r deploy/systemd/unbound.service.d /etc/systemd/system/
systemctl daemon-reload
```

The application role and DB (an analog of deploy/docker/postgres/init — the dev
setup points there; the pg_stat_statements extension and preload — the postgresql config of T-086):

```bash
sudo -u postgres psql -c "CREATE ROLE crawler LOGIN PASSWORD '...';"
sudo -u postgres createdb -O crawler crawler
sudo -u postgres psql -d crawler -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
sudo -u postgres psql -d crawler -c "GRANT pg_read_all_stats TO crawler;"
```

The `pg_read_all_stats` grant (T-177): without it `pg_stat_activity` hides
the query text of other sessions from the `crawler` role — monitor's slow-query cycle
sees the durations but not the texts (messages marked "text
unavailable"); the grant gives access to statistics (pg_stat_activity/
pg_stat_statements), not permissions.

Unbound: the recursion config is `deploy/docker/unbound/unbound.internet.conf`
(the caches for a ≈ 0.7 GB budget are already computed in it; a copy goes to
`/etc/unbound/unbound.conf.d/crawler.conf` with the `interface:
127.0.0.1` and `access-control` edits for loopback only — in a native installation
all clients are local; the DNSSEC validator is turned off in the config itself).
There is no mandatory path for the host resolver, but so that render pages and
the rest of the server software go through the local cache (spec 5.4), point to unbound
in `/etc/resolv.conf` (`nameserver 127.0.0.1`) or in systemd-resolved.

Redis listens on 127.0.0.1 by the package default; maxmemory/eviction — the config
of T-087 (≤ 512M, under the MemoryMax cap).

### 4.2. Go binaries

Static builds — like the production render image
(`deploy/docker/Dockerfile.render`): `CGO_ENABLED=0`, `-trimpath -ldflags
"-s -w"`. On a machine with Go (or in the tools container: `make shell`):

```bash
# The landing page's Tailwind bundle (T-124): go:embed picks up internal/web/static/
# app.css; it is not stored in git (.gitignore), and without this step the landing page
# will come up unstyled. Built with a one-off container (node is
# not needed); the production docker build does it itself (the css stage of Dockerfile.go).
make web-css

version="$(git describe --tags --always --dirty 2>/dev/null || echo unknown)"
revision="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
mkdir -p out
for svc in ingest scheduler dns-worker http-worker render-worker analyzer monitor web dashboard crawlerctl; do
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X crawler/internal/metrics.buildMode=prod -X crawler/internal/metrics.buildVersion=$version -X crawler/internal/metrics.buildRevision=$revision" \
      -o "out/$svc" "./cmd/$svc"
done
sudo install -D -t /opt/crawler/bin out/*
```

`buildMode=prod` marks production builds in the `crawler_build_info` metric
(the `mode` label); `buildVersion`/`buildRevision` (T-111) are computed from git —
`version` = `git describe --tags --always --dirty` (a tag or a hash marked
with unsaved edits), `revision` = the short commit hash; when git is unavailable
`unknown` is substituted. Dev builds without `-X` stay at `dev-stub` /
`dev` / an empty `revision`. In the Docker build the values are passed as build args
`BUILD_VERSION`/`BUILD_REVISION`: `.git` is excluded from the context
(`.dockerignore`), so the caller computes the values — that is what
`make build-runtime` does; a direct `docker build` without args gets `unknown`.

Schema migrations are embedded in the binaries (`migrations/embed.go`, golang-migrate).

### 4.3. Chromium

The same headless-shell build as in dev/dev-runtime (the version —
`CHROMEDP_HEADLESS_IMAGE` in `.env.example`, pinned): the
`chromedp/headless-shell` distribution is unpacked into `/opt/crawler/chromium/`
(the `headless-shell` executable in its root — the path in the unit's `ExecStart`). A separate
package/directory instead of an OS package: the browser version is aligned with the worker's chromedp
stack, and the Ubuntu chromium package is a snap wrapper, unfit for headless CDP
without user-data-dir tricks.

Only the binary is transferred to the host, but it is built for the image environment —
after unpacking the distribution, install the missing libraries and
font stack (before the unit's first start, §4.6):

```bash
apt install libnspr4 libnss3 fontconfig fonts-liberation fonts-dejavu-core
# binary libraries: libs ok — resolved; LIBS MISSING — not found/ldd failure:
ldd /opt/crawler/chromium/headless-shell 2>&1 \
  | grep -E 'not found|No such file|not a dynamic' \
  && echo "LIBS MISSING" || echo "libs ok"
# font stack: fonts ok — visible; FONTS MISSING — no fonts:
fc-list | grep -q . && echo "fonts ok" || echo "FONTS MISSING"
```

- `libnspr4 libnss3` — libraries required to run the binary (installed
  during the physical deploy of T-135; before T-140 they were missing from this list);
- `fontconfig fonts-liberation fonts-dejavu-core` — the Skia font stack:
  without fontconfig some pages (those requiring the font stack) crash
  Chromium with FATAL `SkFontMgr_FontConfigInterface: Not implemented`
  (SIGTRAP) → a crash loop until start-limit (10 restarts in 5 minutes, §5) →
  the unit failed, q:render accumulates attempts. In dev the defect is invisible — fontconfig
  and the fonts are already inside the `chromedp/headless-shell` image; on a bare host
  they are absent. The incident of the production measurement T-092 and the operational fix —
  [`load-measurements.md`](load-measurements.md) §4; a unit already gone
  to failed comes back up manually after the packages are installed: `systemctl
  reset-failed crawler-chromium && systemctl start crawler-chromium` (§5).

### 4.4. Users and directories

```bash
groupadd -r crawler
for s in ingest scheduler dns-worker http-worker render-worker render-fallback-worker analyzer monitor web dashboard chromium; do
    useradd -r -g crawler -d /var/lib/crawler -s /usr/sbin/nologin "crawler-$s"
done
install -d -o root -g crawler -m 0755 /var/lib/crawler
install -d -o crawler-ingest -g crawler -m 0770 /var/lib/crawler/incoming
install -d -o root -g crawler -m 2775 /var/lib/crawler/content
install -d -o crawler-chromium -g crawler -m 0770 /var/lib/crawler/chromium
install -d -o root -g crawler -m 0750 /etc/crawler
```

### 4.5. Configuration

```bash
cp config.example.yaml /etc/crawler/config.yaml   # edits below
cp deploy/systemd/crawler.env.example /etc/crawler/crawler.env
chmod 0640 /etc/crawler/crawler.env && chown root:crawler /etc/crawler/crawler.env
cp -r rules /etc/crawler/rules                    # Laravel rules for analyzer
```

Required edits to `config.yaml` relative to the example
(the full reference — [`configuration.md`](configuration.md)):

- `postgres.dsn`, `redis.addr` — replace the examples with local values
  (or leave them as is and set the exact ones in `crawler.env` — environment
  variables win over YAML, docs/configuration.md);
- `postgres.pool_size` — per service, while the spec §6.3 budget (20–50) is
  the total across all: the example value (5) gives 8 × 5 + dashboard (2,
  set by its unit — the ninth service did not fit 5 under the cap;
  the global-variable warning is below) +
  `crawlerctl` (2) + admin ≈ 44–46 at `max_connections=50` (3 slots
  are reserved by `superuser_reserved_connections`). Change it — only
  with the total recomputed; for all services at once —
  `CRAWLER_POSTGRES_POOL_SIZE` in `crawler.env`
  (docs/db.md, "Connection budget"). The fallback worker B (§2.3) is the tenth
  service with its own pool: when it is enabled the total grows by another
  `pool_size` (10 × 5 + 2 + admin ≈ 52 against 47 available slots) —
  recalculate the budget: globally `CRAWLER_POSTGRES_POOL_SIZE=4`
  (10 × 4 + 2 + admin ≈ 44), or a smaller pool for the single worker B
  via an override in its unit. Caution: EnvironmentFile
  (crawler.env) overrides the unit's Environment — enabling the global
  variable will also affect the dashboard pool (9 × 5 + 2 + admin ≈ 48 will hit
  the cap of available slots); the safe global maximum is 4
  (9 × 4 + 2 + admin ≈ 40), or raise `max_connections`,
  or lower the pool of individual services;
- `ingest.dir: /var/lib/crawler/incoming`, `content.dir:
  /var/lib/crawler/content`, `content.zstd_dictionary:
  /var/lib/crawler/content/dict.zst`, `analyzer.rules_dir: /etc/crawler/rules`;
- `render.remote_url: "ws://127.0.0.1:9222"` — an external browser unit
  (empty in the example = the worker's own process, that is the docker-runtime mode);
- `monitor.listen` — the example already has `127.0.0.1:9100`: metrics are not
  exposed on a production-facing interface (spec §9); the monitor unit additionally
  pins loopback with the `CRAWLER_MONITOR_LISTEN` variable (the environment wins over
  YAML — a guard against an unedited config.yaml), the other services listen on
  loopback via `METRICS_ADDR` in their units (127.0.0.1:9101…9108);
- `monitor.zone_stats_interval: "30m"` — the example already has the production value
  (it matches the default, T-199): the reconciliation of incremental counters
  is the only remaining full pass over `domains` (~40 s per 42M
  rows); at a 5-minute interval it contributed 48% of the total slow
  DB time (measured over the slow-query window after the deploy of 3bffc62,
  26.10: 100 events >3 s, median 39 s, max 501 s) — the operator spread the
  interval to 30 minutes following the investigation spec §7.1 template (counter drift in
  steady state must be zero). The alert cycle was decoupled with its own
  `monitor.alerts_interval` field (T-206, default 5 min — the former
  frequency): detection windows returned to their former minutes (queue_dead
  ≈15 min, memory_high/render_debt — 10 min, the bad_hashes window — 75 min);
  the progress-stall threshold remains in reconciliation periods —
  `AlertStallFactor × zone_stats_interval`, 2.5 h (the
  `zones.updated_at` label is written exactly by the reconciliation);
- `web.listen` — the landing page's public address: the default is loopback
  `127.0.0.1:8080` (next to nginx — §6 below); switching to a unix socket —
  an absolute path as the value;
- `web.public_host` — the public landing page's domain (for example
  `crawler.example.com`, with the port if non-standard): absolute `sitemap.xml` URLs
  are built with it. Without it, behind a reverse proxy
  `<loc>` is taken from the visitor's `Host` header (host-header
  poisoning) — a landing page published to the outside must set the field;
  in the example it is commented out (T-227);
- `dashboard.username` / `dashboard.password_hash` — the operator's dashboard
  credentials (required to start `crawler-dashboard`, dashboard spec §7.7).
  The hash is generated by the CLI (the password — as an argument or via stdin; stdin — does not
  stay in shell history):

  ```bash
  /opt/crawler/bin/crawlerctl dashboard-secret < password.txt
  printf 'password' | /opt/crawler/bin/crawlerctl dashboard-secret
  ```

  The line format is `pbkdf2-sha256$<iterations>$<salt>$<key>` (600,000
  iterations by default); a plaintext password in the configuration and the environment
  is forbidden (dashboard spec §7.2); changing `password_hash` invalidates all
  cookie sessions (docs/configuration.md, "dashboard");
  `dashboard.probes` — the `/livez` addresses of the pipeline services for the
  health section: in the systemd deploy these are the units' loopback metric addresses
  (`http://127.0.0.1:9100` monitor, `9101` ingest, `9102` scheduler,
  `9103` dns-worker, `9104` http-worker, `9105` render-worker,
  `9106` analyzer);
- `http.user_agent` — the operator's real contacts (if not set in env);
- `dns.ipv6: true` — if the server has IPv6 (the docker network does not).

An optional setup after the first start — delivering monitor alerts and load
reports to Telegram (T-162/T-173): **both secrets `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` are required**
(the same bot and group as the CI/CD notifications) in `/etc/crawler/crawler.env` — the sample and where
to get the values: `deploy/systemd/crawler.env.example`; delivery is possible
via the `ops-telegram-env` ops workflow (Actions → Run workflow: the values
live in the GitHub repository secrets and are not read from there — CI
is the sole holder; the workflow appends them to the env file and
restarts monitor). The sending script `telegram-notify.sh` rides in the
deploy archive together with the binaries (deploy.yml puts
`deploy/telegram-notify.sh` into the archive, install unpacks it into
`/opt/crawler/bin` and chmods it 0755) — no separate installation is needed;
the script needs the `jq` package (§4.1). The third condition is network reachability of
`api.telegram.org`: from Russian hostings the Telegram API is often
unreachable (TCP drops while the internet is alive) — delivery then goes
through `https_proxy` in `crawler.env` and a proxy on an external VPS.
Step by step — [`operations.md`](operations.md) §4.3 (secrets, script,
proxy) and §5 (the 24.10 entry — proxy installation); without the secrets, alerts
and reports remain Warn/Info in monitor's journald.

### 4.6. Units and startup

```bash
cp deploy/systemd/crawler*.service deploy/systemd/crawler*.timer deploy/systemd/crawler.target /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now crawler.target \
    crawler-chromium.service crawler-chromium-recycle.timer crawler-ingest.service crawler-scheduler.service \
    crawler-dns-worker.service crawler-http-worker.service crawler-render-worker.service \
    crawler-analyzer.service crawler-monitor.service crawler-web.service \
    crawler-dashboard.service
```

Infrastructure is enabled as packages: `systemctl enable --now postgresql
redis-server unbound`.

### 4.7. First start: migrations

There is no separate migration step: every service applies the PostgreSQL
migrations at startup under an advisory lock (T-002, internal/migrate — docs/services.md,
"Lifecycle"). The service that comes up first applies the schema; the others
wait for the lock and see the current version. The order of enabling units does not matter.

### 4.8. Drive SMART/TBW monitoring

The `deploy/smartd/` set (T-089, spec §10.3): the smartd config (critical
warning, error log growth, temperature), a warning handler in
syslog/journald, and access for the monitor service's wear alert to `/dev/nvme*`
(the `nvme-smart` udev group + a unit drop-in). Installation, verification, and rollback —
[`deploy/smartd/README.md`](../deploy/smartd/README.md); the rationale,
the TBW estimate from the load, and how the layers relate — [`disks.md`](disks.md).

### 4.9. The inbound NRD smet.cz feed (cron)

The [`deploy/nrd/`](../deploy/nrd/) set: a daily automatic feed of the new-domain list (NRD) of
smet.cz (~5·10⁵ domains/day, various zones) into the ingest watched folder. Cron runs the script at **05:33 UTC**
(moved from 20:33 UTC 26.10, T-205: applying the daily file took
minutes in the evening CPU-starvation window and the midnight storm of the daily
re-check; the morning is outside the starvation hours, and the source's day has been closed for >9 h —
the closed-days contract is preserved); the script downloads the **closed day** —
`https://smet.cz/nrd/data/daily/<yesterday's date in UTC>.txt.gz` (~2.6 MiB) — into `<ingest.dir>/add/nrd-<date>.txt.gz`
following the delivery contract ([`incoming-files.md`](incoming-files.md) §2:
the temporary `*.part` name is ignored by the watcher, + an atomic
rename). The list is **zone-less**: zones are derived from the domains themselves
and created automatically (`incoming-files.md` §1); splitting into
zone directories is not required; `.gz` is parsed as an archive (§9, one member).

```bash
install -m 0755 deploy/nrd/fetch-nrd.sh /usr/local/bin/crawler-fetch-nrd
install -m 0644 deploy/nrd/crawler-nrd.cron /etc/cron.d/crawler-nrd
```

Nuances (details — [`deploy/nrd/README.md`](../deploy/nrd/README.md)):

- **why yesterday's day and not `today.txt`**: the source's days are
  immutable `/daily/<date>` files ("it grows until the day is closed, then does not
  change", kept for 90 days); generation runs ~20:10–20:19 UTC; the
  current day's file is absent before it (404) and right after it is still growing.
  At 05:33 UTC yesterday's file has been closed for > 9 h (the generation of the previous
  day) and is unchanged — the day's composition is guaranteed complete; the day is read
  from the name. The source's documentation itself recommends fetching only closed
  days;
- **cron has no time zone**: in `crawler-nrd.cron` the hours are converted
  to the server's local time (Europe/Moscow, UTC+3 → 08:33); when the
  server's zone changes (`timedatectl`), convert back to 05:33 UTC.
  The `/etc/cron.d` files on the server are executed by `systemd-cron` (the classic
  cron daemon is not installed): the timer is visible as
  `systemctl list-timers 'cron-*'` → `cron-crawler-nrd-root-0.timer`;
- **failures** (network/HTTP, a non-gzip response): the file is not delivered at all,
  a `FAILURE: …` entry in `journalctl -t crawler-nrd-fetch`; the retry —
  the next day. A day is kept for 90 days; a missed run
  is restored manually: `/usr/local/bin/crawler-fetch-nrd` or
  `CRAWLER_NRD_DAY=<date> …` for any day in depth; bulk backfilling
  of history/gaps — `deploy/nrd/backfill-nrd.sh` (all closed days
  earlier than `NRD_BEFORE` per manifest.json, idempotent). Success — the line
  `OK: day … closed` with the `sha256` of the `.gz` bytes matching the journal field
  `ingest_files`. Redelivering the same day — a `sha256` duplicate,
  not processed (`incoming-files.md` §5).

One more step — the operator's configuration (like the units and nginx, [`ci-cd.md`](ci-cd.md)):
the deploy pipeline does not install or update it.

## 5. Operations

```bash
systemctl list-units 'crawler-*'        # status of all services in the set
systemctl stop crawler.target           # the whole set (graceful: PartOf=);
                                       # the recycle timer is shut down with the set (§2.2, T-215)
journalctl -t crawler-scheduler -f      # logs by SyslogIdentifier
curl -fsS 127.0.0.1:9102/livez         # liveness of each service
curl -fsS 127.0.0.1:9102/healthz       # readiness (DB pool); /metrics — same port
systemctl kill -s HUP crawler-analyzer  # reload Laravel rules without a restart
/opt/crawler/bin/crawlerctl status      # CLI: status, search, control (spec 4.1)
```

- **Restarts**: `Restart=on-failure`, `RestartSec=5s`, the limits
  `StartLimitIntervalSec=300` / `StartLimitBurst=10` — after 10 failed
  starts within 5 minutes the unit goes to failed and waits for the operator
  (`journalctl -u …` + `systemctl reset-failed crawler-… && systemctl start
  crawler-…`). This is protection against an endless crash loop (for example, PostgreSQL
  unreachable), not against normal failures: a one-off crash restart does not
  count toward the limit (it resets with time).
- **Version updates: the CD boundaries and manual steps.** The deploy pipeline
  ([`ci-cd.md`](ci-cd.md)) replaces **only the binaries** in `/opt/crawler/bin`.
  The systemd units, `/etc/crawler/config.yaml`, and the infrastructure configs
  (postgresql/redis/unbound) are **not touched** by the pipeline — for a release that
  changes `deploy/systemd/*` or the configuration defaults:
  1. before/right after the deploy, sync the changed units:
     `cp deploy/systemd/<unit> /etc/systemd/system/ && systemctl
     daemon-reload && systemctl restart <unit>` (examples: MemoryMax=900M,
     T-143; MemoryHigh=850M, T-145; CPUWeight/CPUQuota, T-158 — §2.1;
     MemoryMax unbound 800M / dns-worker 384M, T-160 — §2;
     MemoryMax render-worker 256M / contexts 8, T-169 — §2);
     the temporary `systemctl set-property` drop-ins
     (`/etc/systemd/system.control/`) must be removed so the unit file
     is the single source of truth;
  2. reconcile `/etc/crawler/config.yaml` with the new defaults: **explicit
     values in the config override the code defaults** (incident 22–23.10.2026:
     `scheduler.batch_size: 5000` and `render.contexts: 3` overrode the new
     defaults of 1000/4 of wave 21 — the fix's effect did not show up until a manual edit);
  3. re-running the deploy after fixing the failure cause —
     `gh workflow run deploy.yml` (workflow_dispatch, deploys the current
     `master`): the automatic rollback (healthcheck) has already restored `bin.prev`;
     the server stays on the previous version until the next run.
- **Version rollback** (the last successful deploy — `/opt/crawler/bin.prev`):
  `deploy/deploy-rollback.sh` — stop → restoring `bin.prev` → start →
  unit verification; the pipeline runs the same script automatically when the
  healthcheck fails ([`ci-cd.md`](ci-cd.md)). Only the binaries are restored:
  the DB schema stays at the new release's version; a release with a migration is not
  restored by a rollback ([`operations.md`](operations.md) §4.1). Manually — over
  SSH from a repository checkout (`ssh root@<server> 'bash -s' -- <
  deploy/deploy-rollback.sh`); the script is not stored on the server — if the
  checkout is unavailable, the same steps by hand: `systemctl stop
  crawler.target && rm -rf /opt/crawler/bin && cp -a
  /opt/crawler/bin.prev /opt/crawler/bin && systemctl start
  crawler.target`.
- **Dependencies**: the units are ordered `After=` the infrastructure, but without
  `Requires=`/a stop binding — a PostgreSQL/Redis failure and restart does not
  stop the workers (the pgx pool reconnects on its own; the queue — Redis
  Streams — survives the restart, and loss is recovered by the scheduler,
  spec 3.4). DB readiness at startup is handled by the same restart cycle
  (migrations exit with code 1 until it appears).
- **Graceful shutdown** (spec 3.4): SIGTERM from systemd → finishing the current
  task or returning it to the queue, the framework's budget of 10 s (`ShutdownTimeout`,
  docs/services.md); the unit's `TimeoutStopSec=30s` gives headroom for closing the pool
  and the metrics HTTP server.
- **Memory**: `systemctl status crawler-http-worker` shows
  `Memory: …` (the current cgroup consumption); the cap is visible in
  `systemctl show crawler-http-worker -p MemoryMax`. Verifying a 24-hour
  run without OOM is the acceptance criterion of spec 11.4 item 7 (task T-093,
  **done** — the 23–24.10 window of ≈25 h: `journalctl -k --since
  "2026-10-23 09:00" --grep 'oom'` is empty — `--since` is mandatory:
  without a window the command will show incident records from before the window, e.g. the cgroup OOM
  of T-157 at 23.10 05:48; unit oom_kill/NRestarts are zero, the peaks
  are within the MemoryMax budgets —
  [`load-measurements.md`](load-measurements.md) §13).
- **Metrics**: `127.0.0.1:9100…9108/metrics` (monitor — 9100, web —
  9107, dashboard — 9108; the second and further render-worker instances — 9109 and
  on, by instance name, §2.2). Scraped by the Prometheus installed on the
  server (its unit is the operator's infrastructure, outside the set; the job
  config — like `deploy/docker/prometheus/prometheus.yml`, host addresses).
- **Dashboard**: the operator's pipeline summary — directly on loopback
  `curl -fsS -u <username>:<password> http://127.0.0.1:8090/api/status`
  (Basic credentials from `dashboard.username`/`password_hash`; the numbers match
  `crawlerctl status` — the shared `internal/status` package); publishing for
  the operator's browser — via a reverse proxy with TLS (§6).
- **Landing page**: the system's public URL for visitors — via a
  reverse proxy with TLS (`curl -fsS https://<domain>/healthz` behind nginx,
  §6 below); the operator dashboard — a separate entrance of the same proxy (§6).
  Directly, the service answers on loopback at `curl -fsS 127.0.0.1:8080/healthz`.
- **Drive SMART/TBW**: smartd and monitor alert warnings — in
  journald (`journalctl -t smartd -p warning`); live readings —
  `smartctl -H -A /dev/nvme0` (percentage_used, TBW). Details —
  [`disks.md`](disks.md) "SMART and TBW monitoring".

## 6. Public entrance: nginx + TLS (T-124; dashboard — dashboard spec §9.1)

The landing page (`crawler-web.service`) listens on loopback (`web.listen`, default
`127.0.0.1:8080`) — a reverse proxy terminating TLS publishes the system
to the outside. The first entrance sample is
[`deploy/nginx/`](../deploy/nginx/) (`crawler.example.conf` + README:
installation, headers, the sitemap.xml scheme, a unix-socket variant instead of
TCP — `CRAWLER_WEB_LISTEN=/run/crawler-web/web.sock` in `crawler.env`).
Only the landing page goes out through the proxy: the web metrics and probes
(`127.0.0.1:9107`) are not published (spec §9). HTTP→HTTPS, certbot's ACME
checks, and the security headers are in the sample; edits for the specific
domain are made at the deploy site. The landing page's canonical host is set by
`web.public_host` (T-227, a required §4.5 edit): sitemap.xml
builds the absolute URLs with it, not with the request's `Host` (which the proxy
passes as the visitor's value — without the canon, `<loc>` would depend on a
foreign header; an empty field — a fallback to `Host`, for loopback/dev
without publication).

The dashboard (`crawler-dashboard.service`) uses the same scheme (dashboard spec §9.1):
it listens on loopback `127.0.0.1:8090`; to the outside — a separate nginx server block
for its own domain/subdomain (for example `dashboard.crawler.example.com`); the block
below is reduced to the specifics — take the server for HTTP→HTTPS with certbot's ACME
checks and the HSTS header from the landing page block above. TLS is required:
authorization is HTTP Basic; credentials must not be transmitted in plaintext outside
loopback (dashboard spec §7.1); `X-Real-IP` is needed for the per-IP brute-force
protection (dashboard spec §7.6 — the service trusts the header from a loopback peer),
`X-Forwarded-Proto` — for the `Secure` flag of the cookie session (the service trusts the header from the same peer,
T-227: behind a TLS entrance the browser will not carry the session over plaintext on a direct
visit bypassing the proxy). The flag is set at cookie issue time: sessions
issued before TLS/XFP is enabled stay without `Secure` until their TTL
expires (24 h) — closing them immediately is possible by changing `dashboard.password_hash`
(it invalidates all sessions). The dashboard metrics and probes
(`127.0.0.1:9108`) do not go through the proxy:

```nginx
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name dashboard.crawler.example.com;
    ssl_certificate     /etc/letsencrypt/live/dashboard.crawler.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/dashboard.crawler.example.com/privkey.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    add_header X-Content-Type-Options "nosniff" always;
    location / {
        proxy_pass http://127.0.0.1:8090;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## 7. Set boundaries (adjacent tasks of stage 9)

- **T-086** — postgresql config tuning for NVMe (docs/db.md): the server's
  allocations must fit into the drop-in's `MemoryMax=3G` (including maintenance
  peaks — work_mem/maintenance/autovacuum on top of shared_buffers).
- **T-087** — the redis config: `maxmemory` ≤ 512M and eviction — under the
  drop-in's cap (eviction before OOM).
- **T-088** — kernel sysctls (`ip_local_port_range`, `tcp_tw_reuse`,
  `fs.file-max`/`nr_open`, `nf_conntrack`, UDP buffers). The units pin
  only the per-process `LimitNOFILE=65535` — parity with the dev environment
  (`deploy/docker/compose.yaml`, ulimits); the sysctl cap is owned by T-088.
- **T-089** — disks and SMART — [`disks.md`](disks.md): the PostgreSQL
  volumes (`/var/lib/postgresql`) and content (`/var/lib/crawler/content`) —
  LVM/ext4/`noatime`, installation step §4 above; NVMe wear monitoring —
  smartd + the monitor unit's access to `/dev/nvme*` (`deploy/smartd/`, §4.8).

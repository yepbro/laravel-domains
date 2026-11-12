# Operations: startup and maintenance

The operator's entry point: from a bare server to routine operations.
Specialized topics live in separate documents (the map below); this page
holds the step-by-step "what to do" scenarios and routine maintenance.
For development — [`dev-environment.md`](dev-environment.md).

## 1. Documentation map

| Document | What's inside | When to read |
|---|---|---|
| [`README.md`](../README.md) | quick start, repository structure, CI | first acquaintance |
| **this page** | startup scenarios, updates, logs/metrics/alerts | operations |
| [`dev-environment.md`](dev-environment.md) | the Docker dev environment: profiles, testbed, CLI, diagnostics | development and debugging |
| [`docker.md`](docker.md) | the Docker Compose cheat sheet (running, commands, volumes) | working with the dev stack |
| [`configuration.md`](configuration.md) | the configuration reference: all sections/fields/env, validation | any config.yaml edit |
| [`incoming-files.md`](incoming-files.md) | the contract of the `incoming/` input files | feeding domains |
| [`services.md`](services.md) | the service framework, lifecycle, metrics and alerts | service behavior |
| [`schema.md`](schema.md) / [`migrations.md`](migrations.md) / [`db.md`](db.md) | the DB schema, migrations, the PostgreSQL pool and tuning | working with data |
| [`redis.md`](redis.md) | the task queue and the Redis configuration | queues |
| [`content.md`](content.md) / [`render.md`](render.md) / [`analyzer.md`](analyzer.md) | the content store, render, the analyzer | pipeline stages |
| [`runbooks.md`](runbooks.md) | procedures: the zstd dictionary, `q:dead`, re-analysis | as needed |
| [`mutation-testing.md`](mutation-testing.md) | mutation testing: `make mutate`, thresholds, reports | test quality |
| [`deploy.md`](deploy.md) | the production systemd deploy: units, the memory budget | server installation |
| [`ci-cd.md`](ci-cd.md) | the GitHub Actions continuous deploy: build, install, rollback | software updates |
| [`disks.md`](disks.md) / [`os-tuning.md`](os-tuning.md) | volumes and SMART, the production server's sysctl | server preparation |
| [`load-measurements.md`](load-measurements.md) | the production server's load measurements (T-092): stage rates, memory, the cycle estimate | capacity planning |

## 2. Scenario: first startup (dev)

The full instructions — [`dev-environment.md`](dev-environment.md) §1–3;
here — the checkpoint steps and the "it works" criteria.

1. `cp .env.example .env` — environment (Linux, Docker ≥ 24, Compose v2;
   no Go needed on the host).
2. `make up` — infrastructure + testbed + the apps; the first start
   takes 5–15 minutes (image builds).
3. `make ps` — wait for `healthy` on all services (the healthcheck —
   `/livez` on the metrics port; the readiness `/healthz` with a DB check
   is separate, [`services.md`](services.md) "Liveness and Readiness Probes").
4. `make seed` — test domain lists in `var/incoming/`; watch the
   processing: the files move to `var/incoming/processed/` (the contract —
   [`incoming-files.md`](incoming-files.md)).
5. Check the pipeline: `make status` (a per-zone/queue summary),
   `make domain NAME=<seed domain>` (status, results, signs).
6. `make test` / `make test-integration` — environment checks.

Next: feeding your own lists ([`incoming-files.md`](incoming-files.md)),
the Laravel rules (§5 of dev-environment), monitoring (§4.3 below).

## 3. Scenario: production deploy

The kit: [`deploy/systemd/`](../deploy/systemd/) (T-085), the instructions —
[`deploy.md`](deploy.md). The order of the first deployment (every step
in detail — via the links):

1. **Disks** — the `/var/lib/postgresql` and `/var/lib/crawler/content`
   volumes (LVM/ext4/`noatime`) **before** installing the packages:
   [`disks.md`](disks.md) §4.
2. **OS packages** — `postgresql redis-server unbound smartmontools` +
   the memory-budget drop-ins + the app role/DB + the unbound config:
   [`deploy.md`](deploy.md) §4.1 (the Chromium headless-shell dependencies —
   a separate list there too, §4.3, installed at step 4).
3. **Sysctl** — `deploy/sysctl/99-crawler.conf` into `/etc/sysctl.d`
   (ports/TIME_WAIT/conntrack/UDP buffers):
   [`os-tuning.md`](os-tuning.md), `deploy/sysctl/README.md`.
4. **Binaries** — the static builds into `/opt/crawler/bin` and Chromium
   headless-shell into `/opt/crawler/chromium` with its dependency packages
   (`libnspr4 libnss3 fontconfig` + fonts: without a font stack Skia brings
   the browser down with a FATAL on some pages — a crash loop into the
   start limit): [`deploy.md`](deploy.md) §4.2–4.3.
5. **Users and directories** — `crawler-<service>` in the `crawler` group,
   working directories with permissions: [`deploy.md`](deploy.md) §4.4.
6. **Configuration** — `/etc/crawler/config.yaml` (the edits required
   against the example — DSN/paths/`render.remote_url`/`monitor.listen`/
   `http.user_agent`: [`deploy.md`](deploy.md) §4.5; the reference —
   [`configuration.md`](configuration.md)) and `crawler.env`.
7. **Units and startup** — `systemctl enable --now crawler.target …`:
   [`deploy.md`](deploy.md) §4.6. Migrations apply automatically at
   startup (no separate step, §4.1 below).
8. **SMART monitoring** — `deploy/smartd/`: [`deploy.md`](deploy.md)
   §4.8, [`disks.md`](disks.md) §6.
9. **Verification**: `systemctl list-units 'crawler-*'` (all active),
   `curl -fsS 127.0.0.1:9100…9108/healthz`,
   `/opt/crawler/bin/crawlerctl status`; the font stack —
   `fc-list | grep -q . && echo "fonts ok" || echo "FONTS MISSING"`
   (FONTS MISSING — no fonts, render goes into a crash loop up to the
   start limit; [`deploy.md`](deploy.md) §4.3).
10. **Feed the domains** — files into `/var/lib/crawler/incoming/`
    (the contract — [`incoming-files.md`](incoming-files.md)).
11. **The incoming NRD feed** (optional) — a cron export of the smet.cz
    new-domains list into `incoming/add/` daily at 20:33 UTC:
    [`deploy/nrd/`](../deploy/nrd/), [`deploy.md`](deploy.md) §4.9.

The memory layout (MemoryMax per unit) and the rationale — [`deploy.md`](deploy.md) §2.

## 4. Maintenance

### 4.1. Software updates and migrations

**Dev.** `git pull` → `make up`: whatever changed is rebuilt, air keeps
watching the code; after changing dependencies — `make mod-download`.
The services apply the migrations themselves at startup (a container restart).

**Production.** The normal path is CI/CD: a push/merge to `master` with a
green CI deploys automatically (build → `/opt/crawler/bin` → restart →
healthz). The rollback is automatic if any step fails after the set is
stopped (spec §10.4): an install failure — via the install script's trap,
a healthcheck failure — by the pipeline's rollback step
(`deploy/deploy-rollback.sh`, restoring
`bin.prev`); the exceptions — the first deploy without `bin.prev`, a
failure to stop the set, and a broken ssh connection — then the
notification calls the operator directly ([`ci-cd.md`](ci-cd.md)). Every
deploy brings a Telegram notification: ✅ success; ⛔️ failure with the
failing step and the rollback fact (done / done but not all units active /
nothing to roll back / failed / unknown). "Operator required" in the text
is the signal to investigate via [`ci-cd.md`](ci-cd.md) and `journalctl`.
The manual path (outside CI):

1. Build the binaries (a machine with Go or `make shell` in the dev
   environment): the `CGO_ENABLED=0 go build …` loop —
   [`deploy.md`](deploy.md) §4.2.
2. Install into `/opt/crawler/bin` (the atomic-replacement script —
   `deploy/deploy-install.sh`, the check — `deploy/deploy-healthcheck.sh`).
3. `systemctl restart crawler.target` — **all units at once**, not one
   by one: the storage pack-file interprocess protocol (flock) assumes a
   single software version among the writers and compaction
   ([`content.md`](content.md) "Exclusivity for writers").
4. The services will apply the new migrations themselves at startup:
   the advisory lock serializes simultaneous starts, the start order does
   not matter, restarts are idempotent ([`migrations.md`](migrations.md)).

**Schema version control** (what has been applied):

```sql
-- make psql / sudo -u postgres psql -d crawler
SELECT * FROM schema_migrations;   -- version, dirty = false
```

A schema rollback (`migrate-down`) — only manually and deliberately: the
services can only apply up; running downgraded software on a rolled-back
schema is not supported. Recovery from an interrupted migration (dirty) —
[`migrations.md`](migrations.md), the "Recovery from a failure" section.

### 4.2. Logs

| Where | Command |
|---|---|
| dev | `make logs SERVICE=<name>`; `dc logs -f --tail=100 <service>` ([`docker.md`](docker.md) §2) |
| production, services | `journalctl -t crawler-<service> -f` (SyslogIdentifier; the names — `crawler-ingest`, `crawler-scheduler`, `crawler-dns-worker`, `crawler-http-worker`, `crawler-render-worker`, `crawler-analyzer`, `crawler-monitor`, `crawler-web`, `crawler-dashboard`) |
| production, OS packages | `journalctl -u postgresql -u redis-server -u unbound` |
| SMART | `journalctl -t smartd -p warning` ([`disks.md`](disks.md) §6) |

The format is structured slog, every record carrying a `service=<name>`
attribute; ingest's row-level rejects go to a separate
`service=rejected` stream. The level — the `log.level` configuration
(`CRAWLER_LOG_LEVEL`, default `info`).

### 4.3. Metrics and alerts

**Metrics.** Each service serves `/livez`, `/healthz` and `/metrics` on
its own port (the address table — [`services.md`](services.md) "Metrics";
production — loopback). The key pipeline metrics
([`services.md`](services.md), the "Working Logic Metrics" table):

- `crawler_stage_tasks_total{stage}` / `crawler_stage_seconds` —
  the throughput of the dns/http/render/analyze stages;
- `crawler_queue_depth{queue}` — the depth of the `q:*` queues (the
  backlog of the consumer groups; the lag is bounded by the stream length
  — the T-168 phantom clamp);
- `crawler_queue_dead_buried_total{queue}` /
  `crawler_queue_dead_unidentified_total{queue}` /
  `crawler_redis_memory_ratio` — task deaths (moves to q:dead, T-154),
  quarantines of unidentifiable entries without a `task_id` (T-260), and
  the `maxmemory` fill of Redis;
- `crawler_backlog_domains{state}` — pending/queued in the DB;
- `crawler_domain_status_total{type}` — domain statuses;
- `crawler_laravel_near_findings` — Laravel findings in the gray zone of
  the threshold (rule calibration material, T-115);
- `crawler_laravel_appeared_total` / `crawler_laravel_lost_total` —
  appearances/disappearances of Laravel domains (the T-118 detection history);
- `crawler_web_checks_total{result}` / `crawler_web_check_errors_total{type}`
  / `crawler_web_check_seconds` — the health of the public check form
  (web, T-221): request outcomes, failures by classes (a growth of
  `type="journal"` = the loss of `web_checks` log rows, `type="non_ip"` =
  the proxy does not pass the visitor's address), duration;
- `crawler_service_up{service}` — liveness (including the full death
  of the process).

Prometheus: dev — the `monitoring` profile (the UI at 127.0.0.1:9090);
production — the operator's Prometheus, the jobs config modeled on
`deploy/docker/prometheus/prometheus.yml` ([`deploy.md`](deploy.md) §5).

**CLI summary** — `crawlerctl status` (dev: `make status`): progress by
zone (total/checked), the queue lengths including `q:dead`, the rate
(domains/day, the average since the start of the day) and the ETA of the
first cycle's completion; `--json` — machine output.

**Alerts** of the minimal spec 5.9 set are evaluated by the monitor
service on a `monitor.alerts_interval` cycle (default 5 min — T-206; the
counter reconciliation runs separately, once per
`monitor.zone_stats_interval`, 30 min since T-199): the DNS error share,
queue overflow, a progress stall, disk space shortage (a percent and an
absolute minimum with escalation, T-162), NVMe wear,
a burst of junk `content_hash` values when purging removed domains
(T-099), OOM kills and crash restarts of the crawler units (T-145; the
render engine units — `crawler-*.service` — are covered by the same
alert), untriaged `q:dead` and Redis memory (T-154), MemoryHigh
throttling, a frozen queue and PSI CPU (T-162), the growth of the render
fallback queue depth, the fallback share budget excess and the growth of
the render stage's browser errors (T-246/S-P2-6, thresholds — §4.4), the
"status hung" domains `new ∧ done` older than a day (T-256, FSM spec
FR-2.2 — lost `q:http` tasks). The signal, on transitions: a **Warn in
the monitor log/journald** and, when delivery is enabled, a **Telegram
message** (below) —

```bash
journalctl -t crawler-monitor -p warning      # production
make logs SERVICE=monitor | grep -i warn      # dev
```

the thresholds and the rationale — [`services.md`](services.md) (the
alert sections); disk space — `content.disk_free_percent` and
`monitor.alerts.disk_free_*_bytes` ([`content.md`](content.md) "Disk
limits and overflow", [`configuration.md`](configuration.md)), SMART —
[`disks.md`](disks.md) §6. The effective alert windows —
[`services.md`](services.md) (the alert evaluation cycle); T-206 returned
the former alert frequency (5 min) alongside the 30-min reconciliation.

**Alert delivery to Telegram** (T-162): fired and cleared alerts arrive
as messages in the same dedicated group as the CI/CD notifications
(spec §10.4) — "🚨 Alert …" on firing and "✅ Recovered …" on clearing;
duplicates are debounced (one episode = two messages), the monitor log
does not flood. Enabled by default (`monitor.alerts.telegram`); it
actually works under two conditions:

1. Secrets in `/etc/crawler/crawler.env` (the monitor unit's
   EnvironmentFile, the sample — `deploy/systemd/crawler.env.example`):

   ```
   TELEGRAM_BOT_TOKEN=<token from @BotFather>
   TELEGRAM_CHAT_ID=<id of the dedicated group>
   ```

   **both are mandatory**: if either is missing — delivery is off (a
   record in the monitor log at startup), the alerts stay Warns in
   journald — just as a CI step stays silent without the secrets. The
   values live in the GitHub repository secrets
   (`TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` — the same ones as the CI/CD
   notifications) and are not read back from there (write-only), so
   delivery to the server happens either via the ops workflow
   **`ops-telegram-env`** (Actions → Run workflow: with the deploy SSH
   key it appends/replaces both keys in the env file, keeps the
   root:crawler 640 permissions, and restarts monitor — the
   `deploy/telegram-env.sh` script), or manually with an editor on the
   server;
2. The sending script on the host: `/opt/crawler/bin/telegram-notify.sh` —
   **it ships in the deploy archive together with the binaries**
   (deploy.yml copies `deploy/telegram-notify.sh` into the archive,
   install unpacks it into `/opt/crawler/bin` and chmods 0755): no
   separate installation is needed and it survives deploys. Manual
   copying is not needed (the historical "scp once" installation was
   wiped by the very first deploy — install recreates the directory from
   the archive).

   the script needs `bash`, `curl` and `jq` on the server (`jq` — the
   package list in [`deploy.md`](deploy.md) §4.1; without it sending
   fails with exit 127); the path is overridden by
   `monitor.alerts.telegram_script`. A check without sending:
   `TELEGRAM_BOT_TOKEN=x TELEGRAM_CHAT_ID=y /opt/crawler/bin/telegram-notify.sh --message "test" --dry-run`;
3. Network access to `api.telegram.org`: the production server (Russian
   hosting) cannot reach the Telegram API — TCP to `api.telegram.org:443`
   is dropped (RKN blocking; the 24.10 measurement: github.com → 200,
   ya.ru → 302, telegram → timeout over IPv4 and IPv6, DNS correct). The
   symptom — a Warn in the monitor log every 10 minutes: `curl: (28)
   Failed to connect to api.telegram.org … Timeout was reached (HTTP 000)`.
   The solution — an HTTP proxy on an externally reachable VPS (the
   operator's external host `104.253.1.37`): `tinyproxy`, port 3128,
   only `CONNECT 443`, `Allow` only the production server's IP; enabling
   it — the `https_proxy=http://104.253.1.37:3128` line in
   `/etc/crawler/crawler.env` (the script's curl picks the variable up
   from the unit environment automatically). Proxy
   setup/diagnostics/rollback — the operations journal §5 (24.10) and
   `/root/README.crawler-telegram-proxy.txt` on the VPS itself. Without
   the proxy the alerts and reports keep being written to journald — the
   crawler does not suffer, only the delivery is quietly unavailable.

After editing `crawler.env` — `systemctl restart crawler-monitor`.

The same delivery channel carries two more monitor message types: the 📊
load report (T-173, once per `monitor.load_report_interval`) and the
PostgreSQL slow-query notifications 🐌 (T-177, only with
`monitor.slow_query_telegram` — disabled by default): statements running
longer than `monitor.slow_query_min_duration` (3 s) — one message per new
statement (pid+query_start debouncing), with the service label and pid;
without the flag the loop is limited to Warns in the monitor log and the
metrics; the full log of slow statements — the PostgreSQL server log
(`log_min_duration_statement = 3s`, [db.md](db.md) §"Monitoring"). For
reading other sessions' texts a `GRANT pg_read_all_stats` to the
`crawler` role is needed ([deploy.md](deploy.md) §4.1) — without the
grant the message carries the "text unavailable" mark.

The 25.10 lock-cluster analysis (the `domains` convoy, T-192) shows how
to read the slow-query log in a similar incident. The convoy signature:
point PK statements of the write path (writing a result, publishing a
bit, returning a lease) with anomalous durations under perfect plans (an
Index Scan over `domains_default_pkey`, rows 1 — the time is spent not on
the scan but on waiting for a row lock) together with a group of victims
being released in the same second — the fingerprint of a single holder:
as soon as its transaction closes, the whole queue passes instantly. Who
the holder is, the slow-query log does not say — the name comes from
`log_lock_waits = on` (T-192): on a wait longer than `deadlock_timeout`
(1 s) the server logs `process N still waiting for ShareLock on
transaction X …` — the pid, the transaction and the holder's associated
statement. Before T-192 the parameter was off — the ≈37-minute holder of
the 25.10 incident is not recoverable retroactively; the class is closed
by the `lock_timeout = 30 s` cap on the write pools (dns-worker,
http-worker, scheduler — [db.md](db.md) §"AfterConnect hooks"): the
convoy is cut off by a transaction rollback and a delivery retry
(at-least-once) instead of the pipeline standing silently.

The "🧮 Top CPU" row of the load report is the share of a core over the
window (1.0c = one whole core): the delta of the name's utime+stime ticks
over the delta of one core. A name whose processes restarted within the
window (the name's tick sum dropped — 25.10: postgres went 18 → 16
processes and printed "310613997334638.2c", an underflow of unsigned
subtraction) is skipped for one window — an honest absence instead of
junk (T-189); a new name is counted from its full tick sum — the
processes started inside the window.

The "📬 Queues" row of the load report (T-188) — the backlog of the
consumer groups: the same figure as `crawlerctl status` and the
`crawler_queue_depth` metric (the shared `queue.GroupDepth` calculation
with the backpressure gate); `q:render 27529 (−1200, 2.0/s)` reads as
"undelivered remainder + pending — 27,529; 1,200 drained over the
window; deliveries run at 2/s": the ±N delta is the growth or the drain
of the real queue, the rate — deliveries (the acknowledged stream history
up to the XTRIM trim does not affect it — consistency does not depend on
TrimLoop working); a stream without groups and `q:dead` — the inventory
XLEN. Before T-188 the row printed the raw XLEN — the 25.10 discrepancy
(q:render ×5 with the TrimLoop stopped by the freeze, the phantom growth
of q:analyze at lag 0) — see [redis.md](redis.md) §3.

**Junk `content_hash`** (`ingest_bad_hashes`): the ingest service's
`crawler_ingest_bad_hashes_total` counter (results with a wrong-length
hash removed by the retention pass) grew by ≥ 10 over a window of 15
alert passes (≈75 min at the default 5 min, T-206; on the 30-min T-199
cycle this stretched to 7.5 h) —
a software bug in a result writer (T-056: http-worker writing
`subdomain_results` / render-worker replacing the DOM link), not load
noise: the contract invariant is exactly 16 octets (spec 6.1). No data is
lost and the retention pass is not blocked (the poison pill is
neutralized, ING-P2-4): the `refcount` over the junk link did not change,
the rows were deleted. Operator actions:

1. Gauge the scale and the period: the ingest log
   (`journalctl -t crawler-ingest | grep -i content_hash` / dev:
   `make logs SERVICE=ingest`) — every pass with an anomaly writes a Warn
   with the row count; the counter rate —
   `rate(crawler_ingest_bad_hashes_total[15m])` in Prometheus.
2. The junk rows are written by http-worker (the `Recorder`, T-058) and
   render-worker (replacing `content_hash` with the DOM): check their
   logs around the burst time, recall the recent software updates/deploys
   of these services — a writer regression (case analysis — runbooks §2:
   the worker log by the `service` attribute and time).
3. File the bug (a report with the logs and the burst time): the rows
   have already been deleted by the retention pass and cannot be
   verified with SQL — the source of truth about the scale is the Warn
   log and the counter. The cure is fixing the writer; service restarts
   are not needed (the pipeline and the retention pass keep working).

Single values below the threshold (relics planted before the ING-P2-4
fix) do not count as an alert. The alert source is the ingest `/metrics`
probe (`monitor.ingest_metrics_url`): ingest stopped or the metric absent
from the exposition — the alert is "not measured" (a Warn); ingest's
death itself is visible via `crawler_service_up`.

**Render fallback** (`render_fallback_depth` / `render_fallback_budget` /
`render_browser_errors`, T-246/S-P2-6): the observability alerts of the
render engine selection ([`render.md`](render.md)).

- `render_fallback_depth` — the depth of `q:render-fallback` (the backlog
  of worker B's group; on the dashboard — the fallback row of the "Render
  engines" panel, in the metrics — the B build's
  `crawler_queue_depth{queue="q:render-fallback"}`)
  has reached the stream's effective threshold of **7 500** entries and
  (it grows for 2 alert passes in a row (10 min) OR it holds above the
  threshold for 2 passes in a row without growth — a stationary depth: a
  stopped B, pressed against the stream's MAXLEN limit of 15 000, when
  XADD evicts the oldest undelivered entries and the depth stops growing,
  or an episode that survived a monitor restart; S-P2-1) — the chromium
  worker B does not keep up with working off the inflow (the
  `render.fallback.worker_rate` rate below the publication flow, up to a
  stopped B: monitor reads the depth from Redis directly, so the
  worker's death is visible to the alert). The threshold — modeled on
  q:render from the configuration:
  `queue.StreamWatermark(scheduler.queue_high_watermark, redis.maxlen,
  q:render-fallback)` — the configured 100K is clamped by half of the
  stream's MAXLEN limit (15 000 → 7 500; an explicitly low watermark
  gives smaller numbers, as with the working streams). A fired alert
  holds until the drain falls below 90 % of the threshold (6 750).
- `render_fallback_budget` — the suspension-event counter
  `crawler_render_fallback_suspended_total{reason="budget"}` (worker A)
  has grown: the share of fallback publications over the budget window
  exceeded the `render.fallback.share_percent` cap — the primary engine
  is degrading, the fallback rate is beyond the economic cap. Tasks are
  not lost (candidates are finalized normally, FR-9). The event holds the
  alert for a window of 6 passes (≈30 min), cleared by aging. The source
  — the render worker's `/metrics` probe (`monitor.render_metrics_url`);
  an empty address or an unreachable exposition — "not measured" (a Warn
  on a probe error). The `suspended{reason="budget"}` series is
  pre-created at zero since T-250 (S-P1-4): the probe reads 0 from the
  worker's first start (and 0 for an old build without the event — "no
  series" ≠ "not measured"), the first 0→1 event is seen by the alert and
  not lost, including as the first one after a worker restart.
- `render_browser_errors` — the render stage's browser-class error
  counter `crawler_stage_errors_total{stage="render",type="browser"}` (the
  same render worker `/metrics` probe, S-P2-6) grows for 2 alert passes
  in a row (10 min): the candidate engine (moli/lightpanda) is hung or
  unreachable — the health gate has already suspended the fallback
  publications (`suspended{reason="gate"}` — the counter and the
  worker's Warn log; the alert is about the root cause), the browser
  class waits in the redelivery of q:render within the retry window. A
  one-off browser error (a normal session restart) and a worker restart
  (the counter is per-process) — silent. The series is lazy: a worker
  without browser errors has no series — the alert reads 0; a foreign
  exposition (no `crawler_stage_tasks_total{stage=
  "render"}` marker) — "not measured" without a Warn: check the
  `monitor.render_metrics_url` address.

The fall of a render engine unit (`crawler-chromium.service` today,
moli/lightpanda per the render spec §4.7 — the `crawler-*.service`
units) has no dedicated alert: OOM/crash restarts are caught by the
existing `unit_oom` (T-145), the engine's CDP hang/unavailability — by
the `render_browser_errors` alert (the growth of the stage's browser
errors, S-P2-6) and the fallback health gate (`suspended{reason="gate"}`).

### 4.4. Routine checks (checklist)

Daily/as needed:

```bash
crawlerctl status                  # zones, queues, q:dead, ETA
crawlerctl near                    # the gray zone of Laravel detection (T-115)
crawlerctl check <domain>          # ad-hoc triage of a contentious domain
                                   # (T-119): read-only — no writes to the DB
                                   # or the queues; for writing — recheck
# alerts and errors:
journalctl -t crawler-monitor -p warning --since -1d   # production
journalctl -t crawler-nrd-fetch --since -1d            # the NRD feed delivery
                                                       # (deploy.md §4.9:
                                                       # OK daily, FAILURE — manual)
systemctl --failed                 # units in failed (StartLimit)
systemctl show 'crawler-*.service' -p Id -p NRestarts   # unit OOM/restarts (the unit_oom alert, T-145)
df -h /var/lib/crawler/content /var/lib/postgresql    # volumes
```

- **`q:dead` is not empty or grows** (the `queue_dead` alert fires on 3
  passes of sustained depth, T-154: ≈15 min at the default 5 min — T-206;
  the death rate — `crawler_queue_dead_buried_total`, the stream of
  unidentifiable entries — `crawler_queue_dead_unidentified_total`,
  T-260) — triage: [`runbooks.md`](runbooks.md) §2.
- **Frozen queue** (the `queue_frozen` alert, T-162: the group's
  last-delivered-id is not moving for ≥ 15 min at depth > 0 —
  a consumer is not reading the stream, the poison signature of T-159;
  the stillness is evaluated by an alert-cycle pass — the latency adds
  up to the period: at the default 5 min, T-206, a freeze is noticed in
  ≈15–20 min): the log of the stream's worker from the detail
  (`journalctl -t crawler-http-worker` and so on), a worker restart;
  entries that crash the consumer end up in `q:dead` after restarts —
  triage per runbooks §2.
- **Render debt grows** (the `render_debt` alert, T-175: the
  `render_pending` flags + the `q:render` lag above 50K and growing for
  ≥ 2 alert passes in a row — the SPA inflow outpaces the drain; a fired
  alert holds until the drain falls below 45K — a growth stall does not
  re-fire but is visible in the "Render debt" row of the load report and
  the `crawler_render_pending_domains` gauge): compare the rates in the
  load report (the drain of `q:render`, the inflow) and `render.contexts`
  — when render capacity is exhausted, raise the contexts/add a second
  Chromium (deploy.md §2.2), temporarily freeze the scheduler inflow
  (the operations journal §5); the flag counter is maintained by bit
  transitions — manual bit operations adjust the counter (runbooks §6).
- **The render fallback queue grows** (the `render_fallback_depth`
  alert, T-246: the depth of `q:render-fallback` ≥ 7 500 and growing for
  ≥ 2 alert passes, OR holding ≥ 7 500 without growth for 2 passes —
  a stationary depth: a stopped B or pressed against the MAXLEN limit
  (S-P2-1); cleared — a drain below 6 750): compare the consumption rate
  of `crawler_render_fallback_consumed_
  total` (is worker B alive? `systemctl status crawler-render-fallback-
  worker`, the metric — port 9120) against the publication flow
  `crawler_render_fallback_published_total`; the rate is capped by
  `render.fallback.worker_rate` (a 1 min window) — raise the cap or
  start a second worker B modeled on render-worker@ (deploy.md §2.2);
  a growing publication flow with a healthy B is a signal of primary
  engine degradation (look at `render_fallback_budget` and the
  finalization reasons `crawler_render_task_finalized_total{reason}`);
  entries lost to the MAXLEN trim while undelivered are brought back by
  the T-259 late republisher (http-worker, once a minute): its metric
  `crawler_render_late_republish_total` grows after an incident (a loss
  drain) and decays back to zero — a stuck non-zero value with an empty
  `q:render` is a reason to look at the http-worker log (render.md
  §"Late republisher").
- **The fallback share budget is exceeded** (the `render_fallback_budget`
  alert, T-246: the suspension event `crawler_render_fallback_suspended_total
  {reason="budget"}` — the share of publications over the budget window
  exceeded the `render.fallback.share_percent` cap; cleared by aging in
  ≈30 min): the primary engine is degrading — the fallback rate and the
  per-engine success rate are on the dashboard's "Render engines" panel
  and the stage metrics; decide: roll the engine back to chromium (the
  render spec §6.4, the worker profile — an edit of
  `render.engine` + a restart) or revisit the share cap; tasks are not
  lost during a suspension (normal finalization with the marker).
- **Render browser errors grow** (the `render_browser_errors` alert,
  S-P2-6: `crawler_stage_errors_total{stage="render",type="browser"}`
  grows for ≥ 2 alert passes — the candidate engine is
  hung/unreachable; cleared by the growth stopping): fix the render
  engine — the health gate status (`suspended{reason="gate"}` in the
  `/metrics` of the render worker, the Warn logs), the engine unit
  (`systemctl status crawler-chromium.service` and so on); if it cannot
  be fixed quickly — roll back to chromium (the render spec §6.4:
  `render.engine` + `render.remote_url`, a restart of worker A; worker B
  stays on chromium — §2.3); tasks are not lost while the gate is alive
  (the q:render redelivery; after exhaustion — normal finalization).
- **"Status hung" domains** (the `domains_new_done_stale` alert, T-256:
  the `crawler_counters['new_done_stale']` row is non-zero in 2 NEW
  reconciliation snapshots in a row — the series grows only when the
  row's `updated_at` changes; repeated reads of one snapshot (the alert
  runs every 5 min, the source — once per 30) do not count as
  observations, ≈1 h of a sustained value; domains
  `status = new ∧ check_state = done` with `last_checked_at` older than
  a day; the level — the `crawler_domains_new_done_stale` gauge): the
  DNS stage recorded "has addresses" but the HTTP outcome did not arrive
  — lost `q:http` tasks (the status specification —
  `internal/model/enums.go`). The cohort is treated by the T-270
  compensator (an early recheck: the scheduler's hourly pass returns
  new∧done older than 24 h to `pending` by itself, the counter
  `crawler_scheduler_stale_requeued_total`, runbooks.md §2.10) —
  usually no action is needed; the alert remains a signal about NEW
  losses: cross-check the cause (a Redis restart — the parallel
  `redis_restart` alert; an OOM window — the http-worker log) and watch
  that the compensator drains the cohort (the counter grows, the gauge
  falls back to zero within an hour after the reconciliation). If it
  does not drain — the analysis per runbooks.md §2.10.
- **MemoryHigh throttling** (the `memory_high` alert, T-162: the unit's
  memory.events `high` counter grows for ≥ 2 passes in a row —
  sustained reclaim under the soft cap, the herald of the T-157
  fatals): check `systemctl status crawler-http-worker` (Memory: at the
  MemoryHigh=850M cap) and the unit log; recurring throttling is a
  reason to revisit the budget ([`deploy.md`](deploy.md) §2), the
  metric — `crawler_unit_memory_high_events`.
- **CPU starvation** (the `psi_cpu` alert, T-162: PSI some avg300 >
  90 % sustained for 30 min — all the stages drown at once):
  `cat /proc/pressure/cpu`, the top consumers (`ps aux --sort=-%cpu`),
  cross-check with the competitors on the server (the postgres
  aggregates, compaction); the metric —
  `crawler_psi_cpu_some_percent`.
- **The content volume is near the threshold** (`content.disk_free_percent`,
  5 %; the T-162 absolute minimums —
  `monitor.alerts.disk_free_min_bytes` 15 GiB Warn /
  `disk_free_crit_bytes` 10 GiB escalation) —
  content writing has paused (the disk_free/disk_free_min alerts):
  free up space (compaction runs by itself), expand the volume
  ([`disks.md`](disks.md)).
- **Unit OOM/restarts** — the `unit_oom` alert (T-145): one Warn per
  episode — `oom`/`oom_kill` (the unit cgroup's memory.events) or
  `NRestarts` > 0. Resetting an episode: `systemctl reset-failed` (or a
  new start) zeroes NRestarts, but the `oom`/`oom_kill` counters of a
  live unit persist until the cgroup is recreated — a unit restart; the
  deploy resets everything itself (stop → `reset-failed` → start in
  deploy-install.sh), manually —
  `systemctl restart crawler-<service>`. The metrics —
  `crawler_unit_oom_events{unit,kind}` / `crawler_unit_restarts{unit}`
  ([`services.md`](services.md)); the source — `systemctl show` + reads
  of cgroup v2, in the dev container the alert is "not measured". The
  event is a symptom of the memory budget not being met (http-worker —
  MemoryHigh/MemoryMax, [`deploy.md`](deploy.md) §2): investigate via
  `journalctl -k --grep oom` and the unit log.
- **Memory**: `systemctl status crawler-<service>` shows `Memory:`;
  the cap — `systemctl show -p MemoryMax` (http-worker — also the soft
  `-p MemoryHigh`, T-145). A daily run without OOM — the spec 11.4
  item 7 criterion (T-093, **met** on 24.10: `journalctl -k --since
  "2026-10-23 09:00" --grep oom` is empty — the window in `--since` is
  mandatory, without it the pre-window incident records are visible;
  the peaks against the MemoryMax budgets —
  [`load-measurements.md`](load-measurements.md) §13); the current
  check: `journalctl -k --grep oom` is empty,
  restarts — via `NRestarts` (covered by the unit_oom alert, T-145).
- **Redis memory** — `crawler_redis_memory_ratio` (the `redis_memory`
  alert at ≥ 80 % of `maxmemory`, T-154; with noeviction — a warning of
  write rejections): cross-check with `redis-cli INFO memory`, the
  analysis — [`redis.md`](redis.md).
- **Redis restart** — the `redis_restart` alert (T-270: the uptime from
  `INFO server` < 15 min): the streams' undelivered entries are lost —
  q:dns will return them by lease, the q:http losses with a recorded
  done — the T-270 compensator (`new_done_stale` drains within an hour,
  runbooks.md §2.10); cross-check the restart cause (the unit/OOM/the
  host), the analysis — [`redis.md`](redis.md).
- **SMART/TBW** — `smartctl -H -A /dev/nvme0`, the smartd warnings:
  [`disks.md`](disks.md) §6.
- **Fault tolerance**: a PostgreSQL/Redis fall does not stop the workers
  (reconnect/`Warn` loops, spec 3.4); a Redis loss is recovered by a
  lease pass within ≈ `scheduler.lease`
  ([`redis.md`](redis.md)).

### 4.5. Stop and restart

Graceful shutdown is built into the framework (spec 3.4): SIGTERM →
finishing the current task or returning it to the queue (a 10 s budget)
→ closing the DB pool.

```bash
systemctl stop crawler.target      # the whole set (production); per-unit: systemctl stop crawler-http-worker
make down                          # dev (the volumes are kept)
```

The chromium recycle timer (`crawler-chromium-recycle.timer`, T-208) is
part of the set (deploy.md §2.2, T-215): `PartOf=crawler.target` in both
units of the pair stops the service and the timer together with the set
— the next tick (every 2 h) will not bring up a stopped chromium;
`WantedBy=crawler.target` brings the timer back at the start of the set.
A separate stop when halting the set is not needed. Stopping a single
`crawler-chromium.service` does not stop the timer — the nearest tick
(≤2 h) will bring the browser back; with the set running this is normal.

Restarting any service is safe: unfinished tasks come back via duplicate
delivery, domains — via a lease pass (spec 11.4 item 9).

### 4.6. Quiet window: stopping the scheduler for the retention drain

A temporary stop of the front (`crawler-scheduler`) so that the heavy
http-worker cycles — the content retention pass, pack compaction, the
render late republisher — can work off the backlog without competing
with the full front for CPU/IO/PG memory. Precedents: 2026-10-24 (the
q:render drain), 2026-11-10…11 (the retention drain after the T-273
hotfix; the journal — §5).

**Trigger criteria** (any one suffices):

- the retention pass is stuck: `crawler_content_retention_errors_total`
  grows (a WARN series of "content retention pass" with err=),
  `freed_bytes_total` does not grow, disk/content grow while the
  pass stands still;
- the system is in the red zone under the full front: swap > ~3 GiB,
  serial PG OOM kills, available < ~1.2 GiB (losing the front for hours
  is not critical: the share of the daily output is small compared to
  the risk of losing the disk).

**Important before stopping:**

- retention/compaction live in http-worker — stopping the scheduler
  does NOT stop them (the 10-24 freeze of the DB inflow is only
  partially applicable either — see the journal: the `render_pending`
  bit is published by http-worker itself);
- the queue tail takes another ~2–3 h to work off (q:dns batches × 1000
  domains): the load does not fall instantly, full quiet comes after the
  tail is finished; draining a day's backlog — a 12–24 h window (11.11:
  the first pass ~60 min just under the wire + ~13 h of overnight drain);
- **a deploy will bring the scheduler up by itself** (install starts
  crawler.target; the precedents 10-24 13:07 and 10-11 14:58), and
  **a server reboot — all the more so**
  (all enabled units start at boot; the precedent 11-11 11:03 —
  an external power-cycle of the VM cut a freeze short) — a window that
  must survive a deploy or a reboot has to be re-established by stopping
  again;
- an http-worker restart no longer resets the retention drain (T-274):
  the cursor is persistent in the DB (`retention_drain_cursor`), the
  process continues from the saved position — before T-274 every restart
  meant a new full catch-up from scratch (an hour of traversing the
  clean prefix, the 10–11.11 measurements). The selection cap is changed
  by the `content.retention_select_timeout` key (default 2 h) without
  a deploy.

```bash
systemctl stop crawler-scheduler   # the unit stays enabled
```

**Success metrics during the window:**

- `crawler_content_retention_errors_total` is not growing, the WARN
  series has stopped; in `pg_stat_activity` the live batch loop is
  visible (SELECT over subdomain_results → commit);
- the `contents` n_live_tup falls (`pg_stat_user_tables`), the oldest
  pack grows younger, `du /var/lib/crawler/content` decreases; the
  hourly INFO "pack compaction pass" with freed_bytes > 0 — disk going down;
- after a catch-up pass completes: `retention_pass_seconds_count` > 0,
  rows/contents/freed_bytes_total — batched increments. **Zero counters
  during a long live pass are normal**: the increment lands at the end
  of the pass (T-273); watch progress via contents/packs/compaction;
- after the render-late gate frees up: `crawler_render_late_errors_total`
  is not growing.

**Bringing the front back:**

```bash
systemctl start crawler-scheduler
```

Preferably after `pass_seconds_count` > 0 (a pass has completed) and/or
at a normal disk and steady progress (since T-274 the drain cursor is
persistent in the DB — an http-worker restart continues from the saved
position, a single-batch timeout does not reset the drain; before that
the cursor lived in the process memory and every window required
finishing a pass before a restart). Watch for 1–2 h after the start:
the load returns to 20+ (the render norm —
[`load-measurements.md`](load-measurements.md)), there should be no OOM
kills, the retention passes take their normal minutes, the disk is not
growing.

## 5. Operations journal (production server)

A chronology of unplanned work in production: what was changed by hand,
why, and how to roll it back. Normal CD deploys are not recorded here
(their journal — the Telegram and Actions notifications).

### 2026-10-22 — the wave 20 rollout (T-138…T-140) and stabilization

- The `be3a57f` deploy via CD (18:06 MSK), all healthz ok.
- The T-139 rollout step by step (load-measurements §3): the config
  `/etc/crawler/config.yaml` got the `http.task_*` keys (step 128 →
  later 256, settled at 192, see 23.10), the http-worker unit was synced
  from the repo, the `set-property` drop-ins were removed.
- **Redis, the temporary T-093 measures** (all removed on 23.10, see
  below): `maxmemory` 512→600→640→900 MiB at runtime and persistently,
  the cgroup `MemoryMax` 640 MiB→1 GiB; the `t092-trimhist` unit (XTRIM
  MINID of the oldest pending every 2 min, without losing work) —
  a crutch until the T-141 code fix.
- The inflow freeze during the measurements: `systemctl stop
  crawler-scheduler [crawler-dns-worker]` and the reverse start; the
  q:dns sink of 1735 legacy batches (~237 KiB each) kept Redis at the
  cap — the source of the 16:00–18:15 OOM window (the post-mortem —
  load-measurements §11.1).
- The step-256 measurement (12 min): 47.6 domains/s, anon 834–873 MiB —
  the §3 criterion not met; the working step 192 was kept (report §11).

### 2026-10-23 — wave 21 (T-141…T-143), incident analysis, budget restoration

- The `4c5410f` deploy: **the first run failed on the http-worker
  healthz** (the startup memory peak of 256 slots against the 900 MiB
  cap → GC thrash; the automatic rollback worked as designed). A retry
  after `task_concurrency: 256→192` via `gh workflow run deploy.yml` —
  success.
- Config: `scheduler.batch_size: 5000→1000` (the T-141 measurement:
  a batch of 1000 domains ≈ 64 KiB; "237 KiB" was the batch_size 5000
  era), `render.contexts: 3→4` (T-142; the Chromium budget ≈2 GB). The
  lesson about explicit values versus defaults — deploy.md §5 "Version
  updates".
- T-149: `XTRIM q:dns MAXLEN 125` — 1754 legacy batches trimmed off
  (617 MiB; safe: PostgreSQL is the source of truth, the 30-min lease
  would have returned them unchecked) → Redis 683→104 MiB. The budgets
  were restored: `maxmemory 512mb` (CONFIG SET + /etc/redis/redis.conf),
  the redis cgroup `MemoryMax=640M`. The `t092-trimhist` unit was
  stopped — replaced by the built-in TrimLoop (T-141, a loop in the
  scheduler, 1 min).
- T-150: 5507 q:dead entries triaged (all from q:render, the root — the
  Redis OOM window, not tarpits; the subjects `check_state=done`, no
  manual requeue needed), `XTRIM q:dead MAXLEN 0`; the PEL tail of ~940
  entries with RC=3 keeps adding deaths by the hundred — watch XLEN and
  repeat the XTRIM after things stabilize.
- T-151: the zstd dictionary trained and placed: the samples
  `testdata/laravel-samples` → `/opt/crawler/samples`,
  `crawlerctl dict train /var/lib/crawler/content/dict.zst --samples
  /opt/crawler/samples` (ID `f2ddc163`, 5233 B, −32 % on the samples).
  Writers/readers pick it up at the next restart — verification: the
  dictionary-load log line in the storage services after the deploy.
- Locally (dev, not production): the race for static IPs of the docker
  network was fixed twice (postgres→web, then dashboard→testweb took
  neighbors' addresses while the owners were stopped) — the start order:
  stop the usurper → start the IP owner → bring the usurper back.

### 2026-10-24 — Telegram delivery via a proxy (unlocking T-162/T-173)

- After the T-173 deploy the load reports were generated every 10
  minutes, but delivery was silent: `curl: (28) Failed to connect to
  api.telegram.org:443 — Timeout` (HTTP 000) on every send. The failed
  `ops-telegram-env` ops run (06:58, code 3 "enable line not found") —
  a false alarm: it started before the deploy of the `61b3484` fix (the
  script was not yet in `/opt/crawler/bin`), while the secrets had been
  written correctly; after the 07:17 deploy delivery switched on by
  itself and ran into the network.
- Diagnosis: from the production IP `api.telegram.org` is unreachable
  (TCP dropped), the rest of the internet works (github → 200, ya.ru →
  302); the CI notifications did arrive because the GitHub runner sends
  them — that masked the problem.
- The fix: on the operator's external VPS `104.253.1.37` (vpn, Ubuntu
  24.04) `tinyproxy 1.11.1` was installed — port 3128 (free: the
  Amnezia stack listens on 53/443/500/4500/47386/34116/41289/35826,
  mtproto-proxy — 8443), only `CONNECT 443`, `Allow` only 155.212.174.88
  and localhost, `enable --now`. Config `/etc/tinyproxy/tinyproxy.conf`,
  the memo — `/root/README.crawler-telegram-proxy.txt` on the VPS.
- In production: `https_proxy=http://104.253.1.37:3128` appended to
  `/etc/crawler/crawler.env` (survives deploys and ops runs — those only
  change the `TELEGRAM_*` keys), `systemctl restart
  crawler-monitor`. The check at 09:57 MSK: CONNECT in the proxy log,
  no send errors, the reports arrive in the group.
- Rollback: remove `https_proxy` from `crawler.env` + restart monitor;
  on the VPS — `systemctl disable --now tinyproxy && apt purge tinyproxy`.

### 2026-10-24 — the inflow freeze: stopping the scheduler to drain q:render

- Cause: the q:render backlog of ~119K entries (118 681 at the moment
  of the stop) — the inflow matched the drain, the lag at the 100K
  watermark did not shrink (the config.go pattern of T-142). The
  decision — freeze the inflow and let the queue dissipate (the
  2026-10-22 freeze precedent).
- Action at 10:26:37 MSK: `systemctl stop crawler-scheduler` — a clean
  graceful shutdown (the current batch finished, the DB pool closed,
  exit 0). The unit stayed enabled (a reboot would bring it back —
  deliberately); the pipeline (dns/http/render/analyzer/monitor) was
  not touched.
- The check after 2 h: q:dns (the sink of 122 batches at the watermark
  125) and q:http drained to zero (lag 0, pending 0), the wave that had
  made it through was reworked by the http stage; the live remainder of
  q:render ≈100K (lag 94 765 + pending 5 222) with zero inflow — a clean
  monotonic drain of ~29–34 entries/s (T-092), ETA ~1 h; q:analyze keeps
  up with render (lag 0).
- A nuance of the stopped scheduler (all the loops live in one service,
  cmd/scheduler/main.go): TrimLoop does not trim the acknowledged
  history — the streams' XLEN grows (q:http 41K, q:analyze 81K at the
  moment of the check), the MAXLEN caps of T-025 hold the budget;
  LeaseWatch stands still — the stuck queued rows will be returned by
  the first lease pass after the start. While paused, XLEN is not an
  indicator of depth — look at the group lags
  (`XINFO GROUPS` / the workers' `crawler_queue_depth`).
- Return: `systemctl start crawler-scheduler` after q:render → 0 —
  EnqueueLoop will pick up the accumulated pending (the T-026 gate will
  cap q:dns at 125 batches), TrimLoop will trim the accumulated history
  in one pass.
- Continuation (13:0x MSK): the deploy run `40083436999` (12:48 MSK,
  success) restarted the whole set — the healthcheck requires all 10
  units active and brought the stopped scheduler up (it does not
  distinguish a freeze). Stopped again at 13:07 MSK. Along the way the
  actual drain rate was measured:
  `crawler_stage_tasks_total{stage="render"}` ≈ **0.7 tasks/s**
  (not the 29–34/s of T-092 §4) — the head of q:render turned out to be
  a wave of nasty
  .рф domains (render tarpits 15s×3 attempts, ERR_CERT_*,
  ERR_CONNECTION_REFUSED — the entries are finalized without rendering,
  but burn tens of seconds on 4 contexts). With such a composition the
  ~100K remainder takes not hours but ~1.5–2 days of draining; an
  estimate by an average entries/s is inapplicable to a backlog of
  uniformly bad composition. Lesson: before a freeze, assess the
  composition of the queue head (the render WARN log), not just the
  depth.
- A production experiment without a deploy (edits of
  `/etc/crawler/config.yaml`, the `config.yaml.bak-20261024-{contexts8,toa2}`
  backups alongside; a deploy does not touch the server configs —
  ci-cd.md, but a drift from the repo appeared):
  14:22 `render.contexts: 8→4` (4 cores, load 16.5 — CPU starvation),
  ~14:29 `render.timeout_attempts: 2→1` (a tarpit is finalized on the
  first timeout, without the 10-min claim cycle of T-148); after each
  edit `systemctl restart crawler-render-worker`. Measurements by era
  (`crawler_stage_tasks_total` over 2–5 min): 8ctx+toa2 — ~63–66
  tasks/min, load 16.5; 4ctx+toa2 — 65/min, load 12.6 (the rate not
  lost, the CPU relieved); 4ctx+toa1 — 52/min, load 14.8 — the counter
  counts deliveries, the drop is the disappearance of retries, not
  a degradation (pending 5.1k→5.0k keeps draining). Decision: both
  edits kept.
- **The root found (the main conclusion of the day): draining q:render
  is impossible — the inflow does not come from the queues.** With
  q:dns/q:http empty, the q:render entries-added grew at ~100/min:
  http-worker runs the "render rework pass" every minute (renderq.go,
  T-066) — publishing to q:render the rows with the `render_pending` bit
  from the DB, bypassing the scheduler. A scheduler freeze does not stop
  this path. **394 833 domains** with the bit are waiting in the DB
  (xn--p1ai 23 912, ru 18 359, baby 11 842,
  wiki 9 394, org.cn 7 207, …) — at ~100 rows/min that is 4–6 days of
  continuous inflow. The lag of ~95–100K — the equilibrium of the inflow
  (~100/min) with the render capacity (~55–65/min) at the 100K
  watermark: the queue has not "hung", it is a pressure buffer from the
  DB (the same pattern as the config.go comment of T-142 "the lag had
  not shrunk since 22.10").
- A correction to the previous item: the 29–34 of T-092 §4 are tasks/**min**,
  not /s (a typo above); render was never fast. "~40 domains/s" —
  the rate of the dns/http stages (T-139: 47.6 domains/s); the render
  capacity is 29–66 tasks/min in all measurements. The backlog
  accumulated in the DB over weeks and surfaced now through the rework
  pass.
- What next: the scheduler freeze does not affect q:render — it can be
  lifted at any moment (the top of the pipeline is clean); the real
  capacity fix — a second Chromium + a second render worker on separate
  hardware (deploy.md §2.2), on 4 cores the ceiling has been reached.
  A candidate for code tasks: cert hosts get the `render_pending` bit
  again with every daily recheck episode (T-159 deliberately "not
  forever") — for hundreds of thousands of nasty hosts this is an
  eternal generator of render work; a cooldown for hopeless classes is
  asking to be added.

### 2026-10-24 — the T-177 deploy (slow PG queries): config, grant, verification

- The `103171b`–`e85bc61` deploy went normally (ci 11m47s → deploy
  5m51s, the healthz green, the Telegram notification delivered — the
  proxy works).
- PostgreSQL: `/etc/postgresql/16/main/crawler-postgresql.conf` replaced
  with the repo version (the md5 checked against the old repo copy
  before the replacement — no server-side edits existed; the
  `.bak-t177` backup alongside), `systemctl
  reload` — `SHOW log_min_duration_statement` = 3s without a restart;
  `GRANT pg_read_all_stats TO crawler` — the texts of other sessions in
  pg_stat_activity are readable.
- Verification (12:50–12:55 MSK): the monitor loop came up (the "slow
  query cycle" interval=10s threshold=3s), the gauge
  `crawler_pg_slow_queries` and the `{app}` counter in /metrics; 11
  events in 5 minutes, 0 Telegram delivery errors. Real finds
  immediately: the LATERAL scan of RequeueRenderPending (~10 s,
  scheduler), the dashboard/crawlerctl status aggregates (5–12 s),
  the http-worker batches — the stage-25 render debt visible in
  Telegram without manual SQL (the T-175 goal partially met by the
  signal).
- An observation for the operator: at the current CPU starvation
  (PSI ~80–90 %) the normal aggregates regularly exceed 3 s — a message
  rate of ~2/min. If it drowns you — raise
  `monitor.slow_query_min_duration` (for example, to 10–15 s; the
  allowed range up to 1h) or switch the loop off with an interval of 0;
  the full trace stays in the server log regardless.
- Rollback: restore `.bak-t177` + reload; `REVOKE pg_read_all_stats FROM
  crawler`; the binaries — a deploy of the previous version (`bin.prev`).

### 2026-10-25 — the domains lock convoy (T-192): the analysis, the config in the repo, a reload needed

- The incident 10:22–12:44 UTC: a transaction holding row locks on
  `domains` for ~37 min (wave 2 — five victims on one row
  `(560, 25398110)`, the whole queue released in one second), wave 1 —
  a holder of scattered rows for ≈16 min; the point write statements
  stood for up to 39 min (entries №2–№4/№6/№11 of the slow-query log),
  the pipeline stood still for ≈55 min (q:dns at the watermark). There
  were no operator sessions; the holder's name is not recoverable
  retroactively — `log_lock_waits = off`. The full analysis — the T-192
  spec (`tasks/TZ_slow_lock_convoy.md`), how to read the log — §4.3.
- Config: `deploy/postgres/postgresql.conf` and the dev config in the
  repo got `log_lock_waits = on` (a reload parameter). **Left for the
  operator:** replace `/etc/postgresql/16/main/crawler-postgresql.conf`
  with the repo version (modeled on the 24.10 entry: a backup copy
  alongside, an md5 check), then `systemctl reload postgresql` —
  a restart is not needed; verify with
  `sudo -u postgres psql -c 'SHOW log_lock_waits'` → on. At the next
  convoy the log will say `process N still waiting for ShareLock on
  transaction X …` — the holder by name (pid, transaction).
- Code (the next deploy): the write pools of
  dns-worker/http-worker/scheduler get the AfterConnect hook
  `db.LockTimeout(30s)` — the convoy is cut off by SQLSTATE 55P03 with
  a transaction rollback and a delivery retry instead of a half-hour
  wait; verification — the T-177 slow-query log (waits outside the
  holder do not exceed 30 s).

### 2026-10-25 — the operational part of T-192 (log_lock_waits) and the T-191…T-190 wave deploy

- The operational part of T-192 was applied before the deploy (modeled
  on the 24.10 entry): the server md5 of
  `/etc/postgresql/16/main/crawler-postgresql.conf` checked against the
  pre-T-192 repo version (a match — no server-side edits existed), the
  `.bak-t192` backup alongside; after the replacement md5 = repo HEAD,
  `systemctl reload postgresql` (without a restart):
  `SHOW log_lock_waits` = **on**, `log_min_duration_statement` = 3s
  (T-177) untouched.
- The `e9e3c4d..3bffc62` wave deploy (T-191 retry-SEQ+guard, T-192
  lock_timeout hooks, T-193 the reanalysis tick on a time cursor, T-190
  the -race tests + a CI step) went normally: ci 13m14s (including the
  new "Tests with the race detector" step) → deploy 1m53s,
  `crawler_build_info` revision=3bffc62, 11 units active (including
  render-worker@9109), the healthz 9100–9106 green, NRestarts=0 on all.
- The check after (the first minutes): the T-191 retry loop came up
  normally, no errors on the `status_temp_error` guard line; the first
  T-193 reanalysis tick — selected=3 published=3 (a fresh slice by the
  cursor instead of a full scan); error/fatal in the dns/http workers —
  0.
- Observation (a day): the `retrySelectSQL` entries disappear from the
  slow-query log (T-191); on a convoy recurrence the log says "process N
  still waiting for ShareLock on transaction X …" (the holder by
  name), the waits of point statements outside the holder ≤ 30 s
  (the 55P03 cutoff — a transaction rollback, a delivery retry);
  the `reanalyzeSelectSQL` tick leaves the log, the drain on rule
  changes remains (deliberately full, T-193).
- Rollback: the config — restore `.bak-t192` + reload; the binaries —
  a deploy of the previous version (`bin.prev`) through the normal
  pipeline.

### 2026-10-26 — the analysis of the slow-query window after 3bffc62; reconciliation at 30 min, the NRD cron at 05:33 UTC (T-199/T-205, the operational part)

- The analysis of the 18:16 UTC 25.10 → 03:05 UTC 26.10 window (the log
  `tmp/pg_slow_after_3bffc62.log`, outside git): 1053 statements /
  25 kinds / ≈4.9 h of DB time. The lock convoy did not recur (459
  waits, all ≤1.1 s, all acquired — T-192 works); the T-191/T-193 fixes
  confirmed (retrySelect 150 → 10 events, the reanalysis tick 18–25 →
  5–12 s, renderRequeue 137 → 1). As a result, stage 31 was put on the
  plan (T-199…T-205, tasks/tasks.md); the lease and retention analysis
  specs — `tasks/TZ_slow_lease_return.md`,
  `tasks/TZ_slow_retention_residual.md`.
- T-199 (operationally, before the defaults deploy):
  `/etc/crawler/config.yaml` `monitor.zone_stats_interval: 5m → 30m`
  (the `config.yaml.bak-20261026-zonestats30m` backup alongside),
  `systemctl restart crawler-monitor` — the log: "pipeline counter
  reconciliation cycle interval=30m0s", healthz ok. The operator
  decision per the incident spec §7.1 (space out the reconciliation
  after the wave's break-in). Consequence: the monitor alert cycle is
  now 30 min (the freshness of the alerts/the zone dashboard; the
  progress-stall threshold AlertStallFactor × interval = 2.5 h).
- T-205 (operationally): `/etc/cron.d/crawler-nrd` 23:33 → 08:33 MSK
  (05:33 UTC — outside the evening CPU starvation and the midnight
  recheck storm; the source's day is closed with >9 h to spare).
  A nuance: the backup copy in `/etc/cron.d` was picked up by
  systemd-cron as a second cron job (`cron-crawler-nrdbak-…-0.timer` at
  23:33) — moved to `/root/crawler-nrd.bak-20261026`; lesson: keep
  `/etc/cron.d` backups outside the directory. Verification: one timer,
  the next run 08:33 MSK on 26.10 (the day 2026-10-25 — the continuity
  of the daily feed preserved).
- Rollback: the config — restore `5m` + restart monitor; the cron —
  restore the `33 23 * * *` line and the comment (a copy in /root).

### 2026-11-02 — T-215: the chromium recycle unit pair brought into crawler.target (PartOf/WantedBy)

- On the production server, `PartOf=crawler.target` was added to
  `crawler-chromium-recycle.{service,timer}`, the timer moved from
  `WantedBy=timers.target` to `WantedBy=crawler.target` (`reenable`:
  the symlink moved from `timers.target.wants/` to
  `crawler.target.wants/`), `daemon-reload`; the units were synced into
  the `deploy/systemd/` kit (deploy.md §2.2).
  Reason: after `systemctl stop crawler.target` the next timer tick
  (every 2 h) started the stopped chromium. The backup of the previous
  units — `/root/t215-backup-20261102/`.
- No live stop test of the set was performed: the q:render drain is in
  progress (150K entries), the scheduler deliberately stopped since
  30.10 — `start crawler.target` would bring it up on top of the drain.
  The bindings were verified statically
  (`systemctl show -p PartOf,WantedBy`,
  `list-dependencies --reverse`: both units under crawler.target) and
  the PartOf→timer semantics confirmed with trial units in
  `/run/systemd/system` (stopping the target stops the timer, start
  brings it back; systemd 255). Run a short live test (stop → the timer
  and chromium inactive → start → everything back) after the q:render
  drain finishes and the scheduler is back in service.
- Rollback: `cp /root/t215-backup-20261102/{crawler-chromium-recycle.service,crawler-chromium-recycle.timer} /etc/systemd/system/`,
  `daemon-reload`, `reenable --now` the timer (it will return to
  timers.target.wants/;
  `--now` is mandatory: without it `start crawler.target` will not
  bring up the stopped timer — after the rollback WantedBy is
  timers.target again, and that target is active from boot), the check —
  `systemctl is-active crawler-chromium-recycle.timer`.

### 2026-11-10…11 — the T-271 regression (the retention loop), the T-273 hotfix, and the drain quiet window

- **The incident**: since the T-271 deploy (09.11 14:13) both heavy
  http-worker selects — retention and the late republisher — were in an
  eternal loop of doomed catch-up: the client timeout (600/60 s) killed
  the first pass from the zero cursor BEFORE the commit, the cursor did
  not move, every retry ran the same scan through the clean prefix.
  Retention was dead for 21 h (70+ failed passes), 21 cgroup OOM kills
  of PG during the night of 09–10.11.
  The investigation (the 2026-11-09/10 prod reports, the T-271/T-273
  tasks): the root — a timeout smaller than the cost of the first pass +
  the T-271 plan pinning had flipped the render-late join to start from
  domains.
- **The T-273 hotfix** (SelectTimeout 600→3600 s; render-late without
  pins + 300 s; a ticker reset on every error; the `*_errors_total`
  metrics; catch-up/backoff tests): deployed 10.11 14:58,
  the healthcheck green.
- **The pitfall**: the deploy brought up the scheduler stopped at 08:00
  (install starts crawler.target) — the front and the first catch-up
  scan coincided; under the full load an hour was not enough — passes
  1–3 hit the 3600 s cap (`retention_errors_total` = 3), the cursor
  stayed zero.
- **The action at 16:29:23 MSK**: `systemctl stop crawler-scheduler` —
  the drain window (the front's tail worked off until ~19:29). Pass 4
  (18:43): the first selection made it just under the wire (~60 min on
  a quiet system), then the drain went in batches of 10K all night
  without a single error.
- **The outcome by 08:04 11.11**: disk 74→53 % (36 GB free), content
  21→5.3 GB (packs 40→12, the oldest grew younger by a day), contents
  live 1.25M→262K (−~1M), compaction freeing 1.5–2 GB/h
  (~13 GB over the night), 0 PG kills, 0 WARNs for 12+ h. The price:
  the front stood for ~15.5 h (~0.5M checks ≈ 1.2 % of the remaining
  cycle).
- **Lessons**: (a) the "30–60 min" estimate of the first pass is the
  lower bound; under load/with an unfinished tail — two to three times
  more: the 3600 s cap is just under the wire (a follow-up — 7200 s as
  a config key, or a partial index + a persistent cursor); (b) the
  retention counters aggregate at the end of a pass — zeros during
  a live drain are normal, watch progress via contents/packs/
  compaction; (c) any deploy brings the scheduler up — a window that
  must survive a deploy has to be re-established manually; (d) with
  a live (non-zero) cursor a batch timeout does not reset the drain —
  the front can be brought back before the pass fully completes.
- The operation's runbook — §4.6; the snapshots —
  `prod-reports/2026-11-09-204415.md`
  … `prod-reports/2026-11-10-210400.md`.
- **The risk closed in code by T-274** (the deploy — per the task
  plan): the `content.retention_select_timeout` cap config key (default
  7200 s) + the persistent drain cursor (the `retention_drain_cursor`
  table, migration 000020) — an http-worker restart continues the drain
  from the saved position, the hourly penalty of traversing the clean
  prefix is gone; forced catch-up circles (render debt removal and
  others) clean the state by themselves.

### 2026-11-11 — an external power-cycle of the VM at 11:03 (no operator, no deploy)

- The diagnostics from the journals: at 11:03:17 the guest started a
  clean stop of all targets (multi-user, crawler, timers) → 11:03:23
  `poweroff.target`/`systemd-poweroff` → boot at 11:03:44 (`uptime -s`).
  No initiator inside the guest: no systemctl commands, no sudo, no SSH
  sessions; the unattended-upgrades `Automatic-Reboot` is commented out
  (its "Unattended Upgrades Shutdown" unit — a consequence of the stop,
  not the cause);
  there was no deploy (the binaries from 10.11 14:57, the deploy
  workflow did not run).
  Conclusion: a hypervisor-level restart — the hosting panel/API or the
  provider's infrastructure (fvds.ru).
- The consequences for the pipeline: all units started at boot —
  including the scheduler stopped on 10.11 at 16:29 (the drain freeze
  ended naturally 19 min BEFORE the reboot — lucky timing); http-worker
  started the retention catch-up from the zero cursor under a warming-up
  front (the "the 3600 s cap is just under the wire" risk — T-274);
  PG/Redis were restarted, the process metrics and pg_stat_statements
  were reset.
- **The risk closed by T-274** (code; the deploy per the plan): the
  persistent drain cursor survives an external power-cycle too —
  a restart without saved state no longer means a new hourly catch-up.
- Lessons: (a) a server reboot is equivalent to a deploy in effect —
  "brought everything up and cut any window short" — account for it in
  runbook §4.6; (b) when investigating set restarts, check
  `uptime -s`/`who -b` and the tail of `journalctl
  -b -1` (poweroff vs reboot vs crash) before forming hypotheses; (c)
  external power-cycles are outside our control — one more argument for
  the persistent drain cursor (implemented by T-274).

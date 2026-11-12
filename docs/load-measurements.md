# T-092: load measurements on the production server

Date: 2026-10-22. Server: 155.212.174.88 (VDS 18561438, 4 vCPU, 7.7 GiB RAM,
NVMe virtio 80 GB, Ubuntu 24.04.5, kernel 6.8.0-142; deploy T-135 — systemd,
10 units + PostgreSQL 16 / Redis 7 / Unbound 1.19). The measurements follow
spec §11.3 on real domain lists: `ru_domains.gz` 6,229,925 rows, `rf_domains.gz`
816,490, `su_domains.gz` 111,560 (7,157,975 total; submitted to
`incoming/{ru,xn--p1ai,su}/add/` as `.gz` archives — the ingest format is T-133,
the zone from the directory, domain extraction from registry lines
`domain\tregistrar\tdates\tflag` — T-132).

Tooling: `deploy/loadtest/t092-sample.sh` — a continuous sampler
(15 s): stage counters, queue depths, backlog, unit `MemoryCurrent`,
DB/storage size, PSI; `t092-analyze.py` — rates from deltas. Ingest logs —
`ingest_files`/journalctl, histograms — `/metrics` (snapshots).

## 1. Ingest

| File | Rows | Processed in | Rate |
|---|---|---|---|
| su_domains.gz | 111,560 | ≈5 s (after stable_seconds 10) | ≈22k rows/s |
| rf_domains.gz | 816,490 | ≈27 s | ≈30k rows/s |
| ru_domains.gz | 6,229,925 | ≈184 s | ≈34k rows/s |

Streaming .gz parsing + normalization + COPY in batches of 50,000: the ingest
memory is stable (10.5 MiB at the peak). Deviations: su 52, rf 9, ru 34 rows
(0.0007–0.05%) — almost all `no_domain` on the 52 geographic `.su` suffixes
from the Public Suffix List (`nalchik.su`, `msk.su`, `georgia.su`…):
for the PSL such names are a zone without a label, and ingest correctly
rejects them. The measurement predates the switch to the TLD zone
semantics (the tld-only-zones spec): today these rows are accepted as
`su` zone domains with a compound label
(`nalchik.su` → label `nalchik`) — the share of `.su` dump rejections drops
to background noise.
Config implications: none — the ingest throughput is
orders of magnitude above the file arrival rate.

## 2. DNS stage

A capability measurement — the initial SU/.рф wave (while q:http had not yet hit the
watermark): 35,000 domains in the first ~165 s after the su ingest (**≈210 domains/s**
on average across the wave), the mean domain task duration 9.5 s (histogram:
p50 ≈ 0.8 s, p90 ≈ 5 s, a tail >10 s — retries/TCP fallback),
effective parallelism 2000 → the actual cap ≈ 210 domains/s matches
the observed one. Unbound: ~3.6 queries per domain (apex + subdomains, AAAA
served locally), ≈760 QPS during the wave, 88% cache miss (unique
names — expected), 1 thread, no per-IP rate limits; cache hits grow on
repeated subdomain zones. `dns.max_qps=10000` was not reached — the narrow
spot is not the limiter but the recursion latency (mean 9.5 s per task).

Fallback resolvers: 685 switches per wave (transport failures of unbound
under load), 24 exhaustions — the fallback works.

## 3. HTTP stage — the pipeline bottleneck

Steady state (all three zones ingested, q:http at the threshold of
`scheduler.queue_high_watermark=100000`): **≈2 domains/s** (histograms:
mean 1.67 s, p50 ≈ 0.3 s, a 20–30 s tail — tarpits/parked domains). In requests:
8,959 HTTP exchanges over 3,089 tasks ≈ **2.9 requests/domain** (the live subdomains
of SU domains average apex+www), i.e. ≈6 requests/s at the stage — against the
configured cap `http.max_rps=1000` (not reached: the bottleneck is not
the transport). The share of entries sent to render (the SPA heuristic): 5.4%.

The cause is not the network and not the CPU (load 1–3 of 4, PSI CPU <10%): the loop
`httpworker.Run` reads q:http in windows of **`Consume=16`** deliveries and
must wait for the completion of the **entire window** (a batched barrier) before
the next read; the task parallelism is hardcoded in `Options.withDefaults`
(`Concurrency=64`, `Consume=16`, `PerDomain=8`) and **is not overridden by
the config** — `http.concurrency=2000` affects only the transport
(RPS/connections). With a duration tail of 20–25 s (`total_timeout=20s`) the stage cap is ≈ 16/25 s ≈ 0.6–2 domains/s — which is what is observed. Complementary
serialization: `per_ip_limit=4` against the parking IPs (at the head of q:http
the top IPs hold up to 8% of tasks — 77.223.111.x, 95.163.244.138 etc.) under the FIFO consumption
order (head-of-line blocking).

The backpressure coupling: q:http at the watermark → the dns-worker cannot publish
tasks of live domains → the DNS stage is held to the HTTP pace → the entire pipeline
moves at ~2–3 domains/s. For the current run (7.16 million domains,
~79% of SU domains resolve → ~5.4 million HTTP tasks) that is ≈31 days of
the HTTP stage alone.

**Conclusion (§13.2 “refined by load measurements”):** the default values of the task parallelism of the HTTP stage need to be made
configurable and revisited — filed as task T-139 (sliding
consumption instead of the barrier + passing `Concurrency/Consume/PerDomain`
through to the config; the goal ≥100 domains/s within the memory budget — bodies in flight are bounded by
`max_body_bytes=2 MiB`, tarpits do not hold bodies).

> **T-139 is implemented** (after the measurements): the `httpworker.Run` loop has switched to
> sliding consumption (a read on slot release, without a window
> barrier), the task parallelism is configured by `http.task_concurrency`
> (default 256 → an estimated ≈150 domains/s), `http.task_consume` (16),
> `http.task_per_domain` (8) — docs/configuration.md, §http. The rollout
> order (the §6 memory was captured at the old parallelism, the unit
> MemoryMax of 384 MiB was not raised): ship with a conservative `task_concurrency`
> (64–128) → run the T-092 kit → raise to 256 based on the observed memory.
> The acceptance criterion (at step 256; conservative lower steps arithmetically
> do not give ≥100 domains/s at a mean task of 1.67 s: 64 → ≈38, 128 → ≈77):
> http-worker steady state avg < 300 MiB and max/p95 < 350 MiB (the peaks do not push against
> MemoryMax 384 MiB), the corroborating signal —
> `memory.events oom=0` and no unit restarts, the rate
> ≥100 domains/s (a day of observation, as T-093). A repeat measurement with the same
> kit is an operational step after the deploy.
>
> **The rollout is done (2026-10-22, details — §11):** step 128 — anon
> ≈ 380 MiB, right up against MemoryMax 384 MiB (softly, oom=0); step 256 —
> **47.6 domains/s**, anon 834–873 MiB (avg ≈ 855, a 12 min window). The rate criterion
> ≥100 domains/s and the memory criterion avg < 300 / p95 < 350 (the above —
> historical, written before the per-slot measurement) are not met: the fact is ≈ 3.34 MiB
> anon per slot at step 256 (at 128 — ≈ 3.0; the bodies of the original entries,
> each ≤ `max_body_bytes` 2 MiB, + buffers — a typical mixture, not an upper
> bound), while the rate is dictated by the mixture of durations (mean ≈ 5.3 s),
> not by the number of slots. The rollout outcome: the unit MemoryMax raised 384 →
> 900 MiB and pinned in the unit (T-143; on the server — a persistent drop-in
> `systemctl set-property`; after a unit file update (deploy.md §4.6)
> remove it: `systemctl revert crawler-http-worker` + daemon-reload +
> `restart` — otherwise the drop-in will keep overriding the unit; the check:
> `systemctl show crawler-http-worker -p MemoryMax` = 943718400);
> the effective memory criterion — oom=0, NRestarts=0, anon ≤ 900 MiB
> at 256 slots (a day of observation — T-093); the Go services total
> ≈ 1.0–1.15 GiB < the ≈ 1.3 GB of spec 3.2. The next rate reserves are —
> T-142 (render) and the `total_timeout`/per-IP tail; the direction of reducing
> the slot memory — streaming the bodies into the content-store as they are downloaded instead of
> holding them until the end of the task (requires revisiting the alias deduplication
> scheme; not implemented).

## 4. Render

Chromium headless-shell 156: the mean render duration **5.1 s/page**
(p50 ≈ 5–8 s by the histogram), `contexts=3` → a cap ≈ 0.6 pages/s;
after the font fix the actual rate reached ≈0.28 pages/s at
a demand of 5.4% of entries — not a bottleneck, but the window barrier is the same
(`concurrency=4, consume=16`; T-139 did not change the render loop — the cap of
the stage is dictated by Chromium, not by the read order; the revisit is a separate
follow-up as the render demand grows).

**Incident during the measurements**: Chromium crashed in a crash loop
(`FATAL SkFontMgr_FontConfigInterface: Not implemented`) — the bare host
has no fontconfig/fonts (only the binary was taken from the docker image at T-135);
some pages require a font stack → SIGTRAP → 10 restarts →
start-limit, q:render accumulated attempts. Fixed operationally
(`apt install fontconfig fonts-liberation fonts-dejavu-core` +
`systemctl reset-failed && start`); the package list of deploy.md §4.3
to be corrected by task T-140. No lost tasks (at-least-once returned
the deliveries), one relic in q:dead (yandex.ru/admin, the T-135 era) — to triage
per runbooks §2.

## 5. Analyzer

5,214 entries, a mean of **12.9 ms/entry** (p50 ≈ 3 ms): not a bottleneck, it finds Laravel along the run
(37 domains / 203 matches in the first hours on SU/.рф).

## 6. Memory by service (the spec 3.2 budget)

A steady-state window of 25.5 min (99 measurements, the unit MemoryMax — the cgroup cap):

| Unit | min / avg / max | MemoryMax |
|---|---|---|
| ingest | 9 / 10 / 11 MiB | 128 MiB |
| scheduler | 8 / 9 / 10 MiB | 128 MiB |
| dns-worker | 76 / 91 / 141 MiB | 256 MiB |
| http-worker | 112 / 185 / 225 MiB | 384 MiB |
| render-worker | 23 / 116 / 128 MiB | 128 MiB |
| analyzer | 11 / 13 / 18 MiB | 128 MiB |
| monitor | 5 MiB | 128 MiB |
| web / dashboard | 6 / 7 MiB | 256 / 128 MiB |
| chromium | 111 / 237 / 1024 MiB | 2 GiB (restart_memory_bytes 1.5 GiB not reached) |
| PostgreSQL | ≈3.0 GiB (at the cap) | 3 GiB |
| Redis | 179 / 349 / 465 MiB | 640 MiB (maxmemory 512 MiB) |
| Unbound | 452 / 452 / 459 MiB | 700 MiB |

No OOM was recorded either by the kernel (`journalctl -k` is empty) or by the cgroups
(`memory.events oom=0` in all units); unit NRestarts 0 over the observation
window (the Chromium crash loop before the font fix — §4, the counter was reset by
`reset-failed` together with the start-limit). The Go services total ≈440 MiB
on average / ≈550 MiB peak — the ≈1.3 GB budget holds with headroom.
Observations for T-093 (the run is closed — §13): PostgreSQL holds
near the MemoryMax cap
(file cache pages in the cgroup accounting — no throttling of the backends is visible);
render-worker is approaching 128 MiB (the dips are the Chromium restart cycles by
the manager); Redis grows toward maxmemory — a consequence of the stream depth
at the watermark (entries without payload, ≈100K × q:http + q:dns).

## 7. DB and storage write rate

A steady-state window of 25.5 min: the DB growth **426 MiB ≈ 285 KiB/s** (≈150 KiB
per checked domain: `subdomain_results` rows + `domains` status updates
), the content store growth **54 MiB ≈ 36 KiB/s** (≈29 KiB
of stored content per domain: 2.9 requests/domain × ≈10 KiB of zstd-compressed
body). The RU ingest wave produced a write peak of up to ≈2.4 GB over 49.5 min
of observation (the domains table + indexes).

The sizes after the ingest of the three zones and the first ~20 thousand checked: `domains`
7.16 million rows ≈ 1.81 GiB (≈265 B/row with indexes); `contents`
3,252+ blocks in one pack of 52 MiB (≈16 KiB/block); `subdomain_results`
≈7 thousand rows; the DB as a whole 2.13 GiB. Disk: 72 GB free of 80 —
a full 350-million cycle is not budgeted for 80 GB (disks.md §3: 8 TB+), for
the current run the margin is multiple. Pack compaction (threshold 0.5,
once an hour) runs in the background, no losses were observed.

## 8. Estimate of the first cycle duration (spec §3.3: 350 million)

By the measured stage caps (the SU zone structure as the share reference:
79% resolve → an HTTP task; alive ≈ 3%, temp_error ≈ 6%):

| Stage | Cap | 350 million (at the measured shares) |
|---|---|---|
| Ingest | ≈34k rows/s | ≈3 hours (as 6-million files) |
| DNS | ≈210 domains/s (without HTTP throttling) | ≈19 days |
| HTTP | **≈1.9 domains/s (window barrier, default)** | **≈4.6 years** (277 million tasks) |
| Render | ≈0.5–0.6 pages/s | only the SPA share (≈5% of entries) |
| Analyze | ≈80 entries/s | not a constraint |

Bottom line: with the shipped defaults the first cycle is stuck on the HTTP stage for
years against the spec reference “1–2 weeks” (≈200–450 domains/s are needed — two
orders of magnitude more). The defaults require reworking the q:http
consumption (T-139); after it the measurements are reproducible with the same kit in one
evening. The current run of 7.16 million domains continues uninterrupted
(≈31 days of the HTTP stage at the current defaults; it will speed up automatically
after the T-139 deploy — the queue and the backlog persist) and serves as the daily
run of T-093 (the criterion is closed over this window — §13): the sampler
keeps writing metrics, the criterion is a day
without OOM (at the time of the measurement — zero events over 50 min of steady state at
all queues at the thresholds).

## 9. Findings for tasks

- **T-138** (bug): an empty response body loses the link to the content —
  `httpfetch.ToUTF8` on an empty body returns `UTF8=nil`, the http-worker
  sets a valid hash of the empty body, but `Body=nil` → the ContentSaver guard
  suppresses `content_hash` (Warn “entry body unavailable”,
  ~4% of tasks; parked domains and 400/404/500 with `Content-Length: 0`). The contract
  “an empty-body hash is valid” is broken (dedup/classes of empty pages).
- **T-139** (perf): the task parallelism of the HTTP stage is hardcoded and not
  configurable; the window barrier of 16 deliveries; pass-through to the config + sliding
  consumption; the goal ≥100 domains/s. *(Implemented: sliding consumption +
  `http.task_*` with the defaults 256/16/8; a repeat measurement — after the deploy.)*
- **T-140** (deploy): fontconfig/fonts for headless-shell into the package list of
  deploy.md §4.3 (otherwise a crash loop on some pages).
- A q:dead relic (yandex.ru/admin, a render of the T-135 era) — triage per
  runbooks §2 manually.

## 10. Reproduction and artifacts

The kit in the repository: `deploy/loadtest/t092-sample.sh` (the sampler),
`deploy/loadtest/t092-analyze.py` (the delta analysis). On the server the copies are in
`/var/lib/crawler/t092/` (the sampler runs under the `t092-sampler` unit,
the output is `metrics.tsv`; to stop/analyze:

```bash
systemctl stop t092-sampler           # if needed
python3 /var/lib/crawler/t092/t092-analyze.py /var/lib/crawler/t092/metrics.tsv
```

The measurements were taken by version 6884d85 (deploy 19de375 — the same code, a status
commit); the stack restart at 15:18:57 (a deploy by push — the routine CI/CD
path, graceful, without task losses) is accounted for in the analysis (a counter reset).

## 11. Repeat measurement after T-139 (2026-10-22)

Deploy `be3a57f` (T-138/T-139/T-140) via CD at 18:06 MSK; the rollout per §3:
step 128 (18:11–20:36, MemoryMax 384 MiB) → step 256 (20:40+, MemoryMax
temporarily 900 MiB via `systemctl set-property`). The counters —
`crawler_stage_tasks_total`, memory — the unit cgroup (anon), the window of step 256 —
12 min (4 snapshots).

| Configuration | Rate | http-worker memory | oom |
|---|---|---|---|
| baseline (barrier, Consume=16) | ≈1.9 domains/s | 63–160 MiB | 0 |
| sliding, 128 slots | 18–40 domains/s (a mixture) | anon ≈380 MiB ≈ MemoryMax 384 (softly) | 0 |
| sliding, 256 slots | **47.6 domains/s** (windows 50.6/51.5/40.7) | 834–873 MiB (avg ≈855) at 900 | 0 |

**Verdict on the §3 criterion:** the ≥100 domains/s rate at step 256 **not reached**
(47.6 ±5) — on the measured mixture the mean task is ≈5.3 s (retries of the jam
period, tarpits of 20–25 s, per_ip_limit=4 on the parking IPs), the cap
is dictated not by the number of slots but by the time-in-flight; the memory avg<300/p95<350 is also
unreachable at ~3.3 MiB anon per slot. Against the baseline — **×25** (1.9 → 47.6);
the 350-million first cycle ≈ 85 days against 4.6 years on the old defaults.
The further reserves: T-142 (render), the total_timeout/per-IP tail, the reduction of
the slot memory (T-143).

**Measurement findings** (filed as stage 21 tasks):

- **Redis stream memory** (T-141): XACK does not delete the entries — the history up to
  MAXLEN (1M individual entries ≈ 0.3–0.7 KiB; q:dns 10K batches × ~237 KiB) overflows
  the 512 MiB budget. The daily run was stuck on “OOM command not allowed” even
  **before** the deploy (the render/analyze publications failed, q:render jammed at
  792, the q:dns sink 1,735 batches = 410 MiB). Temporary measures on the server:
  the `t092-trimhist` unit (XTRIM MINID of the oldest pending every 2 min —
  without losing work) + runtime Redis maxmemory 900 MiB / cgroup 1 GiB
  (persistent in /etc/redis/redis.conf with a note to revert after T-141).
- **Render** (T-142): an inflow of 5.4% × ≥40 domains/s against a cap of 0.6 pages/s —
  q:render 7.7K→39.8K in ~2 h; the 100K watermark gate will stop HTTP until the render pace of ≈11 domains/s; spec §5.6 requires render_pending without blocking.
- **http slot memory** (T-143): ~3.3 MiB/slot (bodies in flight ≤2 MiB);
  MemoryMax 900 MiB pinned in the unit, the Go services total ≈1.05 GiB <
  the ≈1.3 GiB of spec 3.2.

Measurement conditions: the scheduler was stopped 18:14–21:26 (a freeze of q:dns against
the Redis overflow — with the trim loop the stop is not needed, the scheduler
was started back at ≈21:26); the dns-worker fed q:http; the sampler (t092-sampler)
wrote continuously (metrics.tsv covers both rollout steps).

### 11.1. Post-mortem of q:dead 5,507 (2026-10-23)

The morning attribution “render tarpits burn the attempts” turned out to be secondary:
**the root cause is the Redis OOM window of 22.10 ≈16:00–18:15 MSK** (legacy batches
of q:dns of 5000 domains ≈ 620 MiB at the temporary maxmemory 900 MiB). The error
of the result publication (`XADD: OOM command not allowed`) was handled
as an error of the task itself: the delivery stayed without XACK, RecoverLoop
kept re-claiming it in a circle (16/30 s — T-153), the attempts burned — 5,507 deaths
at fully working processing; the attempts 9/11 spikes — OOM also fell on
the publications of q:dead itself. All the deaths are from the narrow source window,
stretched over hours by the slow claiming.

Triage and closure (2026-10-23, T-149/T-150): the subjects were returned by the
pipeline without a manual requeue — a spot check of the domains from q:dead:
`check_state=2 (done)`, the results were written on 22.10 (the HTTP stage had passed before
the deaths); the render that did not make it is finished by the background render_pending pass,
the analysis — by the hourly ReanalyzeLoop. `XTRIM q:dead MAXLEN 0` after the check;
the PEL tail (~940 entries with RC=3, some on dead consumers) keeps adding
deaths by the hundreds — a repeated XTRIM after stabilization. The drain of legacy-q:dns:
`XTRIM q:dns MAXLEN 125` (the source of truth is PostgreSQL, the 30 min lease
would have returned the unverified) freed **575 MiB**: Redis 683→104 MiB,
maxmemory and the cgroup returned to 512 MiB/640 MiB (the T-093 temporary measures
removed in full). Code fixes of the consequences — T-152 (OOM does not burn
attempts), T-153 (the RecoverLoop pace), T-154 (the observability of deaths),
T-156 (the signature and the recipe — runbooks §2.6, a note on the example noeviction incident —
docs/redis.md §4).

## 12. Control measurement 2026-10-23 (stabilization)

The goal — a reconciliation of the state after the waves of 20–23; in the course of the measurement the operator
postponed the “50 domains/s” goal and confirmed the unavailability of horizontal
scaling (server/disk). The tooling is the same: t092-sampler kept
writing metrics.tsv without a stop since 2026-10-22; the windows 1 h / 6 h
(t092-analyze.py) + live snapshots of /metrics (9103–9106), Redis
(XINFO/XLEN), pg_stat_activity, cgroup memory.events, top/PSI.

Conditions vs §11: the DB has ≈47 million domains across dozens of zones — com 13.87
million (5.5 thousand checked), ru 6.27 million (370 thousand), xyz 4.32, top 1.53,
shop 0.93, org/online/net/cn/site/info…; backlog_pending 40.3 million
(the filling went outside the ingest files — ingest_files has only ru/rf/su).
The task mixture is heavier than ru/rf/su: parked domains and dead hosts of mass zones.

| Metric | §11 (22.10) | Measurement 23.10 (~11:00 MSK) |
|---|---|---|
| HTTP, domains/s (256 slots) | 47.6 | **21–23** (windows 1 h / 6 h) |
| mean HTTP task | 5.3 s | **11.5 s** (p50 ≈ 13.6, p90 ≈ 27, p99 ≈ 30) |
| HTTP transport | — | 0.54 s/request, 3.1 requests/task |
| DNS, domains/s | — | 26–28 (throttled by the gate q:http=75,000) |
| Render, pages/s | ≈0.28 | 0.20–0.23; q:render 103,868 at the watermark of 100K |
| load / PSI CPU some | 1–3 / <10% | **25/4 vCPU / 91–95%** |
| http-worker memory | ≈855 MiB | 629–850; two fatals per day |
| Disk free | 72.3 GB | **40 GB** (a growth of ≈36 GiB/day) |

Error classes of the http tasks (since the 09:17 restart, 120,231 tasks): timeout
50,280 (42%), network 36,870 (31%), body 1,089, redirects 398, tls 52;
60% of tasks in the 10–30 s bucket — a timeout tail with a fast transport.
Domain statuses: alive 91.5%, temp_error 8.9%, no_http 1.7% (the timeouts are mostly
inside “live” domains — subdomain tarpits). CPU in steady state:
postgres 177%, chromium 140%, analyzer 66%, dns-worker 26%, http-worker
~90% of one core.

Problems and findings (filed as stage 24, tasks/tasks.md):

- **http-worker memory (P0, T-157)**: at 05:48 killed by OOM killer,
  at 09:17 an exit code at the peak of 900.5 MiB = MemoryMax; MemoryHigh=850M —
  120,095 throttling events (reclaim storms, swap 3.1–56 MiB).
- **CPU starvation of the box (P0, T-158)**: the monitoring aggregates over 47 million rows
  (BacklogLoop `GROUP BY check_state` every 30 s — a pass of 17+ s with
  parallel workers; ZoneStats `UPDATE zones`; status `SELECT z.id…count`)
  + the render cores + the analyzer eat the CPU away from the pipeline.
- **Render cores (P1, T-159)**: ERR_CERT_AUTHORITY_INVALID /
  ERR_CERT_COMMON_NAME_INVALID — permanent host errors — retried in
  a circle (no XACK → XAUTOCLAIM → attempts): last-delivered-id of q:render
  frozen since ≈08:59, the lag 99,161, fresh tasks not read for 2+ h;
  q:dead +366 “attempt limit 3 exhausted”.
- **Resolver caps (P1, T-160)**: unbound 733.9/734.0 MiB at
  MemoryMax 700M (1.19 million reclaim events; in §6 it was 452 MiB),
  the dns-worker pushes against 256 MiB; MemAvailable 2.6 GiB — there is headroom.
  Redis is at a normal 53/512 MiB (the measures
  of T-141/T-149 hold).
- **Monitoring aggregates (P1, T-161)**: the GROUP BY pass is longer than
  the cycle interval — the PG load piles up.
- **Alerts do not reach the operator (P2, T-162)**: all the P0 were found
  manually; the monitor/alerts.go framework (T-080/T-145/T-154) covers
  disk/queues/OOM/q:dead/Redis, but the signal is only a Warn in the journal.
- **Disk (P0, T-162/T-166/T-167)**: 72.3 → 40 GB in 20 h (content 20.3 +
  DB 15.5 GiB/day); expansion is unavailable — the observability T-162,
  “content retention + the subdomain_results trim” (T-166, the decision
  of the operator 2026-10-23; the journal rows are written only by live domains —
  a su snapshot: 70,981 alive → 140,944 rows, the other statuses 0) and
  a one-off cleanup of the accumulated 20 GB of content / 3.7 GB of journal (T-167).
- **Frozen with the goal of 50 domains/s (code — T-163…T-165)**: the slot memory
  (bodies until the end of the task), the timeout tail (dead-host cutoff),
  per-IP head-of-line — meanwhile the transport is fast (0.54 s/request),
  the bottleneck is in the task lifetime, not in the network.

The measurement was performed by an operator session on 2026-10-23 ~11:00 MSK; the filing
of stage 24 — tasks/tasks.md (stage 24).

## 13. The T-093 daily run — closing the criterion (2026-10-24)

The acceptance criterion of spec §11.4 item 7: “the service runs on the target server
within the memory budget (section 3.2) without the OOM killer firing during
a daily run”. Unlocked after T-135 (the server under load)
and T-085 (the budget pinned by the unit MemoryMax — deploy.md §2).

**The verification window: 23.10 09:17:40 → 24.10 10:26:37 MSK ≈ 25.1 h**
of continuous full load (the start boundaries: the completion of the http-worker fatal at 09:17 — the end of the T-157 incident, immediately after it the deploy lowering
256 → 192 slots; the end — an operational stop of the scheduler for the q:render drain,
the operations journal §5 of 24.10). The daily interval is covered
with a margin (~1 h). Inside the window 7 routine deploys (graceful stops,
NRestarts does not grow) and an apt-daily-upgrade wave at 06:08 (a restart of
postgresql/unbound/nginx — scheduled OS maintenance, not an incident);
the scheduler ran until 10:26 — the inflow was not frozen.

**Method** (all read-only): the kernel journal `journalctl -k` since 23.10
09:00; `systemctl show` + `memory.events` of the unit cgroups (oom/oom_kill);
the peaks of the completed unit lives — the systemd records “Consumed … memory
peak” in `journalctl -u <unit>`; the continuous T-092 sampler
(`t092-sampler`, `metrics.tsv`, the interval nominally 15 s, in fact
≈16.6 s under CPU starvation — 5,434 samples per window);
the current memory — the cgroup `memory.current`. The systemd/cgroup peaks include
file cache (the pages of read files), not only the process anon —
this is expected and not related to OOM: under pressure the kernel evicts the page cache
earlier than it kills the process (the anon throttling is visible via the swap peak in
the life records).

**Results.** The kernel OOM killer over the window: `journalctl -k --since
"2026-10-23 09:00" | grep -i oom` — **empty** (`--grep oom` — “No
entries”). No reboot in the window (the same boot); over the whole boot
the only kernel OOM records — 23.10 05:48:34, the cgroup OOM
of http-worker `CONSTRAINT_MEMCG` (the T-157 incident, before the window). For all
the units at the verification moment `memory.events`: oom=0, oom_kill=0;
NRestarts=0 — the counters are per-life (a deploy recreates the cgroup and
zeroes them, operations.md §4.4), so the end-to-end proof
over the window is held by `journalctl -k` (empty), the peaks of the completed lives
(below) and the absence of OOM records in `journalctl -u <unit>`;
no unscheduled restarts between the deploys, `journalctl -u` for
postgresql/unbound/redis/chromium — also without OOM records.

Memory peaks by unit (the budget — the MemoryMax from deploy.md §2; the peak —
the maximum across the completed lives from the systemd journal, for end-to-end
live units — the sampler; for a life at the cap — the fact without a kill):

| Unit | Budget (MemoryMax) | Peak over the window | oom_kill / NRestarts | Within the budget? |
|---|---|---|---|---|
| ingest | 128M | 43.0M | 0 / 0 | yes |
| scheduler | 128M | 71.9M | 0 / 0 | yes |
| dns-worker | 384M (256M before the 18:32 deploy) | 256.1M before 18:32 (a life at the old cap, the page cache was given back by reclaim — the finding of T-160), after — 190.6M | 0 / 0 | yes |
| http-worker | 900M (MemoryHigh 850M) | 851.2M (lives 850.7–851.2M) | 0 / 0 | yes |
| render-worker | 256M | 220.6M | 0 / 0 | yes |
| render-worker@9109 | 256M | 183.1M | 0 / 0 | yes |
| analyzer | 128M | 128.0M — at the cap: the page cache of the read content, anon ≪ | 0 / 0 | yes (see the observations) |
| monitor | 128M | 23.0M | 0 / 0 | yes |
| web | 256M | 57.0M | 0 / 0 | yes |
| dashboard | 128M | 23.8M | 0 / 0 | yes |
| chromium | 2048M | 1.6G — a cgroup peak with page cache (the journal format; the restart threshold 1.5 GiB — a worker metric, not reached) | 0 / 0 | yes |
| postgresql@16-main | 3072M | ≈3072M — at the cap (page cache, nominal, §6) | 0 / 0 | yes |
| redis-server | 640M (maxmemory 512 MiB, noeviction) | 65 MiB by the sampler (used_memory at the verification — 33 MiB) | 0 / 0 | yes |
| unbound | 800M | 769M — 96% of the cap (the cache growth over ≈47 million names, T-160) | 0 / 0 | yes |

http-worker — the subject of the T-157 incident — over the window: the sampler avg ≈747 MiB,
max 850.4 MiB (after the 09:17:40 boundary; a sample of 868.8 MiB at 09:17:11 —
the tail of the 256-slot life before the deploy), above MemoryHigh 850M — 24 samples
out of 5,434 (0.44% of the time). The profile changed inside the window: before the deploy
of 00:02 24.10 (the T-163 wave — a soft heap cap `http.mem_limit`
768 MiB, “the GC at the threshold returns the pages itself”, deploy.md §2) the unit
lived at MemoryHigh — the peaks of the completed lives 850.7–851.2M, the soak level
of wave 21 “860–893 MiB” (deploy.md §2, measured before T-163);
after — the GC holds the heap below the threshold (avg ≈747). The swap peaks of the lives
up to 483 MiB — the MemoryHigh reclaim hands the cold pages to swap;
no fatals since 09:17.

**Reconciliation with the spec §3.2 budget.** The budget mechanism — the unit MemoryMax
(T-085, deploy.md §2): no cap exceeded by any unit,
oom_kill=0 in all. The planned lines: Redis ≤0.5 GB — yes (a peak of 65 MiB
at maxmemory 512 MiB); Unbound ≈0.7 GB — at the upper bound (769 MiB
of 800M, the cache growth over ≈47 million names, T-160); PostgreSQL ≈2.5 GB —
at the 3072M cap together with the page cache (nominal, §6); Chromium ≈2 GB —
a peak of 1.6 GiB out of 2048M. The line “Go services (total) ≈1.3 GB”: the sum
of the cgroup memory of the nine units per the sampler avg ≈1.16 GiB (without
render-worker@9109 — that one added ≈0.11–0.18 GiB from 06:53, in total
actually ≈1.3 GiB), a one-off maximum of the sum 1.41 GiB (24.10 05:08;
the largest summands — http 849 at MemoryHigh, render 204,
analyzer 126, dns 146, the others ≈120) —
the cgroup accounting includes the page cache of the read files and brief coincidences
of the peaks of individual units, the page is evicted by the kernel under pressure;
the actual anon consumption is lower, each unit within its cap.
The spec line — “a reference, adjustable” (§3.2), the enforcement — by the mechanism
of the unit caps, as fixed in deploy.md §2 (“enforced by the actual consumption, not by the sum of the caps”).

**Observations (outside the T-093 criterion):**

- **MemoryHigh throttling of http-worker** — the `memory_high` alert went
  in bursts alternating with clearances, per the monitor journal: 24.10
  00:16 a firing (the monitor life was replaced by the 06:53 deploy —
  the clearance was not logged); 07:11 and 07:36 firings → a clearance at 09:55; 10:09 a firing → a clearance at 10:28 (right after the window);
  after the window: 10:39 → a clearance at 11:08, 12:28 → a clearance at 12:33, 13:02
  and 13:19 — open at the verification moment. Against the closure criterion
  of T-157 “MemoryHigh events are episodic, not continuous” —
  it matches: 0.44% of the samples above the threshold, no series of fatals.
  A controlled mode, not an OOM precursor.
- **PSI CPU some avg 88% (max 98.7%)** — the chronic CPU starvation
  of the box (the §12 measurement, T-158), does not affect the memory criterion; MemAvailable
  over the window did not fall below ≈1.7 GiB.
- **dns-worker at the old cap 256M** before the 18:32 deploy — a confirmation
  of the T-160 finding (the raise to 384M); the life peak 256.1M is 0.1M above
  the old cap — `memory.peak` catches the moment before the reclaim (the page cache
  was given back without a kill, up to 123.6 MiB went to swap); after the raise
  the peak is 190M.
- **analyzer/pg at MemoryMax by the cgroup memory** — the page cache of the read
  content/DB pages; the anon is significantly lower, the kernel gives the cache back under
  pressure (declared in §6 and deploy.md §2).
- **The freeze after the window** (10:26, again at 13:07) — an operational pause of
  the inflow for the q:render drain (the operations journal §5 of 24.10); the T-093 criterion
  is closed by the window before it, the freeze does not devalue it.

**Conclusion: the criterion of spec §11.4 item 7 is met.** The daily (25.1 h) run
on the target server at full load passed within the memory budget without
the OOM killer firing — neither the kernel one (`journalctl -k` over the window is empty),
nor the cgroup ones (oom=oom_kill=0 in all units, per-life), no unscheduled
restarts, all the peaks ≤ the unit MemoryMax (the table above).
Verification 24.10 13:20–13:45 MSK, read-only.

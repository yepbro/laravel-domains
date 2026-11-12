# Redis: the queue transport and configuration

In this project Redis is only a task transport (Redis Streams with consumer groups,
spec 4.2): six streams `q:dns`, `q:http`, `q:render`, `q:render-fallback`
(the queue of the render fallback to Chromium, spec 4.3), `q:analyze`, `q:dead`
and their groups. The source of truth is PostgreSQL (spec 3.4): losing Redis is not fatal,
the tasks are restored by the scheduler via the lease. This principle determines the memory
and persistence choices — see the trade-offs below.

Contents:

- [Configurations: dev and production](#1-configurations-dev-and-production)
- [What is stored in Redis](#2-what-is-stored-in-redis)
- [Memory: the budget and four layers of protection](#3-memory-the-budget-and-four-layers-of-protection)
- [Eviction policy: why noeviction](#4-eviction-policy-why-noeviction)
- [Persistence: why it is disabled](#5-persistence-why-it-is-disabled)
- [Network and other settings](#6-network-and-other-settings)
- [systemd and MemoryMax](#7-systemd-and-memorymax)
- [Checking the configuration](#8-checking-the-configuration)

## 1. Configurations: dev and production

| | Dev (docker) | Production (server) |
|---|---|---|
| File | `deploy/docker/redis/redis.dev.conf` | `deploy/redis/redis.conf` → `/etc/redis/redis.conf` |
| Launch | the `compose` container (the default profile), `mem_limit 256m` | the Ubuntu OS package under systemd (spec 10.1; the units — `deploy/systemd/`, T-085) |
| Network | `bind 0.0.0.0`, `protected-mode no` — the isolated docker network `crawler_net` | `bind 127.0.0.1 -::1`, `protected-mode yes` — all the services on the same server |
| `maxmemory` | `224mb` (below the container `mem_limit`, so that the OOM killer hits not Redis) | `512mb` — the spec 3.2 budget |
| `maxmemory-policy` | `noeviction` | `noeviction` |
| Persistence | none (`save ""`, `appendonly no`) | none (`save ""`, `appendonly no`) |

Both configs agree in substance (eviction, persistence), differing only
in the network binding and the memory limit of the environment. The connection parameters
of the services (`redis.addr`, by default `127.0.0.1:6379`) — in
[`configuration.md`](configuration.md), the `redis` section.

## 2. What is stored in Redis

- The stream keys `q:*` and the consumer groups nested in them: the message entries
  (the `data` field with a JSON envelope, the formats — `internal/queue/message.go`),
  the pending lists of the groups (the PEL — entries read but not acknowledged) and
  the delivery counters (the attempt counter `max_attempts` lives in the PEL, T-025).
  The acknowledged (XACK) prefix of the streams does not accumulate: it is freed by
  the scheduler XTRIM MINID loop (T-141, §3); in the streams live only
  the undelivered and the unacknowledged.
- The application keys with a TTL — two. The short-lived write probe
  `crawler:queue:write-probe` (T-152): a SET with a TTL of 2 h, refreshed by the live
  RecoverLoop (the claim gate — while Redis rejects the writes, the delivery
  attempts are not spent). The sets of the tarpit-classified
  render deliveries `render:tarpit:{task_id}` (T-261, TZ-fsm spec §3 FR-7):
  the tarpit finalization is decided by the number of deliveries that ended with a page timeout
  (SCARD ≥ `render.timeout_attempts`), and not by the PEL delivery number —
  mixed streaks do not finalize earlier than the threshold. A set member is
  “entry-id#delivery-number” (SADD is idempotent per delivery), a TTL of 48 h
  (2× the retry horizon of an episode) is refreshed by each mark; the budget — one
  set of ~150–250 B per tarpit task within the TTL window, worst case
  “all the render slots are tarpits” (16 contexts × 25 s ≈ 0.64 deliveries/s
  ≈ 110 thousand deliveries over 48 h, 2 deliveries per task) — ~55 thousand
  sets ≈ 10–15 MiB, a multiple below the headroom of the §3 budget; the real share
  of tarpits is visible in `crawler_render_tarpit_finalized_total`
  (docs/render.md). The loss of Redis (a command error) — a degradation of the decision to
  the PEL delivery number (the behavior before T-261), the loss of the keys themselves with
  a live Redis — the count anew: it will not finalize earlier than the threshold. Other
  data (caches, pub/sub states) Redis does not hold. The services run
  in db 0; db 15 is allotted to the resilience integration tests
  (the dev config, 16 DBs).
- Only the entries “in flight” (pending without a result in the DB) must not be lost —
  and those are recoverable: a domain in `queued` with an expired lease returns to
  `pending` and the task will be re-published (T-029, spec 3.4).

## 3. Memory: the budget and four layers of protection

Spec 3.2 gives Redis ≤ 0.5 GB — in production `maxmemory 512mb` (data), in dev
224 MB under a 256 MB container. The order of protection against overflow — from soft
to hard:

1. **Backpressure (T-026)** — soft regulation: the scheduler and the workers
   publish only when the stream depth (the backlog of the consumer group:
   the unread remainder `lag`, bounded by the stream length, plus
   the unacknowledged `pending`, XINFO GROUPS + XLEN) is strictly below
   `scheduler.queue_high_watermark`; above — the publication waits, one Warn goes to the log. The exception — `q:render`: the publication there
   is non-blocking (§5.6: the render does not delay the other stages), the capacity
   gate — a separate `render.queue_high_watermark` (T-174; 0 —
   shared, the default 10K ≈ 2 h of consumption): closed — the domain gets
   the `render_pending` flag (T-066), the same MAXLEN/2 clamp. `q:render-fallback`
   and `q:dead` are not gated at all (item 3 below). The effective
   threshold of a stream is not higher than half of its
   MAXLEN — the gate fires before the trim (for `q:dns` it is 125 batches
   ≈ 8 MiB, for the individual-entry `q:http`/`q:analyze` — 75,000 entries
   ≈ 52 MiB; `q:render` — 10K, see the table below). Bounding the lag
   by the stream length — the clamp of T-168: `lag` = `entries_added − entries_read`
   does not subtract the entries cut off by the trim as undelivered, and Redis
   7.0.x (production) holds the phantom lag forever — the incident of 22–23.10.2026
   (the T-149 drain left a lag of 1,753 at XLEN 1, the gate of `q:dns` closed
   for ~15.5 h; the analysis — runbooks §2.7). Redis 7.4+ corrects the lag on
   the trim by itself, the clamp makes the gate independent of the server version:
   the real backlog is not understated (as long as the undelivered are in the stream,
   lag ≤ XLEN), the phantom is cut off. The same formula is the only
   queue depth in the project (`queue.GroupDepth`, backpressure.go):
   the gate, `crawlerctl status`, `crawler_queue_depth`, the alert
   `queue_frozen` (T-162) and the “📬 Queues” line of the load report
   (T-173, with T-188) compute it by one shared formula — an observation has no
   right to diverge from the gate on whether there is work; XLEN as the depth
   remains only for a stream without groups (an accumulation without a consumer) and
   the quarantine of `q:dead` (the operator's inventory). A raw XLEN is never a depth: the acknowledged history before the trim is not a queue (the discrepancy
   of 25.10: the report printed XLEN and overstated `q:render` ~5-fold with
   the TrimLoop stopped by the freeze — analyzed in the operations journal
   [operations.md](operations.md)).
2. **Freeing the acknowledged prefix (T-141, spec 4.2 “Supplement
   2026-10-23”)** — `XACK` removes the entry from the group PEL, but not from the stream:
   the acknowledged history would accumulate to the MAXLEN cap and overflow the
   budget (the T-093 incident: the `q:dns` sink 1,735 batches ≈ 410 MiB,
   “OOM command not allowed”, the render/analyze publications failed).
   The scheduler loop (`queue.TrimLoop`, `redis.trim_interval`, by
   default 1 min) runs `XTRIM MINID` at the oldest entry that must
   remain: the oldest pending of the group (everything below is delivered and
   acknowledged — a stuck PEL is not lost), with an empty pending —
   `last-delivered-id` (the undelivered tail remains). Why XTRIM
   MINID and not XDEL after the Ack: three commands per stream per pass (~15/min
   across five streams) against an extra command per each
   acknowledged entry (thousands per minute at the peak of the HTTP stage); XTRIM is
   an atomic command, the race between reading the point and the trim is safe
   (new pending appear only above `last-delivered`, the entry ids
   are monotonic); the group PEL and lag are not touched (the undelivered is not
   trimmed; the phantom lag from a third-party trim of the undelivered —
   by the gate clamp, item 1). The mechanism reproduces the proven server-side
   solution of the incident (the `t092-trimhist` unit); while the scheduler
   is stopped, the streams are held by the MAXLEN caps.
   The integration guarantee — `TestResilience_TrimConfirmedPrefix`
   (`test/integration/resilience_test.go`): after “publishing →
   consuming → XACK → TrimOnce” the stream length returns to the boundary
   entry, and the key memory does not grow with the number of processed entries.
3. **MAXLEN ~ (T-025)** — the emergency cap of the length of each stream
   (the numbers — the table §3.1): there is no unbounded growth of keys
   (350 million domains × several streams); before it the gate has already stopped
   the publication to the working streams
   (`q:dead` and `q:render-fallback` are not bounded by the gate: the transfer
   into quarantine must not be blocked, and the publication of the fallback is part
   of the render finalization — waiting for capacity would delay the XACK of the task
   of q:render — the pathology of T-066; the fallback flow is held by the budget of the share
   and the pace of worker B, spec 4.3 item 3). The budget of `q:dead` is counted in bytes,
   and not only in entries (T-105): the entries **do not carry the payload of the original
   task** — only the identifiers of the subject (~150–300 B per entry,
   a `q:dns` batch — the size instead of the domain list), so the cap
   10,000 ≈ 2–3 MB. With the payload of the `q:dns` batches of the era of batch_size 5000
   (~250 KB per entry — the historical context T-105) the same cap
   gave a worst-case ~2.5 GB — a multiple of the budget at `noeviction`
   (the failure of the whole transport), so the byte protection is made a property of the entry shape,
   and not a scheduled trim.
4. **`maxmemory` + `noeviction`** — the last line: at the exhaustion of 512 MB
   the entries are rejected with the error `OOM` (see below), the data is not corrupted.

### 3.1. The budget arithmetic by the actual entry sizes

Measurements: a `q:dns` batch of 1,000 domains (`scheduler.batch_size`) ≈ 64 KiB
— ~66 B per domain with the stream overhead (the T-141 measurement on a live Redis 7.4,
MEMORY USAGE; the earlier estimate “237 KiB per batch” referred to the batches of the era of
batch_size 5000 — the incident 1,735 batches = 410 MiB ≈ 3.7 thousand domains
per batch); the individual entries of `q:http`/`q:render`/`q:analyze` ≈ 0.3–0.7 KiB
(docs/load-measurements.md §11, and not the “100–200 bytes” of the earlier estimate
T-025). The sum of the MAXLEN caps must fit the budget: at
`noeviction` a cap above the budget would mean a write rejection (OOM) earlier
than the trim — exactly this happened in the T-093 incident (the old caps:
`q:dns` 10,000 batches ≈ 2.3 GiB).

| Stream | MAXLEN | Entry size | Cap | Gate (min(watermark, MAXLEN/2)) |
|---|---|---|---|---|
| `q:dns` | 250 batches | 64 KiB (1,000 domains) | ≈ 16 MiB | 125 batches ≈ 8 MiB |
| `q:http` | 150,000 | 0.3–0.7 KiB | ≈ 103 MiB | 75,000 ≈ 52 MiB |
| `q:render` | 150,000 | 0.3–0.7 KiB | ≈ 103 MiB | 10,000 ≈ 7 MiB (`render.queue_high_watermark`, T-174; 0 — shared → 75,000) |
| `q:render-fallback` | 15,000 | 0.3–0.7 KiB | ≈ 10 MiB | — (not gated: the cap — the budget of the share and the pace of worker B, spec 4.3 item 3) |

The coupling of the cap with the control rate of the engine selection (the extra review round,
S-P3-6): the control fallback rate of the canary — ≤15% of the flow (spec §5.3),
i.e. with a live worker B (the pace ≥ the inflow) the depth is far below the cap.
The cap 15,000 — the last line of memory: the scenario of task loss —
a stopped B **and** an ignored depth alert (7,500) at the same time;
the control “depth ≪ 15,000” — in the acceptance of spec §6.2 (v0.2). Raising the cap
is not allowed (the budget 480 MiB), lowering the default of the share budget below the control
rate — would break the routine work of the candidate engine.
| `q:analyze` | 150,000 | 0.3–0.7 KiB | ≈ 103 MiB | 75,000 ≈ 52 MiB |
| `q:dead` | 10,000 | ~0.2 KiB (without payload, T-105) | ≈ 2 MiB | — (not gated) |
| **Total** | | | **≈ 337 MiB** | simultaneously ≈ 119 MiB (the q:render gate — 10K ≈ 7 MiB, T-174) |

512 MiB − 337 MiB ≈ 175 MiB of headroom — the service memory of Redis,
the allocator fragmentation, the client buffers and the growth of entries with long
names (the between-trims remainder of TrimLoop — a minute of acknowledged
entries: `q:dns` ≤ 60 batches ≈ 3.8 MiB at an enqueue pace of a batch/s,
individual ≈ 2 MiB per stream — and itself bounded by the MAXLEN caps). Even
a simultaneous stop of all the
consumers with live producers (the gates full: 8 + 3 × 52 + 2 MiB
≈ 166 MiB) leaves two thirds of the budget; in a healthy system the occupancy —
tens of MiB (the depths near zero, between the TrimLoop passes — seconds of entries).

The `q:dns` batch size is configurable (`scheduler.batch_size`
1000..10000, spec 5.3): the stream cap is proportional to it, so the q:dns limit is chosen for the worst normative batch — the invariant “the sum of the caps
≤ the budget” for the whole range (the worst case: a batch of 10,000 domains →
q:dns ≈ 157 MiB, the sum ≈ 478 MiB) is pinned by the pin tests `queue.TestStreamPolicyFitsBudget`
and `config.TestSchedulerBatchFitsStreamBudget` (from the actual validation
boundary). The cap `q:render-fallback` (15,000 — one tenth of
the individual-entry one: the fallback flow is bounded by the budget of the share and the pace of worker B,
spec 4.3/FR-9) is agreed with the same invariant: individual-entry 150,000 would make
the worst sum ≈ 570 MiB — a budget breach. The fallback cap —
the last line of memory, and not a working regulator: the standing depth
is held by the budget of the share (T-239) and the pace of worker B (T-240), growth is caught by the depth alert
(spec 4.6 item 3); the trimmed unacknowledged comes back only via a recheck —
as with the other MAXLEN caps. The explicit `redis.maxlen` —
a deliberate exit from the policy: its arithmetic is counted by the operator. The dev caveat:
the arithmetic of this section — the production budget of 512 MiB; dev Redis
(`maxmemory 224mb`) is below the sum of the caps, but the dev load is orders of magnitude
smaller (the depths near zero), and before T-141 the caps were ≥ 500 MiB for one
individual-entry stream — dev became only safer.

### 3.2. The temporary measures of the T-093 incident — remove after T-141

During the incident the runtime memory bonuses and the external
trim loop were raised on the server. After the merge of T-141 they are redundant (the freeing is built in
the scheduler) and are subject to removal:

```bash
systemctl disable --now t092-trimhist   # the external XTRIM MINID loop
# /etc/redis/redis.conf: revert maxmemory 512mb (it was 900mb),
# to the redis unit (T-085) — MemoryMax=640M (it was 1G); then
systemctl restart redis
```

## 4. Eviction policy: why noeviction

The comparison of the options for our keyspace (only streams and groups, spec 3.4 —
everything is restored from PostgreSQL):

| Policy | Behavior at exhaustion | Trade-off |
|---|---|---|
| `volatile-ttl` / `volatile-lru` | Evicts only the keys with a TTL | There are no keys with a TTL — the policy degenerates into `noeviction`, creating a false sense of protection |
| `allkeys-lru` / `allkeys-lfu` | Silently deletes whole keys | Eviction in Redis works at the level of whole keys: a stream key disappears together with **all** the contents, the consumer groups and the PEL (the “in flight” messages, the T-025 attempt counters), and the next `XADD`s silently recreate the stream already without the groups — not a single error to the publishers. The long-unread `q:dead` — the first candidate. The tasks disappear without a trace in the logs |
| `noeviction` (chosen) | Rejects the entries (`OOM command not allowed when used memory > 'maxmemory'`) | The publication fails with an error — the workers/scheduler log a Warn and live on (spec 3.4: the failure of a component does not block the others), not a single accepted entry is lost |

Both alternatives were checked on `redis:7.4` (30,000 `XADD` into one stream with
a group created in advance, `maxmemory 2mb`): `allkeys-lru` accepted all the
entries silently, but on reaching the limit deleted the stream key whole —
the first ~17,000 entries and the group disappeared, the next `XADD` recreated
the stream without the group, in the end 12,874 entries remained and not a single write
rejection; `noeviction` accepted 17,211, rejected the rest with the error `OOM` —
and all the accepted are intact, the group is in place. A loud rejection — a visible signal
to the on-duty operator (and to the monitor metrics, T-077/T-080), a silent corruption — is not.

The choice of **`noeviction`** — a deliberate continuation of the architecture “Redis —
an expendable transport”: a loud write rejection is preferable to a silent corruption,
because (a) up to the cap the publication is stopped by the backpressure gate
long before 512 MB, (b) the streams are already bounded by MAXLEN, (c) even the complete loss of
Redis is routinely restored by the lease pass (T-029, the integration test
T-032). Eviction gives nothing: MAXLEN protects from the truncation, and
“saving memory” at the price of a silent loss of messages and the PEL contradicts
at-least-once delivery (spec 3.4). The dev config uses the same policy.

Confirmation in production (22–23.10.2026): the OOM window (the legacy batches of `q:dns`
≈620 MiB on top of the temporarily raised maxmemory, T-093) for about two hours
rejected the result publication XADDs with the error `OOM command not allowed`
(16,953 rejections at http-worker, 1,953 at render-worker, 392 at dns-worker,
243 at the scheduler) — a loud rejection, visible in the journals; the streams, the groups
and the PEL survived, the data is not corrupted. A side effect — a mass growth of
`q:dead` (the publication error burned the attempts by claims): the signature,
the diagnostics and the triage — [`runbooks.md`](runbooks.md) §2.6, the post-mortem —
[`load-measurements.md`](load-measurements.md) §11.1.

### 4.1. OOM behavior: OOM does not burn attempts (T-152)

A publication error — an infrastructure error, not a task one: the result is already
computed, a reprocessing would give the same write rejection. After T-152
the pipeline survives an OOM window without attempt losses (before T-152 one window
of 2.25 h cost 5,507 deaths in `q:dead` at fully working processing):

- **Classification** — `queue.InfraError` (internal/queue/infra.go):
  the OOM/LOADING/READONLY server responses and the transport failures (a connection
  drop, a timeout, the pool) are marked in `Publish` (XADD) and `Ack`;
  processing errors (the result DB, decoding) are not wrapped with the marker — their repeated
  deliveries are meaningful. The worker errors carry the marker through the `%w` chains (`queue.IsInfra`).
- **Publication retry** — XADD at an infrastructure failure is retried
  2 times with a doubling of the pause from 250 ms (≈0.75 s worst case): second-scale
  connection flaps are closed on the spot, without a circle of claims (each claim —
  +1 delivery toward the `redis.max_attempts` limit).
- **Consumption pause** — the http/render workers on a publication `InfraError`
  stop reading their streams until the write is restored (the probe
  `Reliab.Writable`, the fuse pattern of T-139/T-142): drawing tasks whose
  results cannot be written is pointless — they would accumulate in the PEL. The reader modes (reading | browser pause | infra pause | fuse pause | rate pause; in q:http the browser and the rate are not mounted — the modes
  are unreachable) are formalized by the state machines of
  `internal/renderworker/readerstate.go` and `internal/httpworker/readerstate.go` (T-262): the pass mode is chosen by one function over the observations (`Alive`, `Writable`,
  `failed`, allowance), the pause priorities — the transition table there;
  the infra pause does not read the stream — the delivery attempts do not burn.
- **Claim gate** — RecoverLoop before each pass checks the write (a SET of the short-lived key `crawler:queue:write-probe`): while Redis
  rejects the writes, the claim is skipped — the PEL delivery counter
  (XPENDING `RetryCount`) does not grow. The write is restored — the claim
  returns the deliveries, the processing carries them to XACK: the tasks pass
  the OOM window with at most +1 delivery, not getting into `q:dead`.

The integration check of the scenario (a live Redis, `maxmemory` below
the usage) — `TestResilience_OOMDoesNotBurnAttempts`
(test/integration/resilience_test.go).

### 4.2. A RecoverLoop cancellation does not burn attempts (T-222)

The second source of attempt burn-down (next to the OOM §4.1) — a cancellation of the recovery loop
in the middle of a claimed batch. The semantics of `XAUTOCLAIM`: the PEL delivery counter
(XPENDING `RetryCount`) is incremented for **all the entries of
the batch at the moment of the claim**, and not at the moment of the start of processing of the entry.
RecoverLoop walks the batch sequentially (the render — seconds per entry),
the graceful shutdown budget — 10 s (the `ShutdownTimeout` of the service
framework, not changed by configuration): a stop in the middle of a batch left a tail
with a burned attempt without a single handler call. Minus an attempt per
restart; after `redis.max_attempts` the task went into `q:dead`,
having not been processed even once — the same mechanism that burned the cohort
in the 22.10 incident (the gate of §4.1 covers an unavailable Redis; it does not see
a context cancellation).

The fix — the return of the attempt to the unprocessed tail: `XCLAIM <stream> <group>
<consumer> 0 <ids…> JUSTID RETRYCOUNT <attempts-1>` under a separate
short context (up to 5 s inside the overall stop budget, the sample —
the discard in ingest): `RETRYCOUNT` sets the counter to the exact value
(overriding the claim increment, Redis ≥ 6.2), `JUSTID` saves the bodies
in the response and does not touch the counter by itself. An entry started by processing
spends the attempt — the delivery has happened. The counter classification after
the claim survives a ctx cancellation (without cancelling the caller): the classified
entries are returnable with the already known counter. The return runs while
the batch walk fits into `claim_min_idle` (the T-153 invariant): after
a re-claim window by a neighboring worker the return would cancel a foreign increment —
it is skipped with a Warn. The price — `XCLAIM` refreshes the delivery time:
the returned entries ripen for `claim_min_idle` again. The boundaries (best
effort): a hard kill of the process between the claim and the return, or
Redis unavailable at the stop, leave the attempts spent.

The integration check on a live Redis (including the real semantics
of `XCLAIM JUSTID RETRYCOUNT` and `JUSTID` without `RETRYCOUNT`) —
`TestResilience_CancelMidBatchDoesNotBurnAttempts`
(test/integration/resilience_test.go).

### 4.3. Unfinished classification and entries without task_id (T-260)

The return window of §4.2 is extended from “a ctx cancellation” to “any unfinished
classification” (T-260, FR-6.2 of TZ-fsm): a failure of the XPENDING read after
the claim (a transient error, the classification budget) earlier burned an attempt
without a trace — an entry without a read counter did not get into the return.
Now `Claim` returns such entries as a separate result, and
`RefundUnclassified` reads the counter **at the moment of the return** (a repeated
XPENDING per each entry) and rolls it back by the same mechanism
`XCLAIM JUSTID RETRYCOUNT <counter-1>`: a repeated read failure or
an unavailable `Do` window leave the attempt spent (no worse than
the behavior before the fix — the next successful classification will decide the fate
of the entry). The return budget of a pass — one `refundTimeout` (5 s) for all
the returns (the unclassified entries and the unprocessed tail at
a cancellation): both returns really happen in one pass (a failure
of classification + a cancellation), independent budgets would sum up to 10 s —
the whole graceful shutdown budget. A live Redis does not crash XPENDING —
the scenario is checked
by injecting a failure into the client:
`TestResilience_PendingFailureDoesNotBurnAttempts`
(test/integration/resilience_test.go).

There the eternal claim loop of the entries without `task_id` is closed (FR-6.1):
a body outside the envelope formats earlier was not moved into `q:dead` (bury
rejected the transfer), the entry spun in the PEL in a circle “claim → transfer
error”. Now `bury` publishes Dead with a synthetic identifier
`unknown:<stream>:<entry_id>` (without subject fields — the subject of such
a body cannot be identified) and acknowledges the original entry: the quarantine
is terminal, the triage is manual, the flow counts
`crawler_queue_dead_unidentified_total` (a steady zero). The live
semantics — `TestResilience_DeadUnidentifiedNoTaskID`. The specification
of the entry delivery state machine (states/transitions/invariants) —
`internal/queue/deliverystate.go`.

## 5. Persistence: why it is disabled

`save ""` + `appendonly no` — as in dev. The arguments:

- Spec 3.4 explicitly allows the loss of Redis: the tasks are restored by the scheduler.
  This is pinned by the integration test `TestResilience_RedisLossSchedulerRecovery`
  (T-032, `test/integration/resilience_test.go`): after the streams and
  groups are deleted the lease return (T-029) returns the domains from `queued` into `pending`,
  the dispatcher (T-027) re-selects, the enqueueing (T-028) re-publishes, the live
  worker recreates the group after `NOGROUP` (`internal/queue/redisq.go`).
  Exactly this path also runs at a Redis restart without persistence —
  the recovery is bounded by the lease term (`scheduler.lease`, 30 minutes),
  and not by the AOF load speed.
- The price of AOF: fsync (`appendfsync everysec` — we lose up to a second of tasks, that is
  the “in flight” guarantees are absent anyway) on every XADD; the write
  load is high already (spec 10.3 — the NVMe wear monitoring, [`disks.md`](disks.md))
  — adding a write stream for the sake of data that is cheaper to re-publish,
  is unprofitable. RDB snapshots do not suffer from the same problem, but give no
  benefit either: the freshness of the task batch is not critical.
- What is lost at a restart without persistence: the contents of `q:dead` —
  the history of the tasks that exhausted the attempts; the undelivered entries of the working
  streams WHOSE domains have already left `queued` — a `q:http` task with
  a written DNS result “there are addresses” (check_state = done, status new):
  the lease pass does not see such a row, the compensator “dangling status” returns it
  (T-270, an hourly pass of `new ∧ done` older than 24 h —
  the gauge `new_done_stale`, FR-2.2 of TZ-fsm; the incident of 22.10 — a cohort
  of 1,865 domains). The other tasks are not lost: their domains
  stay in `queued`/`leased`, the lease pass will re-publish, at repeated
  rejections they return into `q:dead`. For “manual triage” (spec 4.2) this
  means triage by the fact of recurrence, and not by the accumulated history.

Enabling AOF makes sense only if the measurements (spec 11.3) show that the window
of re-publishing by the lease pass (up to 30 minutes of stage downtime after a Redis restart)
costs more than the disk wear; then — `appendonly yes`, `appendfsync everysec`,
`aof-use-rdb-preamble yes` in `/etc/redis/redis.conf`.

## 6. Network and other settings

- `bind 127.0.0.1 -::1`, `protected-mode yes` — all the Go services are on the same
  server (spec 3.1, 10.1) and go to `redis.addr 127.0.0.1:6379`; there is no password in
  the service configuration (`internal/config`, the `redis` section), so
  Redis does not stick out outward. The prefix `-` at `::1` — an “optional address”:
  a host without IPv6 does not break the config. A Unix socket instead of TCP is possible
  (`unixsocket`/`unixsocketperm 770` + access by a shared services group), but
  it complicates the units of T-085 and gives no measurable benefit on loopback.
- `tcp-backlog 511` — the connections are few (7 of 9 services go into Redis —
  the scheduler, four workers, monitor, dashboard; ingest and web — only
  PostgreSQL; the go-redis
  pools are lazy, the workers hold one connection blocked in `XREADGROUP`
  each), no connection bursts; consistent with
  the host `net.core.somaxconn` (T-088).
- `timeout 0` + `tcp-keepalive 300` — the worker connections are long-lived: an “idle” connection is busy with a blocked read (`BLOCK 2s`),
  tearing it down — an idle reconnect; dead clients are revealed by keepalive.
- `io-threads 1` (off) — the load is hundreds of commands/s (XADD/XREADGROUP/
  XACK; the pace is set by the DNS/HTTP stages), the single-threaded core suffices by orders of magnitude;
  the benefit of the I/O threads starts at tens of thousands of ops/s. Turning on —
  only by the measurements of spec 11.3.
- `databases 16` — as in dev: db 0 for the services, db 15 — the contract
  of the resilience integration tests.
- `supervised auto`, `daemonize no`, `logfile ""` — running under systemd
  (T-085) in the foreground, the logs in journald.

## 7. systemd and MemoryMax

`maxmemory` limits only the data; on top sit the client buffers,
the allocator fragmentation and the service memory of the process. Therefore the systemd unit
of Redis (T-085) must have `MemoryMax` **with headroom over `maxmemory`** —
the recommendation `MemoryMax=640M` (512 MB of data + ~128 MB of service memory): then
at memory exhaustion `noeviction` fires (a visible write rejection,
Redis alive), and not the systemd OOM-killer (a noisy restart for the same
result). Formally this is a bit above the line “Redis ≤ 0.5 GB” of spec 3.2,
but the budget of the line refers to the data (`maxmemory`), and the actually occupied
in a healthy system is ~100 MB (see §3).

## 8. Checking the configuration

`redis-server` has no “check the config” mode — the check is by running a
one-off container with the production config (does not touch the dev stack). A nuance: `dir`
from the file is applied at the parsing moment (an immediate `chdir`), an override
`--dir` in the arguments does not save — the directory is mounted as a volume:

```bash
docker run --rm -d --name redis-conf-check \
  -v "$PWD/deploy/redis/redis.conf:/etc/redis/redis.conf:ro" \
  -v redis-conf-check:/var/lib/redis \
  redis:7.4-alpine redis-server /etc/redis/redis.conf
docker exec redis-conf-check redis-cli CONFIG GET \
  maxmemory maxmemory-policy appendonly save bind protected-mode io-threads
docker exec redis-conf-check redis-cli SET probe ok && \
docker exec redis-conf-check redis-cli GET probe   # the writes work
docker stop redis-conf-check && docker volume rm redis-conf-check
```

Outside systemd `supervised auto` quietly returns to running without supervision.
The meaningful content (`CONFIG GET`) must match the file; a clean
start — a syntax validation. The same way the behavior
of the eviction policies was checked (§4): `--maxmemory 2mb --maxmemory-policy allkeys-lru`
against `noeviction` on a flow of 30,000 `XADD`.

At the check in the container Redis prints `WARNING Memory overcommit must
be enabled!` — this is a host-level recommendation (`vm.overcommit_memory=1`,
sysctl — the territory of T-088), and it matters only for the background saves
of RDB/AOF and replication, which are disabled in our configuration.

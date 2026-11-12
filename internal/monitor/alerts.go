// Monitoring alerts — the minimal set of spec 5.9 (T-080; spec 8, spec 10.3),
// the evaluation loop lives in the same monitor service as the zone recount (T-078).
//
// The set and its sources:
//
//   - growth of the DNS error share — the share of temp_error among the checks of
//     the current day (the daily crawler_counters rows temp_error:/checks: —
//     point reads, T-182; temp_error is the DNS-stage outcome after
//     retries are exhausted, spec 5.4; NXDOMAIN → not_delegated is a valid
//     outcome, not an error);
//   - queue overflow — the depth of the working streams q:dns/q:http/
//     q:render/q:analyze (the consumer group backlog) against
//     scheduler.queue_high_watermark (the backpressure threshold, spec 4.2/5.3;
//     q:dead is not capped by the gate — see the header of
//     internal/queue/backpressure.go — and is not checked);
//   - progress stall — zones.updated_at (written by ZoneStatsLoop, T-078)
//     older than N recount periods: a live monitor marks its own work itself;
//   - disk space shortage — statfs of the content directory against
//     content.disk_free_percent (the same threshold that puts content Put
//     on pause, spec 8; the formula is as in internal/content/disk.go, the approach
//     is reused via an owned copy — the storage package is not pulled in);
//   - NVMe wear (SMART) — percentage_used from the NVMe health log (spec 10.3),
//     read via an ioctl of the admin command, without nvme-cli;
//   - junk content_hash of cleanup (T-099) — a spike of the
//     crawler_ingest_bad_hashes_total counter in the /metrics exposition of the
//     ingest service (BadHashesSource, the IngestBadHashes implementation — badhashes.go):
//     counter growth is a result-writer error (T-056), no data is
//     lost (the refcount on the junk reference did not change, ING-P2-4);
//   - OOM and restarts of crawler units (T-145) — the memory.events
//     counters (oom/oom_kill) of cgroup v2 and the systemd NRestarts (UnitEventsSource,
//     the OSUnitEvents implementation — unitevents.go): an event is an unscheduled
//     pipeline downtime and a symptom of the memory budget not being met (spec 3.4,
//     docs/deploy.md §2).
//   - untriaged q:dead (T-154) — the depth of the quarantine (XLEN via the
//     same StreamLener: q:dead has no consumer groups) persistently
//     non-zero; cross-checked against the deaths counter
//     crawler_queue_dead_buried_total (metrics/dead.go) — the depth
//     (the sink) holds the signal that the rate stretches over hours
//     with slow claiming (post-mortem docs/load-measurements.md §11.1);
//   - Redis memory (T-154) — used_memory/maxmemory from INFO memory
//     (RedisMemorySource, the RedisInfo implementation — redismem.go): with
//     noeviction (spec 3.2) filling maxmemory is a proxy metric of
//     upcoming OOM write refusals; INFO does not expose a direct refusal
//     counter;
//   - Redis restart (T-270, TZ-fsm spec §3 FR-2.2) — uptime_in_seconds from
//     INFO server (RedisUptimeSource, the RedisInfo implementation —
//     redisuptime.go) below the AlertRedisRestartWindow: a restart without
//     persistence (docs/redis.md §5) loses all undelivered stream
//     entries — the main cause of the "status hung" loss class
//     (lost q:http with done already written is handled by the T-270
//     compensator); the signal comes at the moment of the event, not as a
//     gauge cohort a day later;
//   - absolute minimum of disk space (T-162) — the free bytes of the
//     content directory against monitor.alerts.disk_free_min_bytes
//     (Warn) and disk_free_crit_bytes (escalated as a separate alert):
//     a percentage threshold is late on a large volume — 10% of 79 GB = 8 GB
//     ≈ 5h until full at a rate of ≈36 GiB/day (measurement §12);
//   - MemoryHigh throttling (T-162) — growth of the unit's memory.events high
//     counter between passes (the source is the same UnitEventsSource as
//     unit_oom): a harbinger of http-worker fatals (T-157) before the OOM kill;
//   - frozen queue (T-162) — the last-delivered-id of the working
//     stream's group does not move for ≥ monitor.alerts.queue_freeze_window while
//     depth > 0 (StreamGroupsSource, the RedisStreamGroups implementation —
//     streamgroups.go): the signature of the poisoned task of T-159 — an entry
//     kills the consumer, XREADGROUP reads nothing, the queue stands still;
//   - PSI CPU (T-162) — some avg300 of /proc/pressure/cpu above
//     monitor.alerts.psi_cpu_percent sustained for monitor.alerts.
//     psi_cpu_window (PSISource, the OSPSI implementation — psi.go): scheduler
//     starvation that drowns all stages at once (measurement §12:
//     some 91–95% at peaks; T-158 gave monitor a CPU weight of 50 against
//     1000 — its own passes do not approach starvation);
//   - render debt accumulation (T-175) — render_pending flags
//     (the crawler_counters counter, migration 000015) + the q:render lag
//     (renderdebt.go) above monitor.alerts.render_debt_domains AND growing
//     for monitor.alerts.render_debt_passes passes in a row: the queue is
//     overflowing and inflow outruns drain — the T-162 "frozen queue"
//     alert does not catch this case (deliveries go through);
//     the debt of 443k (the freeze of Oct 24) was found by manual SQL;
//   - growth of the render fallback queue depth (T-246, render spec §4.6
//     item 3) — the depth of q:render-fallback (with the same semantics as the
//     queue alert: the backlog of worker B's consumer group) at the effective
//     threshold of the stream AND growing for 2 passes in a row, OR holding above
//     the threshold for 2 passes in a row without growth (S-P2-1: a stationary
//     depth — B stalled, pinned at MAXLEN, or an episode after
//     a monitor restart) — the chromium worker B does not
//     keep up with finishing its work (the worker rate is below the fallback inflow, up to
//     a full stop of B — the Redis read is alive with a dead worker;
//     the crawler_queue_depth{queue="q:render-fallback"} metric exposes
//     B itself, and when it dies the series is gone);
//   - exceeding the fallback share budget (T-246, render spec §4.6 item 3) —
//     growth of the suspension events counter
//     crawler_render_fallback_suspended_total{reason="budget"} (T-239,
//     the /metrics probe of the render worker, suspends.go): the share of publications over
//     the budget window went past the render.fallback.share_percent cap —
//     degradation of the primary engine (engine-dependent finalizations
//     multiply); tasks are not lost meanwhile (finalization is normal, FR-9).
//     A falling engine unit is not introduced as a separate alert: the engine
//     units of render — crawler-*.service (crawler-chromium today, moli/
//     lightpanda per the render spec §4.7 item 3) — are covered by the existing
//     unit_oom (T-145, the unitEventsPattern template);
//   - sustained growth of browser errors of the render stage (S-P2-6, the render spec
//     §4.6 item 3, the operator's decision — a rate detector) — the counter
//     crawler_stage_errors_total{stage="render",type="browser"} from
//     the same /metrics probe of the render worker grows for 2 passes in a row:
//     the candidate engine (moli/lightpanda) is hung or unavailable (the gate
//     of T-239 suspends fallback publications; the gate event —
//     the suspended{reason="gate"} counter and the worker's Warn log; the detector —
//     here);
//   - "status hung" domains (T-256, TZ-fsm spec §3 FR-2.2) — the
//     crawler_counters new_done_stale row (the FILTER column of the
//     reconciliation pass of zonestats.go: status = new ∧ check_state = done ∧
//     last_checked_at older than a day — DNS wrote the "has addresses" branch, the HTTP
//     outcome never arrived: a lost q:http task)
//     non-zero in 2 NEW reconciliation snapshots in a row (the series — by
//     the change of the row's updated_at: alert passes are more frequent than the source,
//     repeated reads of one snapshot do not count as observations).
//     A point PK read, no scan of domains; stationary zero normally.
//     Treated by the T-270 compensator (the early recheck of R15 of the
//     check_state specification, the hourly scheduler pass of stalerequeue.go —
//     its guard reads the same row); the alert remains the signal about new
//     losses (the crawler_domains_new_done_stale gauge exposes
//     the level).
//
// Delivery (T-162): alert transitions (firing/clearing) go
// to the operator in Telegram — the AlertsOptions.OnPass hook (TelegramNotifier,
// notify.go) calls exec deploy/telegram-notify.sh --message with debouncing
// of "state change only"; without secrets/the script delivery is off,
// the signal remains a Warn in the log (spec 10.4 — the same channel as CI/CD).
//
// Signaling is front-loaded (on transitions, the DiskWatchLoop style in internal/content):
// Warn on firing, Info on clearing, Debug when the alert does not apply
// ("not measured"). Source errors (DB, Redis, statfs) — a Warn every
// pass, the loop keeps living (the OnRun contract, T-012). The exception is NVMe SMART:
// in a Docker container /dev/nvme* is unavailable — this is an expected state, not
// an incident — an honest "not measured" degradation without a Warn and without
// a false firing; on a production systemd host the layer is enabled by granting the unit access to
// /dev/nvme* (the nvme-smart group, T-089 — docs/disks.md) next to the host-side
// smartd.
//
// Hysteresis — for alerts that flap at the threshold: the DNS error share (firing
// 30% / clearing 20%), queues (firing at the gate threshold / clearing below
// 90% of the threshold) and Redis memory (80% / 75%, T-154); q:dead is damped
// by persistence (3 passes) and cleared by an empty quarantine (T-154). Disk
// mirrors the Put pause threshold without a gap: the alert is visible
// exactly when content writing is stopped; progress stall and
// wear change only via events, not via a threshold — hysteresis is not needed.
// The new T-162 alerts are damped by persistence instead of hysteresis:
// memory_high — a series of passes with counter growth; queue_frozen — a window
// of last-delivered-id immobility at non-zero depth; psi_cpu — a window
// of holding avg300 above the threshold (avg300 itself smooths over 5 minutes);
// the absolute disk minimums — without a gap, like the percentage one.
//
// State export — the AlertsResult pass snapshot into the
// AlertsOptions.OnPass hook (the decision: log + field). An own Prometheus
// gauge registry is not created in monitor — do not duplicate internal/metrics
// (T-076): the alert metrics (crawler_alert_firing by labels) will be added by the
// T-077 layer on top of the shared registry; the hook gives an attach point without editing
// the loop.
//
// The alert state machine (T-264, spec tasks/TZ-fsm.md §3 FR-10): each alert is
// a value of a single "{name, status, track memory}" structure
// (alertMachine) with the single state-writing point transition;
// the eval functions of this file return an observation; the decision
// is made by the typed track memories. The specification of states and
// transitions — internal/monitor/alertstate.go.
package monitor

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"crawler/internal/db"
	"crawler/internal/queue"
)

// Alert statuses (AlertSnapshot.Status).
const (
	// AlertStatusOK — the alert condition is not met.
	AlertStatusOK = "ok"
	// AlertStatusFiring — the alert fired (Warn on the transition).
	AlertStatusFiring = "firing"
	// AlertStatusNotMeasured — the alert does not apply in the current environment
	// (no data/the source is unavailable): it never fires falsely.
	AlertStatusNotMeasured = "not_measured"
)

// Alert names — the label in the logs and the pass snapshot.
const (
	AlertDNSErrors        = "dns_temp_error_share"   // the temp_error share of the day
	AlertQueueOverflow    = "queue_overflow"         // queue depth at the gate threshold
	AlertProgressStall    = "progress_stall"         // zones.updated_at is stale
	AlertDiskFree         = "disk_free"              // content directory space
	AlertNVMeWear         = "nvme_wear"              // SMART percentage_used
	AlertBadHashes        = "ingest_bad_hashes"      // junk content_hash spike
	AlertUnitOOM          = "unit_oom"               // OOM/restarts of crawler units (T-145)
	AlertQueueDead        = "queue_dead"             // untriaged q:dead (T-154)
	AlertRedisMemory      = "redis_memory"           // Redis memory near maxmemory (T-154)
	AlertRedisRestart     = "redis_restart"          // Redis restart: undelivered stream entries lost (T-270)
	AlertDiskMin          = "disk_free_min"          // absolute space minimum, Warn (T-162)
	AlertDiskCrit         = "disk_free_crit"         // absolute minimum escalation (T-162)
	AlertMemHigh          = "memory_high"            // MemoryHigh throttling (T-162)
	AlertQueueFreeze      = "queue_frozen"           // frozen queue (T-162)
	AlertPSICPU           = "psi_cpu"                // PSI CPU some (T-162)
	AlertRenderDebt       = "render_debt"            // render debt accumulation (T-175)
	AlertRenderFbDepth    = "render_fallback_depth"  // q:render-fallback depth growth (T-246)
	AlertRenderFbBudget   = "render_fallback_budget" // fallback share budget exceeded (T-246)
	AlertRenderBrowserErr = "render_browser_errors"  // stage browser-error growth: engine unavailable (S-P2-6)
	AlertDomainsStale     = "domains_new_done_stale" // "status hung" domains new∧done older than a day (T-256, FR-2.2)
)

// alertDescs — human-readable alert descriptions for log entries.
var alertDescs = map[string]string{
	AlertDNSErrors:        "DNS error share growth",
	AlertQueueOverflow:    "queue overflow",
	AlertProgressStall:    "progress stall",
	AlertDiskFree:         "disk space shortage",
	AlertNVMeWear:         "NVMe wear",
	AlertBadHashes:        "junk content_hash in deleted-domain cleanup",
	AlertUnitOOM:          "OOM kills and crash restarts of crawler units",
	AlertQueueDead:        "dead tasks in q:dead are not triaged",
	AlertRedisMemory:      "Redis memory near the maxmemory cap",
	AlertRedisRestart:     "Redis restart: undelivered stream entries lost (no persistence)",
	AlertDiskMin:          "free space below the absolute minimum",
	AlertDiskCrit:         "free space below the escalation threshold",
	AlertMemHigh:          "unit MemoryHigh throttling (sustained growth of reclaim events)",
	AlertQueueFreeze:      "frozen queue: the group is not reading the stream",
	AlertPSICPU:           "CPU starvation (PSI some) sustained above the threshold",
	AlertRenderDebt:       "render debt accumulation: SPA inflow outruns the q:render drain",
	AlertRenderFbDepth:    "q:render-fallback queue buildup: worker B cannot keep up",
	AlertRenderFbBudget:   "fallback share budget exceeded: primary engine degradation",
	AlertRenderBrowserErr: "render stage browser-error growth: the engine is hung or unavailable",
	AlertDomainsStale:     "domains \"status hung\": new with a finished check older than 24 h",
}

// Evaluation thresholds. The spec does not set numbers; existing config
// thresholds are reused (queue_high_watermark, disk_free_percent); the rest —
// constants with justification (a smaller config surface, the values
// are tied by stage mechanics, not by operator tuning).
const (
	// DefaultAlertsInterval — the alert evaluation period: 5 minutes is the
	// fallback for a zero AlertsOptions.Interval; the working value is
	// monitor.alerts_interval (T-206 — decoupling from reconciliation,
	// the operator's decision of Oct 26). Frequency history: the original 60 s was the
	// top of the spec 5.9 corridor "30–60 s", sized for a small corpus; T-161
	// kept "one pair of cycles" on zone_stats_interval (5 min);
	// T-199 thinned reconciliation to 30 min — the detection windows in passes
	// stretched 6×; T-206 gave the alert loop its own
	// 5 min, the effective windows unchanged: queue_dead fires in ≈15 min
	// (3 passes), mem_high/render_debt — in 10 min (2 passes),
	// bad_hashes holds a 75 min window (15 passes), the
	// PSI sampling error is ~5 min.
	DefaultAlertsInterval = 5 * time.Minute

	// AlertStallFactor — the progress stall threshold in periods
	// of RECONCILIATION: updated_at older than 5 × zone_stats_interval —
	// a stall. The threshold unit is the ZoneStatsLoop period (the stamp is written
	// by exactly that loop), not the alert interval: with 30 min reconciliation
	// a threshold of 5 × 5 min would fire forever; more frequent alert
	// passes (T-206) only poll freshness more often; the threshold stayed
	// "5 missed reconciliation passes" (≈2.5h at the default
	// 30 min). A T-078 pass over tens of millions of rows takes minutes: a single
	// missed period is not an incident, 5 periods is a confident stall.
	AlertStallFactor = 5

	// AlertDNSFireShare — the temp_error share that fires the alert: 30%.
	// The adaptive DNS limiter (T-035) keeps the timeout share ≤10%,
	// lowering QPS: 30% is a sustained resolver failure (Unbound/upstream
	// unavailable), not an adaptive dip.
	AlertDNSFireShare = 0.30

	// AlertDNSRecoverShare — alert clearing below 20%: hysteresis, a share
	// in the 20–30% corridor does not flap the Warn.
	AlertDNSRecoverShare = 0.20

	// AlertDNSMinSamples — the minimum number of checks in the window to evaluate the share:
	// 2 of 2 in temp_error is the noise of an empty window (right after midnight or
	// downtime), not error growth.
	AlertDNSMinSamples = 100

	// AlertQueueRecoverFactor — queue alert clearing below 90% of the threshold.
	// The gate is soft (between the check and publication other producers
	// add entries, T-026): depth in the 90–100% corridor of the threshold
	// would flap the Warn. Firing is exactly at the gate threshold
	// (depth ≥ watermark): the alert is visible at the same moment as the
	// publication suspension.
	AlertQueueRecoverFactor = 0.9

	// AlertNVMeWearPercent — the percentage_used that fires the wear alert:
	// 90% — ≤10% of the rated lifespan left, the threshold for planning
	// a replacement. Lifespan is monotonic (does not recover) — hysteresis is
	// not needed; values >100% are valid (lifespan exhausted) and also alert.
	AlertNVMeWearPercent = 90

	// AlertBadHashesThreshold — the junk content_hash spike threshold
	// (T-099): new rows over the AlertBadHashesWindow window. Normally the result-writer
	// invariant (spec 6.1: 16 octets) always holds zero, but
	// single relics — rows planted before the ING-P2-4 fix and
	// picked off by cleanup one by one — must not wake the operator:
	// 10 over the window is a confident writer error (T-056), not an echo of the past.
	AlertBadHashesThreshold = 10

	// AlertBadHashesWindow — the spike window in alert passes:
	// 15 × the alert period (5 min at the default, T-206) = 75 min
	// (the original "15 × 60 s = 15 min" — before T-161; on the 30 min cycle
	// of T-199 the window stretched to 7.5h). The clearing delay is the same
	// window: a one-off spike keeps the alert firing until eviction from
	// the ring. Cleanup passes run once a minute (DefaultCleanupInterval):
	// the window covers dozens of cleanup passes — a spike does not slip
	// between evaluations, and a short one (one or two junk rows) does not reach the
	// threshold; no hysteresis is needed against flapping — the window sum decreases
	// only by delta aging, and near the threshold it holds only while the
	// flow continues.
	AlertBadHashesWindow = 15

	// AlertDeadMinPasses — the number of alert passes with a non-zero q:dead depth
	// before queue_dead fires (T-154): 3 × the alert period (5 min at
	// the default, T-206) ≈ 15 min (the original "3 × 60 s ≈ 3 min" — before T-161;
	// on the 30 min cycle of T-199 the window stretched to ≈1.5h). "Persistently
	// non-zero", not "any death": the quarantine does not empty itself
	// (only operator triage, spec 4.2), and a single death with triage
	// alive is normal q:dead operation, not an incident; at the same time the window
	// is incommensurate with the incident scale (5507 deaths in an evening,
	// post-mortem §11.1) — the signal is late by at most one pass.
	AlertDeadMinPasses = 3

	// AlertRedisMemFireRatio — the used_memory/maxmemory share that fires
	// redis_memory (T-154): 80%. With noeviction (spec 3.2) filling
	// maxmemory is an XADD write refusal; the 20% headroom — the between-trims remainders
	// of TrimLoop (the confirmed prefix before the trim), fragmentation and client
	// buffers growing faster than the trim (the incident of Oct 22: 620 MiB of data
	// at a cap of 900 MiB already refused publications).
	AlertRedisMemFireRatio = 0.80

	// AlertRedisMemRecoverRatio — clearing redis_memory below 75%:
	// a 5 pp hysteresis — memory creeps up (remainders/fragmentation) and drops
	// abruptly (XTRIM/Drain); a share in the 75–80% corridor would not flap the Warn
	// at the threshold.
	AlertRedisMemRecoverRatio = 0.75

	// DefaultDiskFreeMinBytes — the absolute minimum of free space
	// of the content directory (T-162): 15 GiB Warn. The percentage threshold
	// content.disk_free_percent is late on a 79 GB volume: 10% = 8 GB ≈ 5h
	// until full at a write rate of ≈36 GiB/day (measurement §12) — a minimum
	// in bytes gives the operator headroom for triage (freeing space
	// by compaction takes hours, docs/disks.md).
	DefaultDiskFreeMinBytes = int64(15 << 30)

	// DefaultDiskFreeCritBytes — the escalation of the absolute minimum (T-162):
	// 10 GiB — a separate disk_free_crit alert; at a rate of 36 GiB/day this is
	// ≈6.5h until full — there is still time, but action is needed now.
	DefaultDiskFreeCritBytes = int64(10 << 30)

	// DefaultQueueFreezeWindow — the immobility window of the last-delivered-id
	// of a stream group at depth > 0 (T-162): 15 minutes. The poison of T-159 kills
	// the consumer on the very first entry — active triage (a live group)
	// moves last-delivered-id every few seconds; 15 min without movement at
	// non-zero depth — there is no consumer; shorter — false firings
	// on the pauses between batches; longer — half of the step
	// "restart the consumer and look at the logs" is lost.
	DefaultQueueFreezeWindow = 15 * time.Minute

	// DefaultPSICPUPercent — the PSI CPU some avg300 threshold (T-162): 90%.
	// Measurement §12: working scheduler starvation is some 91–95%; below 90%
	// — utilization without a queue (normal for a CPU-bound pipeline).
	DefaultPSICPUPercent = 90.0

	// DefaultPSIWindow — the PSI persistence window (T-162): 30 min. avg300
	// itself smooths over 5 minutes; the window demands sustained starvation — one-off
	// spikes (a deploy, triage of retro batches) do not count as an alert.
	// The discretization of the evaluation is the alert loop period (monitor.
	// alerts_interval, 5 min at the default — T-206 restored the frequency,
	// error ~5 min; on the 30 min cycle of T-199 a window ≤ the period
	// degenerated into "avg300 above the threshold on two adjacent passes" —
	// only starvation of ≳60 min was reliably caught).
	DefaultPSIWindow = 30 * time.Minute

	// DefaultMemHighMinPasses — alert passes in a row with growth of the
	// memory.events high counter (T-162): 2 × the alert period (5 min at the default,
	// T-206) = 10 min of sustained throttling (on the 30 min cycle of T-199
	// it stretched to 1h). A single reclaim spike is normal operation
	// of MemoryHigh (850M at http-worker, T-145): a soft cap is supposed
	// to press down peaks; sustained growth is the harbinger of the T-157 fatals.
	DefaultMemHighMinPasses = 2

	// DefaultRenderDebtDomains — the render debt threshold that fires the alert
	// on growth (T-175): 50k domains. The reference point — the actual debt
	// of the Oct 24 freeze (443k flags + the q:render lag, found by manual SQL)
	// is an order of magnitude above the threshold, and the healthy background is thousands
	// (local overflow during an SPA wave is extinguished by a finishing pass in minutes):
	// 50k is a day of drain stagnation at the wave inflow rate
	// (~0.6 domains/s), confident accumulation, not background noise. Above the threshold without
	// growth the alert does not fire — stagnation is visible in the debt row of the load
	// report (T-173); the alert is about accumulation.
	DefaultRenderDebtDomains = 50000

	// DefaultRenderDebtPasses — alert passes in a row with debt growth
	// before render_debt fires (T-175): 2 × the alert period (5 min at
	// the default, T-206) = 10 min of continuous growth (modeled on
	// mem_high_passes; on the 30 min cycle of T-199 it stretched to 1h).
	// A one-off jump (an SPA wave against a frozen drain for a minute) —
	// normal overflow with flags; a finishing pass resolves it itself;
	// sustained growth — the overflow has become stationary; it is time
	// for the operator to act (render contexts, scheduler inflow).
	DefaultRenderDebtPasses = 2

	// AlertRenderFallbackPasses — alert passes in a row before
	// render_fallback_depth fires (T-246): 2 × the alert period (5 min at
	// the default, T-206) = 10 min (modeled on render_debt_passes/
	// mem_high_passes). The threshold by itself does not wake the operator: a one-off
	// publication spike (an engine degradation episode resolved by the
	// health gate) is normal fallback operation of "collect and finish"
	// (FR-13); worker B rakes it away at its rate; a series of passes is either
	// continuous growth (the worker rate is insufficient or B is stopped), or
	// a stationary depth above the threshold (the second trigger, S-P2-1:
	// a stopped B, pinned at the stream's MAXLEN limit — DefaultStreamMaxLen:
	// q:render-fallback — 15,000 — or an episode that survived a restart
	// of the monitor) — accumulation approaching the MAXLEN limit; it is time
	// for the operator to raise the rate cap/change engine B. A constant, not a config
	// field: the window is mechanically tied to the render_debt series, and the level
	// threshold is already the configurable scheduler.queue_high_watermark.
	AlertRenderFallbackPasses = 2

	// AlertFallbackBudgetWindow — the window of fallback share budget suspension
	// events in alert passes (T-246): 6 × the alert period (5 min at
	// the default, T-206) = 30 min. The budget window of T-239 is 1000
	// confirmed deliveries of the stage (≈17 min at a render capacity of ~60/min,
	// measurement §12): the alert window fits an episode and one recurrence of the next
	// window; clearing is by aging (events are latchings, not state:
	// while the suspension holds there are no new points — only the
	// window keeps the alert; the operator sees the fallback rate on the engines dashboard). An event is
	// always an anomaly (normally publications fit in the share cap),
	// the threshold is a single event, the NVMe wear style.
	AlertFallbackBudgetWindow = 6

	// AlertRenderBrowserErrorsPasses — alert passes in a row with growth of
	// the render stage browser-error counter before
	// render_browser_errors fires (S-P2-6, the operator's decision — a rate detector):
	// 2 × the alert period (5 min at the default, T-206) = 10 min (modeled on
	// mem_high/render_debt). A single browser error is normal operation of the
	// render core (a session/context restart, T-063) and an episode resolved by the
	// health gate; a growth series — the engine (moli/lightpanda) is hung or
	// unavailable: the gate of T-239 has already suspended fallback publications,
	// the browser class waits in the q:render redelivery — the operator needs a signal
	// about the engine itself. The gate event remains the
	// suspended{reason="gate"} counter and the worker's Warn log: the gate is a streak threshold
	// in the worker, the alert is observability by monitor means.
	AlertRenderBrowserErrorsPasses = 2

	// AlertNewDoneStalePasses — NEW reconciliation snapshots in a row with
	// a non-zero new_done_stale row before domains_new_done_stale fires
	// (T-256, FR-2.2): 2 × the reconciliation period (30 min at the default,
	// T-199) ≈ 1h of sustained non-zero value. Alert passes
	// (5 min, T-206) are more frequent than source passes — the series grows only on
	// a change of the snapshot's updated_at; repeated reads of one row do not count
	// as an observation: a single boundary snapshot of the day boundary (midnight
	// aged yesterday's in-flight episodes of a "new" check — the DNS outcome
	// yesterday, HTTP in flight on the aggregate scan) is overwritten
	// by zero by the next pass and does not produce an alert (T-256 review, P2-1).
	// Stationary zero: the norm does not wake anyone at all.
	AlertNewDoneStalePasses = 2

	// AlertRedisRestartWindow — the "recent restart" window of Redis (T-270):
	// uptime_in_seconds below the window — the redis_restart alert. 15 min =
	// 3 alert passes (5 min, T-206): a restart between passes does not
	// slip through; after leaving the window the alert clears itself (uptime
	// grows monotonically — a level, no machine memory needed; a "no gap"
	// threshold like NVMe wear). A restart without
	// persistence (docs/redis.md §5) loses ALL undelivered
	// stream entries: q:dns self-heals via the T-029 lease; lost q:http
	// with done written — the T-270 compensator; the signal to the operator — at
	// the moment of the event (expect new_done_stale growth and a spike of
	// crawler_scheduler_stale_requeued_total), not as a cohort
	// a day later. A scheduled restart alerts the same way: the losses of undelivered
	// data do not depend on intentions.
	AlertRedisRestartWindow = 15 * time.Minute
)

// dnsDailyCountersSQL — the numerator and denominator of the temp_error share of the
// current day via TWO point reads of the daily crawler_counters rows (T-182,
// TZ_slow_dns_share §7): temp_error:YYYY-MM-DD — events of result
// writes with the temp_error verdict (incremented by httpworker/record.go,
// T-182), checks:YYYY-MM-DD — events of result writes (T-179);
// the authority of both rows is the monitor reconciliation pass (FILTER columns
// last_checked_at = current_date, understated between passes by DNS-only
// completions — the share is stable; the pass fixes the absolutes). The day name
// is computed by the DB (current_date — the same timezone as the now() of the
// last_checked_at write and the reconciliation window); coalesce OUTSIDE the subqueries
// (the throughputSQL style of T-179): before the first write of the day the subquery is empty —
// an honest zero. The former live aggregate over the day window (dnsShareSQL,
// count(*) FILTER … WHERE last_checked_at >= current_date) is removed:
// the Index Scan of the window had to visit the heap of every row for the status
// (status cannot be in the index — the HOT invariant, docs/schema.md),
// slow-query log entry #6 of the production day of Oct 24 (33 firings >3 s, median
// 27.7 s, max 110.7 s — ≈1.3 GB of reads per evaluation).
const dnsDailyCountersSQL = "SELECT " +
	"coalesce((SELECT value FROM crawler_counters " +
	"WHERE name = 'temp_error:' || to_char(current_date, 'YYYY-MM-DD')), 0), " +
	"coalesce((SELECT value FROM crawler_counters " +
	"WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')), 0)"

// newDoneStaleSQL — a point PK read of the new_done_stale row (T-256,
// FR-2.2): "status hung" domains (status = new ∧ check_state = done ∧
// last_checked_at older than a day) — the value is written by the FILTER column
// f_new_done_stale of the reconciliation pass (zonestats.go); the alert, at the cost
// of one lookup, observes it between passes (the window depth —
// zone_stats_interval); a seq scan over domains adds nothing. updated_at —
// the snapshot stamp: alert passes are more frequent than source passes (5 min vs
// 30), the alert series grows only on a NEW snapshot — two reads of one
// row are not "two observations" (T-256 review, P2-1). An empty result —
// the row does not exist yet: it is created by the first reconciliation pass (both monitor
// loops start together; unlike render_pending, which is created
// by a migration, a correct "no row" state of new_done_stale does not exist — either
// a start race or a manual edit of the table) — a quiet "not measured"
// without a Warn, not a false zero and not an alarm.
const newDoneStaleSQL = "SELECT value, updated_at FROM crawler_counters WHERE name = 'new_done_stale'"

// zoneFreshSQL — the freshness of progress: the last moment of the zone recount
// (zones.updated_at is written by ZoneStatsLoop, T-078). The zone reference table is small;
// max over an empty set is NULL.
const zoneFreshSQL = "SELECT max(updated_at) FROM zones"

// --------------------------------------------------------------- disk space

// SpaceStat — a snapshot of the file system space of the directory, bytes.
type SpaceStat struct {
	// Avail — available to processes without the root reserve (statfs.Bavail):
	// crawler services run unprivileged (spec 10.1).
	Avail uint64
	// Total — the volume capacity (statfs.Blocks).
	Total uint64
}

// SpaceChecker — a source of file system space information; factored out
// as an interface so unit tests could substitute statfs without a real volume.
// The internal/content/disk.go approach (T-046) via an owned copy: monitor does
// not pull the storage package (PackStore, zstd) for the sake of one syscall;
// the space check is an independent view of monitoring (spec 5.9), parallel to
// the DiskWatchLoop of the content writer.
type SpaceChecker interface {
	// Space returns a space snapshot of the dir directory.
	Space(dir string) (SpaceStat, error)
}

// OSStatfs — a SpaceChecker on top of syscall.Statfs.
type OSStatfs struct{}

// compile-time: OSStatfs is a SpaceChecker.
var _ SpaceChecker = OSStatfs{}

// Space takes a statfs of the dir directory.
func (OSStatfs) Space(dir string) (SpaceStat, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return SpaceStat{}, err
	}
	bs := uint64(st.Bsize)
	return SpaceStat{
		Avail: st.Bavail * bs,
		Total: st.Blocks * bs,
	}, nil
}

// ------------------------------------------------------------------ NVMe wear

// NVMeWear — the wear of one NVMe device per the health log (spec 10.3).
type NVMeWear struct {
	// Device — the controller name (nvme0), the base name in sysfs.
	Device string
	// PercentUsed — the percentage_used of the health log: 0..100; values >100
	// are valid — the rated lifespan is exhausted (NVMe 1.x).
	PercentUsed int
	// DataUnitsWritten — written data units (one NVMe unit = 512,000
	// bytes): TB ≈ units × 5.12e-7; spec 10.3 mentions TBW — computed
	// from the same page.
	DataUnitsWritten uint64
}

// WearSource — a source of NVMe wear data; an interface for substitution in
// unit tests (the real read is an ioctl; there is no hardware in tests).
type WearSource interface {
	// Wear returns the wear of all NVMe devices. An error = "not
	// measured" with a reason (a container without /dev/nvme*, an empty sysfs):
	// the alert degrades instead of firing falsely.
	Wear(ctx context.Context) ([]NVMeWear, error)
}

// OSWear — a WearSource on top of sysfs + the NVMe admin command ioctl: devices
// are discovered in sysfs (available in a container too — but /dev/nvme*
// itself is not passed through), the wear is read from the health log (log page 0x02) via
// NVME_IOCTL_ADMIN_CMD — without nvme-cli and without new dependencies.
type OSWear struct {
	// SysFS — the sysfs root ("" — "/sys"); tests substitute a directory.
	SysFS string
	// DevFS — the devfs root ("" — "/dev"); tests substitute a directory.
	DevFS string
}

// compile-time: OSWear is a WearSource.
var _ WearSource = OSWear{}

// Wear walks the NVMe controllers of sysfs and reads the health log of each.
// No devices at all or no accessible /dev node — a degradation
// error (in a Docker container this is the norm: sysfs sees the host
// devices, /dev/nvme0 is not passed through — it will not be opened).
func (w OSWear) Wear(ctx context.Context) ([]NVMeWear, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sysfs := w.SysFS
	// The default "/sys" lives only on the production host: unit tests always set
	// SysFS to a directory, and reading the real /sys is not hermetic (the set
	// of devices depends on the hardware).
	// mutator-disable-next-line branch/if
	if sysfs == "" {
		// mutator-disable-next-line statement/remove
		sysfs = "/sys"
	}
	devfs := w.DevFS
	// The default "/dev" — as with sysfs above.
	// mutator-disable-next-line branch/if
	if devfs == "" {
		// mutator-disable-next-line statement/remove
		devfs = "/dev"
	}
	names, err := filepath.Glob(filepath.Join(sysfs, "class", "nvme", "nvme*"))
	// Glob with a fixed valid pattern returns no error (only
	// a malformed pattern) — the branch is unreachable, cannot be killed by a test.
	// mutator-disable-next-line branch/if
	if err != nil {
		return nil, fmt.Errorf("NVMe: %s scan: %w", sysfs, err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no NVMe devices found (%s)",
			filepath.Join(sysfs, "class", "nvme"))
	}
	var wears []NVMeWear
	var errs []string
	for _, name := range names {
		dev := filepath.Base(name)
		percent, units, err := wearOf(filepath.Join(devfs, dev))
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", dev, err))
			continue
		}
		wears = append(wears, NVMeWear{
			Device:           dev,
			PercentUsed:      percent,
			DataUnitsWritten: units,
		})
	}
	if len(wears) == 0 {
		return nil, fmt.Errorf("SMART unavailable: %s", strings.Join(errs, "; "))
	}
	return wears, nil
}

// NVMe constants (uapi/linux/nvme.h, uapi/linux/nvme_ioctl.h).
const (
	// nvmeIoctlAdminCmd — _IOWR('N', 0x41, struct nvme_admin_cmd):
	// dir=3<<30 | size=72<<16 | type='N'<<8 | nr=0x41.
	nvmeIoctlAdminCmd = 0xc0484e41
	// nvmeAdminCmdSize — sizeof(struct nvme_admin_cmd) on 64-bit.
	nvmeAdminCmdSize = 72
	// nvmeAdminGetLogPage — the "get log page" admin opcode.
	nvmeAdminGetLogPage = 0x02
	// nvmeLogHealth — the health information page identifier.
	nvmeLogHealth = 0x02
	// nvmeHealthLogSize — the size of the health information page, bytes.
	nvmeHealthLogSize = 512
	// nvmeNsidNone — the controller scope (host/any namespace).
	nvmeNsidNone = 0xFFFFFFFF
)

// nvmeAdminCmd — struct nvme_admin_cmd (uapi/linux/nvme_ioctl.h): the layout
// is fixed by the kernel; the field order and the Go struct alignment must
// match (the size check is compile-time below). 72 bytes on 64-bit.
type nvmeAdminCmd struct {
	opcode      uint8
	flags       uint8
	rsvd1       uint16
	nsid        uint32
	cdw2, cdw3  uint32
	metadata    uint64
	addr        uint64
	metadataLen uint32
	dataLen     uint32
	cdw10       uint32
	cdw11       uint32
	cdw12       uint32
	cdw13       uint32
	cdw14       uint32
	cdw15       uint32
	timeoutMs   uint32
	result      uint32
}

// compile-time: the struct size agrees with the ioctl number constant
// (the number encodes size=72). The [2]struct{}{} mutant also compiles and
// is not observable by behavior — a compile-time check.
// mutator-disable-next-line numbers/incrementer
var _ = [1]struct{}{}[unsafe.Sizeof(nvmeAdminCmd{})-nvmeAdminCmdSize]

// wearOf reads the wear of one device: opens the controller char node
// /dev/nvmeX (O_RDWR — the ioctl admin command requires write access) and takes the
// health page in one NVME_IOCTL_ADMIN_CMD. Decoding — parseHealthLog.
//
// mutator-disable-func — hardware passthrough: the ioctl to /dev/nvme*
// runs only on the production host; in the CI container the device is not
// passed through (unit tests only reach the open failure, and the
// return values on error are ignored by the single caller).
func wearOf(devPath string) (percent int, unitsWritten uint64, err error) {
	// O_RDWR: the ioctl of admin commands requires write access to the node —
	// a container without the passed-through device gets EPERM/ENOENT (degradation).
	f, err := os.OpenFile(devPath, os.O_RDWR, 0)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()

	// The page buffer — in a wrapper with a zero-size uint64 field: a byte array by
	// itself is not aligned, but the struct type inherits the field's alignment (8) —
	// the data address goes to the driver aligned.
	var pageBuf struct {
		_    [0]uint64
		data [nvmeHealthLogSize]byte
	}
	cmd := nvmeAdminCmd{
		opcode:  nvmeAdminGetLogPage,
		nsid:    nvmeNsidNone,
		addr:    uint64(uintptr(unsafe.Pointer(&pageBuf.data[0]))),
		dataLen: nvmeHealthLogSize,
		// cdw10: NUMD (the number of double words − 1) in the high bits, LID —
		// in the low ones.
		cdw10: uint32(nvmeHealthLogSize/4-1)<<16 | nvmeLogHealth,
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(),
		nvmeIoctlAdminCmd, uintptr(unsafe.Pointer(&cmd)))
	// The buffer address went into the ioctl as an integer — the GC does not see it; KeepAlive
	// keeps the buffer alive until the syscall returns (the unsafe.Pointer rules).
	runtime.KeepAlive(&pageBuf)
	if errno != 0 {
		return 0, 0, fmt.Errorf("ioctl NVME_ADMIN_GET_LOG_PAGE: %w", errno)
	}
	return parseHealthLog(pageBuf.data[:])
}

// parseHealthLog decodes the health information page (the layout —
// struct nvme_smart_log from uapi/linux/nvme.h): critical_warning — byte 0,
// composite temperature — 1–2, available spare — 3, spare threshold — 4,
// percentage_used — byte 5 (values >100 are valid — lifespan exhausted);
// data units written — bytes 48–63, 128-bit little-endian, the high
// half is 0 in practice. Factored out as a pure function for a table test
// of offsets without hardware.
func parseHealthLog(page []byte) (percent int, unitsWritten uint64, err error) {
	if len(page) < nvmeHealthLogSize {
		return 0, 0, fmt.Errorf("NVMe: the health page is shorter than %d bytes: %d", nvmeHealthLogSize, len(page))
	}
	// Equivalent to only the slice-end mutant page[48:57]: Uint64 reads
	// the first 8 bytes, the ninth has no effect. The annotation applies to the whole
	// line and also suppresses the killable mutants page[5]→[6] and
	// page[49:56] — both remain covered by the assertions
	// of TestParseHealthLog; a point tool (a checksum blacklist) was rejected
	// as fragile (docs/mutation-testing.md §4).
	// mutator-disable-next-line numbers/incrementer
	return int(page[5]), binary.LittleEndian.Uint64(page[48:56]), nil
}

// ------------------------------------------------------------------- alerts

// StreamLener — stream depths; *queue.BackpressureGate (T-026)
// satisfies the interface — the same narrow API over go-redis as the
// scheduler and crawlerctl status use (T-079). A separate interface, not
// go-redis: tests substitute a fake, the transport is replaceable (spec 4.2).
type StreamLener interface {
	// Len returns the stream depth: the unconsumed
	// remainder plus unacknowledged deliveries (the backlog, see
	// BackpressureGate.Len).
	Len(ctx context.Context, stream string) (int64, error)
}

// BadHashesSource — the junk content_hash counter of ingest cleanup
// (crawler_ingest_bad_hashes_total, T-099); the IngestBadHashes implementation
// reads it from the /metrics exposition of the ingest service; unit tests substitute
// a fake. A separate interface in the StreamLener/SpaceChecker style: the source
// is replaceable, the alert does not depend on HTTP.
type BadHashesSource interface {
	// BadHashes returns the current counter value (it grows monotonically
	// with the results of CleanupDeleted passes; an ingest restart resets it).
	BadHashes(ctx context.Context) (int64, error)
}

// FallbackBudgetSource — the counter of fallback suspensions by the share
// budget (crawler_render_fallback_suspended_total{reason="budget"}, T-239);
// the RenderSuspends implementation (suspends.go) reads it from the
// /metrics exposition of the render worker; unit tests substitute a fake. The
// BadHashesSource style: the source is replaceable, the alert does not depend on HTTP.
type FallbackBudgetSource interface {
	// FallbackBudget returns the current counter value (it grows monotonically
	// with suspension latchings; a worker restart resets it).
	FallbackBudget(ctx context.Context) (int64, error)
}

// RenderBrowserErrorsSource — the counter of browser-class errors of the
// render stage (crawler_stage_errors_total{stage="render",type="browser"},
// S-P2-6); the RenderSuspends implementation (suspends.go) reads it from the same
// /metrics exposition of the render worker (the address monitor.render_metrics_url),
// unit tests substitute a fake. The FallbackBudgetSource style: the source
// is replaceable, the alert does not depend on HTTP.
type RenderBrowserErrorsSource interface {
	// RenderBrowserErrors returns the current counter value (it grows monotonically
	// with engine infra errors; a worker restart resets it). The error
	// errNotRenderExposition — "the address is not configured to a render worker".
	RenderBrowserErrors(ctx context.Context) (int64, error)
}

// alertStreams — the checked streams: the working ones, capped by the
// backpressure gate (q:dead is not capped — see the backpressure.go header).
var alertStreams = []string{
	queue.StreamDNS, queue.StreamHTTP, queue.StreamRender, queue.StreamAnalyze,
}

// AlertsDeps — the alert data sources. Every source may degrade
// ("not measured" with a reason), except the DB pool: a monitor without a DB is pointless
// (the loop ends with an error).
type AlertsDeps struct {
	// Pool — the DB: the temp_error share of the day, zones.updated_at.
	Pool db.DB
	// Redis — the stream lengths; nil — the queue alert is "not measured".
	Redis StreamLener
	// Space — the content directory space; nil — OSStatfs.
	Space SpaceChecker
	// Wear — the NVMe wear; nil — OSWear (sysfs/ioctl).
	Wear WearSource
	// BadHashes — the junk content_hash counter of ingest cleanup (T-099);
	// nil — the alert is "not measured".
	BadHashes BadHashesSource
	// FallbackBudget — the counter of fallback share budget suspension
	// events (T-246); nil — the alert is "not measured". The source is driven by
	// cmd/monitor (RenderSuspends by monitor.render_metrics_url).
	FallbackBudget FallbackBudgetSource
	// RenderBrowserErrors — the counter of browser-class errors of the render stage
	// (S-P2-6); nil — the alert is "not measured". The source is the same
	// RenderSuspends probe by monitor.render_metrics_url (cmd/monitor).
	RenderBrowserErrors RenderBrowserErrorsSource
	// UnitEvents — OOM/restarts of crawler units (T-145); nil — the alert
	// is "not measured". There is no default OS implementation (unlike
	// Space/Wear): OSUnitEvents execs systemctl and reads the host
	// cgroupfs — unacceptable in the package's unit tests; the source is
	// driven by cmd/monitor (OSUnitEvents, wrapped in the metrics
	// NewUnitEventsMetrics).
	UnitEvents UnitEventsSource
	// RedisMemory — used_memory/maxmemory from INFO memory (T-154);
	// nil — the redis_memory alert is "not measured". The source is driven by
	// cmd/monitor (RedisInfo of the same client as the depth gate,
	// wrapped in the metrics NewRedisMemoryMetrics).
	RedisMemory RedisMemorySource
	// RedisUptime — uptime_in_seconds from INFO server (T-270); nil —
	// the redis_restart alert is "not measured". The source is the same Redis
	// client as for memory (cmd/monitor: RedisInfo).
	RedisUptime RedisUptimeSource
	// StreamGroups — the groups of working streams for the frozen queue
	// alert (T-162); nil — the alert is "not measured". The source is driven by
	// cmd/monitor (RedisStreamGroups of the same client as the gate).
	StreamGroups StreamGroupsSource
	// PSI — the CPU pressure of /proc/pressure/cpu (T-162); nil — OSPSI
	// (a pure file read, like OSStatfs/OSWear).
	PSI PSISource
}

// AlertsOptions — evaluation parameters; zero values are replaced
// by defaults (withDefaults, the ZoneStatsOptions style).
type AlertsOptions struct {
	// Interval — the AlertsLoop cycle period (DefaultAlertsInterval);
	// the working value is monitor.alerts_interval (T-206): the detection
	// windows in passes (AlertDeadMinPasses, MemHighMinPasses,
	// RenderDebtPasses, AlertBadHashesWindow) are counted from it.
	Interval time.Duration
	// ZoneStatsInterval — the ZoneStatsLoop period: the unit of the stall
	// threshold (AlertStallFactor × the interval; updated_at is written by the
	// reconciliation loop — the threshold must count its misses, not alert
	// passes); 0 — DefaultZoneStatsInterval. The working value is
	// monitor.zone_stats_interval.
	ZoneStatsInterval time.Duration
	// QueueWatermark — the stream depth threshold
	// (scheduler.queue_high_watermark); ≤ 0 — the queue alert is "not
	// measured" (config validation requires ≥ 1). The effective
	// stream threshold is capped at half of its MAXLEN limit — like the
	// backpressure gate (queue.StreamWatermark, T-026).
	QueueWatermark int64
	// QueueMaxLen — the MAXLEN limit of stream lengths (mirrors redis.maxlen;
	// 0 — the per-stream limits of the T-025 policy): needed for the effective
	// stream threshold (see QueueWatermark).
	QueueMaxLen int64
	// ContentDir — the content directory (content.dir); "" — the space alert is "not
	// measured".
	ContentDir string
	// DiskFreePercent — the free space threshold
	// (content.disk_free_percent); ≤ 0 — the check is off, the alert is "not
	// measured".
	DiskFreePercent float64
	// DiskFreeMinBytes — the absolute minimum of free space of the content
	// directory, Warn (monitor.alerts.disk_free_min_bytes, T-162);
	// ≤ 0 — DefaultDiskFreeMinBytes (no off switch: the absolute minimum
	// is the safety net of the percentage threshold; there is no reason to disable it).
	DiskFreeMinBytes int64
	// DiskFreeCritBytes — the escalation of the absolute minimum, a separate
	// alert (monitor.alerts.disk_free_crit_bytes, T-162); ≤ 0 —
	// DefaultDiskFreeCritBytes.
	DiskFreeCritBytes int64
	// QueueFreeze — the immobility window of the group's last-delivered-id at
	// depth > 0 (monitor.alerts.queue_freeze_window, T-162);
	// ≤ 0 — DefaultQueueFreezeWindow.
	QueueFreeze time.Duration
	// PSICPUPercent — the PSI CPU some avg300 threshold, %
	// (monitor.alerts.psi_cpu_percent, T-162); ≤ 0 — DefaultPSICPUPercent.
	PSICPUPercent float64
	// PSIWindow — the PSI persistence window above the threshold
	// (monitor.alerts.psi_cpu_window, T-162); ≤ 0 — DefaultPSIWindow.
	PSIWindow time.Duration
	// MemHighMinPasses — passes in a row with growth of the
	// memory.events high counter before memory_high fires
	// (monitor.alerts.mem_high_passes, T-162); ≤ 0 — DefaultMemHighMinPasses.
	MemHighMinPasses int
	// RenderDebtDomains — the render debt threshold (flags + the q:render lag)
	// that fires the alert on growth (monitor.alerts.render_debt_domains,
	// T-175); ≤ 0 — DefaultRenderDebtDomains.
	RenderDebtDomains int64
	// RenderDebtPasses — passes in a row with debt growth before
	// render_debt fires (monitor.alerts.render_debt_passes, T-175); ≤ 0 —
	// DefaultRenderDebtPasses.
	RenderDebtPasses int
	// Now — the time source (the age of zones.updated_at); nil — time.Now.
	Now func() time.Time
	// Log — the loop logger; nil — entries suppressed (slog.DiscardHandler).
	Log *slog.Logger
	// OnPass — the state export hook: a snapshot of every pass (log +
	// field; see the file header). Called in the pass loop — it must be
	// fast and must not panic.
	OnPass func(AlertsResult)
}

// withDefaults replaces non-positive periods and thresholds with defaults (Now/Log
// are checked separately: functions/pointers — nil).
func (o AlertsOptions) withDefaults() AlertsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultAlertsInterval
	}
	if o.ZoneStatsInterval <= 0 {
		o.ZoneStatsInterval = DefaultZoneStatsInterval
	}
	if o.DiskFreeMinBytes <= 0 {
		o.DiskFreeMinBytes = DefaultDiskFreeMinBytes
	}
	if o.DiskFreeCritBytes <= 0 {
		o.DiskFreeCritBytes = DefaultDiskFreeCritBytes
	}
	if o.QueueFreeze <= 0 {
		o.QueueFreeze = DefaultQueueFreezeWindow
	}
	if o.PSICPUPercent <= 0 {
		o.PSICPUPercent = DefaultPSICPUPercent
	}
	if o.PSIWindow <= 0 {
		o.PSIWindow = DefaultPSIWindow
	}
	if o.MemHighMinPasses <= 0 {
		o.MemHighMinPasses = DefaultMemHighMinPasses
	}
	if o.RenderDebtDomains <= 0 {
		o.RenderDebtDomains = DefaultRenderDebtDomains
	}
	if o.RenderDebtPasses <= 0 {
		o.RenderDebtPasses = DefaultRenderDebtPasses
	}
	return o
}

// AlertSnapshot — the state of one alert after a pass.
type AlertSnapshot struct {
	// Name — the alert name (the Alert* constants).
	Name string
	// Status — AlertStatusOK | AlertStatusFiring | AlertStatusNotMeasured.
	Status string
	// Detail — the measurement or the "not measured" reason.
	Detail string
}

// AlertsResult — a snapshot of the evaluation pass: one AlertSnapshot per alert,
// the order is fixed (NewAlerts).
type AlertsResult struct {
	Alerts []AlertSnapshot
}

// Firing — the fired alerts of the pass.
func (r AlertsResult) Firing() []AlertSnapshot {
	var firing []AlertSnapshot
	for _, a := range r.Alerts {
		if a.Status == AlertStatusFiring {
			firing = append(firing, a)
		}
	}
	return firing
}

// Alerts — alert evaluation with state memory between passes
// (hysteresis, front-loaded log entries only on transitions). Each alert
// is an alertMachine (T-264, TZ-fsm spec §3 FR-10): "{name, status,
// track memory}" with the single state-writing point transition —
// the machine specification and the typed track memories are in alertstate.go;
// this file — the observation sources (observe*) and the machine assembly.
type Alerts struct {
	deps AlertsDeps
	opts AlertsOptions
	log  *slog.Logger
	now  func() time.Time
	// machines — the alert machines in a fixed order (invariant I4
	// of alertstate.go): the AlertsResult snapshot is deterministic, the
	// order of Redis reads of the sources is stable.
	machines []alertRunner
}

// NewAlerts builds the evaluator: normalizes the options (period defaults),
// the Space/Wear/PSI sources (nil → OS implementations) and assembles
// the alert machines (the newXMachine constructors — next to the
// observe functions of the sources; the sources and thresholds — the file header). The machine order —
// the former evaluators order (T-080…T-256): the queue of reads of the shared
// sources depends on it (the test fakes position errors by the read
// number of Len).
func NewAlerts(deps AlertsDeps, opts AlertsOptions) *Alerts {
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	if deps.Space == nil {
		deps.Space = OSStatfs{}
	}
	if deps.Wear == nil {
		deps.Wear = OSWear{}
	}
	if deps.PSI == nil {
		deps.PSI = OSPSI{}
	}
	a := &Alerts{deps: deps, opts: o, log: log, now: now}
	a.machines = []alertRunner{
		a.newDNSMachine(),
		a.newQueuesMachine(),
		a.newStallMachine(),
		a.newDiskMachine(),
		a.newDiskMinMachine(),
		a.newDiskCritMachine(),
		a.newWearMachine(),
		a.newBadHashesMachine(),
		a.newUnitOOMMachine(),
		a.newMemHighMachine(),
		a.newDeadMachine(),
		a.newQueueFreezeMachine(),
		a.newRedisMemoryMachine(),
		a.newPSIMachine(),
		a.newRenderDebtMachine(),
		a.newRenderFbDepthMachine(),
		a.newRenderFbBudgetMachine(),
		a.newRenderBrowserErrMachine(),
		a.newDomainsStaleMachine(),
		a.newRedisRestartMachine(),
	}
	return a
}

// mutator-disable-regexp a\.log\.Debug\("alert statement/remove
// mutator-disable-regexp a\.log\.Info\("alert statement/remove
// Justification: the service entries of the loop (the pass result Debug, the start Info) —
// they do not change alert behavior, damped per docs/mutation-testing.md §4.

// Pass performs one pass: runs all machines (a source degradation of one
// does not stop the others), logs the transitions,
// hands the snapshot to OnPass and returns it.
func (a *Alerts) Pass(ctx context.Context) AlertsResult {
	snaps := make([]AlertSnapshot, 0, len(a.machines))
	for _, m := range a.machines {
		snaps = append(snaps, m.run(ctx, a))
	}
	res := AlertsResult{Alerts: snaps}
	if a.opts.OnPass != nil {
		a.opts.OnPass(res)
	}
	a.log.Debug("alert pass", "alerts", len(snaps), "fired", len(res.Firing()))
	return res
}

// logFront writes a front entry on the state change (invariant I3
// of alertstate.go): firing — Warn, clearing — Info, inapplicability —
// Debug. The exception — a firing alert going to "not measured" (a source
// failure): the Warn of the error is written by transition; the loss of the alert itself
// is recorded with Info — otherwise the incident would vanish from the log without a single
// entry about the termination.
func (a *Alerts) logFront(name, prev, status, detail string) {
	if prev == status {
		return
	}
	desc := name
	if d, ok := alertDescs[name]; ok {
		desc = d
	}
	switch status {
	case AlertStatusFiring:
		a.log.Warn("ALERT: "+desc, "alert", name, "detail", detail)
	case AlertStatusOK:
		if prev == AlertStatusFiring {
			a.log.Info("alert cleared: "+desc, "alert", name, "detail", detail)
		}
	case AlertStatusNotMeasured:
		if prev == AlertStatusFiring {
			a.log.Info("firing alert no longer measured: "+desc, "alert", name, "reason", detail)
		} else {
			a.log.Debug("alert not measured: "+desc, "alert", name, "reason", detail)
		}
	}
}

// observeDNS — the temp_error share among the checks of the current day: the numerator
// and the denominator — the daily crawler_counters rows (T-182, point
// reads dnsDailyCountersSQL; result-write events — retries and
// delivery duplicates stay in the event history, the share is stable, the absolutes
// are fixed by reconciliation). Below 100 denominator events (AlertDNSMinSamples)
// the share is the noise of an empty window, a quiet "not measured"; the share hysteresis —
// the machine track (the T-080/T-162 thresholds were not changed since T-182).
func (a *Alerts) observeDNS(ctx context.Context) observation[dnsShare] {
	rows, err := a.deps.Pool.Query(ctx, dnsDailyCountersSQL)
	if err != nil {
		return observation[dnsShare]{err: fmt.Errorf("monitor: DNS error share: %w", err)}
	}
	defer rows.Close()
	if !rows.Next() {
		return observation[dnsShare]{err: errors.New("monitor: DNS error share: empty response")}
	}
	var temp, total int64
	if err := rows.Scan(&temp, &total); err != nil {
		return observation[dnsShare]{err: fmt.Errorf("monitor: DNS error share: scan: %w", err)}
	}
	if err := rows.Err(); err != nil {
		return observation[dnsShare]{err: fmt.Errorf("monitor: DNS error share: %w", err)}
	}
	if total < AlertDNSMinSamples {
		return observation[dnsShare]{
			reason: fmt.Sprintf("checks for the current day %d < %d — the share is not meaningful", total, AlertDNSMinSamples),
		}
	}
	return observation[dnsShare]{value: dnsShare{temp: temp, total: total}}
}

// newDNSMachine — the dns_temp_error_share machine: the hysteresis of the share thresholds
// (firing ≥ AlertDNSFireShare 30%, holding the fired state until
// AlertDNSRecoverShare 20% — the corridor does not flap the Warn).
func (a *Alerts) newDNSMachine() *alertMachine[dnsShare] {
	return &alertMachine[dnsShare]{
		name: AlertDNSErrors,
		track: hysteresisTrack[dnsShare]{
			fire: func(v dnsShare) bool { return v.share() >= AlertDNSFireShare },
			hold: func(v dnsShare) bool { return v.share() >= AlertDNSRecoverShare },
			okDetail: func(v dnsShare) string {
				return fmt.Sprintf("temp_error share %.1f%% (%d of %d checks for the current day)",
					100*v.share(), v.temp, v.total)
			},
		},
		eval: a.observeDNS,
	}
}

// observeQueues — the depth of the working streams (the observation source): Len of
// each working stream of alertStreams; the "gate threshold /
// 90% of the threshold" hysteresis — the machine track. q:dead is not polled by the queue alert
// (the quarantine depth is read by queue_dead).
func (a *Alerts) observeQueues(ctx context.Context) observation[map[string]int64] {
	if a.deps.Redis == nil {
		return observation[map[string]int64]{reason: "Redis not set"}
	}
	if a.opts.QueueWatermark <= 0 {
		return observation[map[string]int64]{
			reason: "scheduler.queue_high_watermark threshold not set",
		}
	}
	lens := make(map[string]int64, len(alertStreams))
	for _, s := range alertStreams {
		n, err := a.deps.Redis.Len(ctx, s)
		if err != nil {
			return observation[map[string]int64]{err: fmt.Errorf("monitor: %s depth: %w", s, err)}
		}
		lens[s] = n
	}
	return observation[map[string]int64]{value: lens}
}

// newQueuesMachine — the queue_overflow machine: the hysteresis of the working
// stream depths — firing at the effective stream threshold (at the same moment
// the gate closes publication), clearing below 90% of the threshold
// (AlertQueueRecoverFactor — a soft gate: between the check and publication
// other producers add entries, T-026). The threshold is per stream, like the
// gate's (T-026): for q:dns the configured 100,000 entries are capped
// at half of its MAXLEN by policy (DefaultStreamMaxLen, T-141:
// 125 batches).
func (a *Alerts) newQueuesMachine() *alertMachine[map[string]int64] {
	wm := make(map[string]int64, len(alertStreams))
	for _, s := range alertStreams {
		wm[s] = queue.StreamWatermark(a.opts.QueueWatermark, a.opts.QueueMaxLen, s)
	}
	holdAt := func(s string) int64 {
		return int64(float64(wm[s]) * AlertQueueRecoverFactor)
	}
	return &alertMachine[map[string]int64]{
		name: AlertQueueOverflow,
		track: hysteresisTrack[map[string]int64]{
			fire: func(lens map[string]int64) bool {
				for _, s := range alertStreams {
					if lens[s] >= wm[s] {
						return true
					}
				}
				return false
			},
			hold: func(lens map[string]int64) bool {
				for _, s := range alertStreams {
					if lens[s] >= holdAt(s) {
						return true
					}
				}
				return false
			},
			okDetail: func(lens map[string]int64) string {
				// The "ok" detail — the maximum depth among the working
				// streams; the starting value is the first stream: with all
				// zeros (idle queues) the detail names a stream,
				// not an empty string.
				maxStream, maxLen := alertStreams[0], lens[alertStreams[0]]
				// The equivalent mutant alertStreams[0:]: the first element
				// does not exceed itself (the maximum start), the result does
				// not change.
				// mutator-disable-next-line numbers/decrementer
				for _, s := range alertStreams[1:] {
					if lens[s] > maxLen {
						maxStream, maxLen = s, lens[s]
					}
				}
				return fmt.Sprintf("max %s=%d", maxStream, maxLen)
			},
			hotDetail: func(lens map[string]int64) string {
				// The fired detail names the hot streams; when there are none —
				// the ones holding the hysteresis corridor.
				var hot, hold []string
				for _, s := range alertStreams {
					n := lens[s]
					switch {
					case n >= wm[s]:
						hot = append(hot, fmt.Sprintf("%s=%d (threshold %d)", s, n, wm[s]))
					case n >= holdAt(s):
						hold = append(hold, fmt.Sprintf("%s=%d (threshold %d)", s, n, wm[s]))
					}
				}
				at := hot
				if len(at) == 0 {
					at = hold // hysteresis corridor: below the gate threshold, but close
				}
				return strings.Join(at, ", ") + " — depth at the threshold"
			},
		},
		eval: a.observeQueues,
	}
}

// observeStall — the freshness of the last zone recount: zones.updated_at
// (written by ZoneStatsLoop, T-078); NULL — no zones, progress is
// not tracked (a quiet "not measured"). The age threshold — the machine track;
// a self-check caveat: monitor writes updated_at itself — with the process
// fully dead there is nobody to fire the alert; the service loss is seen by the
// crawler_service_up metric of the framework (T-012).
func (a *Alerts) observeStall(ctx context.Context) observation[time.Time] {
	rows, err := a.deps.Pool.Query(ctx, zoneFreshSQL)
	if err != nil {
		return observation[time.Time]{err: fmt.Errorf("monitor: progress stall: %w", err)}
	}
	defer rows.Close()
	if !rows.Next() {
		return observation[time.Time]{err: errors.New("monitor: progress stall: empty response")}
	}
	var last sql.NullTime
	if err := rows.Scan(&last); err != nil {
		return observation[time.Time]{err: fmt.Errorf("monitor: progress stall: scan: %w", err)}
	}
	if err := rows.Err(); err != nil {
		return observation[time.Time]{err: fmt.Errorf("monitor: progress stall: %w", err)}
	}
	if !last.Valid {
		return observation[time.Time]{reason: "the zone reference table is empty — progress is not tracked"}
	}
	return observation[time.Time]{value: last.Time}
}

// newStallMachine — the progress_stall machine: the stamp age threshold in
// RECONCILIATION periods (AlertStallFactor × ZoneStatsInterval — the stamp
// is written by exactly that loop, not by alert passes: with 30 min
// reconciliation a threshold of 5 × 5 min would fire forever; a T-078 pass over
// tens of millions of rows takes minutes, a single missed period is not an incident,
// 5 periods is a confident stall). No hysteresis needed: the age
// decreases only with a new pass, there is no flapping at the threshold.
func (a *Alerts) newStallMachine() *alertMachine[time.Time] {
	stallAfter := time.Duration(AlertStallFactor) * a.opts.ZoneStatsInterval
	return &alertMachine[time.Time]{
		name: AlertProgressStall,
		track: thresholdTrack[time.Time]{
			fire: func(now, last time.Time) bool {
				return now.Sub(last) > stallAfter
			},
			okDetail: func(now, last time.Time) string {
				return fmt.Sprintf("the last recount %s ago (threshold %s)",
					now.Sub(last).Round(time.Second), stallAfter)
			},
			hotDetail: func(now, last time.Time) string {
				return fmt.Sprintf("zones.updated_at older than the threshold: %s (threshold %d × %s)",
					now.Sub(last).Round(time.Second), AlertStallFactor, a.opts.ZoneStatsInterval)
			},
		},
		eval: a.observeStall,
	}
}

// observeDisk — the observation of the percentage space alert: the content directory
// not set / the check off (content.disk_free_percent ≤ 0) — a quiet
// "not measured", not a false firing; next — the shared space
// snapshot observeDiskSpace.
func (a *Alerts) observeDisk(_ context.Context) observation[SpaceStat] {
	if a.opts.ContentDir == "" {
		return observation[SpaceStat]{reason: "content directory not set"}
	}
	if a.opts.DiskFreePercent <= 0 {
		return observation[SpaceStat]{
			reason: "space monitoring is off (content.disk_free_percent ≤ 0)",
		}
	}
	return a.observeDiskSpace()
}

// observeDiskSpace — the shared statfs snapshot of the content directory: the share
// of free space (the same formula as the Put pause in internal/content,
// T-046) and the absolute minimums read the same snapshot (two
// syscall copies per pass are cheaper than coupling the alerts with a shared cache). An error
// of statfs — a Warn; total=0 — the file system does not report capacity.
func (a *Alerts) observeDiskSpace() observation[SpaceStat] {
	sp, err := a.deps.Space.Space(a.opts.ContentDir)
	if err != nil {
		return observation[SpaceStat]{err: fmt.Errorf("monitor: free space %s: %w", a.opts.ContentDir, err)}
	}
	if sp.Total == 0 {
		return observation[SpaceStat]{reason: "the file system does not report capacity (total=0)"}
	}
	return observation[SpaceStat]{value: sp}
}

// observeDiskBytes — the observation of the absolute space minimums
// (disk_free_min/disk_free_crit, T-162): the directory not set — a quiet "not
// measured"; next — the shared snapshot observeDiskSpace.
func (a *Alerts) observeDiskBytes(_ context.Context) observation[SpaceStat] {
	if a.opts.ContentDir == "" {
		return observation[SpaceStat]{reason: "content directory not set"}
	}
	return a.observeDiskSpace()
}

// newDiskMachine — the disk_free machine: firing when the free share is
// strictly below the threshold — exactly when content Pause stops writing;
// a separate threshold would desync the signal from the real pause,
// hence no hysteresis gap.
func (a *Alerts) newDiskMachine() *alertMachine[SpaceStat] {
	pct := a.opts.DiskFreePercent
	return &alertMachine[SpaceStat]{
		name: AlertDiskFree,
		track: thresholdTrack[SpaceStat]{
			fire: func(_ time.Time, sp SpaceStat) bool {
				return 100*float64(sp.Avail)/float64(sp.Total) < pct
			},
			okDetail: func(_ time.Time, sp SpaceStat) string {
				free := 100 * float64(sp.Avail) / float64(sp.Total)
				return fmt.Sprintf("free %.1f%% (%s of %s), threshold %.1f%%",
					free, alertBytes(sp.Avail), alertBytes(sp.Total), pct)
			},
		},
		eval: a.observeDisk,
	}
}

// newDiskBytesMachine — the absolute space minimum machine (T-162):
// Warn at Avail < the threshold, the escalation — a separate alert, not
// a "level" of the first: every state is visible via its own firing/clearing
// message pair (the notify.go debouncing) and its own log line;
// the Warn→escalation hierarchy reads by name. Firing is strict (<),
// recovery by returning to the threshold; no hysteresis, like
// the percentage one: space decreases monotonically during an incident (~1.5 GB/h) —
// nothing moves the flapping at the threshold. A threshold ≥ the volume capacity is inapplicable:
// free space would never reach the minimum (a 10 GB dev volume against
// 15 GiB) — an eternal firing would be false; small volumes stay
// under the wing of the percentage threshold.
func (a *Alerts) newDiskBytesMachine(name string, threshold int64) *alertMachine[SpaceStat] {
	return &alertMachine[SpaceStat]{
		name: name,
		track: thresholdTrack[SpaceStat]{
			fire: func(_ time.Time, sp SpaceStat) bool {
				return int64(sp.Avail) < threshold
			},
			na: func(sp SpaceStat) string {
				if threshold >= int64(sp.Total) {
					return fmt.Sprintf("the minimum %s ≥ the volume capacity %s — the threshold is inapplicable, a small volume stays under the percentage one",
						alertBytes(uint64(threshold)), alertBytes(sp.Total))
				}
				return ""
			},
			okDetail: func(_ time.Time, sp SpaceStat) string {
				return fmt.Sprintf("free %s of %s, threshold %s",
					alertBytes(sp.Avail), alertBytes(sp.Total), alertBytes(uint64(threshold)))
			},
		},
		eval: a.observeDiskBytes,
	}
}

// newDiskMinMachine / newDiskCritMachine — the Warn minimum (15 GiB,
// DefaultDiskFreeMinBytes) and the escalation (10 GiB, DefaultDiskFreeCritBytes)
// of the absolute space reserve: a percentage threshold is late on a large volume
// (10% of 79 GB = 8 GB ≈ 5h until full at a rate of ≈36 GiB/day,
// measurement §12); a minimum in bytes gives triage headroom regardless of
// the volume size.
func (a *Alerts) newDiskMinMachine() *alertMachine[SpaceStat] {
	return a.newDiskBytesMachine(AlertDiskMin, a.opts.DiskFreeMinBytes)
}

func (a *Alerts) newDiskCritMachine() *alertMachine[SpaceStat] {
	return a.newDiskBytesMachine(AlertDiskCrit, a.opts.DiskFreeCritBytes)
}

// observeWear — the NVMe wear per the health log. SMART unavailability —
// an expected container state: a quiet "not measured" without a Warn —
// the degradation is honest (see the file header); on a production systemd host the layer
// is enabled by granting the unit access to /dev/nvme* (the nvme-smart group, T-089 —
// docs/disks.md). An empty list — the source is alive, no devices.
func (a *Alerts) observeWear(ctx context.Context) observation[[]NVMeWear] {
	wears, err := a.deps.Wear.Wear(ctx)
	if err != nil {
		return observation[[]NVMeWear]{reason: err.Error()}
	}
	if len(wears) == 0 {
		return observation[[]NVMeWear]{reason: "no NVMe devices found"}
	}
	return observation[[]NVMeWear]{value: wears}
}

// newWearMachine — the nvme_wear machine: the alert at percentage_used ≥
// AlertNVMeWearPercent (90% — ≤10% of the rated lifespan left, the threshold
// for planning a replacement) of any device; lifespan is monotonic (it
// does not recover) — hysteresis is not needed; values >100% are valid
// (lifespan exhausted) and also alert.
func (a *Alerts) newWearMachine() *alertMachine[[]NVMeWear] {
	// The worst by wear; the start is the first device, not the zero value:
	// a new drive (0%) would leave an empty name in the detail.
	worstOf := func(wears []NVMeWear) NVMeWear {
		worst := wears[0]
		// The equivalent mutant wears[0:]: the first element does not exceed
		// itself (the maximum start), the result does not change.
		// mutator-disable-next-line numbers/decrementer
		for _, w := range wears[1:] {
			if w.PercentUsed > worst.PercentUsed {
				worst = w
			}
		}
		return worst
	}
	return &alertMachine[[]NVMeWear]{
		name: AlertNVMeWear,
		track: thresholdTrack[[]NVMeWear]{
			fire: func(_ time.Time, wears []NVMeWear) bool {
				return worstOf(wears).PercentUsed >= AlertNVMeWearPercent
			},
			okDetail: func(_ time.Time, wears []NVMeWear) string {
				worst := worstOf(wears)
				return fmt.Sprintf("%s: %d%% of the rated lifespan used, %.1f TB written (TBW)",
					worst.Device, worst.PercentUsed, nvmeTBW(worst))
			},
			hotDetail: func(_ time.Time, wears []NVMeWear) string {
				worst := worstOf(wears)
				var hot []string
				for _, w := range wears {
					if w.PercentUsed >= AlertNVMeWearPercent {
						hot = append(hot, fmt.Sprintf("%s=%d%%", w.Device, w.PercentUsed))
					}
				}
				return fmt.Sprintf("wear %s ≥ threshold %d%% (%s: %d%% of the rated lifespan used, %.1f TB written (TBW))",
					strings.Join(hot, ", "), AlertNVMeWearPercent,
					worst.Device, worst.PercentUsed, nvmeTBW(worst))
			},
		},
		eval: a.observeWear,
	}
}

// nvmeTBW — written terabytes (one NVMe unit = 512,000 bytes, decimal
// TB — as in the TBW specification).
func nvmeTBW(w NVMeWear) float64 {
	return float64(w.DataUnitsWritten) * 512000 / 1e12
}

// observeBadHashes — the junk content_hash counter of ingest cleanup
// (T-099): the source is the /metrics exposition of the ingest service (BadHashes).
// A source error (ingest unavailable, no metrics) — "not measured"
// with a Warn: the death of ingest itself is seen via crawler_service_up, not by this
// alert; the spike window (a delta ring, clearing by aging — hysteresis
// is not needed, the window sum decreases only by aging) — the machine track.
func (a *Alerts) observeBadHashes(ctx context.Context) observation[int64] {
	if a.deps.BadHashes == nil {
		return observation[int64]{
			reason: "source not set (monitor.ingest_metrics_url)",
		}
	}
	cur, err := a.deps.BadHashes.BadHashes(ctx)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: junk content_hash: %w", err)}
	}
	return observation[int64]{value: cur}
}

// newBadHashesMachine — the ingest_bad_hashes machine: the delta ring over the
// AlertBadHashesWindow passes, the AlertBadHashesThreshold threshold (normally
// the result-writer invariant always holds zero; the threshold cuts off
// single relics from before the ING-P2-4 fix — the justification is at the constant).
func (a *Alerts) newBadHashesMachine() *alertMachine[int64] {
	return &alertMachine[int64]{
		name: AlertBadHashes,
		track: ringTrack{
			ring:      &deltaRing{},
			threshold: AlertBadHashesThreshold,
			passes:    AlertBadHashesWindow,
			interval:  a.opts.Interval,
			label:     "counter",
			fireNote:  " — result-writer error (T-056), the refcount did not change",
		},
		eval: a.observeBadHashes,
	}
}

// observeUnitsSource — the unit source read without interpreting an empty
// list: the degradations are shared by the unit_oom and memory_high machines — the source
// not set (quiet), errNoSystemd (a dev container — quiet, like SMART:
// an environment without systemd is the dev norm, not an incident) and other errors
// (a production host, systemd/cgroupfs broken — a Warn every pass, the style
// of the other sources; without a repeated "monitor: units:" — the source
// already puts the prefix).
func (a *Alerts) observeUnitsSource(ctx context.Context) observation[[]UnitEvents] {
	if a.deps.UnitEvents == nil {
		return observation[[]UnitEvents]{reason: "source not set"}
	}
	units, err := a.deps.UnitEvents.UnitEvents(ctx)
	if err != nil {
		if errors.Is(err, errNoSystemd) {
			return observation[[]UnitEvents]{reason: err.Error()}
		}
		return observation[[]UnitEvents]{err: err}
	}
	return observation[[]UnitEvents]{value: units}
}

// observeUnitEvents — the unit_oom observation: an empty list — units not
// loaded, "not measured" (unit events outside the observation).
func (a *Alerts) observeUnitEvents(ctx context.Context) observation[[]UnitEvents] {
	o := a.observeUnitsSource(ctx)
	if o.err != nil || o.reason != "" {
		return o
	}
	if len(o.value) == 0 {
		return observation[[]UnitEvents]{reason: "units " + unitEventsPattern + " not found"}
	}
	return o
}

// observeMemHighUnits — the memory_high observation: an empty list is a
// VALID measurement (0 units), not "not found" — the asymmetry
// is deliberate and old (evalMemHigh in master had no empty-list branch):
// throttling is about the rate of the counters of live units; an empty
// answer cleans the memory of vanished units and gives an ok "0 units" (an episode without
// hot units closes honestly), while unit_oom is about events; their
// absence outside loaded units is not observable.
func (a *Alerts) observeMemHighUnits(ctx context.Context) observation[[]UnitEvents] {
	return a.observeUnitsSource(ctx)
}

// newUnitOOMMachine — the unit_oom machine: firing while any
// unit has oom/oom_kill (memory.events) or NRestarts > 0. The counters
// are monotonic within the life of a cgroup/unit — hysteresis is not needed (the
// NVMe wear style): the episode stretches from the first event to the state reset
// of the unit — a restart (a deploy resets it itself: stop → reset-failed →
// start in deploy-install.sh; reset-failed/start zeroes NRestarts,
// a recreated cgroup takes the oom/oom_kill with it). One Warn per
// episode is given by the front mechanics of transition.
func (a *Alerts) newUnitOOMMachine() *alertMachine[[]UnitEvents] {
	return &alertMachine[[]UnitEvents]{
		name: AlertUnitOOM,
		track: thresholdTrack[[]UnitEvents]{
			fire: func(_ time.Time, units []UnitEvents) bool {
				for _, u := range units {
					if u.hot() {
						return true
					}
				}
				return false
			},
			okDetail: func(_ time.Time, units []UnitEvents) string {
				return fmt.Sprintf("no OOM or restarts (%d units)", len(units))
			},
			hotDetail: func(_ time.Time, units []UnitEvents) string {
				var hot []string
				for _, u := range units {
					if !u.hot() {
						continue
					}
					hot = append(hot, fmt.Sprintf("%s: oom=%d oom_kill=%d restarts=%d",
						u.Unit, u.OOM, u.OOMKill, u.NRestarts))
				}
				return strings.Join(hot, ", ") + " — episode until the unit restart (NRestarts — reset-failed, oom/oom_kill — a new cgroup)"
			},
		},
		eval: a.observeUnitEvents,
	}
}

// newMemHighMachine — the memory_high machine (T-162): a series of counter growth
// of memory.events high across units — the threshold is not a value but a rate (the counter
// accumulates over the cgroup lifetime: http-worker with MemoryHigh=850M gains
// >100k per day). Persistence — growth for MemHighMinPasses passes
// in a row (2 × the alert period = 10 min at the default 5 min, T-206):
// a single spike is normal soft-cap operation; sustained
// throttling is the harbinger of the http-worker fatals (T-157) and a request for a
// higher cap (docs/deploy.md §2). Clearing — the first pass without growth:
// no hysteresis needed, the clearing event is exactly "growth stopped",
// flapping at the series zero is impossible (the series is integer). The source and degradations
// are the same as unit_oom, EXCEPT the empty list: for memory_high it is
// a valid "0 units" measurement (observeMemHighUnits), not "not
// found" — the former master asymmetry.
func (a *Alerts) newMemHighMachine() *alertMachine[[]UnitEvents] {
	return &alertMachine[[]UnitEvents]{
		name:  AlertMemHigh,
		track: &memHighStreakTrack{minPasses: a.opts.MemHighMinPasses, units: make(map[string]memHighTrack)},
		eval:  a.observeMemHighUnits,
	}
}

// observeDead — the q:dead depth (T-154): a quarantine without consumers
// (manual triage, spec 4.2); Len reads XLEN; entries leave the quarantine
// only by operator triage (XTRIM/runbooks §2) — the depth is exactly the
// "untriaged remainder". The source is the same Redis as the queue
// alert (StreamLener); the death rate cross-check — the counter
// crawler_queue_dead_buried_total (T-154) explains where the sink is.
// Persistence (a series of passes, holding by any non-zero depth,
// clearing by an empty quarantine) — the machine track; a separate growth check
// is not needed: growth is a special case of sustained non-zero, and
// a "growing/stalled" differentiation would flap on the moving XTRIM
// of triage.
func (a *Alerts) observeDead(ctx context.Context) observation[int64] {
	if a.deps.Redis == nil {
		return observation[int64]{reason: "Redis not set"}
	}
	n, err := a.deps.Redis.Len(ctx, queue.StreamDead)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: q:dead depth: %w", err)}
	}
	return observation[int64]{value: n}
}

// newDeadMachine — the queue_dead machine: firing on the AlertDeadMinPasses
// pass with a non-zero depth ("persistently non-zero", not "any
// death": the quarantine does not empty itself, but the window is incommensurate with
// the incident scale of post-mortem §11.1 — the signal is late by at most
// one pass), holding by any non-zero depth, clearing by an empty
// q:dead.
func (a *Alerts) newDeadMachine() *alertMachine[int64] {
	return &alertMachine[int64]{
		name:  AlertQueueDead,
		track: &deadStreakTrack{minPasses: AlertDeadMinPasses},
		eval:  a.observeDead,
	}
}

// observeQueueFreeze — the groups of the working streams (T-162):
// the last-delivered-id and the depth of the consumer groups (the source
// StreamGroupsSource). Consistent with BackpressureGate.Len on the depth
// (the T-168 clamp in the source): the stuck phantom lag of the incident
// of Oct 22–23 is not "depth" by itself, but frozen deliveries were there
// — the alert catches this downtime class too. The immobility window — the
// machine track.
func (a *Alerts) observeQueueFreeze(ctx context.Context) observation[[]StreamGroupState] {
	if a.deps.StreamGroups == nil {
		return observation[[]StreamGroupState]{reason: "source not set"}
	}
	groups, err := a.deps.StreamGroups.StreamGroups(ctx)
	if err != nil {
		return observation[[]StreamGroupState]{err: err}
	}
	return observation[[]StreamGroupState]{value: groups}
}

// newQueueFreezeMachine — the queue_frozen machine: the immobility window
// of the group's last-delivered-id at depth > 0 (QueueFreeze — 15 min:
// a live consumer moves the ID with every XREADGROUP, batches in seconds —
// spec 3.3; shorter — false firings on the pauses between batches,
// longer — half of the step "restart the consumer and look at the
// logs" is lost). Clearing — the first pass where no group is frozen
// (the depth went to zero or deliveries resumed): no hysteresis needed; the window
// is the natural flap filter.
func (a *Alerts) newQueueFreezeMachine() *alertMachine[[]StreamGroupState] {
	return &alertMachine[[]StreamGroupState]{
		name:  AlertQueueFreeze,
		track: &queueFreezeTrack{window: a.opts.QueueFreeze, groups: make(map[string]freezeEntry)},
		eval:  a.observeQueueFreeze,
	}
}

// observeRedisMemory — the Redis memory share used_memory/maxmemory from INFO
// memory (T-154): with noeviction (the budget policy of spec 3.2) filling
// maxmemory is an XADD write refusal: INFO does not expose a direct refusal
// counter; the share is a proxy metric with headroom for buffers/fragmentation
// (the incident of Oct 22: OOM publication refusals at 620 of 900 MiB).
// maxmemory 0 (no cap) — "not measured": the ratio is undefined,
// there are no maxmemory write refusals. The 80%/75% hysteresis — the
// machine track.
func (a *Alerts) observeRedisMemory(ctx context.Context) observation[RedisMemory] {
	if a.deps.RedisMemory == nil {
		return observation[RedisMemory]{reason: "source not set"}
	}
	m, err := a.deps.RedisMemory.RedisMemory(ctx)
	if err != nil {
		return observation[RedisMemory]{err: fmt.Errorf("monitor: Redis memory: %w", err)}
	}
	if m.MaxBytes <= 0 {
		return observation[RedisMemory]{
			reason: fmt.Sprintf("maxmemory=%d — no cap, the share is undefined", m.MaxBytes),
		}
	}
	return observation[RedisMemory]{value: m}
}

// newRedisMemoryMachine — the redis_memory machine: the hysteresis of the
// used/max share (firing AlertRedisMemFireRatio 80% — a 20% headroom for
// the between-trims remainders of TrimLoop, fragmentation and client buffers;
// holding the fired state until AlertRedisMemRecoverRatio 75% — memory
// creeps and drops abruptly after XTRIM; a share in the corridor would not flap the
// Warn at the threshold).
func (a *Alerts) newRedisMemoryMachine() *alertMachine[RedisMemory] {
	share := func(m RedisMemory) float64 { return float64(m.UsedBytes) / float64(m.MaxBytes) }
	detail := func(m RedisMemory) string {
		return fmt.Sprintf("used_memory %s of %s maxmemory (%.1f%%), policy %s",
			alertBytes(uint64(m.UsedBytes)), alertBytes(uint64(m.MaxBytes)), 100*share(m), m.Policy)
	}
	return &alertMachine[RedisMemory]{
		name: AlertRedisMemory,
		track: hysteresisTrack[RedisMemory]{
			fire:     func(m RedisMemory) bool { return share(m) >= AlertRedisMemFireRatio },
			hold:     func(m RedisMemory) bool { return share(m) >= AlertRedisMemRecoverRatio },
			okDetail: func(m RedisMemory) string { return detail(m) },
			hotDetail: func(m RedisMemory) string {
				return detail(m) + " — with noeviction, write refusals at the cap (OOM command not allowed)"
			},
		},
		eval: a.observeRedisMemory,
	}
}

// observeRedisUptime — the Redis uptime (INFO server, T-270): a level,
// monotonically growing from the start; a restart resets it — the "below N min" window
// catches the event without machine memory.
func (a *Alerts) observeRedisUptime(ctx context.Context) observation[int64] {
	if a.deps.RedisUptime == nil {
		return observation[int64]{reason: "source not set"}
	}
	up, err := a.deps.RedisUptime.RedisUptime(ctx)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: Redis uptime: %w", err)}
	}
	return observation[int64]{value: up.Seconds}
}

// newRedisRestartMachine — the redis_restart machine (T-270): a level threshold
// of uptime — below the window (a "recent" restart); clearing — leaving the window
// by itself, no track memory needed (thresholdTrack: uptime is monotonic between
// restarts, flapping at the threshold is excluded by the source mechanics).
func (a *Alerts) newRedisRestartMachine() *alertMachine[int64] {
	window := int64(AlertRedisRestartWindow / time.Second)
	uptime := func(v int64) string { return (time.Duration(v) * time.Second).Round(time.Second).String() }
	return &alertMachine[int64]{
		name: AlertRedisRestart,
		track: thresholdTrack[int64]{
			fire:     func(_ time.Time, v int64) bool { return v < window },
			okDetail: func(_ time.Time, v int64) string { return "uptime " + uptime(v) },
			hotDetail: func(_ time.Time, v int64) string {
				return fmt.Sprintf("Redis started %s ago — undelivered stream entries lost (no persistence, docs/redis.md §5); expect new_done_stale growth and the cohort returned by the T-270 compensator",
					uptime(v))
			},
		},
		eval: a.observeRedisUptime,
	}
}

// observePSI — the CPU pressure of /proc/pressure/cpu (some avg10/60/300 —
// the some metric: the delay of at least one task, not full and not the load
// average: 100% utilization without a queue is the norm for a CPU-bound pipeline).
// A kernel without PSI — errNoPSI, a quiet degradation (like SMART in a container);
// the persistence window — the machine track.
func (a *Alerts) observePSI(ctx context.Context) observation[PSIStat] {
	st, err := a.deps.PSI.PSI(ctx)
	if err != nil {
		if errors.Is(err, errNoPSI) {
			return observation[PSIStat]{reason: err.Error()}
		}
		return observation[PSIStat]{err: fmt.Errorf("monitor: PSI CPU: %w", err)}
	}
	return observation[PSIStat]{value: st}
}

// newPSIMachine — the psi_cpu machine: the window of holding avg300 above the threshold
// (PSICPUPercent 90% — working scheduler starvation per measurement §12
// 91–95%, below — utilization without a queue; PSIWindow 30 min — avg300
// itself smooths over 5 minutes; the window demands sustained starvation, one-off
// deploy/retro-batch spikes do not count as an alert; T-158 lowered the weight
// for monitor — its own passes do not approach starvation).
func (a *Alerts) newPSIMachine() *alertMachine[PSIStat] {
	return &alertMachine[PSIStat]{
		name:  AlertPSICPU,
		track: &psiWindowTrack{percent: a.opts.PSICPUPercent, window: a.opts.PSIWindow},
		eval:  a.observePSI,
	}
}

// observeRenderDebt — the render debt snapshot (T-175): the render_pending flags
// (a point Lookup of the crawler_counters counter) + the q:render lag (XLEN of the
// same semantics as queue_overflow) — renderDebtOf: one snapshot with
// the gauge loop and the load report row (the semantics do not diverge).
// The sources are the same DB pool and depth gate: an error of either — "not
// measured" with a Warn; the tracking memory is not touched (recovery
// will arrive by the actual debt). A missing counter row (the migration
// not applied / a manual table edit, runbooks §6) — an error with a Warn,
// not a silent zero: the debt gauge must not lie. The threshold/series/latch —
// the machine track.
func (a *Alerts) observeRenderDebt(ctx context.Context) observation[RenderDebt] {
	if a.deps.Redis == nil {
		return observation[RenderDebt]{reason: "Redis not set"}
	}
	d, err := renderDebtOf(ctx, a.deps.Pool, a.deps.Redis)
	if err != nil {
		return observation[RenderDebt]{err: err}
	}
	return observation[RenderDebt]{value: d}
}

// newRenderDebtMachine — the render_debt machine: a level latch + a series
// of growth (RenderDebtDomains 50k — the healthy background is thousands of flags,
// the alert is about accumulation at the scale of a drain day; RenderDebtPasses 2 —
// 10 min of continuous growth, a one-off jump is resolved by a finishing
// pass). The transition specification — alertstate.go (renderDebtTrack).
func (a *Alerts) newRenderDebtMachine() *alertMachine[RenderDebt] {
	return &alertMachine[RenderDebt]{
		name:  AlertRenderDebt,
		track: &renderDebtTrack{domains: a.opts.RenderDebtDomains, passes: a.opts.RenderDebtPasses},
		eval:  a.observeRenderDebt,
	}
}

// observeRenderFallbackDepth — the q:render-fallback depth (T-246):
// the backlog of worker B's consumer group via the same gate as the
// queue alerts. The mirror metric of the B build
// crawler_queue_depth{queue="q:render-fallback"} is exposed by the
// worker itself and when it dies the series is gone — the incident "B is stalled" is seen only
// by a direct Redis read, hence the source here is Redis, not a metrics probe.
// The threshold — the single calculation of queue.StreamWatermark (for q:render-fallback
// this is not a gate threshold — the stream is not capped by the gate, the render spec
// §4.3 item 3 — but an observation threshold "the depth approaches the
// MAXLEN limit of the stream": 15,000; half is 7,500 at the default watermark
// of 100k); the triggers (a growth series OR a stationary depth) and the latch
// — the machine track.
func (a *Alerts) observeRenderFallbackDepth(ctx context.Context) observation[int64] {
	if a.deps.Redis == nil {
		return observation[int64]{reason: "Redis not set"}
	}
	if a.opts.QueueWatermark <= 0 {
		return observation[int64]{
			reason: "scheduler.queue_high_watermark threshold not set",
		}
	}
	n, err := a.deps.Redis.Len(ctx, queue.StreamRenderFallback)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: %s depth: %w", queue.StreamRenderFallback, err)}
	}
	return observation[int64]{value: n}
}

// newRenderFbDepthMachine — the render_fallback_depth machine: a level
// latch with two triggers — growth for AlertRenderFallbackPasses passes
// in a row (the render_debt mechanics) OR a stationary depth above the threshold
// for the same number of passes (S-P2-1: a stopped B, pinned at MAXLEN, or
// an episode that survived a monitor restart). A one-off publication spike —
// normal fallback operation of "collect and finish" (FR-13); it does not
// wake the operator. The transition specification — alertstate.go (renderFbDepthTrack).
func (a *Alerts) newRenderFbDepthMachine() *alertMachine[int64] {
	wm := queue.StreamWatermark(a.opts.QueueWatermark, a.opts.QueueMaxLen, queue.StreamRenderFallback)
	return &alertMachine[int64]{
		name:  AlertRenderFbDepth,
		track: &renderFbDepthTrack{wm: wm, passes: AlertRenderFallbackPasses},
		eval:  a.observeRenderFallbackDepth,
	}
}

// observeRenderFallbackBudget — the counter of the fallback share budget
// suspension events crawler_render_fallback_suspended_total{reason="budget"}
// (T-239, the /metrics probe of the render worker): an event, not a state — one
// suspension latching is one counter point; while the suspension
// holds, fallback candidates finalize normally (tasks are not lost,
// FR-9). An empty monitor.render_metrics_url — a quiet "not measured"
// (the source not connected, the ingest probe style); a probe error (the worker
// stopped, no metrics) — "not measured" with a Warn: the death of the
// worker itself is seen via crawler_service_up. The events window — the machine track.
func (a *Alerts) observeRenderFallbackBudget(ctx context.Context) observation[int64] {
	if a.deps.FallbackBudget == nil {
		return observation[int64]{
			reason: "source not set (monitor.render_metrics_url)",
		}
	}
	cur, err := a.deps.FallbackBudget.FallbackBudget(ctx)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: fallback share budget: %w", err)}
	}
	return observation[int64]{value: cur}
}

// newRenderFbBudgetMachine — the render_fallback_budget machine: a
// delta ring of events over the AlertFallbackBudgetWindow passes window (30 min —
// the window fits an episode of the T-239 budget window and one recurrence), the threshold 1 —
// every event is an anomaly (normally publications fit in the share
// cap); clearing by aging: while the suspension holds there are no new points
// — only the window keeps it; the operator sees the fallback rate on the engines dashboard.
func (a *Alerts) newRenderFbBudgetMachine() *alertMachine[int64] {
	return &alertMachine[int64]{
		name: AlertRenderFbBudget,
		track: ringTrack{
			// The window of 6 passes (not 15 like the junk hash window): the ring capacity
			// is set here, the default push does not know it.
			ring:      &deltaRing{size: AlertFallbackBudgetWindow},
			threshold: 1,
			passes:    AlertFallbackBudgetWindow,
			interval:  a.opts.Interval,
			label:     "budget events",
			fireNote:  " — the fallback share went past the render.fallback.share_percent cap: primary engine degradation (the fallback rate — the dashboard engines panel, the reasons — crawler_render_task_finalized_total)",
		},
		eval: a.observeRenderFallbackBudget,
	}
}

// observeRenderBrowserErrors — the counter of the browser-class errors of the
// render stage crawler_stage_errors_total{stage="render",type="browser"}
// (S-P2-6) from the /metrics probe of the render worker (the same address as the
// budget probe). The stage_errors family is lazy: "no series" at the render worker
// — 0, not "not measured" (the marker crawler_stage_tasks_total{stage=
// "render"}; like suspended after the T-250 pre-creation). A foreign exposition
// (no marker) — a quiet "not measured" without a Warn: the address
// monitor.render_metrics_url is not configured to a render worker —
// a configuration problem, the probe is alive; a network error of the probe — "not
// measured" with a Warn, like the other probes. The growth series — the
// machine track.
func (a *Alerts) observeRenderBrowserErrors(ctx context.Context) observation[int64] {
	if a.deps.RenderBrowserErrors == nil {
		return observation[int64]{
			reason: "source not set (monitor.render_metrics_url)",
		}
	}
	cur, err := a.deps.RenderBrowserErrors.RenderBrowserErrors(ctx)
	if err != nil {
		if errors.Is(err, errNotRenderExposition) {
			return observation[int64]{
				reason: "the exposition is not a render worker (no crawler_stage_tasks_total{stage=\"render\"}) — check monitor.render_metrics_url",
			}
		}
		return observation[int64]{err: fmt.Errorf("monitor: render browser errors: %w", err)}
	}
	return observation[int64]{value: cur}
}

// newRenderBrowserErrMachine — the render_browser_errors machine: a
// counter growth series for AlertRenderBrowserErrorsPasses passes (10 min):
// a single browser error is a normal session/context restart (T-063)
// and an episode resolved by the health gate; it does not build a series; a growth series
// — the candidate engine (moli/lightpanda) is hung or unavailable: the gate of T-239
// has already suspended fallback publications, the browser class waits in the redelivery
// of q:render — the operator needs a signal about the engine itself. The gate event
// remains the suspended{reason="gate"} counter and the worker's Warn log:
// the detector does not duplicate the latching, it sees the root cause.
func (a *Alerts) newRenderBrowserErrMachine() *alertMachine[int64] {
	return &alertMachine[int64]{
		name:  AlertRenderBrowserErr,
		track: &counterGrowthTrack{minPasses: AlertRenderBrowserErrorsPasses},
		eval:  a.observeRenderBrowserErrors,
	}
}

// observeDomainsNewDoneStale — the crawler_counters new_done_stale row
// (T-256, FR-2.2): "status hung" domains (status = new ∧ check_state =
// done ∧ last_checked_at older than a day; the DNS stage wrote the "has
// addresses" branch — O6 of the shared status table internal/model/enums.go; the HTTP outcome
// of O7–O9 never arrived: the q:http task is lost). The value
// is written by the FILTER column f_new_done_stale of the reconciliation pass
// (zonestats.go); the alert reads it with a point lookup — a seq scan over
// domains adds nothing; updated_at — the snapshot stamp (the series grows
// only on a NEW snapshot — the machine track). An empty result (no
// row before the first reconciliation pass — both monitor loops start
// together; a correct "no row" state does not exist) — a quiet "not measured";
// a read error — a Warn. The cohort is treated by the T-270 compensator (the early recheck
// of R15, stalerequeue.go — the guard of the pass reads the same row): the alert is
// the signal about new losses; the level is observed by the gauge
// crawler_domains_new_done_stale.
func (a *Alerts) observeDomainsNewDoneStale(ctx context.Context) observation[staleRow] {
	rows, err := a.deps.Pool.Query(ctx, newDoneStaleSQL)
	if err != nil {
		return observation[staleRow]{err: fmt.Errorf("monitor: \"status hung\" domains: %w", err)}
	}
	defer rows.Close()
	if !rows.Next() {
		// A dropped connection — not "no row": first a read failure
		// (the retryMatureCount style).
		if err := rows.Err(); err != nil {
			return observation[staleRow]{err: fmt.Errorf("monitor: \"status hung\" domains: %w", err)}
		}
		return observation[staleRow]{
			reason: "the new_done_stale row is missing — the first reconciliation pass has not completed yet",
		}
	}
	var n int64
	var snap sql.NullTime
	if err := rows.Scan(&n, &snap); err != nil {
		return observation[staleRow]{err: fmt.Errorf("monitor: \"status hung\" domains: scan: %w", err)}
	}
	if err := rows.Err(); err != nil {
		return observation[staleRow]{err: fmt.Errorf("monitor: \"status hung\" domains: %w", err)}
	}
	return observation[staleRow]{value: staleRow{value: n, snap: snap}}
}

// newDomainsStaleMachine — the domains_new_done_stale machine: a series of NEW
// non-zero reconciliation snapshots (AlertNewDoneStalePasses — 2 × the
// reconciliation period ≈ 1h of sustained non-zero value; stationary
// zero normally — the norm does not wake anyone at all).
func (a *Alerts) newDomainsStaleMachine() *alertMachine[staleRow] {
	return &alertMachine[staleRow]{
		name:  AlertDomainsStale,
		track: &newDoneStaleTrack{minPasses: AlertNewDoneStalePasses},
		eval:  a.observeDomainsNewDoneStale,
	}
}

// AlertsLoop — the periodic alert evaluation loop (the ZoneStatsLoop style):
// the first pass right at start, then by ticker. Source errors —
// a Warn inside the pass, the loop keeps living (the OnRun contract, T-012): the next
// pass re-evaluates on live data. A ctx cancellation — a clean nil (graceful
// shutdown, spec 3.4).
func AlertsLoop(ctx context.Context, deps AlertsDeps, opts AlertsOptions) error {
	if deps.Pool == nil {
		return errors.New("monitor: alerts: DB pool not set")
	}
	a := NewAlerts(deps, opts)
	a.log.Info("alert evaluation loop",
		"interval", a.opts.Interval.String(), "alerts", len(a.machines))

	pass := func() { a.Pass(ctx) }
	pass()
	ticker := time.NewTicker(a.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// alertBytes — a human-readable size for alert details (a local
// analog of content.humanBytes: the storage package is not pulled in).
func alertBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

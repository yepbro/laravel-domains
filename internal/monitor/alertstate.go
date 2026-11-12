// The monitor alert state machine (T-264, spec tasks/TZ-fsm.md §3 FR-10): the single
// point of truth about the states and transitions of the AlertsLoop cycle alerts
// (monitor/alerts.go). Before T-264 the state of one alert was smeared
// across 2–3 variables (a status map + personal memory fields:
// deadStreak, psiHot, renderDebt, renderFbBudget, ...); consistency
// was maintained manually by each eval function; now each alert is a value
// of the single alertMachine structure "{name, status, track memory}"; the transition is
// the single method transition; the source observation and
// the transition decision (the typed track memory) are separated.
// The formalization does NOT change alert behavior (FR-10): the thresholds, windows, series,
// latches and detail texts are the same; the existing tests are migrated to
// the "sequence of observations → sequence of statuses" format
// without changing the expectations. The formalization does not rewrite the transitions onto
// an FSM framework (TZ-fsm spec §1): they remain functions over observations with
// guard predicates; this specification is what the eval
// (observations) and track (transitions) of alerts.go are checked against.
//
// # States (shared by all 19 machines)
//
//	ok           the alert condition is not met
//	firing       the alert fired (Warn on the transition; the delivery debouncing —
//	             notify.go "state change only")
//	not_measured the alert does not apply or the source degraded: an error (err
//	             — a Warn every pass, the loop keeps living) or a quiet reason
//	             (reason — Debug: the inapplicability is expected, SMART in
//	             a container); it never yields false firings
//
// The status before the first pass is "" (empty): all transitions out of it are
// front ones (firing/inapplicability), like the former empty state
// map.
//
// # Events
//
//	measurement  observation{value} — the source is alive, the observation is valid
//	             (validity is judged by eval: a sample minimum, a disabled
//	             check, maxmemory 0 — a reason, not a value)
//	degradation  observation{err | reason} — the source is broken/inapplicable;
//	             the track memory is NOT touched (an unknown observation is not
//	             an observation: the series, windows and latches survive the failure;
//	             recovery finishes the job on the actual data)
//
// # Transitions: track type × guard → target
//
//	| track type | machines | firing entry | firing hold | clearing → ok |
//	|-----------|--------|--------------------|-----------------|-------------|
//	| level threshold thresholdTrack | progress_stall, disk_free, disk_free_min, disk_free_crit, nvme_wear, unit_oom, redis_restart | the observation predicate | — (no memory: every pass decides anew; the "memory" is the observed level itself, monotonic by mechanics: the stamp age, space, wear, event counters, Redis uptime) | the same predicate |
//	| threshold hysteresis hysteresisTrack | dns_temp_error_share, queue_overflow, redis_memory | fire(v) | prev = firing ∧ hold(v) — the clearing corridor does not flap the Warn | ¬(fire ∨ hold) |
//	| series: deadStreakTrack | queue_dead | a non-zero depth for N=3 passes in a row | prev = firing ∧ depth > 0 | a zero depth |
//	| series: memHighStreakTrack | memory_high | growth of the unit's high counter for N=2 passes in a row (any unit) | the hot unit series is not broken | the first pass without growth |
//	| series: counterGrowthTrack | render_browser_errors | counter growth for N=2 passes in a row | the series is not broken | the first pass without growth |
//	| series: newDoneStaleTrack | domains_new_done_stale | a NEW non-zero reconciliation snapshot N=2 in a row (a change of updated_at: alert passes are more frequent than the source; a repeated read does not count as an observation) | prev = firing ∧ value > 0 | a zero snapshot |
//	| level latch: renderDebtTrack | render_debt | the level > the threshold ∧ a growth series N=2 (strict >) | the latch holds firing while the level ≥ the clearing threshold (90%, ceil) — stagnation and lag swings do not break the episode | the level < the clearing threshold |
//	| level latch: renderFbDepthTrack | render_fallback_depth | the depth ≥ the threshold ∧ (a growth series OR an "at the threshold" series N=2 — a stationary depth, S-P2-1) | the latch holds while the depth ≥ the clearing threshold | the depth < the clearing threshold |
//	| delta ring ringTrack | ingest_bad_hashes, render_fallback_budget | the sum of the deltas over the window (15/6 passes) ≥ the threshold (10/1) | a continuing event flow extends the sum | aging: eviction of deltas from the ring |
//	| time window: psiWindowTrack | psi_cpu | avg300 > the threshold sustained over the window (30 min: the first pass above the threshold starts the window, firing on its close) | hold = the window ticks | a dip below the threshold resets the window (a spike does not extend it) |
//	| time window: queueFreezeTrack | queue_frozen | the group's last-delivered-id is immobile ≥ the window (15 min) at depth > 0 | the immobility holds | the ID moves / depth 0 / no groups |
//
//	Degradation (any machine): → not_measured, the track memory untouched.
//	Level latches are the only ones whose LIVED episode survives
//	degradation (firing lives in the track memory, not in the status); for the hystereses
//	and series "not measured" overwrites the status — the return of the source
//	in the hold corridor yields ok, not a hold (the former state map
//	semantics, documented as a decision).
//
// # Transition side effects (shared, only transition)
//
//   - a status change — a front log entry (logFront): firing
//     "ALERT" Warn, clearing Info, inapplicability Debug; a fired alert going
//     to "not measured" — Info (the loss of an active incident is visible in the log);
//   - a source error — a Warn every pass (except a ctx cancellation — part of
//     the service shutdown);
//   - the pass snapshot — an AlertSnapshot into AlertsResult → the OnPass hook
//     (Telegram delivery by notify.go, the T-077 metrics).
//
// # Invariants
//
//	I1  The machine state (the status and the track memory) is written ONLY by
//	    alertMachine.transition: the eval functions return an observation and
//	    do not touch the state; the decide of the track memory is the only writer
//	    of its own memory and is called only by transition with a valid
//	    measurement.
//	I2  A source degradation does not touch the track memory: the series/windows/latches
//	    survive the failure; the status is meanwhile overwritten to
//	    not_measured (the status-bound hysteresis corridors
//	    are reset — the former behavior).
//	I3  Front entries — only on a status change (debouncing); a repeated
//	    pass in the same state is silent.
//	I4  The machine order is fixed (NewAlerts): the AlertsResult
//	    snapshot is deterministic, the order of Redis reads of the sources is stable.
//	I5  Each alert is a value of the single alertMachine structure; what distinguishes
//	    machines is only the track memory type and the observation source (the
//	    machine registry — NewAlerts; the source specifications — alerts.go).
package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"crawler/internal/queue"
)

// observation — the observation of one source pass: a measurement
// (value), a quiet degradation with a reason (reason — "not measured" without
// a Warn: the inapplicability is expected — SMART in a container, a disabled
// check, too little data) or a source error (err — a Warn every
// pass, the loop keeps living). The eval functions return an observation and do NOT write
// state — only alertMachine.transition writes state (I1).
type observation[V any] struct {
	value  V
	reason string
	err    error
}

// decision — the transition decision of one machine: the pass status and the
// snapshot detail (the details are the same — the texts were migrated from the eval functions without
// edits).
type decision struct {
	status string
	detail string
}

// alertTrack — the typed track memory of a single alert's machine:
// applies the measurement to the memory (series/windows/latches) and decides the
// pass status. decide is the only writer of its own memory; it is called
// only by transition and only with a valid measurement — degradations
// do not touch the memory (I1/I2). The implementations (families): thresholdTrack (a
// level threshold, no memory), hysteresisTrack (a threshold hysteresis),
// deadStreakTrack/memHighStreakTrack/counterGrowthTrack/
// newDoneStaleTrack (a series), renderDebtTrack/renderFbDepthTrack
// (a level latch), ringTrack (a delta ring), psiWindowTrack/
// queueFreezeTrack (a time window).
type alertTrack[V any] interface {
	// decide applies the observation v to the memory; prev — the past status
	// of the machine (the hysteresis guards and series holds), now — the current
	// time (windows, ages).
	decide(prev string, now time.Time, v V) decision
}

// alertMachine — the single state machine structure of an alert (FR-10): "{name,
// status, track memory}". All 20 alerts are values of this type;
// what distinguishes machines is only the track memory type (alertTrack) and the
// observation source (eval — an observe* function in alerts.go). The single
// state-writing point — transition (I1).
type alertMachine[V any] struct {
	name   string
	status string
	track  alertTrack[V]
	eval   func(ctx context.Context) observation[V]
}

// alertRunner — the type erasure of alertMachine[V] for the heterogeneous list
// of the pass machines (the order is fixed — I4).
type alertRunner interface {
	run(ctx context.Context, a *Alerts) AlertSnapshot
}

// compile-time: alertMachine implements alertRunner for any V.
var _ alertRunner = (*alertMachine[int64])(nil)

// run — one machine pass: the source observation → the transition.
func (m *alertMachine[V]) run(ctx context.Context, a *Alerts) AlertSnapshot {
	return m.transition(a, ctx, m.eval(ctx))
}

// transition — the SINGLE state-writing point of the machine (FR-10, I1):
// applies the observation (a degradation moves to "not measured", without
// touching the track memory — I2), writes a front entry on a status change
// (I3) and updates the status. A source error — a Warn every pass,
// except a ctx cancellation (part of the shutdown, the ZoneStatsLoop style).
func (m *alertMachine[V]) transition(a *Alerts, ctx context.Context, o observation[V]) AlertSnapshot {
	prev := m.status
	var status, detail string
	switch {
	case o.err != nil:
		if ctx.Err() == nil {
			a.log.Warn("оценка алерта: "+alertDescs[m.name], "alert", m.name, "err", o.err)
		}
		status, detail = AlertStatusNotMeasured, o.err.Error()
	case o.reason != "":
		status, detail = AlertStatusNotMeasured, o.reason
	default:
		d := m.track.decide(prev, a.now(), o.value)
		status, detail = d.status, d.detail
	}
	a.logFront(m.name, prev, status, detail)
	m.status = status
	return AlertSnapshot{Name: m.name, Status: status, Detail: detail}
}

// ------------------------------------------------------------ level threshold

// thresholdTrack — a machine without track memory: the decision is the
// observation predicate (a level threshold). The memory is the observed level itself
// (the zone stamp age, the disk space, the NVMe wear, the unit event
// counters) — the quantities are monotonic or event-driven by the source mechanics;
// flapping at the threshold is excluded without a gap (the justifications are at the sources in
// alerts.go). The threshold may be inapplicable to an observation (na: the absolute
// minimum ≥ the volume capacity) — a quiet "not measured"; nil — always applicable.
type thresholdTrack[V any] struct {
	fire      func(now time.Time, v V) bool
	okDetail  func(now time.Time, v V) string
	hotDetail func(now time.Time, v V) string // nil — the ok detail (disk: one detail for all)
	na        func(v V) string                // the inapplicability (detail) or "" — applicable; nil — no check
}

// decide — the threshold transition: na → not_measured, fire → firing, otherwise ok.
func (t thresholdTrack[V]) decide(_ string, now time.Time, v V) decision {
	if t.na != nil {
		if d := t.na(v); d != "" {
			return decision{AlertStatusNotMeasured, d}
		}
	}
	if t.fire(now, v) {
		d := t.okDetail(now, v)
		if t.hotDetail != nil {
			d = t.hotDetail(now, v)
		}
		return decision{AlertStatusFiring, d}
	}
	return decision{AlertStatusOK, t.okDetail(now, v)}
}

// --------------------------------------------------------- threshold hysteresis

// hysteresisTrack — a threshold hysteresis: entry by fire, holding
// the fired state by hold (the clearing corridor); the memory is the machine
// status (prev): a value in the corridor between the thresholds does not flap the Warn.
// The corridor lives only while the status is firing: "not measured" (a source
// failure) overwrites the status — the return of the source in the corridor yields
// ok, not a hold (the former state map semantics, I2).
type hysteresisTrack[V any] struct {
	fire      func(v V) bool
	hold      func(v V) bool
	okDetail  func(v V) string
	hotDetail func(v V) string // nil — the ok detail (one DNS error share for both statuses)
}

// decide — the hysteresis transition: fire ∨ (prev = firing ∧ hold) → firing.
func (t hysteresisTrack[V]) decide(prev string, _ time.Time, v V) decision {
	if t.fire(v) || (prev == AlertStatusFiring && t.hold(v)) {
		d := t.okDetail(v)
		if t.hotDetail != nil {
			d = t.hotDetail(v)
		}
		return decision{AlertStatusFiring, d}
	}
	return decision{AlertStatusOK, t.okDetail(v)}
}

// dnsShare — the observation of the day's temp_error share: the numerator (the events
// of result writes with the temp_error verdict) and the denominator (the events
// of result writes) of the daily crawler_counters rows (T-182).
type dnsShare struct {
	temp  int64
	total int64
}

// share — the temp_error share (the denominator ≥ AlertDNSMinSamples — guaranteed
// by observeDNS; an empty window does not become an observation).
func (d dnsShare) share() float64 {
	return float64(d.temp) / float64(d.total)
}

// staleRow — the observation of the new_done_stale row: the value (the
// "status hung" domains) and the reconciliation snapshot stamp (the row's updated_at —
// the moment of the pass that wrote the value).
type staleRow struct {
	value int64
	snap  sql.NullTime
}

// ------------------------------------------------------------- pass series

// deadStreakTrack — a series of non-zero passes (queue_dead, T-154):
// a "persistently non-zero" quarantine instead of an instant firing on the
// first death; the fired state is held by any non-zero depth
// (the episode hysteresis: from the third pass to the operator's triage),
// clearing — an empty q:dead. Growth is a special case of non-zero: a separate
// growth check would flap on the moving XTRIM of triage.
type deadStreakTrack struct {
	minPasses int
	streak    int
}

// decide — the q:dead series transition.
func (t *deadStreakTrack) decide(prev string, _ time.Time, n int64) decision {
	if n > 0 {
		t.streak++
	} else {
		t.streak = 0
	}
	detail := fmt.Sprintf("глубина q:dead=%d, ненулевых проходов подряд %d/%d; темп смертей — crawler_queue_dead_buried_total",
		n, t.streak, t.minPasses)
	if t.streak >= t.minPasses || (prev == AlertStatusFiring && n > 0) {
		return decision{AlertStatusFiring, detail + " — разбор: runbooks §2"}
	}
	return decision{AlertStatusOK, detail}
}

// memHighTrack — the series memory of one unit: the past
// memory.events high counter and the series of passes in a row with its growth. Growth
// is only an increase: a unit restart recreates the cgroup and
// zeroes the counter — a decrease starts a new series without a false spike
// (the deltaRing style).
type memHighTrack struct {
	prev   uint64
	streak int
	seen   bool
}

// memHighStreakTrack — a series of MemoryHigh throttling growth across units
// (memory_high, T-162): the event is the growth of the unit's high counter between
// passes (the threshold is not a value but a rate: the counter accumulates over the
// cgroup lifetime). Firing when some unit's series reaches
// minPasses; clearing — the first pass without growth (the clearing event is exactly
// "growth stopped"; flapping at the series zero is impossible — the series is integer).
// Units that vanished from the answer (renamed/unloaded) do not
// accumulate memory.
type memHighStreakTrack struct {
	minPasses int
	units     map[string]memHighTrack
}

// decide — the MemoryHigh series transition.
func (t *memHighStreakTrack) decide(_ string, _ time.Time, units []UnitEvents) decision {
	seen := make(map[string]bool, len(units))
	var hot []string
	for _, u := range units {
		seen[u.Unit] = true
		tr := t.units[u.Unit]
		grew := int64(0)
		if tr.seen && u.High > tr.prev {
			tr.streak++
			grew = int64(u.High - tr.prev)
		} else {
			tr.streak = 0
		}
		tr.seen, tr.prev = true, u.High
		t.units[u.Unit] = tr
		if tr.streak >= t.minPasses {
			hot = append(hot, fmt.Sprintf("%s: +%d за проход, серия %d", u.Unit, grew, tr.streak))
		}
	}
	for name := range t.units { // the unit vanished (renamed/unloaded) — do not accumulate
		if !seen[name] {
			delete(t.units, name)
		}
	}
	if len(hot) > 0 {
		return decision{
			AlertStatusFiring,
			strings.Join(hot, ", ") + " — устойчивый троттлинг MemoryHigh (reclaim), предвестник фаталов (T-157)",
		}
	}
	return decision{
		AlertStatusOK,
		fmt.Sprintf("роста троттлинга MemoryHigh нет (%d юнитов, порог — рост %d проходов подряд)",
			len(units), t.minPasses),
	}
}

// counterGrowthTrack — a series of growth of a single counter between passes
// (render_browser_errors, S-P2-6): the past probe snapshot and the series of growing
// passes (the memHighTrack style; a separate type — the counter is int64, like
// the values of the /metrics probes). An owner restart (a decrease) — a zero delta,
// the series starts from scratch.
type counterGrowthTrack struct {
	minPasses int
	prev      int64
	streak    int
	seen      bool
}

// decide — the counter growth series transition.
func (t *counterGrowthTrack) decide(_ string, _ time.Time, cur int64) decision {
	var delta int64
	if t.seen && cur > t.prev {
		t.streak++
		delta = cur - t.prev
	} else {
		t.streak = 0
	}
	t.seen, t.prev = true, cur
	detail := fmt.Sprintf("browser-ошибки рендера %d, серия роста %d/%d (+%d за проход)",
		cur, t.streak, t.minPasses, delta)
	if t.streak >= t.minPasses {
		return decision{
			AlertStatusFiring,
			detail + " — движок рендера завис или недоступен: health-гейт приостановил фолбэк-публикации, браузер-класс в redelivery q:render (откат движка — ТЗ рендера §6.4, разбор — docs/render.md)",
		}
	}
	return decision{AlertStatusOK, detail}
}

// newDoneStaleTrack — a series of NEW non-zero reconciliation snapshots
// (domains_new_done_stale, T-256 FR-2.2): the alert passes (5 min) are more frequent
// than the source passes (zone_stats_interval, 30 min) — the series grows only
// on a change of the snapshot stamp (the row's updated_at); a repeated read of the same
// row does not count as an observation (a single boundary snapshot
// of the day boundary is overwritten by zero by the next pass — no
// alert). A zero snapshot resets the series; the fired state is held by any
// non-zero row (the queue_dead style).
type newDoneStaleTrack struct {
	minPasses int
	snap      time.Time
	streak    int
	seen      bool
}

// decide — the new_done_stale snapshot series transition.
func (t *newDoneStaleTrack) decide(prev string, _ time.Time, row staleRow) decision {
	n := row.value
	if n > 0 {
		// The series is the number of distinct non-zero snapshots: a repeated read
		// of the same row (the alert polls more often than reconciliation) does not
		// grow the series; a zero snapshot resets it.
		switch {
		case !t.seen || !row.snap.Valid || !row.snap.Time.Equal(t.snap):
			t.streak++
		}
		t.seen = true
		t.snap = row.snap.Time
	} else {
		t.streak = 0
	}
	detail := fmt.Sprintf("доменов new∧done старше суток %d, ненулевых снапшотов реконсиляции подряд %d/%d (значение — проход реконсиляции, gauge crawler_domains_new_done_stale)",
		n, t.streak, t.minPasses)
	if t.streak >= t.minPasses || (prev == AlertStatusFiring && n > 0) {
		return decision{
			AlertStatusFiring,
			detail + " — потеря задач q:http: HTTP-итог не записан, когорту вернёт компенсатор T-270 (ранний recheck, stalerequeue.go; спецификация — internal/model/enums.go)",
		}
	}
	return decision{AlertStatusOK, detail}
}

// ------------------------------------------------------------- level latch

// depthGrowth — the memory of a depth level: the past snapshot, the series of passes with
// growth, the latch of the fired episode and the series of "at the threshold" passes
// (hot — only render_fallback_depth). Growth is a strict increase:
// the drain and the inflow splash around a constant — stagnation, not
// accumulation, it does not build a series. The firing latch survives "not measured"
// (a source failure overwrites the alert state; the T-175 review round 2,
// P2-1): the hold must rely on it, not on the past status —
// otherwise a short DB/Redis failure in the middle of an episode would give a false ✅
// "Recovered" at a debt above the clearing threshold; only the actual
// drain below the threshold resets the latch. hot catches a stationary high
// depth that the growth series does not see — a monitor restart in the middle of
// an episode (the first pass bases prev and the growth series at zero) and pinning at
// the MAXLEN limit of the stream (the trim evicts the oldest undelivered
// entries — the depth stops growing).
type depthGrowth struct {
	prev   int64
	streak int
	seen   bool
	firing bool
	hot    int
}

// growthStep applies the new level: updates the growth series (a strict
// increase) and the "at the threshold" series (atThreshold — the level reached the
// firing threshold; render_debt keeps no "level" threshold — debt stagnation
// is a load report row, not an alert), bases the first snapshot; returns the
// growth delta for the detail.
func (g *depthGrowth) growthStep(level int64, atThreshold bool) (delta int64) {
	if g.seen && level > g.prev {
		g.streak++
		delta = level - g.prev
	} else {
		g.streak = 0
	}
	if atThreshold {
		g.hot++
	} else {
		g.hot = 0
	}
	g.seen, g.prev = true, level
	return delta
}

// renderDebtTrack — the render debt level latch (render_debt, T-175):
// firing — the debt (the render_pending flags + the q:render lag) strictly above
// the threshold AND growing for minPasses passes in a row (a series of positive deltas,
// the memory_high style). Clearing — the drain below 90% of the threshold: the debt is a LEVEL,
// not a monotonic counter; the lag swings by hundreds–thousands per pass at
// a drain of ~1–2 rows/s — a "by no-growth" clearing would swing the episode 🚨/✅ every
// few passes at a stationary high debt (the T-175 review, P2-1).
// Hence growth is only a trigger (stagnation without a growth series does not wake
// the operator — it is visible in the "Render debt" row of the load report and the gauge);
// the level — the hold and the clearing (the clearing threshold is 90%, ceil — at a threshold of 1
// the clearing < 1 is reached by zero; review round 2 P3-3). The hold
// relies on the memory latch, not the past status (I2). The first pass —
// the base (growth is unknown).
type renderDebtTrack struct {
	domains int64
	passes  int
	mem     depthGrowth
}

// decide — the render debt latch transition.
func (t *renderDebtTrack) decide(_ string, _ time.Time, d RenderDebt) decision {
	total := d.Total()
	delta := t.mem.growthStep(total, false)

	// The clearing threshold — 90% of the firing threshold, an integer ceil:
	// a corridor ≥ 1 at any valid threshold (the minimum 1) — the fired
	// alert is always reachable for clearing; at 50k the clearing is below 45k.
	recoverAt := (t.domains*9 + 9) / 10
	detail := fmt.Sprintf("рендер-долг: %s, порог %d (снятие < %d), серия роста %d/%d проходов (+%d за проход)",
		d.String(), t.domains, recoverAt, t.mem.streak, t.passes, delta)
	switch {
	case total > t.domains && t.mem.streak >= t.passes:
		t.mem.firing = true
		return decision{
			AlertStatusFiring,
			detail + " — накопление: приток SPA обгоняет дренаж (render.contexts / приток планировщика, разбор — docs/runbooks.md)",
		}
	case t.mem.firing && total >= recoverAt:
		// Holding the fired state: lag swings at a stationary debt
		// (the growth series is reset, the level above the clearing threshold) — the
		// episode lives until the actual drain. A latch, not the past status:
		// a source failure ("not measured") does not close the episode.
		return decision{
			AlertStatusFiring,
			detail + " — долг держится выше порога снятия (уровень — строка «Рендер-долг» отчёта нагрузки)",
		}
	default:
		t.mem.firing = false
		return decision{AlertStatusOK, detail}
	}
}

// renderFbDepthTrack — the level latch of the q:render-fallback depth
// (render_fallback_depth, T-246, render spec §4.6 item 3; the second trigger —
// S-P2-1, the operator's decision): firing — the depth at the effective
// stream threshold (the single queue.StreamWatermark calculation — like the
// queue alert; for q:render-fallback this is not a gate threshold but an observation
// threshold "the depth approaches the MAXLEN limit") AND (growing for minPasses passes
// in a row — the render_debt mechanics — OR holding above the threshold for minPasses
// passes without a growth requirement: a stationary depth at the threshold —
// a stopped worker B or pinning at MAXLEN; XADD evicts the oldest
// undelivered entries and new fallback tasks are silently lost).
// Clearing — the drain below 90% of the threshold (ceil, the render_debt style); stagnation
// above the threshold is held by the latch and does not wake again.
type renderFbDepthTrack struct {
	wm     int64
	passes int
	mem    depthGrowth
}

// decide — the fallback depth latch transition.
func (t *renderFbDepthTrack) decide(_ string, _ time.Time, n int64) decision {
	delta := t.mem.growthStep(n, n >= t.wm)

	// The clearing threshold — 90% of the firing threshold, an integer ceil
	// (the render_debt style): a corridor ≥ 1 at any valid watermark.
	recoverAt := (t.wm*9 + 9) / 10
	detail := fmt.Sprintf("глубина %s=%d, порог %d (снятие < %d), серия роста %d/%d (+%d за проход), у порога %d/%d проходов",
		queue.StreamRenderFallback, n, t.wm, recoverAt, t.mem.streak, t.passes, delta,
		t.mem.hot, t.passes)
	switch {
	case n >= t.wm && (t.mem.streak >= t.passes || t.mem.hot >= t.passes):
		t.mem.firing = true
		if t.mem.streak >= t.passes {
			return decision{
				AlertStatusFiring,
				detail + " — накопление: приток фолбэка обгоняет темп воркера B (render.fallback.worker_rate / движок-кандидат, разбор — docs/render.md)",
			}
		}
		return decision{
			AlertStatusFiring,
			detail + " — глубина стационарно у порога без роста: воркер B стоит либо очередь упёрлась в предел MAXLEN стрима (XADD вытесняет недоставленное), разбор — docs/render.md",
		}
	case t.mem.firing && n >= recoverAt:
		return decision{
			AlertStatusFiring,
			detail + " — глубина держится выше порога снятия (темп/глубина — панель движков дашборда)",
		}
	default:
		t.mem.firing = false
		return decision{AlertStatusOK, detail}
	}
}

// ------------------------------------------------------------- delta ring

// deltaRing — a delta ring of the counter between passes (the window memory):
// the capacity size; 0 — AlertBadHashesWindow (the historical behavior
// of the junk hash spike). The first observation only bases prev —
// the monitoring start later than the accumulated counter is not retrospective (the
// window counts new events, not the whole history); a counter decrease (an owner
// restart) yields a zero delta — the counter starts anew, no false
// spike.
type deltaRing struct {
	prev   int64
	seen   bool
	deltas []int64
	i      int
	// size — the ring capacity in passes; 0 — AlertBadHashesWindow.
	size int
}

// push takes the current counter value and returns the window sum.
func (w *deltaRing) push(cur int64) int64 {
	var d int64
	if !w.seen {
		w.seen = true // the base: what accumulated before the monitoring start does not count
		// The equivalent mutant: at cur == prev both branches give a delta of 0.
		// mutator-disable-next-line expression/comparison
	} else if cur > w.prev {
		d = cur - w.prev
	}
	w.prev = cur
	if w.deltas == nil {
		size := w.size
		if size <= 0 {
			size = AlertBadHashesWindow
		}
		w.deltas = make([]int64, size)
	}
	w.deltas[w.i] = d
	// The equivalent mutant of a +2 step: 2 is coprime with the window length 15 —
	// the ring contains the last 15 deltas in both cases; the window sum
	// is indistinguishable by the test.
	// mutator-disable-next-line numbers/incrementer
	w.i = (w.i + 1) % len(w.deltas)
	var sum int64
	for _, d := range w.deltas {
		sum += d
	}
	return sum
}

// ringTrack — an events window in passes (a delta ring): the sum of the counter
// deltas over the window ≥ the threshold — firing; clearing — by aging (the window
// holds the fired state only on a continuing event flow); no hysteresis is needed
// against flapping — the window sum decreases only by evicting deltas, and
// near the threshold it holds only while the flow continues. A threshold of 1 turns
// the ring into "any event within the window" (the fallback share budget: every
// suspension latching is an anomaly, the NVMe wear style).
type ringTrack struct {
	ring      *deltaRing
	threshold int64
	// passes — the window in passes, and interval — the alert period: only for the
	// detail (a human-readable window duration).
	passes   int
	interval time.Duration
	// label — the measurement name in the detail ("counter", "budget events"),
	// fireNote — the suffix of the firing detail.
	label    string
	fireNote string
}

// decide — the ring transition: the window sum against the threshold.
func (t ringTrack) decide(_ string, _ time.Time, cur int64) decision {
	sum := t.ring.push(cur)
	detail := fmt.Sprintf("%s %d, новых за окно %s — %d (порог %d)",
		t.label, cur, (time.Duration(t.passes) * t.interval).Round(time.Second), sum, t.threshold)
	if sum >= t.threshold {
		return decision{AlertStatusFiring, detail + t.fireNote}
	}
	return decision{AlertStatusOK, detail}
}

// --------------------------------------------------------------- time window

// psiWindowTrack — the PSI CPU hold window (psi_cpu, T-162): avg300 above
// the threshold sustained over the window — firing. The first pass above the
// threshold starts the window, firing on its close; avg300 itself smooths
// over 5 minutes; the window discards one-off spikes (a deploy, retro batches).
// No hysteresis needed: to "clear", avg300 must fall to the threshold
// after 30+ minutes above — an actual recovery; the window reset on
// a brief dip is more conservative than a hold (a spike does not extend
// the alert).
type psiWindowTrack struct {
	percent float64
	window  time.Duration
	hot     time.Time
}

// decide — the PSI window transition.
func (t *psiWindowTrack) decide(_ string, now time.Time, st PSIStat) decision {
	over := st.Avg300 > t.percent
	switch {
	case !over:
		t.hot = time.Time{}
	case t.hot.IsZero():
		t.hot = now
	}
	detail := fmt.Sprintf("some avg10=%.2f%% avg60=%.2f%% avg300=%.2f%%, порог %.0f%% (окно %s)",
		st.Avg10, st.Avg60, st.Avg300, t.percent, t.window)
	if over {
		held := now.Sub(t.hot)
		if held >= t.window {
			return decision{
				AlertStatusFiring,
				detail + fmt.Sprintf(" — выше порога уже %s", held.Round(time.Second)),
			}
		}
		return decision{
			AlertStatusOK,
			detail + fmt.Sprintf(" — выше порога %s из окна, накопление", held.Round(time.Second)),
		}
	}
	return decision{AlertStatusOK, detail}
}

// freezeEntry — the memory of one stream group: the last seen
// last-delivered-id and the moment it froze at. since lives only
// at depth > 0 — the idle time of an empty queue does not accumulate "time
// without deliveries" (an hour of idle plus the first unread entry would give an instant
// false firing).
type freezeEntry struct {
	lastID string
	since  time.Time
}

// queueFreezeTrack — the group immobility window (queue_frozen, T-162):
// the last-delivered-id of a working stream group does not move ≥ the window at
// depth > 0. A live consumer moves the ID with every XREADGROUP (batches in
// seconds); a standing ID at a non-empty backlog — entries are not
// delivered at all: the consumer is dead or crashes on the first entry (the T-159 poison),
// lease recovery does not raise it. The first observation of a group — the base from
// the current moment (after a monitor restart the alert fires no earlier than
// the window, not retrospectively); an ID movement updates the moment; depth 0 —
// the entry is cleaned out; a group vanished from the answer (the stream deleted) — does not accumulate.
// Clearing — the first pass without frozen groups; no hysteresis needed,
// the window is the natural flap filter.
type queueFreezeTrack struct {
	window time.Duration
	groups map[string]freezeEntry
}

// decide — the group freeze window transition.
func (t *queueFreezeTrack) decide(_ string, now time.Time, groups []StreamGroupState) decision {
	seen := make(map[string]bool, len(groups))
	var frozen []string
	var moving int
	for _, g := range groups {
		key := g.Stream + "/" + g.Group
		seen[key] = true
		if g.Depth <= 0 {
			delete(t.groups, key) // the work is over — nothing to accumulate
			moving++
			continue
		}
		tr, ok := t.groups[key]
		if !ok || g.LastDeliveredID != tr.lastID {
			t.groups[key] = freezeEntry{lastID: g.LastDeliveredID, since: now}
			moving++
			continue
		}
		idle := now.Sub(tr.since)
		if idle >= t.window {
			frozen = append(frozen, fmt.Sprintf("%s/%s: доставки нет %s при глубине %d (последний ID %s)",
				g.Stream, g.Group, idle.Round(time.Second), g.Depth, g.LastDeliveredID))
		}
	}
	for key := range t.groups { // the group vanished (the stream deleted) — do not accumulate
		if !seen[key] {
			delete(t.groups, key)
		}
	}
	if len(frozen) > 0 {
		return decision{
			AlertStatusFiring,
			strings.Join(frozen, "; ") + " — потребитель не читает: лог юнита-воркера и q:dead (яд T-159), перезапуск воркера",
		}
	}
	return decision{
		AlertStatusOK,
		fmt.Sprintf("групп с движением/без backlog — %d из %d (окно %s)",
			moving, len(groups), t.window),
	}
}

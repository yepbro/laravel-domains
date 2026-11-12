// The mutant triage tests of the CI run of Nov 09, 2026 (half A): the finishing
// of alerts.go/alertstate.go — the pins of the withDefaults defaults and boundaries (0 →
// default, 1 — a working value), the alert machine boundaries (the depth detail
// exactly at the watermark/hold threshold, a threshold ≥ the volume capacity, the exact
// free percentage, the observation of the depths of all streams, the ceil boundary
// of the render latches, the new_done_stale series by snapshot stamps, the PSI window at
// the percentage boundary, the delta ring, a q:dead depth of 1, the deletion of vanished
// units from the memory) and the direct boundaries of the track memories.
package monitor

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"crawler/internal/queue"
)

// TestAlertsDefaultsPins — the pins of the default constants (the values are the
// contract of the operator's configuration).
func TestAlertsDefaultsPins(t *testing.T) {
	if DefaultAlertsInterval != 5*time.Minute {
		t.Errorf("DefaultAlertsInterval = %v, want 5m", DefaultAlertsInterval)
	}
	if DefaultQueueFreezeWindow != 15*time.Minute {
		t.Errorf("DefaultQueueFreezeWindow = %v, want 15m", DefaultQueueFreezeWindow)
	}
	if DefaultPSICPUPercent != 90.0 {
		t.Errorf("DefaultPSICPUPercent = %v, want 90", DefaultPSICPUPercent)
	}
	if DefaultPSIWindow != 30*time.Minute {
		t.Errorf("DefaultPSIWindow = %v, want 30m", DefaultPSIWindow)
	}
	if DefaultMemHighMinPasses != 2 {
		t.Errorf("DefaultMemHighMinPasses = %d, want 2", DefaultMemHighMinPasses)
	}
	if DefaultRenderDebtDomains != 50000 {
		t.Errorf("DefaultRenderDebtDomains = %d, want 50000", DefaultRenderDebtDomains)
	}
	if DefaultRenderDebtPasses != 2 {
		t.Errorf("DefaultRenderDebtPasses = %d, want 2", DefaultRenderDebtPasses)
	}
}

// TestAlertsWithDefaultsBoundaries — zeros and negatives are replaced
// by defaults, unit values are working values (the "≤ 0" boundaries).
func TestAlertsWithDefaultsBoundaries(t *testing.T) {
	o := AlertsOptions{}.withDefaults()
	if o.QueueFreeze != DefaultQueueFreezeWindow || o.PSICPUPercent != DefaultPSICPUPercent ||
		o.RenderDebtPasses != DefaultRenderDebtPasses || o.MemHighMinPasses != DefaultMemHighMinPasses ||
		o.RenderDebtDomains != DefaultRenderDebtDomains || o.PSIWindow != DefaultPSIWindow ||
		o.Interval != DefaultAlertsInterval || o.ZoneStatsInterval != DefaultZoneStatsInterval ||
		o.DiskFreeMinBytes != DefaultDiskFreeMinBytes || o.DiskFreeCritBytes != DefaultDiskFreeCritBytes {
		t.Errorf("нулевые опции не дефолты: %+v", o)
	}
	// Unit values are working values, not defaults (1ns > 0 — the "≤ 0" boundary
	// is distinguishable in both directions).
	o = AlertsOptions{
		QueueFreeze: time.Nanosecond, PSICPUPercent: 1, RenderDebtPasses: 1,
		MemHighMinPasses: 1, RenderDebtDomains: 1, PSIWindow: time.Nanosecond,
		Interval: time.Nanosecond, ZoneStatsInterval: time.Nanosecond,
		DiskFreeMinBytes: 1, DiskFreeCritBytes: 1,
	}.withDefaults()
	if o.QueueFreeze != time.Nanosecond || o.PSICPUPercent != 1 || o.RenderDebtPasses != 1 ||
		o.MemHighMinPasses != 1 || o.RenderDebtDomains != 1 || o.PSIWindow != time.Nanosecond ||
		o.Interval != time.Nanosecond || o.ZoneStatsInterval != time.Nanosecond ||
		o.DiskFreeMinBytes != 1 || o.DiskFreeCritBytes != 1 {
		t.Errorf("единичные значения затёрты дефолтами: %+v", o)
	}
}

// TestAlertsQueueHotDetailBoundary — the fired detail: a stream exactly at
// the watermark is hot (the detail names it, not the hold corridor);
// a stream exactly at the hold threshold is a corridor; the detail, when there are no
// hot ones, names it.
func TestAlertsQueueHotDetailBoundary(t *testing.T) {
	// Hot A exactly at the watermark, B in the corridor: the detail names
	// only A (a ">" mutant would send A to the corridor, the detail — both).
	env := newAlertsTestEnv(t, nil)
	a, b := alertStreams[0], alertStreams[1]
	wmA := queue.StreamWatermark(env.opts.QueueWatermark, env.opts.QueueMaxLen, a)
	wmB := queue.StreamWatermark(env.opts.QueueWatermark, env.opts.QueueMaxLen, b)
	holdB := int64(float64(wmB) * AlertQueueRecoverFactor)
	if holdB >= wmB {
		t.Skip("коридор удержания совпадает с watermark — граница неотличима")
	}
	m := env.newQueuesMachine()
	env.lener.lens = map[string]int64{a: wmA, b: holdB}
	s := m.transition(env.Alerts, context.Background(), obs(env.lener.lens))
	if s.Status != AlertStatusFiring {
		t.Fatalf("глубина у watermark: %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, a+"="+strconv.FormatInt(wmA, 10)+" (порог") {
		t.Errorf("горячий стрим не назван: %s", s.Detail)
	}
	if strings.Contains(s.Detail, b+"=") {
		t.Errorf("коридорный стрим затопил горячий: %s", s.Detail)
	}

	// No hot ones, B exactly at the hold threshold — the detail names B
	// (a ">" mutant in holdAt would lose it).
	env2 := newAlertsTestEnv(t, nil)
	m2 := env2.newQueuesMachine()
	// First a firing, then a hold exactly in the corridor.
	env2.lener.lens = map[string]int64{b: wmB}
	_ = m2.transition(env2.Alerts, context.Background(), obs(env2.lener.lens))
	env2.lener.lens = map[string]int64{b: holdB}
	s2 := m2.transition(env2.Alerts, context.Background(), obs(env2.lener.lens))
	if s2.Status != AlertStatusFiring {
		t.Fatalf("удержание в коридоре: %s (%s), want firing", s2.Status, s2.Detail)
	}
	if !strings.Contains(s2.Detail, b+"="+strconv.FormatInt(holdB, 10)+" (порог") {
		t.Errorf("коридорный стрим не назван: %s", s2.Detail)
	}
}

// TestAlertsDiskBytesNABoundary — an absolute minimum threshold exactly
// the volume capacity — inapplicability; one less — applicable.
func TestAlertsDiskBytesNABoundary(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	total := int64(200 << 30)
	env.space.stat = SpaceStat{Avail: 100 << 30, Total: uint64(total)}
	mm := env.newDiskBytesMachine(AlertDiskMin, total)
	s := mm.transition(env.Alerts, context.Background(), obs(env.space.stat))
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "неприменим") {
		t.Errorf("порог = ёмкости: %s (%s), want not_measured/неприменим", s.Status, s.Detail)
	}
	mm2 := env.newDiskBytesMachine(AlertDiskMin, total-1)
	s2 := mm2.transition(env.Alerts, context.Background(), obs(env.space.stat))
	if s2.Status == AlertStatusNotMeasured {
		t.Errorf("порог < ёмкости: %s (%s), want измеряется", s2.Status, s2.Detail)
	}
}

// TestAlertsDiskPercentDetail — the exact percentage of free space in the
// details (100 × avail / total).
func TestAlertsDiskPercentDetail(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newDiskMachine()
	env.space.stat = SpaceStat{Avail: 50 << 30, Total: 200 << 30} // exactly 25%
	s := m.transition(env.Alerts, context.Background(), obs(env.space.stat))
	if !strings.Contains(s.Detail, "свободно 25.0%") {
		t.Errorf("деталь процента = %q, want «свободно 25.0%%»", s.Detail)
	}
}

// TestAlertsQueueLensObserved — the queue observation returns ALL
// streams with depths (a map-write-cut mutant swallowed streams).
func TestAlertsQueueLensObserved(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	lens := map[string]int64{}
	for i, s := range alertStreams {
		lens[s] = int64(i + 1)
	}
	env.lener.lens = lens
	o := env.observeQueues(context.Background())
	if o.err != nil || o.reason != "" {
		t.Fatalf("наблюдение: %+v", o)
	}
	for _, s := range alertStreams {
		if o.value[s] != lens[s] {
			t.Errorf("lens[%s] = %d, want %d", s, o.value[s], lens[s])
		}
	}
}

// TestAlertsRedisMemoryMaxBytesBoundary — maxmemory = 1 is a valid
// observation (there is a cap); = 0 — a quiet inapplicability.
func TestAlertsRedisMemoryMaxBytesBoundary(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.redisMem.mem = RedisMemory{UsedBytes: 1, MaxBytes: 1, Policy: "noeviction"}
	o := env.observeRedisMemory(context.Background())
	if o.err != nil || o.reason != "" {
		t.Errorf("maxmemory=1: %+v, want измерение", o)
	}
	env.redisMem.mem = RedisMemory{UsedBytes: 1, MaxBytes: 0, Policy: "noeviction"}
	o = env.observeRedisMemory(context.Background())
	if o.reason == "" {
		t.Errorf("maxmemory=0: %+v, want неприменимость", o)
	}
}

// TestAlertsRenderFbDepthWatermarkBoundary — QueueWatermark=1 is a working
// value (the depth observation is alive, not "the threshold is not set").
func TestAlertsRenderFbDepthWatermarkBoundary(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 1 })
	o := env.observeRenderFallbackDepth(context.Background())
	if o.err != nil || o.reason != "" {
		t.Errorf("QueueWatermark=1: %+v, want измерение", o)
	}
}

// TestAlertsDiskPercentEnabledBoundary — DiskFreePercent=1 — the check
// is on (a measurement, not "off").
func TestAlertsDiskPercentEnabledBoundary(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.DiskFreePercent = 1 })
	o := env.observeDisk(context.Background())
	if o.err != nil || o.reason != "" {
		t.Errorf("DiskFreePercent=1: %+v, want измерение", o)
	}
}

// TestDeadStreakNOne — a depth of 1 is a full series event (the
// n > 0 boundary); the fired state is held by any non-zero depth.
func TestDeadStreakNOne(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newDeadMachine()
	got := runSequence(env.Alerts, m, obs(int64(1)), obs(int64(1)), obs(int64(1)))
	wantSequence(t, "серия единиц", got, []string{AlertStatusOK, AlertStatusOK, AlertStatusFiring})
	got = runSequence(env.Alerts, m, obs(int64(1)), obs(int64(0)))
	wantSequence(t, "удержание единицей", got, []string{AlertStatusFiring, AlertStatusOK})
}

// TestMemHighStreakGrewAndEvict — a High growth builds the series with the
// "+N per pass" detail (the exact delta); stagnation — clearing; a vanished
// unit leaves the memory, a return starts the series anew.
func TestMemHighStreakGrewAndEvict(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newMemHighMachine()
	_ = m.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 100}}))
	_ = m.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 130}}))
	s3 := m.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 170}}))
	if s3.Status != AlertStatusFiring || !strings.Contains(s3.Detail, "+40 за проход") {
		t.Errorf("серия роста: %s (%s), want firing с «+40 за проход»", s3.Status, s3.Detail)
	}
	s4 := m.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 170}}))
	if s4.Status != AlertStatusOK {
		t.Errorf("стагнация: %s, want ok", s4.Status)
	}
	// The unit vanished and returned with growth — a fresh base: no series (a
	// no-cleanup mutant would continue the series and fire falsely).
	m2 := env.newMemHighMachine()
	_ = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 100}}))
	_ = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 150}})) // series 1
	_ = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{}))                       // the unit vanished
	s := m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 160}}))
	if s.Status != AlertStatusOK {
		t.Errorf("после исчезновения: %s (%s), want ok (свежая база, серия с нуля)", s.Status, s.Detail)
	}
	// The series after the return is counted anew: two growths — firing.
	s = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 200}}))
	s = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 260}}))
	if s.Status != AlertStatusFiring {
		t.Errorf("новая серия после исчезновения не сработала: %s (%s)", s.Status, s.Detail)
	}
}

// TestRenderDebtTrackBoundaries — the firing threshold is strictly greater,
// the hold from the latch at the clearing threshold (ceil), the clearing below; stagnation
// without growth does not fire.
func TestRenderDebtTrackBoundaries(t *testing.T) {
	d := func(total int64) RenderDebt { return RenderDebt{Flags: total} }
	// A threshold of 10, a clearing < 9 (ceil): a level of 9 holds, 8 clears.
	tr := &renderDebtTrack{domains: 10, passes: 2}
	if s := tr.decide("", time.Time{}, d(8)); s.status != AlertStatusOK {
		t.Errorf("база: %s", s.status)
	}
	if s := tr.decide("", time.Time{}, d(9)); s.status != AlertStatusOK {
		t.Errorf("рост до 9: %s, want ok (9 < 10)", s.status)
	}
	if s := tr.decide("", time.Time{}, d(12)); s.status != AlertStatusFiring {
		t.Errorf("рост выше порога: %s, want firing", s.status)
	}
	if s := tr.decide("", time.Time{}, d(9)); s.status != AlertStatusFiring {
		t.Errorf("удержание на 9: %s (%s), want firing", s.status, s.detail)
	}
	if s := tr.decide("", time.Time{}, d(8)); s.status != AlertStatusOK {
		t.Errorf("снятие на 8: %s, want ok", s.status)
	}
	// Stagnation above the threshold without growth — ok.
	tr2 := &renderDebtTrack{domains: 10, passes: 2}
	_ = tr2.decide("", time.Time{}, d(5))
	if s := tr2.decide("", time.Time{}, d(20)); s.status != AlertStatusOK {
		t.Errorf("первый проход выше порога: %s, want ok (серия 1)", s.status)
	}
	if s := tr2.decide("", time.Time{}, d(20)); s.status != AlertStatusOK {
		t.Errorf("стагнация: %s, want ok (нет роста)", s.status)
	}
}

// TestRenderFbDepthBoundaries — a depth exactly at the watermark — a hot
// series, firing by a stationary depth; the hold of the ceil clearing threshold.
func TestRenderFbDepthBoundaries(t *testing.T) {
	tr := &renderFbDepthTrack{wm: 100, passes: 2}
	if s := tr.decide("", time.Time{}, 100); s.status != AlertStatusOK {
		t.Errorf("первый у порога: %s, want ok", s.status)
	}
	s := tr.decide("", time.Time{}, 100)
	if s.status != AlertStatusFiring || !strings.Contains(s.detail, "стационарно у порога") {
		t.Errorf("стационарная глубина: %s (%s), want firing «стационарно»", s.status, s.detail)
	}
	if s := tr.decide("", time.Time{}, 90); s.status != AlertStatusFiring {
		t.Errorf("удержание на 90: %s (%s), want firing", s.status, s.detail)
	}
	if s := tr.decide("", time.Time{}, 89); s.status != AlertStatusOK {
		t.Errorf("снятие на 89: %s, want ok", s.status)
	}
	// 99 is not "at the threshold": hot does not accumulate.
	tr2 := &renderFbDepthTrack{wm: 100, passes: 2}
	_ = tr2.decide("", time.Time{}, 99)
	if s := tr2.decide("", time.Time{}, 99); s.status != AlertStatusOK {
		t.Errorf("99 два прохода: %s (%s), want ok", s.status, s.detail)
	}
}

// TestNewDoneStaleSnapshotBoundaries — the series grows only on new
// snapshots (a stamp change); a row repeat — no; an invalid stamp —
// growth; a zero snapshot resets; the fired state is held by a value of one.
func TestNewDoneStaleSnapshotBoundaries(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	row := func(v int64, at time.Time) staleRow {
		return staleRow{value: v, snap: sql.NullTime{Time: at, Valid: true}}
	}
	tr := &newDoneStaleTrack{minPasses: 2}
	if s := tr.decide("", time.Time{}, row(5, t0)); !strings.Contains(s.detail, "1/2") {
		t.Errorf("первый снапшот: %s", s.detail)
	}
	if s := tr.decide("", time.Time{}, row(5, t0)); !strings.Contains(s.detail, "1/2") {
		t.Errorf("повтор снапшота нарастил серию: %s", s.detail)
	}
	if s := tr.decide("", time.Time{}, row(5, t0.Add(time.Minute))); s.status != AlertStatusFiring {
		t.Errorf("новый снапшот: %s (%s), want firing", s.status, s.detail)
	}
	// An invalid stamp — series growth (the first pass of such a row).
	tr2 := &newDoneStaleTrack{minPasses: 2}
	_ = tr2.decide("", time.Time{}, row(5, t0))
	if s := tr2.decide("", time.Time{}, staleRow{value: 5}); !strings.Contains(s.detail, "2/2") {
		t.Errorf("невалидная метка не нарастила серию: %s", s.detail)
	}
	// The fired state is held by a value of one; a zero snapshot clears.
	tr3 := &newDoneStaleTrack{minPasses: 2}
	_ = tr3.decide("", time.Time{}, row(5, t0))
	_ = tr3.decide("", time.Time{}, row(5, t0.Add(time.Minute)))
	if s := tr3.decide(AlertStatusFiring, time.Time{}, row(1, t0.Add(2*time.Minute))); s.status != AlertStatusFiring {
		t.Errorf("удержание единицей: %s, want firing", s.status)
	}
	if s := tr3.decide(AlertStatusFiring, time.Time{}, row(0, t0.Add(2*time.Minute))); s.status != AlertStatusOK {
		t.Errorf("нулевой снапшот: %s, want ok", s.status)
	}
}

// TestPsiWindowBoundary — avg300 exactly at the threshold is not "above" (a strict
// >); slightly above — the window accumulates and closes with firing.
func TestPsiWindowBoundary(t *testing.T) {
	base := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tr := &psiWindowTrack{percent: 90, window: 30 * time.Minute}
	s := tr.decide("", base, PSIStat{Avg10: 90, Avg60: 90, Avg300: 90})
	if s.status != AlertStatusOK || strings.Contains(s.detail, "выше порога") {
		t.Errorf("ровно на пороге: %s (%s), want ok без накопления", s.status, s.detail)
	}
	tr2 := &psiWindowTrack{percent: 90, window: 30 * time.Minute}
	_ = tr2.decide("", base, PSIStat{Avg300: 90.01})
	s = tr2.decide("", base.Add(31*time.Minute), PSIStat{Avg300: 90.01})
	if s.status != AlertStatusFiring {
		t.Errorf("окно закрылось: %s (%s), want firing", s.status, s.detail)
	}
}

// TestDeltaRingSizeOneAndValues — a ring of size 1 — a one-pass
// window; the sum — the exact delta; a restart — a zero delta.
func TestDeltaRingSizeOneAndValues(t *testing.T) {
	w := &deltaRing{size: 1}
	if got := w.push(100); got != 0 {
		t.Errorf("база: сумма = %d, want 0", got)
	}
	if got := w.push(130); got != 30 {
		t.Errorf("окно 1: сумма = %d, want 30", got)
	}
	if got := w.push(10); got != 0 {
		t.Errorf("рестарт: сумма = %d, want 0", got)
	}
	if got := w.push(15); got != 5 {
		t.Errorf("после рестарта: сумма = %d, want 5", got)
	}
}

// TestQueueFreezeDepthZero — a zero depth does not accumulate immobility:
// however many passes — ok; a depth of 1 is full window operation (the
// "> 0" boundary); the ok detail names the counter of moved/empty groups.
func TestQueueFreezeDepthZero(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newQueueFreezeMachine()
	zero := []StreamGroupState{{Stream: "q:dns", Group: "g", Depth: 0, LastDeliveredID: "1-1"}}
	for i := 0; i < 6; i++ {
		s := m.transition(env.Alerts, context.Background(), obs(zero))
		if s.Status != AlertStatusOK {
			t.Errorf("глубина 0, проход %d: %s (%s), want ok", i, s.Status, s.Detail)
		}
		if !strings.Contains(s.Detail, "1 из 1") {
			t.Errorf("деталь окна: %q, want счётчик «1 из 1» (пустая группа — движимая)", s.Detail)
		}
	}

	// A depth of 1 immobile over the window — firing (the minimal live depth).
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	env2 := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.QueueFreeze = 15 * time.Minute
		o.Now = func() time.Time { return now }
	})
	m2 := env2.newQueueFreezeMachine()
	one := []StreamGroupState{{Stream: "q:dns", Group: "g", Depth: 1, LastDeliveredID: "1-1"}}
	if s := m2.transition(env2.Alerts, context.Background(), obs(one)); s.Status != AlertStatusOK {
		t.Fatalf("глубина 1, база: %s", s.Status)
	}
	now = now.Add(16 * time.Minute)
	s := m2.transition(env2.Alerts, context.Background(), obs(one))
	if s.Status != AlertStatusFiring {
		t.Errorf("глубина 1 без движения окно: %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "глубине 1") {
		t.Errorf("деталь заморозки: %q, want «глубине 1»", s.Detail)
	}
}

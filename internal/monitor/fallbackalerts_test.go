// Unit tests of the render fallback queue alerts (T-246, render spec §4.6
// item 3): the growth of the q:render-fallback depth (the threshold — the effective
// watermark of the stream, the render_debt mechanics: a level + a growth/at-threshold
// series + a latch), exceeding the share budget (a delta window of the event
// counter suspended{reason="budget"} of the render worker /metrics probe, suspends.go
// — the parser and the degradations) and the growth of the stage browser errors (S-P2-6,
// a rate detector over the counter
// crawler_stage_errors_total{stage="render",type="browser"} of the same
// probe). Fake sources, the style of the renderdebt/badhashes tests.
package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"crawler/internal/queue"
)

// fakeBudgetEvents — a share budget event counter source per a stub
// (the fakeBadHashes style): the value changes between passes, the error
// queue is empty — an answer without an error.
type fakeBudgetEvents struct {
	mu    sync.Mutex
	val   int64
	errs  []error
	calls int
}

func (f *fakeBudgetEvents) set(v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.val = v
}

func (f *fakeBudgetEvents) FallbackBudget(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := popErr(&f.errs); err != nil {
		return 0, err
	}
	return f.val, nil
}

// --- the q:render-fallback depth growth -----------------------------------------

// setFallbackDepth — a stub of the pass depth: Len(q:render-fallback)
// reads the lener map (the same for all subsequent passes; the test
// sets the value per step).
func setFallbackDepth(env *alertsTestEnv, depth int64) {
	env.lener.lens[queue.StreamRenderFallback] = depth
}

// TestAlertsRenderFallbackDepthGrowth — firing by growth: the depth
// grows for 2 passes in a row, crossing the threshold; holding by
// stagnation (the latch), clearing by a drain below 90% of the threshold; a one-off publication
// spike (a jump that dissolved before the next pass) does not wake the operator.
// A stationary depth at the threshold — the second trigger, a separate test below
// (TestAlertsRenderFallbackDepthStationary).
func TestAlertsRenderFallbackDepthGrowth(t *testing.T) {
	// A threshold of 1000 (the watermark from the test environment default; the
	// stream's MAXLEN limit is higher — without a clamp), clearing < 900.
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRenderFbDepthMachine()

	// The base and growth below the threshold: the growth series accumulates; the threshold is
	// not yet engaged.
	s := m.transition(env.Alerts, ctx, obs(int64(500)))
	if s.Status != AlertStatusOK {
		t.Fatalf("база: %s (%s), want ok", s.Status, s.Detail)
	}
	for _, want := range []string{"q:render-fallback=500", "порог 1000", "снятие < 900", "у порога 0/2"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// One growth pass — the series is short (a one-off publication spike is
	// normal fallback operation of "collect and finish").
	s = m.transition(env.Alerts, ctx, obs(int64(800)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Fatalf("один проход роста: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	// The second growth pass with a threshold crossing — firing.
	s = m.transition(env.Alerts, ctx, obs(int64(1420)))
	if s.Status != AlertStatusFiring {
		t.Fatalf("второй проход роста: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"q:render-fallback=1420", "+620", "воркера B"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// Stagnation above the clearing threshold — a hold (not a repeated Warn;
	// one episode).
	if s := m.transition(env.Alerts, ctx, obs(int64(1410))); s.Status != AlertStatusFiring {
		t.Errorf("стагнация: %s (%s), want firing (удержание)", s.Status, s.Detail)
	}
	// A drain below 90% of the threshold — clearing.
	if s := m.transition(env.Alerts, ctx, obs(int64(890))); s.Status != AlertStatusOK {
		t.Errorf("дренаж: %s (%s), want ok", s.Status, s.Detail)
	}
	// The hysteresis corridor (900..1000) without a fired alert — ok: the firing
	// threshold is strict; growth in the corridor builds the series.
	if s := m.transition(env.Alerts, ctx, obs(int64(950))); s.Status != AlertStatusOK {
		t.Errorf("коридор без алерта: %s (%s), want ok", s.Status, s.Detail)
	}
	// A dip resets the series; re-accumulation is again two
	// growth passes (growth through the threshold is the same accumulation).
	_ = m.transition(env.Alerts, ctx, obs(int64(800))) // streak 0
	s = m.transition(env.Alerts, ctx, obs(int64(1010)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Errorf("первый проход повторного роста: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, obs(int64(1060))); s.Status != AlertStatusFiring {
		t.Errorf("второй проход повторного роста: %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsRenderFallbackDepthStationary — the second trigger (S-P2-1,
// the operator's decision): a depth ≥ the threshold for 2 passes in a row WITHOUT growth —
// firing from the 2nd pass (the growth series is based at zero and does not see the
// stationary depth — a monitor restart in the middle of an episode no longer mutes
// the alert); pinning at the MAXLEN cap — firing and a hold (XADD evicts the
// oldest undelivered entries, the depth stands); a one-off spike dissolved
// between passes — silent (anti-spike); the latch/clearing
// are the same.
func TestAlertsRenderFallbackDepthStationary(t *testing.T) {
	// The threshold — a clamp at half of the q:render-fallback MAXLEN (15,000/2 =
	// 7,500), as at the default watermark of 100k.
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 100000 })
	ctx := context.Background()
	m := env.newRenderFbDepthMachine()

	// A stationary depth of 9000 without growth: the first pass — the
	// base (at the threshold 1/2), the second — firing.
	s := m.transition(env.Alerts, ctx, obs(int64(9000)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "у порога 1/2") {
		t.Fatalf("стационарная база: %s (%s), want ok с серией у порога 1/2", s.Status, s.Detail)
	}
	s = m.transition(env.Alerts, ctx, obs(int64(9000)))
	if s.Status != AlertStatusFiring {
		t.Fatalf("стационарная 9000: %s (%s), want firing со 2-го прохода", s.Status, s.Detail)
	}
	for _, want := range []string{"q:render-fallback=9000", "порог 7500", "снятие < 6750", "серия роста 0/2", "у порога 2/2", "стационарно"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}

	// Pinning at the MAXLEN cap: the depth does not grow (the trim evicts
	// the undelivered) — firing and a hold.
	if s := m.transition(env.Alerts, ctx, obs(int64(15000))); s.Status != AlertStatusFiring {
		t.Errorf("упор в MAXLEN: %s (%s), want firing", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, obs(int64(15000))); s.Status != AlertStatusFiring {
		t.Errorf("упор в MAXLEN, второй проход: %s (%s), want firing (удержание)", s.Status, s.Detail)
	}

	// A drain below 90% of the threshold — clearing (the latch is not eternal).
	if s := m.transition(env.Alerts, ctx, obs(int64(6000))); s.Status != AlertStatusOK {
		t.Errorf("дренаж: %s (%s), want ok", s.Status, s.Detail)
	}

	// A one-off spike between the passes (dissolving) — silent: the
	// at-threshold series does not build up; one growth pass is not a trigger.
	_ = m.transition(env.Alerts, ctx, obs(int64(9000))) // at the threshold 1/2, ok
	_ = m.transition(env.Alerts, ctx, obs(int64(100)))  // dissolved, ok
	s = m.transition(env.Alerts, ctx, obs(int64(9000)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "у порога 1/2") {
		t.Errorf("одиночный всплеск: %s (%s), want ok с серией у порога 1/2 (анти-спайк)", s.Status, s.Detail)
	}
}

// TestAlertsRenderFallbackDepthWatermark — the threshold from the configuration
// following the q:render model: the effective watermark of the stream
// queue.StreamWatermark(scheduler.queue_high_watermark, redis.maxlen,
// q:render-fallback) — a clamp at half of the MAXLEN limit (the T-025 policy:
// q:render-fallback — 15,000 → 7,500 at the default 100k).
func TestAlertsRenderFallbackDepthWatermark(t *testing.T) {
	cases := []struct {
		name      string
		watermark int64
		maxLen    int64
		want      int64
	}{
		{"дефолт конфигурации: кламп по MAXLEN/2", 100000, 0, 7500},
		{"заниженный watermark ниже предела", 5000, 0, 5000},
		{"нестандартный MaxLen: кламп по его половине", 100000, 300, 150},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newAlertsTestEnv(t, func(o *AlertsOptions) {
				o.QueueWatermark = tc.watermark
				o.QueueMaxLen = tc.maxLen
			})
			ctx := context.Background()
			m := env.newRenderFbDepthMachine()
			// A growth series by two passes at a depth exactly the threshold.
			_ = m.transition(env.Alerts, ctx, obs(tc.want+10)) // the base
			_ = m.transition(env.Alerts, ctx, obs(tc.want+20)) // growth 1
			s := m.transition(env.Alerts, ctx, obs(tc.want+30))
			if s.Status != AlertStatusFiring {
				t.Fatalf("глубина %d > порога %d: %s (%s), want firing",
					tc.want+30, tc.want, s.Status, s.Detail)
			}
			if got := queue.StreamWatermark(tc.watermark, tc.maxLen, queue.StreamRenderFallback); got != tc.want {
				t.Errorf("StreamWatermark = %d, want %d (порог алерта — единый расчёт)", got, tc.want)
			}
			if !strings.Contains(s.Detail, "порог ") || !strings.Contains(s.Detail, "серия роста 2/2") {
				t.Errorf("detail = %q, want порог и готовую серию роста", s.Detail)
			}
		})
	}
	// A depth exactly the threshold: a single pass — not firing (anti-spike:
	// the spike must survive the second pass); stationary — firing from
	// the second pass (S-P2-1: equality is already "at the threshold"; an
	// at-threshold series without growth is a full trigger); growth with a
	// threshold crossing — firing.
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 1000 })
	ctx := context.Background()
	m := env.newRenderFbDepthMachine()
	_ = m.transition(env.Alerts, ctx, obs(int64(1000))) // the base (at the threshold 1/2)
	if s := m.transition(env.Alerts, ctx, obs(int64(1000))); s.Status != AlertStatusFiring {
		t.Errorf("глубина == порогу без роста два прохода: %s (%s), want firing (стационарно у порога, S-P2-1)", s.Status, s.Detail)
	}
	_ = m.transition(env.Alerts, ctx, obs(int64(1001))) // growth 1 (hold)
	if s := m.transition(env.Alerts, ctx, obs(int64(1002))); s.Status != AlertStatusFiring {
		t.Errorf("рост через порог: %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsRenderFallbackDepthDegradation — Redis not set, the threshold not
// set, a Len error — "not measured" (an error — with a Warn); the tracking
// memory and the latch of the fired episode survive a source failure (the
// render_debt style).
func TestAlertsRenderFallbackDepthDegradation(t *testing.T) {
	// Redis not set — like the queue alerts.
	env := newAlertsTestEnv(t, nil)
	a := NewAlerts(AlertsDeps{Pool: env.pool}, env.opts)
	if s := snapshotOf(t, a.Pass(context.Background()), AlertRenderFbDepth); s.Status != AlertStatusNotMeasured {
		t.Errorf("Redis не задан: %s, want not_measured", s.Status)
	}

	// The threshold not set (the gate off) — not measured.
	env2 := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 0 })
	if s := snapshotOf(t, env2.Pass(context.Background()), AlertRenderFbDepth); s.Status != AlertStatusNotMeasured {
		t.Errorf("watermark 0: %s, want not_measured", s.Status)
	}

	// A Len error — a Warn; the error queue: before this alert Len is called
	// 6 times (4 streams of the queue alert + q:dead + the q:render of render debt
	// — it is earlier in the NewAlerts machine order); the error is the seventh.
	env3 := newAlertsTestEnv(t, nil)
	env3.lener.errs = []error{nil, nil, nil, nil, nil, nil, errors.New("redis: xinfo")}
	s := snapshotOf(t, env3.Pass(context.Background()), AlertRenderFbDepth)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "redis: xinfo") {
		t.Errorf("ошибка Len: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env3.buf.String(), "level=WARN") {
		t.Errorf("ошибка Len без Warn:\n%s", env3.buf.String())
	}

	// The tracking memory survives the failure: growth is counted from the last
	// successful snapshot; the latch of the fired episode is not broken by the failure.
	env4 := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	setFallbackDepth(env4, 1200)
	_ = snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth) // the base
	setFallbackDepth(env4, 1300)
	_ = snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth) // growth
	setFallbackDepth(env4, 1400)
	if s := snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
		t.Fatalf("до сбоя: %s, want firing", s.Status)
	}
	env4.lener.errs = []error{nil, nil, nil, nil, nil, nil, errors.New("redis: xinfo")}
	if s := snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusNotMeasured {
		t.Fatalf("сбой источника: %s, want not_measured", s.Status)
	}
	setFallbackDepth(env4, 1350) // above the clearing threshold of 900
	if s := snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
		t.Errorf("после сбоя, глубина 1350: %s, want firing (эпизод пережил сбой)", s.Status)
	}
	setFallbackDepth(env4, 100)
	if s := snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusOK {
		t.Errorf("дренаж после сбоя: %s, want ok", s.Status)
	}
}

// --- the share budget exceeding --------------------------------------------------

// TestAlertsRenderFallbackBudgetEpisode — a suspension event (counter
// growth) — firing; the window holds the alert until the delta ages
// (6 passes); an event flow extends it; a worker restart (a counter
// decrease) — without a false spike.
func TestAlertsRenderFallbackBudgetEpisode(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRenderFbBudgetMachine()

	// The base: what accumulated before the monitoring start is not an event.
	s := m.transition(env.Alerts, ctx, obs(int64(3)))
	if s.Status != AlertStatusOK {
		t.Fatalf("база: %s (%s), want ok", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "события бюджета 3") {
		t.Errorf("detail = %q, want значение счётчика", s.Detail)
	}
	// An event — firing (the threshold 1: every latching is an anomaly).
	s = m.transition(env.Alerts, ctx, obs(int64(4)))
	if s.Status != AlertStatusFiring {
		t.Fatalf("событие: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"новых за окно", "1 (порог 1)", "share_percent"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// The window of 6 passes holds the alert by aging: 5 passes without events —
	// still holding; the sixth evicts the delta — clearing.
	for range 5 {
		if got := m.transition(env.Alerts, ctx, obs(int64(4))); got.Status != AlertStatusFiring {
			t.Fatalf("окно держит событие: %s (%s), want firing", got.Status, got.Detail)
		}
	}
	if got := m.transition(env.Alerts, ctx, obs(int64(4))); got.Status != AlertStatusOK {
		t.Fatalf("после старения окна: %s (%s), want ok", got.Status, got.Detail)
	}
	// A worker restart (the counter reset) — a decrease is not a spike.
	if got := m.transition(env.Alerts, ctx, obs(int64(0))); got.Status != AlertStatusOK {
		t.Errorf("рестарт воркера: %s (%s), want ok без всплеска", got.Status, got.Detail)
	}
	// An event flow extends the alert: an event in every pass.
	for i := int64(1); i <= 3; i++ {
		if got := m.transition(env.Alerts, ctx, obs(i)); got.Status != AlertStatusFiring {
			t.Fatalf("поток событий, проход %d: %s, want firing", i, got.Status)
		}
	}
}

// TestAlertsRenderFallbackBudgetPrecreatedSeries — S-P1-4 (the monitor
// part) on top of the T-250 pre-created series: the first budget event 0→1 —
// firing (the first probe pass reads 0 and bases prev at zero — the event
// is no longer lost); a worker restart (a process counter, reset) —
// the first event again fires; a decrease — without a false spike.
func TestAlertsRenderFallbackBudgetPrecreatedSeries(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRenderFbBudgetMachine()

	// The pre-created series: the probe reads 0 (not "not measured"); prev
	// is based at zero.
	if s := m.transition(env.Alerts, ctx, obs(int64(0))); s.Status != AlertStatusOK {
		t.Fatalf("нулевая серия: %s (%s), want ok (предсоздание T-250)", s.Status, s.Detail)
	}
	// The first event 0→1 — firing: the delta is visible, the window holds it.
	if s := m.transition(env.Alerts, ctx, obs(int64(1))); s.Status != AlertStatusFiring {
		t.Fatalf("первое событие 0→1: %s (%s), want firing", s.Status, s.Detail)
	}
	// The window ages: 5 passes hold the event, the sixth evicts — clearing.
	for range 5 {
		if got := m.transition(env.Alerts, ctx, obs(int64(1))); got.Status != AlertStatusFiring {
			t.Fatalf("окно держит событие: %s (%s), want firing", got.Status, got.Detail)
		}
	}
	if got := m.transition(env.Alerts, ctx, obs(int64(1))); got.Status != AlertStatusOK {
		t.Fatalf("после старения окна: %s (%s), want ok", got.Status, got.Detail)
	}
	// A worker restart: a process counter — reset; a decrease is not a spike.
	if got := m.transition(env.Alerts, ctx, obs(int64(0))); got.Status != AlertStatusOK {
		t.Fatalf("рестарт воркера: %s (%s), want ok без всплеска", got.Status, got.Detail)
	}
	// A repeated first event (0→1 of the new process) — firing again.
	if got := m.transition(env.Alerts, ctx, obs(int64(1))); got.Status != AlertStatusFiring {
		t.Fatalf("первое событие после рестарта: %s (%s), want firing", got.Status, got.Detail)
	}
}

// TestAlertsRenderFallbackBudgetDegradation — the source not set — a quiet
// "not measured" (the address not configured); a probe error — "not measured"
// with a Warn (no metric in the exposition — a parse error, also a Warn).
func TestAlertsRenderFallbackBudgetDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	s := snapshotOf(t, env.Pass(context.Background()), AlertRenderFbBudget)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("источник не задан: %s, want not_measured", s.Status)
	}
	if !strings.Contains(s.Detail, "monitor.render_metrics_url") {
		t.Errorf("detail = %q, want подсказка адреса", s.Detail)
	}
	if strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("источник не задан дал Warn:\n%s", env.buf.String())
	}

	env2 := newAlertsTestEnv(t, nil)
	src := &fakeBudgetEvents{errs: []error{errors.New("connection refused")}}
	env2.deps.FallbackBudget = src
	s = snapshotOf(t, env2.Pass(context.Background()), AlertRenderFbBudget)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "connection refused") {
		t.Errorf("ошибка зонда: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env2.buf.String(), "level=WARN") {
		t.Errorf("ошибка зонда без Warn:\n%s", env2.buf.String())
	}

	// The error queue emptied — the next pass of the live probe works
	// (the success path of observeRenderFallbackBudget with the full stack): a ring
	// base without a spike.
	src.set(5)
	if s := snapshotOf(t, env2.Pass(context.Background()), AlertRenderFbBudget); s.Status != AlertStatusOK ||
		!strings.Contains(s.Detail, "события бюджета 5") {
		t.Errorf("живой зонд после ошибки: %s (%s), want ok с базой кольца", s.Status, s.Detail)
	}
}

// --- the stage browser errors of render (S-P2-6) ----------------------------------

// fakeBrowserErrors — a browser error counter source per a stub
// (the fakeBudgetEvents style).
type fakeBrowserErrors struct {
	mu   sync.Mutex
	val  int64
	errs []error
}

func (f *fakeBrowserErrors) RenderBrowserErrors(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := popErr(&f.errs); err != nil {
		return 0, err
	}
	return f.val, nil
}

// TestAlertsRenderBrowserErrors — the rate detector (S-P2-6): a series of
// browser errors (counter growth for 2 passes in a row) — firing; a single
// error — silent; the counter stopped growing — clearing (an event alert, not
// a state one); a worker restart (a decrease) — without a spike; a new
// accumulation from scratch.
func TestAlertsRenderBrowserErrors(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRenderBrowserErrMachine()

	// The base: the lazy family — 0 at a live worker without browser errors.
	s := m.transition(env.Alerts, ctx, obs(int64(0)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "browser-ошибки рендера 0") {
		t.Fatalf("база: %s (%s), want ok с нулём", s.Status, s.Detail)
	}
	// A single error — a series of 1/2, silent (a normal session restart).
	s = m.transition(env.Alerts, ctx, obs(int64(1)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Fatalf("одиночная: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	// A series — the second growing pass: firing.
	s = m.transition(env.Alerts, ctx, obs(int64(3)))
	if s.Status != AlertStatusFiring {
		t.Fatalf("серия: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"browser-ошибки рендера 3", "+2", "движок"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// The counter stands — the series is reset, the alert cleared (an engine recovery
	// or the gate hold: no new errors).
	if s := m.transition(env.Alerts, ctx, obs(int64(3))); s.Status != AlertStatusOK {
		t.Errorf("стационарно: %s (%s), want ok", s.Status, s.Detail)
	}
	// A worker restart (a process counter, reset) — a decrease is not a spike.
	if s := m.transition(env.Alerts, ctx, obs(int64(0))); s.Status != AlertStatusOK {
		t.Errorf("рестарт воркера: %s (%s), want ok без всплеска", s.Status, s.Detail)
	}
	// A new accumulation — again two growing passes.
	s = m.transition(env.Alerts, ctx, obs(int64(2)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Errorf("первый проход новой серии: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, obs(int64(5))); s.Status != AlertStatusFiring {
		t.Errorf("второй проход новой серии: %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsRenderBrowserErrorsDegradation — the source not set — a quiet
// "not measured" with the address hint; a foreign exposition (the address is
// not configured to a render worker) — a quiet "not measured" without a Warn (S-P2-6:
// a configuration, not a probe incident); a network error — "not measured"
// with a Warn.
func TestAlertsRenderBrowserErrorsDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	s := snapshotOf(t, env.Pass(context.Background()), AlertRenderBrowserErr)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "monitor.render_metrics_url") {
		t.Fatalf("источник не задан: %s (%s), want not_measured с подсказкой", s.Status, s.Detail)
	}
	if strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("источник не задан дал Warn:\n%s", env.buf.String())
	}

	env2 := newAlertsTestEnv(t, nil)
	env2.deps.RenderBrowserErrors = &fakeBrowserErrors{errs: []error{errNotRenderExposition}}
	s = snapshotOf(t, env2.Pass(context.Background()), AlertRenderBrowserErr)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "не рендер-воркера") {
		t.Fatalf("чужая экспозиция: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if strings.Contains(env2.buf.String(), "level=WARN") {
		t.Errorf("чужая экспозиция дала Warn:\n%s", env2.buf.String())
	}

	env3 := newAlertsTestEnv(t, nil)
	src := &fakeBrowserErrors{errs: []error{errors.New("connection refused")}}
	env3.deps.RenderBrowserErrors = src
	s = snapshotOf(t, env3.Pass(context.Background()), AlertRenderBrowserErr)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "connection refused") {
		t.Fatalf("сетевая ошибка: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env3.buf.String(), "level=WARN") {
		t.Errorf("сетевая ошибка без Warn:\n%s", env3.buf.String())
	}

	// The error queue emptied — the live probe reads the lazy series (the
	// success of observeRenderBrowserErrors with the full stack): 0 without errors — ok.
	if s := snapshotOf(t, env3.Pass(context.Background()), AlertRenderBrowserErr); s.Status != AlertStatusOK ||
		!strings.Contains(s.Detail, "browser-ошибки рендера 0") {
		t.Errorf("живой зонд после ошибки: %s (%s), want ok с нулём", s.Status, s.Detail)
	}
}

// TestParseBrowserErrorsExposition — a table parse of the
// stage="render",type="browser" branch: neighboring classes (task/context) and
// a foreign stage do not interfere; the lazy family (no series at the render worker) — 0;
// a foreign exposition without the marker — errNotRenderExposition.
func TestParseBrowserErrorsExposition(t *testing.T) {
	cases := []struct {
		name    string
		expo    string
		want    int64
		wantErr string
	}{
		{"ветка browser", `crawler_stage_errors_total{stage="render",type="browser"} 7`, 7, ""},
		{"соседние классы этапа не мешают",
			"crawler_stage_errors_total{stage=\"render\",type=\"task\"} 3\n" +
				"crawler_stage_errors_total{stage=\"render\",type=\"context\"} 2\n" +
				"crawler_stage_errors_total{stage=\"render\",type=\"browser\"} 1\n", 1, ""},
		{"чужой stage (http-воркер) — чужая экспозиция",
			"crawler_stage_errors_total{stage=\"http\",type=\"timeout\"} 99", 0, "не рендер-воркера"},
		{"ленивое семейство у рендер-воркера — 0",
			"crawler_stage_tasks_total{stage=\"render\"} 5\n", 0, ""},
		{"без метрик вовсе — чужая экспозиция", "# HELP x y\n", 0, "не рендер-воркера"},
		{"не-число", `crawler_stage_errors_total{stage="render",type="browser"} много`, 0, "не счётчик"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseBrowserErrorsExposition(strings.NewReader(tc.expo))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("разбор: %v", err)
			}
			if got != tc.want {
				t.Errorf("значение = %d, want %d", got, tc.want)
			}
		})
	}
}

// --- the /metrics probe of the render worker (suspends.go) ------------------------

// TestRenderSuspendsHTTP — the probe reads the reason="budget" branch and the
// browser error counter from a live exposition (httptest): gate events and other
// gauges do not interfere; a 404 path — an error with the address.
func TestRenderSuspendsHTTP(t *testing.T) {
	const expo = `# HELP crawler_render_fallback_suspended_total События приостановки
# TYPE crawler_render_fallback_suspended_total counter
crawler_render_fallback_suspended_total{reason="gate"} 2
crawler_render_fallback_suspended_total{reason="budget"} 7
crawler_stage_errors_total{stage="render",type="browser"} 4
crawler_stage_errors_total{stage="render",type="task"} 9
crawler_stage_tasks_total{stage="render"} 123
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(expo))
	}))
	defer srv.Close()

	got, err := RenderSuspends{URL: srv.URL}.FallbackBudget(context.Background())
	if err != nil {
		t.Fatalf("FallbackBudget: %v", err)
	}
	if got != 7 {
		t.Errorf("счётчик = %d, want 7 (только ветка budget)", got)
	}

	browser, err := RenderSuspends{URL: srv.URL}.RenderBrowserErrors(context.Background())
	if err != nil {
		t.Fatalf("RenderBrowserErrors: %v", err)
	}
	if browser != 4 {
		t.Errorf("browser-ошибки = %d, want 4 (ветка browser, соседние классы не мешают)", browser)
	}

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer broken.Close()
	if _, err := (RenderSuspends{URL: broken.URL}).FallbackBudget(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "500") {
		t.Errorf("не-200: %v, want ошибка со статусом", err)
	}
	if _, err := (RenderSuspends{URL: broken.URL}).RenderBrowserErrors(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "500") {
		t.Errorf("не-200 (browser): %v, want ошибка со статусом", err)
	}
}

// TestParseSuspendExposition — a table parse: the exact label pair,
// skipping HELP/TYPE and foreign metrics; the T-250 pre-created series (0 from
// the first start); a missing metric/branch — 0 at the render-worker
// marker (S-P1-4) and an error for a foreign exposition; a junk
// value — an error.
func TestParseSuspendExposition(t *testing.T) {
	cases := []struct {
		name    string
		expo    string
		want    int64
		wantErr string
	}{
		{"ветка бюджета", `crawler_render_fallback_suspended_total{reason="budget"} 7`, 7, ""},
		{"предсозданная серия T-250 — 0 с первого старта",
			"crawler_stage_tasks_total{stage=\"render\"} 123\n" +
				"crawler_render_fallback_suspended_total{reason=\"budget\"} 0\n", 0, ""},
		{"семейство есть, ветки budget нет, маркер рендер-воркера — 0 (S-P1-4)",
			"crawler_render_fallback_suspended_total{reason=\"gate\"} 2\n" +
				"crawler_stage_tasks_total{stage=\"render\"} 123\n", 0, ""},
		{"старая сборка: маркер есть, семейства нет — 0",
			"crawler_stage_tasks_total{stage=\"render\"} 123\n", 0, ""},
		{"только gate, маркера нет — чужая экспозиция", `crawler_render_fallback_suspended_total{reason="gate"} 2`, 0, "отсутствует"},
		{"без метрик вовсе (чужая экспозиция)", "# HELP x y\n", 0, "отсутствует"},
		{"не-число", `crawler_render_fallback_suspended_total{reason="budget"} много`, 0, "не счётчик"},
		{"отрицательное", `crawler_render_fallback_suspended_total{reason="budget"} -1`, 0, "не счётчик"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSuspendExposition(strings.NewReader(tc.expo))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("разбор: %v", err)
			}
			if got != tc.want {
				t.Errorf("значение = %d, want %d", got, tc.want)
			}
		})
	}
}

// Tests of the render-worker wiring points of T-283 (the engine restart
// survival): the recovery loop of the Run cycle must not claim stalled
// deliveries while the render engine is down (Options.Alive →
// RecoverOptions.Ready — the attempts are not spent on a refusing
// engine), and the generation restarts of the manager (for an external
// engine — a reconnect after its container restart) must be visible in
// the metrics (the delta poll of Options.Restarts →
// crawler_render_browser_restarts_total{engine}).
package renderworker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/queue"
)

func TestRecoverOptionsReadyWiring(t *testing.T) {
	// The wiring: the Run loop passes the browser probe Options.Alive as
	// the claim gate RecoverOptions.Ready (T-283); a build without the
	// probe keeps nil — the gate is transparent (isolated builds).
	t.Run("the probe is mounted", func(t *testing.T) {
		o := Options{Alive: func() bool { return true }}
		ro := recoverOptions(o, nil)
		if ro.Ready == nil {
			t.Fatal("Ready is nil with a mounted probe — the claims would burn attempts during an engine outage")
		}
		if !ro.Ready() {
			t.Error("Ready must reflect the probe (true)")
		}
	})
	t.Run("no probe — no gate", func(t *testing.T) {
		ro := recoverOptions(Options{}, nil)
		if ro.Ready != nil {
			t.Error("Ready must be nil without a probe (the zero behavior)")
		}
	})
	t.Run("the parameters are passed through", func(t *testing.T) {
		o := Options{Alive: func() bool { return false },
			Stream: queue.StreamRender, Group: DefaultGroup, Consumer: "c",
			RecoverBatch: 7, RecoverInterval: 13 * time.Second}
		ro := recoverOptions(o, nil)
		if ro.Stream != queue.StreamRender || ro.Group != DefaultGroup || ro.Consumer != "c" ||
			ro.Batch != 7 || ro.Interval != 13*time.Second {
			t.Errorf("the parameters are not passed through: %+v", ro)
		}
		if ro.Ready() {
			t.Error("Ready must reflect the probe (false)")
		}
	})
}

func TestCollectRestarts(t *testing.T) {
	// The delta poll: the source counter (Manager.Restarts) grows on
	// every generation swap; the metric repeats the DELTA, so several
	// restarts between polls are not lost (+2 at once — two observed
	// engine restarts). A source that does not grow does not touch the
	// metric. The baseline poll is waited for explicitly: incrementing
	// the source before the first poll would be swallowed by the
	// baseline itself.
	reg := metrics.New("render-worker")
	met := NewMetrics(reg, config.EngineMoli)
	var polls atomic.Int32
	var src atomic.Int64
	srcFn := func() int64 {
		polls.Add(1)
		return src.Load()
	}
	interval := 2 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		collectRestarts(ctx, srcFn, met, interval, nil)
	}()

	// The baseline poll has happened — the source growth is a delta now.
	waitFor(t, func() bool { return polls.Load() >= 1 })
	// Two restarts between polls: the next tick must add exactly 2.
	src.Add(2)
	waitFor(t, func() bool {
		return rwMetricValue(t, reg, "crawler_render_browser_restarts_total",
			map[string]string{"engine": config.EngineMoli}) >= 2
	})
	// A quiet window: the metric does not grow without new restarts.
	base := polls.Load()
	time.Sleep(5 * interval)
	if got := rwMetricValue(t, reg, "crawler_render_browser_restarts_total",
		map[string]string{"engine": config.EngineMoli}); got != 2 {
		t.Fatalf("crawler_render_browser_restarts_total = %v, want 2 (the delta of the source, no growth without restarts; polls %d→%d)",
			got, base, polls.Load())
	}
	// One more restart — +1.
	src.Add(1)
	waitFor(t, func() bool {
		return rwMetricValue(t, reg, "crawler_render_browser_restarts_total",
			map[string]string{"engine": config.EngineMoli}) >= 3
	})
	cancel()
	<-done
	if got := rwMetricValue(t, reg, "crawler_render_browser_restarts_total",
		map[string]string{"engine": config.EngineMoli}); got != 3 {
		t.Errorf("crawler_render_browser_restarts_total = %v, want 3", got)
	}
	// The engine label is fixed by the build (T-235): a foreign engine's
	// series is not exported.
	if got := rwMetricValue(t, reg, "crawler_render_browser_restarts_total",
		map[string]string{"engine": config.EngineChromium}); got != -1 {
		t.Errorf("crawler_render_browser_restarts_total{engine=chromium} = %v, want -1 (a foreign engine)", got)
	}
}

// waitFor — polls cond until it holds or the 5 s deadline expires (the
// observation of a background collector's effect).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("the condition did not hold within 5 s (a background effect did not appear)")
	}
}

func TestCollectRestartsNil(t *testing.T) {
	// A build without the source (Options.Restarts = nil — isolated
	// builds/tests) and nil metrics: no polling, no panic.
	collectRestarts(context.Background(), nil, nil, time.Millisecond, nil)
	src := func() int64 { return 42 }
	collectRestarts(context.Background(), src, nil, time.Millisecond, nil)
	met := NewMetrics(metrics.New("render-worker"), config.EngineChromium)
	// A nil source with live metrics — nothing to poll: exits at once.
	done := make(chan struct{})
	go func() {
		defer close(done)
		collectRestarts(context.Background(), nil, met, time.Millisecond, nil)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("collectRestarts did not exit with a nil source")
	}
}

// Unit tests of the DNS fallback switch collector (T-107): a no-op on
// nil dependencies, the export of the first snapshot into the counters,
// accumulation by the deltas between passes, and a source restart (a
// negative delta). The state source is a closure over a mutable snapshot
// (FallbackStatsFunc).

package dnsworker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
)

// fallbackCounterValue — the value of the counter metric name (without
// labels except the service one): the first series of the family.
func fallbackCounterValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1 // the metric is not found — the test will fail by comparison
}

// mutateStats — a mutable snapshot source: the collector runs in the
// background, the test changes the values between the passes.
type mutateStats struct {
	mu  sync.Mutex
	val dnsclient.FallbackStats
}

func (m *mutateStats) set(v dnsclient.FallbackStats) {
	m.mu.Lock()
	m.val = v
	m.mu.Unlock()
}

func (m *mutateStats) get() dnsclient.FallbackStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.val
}

func TestCollectFallbackLoopNoop(t *testing.T) {
	// Without a registry or a source — a no-op: no panic, no
	// registration.
	if err := CollectFallbackLoop(context.Background(), nil, func() dnsclient.FallbackStats { return dnsclient.FallbackStats{} }, time.Millisecond, nil); err != nil {
		t.Fatalf("a nil registry: %v", err)
	}
	reg := metrics.New("dns-worker")
	if err := CollectFallbackLoop(context.Background(), reg, nil, time.Millisecond, nil); err != nil {
		t.Fatalf("a nil source: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if strings.HasPrefix(mf.GetName(), "crawler_dns_fallback_") {
			t.Fatalf("switchover instruments registered without a source: %s", mf.GetName())
		}
	}
}

func TestCollectFallbackLoopExportsAndDeltas(t *testing.T) {
	reg := metrics.New("dns-worker")
	src := &mutateStats{val: dnsclient.FallbackStats{Switches: 5, Exhausted: 2}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- CollectFallbackLoop(ctx, reg, src.get, time.Millisecond, nil)
	}()

	// The first pass — right at startup: the snapshot {5, 2}.
	waitFor(t, func() bool {
		return fallbackCounterValue(t, reg, "crawler_dns_fallback_switches_total") == 5 &&
			fallbackCounterValue(t, reg, "crawler_dns_fallback_exhausted_total") == 2
	})

	// The source accumulated more: the counters grow by a delta rather
	// than being reassigned.
	src.set(dnsclient.FallbackStats{Switches: 8, Exhausted: 3})
	waitFor(t, func() bool {
		return fallbackCounterValue(t, reg, "crawler_dns_fallback_switches_total") == 8 &&
			fallbackCounterValue(t, reg, "crawler_dns_fallback_exhausted_total") == 3
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CollectFallbackLoop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the collector did not finish after the ctx cancel")
	}
}

// TestCollectFallbackLoopSourceRestart — the source counter "went
// down" (the chain was recreated): a Prometheus counter is monotonic,
// so the value of the new snapshot is added in full (5 → 3 yields 5+3).
func TestCollectFallbackLoopSourceRestart(t *testing.T) {
	reg := metrics.New("dns-worker")
	src := &mutateStats{}
	src.set(dnsclient.FallbackStats{Switches: 5, Exhausted: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- CollectFallbackLoop(ctx, reg, src.get, time.Millisecond, nil)
	}()

	waitFor(t, func() bool {
		return fallbackCounterValue(t, reg, "crawler_dns_fallback_switches_total") == 5
	})
	src.set(dnsclient.FallbackStats{Switches: 3, Exhausted: 1})
	waitFor(t, func() bool {
		return fallbackCounterValue(t, reg, "crawler_dns_fallback_switches_total") == 8 &&
			fallbackCounterValue(t, reg, "crawler_dns_fallback_exhausted_total") == 3
	})

	cancel()
	<-done
}

// waitFor — polling of the condition for up to 2 s (the collector ticks
// in the background).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("the collector condition not met within 2s")
	}
}

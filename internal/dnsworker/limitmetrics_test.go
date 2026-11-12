// Unit tests of the DNS limiter state collector (T-077): a no-op on nil
// dependencies, the export of a LimitStats snapshot into the gauges by
// the kind label, a clean exit on a ctx cancellation. The state source is
// a closure over a prepared snapshot (LimitStatsFunc).
package dnsworker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
)

// limitGaugeValue — the value of the gauge metric name without labels
// (except the service one): the first series of the family.
func limitGaugeValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
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
			if ga := m.GetGauge(); ga != nil {
				return ga.GetValue()
			}
		}
	}
	return -1 // the metric is not found — the test will fail by comparison
}

func TestCollectLimitLoopNoop(t *testing.T) {
	// Without a registry or a source — a no-op: no panic, no
	// registration.
	if err := CollectLimitLoop(context.Background(), nil, func() dnsclient.LimitStats { return dnsclient.LimitStats{} }, time.Millisecond, nil); err != nil {
		t.Fatalf("a nil registry: %v", err)
	}
	reg := metrics.New("dns-worker")
	if err := CollectLimitLoop(context.Background(), reg, nil, time.Millisecond, nil); err != nil {
		t.Fatalf("a nil source: %v", err)
	}
	// The service framework's base metrics (service_up, build_info,
	// Go/runtime) are in the registry; there must be no limiter
	// instruments.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if strings.HasPrefix(mf.GetName(), "crawler_dns_") {
			t.Fatalf("limiter instruments registered without a source: %s", mf.GetName())
		}
	}
}

func TestCollectLimitLoopExportsStats(t *testing.T) {
	reg := metrics.New("dns-worker")
	// A snapshot of "the adaptive at the floor": 2/2000, 100/10000, a
	// window of 150/200 timeouts.
	snap := dnsclient.LimitStats{
		Concurrency:    2,
		ConcurrencyCfg: 2000,
		QPS:            100,
		QPSCfg:         10000,
		InFlight:       7,
		WindowFilled:   true,
		WindowSize:     200,
		WindowTimeouts: 150,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- CollectLimitLoop(ctx, reg, func() dnsclient.LimitStats { return snap }, time.Millisecond, nil)
	}()

	// The first pass — right at startup: wait for the values to appear.
	deadline := time.Now().Add(2 * time.Second)
	for dnsMetricValue(t, reg, "crawler_dns_limit_concurrency", limitKindEffective) < 0 &&
		time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CollectLimitLoop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the collector did not finish after the ctx cancel")
	}

	if got := dnsMetricValue(t, reg, "crawler_dns_limit_concurrency", limitKindEffective); got != 2 {
		t.Errorf("crawler_dns_limit_concurrency{effective} = %v, want 2", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_dns_limit_concurrency", limitKindConfigured); got != 2000 {
		t.Errorf("crawler_dns_limit_concurrency{configured} = %v, want 2000", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_dns_limit_qps", limitKindEffective); got != 100 {
		t.Errorf("crawler_dns_limit_qps{effective} = %v, want 100", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_dns_limit_qps", limitKindConfigured); got != 10000 {
		t.Errorf("crawler_dns_limit_qps{configured} = %v, want 10000", got)
	}
	if got := limitGaugeValue(t, reg, "crawler_dns_limit_timeout_ratio"); got != 0.75 {
		t.Errorf("crawler_dns_limit_timeout_ratio = %v, want 0.75", got)
	}
	if got := limitGaugeValue(t, reg, "crawler_dns_inflight"); got != 7 {
		t.Errorf("crawler_dns_inflight = %v, want 7", got)
	}
}

// TestCollectLimitLoopRatioEmptyWindow — the window is still filling:
// the share is 0, not a division-by-zero panic.
func TestCollectLimitLoopRatioEmptyWindow(t *testing.T) {
	reg := metrics.New("dns-worker")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- CollectLimitLoop(ctx, reg, func() dnsclient.LimitStats {
			return dnsclient.LimitStats{Concurrency: 2000, ConcurrencyCfg: 2000}
		}, time.Hour, nil) // the first pass — right away; the ticks are not needed
	}()

	deadline := time.Now().Add(2 * time.Second)
	for limitGaugeValue(t, reg, "crawler_dns_limit_timeout_ratio") < 0 &&
		time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if got := limitGaugeValue(t, reg, "crawler_dns_limit_timeout_ratio"); got != 0 {
		t.Errorf("the crawler_dns_limit_timeout_ratio of an empty window = %v, want 0", got)
	}
}

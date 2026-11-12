// Unit tests of the DNS stage instruments (T-077, spec 5.9): nil safety,
// registration and the export of the values via Gather (the episode
// counter, the duration histogram, the final status counter), the
// increments on the typical path of the Run loop — on the pool/queue/
// resolver fakes from result_test.go (two domains: with addresses — no
// status, NXDOMAIN — not_delegated).
package dnsworker

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// dnsMetricValue — the value of the counter/histogram family name's
// series with the label label (for a histogram the observation count
// _count is taken).
func dnsMetricValue(t *testing.T, g prometheus.Gatherer, name, label string) float64 {
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
			for _, l := range m.GetLabel() {
				if l.GetValue() == label {
					if h := m.GetHistogram(); h != nil {
						return float64(h.GetSampleCount())
					}
					if c := m.GetCounter(); c != nil {
						return c.GetValue()
					}
					if ga := m.GetGauge(); ga != nil {
						return ga.GetValue()
					}
				}
			}
		}
	}
	return -1 // the series is not found — the test will fail by comparison
}

func TestMetricsNilSafe(t *testing.T) {
	if NewMetrics(nil) != nil {
		t.Fatal("NewMetrics(nil) != nil — isolated builds without a registry")
	}
	var m *Metrics
	m.Episode(time.Second)             // a no-op, no panic
	m.DomainStatus(model.StatusNoHTTP) // a no-op
	m.DBDeadlockRetried(1)             // a no-op (T-268)
}

func TestMetricsRegistered(t *testing.T) {
	reg := metrics.New("dns-worker")
	m := NewMetrics(reg)
	if m == nil {
		t.Fatal("NewMetrics with a registry = nil")
	}

	m.Episode(150 * time.Millisecond)
	m.Episode(40 * time.Millisecond)
	m.DomainStatus(model.StatusNotDelegated)
	m.DomainStatus(model.StatusTempError)
	m.DomainStatus(model.StatusTempError)
	m.DBDeadlockRetried(1) // T-268
	m.DBDeadlockRetried(2)

	// Series checks (the name + the label value): the tasks and the
	// durations.
	if got := dnsMetricValue(t, reg, "crawler_stage_tasks_total", metrics.StageDNS); got != 2 {
		t.Errorf("crawler_stage_tasks_total = %v, want 2", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_stage_seconds", metrics.StageDNS); got != 2 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 2", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_domain_status_total", model.StatusNotDelegated.String()); got != 1 {
		t.Errorf("crawler_domain_status_total{not_delegated} = %v, want 1", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_domain_status_total", model.StatusTempError.String()); got != 2 {
		t.Errorf("crawler_domain_status_total{temp_error} = %v, want 2", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_domain_status_total", model.StatusAlive.String()); got != -1 {
		t.Errorf("crawler_domain_status_total{alive} = %v, want the series absent (the HTTP stage writes the alive status)", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_db_deadlock_retried_total", "dns-worker"); got != 2 {
		t.Errorf("crawler_db_deadlock_retried_total = %v, want 2 (T-268)", got)
	}
}

// TestMetricsRunSmoke — the instruments are incremented on the typical
// loop path: a q:dns batch with two domains (addresses present — no
// status; NXDOMAIN — not_delegated) → 2 episodes, 1 terminal status.
func TestMetricsRunSmoke(t *testing.T) {
	reg := metrics.New("dns-worker")
	j := &resultJournal{ackNotify: make(chan struct{}, 4)}
	pool := &resultFakePool{j: j, subdomains: resultSubs()}
	q := &resultFakeQueue{j: j, deliveries: []queue.Delivery{
		resultDelivery("1-1", "task-1",
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"},
			queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "gone.example.com"}),
	}}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")
	resultSetNXDOMAIN(resolve, "gone.example.com")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, pool, q, resolve, Options{
			Concurrency: 2, Group: "g", Consumer: "c",
			Metrics: reg,
		})
	}()

	if !resultWaitAck(j, 5*time.Second) {
		cancel()
		t.Fatal("XACK not awaited: the delivery not processed")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancel")
	}

	if got := dnsMetricValue(t, reg, "crawler_stage_tasks_total", metrics.StageDNS); got != 2 {
		t.Errorf("crawler_stage_tasks_total = %v, want 2 episodes", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_stage_seconds", metrics.StageDNS); got != 2 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 2", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_domain_status_total", model.StatusNotDelegated.String()); got != 1 {
		t.Errorf("crawler_domain_status_total{not_delegated} = %v, want 1", got)
	}
}

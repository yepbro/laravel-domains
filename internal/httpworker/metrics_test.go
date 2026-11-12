// Unit tests of the HTTP stage's instruments (T-077, spec 5.9): nil safety,
// exporting values via Gather (the task counter and durations, errors
// by ErrorClass class, the final status, the response time without cache
// hits, the rendered=false records), increments in the typical path of the Run
// loop — on the load and queue seam fakes from worker_test.go.
package httpworker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// httpMetricValue — the value of the metric name's series, all labels labels
// matching (for a histogram — the observation count). The series is not found —
// −1 (distinguishable from an honest zero).
func httpMetricValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			for lv, want := range labels {
				found := false
				for _, l := range m.GetLabel() {
					if l.GetName() == lv && l.GetValue() == want {
						found = true
						break
					}
				}
				if !found {
					continue series
				}
			}
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
	return -1
}

func TestMetricsNilSafe(t *testing.T) {
	if NewMetrics(nil) != nil {
		t.Fatal("NewMetrics(nil) != nil — isolated builds without a registry")
	}
	var m *Metrics
	m.Episode(&DomainResult{Name: "x"}, time.Second) // no-op, no panic
	m.Episode(nil, time.Second)                      // the guard of direct calls
	m.SlotEnter()
	m.SlotLeave()
	m.BodyHeld(1 << 20)
	m.BodyHeld(-(1 << 20))
	m.BodySize(4096)
	m.PerIPWait(time.Second)
}

// TestMetricsSlotBody — the slot memory instruments (T-163): the slot occupancy
// by an enter/exit pair, the transit body bytes by a converging ±n pair, the body
// size — a histogram observation. The pairs must bring the gauges to zero —
// otherwise the sampler would measure a leak where there is none.
func TestMetricsSlotBody(t *testing.T) {
	reg := metrics.New("http-worker")
	m := NewMetrics(reg)

	m.SlotEnter()
	m.SlotEnter()
	if got := httpMetricValue(t, reg, "crawler_http_tasks_inflight", nil); got != 2 {
		t.Errorf("crawler_http_tasks_inflight = %v, want 2", got)
	}
	m.SlotLeave()
	m.SlotLeave()
	if got := httpMetricValue(t, reg, "crawler_http_tasks_inflight", nil); got != 0 {
		t.Errorf("crawler_http_tasks_inflight after the exits = %v, want 0", got)
	}

	m.BodyHeld(65536)
	m.BodyHeld(4096)
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes_inflight", nil); got != 69632 {
		t.Errorf("crawler_http_body_bytes_inflight = %v, want 69632", got)
	}
	m.BodyHeld(-69632)
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes_inflight", nil); got != 0 {
		t.Errorf("crawler_http_body_bytes_inflight after the window exit = %v, want 0", got)
	}

	m.BodySize(20000)
	m.BodySize(20000)
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes", nil); got != 2 {
		t.Errorf("crawler_http_body_bytes (count) = %v, want 2", got)
	}
}

// TestMetricsPerIPWait — the per-IP semaphore wait histogram (T-165):
// observations land in the counter, the le=2 bucket (the boundary of the
// "share of waits > 2 s < 5%" criterion) gives a cumulative below the overall counter —
// the share is read from the buckets without a separate counter.
func TestMetricsPerIPWait(t *testing.T) {
	reg := metrics.New("http-worker")
	m := NewMetrics(reg)

	m.PerIPWait(150 * time.Millisecond)
	m.PerIPWait(3 * time.Second)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var count, le2 uint64
	found := false
	for _, mf := range families {
		if mf.GetName() != "crawler_http_per_ip_wait_seconds" {
			continue
		}
		found = true
		h := mf.GetMetric()[0].GetHistogram()
		count = h.GetSampleCount()
		for _, b := range h.GetBucket() {
			if b.GetUpperBound() == 2 {
				le2 = b.GetCumulativeCount()
			}
		}
	}
	if !found {
		t.Fatal("crawler_http_per_ip_wait_seconds is not exported")
	}
	if count != 2 {
		t.Errorf("crawler_http_per_ip_wait_seconds (count) = %d, want 2", count)
	}
	if le2 != 1 {
		t.Errorf("the le=2 bucket = %d, want 1 (a 3 s wait lies above the boundary)", le2)
	}
}

// TestMetricsEpisode — one episode: two checks with errors of different
// classes, one available non-cached (the response time), one cache
// hit (no response time), two result records → alive.
func TestMetricsEpisode(t *testing.T) {
	reg := metrics.New("http-worker")
	m := NewMetrics(reg)

	res := &DomainResult{
		ZoneID: 3, DomainID: 11, Name: "example.com",
		Subdomains: []SubdomainResult{
			{
				SubdomainID: 0, Label: "",
				HTTPS: SchemeCheck{Available: true, ResponseMS: 120},
				HTTP:  SchemeCheck{ErrClass: ErrTimeout},
			},
			{
				SubdomainID: 1, Label: "www",
				HTTPS: SchemeCheck{ErrClass: ErrTLS},
				HTTP:  SchemeCheck{Available: true, Cached: true, ResponseMS: 0},
			},
		},
	}
	res.Subdomains[0].Records = []Record{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}}
	res.Subdomains[1].Records = []Record{{Variant: model.VariantMain, Scheme: model.SchemeHTTP}}

	m.Episode(res, 250*time.Millisecond)

	stage := map[string]string{"stage": metrics.StageHTTP}
	if got := httpMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
		t.Errorf("crawler_stage_tasks_total = %v, want 1", got)
	}
	if got := httpMetricValue(t, reg, "crawler_stage_seconds", stage); got != 1 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 1", got)
	}
	for typ, want := range map[string]float64{
		ErrTimeout.String(): 1,
		ErrTLS.String():     1,
		ErrNetwork.String(): -1,
	} {
		labels := map[string]string{"stage": metrics.StageHTTP, "type": typ}
		if got := httpMetricValue(t, reg, "crawler_stage_errors_total", labels); got != want {
			t.Errorf("crawler_stage_errors_total{type=%s} = %v, want %v", typ, got, want)
		}
	}
	if got := httpMetricValue(t, reg, "crawler_domain_status_total", map[string]string{"type": model.StatusAlive.String()}); got != 1 {
		t.Errorf("crawler_domain_status_total{alive} = %v, want 1 (there are records)", got)
	}
	// The response time: only the available non-cached check (the cache did
	// not touch the network, ResponseMS = 0).
	if got := httpMetricValue(t, reg, "crawler_http_response_seconds", nil); got != 1 {
		t.Errorf("crawler_http_response_seconds (count) = %v, want 1", got)
	}
	// The render share: 2 records with rendered=false.
	if got := httpMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "false"}); got != 2 {
		t.Errorf("crawler_render_records_total{rendered=\"false\"} = %v, want 2", got)
	}
	if got := httpMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != -1 {
		t.Errorf("crawler_render_records_total{rendered=\"true\"} = %v, want the series missing (the render branch)", got)
	}
}

// TestMetricsEpisodeCutoff — the dead-host cutoff instruments (T-164): a task with
// the Cutoff sign increments the cutoff-task counter, synthetic
// ErrCutoff — the saved checks counter; the skipped checks do not count
// as stage errors (crawler_stage_errors_total has no type=cutoff
// series), the performed head checks count as usual.
func TestMetricsEpisodeCutoff(t *testing.T) {
	reg := metrics.New("http-worker")
	m := NewMetrics(reg)

	res := &DomainResult{
		ZoneID: 4, DomainID: 21, Name: "example.net", Cutoff: true,
		Subdomains: []SubdomainResult{
			{
				SubdomainID: 0, Label: "",
				HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrNetwork, DeadDial: true},
				HTTP:  SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrNetwork, DeadDial: true},
			},
			{
				SubdomainID: 1, Label: "www",
				HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrNetwork, DeadDial: true},
				HTTP:  SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrNetwork, DeadDial: true},
			},
			{
				SubdomainID: 3, Label: "blog", Hostname: "blog.example.net",
				HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrCutoff},
				HTTP:  SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrCutoff},
			},
		},
	}
	m.Episode(res, 2*time.Second)

	if got := httpMetricValue(t, reg, "crawler_http_dead_host_cutoff_tasks_total", nil); got != 1 {
		t.Errorf("crawler_http_dead_host_cutoff_tasks_total = %v, want 1", got)
	}
	if got := httpMetricValue(t, reg, "crawler_http_dead_host_cutoff_skipped_checks_total", nil); got != 2 {
		t.Errorf("crawler_http_dead_host_cutoff_skipped_checks_total = %v, want 2", got)
	}
	cutLabels := map[string]string{"stage": metrics.StageHTTP, "type": ErrCutoff.String()}
	if got := httpMetricValue(t, reg, "crawler_stage_errors_total", cutLabels); got != -1 {
		t.Errorf("crawler_stage_errors_total{type=cutoff} = %v, want the series missing (the check was not performed)", got)
	}
	netLabels := map[string]string{"stage": metrics.StageHTTP, "type": ErrNetwork.String()}
	if got := httpMetricValue(t, reg, "crawler_stage_errors_total", netLabels); got != 4 {
		t.Errorf("crawler_stage_errors_total{type=network} = %v, want 4 (the head checks)", got)
	}
}

// TestMetricsRunSmoke — the instruments are incremented by the typical path of the loop:
// a q:http delivery → checks → OnResult → Episode (before the XACK).
func TestMetricsRunSmoke(t *testing.T) {
	body := "<html>" + strings.Repeat("a page for the http metrics smoke test ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"metrics.test":         {status: 200, body: body},
		"http://metrics.test/": {status: 200, body: body},
	})
	reg := metrics.New("http-worker")
	w := workerWorker(t, fetch, func(o *Options) { o.Metrics = reg })
	q := &workerFakeQueue{deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "metrics.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}

	var mu sync.Mutex
	records := 0
	opts := Options{OnResult: func(_ context.Context, res *DomainResult) error {
		mu.Lock()
		defer mu.Unlock()
		records = res.RecordCount()
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, q, w, opts)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if q.ackedCount() > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancellation")
	}
	if q.ackedCount() != 1 {
		t.Fatalf("XACK = %d, want 1 — the processing did not reach the acknowledgment", q.ackedCount())
	}

	stage := map[string]string{"stage": metrics.StageHTTP}
	if got := httpMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
		t.Errorf("crawler_stage_tasks_total = %v, want 1", got)
	}
	if got := httpMetricValue(t, reg, "crawler_stage_seconds", stage); got != 1 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 1", got)
	}
	// Both protocols answer identically → one record variant (T-054);
	// the domain status — alive.
	mu.Lock()
	recN := records
	mu.Unlock()
	if got := httpMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "false"}); got != float64(recN) || got < 1 {
		t.Errorf("crawler_render_records_total{false} = %v, want %v (≥1)", got, recN)
	}
	if got := httpMetricValue(t, reg, "crawler_domain_status_total", map[string]string{"type": model.StatusAlive.String()}); got != 1 {
		t.Errorf("crawler_domain_status_total{alive} = %v, want 1", got)
	}
	// https and http — independent network checks: two response time
	// observations.
	if got := httpMetricValue(t, reg, "crawler_http_response_seconds", nil); got != 2 {
		t.Errorf("crawler_http_response_seconds (count) = %v, want 2", got)
	}
}

// TestMetricsSlotMemoryRun — the slot memory instruments (T-163) in the typical
// path of the loop: during processing (OnResult is called inside
// processDelivery — a deterministic point) exactly one slot is busy,
// the transit body bytes are already released (the hold window closes in
// checkScheme before the return); after the loop completes the slots and bytes
// converge to zero, the read bodies — in the mix histogram (both protocol
// checks read the body, the final URLs' cache does not spread
// across schemes).
func TestMetricsSlotMemoryRun(t *testing.T) {
	body := "<html>" + strings.Repeat("a page for the slot memory measurement ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"mem.test": {status: 200, body: body},
	})
	reg := metrics.New("http-worker")
	w := workerWorker(t, fetch, func(o *Options) { o.Metrics = reg })
	q := &workerFakeQueue{deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "mem.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}

	var inSlots, inBodyHeld float64
	opts := Options{OnResult: func(_ context.Context, res *DomainResult) error {
		inSlots = httpMetricValue(t, reg, "crawler_http_tasks_inflight", nil)
		inBodyHeld = httpMetricValue(t, reg, "crawler_http_body_bytes_inflight", nil)
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, q, w, opts)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if q.ackedCount() > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancellation")
	}

	if inSlots != 1 {
		t.Errorf("crawler_http_tasks_inflight inside the processing = %v, want 1", inSlots)
	}
	// The bodies are read and released before OnResult (the early Put of T-147): the
	// hold window is closed in checkScheme.
	if inBodyHeld != 0 {
		t.Errorf("crawler_http_body_bytes_inflight inside OnResult = %v, want 0 (the bodies are already released)", inBodyHeld)
	}
	if got := httpMetricValue(t, reg, "crawler_http_tasks_inflight", nil); got != 0 {
		t.Errorf("crawler_http_tasks_inflight after the processing = %v, want 0", got)
	}
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes_inflight", nil); got != 0 {
		t.Errorf("crawler_http_body_bytes_inflight after the processing = %v, want 0", got)
	}
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes", nil); got != 2 {
		t.Errorf("crawler_http_body_bytes (count) = %v, want 2 (https and http)", got)
	}
}

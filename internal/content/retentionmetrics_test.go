// Unit tests of the retention cycle metrics (T-166): accumulation of pass
// outcomes in counters (rows/refs/contents/bytes), interlock skips via the
// passes{result} label, queue-guard read errors (passes{skipped_pending}
// + pending_errors), records excluded by the guard per stream
// (deferred{stream}), bookkeeping anomalies, no-op without a registry,
// constructor idempotency (T-076).
package content

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// retentionCounterValue — the value of the counter name (no labels except
// the service one): the first series of the family; -1 if the metric is not
// exported (the style of cleanupCounterValue in ingest).
func retentionCounterValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
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
	return -1
}

// retentionCounterByLabels — the value of the counter name with an exact
// label set (except the service label); -1 if no series is found.
func retentionCounterByLabels(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	nextMetric:
		for _, m := range mf.GetMetric() {
			got := make(map[string]string)
			for _, lp := range m.GetLabel() {
				if lp.GetName() == metrics.LabelService {
					continue
				}
				got[lp.GetName()] = lp.GetValue()
			}
			if len(got) != len(labels) {
				continue
			}
			for k, v := range labels {
				if got[k] != v {
					continue nextMetric
				}
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// TestRetentionMetricsObserve — pass outcomes accumulate in counters,
// zero values change nothing, the interlock is counted separately (skips +
// passes{result="skipped"}), working passes go to passes{result="ok"}.
func TestRetentionMetricsObserve(t *testing.T) {
	reg := metrics.New("http-worker")
	m := NewRetentionMetrics(reg)

	m.Observe(RetentionStats{}, time.Second) // empty pass: counters do not grow
	for _, name := range []string{
		"crawler_content_retention_rows_total",
		"crawler_content_retention_refs_total",
		"crawler_content_retention_contents_total",
		"crawler_content_retention_freed_bytes_total",
		"crawler_content_retention_bad_hashes_total",
		"crawler_content_retention_missing_refs_total",
	} {
		if got := retentionCounterValue(t, reg, name); got != 0 {
			t.Fatalf("%s after a zero result = %v, want 0", name, got)
		}
	}

	m.Observe(RetentionStats{Rows: 10, Refs: 8, Contents: 6, FreedBytes: 42000}, 2*time.Second)
	m.Observe(RetentionStats{Rows: 5, Refs: 4, Contents: 4, FreedBytes: 21000, BadHashes: 1, Missing: 2}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 15 {
		t.Errorf("rows = %v, want 15", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_refs_total"); got != 12 {
		t.Errorf("refs = %v, want 12", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_contents_total"); got != 10 {
		t.Errorf("contents = %v, want 10", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_freed_bytes_total"); got != 63000 {
		t.Errorf("freed = %v, want 63000", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_bad_hashes_total"); got != 1 {
		t.Errorf("bad_hashes = %v, want 1 (grows only on an anomaly)", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_missing_refs_total"); got != 2 {
		t.Errorf("missing = %v, want 2", got)
	}
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_passes_total", map[string]string{"result": "ok"}); got != 3 {
		t.Errorf("passes{ok} = %v, want 3 (including the empty one)", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_skips_total"); got != 0 {
		t.Errorf("skips = %v, want 0", got)
	}

	// Interlock skip: work counters do not grow (there was no work),
	// skips and the skipped label.
	m.Observe(RetentionStats{Skipped: true, Depth: 150}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 15 {
		t.Errorf("rows after the skip = %v, want the previous 15", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_skips_total"); got != 1 {
		t.Errorf("skips = %v, want 1", got)
	}
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_passes_total", map[string]string{"result": "skipped"}); got != 1 {
		t.Errorf("passes{skipped} = %v, want 1", got)
	}

	// A skip in the middle of a drain does NOT lose the work already done
	// (review fix: the batches are committed in the DB): an interlock after
	// some batches — the work counters are credited in the skipped pass too.
	m.Observe(RetentionStats{Skipped: true, Depth: 150, Rows: 7, Refs: 7, Contents: 5, FreedBytes: 1000}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 22 {
		t.Errorf("rows = %v, want 22 (15 + 7 from the one skipped mid-drain)", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_contents_total"); got != 15 {
		t.Errorf("contents = %v, want 15 (10 + 5)", got)
	}

	// Skip due to a guard stream read error (T-166/T-167): a separate
	// passes{skipped_pending} label and an error counter; the interlock
	// skips counter does not grow — the causes differ, the alerts are
	// separate.
	m.Observe(RetentionStats{SkippedPending: true}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_pending_errors_total"); got != 1 {
		t.Errorf("pending_errors = %v, want 1", got)
	}
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_passes_total", map[string]string{"result": "skipped_pending"}); got != 1 {
		t.Errorf("passes{skipped_pending} = %v, want 1", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_skips_total"); got != 2 {
		t.Errorf("skips = %v, want the previous 2 (the interlock not involved)", got)
	}

	// Records excluded by the guard — per stream: a record waiting on both
	// streams is counted in each; the working counters are not touched.
	m.Observe(RetentionStats{
		Rows:     2,
		Deferred: map[string]int64{"q:render": 1, "q:analyze": 2},
	}, time.Second)
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_deferred_total", map[string]string{"stream": "q:render"}); got != 1 {
		t.Errorf("deferred{q:render} = %v, want 1", got)
	}
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_deferred_total", map[string]string{"stream": "q:analyze"}); got != 2 {
		t.Errorf("deferred{q:analyze} = %v, want 2", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 24 {
		t.Errorf("rows = %v, want 24 (the purge went along with the exclusions)", got)
	}
}

// TestRetentionMetricsNoRegistry — without a registry there is no collector
// (isolated builds); Observe calls are a no-op without a panic.
func TestRetentionMetricsNoRegistry(t *testing.T) {
	if m := NewRetentionMetrics(nil); m != nil {
		t.Fatalf("NewRetentionMetrics(nil) = %v, want nil", m)
	}
	NewRetentionMetrics(nil).Observe(RetentionStats{Rows: 7}, time.Second) // does not panic

	families, err := metrics.New("http-worker").Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == "crawler_content_retention_rows_total" {
			t.Fatalf("an instrument registered without a registry: %s", mf.GetName())
		}
	}
}

// TestRetentionMetricsIdempotent — a repeated registration shares the same
// collectors (T-076): the constructor may be called on every cycle.
func TestRetentionMetricsIdempotent(t *testing.T) {
	reg := metrics.New("http-worker")
	first := NewRetentionMetrics(reg)
	second := NewRetentionMetrics(reg)
	first.Observe(RetentionStats{Rows: 3}, time.Second)
	second.Observe(RetentionStats{Rows: 4}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 7 {
		t.Errorf("rows = %v, want 7 (one collector for two constructors)", got)
	}
}

// TestRetentionMetricsErrorsCounter — the failed-pass counter
// crawler_content_retention_errors_total (T-273): exported, grows on
// ObserveError (one per failed pass — the RetentionOptions.OnError hook),
// a successful/skipped pass does not touch it; a nil receiver is a no-op.
// Before T-273 pass errors were visible only in journalctl
// (the 2026-11-09/10 incident: a day of WARNs with zero pass metrics).
func TestRetentionMetricsErrorsCounter(t *testing.T) {
	reg := metrics.New("http-worker")
	m := NewRetentionMetrics(reg)
	const name = "crawler_content_retention_errors_total"

	m.Observe(RetentionStats{}, time.Second)              // successful (empty) pass
	m.Observe(RetentionStats{Skipped: true}, time.Second) // interlock skip
	if got := retentionCounterValue(t, reg, name); got != 0 {
		t.Fatalf("%s before the errors = %v, want 0 (success/skips do not count)", name, got)
	}
	m.ObserveError(nil)
	m.ObserveError(nil)
	if got := retentionCounterValue(t, reg, name); got != 2 {
		t.Fatalf("%s after two failed passes = %v, want 2", name, got)
	}

	// A nil receiver (a service without a registry) — a no-op, no panic.
	NewRetentionMetrics(nil).ObserveError(nil)
}

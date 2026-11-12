// Unit tests of the background cleanup metric (T-099): the export of
// pass BadHashes into the crawler_ingest_bad_hashes_total counter, a
// zero result not changing the value, the no-op without a registry, the
// constructor idempotency (T-076), and the delivery of pass results by
// the loop via CleanupOptions.OnPass.
package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// cleanupCounterValue — the value of the name counter (no labels except
// the service one): the first row of the family; -1 if the metric is not
// exported (the fallbackCounterValue style in dnsworker).
func cleanupCounterValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
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

// TestCleanupMetricsObserve — pass results accumulate in the counter: a
// zero BadHashes does not change the value, positive ones are summed
// (the Prometheus _total semantics are preserved).
func TestCleanupMetricsObserve(t *testing.T) {
	reg := metrics.New("ingest")
	m := NewCleanupMetrics(reg)

	m.Observe(CleanupResult{Domains: 3, Results: 10}) // BadHashes 0 — no growth
	if got := cleanupCounterValue(t, reg, "crawler_ingest_bad_hashes_total"); got != 0 {
		t.Fatalf("после нулевого BadHashes счётчик = %v, want 0", got)
	}

	m.Observe(CleanupResult{BadHashes: 3})
	m.Observe(CleanupResult{BadHashes: 0})
	m.Observe(CleanupResult{BadHashes: 2})
	if got := cleanupCounterValue(t, reg, "crawler_ingest_bad_hashes_total"); got != 5 {
		t.Errorf("счётчик = %v, want 5 (3 + 2, нулевые итоги не меняют)", got)
	}
}

// TestCleanupMetricsNoRegistry — without a registry there is no
// collector (isolated builds); Observe calls are a no-op without a
// panic.
func TestCleanupMetricsNoRegistry(t *testing.T) {
	if m := NewCleanupMetrics(nil); m != nil {
		t.Fatalf("NewCleanupMetrics(nil) = %v, want nil", m)
	}
	NewCleanupMetrics(nil).Observe(CleanupResult{BadHashes: 7}) // does not panic

	reg := metrics.New("ingest")
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == "crawler_ingest_bad_hashes_total" {
			t.Fatalf("прибор зарегистрирован без реестра: %s", mf.GetName())
		}
	}
}

// TestCleanupMetricsIdempotent — a repeated registration shares the same
// counter (T-076): calls through the second instance continue the
// series.
func TestCleanupMetricsIdempotent(t *testing.T) {
	reg := metrics.New("ingest")
	first := NewCleanupMetrics(reg)
	second := NewCleanupMetrics(reg) // duplicate registration — no panic

	first.Observe(CleanupResult{BadHashes: 4})
	second.Observe(CleanupResult{BadHashes: 6})
	if got := cleanupCounterValue(t, reg, "crawler_ingest_bad_hashes_total"); got != 10 {
		t.Errorf("счётчик = %v, want 10 (один прибор на оба экземпляра)", got)
	}
}

// TestCleanupDeletedOnPass — the loop hands the hook the result of every
// successful pass (including a zero one) and does not hand over failed
// ones: Warn is already recorded, and the metric must not lie about a
// pass that did not happen.
func TestCleanupDeletedOnPass(t *testing.T) {
	var mu sync.Mutex
	var passes []CleanupResult

	pool := &cleanupFakePool{
		// Pass 1 (immediately) — work with a junk hash; pass 2 — a DB
		// error; pass 3 — an empty set; then — empty ones.
		selects: []*cleanupFakeRows{
			cleanupRows([]any{int16(1), int64(10)}),
			cleanupRows(),
		},
		selectErrs: []error{nil, errors.New("бд недоступна")},
		deletes: []*cleanupFakeRows{cleanupRows(
			[]any{int16(1), int64(10), cleanupHash16(0xaa)},
			[]any{int16(1), int64(10), make([]byte, 9)},
		)},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- CleanupDeleted(ctx, pool, CleanupOptions{
			Interval: 2 * time.Millisecond,
			OnPass: func(res CleanupResult) {
				mu.Lock()
				defer mu.Unlock()
				passes = append(passes, res)
			},
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats().selects < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до третьего прохода: %+v", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("CleanupDeleted: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// After the cancel an extra empty pass is possible (a race between
	// the counter poll and cancel) — the first result and the absence of
	// the failed pass's data are deterministic: all passes after the
	// first are zero.
	if len(passes) < 2 {
		t.Fatalf("проходов в хуке = %d (%+v), want ≥2", len(passes), passes)
	}
	wantFirst := CleanupResult{Domains: 1, Results: 2, Contents: 1, BadHashes: 1}
	if passes[0] != wantFirst {
		t.Errorf("итог первого прохода = %+v, want %+v", passes[0], wantFirst)
	}
	for i, p := range passes[1:] {
		if p != (CleanupResult{}) {
			t.Errorf("проход %d после первого = %+v, want нулевой (ошибочный проход хуку не отдаётся)", i+1, p)
		}
	}
}

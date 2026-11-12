// Unit tests of the q:dead move counter (T-154): the hook increments the
// counter per stream (the queue label is the source stream), a nil
// registry — a nil hook (isolated builds), a repeated hook assembly
// shares the instrument (the T-076 idempotency).
package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// counterValue — the current value of the counter series by the queue
// label (no series — ok=false); the gaugeValue style (collect_test.go).
func counterValue(t *testing.T, g prometheus.Gatherer, name, queue string) (float64, bool) {
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
				if l.GetName() == LabelQueue && l.GetValue() == queue {
					return m.GetCounter().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// TestDeadBuriedHook — the hook increments crawler_queue_dead_buried_total
// {queue} over the source streams; a nil reg — nil (a worker without a
// registry works, the bury is silent — checked by the queue).
func TestDeadBuriedHook(t *testing.T) {
	if hook := NewDeadBuriedHook(nil); hook != nil {
		t.Fatalf("nil реестр: хук задан (%T), хочу nil", hook)
	}

	reg := New("dns-worker")
	hook := NewDeadBuriedHook(reg)
	hook("q:dns")
	hook("q:dns")
	hook("q:render")

	for queue, want := range map[string]float64{"q:dns": 2, "q:render": 1} {
		if v, ok := counterValue(t, reg, "crawler_queue_dead_buried_total", queue); !ok || v != want {
			t.Errorf("crawler_queue_dead_buried_total{queue=%q} = (%v, %v), хочу (%v, true)", queue, v, ok, want)
		}
	}
	if _, ok := counterValue(t, reg, "crawler_queue_dead_buried_total", "q:http"); ok {
		t.Error("серия q:http без смертей не должна существовать")
	}

	// A repeated hook assembly — the same instrument (the T-076
	// idempotency): the increments add up, not duplicate the registry.
	NewDeadBuriedHook(reg)("q:dns")
	if v, _ := counterValue(t, reg, "crawler_queue_dead_buried_total", "q:dns"); v != 3 {
		t.Errorf("после второго хука q:dns = %v, хочу 3", v)
	}
}

// TestDeadUnidentifiedHook — the hook increments
// crawler_queue_dead_unidentified_total{queue} over the source streams
// (T-260, FR-6.1); a nil reg — nil; an event does not write a row of the
// neighboring dead_buried death counter (the death of a task and the
// quarantine of an unidentifiable entry are different events); a repeated
// assembly shares the instrument (the T-076 idempotency).
func TestDeadUnidentifiedHook(t *testing.T) {
	if hook := NewDeadUnidentifiedHook(nil); hook != nil {
		t.Fatalf("nil реестр: хук задан (%T), хочу nil", hook)
	}

	reg := New("http-worker")
	hook := NewDeadUnidentifiedHook(reg)
	hook("q:http")
	hook("q:render-fallback")
	hook("q:http")

	for queue, want := range map[string]float64{"q:http": 2, "q:render-fallback": 1} {
		if v, ok := counterValue(t, reg, "crawler_queue_dead_unidentified_total", queue); !ok || v != want {
			t.Errorf("crawler_queue_dead_unidentified_total{queue=%q} = (%v, %v), хочу (%v, true)", queue, v, ok, want)
		}
	}
	if _, ok := counterValue(t, reg, "crawler_queue_dead_unidentified_total", "q:dns"); ok {
		t.Error("серия q:dns без событий не должна существовать")
	}
	if _, ok := counterValue(t, reg, "crawler_queue_dead_buried_total", "q:http"); ok {
		t.Error("карантин неидентифицируемой записи не должен писать dead_buried_total")
	}

	// A repeated hook assembly — the same instrument (the T-076
	// idempotency).
	NewDeadUnidentifiedHook(reg)("q:http")
	if v, _ := counterValue(t, reg, "crawler_queue_dead_unidentified_total", "q:http"); v != 3 {
		t.Errorf("после второго хука q:http = %v, хочу 3", v)
	}
}

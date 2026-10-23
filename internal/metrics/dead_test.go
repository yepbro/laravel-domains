// Юнит-тесты счётчика переводов в q:dead (T-154): хук инкрементит
// counter per-stream (метка queue — стрим-источник), nil-реестр —
// nil-хук (изолированные сборки), повторная сборка хука делит прибор
// (идемпотентность T-076).
package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// counterValue — текущее значение серии counter по метке queue
// (нет серии — ok=false); стиль gaugeValue (collect_test.go).
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

// TestDeadBuriedHook — хук инкрементит crawler_queue_dead_buried_total
// {queue} по стримам-источникам; nil reg — nil (воркер без реестра
// работает, bury молчалив — проверяется очередью).
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

	// Повторная сборка хука — тот же прибор (идемпотентность T-076):
	// инкременты складываются, а не дублируются реестром.
	NewDeadBuriedHook(reg)("q:dns")
	if v, _ := counterValue(t, reg, "crawler_queue_dead_buried_total", "q:dns"); v != 3 {
		t.Errorf("после второго хука q:dns = %v, хочу 3", v)
	}
}

// Юнит-тесты приборов этапа анализа (T-077, ТЗ 5.9): nil-безопасность,
// экспорт значений через Gather (счётчик задач и гистограмма
// длительностей), инкременты в типовом пути processDelivery — на фейках
// пула, хранилища и набора правил из worker_test.go.
package analyzer

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// anMetricValue — значение серии метрики name, все метки labels совпадают
// (для гистограммы — счётчик наблюдений). Серия не найдена — −1.
func anMetricValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
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
		t.Fatal("NewMetrics(nil) != nil — изолированные сборки без реестра")
	}
	var m *Metrics
	m.Task(time.Millisecond) // no-op, не паникует
}

func TestMetricsTask(t *testing.T) {
	reg := metrics.New("analyzer")
	m := NewMetrics(reg)

	m.Task(500 * time.Microsecond)
	m.Task(2 * time.Millisecond)

	stage := map[string]string{"stage": metrics.StageAnalyze}
	if got := anMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 2 {
		t.Errorf("crawler_stage_tasks_total = %v, want 2", got)
	}
	if got := anMetricValue(t, reg, "crawler_stage_seconds", stage); got != 2 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 2", got)
	}
	// Этап не имеет своих счётчиков ошибок/статусов (пассивный анализ):
	// одноимённые метрики других этапов в этом реестре не появляются.
	for _, name := range []string{"crawler_stage_errors_total", "crawler_domain_status_total"} {
		if got := anMetricValue(t, reg, name, nil); got != -1 {
			t.Errorf("%s = %v, want серия отсутствует (не этап-источник)", name, got)
		}
	}
}

// TestMetricsProcessDelivery — прибор инкрементируется типовым путём:
// доставка q:analyze → анализ → Task (до XACK).
func TestMetricsProcessDelivery(t *testing.T) {
	reg := metrics.New("analyzer")
	f := newANFixture(t, anSet(7), Options{Metrics: reg})
	hash := anTestHash(1)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])

	if err := f.w.processDelivery(context.Background(), anDelivery("e1")); err != nil {
		t.Fatalf("processDelivery: %v", err)
	}

	stage := map[string]string{"stage": metrics.StageAnalyze}
	if got := anMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
		t.Errorf("crawler_stage_tasks_total = %v, want 1", got)
	}
	if got := anMetricValue(t, reg, "crawler_stage_seconds", stage); got != 1 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 1", got)
	}
}

// Юнит-тесты приборов этапа рендера (T-077, ТЗ 5.9): nil-безопасность,
// экспорт значений через Gather (счётчик задач и длительности, ошибки
// по классам render.Class, ветка rendered=true доли рендера),
// инкременты в типовом пути processDelivery — на фейках пула,
// хранилища, рендера и очереди из worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/render"
)

// rwMetricValue — значение серии метрики name, все метки labels совпадают
// (для гистограммы — счётчик наблюдений). Серия не найдена — −1.
func rwMetricValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
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
	m.Task(time.Second)       // no-op, не паникует
	m.Rendered()              // no-op
	m.Error(render.ClassTask) // no-op
	m.TarpitFinalized()       // no-op (T-148)
	m.CertFinalized()         // no-op (T-159)
}

func TestMetricsMethods(t *testing.T) {
	reg := metrics.New("render-worker")
	m := NewMetrics(reg)

	m.Task(2 * time.Second)
	m.Task(3 * time.Second)
	m.Rendered()
	m.Error(render.ClassTask)
	m.Error(render.ClassTask)
	m.Error(render.ClassBrowser)
	m.TarpitFinalized()
	m.CertFinalized()

	stage := map[string]string{"stage": metrics.StageRender}
	if got := rwMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 2 {
		t.Errorf("crawler_stage_tasks_total = %v, want 2", got)
	}
	if got := rwMetricValue(t, reg, "crawler_stage_seconds", stage); got != 2 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 2", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 1 {
		t.Errorf("crawler_render_records_total{rendered=\"true\"} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 1", got)
	}
	for typ, want := range map[string]float64{
		render.ClassTask.String():    2,
		render.ClassBrowser.String(): 1,
		render.ClassContext.String(): -1,
	} {
		labels := map[string]string{"stage": metrics.StageRender, "type": typ}
		if got := rwMetricValue(t, reg, "crawler_stage_errors_total", labels); got != want {
			t.Errorf("crawler_stage_errors_total{type=%s} = %v, want %v", typ, got, want)
		}
	}
}

// TestMetricsProcessDelivery — приборы инкрементируются типовым путём:
// успешная доставка (DOM записан → rendered=true) и неудачная (класс
// ошибки страницы → счётчик ошибок, задача не посчитана).
func TestMetricsProcessDelivery(t *testing.T) {
	reg := metrics.New("render-worker")

	t.Run("успех", func(t *testing.T) {
		pool := newRWFakePool()
		store := &rwFakeStore{j: pool.j, p: pool}
		renderer := &rwFakeRenderer{j: pool.j, output: rwOutput("<html><body>dom</body></html>")}
		q := &rwFakeQueue{j: pool.j}
		w, err := New(pool, q, store, renderer, Options{Metrics: reg})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		pool.seedRow(rwKey, "https://spa.example/", &raw)

		if err := w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		stage := map[string]string{"stage": metrics.StageRender}
		if got := rwMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
			t.Errorf("crawler_stage_tasks_total = %v, want 1", got)
		}
		if got := rwMetricValue(t, reg, "crawler_stage_seconds", stage); got != 1 {
			t.Errorf("crawler_stage_seconds (count) = %v, want 1", got)
		}
		if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 1 {
			t.Errorf("crawler_render_records_total{rendered=\"true\"} = %v, want 1", got)
		}
	})

	t.Run("ошибка рендера — класс в счётчик ошибок", func(t *testing.T) {
		pool := newRWFakePool()
		store := &rwFakeStore{j: pool.j, p: pool}
		renderer := &rwFakeRenderer{j: pool.j,
			err: &render.Error{Class: render.ClassTask, URL: "https://spa.example/", Err: errors.New("таймаут")}}
		q := &rwFakeQueue{j: pool.j}
		w, err := New(pool, q, store, renderer, Options{Metrics: reg})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		pool.seedRow(rwKey, "https://spa.example/", nil)

		if err := w.processDelivery(context.Background(), rwDelivery("e2"), 1); err == nil {
			t.Fatal("processDelivery ошибочного рендера = nil, want ошибка")
		}
		stage := map[string]string{"stage": metrics.StageRender}
		if got := rwMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
			t.Errorf("crawler_stage_tasks_total = %v, want по-прежнему 1 (ошибка не считается задачей)", got)
		}
		labels := map[string]string{"stage": metrics.StageRender, "type": render.ClassTask.String()}
		if got := rwMetricValue(t, reg, "crawler_stage_errors_total", labels); got != 1 {
			t.Errorf("crawler_stage_errors_total{task} = %v, want 1", got)
		}
	})
}

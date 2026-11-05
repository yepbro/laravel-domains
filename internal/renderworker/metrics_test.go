// Юнит-тесты приборов этапа рендера (T-077, ТЗ 5.9): nil-безопасность,
// экспорт значений через Gather (счётчик задач и длительности, ошибки
// по классам render.Class, ветка rendered=true доли рендера), метка
// engine движка сборки на доле рендера и финализациях класса задачи
// (T-235), инкременты в типовом пути processDelivery — на фейках пула,
// хранилища, рендера и очереди из worker_test.go; потребление фолбэка
// с меткой исхода (T-245) — точки проводки воркера B покрыты
// TestRenderWorker_FallbackConsumedMetrics (fallback_test.go).
package renderworker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/config"
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
	if NewMetrics(nil, config.EngineChromium) != nil {
		t.Fatal("NewMetrics(nil) != nil — изолированные сборки без реестра")
	}
	var m *Metrics
	m.Task(time.Second)                  // no-op, не паникует
	m.Rendered()                         // no-op
	m.Error(render.ClassTask)            // no-op
	m.TarpitFinalized()                  // no-op (T-148)
	m.CertFinalized()                    // no-op (T-159)
	m.DNSFinalized()                     // no-op (T-211)
	m.TaskFinalized("other")             // no-op (T-172)
	m.FallbackPublished("timeout")       // no-op (T-238)
	m.FallbackSuspended(fbReasonGate)    // no-op (T-239)
	m.FallbackSuspended(fbReasonBudget)  // no-op (T-239)
	m.FallbackConsumed(fbResultRendered) // no-op (T-245)
}

func TestMetricsMethods(t *testing.T) {
	reg := metrics.New("render-worker")
	m := NewMetrics(reg, config.EngineLightpanda)

	m.Task(2 * time.Second)
	m.Task(3 * time.Second)
	m.Rendered()
	m.Error(render.ClassTask)
	m.Error(render.ClassTask)
	m.Error(render.ClassBrowser)
	m.TarpitFinalized()
	m.CertFinalized()
	m.DNSFinalized()
	m.TaskFinalized("net::ERR_CONNECTION_REFUSED")
	m.TaskFinalized("net::ERR_CONNECTION_REFUSED")
	m.TaskFinalized("other")
	m.FallbackPublished("timeout")
	m.FallbackPublished("timeout")
	m.FallbackPublished("net::ERR_CONNECTION_REFUSED")
	m.FallbackSuspended(fbReasonGate)
	m.FallbackSuspended(fbReasonBudget)
	m.FallbackSuspended(fbReasonBudget)
	m.FallbackConsumed(fbResultRendered)
	m.FallbackConsumed(fbResultRendered)
	m.FallbackConsumed(fbResultFinalized)
	m.FallbackConsumed(fbResultSkipped)

	stage := map[string]string{"stage": metrics.StageRender}
	if got := rwMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 2 {
		t.Errorf("crawler_stage_tasks_total = %v, want 2", got)
	}
	if got := rwMetricValue(t, reg, "crawler_stage_seconds", stage); got != 2 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 2", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true", "engine": config.EngineLightpanda}); got != 1 {
		t.Errorf("crawler_render_records_total{rendered=\"true\",engine=%s} = %v, want 1", config.EngineLightpanda, got)
	}
	// Метка engine фиксирована сборкой (T-235): серий чужого движка и
	// без неё воркер не экспортирует — кардинальность 3 держит
	// инициализация, а не вызывающий.
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true", "engine": config.EngineChromium}); got != -1 {
		t.Errorf("crawler_render_records_total{engine=%s} = %v, want -1 (серия чужого движка)", config.EngineChromium, got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_dns_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_dns_finalized_total = %v, want 1", got)
	}
	for reason, want := range map[string]float64{
		"net::ERR_CONNECTION_REFUSED": 2,
		"other":                       1,
		"net::ERR_NAME_NOT_RESOLVED":  -1,
	} {
		labels := map[string]string{"reason": reason, "engine": config.EngineLightpanda}
		if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", labels); got != want {
			t.Errorf("crawler_render_task_finalized_total{reason=%s,engine=%s} = %v, want %v", reason, config.EngineLightpanda, got, want)
		}
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": "other", "engine": config.EngineMoli}); got != -1 {
		t.Errorf("crawler_render_task_finalized_total{engine=%s} = %v, want -1 (серия чужого движка)", config.EngineMoli, got)
	}
	for reason, want := range map[string]float64{
		"timeout":                     2,
		"net::ERR_CONNECTION_REFUSED": 1,
		"other":                       -1, // нет серии
	} {
		if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total", map[string]string{"reason": reason}); got != want {
			t.Errorf("crawler_render_fallback_published_total{reason=%s} = %v, want %v", reason, got, want)
		}
	}
	// Приостановки фолбэка (T-239): события с причиной gate|budget —
	// замкнутый словарь, иных серий нет.
	for reason, want := range map[string]float64{
		fbReasonGate:   1,
		fbReasonBudget: 2,
	} {
		if got := rwMetricValue(t, reg, "crawler_render_fallback_suspended_total", map[string]string{"reason": reason}); got != want {
			t.Errorf("crawler_render_fallback_suspended_total{reason=%s} = %v, want %v", reason, got, want)
		}
	}
	// Потребление фолбэка (T-245): исходы rendered|finalized|skipped —
	// замкнутый словарь, иных серий нет (считает сборка воркера B,
	// проводка — TestRenderWorker_FallbackConsumedMetrics).
	for result, want := range map[string]float64{
		fbResultRendered:  2,
		fbResultFinalized: 1,
		fbResultSkipped:   1,
		"other":           -1,
	} {
		if got := rwMetricValue(t, reg, "crawler_render_fallback_consumed_total", map[string]string{"result": result}); got != want {
			t.Errorf("crawler_render_fallback_consumed_total{result=%s} = %v, want %v", result, got, want)
		}
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

// TestMetricsEngineDefault — пустое значение engine (сборки без
// конфигурации, Options{Metrics: reg} в тестах) трактуется как
// chromium, как у render.Options.Engine (T-235): нулевое значение
// Options не расширяет кардинальность метки за пределы словаря из 3
// движков.
func TestMetricsEngineDefault(t *testing.T) {
	reg := metrics.New("render-worker")
	m := NewMetrics(reg, "")

	m.Rendered()
	m.TaskFinalized("other")

	if got := rwMetricValue(t, reg, "crawler_render_records_total",
		map[string]string{"rendered": "true", "engine": config.EngineChromium}); got != 1 {
		t.Errorf("crawler_render_records_total{engine=%s} = %v, want 1", config.EngineChromium, got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": "other", "engine": config.EngineChromium}); got != 1 {
		t.Errorf("crawler_render_task_finalized_total{engine=%s} = %v, want 1", config.EngineChromium, got)
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
		// Engine Options воркера → метка engine приборов (T-235).
		w, err := New(pool, q, store, renderer, Options{Metrics: reg, Engine: config.EngineMoli})
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
		if got := rwMetricValue(t, reg, "crawler_render_records_total",
			map[string]string{"rendered": "true", "engine": config.EngineMoli}); got != 1 {
			t.Errorf("crawler_render_records_total{rendered=\"true\",engine=%s} = %v, want 1", config.EngineMoli, got)
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

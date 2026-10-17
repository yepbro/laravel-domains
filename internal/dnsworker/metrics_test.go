// Юнит-тесты приборов DNS-этапа (T-077, ТЗ 5.9): nil-безопасность,
// регистрация и экспорт значений через Gather (счётчик эпизодов,
// гистограмма длительностей, счётчик финальных статусов), инкременты
// в типовом пути цикла Run — на фейках пула/очереди/резолвера из
// result_test.go (два домена: с адресами — без статуса, NXDOMAIN —
// not_delegated).
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

// dnsMetricValue — значение серии counter/histogram-семейства name с
// меткой label (для гистограммы берётся счётчик наблюдений _count).
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
	return -1 // серия не найдена — тест провалит сравнением
}

func TestMetricsNilSafe(t *testing.T) {
	if NewMetrics(nil) != nil {
		t.Fatal("NewMetrics(nil) != nil — изолированные сборки без реестра")
	}
	var m *Metrics
	m.Episode(time.Second)             // no-op, не паникует
	m.DomainStatus(model.StatusNoHTTP) // no-op
}

func TestMetricsRegistered(t *testing.T) {
	reg := metrics.New("dns-worker")
	m := NewMetrics(reg)
	if m == nil {
		t.Fatal("NewMetrics с реестром = nil")
	}

	m.Episode(150 * time.Millisecond)
	m.Episode(40 * time.Millisecond)
	m.DomainStatus(model.StatusNotDelegated)
	m.DomainStatus(model.StatusTempError)
	m.DomainStatus(model.StatusTempError)

	// Проверки серий (имя + значение метки): задачи и длительности.
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
		t.Errorf("crawler_domain_status_total{alive} = %v, want серия отсутствует (живой статус пишет HTTP-этап)", got)
	}
}

// TestMetricsRunSmoke — приборы инкрементируются типовым путём цикла:
// пачка q:dns с двумя доменами (адреса есть — без статуса; NXDOMAIN —
// not_delegated) → 2 эпизода, 1 терминальный статус.
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
		t.Fatal("XACK не дождались: доставка не обработана")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}

	if got := dnsMetricValue(t, reg, "crawler_stage_tasks_total", metrics.StageDNS); got != 2 {
		t.Errorf("crawler_stage_tasks_total = %v, want 2 эпизода", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_stage_seconds", metrics.StageDNS); got != 2 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 2", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_domain_status_total", model.StatusNotDelegated.String()); got != 1 {
		t.Errorf("crawler_domain_status_total{not_delegated} = %v, want 1", got)
	}
}

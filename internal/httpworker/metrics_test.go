// Юнит-тесты приборов HTTP-этапа (T-077, ТЗ 5.9): nil-безопасность,
// экспорт значений через Gather (счётчик задач и длительности, ошибки
// по классам ErrorClass, финальный статус, время ответа без кэш-
// попаданий, записи rendered=false), инкременты в типовом пути цикла
// Run — на фейках шва загрузки и очереди из worker_test.go.
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

// httpMetricValue — значение серии метрики name, все метки labels
// совпадают (для гистограммы — счётчик наблюдений). Серия не найдена —
// −1 (различимо от честного нуля).
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
		t.Fatal("NewMetrics(nil) != nil — изолированные сборки без реестра")
	}
	var m *Metrics
	m.Episode(&DomainResult{Name: "x"}, time.Second) // no-op, не паникует
	m.Episode(nil, time.Second)                      // защита прямых вызовов
	m.SlotEnter()
	m.SlotLeave()
	m.BodyHeld(1 << 20)
	m.BodyHeld(-(1 << 20))
	m.BodySize(4096)
}

// TestMetricsSlotBody — приборы памяти слота (T-163): занятость слота
// парой вход/выход, транзитные байты тел сходимой парой ±n, размер
// тела — наблюдение гистограммы. Пары обязаны сводить gauges к нулю —
// иначе сэмплер намерил бы утечку там, где её нет.
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
		t.Errorf("crawler_http_tasks_inflight после выходов = %v, want 0", got)
	}

	m.BodyHeld(65536)
	m.BodyHeld(4096)
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes_inflight", nil); got != 69632 {
		t.Errorf("crawler_http_body_bytes_inflight = %v, want 69632", got)
	}
	m.BodyHeld(-69632)
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes_inflight", nil); got != 0 {
		t.Errorf("crawler_http_body_bytes_inflight после выхода окна = %v, want 0", got)
	}

	m.BodySize(20000)
	m.BodySize(20000)
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes", nil); got != 2 {
		t.Errorf("crawler_http_body_bytes (count) = %v, want 2", got)
	}
}

// TestMetricsEpisode — один эпизод: две проверки с ошибками разных
// классов, одна доступная некэшированная (время ответа), одна кэш-
// попадание (без времени ответа), две записи результата → alive.
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
		t.Errorf("crawler_domain_status_total{alive} = %v, want 1 (записи есть)", got)
	}
	// Время ответа: только доступная некэшированная проверка (кэш сеть
	// не трогал, ResponseMS = 0).
	if got := httpMetricValue(t, reg, "crawler_http_response_seconds", nil); got != 1 {
		t.Errorf("crawler_http_response_seconds (count) = %v, want 1", got)
	}
	// Доля рендера: 2 записи с rendered=false.
	if got := httpMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "false"}); got != 2 {
		t.Errorf("crawler_render_records_total{rendered=\"false\"} = %v, want 2", got)
	}
	if got := httpMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != -1 {
		t.Errorf("crawler_render_records_total{rendered=\"true\"} = %v, want серия отсутствует (ветка рендера)", got)
	}
}

// TestMetricsEpisodeCutoff — приборы dead-host cutoff (T-164): задача с
// признаком Cutoff инкрементирует счётчик cutoff-задач, синтетические
// ErrCutoff — счётчик сэкономленных проверок; ошибками этапа пропущенные
// проверки не считаются (серии type=cutoff у crawler_stage_errors_total
// нет), выполненные проверки головы считаются как обычно.
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
		t.Errorf("crawler_stage_errors_total{type=cutoff} = %v, want серия отсутствует (проверка не выполнялась)", got)
	}
	netLabels := map[string]string{"stage": metrics.StageHTTP, "type": ErrNetwork.String()}
	if got := httpMetricValue(t, reg, "crawler_stage_errors_total", netLabels); got != 4 {
		t.Errorf("crawler_stage_errors_total{type=network} = %v, want 4 (проверки головы)", got)
	}
}

// TestMetricsRunSmoke — приборы инкрементируются типовым путём цикла:
// доставка q:http → проверки → OnResult → Episode (до XACK).
func TestMetricsRunSmoke(t *testing.T) {
	body := "<html>" + strings.Repeat("страница для дымового теста метрик http ", 30) + "</html>"
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
		t.Fatal("Run не завершился после отмены ctx")
	}
	if q.ackedCount() != 1 {
		t.Fatalf("XACK = %d, want 1 — обработка не дошла до подтверждения", q.ackedCount())
	}

	stage := map[string]string{"stage": metrics.StageHTTP}
	if got := httpMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
		t.Errorf("crawler_stage_tasks_total = %v, want 1", got)
	}
	if got := httpMetricValue(t, reg, "crawler_stage_seconds", stage); got != 1 {
		t.Errorf("crawler_stage_seconds (count) = %v, want 1", got)
	}
	// Оба протокола отвечают одинаково → один вариант записи (T-054);
	// статус домена — alive.
	mu.Lock()
	recN := records
	mu.Unlock()
	if got := httpMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "false"}); got != float64(recN) || got < 1 {
		t.Errorf("crawler_render_records_total{false} = %v, want %v (≥1)", got, recN)
	}
	if got := httpMetricValue(t, reg, "crawler_domain_status_total", map[string]string{"type": model.StatusAlive.String()}); got != 1 {
		t.Errorf("crawler_domain_status_total{alive} = %v, want 1", got)
	}
	// https и http — независимые проверки сети: два наблюдения времени
	// ответа.
	if got := httpMetricValue(t, reg, "crawler_http_response_seconds", nil); got != 2 {
		t.Errorf("crawler_http_response_seconds (count) = %v, want 2", got)
	}
}

// TestMetricsSlotMemoryRun — приборы памяти слота (T-163) в типовом
// пути цикла: во время обработки (OnResult вызывается внутри
// processDelivery — детерминированная точка) занят ровно один слот,
// транзитные байты тел уже освобены (окно удержания закрывается в
// checkScheme до возврата); после завершения цикла слоты и байты
// сходятся к нулю, прочитанные тела — в гистограмме смеси (обе проверки
// протоколов читают тело, кэш итоговых URL на разные схемы не
// распространяется).
func TestMetricsSlotMemoryRun(t *testing.T) {
	body := "<html>" + strings.Repeat("страница для замера памяти слота ", 30) + "</html>"
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
		t.Fatal("Run не завершился после отмены ctx")
	}

	if inSlots != 1 {
		t.Errorf("crawler_http_tasks_inflight внутри обработки = %v, want 1", inSlots)
	}
	// Тела прочитаны и отпущены до OnResult (ранний Put T-147): окно
	// удержания закрыто в checkScheme.
	if inBodyHeld != 0 {
		t.Errorf("crawler_http_body_bytes_inflight внутри OnResult = %v, want 0 (тела уже отпущены)", inBodyHeld)
	}
	if got := httpMetricValue(t, reg, "crawler_http_tasks_inflight", nil); got != 0 {
		t.Errorf("crawler_http_tasks_inflight после обработки = %v, want 0", got)
	}
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes_inflight", nil); got != 0 {
		t.Errorf("crawler_http_body_bytes_inflight после обработки = %v, want 0", got)
	}
	if got := httpMetricValue(t, reg, "crawler_http_body_bytes", nil); got != 2 {
		t.Errorf("crawler_http_body_bytes (count) = %v, want 2 (https и http)", got)
	}
}

package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// scrape raises a temporary HTTP server with the Handler and returns the
// body of the /metrics response.
func scrape(t *testing.T, g prometheus.Gatherer) string {
	t.Helper()
	srv := httptest.NewServer(Handler(g))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestHandler(t *testing.T) {
	srv := httptest.NewServer(Handler(New("test-service")))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("статус = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain (формат Prometheus)", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		`crawler_service_up{service="test-service"} 1`,
		`crawler_build_info{mode="dev-stub",revision="",service="test-service",version="dev"} 1`,
		`go_goroutines`, // the Go/runtime collector
	} {
		if !strings.Contains(text, want) {
			t.Errorf("в /metrics нет строки %q", want)
		}
	}
}

// buildMode/buildVersion/buildRevision — the ldflags injection points:
// substituting the variables changes the crawler_build_info labels (the
// production build -X ...buildMode=prod, see TestBuildInfoVersionRevision).
func TestBuildInfoMode(t *testing.T) {
	prev := buildMode
	buildMode = "prod"
	defer func() { buildMode = prev }()

	if text := scrape(t, New("svc")); !strings.Contains(text, `crawler_build_info{mode="prod",revision="",service="svc",version="dev"} 1`) {
		t.Errorf("в /metrics нет crawler_build_info с инъекцированным mode:\n%s", text)
	}
}

// A production build injects both the version and the revision (git
// describe and the short commit hash): all three labels get into the
// exposition; the label name set is the same as that of a dev build —
// the values are substituted in place.
func TestBuildInfoVersionRevision(t *testing.T) {
	prevV, prevR := buildVersion, buildRevision
	buildVersion = "v1.2.3-4-gabc1234-dirty"
	buildRevision = "abc1234"
	defer func() { buildVersion, buildRevision = prevV, prevR }()

	if text := scrape(t, New("svc")); !strings.Contains(text,
		`crawler_build_info{mode="dev-stub",revision="abc1234",service="svc",version="v1.2.3-4-gabc1234-dirty"} 1`,
	) {
		t.Errorf("в /metrics нет crawler_build_info с инъекцированными version/revision:\n%s", text)
	}
}

// Handler accepts a bare *prometheus.Registry (Gatherer) too, not only a
// registry of the factory.
func TestHandlerBareRegistry(t *testing.T) {
	bare := prometheus.NewRegistry()
	bare.MustRegister(collectors.NewGoCollector())
	if text := scrape(t, bare); !strings.Contains(text, "go_goroutines") {
		t.Error("Handler должен принимать *prometheus.Registry напрямую")
	}
}

// The factory methods substitute the service label automatically: the
// caller passes the values of the other labels only; the HELP/TYPE come
// from the Opts (spec 5.9).
func TestFactoryAutoServiceLabel(t *testing.T) {
	reg := New("dns-worker")

	tasks := reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}, LabelStage)
	tasks.WithLabelValues("dns").Inc()

	depth := reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_queue_depth",
		Help: "Длина очереди задач.",
	}, LabelQueue)
	depth.WithLabelValues("q:dns").Set(42)

	// An instrument without extra labels: WithLabelValues without
	// arguments
	memory := reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_worker_memory_bytes",
		Help: "Память воркеров.",
	})
	memory.WithLabelValues().Set(1 << 20)

	latency := reg.MustHistogram(prometheus.HistogramOpts{
		Name:    "crawler_stage_seconds",
		Help:    "Время обработки этапа.",
		Buckets: []float64{0.1, 0.5, 1},
	}, LabelStage)
	latency.WithLabelValues("http").Observe(0.25)

	errs := reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_stage_errors_total",
		Help: "Ошибки по типам.",
	}, LabelStage, LabelType)
	errs.WithLabelValues("dns", "timeout").Add(3)

	text := scrape(t, reg)
	for _, want := range []string{
		// the labels in the Prometheus exposition are sorted by name;
		// large values are formatted with an exponent (1.048576e+06)
		`crawler_stage_tasks_total{service="dns-worker",stage="dns"} 1`,
		`crawler_queue_depth{queue="q:dns",service="dns-worker"} 42`,
		`crawler_worker_memory_bytes{service="dns-worker"} 1.048576e+06`,
		`crawler_stage_errors_total{service="dns-worker",stage="dns",type="timeout"} 3`,
		// the metric documentation (HELP/TYPE) — from the Opts
		`# HELP crawler_stage_tasks_total Обработанные домены по этапам конвейера.`,
		`# TYPE crawler_stage_tasks_total counter`,
		`# TYPE crawler_queue_depth gauge`,
		`# TYPE crawler_stage_seconds histogram`,
		`crawler_stage_seconds_bucket{service="dns-worker",stage="http",le="0.5"} 1`,
		`crawler_stage_seconds_sum{service="dns-worker",stage="http"} 0.25`,
		`crawler_stage_seconds_count{service="dns-worker",stage="http"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("в /metrics нет строки %q", want)
		}
	}
}

// A duplicate registration of an identical metric does not panic and is
// not an error: the already registered vector is returned — the calls
// share one collector (the instrument constructors may be called
// repeatedly, as the service tests do).
func TestReRegistrationSameCollector(t *testing.T) {
	reg := New("test-svc")
	counterOpts := prometheus.CounterOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}
	first := reg.MustCounter(counterOpts, LabelStage)
	second, err := reg.Counter(counterOpts, LabelStage)
	if err != nil {
		t.Fatalf("дубль-регистрация counter: %v", err)
	}
	first.WithLabelValues("dns").Inc()
	second.WithLabelValues("dns").Add(2)

	gaugeOpts := prometheus.GaugeOpts{
		Name: "crawler_queue_depth",
		Help: "Длина очереди задач.",
	}
	if _, err := reg.Gauge(gaugeOpts, LabelQueue); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Gauge(gaugeOpts, LabelQueue); err != nil {
		t.Fatalf("дубль-регистрация gauge: %v", err)
	}
	histOpts := prometheus.HistogramOpts{
		Name: "crawler_stage_seconds",
		Help: "Время обработки этапа.",
	}
	if _, err := reg.Histogram(histOpts, LabelStage); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Histogram(histOpts, LabelStage); err != nil {
		t.Fatalf("дубль-регистрация histogram: %v", err)
	}

	text := scrape(t, reg)
	want := `crawler_stage_tasks_total{service="test-svc",stage="dns"} 3`
	if !strings.Contains(text, want) {
		t.Errorf("в /metrics нет %q: дубли должны делить один коллектор", want)
	}
}

// The descriptor conflicts are an error (not a panic); the Must variant
// panics: the same name with another help, another label set or another
// collector type is a program error.
func TestRegistrationConflict(t *testing.T) {
	reg := New("test-svc")
	opts := prometheus.CounterOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}
	if _, err := reg.Counter(opts, LabelStage); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Counter(opts, LabelQueue); err == nil {
		t.Error("другой набор метк: ожидалась ошибка")
	}
	otherHelp := opts
	otherHelp.Help = "Другой help."
	if _, err := reg.Counter(otherHelp, LabelStage); err == nil {
		t.Error("другой help: ожидалась ошибка")
	}
	// the same name/help/labels but a gauge: an AlreadyRegisteredError
	// with the same descriptor but a collector of another type
	if _, err := reg.Gauge(prometheus.GaugeOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}, LabelStage); err == nil {
		t.Error("другой тип коллектора: ожидалась ошибка")
	}
	// histogram against counter — the same type conflict
	if _, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}, LabelStage); err == nil {
		t.Error("histogram против counter: ожидалась ошибка")
	}

	// The Must variants panic on a conflict (a programming error)
	mustPanic := func(name string, f func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("%s на конфликте: ожидалась паника", name)
			}
		}()
		f()
	}
	mustPanic("MustGauge", func() {
		reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные домены по этапам конвейера.",
		}, LabelStage)
	})
	mustPanic("MustCounter", func() {
		reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные домены по этапам конвейера.",
		}, LabelQueue)
	})
	mustPanic("MustHistogram", func() {
		reg.MustHistogram(prometheus.HistogramOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные домены по этапам конвейера.",
		}, LabelQueue)
	})
}

// The reserved LabelService label and duplicate labels are rejected with
// an error, not a prometheus.New*Vec panic.
func TestLabelValidation(t *testing.T) {
	reg := New("test-svc")
	if _, err := reg.Counter(prometheus.CounterOpts{
		Name: "crawler_a_total",
		Help: "Счётчик.",
	}, LabelService); err == nil {
		t.Error("LabelService: ожидалась ошибка")
	}
	if _, err := reg.Counter(prometheus.CounterOpts{
		Name: "crawler_a_total",
		Help: "Счётчик.",
	}, LabelStage, LabelStage); err == nil {
		t.Error("дубликат метки counter: ожидалась ошибка")
	}
	if _, err := reg.Gauge(prometheus.GaugeOpts{
		Name: "crawler_a",
		Help: "Гейдж.",
	}, LabelQueue, LabelQueue); err == nil {
		t.Error("дубликат метки gauge: ожидалась ошибка")
	}
	if _, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "crawler_a",
		Help: "Гистограмма.",
	}, LabelType, LabelType); err == nil {
		t.Error("дубликат метки histogram: ожидалась ошибка")
	}
	if text := scrape(t, reg); strings.Contains(text, "crawler_a") {
		t.Error("метрика с невалидными метками не должна была зарегистрироваться")
	}
}

// The histogram buckets are not part of the prometheus descriptor: a
// repeated registration returns the vector with the buckets of the first
// call — the behavior is fixed (the buckets of a registered vector cannot
// be compared).
func TestReRegistrationHistogramBuckets(t *testing.T) {
	reg := New("test-svc")
	first := reg.MustHistogram(prometheus.HistogramOpts{
		Name:    "crawler_stage_seconds",
		Help:    "Время обработки этапа.",
		Buckets: []float64{0.5, 1},
	}, LabelStage)
	second, err := reg.Histogram(prometheus.HistogramOpts{
		Name:    "crawler_stage_seconds",
		Help:    "Время обработки этапа.",
		Buckets: []float64{9, 99}, // ignored: the first call wins
	}, LabelStage)
	if err != nil {
		t.Fatalf("дубль-регистрация histogram: %v", err)
	}
	first.WithLabelValues("http").Observe(0.75)
	second.WithLabelValues("http").Observe(0.25)

	text := scrape(t, reg)
	for _, want := range []string{
		`crawler_stage_seconds_bucket{service="test-svc",stage="http",le="0.5"} 1`,
		`crawler_stage_seconds_bucket{service="test-svc",stage="http",le="1"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("в /metrics нет строки %q (корзины первого вызова)", want)
		}
	}
	if strings.Contains(text, `le="9"`) || strings.Contains(text, `le="99"`) {
		t.Error("корзины повторного вызова не должны попадать в экспорт")
	}
}

	// The parallel registration of same-named metrics and the increments
	// are safe: the prometheus registry is synchronized, the curried
	// vectors are thread-safe (a run with -race catches the races).
func TestConcurrentRegistration(t *testing.T) {
	reg := New("test-svc")
	const goroutines = 16
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vec, err := reg.Counter(prometheus.CounterOpts{
				Name: "crawler_stage_tasks_total",
				Help: "Обработанные домены по этапам конвейера.",
			}, LabelStage)
			if err != nil {
				t.Error(err)
				return
			}
			vec.WithLabelValues("dns").Inc()
		}()
	}
	wg.Wait()
	want := `crawler_stage_tasks_total{service="test-svc",stage="dns"} 16`
	if text := scrape(t, reg); !strings.Contains(text, want) {
		t.Errorf("в /metrics нет строки %q", want)
	}
}

// The service registries are independent: the same-named metrics of
// different services do not conflict and do not leak into each other —
// each service has its own /metrics (spec 5.9).
func TestRegistriesIndependent(t *testing.T) {
	regDNS, regHTTP := New("dns-worker"), New("http-worker")
	opts := prometheus.CounterOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}
	regDNS.MustCounter(opts, LabelStage).WithLabelValues("dns").Inc()
	regHTTP.MustCounter(opts, LabelStage).WithLabelValues("http").Add(7)

	dnsText := scrape(t, regDNS)
	if want := `crawler_stage_tasks_total{service="dns-worker",stage="dns"} 1`; !strings.Contains(dnsText, want) {
		t.Errorf("в /metrics dns-worker нет строки %q", want)
	}
	if strings.Contains(dnsText, `stage="http"`) {
		t.Error("метрики http-worker попали в реестр dns-worker")
	}
	if want := `crawler_stage_tasks_total{service="http-worker",stage="http"} 7`; !strings.Contains(scrape(t, regHTTP), want) {
		t.Errorf("в /metrics http-worker нет строки %q", want)
	}
}

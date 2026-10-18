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

// scrape поднимает временный HTTP-сервер с Handler и возвращает тело
// ответа /metrics.
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
		`go_goroutines`, // коллектор Go/runtime
	} {
		if !strings.Contains(text, want) {
			t.Errorf("в /metrics нет строки %q", want)
		}
	}
}

// buildMode/buildVersion/buildRevision — точки инъекции ldflags: подмена
// переменных меняет метки crawler_build_info (боевая сборка -X
// ...buildMode=prod, см. TestBuildInfoVersionRevision).
func TestBuildInfoMode(t *testing.T) {
	prev := buildMode
	buildMode = "prod"
	defer func() { buildMode = prev }()

	if text := scrape(t, New("svc")); !strings.Contains(text, `crawler_build_info{mode="prod",revision="",service="svc",version="dev"} 1`) {
		t.Errorf("в /metrics нет crawler_build_info с инъекцированным mode:\n%s", text)
	}
}

// Боевая сборка инъекцирует и version, и revision (git describe и короткий
// хеш коммита): все три метки попадают в экспозицию; набор имён меток тот
// же, что и у dev-сборки, — значения подменяются на месте.
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

// Handler принимает и голый *prometheus.Registry (Gatherer), не только
// реестр фабрики.
func TestHandlerBareRegistry(t *testing.T) {
	bare := prometheus.NewRegistry()
	bare.MustRegister(collectors.NewGoCollector())
	if text := scrape(t, bare); !strings.Contains(text, "go_goroutines") {
		t.Error("Handler должен принимать *prometheus.Registry напрямую")
	}
}

// Фабричные методы подставляют метку service автоматически: вызывающий
// передаёт значения только остальных метк; HELP/TYPE берутся из Opts
// (ТЗ 5.9).
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

	// прибор без дополнительных метк: WithLabelValues без аргументов
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
		// метки в экспозиции Prometheus отсортированы по имени;
		// большие значения форматируются экспонентой (1.048576e+06)
		`crawler_stage_tasks_total{service="dns-worker",stage="dns"} 1`,
		`crawler_queue_depth{queue="q:dns",service="dns-worker"} 42`,
		`crawler_worker_memory_bytes{service="dns-worker"} 1.048576e+06`,
		`crawler_stage_errors_total{service="dns-worker",stage="dns",type="timeout"} 3`,
		// документация метрик (HELP/TYPE) — из Opts
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

// Дубль-регистрация идентичной метрики не паникует и не ошибочна:
// возвращается уже зарегистрированный вектор — вызовы делят один
// коллектор (конструкторы приборов можно вызывать многократно, как это
// делают тесты сервисов).
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

// Конфликты описаний — ошибка (не паника), Must-вариант паникует:
// то же имя с другим help, другим набором меток или другим типом
// коллектора — программная ошибка.
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
	// то же имя/help/метки, но gauge: AlreadyRegisteredError с тем же
	// дескриптором, но коллектор другого типа
	if _, err := reg.Gauge(prometheus.GaugeOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}, LabelStage); err == nil {
		t.Error("другой тип коллектора: ожидалась ошибка")
	}
	// histogram против counter — тот же конфликт типа
	if _, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}, LabelStage); err == nil {
		t.Error("histogram против counter: ожидалась ошибка")
	}

	// Must-варианты на конфликте паникуют (ошибка программирования)
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

// Зарезервированная метка LabelService и дубликаты меток отклоняются
// ошибкой, а не паникой prometheus.New*Vec.
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

// Корзины гистограммы не входят в дескриптор prometheus: повторная
// регистрация возвращает вектор с корзинами первого вызова — поведение
// зафиксировано (сравнить корзины зарегистрированного вектора нельзя).
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
		Buckets: []float64{9, 99}, // проигнорируется: первый вызов выигрывает
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

// Параллельная регистрация одноимённых метрик и инкременты безопасны:
// реестр prometheus синхронизирован, каррированные векторы
// потокобезопасны (прогон с -race ловит гонки).
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

// Реестры сервисов независимы: одноимённые метрики разных сервисов не
// конфликтуют и не перетекают друг в друга — у каждого сервиса свой
// /metrics (ТЗ 5.9).
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

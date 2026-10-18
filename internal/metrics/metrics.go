// Package metrics — общая точка регистрации метрик Prometheus всех
// сервисов Domain Crawler (ТЗ 5.9): фабрика Registry поверх prometheus
// с единой меткой service.
//
// Каждый сервис создаёт собственный реестр New(name) — каркас
// internal/service поднимает его вместе с /healthz и /metrics: адрес
// monitor — monitor.listen из конфигурации (ТЗ 5.10), адреса остальных
// сервисов — METRICS_ADDR; Prometheus скрейпит каждый сервис отдельно,
// агрегации через monitor нет. Реестр уже несёт метрики каркаса
// (crawler_service_up, crawler_build_info) и коллекторы Go/runtime и
// процесса: uptime выводится из process_start_time_seconds в PromQL,
// отдельная метрика не нужна. Рабочая логика добавляет приборы
// фабричными методами Counter/Gauge/Histogram (T-076) — метка service
// подставляется автоматически, единые имена меток зафиксированы
// константами Label*. Приборы этапов (T-077, ТЗ 5.9) регистрируют
// сами сервисы через NewMetrics-конструкторы своих пакетов воркеров;
// коллектор длин очередей — collect.go этого пакета.
package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Единые имена меток метрик проекта (ТЗ 5.9): приборы этапов — T-077,
// имена зафиксированы здесь, чтобы метрики разных сервисов имели
// одинаковые метки.
const (
	// LabelService — имя сервиса. Подставляется фабрикой автоматически,
	// вручную не передаётся.
	LabelService = "service"
	// LabelStage — этап конвейера (dns, http, render, analyze) для
	// пропускной способности по этапам и времени ответа.
	LabelStage = "stage"
	// LabelQueue — имя очереди (q:dns, q:http, q:render, q:analyze,
	// q:dead — internal/queue) для длин очередей и глубины backlog.
	LabelQueue = "queue"
	// LabelType — тип ошибки (timeout, tls, network, body, …) для
	// ошибок по типам; он же — имя статуса домена в счётчике финальных
	// статусов (not_delegated, temp_error, no_http, …).
	LabelType = "type"
	// LabelState — состояние проверки домена (pending, queued) для
	// глубины backlog из БД: имя метки зафиксировано документацией
	// (docs/services.md, docs/operations.md, ТЗ 5.9) — отдельное от
	// LabelType: «type» уже занят типом ошибки/статусом домена.
	LabelState = "state"
)

// buildMode — значение метки mode метрики crawler_build_info. Боевые
// сборки инъекцируют его линковщиком:
//
//	go build -ldflags "-X crawler/internal/metrics.buildMode=prod"
//
// (deploy/docker/Dockerfile.go, Dockerfile.render, docs/deploy.md §4.2).
// Без инъекции — dev-сборка (go build / air / go test): каркас
// разработки, значение по умолчанию сохранено, чтобы не ломать dev.
var buildMode = "dev-stub"

// Значения метки LabelStage — этапы конвейера (ТЗ 4.2, §7). Единые
// значения фиксируются здесь, чтобы метрики всех сервисов агрегировались
// в PromQL без нормализации.
const (
	// StageDNS — DNS-этап (dns-worker).
	StageDNS = "dns"
	// StageHTTP — HTTP-этап (http-worker).
	StageHTTP = "http"
	// StageRender — этап рендера (render-worker).
	StageRender = "render"
	// StageAnalyze — этап анализа (analyzer).
	StageAnalyze = "analyze"
)

// Registry — реестр метрик одного сервиса: собственный
// *prometheus.Registry (встроен — методы Register/Gather доступны для
// нестандартных коллекторов) и имя сервиса для автометки service.
// Реестры сервисов независимы: одноимённые метрики разных сервисов не
// конфликтуют — у каждого свой /metrics (ТЗ 5.9).
type Registry struct {
	*prometheus.Registry
	service string
}

// New создаёт реестр Prometheus для сервиса name, регистрирует
// коллекторы Go/runtime и процесса и базовые метрики каркаса. Для
// метрик рабочей логики реестр используется как фабрика:
// Counter/Gauge/Histogram.
func New(name string) *Registry {
	reg := &Registry{
		Registry: prometheus.NewRegistry(),
		service:  name,
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	up := reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_service_up",
		Help: "Сервис запущен и отвечает.",
	})
	up.WithLabelValues().Set(1)

	build := reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_build_info",
		Help: "Информация о сборке: mode — режим сборки (dev-stub — dev/тесты, prod — боевые сборки через -ldflags -X).",
	}, "mode")
	build.WithLabelValues(buildMode).Set(1)

	return reg
}

// Counter создаёт и регистрирует счётчик с метками [LabelService] +
// labelNames и возвращает вектор, каррированный по service: вызывающий
// передаёт значения только остальных метк —
//
//	tasks := reg.MustCounter(prometheus.CounterOpts{
//		Name: "crawler_stage_tasks_total",
//		Help: "Обработанные домены по этапам конвейера.",
//	}, LabelStage)
//	tasks.WithLabelValues("dns").Inc() // service подставится сама
//
// HELP/TYPE экспортируются Prometheus из Name/Help: документация метрики
// пишется там же, где создаётся прибор.
//
// Зарезервированная метка LabelService и дубликаты меток отклоняются
// ошибкой (а не паникой prometheus.New*Vec). Повторная регистрация
// идентичной метрики (то же имя, help и метки) — не ошибка:
// возвращается уже зарегистрированный вектор, поэтому конструкторы
// приборов можно вызывать многократно (типовая ловушка
// AlreadyRegisteredError в тестах не паникует). Конфликт описаний —
// ошибка.
func (r *Registry) Counter(opts prometheus.CounterOpts, labelNames ...string) (*prometheus.CounterVec, error) {
	name := prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)
	labels, err := withServiceLabel(labelNames)
	if err != nil {
		return nil, err
	}
	c, err := r.register(name, prometheus.NewCounterVec(opts, labels))
	if err != nil {
		return nil, err
	}
	vec, ok := c.(*prometheus.CounterVec)
	if !ok {
		return nil, fmt.Errorf("metrics: метрика %q уже зарегистрирована коллектором другого типа", name)
	}
	return vec.CurryWith(r.serviceLabels())
}

// Gauge — аналог Counter для gauge-метрик (длины очередей, глубина
// backlog, память воркеров): метки [LabelService] + labelNames, метка
// service подставляется автоматически, дубль идентичной метрики — не
// ошибка.
func (r *Registry) Gauge(opts prometheus.GaugeOpts, labelNames ...string) (*prometheus.GaugeVec, error) {
	name := prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)
	labels, err := withServiceLabel(labelNames)
	if err != nil {
		return nil, err
	}
	c, err := r.register(name, prometheus.NewGaugeVec(opts, labels))
	if err != nil {
		return nil, err
	}
	vec, ok := c.(*prometheus.GaugeVec)
	if !ok {
		return nil, fmt.Errorf("metrics: метрика %q уже зарегистрирована коллектором другого типа", name)
	}
	return vec.CurryWith(r.serviceLabels())
}

// Histogram — аналог Counter для гистограмм (время ответа): метки
// [LabelService] + labelNames, метка service подставляется
// автоматически, дубль идентичной метрики — не ошибка. Возвращается
// интерфейс ObserverVec (CurryWith HistogramVec): наблюдения
// добавляются WithLabelValues(...).Observe(...), специфика
// HistogramVec вызывающему не нужна. Корзины (Buckets) не входят в
// дескриптор prometheus: повторная регистрация с другими корзинами
// возвращает существующий вектор — корзины первого вызова выигрывают.
func (r *Registry) Histogram(opts prometheus.HistogramOpts, labelNames ...string) (prometheus.ObserverVec, error) {
	name := prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)
	labels, err := withServiceLabel(labelNames)
	if err != nil {
		return nil, err
	}
	c, err := r.register(name, prometheus.NewHistogramVec(opts, labels))
	if err != nil {
		return nil, err
	}
	vec, ok := c.(*prometheus.HistogramVec)
	if !ok {
		return nil, fmt.Errorf("metrics: метрика %q уже зарегистрирована коллектором другого типа", name)
	}
	return vec.CurryWith(r.serviceLabels())
}

// MustCounter — Counter с паникой вместо ошибки. Паника — только на
// реальном конфликте описаний (дубль идентичной метрики — не конфликт);
// уместен для приборов, создаваемых при старте сервиса, где ошибка —
// баг программы.
func (r *Registry) MustCounter(opts prometheus.CounterOpts, labelNames ...string) *prometheus.CounterVec {
	vec, err := r.Counter(opts, labelNames...)
	if err != nil {
		panic(err)
	}
	return vec
}

// MustGauge — Gauge с паникой вместо ошибки (см. MustCounter).
func (r *Registry) MustGauge(opts prometheus.GaugeOpts, labelNames ...string) *prometheus.GaugeVec {
	vec, err := r.Gauge(opts, labelNames...)
	if err != nil {
		panic(err)
	}
	return vec
}

// MustHistogram — Histogram с паникой вместо ошибки (см. MustCounter).
func (r *Registry) MustHistogram(opts prometheus.HistogramOpts, labelNames ...string) prometheus.ObserverVec {
	vec, err := r.Histogram(opts, labelNames...)
	if err != nil {
		panic(err)
	}
	return vec
}

// Handler возвращает HTTP-хендлер, отдающий метрики реестра в формате
// Prometheus (эндпоинт /metrics каждого сервиса, ТЗ 5.9). g — любой
// Gatherer: *Registry фабрики или *prometheus.Registry напрямую.
func Handler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}

// register регистрирует collector в реестре. Идентичная уже
// зарегистрированная метрика (AlreadyRegisteredError с тем же
// дескриптором — то же имя, help и метки) — не ошибка: возвращается
// существующий коллектор, дубль-регистрация идемпотентна. Другие
// коллизии отклоняет сам prometheus: то же имя с другим help или
// набором меток — ошибка размерности.
func (r *Registry) register(name string, c prometheus.Collector) (prometheus.Collector, error) {
	err := r.Register(c)
	if err == nil {
		return c, nil
	}
	var are prometheus.AlreadyRegisteredError
	if !errors.As(err, &are) {
		return nil, err
	}
	if !descEqual(are.ExistingCollector, c) {
		return nil, fmt.Errorf("metrics: метрика %q уже зарегистрирована с другим описанием: %w", name, err)
	}
	return are.ExistingCollector, nil
}

// withServiceLabel — имена меток фабричных метрик: LabelService всегда
// первая, значения остальных передаёт вызывающий. LabelService и
// дубликаты меток отклоняются ошибкой — prometheus.New*Vec паникует на
// повторяющихся именах меток.
func withServiceLabel(labelNames []string) ([]string, error) {
	labels := make([]string, 0, len(labelNames)+1)
	labels = append(labels, LabelService)
	for _, l := range labelNames {
		if slices.Contains(labels, l) {
			return nil, fmt.Errorf("metrics: метка %q уже занята (LabelService добавляется автоматически)", l)
		}
		labels = append(labels, l)
	}
	return labels, nil
}

// serviceLabels — каррирующий набор метки service.
func (r *Registry) serviceLabels() prometheus.Labels {
	return prometheus.Labels{LabelService: r.service}
}

// descEqual — совпадают ли дескрипторы двух коллекторов (имя, help,
// метки). Гистограммные корзины (Buckets) в дескриптор не входят —
// сравнить их у зарегистрированного вектора нельзя, повторная
// регистрация гистограммы с другими корзинами отдаёт существующий
// вектор (см. Histogram).
func descEqual(a, b prometheus.Collector) bool {
	da, oka := descOf(a)
	db, okb := descOf(b)
	return oka && okb && da.String() == db.String()
}

// descOf — первый дескриптор коллектора. Describe выполняется в
// отдельной горутине и его остаток дренируется: нестандартный
// коллектор может прислать больше одного дескриптора или ни одного
// (тогда ok=false) — ни то, ни другое не должно зависывать вызов.
// Векторы Counter/Gauge/Histogram отдают ровно один дескриптор.
func descOf(c prometheus.Collector) (d *prometheus.Desc, ok bool) {
	ch := make(chan *prometheus.Desc)
	go func() {
		defer close(ch)
		c.Describe(ch)
	}()
	d, ok = <-ch
	if !ok {
		return nil, false
	}
	for range ch { // дренировать остаток дескрипторов
	}
	return d, true
}

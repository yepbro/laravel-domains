// Приборы HTTP-этапа (T-077, ТЗ 5.9): пропускная способность (счётчик
// задач и гистограмма длительностей), ошибки проверок по типам
// (ErrorClass → LabelType: timeout/redirects/tls/body/network, §8),
// финальные статусы доменов (alive/no_http/temp_error, §7), время
// ответа (гистограмма ResponseMS итоговой страницы, ТЗ 5.5 п. 7) и доля
// рендера — счётчик записей rendered=false (пишет HTTP-этап; ветку
// rendered=true наращивает render-worker тем же счётчиком, доля —
// отношением rate в PromQL). Счётчики статусов и записей считаются по
// завершённому эпизоду (после успешного OnResult — записи зафиксированы
// транзакцией T-058): гонка с удалением домена (§13.1) не различается,
// at-least-once повторы учитываются как работа (ТЗ 3.4).
package httpworker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// Корзины гистограмм этапа. Время ответа — до заголовков итоговой
// страницы (мс ResponseMS, ТЗ 5.5 п. 7): локальные сайты десятки
// миллисекунд, таймауты этапа (connect 5 с, общий ~10 с, ТЗ 5.10)
// обрезают хвост корзинами 5/10 с. Длительность задачи домена — до всех
// проверок субдоменов параллельным пулом с per-IP-лимитами (T-049):
// от долей секунды до десятков секунд на «молчащие» домены с таймаутами.
var (
	httpResponseBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	httpSecondsBuckets  = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
)

// Metrics — приборы HTTP-этапа; строится NewMetrics из реестра сервиса,
// nil — все методы no-op (изолированные сборки). Потокобезопасен:
// задачи идут параллельно, приборы prometheus синхронизированы.
type Metrics struct {
	tasks    prometheus.Counter     // crawler_stage_tasks_total{stage="http"}
	seconds  prometheus.Observer    // crawler_stage_seconds{stage="http"}
	errors   *prometheus.CounterVec // crawler_stage_errors_total{stage,type}
	statuses *prometheus.CounterVec // crawler_domain_status_total{type}
	response prometheus.Observer    // crawler_http_response_seconds
	rendered *prometheus.CounterVec // crawler_render_records_total{rendered}
}

// NewMetrics регистрирует приборы HTTP-этапа в реестре reg (каррированные
// по service, фиксированные метки значений — по месту); nil — nil.
// Идемпотентен (T-076): повторный вызов делит те же коллекторы.
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные задачи этапов конвейера: dns — домены, http — домены, render/analyze — записи субдомен×вариант.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageHTTP),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Длительность обработки одной задачи этапа (эпизод домена/записи).",
			Buckets: httpSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageHTTP),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_errors_total",
			Help: "Ошибки этапов по типам: http — классы проверок (timeout, redirects, tls, body, network), render — классы рендера (task, context, browser).",
		}, metrics.LabelStage, metrics.LabelType),
		statuses: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_domain_status_total",
			Help: "Финальные статусы доменов по итогам эпизодов (ТЗ 6.2): DNS-этап — not_delegated/delegated_no_address/temp_error, HTTP-этап — alive/no_http/temp_error.",
		}, metrics.LabelType),
		response: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_http_response_seconds",
			Help:    "Время ответа итоговой страницы HTTP-проверки (до заголовков, без кэш-попаданий).",
			Buckets: httpResponseBuckets,
		}).WithLabelValues(),
		rendered: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_records_total",
			Help: "Записи результатов по признаку rendered: false пишет HTTP-этап, true — render-worker; доля рендера — отношение rate.",
		}, "rendered"),
	}
}

// Episode — задача q:http завершена успешно: +1 задача этапа и её
// длительность; финальный статус домена (recordStatus, §7); ошибки
// проверок по классам (§8), время ответа доступных некэшированных
// проверок; записи результата — в счётчик rendered=false (доля рендера,
// ТЗ 5.9). Кэш-попадания (Cached) времени ответа не дают — сеть не
// трогалась (ResponseMS = 0); суб-миллисекундные ответы честно ложатся
// в нижнюю корзину.
func (m *Metrics) Episode(res *DomainResult, d time.Duration) {
	if m == nil || res == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
	m.statuses.WithLabelValues(recordStatus(res).String()).Inc()
	m.rendered.WithLabelValues("false").Add(float64(res.RecordCount()))
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		for _, check := range []SchemeCheck{sub.HTTPS, sub.HTTP} {
			if check.ErrClass != ErrNone {
				m.errors.WithLabelValues(metrics.StageHTTP, check.ErrClass.String()).Inc()
			}
			if check.Available && !check.Cached {
				m.response.Observe(float64(check.ResponseMS) / 1000)
			}
		}
	}
}

// Приборы этапа рендера (T-077, ТЗ 5.9): пропускная способность (счётчик
// задач и гистограмма длительностей), ошибки по классам render.Class
// (task/context/browser — реакция этапа на сбой, T-061/T-063: повтор
// страницы, контекста или перезапуск браузера), доля рендера — ветка
// rendered=true счётчика crawler_render_records_total (ветку false
// наращивает HTTP-этап после записи результата; доля — отношением rate
// в PromQL), тарпит-финализации без рендера (T-148), финализации по
// постоянной ошибке сертификата хоста (T-159) и финализации ошибки
// класса задачи на последней попытке с меткой причины (T-172).
package renderworker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/render"
)

// renderSecondsBuckets — корзины длительности задачи рендера: страница
// рендерится в изолированном контексте с ожиданием готовности до
// render.timeout (по умолчанию 25 с, ТЗ 5.6/5.10), быстрые статические
// страницы — единицы секунд.
var renderSecondsBuckets = []float64{0.5, 1, 2.5, 5, 10, 15, 20, 25, 30}

// Metrics — приборы этапа рендера; строится NewMetrics из реестра
// сервиса, nil — все методы no-op (изолированные сборки).
// Потокобезопасен: доставки идут параллельно, приборы prometheus
// синхронизированы.
type Metrics struct {
	tasks    prometheus.Counter     // crawler_stage_tasks_total{stage="render"}
	seconds  prometheus.Observer    // crawler_stage_seconds{stage="render"}
	errors   *prometheus.CounterVec // crawler_stage_errors_total{stage,type}
	rendered prometheus.Counter     // crawler_render_records_total{rendered="true"}
	tarpits  prometheus.Counter     // crawler_render_tarpit_finalized_total (T-148)
	certs    prometheus.Counter     // crawler_render_cert_finalized_total (T-159)
	tasksFin *prometheus.CounterVec // crawler_render_task_finalized_total{reason} (T-172)
}

// NewMetrics регистрирует приборы этапа в реестре reg (каррированные по
// service и stage); nil — nil. Идемпотентен (T-076).
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные задачи этапов конвейера: dns — домены, http — домены, render/analyze — записи субдомен×вариант.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageRender),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Длительность обработки одной задачи этапа (эпизод домена/записи).",
			Buckets: renderSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageRender),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_errors_total",
			Help: "Ошибки этапов по типам: http — классы проверок (timeout, redirects, tls, body, network), render — классы рендера (task, context, browser).",
		}, metrics.LabelStage, metrics.LabelType),
		rendered: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_records_total",
			Help: "Записи результатов по признаку rendered: false пишет HTTP-этап, true — render-worker; доля рендера — отношение rate.",
		}, "rendered").WithLabelValues("true"),
		tarpits: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_tarpit_finalized_total",
			Help: "Записи, финализированные без рендера после исчерпания render.timeout_attempts (T-148): итог — сырой HTML HTTP-фазы, запись уходит в q:analyze; рост — доля таймаут-тарпитов на рендере.",
		}).WithLabelValues(),
		certs: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_cert_finalized_total",
			Help: "Записи, финализированные без рендера по постоянной ошибке TLS-сертификата хоста — net::ERR_CERT_AUTHORITY_INVALID / net::ERR_CERT_COMMON_NAME_INVALID (T-159): итог — сырой HTML HTTP-фазы, запись уходит в q:analyze, в q:dead cert-класс не попадает.",
		}).WithLabelValues(),
		tasksFin: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_task_finalized_total",
			Help: "Записи, финализированные без рендера по ошибке класса задачи рендера на последней доставке (T-172): итог — сырой HTML HTTP-фазы, запись уходит в q:analyze, в q:dead не попадает. Метка reason — код net::ERR_* (например net::ERR_CONNECTION_REFUSED), timeout или other: состав того, что раньше уносило записи в q:dead.",
		}, "reason"),
	}
}

// Task — задача q:render обработана успешно (включая штатный отказ —
// строки нет или ушла из-под задачи): +1 задача этапа, длительность
// в гистограмму.
func (m *Metrics) Task(d time.Duration) {
	if m == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
}

// Rendered — DOM записан в строку (rendered = true, ТЗ 5.6): +1 в ветку
// rendered=true счётчика доли рендера.
func (m *Metrics) Rendered() {
	if m == nil {
		return
	}
	m.rendered.Inc()
}

// TarpitFinalized — запись финализирована без рендера по исчерпании
// лимита таймаутов (T-148): +1 в счётчик тарпит-финализаций.
func (m *Metrics) TarpitFinalized() {
	if m == nil {
		return
	}
	m.tarpits.Inc()
}

// CertFinalized — запись финализирована без рендера по постоянной ошибке
// сертификата хоста (T-159): +1 в счётчик cert-финализаций — материал
// критерия «суточный прирост q:dead от cert-класса = 0» (рост счётчика
// при нулевом приросте q:dead — политика работает).
func (m *Metrics) CertFinalized() {
	if m == nil {
		return
	}
	m.certs.Inc()
}

// TaskFinalized — запись финализирована без рендера по ошибке класса
// задачи на последней доставке (T-172): +1 в счётчик финализаций с
// меткой причины (код net::ERR_* / timeout / other) — материал критерия
// «суточный прирост q:dead от q:render = 0» и видимости состава причин
// (что именно доезжало до q:dead прежде).
func (m *Metrics) TaskFinalized(reason string) {
	if m == nil {
		return
	}
	m.tasksFin.WithLabelValues(reason).Inc()
}

// Error — задача рендера не подтверждена (вернётся повторной доставкой):
// +1 в счётчик ошибок этапа по классу render.ClassOf.
func (m *Metrics) Error(class render.Class) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(metrics.StageRender, class.String()).Inc()
}

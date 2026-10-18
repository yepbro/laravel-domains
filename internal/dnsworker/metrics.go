// Приборы DNS-этапа (T-077, ТЗ 5.9): пропускная способность (счётчик
// завершённых эпизодов и гистограмма их длительностей) и финальные
// статусы доменов терминальных веток DNS (not_delegated /
// delegated_no_address / temp_error / invalid, §6.2/§7). Статусы ветки
// «есть адреса» не пишутся (итог за HTTP-этапом) — их считает http-worker
// тем же счётчиком crawler_domain_status_total. invalid — диагностика
// ядовитой строки (T-109), не факт о домене. Ошибки обменов отдельным
// счётчиком не типизируются: DNS-этап не классифицирует сбои резолвера
// (транспорт после ретраев и любые коды ответа сводятся к temp_error,
// T-036) — счётчик статусов и есть честная типизация итога; типизация
// сбоя эпизода — территория очереди (повторные доставки, q:dead).
package dnsworker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// dnsSecondsBuckets — корзины длительности DNS-эпизода: обмены UDP
// миллисекунды, но эпизод — последовательность обменов (апекс,
// субдомены, wildcard) под лимитами параллелизма/QPS (T-035), а сбойный
// резолвер тратит таймауты и ретраи — до десятков секунд.
var dnsSecondsBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}

// Metrics — приборы DNS-этапа; строется NewMetrics из реестра сервиса,
// nil (реестра нет — изолированные сборки) — все методы no-op.
// Потокобезопасен: эпизоды идут параллельно, приборы prometheus
// синхронизированы.
type Metrics struct {
	tasks    prometheus.Counter     // crawler_stage_tasks_total{stage="dns"}
	seconds  prometheus.Observer    // crawler_stage_seconds{stage="dns"}
	statuses *prometheus.CounterVec // crawler_domain_status_total{type}
}

// NewMetrics регистрирует приборы DNS-этапа в реестре reg (каррированные
// по service и stage); nil — nil. Идемпотентен (T-076): повторный вызов
// делит те же коллекторы.
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные задачи этапов конвейера: dns — домены, http — домены, render/analyze — записи субдомен×вариант.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageDNS),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Длительность обработки одной задачи этапа (эпизод домена/записи).",
			Buckets: dnsSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageDNS),
		statuses: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_domain_status_total",
			Help: "Финальные статусы доменов по итогам эпизодов (ТЗ 6.2): DNS-этап — not_delegated/delegated_no_address/temp_error/invalid (диагноз ядовитой строки, T-109), HTTP-этап — alive/no_http/temp_error.",
		}, metrics.LabelType),
	}
}

// Episode — эпизод домена завершён успешно: +1 задача этапа, длительность
// в гистограмму. Вызывается по завершении ProcessDomain (включая
// повторную доставку уже записанного — at-least-once считает повторную
// работу честной работой).
func (m *Metrics) Episode(d time.Duration) {
	if m == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
}

// DomainStatus — домен получил терминальный статус DNS-этапа
// (not_delegated/delegated_no_address/temp_error, §6.2; invalid —
// диагноз ядовитой строки, T-109): +1 в счётчик статусов. Статус живого
// домена (есть адреса) не пишется — итог за HTTP-этапом (T-039, §7).
func (m *Metrics) DomainStatus(status model.Status) {
	if m == nil {
		return
	}
	m.statuses.WithLabelValues(status.String()).Inc()
}

// Коллектор состояния лимитёра DNS (T-077, ТЗ 5.9): периодический съём
// LimitResolver.Stats в gauges — адаптивное снижение параллелизма/QPS по
// доле таймаутов (T-035, ТЗ 5.4) обязано быть наблюдаемым. Без экспорта
// конвейер мог бы молча работать на 1 % плановой мощности (резолвер
// таймаутит, лимиты на «дне»), а Prometheus показывал бы только рост
// глубины q:dns и доли temp_error — без причины.
//
// Архитектурно — по образцу коллектора глубины очередей
// (metrics.CollectQueueLoop): цикл живёт в сервисе-носителе лимитёра
// (dns-worker), источник состояния внедряется функцией LimitStatsFunc
// (метод Stats декоратора) — пакет не создаёт обратных зависимостей
// dnsclient → metrics. Данные и приборы разделены как в dnsclient:
// LimitStats — снимок без метрик, сюда — только экспорт.
package dnsworker

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
)

// DefaultLimitCollectInterval — период съёма состояния лимитёра: как у
// коллектора очередей, равен типовому интервалу скрейпа Prometheus
// (dev-конфиг deploy/docker/prometheus) — каждый скрейп видит свежее
// значение, чаще незачем: адаптив меняет лимиты на масштабе окна обменов,
// а не миллисекунд.
const DefaultLimitCollectInterval = 15 * time.Second

// Значения метки kind: configured — настроенные потолки (dns.concurrency,
// dns.max_qps, ТЗ 5.10), effective — действующие после адаптива. Пара рядов
// в одной метрике даёт PromQL-готовую долю деградации:
// crawler_dns_limit_qps{kind="effective"} / {kind="configured"}.
const (
	limitKindEffective  = "effective"
	limitKindConfigured = "configured"
)

// LimitStatsFunc — источник состояния лимитёра: удовлетворяется методом
// Stats декоратора *dnsclient.LimitResolver (значение метода); юнит-тесты
// подменяют замыканием над заготовленным снимком.
type LimitStatsFunc func() dnsclient.LimitStats

// limitGauges — приборы коллектора (создаются один раз, значения
// перезаписываются каждым проходом).
type limitGauges struct {
	concurrency *prometheus.GaugeVec // crawler_dns_limit_concurrency{kind}
	qps         *prometheus.GaugeVec // crawler_dns_limit_qps{kind}
	ratio       prometheus.Gauge     // crawler_dns_limit_timeout_ratio
	inflight    prometheus.Gauge     // crawler_dns_inflight
}

// newLimitGauges регистрирует приборы в реестре reg; nil — nil (сервис
// без реестра — изолированные сборки — коллектор не поднимается).
// Идемпотентен (T-076): повторный вызов делит те же векторы.
func newLimitGauges(reg *metrics.Registry) *limitGauges {
	if reg == nil {
		return nil
	}
	return &limitGauges{
		concurrency: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_dns_limit_concurrency",
			Help: "Потолок параллелизма DNS-обменов: effective — действующий после адаптивного снижения (T-035), configured — dns.concurrency.",
		}, metrics.LabelKind),
		qps: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_dns_limit_qps",
			Help: "Средний темп DNS-обменов в секунду: effective — действующий после адаптивного снижения (T-035), configured — dns.max_qps.",
		}, metrics.LabelKind),
		ratio: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_dns_limit_timeout_ratio",
			Help: "Доля таймаутов в скользящем окне адаптива лимитёра DNS (0, пока окно наполняется).",
		}).WithLabelValues(),
		inflight: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_dns_inflight",
			Help: "DNS-обменов в нижнем резолвере прямо сейчас (занятые слоты параллелизма).",
		}).WithLabelValues(),
	}
}

// collectLimitPass — один проход: снимок источника → значения gauges.
func collectLimitPass(g *limitGauges, stats LimitStatsFunc) {
	st := stats()
	g.concurrency.WithLabelValues(limitKindEffective).Set(float64(st.Concurrency))
	g.concurrency.WithLabelValues(limitKindConfigured).Set(float64(st.ConcurrencyCfg))
	g.qps.WithLabelValues(limitKindEffective).Set(st.QPS)
	g.qps.WithLabelValues(limitKindConfigured).Set(st.QPSCfg)
	ratio := 0.0
	if st.WindowSize > 0 { // окно наполняется — доля по набранному
		ratio = float64(st.WindowTimeouts) / float64(st.WindowSize)
	}
	g.ratio.Set(ratio)
	g.inflight.Set(float64(st.InFlight))
}

// CollectLimitLoop — периодический цикл съёма состояния лимитёра DNS:
// первый проход сразу при старте, далее по ticker с периодом interval
// (0 — DefaultLimitCollectInterval). Отмена ctx — чистый выход (graceful
// shutdown, ТЗ 3.4); nil-зависимости — no-op: сервис без реестра не
// должен падать на старте. Работает до отмены ctx и возвращает nil —
// тот же контракт, что у metrics.CollectQueueLoop (фоновый коллектор
// сервиса, T-012).
func CollectLimitLoop(
	ctx context.Context,
	reg *metrics.Registry,
	stats LimitStatsFunc,
	interval time.Duration,
	log *slog.Logger,
) error {
	if stats == nil {
		return nil // раньше регистрации: приборы не плодятся без источника
	}
	g := newLimitGauges(reg)
	if g == nil {
		return nil
	}
	if interval <= 0 {
		interval = DefaultLimitCollectInterval
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("коллектор лимитёра DNS", "interval", interval.String())

	pass := func() { collectLimitPass(g, stats) }
	pass()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

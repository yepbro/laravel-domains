// Коллектор переключений DNS на резервные резолверы (T-107, ТЗ 5.9):
// периодический съём FallbackResolver.Stats в счётчики. Переключение —
// штатная деградация (ТЗ 5.4: «публичные резолверы — только как fallback»),
// но обязанная быть наблюдаемой: без метрики отказ локального unbound
// виден только косвенно — ростом латентности эпизодов (каждый обмен
// оплачивает цикл ретраев отказавшего) и метриками лимитёра, которые
// при живом резерве остаются «здоровыми» (адаптив T-035 видит итог-успех,
// и это правильно: конвейер работает).
//
// Архитектурно — тот же образец, что и коллектор лимитёра
// (limitmetrics.go): цикл живёт в сервисе-носителе цепи (dns-worker),
// источник состояния внедряется функцией FallbackStatsFunc (метод Stats
// декоратора) — пакет не создаёт обратных зависимостей dnsclient →
// metrics. Счётчики, а не gauges: значения накапливаются в источнике,
// коллектор добавляет дельту между проходами (Prometheus-семантика
// _total и rate() сохраняется).
package dnsworker

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
)

// FallbackStatsFunc — источник состояния переключений: удовлетворяется
// методом Stats декоратора *dnsclient.FallbackResolver (значение метода);
// юнит-тесты подменяют замыканием над заготовленным снимком.
type FallbackStatsFunc func() dnsclient.FallbackStats

// fallbackCounters — приборы коллектора (создаются один раз, значения
// монотонно растут дельтами проходов).
type fallbackCounters struct {
	switches  prometheus.Counter // crawler_dns_fallback_switches_total
	exhausted prometheus.Counter // crawler_dns_fallback_exhausted_total
}

// newFallbackCounters регистрирует приборы в реестре reg; nil — nil
// (сервис без реестра — коллектор не поднимается). Идемпотентен (T-076):
// повторный вызов делит те же счётчики.
func newFallbackCounters(reg *metrics.Registry) *fallbackCounters {
	if reg == nil {
		return nil
	}
	return &fallbackCounters{
		switches: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_dns_fallback_switches_total",
			Help: "DNS-обменов, отвеченных резервным резолвером: основной не ответил транспортно, сработало переключение dns.fallback_resolvers (T-107).",
		}).WithLabelValues(),
		exhausted: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_dns_fallback_exhausted_total",
			Help: "DNS-обменов, провалившихся на всей цепи резолверов (основной + резервные): каждый — будущий temp_error (T-036/T-039).",
		}).WithLabelValues(),
	}
}

// collectFallbackPass — один проход: дельта снимка → счётчики. Отрицательная
// дельта (счётчик источника меньше прежнего — цепь пересоздали в тесте
// или рестартом в процессе) добавляется целиком: счётчик Prometheus
// монотонен, «отмотать» его нельзя.
func collectFallbackPass(c *fallbackCounters, stats FallbackStatsFunc, prev *dnsclient.FallbackStats) {
	st := stats()
	if d := st.Switches - prev.Switches; d > 0 {
		c.switches.Add(float64(d))
	} else if d < 0 {
		c.switches.Add(float64(st.Switches))
	}
	if d := st.Exhausted - prev.Exhausted; d > 0 {
		c.exhausted.Add(float64(d))
	} else if d < 0 {
		c.exhausted.Add(float64(st.Exhausted))
	}
	*prev = st
}

// CollectFallbackLoop — периодический цикл съёма состояния переключений:
// первый проход сразу при старте, далее по ticker с периодом interval
// (0 — DefaultLimitCollectInterval, тот же интервал скрейпа, что у
// коллектора лимитёра). Отмена ctx — чистый выход (graceful shutdown,
// ТЗ 3.4); nil-зависимости — no-op. Работает до отмены ctx и возвращает
// nil — контракт фоновых коллекторов сервиса (T-012).
func CollectFallbackLoop(
	ctx context.Context,
	reg *metrics.Registry,
	stats FallbackStatsFunc,
	interval time.Duration,
	log *slog.Logger,
) error {
	if stats == nil {
		return nil // раньше регистрации: приборы не плодятся без источника
	}
	c := newFallbackCounters(reg)
	if c == nil {
		return nil
	}
	if interval <= 0 {
		interval = DefaultLimitCollectInterval
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("коллектор переключений DNS на резерв", "interval", interval.String())

	var prev dnsclient.FallbackStats
	pass := func() { collectFallbackPass(c, stats, &prev) }
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

// Метрика фоновой очистки удалённых доменов (T-099, ТЗ 5.9): счётчик
// crawler_ingest_bad_hashes_total — удалённые проходом результаты с
// content_hash неверной длины (CleanupResult.BadHashes). В норме инвариант
// писателя результатов держит 16 октет (ТЗ 6.1, T-056) и значение не растёт
// вовсе: любой рост — программная ошибка писателя, а не шум нагрузки;
// всплеск отлавливает алерт мониторинга (internal/monitor, T-099) по
// экспозиции этого счётчика.
//
// Архитектурно — не полл, а push: результаты проходов очистки —
// возвращаемые значения CleanupOnce, долговременного состояния со снимками
// (как LimitStats/FallbackStats у DNS-коллекторов) нет — поллить нечего.
// Цикл CleanupDeleted отдаёт итог каждому проходу хуком CleanupOptions.OnPass,
// коллектор лишь переводит его в счётчик — тот же шов, что AlertsOptions.OnPass
// в мониторинге («лог + поле»), пакет cleanup.go зависимостей на метрики не
// получает.
package ingest

import (
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// CleanupMetrics — приборы очистки удалённых доменов: счётчик мусорных
// content_hash (создаётся один раз, значения монотонно растут итогами
// проходов).
type CleanupMetrics struct {
	badHashes prometheus.Counter // crawler_ingest_bad_hashes_total
}

// NewCleanupMetrics регистрирует приборы очистки в реестре reg; nil — nil
// (сервис без реестра — изолированные сборки — метрик не поднимает).
// Идемпотентен (T-076): повторный вызов делит тот же счётчик.
func NewCleanupMetrics(reg *metrics.Registry) *CleanupMetrics {
	if reg == nil {
		return nil
	}
	return &CleanupMetrics{
		badHashes: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_ingest_bad_hashes_total",
			Help: "Удалённые очисткой deleted-доменов результаты с content_hash неверной длины (16 октет, ТЗ 6.1): refcount по ним не менялся, рост — ошибка писателя результатов (T-056); алерт ingest_bad_hashes мониторит всплески.",
		}).WithLabelValues(),
	}
}

// Observe — итог одного прохода очистки (подключается хуком
// CleanupOptions.OnPass): добавляет BadHashes прохода в счётчик.
// Нулевой итог значение не меняет; nil-приёмник — no-op (сервис без
// реестра), чтобы cmd/ingest подключал хук безусловно.
func (m *CleanupMetrics) Observe(res CleanupResult) {
	if m == nil || res.BadHashes <= 0 {
		return
	}
	m.badHashes.Add(float64(res.BadHashes))
}

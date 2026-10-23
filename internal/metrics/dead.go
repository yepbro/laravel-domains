// Счётчик переводов задач в q:dead (T-154, ТЗ 4.2/5.9): bury слоя
// надёжности очереди (queue.Reliab) до T-154 был молчалив — 5507 смертей
// инцидента 22.10 (docs/load-measurements.md §11.1) прошли без единого
// алерта. Счётчик монотонный: смерть — свершившийся факт, «разобранным»
// бывает только q:dead целиком (XTRIM меняет глубину, не историю
// смертей). Ряд per-stream: метка queue — стрим-источник умершей задачи
// (q:dns/q:http/q:render/q:analyze), как у crawler_queue_depth.
//
// Архитектурно — хук queue.ReliabOptions.OnBury, монтируемый сервисами
// (cmd-воркеры: RecoverLoop живёт в них): пакет очереди не зависит от
// Prometheus — тот же шов-функция, что LenFunc коллектора глубин
// (collect.go), прибор живёт в реестре сервиса, где работает
// восстановитель, и смерть задачи видна в /metrics того воркера, чей
// RecoverLoop её похоронил. Глубину q:dead (неразобранный остаток) и
// алерт queue_dead держит monitor (T-154): счётчик — темп смертей
// (rate), глубина — сток.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// NewDeadBuriedHook строит хук для queue.ReliabOptions.OnBury: инкремент
// counter'а crawler_queue_dead_buried_total{queue} в реестре reg.
// Возвращается готовая к передаче в опции функция; nil reg — nil
// (сервис без реестра — изолированные сборки: Reliab проверяет хук
// на nil и работает без него). Идемпотентен (T-076): повторная
// регистрация идентичного прибора делит уже созданный вектор.
func NewDeadBuriedHook(reg *Registry) func(stream string) {
	if reg == nil {
		return nil
	}
	buried := reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_queue_dead_buried_total",
		Help: "Переводы задач в q:dead (Reliab.bury, T-154): запись исчерпала лимит попыток доставок без подтверждения и ушла в карантин ручного разбора. Метка queue — стрим-источник задачи. Рост — смерти задач: темп(rate) — поток смертей, неразобранный остаток — crawler_queue_depth{queue=\"q:dead\"} монитора (алерт queue_dead); разбор — runbooks §2.",
	}, LabelQueue)
	return func(stream string) { buried.WithLabelValues(stream).Inc() }
}

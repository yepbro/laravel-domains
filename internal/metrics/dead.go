// Счётчики переводов в q:dead (T-154, T-260; ТЗ 4.2/5.9): bury слоя
// надёжности очереди (queue.Reliab) до T-154 был молчалив — 5507 смертей
// инцидента 22.10 (docs/load-measurements.md §11.1) прошли без единого
// алерта. Счётчики монотонные: смерть — свершившийся факт, «разобранным»
// бывает только q:dead целиком (XTRIM меняет глубину, не историю
// смертей). Ряды per-stream: метка queue — стрим-источник умершей задачи
// (q:dns/q:http/q:render/q:analyze), как у crawler_queue_depth.
//
// Два события — два счётчика: crawler_queue_dead_buried_total считает
// смерти задач с task_id; crawler_queue_dead_unidentified_total (T-260,
// ФТ-6.1) — карантины неидентифицируемых записей (без task_id, тело вне
// форматов пакета): до T-260 такие записи крутились вечным циклом клейма
// в PEL, теперь покидают его с синтетическим идентификатором
// unknown:<stream>:<entry-id> (разбор — runbooks §2).
//
// Архитектурно — хуки queue.ReliabOptions.OnBury/OnBuryUnidentified,
// монтируемые сервисами (cmd-воркеры: RecoverLoop живёт в них): пакет
// очереди не зависит от Prometheus — тот же шов-функция, что LenFunc
// коллектора глубин (collect.go), прибор живёт в реестре сервиса, где
// работает восстановитель, и событие видно в /metrics того воркера, чей
// RecoverLoop его произвёл. Глубину q:dead (неразобранный остаток) и
// алерт queue_dead держит monitor (T-154): счётчики — темп событий
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

// NewDeadUnidentifiedHook строит хук для
// queue.ReliabOptions.OnBuryUnidentified: инкремент counter'а
// crawler_queue_dead_unidentified_total{queue} в реестре reg (T-260,
// ФТ-6.1). nil reg — nil, идемпотентность T-076 — как у
// NewDeadBuriedHook.
func NewDeadUnidentifiedHook(reg *Registry) func(stream string) {
	if reg == nil {
		return nil
	}
	unidentified := reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_queue_dead_unidentified_total",
		Help: "Неидентифицируемые записи, переведённые в q:dead (Reliab.bury, T-260/ФТ-6.1): запись PEL без task_id (тело вне форматов пакета) исчерпала лимит попыток доставок; переведена с синтетическим task_id unknown:<stream>:<entry-id>, без предметных полей — разбор ручной (runbooks §2). Метка queue — стрим-источник записи. Стационарно ноль: рост — записи публикуются мимо контракта пакета (ручной XADD, баг писателя) — найти источник по entry_id.",
	}, LabelQueue)
	return func(stream string) { unidentified.WithLabelValues(stream).Inc() }
}

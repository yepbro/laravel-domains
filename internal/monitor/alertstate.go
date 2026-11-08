// Автомат алертов монитора (T-264, ТЗ tasks/TZ-fsm.md §3 ФТ-10): единая
// точка правды о состояниях и переходах алертов цикла AlertsLoop
// (monitor/alerts.go). До T-264 состояние одного алерта было размазано
// на 2–3 переменные (карта статусов + персональные поля памяти:
// deadStreak, psiHot, renderDebt, renderFbBudget, ...), консистентность
// держалась вручную каждой eval-функцией; теперь каждый алерт — значение
// единой структуры alertMachine «{имя, статус, track-память}», переход —
// единственный метод transition, наблюдение источника (observation) и
// решение о переходе (типизированная track-память) разделены.
// Формализация НЕ меняет поведение алертов (ФТ-10): пороги, окна, серии,
// защёлки и тексты деталей прежние; существующие тесты мигрированы в
// формат «последовательность наблюдений → последовательность статусов»
// без изменения ожиданий. Формализация не переписывает переходы на
// FSM-фреймворк (ТЗ-fsm §1): они остаются функциями над наблюдениями с
// охранными предикатами; эта спецификация — то, с чем сверяются eval
// (наблюдения) и track (переходы) в alerts.go.
//
// # Состояния (единые для всех 19 машин)
//
//	ok           условие алерта не выполнено
//	firing       алерт сработал (Warn на переходе; дебаунс доставки —
//	             notify.go «только изменение состояния»)
//	not_measured алерт неприменим или источник деградировал: ошибка (err
//	             — Warn каждого прохода, цикл живёт) или тихая причина
//	             (reason — Debug: неприменимость ожидаема, SMART в
//	             контейнере); ложных срабатываний не даёт
//
// Статус до первого прохода — «» (пустой): все переходы из него
// фронтовые (срабатывание/неприменимость), как у прежней пустой карты
// состояний.
//
// # События
//
//	измерение  observation{value} — источник жив, наблюдение валидно
//	           (валидность оценивает eval: минимум выборки, выключенный
//	           контроль, maxmemory 0 — reason, не значение)
//	деградация observation{err | reason} — источник сломан/неприменим;
//	           track-память НЕ трогается (неизвестное наблюдение — не
//	           наблюдение: серии, окна, защёлки переживают сбой;
//	           восстановление доедает по фактическим данным)
//
// # Переходы: track-тип × охрана → цель
//
//	| track-тип | машины | включение → firing | удержание firing | снятие → ok |
//	|-----------|--------|--------------------|-----------------|-------------|
//	| порог уровня thresholdTrack | progress_stall, disk_free, disk_free_min, disk_free_crit, nvme_wear, unit_oom | предикат наблюдения | — (памяти нет: каждый проход решает заново; «память» — сам наблюдаемый уровень, монотонный по механике: возраст метки, место, износ, счётчики событий) | тот же предикат |
//	| гистерезис порогов hysteresisTrack | dns_temp_error_share, queue_overflow, redis_memory | fire(v) | prev = firing ∧ hold(v) — коридор снятия не дребезжит Warn | ¬(fire ∨ hold) |
//	| серия: deadStreakTrack | queue_dead | ненулевая глубина N=3 проходов подряд | prev = firing ∧ глубина > 0 | нулевая глубина |
//	| серия: memHighStreakTrack | memory_high | рост счётчика high юнита N=2 проходов подряд (любой юнит) | серия горячего юнита не оборвана | первый проход без роста |
//	| серия: counterGrowthTrack | render_browser_errors | рост счётчика N=2 проходов подряд | серия не оборвана | первый проход без роста |
//	| серия: newDoneStaleTrack | domains_new_done_stale | НОВЫЙ ненулевой снапшот реконсиляции N=2 подряд (смена updated_at: проходы алертов чаще источника, повторное чтение наблюдением не считается) | prev = firing ∧ значение > 0 | нулевой снапшот |
//	| защёлка уровня: renderDebtTrack | render_debt | уровень > порога ∧ серия роста N=2 (строгое >) | защёлка firing при уровне ≥ порога снятия (90 %, ceil) — стагнация и колебания лага эпизод не рвут | уровень < порога снятия |
//	| защёлка уровня: renderFbDepthTrack | render_fallback_depth | глубина ≥ порога ∧ (серия роста ИЛИ серия «у порога» N=2 — стационарная глубина, S-P2-1) | защёлка при глубине ≥ порога снятия | глубина < порога снятия |
//	| кольцо дельт ringTrack | ingest_bad_hashes, render_fallback_budget | сумма дельт за окно (15/6 проходов) ≥ порога (10/1) | продолжение потока событий продлевает сумму | старение: вытеснение дельт из кольца |
//	| окно времени: psiWindowTrack | psi_cpu | avg300 > порога устойчиво окно (30 мин: первый проход выше порога стартует окно, срабатывание при закрытии) | удержание = окно тикает | провал под порог сбрасывает окно (шпилька не продлевает) |
//	| окно времени: queueFreezeTrack | queue_frozen | last-delivered-id группы неподвижен ≥ окна (15 мин) при глубине > 0 | неподвижность держится | движение ID / глубина 0 / групп нет |
//
//	Деградация (любая машина): → not_measured, track-память нетронута.
//	Защёлки уровня — единственные, чей ПРОЖИТЫЙ эпизод переживает
//	деградацию (firing живёт в track-памяти, не в статусе); у гистерезисов
//	и серий «не измеряется» перезаписывает статус — возврат источника
//	в коридоре удержания даёт ok, не удержание (прежняя семантика карты
//	состояний, задокументирована как решение).
//
// # Побочные действия перехода (единые, только transition)
//
//   - смена статуса — фронтовая запись журнала (logFront): срабатывание
//     Warn «АЛЕРТ», снятие Info, неприменимость Debug; уход сработавшего
//     в «не измеряется» — Info (потеря активной аварии видна в логе);
//   - ошибка источника — Warn каждого прохода (кроме отмены ctx — часть
//     остановки сервиса);
//   - снапшот прохода — AlertSnapshot в AlertsResult → хук OnPass
//     (Telegram-доставка notify.go, метрики T-077).
//
// # Инварианты
//
//	И1  Состояние автомата (статус и track-память) пишет ТОЛЬКО
//	    alertMachine.transition: eval-функции возвращают наблюдение и
//	    состояния не касаются; decide трек-памяти — единственный писатель
//	    собственной памяти и вызывается только transition-ом с валидным
//	    измерением.
//	И2  Деградация источника не трогает track-память: серии/окна/защёлки
//	    переживают сбой; статус при этом перезаписывается в
//	    not_measured (коридоры гистерезиса, привязанные к статусу,
//	    сбрасываются — прежнее поведение).
//	И3  Фронтовые записи — только на смене статуса (дебаунс); повторный
//	    проход в том же состоянии тих.
//	И4  Порядок машин фиксирован (NewAlerts): снапшот AlertsResult
//	    детерминирован, очередь чтений Redis источников стабильна.
//	И5  Каждый алерт — значение единой структуры alertMachine; отличает
//	    машины только тип track-памяти и источник наблюдения (реестр
//	    машин — NewAlerts, спецификации источников — alerts.go).
package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"crawler/internal/queue"
)

// observation — наблюдение одного прохода источника алерта: измерение
// (value), тихая деградация с причиной (reason — «не измеряется» без
// Warn: неприменимость ожидаема — SMART в контейнере, выключенный
// контроль, мало данных) или ошибка источника (err — Warn каждого
// прохода, цикл живёт). Eval-функции возвращают наблюдение и НЕ пишут
// состояние — состояние пишет только alertMachine.transition (И1).
type observation[V any] struct {
	value  V
	reason string
	err    error
}

// decision — решение перехода одной машины: статус прохода и деталь
// снапшота (детали прежние — тексты мигрированы из eval-функций без
// правок).
type decision struct {
	status string
	detail string
}

// alertTrack — типизированная track-память автомата одного алерта:
// применяет измерение к памяти (серии/окна/защёлки) и решает статус
// прохода. decide — единственный писатель собственной памяти; вызывается
// только transition-ом и только с валидным измерением — деградации
// память не трогают (И1/И2). Реализации (семьи): thresholdTrack (порог
// уровня, без памяти), hysteresisTrack (гистерезис порогов),
// deadStreakTrack/memHighStreakTrack/counterGrowthTrack/
// newDoneStaleTrack (серия), renderDebtTrack/renderFbDepthTrack
// (защёлка уровня), ringTrack (кольцо дельт), psiWindowTrack/
// queueFreezeTrack (окно времени).
type alertTrack[V any] interface {
	// decide применяет наблюдение v к памяти; prev — прошлый статус
	// автомата (охраны гистерезиса и удержания серий), now — текущее
	// время (окна, возрасты).
	decide(prev string, now time.Time, v V) decision
}

// alertMachine — единая структура автомата алерта (ФТ-10): «{имя,
// статус, track-память}». Все 19 алертов — значения этого типа;
// отличается машины только тип track-памяти (alertTrack) и источник
// наблюдения (eval — observe*-функция в alerts.go). Единственная точка
// записи состояния — transition (И1).
type alertMachine[V any] struct {
	name   string
	status string
	track  alertTrack[V]
	eval   func(ctx context.Context) observation[V]
}

// alertRunner — стирание типа alertMachine[V] для гетерогенного списка
// машин прохода (порядок фиксирован — И4).
type alertRunner interface {
	run(ctx context.Context, a *Alerts) AlertSnapshot
}

// compile-time: alertMachine реализует alertRunner при любом V.
var _ alertRunner = (*alertMachine[int64])(nil)

// run — один проход машины: наблюдение источника → переход.
func (m *alertMachine[V]) run(ctx context.Context, a *Alerts) AlertSnapshot {
	return m.transition(a, ctx, m.eval(ctx))
}

// transition — ЕДИНСТВЕННАЯ точка записи состояния автомата (ФТ-10, И1):
// применяет наблюдение (деградация переводит в «не измеряется», не
// трогая track-память — И2), пишет фронтовую запись на смене статуса
// (И3) и обновляет статус. Ошибка источника — Warn каждого прохода,
// кроме отмены ctx (часть остановки, стиль ZoneStatsLoop).
func (m *alertMachine[V]) transition(a *Alerts, ctx context.Context, o observation[V]) AlertSnapshot {
	prev := m.status
	var status, detail string
	switch {
	case o.err != nil:
		if ctx.Err() == nil {
			a.log.Warn("оценка алерта: "+alertDescs[m.name], "alert", m.name, "err", o.err)
		}
		status, detail = AlertStatusNotMeasured, o.err.Error()
	case o.reason != "":
		status, detail = AlertStatusNotMeasured, o.reason
	default:
		d := m.track.decide(prev, a.now(), o.value)
		status, detail = d.status, d.detail
	}
	a.logFront(m.name, prev, status, detail)
	m.status = status
	return AlertSnapshot{Name: m.name, Status: status, Detail: detail}
}

// ------------------------------------------------------------ порог уровня

// thresholdTrack — машина без track-памяти: решение — предикат
// наблюдения (порог уровня). Памятью служит сам наблюдаемый уровень
// (возраст метки зон, место на диске, износ NVMe, счётчики событий
// юнитов) — величины монотонны или событийны по механике источника,
// дребезг у порога исключён без зазора (обоснования — у источников в
// alerts.go). Порог может быть неприменим к наблюдению (na: абсолютный
// минимум ≥ ёмкости тома) — тихое «не измеряется»; nil — всегда применим.
type thresholdTrack[V any] struct {
	fire      func(now time.Time, v V) bool
	okDetail  func(now time.Time, v V) string
	hotDetail func(now time.Time, v V) string // nil — деталь ok (диск: деталь одна)
	na        func(v V) string                // неприменимость (деталь) или "" — применим; nil — нет проверки
}

// decide — переход порога: na → not_measured, fire → firing, иначе ok.
func (t thresholdTrack[V]) decide(_ string, now time.Time, v V) decision {
	if t.na != nil {
		if d := t.na(v); d != "" {
			return decision{AlertStatusNotMeasured, d}
		}
	}
	if t.fire(now, v) {
		d := t.okDetail(now, v)
		if t.hotDetail != nil {
			d = t.hotDetail(now, v)
		}
		return decision{AlertStatusFiring, d}
	}
	return decision{AlertStatusOK, t.okDetail(now, v)}
}

// --------------------------------------------------------- гистерезис порогов

// hysteresisTrack — гистерезис порогов: включение по fire, удержание
// сработавшего по hold (коридор снятия), память — статус автомата
// (prev): значение в коридоре между порогами не дребезжит Warn.
// Коридор живёт только пока статус firing: «не измеряется» (сбой
// источника) статус перезаписывает — возврат источника в коридоре даёт
// ok, а не удержание (прежняя семантика карты состояний, И2).
type hysteresisTrack[V any] struct {
	fire      func(v V) bool
	hold      func(v V) bool
	okDetail  func(v V) string
	hotDetail func(v V) string // nil — деталь ok (доля ошибок DNS одна на оба статуса)
}

// decide — переход гистерезиса: fire ∨ (prev = firing ∧ hold) → firing.
func (t hysteresisTrack[V]) decide(prev string, _ time.Time, v V) decision {
	if t.fire(v) || (prev == AlertStatusFiring && t.hold(v)) {
		d := t.okDetail(v)
		if t.hotDetail != nil {
			d = t.hotDetail(v)
		}
		return decision{AlertStatusFiring, d}
	}
	return decision{AlertStatusOK, t.okDetail(v)}
}

// dnsShare — наблюдение доли temp_error суток: числитель (события
// записей результатов с вердиктом temp_error) и знаменатель (события
// записей результатов) суточных строк crawler_counters (T-182).
type dnsShare struct {
	temp  int64
	total int64
}

// share — доля temp_error (знаменатель ≥ AlertDNSMinSamples — гарантирует
// observeDNS, пустое окно наблюдением не становится).
func (d dnsShare) share() float64 {
	return float64(d.temp) / float64(d.total)
}

// staleRow — наблюдение строки new_done_stale: значение (домены
// «статус повис») и метка снапшота реконсиляции (updated_at строки —
// момент прохода, писавшего значение).
type staleRow struct {
	value int64
	snap  sql.NullTime
}

// ------------------------------------------------------------- серия проходов

// deadStreakTrack — серия ненулевых проходов (queue_dead, T-154):
// «устойчиво ненулевой» карантин вместо мгновенного срабатывания на
// первую смерть; сработавший удерживается любой ненулевой глубиной
// (гистерезис эпизода: от третьего прохода до разбора оператором),
// снятие — пустой q:dead. Рост — частный случай ненулёвости: отдельный
// контроль роста дребезжал бы на подвижном XTRIM разбора.
type deadStreakTrack struct {
	minPasses int
	streak    int
}

// decide — переход серии q:dead.
func (t *deadStreakTrack) decide(prev string, _ time.Time, n int64) decision {
	if n > 0 {
		t.streak++
	} else {
		t.streak = 0
	}
	detail := fmt.Sprintf("глубина q:dead=%d, ненулевых проходов подряд %d/%d; темп смертей — crawler_queue_dead_buried_total",
		n, t.streak, t.minPasses)
	if t.streak >= t.minPasses || (prev == AlertStatusFiring && n > 0) {
		return decision{AlertStatusFiring, detail + " — разбор: runbooks §2"}
	}
	return decision{AlertStatusOK, detail}
}

// memHighTrack — память серии одного юнита: прошлый счётчик
// memory.events high и серия проходов подряд с его ростом. Ростом
// считается только увеличение: рестарт юнита пересоздаёт cgroup и
// обнуляет счётчик — убыль начинает новую серию без ложного всплеска
// (стиль deltaRing).
type memHighTrack struct {
	prev   uint64
	streak int
	seen   bool
}

// memHighStreakTrack — серия роста троттлинга MemoryHigh по юнитам
// (memory_high, T-162): событие — рост счётчика high юнита между
// проходами (порог — не значение, а скорость: счётчик накапливается за
// жизнь cgroup). Срабатывание, когда серия какого-то юнита достигает
// minPasses; снятие — первый проход без роста (событие снятия и есть
// «рост прекратился», дребезг у нуля серии невозможен — серия целая).
// Юниты, исчезнувшие из ответа (переименован/выгружен), память не
// копят.
type memHighStreakTrack struct {
	minPasses int
	units     map[string]memHighTrack
}

// decide — переход серии MemoryHigh.
func (t *memHighStreakTrack) decide(_ string, _ time.Time, units []UnitEvents) decision {
	seen := make(map[string]bool, len(units))
	var hot []string
	for _, u := range units {
		seen[u.Unit] = true
		tr := t.units[u.Unit]
		grew := int64(0)
		if tr.seen && u.High > tr.prev {
			tr.streak++
			grew = int64(u.High - tr.prev)
		} else {
			tr.streak = 0
		}
		tr.seen, tr.prev = true, u.High
		t.units[u.Unit] = tr
		if tr.streak >= t.minPasses {
			hot = append(hot, fmt.Sprintf("%s: +%d за проход, серия %d", u.Unit, grew, tr.streak))
		}
	}
	for name := range t.units { // юнит исчез (переименован/выгружен) — не копить
		if !seen[name] {
			delete(t.units, name)
		}
	}
	if len(hot) > 0 {
		return decision{
			AlertStatusFiring,
			strings.Join(hot, ", ") + " — устойчивый троттлинг MemoryHigh (reclaim), предвестник фаталов (T-157)",
		}
	}
	return decision{
		AlertStatusOK,
		fmt.Sprintf("роста троттлинга MemoryHigh нет (%d юнитов, порог — рост %d проходов подряд)",
			len(units), t.minPasses),
	}
}

// counterGrowthTrack — серия роста одиночного счётчика между проходами
// (render_browser_errors, S-P2-6): прошлый снимок зонда и серия растущих
// проходов (стиль memHighTrack; отдельный тип — счётчик int64, как
// значения зондов /metrics). Рестарт владельца (убыль) — нулевая дельта,
// серия с нуля.
type counterGrowthTrack struct {
	minPasses int
	prev      int64
	streak    int
	seen      bool
}

// decide — переход серии роста счётчика.
func (t *counterGrowthTrack) decide(_ string, _ time.Time, cur int64) decision {
	var delta int64
	if t.seen && cur > t.prev {
		t.streak++
		delta = cur - t.prev
	} else {
		t.streak = 0
	}
	t.seen, t.prev = true, cur
	detail := fmt.Sprintf("browser-ошибки рендера %d, серия роста %d/%d (+%d за проход)",
		cur, t.streak, t.minPasses, delta)
	if t.streak >= t.minPasses {
		return decision{
			AlertStatusFiring,
			detail + " — движок рендера завис или недоступен: health-гейт приостановил фолбэк-публикации, браузер-класс в redelivery q:render (откат движка — ТЗ рендера §6.4, разбор — docs/render.md)",
		}
	}
	return decision{AlertStatusOK, detail}
}

// newDoneStaleTrack — серия НОВЫХ ненулевых снапшотов реконсиляции
// (domains_new_done_stale, T-256 ФТ-2.2): проходы алертов (5 мин) чаще
// проходов источника (zone_stats_interval, 30 мин) — серия растёт только
// при смене метки снапшота (updated_at строки), повторное чтение той же
// строки наблюдением не считается (одиночный пограничный снапшот
// границы суток следующим проходом перезаписывается нулём — алерта
// нет). Нулевой снапшот серию обнуляет; сработавший удерживается любой
// ненулевой строкой (стиль queue_dead).
type newDoneStaleTrack struct {
	minPasses int
	snap      time.Time
	streak    int
	seen      bool
}

// decide — переход серии снапшотов new_done_stale.
func (t *newDoneStaleTrack) decide(prev string, _ time.Time, row staleRow) decision {
	n := row.value
	if n > 0 {
		// Серия — число различных ненулевых снапшотов: повторное чтение
		// той же строки (алерт ходит чаще реконсиляции) серию не
		// наращивает; нулевой снапшот обнуляет.
		switch {
		case !t.seen || !row.snap.Valid || !row.snap.Time.Equal(t.snap):
			t.streak++
		}
		t.seen = true
		t.snap = row.snap.Time
	} else {
		t.streak = 0
	}
	detail := fmt.Sprintf("доменов new∧done старше суток %d, ненулевых снапшотов реконсиляции подряд %d/%d (значение — проход реконсиляции, gauge crawler_domains_new_done_stale)",
		n, t.streak, t.minPasses)
	if t.streak >= t.minPasses || (prev == AlertStatusFiring && n > 0) {
		return decision{
			AlertStatusFiring,
			detail + " — потеря задач q:http: HTTP-итог не записан, компенсатора нет (лечит перепроверка T-030; спецификация — internal/model/enums.go)",
		}
	}
	return decision{AlertStatusOK, detail}
}

// ------------------------------------------------------------- защёлка уровня

// depthGrowth — память уровня-глубины: прошлый снимок, серия проходов с
// ростом, защёлка сработавшего эпизода и серия проходов «у порога»
// (hot — только render_fallback_depth). Рост — строго увеличение:
// дренаж и приток плещутся вокруг константы — стагнация, а не
// накопление, серию не копит. Защёлка firing переживает «не измеряется»
// (сбой источника состояние алерта перезаписывает, ревью T-175 раунд 2,
// P2-1): удержание обязано опираться на неё, а не на прошлый статус —
// иначе короткий сбой БД/Redis посреди эпизода давал бы ложное ✅
// «Восстановлено» при долге выше порога снятия; сбрасывает защёлку
// только фактический дренаж ниже порога. hot ловит стационарно высокую
// глубину, которую серия роста не видит — рестарт монитора посреди
// эпизода (первый проход базирует prev и серию роста нулём) и упор в
// предел MAXLEN стрима (обрезка вытесняет старейшие недоставленные
// записи — глубина перестаёт расти).
type depthGrowth struct {
	prev   int64
	streak int
	seen   bool
	firing bool
	hot    int
}

// growthStep применяет новый уровень: обновляет серию роста (строгое
// увеличение) и серию «у порога» (atThreshold — уровень достиг порога
// срабатывания; render_debt порога «уровня» не ведёт — стагнация долга
// — строка отчёта нагрузки, не алерт), базирует первый снимок; возвращает
// дельту роста для детали.
func (g *depthGrowth) growthStep(level int64, atThreshold bool) (delta int64) {
	if g.seen && level > g.prev {
		g.streak++
		delta = level - g.prev
	} else {
		g.streak = 0
	}
	if atThreshold {
		g.hot++
	} else {
		g.hot = 0
	}
	g.seen, g.prev = true, level
	return delta
}

// renderDebtTrack — защёлка уровня рендер-долга (render_debt, T-175):
// срабатывание — долг (флаги render_pending + лаг q:render) строго выше
// порога И растёт minPasses проходов подряд (серия положительных дельт,
// стиль memory_high). Снятие — дренаж ниже 90 % порога: долг — УРОВЕНЬ,
// а не монотонный счётчик, лаг колеблется на сотни-тысячи за проход при
// дренаже ~1–2 стр/с — снятие «по не-росту» качало бы эпизод 🚨/✅ каждые
// несколько проходов на стационарном высоком долге (ревью T-175, P2-1).
// Поэтому рост — только триггер (стагнация без серии роста не будит
// оператора — она видна строкой «Рендер-долг» отчёта нагрузки и gauge),
// уровень — удержание и снятие (порог снятия 90 %, ceil — при пороге 1
// снятие < 1 достигается нулём, ревью раунд 2 P3-3). Удержание
// опирается на защёлку памяти, а не прошлый статус (И2). Первый проход —
// база (рост неизвестен).
type renderDebtTrack struct {
	domains int64
	passes  int
	mem     depthGrowth
}

// decide — переход защёлки рендер-долга.
func (t *renderDebtTrack) decide(_ string, _ time.Time, d RenderDebt) decision {
	total := d.Total()
	delta := t.mem.growthStep(total, false)

	// Порог снятия — 90 % порога срабатывания, целочисленный ceil:
	// коридор ≥ 1 при любом допустимом пороге (минимум 1) — сработавший
	// алерт всегда достижим для снятия; при 50 тыс. снятие — ниже 45 тыс.
	recoverAt := (t.domains*9 + 9) / 10
	detail := fmt.Sprintf("рендер-долг: %s, порог %d (снятие < %d), серия роста %d/%d проходов (+%d за проход)",
		d.String(), t.domains, recoverAt, t.mem.streak, t.passes, delta)
	switch {
	case total > t.domains && t.mem.streak >= t.passes:
		t.mem.firing = true
		return decision{
			AlertStatusFiring,
			detail + " — накопление: приток SPA обгоняет дренаж (render.contexts / приток планировщика, разбор — docs/runbooks.md)",
		}
	case t.mem.firing && total >= recoverAt:
		// Удержание сработавшего: колебания лага при стационарном долге
		// (серия роста сброшена, уровень выше порога снятия) — эпизод
		// живёт до фактического дренажа. Защёлка, а не прошлый статус:
		// сбой источника («не измеряется») эпизод не закрывает.
		return decision{
			AlertStatusFiring,
			detail + " — долг держится выше порога снятия (уровень — строка «Рендер-долг» отчёта нагрузки)",
		}
	default:
		t.mem.firing = false
		return decision{AlertStatusOK, detail}
	}
}

// renderFbDepthTrack — защёлка уровня глубины q:render-fallback
// (render_fallback_depth, T-246, ТЗ рендера §4.6 п.3; второй триггер —
// S-P2-1, решение оператора): срабатывание — глубина у эффективного
// порога стрима (единый расчёт queue.StreamWatermark — как у алерта
// очередей; для q:render-fallback это не порог гейта, а порог наблюдения
// «глубина приближается к пределу MAXLEN») И (растёт minPasses проходов
// подряд — механика render_debt — ИЛИ держится над порогом minPasses
// проходов без требования роста: стационарная глубина у порога —
// остановленный воркер B либо упор в MAXLEN, XADD вытесняет старейшие
// недоставленные записи и новые фолбэк-задачи молча теряются).
// Снятие — дренаж ниже 90 % порога (ceil, стиль render_debt); стагнация
// выше порога удерживается защёлкой, повторно не будит.
type renderFbDepthTrack struct {
	wm     int64
	passes int
	mem    depthGrowth
}

// decide — переход защёлки глубины фолбэка.
func (t *renderFbDepthTrack) decide(_ string, _ time.Time, n int64) decision {
	delta := t.mem.growthStep(n, n >= t.wm)

	// Порог снятия — 90 % порога срабатывания, целочисленный ceil
	// (стиль render_debt): коридор ≥ 1 при любом валидном watermark.
	recoverAt := (t.wm*9 + 9) / 10
	detail := fmt.Sprintf("глубина %s=%d, порог %d (снятие < %d), серия роста %d/%d (+%d за проход), у порога %d/%d проходов",
		queue.StreamRenderFallback, n, t.wm, recoverAt, t.mem.streak, t.passes, delta,
		t.mem.hot, t.passes)
	switch {
	case n >= t.wm && (t.mem.streak >= t.passes || t.mem.hot >= t.passes):
		t.mem.firing = true
		if t.mem.streak >= t.passes {
			return decision{
				AlertStatusFiring,
				detail + " — накопление: приток фолбэка обгоняет темп воркера B (render.fallback.worker_rate / движок-кандидат, разбор — docs/render.md)",
			}
		}
		return decision{
			AlertStatusFiring,
			detail + " — глубина стационарно у порога без роста: воркер B стоит либо очередь упёрлась в предел MAXLEN стрима (XADD вытесняет недоставленное), разбор — docs/render.md",
		}
	case t.mem.firing && n >= recoverAt:
		return decision{
			AlertStatusFiring,
			detail + " — глубина держится выше порога снятия (темп/глубина — панель движков дашборда)",
		}
	default:
		t.mem.firing = false
		return decision{AlertStatusOK, detail}
	}
}

// ------------------------------------------------------------- кольцо дельт

// deltaRing — кольцо дельт счётчика между проходами (память окна):
// ёмкость size; 0 — AlertBadHashesWindow (историческое поведение
// всплеска мусорных хешей). Первое наблюдение только базирует prev —
// старт мониторинга позже накопленного счётчика не ретроспективен (окно
// считает новые события, а не всю историю); убывание счётчика (рестарт
// владельца) даёт нулевую дельту — счётчик начинается заново, ложного
// всплеска нет.
type deltaRing struct {
	prev   int64
	seen   bool
	deltas []int64
	i      int
	// size — ёмкость кольца в проходах; 0 — AlertBadHashesWindow.
	size int
}

// push принимает текущее значение счётчика и возвращает сумму окна.
func (w *deltaRing) push(cur int64) int64 {
	var d int64
	if !w.seen {
		w.seen = true // база: накопленное до старта мониторинга не считается
		// Эквивалентный мутант: при cur == prev обе ветви дают дельту 0.
		// mutator-disable-next-line expression/comparison
	} else if cur > w.prev {
		d = cur - w.prev
	}
	w.prev = cur
	if w.deltas == nil {
		size := w.size
		if size <= 0 {
			size = AlertBadHashesWindow
		}
		w.deltas = make([]int64, size)
	}
	w.deltas[w.i] = d
	// Эквивалентный мутант шага +2: 2 взаимно просто с длиной окна 15 —
	// кольцо в обоих случаях содержит последние 15 дельт, сумма окна
	// неотличима тестом.
	// mutator-disable-next-line numbers/incrementer
	w.i = (w.i + 1) % len(w.deltas)
	var sum int64
	for _, d := range w.deltas {
		sum += d
	}
	return sum
}

// ringTrack — окно событий в проходах (кольцо дельт): сумма дельт
// счётчика за окно ≥ threshold — срабатывание; снятие — старением (окно
// держит сработавшим только продолжение потока событий), против дребезга
// гистерезис не нужен — сумма окна убывает только вытеснением дельт, у
// порога она держится лишь при продолжении потока. Порог 1 превращает
// кольцо в «любое событие за окно» (бюджет доли фолбэка: каждое
// защёлкивание приостановки — аномалия, стиль износа NVMe).
type ringTrack struct {
	ring      *deltaRing
	threshold int64
	// passes — окно в проходах и interval — период алертов: только для
	// детали (человекочитаемая длительность окна).
	passes   int
	interval time.Duration
	// label — имя измерения в детали («счётчик», «события бюджета»),
	// fireNote — суффикс детали срабатывания.
	label    string
	fireNote string
}

// decide — переход кольца: сумма окна против порога.
func (t ringTrack) decide(_ string, _ time.Time, cur int64) decision {
	sum := t.ring.push(cur)
	detail := fmt.Sprintf("%s %d, новых за окно %s — %d (порог %d)",
		t.label, cur, (time.Duration(t.passes) * t.interval).Round(time.Second), sum, t.threshold)
	if sum >= t.threshold {
		return decision{AlertStatusFiring, detail + t.fireNote}
	}
	return decision{AlertStatusOK, detail}
}

// --------------------------------------------------------------- окно времени

// psiWindowTrack — окно удержания PSI CPU (psi_cpu, T-162): avg300 выше
// порога устойчиво окно — срабатывание. Первый проход выше порога
// стартует окно, срабатывание при его закрытии; avg300 сам сглаживает
// 5 минут, окно отбрасывает разовые шпильки (деплой, ретро-пачки).
// Гистерезис не нужен: чтобы «снять», avg300 должен опуститься к порогу
// после 30+ минут выше — фактическое восстановление; сброс окна при
// кратком провале — консервативнее удержания (шпилька не продлевает
// алерт).
type psiWindowTrack struct {
	percent float64
	window  time.Duration
	hot     time.Time
}

// decide — переход окна PSI.
func (t *psiWindowTrack) decide(_ string, now time.Time, st PSIStat) decision {
	over := st.Avg300 > t.percent
	switch {
	case !over:
		t.hot = time.Time{}
	case t.hot.IsZero():
		t.hot = now
	}
	detail := fmt.Sprintf("some avg10=%.2f%% avg60=%.2f%% avg300=%.2f%%, порог %.0f%% (окно %s)",
		st.Avg10, st.Avg60, st.Avg300, t.percent, t.window)
	if over {
		held := now.Sub(t.hot)
		if held >= t.window {
			return decision{
				AlertStatusFiring,
				detail + fmt.Sprintf(" — выше порога уже %s", held.Round(time.Second)),
			}
		}
		return decision{
			AlertStatusOK,
			detail + fmt.Sprintf(" — выше порога %s из окна, накопление", held.Round(time.Second)),
		}
	}
	return decision{AlertStatusOK, detail}
}

// freezeEntry — память одной группы стрима: последний замеченный
// last-delivered-id и момент, с которого он стоит. since живёт только
// при глубине > 0 — простой пустой очереди не накапливает «время без
// доставок» (час простоя + первая нечитаемая запись дали бы мгновенное
// ложное срабатывание).
type freezeEntry struct {
	lastID string
	since  time.Time
}

// queueFreezeTrack — окно неподвижности групп (queue_frozen, T-162):
// last-delivered-id группы рабочего стрима не движется ≥ окна при
// глубине > 0. Живой потребитель двигает ID каждым XREADGROUP (пачки в
// секунды); стоящий ID при непустом backlog — записи не доставляются
// вовсе: потребитель мёртв или валится на первой записи (яд T-159),
// lease-восстановление не поднимает. Первое наблюдение группы — база с
// текущего момента (после рестарта monitor алерт сработает не раньше
// окна, не ретроспективно); движение ID обновляет момент; глубина 0 —
// запись вычищена; группа исчезла из ответа (стрим удалён) — не копится.
// Снятие — первый проход без замороженных групп; гистерезис не нужен,
// окно — естественный фильтр дребезга.
type queueFreezeTrack struct {
	window time.Duration
	groups map[string]freezeEntry
}

// decide — переход окна заморозки групп.
func (t *queueFreezeTrack) decide(_ string, now time.Time, groups []StreamGroupState) decision {
	seen := make(map[string]bool, len(groups))
	var frozen []string
	var moving int
	for _, g := range groups {
		key := g.Stream + "/" + g.Group
		seen[key] = true
		if g.Depth <= 0 {
			delete(t.groups, key) // работа кончилась — копить нечего
			moving++
			continue
		}
		tr, ok := t.groups[key]
		if !ok || g.LastDeliveredID != tr.lastID {
			t.groups[key] = freezeEntry{lastID: g.LastDeliveredID, since: now}
			moving++
			continue
		}
		idle := now.Sub(tr.since)
		if idle >= t.window {
			frozen = append(frozen, fmt.Sprintf("%s/%s: доставки нет %s при глубине %d (последний ID %s)",
				g.Stream, g.Group, idle.Round(time.Second), g.Depth, g.LastDeliveredID))
		}
	}
	for key := range t.groups { // группа исчезла (стрим удалён) — не копить
		if !seen[key] {
			delete(t.groups, key)
		}
	}
	if len(frozen) > 0 {
		return decision{
			AlertStatusFiring,
			strings.Join(frozen, "; ") + " — потребитель не читает: лог юнита-воркера и q:dead (яд T-159), перезапуск воркера",
		}
	}
	return decision{
		AlertStatusOK,
		fmt.Sprintf("групп с движением/без backlog — %d из %d (окно %s)",
			moving, len(groups), t.window),
	}
}

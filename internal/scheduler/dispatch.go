// Package scheduler — планировщик проверок (ТЗ 5.3): выборка доменов
// check_state = pending пачками с круговым распределением между зонами
// (этот файл, T-027), перевод выбранных в queued с публикацией в q:dns
// (T-028), возврат доменов с истёкшим lease (T-029), плановые перепроверки
// done- и temp_error-доменов (T-030/T-031).
//
// Выборка пачки (T-027) — два дешёвых запроса вместо одной оконной
// функции по всему множеству pending (которое на целевом корпусе — сотни
// миллионов строк, ТЗ 3.3):
//
//   - активные зоны с работой: zones — малый справочник, читается целиком
//     с фильтром domains_pending > 0 (T-183, счётчик T-178): семантика
//     колонки — предикат LATERAL-выборки (check_state = pending по
//     базовому множеству status <> deleted), фильтр отсекает ровно зоны,
//     чья проба вернула бы 0 строк, — до T-183 круг 1 пробил все активные
//     зоны справочника IANA (T-125), ~2000 из них пусты по pending;
//   - префикс очереди каждой зоны: LATERAL-подзапрос по зоне с LIMIT —
//     ровно частичный индекс domains_pending_idx (zone_id, id) WHERE
//     check_state = 0 (миграция 000001): чтение и сортировка идут по
//     готовому индексу, число прочитанных строк пропорционально пачке и
//     числу зон, а не объёму pending.
//
// Круговое чередование зон (ТЗ 5.3: «по кругу между зонами, чтобы крупная
// зона не задерживала остальные») собирается в приложении по тем же
// правилам, что раньше задавал SQL:
//
//   - каждая зона — очередь: внутри зоны домены упорядочены по id (порядок
//     LATERAL-выборки = префикс очереди зоны, предыдущие пачки потребили
//     её начало — переведённые в queued строки покидают выборку);
//   - позиция rn — номер в очереди зоны (1-based); слияние берёт
//     элемент зоны с минимальным ключом (rn − 0.5)/weight, при
//     равенстве — меньший zone_id: за одной зоной в пачке не идут её
//     соседние строки, крупная зона не забивает пачку;
//   - вес зоны (ТЗ 5.3: «веса зон — опционально») сдвигает позицию её
//     элемента к (rn − 0.5)/weight — зона с весом w занимает в общем
//     порядке долю, пропорциональную w (шаговое планирование); вес 1 у
//     всех вырождается в равный круговой порядок. Колонки weight в схеме
//     нет (ТЗ 6.1 её не требует), веса приходят параметрами DispatchQuery;
//     источник значений — конфигурация scheduler.zone_weights: имена зон
//     разрешаются в zone_id по справочнику (ResolveZoneWeights, T-106).
//
// Долевая квота зоны на пачку — LIMIT LATERAL-подзапроса: ceil(L ×
// max_вес / Σвесов зон раунда) + 1 — верхняя оценка доли самой тяжёлой
// зоны (у равных весов — ceil(L/число зон) + 1: столько строк одной зоны
// максимально в первых L кругового порядка; зоны раунда — только
// активные с работой, отфильтрованные зоны строк не дают). Запас +1 и
// чтение «лишних» строк безвредны: внешнее ограничение — LIMIT пачки
// в слиянии.
//
// Дозаполнение: если пула прочитанных строк не хватило на полную пачку
// (часть зон исчерпана — их доли уходят соседям), выборка повторяется
// только для «голодных» зон (вернули ровно квоту раунда — возможно,
// обрезаны), с keyset-курсором по последнему id зоны (d.id > after:
// смещения OFFSET не нужны и невозможны — перевод в queued меняет
// множество между запросами). Квота раунда дозаполнения — остаток пачки
// (Limit − набрано, T-183): вклад любой зоны в пачку не превосходит
// остатка, чтение сверх в слияние не попадает (внешнее ограничение —
// LIMIT пачки); худший случай «пачка из одной зоны» — остаток = Limit,
// поведение прежнее. Не более dispatchMaxRounds раундов: хвост доберёт
// следующий вызов цикла (доставка at-least-once, ТЗ 3.4; выборка
// состояние не меняет — до T-028).
//
// Охрана d.status <> deleted: применение delete (T-019) переводит статус,
// не трогая check_state, — без охраны удалённые домены оставались бы
// кандидатами вечно, хотя их результаты воркеры не записывают (ТЗ §8);
// воскрешённый add-файлом домен имеет status = new и возвращается в
// выборку сам (T-018). Коды check_state/status вшиты в текст литералами
// через model.* — это коды схемы, а не данные (стиль internal/ingest);
// литералы нужны и для пригодности частичного индекса domains_pending_idx
// (параметр в предикате частичного индекса планировщик не доказывает).
// Неактивные зоны (is_active = false, crawlerctl zones disable) в выборку
// не попадают: их проверка приостановлена, домены остаются pending и
// вернутся в цикл при включении зоны.
//
// Память (ТЗ 3.5): пачка ограничена Limit (scheduler.batch_size, ТЗ 5.3:
// 1000–10000), прочитанные строки — зоны × квота.
package scheduler

import (
	"context"
	"fmt"
	"sort"

	"crawler/internal/db"
	"crawler/internal/model"
)

// dispatchMaxRounds — предел раундов дозаполнения пачки: каждый следующий
// раунд — отдельный запрос, но только по «голодным» зонам с keyset; чаще
// двух раундов не бывает (равные доли исчерпываются первым раундом),
// предел страхует патологические веса.
// Эквивалентные мутанты (T-M05): 3 и 5 неотличимы от 4 — тело цикла
// выполняется не более 2 раз: раунд 2 требует «голодную» зону (n == quota)
// при неполной пачке, а с раунда 1 quota = остаток пачки (Limit − набрано)
// и такая зона добавила остаток поверх уже прочитанных строк — суммарно
// доступно ≥ Limit, слияние заполняет пачку до Limit (break). Наблюдаемое
// число запросов одно и то же.
// mutator-disable-next-line numbers/decrementer, numbers/incrementer
const dispatchMaxRounds = 4

// dispatchZonesSQL — активные зоны с работой (малый справочник, без
// агрегатов): domains_pending > 0 — инкрементальный счётчик T-178 (ТЗИ
// §4.1), чья семантика — сам предикат LATERAL-выборки (check_state =
// pending по базовому множеству status <> deleted), поэтому фильтр
// отсекает ровно зоны, чья проба круга 1 вернула бы 0 строк (T-183:
// до фильтра круг 1 пробил все ~2204 активные зоны справочника IANA,
// ~2000 пусты по pending — запрос №9 журнала медленных, §2 ТЗ T-183).
// Свежость держат дельты переходов в тех же транзакциях (T-178): enqueue
// диспетчера сам декрементирует — колонка точна к следующей пачке;
// apply_add открывает новую зону сразу, без ожидания реконсиляции.
// Граница доверия: дрейф (ручной SQL мимо инкрементов) может скрыть зону
// с работой до ближайшей реконсиляции (monitor.zone_stats_interval) —
// наблюдается метрикой crawler_counters_drift; обратный случай (счётчик
// > 0, очередь пуста) — одна «холостая» проба, деградации нет. Зона вне
// выборки — её pending-домены ждут включения (crawlerctl) или
// пополнения.
const dispatchZonesSQL = "SELECT id, name FROM zones WHERE is_active AND domains_pending > 0"

// resolveZoneWeightsSQL — справочник зон целиком для разрешения имён
// весов из конфигурации (scheduler.zone_weights, T-106): не только
// активные — вес неактивной зоны разрешается и вступит в силу при её
// включении (crawlerctl zones enable, T-084) без рестарта планировщика.
// zones — малый справочник; name уникален (миграция 000001).
const resolveZoneWeightsSQL = "SELECT id, name FROM zones"

// dispatchZoneSQL — префиксы очередей зон: unnest пачки зон с keyset-
// курсорами (after = 0 — с головы), LATERAL читает очередь зоны по
// частичному индексу domains_pending_idx (ORDER BY id, LIMIT — квота).
// Порядок строк ответа не определён — слияние сортирует само.
var dispatchZoneSQL = fmt.Sprintf(
	"SELECT d.zone_id, d.id, d.label "+
		"FROM unnest($1::int2[], $2::int8[]) AS z(zone_id, after_id) "+
		"JOIN LATERAL ("+
		"SELECT dd.zone_id, dd.id, dd.label FROM domains AS dd "+
		"WHERE dd.zone_id = z.zone_id AND dd.id > z.after_id "+
		"AND dd.check_state = %d AND dd.status <> %d "+
		"ORDER BY dd.id LIMIT $3"+
		") AS d ON true",
	int16(model.CheckStatePending), int16(model.StatusDeleted))

// DispatchRow — домен выбранной пачки: вход перевода в queued и
// публикации в q:dns (T-028). Name — полное имя домена в punycode
// (label || '.' || zones.name, ТЗ 5.2: внутри системы ASCII-форма).
type DispatchRow struct {
	DomainID int64  `db:"domain_id"`
	ZoneID   int16  `db:"zone_id"`
	Name     string `db:"name"`
}

// DispatchQuery — параметры выборки пачки.
type DispatchQuery struct {
	// Limit — размер пачки: scheduler.batch_size (ТЗ 5.3: 1000–10000).
	Limit int
	// ZoneWeights — веса зон: zone_id → вес ≥ 1. nil или пустая карта —
	// все веса 1 (равный круговой порядок). Колонки weight в схеме нет
	// (ТЗ 6.1) — веса аргумент выборки, а не данные БД.
	ZoneWeights map[int16]int
}

// dispatchZone — зона выборки: справочное имя и очередь прочитанных
// строк (префикс pending-очереди зоны, id по возрастанию).
type dispatchZone struct {
	id   int16
	name string
	rows []dispatchPick
}

// dispatchPick — строка очереди зоны: ключ домена, метка (имя без зоны)
// и зона (для полного имени и слияния).
type dispatchPick struct {
	zoneID   int16
	domainID int64
	label    string
}

// DispatchBatch выбирает пачку доменов check_state = pending в круговом
// порядке между зонами (ТЗ 5.3): активные зоны с работой (domains_pending
// > 0, T-183) → префиксы их очередей по индексу → слияние с весами до
// Limit строк. Пустой результат — пустой срез без ошибки. Состояние
// доменов не меняется: перевод в queued и
// публикацию выполняет следующий шаг цикла (T-028), следующая выборка
// продолжает очереди зон с места потребления (переведённые строки
// покидают множество pending).
func DispatchBatch(ctx context.Context, pool db.DB, q DispatchQuery) ([]DispatchRow, error) {
	if err := q.dispatchValidate(); err != nil {
		return nil, err
	}
	zones, err := dispatchActiveZones(ctx, pool)
	if err != nil {
		return nil, err
	}
	if len(zones) == 0 {
		// Зон с работой нет — пробировать очереди нечего (активные зоны
		// без pending отфильтрованы domains_pending > 0, T-183; неактивные
		// — is_active, их pending ждёт включения, crawlerctl).
		return []DispatchRow{}, nil
	}

	// Квота зоны на раунд — верхняя оценка её доли в пачке (см. шапку).
	weights := q.ZoneWeights
	// Эквивалентный мутант (T-M05): maxW := 0 ≡ 1 — зоны здесь непусты
	// (ранний возврат выше), а каждый нормализованный вес ≥ 1, первый же
	// проход цикла выставит maxW ≥ 1.
	// mutator-disable-next-line numbers/decrementer
	sumW, maxW := 0, 1
	for _, z := range zones {
		w := weights[z.id]
		// Эквивалентные мутанты (T-M05): `w <= 1` и `w < 2` не меняют
		// поведение — значения карты ≥ 1 (dispatchValidate), отсутствующая
		// зона даёт 0: обе границы превращают 0 → 1 и присваивают w = 1
		// то же значение (no-op). `w < 0` (decrementer) отличим — сумма
		// нулевых весов даст деление на ноль — и не гасится.
		// mutator-disable-next-line expression/comparison, numbers/incrementer
		if w < 1 {
			w = 1
		}
		sumW += w
		// Эквивалентный мутант (T-M05): `w >= maxW` при w == maxW
		// присваивает maxW то же значение — итог (максимум весов) тот же.
		// mutator-disable-next-line expression/comparison
		if w > maxW {
			maxW = w
		}
	}
	quota := (q.Limit*maxW + sumW - 1) / sumW // ceil(L × maxW / ΣW)
	// Эквивалентные мутанты (T-M05): quota ≥ 1 всегда — L ≥ 1 (валидация),
	// maxW ≥ 1 и sumW ≥ maxW (максимум — один из весов), числитель
	// L×maxW + sumW − 1 ≥ sumW, целочисленное деление даёт ≥ 1; ветка —
	// защитная, недостижима (включая вырезание тела branch/if — no-op
	// присваивание).
	// mutator-disable-next-line expression/comparison, numbers/decrementer, numbers/incrementer, branch/if
	if quota < 1 {
		// Эквивалентные мутанты (T-M05): тело недостижимой ветки (см. if
		// выше) — в том числе вырезание присваивания (statement/remove:
		// no-op `_ = quota`) и числовые сдвиги.
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer, statement/remove
		quota = 1
	}
	quota++

	// Раунды: сперва все зоны, далее — только «голодные» (вернули ровно
	// квоту: возможно, обрезаны LIMIT), keyset от последнего id зоны.
	var merged []dispatchPick
	pending := zones // зоны раунда
	// Эквивалентные мутанты (T-M05): счётчик round в теле не используется,
	// а тело выполняется не более 2 раз (см. dispatchMaxRounds), поэтому
	// `round <= …`, старт с −1 или 1 дают те же выполнения.
	// mutator-disable-next-line expression/comparison, numbers/decrementer, numbers/incrementer
	for round := 0; round < dispatchMaxRounds; round++ {
		ids := make([]any, len(pending))
		afters := make([]any, len(pending))
		for i, z := range pending {
			after := int64(0) // с головы очереди зоны
			// Эквивалентный мутант (T-M05): `n > 1` ≡ `n > 0` — в раунде 0
			// z.rows пуст, с раунда 1 pending — только «голодные» зоны,
			// у каждой — квота предыдущего раунда строк (раунд 0: ≥ 2,
			// см. quota++; в раундах дозаполнения строки только
			// накапливаются); длина 1 не встречается.
			// mutator-disable-next-line numbers/incrementer
			if n := len(z.rows); n > 0 {
				after = z.rows[n-1].domainID
			}
			ids[i] = z.id
			afters[i] = after
		}
		picked, err := dispatchPickZones(ctx, pool, ids, afters, quota)
		if err != nil {
			return nil, err
		}
		starving := make([]*dispatchZone, 0, len(pending))
		for _, z := range pending {
			n := len(picked[z.id])
			// Эквивалентные мутанты (T-M05): `n == -1` невыполним (len ≥ 0),
			// а провал continue для n == 0 безразличен: append пустого среза
			// — no-op, n == quota ложно (quota ≥ 1 > 0 = n); то же — вырезание
			// тела if (branch/if): no-op вместо continue.
			// Замена continue на break не эквивалентна — закрывается тестом
			// RefillStopsAtExhaustedZone.
			// mutator-disable-next-line numbers/decrementer, branch/if
			if n == 0 {
				continue
			}
			z.rows = append(z.rows, picked[z.id]...)
			if n == quota {
				starving = append(starving, z) // возможно, есть ещё
			}
		}
		merged = dispatchMerge(zones, weights, q.Limit)
		if len(merged) == q.Limit {
			break // пачка полная
		}
		if len(starving) == 0 {
			break // все зоны отдали всё pending — добирать нечего
		}
		// Кап квоты остатком пачки (T-183): вклад любой зоны в пачку не
		// превосходит остатка — слиянию достаточно remaining строк зоны,
		// чтобы заполнить пачку целиком; чтение сверх (раньше — Limit на
		// каждую голодную зону) в слияние попасть не может. Здесь
		// remaining ≥ 1: неполная пачка (break выше).
		quota = q.Limit - len(merged)
		pending = starving
	}

	names := make(map[int16]string, len(zones))
	for _, z := range zones {
		names[z.id] = z.name
	}
	batch := make([]DispatchRow, 0, len(merged))
	for _, m := range merged {
		batch = append(batch, DispatchRow{
			DomainID: m.domainID,
			ZoneID:   m.zoneID,
			Name:     m.label + "." + names[m.zoneID],
		})
	}
	return batch, nil
}

// dispatchActiveZones — активные зоны с работой (domains_pending > 0,
// T-183): id и имя (имя — суффикс полного имени домена, ТЗ 5.2). Зоны —
// малая таблица, чтение целиком дёшево; снимок согласован в пределах
// вызова (выключение зоны между запросами выборки оставит её домены в
// очереди раунда — их перевод в queued идёт вслед за выборкой,
// at-least-once, ТЗ 3.4; симметрично, исчерпание зоны конкурентным
// enqueue между запросами даёт одну «холостую» пробу — деградации нет).
func dispatchActiveZones(ctx context.Context, pool db.DB) ([]*dispatchZone, error) {
	rows, err := pool.Query(ctx, dispatchZonesSQL)
	if err != nil {
		return nil, fmt.Errorf("scheduler: выборка пачки доменов: активные зоны: %w", err)
	}
	defer rows.Close()

	zones := make([]*dispatchZone, 0, 16)
	for rows.Next() {
		var z dispatchZone
		if err := rows.Scan(&z.id, &z.name); err != nil {
			return nil, fmt.Errorf("scheduler: выборка пачки доменов: активные зоны: чтение: %w", err)
		}
		zones = append(zones, &z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: выборка пачки доменов: активные зоны: %w", err)
	}
	return zones, nil
}

// ResolveZoneWeights — источник весов зон из конфигурации (ТЗ 5.3:
// «веса зон — опционально в конфиге», T-106): карта «имя зоны
// (punycode, ключи scheduler.zone_weights) → вес» переводится в карту
// «zone_id → вес» (DispatchQuery.ZoneWeights) чтением справочника zones
// одним запросом — все зоны, не только активные (вес неактивной вступит
// в силу при её включении, crawlerctl zones enable, без рестарта).
// Карта вызывающего не изменяется. Значения уже проверены конфигурацией
// (вес 1..1000); повторная проверка — dispatchValidate на каждой выборке.
//
// Имена без строки в справочнике — не ошибка: зоны создаёт ingest при
// первом файле (ТЗ 5.1), планировщик может стартовать раньше; такие
// имена возвращаются отдельно (unknown, по возрастанию) для Warn
// вызывающего — до появления зоны и рестарта она проверяется с весом 1.
// Пустая карта или nil — nil без обращения к БД: равные веса,
// поведение по умолчанию.
func ResolveZoneWeights(ctx context.Context, pool db.DB, byName map[string]int) (weights map[int16]int, unknown []string, err error) {
	if len(byName) == 0 {
		return nil, nil, nil
	}
	rows, err := pool.Query(ctx, resolveZoneWeightsSQL)
	if err != nil {
		return nil, nil, fmt.Errorf("scheduler: веса зон: справочник зон: %w", err)
	}
	defer rows.Close()

	rest := make(map[string]int, len(byName)) // имена, ждущие строку справочника
	for name, w := range byName {
		rest[name] = w
	}
	weights = make(map[int16]int, len(byName))
	for rows.Next() {
		var id int16
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, nil, fmt.Errorf("scheduler: веса зон: справочник зон: чтение: %w", err)
		}
		if w, ok := rest[name]; ok {
			weights[id] = w
			delete(rest, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("scheduler: веса зон: справочник зон: %w", err)
	}
	// Эквивалентный мутант (T-M05): unknown = nil вместо пустого среза —
	// при rest без имён; вызывающий (cmd/scheduler) читает только len и
	// strings.Join под guard len > 0, пустое и nil неразличимы.
	// mutator-disable-next-line statement/remove
	unknown = make([]string, 0, len(rest))
	for name := range rest {
		unknown = append(unknown, name)
	}
	sort.Strings(unknown)
	return weights, unknown, nil
}

// dispatchPickZones — префиксы очередей зон (dispatchZoneSQL): по after
// (последний прочитанный id зоны, 0 — с головы) до limit строк каждой.
// Ответ группируется по зоне; внутри зоны сортируется по id — порядок
// строк SQL не определён, очередь обязана быть возрастающей.
func dispatchPickZones(ctx context.Context, pool db.DB, zoneIDs, afters []any, limit int) (map[int16][]dispatchPick, error) {
	rows, err := pool.Query(ctx, dispatchZoneSQL, zoneIDs, afters, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: выборка пачки доменов: %w", err)
	}
	defer rows.Close()

	picked := make(map[int16][]dispatchPick, len(zoneIDs))
	for rows.Next() {
		var p dispatchPick
		if err := rows.Scan(&p.zoneID, &p.domainID, &p.label); err != nil {
			return nil, fmt.Errorf("scheduler: выборка пачки доменов: чтение строки: %w", err)
		}
		picked[p.zoneID] = append(picked[p.zoneID], p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: выборка пачки доменов: %w", err)
	}
	for z := range picked {
		ps := picked[z]
		// Эквивалентный мутант (T-M05): `<=` отличается от `<` только на
		// равных domainID внутри зоны — их не бывает: id — часть PK domains
		// (миграция 000001), LATERAL-выборка строк не повторяет.
		// mutator-disable-next-line expression/comparison
		sort.Slice(ps, func(i, j int) bool { return ps[i].domainID < ps[j].domainID })
	}
	return picked, nil
}

// dispatchMerge — слияние очередей зон в круговой порядок пачки: пока не
// набрано limit строк, берётся голова зоны с минимальным ключом
// (rn − 0.5)/вес (rn — позиция в очереди зоны, 1-based), при равенстве —
// меньший zone_id. Повторяет порядок оконной выборки T-027 (глобальный
// ORDER BY (rn − 0.5)/вес, zone_id, rn) над прочитанными префиксами.
func dispatchMerge(zones []*dispatchZone, weights map[int16]int, limit int) []dispatchPick {
	type head struct {
		zone *dispatchZone
		rn   int     // позиция следующей строки очереди зоны
		key  float64 // (rn − 0.5)/вес
	}
	heads := make([]*head, 0, len(zones))
	for _, z := range zones {
		if len(z.rows) == 0 {
			continue
		}
		w := float64(weights[z.id])
		// Эквивалентные мутанты (T-M05): `w <= 1`/`w < 2` не меняют
		// поведение — значения карты ≥ 1 (валидация), отсутствующая зона
		// даёт 0 → 1, а w = 1 присваивается повторно (no-op). `w < 0`
		// (decrementer) отличим — ключ 0.5/0 = +Inf ломает порядок —
		// и не гасится.
		// mutator-disable-next-line expression/comparison, numbers/incrementer
		if w < 1 {
			w = 1
		}
		h := &head{zone: z, rn: 1, key: (1 - 0.5) / w}
		heads = append(heads, h)
	}

	out := make([]dispatchPick, 0, limit)
	for len(out) < limit && len(heads) > 0 {
		// Минимальный ключ; равные — меньший zone_id (детерминизм).
		best := 0
		// Эквивалентный мутант (T-M05): старт i = 0 сравнивает голову с
		// собой (ключи и zone.id равны) — выбор best не меняется.
		// mutator-disable-next-line numbers/decrementer
		for i := 1; i < len(heads); i++ {
			h, b := heads[i], heads[best]
			// Эквивалентный мутант (T-M05): `<=` на zone.id отличается от
			// `<` только при равных id — головы суть разные зоны, id
			// различны. Аннотация строки гасит и убиваемые мутанты других
			// сравнений строки (trade-off §4 docs/mutation-testing.md:
			// разнос по строкам — правка продуктового кода).
			// mutator-disable-next-line expression/comparison
			if h.key < b.key || (h.key == b.key && h.zone.id < b.zone.id) {
				best = i
			}
		}
		h := heads[best]
		out = append(out, h.zone.rows[h.rn-1])
		h.rn++
		if h.rn > len(h.zone.rows) {
			heads = append(heads[:best], heads[best+1:]...) // зона исчерпана
			continue
		}
		w := float64(weights[h.zone.id])
		// Эквивалентные мутанты (T-M05): `w <= 1`/`w < 2` — как в первом
		// нормализующем if выше; `w < 0` (decrementer) отличим (ключ +Inf)
		// и не гасится.
		// mutator-disable-next-line expression/comparison, numbers/incrementer
		if w < 1 {
			w = 1
		}
		h.key = (float64(h.rn) - 0.5) / w
	}
	return out
}

// dispatchValidate проверяет параметры до обращения к БД: невалидный
// запрос не тратит соединение.
func (q DispatchQuery) dispatchValidate() error {
	if q.Limit < 1 {
		return fmt.Errorf("scheduler: выборка пачки: Limit = %d, ожидается ≥ 1 (scheduler.batch_size, ТЗ 5.3)", q.Limit)
	}
	for zoneID, w := range q.ZoneWeights {
		if w < 1 {
			return fmt.Errorf("scheduler: выборка пачки: вес зоны %d = %d, ожидается ≥ 1", zoneID, w)
		}
	}
	return nil
}

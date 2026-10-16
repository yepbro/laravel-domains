// Package scheduler — планировщик проверок (ТЗ 5.3): выборка доменов
// check_state = pending пачками с круговым распределением между зонами
// (этот файл, T-027), перевод выбранных в queued с публикацией в q:dns
// (T-028), возврат доменов с истёкшим lease (T-029), плановые перепроверки
// done- и temp_error-доменов (T-030/T-031).
//
// Выборка пачки — один SQL, чередующий зоны (ТЗ 5.3: «по кругу между
// зонами, чтобы крупная зона не задерживала остальные»):
//
//   - каждая зона — очередь: внутри зоны домены упорядочены по id, ровно
//     по частичному индексу domains_pending_idx (zone_id, id) WHERE
//     check_state = 0 (миграция 000001) — нумерация очередей идёт по
//     готовому индексу, без дополнительных структур;
//   - row_number() PARTITION BY zone_id нумерует позиции в очередях зон,
//     а сортировка по (позиция, zone_id) отдаёт по одному домену из
//     каждой зоны за круг: за одной зоной в пачке не идут её соседние
//     строки, крупная зона не забивает пачку (что было бы при ORDER BY
//     zone_id);
//   - вес зоны (ТЗ 5.3: «веса зон — опционально») сдвигает позицию её
//     элемента к (rn − 0.5)/weight — зона с весом w занимает в общем
//     порядке долю, пропорциональную w (шаговое планирование); вес 1 у
//     всех вырождается в равный круговой порядок. Колонки weight в схеме
//     нет (ТЗ 6.1 её не требует), поэтому веса приходят параметрами
//     (unnest-массивы zone_id/weight, стиль пакетных операций
//     internal/db) — источник весов задаёт вызывающий; миграция не
//     вводится.
//
// Охрана d.status <> deleted: применение delete (T-019) переводит статус,
// не трогая check_state, — без охраны удалённые домены оставались бы
// кандидатами вечно, хотя их результаты воркеры не записывают (ТЗ §8);
// воскрешённый add-файлом домен имеет status = new и возвращается в
// выборку сам (T-018). JOIN по zones задерживает неактивные зоны
// (is_active = false, crawlerctl zones disable): их проверка приостановлена,
// домены остаются pending и вернутся в цикл при включении зоны.
//
// Память (ТЗ 3.5): строки читаются потоком, размер пачки ограничен
// Limit (scheduler.batch_size, ТЗ 5.3: 1000–10000).
package scheduler

import (
	"context"
	"fmt"
	"sort"

	"crawler/internal/db"
	"crawler/internal/model"
)

// dispatchSelectSQL — пачка pending-доменов в круговом порядке между
// зонами. Семантические константы (коды check_state/status) вшиты в текст
// через model.* — это коды схемы, а не данные (стиль internal/ingest);
// тест-контракт связывает литералы с константами. LIMIT/OFFSET — пачка и
// страница повторной выборки; name — полное имя в ASCII-форме (метка
// без зоны + punycode-имя зоны, ТЗ 5.2) — готовые данные сообщения q:dns
// (ТЗ 4.2: только идентификаторы и имя домена).
var dispatchSelectSQL = fmt.Sprintf(
	"SELECT t.domain_id, t.zone_id, t.label || '.' || t.zone_name AS name "+
		"FROM (SELECT d.id AS domain_id, d.zone_id, d.label, z.name AS zone_name, "+
		"row_number() OVER (PARTITION BY d.zone_id ORDER BY d.id) AS rn "+
		"FROM domains AS d "+
		"JOIN zones AS z ON z.id = d.zone_id AND z.is_active "+
		"WHERE d.check_state = %d AND d.status <> %d) AS t "+
		"LEFT JOIN unnest($1::int2[], $2::int4[]) AS w(zone_id, weight) ON w.zone_id = t.zone_id "+
		"ORDER BY (t.rn - 0.5) / COALESCE(w.weight, 1), t.zone_id, t.rn "+
		"LIMIT $3 OFFSET $4",
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
	// Offset — смещение в порядке выдачи для повторных выборок.
	//
	// Выборка не меняет состояние доменов (до T-028 строки остаются
	// pending), поэтому повторный вызов с тем же смещением отдал бы те же
	// строки; следующая пачка — Offset += Limit. Порядок выдачи
	// детерминирован ((rn − 0.5)/вес, zone_id, rn), и пока множество
	// pending не меняется, страницы не пересекаются. В рабочем цикле
	// после T-028 выбранные строки уходят в queued и покидают выборку —
	// Offset сбрасывается в 0, смещение нужно только повторным выборкам
	// без перевода состояния (например, публикация у порога очереди —
	// T-026). Побочный эффект конкурентных изменений множества pending
	// (возврат по lease T-029, плановые перепроверки T-030/T-031: домены
	// возвращаются с прежними, часто меньшими id и сдвигают rn хвоста
	// зоны) — на границе страниц возможен повтор отдельных строк
	// (безопасно: доставка at-least-once, ТЗ 3.4) или пропуск единичных
	// строк (подберутся следующим циклом планировщика). Keyset по domain_id
	// невозможен для глобального кругового порядка (он строится по позициям
	// сразу всех зон), а курсоры на зону — запрос на каждую зону; выбран
	// OFFSET по детерминированной сортировке.
	Offset int
	// ZoneWeights — веса зон: zone_id → вес ≥ 1. nil или пустая карта —
	// все веса 1 (равный круговой порядок). Колонки weight в схеме нет
	// (ТЗ 6.1) — веса аргумент выборки, а не данные БД.
	ZoneWeights map[int16]int
}

// DispatchBatch выбирает пачку доменов check_state = pending в круговом
// порядке между зонами (ТЗ 5.3). Пачка ограничена q.Limit, строки
// читаются потоком (ТЗ 3.5); пустой результат — пустой срез без ошибки.
// Состояние доменов не меняется: перевод в queued и публикацию выполняет
// следующий шаг цикла (T-028), поэтому повторные выборки адресуют
// следующие строки смещением q.Offset.
func DispatchBatch(ctx context.Context, pool db.DB, q DispatchQuery) ([]DispatchRow, error) {
	if err := q.dispatchValidate(); err != nil {
		return nil, err
	}
	zoneIDs, weights := q.dispatchWeightArrays()

	rows, err := pool.Query(ctx, dispatchSelectSQL, zoneIDs, weights, q.Limit, q.Offset)
	if err != nil {
		return nil, fmt.Errorf("scheduler: выборка пачки доменов: %w", err)
	}
	defer rows.Close()

	batch := make([]DispatchRow, 0, q.Limit)
	for rows.Next() {
		var r DispatchRow
		if err := rows.Scan(&r.DomainID, &r.ZoneID, &r.Name); err != nil {
			return nil, fmt.Errorf("scheduler: выборка пачки доменов: чтение строки: %w", err)
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: выборка пачки доменов: %w", err)
	}
	return batch, nil
}

// dispatchValidate проверяет параметры до обращения к БД: невалидный
// запрос не тратит соединение.
func (q DispatchQuery) dispatchValidate() error {
	if q.Limit < 1 {
		return fmt.Errorf("scheduler: выборка пачки: Limit = %d, ожидается ≥ 1 (scheduler.batch_size, ТЗ 5.3)", q.Limit)
	}
	if q.Offset < 0 {
		return fmt.Errorf("scheduler: выборка пачки: Offset = %d, ожидается ≥ 0", q.Offset)
	}
	for zoneID, w := range q.ZoneWeights {
		if w < 1 {
			return fmt.Errorf("scheduler: выборка пачки: вес зоны %d = %d, ожидается ≥ 1", zoneID, w)
		}
	}
	return nil
}

// dispatchWeightArrays — карта весов в параллельные массивы zone_id/weight
// для unnest: порядок детерминирован (по zone_id), длины совпадают.
func (q DispatchQuery) dispatchWeightArrays() ([]int16, []int32) {
	zoneIDs := make([]int16, 0, len(q.ZoneWeights))
	for id := range q.ZoneWeights {
		zoneIDs = append(zoneIDs, id)
	}
	sort.Slice(zoneIDs, func(i, j int) bool { return zoneIDs[i] < zoneIDs[j] })

	weights := make([]int32, 0, len(zoneIDs))
	for _, id := range zoneIDs {
		weights = append(weights, int32(q.ZoneWeights[id]))
	}
	return zoneIDs, weights
}

// Применение партии add к domains (T-018, ТЗ 5.1): строки партии в
// staging вставляются в domains пачками по ingest.batch_size (T-205)
// через INSERT … SELECT … ON CONFLICT DO NOTHING (конфликт — уникальный
// (zone_id, label) партиций domains); существующий домен не меняется,
// кроме статуса deleted — такой возвращается в проверку (status → new,
// check_state → pending, ТЗ 6.2). Пачки — курсором по PK staging
// (batch_id, line_no), всё применение — одна транзакция: атомарность
// эквивалентна одному оператору эпохи T-018, а время удержания
// блокировок и запись в журнал медленных запросов ограничены пачкой
// (боевой замер T-205: суточный NRD-файл ~470 тыс. доменов одним
// оператором шёл 63–200 с под вечерним CPU-голодом; пачка 50 тыс. —
// секунды). После применения партия удаляется из staging:
// применить → очистить.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
)

// ApplyDB — пул для применения add: db.DB, из которого можно начать
// транзакцию. Применение партии атомарно (T-205): пачки вставки, дельты
// счётчиков зон, возврат удалённых и очистка staging — одна транзакция,
// поэтому db.DB мало — нужен Begin; *pgxpool.Pool удовлетворяет
// (стиль CleanupDB).
type ApplyDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ ApplyDB = (*pgxpool.Pool)(nil)

// SQL применения add. Семантические константы (коды статусов, биты flags)
// вшиты в текст через model.*: это коды схемы, а не данные (стиль zones.go
// с is_active = true); тест-контракт связывает литералы с константами.
var (
	// addChunkBoundSQL — граница следующей пачки применения (T-205):
	// line_no N-й строки партии после курсора $3 в порядке PK staging
	// (batch_id, line_no, миграция 000004) — подзапрос режет диапазон
	// LIMIT $4, внешний max даёт line_no последней строки пачки (порядок
	// возрастающий). Пустой диапазон — COALESCE → 0: строк после курсора
	// нет, партия исчерпана. Курсор двигается по диапазону staging, а не
	// по вставленным строкам: пачка из одних конфликтов (ON CONFLICT DO
	// NOTHING пропустил всё) всё равно закрывается границей — иначе
	// применение партии из сплошных существующих доменов зациклилось бы.
	// Скан только по первичному ключу: ровно пачка строк, без чтения
	// данных таблицы.
	addChunkBoundSQL = "SELECT COALESCE(max(t.line_no), 0) FROM (" +
		"SELECT s.line_no FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 AND s.line_no > $3 " +
		"ORDER BY s.line_no LIMIT $4) AS t"

	// insertAddSQL — новые домены одной пачки партии (ТЗ 5.1; T-205):
	// строки staging в диапазоне курсора ($3, $4] вставляются за один
	// INSERT … SELECT, ON CONFLICT DO NOTHING пропускает существующие
	// (в том числе повторяющиеся метки внутри одного файла, миграция
	// 000004). Новые домены получают DEFAULT схемы: status = new,
	// check_state = pending (ТЗ 6.2) — домен сразу в цикле проверок.
	// is_idn вычисляется из метки (ТЗ 5.2 п. 9): staging флага не хранит,
	// а метка — его единственный источник. Условие — любая метка имени
	// начинается с xn-- (метка бывает составной, ING-P1-1): первая
	// ('xn--%') либо после точки ('%.xn--%') — эквивалент
	// idn.HasXNLabel, ILIKE — без учёта регистра.
	//
	// RETURNING zone_id вставленных строк читается потоком: счётчик
	// Inserted и дельты счётчиков зон (ТЗИ §6.2) копятся в Go и
	// применяются одним итоговым оператором по завершении пачек
	// (zoneDeltaAddSQL) — память ограничена пачкой и числом зон партии,
	// а не размером файла (ТЗ 3.5).
	insertAddSQL = "INSERT INTO domains (zone_id, label, flags) " +
		"SELECT s.zone_id, s.label, CASE WHEN s.label ILIKE 'xn--%' OR s.label ILIKE '%.xn--%' THEN " + fmt.Sprint(int16(model.FlagIDN)) + " ELSE 0 END " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 AND s.line_no > $3 AND s.line_no <= $4 " +
		"ON CONFLICT DO NOTHING RETURNING zone_id"

	// zoneDeltaAddSQL — итоговые дельты счётчиков зон вставки (T-205,
	// ТЗИ §6.2: новые домены попадают и в total, и в pending): unnest-пачка
	// пар zone_id→n, стиль внутренних операторов пакета (internal/db,
	// cleanup.go). Один оператор на партию вместо CTE в каждом INSERT:
	// счётчики зон трогаются один раз за применение — точечная запись
	// вместо шторма мелких UPDATE, атомарность общая с вставкой
	// (транзакция ApplyAdd). В SET колонки не квалифицируются алиасом —
	// требование PG (см. internal/db/update.go).
	zoneDeltaAddSQL = "UPDATE zones AS z " +
		"SET domains_total = z.domains_total + d.n, " +
		"domains_pending = z.domains_pending + d.n " +
		"FROM unnest($1::int2[], $2::int8[]) AS d(zone_id, n) " +
		"WHERE z.id = d.zone_id"

	// reviveAddSQL — возврат удалённых доменов партии в проверку (ТЗ 5.1:
	// существующий домен не меняется, «кроме случая статуса deleted — он
	// возвращается в new/pending»). Охрана d.status = deleted трогает
	// только удалённые строки: остальные существующие домены партия не
	// меняет вовсе. Один SQL вместо пакетного db.Update: ключи уже в БД,
	// выборка меток в память и обратная пересылка противоречат потоковости
	// (ТЗ 3.5). В SET колонки не квалифицируются алиасом — требование PG
	// (см. internal/db/update.go). queued_at сбрасывается: у pending-домена
	// нет действующего lease (ТЗ 5.3). flags, dns_mask, last_checked_at не
	// трогаются: признаки метки и история прошлых проверок сохраняются —
	// перепроверка перепишет их сама.
	//
	// Дельты счётчиков зон (total +n / pending +n) — CTE zc по RETURNING
	// zone_id возвращённых строк (T-178, ТЗИ §6.2: домен был вне базового
	// множества — счётчики его не содержали, декрементов нет).
	//
	// T-205: возврат оставлен одним оператором на всю партию, без
	// чанкинга — revive затрагивает только deleted-домены партии, у
	// NRD-файлов их почти нет (день регистрации моложе любых удалений),
	// масштаб мал и пачками не окупается.
	reviveAddSQL = "WITH rev AS (UPDATE domains AS d " +
		fmt.Sprintf("SET status = %d, check_state = %d, queued_at = NULL ", int16(model.StatusNew), int16(model.CheckStatePending)) +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 " +
		"AND d.zone_id = s.zone_id AND d.label = s.label " +
		fmt.Sprintf("AND d.status = %d RETURNING d.zone_id), ", int16(model.StatusDeleted)) +
		"zc AS (UPDATE zones AS z " +
		"SET domains_total = z.domains_total + c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM rev GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM rev"

	// countAddSQL — уникальные домены партии (по паре zone_id, label —
	// дубликаты строк файла не надувают счётчики) до очистки: из него
	// вычитается вставленное и возвращённое — остаток «уже было». Один
	// запрос на партию (T-205): DISTINCT по staging дешевле вставки и в
	// окно медленных не попадал.
	countAddSQL = "SELECT count(*) FROM (" +
		"SELECT DISTINCT s.zone_id, s.label FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2) AS t"
)

// AddResult — итог применения партии add: счётчики отчёта по файлу
// (ТЗ 5.1 — T-021). Домены считаются по уникальным (zone_id, label)
// партии: повторяющиеся строки файла дедуплицируются конфликтом
// (миграция 000004), каждая метка входит ровно в один счётчик.
type AddResult struct {
	// Inserted — добавлено: вставлено новых доменов (сумма вставок пачек,
	// T-205).
	Inserted int64
	// Existing — уже было: существующие домены не в статусе deleted —
	// партия их не изменила (ТЗ 5.1).
	Existing int64
	// Revived — возвращено из deleted: статус → new, check_state →
	// pending — домен снова в цикле проверок (ТЗ 5.1, 6.2).
	Revived int64
}

// Total — уникальных доменов партии: Inserted + Existing + Revived.
func (r AddResult) Total() int64 { return r.Inserted + r.Existing + r.Revived }

// ApplyAdd применяет партию add к domains (ТЗ 5.1) в ОДНОЙ транзакции
// (T-205) по шагам в фиксированном порядке, закреплённом тестами:
//
//  1. вставка новых доменов пачками по batchSize строк (ingest.batch_size,
//     как COPY в staging): курсор по PK staging (batch_id, line_no) —
//     граница пачки (addChunkBoundSQL), INSERT диапазона (insertAddSQL),
//     следующая пачка продолжает после последней строки предыдущей —
//     независимо от того, вставились ли её строки (конфликтная пачка
//     закрывается той же границей). zone_id и label — из строк staging:
//     зона создана EnsureZone (T-014) и несена каждой строкой партии;
//  2. дельты счётчиков зон (total +n / pending +n, ТЗИ §6.2) — один
//     итоговый UPDATE … FROM unnest по zone_id, накопленным из RETURNING
//     всех пачек;
//  3. UPDATE существующих строк партии со статусом deleted — возврат в
//     проверку (status → new, check_state → pending, ТЗ 6.2), один
//     оператор на партию;
//  4. подсчёт уникальных доменов партии до очистки: Existing = уникальные −
//     Inserted − Revived;
//  5. ClearBatch — партия удаляется из staging (применить → очистить).
//
// Транзакция делает применение атомарным эквивалентом одного оператора
// эпохи T-018: сбой любого шага откатывает всё применение целиком, строки
// партии остаются в staging, повторное применение выполняет те же шаги
// на тех же данных и сходится к тому же итогу (идемпотентность, ТЗ 3.4);
// зафиксированное применение очищает staging, и повторная подача того же
// списка (новая партия) упирается в конфликты — счётчики повторного
// прогона отражают текущее состояние, отчёт по файлу пишет первая
// успешная попытка (T-021). Повторное применение уже применённой
// (очищенной) партии — no-op с нулевым результатом.
func ApplyAdd(ctx context.Context, pool ApplyDB, batch Batch, batchSize int) (AddResult, error) {
	var res AddResult
	if batch.ID == (BatchID{}) {
		return res, errors.New("ingest: партия без идентификатора (ingest.NewBatchID)")
	}
	if batch.Action != model.ActionAdd {
		return res, fmt.Errorf("ingest: операция партии %s: применение add — только для партии add (ТЗ 5.1)", batch.Action)
	}
	if batchSize < 1 {
		return res, fmt.Errorf("ingest: размер пачки %d, минимум 1 (ingest.batch_size)", batchSize)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("ingest: применение партии add: транзакция: %w", err)
	}
	// Откат безусловно: после Commit — no-op. Прерванное применение не
	// оставляет половинчатых изменений (ТЗ 3.4).
	defer func() { _ = tx.Rollback(ctx) }()

	args := []any{batch.ID[:], int16(model.ActionAdd)}

	inserted, zones, err := applyAddInsert(ctx, tx, batch, batchSize)
	if err != nil {
		return res, err
	}
	res.Inserted = inserted

	if err := applyAddZoneDeltas(ctx, tx, zones); err != nil {
		return res, err
	}

	n, err := applyAddCount(ctx, tx, reviveAddSQL, args, "возврат удалённых доменов партии add")
	if err != nil {
		return res, err
	}
	res.Revived = n

	uniques, err := applyAddCount(ctx, tx, countAddSQL, args, "подсчёт доменов партии add")
	if err != nil {
		return res, err
	}
	if uniques < res.Inserted+res.Revived {
		return res, fmt.Errorf("ingest: применение партии add: уникальных доменов %d меньше вставленных и возвращённых %d — счётчики расходятся",
			uniques, res.Inserted+res.Revived)
	}
	res.Existing = uniques - res.Inserted - res.Revived

	// Применено — партия покидает staging той же транзакцией: очистка
	// после применения, а не до (иначе сбой применения терял бы строки
	// партии до повтора).
	if _, err := ClearBatch(ctx, tx, batch.ID); err != nil {
		return res, err
	}
	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("ingest: применение партии add: фиксация: %w", err)
	}
	return res, nil
}

// applyAddInsert вставляет новые домены партии пачками по batchSize строк
// (T-205) и возвращает счётчик вставленных и накопленные по zone_id дельты
// счётчиков зон (применяются одним оператором по завершении пачек).
// Курсор — line_no последней закрытой строки: пачка — диапазон
// (курсор, граница], строки партии нумеруются с единицы, начальный курсор
// 0 закрывает партию целиком. Память ограничена пачкой и числом зон
// партии, а не размером файла (ТЗ 3.5): RETURNING вставленных читается
// потоком, в Go копятся только счётчики.
func applyAddInsert(ctx context.Context, tx pgx.Tx, b Batch, batchSize int) (int64, map[int16]int64, error) {
	zones := make(map[int16]int64)
	var (
		inserted int64
		cursor   int64
	)
	for {
		rows, err := tx.Query(ctx, addChunkBoundSQL, b.ID[:], int16(model.ActionAdd), cursor, batchSize)
		if err != nil {
			return 0, nil, fmt.Errorf("ingest: граница пачки вставки новых доменов партии add: %w", err)
		}
		bound, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
		if err != nil {
			return 0, nil, fmt.Errorf("ingest: граница пачки вставки новых доменов партии add: %w", err)
		}
		if bound == 0 {
			return inserted, zones, nil // партия исчерпана
		}

		rows, err = tx.Query(ctx, insertAddSQL, b.ID[:], int16(model.ActionAdd), cursor, bound)
		if err != nil {
			return 0, nil, fmt.Errorf("ingest: вставка новых доменов партии add: %w", err)
		}
		for rows.Next() {
			var zone int16
			if err := rows.Scan(&zone); err != nil {
				rows.Close()
				return 0, nil, fmt.Errorf("ingest: вставка новых доменов партии add: %w", err)
			}
			inserted++
			zones[zone]++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("ingest: вставка новых доменов партии add: %w", err)
		}
		rows.Close()

		// Курсор закрывает пачку по staging, а не по вставленным строкам:
		// полностью конфликтная пачка тоже двигает курсор вперёд.
		cursor = bound
	}
}

// applyAddZoneDeltas применяет накопленные дельты счётчиков зон одним
// оператором (zoneDeltaAddSQL, T-205). Пустые дельты (партия без
// вставок) — no-op без обращения к БД. Порядок zone_id детерминирован
// (сортировка) — воспроизводимые пачки, как у cleanupRefcounts.
func applyAddZoneDeltas(ctx context.Context, tx pgx.Tx, zones map[int16]int64) error {
	if len(zones) == 0 {
		return nil
	}
	ids := make([]int16, 0, len(zones))
	for id := range zones {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ns := make([]int64, len(ids))
	for i, id := range ids {
		ns[i] = zones[id]
	}
	if _, err := tx.Exec(ctx, zoneDeltaAddSQL, ids, ns); err != nil {
		return fmt.Errorf("ingest: дельты счётчиков зон партии add: %w", err)
	}
	return nil
}

// applyAddCount выполняет CTE-оператор применения (возврат с дельтами
// счётчиков зон одним оператором, T-178) либо подсчёт и возвращает
// счётчик внешнего SELECT (число затронутых строк партии). Вызывается
// в транзакции ApplyAdd: pgx.Tx удовлетворяет db.DB.
func applyAddCount(ctx context.Context, pool db.DB, sql string, args []any, kind string) (int64, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("ingest: %s: %w", kind, err)
	}
	defer rows.Close()
	n, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		return 0, fmt.Errorf("ingest: %s: %w", kind, err)
	}
	return n, nil
}

// Применение партии add к domains (T-018, ТЗ 5.1): строки партии в
// staging одним INSERT … SELECT … ON CONFLICT DO NOTHING становятся
// доменами (конфликт — уникальный (zone_id, label) партиций domains);
// существующий домен не меняется, кроме статуса deleted — такой
// возвращается в проверку (status → new, check_state → pending, ТЗ 6.2).
// После применения партия удаляется из staging: применить → очистить.
package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
	"crawler/internal/model"
)

// SQL применения add. Семантические константы (коды статусов, биты flags)
// вшиты в текст через model.*: это коды схемы, а не данные (стиль zones.go
// с is_active = true); тест-контракт связывает литералы с константами.
var (
	// insertAddSQL — новые домены партии (ТЗ 5.1): вставляются только
	// отсутствующие — ON CONFLICT DO NOTHING пропускает существующие
	// (в том числе повторяющиеся метки внутри одного файла, миграция
	// 000004), счёт вставленных — внешний SELECT по CТЕ ins. Новые домены
	// получают DEFAULT схемы: status = new, check_state = pending (ТЗ 6.2)
	// — домен сразу в цикле проверок. is_idn вычисляется из метки (ТЗ 5.2
	// п. 9): staging флага не хранит, а метка — его единственный источник.
	// Условие — любая метка имени начинается с xn-- (метка бывает
	// составной, ING-P1-1): первая ('xn--%') либо после точки
	// ('%.xn--%') — эквивалент idn.HasXNLabel, ILIKE — без учёта
	// регистра.
	//
	// Дельты счётчиков зон (total +n / pending +n) — CTE zc того же
	// оператора по RETURNING zone_id вставленных строк (T-178, ТЗИ §6.2):
	// вставка и дельты атомарны, повторное применение партии (после сбоя
	// до очистки staging) конфликтует и дельт не добавляет.
	insertAddSQL = "WITH ins AS (INSERT INTO domains (zone_id, label, flags) " +
		"SELECT s.zone_id, s.label, CASE WHEN s.label ILIKE 'xn--%' OR s.label ILIKE '%.xn--%' THEN " + fmt.Sprint(int16(model.FlagIDN)) + " ELSE 0 END " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 " +
		"ON CONFLICT DO NOTHING RETURNING zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_total = z.domains_total + c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM ins GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM ins"

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
	// вычитается вставленное и возвращённое — остаток «уже было».
	countAddSQL = "SELECT count(*) FROM (" +
		"SELECT DISTINCT s.zone_id, s.label FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2) AS t"
)

// AddResult — итог применения партии add: счётчики отчёта по файлу
// (ТЗ 5.1 — T-021). Домены считаются по уникальным (zone_id, label)
// партии: повторяющиеся строки файла дедуплицируются конфликтом
// (миграция 000004), каждая метка входит ровно в один счётчик.
type AddResult struct {
	// Inserted — добавлено: вставлено новых доменов.
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

// ApplyAdd применяет партию add к domains (ТЗ 5.1) в четыре шага,
// фиксированный порядок закреплён тестами:
//
//  1. INSERT … SELECT из staging … ON CONFLICT DO NOTHING — новые домены
//     (zone_id и label — из строк staging: зона создана EnsureZone, T-014,
//     и несена каждой строкой партии);
//  2. UPDATE существующих строк партии со статусом deleted — возврат в
//     проверку (status → new, check_state → pending, ТЗ 6.2);
//  3. подсчёт уникальных доменов партии до очистки: Existing = уникальные −
//     Inserted − Revived;
//  4. ClearBatch — партия удаляется из staging (применить → очистить).
//
// Сбой любого шага возвращает ошибку без очистки: строки партии остаются
// в staging, повторное применение безопасно (идемпотентность, ТЗ 3.4) —
// вставка конфликтует, возврат уже выполнен, данные сходятся к тому же
// итогу; счётчики повторного прогона отражают текущее состояние (добавлять
// нечего), отчёт по файлу пишет первая успешная попытка (T-021).
// Повторное применение уже применённой (очищенной) партии — no-op с
// нулевым результатом.
func ApplyAdd(ctx context.Context, pool db.DB, batch Batch) (AddResult, error) {
	var res AddResult
	if batch.ID == (BatchID{}) {
		return res, errors.New("ingest: партия без идентификатора (ingest.NewBatchID)")
	}
	if batch.Action != model.ActionAdd {
		return res, fmt.Errorf("ingest: операция партии %s: применение add — только для партии add (ТЗ 5.1)", batch.Action)
	}

	args := []any{batch.ID[:], int16(model.ActionAdd)}

	n, err := applyAddCount(ctx, pool, insertAddSQL, args, "вставка новых доменов партии add")
	if err != nil {
		return res, err
	}
	res.Inserted = n

	n, err = applyAddCount(ctx, pool, reviveAddSQL, args, "возврат удалённых доменов партии add")
	if err != nil {
		return res, err
	}
	res.Revived = n

	rows, err := pool.Query(ctx, countAddSQL, args...)
	if err != nil {
		return res, fmt.Errorf("ingest: подсчёт доменов партии add: %w", err)
	}
	uniques, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		return res, fmt.Errorf("ingest: подсчёт доменов партии add: %w", err)
	}
	if uniques < res.Inserted+res.Revived {
		return res, fmt.Errorf("ingest: применение партии add: уникальных доменов %d меньше вставленных и возвращённых %d — счётчики расходятся",
			uniques, res.Inserted+res.Revived)
	}
	res.Existing = uniques - res.Inserted - res.Revived

	if _, err := ClearBatch(ctx, pool, batch.ID); err != nil {
		return res, err
	}
	return res, nil
}

// applyAddCount выполняет CTE-оператор применения (вставка/возврат +
// дельты счётчиков зон одним оператором, T-178) и возвращает счётчик
// внешнего SELECT (число затронутых строк партии).
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

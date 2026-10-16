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
	// 000004), RowsAffected = число вставленных. Новые домены получают
	// DEFAULT схемы: status = new, check_state = pending (ТЗ 6.2) — домен
	// сразу в цикле проверок. is_idn вычисляется из метки (ТЗ 5.2 п. 9):
	// staging флага не хранит, а метка — его единственный источник
	// (ILIKE — префикс xn-- без учёта регистра, как idn.HasXNLabel).
	insertAddSQL = "INSERT INTO domains (zone_id, label, flags) " +
		"SELECT s.zone_id, s.label, CASE WHEN s.label ILIKE 'xn--%' THEN " + fmt.Sprint(int16(model.FlagIDN)) + " ELSE 0 END " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 " +
		"ON CONFLICT DO NOTHING"

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
	reviveAddSQL = fmt.Sprintf(
		"UPDATE domains AS d "+
			"SET status = %d, check_state = %d, queued_at = NULL "+
			"FROM ingest_stage AS s "+
			"WHERE s.batch_id = $1 AND s.action = $2 "+
			"AND d.zone_id = s.zone_id AND d.label = s.label "+
			"AND d.status = %d",
		int16(model.StatusNew), int16(model.CheckStatePending), int16(model.StatusDeleted))

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

	tag, err := pool.Exec(ctx, insertAddSQL, args...)
	if err != nil {
		return res, fmt.Errorf("ingest: вставка новых доменов партии add: %w", err)
	}
	res.Inserted = tag.RowsAffected()

	tag, err = pool.Exec(ctx, reviveAddSQL, args...)
	if err != nil {
		return res, fmt.Errorf("ingest: возврат удалённых доменов партии add: %w", err)
	}
	res.Revived = tag.RowsAffected()

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

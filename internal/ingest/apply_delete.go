// Применение delete (T-019, ТЗ 5.1): партия staging со строками
// action=delete переводит статус подходящих доменов в deleted. Домен
// остаётся в БД (ТЗ 13.1: «удалённый домен остаётся в БД со статусом
// deleted; результаты и контент удаляются фоновой задачей») — их чистит
// фоновая задача T-020, здесь ничего кроме статуса не трогается.
package ingest

import (
	"context"
	"errors"
	"fmt"

	"crawler/internal/db"
	"crawler/internal/model"
)

// applyDeleteSQL — применение delete одним оператором (ТЗ 5.1):
//
//   - CTE applied — перенос статуса: UPDATE domains SET status = deleted
//     для доменов партии (соединение по уникальному ключу (zone_id, label);
//     фильтры batch_id/action адресуют только свою партию и только её
//     delete-строки). Охраны по текущему статусу нет: delete переводит в
//     deleted любой статус, уже удалённый домен остаётся удалённым —
//     повторное применение не меняет состояние (идемпотентность
//     at-least-once, ТЗ 3.4). RETURNING возвращает ключи переведённых
//     доменов — по ним считаются строки отчёта;
//   - внешний SELECT без FROM всегда возвращает одну строку счётчиков:
//     deleted — строки файла, чей домен переведён (повторяющаяся метка в
//     файле считает каждую свою строку), not_found — строки, чьего домена
//     нет в domains. Все обращения к staging — через StageTable: единый
//     источник таблицы с COPY и очисткой партии.
//
// Оба множества (staging и domains) уже находятся в БД, поэтому один
// set-based оператор вместо db.Update с unnest-массивами: ничего не
// возится через приложение, память не зависит от размера файла (ТЗ 3.5),
// а счётчики считаются тем же оператором и не расходятся с применением.
var applyDeleteSQL = `WITH applied AS (
	UPDATE domains AS d
	SET status = $2
	FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1
	  AND s.action = $3
	  AND d.zone_id = s.zone_id
	  AND d.label = s.label
	RETURNING d.zone_id, d.label
)
SELECT
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	JOIN applied AS a USING (zone_id, label)
	WHERE s.batch_id = $1 AND s.action = $3) AS deleted,
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND NOT EXISTS (SELECT 1 FROM domains AS d
	                  WHERE d.zone_id = s.zone_id AND d.label = s.label)) AS not_found`

// DeleteResult — счётчики применения delete по файлу для отчёта (ТЗ 5.1:
// «удалено»; пишет в ingest_files задача T-021). Считаются строки файла:
// каждая строка delete-файла либо перевела свой домен в deleted, либо
// домена в БД нет.
type DeleteResult struct {
	// Deleted — строки, чьи домены переведены в статус deleted. Домен,
	// уже находившийся в deleted, тоже считается удалённым: итог строки
	// «домен удалён» достигнут, отдельного счётчика «уже удалён» нет —
	// ТЗ 5.1/13.1 различают только итоговый статус, а не путь к нему.
	Deleted int64
	// NotFound — строки, чьего домена нет в domains: удалять нечего.
	NotFound int64
}

// ApplyDelete применяет партию action=delete из staging (Stage, T-017):
// переводит статус подходящих доменов в model.StatusDeleted и возвращает
// счётчики строк файла (DeleteResult). Домены остаются в БД (ТЗ 13.1);
// результаты и ссылки на контент удаляются фоновой задачей (T-020).
//
// Применение — один оператор applyDeleteSQL: пустая партия — no-op с
// нулевыми счётчиками. После применения партия очищается из staging
// (ClearBatch, stage.go) — staging не растёт от файла к файлу. Очистка —
// отдельный оператор, не в одной транзакции с применением: сбой между
// ними безвреден — повторное применение переведёт те же домены в тот же
// deleted (no-op по состоянию) и доочистит партию (ТЗ 3.4). При ошибке
// очистки счётчики возвращаются вместе с ошибкой: применение уже
// состоялось, отчёт по нему валиден.
func ApplyDelete(ctx context.Context, pool db.DB, b Batch) (DeleteResult, error) {
	if b.ID == (BatchID{}) {
		return DeleteResult{}, errors.New("ingest: партия без идентификатора (ingest.NewBatchID)")
	}
	if b.Action != model.ActionDelete {
		return DeleteResult{}, fmt.Errorf("ingest: операция партии %s: применение delete ожидает партию delete (ТЗ 5.1)", b.Action)
	}

	res, err := applyDeleteCounters(ctx, pool, b)
	if err != nil {
		return DeleteResult{}, err
	}

	// Применено — партия покидает staging: очистка после применения, а не
	// до (иначе сбой применения терял бы строки партии до повтора).
	if _, err := ClearBatch(ctx, pool, b.ID); err != nil {
		return res, err
	}
	return res, nil
}

// applyDeleteCounters выполняет applyDeleteSQL и разбирает строку
// счетчиков (одна — форма запроса без FROM).
func applyDeleteCounters(ctx context.Context, pool db.DB, b Batch) (DeleteResult, error) {
	rows, err := pool.Query(ctx, applyDeleteSQL, b.ID[:], int16(model.StatusDeleted), int16(model.ActionDelete))
	if err != nil {
		return DeleteResult{}, fmt.Errorf("ingest: применение delete по партии в %s: %w", StageTable, err)
	}
	defer rows.Close()

	var res DeleteResult
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return DeleteResult{}, fmt.Errorf("ingest: применение delete по партии в %s: %w", StageTable, err)
		}
		return DeleteResult{}, fmt.Errorf("ingest: применение delete по партии в %s: счётчики не возвращены", StageTable)
	}
	if err := rows.Scan(&res.Deleted, &res.NotFound); err != nil {
		return DeleteResult{}, fmt.Errorf("ingest: применение delete по партии в %s: %w", StageTable, err)
	}
	return res, nil
}

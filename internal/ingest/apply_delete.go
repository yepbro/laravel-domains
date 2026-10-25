// Применение delete (T-019, ТЗ 5.1): партия staging со строками
// action=delete переводит статус подходящих доменов в deleted. Домен
// остаётся в БД (ТЗ 13.1: «удалённый домен остаётся в БД со статусом
// deleted; результаты и контент удаляются фоновой задачей») — их чистит
// фоновая задача T-020. Вместе со статусом снимается бит render_pending
// (T-186): удаляемый домен покидает цикл проверок, проход доработки
// рендера его больше не выберет — бит снимает перевод в deleted той же
// транзакцией с декрементом счётчика рендер-долга (applyDeleteSQL).
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
//   - CTE pre — снимок кандидатов перевода ДО изменения (тот же снимок
//     оператора, что и у moved): staging×domains по ключу (zone_id,
//     label) с фильтрами партии и охраной «ещё не deleted» (T-178).
//     Единственная цель — старое значение бита render_pending (had_render
//     по снимку): RETURNING обновления не видит старых значений колонок
//     из SET (PG16 без RETURNING OLD), а считать переходы бита обязан
//     тот же оператор, что их выполняет. MATERIALIZED обязателен:
//     одиночная ссылка позволяет планировщику инлинить pre в moved
//     (замер на живой БД) — семантика had_render «старый бит» держится
//     на привязке выражения к СКАНИРУЕМОМУ экземпляру таблицы, а не к
//     целевому; материализация закрепляет её от формы плана (в духе
//     закреплений T-181), цена — пачка размера файла;
//   - CTE moved — перенос статуса: UPDATE domains SET status = deleted
//     для доменов партии — соединение с pre по уникальному ключу
//     (zone_id, label). Охраны по текущему статусу до T-178 не было:
//     повторное применение переводило уже удалённый в тот же deleted
//     (идемпотентность at-least-once, ТЗ 3.4) — отчёт считал строку
//     «удалённой». Охрана d.status <> deleted теперь нужна для дельт
//     счётчиков зон (T-178, ТЗИ §6.2): повторное применение не должно
//     декрементировать счётчики за уже удалённый домен; отчёт это
//     сохраняет — см. внешний SELECT. Охрана повторена в moved (не только
//     в pre): при конкуренции за строку (второй delete-файл по тому же
//     домену) повторная проверка условия после ожидания блокировки
//     отсекает уже переведённую строку от дельт. Второе назначение
//     обновления — снятие бита render_pending (T-186, бит 1 flags):
//     удаляемый домен покидает цикл проверок (§13.1), проход доработки
//     рендера его больше не выбирает (renderq исключает deleted), а бит
//     без владельца держал бы счётчик рендер-долга навсегда — guard
//     T-181 не спал и гонял порожний seq scan каждый тик. CASE читает
//     ТЕКУЩУЮ версию строки (не had_render из снимка): конкурирующий
//     переход бита между снимком и обновлением не оставит рассогласования
//     «бит снят/поставлен, счётчик не сдвинут» худе, чем ±1 до
//     реконсиляции (она чинит, ТЗИ §7). Снятие обязано жить в этом же
//     UPDATE: второй data-modifying CTE по той же строке не работает —
//     PostgreSQL применяет к строке только одну модификацию оператора
//     (проверено на живой БД), а отдельный оператор ломал бы атомарность
//     пары «статус + бит»;
//   - CTE zc — дельты счётчиков зон по RETURNING zone_id, check_state
//     переведённых строк (T-178): total −n и по состоянию —
//     pending −n / queued −n / checked −n (оператор не меняет
//     check_state — RETURNING отдаёт старое значение, ТЗИ §6.1.2);
//     базовое множество колонок зон — status <> deleted (ТЗИ §4.1),
//     удаляемый домен покидает все колонки;
//   - CTE rd — декремент счётчика рендер-долга
//     crawler_counters['render_pending'] на число снимочных битов
//     переведённых строк (T-186, инвариант T-175 «счётчик = число строк
//     с битом, без фильтра статуса»): upsert в стиле реконсиляции
//     (zonestats.go); HAVING > 0 не трогает строку счётчика (единая
//     точка сериализации флажащих транзакций, record.go) файлами без
//     флаговых доменов. CTE стоит после zc — порядок блокировок
//     «domains → zones → счётчик» общий с писателями переходов
//     (record.go: зоны → render_pending) и исключает циклы. Редкий
//     рассинхрон «сняли бит, а счётчик уже сдвинули» (конкурирующий
//     переход бита в окне снимок→обновление) — ±1 до ближайшей
//     реконсиляции (ТЗИ §7, gauge crawler_counters_drift);
//   - внешний SELECT без FROM всегда возвращает одну строку счётчиков:
//     deleted — строки файла, чей домен переведён этим оператором
//     (EXISTS по moved) ЛИБО уже был удалён до него (EXISTS по domains
//     со старым статусом — снимок оператора не видит собственной записи
//     moved): семантика счётчика прежняя (повторяющаяся метка в файле
//     считает каждую свою строку, повторное применение считает свои
//     строки удалёнными), not_found — строки, чьего домена нет
//     в domains. Все обращения к staging — через StageTable: единый
//     источник таблицы с COPY и очисткой партии.
//
// Оба множества (staging и domains) уже находятся в БД, поэтому один
// set-based оператор вместо db.Update с unnest-массивами: ничего не
// возится через приложение, память не зависит от размера файла (ТЗ 3.5),
// а счётчики считаются тем же оператором и не расходятся с применением.
var applyDeleteSQL = `WITH pre AS MATERIALIZED (
	SELECT d.zone_id, d.label, (d.flags & ` + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + `) <> 0 AS had_render
	FROM ` + StageTable.Sanitize() + ` AS s
	JOIN domains AS d ON d.zone_id = s.zone_id AND d.label = s.label
	WHERE s.batch_id = $1
	  AND s.action = $3
	  AND d.status <> $2
), moved AS (
	UPDATE domains AS d
	SET status = $2,
	    flags = CASE WHEN (d.flags & ` + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + `) <> 0
	                 THEN (d.flags & ` + fmt.Sprintf("%d", int16(^int16(model.FlagRenderPending))) + `)::int2
	                 ELSE d.flags END
	FROM pre AS p
	WHERE d.zone_id = p.zone_id
	  AND d.label = p.label
	  AND d.status <> $2
	RETURNING d.zone_id, d.label, d.check_state, p.had_render
), zc AS (
	UPDATE zones AS z
	SET domains_total = z.domains_total - c.n_total,
	    domains_pending = z.domains_pending - c.n_pending,
	    domains_queued = z.domains_queued - c.n_queued,
	    domains_checked = z.domains_checked - c.n_checked
	FROM (SELECT zone_id,
	             count(*) AS n_total,
	             count(*) FILTER (WHERE check_state = ` + fmt.Sprint(int16(model.CheckStatePending)) + `) AS n_pending,
	             count(*) FILTER (WHERE check_state = ` + fmt.Sprint(int16(model.CheckStateQueued)) + `) AS n_queued,
	             count(*) FILTER (WHERE check_state = ` + fmt.Sprint(int16(model.CheckStateDone)) + `) AS n_checked
	      FROM moved GROUP BY zone_id) AS c
	WHERE z.id = c.zone_id
), rd AS (
	INSERT INTO crawler_counters (name, value)
	SELECT 'render_pending', -count(*) FROM moved WHERE had_render
	HAVING count(*) > 0
	ON CONFLICT (name) DO UPDATE
	SET value = crawler_counters.value + EXCLUDED.value
)
SELECT
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND (EXISTS (SELECT 1 FROM moved AS m
	               WHERE m.zone_id = s.zone_id AND m.label = s.label)
	       OR EXISTS (SELECT 1 FROM domains AS d2
	                  WHERE d2.zone_id = s.zone_id AND d2.label = s.label
	                    AND d2.status = $2))) AS deleted,
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
// переводит статус подходящих доменов в model.StatusDeleted (снятием
// бита render_pending с декрементом счётчика рендер-долга той же
// транзакцией, T-186) и возвращает счётчики строк файла (DeleteResult).
// Домены остаются в БД (ТЗ 13.1); результаты и ссылки на контент
// удаляются фоновой задачей (T-020).
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

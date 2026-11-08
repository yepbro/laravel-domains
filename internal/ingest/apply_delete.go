// Применение delete (T-019, ТЗ 5.1): партия staging со строками
// action=delete переводит статус подходящих доменов в deleted. Домен
// остаётся в БД (ТЗ 13.1: «удалённый домен остаётся в БД со статусом
// deleted; результаты и контент удаляются фоновой задачей») — их чистит
// фоновая задача T-020. Вместе со статусом снимается бит render_pending
// (T-186): удаляемый домен покидает цикл проверок, проход доработки
// рендера его больше не выберет — бит снимается парой с декрементом
// счётчика рендер-долга двумя операторами одной транзакции применения
// (applyDeleteSQL + applyClearRenderSQL, T-269: дельта — по факту
// перехода, не по снимку).
package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"crawler/internal/model"
)

// applyDeleteSQL — применение delete: первый оператор транзакции
// ApplyDelete (ТЗ 5.1). В терминах автомата check_state — строка Р14
// спецификации (единая точка правды: internal/scheduler/checkstate.go,
// T-255): статус-переход в deleted, check_state НЕ меняется — строка
// выпадает из всех проходов охранными предикатами status <> deleted.
// Бит render_pending и счётчик рендер-долга этот оператор НЕ трогает
// (T-269, ранее — CASE в moved и CTE rd): пару «бит × счётчик» закрывает
// второй оператор applyClearRenderSQL той же транзакции — см. его
// комментарий (инцидент 08.11.2026: декремент по снимку had_render
// применялся ВТОРОЙ раз поверх конкурентного снятия проходом доработки,
// −36 за один проход). Детали оператора:
//
//   - CTE pre — снимок кандидатов перевода ДО изменения (тот же снимок
//     оператора, что и у moved): staging×domains по ключу (zone_id,
//     label) с фильтрами партии и охраной «ещё не deleted» (T-178).
//     MATERIALIZED стабилизирует множество кандидатов от инлайна
//     (форма плана, замер T-178 на живой БД), цена — пачка размера
//     файла; старое значение бита render_pending здесь больше не
//     нужно — снятие бита ушло в applyClearRenderSQL с охраной по
//     текущей версии строки;
//   - CTE moved — перенос статуса: UPDATE domains SET status = deleted
//     для доменов партии — соединение с pre по уникальному ключу
//     (zone_id, label); flags НЕ трогает. Охраны по текущему статусу
//     до T-178 не было: повторное применение переводило уже удалённый
//     в тот же deleted (идемпотентность at-least-once, ТЗ 3.4) — отчёт
//     считал строку «удалённой». Охрана d.status <> deleted теперь
//     нужна для дельт счётчиков зон (T-178, ТЗИ §6.2): повторное
//     применение не должно декрементировать счётчики за уже удалённый
//     домен; отчёт это сохраняет — см. внешний SELECT. Охрана
//     повторена в moved (не только в pre): при конкуренции за строку
//     (второй delete-файл по тому же домену) повторная проверка
//     условия после ожидания блокировки отсекает уже переведённую
//     строку от дельт. RETURNING несёт had_render — бит render_pending
//     версии строки, С КОТОРОЙ работает обновление: для нетронутых
//     конкурентами строк это снимок оператора, для перезахваченных
//     после ожидания блокировки (EvalPlanQual) — ТЕКУЩАЯ зафиксированная
//     версия. Колонки вне SET RETURNING отдаёт по версии-основанию —
//     это и есть «бит на момент перевода», тот факт перехода, по
//     которому второй оператор ведёт счётчик (PG16 без RETURNING OLD
//     не видит старых значений колонок ИЗ SET — потому бит здесь и не
//     снимается);
//   - CTE zc — дельты счётчиков зон по RETURNING zone_id, check_state
//     переведённых строк (T-178): total −n и по состоянию —
//     pending −n / queued −n / checked −n (оператор не меняет
//     check_state — RETURNING отдаёт старое значение, ТЗИ §6.1.2);
//     базовое множество колонок зон — status <> deleted (ТЗИ §4.1),
//     удаляемый домен покидает все колонки;
//   - внешний SELECT без FROM всегда возвращает одну строку: счётчики
//     deleted (строки файла, чей домен переведён этим оператором —
//     EXISTS по moved — ЛИБО уже был удалён до него — EXISTS по domains
//     со старым статусом: снимок оператора не видит собственной записи
//     moved; семантика счётчика прежняя — повторяющаяся метка в файле
//     считает каждую свою строку, повторное применение считает свои
//     строки удалёнными) и not_found (строки, чьего домена нет
//     в domains), а также ключи битовых строк render_zones/render_labels
//     — параллельные массивы (zone_id, label) переведённых строк с
//     had_render: адресаты второго оператора. Все обращения к staging —
//     через StageTable: единый источник таблицы с COPY и очисткой
//     партии.
//
// Оба множества (staging и domains) уже находятся в БД, поэтому один
// set-based оператор вместо db.Update с unnest-массивами: ничего не
// возится через приложение, память не зависит от размера файла (ТЗ 3.5,
// битовые массивы — только флаговые строки перевода, обычно малы), а
// счётчики считаются тем же оператором и не расходятся с применением.
var applyDeleteSQL = `WITH pre AS MATERIALIZED (
	SELECT d.zone_id, d.label
	FROM ` + StageTable.Sanitize() + ` AS s
	JOIN domains AS d ON d.zone_id = s.zone_id AND d.label = s.label
	WHERE s.batch_id = $1
	  AND s.action = $3
	  AND d.status <> $2
), moved AS (
	UPDATE domains AS d
	SET status = $2
	FROM pre AS p
	WHERE d.zone_id = p.zone_id
	  AND d.label = p.label
	  AND d.status <> $2
	RETURNING d.zone_id, d.label, d.check_state, (d.flags & ` + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + `) <> 0 AS had_render
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
	                  WHERE d.zone_id = s.zone_id AND d.label = s.label)) AS not_found,
	(SELECT coalesce(array_agg(m.zone_id ORDER BY m.zone_id, m.label), '{}'::int2[]) FROM moved AS m WHERE m.had_render) AS render_zones,
	(SELECT coalesce(array_agg(m.label ORDER BY m.zone_id, m.label), '{}'::text[]) FROM moved AS m WHERE m.had_render) AS render_labels`

// applyClearRenderSQL — второй оператор транзакции ApplyDelete: пара
// «снятие бита render_pending × декремент счётчика рендер-долга» (T-186
// владеет переходом, T-269 — форма; переход Д6 спецификации автомата
// бита × счётчика — internal/httpworker/renderq.go у renderSetPendingSQL,
// T-258). Удаляемый домен покидает цикл проверок (§13.1), проход
// доработки рендера его больше не выбирает (renderq исключает deleted),
// а бит без владельца держал бы счётчик рендер-долга навсегда — guard
// T-181 не спал и гонял порожний seq scan каждый тик.
//
// Ключи — параллельные массивы applyDeleteSQL (render_zones/render_labels:
// переведённые строки, чей бит стоял на момент перевода), снятие — по
// ОХРАНЕ бита текущей версии строки `(flags & render_pending) <> 0`, дельта
// счётчика — по ФАКТУ снятых строк (count из RETURNING): ровно семантика
// пары runbooks §6 и операторов httpworker (И1 автомата: RowsAffected 0 —
// перехода не было, счётчик не трогается). Охрана закрывает окно
// «снимок → блокировка», на котором ломался единый оператор T-186
// (инцидент 08.11.2026, prod-reports/2026-11-08-010800.md): декремент
// тогда считался по СНАПШОЧНОМУ had_render из pre, а CASE снятия бита
// читал ТЕКУЩУЮ версию — проход доработки, снявший биты между снимком и
// блокировкой, уже вёл −1 каждый, rd добавлял второе −n по снимку: −36 за
// один проход при единственном наборе снятий −18, счётчик −18 при нулевом
// бите. Теперь: если конкурент (проход доработки, второй экземпляр
// применения) снял бит раньше — охрана даёт 0 строк, счётчик не трогается
// (переход уже учтён конкурентом, Д7); если бит стоит — снятие и −1
// выполняются здесь одной транзакцией применения. Идемпотентность
// повторного применения партии (ТЗ 3.4) сохранена: переведённых строк нет
// — массивы пусты, оператор не выполняется вовсе (Go-сторона), повторных
// дельт нет.
//
// Порядок блокировок «domains → zones → счётчик» инварианта
// docs/schema.md сохранён: cleared пере захватывает ТОЛЬКО строки,
// уже заблокированные moved этой же транзакции (массивы — подмножество
// переведённых), новых рёбер «zones → domains» нет; строка счётчика —
// последней, как у всех писателей переходов. Условие
// `(SELECT count(*) FROM cleared) > 0` не трогает строку счётчика
// файлами без флаговых доменов (единая точка сериализации флажащих
// транзакций, record.go; прецедент HAVING T-186). RETURNING value —
// контроль строки: пустой ответ = строки 'render_pending' нет (миграция
// 000015; пропасть может только ручная правка, runbooks §6) — ошибка
// громче молчаливого рассинхрона (философия renderBumpDebtCounter,
// T-175). Стиль литералов — коды схемы через model.* (как applyDeleteSQL).
var applyClearRenderSQL = `WITH cleared AS (
	UPDATE domains AS d
	SET flags = (d.flags & ` + fmt.Sprintf("%d", int16(^int16(model.FlagRenderPending))) + `)::int2
	FROM unnest($1::int2[], $2::text[]) AS k(zone_id, label)
	WHERE d.zone_id = k.zone_id
	  AND d.label = k.label
	  AND (d.flags & ` + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + `) <> 0
	RETURNING 1
)
UPDATE crawler_counters
SET value = value - (SELECT count(*) FROM cleared)
WHERE name = 'render_pending' AND (SELECT count(*) FROM cleared) > 0
RETURNING value`

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
// переводит статус подходящих доменов в model.StatusDeleted и снимает бит
// render_pending с декрементом счётчика рендер-долга — двумя операторами
// ОДНОЙ транзакции (T-186 — переход, T-269 — форма пары: дельта по факту
// перехода, а не по снимку; инцидент 08.11.2026 — комментарий
// applyClearRenderSQL) — и возвращает счётчики строк файла (DeleteResult).
// Домены остаются в БД (ТЗ 13.1); результаты и ссылки на контент
// удаляются фоновой задачей (T-020).
//
// Применение — транзакция applyDeleteSQL + applyClearRenderSQL: пустая
// партия — no-op с нулевыми счётчиками; сбой любого шага откатывает
// перевод и пару целиком — повторная доставка файла сойдётся к тому же
// состоянию (ТЗ 3.4). После применения партия очищается из staging
// (ClearBatch, stage.go) — staging не растёт от файла к файлу. Очистка —
// отдельный оператор вне транзакции применения: сбой между ними безвреден
// — повторное применение переведёт те же домены в тот же deleted
// (no-op по состоянию, битовые массивы пусты) и доочистит партию (ТЗ
// 3.4). При ошибке очистки счётчики возвращаются вместе с ошибкой:
// применение уже состоялось, отчёт по нему валиден.
func ApplyDelete(ctx context.Context, pool ApplyDB, b Batch) (DeleteResult, error) {
	if b.ID == (BatchID{}) {
		return DeleteResult{}, errors.New("ingest: партия без идентификатора (ingest.NewBatchID)")
	}
	if b.Action != model.ActionDelete {
		return DeleteResult{}, fmt.Errorf("ingest: операция партии %s: применение delete ожидает партию delete (ТЗ 5.1)", b.Action)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("ingest: применение delete по партии в %s: транзакция: %w", StageTable, err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	res, zones, labels, err := applyDeleteCounters(ctx, tx, b)
	if err != nil {
		return DeleteResult{}, err
	}
	if len(zones) > 0 {
		if err := applyDeleteClearRender(ctx, tx, zones, labels); err != nil {
			return DeleteResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return DeleteResult{}, fmt.Errorf("ingest: применение delete по партии в %s: фиксация: %w", StageTable, err)
	}

	// Применено — партия покидает staging: очистка после применения, а не
	// до (иначе сбой применения терял бы строки партии до повтора).
	if _, err := ClearBatch(ctx, pool, b.ID); err != nil {
		return res, err
	}
	return res, nil
}

// applyDeleteCounters выполняет applyDeleteSQL и разбирает строку
// результата (одна — форма запроса без FROM): счётчики строк файла и
// ключи битовых строк (render_zones/render_labels — параллельные массивы
// переведённых строк с битом render_pending) для второго оператора.
func applyDeleteCounters(ctx context.Context, tx pgx.Tx, b Batch) (DeleteResult, []int16, []string, error) {
	rows, err := tx.Query(ctx, applyDeleteSQL, b.ID[:], int16(model.StatusDeleted), int16(model.ActionDelete))
	if err != nil {
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: применение delete по партии в %s: %w", StageTable, err)
	}
	defer rows.Close()

	var res DeleteResult
	var zones []int16
	var labels []string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return DeleteResult{}, nil, nil, fmt.Errorf("ingest: применение delete по партии в %s: %w", StageTable, err)
		}
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: применение delete по партии в %s: счётчики не возвращены", StageTable)
	}
	if err := rows.Scan(&res.Deleted, &res.NotFound, &zones, &labels); err != nil {
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: применение delete по партии в %s: %w", StageTable, err)
	}
	if err := rows.Err(); err != nil {
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: применение delete по партии в %s: %w", StageTable, err)
	}
	if len(zones) != len(labels) {
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: применение delete по партии в %s: битовые массивы разошлись (%d зон, %d меток)", StageTable, len(zones), len(labels))
	}
	return res, zones, labels, nil
}

// applyDeleteClearRender выполняет applyClearRenderSQL — пару «снятие
// бита × декремент счётчика» по ключам битовых строк перевода (T-186/
// T-269; комментарий к SQL). Пустой ответ RETURNING — строки счётчика
// 'render_pending' нет: молчаливый рассинхрон недопустим, ошибка громче
// (философия renderBumpDebtCounter, runbooks §6).
func applyDeleteClearRender(ctx context.Context, tx pgx.Tx, zones []int16, labels []string) error {
	rows, err := tx.Query(ctx, applyClearRenderSQL, zones, labels)
	if err != nil {
		return fmt.Errorf("ingest: применение delete: снятие бита render_pending: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("ingest: применение delete: снятие бита render_pending: %w", err)
		}
		return fmt.Errorf("ingest: применение delete: снятие бита render_pending: строка счётчика 'render_pending' отсутствует (миграция 000015, runbooks §6)")
	}
	var discarded int64
	if err := rows.Scan(&discarded); err != nil {
		return fmt.Errorf("ingest: применение delete: снятие бита render_pending: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ingest: применение delete: снятие бита render_pending: %w", err)
	}
	return nil
}

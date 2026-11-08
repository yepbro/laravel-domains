// Интеграционные тесты автомата бита render_pending × счётчика долга
// (T-258, ТЗ tasks/TZ-fsm.md §3 ФТ-4) против живых PostgreSQL/Redis:
// сценарная цепочка всех переходов спецификации (internal/httpworker/
// renderq.go, таблица у renderSetPendingSQL — единая точка правды)
// реальными писателями на одном домене: Д1 постановка долга записью
// результата при полной очереди (+1), Д2 повторная доставка с уже
// стоящим битом (охрана: второй +1 запрещён), Д8 свежий эпизод при
// свободной очереди (бит не стирается, пара не трогается), Д3 проход
// доработки «опубликован → снят» (−1) и зеркальная охрана снятия Д7
// (второй −1 запрещён), Д4 черновой флаг (снятие без публикации, −1),
// Д5 keep нечитаемого контента (пара сохранена), Д6 снятие delete-файлом
// (−1, T-186). Парность бита и счётчика (инвариант И2) после каждого
// шага сверяет реконсиляция monitor (VerifyCounters — счётчик против
// живого числа строк с битом); переживание «прочих» битов (is_idn)
// проверяется на каждом переходе (стиль httpbit_test.go, T-257).
// Переполнение q:render детерминировано: watermark 1 и одно живое
// сообщение в стриме (групп нет — глубина XLEN); сброс глубины между
// шагами — DEL стрима (стиль render_late_test.go). Ветка рассинхрона
// «счётчик > 0, выборка пуста» (диагностика битов-сирот) уже покрыта
// TestRenderRequeuePass («повторный проход не выбирает снятое»); сами
// сироты-deleted — особое состояние спецификации, тестами ФТ-4 не
// требуется. Уровень вызовов — библиотечные, без запуска сервисов;
// изоляция — withTestSchema + Redis БД 15.
package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/ingest"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
)

// renderBitStaticBody — статическая страница (не-SPA: текст обилен,
// контейнера и noscript нет) — вердикт DetectSPA ложен, рендер не
// требуется (T-057).
var renderBitStaticBody = []byte(`<html><body>plain static page with plenty of visible text</body></html>`)

// renderBitDebt — счётчик рендер-долга crawler_counters['render_pending'].
func renderBitDebt(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'").Scan(&n); err != nil {
		t.Fatalf("счётчик рендер-долга: %v", err)
	}
	return n
}

// renderBitFailContent — RenderContent с транзиентным сбоем чтения
// (сценарий keep, Д5): любой Get — ошибка хранения, не отмена ctx —
// renderDomainKeys сохраняет флаг до следующего тика.
type renderBitFailContent struct{ err error }

func (f renderBitFailContent) Get(context.Context, content.Hash) ([]byte, error) {
	return nil, f.err
}

// TestRenderPendingBit_SpecTransitions — цепочка переходов спецификации
// на одном домене: Д0 стационар → Д1 долг (+1) → Д2 охрана постановки →
// Д8 свежий эпизод без перехода → Д3 «опубликован → снят» (−1) + Д7
// охрана снятия → Д4 черновой флаг (−1 без публикации) → Д5 keep → Д6
// снятие delete-файлом (−1). Значение счётчика ходит 0 → 1 → 0 ровно
// шагами ±1 переходов, повторные доставки/снятия его не двигают.
func TestRenderPendingBit_SpecTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// q:render: публикация реальная (XADD); групп у стрима нет — глубина
	// XLEN, сброс глубины между шагами — DEL (эмуляция потребления).
	// Уборку стримов и закрытие клиента делает resilienceRedisEnv
	// (wipeQueueStreams со свежим контекстом; тестовый ctx к моменту
	// t.Cleanup уже отменён defer cancel — ревью T-258 P1-1).
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// watermark 1: пустой стрим — свободен, одно живое сообщение — уже
	// переполнение («строго ниже порога», ТЗ 4.2).
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := httpworker.NewRenderRecorder(pool, q, renderQ, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	zone := countersSeedZone(t, pool, "fsm-render.test")
	// countersSeedDomains нумерует метки: prefix "spec" → label "spec0".
	ids := countersSeedDomains(t, pool, zone, "spec", 1, model.CheckStateQueued, model.StatusNew)
	id := ids[0]
	const name = "spec0.fsm-render.test"

	// «Прочий бит» is_idn (без связей со счётчиком): спецификация Д1–Д8
	// требует его сохранения — OR/AND ~бита не трогают чужие разряды.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET flags = flags | $1 WHERE zone_id = $2 AND id = $3",
		int16(model.FlagIDN), zone, id); err != nil {
		t.Fatalf("сеем is_idn: %v", err)
	}
	// Ручная вставка мимо инкрементальных дельт — реконсиляция авторитет
	// (точка отсчёта счётчиков зон, стиль httpbit_test.go).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	debt := func() int64 { return renderBitDebt(ctx, t, pool) }
	flags := func() int16 { return httpBitFlags(ctx, t, pool, zone, id) }
	renderLen := func() int64 { return streamLen(t, rdb, queue.StreamRender) }
	wantFlags := func(want int16, what string) {
		t.Helper()
		if f := flags(); f != want {
			t.Fatalf("%s: flags = %b, хочу %b", what, f, want)
		}
	}
	full := func() {
		t.Helper()
		if free, err := renderQ.HasCapacity(ctx); err != nil || free {
			t.Fatalf("HasCapacity = %v err=%v, хочу false (q:render полон, глубина %d)", free, err, renderLen())
		}
	}
	drain := func() {
		t.Helper()
		if err := rdb.Del(ctx, queue.StreamRender).Err(); err != nil {
			t.Fatalf("сброс глубины q:render: %v", err)
		}
	}

	// episode — HTTP-эпизод реальной цепочкой записи: ранний Put контента
	// (T-147, как ContentSaver T-056) → RecordAndPublish (T-058/T-066).
	// Вердикт SPA — DetectSPA над теми же байтами (детерминирован, T-057).
	episode := func(body []byte) {
		t.Helper()
		put, err := store.Put(ctx, body)
		if err != nil {
			t.Fatalf("сохранение контента: %v", err)
		}
		h := put.Hash
		v := httpfetch.DetectSPA(body)
		res := &httpworker.DomainResult{
			ZoneID: zone, DomainID: id, Name: name,
			Subdomains: []httpworker.SubdomainResult{{
				SubdomainID: 0,
				Records: []httpworker.Record{{
					Variant:     0,
					Scheme:      model.SchemeHTTPS,
					FinalURL:    "https://" + name + "/",
					HTTPStatus:  200,
					ContentHash: &h,
					SPA:         &v,
				}},
			}},
		}
		if ok, err := rec.RecordAndPublish(ctx, res); err != nil || !ok {
			t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
		}
	}

	// armFull — заполнить q:render до порога (одно живое сообщение).
	armFull := func() {
		t.Helper()
		if renderLen() == 0 {
			if _, err := q.Publish(ctx, queue.StreamRender, queue.Result{
				ZoneID: zone, DomainID: id, SubdomainID: 0, Name: name,
			}); err != nil {
				t.Fatalf("заполнение q:render: %v", err)
			}
		}
		full()
	}

	const debtFlags = int16(model.FlagIDN | model.FlagRenderPending)
	const cleanFlags = int16(model.FlagIDN)

	// Д0 «без-долга»: стационар — бит чист, счётчик 0; guard T-181 не
	// выполняет тяжёлую выборку (пустой итог без ошибки).
	if debt() != 0 {
		t.Fatalf("Д0: счётчик долга = %d, хочу 0", debt())
	}
	if res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{}); err != nil || res.Selected != 0 {
		t.Fatalf("Д0: RequeueOnce = %+v err=%v, хочу selected=0 (стационар)", res, err)
	}

	// Д1: запись результата при полной очереди — бит и счётчик +1 той
	// же транзакцией, публикация q:render не выполняется, эпизод
	// завершается без задержки (done/alive).
	armFull()
	episode(rqBody)
	wantFlags(debtFlags, "после Д1")
	if debt() != 1 {
		t.Fatalf("Д1: счётчик = %d, хочу 1 (постановка +1)", debt())
	}
	if n := renderLen(); n != 1 {
		t.Fatalf("Д1: q:render = %d, хочу 1 (только заполнение — публикация не выполнялась)", n)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) || r.status != int16(model.StatusAlive) {
		t.Fatalf("после Д1 = %+v, хочу done/alive (флаг не задерживает эпизод, §5.6)", r)
	}
	countersVerifyEmpty(t, pool, "после Д1 (постановка +1)")

	// Д2: повторная доставка того же эпизода — очередь всё ещё полна,
	// бит уже стоит: охрана постановки даёт RowsAffected 0, второй +1
	// ЗАПРЕЩЁН (парность не двоится); строки перезаписаны идемпотентно.
	full()
	episode(rqBody)
	wantFlags(debtFlags, "после Д2")
	if debt() != 1 {
		t.Fatalf("Д2: счётчик = %d, хочу 1 (охрана бита: второй +1 запрещён)", debt())
	}
	if n := renderLen(); n != 1 {
		t.Fatalf("Д2: q:render = %d, хочу 1 (публикации по-прежнему нет)", n)
	}
	countersVerifyEmpty(t, pool, "после Д2 (повторная доставка — дельты нет)")

	// Д8: свежий эпизод при свободной очереди — запись результата бит
	// НЕ стирает и счётчик не трогает: задачи публикуются самим эпизодом
	// (дубль ограничен), держателем остаётся проход доработки.
	drain()
	episode(rqBody)
	wantFlags(debtFlags, "после Д8")
	if debt() != 1 {
		t.Fatalf("Д8: счётчик = %d, хочу 1 (новый эпизод пару не трогает)", debt())
	}
	if n := renderLen(); n != 1 {
		t.Fatalf("Д8: q:render = %d, хочу 1 (свежий эпизод публикует сам)", n)
	}
	countersVerifyEmpty(t, pool, "после Д8 (публикация без перехода)")

	// Д3: проход доработки — «опубликован → снят»: переиздание SPA-задачи
	// в q:render, затем снятие бита со счётчиком −1 одной транзакцией.
	drain()
	res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
	if err != nil {
		t.Fatalf("Д3: RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("Д3: итог %+v, хочу selected=1 cleared=1 published=1", res)
	}
	if n := renderLen(); n != 1 {
		t.Fatalf("Д3: q:render = %d, хочу 1 (переиздание SPA-задачи)", n)
	}
	wantFlags(cleanFlags, "после Д3")
	if debt() != 0 {
		t.Fatalf("Д3: счётчик = %d, хочу 0 (снятие −1)", debt())
	}
	countersVerifyEmpty(t, pool, "после Д3 (снятие −1)")

	// Д7 (зеркальная охрана снятия): конкурент уже снял бит — повторный
	// CLEAR по охране даёт RowsAffected 0, второй −1 ЗАПРЕЩЁН (сырой SQL
	// счётчик не трогает по определению — весь смысл в RowsAffected = 0:
	// ровно его и проверяем; оператор прохода renderClearPendingTx на
	// этом нуле пропускает декремент — юнит-тест TestRequeueOnceClearRace).
	// SQL — литерал renderClearPendingSQL (дублирование — стиль
	// schema_test.go, константа не экспортирована; рассинхрон литерала
	// с кодом ловит TestSchema_RenderDebtCounter — контракт охраны).
	const clearSQL = "UPDATE domains AS t SET flags = (t.flags & -3)::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND (t.flags & 2) <> 0"
	tag, err := pool.Exec(ctx, clearSQL, zone, id)
	if err != nil {
		t.Fatalf("Д7: повторное снятие: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatalf("Д7: повторное снятие: RowsAffected = %d, хочу 0 (охрана бита)", tag.RowsAffected())
	}
	if debt() != 0 {
		t.Fatalf("Д7: счётчик = %d, хочу 0 (второй −1 запрещён)", debt())
	}
	countersVerifyEmpty(t, pool, "после Д7 (охрана снятия)")

	// Д4 «черновой флаг»: долг перевооружен (Д1 — очередь держит
	// сообщение, опубликованное проходом Д3), затем страница перестала
	// быть SPA (свежий эпизод со статическим телом при свободной
	// очереди — рендерить нечего, бит пережил запись, Д8) — проход
	// снимает флаг БЕЗ публикации, счётчик −1.
	full()
	episode(rqBody)
	wantFlags(debtFlags, "после перевооружения Д4")
	if debt() != 1 {
		t.Fatalf("перевооружение Д4: счётчик = %d, хочу 1", debt())
	}
	drain()
	episode(renderBitStaticBody)
	wantFlags(debtFlags, "после не-SPA эпизода (бит пережил запись)")
	if debt() != 1 {
		t.Fatalf("не-SPA эпизод: счётчик = %d, хочу 1 (пара не тронута)", debt())
	}
	if n := renderLen(); n != 0 {
		t.Fatalf("не-SPA эпизод: q:render = %d, хочу 0 (не-SPA не публикуется)", n)
	}
	res, err = httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
	if err != nil {
		t.Fatalf("Д4: RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 1 || res.Published != 0 {
		t.Fatalf("Д4: итог %+v, хочу selected=1 cleared=1 published=0 (черновой флаг)", res)
	}
	wantFlags(cleanFlags, "после Д4")
	if debt() != 0 {
		t.Fatalf("Д4: счётчик = %d, хочу 0 (черновое снятие −1)", debt())
	}
	countersVerifyEmpty(t, pool, "после Д4 (черновое снятие −1)")

	// Д5 keep: контент записей нечитаем (транзиентный сбой хранилища) —
	// проход сохраняет пару «бит × счётчик» до следующего тика; тик с
	// читаемым хранилищем закрывает долг обычным Д3.
	armFull()
	episode(rqBody)
	wantFlags(debtFlags, "после перевооружения Д5")
	if debt() != 1 {
		t.Fatalf("перевооружение Д5: счётчик = %d, хочу 1", debt())
	}
	drain()
	res, err = httpworker.RequeueOnce(ctx, pool, renderBitFailContent{
		err: errors.New("транзиентный сбой хранилища (сценарий)"),
	}, renderQ, httpworker.RequeueOptions{})
	if err != nil {
		t.Fatalf("Д5: RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 0 || res.Published != 0 {
		t.Fatalf("Д5: итог %+v, хочу selected=1 cleared=0 published=0 (keep)", res)
	}
	wantFlags(debtFlags, "после Д5")
	if debt() != 1 {
		t.Fatalf("Д5: счётчик = %d, хочу 1 (пара сохранена)", debt())
	}
	countersVerifyEmpty(t, pool, "после Д5 (keep — без дельты)")

	res, err = httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
	if err != nil {
		t.Fatalf("закрытие Д5: RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("закрытие Д5: итог %+v, хочу selected=1 cleared=1 published=1", res)
	}
	if debt() != 0 {
		t.Fatalf("закрытие Д5: счётчик = %d, хочу 0", debt())
	}
	countersVerifyEmpty(t, pool, "после закрытия Д5 (снятие −1)")

	// Д6: delete-файл снимает бит той же транзакцией перевода статуса и
	// декрементирует счётчик (T-186): удаляемый домен выборку прохода
	// больше не пройдёт, бит без владельца не держит долг; is_idn
	// сохранён; повторное применение партии (at-least-once) не двоит −1
	// (охрана статуса; полная партия — TestCounters_DeleteWithRenderPending).
	full() // очередь держит сообщение, опубликованное закрытием Д5
	episode(rqBody)
	wantFlags(debtFlags, "после перевооружения Д6")
	if debt() != 1 {
		t.Fatalf("перевооружение Д6: счётчик = %d, хочу 1", debt())
	}
	batch := mustBatchID(t)
	if _, err := pool.Exec(ctx,
		"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, 1)",
		batch[:], zone, "spec0", int16(model.ActionDelete)); err != nil {
		t.Fatalf("staging delete: %v", err)
	}
	dres, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch, ZoneID: zone, Action: model.ActionDelete})
	if err != nil || dres.Deleted != 1 || dres.NotFound != 0 {
		t.Fatalf("Д6: ApplyDelete = %+v err=%v, хочу {1 0}", dres, err)
	}
	wantFlags(cleanFlags, "после Д6")
	if debt() != 0 {
		t.Fatalf("Д6: счётчик = %d, хочу 0 (декремент удаления −1)", debt())
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.status != int16(model.StatusDeleted) {
		t.Fatalf("после Д6: status = %d, хочу deleted", r.status)
	}
	countersVerifyEmpty(t, pool, "после Д6 (снятие delete-файлом −1)")
}

// --- инцидент 08.11.2026 (T-269) --------------------------------------------
//
// Расследование рассинхрона «−36 за один проход» из prod-reports/
// 2026-11-08-010800.md: снятие 18 флагов проходом доработки дало −18,
// ожидаемый 0, а следующий тик охраны прочитал уже −18 — «немой»
// (updated_at строки счётчика не двигался) второй −18 поверх
// единственного наблюдаемого набора снятий. Подозрение оператора —
// реконсиляция monitor (T-178) с «устаревшим ожиданием». Итог аудита
// всех писателей crawler_counters['render_pending']:
//
//   - httpworker (renderq.go) — дельты ±1 в паре с битом, охраны
//     RowsAffected (Д1–Д8, тесты выше) — двойной −1 невозможен;
//   - monitor (zonestats.go, recountCountersSQL) — АБСОЛЮТНЫЙ upsert
//     значением агрегата (value = EXCLUDED.value), дельты не пишет;
//     каждая перезапись двигает updated_at — «немого» следа оставить не
//     может. Реальная гонка иная: значение вычислено снапшотом раннего
//     оператора прохода (агрегат), а применяется поздним (upsert) —
//     конкурентные переходы в окне затираются устаревшим абсолютом
//     (ложный долг/ложный ноль), транзиентно, самолечится следующим
//     проходом — воспроизведена тестом опровержения ниже;
//   - ingest (apply_delete.go, T-186 до фикса) — ЕДИНСТВЕННЫЙ
//     «немой» дельта-писатель со снимком: CTE rd декрементировал
//     −count(had_render) по СНАПШОКУ pre, а снятие бита делал CASE по
//     ТЕКУЩЕЙ версии строки. Проход доработки, снявший биты между
//     снимком и блокировкой, уже вёл −1 каждый — rd добавлял второе −n
//     по снимку: ровно −36 инцидента при битах 0 и счётчике −18.
//
// Фикс T-269: применение delete — двумя операторами одной транзакции
// (перевод статуса; охраняемая пара «снятие бита × −count(факта)» по
// ключам битовых строк), окно снимка закрыто охраной текущей версии
// (зеркально Д7). Ниже — репродукция сценария 08.11 (фаза 1: оператор
// T-186 дословно, ДО фикса — обязана дать −18; фаза 2: тот же
// интерливинг с фиксом — инвариант |счётчик − count(бит)| = 0 на каждом
// шаге) и опровержение гипотезы реконсиляции.

// renderBitLegacyDeleteSQL — применение delete ДО T-269 (оператор T-186
// дословно, литералы раскрыты): снимочный had_render из pre кормит CTE
// rd, снятие бита — CASE текущей версии в moved. Контракт
// воспроизводимости инцидента: рассинхрон проявляется только у ЭТОЙ
// формы оператора — расхождение литерала с историей кода заметит ревью
// текста, живой оператор в internal/ingest/apply_delete.go её больше не
// содержит.
const renderBitLegacyDeleteSQL = `WITH pre AS MATERIALIZED (
	SELECT d.zone_id, d.label, (d.flags & 2) <> 0 AS had_render
	FROM ingest_stage AS s
	JOIN domains AS d ON d.zone_id = s.zone_id AND d.label = s.label
	WHERE s.batch_id = $1
	  AND s.action = $3
	  AND d.status <> $2
), moved AS (
	UPDATE domains AS d
	SET status = $2,
	    flags = CASE WHEN (d.flags & 2) <> 0
	                 THEN (d.flags & -3)::int2
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
	             count(*) FILTER (WHERE check_state = 0) AS n_pending,
	             count(*) FILTER (WHERE check_state = 1) AS n_queued,
	             count(*) FILTER (WHERE check_state = 2) AS n_checked
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
	(SELECT count(*) FROM ingest_stage AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND (EXISTS (SELECT 1 FROM moved AS m
	               WHERE m.zone_id = s.zone_id AND m.label = s.label)
	       OR EXISTS (SELECT 1 FROM domains AS d2
	                  WHERE d2.zone_id = s.zone_id AND d2.label = s.label
	                    AND d2.status = $2))) AS deleted,
	(SELECT count(*) FROM ingest_stage AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND NOT EXISTS (SELECT 1 FROM domains AS d
	                  WHERE d.zone_id = s.zone_id AND d.label = s.label)) AS not_found`

// renderBitClearPairSQL — «проход доработки» сценария: снятие бита парой
// со счётчиком по образцу runbooks §6 (охрана текущей версии, дельта по
// факту снятых) пачкой по ключам. В воспроизведении играет роль прохода
// T-066 из транзакции-блокиратора: реальный RequeueOnce лочил бы строку
// счётчика мимо управляемого порядка интерливинга, а семантика переходов
// Д3 (публикация сделана заранее — seeding контента не относится к
// гонке счётчика) здесь та же: бит → 0 и −1 за каждый факт.
const renderBitClearPairSQL = `WITH cleared AS (
	UPDATE domains SET flags = (flags & -3)::int2
	WHERE zone_id = $1 AND id = ANY($2) AND (flags & 2) <> 0
	RETURNING 1
)
UPDATE crawler_counters
SET value = value - (SELECT count(*) FROM cleared)
WHERE name = 'render_pending' AND (SELECT count(*) FROM cleared) > 0`

// renderBitSeedDebt — состояние «11:48:04» инцидента: n живых доменов с
// битом render_pending и счётчиком n (ручная §6-установка атомарно со
// счётчиком), зоны/статусы выровнены реконсиляцией (точка отсчёта
// сверки). Возвращает метки по порядку id.
func renderBitSeedDebt(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, prefix string, n int) []string {
	t.Helper()
	ids := countersSeedDomains(t, pool, zone, prefix, n, model.CheckStateDone, model.StatusAlive)
	for _, id := range ids {
		if _, err := pool.Exec(ctx,
			"UPDATE domains SET flags = flags | $1 WHERE zone_id = $2 AND id = $3",
			int16(model.FlagRenderPending), zone, id); err != nil {
			t.Fatalf("бит домена %d: %v", id, err)
		}
	}
	labels := make([]string, len(ids))
	for i := range ids {
		labels[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) VALUES ('render_pending', $1) "+
			"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value", int64(n)); err != nil {
		t.Fatalf("счётчик долга: %v", err)
	}
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция-точка отсчёта: %v", err)
	}
	return labels
}

// renderBitStageDelete — партия delete по меткам в staging.
func renderBitStageDelete(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, labels []string) ingest.BatchID {
	t.Helper()
	batch := mustBatchID(t)
	for i, label := range labels {
		if _, err := pool.Exec(ctx,
			"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, $5)",
			batch[:], zone, label, int16(model.ActionDelete), int16(i+1)); err != nil {
			t.Fatalf("staging %s: %v", label, err)
		}
	}
	return batch
}

// renderBitBitCount — число строк с битом по ключам (факт парности).
func renderBitBitCount(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, ids []int64) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM domains WHERE zone_id = $1 AND id = ANY($2) AND (flags & $3) <> 0",
		zone, ids, int16(model.FlagRenderPending)).Scan(&n); err != nil {
		t.Fatalf("подсчёт битов: %v", err)
	}
	return n
}

// renderBitWaitBlocked — ждёт, пока горутина-конкурент заблокируется
// именно на транзакции-блокираторе xid (pg_locks: ждущий transactionid
// с целью xid; цель задаёт ожидание ПОДРОБНО — глобальный предикат
// «есть хоть какой-то ждущий» ловил бы чужие пакеты параллельного
// test-integration, ревью T-269 P2-2) — точку детерминированного
// интерливинга. Ошибка горутины до блокировки всплывает сразу.
func renderBitWaitBlocked(t *testing.T, pool *pgxpool.Pool, errc chan error, xid, what string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case err := <-errc:
			t.Fatalf("%s: конкурент упал до блокировки: %v", what, err)
		default:
		}
		var n int
		err := pool.QueryRow(context.Background(),
			"SELECT count(*) FROM pg_locks WHERE locktype = 'transactionid' AND granted = false AND transactionid = $1::xid",
			xid).Scan(&n)
		if err == nil && n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: конкурент не заблокировался за 15 с (pg_locks: err=%v n=%d)", what, err, n)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// renderBitXID — идентификатор транзакции блокиратора (txid_current
// назначает xid, если запись его ещё не назначила) — цель ожидания.
func renderBitXID(ctx context.Context, t *testing.T, tx pgx.Tx) string {
	t.Helper()
	var xid string
	if err := tx.QueryRow(ctx, "SELECT txid_current()::text").Scan(&xid); err != nil {
		t.Fatalf("txid_current: %v", err)
	}
	return xid
}

// renderBitInTxDebt — счётчик долга из открытой транзакции прохода
// (собственные записи видимы, конкурент ещё заблокирован — чтение без
// гонки с его фиксацией, ревью T-269 P2-1).
func renderBitInTxDebt(ctx context.Context, t *testing.T, tx pgx.Tx) int64 {
	t.Helper()
	var n int64
	if err := tx.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'").Scan(&n); err != nil {
		t.Fatalf("счётчик долга в транзакции: %v", err)
	}
	return n
}

// renderBitInTxBits — число строк с битом по ключам из открытой
// транзакции прохода (см. renderBitInTxDebt).
func renderBitInTxBits(ctx context.Context, t *testing.T, tx pgx.Tx, zone int16, ids []int64) int64 {
	t.Helper()
	var n int64
	if err := tx.QueryRow(ctx,
		"SELECT count(*) FROM domains WHERE zone_id = $1 AND id = ANY($2) AND (flags & $3) <> 0",
		zone, ids, int16(model.FlagRenderPending)).Scan(&n); err != nil {
		t.Fatalf("подсчёт битов в транзакции: %v", err)
	}
	return n
}

// renderBitJoinErr — результат горутины-конкурента (одна ошибка).
func renderBitJoinErr(t *testing.T, errc chan error, what string) {
	t.Helper()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: не завершился за 30 с", what)
	}
}

// TestRenderPendingBit_DeleteClearRace — репродукция сценария 08.11.2026:
// параллельно снятие флагов проходом доработки × применение delete-файла
// по тем же доменам. Фаза 1 (оператор T-186 до фикса, дословно):
// проход снимает биты между снимком pre и блокировкой moved — rd
// декрементирует −18 по снимку ВТОРЫМ, счётчик уходит в −18 при нулевом
// бите (суммарный сдвиг −36 за один «проход», updated_at счётчика
// немой — дельта-писатель его не пишет). Фаза 2 (после фикса T-269):
// тот же интерливинг — перевод статуса не трогает бит, пара «снятие ×
// декремент» идёт по факту перехода с охраной текущей версии, инвариант
// |счётчик − count(бит)| = 0 держится на каждом шаге. Фаза 2b —
// зеркальный порядок (применение выиграло гонку): пара выполняется
// применением, парность та же.
func TestRenderPendingBit_DeleteClearRace(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const n = 18 // масштаб инцидента: 18 доменов / 18 флагов / −36 сдвиг
	zone := countersSeedZone(t, pool, "fsm-delrace.test")
	raceIDs := renderBitIDsOfLabels(ctx, t, pool, zone, renderBitSeedDebt(ctx, t, pool, zone, "race", n))

	// --- Фаза 1: ДО фикса — воспроизведение −36 --------------------------
	batch := renderBitStageDelete(ctx, t, pool, zone, renderBitLabels("race", n))
	if debt := renderBitDebt(ctx, t, pool); debt != n {
		t.Fatalf("фаза 1: исход счётчик = %d, хочу %d", debt, n)
	}

	// «Проход доработки» (L): снимает все 18 парой и ДЕРЖИТ транзакцию
	// незакоммиченной — строки заблокированы, изменения невидимы.
	lx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("транзакция прохода: %v", err)
	}
	if _, err := lx.Exec(ctx, renderBitClearPairSQL, zone, raceIDs); err != nil {
		t.Fatalf("проход: снятие парой: %v", err)
	}
	xid := renderBitXID(ctx, t, lx)

	// Применение delete (D) оператором T-186: снимок pre видит биты
	// (снятие L незакоммичено), moved блокируется на строках L.
	errc := make(chan error, 1)
	go func() {
		rows, err := pool.Query(ctx, renderBitLegacyDeleteSQL, batch[:], int16(model.StatusDeleted), int16(model.ActionDelete))
		if err != nil {
			errc <- err
			return
		}
		defer rows.Close()
		var deleted, notFound int64
		if !rows.Next() {
			errc <- fmt.Errorf("legacy применение: итог не возвращен: %w", rows.Err())
			return
		}
		if err := rows.Scan(&deleted, &notFound); err != nil {
			errc <- fmt.Errorf("legacy применение: чтение итога: %w", err)
			return
		}
		if err := rows.Err(); err != nil {
			errc <- err
			return
		}
		if deleted != n || notFound != 0 {
			errc <- fmt.Errorf("legacy применение: итог {deleted %d not_found %d}, хочу {%d 0}", deleted, notFound, n)
			return
		}
		errc <- nil
	}()
	renderBitWaitBlocked(t, pool, errc, xid, "фаза 1: применение")

	// Шаг «после прохода» — внутри транзакции прохода (собственные
	// записи видимы, конкурент ещё заблокирован): биты 0, счётчик 0.
	if debt := renderBitInTxDebt(ctx, t, lx); debt != 0 {
		t.Fatalf("фаза 1: после прохода счётчик = %d, хочу 0", debt)
	}
	if bits := renderBitInTxBits(ctx, t, lx, zone, raceIDs); bits != 0 {
		t.Fatalf("фаза 1: после прохода бит = %d, хочу 0", bits)
	}
	// Фиксация прохода отпускает D — как в 11:50:39.982; промежуточных
	// чтений из другой сессии больше нет (гонка с фиксацией D, ревью
	// T-269 P2-1).
	if err := lx.Commit(ctx); err != nil {
		t.Fatalf("фиксация прохода: %v", err)
	}

	// D продолжает на зафиксированной версии: CASE видит бит снятым,
	// rd всё равно декрементирует −18 по снимку — счётчик −18.
	renderBitJoinErr(t, errc, "фаза 1: применение")
	if debt := renderBitDebt(ctx, t, pool); debt != -n {
		t.Fatalf("фаза 1: РЕПРОДУКЦИЯ: счётчик = %d, хочу −%d (второе −n по снимку, суммарный сдвиг −%d)", debt, n, 2*n)
	}
	if bits := renderBitBitCount(ctx, t, pool, zone, raceIDs); bits != 0 {
		t.Fatalf("фаза 1: биты = %d, хочу 0 (долг закрыт проходом)", bits)
	}
	// Парность сломана ровно на −n: сверка видит позицию.
	mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{})
	if err != nil {
		t.Fatalf("фаза 1: VerifyCounters: %v", err)
	}
	if len(mismatch) != 1 || mismatch[0].Counter != "global:render_pending" ||
		mismatch[0].Old != -n || mismatch[0].New != 0 {
		t.Fatalf("фаза 1: рассинхрон = %+v, хочу [global:render_pending −%d → 0]", mismatch, n)
	}

	// Операторское выравнивание ~11:59 (runbooks §6): счётчик := факт.
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET value = $1 WHERE name = 'render_pending'", int64(0)); err != nil {
		t.Fatalf("выравнивание: %v", err)
	}

	// --- Фаза 2: ПОСЛЕ фикса — тот же интерливинг, инвариант на каждом шаге
	fixIDs := renderBitIDsOfLabels(ctx, t, pool, zone, renderBitSeedDebt(ctx, t, pool, zone, "fix", n))
	batch2 := renderBitStageDelete(ctx, t, pool, zone, renderBitLabels("fix", n))
	if debt := renderBitDebt(ctx, t, pool); debt != n {
		t.Fatalf("фаза 2: исход счётчик = %d, хочу %d", debt, n)
	}

	lx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("фаза 2: транзакция прохода: %v", err)
	}
	if _, err := lx2.Exec(ctx, renderBitClearPairSQL, zone, fixIDs); err != nil {
		t.Fatalf("фаза 2: проход: снятие парой: %v", err)
	}
	xid2 := renderBitXID(ctx, t, lx2)

	resc := make(chan ingest.DeleteResult, 1)
	errc2 := make(chan error, 1)
	go func() {
		res, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch2, ZoneID: zone, Action: model.ActionDelete})
		resc <- res
		errc2 <- err
	}()
	renderBitWaitBlocked(t, pool, errc2, xid2, "фаза 2: применение")

	// Шаг «после прохода» — внутри транзакции прохода: инвариант 0.
	if debt := renderBitInTxDebt(ctx, t, lx2); debt != 0 {
		t.Fatalf("фаза 2: после прохода счётчик = %d, хочу 0", debt)
	}
	if bits := renderBitInTxBits(ctx, t, lx2, zone, fixIDs); bits != 0 {
		t.Fatalf("фаза 2: после прохода биты = %d, хочу 0", bits)
	}
	if err := lx2.Commit(ctx); err != nil {
		t.Fatalf("фаза 2: фиксация прохода: %v", err)
	}

	renderBitJoinErr(t, errc2, "фаза 2: применение")
	var res ingest.DeleteResult
	select {
	case res = <-resc:
	default:
		t.Fatal("фаза 2: итог применения не возвращён")
	}
	if res.Deleted != n || res.NotFound != 0 {
		t.Fatalf("фаза 2: ApplyDelete = %+v, хочу deleted %d", res, n)
	}
	// Шаг «после применения»: перевод статуса дошёл, бит уже снят
	// проходом — пара НЕ выполнялась (ключей нет), счётчик не тронут.
	if debt := renderBitDebt(ctx, t, pool); debt != 0 {
		t.Fatalf("фаза 2: ПОСЛЕ ФИКСА счётчик = %d, хочу 0 (инвариант держится)", debt)
	}
	if bits := renderBitBitCount(ctx, t, pool, zone, fixIDs); bits != 0 {
		t.Fatalf("фаза 2: биты = %d, хочу 0", bits)
	}
	for i, id := range fixIDs {
		if r := checkStateOf(ctx, t, pool, zone, id); r.status != int16(model.StatusDeleted) {
			t.Fatalf("фаза 2: домен fix%d: status = %d, хочу deleted", i, r.status)
		}
	}
	countersVerifyEmpty(t, pool, "фаза 2 (проход × применение, после фикса)")

	// --- Фаза 2b: применение выигрывает гонку (бит стоит) — пара по факту
	winIDs := renderBitIDsOfLabels(ctx, t, pool, zone, renderBitSeedDebt(ctx, t, pool, zone, "win", 1))
	batch3 := renderBitStageDelete(ctx, t, pool, zone, renderBitLabels("win", 1))

	res3, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch3, ZoneID: zone, Action: model.ActionDelete})
	if err != nil || res3.Deleted != 1 {
		t.Fatalf("фаза 2b: ApplyDelete = %+v err=%v, хочу deleted 1", res3, err)
	}
	if bits := renderBitBitCount(ctx, t, pool, zone, winIDs); bits != 0 {
		t.Fatalf("фаза 2b: бит не снят применением (%d)", bits)
	}
	if debt := renderBitDebt(ctx, t, pool); debt != 0 {
		t.Fatalf("фаза 2b: счётчик = %d, хочу 0 (пара −1 по факту)", debt)
	}
	countersVerifyEmpty(t, pool, "фаза 2b (применение без прохода)")
}

// renderBitLabels — метки prefix0..prefix(n−1) в порядке посева.
func renderBitLabels(prefix string, n int) []string {
	labels := make([]string, n)
	for i := range labels {
		labels[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return labels
}

// renderBitIDsOfLabels — id доменов по меткам (порядок посева).
func renderBitIDsOfLabels(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, labels []string) []int64 {
	t.Helper()
	ids := make([]int64, len(labels))
	for i, label := range labels {
		if err := pool.QueryRow(ctx,
			"SELECT id FROM domains WHERE zone_id = $1 AND label = $2", zone, label).Scan(&ids[i]); err != nil {
			t.Fatalf("id домена %s: %v", label, err)
		}
	}
	return ids
}

// TestRenderPendingBit_ReconciliationStaleSnapshot — опровержение
// гипотезы инцидента: «реконсиляция monitor применяет дельту от
// устаревшего ожидания — второе −18». Проход реконсиляции с устаревшим
// снапшотом (агрегат видел биты ДО снятия проходом доработки, upsert
// исполнился ПОСЛЕ) пишет устаревший АБСОЛЮТ значением агрегата, не
// дельту: счётчик становится +18 при нулевом бите (ложный долг), НЕ −18;
// перезапись двигает updated_at («громкий» след — «немого» второго −n
// реконсиляция оставить не может) и самолечится следующим проходом
// (проход авторитетен, ≤ zone_stats_interval). Дрейф самого прохода при
// этом ноль (старое = новому = 18): рассинхрон парности ловит
// VerifyCounters, а не gauge дрейфа.
func TestRenderPendingBit_ReconciliationStaleSnapshot(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const n = 18
	zone := countersSeedZone(t, pool, "fsm-monrace.test")
	ids := renderBitIDsOfLabels(ctx, t, pool, zone, renderBitSeedDebt(ctx, t, pool, zone, "mon", n))
	if debt := renderBitDebt(ctx, t, pool); debt != n {
		t.Fatalf("исход счётчик = %d, хочу %d", debt, n)
	}
	// Метка «ручной операции»: updated_at в прошлое — после гонки
	// обязана сдвинуться (громкость perезаписи).
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET updated_at = now() - interval '1 hour' WHERE name = 'render_pending'"); err != nil {
		t.Fatalf("метка updated_at: %v", err)
	}
	var t0 time.Time
	if err := pool.QueryRow(ctx,
		"SELECT updated_at FROM crawler_counters WHERE name = 'render_pending'").Scan(&t0); err != nil {
		t.Fatalf("чтение updated_at: %v", err)
	}

	// L держит строку счётчика — upsert реконсиляции заблокируется НАД
	// снимком (агрегат уже посчитал 18 бит), проход доработки успеет
	// закрыть долг.
	lx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("транзакция-блокиратор: %v", err)
	}
	if _, err := lx.Exec(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending' FOR UPDATE"); err != nil {
		t.Fatalf("блокировка строки счётчика: %v", err)
	}
	xid := renderBitXID(ctx, t, lx)

	// M — реальный проход реконсиляции (T-178): агрегат видит 18 бит,
	// upsert (значение АБСОЛЮТом из снапшота) блокируется на L.
	resc := make(chan monitor.ZoneStatsResult, 1)
	errc := make(chan error, 1)
	go func() {
		res, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
		resc <- res
		errc <- err
	}()
	renderBitWaitBlocked(t, pool, errc, xid, "реконсиляция")

	// «Проход доработки» внутри L: биты → 0, счётчик −18 (L уже держит
	// строку — декремент без ожидания). Шаг «долг закрыт» — внутри
	// транзакции (собственные записи видимы, M ещё заблокирован): 0/0.
	if _, err := lx.Exec(ctx, renderBitClearPairSQL, zone, ids); err != nil {
		t.Fatalf("проход: снятие парой: %v", err)
	}
	if debt := renderBitInTxDebt(ctx, t, lx); debt != 0 {
		t.Fatalf("после прохода счётчик = %d, хочу 0", debt)
	}
	if bits := renderBitInTxBits(ctx, t, lx, zone, ids); bits != 0 {
		t.Fatalf("после прохода биты = %d, хочу 0", bits)
	}
	if err := lx.Commit(ctx); err != nil {
		t.Fatalf("фиксация прохода: %v", err)
	}

	// M отпущен и применяет устаревший АБСОЛЮТ: +18 при нулевом бите —
	// НЕ −18 (никакой дельты от ожидания), updated_at сдвинут (громко).
	renderBitJoinErr(t, errc, "реконсиляция")
	var mres monitor.ZoneStatsResult
	select {
	case mres = <-resc:
	default:
		t.Fatal("итог реконсиляции не возвращён")
	}
	// Дрейф самого прохода — ноль (старое счётчика = новому = 18): gauge
	// crawler_counters_drift парность не измеряет, ломку парности видит
	// только сверка ниже.
	sawPos := false
	for _, p := range mres.Positions {
		if p.Counter != "global:render_pending" {
			continue
		}
		sawPos = true
		if p.Old != n || p.New != n || p.Drift() != 0 {
			t.Fatalf("дрейф прохода = %+v, хочу old=new=%d drift 0 (дрейф — счётчик о счётчике, парность не видит)", p, n)
		}
	}
	if !sawPos {
		t.Fatal("позиция global:render_pending отсутствует в итоге прохода")
	}
	if debt := renderBitDebt(ctx, t, pool); debt != n {
		t.Fatalf("ОПРОВЕРЖЕНИЕ: реконсиляция с устаревшим снимком оставила счётчик %d, хочу +%d (устаревший АБСОЛЮТ; «второго −18» не существует)", debt, n)
	}
	if bits := renderBitBitCount(ctx, t, pool, zone, ids); bits != 0 {
		t.Fatalf("биты = %d, хочу 0", bits)
	}
	var at time.Time
	if err := pool.QueryRow(ctx,
		"SELECT updated_at FROM crawler_counters WHERE name = 'render_pending'").Scan(&at); err != nil {
		t.Fatalf("чтение updated_at: %v", err)
	}
	if !at.After(t0) {
		t.Fatalf("updated_at не сдвинут (%v ≤ %v): перезапись реконсиляции обязана быть «громкой» — немой дельты она не пишет", at, t0)
	}

	// Парность сломана транзиентно (+18/0) — сверка видит; дрейф самого
	// прохода ноль (старое = новому = 18): gauge дрейфа парность не
	// измеряет, лечит следующий проход (проход авторитетен).
	mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyCounters: %v", err)
	}
	if len(mismatch) != 1 || mismatch[0].Counter != "global:render_pending" ||
		mismatch[0].Old != n || mismatch[0].New != 0 {
		t.Fatalf("рассинхрон = %+v, хочу [global:render_pending %d → 0]", mismatch, n)
	}

	// Самолечение: следующий проход пишет свежий абсолют (битов нет).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("лечащий проход: %v", err)
	}
	if debt := renderBitDebt(ctx, t, pool); debt != 0 {
		t.Fatalf("после следующего прохода счётчик = %d, хочу 0 (самолечение ≤ zone_stats_interval)", debt)
	}
	countersVerifyEmpty(t, pool, "после самолечения реконсиляцией")
}

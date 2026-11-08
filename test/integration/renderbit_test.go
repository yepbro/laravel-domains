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
	"testing"
	"time"

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

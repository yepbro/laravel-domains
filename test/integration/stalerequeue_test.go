// Интеграционный тест компенсатора «статус повис» (T-270, ТЗ
// tasks/TZ-fsm.md §3 ФТ-2.2) против живых PostgreSQL и Redis testbed:
// полный сценарий класса потерь — О6 записан (DNS «есть адреса»,
// задача q:http опубликована в живой Redis), отметка состарена за окно
// 24 ч, недоставленная запись q:http потеряна (XTRIM — рестарт Redis
// без персистентности теряет так же, docs/redis.md §5) — реконсиляция
// поднимает уровень new_done_stale (gauge), StaleRequeueOnce
// возвращает домен в pending (перевод Р15 спецификации check_state,
// дельты зон ведёт оператор перевода, счётчик
// crawler_scheduler_stale_requeued_total растёт), повторный проход —
// no-op (идемпотентность), эпизод завершён ШТАТНО конвейером
// (постановка → терминальная ветка DNS), gauge обнуляется. Контракты
// стоимости: guard — точечный PK-лукап crawler_counters (без seq scan
// по domains), выборка пачки — seq scan по domains с ранним остановом
// LIMIT под закреплением SET LOCAL (T-191). Уровень вызовов —
// библиотечные, без запуска сервисов; изоляция — withTestSchema и
// выделенная БД Redis (resilienceRedisEnv).
package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"crawler/internal/dnsworker"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
)

// TestStaleRequeue_LostHTTPTask — сценарий потери задачи q:http при
// записанном done: gauge видит когорту, компенсатор возвращает домен в
// pending, конвейер завершает эпизод, gauge обнуляется.
func TestStaleRequeue_LostHTTPTask(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	rdb := resilienceRedisEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "stale.test")
	q := &countersStubQueue{}

	// Р10 + Р1: вставка и постановка — домен в очереди на DNS-этап.
	add := checkStateStageAdd(ctx, t, pool, zone, "lost")
	if add.Inserted != 1 {
		t.Fatalf("ApplyAdd = %+v, хочу Inserted=1", add)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"SELECT id FROM domains WHERE zone_id = $1 AND label = 'lost'", zone,
	).Scan(&id); err != nil {
		t.Fatalf("id вставленного: %v", err)
	}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "lost.stale.test"},
	}); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	// О6: DNS-эпизод «есть адреса» — публикация q:http в ЖИВОЙ Redis,
	// пометка бита http_published (T-108, порядок воркера: публикация →
	// бит → запись результата), запись ветки keep-status: check_state =
	// done, статус остаётся new (итог решает HTTP-этап).
	httpq := queue.NewRedisQueue(rdb, queue.RedisOptions{})
	task := queue.HTTPTask{
		TaskID:     "stale-requeue-test",
		ZoneID:     zone,
		DomainID:   id,
		Name:       "lost.stale.test",
		Subdomains: []queue.HTTPSubdomain{{SubdomainID: 1, Label: "lost", Addrs: []string{"127.0.0.1"}}},
	}
	if _, err := httpq.Publish(ctx, queue.StreamHTTP, task); err != nil {
		t.Fatalf("публикация q:http: %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamHTTP); n != 1 {
		t.Fatalf("XLEN q:http = %d, хочу 1 — задача опубликована", n)
	}
	if _, err := dnsworker.MarkHTTPPublished(ctx, pool, zone, id); err != nil {
		t.Fatalf("MarkHTTPPublished: %v", err)
	}
	ok, err := dnsworker.RecordEpisode(ctx, pool, zone, id, dnsworker.EpisodeResult{Mask: 1}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode: ok=%v err=%v (О6)", ok, err)
	}
	r := checkStateOf(ctx, t, pool, zone, id)
	if r.state != int16(model.CheckStateDone) || r.status != int16(model.StatusNew) || r.httpPublished {
		t.Fatalf("после О6 = %+v, хочу done/new/бит снят — «статус повис» без HTTP-итога", r)
	}

	// Когорта состарена за окно 24 ч (инцидентная неделя: last_checked_at
	// давно), недоставленная запись q:http потеряна — XTRIM всего стрима
	// (рестарт Redis без персистентности теряет то же самое).
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 2 WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст проверки: %v", err)
	}
	if err := rdb.XTrimMaxLen(ctx, queue.StreamHTTP, 0).Err(); err != nil {
		t.Fatalf("XTRIM q:http (потеря недоставленного): %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamHTTP); n != 0 {
		t.Fatalf("XLEN q:http после потери = %d, хочу 0", n)
	}

	// Реконсиляция поднимает уровень: gauge new_done_stale = 1.
	st, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if st.NewDoneStale != 1 {
		t.Fatalf("NewDoneStale = %d, хочу 1 — gauge видит когорту", st.NewDoneStale)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции (уровень new_done_stale)")

	// Компенсатор (Р15): домен в pending, статус сохранён, аренды нет;
	// дельты зон ведёт оператор перевода; счётчик растёт.
	reg := metrics.New("scheduler")
	res, err := scheduler.StaleRequeueOnce(ctx, pool, scheduler.StaleRequeueOptions{Metrics: reg})
	if err != nil || res.Domains != 1 || res.Selected != 1 {
		t.Fatalf("StaleRequeueOnce = %+v err=%v, хочу Domains=Selected=1 (Р15)", res, err)
	}
	r = checkStateOf(ctx, t, pool, zone, id)
	if r.state != int16(model.CheckStatePending) || r.status != int16(model.StatusNew) ||
		!r.queuedAtNull || !r.lastChecked || r.httpPublished {
		t.Fatalf("после компенсатора = %+v, хочу pending/new/queued_at NULL/отметка есть/бит снят (Р15)", r)
	}
	countersVerifyEmpty(t, pool, "после компенсатора (Р15) — дельты вёл оператор перевода")
	if got := staleRequeuedTotal(t, reg); got != 1 {
		t.Fatalf("счётчик компенсатора = %v, хочу 1", got)
	}

	// Повторный проход — no-op (идемпотентность: переведённые не
	// выбираются), счётчик не растёт.
	repeat, err := scheduler.StaleRequeueOnce(ctx, pool, scheduler.StaleRequeueOptions{Metrics: reg})
	if err != nil || repeat != (scheduler.StaleRequeueResult{}) {
		t.Fatalf("повторный StaleRequeueOnce = %+v err=%v, хочу нулевой (идемпотентность)", repeat, err)
	}
	if got := staleRequeuedTotal(t, reg); got != 1 {
		t.Fatalf("счётчик после повтора = %v, хочу 1 (пустой проход не считается)", got)
	}

	// Эпизод завершён ШТАТНО конвейером: постановка (Р1) и терминальная
	// ветка DNS (Р2) — домен выходит из аномалии с итоговым статусом.
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "lost.stale.test"},
	}); err != nil {
		t.Fatalf("EnqueueBatch (штатный конвейер): %v", err)
	}
	ok, err = dnsworker.RecordEpisode(ctx, pool, zone, id, dnsworker.EpisodeResult{
		SetStatus: true,
		Status:    model.StatusNotDelegated,
	}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode терминальная: ok=%v err=%v (Р2)", ok, err)
	}
	r = checkStateOf(ctx, t, pool, zone, id)
	if r.state != int16(model.CheckStateDone) || r.status != int16(model.StatusNotDelegated) || !r.queuedAtNull {
		t.Fatalf("после штатного завершения = %+v, хочу done/not_delegated/queued_at NULL (Р2)", r)
	}
	countersVerifyEmpty(t, pool, "после штатного завершения эпизода (Р2)")

	// Gauge обнулился: когорта вылечена, сигнал о новых потерях чист.
	if st, err = monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция повторно: %v", err)
	}
	if st.NewDoneStale != 0 {
		t.Fatalf("NewDoneStale после лечения = %d, хочу 0 (gauge обнулён)", st.NewDoneStale)
	}
	var level int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'new_done_stale'",
	).Scan(&level); err != nil || level != 0 {
		t.Fatalf("строка new_done_stale = (%d, %v), хочу 0", level, err)
	}
	countersVerifyEmpty(t, pool, "после обнуления gauge")

	// Контракт формы выборки под закреплением (T-191): SET LOCAL ×3 —
	// seq scan по domains с ранним остановом LIMIT (марафон по
	// domains_recheck_idx не возвращается).
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, pin := range []string{
		"SET LOCAL enable_indexscan = off",
		"SET LOCAL enable_bitmapscan = off",
		"SET LOCAL enable_indexonlyscan = off",
	} {
		if _, err := tx.Exec(ctx, pin); err != nil {
			t.Fatalf("%s: %v", pin, err)
		}
	}
	plan := checkStateExplain(ctx, t, tx, "SELECT zone_id, id FROM domains AS d "+
		"WHERE d.status = 0 AND d.check_state = 2 AND d.last_checked_at < now() - interval '24 hours' "+
		"LIMIT 1000")
	if !strings.Contains(plan, "Seq Scan on domains") || !strings.Contains(plan, "Limit") {
		t.Errorf("выборка пачки не seq scan с ранним остановом:\n%s", plan)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Контракт стоимости guard: точечный PK-лукап crawler_counters —
	// без seq scan по domains (пустое множество — дешёвая проба);
	// seqscan отключён, чтобы крошечная таблица счётчиков не развалила
	// контракт выбором тривиального скана — стиль TestCheckState_LeaseNullRepair.
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx2.Rollback(ctx) }()
	if _, err := tx2.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatalf("SET LOCAL enable_seqscan: %v", err)
	}
	plan = checkStateExplain(ctx, t, tx2,
		"SELECT coalesce((SELECT value FROM crawler_counters WHERE name = 'new_done_stale'), 0)")
	if !strings.Contains(plan, "crawler_counters_pkey") {
		t.Errorf("guard не идёт по PK crawler_counters:\n%s", plan)
	}
	if strings.Contains(plan, "domains") {
		t.Errorf("guard трогает domains:\n%s", plan)
	}
}

// staleRequeuedTotal — значение счётчика компенсатора в реестре (стиль
// checkStateNullReturned).
func staleRequeuedTotal(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_scheduler_stale_requeued_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

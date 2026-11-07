// Интеграционные тесты автомата check_state (T-255, ТЗ tasks/TZ-fsm.md §3
// ФТ-1) против живого PostgreSQL: сценарная цепочка всех переходов
// спецификации (internal/scheduler/checkstate.go — единая точка правды)
// реальными операторами писателей — вставка add-файлом (Р10), постановка
// планировщика (Р1: queued_at + снятие http_published), запись результата
// DNS (Р3: done, сброс аренды), суточная перепроверка (Р7), lease-возврат
// (Р5), терминальная temp_error-ветка DNS (Р2) и повтор temp_error (Р8),
// удаление (Р14) и воскрешение (Р9) — после каждого эпизода дельты
// счётчиков зон сходятся с T-178-реконсиляцией. Отдельно — терминальная
// страховка ФТ-1.2: строка queued/NULL queued_at (нарушенный инвариант
// И1) невидима основному lease-проходу, возвращается ремонтным проходом
// LeaseNullOnce (счётчик crawler_scheduler_lease_null_returned_total
// растёт), повторный проход — 0; EXPLAIN-контракт выборки страховки —
// предикат IS NULL обслуживается листовым экземпляром domains_lease_idx
// (T-104), форма основного lease-запроса не меняется. Ручной recheck
// (Р11) и массовый recheck субдомена (Р12) — cmd/crawlerctl (package
// main): интеграционный тест recheck_integration_test.go, прогоняет
// make test-integration тем же списком пакетов. Диагноз invalid (Р13)
// живой БД покрыт существующим TestCounters_DNSStageTransitions
// (counters_test.go, RecordInvalid) — здесь не дублируется. Уровень
// вызовов — библиотечные, без запуска сервисов; изоляция — withTestSchema.
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/dnsworker"
	"crawler/internal/ingest"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/scheduler"
)

// checkStateRow — наблюдаемое состояние строки domains для проверок
// переходов: check_state, статус, отметка аренды, счётчик temp_error,
// бит http_published.
type checkStateRow struct {
	state         int16
	status        int16
	queuedAtNull  bool
	lastChecked   bool
	tempAttempts  int16
	httpPublished bool
}

// checkStateOf — строка домена по ключу.
func checkStateOf(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, id int64) checkStateRow {
	t.Helper()
	var r checkStateRow
	var queuedAt, lastChecked *time.Time
	var flags int16
	if err := pool.QueryRow(ctx,
		"SELECT check_state, status, queued_at, last_checked_at, temp_attempts, flags "+
			"FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
	).Scan(&r.state, &r.status, &queuedAt, &lastChecked, &r.tempAttempts, &flags); err != nil {
		t.Fatalf("чтение домена %d: %v", id, err)
	}
	r.queuedAtNull = queuedAt == nil
	r.lastChecked = lastChecked != nil
	r.httpPublished = flags&int16(model.FlagHTTPPublished) != 0
	return r
}

// checkStateStageAdd — партия add в staging (метки labels) и применение
// ApplyAdd: вставка (Р10) или воскрешение (Р9) реальным оператором.
func checkStateStageAdd(ctx context.Context, t *testing.T, pool ingest.ApplyDB, zone int16, labels ...string) ingest.AddResult {
	t.Helper()
	batch, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	for i, label := range labels {
		if _, err := pool.Exec(ctx,
			"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, $5)",
			batch[:], zone, label, int16(model.ActionAdd), int16(i+1)); err != nil {
			t.Fatalf("staging add %s: %v", label, err)
		}
	}
	res, err := ingest.ApplyAdd(ctx, pool, ingest.Batch{ID: batch, ZoneID: zone, Action: model.ActionAdd}, 100)
	if err != nil {
		t.Fatalf("ApplyAdd: %v", err)
	}
	return res
}

// TestCheckState_SpecTransitions — цепочка переходов спецификации по
// порядку Р10 → Р1 → Р3 → Р7 → Р1+Р5 → Р2 → Р8 → Р14 → Р9: каждый
// эпизод — реальный писатель, побочные действия (queued_at, бит
// http_published, temp_attempts, дельты зон) проверяются на строке.
func TestCheckState_SpecTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "fsm.test")
	q := &countersStubQueue{}

	// Р10: вставка нового домена add-файлом — status=new, pending, аренды нет.
	add := checkStateStageAdd(ctx, t, pool, zone, "spec")
	if add.Inserted != 1 || add.Revived != 0 {
		t.Fatalf("ApplyAdd = %+v, хочу Inserted=1", add)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"SELECT id FROM domains WHERE zone_id = $1 AND label = 'spec'", zone,
	).Scan(&id); err != nil {
		t.Fatalf("id вставленного: %v", err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		r.status != int16(model.StatusNew) || !r.queuedAtNull {
		t.Fatalf("после вставки = %+v, хочу new/pending/queued_at NULL (Р10)", r)
	}
	countersVerifyEmpty(t, pool, "после вставки (Р10)")

	// Р1: постановка планировщика — queued, аренда начата, бит
	// http_published прошлого эпизода снят, публикация q:dns по RETURNING.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET flags = flags | $1 WHERE zone_id = $2 AND id = $3",
		int16(model.FlagHTTPPublished), zone, id); err != nil {
		t.Fatalf("сеем бит http_published: %v", err)
	}
	eres, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "spec.fsm.test"},
	})
	if err != nil || eres.Queued != 1 || eres.Published != 1 {
		t.Fatalf("EnqueueBatch = %+v err=%v, хочу Queued=Published=1 (Р1)", eres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) ||
		r.queuedAtNull || r.httpPublished {
		t.Fatalf("после постановки = %+v, хочу queued/queued_at NOT NULL/бит снят (Р1)", r)
	}
	countersVerifyEmpty(t, pool, "после постановки (Р1)")

	// Р3: запись результата DNS «есть адреса» — done, аренда завершена,
	// статус-итог за HTTP-этапом (не пишется), отметка проверки есть.
	ok, err := dnsworker.RecordEpisode(ctx, pool, zone, id, dnsworker.EpisodeResult{Mask: 1}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode: ok=%v err=%v (Р3)", ok, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) ||
		!r.queuedAtNull || !r.lastChecked || r.status != int16(model.StatusNew) {
		t.Fatalf("после записи DNS = %+v, хочу done/queued_at NULL/отметка есть/статус не тронут (Р3)", r)
	}
	countersVerifyEmpty(t, pool, "после записи DNS (Р3)")

	// Р7: суточная возрастная перепроверка — done → pending, статус
	// сохраняется, аренды нет.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 400 WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст проверки: %v", err)
	}
	rk, err := scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Interval: 300 * 24 * time.Hour})
	if err != nil || rk.Domains != 1 {
		t.Fatalf("RecheckOnce = %+v err=%v, хочу 1 (Р7)", rk, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		!r.queuedAtNull || r.status != int16(model.StatusNew) {
		t.Fatalf("после перепроверки = %+v, хочу pending/queued_at NULL (Р7)", r)
	}
	countersVerifyEmpty(t, pool, "после перепроверки (Р7)")

	// Р1 + Р5: повторная постановка и истёкшая аренда — возврат в pending.
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "spec.fsm.test"},
	}); err != nil {
		t.Fatalf("EnqueueBatch (Р1 повторно): %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET queued_at = now() - interval '2 hours' WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст аренды: %v", err)
	}
	lres, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil || lres.Domains != 1 {
		t.Fatalf("LeaseOnce = %+v err=%v, хочу 1 (Р5)", lres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) || !r.queuedAtNull {
		t.Fatalf("после lease-возврата = %+v, хочу pending/queued_at NULL (Р5)", r)
	}
	countersVerifyEmpty(t, pool, "после lease-возврата (Р5)")

	// Р2: терминальная temp_error-ветка DNS — done + temp_error, счётчик
	// попыток сохраняется (новый эпизод с нуля), корзина зрелости сегодня.
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "spec.fsm.test"},
	}); err != nil {
		t.Fatalf("EnqueueBatch (перед Р2): %v", err)
	}
	ok, err = dnsworker.RecordEpisode(ctx, pool, zone, id, dnsworker.EpisodeResult{
		SetStatus: true,
		Status:    model.StatusTempError,
	}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode temp_error: ok=%v err=%v (Р2)", ok, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) ||
		r.status != int16(model.StatusTempError) || !r.queuedAtNull {
		t.Fatalf("после temp_error-ветки DNS = %+v, хочу done/temp_error/queued_at NULL (Р2)", r)
	}
	countersVerifyEmpty(t, pool, "после temp_error-ветки DNS (Р2)")

	// Р8: повтор temp_error — done → pending, temp_attempts + 1 (корзину
	// зрелости прошедших суток кладёт руками: сегодняшние эпизоды не
	// зреют, окно 24 ч по date — возраст проверки за окном).
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 2 WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст temp_error: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) "+
			"VALUES ('retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD'), 1)"); err != nil {
		t.Fatalf("корзина зрелости: %v", err)
	}
	tr, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
	if err != nil || tr.Requeued != 1 || tr.Finalized != 0 {
		t.Fatalf("RetryOnce = %+v err=%v, хочу Requeued=1 (Р8)", tr, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		r.status != int16(model.StatusTempError) || r.tempAttempts != 1 {
		t.Fatalf("после повтора temp_error = %+v, хочу pending/temp_error/attempts=1 (Р8)", r)
	}
	countersVerifyEmpty(t, pool, "после повтора temp_error (Р8)")

	// Р14: delete-файл — status=deleted, check_state НЕ меняется (pending).
	dbatch, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, 'spec', $3, 1)",
		dbatch[:], zone, int16(model.ActionDelete)); err != nil {
		t.Fatalf("staging delete: %v", err)
	}
	dres, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: dbatch, ZoneID: zone, Action: model.ActionDelete})
	if err != nil || dres.Deleted != 1 {
		t.Fatalf("ApplyDelete = %+v err=%v, хочу Deleted=1 (Р14)", dres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		r.status != int16(model.StatusDeleted) {
		t.Fatalf("после delete = %+v, хочу pending/deleted — check_state не меняется (Р14)", r)
	}
	countersVerifyEmpty(t, pool, "после delete (Р14)")

	// Р9: воскрешение add-файлом — new/pending, аренды нет.
	radd := checkStateStageAdd(ctx, t, pool, zone, "spec")
	if radd.Revived != 1 || radd.Inserted != 0 {
		t.Fatalf("ApplyAdd воскрешение = %+v, хочу Revived=1 (Р9)", radd)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		r.status != int16(model.StatusNew) || !r.queuedAtNull {
		t.Fatalf("после воскрешения = %+v, хочу new/pending/queued_at NULL (Р9)", r)
	}
	countersVerifyEmpty(t, pool, "после воскрешения (Р9)")
}

// TestCheckState_LeaseNullRepair — терминальная страховка ФТ-1.2: строка
// queued с NULL queued_at (нарушенный инвариант И1) невидима основному
// lease-проходу (NULL отсеивается сравнением возраста — вечное
// зависание до страховки), возвращается ремонтным проходом LeaseNullOnce
// со счётчиком crawler_scheduler_lease_null_returned_total; повторный
// проход — 0 (идемпотентность); дельты зон ведёт оператор перевода.
func TestCheckState_LeaseNullRepair(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "fsm-null.test")
	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO domains (zone_id, label, check_state, status, queued_at)
		 VALUES ($1, 'broken', $2, $3, NULL) RETURNING id`,
		zone, int16(model.CheckStateQueued), int16(model.StatusNew),
	).Scan(&id); err != nil {
		t.Fatalf("домен queued/NULL: %v", err)
	}
	// Ручной INSERT мимо инкрементальных дельт: реконсиляция — авторитет,
	// после неё счётчики зоны истинны (1 queued).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")

	// Основной lease-проход строку НЕ видит: NULL сравнением отсеивается.
	lres, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if lres.Domains != 0 {
		t.Fatalf("основной lease-проход вернул NULL-строку: %+v, хочу 0 — NULL отсеивается возрастом", lres)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) {
		t.Fatalf("строка изменилась основным проходом: %+v, хочу queued", r)
	}

	// Ремонтный проход ФТ-1.2: возврат в pending, счётчик растёт.
	reg := metrics.New("scheduler")
	nres, err := scheduler.LeaseNullOnce(ctx, pool, scheduler.LeaseOptions{Metrics: reg})
	if err != nil || nres.Domains != 1 || nres.Selected != 1 {
		t.Fatalf("LeaseNullOnce = %+v err=%v, хочу Domains=Selected=1", nres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) || !r.queuedAtNull {
		t.Fatalf("после страховки = %+v, хочу pending/queued_at NULL (Р6)", r)
	}
	countersVerifyEmpty(t, pool, "после страховки (Р6) — дельты вёл оператор перевода")
	if got := checkStateNullReturned(t, reg); got != 1 {
		t.Fatalf("счётчик страховки = %v, хочу 1", got)
	}

	// Повторный проход — пусто (переведённые больше не выбираются).
	repeat, err := scheduler.LeaseNullOnce(ctx, pool, scheduler.LeaseOptions{Metrics: reg})
	if err != nil || repeat != (scheduler.LeaseNullResult{}) {
		t.Fatalf("повторный LeaseNullOnce = %+v err=%v, хочу нулевой (идемпотентность)", repeat, err)
	}
	if got := checkStateNullReturned(t, reg); got != 1 {
		t.Fatalf("счётчик после повтора = %v, хочу 1 (пустой проход не считается)", got)
	}

	// EXPLAIN-контракт выборки страховки (стиль TestLease_CursorPredicateIndex):
	// предикат queued_at IS NULL + ORDER BY ключа обслуживаются листовым
	// экземпляром domains_lease_idx (T-104) — пригодность индекса сохранена,
	// seq scan по domains нет.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatalf("SET LOCAL: %v", err)
	}
	plan := checkStateExplain(ctx, t, tx, fmt.Sprintf(
		"SELECT zone_id, id FROM domains "+
			"WHERE check_state = %d AND status <> %d AND queued_at IS NULL "+
			"ORDER BY queued_at, zone_id, id LIMIT %d",
		int16(model.CheckStateQueued), int16(model.StatusDeleted), scheduler.DefaultLeaseBatch))
	if !strings.Contains(plan, "queued_at_zone_id_id_idx") {
		t.Errorf("выборка страховки не обслуживается листовым экземпляром domains_lease_idx:\n%s", plan)
	}
	if strings.Contains(plan, "Seq Scan on domains") {
		t.Errorf("выборка страховки деградировала в seq scan по domains:\n%s", plan)
	}
}

// checkStateNullReturned — значение счётчика страховки в реестре.
func checkStateNullReturned(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_scheduler_lease_null_returned_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// checkStateExplain — текст плана EXPLAIN (COSTS OFF) запроса.
func checkStateExplain(ctx context.Context, t *testing.T, tx pgx.Tx, query string) string {
	t.Helper()
	rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+query)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("EXPLAIN: чтение: %v", err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	return plan.String()
}

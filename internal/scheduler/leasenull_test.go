// Юнит-тесты терминальной страховки queued/NULL queued_at (T-255, ТЗ
// ТЗ-fsm §3 ФТ-1.2) на фейковом pgx-пуле с транзакциями (расширение
// leaseFakePool — lease_test.go): точные SQL-тексты обоих операторов и
// параметры (LIMIT пачки, охрана queued + IS NULL, сброс queued_at, коды
// check_state/status литералами), пара операторов в одной транзакции,
// счёт по RETURNING, пустая пачка — no-op, guard отсеял изменившиеся
// строки, ошибки шагов и nil-пул, счётчик
// crawler_scheduler_lease_null_returned_total растёт по переводам,
// дренаж цикла LeaseWatch пачками до исчерпания с переживанием ошибки.
// Поведение против живой БД (включая пригодность domains_lease_idx) —
// интеграционные тесты (test/integration/checkstate_test.go).
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// --- заготовки -------------------------------------------------------------

// leaseNullSeedBatch — пачка страховки n строк: одна зона, id по порядку.
func leaseNullSeedBatch(n int, translated int64) leaseNullBatch {
	rows := make([]leaseNullRow, n)
	for i := range rows {
		rows[i] = leaseNullRow{zone: 1, id: int64(i + 1)}
	}
	return leaseNullBatch{rows: rows, translated: translated}
}

// --- ожидаемые SQL-тексты (контракт ФТ-1.2) --------------------------------

const wantLeaseNullSelect = "SELECT zone_id, id FROM domains " +
	"WHERE check_state = 1 AND status <> 6 AND queued_at IS NULL " +
	"ORDER BY queued_at, zone_id, id LIMIT $1"

const wantLeaseNullUpdate = "WITH upd AS (UPDATE domains AS t " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) " +
	"WHERE t.zone_id = u.zone_id AND t.id = u.id " +
	"AND t.check_state = 1 AND t.status <> 6 AND t.queued_at IS NULL " +
	"RETURNING t.zone_id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_queued = z.domains_queued - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT zone_id FROM upd"

// --- LeaseNullOnce ---------------------------------------------------------

// TestLeaseNullOnceSQL — контракт обоих операторов страховки: SELECT пачки
// queued/NULL без deleted по индексному ключу, UPDATE … FROM unnest с
// guard-охранами (IS NULL, сброс queued_at, дельты зон); точные
// параметры, порядок операций одной транзакции, счёт по RETURNING.
func TestLeaseNullOnceSQL(t *testing.T) {
	pool := &leaseFakePool{nullBatches: []leaseNullBatch{leaseNullSeedBatch(3, 2)}}

	res, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{BatchSize: 7000})
	if err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}

	ops := pool.opsSnapshot()
	want := []string{"begin", "null_select", "null_update", "commit"}
	if len(ops) != len(want) {
		t.Fatalf("порядок операций = %v, want %v — пара операторов в одной транзакции", ops, want)
	}
	for i := range want {
		if ops[i] != want[i] {
			t.Fatalf("порядок операций = %v, want %v", ops, want)
		}
	}

	sel := pool.nullSelectSnapshot()
	if len(sel) != 1 {
		t.Fatalf("SELECT-вызовов страховки = %d, want 1", len(sel))
	}
	if sel[0].sql != wantLeaseNullSelect {
		t.Errorf("SQL выборки страховки:\n%s\nwant:\n%s", sel[0].sql, wantLeaseNullSelect)
	}
	if len(sel[0].args) != 1 || sel[0].args[0] != 7000 {
		t.Errorf("аргументы выборки = %v, want [7000] (LIMIT пачки)", sel[0].args)
	}

	upd := pool.nullUpdateSnapshot()
	if len(upd) != 1 {
		t.Fatalf("UPDATE-вызовов страховки = %d, want 1", len(upd))
	}
	if upd[0].sql != wantLeaseNullUpdate {
		t.Errorf("SQL перевода страховки:\n%s\nwant:\n%s", upd[0].sql, wantLeaseNullUpdate)
	}
	wantUpdArgs := []any{
		int16(model.CheckStatePending), // SET check_state
		[]int16{1, 1, 1},               // зоны выбранной пачки
		[]int64{1, 2, 3},               // домены выбранной пачки
	}
	if !reflect.DeepEqual(upd[0].args, wantUpdArgs) {
		t.Errorf("аргументы перевода = %v, want %v", upd[0].args, wantUpdArgs)
	}

	// Счёт по RETURNING: переведено 2 из выбранных 3 (guard отсеял одну).
	if res.Domains != 2 || res.Selected != 3 {
		t.Errorf("итог = %+v, want Domains=2 (RETURNING), Selected=3", res)
	}
}

// TestLeaseNullOnceDefaults — нулевые опции: LIMIT — дефолтная пачка
// основного прохода (один размер на цикл LeaseWatch).
func TestLeaseNullOnceDefaults(t *testing.T) {
	pool := &leaseFakePool{nullBatches: []leaseNullBatch{leaseNullSeedBatch(1, 1)}}
	if _, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{}); err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	sel := pool.nullSelectSnapshot()
	if len(sel) != 1 || sel[0].args[0] != DefaultLeaseBatch {
		t.Fatalf("LIMIT = %v, want дефолт %d", sel[0].args, DefaultLeaseBatch)
	}
}

// TestLeaseNullOnceEmpty — исчерпанное множество: пустая пачка — только
// SELECT, перевод и фиксация не выполняются, итог нулевой.
func TestLeaseNullOnceEmpty(t *testing.T) {
	pool := &leaseFakePool{} // очередь пачек страховки пуста

	res, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	if res != (LeaseNullResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	ops := pool.opsSnapshot()
	if len(ops) != 2 || ops[0] != "begin" || ops[1] != "null_select" {
		t.Errorf("порядок операций = %v, want [begin null_select] — пустая пачка без перевода", ops)
	}
}

// TestLeaseNullOnceGuardFiltered — guard перевода отсеял часть пачки
// (судьба строк изменилась между выборкой и блокировкой): счёт — по
// RETURNING, unnest пробивает всю выбранную пачку.
func TestLeaseNullOnceGuardFiltered(t *testing.T) {
	pool := &leaseFakePool{nullBatches: []leaseNullBatch{leaseNullSeedBatch(5, 1)}}

	res, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	if res.Domains != 1 || res.Selected != 5 {
		t.Fatalf("итог = %+v, want Domains=1 (RETURNING после guard), Selected=5", res)
	}
	upd := pool.nullUpdateSnapshot()
	if got := len(upd[0].args[1].([]int16)); got != 5 {
		t.Errorf("unnest-ключей перевода = %d, want 5 — пробивается вся выбранная пачка", got)
	}
}

// TestLeaseNullOnceError — ошибки шагов прохода обёртываются с контекстом
// страховки; nil-пул — ошибка «пул не задан».
func TestLeaseNullOnceError(t *testing.T) {
	selectPool := &leaseFakePool{
		nullBatches:    []leaseNullBatch{leaseNullSeedBatch(2, 2)},
		nullSelectErrs: []error{errors.New("бд недоступна")},
	}
	_, err := LeaseNullOnce(context.Background(), selectPool, LeaseOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "scheduler: страховка queued без queued_at") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка выборки = %v, want контекст страховки и исходная ошибка", err)
	}

	updatePool := &leaseFakePool{
		nullBatches:    []leaseNullBatch{leaseNullSeedBatch(2, 2)},
		nullUpdateErrs: []error{context.Canceled},
	}
	if _, err := LeaseNullOnce(context.Background(), updatePool, LeaseOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка перевода = %v, want context.Canceled", err)
	}

	commitPool := &leaseFakePool{
		nullBatches: []leaseNullBatch{leaseNullSeedBatch(2, 2)},
		commitErrs:  []error{errors.New("connection refused")},
	}
	if _, err := LeaseNullOnce(context.Background(), commitPool, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "фиксация") {
		t.Errorf("ошибка Commit = %v, want контекст фиксации", err)
	}

	beginPool := &leaseFakePool{beginErr: errors.New("нет соединений")}
	if _, err := LeaseNullOnce(context.Background(), beginPool, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "транзакция") {
		t.Errorf("ошибка Begin = %v, want контекст транзакции", err)
	}

	if _, err := LeaseNullOnce(context.Background(), nil, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// TestLeaseNullOnceMetrics — счётчик crawler_scheduler_lease_null_returned_total
// растёт на фактически переведённые строки (по RETURNING, не по выборке):
// два прохода 2 + 0 — итог 2; без реестра (nil Metrics) проход работает.
func TestLeaseNullOnceMetrics(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &leaseFakePool{nullBatches: []leaseNullBatch{
		leaseNullSeedBatch(3, 2), // переведено 2 (guard отсеял одну)
		leaseNullSeedBatch(0, 0), // множество исчерпано
	}}

	first, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	second, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("повторный LeaseNullOnce: %v", err)
	}
	if first.Domains != 2 || second.Domains != 0 {
		t.Fatalf("проходы = %+v, %+v; want Domains 2, 0", first, second)
	}

	if got := leaseNullReturnedValue(t, reg); got != 2 {
		t.Errorf("счётчик после проходов = %v, want 2 (по RETURNING, пустой проход не считается)", got)
	}

	// Без реестра — no-op, проход выполняется.
	noop := &leaseFakePool{nullBatches: []leaseNullBatch{leaseNullSeedBatch(1, 1)}}
	if _, err := LeaseNullOnce(context.Background(), noop, LeaseOptions{}); err != nil {
		t.Fatalf("LeaseNullOnce без метрик: %v", err)
	}
}

// leaseNullReturnedValue — значение счётчика страховки в реестре
// (стиль driftGaugeValue монитор-тестов).
func leaseNullReturnedValue(t *testing.T, reg *metrics.Registry) float64 {
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

// --- цикл LeaseWatch: ремонтный проход -------------------------------------

// TestLeaseWatchNullDrain — цикл LeaseWatch выполняет ремонтный проход
// страховки тем же тиком (после дренажа основного множества): полная
// пачка продолжает дренаж, неполная останавливает; счётчик растёт на
// итог; переведённые не возвращаются (повторный проход — пустая пачка).
func TestLeaseWatchNullDrain(t *testing.T) {
	reg := metrics.New("scheduler")
	// Полная пачка DefaultLeaseBatch + неполная 2 — дренаж двумя
	// проходами; основное множество пусто (batches нет).
	pool := &leaseFakePool{
		nullBatches: []leaseNullBatch{
			leaseNullSeedBatch(DefaultLeaseBatch, int64(DefaultLeaseBatch)),
			leaseNullSeedBatch(2, 2),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Interval: time.Hour, // тик не наступит: только стартовый дренаж
			Metrics:  reg,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		sel := len(pool.nullSelectSnapshot())
		upd := len(pool.nullUpdateSnapshot())
		if sel >= 2 && upd >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("дренаж страховки не дошёл до второй пачки: selects=%d updates=%d", sel, upd)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if got := leaseNullReturnedValue(t, reg); got != float64(DefaultLeaseBatch+2) {
		t.Errorf("счётчик дренажа = %v, want %d", got, DefaultLeaseBatch+2)
	}
	// Оба прохода — одним тиком старта: без тика новых выборок нет.
	if sel := len(pool.nullSelectSnapshot()); sel != 2 {
		t.Errorf("SELECT-вызовов страховки = %d, want 2 (неполная пачка останавливает)", sel)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}
}

// TestLeaseWatchNullDrainError — ошибка пачки страховки прерывает только
// её дренаж: цикл живёт, очередной тик повторяет (контракт OnRun); Warn
// в журнале.
func TestLeaseWatchNullDrainError(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &leaseFakePool{
		nullBatches: []leaseNullBatch{
			leaseNullSeedBatch(2, 2),
			leaseNullSeedBatch(1, 1),
		},
		nullSelectErrs: []error{errors.New("бд недоступна")}, // первый проход падает
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Timeout: 40 * time.Millisecond, // интервал-дефолт: 20 мс
			Log:     logger,
		})
	}()

	// Первый дренаж падает, тик повторяет и доводит обе пачки.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if upd := len(pool.nullUpdateSnapshot()); upd >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("очередной тик не повторил дренаж страховки: updates=%d", len(pool.nullUpdateSnapshot()))
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}

	log := buf.String()
	for _, want := range []string{
		"level=WARN",
		"бд недоступна",
		"строки queued без queued_at возвращены", // итог дренажа — Warn (сигнал оператору)
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

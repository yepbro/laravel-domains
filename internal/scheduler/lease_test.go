// Юнит-тесты возврата доменов с истёкшим lease (T-029, ТЗ 5.3, 3.4) на
// фейковом pgx-пуле: точный SQL-текст и параметры (граница now() − lease,
// охрана queued, сброс queued_at, коды check_state/status), пачка LIMIT,
// дефолты, пустой набор, повторные проходы, ошибки и отмена ctx, цикл
// LeaseWatch (немедленный дренаж, интервал, переживание ошибки, чистый
// выход). Поведение против живой БД — интеграционные тесты этапа 2.
package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/model"
)

// --- фейковый пул ---------------------------------------------------------

// leaseCall — записанный SQL-вызов с аргументами.
type leaseCall struct {
	sql  string
	args []any
}

// leaseFakePool — db.DB без БД: очередь результатов Exec (число
// возвращённых строк) и ошибок, протокол вызовов. Всё под замком:
// цикл LeaseWatch работает в горутине, тест ожидает прогресс по
// счётчикам.
type leaseFakePool struct {
	mu       sync.Mutex
	tags     []int64 // очередь RowsAffected (кончилась — 0: пустой набор)
	execErrs []error // очередь ошибок Exec (nil — вызов без ошибки)

	execs []leaseCall
}

func (p *leaseFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *leaseFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("Query не должен вызываться")
}

func (p *leaseFakePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execs = append(p.execs, leaseCall{sql: sql, args: args})
	if len(p.execErrs) > 0 {
		err := p.execErrs[0]
		p.execErrs = p.execErrs[1:]
		if err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	n := int64(0)
	if len(p.tags) > 0 {
		n = p.tags[0]
		p.tags = p.tags[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
}

// stats — снимок числа вызовов под замком (ожидание прогресса цикла).
func (p *leaseFakePool) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.execs)
}

// calls — снимок протокола вызовов под замком.
func (p *leaseFakePool) calls() []leaseCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]leaseCall(nil), p.execs...)
}

// --- ожидаемый SQL-текст (контракт) ---------------------------------------

const wantLeaseUpdate = "UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM (SELECT zone_id, id FROM domains " +
	"WHERE check_state = 1 AND status <> 6 " +
	"AND queued_at < COALESCE($2::timestamptz, now()) - make_interval(secs => $3) " +
	"ORDER BY queued_at, zone_id, id LIMIT $4) AS b " +
	"WHERE d.zone_id = b.zone_id AND d.id = b.id " +
	"AND d.check_state = 1 AND d.status <> 6 " +
	"AND d.queued_at < COALESCE($2::timestamptz, now()) - make_interval(secs => $3)"

// leaseFixedClock — часы с фиксированным «сейчас» (порог прохода
// воспроизводим до наносекунды).
func leaseFixedClock() (time.Time, func() time.Time) {
	now := time.Date(2026, 10, 17, 4, 30, 0, 0, time.UTC)
	return now, func() time.Time { return now }
}

// --- LeaseOnce -----------------------------------------------------------

// TestLeaseOnceSQL — один оператор UPDATE с точными параметрами:
// целевое состояние pending со сбросом queued_at, отбор queued/не-deleted
// по возрасту аренды; порог считает сервер БД (NULL «сейчас» — только
// тесты подставляют часы), срок — секунды make_interval, размер пачки.
func TestLeaseOnceSQL(t *testing.T) {
	now, clock := leaseFixedClock()
	pool := &leaseFakePool{tags: []int64{42}}

	res, err := LeaseOnce(context.Background(), pool, LeaseOptions{
		Timeout:   15 * time.Minute,
		BatchSize: 8000,
		Now:       clock,
	})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("вызовов = %d, want 1: проход — один оператор", len(calls))
	}
	if calls[0].sql != wantLeaseUpdate {
		t.Errorf("SQL прохода:\n%s\nwant:\n%s", calls[0].sql, wantLeaseUpdate)
	}
	wantArgs := []any{
		int16(model.CheckStatePending), // SET check_state
		&now,                           // «сейчас» тестовых часов (в бою NULL → now() БД)
		(15 * time.Minute).Seconds(),   // срок аренды — make_interval
		8000,                           // LIMIT пачки
	}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("аргументы = %v, want %v", calls[0].args, wantArgs)
	}
	if res.Domains != 42 {
		t.Errorf("итог = %+v, want Domains=42 (RowsAffected)", res)
	}
}

// TestLeaseOnceClockIsServer — без тестовых часов «сейчас» не передаётся
// вовсе (NULL): порог считает now() сервера БД, а не часы процесса
// планировщика (S-02).
func TestLeaseOnceClockIsServer(t *testing.T) {
	pool := &leaseFakePool{}
	if _, err := LeaseOnce(context.Background(), pool, LeaseOptions{}); err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	args := pool.calls()[0].args
	if got, ok := args[1].(*time.Time); !ok || got != nil {
		t.Errorf("«сейчас» = %v (%T), want nil *time.Time — часы сервера БД", args[1], args[1])
	}
	if got, ok := args[2].(float64); !ok || got != (30*time.Minute).Seconds() {
		t.Errorf("срок = %v, want %v секунд дефолта", args[2], (30 * time.Minute).Seconds())
	}
}

// TestLeaseOnceDefaults — нулевые опции: срок аренды и пачка — дефолты
// (30 минут по ТЗ 5.3), часы фиксированные: «сейчас» и срок уходят в SQL
// параметрами.
func TestLeaseOnceDefaults(t *testing.T) {
	now, clock := leaseFixedClock()
	pool := &leaseFakePool{}

	if _, err := LeaseOnce(context.Background(), pool, LeaseOptions{Now: clock}); err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}

	args := pool.calls()[0].args
	if args[3] != DefaultLeaseBatch {
		t.Errorf("LIMIT = %v, want дефолт %d", args[3], DefaultLeaseBatch)
	}
	if got, ok := args[1].(*time.Time); !ok || !got.Equal(now) {
		t.Errorf("«сейчас» = %v, want фиксированные часы %v", args[1], now)
	}
	if got, ok := args[2].(float64); !ok || got != DefaultLeaseTimeout.Seconds() {
		t.Errorf("срок = %v, want дефолт %s в секундах", args[2], DefaultLeaseTimeout)
	}
}

// TestLeaseOnceEmpty — исчерпанное множество: RowsAffected 0, итог
// нулевой, оператор один.
func TestLeaseOnceEmpty(t *testing.T) {
	pool := &leaseFakePool{} // очередь tags пуста — UPDATE 0

	res, err := LeaseOnce(context.Background(), pool, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res != (LeaseResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	if n := pool.stats(); n != 1 {
		t.Errorf("операторов = %d, want 1", n)
	}
}

// TestLeaseOnceRepeatPasses — переведённые строки не выбираются
// повторно (условие отбора в самом SQL): каждый проход — свой оператор,
// после опустошения — нулевой итог.
func TestLeaseOnceRepeatPasses(t *testing.T) {
	pool := &leaseFakePool{tags: []int64{5000, 7, 0}}

	first, err := LeaseOnce(context.Background(), pool, LeaseOptions{})
	if err != nil {
		t.Fatalf("первый проход: %v", err)
	}
	second, err := LeaseOnce(context.Background(), pool, LeaseOptions{})
	if err != nil {
		t.Fatalf("второй проход: %v", err)
	}
	third, err := LeaseOnce(context.Background(), pool, LeaseOptions{})
	if err != nil {
		t.Fatalf("третий проход: %v", err)
	}
	if first.Domains != 5000 || second.Domains != 7 || third.Domains != 0 {
		t.Errorf("проходы = %d, %d, %d; want 5000, 7, 0",
			first.Domains, second.Domains, third.Domains)
	}
	if n := pool.stats(); n != 3 {
		t.Errorf("операторов = %d, want 3 (по одному на проход)", n)
	}
}

// TestLeaseOnceError — ошибка оператора обёрнута с контекстом задачи;
// отмена ctx сохраняется errors.Is.
func TestLeaseOnceError(t *testing.T) {
	pool := &leaseFakePool{execErrs: []error{errors.New("бд недоступна")}}
	_, err := LeaseOnce(context.Background(), pool, LeaseOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "scheduler: возврат истёкших lease") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка = %v, want контекст задачи и исходная ошибка", err)
	}

	cancelPool := &leaseFakePool{execErrs: []error{context.Canceled}}
	if _, err := LeaseOnce(context.Background(), cancelPool, LeaseOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
	}
}

func TestLeaseOnceNilPool(t *testing.T) {
	if _, err := LeaseOnce(context.Background(), nil, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// --- LeaseOptions ---------------------------------------------------------

func TestLeaseOptionsDefaults(t *testing.T) {
	// Нулевые и отрицательные значения — дефолты; интервал цикла —
	// половина срока аренды (баланс «скорость возврата vs нагрузка»,
	// см. LeaseWatch), в том числе от дефолтного срока.
	got := (LeaseOptions{}).withDefaults()
	if got.Timeout != DefaultLeaseTimeout || got.BatchSize != DefaultLeaseBatch {
		t.Errorf("withDefaults = Timeout=%v BatchSize=%v, want %v/%v",
			got.Timeout, got.BatchSize, DefaultLeaseTimeout, DefaultLeaseBatch)
	}
	if want := DefaultLeaseTimeout / 2; got.Interval != want {
		t.Errorf("Interval = %v, want %v (Timeout/2)", got.Interval, want)
	}

	got = (LeaseOptions{Timeout: -time.Minute, BatchSize: -1, Interval: -time.Second}).withDefaults()
	if got.Timeout != DefaultLeaseTimeout || got.BatchSize != DefaultLeaseBatch ||
		got.Interval != DefaultLeaseTimeout/2 {
		t.Errorf("withDefaults(отрицательные) = %+v, want дефолты", got)
	}

	// Короткий срок — интервал ужимается вместе с ним: цикл не
	// «переспит» настраиваемый lease.
	got = (LeaseOptions{Timeout: 2 * time.Minute}).withDefaults()
	if got.Interval != time.Minute {
		t.Errorf("Interval = %v, want 1m (Timeout/2 при Timeout=2m)", got.Interval)
	}

	// Вырожденный Timeout 1 нс делением дал бы 0 — тикеру нужен
	// положительный интервал.
	if got := (LeaseOptions{Timeout: time.Nanosecond}).withDefaults(); got.Interval <= 0 {
		t.Errorf("Interval = %v при Timeout=1ns, want > 0", got.Interval)
	}

	// Заданные значения не затираются, Now передаётся как есть.
	now := func() time.Time { return time.Unix(0, 0) }
	got = LeaseOptions{
		Timeout:   time.Hour,
		BatchSize: 10,
		Interval:  time.Minute,
		Now:       now,
	}.withDefaults()
	if got.Timeout != time.Hour || got.BatchSize != 10 || got.Interval != time.Minute {
		t.Errorf("withDefaults затирает значения: %+v", got)
	}
	if got.Now == nil || !got.Now().Equal(time.Unix(0, 0)) {
		t.Errorf("withDefaults затирает Now: nil=%v, значение %v", got.Now == nil, got.Now())
	}
}

// --- LeaseWatch -----------------------------------------------------------

// TestLeaseWatchImmediateDrain — первый дренаж выполняется сразу при
// старте, не дожидаясь тика: подбирает зависшее за простой сервиса
// (ТЗ 3.4, 11.4 п. 9).
func TestLeaseWatchImmediateDrain(t *testing.T) {
	pool := &leaseFakePool{tags: []int64{7}} // неполная пачка — дренаж закончен

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Interval: time.Hour, // тик не наступит: только стартовый дренаж
		})
	}()

	// Стартовый дренаж: один оператор и тишина до отмены.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 1 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("стартовый дренаж не выполнен")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if n := pool.stats(); n != 1 {
		t.Errorf("операторов = %d, want 1: без тика повторных дренажей нет", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}
}

// TestLeaseWatchLoop — полный дренаж пачками до исчерпания, ошибка
// пачки прерывает только свой дренаж (Warn), очередной тик продолжает;
// интервал по умолчанию — половина срока аренды; отмена ctx — чистый nil.
func TestLeaseWatchLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Дренаж 1 (старт): ошибка БД — Warn. Дренаж 2 (тик): полная пачка +
	// неполная — Info с суммой. Дальше — пустые множества.
	pool := &leaseFakePool{
		tags:     []int64{DefaultLeaseBatch, 3},
		execErrs: []error{errors.New("бд недоступна")},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Timeout: 40 * time.Millisecond, // интервал-дефолт: 20 мс
			Log:     logger,
		})
	}()

	// Три оператора: провал первого дренажа и оба пакета второго.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до третьего оператора: %d", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}

	log := buf.String()
	for _, want := range []string{
		"возврат доменов с истёкшим lease", // старт
		"interval=20ms", // дефолт: Timeout/2
		"level=WARN",    // ошибка дренажа не убила цикл
		"бд недоступна",
		fmt.Sprintf("domains=%d", DefaultLeaseBatch+3), // итог дренажа
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

// TestLeaseWatchCancelDuringDrain — отмена ctx во время дренажа — не
// тревога: без Warn, чистый nil (контракт OnRun, T-012).
func TestLeaseWatchCancelDuringDrain(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &leaseFakePool{execErrs: []error{context.Canceled}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до старта: первый же оператор дренажа провалится

	done := make(chan error, 1)
	go func() { done <- LeaseWatch(ctx, pool, LeaseOptions{Log: logger}) }()

	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("отмена во время дренажа — Warn:\n%s", buf.String())
	}
}

func TestLeaseWatchNilPool(t *testing.T) {
	err := LeaseWatch(context.Background(), nil, LeaseOptions{})
	if err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

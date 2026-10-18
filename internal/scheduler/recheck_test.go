// Юнит-тесты суточной перепроверки доменов (T-030, ТЗ 5.3, 3.3, 11.4
// п. 8) на фейковом pgx-пуле: точный SQL-текст и параметры (граница
// now()−interval, размер пачки, коды check_state/status), дефолты,
// пустой набор, повторные проходы, ошибки и отмена ctx, цикл
// RecheckDaily (немедленный дренаж, переживание ошибки, чистый выход).
// Поведение против живой БД — интеграционные тесты этапа 2.
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

// recheckCall — записанный SQL-вызов с аргументами.
type recheckCall struct {
	sql  string
	args []any
}

// recheckFakePool — db.DB без БД: очередь результатов Exec (число
// переведённых строк) и ошибок, протокол вызовов. Всё под замком:
// цикл RecheckDaily работает в горутине, тест ожидает прогресс по
// счётчикам.
type recheckFakePool struct {
	mu       sync.Mutex
	tags     []int64 // очередь RowsAffected (кончилась — 0: пустой набор)
	execErrs []error // очередь ошибок Exec (nil — вызов без ошибки)

	execs []recheckCall
}

func (p *recheckFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *recheckFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("Query не должен вызываться")
}

func (p *recheckFakePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execs = append(p.execs, recheckCall{sql: sql, args: args})
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
func (p *recheckFakePool) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.execs)
}

// calls — снимок протокола вызовов под замком.
func (p *recheckFakePool) calls() []recheckCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]recheckCall(nil), p.execs...)
}

// --- ожидаемый SQL-текст (контракт) ---------------------------------------

const wantRecheckUpdate = "UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM (SELECT zone_id, id FROM domains " +
	"WHERE check_state = 2 AND status <> 6 AND status <> 7 AND last_checked_at < $2 " +
	"ORDER BY last_checked_at, zone_id, id LIMIT $3) AS b " +
	"WHERE d.zone_id = b.zone_id AND d.id = b.id " +
	"AND d.check_state = 2 AND d.status <> 6 AND d.status <> 7 AND d.last_checked_at < $2"

// recheckFixedClock — часы с фиксированным «сейчас» (порог прохода
// воспроизводим до наносекунды).
func recheckFixedClock() (time.Time, func() time.Time) {
	now := time.Date(2026, 10, 17, 3, 0, 0, 0, time.UTC)
	return now, func() time.Time { return now }
}

// --- RecheckOnce -----------------------------------------------------------

// TestRecheckOnceSQL — один оператор UPDATE с точными параметрами:
// целевое состояние pending, отбор done/не-deleted, граница
// now() − interval и размер пачки.
func TestRecheckOnceSQL(t *testing.T) {
	now, clock := recheckFixedClock()
	pool := &recheckFakePool{tags: []int64{42}}

	res, err := RecheckOnce(context.Background(), pool, RecheckOptions{
		Interval:  2190 * time.Hour,
		BatchSize: 7000,
		Now:       clock,
	})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("вызовов = %d, want 1: проход — один оператор", len(calls))
	}
	if calls[0].sql != wantRecheckUpdate {
		t.Errorf("SQL прохода:\n%s\nwant:\n%s", calls[0].sql, wantRecheckUpdate)
	}
	wantArgs := []any{
		int16(model.CheckStatePending), // SET check_state
		now.Add(-2190 * time.Hour),     // граница now() − interval
		7000,                           // LIMIT пачки
	}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("аргументы = %v, want %v", calls[0].args, wantArgs)
	}
	if res.Domains != 42 {
		t.Errorf("итог = %+v, want Domains=42 (RowsAffected)", res)
	}
}

// TestRecheckOnceDefaults — нулевые опции: интервал и пачка — дефолты
// (4380 ч ≈ 6 месяцев, ТЗ 3.3), часы — фиксированные: порог считается
// точно.
func TestRecheckOnceDefaults(t *testing.T) {
	now, clock := recheckFixedClock()
	pool := &recheckFakePool{}

	if _, err := RecheckOnce(context.Background(), pool, RecheckOptions{Now: clock}); err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}

	args := pool.calls()[0].args
	if args[2] != DefaultRecheckBatch {
		t.Errorf("LIMIT = %v, want дефолт %d", args[2], DefaultRecheckBatch)
	}
	if got, ok := args[1].(time.Time); !ok || !got.Equal(now.Add(-DefaultRecheckInterval)) {
		t.Errorf("порог = %v, want %v (now − дефолт %s)", args[1], now.Add(-DefaultRecheckInterval), DefaultRecheckInterval)
	}
}

// TestRecheckOnceEmpty — исчерпанное множество: RowsAffected 0, итог
// нулевой, оператор один.
func TestRecheckOnceEmpty(t *testing.T) {
	pool := &recheckFakePool{} // очередь tags пуста — UPDATE 0

	res, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}
	if res != (RecheckResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	if n := pool.stats(); n != 1 {
		t.Errorf("операторов = %d, want 1", n)
	}
}

// TestRecheckOnceRepeatPasses — переведённые строки не выбираются
// повторно (условие отбора в самом SQL): каждый проход — свой оператор,
// после опустошения — нулевой итог.
func TestRecheckOnceRepeatPasses(t *testing.T) {
	pool := &recheckFakePool{tags: []int64{5000, 3, 0}}

	first, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err != nil {
		t.Fatalf("первый проход: %v", err)
	}
	second, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err != nil {
		t.Fatalf("второй проход: %v", err)
	}
	third, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err != nil {
		t.Fatalf("третий проход: %v", err)
	}
	if first.Domains != 5000 || second.Domains != 3 || third.Domains != 0 {
		t.Errorf("проходы = %d, %d, %d; want 5000, 3, 0",
			first.Domains, second.Domains, third.Domains)
	}
	if n := pool.stats(); n != 3 {
		t.Errorf("операторов = %d, want 3 (по одному на проход)", n)
	}
}

// TestRecheckOnceError — ошибка оператора обёрнута с контекстом
// задачи; отмена ctx сохраняется errors.Is.
func TestRecheckOnceError(t *testing.T) {
	pool := &recheckFakePool{execErrs: []error{errors.New("бд недоступна")}}
	_, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "scheduler: перепроверка доменов") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка = %v, want контекст задачи и исходная ошибка", err)
	}

	cancelPool := &recheckFakePool{execErrs: []error{context.Canceled}}
	if _, err := RecheckOnce(context.Background(), cancelPool, RecheckOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
	}
}

func TestRecheckOnceNilPool(t *testing.T) {
	if _, err := RecheckOnce(context.Background(), nil, RecheckOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// --- RecheckOptions --------------------------------------------------------

// recheckCheckDefaults — значения после withDefaults (Now — функция,
// сравнируется отдельно: struct с полем-функцией не сравнивается целиком).
func recheckCheckDefaults(t *testing.T, got RecheckOptions, wantInterval time.Duration, wantBatch int, wantSchedule time.Duration) {
	t.Helper()
	if got.Interval != wantInterval || got.BatchSize != wantBatch || got.Schedule != wantSchedule {
		t.Errorf("withDefaults = Interval=%v BatchSize=%v Schedule=%v, want %v/%v/%v",
			got.Interval, got.BatchSize, got.Schedule, wantInterval, wantBatch, wantSchedule)
	}
}

func TestRecheckOptionsDefaults(t *testing.T) {
	// Нулевые и отрицательные значения — дефолты.
	recheckCheckDefaults(t, (RecheckOptions{}).withDefaults(),
		DefaultRecheckInterval, DefaultRecheckBatch, DefaultRecheckSchedule)
	recheckCheckDefaults(t, (RecheckOptions{Interval: -time.Hour, BatchSize: -1, Schedule: -time.Minute}).withDefaults(),
		DefaultRecheckInterval, DefaultRecheckBatch, DefaultRecheckSchedule)

	// Заданные значения не затираются, Now передаётся как есть.
	now := func() time.Time { return time.Unix(0, 0) }
	got := RecheckOptions{
		Interval:  time.Hour,
		BatchSize: 10,
		Schedule:  time.Minute,
		Now:       now,
	}.withDefaults()
	recheckCheckDefaults(t, got, time.Hour, 10, time.Minute)
	if got.Now == nil || !got.Now().Equal(time.Unix(0, 0)) {
		t.Errorf("withDefaults затирает Now: nil=%v, значение %v", got.Now == nil, got.Now())
	}
}

// --- RecheckDaily ----------------------------------------------------------

// TestRecheckDailyImmediateDrain — первый дренаж выполняется сразу при
// старте, не дожидаясь суток: подбирает созревшее за простой.
func TestRecheckDailyImmediateDrain(t *testing.T) {
	pool := &recheckFakePool{tags: []int64{7}} // неполная пачка — дренаж закончен

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RecheckDaily(ctx, pool, RecheckOptions{
			Schedule: time.Hour, // тик не наступит: только стартовый дренаж
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
		t.Fatalf("RecheckDaily: %v, want nil при отмене ctx", err)
	}
}

// TestRecheckDailyLoop — полный дренаж пачками до исчерпания, ошибка
// пачки прерывает только свой дренаж (Warn), очередной тик продолжает;
// отмена ctx — чистый nil.
func TestRecheckDailyLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Дренаж 1 (старт): ошибка БД — Warn. Дренаж 2 (тик): полная пачка +
	// неполная — Info с суммой. Дальше — пустые множества.
	pool := &recheckFakePool{
		tags:     []int64{DefaultRecheckBatch, 3},
		execErrs: []error{errors.New("бд недоступна")},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RecheckDaily(ctx, pool, RecheckOptions{
			Schedule: 2 * time.Millisecond,
			Log:      logger,
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
		t.Fatalf("RecheckDaily: %v, want nil при отмене ctx", err)
	}

	log := buf.String()
	for _, want := range []string{
		"суточная перепроверка доменов", // старт
		"level=WARN", // ошибка дренажа не убила цикл
		"бд недоступна",
		fmt.Sprintf("domains=%d", DefaultRecheckBatch+3), // итог дренажа
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

// TestRecheckDailyCancelDuringDrain — отмена ctx во время дренажа — не
// тревога: без Warn, чистый nil (контракт OnRun, T-012).
func TestRecheckDailyCancelDuringDrain(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &recheckFakePool{execErrs: []error{context.Canceled}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до старта: первый же оператор дренажа провалится

	done := make(chan error, 1)
	go func() { done <- RecheckDaily(ctx, pool, RecheckOptions{Log: logger}) }()

	if err := <-done; err != nil {
		t.Fatalf("RecheckDaily: %v, want nil при отмене ctx", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("отмена во время дренажа — Warn:\n%s", buf.String())
	}
}

func TestRecheckDailyNilPool(t *testing.T) {
	err := RecheckDaily(context.Background(), nil, RecheckOptions{})
	if err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

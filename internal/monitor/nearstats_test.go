// Юнит-тесты счётчика серой зоны Laravel-находок (T-115) на фейковом
// pgx-пуле: точный SQL-текст и параметры (границы диапазона real),
// валидация порога, дефолты, ошибки запроса и деградации формы ответа
// (пустой, нечитаемая строка, ошибка итерации), отмена ctx, цикл
// NearStatsLoop (немедленный первый проход, gauge обновляется, Warn и
// переживание ошибки, успешный проход под отменой, чистый выход, no-op
// без реестра). Поведение против живой БД — dev-окружение (crawlerctl
// near + /metrics monitor).
package monitor

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
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/metrics"
)

// --- фейковый пул (Query — агрегат count) -----------------------------------

// nearstatsCall — записанный SQL-вызов с аргументами.
type nearstatsCall struct {
	sql  string
	args []any
}

// nearstatsFakePool — db.DB без БД: очередь ответов агрегата count(*)
// и ошибок, протокол вызовов. Под замком: цикл NearStatsLoop работает
// в горутине.
type nearstatsFakePool struct {
	mu     sync.Mutex
	counts []int64 // очередь значений count(*) (кончилась — 0)
	errs   []error // очередь ошибок (nil — вызов без ошибки)

	// Деградации формы ответа (стиль alertsPool): пустой ответ (Next
	// ложен), строка с нечитаемым типом (ошибка Scan), ошибка итерации
	// (rows.Err).
	empty   bool
	badRow  bool
	rowsErr error
	// ignoreCtx — отвечать успехом без проверки ctx: сценарий «проход
	// успешен, отмены во время него не было» для классификации в цикле.
	// Неизменяемо после конструирования (задаётся композит-литералом до
	// старта горутины цикла) — замок очередей его не защищает и не должен.
	ignoreCtx bool

	queries []nearstatsCall
}

func (p *nearstatsFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *nearstatsFakePool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("nearstatsFakePool: неожидаемый Exec: %s", sql)
}

func (p *nearstatsFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !p.ignoreCtx {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, nearstatsCall{sql: sql, args: args})
	if err := popErr(&p.errs); err != nil {
		return nil, err
	}
	if p.empty {
		return &nearstatsRows{err: p.rowsErr}, nil
	}
	if p.badRow {
		return &nearstatsRows{rows: [][]any{{"байты"}}, err: p.rowsErr}, nil
	}
	n := int64(0)
	if len(p.counts) > 0 {
		n = p.counts[0]
		p.counts = p.counts[1:]
	}
	return &nearstatsRows{rows: [][]any{{n}}, err: p.rowsErr}, nil
}

// stats — снимок числа вызовов (ожидание прогресса цикла).
func (p *nearstatsFakePool) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

// calls — снимок протокола вызовов.
func (p *nearstatsFakePool) calls() []nearstatsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]nearstatsCall(nil), p.queries...)
}

// nearstatsRows — pgx.Rows с одной строкой агрегата. err — ответ
// rows.Err (nil по умолчанию), тип значения строки проверяется в Scan.
type nearstatsRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error
}

func (r *nearstatsRows) Close()                                       { r.closed = true }
func (r *nearstatsRows) Err() error                                   { return r.err }
func (r *nearstatsRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *nearstatsRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *nearstatsRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *nearstatsRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("nearstatsRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("nearstatsRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	n, ok := row[0].(int64)
	if !ok {
		return fmt.Errorf("nearstatsRows: колонка 0: %T не int64", row[0])
	}
	*dest[0].(*int64) = n
	return nil
}

func (r *nearstatsRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *nearstatsRows) RawValues() [][]byte    { return nil }
func (r *nearstatsRows) Conn() *pgx.Conn        { return nil }
func (r *nearstatsRows) TypeMap() *pgtype.Map   { return nil }

// --- ожидаемый SQL-текст (контракт) ---------------------------------------

const wantNearStatsSQL = "SELECT count(*) FROM laravel_findings WHERE score >= $1::real AND score < $2::real"

// --- NearStatsOnce ---------------------------------------------------------

// TestNearStatsOnceSQL — один запрос с точными параметрами: границы
// серой зоны [min/2, min) приводятся к real.
func TestNearStatsOnceSQL(t *testing.T) {
	pool := &nearstatsFakePool{counts: []int64{42}}
	n, err := NearStatsOnce(context.Background(), pool, 0.6)
	if err != nil {
		t.Fatalf("NearStatsOnce: %v", err)
	}
	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("вызовов = %d, хочу 1", len(calls))
	}
	if calls[0].sql != wantNearStatsSQL {
		t.Errorf("SQL:\n%s\nwant:\n%s", calls[0].sql, wantNearStatsSQL)
	}
	wantArgs := []any{float32(0.3), float32(0.6)}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("args = %v, want %v", calls[0].args, wantArgs)
	}
	if n != 42 {
		t.Errorf("count = %d, want 42", n)
	}
}

// TestNearStatsOnceThreshold — порог вне (0, 1) — ошибка до БД
// (половина отрицательного или >= 1 бессмысленна).
func TestNearStatsOnceThreshold(t *testing.T) {
	pool := &nearstatsFakePool{}
	for _, m := range []float64{0, -0.1, 1, 1.5} {
		n, err := NearStatsOnce(context.Background(), pool, m)
		if err == nil {
			t.Errorf("NearStatsOnce(%v) = nil error, хочу ошибку порога", m)
		}
		if n != 0 {
			t.Errorf("NearStatsOnce(%v) = %d, want 0 при ошибке", m, n)
		}
	}
	if pool.stats() != 0 {
		t.Error("Query не должен вызываться при невалидном пороге")
	}
}

// TestNearStatsOnceNilPool — пул не задан: ошибка, не паника; значение
// ошибки — строго 0.
func TestNearStatsOnceNilPool(t *testing.T) {
	n, err := NearStatsOnce(context.Background(), nil, 0.6)
	if err == nil {
		t.Fatal("NearStatsOnce(nil pool) = nil error, хочу ошибку")
	}
	if n != 0 {
		t.Errorf("значение = %d, want 0 при ошибке", n)
	}
}

// TestNearStatsOnceErrors — деградации ответа агрегата: ошибка запроса,
// пустой ответ, нечитаемая строка, ошибка итерации — каждая с контекстом
// задачи в тексте и нулевым значением (не ±1).
func TestNearStatsOnceErrors(t *testing.T) {
	cases := []struct {
		name    string
		pool    *nearstatsFakePool
		wantErr string
	}{
		{
			"ошибка запроса",
			&nearstatsFakePool{errs: []error{errors.New("бд недоступна")}},
			"счётчик серой зоны: бд недоступна",
		},
		{"пустой ответ агрегата", &nearstatsFakePool{empty: true}, "пустой ответ агрегата"},
		{"нечитаемая строка", &nearstatsFakePool{badRow: true}, "чтение"},
		{
			"ошибка итерации",
			&nearstatsFakePool{rowsErr: errors.New("соединение сброшено")},
			"соединение сброшено",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			n, err := NearStatsOnce(context.Background(), tt.pool, 0.6)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ошибка = %v, want содержит %q", err, tt.wantErr)
			}
			if n != 0 {
				t.Errorf("значение = %d, want 0 при ошибке", n)
			}
		})
	}
}

// TestNearStatsOptionsDefaults — нулевой интервал — час; граница «≤ 0»:
// положительный наносекундный интервал дефолтом не заменяется.
func TestNearStatsOptionsDefaults(t *testing.T) {
	if got := (NearStatsOptions{}).withDefaults().Interval; got != DefaultNearStatsInterval {
		t.Errorf("дефолт интервала = %v, want %v", got, DefaultNearStatsInterval)
	}
	if got := (NearStatsOptions{Interval: time.Minute}).withDefaults().Interval; got != time.Minute {
		t.Errorf("явный интервал = %v, want time.Minute", got)
	}
	if got := (NearStatsOptions{Interval: time.Nanosecond}).withDefaults().Interval; got != time.Nanosecond {
		t.Errorf("интервал 1нс = %v, want 1нс (заменяется только ≤ 0)", got)
	}
}

// nearGaugeValue — текущее значение gauge серой зоны из реестра.
func nearGaugeValue(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "crawler_laravel_near_findings" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	t.Fatal("серия crawler_laravel_near_findings не найдена")
	return -1
}

// TestNearStatsLoopImmediatePass — первый проход сразу: gauge получает
// значение до первого тика; отмена — чистый nil.
func TestNearStatsLoopImmediatePass(t *testing.T) {
	pool := &nearstatsFakePool{counts: []int64{7}}
	reg := metrics.New("monitor-test")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NearStatsLoop(ctx, pool, reg, NearStatsOptions{MinScore: 0.6})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() == 0 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("первый проход не выполнен")
		}
		time.Sleep(time.Millisecond)
	}
	if got := nearGaugeValue(t, reg); got != 7 {
		t.Errorf("gauge = %v, want 7", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("NearStatsLoop: %v, want nil при отмене", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
}

// TestNearStatsLoopNoop — nil реестр или пул — no-op без паники
// (изолированные сборки, стиль CollectQueueLoop).
func TestNearStatsLoopNoop(t *testing.T) {
	pool := &nearstatsFakePool{}
	if err := NearStatsLoop(context.Background(), pool, nil, NearStatsOptions{MinScore: 0.6}); err != nil {
		t.Fatalf("nil реестр: %v, want nil", err)
	}
	reg := metrics.New("monitor-test")
	if err := NearStatsLoop(context.Background(), nil, reg, NearStatsOptions{MinScore: 0.6}); err != nil {
		t.Fatalf("nil пул: %v, want nil", err)
	}
	if pool.stats() != 0 {
		t.Error("проходов быть не должно")
	}
}

// TestNearStatsLoopErrorSurvives — ошибка прохода не убивает цикл:
// Warn в лог (отмена ctx ни при чём — она классифицирует только ошибки
// под уже отменённым контекстом), очередной проход считает заново.
func TestNearStatsLoopErrorSurvives(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &nearstatsFakePool{errs: []error{errors.New("бд недоступна")}}
	reg := metrics.New("monitor-test")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NearStatsLoop(ctx, pool, reg, NearStatsOptions{
			Interval: time.Millisecond, MinScore: 0.6, Log: logger,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 2 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до второго прохода: %d", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("NearStatsLoop: %v, want nil (ошибка прохода — не смерть цикла)", err)
	}
	log := buf.String()
	if !strings.Contains(log, "level=WARN") || !strings.Contains(log, "бд недоступна") {
		t.Errorf("ошибка прохода при живом ctx без Warn:\n%s", log)
	}
	if got := nearGaugeValue(t, reg); got != 0 {
		// Очередь значений пуста (counts не задан) — успешный проход
		// вернул 0; важнее, что gauge существует и цикл жив.
		t.Logf("gauge = %v после восстановления", got)
	}
}

// TestNearStatsLoopPassDuringCancel — успешный проход под уже отменённым
// ctx (пул игнорирует отмену) — штатный итог в gauge, а не тишина ветки
// «отмена во время прохода»: отмена классифицирует только ошибки.
func TestNearStatsLoopPassDuringCancel(t *testing.T) {
	pool := &nearstatsFakePool{counts: []int64{42}, ignoreCtx: true}
	reg := metrics.New("monitor-test")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- NearStatsLoop(ctx, pool, reg, NearStatsOptions{MinScore: 0.6})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("NearStatsLoop: %v, want nil при отмене", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
	if got := nearGaugeValue(t, reg); got != 42 {
		t.Errorf("gauge = %v, want 42: успешный проход под отменой — штатный итог", got)
	}
}

// Юнит-тесты совмещённой реконсиляции счётчиков конвейера (T-178, ТЗИ
// §7) на фейковом pgx-пуле с транзакциями: точные SQL-тексты и параметры
// операторов прохода (чтение старых значений → агрегаты → перезапись
// зонunnest-пачкой → upsert счётчиков), дрейф (старое/новое по
// позициям), ошибки шагов и отмена ctx, цикл ZoneStatsLoop (немедленный
// первый проход без дрейф-шума, Warn+gauge при дрейфе, single-flight
// T-161, чистый выход), сверка VerifyCounters. Поведение против живой
// БД — интеграционные тесты (test/integration/counters_test.go).
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

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

// --- фейковый пул с транзакциями -----------------------------------------

// zonestatsCall — записанный SQL-вызов с аргументами.
type zonestatsCall struct {
	sql  string
	args []any
}

// zonestatsFakeRows — pgx.Rows с заготовленными строками; Scan
// позиционный, в порядке колонок SQL.
type zonestatsFakeRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error
}

func zonestatsRows(rows ...[]any) *zonestatsFakeRows {
	return &zonestatsFakeRows{rows: rows}
}

func (r *zonestatsFakeRows) Close() { r.closed = true }

func (r *zonestatsFakeRows) Err() error { return r.err }

func (r *zonestatsFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *zonestatsFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *zonestatsFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *zonestatsFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("zonestatsFakeRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("zonestatsFakeRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(row[i]))
	}
	return nil
}

func (r *zonestatsFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *zonestatsFakeRows) RawValues() [][]byte { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая позиционным Scan.
func (r *zonestatsFakeRows) Conn() *pgx.Conn      { return nil }
func (r *zonestatsFakeRows) TypeMap() *pgtype.Map { return nil }

// zonestatsStep — заготовленный результат одного Query прохода.
type zonestatsStep struct {
	rows *zonestatsFakeRows
	err  error
}

// zonestatsFakePool — ZoneStatsDB без БД: очередь результатов Query (в
// порядке операторов прохода) и ошибок, протокол вызовов, счётчики
// транзакций. Всё под замком: цикл работает в горутине, тест ожидает
// прогресс по счётчикам. delay — пауза каждого Query (медленный проход
// для single-flight теста T-161).
type zonestatsFakePool struct {
	mu        sync.Mutex
	steps     []zonestatsStep
	commitErr error
	delay     time.Duration

	queries   []zonestatsCall
	begins    int
	commits   int
	rollbacks int
}

func (p *zonestatsFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *zonestatsFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

func (p *zonestatsFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	if d := p.delaySnapshot(); d > 0 {
		time.Sleep(d)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, zonestatsCall{sql: sql, args: args})
	if len(p.steps) == 0 {
		return zonestatsRows(), nil
	}
	step := p.steps[0]
	p.steps = p.steps[1:]
	if step.err != nil {
		return nil, step.err
	}
	if step.rows == nil {
		return zonestatsRows(), nil
	}
	return step.rows, nil
}

func (p *zonestatsFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	return &zonestatsFakeTx{pool: p}, nil
}

// stats — снимок числа запросов под замком (ожидание прогресса цикла).
func (p *zonestatsFakePool) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

// calls — снимок протокола вызовов под замком.
func (p *zonestatsFakePool) calls() []zonestatsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]zonestatsCall(nil), p.queries...)
}

// txCounts — счётчики транзакций.
func (p *zonestatsFakePool) txCounts() (begins, commits, rollbacks int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.begins, p.commits, p.rollbacks
}

func (p *zonestatsFakePool) delaySnapshot() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.delay
}

func (p *zonestatsFakePool) setDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

// zonestatsFakeTx — транзакция поверх фейкового пула (Query/Exec — в
// протокол пула; Commit/Rollback — счётчики). Остальные методы pgx.Tx
// проход не использует.
type zonestatsFakeTx struct {
	pool *zonestatsFakePool
	pgx.Tx
}

func (t *zonestatsFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.pool.Query(ctx, sql, args...)
}

func (t *zonestatsFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.pool.Exec(ctx, sql, args...)
}

func (t *zonestatsFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.commits++
	return t.pool.commitErr
}

func (t *zonestatsFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.rollbacks++
	return nil
}

// --- заготовки прохода ------------------------------------------------------
//
// Шаги одного прохода (11 операторов): старые зоны, старые счётчики,
// старые суточные строки проверок и temp_error (T-179/T-182), per-zone
// агрегат (16 колонок: зона, 4 состояния, 8 статусов, бит, суточное
// окно, temp-числитель), laravel-агрегат, история, перезапись зон
// (RETURNING новых значений), upsert счётчиков (RETURNING новых
// значений), upsert обеих суточных строк (в порядке checks: →
// temp_error: — общий с инкрементами record.go).

// aggRowOf — строка per-zone агрегата: зона, total, pending, queued,
// checked, затем легенда статусов по порядку statusAggCols, затем бит,
// затем суточное окно проверок и его temp-числитель (T-179/T-182;
// вариативные аргументы — по умолчанию нули, чтобы базовые проходы
// тестов не указывали их явно: [checks] либо [checks, temp]).
func aggRowOf(zone int16, total, pending, queued, checked int64, statuses []int64, render int64, daily ...int64) []any {
	if len(statuses) != len(statusAggCols) {
		panic("aggRowOf: легенда статусов неполна")
	}
	if len(daily) > 2 {
		panic("aggRowOf: суточных колонок две (T-179/T-182)")
	}
	var today, temp int64
	if len(daily) > 0 {
		today = daily[0]
	}
	if len(daily) > 1 {
		temp = daily[1]
	}
	row := []any{zone, total, pending, queued, checked}
	for _, s := range statuses {
		row = append(row, s)
	}
	return append(row, render, today, temp)
}

// zoneValsOf — строка readZonesSQL/recountZonesSQL RETURNING.
func zoneValsOf(id int16, total, pending, queued, checked, laravel int64) []any {
	return []any{id, total, pending, queued, checked, laravel}
}

// counterRowOf — строка readCountersSQL/recountCountersSQL RETURNING.
func counterRowOf(name string, v int64) []any { return []any{name, v} }

// stepsOfPass — очередь результатов одного полного прохода. Шаги:
// старые зоны, старые счётчики, старые суточные строки проверок и
// temp_error (T-179/T-182; по умолчанию пустые наборы — строк дня нет,
// проход создаёт: заполнение без дрейфа), per-zone агрегат,
// laravel-агрегат, история, перезапись зон (RETURNING новых значений =
// zoneNew), upsert счётчиков (RETURNING новых значений), upsert суточных
// строк (значения 0 — тесты с ненулевым окном мутируют шаг, как мутируют
// агрегат).
func stepsOfPass(zoneOld, zoneNew, agg []any, laravel *zonestatsFakeRows, oldCounters, newCounters *zonestatsFakeRows) []zonestatsStep {
	return []zonestatsStep{
		{rows: zonestatsRows(zoneOld)},
		{rows: oldCounters},
		{},
		{},
		{rows: zonestatsRows(agg)},
		{rows: laravel},
		{rows: zonestatsRows([]any{int64(0), int64(0)})}, // история
		{rows: zonestatsRows(zoneNew)},
		{rows: newCounters},
		{rows: zonestatsRows([]any{int64(0)})},
		{rows: zonestatsRows([]any{int64(0)})},
	}
}

// fullCounterRows — полный ответ upsert/чтения счётчиков: легенда
// CounterNames со значениями из карты (отсутствие — 0).
func fullCounterRows(values map[string]int64) *zonestatsFakeRows {
	rows := make([][]any, 0, len(CounterNames()))
	for _, name := range CounterNames() {
		rows = append(rows, counterRowOf(name, values[name]))
	}
	return zonestatsRows(rows...)
}

// zonestatsFixedClock — часы с фиксированным «сейчас».
func zonestatsFixedClock() (time.Time, func() time.Time) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	return now, func() time.Time { return now }
}

// --- ZoneStatsOnce ---------------------------------------------------------

// TestZoneStatsOnceSQL — последовательность операторов прохода в одной
// транзакции с точными текстами: чтение старых зон, счётчиков и обеих
// суточных строк → агрегат (единственный скан domains) → laravel →
// история → перезапись зон (блокировки первыми) → upsert счётчиков →
// upsert суточных строк (checks:, затем temp_error:) → Commit.
func TestZoneStatsOnceSQL(t *testing.T) {
	now, clock := zonestatsFixedClock()
	statuses := make([]int64, len(statusAggCols))
	statuses[4] = 5 // alive
	statuses[5] = 1 // temp_error
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 2, 8, 1),
		zonestatsRows([]any{int16(7), int64(1)}),
		fullCounterRows(map[string]int64{"status_alive": 5, "render_pending": 2}),
		fullCounterRows(map[string]int64{"status_alive": 5, "render_pending": 2}),
	)}
	// Суточные строки (T-179/T-182): старое checks 7 / temp_error 2
	// (инкременты обогнали живое окно на один temp_error-ретрай), новые —
	// суммы FILTER-колонок агрегата (8 и 1).
	pool.steps[2].rows = zonestatsRows([]any{int64(7)})
	pool.steps[3].rows = zonestatsRows([]any{int64(2)})
	pool.steps[9].rows = zonestatsRows([]any{int64(8)})
	pool.steps[10].rows = zonestatsRows([]any{int64(1)})

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{Now: clock})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 11 {
		t.Fatalf("запросов = %d, want 11 (чтения ×4, агрегаты ×3, перезапись зон, upsert ×3)", len(calls))
	}
	wantSQL := []string{readZonesSQL, readCountersSQL, readDailyChecksSQL, readDailyTempSQL, recountAggQuerySQL, laravelAggQuerySQL, historyAggSQL, recountZonesSQL, recountCountersSQL, recountDailyChecksSQL, recountDailyTempSQL}
	for i, want := range wantSQL {
		if calls[i].sql != want {
			t.Errorf("запрос %d:\n%s\nwant:\n%s", i, calls[i].sql, want)
		}
	}
	// Легенда счётчиков — полная, в детерминированном порядке.
	if !reflect.DeepEqual(calls[1].args, []any{CounterNames()}) {
		t.Errorf("аргументы чтения счётчиков = %v, want %v", calls[1].args, CounterNames())
	}
	// Перезапись зон: unnest-пачка — все параметры в порядке SQL
	// $1 ids, $2 total, $3 pending, $4 queued, $5 checked, $6 момент,
	// $7 laravel — из laravel-агрегата (шаг 5), не из RETURNING зон
	// (перепутанные колонки тест обязан ловить, ревью R2).
	wantZone := []any{[]int16{7}, []int64{10}, []int64{4}, []int64{2}, []int64{3}, now, []int64{1}}
	if !reflect.DeepEqual(calls[7].args, wantZone) {
		t.Errorf("unnest-пачка зон = %#v\nwant %#v", calls[7].args, wantZone)
	}
	// Upsert: имена полной легенды + значения + момент.
	if got := calls[8].args[0]; !reflect.DeepEqual(got, CounterNames()) {
		t.Errorf("имена upsert = %v, want легенда CounterNames", got)
	}
	if calls[8].args[2] != any(now) {
		t.Errorf("updated_at upsert = %#v, want %v", calls[8].args[2], now)
	}
	// Значения upsert соответствуют агрегату: легенда по порядку.
	wantVals := calls[8].args[1].([]int64)
	if wantVals[len(statusAggCols)] != 2 { // render_pending после статусов
		t.Errorf("render_pending в upsert = %d, want 2", wantVals[len(statusAggCols)])
	}
	// Суточные строки (T-179/T-182): значения — суммы FILTER-колонок
	// агрегата, момент — часы прохода; имена дней вычисляет БД — в текстах
	// операторов current_date, параметров имени нет; порядок записей
	// «checks: → temp_error:» общий с инкрементами record.go.
	if !reflect.DeepEqual(calls[9].args, []any{int64(8), any(now)}) {
		t.Errorf("аргументы upsert строки проверок = %#v, want значение 8 + момент", calls[9].args)
	}
	if !reflect.DeepEqual(calls[10].args, []any{int64(1), any(now)}) {
		t.Errorf("аргументы upsert temp-строки = %#v, want значение 1 + момент", calls[10].args)
	}
	if !strings.Contains(recountDailyChecksSQL, "'checks:' || to_char(current_date, 'YYYY-MM-DD')") {
		t.Errorf("recountDailyChecksSQL без имени дня от БД:\n%s", recountDailyChecksSQL)
	}
	if !strings.Contains(recountDailyTempSQL, "'temp_error:' || to_char(current_date, 'YYYY-MM-DD')") {
		t.Errorf("recountDailyTempSQL без имени дня от БД:\n%s", recountDailyTempSQL)
	}

	begins, commits, _ := pool.txCounts()
	if begins != 1 || commits != 1 {
		t.Errorf("транзакций: begin=%d commit=%d, want 1/1", begins, commits)
	}
	if res.Zones != 1 {
		t.Errorf("Zones = %d, want 1", res.Zones)
	}
	// Позиции дрейфа: 5 колонок зоны + дельта-ведомые глобальные +
	// суточные строки (T-179/T-182).
	if len(res.Positions) != 5+len(driftCounterNames())+2 {
		t.Fatalf("позиций = %d, want %d", len(res.Positions), 5+len(driftCounterNames())+2)
	}
	byName := make(map[string]CounterPosition, len(res.Positions))
	for _, p := range res.Positions {
		byName[p.Counter] = p
	}
	if p := byName["zone:7:pending"]; p.Old != 4 || p.New != 4 || p.Drift() != 0 {
		t.Errorf("zone:7:pending = %+v, want old=new=4", p)
	}
	if _, ok := byName["global:status_alive"]; ok {
		t.Error("status_* в позициях дрейфа: распределение статусов пишет только реконсиляция")
	}
	if p := byName["global:render_pending"]; p.Old != 2 || p.New != 2 {
		t.Errorf("global:render_pending = %+v, want old=new=2", p)
	}
	// Отсутствующая строка счётчика — старое 0 (первый проход заполняет).
	if p := byName["global:laravel_appeared_total"]; p.Old != 0 || p.New != 0 {
		t.Errorf("global:laravel_appeared_total = %+v, want old=new=0 (строки нет)", p)
	}
	// Суточные позиции: дрейф |7 − 8| = 1 — события записи сверх живого
	// окна (повтор суток, TZ_slow_done_today §5.1); temp-числитель
	// |2 − 1| — ретрай закрыл ошибку, событие осталось в истории
	// (TZ_slow_dns_share §5.1).
	if p := byName["daily:checks"]; p.Old != 7 || p.New != 8 || p.Drift() != 1 {
		t.Errorf("daily:checks = %+v, want old=7 new=8 drift=1", p)
	}
	if p := byName["daily:temp_error"]; p.Old != 2 || p.New != 1 || p.Drift() != 1 {
		t.Errorf("daily:temp_error = %+v, want old=2 new=1 drift=1", p)
	}
}

// TestZoneStatsOnceDrift — дрейф: старое ≠ новое по колонке зоны и
// глобальному счётчику; Drift() — |Δ|; новая в агрегате зона без старой
// строки — старое 0.
func TestZoneStatsOnceDrift(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	// Зона 9 в старом чтении с нулями — «новая зона» (счётчики были
	// обнулены рестартом на миграции).
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 6, 2, 3, 1),
		aggRowOf(7, 10, 6, 2, 3, statuses, 4),
		zonestatsRows(),
		fullCounterRows(map[string]int64{"render_pending": 2}),
		fullCounterRows(map[string]int64{"render_pending": 4}),
	)}
	pool.steps[0].rows = zonestatsRows(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(9, 0, 0, 0, 0, 0))
	pool.steps[4].rows = zonestatsRows(
		aggRowOf(7, 10, 6, 2, 3, statuses, 4),
		aggRowOf(9, 2, 2, 0, 0, make([]int64, len(statusAggCols)), 0))
	pool.steps[7].rows = zonestatsRows(
		zoneValsOf(7, 10, 6, 2, 3, 1),
		zoneValsOf(9, 2, 2, 0, 0, 0))

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if res.Zones != 2 {
		t.Fatalf("Zones = %d, want 2", res.Zones)
	}
	byName := make(map[string]CounterPosition, len(res.Positions))
	for _, p := range res.Positions {
		byName[p.Counter] = p
	}
	if p := byName["zone:7:pending"]; p.Old != 4 || p.New != 6 || p.Drift() != 2 {
		t.Errorf("zone:7:pending = %+v, want old=4 new=6 drift=2", p)
	}
	// Зона 9 пришла из нулей (рестарт на миграции) — старое 0.
	if p := byName["zone:9:total"]; p.Old != 0 || p.New != 2 || p.Drift() != 2 {
		t.Errorf("zone:9:total = %+v, want old=0 new=2", p)
	}
	if p := byName["global:render_pending"]; p.Old != 2 || p.New != 4 || p.Drift() != 2 {
		t.Errorf("global:render_pending = %+v, want old=2 new=4", p)
	}
	drifted := 0
	for _, p := range res.Positions {
		if p.Drift() > 0 {
			drifted++
		}
	}
	if drifted != 4 { // pending зоны 7, total+pending зоны 9, render_pending
		t.Errorf("позиций с дрейфом = %d, want 4", drifted)
	}
}

// TestZoneStatsOnceDailyChecks — суточные строки прохода (T-179/T-182):
// строки дня ещё нет — проход создаёт её значением окна, заполнение
// дрейфом не считается (Old := New; расхождение «0 против in-flight
// DNS-записей» к инкрементам отношения не имеет); существующая строка с
// расхождением — дрейф, позиции daily:checks/daily:temp_error. Числитель
// temp_error расходится с окном дополнительно ретраями, закрывшими
// ошибку (событие остаётся в истории, TZ_slow_dns_share §5.1) — окно 1
// против инкрементов 2.
func TestZoneStatsOnceDailyChecks(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	statuses[4] = 3 // alive
	statuses[5] = 1 // temp_error

	dailyOf := func(res ZoneStatsResult, name string) CounterPosition {
		t.Helper()
		for _, p := range res.Positions {
			if p.Counter == name {
				return p
			}
		}
		t.Fatalf("позиция %s отсутствует в итогах прохода", name)
		return CounterPosition{}
	}

	// Строк дня нет, живое окно 3 / temp-окно 1 (первый проход после
	// полуночи): создание — не дрейф.
	create := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 3, 1),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	// stepsOfPass сеет пустые старые суточные строки (шаги 2–3) —
	// оставляем; новые значения шагов 9–10 — окна 3 и 1.
	create[9].rows = zonestatsRows([]any{int64(3)})
	create[10].rows = zonestatsRows([]any{int64(1)})
	res, err := ZoneStatsOnce(context.Background(), &zonestatsFakePool{steps: create}, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if p := dailyOf(res, "daily:checks"); p.New != 3 || p.Drift() != 0 {
		t.Errorf("создание строки дня: daily:checks = %+v, want new=3 drift=0 (заполнение)", p)
	}
	if p := dailyOf(res, "daily:temp_error"); p.New != 1 || p.Drift() != 0 {
		t.Errorf("создание temp-строки: daily:temp_error = %+v, want new=1 drift=0 (заполнение)", p)
	}

	// Строки есть (инкременты обогнали окно на два повтора; temp_error
	// закрывался ретраем — событие осталось): дрейф 2 и 1.
	drift := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 3, 1),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	drift[2].rows = zonestatsRows([]any{int64(5)})
	drift[3].rows = zonestatsRows([]any{int64(2)})
	drift[9].rows = zonestatsRows([]any{int64(3)})
	drift[10].rows = zonestatsRows([]any{int64(1)})
	res, err = ZoneStatsOnce(context.Background(), &zonestatsFakePool{steps: drift}, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if p := dailyOf(res, "daily:checks"); p.Old != 5 || p.New != 3 || p.Drift() != 2 {
		t.Errorf("дрейф строки дня: daily:checks = %+v, want old=5 new=3 drift=2", p)
	}
	if p := dailyOf(res, "daily:temp_error"); p.Old != 2 || p.New != 1 || p.Drift() != 1 {
		t.Errorf("дрейф temp-строки: daily:temp_error = %+v, want old=2 new=1 drift=1", p)
	}
}

// TestZoneStatsOnceZoneFilter — фильтр зоны (recount --zone): чтение
// старых зон ограничено, перезапись — только выбранной зоны.
func TestZoneStatsOnceZoneFilter(t *testing.T) {
	zone := int16(7)
	statuses := make([]int64, len(statusAggCols))
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 2),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil),
	)}

	if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{Zone: &zone}); err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	calls := pool.calls()
	if got, ok := calls[0].args[0].(*int16); !ok || got == nil || *got != zone {
		t.Errorf("фильтр зоны чтения старых зон = %#v, want *int16(7)", calls[0].args[0])
	}
	if got := calls[7].args[0]; !reflect.DeepEqual(got, []int16{7}) {
		t.Errorf("unnest зоны перезаписи = %#v, want [7] (перезапись зон — шаг 7)", got)
	}
}

// TestZoneStatsOnceEmpty — зон нет: пустые чтения, нулевой итог,
// транзакция выполняется (строки счётчиков создаёт upsert прохода).
func TestZoneStatsOnceEmpty(t *testing.T) {
	// Пачка зон пуста — оператор перезаписи не выполняется (пропуск
	// no-op в writeZonePositions), шаги: чтения ×4, агрегаты ×3,
	// upsert ×3.
	pool := &zonestatsFakePool{steps: []zonestatsStep{
		{}, {}, {}, {}, {rows: zonestatsRows()}, {rows: zonestatsRows()},
		{rows: zonestatsRows([]any{int64(0), int64(0)})},
		{rows: fullCounterRows(nil)},
		{rows: zonestatsRows([]any{int64(0)})},
		{rows: zonestatsRows([]any{int64(0)})},
	}}

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if res.Zones != 0 {
		t.Errorf("Zones = %d, want 0", res.Zones)
	}
	if n := pool.stats(); n != 10 {
		t.Errorf("запросов = %d, want 10 (перезапись зон пропущена)", n)
	}
	if len(res.Positions) != len(driftCounterNames())+len(dailyStreamingPositions()) {
		t.Errorf("позиций = %d, want глобальные + суточные (%d)", len(res.Positions), len(driftCounterNames())+len(dailyStreamingPositions()))
	}
}

// TestZoneStatsOnceErrors — ошибка каждого шага обёрнута контекстом
// задачи; отмена ctx сохраняется errors.Is; Commit тоже может падать.
func TestZoneStatsOnceErrors(t *testing.T) {
	for i := 0; i < 11; i++ {
		steps := stepsOfPass(
			zoneValsOf(7, 10, 4, 2, 3, 1),
			zoneValsOf(7, 10, 4, 2, 3, 1),
			aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
			zonestatsRows(),
			fullCounterRows(nil),
			fullCounterRows(nil))
		steps[i].err = errors.New("бд недоступна")
		pool := &zonestatsFakePool{steps: steps}
		_, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
		if err == nil || !strings.Contains(err.Error(), "monitor: реконсиляция счётчиков") ||
			!strings.Contains(err.Error(), "бд недоступна") {
			t.Errorf("шаг %d: ошибка = %v, want контекст задачи и исходная", i, err)
		}
		if _, _, rollbacks := pool.txCounts(); rollbacks != 1 {
			t.Errorf("шаг %d: откатов = %d, want 1 (сбой до Commit)", i, rollbacks)
		}
	}

	cancelSteps := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	cancelSteps[4].err = context.Canceled // шаг агрегата — тяжёлое чтение
	cancelPool := &zonestatsFakePool{steps: cancelSteps}
	if _, err := ZoneStatsOnce(context.Background(), cancelPool, ZoneStatsOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
	}

	commitPool := &zonestatsFakePool{
		commitErr: errors.New("fix failed"),
		steps: stepsOfPass(
			zoneValsOf(7, 10, 4, 2, 3, 1),
			zoneValsOf(7, 10, 4, 2, 3, 1),
			aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
			zonestatsRows(),
			fullCounterRows(nil),
			fullCounterRows(nil)),
	}
	if _, err := ZoneStatsOnce(context.Background(), commitPool, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "фиксация") {
		t.Errorf("ошибка = %v, want сбой фиксации", err)
	}
}

func TestZoneStatsOnceNilPool(t *testing.T) {
	if _, err := ZoneStatsOnce(context.Background(), nil, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// --- ZoneStatsOptions ------------------------------------------------------

// TestZoneStatsOptionsDefaults — нулевые и отрицательные значения —
// дефолт; заданные не затираются, Now передаётся как есть. Дефолт
// pinned литералом 30м (T-199: решение оператора 26.10 по замеру окна
// медленных после 3bffc62 — 48 % медленного времени БД при 5 мин;
// прежние 5м — T-161, замер §12).
func TestZoneStatsOptionsDefaults(t *testing.T) {
	if DefaultZoneStatsInterval != 30*time.Minute {
		t.Errorf("DefaultZoneStatsInterval = %v, want 30m (T-199)", DefaultZoneStatsInterval)
	}
	got := (ZoneStatsOptions{}).withDefaults()
	if got.Interval != DefaultZoneStatsInterval {
		t.Errorf("Interval = %v, want дефолт %v", got.Interval, DefaultZoneStatsInterval)
	}
	got = (ZoneStatsOptions{Interval: -time.Second}).withDefaults()
	if got.Interval != DefaultZoneStatsInterval {
		t.Errorf("Interval = %v после отрицательного, want дефолт %v", got.Interval, DefaultZoneStatsInterval)
	}
	got = (ZoneStatsOptions{Interval: time.Nanosecond}).withDefaults()
	if got.Interval != time.Nanosecond {
		t.Errorf("Interval = %v после 1нс, want 1нс (заменяется только ≤ 0)", got.Interval)
	}
	now := func() time.Time { return time.Unix(0, 0) }
	got = ZoneStatsOptions{Interval: 30 * time.Second, Now: now}.withDefaults()
	if got.Interval != 30*time.Second || got.Now == nil || !got.Now().Equal(time.Unix(0, 0)) {
		t.Errorf("withDefaults затирает заданные значения: %+v", got)
	}
}

// --- ZoneStatsLoop ---------------------------------------------------------

// driftGaugeValue — текущее значение crawler_counters_drift{counter}.
func driftGaugeValue(t *testing.T, reg *metrics.Registry, counter string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_counters_drift" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "counter" && l.GetValue() == counter {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}

// TestZoneStatsLoopImmediatePass — первый проход выполняется сразу при
// старте, не дожидаясь тика; дрейф первого прохода (старые нули) — без
// Warn и метрик (заполняет, ТЗИ §7.1.3).
func TestZoneStatsLoopImmediatePass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := metrics.New("monitor")

	statuses := make([]int64, len(statusAggCols))
	// Дрейф есть (старые нули, новые значения) — первый проход молчит.
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 0, 0, 0, 0, 0),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 5),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(map[string]int64{"status_alive": 5}))}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: time.Hour, // тик не наступит: только стартовый проход
			Log:      logger,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 11 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("стартовый проход не выполнен")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if n := pool.stats(); n != 11 {
		t.Errorf("запросов = %d, want 11: без тика повторных проходов нет", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil при отмене ctx", err)
	}
	log := buf.String()
	if strings.Contains(log, "level=WARN") {
		t.Errorf("первый проход дал Warn (дрейф заполнения — не тревога):\n%s", log)
	}
	if driftGaugeValue(t, reg, "zone:7:total") != 0 {
		t.Errorf("gauge дрейфа первого прохода не нулевой/отсутствует: метрики первого прохода не поднимаются")
	}
}

// zonestatsSyncBuf — bytes.Buffer под замком: цикл пишет лог в своей
// горутине, тест читает его до завершения цикла.
type zonestatsSyncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *zonestatsSyncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *zonestatsSyncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *zonestatsSyncBuf) contains(s string) bool {
	return strings.Contains(b.String(), s)
}

// waitCond — ожидание условия с таймаутом.
func waitCond(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("условие не наступило за %s: %s", timeout, what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestZoneStatsLoopFailedFirstPass — сбойный первый проход (БД не
// готова) не снимает «first» (ревью R1 P3-5): следующий успешный проход
// заполняет нули без Warn — иначе рестарт на поднятой БД репортировал
// бы дрейф заполнения как аномалию.
func TestZoneStatsLoopFailedFirstPass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := metrics.New("monitor")

	statuses := make([]int64, len(statusAggCols))
	fill := stepsOfPass(
		zoneValsOf(7, 0, 0, 0, 0, 0),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	// Первый проход: ровно четыре шага чтения до сбоя на агрегате (хвост не
	// оставляем — иначе второй проход ест заготовку со сдвигом).
	fail := []zonestatsStep{
		{rows: zonestatsRows(zoneValsOf(7, 0, 0, 0, 0, 0))}, // старые зоны
		{rows: fullCounterRows(nil)},                        // старые счётчики
		{},                                                  // старая строка проверок (T-179)
		{},                                                  // старая temp-строка (T-182)
		{err: errors.New("бд недоступна")}, // агрегат — сбой
	}
	pool := &zonestatsFakePool{steps: append(fail, fill...)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	waitCond(t, 2*time.Second, func() bool { return pool.stats() >= 16 }, "проход после сбойного старта")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v", err)
	}
	log := buf.String()
	if !strings.Contains(log, "level=WARN") || !strings.Contains(log, "бд недоступна") {
		t.Errorf("сбойный старт без Warn с причиной:\n%s", log)
	}
	if strings.Contains(log, "реконсиляция: дрейф") {
		t.Errorf("заполняющий проход после сбоя репортирован как дрейф:\n%s", log)
	}
	if driftGaugeValue(t, reg, "zone:7:total") != 0 {
		t.Errorf("gauge дрейфа поднят заполняющим проходом после сбоя")
	}
}

// TestZoneStatsLoopTickerDriftWarn — проход по тику с ненулевым дрейфом
// (не первый): Warn с числом позиций и gauge |Δ| по всем позициям
// (стационар — 0). Синхронизация по самому Warn в логе (не по счётчику
// запросов): третий проход не должен перезаписать gauge нулями до
// проверок — после Warn цикл отменяется и дожидается завершения
// (фикс флейка, ревью P3-1).
func TestZoneStatsLoopTickerDriftWarn(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := slog.New(slog.NewTextHandler(slogBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := metrics.New("monitor")

	statuses := make([]int64, len(statusAggCols))
	noDrift := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 5),
		zonestatsRows(),
		fullCounterRows(map[string]int64{"render_pending": 5}),
		fullCounterRows(map[string]int64{"render_pending": 5}))
	withDrift := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 5, 2, 3, 1),
		aggRowOf(7, 10, 5, 2, 3, statuses, 6),
		zonestatsRows(),
		fullCounterRows(map[string]int64{"render_pending": 5}),
		fullCounterRows(map[string]int64{"render_pending": 6}))
	// Суточные строки второго прохода — ненулевой «дрейф» (потоковые
	// позиции, T-179/T-182): gauge их экспонирует, но Warn по ним не
	// поднимается (шум под живой нагрузкой — reportDrift).
	withDrift[2].rows = zonestatsRows([]any{int64(12)})
	withDrift[3].rows = zonestatsRows([]any{int64(8)})
	withDrift[9].rows = zonestatsRows([]any{int64(7)})
	withDrift[10].rows = zonestatsRows([]any{int64(5)})
	pool := &zonestatsFakePool{steps: append(noDrift, withDrift...)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Второй проход (по тику) несёт дрейф — ждём сам Warn: отчетDrift
	// уже выставил gauge до записи строки.
	waitCond(t, 2*time.Second, func() bool { return slogBuf.contains("реконсиляция: дрейф") }, "Warn дрейфа")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v", err)
	}

	log := slogBuf.String()
	for _, want := range []string{
		"цикл реконсиляции счётчиков", // старт
		"level=WARN", // дрейф второго прохода
		"positions", "sum_abs",
		"zone:7:pending: 4 → 5",
		"global:render_pending: 5 → 6",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
	// Суточные позиции — вне Warn (потоковые, reportDrift): в топе
	// дрейфа их быть не должно, gauge при этом экспонирует |12 − 7| и
	// |8 − 5|.
	for _, streaming := range dailyStreamingPositions() {
		if strings.Contains(log, streaming) {
			t.Errorf("суточная позиция %s в Warn дрейфа (шум):\n%s", streaming, log)
		}
	}
	if v := driftGaugeValue(t, reg, "zone:7:pending"); v != 1 {
		t.Errorf("drift{zone:7:pending} = %v, want 1", v)
	}
	if v := driftGaugeValue(t, reg, "zone:7:queued"); v != 0 {
		t.Errorf("drift{zone:7:queued} = %v, want 0 (стационарная позиция)", v)
	}
	if v := driftGaugeValue(t, reg, "global:render_pending"); v != 1 {
		t.Errorf("drift{global:render_pending} = %v, want 1", v)
	}
	if v := driftGaugeValue(t, reg, dailyChecksCounter); v != 5 {
		t.Errorf("drift{daily:checks} = %v, want 5 (gauge ведётся и вне Warn)", v)
	}
	if v := driftGaugeValue(t, reg, dailyTempCounter); v != 3 {
		t.Errorf("drift{daily:temp_error} = %v, want 3 (gauge ведётся и вне Warn)", v)
	}
}

// TestZoneStatsLoopCancelDuringPass — отмена ctx во время прохода — не
// тревога: без Warn, чистый nil (контракт OnRun, T-012).
func TestZoneStatsLoopCancelDuringPass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	steps := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	steps[2].err = context.Canceled
	pool := &zonestatsFakePool{steps: steps}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до старта: проход провалится отменой

	done := make(chan error, 1)
	go func() { done <- ZoneStatsLoop(ctx, pool, nil, ZoneStatsOptions{Log: logger}) }()

	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil при отмене ctx", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("отмена во время прохода — Warn:\n%s", buf.String())
	}
}

func TestZoneStatsLoopNilPool(t *testing.T) {
	err := ZoneStatsLoop(context.Background(), nil, nil, ZoneStatsOptions{})
	if err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// TestZoneStatsLoopPoolWithoutTx — пул без транзакций (db.DB) — ошибка
// старта: проход требует одного снапшота. Обёртка скрывает Begin фейка
// (встраивание продвигало бы и его — assertion прошёл бы).
func TestZoneStatsLoopPoolWithoutTx(t *testing.T) {
	inner := &zonestatsFakePool{}
	pool := struct{ db.DB }{DB: inner} // только db.DB, без Begin
	err := ZoneStatsLoop(context.Background(), pool, nil, ZoneStatsOptions{})
	if err == nil || !strings.Contains(err.Error(), "ZoneStatsDB") {
		t.Errorf("ошибка = %v, want «пул без транзакций»", err)
	}
}

// --- single-flight (T-161) --------------------------------------------------

// zonestatsSkippedValue — текущее значение счётчика
// crawler_zonestats_skipped_ticks_total из реестра.
func zonestatsSkippedValue(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_zonestats_skipped_ticks_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// zonestatsHistogramSamples — число наблюдений
// crawler_counters_recount_seconds (каждый проход обязан попасть).
func zonestatsHistogramSamples(t *testing.T, reg *metrics.Registry) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_counters_recount_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

// passSteps — заготовки N «тихих» проходов (нулевой дрейф).
func passSteps(n int) []zonestatsStep {
	statuses := make([]int64, len(statusAggCols))
	out := make([]zonestatsStep, 0, n*11)
	for i := 0; i < n; i++ {
		out = append(out, stepsOfPass(
			zoneValsOf(7, 10, 4, 2, 3, 1),
			zoneValsOf(7, 10, 4, 2, 3, 1),
			aggRowOf(7, 10, 4, 2, 3, statuses, 0),
			zonestatsRows(),
			fullCounterRows(nil),
			fullCounterRows(nil))...)
	}
	return out
}

// TestZoneStatsLoopSingleFlight — проход дольше интервала: накопившийся
// тик не запускает новый проход сразу за медленным — Warn «дольше
// интервала», счётчик пропусков, гистограмма длительности; следующий
// проход — по следующему тику, цикл живёт.
func TestZoneStatsLoopSingleFlight(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := slog.New(slog.NewTextHandler(slogBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := metrics.New("monitor")
	pool := &zonestatsFakePool{steps: passSteps(12)}
	pool.setDelay(120 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: 50 * time.Millisecond,
			Log:      logger,
		})
	}()

	waitCond(t, 3*time.Second, func() bool { return slogBuf.contains("дольше интервала") }, "Warn медленного прохода")
	if got := zonestatsSkippedValue(t, reg); got != 1 {
		t.Errorf("skipped_ticks = %v, want 1", got)
	}
	// Инвариант single-flight: в момент Warn тик пропущен — второй проход
	// не начался ДО дренажа. Точное число запросов не проверяем: под
	// нагрузкой прогона между Warn и проверкой мог наступить следующий
	// тик (150 мс) и запустить проход 2 — это нормальный режим.
	if n := pool.stats(); n < 11 {
		t.Errorf("запросов = %d после дренажа, want ≥ 11 (первый проход завершён)", n)
	}

	// Проход 2 быстрый: обычный режим восстанавливается, цикл живёт.
	// Второй проверки skipped == 1 нет сознательно (ревью R2 P3-1):
	// между первой проверкой и setDelay(0) тикер мог запустить проход 2
	// со старой задержкой — честный skipped = 2, это нормальный режим;
	// инвариант single-flight («не чаще интервала») проверен выше.
	pool.setDelay(0)
	waitCond(t, 2*time.Second, func() bool { return pool.stats() >= 16 }, "проход после пропуска")
	if got := zonestatsSkippedValue(t, reg); got < 1 {
		t.Errorf("skipped_ticks = %v после быстрого прохода, want ≥ 1", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ZoneStatsLoop: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ZoneStatsLoop не завершился после отмены ctx")
	}
	if got := zonestatsHistogramSamples(t, reg); got < 2 {
		t.Errorf("наблюдений гистограммы = %d, want ≥ 2 (по одному на проход)", got)
	}
}

// TestZoneStatsLoopNilRegistry — цикл без реестра жив: смысл цикла —
// реконсиляция, метрики — наблюдаемость (T-161).
func TestZoneStatsLoopNilRegistry(t *testing.T) {
	pool := &zonestatsFakePool{steps: passSteps(1)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, nil, ZoneStatsOptions{
			Interval: time.Hour, // тик не наступит: только стартовый проход
		})
	}()

	waitCond(t, 2*time.Second, func() bool { return pool.stats() >= 11 }, "стартовый проход без реестра")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil без реестра", err)
	}
}

// --- тексты SQL (контракт) ---------------------------------------------------

// TestRecountSQLContracts — литералы кодов в агрегате и операторах
// перезаписи: коды схемы вшиты в текст (стиль lease.go), фильтры
// check_state/status соответствуют model.*;unnest-пачки значений.
func TestRecountSQLContracts(t *testing.T) {
	for _, s := range []struct{ code, name string }{
		{fmt.Sprintf("check_state = %d", int16(model.CheckStatePending)), "pending FILTER"},
		{fmt.Sprintf("check_state = %d", int16(model.CheckStateQueued)), "queued FILTER"},
		{fmt.Sprintf("check_state = %d", int16(model.CheckStateDone)), "checked FILTER"},
		{fmt.Sprintf("status <> %d", int16(model.StatusDeleted)), "базовое множество"},
	} {
		if !strings.Contains(domainsAggSQL, s.code) {
			t.Errorf("domainsAggSQL не содержит %s: %q", s.name, s.code)
		}
	}
	// Легенда статусов — полная и согласованная с statusAggCols (один
	// источник генерации SQL и чтения, ревью P3-4).
	if len(statusAggCols) != len(model.Statuses) {
		t.Fatalf("statusAggCols = %d позиций, want легенда model.Statuses (%d)", len(statusAggCols), len(model.Statuses))
	}
	for _, c := range statusAggCols {
		want := fmt.Sprintf("count(*) FILTER (WHERE status = %d) AS %s", int16(c.status), c.col)
		if !strings.Contains(domainsAggSQL, want) {
			t.Errorf("domainsAggSQL без колонки %s статуса %v", c.col, c.status)
		}
	}
	if !strings.Contains(domainsAggSQL, fmt.Sprintf("(flags & %d) <> 0", int16(model.FlagRenderPending))) {
		t.Error("domainsAggSQL без бита render_pending (предикат бэкфилла 000015)")
	}
	// f_render — без фильтра статуса (инвариант бэкфилла 000015).
	if strings.Contains(domainsAggSQL, fmt.Sprintf("status <> %d AND (flags", int16(model.StatusDeleted))) {
		t.Error("domainsAggSQL фильтрует бит рендера по статусу — предикат обязан совпадать с бэкфиллом 000015 (без фильтра)")
	}
	// Суточные окна (T-179/T-182): предикат окна один, temp-числитель —
	// то же окно × статус temp_error.
	if !strings.Contains(domainsAggSQL, "count(*) FILTER (WHERE last_checked_at = current_date) AS f_checks_today") {
		t.Error("domainsAggSQL без суточного окна f_checks_today (T-179)")
	}
	if !strings.Contains(domainsAggSQL, fmt.Sprintf("count(*) FILTER (WHERE last_checked_at = current_date AND status = %d) AS f_temp_today", int16(model.StatusTempError))) {
		t.Errorf("domainsAggSQL без temp-числителя f_temp_today (T-182): статус %d", int16(model.StatusTempError))
	}
	for _, sql := range []string{recountAggQuerySQL, verifyZonesSQL} {
		if !strings.Contains(sql, "GROUP BY zone_id") {
			t.Errorf("агрегат без partitionwise GROUP BY zone_id: %s", sql)
		}
	}
	// Перезапись: зоны блокируются до счётчиков (порядок против дедлоков,
	// ревью P3-2) — unnest-пачки обоих операторов.
	for _, sql := range []string{recountZonesSQL, recountCountersSQL} {
		if !strings.Contains(sql, "unnest($1") {
			t.Errorf("оператор без unnest-пачки: %s", sql)
		}
	}
	if !strings.Contains(recountCountersSQL, "ON CONFLICT (name) DO UPDATE") {
		t.Error("recountCountersSQL без upsert строк счётчиков (строки создаёт первый проход)")
	}
}

// --- VerifyCounters ---------------------------------------------------------

// TestVerifyCounters — сверка без записи: расхождения зон и глобальных
// (сохранённое ≠ агрегату) с допуском; согласованность — пустой итог.
func TestVerifyCounters(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	statuses[4] = 6 // alive
	pool := &zonestatsFakePool{steps: []zonestatsStep{
		// verifyZonesSQL: id, total, live_total, pending, live_pending, …
		{rows: zonestatsRows(
			[]any{int16(7), int64(10), int64(10), int64(4), int64(4), int64(2), int64(2), int64(3), int64(3), int64(1), int64(1)},
			[]any{int16(9), int64(5), int64(6), int64(1), int64(1), int64(0), int64(0), int64(4), int64(4), int64(0), int64(0)},
		)},
		// recountAggQuerySQL: живой per-zone агрегат (статус alive=6).
		{rows: zonestatsRows(aggRowOf(7, 10, 4, 2, 3, statuses, 3))},
		// historyAggSQL: живые кумулятивы.
		{rows: zonestatsRows([]any{int64(10), int64(2)})},
		// storedCountersSQL: сохранённые.
		{rows: zonestatsRows(
			counterRowOf("status_alive", 5),
			counterRowOf("render_pending", 2),
			counterRowOf("laravel_appeared_total", 10),
			counterRowOf("laravel_lost_total", 2))},
	}}
	// Вторая сверка (допуск) — свежие порции шагов: заготовленные строки
	// итерируются один раз (i в *zonestatsFakeRows), переиспользовать
	// указатели нельзя.
	replay := func() []zonestatsStep {
		return []zonestatsStep{
			{rows: zonestatsRows(
				[]any{int16(7), int64(10), int64(10), int64(4), int64(4), int64(2), int64(2), int64(3), int64(3), int64(1), int64(1)},
				[]any{int16(9), int64(5), int64(6), int64(1), int64(1), int64(0), int64(0), int64(4), int64(4), int64(0), int64(0)},
			)},
			{rows: zonestatsRows(aggRowOf(7, 10, 4, 2, 3, statuses, 3))},
			{rows: zonestatsRows([]any{int64(10), int64(2)})},
			{rows: zonestatsRows(
				counterRowOf("status_alive", 5),
				counterRowOf("render_pending", 2),
				counterRowOf("laravel_appeared_total", 10),
				counterRowOf("laravel_lost_total", 2))},
		}
	}
	pool.steps = append(pool.steps, replay()...)

	mismatch, err := VerifyCounters(context.Background(), pool, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyCounters: %v", err)
	}
	names := make([]string, len(mismatch))
	for i, p := range mismatch {
		names[i] = p.Counter
	}
	// zone:9:total (5→6) и global:render_pending (2→3); status_alive
	// (5→6) вне сверки — свежесть интервала, не дрейф.
	if len(mismatch) != 2 {
		t.Fatalf("расхождений = %v (%d), want 2 позиции", names, len(mismatch))
	}
	byName := map[string]CounterPosition{}
	for _, p := range mismatch {
		byName[p.Counter] = p
	}
	if p := byName["zone:9:total"]; p.Old != 5 || p.New != 6 {
		t.Errorf("zone:9:total = %+v, want old=5 new=6", p)
	}
	if p := byName["global:render_pending"]; p.Old != 2 || p.New != 3 {
		t.Errorf("global:render_pending = %+v, want old=2 new=3", p)
	}

	// Допуск закрывает единичные расхождения.
	if mismatch, err = VerifyCounters(context.Background(), pool, VerifyOptions{Tolerance: 1}); err != nil || len(mismatch) != 0 {
		t.Errorf("verify с допуском 1: %v (расхождений %d), want пусто", err, len(mismatch))
	}
}

func TestVerifyCountersNilPool(t *testing.T) {
	if _, err := VerifyCounters(context.Background(), nil, VerifyOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// TestGlobalCounters — функция глобальных сумм: статусы складываются по
// легенде, бит рендера суммируется, кумулятивы проходят.
func TestGlobalCounters(t *testing.T) {
	s1 := make([]int64, len(statusAggCols))
	s1[4] = 3 // alive
	s2 := make([]int64, len(statusAggCols))
	s2[4] = 2
	s2[6] = 1 // deleted
	agg := []zoneAggRow{
		{zone: 7, statuses: s1, render: 4},
		{zone: 9, statuses: s2, render: 1},
	}
	got := globalCounters(agg, 12, 3)
	if got["status_alive"] != 5 || got["status_deleted"] != 1 || got["status_new"] != 0 {
		t.Errorf("статусы = %v, want alive=5 deleted=1 new=0", got)
	}
	if got["render_pending"] != 5 || got["laravel_appeared_total"] != 12 || got["laravel_lost_total"] != 3 {
		t.Errorf("скаляры = %v, want render=5 appeared=12 lost=3", got)
	}
	// Полная легенда имён.
	if len(got) != len(CounterNames()) {
		t.Errorf("легенда = %d имён, want %d", len(got), len(CounterNames()))
	}
}

// Юнит-тесты пересчёта счётчиков зон (T-078, ТЗ 5.9, 11.4 п. 6) на
// фейковом pgx-пуле: точный SQL-текст и параметры (коды check_state/status,
// updated_at), дефолты, пустой набор, повторные проходы, ошибки и отмена
// ctx, цикл ZoneStatsLoop (немедленный первый проход, тик, переживание
// ошибки, чистый выход). Поведение против живой БД — интеграционные
// тесты (test/integration).
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

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// --- фейковый пул ---------------------------------------------------------

// zonestatsCall — записанный SQL-вызов с аргументами.
type zonestatsCall struct {
	sql  string
	args []any
}

// zonestatsFakePool — db.DB без БД: очередь результатов Exec (число
// обновлённых зон) и ошибок, протокол вызовов. Всё под замком: цикл
// ZoneStatsLoop работает в горутине, тест ожидает прогресс по
// счётчикам. delay — пауза каждого Exec (медленный проход для
// single-flight теста T-161).
type zonestatsFakePool struct {
	mu       sync.Mutex
	tags     []int64 // очередь RowsAffected (кончилась — 0: пустой набор)
	execErrs []error // очередь ошибок Exec (nil — вызов без ошибки)
	delay    time.Duration

	execs []zonestatsCall
}

func (p *zonestatsFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *zonestatsFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("Query не должен вызываться")
}

func (p *zonestatsFakePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	// Пауза вне замка: имитация долгого агрегата не должна держать
	// замок, пока тест читает протокол.
	if d := p.delaySnapshot(); d > 0 {
		time.Sleep(d)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execs = append(p.execs, zonestatsCall{sql: sql, args: args})
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
func (p *zonestatsFakePool) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.execs)
}

// delaySnapshot — текущая пауза Exec под замком.
func (p *zonestatsFakePool) delaySnapshot() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.delay
}

// setDelay меняет паузу Exec (сценарий медленного прохода).
func (p *zonestatsFakePool) setDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

// calls — снимок протокола вызовов под замком.
func (p *zonestatsFakePool) calls() []zonestatsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]zonestatsCall(nil), p.execs...)
}

// --- ожидаемый SQL-текст (контракт) ---------------------------------------

const wantZoneStatsSQL = "UPDATE zones AS z " +
	"SET domains_total = s.total, " +
	"domains_checked = s.checked, " +
	"domains_laravel = s.laravel, " +
	"updated_at = $3 " +
	"FROM (SELECT z2.id AS zone_id, " +
	"COALESCE(c.total, 0) AS total, " +
	"COALESCE(c.checked, 0) AS checked, " +
	"COALESCE(l.laravel, 0) AS laravel " +
	"FROM zones AS z2 " +
	"LEFT JOIN (" +
	"SELECT zone_id, count(*) AS total, " +
	"count(*) FILTER (WHERE check_state = $1) AS checked " +
	"FROM domains WHERE status <> $2 GROUP BY zone_id" +
	") AS c ON c.zone_id = z2.id " +
	"LEFT JOIN (" +
	"SELECT zone_id, count(*) AS laravel " +
	"FROM laravel_domains GROUP BY zone_id" +
	") AS l ON l.zone_id = z2.id) AS s " +
	"WHERE z.id = s.zone_id"

// zonestatsFixedClock — часы с фиксированным «сейчас» (updated_at прохода
// воспроизводим до наносекунды).
func zonestatsFixedClock() (time.Time, func() time.Time) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	return now, func() time.Time { return now }
}

// --- ZoneStatsOnce ---------------------------------------------------------

// TestZoneStatsOnceSQL — один оператор UPDATE с точными параметрами:
// проверенные — check_state = done, база множества — не deleted,
// updated_at — момент прохода.
func TestZoneStatsOnceSQL(t *testing.T) {
	now, clock := zonestatsFixedClock()
	pool := &zonestatsFakePool{tags: []int64{12}}

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{Now: clock})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("вызовов = %d, want 1: проход — один оператор", len(calls))
	}
	if calls[0].sql != wantZoneStatsSQL {
		t.Errorf("SQL прохода:\n%s\nwant:\n%s", calls[0].sql, wantZoneStatsSQL)
	}
	wantArgs := []any{
		int16(model.CheckStateDone), // FILTER: проверенные в текущем цикле
		int16(model.StatusDeleted),  // база: удалённые вне обоих счётчиков
		now,                         // updated_at
	}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("аргументы = %v, want %v", calls[0].args, wantArgs)
	}
	if res.Zones != 12 {
		t.Errorf("итог = %+v, want Zones=12 (RowsAffected)", res)
	}
}

// TestZoneStatsOnceDefaults — нулевые опции: интервал не влияет на разовый
// проход (один оператор), часы — time.Now (updated_at — свежий момент
// того же типа).
func TestZoneStatsOnceDefaults(t *testing.T) {
	before := time.Now()
	pool := &zonestatsFakePool{}

	if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{}); err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	after := time.Now()

	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("вызовов = %d, want 1", len(calls))
	}
	got, ok := calls[0].args[2].(time.Time)
	if !ok || got.Before(before) || got.After(after) {
		t.Errorf("updated_at = %v, want time.Time в [%v, %v]", calls[0].args[2], before, after)
	}
}

// TestZoneStatsOnceEmpty — зон нет: RowsAffected 0, итог нулевой,
// оператор один.
func TestZoneStatsOnceEmpty(t *testing.T) {
	pool := &zonestatsFakePool{} // очередь tags пуста — UPDATE 0

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if res != (ZoneStatsResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	if n := pool.stats(); n != 1 {
		t.Errorf("операторов = %d, want 1", n)
	}
}

// TestZoneStatsOnceRepeatPasses — проходы независимы и идемпотентны:
// каждый — свой оператор с текущим моментом.
func TestZoneStatsOnceRepeatPasses(t *testing.T) {
	pool := &zonestatsFakePool{tags: []int64{5, 5, 5}}

	for i := 0; i < 3; i++ {
		if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{}); err != nil {
			t.Fatalf("проход %d: %v", i, err)
		}
	}
	if n := pool.stats(); n != 3 {
		t.Errorf("операторов = %d, want 3 (по одному на проход)", n)
	}
}

// TestZoneStatsOnceError — ошибка оператора обёрнута с контекстом задачи;
// отмена ctx сохраняется errors.Is.
func TestZoneStatsOnceError(t *testing.T) {
	pool := &zonestatsFakePool{execErrs: []error{errors.New("бд недоступна")}}
	_, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "monitor: пересчёт счётчиков зон") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка = %v, want контекст задачи и исходная ошибка", err)
	}

	cancelPool := &zonestatsFakePool{execErrs: []error{context.Canceled}}
	if _, err := ZoneStatsOnce(context.Background(), cancelPool, ZoneStatsOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
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
// pinned литералом 5m: константа в проверке мутировала бы вместе с
// мутантом и «следовала» за ним (T-M01, alerts); граница «≤ 0» —
// положительный наносекундный интервал дефолтом не заменяется.
func TestZoneStatsOptionsDefaults(t *testing.T) {
	if DefaultZoneStatsInterval != 5*time.Minute {
		t.Errorf("DefaultZoneStatsInterval = %v, want 5m (T-161, замер §12)", DefaultZoneStatsInterval)
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
	if got.Interval != 30*time.Second {
		t.Errorf("Interval = %v, want заданный 30s (дефолт не затирает)", got.Interval)
	}
	if got.Now == nil || !got.Now().Equal(time.Unix(0, 0)) {
		t.Errorf("withDefaults затирает Now: nil=%v, значение %v", got.Now == nil, got.Now())
	}
}

// --- ZoneStatsLoop ---------------------------------------------------------

// TestZoneStatsLoopImmediatePass — первый проход выполняется сразу при
// старте, не дожидаясь тика: счётчики свежи после простоя/рестарта.
func TestZoneStatsLoopImmediatePass(t *testing.T) {
	pool := &zonestatsFakePool{tags: []int64{7}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, nil, ZoneStatsOptions{
			Interval: time.Hour, // тик не наступит: только стартовый проход
		})
	}()

	// Стартовый проход: один оператор и тишина до отмены.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 1 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("стартовый проход не выполнен")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if n := pool.stats(); n != 1 {
		t.Errorf("операторов = %d, want 1: без тика повторных проходов нет", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil при отмене ctx", err)
	}
}

// TestZoneStatsLoopTicker — проходы повторяются по тику: ошибка первого
// прохода — Warn (цикл живёт), тик — следующий проход с итогом Debug
// (хендлер с уровнем Debug: дефолтный Info его подавляет).
func TestZoneStatsLoopTicker(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Стартовый проход — ошибка БД; проход по тику — успешный.
	pool := &zonestatsFakePool{
		tags:     []int64{9},
		execErrs: []error{errors.New("бд недоступна")},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, nil, ZoneStatsOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Два оператора: провал стартового и проход по тику.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 2 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до второго оператора: %d", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil при отмене ctx", err)
	}

	log := buf.String()
	for _, want := range []string{
		"цикл пересчёта счётчиков зон", // старт
		"level=WARN", // ошибка прохода не убила цикл
		"бд недоступна",
		"zones=9", // итог успешного прохода
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

// TestZoneStatsLoopCancelDuringPass — отмена ctx во время прохода — не
// тревога: без Warn, чистый nil (контракт OnRun, T-012).
func TestZoneStatsLoopCancelDuringPass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &zonestatsFakePool{execErrs: []error{context.Canceled}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до старта: первый же оператор прохода провалится

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

// TestZoneStatsLoopPassDuringCancel — успешный проход под уже отменённым
// ctx (фейк не смотрит на отмену) — штатный итог Debug, а не тишина
// ветки «отмена во время прохода»: отмена классифицирует только ошибки.
func TestZoneStatsLoopPassDuringCancel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	pool := &zonestatsFakePool{tags: []int64{9}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- ZoneStatsLoop(ctx, pool, nil, ZoneStatsOptions{Log: logger}) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ZoneStatsLoop: %v, want nil при отмене", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
	log := buf.String()
	if !strings.Contains(log, "zones=9") {
		t.Errorf("успешный проход под отменой не зафиксирован:\n%s", log)
	}
	if strings.Contains(log, "level=WARN") {
		t.Errorf("успешный проход под отменой дал Warn:\n%s", log)
	}
}

// --- single-flight (T-161) --------------------------------------------------

// zonestatsSyncBuf — bytes.Buffer под замком: цикл пишет лог в своей
// горутине, тест читает его до завершения цикла (ожидание Warn вживую).
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

// zonestatsHistogramSamples — число наблюдений crawler_zonestats_pass_seconds
// (каждый проход обязан попасть в гистограмму).
func zonestatsHistogramSamples(t *testing.T, reg *metrics.Registry) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_zonestats_pass_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

// TestZoneStatsLoopSingleFlight — проход дольше интервала (рост таблицы,
// замер §12): накопившийся тик не запускает новый проход сразу за
// медленным — Warn «дольше интервала», счётчик пропусков, гистограмма
// длительности; следующий проход — по следующему тику, цикл живёт.
//
// Раскладка без реальных секунд ожидания: интервал 50 мс, медленный
// стартовый проход 120 мс (тик 50 мс буферизуется, тик 100 мс тонет —
// канал тикера ёмкостью 1, тик 150 мс ещё не наступил к моменту дренажа
// в 120 мс): дренаж снимает тик без гонки на границе, проход 2 — не
// раньше 150 мс.
func TestZoneStatsLoopSingleFlight(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := slog.New(slog.NewTextHandler(slogBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := metrics.New("monitor")
	pool := &zonestatsFakePool{tags: []int64{3, 3, 3, 3, 3, 3}}
	pool.setDelay(120 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: 50 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Warn приходит в конце медленного прохода (после дренажа тика).
	deadline := time.Now().Add(2 * time.Second)
	for !slogBuf.contains("дольше интервала") {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("медленный проход не зафиксирован:\n%s", slogBuf)
		}
		time.Sleep(time.Millisecond)
	}
	if got := zonestatsSkippedValue(t, reg); got != 1 {
		t.Errorf("skipped_ticks = %v, want 1", got)
	}
	// Момент наблюдения Warn — до тика 150 мс: второй проход ещё не
	// мог начаться (единственный buffered-тик снят дренажом).
	if n := pool.stats(); n != 1 {
		t.Errorf("операторов = %d после дренажа, want 1 (тип пропущен)", n)
	}

	// Проход 2 быстрый: обычный режим восстанавливается, цикл живёт.
	pool.setDelay(0)
	deadline = time.Now().Add(2 * time.Second)
	for pool.stats() < 2 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("проход после пропуска не начался: %d", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	if got := zonestatsSkippedValue(t, reg); got != 1 {
		t.Errorf("skipped_ticks = %v после быстрого прохода, want 1", got)
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
// UPDATE счётчиков, метрики — наблюдаемость (T-161).
func TestZoneStatsLoopNilRegistry(t *testing.T) {
	pool := &zonestatsFakePool{tags: []int64{7}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, nil, ZoneStatsOptions{
			Interval: time.Hour, // тик не наступит: только стартовый проход
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 1 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("стартовый проход не выполнен без реестра")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil без реестра", err)
	}
}

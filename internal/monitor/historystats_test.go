// Юнит-тесты счётчиков истории детекции appeared/lost (T-118) на
// фейковом pgx-пуле: точный SQL-текст (FILTER-агрегат одним сканом),
// значения gauge после первого прохода, дефолты, no-op без реестра,
// переживание ошибки. Поведение против живой БД — dev-окружение.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/metrics"
)

// historystatsFakePool — db.DB без БД: очередь пар (appeared, lost) и
// ошибок агрегата.
type historystatsFakePool struct {
	mu    sync.Mutex
	stats [][2]int64 // очередь ответов (кончилась — {0,0})
	errs  []error
	calls []nearstatsCall
}

func (p *historystatsFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *historystatsFakePool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("historystatsFakePool: неожидаемый Exec: %s", sql)
}

func (p *historystatsFakePool) Query(ctx context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, nearstatsCall{sql: sql})
	if len(p.errs) > 0 {
		err := p.errs[0]
		p.errs = p.errs[1:]
		if err != nil {
			return nil, err
		}
	}
	st := [2]int64{}
	if len(p.stats) > 0 {
		st = p.stats[0]
		p.stats = p.stats[1:]
	}
	return &historyRows{row: []any{st[0], st[1]}}, nil
}

func (p *historystatsFakePool) statsCalls() []nearstatsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]nearstatsCall(nil), p.calls...)
}

// historyRows — pgx.Rows с одной строкой агрегата (appeared, lost).
type historyRows struct {
	row    []any
	next   bool
	closed bool
}

func (r *historyRows) Close()                                       { r.closed = true }
func (r *historyRows) Err() error                                   { return nil }
func (r *historyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *historyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *historyRows) Next() bool {
	if r.closed || r.next {
		return false
	}
	r.next = true
	return true
}

func (r *historyRows) Scan(dest ...any) error {
	if !r.next || len(dest) != 2 {
		return errors.New("historyRows: Scan без Next или приёмников ≠ 2")
	}
	*dest[0].(*int64) = r.row[0].(int64)
	*dest[1].(*int64) = r.row[1].(int64)
	return nil
}

func (r *historyRows) Values() ([]any, error) { return r.row, nil }
func (r *historyRows) RawValues() [][]byte    { return nil }
func (r *historyRows) Conn() *pgx.Conn        { return nil }
func (r *historyRows) TypeMap() *pgtype.Map   { return nil }

// --- тесты --------------------------------------------------------------------

const wantHistoryStatsSQL = "SELECT count(*) FILTER (WHERE appeared), " +
	"count(*) FILTER (WHERE NOT appeared) FROM laravel_history"

func TestHistoryStatsOnceSQL(t *testing.T) {
	pool := &historystatsFakePool{stats: [][2]int64{{12, 3}}}
	st, err := HistoryStatsOnce(context.Background(), pool)
	if err != nil {
		t.Fatalf("HistoryStatsOnce: %v", err)
	}
	calls := pool.statsCalls()
	if len(calls) != 1 || calls[0].sql != wantHistoryStatsSQL {
		t.Fatalf("SQL = %v, want %s", calls, wantHistoryStatsSQL)
	}
	if st.Appeared != 12 || st.Lost != 3 {
		t.Errorf("итог = %+v, want {12 3}", st)
	}
}

func TestHistoryStatsOnceNilPool(t *testing.T) {
	if _, err := HistoryStatsOnce(context.Background(), nil); err == nil {
		t.Fatal("HistoryStatsOnce(nil pool) = nil error, хочу ошибку")
	}
}

func TestHistoryStatsOptionsDefaults(t *testing.T) {
	if got := (HistoryStatsOptions{}).withDefaults().Interval; got != DefaultHistoryStatsInterval {
		t.Errorf("дефолт интервала = %v, want %v", got, DefaultHistoryStatsInterval)
	}
}

// historyGaugeValue — значение gauge по имени из реестра.
func historyGaugeValue(t *testing.T, reg *metrics.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	t.Fatalf("серия %s не найдена", name)
	return -1
}

func TestHistoryStatsLoopImmediatePass(t *testing.T) {
	pool := &historystatsFakePool{stats: [][2]int64{{7, 2}}}
	reg := metrics.New("monitor-test")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- HistoryStatsLoop(ctx, pool, reg, HistoryStatsOptions{})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for len(pool.statsCalls()) == 0 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("первый проход не выполнен")
		}
		time.Sleep(time.Millisecond)
	}
	if got := historyGaugeValue(t, reg, "crawler_laravel_appeared_total"); got != 7 {
		t.Errorf("appeared gauge = %v, want 7", got)
	}
	if got := historyGaugeValue(t, reg, "crawler_laravel_lost_total"); got != 2 {
		t.Errorf("lost gauge = %v, want 2", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("HistoryStatsLoop: %v, want nil при отмене", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
}

func TestHistoryStatsLoopNoop(t *testing.T) {
	pool := &historystatsFakePool{}
	if err := HistoryStatsLoop(context.Background(), pool, nil, HistoryStatsOptions{}); err != nil {
		t.Fatalf("nil реестр: %v, want nil", err)
	}
	reg := metrics.New("monitor-test")
	if err := HistoryStatsLoop(context.Background(), nil, reg, HistoryStatsOptions{}); err != nil {
		t.Fatalf("nil пул: %v, want nil", err)
	}
	if calls := pool.statsCalls(); len(calls) != 0 {
		t.Errorf("проходов = %d, хочу 0", len(calls))
	}
}

func TestHistoryStatsLoopErrorSurvives(t *testing.T) {
	pool := &historystatsFakePool{errs: []error{errors.New("бд недоступна")}}
	reg := metrics.New("monitor-test")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- HistoryStatsLoop(ctx, pool, reg, HistoryStatsOptions{Interval: time.Millisecond})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for len(pool.statsCalls()) < 2 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до второго прохода: %d", len(pool.statsCalls()))
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("HistoryStatsLoop: %v, want nil (ошибка прохода — не смерть цикла)", err)
	}
}

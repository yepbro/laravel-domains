// Юнит-тесты пакета status (T-079 → T-126, ТЗ 5.9 / ТЗД §4.5) на фейковых
// пуле (db.DB) и клиенте XLEN — без БД и Redis. Проверяются точный
// SQL-текст выборок (контракт, стиль reanalyze_test.go) и живая семантика
// фейка: агрегат domains по check_state с исключением deleted из pending,
// LEFT JOIN зон без доменов, сортировка по имени; окно темпа и защита от
// деления на ноль в ComputeRates (короткое окно, нулевая скорость,
// пустой цикл, минимальная ETA); формат интервалов. Вывод (таблица/JSON)
// и роутинг — тесты CLI (cmd/crawlerctl/status_test.go); поведение против
// живых PostgreSQL/Redis — dev-окружение (make status).
package status

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/db"
	"crawler/internal/model"
)

// --- SQL-контракты ----------------------------------------------------------

// wantZoneStatsSQL / wantThroughputSQL — ожидаемый текст запросов; фейк
// сопоставляет SQL точно, неожидаемый запрос валит тест громко.
const wantZoneStatsSQL = "SELECT z.id, z.name, coalesce(z.name_unicode, '') AS name_unicode, " +
	"z.is_active, z.domains_total, z.domains_checked, " +
	"count(d.id) FILTER (WHERE d.check_state = 0 AND d.status <> 6) AS pending, " +
	"count(d.id) FILTER (WHERE d.check_state = 1) AS queued, " +
	"count(d.id) FILTER (WHERE d.check_state = 2) AS done " +
	"FROM zones AS z LEFT JOIN domains AS d ON d.zone_id = z.id " +
	"GROUP BY z.id ORDER BY z.name"

const wantThroughputSQL = "SELECT " +
	"(SELECT count(*) FROM domains WHERE last_checked_at = current_date) AS done_today, " +
	"extract(epoch FROM (now() - date_trunc('day', now())))::float8 AS elapsed_sec, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour') AS results_hour, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour' AND rendered) AS rendered_hour"

// --- фейковый пул (db.DB) ----------------------------------------------------

// statusZoneSeed — зона в фейке: счётчики и атрибуты отображения.
type statusZoneSeed struct {
	id      int16
	name    string
	unicode string
	active  bool
	total   int64
	checked int64
	hasUni  bool // name_unicode не NULL (в SQL — coalesce в '')
}

// statusDomSeed — домен для агрегата check_state.
type statusDomSeed struct {
	zone  int16
	state model.CheckState
	stats model.Status
}

// statusPool — db.DB без БД: живая семантика zoneStatsSQL (GROUP BY зоны,
// pending без deleted, queued/done без фильтра статуса, LEFT JOIN — зона
// без доменов остаётся, ORDER BY name) и throughputSQL (готовые значения).
type statusPool struct {
	zones   []statusZoneSeed
	domains []statusDomSeed
	thr     Throughput

	mu        sync.Mutex
	queryErr  error
	queries   []string
	execCalls int
}

var _ db.DB = (*statusPool)(nil)

func newStatusPool() *statusPool {
	return &statusPool{}
}

func (p *statusPool) seedZone(id int16, name string, active bool, total, checked int64) {
	p.zones = append(p.zones, statusZoneSeed{id: id, name: name, active: active, total: total, checked: checked})
}

// seedZoneUnicode — зона с непустым name_unicode (NULL vs значение).
func (p *statusPool) seedZoneUnicode(id int16, name, unicode string, active bool, total, checked int64) {
	p.zones = append(p.zones, statusZoneSeed{id: id, name: name, unicode: unicode, active: active, total: total, checked: checked, hasUni: true})
}

func (p *statusPool) seedDomain(zone int16, state model.CheckState, stats model.Status) {
	p.domains = append(p.domains, statusDomSeed{zone: zone, state: state, stats: stats})
}

func (p *statusPool) setQueryErr(err error) {
	p.mu.Lock()
	p.queryErr = err
	p.mu.Unlock()
}

func (p *statusPool) calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.queries...)
}

func (p *statusPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("statusPool: CopyFrom не должен вызываться")
}

func (p *statusPool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	p.execCalls++
	p.mu.Unlock()
	return pgconn.CommandTag{}, fmt.Errorf("statusPool: неожидаемый Exec: %s", sql)
}

func (p *statusPool) Query(ctx context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, sql)
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	switch sql {
	case zoneStatsSQL:
		return p.zoneRows(), nil
	case throughputSQL:
		return statusRowsOf([]any{p.thr.DoneToday, p.thr.ElapsedSec, p.thr.ResultsHour, p.thr.RenderedHour}), nil
	default:
		return nil, fmt.Errorf("statusPool: неожидаемый SQL Query: %s", sql)
	}
}

// zoneRows применяет семантику zoneStatsSQL к сидам.
func (p *statusPool) zoneRows() pgx.Rows {
	pending := make(map[int16]int64)
	queued := make(map[int16]int64)
	done := make(map[int16]int64)
	for _, d := range p.domains {
		switch d.state {
		case model.CheckStatePending:
			if d.stats != model.StatusDeleted { // status <> deleted
				pending[d.zone]++
			}
		case model.CheckStateQueued:
			queued[d.zone]++ // без фильтра статуса: домен в работе
		case model.CheckStateDone:
			done[d.zone]++
		}
	}
	sorted := append([]statusZoneSeed(nil), p.zones...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
	rows := make([][]any, len(sorted))
	for i, z := range sorted {
		uni := ""
		if z.hasUni {
			uni = z.unicode // coalesce(name_unicode, '')
		}
		rows[i] = []any{z.id, z.name, uni, z.active, z.total, z.checked,
			pending[z.id], queued[z.id], done[z.id]}
	}
	return statusRowsOf(rows...)
}

// statusRows — pgx.Rows с заготовленными строками (Scan позиционный,
// типы int16/string/bool/int64/float32/float64/[]byte). Копия общего
// фейка тестов CLI (cmd/crawlerctl): тест-файлы разных пакетов не
// импортируют друг друга.
type statusRows struct {
	rows [][]any
	i    int
}

func statusRowsOf(rows ...[]any) *statusRows { return &statusRows{rows: rows} }

func (r *statusRows) Close()                                       {}
func (r *statusRows) Err() error                                   { return nil }
func (r *statusRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *statusRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *statusRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *statusRows) RawValues() [][]byte                          { return nil }
func (r *statusRows) Conn() *pgx.Conn                              { return nil }
func (r *statusRows) TypeMap() *pgtype.Map                         { return nil }

func (r *statusRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *statusRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("statusRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("statusRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *int16:
			*dst = v.(int16)
		case *string:
			*dst = v.(string)
		case *bool:
			*dst = v.(bool)
		case *int64:
			*dst = v.(int64)
		case *float32:
			*dst = v.(float32)
		case *float64:
			*dst = v.(float64)
		case *[]byte:
			*dst = v.([]byte)
		default:
			return fmt.Errorf("statusRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// --- фейковый XLEN -----------------------------------------------------------

// statusLenner — StreamLener: длины по стримам (или сбой любой длины),
// фиксирует порядок опроса.
type statusLenner struct {
	lens  map[string]int64
	err   error
	mu    sync.Mutex
	calls []string
}

func (l *statusLenner) Len(_ context.Context, stream string) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, stream)
	if l.err != nil {
		return 0, l.err
	}
	return l.lens[stream], nil
}

func (l *statusLenner) order() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

// --- SQL и семантика выборок ---------------------------------------------------

func TestStatusSQLContract(t *testing.T) {
	if zoneStatsSQL != wantZoneStatsSQL {
		t.Errorf("zoneStatsSQL:\n%s\nwant:\n%s", zoneStatsSQL, wantZoneStatsSQL)
	}
	if throughputSQL != wantThroughputSQL {
		t.Errorf("throughputSQL:\n%s\nwant:\n%s", throughputSQL, wantThroughputSQL)
	}
}

// TestFetchZoneStats — агрегат по зонам: pending без deleted, queued/done
// без фильтра статуса, зона без доменов (LEFT JOIN) в сводке, сортировка
// по имени, unicode и флаг активности дочитываются.
func TestFetchZoneStats(t *testing.T) {
	pool := newStatusPool()
	pool.seedZoneUnicode(2, "com", "com", true, 1000, 400)
	pool.seedZone(1, "crawl.test", true, 100, 40)
	pool.seedZone(3, "empty.test", false, 0, 0) // LEFT JOIN: доменов нет

	pool.seedDomain(1, model.CheckStatePending, model.StatusNew)
	pool.seedDomain(1, model.CheckStatePending, model.StatusNew)
	pool.seedDomain(1, model.CheckStatePending, model.StatusDeleted) // не ждёт проверки
	pool.seedDomain(1, model.CheckStateQueued, model.StatusNew)
	pool.seedDomain(1, model.CheckStateQueued, model.StatusDeleted) // в работе
	pool.seedDomain(1, model.CheckStateDone, model.StatusAlive)
	pool.seedDomain(2, model.CheckStateDone, model.StatusNotDelegated)

	zones, err := FetchZoneStats(context.Background(), pool)
	if err != nil {
		t.Fatalf("FetchZoneStats: %v", err)
	}
	if calls := pool.calls(); len(calls) != 1 || calls[0] != wantZoneStatsSQL {
		t.Fatalf("запросы = %q, хочу один zoneStatsSQL", calls)
	}
	want := []ZoneStats{
		{ID: 2, Name: "com", NameUnicode: "com", IsActive: true, DomainsTotal: 1000, DomainsChecked: 400, Done: 1},
		{ID: 1, Name: "crawl.test", IsActive: true, DomainsTotal: 100, DomainsChecked: 40,
			Pending: 2, Queued: 2, Done: 1},
		{ID: 3, Name: "empty.test", IsActive: false},
	}
	if !reflect.DeepEqual(zones, want) {
		t.Errorf("zones =\n%+v\nwant\n%+v", zones, want)
	}
}

// TestFetchZoneStatsEmpty — зон нет: пустой результат без ошибки.
func TestFetchZoneStatsEmpty(t *testing.T) {
	zones, err := FetchZoneStats(context.Background(), newStatusPool())
	if err != nil {
		t.Fatalf("FetchZoneStats: %v", err)
	}
	if len(zones) != 0 {
		t.Errorf("zones = %+v, хочу пусто", zones)
	}
}

// TestFetchThroughput — одна строка замеров; типы колонок (int64/float64).
func TestFetchThroughput(t *testing.T) {
	pool := newStatusPool()
	pool.thr = Throughput{DoneToday: 2279, ElapsedSec: 3720.5, ResultsHour: 6132, RenderedHour: 1025}

	thr, err := FetchThroughput(context.Background(), pool)
	if err != nil {
		t.Fatalf("FetchThroughput: %v", err)
	}
	if calls := pool.calls(); len(calls) != 1 || calls[0] != wantThroughputSQL {
		t.Fatalf("запросы = %q, хочу один throughputSQL", calls)
	}
	if thr != pool.thr {
		t.Errorf("thr = %+v, want %+v", thr, pool.thr)
	}
}

// TestStatusFetchErrors — ошибки пула возвращаются с контекстом пакета;
// nil-пул — ошибка, а не паника.
func TestStatusFetchErrors(t *testing.T) {
	boom := errors.New("boom")
	pool := newStatusPool()
	pool.setQueryErr(boom)
	if _, err := FetchZoneStats(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: зоны:") {
		t.Errorf("FetchZoneStats err = %v, хочу контекст «status: зоны:»", err)
	}
	if _, err := FetchThroughput(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: темп:") {
		t.Errorf("FetchThroughput err = %v, хочу контекст «status: темп:»", err)
	}
	if _, err := FetchZoneStats(context.Background(), nil); err == nil {
		t.Error("FetchZoneStats(nil) = nil, хочу ошибку")
	}
	if _, err := FetchThroughput(context.Background(), nil); err == nil {
		t.Error("FetchThroughput(nil) = nil, хочу ошибку")
	}
}

// --- очереди -------------------------------------------------------------------

// TestFetchQueueLens — все рабочие стримы в порядке ТЗ 4.2; ошибка
// останавливает сбор (Redis один); nil-клиент — ошибка.
func TestFetchQueueLens(t *testing.T) {
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:analyze": 3, "q:dead": 1}
	lenner := &statusLenner{lens: lens}
	got, err := FetchQueueLens(context.Background(), lenner)
	if err != nil {
		t.Fatalf("FetchQueueLens: %v", err)
	}
	if !reflect.DeepEqual(got, lens) {
		t.Errorf("lens = %v, want %v", got, lens)
	}
	if order := lenner.order(); !reflect.DeepEqual(order, Streams) {
		t.Errorf("порядок опроса = %v, want %v", order, Streams)
	}
	if want := []string{"q:dns", "q:http", "q:render", "q:analyze", "q:dead"}; !reflect.DeepEqual(Streams, want) {
		t.Errorf("Streams = %v, want %v (все стримы ТЗ 4.2)", Streams, want)
	}

	_, err = FetchQueueLens(context.Background(), &statusLenner{err: errors.New("connection refused")})
	if err == nil {
		t.Error("ошибка XLEN потеряна")
	}
	if _, err := FetchQueueLens(context.Background(), nil); err == nil {
		t.Error("FetchQueueLens(nil) = nil, хочу ошибку")
	}
}

// --- расчёты ---------------------------------------------------------------------

func TestComputeTotals(t *testing.T) {
	zones := []ZoneStats{
		{DomainsTotal: 100, DomainsChecked: 40, Pending: 60, Queued: 1, Done: 39},
		{DomainsTotal: 1000, DomainsChecked: 400, Pending: 500, Queued: 5, Done: 495},
	}
	want := Totals{DomainsTotal: 1100, DomainsChecked: 440, Pending: 560, Queued: 6, Done: 534}
	if got := ComputeTotals(zones); got != want {
		t.Errorf("ComputeTotals = %+v, want %+v", got, want)
	}
	if got := ComputeTotals(nil); got != (Totals{}) {
		t.Errorf("ComputeTotals(nil) = %+v, хочу нули", got)
	}
}

// TestComputeRates — защита от деления на ноль и коротких окон: скорость
// доменов только при окне ≥ MinSpeedWindowSec; ETA — при известной
// ненулевой скорости (или пустом цикле), округление и минимум 1с.
func TestComputeRates(t *testing.T) {
	// Рабочий случай: 3600 проверок за час → 1/с; остаток 90 → ETA 90с.
	r := ComputeRates(Throughput{DoneToday: 3600, ElapsedSec: 3600, ResultsHour: 7200, RenderedHour: 3600}, 60, 30)
	if !r.DomainsKnown || r.DomainsPerSec != 1 {
		t.Errorf("скорость доменов = %v (%v), хочу 1 (известна)", r.DomainsPerSec, r.DomainsKnown)
	}
	if r.ResultsPerSec != 2 || r.RenderPerSec != 1 {
		t.Errorf("скорости часа = %v/%v, хочу 2/1", r.ResultsPerSec, r.RenderPerSec)
	}
	if r.Remaining != 90 {
		t.Errorf("остаток = %d, хочу 90", r.Remaining)
	}
	if !r.ETAKnown || r.ETASec != 90 {
		t.Errorf("ETA = %d (%v), хочу 90 (известна)", r.ETASec, r.ETAKnown)
	}

	// Окно короче минимума: скорости доменов и ETA нет, часовые считаются.
	r = ComputeRates(Throughput{ElapsedSec: MinSpeedWindowSec - 1, ResultsHour: 3600}, 10, 0)
	if r.DomainsKnown || r.DomainsPerSec != 0 {
		t.Errorf("короткое окно: Known=%v speed=%v, хочу неизвестную", r.DomainsKnown, r.DomainsPerSec)
	}
	if r.ETAKnown {
		t.Error("короткое окно: ETA известна, хочу н/д")
	}
	if r.ResultsPerSec != 1 {
		t.Errorf("результаты/с = %v, хочу 1 (окно фиксированное)", r.ResultsPerSec)
	}

	// Граница окна ровно на минимуме — скорость известна.
	if r := ComputeRates(Throughput{DoneToday: MinSpeedWindowSec, ElapsedSec: MinSpeedWindowSec}, 1, 0); !r.DomainsKnown {
		t.Error("окно = MinSpeedWindowSec: скорость должна быть известна")
	}

	// Скорость 0 (за окно ни одной проверки): ETA н/д, не паника.
	r = ComputeRates(Throughput{DoneToday: 0, ElapsedSec: 7200}, 5, 0)
	if !r.DomainsKnown || r.DomainsPerSec != 0 {
		t.Errorf("нулевые проверки: Known=%v speed=%v, хочу известную 0", r.DomainsKnown, r.DomainsPerSec)
	}
	if r.ETAKnown {
		t.Error("скорость 0: ETA не должна быть известна (деление на ноль)")
	}

	// Пустой цикл: ETA 0 даже без скорости.
	r = ComputeRates(Throughput{ElapsedSec: 1}, 0, 0)
	if !r.ETAKnown || r.ETASec != 0 {
		t.Errorf("пустой цикл: ETA = %d (%v), хочу 0 (известна)", r.ETASec, r.ETAKnown)
	}

	// Округление вверх/вниз и минимум: 100/3 = 33.3 → 33; 1/3 = 0.3 → 1.
	if r := ComputeRates(Throughput{DoneToday: 3000, ElapsedSec: 1000}, 100, 0); r.ETASec != 33 {
		t.Errorf("ETA 100/3 = %d, хочу 33", r.ETASec)
	}
	if r := ComputeRates(Throughput{DoneToday: 3000, ElapsedSec: 1000}, 1, 0); r.ETASec != 1 {
		t.Errorf("ETA 1/3 = %d, хочу минимум 1", r.ETASec)
	}
}

// TestFormatDuration — округлые интервалы: секунды/минуты/часы/дни.
func TestFormatDuration(t *testing.T) {
	cases := []struct {
		sec  int64
		want string
	}{
		{0, "0с"},
		{45, "45с"},
		{59, "59с"},
		{60, "1м"},
		{90, "2м"},    // округление к минуте
		{3599, "60м"}, // 59м59с → 60м
		{3600, "1ч 00м"},
		{19200, "5ч 20м"},
		{86400, "1д 0ч"},
		{275400, "3д 4ч"},
	}
	for _, tc := range cases {
		if got := FormatDuration(tc.sec); got != tc.want {
			t.Errorf("FormatDuration(%d) = %q, want %q", tc.sec, got, tc.want)
		}
	}
}

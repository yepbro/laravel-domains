// Юнит-тесты crawlerctl status (T-079, ТЗ 5.9) на фейковых пуле (db.DB)
// и клиенте XLEN — без БД и Redis. Проверяются точный SQL-текст выборок
// (контракт, стиль reanalyze_test.go) и живая семантика фейка: агрегат
// domains по check_state с исключением deleted из pending, LEFT JOIN зон
// без доменов, сортировка по имени; окно темпа и защита от деления на
// ноль в computeRates (короткое окно, нулевая скорость, пустой цикл,
// минимальная ETA); формат таблицы и JSON (null для «н/д»); ошибки Redis
// не прячут сводку, а завершают команду кодом 1. Поведение против живых
// PostgreSQL/Redis — dev-окружение (make status).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
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
	thr     throughput

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
// типы int16/string/bool/int64/float32/float64/[]byte).
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

// statusLenner — streamLener: длины по стримам (или сбой любой длины),
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

	zones, err := fetchZoneStats(context.Background(), pool)
	if err != nil {
		t.Fatalf("fetchZoneStats: %v", err)
	}
	if calls := pool.calls(); len(calls) != 1 || calls[0] != wantZoneStatsSQL {
		t.Fatalf("запросы = %q, хочу один zoneStatsSQL", calls)
	}
	want := []zoneStats{
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
	zones, err := fetchZoneStats(context.Background(), newStatusPool())
	if err != nil {
		t.Fatalf("fetchZoneStats: %v", err)
	}
	if len(zones) != 0 {
		t.Errorf("zones = %+v, хочу пусто", zones)
	}
}

// TestFetchThroughput — одна строка замеров; типы колонок (int64/float64).
func TestFetchThroughput(t *testing.T) {
	pool := newStatusPool()
	pool.thr = throughput{DoneToday: 2279, ElapsedSec: 3720.5, ResultsHour: 6132, RenderedHour: 1025}

	thr, err := fetchThroughput(context.Background(), pool)
	if err != nil {
		t.Fatalf("fetchThroughput: %v", err)
	}
	if calls := pool.calls(); len(calls) != 1 || calls[0] != wantThroughputSQL {
		t.Fatalf("запросы = %q, хочу один throughputSQL", calls)
	}
	if thr != pool.thr {
		t.Errorf("thr = %+v, want %+v", thr, pool.thr)
	}
}

// TestStatusFetchErrors — ошибки пула возвращаются с контекстом команды;
// nil-пул — ошибка, а не паника.
func TestStatusFetchErrors(t *testing.T) {
	boom := errors.New("boom")
	pool := newStatusPool()
	pool.setQueryErr(boom)
	if _, err := fetchZoneStats(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: зоны:") {
		t.Errorf("fetchZoneStats err = %v, хочу контекст «status: зоны:»", err)
	}
	if _, err := fetchThroughput(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: темп:") {
		t.Errorf("fetchThroughput err = %v, хочу контекст «status: темп:»", err)
	}
	if _, err := fetchZoneStats(context.Background(), nil); err == nil {
		t.Error("fetchZoneStats(nil) = nil, хочу ошибку")
	}
	if _, err := fetchThroughput(context.Background(), nil); err == nil {
		t.Error("fetchThroughput(nil) = nil, хочу ошибку")
	}
}

// --- очереди -------------------------------------------------------------------

// TestFetchQueueLens — все рабочие стримы в порядке ТЗ 4.2; ошибка
// останавливает сбор (Redis один); nil-клиент — ошибка.
func TestFetchQueueLens(t *testing.T) {
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:analyze": 3, "q:dead": 1}
	lenner := &statusLenner{lens: lens}
	got, err := fetchQueueLens(context.Background(), lenner)
	if err != nil {
		t.Fatalf("fetchQueueLens: %v", err)
	}
	if !reflect.DeepEqual(got, lens) {
		t.Errorf("lens = %v, want %v", got, lens)
	}
	if order := lenner.order(); !reflect.DeepEqual(order, statusStreams) {
		t.Errorf("порядок опроса = %v, want %v", order, statusStreams)
	}
	if want := []string{"q:dns", "q:http", "q:render", "q:analyze", "q:dead"}; !reflect.DeepEqual(statusStreams, want) {
		t.Errorf("statusStreams = %v, want %v (все стримы ТЗ 4.2)", statusStreams, want)
	}

	_, err = fetchQueueLens(context.Background(), &statusLenner{err: errors.New("connection refused")})
	if err == nil {
		t.Error("ошибка XLEN потеряна")
	}
	if _, err := fetchQueueLens(context.Background(), nil); err == nil {
		t.Error("fetchQueueLens(nil) = nil, хочу ошибку")
	}
}

// --- расчёты ---------------------------------------------------------------------

func TestComputeTotals(t *testing.T) {
	zones := []zoneStats{
		{DomainsTotal: 100, DomainsChecked: 40, Pending: 60, Queued: 1, Done: 39},
		{DomainsTotal: 1000, DomainsChecked: 400, Pending: 500, Queued: 5, Done: 495},
	}
	want := statusTotals{DomainsTotal: 1100, DomainsChecked: 440, Pending: 560, Queued: 6, Done: 534}
	if got := computeTotals(zones); got != want {
		t.Errorf("computeTotals = %+v, want %+v", got, want)
	}
	if got := computeTotals(nil); got != (statusTotals{}) {
		t.Errorf("computeTotals(nil) = %+v, хочу нули", got)
	}
}

// TestComputeRates — защита от деления на ноль и коротких окон: скорость
// доменов только при окне ≥ minSpeedWindowSec; ETA — при известной
// ненулевой скорости (или пустом цикле), округление и минимум 1с.
func TestComputeRates(t *testing.T) {
	// Рабочий случай: 3600 проверок за час → 1/с; остаток 90 → ETA 90с.
	r := computeRates(throughput{DoneToday: 3600, ElapsedSec: 3600, ResultsHour: 7200, RenderedHour: 3600}, 60, 30)
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
	r = computeRates(throughput{ElapsedSec: minSpeedWindowSec - 1, ResultsHour: 3600}, 10, 0)
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
	if r := computeRates(throughput{DoneToday: minSpeedWindowSec, ElapsedSec: minSpeedWindowSec}, 1, 0); !r.DomainsKnown {
		t.Error("окно = minSpeedWindowSec: скорость должна быть известна")
	}

	// Скорость 0 (за окно ни одной проверки): ETA н/д, не паника.
	r = computeRates(throughput{DoneToday: 0, ElapsedSec: 7200}, 5, 0)
	if !r.DomainsKnown || r.DomainsPerSec != 0 {
		t.Errorf("нулевые проверки: Known=%v speed=%v, хочу известную 0", r.DomainsKnown, r.DomainsPerSec)
	}
	if r.ETAKnown {
		t.Error("скорость 0: ETA не должна быть известна (деление на ноль)")
	}

	// Пустой цикл: ETA 0 даже без скорости.
	r = computeRates(throughput{ElapsedSec: 1}, 0, 0)
	if !r.ETAKnown || r.ETASec != 0 {
		t.Errorf("пустой цикл: ETA = %d (%v), хочу 0 (известна)", r.ETASec, r.ETAKnown)
	}

	// Округление вверх/вниз и минимум: 100/3 = 33.3 → 33; 1/3 = 0.3 → 1.
	if r := computeRates(throughput{DoneToday: 3000, ElapsedSec: 1000}, 100, 0); r.ETASec != 33 {
		t.Errorf("ETA 100/3 = %d, хочу 33", r.ETASec)
	}
	if r := computeRates(throughput{DoneToday: 3000, ElapsedSec: 1000}, 1, 0); r.ETASec != 1 {
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
		if got := formatDuration(tc.sec); got != tc.want {
			t.Errorf("formatDuration(%d) = %q, want %q", tc.sec, got, tc.want)
		}
	}
}

// TestPercentMark — доля проверенных; пустая зона — «—».
func TestPercentMark(t *testing.T) {
	cases := []struct {
		checked, total int64
		want           string
	}{
		{40, 100, "40%"},
		{1, 3, "33%"}, // доля до целых процентов
		{0, 0, "—"},
		{5, 0, "—"},
	}
	for _, tc := range cases {
		if got := percentMark(tc.checked, tc.total); got != tc.want {
			t.Errorf("percentMark(%d, %d) = %q, want %q", tc.checked, tc.total, got, tc.want)
		}
	}
}

// --- вывод ------------------------------------------------------------------------

// statusTestSnapshot — согласованный набор данных для тестов вывода:
// две зоны (одна без доменов), замеры и скорости из TestComputeRates.
func statusTestSnapshot() (zones []zoneStats, tot statusTotals, thr throughput, rates statusRates) {
	zones = []zoneStats{
		{ID: 1, Name: "crawl.test", IsActive: true, DomainsTotal: 100, DomainsChecked: 40, Pending: 60, Queued: 2, Done: 38},
		// Зона без доменов (LEFT JOIN): счётчик monitor остался от прошлого
		// наполнения — в итог он всё равно входит.
		{ID: 3, Name: "empty.test", IsActive: false, DomainsTotal: 10, DomainsChecked: 0},
	}
	tot = computeTotals(zones)
	thr = throughput{DoneToday: 3600, ElapsedSec: 3600, ResultsHour: 7200, RenderedHour: 3600}
	rates = computeRates(thr, tot.Pending, tot.Queued) // 1 дом./с → ETA 62с
	return zones, tot, thr, rates
}

// TestWriteStatusTable — формат: заголовок и колонки зон, итого, очереди
// по всем стримам, скорость с окнами, цикл с ETA; выравнивание чисел
// вправо не рвёт колонки итога.
func TestWriteStatusTable(t *testing.T) {
	zones, tot, thr, rates := statusTestSnapshot()
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:analyze": 3, "q:dead": 1}

	var b bytes.Buffer
	writeStatusTable(&b, zones, tot, thr, rates, lens)
	got := b.String()

	for _, want := range []string{
		"зона", "pending", "queued", "done", // заголовок
		"crawl.test +", "empty.test -",
		"60", "38", "итого", "40%", // агрегаты и итог
		"q:dns 5", "q:render 12", "q:dead 1", // очереди
		"доменов/с          1.0   (3600 проверено с начала суток, прошло 1ч 00м)",
		"HTTP-результатов/с 2.0   (7200 за последний час)",
		"рендеров/с         1.0   (3600 с итоговым DOM за час)",
		"цикл: осталось 62 (pending+queued); ETA ≈ 1м (по 1.0 дом./с)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("таблица не содержит %q:\n%s", want, got)
		}
	}

	// Итог выровнен по правому краю числовых колонок: ширина колонки
	// «всего» — не меньше самого длинного значения (100), итого (110)
	// расширяет её; строка итога заканчивается числами done.
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "итого") {
			if !strings.HasSuffix(line, "38") {
				t.Errorf("строка итога не кончается done: %q", line)
			}
			if !strings.Contains(line, "110") {
				t.Errorf("строка итога без суммы всего: %q", line)
			}
		}
	}
}

// TestWriteStatusTableDegraded — Redis недоступен: очереди «н/д», сводка
// по БД печатается; пустой цикл помечается завершённым.
func TestWriteStatusTableDegraded(t *testing.T) {
	_, tot, thr, _ := statusTestSnapshot()
	rates := computeRates(thr, 0, 0) // цикл пуст

	var b bytes.Buffer
	writeStatusTable(&b, nil, tot, thr, rates, nil)
	got := b.String()
	for _, want := range []string{
		"н/д (Redis недоступен)",
		"итого",
		"доменов/с          1.0",
		"цикл: осталось 0 (pending и queued пусты) — проверять нечего",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("деградированный вывод не содержит %q:\n%s", want, got)
		}
	}

	// Короткое окно суток — скорость и ETA честно «н/д».
	thr = throughput{ElapsedSec: 300, ResultsHour: 0}
	rates = computeRates(thr, 10, 0)
	b.Reset()
	writeStatusTable(&b, nil, statusTotals{}, thr, rates, nil)
	got = b.String()
	for _, want := range []string{
		"н/д   (окно с начала суток короче 10м)",
		"ETA н/д (скорость неизвестна или 0)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("вывод без скорости не содержит %q:\n%s", want, got)
		}
	}
}

// TestWriteStatusJSON — машинный вывод: поля зон/итога/очередей/темпа,
// округление скоростей, null для «н/д» (nil-карта очередей, скорости
// без окна, ETA без скорости).
func TestWriteStatusJSON(t *testing.T) {
	zones, tot, thr, rates := statusTestSnapshot()
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:analyze": 3, "q:dead": 1}

	var b bytes.Buffer
	if err := writeStatusJSON(&b, zones, tot, thr, rates, lens); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}

	type zoneJSON struct {
		Name           string `json:"name"`
		NameUnicode    string `json:"name_unicode"`
		Active         bool   `json:"active"`
		DomainsTotal   int64  `json:"domains_total"`
		DomainsChecked int64  `json:"domains_checked"`
		Pending        int64  `json:"pending"`
		Queued         int64  `json:"queued"`
		Done           int64  `json:"done"`
	}
	var doc struct {
		Zones      []zoneJSON       `json:"zones"`
		Totals     map[string]int64 `json:"totals"`
		Queues     map[string]int64 `json:"queues"`
		Throughput struct {
			DoneToday   int64   `json:"done_today"`
			ElapsedSec  float64 `json:"elapsed_sec"`
			ResultsHour int64   `json:"results_hour"`
		} `json:"throughput"`
		Rates struct {
			DomainsPerSec *float64 `json:"domains_per_sec"`
			ResultsPerSec float64  `json:"results_per_sec"`
			Remaining     int64    `json:"remaining"`
			ETASec        *int64   `json:"eta_sec"`
		} `json:"rates"`
	}
	if err := json.Unmarshal(b.Bytes(), &doc); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, b.String())
	}
	if len(doc.Zones) != 2 || doc.Zones[0].Name != "crawl.test" || !doc.Zones[0].Active {
		t.Errorf("zones = %+v", doc.Zones)
	}
	if doc.Zones[1].NameUnicode != "" {
		t.Errorf("name_unicode пустой зоны = %q, хочу \"\" (omitempty вне теста)", doc.Zones[1].NameUnicode)
	}
	if doc.Totals["pending"] != 60 || doc.Totals["domains_total"] != 110 {
		t.Errorf("totals = %v", doc.Totals)
	}
	if doc.Queues["q:render"] != 12 || len(doc.Queues) != 5 {
		t.Errorf("queues = %v", doc.Queues)
	}
	if doc.Throughput.DoneToday != 3600 || doc.Throughput.ElapsedSec != 3600 {
		t.Errorf("throughput = %+v", doc.Throughput)
	}
	if doc.Rates.DomainsPerSec == nil || *doc.Rates.DomainsPerSec != 1 {
		t.Errorf("domains_per_sec = %v, хочу 1", doc.Rates.DomainsPerSec)
	}
	if doc.Rates.ResultsPerSec != 2 {
		t.Errorf("results_per_sec = %v, хочу 2", doc.Rates.ResultsPerSec)
	}
	if doc.Rates.ETASec == nil || *doc.Rates.ETASec != 62 {
		t.Errorf("eta_sec = %v, хочу 62", doc.Rates.ETASec)
	}

	// Деградация: короткое окно + недоступный Redis → null-поля, остаток
	// без ETA; json при этом валиден.
	b.Reset()
	thr = throughput{ElapsedSec: 300}
	rates = computeRates(thr, 10, 0)
	if err := writeStatusJSON(&b, nil, statusTotals{}, thr, rates, nil); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}
	var deg struct {
		Zones  []struct{}       `json:"zones"`
		Queues map[string]int64 `json:"queues"`
		Rates  struct {
			DomainsPerSec *float64 `json:"domains_per_sec"`
			Remaining     int64    `json:"remaining"`
			ETASec        *int64   `json:"eta_sec"`
		} `json:"rates"`
	}
	if err := json.Unmarshal(b.Bytes(), &deg); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, b.String())
	}
	if len(deg.Zones) != 0 {
		t.Errorf("zones = %d, хочу пустой массив", len(deg.Zones))
	}
	if deg.Queues != nil {
		t.Errorf("queues = %v, хочу null", deg.Queues)
	}
	if deg.Rates.DomainsPerSec != nil || deg.Rates.ETASec != nil {
		t.Errorf("rates = %+v, хочу null/null", deg.Rates)
	}
	if deg.Rates.Remaining != 10 {
		t.Errorf("remaining = %d, хочу 10", deg.Rates.Remaining)
	}
}

// jsonUnmarshalStrict удалён: тесты используют json.Unmarshal напрямую.

// --- роутинг/коды -------------------------------------------------------------------

// TestRunStatus — путь выполнения без инфраструктуры: команды нужен
// конфиг (DSN PostgreSQL, Redis) — при его отсутствии ошибка выполнения
// (код 1) до соединений (стиль TestRunReanalyze); --json не ломает путь.
func TestRunStatus(t *testing.T) {
	// Явно отсутствующий конфиг-файл: детерминированный отказ до любых
	// соединений (в контейнере tools переменные CRAWLER_* могли бы собрать
	// конфигурацию из окружения — тест не должен ходить в живой стек).
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	var stdout, stderr bytes.Buffer
	code := run([]string{"status"}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitError, stderr.String())
	}
	if !strings.Contains(stderr.String(), "status: config:") {
		t.Errorf("stderr не содержит контекст конфигурации:\n%s", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout до соединения не пуст:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"status", "--json"}, &stdout, &stderr)
	if code != exitError || !strings.Contains(stderr.String(), "status: config:") {
		t.Errorf("--json: код = %d, stderr:\n%s; хочу ошибку конфигурации (код 1)", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("--json: stdout до соединения не пуст:\n%s", stdout.String())
	}
}

// TestStatusHelpMentionsStreams — справка перечисляет все стримы и ETA
// (содержание целиком проверяет TestRunHelpPerCommand в main_test.go).
func TestStatusHelpMentionsStreams(t *testing.T) {
	for _, s := range statusStreams {
		if !strings.Contains(statusHelp, s) {
			t.Errorf("statusHelp не упоминает стрим %q", s)
		}
	}
	if !strings.Contains(statusHelp, "ETA") {
		t.Error("statusHelp не описывает ETA цикла")
	}
}

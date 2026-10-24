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
	"time"

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
	"z.is_active, z.domains_total, z.domains_pending, z.domains_queued, z.domains_checked " +
	"FROM zones AS z ORDER BY z.name"

const wantThroughputSQL = "SELECT " +
	"(SELECT count(*) FROM domains WHERE last_checked_at = current_date) AS done_today, " +
	"extract(epoch FROM (now() - date_trunc('day', now())))::float8 AS elapsed_sec, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour') AS results_hour, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour' AND rendered) AS rendered_hour"

const wantDomainStatusSQL = "SELECT name, value FROM crawler_counters WHERE name LIKE 'status\\_%' ORDER BY name"

const wantLaravelTotalsSQL = "SELECT (SELECT coalesce(sum(domains_laravel), 0) FROM zones), " +
	"count(*) FILTER (WHERE updated_at >= now() - interval '24 hours') " +
	"FROM laravel_domains"

const wantLaravelRecentSQL = "SELECT d.label || '.' || z.name AS domain, z.name AS zone, " +
	"ld.score, ld.ver_min, ld.ver_max, ld.updated_at " +
	"FROM laravel_domains AS ld " +
	"JOIN domains AS d ON d.zone_id = ld.zone_id AND d.id = ld.domain_id " +
	"JOIN zones AS z ON z.id = ld.zone_id " +
	"ORDER BY ld.updated_at DESC LIMIT 10"

const wantIngestRecentSQL = "SELECT path, action, lines_total, added, existed, deleted, rejected, processed_at " +
	"FROM ingest_files ORDER BY processed_at DESC NULLS LAST, id DESC LIMIT 10"

// --- фейковый пул (db.DB) ----------------------------------------------------

// statusZoneSeed — зона в фейке: счётчики и атрибуты отображения.
type statusZoneSeed struct {
	id      int16
	name    string
	unicode string
	active  bool
	total   int64
	pending int64
	queued  int64
	checked int64
	hasUni  bool // name_unicode не NULL (в SQL — coalesce в '')
}

// statusStatusSeed — строка счётчика распределения статусов (§5.5,
// T-178): имя статуса легенды и значение строки crawler_counters.
type statusStatusSeed struct {
	name  string
	count int64
}

// statusLaravelSeed — строка laravel_domains для секции Laravel (§5.6):
// label домена и его зона — JOIN выборки последних находок.
type statusLaravelSeed struct {
	zone      int16
	domain    int64
	label     string
	score     float32
	verMin    *string
	verMax    *string
	updatedAt time.Time
}

// statusIngestSeed — строка ingest_files (§5.7); указатели — NULL-колонки
// схемы (журнал пишет полными строками, NULL легален для ручных вставок).
type statusIngestSeed struct {
	id          int64
	path        *string
	action      *int16
	lines       *int64
	added       *int64
	existed     *int64
	deleted     *int64
	rejected    *int64
	processedAt *time.Time
}

// statusGlitch — деградация формы ответа одного запроса (стиль фейков
// monitor, T-M01/T-M02): ошибка самого Query, пустой набор строк (Next
// сразу ложен), нечитаемая колонка первой строки (ошибка Scan), ошибка
// итерации (rows.Err).
type statusGlitch struct {
	queryErr error
	empty    bool
	badRow   bool
	rowsErr  error
}

// statusPool — db.DB без БД: живая семантика zoneStatsSQL (GROUP BY зоны,
// pending без deleted, queued/done без фильтра статуса, LEFT JOIN — зона
// без доменов остаётся, ORDER BY name), throughputSQL (готовые значения)
// и секций дашборда (§5.5–5.7): распределение статусов, счётчики/окно
// Laravel по часам фейка, сортировка последних находок и ingest-файлов.
type statusPool struct {
	zones       []statusZoneSeed
	thr         Throughput
	domStatuses []statusStatusSeed
	laravel     []statusLaravelSeed
	zoneLaravel int64 // сумма zones.domains_laravel (laravelTotalsSQL, T-178)
	ingest      []statusIngestSeed
	now         time.Time // часы сервера БД для окон laravelTotalsSQL

	// glitches — деградации ответов по тексту запроса.
	glitches map[string]statusGlitch

	mu        sync.Mutex
	queryErr  error
	queries   []string
	execCalls int
	closes    int // сколько раз закрыты set'ы строк (rows.Close)
}

var _ db.DB = (*statusPool)(nil)

func newStatusPool() *statusPool {
	return &statusPool{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
}

func (p *statusPool) seedZone(id int16, name string, active bool, total, pending, queued, checked int64) {
	p.zones = append(p.zones, statusZoneSeed{id: id, name: name, active: active,
		total: total, pending: pending, queued: queued, checked: checked})
}

// seedZoneUnicode — зона с непустым name_unicode (NULL vs значение).
func (p *statusPool) seedZoneUnicode(id int16, name, unicode string, active bool, total, pending, queued, checked int64) {
	p.zones = append(p.zones, statusZoneSeed{id: id, name: name, unicode: unicode, active: active,
		total: total, pending: pending, queued: queued, checked: checked, hasUni: true})
}

func (p *statusPool) setQueryErr(err error) {
	p.mu.Lock()
	p.queryErr = err
	p.mu.Unlock()
}

// setGlitch — включить деградацию ответа конкретного запроса.
func (p *statusPool) setGlitch(sql string, g statusGlitch) {
	p.mu.Lock()
	if p.glitches == nil {
		p.glitches = make(map[string]statusGlitch)
	}
	p.glitches[sql] = g
	p.mu.Unlock()
}

// closeCount — сколько раз закрыты set'ы строк: FetchLaravelStats
// закрывает первый set ДО второго запроса (без defer) — Close каждого
// пути наблюдаем через счётчик.
func (p *statusPool) closeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closes
}

// bumpClose — счётчик Close из statusRows (вызывается вне замка Query:
// set закрывает вызывающий после возврата Query).
func (p *statusPool) bumpClose() {
	p.mu.Lock()
	p.closes++
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
	if g := p.glitches[sql]; g.queryErr != nil {
		return nil, g.queryErr
	}
	switch sql {
	case zoneStatsSQL:
		return p.decorate(sql, p.zoneRowsData()), nil
	case throughputSQL:
		return p.decorate(sql, [][]any{{p.thr.DoneToday, p.thr.ElapsedSec, p.thr.ResultsHour, p.thr.RenderedHour}}), nil
	case domainStatusSQL:
		rows := make([][]any, len(p.domStatuses))
		for i, s := range p.domStatuses {
			rows[i] = []any{"status_" + s.name, s.count}
		}
		return p.decorate(sql, rows), nil
	case laravelTotalsSQL:
		var last24h int64
		for _, l := range p.laravel {
			if !l.updatedAt.Before(p.now.Add(-24 * time.Hour)) {
				last24h++
			}
		}
		return p.decorate(sql, [][]any{{p.zoneLaravel, last24h}}), nil
	case laravelRecentSQL:
		// Модель pool_size=1: второй запрос при открытом первом set
		// занял бы единственное соединение (дедлок, комментарий
		// FetchLaravelStats) — фейк требует, чтобы первый set был закрыт
		// ДО второго запроса: порядок, а не только сумма Close.
		if p.closes != 1 {
			return nil, fmt.Errorf("statusPool: laravelRecentSQL при %d закрытых set'ах, want 1 (первый set закрывается до второго запроса)", p.closes)
		}
		return p.decorate(sql, p.laravelRowsData()), nil
	case ingestRecentSQL:
		return p.decorate(sql, p.ingestRowsData()), nil
	default:
		return nil, fmt.Errorf("statusPool: неожидаемый SQL Query: %s", sql)
	}
}

// statusBadColumn — маркер нечитаемой колонки: Scan фейка отвечает
// ошибкой (как pgx на несовместимый тип приёмника).
type statusBadColumn struct{}

// decorate собирает statusRows запроса с его деградацией: empty — набор
// без строк (Next сразу ложен), badRow — первая строка с нечитаемой
// колонкой (ошибка Scan), rowsErr — ошибка итерации; Close считается
// в пул (наблюдаемость закрытия set'а).
func (p *statusPool) decorate(sql string, rows [][]any) *statusRows {
	r := &statusRows{rows: rows, onClose: p.bumpClose}
	g := p.glitches[sql]
	if g.empty {
		r.rows = nil
	}
	if g.badRow && len(r.rows) > 0 {
		row := append([]any(nil), r.rows[0]...)
		row[0] = statusBadColumn{}
		r.rows[0] = row
	}
	r.err = g.rowsErr
	return r
}

// zoneRowsData применяет семантику zoneStatsSQL к сидам: справочник
// зон с инкрементальными счётчиками (T-178) — без прохода domains.
func (p *statusPool) zoneRowsData() [][]any {
	sorted := append([]statusZoneSeed(nil), p.zones...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
	rows := make([][]any, len(sorted))
	for i, z := range sorted {
		uni := ""
		if z.hasUni {
			uni = z.unicode // coalesce(name_unicode, '')
		}
		rows[i] = []any{z.id, z.name, uni, z.active, z.total, z.pending, z.queued, z.checked}
	}
	return rows
}

// laravelRowsData применяет семантику laravelRecentSQL: JOIN домена и
// зоны по ключу строки, сортировка по updated_at DESC, LIMIT 10.
func (p *statusPool) laravelRowsData() [][]any {
	names := make(map[int16]string, len(p.zones))
	for _, z := range p.zones {
		names[z.id] = z.name
	}
	sorted := append([]statusLaravelSeed(nil), p.laravel...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].updatedAt.After(sorted[j].updatedAt) })
	if len(sorted) > 10 {
		sorted = sorted[:10]
	}
	rows := make([][]any, len(sorted))
	for i, l := range sorted {
		rows[i] = []any{l.label + "." + names[l.zone], names[l.zone],
			l.score, l.verMin, l.verMax, l.updatedAt}
	}
	return rows
}

// ingestRowsData применяет семантику ingestRecentSQL: processed_at DESC
// (NULL последними), id DESC внутри одной секунды, LIMIT 10.
func (p *statusPool) ingestRowsData() [][]any {
	sorted := append([]statusIngestSeed(nil), p.ingest...)
	sort.SliceStable(sorted, func(i, j int) bool {
		pi, pj := sorted[i].processedAt, sorted[j].processedAt
		switch {
		case pi == nil && pj != nil:
			return false // NULLS LAST
		case pi != nil && pj == nil:
			return true
		case pi != nil && pj != nil && !pi.Equal(*pj):
			return pi.After(*pj)
		default:
			return sorted[i].id > sorted[j].id
		}
	})
	if len(sorted) > 10 {
		sorted = sorted[:10]
	}
	rows := make([][]any, len(sorted))
	for i, s := range sorted {
		rows[i] = []any{s.path, s.action, s.lines, s.added, s.existed, s.deleted, s.rejected, s.processedAt}
	}
	return rows
}

// statusRows — pgx.Rows с заготовленными строками (Scan позиционный,
// типы int16/string/bool/int64/float32/float64/[]byte и указатели для
// NULL-колонок — одинарные значения и двойные для разворота NULL).
// Счётчик Close считает вызовы Close (не состояние соединения): явные
// закрытия set'ов наблюдаются по одному на каждый путь выхода.
// Копия общего фейка тестов CLI (cmd/crawlerctl): тест-файлы разных
// пакетов не импортируют друг друга.
type statusRows struct {
	rows    [][]any
	i       int
	err     error  // ответ Err — деградация итерации
	onClose func() // счётчик Close пула
}

func (r *statusRows) Close() {
	if r.onClose != nil {
		r.onClose()
	}
}
func (r *statusRows) Err() error                                   { return r.err }
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
		if _, bad := v.(statusBadColumn); bad {
			return fmt.Errorf("statusRows: колонка %d нечитаема", i)
		}
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
		case *time.Time:
			*dst = v.(time.Time)
		case **string:
			assignPtr(dst, v)
		case **int16:
			assignPtr(dst, v)
		case **int64:
			assignPtr(dst, v)
		case **time.Time:
			assignPtr(dst, v)
		default:
			return fmt.Errorf("statusRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// assignPtr — разворот NULL-колонок: значение-указатель из строки фейка
// (nil — NULL) в приёмник-указатель-на-указатель pgx.
func assignPtr[T any](dst **T, v any) {
	if p, ok := v.(*T); ok {
		*dst = p
		return
	}
	*dst = nil
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
	if domainStatusSQL != wantDomainStatusSQL {
		t.Errorf("domainStatusSQL:\n%s\nwant:\n%s", domainStatusSQL, wantDomainStatusSQL)
	}
	if laravelTotalsSQL != wantLaravelTotalsSQL {
		t.Errorf("laravelTotalsSQL:\n%s\nwant:\n%s", laravelTotalsSQL, wantLaravelTotalsSQL)
	}
	if laravelRecentSQL != wantLaravelRecentSQL {
		t.Errorf("laravelRecentSQL:\n%s\nwant:\n%s", laravelRecentSQL, wantLaravelRecentSQL)
	}
	if ingestRecentSQL != wantIngestRecentSQL {
		t.Errorf("ingestRecentSQL:\n%s\nwant:\n%s", ingestRecentSQL, wantIngestRecentSQL)
	}
}

// TestFetchZoneStats — сводка зон из счётчиков справочника (T-178):
// без JOIN domains; pending/queued/checked — колонки зон (базовое
// множество status <> deleted ведут дельты и реконсиляция), Done —
// та же колонка domains_checked; сортировка по имени, unicode и флаг
// активности дочитываются.
func TestFetchZoneStats(t *testing.T) {
	pool := newStatusPool()
	pool.seedZoneUnicode(2, "com", "com", true, 1000, 3, 1, 400)
	pool.seedZone(1, "crawl.test", true, 100, 2, 2, 40)
	pool.seedZone(3, "empty.test", false, 0, 0, 0, 0) // зона без доменов

	zones, err := FetchZoneStats(context.Background(), pool)
	if err != nil {
		t.Fatalf("FetchZoneStats: %v", err)
	}
	if calls := pool.calls(); len(calls) != 1 || calls[0] != wantZoneStatsSQL {
		t.Fatalf("запросы = %q, хочу один zoneStatsSQL", calls)
	}
	want := []ZoneStats{
		{ID: 2, Name: "com", NameUnicode: "com", IsActive: true, DomainsTotal: 1000,
			DomainsChecked: 400, Pending: 3, Queued: 1, Done: 400},
		{ID: 1, Name: "crawl.test", IsActive: true, DomainsTotal: 100, DomainsChecked: 40,
			Pending: 2, Queued: 2, Done: 40},
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

// TestFetchRowDegradations — деградации формы ответа на уровне строк
// (стиль фейков monitor): нечитаемая колонка — ошибка Scan с контекстом
// «чтение», ошибка итерации (rows.Err) — ошибка выборки целиком с
// исходным текстом; пустой скалярный агрегат темпа — «пустой ответ»,
// а не «чтение» (мутанты вырезания return глотали бы ошибку или
// подменяли её текст).
func TestFetchRowDegradations(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		glitch  statusGlitch
		wantErr string
	}{
		{"зоны: нечитаемая строка", zoneStatsSQL, statusGlitch{badRow: true}, "status: зоны: чтение:"},
		{"зоны: ошибка итерации", zoneStatsSQL, statusGlitch{rowsErr: errors.New("соединение сброшено")}, "status: зоны: соединение сброшено"},
		{"темп: пустой ответ", throughputSQL, statusGlitch{empty: true}, "status: темп: пустой ответ"},
		{"темп: нечитаемая строка", throughputSQL, statusGlitch{badRow: true}, "status: темп: чтение:"},
		{"темп: ошибка итерации", throughputSQL, statusGlitch{rowsErr: errors.New("соединение сброшено")}, "status: темп: соединение сброшено"},
		{"статусы: нечитаемая строка", domainStatusSQL, statusGlitch{badRow: true}, "status: статусы: чтение:"},
		{"статусы: ошибка итерации", domainStatusSQL, statusGlitch{rowsErr: errors.New("соединение сброшено")}, "status: статусы: соединение сброшено"},
		{"ingest: нечитаемая строка", ingestRecentSQL, statusGlitch{badRow: true}, "status: ingest: чтение:"},
		{"ingest: ошибка итерации", ingestRecentSQL, statusGlitch{rowsErr: errors.New("соединение сброшено")}, "status: ingest: соединение сброшено"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newStatusPool()
			pool.seedZone(1, "crawl.test", true, 1, 1, 0, 0)
			pool.domStatuses = []statusStatusSeed{{model.StatusNew.String(), 1}}
			pool.ingest = []statusIngestSeed{{id: 1}}
			pool.setGlitch(tc.sql, tc.glitch)
			var err error
			switch tc.sql {
			case zoneStatsSQL:
				_, err = FetchZoneStats(context.Background(), pool)
			case throughputSQL:
				_, err = FetchThroughput(context.Background(), pool)
			case domainStatusSQL:
				_, err = FetchDomainStatuses(context.Background(), pool)
			case ingestRecentSQL:
				_, err = FetchIngestReports(context.Background(), pool)
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
			}
		})
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

// --- секции дашборда (ТЗД §5.5–5.7) ----------------------------------------------

// strPtr — указатель строки (NULL-колонки сидов).
func strPtr(s string) *string { return &s }

// int16Ptr/int64Ptr — указатели чисел (NULL-колонки сидов ingest).
func int16Ptr(v int16) *int16 { return &v }

func int64Ptr(v int64) *int64 { return &v }

// TestFetchDomainStatuses — распределение по именам ТЗ 6.2 из строк
// status_* crawler_counters (T-178): все статусы model.Statuses с нулями
// (полная легенда), отсутствующие строки — 0; строка с именем вне
// легенды не отображается (реконсиляция пишет только model.Statuses —
// чужая строка это ручная правка таблицы); ошибки и nil-пул.
func TestFetchDomainStatuses(t *testing.T) {
	pool := newStatusPool()
	pool.domStatuses = []statusStatusSeed{
		{model.StatusNew.String(), 1000},
		{model.StatusAlive.String(), 40},
		{model.StatusDeleted.String(), 3},
		{"unknown", 1}, // имя вне легенды model.Statuses — не отображается
	}
	got, err := FetchDomainStatuses(context.Background(), pool)
	if err != nil {
		t.Fatalf("FetchDomainStatuses: %v", err)
	}
	if calls := pool.calls(); len(calls) != 1 || calls[0] != wantDomainStatusSQL {
		t.Fatalf("запросы = %q, хочу один domainStatusSQL", calls)
	}
	want := map[string]int64{
		"new": 1000, "not_delegated": 0, "delegated_no_address": 0, "no_http": 0,
		"alive": 40, "temp_error": 0, "deleted": 3, "invalid": 0,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("statuses = %v\nwant %v", got, want)
	}

	// Пустая таблица — легенда из нулей, не пустая карта.
	got, err = FetchDomainStatuses(context.Background(), newStatusPool())
	if err != nil {
		t.Fatalf("FetchDomainStatuses (пусто): %v", err)
	}
	if len(got) != len(model.Statuses) {
		t.Fatalf("пустая таблица: %d статусов, хочу легенду %d", len(got), len(model.Statuses))
	}
	for name, n := range got {
		if n != 0 {
			t.Errorf("статус %s = %d, хочу 0", name, n)
		}
	}

	// Ошибки пула и nil-пул.
	pool = newStatusPool()
	pool.setQueryErr(errors.New("boom"))
	if _, err := FetchDomainStatuses(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: статусы:") {
		t.Errorf("ошибка пула = %v, хочу контекст «status: статусы:»", err)
	}
	if _, err := FetchDomainStatuses(context.Background(), nil); err == nil {
		t.Error("FetchDomainStatuses(nil) = nil, хочу ошибку")
	}
}

// TestFetchLaravelStats — счётчики (всего и окно 24 ч по updated_at),
// последние находки: домен label.зона через JOIN, сортировка updated_at
// DESC, LIMIT 10, ver_min/ver_max NULL → пустые строки.
func TestFetchLaravelStats(t *testing.T) {
	pool := newStatusPool()
	pool.seedZone(1, "crawl.test", true, 0, 0, 0, 0)
	pool.zoneLaravel = 3 // сумма zones.domains_laravel (T-178): Total секции
	fresh := pool.now.Add(-2 * time.Hour)
	stale := pool.now.Add(-30 * time.Hour)
	pool.laravel = []statusLaravelSeed{
		{zone: 1, domain: 2, label: "old", score: 0.7, updatedAt: stale},
		{zone: 1, domain: 1, label: "shop", score: 0.9, verMin: strPtr("9.19"), verMax: strPtr("11"), updatedAt: fresh},
		{zone: 1, domain: 3, label: "mid", score: 0.8, verMin: strPtr("10"), updatedAt: pool.now.Add(-25 * time.Hour)},
	}
	st, err := FetchLaravelStats(context.Background(), pool)
	if err != nil {
		t.Fatalf("FetchLaravelStats: %v", err)
	}
	if calls := pool.calls(); !reflect.DeepEqual(calls, []string{wantLaravelTotalsSQL, wantLaravelRecentSQL}) {
		t.Fatalf("запросы = %q, хочу [laravelTotalsSQL laravelRecentSQL]", calls)
	}
	// Первый set закрывается ДО второго запроса, второй — defer: ровно
	// два Close на успешном проходе (удержание set'а занимало бы
	// соединение пула — комментарий FetchLaravelStats).
	if n := pool.closeCount(); n != 2 {
		t.Errorf("закрытий set'ов = %d, want 2 (агрегат до второго запроса + последние через defer)", n)
	}
	if st.Total != 3 || st.Last24h != 1 {
		t.Errorf("счётчики = %d/%d, хочу 3/1 (окно 24 ч по updated_at)", st.Total, st.Last24h)
	}
	want := []LaravelRecent{
		{Domain: "shop.crawl.test", Zone: "crawl.test", Score: 0.9, VerMin: "9.19", VerMax: "11", UpdatedAt: fresh},
		{Domain: "mid.crawl.test", Zone: "crawl.test", Score: 0.8, VerMin: "10", UpdatedAt: pool.now.Add(-25 * time.Hour)},
		{Domain: "old.crawl.test", Zone: "crawl.test", Score: 0.7, UpdatedAt: stale},
	}
	if !reflect.DeepEqual(st.Recent, want) {
		t.Errorf("recent =\n%+v\nwant\n%+v", st.Recent, want)
	}

	// Пустая таблица: нули и пустой срез последних (не nil — секция жива).
	st, err = FetchLaravelStats(context.Background(), newStatusPool())
	if err != nil {
		t.Fatalf("FetchLaravelStats (пусто): %v", err)
	}
	if st.Total != 0 || st.Last24h != 0 || len(st.Recent) != 0 {
		t.Errorf("пустая таблица: %+v, хочу нули и пустой recent", st)
	}

	// Ошибка любого из двух запросов — ошибка секции целиком.
	pool = newStatusPool()
	pool.seedZone(1, "crawl.test", true, 0, 0, 0, 0)
	pool.laravel = []statusLaravelSeed{{zone: 1, domain: 1, label: "shop", updatedAt: fresh}}
	pool.setQueryErr(errors.New("boom"))
	if _, err := FetchLaravelStats(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: laravel:") {
		t.Errorf("ошибка пула = %v, хочу контекст «status: laravel:»", err)
	}
	if _, err := FetchLaravelStats(context.Background(), nil); err == nil {
		t.Error("FetchLaravelStats(nil) = nil, хочу ошибку")
	}
}

// TestFetchLaravelStatsDegradations — деградации обоих запросов секции:
// пустой агрегат различает rows-ошибку (реальная причина ценнее
// синтетической) и честное «пусто»; первый set закрывается ДО второго
// запроса (без defer) — Close каждого пути выхода виден счётчиком
// фейка (мутанты вырезания rows.Close глотали бы закрытие set'а,
// вырезание return — ошибку целиком).
func TestFetchLaravelStatsDegradations(t *testing.T) {
	seeded := func(glitches map[string]statusGlitch) *statusPool {
		pool := newStatusPool()
		pool.seedZone(1, "crawl.test", true, 0, 0, 0, 0)
		pool.laravel = []statusLaravelSeed{{zone: 1, domain: 1, label: "shop", updatedAt: pool.now}}
		for sql, g := range glitches {
			pool.setGlitch(sql, g)
		}
		return pool
	}
	ctx := context.Background()

	// Пустой агрегат без rows-ошибки — честное «пусто».
	pool := seeded(map[string]statusGlitch{laravelTotalsSQL: {empty: true}})
	_, err := FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: пустой ответ агрегата") {
		t.Fatalf("пустой агрегат: ошибка = %v, want «пустой ответ агрегата»", err)
	}
	if n := pool.closeCount(); n != 1 {
		t.Errorf("пустой агрегат: Close = %d, want 1 (set закрыт до синтетической ошибки)", n)
	}

	// Пустой агрегат с rows-ошибкой — ошибка уровня строк ценнее.
	pool = seeded(map[string]statusGlitch{laravelTotalsSQL: {empty: true, rowsErr: errors.New("соединение сброшено")}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: соединение сброшено") {
		t.Fatalf("пустой агрегат с rows-ошибкой: ошибка = %v, want причину уровня строк", err)
	}

	// Нечитаемая строка агрегата: set закрывается до возврата ошибки.
	pool = seeded(map[string]statusGlitch{laravelTotalsSQL: {badRow: true}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: чтение:") {
		t.Fatalf("нечитаемая строка агрегата: ошибка = %v, want «чтение»", err)
	}
	if n := pool.closeCount(); n != 1 {
		t.Errorf("нечитаемая строка агрегата: Close = %d, want 1", n)
	}

	// Ошибка итерации агрегата — после успешного Scan, set закрыт.
	pool = seeded(map[string]statusGlitch{laravelTotalsSQL: {rowsErr: errors.New("соединение сброшено")}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: соединение сброшено") {
		t.Fatalf("ошибка итерации агрегата: ошибка = %v, want причину уровня строк", err)
	}
	if n := pool.closeCount(); n != 1 {
		t.Errorf("ошибка итерации агрегата: Close = %d, want 1", n)
	}

	// Ошибка второго запроса — контекст «последние».
	pool = seeded(map[string]statusGlitch{laravelRecentSQL: {queryErr: errors.New("бд недоступна")}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: последние: бд недоступна") {
		t.Fatalf("ошибка второго запроса: ошибка = %v, want «последние»", err)
	}

	// Нечитаемая строка последних находок.
	pool = seeded(map[string]statusGlitch{laravelRecentSQL: {badRow: true}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: последние: чтение:") {
		t.Fatalf("нечитаемая строка последних: ошибка = %v, want «чтение»", err)
	}

	// Ошибка итерации последних находок.
	pool = seeded(map[string]statusGlitch{laravelRecentSQL: {rowsErr: errors.New("соединение сброшено")}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: последние: соединение сброшено") {
		t.Fatalf("ошибка итерации последних: ошибка = %v, want причину уровня строк", err)
	}
}

// TestFetchIngestReports — последние файлы: processed_at DESC (NULL
// последними), id DESC при равенстве, LIMIT 10; действия add/delete
// по модельным именам, NULL-колонки — нулевые значения.
func TestFetchIngestReports(t *testing.T) {
	pool := newStatusPool()
	at := func(h time.Duration) *time.Time {
		t := pool.now.Add(-h * time.Hour)
		return &t
	}
	pool.ingest = []statusIngestSeed{
		// NULL processed_at — «последним», несмотря на большой id.
		{id: 77},
		{id: 3, path: strPtr("crawl.test/add/three.txt"), action: int16Ptr(int16(model.ActionAdd)),
			lines: int64Ptr(30), added: int64Ptr(28), existed: int64Ptr(2), rejected: int64Ptr(0), processedAt: at(1)},
		// Та же секунда, что id 3 — порядок по id DESC.
		{id: 5, path: strPtr("crawl.test/delete/five.txt"), action: int16Ptr(int16(model.ActionDelete)),
			lines: int64Ptr(50), deleted: int64Ptr(49), rejected: int64Ptr(1), processedAt: at(1)},
		{id: 4, path: strPtr("crawl.test/add/four.txt"), action: int16Ptr(int16(model.ActionAdd)),
			lines: int64Ptr(10), added: int64Ptr(10), processedAt: at(5)},
	}
	got, err := FetchIngestReports(context.Background(), pool)
	if err != nil {
		t.Fatalf("FetchIngestReports: %v", err)
	}
	if calls := pool.calls(); len(calls) != 1 || calls[0] != wantIngestRecentSQL {
		t.Fatalf("запросы = %q, хочу один ingestRecentSQL", calls)
	}
	want := []IngestReport{
		{Path: "crawl.test/delete/five.txt", Action: "delete", LinesTotal: 50, Deleted: 49, Rejected: 1, ProcessedAt: *at(1)},
		{Path: "crawl.test/add/three.txt", Action: "add", LinesTotal: 30, Added: 28, Existed: 2, ProcessedAt: *at(1)},
		{Path: "crawl.test/add/four.txt", Action: "add", LinesTotal: 10, Added: 10, ProcessedAt: *at(5)},
		{}, // NULL processed_at — последняя строка, колонки развёрнуты в нули
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("reports =\n%+v\nwant\n%+v", got, want)
	}

	// Код действия вне таблицы ТЗ 6.1 — имя Action(N), не паника.
	pool2 := newStatusPool()
	pool2.ingest = []statusIngestSeed{{id: 1, action: int16Ptr(7), processedAt: at(1)}}
	got, err = FetchIngestReports(context.Background(), pool2)
	if err != nil {
		t.Fatalf("FetchIngestReports (чужой код): %v", err)
	}
	if len(got) != 1 || got[0].Action != "Action(7)" {
		t.Errorf("чужой код действия: %+v, хочу Action(7)", got)
	}

	pool3 := newStatusPool()
	pool3.setQueryErr(errors.New("boom"))
	if _, err := FetchIngestReports(context.Background(), pool3); err == nil || !strings.Contains(err.Error(), "status: ingest:") {
		t.Errorf("ошибка пула = %v, хочу контекст «status: ingest:»", err)
	}
	if _, err := FetchIngestReports(context.Background(), nil); err == nil {
		t.Error("FetchIngestReports(nil) = nil, хочу ошибку")
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

	// Пустой цикл при известной ненулевой скорости: ETA остаётся 0 —
	// ветка расчёта требует положительный остаток, кламп «минимум 1с»
	// на нулевом остатке не действует (мутант «> 0 → >= 0» поднял бы 1).
	r = ComputeRates(Throughput{DoneToday: 600, ElapsedSec: 600}, 0, 0)
	if !r.DomainsKnown || r.DomainsPerSec != 1 {
		t.Errorf("пустой цикл при скорости: Known=%v speed=%v, хочу известную 1", r.DomainsKnown, r.DomainsPerSec)
	}
	if !r.ETAKnown || r.ETASec != 0 {
		t.Errorf("пустой цикл при скорости: ETA = %d (%v), хочу 0 (известна)", r.ETASec, r.ETAKnown)
	}

	// Округление вверх/вниз и минимум: 100/3 = 33.3 → 33; 1/3 = 0.3 → 1.
	if r := ComputeRates(Throughput{DoneToday: 3000, ElapsedSec: 1000}, 100, 0); r.ETASec != 33 {
		t.Errorf("ETA 100/3 = %d, хочу 33", r.ETASec)
	}
	if r := ComputeRates(Throughput{DoneToday: 3000, ElapsedSec: 1000}, 1, 0); r.ETASec != 1 {
		t.Errorf("ETA 1/3 = %d, хочу минимум 1", r.ETASec)
	}
}

// TestMinSpeedWindowSec — лимит окна пин литералом: адаптивные проверки
// TestComputeRates используют константу и мутанта ±1 не замечают.
func TestMinSpeedWindowSec(t *testing.T) {
	if MinSpeedWindowSec != 600 {
		t.Errorf("MinSpeedWindowSec = %d, want 600 (10 минут, ТЗ 3.3)", MinSpeedWindowSec)
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
		{89, "1м"},    // (89+30)/60 = 1: граница округления к минуте
		{90, "2м"},    // округление к минуте
		{3599, "60м"}, // 59м59с → 60м
		{3600, "1ч 00м"},
		{5370, "1ч 29м"}, // 1ч29м30с: минуты от остатка, не от полного числа
		{7199, "1ч 59м"}, // 1ч59м59с: час — полные 3600-секундные блоки
		{19200, "5ч 20м"},
		{86399, "23ч 59м"}, // край ветки часов: сутки ещё не начались
		{86400, "1д 0ч"},
		{89999, "1д 0ч"},   // 1д 0ч59м59с: час от остатка суток
		{93599, "1д 1ч"},   // 1д 1ч59м59с
		{93600, "1д 2ч"},   // ровно 1д 2ч
		{172799, "1д 23ч"}, // 1д 23ч59м59с: край перед «2д»
		{275400, "3д 4ч"},
	}
	for _, tc := range cases {
		if got := FormatDuration(tc.sec); got != tc.want {
			t.Errorf("FormatDuration(%d) = %q, want %q", tc.sec, got, tc.want)
		}
	}
}

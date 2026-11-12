// Unit tests of the status package (T-079 → T-126, spec 5.9 / dashboard
// spec §4.5) on a fake pool (db.DB) and an XLEN client — without the DB
// and Redis. Both the exact SQL text of the selections (the contract, the
// reanalyze_test.go style) and the live semantics of the fake are
// checked: the domains aggregate by check_state with deleted excluded
// from pending, the LEFT JOIN of zones without domains, the sort by name;
// the throughput window and the division-by-zero protection in
// ComputeRates (a short window, a zero rate, an empty circle, the minimum
// ETA); the interval format. The output (table/JSON) and the routing are
// the CLI tests (cmd/crawlerctl/status_test.go); the behavior against
// live PostgreSQL/Redis is the dev environment (make status).
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
	"crawler/internal/queue"
)

// --- SQL contracts ----------------------------------------------------------

// wantZoneStatsSQL / wantThroughputSQL — the expected query texts; the
// fake matches the SQL exactly, an unexpected query fails the test
// loudly.
const wantZoneStatsSQL = "SELECT z.id, z.name, coalesce(z.name_unicode, '') AS name_unicode, " +
	"z.is_active, z.domains_total, z.domains_pending, z.domains_queued, z.domains_checked " +
	"FROM zones AS z ORDER BY z.name"

// wantThroughputSQL — every number is a point read / a sum of
// crawler_counters rows, there are no passes over large tables in the
// query: done_today is the daily check counter
// crawler_counters['checks:YYYY-MM-DD'] by a point lookup over the PK
// (T-179); the coalesce is OUTSIDE the subquery — before the first record
// of the day the subquery is empty (NULL), the idle hours show an honest
// zero; the day name is computed by the DB (current_date) — consistent
// with the now() of the last_checked_at record (T-058) and the
// reconciliation window.
// results_hour/rendered_hour — the sums of the HOURLY throughput buckets
// (T-203): the former live subqueries over subdomain_results (the "last
// hour" window over checked_at) degraded with heap fetches of fresh pages
// (the stage 27 checkpoint); a bucket is a calendar hour, the reader sums
// two live buckets (the previous and the current hour, an IN list);
// hour_window_sec is the actual length of the covered window for the
// honest rate divisor; the bucket names are computed by the DB
// (date_trunc from now()) — consistent with the record/render increments
// and the reconciliation.
const wantThroughputSQL = "SELECT " +
	"coalesce((SELECT value FROM crawler_counters " +
	"WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')), 0) AS done_today, " +
	"extract(epoch FROM (now() - date_trunc('day', now())))::float8 AS elapsed_sec, " +
	"(SELECT coalesce(sum(value), 0) FROM crawler_counters " +
	"WHERE name IN ('results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), " +
	"'results:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'))) AS results_hour, " +
	"(SELECT coalesce(sum(value), 0) FROM crawler_counters " +
	"WHERE name IN ('rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), " +
	"'rendered:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'))) AS rendered_hour, " +
	"extract(epoch FROM (now() - date_trunc('hour', now() - interval '1 hour')))::float8 AS hour_window_sec"

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

// --- fake pool (db.DB) ----------------------------------------------------

// statusZoneSeed — a zone in the fake: the counters and the display
// attributes.
type statusZoneSeed struct {
	id      int16
	name    string
	unicode string
	active  bool
	total   int64
	pending int64
	queued  int64
	checked int64
	hasUni  bool // name_unicode not NULL (in SQL — coalesce into '')
}

// statusStatusSeed — a row of the status distribution counter (§5.5,
// T-178): the legend status name and the crawler_counters row value.
type statusStatusSeed struct {
	name  string
	count int64
}

// statusLaravelSeed — a laravel_domains row for the Laravel section
// (§5.6): the domain label and its zone — the JOIN of the recent finds
// selection.
type statusLaravelSeed struct {
	zone      int16
	domain    int64
	label     string
	score     float32
	verMin    *string
	verMax    *string
	updatedAt time.Time
}

// statusIngestSeed — an ingest_files row (§5.7); the pointers are the
// NULL schema columns (the log is written with full rows, NULL is legal
// for manual inserts).
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

// statusGlitch — a degradation of the response shape of one query (the
// monitor fake style, T-M01/T-M02): an error of Query itself, an empty
// row set (Next is false immediately), an unreadable column of the first
// row (a Scan error), an iteration error (rows.Err).
type statusGlitch struct {
	queryErr error
	empty    bool
	badRow   bool
	rowsErr  error
}

// statusPool — a db.DB without a DB: the live semantics of zoneStatsSQL
// (a GROUP BY of zones, pending without deleted, queued/done without a
// status filter, LEFT JOIN — a zone without domains stays, ORDER BY
// name), throughputSQL (ready values) and the dashboard sections
// (§5.5–5.7): the status distribution, the Laravel counters/window by
// the fake clock, the sorting of the recent finds and ingest files.
type statusPool struct {
	zones       []statusZoneSeed
	thr         Throughput
	domStatuses []statusStatusSeed
	laravel     []statusLaravelSeed
	zoneLaravel int64 // the sum of zones.domains_laravel (laravelTotalsSQL, T-178)
	ingest      []statusIngestSeed
	now         time.Time // the DB server clock for the laravelTotalsSQL windows

	// glitches — the response degradations by the query text.
	glitches map[string]statusGlitch

	mu        sync.Mutex
	queryErr  error
	queries   []string
	execCalls int
	closes    int // how many times the row sets were closed (rows.Close)
}

var _ db.DB = (*statusPool)(nil)

func newStatusPool() *statusPool {
	return &statusPool{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
}

func (p *statusPool) seedZone(id int16, name string, active bool, total, pending, queued, checked int64) {
	p.zones = append(p.zones, statusZoneSeed{id: id, name: name, active: active,
		total: total, pending: pending, queued: queued, checked: checked})
}

// seedZoneUnicode — a zone with a non-empty name_unicode (NULL vs a
// value).
func (p *statusPool) seedZoneUnicode(id int16, name, unicode string, active bool, total, pending, queued, checked int64) {
	p.zones = append(p.zones, statusZoneSeed{id: id, name: name, unicode: unicode, active: active,
		total: total, pending: pending, queued: queued, checked: checked, hasUni: true})
}

func (p *statusPool) setQueryErr(err error) {
	p.mu.Lock()
	p.queryErr = err
	p.mu.Unlock()
}

// setGlitch — enable a degradation of the response of a specific query.
func (p *statusPool) setGlitch(sql string, g statusGlitch) {
	p.mu.Lock()
	if p.glitches == nil {
		p.glitches = make(map[string]statusGlitch)
	}
	p.glitches[sql] = g
	p.mu.Unlock()
}

// closeCount — how many times the row sets were closed: FetchLaravelStats
// closes the first set BEFORE the second query (without defer) — the Close
// of every path is observable through the counter.
func (p *statusPool) closeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closes
}

// bumpClose — the Close counter from statusRows (called outside the
// Query lock: the caller closes the set after Query returns).
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
		return p.decorate(sql, [][]any{{p.thr.DoneToday, p.thr.ElapsedSec, p.thr.ResultsHour, p.thr.RenderedHour, p.thr.HourWindowSec}}), nil
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
		// The pool_size=1 model: a second query with the first set open
		// would take the only connection (a deadlock, the
		// FetchLaravelStats comment) — the fake requires the first set to
		// be closed BEFORE the second query: the order, not just the sum
		// of the Closes.
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

// statusBadColumn — the marker of an unreadable column: the fake's Scan
// answers with an error (like pgx on an incompatible destination type).
type statusBadColumn struct{}

// decorate assembles the statusRows of the query with its degradation:
// empty — a set without rows (Next is false immediately), badRow — the
// first row with an unreadable column (a Scan error), rowsErr — an
// iteration error; Close is counted into the pool (the observability of
// the set closing).
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

// zoneRowsData applies the zoneStatsSQL semantics to the seeds: the zone
// reference table with incremental counters (T-178) — without a pass over
// domains.
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

// laravelRowsData applies the laravelRecentSQL semantics: a JOIN of the
// domain and the zone by the row key, the updated_at DESC sort,
// LIMIT 10.
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

// ingestRowsData applies the ingestRecentSQL semantics: processed_at DESC
// (NULL last), id DESC within one second, LIMIT 10.
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

// statusRows — pgx.Rows over prepared rows (a positional Scan, the types
// int16/string/bool/int64/float32/float64/[]byte and pointers for the
// NULL columns — single values and double ones for unfolding NULL).
// The Close counter counts Close calls (not the connection state): the
// explicit set closures are observed one per exit path.
// A copy of the CLI tests' shared fake (cmd/crawlerctl): the test files
// of different packages do not import each other.
type statusRows struct {
	rows    [][]any
	i       int
	err     error  // the Err answer — the iteration degradation
	onClose func() // the pool Close counter
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

// assignPtr — the unfolding of NULL columns: a pointer value from a fake
// row (nil — NULL) into a pointer-to-pointer pgx destination.
func assignPtr[T any](dst **T, v any) {
	if p, ok := v.(*T); ok {
		*dst = p
		return
	}
	*dst = nil
}

// --- fake XLEN -----------------------------------------------------------

// statusLenner — StreamLener: the lengths per stream (or a failure of any
// length), records the polling order.
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

// --- SQL and selection semantics -------------------------------------------------

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

// TestFetchZoneStats — the zone summary from the reference table counters
// (T-178): without a JOIN over domains; pending/queued/checked are the
// zone columns (the base set status <> deleted is fed by the deltas and
// the reconciliation); Done is the same domains_checked column; the sort
// by name, the unicode and the active flag are read through.
func TestFetchZoneStats(t *testing.T) {
	pool := newStatusPool()
	pool.seedZoneUnicode(2, "com", "com", true, 1000, 3, 1, 400)
	pool.seedZone(1, "crawl.test", true, 100, 2, 2, 40)
	pool.seedZone(3, "empty.test", false, 0, 0, 0, 0) // a zone without domains

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

// TestFetchZoneStatsEmpty — no zones: an empty result without an error.
func TestFetchZoneStatsEmpty(t *testing.T) {
	zones, err := FetchZoneStats(context.Background(), newStatusPool())
	if err != nil {
		t.Fatalf("FetchZoneStats: %v", err)
	}
	if len(zones) != 0 {
		t.Errorf("zones = %+v, хочу пусто", zones)
	}
}

// TestFetchThroughput — one row of the measurements; the column types
// (int64/float64), including the bucket window length (T-203).
func TestFetchThroughput(t *testing.T) {
	pool := newStatusPool()
	pool.thr = Throughput{DoneToday: 2279, ElapsedSec: 3720.5, ResultsHour: 6132, RenderedHour: 1025, HourWindowSec: 5400}

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

// TestStatusFetchErrors — the pool errors are returned with the package
// context; a nil pool is an error, not a panic.
func TestStatusFetchErrors(t *testing.T) {
	boom := errors.New("boom")
	pool := newStatusPool()
	pool.setQueryErr(boom)
	if _, err := FetchZoneStats(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: zones:") {
		t.Errorf("FetchZoneStats err = %v, хочу контекст «status: зоны:»", err)
	}
	if _, err := FetchThroughput(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: throughput:") {
		t.Errorf("FetchThroughput err = %v, хочу контекст «status: темп:»", err)
	}
	if _, err := FetchZoneStats(context.Background(), nil); err == nil {
		t.Error("FetchZoneStats(nil) = nil, хочу ошибку")
	}
	if _, err := FetchThroughput(context.Background(), nil); err == nil {
		t.Error("FetchThroughput(nil) = nil, хочу ошибку")
	}
}

// TestFetchRowDegradations — the row-level degradations of the response
// shape (the monitor fake style): an unreadable column is a Scan error
// with the "reading" context, an iteration error (rows.Err) is the error
// of the whole selection with the original text; an empty scalar
// throughput aggregate is "empty answer", not "reading" (the return-cut
// mutants would swallow the error or substitute its text).
func TestFetchRowDegradations(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		glitch  statusGlitch
		wantErr string
	}{
		{"зоны: нечитаемая строка", zoneStatsSQL, statusGlitch{badRow: true}, "status: zones: reading:"},
		{"зоны: ошибка итерации", zoneStatsSQL, statusGlitch{rowsErr: errors.New("соединение сброшено")}, "status: zones: соединение сброшено"},
		{"темп: пустой ответ", throughputSQL, statusGlitch{empty: true}, "status: throughput: empty response"},
		{"темп: нечитаемая строка", throughputSQL, statusGlitch{badRow: true}, "status: throughput: reading:"},
		{"темп: ошибка итерации", throughputSQL, statusGlitch{rowsErr: errors.New("соединение сброшено")}, "status: throughput: соединение сброшено"},
		{"статусы: нечитаемая строка", domainStatusSQL, statusGlitch{badRow: true}, "status: statuses: reading:"},
		{"статусы: ошибка итерации", domainStatusSQL, statusGlitch{rowsErr: errors.New("соединение сброшено")}, "status: statuses: соединение сброшено"},
		{"ingest: нечитаемая строка", ingestRecentSQL, statusGlitch{badRow: true}, "status: ingest: reading:"},
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

// --- queues -------------------------------------------------------------------

// TestFetchQueueLens — all the working streams in the spec 4.2 order; an
// error stops the collection (a single Redis); a nil client is an error.
func TestFetchQueueLens(t *testing.T) {
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:render-fallback": 2, "q:analyze": 3, "q:dead": 1}
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
	// S-P3-1: the list is derived from the single source of the queue
	// (queue.allStreams), the pipeline order; q:render-fallback (T-237)
	// enters as a working stream. A copy, not a slice of the source: a
	// mutation of the result does not affect the package lists.
	if want := []string{"q:dns", "q:http", "q:render", "q:render-fallback", "q:analyze", "q:dead"}; !reflect.DeepEqual(Streams, want) {
		t.Errorf("Streams = %v, want %v (все стримы ТЗ 4.2 + q:render-fallback)", Streams, want)
	}
	if !reflect.DeepEqual(Streams, queue.AllStreams()) {
		t.Errorf("Streams = %v, want queue.AllStreams() = %v (единый источник)", Streams, queue.AllStreams())
	}
	mutated := append(Streams, "q:extra")
	if len(queue.AllStreams()) == len(mutated) {
		t.Error("AllStreams возвращает срез источника: мутация копии изменила источник")
	}

	_, err = FetchQueueLens(context.Background(), &statusLenner{err: errors.New("connection refused")})
	if err == nil {
		t.Error("ошибка XLEN потеряна")
	}
	if _, err := FetchQueueLens(context.Background(), nil); err == nil {
		t.Error("FetchQueueLens(nil) = nil, хочу ошибку")
	}
}

// --- dashboard sections (dashboard spec §5.5–5.7) ----------------------------------------------

// strPtr — a string pointer (the NULL columns of the seeds).
func strPtr(s string) *string { return &s }

// int16Ptr/int64Ptr — number pointers (the NULL columns of the ingest
// seeds).
func int16Ptr(v int16) *int16 { return &v }

func int64Ptr(v int64) *int64 { return &v }

// TestFetchDomainStatuses — the distribution over the spec 6.2 names from
// the status_* rows of crawler_counters (T-178): all the model.Statuses
// statuses with zeros (the full legend), the missing rows are 0; a row
// with a name outside the legend is not displayed (the reconciliation
// writes only model.Statuses — a foreign row means a manual fix of the
// table); the errors and a nil pool.
func TestFetchDomainStatuses(t *testing.T) {
	pool := newStatusPool()
	pool.domStatuses = []statusStatusSeed{
		{model.StatusNew.String(), 1000},
		{model.StatusAlive.String(), 40},
		{model.StatusDeleted.String(), 3},
		{"unknown", 1}, // a name outside the model.Statuses legend — not displayed
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

	// An empty table — a legend of zeros, not an empty map.
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

	// The pool errors and a nil pool.
	pool = newStatusPool()
	pool.setQueryErr(errors.New("boom"))
	if _, err := FetchDomainStatuses(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "status: statuses:") {
		t.Errorf("ошибка пула = %v, хочу контекст «status: статусы:»", err)
	}
	if _, err := FetchDomainStatuses(context.Background(), nil); err == nil {
		t.Error("FetchDomainStatuses(nil) = nil, хочу ошибку")
	}
}

// TestFetchLaravelStats — the counters (the total and the 24 h window by
// updated_at), the recent finds: the domain label.zone via a JOIN, the
// updated_at DESC sort, LIMIT 10, ver_min/ver_max NULL → empty strings.
func TestFetchLaravelStats(t *testing.T) {
	pool := newStatusPool()
	pool.seedZone(1, "crawl.test", true, 0, 0, 0, 0)
	pool.zoneLaravel = 3 // the sum of zones.domains_laravel (T-178): the section Total
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
	// The first set is closed BEFORE the second query, the second one —
	// with defer: exactly two Closes on a successful pass (holding the
	// set would occupy a pool connection — the FetchLaravelStats
	// comment).
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

	// An empty table: zeros and an empty recent slice (not nil — the
	// section is alive).
	st, err = FetchLaravelStats(context.Background(), newStatusPool())
	if err != nil {
		t.Fatalf("FetchLaravelStats (пусто): %v", err)
	}
	if st.Total != 0 || st.Last24h != 0 || len(st.Recent) != 0 {
		t.Errorf("пустая таблица: %+v, хочу нули и пустой recent", st)
	}

	// An error of either of the two queries is the error of the whole
	// section.
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

// TestFetchLaravelStatsDegradations — the degradations of both queries of
// the section: an empty aggregate distinguishes a rows error (the real
// cause is more valuable than a synthetic one) from an honest "empty";
// the first set is closed BEFORE the second query (without defer) — the
// Close of every exit path is visible to the fake's counter (the
// rows.Close-cut mutants would swallow the set closing, the return-cut —
// the whole error).
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

	// An empty aggregate without a rows error — an honest "empty".
	pool := seeded(map[string]statusGlitch{laravelTotalsSQL: {empty: true}})
	_, err := FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: empty aggregate response") {
		t.Fatalf("пустой агрегат: ошибка = %v, want «пустой ответ агрегата»", err)
	}
	if n := pool.closeCount(); n != 1 {
		t.Errorf("пустой агрегат: Close = %d, want 1 (set закрыт до синтетической ошибки)", n)
	}

	// An empty aggregate with a rows error — a row-level error is more
	// valuable.
	pool = seeded(map[string]statusGlitch{laravelTotalsSQL: {empty: true, rowsErr: errors.New("соединение сброшено")}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: соединение сброшено") {
		t.Fatalf("пустой агрегат с rows-ошибкой: ошибка = %v, want причину уровня строк", err)
	}

	// An unreadable aggregate row: the set is closed before the error is
	// returned.
	pool = seeded(map[string]statusGlitch{laravelTotalsSQL: {badRow: true}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: reading:") {
		t.Fatalf("нечитаемая строка агрегата: ошибка = %v, want «чтение»", err)
	}
	if n := pool.closeCount(); n != 1 {
		t.Errorf("нечитаемая строка агрегата: Close = %d, want 1", n)
	}

	// An aggregate iteration error — after a successful Scan, the set is
	// closed.
	pool = seeded(map[string]statusGlitch{laravelTotalsSQL: {rowsErr: errors.New("соединение сброшено")}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: соединение сброшено") {
		t.Fatalf("ошибка итерации агрегата: ошибка = %v, want причину уровня строк", err)
	}
	if n := pool.closeCount(); n != 1 {
		t.Errorf("ошибка итерации агрегата: Close = %d, want 1", n)
	}

	// An error of the second query — the "recent" context.
	pool = seeded(map[string]statusGlitch{laravelRecentSQL: {queryErr: errors.New("бд недоступна")}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: последние: бд недоступна") {
		t.Fatalf("ошибка второго запроса: ошибка = %v, want «последние»", err)
	}

	// An unreadable row of the recent finds.
	pool = seeded(map[string]statusGlitch{laravelRecentSQL: {badRow: true}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: последние: чтение:") {
		t.Fatalf("нечитаемая строка последних: ошибка = %v, want «чтение»", err)
	}

	// An iteration error of the recent finds.
	pool = seeded(map[string]statusGlitch{laravelRecentSQL: {rowsErr: errors.New("соединение сброшено")}})
	_, err = FetchLaravelStats(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "status: laravel: последние: соединение сброшено") {
		t.Fatalf("ошибка итерации последних: ошибка = %v, want причину уровня строк", err)
	}
}

// TestFetchIngestReports — the recent files: processed_at DESC (NULL
// last), id DESC on ties, LIMIT 10; the add/delete actions by the model
// names, the NULL columns — zero values.
func TestFetchIngestReports(t *testing.T) {
	pool := newStatusPool()
	at := func(h time.Duration) *time.Time {
		t := pool.now.Add(-h * time.Hour)
		return &t
	}
	pool.ingest = []statusIngestSeed{
		// NULL processed_at — "last", despite the large id.
		{id: 77},
		{id: 3, path: strPtr("crawl.test/add/three.txt"), action: int16Ptr(int16(model.ActionAdd)),
			lines: int64Ptr(30), added: int64Ptr(28), existed: int64Ptr(2), rejected: int64Ptr(0), processedAt: at(1)},
		// The same second as id 3 — the id DESC order.
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
		{}, // NULL processed_at — the last row, the columns unfolded into zeros
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("reports =\n%+v\nwant\n%+v", got, want)
	}

	// An action code outside the spec 6.1 table — the Action(N) name, not
	// a panic.
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

// --- calculations ---------------------------------------------------------------

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

// TestComputeRates — the division-by-zero and short-window protection:
// the domain rate only with a window ≥ MinSpeedWindowSec; the ETA — with
// a known non-zero rate (or an empty circle), the rounding and the
// minimum of 1s.
func TestComputeRates(t *testing.T) {
	// A working case: 3600 checks per hour → 1/s; the remainder 90 →
	// the ETA 90s.
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

	// A window shorter than the minimum: no domain rate and no ETA; the
	// hourly ones are computed (a zero bucket window — the fixed hour
	// divisor, T-203).
	r = ComputeRates(Throughput{ElapsedSec: MinSpeedWindowSec - 1, ResultsHour: 3600}, 10, 0)
	if r.DomainsKnown || r.DomainsPerSec != 0 {
		t.Errorf("короткое окно: Known=%v speed=%v, хочу неизвестную", r.DomainsKnown, r.DomainsPerSec)
	}
	if r.ETAKnown {
		t.Error("короткое окно: ETA известна, хочу н/д")
	}
	if r.ResultsPerSec != 1 {
		t.Errorf("результаты/с = %v, хочу 1 (фиксированный делитель при нулевом окне)", r.ResultsPerSec)
	}

	// The throughput bucket window (T-203): the divisor is the actual
	// length of the covered window (the prev+cur buckets, 1–2 h); the
	// rate is unbiased: 7200 rows over a 5400 s window → 4/3 per second;
	// a zero window of a manual assembly — the former fixed hour.
	r = ComputeRates(Throughput{ResultsHour: 7200, RenderedHour: 3600, HourWindowSec: 5400}, 0, 0)
	if r.ResultsPerSec != 7200.0/5400.0 || r.RenderPerSec != 3600.0/5400.0 {
		t.Errorf("скорости окна корзин = %v/%v, хочу %v/%v (деление на длину окна)",
			r.ResultsPerSec, r.RenderPerSec, 7200.0/5400.0, 3600.0/5400.0)
	}

	// The window boundary exactly at the minimum — the rate is known.
	if r := ComputeRates(Throughput{DoneToday: MinSpeedWindowSec, ElapsedSec: MinSpeedWindowSec}, 1, 0); !r.DomainsKnown {
		t.Error("окно = MinSpeedWindowSec: скорость должна быть известна")
	}

	// A rate of 0 (no checks in the window): the ETA is n/a, not a panic.
	r = ComputeRates(Throughput{DoneToday: 0, ElapsedSec: 7200}, 5, 0)
	if !r.DomainsKnown || r.DomainsPerSec != 0 {
		t.Errorf("нулевые проверки: Known=%v speed=%v, хочу известную 0", r.DomainsKnown, r.DomainsPerSec)
	}
	if r.ETAKnown {
		t.Error("скорость 0: ETA не должна быть известна (деление на ноль)")
	}

	// An empty circle: the ETA is 0 even without a rate.
	r = ComputeRates(Throughput{ElapsedSec: 1}, 0, 0)
	if !r.ETAKnown || r.ETASec != 0 {
		t.Errorf("пустой цикл: ETA = %d (%v), хочу 0 (известна)", r.ETASec, r.ETAKnown)
	}

	// An empty circle with a known non-zero rate: the ETA stays 0 — the
	// calculation branch requires a positive remainder; the "minimum 1s"
	// clamp does not act on a zero remainder (the "> 0 → >= 0" mutant
	// would have lifted a 1).
	r = ComputeRates(Throughput{DoneToday: 600, ElapsedSec: 600}, 0, 0)
	if !r.DomainsKnown || r.DomainsPerSec != 1 {
		t.Errorf("пустой цикл при скорости: Known=%v speed=%v, хочу известную 1", r.DomainsKnown, r.DomainsPerSec)
	}
	if !r.ETAKnown || r.ETASec != 0 {
		t.Errorf("пустой цикл при скорости: ETA = %d (%v), хочу 0 (известна)", r.ETASec, r.ETAKnown)
	}

	// Rounding up/down and the minimum: 100/3 = 33.3 → 33; 1/3 = 0.3 → 1.
	if r := ComputeRates(Throughput{DoneToday: 3000, ElapsedSec: 1000}, 100, 0); r.ETASec != 33 {
		t.Errorf("ETA 100/3 = %d, хочу 33", r.ETASec)
	}
	if r := ComputeRates(Throughput{DoneToday: 3000, ElapsedSec: 1000}, 1, 0); r.ETASec != 1 {
		t.Errorf("ETA 1/3 = %d, хочу минимум 1", r.ETASec)
	}
}

// TestMinSpeedWindowSec — the window limit pinned by a literal: the
// adaptive checks of TestComputeRates use the constant and would not
// notice the ±1 mutant.
func TestMinSpeedWindowSec(t *testing.T) {
	if MinSpeedWindowSec != 600 {
		t.Errorf("MinSpeedWindowSec = %d, want 600 (10 минут, ТЗ 3.3)", MinSpeedWindowSec)
	}
}

// TestFormatDuration — rounded intervals: seconds/minutes/hours/days.
func TestFormatDuration(t *testing.T) {
	cases := []struct {
		sec  int64
		want string
	}{
		{0, "0s"},
		{45, "45s"},
		{59, "59s"},
		{60, "1m"},
		{89, "1m"},    // (89+30)/60 = 1: the rounding boundary to a minute
		{90, "2m"},    // rounding to a minute
		{3599, "60m"}, // 59m59s → 60m
		{3600, "1h 00m"},
		{5370, "1h 29m"}, // 1h29m30s: the minutes from the remainder, not from the whole number
		{7199, "1h 59m"}, // 1h59m59s: an hour is full 3600-second blocks
		{19200, "5h 20m"},
		{86399, "23h 59m"}, // the edge of the hours branch: the day has not started yet
		{86400, "1d 0h"},
		{89999, "1d 0h"},   // 1d 0h59m59s: the hour from the day remainder
		{93599, "1d 1h"},   // 1d 1h59m59s
		{93600, "1d 2h"},   // exactly 1d 2h
		{172799, "1d 23h"}, // 1d 23h59m59s: the edge before "2d"
		{275400, "3d 4h"},
	}
	for _, tc := range cases {
		if got := FormatDuration(tc.sec); got != tc.want {
			t.Errorf("FormatDuration(%d) = %q, want %q", tc.sec, got, tc.want)
		}
	}
}

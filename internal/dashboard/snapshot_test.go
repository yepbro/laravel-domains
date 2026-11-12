// Unit tests of the snapshot collector and /api/status (T-129,
// dashboard spec §4.3–4.4, §5, §6.2, §5.9): section assembly on a fake
// pool (the SQL contracts of selections — internal/status tests),
// section degradation (null + the errors map), parallel /livez probes
// (up/refusal/timeout/non-200, the path and trailing slash of the
// base), the snapshot JSON shape (the CLI fields + new sections +
// null), readiness 503→200, the collection loop (the period, atomic
// replacement, the readiness latch). Behavior against live
// PostgreSQL/Redis — test/integration/dashboard_test.go.
package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/status"
)

// --- fake pool (db.DB) ------------------------------------------------------------

// snapZone — a fake zone: the zoneStatsSQL columns.
type snapZone struct {
	id      int16
	name    string
	active  bool
	total   int64
	checked int64
	pending int64
	queued  int64
	done    int64
}

// snapPool — a db.DB answering the internal/status selections in the
// SQL shape (the query texts are the status package contract, here —
// recognition): zones, throughput, statuses, Laravel, ingest. failSQL —
// the selection(s) answering with an error (section degradation).
type snapPool struct {
	mu      sync.Mutex
	zones   []snapZone
	thr     status.Throughput
	stats   map[string]int64
	laravel status.LaravelStats
	ingest  []status.IngestReport

	failSQL func(sql string) error // nil — all queries healthy
	queries []string
}

var _ db.DB = (*snapPool)(nil)

// Ping — the /healthz dependency; the pool is alive while it is.
func (p *snapPool) Ping(context.Context) error { return nil }

func (p *snapPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("snapPool: CopyFrom must not be called")
}

func (p *snapPool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("snapPool: an unexpected Exec: %s", sql)
}

func (p *snapPool) Query(ctx context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, sql)
	if p.failSQL != nil {
		if err := p.failSQL(sql); err != nil {
			return nil, err
		}
	}
	switch {
	case strings.Contains(sql, "FROM zones AS z"):
		// Zone summary — the reference table counters (T-178); Done —
		// the same domains_checked column (a unification of the base
		// set).
		rows := make([][]any, len(p.zones))
		for i, z := range p.zones {
			rows[i] = []any{z.id, z.name, "", z.active, z.total, z.pending, z.queued, z.checked}
		}
		return snapRowsOf(rows...), nil
	case strings.Contains(sql, "AS done_today"):
		// Throughput: done_today — the daily counter of checks (T-179),
		// the rest — live windows; values from the seed.
		return snapRowsOf([]any{p.thr.DoneToday, p.thr.ElapsedSec, p.thr.ResultsHour, p.thr.RenderedHour, p.thr.HourWindowSec}), nil
	case strings.Contains(sql, "LIKE 'status"):
		// Status distribution — the status_* rows of the counters
		// (T-178).
		rows := make([][]any, 0, len(p.stats))
		for _, st := range model.Statuses {
			if n, ok := p.stats[st.String()]; ok {
				rows = append(rows, []any{"status_" + st.String(), n})
			}
		}
		return snapRowsOf(rows...), nil
	case strings.Contains(sql, "FROM laravel_domains") && strings.Contains(sql, "FILTER"):
		return snapRowsOf([]any{p.laravel.Total, p.laravel.Last24h}), nil
	case strings.Contains(sql, "ORDER BY ld.updated_at"):
		rows := make([][]any, len(p.laravel.Recent))
		for i, it := range p.laravel.Recent {
			verMin, verMax := (*string)(nil), (*string)(nil)
			if it.VerMin != "" {
				verMin = &it.VerMin
			}
			if it.VerMax != "" {
				verMax = &it.VerMax
			}
			rows[i] = []any{it.Domain, it.Zone, it.Score, verMin, verMax, it.UpdatedAt}
		}
		return snapRowsOf(rows...), nil
	case strings.Contains(sql, "FROM ingest_files"):
		rows := make([][]any, len(p.ingest))
		for i, r := range p.ingest {
			path, action := (*string)(nil), (*int16)(nil)
			if r.Path != "" {
				path = &r.Path
			}
			switch r.Action {
			case "add":
				a := int16(model.ActionAdd)
				action = &a
			case "delete":
				d := int16(model.ActionDelete)
				action = &d
			}
			var processed *time.Time
			if !r.ProcessedAt.IsZero() {
				at := r.ProcessedAt
				processed = &at
			}
			lines, added, existed := &r.LinesTotal, &r.Added, &r.Existed
			deleted, rejected := &r.Deleted, &r.Rejected
			rows[i] = []any{path, action, lines, added, existed, deleted, rejected, processed}
		}
		return snapRowsOf(rows...), nil
	default:
		return nil, fmt.Errorf("snapPool: an unexpected SQL Query: %s", sql)
	}
}

// calls — the accumulated SQL (the pass order).
func (p *snapPool) calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.queries...)
}

// snapRows — pgx.Rows with pre-cooked rows; the receivers are the types
// scanning the internal/status selections (including the double
// pointers of NULL columns). A local copy of the fake (test packages do
// not import each other).
type snapRows struct {
	rows [][]any
	i    int
}

func snapRowsOf(rows ...[]any) *snapRows { return &snapRows{rows: rows} }

func (r *snapRows) Close()                                       {}
func (r *snapRows) Err() error                                   { return nil }
func (r *snapRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *snapRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *snapRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *snapRows) RawValues() [][]byte                          { return nil }
func (r *snapRows) Conn() *pgx.Conn                              { return nil }
func (r *snapRows) TypeMap() *pgtype.Map                         { return nil }

func (r *snapRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *snapRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("snapRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("snapRows: dest %d, columns %d", len(dest), len(row))
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
		case *time.Time:
			*dst = v.(time.Time)
		case **string:
			dst2, ok := v.(*string)
			if !ok && v != nil {
				return fmt.Errorf("snapRows: column %d: %T — a *string expected", i, v)
			}
			*dst = dst2
		case **int16:
			dst2, ok := v.(*int16)
			if !ok && v != nil {
				return fmt.Errorf("snapRows: column %d: %T — an *int16 expected", i, v)
			}
			*dst = dst2
		case **int64:
			dst2, ok := v.(*int64)
			if !ok && v != nil {
				return fmt.Errorf("snapRows: column %d: %T — an *int64 expected", i, v)
			}
			*dst = dst2
		case **time.Time:
			dst2, ok := v.(*time.Time)
			if !ok && v != nil {
				return fmt.Errorf("snapRows: column %d: %T — a *time.Time expected", i, v)
			}
			*dst = dst2
		default:
			return fmt.Errorf("snapRows: the dest %T is not supported", d)
		}
	}
	return nil
}

// --- helper fakes -------------------------------------------------------------------

// fakeLenner — a StreamLener with a switchable refusal (err ≠ nil —
// Redis "down": the queue section degradation scenario, including from
// the very start); healthy calls return increasing depths.
type fakeLenner struct {
	err   atomic.Value // error or nil
	calls atomic.Int64
	seq   atomic.Int64
}

func (l *fakeLenner) Len(_ context.Context, _ string) (int64, error) {
	l.calls.Add(1)
	if e := l.err.Load(); e != nil {
		return 0, e.(error)
	}
	return l.seq.Add(1), nil
}

// staticSource — a snapshot source for the handler (a ready snapshot).
type staticSource struct{ snap *Snapshot }

func (s staticSource) Current() *Snapshot { return s.snap }

// newSnapCollector — a collector on fakes: pool, lenner, probes;
// mutators configure the options (the probe client for the timeout
// test, the clock).
func newSnapCollector(t *testing.T, pool *snapPool, lens status.StreamLener, mutate func(*CollectorOptions)) *Collector {
	t.Helper()
	cfg := config.Default()
	// Empty (not nil) probes/panel addresses — "sections not
	// configured": nil would pull the cfg defaults (since S-P2-5 — the
	// main build exposure 127.0.0.1:9105) and the tests would hit the
	// network.
	opts := CollectorOptions{Probes: map[string]string{}, RenderMetrics: []string{}}
	if mutate != nil {
		mutate(&opts)
	}
	c, err := NewCollector(pool, lens, cfg.Dashboard, opts)
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	return c
}

// healthyPool — consistent data of all sections.
func healthyPool() *snapPool {
	return &snapPool{
		zones: []snapZone{{id: 1, name: "crawl.test", active: true, total: 100, checked: 40, pending: 58, queued: 2, done: 40}},
		thr:   status.Throughput{DoneToday: 3600, ElapsedSec: 3600, ResultsHour: 7200, RenderedHour: 3600, HourWindowSec: 3600},
		stats: map[string]int64{model.StatusNew.String(): 58, model.StatusAlive.String(): 40, model.StatusDeleted.String(): 2},
		laravel: status.LaravelStats{
			Total: 2, Last24h: 1,
			Recent: []status.LaravelRecent{{
				Domain: "shop.crawl.test", Zone: "crawl.test", Score: 0.9,
				VerMin: "9.19", VerMax: "11", UpdatedAt: time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC),
			}},
		},
		ingest: []status.IngestReport{{
			Path: "crawl.test/add/one.txt", Action: "add", LinesTotal: 100, Added: 98, Existed: 2,
			ProcessedAt: time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC),
		}},
	}
}

// --- snapshot assembly ---------------------------------------------------------------

// TestCollectOnceSections — healthy sources: all sections collected,
// totals and rates computed (the same formulas as crawlerctl status),
// errors empty, the collection moment — RFC3339.
func TestCollectOnceSections(t *testing.T) {
	pool := healthyPool()
	lens := &fakeLenner{}
	coll := newSnapCollector(t, pool, lens, nil)
	coll.opt.Now = func() time.Time { return time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC) }

	snap := coll.CollectOnce(context.Background())
	if snap == nil {
		t.Fatal("CollectOnce = nil")
	}
	if len(snap.Errors) != 0 {
		t.Fatalf("errors = %v, want empty", snap.Errors)
	}
	// §5.1–5.4: the common sections.
	if len(snap.Zones) != 1 || snap.Zones[0].Name != "crawl.test" {
		t.Errorf("zones = %+v", snap.Zones)
	}
	if snap.Totals == nil || snap.Totals.DomainsTotal != 100 || snap.Totals.Pending != 58 {
		t.Errorf("totals = %+v", snap.Totals)
	}
	if snap.Throughput == nil || snap.Throughput.DoneToday != 3600 {
		t.Errorf("throughput = %+v", snap.Throughput)
	}
	if snap.Rates == nil || snap.Rates.Remaining != 60 || snap.Rates.DomainsPerSec == nil {
		t.Errorf("rates = %+v, want remaining 60 and a known rate", snap.Rates)
	}
	if len(snap.Queues) != len(status.Streams) {
		t.Errorf("queues = %v, want all %d streams", snap.Queues, len(status.Streams))
	}
	// §5.5–5.7.
	if snap.Statuses["new"] != 58 || snap.Statuses["alive"] != 40 || snap.Statuses["temp_error"] != 0 {
		t.Errorf("statuses = %v", snap.Statuses)
	}
	if snap.Laravel == nil || snap.Laravel.Total != 2 || snap.Laravel.Last24h != 1 || len(snap.Laravel.Recent) != 1 {
		t.Errorf("laravel = %+v", snap.Laravel)
	}
	if snap.Ingest == nil || len(snap.Ingest) != 1 || snap.Ingest[0].Action != "add" {
		t.Errorf("ingest = %+v", snap.Ingest)
	}
	// §5.4: the queue highlight thresholds — from the configuration,
	// not a section; the gate not configured — the effective thresholds
	// of the working streams are 0.
	if len(snap.QueueWatermarks) != len(status.Streams)-1 {
		t.Errorf("queue_watermarks = %v, want %d working streams (no q:dead)",
			snap.QueueWatermarks, len(status.Streams)-1)
	}
	for s, wm := range snap.QueueWatermarks {
		if wm != 0 {
			t.Errorf("queue_watermarks[%s] = %d, want 0 (the gate is not configured)", s, wm)
		}
	}
	// Probes not configured — the section is "n/a" without an error.
	if snap.Health != nil {
		t.Errorf("health = %+v, want nil (no probes set)", snap.Health)
	}
	if snap.BuiltAt != "2026-10-19T12:00:00Z" {
		t.Errorf("built_at = %q, want the RFC3339 of the Now moment", snap.BuiltAt)
	}
}

// TestCollectOnceDegradation — a DB refusal does not take down the
// queues and probes (§5.9): sections null, reasons in errors; the
// error map reaches the JSON.
func TestCollectOnceDegradation(t *testing.T) {
	pool := healthyPool()
	pool.failSQL = func(string) error { return errors.New("db: connection refused") }
	lens := &fakeLenner{}
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer alive.Close()
	coll := newSnapCollector(t, pool, lens, func(o *CollectorOptions) {
		o.Probes = map[string]string{"svc": alive.URL}
	})

	snap := coll.CollectOnce(context.Background())
	for _, section := range []string{"zones", "throughput", "queues", "statuses", "laravel", "ingest"} {
		// "queues" is alive here (the lenner healthy) — it must not be
		// in errors.
		if section == "queues" {
			continue
		}
		if _, ok := snap.Errors[section]; !ok {
			t.Errorf("errors without the section %q: %v", section, snap.Errors)
		}
	}
	if _, ok := snap.Errors["queues"]; ok {
		t.Errorf("a live Redis in errors: %v", snap.Errors)
	}
	if snap.Zones != nil || snap.Totals != nil || snap.Throughput != nil || snap.Rates != nil {
		t.Error("the DB sections are not null on a pool failure")
	}
	if snap.Statuses != nil || snap.Laravel != nil || snap.Ingest != nil {
		t.Error("the statuses/laravel/ingest sections are not null on a pool failure")
	}
	if snap.Queues == nil {
		t.Error("the queues are null with a live Redis (a DB section degradation does not take them down)")
	}
	if len(snap.Health) != 1 || !snap.Health[0].Up {
		t.Errorf("health = %+v, want a live probe (§5.9: the DB does not take the probes down)", snap.Health)
	}

	// JSON: degraded sections — null, errors — the reason map.
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	doc := string(b)
	for _, want := range []string{`"zones":null`, `"totals":null`, `"throughput":null`, `"rates":null`,
		`"statuses":null`, `"laravel":null`, `"ingest":null`, `"queues":{`,
		`"errors":{"ingest":"status: ingest: db: connection refused"`, `"health":[{"name":"svc","up":true}]`} {
		if !strings.Contains(doc, want) {
			t.Errorf("the JSON without %s:\n%s", want, doc)
		}
	}
}

// TestCollectOnceRedisDegradation — a Redis refusal: only the queues
// are null (like crawlerctl: the DB summary is alive), the rest is
// collected.
func TestCollectOnceRedisDegradation(t *testing.T) {
	pool := healthyPool()
	lens := &fakeLenner{}
	lens.err.Store(errors.New("redis: timeout"))
	coll := newSnapCollector(t, pool, lens, nil)

	snap := coll.CollectOnce(context.Background())
	if snap.Queues != nil {
		t.Error("the queues are not null on a Redis failure")
	}
	if _, ok := snap.Errors["queues"]; !ok {
		t.Errorf("errors without the queues reason: %v", snap.Errors)
	}
	if len(snap.Errors) != 1 || snap.Zones == nil || snap.Laravel == nil {
		t.Errorf("the Redis failure touched foreign sections: %v", snap.Errors)
	}
}

// hangLenner — a StreamLener hanging until ctx cancellation: a model of
// a dial to a stopped Redis container (the network endpoint persists,
// no RST).
type hangLenner struct{}

func (hangLenner) Len(ctx context.Context, _ string) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestCollectOnceRedisHang — a hung Redis (a dial to a stopped
// container without RST): Len respects ctx but does not return on its
// own — the queueFetchTimeout budget must cut the wait short, the
// queues section degrades, the other sections and the snapshot
// publication live (§5.9). The live acceptance scenario of T-131
// §12.3: a Redis stop → "n/a" within ~2 refresh cycles, not tens of
// seconds of go-redis timeouts.
func TestCollectOnceRedisHang(t *testing.T) {
	old := queueFetchTimeout
	queueFetchTimeout = 20 * time.Millisecond
	t.Cleanup(func() { queueFetchTimeout = old })

	pool := healthyPool()
	// hangLenner — instead of XLEN it simply waits for the ctx
	// cancellation (a model of a dial that hangs: a stopped container
	// does not answer with RST).
	hang := hangLenner{}
	coll := newSnapCollector(t, pool, hang, nil)

	done := make(chan *Snapshot, 1)
	go func() { done <- coll.CollectOnce(context.Background()) }()

	select {
	case snap := <-done:
		if snap.Queues != nil {
			t.Error("the queues are not null with a hung Redis")
		}
		if _, ok := snap.Errors["queues"]; !ok {
			t.Errorf("errors without the queues reason: %v", snap.Errors)
		}
		if len(snap.Errors) != 1 || snap.Totals == nil || snap.Zones == nil || snap.Laravel == nil {
			t.Errorf("the hung Redis touched foreign sections: %v (tot=%v zones=%v)", snap.Errors, snap.Totals, snap.Zones)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CollectOnce did not return: the queueFetchTimeout budget does not cut through a hung Redis")
	}
}

// TestCollectOnceNilLenner — a constructor without a lenner: the queues
// section is degraded on every pass (the Redis client is not mandatory
// for the collector).
func TestCollectOnceNilLenner(t *testing.T) {
	coll := newSnapCollector(t, healthyPool(), nil, nil)
	snap := coll.CollectOnce(context.Background())
	if snap.Queues != nil || snap.Errors["queues"] == "" {
		t.Errorf("a nil lenner: queues = %v, errors = %v — a section degradation expected", snap.Queues, snap.Errors)
	}
}

// countSQLCalls — how many times the pool saw SQL with the what
// substring.
func countSQLCalls(calls []string, what string) int {
	n := 0
	for _, q := range calls {
		if strings.Contains(q, what) {
			n++
		}
	}
	return n
}

// TestCollectOnceStatusCache — the heavy-aggregate TTL cache (T-161):
// within the TTL window the refresh passes do not repeat the full
// domains passes (zones/throughput/statuses — one selection per
// window), the light sections stay live; after the TTL expires — a
// recount.
func TestCollectOnceStatusCache(t *testing.T) {
	pool := healthyPool()
	coll := newSnapCollector(t, pool, &fakeLenner{}, func(o *CollectorOptions) {
		o.StatusCacheTTL = 50 * time.Millisecond // the window is enough for several passes
	})
	ctx := context.Background()

	snap1 := coll.CollectOnce(ctx)
	if snap1.Totals == nil || len(snap1.Errors) != 0 {
		t.Fatalf("the first pass: totals=%v errors=%v", snap1.Totals, snap1.Errors)
	}
	snap2 := coll.CollectOnce(ctx)
	if snap2.Totals == nil || len(snap2.Errors) != 0 {
		t.Fatalf("the second pass (from the cache): totals=%v errors=%v", snap2.Totals, snap2.Errors)
	}
	if snap2.Totals.DomainsTotal != snap1.Totals.DomainsTotal {
		t.Errorf("the totals from the cache = %v, want %v", snap2.Totals.DomainsTotal, snap1.Totals.DomainsTotal)
	}

	// Heavy sections — one selection per TTL window, light ones — every
	// pass.
	calls := pool.calls()
	if n := countSQLCalls(calls, "FROM zones AS z"); n != 1 {
		t.Errorf("zone selects = %d, want 1 (the cache within the TTL window)", n)
	}
	if n := countSQLCalls(calls, "AS done_today"); n != 1 {
		t.Errorf("throughput selects = %d, want 1 (the cache within the TTL window)", n)
	}
	// The status marker — LIKE 'status (domainStatusSQL): throughputSQL
	// with T-179 also reads crawler_counters (the daily counter of
	// checks).
	if n := countSQLCalls(calls, "LIKE 'status"); n != 1 {
		t.Errorf("status selects = %d, want 1 (the cache within the TTL window)", n)
	}
	if n := countSQLCalls(calls, "ORDER BY ld.updated_at"); n != 2 {
		t.Errorf("latest finds selects = %d, want 2 (a light section without a cache)", n)
	}
	if n := countSQLCalls(calls, "FROM ingest_files"); n != 2 {
		t.Errorf("ingest journal selects = %d, want 2 (a light section without a cache)", n)
	}

	// After the TTL expires the heavy sections are recomputed.
	time.Sleep(80 * time.Millisecond)
	if snap3 := coll.CollectOnce(ctx); snap3.Totals == nil || len(snap3.Errors) != 0 {
		t.Fatalf("the pass after the TTL: totals=%v errors=%v", snap3.Totals, snap3.Errors)
	}
	calls = pool.calls()
	if n := countSQLCalls(calls, "FROM zones AS z"); n != 2 {
		t.Errorf("zone selects after the TTL = %d, want 2 (a recount)", n)
	}
	if n := countSQLCalls(calls, "AS done_today"); n != 2 {
		t.Errorf("throughput selects after the TTL = %d, want 2 (a recount)", n)
	}
}

// TestNewCollectorNilPool — without a DB pool the collector is
// pointless: an error.
func TestNewCollectorNilPool(t *testing.T) {
	if _, err := NewCollector(nil, nil, config.Default().Dashboard, CollectorOptions{}); err == nil {
		t.Error("NewCollector(nil) = nil, want an error")
	}
}

// --- probes (§5.8) ---------------------------------------------------------------------

// TestProbes — the base URL gets the /livez suffix; up — 200, a
// connection refusal and non-200 — down with a reason; parallelism: two
// slow probes fit into one timeout (2 × sleep < 2 × sleep under
// sequential polling would give the sum).
func TestProbes(t *testing.T) {
	var mu sync.Mutex
	paths := map[string]string{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths[r.Host] = r.URL.Path
		mu.Unlock()
		_, _ = w.Write([]byte("ok"))
	}))
	defer up.Close()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer broken.Close()
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := closed.URL
	closed.Close()

	// Slow probes: each sleeps 250 ms, the client — 2 s (the default):
	// with parallel probes the total collection fits noticeably faster
	// than the sum.
	const slow = 250 * time.Millisecond
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(slow)
		_, _ = w.Write([]byte("ok"))
	}))
	defer slowSrv.Close()

	// A redirect from /livez — the probe does not follow it (§9.3:
	// outgoing connections to the configured addresses only): 301 stays
	// a != 200 response → down with a reason.
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, up.URL+"/elsewhere", http.StatusMovedPermanently)
	}))
	defer redir.Close()

	coll := newSnapCollector(t, healthyPool(), &fakeLenner{}, func(o *CollectorOptions) {
		o.Probes = map[string]string{
			"alive":  up.URL,
			"broken": broken.URL,
			"down":   closedURL,
			"redir":  redir.URL,
			"slow-a": slowSrv.URL,
			"slow-b": slowSrv.URL + "/", // a trailing slash of the base — not "//livez"
		}
	})

	started := time.Now()
	snap := coll.CollectOnce(context.Background())
	elapsed := time.Since(started)
	if elapsed >= 2*slow {
		t.Errorf("the probes ran sequentially: %v >= 2×%v (§5.8 — in parallel)", elapsed, slow)
	}

	if len(snap.Health) != 6 {
		t.Fatalf("health = %+v, want 6 probes", snap.Health)
	}
	// Order — by name (deterministic JSON).
	wantNames := []string{"alive", "broken", "down", "redir", "slow-a", "slow-b"}
	for i, p := range snap.Health {
		if p.Name != wantNames[i] {
			t.Errorf("health[%d].name = %q, want %q", i, p.Name, wantNames[i])
		}
	}
	if !snap.Health[0].Up {
		t.Errorf("a live probe down: %+v", snap.Health[0])
	}
	if snap.Health[1].Up || !strings.Contains(snap.Health[1].Error, "HTTP 500") {
		t.Errorf("a non-200: %+v, want down HTTP 500", snap.Health[1])
	}
	if snap.Health[2].Up || snap.Health[2].Error == "" {
		t.Errorf("a connection refusal: %+v, want down with a reason", snap.Health[2])
	}
	if snap.Health[3].Up || !strings.Contains(snap.Health[3].Error, "HTTP 301") {
		t.Errorf("a redirect: %+v, want down HTTP 301 (the probe does not follow Location)", snap.Health[3])
	}
	if !snap.Health[4].Up || !snap.Health[5].Up {
		t.Errorf("slow but answering: %+v %+v, want up", snap.Health[4], snap.Health[5])
	}

	// The probe path: base + /livez, including with a trailing slash.
	mu.Lock()
	defer mu.Unlock()
	for host, path := range paths {
		if path != "/livez" {
			t.Errorf("the probe %s went to %q, want /livez", host, path)
		}
	}
}

// TestProbeTimeout — a 2 s timeout per probe: a sleeping probe is cut
// off, the section is collected without hanging (§5.8 "unavailable or
// timed out"). The HTTPClient seam speeds the test up to 60 ms.
func TestProbeTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	defer slow.Close()

	coll := newSnapCollector(t, healthyPool(), &fakeLenner{}, func(o *CollectorOptions) {
		o.Probes = map[string]string{"slow": slow.URL}
		o.HTTPClient = &http.Client{Timeout: 60 * time.Millisecond}
	})
	snap := coll.CollectOnce(context.Background())
	if len(snap.Health) != 1 || snap.Health[0].Up {
		t.Fatalf("health = %+v, want a single down probe", snap.Health)
	}
	if !strings.Contains(snap.Health[0].Error, "Timeout") {
		t.Errorf("the reason = %q, want a client timeout", snap.Health[0].Error)
	}
}

// --- JSON shape (§6.2) -----------------------------------------------------------------

// TestSnapshotJSONForm — the /api/status shape: the common CLI sections
// at the top level (without a summary wrapper), the new §5.5–5.8
// sections, the errors map, built_at; null for the unavailable (probes
// not configured).
func TestSnapshotJSONForm(t *testing.T) {
	coll := newSnapCollector(t, healthyPool(), &fakeLenner{}, func(o *CollectorOptions) {
		o.Probes = map[string]string{"mon": "http://127.0.0.1:9100"}
		o.QueueWatermark = 100000 // the configuration default
	})
	snap := coll.CollectOnce(context.Background())
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("the snapshot is not a JSON object: %v;\n%s", err, b)
	}
	// The exact key set: 5 CLI sections + the dashboard sections +
	// built_at. render — the engines panel (T-246): null — the
	// exposure addresses not set ("n/a", not an error — like health
	// without probes).
	wantKeys := []string{"zones", "totals", "queues", "throughput", "rates",
		"statuses", "laravel", "ingest", "health", "errors", "built_at", "queue_watermarks", "render"}
	if len(doc) != len(wantKeys) {
		t.Errorf("the keys = %d (%v), want %d", len(doc), docKeys(doc), len(wantKeys))
	}
	for _, k := range wantKeys {
		if _, ok := doc[k]; !ok {
			t.Errorf("the JSON without the key %q: %s", k, b)
		}
	}
	// §5.4: effective thresholds per stream — clamped by half the
	// MAXLEN (T-141: q:dns — 125 chunks, single items — 75,000;
	// q:render-fallback — 15,000/2 = 7,500, the T-237 policy).
	if want := `{"q:analyze":75000,"q:dns":125,"q:http":75000,"q:render":75000,"q:render-fallback":7500}`; string(doc["queue_watermarks"]) != want {
		t.Errorf("queue_watermarks = %s, want %s (§5.4, the effective T-141 thresholds)", doc["queue_watermarks"], want)
	}
	// Key order — the CLI sections first (embedded SummaryJSON).
	if got := string(b); !strings.HasPrefix(got, `{"zones":`) {
		t.Errorf("the JSON starts with %q — the CLI sections must come first", got[:20])
	}
	// errors — an empty object, built_at — RFC3339.
	if string(doc["errors"]) != "{}" {
		t.Errorf("errors = %s, want {}", doc["errors"])
	}
	var builtAt time.Time
	if err := json.Unmarshal(doc["built_at"], &builtAt); err != nil {
		t.Errorf("built_at = %s: %v", doc["built_at"], err)
	}
	// Section content: a laravel find with a version range, an ingest
	// file.
	if !strings.Contains(string(doc["laravel"]), `"ver_min":"9.19"`) ||
		!strings.Contains(string(doc["laravel"]), `"last_24h":1`) {
		t.Errorf("laravel = %s", doc["laravel"])
	}
	if !strings.Contains(string(doc["ingest"]), `"path":"crawl.test/add/one.txt"`) {
		t.Errorf("ingest = %s", doc["ingest"])
	}
	// Statuses: the full spec 6.2 legend.
	var statuses map[string]int64
	if err := json.Unmarshal(doc["statuses"], &statuses); err != nil {
		t.Fatalf("statuses: %v", err)
	}
	if len(statuses) != len(model.Statuses) || statuses["new"] != 58 {
		t.Errorf("statuses = %v, want the legend of %d statuses", statuses, len(model.Statuses))
	}
}

// docKeys — the key names of a JSON object (error messages).
func docKeys(doc map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(doc))
	for k := range doc {
		keys = append(keys, k)
	}
	return keys
}

// --- /api/status (§5.9/§6.2) -----------------------------------------------------------

// TestAPIStatusSnapshot — a snapshot source: 200, JSON, no-store; a nil
// source and "no successful collection yet" — 503 with the same
// semantics as the T-128 stub.
func TestAPIStatusSnapshot(t *testing.T) {
	snap := newSnapCollector(t, healthyPool(), &fakeLenner{}, nil).
		CollectOnce(context.Background())
	srv := newTestServer(t, func(o *Options) { o.Snapshots = staticSource{snap} })
	h := srv.Handler()

	rec := doReq(h, http.MethodGet, "/api/status", "10.0.0.20:2020", authed())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/status = %d, want 200 (the body: %s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store (§6.2)", cc)
	}
	var doc Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("the body is not a JSON snapshot: %v;\n%s", err, rec.Body.String())
	}
	if len(doc.Zones) != 1 || doc.Totals == nil {
		t.Errorf("the snapshot without the CLI sections: %+v", doc)
	}

	// A source without a snapshot (the loop has not collected yet) —
	// 503 snapshot_not_ready.
	srv = newTestServer(t, func(o *Options) { o.Snapshots = staticSource{} })
	rec = doReq(srv.Handler(), http.MethodGet, "/api/status", "10.0.0.20:2021", authed())
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "snapshot_not_ready") {
		t.Errorf("without a snapshot: %d %s, want 503 snapshot_not_ready", rec.Code, rec.Body.String())
	}
}

// TestRFC3339Zero — formatting of snapshot moments: a zero time (NULL
// schema columns) — an empty string ("0001-01-01…" in the interface is
// noise), non-zero — RFC3339 in UTC regardless of the input zone.
func TestRFC3339Zero(t *testing.T) {
	if got := rfc3339Zero(time.Time{}); got != "" {
		t.Errorf("rfc3339Zero(zero) = %q, want an empty string (a NULL processed_at — empty)", got)
	}
	plus3 := time.FixedZone("utc+3", 3*60*60)
	tm := time.Date(2026, 10, 19, 9, 0, 0, 0, plus3)
	if got, want := rfc3339Zero(tm), "2026-10-19T06:00:00Z"; got != want {
		t.Errorf("rfc3339Zero(%v) = %q, want %q (a reduction to UTC)", tm, got, want)
	}
}

// TestAPIStatusNoEscapeHTML — "&" in the snapshot data is not escaped
// (SetEscapeHTML false — the crawlerctl status CLI style): a domain
// with an ampersand arrives in its original form, without \u0026.
func TestAPIStatusNoEscapeHTML(t *testing.T) {
	srv := newTestServer(t, func(o *Options) {
		o.Snapshots = staticSource{&Snapshot{Laravel: &LaravelJSON{
			Recent: []LaravelItemJSON{{Domain: "shop&co.crawl.test"}},
		}}}
	})
	rec := doReq(srv.Handler(), http.MethodGet, "/api/status", "10.0.0.21:2121", authed())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/status = %d (the body: %s)", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "shop&co.crawl.test") || strings.Contains(body, `\u0026`) {
		t.Errorf("the domain & is escaped — it must come as is (the CLI style):\n%s", body)
	}
}

// TestAPIStatusEncodeError — a snapshot encoding error (a NaN in a real
// field — the handler comment's hypothesis): 500 with a diagnosis, not
// 200 with a partial JSON — the buffer is not served.
func TestAPIStatusEncodeError(t *testing.T) {
	srv := newTestServer(t, func(o *Options) {
		o.Snapshots = staticSource{&Snapshot{Laravel: &LaravelJSON{
			Recent: []LaravelItemJSON{{Domain: "nan.crawl.test", Score: float32(math.NaN())}},
		}}}
	})
	rec := doReq(srv.Handler(), http.MethodGet, "/api/status", "10.0.0.21:2122", authed())
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("a NaN in the snapshot: status = %d, want 500 (a partial JSON with a 200 is not allowed; the body: %s)",
			rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "snapshot encode failed") {
		t.Errorf("the body = %q, want the snapshot encode failed diagnosis", rec.Body.String())
	}
}

// --- the collection loop (§4.4) ---------------------------------------------------------

// TestCollectorLoop — the first collection immediately (readiness with
// a live summary skeleton), the period — Interval, the snapshot is
// replaced atomically (the new data of the next pass). The status cache
// TTL — a millisecond: every tick and the recount of the heavy sections
// have time to expire (the T-161 cache does not stretch the data
// substitution between passes).
func TestCollectorLoop(t *testing.T) {
	pool := healthyPool()
	coll := newSnapCollector(t, pool, &fakeLenner{}, func(o *CollectorOptions) {
		o.Interval = 20 * time.Millisecond
		o.StatusCacheTTL = time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coll.Loop(ctx) }()

	// Readiness: the very first pass collected the skeleton
	// (zones/totals) — the snapshot appears without waiting for the
	// period.
	deadline := time.Now().Add(3 * time.Second)
	for coll.Current() == nil {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the snapshot did not become ready after the first pass")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Snapshot replacement: the pool data changes — the next tick
	// brings the new value; the pass counter grows with the period.
	before := len(pool.calls())
	pool.mu.Lock()
	pool.zones[0].checked = 41
	pool.mu.Unlock()
	deadline = time.Now().Add(3 * time.Second)
	var fresh *Snapshot
	for {
		fresh = coll.Current()
		if fresh != nil && fresh.Totals != nil && fresh.Totals.DomainsChecked == 41 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			if fresh == nil || fresh.Totals == nil {
				t.Fatalf("the snapshot was not replaced: a snapshot without the totals")
			}
			t.Fatalf("the snapshot was not replaced: domains_checked = %d, want 41", fresh.Totals.DomainsChecked)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if after := len(pool.calls()); after <= before {
		t.Errorf("passes after the wait = %d (was %d) — the loop does not tick", after, before)
	}

	// A ctx cancellation — a clean exit.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Loop after the cancellation: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Loop did not finish after the ctx cancellation")
	}
}

// TestCollectorLoopRedisDownFromStart — Redis is down from the very
// start: the summary skeleton is alive — the snapshot is served from the
// first pass (queues "n/a", §5.9/§12.3 — "a Redis stop does not hide
// the DB summary", including for a dashboard restart with Redis
// already down).
func TestCollectorLoopRedisDownFromStart(t *testing.T) {
	pool := healthyPool()
	lens := &fakeLenner{}
	lens.err.Store(errors.New("redis: connection refused"))
	coll := newSnapCollector(t, pool, lens, func(o *CollectorOptions) { o.Interval = 20 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- coll.Loop(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	var snap *Snapshot
	for {
		snap = coll.Current()
		if snap != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Redis is down from the very start: the snapshot is never served (an eternal 503)")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if snap.Queues != nil || snap.Errors["queues"] == "" {
		t.Errorf("queues = %v, errors = %v — a section degradation expected", snap.Queues, snap.Errors)
	}
	if snap.Totals == nil || snap.Laravel == nil {
		t.Errorf("the DB summary not collected with a live scaffold: %+v", snap)
	}
}

// TestCollectorLoopDegradedFirstPass — the DB is down: no summary
// skeleton — Current() holds nil (503, §5.9), although degraded
// snapshots are published and replaced.
func TestCollectorLoopDegradedFirstPass(t *testing.T) {
	pool := healthyPool()
	pool.failSQL = func(string) error { return errors.New("db: down") } // the DB is always down
	coll := newSnapCollector(t, pool, &fakeLenner{}, func(o *CollectorOptions) { o.Interval = 10 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- coll.Loop(ctx) }()

	deadline := time.Now().Add(500 * time.Millisecond)
	for len(pool.calls()) < 6 { // several degraded passes
		if time.Now().After(deadline) {
			t.Fatal("the loop made no passes")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := coll.Current(); got != nil {
		t.Errorf("Current() = %+v without the summary scaffold, want nil (503)", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Loop: %v", err)
	}
}

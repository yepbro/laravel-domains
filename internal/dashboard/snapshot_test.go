// Юнит-тесты сборщика снимков и /api/status (T-129, ТЗД §4.3–4.4, §5,
// §6.2, §5.9): сборка секций на фейковом пуле (SQL-контракты выборок —
// тесты internal/status), деградация секций (null + карта errors),
// параллельные пробы /livez (up/отказ/таймаут/не-200, путь и хвостовой
// слэш базы), JSON-форма снимка (поля CLI + новые секции + null),
// готовность 503→200, цикл сборки (период, атомарная замена, latch
// готовности). Поведение против живых PostgreSQL/Redis —
// test/integration/dashboard_test.go.
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

// --- фейковый пул (db.DB) --------------------------------------------------------

// snapZone — зона фейка: колонки zoneStatsSQL.
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

// snapPool — db.DB, отвечающий на выборки internal/status по форме SQL
// (тексты запросов — контракт пакета status, здесь — узнавание): зоны,
// темп, статусы, Laravel, ingest. failSQL — выборка(и), отвечающие
// ошибкой (деградация секций).
type snapPool struct {
	mu      sync.Mutex
	zones   []snapZone
	thr     status.Throughput
	stats   map[int16]int64
	laravel status.LaravelStats
	ingest  []status.IngestReport

	failSQL func(sql string) error // nil — все запросы здоровы
	queries []string
}

var _ db.DB = (*snapPool)(nil)

// Ping — /healthz зависимости; пул жив, пока жив.
func (p *snapPool) Ping(context.Context) error { return nil }

func (p *snapPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("snapPool: CopyFrom не должен вызываться")
}

func (p *snapPool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("snapPool: неожидаемый Exec: %s", sql)
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
	case strings.Contains(sql, "FROM zones AS z LEFT JOIN"):
		rows := make([][]any, len(p.zones))
		for i, z := range p.zones {
			rows[i] = []any{z.id, z.name, "", z.active, z.total, z.checked, z.pending, z.queued, z.done}
		}
		return snapRowsOf(rows...), nil
	case strings.Contains(sql, "AS done_today"):
		return snapRowsOf([]any{p.thr.DoneToday, p.thr.ElapsedSec, p.thr.ResultsHour, p.thr.RenderedHour}), nil
	case strings.Contains(sql, "GROUP BY status"):
		rows := make([][]any, 0, len(p.stats))
		for code := int16(0); code <= 9; code++ {
			if n, ok := p.stats[code]; ok {
				rows = append(rows, []any{code, n})
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
		return nil, fmt.Errorf("snapPool: неожидаемый SQL Query: %s", sql)
	}
}

// calls — накопленные SQL (порядок прохода).
func (p *snapPool) calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.queries...)
}

// snapRows — pgx.Rows с заготовленными строками; приёмники — типы,
// которые сканируют выборки internal/status (включая двойные указатели
// NULL-колонок). Локальная копия фейка (пакеты тестов не импортируют
// друг друга).
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
		return errors.New("snapRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("snapRows: приёмников %d, колонок %d", len(dest), len(row))
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
				return fmt.Errorf("snapRows: колонка %d: %T — ожидался *string", i, v)
			}
			*dst = dst2
		case **int16:
			dst2, ok := v.(*int16)
			if !ok && v != nil {
				return fmt.Errorf("snapRows: колонка %d: %T — ожидался *int16", i, v)
			}
			*dst = dst2
		case **int64:
			dst2, ok := v.(*int64)
			if !ok && v != nil {
				return fmt.Errorf("snapRows: колонка %d: %T — ожидался *int64", i, v)
			}
			*dst = dst2
		case **time.Time:
			dst2, ok := v.(*time.Time)
			if !ok && v != nil {
				return fmt.Errorf("snapRows: колонка %d: %T — ожидался *time.Time", i, v)
			}
			*dst = dst2
		default:
			return fmt.Errorf("snapRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// --- вспомогательные фейки --------------------------------------------------------

// fakeLenner — StreamLener с переключаемым отказом (err ≠ nil — Redis
// «лежит»: сценарий деградации секции очередей, в том числе с самого
// старта); здоровые вызовы возвращают возрастающие глубины.
type fakeLenner struct {
	err   atomic.Value // error или nil
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

// staticSource — источник снимка для хендлера (готовый снимок).
type staticSource struct{ snap *Snapshot }

func (s staticSource) Current() *Snapshot { return s.snap }

// newSnapCollector — сборщик на фейках: пул, lenner, пробы; мутаторы
// настраивают опции (клиент проб для таймаут-теста, часы).
func newSnapCollector(t *testing.T, pool *snapPool, lens status.StreamLener, mutate func(*CollectorOptions)) *Collector {
	t.Helper()
	cfg := config.Default()
	opts := CollectorOptions{Probes: map[string]string{}}
	if mutate != nil {
		mutate(&opts)
	}
	c, err := NewCollector(pool, lens, cfg.Dashboard, opts)
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	return c
}

// healthyPool — согласованные данные всех секций.
func healthyPool() *snapPool {
	return &snapPool{
		zones: []snapZone{{id: 1, name: "crawl.test", active: true, total: 100, checked: 40, pending: 58, queued: 2, done: 40}},
		thr:   status.Throughput{DoneToday: 3600, ElapsedSec: 3600, ResultsHour: 7200, RenderedHour: 3600},
		stats: map[int16]int64{int16(model.StatusNew): 58, int16(model.StatusAlive): 40, int16(model.StatusDeleted): 2},
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

// --- сборка снимка ----------------------------------------------------------------

// TestCollectOnceSections — здоровые источники: все секции собраны,
// итоги и скорости посчитаны (одни формулы с crawlerctl status),
// errors пуста, момент сборки — RFC3339.
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
		t.Fatalf("errors = %v, хочу пусто", snap.Errors)
	}
	// §5.1–5.4: общие секции.
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
		t.Errorf("rates = %+v, хочу remaining 60 и известную скорость", snap.Rates)
	}
	if len(snap.Queues) != len(status.Streams) {
		t.Errorf("queues = %v, хочу все %d стримов", snap.Queues, len(status.Streams))
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
	// §5.4: пороги подсветки очередей — из конфигурации, не секция;
	// гейт не настроен — эффективные пороги рабочих стримов 0.
	if len(snap.QueueWatermarks) != len(status.Streams)-1 {
		t.Errorf("queue_watermarks = %v, хочу %d рабочих стримов (без q:dead)",
			snap.QueueWatermarks, len(status.Streams)-1)
	}
	for s, wm := range snap.QueueWatermarks {
		if wm != 0 {
			t.Errorf("queue_watermarks[%s] = %d, хочу 0 (гейт не настроен)", s, wm)
		}
	}
	// Пробы не заданы — секция «н/д» без ошибки.
	if snap.Health != nil {
		t.Errorf("health = %+v, хочу nil (пробы не заданы)", snap.Health)
	}
	if snap.BuiltAt != "2026-10-19T12:00:00Z" {
		t.Errorf("built_at = %q, хочу RFC3339 момента Now", snap.BuiltAt)
	}
}

// TestCollectOnceDegradation — отказ БД не роняет очереди и пробы
// (§5.9): секции null, причины в errors; карта ошибок попадает в JSON.
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
		// «queues» здесь жива (lenner здоров) — её в errors быть не должно.
		if section == "queues" {
			continue
		}
		if _, ok := snap.Errors[section]; !ok {
			t.Errorf("errors не содержит секцию %q: %v", section, snap.Errors)
		}
	}
	if _, ok := snap.Errors["queues"]; ok {
		t.Errorf("живой Redis в errors: %v", snap.Errors)
	}
	if snap.Zones != nil || snap.Totals != nil || snap.Throughput != nil || snap.Rates != nil {
		t.Error("секции БД не null при отказе пула")
	}
	if snap.Statuses != nil || snap.Laravel != nil || snap.Ingest != nil {
		t.Error("секции статусов/laravel/ingest не null при отказе пула")
	}
	if snap.Queues == nil {
		t.Error("очереди null при живом Redis (деградация секции БД не роняет их)")
	}
	if len(snap.Health) != 1 || !snap.Health[0].Up {
		t.Errorf("health = %+v, хочу живую пробу (§5.9: БД не роняет пробы)", snap.Health)
	}

	// JSON: деградированные секции — null, errors — карта причин.
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	doc := string(b)
	for _, want := range []string{`"zones":null`, `"totals":null`, `"throughput":null`, `"rates":null`,
		`"statuses":null`, `"laravel":null`, `"ingest":null`, `"queues":{`,
		`"errors":{"ingest":"status: ingest: db: connection refused"`, `"health":[{"name":"svc","up":true}]`} {
		if !strings.Contains(doc, want) {
			t.Errorf("JSON не содержит %s:\n%s", want, doc)
		}
	}
}

// TestCollectOnceRedisDegradation — отказ Redis: только очереди null
// (как crawlerctl: сводка по БД жива), остальное собрано.
func TestCollectOnceRedisDegradation(t *testing.T) {
	pool := healthyPool()
	lens := &fakeLenner{}
	lens.err.Store(errors.New("redis: timeout"))
	coll := newSnapCollector(t, pool, lens, nil)

	snap := coll.CollectOnce(context.Background())
	if snap.Queues != nil {
		t.Error("queues не null при отказе Redis")
	}
	if _, ok := snap.Errors["queues"]; !ok {
		t.Errorf("errors без причины очередей: %v", snap.Errors)
	}
	if len(snap.Errors) != 1 || snap.Zones == nil || snap.Laravel == nil {
		t.Errorf("отказ Redis задел чужие секции: %v", snap.Errors)
	}
}

// hangLenner — StreamLener, зависающий до отмены ctx: модель dial к
// остановленному контейнеру Redis (endpoint сети сохраняется, RST нет).
type hangLenner struct{}

func (hangLenner) Len(ctx context.Context, _ string) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestCollectOnceRedisHang — зависший Redis (dial к остановленному
// контейнеру без RST): Len уважает ctx, но не возвращается сам — бюджет
// queueFetchTimeout обязан разрубить ожидание, секция очередей
// деградирует, остальные секции и публикация снимка живут (§5.9).
// Сценарий живой приёмки T-131 §12.3: остановка Redis → «н/д» за ~2
// цикла refresh, а не десятки секунд таймаутов go-redis.
func TestCollectOnceRedisHang(t *testing.T) {
	old := queueFetchTimeout
	queueFetchTimeout = 20 * time.Millisecond
	t.Cleanup(func() { queueFetchTimeout = old })

	pool := healthyPool()
	// hangLenner — вместо XLEN просто ждёт отмены ctx (модель dial,
	// который висит: остановленный контейнер не отдаёт RST).
	hang := hangLenner{}
	coll := newSnapCollector(t, pool, hang, nil)

	done := make(chan *Snapshot, 1)
	go func() { done <- coll.CollectOnce(context.Background()) }()

	select {
	case snap := <-done:
		if snap.Queues != nil {
			t.Error("queues не null при зависшем Redis")
		}
		if _, ok := snap.Errors["queues"]; !ok {
			t.Errorf("errors без причины очередей: %v", snap.Errors)
		}
		if len(snap.Errors) != 1 || snap.Totals == nil || snap.Zones == nil || snap.Laravel == nil {
			t.Errorf("зависший Redis задел чужие секции: %v (tot=%v zones=%v)", snap.Errors, snap.Totals, snap.Zones)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CollectOnce не вернулся: бюджет queueFetchTimeout не разрубает зависший Redis")
	}
}

// TestCollectOnceNilLenner — конструктор без lenner: секция очередей
// деградирована каждый проход (клиент Redis не обязателен сборщику).
func TestCollectOnceNilLenner(t *testing.T) {
	coll := newSnapCollector(t, healthyPool(), nil, nil)
	snap := coll.CollectOnce(context.Background())
	if snap.Queues != nil || snap.Errors["queues"] == "" {
		t.Errorf("nil lenner: queues = %v, errors = %v — хочу деградацию секции", snap.Queues, snap.Errors)
	}
}

// countSQLCalls — сколько раз пул видел SQL с подстрокой what.
func countSQLCalls(calls []string, what string) int {
	n := 0
	for _, q := range calls {
		if strings.Contains(q, what) {
			n++
		}
	}
	return n
}

// TestCollectOnceStatusCache — TTL-кэш тяжёлых агрегатов (T-161): в
// окне TTL проходы refresh не повторяют полные проходы domains
// (зоны/темп/статусы — одна выборка на окно), лёгкие секции остаются
// живыми; по истечении TTL — перечёт.
func TestCollectOnceStatusCache(t *testing.T) {
	pool := healthyPool()
	coll := newSnapCollector(t, pool, &fakeLenner{}, func(o *CollectorOptions) {
		o.StatusCacheTTL = 50 * time.Millisecond // окно хватает на несколько проходов
	})
	ctx := context.Background()

	snap1 := coll.CollectOnce(ctx)
	if snap1.Totals == nil || len(snap1.Errors) != 0 {
		t.Fatalf("первый проход: totals=%v errors=%v", snap1.Totals, snap1.Errors)
	}
	snap2 := coll.CollectOnce(ctx)
	if snap2.Totals == nil || len(snap2.Errors) != 0 {
		t.Fatalf("второй проход (из кэша): totals=%v errors=%v", snap2.Totals, snap2.Errors)
	}
	if snap2.Totals.DomainsTotal != snap1.Totals.DomainsTotal {
		t.Errorf("итоги из кэша = %v, want %v", snap2.Totals.DomainsTotal, snap1.Totals.DomainsTotal)
	}

	// Тяжёлые секции — одна выборка на окно TTL, лёгкие — каждый проход.
	calls := pool.calls()
	if n := countSQLCalls(calls, "FROM zones AS z LEFT JOIN"); n != 1 {
		t.Errorf("выборок зон = %d, want 1 (кэш в окне TTL)", n)
	}
	if n := countSQLCalls(calls, "AS done_today"); n != 1 {
		t.Errorf("выборок темпа = %d, want 1 (кэш в окне TTL)", n)
	}
	if n := countSQLCalls(calls, "GROUP BY status"); n != 1 {
		t.Errorf("выборок статусов = %d, want 1 (кэш в окне TTL)", n)
	}
	if n := countSQLCalls(calls, "ORDER BY ld.updated_at"); n != 2 {
		t.Errorf("выборок последних находок = %d, want 2 (лёгкая секция без кэша)", n)
	}
	if n := countSQLCalls(calls, "FROM ingest_files"); n != 2 {
		t.Errorf("выборок журнала ingest = %d, want 2 (лёгкая секция без кэша)", n)
	}

	// По истечении TTL тяжёлые секции пересчитываются.
	time.Sleep(80 * time.Millisecond)
	if snap3 := coll.CollectOnce(ctx); snap3.Totals == nil || len(snap3.Errors) != 0 {
		t.Fatalf("проход после TTL: totals=%v errors=%v", snap3.Totals, snap3.Errors)
	}
	calls = pool.calls()
	if n := countSQLCalls(calls, "FROM zones AS z LEFT JOIN"); n != 2 {
		t.Errorf("выборок зон после TTL = %d, want 2 (перечёт)", n)
	}
	if n := countSQLCalls(calls, "AS done_today"); n != 2 {
		t.Errorf("выборок темпа после TTL = %d, want 2 (перечёт)", n)
	}
}

// TestNewCollectorNilPool — без пула БД сборщик бессмыслен: ошибка.
func TestNewCollectorNilPool(t *testing.T) {
	if _, err := NewCollector(nil, nil, config.Default().Dashboard, CollectorOptions{}); err == nil {
		t.Error("NewCollector(nil) = nil, хочу ошибку")
	}
}

// --- пробы (§5.8) ------------------------------------------------------------------

// TestProbes — базовый URL получает суффикс /livez; up — 200, отказ
// соединения и не-200 — down с причиной; параллельность: две медленные
// пробы укладываются в один таймаут (2 × sleep < 2 × sleep при
// последовательном опросе дала бы сумму).
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

	// Медленные пробы: каждая спит 250 мс, клиент — 2 с (дефолт): при
	// параллельных пробах общий сбор укладывается заметно быстрее суммы.
	const slow = 250 * time.Millisecond
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(slow)
		_, _ = w.Write([]byte("ok"))
	}))
	defer slowSrv.Close()

	// Редирект с /livez — проба не следует ему (§9.3: исходящие
	// соединения только на настроенные адреса): 301 остаётся ответом
	// != 200 → down с причиной.
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
			"slow-b": slowSrv.URL + "/", // хвостовой слэш базы — не «//livez»
		}
	})

	started := time.Now()
	snap := coll.CollectOnce(context.Background())
	elapsed := time.Since(started)
	if elapsed >= 2*slow {
		t.Errorf("пробы шли последовательно: %v >= 2×%v (§5.8 — параллельно)", elapsed, slow)
	}

	if len(snap.Health) != 6 {
		t.Fatalf("health = %+v, хочу 6 проб", snap.Health)
	}
	// Порядок — по имени (детерминированный JSON).
	wantNames := []string{"alive", "broken", "down", "redir", "slow-a", "slow-b"}
	for i, p := range snap.Health {
		if p.Name != wantNames[i] {
			t.Errorf("health[%d].name = %q, хочу %q", i, p.Name, wantNames[i])
		}
	}
	if !snap.Health[0].Up {
		t.Errorf("живая проба down: %+v", snap.Health[0])
	}
	if snap.Health[1].Up || !strings.Contains(snap.Health[1].Error, "HTTP 500") {
		t.Errorf("не-200: %+v, хочу down «HTTP 500»", snap.Health[1])
	}
	if snap.Health[2].Up || snap.Health[2].Error == "" {
		t.Errorf("отказ соединения: %+v, хочу down с причиной", snap.Health[2])
	}
	if snap.Health[3].Up || !strings.Contains(snap.Health[3].Error, "HTTP 301") {
		t.Errorf("редирект: %+v, хочу down «HTTP 301» (проба не следует за Location)", snap.Health[3])
	}
	if !snap.Health[4].Up || !snap.Health[5].Up {
		t.Errorf("медленные, но ответившие: %+v %+v, хочу up", snap.Health[4], snap.Health[5])
	}

	// Путь пробы: base + /livez, в том числе с хвостовым слэшем.
	mu.Lock()
	defer mu.Unlock()
	for host, path := range paths {
		if path != "/livez" {
			t.Errorf("проба %s пошла на %q, хочу /livez", host, path)
		}
	}
}

// TestProbeTimeout — таймаут 2 с на пробу: спящая проба обрывается,
// секция собирается без зависания (§5.8 «недоступен или таймаут»).
// Шов HTTPClient ускоряет тест до 60 мс.
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
		t.Fatalf("health = %+v, хочу одну down-пробу", snap.Health)
	}
	if !strings.Contains(snap.Health[0].Error, "Timeout") {
		t.Errorf("причина = %q, хочу таймаут клиента", snap.Health[0].Error)
	}
}

// --- JSON-форма (§6.2) --------------------------------------------------------------

// TestSnapshotJSONForm — форма /api/status: общие секции CLI на верхнем
// уровне (без обёртки summary), новые секции §5.5–5.8, карта errors,
// built_at; null для недоступного (пробы не заданы).
func TestSnapshotJSONForm(t *testing.T) {
	coll := newSnapCollector(t, healthyPool(), &fakeLenner{}, func(o *CollectorOptions) {
		o.Probes = map[string]string{"mon": "http://127.0.0.1:9100"}
		o.QueueWatermark = 100000 // дефолт конфигурации
	})
	snap := coll.CollectOnce(context.Background())
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("снимок не JSON-объект: %v;\n%s", err, b)
	}
	// Точный набор ключей: 5 секций CLI + секции дашборда + built_at.
	wantKeys := []string{"zones", "totals", "queues", "throughput", "rates",
		"statuses", "laravel", "ingest", "health", "errors", "built_at", "queue_watermarks"}
	if len(doc) != len(wantKeys) {
		t.Errorf("ключи = %d (%v), хочу %d", len(doc), docKeys(doc), len(wantKeys))
	}
	for _, k := range wantKeys {
		if _, ok := doc[k]; !ok {
			t.Errorf("JSON без ключа %q: %s", k, b)
		}
	}
	// §5.4: эффективные пороги по стримам — кламп по половине MAXLEN
	// (T-141: q:dns — 125 пачек, штучные — 75 000 при дефолтах).
	if want := `{"q:analyze":75000,"q:dns":125,"q:http":75000,"q:render":75000}`; string(doc["queue_watermarks"]) != want {
		t.Errorf("queue_watermarks = %s, хочу %s (§5.4, эффективные пороги T-141)", doc["queue_watermarks"], want)
	}
	// Порядок ключей — секции CLI первыми (embedded SummaryJSON).
	if got := string(b); !strings.HasPrefix(got, `{"zones":`) {
		t.Errorf("JSON начинается с %q — секции CLI должны быть первыми", got[:20])
	}
	// errors — пустой объект, built_at — RFC3339.
	if string(doc["errors"]) != "{}" {
		t.Errorf("errors = %s, хочу {}", doc["errors"])
	}
	var builtAt time.Time
	if err := json.Unmarshal(doc["built_at"], &builtAt); err != nil {
		t.Errorf("built_at = %s: %v", doc["built_at"], err)
	}
	// Содержимое секций: laravel-находка с диапазоном версий, ingest-файл.
	if !strings.Contains(string(doc["laravel"]), `"ver_min":"9.19"`) ||
		!strings.Contains(string(doc["laravel"]), `"last_24h":1`) {
		t.Errorf("laravel = %s", doc["laravel"])
	}
	if !strings.Contains(string(doc["ingest"]), `"path":"crawl.test/add/one.txt"`) {
		t.Errorf("ingest = %s", doc["ingest"])
	}
	// Статусы: полная легенда ТЗ 6.2.
	var statuses map[string]int64
	if err := json.Unmarshal(doc["statuses"], &statuses); err != nil {
		t.Fatalf("statuses: %v", err)
	}
	if len(statuses) != len(model.Statuses) || statuses["new"] != 58 {
		t.Errorf("statuses = %v, хочу легенду %d статусов", statuses, len(model.Statuses))
	}
}

// docKeys — имена ключей JSON-объекта (сообщения об ошибке).
func docKeys(doc map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(doc))
	for k := range doc {
		keys = append(keys, k)
	}
	return keys
}

// --- /api/status (§5.9/§6.2) ---------------------------------------------------------

// TestAPIStatusSnapshot — источник снимка: 200, JSON, no-store; nil-источник
// и «нет ещё успешного сбора» — 503 с той же семантикой, что заглушка T-128.
func TestAPIStatusSnapshot(t *testing.T) {
	snap := newSnapCollector(t, healthyPool(), &fakeLenner{}, nil).
		CollectOnce(context.Background())
	srv := newTestServer(t, func(o *Options) { o.Snapshots = staticSource{snap} })
	h := srv.Handler()

	rec := doReq(h, http.MethodGet, "/api/status", "10.0.0.20:2020", authed())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/status = %d, хочу 200 (тело: %s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, хочу application/json", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, хочу no-store (§6.2)", cc)
	}
	var doc Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("тело не JSON-снимок: %v;\n%s", err, rec.Body.String())
	}
	if len(doc.Zones) != 1 || doc.Totals == nil {
		t.Errorf("снимок без секций CLI: %+v", doc)
	}

	// Источник без снимка (цикл ещё не собрал) — 503 snapshot_not_ready.
	srv = newTestServer(t, func(o *Options) { o.Snapshots = staticSource{} })
	rec = doReq(srv.Handler(), http.MethodGet, "/api/status", "10.0.0.20:2021", authed())
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "snapshot_not_ready") {
		t.Errorf("без снимка: %d %s, хочу 503 snapshot_not_ready", rec.Code, rec.Body.String())
	}
}

// TestRFC3339Zero — форматирование моментов снимка: zero-время (NULL
// колонки схемы) — пустая строка («0001-01-01…» в интерфейсе — шум),
// ненулевое — RFC3339 в UTC независимо от зоны входа.
func TestRFC3339Zero(t *testing.T) {
	if got := rfc3339Zero(time.Time{}); got != "" {
		t.Errorf("rfc3339Zero(zero) = %q, хочу пустую строку (NULL processed_at — пусто)", got)
	}
	plus3 := time.FixedZone("utc+3", 3*60*60)
	tm := time.Date(2026, 10, 19, 9, 0, 0, 0, plus3)
	if got, want := rfc3339Zero(tm), "2026-10-19T06:00:00Z"; got != want {
		t.Errorf("rfc3339Zero(%v) = %q, хочу %q (приведение к UTC)", tm, got, want)
	}
}

// TestAPIStatusNoEscapeHTML — «&» в данных снимка не экранируется
// (SetEscapeHTML false — стиль CLI crawlerctl status): домен с
// амперсандом приходит в исходном виде, без \u0026.
func TestAPIStatusNoEscapeHTML(t *testing.T) {
	srv := newTestServer(t, func(o *Options) {
		o.Snapshots = staticSource{&Snapshot{Laravel: &LaravelJSON{
			Recent: []LaravelItemJSON{{Domain: "shop&co.crawl.test"}},
		}}}
	})
	rec := doReq(srv.Handler(), http.MethodGet, "/api/status", "10.0.0.21:2121", authed())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/status = %d (тело: %s)", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "shop&co.crawl.test") || strings.Contains(body, `\u0026`) {
		t.Errorf("«&» домена экранирован — должен приходить как есть (стиль CLI):\n%s", body)
	}
}

// TestAPIStatusEncodeError — ошибка кодирования снимка (NaN в real-поле —
// гипотеза комментария хендлера): 500 с диагнозом, а не 200 с частичным
// JSON — буфер не отдаётся.
func TestAPIStatusEncodeError(t *testing.T) {
	srv := newTestServer(t, func(o *Options) {
		o.Snapshots = staticSource{&Snapshot{Laravel: &LaravelJSON{
			Recent: []LaravelItemJSON{{Domain: "nan.crawl.test", Score: float32(math.NaN())}},
		}}}
	})
	rec := doReq(srv.Handler(), http.MethodGet, "/api/status", "10.0.0.21:2122", authed())
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("NaN в снимке: статус = %d, хочу 500 (частичный JSON со 200 недопустим; тело: %s)",
			rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "snapshot encode failed") {
		t.Errorf("тело = %q, хочу диагноз snapshot encode failed", rec.Body.String())
	}
}

// --- цикл сборки (§4.4) --------------------------------------------------------------

// TestCollectorLoop — первый сбор сразу (готовность при живом каркасе
// сводки), период — Interval, снимок атомарно заменяется (новые данные
// следующего прохода). TTL кэша статуса — миллисекунда: каждые тик и
// перечёт тяжёлых секций успевают протухнуть (кэш T-161 не затягивает
// подмену данных между проходами).
func TestCollectorLoop(t *testing.T) {
	pool := healthyPool()
	coll := newSnapCollector(t, pool, &fakeLenner{}, func(o *CollectorOptions) {
		o.Interval = 20 * time.Millisecond
		o.StatusCacheTTL = time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coll.Loop(ctx) }()

	// Готовность: первый же проход собрал каркас (зоны/итоги) — снимок
	// появляется без задержки на период.
	deadline := time.Now().Add(3 * time.Second)
	for coll.Current() == nil {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("снимок не стал готовым после первого прохода")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Замена снимка: меняем данные пула — очередной тик приносит новое
	// значение; счётчик проходов растёт с периодом.
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
				t.Fatalf("снимок не заменился: снимок без итогов")
			}
			t.Fatalf("снимок не заменился: domains_checked = %d, хочу 41", fresh.Totals.DomainsChecked)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if after := len(pool.calls()); after <= before {
		t.Errorf("проходов после ожидания = %d (было %d) — цикл не тикает", after, before)
	}

	// Отмена ctx — чистый выход.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Loop после отмены: %v, хочу nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Loop не завершился после отмены ctx")
	}
}

// TestCollectorLoopRedisDownFromStart — Redis лежит с самого старта:
// каркас сводки жив — снимок обслуживается с первого прохода (очереди
// «н/д», §5.9/§12.3 — «остановка Redis не прячет сводку по БД», в том
// числе для рестарта дашборда при уже лежащем Redis).
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
			t.Fatal("Redis вниз с самого старта: снимок так и не обслуживается (вечный 503)")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if snap.Queues != nil || snap.Errors["queues"] == "" {
		t.Errorf("очереди = %v, errors = %v — хочу деградацию секции", snap.Queues, snap.Errors)
	}
	if snap.Totals == nil || snap.Laravel == nil {
		t.Errorf("сводка по БД не собрана при живом каркасе: %+v", snap)
	}
}

// TestCollectorLoopDegradedFirstPass — БД лежит: каркаса сводки нет —
// Current() держит nil (503, §5.9), хотя деградированные снимки
// публикуются и заменяются.
func TestCollectorLoopDegradedFirstPass(t *testing.T) {
	pool := healthyPool()
	pool.failSQL = func(string) error { return errors.New("db: down") } // БД лежит всегда
	coll := newSnapCollector(t, pool, &fakeLenner{}, func(o *CollectorOptions) { o.Interval = 10 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- coll.Loop(ctx) }()

	deadline := time.Now().Add(500 * time.Millisecond)
	for len(pool.calls()) < 6 { // несколько деградированных проходов
		if time.Now().After(deadline) {
			t.Fatal("цикл не сделал проходов")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := coll.Current(); got != nil {
		t.Errorf("Current() = %+v без каркаса сводки, хочу nil (503)", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Loop: %v", err)
	}
}

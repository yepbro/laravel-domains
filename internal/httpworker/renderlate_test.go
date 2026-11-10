// Юнит-тесты позднего переиздателя рендера (T-259, ТЗ-fsm §3 ФТ-5.3)
// на фейковых пуле/хранилище/очереди/снимке стримов — без БД, Redis и
// сети (стиль renderq_test.go, чьи фейки очереди/хранилища/глубины и
// pgx-строки переиспользуются). Проверяются: точные SQL и параметры
// выборки (курсор PK, окно возраста, охраны бита/статуса/маркера),
// интерлок снимком непотреблённых q:render/q:render-fallback (живая
// задача не переиздаётся, ошибка снимка — консервативный пропуск),
// SPA-вердикт по контенту и наследование алиасом, курсорная дисциплина
// (пачка целиком — курсор вперёд, короткая — замыкание, Full/ошибка —
// курсор на месте), метрика crawler_render_late_republish_total, цикл.
// ТЗ T-271: tx-оболочка закрепления плана выборки (SET LOCAL ×4 →
// SELECT → COMMIT), потолок SelectTimeout, гейт тяжёлых сканов и
// backoff цикла после ошибки. Поведение против живых PostgreSQL/Redis —
// test/integration render_late_test.go.
package httpworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- фейковый источник непотреблённых ключей (интерлок) -----------------------

// latePending — content.RetentionPending с заготовленными множествами
// по стримам и настраиваемым сбоем.
type latePending struct {
	mu    sync.Mutex
	sets  map[string]map[content.RetentionPendingKey]struct{}
	errOn map[string]error
	calls []string
}

var _ content.RetentionPending = (*latePending)(nil)

func newLatePending() *latePending {
	return &latePending{
		sets:  make(map[string]map[content.RetentionPendingKey]struct{}),
		errOn: make(map[string]error),
	}
}

// add вызывается из горутины теста, пока цикл читает снимки через
// PendingKeys — доступ к sets строго под мьютексом (race-детектор).
func (p *latePending) add(stream string, k content.RetentionPendingKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	set := p.sets[stream]
	if set == nil {
		set = make(map[content.RetentionPendingKey]struct{})
		p.sets[stream] = set
	}
	set[k] = struct{}{}
}

func (p *latePending) PendingKeys(ctx context.Context, stream string) (map[content.RetentionPendingKey]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, stream)
	if err := p.errOn[stream]; err != nil {
		return nil, err
	}
	out := make(map[content.RetentionPendingKey]struct{}, len(p.sets[stream]))
	for k := range p.sets[stream] {
		out[k] = struct{}{}
	}
	return out, nil
}

// --- фейковый пул (db.DB): candidates/alias -----------------------------------

// lateSeedRow — строка subdomain_results фейка.
type lateSeedRow struct {
	key      lateKey
	hash     []byte // nil — content_hash NULL
	aliasSub *int16 // не nil — алиас (наследует вердикт оригинала)
	rendered bool
	finalURL string // "" — NULL (фильтр final_url IS NOT NULL)
	marked   bool   // штамп render_failed_at (фильтр IS NULL)
	old      bool   // checked_at старше окна возраста (фильтр ФТ-5.3)
}

// lateSeedDomain — строка domains фейка.
type lateSeedDomain struct {
	zone   int16
	id     int64
	label  string
	flags  model.Flags
	status model.Status
}

// latePool — db.DB прохода без БД: живая семантика фильтров
// lateRenderRowsSQL/lateRenderAliasSQL (rendered, final_url, маркер,
// окно возраста, бит render_pending, status <> deleted, курсор PK,
// LIMIT; алиасный запрос — предикаты renderPendingRowsSQL по субдоменам
// ANY); SQL сопоставляется ТОЧНО (неожидаемый — ошибка, тест падает
// громко, стиль renderqPool). С T-271 пул — RecordDB: тяжёлая выборка
// идёт в транзакции закрепления плана (Begin → lateTx), алиасный запрос
// остаётся точечным через пул.
type latePool struct {
	mu      sync.Mutex
	domains map[lateKey]*lateSeedDomain // key.domain — идентификатор (sub/variant нулевые)
	rows    []lateSeedRow

	rowsErr   error
	aliasErr  error
	rowsCalls int
	aliasLast []int16 // субдомены последнего алиасного запроса
	begins    int
	txs       []*lateTx
	// rowsBlock — «зависшая» выборка: ответ только по ctx-дедлайну
	// (тест ctx-таймаута T-271).
	rowsBlock bool
	// rowsDelay — задержка тяжёлой выборки (тест семантики backoff
	// T-273: проход длится дольше тика интервала); beginsAt — метки
	// времени Begin (старты проходов).
	rowsDelay time.Duration
	beginsAt  []time.Time
}

var _ RecordDB = (*latePool)(nil)

func newLatePool() *latePool {
	return &latePool{domains: make(map[lateKey]*lateSeedDomain)}
}

func (p *latePool) seedDomain(zone int16, id int64, label string, flags model.Flags, status model.Status) {
	p.domains[lateKey{zone: zone, domain: id}] = &lateSeedDomain{zone: zone, id: id, label: label, flags: flags, status: status}
}

func (p *latePool) seedRow(r lateSeedRow) { p.rows = append(p.rows, r) }

func (p *latePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("latePool: CopyFrom не должен вызываться")
}

func (p *latePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("latePool: Exec не должен вызываться (проход БД не пишет)")
}

func (p *latePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch sql {
	case lateRenderAliasSQL:
		return p.queryAlias(args)
	default:
		return nil, fmt.Errorf("latePool: неожидаемый SQL Query: %s", sql)
	}
}

// Begin — транзакция потолка выборки (T-271): тяжёлая выборка пачки
// идёт через lateTx, точечные запросы остаются в пуле.
func (p *latePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	p.beginsAt = append(p.beginsAt, time.Now())
	tx := &lateTx{pool: p}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// lateTx — транзакция потолка выборки: единственный SET LOCAL —
// statement_timeout до SELECT (закрепления плана сняты T-273), calls —
// упорядоченный журнал операторов (контракт порядка, стиль
// retentionFakeTx.calls).
type lateTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует

	pool      *latePool
	calls     []string
	committed bool
}

func (t *lateTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, sql)
	// С T-273 оболочка — только потолок оператора: закрепления формы
	// (SET LOCAL enable_*) разворачивали join от domains на прод-объёме
	// (вечная петля за дедлайном) — неожидаемый Exec падает громко.
	if strings.HasPrefix(sql, "SET LOCAL statement_timeout = ") {
		return pgconn.NewCommandTag("SET"), nil
	}
	return pgconn.CommandTag{}, fmt.Errorf("lateTx: неожидаемый Exec %q", sql)
}

func (t *lateTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	// «Зависшая» выборка (тест ctx-таймаута T-271): ответ только по
	// дедлайну, до журналов и семантики.
	if sql == lateRenderRowsSQL && t.pool.rowsBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	// Задержка тяжёлой выборки (семантика backoff T-273): сон вне
	// мьютекса — пул остаётся доступен тесту.
	if sql == lateRenderRowsSQL {
		t.pool.mu.Lock()
		delay := t.pool.rowsDelay
		t.pool.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, sql)
	if sql != lateRenderRowsSQL {
		return nil, fmt.Errorf("lateTx: неожидаемый SQL Query: %s", sql)
	}
	return t.pool.queryRows(args)
}

func (t *lateTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.committed = true
	return nil
}

func (t *lateTx) Rollback(context.Context) error { return nil }

func (p *latePool) queryRows(args []any) (pgx.Rows, error) {
	if p.rowsErr != nil {
		return nil, p.rowsErr
	}
	if len(args) != 6 {
		return nil, fmt.Errorf("latePool: выборка: параметров %d, нужно 6", len(args))
	}
	before, ok := args[0].(time.Time)
	if !ok {
		return nil, fmt.Errorf("latePool: выборка: граница возраста %T, нужно time.Time", args[0])
	}
	if before.IsZero() || before.After(time.Now()) {
		return nil, fmt.Errorf("latePool: выборка: граница возраста %v вне окна (now−After)", before)
	}
	cur, ok := lateKeyOf(args[1], args[2], args[3], args[4])
	if !ok {
		return nil, fmt.Errorf("latePool: выборка: курсор %T/%T/%T/%T", args[1], args[2], args[3], args[4])
	}
	limit, ok := args[5].(int)
	if !ok || limit <= 0 {
		return nil, fmt.Errorf("latePool: выборка: LIMIT %v (%T)", args[5], args[5])
	}
	p.rowsCalls++

	// Детерминированный PK-порядок (ORDER BY zone, domain, sub, variant).
	sorted := make([]lateSeedRow, len(p.rows))
	copy(sorted, p.rows)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if lateKeyLess(sorted[j].key, sorted[i].key) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	out := make([][]any, 0, limit)
	for i := range sorted {
		r := &sorted[i]
		if r.rendered || r.finalURL == "" || r.marked || !r.old {
			continue // предикаты строки
		}
		d := p.domains[lateKey{zone: r.key.zone, domain: r.key.domain}]
		if d == nil {
			continue // JOIN domains — строки нет
		}
		if d.flags.Test(model.FlagRenderPending) || d.status == model.StatusDeleted {
			continue // бит render_pending / §13.1
		}
		if !lateKeyLess(cur, r.key) {
			continue // курсор row-value >
		}
		var hash, alias any
		if r.hash != nil {
			hash = r.hash
		}
		if r.aliasSub != nil {
			sub := *r.aliasSub
			alias = &sub
		}
		out = append(out, []any{r.key.zone, r.key.domain, r.key.sub, int16(r.key.variant), hash, alias, d.label + ".zone"})
		if len(out) == limit {
			break
		}
	}
	return renderqRowsOf(out...), nil
}

func (p *latePool) queryAlias(args []any) (pgx.Rows, error) {
	if p.aliasErr != nil {
		return nil, p.aliasErr
	}
	if len(args) != 3 {
		return nil, fmt.Errorf("latePool: алиасы: параметров %d, нужно 3", len(args))
	}
	zone, okZ := args[0].(int16)
	id, okI := args[1].(int64)
	subs, okS := args[2].([]int16)
	if !okZ || !okI || !okS {
		return nil, fmt.Errorf("latePool: алиасы: параметры %T/%T/%T", args[0], args[1], args[2])
	}
	p.aliasLast = append(p.aliasLast[:0], subs...)

	want := make(map[int16]bool, len(subs))
	for _, s := range subs {
		want[s] = true
	}
	rows := make([]lateSeedRow, 0, 4)
	for i := range p.rows {
		r := p.rows[i]
		// Предикат lateRenderAliasSQL: кандидаты (не отрендерено, есть
		// цель, нет маркера) ПЛЮС отрендеренные (факт рендера — вердикт
		// класса, ревью T-259 P2-1); отмаркированные — нет.
		if r.key.zone == zone && r.key.domain == id && want[r.key.sub] &&
			(r.rendered || (!r.marked && r.finalURL != "")) {
			rows = append(rows, r)
		}
	}
	// ORDER BY subdomain_id, variant.
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			a, b := rows[i].key, rows[j].key
			if a.sub > b.sub || (a.sub == b.sub && a.variant > b.variant) {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
	out := make([][]any, 0, len(rows))
	for _, r := range rows {
		var hash any
		if r.hash != nil {
			hash = r.hash
		}
		out = append(out, []any{r.key.sub, int16(r.key.variant), hash, r.rendered})
	}
	return renderqRowsOf(out...), nil
}

// lateKeyOf — курсор из параметров SQL (int16/int64/int16/int16).
func lateKeyOf(zone, domain, sub, variant any) (lateKey, bool) {
	z, ok1 := zone.(int16)
	d, ok2 := domain.(int64)
	s, ok3 := sub.(int16)
	v, ok4 := variant.(int16)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return lateKey{}, false
	}
	return lateKey{zone: z, domain: d, sub: s, variant: model.Variant(v)}, true
}

func lateKeyLess(a, b lateKey) bool {
	if a.zone != b.zone {
		return a.zone < b.zone
	}
	if a.domain != b.domain {
		return a.domain < b.domain
	}
	if a.sub != b.sub {
		return a.sub < b.sub
	}
	return a.variant < b.variant
}

// testLateAfter — окно возраста тестов (1 час: old=true — «старше окна»).
const testLateAfter = time.Hour

// lateFixture — сборка прохода: пул, контент (SPA/не-SPA тела), очередь,
// глубина (свободна), снимок, метрики.
type lateFixture struct {
	pool     *latePool
	bodies   *renderqContent
	q        *renderqQueue
	lenner   *renderqLen
	pending  *latePending
	met      *Metrics
	reg      *metrics.Registry
	spaHash  content.Hash
	spaHash2 content.Hash
	rawHash  content.Hash
}

// newLateFixture — домен/зона сидированы отдельно тестом.
func newLateFixture(t *testing.T) *lateFixture {
	t.Helper()
	f := &lateFixture{
		pool:    newLatePool(),
		bodies:  &renderqContent{bodies: make(map[content.Hash][]byte)},
		q:       &renderqQueue{},
		lenner:  &renderqLen{},
		pending: newLatePending(),
		reg:     metrics.New("http-worker-test"),
	}
	f.met = NewMetrics(f.reg)
	// SPA-тела (T-057) и статическое — те же, что в фикстурах renderq;
	// второе SPA-тело — для многорядных сценариев с независимым сбоем
	// чтения (копия тела с постфиксом — вердикт детерминирован). Хэши
	// различаются старшим байтом: ключи карт тел/сбоев не сливаются.
	f.spaHash[0] = 0x02
	f.spaHash2[0] = 0x03
	f.rawHash[0] = 0x01
	f.bodies.bodies[f.spaHash] = []byte(`<html><head><title>x</title></head><body><div id="root"></div><noscript>please enable javascript</noscript></body></html>`)
	f.bodies.bodies[f.spaHash2] = append([]byte(`<html><head><title>x</title></head><body><div id="root"></div><noscript>please enable javascript</noscript></body></html>`), ' ')
	f.bodies.bodies[f.rawHash] = []byte(`<html><body>plain static page with lots of visible text here</body></html>`)
	return f
}

func (f *lateFixture) renderQ(t *testing.T) *RenderQueue {
	t.Helper()
	rq, err := NewRenderQueue(f.q, f.lenner, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	return rq
}

// published — снимок публикаций q:render.
func (f *lateFixture) published() []renderqPublish { return f.q.snapshot() }

// lateOpts — опции тестов: окно 1 ч, пачка 100.
func lateOpts() LateOptions { return LateOptions{BatchSize: 100, After: testLateAfter} }

// --- выборка -------------------------------------------------------------------

// TestRenderLateSelect — точный SQL выборки: курсор row-value > по PK,
// ORDER BY PK, LIMIT, граница возраста параметром; предикаты (rendered,
// final_url, маркер, возраст, бит render_pending, deleted) — живой
// смысл фейка проверяется поведением ниже. Выборка идёт в tx-оболочке
// потолка statement_timeout (T-271/T-273) — порядок операторов
// проверяет TestRenderLateSelectShell.
func TestRenderLateSelect(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	cur := &RenderLateCursor{}
	rows, err := lateSelect(ctx, f.pool, lateOpts(), time.Now().Add(-testLateAfter), cur.key)
	if err != nil {
		t.Fatalf("lateSelect пустого пула: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("пустой пул: строк %d, хочу 0", len(rows))
	}

	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 0, variant: 0}, old: true, finalURL: "https://x/", hash: f.spaHash[:]})
	rows, err = lateSelect(ctx, f.pool, lateOpts(), time.Now().Add(-testLateAfter), cur.key)
	if err != nil {
		t.Fatalf("lateSelect: %v", err)
	}
	if len(rows) != 1 || rows[0].name != "spa.zone" {
		t.Fatalf("строки %+v, хочу одну с именем spa.zone", rows)
	}
	if rows[0].hash == nil || rows[0].aliasSub != nil {
		t.Fatalf("hash/alias строки: %+v", rows[0])
	}

	// Курсор: строка за курсором не выбирается.
	cur.key = rows[0].key
	if rows, err = lateSelect(ctx, f.pool, lateOpts(), time.Now().Add(-testLateAfter), cur.key); err != nil || len(rows) != 0 {
		t.Fatalf("выборка за курсором: %d строк (err=%v), хочу 0", len(rows), err)
	}
}

// TestRenderLateSelectShell — tx-оболочка выборки (T-271 завёл
// закрепление формы, T-273 снял его): пачка читается в явной
// транзакции строго последовательностью SET LOCAL statement_timeout →
// SELECT → COMMIT. Закрепления (SET LOCAL enable_seqscan/hashjoin/
// mergejoin/bitmapscan = off) удалены сознательно — на прод-объёме
// (domains 43 млн) пин оставлял планировщику только Nested Loop и
// разворачивал join от domains, cost ≈157 млн при дедлайне 60 с —
// вечная петля обречённых выборок (инцидент 09–10.11.2026); неожидаемый
// enable_* в журнале падает громко (фейк не знает такой Exec).
func TestRenderLateSelectShell(t *testing.T) {
	f := newLateFixture(t)
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://x/", hash: f.spaHash[:]})

	if _, err := LateOnce(context.Background(), f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), &RenderLateCursor{}); err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	if len(f.pool.txs) != 1 {
		t.Fatalf("транзакций = %d, хочу 1 (tx-оболочка выборки)", len(f.pool.txs))
	}
	tx := f.pool.txs[0]
	// Текст потолка — литерал (контракт интеграционного теста, стиль
	// renderq_test.go); значение — дефолт 300 с T-273.
	if lateSelectTimeoutFmt != "SET LOCAL statement_timeout = '%dms'" {
		t.Fatalf("текст потолка выборки изменён")
	}
	want := []string{
		fmt.Sprintf(lateSelectTimeoutFmt, DefaultRenderLateSelectTimeout.Milliseconds()),
		lateRenderRowsSQL,
	}
	if len(tx.calls) != len(want) {
		t.Fatalf("операторов tx-оболочки = %d, хочу %d (SET LOCAL ×1 + SELECT): %v", len(tx.calls), len(want), tx.calls)
	}
	for i, sql := range want {
		if tx.calls[i] != sql {
			t.Errorf("шаг %d = %s, хочу %s (порядок SET LOCAL statement_timeout → SELECT)", i, tx.calls[i], sql)
		}
	}
	for _, sql := range tx.calls {
		if strings.HasPrefix(sql, "SET LOCAL enable_") {
			t.Errorf("оболочка содержит закрепление плана %q — сняты T-273 (вечная петля обречённых выборок)", sql)
		}
	}
	if !tx.committed {
		t.Error("tx-оболочка выборки не зафиксирована")
	}
}

// TestRenderLateSelectCtxTimeout — зависшая выборка режется клиентским
// ctx-дедлайном SelectTimeout (T-271): обёрнутая ошибка с контекстом
// «выборка», курсор на месте — пачка повторится (идемпотентность, ТЗ 3.4).
func TestRenderLateSelectCtxTimeout(t *testing.T) {
	f := newLateFixture(t)
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://x/", hash: f.spaHash[:]})
	f.pool.rowsBlock = true // SELECT не отвечает до отмены ctx

	opts := lateOpts()
	opts.SelectTimeout = 40 * time.Millisecond
	cur := &RenderLateCursor{}
	_, err := LateOnce(context.Background(), f.pool, f.bodies, f.renderQ(t), f.pending, f.met, opts, cur)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "выборка") {
		t.Fatalf("ошибка = %v, хочу обёрнутый context.DeadlineExceeded «выборка»", err)
	}
	if cur.key != (lateKey{}) {
		t.Fatalf("курсор при таймауте = %+v, хочу нулевой (пачка повторится)", cur.key)
	}
	if len(f.published()) != 0 {
		t.Fatalf("публикаций = %d, хочу 0", len(f.published()))
	}
}

// TestRenderLateSelectGate — тяжёлая выборка под общим гейтом сканов
// (db.ScanGate, T-271): занятый гейт держит выборку ДО Begin, отпускание
// пропускает её; взаимное исключение двух держателей — TestScanGate в
// internal/db.
func TestRenderLateSelectGate(t *testing.T) {
	f := newLateFixture(t)
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://x/", hash: f.spaHash[:]})
	gate := make(db.ScanGate, 1)
	if err := gate.Acquire(context.Background()); err != nil {
		t.Fatalf("занятие гейта тестом: %v", err)
	}
	opts := lateOpts()
	opts.Gate = gate

	done := make(chan error, 1)
	go func() {
		_, err := lateSelect(context.Background(), f.pool, opts, time.Now().Add(-testLateAfter), lateKey{})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("выборка прошла под занятым гейтом: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	f.pool.mu.Lock()
	begins := f.pool.begins
	f.pool.mu.Unlock()
	if begins != 0 {
		t.Fatalf("Begin под занятым гейтом: транзакций %d, хочу 0 (гейт до Begin)", begins)
	}
	gate.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("выборка после освобождения гейта: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("выборка не прошла после освобождения гейта")
	}
}

// TestRenderLateLostRowPublished — потерянная SPA-запись (старше окна,
// без бита, без сообщений) переиздаётся; не-SPA, свежая, отмаркированная,
// rendered, без контента — нет.
func TestRenderLateLostRowPublished(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "lost", 0, model.StatusAlive)
	f.pool.seedDomain(1, 20, "fresh", 0, model.StatusAlive)
	f.pool.seedDomain(1, 30, "marked", 0, model.StatusAlive)
	f.pool.seedDomain(1, 40, "rendered", 0, model.StatusAlive)
	f.pool.seedDomain(1, 50, "nocontent", 0, model.StatusAlive)
	f.pool.seedDomain(1, 60, "static", 0, model.StatusAlive)

	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://lost/", hash: f.spaHash[:]})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 20}, old: false, finalURL: "https://fresh/", hash: f.spaHash[:]})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 30}, old: true, finalURL: "https://marked/", hash: f.spaHash[:], marked: true})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 40}, old: true, finalURL: "https://rendered/", hash: f.spaHash[:], rendered: true})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 50}, old: true, finalURL: "https://nocontent/"})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 60}, old: true, finalURL: "https://static/", hash: f.rawHash[:]})

	cur := &RenderLateCursor{}
	res, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), cur)
	if err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	// Пачка — трое прошедших предикаты (потерянная SPA, без контента,
	// не-SPA): свежая/отмаркированная/rendered отсечены SQL; из пачки
	// издана одна, две пропущены вердиктом (нет тела / не-SPA).
	if res.Selected != 3 || res.Published != 1 || res.Skipped != 2 {
		t.Fatalf("итог %+v, хочу selected=3 published=1 skipped=2", res)
	}
	if !res.Wrapped || cur.key != (lateKey{}) {
		t.Fatalf("курсор после короткой пачки = %+v, wrapped=%v — хочу замыкание", cur.key, res.Wrapped)
	}
	pub := f.published()
	if len(pub) != 1 || pub[0].stream != queue.StreamRender {
		t.Fatalf("публикации %+v, хочу одну в %s", pub, queue.StreamRender)
	}
	want := queue.Result{ZoneID: 1, DomainID: 10, SubdomainID: 0, Variant: 0, Name: "lost.zone"}
	if pub[0].msg != want {
		t.Fatalf("сообщение %+v, хочу %+v", pub[0].msg, want)
	}
	if v := httpMetricValue(t, f.reg, "crawler_render_late_republish_total", nil); v != 1 {
		t.Fatalf("crawler_render_late_republish_total = %v, хочу 1", v)
	}
}

// TestRenderLateBitAndDeleted — домен под битом render_pending (долг —
// владение прохода доработки) и deleted-домен не переиздаются.
func TestRenderLateBitAndDeleted(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "debt", model.FlagRenderPending, model.StatusAlive)
	f.pool.seedDomain(1, 20, "gone", 0, model.StatusDeleted)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://debt/", hash: f.spaHash[:]})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 20}, old: true, finalURL: "https://gone/", hash: f.spaHash[:]})

	res, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), &RenderLateCursor{})
	if err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	if res.Selected != 0 || res.Published != 0 || res.Skipped != 0 {
		t.Fatalf("итог %+v, хочу пустой (бит и deleted исключены выборкой)", res)
	}
	if got := len(f.published()); got != 0 {
		t.Fatalf("публикаций %d, хочу 0", got)
	}
}

// TestRenderLateInterlock — запись с живым сообщением q:render ИЛИ
// q:render-fallback не переиздаётся (критерий приёмки ФТ-5.3); ошибка
// снимка — консервативный пропуск прохода (курсор на месте).
func TestRenderLateInterlock(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "live", 0, model.StatusAlive)
	f.pool.seedDomain(1, 20, "fb", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://live/", hash: f.spaHash[:]})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 20}, old: true, finalURL: "https://fb/", hash: f.spaHash[:]})
	f.pending.add(queue.StreamRender, content.RetentionPendingKey{Zone: 1, Domain: 10})
	f.pending.add(queue.StreamRenderFallback, content.RetentionPendingKey{Zone: 1, Domain: 20})

	cur := &RenderLateCursor{}
	res, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), cur)
	if err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	if res.Selected != 2 || res.Published != 0 || res.Skipped != 2 {
		t.Fatalf("итог %+v, хочу selected=2 published=0 skipped=2 (живые задачи не трогаются)", res)
	}
	if len(f.published()) != 0 {
		t.Fatalf("публикаций %d, хочу 0", len(f.published()))
	}
	// Пачка короче лимита — курсор замкнут (исключённые держателями
	// решаются сами, следующий цикл вернётся к ним с начала).
	if cur.key != (lateKey{}) || !res.Wrapped {
		t.Fatalf("курсор = %+v wrapped=%v, хочу замыкание после короткой пачки", cur.key, res.Wrapped)
	}

	// Ошибка снимка — ошибка прохода, курсор на месте (следующий тик
	// повторит), публикаций нет.
	f.pending.errOn[queue.StreamRender] = errors.New("xinfo: connection reset")
	cur2 := &RenderLateCursor{}
	if _, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), cur2); err == nil {
		t.Fatal("ошибка снимка не возвращена ошибкой прохода")
	}
	if cur2.key != (lateKey{}) {
		t.Fatalf("курсор при ошибке снимка = %+v, хочу нулевой", cur2.key)
	}
}

// TestRenderLateAlias — алиас наследует SPA-вердикт субдомена-оригинала
// (T-055): оригинал-кандидат с SPA-телом — алиас переиздаётся;
// нечитаемый контент оригинала и не-SPA оригинал — алиас пропускается.
func TestRenderLateAlias(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "alias", 0, model.StatusAlive)
	orig := int16(5)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 5}, old: true, finalURL: "https://orig/", hash: f.spaHash[:]})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 7}, old: true, finalURL: "https://alias/", aliasSub: &orig})

	res, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), &RenderLateCursor{})
	if err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	if res.Published != 2 || res.Skipped != 0 {
		t.Fatalf("итог %+v, хочу published=2 (оригинал + алиас)", res)
	}
	pub := f.published()
	if len(pub) != 2 || pub[0].msg.SubdomainID != 5 || pub[1].msg.SubdomainID != 7 {
		t.Fatalf("публикации %+v, хочу оригинал (sub=5) и алиас (sub=7) по порядку PK", pub)
	}

	// Не-SPA оригинал — алиас без вердикта: обе строки пропускаются.
	f.resetPublishes()
	f.pool.rows = nil
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 5}, old: true, finalURL: "https://orig/", hash: f.rawHash[:]})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 7}, old: true, finalURL: "https://alias/", aliasSub: &orig})
	res, err = LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), &RenderLateCursor{})
	if err != nil {
		t.Fatalf("LateOnce (не-SPA): %v", err)
	}
	if res.Published != 0 || res.Skipped != 2 {
		t.Fatalf("итог %+v, хочу published=0 skipped=2", res)
	}
}

// resetPublishes — сброс публикаций фикстуры (повторное использование).
func (f *lateFixture) resetPublishes() {
	f.q.mu.Lock()
	f.q.publishes = nil
	f.q.mu.Unlock()
}

// TestRenderLateCursorDiscipline — курсор двигается только за пачку,
// отработанную целиком: Full и ошибка публикации оставляют курсор
// (следующий проход повторит пачку, дубль ограничен); длинная пачка
// сохраняет курсор на последней строке (без замыкания).
func TestRenderLateCursorDiscipline(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://spa/", hash: f.spaHash[:]})

	// Full: очередь у порога при входе — БД не читается.
	f.lenner.seq = []int64{1000}
	rq, err := NewRenderQueue(f.q, f.lenner, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	cur := &RenderLateCursor{}
	res, err := LateOnce(ctx, f.pool, f.bodies, rq, f.pending, f.met, lateOpts(), cur)
	if err != nil {
		t.Fatalf("LateOnce Full: %v", err)
	}
	if !res.Full || res.Selected != 0 || cur.key != (lateKey{}) {
		t.Fatalf("итог %+v курсор %+v: хочу Full без выборки и без движения курсора", res, cur.key)
	}
	if f.pool.rowsCalls != 0 {
		t.Fatalf("выборок при полной очереди: %d, хочу 0", f.pool.rowsCalls)
	}

	// Ошибка публикации: курсор на месте, повторный проход переиздаёт
	// пачку (дубль ограничен, гасится идемпотентностью).
	f.lenner.seq = nil // ёмкость всегда есть
	f.q.errs = []error{errors.New("xadd: oops")}
	res, err = LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), cur)
	if err == nil {
		t.Fatal("ошибка публикации не возвращена")
	}
	if cur.key != (lateKey{}) {
		t.Fatalf("курсор при ошибке публикации = %+v, хочу нулевой", cur.key)
	}
	f.q.errs = nil
	res, err = LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), cur)
	if err != nil {
		t.Fatalf("повторный проход: %v", err)
	}
	if res.Published != 1 {
		t.Fatalf("повторный проход: published=%d, хочу 1 (пачка повторена)", res.Published)
	}

	// Длинная пачка (LIMIT сработал): курсор за последней строкой,
	// замыкания нет — следующая пачка продолжит с курсора.
	f.resetPublishes()
	f.pool.rows = nil
	for i := int64(1); i <= 3; i++ {
		f.pool.seedDomain(1, i, fmt.Sprintf("d%d", i), 0, model.StatusAlive)
		f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: i}, old: true, finalURL: "https://x/", hash: f.rawHash[:]})
	}
	cur = &RenderLateCursor{}
	opts := LateOptions{BatchSize: 2, After: testLateAfter}
	res, err = LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, opts, cur)
	if err != nil {
		t.Fatalf("проход с LIMIT=2: %v", err)
	}
	if res.Selected != 2 || res.Wrapped {
		t.Fatalf("итог %+v, хочу selected=2 без замыкания (пачка полная)", res)
	}
	if cur.key != (lateKey{zone: 1, domain: 2}) {
		t.Fatalf("курсор = %+v, хочу вторую строку", cur.key)
	}
	res, err = LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, opts, cur)
	if err != nil {
		t.Fatalf("вторая пачка: %v", err)
	}
	if res.Selected != 1 || !res.Wrapped {
		t.Fatalf("итог %+v, хочу selected=1 wrapped=true (хвост исчерпан)", res)
	}
	if cur.key != (lateKey{}) {
		t.Fatalf("курсор после замыкания = %+v, хочу нулевой", cur.key)
	}
}

// TestRenderLateMetricAndNil — метрика растёт числом изданий, nil-метрики
// не паникуют; Full/ошибка метрику не трогают.
func TestRenderLateMetricAndNil(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 0}, old: true, finalURL: "https://a/", hash: f.spaHash[:]})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 1}, old: true, finalURL: "https://b/", hash: f.spaHash[:]})

	if _, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), &RenderLateCursor{}); err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	if v := httpMetricValue(t, f.reg, "crawler_render_late_republish_total", nil); v != 2 {
		t.Fatalf("метрика = %v, хочу 2", v)
	}

	// nil-метрики — no-op без паники.
	f.pool.rows = nil
	f.pool.seedDomain(2, 10, "spa2", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 2, domain: 10}, old: true, finalURL: "https://c/", hash: f.spaHash[:]})
	if _, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, nil, lateOpts(), &RenderLateCursor{}); err != nil {
		t.Fatalf("LateOnce с nil-метриками: %v", err)
	}
	if v := httpMetricValue(t, f.reg, "crawler_render_late_republish_total", nil); v != 2 {
		t.Fatalf("метрика после nil-прохода = %v, хочу прежние 2", v)
	}
}

// TestRenderLateAliasRenderedOrigin — отрендеренный оригинал доказывает
// SPA-вердикт класса сам по себе (ревью T-259 P2-1): алиас с потерянным
// сообщением переиздаётся и тогда, когда оригинал уже rendered=true
// (контент оригинала — DOM, читаться не должен: хэш оригинала в
// хранилище фейка отсутствует — попытка чтения провалила бы проход).
func TestRenderLateAliasRenderedOrigin(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "done", 0, model.StatusAlive)
	orig := int16(5)
	var missing content.Hash // есть только в строке, в хранилище нет
	missing[0] = 0xEE
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 5}, old: true, finalURL: "https://orig/", hash: missing[:], rendered: true})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10, sub: 7}, old: true, finalURL: "https://alias/", aliasSub: &orig})

	res, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), &RenderLateCursor{})
	if err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	if res.Selected != 1 || res.Published != 1 || res.Skipped != 0 {
		t.Fatalf("итог %+v, хочу selected=1 published=1 (алиас наследует вердикт от rendered-оригинала)", res)
	}
	pub := f.published()
	if len(pub) != 1 || pub[0].msg.SubdomainID != 7 {
		t.Fatalf("публикации %+v, хочу алиас (sub=7)", pub)
	}
}

// TestRenderLateSecondCapacityFull — первая проверка ёмкости свободна,
// вторая (перед публикацией) — Full: публикаций нет, курсор на месте,
// следующая пачка повторит (ревью T-259 P3-8).
func TestRenderLateSecondCapacityFull(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://spa/", hash: f.spaHash[:]})
	// Глубина по вызовам: вход свободен (0), перед публикацией — полна
	// (1000 при watermark 100; дальше повторяется последнее значение).
	f.lenner.seq = []int64{0, 1000}
	rq, err := NewRenderQueue(f.q, f.lenner, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	cur := &RenderLateCursor{}
	res, err := LateOnce(ctx, f.pool, f.bodies, rq, f.pending, f.met, lateOpts(), cur)
	if err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	if !res.Full || res.Published != 0 || res.Selected != 1 {
		t.Fatalf("итог %+v, хочу full selected=1 published=0", res)
	}
	if cur.key != (lateKey{}) {
		t.Fatalf("курсор при Full = %+v, хочу нулевой (пачка повторится)", cur.key)
	}
	if n := len(f.published()); n != 0 {
		t.Fatalf("публикаций %d, хочу 0", n)
	}
}

// TestRenderLateTransientContentError — транзиентный сбой чтения тела:
// строка пропускается (Skipped, Warn, не ошибка прохода), соседние
// записи пачки издаются (ревью T-259 P3-8).
func TestRenderLateTransientContentError(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "ok", 0, model.StatusAlive)
	f.pool.seedDomain(1, 20, "sick", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://ok/", hash: f.spaHash2[:]})
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 20}, old: true, finalURL: "https://sick/", hash: f.spaHash[:]})
	f.bodies.errOn = map[content.Hash]error{f.spaHash: errors.New("pack: transient")}

	res, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), &RenderLateCursor{})
	if err != nil {
		t.Fatalf("LateOnce: %v (сбой чтения — не ошибка прохода)", err)
	}
	if res.Selected != 2 || res.Published != 1 || res.Skipped != 1 {
		t.Fatalf("итог %+v, хочу selected=2 published=1 skipped=1", res)
	}
	pub := f.published()
	if len(pub) != 1 || pub[0].msg.DomainID != 10 {
		t.Fatalf("публикации %+v, хочу здоровую строку (домен 10)", pub)
	}
}

// TestRenderLateValidation — швы прохода: nil-зависимости — ошибки.
func TestRenderLateValidation(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	rq := f.renderQ(t)
	cases := []struct {
		name string
		call func() error
	}{
		{"пул", func() error { _, err := LateOnce(ctx, nil, f.bodies, rq, f.pending, nil, lateOpts(), nil); return err }},
		{"контент", func() error { _, err := LateOnce(ctx, f.pool, nil, rq, f.pending, nil, lateOpts(), nil); return err }},
		{"очередь", func() error {
			_, err := LateOnce(ctx, f.pool, f.bodies, nil, f.pending, nil, lateOpts(), nil)
			return err
		}},
		{"снимок", func() error { _, err := LateOnce(ctx, f.pool, f.bodies, rq, nil, nil, lateOpts(), nil); return err }},
		{"цикл: пул", func() error { return RenderLateLoop(ctx, nil, f.bodies, rq, f.pending, nil, lateOpts()) }},
		{"цикл: снимок", func() error { return RenderLateLoop(ctx, f.pool, f.bodies, rq, nil, nil, lateOpts()) }},
	}
	for _, c := range cases {
		if err := c.call(); err == nil {
			t.Errorf("%s: nil-зависимость не отвергнута", c.name)
		}
	}
}

// TestRenderLateDefaults — дефолты опций (интервал/пачка/окно/таймаут/
// backoff) и состав стримов интерлока.
func TestRenderLateDefaults(t *testing.T) {
	o := LateOptions{}.withDefaults()
	if o.Interval != DefaultRenderLateInterval || o.BatchSize != DefaultRenderLateBatch || o.After != DefaultRenderLateAfter {
		t.Fatalf("дефолты %+v", o)
	}
	if o.SelectTimeout != DefaultRenderLateSelectTimeout {
		t.Errorf("SelectTimeout = %s, хочу %s (T-271)", o.SelectTimeout, DefaultRenderLateSelectTimeout)
	}
	if o.ErrorBackoff != DefaultRenderLateErrorBackoff {
		t.Errorf("ErrorBackoff = %s, хочу %s (T-271)", o.ErrorBackoff, DefaultRenderLateErrorBackoff)
	}
	if DefaultRenderLateInterval != time.Minute {
		t.Fatalf("интервал %s, хочу минуту (стиль прохода доработки)", DefaultRenderLateInterval)
	}
	if DefaultRenderLateAfter != 6*time.Hour {
		t.Fatalf("окно возраста %s, хочу 6 ч (ФТ-5.3)", DefaultRenderLateAfter)
	}
	if DefaultRenderLateSelectTimeout != 300*time.Second {
		t.Errorf("потолок выборки %s, хочу 300 с (T-273: дедлайн обязан давать catch-up-проходу завершиться и сдвинуть курсор)", DefaultRenderLateSelectTimeout)
	}
	if DefaultRenderLateErrorBackoff != 15*time.Minute {
		t.Errorf("backoff ошибки %s, хочу 15 мин (T-271)", DefaultRenderLateErrorBackoff)
	}
	want := []string{queue.StreamRender, queue.StreamRenderFallback}
	if len(lateRenderStreams) != 2 || lateRenderStreams[0] != want[0] || lateRenderStreams[1] != want[1] {
		t.Fatalf("стримы интерлока %v, хочу %v", lateRenderStreams, want)
	}
}

// TestRenderLateLoop — цикл: тикер выполняет проходы, курсор цикла живёт
// между тиками, интерлок собственных сообщений прекращает дубли, отмена
// ctx — чистый nil. Без гонки на таймингах (ревью T-259 P3-10):
// проверяется СТАБИЛИЗАЦИЯ числа публикаций после подстановки ключа в
// снимок, а не точное число (дубль до подстановки ограничен и валиден).
func TestRenderLateLoop(t *testing.T) {
	f := newLateFixture(t)
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://spa/", hash: f.spaHash[:]})

	const interval = 25 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RenderLateLoop(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met,
			LateOptions{Interval: interval, BatchSize: 100, After: testLateAfter})
	}()

	// Первая публикация — за щедрый таймаут (CI-задержки).
	deadline := time.Now().Add(10 * time.Second)
	for len(f.published()) == 0 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("цикл не опубликовал потерянную запись за 10 с")
		}
		time.Sleep(2 * time.Millisecond)
	}
	// Собственная публикация — живая задача: ключ в снимок, дальше
	// тики видеть её обязаны; дубль до этого момента ограничен.
	pub := f.published()[0].msg
	f.pending.add(queue.StreamRender, content.RetentionPendingKey{
		Zone: pub.ZoneID, Domain: pub.DomainID, Sub: pub.SubdomainID, Variant: int16(pub.Variant)})

	// Стабилизация: за 4+ тиков число публикаций не меняется.
	time.Sleep(4 * interval)
	n1 := len(f.published())
	time.Sleep(4 * interval)
	n2 := len(f.published())
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	if n1 < 1 {
		t.Fatalf("публикаций %d, хочу ≥1", n1)
	}
	if n2 != n1 {
		t.Fatalf("публикации растут после интерлока: %d → %d, хочу стабилизацию", n1, n2)
	}
}

// TestRenderLateLoop_Backoff — ошибка прохода переводит тикер в backoff
// (T-271): тик 10-мс интервала не перезапускает проход немедленно
// (инцидент 08–09.11.2026 — проход в только что поднятый после
// OOM-килла PostgreSQL); тик паузы повторяет проход, дублей нет
// (публикация не состоялась), Warn в журнале.
func TestRenderLateLoop_Backoff(t *testing.T) {
	f := newLateFixture(t)
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://spa/", hash: f.spaHash[:]})
	f.pool.rowsErr = errors.New("потеря соединения: unexpected EOF")

	var buf bytes.Buffer
	opts := LateOptions{
		Interval:     10 * time.Millisecond, // без backoff — ~10 проходов на 100 мс
		BatchSize:    100,
		After:        testLateAfter,
		ErrorBackoff: 250 * time.Millisecond, // пауза ошибочного тика
		Log:          slog.New(slog.NewTextHandler(&buf, nil)),
	}
	attempts := func() int {
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		return f.pool.begins // каждая попытка прохода открывает tx-оболочку выборки
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RenderLateLoop(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, opts) }()

	// Первый тик ошибся; следующие тики интервала не перезапускают
	// проход в пределах паузы backoff.
	time.Sleep(80 * time.Millisecond)
	if n := attempts(); n != 1 {
		cancel()
		t.Fatalf("попыток за паузу backoff = %d, хочу 1 (без backoff ~8)", n)
	}
	// Тик паузы повторяет проход (ошибка держится — пауза сохраняется).
	deadline := time.Now().Add(3 * time.Second)
	for attempts() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := attempts(); n < 2 {
		cancel()
		t.Fatalf("повтора после паузы нет: попыток %d, хочу ≥2", n)
	}
	if pub := len(f.published()); pub != 0 {
		cancel()
		t.Fatalf("публикаций = %d, хочу 0 (проходы ошибаются до издания)", pub)
	}
	// Лог читается только после остановки цикла: горутина цикла пишет в
	// тот же буфер (ревью T-271 P2-1 — bytes.Buffer не потокобезопасен).
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("лог = %q, хочу Warn ошибочного прохода", buf.String())
	}
}

// TestRenderLateLoop_BackoffEveryErrorResetsTicker — семантика backoff
// (T-273, главный диагностический пробел T-271): reset тикера после
// КАЖДОЙ ошибки, пауза отсчитывается от конца ошибочного прохода.
// До T-273 reset выполнялся только на первую ошибку (условие
// !backoff): якорем тикера оставался конец первого прохода, повторные
// ошибки шли паузой «backoff − длительность прохода» — каденс WARN-ов
// маскировал длительность (инцидент 09–10.11.2026: «ровно 15 мин»
// при 600-с проходах — реальная пауза 300 с). Проход здесь длится
// дольше тика интервала (rowsDelay 150 мс > interval 40 мс) и
// ошибается: старты проходов обязаны идти с паузой ≥ длительность
// прохода + backoff. Здесь же — метрика ошибок (T-273): счётчик
// crawler_render_late_errors_total растёт на каждом неудачном проходе.
func TestRenderLateLoop_BackoffEveryErrorResetsTicker(t *testing.T) {
	f := newLateFixture(t)
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://spa/", hash: f.spaHash[:]})
	f.pool.mu.Lock()
	f.pool.rowsErr = errors.New("httpworker: поздний переиздатель: выборка: timeout: context deadline exceeded")
	f.pool.rowsDelay = 150 * time.Millisecond
	f.pool.mu.Unlock()

	opts := LateOptions{
		Interval:     40 * time.Millisecond,
		BatchSize:    100,
		After:        testLateAfter,
		ErrorBackoff: 500 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RenderLateLoop(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, opts) }()

	// Три ЗАВЕРШЁННЫЕ ошибкой попытки за щедрый бюджет (пауза 650 мс:
	// 150 проход + 500 backoff) — метрика растёт по факту ошибки, по ней
	// и ждём: старт без ошибки (отмена в середине) не считается.
	deadline := time.Now().Add(5 * time.Second)
	var starts []time.Time
	for {
		f.pool.mu.Lock()
		starts = append([]time.Time(nil), f.pool.beginsAt...)
		f.pool.mu.Unlock()
		errCount := httpMetricValue(t, f.reg, "crawler_render_late_errors_total", nil)
		if (len(starts) >= 3 && errCount >= 3) || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
	if len(starts) < 3 {
		t.Fatalf("стартов прохода = %d, хочу ≥3", len(starts))
	}
	// Пауза между стартами ≥ проход (150 мс) + backoff (500 мс) − запас
	// на планировщик (50 мс). Прошлая семантика давала ровно backoff
	// между повторными ошибками — якорь тикера не переанкорился.
	const minGap = 150 + 500 - 50
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < minGap*time.Millisecond {
			t.Errorf("пауза между проходами %d→%d = %v, want ≥ %v (проход + backoff: reset после каждой ошибки)",
				i-1, i, gap, minGap*time.Millisecond)
		}
	}
	// Метрика ошибок (T-273): по одному инкременту на каждый неудачный
	// проход (цикл остановлен — значение финально; последний старт мог
	// быть прерван отменой, потому требование «≥3», а не равенство).
	if v := httpMetricValue(t, f.reg, "crawler_render_late_errors_total", nil); v < 3 {
		t.Errorf("crawler_render_late_errors_total = %v, хочу ≥3 (по одному на каждый неудачный проход)", v)
	}
}

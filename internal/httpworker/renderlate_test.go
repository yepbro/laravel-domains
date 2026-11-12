// Unit tests of the render late re-publisher (T-259, TZ-fsm spec §3 FR-5.3)
// over a fake pool/store/queue/stream snapshot — without a DB, Redis and
// the network (the renderq_test.go style, whose queue/store/depth fakes and
// pgx rows are reused). Checked: the exact selection SQL and parameters
// (the PK cursor, the age window, the bit/status/marker guards),
// the interlock by the snapshot of the unconsumed q:render/q:render-fallback (a live
// task is not re-published, a snapshot error — a conservative skip),
// the SPA verdict over the content and the alias inheritance, the cursor discipline
// (a whole chunk — the cursor forward, a short one — the wrap, Full/error —
// the cursor in place), the crawler_render_late_republish_total metric, the loop.
// The T-271 spec: the selection plan pinning's tx shell (SET LOCAL ×4 →
// SELECT → COMMIT), the SelectTimeout cap, the heavy scans' gate and
// the loop's backoff after an error. The behavior against live PostgreSQL/Redis —
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

// --- the fake unconsumed keys' source (the interlock) -----------------------

// latePending — a content.RetentionPending with preloaded sets
// per stream and a configurable failure.
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

// add is called from the test's goroutine while the loop reads the snapshots via
// PendingKeys — the sets access is strictly under the mutex (the race detector).
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

// --- the fake pool (db.DB): candidates/alias -----------------------------------

// lateSeedRow — a subdomain_results row of the fake.
type lateSeedRow struct {
	key      lateKey
	hash     []byte // nil — content_hash NULL
	aliasSub *int16 // not nil — an alias (inherits the original's verdict)
	rendered bool
	finalURL string // "" — NULL (the final_url IS NOT NULL filter)
	marked   bool   // the render_failed_at stamp (the IS NULL filter)
	old      bool   // checked_at older than the age window (the FR-5.3 filter)
}

// lateSeedDomain — a domains row of the fake.
type lateSeedDomain struct {
	zone   int16
	id     int64
	label  string
	flags  model.Flags
	status model.Status
}

// latePool — the pass's db.DB without a DB: the live semantics of the
// lateRenderRowsSQL/lateRenderAliasSQL filters (rendered, final_url, the marker,
// the age window, the render_pending bit, status <> deleted, the PK cursor,
// LIMIT; the alias query — the renderPendingRowsSQL predicates over the ANY
// subdomains); the SQL is matched EXACTLY (an unexpected one — an error, the test
// falls loudly, the renderqPool style). Since T-271 the pool is a RecordDB: the heavy selection
// goes in the plan pinning's transaction (Begin → lateTx), the alias query
// stays point through the pool.
type latePool struct {
	mu      sync.Mutex
	domains map[lateKey]*lateSeedDomain // key.domain — the identifier (sub/variant zero)
	rows    []lateSeedRow

	rowsErr   error
	aliasErr  error
	rowsCalls int
	aliasLast []int16 // the last alias query's subdomains
	begins    int
	txs       []*lateTx
	// rowsBlock — a "stuck" selection: an answer only by the ctx deadline
	// (the T-271 ctx timeout test).
	rowsBlock bool
	// rowsDelay — the heavy selection's delay (the backoff semantics
	// test of T-273: the pass lasts longer than the interval tick); beginsAt — the
	// Begin time stamps (the pass starts).
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

// Begin — the selection cap's transaction (T-271): the heavy chunk
// selection goes via lateTx, the point queries stay in the pool.
func (p *latePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	p.beginsAt = append(p.beginsAt, time.Now())
	tx := &lateTx{pool: p}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// lateTx — the selection cap's transaction: the only SET LOCAL —
// the statement_timeout before the SELECT (the plan pinnings removed by T-273), calls —
// the ordered operators' journal (the order contract, the
// retentionFakeTx.calls).
type lateTx struct {
	pgx.Tx // the pass does not use the other pgx.Tx methods

	pool      *latePool
	calls     []string
	committed bool
}

func (t *lateTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, sql)
	// Since T-273 the shell is only the operator's cap: the shape
	// pinnings (SET LOCAL enable_*) unrolled the join from domains at the production volume
	// (the eternal loop over the deadline) — an unexpected Exec falls loudly.
	if strings.HasPrefix(sql, "SET LOCAL statement_timeout = ") {
		return pgconn.NewCommandTag("SET"), nil
	}
	return pgconn.CommandTag{}, fmt.Errorf("lateTx: неожидаемый Exec %q", sql)
}

func (t *lateTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	// A "stuck" selection (the T-271 ctx timeout test): an answer only by
	// the deadline, before the journals and the semantics.
	if sql == lateRenderRowsSQL && t.pool.rowsBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	// The heavy selection's delay (the T-273 backoff semantics): the sleep outside
	// the mutex — the pool stays available to the test.
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

	// The deterministic PK order (ORDER BY zone, domain, sub, variant).
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
			continue // the row's predicates
		}
		d := p.domains[lateKey{zone: r.key.zone, domain: r.key.domain}]
		if d == nil {
			continue // the JOIN domains — no row
		}
		if d.flags.Test(model.FlagRenderPending) || d.status == model.StatusDeleted {
			continue // the render_pending bit / §13.1
		}
		if !lateKeyLess(cur, r.key) {
			continue // the row-value > cursor
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
		// The lateRenderAliasSQL predicate: the candidates (not rendered, there is
		// a target, no marker) PLUS the rendered ones (the fact of the render — the
		// class's verdict, the T-259 P2-1 review); the marked ones — no.
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

// lateKeyOf — the cursor from the SQL parameters (int16/int64/int16/int16).
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

// testLateAfter — the tests' age window (1 hour: old=true — "older than the window").
const testLateAfter = time.Hour

// lateFixture — the pass's assembly: the pool, the content (SPA/non-SPA bodies), the queue,
// the depth (free), the snapshot, the metrics.
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

// newLateFixture — the domain/zone are seeded separately by the test.
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
	// The SPA bodies (T-057) and the static one — the same as in the renderq fixtures;
	// the second SPA body — for multi-row scenarios with an independent
	// read failure (a body copy with a postfix — the verdict is deterministic). The hashes
	// differ by the senior byte: the bodies/failures maps' keys do not merge.
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

// published — the q:render publications' snapshot.
func (f *lateFixture) published() []renderqPublish { return f.q.snapshot() }

// lateOpts — the tests' options: a 1 h window, a chunk of 100.
func lateOpts() LateOptions { return LateOptions{BatchSize: 100, After: testLateAfter} }

// --- the selection -------------------------------------------------------------------

// TestRenderLateSelect — the exact selection SQL: a row-value > cursor over the PK,
// the ORDER BY PK, the LIMIT, the age boundary as a parameter; the predicates (rendered,
// final_url, the marker, the age, the render_pending bit, deleted) — the fake's
// live meaning is checked by the behavior below. The selection goes in the tx shell
// of the statement_timeout cap (T-271/T-273) — the operators' order
// is checked by TestRenderLateSelectShell.
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

	// The cursor: a row behind the cursor is not selected.
	cur.key = rows[0].key
	if rows, err = lateSelect(ctx, f.pool, lateOpts(), time.Now().Add(-testLateAfter), cur.key); err != nil || len(rows) != 0 {
		t.Fatalf("выборка за курсором: %d строк (err=%v), хочу 0", len(rows), err)
	}
}

// TestRenderLateSelectShell — the selection's tx shell (T-271 introduced
// the shape pinning, T-273 removed it): the chunk is read in an explicit
// transaction strictly by the SET LOCAL statement_timeout →
// SELECT → COMMIT sequence. The pinnings (SET LOCAL enable_seqscan/hashjoin/
// mergejoin/bitmapscan = off) are removed deliberately — at the production volume
// (domains 43M) the pin left the planner only the Nested Loop and
// unrolled the join from domains, a cost ≈157M at the 60 s deadline —
// the eternal loop of doomed selections (the Nov 9–10, 2026 incident); an unexpected
// enable_* in the journal falls loudly (the fake knows no such Exec).
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
	// The cap's text — a literal (the integration test's contract, the
	// renderq_test.go style); the value — the T-273 default of 300 s.
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

// TestRenderLateSelectCtxTimeout — a stuck selection is cut by the client
// ctx deadline SelectTimeout (T-271): a wrapped error with the
// "selection" context, the cursor in place — the chunk will repeat (idempotency, spec 3.4).
func TestRenderLateSelectCtxTimeout(t *testing.T) {
	f := newLateFixture(t)
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://x/", hash: f.spaHash[:]})
	f.pool.rowsBlock = true // the SELECT does not answer until the ctx cancellation

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

// TestRenderLateSelectGate — the heavy selection under the scans' common gate
// (db.ScanGate, T-271): a busy gate holds the selection BEFORE Begin,
// the release lets it through; the two holders' mutual exclusion — TestScanGate in
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

// TestRenderLateLostRowPublished — a lost SPA record (older than the window,
// without the bit, without messages) is re-published; a non-SPA, a fresh, a marked,
// a rendered one, without content — is not.
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
	// The chunk — three that passed the predicates (a lost SPA, without content,
	// a non-SPA): the fresh/marked/rendered ones are cut off by the SQL; of the chunk
	// one is published, two are skipped by the verdict (no body / non-SPA).
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

// TestRenderLateBitAndDeleted — a domain under the render_pending bit (the debt —
// the rework pass's ownership) and a deleted domain are not re-published.
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

// TestRenderLateInterlock — a record with a live q:render message OR
// q:render-fallback is not re-published (the FR-5.3 acceptance criterion); a snapshot
// error — a conservative pass skip (the cursor in place).
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
	// The chunk is shorter than the limit — the cursor is closed (the holders'
	// excluded ones decide themselves, the next cycle will come back to them from the start).
	if cur.key != (lateKey{}) || !res.Wrapped {
		t.Fatalf("курсор = %+v wrapped=%v, хочу замыкание после короткой пачки", cur.key, res.Wrapped)
	}

	// A snapshot error — a pass error, the cursor in place (the next tick
	// will repeat), no publications.
	f.pending.errOn[queue.StreamRender] = errors.New("xinfo: connection reset")
	cur2 := &RenderLateCursor{}
	if _, err := LateOnce(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, lateOpts(), cur2); err == nil {
		t.Fatal("ошибка снимка не возвращена ошибкой прохода")
	}
	if cur2.key != (lateKey{}) {
		t.Fatalf("курсор при ошибке снимка = %+v, хочу нулевой", cur2.key)
	}
}

// TestRenderLateAlias — an alias inherits the original subdomain's SPA verdict
// (T-055): an original candidate with an SPA body — the alias is re-published;
// an unreadable original content and a non-SPA original — the alias is skipped.
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

	// A non-SPA original — an alias without a verdict: both rows are skipped.
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

// resetPublishes — the fixture's publications reset (a reuse).
func (f *lateFixture) resetPublishes() {
	f.q.mu.Lock()
	f.q.publishes = nil
	f.q.mu.Unlock()
}

// TestRenderLateCursorDiscipline — the cursor moves only over a chunk
// processed entirely: Full and a publication error leave the cursor
// (the next pass will repeat the chunk, the duplicate is bounded); a long chunk
// keeps the cursor at the last row (without the wrap).
func TestRenderLateCursorDiscipline(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://spa/", hash: f.spaHash[:]})

	// Full: the queue is at the threshold at the entry — the DB is not read.
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

	// A publication error: the cursor in place, a repeated pass re-publishes
	// the chunk (the duplicate is bounded, extinguished by the idempotency).
	f.lenner.seq = nil // the capacity is always there
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

	// A long chunk (the LIMIT fired): the cursor behind the last row,
	// no wrap — the next chunk will continue from the cursor.
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

// TestRenderLateMetricAndNil — the metric grows by the editions count, nil metrics
// do not panic; Full/error do not touch the metric.
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

	// nil metrics — a no-op without a panic.
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

// TestRenderLateAliasRenderedOrigin — a rendered original proves
// the class's SPA verdict by itself (the T-259 P2-1 review): an alias with a lost
// message is re-published even when the original is already rendered=true
// (the original's content is a DOM, it must not be read: the original's hash is
// absent from the fake's store — a read attempt would fail the pass).
func TestRenderLateAliasRenderedOrigin(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "done", 0, model.StatusAlive)
	orig := int16(5)
	var missing content.Hash // it exists only in the row, not in the store
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

// TestRenderLateSecondCapacityFull — the first capacity check is free,
// the second one (before the publication) — Full: no publications, the cursor in place,
// the next chunk will repeat (the T-259 P3-8 review).
func TestRenderLateSecondCapacityFull(t *testing.T) {
	f := newLateFixture(t)
	ctx := context.Background()
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://spa/", hash: f.spaHash[:]})
	// The depth by calls: the entry is free (0), before the publication — full
	// (1000 at a watermark of 100; further the last value repeats).
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

// TestRenderLateTransientContentError — a transient body read failure:
// the row is skipped (Skipped, a Warn, not a pass error), the neighboring
// records of the chunk are published (the T-259 P3-8 review).
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

// TestRenderLateValidation — the pass's seams: nil dependencies — errors.
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

// TestRenderLateDefaults — the options' defaults (the interval/chunk/window/timeout/
// backoff) and the interlock streams' composition.
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

// TestRenderLateLoop — the loop: the ticker runs the passes, the loop's cursor lives
// between the ticks, the own messages' interlock stops the duplicates, the ctx
// cancellation — a clean nil. Without a race on the timings (the T-259 P3-10 review):
// the STABILIZATION of the publications count after the key's insertion into the
// snapshot is checked, not the exact count (a duplicate before the insertion is bounded and valid).
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

	// The first publication — within a generous timeout (the CI delays).
	deadline := time.Now().Add(10 * time.Second)
	for len(f.published()) == 0 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("цикл не опубликовал потерянную запись за 10 с")
		}
		time.Sleep(2 * time.Millisecond)
	}
	// The own publication — a live task: the key into the snapshot; further
	// the ticks must see it; a duplicate before this moment is bounded.
	pub := f.published()[0].msg
	f.pending.add(queue.StreamRender, content.RetentionPendingKey{
		Zone: pub.ZoneID, Domain: pub.DomainID, Sub: pub.SubdomainID, Variant: int16(pub.Variant)})

	// Stabilization: over 4+ ticks the publications count does not change.
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

// TestRenderLateLoop_Backoff — a pass error switches the ticker into the backoff
// (T-271): a tick of the 10 ms interval does not restart the pass immediately
// (the Nov 8–9, 2026 incident — a pass into a PostgreSQL just
// raised after an OOM kill); the pause's tick repeats the pass, no duplicates
// (the publication did not happen), a Warn in the journal.
func TestRenderLateLoop_Backoff(t *testing.T) {
	f := newLateFixture(t)
	f.pool.seedDomain(1, 10, "spa", 0, model.StatusAlive)
	f.pool.seedRow(lateSeedRow{key: lateKey{zone: 1, domain: 10}, old: true, finalURL: "https://spa/", hash: f.spaHash[:]})
	f.pool.rowsErr = errors.New("потеря соединения: unexpected EOF")

	var buf bytes.Buffer
	opts := LateOptions{
		Interval:     10 * time.Millisecond, // without the backoff — ~10 passes per 100 ms
		BatchSize:    100,
		After:        testLateAfter,
		ErrorBackoff: 250 * time.Millisecond, // the erroneous tick's pause
		Log:          slog.New(slog.NewTextHandler(&buf, nil)),
	}
	attempts := func() int {
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		return f.pool.begins // every pass attempt opens the selection's tx shell
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RenderLateLoop(ctx, f.pool, f.bodies, f.renderQ(t), f.pending, f.met, opts) }()

	// The first tick failed; the following interval ticks do not restart
	// the pass within the backoff pause.
	time.Sleep(80 * time.Millisecond)
	if n := attempts(); n != 1 {
		cancel()
		t.Fatalf("попыток за паузу backoff = %d, хочу 1 (без backoff ~8)", n)
	}
	// The pause's tick repeats the pass (the error persists — the pause is kept).
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
	// The log is read only after the loop's stop: the loop's goroutine writes into
	// the same buffer (the T-271 P2-1 review — bytes.Buffer is not concurrency-safe).
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

// TestRenderLateLoop_BackoffEveryErrorResetsTicker — the backoff
// semantics (T-273, the T-271 main diagnostic gap): the ticker's reset after
// EVERY error, the pause is counted from the erroneous pass's end.
// Before T-273 the reset ran only for the first error (the
// !backoff condition): the ticker's anchor stayed at the first pass's end, the repeated
// errors went with a "backoff − the pass's duration" pause — the WARN cadence
// masked the duration (the Nov 9–10, 2026 incident: "exactly 15 min"
// at 600 s passes — the real pause 300 s). The pass here lasts
// longer than the interval tick (rowsDelay 150 ms > interval 40 ms) and
// fails: the pass starts must go with a pause ≥ the
// pass's duration + the backoff. Here too — the errors metric (T-273): the
// crawler_render_late_errors_total counter grows per every failed pass.
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

	// Three attempts FINISHED with an error within a generous budget (a 650 ms pause:
	// 150 pass + 500 backoff) — the metric grows by the fact of the error, by it
	// we wait too: a start without an error (a cancellation mid-way) does not count.
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
	// The pause between the starts ≥ the pass (150 ms) + the backoff (500 ms) − headroom
	// for the scheduler (50 ms). The former semantics gave exactly the backoff
	// between the repeated errors — the ticker's anchor was not re-anchored.
	const minGap = 150 + 500 - 50
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < minGap*time.Millisecond {
			t.Errorf("пауза между проходами %d→%d = %v, want ≥ %v (проход + backoff: reset после каждой ошибки)",
				i-1, i, gap, minGap*time.Millisecond)
		}
	}
	// The errors metric (T-273): one increment per every failed
	// pass (the loop is stopped — the value is final; the last start could
	// have been interrupted by the cancellation, hence the "≥3" requirement, not an equality).
	if v := httpMetricValue(t, f.reg, "crawler_render_late_errors_total", nil); v < 3 {
		t.Errorf("crawler_render_late_errors_total = %v, хочу ≥3 (по одному на каждый неудачный проход)", v)
	}
}

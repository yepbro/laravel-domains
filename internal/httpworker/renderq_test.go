// Unit tests of the render queue's overflow (T-066; spec 5.6, §7) over fake
// pool/queue/store — without a DB, Redis and the network. Own fake pools
// with the renderq prefix (the task boundary: record_test.go T-058 is not touched):
// the pass's exact SQL and parameters are checked, the render_pending flag in the
// same result transaction, the "commit → q:render publication" order,
// the non-blocking publication (the XACK does not wait for the capacity), the repeated SPA verdict
// over the stored content, the aliases, errors/ctx cancellation, the pass's loop.
// The behavior against live PostgreSQL/Redis — integration tests.
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- the events' protocol --------------------------------------------------------

// renderqJournal — a concurrency-safe operations protocol (the recordJournal style).
type renderqJournal struct {
	mu     sync.Mutex
	events []string
	// done — a non-blocking signal per event of the kind (the
	// pass loop's synchronization in a goroutine).
	done chan string
}

func (j *renderqJournal) record(e string) {
	j.mu.Lock()
	j.events = append(j.events, e)
	j.mu.Unlock()
	if j.done != nil {
		select {
		case j.done <- e:
		default:
		}
	}
}

func (j *renderqJournal) indexOf(e string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, v := range j.events {
		if v == e {
			return i
		}
	}
	return -1
}

func (j *renderqJournal) count(e string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, v := range j.events {
		if v == e {
			n++
		}
	}
	return n
}

// snapshot — a copy of all the events (the steps' sequences check).
func (j *renderqJournal) snapshot() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]string, len(j.events))
	copy(out, j.events)
	return out
}

// --- the fake RenderLen and queue --------------------------------------------

// renderqLen — a RenderLen: the length by the calls counter (seq — the answers in
// order, further the last one repeats), a configurable failure.
type renderqLen struct {
	mu    sync.Mutex
	seq   []int64
	err   error
	calls int
}

func (l *renderqLen) Len(ctx context.Context, stream string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.err != nil {
		return 0, l.err
	}
	if stream != queue.StreamRender {
		return 0, fmt.Errorf("renderqLen: the stream %s, %s expected", stream, queue.StreamRender)
	}
	if len(l.seq) == 0 {
		return 0, nil
	}
	n := l.seq[0]
	if len(l.seq) > 1 {
		l.seq = l.seq[1:]
	}
	return n, nil
}

func (l *renderqLen) callsCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

// renderqPublish — a recorded publication.
type renderqPublish struct {
	stream string
	msg    queue.Result
}

// renderqQueue — a queue.Queue: records the Result publications (failures by
// index), Consume parks until the ctx cancellation, Ack — a stub.
type renderqQueue struct {
	j *renderqJournal

	errs []error

	mu        sync.Mutex
	publishes []renderqPublish
}

var _ queue.Queue = (*renderqQueue)(nil)

func (q *renderqQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	res, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("renderqQueue: an unexpected message %T (a queue.Result is needed)", msg)
	}
	q.mu.Lock()
	i := len(q.publishes)
	q.publishes = append(q.publishes, renderqPublish{stream: stream, msg: res})
	q.mu.Unlock()
	if q.j != nil {
		q.j.record("publish:" + stream)
	}
	if i < len(q.errs) && q.errs[i] != nil {
		return "", q.errs[i]
	}
	return fmt.Sprintf("entry-%s-%d", stream, i+1), nil
}

func (q *renderqQueue) Consume(ctx context.Context, _, _, _ string, _ int) ([]queue.Delivery, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *renderqQueue) Ack(context.Context, string, string, ...string) error { return nil }

func (q *renderqQueue) snapshot() []renderqPublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]renderqPublish, len(q.publishes))
	copy(out, q.publishes)
	return out
}

// renderqContent — a RenderContent: a body by hash, a configurable failure.
type renderqContent struct {
	mu     sync.Mutex
	bodies map[content.Hash][]byte
	errOn  map[content.Hash]error
}

func (c *renderqContent) Get(ctx context.Context, hash content.Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.errOn[hash]; err != nil {
		return nil, err
	}
	body, ok := c.bodies[hash]
	if !ok {
		return nil, fmt.Errorf("renderqContent: the block %s is missing", hash)
	}
	return body, nil
}

// --- the pass's fake pool (RecordDB): domains/zones/subdomain_results --------

// renderqDomKey — the key of a domains row in the pass's fake.
type renderqDomKey struct {
	zone int16
	id   int64
}

// renderqDomain — a domains row the pass reads and changes.
type renderqDomain struct {
	flags  model.Flags
	status model.Status
	label  string
}

// renderqRow — a subdomain_results row for the pass.
type renderqRow struct {
	subdomain int16
	variant   model.Variant
	hash      []byte // nil — content_hash NULL
	aliasSub  *int16 // nil — alias_subdomain_id NULL
	rendered  bool
	finalURL  string // "" — NULL (the final_url IS NOT NULL filter)
	marked    bool   // the "render impossible" stamp (T-210; the render_failed_at IS NULL filter)
}

// renderqPool — the pass's RecordDB without a DB: the live semantics of the SQL
// filters (the render_pending flag, status <> deleted, rendered = false,
// final_url IS NOT NULL, the T-184 zone stages — the zone name's membership in
// the render.requeue_deprioritized_zones list), the flag change with the bit
// guard and the debt counter crawler_counters (T-175: the render_pending row, ±1 in
// the clear transaction — as in the live RequeueOnce); the SQL is matched EXACTLY
// (an unexpected one — an error, the test falls loudly). A RecordDB, not a db.DB
// (T-175): the pass clears the flag by a transaction with the counter.
type renderqPool struct {
	j *renderqJournal

	zones   map[int16]string
	domains map[renderqDomKey]*renderqDomain
	rows    map[renderqDomKey][]renderqRow
	// order — the domains' insertion order: the "physical heap order" for
	// the selection without an ORDER BY (T-181) — the seq scan returns rows
	// by the layout, not the PK; the seed order determines the chunks in the tests.
	order    []renderqDomKey
	counters map[string]int64 // T-175: crawler_counters ('render_pending')

	queryErrs    map[string]error
	beforeHook   func() // called before the clear transaction's first Exec (a race simulation)
	counterNoRow bool   // the counter row is absent (the guard and an UPDATE 0 — the caller's error)

	mu           sync.Mutex
	queries      int
	clears       int
	stage1Select int   // the T-184 first stage's selections (the zones NOT from the list)
	stage2Select int   // the T-184 second stage's selections (the list's zones)
	clearTag     int64 // the flag clear's RowsAffected (0 — an honest answer)
}

var _ RecordDB = (*renderqPool)(nil)

func newRenderqPool() *renderqPool {
	return &renderqPool{
		j:         &renderqJournal{},
		zones:     make(map[int16]string),
		domains:   make(map[renderqDomKey]*renderqDomain),
		rows:      make(map[renderqDomKey][]renderqRow),
		counters:  map[string]int64{"render_pending": 0},
		queryErrs: make(map[string]error),
	}
}

func (p *renderqPool) seedDomain(zone int16, id int64, label string, flags model.Flags, status model.Status) *renderqDomain {
	d := &renderqDomain{flags: flags, status: status, label: label}
	p.domains[renderqDomKey{zone: zone, id: id}] = d
	p.order = append(p.order, renderqDomKey{zone: zone, id: id})
	// The counter invariant (T-175): it equals the number of rows with the bit — like
	// the migration 000015 backfill (without a status filter: a domain deleted with the
	// bit stays in the counter, only the rework pass removes the bit).
	if flags.Test(model.FlagRenderPending) {
		p.counters["render_pending"]++
	}
	return d
}

func (p *renderqPool) seedRow(zone int16, id int64, sub int16, variant model.Variant, hash []byte, aliasSub *int16, rendered bool, finalURL string) {
	p.rows[renderqDomKey{zone: zone, id: id}] = append(p.rows[renderqDomKey{zone: zone, id: id}],
		renderqRow{subdomain: sub, variant: variant, hash: hash, aliasSub: aliasSub, rendered: rendered, finalURL: finalURL})
}

func (p *renderqPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("renderqPool: CopyFrom must not be called")
}

func (p *renderqPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("renderqPool: Exec outside a transaction must not be called (the clearing is done by a transaction, T-175)")
}

// Begin — a snapshot of the domains and the counter (the renderqRecPool style): a rollback
// returns both, a commit keeps the changes.
func (p *renderqPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	snapDomains := make(map[renderqDomKey]*renderqDomain, len(p.domains))
	for k, v := range p.domains {
		c := *v
		snapDomains[k] = &c
	}
	snapCounters := make(map[string]int64, len(p.counters))
	for k, v := range p.counters {
		snapCounters[k] = v
	}
	p.j.record("begin")
	return &renderqTx{
		pool: p, j: p.j,
		snapDomains: snapDomains, snapCounters: snapCounters,
	}, nil
}

// renderqTx — the pass fake's transaction: the flag clear and the debt
// counter's shift (T-175), a rollback returns the snapshot.
type renderqTx struct {
	pgx.Tx

	pool         *renderqPool
	j            *renderqJournal
	snapDomains  map[renderqDomKey]*renderqDomain
	snapCounters map[string]int64
	committed    bool
}

func (t *renderqTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if err := t.pool.queryErrs[sql]; err != nil {
		return pgconn.CommandTag{}, err
	}
	switch sql {
	case renderPlanNoIndexScanSQL, renderPlanNoBitmapScanSQL, renderPlanNoIOSQL:
		// The selection plan pinning (T-181): SET LOCAL ×3 before the SELECT —
		// the sequence is checked by TestRequeueOnceSelectPlan.
		t.j.record("plan")
		return pgconn.NewCommandTag("SET"), nil
	case renderClearPendingSQL:
		if t.pool.beforeHook != nil {
			t.pool.beforeHook()
			t.pool.beforeHook = nil
		}
		if len(args) != 2 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqPool: flag clearing: %d parameters, 2 needed", len(args))
		}
		zone, okZ := args[0].(int16)
		id, okI := args[1].(int64)
		if !okZ || !okI {
			return pgconn.CommandTag{}, fmt.Errorf("renderqPool: flag clearing: the parameters %T/%T, int16/int64 needed", args[0], args[1])
		}
		t.pool.clears++
		d, ok := t.pool.domains[renderqDomKey{zone: zone, id: id}]
		if !ok || !d.flags.Test(model.FlagRenderPending) {
			return pgconn.NewCommandTag("UPDATE 0"), nil // the bit guard
		}
		d.flags.Clear(model.FlagRenderPending)
		t.j.record("clear")
		if t.pool.clearTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", t.pool.clearTag)), nil
		}
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case renderCounterDeltaSQL:
		if len(args) != 1 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqPool: counter: %d parameters, 1 needed", len(args))
		}
		delta, ok := args[0].(int64)
		if !ok {
			return pgconn.CommandTag{}, fmt.Errorf("renderqPool: counter: the parameter %T, int64 needed", args[0])
		}
		if t.pool.counterNoRow {
			return pgconn.NewCommandTag("UPDATE 0"), nil // no row (a manual edit)
		}
		t.pool.counters["render_pending"] += delta
		t.j.record("debtcount")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("renderqPool: an unexpected SQL Exec: %s", sql)
	}
}

func (t *renderqTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.committed = true
	t.j.record("commit")
	return nil
}

// Query — the domains chunk's selection INSIDE the plan pinning's
// transaction (T-181): renderSelectPending calls tx.Query, not the pool — the fake repeats
// the live call point (the pool does not read the chunk directly).
func (t *renderqTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if err := t.pool.queryErrs[sql]; err != nil {
		return nil, err
	}
	t.pool.queries++
	switch sql {
	case renderPendingDomainsSQL, renderPendingDomainsNotInSQL, renderPendingDomainsInSQL:
		if sql == renderPendingDomainsSQL && len(args) != 1 {
			return nil, fmt.Errorf("renderqPool: domains: %d parameters, 1 needed (LIMIT)", len(args))
		}
		if sql != renderPendingDomainsSQL && len(args) != 2 {
			return nil, fmt.Errorf("renderqPool: domains (the T-184 stage): %d parameters, 2 needed (LIMIT, zones)", len(args))
		}
		limit, ok := args[0].(int)
		if !ok || limit <= 0 {
			return nil, fmt.Errorf("renderqPool: domains: LIMIT %v (%T)", args[0], args[0])
		}
		// The T-184 stages: the second column — the zones' names (a text[] array).
		var zoneNames []string
		if sql != renderPendingDomainsSQL {
			zoneNames, ok = args[1].([]string)
			if !ok {
				return nil, fmt.Errorf("renderqPool: domains (the T-184 stage): the zones %T, []string needed", args[1])
			}
		}
		t.j.record("select")
		if sql == renderPendingDomainsNotInSQL {
			t.pool.stage1Select++
		}
		if sql == renderPendingDomainsInSQL {
			t.pool.stage2Select++
		}
		// The order — "physical" (order, the insertion order): the SQL without
		// an ORDER BY, the seq scan returns the heap's layout (T-181).
		rows := make([][]any, 0, limit)
		for _, k := range t.pool.order {
			if len(rows) == limit {
				break
			}
			d, ok := t.pool.domains[k]
			if !ok || !d.flags.Test(model.FlagRenderPending) || d.status == model.StatusDeleted {
				continue // the bit's WHERE and status <> deleted
			}
			// The T-184 stages: the domain's zone name's membership in the list
			// (z.name <> ALL / = ANY — the filter's live semantics).
			depr := false
			for _, name := range zoneNames {
				if t.pool.zones[k.zone] == name {
					depr = true
					break
				}
			}
			if depr != (sql == renderPendingDomainsInSQL) {
				continue
			}
			rows = append(rows, []any{k.zone, k.id, d.label + "." + t.pool.zones[k.zone]})
		}
		return renderqRowsOf(rows...), nil
	default:
		return nil, fmt.Errorf("renderqPool: an unexpected SQL Query in a transaction: %s", sql)
	}
}

func (t *renderqTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.committed {
		return nil
	}
	t.pool.domains, t.pool.counters = t.snapDomains, t.snapCounters
	return nil
}

func (p *renderqPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.queryErrs[sql]; err != nil {
		return nil, err
	}
	p.queries++
	switch sql {
	case renderDebtCounterSQL:
		// The pass's guard (T-181): a point read of the debt counter; the row
		// is mandatory (migration 000015) — its absence is emulated
		// by an empty set (renderDebtCount errors).
		if p.counterNoRow {
			return renderqRowsOf(), nil
		}
		return renderqRowsOf([]any{p.counters["render_pending"]}), nil
	case renderPendingRowsSQL:
		if len(args) != 2 {
			return nil, fmt.Errorf("renderqPool: records: %d parameters, 2 needed", len(args))
		}
		zone, okZ := args[0].(int16)
		id, okI := args[1].(int64)
		if !okZ || !okI {
			return nil, fmt.Errorf("renderqPool: records: the parameters %T/%T, int16/int64 needed", args[0], args[1])
		}
		src := p.rows[renderqDomKey{zone: zone, id: id}]
		rows := make([][]any, 0, len(src))
		for _, r := range src {
			if r.rendered || r.finalURL == "" || r.marked {
				continue // rendered = false AND final_url IS NOT NULL AND render_failed_at IS NULL (T-210)
			}
			var alias any
			if r.aliasSub != nil {
				sub := *r.aliasSub
				alias = &sub
			}
			var hash any
			if r.hash != nil {
				hash = r.hash
			}
			rows = append(rows, []any{r.subdomain, int16(r.variant), hash, alias})
		}
		// ORDER BY subdomain_id, variant (src is already in the insertion order —
		// the sort does not depend on it).
		for i := 0; i < len(rows); i++ {
			for j := i + 1; j < len(rows); j++ {
				a, b := rows[i], rows[j]
				if a[0].(int16) > b[0].(int16) || (a[0].(int16) == b[0].(int16) && a[1].(int16) > b[1].(int16)) {
					rows[i], rows[j] = rows[j], rows[i]
				}
			}
		}
		return renderqRowsOf(rows...), nil
	default:
		return nil, fmt.Errorf("renderqPool: an unexpected SQL Query: %s", sql)
	}
}

// renderqRows — pgx.Rows with preloaded rows (the Scan — positional,
// the types int16/int64/string/[]byte/*int16).
type renderqRows struct {
	rows [][]any
	i    int
}

func renderqRowsOf(rows ...[]any) *renderqRows { return &renderqRows{rows: rows} }

func (r *renderqRows) Close()                                       {}
func (r *renderqRows) Err() error                                   { return nil }
func (r *renderqRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *renderqRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *renderqRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *renderqRows) RawValues() [][]byte                          { return nil }
func (r *renderqRows) Conn() *pgx.Conn                              { return nil }
func (r *renderqRows) TypeMap() *pgtype.Map                         { return nil }

func (r *renderqRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *renderqRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("renderqRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("renderqRows: %d destinations, %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *int16:
			*dst = v.(int16)
		case *int64:
			*dst = v.(int64)
		case *string:
			*dst = v.(string)
		case *bool:
			*dst = v.(bool)
		case *[]byte:
			if v == nil {
				*dst = nil
			} else {
				*dst = v.([]byte)
			}
		case **int16:
			if v == nil {
				*dst = nil
			} else {
				sub := *v.(*int16)
				*dst = &sub
			}
		default:
			return fmt.Errorf("renderqRows: the destination %T is not supported", d)
		}
	}
	return nil
}

// --- the fake result write RecordDB (with the flag) ---------------------------

// renderqRecDomain — the write fake's domains row: like recordFakeDomain,
// plus flags (renderqRecPool applies renderSetPendingSQL).
type renderqRecDomain struct {
	status       model.Status
	checkState   model.CheckState
	queuedAt     *time.Time
	lastChecked  bool
	tempAttempts int16
	flags        model.Flags
}

// renderqRecPool — a RecordDB without a DB: the result write transaction's semantics
// (record.go) with the render_pending flag. The T-058 fake (record_test.go) does not
// track the flag and cannot be edited (the task boundary) — hence an own
// copy: domains (the status UPDATE, the flag's OR), subdomain_results (DELETE …
// RETURNING, the INSERT chunk), contents (the refcount, SwapRefTx T-044),
// snapshot transactions.
type renderqRecPool struct {
	j *renderqJournal

	domains  map[recordDomainKey]*renderqRecDomain
	rows     map[recordDomainKey][]recordFakeRow
	refc     map[content.Hash]int32
	counters map[string]int64 // T-175: crawler_counters ('render_pending')

	failExec     map[string]error
	counterNoRow bool // the counter row is absent (an UPDATE 0 — a write error)
	deleteErr    error
	commitErr    error
	beginErr     error

	mu sync.Mutex
}

var _ RecordDB = (*renderqRecPool)(nil)

func newRenderqRecPool() *renderqRecPool {
	return &renderqRecPool{
		j:        &renderqJournal{},
		domains:  make(map[recordDomainKey]*renderqRecDomain),
		rows:     make(map[recordDomainKey][]recordFakeRow),
		refc:     make(map[content.Hash]int32),
		counters: map[string]int64{"render_pending": 0}, // the migration 000015 row
		failExec: make(map[string]error),
	}
}

func (p *renderqRecPool) seedDomain(zone int16, id int64, status model.Status, flags model.Flags) *renderqRecDomain {
	d := &renderqRecDomain{status: status, checkState: model.CheckStateQueued, flags: flags, tempAttempts: 2}
	now := time.Now()
	d.queuedAt = &now
	p.domains[recordDomainKey{zone: zone, id: id}] = d
	// The counter invariant (T-175): it equals the number of rows with the bit (the
	// migration 000015 backfill).
	if flags.Test(model.FlagRenderPending) {
		p.counters["render_pending"]++
	}
	return d
}

func (p *renderqRecPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("renderqRecPool: CopyFrom must not be called")
}

func (p *renderqRecPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("renderqRecPool: Exec outside a transaction")
}

func (p *renderqRecPool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("renderqRecPool: Query outside a transaction")
}

func (p *renderqRecPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	snapDomains := make(map[recordDomainKey]*renderqRecDomain, len(p.domains))
	for k, v := range p.domains {
		c := *v
		snapDomains[k] = &c
	}
	snapRows := make(map[recordDomainKey][]recordFakeRow, len(p.rows))
	for k, v := range p.rows {
		snapRows[k] = append([]recordFakeRow(nil), v...)
	}
	snapRefc := make(map[content.Hash]int32, len(p.refc))
	for k, v := range p.refc {
		snapRefc[k] = v
	}
	snapCounters := make(map[string]int64, len(p.counters))
	for k, v := range p.counters {
		snapCounters[k] = v
	}
	p.j.record("begin")
	return &renderqRecTx{
		pool: p, j: p.j,
		snapDomains: snapDomains, snapRows: snapRows, snapRefc: snapRefc,
		snapCounters: snapCounters,
	}, nil
}

type renderqRecTx struct {
	pgx.Tx

	pool         *renderqRecPool
	j            *renderqJournal
	snapDomains  map[recordDomainKey]*renderqRecDomain
	snapRows     map[recordDomainKey][]recordFakeRow
	snapRefc     map[content.Hash]int32
	snapCounters map[string]int64
	committed    bool
}

func (t *renderqRecTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if err := t.pool.failExec[sql]; err != nil {
		return pgconn.CommandTag{}, err
	}
	switch sql {
	case recordDomainSQL:
		zone := args[0].(int16)
		id := args[1].(int64)
		status := model.Status(args[2].(int16))
		d, ok := t.pool.domains[recordDomainKey{zone: zone, id: id}]
		if !ok || d.status == model.StatusDeleted {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		d.status = status
		d.checkState = model.CheckStateDone
		d.queuedAt = nil
		d.lastChecked = true
		if status != model.StatusTempError {
			d.tempAttempts = 0
		}
		t.j.record("domains")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case renderSetPendingSQL:
		if len(args) != 2 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: flag: %d parameters, 2 needed", len(args))
		}
		zone, okZ := args[0].(int16)
		id, okI := args[1].(int64)
		if !okZ || !okI {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: flag: the parameters %T/%T", args[0], args[1])
		}
		d, ok := t.pool.domains[recordDomainKey{zone: zone, id: id}]
		if !ok {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		// The bit guard (T-175): RowsAffected — the 0→1 transition indicator,
		// a repeated setting (the flag is already set) does not count as a transition.
		if d.flags.Test(model.FlagRenderPending) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		d.flags.Set(model.FlagRenderPending)
		t.j.record("setflag")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case renderCounterDeltaSQL:
		if len(args) != 1 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: counter: %d parameters, 1 needed", len(args))
		}
		delta, ok := args[0].(int64)
		if !ok {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: counter: the parameter %T, int64 needed", args[0])
		}
		if t.pool.counterNoRow {
			return pgconn.NewCommandTag("UPDATE 0"), nil // no row (a manual edit)
		}
		t.pool.counters["render_pending"] += delta
		t.j.record("debtcount")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case recordZoneShiftSQL:
		// The zone counters' delta (T-178): the values do not matter to the fake —
		// the render debt scenario does not check them; the zone row
		// "exists" always.
		t.j.record("zoneshift")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case recordChecksBumpSQL:
		// The daily checks counter (T-179): the day name is computed by the DB —
		// the render scenarios do not need the value, the transaction must
		// pass the step (the upsert is always 1).
		t.j.record("checks")
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordTempErrBumpSQL:
		// The daily temp_error numerator (T-182): runs only at
		// temp_error episodes — the render scenarios do not need the value,
		// the transaction must pass the step.
		t.j.record("temp-err")
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordReanalyzeBumpSQL:
		// The unanalyzed tails bucket (T-204): the hour name is computed
		// by the DB — the render scenarios do not need the value, the transaction must
		// pass the step (the upsert is always 1).
		t.j.record("reanalyze")
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordResultsBumpSQL:
		// The hourly throughput bucket (T-203): the hour name is computed by the DB —
		// the render scenarios do not need the value, the transaction must
		// pass the step (the upsert is always 1).
		t.j.record("results")
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordInsertSQL:
		if len(args) != 18 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: INSERT: %d parameters, 18 needed", len(args))
		}
		zone := args[0].([]int16)
		domain := args[1].([]int64)
		sub := args[2].([]int16)
		variant := args[3].([]int16)
		scheme := args[4].([]int16)
		httpStatus := args[5].([]*int16)
		finalURL := args[6].([]*string)
		aliasKind := args[7].([]int16)
		aliasSub := args[8].([]*int16)
		addrs := args[9].([][]byte)
		cname := args[10].([]*string)
		tls := args[11].([]*string)
		redirects := args[12].([]*string)
		headers := args[13].([]*string)
		cookies := args[14].([]*string)
		hash := args[15].([][]byte)
		errCode := args[16].([]*int16)
		responseMS := args[17].([]*int32)

		n := len(zone)
		rows := make([]recordFakeRow, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, recordFakeRow{
				subdomainID: sub[i], variant: model.Variant(variant[i]),
				scheme: model.Scheme(scheme[i]), httpStatus: httpStatus[i],
				finalURL: finalURL[i], aliasKind: model.AliasKind(aliasKind[i]),
				aliasSub: aliasSub[i], addrs: addrs[i], cname: cname[i],
				tls: tls[i], redirects: redirects[i], headers: headers[i],
				cookies: cookies[i], rendered: false,
				contentHash: hash[i], errCode: errCode[i], responseMS: responseMS[i],
			})
		}
		key := recordDomainKey{zone: zone[0], id: domain[0]}
		t.pool.rows[key] = append(t.pool.rows[key], rows...)
		t.j.record("insert")
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", n)), nil
	default:
		if !strings.HasPrefix(sql, recordDelRefSQLPrefix) {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: an unexpected SQL: %s", sql)
		}
		var h content.Hash
		copy(h[:], args[0].([]byte))
		if t.pool.refc[h] <= 0 {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		t.pool.refc[h]--
		t.j.record("swap")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
}

// QueryRow — the domain row's states pre-read (recordSelectStateSQL,
// T-178): the values do not matter to the fake (the debt scenarios do not
// change the row's status), absent — ErrNoRows.
func (t *renderqRecTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != recordSelectStateSQL {
		return recordStateRow{err: fmt.Errorf("renderqRecPool: an unexpected QueryRow: %s", sql)}
	}
	key := recordDomainKey{zone: args[0].(int16), id: args[1].(int64)}
	d, ok := t.pool.domains[key]
	if !ok {
		return recordStateRow{err: pgx.ErrNoRows}
	}
	return recordStateRow{status: int16(d.status), state: int16(d.checkState)}
}

func (t *renderqRecTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != recordDeleteSQL {
		return nil, fmt.Errorf("renderqRecPool: an unexpected SQL Query: %s", sql)
	}
	if t.pool.deleteErr != nil {
		return nil, t.pool.deleteErr
	}
	key := recordDomainKey{zone: args[0].(int16), id: args[1].(int64)}
	old := t.pool.rows[key]
	delete(t.pool.rows, key)
	t.j.record("delete")

	out := make([][]any, len(old))
	for i, r := range old {
		if r.contentHash == nil {
			out[i] = []any{nil}
		} else {
			hash := make([]byte, len(r.contentHash))
			copy(hash, r.contentHash)
			out[i] = []any{hash}
		}
	}
	return recordFakeRowsOf(out...), nil
}

func (t *renderqRecTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.committed = true
	t.j.record("commit")
	return nil
}

func (t *renderqRecTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.committed {
		return nil
	}
	t.pool.domains, t.pool.rows, t.pool.refc = t.snapDomains, t.snapRows, t.snapRefc
	t.pool.counters = t.snapCounters
	return nil
}

// --- the fixtures -----------------------------------------------------------------

// renderqSPABody — an SPA shell (T-057: little visible text, a root
// container, a noscript with a JS requirement).
var renderqSPABody = []byte(`<html><head><title>x</title></head><body><div id="root"></div><noscript>please enable javascript</noscript></body></html>`)

// renderqFlatBody — an ordinary page: much visible text, no SPA signs.
var renderqFlatBody = []byte("<html><body><p>" + strings.Repeat("an ordinary page with visible text ", 30) + "</p></body></html>")

// renderqResult — a domain result with three subdomains: the apex — SPA
// (a q:render publication), www — an ordinary page (no publication), api —
// no verdict (nil). Returns the bodies' hashes for the store fake.
func renderqResult() (*DomainResult, content.Hash, content.Hash) {
	hSPA := content.HashOf(renderqSPABody)
	hFlat := content.HashOf(renderqFlatBody)
	spa := &httpfetch.SPAVerdict{SPA: true, Reasons: []httpfetch.SPAReason{httpfetch.SPAReasonRootContainer}}
	flat := &httpfetch.SPAVerdict{SPA: false}
	res := &DomainResult{
		ZoneID: 5, DomainID: 11, Name: "spa.example",
		Subdomains: []SubdomainResult{
			{
				SubdomainID: 0, Label: "", Hostname: "spa.example",
				Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")},
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 0, Label: "",
					FinalURL: "https://spa.example/", Host: "spa.example", Path: "/",
					HTTPStatus: 200, ContentHash: &hSPA, StoreContent: true,
					SPA: spa,
				}},
			},
			{
				SubdomainID: 1, Label: "www", Hostname: "www.spa.example",
				Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")},
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 1, Label: "www",
					FinalURL: "https://spa.example/", Host: "spa.example", Path: "/",
					HTTPStatus: 200, ContentHash: &hFlat, StoreContent: true,
					SPA: flat,
				}},
			},
			{
				SubdomainID: 2, Label: "api", Hostname: "api.spa.example",
				Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")},
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 2, Label: "api",
					FinalURL: "https://api.spa.example/", Host: "api.spa.example", Path: "/",
					HTTPStatus: 200,
					// SPA nil — a page without a verdict (a body load error).
				}},
			},
		},
	}
	return res, hSPA, hFlat
}

// renderqRecorder — the writer with a render queue over the fakes.
func renderqRecorder(t *testing.T, pool *renderqRecPool, q *renderqQueue, ln *renderqLen) *Recorder {
	t.Helper()
	rq, err := NewRenderQueue(q, ln, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	r, err := NewRenderRecorder(pool, q, rq, nil, nil)
	if err != nil {
		t.Fatalf("NewRenderRecorder: %v", err)
	}
	return r
}

// --- the pure logic -------------------------------------------------------------

// TestRenderSPAResults — only the SPA records' keys go into q:render
// (subdomain×variant), including an alias with its own verdict; nil and
// SPA=false do not.
func TestRenderSPAResults(t *testing.T) {
	spa := &httpfetch.SPAVerdict{SPA: true}
	aliasSPA := &httpfetch.SPAVerdict{SPA: true}
	flat := &httpfetch.SPAVerdict{SPA: false}
	res := &DomainResult{
		ZoneID: 5, DomainID: 11, Name: "spa.example",
		Subdomains: []SubdomainResult{
			{SubdomainID: 0, Records: []Record{
				{Variant: model.VariantMain, SubdomainID: 0, SPA: spa},
			}},
			{SubdomainID: 1, Records: []Record{
				{Variant: model.VariantMain, SubdomainID: 1, SPA: aliasSPA},
			}},
			{SubdomainID: 2, Records: []Record{
				{Variant: model.VariantMain, SubdomainID: 2, SPA: flat},
			}},
			{SubdomainID: 3, Records: []Record{
				{Variant: model.VariantHTTP, SubdomainID: 3}, // SPA nil
			}},
		},
	}
	keys := renderSPAResults(res)
	if len(keys) != 2 {
		t.Fatalf("%d keys, 2 expected (the SPA records)", len(keys))
	}
	for i, want := range []int16{0, 1} {
		if keys[i].SubdomainID != want || keys[i].Variant != model.VariantMain {
			t.Errorf("key %d: subdomain %d variant %v, %d/%v expected", i, keys[i].SubdomainID, keys[i].Variant, want, model.VariantMain)
		}
		if keys[i].ZoneID != 5 || keys[i].DomainID != 11 || keys[i].Name != "spa.example" {
			t.Errorf("key %d: the identity %+v", i, keys[i])
		}
	}

	empty := renderSPAResults(&DomainResult{ZoneID: 5, DomainID: 11, Name: "x"})
	if len(empty) != 0 {
		t.Fatalf("without records %d keys, 0 expected", len(empty))
	}
}

// TestRenderQueueHasCapacity — the capacity: strictly below the threshold (len ==
// the watermark — already an overflow, spec 4.2); a watermark <= 0 disables
// the check without a Redis call (the T-026 semantics).
func TestRenderQueueHasCapacity(t *testing.T) {
	q := &renderqQueue{}
	cases := []struct {
		wm   int64
		n    int64
		want bool
	}{
		{wm: 100, n: 0, want: true},
		{wm: 100, n: 99, want: true},
		{wm: 100, n: 100, want: false},
		{wm: 100, n: 1000, want: false},
		{wm: 1, n: 0, want: true},
	}
	for _, tc := range cases {
		ln := &renderqLen{seq: []int64{tc.n}}
		rq, err := NewRenderQueue(q, ln, tc.wm, nil)
		if err != nil {
			t.Fatalf("NewRenderQueue: %v", err)
		}
		got, err := rq.HasCapacity(context.Background())
		if err != nil {
			t.Fatalf("wm=%d n=%d: %v", tc.wm, tc.n, err)
		}
		if got != tc.want {
			t.Errorf("wm=%d n=%d: the capacity %v, %v expected", tc.wm, tc.n, got, tc.want)
		}
	}

	// A disabled gate — without an XLEN.
	ln := &renderqLen{}
	rq, err := NewRenderQueue(q, ln, 0, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	got, err := rq.HasCapacity(context.Background())
	if err != nil || !got {
		t.Fatalf("watermark 0: %v/%v, true/nil expected", got, err)
	}
	if n := ln.callsCount(); n != 0 {
		t.Fatalf("watermark 0: %d XLEN calls, 0 expected", n)
	}

	// An XLEN error — an error to the caller (the decision — at the Recorder/the pass).
	lnErr := &renderqLen{err: errors.New("redis down")}
	rq, err = NewRenderQueue(q, lnErr, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	if _, err := rq.HasCapacity(context.Background()); err == nil {
		t.Fatal("the XLEN error is not returned")
	}
}

// TestRenderQueuePublish — the publication goes into q:render without a
// capacity check; a failure is returned to the caller.
func TestRenderQueuePublish(t *testing.T) {
	q := &renderqQueue{}
	rq, err := NewRenderQueue(q, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	msgs := []queue.Result{
		{ZoneID: 5, DomainID: 11, SubdomainID: 0, Variant: model.VariantMain, Name: "a.example"},
		{ZoneID: 5, DomainID: 11, SubdomainID: 1, Variant: model.VariantMain, Name: "a.example"},
	}
	if err := rq.Publish(context.Background(), msgs); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	pub := q.snapshot()
	if len(pub) != 2 {
		t.Fatalf("%d publications, 2 expected", len(pub))
	}
	for i, p := range pub {
		if p.stream != queue.StreamRender {
			t.Errorf("publication %d: the stream %s, %s expected", i, p.stream, queue.StreamRender)
		}
		if p.msg.SubdomainID != msgs[i].SubdomainID {
			t.Errorf("publication %d: the message %+v", i, p.msg)
		}
	}

	qFail := &renderqQueue{errs: []error{nil, errors.New("xadd failed")}}
	rqFail, err := NewRenderQueue(qFail, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	if err := rqFail.Publish(context.Background(), msgs); err == nil {
		t.Fatal("the XADD failure is not returned")
	}
	if len(qFail.snapshot()) != 2 {
		t.Fatalf("before the failure %d of 2 published", len(qFail.snapshot())-1)
	}
}

// TestNewRenderQueueValidation — nil dependencies — an error (a silent
// publications loss is invisible to the operator).
func TestNewRenderQueueValidation(t *testing.T) {
	if _, err := NewRenderQueue(nil, &renderqLen{}, 100, nil); err == nil {
		t.Fatal("a nil queue: an error expected")
	}
	if _, err := NewRenderQueue(&renderqQueue{}, nil, 100, nil); err == nil {
		t.Fatal("a nil length source: an error expected")
	}
}

// --- the Recorder + the render pipeline ----------------------------------------------

// TestRecorderRenderPublishes — a free queue: q:render is published
// over the SPA records AFTER the transaction's commit; the flag is not set; q:analyze —
// over all the records (the T-058 contract unchanged).
func TestRecorderRenderPublishes(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, model.FlagIDN)
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{0}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}

	pub := q.snapshot()
	var analyze, render int
	for _, p := range pub {
		switch p.stream {
		case queue.StreamAnalyze:
			analyze++
		case queue.StreamRender:
			render++
			if p.msg.SubdomainID != 0 {
				t.Errorf("q:render: subdomain %d, only the SPA apex 0 expected", p.msg.SubdomainID)
			}
		default:
			t.Errorf("a publication into an unexpected stream %s", p.stream)
		}
	}
	if analyze != 3 || render != 1 {
		t.Fatalf("publications q:analyze=%d q:render=%d, 3/1 expected", analyze, render)
	}

	// The q:render publication — after the commit; the flag is not set.
	if i := pool.j.indexOf("commit"); i < 0 || pool.j.indexOf("publish:"+queue.StreamRender) < i {
		t.Fatalf("the order is violated: commit=%d publish:render=%d", pool.j.indexOf("commit"), pool.j.indexOf("publish:"+queue.StreamRender))
	}
	if c := pool.j.count("setflag"); c != 0 {
		t.Fatalf("the flag was set %d times with a free queue", c)
	}
	if f := pool.domains[recordDomainKey{zone: 5, id: 11}].flags; f != model.FlagIDN {
		t.Fatalf("flags=%s, only is_idn expected", f)
	}
}

// TestRecorderRenderOverflowFlags — a q:render overflow: no
// q:render publications, the render_pending flag is set by THE SAME transaction (between
// the status UPDATE and the commit), the episode finishes successfully — the XACK is not
// delayed (§5.6); q:analyze is published as usual.
func TestRecorderRenderOverflowFlags(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, model.FlagIDN)
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}} // len == watermark — an overflow
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v (the render overflow does not delay the episode)", ok, err)
	}

	if c := pool.j.count("publish:" + queue.StreamRender); c != 0 {
		t.Fatalf("q:render publications at the overflow: %d", c)
	}
	if c := pool.j.count("publish:" + queue.StreamAnalyze); c != 3 {
		t.Fatalf("%d q:analyze publications, 3 expected", c)
	}

	// The flag — inside the transaction: after the status UPDATE, before the commit.
	// The counter's shift — the last operator before the Commit (T-175: the
	// counter row's lock window is minimal), +1 exactly on the transition.
	dom, set, commit := pool.j.indexOf("domains"), pool.j.indexOf("setflag"), pool.j.indexOf("commit")
	if dom < 0 || set < dom || commit < set {
		t.Fatalf("the order is violated: domains=%d setflag=%d commit=%d", dom, set, commit)
	}
	insert, debt := pool.j.indexOf("insert"), pool.j.indexOf("debtcount")
	if insert < set || debt < insert || commit < debt {
		t.Fatalf("the order is violated: setflag=%d insert=%d debtcount=%d commit=%d (the counter shift — after the INSERT, before the Commit)",
			set, insert, debt, commit)
	}
	if got := pool.counters["render_pending"]; got != 1 {
		t.Fatalf("the debt counter = %d, 1 expected (exactly one transition)", got)
	}
	f := pool.domains[recordDomainKey{zone: 5, id: 11}].flags
	if !f.Test(model.FlagRenderPending) || !f.Test(model.FlagIDN) {
		t.Fatalf("flags=%s, is_idn|render_pending expected", f)
	}
}

// TestRecorderRenderLenError — an unavailable XLEN is treated as
// an overflow: the flag saves the render tasks, the episode does not fall.
func TestRecorderRenderLenError(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{err: errors.New("redis: xlen")}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}
	if pool.j.count("setflag") != 1 {
		t.Fatalf("the flag is not set at an XLEN error")
	}
	if pool.j.count("publish:"+queue.StreamRender) != 0 {
		t.Fatalf("q:render publications at an XLEN error: %d", pool.j.count("publish:"+queue.StreamRender))
	}
	if !pool.domains[recordDomainKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the render_pending flag is not set")
	}
}

// TestRecorderRenderNoSPA — without SPA records the capacity is not checked,
// the flag is not set and not erased (the rework pass will clear the draft flag).
func TestRecorderRenderNoSPA(t *testing.T) {
	pool := newRenderqRecPool()
	dom := pool.seedDomain(5, 11, model.StatusNew, model.FlagRenderPending) // a draft flag
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{}
	res, _, _ := renderqResult()
	for i := range res.Subdomains {
		for j := range res.Subdomains[i].Records {
			res.Subdomains[i].Records[j].SPA = &httpfetch.SPAVerdict{SPA: false}
		}
	}

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}
	if n := ln.callsCount(); n != 0 {
		t.Fatalf("the capacity was checked %d times without SPA records", n)
	}
	if pool.j.count("setflag") != 0 || pool.j.count("publish:"+queue.StreamRender) != 0 {
		t.Fatal("without SPA records there must be neither a flag nor q:render")
	}
	if !dom.flags.Test(model.FlagRenderPending) {
		t.Fatal("the pre-set flag was wiped by the result write — the pass is in charge of that")
	}
}

// TestRecorderWithoutRender — a NewRecorder without a render queue (render
// nil): the T-058 behavior unchanged (the builds' backward compatibility).
func TestRecorderWithoutRender(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	q := &renderqQueue{j: pool.j}
	res, _, _ := renderqResult()

	r, err := NewRecorder(pool, q, nil, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	ok, err := r.RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}
	if pool.j.count("publish:"+queue.StreamAnalyze) != 3 {
		t.Fatalf("q:analyze: %d, 3 expected", pool.j.count("publish:"+queue.StreamAnalyze))
	}
	if pool.j.count("publish:"+queue.StreamRender) != 0 || pool.j.count("setflag") != 0 {
		t.Fatal("without a render queue there must be neither q:render nor a flag")
	}
}

// TestRecorderRenderDeletedDomain — a deleted domain gets no
// result written (§8/§13.1): neither the flag, nor publications.
func TestRecorderRenderDeletedDomain(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusDeleted, 0)
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}} // an overflow — the flag is not set anyway
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, false/nil expected", ok, err)
	}
	if pool.j.count("setflag") != 0 || pool.j.count("publish:"+queue.StreamAnalyze) != 0 {
		t.Fatal("for a deleted domain — neither a flag nor publications")
	}
}

// TestRecorderRenderSetFlagFailure — a flag operator failure rolls back
// the transaction entirely: no rows and no flag, the delivery will return.
func TestRecorderRenderSetFlagFailure(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	pool.failExec[renderSetPendingSQL] = errors.New("sql: flag")
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err == nil || ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, an error expected", ok, err)
	}
	if c := pool.j.count("commit"); c != 0 {
		t.Fatalf("the transaction was committed at a flag failure (%d)", c)
	}
	dom := pool.domains[recordDomainKey{zone: 5, id: 11}]
	if dom.flags != 0 {
		t.Fatalf("flags=%s — the rollback did not restore the original state", dom.flags)
	}
	if len(pool.rows[recordDomainKey{zone: 5, id: 11}]) != 0 {
		t.Fatal("rows inserted with a rolled-back transaction")
	}
}

// TestRecorderRenderFlagAlreadySet — the bit guard in the SET (T-175): a repeated
// episode delivery with the flag already set — RowsAffected 0 without an error
// (the OR semantics is idempotent), the debt counter is not incremented: only
// the real 0→1 transitions.
func TestRecorderRenderFlagAlreadySet(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, model.FlagRenderPending) // the bit is already set
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}} // an overflow — the flag is set again
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v (setting the bit again is not an error)", ok, err)
	}
	if c := pool.j.count("setflag"); c != 0 {
		t.Fatalf("%d setflag transitions, 0 expected (the bit was already set)", c)
	}
	if c := pool.j.count("debtcount"); c != 0 {
		t.Fatalf("%d counter shifts, 0 expected (there was no transition)", c)
	}
	if got := pool.counters["render_pending"]; got != 1 {
		t.Fatalf("the counter = %d, 1 expected (the seed only)", got)
	}
}

// TestRecorderRenderCounterAtomic — the debt counter in THE SAME transaction
// as the flag (T-175, §3.4): a counter shift failure rolls back the transaction
// entirely — no rows, no flag, no counter.
func TestRecorderRenderCounterAtomic(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	pool.failExec[renderCounterDeltaSQL] = errors.New("sql: counter")
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err == nil || ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, an error expected (the counter must commit with the flag)", ok, err)
	}
	if c := pool.j.count("commit"); c != 0 {
		t.Fatalf("the transaction was committed at a counter failure (%d)", c)
	}
	dom := pool.domains[recordDomainKey{zone: 5, id: 11}]
	if dom.flags != 0 {
		t.Fatalf("flags=%s — the rollback did not restore the bit", dom.flags)
	}
	if got := pool.counters["render_pending"]; got != 0 {
		t.Fatalf("the counter = %d — the rollback did not restore the counter", got)
	}
	if len(pool.rows[recordDomainKey{zone: 5, id: 11}]) != 0 {
		t.Fatal("rows inserted with a rolled-back transaction")
	}
}

// TestRecorderRenderCounterMissingRow — there is no 'render_pending' row in
// crawler_counters (possible only by a manual edit, runbooks §6):
// the error rolls back the transaction — a silent debt counter
// desync is unacceptable, the gauge must not lie.
func TestRecorderRenderCounterMissingRow(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	pool.counterNoRow = true
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err == nil || ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, a missing-counter-row error expected", ok, err)
	}
	if c := pool.j.count("commit"); c != 0 {
		t.Fatalf("the transaction was committed without a counter row (%d)", c)
	}
	if pool.domains[recordDomainKey{zone: 5, id: 11}].flags != 0 {
		t.Fatal("the flag is set without the counter — a desync")
	}
}

// TestRecorderRenderPublishFailure — a q:render publication failure (after
// the commit): the error returns the delivery — a repeated episode will replay
// the decision (the capacity will be checked anew), the duplicates are bounded by the attempts.
func TestRecorderRenderPublishFailure(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	q := &renderqQueue{j: pool.j, errs: []error{nil, nil, nil, errors.New("xadd")}}
	ln := &renderqLen{seq: []int64{0}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err == nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, an error expected with a committed result", ok, err)
	}
	if pool.j.count("commit") != 1 {
		t.Fatal("the result must be committed before the publication")
	}
	if pool.j.count("setflag") != 0 {
		t.Fatal("the flag is set only at an overflow")
	}
}

// --- the rework pass ---------------------------------------------------------

// renderqPassKit — the pass's fakes set: the pool, the content, the render queue.
func renderqPassKit(t *testing.T, lens ...int64) (*renderqPool, *renderqContent, *renderqQueue, *renderqLen, *RenderQueue) {
	t.Helper()
	pool := newRenderqPool()
	pool.zones[5] = "example"
	body := &renderqContent{bodies: make(map[content.Hash][]byte), errOn: make(map[content.Hash]error)}
	q := &renderqQueue{j: pool.j}
	var ln *renderqLen
	if len(lens) > 0 {
		ln = &renderqLen{seq: lens}
	} else {
		ln = &renderqLen{}
	}
	rq, err := NewRenderQueue(q, ln, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	return pool, body, q, ln, rq
}

// TestRequeueOncePublishes — the pass reworks: an SPA content → the
// record's key; an alias inherits the original's verdict; an ordinary page and the
// records without content are not re-published; the rendered ones are excluded; the flag is
// cleared after the publication; is_idn is preserved.
func TestRequeueOncePublishes(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	hFlat := content.HashOf(renderqFlatBody)
	pool, body, q, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	body.bodies[hFlat] = renderqFlatBody

	pool.seedDomain(5, 11, "spa", model.FlagRenderPending|model.FlagIDN, model.StatusAlive)
	aliasTo := int16(0)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.seedRow(5, 11, 1, model.VariantMain, nil, &aliasTo, false, "https://spa.example/")
	pool.seedRow(5, 11, 2, model.VariantMain, hFlat[:], nil, false, "https://spa.example/")
	pool.seedRow(5, 11, 3, model.VariantMain, nil, nil, false, "https://spa.example/") // the content is not stored
	pool.seedRow(5, 11, 4, model.VariantMain, hSPA[:], nil, true, "https://spa.example/")
	// A marked SPA record (T-210 — a finalization without a render):
	// not taken by the pass's selection, not re-published (like rendered/flat).
	pool.seedRow(5, 11, 5, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.rows[renderqDomKey{zone: 5, id: 11}][5].marked = true

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Full || res.Selected != 1 || res.Cleared != 1 || res.Published != 2 {
		t.Fatalf("the outcome %+v, selected=1 cleared=1 published=2 full=false expected", res)
	}

	pub := q.snapshot()
	if len(pub) != 2 {
		t.Fatalf("%d publications, 2 expected (a record + an alias)", len(pub))
	}
	for i, want := range []int16{0, 1} {
		p := pub[i]
		if p.stream != queue.StreamRender {
			t.Errorf("publication %d: the stream %s", i, p.stream)
		}
		if p.msg.SubdomainID != want || p.msg.Variant != model.VariantMain {
			t.Errorf("publication %d: the key (%d,%v), (%d,%v) expected", i, p.msg.SubdomainID, p.msg.Variant, want, model.VariantMain)
		}
		if p.msg.ZoneID != 5 || p.msg.DomainID != 11 || p.msg.Name != "spa.example" {
			t.Errorf("publication %d: the identity %+v", i, p.msg)
		}
	}

	dom := pool.domains[renderqDomKey{zone: 5, id: 11}]
	if dom.flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is not cleared")
	}
	if !dom.flags.Test(model.FlagIDN) {
		t.Fatalf("flags=%s — is_idn lost at the clearing", dom.flags)
	}
}

// TestRequeueOnceFullAtStart — a full queue: the pass does not read the DB
// (one XLEN), does not touch the flags.
func TestRequeueOnceFullAtStart(t *testing.T) {
	pool, body, q, ln, rq := renderqPassKit(t, 100) // len == watermark
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if !res.Full || res.Selected != 0 {
		t.Fatalf("the outcome %+v, full=true selected=0 expected", res)
	}
	if ln.callsCount() != 1 {
		t.Fatalf("%d XLEN calls, 1 expected", ln.callsCount())
	}
	if len(q.snapshot()) != 0 {
		t.Fatal("at a full queue there must be no publications")
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is cleared at a full queue")
	}
}

// TestRequeueOnceFullMidPass — the queue filled between the domains:
// the processed ones are reworked, the rest kept the flag.
func TestRequeueOnceFullMidPass(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, q, _, rq := renderqPassKit(t, 0, 0, 100) // the start is free, after the first domain — full
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.seedDomain(5, 12, "two", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 12, 0, model.VariantMain, hSPA[:], nil, false, "https://two.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if !res.Full || res.Selected != 2 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("the outcome %+v, full selected=2 cleared=1 published=1 expected", res)
	}
	if n := len(q.snapshot()); n != 1 {
		t.Fatalf("%d publications, 1 expected (only the first domain)", n)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 12}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the second domain flag is cleared at a full queue")
	}
}

// TestRequeueOnceNoSPA — no SPA records found: no publications, the flag
// cleared (a draft flag).
func TestRequeueOnceNoSPA(t *testing.T) {
	hFlat := content.HashOf(renderqFlatBody)
	pool, body, q, _, rq := renderqPassKit(t, 0)
	body.bodies[hFlat] = renderqFlatBody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hFlat[:], nil, false, "https://spa.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Published != 0 || res.Cleared != 1 {
		t.Fatalf("the outcome %+v, published=0 cleared=1 expected", res)
	}
	if len(q.snapshot()) != 0 {
		t.Fatal("there must be no publications without SPA records")
	}
}

// TestRequeueOnceRenderFailedMarker — the "render impossible" marker (T-210):
// a SPA record marked by a finalization is not taken by the pass's selection —
// a repeated episode of the broken host does not re-publish it (no publications, the
// flag cleared without a publication, like a draft's); a repeated pass after a new
// episode (the row's rewrite reset the marker) re-publishes the record again.
func TestRequeueOnceRenderFailedMarker(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, q, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.rows[renderqDomKey{zone: 5, id: 11}][0].marked = true // a T-148/T-159/T-172 finalization

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Published != 0 || res.Cleared != 1 {
		t.Fatalf("the outcome %+v, published=0 cleared=1 expected (a marked record is not republished)", res)
	}
	if len(q.snapshot()) != 0 {
		t.Fatal("there must be no publications for a marked record (T-210)")
	}

	// A new episode: the row's rewrite (DELETE + INSERT, T-058) reset
	// the marker, the new episode's result write set the overflow
	// flag again (the bit+counter pair — like seedDomain, without a duplicate in order).
	pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Set(model.FlagRenderPending)
	pool.counters["render_pending"] = 1
	pool.rows[renderqDomKey{zone: 5, id: 11}][0].marked = false

	res, err = RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce after the marker reset: %v", err)
	}
	if res.Published != 1 || res.Cleared != 1 {
		t.Fatalf("the outcome %+v, published=1 cleared=1 expected (a reset marker returns the record into the selection)", res)
	}
}

// TestRequeueOnceContentError — an unavailable block: the record is skipped
// (a Warn), the pass continues without an error, but the domain's flag is PRESERVED:
// a transient read failure must not lose the SPA task before the
// T-030 recheck — the pass's next tick will repeat the domain.
func TestRequeueOnceContentError(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.errOn[hSPA] = errors.New("pack: the block is not readable")
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v (a read error is not a pass error)", err)
	}
	if res.Published != 0 || res.Cleared != 0 {
		t.Fatalf("the outcome %+v, published=0 cleared=0 expected (the flag is kept)", res)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is cleared with unreadable content — the SPA task is lost before T-030")
	}
}

// TestRequeueOnceContentErrorPartial — one record's read failure does not hinder
// the others: a readable SPA record is published, but the domain's flag is held,
// until all are read (the next pass will pick up the unreadable one).
func TestRequeueOnceContentErrorPartial(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	hDead := content.HashOf([]byte("a block that is not readable"))
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	body.errOn[hDead] = errors.New("pack: the block is not readable")
	pool.seedDomain(5, 13, "mix", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 13, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.seedRow(5, 13, 1, model.VariantMain, hDead[:], nil, false, "https://down.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Published != 1 || res.Cleared != 0 {
		t.Fatalf("the outcome %+v, published=1 cleared=0 expected (the flag of the unreadable record is kept)", res)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 13}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is cleared although one record was not read")
	}
}

// TestRequeueOnceSelection — the selection: only the domains with the flag, the deleted
// excluded (§13.1), the chunk's LIMIT; an empty set — an empty outcome.
func TestRequeueOnceSelection(t *testing.T) {
	pool, body, _, ln, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusDeleted) // outside the cycle
	pool.seedDomain(5, 12, "live", 0, model.StatusAlive)                        // without the flag

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 0 || res.Cleared != 0 || res.Published != 0 || res.Full {
		t.Fatalf("the outcome %+v, an empty one expected", res)
	}
	if n := ln.callsCount(); n != 1 {
		t.Fatalf("XLEN %d, 1 expected", n)
	}
}

// TestRequeueOnceBatchLimit — the chunk's LIMIT without an ORDER BY (T-181): the selection
// takes the first limit rows of the "physical" order, the other domains
// keep the flag until the next pass (the chunks' determinism is not needed —
// the flag clear excludes repeats).
func TestRequeueOnceBatchLimit(t *testing.T) {
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "one", model.FlagRenderPending, model.StatusAlive)
	pool.seedDomain(5, 12, "two", model.FlagRenderPending, model.StatusAlive)
	pool.seedDomain(5, 13, "three", model.FlagRenderPending, model.StatusAlive)

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 2 || res.Full {
		t.Fatalf("the outcome %+v, selected=2 (LIMIT) full=false expected", res)
	}
	// The first two in the insertion order are cleared (draft flags — without SPA records),
	// the third waits for the next pass.
	for _, id := range []int64{11, 12} {
		if pool.domains[renderqDomKey{zone: 5, id: id}].flags.Test(model.FlagRenderPending) {
			t.Fatalf("domain %d was not cleared by the first pass", id)
		}
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 13}].flags.Test(model.FlagRenderPending) {
		t.Fatal("a domain beyond the LIMIT was cleared — the batch is larger than the limit")
	}
}

// TestRequeueOnceSelectPlan — the selection plan shape's pinning (T-181):
// the chunk is read in an explicit transaction strictly by the
// SET LOCAL ×3 → SELECT → COMMIT sequence (a replica of spec §2's diagram: without
// the pinning the planner takes a PK index scan — query #2 of the Oct 24
// slow-query log); the selection goes via a transaction, not the pool.
func TestRequeueOnceSelectPlan(t *testing.T) {
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)

	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	ev := pool.j.snapshot()
	sel := pool.j.indexOf("select")
	if sel < 4 {
		t.Fatalf("a select in the journal at position %d — no step history: %v", sel, ev)
	}
	if ev[sel-4] != "begin" || ev[sel-3] != "plan" || ev[sel-2] != "plan" || ev[sel-1] != "plan" {
		t.Fatalf("no plan-pinning transaction before the SELECT: %v", ev)
	}
	if len(ev) <= sel+1 || ev[sel+1] != "commit" {
		t.Fatalf("no commit after the SELECT: %v", ev)
	}
}

// TestRequeueRenderPendingDrainNoProgress — the initial drainage does not spin
// idle over a chunk without progress (T-181): the content is not read (keep),
// the flags are set — the next selection would return the same domains; without a stop
// the loop would hammer the DB without a pacer (the slow-query log's
// expensive selection no longer slows it down itself).
func TestRequeueRenderPendingDrainNoProgress(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool := newRenderqPool()
	pool.zones[5] = "example"
	body := &renderqContent{
		bodies: map[content.Hash][]byte{},
		errOn:  map[content.Hash]error{hSPA: errors.New("pack: the block is not readable")},
	}
	pool.j.done = make(chan string, 64)
	q := &renderqQueue{j: pool.j}
	rq, err := NewRenderQueue(q, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RequeueRenderPending(ctx, pool, body, rq,
			RequeueOptions{Interval: time.Hour, BatchSize: 1})
	}()

	// We wait for the initial pass's selection.
	deadline := time.After(3 * time.Second)
	for pool.j.count("select") == 0 {
		select {
		case <-deadline:
			t.Fatal("the initial drain did not perform a selection")
		case <-pool.j.done:
		case <-time.After(time.Millisecond):
		}
	}
	// The chunk is full (BatchSize = 1), the flag is not cleared (the content is not read):
	// the drainage must stop — the next tick no earlier than the Interval.
	time.Sleep(50 * time.Millisecond)
	if c := pool.j.count("select"); c != 1 {
		t.Fatalf("%d selections during the initial pass, 1 expected (a batch without progress — wait for a tick)", c)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is cleared with unreadable content")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RequeueRenderPending: %v (a cancellation is a clean nil)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancellation")
	}
}

// TestRequeueOnceGuardZeroDebt — the debt counter guard (T-181): at a
// zero counter the heavy selection does not run at all — even if the
// flags are alive (a manual SQL desync: the drainage stands until the recount,
// runbooks §6; an honest steady state — the same behavior without reading domains).
func TestRequeueOnceGuardZeroDebt(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, q, ln, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.counters["render_pending"] = 0 // a manual desync: the flag is alive, the counter is zero

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 0 || res.Cleared != 0 || res.Published != 0 || res.Full {
		t.Fatalf("the outcome %+v, an empty one expected (the guard: no debt)", res)
	}
	if c := pool.j.count("select"); c != 0 {
		t.Fatalf("the heavy SELECT was executed %d times at a zero debt counter", c)
	}
	if c := pool.j.count("plan"); c != 0 {
		t.Fatalf("the plan pinning was executed %d times at a zero debt counter", c)
	}
	if n := len(q.snapshot()); n != 0 {
		t.Fatalf("%d publications at a zero debt counter", n)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is cleared at a zero counter — the guard must hold the drain until the recount")
	}
	if n := ln.callsCount(); n != 1 {
		t.Fatalf("%d XLEN calls, 1 expected (the guard — after the capacity check)", n)
	}

	// A negative counter (impossible by code — only a manual edit):
	// the same early "no debt" return — a quiet counter bug must not
	// launch a drainage over a negative debt.
	pool.counters["render_pending"] = -1
	res, err = RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce (a negative counter): %v", err)
	}
	if res.Selected != 0 || res.Cleared != 0 || res.Published != 0 {
		t.Fatalf("the outcome %+v at a negative counter, an empty one expected", res)
	}
	if c := pool.j.count("select"); c != 0 {
		t.Fatalf("the heavy SELECT was executed %d times at a negative counter", c)
	}
}

// TestRequeueOnceGuardCounter — the counter's guard read: the row is there —
// a point lookup by name; there is debt — the selection runs (the counter
// is consistent with the flags count: the migration 000015 backfill's invariant).
func TestRequeueOnceGuardCounter(t *testing.T) {
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedDomain(5, 12, "two", model.FlagRenderPending, model.StatusAlive)
	if got := pool.counters["render_pending"]; got != 2 {
		t.Fatalf("the counter seed = %d, 2 expected", got)
	}

	// Two passes: the first clears both flags (drafts — without SPA records),
	// the counter to zero; the second — the guard: no heavy selection.
	for i := 1; i <= 2; i++ {
		res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
		if err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
		switch i {
		case 1:
			if res.Selected != 2 || res.Cleared != 2 {
				t.Fatalf("pass 1: the outcome %+v, selected=2 cleared=2 expected", res)
			}
		case 2:
			if res.Selected != 0 || res.Cleared != 0 {
				t.Fatalf("pass 2: the outcome %+v, an empty one expected (the debt is exhausted)", res)
			}
			if c := pool.j.count("select"); c != 1 {
				t.Fatalf("%d heavy SELECTs over two passes, 1 expected (the second is the guard)", c)
			}
		}
	}
}

// TestRequeueOnceZonePriority — the zones' priority (T-184, the decision (a) of T-176):
// the NOT-from-the-list zones' domains are published ahead of the list's zones' domains, even
// when the latter lie earlier in the "heap" (the production layout: the .rf flag
// debt is physically at the heap's start); no records are lost — the
// list's zones' debt is reworked by the following passes, last.
func TestRequeueOnceZonePriority(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, q, _, rq := renderqPassKit(t, 0)
	pool.zones[6] = "xn--p1ai" // a deprioritized zone (production — .rf)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(6, 21, "rf1", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hSPA[:], nil, false, "https://rf1.xn--p1ai/")
	pool.seedDomain(6, 22, "rf2", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 22, 0, model.VariantMain, hSPA[:], nil, false, "https://rf2.xn--p1ai/")
	pool.seedDomain(5, 11, "prio", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://prio.example/")

	opts := RequeueOptions{BatchSize: 1, DeprioritizedZones: []string{"xn--p1ai"}}
	// Pass 1: the first stage takes the priority zone, skipping the .rf at
	// the "heap's" start.
	res, err := RequeueOnce(context.Background(), pool, body, rq, opts)
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Deprioritized != 0 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("pass 1: the outcome %+v, selected=1 deprioritized=0 cleared=1 published=1 expected", res)
	}
	if pub := q.snapshot(); len(pub) != 1 || pub[0].msg.DomainID != 11 || pub[0].msg.ZoneID != 5 {
		t.Fatalf("pass 1: published %+v, the prioritized domain 5/11 expected", pub)
	}
	if !pool.domains[renderqDomKey{zone: 6, id: 21}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the .рф domain was reworked before the prioritized one — the priority does not work")
	}

	// Passes 2–3: the first stage is exhausted — the second reworks the .rf
	// (a one-shot RequeueOnce keeps no stages' memory — the first stage
	// runs every pass, that is the price of one-shotness; the loop remembers).
	for i, want := range []int64{21, 22} {
		res, err := RequeueOnce(context.Background(), pool, body, rq, opts)
		if err != nil {
			t.Fatalf("pass %d: %v", i+2, err)
		}
		if res.Selected != 1 || res.Deprioritized != 1 || res.Cleared != 1 {
			t.Fatalf("pass %d: the outcome %+v, selected=1 deprioritized=1 cleared=1 expected (the debt of the list zones)", i+2, res)
		}
		if pub := q.snapshot(); pub[len(pub)-1].msg.DomainID != want || pub[len(pub)-1].msg.ZoneID != 6 {
			t.Fatalf("pass %d: published %+v, the .рф domain 6/%d expected", i+2, pub[len(pub)-1], want)
		}
	}
	if got := pool.counters["render_pending"]; got != 0 {
		t.Fatalf("the debt counter = %d, 0 expected (everything is reworked)", got)
	}
}

// TestRequeueOnceZoneBatchFill — an incomplete first stage is topped up
// by the second to a full chunk (T-184): the exhaustion is proven (the LIMIT did
// not fire — the scan reached the table's end), the priority domains go at
// the chunk's start (the publications' order), the debt queue loses no records.
func TestRequeueOnceZoneBatchFill(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, q, _, rq := renderqPassKit(t, 0)
	pool.zones[6] = "xn--p1ai"
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(6, 21, "rf1", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hSPA[:], nil, false, "https://rf1.xn--p1ai/")
	pool.seedDomain(5, 11, "prio", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://prio.example/")
	pool.seedDomain(6, 22, "rf2", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 22, 0, model.VariantMain, hSPA[:], nil, false, "https://rf2.xn--p1ai/")

	res, err := RequeueOnce(context.Background(), pool, body, rq,
		RequeueOptions{BatchSize: 3, DeprioritizedZones: []string{"xn--p1ai"}})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 3 || res.Deprioritized != 2 || res.Cleared != 3 || res.Published != 3 || res.Full {
		t.Fatalf("the outcome %+v, selected=3 deprioritized=2 cleared=3 published=3 full=false expected", res)
	}
	// The publications' order: the priority zone first, then — the list's zones.
	pub := q.snapshot()
	if len(pub) != 3 {
		t.Fatalf("%d publications, 3 expected", len(pub))
	}
	want := []renderqDomKey{{zone: 5, id: 11}, {zone: 6, id: 21}, {zone: 6, id: 22}}
	for i, w := range want {
		if pub[i].msg.ZoneID != w.zone || pub[i].msg.DomainID != w.id {
			t.Fatalf("publication %d: the domain %d/%d, %d/%d expected (the priority — at the head of the batch)",
				i, pub[i].msg.ZoneID, pub[i].msg.DomainID, w.zone, w.id)
		}
	}
}

// TestRequeueOnceZoneListEmpty — an empty zones list (the default):
// the only stage without a filter — the T-181 base selection, the staged
// SQL does not run at all.
func TestRequeueOnceZoneListEmpty(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.zones[6] = "xn--p1ai"
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(6, 21, "rf1", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hSPA[:], nil, false, "https://rf1.xn--p1ai/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Deprioritized != 0 || res.Cleared != 1 {
		t.Fatalf("the outcome %+v, selected=1 cleared=1 expected without stages", res)
	}
	if pool.stage1Select != 0 || pool.stage2Select != 0 {
		t.Fatalf("the staged selections were executed (st1=%d st2=%d) with an empty zone list", pool.stage1Select, pool.stage2Select)
	}
}

// TestRequeueZoneStageMemory — the first stage's exhaustion memory (T-184):
// after the proof (an empty/incomplete chunk — the seq scan reached the table's
// end) the first stage's idle full pass is not repeated while
// the debt counter matches the loop's expectation; a new bit setting (+1,
// T-175) resumes the first stage.
func TestRequeueZoneStageMemory(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, q, _, rq := renderqPassKit(t, 0)
	pool.zones[6] = "xn--p1ai"
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(6, 21, "rf1", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hSPA[:], nil, false, "https://rf1.xn--p1ai/")
	pool.seedDomain(6, 22, "rf2", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 22, 0, model.VariantMain, hSPA[:], nil, false, "https://rf2.xn--p1ai/")

	ctx := context.Background()
	stage := &renderStage{}
	opts := RequeueOptions{BatchSize: 1, DeprioritizedZones: []string{"xn--p1ai"}, Log: slog.New(slog.DiscardHandler)}.withDefaults()

	// Pass 1: the first stage is empty (the exhaustion proof at
	// a debt of 2), the second takes the first .rf domain.
	res, err := requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if res.Deprioritized != 1 || res.Cleared != 1 || !stage.known || stage.proofDebt != 2 || stage.ownClears != 1 {
		t.Fatalf("pass 1: the outcome %+v the memory %+v", res, *stage)
	}
	if pool.stage1Select != 1 || pool.stage2Select != 1 {
		t.Fatalf("pass 1: %d selections st1=%d st2=%d, 1/1 expected", pool.stage1Select, pool.stage2Select)
	}

	// Pass 2: the debt 1 == the expectation (2 − its own withdrawal) — the first stage
	// is skipped, the second reworks the .rf.
	res, err = requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if res.Deprioritized != 1 || res.Cleared != 1 {
		t.Fatalf("pass 2: the outcome %+v", res)
	}
	if pool.stage1Select != 1 || pool.stage2Select != 2 {
		t.Fatalf("pass 2: the selections st1=%d st2=%d, 1/2 expected — the empty pass of the first stage must not repeat",
			pool.stage1Select, pool.stage2Select)
	}

	// Pass 3: the debt 0 — the guard, no heavy selections at all.
	res, err = requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	if res.Selected != 0 || res.Deprioritized != 0 {
		t.Fatalf("pass 3: the outcome %+v, an empty one expected (the guard)", res)
	}
	if pool.stage1Select != 1 || pool.stage2Select != 2 {
		t.Fatalf("pass 3: selections at a zero debt (st1=%d st2=%d)", pool.stage1Select, pool.stage2Select)
	}

	// A new bit setting in the priority zone: the counter 0 → 1 —
	// the divergence from the expectation resumes the first stage.
	pool.seedDomain(5, 11, "prio", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://prio.example/")
	res, err = requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("pass 4: %v", err)
	}
	if res.Selected != 1 || res.Deprioritized != 0 || res.Cleared != 1 {
		t.Fatalf("pass 4: the outcome %+v, selected=1 deprioritized=0 expected (a prioritized domain)", res)
	}
	if pool.stage1Select != 2 {
		t.Fatalf("pass 4: %d first-stage selections, 2 expected — debt growth must resume the first stage", pool.stage1Select)
	}
	if pub := len(q.snapshot()); pub != 3 { // rf1, rf2, prio
		t.Fatalf("%d publications, 3 expected", pub)
	}
}

// TestRequeueZoneStageFullBatchInvalidates — a full first-stage chunk
// strips the memory of the proof (domains may remain beyond the LIMIT):
// the next pass runs the first stage (T-184).
func TestRequeueZoneStageFullBatchInvalidates(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.zones[6] = "xn--p1ai"
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "prio", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://prio.example/")

	ctx := context.Background()
	stage := &renderStage{}
	// A "foreign" exhaustion proof (as if it was there before) — a full
	// chunk must reset it.
	stage.known, stage.proofDebt = true, 100
	opts := RequeueOptions{BatchSize: 1, DeprioritizedZones: []string{"xn--p1ai"}, Log: slog.New(slog.DiscardHandler)}.withDefaults()

	res, err := requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("the pass: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 1 || res.Deprioritized != 0 {
		t.Fatalf("the outcome %+v, selected=1 expected (a full batch of the first stage)", res)
	}
	if pool.stage1Select != 1 {
		t.Fatalf("%d first-stage selections, 1 expected — a full batch resets the proof", pool.stage1Select)
	}
	if stage.known {
		t.Fatal("a full batch left the exhaustion proof — the memory would lie")
	}
}

// TestRequeueZoneStageKeepInvalidates — an uncleared first-stage domain
// removes the exhaustion proof (the T-184 review, P1): a keep at
// unreadable content leaves the flag alive, the counter and the expectation do
// not diverge — without removing the proof the first stage would be skipped
// forever, and the second (the list's zones) does not see the priority domain: the debt
// would stick with a Warn "orphan bits" and a full seq scan every tick.
func TestRequeueZoneStageKeepInvalidates(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	hDead := content.HashOf([]byte("a prioritized block that is not readable"))
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.zones[6] = "xn--p1ai"
	pool.seedDomain(5, 11, "prio", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hDead[:], nil, false, "https://prio.example/")
	pool.seedDomain(6, 21, "rf", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hSPA[:], nil, false, "https://rf.xn--p1ai/")
	body.bodies[hSPA] = renderqSPABody
	body.bodies[hDead] = renderqSPABody
	// The priority domain's content is not read — a keep (a transient failure).
	body.errOn[hDead] = errors.New("pack: the block is not readable")

	ctx := context.Background()
	stage := &renderStage{}
	opts := RequeueOptions{BatchSize: 2, DeprioritizedZones: []string{"xn--p1ai"}, Log: slog.New(slog.DiscardHandler)}.withDefaults()

	// Pass 1: both stages gave a domain each, the priority one not cleared (a keep)
	// — the proof must be removed, the .rf reworked.
	res, err := requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if res.Selected != 2 || res.Deprioritized != 1 || res.Cleared != 1 {
		t.Fatalf("pass 1: the outcome %+v, selected=2 deprioritized=1 cleared=1 expected (keep of the prioritized)", res)
	}
	if stage.known {
		t.Fatal("an uncleared first-stage domain left the exhaustion proof — a stuck prioritized debt")
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the prioritized domain flag is cleared with unreadable content")
	}

	// Pass 2: the content is read (the failure was transient) — the first stage
	// runs again and reworks the stuck domain.
	delete(body.errOn, hDead)
	res, err = requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 1 || res.Deprioritized != 0 {
		t.Fatalf("pass 2: the outcome %+v, selected=1 cleared=1 expected (the stuck prioritized one is reworked)", res)
	}
	if pool.stage1Select != 2 {
		t.Fatalf("%d first-stage selections, 2 expected — an uncleared domain must resume the first stage", pool.stage1Select)
	}
	if got := pool.counters["render_pending"]; got != 0 {
		t.Fatalf("the debt counter = %d, 0 expected", got)
	}
}

// TestRequeueZoneStageFullMidBatchInvalidates — an early exit by the capacity
// (Full) with unprocessed first-stage domains removes the proof
// (the T-184 review, P1): the counter and the expectation do not diverge (neither
// clears nor settings) — without the removal the first stage would be skipped, and the Full tail
// would stay with the flag forever.
func TestRequeueZoneStageFullMidBatchInvalidates(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0, 0, 100) // after the first domain the queue is full
	pool.zones[6] = "xn--p1ai"
	body.bodies[hSPA] = renderqSPABody
	// Two priority + the .rf: the first stage is incomplete (the proof
	// set), Full cuts the processing at the second priority one.
	pool.seedDomain(5, 11, "prio1", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://prio1.example/")
	pool.seedDomain(5, 12, "prio2", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 12, 0, model.VariantMain, hSPA[:], nil, false, "https://prio2.example/")
	pool.seedDomain(6, 21, "rf", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hSPA[:], nil, false, "https://rf.xn--p1ai/")

	ctx := context.Background()
	stage := &renderStage{}
	opts := RequeueOptions{BatchSize: 3, DeprioritizedZones: []string{"xn--p1ai"}, Log: slog.New(slog.DiscardHandler)}.withDefaults()

	res, err := requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("the pass: %v", err)
	}
	if !res.Full || res.Selected != 3 || res.Cleared != 1 {
		t.Fatalf("the outcome %+v, full selected=3 cleared=1 expected (the second prioritized one waits for capacity)", res)
	}
	if stage.known {
		t.Fatal("an early Full exit left the exhaustion proof — the unworked prioritized debt would get stuck")
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 12}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the second prioritized domain flag is cleared at a full queue")
	}
}

// TestRequeueZoneStageSecondSelectError — the second stage's selection
// error removes the exhaustion proof (the T-184 review, round 2): the
// chunk's first-stage domains stayed with the flags, while the counter and the expectation would
// not diverge — without the removal the first stage would be skipped forever.
func TestRequeueZoneStageSecondSelectError(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.zones[6] = "xn--p1ai"
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "prio", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://prio.example/")
	pool.seedDomain(6, 21, "rf", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hSPA[:], nil, false, "https://rf.xn--p1ai/")
	pool.queryErrs[renderPendingDomainsInSQL] = errors.New("sql: the second stage")

	ctx := context.Background()
	stage := &renderStage{}
	opts := RequeueOptions{BatchSize: 2, DeprioritizedZones: []string{"xn--p1ai"}, Log: slog.New(slog.DiscardHandler)}.withDefaults()

	if _, err := requeuePass(ctx, pool, body, rq, opts, stage); err == nil {
		t.Fatal("the second stage error is not returned")
	}
	if stage.known {
		t.Fatal("the second stage error left the exhaustion proof — the prioritized debt of the batch would get stuck")
	}
}

// TestRequeueZoneStageSecondKeepKeepsProof — a stuck SECOND-stage
// domain (the list's zones) does not remove the proof (the T-184 review, round 2):
// the second stage of the next pass will pick it up itself (it runs
// at every first stage's skip) — false removals would return the first stage's
// idle full seq scan after every tail keep episode.
func TestRequeueZoneStageSecondKeepKeepsProof(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	hDead := content.HashOf([]byte("a .рф block that is not readable"))
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.zones[6] = "xn--p1ai"
	pool.seedDomain(5, 11, "prio", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://prio.example/")
	pool.seedDomain(6, 21, "rf", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hDead[:], nil, false, "https://rf.xn--p1ai/")
	body.bodies[hSPA] = renderqSPABody
	body.bodies[hDead] = renderqSPABody
	body.errOn[hDead] = errors.New("pack: the block is not readable")

	ctx := context.Background()
	stage := &renderStage{}
	opts := RequeueOptions{BatchSize: 2, DeprioritizedZones: []string{"xn--p1ai"}, Log: slog.New(slog.DiscardHandler)}.withDefaults()

	// Pass 1: the priority one cleared, the .rf stuck (a keep) — the proof
	// survives: the second stage will pick up the one stuck in a list zone.
	res, err := requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if res.Cleared != 1 || res.Deprioritized != 1 {
		t.Fatalf("pass 1: the outcome %+v, cleared=1 (prioritized) deprioritized=1 expected", res)
	}
	if !stage.known {
		t.Fatal("the keep of a second-stage domain cleared the proof — a false clearing brings back empty full passes")
	}
	if got, want := stage.expectedDebt(), pool.counters["render_pending"]; got != want {
		t.Fatalf("the counter expectation %d, the actual debt %d — a memory desync", got, want)
	}

	// Pass 2: the counter matches — the first stage is skipped, the second
	// repeats the stuck .rf (the content is read).
	delete(body.errOn, hDead)
	res, err = requeuePass(ctx, pool, body, rq, opts, stage)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if res.Selected != 1 || res.Deprioritized != 1 || res.Cleared != 1 {
		t.Fatalf("pass 2: the outcome %+v, selected=1 deprioritized=1 cleared=1 expected (the second stage picks up its own)", res)
	}
	if pool.stage1Select != 1 {
		t.Fatalf("%d first-stage selections, 1 expected — the keep of the second stage must not resume the first", pool.stage1Select)
	}
	if got := pool.counters["render_pending"]; got != 0 {
		t.Fatalf("the debt counter = %d, 0 expected", got)
	}
}

// TestRequeueRenderPendingZoneCycle — the loop with the zones' priority (T-184):
// the initial drainage brings the whole debt down (the priority zones first, the
// list's zones — last), the stages' memory survives the passes, a ctx cancellation —
// a clean nil.
func TestRequeueRenderPendingZoneCycle(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool := newRenderqPool()
	pool.zones[5] = "example"
	pool.zones[6] = "xn--p1ai"
	body := &renderqContent{bodies: map[content.Hash][]byte{hSPA: renderqSPABody}, errOn: make(map[content.Hash]error)}
	pool.j.done = make(chan string, 64)
	q := &renderqQueue{j: pool.j}
	rq, err := NewRenderQueue(q, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	// The "heap": the .rf first, the priority ones — behind them.
	pool.seedDomain(6, 21, "rf1", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(6, 21, 0, model.VariantMain, hSPA[:], nil, false, "https://rf1.xn--p1ai/")
	pool.seedDomain(5, 11, "prio1", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://prio1.example/")
	pool.seedDomain(5, 12, "prio2", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 12, 0, model.VariantMain, hSPA[:], nil, false, "https://prio2.example/")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RequeueRenderPending(ctx, pool, body, rq,
			RequeueOptions{Interval: time.Hour, BatchSize: 1, DeprioritizedZones: []string{"xn--p1ai"}})
	}()

	// We wait for the full drainage: all three flags cleared.
	deadline := time.After(3 * time.Second)
	for {
		pool.mu.Lock()
		left := 0
		for _, d := range pool.domains {
			if d.flags.Test(model.FlagRenderPending) {
				left++
			}
		}
		pool.mu.Unlock()
		if left == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("the drain is not complete: %d flags left", left)
		case <-pool.j.done:
		case <-time.After(time.Millisecond):
		}
	}

	// The publications' order: the priority zones first (the .rf first in the "heap",
	// but reworked last).
	pub := q.snapshot()
	if len(pub) != 3 {
		t.Fatalf("%d publications, 3 expected", len(pub))
	}
	want := []renderqDomKey{{zone: 5, id: 11}, {zone: 5, id: 12}, {zone: 6, id: 21}}
	for i, w := range want {
		if pub[i].msg.ZoneID != w.zone || pub[i].msg.DomainID != w.id {
			t.Fatalf("publication %d: the domain %d/%d, %d/%d expected (the zone priority)",
				i, pub[i].msg.ZoneID, pub[i].msg.DomainID, w.zone, w.id)
		}
	}
	if got := pool.counters["render_pending"]; got != 0 {
		t.Fatalf("the debt counter = %d, 0 expected", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RequeueRenderPending: %v (a cancellation is a clean nil)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancellation")
	}
}

// TestRequeueOnceClearRace — the bit guard at the clear: a competitor has already
// cleared the flag — RowsAffected 0 without an error, the domain not counted.
func TestRequeueOnceClearRace(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	// Between the selection and the clear a competitor cleared the flag.
	pool.beforeHook = func() {
		pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Clear(model.FlagRenderPending)
	}

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Cleared != 0 {
		t.Fatalf("the outcome %+v: a clearing without a row counted", res)
	}
}

// TestRequeueOnceCounterInvariant — the flag clear decrements the debt
// counter by the same transaction (T-175): after the pass the counter equals the number
// of rows with the bit (the migration 000015 backfill's invariant); a domain with
// unreadable content (the flag kept) and a deleted one (outside the cycle) stay
// in the counter.
func TestRequeueOnceCounterInvariant(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	hDead := content.HashOf([]byte("a block that is not readable"))
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	body.errOn[hDead] = errors.New("pack: the block is not readable")

	// Reworked (a clear −1), left (the content is not read), deleted with
	// the bit (outside the cycle — the bit and the counter hold).
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.seedDomain(5, 12, "stuck", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 12, 0, model.VariantMain, hDead[:], nil, false, "https://down.example/")
	pool.seedDomain(5, 13, "gone", model.FlagRenderPending, model.StatusDeleted)
	if got := pool.counters["render_pending"]; got != 3 {
		t.Fatalf("the counter seed = %d, 3 expected", got)
	}

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Cleared != 1 {
		t.Fatalf("the outcome %+v, cleared=1 expected", res)
	}
	// The invariant: the counter == the number of rows with the bit (including the deleted —
	// the migration backfill has no status filter).
	want := int64(0)
	for _, d := range pool.domains {
		if d.flags.Test(model.FlagRenderPending) {
			want++
		}
	}
	if got := pool.counters["render_pending"]; got != want {
		t.Fatalf("the counter = %d, the number of rows with the bit = %d (a desync)", got, want)
	}
	if want != 2 {
		t.Fatalf("%d rows with the bit after the pass, 2 expected (stuck+gone)", want)
	}
}

// TestRequeueOnceCounterFailure — a counter shift failure rolls back the flag
// clear too (one transaction, T-175): the pass returns an error,
// the domain stays with the bit and in the counter — the next tick will repeat the pair.
func TestRequeueOnceCounterFailure(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.queryErrs[renderCounterDeltaSQL] = errors.New("sql: counter")

	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("the counter failure is not returned by the pass error")
	}
	dom := pool.domains[renderqDomKey{zone: 5, id: 11}]
	if !dom.flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is cleared without the counter — a desync (the rollback must restore the bit)")
	}
	if got := pool.counters["render_pending"]; got != 1 {
		t.Fatalf("the counter = %d, 1 expected (the rollback restored it)", got)
	}
}

// TestRequeueOnceCounterMissingRow — there is no counter row (a manual
// edit): the pass's guard (T-181) errors BEFORE the selection — a silent
// desync would make both the gauge and the guard itself a lie; the flag is not cleared — the
// bit+counter pair does not diverge.
func TestRequeueOnceCounterMissingRow(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.counterNoRow = true

	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("a missing counter row is not returned as an error")
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is cleared with a missing counter row — a desync")
	}
}

// TestRequeueOnceErrors — the selection/clear errors are returned to the caller
// (the loop's Warn), the domain's flag stays.
func TestRequeueOnceErrors(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)

	// A heavy selection error: there is debt (the flag + the counter) — the guard
	// lets it through to the selection, the SELECT failure is returned to the caller.
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.queryErrs[renderPendingDomainsSQL] = errors.New("sql: select")
	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("the selection error is not returned")
	}

	// A plan pinning failure (SET LOCAL) — a pass error, without a SELECT.
	pool2, body2, _, _, rq2 := renderqPassKit(t, 0)
	pool2.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool2.queryErrs[renderPlanNoIndexScanSQL] = errors.New("sql: set local")
	if _, err := RequeueOnce(context.Background(), pool2, body2, rq2, RequeueOptions{}); err == nil {
		t.Fatal("the SET LOCAL error is not returned")
	}
	if c := pool2.j.count("select"); c != 0 {
		t.Fatalf("the SELECT was executed %d times at a plan pinning failure", c)
	}

	// A counter guard read failure — a pass error, without a selection.
	pool3, body3, _, _, rq3 := renderqPassKit(t, 0)
	pool3.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool3.queryErrs[renderDebtCounterSQL] = errors.New("sql: counter")
	if _, err := RequeueOnce(context.Background(), pool3, body3, rq3, RequeueOptions{}); err == nil {
		t.Fatal("the counter read error is not returned")
	}
	if c := pool3.j.count("select"); c != 0 {
		t.Fatalf("the SELECT was executed %d times at a guard failure", c)
	}

	pool, body, _, _, rq = renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.queryErrs[renderClearPendingSQL] = errors.New("sql: update")
	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("the flag clearing error is not returned")
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("the flag is cleared at a clearing statement failure")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RequeueOnce(ctx, pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("the ctx cancellation is not returned")
	}
}

// TestRequeueOnceValidation — nil dependencies — an error.
func TestRequeueOnceValidation(t *testing.T) {
	pool := newRenderqPool()
	body := &renderqContent{}
	rq, err := NewRenderQueue(&renderqQueue{}, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	if _, err := RequeueOnce(context.Background(), nil, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("a nil pool: an error expected")
	}
	if _, err := RequeueOnce(context.Background(), pool, nil, rq, RequeueOptions{}); err == nil {
		t.Fatal("a nil store: an error expected")
	}
	if _, err := RequeueOnce(context.Background(), pool, body, nil, RequeueOptions{}); err == nil {
		t.Fatal("a nil render queue: an error expected")
	}
}

// TestRequeueRenderPendingLoop — the loop: the initial drainage immediately (the
// flag reworked before the first tick), a ctx cancellation — a clean nil.
func TestRequeueRenderPendingLoop(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool := newRenderqPool()
	pool.zones[5] = "example"
	body := &renderqContent{bodies: map[content.Hash][]byte{hSPA: renderqSPABody}, errOn: make(map[content.Hash]error)}
	pool.j.done = make(chan string, 16)
	q := &renderqQueue{j: pool.j}
	rq, err := NewRenderQueue(q, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RequeueRenderPending(ctx, pool, body, rq, RequeueOptions{Interval: 5 * time.Millisecond})
	}()

	// We wait for the flag's clear (the initial drainage — before the first tick).
	deadline := time.After(3 * time.Second)
	for {
		pool.mu.Lock()
		cleared := !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending)
		pool.mu.Unlock()
		if cleared {
			break
		}
		select {
		case <-deadline:
			t.Fatal("the flag is not cleared: the initial drain did not rework the domain")
		case <-pool.j.done:
		case <-time.After(time.Millisecond):
		}
	}
	if len(q.snapshot()) != 1 {
		t.Fatalf("%d publications, 1 expected", len(q.snapshot()))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RequeueRenderPending: %v (a cancellation is a clean nil)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancellation")
	}
}

// TestRenderSQLContract — the schema literals and the operators' structure (the
// record_test.go SQL-contract style).
func TestRenderSQLContract(t *testing.T) {
	if !strings.Contains(renderSetPendingSQL, "SET flags = (t.flags | 2)::int2") ||
		!strings.Contains(renderSetPendingSQL, "WHERE t.zone_id = $1::int2 AND t.id = $2::int8") ||
		!strings.Contains(renderSetPendingSQL, "AND (t.flags & 2) = 0") {
		t.Fatalf("renderSetPendingSQL diverged from the §6.1/T-008/T-175 contract:\n%s", renderSetPendingSQL)
	}
	if !strings.Contains(renderPendingDomainsSQL, "(d.flags & 2) <> 0") ||
		!strings.Contains(renderPendingDomainsSQL, "d.status <> 6") ||
		!strings.Contains(renderPendingDomainsSQL, "LIMIT $1") ||
		strings.Contains(renderPendingDomainsSQL, "ORDER BY") {
		t.Fatalf("renderPendingDomainsSQL diverged from the T-181 contract (no PK order, LIMIT — a batch):\n%s", renderPendingDomainsSQL)
	}
	// The zones' priority stages (T-184): the zone name filter in the JOIN,
	// LIMIT — the chunk, no sorting (the T-181 plan shape preserved).
	if !strings.Contains(renderPendingDomainsNotInSQL, "z.name <> ALL($2::text[])") ||
		!strings.Contains(renderPendingDomainsNotInSQL, "LIMIT $1") ||
		strings.Contains(renderPendingDomainsNotInSQL, "ORDER BY") {
		t.Fatalf("renderPendingDomainsNotInSQL diverged from the T-184 contract (the first stage — the zones NOT in the list):\n%s", renderPendingDomainsNotInSQL)
	}
	if !strings.Contains(renderPendingDomainsInSQL, "z.name = ANY($2::text[])") ||
		!strings.Contains(renderPendingDomainsInSQL, "LIMIT $1") ||
		strings.Contains(renderPendingDomainsInSQL, "ORDER BY") {
		t.Fatalf("renderPendingDomainsInSQL diverged from the T-184 contract (the second stage — the list zones):\n%s", renderPendingDomainsInSQL)
	}
	if !strings.Contains(renderPendingDomainsNotInSQL, "(d.flags & 2) <> 0") ||
		!strings.Contains(renderPendingDomainsNotInSQL, "d.status <> 6") ||
		!strings.Contains(renderPendingDomainsInSQL, "(d.flags & 2) <> 0") ||
		!strings.Contains(renderPendingDomainsInSQL, "d.status <> 6") {
		t.Fatalf("the T-184 stages lost the filters of the base T-181 selection:\n%s\n%s", renderPendingDomainsNotInSQL, renderPendingDomainsInSQL)
	}
	if renderPlanNoIndexScanSQL != "SET LOCAL enable_indexscan = off" ||
		renderPlanNoBitmapScanSQL != "SET LOCAL enable_bitmapscan = off" {
		t.Fatalf("the selection plan pinning diverged from the T-181 contract:\n%s\n%s",
			renderPlanNoIndexScanSQL, renderPlanNoBitmapScanSQL)
	}
	if !strings.Contains(renderDebtCounterSQL, "SELECT value FROM crawler_counters") ||
		!strings.Contains(renderDebtCounterSQL, "WHERE name = 'render_pending'") {
		t.Fatalf("renderDebtCounterSQL diverged from the T-181/T-175 contract:\n%s", renderDebtCounterSQL)
	}
	if !strings.Contains(renderPendingRowsSQL, "rendered = false") ||
		!strings.Contains(renderPendingRowsSQL, "final_url IS NOT NULL") ||
		!strings.Contains(renderPendingRowsSQL, "render_failed_at IS NULL") ||
		!strings.Contains(renderPendingRowsSQL, "ORDER BY subdomain_id, variant") {
		t.Fatalf("renderPendingRowsSQL diverged from the §6.1/T-210 contract:\n%s", renderPendingRowsSQL)
	}
	if !strings.Contains(renderClearPendingSQL, "(t.flags & -3)::int2") ||
		!strings.Contains(renderClearPendingSQL, "(t.flags & 2) <> 0") {
		t.Fatalf("renderClearPendingSQL diverged from the T-008 contract:\n%s", renderClearPendingSQL)
	}
	if !strings.Contains(renderCounterDeltaSQL, "SET value = c.value + $1::int8") ||
		!strings.Contains(renderCounterDeltaSQL, "WHERE c.name = 'render_pending'") {
		t.Fatalf("renderCounterDeltaSQL diverged from the T-175 contract:\n%s", renderCounterDeltaSQL)
	}
}

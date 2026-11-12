// Unit tests of the render-worker (T-067; spec 5.6, §5.7, §7, 3.4) over fake
// pool, storage, render and queue — without a DB, Redis, Chromium and the network.
// The fake pool lives the subdomain_results semantics (the target SELECT, an UPDATE with
// the optimistic content_hash guard) and contents (the refcount for
// SwapRefTx, T-044); the fake storage is linked with the pool by the contents table
// (Put +1, Delete −1) — the reference balance is checked end-to-end. Checked:
// the exact SQL and parameters of the UPDATE, the order "render → Put → transaction →
// q:analyze publication → XACK", the re-delivery idempotency,
// races (the row slipped out from under the task), step failures and ctx cancellation; smoke —
// the Run loop. The live render pipeline (Chromium + DB + Redis) —
// the integration tests of the later tasks.
package renderworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// --- event protocol (the "render → Put → transaction → publication" order) ----

// rwEventKind — the protocol event's kind; the order is checked by indexes.
type rwEventKind string

const (
	rwEvRender  rwEventKind = "render"
	rwEvPut     rwEventKind = "put"
	rwEvDelete  rwEventKind = "delete"
	rwEvBegin   rwEventKind = "begin"
	rwEvUpdate  rwEventKind = "update"
	rwEvSwap    rwEventKind = "swapref"
	rwEvTail    rwEventKind = "bump-tail"     // the delta of the unanalyzed tails bucket (T-204)
	rwEvBump    rwEventKind = "bump-rendered" // the delta of the rendered-rate bucket (T-203)
	rwEvCommit  rwEventKind = "commit"
	rwEvPublish rwEventKind = "publish"
	rwEvAck     rwEventKind = "ack"
)

// rwJournal — the thread-safe protocol (the Run loop runs in a goroutine).
type rwJournal struct {
	mu     sync.Mutex
	events []rwEventKind

	// ackNotify — a non-blocking signal per every Ack (the smoke sync).
	ackNotify chan struct{}
}

func (j *rwJournal) record(k rwEventKind) {
	j.mu.Lock()
	j.events = append(j.events, k)
	j.mu.Unlock()
}

func (j *rwJournal) snapshot() []rwEventKind {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]rwEventKind, len(j.events))
	copy(out, j.events)
	return out
}

func (j *rwJournal) indexOf(k rwEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.events {
		if e == k {
			return i
		}
	}
	return -1
}

// countEvents — the number of events of the kind (fixing the "not zero" fact).
func (j *rwJournal) countEvents(k rwEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, e := range j.events {
		if e == k {
			n++
		}
	}
	return n
}

func (j *rwJournal) signalAck() {
	if j.ackNotify == nil {
		return
	}
	select {
	case j.ackNotify <- struct{}{}:
	default:
	}
}

// --- the fake pgx pool: subdomain_results + contents --------------------------

// rwRowKey — the subdomain_results PK in the fake.
type rwRowKey struct {
	zone      int16
	domain    int64
	subdomain int16
	variant   int16
}

// rwRow — a subdomain_results row read by renderSelectSQL and
// changed by renderUpdateSQL (a successful render) and renderFailMarkerSQL
// (the "render impossible" marker, T-210).
type rwRow struct {
	finalURL  *string
	hash      []byte // nil — NULL (no content, T-056)
	rendered  bool
	failedAt  *time.Time // the "render impossible" stamp (T-210); nil — no marker
	cookies   *string
	headers   *string
	checkedAt time.Time // the hour of the rendered-rate bucket (T-203)
}

// rwRowCheckedAt — a fixed moment of the seed rows' episode: the fake's
// rendered-rate bucket is a single value (the DB computes the hour name).
var rwRowCheckedAt = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

// rwFakePool — a RenderDB without a DB: the live subdomain_results semantics
// (a SELECT by PK, an UPDATE with the content_hash IS NOT DISTINCT FROM guard,
// RETURNING checked_at — T-203), contents (a refcount with a > 0 guard —
// sqlMetaDelRef from SwapRefTx, T-044) and the rendered-rate buckets
// crawler_counters (renderBumpRenderedSQL, T-203 — the DB computes the hour
// name, the fake keeps one value). The transaction works on a snapshot:
// Rollback restores the state, Commit keeps it (concurrent
// transactions are not modeled — PostgreSQL serializes them).
type rwFakePool struct {
	j *rwJournal

	rows      map[rwRowKey]*rwRow
	refc      map[content.Hash]int32
	rendered  int64 // the rendered-rate bucket of the seed row's hour (T-203)
	reanalyze int64 // the unanalyzed tails bucket of the seed row's hour (T-204)

	queryErr  error
	updateErr error
	commitErr error
	beginErr  error
	bumpErr   error // a failure of the rendered-rate bucket increment (T-203)
	markErr   error // a failure of the "render impossible" marker (T-210)
	bumpTag   int64 // the RowsAffected of the bucket upsert (0 — an honest answer)

	// mutateBeforeUpdate — a hook: called before the UPDATE inside the transaction
	// (a simulation of the row being rewritten by a recheck between the SELECT and the UPDATE).
	mutateBeforeUpdate func(p *rwFakePool)

	// mutateBeforeMark — a hook: called before the renderFailMarkerSQL marker
	// (T-210, a simulation of the row being rewritten by a recheck between the target SELECT
	// and the finalization stamp).
	mutateBeforeMark func(p *rwFakePool)

	mu sync.Mutex
}

func newRWFakePool() *rwFakePool {
	return &rwFakePool{
		j:    &rwJournal{},
		rows: make(map[rwRowKey]*rwRow),
		refc: make(map[content.Hash]int32),
	}
}

var _ RenderDB = (*rwFakePool)(nil)

func (p *rwFakePool) seedRow(key rwRowKey, finalURL string, hash *content.Hash) {
	r := &rwRow{rendered: false, checkedAt: rwRowCheckedAt}
	if finalURL != "" {
		r.finalURL = &finalURL
	}
	if hash != nil {
		r.hash = hash[:]
		p.refc[*hash]++
	}
	p.rows[key] = r
}

// renderedValue — the value of the rendered-rate bucket (T-203) under the lock.
func (p *rwFakePool) renderedValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rendered
}

// reanalyzeValue — the value of the unanalyzed tails bucket (T-204)
// under the lock.
func (p *rwFakePool) reanalyzeValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reanalyze
}

// seedRowMarked — seeding a row with the "render impossible" marker (T-210):
// the stamp is already set (a finalization of a previous delivery), the content is the same.
func (p *rwFakePool) seedRowMarked(key rwRowKey, finalURL string, hash *content.Hash) {
	p.seedRow(key, finalURL, hash)
	now := time.Now()
	p.rows[key].failedAt = &now
}

func (p *rwFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("rwFakePool: CopyFrom не должен вызываться")
}

// Exec — the "render impossible" marker (T-210, renderFailMarkerSQL):
// the finalization stamps the row with an autocommit (without a transaction), the guards —
// rendered = false and content_hash IS NOT DISTINCT FROM $5; other SQL
// outside a transaction — an error (as before).
func (p *rwFakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if sql != renderFailMarkerSQL {
		return pgconn.CommandTag{}, errors.New("rwFakePool: неожидаемый Exec вне транзакции: " + sql)
	}
	if p.mutateBeforeMark != nil {
		p.mutateBeforeMark(p)
	}
	if p.markErr != nil {
		return pgconn.CommandTag{}, p.markErr
	}
	if len(args) != 5 {
		return pgconn.CommandTag{}, fmt.Errorf("rwFakePool: маркер: параметров %d, нужно 5", len(args))
	}
	key := rwRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
	oldHash := args[4].([]byte)
	r, ok := p.rows[key]
	if !ok || r.rendered {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	same := (r.hash == nil && oldHash == nil) ||
		(r.hash != nil && oldHash != nil && string(r.hash) == string(oldHash))
	if !same {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	now := time.Now()
	if r.failedAt == nil {
		r.failedAt = &now // coalesce: the stamp of the FIRST finalization (T-210)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

// rwFakeRows — pgx.Rows with prepared SELECT rows (final_url,
// content_hash, rendered, checked_at).
type rwFakeRows struct {
	rows [][]any
	i    int
}

func (r *rwFakeRows) Close()                                       {}
func (r *rwFakeRows) Err() error                                   { return nil }
func (r *rwFakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *rwFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *rwFakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *rwFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) || len(dest) != 4 {
		return fmt.Errorf("rwFakeRows: Scan без Next или приёмников %d (нужно 4)", len(dest))
	}
	row := r.rows[r.i-1]
	// NULL columns leave the destination untouched (the pgx semantics:
	// *string/*[]byte stay nil) — final_url and content_hash.
	if row[0] != nil {
		if d, ok := dest[0].(**string); ok {
			*d = row[0].(*string)
		} else {
			return fmt.Errorf("rwFakeRows: приёмник[0] %T, ожидается **string", dest[0])
		}
	}
	if row[1] != nil {
		if d, ok := dest[1].(*[]byte); ok {
			*d = row[1].([]byte)
		} else {
			return fmt.Errorf("rwFakeRows: приёмник[1] %T, ожидается *[]byte", dest[1])
		}
	}
	if d, ok := dest[2].(*bool); ok {
		*d = row[2].(bool)
	} else {
		return fmt.Errorf("rwFakeRows: приёмник[2] %T, ожидается *bool", dest[2])
	}
	if d, ok := dest[3].(*time.Time); ok {
		*d = row[3].(time.Time)
	} else {
		return fmt.Errorf("rwFakeRows: приёмник[3] %T, ожидается *time.Time", dest[3])
	}
	return nil
}

func (r *rwFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *rwFakeRows) RawValues() [][]byte    { return nil }
func (r *rwFakeRows) Conn() *pgx.Conn        { return nil }
func (r *rwFakeRows) TypeMap() *pgtype.Map   { return nil }

func (p *rwFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if sql != renderSelectSQL {
		return nil, fmt.Errorf("rwFakePool: неожидаемый SQL: %s", sql)
	}
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	key := rwRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
	r, ok := p.rows[key]
	if !ok {
		return &rwFakeRows{}, nil
	}
	var hash any
	if r.hash != nil {
		h := make([]byte, len(r.hash))
		copy(h, r.hash)
		hash = h
	}
	return &rwFakeRows{rows: [][]any{{r.finalURL, hash, r.rendered, r.checkedAt}}}, nil
}

func (p *rwFakePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	// The "the recheck rewrote the row" hook runs before the snapshot:
	// the foreign transaction is already committed — our Rollback does not roll it back.
	if p.mutateBeforeUpdate != nil {
		p.mutateBeforeUpdate(p)
	}
	snapRows := make(map[rwRowKey]*rwRow, len(p.rows))
	for k, v := range p.rows {
		c := *v
		snapRows[k] = &c
	}
	snapRefc := make(map[content.Hash]int32, len(p.refc))
	for k, v := range p.refc {
		snapRefc[k] = v
	}
	p.j.record(rwEvBegin)
	return &rwFakeTx{pool: p, snapRows: snapRows, snapRefc: snapRefc,
		snapRendered: p.rendered, snapReanalyze: p.reanalyze}, nil
}

// rwDelRefSQLPrefix — the prefix of sqlMetaDelRef from content/pack.go (T-042):
// the constant is not exported, the exact text is maintained by hand;
// a mismatch breaks the test loudly — sync it with pack.go.
const rwDelRefSQLPrefix = "UPDATE contents SET refcount = refcount - 1"

type rwFakeTx struct {
	pgx.Tx // the stage uses no other pgx.Tx methods

	pool          *rwFakePool
	snapRows      map[rwRowKey]*rwRow
	snapRefc      map[content.Hash]int32
	snapRendered  int64
	snapReanalyze int64
	commit        bool
}

// rwUpdateRow — the pgx.Row of the UPDATE result (RETURNING checked_at, T-203):
// one time.Time destination; err — a statement error or pgx.ErrNoRows
// (the guard cut it off — the former RowsAffected 0).
type rwUpdateRow struct {
	checkedAt time.Time
	err       error
}

func (r rwUpdateRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("rwUpdateRow: приёмников %d, нужно 1", len(dest))
	}
	if d, ok := dest[0].(*time.Time); ok {
		*d = r.checkedAt
		return nil
	}
	return fmt.Errorf("rwUpdateRow: приёмник %T, ожидается *time.Time", dest[0])
}

// QueryRow — the content replacement UPDATE (renderUpdateSQL, T-203): the live
// semantics of the content_hash IS NOT DISTINCT FROM $9 guard, RETURNING
// checked_at of the moved row; an empty result — pgx.ErrNoRows.
func (t *rwFakeTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != renderUpdateSQL {
		return rwUpdateRow{err: fmt.Errorf("rwFakePool: неожидаемый QueryRow: %s", sql)}
	}
	if t.pool.updateErr != nil {
		return rwUpdateRow{err: t.pool.updateErr}
	}
	key := rwRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
	finalURL := args[4].(string)
	newHash := args[5].([]byte)
	cookies := args[6].(*string)
	headers := args[7].(*string)
	oldHash := args[8].([]byte)
	r, ok := t.pool.rows[key]
	if !ok {
		return rwUpdateRow{err: pgx.ErrNoRows}
	}
	// The content_hash IS NOT DISTINCT FROM $9 guard.
	same := (r.hash == nil && oldHash == nil) ||
		(r.hash != nil && oldHash != nil && string(r.hash) == string(oldHash))
	if !same {
		return rwUpdateRow{err: pgx.ErrNoRows}
	}
	checkedAt := r.checkedAt
	r.rendered = true
	r.failedAt = nil // a successful render removes the "render impossible" marker (T-210)
	r.finalURL = &finalURL
	h := make([]byte, len(newHash))
	copy(h, newHash)
	r.hash = h
	r.cookies, r.headers = cookies, headers
	t.pool.j.record(rwEvUpdate)
	return rwUpdateRow{checkedAt: checkedAt}
}

func (t *rwFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	switch {
	case sql == renderBumpReanalyzeSQL:
		// An increment of the unanalyzed tails bucket (T-204): the parameter is
		// the row's checked_at (the bucket hour — the EPISODE's hour), the delta is 1; the
		// fake keeps one value (the DB computes the hour name).
		t.pool.reanalyze += args[1].(int64)
		t.pool.j.record(rwEvTail)
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case sql == renderBumpRenderedSQL:
		// An increment of the rendered-rate bucket (T-203): the parameter is the
		// row's checked_at (the bucket hour), the delta is 1; the fake keeps one value
		// (the DB computes the hour name). bumpTag — the safety branch
		// of RowsAffected ≠ 1.
		if t.pool.bumpErr != nil {
			return pgconn.CommandTag{}, t.pool.bumpErr
		}
		t.pool.rendered += args[1].(int64)
		t.pool.j.record(rwEvBump)
		if t.pool.bumpTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.bumpTag)), nil
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case strings.HasPrefix(sql, rwDelRefSQLPrefix):
		// sqlMetaDelRef (SwapRefTx, T-044): −1 with a refcount > 0 guard.
		var h content.Hash
		copy(h[:], args[0].([]byte))
		if t.pool.refc[h] <= 0 {
			return pgconn.NewCommandTag("UPDATE 0"), nil // ErrRefIntegrity at the caller
		}
		t.pool.refc[h]--
		t.pool.j.record(rwEvSwap)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("rwFakePool: неожидаемый SQL: %s", sql)
	}
}

func (t *rwFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.commit = true
	t.pool.j.record(rwEvCommit)
	return nil
}

func (t *rwFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.commit {
		return nil // a rollback after Commit — a no-op
	}
	t.pool.rows, t.pool.refc = t.snapRows, t.snapRefc
	t.pool.rendered = t.snapRendered
	t.pool.reanalyze = t.snapReanalyze
	return nil
}

// --- the fake content storage (linked with the pool's contents) ---------------

// rwFakeStore — a ContentStore: Put and Delete live in the contents table
// shared with the pool (the refcount), modeling a PackStore over the same DB (T-042).
type rwFakeStore struct {
	j *rwJournal
	p *rwFakePool

	putErr    error
	deleteErr error

	mu      sync.Mutex
	puts    [][]byte
	deletes []content.Hash
}

var _ ContentStore = (*rwFakeStore)(nil)

func (s *rwFakeStore) Put(ctx context.Context, data []byte) (content.PutResult, error) {
	if err := ctx.Err(); err != nil {
		return content.PutResult{}, err
	}
	h := content.HashOf(data)
	s.mu.Lock()
	s.puts = append(s.puts, append([]byte(nil), data...))
	s.mu.Unlock()
	s.p.mu.Lock()
	created := s.p.refc[h] == 0
	s.p.refc[h]++
	s.p.mu.Unlock()
	s.j.record(rwEvPut)
	if s.putErr != nil {
		return content.PutResult{}, s.putErr
	}
	return content.PutResult{Hash: h, Created: created}, nil
}

func (s *rwFakeStore) Delete(ctx context.Context, h content.Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.deletes = append(s.deletes, h)
	s.mu.Unlock()
	s.p.mu.Lock()
	n := s.p.refc[h]
	if n > 0 {
		s.p.refc[h]--
	}
	s.p.mu.Unlock()
	s.j.record(rwEvDelete)
	if s.deleteErr != nil {
		return s.deleteErr
	}
	if n == 0 {
		return content.ErrNotFound
	}
	return nil
}

func (s *rwFakeStore) putsSnapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.puts))
	copy(out, s.puts)
	return out
}

func (s *rwFakeStore) deletesSnapshot() []content.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]content.Hash, len(s.deletes))
	copy(out, s.deletes)
	return out
}

// --- the fake render ----------------------------------------------------------

// rwFakeRenderer — a Renderer: returns a prepared output by URL.
type rwFakeRenderer struct {
	j *rwJournal

	output  render.RenderOutput
	err     error
	panicOn string // the URLs of tasks Run panics on (the R-04 test)
	// blockOn — the URLs of tasks hanging in the render until ctx is cancelled ("page
	// tarpits" up to render.timeout): holds a delivery slot — for the
	// sliding consumption tests (T-142).
	blockOn map[string]bool

	mu    sync.Mutex
	tasks []render.RenderTask
}

var _ Renderer = (*rwFakeRenderer)(nil)

func (r *rwFakeRenderer) Run(ctx context.Context, task render.RenderTask) (render.RenderOutput, error) {
	r.mu.Lock()
	r.tasks = append(r.tasks, task)
	r.mu.Unlock()
	r.j.record(rwEvRender)
	if err := ctx.Err(); err != nil {
		return render.RenderOutput{}, err
	}
	if r.panicOn != "" && task.URL == r.panicOn {
		panic("фейк рендера: паника в задаче " + task.URL)
	}
	if r.blockOn[task.URL] {
		<-ctx.Done() // the slot is busy until cancellation — the stage's duration tail
		return render.RenderOutput{}, ctx.Err()
	}
	if r.err != nil {
		return render.RenderOutput{}, r.err
	}
	return r.output, nil
}

func (r *rwFakeRenderer) tasksSnapshot() []render.RenderTask {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]render.RenderTask, len(r.tasks))
	copy(out, r.tasks)
	return out
}

// tasksCountURL — how many times the render got a task with this URL
// (0 — the delivery did not start: the slots are busy).
func (r *rwFakeRenderer) tasksCountURL(url string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, t := range r.tasks {
		if t.URL == url {
			n++
		}
	}
	return n
}

// --- the fake queue -----------------------------------------------------------

// rwPublishRec — a recorded publication.
type rwPublishRec struct {
	stream string
	msg    queue.Result
}

// rwFakeQueue — a queue.Queue: Publish records the Result messages (and may
// fail by the counter), Consume hands out the prepared deliveries once and
// afterwards parks until ctx is cancelled, Ack is recorded and signals.
type rwFakeQueue struct {
	j *rwJournal

	deliveries []queue.Delivery

	publishErrs []error

	mu        sync.Mutex
	publishes []rwPublishRec
	acked     []string
	consumes  int
}

var _ queue.Queue = (*rwFakeQueue)(nil)

func (q *rwFakeQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	res, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("rwFakeQueue: неожидаемое сообщение %T (нужен queue.Result)", msg)
	}
	q.mu.Lock()
	i := len(q.publishes)
	q.publishes = append(q.publishes, rwPublishRec{stream: stream, msg: res})
	q.mu.Unlock()
	q.j.record(rwEvPublish)
	if i < len(q.publishErrs) && q.publishErrs[i] != nil {
		return "", q.publishErrs[i]
	}
	return fmt.Sprintf("entry-analyze-%d", i+1), nil
}

func (q *rwFakeQueue) Consume(ctx context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	q.consumes++
	var out []queue.Delivery
	if n > 0 && len(q.deliveries) > 0 {
		take := n
		if take > len(q.deliveries) {
			take = len(q.deliveries)
		}
		out = append(out, q.deliveries[:take]...)
		q.deliveries = q.deliveries[take:]
	}
	q.mu.Unlock()
	if len(out) > 0 {
		return out, nil
	}
	<-ctx.Done() // a real queue blocks in XREADGROUP
	return nil, ctx.Err()
}

func (q *rwFakeQueue) Ack(ctx context.Context, _, _ string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked = append(q.acked, ids...)
	q.mu.Unlock()
	for range ids {
		q.j.record(rwEvAck)
	}
	q.j.signalAck()
	return nil
}

func (q *rwFakeQueue) publishesSnapshot() []rwPublishRec {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]rwPublishRec, len(q.publishes))
	copy(out, q.publishes)
	return out
}

func (q *rwFakeQueue) ackedSnapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, len(q.acked))
	copy(out, q.acked)
	return out
}

func (q *rwFakeQueue) consumesCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

// --- builders -----------------------------------------------------------------

// rwTestHash — distinguishable hashes: rwTestHash(1), (2), …
func rwTestHash(n byte) content.Hash {
	var h content.Hash
	h[0] = n
	return h
}

// rwFixture — the assembled set of fakes and the worker.
type rwFixture struct {
	pool     *rwFakePool
	store    *rwFakeStore
	renderer *rwFakeRenderer
	q        *rwFakeQueue
	w        *Worker
}

func newRWFixture(t *testing.T) *rwFixture {
	t.Helper()
	pool := newRWFakePool()
	store := &rwFakeStore{j: pool.j, p: pool}
	renderer := &rwFakeRenderer{j: pool.j}
	q := &rwFakeQueue{j: pool.j}
	w, err := New(pool, q, store, renderer, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &rwFixture{pool: pool, store: store, renderer: renderer, q: q, w: w}
}

// rwKey — the fixture's row key.
var rwKey = rwRowKey{zone: 3, domain: 42, subdomain: 1, variant: 0}

// rwTask — a q:render task on the fixture's key.
var rwTask = queue.Result{
	ZoneID: rwKey.zone, DomainID: rwKey.domain,
	SubdomainID: rwKey.subdomain, Variant: 0, Name: "spa.example",
}

// rwDelivery — a valid delivery with a Result message.
func rwDelivery(id string) queue.Delivery {
	payload, err := json.Marshal(rwTask)
	if err != nil {
		panic(err)
	}
	return queue.Delivery{Stream: queue.StreamRender, ID: id, Payload: payload}
}

// rwKeyDelivery — a valid delivery with the Result key of an arbitrary row
// (variant 0, like the loop tests' rows; T-142).
func rwKeyDelivery(t *testing.T, id string, key rwRowKey, name string) queue.Delivery {
	t.Helper()
	task := queue.Result{
		ZoneID: key.zone, DomainID: key.domain,
		SubdomainID: key.subdomain, Variant: 0, Name: name,
	}
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return queue.Delivery{Stream: queue.StreamRender, ID: id, Payload: payload}
}

// rwOutput — a typical render result.
func rwOutput(dom string) render.RenderOutput {
	return render.RenderOutput{
		FinalURL: "https://spa.example/home",
		Title:    "SPA",
		DOM:      dom,
		Cookies: []render.ExtractCookie{
			{Name: "js_session", Value: "v1", Path: "/", HTTPOnly: true},
		},
		ResponseHeaders: http.Header{"X-Rendered": []string{"yes"}},
	}
}

// rwRefc — a snapshot of the pool's refcounts.
func rwRefc(p *rwFakePool) map[content.Hash]int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[content.Hash]int32, len(p.refc))
	for k, v := range p.refc {
		out[k] = v
	}
	return out
}

// rwRowSnapshot — a snapshot of the row.
func rwRowSnapshot(p *rwFakePool, key rwRowKey) rwRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r, ok := p.rows[key]; ok {
		return *r
	}
	return rwRow{}
}

// --- ProcessResult ------------------------------------------------------------

// TestRenderWorker_HappyPath — the end-to-end contract: the SELECT of the target → the render
// of the row's final URL → the DOM Put → the transaction (an UPDATE + a SwapRef of the old
// reference) → Commit → a q:analyze publication with the same key; the refcount balance:
// the old content −1, the DOM +1.
func TestRenderWorker_HappyPath(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	dom := "<html><body>rendered</body></html>"
	f.renderer.output = rwOutput(dom)

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if !written {
		t.Fatal("строка должна быть записана (written=true)")
	}

	// The render got the row's final URL.
	tasks := f.renderer.tasksSnapshot()
	if len(tasks) != 1 || tasks[0].URL != "https://spa.example/" {
		t.Fatalf("рендер: задачи %+v, нужен один URL https://spa.example/", tasks)
	}

	// The row: rendered=true, the DOM hash, the final URL and the render data.
	domHash := content.HashOf([]byte(dom))
	row := rwRowSnapshot(f.pool, rwKey)
	if !row.rendered {
		t.Error("rendered должен быть true (ТЗ 5.6)")
	}
	if row.finalURL == nil || *row.finalURL != "https://spa.example/home" {
		t.Errorf("final_url = %v, нужен итоговый URL рендера (JS-редиректы)", row.finalURL)
	}
	if row.hash == nil || string(row.hash) != string(domHash[:]) {
		t.Errorf("content_hash = %v, нужен хэш DOM %s", row.hash, domHash)
	}
	if row.cookies == nil || !strings.Contains(*row.cookies, "js_session") {
		t.Errorf("cookies = %v, нужен набор рендера (все cookies, ТЗ 5.6)", row.cookies)
	}
	if row.headers == nil || !strings.Contains(*row.headers, "X-Rendered") {
		t.Errorf("headers = %v, нужны заголовки итоговой навигации (ТЗ 5.8)", row.headers)
	}

	// The refcount balance: the raw HTML −1 (was 1 → 0), the DOM +1.
	refc := rwRefc(f.pool)
	if refc[raw] != 0 {
		t.Errorf("refcount сырого HTML = %d, нужен 0 (ссылка снята, ТЗ 5.7)", refc[raw])
	}
	if refc[domHash] != 1 {
		t.Errorf("refcount DOM = %d, нужен 1", refc[domHash])
	}

	// The q:analyze publication — with the same key, after the Commit.
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze", pubs)
	}
	if pubs[0].msg.ZoneID != rwTask.ZoneID || pubs[0].msg.DomainID != rwTask.DomainID ||
		pubs[0].msg.SubdomainID != rwTask.SubdomainID || pubs[0].msg.Variant != rwTask.Variant {
		t.Errorf("q:analyze ключ %+v не совпадает с задачей", pubs[0].msg)
	}
	if i, j := f.pool.j.indexOf(rwEvCommit), f.pool.j.indexOf(rwEvPublish); i < 0 || j < 0 || i > j {
		t.Errorf("публикация до Commit: commit=%d publish=%d", i, j)
	}
	// The order: render → Put → transaction.
	ev := f.pool.j.snapshot()
	wantOrder := []rwEventKind{rwEvRender, rwEvPut, rwEvBegin, rwEvUpdate, rwEvSwap, rwEvCommit, rwEvPublish}
	last := -1
	for _, w := range wantOrder {
		i := f.pool.j.indexOf(w)
		if i < 0 {
			t.Fatalf("событие %s не наступило: %v", w, ev)
		}
		if i < last {
			t.Fatalf("нарушен порядок %v: %s на позиции %d после %d", ev, w, i, last)
		}
		last = i
	}
}

// TestRenderWorker_SQLContract — the exact text of the SELECT and UPDATE with
// the spec literals (rendered = true, rules_version = NULL; render_failed_at
// = NULL — a successful render removes the T-210 marker; RETURNING checked_at —
// the hour of the rendered-rate bucket, T-203), the "render impossible"
// marker (T-210) and the bucket upsert.
func TestRenderWorker_SQLContract(t *testing.T) {
	selectWant := "SELECT final_url, content_hash, rendered, checked_at FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"
	if renderSelectSQL != selectWant {
		t.Errorf("renderSelectSQL =\n%s\nхочу\n%s", renderSelectSQL, selectWant)
	}
	updateWant := "UPDATE subdomain_results AS r " +
		"SET rendered = true, final_url = $5::text, content_hash = $6::bytea, " +
		"cookies = $7::jsonb, headers = $8::jsonb, rules_version = NULL, render_failed_at = NULL " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.content_hash IS NOT DISTINCT FROM $9::bytea " +
		"RETURNING r.checked_at"
	if renderUpdateSQL != updateWant {
		t.Errorf("renderUpdateSQL =\n%s\nхочу\n%s", renderUpdateSQL, updateWant)
	}
	markerWant := "UPDATE subdomain_results AS r " +
		"SET render_failed_at = coalesce(r.render_failed_at, now()) " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.rendered = false " +
		"AND r.content_hash IS NOT DISTINCT FROM $5::bytea"
	if renderFailMarkerSQL != markerWant {
		t.Errorf("renderFailMarkerSQL =\n%s\nхочу\n%s", renderFailMarkerSQL, markerWant)
	}
	bumpWant := "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('rendered:' || to_char(date_trunc('hour', $1::timestamptz), 'YYYY-MM-DD\"T\"HH24'), $2::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"
	if renderBumpRenderedSQL != bumpWant {
		t.Errorf("renderBumpRenderedSQL =\n%s\nхочу\n%s", renderBumpRenderedSQL, bumpWant)
	}
	tailWant := "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', $1::timestamptz), 'YYYY-MM-DD\"T\"HH24'), $2::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"
	if renderBumpReanalyzeSQL != tailWant {
		t.Errorf("renderBumpReanalyzeSQL =\n%s\nхочу\n%s", renderBumpReanalyzeSQL, tailWant)
	}
}

// TestRenderWorker_RenderedCounter — the rendered-rate buckets (T-203) and
// the unanalyzed tails (T-204): +1 in the same transaction as the flag
// (a failure/rollback — together, the order "UPDATE → tails → rendered →
// commit"); a guard (the row slipped away) does not touch the buckets; a repeated render
// of the same row — another event of both buckets (the streaming semantics,
// reconciliation aligns the absolutes).
func TestRenderWorker_RenderedCounter(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html><body>a</body></html>")

	if written, err := f.w.ProcessResult(context.Background(), rwTask); err != nil || !written {
		t.Fatalf("ProcessResult: ok=%v err=%v", written, err)
	}
	if got := f.pool.renderedValue(); got != 1 {
		t.Fatalf("после рендера корзина rendered-темпа = %d, want 1", got)
	}
	if got := f.pool.reanalyzeValue(); got != 1 {
		t.Fatalf("после рендера корзина хвостов = %d, want 1 (rules_version → NULL вернул строку в неанализированные, T-204)", got)
	}
	update, tail, bump, commit := f.pool.j.indexOf(rwEvUpdate), f.pool.j.indexOf(rwEvTail),
		f.pool.j.indexOf(rwEvBump), f.pool.j.indexOf(rwEvCommit)
	if tail < update || bump < tail || bump > commit {
		t.Fatalf("порядок: update=%d tail=%d bump=%d commit=%d (флаг → хвосты → rendered → фиксация)",
			update, tail, bump, commit)
	}

	// A re-delivery of the same task: a DOM with the same content — the hash
	// guard lets the rewrite through, rendered is already true; the +1 event to the
	// live zero — reconciliation reconciles the semantic difference.
	if written, err := f.w.ProcessResult(context.Background(), rwTask); err != nil || !written {
		t.Fatalf("повтор ProcessResult: ok=%v err=%v", written, err)
	}
	if got := f.pool.renderedValue(); got != 2 {
		t.Fatalf("после повтора корзина = %d, want 2 (событие на каждую запись флага)", got)
	}

	// An increment failure rolls back the transaction: the flag and the bucket change
	// only together (§3.4).
	f2 := newRWFixture(t)
	f2.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f2.renderer.output = rwOutput("<html><body>b</body></html>")
	f2.pool.bumpErr = errors.New("счётчики недоступны")
	if _, err := f2.w.ProcessResult(context.Background(), rwTask); err == nil {
		t.Fatal("сбой корзины обязан вернуть ошибку")
	}
	if f2.pool.renderedValue() != 0 {
		t.Fatalf("корзина = %d после отката, want 0", f2.pool.renderedValue())
	}
	if rwRowSnapshot(f2.pool, rwKey).rendered {
		t.Error("флаг rendered не откатился вместе с корзиной")
	}
	if n := f2.pool.j.countEvents(rwEvCommit); n != 0 {
		t.Errorf("фиксаций = %d, want 0", n)
	}

	// The row slipped out from under the task (the guard): the buckets are untouched.
	f3 := newRWFixture(t)
	f3.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f3.renderer.output = rwOutput("<html><body>c</body></html>")
	f3.pool.mutateBeforeUpdate = func(p *rwFakePool) {
		if r, ok := p.rows[rwKey]; ok {
			other := rwTestHash(9)
			r.hash = other[:]
		}
	}
	if written, err := f3.w.ProcessResult(context.Background(), rwTask); err != nil || written {
		t.Fatalf("ProcessResult охраны: ok=%v err=%v, хочу (false, nil)", written, err)
	}
	if got := f3.pool.renderedValue(); got != 0 {
		t.Fatalf("корзина = %d при охране, want 0 (строку не перевели)", got)
	}
}

// TestRenderWorker_NoRow — no row: a success without a render, Put and publications.
func TestRenderWorker_NoRow(t *testing.T) {
	f := newRWFixture(t)
	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v (строки нет — не ошибка)", err)
	}
	if written {
		t.Error("written должен быть false")
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("ренер не должен запускаться: %+v", tasks)
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не должен вызываться: %d", len(puts))
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_NoFinalURL — no final URL (NULL): nothing
// to render, a success without a write.
func TestRenderWorker_NoFinalURL(t *testing.T) {
	f := newRWFixture(t)
	f.pool.seedRow(rwKey, "", nil) // a NULL final_url, no content
	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if written {
		t.Error("written должен быть false")
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("рендер не должен запускаться: %+v", tasks)
	}
}

// TestRenderWorker_SameContent — the DOM is byte-identical to the stored
// content: Put and the swap are not executed, the row gets rendered=true.
func TestRenderWorker_SameContent(t *testing.T) {
	f := newRWFixture(t)
	dom := "<html><body>stable</body></html>"
	domHash := content.HashOf([]byte(dom))
	f.pool.seedRow(rwKey, "https://spa.example/", &domHash)
	f.renderer.output = rwOutput(dom)

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if !written {
		t.Fatal("written должен быть true")
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не нужен при том же контенте: %d вызовов", len(puts))
	}
	if i := f.pool.j.indexOf(rwEvSwap); i >= 0 {
		t.Errorf("своп ссылок не нужен при том же контенте (событие на %d)", i)
	}
	refc := rwRefc(f.pool)
	if refc[domHash] != 1 {
		t.Errorf("refcount = %d, нужен 1 (без лишних +1)", refc[domHash])
	}
	if !rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("rendered должен быть true")
	}
}

// TestRenderWorker_AliasRow — an alias row (a NULL content_hash, the content
// at the original, T-055): a render by its own URL, the DOM — its own reference, nothing
// to remove.
func TestRenderWorker_AliasRow(t *testing.T) {
	f := newRWFixture(t)
	f.pool.seedRow(rwKey, "https://spa.example/", nil) // a NULL hash
	dom := "<html>alias dom</html>"
	f.renderer.output = rwOutput(dom)

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if !written {
		t.Fatal("written должен быть true")
	}
	if i := f.pool.j.indexOf(rwEvSwap); i >= 0 {
		t.Errorf("своп при NULL старой ссылки не нужен (событие на %d)", i)
	}
	refc := rwRefc(f.pool)
	domHash := content.HashOf([]byte(dom))
	if refc[domHash] != 1 {
		t.Errorf("refcount DOM = %d, нужен 1", refc[domHash])
	}
}

// TestRenderWorker_StaleRow — the row was rewritten between the SELECT and the UPDATE
// (a domain recheck): the guard cuts off the write, the Put reference is removed,
// no publications, the delivery is acknowledged (false, nil).
func TestRenderWorker_StaleRow(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	dom := "<html>fresh dom</html>"
	f.renderer.output = rwOutput(dom)
	// The T-058 recheck rewrote the row before our transaction: the old
	// references were removed by its DELETE, the new row references fresh content.
	fresh := rwTestHash(2)
	f.pool.mutateBeforeUpdate = func(p *rwFakePool) {
		r := p.rows[rwKey]
		p.refc[raw]-- // DELETE of the episode's old rows (SwapRef, T-058)
		r.hash = fresh[:]
		p.refc[fresh]++ // INSERT of the new rows
	}

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v (строка ушла из-под задачи — не ошибка)", err)
	}
	if written {
		t.Error("written должен быть false")
	}
	// The Put reference is removed: the DOM does not hang, the foreign content is intact.
	refc := rwRefc(f.pool)
	domHash := content.HashOf([]byte(dom))
	if refc[domHash] != 0 {
		t.Errorf("refcount DOM = %d, нужен 0 (ссылка Put снята Delete)", refc[domHash])
	}
	if refc[fresh] != 1 {
		t.Errorf("refcount чужого контента = %d, нужен 1", refc[fresh])
	}
	if refc[raw] != 0 {
		t.Errorf("refcount старого контента = %d, нужен 0 (снят эпизодом перепроверки)", refc[raw])
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
	if i := f.pool.j.indexOf(rwEvCommit); i >= 0 {
		t.Errorf("Commit не должен выполняться (событие на %d)", i)
	}
	if deletes := f.store.deletesSnapshot(); len(deletes) != 1 || deletes[0] != domHash {
		t.Errorf("Delete: %+v, нужен один %s", deletes, domHash)
	}
}

// TestRenderWorker_RepeatDelivery — the re-delivery idempotency:
// a repeated render overwrites the content and the refcount, the balance converges
// without leaks (spec 3.4, the T-058 style).
func TestRenderWorker_RepeatDelivery(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)

	dom1 := "<html>render 1</html>"
	f.renderer.output = rwOutput(dom1)
	if _, err := f.w.ProcessResult(context.Background(), rwTask); err != nil {
		t.Fatalf("первая обработка: %v", err)
	}
	dom2 := "<html>render 2</html>"
	f.renderer.output = rwOutput(dom2)
	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("повторная обработка: %v", err)
	}
	if !written {
		t.Fatal("повторная обработка должна записать строку")
	}

	refc := rwRefc(f.pool)
	h1, h2 := content.HashOf([]byte(dom1)), content.HashOf([]byte(dom2))
	if refc[raw] != 0 {
		t.Errorf("refcount сырого HTML = %d, нужен 0", refc[raw])
	}
	if refc[h1] != 0 {
		t.Errorf("refcount DOM первой доставки = %d, нужен 0 (снят при перезаписи)", refc[h1])
	}
	if refc[h2] != 1 {
		t.Errorf("refcount DOM второй доставки = %d, нужен 1", refc[h2])
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 2 {
		t.Errorf("публикаций q:analyze = %d, нужно 2 (после каждой записи)", len(pubs))
	}
	if !rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("rendered должен быть true")
	}
}

// TestRenderWorker_RenderError — a render error: nothing written,
// the error outward (a delivery without an XACK will return with a re-delivery).
func TestRenderWorker_RenderError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = &render.Error{Class: render.ClassTask, URL: "https://spa.example/", Err: errors.New("таймаут")}

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil {
		t.Fatal("ошибка рендера обязана возвращаться")
	}
	if written {
		t.Error("written должен быть false")
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не должен вызываться: %d", len(puts))
	}
	if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
		t.Errorf("транзакция не должна открываться (событие на %d)", i)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_EmptyOutput — an empty DOM or final URL —
// the T-064 contract guard: the row is not touched, a task error.
func TestRenderWorker_EmptyOutput(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = render.RenderOutput{FinalURL: "https://spa.example/x"} // the DOM is empty

	if _, err := f.w.ProcessResult(context.Background(), rwTask); err == nil {
		t.Fatal("пустой DOM — ошибка")
	}
	f.renderer.output = render.RenderOutput{DOM: "<html/>"} // the FinalURL is empty
	if _, err := f.w.ProcessResult(context.Background(), rwTask); err == nil {
		t.Fatal("пустой итоговый URL — ошибка")
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("rendered должен остаться false")
	}
}

// TestRenderWorker_PutError — a Put failure (ErrTooLarge): an error without
// a transaction; a re-delivery will replay the scenario, permanent
// errors will exhaust the attempts and go to q:dead (T-025).
func TestRenderWorker_PutError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html>big dom</html>")
	f.store.putErr = fmt.Errorf("%w: DOM больше лимита", content.ErrTooLarge)

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil || !errors.Is(err, content.ErrTooLarge) {
		t.Fatalf("Put ErrTooLarge обязан возвращаться: %v", err)
	}
	if written {
		t.Error("written должен быть false")
	}
	if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
		t.Errorf("транзакция не должна открываться (событие на %d)", i)
	}
}

// TestRenderWorker_UpdateError — an UPDATE failure: a transaction rollback, the
// Put reference stays as a leak on the safe side (T-044).
func TestRenderWorker_UpdateError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	dom := "<html>dom</html>"
	f.renderer.output = rwOutput(dom)
	f.pool.updateErr = errors.New("нет соединения")

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil {
		t.Fatal("ошибка UPDATE обязана возвращаться")
	}
	if written {
		t.Error("written должен быть false")
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("строка не изменилась (откат)")
	}
	refc := rwRefc(f.pool)
	domHash := content.HashOf([]byte(dom))
	if refc[domHash] != 1 || refc[raw] != 1 {
		t.Errorf("refcount после отката: dom=%d raw=%d, хочу 1/1 (Put не откатывается — безопасная утечка)",
			refc[domHash], refc[raw])
	}
}

// TestRenderWorker_SwapIntegrity — an accounting divergence (the old content
// has no live reference): ErrRefIntegrity with a rollback (T-044).
func TestRenderWorker_SwapIntegrity(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.pool.refc[raw] = 0 // the reference is dead — the accounting diverged
	f.renderer.output = rwOutput("<html>dom</html>")

	_, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil || !errors.Is(err, content.ErrRefIntegrity) {
		t.Fatalf("ожидался ErrRefIntegrity: %v", err)
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("строка не изменилась (откат ErrRefIntegrity)")
	}
}

// TestRenderWorker_CommitError — a commit failure: an error, the row did not
// change, the Put reference — a leak on the safe side.
func TestRenderWorker_CommitError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html>dom</html>")
	f.pool.commitErr = errors.New("сервер закрыл соединение")

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil {
		t.Fatal("ошибка Commit обязана возвращаться")
	}
	if written {
		t.Error("written должен быть false")
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("строка не изменилась (откат)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикации после Commit не должно быть: %+v", pubs)
	}
}

// TestRenderWorker_PublishError — a q:analyze publication failure with
// the row committed: written=false + an error — the publication is part
// of the task, the row is not processed entirely (the T-225 contract); the delivery
// will return with a re-delivery and publish again (at-least-once,
// spec 3.4). The fact of the written DOM is visible in the row (the Commit before the publication).
func TestRenderWorker_PublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html>dom</html>")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil {
		t.Fatal("ошибка публикации обязана возвращаться")
	}
	if written {
		t.Error("written должен быть false — публикация не состоялась, запись не обработана целиком (T-225)")
	}
	if !rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("строка записана (Commit до публикации)")
	}
}

// TestProcessResult_WrittenContract — the ProcessResult contract (T-225):
// true is returned only together with a nil error — the row is processed
// entirely (the DOM written and q:analyze published); every failing path —
// false + an error (the delivery will return with a re-delivery). A regular rejection
// (no row) — false without an error: nothing to process.
func TestProcessResult_WrittenContract(t *testing.T) {
	raw := rwTestHash(1)
	seed := func(f *rwFixture) {
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.output = rwOutput("<html>dom</html>")
	}
	cases := []struct {
		name    string
		setup   func(f *rwFixture)
		wantOK  bool
		wantErr bool
	}{
		{"сбой чтения строки", func(f *rwFixture) {
			f.pool.queryErr = errors.New("база недоступна")
		}, false, true},
		{"ошибка рендера", func(f *rwFixture) {
			seed(f)
			f.renderer.err = errors.New("страница не загрузилась")
		}, false, true},
		{"пустой результат рендера", func(f *rwFixture) {
			seed(f)
			f.renderer.output = render.RenderOutput{FinalURL: "https://spa.example/"}
		}, false, true},
		{"сбой сохранения DOM", func(f *rwFixture) {
			seed(f)
			f.store.putErr = errors.New("хранилище переполнено")
		}, false, true},
		{"сбой транзакции", func(f *rwFixture) {
			seed(f)
			f.pool.beginErr = errors.New("пул соединений исчерпан")
		}, false, true},
		{"сбой UPDATE строки", func(f *rwFixture) {
			seed(f)
			f.pool.updateErr = errors.New("serialize failure")
		}, false, true},
		{"сбой фиксации", func(f *rwFixture) {
			seed(f)
			f.pool.commitErr = errors.New("сервер закрыл соединение")
		}, false, true},
		{"сбой корзины rendered-темпа", func(f *rwFixture) {
			seed(f)
			f.pool.bumpErr = errors.New("счётчики недоступны")
		}, false, true},
		{"расхождение учёта ссылок", func(f *rwFixture) {
			seed(f)
			f.pool.refc[raw] = 0
		}, false, true},
		{"сбой публикации q:analyze", func(f *rwFixture) {
			seed(f)
			f.q.publishErrs = []error{errors.New("redis недоступен")}
		}, false, true},
		{"успех", seed, true, false},
		{"штатный отказ — строки нет", func(f *rwFixture) {}, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFixture(t)
			c.setup(f)
			written, err := f.w.ProcessResult(context.Background(), rwTask)
			if (err != nil) != c.wantErr {
				t.Fatalf("ошибка = %v, wantErr = %v", err, c.wantErr)
			}
			if written != c.wantOK {
				t.Errorf("written = %v, want %v", written, c.wantOK)
			}
			if written && err != nil {
				t.Errorf("written=true при ошибке %v — контракт «true только при err == nil» нарушен", err)
			}
		})
	}
}

// TestRenderWorker_SelectError — a target read failure: an error without a render.
func TestRenderWorker_SelectError(t *testing.T) {
	f := newRWFixture(t)
	f.pool.queryErr = errors.New("база недоступна")
	if _, err := f.w.ProcessResult(context.Background(), rwTask); err == nil {
		t.Fatal("ошибка SELECT обязана возвращаться")
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("рендер не должен запускаться: %+v", tasks)
	}
}

// TestRenderWorker_NewValidation — nil dependencies are rejected.
func TestRenderWorker_NewValidation(t *testing.T) {
	pool := newRWFakePool()
	store := &rwFakeStore{j: pool.j, p: pool}
	renderer := &rwFakeRenderer{j: pool.j}
	q := &rwFakeQueue{j: pool.j}
	cases := []struct {
		name string
		db   RenderDB
		q    queue.Queue
		s    ContentStore
		r    Renderer
	}{
		{"без БД", nil, q, store, renderer},
		{"без очереди", pool, nil, store, renderer},
		{"без хранилища", pool, q, nil, renderer},
		{"без рендера", pool, q, store, nil},
	}
	for _, c := range cases {
		if _, err := New(c.db, c.q, c.s, c.r, Options{}); err == nil {
			t.Errorf("%s: New должен отклонять nil-зависимость", c.name)
		}
	}
}

// --- the Run loop -------------------------------------------------------------

// TestRenderWorker_RunSmoke — the loop: the deliveries are processed, an XACK after
// the publication; a poisoned entry — without an XACK (it will return with a re-delivery,
// T-025); a ctx cancellation — a clean nil.
func TestRenderWorker_RunSmoke(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html>dom</html>")

	key2 := rwRowKey{zone: rwKey.zone, domain: rwKey.domain, subdomain: 2, variant: 0}
	raw2 := rwTestHash(2)
	f.pool.seedRow(key2, "https://two.example/", &raw2)
	task2 := queue.Result{ZoneID: key2.zone, DomainID: key2.domain, SubdomainID: key2.subdomain, Variant: 0, Name: "two.example"}
	payload2, _ := json.Marshal(task2)

	f.pool.j.ackNotify = make(chan struct{}, 4)
	f.q.deliveries = []queue.Delivery{
		rwDelivery("e1"),
		{Stream: queue.StreamRender, ID: "e2", Payload: []byte("{битый json")},
		{Stream: queue.StreamRender, ID: "e3", Payload: payload2},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{}) }()

	// We wait for two XACKs (e1, e3); e2 — poisoned, not acknowledged.
	for want := 2; want > 0; {
		select {
		case <-f.pool.j.ackNotify:
			want--
		case <-time.After(5 * time.Second):
			t.Fatal("XACK не наступил")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}

	acked := f.q.ackedSnapshot()
	if len(acked) != 2 {
		t.Fatalf("XACK = %v, нужны e1 и e3 (e2 ядовитая — без XACK)", acked)
	}
	got := map[string]bool{acked[0]: true, acked[1]: true}
	if !got["e1"] || !got["e3"] {
		t.Errorf("XACK = %v, нужны e1 и e3 (e2 ядовитая — без XACK)", acked)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 {
		t.Fatalf("публикаций q:analyze = %d, нужно 2", len(pubs))
	}
	if i, j := f.pool.j.indexOf(rwEvPublish), f.pool.j.indexOf(rwEvAck); i < 0 || j < 0 || i > j {
		t.Errorf("XACK должен следовать за публикацией: publish=%d ack=%d", i, j)
	}
}

// TestRenderWorker_RunBrowserDead — an unavailable browser: the queue is not
// read (the deliveries stay in the stream, not in the PEL), the loop waits and finishes
// cleanly on the ctx cancellation.
func TestRenderWorker_RunBrowserDead(t *testing.T) {
	f := newRWFixture(t)
	f.q.deliveries = []queue.Delivery{rwDelivery("e1")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, f.w, Options{Alive: func() bool { return false }})
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if n := f.q.consumesCount(); n != 0 {
		t.Errorf("Consume вызван %d раз при мёртвом браузере — очередь читаться не должна", n)
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("рендер не должен запускаться: %+v", tasks)
	}
}

// TestRenderWorker_RunPanicRecovered — a panic while processing one delivery
// (the render/ProcessResult) does not crash the process (the framework invariant, R-04):
// the panicking entry is not acknowledged, the neighboring deliveries of the window
// are finished and acknowledged.
func TestRenderWorker_RunPanicRecovered(t *testing.T) {
	f := newRWFixture(t)
	// Two rows: the render of the first panics (like CDP mid-page),
	// the second — an ordinary delivery of the same window.
	f.pool.seedRow(rwKey, "https://spa.example/", nil)
	key2 := rwRowKey{zone: rwKey.zone, domain: rwKey.domain, subdomain: 2, variant: 0}
	f.pool.seedRow(key2, "https://two.example/", nil)
	f.renderer.panicOn = "https://spa.example/"
	f.renderer.output = rwOutput("<html>dom</html>")

	task2 := queue.Result{ZoneID: key2.zone, DomainID: key2.domain, SubdomainID: key2.subdomain, Variant: 0, Name: "two.example"}
	payload2, err := json.Marshal(task2)
	if err != nil {
		t.Fatal(err)
	}
	f.pool.j.ackNotify = make(chan struct{}, 4)
	f.q.deliveries = []queue.Delivery{rwDelivery("e1"),
		{Stream: queue.StreamRender, ID: "e2", Payload: payload2}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{}) }()

	// e1 must reach the render BEFORE the cancellation (the T-142 review, P2-4): a
	// cancellation right after the neighbor's XACK left the panic non-deterministic —
	// the delivery might not start at all (the ctx already cancelled), the recover branch
	// not executed. A delivery that reached the render with a live ctx panics
	// inevitably (between the task record and panicOn there is only the ctx check).
	rwWaitURL(t, f.renderer, "https://spa.example/")

	// e2 must finish and be acknowledged, despite e1's panic.
	select {
	case <-f.pool.j.ackNotify:
	case <-time.After(5 * time.Second):
		t.Fatal("XACK здоровой доставки не наступил (паника убила цикл?)")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	acked := f.q.ackedSnapshot()
	if len(acked) != 1 || acked[0] != "e2" {
		t.Errorf("XACK = %v, нужен только e2 (паникующая e1 — без XACK)", acked)
	}
	if !rwRowSnapshot(f.pool, key2).rendered {
		t.Error("здоровая доставка не записана")
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("паникующая доставка не должна записывать строку")
	}
}

// TestRenderWorker_RunValidation — a nil worker is rejected.
func TestRenderWorker_RunValidation(t *testing.T) {
	if err := Run(context.Background(), nil, Options{}); err == nil {
		t.Fatal("Run должен отклонять nil-воркер")
	}
}

// TestRenderWorker_RunCancelWhileWaitingSlot — a ctx cancellation during
// the semaphore slot wait does not start a delivery with an already cancelled ctx
// (a port of TestWorkerRunCancelWhileWaitingSlot httpworker, T-139; the
// T-142 review, P3-2 r2): a window of two tasks at Concurrency=1, the first hangs in
// the render until the cancellation — the second must stay unstarted (its URL never
// reaches the render), the window returns to the queue with a re-delivery.
func TestRenderWorker_RunCancelWhileWaitingSlot(t *testing.T) {
	s := rwSlidingSetup(t, 1, 1) // the blocking one holds the only slot
	f := s.f

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{Concurrency: 1, Consume: 2}) }()

	// The blocking delivery is in the render (holding the slot) — we cancel: the fast
	// one is waiting for the semaphore at this moment.
	rwWaitURL(t, f.renderer, s.blockURLs[0])
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if n := f.renderer.tasksCountURL(s.bufURLs[0]); n != 0 {
		t.Errorf("доставка запущена после отмены ctx: рендер вызван %d раз", n)
	}
	if acks := len(f.q.ackedSnapshot()); acks != 0 {
		t.Errorf("подтверждений %d — окно обязано вернуться очереди", acks)
	}
}

// TestRenderWorker_OptionsDefaults — the worker loop's defaults and
// the configuration are consistent (the TestWorkerOptionsDefaults T-139 pattern;
// the T-142 review, P3-1 r2): DefaultConcurrency — the floor of the delivery parallelism
// of isolated builds (the upper bound of the spec 5.6 recommendation), the production
// build passes max(DefaultConcurrency, render.contexts). T-169 raised
// the default render.contexts (8) above the floor: the invariant — the floor is no higher
// than the default contexts (the max equality — the documented
// parallelism), a downward mismatch would silently change the production
// instance's behavior against the documentation.
func TestRenderWorker_OptionsDefaults(t *testing.T) {
	o := Options{}.withDefaults()
	if o.Concurrency != DefaultConcurrency || o.Consume != DefaultConsume {
		t.Errorf("withDefaults: concurrency=%d consume=%d, дефолты %d/%d",
			o.Concurrency, o.Consume, DefaultConcurrency, DefaultConsume)
	}
	cfg := config.Default()
	if cfg.Render.Contexts < DefaultConcurrency {
		t.Errorf("render.contexts=%d ниже DefaultConcurrency=%d: max() боевой сборки превысил бы слоты пула",
			cfg.Render.Contexts, DefaultConcurrency)
	}
	if cfg.Render.TimeoutAttempts != DefaultTimeoutAttempts {
		t.Errorf("render.timeout_attempts=%d расходится с DefaultTimeoutAttempts=%d (боевая сборка передаёт поле воркеру)",
			cfg.Render.TimeoutAttempts, DefaultTimeoutAttempts)
	}
}

// --- the loop's sliding consumption (T-142) -----------------------------------

// rwSlidingFixture — the rows and deliveries of the sliding consumption tests:
// keys with different subdomain_ids, each row has its own final URL.
type rwSlidingFixture struct {
	f         *rwFixture
	blockKeys []rwRowKey
	blockURLs []string
	bufKeys   []rwRowKey
	bufURLs   []string
}

// rwSlidingSetup — N blocking rows ("page tarpits", they hold the
// slots until ctx is cancelled) and M fast ones; the deliveries — the blocking ones first,
// then the fast ones (the chunk's tail — like in the barrier window before T-142).
func rwSlidingSetup(t *testing.T, blocks, fasts int) *rwSlidingFixture {
	t.Helper()
	f := newRWFixture(t)
	f.renderer.output = rwOutput("<html>dom</html>")
	f.renderer.blockOn = make(map[string]bool)
	s := &rwSlidingFixture{f: f}
	var deliveries []queue.Delivery
	for i := 0; i < blocks; i++ {
		key := rwRowKey{zone: rwKey.zone, domain: rwKey.domain, subdomain: int16(10 + i), variant: 0}
		url := fmt.Sprintf("https://block%d.example/", i)
		f.pool.seedRow(key, url, nil)
		s.blockKeys = append(s.blockKeys, key)
		s.blockURLs = append(s.blockURLs, url)
		f.renderer.blockOn[url] = true
		deliveries = append(deliveries, rwKeyDelivery(t, fmt.Sprintf("b%d", i), key, "slide.example"))
	}
	for i := 0; i < fasts; i++ {
		key := rwRowKey{zone: rwKey.zone, domain: rwKey.domain, subdomain: int16(20 + i), variant: 0}
		url := fmt.Sprintf("https://fast%d.example/", i)
		f.pool.seedRow(key, url, nil)
		s.bufKeys = append(s.bufKeys, key)
		s.bufURLs = append(s.bufURLs, url)
		deliveries = append(deliveries, rwKeyDelivery(t, fmt.Sprintf("f%d", i), key, "slide.example"))
	}
	f.q.deliveries = deliveries
	return s
}

// rwWaitURL — waits until the render gets a task with the URL.
func rwWaitURL(t *testing.T, r *rwFakeRenderer, url string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for r.tasksCountURL(url) == 0 {
		select {
		case <-deadline:
			t.Fatalf("доставка %s не дошла до рендера", url)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// rwStopLoop — the ctx cancellation and a wait for a clean nil from Run.
func rwStopLoop(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
}

// TestRenderWorker_RunSlidingConsumption — the essence of T-142: the q:render read does
// not wait for the whole chunk to finish. The window's first delivery hangs in the render until
// ctx is cancelled (the duration tail — a page up to render.timeout); with
// a window barrier (before T-142) wg.Wait would hold the loop on it — the chunk's
// tail deliveries and the next read would not start. The sliding consumption
// must acknowledge all the fast deliveries while the blocking one is in flight:
// the slots are freed by the finished tasks, the reader moves on.
func TestRenderWorker_RunSlidingConsumption(t *testing.T) {
	s := rwSlidingSetup(t, 1, 4) // a blocking one + 4 fast ones
	f := s.f
	f.pool.j.ackNotify = make(chan struct{}, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{Concurrency: 2, Consume: 4}) }()

	// The blocking delivery is in the render and holds the slot — the fast ones finish
	// and are acknowledged past it (the chunk 4: b0 + f0..f2, the read of f3 —
	// by the next chunk, possible only with sliding consumption).
	rwWaitURL(t, f.renderer, s.blockURLs[0])
	deadline := time.After(5 * time.Second)
	for acks := 0; acks < 4; {
		select {
		case <-f.pool.j.ackNotify:
			acks++
		case <-deadline:
			cancel()
			t.Fatalf("скользящее потребление не подхватило хвост: подтверждено %d из 4 быстрых", acks)
		}
	}

	rwStopLoop(t, cancel, done)
	// The blocking delivery is not acknowledged — it will return with a
	// re-delivery (at-least-once, spec 3.4), the fast ones are acknowledged.
	acked := f.q.ackedSnapshot()
	if len(acked) != 4 {
		t.Fatalf("XACK = %v, нужны 4 быстрые (блокирующая — без XACK)", acked)
	}
	got := make(map[string]bool, len(acked))
	for _, id := range acked {
		got[id] = true
	}
	for _, id := range []string{"f0", "f1", "f2", "f3"} {
		if !got[id] {
			t.Errorf("XACK = %v, нет быстрой %s", acked, id)
		}
	}
	for i, key := range s.bufKeys {
		if !rwRowSnapshot(f.pool, key).rendered {
			t.Errorf("быстрая доставка f%d не записана", i)
		}
	}
}

// TestRenderWorker_RunCancelKeepsBufferUnacked — the sliding
// buffer guarantee (T-142, following the httpworker T-139 pattern): the read ahead but not
// started deliveries (the slots are busy with the blocking tasks) are NOT started and NOT
// acknowledged on a ctx cancellation — together with the reader's current chunks
// they stay in the stream's pending and return by an XAUTOCLAIM claim
// (at-least-once, spec 3.4). The sync — by the fact of the prefetch (the second
// read happened while the first chunk was still in progress): a barrier loop before
// T-142 would not read the second chunk at all — waiting for the reads is the
// regression part (the T-142 review, P2-3), a time-based Sleep is not needed.
func TestRenderWorker_RunCancelKeepsBufferUnacked(t *testing.T) {
	s := rwSlidingSetup(t, 2, 4) // both blocking ones hold the slots
	f := s.f

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	// Consume=4: chunk 1 = [b0, b1, f0, f1], chunk 2 = [f2, f3] —
	// the reader must read both while b0/b1 hold both slots.
	go func() { done <- Run(ctx, f.w, Options{Concurrency: 2, Consume: 4}) }()

	// Both blocking ones are in the render (holding the slots).
	for _, url := range s.blockURLs {
		rwWaitURL(t, f.renderer, url)
	}
	// The prefetch: the second chunk is read before the first one finishes —
	// the f2/f3 buffer really exists (on a barrier loop of reads
	// there would be 1 left and the wait here would fail).
	deadline := time.After(5 * time.Second)
	for f.q.consumesCount() < 2 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("предвыборка не состоялась: чтений %d, нужно ≥2 (хвост окна обязан читаться до завершения первой пачки)", f.q.consumesCount())
		case <-time.After(2 * time.Millisecond):
		}
	}

	// The fast ones are read ahead but must stay unstarted —
	// never reaching the render however long the loop runs.
	for i, url := range s.bufURLs {
		if n := f.renderer.tasksCountURL(url); n != 0 {
			t.Errorf("буферная доставка f%d запущена при занятых слотах: рендер вызван %d раз", i, n)
		}
	}
	if acks := len(f.q.ackedSnapshot()); acks != 0 {
		t.Errorf("подтверждений %d до отмены — не должно быть", acks)
	}

	rwStopLoop(t, cancel, done)
	for i, url := range s.bufURLs {
		if n := f.renderer.tasksCountURL(url); n != 0 {
			t.Errorf("буферная доставка f%d запущена после отмены: рендер вызван %d раз", i, n)
		}
	}
	if acks := len(f.q.ackedSnapshot()); acks != 0 {
		t.Errorf("подтверждений %d — не начатые доставки обязаны вернуться очереди", acks)
	}
}

// rwPoisonQueue — an endless source of poisoned deliveries: every read
// immediately returns a chunk of invalid entries (Decode in
// processDelivery fails, the delivery stays without an XACK). The read rate
// is limited only by the loop's fuse — a perfect gauge of its work without
// processing timings (the pattern — workerPoisonQueue httpworker).
type rwPoisonQueue struct {
	mu       sync.Mutex
	consumes int
	acked    []string
}

func (q *rwPoisonQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", nil
}

func (q *rwPoisonQueue) Consume(_ context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if n < 1 {
		n = 1
	}
	out := make([]queue.Delivery, n)
	for i := range out {
		out[i] = queue.Delivery{Stream: queue.StreamRender, ID: fmt.Sprintf("poison-%d", i), Payload: []byte("{")}
	}
	q.mu.Lock()
	q.consumes++
	q.mu.Unlock()
	return out, nil
}

func (q *rwPoisonQueue) Ack(_ context.Context, _, _ string, ids ...string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked = append(q.acked, ids...)
	return nil
}

func (q *rwPoisonQueue) consumeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

func (q *rwPoisonQueue) ackedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.acked)
}

// TestRenderWorker_RunBackoffOnFailures — the sliding loop's fuse
// (T-142): on a systematic processing failure (every delivery
// fails — poisoned entries here) the q:render read is suspended with
// the pause doubling, otherwise the loop would burn the delivery attempts (XAUTOCLAIM →
// q:dead, T-025) in a hot mode; the reads continue — the counter
// does not block the loop forever. The pause is shrunk to 100 ms: seconds are not
// waited out in a unit test, the boundaries are taken with a multiple margin against CI jitter.
func TestRenderWorker_RunBackoffOnFailures(t *testing.T) {
	old := renderFailWaitBase
	renderFailWaitBase = 100 * time.Millisecond
	t.Cleanup(func() { renderFailWaitBase = old })

	// The read chunk is minimal: the fuse threshold — Consume failures
	// in a row (1), the read pipeline is no deeper than a couple of deliveries.
	f := newRWFixture(t)
	q := &rwPoisonQueue{}
	w, err := New(f.pool, q, f.store, f.renderer, Options{Concurrency: 1, Consume: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, w, Options{Concurrency: 1, Consume: 1}) }()

	// The first reads happen before the counter trips (the failure
	// registration is asynchronous) — a 450 ms window covers them with a margin; then the
	// 100/200/400 ms pauses throttle the read rate to units.
	deadline := time.After(5 * time.Second)
	waitCount := func(n int) int {
		for {
			got := q.consumeCount()
			if got >= n {
				return got
			}
			select {
			case <-deadline:
				t.Fatalf("чтений %d, ждалось ≥%d — предохранитель заблокировал цикл", got, n)
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	waitCount(1)
	time.Sleep(450 * time.Millisecond)
	first := q.consumeCount()
	if first > 10 {
		t.Errorf("чтений %d за 450 мс — предохранитель не душит темп (ждалось ≤10)", first)
	}

	// The pauses double: the next interval adds a few reads,
	// not tens. The growth between two measurements — no more than a couple of reads.
	time.Sleep(450 * time.Millisecond)
	if grew := q.consumeCount() - first; grew > 2 {
		t.Errorf("за следующие 450 мс ещё %d чтений — пауза не растёт с серией неудач", grew)
	}

	rwStopLoop(t, cancel, done)
	if q.ackedCount() != 0 {
		t.Errorf("подтверждений %d при ядовитых доставках", q.ackedCount())
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("рендер не должен запускаться: %+v", tasks)
	}
}

// rwPanicQueue — an endless source of valid deliveries of a panicking
// key: Decode passes, the row exists, the render panics (a recover in the loop,
// the T-142 P2-4 review) — deliveries without an XACK; the read rate is throttled by
// the fuse, the panics are counted by the failed counter as failures.
type rwPanicQueue struct {
	payload []byte // the Result of the panicking key (rwKey)

	mu       sync.Mutex
	consumes int
}

func (q *rwPanicQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", nil // a panicking delivery never reaches the publication
}

func (q *rwPanicQueue) Consume(_ context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if n < 1 {
		n = 1
	}
	out := make([]queue.Delivery, n)
	for i := range out {
		out[i] = queue.Delivery{Stream: queue.StreamRender, ID: fmt.Sprintf("panic-%d", i), Payload: q.payload}
	}
	q.mu.Lock()
	q.consumes++
	q.mu.Unlock()
	return out, nil
}

func (q *rwPanicQueue) Ack(context.Context, string, string, ...string) error {
	panic("rwPanicQueue: Ack недостижим — паникующие доставки не подтверждаются")
}

func (q *rwPanicQueue) consumeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

// TestRenderWorker_RunPanicBackoff — panics in the fuse counter
// (T-142): a series of panicking deliveries without a single acknowledgment — the same
// failure series as processing errors: the q:render read is throttled by a pause
// with doubling (without counting the panics the loop would burn the delivery attempts
// at full speed), the process does not crash.
func TestRenderWorker_RunPanicBackoff(t *testing.T) {
	old := renderFailWaitBase
	renderFailWaitBase = 100 * time.Millisecond
	t.Cleanup(func() { renderFailWaitBase = old })

	f := newRWFixture(t)
	f.pool.seedRow(rwKey, "https://spa.example/", nil)
	f.renderer.panicOn = "https://spa.example/"
	q := &rwPanicQueue{payload: rwDelivery("p").Payload}
	w, err := New(f.pool, q, f.store, f.renderer, Options{Concurrency: 1, Consume: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, w, Options{Concurrency: 1, Consume: 1}) }()

	// The panic happened (the delivery reached the render), the reads are throttled —
	// the same boundaries as TestRenderWorker_RunBackoffOnFailures.
	rwWaitURL(t, f.renderer, "https://spa.example/")
	deadline := time.After(5 * time.Second)
	waitCount := func(n int) int {
		for {
			got := q.consumeCount()
			if got >= n {
				return got
			}
			select {
			case <-deadline:
				t.Fatalf("чтений %d, ждалось ≥%d — предохранитель заблокировал цикл", got, n)
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	waitCount(1)
	time.Sleep(450 * time.Millisecond)
	first := q.consumeCount()
	if first > 10 {
		t.Errorf("чтений %d за 450 мс — паники не душат темп (ждалось ≤10)", first)
	}
	time.Sleep(450 * time.Millisecond)
	if grew := q.consumeCount() - first; grew > 2 {
		t.Errorf("за следующие 450 мс ещё %d чтений — пауза не растёт с серией паник", grew)
	}

	rwStopLoop(t, cancel, done)
	if n := f.renderer.tasksCountURL("https://spa.example/"); n == 0 {
		t.Error("паникующая доставка не дошла до рендера — тест ничего не проверил")
	}
}

// rwInfraQueue — an endless source of valid q:render deliveries with
// a switchable q:analyze publication error (T-152): in the OOM mode
// Publish returns an InfraError — a model of the result write error,
// the delivery processing fails already after the DB write.
type rwInfraQueue struct {
	mu       sync.Mutex
	consumes int
	acked    int
	publish  error
}

func (q *rwInfraQueue) setPublishErr(err error) {
	q.mu.Lock()
	q.publish = err
	q.mu.Unlock()
}

func (q *rwInfraQueue) Publish(context.Context, string, queue.Message) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return "", q.publish
}

func (q *rwInfraQueue) Consume(_ context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if n < 1 {
		n = 1
	}
	out := make([]queue.Delivery, n)
	for i := range out {
		out[i] = rwDelivery(fmt.Sprintf("infra-%d", i))
	}
	q.mu.Lock()
	q.consumes++
	q.mu.Unlock()
	return out, nil
}

func (q *rwInfraQueue) Ack(_ context.Context, _, _ string, _ ...string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked++
	return nil
}

func (q *rwInfraQueue) consumeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

func (q *rwInfraQueue) ackedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.acked
}

// rwProbeRedis — a SET/XAUTOCLAIM/XPENDING client for the write probe
// Reliab.Writable (T-152): a switchable SET error, the claim is always empty.
type rwProbeRedis struct {
	mu     sync.Mutex
	setErr error
}

func (f *rwProbeRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	err := f.setErr
	f.mu.Unlock()
	return redis.NewStatusResult("OK", err)
}

func (f *rwProbeRedis) setProbeErr(err error) {
	f.mu.Lock()
	f.setErr = err
	f.mu.Unlock()
}

func (f *rwProbeRedis) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	cmd := redis.NewXAutoClaimCmd(ctx, "xautoclaim")
	cmd.SetVal(nil, "0-0")
	return cmd
}

func (f *rwProbeRedis) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return redis.NewXPendingExtCmd(ctx, "xpending")
}

// TestRenderWorker_RunInfraPause — the consumption pause on a sustained
// InfraError of publication (T-152, the httpworker pattern): a publication error
// of q:analyze (a Redis OOM) is not a render error, the q:render read
// stops until the write recovers (Reliab.Writable), after the
// recovery — resumes and brings the deliveries to XACK.
func TestRenderWorker_RunInfraPause(t *testing.T) {
	old := renderFailWaitBase
	renderFailWaitBase = 5 * time.Millisecond
	t.Cleanup(func() { renderFailWaitBase = old })

	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html><body>infra pause</body></html>")

	q := &rwInfraQueue{}
	q.setPublishErr(&queue.InfraError{
		Op:  "publish в q:analyze",
		Err: errors.New("OOM command not allowed when used memory > 'maxmemory'"),
	})
	probe := &rwProbeRedis{}
	probe.setProbeErr(errors.New("OOM command not allowed when used memory > 'maxmemory'"))
	reliab := queue.NewReliab(probe, q, queue.ReliabOptions{MinIdle: time.Hour, MaxAttempts: 3})
	w, err := New(f.pool, q, f.store, f.renderer, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	opts := Options{Concurrency: 1, Consume: 1, Recover: reliab}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, w, opts) }()

	// The OOM window: the first chunk is read, the publication failed — the read
	// is stopped by the write probe.
	deadline := time.After(5 * time.Second)
	for q.consumeCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("первое чтение q:render не состоялось")
		case <-time.After(2 * time.Millisecond):
		}
	}
	time.Sleep(300 * time.Millisecond)
	if n := q.consumeCount(); n > 6 {
		t.Errorf("чтений %d за 300 мс инфраструктурной паузы — чтение не остановлено (ждалось ≤6)", n)
	}
	if q.ackedCount() != 0 {
		t.Errorf("подтверждений %d при падающей публикации", q.ackedCount())
	}

	// The write recovered: the read resumes, the deliveries are brought
	// to XACK.
	probe.setProbeErr(nil)
	q.setPublishErr(nil)
	for q.ackedCount() < 2 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("после восстановления записи подтверждено %d, хочу ≥2", q.ackedCount())
		case <-time.After(2 * time.Millisecond):
		}
	}

	rwStopLoop(t, cancel, done)
}

// TestRenderFailBackoff — the fuse pause series: a doubling from the base,
// the DefaultErrorWait cap, the clamp of a negative series (the T-142 review,
// P3-5: streak < 1 must not panic with a shift).
func TestRenderFailBackoff(t *testing.T) {
	old := renderFailWaitBase
	renderFailWaitBase = time.Second
	t.Cleanup(func() { renderFailWaitBase = old })

	for _, c := range []struct {
		streak int
		want   time.Duration
	}{
		{-3, time.Second},
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second},   // 32 s above the DefaultErrorWait cap
		{100, 30 * time.Second}, // the series does not grow beyond the cap
	} {
		if got := renderFailBackoff(c.streak); got != c.want {
			t.Errorf("renderFailBackoff(%d) = %s, хочу %s", c.streak, got, c.want)
		}
	}
}

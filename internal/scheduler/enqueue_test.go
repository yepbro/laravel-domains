// Unit tests of scheduler batch enqueueing (T-028, spec 5.3, 4.2, 3.4) on
// a fake pgx pool and queue (the dispatch_test.go / retry_test.go style):
// the exact SQL text of the move and parameters, the pending and queued_at guard,
// publishing exactly the moved rows (a race — not all moved),
// grouping the batch into DNSBatch (names from the selection, order preserved),
// DB/read/publication errors, an empty batch, validation, ctx cancellation;
// an EnqueueLoop smoke (selection → move → publication → pause,
// waiting on an empty batch, continuing after errors). Behavior against
// a live DB/Redis — integration tests (T-032).
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

// --- fake pool and queue ----------------------------------------------
//
// Names prefixed with enqueue: fakes of the T-027/T-030/T-031 tasks live
// in the package in parallel (dispatchPool, retryFakePool); no common helpers
// are introduced here.

// enqueueCallKind — the loop step served by a Query.
type enqueueCallKind string

const (
	enqueueKindZones    enqueueCallKind = "zones"    // the SELECT of active zones (T-027)
	enqueueKindDispatch enqueueCallKind = "dispatch" // the SELECT of zone queues (T-027)
	enqueueKindUpdate   enqueueCallKind = "update"   // UPDATE … RETURNING (T-028)
)

// enqueueCall — a recorded Query call.
type enqueueCall struct {
	kind enqueueCallKind
	sql  string
	args []any
}

// enqueueFakeRows — pgx.Rows with pre-made rows; Scan is positional
// (in the SQL column order), like retryFakeRows.
type enqueueFakeRows struct {
	rows    [][]any
	i       int
	closed  bool
	err     error // Err() after the rows are exhausted
	scanErr error // a Scan error, if set
}

func enqueueFakeRowsOf(rows ...[]any) *enqueueFakeRows {
	return &enqueueFakeRows{rows: rows}
}

func (r *enqueueFakeRows) Close() { r.closed = true }

func (r *enqueueFakeRows) Err() error { return r.err }

func (r *enqueueFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *enqueueFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *enqueueFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *enqueueFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("enqueueFakeRows: Scan without Next")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("enqueueFakeRows: %d sinks, %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(row[i]))
	}
	return nil
}

func (r *enqueueFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *enqueueFakeRows) RawValues() [][]byte    { return nil }

// Conn and TypeMap — part of pgx.Rows not used by the positional Scan.
func (r *enqueueFakeRows) Conn() *pgx.Conn      { return nil }
func (r *enqueueFakeRows) TypeMap() *pgtype.Map { return nil }

// enqueueFakePool — a db.DB without a DB: Query distinguishes the selection
// queries (the zone reference table and zone queues, dispatch.go) from the move (UPDATE) by
// text, and hands out pre-made results per step. The selection queue
// exhausted — an empty set (pending is empty); the move queue exhausted —
// all batch keys are moved (no race). Everything under a lock: the
// EnqueueLoop loop runs in a goroutine.
type enqueueFakePool struct {
	mu          sync.Mutex
	zones       [][]any // the active zones reference table response (nil — no zones)
	dispatches  []*enqueueFakeRows
	updates     []*enqueueFakeRows
	dispatchErr error // a sticky selection error (zones/queues)
	updateErr   error // a sticky move error
	calls       []enqueueCall
	notify      chan struct{} // if set — a non-blocking signal on every Query
}

func (p *enqueueFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("enqueueFakePool: CopyFrom must not be called")
}

func (p *enqueueFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("enqueueFakePool: Exec must not be called")
}

func (p *enqueueFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	kind := enqueueKindDispatch
	switch {
	case strings.HasPrefix(sql, "WITH upd AS (UPDATE"):
		kind = enqueueKindUpdate
	case strings.HasPrefix(sql, "SELECT id, name FROM zones"):
		kind = enqueueKindZones
	}
	p.calls = append(p.calls, enqueueCall{kind: kind, sql: sql, args: args})
	if p.notify != nil {
		select {
		case p.notify <- struct{}{}:
		default:
		}
	}

	if kind == enqueueKindZones {
		if p.dispatchErr != nil {
			return nil, p.dispatchErr
		}
		// The reference table is small and stable — the same response on every
		// call (rows are not consumed, unlike the queues).
		return enqueueFakeRowsOf(p.zones...), nil
	}

	if kind == enqueueKindDispatch {
		if p.dispatchErr != nil {
			return nil, p.dispatchErr
		}
		if len(p.dispatches) > 0 {
			rows := p.dispatches[0]
			p.dispatches = p.dispatches[1:]
			return rows, nil
		}
		return enqueueFakeRowsOf(), nil // pending is empty
	}

	if p.updateErr != nil {
		return nil, p.updateErr
	}
	if len(p.updates) > 0 {
		rows := p.updates[0]
		p.updates = p.updates[1:]
		return rows, nil
	}
	// Default: no race — all batch keys are moved (args: []any zones,
	// []any identifiers; the db.Update builder columns are []any).
	zones := args[0].([]any)
	ids := args[1].([]any)
	rows := make([][]any, len(zones))
	for i := range zones {
		rows[i] = []any{zones[i], ids[i]}
	}
	return enqueueFakeRowsOf(rows...), nil
}

var _ db.DB = (*enqueueFakePool)(nil)

// enqueuePoolCalls — a snapshot of the call protocol.
func (p *enqueueFakePool) enqueuePoolCalls() []enqueueCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]enqueueCall, len(p.calls))
	copy(out, p.calls)
	return out
}

// enqueuePublish — a recorded publication attempt.
type enqueuePublish struct {
	stream string
	msg    queue.DNSBatch
	err    error // the error returned by Publish (nil — success)
}

// enqueueFakeQueue — a queue.Queue for enqueueing: Publish checks
// the context, remembers the message and stream, hands out pre-made errors one
// per call; Consume/Ack are not needed for enqueueing.
type enqueueFakeQueue struct {
	mu        sync.Mutex
	publishes []enqueuePublish
	errs      []error // the error queue (empty — success)
	notify    chan struct{}
	// onPublish — if set, called at the beginning of Publish under a
	// non-reentrant lock: the hook must not call this queue's methods —
	// a self-deadlock; the fakes' state is legally read only from the goroutine
	// that called EnqueueBatch (a recording seam of the order, T-224).
	onPublish func()
}

var _ queue.Queue = (*enqueueFakeQueue)(nil)

func (q *enqueueFakeQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.onPublish != nil {
		q.onPublish()
	}
	var err error
	if len(q.errs) > 0 {
		err = q.errs[0]
		q.errs = q.errs[1:]
	}
	m, _ := msg.(queue.DNSBatch)
	q.publishes = append(q.publishes, enqueuePublish{stream: stream, msg: m, err: err})
	if q.notify != nil {
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("id-%d", len(q.publishes)), nil
}

func (q *enqueueFakeQueue) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	return nil, errors.New("enqueueFakeQueue: Consume must not be called")
}

func (q *enqueueFakeQueue) Ack(context.Context, string, string, ...string) error {
	return errors.New("enqueueFakeQueue: Ack must not be called")
}

// enqueueQueuePublishes — a snapshot of publication attempts.
func (q *enqueueFakeQueue) enqueueQueuePublishes() []enqueuePublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]enqueuePublish, len(q.publishes))
	copy(out, q.publishes)
	return out
}

// enqueueWaitSignal waits for a signal with a timeout (a fake event or cancellation).
func enqueueWaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("did not wait out the event: %s", what)
	}
}

// enqueueWaitFor polls a condition until a timeout.
func enqueueWaitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not reached in %s: %s", timeout, what)
}

// --- EnqueueBatch ---------------------------------------------------------

// enqueueBatch3 — a batch of three domains of two zones (the selection order —
// the T-027 zone interleaving).
func enqueueBatch3() []DispatchRow {
	return []DispatchRow{
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 201, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 102, ZoneID: 1, Name: "c.example.com"},
	}
}

func TestEnqueueBatch(t *testing.T) {
	pool := &enqueueFakePool{} // a move without a race: all batch keys
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 3, Published: 3, Skipped: 0}) {
		t.Errorf("result = %+v, want {Queued:3 Published:3 Skipped:0}", res)
	}

	// One batch UPDATE with the pending and queued_at guard (the SQL contract —
	// TestEnqueue_SQLContract), the keys — parallel arrays of zones/domains.
	calls := pool.enqueuePoolCalls()
	if len(calls) != 1 || calls[0].kind != enqueueKindUpdate {
		t.Fatalf("DB calls = %v, want a single UPDATE", calls)
	}
	wantArgs := []any{
		[]any{int16(1), int16(2), int16(1)},
		[]any{int64(101), int64(201), int64(102)},
	}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("args = %v, want [zone_id[], domain_id[]] = %v", calls[0].args, wantArgs)
	}

	// Publication: one DNSBatch message to q:dns, the domains — the moved
	// rows in selection order (the name from the selection: label || '.' || zone).
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 {
		t.Fatalf("publications %d, want 1 (a batch is one message, spec 4.2)", len(pubs))
	}
	if pubs[0].stream != queue.StreamDNS {
		t.Errorf("stream = %q, want %q", pubs[0].stream, queue.StreamDNS)
	}
	wantDomains := []queue.DNSDomain{
		{ZoneID: 1, DomainID: 101, Name: "a.example.com"},
		{ZoneID: 2, DomainID: 201, Name: "b.example.net"},
		{ZoneID: 1, DomainID: 102, Name: "c.example.com"},
	}
	if !reflect.DeepEqual(pubs[0].msg.Domains, wantDomains) {
		t.Errorf("domains = %+v, want %+v (the selection order is preserved)", pubs[0].msg.Domains, wantDomains)
	}
}

// TestEnqueueBatch_RaceNotAllTranslated — a race: between the selection (T-027)
// and the move some rows left pending (a lease return T-029, a second
// scheduler). RETURNING gave only the moved ones — the publication is built
// exactly from them; the losers wait for the next cycle (at-least-once, spec 3.4).
func TestEnqueueBatch_RaceNotAllTranslated(t *testing.T) {
	// The first and third are moved (zone 1), the second (zone 2) — no longer pending.
	pool := &enqueueFakePool{
		updates: []*enqueueFakeRows{enqueueFakeRowsOf(
			[]any{int16(1), int64(101)},
			[]any{int16(1), int64(102)},
		)},
	}
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 2, Published: 2, Skipped: 1}) {
		t.Errorf("result = %+v, want {Queued:2 Published:2 Skipped:1}", res)
	}

	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 {
		t.Fatalf("publications %d, want 1", len(pubs))
	}
	want := []queue.DNSDomain{
		{ZoneID: 1, DomainID: 101, Name: "a.example.com"},
		{ZoneID: 1, DomainID: 102, Name: "c.example.com"},
	}
	if !reflect.DeepEqual(pubs[0].msg.Domains, want) {
		t.Errorf("domains = %+v, want %+v — an unmoved row is not published", pubs[0].msg.Domains, want)
	}
}

// TestEnqueueBatch_RaceAllLost — the whole batch lost the race: no move,
// no publication at all (an empty DNSBatch is forbidden by the queue).
func TestEnqueueBatch_RaceAllLost(t *testing.T) {
	pool := &enqueueFakePool{updates: []*enqueueFakeRows{enqueueFakeRowsOf()}}
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 0, Published: 0, Skipped: 3}) {
		t.Errorf("result = %+v, want {Queued:0 Published:0 Skipped:3}", res)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications %d, want 0", len(pubs))
	}
}

// TestEnqueueBatch_DuplicateKeys — a duplicate key in the batch is published
// once (the key is consumed at the first match).
func TestEnqueueBatch_DuplicateKeys(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}
	batch := []DispatchRow{
		{DomainID: 1, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 1, ZoneID: 1, Name: "a.example.com"},
	}

	res, err := EnqueueBatch(context.Background(), pool, q, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 1, Published: 1, Skipped: 1}) {
		t.Errorf("result = %+v, want {Queued:1 Published:1 Skipped:1}", res)
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 || len(pubs[0].msg.Domains) != 1 {
		t.Fatalf("publications = %+v, want one domain", pubs)
	}
}

// TestEnqueueBatch_EmptyBatch — an empty batch: neither the DB nor the queue.
func TestEnqueueBatch_EmptyBatch(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, nil)
	if err != nil || res != (EnqueueResult{}) {
		t.Fatalf("(%+v, %v), want ({0 0 0}, nil)", res, err)
	}
	if calls := pool.enqueuePoolCalls(); len(calls) != 0 {
		t.Errorf("DB calls = %d, want 0", len(calls))
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications = %d, want 0", len(pubs))
	}
}

// TestEnqueueBatch_ValidationNumbers — the domain number in the error message is
// 1-based: an invalid second batch row is named "domain #2".
func TestEnqueueBatch_ValidationNumbers(t *testing.T) {
	valid := DispatchRow{ZoneID: 1, DomainID: 1, Name: "ok.example.com"}
	cases := []struct {
		name string
		row  DispatchRow
		want string
	}{
		{"zone 0", DispatchRow{DomainID: 2, Name: "b.example.com"}, "domain #2: zone_id 0 ≤ 0"},
		{"domain 0", DispatchRow{ZoneID: 2, Name: "b.example.com"}, "domain #2: domain_id 0 ≤ 0"},
		{"empty name", DispatchRow{ZoneID: 1, DomainID: 2}, "domain #2: empty name"},
		{"a long name", DispatchRow{ZoneID: 1, DomainID: 2, Name: strings.Repeat("a", 254)}, "domain #2: name longer than 253 octets"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &enqueueFakePool{}
			_, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, []DispatchRow{valid, tc.row})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want the message with %q (the row number is 1-based)", err, tc.want)
			}
			if calls := pool.enqueuePoolCalls(); len(calls) != 0 {
				t.Errorf("the invalid batch reached the DB: %v", calls)
			}
		})
	}
}

// TestEnqueueBatch_NameLenBoundary — a name of exactly 253 octets (the spec 5.2
// item 4 limit) is valid: the boundary is strict, 253 passes (254 — no, the table above).
func TestEnqueueBatch_NameLenBoundary(t *testing.T) {
	name := strings.Repeat("a", 249) + ".com"
	if len(name) != enqueueMaxNameLen {
		t.Fatalf("the boundary name = %d octets, want %d", len(name), enqueueMaxNameLen)
	}
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, []DispatchRow{{ZoneID: 1, DomainID: 7, Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 1, Published: 1, Skipped: 0}) {
		t.Errorf("result = %+v, want {Queued:1 Published:1 Skipped:0}", res)
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 || len(pubs[0].msg.Domains) != 1 || pubs[0].msg.Domains[0].Name != name {
		t.Errorf("publications = %+v, want one with the boundary name", pubs)
	}
}

// TestEnqueueBatch_Validation — a nil pool/queue and invalid rows
// are rejected before touching the DB: the q:dns publication would reject such
// values, and the move would already be committed by then.
func TestEnqueueBatch_Validation(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}

	if _, err := EnqueueBatch(context.Background(), nil, q, enqueueBatch3()); err == nil || !strings.Contains(err.Error(), "pool not set") {
		t.Errorf("nil pool: err = %v, want \"pool not set\"", err)
	}
	if _, err := EnqueueBatch(context.Background(), pool, nil, enqueueBatch3()); err == nil || !strings.Contains(err.Error(), "queue not set") {
		t.Errorf("nil queue: err = %v, want \"queue not set\"", err)
	}

	cases := []struct {
		name string
		row  DispatchRow
		want string
	}{
		{"zone 0", DispatchRow{DomainID: 1, Name: "a.example.com"}, "zone_id 0"},
		{"domain 0", DispatchRow{ZoneID: 1, Name: "a.example.com"}, "domain_id 0"},
		{"empty name", DispatchRow{ZoneID: 1, DomainID: 1}, "empty name"},
		{"a long name", DispatchRow{ZoneID: 1, DomainID: 1, Name: strings.Repeat("a", 254) + ".com"}, "longer than 253"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &enqueueFakePool{}
			_, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, []DispatchRow{{ZoneID: 1, DomainID: 1, Name: "ok.example.com"}, tc.row})
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "scheduler:") {
				t.Fatalf("err = %v, want the description with %q and the \"scheduler:\" context", err, tc.want)
			}
			if calls := pool.enqueuePoolCalls(); len(calls) != 0 {
				t.Errorf("the invalid batch reached the DB: %v", calls)
			}
		})
	}
}

// TestEnqueueBatch_UpdateError — a move error: nothing published,
// the error wrapped with the task context.
func TestEnqueueBatch_UpdateError(t *testing.T) {
	boom := errors.New("the db went to lunch")
	pool := &enqueueFakePool{updateErr: boom}
	q := &enqueueFakeQueue{}

	_, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want \"the db went to lunch\" wrapped", err)
	}
	if !strings.Contains(err.Error(), "scheduler: batch enqueue") {
		t.Errorf("message = %q, want the \"scheduler: batch enqueue\" context", err)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications = %d, want 0 (the move did not happen)", len(pubs))
	}
}

// TestEnqueueBatch_ReadErrors — a RETURNING read error (Scan and Err)
// interrupts the enqueueing: publishing from an incomplete list of moved rows
// would lose domains that got into queued.
func TestEnqueueBatch_ReadErrors(t *testing.T) {
	boom := errors.New("the decoding failed")

	// The cursor is closed on the error read paths too (T-224: an explicit
	// Close on every branch) — references to the fake's rows pin this.
	scanRows := &enqueueFakeRows{rows: [][]any{{int16(1), int64(101)}, {int16(1), int64(102)}}, scanErr: boom}
	pool := &enqueueFakePool{updates: []*enqueueFakeRows{scanRows}}
	_, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, enqueueBatch3()[:2])
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "reading moved rows") {
		t.Fatalf("err = %v, want the wrapped RETURNING read error", err)
	}
	if !scanRows.closed {
		t.Error("the RETURNING cursor is not closed on the Scan error path (T-224)")
	}

	errRows := &enqueueFakeRows{rows: [][]any{{int16(1), int64(101)}}, err: boom}
	pool = &enqueueFakePool{updates: []*enqueueFakeRows{errRows}}
	_, err = EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, enqueueBatch3()[:2])
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the iteration error (rows.Err)", err)
	}
	if !errRows.closed {
		t.Error("the RETURNING cursor is not closed on the rows.Err error path (T-224)")
	}
}

// TestEnqueueBatch_PublishError — a publication failure: the move is already committed
// and not rolled back (the enqueue.go comment: XADD may have succeeded; a rollback would give
// duplicates in progress) — the outcome returns Queued; the rows await the lease return
// (T-029); the publication attempt is one.
func TestEnqueueBatch_PublishError(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{errs: []error{errors.New("redis is down")}}

	res, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err == nil || !strings.Contains(err.Error(), "q:dns") {
		t.Fatalf("err = %v, want a publish error with the q:dns context", err)
	}
	if res.Queued != 3 || res.Published != 0 {
		t.Errorf("result = %+v, want {Queued:3 Published:0} — the move is committed, there is no publication", res)
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 || pubs[0].err == nil {
		t.Fatalf("publications = %+v, want one failed attempt", pubs)
	}
}

// TestEnqueueBatch_ClosesRowsBeforePublish — the RETURNING cursor is closed before
// publication (T-224): q.Publish is network I/O to Redis (XADD with retries and
// pauses up to ~0.75 s on failures, redisq.go); an open cursor would hold
// a pool connection for all that time — with a small connection budget
// the scheduler hits the pool, not the queue speed. The pool is replaced
// by a fake (unit tests without a live DB, the file style); a deadlock on pgxpool
// MaxConns=1 cannot be built here: the test pins the Close→Publish order invariant
// with a recording seam — onPublish runs inside Publish
// and sees rows.closed; before the fix (defer rows.Close on function exit)
// Publish entered with an open cursor and the test failed.
func TestEnqueueBatch_ClosesRowsBeforePublish(t *testing.T) {
	for _, tc := range []struct {
		name string
		errs []error // a publication error (nil — success): the cursor must be closed here too
	}{
		{"a successful publication", nil},
		{"a publication failure", []error{errors.New("redis is down")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := enqueueFakeRowsOf(
				[]any{int16(1), int64(101)},
				[]any{int16(1), int64(102)},
			)
			pool := &enqueueFakePool{updates: []*enqueueFakeRows{rows}}
			q := &enqueueFakeQueue{errs: tc.errs}
			q.onPublish = func() {
				if !rows.closed {
					t.Error("q.Publish runs before rows.Close() — the cursor holds the pool connection for the duration of the queue's network I/O (T-224)")
				}
			}

			_, _ = EnqueueBatch(context.Background(), pool, q, enqueueBatch3()[:2])
			if !rows.closed {
				t.Error("the RETURNING cursor is not closed after the enqueue finishes")
			}
		})
	}

	// No publication at all (the whole batch lost the race) — the cursor is still
	// closed: the exit path without a queue must not leak a connection.
	rows := enqueueFakeRowsOf()
	pool := &enqueueFakePool{updates: []*enqueueFakeRows{rows}}
	if _, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, enqueueBatch3()); err != nil {
		t.Fatal(err)
	}
	if !rows.closed {
		t.Error("the RETURNING cursor is not closed on the no-publication path (the batch lost the race)")
	}
}

// TestEnqueueBatch_ContextCanceled — a cancelled context is refused before
// the calls (like real pgx and go-redis).
func TestEnqueueBatch_ContextCanceled(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := EnqueueBatch(ctx, pool, q, enqueueBatch3())
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls := pool.enqueuePoolCalls(); len(calls) != 0 {
		t.Errorf("DB calls = %d, want 0", len(calls))
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications = %d, want 0", len(pubs))
	}
}

// ------------------------------------------------------------ SQL contract

// wantEnqueueUpdateSQL — the batch move + zone counter deltas in one
// statement (T-178): the upd CTE — the move (the former single T-028 statement),
// the zc CTE — pending −n / queued +n over the zones of the moved rows; the outer
// SELECT — the keys of the moved rows for publication.
const wantEnqueueUpdateSQL = `WITH upd AS (UPDATE "domains" AS t ` +
	`SET check_state = 1, queued_at = now(), flags = (t.flags & -5)::int2 ` +
	`FROM unnest($1::int2[], $2::int8[]) AS u("zone_id", "id") ` +
	`WHERE t."zone_id" = u."zone_id" AND t."id" = u."id" AND t.check_state = 0 AND t.status <> 6 ` +
	`RETURNING t.zone_id, t.id), ` +
	`zc AS (UPDATE zones AS z ` +
	`SET domains_pending = z.domains_pending - c.n, ` +
	`domains_queued = z.domains_queued + c.n ` +
	`FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c ` +
	`WHERE z.id = c.zone_id) ` +
	`SELECT u.zone_id, u.id FROM upd AS u`

// TestEnqueue_SQLContract — the exact move text: keys by the domains PK,
// check_state as a literal of the queued code, queued_at = now() (the DB server, a single
// lease moment of the batch), clearing the http_published bit (T-108: the enqueue is
// the single entry point into a new episode; the previous episode's bit must not
// suppress the fresh one's publication), the pending guard, RETURNING of the keys
// of the rows actually moved; the zone counter deltas — by the same
// statement (T-178). The literals are tied to the model codes and the
// schema of migrations 000001/000016 (queued_at, the PK, the counter columns).
func TestEnqueue_SQLContract(t *testing.T) {
	pool := &enqueueFakePool{}
	if _, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, enqueueBatch3()[:1]); err != nil {
		t.Fatal(err)
	}
	calls := pool.enqueuePoolCalls()
	if len(calls) != 1 || calls[0].sql != wantEnqueueUpdateSQL {
		t.Fatalf("SQL = %q,\nwant %q", calls[0].sql, wantEnqueueUpdateSQL)
	}

	// The text literals must follow the model codes (spec 6.1): when
	// the codes change the test catches the desync; the bit-clearing mask —
	// the signed ~int16(FlagHTTPPublished) (T-108).
	if int16(model.CheckStateQueued) != 1 || int16(model.CheckStatePending) != 0 {
		t.Errorf("the model codes changed (pending=%d queued=%d): update the literals in wantEnqueueUpdateSQL",
			int16(model.CheckStatePending), int16(model.CheckStateQueued))
	}
	if int16(^int16(model.FlagHTTPPublished)) != -5 {
		t.Errorf("the http_published clear mask = %d, want -5: update the literals in wantEnqueueUpdateSQL",
			int16(^int16(model.FlagHTTPPublished)))
	}

	up := dispatchReadMigration(t, "000001_init.up.sql")

	// queued_at — a lease column (spec 6.1); the UPDATE references the schema.
	if !strings.Contains(up, "queued_at") {
		t.Error("migration 000001 has no domains.queued_at — the lease stamp references the schema")
	}
	// The batch keys — the table's PK: the UPDATE addresses rows by the (zone_id, id) pair.
	if !strings.Contains(up, "PRIMARY KEY (zone_id, id)") {
		t.Error("migration 000001 has no domains PK (zone_id, id) — the move keys must match it")
	}
	// The zone counter columns (T-178): the zc deltas write domains_pending/
	// domains_queued — they must exist (migration 000016).
	mig := dispatchReadMigration(t, "000016_zone_pipeline_counters.up.sql")
	for _, col := range []string{"domains_pending", "domains_queued"} {
		if !strings.Contains(mig, col) {
			t.Errorf("migration 000016 has no zones.%s — the move deltas reference the column", col)
		}
	}
}

// TestEnqueueDefaults — loop defaults pinned by literals (spec 5.10 and
// the constants' comments): a desync is caught by the test, not by review.
func TestEnqueueDefaults(t *testing.T) {
	if DefaultEnqueueBatch != 1000 {
		t.Errorf("DefaultEnqueueBatch = %d, want 1000 (mirrors the scheduler.batch_size default, T-141)", DefaultEnqueueBatch)
	}
	if DefaultEnqueueInterval != time.Second {
		t.Errorf("DefaultEnqueueInterval = %v, want 1s", DefaultEnqueueInterval)
	}
	if DefaultEnqueueIdleWait != 30*time.Second {
		t.Errorf("DefaultEnqueueIdleWait = %v, want 30s", DefaultEnqueueIdleWait)
	}
}

// TestEnqueueOptionsWithDefaults — withDefaults replaces only
// non-positive values: BatchSize = 1 and 1 ns pauses are valid (they stay
// as they are); zero and negative — the defaults.
func TestEnqueueOptionsWithDefaults(t *testing.T) {
	cases := []struct {
		name         string
		in           EnqueueOptions
		wantBatch    int
		wantInterval time.Duration
		wantIdleWait time.Duration
	}{
		{"zeros", EnqueueOptions{}, DefaultEnqueueBatch, DefaultEnqueueInterval, DefaultEnqueueIdleWait},
		{"negatives", EnqueueOptions{BatchSize: -5, Interval: -time.Second, IdleWait: -time.Minute},
			DefaultEnqueueBatch, DefaultEnqueueInterval, DefaultEnqueueIdleWait},
		{"batch 1 is valid", EnqueueOptions{BatchSize: 1}, 1, DefaultEnqueueInterval, DefaultEnqueueIdleWait},
		{"interval 1ns is valid", EnqueueOptions{Interval: 1}, DefaultEnqueueBatch, 1, DefaultEnqueueIdleWait},
		{"idle wait 1ns is valid", EnqueueOptions{IdleWait: 1}, DefaultEnqueueBatch, DefaultEnqueueInterval, 1},
		{"explicit values", EnqueueOptions{BatchSize: 10, Interval: time.Minute, IdleWait: 5 * time.Second},
			10, time.Minute, 5 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.in.withDefaults()
			if got.BatchSize != tc.wantBatch || got.Interval != tc.wantInterval || got.IdleWait != tc.wantIdleWait {
				t.Errorf("withDefaults() = {BatchSize:%d Interval:%v IdleWait:%v}, want {%d %v %v}",
					got.BatchSize, got.Interval, got.IdleWait, tc.wantBatch, tc.wantInterval, tc.wantIdleWait)
			}
		})
	}
}

// --- EnqueueLoop ------------------------------------------------------------

// enqueueLoopRun starts the loop in a goroutine and returns the result channel.
func enqueueLoopRun(ctx context.Context, pool *enqueueFakePool, q *enqueueFakeQueue, o EnqueueOptions) <-chan error {
	done := make(chan error, 1)
	go func() { done <- EnqueueLoop(ctx, pool, q, o) }()
	return done
}

// TestEnqueueLoop — a service smoke on fakes: the circle "selection → move →
// q:dns publication", then a pause; ctx cancellation — a clean nil (the OnRun contract,
// T-012). Long pauses: after the first batch the loop is guaranteed
// to wait — the test is deterministic.
func TestEnqueueLoop(t *testing.T) {
	pool := &enqueueFakePool{
		zones: [][]any{
			{int16(1), "example.com"},
			{int16(2), "example.net"},
		},
		dispatches: []*enqueueFakeRows{enqueueFakeRowsOf(
			[]any{int16(1), int64(101), "a"},
			[]any{int16(2), int64(201), "b"},
		)},
		notify: make(chan struct{}, 8),
	}
	q := &enqueueFakeQueue{notify: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{
		BatchSize: 1000,
		Interval:  time.Hour, // one batch — then a pause until cancellation
		IdleWait:  time.Hour,
	})

	enqueueWaitSignal(t, pool.notify, "batch selection")
	enqueueWaitSignal(t, pool.notify, "batch move")
	enqueueWaitSignal(t, q.notify, "q:dns publication")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil (a ctx cancellation is a clean exit)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}

	calls := pool.enqueuePoolCalls()
	if len(calls) != 3 || calls[0].kind != enqueueKindZones ||
		calls[1].kind != enqueueKindDispatch || calls[2].kind != enqueueKindUpdate {
		t.Fatalf("calls = %v, want zones, the selection, then the move", calls)
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 {
		t.Fatalf("publications %d, want 1", len(pubs))
	}
	want := []queue.DNSDomain{
		{ZoneID: 1, DomainID: 101, Name: "a.example.com"},
		{ZoneID: 2, DomainID: 201, Name: "b.example.net"},
	}
	if !reflect.DeepEqual(pubs[0].msg.Domains, want) {
		t.Errorf("domains = %+v, want %+v", pubs[0].msg.Domains, want)
	}
}

// TestEnqueueLoop_EmptyBatchWaits — an empty selection: no publications, the loop
// waits for replenishment (recheck T-030 / retry T-031 / lease T-029 / ingest),
// cancellation interrupts the wait.
func TestEnqueueLoop_EmptyBatchWaits(t *testing.T) {
	pool := &enqueueFakePool{notify: make(chan struct{}, 8)} // pending is empty
	q := &enqueueFakeQueue{notify: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{
		Interval: time.Hour,
		IdleWait: time.Hour,
	})

	enqueueWaitSignal(t, pool.notify, "selection (empty)")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}
	if calls := pool.enqueuePoolCalls(); len(calls) != 1 || calls[0].kind != enqueueKindZones {
		t.Errorf("calls = %v, want a single zones query without queue selection and the move", calls)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications = %d, want 0", len(pubs))
	}
}

// TestEnqueueLoop_DispatchError — a selection error does not stop the loop:
// a Warn in the log, a pause, a new circle; the exit is only by ctx cancellation.
func TestEnqueueLoop_DispatchError(t *testing.T) {
	pool := &enqueueFakePool{
		dispatchErr: errors.New("the db blipped"),
		notify:      make(chan struct{}, 8),
	}
	q := &enqueueFakeQueue{notify: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{Interval: time.Hour, IdleWait: time.Hour})
	enqueueWaitSignal(t, pool.notify, "a failed selection")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil (a step error does not stop the service)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications = %d, want 0", len(pubs))
	}
}

// TestEnqueueLoop_PublishErrorContinues — a publication failure: the moved
// rows stay in queued (lease will return them, T-029); the loop after the pause
// keeps working and publishes the following batches.
func TestEnqueueLoop_PublishErrorContinues(t *testing.T) {
	// The fakes keep a read cursor — each selection gets its own instance of rows
	// (the zone queue columns: zone_id, id, label — the name is assembled by the selection).
	batch := func() *enqueueFakeRows {
		return enqueueFakeRowsOf(
			[]any{int16(1), int64(101), "a"},
			[]any{int16(2), int64(201), "b"},
		)
	}
	pool := &enqueueFakePool{
		zones: [][]any{
			{int16(1), "example.com"},
			{int16(2), "example.net"},
		},
		dispatches: []*enqueueFakeRows{batch(), batch()},
		notify:     make(chan struct{}, 64),
	}
	q := &enqueueFakeQueue{
		errs:   []error{errors.New("redis blipped")},
		notify: make(chan struct{}, 64),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{
		Interval: time.Millisecond,
		IdleWait: time.Millisecond,
	})

	// The first batch — a failed publication; the second — successful: the loop lives.
	enqueueWaitFor(t, 5*time.Second, func() bool {
		return len(q.enqueueQueuePublishes()) >= 2
	}, "two publication attempts (after the error the loop continued)")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) < 2 || pubs[0].err == nil || pubs[1].err != nil {
		t.Fatalf("publications = %+v, want the first failed, the second successful", pubs)
	}
}

// TestEnqueueLoop_Validation — a nil pool/queue: an error before the loop starts
// (the service assembly fails clearly, not silently).
func TestEnqueueLoop_Validation(t *testing.T) {
	if err := EnqueueLoop(context.Background(), nil, &enqueueFakeQueue{}, EnqueueOptions{}); err == nil || !strings.Contains(err.Error(), "pool not set") {
		t.Errorf("nil pool: err = %v, want \"pool not set\"", err)
	}
	if err := EnqueueLoop(context.Background(), &enqueueFakePool{}, nil, EnqueueOptions{}); err == nil || !strings.Contains(err.Error(), "queue not set") {
		t.Errorf("nil queue: err = %v, want \"queue not set\"", err)
	}
}

// TestEnqueueLoop_DispatchErrorWaits — after a selection error the loop goes into
// the IdleWait pause instead of hammering the DB with idle circles: until cancellation —
// exactly one attempt.
func TestEnqueueLoop_DispatchErrorWaits(t *testing.T) {
	pool := &enqueueFakePool{
		zones:       [][]any{{int16(1), "example.com"}},
		dispatchErr: errors.New("the db blipped"),
		notify:      make(chan struct{}, 8),
	}
	q := &enqueueFakeQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{Interval: time.Hour, IdleWait: time.Hour})
	enqueueWaitSignal(t, pool.notify, "a failed selection")
	time.Sleep(100 * time.Millisecond) // the loop is in the IdleWait pause; without it — a stream of queries

	if calls := pool.enqueuePoolCalls(); len(calls) != 1 || calls[0].kind != enqueueKindZones {
		t.Fatalf("calls = %v, want a single selection attempt (an IdleWait pause after the error)", calls)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications %d, want 0", len(pubs))
	}
}

// TestEnqueueLoop_EmptyBatchWaitsStrict — an empty batch is exactly a wait
// for replenishment of IdleWait, not a run of enqueueing an empty batch with the
// Interval pause: until cancellation — one selection, no idle loop.
func TestEnqueueLoop_EmptyBatchWaitsStrict(t *testing.T) {
	pool := &enqueueFakePool{notify: make(chan struct{}, 8)} // pending is empty
	q := &enqueueFakeQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{Interval: time.Millisecond, IdleWait: time.Hour})
	enqueueWaitSignal(t, pool.notify, "an empty selection")
	time.Sleep(100 * time.Millisecond)

	if calls := pool.enqueuePoolCalls(); len(calls) != 1 || calls[0].kind != enqueueKindZones {
		t.Fatalf("calls = %v, want a single selection (an empty batch waits for replenishment)", calls)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications %d, want 0", len(pubs))
	}
}

// TestEnqueueLoop_SingleRowBatchEnqueued — a batch of one domain is not
// "empty": the move and publication run.
func TestEnqueueLoop_SingleRowBatchEnqueued(t *testing.T) {
	pool := &enqueueFakePool{
		zones: [][]any{{int16(1), "example.com"}},
		dispatches: []*enqueueFakeRows{enqueueFakeRowsOf(
			[]any{int16(1), int64(101), "a"},
		)},
		notify: make(chan struct{}, 8),
	}
	q := &enqueueFakeQueue{notify: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{Interval: time.Hour, IdleWait: time.Hour})
	enqueueWaitFor(t, 5*time.Second, func() bool {
		return len(q.enqueueQueuePublishes()) >= 1
	}, "publication of a single-domain batch")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 || len(pubs[0].msg.Domains) != 1 ||
		pubs[0].msg.Domains[0] != (queue.DNSDomain{ZoneID: 1, DomainID: 101, Name: "a.example.com"}) {
		t.Errorf("publications = %+v, want the single domain a.example.com", pubs)
	}
}

// TestEnqueueLoop_EnqueueErrorWaits — after an enqueue error (the move
// failed) the loop goes into the IdleWait pause: until cancellation — one full circle
// (zones, selection, move) without new calls.
func TestEnqueueLoop_EnqueueErrorWaits(t *testing.T) {
	pool := &enqueueFakePool{
		zones: [][]any{{int16(1), "example.com"}},
		dispatches: []*enqueueFakeRows{enqueueFakeRowsOf(
			[]any{int16(1), int64(101), "a"},
			[]any{int16(2), int64(201), "b"},
		)},
		updateErr: errors.New("the update failed"),
		notify:    make(chan struct{}, 16),
	}
	q := &enqueueFakeQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{Interval: time.Millisecond, IdleWait: time.Hour})
	enqueueWaitSignal(t, pool.notify, "zones")
	enqueueWaitSignal(t, pool.notify, "selection")
	enqueueWaitSignal(t, pool.notify, "the move (failed)")
	time.Sleep(100 * time.Millisecond)

	calls := pool.enqueuePoolCalls()
	wantKinds := []enqueueCallKind{enqueueKindZones, enqueueKindDispatch, enqueueKindUpdate}
	if len(calls) != len(wantKinds) {
		t.Fatalf("calls = %d (%v), want %d: after an enqueue error — an IdleWait pause", len(calls), calls, len(wantKinds))
	}
	for i, kind := range wantKinds {
		if calls[i].kind != kind {
			t.Fatalf("call[%d] = %s, want %s", i, calls[i].kind, kind)
		}
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil (a step error does not stop the service)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications %d, want 0", len(pubs))
	}
}

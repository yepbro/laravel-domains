// Unit tests of re-analysis (T-074, spec 5.8 "Re-analysis", §11.4 item
// 4) on fake pool/queue/rule source — no DB, Redis or filesystem.
// Checked: the exact SQL text and parameters of the selects of both
// forms (the drain PK form: IS DISTINCT FROM, the §13.1 deleted guard,
// the zone filter, the batch cursor, LIMIT; the T-193 tick time form:
// the composite (checked_at, PK) cursor, ORDER BY by the same key, the
// now−lookback anchor), the fake's live filter semantics, drain
// batching without repeats, the tick cursor between passes (advanced by
// a batch, an empty batch does not move it), the empty set, repeat pass
// idempotency, errors and ctx cancellation, the loop (the startup
// drain in the PK form, a Warn with continuation, the tick pass in the
// time form over a fresh slice, an out-of-turn drain on a rule change,
// a clean exit). Behavior against live PostgreSQL/Redis is covered by the stage 7 integration tests (test/integration/reanalyze_test.go).
package analyzer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	"crawler/internal/rules"
)

// --- fake queue ------------------------------------------------------------

// reanPublish is a recorded publish.
type reanPublish struct {
	stream string
	msg    queue.Result
}

// reanQueue is a queue.Queue: it records Result publishes into
// q:analyze (failures by index), Consume parks until ctx is cancelled, Ack is a stub.
type reanQueue struct {
	errs []error // the publish error by index (nil — success)

	mu        sync.Mutex
	publishes []reanPublish
}

var _ queue.Queue = (*reanQueue)(nil)

func (q *reanQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	res, ok := msg.(queue.Result)
	if !ok || stream != queue.StreamAnalyze {
		return "", fmt.Errorf("reanQueue: publishing %T into %s (want queue.Result into %s)",
			msg, stream, queue.StreamAnalyze)
	}
	q.mu.Lock()
	i := len(q.publishes)
	q.publishes = append(q.publishes, reanPublish{stream: stream, msg: res})
	q.mu.Unlock()
	if i < len(q.errs) && q.errs[i] != nil {
		return "", q.errs[i]
	}
	return fmt.Sprintf("entry-%d", i+1), nil
}

func (q *reanQueue) Consume(ctx context.Context, _, _, _ string, _ int) ([]queue.Delivery, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *reanQueue) Ack(context.Context, string, string, ...string) error { return nil }

// snapshot / count — snapshots under the lock (the loop runs in a
// goroutine, the test waits for progress by the counter).
func (q *reanQueue) snapshot() []reanPublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]reanPublish, len(q.publishes))
	copy(out, q.publishes)
	return out
}

func (q *reanQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.publishes)
}

// --- fake pool (db.DB) -----------------------------------------------------

// reanCall is a recorded Query call with arguments.
type reanCall struct {
	sql  string
	args []any
}

// reanDomKey is the domains row key in the fake.
type reanDomKey struct {
	zone int16
	id   int64
}

// reanDomain is a domains row the select sees (the JOIN).
type reanDomain struct {
	label  string
	status model.Status
}

// reanRow is a subdomain_results row: the key, rules_version (nil —
// NULL, never analyzed) and checked_at (zero — only for PK-form rows;
// the time form seeds via seedRowAt).
type reanRow struct {
	key       resultKey
	version   *int32
	checkedAt time.Time
}

// reanPool is a db.DB without a DB: the live semantics of the
// re-analysis select (the domains/zones JOIN, IS DISTINCT FROM, status
// <> deleted, the zone filter, a row-value > cursor, ORDER BY by the
// key, LIMIT), the tail buckets guard (T-204: the sum of mature buckets
// + the hour bound) and the zone lookup by name; SQL is matched
// EXACTLY (an unexpected one is an error, the test fails loudly).
// The *ScanErr/*RowsErr fields are T-M10 row Scan/Err failure injections.
type reanPool struct {
	zones      map[int16]string
	zoneByName map[string]int16
	domains    map[reanDomKey]*reanDomain
	rows       []reanRow

	// guardTails is the guard's answer (T-204): the sum of tail buckets
	// older than the current hour; 1 by default — "mature tails exist",
	// the time-form passes run (the pass tests), 0/negative — the guard is closed.
	// guardHour is the current hour bound from the "DB" (the cursor jump target).
	guardTails int64
	guardHour  time.Time
	guardEmpty bool // an empty aggregate response — a protocol break (T-M10)

	selectScanErr error
	selectRowsErr error
	zoneScanErr   error
	zoneRowsErr   error

	// Injections of the 11/09/2026 mutant triage: the guard aggregate and
	// tick select failures separately (queryErr hits the very first select).
	guardScanErr error
	guardRowsErr error
	tickQueryErr error
	tickScanErr  error
	tickRowsErr  error

	mu       sync.Mutex
	queryErr error // a configurable failure of any select
	queries  []reanCall
}

var _ db.DB = (*reanPool)(nil)

func newReanPool() *reanPool {
	return &reanPool{
		zones:      make(map[int16]string),
		zoneByName: make(map[string]int16),
		domains:    make(map[reanDomKey]*reanDomain),
		guardTails: 1, // the guard is open: time-form passes run (T-204)
	}
}

func (p *reanPool) seedZone(id int16, name string) {
	p.zones[id] = name
	p.zoneByName[name] = id
}

func (p *reanPool) seedDomain(zone int16, id int64, label string, status model.Status) {
	p.domains[reanDomKey{zone: zone, id: id}] = &reanDomain{label: label, status: status}
}

func (p *reanPool) seedRow(zone int16, domain int64, sub int16, variant model.Variant, version *int32) {
	p.rows = append(p.rows, reanRow{key: resultKey{zone: zone, domain: domain, sub: sub, variant: variant}, version: version})
}

// seedRowAt is a row with a checked_at mark (the time form, T-193):
// the tick cursor and order go by (checked_at, PK). Under the lock:
// the loop tests seed fresh episodes into a running goroutine (the T-193 loop tests).
func (p *reanPool) seedRowAt(zone int16, domain int64, sub int16, variant model.Variant, version *int32, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows = append(p.rows, reanRow{
		key:       resultKey{zone: zone, domain: domain, sub: sub, variant: variant},
		version:   version,
		checkedAt: at,
	})
}

// markAnalyzed imitates the T-071 worker: every row gets the current
// rule set version (the rules_version commit per a processed message).
func (p *reanPool) markAnalyzed(version int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.rows {
		v := version
		p.rows[i].version = &v
	}
}

func (p *reanPool) setQueryErr(err error) {
	p.mu.Lock()
	p.queryErr = err
	p.mu.Unlock()
}

func (p *reanPool) calls() []reanCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]reanCall(nil), p.queries...)
}

func (p *reanPool) queryCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

func (p *reanPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("reanPool: CopyFrom must not be called")
}

func (p *reanPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("reanPool: Exec must not be called")
}

func (p *reanPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, reanCall{sql: sql, args: args})
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	switch sql {
	case reanalyzeSelectSQL:
		return p.selectRows(args)
	case reanalyzeTickSQL:
		if p.tickQueryErr != nil {
			return nil, p.tickQueryErr
		}
		return p.tickSelectRows(args)
	case reanalyzeGuardSQL:
		if len(args) != 0 {
			return nil, fmt.Errorf("reanPool: the bucket guard: %d arguments, want 0", len(args))
		}
		if p.guardEmpty {
			// A protocol break (the aggregate must be a row): an empty set;
			// the Err injection is "a break BEFORE the row", not an "empty response".
			return &reanRows{err: p.guardRowsErr}, nil
		}
		hour := p.guardHour
		if hour.IsZero() {
			hour = time.Now().Truncate(time.Hour)
		}
		return &reanRows{rows: [][]any{{p.guardTails, hour}},
			scanErr: p.guardScanErr, err: p.guardRowsErr}, nil
	case zoneIDByNameSQL:
		if len(args) != 1 {
			return nil, fmt.Errorf("reanPool: zone by name: %d arguments, want 1", len(args))
		}
		name, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("reanPool: zone by name: the argument %T, want string", args[0])
		}
		id, found := p.zoneByName[name]
		if !found {
			return &reanRows{err: p.zoneRowsErr}, nil // an empty response (with the Err injection)
		}
		return &reanRows{rows: [][]any{{id}}, scanErr: p.zoneScanErr, err: p.zoneRowsErr}, nil
	default:
		return nil, fmt.Errorf("reanPool: unexpected SQL Query: %s", sql)
	}
}

// selectRows applies the reanalyzeSelectSQL semantics to the seeds.
func (p *reanPool) selectRows(args []any) (pgx.Rows, error) {
	if len(args) != 8 {
		return nil, fmt.Errorf("reanPool: the select: %d arguments, want 8", len(args))
	}
	version, okV := args[0].(int32)
	deleted, okD := args[1].(int16)
	if !okV || !okD {
		return nil, fmt.Errorf("reanPool: the select: version %T (%v), status %T (%v)", args[0], args[0], args[1], args[1])
	}
	if deleted != int16(model.StatusDeleted) {
		return nil, fmt.Errorf("reanPool: the select: the status guard %d, want the deleted code %d (§13.1)",
			deleted, model.StatusDeleted)
	}
	var zone *int16
	if z, ok := args[2].(int16); ok {
		zone = &z
	} else if args[2] != nil {
		return nil, fmt.Errorf("reanPool: the select: zone %T (%v), want int16 or nil", args[2], args[2])
	}
	cz, ok1 := args[3].(int16)
	cd, ok2 := args[4].(int64)
	cs, ok3 := args[5].(int16)
	cv, ok4 := args[6].(int16)
	limit, ok5 := args[7].(int)
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return nil, fmt.Errorf("reanPool: the select: cursor/LIMIT %v", args[3:])
	}
	if limit <= 0 {
		return nil, fmt.Errorf("reanPool: the select: LIMIT %d ≤ 0", limit)
	}
	cursor := resultKey{zone: cz, domain: cd, sub: cs, variant: model.Variant(cv)}

	var sel []reanRow
	for _, r := range p.rows {
		d, ok := p.domains[reanDomKey{zone: r.key.zone, id: r.key.domain}]
		if !ok {
			continue // the domains JOIN: rows without a domain are not selected
		}
		if d.status == model.StatusDeleted {
			continue // §13.1
		}
		if r.version != nil && *r.version == version {
			continue // rules_version equals the current one — up to date
		}
		if zone != nil && r.key.zone != *zone {
			continue // the zone filter
		}
		if !reanKeyGreater(r.key, cursor) {
			continue // the pagination cursor
		}
		sel = append(sel, r)
	}
	// ORDER BY zone_id, domain_id, subdomain_id, variant (a bubble sort:
	// the batches are small) + LIMIT.
	for i := 0; i < len(sel); i++ {
		for j := i + 1; j < len(sel); j++ {
			if reanKeyGreater(sel[i].key, sel[j].key) {
				sel[i], sel[j] = sel[j], sel[i]
			}
		}
	}
	if len(sel) > limit {
		sel = sel[:limit]
	}
	rows := make([][]any, len(sel))
	for i, r := range sel {
		d := p.domains[reanDomKey{zone: r.key.zone, id: r.key.domain}]
		rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, int16(r.key.variant), d.label + "." + p.zones[r.key.zone]}
	}
	out := reanRowsOf(rows...)
	out.scanErr, out.err = p.selectScanErr, p.selectRowsErr
	return out, nil
}

// reanKeyGreater is the row-value ">" key comparison (lexicographic,
// like (a,b,c,d) > (x,y,z,w) in PostgreSQL).
func reanKeyGreater(a, b resultKey) bool {
	switch {
	case a.zone != b.zone:
		return a.zone > b.zone
	case a.domain != b.domain:
		return a.domain > b.domain
	case a.sub != b.sub:
		return a.sub > b.sub
	default:
		return a.variant > b.variant
	}
}

// reanCursorGreater is the row-value ">" comparison of the time-form
// cursor by (checked_at, PK), like (t, a,b,c,d) > (u, w,x,y,z) in
// PostgreSQL: time leads, the PK decides within equal marks.
func reanCursorGreater(r reanRow, after ReanalyzeCursor) bool {
	if !r.checkedAt.Equal(after.checkedAt) {
		return r.checkedAt.After(after.checkedAt)
	}
	return reanKeyGreater(r.key, after.key)
}

// tickSelectRows applies the reanalyzeTickSQL semantics to the seeds
// (T-193): the same selection as the PK form, but the cursor and order go by (checked_at, PK).
func (p *reanPool) tickSelectRows(args []any) (pgx.Rows, error) {
	if len(args) != 9 {
		return nil, fmt.Errorf("reanPool: the tick select: %d arguments, want 9", len(args))
	}
	version, okV := args[0].(int32)
	deleted, okD := args[1].(int16)
	if !okV || !okD {
		return nil, fmt.Errorf("reanPool: the tick select: version %T (%v), status %T (%v)", args[0], args[0], args[1], args[1])
	}
	if deleted != int16(model.StatusDeleted) {
		return nil, fmt.Errorf("reanPool: the tick select: the status guard %d, want the deleted code %d (§13.1)",
			deleted, model.StatusDeleted)
	}
	var zone *int16
	if z, ok := args[2].(int16); ok {
		zone = &z
	} else if args[2] != nil {
		return nil, fmt.Errorf("reanPool: the tick select: zone %T (%v), want int16 or nil", args[2], args[2])
	}
	afterAt, okT := args[3].(time.Time)
	cz, ok1 := args[4].(int16)
	cd, ok2 := args[5].(int64)
	cs, ok3 := args[6].(int16)
	cv, ok4 := args[7].(int16)
	limit, ok5 := args[8].(int)
	if !okT || !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return nil, fmt.Errorf("reanPool: the tick select: cursor/LIMIT %v", args[3:])
	}
	if limit <= 0 {
		return nil, fmt.Errorf("reanPool: the tick select: LIMIT %d ≤ 0", limit)
	}
	after := ReanalyzeCursor{
		checkedAt: afterAt,
		key:       resultKey{zone: cz, domain: cd, sub: cs, variant: model.Variant(cv)},
	}

	var sel []reanRow
	for _, r := range p.rows {
		d, ok := p.domains[reanDomKey{zone: r.key.zone, id: r.key.domain}]
		if !ok {
			continue // the domains JOIN: rows without a domain are not selected
		}
		if d.status == model.StatusDeleted {
			continue // §13.1
		}
		if r.version != nil && *r.version == version {
			continue // rules_version equals the current one — up to date
		}
		if zone != nil && r.key.zone != *zone {
			continue // the zone filter
		}
		if !reanCursorGreater(r, after) {
			continue // the composite (checked_at, PK) cursor
		}
		sel = append(sel, r)
	}
	// ORDER BY checked_at, zone_id, domain_id, subdomain_id, variant
	// (a bubble sort: the batches are small) + LIMIT.
	for i := 0; i < len(sel); i++ {
		for j := i + 1; j < len(sel); j++ {
			gt := false
			if !sel[i].checkedAt.Equal(sel[j].checkedAt) {
				gt = sel[i].checkedAt.After(sel[j].checkedAt)
			} else {
				gt = reanKeyGreater(sel[i].key, sel[j].key)
			}
			if gt {
				sel[i], sel[j] = sel[j], sel[i]
			}
		}
	}
	if len(sel) > limit {
		sel = sel[:limit]
	}
	rows := make([][]any, len(sel))
	for i, r := range sel {
		d := p.domains[reanDomKey{zone: r.key.zone, id: r.key.domain}]
		rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, int16(r.key.variant),
			d.label + "." + p.zones[r.key.zone], r.checkedAt}
	}
	out := reanRowsOf(rows...)
	out.scanErr, out.err = p.tickScanErr, p.tickRowsErr
	return out, nil
}

// reanRows is pgx.Rows with prepared rows (Scan is positional, the
// types are int16/int64/string); scanErr/err are Scan/Err failure injections (T-M10).
type reanRows struct {
	rows    [][]any
	i       int
	scanErr error
	err     error
}

func reanRowsOf(rows ...[]any) *reanRows { return &reanRows{rows: rows} }

func (r *reanRows) Close()                                       {}
func (r *reanRows) Err() error                                   { return r.err }
func (r *reanRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *reanRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *reanRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *reanRows) RawValues() [][]byte                          { return nil }
func (r *reanRows) Conn() *pgx.Conn                              { return nil }
func (r *reanRows) TypeMap() *pgtype.Map                         { return nil }

func (r *reanRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *reanRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("reanRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("reanRows: %d destinations, %d columns", len(dest), len(row))
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
		case *time.Time:
			*dst = v.(time.Time)
		default:
			return fmt.Errorf("reanRows: the destination %T is not supported", d)
		}
	}
	return nil
}

// --- fake rule source ------------------------------------------------------

// reanSource is a RulesSource: a switchable current set and a
// notification channel the test fills. The set field is under the lock:
// the loop reads it from its goroutine, the test switches it from its own (like Reloader, T-068).
type reanSource struct {
	mu   sync.Mutex
	set  *rules.Set
	next chan *rules.Set
}

func newReanSource(version int32) *reanSource {
	return &reanSource{set: &rules.Set{Version: version}, next: make(chan *rules.Set, 1)}
}

func (s *reanSource) Set() *rules.Set {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.set
}

func (s *reanSource) Subscribe() (<-chan *rules.Set, func()) {
	return s.next, func() {}
}

// switchTo changes the set and notifies the subscribers (like Reloader.Reload).
func (s *reanSource) switchTo(version int32) {
	s.mu.Lock()
	s.set = &rules.Set{Version: version}
	set := s.set
	s.mu.Unlock()
	select {
	case s.next <- set:
	default:
	}
}

// reanBuf is a thread-safe bytes.Buffer: the loop writes via slog, the
// test reads the contents before the loop stops (waiting for progress by the log).
type reanBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *reanBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *reanBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// --- the expected SQL text (the contract) ---------------------------------

const wantReanalyzeSelect = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::int2, $5::int8, $6::int2, $7::int2) " +
	"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $8"

// wantReanalyzeTickSQL is the time-form contract (T-193 §4.1): the
// same selection, the composite (checked_at, PK) cursor + ORDER BY by
// the same key + LIMIT; checked_at in the column list is the next batch's cursor.
const wantReanalyzeTickSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name, r.checked_at " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::timestamptz, $5::int2, $6::int8, $7::int2, $8::int2) " +
	"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $9"

const wantZoneIDByName = "SELECT id FROM zones WHERE name = $1"

// wantReanalyzeGuardSQL is the tail buckets guard contract (T-204):
// the sum of reanalyze: buckets older than the current hour (a PK
// prefix range, the bound from the DB) + the current hour bound (the
// closed guard's cursor jump target); coalesce is an honest zero with no buckets.
const wantReanalyzeGuardSQL = "SELECT coalesce(sum(value), 0), date_trunc('hour', now()) FROM crawler_counters " +
	"WHERE name >= 'reanalyze:' " +
	"AND name < 'reanalyze:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"

// reanV is a helper version pointer (a row seed).
func reanV(v int32) *int32 { return &v }

// --- ReanalyzeOnce ---------------------------------------------------------

// TestReanalyzeOnceSQL — the exact SQL text and parameters: the set
// version, the deleted guard (§13.1), a NULL zone (all), the zero
// cursor, the batch LIMIT; a Result with the key and name per row into q:analyze.
func TestReanalyzeOnceSQL(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(3, "crawl.test")
	pool.seedDomain(3, 7, "example", model.StatusAlive)
	pool.seedRow(3, 7, 0, model.VariantHTTP, nil) // NULL — never analyzed
	q := &reanQueue{}

	res, err := ReanalyzeOnce(context.Background(), pool, q, int32(42), nil, ReanalyzeOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("selects = %d, want 1", len(calls))
	}
	if calls[0].sql != wantReanalyzeSelect {
		t.Errorf("the select SQL:\n%s\nwant:\n%s", calls[0].sql, wantReanalyzeSelect)
	}
	if reanalyzeSelectSQL != wantReanalyzeSelect {
		t.Errorf("the constant diverged from the test contract")
	}
	wantArgs := []any{
		int32(42),                              // the set version
		int16(model.StatusDeleted),             // §13.1
		nil,                                    // the zone — all
		int16(0), int64(0), int16(0), int16(0), // the zero cursor — from the beginning
		100, // the batch LIMIT
	}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("the arguments = %#v, want %#v", calls[0].args, wantArgs)
	}

	if res.Selected != 1 || res.Published != 1 {
		t.Errorf("the outcome = %+v, want 1/1", res)
	}
	pubs := q.snapshot()
	if len(pubs) != 1 {
		t.Fatalf("publications = %d, want 1", len(pubs))
	}
	want := queue.Result{
		ZoneID: 3, DomainID: 7, SubdomainID: 0,
		Variant: model.VariantHTTP, Name: "example.crawl.test",
	}
	if pubs[0].stream != queue.StreamAnalyze || pubs[0].msg != want {
		t.Errorf("the publication = %s %+v, want %s %+v", pubs[0].stream, pubs[0].msg, queue.StreamAnalyze, want)
	}
}

// TestReanalyzeOnceZoneParam — the zone filter: an int16 parameter,
// rows of foreign zones are not selected.
func TestReanalyzeOnceZoneParam(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "a.test")
	pool.seedZone(2, "b.test")
	pool.seedDomain(1, 1, "one", model.StatusAlive)
	pool.seedDomain(2, 2, "two", model.StatusAlive)
	pool.seedRow(1, 1, 0, model.VariantMain, nil)
	pool.seedRow(2, 2, 0, model.VariantMain, nil)
	q := &reanQueue{}

	zone := int16(2)
	res, err := ReanalyzeOnce(context.Background(), pool, q, 1, &zone, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if args := pool.calls()[0].args; args[2] != int16(2) {
		t.Errorf("the zone argument = %v (%T), want int16(2)", args[2], args[2])
	}
	if res.Selected != 1 || res.Published != 1 {
		t.Fatalf("the outcome = %+v, want only the zone 2 row", res)
	}
	if msg := q.snapshot()[0].msg; msg.ZoneID != 2 || msg.Name != "two.b.test" {
		t.Errorf("the publication = %+v, want the zone 2 row", msg)
	}
}

// TestReanalyzeOnceFilters — the selection semantics: NULL and a stale
// version are selected, a matching one is not; a deleted domain and a
// row without a domain (the JOIN) are not selected.
func TestReanalyzeOnceFilters(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "never", model.StatusAlive)     // NULL — before T-071
	pool.seedDomain(1, 2, "stale", model.StatusAlive)     // a stale version
	pool.seedDomain(1, 3, "fresh", model.StatusAlive)     // the current version
	pool.seedDomain(1, 4, "deleted", model.StatusDeleted) // §13.1
	pool.seedDomain(1, 5, "gone", model.StatusAlive)      // the domain of row 5 below
	pool.seedRow(1, 1, 0, model.VariantMain, nil)
	pool.seedRow(1, 2, 0, model.VariantMain, reanV(7))
	pool.seedRow(1, 3, 0, model.VariantMain, reanV(42))
	pool.seedRow(1, 4, 0, model.VariantMain, nil)
	pool.seedRow(1, 9, 0, model.VariantMain, nil) // no domains row
	q := &reanQueue{}

	res, err := ReanalyzeOnce(context.Background(), pool, q, 42, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if res.Selected != 2 || res.Published != 2 {
		t.Fatalf("the outcome = %+v, want 2/2 (NULL + stale)", res)
	}
	for i, want := range []string{"never.test", "stale.test"} {
		if got := q.snapshot()[i].msg.Name; got != want {
			t.Errorf("publication[%d] = %s, want %s", i, got, want)
		}
	}
}

// TestReanalyzeOnceBatchLimit — the batch is cut by LIMIT in key order.
func TestReanalyzeOnceBatchLimit(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 5; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	q := &reanQueue{}

	res, err := ReanalyzeOnce(context.Background(), pool, q, 1, nil, ReanalyzeOptions{BatchSize: 3})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if res.Selected != 3 || res.Published != 3 {
		t.Fatalf("the outcome = %+v, want batch 3", res)
	}
	if got := q.snapshot()[2].msg.Name; got != "d3.test" {
		t.Errorf("the third publication = %s, want d3.test (the key order)", got)
	}
}

// TestReanalyzeOnceEmpty — an empty set: one select, zeros, not a single
// publish.
func TestReanalyzeOnceEmpty(t *testing.T) {
	pool := newReanPool()
	q := &reanQueue{}

	res, err := ReanalyzeOnce(context.Background(), pool, q, 5, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if res != (ReanalyzeResult{}) {
		t.Errorf("the outcome = %+v, want zero", res)
	}
	if n := q.count(); n != 0 {
		t.Errorf("publications = %d, want 0", n)
	}
}

// TestReanalyzeOnceDefaults — zero options: the batch is
// DefaultReanalyzeBatch (the literal 5000 is a pin against ±1 mutants
// of the constant, T-M10: comparing against the constant symbol would change with the mutant).
func TestReanalyzeOnceDefaults(t *testing.T) {
	pool := newReanPool()
	if _, err := ReanalyzeOnce(context.Background(), pool, &reanQueue{}, 1, nil, ReanalyzeOptions{}); err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if got := pool.calls()[0].args[7]; got != 5000 {
		t.Errorf("LIMIT = %v, want the default 5000", got)
	}
}

// --- ReanalyzeDrain --------------------------------------------------------

// TestReanalyzeDrainBatches — draining in batches until exhausted: the
// cursor is passed to the next select, every row is published exactly
// once (no repeats within a pass), the outcome is the sums.
func TestReanalyzeDrainBatches(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 7; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	q := &reanQueue{}

	res, err := ReanalyzeDrain(context.Background(), pool, q, 1, nil, ReanalyzeOptions{BatchSize: 3})
	if err != nil {
		t.Fatalf("ReanalyzeDrain: %v", err)
	}
	if res.Selected != 7 || res.Published != 7 {
		t.Fatalf("the outcome = %+v, want 7/7", res)
	}

	calls := pool.calls()
	if len(calls) != 3 { // 3 + 3 + 1 (a short one — stop)
		t.Fatalf("selects = %d, want 3", len(calls))
	}
	// The 2nd batch's cursor is the last row of the 1st, the 3rd's is the
	// last of the 2nd: the row-value parameters grow.
	if got := calls[1].args[4]; got != int64(3) {
		t.Errorf("the batch 2 cursor's domain = %v, want 3", got)
	}
	if got := calls[2].args[4]; got != int64(6) {
		t.Errorf("the batch 3 cursor's domain = %v, want 6", got)
	}

	pubs := q.snapshot()
	if len(pubs) != 7 {
		t.Fatalf("publications = %d, want 7 (no repeats)", len(pubs))
	}
	for i, p := range pubs {
		if want := int64(i + 1); p.msg.DomainID != want {
			t.Errorf("publication[%d].DomainID = %d, want %d (the key order)", i, p.msg.DomainID, want)
		}
	}
}

// TestReanalyzeDrainExactBatch — a set that is a multiple of the batch:
// one control select after the last full batch (short=empty — stop),
// the publishes are exactly the number of rows.
func TestReanalyzeDrainExactBatch(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 6; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	q := &reanQueue{}

	res, err := ReanalyzeDrain(context.Background(), pool, q, 1, nil, ReanalyzeOptions{BatchSize: 3})
	if err != nil {
		t.Fatalf("ReanalyzeDrain: %v", err)
	}
	if res.Selected != 6 || res.Published != 6 {
		t.Fatalf("the outcome = %+v, want 6/6", res)
	}
	if n := len(pool.calls()); n != 3 {
		t.Errorf("selects = %d, want 3 (2 full + the check one)", n)
	}
}

// TestReanalyzeIdempotent — after the T-071 worker commits the current
// version, a repeated drain finds nothing: the select+publish are idempotent.
func TestReanalyzeIdempotent(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedDomain(1, 2, "b", model.StatusAlive)
	pool.seedRow(1, 1, 0, model.VariantMain, nil)
	pool.seedRow(1, 2, 0, model.VariantMain, reanV(7))
	q := &reanQueue{}

	first, err := ReanalyzeDrain(context.Background(), pool, q, 42, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("the first drain: %v", err)
	}
	if first.Published != 2 {
		t.Fatalf("the first drain = %+v, want 2", first)
	}

	pool.markAnalyzed(42) // the worker committed the version for both rows
	q2 := &reanQueue{}
	second, err := ReanalyzeDrain(context.Background(), pool, q2, 42, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("the second drain: %v", err)
	}
	if second != (ReanalyzeResult{}) {
		t.Errorf("the repeated drain = %+v, want zero (idempotency)", second)
	}
	if n := q2.count(); n != 0 {
		t.Errorf("re-publications = %d, want 0", n)
	}
}

// --- ReanalyzeTick (the time form, T-193) ----------------------------------

// TestReanalyzeTickSQL — the SQL contract of the time form, the tail
// buckets guard and the start anchor: the guard (T-204) is the pass's
// first query, the exact text (the sum of buckets older than the
// current hour + the hour bound) without parameters; then the exact
// select text (the composite (checked_at, PK) cursor, ORDER BY by the
// same key, LIMIT, checked_at in the columns), the first select of the
// zero cursor starts from the now() − DefaultReanalyzeLookback anchor
// with a zero PK part (§4.2); a publish is a Result with the key and
// name into q:analyze.
func TestReanalyzeTickSQL(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(3, "crawl.test")
	pool.seedDomain(3, 7, "example", model.StatusAlive)
	before := time.Now().Add(-DefaultReanalyzeLookback)
	pool.seedRowAt(3, 7, 0, model.VariantHTTP, nil, time.Now().Add(-time.Minute)) // NULL + fresh
	q := &reanQueue{}

	res, err := ReanalyzeTick(context.Background(), pool, q, int32(42), &ReanalyzeCursor{}, ReanalyzeOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("ReanalyzeTick: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 2 { // the guard + one batch (1 < 100) — short, stop
		t.Fatalf("selects = %d, want 2 (the guard + a batch)", len(calls))
	}
	if calls[0].sql != wantReanalyzeGuardSQL {
		t.Errorf("the guard SQL:\n%s\nwant:\n%s", calls[0].sql, wantReanalyzeGuardSQL)
	}
	if reanalyzeGuardSQL != wantReanalyzeGuardSQL {
		t.Errorf("the guard constant diverged from the test contract")
	}
	if len(calls[0].args) != 0 {
		t.Errorf("the guard's arguments = %#v, want none", calls[0].args)
	}
	if calls[1].sql != wantReanalyzeTickSQL {
		t.Errorf("the tick select SQL:\n%s\nwant:\n%s", calls[1].sql, wantReanalyzeTickSQL)
	}
	if reanalyzeTickSQL != wantReanalyzeTickSQL {
		t.Errorf("the constant diverged from the test contract")
	}
	args := calls[1].args
	if args[0] != int32(42) || args[1] != int16(model.StatusDeleted) || args[2] != nil {
		t.Errorf("the selection arguments = %#v, want version 42, deleted, zone nil", args[:3])
	}
	anchor, ok := args[3].(time.Time)
	if !ok {
		t.Fatalf("the time cursor = %T (%v), want time.Time", args[3], args[3])
	}
	// The anchor = now − lookback (a minute of slack for a slow test).
	if d := anchor.Sub(before); d < -time.Minute || d > time.Minute {
		t.Errorf("the anchor = %v, want now − %v (±1 min, the drift %v)",
			anchor, DefaultReanalyzeLookback, d)
	}
	if args[4] != int16(0) || args[5] != int64(0) || args[6] != int16(0) || args[7] != int16(0) {
		t.Errorf("the anchor's PK part = %v, want zeros", args[4:8])
	}
	if args[8] != 100 {
		t.Errorf("LIMIT = %v, want 100", args[8])
	}

	if res.Selected != 1 || res.Published != 1 {
		t.Fatalf("the outcome = %+v, want 1/1", res)
	}
	pubs := q.snapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications = %+v, want one into q:analyze", pubs)
	}
	if want := (queue.Result{
		ZoneID: 3, DomainID: 7, SubdomainID: 0,
		Variant: model.VariantHTTP, Name: "example.crawl.test",
	}); pubs[0].msg != want {
		t.Errorf("the publication = %+v, want %+v", pubs[0].msg, want)
	}
}

// TestReanalyzeTickAnchorWindow — the anchor bounds the window: older
// than lookback is not selected (full drain territory), fresh NULLs
// and stale ones inside the window are selected; a row analyzed by
// the current version is not.
func TestReanalyzeTickAnchorWindow(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "fresh", model.StatusAlive)
	pool.seedDomain(1, 2, "stale", model.StatusAlive)
	pool.seedDomain(1, 3, "done", model.StatusAlive)
	pool.seedDomain(1, 4, "ancient", model.StatusAlive)
	now := time.Now()
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, now.Add(-time.Hour))                               // a new episode without analysis
	pool.seedRowAt(1, 2, 0, model.VariantMain, reanV(7), now.Add(-2*time.Hour))                        // a stale one inside the window
	pool.seedRowAt(1, 3, 0, model.VariantMain, reanV(42), now.Add(-time.Minute))                       // up to date
	pool.seedRowAt(1, 4, 0, model.VariantMain, reanV(7), now.Add(-DefaultReanalyzeLookback-time.Hour)) // outside the window
	q := &reanQueue{}

	res, err := ReanalyzeTick(context.Background(), pool, q, 42, &ReanalyzeCursor{}, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeTick: %v", err)
	}
	if res.Selected != 2 || res.Published != 2 {
		t.Fatalf("the outcome = %+v, want 2/2 (NULL + stale in the window)", res)
	}
	// The batch order is by (checked_at, PK): the stale one (2h) ahead of
	// the fresh one (1h).
	for i, want := range []string{"stale.test", "fresh.test"} {
		if got := q.snapshot()[i].msg.Name; got != want {
			t.Errorf("publication[%d] = %s, want %s", i, got, want)
		}
	}
}

// TestReanalyzeTickCursor — the cursor lives between passes and
// advances by a batch: 5 rows in batches of 2 (2+2+1), each select's
// cursor is the previous batch's last row (time leads, the PK decides
// within a mark); a repeated tick over an exhausted window is one
// empty select with the same cursor (an empty batch does not move it).
func TestReanalyzeTickCursor(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	base := time.Now().Add(-time.Hour)
	for i := int64(1); i <= 5; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		// d1/d2 have the same mark — the PK decides the order within.
		at := base.Add(time.Duration(i) * time.Minute)
		if i == 2 {
			at = base.Add(time.Duration(1) * time.Minute)
		}
		pool.seedRowAt(1, i, 0, model.VariantMain, nil, at)
	}
	q := &reanQueue{}
	var cur ReanalyzeCursor

	res, err := ReanalyzeTick(context.Background(), pool, q, 1, &cur, ReanalyzeOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("ReanalyzeTick: %v", err)
	}
	if res.Selected != 5 || res.Published != 5 {
		t.Fatalf("the outcome = %+v, want 5/5", res)
	}
	calls := pool.calls()
	if len(calls) != 4 { // the guard + 2 + 2 + 1 (a short one — stop)
		t.Fatalf("selects = %d, want 4", len(calls))
	}
	// The 2nd batch's cursor is the 1st's last row (d2, the base+1m mark),
	// the 3rd's is the 2nd's last (d4).
	if gotAt, _ := calls[2].args[3].(time.Time); !gotAt.Equal(base.Add(time.Minute)) {
		t.Errorf("the batch 2 cursor's checked_at = %v, want %v", gotAt, base.Add(time.Minute))
	}
	if calls[2].args[5] != int64(2) {
		t.Errorf("the batch 2 cursor's domain = %v, want 2", calls[2].args[5])
	}
	if gotAt, _ := calls[3].args[3].(time.Time); !gotAt.Equal(base.Add(4 * time.Minute)) {
		t.Errorf("the batch 3 cursor's checked_at = %v, want %v", gotAt, base.Add(4*time.Minute))
	}
	if calls[3].args[5] != int64(4) {
		t.Errorf("the batch 3 cursor's domain = %v, want 4", calls[3].args[5])
	}
	// The cursor after the pass is the last row of the last batch.
	if !cur.checkedAt.Equal(base.Add(5*time.Minute)) || cur.key.domain != 5 {
		t.Errorf("the cursor after the pass = %v/%d, want %v/5",
			cur.checkedAt, cur.key.domain, base.Add(5*time.Minute))
	}

	// A repeated tick: the window is exhausted — the guard + one empty
	// select with the last row's cursor (an empty batch does not move it).
	res2, err := ReanalyzeTick(context.Background(), pool, q, 1, &cur, ReanalyzeOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("the repeated ReanalyzeTick: %v", err)
	}
	if res2 != (ReanalyzeResult{}) {
		t.Errorf("the repeated tick = %+v, want zero (the window is exhausted)", res2)
	}
	calls2 := pool.calls()[4:]
	if len(calls2) != 2 {
		t.Fatalf("the repeated tick's selects = %d, want 2 (the guard + an empty one)", len(calls2))
	}
	if gotAt, _ := calls2[1].args[3].(time.Time); !gotAt.Equal(base.Add(5 * time.Minute)) {
		t.Errorf("the empty tick's cursor checked_at = %v, want the previous %v",
			gotAt, base.Add(5*time.Minute))
	}
	if calls2[1].args[5] != int64(5) {
		t.Errorf("the empty tick's cursor domain = %v, want 5", calls2[1].args[5])
	}
	// A new row ahead of the cursor — the next tick will pick it up.
	pool.seedDomain(1, 6, "d6", model.StatusAlive)
	pool.seedRowAt(1, 6, 0, model.VariantMain, nil, base.Add(time.Hour))
	res3, err := ReanalyzeTick(context.Background(), pool, q, 1, &cur, ReanalyzeOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("the tick after a fresh row: %v", err)
	}
	if res3.Selected != 1 || res3.Published != 1 {
		t.Fatalf("the outcome = %+v, want 1/1 (a fresh episode ahead of the cursor)", res3)
	}
	if msg := q.snapshot()[5].msg; msg.DomainID != 6 {
		t.Errorf("the fresh episode's publication = %+v, want domain 6", msg)
	}
}

// TestReanalyzeTickGuard — the tail buckets guard (T-204): a zero sum —
// the heavy time-form select does not run at all, the cursor jumps to
// the current hour bound (the immature area stays ahead); the zero
// cursor's anchor is not read with a closed guard either (the startup
// drain already covered everything stale, the first hour's losses show
// in the buckets); a negative sum is the same outcome (a decrement
// drift, the guard stays silent, the T-202 legacy); a guard error is
// a pass error; an empty aggregate response is a protocol break.
func TestReanalyzeTickGuard(t *testing.T) {
	hour := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)

	seed := func() (*reanPool, *reanQueue, *ReanalyzeCursor) {
		pool := newReanPool()
		pool.guardTails = 0 // the mature tail buckets are empty
		pool.guardHour = hour
		pool.seedZone(1, "test")
		pool.seedDomain(1, 1, "a", model.StatusAlive)
		// A tail inside the anchor window: without the guard it would be selected and published.
		pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Hour))
		q := &reanQueue{}
		return pool, q, &ReanalyzeCursor{}
	}

	pool, q, cur := seed()
	res, err := ReanalyzeTick(context.Background(), pool, q, 1, cur, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeTick (the guard closed): %v", err)
	}
	if res != (ReanalyzeResult{}) {
		t.Errorf("the outcome = %+v, want zero (the heavy select does not run)", res)
	}
	if n := q.count(); n != 0 {
		t.Errorf("publications = %d, want 0", n)
	}
	calls := pool.calls()
	if len(calls) != 1 || calls[0].sql != reanalyzeGuardSQL {
		t.Fatalf("selects = %d, want 1 (the guard only)", len(calls))
	}
	// The cursor jumped to the hour bound with a zero PK part: the
	// immature area (the current hour) stays ahead.
	if !cur.checkedAt.Equal(hour) || cur.key != (resultKey{}) {
		t.Errorf("the cursor = %v/%+v, want the hour boundary %v with a zero PK", cur.checkedAt, cur.key, hour)
	}

	// A negative sum — the same outcome: a decrement drift does not hammer the DB.
	pool, q, cur = seed()
	pool.guardTails = -5
	res, err = ReanalyzeTick(context.Background(), pool, q, 1, cur, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeTick (a negative sum): %v", err)
	}
	if res != (ReanalyzeResult{}) || q.count() != 0 || len(pool.calls()) != 1 {
		t.Errorf("a negative sum: the outcome %+v, publications %d, selects %d — want a silent skip",
			res, q.count(), len(pool.calls()))
	}

	// A guard error — a pass error (no tick select).
	pool, q, cur = seed()
	pool.setQueryErr(errors.New("db unavailable"))
	if _, err := ReanalyzeTick(context.Background(), pool, q, 1, cur, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "analyzer: re-analysis: tail buckets") ||
		!strings.Contains(err.Error(), "db unavailable") {
		t.Errorf("the guard error = %v, want the buckets context and the original", err)
	}

	// An empty aggregate response — a protocol break, an error.
	pool, q, cur = seed()
	pool.guardEmpty = true
	if _, err := ReanalyzeTick(context.Background(), pool, q, 1, cur, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "empty aggregate response") {
		t.Errorf("the guard's empty response = %v, want a protocol break", err)
	}
}

// TestReanalyzeTickPublishError — a publish failure in the middle of a
// batch does not move the cursor past the failed batch: a repeated
// tick selects its rows again (at-least-once, the duplicate is bounded by the batch).
func TestReanalyzeTickPublishError(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	base := time.Now().Add(-time.Hour)
	for i := int64(1); i <= 3; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRowAt(1, i, 0, model.VariantMain, nil, base.Add(time.Duration(i)*time.Minute))
	}
	var cur ReanalyzeCursor
	res, err := ReanalyzeTick(context.Background(), pool,
		&reanQueue{errs: []error{nil, errors.New("redis went away")}}, 1, &cur, ReanalyzeOptions{BatchSize: 3})
	if err == nil || !strings.Contains(err.Error(), "redis went away") {
		t.Fatalf("the error = %v, want the publish cause", err)
	}
	if res.Selected != 3 || res.Published != 1 {
		t.Errorf("the outcome = %+v, want 3/1 (before the failure)", res)
	}
	// The cursor did not advance: the first batch was not published in full.
	anchor := time.Now().Add(-DefaultReanalyzeLookback)
	if d := cur.checkedAt.Sub(anchor); d < -time.Minute || d > time.Minute {
		t.Errorf("the cursor after the publish failure = %v, want the anchor %v (the batch not advanced)",
			cur.checkedAt, anchor)
	}

	// A repeated tick with the same cursor selects the failed batch again.
	q2 := &reanQueue{}
	res2, err := ReanalyzeTick(context.Background(), pool, q2, 1, &cur, ReanalyzeOptions{BatchSize: 3})
	if err != nil {
		t.Fatalf("the repeated tick: %v", err)
	}
	if res2.Selected != 3 || res2.Published != 3 {
		t.Fatalf("the repeated outcome = %+v, want 3/3 (the batch repeated)", res2)
	}
}

// TestReanalyzeTickCancel — a ctx cancellation before the pass: a
// clean context error without a double wrap (a text pin, the TestReanalyzeDrainCancel style).
func TestReanalyzeTickCancel(t *testing.T) {
	pool := newReanPool()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := ReanalyzeTick(ctx, pool, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("the error = %v, want context.Canceled", err)
	}
	if err != nil && err.Error() != "analyzer: re-analysis: context canceled" {
		t.Errorf("the error = %q, want the exact wrap «analyzer: re-analysis: context canceled»", err)
	}
	if res.Published != 0 {
		t.Errorf("the outcome = %+v, want no publications", res)
	}
}

// TestReanalyzeTickNilCursor — a nil cursor: a pass from the anchor
// without saving (a one-off call), no panic.
func TestReanalyzeTickNilCursor(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}

	res, err := ReanalyzeTick(context.Background(), pool, q, 1, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeTick(nil): %v", err)
	}
	if res.Selected != 1 || res.Published != 1 {
		t.Fatalf("the outcome = %+v, want 1/1", res)
	}
}

// --- errors and cancellation -----------------------------------------------

// TestReanalyzePublishError — a publish failure in the middle of a
// batch: the error is wrapped with the domain name and the record
// position (the "record N/M" pin against the i±1 mutants, T-M10), the
// outcome contains what was published before the failure.
func TestReanalyzePublishError(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 2; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	q := &reanQueue{errs: []error{nil, errors.New("redis went away")}}

	res, err := ReanalyzeOnce(context.Background(), pool, q, 1, nil, ReanalyzeOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "analyzer: re-analysis: q:analyze publish") ||
		!strings.Contains(err.Error(), "record 2/2") ||
		!strings.Contains(err.Error(), "d2.test") ||
		!strings.Contains(err.Error(), "redis went away") {
		t.Errorf("the error = %v, want the publish context, the position 2/2, the domain and the cause", err)
	}
	if res.Selected != 2 || res.Published != 1 {
		t.Errorf("the outcome = %+v, want 2/1 (before the failure)", res)
	}

	// The drain does not swallow its batch's publish error (its own
	// return, distinguishable from the Once pass; a cut mutant, T-M10):
	// the error goes out, what was published before the failure is in the outcome.
	drainPool := newReanPool()
	drainPool.seedZone(1, "test")
	for i := int64(1); i <= 2; i++ {
		drainPool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		drainPool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	drainRes, err := ReanalyzeDrain(context.Background(), drainPool,
		&reanQueue{errs: []error{nil, errors.New("redis went away")}}, 1, nil, ReanalyzeOptions{})
	if err == nil || !strings.Contains(err.Error(), "redis went away") {
		t.Errorf("the drain: the error = %v, want the publish cause (not swallowed)", err)
	}
	if drainRes.Selected != 2 || drainRes.Published != 1 {
		t.Errorf("the drain: the outcome = %+v, want 2/1 (before the failure)", drainRes)
	}
}

// TestReanalyzeSelectError — a select error is wrapped; a ctx
// cancellation is preserved via errors.Is.
func TestReanalyzeSelectError(t *testing.T) {
	pool := newReanPool()
	pool.setQueryErr(errors.New("db unavailable"))
	_, err := ReanalyzeOnce(context.Background(), pool, &reanQueue{}, 1, nil, ReanalyzeOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "analyzer: re-analysis: select") ||
		!strings.Contains(err.Error(), "db unavailable") {
		t.Errorf("the error = %v, want the select context and the original error", err)
	}

	// The drain does not swallow a select error (a return-cut mutant, T-M10):
	// the outcome is "as is", the error goes out.
	pool2 := newReanPool()
	pool2.setQueryErr(errors.New("db unavailable"))
	if _, err := ReanalyzeDrain(context.Background(), pool2, &reanQueue{}, 1, nil, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "analyzer: re-analysis: select") {
		t.Errorf("the drain: the error = %v, want the select context (not swallowed)", err)
	}

	pool.setQueryErr(context.Canceled)
	if _, err := ReanalyzeOnce(context.Background(), pool, &reanQueue{}, 1, nil, ReanalyzeOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("the error = %v, want context.Canceled", err)
	}
}

// TestReanalyzeRowErrors — the select rows' Scan and Err failures: the
// errors are wrapped with the "reading" context (return-cut mutants, T-M10), no publishes.
func TestReanalyzeRowErrors(t *testing.T) {
	seed := func() *reanPool {
		pool := newReanPool()
		pool.seedZone(1, "test")
		pool.seedDomain(1, 1, "a", model.StatusAlive)
		pool.seedRow(1, 1, 0, model.VariantMain, nil)
		return pool
	}
	pool := seed()
	pool.selectScanErr = errors.New("scan boom")
	q := &reanQueue{}
	if _, err := ReanalyzeOnce(context.Background(), pool, q, 1, nil, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "analyzer: re-analysis: select: read: scan boom") {
		t.Errorf("the Scan: the error = %v, want «select: read: scan boom»", err)
	}
	if n := q.count(); n != 0 {
		t.Errorf("publications on the Scan failure = %d, want 0", n)
	}

	pool = seed()
	pool.selectRowsErr = errors.New("rows boom")
	if _, err := ReanalyzeOnce(context.Background(), pool, q, 1, nil, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "analyzer: re-analysis: select: rows boom") {
		t.Errorf("the Err: the error = %v, want «select: rows boom» (without «read»)", err)
	}
	if n := q.count(); n != 0 {
		t.Errorf("publications on the Err failure = %d, want 0", n)
	}
}

// TestReanalyzeDrainCancel — a ctx cancellation between batches: a
// clean exit with the context error (no double "select" wrap — a text
// pin against the return-cut mutant, T-M10), what was published stays in the outcome.
func TestReanalyzeDrainCancel(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 4; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // no batch: the very first select returns ctx.Err()
	q := &reanQueue{}

	res, err := ReanalyzeDrain(ctx, pool, q, 1, nil, ReanalyzeOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("the error = %v, want context.Canceled", err)
	}
	if err != nil && err.Error() != "analyzer: re-analysis: context canceled" {
		t.Errorf("the error = %q, want the exact wrap «analyzer: re-analysis: context canceled»", err)
	}
	if res.Published != 0 {
		t.Errorf("the outcome = %+v, want no publications", res)
	}
}

// TestReanalyzeNilArgs — nil dependencies are rejected with a clear
// error (all three entries: a one-off pass, a drain, the loop).
func TestReanalyzeNilArgs(t *testing.T) {
	for _, check := range []struct {
		name string
		call func() error
		want string
	}{
		{"a pass without a pool", func() error {
			_, err := ReanalyzeOnce(context.Background(), nil, &reanQueue{}, 1, nil, ReanalyzeOptions{})
			return err
		}, "pool is not set"},
		{"a pass without a queue", func() error {
			_, err := ReanalyzeOnce(context.Background(), newReanPool(), nil, 1, nil, ReanalyzeOptions{})
			return err
		}, "queue is not set"},
		{"a drain without a pool", func() error {
			_, err := ReanalyzeDrain(context.Background(), nil, &reanQueue{}, 1, nil, ReanalyzeOptions{})
			return err
		}, "pool is not set"},
		{"a drain without a queue", func() error {
			_, err := ReanalyzeDrain(context.Background(), newReanPool(), nil, 1, nil, ReanalyzeOptions{})
			return err
		}, "queue is not set"},
		{"a loop without a pool", func() error {
			return ReanalyzeLoop(context.Background(), nil, &reanQueue{}, newReanSource(1), ReanalyzeOptions{})
		}, "pool is not set"},
		{"a loop without a queue", func() error {
			return ReanalyzeLoop(context.Background(), newReanPool(), nil, newReanSource(1), ReanalyzeOptions{})
		}, "queue is not set"},
		{"a loop without a rules source", func() error {
			return ReanalyzeLoop(context.Background(), newReanPool(), &reanQueue{}, nil, ReanalyzeOptions{})
		}, "rule set source is not set"},
		{"a tick without a pool", func() error {
			_, err := ReanalyzeTick(context.Background(), nil, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{})
			return err
		}, "pool is not set"},
		{"a tick without a queue", func() error {
			_, err := ReanalyzeTick(context.Background(), newReanPool(), nil, 1, &ReanalyzeCursor{}, ReanalyzeOptions{})
			return err
		}, "queue is not set"},
	} {
		if err := check.call(); err == nil || !strings.Contains(err.Error(), check.want) {
			t.Errorf("%s: the error = %v, want %q", check.name, err, check.want)
		}
	}
}

// --- options ---------------------------------------------------------------

func TestReanalyzeOptionsDefaults(t *testing.T) {
	got := (ReanalyzeOptions{}).withDefaults()
	if got.Interval != DefaultReanalyzeInterval || got.BatchSize != DefaultReanalyzeBatch {
		t.Errorf("withDefaults = %v/%v, want %v/%v",
			got.Interval, got.BatchSize, DefaultReanalyzeInterval, DefaultReanalyzeBatch)
	}
	got = ReanalyzeOptions{Interval: -time.Minute, BatchSize: -1}.withDefaults()
	if got.Interval != DefaultReanalyzeInterval || got.BatchSize != DefaultReanalyzeBatch {
		t.Errorf("withDefaults(negative) = %v/%v, want the defaults",
			got.Interval, got.BatchSize)
	}
	keep := ReanalyzeOptions{Interval: time.Minute, BatchSize: 10}.withDefaults()
	if keep.Interval != time.Minute || keep.BatchSize != 10 {
		t.Errorf("withDefaults overwrites the given values: %v/%v", keep.Interval, keep.BatchSize)
	}
	// Unit values are valid and are not replaced by the defaults (the
	// "<= 0 → <= 1" bound, T-M10): 1ns and 1 row are working settings.
	unit := ReanalyzeOptions{Interval: 1, BatchSize: 1}.withDefaults()
	if unit.Interval != 1 || unit.BatchSize != 1 {
		t.Errorf("withDefaults(unit) = %v/%v, want 1/1", unit.Interval, unit.BatchSize)
	}
}

// --- ZoneIDByName ----------------------------------------------------------

func TestZoneIDByName(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(3, "crawl.test")

	id, found, err := ZoneIDByName(context.Background(), pool, "crawl.test")
	if err != nil || !found || id != 3 {
		t.Fatalf("ZoneIDByName = %d, %v, %v; want 3, true, nil", id, found, err)
	}
	calls := pool.calls()
	if len(calls) != 1 || calls[0].sql != wantZoneIDByName {
		t.Fatalf("the SQL = %v, want the exact contract %s", calls, wantZoneIDByName)
	}
	if zoneIDByNameSQL != wantZoneIDByName {
		t.Errorf("the constant diverged from the test contract")
	}

	// No zone: id is exactly 0 and found=false (the "return 0" → ±1 mutants, T-M10).
	id, found, err = ZoneIDByName(context.Background(), pool, "nope.test")
	if err != nil || found || id != 0 {
		t.Errorf("a missing zone = %d, %v, %v; want 0, false, nil", id, found, err)
	}

	fail := newReanPool()
	fail.setQueryErr(errors.New("db unavailable"))
	id, _, err = ZoneIDByName(context.Background(), fail, "x.test")
	if err == nil || !strings.Contains(err.Error(), "analyzer: zone by name") {
		t.Errorf("the error = %v, want the task context", err)
	}
	if id != 0 {
		t.Errorf("the id on error = %d, want 0 (the «return 0» mutants → ±1, T-M10)", id)
	}

	// A Scan failure of a found zone: a "reading" error, found=false,
	// id=0 (the return-cut and "return 0" → ±1 mutants, T-M10).
	scan := newReanPool()
	scan.seedZone(3, "crawl.test")
	scan.zoneScanErr = errors.New("scan boom")
	id, found, err = ZoneIDByName(context.Background(), scan, "crawl.test")
	if err == nil || !strings.Contains(err.Error(), "analyzer: zone by name: read: scan boom") {
		t.Errorf("the Scan: the error = %v, want «zone by name: read: scan boom»", err)
	}
	if found || id != 0 {
		t.Errorf("Scan: id/found = %d, %v; want 0, false", id, found)
	}

	// An Err failure of an empty select's rows: a task context error, id=0.
	rows := newReanPool()
	rows.zoneRowsErr = errors.New("rows boom")
	id, found, err = ZoneIDByName(context.Background(), rows, "x.test")
	if err == nil || !strings.Contains(err.Error(), "analyzer: zone by name: rows boom") {
		t.Errorf("the Err: the error = %v, want «zone by name: rows boom»", err)
	}
	if found || id != 0 {
		t.Errorf("Err: id/found = %d, %v; want 0, false", id, found)
	}

	id, _, err = ZoneIDByName(context.Background(), nil, "x.test")
	if err == nil || !strings.Contains(err.Error(), "pool is not set") {
		t.Errorf("the error = %v, want «the pool is not set»", err)
	}
	if id != 0 {
		t.Errorf("the id without a pool = %d, want 0", id)
	}
}

// --- ReanalyzeLoop ---------------------------------------------------------

// reanWait waits for a condition with a deadline (the loop runs in a goroutine).
func reanWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("the loop did not reach: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestReanalyzeLoopImmediateDrain — the startup FULL drain runs at once:
// what went stale during the downtime (including rules that changed
// during it) is published before the first tick — in the PK form from
// the zero cursor (§4.3). Without Options.Log: a nil logger must be
// replaced with a stub — the "re-analysis pass" Info on a drain
// publish would panic on a nil pointer (the Discard substitution cut mutant, T-M10).
func TestReanalyzeLoopImmediateDrain(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, newReanSource(42), ReanalyzeOptions{
			Interval: time.Hour, // the tick never fires: only the startup drain
		})
	}()

	reanWait(t, "the startup drain", func() bool { return q.count() >= 1 })
	if n := pool.queryCount(); n != 1 {
		t.Errorf("selects = %d, want 1: without a tick there are no repeat passes", n)
	}
	call := pool.calls()[0]
	if call.sql != reanalyzeSelectSQL {
		t.Errorf("the startup pass SQL — the drain PK form:\n%s\nwant:\n%s",
			call.sql, reanalyzeSelectSQL)
	}
	// The drain cursor is a zero PK (from the beginning of the set).
	if call.args[3] != int16(0) || call.args[4] != int64(0) {
		t.Errorf("the startup drain's cursor = %v, want a zero PK", call.args[3:5])
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil on the ctx cancellation", err)
	}
}

// TestReanalyzeLoopTickAndTrigger — the ticker repeats the pass in the
// time form (the safety net): the first tick with the anchored cursor
// re-publishes rows in the window that are not yet stamped (in-flight
// duplicates, T-071), the cursor passes them — later ticks do not
// re-read the window; a rule set change — an out-of-turn FULL drain
// in the PK form from scratch; a ctx cancellation — a clean nil.
func TestReanalyzeLoopTickAndTrigger(t *testing.T) {
	var buf reanBuf
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedDomain(1, 2, "b", model.StatusAlive)
	// Both rows are in the first tick's anchor window (NULL versions, not stamped).
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-2*time.Hour))
	pool.seedRowAt(1, 2, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}
	src := newReanSource(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, src, ReanalyzeOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// The startup drain: both rows published exactly once.
	reanWait(t, "the startup drain", func() bool { return q.count() >= 2 })

	// The first tick pass in the time form: the anchor window (24h) fits
	// both unstamped rows — an in-flight re-publish (at-least-once), the
	// cursor moves past the last one (b).
	reanWait(t, "the tick pass", func() bool { return q.count() >= 4 })
	var tickSeen bool
	for _, c := range pool.calls() {
		if c.sql == reanalyzeTickSQL {
			tickSeen = true
		}
	}
	if !tickSeen {
		t.Errorf("the tick pass did not walk the time form (its SQL was not seen)")
	}

	// Later ticks: both rows are BEHIND the cursor — no re-selection
	// (the time cursor, not a full by-version circle).
	reanWait(t, "the tick selects after the window is exhausted", func() bool { return pool.queryCount() >= 4 })
	if n := q.count(); n != 4 {
		t.Errorf("publications after the window is exhausted = %d, want 4 (no re-election)", n)
	}

	src.switchTo(2) // SIGHUP: an out-of-turn FULL drain with the new version
	reanWait(t, "the out-of-turn pass on a rule change", func() bool {
		return strings.Contains(buf.String(), "out-of-turn re-analysis") &&
			strings.Contains(buf.String(), "rules_version=2")
	})
	// The drain is the PK form from the zero cursor: both rows (stale
	// against version 2) are published again.
	reanWait(t, "the drain publications", func() bool { return q.count() >= 6 })
	drained := false
	for _, c := range pool.calls() {
		if c.sql != reanalyzeSelectSQL {
			continue
		}
		drained = true
		if c.args[3] != int16(0) || c.args[4] != int64(0) {
			t.Errorf("the drain's cursor = %v, want a zero PK (the from-scratch form)", c.args[3:5])
		}
	}
	if !drained {
		t.Errorf("no PK-form select among the selects: a rule change must go as a from-scratch drain")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil on the ctx cancellation", err)
	}
}

// TestReanalyzeLoopErrorSurvives — a pass error (the DB is down) is a
// Warn, the loop lives: after recovery the tick pass publishes; a
// cancellation is a clean nil. The cursor stays zero (the anchor),
// the recovery is the lookback window, nothing is lost (§4.2).
func TestReanalyzeLoopErrorSurvives(t *testing.T) {
	var buf reanBuf
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}

	pool.setQueryErr(errors.New("db unavailable")) // the startup pass fails

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, newReanSource(1), ReanalyzeOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	reanWait(t, "the pass-error Warn", func() bool {
		return strings.Contains(buf.String(), "level=WARN") &&
			strings.Contains(buf.String(), "db unavailable")
	})

	pool.setQueryErr(nil) // the DB is back
	reanWait(t, "the pass after the recovery", func() bool { return q.count() >= 1 })

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil on the ctx cancellation", err)
	}
}

// TestReanalyzeLoopCancelDuringDrain — a cancellation during a pass is
// not an alarm: no Warn, a clean nil (the OnRun contract, T-012).
func TestReanalyzeLoopCancelDuringDrain(t *testing.T) {
	var buf reanBuf
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := newReanPool()
	pool.setQueryErr(context.Canceled)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before the start: the very first pass returns the cancellation

	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, &reanQueue{}, newReanSource(1), ReanalyzeOptions{Log: logger})
	}()

	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil on the ctx cancellation", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("a cancellation during a pass — a Warn:\n%s", buf.String())
	}
}

// TestReanalyzeLoopNilSet — a source without a set: a skip with a
// Warn, the loop lives (after the set appears the pass works).
func TestReanalyzeLoopNilSet(t *testing.T) {
	var buf reanBuf
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}
	src := &reanSource{next: make(chan *rules.Set, 1)} // Set() = nil

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, src, ReanalyzeOptions{
			Interval: time.Hour,
			Log:      logger,
		})
	}()

	reanWait(t, "the empty-set Warn", func() bool {
		return strings.Contains(buf.String(), "rule set not loaded")
	})
	if n := q.count(); n != 0 {
		t.Errorf("publications = %d, want 0 without a set", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil on the ctx cancellation", err)
	}
}

// TestReanalyzeLoopPassInfo — the "re-analysis pass" Info threshold by
// the drain outcome: an empty pass is silent, a pass with publishes
// writes the summary (the "Published > 0" ±/cut mutants and removing
// the Info itself, T-M10). The second select proves the first pass
// finished (drains are sequential); by the check time the log is already settled.
func TestReanalyzeLoopPassInfo(t *testing.T) {
	t.Run("an empty pass is silent", func(t *testing.T) {
		var buf reanBuf
		pool := newReanPool() // empty: a drain without publishes
		q := &reanQueue{}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- ReanalyzeLoop(ctx, pool, q, newReanSource(1), ReanalyzeOptions{
				Interval: 2 * time.Millisecond,
				Log:      slog.New(slog.NewTextHandler(&buf, nil)),
			})
		}()

		reanWait(t, "the second pass", func() bool { return pool.queryCount() >= 2 })
		if strings.Contains(buf.String(), `msg="re-analysis pass"`) {
			t.Errorf("the empty-pass Info was written:\n%s", buf.String())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("ReanalyzeLoop: %v", err)
		}
	})

	t.Run("a pass with publications writes Info", func(t *testing.T) {
		var buf reanBuf
		pool := newReanPool()
		pool.seedZone(1, "test")
		pool.seedDomain(1, 1, "a", model.StatusAlive)
		pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
		q := &reanQueue{}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- ReanalyzeLoop(ctx, pool, q, newReanSource(1), ReanalyzeOptions{
				Interval: 2 * time.Millisecond,
				Log:      slog.New(slog.NewTextHandler(&buf, nil)),
			})
		}()

		reanWait(t, "the second pass", func() bool { return pool.queryCount() >= 2 })
		if log := buf.String(); !strings.Contains(log, `msg="re-analysis pass"`) || !strings.Contains(log, "published=1") {
			t.Errorf("the pass-with-publication Info was not written:\n%s", log)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("ReanalyzeLoop: %v", err)
		}
	})
}

// TestReanalyzeLoopClosedSets — a closed subscription channel does not
// loop the select: the branch is muted (sets = nil), the loop keeps
// living by the ticker and exits only on cancellation. It kills the
// whole branch-cut mutant (it would pour a stream of "out-of-turn
// re-analysis" drain/logs over the closed channel); continue → break
// in the select is equivalent and is covered by the annotation in
// reanalyze.go. An empty pool: a correct loop after the close makes no
// selects and no log writes.
func TestReanalyzeLoopClosedSets(t *testing.T) {
	var buf reanBuf
	pool := newReanPool()
	q := &reanQueue{}
	src := newReanSource(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, src, ReanalyzeOptions{
			Interval: time.Hour, // the tick never fires: after the close the loop has nothing to do
			Log:      slog.New(slog.NewTextHandler(&buf, nil)),
		})
	}()

	reanWait(t, "the startup pass", func() bool { return pool.queryCount() >= 1 })
	if !strings.Contains(buf.String(), "re-analysis loop started") {
		t.Errorf("the startup Info was not written:\n%s", buf.String())
	}
	close(src.next) // the Reloader is gone without cancelling the subscription

	// A 100 ms window: the mutants get to show themselves (a hot loop
	// with a drain per iteration or an instant loop exit).
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("the loop finished on the closed channel: %v", err)
	default:
	}
	if strings.Contains(buf.String(), "out-of-turn re-analysis") {
		t.Errorf("the closed channel treated as a set change:\n%s", buf.String())
	}
	if n := pool.queryCount(); n != 1 {
		t.Fatalf("selects after the channel closed = %d, want 1 (no out-of-turn passes)", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil on the ctx cancellation", err)
	}
}

// Unit tests of content retention and jsonb trimming of non-finds (T-166;
// specs 5.7, 5.5, 6.1, 3.4) on a fake pgx pool with transactions: the
// selection criteria (age, Laravel find, render_pending/rendered,
// deleted), exact SQL texts and parameters, decref aggregation, refcount
// invariants under a failure between UPDATE and decref (the T-044 sample),
// the q:analyze interlock, draining in batches, the cycle. Spec T-271: the
// plan-pinning tx wrapper of the selection (SET LOCAL ×4 → SELECT →
// COMMIT), the SelectTimeout cap, the heavy-scan gate, and the cycle
// backoff after an error. The behavior against a live DB — integration
// tests (test/integration).
package content

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
)

// --- fake pool with retention transactions -------------------------------------

// retentionCall — a recorded SQL call with arguments.
type retentionCall struct {
	sql  string
	args []any
}

// retentionRows — pgx.Rows with pre-prepared rows; Scan is positional, in
// the SQL column order (the cleanupFakeRows style).
type retentionRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error
}

func retentionResultRows(rows ...[]any) *retentionRows {
	return &retentionRows{rows: rows}
}

func (r *retentionRows) Close() { r.closed = true }

func (r *retentionRows) Err() error { return r.err }

func (r *retentionRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *retentionRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *retentionRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *retentionRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("retentionRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("retentionRows: %d destinations, %d columns", len(dest), len(row))
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

func (r *retentionRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *retentionRows) RawValues() [][]byte { return nil }

// Conn and TypeMap — the part of pgx.Rows unused by the positional Scan.
func (r *retentionRows) Conn() *pgx.Conn      { return nil }
func (r *retentionRows) TypeMap() *pgtype.Map { return nil }

// retentionRow — a subdomain_results row: the age, the content reference,
// whether the heavy jsonb is present (any of the columns), and rendered.
type retentionRow struct {
	key       retentionKey
	checkedAt time.Time
	hash      []byte // nil — no reference (T-056)
	jsonb     bool   // headers/cookies/tls/redirects/addrs/cname present
	rendered  bool
}

// retentionDomKey — the domain key.
type retentionDomKey struct {
	zone int16
	id   int64
}

// retentionFakeDomain — the domains row the selection needs: flags and
// status.
type retentionFakeDomain struct {
	flags  int16
	status int16
}

// retentionFakeContent — a contents row.
type retentionFakeContent struct {
	refc int32
	clen int32
}

// retentionFakePool — RetentionDB without a DB: the subdomain_results/
// domains/laravel_domains/contents tables in memory; the retention
// statements follow the SQL semantics (guards, RETURNING, GREATEST), and
// transactions accumulate changes applying them only in Commit (a rollback
// honestly cancels). Step failures — the crash windows of the T-044
// invariants.
type retentionFakePool struct {
	mu       sync.Mutex
	now      time.Time // the server-side now() for the aging boundaries
	results  []retentionRow
	domains  map[retentionDomKey]retentionFakeDomain
	laravel  map[retentionDomKey]bool
	contents map[Hash]retentionFakeContent
	counters map[string]int64 // crawler_counters (T-175); nil values are not supported — no row = no key

	selectErr error
	beginErr  error
	purgeErr  error
	decrefErr error
	deleteErr error
	commitErr error
	debtErr   error // the render-debt counter read failure
	// cursorErr — the persistent cursor read failure at cycle start
	// (T-274); cursorSaveErr — the cursor upsert failure in the batch
	// transaction (the "batch ⟺ cursor" atomicity); cursorRow — the
	// retention_drain_cursor row (nil — no state); cursorSaves/
	// cursorDelete — the applied (committed) upserts and deletions.
	cursorErr     error
	cursorSaveErr error
	cursorRow     *retentionCursor
	cursorSaves   int
	cursorDelete  int
	// planErr — the plan pinning failure (SET LOCAL, T-271); selectBlock —
	// a "stuck" SELECT of the selection: it answers only after the ctx
	// deadline (the T-271 ctx timeout test);
	// selectBeginErr/selectCommitErr — the Begin/Commit failure of the
	// selection tx wrapper ("selection transaction/commit").
	//
	// beginErr/commitErr — injections into the PURGE transaction, not the
	// selection tx wrapper (review of T-271 P2-2): since T-271 the first
	// transaction of a pass is the pinning wrapper, and without separating
	// the roles the "commit failure" case of TestRetentionOnce_FailureRollback
	// would fail on the wrapper's Commit — before the purge, hollowing out
	// the T-044 invariant (the "rows/refcount/contents intact" checks
	// would pass trivially, on an empty batch). The wrapper branches are
	// selectBeginErr/selectCommitErr here; the cases — TestRetentionOnceErrors.
	planErr         error
	selectBlock     bool
	selectBeginErr  error
	selectCommitErr error

	// selectDelay — the heavy selection delay (the T-273 backoff semantics
	// test: the pass lasts longer than the interval tick); selectsAt — the
	// timestamps of the drain selection calls (the pass starts).
	selectDelay time.Duration
	selectsAt   []time.Time

	// onSelect — the hook between the selection and the purge (the rewrite
	// races): called after the candidates are chosen, before the rows are
	// returned.
	onSelect func()

	queries   []retentionCall // the drain/probe SELECTs (in the plan-pinning tx wrapper — T-271; without the counter read)
	debtReads int             // the render-debt counter accesses
	txCalls   []retentionCall // the purge statements via transactions (purge/decref/delete)
	txs       []*retentionFakeTx
	begins    int
	commits   int
	rollbacks int
}

func newRetentionFakePool(now time.Time) *retentionFakePool {
	return &retentionFakePool{
		now:      now,
		domains:  make(map[retentionDomKey]retentionFakeDomain),
		laravel:  make(map[retentionDomKey]bool),
		contents: make(map[Hash]retentionFakeContent),
		// The counter row exists (migration 000015): a zero debt — the
		// default of the production DB after the T-175 deploy.
		counters: map[string]int64{"render_pending": 0},
	}
}

// compile-time: the fake pool is a RetentionDB for tests without a DB.
var _ RetentionDB = (*retentionFakePool)(nil)

func (p *retentionFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

// The retention pool path uses no Exec — all statements go inside a
// transaction.
func (p *retentionFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec via the pool must not be called")
}

// cutoff — the aging boundary "now() − interval" (Duration.String()
// round-trips through ParseDuration — the roundtrip property).
func (p *retentionFakePool) cutoff(interval string) time.Time {
	d, err := time.ParseDuration(interval)
	if err != nil {
		panic("retention fake: malformed interval " + interval)
	}
	return p.now.Add(-d)
}

func (p *retentionFakePool) row(key retentionKey) *retentionRow {
	for i := range p.results {
		if p.results[i].key == key {
			return &p.results[i]
		}
	}
	return nil
}

// candidate — whether the row passes the selection filters (JOIN domains,
// age, presence of content/jsonb, render, deletion, Laravel find).
func (p *retentionFakePool) candidate(r *retentionRow, cutoff time.Time) bool {
	d, ok := p.domains[retentionDomKey{zone: r.key.zone, id: r.key.domain}]
	if !ok {
		return false // JOIN domains
	}
	if !r.checkedAt.Before(cutoff) {
		return false
	}
	if r.hash == nil && !r.jsonb {
		return false // nothing to purge
	}
	if !r.rendered && (d.flags&int16(model.FlagRenderPending)) != 0 {
		return false // waiting for the render (the T-066 rework pass will re-read the content)
	}
	if d.status == int16(model.StatusDeleted) {
		return false
	}
	if p.laravel[retentionDomKey{zone: r.key.zone, id: r.key.domain}] {
		return false
	}
	return true
}

// Query serves the pool reads: the render-debt counter (a point row), the
// persistent drain cursor (T-274, a single row — not written to the drain
// journal), the retry probe (PK = ANY of the keys, selection filters
// without a cursor), and the point drain of a domain (T-187). Since T-271
// the heavy drain selection (retentionSelectSQL) runs inside the plan
// pinning transaction — tx.Query (retentionFakeTx).
func (p *retentionFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch sql {
	case retentionDebtSQL:
		p.debtReads++
		if p.debtErr != nil {
			return nil, p.debtErr
		}
		n, ok := p.counters["render_pending"]
		if !ok {
			return retentionResultRows(), nil // the counter row is missing
		}
		return retentionResultRows([]any{n}), nil

	case retentionCursorLoadSQL:
		if p.cursorErr != nil {
			return nil, p.cursorErr
		}
		if p.cursorRow == nil {
			return retentionResultRows(), nil // no state
		}
		return retentionResultRows([]any{
			p.cursorRow.checkedAt, p.cursorRow.key.zone, p.cursorRow.key.domain,
			p.cursorRow.key.sub, p.cursorRow.key.variant,
		}), nil

	case retentionDomainSQL:
		p.queries = append(p.queries, retentionCall{sql: sql, args: args})
		if p.selectErr != nil {
			return nil, p.selectErr
		}
		zone, domain := args[0].(int16), args[1].(int64)
		cutoff := p.cutoff(args[2].(string))
		var sel []*retentionRow
		for i := range p.results {
			r := &p.results[i]
			if r.key.zone != zone || r.key.domain != domain {
				continue // the PK prefix (zone_id, domain_id)
			}
			if !p.candidate(r, cutoff) {
				continue
			}
			sel = append(sel, r)
		}
		// ORDER BY PK — deterministic batch arguments.
		sorted := make([]*retentionRow, len(sel))
		copy(sorted, sel)
		for i := 1; i < len(sorted); i++ {
			for j := i; j > 0 && sorted[j].key.less(sorted[j-1].key); j-- {
				sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
			}
		}
		rows := make([][]any, len(sorted))
		for i, r := range sorted {
			rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, r.key.variant, r.hash, r.checkedAt}
		}
		return retentionResultRows(rows...), nil

	case retentionProbeSQL:
		p.queries = append(p.queries, retentionCall{sql: sql, args: args})
		cutoff := p.cutoff(args[4].(string))
		zones := args[0].([]int16)
		domains := args[1].([]int64)
		subs := args[2].([]int16)
		variants := args[3].([]int16)
		var rows [][]any
		for i := range zones { // the input key order (the probe sorts)
			key := retentionKey{zone: zones[i], domain: domains[i], sub: subs[i], variant: variants[i]}
			r := p.row(key)
			if r == nil || !p.candidate(r, cutoff) {
				continue // not a candidate: nothing to purge / fresh / a find / a render
			}
			rows = append(rows, []any{r.key.zone, r.key.domain, r.key.sub, r.key.variant, r.hash})
		}
		return retentionResultRows(rows...), nil

	default:
		return nil, fmt.Errorf("retention fake: unexpected SELECT %q", sql)
	}
}

// retentionRowLess/After — comparing a row with a cursor over
// (checked_at, PK), the row-value order of the selection (T-185).
func retentionRowLess(a, b *retentionRow) bool {
	if !a.checkedAt.Equal(b.checkedAt) {
		return a.checkedAt.Before(b.checkedAt)
	}
	return a.key.less(b.key)
}

func retentionRowAfter(r *retentionRow, after retentionCursor) bool {
	if !r.checkedAt.Equal(after.checkedAt) {
		return r.checkedAt.After(after.checkedAt)
	}
	return after.key.less(r.key)
}

func (p *retentionFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	// Separating the injections by transaction roles (review of T-271
	// P2-2): the first transaction of a pass is the selection tx wrapper
	// (selectBeginErr), the second — the batch purge transaction (beginErr).
	// The counter is correct on a fresh fake: RetentionOnce opens the
	// wrapper, then the purge.
	if p.begins == 1 && p.selectBeginErr != nil {
		return nil, p.selectBeginErr
	}
	if p.begins > 1 && p.beginErr != nil {
		return nil, p.beginErr
	}
	tx := &retentionFakeTx{pool: p}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// retentionFakeTx — a transaction: the purges/decreases accumulate in an
// overlay and apply only in Commit (PostgreSQL sees its own changes, hence
// the transaction statements read the overlay). Since T-271 the transaction
// also serves the selection tx wrapper: the plan pinning SET LOCAL
// (retentionFakeTx.Exec) and the batch SELECT (retentionFakeTx.Query);
// calls — the ordered journal of the transaction's statements (the pinning
// order contract, the retryFakeTx.calls style). Since T-274 — the
// upsert/deletion of the persistent cursor (savedCursor/deletedCursor are
// applied in Commit together with the batch — the atomicity is reproduced
// honestly).
type retentionFakeTx struct {
	pgx.Tx // the pass does not use the rest of the pgx.Tx methods

	pool    *retentionFakePool
	purged  map[retentionKey]retentionRow // trimmed rows
	updated map[Hash]retentionFakeContent // decreased refcounts
	deleted map[Hash]struct{}             // deleted contents

	savedCursor   *retentionCursor // the cursor upsert (T-274)
	deletedCursor bool             // the DELETE of the cursor row (a catch-up circle)

	calls      []retentionCall // all the transaction statements in order
	isSelect   bool            // the plan-pinning tx wrapper (the first Exec is a SET LOCAL)
	committed  bool
	rolledBack bool
}

// A view of the state accounting for the transaction's own changes.
func (t *retentionFakeTx) row(key retentionKey) *retentionRow {
	if r, ok := t.purged[key]; ok {
		return &r
	}
	return t.pool.row(key)
}

func (t *retentionFakeTx) content(h Hash) (retentionFakeContent, bool) {
	if _, ok := t.deleted[h]; ok {
		return retentionFakeContent{}, false
	}
	if c, ok := t.updated[h]; ok {
		return c, true
	}
	c, ok := t.pool.contents[h]
	return c, ok
}

// Query — the statements with RETURNING (retentionPurgeSQL,
// retentionDecrefSQL) and the heavy drain selection (retentionSelectSQL —
// since T-271 inside the plan pinning transaction; the filters, the
// row-value cursor, the LIMIT — the same semantics).
func (t *retentionFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	// A "stuck" SELECT (the T-271 ctx timeout test): it answers only after
	// the selection deadline, before the journals and the semantics.
	if sql == retentionSelectSQL && t.pool.selectBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if sql == retentionSelectSQL {
		// The pass start moment and the selection delay (the T-273 backoff
		// semantics): the sleep is outside the mutex — the pool stays
		// available to the test.
		t.pool.mu.Lock()
		t.pool.selectsAt = append(t.pool.selectsAt, time.Now())
		delay := t.pool.selectDelay
		t.pool.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, retentionCall{sql: sql, args: args})
	switch sql {
	case retentionSelectSQL:
		t.pool.queries = append(t.pool.queries, retentionCall{sql: sql, args: args})
		if t.pool.selectErr != nil {
			return nil, t.pool.selectErr
		}
		cutoff := t.pool.cutoff(args[0].(string))
		after := retentionCursor{
			checkedAt: args[1].(time.Time),
			key: retentionKey{
				zone:    args[2].(int16),
				domain:  args[3].(int64),
				sub:     args[4].(int16),
				variant: args[5].(int16),
			},
		}
		limit := args[6].(int)

		var sel []*retentionRow
		for i := range t.pool.results {
			r := &t.pool.results[i]
			if !t.pool.candidate(r, cutoff) {
				continue
			}
			if !retentionRowAfter(r, after) {
				continue
			}
			sel = append(sel, r)
		}
		// ORDER BY (checked_at, PK) — deterministic batches (T-185).
		sorted := make([]*retentionRow, len(sel))
		copy(sorted, sel)
		for i := 1; i < len(sorted); i++ {
			for j := i; j > 0 && retentionRowLess(sorted[j], sorted[j-1]); j-- {
				sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
			}
		}
		if len(sorted) > limit {
			sorted = sorted[:limit]
		}
		// The SELECT snapshot is materialized BEFORE the hook: a rewrite in
		// the race window changes the rows after the read (as in the real
		// recheck pipeline).
		rows := make([][]any, len(sorted))
		for i, r := range sorted {
			rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, r.key.variant, r.hash, r.checkedAt}
		}
		if t.pool.onSelect != nil {
			t.pool.onSelect() // the rewrite race between the selection and the purge
		}
		return retentionResultRows(rows...), nil

	case retentionPurgeSQL:
		t.pool.txCalls = append(t.pool.txCalls, retentionCall{sql: sql, args: args})
		if t.pool.purgeErr != nil {
			return nil, t.pool.purgeErr
		}
		cutoff := t.pool.cutoff(args[5].(string))
		zones := args[0].([]int16)
		domains := args[1].([]int64)
		subs := args[2].([]int16)
		variants := args[3].([]int16)
		hashes := args[4].([][]byte)
		var out [][]any
		for i := range zones {
			key := retentionKey{zone: zones[i], domain: domains[i], sub: subs[i], variant: variants[i]}
			r := t.row(key)
			if r == nil {
				continue // the row vanished (a recheck rewrote it) — the guard
			}
			if !r.checkedAt.Before(cutoff) {
				continue // rewritten by a fresh episode — the age guard
			}
			if !retentionBytesEqual(r.hash, hashes[i]) {
				continue // the content changed — the content_hash guard
			}
			if t.purged == nil {
				t.purged = make(map[retentionKey]retentionRow)
			}
			clean := *r
			clean.hash, clean.jsonb = nil, false
			t.purged[key] = clean
			out = append(out, []any{hashes[i]}) // RETURNING k.hash
		}
		return retentionResultRows(out...), nil

	case retentionDecrefSQL:
		t.pool.txCalls = append(t.pool.txCalls, retentionCall{sql: sql, args: args})
		if t.pool.decrefErr != nil {
			return nil, t.pool.decrefErr
		}
		hashes := args[0].([][]byte)
		counts := args[1].([]int32)
		var out [][]any
		for i := range hashes {
			var h Hash
			copy(h[:], hashes[i])
			c, ok := t.content(h)
			if !ok {
				continue // no contents row — Missing (a bookkeeping divergence)
			}
			refc := c.refc - counts[i] // GREATEST(..., 0)
			if refc < 0 {
				refc = 0
			}
			if t.updated == nil {
				t.updated = make(map[Hash]retentionFakeContent)
			}
			t.updated[h] = retentionFakeContent{refc: refc, clen: c.clen}
			out = append(out, []any{hashes[i], refc, c.clen})
		}
		return retentionResultRows(out...), nil

	default:
		return nil, fmt.Errorf("retention fake: unexpected tx-SELECT %q", sql)
	}
}

// Exec — the SET LOCAL of the selection plan pinning (T-271) and the
// deletion of contents without references (retentionDeleteSQL).
func (t *retentionFakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, retentionCall{sql: sql, args: args})
	switch {
	case sql == retentionPlanNoSeqScanSQL || sql == retentionPlanNoHashJoinSQL ||
		sql == retentionPlanNoMergeJoinSQL || sql == retentionPlanNoBitmapScanSQL ||
		strings.HasPrefix(sql, "SET LOCAL statement_timeout = "):
		// The plan pinning (T-271): the statement_timeout cap is Sprintf'd
		// from SelectTimeout, hence the prefix. The one that executed a SET
		// LOCAL is the selection tx wrapper (the marker for the purge's
		// commitErr injection).
		t.isSelect = true
		if t.pool.planErr != nil {
			return pgconn.CommandTag{}, t.pool.planErr
		}
		return pgconn.NewCommandTag("SET"), nil
	case sql == retentionDeleteSQL:
		t.pool.txCalls = append(t.pool.txCalls, retentionCall{sql: sql, args: args})
		if t.pool.deleteErr != nil {
			return pgconn.CommandTag{}, t.pool.deleteErr
		}
		hashes := args[0].([][]byte)
		var n int64
		for _, hb := range hashes {
			var h Hash
			copy(h[:], hb)
			if c, ok := t.content(h); ok && c.refc <= 0 {
				if t.deleted == nil {
					t.deleted = make(map[Hash]struct{})
				}
				t.deleted[h] = struct{}{}
				n++
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), nil
	case sql == retentionCursorSaveSQL:
		// The persistent cursor upsert (T-274): applied in Commit (the
		// last write in the transaction wins); a failure rolls back the
		// whole transaction (the atomicity with the batch).
		if t.pool.cursorSaveErr != nil {
			return pgconn.CommandTag{}, t.pool.cursorSaveErr
		}
		cur := retentionCursor{
			checkedAt: args[0].(time.Time),
			key: retentionKey{
				zone:    args[1].(int16),
				domain:  args[2].(int64),
				sub:     args[3].(int16),
				variant: args[4].(int16),
			},
		}
		t.savedCursor = &cur
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case sql == retentionCursorDeleteSQL:
		t.deletedCursor = true
		return pgconn.NewCommandTag("DELETE 1"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("retention fake: unexpected tx-Exec %q", sql)
	}
}

func (t *retentionFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	// Separating the injections by roles (review of T-271 P2-2): the
	// selection tx wrapper (isSelect — a SET LOCAL was executed) —
	// selectCommitErr; the purge transaction commit — commitErr (the T-044
	// invariant: a Commit failure rolls back purge/decref/delete).
	if t.isSelect && t.pool.selectCommitErr != nil {
		return t.pool.selectCommitErr
	}
	if !t.isSelect && t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.pool.commits++
	t.committed = true
	for key, r := range t.purged {
		if live := t.pool.row(key); live != nil {
			*live = r
		}
	}
	for h := range t.deleted {
		delete(t.pool.contents, h)
	}
	for h, c := range t.updated {
		if _, ok := t.pool.contents[h]; ok {
			t.pool.contents[h] = c
		}
	}
	if t.savedCursor != nil {
		t.pool.cursorSaves++
		t.pool.cursorRow = t.savedCursor
	}
	if t.deletedCursor {
		t.pool.cursorDelete++
		t.pool.cursorRow = nil
	}
	return nil
}

func (t *retentionFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	// A rollback after Commit — ErrTxClosed, as in pgx (review of T-271
	// P3-3): every transaction of a pass has a defer Rollback; successful
	// passes do not count as rollbacks (the prod code ignores the error —
	// `_ =`).
	if t.committed {
		return pgx.ErrTxClosed
	}
	t.pool.rollbacks++
	t.rolledBack = true
	return nil
}

// retentionBytesEqual — the IS NOT DISTINCT FROM semantics for bytea.
func retentionBytesEqual(a, b []byte) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return string(a) == string(b)
}

// retentionHash16 — a test 16-octet hash with a filler byte.
func retentionHash16(b byte) []byte {
	h := make([]byte, HashLen)
	for i := range h {
		h[i] = b
	}
	return h
}

// retentionTestLogger — a logger into a buffer (checking the cycle's
// Warn/Info).
func retentionTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// retentionSeed — the typical set: an old non-find with content and jsonb,
// a find domain, render_pending, deleted, a fresh record.
func retentionSeedPool(now time.Time) *retentionFakePool {
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{flags: 0, status: int16(model.StatusAlive)}
	pool.domains[retentionDomKey{zone: 1, id: 20}] = retentionFakeDomain{flags: 0, status: int16(model.StatusAlive)}
	pool.domains[retentionDomKey{zone: 1, id: 30}] = retentionFakeDomain{flags: int16(model.FlagRenderPending), status: int16(model.StatusAlive)}
	pool.domains[retentionDomKey{zone: 1, id: 40}] = retentionFakeDomain{flags: int16(model.FlagRenderPending), status: int16(model.StatusAlive)}
	pool.domains[retentionDomKey{zone: 1, id: 50}] = retentionFakeDomain{flags: 0, status: int16(model.StatusDeleted)}
	pool.domains[retentionDomKey{zone: 1, id: 60}] = retentionFakeDomain{flags: 0, status: int16(model.StatusAlive)}
	pool.laravel[retentionDomKey{zone: 1, id: 20}] = true
	pool.results = []retentionRow{
		// 10: a regular non-find — purged.
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xAA), jsonb: true},
		// 20: a Laravel find — its content is kept always.
		{key: retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xBB), jsonb: true},
		// 30: render_pending and not rendered — waiting for the render.
		{key: retentionKey{zone: 1, domain: 30, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xCC), jsonb: true},
		// 40: render_pending but rendered — the render already replaced the
		// content.
		{key: retentionKey{zone: 1, domain: 40, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xDD), jsonb: true, rendered: true},
		// 50: deleted — T-020 cleanup territory.
		{key: retentionKey{zone: 1, domain: 50, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xEE), jsonb: true},
		// 60: a fresh record — not aged.
		{key: retentionKey{zone: 1, domain: 60, sub: 0, variant: 0}, checkedAt: now.Add(-time.Minute), hash: retentionHash16(0xFF), jsonb: true},
	}
	pool.contents[retentionHash(0xAA)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xBB)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xCC)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xDD)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xEE)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xFF)] = retentionFakeContent{refc: 1, clen: 7000}
	return pool
}

// retentionHash — a Hash from a filler byte.
func retentionHash(b byte) Hash {
	var h Hash
	for i := range h {
		h[i] = b
	}
	return h
}

// retentionDefaultOpts — typical pass parameters with the interlock off
// (the tests of the selection criteria and the SQL).
func retentionDefaultOpts() RetentionOptions {
	return RetentionOptions{Retention: DefaultRetention, BatchSize: 100}
}

// --- selection criteria --------------------------------------------------------

// TestRetentionOnce_Selection — the T-166 criteria: only aged records of
// non-finds not waiting for a render, on live domains, are purged; the
// passport columns are not restored (the row remains).
func TestRetentionOnce_Selection(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 2 || stats.Refs != 2 || stats.Contents != 2 || stats.FreedBytes != 2*7000 {
		t.Fatalf("stats = %+v, want Rows=2 Refs=2 Contents=2 FreedBytes=14000 (domains 10 and 40)", stats)
	}
	if stats.BadHashes != 0 || stats.Missing != 0 || stats.Skipped {
		t.Errorf("there must be no anomalies: %+v", stats)
	}
	for _, dom := range []int64{10, 40} {
		r := pool.row(retentionKey{zone: 1, domain: dom, sub: 0, variant: 0})
		if r == nil || r.hash != nil || r.jsonb {
			t.Errorf("domain %d: the record not purged: %+v", dom, r)
		}
	}
	for _, dom := range []int64{20, 30, 50, 60} {
		r := pool.row(retentionKey{zone: 1, domain: dom, sub: 0, variant: 0})
		if r == nil || r.hash == nil || !r.jsonb {
			t.Errorf("domain %d: the record must stay untouched: %+v", dom, r)
		}
	}
	if _, ok := pool.contents[retentionHash(0xAA)]; ok {
		t.Error("contents 0xAA (refcount 1 → 0) must be removed")
	}
	if c, ok := pool.contents[retentionHash(0xBB)]; !ok || c.refc != 1 {
		t.Errorf("contents 0xBB (a find) touched: %+v", c)
	}
	if c, ok := pool.contents[retentionHash(0xCC)]; !ok || c.refc != 1 {
		t.Errorf("contents 0xCC (render_pending) touched: %+v", c)
	}
	if c, ok := pool.contents[retentionHash(0xFF)]; !ok || c.refc != 1 {
		t.Errorf("contents 0xFF (fresh) touched: %+v", c)
	}
}

// TestRetentionOnce_OnlyJSONB — a record without content (T-056) but with
// jsonb: the trim proceeds, the refcount is not touched, the transaction
// commits.
func TestRetentionOnce_OnlyJSONB(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.results[0].hash = nil // the first record has no content
	_, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0})
	if r == nil || r.jsonb {
		t.Errorf("a record without content: the jsonb not trimmed: %+v", r)
	}
	for h, c := range pool.contents {
		if c.refc != 1 {
			t.Errorf("contents %x: the refcount changed without a reference: %+v", h[:2], c)
		}
	}
}

// TestRetentionOnce_AlreadyClean — an empty selection (all clean): one
// SELECT in the plan-pinning tx wrapper (T-271); the purge transaction is
// not opened.
func TestRetentionOnce_AlreadyClean(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.results = nil

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Errorf("stats = %+v, want an empty pass", stats)
	}
	if len(pool.queries) != 1 || pool.begins != 1 {
		t.Errorf("calls = %d (transactions %d), want one SELECT in the pinning tx wrapper", len(pool.queries), pool.begins)
	}
}

// --- SQL contract ----------------------------------------------------------------

// TestRetentionOnce_SQLContract — exact statement texts and parameters:
// the aging boundary as an interval, the row-value cursor, the batch
// LIMIT; the keys and the read hashes go into the UPDATE; the decref
// aggregation is by hashes. Since T-271 the selection runs in the
// plan-pinning tx wrapper: the strict order
// BEGIN → SET LOCAL seq/hash/merge/bitmap off → SET LOCAL statement_timeout
// → SELECT → COMMIT (a replica of T-191 retry_test.go / T-181
// renderq_test.go; without pinning a degraded plan OOM-killed PostgreSQL —
// the 2026-11-08/09 incident).
func TestRetentionOnce_SQLContract(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	// Keep two candidates with different hashes — to check the batch.
	keep := []retentionRow{pool.results[0], pool.results[3]}
	pool.results = keep

	if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if len(pool.queries) != 1 {
		t.Fatalf("drain SELECTs = %d, want 1 (one incomplete batch; the debt counter read does not count)", len(pool.queries))
	}
	sel := pool.queries[0]
	if sel.sql != retentionSelectSQL {
		t.Errorf("the selection SQL changed")
	}
	wantArgs := []any{
		DefaultRetention.String(),
		time.Time{}, int16(0), int64(0), int16(0), int16(0), // the zero cursor of the first batch (time + PK)
		100,
	}
	if !reflect.DeepEqual(sel.args, wantArgs) {
		t.Errorf("selection args = %v, want %v", sel.args, wantArgs)
	}

	// The pinning wrapper (T-271): the first transaction is SET LOCAL ×5 +
	// a SELECT, committed without a rollback; the texts are the package
	// literals (the statement_timeout cap — the 7200 s default of T-274,
	// formerly 3600 in T-273: the timeout must cover the first catch-up
	// pass from the zero cursor under load, not only the regular batches of
	// the T-201 measurement; the bitmap off — the T-271 fine-tuning: the
	// bitmap path breaks the cursor order).
	if retentionPlanNoSeqScanSQL != "SET LOCAL enable_seqscan = off" ||
		retentionPlanNoHashJoinSQL != "SET LOCAL enable_hashjoin = off" ||
		retentionPlanNoMergeJoinSQL != "SET LOCAL enable_mergejoin = off" ||
		retentionPlanNoBitmapScanSQL != "SET LOCAL enable_bitmapscan = off" ||
		retentionSelectTimeoutFmt != "SET LOCAL statement_timeout = '%dms'" {
		t.Fatalf("the plan pinning texts changed (the integration EXPLAIN test contract)")
	}
	if len(pool.txs) != 2 {
		t.Fatalf("transactions = %d, want 2 (the selection tx wrapper + the batch purge tx)", len(pool.txs))
	}
	pin := pool.txs[0]
	wantOrder := []string{
		retentionPlanNoSeqScanSQL,
		retentionPlanNoHashJoinSQL,
		retentionPlanNoMergeJoinSQL,
		retentionPlanNoBitmapScanSQL,
		fmt.Sprintf(retentionSelectTimeoutFmt, DefaultRetentionSelectTimeout.Milliseconds()),
		retentionSelectSQL,
	}
	if len(pin.calls) != len(wantOrder) {
		t.Fatalf("tx wrapper statements = %d, want %d (SET LOCAL ×5 + SELECT): %v", len(pin.calls), len(wantOrder), pin.calls)
	}
	for i, sql := range wantOrder {
		if pin.calls[i].sql != sql {
			t.Errorf("tx wrapper step %d = %s, want %s (the SET LOCAL ×5 → SELECT order)", i, pin.calls[i].sql, sql)
		}
	}
	if !pin.committed || pin.rolledBack {
		t.Errorf("tx wrapper: committed=%v rolledBack=%v, want a commit without rollback", pin.committed, pin.rolledBack)
	}
	if pool.txs[1].committed != true {
		t.Error("the batch purge transaction not committed")
	}

	if len(pool.txCalls) != 3 {
		t.Fatalf("transaction statements = %d, want 3 (purge, decref, delete)", len(pool.txCalls))
	}
	purge := pool.txCalls[0]
	if purge.sql != retentionPurgeSQL {
		t.Errorf("the purge SQL changed")
	}
	if !reflect.DeepEqual(purge.args[:5], []any{
		[]int16{1, 1}, []int64{10, 40}, []int16{0, 0}, []int16{0, 0},
		[][]byte{retentionHash16(0xAA), retentionHash16(0xDD)},
	}) {
		t.Errorf("purge args (keys/hashes) = %v", purge.args[:5])
	}
	if purge.args[5] != DefaultRetention.String() {
		t.Errorf("the purge boundary = %v, want %s", purge.args[5], DefaultRetention)
	}

	decref := pool.txCalls[1]
	if decref.sql != retentionDecrefSQL {
		t.Errorf("the decref SQL changed")
	}
	if !reflect.DeepEqual(decref.args, []any{
		[][]byte{retentionHash16(0xAA), retentionHash16(0xDD)},
		[]int32{1, 1},
	}) {
		t.Errorf("decref args = %v (hashes sorted, counters per reference)", decref.args)
	}

	del := pool.txCalls[2]
	if del.sql != retentionDeleteSQL {
		t.Errorf("the delete SQL changed")
	}
	if !reflect.DeepEqual(del.args, []any{
		[][]byte{retentionHash16(0xAA), retentionHash16(0xDD)},
	}) {
		t.Errorf("delete args = %v (both refcounts reached zero)", del.args)
	}
}

// --- plan pinning, timeouts, gate (T-271) ---------------------------------------

// TestRetentionOnce_PlanError — the plan pinning failure (SET LOCAL) — a
// pass error BEFORE the selection: the SELECT did not run, the batch does
// not commit, and no purge is opened (the style of renderq_test.go
// TestRequeueOncePlanError).
func TestRetentionOnce_PlanError(t *testing.T) {
	now := time.Date(2026, 11, 8, 20, 43, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.planErr = errors.New("connection loss: unexpected EOF")

	if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "pinning the plan") {
		t.Fatalf("error = %v, want the plan pinning context", err)
	}
	if len(pool.queries) != 0 {
		t.Errorf("SELECTs = %d, want 0 (the pinning failed before the selection)", len(pool.queries))
	}
	if len(pool.txCalls) != 0 {
		t.Errorf("purge statements = %d, want 0", len(pool.txCalls))
	}
}

// TestRetentionOnce_SelectCtxTimeout — a stuck selection is cut by the
// client-side ctx deadline of SelectTimeout (T-271): the server-side
// statement_timeout and ctx are one cap by two levels; the wrapped error
// carries the "candidate selection" context, and the retry is idempotent.
func TestRetentionOnce_SelectCtxTimeout(t *testing.T) {
	pool := retentionSeedPool(time.Now())
	pool.selectBlock = true // the SELECT does not answer until ctx is cancelled

	opts := retentionDefaultOpts()
	opts.SelectTimeout = 40 * time.Millisecond
	if _, err := RetentionOnce(context.Background(), pool, opts); err == nil ||
		!errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), "candidate selection") {
		t.Fatalf("error = %v, want a wrapped context.DeadlineExceeded \"candidate selection\"", err)
	}
}

// TestRetentionSelectGate — the heavy selection under the common scan gate
// (db.ScanGate, T-271): a busy gate holds the selection BEFORE Begin (a
// pool connection does not idle in the queue), and releasing it lets the
// selection through; the mutual exclusion of two holders — TestScanGate in
// internal/db.
func TestRetentionSelectGate(t *testing.T) {
	pool := retentionSeedPool(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
	gate := make(db.ScanGate, 1)
	if err := gate.Acquire(context.Background()); err != nil {
		t.Fatalf("the test holding the gate: %v", err)
	}
	opts := retentionDefaultOpts()
	opts.Gate = gate

	done := make(chan error, 1)
	go func() {
		_, err := retentionSelect(context.Background(), pool, opts, retentionCursor{})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the selection proceeded under a held gate: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	pool.mu.Lock()
	begins := pool.begins
	pool.mu.Unlock()
	if begins != 0 {
		t.Fatalf("Begin under a held gate: %d transactions, want 0 (the gate before Begin)", begins)
	}
	gate.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the selection after the gate release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the selection did not proceed after the gate release")
	}
}

// TestRetentionLoop_Backoff — a pass error switches the ticker to backoff
// (T-271): the interval tick no longer restarts the pass immediately
// (the 2026-11-08/09 incident — a pass into a PostgreSQL just booted after
// an OOM kill); after the pause the pass retries, and a success returns
// the regular interval.
func TestRetentionLoop_Backoff(t *testing.T) {
	var buf bytes.Buffer
	pool := retentionSeedPool(time.Date(2026, 11, 8, 20, 43, 0, 0, time.UTC))
	pool.mu.Lock()
	pool.selectErr = errors.New("connection loss: unexpected EOF")
	pool.mu.Unlock()
	opts := RetentionOptions{
		Retention:    DefaultRetention,
		Interval:     10 * time.Millisecond,  // without backoff — ~10 passes per 100 ms
		ErrorBackoff: 250 * time.Millisecond, // the failed tick's pause
		BatchSize:    100,
		Log:          retentionTestLogger(&buf),
	}
	passes := func() int {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return pool.debtReads // one counter read = one started pass
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()

	// The starting pass failed; the ticks of the ... 10-ms interval do not
	// restart it within the backoff pause.
	time.Sleep(80 * time.Millisecond)
	if n := passes(); n != 1 {
		cancel()
		t.Fatalf("passes during the backoff pause = %d, want 1 (the starting one only; ~8 without the backoff)", n)
	}
	// The error is lifted: the pause tick retries the pass, and a success
	// returns the regular interval — the passes continue.
	pool.mu.Lock()
	pool.selectErr = nil
	pool.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for passes() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := passes(); n < 3 {
		cancel()
		t.Fatalf("passes after the pause = %d, want ≥3 (success restores the regular interval)", n)
	}
	// The log is read only after the cycle stops: the cycle goroutine
	// writes into the same buffer (review of T-271 P2-1 — bytes.Buffer is
	// not goroutine-safe).
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancel")
	}
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("log = %q, want the failed pass Warn", buf.String())
	}
}

// TestRetentionLoop_BackoffEveryErrorResetsTicker — the backoff semantics
// (T-273, the main diagnostic gap of T-271): the ticker reset after EVERY
// error. Before T-273 the reset ran only on the first one (the !backoff
// condition): the ticker's anchor stayed at the end of the first failed
// pass, and repeated errors went with a pause of "backoff − pass duration"
// (a 600 s pass at a 15 min backoff — a real pause of 300 s); the WARN
// cadence masked the pass durations. Here the pass lasts longer than the
// interval tick (selectDelay 150 ms > interval 40 ms) and fails: the pass
// starts must be spaced by ≥ pass duration + backoff (a bottom guarantee:
// the sleep and the ticker do not end early).
func TestRetentionLoop_BackoffEveryErrorResetsTicker(t *testing.T) {
	pool := retentionSeedPool(time.Date(2026, 11, 9, 14, 13, 0, 0, time.UTC))
	pool.mu.Lock()
	pool.selectErr = errors.New("content: retention: candidate selection: timeout: context deadline exceeded")
	pool.selectDelay = 150 * time.Millisecond
	pool.mu.Unlock()
	opts := RetentionOptions{
		Retention:    DefaultRetention,
		Interval:     40 * time.Millisecond,
		ErrorBackoff: 500 * time.Millisecond,
		BatchSize:    100,
	}
	attempts := func() []time.Time {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return append([]time.Time(nil), pool.selectsAt...)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()

	// The starting pass + two retries within a generous budget (a 650 ms
	// pause).
	deadline := time.Now().Add(5 * time.Second)
	var starts []time.Time
	for {
		starts = attempts()
		if len(starts) >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancel")
	}
	if len(starts) < 3 {
		t.Fatalf("pass starts = %d, want ≥3 (the starting one + two retries)", len(starts))
	}
	// The gap between the starts ≥ pass (150 ms) + backoff (500 ms) − a
	// scheduler margin (50 ms). The old semantics gave exactly the backoff
	// (500 ms) between repeated errors — the ticker anchor was not
	// re-anchored.
	const minGap = 150 + 500 - 50
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < minGap*time.Millisecond {
			t.Errorf("the pause between passes %d→%d = %v, want ≥ %v (pass + backoff: reset after each error)",
				i-1, i, gap, minGap*time.Millisecond)
		}
	}
}

// TestRetentionLoop_OnError — the pass error hook (T-273): called on every
// failed pass (together with the Warn), not on successful ones; the cycle
// error counter (crawler_content_retention_errors_total) grows by exactly
// the number of failed passes — before T-273 the errors were visible only
// in journalctl (the :9104 metrics were zeros at 70+ WARNs per day).
func TestRetentionLoop_OnError(t *testing.T) {
	pool := retentionSeedPool(time.Date(2026, 11, 9, 14, 13, 0, 0, time.UTC))
	pool.mu.Lock()
	pool.selectErr = errors.New("connection loss: unexpected EOF")
	pool.mu.Unlock()

	errs := make(chan error, 16)
	opts := RetentionOptions{
		Retention:    DefaultRetention,
		Interval:     20 * time.Millisecond,
		ErrorBackoff: 60 * time.Millisecond,
		BatchSize:    100,
		OnError:      func(err error) { errs <- err },
	}
	count := func() int {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return len(pool.selectsAt)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()

	// The starting pass and the first retry — errors: one hook call per
	// each.
	deadline := time.Now().Add(3 * time.Second)
	for count() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// The error is lifted: successful passes do not call the hook.
	pool.mu.Lock()
	pool.selectErr = nil
	pool.mu.Unlock()
	errored := count()
	deadline = time.Now().Add(3 * time.Second)
	for count() < errored+2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancel")
	}
	close(errs)
	n := 0
	var last error
	for err := range errs {
		n++
		last = err
	}
	// Exactly as many calls as failed passes: errored attempts ran with the
	// error active (+1 — a pass that started before the error was lifted
	// but read it later); successful passes add no hook calls.
	if n < errored || n > errored+1 {
		t.Fatalf("OnError calls = %d, want %d..%d (one per failed pass)", n, errored, errored+1)
	}
	if last == nil || !strings.Contains(last.Error(), "unexpected EOF") {
		t.Errorf("OnError received %v, want the pass error", last)
	}
}

// TestRetentionOnce_DedupAggregation — one content shared by several
// records (deduplication, spec 5.7): the decrease is by the number of cut
// references, and the contents row dies only with the last one.
func TestRetentionOnce_DedupAggregation(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11)},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old, hash: retentionHash16(0x11)},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 2, clen: 5000}

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Refs != 2 || stats.Contents != 1 || stats.FreedBytes != 5000 {
		t.Fatalf("stats = %+v, want Refs=2 Contents=1 FreedBytes=5000", stats)
	}
	if _, ok := pool.contents[retentionHash(0x11)]; ok {
		t.Error("contents with refcount 2 and two trimmed references must be removed")
	}
}

// TestRetentionOnce_ClampDesync — the GREATEST clamp on a bookkeeping
// desync (T-265, invariant I1 of the refcount automaton; the specification
// is the doc comment of pack.go, transition T9): an aggregated cut larger
// than the actual refcount (two aged references to content with a counter
// of 1) does not push the counter negative: 1 − 2 clamps to 0, and the row
// is deleted as one that lost its last reference (retention skips the
// tombstone); the pass does not fail.
// A negative refcount would also be rejected by the CHECK
// contents_refcount_check (migration 000019) — the clamp guarantees that
// the mass pass does not trigger it. The GREATEST behavior against a live
// PostgreSQL — test/integration (TestContent_RefcountClampDesync).
func TestRetentionOnce_ClampDesync(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x22)},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old, hash: retentionHash16(0x22)},
	}
	// The desync: 2 holders, the counter 1 (the reference was already
	// removed by another path).
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 5000}

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce on a desync: %v", err)
	}
	if stats.Refs != 2 || stats.Contents != 1 || stats.FreedBytes != 5000 {
		t.Fatalf("stats = %+v, want Refs=2 Contents=1 FreedBytes=5000 (clamp 1−2 → 0, the row removed)", stats)
	}
	if c, ok := pool.contents[retentionHash(0x22)]; ok {
		t.Errorf("contents after the clamp = %+v, want removed (the refcount reached zero)", c)
	}
	for _, sub := range []int16{0, 1} {
		r := pool.row(retentionKey{zone: 1, domain: 10, sub: sub, variant: 0})
		if r == nil || r.hash != nil {
			t.Errorf("record sub=%d not purged: %+v", sub, r)
		}
	}
}

// TestRetentionOnce_SharedContentLives — content referenced by a not-yet-
// aged record does not die (refcount > 0 after the decref).
func TestRetentionOnce_SharedContentLives(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	// The fresh record 60 shares the 0xAA content with the aged 10.
	pool.results[5].hash = retentionHash16(0xAA)
	pool.contents[retentionHash(0xAA)] = retentionFakeContent{refc: 2, clen: 7000}

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Contents != 1 {
		t.Fatalf("Contents = %d, want 1 (only 0xDD of domain 40)", stats.Contents)
	}
	if c, ok := pool.contents[retentionHash(0xAA)]; !ok || c.refc != 1 {
		t.Errorf("contents 0xAA after the decref = %+v (ok=%v), want refcount 1 (the fresh record's reference is alive)", c, ok)
	}
}

// --- bookkeeping anomalies -----------------------------------------------------

// TestRetentionOnce_BadHashes — a cut reference of invalid length: the row
// is trimmed, its refcount was not changed, the pass is not dropped
// (a poison pill, the ING-P2-4 style).
func TestRetentionOnce_BadHashes(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.results[0].hash = make([]byte, 9) // 9 octets instead of 16 (spec 6.1)

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.BadHashes != 1 || stats.Refs != 2 || stats.Rows != 2 {
		t.Fatalf("stats = %+v, want BadHashes=1 with Rows=Refs=2", stats)
	}
	if len(pool.txCalls) != 3 {
		t.Fatalf("statements = %d, want 3 (purge, decref and delete only for the valid 0xDD)", len(pool.txCalls))
	}
	decref := pool.txCalls[1]
	hashes := decref.args[0].([][]byte)
	if len(hashes) != 1 || !retentionBytesEqual(hashes[0], retentionHash16(0xDD)) {
		t.Errorf("decref over hashes %v, want only the valid 0xDD", hashes)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Errorf("the row with a malformed hash not purged: %+v", r)
	}
}

// TestRetentionOnce_Missing — a cut reference without a contents row: a
// bookkeeping divergence — a counter, not a batch error.
func TestRetentionOnce_Missing(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	delete(pool.contents, retentionHash(0xAA)) // the reference is there, no contents

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Missing != 1 {
		t.Fatalf("stats = %+v, want Missing=1", stats)
	}
	if stats.Contents != 1 { // the 0xDD of domain 40 deleted normally
		t.Errorf("Contents = %d, want 1", stats.Contents)
	}
}

// --- refcount invariants under a failure (T-044) -------------------------------

// TestRetentionOnce_FailureRollback — the crash windows of the batch
// transaction: a failure of any step (purge / decref / delete / commit)
// rolls everything back — the rows and the refcount do not diverge.
// A failure exactly BETWEEN the UPDATE and the decref (decrefErr) is the
// subject of the T-044 invariant: a reference cut without a decref would
// break the bookkeeping forever. The overlay depths (review of T-271
// P2-2) — "how much work the purge managed to do in the transaction before
// the failure": without them the "commit failure" case could fail on the
// selection wrapper's Commit (before the purge), and the "rows/refcount/
// contents intact" checks would pass trivially, on an empty batch — the
// expected depths pin the failure step.
func TestRetentionOnce_FailureRollback(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		mut     func(*retentionFakePool)
		purged  int // rows trimmed before the failure (the purge tx overlay)
		decrefd int // contents with a decreased refcount
		deleted int // contents slated for deletion
	}{
		{"purge failure", func(p *retentionFakePool) { p.purgeErr = errors.New("db") }, 0, 0, 0},
		{"a failure between UPDATE and decref", func(p *retentionFakePool) { p.decrefErr = errors.New("db") }, 2, 0, 0},
		{"a failure between decref and delete", func(p *retentionFakePool) { p.deleteErr = errors.New("db") }, 2, 2, 0},
		{"commit failure", func(p *retentionFakePool) { p.commitErr = errors.New("db") }, 2, 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := retentionSeedPool(now)
			tc.mut(pool)
			if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err == nil {
				t.Fatalf("a pass error expected")
			}
			// The pass structure (T-271): the selection tx wrapper, then
			// the purge transaction; the purge injections (review P2-2)
			// must fail in the second one — a failure in the wrapper would
			// leave the purge unopened and the checks below trivially
			// empty.
			if len(pool.txs) != 2 {
				t.Fatalf("pass transactions = %d, want 2 (the selection wrapper + the purge)", len(pool.txs))
			}
			tx := pool.txs[1]
			if len(tx.purged) != tc.purged || len(tx.updated) != tc.decrefd || len(tx.deleted) != tc.deleted {
				t.Fatalf("purge overlay = purge:%d decref:%d delete:%d, want %d/%d/%d — the failure is not in the case's step",
					len(tx.purged), len(tx.updated), len(tx.deleted), tc.purged, tc.decrefd, tc.deleted)
			}
			if !tx.rolledBack {
				t.Fatal("the purge transaction not rolled back after the failure")
			}
			// Invariant: nothing changed — the rows are intact (content and
			// jsonb in place), the refcount is intact, the contents are
			// alive.
			for _, r := range pool.results {
				if r.hash == nil || !r.jsonb {
					t.Fatalf("row %v purged with a rolled-back transaction", r.key)
				}
			}
			for h, c := range pool.contents {
				if c.refc != 1 {
					t.Fatalf("contents %x: refcount = %d, want 1 (the decref rollback)", h[:2], c.refc)
				}
			}
			if len(pool.contents) != 6 {
				t.Fatalf("contents = %d rows, want 6 (nothing removed)", len(pool.contents))
			}
		})
	}
}

// TestRetentionOnce_RaceRewritten — a result rewrite between the selection
// and the purge (the T-030 recheck): the double guard (age + content_hash)
// screens out the foreign episode — 0 touched rows, nothing broken.
func TestRetentionOnce_RaceRewritten(t *testing.T) {
	fresh := func(r *retentionRow) { // the DELETE+INSERT rewrite (T-058)
		r.checkedAt = r.checkedAt.Add(3 * DefaultRetention) // a fresh now()
	}
	t.Run("a fresh episode", func(t *testing.T) {
		now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
		pool := retentionSeedPool(now)
		pool.results = pool.results[:1]
		pool.onSelect = func() { fresh(&pool.results[0]) }
		stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
		if err != nil {
			t.Fatalf("RetentionOnce: %v", err)
		}
		if stats.Rows != 0 || stats.Refs != 0 || stats.Batches != 1 {
			t.Fatalf("stats = %+v, want an empty committed batch", stats)
		}
		if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil || !r.jsonb {
			t.Fatalf("the overwritten row touched: %+v", r)
		}
	})
	t.Run("the content changed", func(t *testing.T) {
		now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
		pool := retentionSeedPool(now)
		pool.results = pool.results[:1]
		pool.onSelect = func() { pool.results[0].hash = retentionHash16(0x99) }
		stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
		if err != nil {
			t.Fatalf("RetentionOnce: %v", err)
		}
		if stats.Rows != 0 || stats.Refs != 0 {
			t.Fatalf("stats = %+v, want 0 rows (the content_hash guard)", stats)
		}
	})
}

// --- the q:analyze interlock ----------------------------------------------------

// fakeDepth — a source of the q:analyze depth with a pre-prepared answer.
type fakeDepth struct {
	mu    sync.Mutex
	n     int64
	err   error
	calls int
}

func (f *fakeDepth) AnalyzeDepth(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.n, f.err
}

// TestRetentionOnce_Interlock — the q:analyze depth at the threshold: the
// pass is skipped, the DB is untouched; below the threshold — the work
// proceeds; a read error — a pass error (conservatively); a threshold ≤ 0 /
// a nil source — off.
func TestRetentionOnce_Interlock(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	opts := retentionDefaultOpts()
	opts.AnalyzeWatermark = 100

	pool := retentionSeedPool(now)
	depth := &fakeDepth{n: 100}
	opts.Depth = depth
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if !stats.Skipped || stats.Depth != 100 || stats.Rows != 0 {
		t.Fatalf("stats = %+v, want Skipped with Depth=100 and no work", stats)
	}
	if len(pool.queries) != 0 || pool.begins != 0 {
		t.Fatalf("a skipped pass must leave the DB alone: %d SELECTs, %d transactions", len(pool.queries), pool.begins)
	}

	pool = retentionSeedPool(now)
	depth = &fakeDepth{n: 99} // strictly below the threshold — like the T-026 gate
	opts.Depth = depth
	if stats, err := RetentionOnce(context.Background(), pool, opts); err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	} else if stats.Skipped || stats.Rows != 2 {
		t.Fatalf("stats = %+v, want work at a depth below the threshold", stats)
	}

	pool = retentionSeedPool(now)
	depth = &fakeDepth{err: errors.New("redis unavailable")}
	opts.Depth = depth
	if _, err := RetentionOnce(context.Background(), pool, opts); err == nil ||
		!strings.Contains(err.Error(), "the q:analyze interlock") {
		t.Fatalf("error = %v, want the interlock context", err)
	}
	if len(pool.queries) != 0 {
		t.Fatalf("depth error: leave the DB alone (%d SELECTs)", len(pool.queries))
	}

	// The interlock off: a threshold ≤ 0 — the source is not polled at all.
	pool = retentionSeedPool(now)
	depth = &fakeDepth{n: 1 << 30}
	opts.Depth = depth
	opts.AnalyzeWatermark = 0
	if _, err := RetentionOnce(context.Background(), pool, opts); err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if depth.calls != 0 {
		t.Fatalf("a disabled interlock does not poll the depth (%d calls)", depth.calls)
	}
}

// TestRetentionOnce_InterlockMidDrain — the interlock is checked on every
// drain batch: the depth grew in the middle of a pass — what was done
// stands (the batches are atomic in themselves), and the remainder waits
// for the next pass.
func TestRetentionOnce_InterlockMidDrain(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // a batch = a row: the first is purged, the second — the interlock
	depth := &fakeDepth{n: 0}
	opts.Depth = depth
	opts.AnalyzeWatermark = 100
	var selects int
	pool.onSelect = func() {
		selects++
		if selects >= 1 {
			depth.mu.Lock()
			depth.n = 150 // the analyzer stopped keeping up after the first batch
			depth.mu.Unlock()
		}
	}
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 1 || !stats.Skipped || stats.Depth != 150 {
		t.Fatalf("stats = %+v, want one batch before the interlock", stats)
	}
}

// --- drain ----------------------------------------------------------------------

// TestRetentionOnce_Drain — draining in batches: a full batch → the next
// one with the cursor past the last row; an incomplete one — stop; LIMIT —
// the batch size.
func TestRetentionOnce_Drain(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	var rows []retentionRow
	for sub := int16(0); sub < 3; sub++ {
		rows = append(rows, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old, hash: retentionHash16(byte(0x10 + sub)), jsonb: true,
		})
		pool.contents[retentionHash(byte(0x10+sub))] = retentionFakeContent{refc: 1, clen: 1000}
	}
	pool.results = rows

	opts := retentionDefaultOpts()
	opts.BatchSize = 2
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 3 || stats.Batches != 2 {
		t.Fatalf("stats = %+v, want 3 rows in two batches", stats)
	}
	if len(pool.queries) != 2 {
		t.Fatalf("SELECTs = %d, want 2", len(pool.queries))
	}
	// The second batch's cursor — (checked_at, PK) past the last row of the
	// first (sub=1): time leads, the PK determines the order of equal
	// stamps.
	second := pool.queries[1]
	want := []any{DefaultRetention.String(), old, int16(1), int64(10), int16(1), int16(0), 2}
	if !reflect.DeepEqual(second.args, want) {
		t.Errorf("second batch args = %v, want %v", second.args, want)
	}
}

// --- errors and cancellation ----------------------------------------------------

func TestRetentionOnceErrors(t *testing.T) {
	if _, err := RetentionOnce(context.Background(), nil, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "the pool is not set") {
		t.Errorf("error = %v, want \"the pool is not set\"", err)
	}

	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	boom := retentionSeedPool(now)
	boom.selectErr = errors.New("db unavailable")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "candidate selection") {
		t.Errorf("error = %v, want the selection context", err)
	}

	// The Begin failure of the PURGE transaction (the second transaction of
	// a pass; the first is the selection tx wrapper; the injection
	// separation, review of T-271 P2-2): the purge-branch error prefix
	// asserted below (the HasPrefix) tells the purge branch apart from the
	// selection-transaction error — the old generic Contains check (the
	// Russian word for "transaction") masked which of the transactions
	// failed.
	boom = retentionSeedPool(now)
	boom.beginErr = errors.New("db unavailable")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.HasPrefix(err.Error(), "content: retention: transaction: ") {
		t.Errorf("error = %v, want the purge transaction Begin rejection (\"content: retention: transaction:\")", err)
	}

	// The Begin failure of the selection tx wrapper (the first transaction
	// of a pass, T-271).
	boom = retentionSeedPool(now)
	boom.selectBeginErr = errors.New("db unavailable")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "selection transaction") {
		t.Errorf("error = %v, want \"selection transaction\"", err)
	}

	// The wrapper's Commit failure: the batch is not committed and no purge
	// is opened — the retry is idempotent (the cursor did not move).
	boom = retentionSeedPool(now)
	boom.selectCommitErr = errors.New("db unavailable")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "committing the selection") {
		t.Errorf("error = %v, want \"committing the selection\"", err)
	}
	if len(boom.txs) != 1 || len(boom.txCalls) != 0 {
		t.Errorf("after the wrapper commit failure: transactions = %d, purge statements = %d, want 1/0", len(boom.txs), len(boom.txCalls))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := retentionSeedPool(now)
	if _, err := RetentionOnce(ctx, canceled, retentionDefaultOpts()); !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

// --- options and cycle -----------------------------------------------------------

func TestRetentionOptionsDefaults(t *testing.T) {
	got := RetentionOptions{}.withDefaults()
	want := RetentionOptions{
		Interval:      DefaultRetentionInterval,
		BatchSize:     DefaultRetentionBatch,
		SelectTimeout: DefaultRetentionSelectTimeout,
		ErrorBackoff:  DefaultRetentionErrorBackoff,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("withDefaults = %+v, want %+v", got, want)
	}
	if DefaultRetentionSelectTimeout != 7200*time.Second {
		t.Errorf("DefaultRetentionSelectTimeout = %s, want 7200s (T-274: double the margin over the production measurements of 2026-11-10–11 of the first catch-up pass under load; the T-273 3600s only sufficed for a quiet system — three passes under the full front hit the wall)", DefaultRetentionSelectTimeout)
	}
	if DefaultRetentionErrorBackoff != 15*time.Minute {
		t.Errorf("DefaultRetentionErrorBackoff = %s, want 15m (the T-271 incident backoff)", DefaultRetentionErrorBackoff)
	}
	set := RetentionOptions{Interval: time.Minute, BatchSize: 5,
		SelectTimeout: time.Minute, ErrorBackoff: time.Hour}.withDefaults()
	if set.Interval != time.Minute || set.BatchSize != 5 ||
		set.SelectTimeout != time.Minute || set.ErrorBackoff != time.Hour {
		t.Errorf("withDefaults overwrites the given values: %+v", set)
	}
}

// TestRetentionLoop_Disabled — retention = 0: the cycle does not start and
// returns nil (the OnRun contract).
func TestRetentionLoop_Disabled(t *testing.T) {
	var buf bytes.Buffer
	pool := retentionSeedPool(time.Now())
	if err := RetentionLoop(context.Background(), pool, RetentionOptions{Log: retentionTestLogger(&buf)}); err != nil {
		t.Fatalf("RetentionLoop: %v", err)
	}
	if len(pool.queries) != 0 {
		t.Fatalf("SELECTs = %d, want 0 (retention disabled)", len(pool.queries))
	}
	if !strings.Contains(buf.String(), "content retention disabled") {
		t.Errorf("log = %q, want the retention-disabled entry", buf.String())
	}
}

// TestRetentionLoop — the starting drain runs immediately, the pass
// outcome goes to the OnPass hook (the metrics), and a ctx cancellation
// returns nil (the OnRun contract, T-012).
func TestRetentionLoop(t *testing.T) {
	var buf bytes.Buffer
	pool := retentionSeedPool(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
	opts := RetentionOptions{
		Retention: DefaultRetention,
		Interval:  time.Hour, // no ticker wait — the starting drain is what is checked
		BatchSize: 100,
		Log:       retentionTestLogger(&buf),
	}
	passed := make(chan RetentionStats, 4)
	opts.OnPass = func(s RetentionStats, _ time.Duration) { passed <- s }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()
	// The starting drain finished before the cancellation — the outcome is
	// deterministic.
	select {
	case s := <-passed:
		if s.Rows != 2 {
			t.Fatalf("the starting pass result = %+v, want 2 rows", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the starting pass did not finish")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RetentionLoop: %v", err)
	}
	if !strings.Contains(buf.String(), "content retention loop started") ||
		!strings.Contains(buf.String(), "content retention pass") {
		t.Errorf("log = %q, want the loop start and the pass result", buf.String())
	}
	// Two commits: the plan-pinning tx wrapper of the selection (T-271) +
	// the purge tx.
	if pool.commits != 2 {
		t.Errorf("commits = %d, want 2 (pinning + purge)", pool.commits)
	}
}

// TestRetentionLoop_NilPool — a nil pool — a startup error (the service
// build must fail loudly, not silently).
func TestRetentionLoop_NilPool(t *testing.T) {
	if err := RetentionLoop(context.Background(), nil, RetentionOptions{}); err == nil ||
		!strings.Contains(err.Error(), "the pool is not set") {
		t.Errorf("error = %v, want \"the pool is not set\"", err)
	}
}

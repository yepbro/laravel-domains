// Unit tests of the HTTP episode's result write and the q:analyze publication (T-058;
// spec 5.5 items 7, 9, §6.1, §7, §8, 3.4; the write transaction's 40P01 retry —
// T-268) over a fake pool and queue — without a DB,
// Redis and the network. The fake pool lives by the tables' semantics (domains, contents,
// subdomain_results) with snapshot transactions: the exact SQL and
// parameters are checked, the "write → q:analyze publication" order, the transaction
// boundaries with the reference swap (SwapRefTx, T-044), the alive/no_http/temp_error statuses,
// the deleted guard (§8/§13.1), the idempotent delivery repeat, the step failures and
// the ctx cancellation; the smoke — the Run loop with the OnResult chain "ContentSaver →
// write → publication" (the cmd/http-worker build). The behavior against live
// PostgreSQL/Redis — integration tests (T-032, T-046).
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/httpfetch"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- the event protocol (the "write → publication → ack" order) -----------------

// recordEventKind — the protocol's event type; the order is checked by the indexes.
type recordEventKind string

const (
	recordEvBegin   recordEventKind = "begin"
	recordEvDomains recordEventKind = "update-domains"
	recordEvDelete  recordEventKind = "delete"
	recordEvSwap    recordEventKind = "swapref"
	recordEvInsert  recordEventKind = "insert"
	recordEvShift   recordEventKind = "zone-shift"     // the zone counters' delta (T-178)
	recordEvChecks  recordEventKind = "checks-bump"    // the daily checks counter's delta (T-179)
	recordEvTempErr recordEventKind = "temp-err-bump"  // the daily temp_error numerator's delta (T-182)
	recordEvMature  recordEventKind = "mature-bump"    // the retry maturity bucket's delta (T-202)
	recordEvRean    recordEventKind = "reanalyze-bump" // the unanalyzed tails bucket's delta (T-204)
	recordEvResults recordEventKind = "results-bump"   // the hourly throughput bucket's delta (T-203)
	recordEvCommit  recordEventKind = "commit"
	recordEvPublish recordEventKind = "publish"
	recordEvAck     recordEventKind = "ack"
)

// recordJournal — a concurrency-safe protocol (the Run loop runs in a goroutine).
type recordJournal struct {
	mu     sync.Mutex
	events []recordEventKind

	// ackNotify — a non-blocking signal per Ack (the smoke's synchronization).
	ackNotify chan struct{}
}

func (j *recordJournal) record(k recordEventKind) {
	j.mu.Lock()
	j.events = append(j.events, k)
	j.mu.Unlock()
}

func (j *recordJournal) snapshot() []recordEventKind {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]recordEventKind, len(j.events))
	copy(out, j.events)
	return out
}

func (j *recordJournal) count(k recordEventKind) int {
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

// indexOf returns the index of the first event of the kind (0-based) or −1.
func (j *recordJournal) indexOf(k recordEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.events {
		if e == k {
			return i
		}
	}
	return -1
}

func (j *recordJournal) signalAck() {
	if j.ackNotify == nil {
		return
	}
	select {
	case j.ackNotify <- struct{}{}:
	default:
	}
}

// --- the fake pgx pool: three tables + snapshot transactions --------------------

// recordDomainKey — the key of a domains row in the fake.
type recordDomainKey struct {
	zone int16
	id   int64
}

// recordFakeDomain — the domains row recordDomainSQL changes.
type recordFakeDomain struct {
	status       model.Status
	checkState   model.CheckState
	queuedAt     *time.Time
	lastChecked  bool
	tempAttempts int16
	cutoff       bool // the dead_host_cutoff bit of the last episode (T-164)
	flags        model.Flags
}

// recordFakeRow — a subdomain_results row: the INSERT chunk's columns.
type recordFakeRow struct {
	subdomainID int16
	variant     model.Variant
	scheme      model.Scheme
	httpStatus  *int16
	finalURL    *string
	aliasKind   model.AliasKind
	aliasSub    *int16
	addrs       []byte
	cname       *string
	tls         *string
	redirects   *string
	headers     *string
	cookies     *string
	rendered    bool
	contentHash []byte
	errCode     *int16
	responseMS  *int32
}

// recordFakePool — a RecordDB without a DB: the live semantics of domains (the
// status <> deleted guard at the UPDATE), subdomain_results (DELETE … RETURNING,
// the INSERT chunk), contents (a refcount with the > 0 guard), the zone
// counters (recordZoneShiftSQL, T-178), the daily checks counter, its
// temp_error numerator (recordChecksBumpSQL/recordTempErrBumpSQL,
// T-179/T-182), the retry maturity bucket (recordRetryMatureBumpSQL,
// T-202) and the hourly throughput bucket (recordResultsBumpSQL, T-203 — the
// day/hour names are computed by the DB, the fake keeps one value of "the current
// day/hour's rows"). The transaction runs
// on a snapshot: Rollback restores the pool's state, Commit keeps
// it (enough for sequential write calls; concurrent
// transactions are not modeled by the fake — PostgreSQL serializes them).
type recordFakePool struct {
	j *recordJournal

	domains map[recordDomainKey]*recordFakeDomain
	rows    map[recordDomainKey][]recordFakeRow
	refc    map[content.Hash]int32
	zones   map[int16]*recordFakeZone
	checks  int64
	tempErr int64
	// retryMature — the current day's retry maturity bucket (T-202): the
	// name is computed by the DB, the fake keeps one value (the daily rows' style).
	retryMature int64
	// reanalyze — the current hour's unanalyzed tails bucket
	// (T-204): the name is computed by the DB, the fake keeps one value.
	reanalyze int64
	// results — the current hour's hourly throughput bucket (T-203): the name
	// is computed by the DB, the fake keeps one value (the daily rows' style).
	results int64

	begins   int
	txs      int // transactions created
	beginErr error

	failExec     map[string]error // an Exec failure by the exact SQL (all the attempts)
	failExecOnce map[string]error // an Exec failure by the exact SQL — only the first attempt (the 40P01 fault injection, T-268)
	deleteErr    error
	commitErr    error
	insertTag    int64 // the INSERT's RowsAffected (0 — an honest answer)
	matureTag    int64 // the maturity bucket upsert's RowsAffected (0 — an honest answer)
	reanTag      int64 // the tails bucket upsert's RowsAffected (0 — an honest answer)
	resultsTag   int64 // the throughput bucket upsert's RowsAffected (0 — an honest answer)

	mu sync.Mutex
}

// recordFakeZone — a zones row: the incremental counters' columns
// (T-178); recordZoneShiftSQL shifts it.
type recordFakeZone struct {
	total, pending, queued, checked int64
}

func newRecordFakePool() *recordFakePool {
	return &recordFakePool{
		j:       &recordJournal{},
		domains: make(map[recordDomainKey]*recordFakeDomain),
		rows:    make(map[recordDomainKey][]recordFakeRow),
		refc:    make(map[content.Hash]int32),
		zones:   make(map[int16]*recordFakeZone),
	}
}

// recordSeedZone — a zone with the given counters (T-178); a zone without a record —
// the reference table row is absent, the shift will honestly answer UPDATE 0.
func (p *recordFakePool) recordSeedZone(id int16, total, pending, queued, checked int64) *recordFakeZone {
	z := &recordFakeZone{total: total, pending: pending, queued: queued, checked: checked}
	p.zones[id] = z
	return z
}

// checksValue — the daily checks counter's value (T-179) under the lock.
func (p *recordFakePool) checksValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checks
}

// tempErrValue — the daily temp_error numerator's value (T-182) under
// the lock.
func (p *recordFakePool) tempErrValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tempErr
}

// retryMatureValue — the retry maturity bucket's value (T-202) under the lock.
func (p *recordFakePool) retryMatureValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.retryMature
}

// reanalyzeValue — the unanalyzed tails bucket's value (T-204)
// under the lock.
func (p *recordFakePool) reanalyzeValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reanalyze
}

// resultsValue — the hourly throughput bucket's value (T-203) under the lock.
func (p *recordFakePool) resultsValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.results
}

var _ RecordDB = (*recordFakePool)(nil)

// recordSeedDomain — a domain in the queued state (before the episode's write).
func (p *recordFakePool) recordSeedDomain(zone int16, id int64, status model.Status, tempAttempts int16) *recordFakeDomain {
	d := &recordFakeDomain{status: status, checkState: model.CheckStateQueued, tempAttempts: tempAttempts}
	now := time.Now()
	d.queuedAt = &now
	p.domains[recordDomainKey{zone: zone, id: id}] = d
	if _, ok := p.zones[zone]; !ok {
		// The reference-table zone for the counters' deltas (T-178): zero
		// counters — the tests that need values seed them explicitly via recordSeedZone.
		p.recordSeedZone(zone, 0, 0, 0, 0)
	}
	return d
}

func (p *recordFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("recordFakePool: CopyFrom must not be called (the INSERT is done by an unnest chunk)")
}

func (p *recordFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("recordFakePool: Exec outside a transaction")
}

func (p *recordFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("recordFakePool: Query outside a transaction")
}

func (p *recordFakePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	// The snapshot: the struct values are copied, the row slices share
	// the pointers of the immutable fields — the transaction only adds/removes
	// elements, it does not mutate the old ones.
	snapDomains := make(map[recordDomainKey]*recordFakeDomain, len(p.domains))
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
	snapZones := make(map[int16]*recordFakeZone, len(p.zones))
	for k, v := range p.zones {
		c := *v
		snapZones[k] = &c
	}
	snapChecks := p.checks
	snapTempErr := p.tempErr
	snapMature := p.retryMature
	snapResults := p.results
	p.txs++
	p.j.record(recordEvBegin)
	return &recordFakeTx{
		pool: p, j: p.j,
		snapDomains: snapDomains, snapRows: snapRows, snapRefc: snapRefc, snapZones: snapZones,
		snapChecks: snapChecks, snapTempErr: snapTempErr, snapMature: snapMature, snapResults: snapResults,
	}, nil
}

// recordFakeTx — a transaction over the fake: the operators apply to the pool
// immediately, Rollback rolls back to the snapshot, Commit commits.
type recordFakeTx struct {
	pgx.Tx // the write does not use the other pgx.Tx methods

	pool        *recordFakePool
	j           *recordJournal
	snapDomains map[recordDomainKey]*recordFakeDomain
	snapRows    map[recordDomainKey][]recordFakeRow
	snapRefc    map[content.Hash]int32
	snapZones   map[int16]*recordFakeZone
	snapChecks  int64
	snapTempErr int64
	snapMature  int64
	snapResults int64
	committed   bool
}

// recordFakeRow — the pgx.Row of the states' pre-read (T-178): two int16
// (status, check_state) or an error (pgx.ErrNoRows — no row).
type recordStateRow struct {
	status, state int16
	err           error
}

func (r recordStateRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 2 {
		return fmt.Errorf("recordStateRow: %d destinations, 2 needed", len(dest))
	}
	*dest[0].(*int16) = r.status
	*dest[1].(*int16) = r.state
	return nil
}

// QueryRow — the point pre-read of the FOR UPDATE states (recordSelectStateSQL,
// T-178): a domain row of any status transparency — the lock "is there";
// an absent row — ErrNoRows.
func (t *recordFakeTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != recordSelectStateSQL {
		return recordStateRow{err: fmt.Errorf("recordFakePool: an unexpected QueryRow: %s", sql)}
	}
	key := recordDomainKey{zone: args[0].(int16), id: args[1].(int64)}
	d, ok := t.pool.domains[key]
	if !ok {
		return recordStateRow{err: pgx.ErrNoRows}
	}
	return recordStateRow{status: int16(d.status), state: int16(d.checkState)}
}

// recordDelRefSQLPrefix — the sqlMetaDelRef prefix from content/pack.go
// (T-042): the constant is not exported, the exact text is maintained
// manually; a divergence breaks the test loudly — sync it with pack.go.
const recordDelRefSQLPrefix = "UPDATE contents SET refcount = refcount - 1"

func (t *recordFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	// A one-shot fault injection (T-268): the operator's first attempt fails
	// with a 40P01 deadlock, the repeat (after the attempt transaction's rollback) — passes.
	if err, ok := t.pool.failExecOnce[sql]; ok {
		delete(t.pool.failExecOnce, sql)
		return pgconn.CommandTag{}, err
	}
	if err, ok := t.pool.failExec[sql]; ok {
		return pgconn.CommandTag{}, err
	}
	switch sql {
	case recordDomainSQL:
		zone := args[0].(int16)
		id := args[1].(int64)
		status := model.Status(args[2].(int16))
		cutoff := args[3].(bool) // the dead_host_cutoff bit (T-164)
		key := recordDomainKey{zone: zone, id: id}
		d, ok := t.pool.domains[key]
		if !ok || d.status == model.StatusDeleted {
			return pgconn.NewCommandTag("UPDATE 0"), nil // the §8/§13.1 guard
		}
		d.status = status
		d.checkState = model.CheckStateDone
		d.queuedAt = nil
		d.lastChecked = true
		if status == model.StatusTempError {
			// The CASE from recordDomainSQL: temp_error keeps the counter.
		} else {
			d.tempAttempts = 0
		}
		// The recordDomainSQL CASE flags (T-164): a cutoff episode sets
		// the bit, a usual one removes it; the other bits untouched.
		if cutoff {
			d.flags.Set(model.FlagDeadHostCutoff)
		} else {
			d.flags.Clear(model.FlagDeadHostCutoff)
		}
		d.cutoff = d.flags.IsDeadHostCutoff()
		t.j.record(recordEvDomains)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case recordInsertSQL:
		if len(args) != 18 {
			return pgconn.CommandTag{}, fmt.Errorf("recordFakePool: INSERT: %d parameters, 18 needed", len(args))
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
				cookies: cookies[i], rendered: false, // the false literal in the SELECT
				contentHash: hash[i], errCode: errCode[i], responseMS: responseMS[i],
			})
			if domain[i] == 0 {
				return pgconn.CommandTag{}, errors.New("recordFakePool: INSERT: domain_id 0")
			}
		}
		key := recordDomainKey{zone: zone[0], id: domain[0]}
		t.pool.rows[key] = append(t.pool.rows[key], rows...)
		t.j.record(recordEvInsert)
		if t.pool.insertTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.insertTag)), nil
		}
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", n)), nil
	case recordZoneShiftSQL:
		// The zone counters' delta by the row's old state (T-178):
		// the zone + the pending/queued/checked deltas (the result write
		// does not change total; zero deltas — the column untouched).
		zone := args[0].(int16)
		z, ok := t.pool.zones[zone]
		if !ok {
			return pgconn.NewCommandTag("UPDATE 0"), nil // the zone row is mandatory
		}
		z.pending += args[1].(int64)
		z.queued += args[2].(int64)
		z.checked += args[3].(int64)
		t.j.record(recordEvShift)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case recordChecksBumpSQL:
		// The daily checks counter's increment (T-179): the day row's
		// upsert — the name is computed by the DB, the fake keeps one value. The
		// upsert's RowsAffected is always 1 (the conflict branch creates the row).
		t.pool.checks += args[0].(int64)
		t.j.record(recordEvChecks)
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordTempErrBumpSQL:
		// The daily temp_error numerator's increment (T-182): the same pair —
		// only temp_error episodes run the operator (the caller
		// guarantees the status), the fake keeps one day row value.
		t.pool.tempErr += args[0].(int64)
		t.j.record(recordEvTempErr)
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordRetryMatureBumpSQL:
		// The retry maturity bucket's increment (T-202): the third pair —
		// only temp_error episodes run it (the caller
		// guarantees the status), the fake keeps one day bucket value;
		// matureTag — the RowsAffected ≠ 1 guard branch.
		t.pool.retryMature += args[0].(int64)
		t.j.record(recordEvMature)
		if t.pool.matureTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.matureTag)), nil
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordReanalyzeBumpSQL:
		// The unanalyzed tails bucket's increment (T-204): the delta —
		// the episode's inserted rows (0 — the operator is not called at all:
		// all the inserted rows carry rules_version = NULL), the fake keeps
		// one hour bucket value; reanTag — the guard branch of
		// RowsAffected ≠ 1.
		t.pool.reanalyze += args[0].(int64)
		t.j.record(recordEvRean)
		if t.pool.reanTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.reanTag)), nil
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordResultsBumpSQL:
		// The hourly throughput bucket's increment (T-203): the final pair —
		// the delta equals the episode's inserted rows count (0 — the operator is not
		// called at all), the fake keeps one hour bucket value;
		// resultsTag — the RowsAffected ≠ 1 guard branch.
		t.pool.results += args[0].(int64)
		t.j.record(recordEvResults)
		if t.pool.resultsTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.resultsTag)), nil
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	default:
		if !strings.HasPrefix(sql, recordDelRefSQLPrefix) {
			return pgconn.CommandTag{}, fmt.Errorf("recordFakePool: an unexpected SQL: %s", sql)
		}
		// sqlMetaDelRef (SwapRefTx, T-044): −1 with the refcount > 0 guard.
		var h content.Hash
		copy(h[:], args[0].([]byte))
		if t.pool.refc[h] <= 0 {
			return pgconn.NewCommandTag("UPDATE 0"), nil // ErrRefIntegrity at the caller
		}
		t.pool.refc[h]--
		t.j.record(recordEvSwap)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
}

func (t *recordFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != recordDeleteSQL {
		return nil, fmt.Errorf("recordFakePool: an unexpected SQL: %s", sql)
	}
	if t.pool.deleteErr != nil {
		return nil, t.pool.deleteErr
	}
	key := recordDomainKey{zone: args[0].(int16), id: args[1].(int64)}
	old := t.pool.rows[key]
	delete(t.pool.rows, key)
	t.j.record(recordEvDelete)

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

func (t *recordFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.committed = true
	t.j.record(recordEvCommit)
	return nil
}

func (t *recordFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.committed {
		return nil // a rollback after the Commit — a no-op
	}
	t.pool.domains, t.pool.rows, t.pool.refc, t.pool.zones = t.snapDomains, t.snapRows, t.snapRefc, t.snapZones
	t.pool.checks = t.snapChecks
	t.pool.tempErr = t.snapTempErr
	t.pool.retryMature = t.snapMature
	t.pool.results = t.snapResults
	return nil
}

// recordFakeRows — pgx.Rows with preloaded rows (one
// content_hash column); a positional Scan into *[]byte (the resultFakeRows T-039 style).
type recordFakeRows struct {
	rows [][]any
	i    int
}

func recordFakeRowsOf(rows ...[]any) *recordFakeRows { return &recordFakeRows{rows: rows} }

func (r *recordFakeRows) Close()                                       {}
func (r *recordFakeRows) Err() error                                   { return nil }
func (r *recordFakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *recordFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *recordFakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *recordFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("recordFakeRows: Scan without Next")
	}
	if len(dest) != 1 {
		return fmt.Errorf("recordFakeRows: %d destinations, one column (content_hash)", len(dest))
	}
	switch d := dest[0].(type) {
	case *[]byte:
		if r.rows[r.i-1][0] == nil {
			*d = nil
		} else {
			*d = r.rows[r.i-1][0].([]byte)
		}
	default:
		return fmt.Errorf("recordFakeRows: the destination %T, a *[]byte expected", dest[0])
	}
	return nil
}

func (r *recordFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *recordFakeRows) RawValues() [][]byte    { return nil }
func (r *recordFakeRows) Conn() *pgx.Conn        { return nil }
func (r *recordFakeRows) TypeMap() *pgtype.Map   { return nil }

// --- the fake queue -------------------------------------------------------

// recordPublishRec — a recorded publication.
type recordPublishRec struct {
	stream string
	msg    queue.Result
}

// recordFakeQueue — a queue.Queue: Publish records the Result messages (and
// may fail by the queue), Consume hands out the preloaded deliveries once
// and then parks until the ctx cancellation, Ack is recorded and signals.
type recordFakeQueue struct {
	j *recordJournal

	deliveries []queue.Delivery

	publishErrs []error

	mu        sync.Mutex
	publishes []recordPublishRec
	acked     []string
}

var _ queue.Queue = (*recordFakeQueue)(nil)

func (q *recordFakeQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	res, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("recordFakeQueue: an unexpected message %T (a queue.Result is needed)", msg)
	}
	q.mu.Lock()
	i := len(q.publishes)
	q.publishes = append(q.publishes, recordPublishRec{stream: stream, msg: res})
	q.mu.Unlock()
	q.j.record(recordEvPublish)
	if i < len(q.publishErrs) && q.publishErrs[i] != nil {
		return "", q.publishErrs[i]
	}
	return fmt.Sprintf("entry-analyze-%d", i+1), nil
}

func (q *recordFakeQueue) Consume(ctx context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
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

func (q *recordFakeQueue) Ack(ctx context.Context, _, _ string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked = append(q.acked, ids...)
	q.mu.Unlock()
	for range ids {
		q.j.record(recordEvAck)
	}
	q.j.signalAck()
	return nil
}

func (q *recordFakeQueue) publishesSnapshot() []recordPublishRec {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]recordPublishRec, len(q.publishes))
	copy(out, q.publishes)
	return out
}

func (q *recordFakeQueue) ackedSnapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, len(q.acked))
	copy(out, q.acked)
	return out
}

// --- the builders ---------------------------------------------------------------

// recordTestHash — distinguishable hashes: recordTestHash(1), (2), …
func recordTestHash(n byte) content.Hash {
	var h content.Hash
	h[0] = n
	return h
}

// recordRichResult — a result with two subdomains and all the record fields
// (§5.5 item 7): the apex — the full set (TLS, the chain, the headers, cookies,
// the stored content's hash), www — an alias redirect to the apex with a CNAME,
// a truncated body (ErrBody at a live status) and a NULL content_hash
// (the class's content is stored at the original, T-055/T-056).
func recordRichResult() (*DomainResult, map[string]content.Hash) {
	hApex := recordTestHash(1)
	final := "https://rich.example/"
	aliasTo := int16(0)
	res := &DomainResult{
		ZoneID: 7, DomainID: 91, Name: "rich.example",
		Subdomains: []SubdomainResult{
			{
				SubdomainID: 0, Label: "", Hostname: "rich.example",
				Addrs: []netip.Addr{
					netip.MustParseAddr("192.0.2.1"),
					netip.MustParseAddr("2001:db8::1"),
				},
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 0, Label: "",
					FinalURL: final, Host: "rich.example", Path: "/",
					HTTPStatus: 200,
					Header: http.Header{
						"Content-Type": []string{"text/html; charset=utf-8"},
						"Server":       []string{"nginx"},
					},
					Cookies: []*http.Cookie{
						{Name: "laravel_session", Value: "s3", Path: "/", Secure: true, HttpOnly: true},
						{Name: "XSRF-TOKEN", Value: "tok"},
					},
					TLS: &httpfetch.TLSInfo{
						Valid:             true,
						SubjectCommonName: "rich.example",
						IssuerCommonName:  "R11",
						NotBefore:         time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
						NotAfter:          time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
						DNSNames:          []string{"rich.example", "www.rich.example"},
						ChainLength:       2,
					},
					ResponseMS: 123,
					Redirects: []httpfetch.RedirectStep{
						{URL: "http://rich.example/", Status: 301},
						{URL: final, Status: 200},
					},
					ContentHash:  &hApex,
					StoreContent: true,
					ErrClass:     ErrNone,
				}},
			},
			{
				SubdomainID: 1, Label: "www", Hostname: "www.rich.example",
				Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.1")},
				CNAME: "rich.example",
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 1, Label: "www",
					FinalURL: final, Host: "rich.example", Path: "/",
					HTTPStatus: 200,
					Header:     http.Header{"Server": []string{"nginx"}},
					ResponseMS: 456,
					Redirects:  []httpfetch.RedirectStep{{URL: "https://www.rich.example/", Status: 301}, {URL: final, Status: 200}},
					Truncated:  true,
					// The truncated page's alias after Save: the
					// class's content — at the original (T-055), ContentNotStored
					// is not set for aliases (the R8 matrix, T-266/T-056).
					AliasKind:        model.AliasRedirect,
					AliasSubdomainID: &aliasTo,
					ErrClass:         ErrBody,
				}},
			},
		},
	}
	// A fake hash: content.HashOf is not needed, the distinguishability matters.
	return res, map[string]content.Hash{"apex": hApex}
}

// recordSilentResult — a domain without records: all the checks were silent.
func recordSilentResult(timeout bool) *DomainResult {
	class := ErrNetwork
	if timeout {
		class = ErrTimeout
	}
	return &DomainResult{
		ZoneID: 7, DomainID: 92, Name: "silent.example",
		Subdomains: []SubdomainResult{{
			SubdomainID: 0, Label: "", Hostname: "silent.example",
			Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.9")},
			HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: class},
			HTTP:  SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: class},
		}},
	}
}

func recordRecorder(t *testing.T, pool *recordFakePool, q *recordFakeQueue) *Recorder {
	t.Helper()
	r, err := NewRecorder(pool, q, nil, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	return r
}

// --- the fields' completeness and the operation order ----------------------------------------

// TestRecordWritesAllColumns — the full result: the subdomain_results rows
// carry all the columns (§5.5 item 7), the domain — alive/done with the lease reset and
// temp_attempts, q:analyze — one Result per record; the "write →
// commit → publication" order; the addresses are packed 16 octets each.
func TestRecordWritesAllColumns(t *testing.T) {
	pool := newRecordFakePool()
	pool.recordSeedDomain(7, 91, model.StatusNew, 3)
	zone := pool.recordSeedZone(7, 10, 2, 3, 5) // the zone counters before the write (T-178)
	q := &recordFakeQueue{j: pool.j}
	res, hashes := recordRichResult()

	ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}

	// The zone counters' delta by the same transaction (T-178, incremental-counters spec §6.4): the domain
	// was queued → queued −1, checked +1, total/pending untouched.
	if zone.queued != 2 || zone.checked != 6 || zone.total != 10 || zone.pending != 2 {
		t.Fatalf("the zone counters = %+v, want {10 2 2 6} (queued −1, checked +1)", *zone)
	}

	// The daily checks counter (T-179): one episode — +1, not +2 by
	// the number of subdomain×variant records (the delta — the domain's UPDATE
	// RowsAffected, not the INSERT chunk's); the delta in the same transaction — after
	// the zone shift, before the commit. The temp_error numerator (T-182) is untouched by a
	// live (alive) episode — only temp_error
	// outcomes run the operator.
	if got := pool.checksValue(); got != 1 {
		t.Fatalf("the daily counter = %d, want 1 (one episode of two records)", got)
	}
	if got := pool.tempErrValue(); got != 0 {
		t.Fatalf("the daily temp_error numerator = %d, want 0 (an alive episode)", got)
	}
	if shift, checks := pool.j.indexOf(recordEvShift), pool.j.indexOf(recordEvChecks); shift > checks || checks > pool.j.indexOf(recordEvCommit) {
		t.Fatalf("the counter order: shift=%d checks=%d commit=%d (the zone → checks → commit)",
			shift, checks, pool.j.indexOf(recordEvCommit))
	}

	dom := pool.domains[recordDomainKey{zone: 7, id: 91}]
	if dom.status != model.StatusAlive || dom.checkState != model.CheckStateDone {
		t.Fatalf("the domain: status=%v check_state=%v, alive/done expected", dom.status, dom.checkState)
	}
	if dom.queuedAt != nil || !dom.lastChecked {
		t.Fatalf("the domain: queued_at=%v last_checked=%v, NULL/filled expected", dom.queuedAt, dom.lastChecked)
	}
	if dom.tempAttempts != 0 {
		t.Fatalf("temp_attempts=%d, a reset to 0 expected (the T-031 contract)", dom.tempAttempts)
	}

	rows := pool.rows[recordDomainKey{zone: 7, id: 91}]
	if len(rows) != 2 {
		t.Fatalf("%d rows, 2 expected", len(rows))
	}
	apex, www := rows[0], rows[1]

	if apex.subdomainID != 0 || apex.variant != model.VariantMain || apex.scheme != model.SchemeHTTPS {
		t.Fatalf("the apex: the key (%d,%v,%v)", apex.subdomainID, apex.variant, apex.scheme)
	}
	if apex.httpStatus == nil || *apex.httpStatus != 200 {
		t.Fatalf("the apex: http_status=%v", apex.httpStatus)
	}
	if apex.finalURL == nil || *apex.finalURL != "https://rich.example/" {
		t.Fatalf("the apex: final_url=%v", apex.finalURL)
	}
	if apex.aliasKind != model.AliasNone || apex.aliasSub != nil {
		t.Fatalf("the apex: aliases %v/%v", apex.aliasKind, apex.aliasSub)
	}
	// addrs: v4-mapped (::ffff:192.0.2.1) + IPv6, 16 octets each.
	if len(apex.addrs) != 32 {
		t.Fatalf("the apex: addrs %d bytes, 32 expected (2×16)", len(apex.addrs))
	}
	want4 := netip.MustParseAddr("192.0.2.1").As16()
	want6 := netip.MustParseAddr("2001:db8::1").As16()
	if string(apex.addrs[:16]) != string(want4[:]) || string(apex.addrs[16:]) != string(want6[:]) {
		t.Fatalf("the apex: addrs did not match the v4-mapped+IPv6 packing: %x", apex.addrs)
	}
	if apex.cname != nil {
		t.Fatalf("the apex: cname=%v, NULL expected", *apex.cname)
	}
	for col, v := range map[string]*string{"tls": apex.tls, "redirects": apex.redirects, "headers": apex.headers, "cookies": apex.cookies} {
		if v == nil {
			t.Fatalf("the apex: %s = NULL, a jsonb expected", col)
		}
	}
	if !strings.Contains(*apex.tls, `"subject_cn":"rich.example"`) || !strings.Contains(*apex.tls, `"dns_names"`) {
		t.Fatalf("the apex: the tls jsonb: %s", *apex.tls)
	}
	if !strings.Contains(*apex.redirects, `"url":"http://rich.example/","status":301`) {
		t.Fatalf("the apex: the redirects jsonb: %s", *apex.redirects)
	}
	if !strings.Contains(*apex.headers, `"Content-Type":["text/html; charset=utf-8"]`) {
		t.Fatalf("the apex: the headers jsonb: %s", *apex.headers)
	}
	if !strings.Contains(*apex.cookies, `"name":"laravel_session"`) || !strings.Contains(*apex.cookies, `"http_only":true`) {
		t.Fatalf("the apex: the cookies jsonb: %s", *apex.cookies)
	}
	if apex.rendered {
		t.Fatal("the apex: rendered=true, the render is T-067")
	}
	wantApex := hashes["apex"]
	if string(apex.contentHash) != string(wantApex[:]) {
		t.Fatalf("the apex: content_hash %x, %x expected", apex.contentHash, wantApex)
	}
	if apex.errCode != nil {
		t.Fatalf("the apex: error=%v, NULL expected (ErrNone)", *apex.errCode)
	}
	if apex.responseMS == nil || *apex.responseMS != 123 {
		t.Fatalf("the apex: response_ms=%v", apex.responseMS)
	}

	// www: an alias redirect to the apex, a CNAME, a truncated body — the error
	// code and the NULL hash (T-056).
	if www.aliasKind != model.AliasRedirect || www.aliasSub == nil || *www.aliasSub != 0 {
		t.Fatalf("www: the alias %v/%v", www.aliasKind, www.aliasSub)
	}
	if www.cname == nil || *www.cname != "rich.example" {
		t.Fatalf("www: cname=%v", www.cname)
	}
	if www.contentHash != nil {
		t.Fatal("www: content_hash of a truncated body, NULL expected")
	}
	if www.errCode == nil || *www.errCode != int16(ErrBody) {
		t.Fatalf("www: error=%v, ErrBody expected", www.errCode)
	}
	if www.responseMS == nil || *www.responseMS != 456 {
		t.Fatalf("www: response_ms=%v", www.responseMS)
	}
	if len(www.addrs) != 16 {
		t.Fatalf("www: addrs %d bytes, 16 expected", len(www.addrs))
	}

	// The SQL contract: the schema literals and the operators' structure.
	if !strings.Contains(recordDomainSQL, "check_state = 2") ||
		!strings.Contains(recordDomainSQL, "queued_at = NULL") ||
		!strings.Contains(recordDomainSQL, "last_checked_at = now()") ||
		!strings.Contains(recordDomainSQL, "t.status <> 6") ||
		!strings.Contains(recordDomainSQL, "temp_attempts = CASE") {
		t.Fatalf("recordDomainSQL diverged from the §6.1/T-031 contract:\n%s", recordDomainSQL)
	}
	if !strings.Contains(recordInsertSQL, "false, z.content_hash, z.error, z.response_ms, now(), NULL") ||
		!strings.Contains(recordInsertSQL, "::jsonb[]") || !strings.Contains(recordInsertSQL, "::bytea[]") {
		t.Fatalf("recordInsertSQL diverged from the §6.1 columns:\n%s", recordInsertSQL)
	}
	// T-210: render_failed_at is not in the INSERT — the episode's
	// rewrite (DELETE + INSERT) resets the "render impossible" marker: the new
	// content has the right to a render (DEFAULT NULL).
	if strings.Contains(recordInsertSQL, "render_failed_at") {
		t.Fatalf("recordInsertSQL writes render_failed_at — the T-210 marker must be reset by the episode rewrite (DEFAULT NULL):\n%s", recordInsertSQL)
	}
	if !strings.Contains(recordDeleteSQL, "RETURNING content_hash") {
		t.Fatalf("recordDeleteSQL without RETURNING content_hash (the T-044 reference swap):\n%s", recordDeleteSQL)
	}

	// The publication: after the transaction's commit, one message per record.
	pubs := q.publishesSnapshot()
	if len(pubs) != 2 {
		t.Fatalf("%d q:analyze publications, 2 expected", len(pubs))
	}
	for i, want := range []struct {
		sub     int16
		variant model.Variant
	}{{0, model.VariantMain}, {1, model.VariantMain}} {
		got := pubs[i]
		if got.stream != queue.StreamAnalyze {
			t.Fatalf("publication %d: the stream %s", i, got.stream)
		}
		if got.msg.ZoneID != 7 || got.msg.DomainID != 91 || got.msg.SubdomainID != want.sub ||
			got.msg.Variant != want.variant || got.msg.Name != "rich.example" {
			t.Fatalf("publication %d: %+v", i, got.msg)
		}
	}
	ev := pool.j.snapshot()
	for _, pair := range [][2]recordEventKind{
		{recordEvDomains, recordEvDelete}, {recordEvDelete, recordEvInsert},
		{recordEvInsert, recordEvCommit}, {recordEvCommit, recordEvPublish},
	} {
		if pool.j.indexOf(pair[0]) > pool.j.indexOf(pair[1]) {
			t.Fatalf("the event order is violated: %v after %v (%v)", pair[0], pair[1], ev)
		}
	}
}

// --- the domain statuses (§5.5 item 9, §7) ------------------------------------------

// recordClassSub — a subdomain without records with both protocols'
// error classes (the pure recordStatus decision table).
func recordClassSub(https, http ErrorClass) SubdomainResult {
	return SubdomainResult{
		SubdomainID: 0, Label: "", Hostname: "t.example",
		Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.9")},
		HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: https},
		HTTP:  SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: http},
	}
}

// TestRecordStatusDecision — the HTTP stage's branch of the status automaton's decision table
// (T-256, TZ-fsm spec §3 FR-2; the specification — the doc comment at
// recordDomainSQL, the common part — internal/model/enums.go): the pure
// function recordStatus over the B1–B3 rows, including the A2
// asymmetry (connect-refused → no_http against a connect timeout → temp_error) and
// the B3 non-temp classes (the cutoff synthetics do not breed temp_error).
func TestRecordStatusDecision(t *testing.T) {
	recorded := recordClassSub(ErrNone, ErrNone)
	recorded.Records = []Record{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}}
	cases := []struct {
		name string
		res  *DomainResult
		want model.Status
	}{
		{"V1 (O7): there are records — alive", &DomainResult{Subdomains: []SubdomainResult{recorded}}, model.StatusAlive},
		{"V2 (O8): no records, a connect timeout — temp_error (the A2 asymmetry)",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrTimeout, ErrNetwork)}}, model.StatusTempError},
		{"V2 (O8): a timeout of the second protocol is temp_error too",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrNone, ErrTimeout)}}, model.StatusTempError},
		{"V3 (O9): no records, connection refused — no_http (the A2 asymmetry)",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrNetwork, ErrNetwork)}}, model.StatusNoHTTP},
		{"V3 (O9): a TLS handshake failure — no_http",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrTLS, ErrNone)}}, model.StatusNoHTTP},
		{"V3 (O9): the redirect limit — no_http",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrRedirects, ErrRedirects)}}, model.StatusNoHTTP},
		{"V3 (O9): a body truncation — no_http",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrBody, ErrNone)}}, model.StatusNoHTTP},
		{"V3 (O9): the dead-host cutoff synthetic — no_http (ErrCutoff is not temp)",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrCutoff, ErrCutoff)}}, model.StatusNoHTTP},
		{"V2 dominates V3 across subdomains: a timeout of one — temp_error",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrNone, ErrNone), recordClassSub(ErrTimeout, ErrTimeout)}}, model.StatusTempError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := recordStatus(tc.res); got != tc.want {
				t.Fatalf("recordStatus = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRecordStatuses(t *testing.T) {
	cases := []struct {
		name string
		res  *DomainResult
		want model.Status
	}{
		{"there are records — alive", func() *DomainResult { r, _ := recordRichResult(); return r }(), model.StatusAlive},
		{"silent constantly — no_http", recordSilentResult(false), model.StatusNoHTTP},
		{"silent with timeouts — temp_error", recordSilentResult(true), model.StatusTempError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newRecordFakePool()
			pool.recordSeedDomain(tc.res.ZoneID, tc.res.DomainID, model.StatusNew, 2)
			q := &recordFakeQueue{j: pool.j}
			if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), tc.res); err != nil {
				t.Fatalf("RecordAndPublish: %v", err)
			}
			dom := pool.domains[recordDomainKey{zone: tc.res.ZoneID, id: tc.res.DomainID}]
			if dom.status != tc.want {
				t.Fatalf("status=%v, %v expected", dom.status, tc.want)
			}
			if dom.tempAttempts != 0 && tc.want != model.StatusTempError {
				t.Fatalf("temp_attempts=%d at a non-temp_error outcome", dom.tempAttempts)
			}
		})
	}

	t.Run("temp_error keeps the retry counter", func(t *testing.T) {
		pool := newRecordFakePool()
		res := recordSilentResult(true)
		pool.recordSeedDomain(res.ZoneID, res.DomainID, model.StatusNew, 4)
		q := &recordFakeQueue{j: pool.j}
		if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res); err != nil {
			t.Fatalf("RecordAndPublish: %v", err)
		}
		if got := pool.domains[recordDomainKey{zone: res.ZoneID, id: res.DomainID}].tempAttempts; got != 4 {
			t.Fatalf("temp_attempts=%d, temp_error must keep the accumulated value (T-031)", got)
		}
	})

	t.Run("without records no rows are inserted, the old ones are cleaned", func(t *testing.T) {
		pool := newRecordFakePool()
		res := recordSilentResult(false)
		key := recordDomainKey{zone: res.ZoneID, id: res.DomainID}
		pool.recordSeedDomain(res.ZoneID, res.DomainID, model.StatusAlive, 0)
		oldHash := recordTestHash(9)
		pool.rows[key] = []recordFakeRow{{subdomainID: 0, variant: model.VariantMain, contentHash: oldHash[:]}}
		pool.refc[oldHash] = 1
		q := &recordFakeQueue{j: pool.j}
		if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res); err != nil {
			t.Fatalf("RecordAndPublish: %v", err)
		}
		if len(pool.rows[key]) != 0 {
			t.Fatalf("rows after no_http: %d, 0 expected (§3.5 — only the last result)", len(pool.rows[key]))
		}
		if pool.j.count(recordEvInsert) != 0 {
			t.Fatal("no INSERT is needed with an empty result")
		}
		if pool.refc[oldHash] != 0 {
			t.Fatalf("the old content refcount %d, 0 expected", pool.refc[oldHash])
		}
		if got := len(q.publishesSnapshot()); got != 0 {
			t.Fatalf("%d publications — there is nothing to publish", got)
		}
	})
}

// TestRecordCutoffFlag — the dead_host_cutoff bit of the last episode (T-164):
// a cutoff result sets the bit, a usual one removes it (the false
// cutoffs' control sample selects the domains by the bit mask); the other bits untouched.
func TestRecordCutoffFlag(t *testing.T) {
	pool := newRecordFakePool()
	key := recordDomainKey{zone: 7, id: 92}
	dom := pool.recordSeedDomain(key.zone, key.id, model.StatusNew, 0)
	dom.flags.Set(model.FlagRenderPending) // a foreign bit must survive
	q := &recordFakeQueue{j: pool.j}

	cut := recordSilentResult(false)
	cut.Cutoff = true
	if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), cut); err != nil {
		t.Fatalf("RecordAndPublish (cutoff): %v", err)
	}
	if !dom.cutoff || !dom.flags.IsDeadHostCutoff() {
		t.Fatalf("after the cutoff episode the dead_host_cutoff bit is not set: flags=%s", dom.flags)
	}
	if !dom.flags.IsRenderPending() {
		t.Fatalf("the CASE flags wiped a foreign render_pending bit: flags=%s", dom.flags)
	}

	// A usual episode (a full check at the T-030 recheck) removes the bit.
	full := recordSilentResult(false)
	if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), full); err != nil {
		t.Fatalf("RecordAndPublish (a full episode): %v", err)
	}
	if dom.cutoff || dom.flags.IsDeadHostCutoff() {
		t.Fatalf("after the full episode the dead_host_cutoff bit is set: flags=%s", dom.flags)
	}
	if !dom.flags.IsRenderPending() {
		t.Fatalf("the CASE flags wiped a foreign render_pending bit: flags=%s", dom.flags)
	}

	// The SQL contract: the bit and the mask — as the model's literals.
	if !strings.Contains(recordDomainSQL, fmt.Sprintf("(t.flags | %d)::int2", int16(model.FlagDeadHostCutoff))) ||
		!strings.Contains(recordDomainSQL, fmt.Sprintf("(t.flags & %d)::int2", int16(^int16(model.FlagDeadHostCutoff)))) {
		t.Fatalf("recordDomainSQL without the dead_host_cutoff bit CASE (T-164):\n%s", recordDomainSQL)
	}
}

// TestRecordTempErrCounter — the daily temp_error numerator (T-182) and
// the retry maturity bucket (T-202): the increment — only at the episode's temp_error
// outcome (the same transaction, after the checks: denominator, before the commit —
// the order shared with the reconciliation pass); a no_http outcome does not
// touch the day row; a temp_error episode's duplicate delivery — one more event in
// the numerator, the denominator and the bucket at once (the share is resilient,
// the retry decrement levels the bucket between the passes, the reconciliation
// fixes the absolutes, TZ_slow_dns_share §5 / T-202).
func TestRecordTempErrCounter(t *testing.T) {
	pool := newRecordFakePool()
	q := &recordFakeQueue{j: pool.j}
	r := recordRecorder(t, pool, q)

	// A temp_error episode: +1 denominator, +1 numerator, +1 bucket;
	// the increments — between the denominator and the commit, the bucket —
	// after the numerator (the reconciliation pass's lock order).
	temp := recordSilentResult(true)
	pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
	if ok, err := r.RecordAndPublish(context.Background(), temp); err != nil || !ok {
		t.Fatalf("the temp_error episode: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 1 {
		t.Fatalf("after the temp_error episode checks = %d, want 1", got)
	}
	if got := pool.tempErrValue(); got != 1 {
		t.Fatalf("after the temp_error episode temp_error = %d, want 1", got)
	}
	if got := pool.retryMatureValue(); got != 1 {
		t.Fatalf("after the temp_error episode retry_mature = %d, want 1", got)
	}
	checks, tempBump := pool.j.indexOf(recordEvChecks), pool.j.indexOf(recordEvTempErr)
	matureBump, commit := pool.j.indexOf(recordEvMature), pool.j.indexOf(recordEvCommit)
	if tempBump < checks || matureBump < tempBump || matureBump > commit {
		t.Fatalf("the order: checks=%d temp=%d mature=%d commit=%d (checks → temp_error → retry_mature → commit)",
			checks, tempBump, matureBump, commit)
	}

	// A no_http episode: the denominator grows, the numerator and the bucket untouched —
	// only temp_error outcomes run the operators.
	silent := recordSilentResult(false)
	pool.recordSeedDomain(silent.ZoneID, silent.DomainID, model.StatusNew, 0)
	if ok, err := r.RecordAndPublish(context.Background(), silent); err != nil || !ok {
		t.Fatalf("the no_http episode: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 2 {
		t.Fatalf("after the no_http episode checks = %d, want 2", got)
	}
	if got := pool.tempErrValue(); got != 1 {
		t.Fatalf("after the no_http episode temp_error = %d, want 1 (a non-temp_error outcome does not increment)", got)
	}
	if got := pool.retryMatureValue(); got != 1 {
		t.Fatalf("after the no_http episode retry_mature = %d, want 1 (only temp_error episodes ripen)", got)
	}
	if n := pool.j.count(recordEvTempErr); n != 1 {
		t.Fatalf("%d temp_error increments, want 1", n)
	}
	if n := pool.j.count(recordEvMature); n != 1 {
		t.Fatalf("%d bucket increments, want 1", n)
	}

	// A temp_error episode's duplicate delivery: +1 into the numerator, the denominator and
	// the bucket together — a write event; the reconciliation levels
	// the drift against the live domain count (T-202).
	if ok, err := r.RecordAndPublish(context.Background(), temp); err != nil || !ok {
		t.Fatalf("the temp_error episode repeat: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 3 {
		t.Fatalf("after the repeat checks = %d, want 3", got)
	}
	if got := pool.tempErrValue(); got != 2 {
		t.Fatalf("after the repeat temp_error = %d, want 2 (one event per record)", got)
	}
	if got := pool.retryMatureValue(); got != 2 {
		t.Fatalf("after the repeat retry_mature = %d, want 2 (one event per record)", got)
	}
}

// TestRecordResultsCounter — the hourly throughput bucket (T-203): the delta —
// the episode's inserted rows (not episodes: a domain with two records gives
// +2, a silent one — 0 and the operator does not run at all); the order — the
// finale of the counter delta chain (after retry_mature: and reanalyze:, before
// the commit), shared with the reconciliation pass.
func TestRecordResultsCounter(t *testing.T) {
	pool := newRecordFakePool()
	q := &recordFakeQueue{j: pool.j}
	r := recordRecorder(t, pool, q)

	// A rich episode: two subdomain×variant records → the buckets +2/+2
	// (the reanalysis tails T-204 — the same rows with rules_version = NULL,
	// the T-203 throughput — the same rows as insertion events).
	rich, _ := recordRichResult()
	pool.recordSeedDomain(rich.ZoneID, rich.DomainID, model.StatusNew, 0)
	if ok, err := r.RecordAndPublish(context.Background(), rich); err != nil || !ok {
		t.Fatalf("the rich episode: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 2 {
		t.Fatalf("after the rich episode results = %d, want 2 (two inserted rows)", got)
	}
	if got := pool.reanalyzeValue(); got != 2 {
		t.Fatalf("after the rich episode reanalyze = %d, want 2 (each inserted row is a tail until the stamp, T-204)", got)
	}
	resultsBump, commit := pool.j.indexOf(recordEvResults), pool.j.indexOf(recordEvCommit)
	matureBump := pool.j.indexOf(recordEvMature)
	reanBump := pool.j.indexOf(recordEvRean)
	if resultsBump > commit || (matureBump >= 0 && resultsBump < matureBump) || reanBump > resultsBump {
		t.Fatalf("the order: mature=%d reanalyze=%d results=%d commit=%d (… → retry_mature: → reanalyze: → results: → commit)",
			matureBump, reanBump, resultsBump, commit)
	}

	// A silent episode (temp_error, without records): no rows — the
	// bucket operators do not run at all (zero upsert passes do not warm
	// the hours' locks).
	silent := recordSilentResult(true)
	pool.recordSeedDomain(silent.ZoneID, silent.DomainID, model.StatusNew, 1)
	if ok, err := r.RecordAndPublish(context.Background(), silent); err != nil || !ok {
		t.Fatalf("the silent episode: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 2 {
		t.Fatalf("after the silent episode results = %d, want 2 (an episode without rows does not touch the bucket)", got)
	}
	if got := pool.reanalyzeValue(); got != 2 {
		t.Fatalf("after the silent episode reanalyze = %d, want 2 (an episode without rows does not touch the bucket)", got)
	}
	if n := pool.j.count(recordEvResults); n != 1 {
		t.Fatalf("%d rate bucket increments, want 1 (a zero delta is not executed)", n)
	}
	if n := pool.j.count(recordEvRean); n != 1 {
		t.Fatalf("%d tails bucket increments, want 1 (a zero delta is not executed)", n)
	}

	// A rich episode repeat (a duplicate delivery): two more insertion events into
	// both buckets — a live window would count the same 2 rows; the
	// semantics' difference is leveled by the reconciliation (streaming, as at the daily rows).
	// A repeated delivery replays the content storing too (the style of
	// TestRecordIdempotentRepeat): a repeated Put gives +1 refcount.
	_, hashes := recordRichResult()
	pool.refc[hashes["apex"]] = 2
	if ok, err := r.RecordAndPublish(context.Background(), rich); err != nil || !ok {
		t.Fatalf("the rich episode repeat: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 4 {
		t.Fatalf("after the repeat results = %d, want 4 (an insert event per delivery)", got)
	}
	if got := pool.reanalyzeValue(); got != 4 {
		t.Fatalf("after the repeat reanalyze = %d, want 4 (an insert event per delivery)", got)
	}
}

// TestRecordReanalyzeCounterErrors — the tails bucket upsert's RowsAffected ≠ 1
// guard branch (T-204): a step error rolls back the transaction
// entirely (no rows and no status), the delivery will return by a repeat.
func TestRecordReanalyzeCounterErrors(t *testing.T) {
	pool := newRecordFakePool()
	pool.reanTag = 2 // a foreign SQL: 2 rows affected
	rich, _ := recordRichResult()
	pool.recordSeedDomain(rich.ZoneID, rich.DomainID, model.StatusNew, 0)
	q := &recordFakeQueue{j: pool.j}

	ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), rich)
	if err == nil || !strings.Contains(err.Error(), "reanalysis tails bucket") {
		t.Fatalf("the error = %v, want the tails bucket context", err)
	}
	if ok {
		t.Fatal("at a statement failure the result does not count as written")
	}
	if _, exists := pool.rows[recordDomainKey{zone: rich.ZoneID, id: rich.DomainID}]; exists {
		t.Fatal("the episode rows were written at a bucket failure (the §3.4 rollback)")
	}
}

// --- the deleted guard (§8/§13.1) ----------------------------------------------

func TestRecordDeletedGuard(t *testing.T) {
	pool := newRecordFakePool()
	res, _ := recordRichResult()
	key := recordDomainKey{zone: res.ZoneID, id: res.DomainID}
	dom := pool.recordSeedDomain(res.ZoneID, res.DomainID, model.StatusAlive, 1)
	dom.status = model.StatusDeleted
	pool.rows[key] = []recordFakeRow{{subdomainID: 0, variant: model.VariantMain}}
	q := &recordFakeQueue{j: pool.j}

	ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
	if err != nil {
		t.Fatalf("a deleted domain — not an error: %v", err)
	}
	if ok {
		t.Fatal("the write must report \"not written\"")
	}
	if dom.status != model.StatusDeleted || dom.checkState != model.CheckStateQueued {
		t.Fatalf("the domain was changed: %v/%v", dom.status, dom.checkState)
	}
	if len(pool.rows[key]) != 1 {
		t.Fatalf("the old rows were touched (%d), no result is written for a deleted domain", len(pool.rows[key]))
	}
	if pool.j.count(recordEvInsert) != 0 || pool.j.count(recordEvDelete) != 0 {
		t.Fatal("the transaction must stop at the UPDATE guard")
	}
	// No result is written to the deleted — the daily counter does not grow either
	// (T-179: the delta only at an UPDATE RowsAffected > 0, incremental-counters spec §6.1.3);
	// the temp_error numerator — all the more (T-182).
	if pool.j.count(recordEvChecks) != 0 || pool.checksValue() != 0 ||
		pool.j.count(recordEvTempErr) != 0 || pool.tempErrValue() != 0 {
		t.Fatal("the daily counters were touched at the deleted guard")
	}
	if got := len(q.publishesSnapshot()); got != 0 {
		t.Fatalf("%d publications — publishing without a write is forbidden", got)
	}

	t.Run("an absent domain — the same path", func(t *testing.T) {
		ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), &DomainResult{
			ZoneID: 7, DomainID: 999, Name: "ghost.example",
			Subdomains: []SubdomainResult{{
				SubdomainID: 0, Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.1")},
				Records: []Record{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
			}},
		})
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v, false/nil expected", ok, err)
		}
	})
}

// --- the rewrite + SwapRef (T-044/T-056) ---------------------------------------

func TestRecordRewriteSwapsRefs(t *testing.T) {
	pool := newRecordFakePool()
	res, _ := recordRichResult()
	key := recordDomainKey{zone: 7, id: 91}
	pool.recordSeedDomain(7, 91, model.StatusAlive, 0)

	// The old check: the apex with content H1, www with content H2
	// (an old record version's alias could have stored its own block).
	h1, h2 := recordTestHash(1), recordTestHash(2)
	pool.refc[h1], pool.refc[h2] = 1, 1
	oldFinal := "https://old.rich.example/"
	pool.rows[key] = []recordFakeRow{
		{subdomainID: 0, variant: model.VariantMain, contentHash: h1[:], finalURL: &oldFinal},
		{subdomainID: 1, variant: model.VariantMain, contentHash: h2[:]},
		{subdomainID: 1, variant: model.VariantHTTP, scheme: model.SchemeHTTP}, // a key absent from the new outcome
	}
	// The apex's new content is already written by a Put (T-056): +1 from the Put.
	h3 := recordTestHash(3)
	pool.refc[h3] = 1

	q := &recordFakeQueue{j: pool.j}
	ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}

	if pool.refc[h1] != 0 || pool.refc[h2] != 0 {
		t.Fatalf("the old references: H1=%d H2=%d, 0/0 expected (the row is dead — T-042)", pool.refc[h1], pool.refc[h2])
	}
	if pool.refc[h3] != 1 {
		t.Fatalf("the new content refcount %d, 1 expected (+1 Put already existed, no new ones needed)", pool.refc[h3])
	}
	rows := pool.rows[key]
	if len(rows) != 2 {
		t.Fatalf("%d rows — the rewrite also deletes the disappeared keys (§3.5)", len(rows))
	}
	for _, r := range rows {
		if r.finalURL != nil && *r.finalURL == oldFinal {
			t.Fatal("an old row survived the rewrite")
		}
	}
	// The operator order: −1 of the old ones — before the INSERT, everything after the domain's UPDATE.
	for _, pair := range [][2]recordEventKind{
		{recordEvDomains, recordEvSwap}, {recordEvSwap, recordEvInsert}, {recordEvInsert, recordEvCommit},
	} {
		if pool.j.indexOf(pair[0]) > pool.j.indexOf(pair[1]) {
			t.Fatalf("the order is violated: %v after %v", pair[0], pair[1])
		}
	}
}

func TestRecordIdempotentRepeat(t *testing.T) {
	pool := newRecordFakePool()
	res, hashes := recordRichResult()
	hApex := hashes["apex"]
	key := recordDomainKey{zone: 7, id: 91}
	pool.recordSeedDomain(7, 91, model.StatusNew, 0)
	// A repeated delivery replays the storing too (T-056): a repeated Put
	// of the same content gives an extra +1.
	pool.refc[hApex] = 1
	q := &recordFakeQueue{j: pool.j}
	r := recordRecorder(t, pool, q)

	if ok, err := r.RecordAndPublish(context.Background(), res); err != nil || !ok {
		t.Fatalf("the first write: ok=%v err=%v", ok, err)
	}
	if pool.refc[hApex] != 1 {
		t.Fatalf("after the first write refcount=%d, 1 expected", pool.refc[hApex])
	}

	// The recheck scenario: the Put added the reference anew (refcount 2), the
	// rewrite removes the old row's reference — the outcome is balanced.
	pool.refc[hApex] = 2
	if ok, err := r.RecordAndPublish(context.Background(), res); err != nil || !ok {
		t.Fatalf("the repeated write: ok=%v err=%v (the idempotency, spec 3.4)", ok, err)
	}
	if pool.refc[hApex] != 1 {
		t.Fatalf("the refcount after the repeat=%d, a double release would go negative (T-044)", pool.refc[hApex])
	}
	if len(pool.rows[key]) != 2 {
		t.Fatalf("%d rows — the repeat must rewrite, not duplicate", len(pool.rows[key]))
	}
	// A repeat after a successful write publishes q:analyze anew: the
	// "written, not published" window is closed by the repeated delivery.
	if got := len(q.publishesSnapshot()); got != 4 {
		t.Fatalf("%d publications in total, 4 expected (2×2)", got)
	}
	// The daily counter counts the write events (T-179, TZ_slow_done_today
	// §5.1): a repeated delivery — one more event, +1 (not ×2 per
	// delivery); they converge to the live window by the reconciliation (the
	// integration scenario — TestCounters_RecordDoubleDelivery).
	if got := pool.checksValue(); got != 2 {
		t.Fatalf("the daily counter = %d, want 2 (one event per write)", got)
	}
}

// --- transactionality and step failures -------------------------------------------

func TestRecordTransactionFailures(t *testing.T) {
	res, _ := recordRichResult()
	seed := func(t *testing.T) (*recordFakePool, *recordFakeQueue) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		return pool, &recordFakeQueue{j: pool.j}
	}
	// assertRolledBack checks the rollback: the domain in the initial state,
	// oldRows rows (a reset to the snapshot), no publications.
	assertRolledBack := func(t *testing.T, pool *recordFakePool, q *recordFakeQueue, err error, oldRows int) {
		t.Helper()
		if err == nil {
			t.Fatal("a step error expected")
		}
		key := recordDomainKey{zone: 7, id: 91}
		if dom := pool.domains[key]; dom.checkState != model.CheckStateQueued || dom.status != model.StatusNew {
			t.Fatalf("the domain was changed at a failure: %v/%v — the transaction must roll back", dom.status, dom.checkState)
		}
		if len(pool.rows[key]) != oldRows {
			t.Fatalf("%d rows after the failure, %d expected — the snapshot rollback did not work", len(pool.rows[key]), oldRows)
		}
		if got := len(q.publishesSnapshot()); got != 0 {
			t.Fatalf("%d publications with an unwritten result", got)
		}
	}

	t.Run("Begin", func(t *testing.T) {
		pool, q := seed(t)
		pool.beginErr = errors.New("the connection is unavailable")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("UPDATE domains", func(t *testing.T) {
		pool, q := seed(t)
		pool.failExec = map[string]error{recordDomainSQL: errors.New("a lock")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("DELETE of the old rows", func(t *testing.T) {
		pool, q := seed(t)
		pool.deleteErr = errors.New("a read failure")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("INSERT", func(t *testing.T) {
		pool, q := seed(t)
		pool.failExec = map[string]error{recordInsertSQL: errors.New("a PK violation")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("the daily checks counter", func(t *testing.T) {
		// T-179: an increment failure rolls back the transaction entirely —
		// the result and the counter change only together.
		pool, q := seed(t)
		pool.failExec = map[string]error{recordChecksBumpSQL: errors.New("the counters are unavailable")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if got := pool.checksValue(); got != 0 {
			t.Fatalf("the daily counter = %d after the rollback, want 0", got)
		}
	})
	t.Run("the daily temp_error numerator", func(t *testing.T) {
		// T-182: a numerator increment failure rolls back both the result and
		// the denominator — the counter pair changes only together with the write.
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.failExec = map[string]error{recordTempErrBumpSQL: errors.New("the counters are unavailable")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		dom := pool.domains[recordDomainKey{zone: temp.ZoneID, id: temp.DomainID}]
		if dom.status != model.StatusNew || dom.checkState != model.CheckStateQueued {
			t.Fatalf("the temp_error domain was changed at a numerator failure: %v/%v — the transaction must roll back",
				dom.status, dom.checkState)
		}
		if got := pool.checksValue(); got != 0 {
			t.Fatalf("the daily counter = %d after the rollback at the numerator, want 0", got)
		}
		if got := pool.tempErrValue(); got != 0 {
			t.Fatalf("the daily temp_error numerator = %d after the rollback, want 0", got)
		}
	})
	t.Run("the retry maturity bucket", func(t *testing.T) {
		// T-202: a bucket increment failure rolls back the transaction entirely —
		// an episode without the bucket would open the retry guard a day less.
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.failExec = map[string]error{recordRetryMatureBumpSQL: errors.New("the counters are unavailable")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		dom := pool.domains[recordDomainKey{zone: temp.ZoneID, id: temp.DomainID}]
		if dom.status != model.StatusNew || dom.checkState != model.CheckStateQueued {
			t.Fatalf("the temp_error domain was changed at a bucket failure: %v/%v — the transaction must roll back",
				dom.status, dom.checkState)
		}
		if got := pool.retryMatureValue(); got != 0 {
			t.Fatalf("the maturity bucket = %d after the rollback, want 0", got)
		}
	})
	t.Run("the maturity bucket: RowsAffected ≠ 1", func(t *testing.T) {
		// T-202, the P3-2 review: the upsert's guard branch — "a silent
		// must be an error and a rollback (silence — a counter bug).
		// must be an error and a rollback (silence — a counter bug).
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.matureTag = 2 // INSERT 0 2 — a foreign SQL/a protocol decay
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		if err == nil || !strings.Contains(err.Error(), "2 rows affected (the day bucket upsert must affect 1)") {
			t.Fatalf("the error = %v, want the maturity bucket RowsAffected guard", err)
		}
		if got := pool.retryMatureValue(); got != 0 {
			t.Fatalf("the maturity bucket = %d after the rollback, want 0", got)
		}
	})
	t.Run("the hourly rate bucket", func(t *testing.T) {
		// T-203: a bucket increment failure rolls back the transaction entirely —
		// undercounted silently).
		// undercounted silently).
		pool, q := seed(t)
		pool.failExec = map[string]error{recordResultsBumpSQL: errors.New("the counters are unavailable")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if got := pool.resultsValue(); got != 0 {
			t.Fatalf("the hourly rate bucket = %d after the rollback, want 0", got)
		}
	})
	t.Run("the hourly rate bucket: RowsAffected ≠ 1", func(t *testing.T) {
		// T-203: the upsert's guard branch (the maturity bucket's style,
		// the T-202 P3-2 review) — a silent delta loss would understate the throughput.
		pool, q := seed(t)
		pool.resultsTag = 2 // INSERT 0 2 — a foreign SQL/a protocol decay
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if err == nil || !strings.Contains(err.Error(), "2 rows affected (the hour bucket upsert must affect 1)") {
			t.Fatalf("the error = %v, want the rate bucket RowsAffected guard", err)
		}
		if got := pool.resultsValue(); got != 0 {
			t.Fatalf("the hourly rate bucket = %d after the rollback, want 0", got)
		}
	})
	t.Run("Commit", func(t *testing.T) {
		pool, q := seed(t)
		pool.commitErr = errors.New("the server crashed at the commit")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("ErrRefIntegrity rolls everything back", func(t *testing.T) {
		pool, q := seed(t)
		h := recordTestHash(5)
		pool.rows[recordDomainKey{zone: 7, id: 91}] = []recordFakeRow{
			{subdomainID: 0, variant: model.VariantMain, contentHash: h[:]},
		}
		// The hash's refcount is zero (a dead row) — the accounting diverged.
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		if !errors.Is(err, content.ErrRefIntegrity) {
			t.Fatalf("ErrRefIntegrity expected: %v", err)
		}
		assertRolledBack(t, pool, q, err, 1)
		// The old row is intact: the deletion rolled back together with the transaction.
		if rows := pool.rows[recordDomainKey{zone: 7, id: 91}]; len(rows) != 1 || string(rows[0].contentHash) != string(h[:]) {
			t.Fatalf("the old row must survive the rollback: %+v", rows)
		}
	})
	t.Run("the ctx cancellation", func(t *testing.T) {
		pool, q := seed(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := recordRecorder(t, pool, q).RecordAndPublish(ctx, res)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the ctx cancellation expected: %v", err)
		}
		assertRolledBack(t, pool, q, err, 0)
	})
}

// TestRecordDeadlockRetry — the 40P01 fault injection (T-268): a deadlock inside
// the write transaction is retried WHOLE (the attempt is rolled back and
// executed anew — begin ×2, no half-done states), the
// q:analyze publication — one (the failed attempt had none); after
// the retries are exhausted the error goes out (a delivery without an XACK, spec 3.4), the
// crawler_db_deadlock_retried_total metric grows per every repeat.
func TestRecordDeadlockRetry(t *testing.T) {
	deadlock := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
	res, _ := recordRichResult()
	seed := func(t *testing.T) (*recordFakePool, *recordFakeQueue) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		return pool, &recordFakeQueue{j: pool.j}
	}

	t.Run("a deadlock absorbed — the transaction repeated entirely", func(t *testing.T) {
		pool, q := seed(t)
		reg := metrics.New("http-worker")
		pool.failExecOnce = map[string]error{recordDomainSQL: deadlock}
		r, err := NewRecorder(pool, q, NewMetrics(reg), nil)
		if err != nil {
			t.Fatalf("NewRecorder: %v", err)
		}
		ok, err := r.RecordAndPublish(context.Background(), res)
		if err != nil || !ok {
			t.Fatalf("RecordAndPublish = (%v, %v), want (true, nil) — the deadlock is absorbed", ok, err)
		}
		// Two transaction attempts: the first rolled back at the UPDATE, the second
		// passed to the Commit; the publications — after the commit, one
		// chunk (one per subdomain×variant record, no attempt's duplicate).
		if pool.begins != 2 {
			t.Errorf("transaction attempts = %d, want 2 (a full repeat)", pool.begins)
		}
		if got := pool.j.count(recordEvCommit); got != 1 {
			t.Errorf("Commit = %d, want 1 (only the second attempt was committed)", got)
		}
		if got, want := len(q.publishesSnapshot()), res.RecordCount(); got != want {
			t.Errorf("q:analyze publications = %d, want %d (one batch after the commit)", got, want)
		}
		// An honest repeat: attempt 1 committed nothing, the counters are not doubled.
		if got := pool.checksValue(); got != 1 {
			t.Errorf("the daily counter = %d, want 1 (no delta duplication)", got)
		}
		if got := httpMetricValue(t, reg, "crawler_db_deadlock_retried_total", nil); got != 1 {
			t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
		}
	})

	t.Run("a deadlock of the counters bucket — the same transaction retry", func(t *testing.T) {
		pool, q := seed(t)
		pool.failExecOnce = map[string]error{recordChecksBumpSQL: deadlock}
		ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		if err != nil || !ok {
			t.Fatalf("RecordAndPublish = (%v, %v), want (true, nil)", ok, err)
		}
		if pool.begins != 2 || pool.checksValue() != 1 {
			t.Errorf("%d attempts, the counter %d — want 2 and 1 (the tail deltas in the repeat)", pool.begins, pool.checksValue())
		}
	})

	t.Run("the retries exhausted — an error outward, the state rolled back", func(t *testing.T) {
		pool, q := seed(t)
		reg := metrics.New("http-worker")
		pool.failExec = map[string]error{recordDomainSQL: deadlock}
		r, err := NewRecorder(pool, q, NewMetrics(reg), nil)
		if err != nil {
			t.Fatalf("NewRecorder: %v", err)
		}
		ok, err := r.RecordAndPublish(context.Background(), res)
		if err == nil || !db.IsDeadlock(err) || ok {
			t.Fatalf("RecordAndPublish = (%v, %v), want (false, 40P01) outward", ok, err)
		}
		if want := 1 + db.MaxDeadlockRetries; pool.begins != want {
			t.Errorf("attempts = %d, want %d (the first + the retries)", pool.begins, want)
		}
		if dom := pool.domains[recordDomainKey{zone: 7, id: 91}]; dom.status != model.StatusNew ||
			dom.checkState != model.CheckStateQueued {
			t.Errorf("the domain was changed: %v/%v — all the attempts must roll back", dom.status, dom.checkState)
		}
		if got := len(q.publishesSnapshot()); got != 0 {
			t.Errorf("%d publications with an unwritten result", got)
		}
		if got := httpMetricValue(t, reg, "crawler_db_deadlock_retried_total", nil); got != db.MaxDeadlockRetries {
			t.Errorf("crawler_db_deadlock_retried_total = %v, want %d", got, db.MaxDeadlockRetries)
		}
	})
}

// TestRecordDeadlockRunAck — the T-268 acceptance criterion in full: the write
// transaction's deadlock in the OnResult chain is absorbed by the repeat — the
// q:http delivery is acknowledged (XACK), the task does not burn in the redelivery.
func TestRecordDeadlockRunAck(t *testing.T) {
	body := "<html>" + strings.Repeat("a page for the deadlock retry check ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"deadlock.test":         {status: 200, body: body},
		"http://deadlock.test/": {status: 200, body: body},
	})
	reg := metrics.New("http-worker")
	w := workerWorker(t, fetch, nil)
	pool := newRecordFakePool()
	pool.recordSeedDomain(5, 42, model.StatusNew, 0)
	pool.failExecOnce = map[string]error{
		recordDomainSQL: &pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
	}
	q := &workerFakeQueue{deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "deadlock.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}
	rec, err := NewRecorder(pool, q, NewMetrics(reg), nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	opts := Options{OnResult: func(c context.Context, res *DomainResult) error {
		_, err := rec.RecordAndPublish(c, res)
		return err
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	deadline := time.After(5 * time.Second)
	for q.ackedCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("no XACK seen: the deadlock is not absorbed — the task burns in the redelivery")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancellation")
	}
	if pool.begins != 2 {
		t.Errorf("transaction attempts = %d, want 2", pool.begins)
	}
	if got := httpMetricValue(t, reg, "crawler_db_deadlock_retried_total", nil); got != 1 {
		t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
	}
}

// --- the publication: after the write, errors are not silent -------------------------------

func TestRecordPublishFailures(t *testing.T) {
	t.Run("a publication failure — the result is written, the delivery will come back", func(t *testing.T) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		q := &recordFakeQueue{j: pool.j, publishErrs: []error{nil, errors.New("Redis fell off")}}
		res, _ := recordRichResult()

		ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "q:analyze publication") {
			t.Fatalf("a publication error expected: %v", err)
		}
		if !ok {
			t.Fatal("the write happened before the publication — recorded=true")
		}
		if len(pool.rows[recordDomainKey{zone: 7, id: 91}]) != 2 {
			t.Fatal("the rows must stay: a re-delivery publication will append the tail")
		}
	})
	t.Run("the very first message does not go out", func(t *testing.T) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		q := &recordFakeQueue{j: pool.j, publishErrs: []error{errors.New("no")}}
		res, _ := recordRichResult()
		if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res); err == nil {
			t.Fatal("a publication error expected")
		}
	})
}

// --- the validation before the transaction ---------------------------------------------------

func TestRecordValidation(t *testing.T) {
	newRes := func(mutate func(*Record)) *DomainResult {
		res, _ := recordRichResult()
		if mutate != nil {
			mutate(&res.Subdomains[0].Records[0])
		}
		return res
	}
	cases := []struct {
		name string
		res  *DomainResult
		frag string
	}{
		{"a zero zone", &DomainResult{ZoneID: 0, DomainID: 1, Name: "x.test", Subdomains: []SubdomainResult{{Records: []Record{{}}}}}, "identifiers"},
		{"an invalid variant", newRes(func(r *Record) { r.Variant = model.Variant(7) }), "variant"},
		{"an invalid scheme", newRes(func(r *Record) { r.Scheme = model.Scheme(9) }), "scheme"},
		{"an alias without a reference", newRes(func(r *Record) { r.AliasKind = model.AliasIdentical }), "T-055"},
		{"a duplicate key", func() *DomainResult {
			res, _ := recordRichResult()
			rec := res.Subdomains[0].Records[0]
			res.Subdomains[1].Records = append(res.Subdomains[1].Records, rec)
			res.Subdomains[1].Records[1].SubdomainID = 0
			return res
		}(), "duplicate"},
		{"ContentNotStored with a hash", newRes(func(r *Record) { r.ContentNotStored = true }), "T-056"},
		{"ContentNotStored on an alias", newRes(func(r *Record) {
			r.AliasKind = model.AliasRedirect
			id := int16(1)
			r.AliasSubdomainID = &id
			r.ContentHash = nil
			r.StoreContent = false
			r.ContentNotStored = true
		}), "T-056"},
		{"an original without content and the sign", newRes(func(r *Record) { r.ContentHash = nil }), "T-056"},
		{"an alias with StoreContent", newRes(func(r *Record) {
			r.AliasKind = model.AliasRedirect
			id := int16(1)
			r.AliasSubdomainID = &id
			r.StoreContent = true
		}), "T-055"},
		{"a record class outside an available check", newRes(func(r *Record) { r.ErrClass = ErrTimeout }), "available"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newRecordFakePool()
			pool.recordSeedDomain(7, 91, model.StatusNew, 0)
			q := &recordFakeQueue{j: pool.j}
			_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), tc.res)
			if err == nil || !strings.Contains(err.Error(), tc.frag) {
				t.Fatalf("a validation error with %q expected: %v", tc.frag, err)
			}
			if pool.begins != 0 {
				t.Fatal("the validation must cut off before the transaction opens")
			}
		})
	}

	t.Run("a nil result and dependencies", func(t *testing.T) {
		pool := newRecordFakePool()
		q := &recordFakeQueue{j: pool.j}
		if ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), nil); err != nil || ok {
			t.Fatalf("nil: ok=%v err=%v", ok, err)
		}
		if _, err := NewRecorder(nil, q, nil, nil); err == nil {
			t.Fatal("NewRecorder without a pool passed")
		}
		if _, err := NewRecorder(pool, nil, nil, nil); err == nil {
			t.Fatal("NewRecorder without a queue passed")
		}
	})
}

// --- the smoke: the Run loop with the OnResult chain (the cmd/http-worker build) --------------

// TestRecordOnResultSmoke — a q:http delivery → ProcessDomain → ContentSaver
// (T-056) → the result write + the q:analyze publication (T-058) → the XACK:
// the "publication before ack" order, messages per records, the domain done.
func TestRecordOnResultSmoke(t *testing.T) {
	page := storePageBody("smoke")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"https://ack.test/": {status: 200, body: page},
		"ack.test":          {status: 200, body: page, final: "https://ack.test/"},
	})
	pool := newRecordFakePool()
	pool.recordSeedDomain(5, 42, model.StatusNew, 1)
	q := &recordFakeQueue{j: pool.j, deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "ack.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}
	q.j.ackNotify = make(chan struct{}, 1)

	st := newStoreFake()
	saver, err := NewContentSaver(st, nil)
	if err != nil {
		t.Fatalf("NewContentSaver: %v", err)
	}
	recorder := recordRecorder(t, pool, q)

	// The service build's chain (cmd/http-worker): ContentSaver → the write →
	// the publication; OnResult is read from the Run options (worker.go).
	opts := Options{OnResult: func(c context.Context, res *DomainResult) error {
		if err := saver.Save(c, res); err != nil {
			return err
		}
		_, err := recorder.RecordAndPublish(c, res)
		return err
	}}
	opts.Store = st // the early Puts go into the same fake as the Save reconciliation (T-147)
	w, err := New(fetch, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()
	select {
	case <-q.j.ackNotify:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the delivery was not acknowledged within 5 s")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	dom := pool.domains[recordDomainKey{zone: 5, id: 42}]
	if dom.status != model.StatusAlive || dom.checkState != model.CheckStateDone {
		t.Fatalf("the domain: %v/%v, alive/done expected", dom.status, dom.checkState)
	}
	rows := pool.rows[recordDomainKey{zone: 5, id: 42}]
	if len(rows) != 1 || rows[0].contentHash == nil {
		t.Fatalf("the rows: %+v — the original content was written by a reference", rows)
	}
	pubs := q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].msg.SubdomainID != 0 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the q:analyze publications: %+v", pubs)
	}
	if q.j.indexOf(recordEvPublish) > q.j.indexOf(recordEvAck) {
		t.Fatal("the XACK precedes the q:analyze publication — the order of spec 4.2/5.5 item 9 is violated")
	}
	if _, err := st.Get(context.Background(), content.HashOf([]byte(page))); err != nil {
		t.Fatalf("the content was not stored: %v", err)
	}
}

// TestRecordOnResultPublishErrorNoAck — a publication failure leaves the delivery
// without an XACK: a repeated delivery will rewrite the result and publish anew.
func TestRecordOnResultPublishErrorNoAck(t *testing.T) {
	page := storePageBody("pub-err")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"https://pub.test/": {status: 200, body: page},
		"pub.test":          {status: 200, body: page, final: "https://pub.test/"},
	})
	pool := newRecordFakePool()
	pool.recordSeedDomain(5, 43, model.StatusNew, 0)
	q := &recordFakeQueue{j: pool.j, deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 43, Name: "pub.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}, publishErrs: []error{errors.New("Redis is unavailable")}}

	recorder := recordRecorder(t, pool, q)
	opts := Options{OnResult: func(c context.Context, res *DomainResult) error {
		_, err := recorder.RecordAndPublish(c, res)
		return err
	}}
	opts.Store = newStoreFake() // the early Puts T-147 — a fresh fake (no Save in the chain)
	w, err := New(fetch, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	deadline := time.After(5 * time.Second)
	for q.j.count(recordEvPublish) < 1 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("the publication did not attempt to run within 5 s")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// The delivery must stay unacknowledged.
	time.Sleep(50 * time.Millisecond)
	if got := q.j.count(recordEvAck); got != 0 {
		t.Fatalf("XACK %d at a publication failure — the delivery must come back (spec 3.4)", got)
	}
	if len(q.ackedSnapshot()) != 0 {
		t.Fatal("the ack was performed at an OnResult error")
	}
	// The write did happen at that: the repeat will only finish publishing.
	if len(pool.rows[recordDomainKey{zone: 5, id: 43}]) != 1 {
		t.Fatal("the result was not written before the publication")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

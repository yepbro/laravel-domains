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
	return 0, errors.New("recordFakePool: CopyFrom не должен вызываться (INSERT — unnest-пачкой)")
}

func (p *recordFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("recordFakePool: Exec вне транзакции")
}

func (p *recordFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("recordFakePool: Query вне транзакции")
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
		return fmt.Errorf("recordStateRow: приёмников %d, нужно 2", len(dest))
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
		return recordStateRow{err: fmt.Errorf("recordFakePool: неожидаемый QueryRow: %s", sql)}
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
			return pgconn.CommandTag{}, fmt.Errorf("recordFakePool: INSERT: параметров %d, нужно 18", len(args))
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
			return pgconn.CommandTag{}, fmt.Errorf("recordFakePool: неожидаемый SQL: %s", sql)
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
		return nil, fmt.Errorf("recordFakePool: неожидаемый SQL: %s", sql)
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
		return errors.New("recordFakeRows: Scan без Next")
	}
	if len(dest) != 1 {
		return fmt.Errorf("recordFakeRows: приёмников %d, колонка одна (content_hash)", len(dest))
	}
	switch d := dest[0].(type) {
	case *[]byte:
		if r.rows[r.i-1][0] == nil {
			*d = nil
		} else {
			*d = r.rows[r.i-1][0].([]byte)
		}
	default:
		return fmt.Errorf("recordFakeRows: приёмник %T, ожидается *[]byte", dest[0])
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
		return "", fmt.Errorf("recordFakeQueue: неожидаемое сообщение %T (нужен queue.Result)", msg)
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
		t.Fatalf("счётчики зоны = %+v, want {10 2 2 6} (queued −1, checked +1)", *zone)
	}

	// The daily checks counter (T-179): one episode — +1, not +2 by
	// the number of subdomain×variant records (the delta — the domain's UPDATE
	// RowsAffected, not the INSERT chunk's); the delta in the same transaction — after
	// the zone shift, before the commit. The temp_error numerator (T-182) is untouched by a
	// live (alive) episode — only temp_error
	// outcomes run the operator.
	if got := pool.checksValue(); got != 1 {
		t.Fatalf("суточный счётчик = %d, want 1 (один эпизод из двух записей)", got)
	}
	if got := pool.tempErrValue(); got != 0 {
		t.Fatalf("суточный числитель temp_error = %d, want 0 (эпизод alive)", got)
	}
	if shift, checks := pool.j.indexOf(recordEvShift), pool.j.indexOf(recordEvChecks); shift > checks || checks > pool.j.indexOf(recordEvCommit) {
		t.Fatalf("порядок счётчиков: shift=%d checks=%d commit=%d (зона → checks → фиксация)",
			shift, checks, pool.j.indexOf(recordEvCommit))
	}

	dom := pool.domains[recordDomainKey{zone: 7, id: 91}]
	if dom.status != model.StatusAlive || dom.checkState != model.CheckStateDone {
		t.Fatalf("домен: status=%v check_state=%v, ожидалось alive/done", dom.status, dom.checkState)
	}
	if dom.queuedAt != nil || !dom.lastChecked {
		t.Fatalf("домен: queued_at=%v last_checked=%v, ожидался NULL/заполнен", dom.queuedAt, dom.lastChecked)
	}
	if dom.tempAttempts != 0 {
		t.Fatalf("temp_attempts=%d, ожидался сброс в 0 (контракт T-031)", dom.tempAttempts)
	}

	rows := pool.rows[recordDomainKey{zone: 7, id: 91}]
	if len(rows) != 2 {
		t.Fatalf("строк %d, ожидалось 2", len(rows))
	}
	apex, www := rows[0], rows[1]

	if apex.subdomainID != 0 || apex.variant != model.VariantMain || apex.scheme != model.SchemeHTTPS {
		t.Fatalf("апекс: ключ (%d,%v,%v)", apex.subdomainID, apex.variant, apex.scheme)
	}
	if apex.httpStatus == nil || *apex.httpStatus != 200 {
		t.Fatalf("апекс: http_status=%v", apex.httpStatus)
	}
	if apex.finalURL == nil || *apex.finalURL != "https://rich.example/" {
		t.Fatalf("апекс: final_url=%v", apex.finalURL)
	}
	if apex.aliasKind != model.AliasNone || apex.aliasSub != nil {
		t.Fatalf("апекс: алиасы %v/%v", apex.aliasKind, apex.aliasSub)
	}
	// addrs: v4-mapped (::ffff:192.0.2.1) + IPv6, 16 octets each.
	if len(apex.addrs) != 32 {
		t.Fatalf("апекс: addrs %d байт, ожидалось 32 (2×16)", len(apex.addrs))
	}
	want4 := netip.MustParseAddr("192.0.2.1").As16()
	want6 := netip.MustParseAddr("2001:db8::1").As16()
	if string(apex.addrs[:16]) != string(want4[:]) || string(apex.addrs[16:]) != string(want6[:]) {
		t.Fatalf("апекс: addrs не совпали с упаковкой v4-mapped+IPv6: %x", apex.addrs)
	}
	if apex.cname != nil {
		t.Fatalf("апекс: cname=%v, ожидался NULL", *apex.cname)
	}
	for col, v := range map[string]*string{"tls": apex.tls, "redirects": apex.redirects, "headers": apex.headers, "cookies": apex.cookies} {
		if v == nil {
			t.Fatalf("апекс: %s = NULL, ожидался jsonb", col)
		}
	}
	if !strings.Contains(*apex.tls, `"subject_cn":"rich.example"`) || !strings.Contains(*apex.tls, `"dns_names"`) {
		t.Fatalf("апекс: tls jsonb: %s", *apex.tls)
	}
	if !strings.Contains(*apex.redirects, `"url":"http://rich.example/","status":301`) {
		t.Fatalf("апекс: redirects jsonb: %s", *apex.redirects)
	}
	if !strings.Contains(*apex.headers, `"Content-Type":["text/html; charset=utf-8"]`) {
		t.Fatalf("апекс: headers jsonb: %s", *apex.headers)
	}
	if !strings.Contains(*apex.cookies, `"name":"laravel_session"`) || !strings.Contains(*apex.cookies, `"http_only":true`) {
		t.Fatalf("апекс: cookies jsonb: %s", *apex.cookies)
	}
	if apex.rendered {
		t.Fatal("апекс: rendered=true, рендер — T-067")
	}
	wantApex := hashes["apex"]
	if string(apex.contentHash) != string(wantApex[:]) {
		t.Fatalf("апекс: content_hash %x, ожидался %x", apex.contentHash, wantApex)
	}
	if apex.errCode != nil {
		t.Fatalf("апекс: error=%v, ожидался NULL (ErrNone)", *apex.errCode)
	}
	if apex.responseMS == nil || *apex.responseMS != 123 {
		t.Fatalf("апекс: response_ms=%v", apex.responseMS)
	}

	// www: an alias redirect to the apex, a CNAME, a truncated body — the error
	// code and the NULL hash (T-056).
	if www.aliasKind != model.AliasRedirect || www.aliasSub == nil || *www.aliasSub != 0 {
		t.Fatalf("www: алиас %v/%v", www.aliasKind, www.aliasSub)
	}
	if www.cname == nil || *www.cname != "rich.example" {
		t.Fatalf("www: cname=%v", www.cname)
	}
	if www.contentHash != nil {
		t.Fatal("www: content_hash у обрезанного тела, ожидался NULL")
	}
	if www.errCode == nil || *www.errCode != int16(ErrBody) {
		t.Fatalf("www: error=%v, ожидался ErrBody", www.errCode)
	}
	if www.responseMS == nil || *www.responseMS != 456 {
		t.Fatalf("www: response_ms=%v", www.responseMS)
	}
	if len(www.addrs) != 16 {
		t.Fatalf("www: addrs %d байт, ожидалось 16", len(www.addrs))
	}

	// The SQL contract: the schema literals and the operators' structure.
	if !strings.Contains(recordDomainSQL, "check_state = 2") ||
		!strings.Contains(recordDomainSQL, "queued_at = NULL") ||
		!strings.Contains(recordDomainSQL, "last_checked_at = now()") ||
		!strings.Contains(recordDomainSQL, "t.status <> 6") ||
		!strings.Contains(recordDomainSQL, "temp_attempts = CASE") {
		t.Fatalf("recordDomainSQL разошёлся с контрактом §6.1/T-031:\n%s", recordDomainSQL)
	}
	if !strings.Contains(recordInsertSQL, "false, z.content_hash, z.error, z.response_ms, now(), NULL") ||
		!strings.Contains(recordInsertSQL, "::jsonb[]") || !strings.Contains(recordInsertSQL, "::bytea[]") {
		t.Fatalf("recordInsertSQL разошёлся с колонками §6.1:\n%s", recordInsertSQL)
	}
	// T-210: render_failed_at is not in the INSERT — the episode's
	// rewrite (DELETE + INSERT) resets the "render impossible" marker: the new
	// content has the right to a render (DEFAULT NULL).
	if strings.Contains(recordInsertSQL, "render_failed_at") {
		t.Fatalf("recordInsertSQL пишет render_failed_at — маркер T-210 обязан сбрасываться перезаписью эпизода (DEFAULT NULL):\n%s", recordInsertSQL)
	}
	if !strings.Contains(recordDeleteSQL, "RETURNING content_hash") {
		t.Fatalf("recordDeleteSQL без RETURNING content_hash (своп ссылок T-044):\n%s", recordDeleteSQL)
	}

	// The publication: after the transaction's commit, one message per record.
	pubs := q.publishesSnapshot()
	if len(pubs) != 2 {
		t.Fatalf("публикаций q:analyze %d, ожидалось 2", len(pubs))
	}
	for i, want := range []struct {
		sub     int16
		variant model.Variant
	}{{0, model.VariantMain}, {1, model.VariantMain}} {
		got := pubs[i]
		if got.stream != queue.StreamAnalyze {
			t.Fatalf("публикация %d: стрим %s", i, got.stream)
		}
		if got.msg.ZoneID != 7 || got.msg.DomainID != 91 || got.msg.SubdomainID != want.sub ||
			got.msg.Variant != want.variant || got.msg.Name != "rich.example" {
			t.Fatalf("публикация %d: %+v", i, got.msg)
		}
	}
	ev := pool.j.snapshot()
	for _, pair := range [][2]recordEventKind{
		{recordEvDomains, recordEvDelete}, {recordEvDelete, recordEvInsert},
		{recordEvInsert, recordEvCommit}, {recordEvCommit, recordEvPublish},
	} {
		if pool.j.indexOf(pair[0]) > pool.j.indexOf(pair[1]) {
			t.Fatalf("порядок событий нарушен: %v после %v (%v)", pair[0], pair[1], ev)
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
		{"В1 (О7): есть записи — alive", &DomainResult{Subdomains: []SubdomainResult{recorded}}, model.StatusAlive},
		{"В2 (О8): записей нет, connect-таймаут — temp_error (асимметрия А2)",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrTimeout, ErrNetwork)}}, model.StatusTempError},
		{"В2 (О8): таймаут второго протокола тоже temp_error",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrNone, ErrTimeout)}}, model.StatusTempError},
		{"В3 (О9): записей нет, connect-refused — no_http (асимметрия А2)",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrNetwork, ErrNetwork)}}, model.StatusNoHTTP},
		{"В3 (О9): сбой TLS-рукопожатия — no_http",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrTLS, ErrNone)}}, model.StatusNoHTTP},
		{"В3 (О9): лимит редиректов — no_http",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrRedirects, ErrRedirects)}}, model.StatusNoHTTP},
		{"В3 (О9): обрезка тела — no_http",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrBody, ErrNone)}}, model.StatusNoHTTP},
		{"В3 (О9): dead-host cutoff синтетик — no_http (ErrCutoff не temp)",
			&DomainResult{Subdomains: []SubdomainResult{recordClassSub(ErrCutoff, ErrCutoff)}}, model.StatusNoHTTP},
		{"В2 доминирует над В3 у разных субдоменов: таймаут одного — temp_error",
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
		{"записи есть — alive", func() *DomainResult { r, _ := recordRichResult(); return r }(), model.StatusAlive},
		{"молчит постоянно — no_http", recordSilentResult(false), model.StatusNoHTTP},
		{"молчит с таймаутами — temp_error", recordSilentResult(true), model.StatusTempError},
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
				t.Fatalf("status=%v, ожидался %v", dom.status, tc.want)
			}
			if dom.tempAttempts != 0 && tc.want != model.StatusTempError {
				t.Fatalf("temp_attempts=%d при не-temp_error итоге", dom.tempAttempts)
			}
		})
	}

	t.Run("temp_error сохраняет счётчик повторов", func(t *testing.T) {
		pool := newRecordFakePool()
		res := recordSilentResult(true)
		pool.recordSeedDomain(res.ZoneID, res.DomainID, model.StatusNew, 4)
		q := &recordFakeQueue{j: pool.j}
		if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res); err != nil {
			t.Fatalf("RecordAndPublish: %v", err)
		}
		if got := pool.domains[recordDomainKey{zone: res.ZoneID, id: res.DomainID}].tempAttempts; got != 4 {
			t.Fatalf("temp_attempts=%d, temp_error обязан сохранить накопленное (T-031)", got)
		}
	})

	t.Run("без записей строки не вставляются, старые вычищаются", func(t *testing.T) {
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
			t.Fatalf("строк после no_http: %d, ожидалось 0 (§3.5 — только последний результат)", len(pool.rows[key]))
		}
		if pool.j.count(recordEvInsert) != 0 {
			t.Fatal("INSERT при пустом результате не нужен")
		}
		if pool.refc[oldHash] != 0 {
			t.Fatalf("refcount старого контента %d, ожидался 0", pool.refc[oldHash])
		}
		if got := len(q.publishesSnapshot()); got != 0 {
			t.Fatalf("публикаций %d — публиковать нечего", got)
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
		t.Fatalf("после cutoff-эпизода бит dead_host_cutoff не стоит: flags=%s", dom.flags)
	}
	if !dom.flags.IsRenderPending() {
		t.Fatalf("CASE flags снял чужой бит render_pending: flags=%s", dom.flags)
	}

	// A usual episode (a full check at the T-030 recheck) removes the bit.
	full := recordSilentResult(false)
	if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), full); err != nil {
		t.Fatalf("RecordAndPublish (полный эпизод): %v", err)
	}
	if dom.cutoff || dom.flags.IsDeadHostCutoff() {
		t.Fatalf("после полного эпизода бит dead_host_cutoff стоит: flags=%s", dom.flags)
	}
	if !dom.flags.IsRenderPending() {
		t.Fatalf("CASE flags снял чужой бит render_pending: flags=%s", dom.flags)
	}

	// The SQL contract: the bit and the mask — as the model's literals.
	if !strings.Contains(recordDomainSQL, fmt.Sprintf("(t.flags | %d)::int2", int16(model.FlagDeadHostCutoff))) ||
		!strings.Contains(recordDomainSQL, fmt.Sprintf("(t.flags & %d)::int2", int16(^int16(model.FlagDeadHostCutoff)))) {
		t.Fatalf("recordDomainSQL без CASE битa dead_host_cutoff (T-164):\n%s", recordDomainSQL)
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
		t.Fatalf("temp_error-эпизод: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 1 {
		t.Fatalf("после temp_error-эпизода checks = %d, want 1", got)
	}
	if got := pool.tempErrValue(); got != 1 {
		t.Fatalf("после temp_error-эпизода temp_error = %d, want 1", got)
	}
	if got := pool.retryMatureValue(); got != 1 {
		t.Fatalf("после temp_error-эпизода retry_mature = %d, want 1", got)
	}
	checks, tempBump := pool.j.indexOf(recordEvChecks), pool.j.indexOf(recordEvTempErr)
	matureBump, commit := pool.j.indexOf(recordEvMature), pool.j.indexOf(recordEvCommit)
	if tempBump < checks || matureBump < tempBump || matureBump > commit {
		t.Fatalf("порядок: checks=%d temp=%d mature=%d commit=%d (checks → temp_error → retry_mature → фиксация)",
			checks, tempBump, matureBump, commit)
	}

	// A no_http episode: the denominator grows, the numerator and the bucket untouched —
	// only temp_error outcomes run the operators.
	silent := recordSilentResult(false)
	pool.recordSeedDomain(silent.ZoneID, silent.DomainID, model.StatusNew, 0)
	if ok, err := r.RecordAndPublish(context.Background(), silent); err != nil || !ok {
		t.Fatalf("no_http-эпизод: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 2 {
		t.Fatalf("после no_http-эпизода checks = %d, want 2", got)
	}
	if got := pool.tempErrValue(); got != 1 {
		t.Fatalf("после no_http-эпизода temp_error = %d, want 1 (не temp_error-итог не инкрементирует)", got)
	}
	if got := pool.retryMatureValue(); got != 1 {
		t.Fatalf("после no_http-эпизода retry_mature = %d, want 1 (зреют только temp_error-эпизоды)", got)
	}
	if n := pool.j.count(recordEvTempErr); n != 1 {
		t.Fatalf("инкрементов temp_error = %d, want 1", n)
	}
	if n := pool.j.count(recordEvMature); n != 1 {
		t.Fatalf("инкрементов корзины = %d, want 1", n)
	}

	// A temp_error episode's duplicate delivery: +1 into the numerator, the denominator and
	// the bucket together — a write event; the reconciliation levels
	// the drift against the live domain count (T-202).
	if ok, err := r.RecordAndPublish(context.Background(), temp); err != nil || !ok {
		t.Fatalf("повтор temp_error-эпизода: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 3 {
		t.Fatalf("после повтора checks = %d, want 3", got)
	}
	if got := pool.tempErrValue(); got != 2 {
		t.Fatalf("после повтора temp_error = %d, want 2 (по одному событию на запись)", got)
	}
	if got := pool.retryMatureValue(); got != 2 {
		t.Fatalf("после повтора retry_mature = %d, want 2 (по одному событию на запись)", got)
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
		t.Fatalf("богатый эпизод: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 2 {
		t.Fatalf("после богатого эпизода results = %d, want 2 (две вставленные строки)", got)
	}
	if got := pool.reanalyzeValue(); got != 2 {
		t.Fatalf("после богатого эпизода reanalyze = %d, want 2 (каждая вставленная строка — хвост до штампа, T-204)", got)
	}
	resultsBump, commit := pool.j.indexOf(recordEvResults), pool.j.indexOf(recordEvCommit)
	matureBump := pool.j.indexOf(recordEvMature)
	reanBump := pool.j.indexOf(recordEvRean)
	if resultsBump > commit || (matureBump >= 0 && resultsBump < matureBump) || reanBump > resultsBump {
		t.Fatalf("порядок: mature=%d reanalyze=%d results=%d commit=%d (… → retry_mature: → reanalyze: → results: → фиксация)",
			matureBump, reanBump, resultsBump, commit)
	}

	// A silent episode (temp_error, without records): no rows — the
	// bucket operators do not run at all (zero upsert passes do not warm
	// the hours' locks).
	silent := recordSilentResult(true)
	pool.recordSeedDomain(silent.ZoneID, silent.DomainID, model.StatusNew, 1)
	if ok, err := r.RecordAndPublish(context.Background(), silent); err != nil || !ok {
		t.Fatalf("молчаливый эпизод: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 2 {
		t.Fatalf("после молчаливого эпизода results = %d, want 2 (эпизод без строк корзину не трогает)", got)
	}
	if got := pool.reanalyzeValue(); got != 2 {
		t.Fatalf("после молчаливого эпизода reanalyze = %d, want 2 (эпизод без строк корзину не трогает)", got)
	}
	if n := pool.j.count(recordEvResults); n != 1 {
		t.Fatalf("инкрементов корзины темпа = %d, want 1 (нулевая дельта не выполняется)", n)
	}
	if n := pool.j.count(recordEvRean); n != 1 {
		t.Fatalf("инкрементов корзины хвостов = %d, want 1 (нулевая дельта не выполняется)", n)
	}

	// A rich episode repeat (a duplicate delivery): two more insertion events into
	// both buckets — a live window would count the same 2 rows; the
	// semantics' difference is leveled by the reconciliation (streaming, as at the daily rows).
	// A repeated delivery replays the content storing too (the style of
	// TestRecordIdempotentRepeat): a repeated Put gives +1 refcount.
	_, hashes := recordRichResult()
	pool.refc[hashes["apex"]] = 2
	if ok, err := r.RecordAndPublish(context.Background(), rich); err != nil || !ok {
		t.Fatalf("повтор богатого эпизода: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 4 {
		t.Fatalf("после повтора results = %d, want 4 (по событиям вставки на доставку)", got)
	}
	if got := pool.reanalyzeValue(); got != 4 {
		t.Fatalf("после повтора reanalyze = %d, want 4 (по событиям вставки на доставку)", got)
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
	if err == nil || !strings.Contains(err.Error(), "корзина хвостов переанализа") {
		t.Fatalf("ошибка = %v, want контекст корзины хвостов", err)
	}
	if ok {
		t.Fatal("при сбое оператора результат не считается записанным")
	}
	if _, exists := pool.rows[recordDomainKey{zone: rich.ZoneID, id: rich.DomainID}]; exists {
		t.Fatal("строки эпизода записаны при сбое корзины (откат §3.4)")
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
		t.Fatalf("удалённый домен — не ошибка: %v", err)
	}
	if ok {
		t.Fatal("запись обязана сообщить «не записано»")
	}
	if dom.status != model.StatusDeleted || dom.checkState != model.CheckStateQueued {
		t.Fatalf("домен изменён: %v/%v", dom.status, dom.checkState)
	}
	if len(pool.rows[key]) != 1 {
		t.Fatalf("старые строки тронуты (%d), удалённому результат не пишется", len(pool.rows[key]))
	}
	if pool.j.count(recordEvInsert) != 0 || pool.j.count(recordEvDelete) != 0 {
		t.Fatal("транзакция обязана остановиться на охране UPDATE")
	}
	// No result is written to the deleted — the daily counter does not grow either
	// (T-179: the delta only at an UPDATE RowsAffected > 0, incremental-counters spec §6.1.3);
	// the temp_error numerator — all the more (T-182).
	if pool.j.count(recordEvChecks) != 0 || pool.checksValue() != 0 ||
		pool.j.count(recordEvTempErr) != 0 || pool.tempErrValue() != 0 {
		t.Fatal("суточные счётчики тронуты при охране deleted")
	}
	if got := len(q.publishesSnapshot()); got != 0 {
		t.Fatalf("публикаций %d — без записи публиковать нельзя", got)
	}

	t.Run("отсутствующий домен — тот же путь", func(t *testing.T) {
		ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), &DomainResult{
			ZoneID: 7, DomainID: 999, Name: "ghost.example",
			Subdomains: []SubdomainResult{{
				SubdomainID: 0, Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.1")},
				Records: []Record{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
			}},
		})
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v, ожидалось false/nil", ok, err)
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
		t.Fatalf("старые ссылки: H1=%d H2=%d, ожидался 0/0 (строка мёртвая — T-042)", pool.refc[h1], pool.refc[h2])
	}
	if pool.refc[h3] != 1 {
		t.Fatalf("refcount нового контента %d, ожидался 1 (+1 Put уже был, −новых не нужно)", pool.refc[h3])
	}
	rows := pool.rows[key]
	if len(rows) != 2 {
		t.Fatalf("строк %d — перезапись удаляет и исчезнувшие ключи (§3.5)", len(rows))
	}
	for _, r := range rows {
		if r.finalURL != nil && *r.finalURL == oldFinal {
			t.Fatal("старая строка пережила перезапись")
		}
	}
	// The operator order: −1 of the old ones — before the INSERT, everything after the domain's UPDATE.
	for _, pair := range [][2]recordEventKind{
		{recordEvDomains, recordEvSwap}, {recordEvSwap, recordEvInsert}, {recordEvInsert, recordEvCommit},
	} {
		if pool.j.indexOf(pair[0]) > pool.j.indexOf(pair[1]) {
			t.Fatalf("порядок нарушен: %v после %v", pair[0], pair[1])
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
		t.Fatalf("первая запись: ok=%v err=%v", ok, err)
	}
	if pool.refc[hApex] != 1 {
		t.Fatalf("после первой записи refcount=%d, ожидался 1", pool.refc[hApex])
	}

	// The recheck scenario: the Put added the reference anew (refcount 2), the
	// rewrite removes the old row's reference — the outcome is balanced.
	pool.refc[hApex] = 2
	if ok, err := r.RecordAndPublish(context.Background(), res); err != nil || !ok {
		t.Fatalf("повторная запись: ok=%v err=%v (идемпотентность, ТЗ 3.4)", ok, err)
	}
	if pool.refc[hApex] != 1 {
		t.Fatalf("refcount после повтора=%d, двойное снятие ушло бы в минус (T-044)", pool.refc[hApex])
	}
	if len(pool.rows[key]) != 2 {
		t.Fatalf("строк %d — повтор обязан перезаписать, не дублировать", len(pool.rows[key]))
	}
	// A repeat after a successful write publishes q:analyze anew: the
	// "written, not published" window is closed by the repeated delivery.
	if got := len(q.publishesSnapshot()); got != 4 {
		t.Fatalf("публикаций суммарно %d, ожидалось 4 (2×2)", got)
	}
	// The daily counter counts the write events (T-179, TZ_slow_done_today
	// §5.1): a repeated delivery — one more event, +1 (not ×2 per
	// delivery); they converge to the live window by the reconciliation (the
	// integration scenario — TestCounters_RecordDoubleDelivery).
	if got := pool.checksValue(); got != 2 {
		t.Fatalf("суточный счётчик = %d, want 2 (по одному событию на запись)", got)
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
			t.Fatal("ожидалась ошибка шага")
		}
		key := recordDomainKey{zone: 7, id: 91}
		if dom := pool.domains[key]; dom.checkState != model.CheckStateQueued || dom.status != model.StatusNew {
			t.Fatalf("домен изменён при сбое: %v/%v — транзакция обязана откатиться", dom.status, dom.checkState)
		}
		if len(pool.rows[key]) != oldRows {
			t.Fatalf("строк %d после сбоя, ожидалось %d — откат к снапшоту не сработал", len(pool.rows[key]), oldRows)
		}
		if got := len(q.publishesSnapshot()); got != 0 {
			t.Fatalf("публикаций %d при незаписанном результате", got)
		}
	}

	t.Run("Begin", func(t *testing.T) {
		pool, q := seed(t)
		pool.beginErr = errors.New("соединение недоступно")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("UPDATE domains", func(t *testing.T) {
		pool, q := seed(t)
		pool.failExec = map[string]error{recordDomainSQL: errors.New("блокировка")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("DELETE старых строк", func(t *testing.T) {
		pool, q := seed(t)
		pool.deleteErr = errors.New("сбой чтения")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("INSERT", func(t *testing.T) {
		pool, q := seed(t)
		pool.failExec = map[string]error{recordInsertSQL: errors.New("нарушение PK")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("суточный счётчик проверок", func(t *testing.T) {
		// T-179: an increment failure rolls back the transaction entirely —
		// the result and the counter change only together.
		pool, q := seed(t)
		pool.failExec = map[string]error{recordChecksBumpSQL: errors.New("счётчики недоступны")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if got := pool.checksValue(); got != 0 {
			t.Fatalf("суточный счётчик = %d после отката, want 0", got)
		}
	})
	t.Run("суточный числитель temp_error", func(t *testing.T) {
		// T-182: a numerator increment failure rolls back both the result and
		// the denominator — the counter pair changes only together with the write.
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.failExec = map[string]error{recordTempErrBumpSQL: errors.New("счётчики недоступны")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		dom := pool.domains[recordDomainKey{zone: temp.ZoneID, id: temp.DomainID}]
		if dom.status != model.StatusNew || dom.checkState != model.CheckStateQueued {
			t.Fatalf("temp_error-домен изменён при сбое числителя: %v/%v — транзакция обязана откатиться",
				dom.status, dom.checkState)
		}
		if got := pool.checksValue(); got != 0 {
			t.Fatalf("суточный счётчик = %d после отката на числителе, want 0", got)
		}
		if got := pool.tempErrValue(); got != 0 {
			t.Fatalf("суточный числитель temp_error = %d после отката, want 0", got)
		}
	})
	t.Run("корзина зрелости ретрая", func(t *testing.T) {
		// T-202: a bucket increment failure rolls back the transaction entirely —
		// an episode without the bucket would open the retry guard a day less.
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.failExec = map[string]error{recordRetryMatureBumpSQL: errors.New("счётчики недоступны")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		dom := pool.domains[recordDomainKey{zone: temp.ZoneID, id: temp.DomainID}]
		if dom.status != model.StatusNew || dom.checkState != model.CheckStateQueued {
			t.Fatalf("temp_error-домен изменён при сбое корзины: %v/%v — транзакция обязана откатиться",
				dom.status, dom.checkState)
		}
		if got := pool.retryMatureValue(); got != 0 {
			t.Fatalf("корзина зрелости = %d после отката, want 0", got)
		}
	})
	t.Run("корзина зрелости: RowsAffected ≠ 1", func(t *testing.T) {
		// T-202, the P3-2 review: the upsert's guard branch — "a silent
		// must be an error and a rollback (silence — a counter bug).
		// must be an error and a rollback (silence — a counter bug).
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.matureTag = 2 // INSERT 0 2 — a foreign SQL/a protocol decay
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		if err == nil || !strings.Contains(err.Error(), "затронуто 2 строк (upsert корзины суток обязан 1)") {
			t.Fatalf("ошибка = %v, want RowsAffected-гард корзины зрелости", err)
		}
		if got := pool.retryMatureValue(); got != 0 {
			t.Fatalf("корзина зрелости = %d после отката, want 0", got)
		}
	})
	t.Run("часовая корзина темпа", func(t *testing.T) {
		// T-203: a bucket increment failure rolls back the transaction entirely —
		// undercounted silently).
		// undercounted silently).
		pool, q := seed(t)
		pool.failExec = map[string]error{recordResultsBumpSQL: errors.New("счётчики недоступны")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if got := pool.resultsValue(); got != 0 {
			t.Fatalf("часовая корзина темпа = %d после отката, want 0", got)
		}
	})
	t.Run("часовая корзина темпа: RowsAffected ≠ 1", func(t *testing.T) {
		// T-203: the upsert's guard branch (the maturity bucket's style,
		// the T-202 P3-2 review) — a silent delta loss would understate the throughput.
		pool, q := seed(t)
		pool.resultsTag = 2 // INSERT 0 2 — a foreign SQL/a protocol decay
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if err == nil || !strings.Contains(err.Error(), "затронуто 2 строк (upsert корзины часа обязан 1)") {
			t.Fatalf("ошибка = %v, want RowsAffected-гард корзины темпа", err)
		}
		if got := pool.resultsValue(); got != 0 {
			t.Fatalf("часовая корзина темпа = %d после отката, want 0", got)
		}
	})
	t.Run("Commit", func(t *testing.T) {
		pool, q := seed(t)
		pool.commitErr = errors.New("сервер упал на фиксации")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("ErrRefIntegrity откатывает всё", func(t *testing.T) {
		pool, q := seed(t)
		h := recordTestHash(5)
		pool.rows[recordDomainKey{zone: 7, id: 91}] = []recordFakeRow{
			{subdomainID: 0, variant: model.VariantMain, contentHash: h[:]},
		}
		// The hash's refcount is zero (a dead row) — the accounting diverged.
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		if !errors.Is(err, content.ErrRefIntegrity) {
			t.Fatalf("ожидался ErrRefIntegrity: %v", err)
		}
		assertRolledBack(t, pool, q, err, 1)
		// The old row is intact: the deletion rolled back together with the transaction.
		if rows := pool.rows[recordDomainKey{zone: 7, id: 91}]; len(rows) != 1 || string(rows[0].contentHash) != string(h[:]) {
			t.Fatalf("старая строка обязана пережить откат: %+v", rows)
		}
	})
	t.Run("отмена ctx", func(t *testing.T) {
		pool, q := seed(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := recordRecorder(t, pool, q).RecordAndPublish(ctx, res)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ожидалась отмена ctx: %v", err)
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

	t.Run("дедлок поглощён — транзакция повторена целиком", func(t *testing.T) {
		pool, q := seed(t)
		reg := metrics.New("http-worker")
		pool.failExecOnce = map[string]error{recordDomainSQL: deadlock}
		r, err := NewRecorder(pool, q, NewMetrics(reg), nil)
		if err != nil {
			t.Fatalf("NewRecorder: %v", err)
		}
		ok, err := r.RecordAndPublish(context.Background(), res)
		if err != nil || !ok {
			t.Fatalf("RecordAndPublish = (%v, %v), want (true, nil) — дедлок поглощён", ok, err)
		}
		// Two transaction attempts: the first rolled back at the UPDATE, the second
		// passed to the Commit; the publications — after the commit, one
		// chunk (one per subdomain×variant record, no attempt's duplicate).
		if pool.begins != 2 {
			t.Errorf("транзакций-попыток = %d, want 2 (повтор целиком)", pool.begins)
		}
		if got := pool.j.count(recordEvCommit); got != 1 {
			t.Errorf("Commit = %d, want 1 (зафиксирована только вторая попытка)", got)
		}
		if got, want := len(q.publishesSnapshot()), res.RecordCount(); got != want {
			t.Errorf("публикаций q:analyze = %d, want %d (одна пачка после фиксации)", got, want)
		}
		// An honest repeat: attempt 1 committed nothing, the counters are not doubled.
		if got := pool.checksValue(); got != 1 {
			t.Errorf("суточный счётчик = %d, want 1 (без задвоения дельт)", got)
		}
		if got := httpMetricValue(t, reg, "crawler_db_deadlock_retried_total", nil); got != 1 {
			t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
		}
	})

	t.Run("дедлок корзины счётчиков — тот же ретрай транзакции", func(t *testing.T) {
		pool, q := seed(t)
		pool.failExecOnce = map[string]error{recordChecksBumpSQL: deadlock}
		ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		if err != nil || !ok {
			t.Fatalf("RecordAndPublish = (%v, %v), want (true, nil)", ok, err)
		}
		if pool.begins != 2 || pool.checksValue() != 1 {
			t.Errorf("попыток %d, счётчик %d — want 2 и 1 (хвостовые дельты в повторе)", pool.begins, pool.checksValue())
		}
	})

	t.Run("исчерпание повторов — ошибка наружу, состояние откатано", func(t *testing.T) {
		pool, q := seed(t)
		reg := metrics.New("http-worker")
		pool.failExec = map[string]error{recordDomainSQL: deadlock}
		r, err := NewRecorder(pool, q, NewMetrics(reg), nil)
		if err != nil {
			t.Fatalf("NewRecorder: %v", err)
		}
		ok, err := r.RecordAndPublish(context.Background(), res)
		if err == nil || !db.IsDeadlock(err) || ok {
			t.Fatalf("RecordAndPublish = (%v, %v), want (false, 40P01) наружу", ok, err)
		}
		if want := 1 + db.MaxDeadlockRetries; pool.begins != want {
			t.Errorf("попыток = %d, want %d (первая + повторы)", pool.begins, want)
		}
		if dom := pool.domains[recordDomainKey{zone: 7, id: 91}]; dom.status != model.StatusNew ||
			dom.checkState != model.CheckStateQueued {
			t.Errorf("домен изменён: %v/%v — все попытки обязаны откатиться", dom.status, dom.checkState)
		}
		if got := len(q.publishesSnapshot()); got != 0 {
			t.Errorf("публикаций %d при незаписанном результате", got)
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
	body := "<html>" + strings.Repeat("страница для проверки ретрая дедлока ", 30) + "</html>"
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
			t.Fatal("XACK не дождались: дедлок не поглощён — задача сгорает в ределивери")
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
		t.Fatal("Run не завершился после отмены ctx")
	}
	if pool.begins != 2 {
		t.Errorf("попыток транзакции = %d, want 2", pool.begins)
	}
	if got := httpMetricValue(t, reg, "crawler_db_deadlock_retried_total", nil); got != 1 {
		t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
	}
}

// --- the publication: after the write, errors are not silent -------------------------------

func TestRecordPublishFailures(t *testing.T) {
	t.Run("сбой публикации — результат записан, доставка вернётся", func(t *testing.T) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		q := &recordFakeQueue{j: pool.j, publishErrs: []error{nil, errors.New("Redis отвалился")}}
		res, _ := recordRichResult()

		ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "публикация q:analyze") {
			t.Fatalf("ожидалась ошибка публикации: %v", err)
		}
		if !ok {
			t.Fatal("запись состоялась до публикации — recorded=true")
		}
		if len(pool.rows[recordDomainKey{zone: 7, id: 91}]) != 2 {
			t.Fatal("строки обязаны остаться: публикация повторной доставкой допишет хвост")
		}
	})
	t.Run("первое же сообщение не уходит", func(t *testing.T) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		q := &recordFakeQueue{j: pool.j, publishErrs: []error{errors.New("нет")}}
		res, _ := recordRichResult()
		if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res); err == nil {
			t.Fatal("ожидалась ошибка публикации")
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
		{"нулевая зона", &DomainResult{ZoneID: 0, DomainID: 1, Name: "x.test", Subdomains: []SubdomainResult{{Records: []Record{{}}}}}, "идентификаторы"},
		{"невалидный вариант", newRes(func(r *Record) { r.Variant = model.Variant(7) }), "вариант"},
		{"невалидная схема", newRes(func(r *Record) { r.Scheme = model.Scheme(9) }), "схема"},
		{"алиас без ссылки", newRes(func(r *Record) { r.AliasKind = model.AliasIdentical }), "T-055"},
		{"дубликат ключа", func() *DomainResult {
			res, _ := recordRichResult()
			rec := res.Subdomains[0].Records[0]
			res.Subdomains[1].Records = append(res.Subdomains[1].Records, rec)
			res.Subdomains[1].Records[1].SubdomainID = 0
			return res
		}(), "дубликат"},
		{"ContentNotStored с хэшем", newRes(func(r *Record) { r.ContentNotStored = true }), "T-056"},
		{"ContentNotStored у алиаса", newRes(func(r *Record) {
			r.AliasKind = model.AliasRedirect
			id := int16(1)
			r.AliasSubdomainID = &id
			r.ContentHash = nil
			r.StoreContent = false
			r.ContentNotStored = true
		}), "T-056"},
		{"оригинал без контента и признака", newRes(func(r *Record) { r.ContentHash = nil }), "T-056"},
		{"алиас с StoreContent", newRes(func(r *Record) {
			r.AliasKind = model.AliasRedirect
			id := int16(1)
			r.AliasSubdomainID = &id
			r.StoreContent = true
		}), "T-055"},
		{"класс записи вне доступной проверки", newRes(func(r *Record) { r.ErrClass = ErrTimeout }), "доступной"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newRecordFakePool()
			pool.recordSeedDomain(7, 91, model.StatusNew, 0)
			q := &recordFakeQueue{j: pool.j}
			_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), tc.res)
			if err == nil || !strings.Contains(err.Error(), tc.frag) {
				t.Fatalf("ожидалась ошибка валидации с %q: %v", tc.frag, err)
			}
			if pool.begins != 0 {
				t.Fatal("валидация обязана отсекать до открытия транзакции")
			}
		})
	}

	t.Run("nil-результат и зависимости", func(t *testing.T) {
		pool := newRecordFakePool()
		q := &recordFakeQueue{j: pool.j}
		if ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), nil); err != nil || ok {
			t.Fatalf("nil: ok=%v err=%v", ok, err)
		}
		if _, err := NewRecorder(nil, q, nil, nil); err == nil {
			t.Fatal("NewRecorder без пула прошёл")
		}
		if _, err := NewRecorder(pool, nil, nil, nil); err == nil {
			t.Fatal("NewRecorder без очереди прошёл")
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
		t.Fatal("доставка не подтверждена за 5 с")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	dom := pool.domains[recordDomainKey{zone: 5, id: 42}]
	if dom.status != model.StatusAlive || dom.checkState != model.CheckStateDone {
		t.Fatalf("домен: %v/%v, ожидался alive/done", dom.status, dom.checkState)
	}
	rows := pool.rows[recordDomainKey{zone: 5, id: 42}]
	if len(rows) != 1 || rows[0].contentHash == nil {
		t.Fatalf("строки: %+v — контент оригинала записан ссылкой", rows)
	}
	pubs := q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].msg.SubdomainID != 0 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации q:analyze: %+v", pubs)
	}
	if q.j.indexOf(recordEvPublish) > q.j.indexOf(recordEvAck) {
		t.Fatal("XACK раньше публикации q:analyze — нарушен порядок ТЗ 4.2/5.5 п. 9")
	}
	if _, err := st.Get(context.Background(), content.HashOf([]byte(page))); err != nil {
		t.Fatalf("контент не сохранён: %v", err)
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
	})}, publishErrs: []error{errors.New("Redis недоступен")}}

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
			t.Fatal("публикация не попыталась выполниться за 5 с")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// The delivery must stay unacknowledged.
	time.Sleep(50 * time.Millisecond)
	if got := q.j.count(recordEvAck); got != 0 {
		t.Fatalf("XACK %d при сбое публикации — доставка обязана вернуться (ТЗ 3.4)", got)
	}
	if len(q.ackedSnapshot()) != 0 {
		t.Fatal("ack зафиксирован при ошибке OnResult")
	}
	// The write did happen at that: the repeat will only finish publishing.
	if len(pool.rows[recordDomainKey{zone: 5, id: 43}]) != 1 {
		t.Fatal("результат не записан перед публикацией")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

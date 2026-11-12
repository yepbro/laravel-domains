// Unit tests of analyzer (T-071; spec 5.8, §7, §13.1, 3.4) on a fake
// pool, content store, rule set source and queue — no DB, Redis or
// network. The fake pool lives by the subdomain_results semantics (the
// analysis input SELECT, the rules_version UPDATE stamp with the
// optimistic content_hash guard and the domain status), domains
// (status <> deleted) and laravel_findings (an upsert by PK / DELETE);
// the fake ContentStore hands out content by hash. Checked: the exact
// SQL and parameters, the order "SELECT → Get content → EvaluateCtx →
// transaction (stamp → findings) → XACK", all hits regardless of the
// threshold and is_laravel, the no content/no hits branches, a version
// conflict, a row that slipped away, a repeated delivery, a set change
// (SIGHUP), step failures, ctx cancellation; the smoke test is the Run loop.
package analyzer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"

	"crawler/internal/content"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// --- the event protocol (the processing step order) -----------------------

// anEventKind is an event type of the protocol; the order is checked by indexes.
type anEventKind string

const (
	anEvGet       anEventKind = "get"         // the content read
	anEvBegin     anEventKind = "begin"       // opening the transaction
	anEvMark      anEventKind = "mark"        // the rules_version UPDATE stamp
	anEvUpsert    anEventKind = "upsert"      // INSERT ... ON CONFLICT laravel_findings
	anEvDelete    anEventKind = "delete"      // DELETE laravel_findings
	anEvLock      anEventKind = "lock"        // the domain advisory lock (T-072)
	anEvAggSelect anEventKind = "agg_select"  // the domain findings SELECT for the aggregation
	anEvAggExists anEventKind = "agg_exists"  // SELECT EXISTS laravel_domains (T-118)
	anEvAggUpsert anEventKind = "agg_upsert"  // upsert laravel_domains
	anEvAggDelete anEventKind = "agg_delete"  // DELETE laravel_domains
	anEvAggClose  anEventKind = "agg_close"   // the Close of the aggregation rows (T-M10)
	anEvHistory   anEventKind = "agg_hist"    // INSERT laravel_history appeared/lost (T-118)
	anEvZoneLarav anEventKind = "agg_zone"    // the zones.domains_laravel delta (T-178)
	anEvCounter   anEventKind = "agg_counter" // the crawler_counters cumulative delta (T-178)
	anEvConsume   anEventKind = "consume"     // the reanalyze: tail bucket decrement (T-204)
	anEvDrain     anEventKind = "drain"       // the domain content point drain (T-187)
	anEvCommit    anEventKind = "commit"
	anEvAck       anEventKind = "ack"
)

// anJournal is a thread-safe protocol journal (the Run loop runs in a goroutine).
type anJournal struct {
	mu     sync.Mutex
	events []anEventKind

	// ackNotify is a non-blocking signal per Ack (the smoke test sync).
	ackNotify chan struct{}
}

func (j *anJournal) record(k anEventKind) {
	j.mu.Lock()
	j.events = append(j.events, k)
	j.mu.Unlock()
}

func (j *anJournal) snapshot() []anEventKind {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]anEventKind, len(j.events))
	copy(out, j.events)
	return out
}

func (j *anJournal) indexOf(k anEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.events {
		if e == k {
			return i
		}
	}
	return -1
}

func (j *anJournal) signalAck() {
	if j.ackNotify == nil {
		return
	}
	select {
	case j.ackNotify <- struct{}{}:
	default:
	}
}

// --- the fake pgx pool: subdomain_results + domains + laravel_findings ----

// anRowKey is the subdomain_results and laravel_findings PK in the fake.
type anRowKey struct {
	zone      int16
	domain    int64
	subdomain int16
	variant   int16
}

// anResultRow is a subdomain_results row that analyzeSelectSQL reads
// and analyzeMarkSQL changes.
type anResultRow struct {
	headers      *string // jsonb
	cookies      *string // jsonb
	hash         []byte  // nil — NULL (no content)
	rulesVersion *int32
	checkedAt    time.Time // the tail bucket hour (RETURNING mark, T-204)
}

// anCheckedAt is the fixture seed row's mark: the reanalysis tail
// bucket hour in the tests (T-204) — fixed,
// a deterministic bucket key in the fake.
var anCheckedAt = time.Date(2026, 10, 27, 10, 15, 0, 0, time.UTC)

// anTailBasket is the tail bucket name of the seed mark's hour (as the
// SQL 'reanalyze:' || to_char(date_trunc('hour', …)) would compute).
func anTailBasket() string {
	return "reanalyze:" + anCheckedAt.Truncate(time.Hour).Format("2006-01-02T15")
}

// anFinding is a laravel_findings row written by findingUpsertSQL.
type anFinding struct {
	score        float32
	verMin       *string
	verMax       *string
	signals      string // jsonb text
	isLaravel    bool
	rulesVersion int32
}

// anFakePool is an AnalyzerDB without a DB: the live semantics of
// subdomain_results (a SELECT by PK, an UPDATE stamp with the
// content_hash IS NOT DISTINCT FROM and "domain not deleted" guards),
// domains (status), laravel_findings (upsert/DELETE by PK) and
// laravel_domains (an upsert/DELETE by the domain PK, the SELECT of
// confident hits — the T-072 aggregation). The transaction works on a
// snapshot: Rollback restores the state, Commit keeps it (concurrent
// transactions are not modeled — PostgreSQL serializes them).
type anFakePool struct {
	j *anJournal

	results     map[anRowKey]*anResultRow
	statuses    map[anRowKey]int16 // the domain status by the row key
	findings    map[anRowKey]*anFinding
	domains     map[anDomainKey]*anDomain
	history     []anHistoryRec   // laravel_history (T-118): an append-only transition journal
	zoneLaravel map[int16]int64  // the zone domains_laravel counters (T-178)
	counters    map[string]int64 // the crawler_counters rows (T-178)
	zoneNoRow   bool             // the zone row is missing (a manual reference-table edit)

	queryErr     error
	beginErr     error
	markErr      error
	consumeErr   error
	upsertErr    error
	deleteErr    error
	lockErr      error
	aggSelectErr error
	aggUpsertErr error
	aggDeleteErr error
	aggHistErr   error
	commitErr    error

	// Mutation test injections (T-M10): a Scan/Err failure of the
	// analysis input SELECT rows, a non-standard findings upsert
	// RowsAffected, an EXISTS read error of the domain row, a Scan/Err
	// failure of the aggregation select rows and a non-standard aggregate upsert RowsAffected.
	resultScanErr    error
	resultRowsErr    error
	upsertRowsAff    int64 // 0 — the standard "INSERT 0 1"
	existsErr        error
	aggScanErr       error
	aggRowsErr       error
	aggUpsertRowsAff int64 // 0 — the standard "INSERT 0 1"

	// Injections of the 11/09/2026 mutant triage: zone/cumulative delta
	// failures, a "no row" from the cumulative (INSERT 0 0) and a
	// non-standard tail bucket decrement RowsAffected.
	zoneLaravelErr  error
	counterDeltaErr error
	counterNoRow    bool
	consumeRowsAff  int64 // 0 — the standard "INSERT 0 1"

	// mutateBeforeTx is a hook: it is called before the transaction
	// snapshot (a simulation of the row being rewritten by a
	// recheck/render between the SELECT and the UPDATE, or of a domain deletion).
	mutateBeforeTx func(p *anFakePool)

	mu sync.Mutex
}

// anHistoryRec is a laravel_history record in the fake (T-118).
type anHistoryRec struct {
	zone     int16
	domain   int64
	appeared bool
}

func newANFakePool() *anFakePool {
	return &anFakePool{
		j:           &anJournal{},
		results:     make(map[anRowKey]*anResultRow),
		statuses:    make(map[anRowKey]int16),
		findings:    make(map[anRowKey]*anFinding),
		domains:     make(map[anDomainKey]*anDomain),
		zoneLaravel: make(map[int16]int64),
		counters:    make(map[string]int64),
	}
}

var _ AnalyzerDB = (*anFakePool)(nil)

// seedResult is a subdomain_results row of a domain with the alive status.
func (p *anFakePool) seedResult(key anRowKey, headers, cookies *string, hash []byte) {
	p.results[key] = &anResultRow{headers: headers, cookies: cookies, hash: hash, checkedAt: anCheckedAt}
	p.statuses[key] = int16(model.StatusAlive)
}

func (p *anFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("anFakePool: CopyFrom не должен вызываться")
}

func (p *anFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("anFakePool: Exec вне транзакции")
}

// anFakeRows is pgx.Rows with prepared SELECT rows (headers, cookies,
// content_hash); scanErr/err are Scan/Err failure injections (T-M10).
type anFakeRows struct {
	rows    [][]any
	i       int
	scanErr error
	err     error
}

func (r *anFakeRows) Close()                        {}
func (r *anFakeRows) Err() error                    { return r.err }
func (r *anFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *anFakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}
func (r *anFakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *anFakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.i == 0 || r.i > len(r.rows) || len(dest) != 3 {
		return fmt.Errorf("anFakeRows: Scan без Next или приёмников %d (нужно 3)", len(dest))
	}
	row := r.rows[r.i-1]
	// NULL → the nil destination stays untouched (the pgx semantics:
	// *string and *[]byte stay nil).
	for i, v := range row {
		if v == nil {
			continue
		}
		switch d := dest[i].(type) {
		case **string:
			*d = v.(*string)
		case *[]byte:
			*d = v.([]byte)
		default:
			return fmt.Errorf("anFakeRows: приёмник[%d] %T", i, dest[i])
		}
	}
	return nil
}

func (r *anFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *anFakeRows) RawValues() [][]byte    { return nil }
func (r *anFakeRows) Conn() *pgx.Conn        { return nil }
func (r *anFakeRows) TypeMap() *pgtype.Map   { return nil }

func (p *anFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if sql != analyzeSelectSQL {
		return nil, fmt.Errorf("anFakePool: неожидаемый SQL: %s", sql)
	}
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	key := anRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
	r, ok := p.results[key]
	if !ok {
		return &anFakeRows{err: p.resultRowsErr}, nil
	}
	var hash []byte
	if r.hash != nil {
		hash = append([]byte(nil), r.hash...)
	}
	return &anFakeRows{rows: [][]any{{r.headers, r.cookies, hash}},
		scanErr: p.resultScanErr, err: p.resultRowsErr}, nil
}

func (p *anFakePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	// The "another episode rewrote the row" hook runs before the snapshot:
	// the foreign transaction is already committed — our Rollback does not roll it back.
	if p.mutateBeforeTx != nil {
		p.mutateBeforeTx(p)
	}
	snapResults := make(map[anRowKey]*anResultRow, len(p.results))
	for k, v := range p.results {
		c := *v
		snapResults[k] = &c
	}
	snapFindings := make(map[anRowKey]*anFinding, len(p.findings))
	for k, v := range p.findings {
		c := *v
		snapFindings[k] = &c
	}
	snapDomains := make(map[anDomainKey]*anDomain, len(p.domains))
	for k, v := range p.domains {
		c := *v
		snapDomains[k] = &c
	}
	snapHistory := append([]anHistoryRec(nil), p.history...)
	snapZoneLaravel := make(map[int16]int64, len(p.zoneLaravel))
	for k, v := range p.zoneLaravel {
		snapZoneLaravel[k] = v
	}
	snapCounters := make(map[string]int64, len(p.counters))
	for k, v := range p.counters {
		snapCounters[k] = v
	}
	p.j.record(anEvBegin)
	return &anFakeTx{pool: p, snapResults: snapResults, snapFindings: snapFindings,
		snapDomains: snapDomains, snapHistory: snapHistory,
		snapZoneLaravel: snapZoneLaravel, snapCounters: snapCounters}, nil
}

type anFakeTx struct {
	pgx.Tx // the stage does not use the other pgx.Tx methods

	pool            *anFakePool
	snapResults     map[anRowKey]*anResultRow
	snapFindings    map[anRowKey]*anFinding
	snapDomains     map[anDomainKey]*anDomain
	snapHistory     []anHistoryRec
	snapZoneLaravel map[int16]int64
	snapCounters    map[string]int64
	commit          bool
}

func (t *anFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	switch sql {
	case analyzeReanalyzeConsumeSQL:
		// The reanalysis tail bucket decrement (T-204): $1 is the row's
		// checked_at from RETURNING mark (the bucket hour), $2 is the delta (−1);
		// the DB computes the name — the fake keeps the value by the mark's hour.
		if t.pool.consumeErr != nil {
			return pgconn.CommandTag{}, t.pool.consumeErr
		}
		if t.pool.consumeRowsAff > 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.consumeRowsAff)), nil
		}
		at, ok := args[0].(time.Time)
		if !ok {
			return pgconn.CommandTag{}, fmt.Errorf("anFakePool: consume: параметр %T, нужен time.Time", args[0])
		}
		t.pool.counters["reanalyze:"+at.Truncate(time.Hour).Format("2006-01-02T15")] += args[1].(int64)
		t.pool.j.record(anEvConsume)
		return pgconn.NewCommandTag("INSERT 0 1"), nil

	case findingUpsertSQL:
		key := anRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
		if t.pool.upsertErr != nil {
			return pgconn.CommandTag{}, t.pool.upsertErr
		}
		rows := int64(1)
		if t.pool.upsertRowsAff > 0 {
			rows = t.pool.upsertRowsAff
		}
		t.pool.findings[key] = &anFinding{
			score:        args[4].(float32),
			verMin:       args[5].(*string),
			verMax:       args[6].(*string),
			signals:      args[7].(string),
			isLaravel:    args[8].(bool),
			rulesVersion: args[9].(int32),
		}
		t.pool.j.record(anEvUpsert)
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", rows)), nil

	case findingDeleteSQL:
		key := anRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
		if t.pool.deleteErr != nil {
			return pgconn.CommandTag{}, t.pool.deleteErr
		}
		delete(t.pool.findings, key)
		t.pool.j.record(anEvDelete)
		return pgconn.NewCommandTag("DELETE 1"), nil

	case aggregateLockSQL:
		if t.pool.lockErr != nil {
			return pgconn.CommandTag{}, t.pool.lockErr
		}
		if args[0].(int64) == 0 {
			return pgconn.CommandTag{}, errors.New("anFakePool: ключ блокировки не задан")
		}
		t.pool.j.record(anEvLock)
		return pgconn.NewCommandTag("SELECT 1"), nil

	case aggregateUpsertSQL:
		key := anDomainKey{zone: args[0].(int16), domain: args[1].(int64)}
		if t.pool.aggUpsertErr != nil {
			return pgconn.CommandTag{}, t.pool.aggUpsertErr
		}
		rows := int64(1)
		if t.pool.aggUpsertRowsAff > 0 {
			rows = t.pool.aggUpsertRowsAff
		}
		t.pool.domains[key] = &anDomain{
			score:        args[2].(float32),
			verMin:       args[3].(*string),
			verMax:       args[4].(*string),
			foundOnMask:  model.DNSMask(args[5].(int64)),
			rulesVersion: args[6].(int32),
			updatedAt:    time.Now(),
		}
		t.pool.j.record(anEvAggUpsert)
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", rows)), nil

	case aggregateDeleteSQL:
		key := anDomainKey{zone: args[0].(int16), domain: args[1].(int64)}
		if t.pool.aggDeleteErr != nil {
			return pgconn.CommandTag{}, t.pool.aggDeleteErr
		}
		_, ok := t.pool.domains[key]
		delete(t.pool.domains, key)
		tag := "DELETE 0"
		if ok {
			tag = "DELETE 1"
		}
		t.pool.j.record(anEvAggDelete)
		return pgconn.NewCommandTag(tag), nil

	case aggregateHistorySQL:
		if t.pool.aggHistErr != nil {
			return pgconn.CommandTag{}, t.pool.aggHistErr
		}
		t.pool.history = append(t.pool.history, anHistoryRec{
			zone:     args[0].(int16),
			domain:   args[1].(int64),
			appeared: args[2].(bool),
		})
		t.pool.j.record(anEvHistory)
		return pgconn.NewCommandTag("INSERT 0 1"), nil

	case aggregateZoneLaravelSQL:
		// The zone counter delta (T-178): delta, zone.
		if t.pool.zoneLaravelErr != nil {
			return pgconn.CommandTag{}, t.pool.zoneLaravelErr
		}
		if t.pool.zoneNoRow {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		t.pool.zoneLaravel[args[1].(int16)] += args[0].(int64)
		t.pool.j.record(anEvZoneLarav)
		return pgconn.NewCommandTag("UPDATE 1"), nil

	case aggregateCounterDeltaSQL:
		// The history cumulative increment (T-178): an upsert — the row may
		// not exist (the first reconciliation has not run yet).
		if t.pool.counterDeltaErr != nil {
			return pgconn.CommandTag{}, t.pool.counterDeltaErr
		}
		if t.pool.counterNoRow {
			return pgconn.NewCommandTag("INSERT 0 0"), nil
		}
		t.pool.counters[args[0].(string)] += args[1].(int64)
		t.pool.j.record(anEvCounter)
		return pgconn.NewCommandTag("INSERT 0 1"), nil

	default:
		return pgconn.CommandTag{}, fmt.Errorf("anFakePool: неожидаемый SQL: %s", sql)
	}
}

// anFakeRow is a pgx.Row with a single value (the T-118 EXISTS —
// *bool; the T-204 mark stamp — *time.Time) or an error.
type anFakeRow struct {
	v   any
	err error
}

func (r anFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("anFakeRow: приёмников %d, нужно 1", len(dest))
	}
	switch d := dest[0].(type) {
	case *bool:
		*d = r.v.(bool)
	case *time.Time:
		at, ok := r.v.(time.Time)
		if !ok {
			return fmt.Errorf("anFakeRow: значение %T, нужен time.Time", r.v)
		}
		*d = at
	default:
		return fmt.Errorf("anFakeRow: приёмник %T (нужен *bool или *time.Time)", dest[0])
	}
	return nil
}

// QueryRow — the transaction's only row queries: the rules_version
// stamp (analyzeMarkSQL, T-204: the freshness guards — content_hash, a
// version different from the current one, the domain not deleted —
// RETURNING checked_at of the moved row) and the laravel_domains row
// presence (aggregateExistsSQL, T-118); anything else fails loudly (unused paths would surface in the tests).
func (t *anFakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if err := ctx.Err(); err != nil {
		return anFakeRow{err: err}
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	switch sql {
	case analyzeMarkSQL:
		key := anRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
		if t.pool.markErr != nil {
			return anFakeRow{err: t.pool.markErr}
		}
		version := args[4].(int32)
		hash := args[5].([]byte)
		r, ok := t.pool.results[key]
		if !ok {
			return anFakeRow{err: pgx.ErrNoRows}
		}
		// The content_hash IS NOT DISTINCT FROM $6 guard.
		same := (r.hash == nil && hash == nil) ||
			(r.hash != nil && hash != nil && string(r.hash) == string(hash))
		if !same {
			return anFakeRow{err: pgx.ErrNoRows}
		}
		// The version freshness guard (T-204): the stamp only for stale
		// rows (NULL or an old version); a current one is a no-op.
		if r.rulesVersion != nil && *r.rulesVersion == version {
			return anFakeRow{err: pgx.ErrNoRows}
		}
		if t.pool.statuses[key] == int16(model.StatusDeleted) {
			return anFakeRow{err: pgx.ErrNoRows}
		}
		v := version
		r.rulesVersion = &v
		t.pool.j.record(anEvMark)
		return anFakeRow{v: r.checkedAt}
	case aggregateExistsSQL:
		if t.pool.existsErr != nil {
			return anFakeRow{err: t.pool.existsErr}
		}
		_, exists := t.pool.domains[anDomainKey{zone: args[0].(int16), domain: args[1].(int64)}]
		t.pool.j.record(anEvAggExists)
		return anFakeRow{v: exists}
	default:
		return anFakeRow{err: fmt.Errorf("anFakeTx: неожидаемый SQL QueryRow: %s", sql)}
	}
}

func (t *anFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.commit = true
	t.pool.j.record(anEvCommit)
	return nil
}

func (t *anFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.commit {
		return nil // a rollback after Commit is a no-op
	}
	t.pool.results, t.pool.findings, t.pool.domains = t.snapResults, t.snapFindings, t.snapDomains
	t.pool.history = t.snapHistory
	t.pool.zoneLaravel, t.pool.counters = t.snapZoneLaravel, t.snapCounters
	return nil
}

// --- the fake content store ------------------------------------------------

// anFakeStore is a ContentGetter: Get by hash.
type anFakeStore struct {
	j *anJournal

	blocks map[content.Hash][]byte
	getErr error

	mu sync.Mutex
}

var _ ContentGetter = (*anFakeStore)(nil)

func (s *anFakeStore) Get(ctx context.Context, h content.Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.j.record(anEvGet)
	if s.getErr != nil {
		return nil, s.getErr
	}
	data, ok := s.blocks[h]
	if !ok {
		return nil, content.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

// --- the fake rule set source ----------------------------------------------

// anFakeSets is a SetSource with a replaceable snapshot (it models the
// Reloader: a startup set + an SIGHUP reload).
type anFakeSets struct {
	mu  sync.Mutex
	set *rules.Set
}

var _ SetSource = (*anFakeSets)(nil)

func (f *anFakeSets) Set() *rules.Set {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.set
}

func (f *anFakeSets) swap(set *rules.Set) {
	f.mu.Lock()
	f.set = set
	f.mu.Unlock()
}

// --- the fake queue --------------------------------------------------------

// anFakeQueue is a queue.Queue: the q:analyze consumer; the stage does
// not need Publish (the §7 pipeline end) — a call breaks the test loudly.
type anFakeQueue struct {
	j *anJournal

	deliveries []queue.Delivery
	consumeErr error

	mu       sync.Mutex
	acked    []string
	consumes int
}

var _ queue.Queue = (*anFakeQueue)(nil)

func (q *anFakeQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", errors.New("anFakeQueue: Publish не должен вызываться (analyzer не публикует)")
}

// Consume reads a q:analyze window. The call counter is kept BEFORE the
// ctx check: a cancelled call is still a call (an extra read after the
// window stop is distinguishable by the test, T-M10).
func (q *anFakeQueue) Consume(ctx context.Context, stream, group, consumer string, n int) ([]queue.Delivery, error) {
	q.mu.Lock()
	q.consumes++
	if err := ctx.Err(); err != nil {
		q.mu.Unlock()
		return nil, err
	}
	if stream != queue.StreamAnalyze {
		q.mu.Unlock()
		return nil, fmt.Errorf("anFakeQueue: неожидаемый стрим %s", stream)
	}
	if group != DefaultGroup {
		q.mu.Unlock()
		return nil, fmt.Errorf("anFakeQueue: неожидаемая группа %s", group)
	}
	if consumer == "" {
		q.mu.Unlock()
		return nil, errors.New("anFakeQueue: пустой consumer")
	}
	var out []queue.Delivery
	if q.consumeErr != nil {
		err := q.consumeErr
		q.mu.Unlock()
		return nil, err
	}
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

func (q *anFakeQueue) Ack(ctx context.Context, _, _ string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked = append(q.acked, ids...)
	q.mu.Unlock()
	for range ids {
		q.j.record(anEvAck)
	}
	q.j.signalAck()
	return nil
}

func (q *anFakeQueue) ackedSnapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, len(q.acked))
	copy(out, q.acked)
	return out
}

func (q *anFakeQueue) consumesCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

// --- builders --------------------------------------------------------------

// anTestHash gives distinguishable hashes: anTestHash(1), (2), …
func anTestHash(n byte) content.Hash {
	var h content.Hash
	h[0] = n
	return h
}

// anTestHashN is the bytes of a distinguishable hash for the fixture's content_hash column.
func anTestHashN(n byte) []byte {
	h := anTestHash(n)
	out := make([]byte, len(h))
	copy(out, h[:])
	return out
}

// anSet is the fixture rule set: the XSRF-TOKEN cookie (0.25), a
// cookie with the _session suffix (a regex, 0.35), an html welcome
// (0.45, the [5.3, 7.x] range); the version is set separately (an SIGHUP reload changes it).
func anSet(version int32) *rules.Set {
	return &rules.Set{
		Rules: []rules.Rule{
			{ID: "cookie_xsrf_token", Target: rules.TargetCookieName, Match: "XSRF-TOKEN", Weight: 0.25},
			{ID: "cookie_session_suffix", Target: rules.TargetCookieName, Regex: `^[a-z0-9_]+_session$`, Weight: 0.35},
			{ID: "html_welcome", Target: rules.TargetHTML, Regex: `<div class="content">`, Weight: 0.45,
				VerMin: "5.3", VerMax: "7.x"},
		},
		Version: version,
	}
}

// anConflictSet is rules with non-intersecting version ranges.
func anConflictSet(version int32) *rules.Set {
	return &rules.Set{
		Rules: []rules.Rule{
			{ID: "a_old", Target: rules.TargetHTML, Regex: `Old-App`, Weight: 0.5, VerMax: "6.x"},
			{ID: "b_new", Target: rules.TargetHTML, Regex: `New-App`, Weight: 0.5, VerMin: "10"},
		},
		Version: version,
	}
}

// anNaNSet is a rule with a NaN weight: the engine passes the weight
// into Evidence as is, json.Marshal returns an error — a deterministic
// marshalSignals failure inside ProcessResult (T-M10).
func anNaNSet(version int32) *rules.Set {
	return &rules.Set{
		Rules: []rules.Rule{
			{ID: "cookie_nan", Target: rules.TargetCookieName, Match: "XSRF-TOKEN", Weight: math.NaN()},
		},
		Version: version,
	}
}

// anJSON serializes values for the fixture's jsonb fields.
func anJSON(t *testing.T, v any) *string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("anJSON: %v", err)
	}
	s := string(b)
	return &s
}

// anFixture is the assembled set of fakes and the worker.
type anFixture struct {
	pool  *anFakePool
	store *anFakeStore
	sets  *anFakeSets
	q     *anFakeQueue
	w     *Worker
}

func newANFixture(t *testing.T, set *rules.Set, opts Options) *anFixture {
	t.Helper()
	pool := newANFakePool()
	store := &anFakeStore{j: pool.j, blocks: make(map[content.Hash][]byte)}
	sets := &anFakeSets{set: set}
	q := &anFakeQueue{j: pool.j}
	w, err := New(pool, q, store, sets, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &anFixture{pool: pool, store: store, sets: sets, q: q, w: w}
}

// anKey is the fixture row key.
var anKey = anRowKey{zone: 3, domain: 42, subdomain: 1, variant: 0}

// anTask is a q:analyze task on the fixture key.
var anTask = queue.Result{
	ZoneID: anKey.zone, DomainID: anKey.domain,
	SubdomainID: anKey.subdomain, Variant: 0, Name: "laravel.example",
}

// anDelivery is a valid delivery with a Result message.
func anDelivery(id string) queue.Delivery {
	payload, err := json.Marshal(anTask)
	if err != nil {
		panic(err)
	}
	return queue.Delivery{Stream: queue.StreamAnalyze, ID: id, Payload: payload}
}

// anHeaders is the jsonb of a typical row's headers.
func anHeaders(t *testing.T) *string {
	return anJSON(t, map[string][]string{
		"Content-Type": {"text/html; charset=UTF-8"},
		"Set-Cookie":   {"XSRF-TOKEN=abc; path=/", "laravel_session=xyz; path=/"},
	})
}

// anCookies is the jsonb of a typical row's cookies: name/value plus
// attributes the engine ignores (the T-058/T-067 contract).
func anCookies(t *testing.T, names ...string) *string {
	t.Helper()
	type cookie struct {
		Name     string `json:"name"`
		Value    string `json:"value"`
		Path     string `json:"path"`
		Domain   string `json:"domain"`
		Secure   bool   `json:"secure"`
		HTTPOnly bool   `json:"http_only"`
	}
	out := make([]cookie, 0, len(names))
	for _, n := range names {
		out = append(out, cookie{Name: n, Value: "eyJpdiI6", Path: "/", HTTPOnly: true})
	}
	return anJSON(t, out)
}

// anPageHTML is a page with the welcome sign (the html_welcome rule).
func anPageHTML() []byte {
	return []byte(`<html><body><div class="content"><div class="title">Laravel</div></div></body></html>`)
}

// anFindingSnapshot is a snapshot of the findings row.
func anFindingSnapshot(p *anFakePool, key anRowKey) (anFinding, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f, ok := p.findings[key]; ok {
		return *f, true
	}
	return anFinding{}, false
}

// anResultSnapshot is a snapshot of the subdomain_results row.
func anResultSnapshot(p *anFakePool, key anRowKey) (anResultRow, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r, ok := p.results[key]; ok {
		return *r, true
	}
	return anResultRow{}, false
}

// --- ProcessResult -----------------------------------------------------------

// anDebugLog is a logger into a thread-safe buffer at the Debug level
// (muting the T-M10 log-line mutants; reanBuf is in reanalyze_test.go).
func anDebugLog(buf *reanBuf) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestAnalyzer_HappyPath — the end-to-end contract: the input SELECT →
// Get content → EvaluateCtx → a transaction (the rules_version stamp →
// the INSERT of findings with all hits) → COMMIT; the step order is per the journal.
func TestAnalyzer_HappyPath(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if !wrote {
		t.Fatal("ожидана запись findings")
	}

	// The order: content → transaction → stamp → findings → commit.
	events := f.pool.j.snapshot()
	for _, pair := range [][2]anEventKind{
		{anEvGet, anEvBegin}, {anEvBegin, anEvMark}, {anEvMark, anEvUpsert}, {anEvUpsert, anEvCommit},
	} {
		if f.pool.j.indexOf(pair[0]) > f.pool.j.indexOf(pair[1]) {
			t.Fatalf("порядок событий %v после %v нарушен: %v", pair[1], pair[0], events)
		}
	}

	// The rule set version stamp on the result row.
	row, ok := anResultSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("строка результата исчезла")
	}
	if row.rulesVersion == nil || *row.rulesVersion != 1001 {
		t.Fatalf("rules_version строки = %v, нужно 1001", row.rulesVersion)
	}

	// Findings: all hits (three rules), the noisy-OR score
	// 1-(1-0.25)(1-0.35)(1-0.45) ≈ 0.7231 ≥ 0.6 → is_laravel,
	// the [5.3, 7.x] range from the html rule.
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("строка laravel_findings не записана")
	}
	wantScore := float32(1 - (1-0.25)*(1-0.35)*(1-0.45))
	if finding.score != wantScore {
		t.Fatalf("score = %v, нужно %v", finding.score, wantScore)
	}
	if !finding.isLaravel {
		t.Fatalf("is_laravel = false при score %v ≥ 0.6", finding.score)
	}
	if finding.verMin == nil || *finding.verMin != "5.3" {
		t.Fatalf("ver_min = %v, нужно 5.3", finding.verMin)
	}
	if finding.verMax == nil || *finding.verMax != "7.x" {
		t.Fatalf("ver_max = %v, нужно 7.x", finding.verMax)
	}
	if finding.rulesVersion != 1001 {
		t.Fatalf("rules_version findings = %d, нужно 1001", finding.rulesVersion)
	}

	// signals: [{rule, weight, evidence}, …] — all three hits, the
	// T-069 format evidence.
	var sigs []map[string]any
	if err := json.Unmarshal([]byte(finding.signals), &sigs); err != nil {
		t.Fatalf("signals не JSON: %v\n%s", err, finding.signals)
	}
	if len(sigs) != 3 {
		t.Fatalf("signals: %d записей, нужно 3 (%s)", len(sigs), finding.signals)
	}
	byRule := make(map[string]map[string]any, len(sigs))
	for _, s := range sigs {
		byRule[s["rule"].(string)] = s
	}
	for _, id := range []string{"cookie_xsrf_token", "cookie_session_suffix", "html_welcome"} {
		if _, ok := byRule[id]; !ok {
			t.Fatalf("в signals нет правила %s: %s", id, finding.signals)
		}
	}
	if ev := byRule["cookie_xsrf_token"]["evidence"]; ev != "cookie XSRF-TOKEN" {
		t.Fatalf("свидетельство cookie_xsrf_token = %v", ev)
	}
}

// TestAnalyzer_SQLContract — the exact stage SQL text (the contract
// against migration 000001: the laravel_findings PK (zone_id,
// domain_id, subdomain_id, variant), score real, ver_min/ver_max
// text, signals jsonb, is_laravel boolean, rules_version integer NOT NULL).
func TestAnalyzer_SQLContract(t *testing.T) {
	if !strings.Contains(analyzeSelectSQL, "SELECT headers, cookies, content_hash FROM subdomain_results") {
		t.Error("select: не читает вход анализа (headers/cookies/content_hash)")
	}
	if !strings.Contains(analyzeMarkSQL, "SET rules_version = $5::int4") {
		t.Error("mark: нет штампа rules_version")
	}
	if !strings.Contains(analyzeMarkSQL, "content_hash IS NOT DISTINCT FROM $6::bytea") {
		t.Error("mark: нет охраны актуальности content_hash")
	}
	if !strings.Contains(analyzeMarkSQL, "rules_version IS DISTINCT FROM $5::int4") {
		t.Error("mark: нет охраны актуальности версии (штамп — только устаревшим, T-204)")
	}
	if !strings.Contains(analyzeMarkSQL, "RETURNING r.checked_at") {
		t.Error("mark: нет RETURNING checked_at (час корзины хвостов, T-204)")
	}
	if !strings.Contains(analyzeMarkSQL, fmt.Sprintf("d.status = %d", int16(model.StatusDeleted))) {
		t.Error("mark: нет охраны «домен не deleted» (§8/§13.1)")
	}
	if !strings.Contains(analyzeReanalyzeConsumeSQL,
		"'reanalyze:' || to_char(date_trunc('hour', $1::timestamptz), 'YYYY-MM-DD\"T\"HH24')") {
		t.Error("consume: нет имени корзины хвостов от БД (T-204)")
	}
	if !strings.Contains(findingUpsertSQL,
		"ON CONFLICT (zone_id, domain_id, subdomain_id, variant) DO UPDATE") {
		t.Error("upsert: нет конфликта по PK laravel_findings (§3.5 перезапись)")
	}
	for _, col := range []string{"score", "ver_min", "ver_max", "signals", "is_laravel", "rules_version"} {
		if !strings.Contains(findingUpsertSQL, col) {
			t.Errorf("upsert: нет колонки %s (§6.1)", col)
		}
	}
	if !strings.Contains(findingUpsertSQL, "$5::float4") {
		t.Error("upsert: score не приводится к real (float4)")
	}
	if !strings.Contains(findingUpsertSQL, "$8::jsonb") {
		t.Error("upsert: signals не приводятся к jsonb")
	}
	if !strings.Contains(findingDeleteSQL, "DELETE FROM laravel_findings") {
		t.Error("delete: не удаляет строку laravel_findings")
	}
}

// TestAnalyzer_AllMatchesBelowThreshold — all hits are written
// regardless of the threshold; a weak set below the threshold →
// is_laravel = false (§13.1: separate confident hits from weak ones).
func TestAnalyzer_AllMatchesBelowThreshold(t *testing.T) {
	// Only XSRF-TOKEN 0.25 < 0.6.
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), nil) // no content

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("слабые срабатывания обязаны записываться независимо от порога")
	}
	if finding.isLaravel {
		t.Fatal("is_laravel = true при score 0.25 < 0.6")
	}
	if finding.score != 0.25 {
		t.Fatalf("score = %v, нужно 0.25", finding.score)
	}
	if !strings.Contains(finding.signals, "cookie_xsrf_token") {
		t.Fatalf("signals не содержат сработавшее правило: %s", finding.signals)
	}
}

// TestAnalyzer_ThresholdBoundary — the threshold is non-strict:
// score ≥ min (equality on the boundary is Laravel, T-070);
// MinScore ≤ 0 is replaced with DefaultMinScore.
func TestAnalyzer_ThresholdBoundary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		minScore float64
		want     bool
	}{
		{"равенство на границе", 0.25, true},
		{"чуть выше score", 0.26, false},
		{"нулевая опция — дефолт 0.6", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), Options{MinScore: tc.minScore})
			f.pool.seedResult(anKey, nil, anCookies(t, "XSRF-TOKEN"), nil)
			if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			finding, ok := anFindingSnapshot(f.pool, anKey)
			if !ok {
				t.Fatal("строка findings не записана")
			}
			if finding.isLaravel != tc.want {
				t.Fatalf("is_laravel = %v при score 0.25, min %v", finding.isLaravel, tc.minScore)
			}
		})
	}
}

// TestAnalyzer_VersionConflict — an empty range intersection:
// NULL ver_min/ver_max, both bounds empty (T-070).
func TestAnalyzer_VersionConflict(t *testing.T) {
	f := newANFixture(t, anConflictSet(2002), Options{})
	hash := anTestHash(9)
	f.store.blocks[hash] = []byte("Old-App New-App")
	f.pool.seedResult(anKey, nil, nil, hash[:])

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("строка findings не записана (оба правила сработали)")
	}
	if finding.score != 0.75 { // noisy-OR(0.5, 0.5)
		t.Fatalf("score = %v, нужно 0.75", finding.score)
	}
	if finding.verMin != nil || finding.verMax != nil {
		t.Fatalf("конфликт диапазонов: границы обязаны быть NULL, получены %v/%v", finding.verMin, finding.verMax)
	}
}

// TestAnalyzer_TailBasket — the unanalyzed reanalyze: tails bucket
// (T-204): stamping a stale row decrements the EPISODE hour bucket
// (the checked_at from RETURNING mark) in the same transaction —
// after the domain aggregation, before the commit; an already current
// version is not stamped and writes no delta; a decrement failure rolls back the whole transaction (no stamp).
func TestAnalyzer_TailBasket(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if got := f.pool.counters[anTailBasket()]; got != -1 {
		t.Fatalf("корзина хвостов = %d, нужно −1 (штамп устаревшей строки)", got)
	}
	// The order: the decrement after the domain aggregation, before the commit.
	if c, k, m := f.pool.j.indexOf(anEvConsume), f.pool.j.indexOf(anEvAggUpsert), f.pool.j.indexOf(anEvCommit); c < k || c > m {
		t.Errorf("порядок: consume=%d agg_upsert=%d commit=%d (агрегация → декремент → фиксация)",
			c, k, m)
	}

	// A decrement failure — a delivery error, the transaction rolls back
	// (no stamp, no findings).
	f2 := newANFixture(t, anSet(1001), Options{})
	f2.store.blocks[hash] = anPageHTML()
	f2.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
	f2.pool.consumeErr = errors.New("consume boom")
	if _, err := f2.w.ProcessResult(context.Background(), anTask); err == nil ||
		!strings.Contains(err.Error(), "корзина хвостов переанализа: consume boom") {
		t.Fatalf("ошибка = %v, want контекст корзины и исходная", err)
	}
	if row, ok := anResultSnapshot(f2.pool, anKey); ok && row.rulesVersion != nil {
		t.Fatal("штамп поставлен при сбое декремента (откат §3.4)")
	}
	if _, ok := anFindingSnapshot(f2.pool, anKey); ok {
		t.Fatal("findings записаны при сбое декремента (откат §3.4)")
	}
}

// TestAnalyzer_NoSignalsDeletesRow — no rule hit: the row is not
// written, an existing one (a past analysis) is deleted — the §3.5
// rewrite; the rules_version stamp is put on an "empty" outcome too.
func TestAnalyzer_NoSignalsDeletesRow(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	stale := "stale"
	f.pool.findings[anKey] = &anFinding{score: 0.9, verMin: &stale, isLaravel: true, rulesVersion: 999}
	hash := anTestHash(5)
	f.store.blocks[hash] = []byte("<html><body>чужой фреймворк без признаков</body></html>")
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), hash[:])

	// An "empty" outcome is still an outcome: the transaction commits
	// (the stamp + the DELETE), but the findings row is not written.
	if wrote, err := f.w.ProcessResult(context.Background(), anTask); err != nil || !wrote {
		t.Fatalf("ProcessResult = %v, %v; нужен успех с записью (штамп/удаление)", wrote, err)
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("прошлая строка findings не удалена (§3.5 перезапись)")
	}
	if f.pool.j.indexOf(anEvDelete) == -1 {
		t.Fatal("DELETE laravel_findings не выполнялся")
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion == nil || *row.rulesVersion != 1001 {
		t.Fatalf("rules_version строки = %v, нужен штамп 1001 и на пустом итоге", row.rulesVersion)
	}
}

// TestAnalyzer_NoContent — no content (a NULL content_hash, lost
// content, an empty DOM) — the analysis by headers and cookies (spec
// 5.8: rules over six targets, html/meta is not the whole analysis).
func TestAnalyzer_NoContent(t *testing.T) {
	cases := []struct {
		name  string
		hash  []byte
		block []byte // nil — no content in the store (ErrNotFound)
	}{
		{"NULL content_hash", nil, nil},
		{"контент потерян", anTestHashN(3), nil},
		{"пустой DOM", anTestHashN(4), []byte{}},
		{"урезанный хэш", []byte{1, 2, 3}, []byte("ignored")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), Options{})
			if tc.block != nil {
				var h content.Hash
				copy(h[:], tc.hash)
				f.store.blocks[h] = tc.block
			}
			f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), tc.hash)

			if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			finding, ok := anFindingSnapshot(f.pool, anKey)
			if !ok {
				t.Fatal("куки обязаны срабатывать без контента")
			}
			if !strings.Contains(finding.signals, "cookie_xsrf_token") {
				t.Fatalf("куки не проанализированы: %s", finding.signals)
			}
			if strings.Contains(finding.signals, "html_welcome") {
				t.Fatalf("html-правило сработало без контента: %s", finding.signals)
			}
		})
	}
}

// TestAnalyzer_RowMissing — no row (the domain was rechecked without
// this key, the rows are purged): success without a write, no
// findings (the renderworker style: 0 rows → success); the skip Debug
// record is captured by the log buffer (the log-cut mutant, T-M10).
func TestAnalyzer_RowMissing(t *testing.T) {
	var buf reanBuf
	f := newANFixture(t, anSet(1001), Options{Log: anDebugLog(&buf)})
	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("без строки findings не пишутся")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны без строки результата")
	}
	if f.pool.j.indexOf(anEvBegin) != -1 {
		t.Fatal("без строки транзакция не открывается")
	}
	if !strings.Contains(buf.String(), "анализ не требуется — строки нет") {
		t.Errorf("Debug о пропуске не записан:\n%s", buf.String())
	}
}

// TestAnalyzer_NoRulesSet — a nil SetSource snapshot (a Reloader never
// has one; a guard for alternative implementations, the symmetry with
// ReanalyzeLoop): a skip without a panic or a write; the unstamped
// row will come back via re-analysis (T-074). The skip Warn is
// captured by the buffer (the log-cut mutant, T-M10).
func TestAnalyzer_NoRulesSet(t *testing.T) {
	var buf reanBuf
	f := newANFixture(t, nil, Options{Log: slog.New(slog.NewTextHandler(&buf, nil))})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), nil)

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("без набора правил запись невозможна")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны без набора правил")
	}
	if f.pool.j.indexOf(anEvBegin) != -1 {
		t.Fatal("без набора правил транзакция не открывается")
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion != nil {
		t.Fatal("штамп без анализа ставить нельзя — переанализ вернётся к строке")
	}
	if !strings.Contains(buf.String(), "набор правил не загружен — анализ пропущен") {
		t.Errorf("Warn о пропуске не записан:\n%s", buf.String())
	}
}

// anLateCancelCtx is a context that cancels between the analysis input
// SELECT and applying the rules (the countingCtx style from
// engine_test): the first Err() call returns nil (the SELECT goes
// through), then a cancellation; the Done channel is already closed,
// so the timeout wrap inside EvaluateCtx inherits the cancellation
// synchronously — a deterministic Partial with no real timers.
type anLateCancelCtx struct {
	calls int
	done  chan struct{}
}

func (c *anLateCancelCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *anLateCancelCtx) Done() <-chan struct{}       { return c.done }
func (c *anLateCancelCtx) Value(any) any               { return nil }

func (c *anLateCancelCtx) Err() error {
	c.calls++
	if c.calls == 1 {
		return nil
	}
	return context.Canceled
}

var anClosedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// TestAnalyzer_PartialResultNotRecorded — the rule scan was interrupted
// (an EvalTimeout/a ctx cancellation, Result.Partial): a truncated
// result is not written and not stamped as a full rules_version; the
// row stays stale — re-analysis (T-074) will return to it.
func TestAnalyzer_PartialResultNotRecorded(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), nil) // no content

	wrote, err := f.w.ProcessResult(&anLateCancelCtx{done: anClosedChan}, anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("неполный результат не записывается")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("усечённые findings записаны как полные")
	}
	if f.pool.j.indexOf(anEvBegin) != -1 {
		t.Fatal("для неполного результата транзакция не открывается")
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion != nil {
		t.Fatal("штамп rules_version на неполном результате ставить нельзя")
	}
}

// TestAnalyzer_StaleRow — the row was rewritten by another episode
// between the SELECT and the UPDATE (a recheck DELETE+INSERT, a render
// changed the content): the content_hash guard → 0 rows → success
// without a write; the new episode has its own q:analyze publish. It is
// captured by the buffer (the log-cut mutant, T-M10).
func TestAnalyzer_StaleRow(t *testing.T) {
	var buf reanBuf
	f := newANFixture(t, anSet(1001), Options{Log: anDebugLog(&buf)})
	old := anTestHash(1)
	f.store.blocks[old] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), old[:])

	f.pool.mutateBeforeTx = func(p *anFakePool) {
		if r, ok := p.results[anKey]; ok {
			newHash := anTestHash(2)
			r.hash = newHash[:]
			r.rulesVersion = nil // T-067: a content change resets the stamp
		}
	}

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("ушедшая из-под задачи строка не перезаписывается")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны по неактуальной строке")
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion != nil {
		t.Fatalf("штамп поставлен неактуальному эпизоду: %v", row.rulesVersion)
	}
	if f.pool.j.indexOf(anEvCommit) != -1 {
		t.Fatal("0 строк — транзакция откатывается без фиксации")
	}
	if !strings.Contains(buf.String(), "findings не записаны — строка перезаписана") {
		t.Errorf("Debug о незаписи не записан:\n%s", buf.String())
	}
}

// TestAnalyzer_DomainDeleted — the domain was deleted during the analysis
// (§8/§13.1): the status guard → 0 rows → success without writing findings.
func TestAnalyzer_DomainDeleted(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
	f.pool.mutateBeforeTx = func(p *anFakePool) {
		p.statuses[anKey] = int16(model.StatusDeleted) // the delete file beat the write
	}

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("удалённому домену результат не записывается (§8)")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны удалённому домену")
	}
}

// TestAnalyzer_RepeatDelivery — a repeated delivery is idempotent
// (spec 3.4): the first pass stamps and writes the findings; a
// repeated one with the same version is a no-op (the mark version
// freshness guard, T-204): the findings are not rewritten (the same
// version × the same content deterministically give the same), the
// tail bucket decrement is not doubled — a repeat has no paired write
// increment. The XACK of a worker that lost its response is safe.
func TestAnalyzer_RepeatDelivery(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])

	for i := 0; i < 2; i++ {
		if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
			t.Fatalf("доставка №%d: %v", i+1, err)
		}
	}
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("строка findings не записана")
	}
	if finding.rulesVersion != 1001 {
		t.Fatalf("rules_version = %d, нужно 1001", finding.rulesVersion)
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion == nil || *row.rulesVersion != 1001 {
		t.Fatalf("rules_version строки = %v, нужно 1001", row.rulesVersion)
	}
	if n := countEvents(f.pool.j.snapshot(), anEvUpsert); n != 1 {
		t.Fatalf("upsert выполнен %d раз (нужно 1 — повтор той же версии no-op, T-204)", n)
	}
	if n := countEvents(f.pool.j.snapshot(), anEvMark); n != 1 {
		t.Fatalf("штамп выполнен %d раз (нужно 1 — повтор той же версии no-op)", n)
	}
	if got := f.pool.counters[anTailBasket()]; got != -1 {
		t.Fatalf("корзина хвостов = %d, нужно −1 (декремент без дубля, T-204)", got)
	}
}

// TestAnalyzer_RulesReload — a set change (an SIGHUP without a
// restart): every delivery takes the current snapshot — the new version in the stamp and the findings.
func TestAnalyzer_RulesReload(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)

	// The first analysis: no signs (PHPSESSID is not Laravel) — the row
	// is not written, the 1001 stamp.
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("по старому набору срабатываний нет")
	}

	// SIGHUP: a set with an extra rule and a new version.
	reloaded := anSet(2002)
	reloaded.Rules = append(reloaded.Rules, rules.Rule{
		ID: "cookie_php_sessid", Target: rules.TargetCookieName, Match: "PHPSESSID", Weight: 0.4,
	})
	f.sets.swap(reloaded)

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult после релоада: %v", err)
	}
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("после релоада правило обязано срабатывать")
	}
	if !strings.Contains(finding.signals, "cookie_php_sessid") {
		t.Fatalf("сработало не новое правило: %s", finding.signals)
	}
	if finding.rulesVersion != 2002 {
		t.Fatalf("rules_version findings = %d, нужно 2002 (актуальный набор)", finding.rulesVersion)
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion == nil || *row.rulesVersion != 2002 {
		t.Fatalf("rules_version строки = %v, нужно 2002", row.rulesVersion)
	}
}

// TestAnalyzer_Errors — a failure of each step: a delivery error (no
// XACK, a repeated delivery), the state rolls back; want is the
// exact wrap substring (text pins against the return-cut mutants, T-M10).
func TestAnalyzer_Errors(t *testing.T) {
	seed := func(t *testing.T) *anFixture {
		f := newANFixture(t, anSet(1001), Options{})
		hash := anTestHash(7)
		f.store.blocks[hash] = anPageHTML()
		f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
		return f
	}
	cases := []struct {
		name string
		inj  func(f *anFixture)
		want string
	}{
		{"чтение строки", func(f *anFixture) { f.pool.queryErr = errors.New("select boom") },
			"analyzer: чтение строки результата"},
		{"чтение строки (Scan)", func(f *anFixture) { f.pool.resultScanErr = errors.New("scan boom") },
			"analyzer: чтение строки результата"},
		{"чтение строки (Err строк)", func(f *anFixture) {
			f.pool.resultRowsErr = errors.New("rows boom")
			delete(f.pool.results, anKey) // the rows Err is checked after an empty Next
		}, "analyzer: чтение строки результата"},
		{"чтение контента", func(f *anFixture) { f.store.getErr = errors.New("disk boom") }, ""},
		{"открытие транзакции", func(f *anFixture) { f.pool.beginErr = errors.New("begin boom") }, ""},
		{"штамп rules_version", func(f *anFixture) { f.pool.markErr = errors.New("mark boom") }, ""},
		{"корзина хвостов переанализа", func(f *anFixture) { f.pool.consumeErr = errors.New("consume boom") },
			"корзина хвостов переанализа: consume boom"},
		{"запись findings", func(f *anFixture) { f.pool.upsertErr = errors.New("upsert boom") },
			"запись findings: upsert boom"},
		{"удаление findings", func(f *anFixture) {
			f.pool.upsertErr = errors.New("не должен вызываться")
			f.pool.deleteErr = errors.New("delete boom")
			f.sets.swap(&rules.Set{Rules: []rules.Rule{anSet(1).Rules[0]}, Version: 1})
			f.pool.seedResult(anKey, nil, anCookies(t, "PHPSESSID"), nil) // not a single hit
		}, ""},
		{"фиксация", func(f *anFixture) { f.pool.commitErr = errors.New("commit boom") }, ""},
		{"битый jsonb заголовков", func(f *anFixture) {
			bad := "{не-json"
			f.pool.results[anKey] = &anResultRow{headers: &bad}
		}, ""},
		{"битый jsonb cookies", func(f *anFixture) {
			bad := "[не-json"
			f.pool.results[anKey] = &anResultRow{cookies: &bad}
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := seed(t)
			tc.inj(f)
			_, err := f.w.ProcessResult(context.Background(), anTask)
			if err == nil {
				t.Fatal("ожидана ошибка доставки")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ошибка = %v, нужна подстрока %q", err, tc.want)
			}
			// The state rolled back: no findings, no stamp.
			if _, ok := anFindingSnapshot(f.pool, anKey); ok {
				t.Fatal("findings записаны при сбое")
			}
			if row, ok := anResultSnapshot(f.pool, anKey); ok && row.rulesVersion != nil {
				t.Fatal("штамп поставлен при сбое")
			}
		})
	}
}

// TestAnalyzer_UpsertRowsAffected — writing the findings must affect
// exactly one row (the PK): a different bad command tag is an error
// with the row count (the return-cut mutant, T-M10).
func TestAnalyzer_UpsertRowsAffected(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
	f.pool.upsertRowsAff = 2

	_, err := f.w.ProcessResult(context.Background(), anTask)
	if err == nil || !strings.Contains(err.Error(), "запись findings: затронуто 2 строк") {
		t.Fatalf("ошибка = %v, нужна «запись findings: затронуто 2 строк»", err)
	}
	if f.pool.j.indexOf(anEvCommit) != -1 {
		t.Fatal("транзакция не должна фиксироваться при нестандартном RowsAffected")
	}
}

// TestAnalyzer_NaNWeightNotRecorded — an evidence with a NaN weight
// is not serialized into jsonb: the error is wrapped with the domain
// name, nothing is written and the transaction is not opened (the
// marshalSignals wrap return-cut mutant, T-M10).
func TestAnalyzer_NaNWeightNotRecorded(t *testing.T) {
	f := newANFixture(t, anNaNSet(3003), Options{})
	f.pool.seedResult(anKey, nil, anCookies(t, "XSRF-TOKEN"), nil)

	_, err := f.w.ProcessResult(context.Background(), anTask)
	if err == nil || !strings.Contains(err.Error(), "analyzer: laravel.example:") ||
		!strings.Contains(err.Error(), "json: unsupported value") {
		t.Fatalf("ошибка = %v, нужна обёртка имени домена и причина NaN", err)
	}
	if f.pool.j.indexOf(anEvBegin) != -1 {
		t.Fatal("транзакция не открывается до успешной сериализации signals")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны при несериализуемых signals")
	}
}

// TestAnalyzer_CtxCancelled — a context cancellation interrupts the
// processing (a graceful shutdown): an error without a write.
func TestAnalyzer_CtxCancelled(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.w.ProcessResult(ctx, anTask); err == nil {
		t.Fatal("отменённый ctx — ошибка")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны при отменённом ctx")
	}
}

// --- jsonb parsing ---------------------------------------------------------

func TestParseHeadersJSON(t *testing.T) {
	if h, err := parseHeadersJSON(nil); err != nil || h != nil {
		t.Fatalf("NULL: %v %v", h, err)
	}
	empty := ""
	if h, err := parseHeadersJSON(&empty); err != nil || h != nil {
		t.Fatalf("пусто: %v %v", h, err)
	}
	in := `{"X-Laravel": ["yes"], "Set-Cookie": ["a=1", "b=2"]}`
	h, err := parseHeadersJSON(&in)
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if len(h) != 2 || len(h["Set-Cookie"]) != 2 || h["X-Laravel"][0] != "yes" {
		t.Fatalf("значения: %v", h)
	}
	bad := "{"
	if _, err := parseHeadersJSON(&bad); err == nil {
		t.Fatal("битый JSON — ошибка")
	}
}

func TestParseCookiesJSON(t *testing.T) {
	if c, err := parseCookiesJSON(nil); err != nil || c != nil {
		t.Fatalf("NULL: %v %v", c, err)
	}
	// An empty string and an empty array — no cookies: nil without
	// parsing and without an empty slice (the "*s == \"\"" and "len(raw) == 0" mutants, T-M10).
	empty := ""
	if c, err := parseCookiesJSON(&empty); err != nil || c != nil {
		t.Fatalf("пусто: %v %v", c, err)
	}
	arr := "[]"
	if c, err := parseCookiesJSON(&arr); err != nil || c != nil {
		t.Fatalf("пустой массив: %v %v", c, err)
	}
	in := `[{"name":"a","value":"1","path":"/","http_only":true}]`
	c, err := parseCookiesJSON(&in)
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if len(c) != 1 || c[0].Name != "a" || c[0].Value != "1" {
		t.Fatalf("cookies: %v", c)
	}
	bad := "["
	if _, err := parseCookiesJSON(&bad); err == nil {
		t.Fatal("битый JSON — ошибка")
	}
}

func TestMarshalSignals(t *testing.T) {
	if s, err := marshalSignals(nil); err != nil || s != "" {
		t.Fatalf("пустой набор: %q %v", s, err)
	}
	in := []rules.Evidence{{Rule: "r", Weight: 0.5, Evidence: "e"}}
	s, err := marshalSignals(in)
	if err != nil {
		t.Fatalf("marshalSignals: %v", err)
	}
	want := `[{"rule":"r","weight":0.5,"evidence":"e"}]`
	if s != want {
		t.Fatalf("формат signals = %s, нужно %s (эскиз §6.1: нижний регистр)", s, want)
	}
	// A NaN weight is not serialized into jsonb — an error before any
	// write (the return-cut mutant, T-M10; the scenario is TestAnalyzer_NaNWeightNotRecorded).
	if _, err := marshalSignals([]rules.Evidence{{Rule: "r", Weight: math.NaN(), Evidence: "e"}}); err == nil {
		t.Fatal("NaN-вес — ошибка сериализации")
	}
}

// TestAnalyzerLogName — the name for logs: the domain name from the
// message, otherwise the row key (Name is optional, T-024; the return-cut mutant, T-M10).
func TestAnalyzerLogName(t *testing.T) {
	if got := analyzeLogName(queue.Result{Name: "laravel.example"}); got != "laravel.example" {
		t.Fatalf("analyzeLogName(с именем) = %q, нужно имя сообщения", got)
	}
	key := queue.Result{ZoneID: 3, DomainID: 42, SubdomainID: 1, Variant: 1}
	if got := analyzeLogName(key); got != "3/42/1/1" {
		t.Fatalf("analyzeLogName(без имени) = %q, нужно 3/42/1/1", got)
	}
}

// --- construction ----------------------------------------------------------

func TestNewValidation(t *testing.T) {
	pool := newANFakePool()
	store := &anFakeStore{j: pool.j, blocks: map[content.Hash][]byte{}}
	sets := &anFakeSets{set: anSet(1)}
	q := &anFakeQueue{j: pool.j}
	if _, err := New(nil, q, store, sets, Options{}); err == nil {
		t.Fatal("без БД: ожидана ошибка")
	}
	if _, err := New(pool, nil, store, sets, Options{}); err == nil {
		t.Fatal("без очереди: ожидана ошибка")
	}
	if _, err := New(pool, q, nil, sets, Options{}); err == nil {
		t.Fatal("без хранилища: ожидана ошибка")
	}
	if _, err := New(pool, q, store, nil, Options{}); err == nil {
		t.Fatal("без набора правил: ожидана ошибка")
	}
	if _, err := New(pool, q, store, sets, Options{}); err != nil {
		t.Fatalf("полный набор: %v", err)
	}
}

func TestOptionsDefaults(t *testing.T) {
	// The defaults are pinned by literals (T-M10): comparing against the
	// Default* constants would change with the mutant and mask it.
	o := Options{Consumer: "c"}.withDefaults()
	if o.Concurrency != 8 || o.Consume != 16 || o.Group != DefaultGroup {
		t.Fatalf("дефолты: %+v", o)
	}
	if DefaultErrorWait != 30*time.Second {
		t.Fatalf("DefaultErrorWait = %v, нужно 30s", DefaultErrorWait)
	}
	if o.Consumer != "c" {
		t.Fatalf("явный Consumer затёрт: %s", o.Consumer)
	}
	o = Options{}.withDefaults()
	if !strings.HasPrefix(o.Consumer, "analyzer-") {
		t.Fatalf("Consumer по умолчанию: %s", o.Consumer)
	}
	if o.Log != nil {
		t.Fatal("нулевой Log остаётся nil (log() подставляет Discard)")
	}
	if o.log() == nil {
		t.Fatal("log() обязан возвращать логгер")
	}
	// Unit values are valid and are not replaced by the defaults (the
	// "<= 0 → <= 1" bound, T-M10): Concurrency 1 and Consume 1 are working settings.
	o = Options{Concurrency: 1, Consume: 1}.withDefaults()
	if o.Concurrency != 1 || o.Consume != 1 {
		t.Fatalf("единичные значения: %+v, нужно 1/1", o)
	}
}

// --- processDelivery and the Run loop --------------------------------------

func TestProcessDelivery_PoisonMessage(t *testing.T) {
	f := newANFixture(t, anSet(1), Options{})
	del := queue.Delivery{Stream: queue.StreamAnalyze, ID: "bad", Payload: []byte("{не-json")}
	if err := f.w.processDelivery(context.Background(), del); err == nil {
		t.Fatal("ядовитая запись — ошибка (queue.dead через попытки, T-025)")
	}
}

// TestRun_Smoke — the loop: window Consume → Processing (analysis →
// Ack); after a window of empty reads the loop waits on ctx; everything is acknowledged.
func TestRun_Smoke(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	f.q.j.ackNotify = make(chan struct{}, 8)
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
	f.q.deliveries = []queue.Delivery{anDelivery("e1"), anDelivery("e2")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{Consumer: "test"}) }()

	// Both deliveries acknowledged after the DB write (Processing).
	for i := 0; i < 2; i++ {
		select {
		case <-f.q.j.ackNotify:
		case <-time.After(5 * time.Second):
			t.Fatal("доставка не подтверждена")
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
	if got := f.q.ackedSnapshot(); len(got) != 2 || got[0] == got[1] {
		t.Fatalf("подтверждены %v, нужно e1 и e2 (порядок параллелен — не важен)", got)
	}
	// The DB write strictly before the XACK: mark precedes ack. A second
	// delivery of the same key (a duplicate) is a no-op of the mark
	// version guard (T-204): the findings are not rewritten, the ack goes through.
	if f.pool.j.indexOf(anEvMark) > f.pool.j.indexOf(anEvAck) {
		t.Fatal("XACK раньше записи в БД")
	}
	if n := countEvents(f.pool.j.snapshot(), anEvUpsert); n != 1 {
		t.Fatalf("upsert выполнен %d раз, нужно 1 (дубль той же версии — no-op, T-204)", n)
	}
}

// TestRun_FailingDelivery — a processing error neither stops the loop
// nor acknowledges the delivery (it will come back via XAUTOCLAIM,
// T-025); the unacknowledged task Warn is written while ctx is alive (the log-cut mutant, T-M10).
func TestRun_FailingDelivery(t *testing.T) {
	var buf reanBuf
	f := newANFixture(t, anSet(1001), Options{})
	f.q.j.ackNotify = make(chan struct{}, 8)
	f.pool.queryErr = errors.New("select boom") // every delivery fails
	f.q.deliveries = []queue.Delivery{anDelivery("e1")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, f.w, Options{Consumer: "test", Log: slog.New(slog.NewTextHandler(&buf, nil))})
	}()

	// The loop is alive: after a failed delivery it reads the queue again.
	deadline := time.After(5 * time.Second)
	for f.q.consumesCount() < 2 {
		select {
		case <-deadline:
			t.Fatal("цикл не продолжил чтение после ошибки доставки")
		case <-time.After(time.Millisecond):
		}
	}
	reanWait(t, "Warn о неподтверждённой задаче", func() bool {
		return strings.Contains(buf.String(), "задача q:analyze не подтверждена")
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if got := f.q.ackedSnapshot(); len(got) != 0 {
		t.Fatalf("упавшая доставка подтверждена: %v", got)
	}
}

func TestRun_Validation(t *testing.T) {
	if err := Run(context.Background(), nil, Options{}); err == nil {
		t.Fatal("Run без воркера — ошибка")
	}
}

// TestRun_CtxCancelledBeforeConsume — a cancellation before the first
// read: a clean nil without trying to wait out the error pause (the
// graceful shutdown branch mutants, T-M10); the startup Info is captured by the buffer (the log-cut mutant).
func TestRun_CtxCancelledBeforeConsume(t *testing.T) {
	var buf reanBuf
	f := newANFixture(t, anSet(1), Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, f.w, Options{Consumer: "test", Log: slog.New(slog.NewTextHandler(&buf, nil))})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v, нужен nil при отменённом ctx", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился на отменённом ctx (зациклился без graceful shutdown)")
	}
	if !strings.Contains(buf.String(), "цикл analyzer") {
		t.Errorf("Info запуска не записан:\n%s", buf.String())
	}
}

// TestRun_ErrorWait — the DefaultErrorWait pause after a queue read
// error: no re-read before the pause (the wait-cut and graceful
// shutdown branch mutants both give a hot re-read loop, T-M10);
// the error Warn is captured by the buffer (the log-cut mutant).
func TestRun_ErrorWait(t *testing.T) {
	var buf reanBuf
	f := newANFixture(t, anSet(1), Options{})
	f.q.consumeErr = errors.New("redis ушёл")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, f.w, Options{Consumer: "test", Log: slog.New(slog.NewTextHandler(&buf, nil))})
	}()

	reanWait(t, "первое чтение", func() bool { return f.q.consumesCount() >= 1 })
	reanWait(t, "Warn об ошибке чтения", func() bool {
		return strings.Contains(buf.String(), "цикл analyzer: чтение q:analyze")
	})
	// A 30s pause: within a short window there must be no re-reads — a
	// wait or shutdown branch cut gives a hot loop.
	time.Sleep(150 * time.Millisecond)
	if n := f.q.consumesCount(); n != 1 {
		t.Fatalf("чтений очереди за паузу = %d, нужно 1 (пауза после ошибки)", n)
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
}

// countEvents counts how many times the event occurs in the journal.
func countEvents(events []anEventKind, k anEventKind) int {
	n := 0
	for _, e := range events {
		if e == k {
			n++
		}
	}
	return n
}

// --- the Run loop: the semaphore, unacknowledged logs, window stop, RecoverLoop

// TestRun_SemaphoreReleased — the delivery goroutine returns the
// semaphore slot (defer <-sem): with Concurrency 1 both window
// deliveries are processed sequentially and acknowledged (the
// defer <-sem cut mutant hangs the second one, T-M10).
func TestRun_SemaphoreReleased(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	f.q.j.ackNotify = make(chan struct{}, 8)
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
	f.q.deliveries = []queue.Delivery{anDelivery("e1"), anDelivery("e2")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{Consumer: "test", Concurrency: 1}) }()

	for i := 0; i < 2; i++ {
		select {
		case <-f.q.j.ackNotify:
		case <-time.After(5 * time.Second):
			t.Fatalf("доставка №%d не подтверждена (слот семафора не вернулся)", i+1)
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
	if got := f.q.ackedSnapshot(); len(got) != 2 {
		t.Fatalf("подтверждены %v, нужно обе доставки", got)
	}
}

// TestRun_NoWarnOnSuccess — successful deliveries with a live ctx do
// not write the "not acknowledged" Warn (the condition left-half cut mutant, T-M10).
func TestRun_NoWarnOnSuccess(t *testing.T) {
	var buf reanBuf
	f := newANFixture(t, anSet(1001), Options{})
	f.q.j.ackNotify = make(chan struct{}, 8)
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
	f.q.deliveries = []queue.Delivery{anDelivery("e1"), anDelivery("e2")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, f.w, Options{Consumer: "test", Log: slog.New(slog.NewTextHandler(&buf, nil))})
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-f.q.j.ackNotify:
		case <-time.After(5 * time.Second):
			t.Fatalf("доставка №%d не подтверждена", i+1)
		}
	}
	if strings.Contains(buf.String(), "не подтверждена") {
		t.Fatalf("успешные доставки пишут Warn:\n%s", buf.String())
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
}

// TestRun_NoWarnOnShutdown — a delivery error with a cancelled ctx is
// part of the stop, not an alarm: no Warn is written (the condition
// right-half ctx.Err() == nil cut mutant, T-M10). The processing is
// held in Begin until the cancellation: after the release Exec sees
// it and the delivery fails.
func TestRun_NoWarnOnShutdown(t *testing.T) {
	var buf reanBuf
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])

	started := make(chan struct{})
	gate := make(chan struct{})
	f.pool.mutateBeforeTx = func(p *anFakePool) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-gate
	}
	f.q.deliveries = []queue.Delivery{anDelivery("e1")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, f.w, Options{Consumer: "test", Log: slog.New(slog.NewTextHandler(&buf, nil))})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("доставка не дошла до транзакции")
	}
	cancel() // stopping: the delivery is in flight, an error is inevitable — but not an alarm
	close(gate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if strings.Contains(buf.String(), "не подтверждена") {
		t.Fatalf("ошибка остановки пишется Warn:\n%s", buf.String())
	}
}

// TestRun_WindowShutdown — a cancellation in the middle of a window:
// the started delivery finishes (wg.Wait), but the loop does not
// read the queue again — the post-check of ctx returns nil before
// the next Consume (the wg.Wait and return nil cut mutants both give a second read, T-M10).
func TestRun_WindowShutdown(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])

	started := make(chan struct{})
	gate := make(chan struct{})
	f.pool.mutateBeforeTx = func(p *anFakePool) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-gate
	}
	f.q.deliveries = []queue.Delivery{anDelivery("e1")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{Consumer: "test"}) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("доставка не дошла до транзакции")
	}
	cancel()    // the window is in flight: wg.Wait holds the loop
	close(gate) // the started delivery finishes (and fails on the cancelled ctx)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if n := f.q.consumesCount(); n != 1 {
		t.Fatalf("чтений очереди = %d, нужно 1 (повторное чтение после остановки окна)", n)
	}
}

// anClaimGate is a fake claimStreams (Reliab, T-025): XAutoClaim
// parks on the gate until the test releases it, then answers with an
// empty batch; XPendingExt is not called (no records). The entered
// signal marks the RecoverLoop entering the first claim.
type anClaimGate struct {
	entered chan struct{}
	gate    chan struct{}
}

func newANClaimGate() *anClaimGate {
	return &anClaimGate{entered: make(chan struct{}), gate: make(chan struct{})}
}

func (g *anClaimGate) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	select {
	case <-g.entered:
	default:
		close(g.entered)
	}
	<-g.gate
	cmd := redis.NewXAutoClaimCmd(ctx, "xautoclaim")
	cmd.SetVal(nil, "0-0")
	return cmd
}

func (g *anClaimGate) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return redis.NewXPendingExtCmd(ctx, "xpending")
}

// TestRun_WaitsRecoverLoop — Run waits for the background RecoverLoop
// before returning (defer <-recDone): the claim parks on the gate —
// after the cancellation Run does not return while the gate is closed
// (the wait-cut mutant, T-M10).
func TestRun_WaitsRecoverLoop(t *testing.T) {
	f := newANFixture(t, anSet(1), Options{})
	gate := newANClaimGate()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, f.w, Options{Consumer: "test",
			Recover: queue.NewReliab(gate, f.q, queue.ReliabOptions{})})
	}()

	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RecoverLoop не дошёл до клейма")
	}
	cancel()
	// The claim holds Run: a return before the release is the no-wait
	// mutant. A 100 ms window gives the mutant time to return.
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Run вернулся раньше RecoverLoop: %v", err)
	default:
	}
	close(gate.gate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не дождался RecoverLoop после освобождения клейма")
	}
}

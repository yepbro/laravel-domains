// Unit tests of recording the DNS episode result and of the dns-worker
// loop (T-039; spec 5.4 items 4–5, §7, 3.4; the retry_mature bucket
// increment by a separate statement after the write — T-267; the 40P01
// retry of the write points — T-268) on fake pool, queue and resolver —
// no DB, Redis or network. The status decision (DecideEpisode), the
// exact SQL texts and the write parameters, the "publish q:http → write
// done" order, the wildcard branch, the errors/cancellation, the
// idempotent batch repeat (the currency filter) and the smoke test of
// the Run loop (read → episodes → XACK). The behavior against live
// Redis/PostgreSQL/unbound — integration tests (T-032, T-040).
package dnsworker

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/miekg/dns"

	"crawler/internal/db"
	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- the common protocol of the pool and queue events ---------------------

// resultEventKind — the type of an event of the common protocol (the
// order of the "publication → write" operations is checked by the event
// indexes).
type resultEventKind string

const (
	resultEvQuerySubs resultEventKind = "query:subdomains"
	resultEvQueryPend resultEventKind = "query:pending"
	resultEvExec      resultEventKind = "exec"
	resultEvPublish   resultEventKind = "publish"
	resultEvAck       resultEventKind = "ack"
	resultEvConsume   resultEventKind = "consume"
)

// resultEvent — a protocol record: the operation, the detail (the SQL/the
// stream) and the arguments.
type resultEvent struct {
	kind   resultEventKind
	detail string
	args   []any
}

// resultJournal — a thread-safe event protocol (the Run loop works in a
// goroutine, the episodes run in parallel).
type resultJournal struct {
	mu     sync.Mutex
	events []resultEvent
	// ackNotify — a non-blocking signal per Ack (the smoke sync).
	ackNotify chan struct{}
}

func (j *resultJournal) record(k resultEventKind, detail string, args ...any) {
	j.mu.Lock()
	j.events = append(j.events, resultEvent{kind: k, detail: detail, args: args})
	j.mu.Unlock()
}

func (j *resultJournal) snapshot() []resultEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]resultEvent, len(j.events))
	copy(out, j.events)
	return out
}

// indexOf returns the index of the first event of the kind (0-based) or
// −1.
func (j *resultJournal) indexOf(k resultEventKind, detail string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.events {
		if e.kind == k && (detail == "" || e.detail == detail) {
			return i
		}
	}
	return -1
}

func (j *resultJournal) count(k resultEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, e := range j.events {
		if e.kind == k {
			n++
		}
	}
	return n
}

func (j *resultJournal) signalAck() {
	if j.ackNotify == nil {
		return
	}
	select {
	case j.ackNotify <- struct{}{}:
	default:
	}
}

// --- the fake pgx pool ------------------------------------------------------

// resultFakeRows — pgx.Rows with prepared rows; a positional Scan (the
// enqueueFakeRows style).
type resultFakeRows struct {
	rows    [][]any
	i       int
	closed  bool
	err     error
	scanErr error
}

func resultFakeRowsOf(rows ...[]any) *resultFakeRows {
	return &resultFakeRows{rows: rows}
}

func (r *resultFakeRows) Close() { r.closed = true }

func (r *resultFakeRows) Err() error { return r.err }

func (r *resultFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *resultFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *resultFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *resultFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("resultFakeRows: Scan without Next")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("resultFakeRows: %d destinations, %d columns", len(dest), len(row))
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

func (r *resultFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *resultFakeRows) RawValues() [][]byte    { return nil }

// Conn and TypeMap — the part of pgx.Rows not used by the positional
// Scan.
func (r *resultFakeRows) Conn() *pgx.Conn      { return nil }
func (r *resultFakeRows) TypeMap() *pgtype.Map { return nil }

// resultFakePool — a db.DB without a DB: Query distinguishes the
// subdomains reference table from the batch filter by the SQL text, and
// Exec returns the prepared results in order (the default — UPDATE 1).
// Everything is written into the common protocol.
type resultFakePool struct {
	j *resultJournal

	subdomains []model.Subdomain // the reference table answer
	subsErr    error             // the reference table read error

	// pending — the prepared rows of the filter (zone, id, flags); nil —
	// "the mirror": all the batch keys are current, with a clean
	// http_published bit.
	pending     [][3]any
	pendingErr  error
	pendScanErr error

	execTags []pgconn.CommandTag // the result of each Exec, in order
	execErrs []error             // the Exec errors, in order
	execErr  error               // a sticky error of all Exec (if set)

	mu      sync.Mutex
	execs   []resultExec
	queries int
}

// resultExec — a recorded Exec call.
type resultExec struct {
	sql  string
	args []any
}

var _ db.DB = (*resultFakePool)(nil)

func (p *resultFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("resultFakePool: CopyFrom must not be called")
}

// recordExec — the accounting of a single write statement (T-178: the
// result write statements are CTEs with an outer SELECT count, they go
// through Query; the protocol and the tag/error preparations are common
// with Exec): returns the number of transitioned rows (an "UPDATE N"
// tag).
func (p *resultFakePool) recordExec(sql string, args ...any) (int64, error) {
	p.mu.Lock()
	p.execs = append(p.execs, resultExec{sql: sql, args: args})
	i := len(p.execs) - 1
	p.mu.Unlock()
	p.j.record(resultEvExec, sql, args...)

	if p.execErr != nil {
		return 0, p.execErr
	}
	if i < len(p.execErrs) && p.execErrs[i] != nil {
		return 0, p.execErrs[i]
	}
	if i < len(p.execTags) {
		var n int
		if _, err := fmt.Sscanf(p.execTags[i].String(), "UPDATE %d", &n); err == nil {
			return int64(n), nil
		}
	}
	return 1, nil
}

func (p *resultFakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	n, err := p.recordExec(sql, args...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
}

func (p *resultFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch sql {
	case subdomainsSelectSQL:
		p.j.record(resultEvQuerySubs, sql)
		if p.subsErr != nil {
			return nil, p.subsErr
		}
		rows := make([][]any, len(p.subdomains))
		for i, s := range p.subdomains {
			rows[i] = []any{s.ID, s.Label, s.Enabled}
		}
		return resultFakeRowsOf(rows...), nil
	case resultUpdateStatusSQL, resultUpdateKeepStatusSQL, resultInvalidSQL:
		// The result write statements (T-178): CTEs with an outer SELECT
		// count(*) — the result is one row.
		n, err := p.recordExec(sql, args...)
		if err != nil {
			return nil, err
		}
		return resultFakeRowsOf([]any{n}), nil
	case resultPendingSQL:
		p.j.record(resultEvQueryPend, sql, args...)
		p.mu.Lock()
		p.queries++
		p.mu.Unlock()
		if p.pendingErr != nil {
			return nil, p.pendingErr
		}
		var rows3 [][3]any
		if p.pending != nil {
			rows3 = p.pending
		} else {
			// The mirror: all the batch keys are current (args:
			// []int2, []int8), the http_published bit is clean (the
			// domain has not published yet).
			zones := args[0].([]int16)
			ids := args[1].([]int64)
			for i := range zones {
				rows3 = append(rows3, [3]any{zones[i], ids[i], int16(0)})
			}
		}
		rows := make([][]any, len(rows3))
		for i, pr := range rows3 {
			rows[i] = []any{pr[0], pr[1], pr[2]}
		}
		out := resultFakeRowsOf(rows...)
		out.scanErr = p.pendScanErr
		return out, nil
	default:
		return nil, fmt.Errorf("resultFakePool: unexpected SQL %q", sql)
	}
}

// resultPoolExecs — a snapshot of the Exec calls.
func (p *resultFakePool) resultPoolExecs() []resultExec {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]resultExec, len(p.execs))
	copy(out, p.execs)
	return out
}

// --- the fake queue ---------------------------------------------------------

// resultFakeQueue — a queue.Queue: Publish only types and remembers the
// values — the queue.HTTPTask.Validate contract is not repeated;
// Consume hands out the prepared deliveries one by one and, once
// exhausted, waits for the ctx cancellation (a real XREADGROUP blocks);
// Ack is recorded.
type resultFakeQueue struct {
	j *resultJournal

	deliveries []queue.Delivery
	consumeErr error // a sticky error of the first read

	publishErrs []error // the Publish errors, in order

	mu        sync.Mutex
	publishes []resultPublish
	acked     []resultAck
}

// resultPublish — a recorded publication.
type resultPublish struct {
	stream string
	task   queue.HTTPTask
	msg    queue.Message
}

// resultAck — a recorded acknowledgment.
type resultAck struct {
	stream string
	group  string
	ids    []string
}

var _ queue.Queue = (*resultFakeQueue)(nil)

func (q *resultFakeQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	q.mu.Lock()
	i := len(q.publishes)
	var task queue.HTTPTask
	if t, ok := msg.(queue.HTTPTask); ok {
		task = t
	}
	q.publishes = append(q.publishes, resultPublish{stream: stream, task: task, msg: msg})
	q.mu.Unlock()
	q.j.record(resultEvPublish, stream, msg)

	var err error
	if i < len(q.publishErrs) {
		err = q.publishErrs[i]
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("entry-publish-%d", i+1), nil
}

func (q *resultFakeQueue) Consume(ctx context.Context, stream, group, consumer string, n int) ([]queue.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.consumeErr != nil {
		err := q.consumeErr
		q.consumeErr = nil // one-shot: the loop must survive and continue
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
	q.j.record(resultEvConsume, stream, group, consumer, len(out))
	if len(out) > 0 {
		return out, nil
	}
	// An empty read: the real queue blocks in XREADGROUP — the fake
	// waits for the cancellation (the smoke loop parks until the end of
	// the test).
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *resultFakeQueue) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked = append(q.acked, resultAck{stream: stream, group: group, ids: append([]string(nil), ids...)})
	q.mu.Unlock()
	for _, id := range ids {
		q.j.record(resultEvAck, stream, id)
	}
	q.j.signalAck()
	return nil
}

func (q *resultFakeQueue) resultPublishes() []resultPublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]resultPublish, len(q.publishes))
	copy(out, q.publishes)
	return out
}

func (q *resultFakeQueue) resultAcks() []resultAck {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]resultAck, len(q.acked))
	copy(out, q.acked)
	return out
}

// --- the fake resolver --------------------------------------------------------

// resultAnswer — a prepared answer to a question: a Result with a code
// and records, or an exchange error (transport after the dnsclient
// retries).
type resultAnswer struct {
	rcode dnsclient.RCode
	an    []dns.RR
	err   error
}

// resultReply — an answer with a code and an answer section.
func resultReply(rcode dnsclient.RCode, an ...dns.RR) resultAnswer {
	return resultAnswer{rcode: rcode, an: an}
}

// errResultNet — a transport error (not a ctx cancellation).
var errResultNet = errors.New("fake network error")

// resultQKey — the question key: the canonical name (lowercase, FQDN)
// and the type.
type resultQKey struct {
	name string
	typ  uint16
}

// resultFakeResolver — a dnsclient.Resolver answering by the name and
// type of the question (unlike the scenario one, which answers by
// order — an episode combines the apex, the subdomains and the wildcard
// detection). Wildcard-like records are looked up by the zone suffix:
// the T-038 detection question carries a random label.
type resultFakeResolver struct {
	mu      sync.Mutex
	answers map[resultQKey]resultAnswer
	wild    map[string]resultAnswer // a zone suffix → a <random>.<zone> answer
	calls   []dnsclient.Question
	onCall  func(n int) // the test pulls the ctx cancellation between the exchanges
}

var _ dnsclient.Resolver = (*resultFakeResolver)(nil)

func resultNewResolver() *resultFakeResolver {
	return &resultFakeResolver{
		answers: make(map[resultQKey]resultAnswer),
		wild:    make(map[string]resultAnswer),
	}
}

// setAnswer sets the exact answer to a question (the name is
// canonicalized).
func (f *resultFakeResolver) setAnswer(name string, typ uint16, a resultAnswer) {
	key := resultQKey{name: strings.ToLower(dns.Fqdn(name)), typ: typ}
	f.answers[key] = a
}

// setWild sets the wildcard detection answer for a zone (suffix — the
// domain name, "example.com" for example).
func (f *resultFakeResolver) setWild(zone string, a resultAnswer) {
	f.wild[strings.ToLower(dns.Fqdn(zone))] = a
}

func (f *resultFakeResolver) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, q)
	n := len(f.calls)
	f.mu.Unlock()
	if f.onCall != nil {
		f.onCall(n)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := resultQKey{name: strings.ToLower(dns.Fqdn(q.Name)), typ: q.Type}
	f.mu.Lock()
	defer f.mu.Unlock()
	if a, ok := f.answers[key]; ok {
		return f.toResult(a)
	}
	for suffix, a := range f.wild {
		if key.name != suffix && strings.HasSuffix(key.name, "."+suffix) {
			return f.toResult(a)
		}
	}
	return nil, fmt.Errorf("resultFakeResolver: no script for %s %s", dns.Type(q.Type), key.name)
}

func (f *resultFakeResolver) toResult(a resultAnswer) (*dnsclient.Result, error) {
	if a.err != nil {
		return nil, a.err
	}
	return &dnsclient.Result{RCode: a.rcode, Answer: a.an}, nil
}

func (f *resultFakeResolver) resultCalls() []dnsclient.Question {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]dnsclient.Question(nil), f.calls...)
}

// --- scenario helpers --------------------------------------------------------

// resultSubs — the reference table: the apex (id 0), www (id 1), admin
// (id 2).
func resultSubs() []model.Subdomain {
	return []model.Subdomain{
		{ID: 0, Label: "", Enabled: true},
		{ID: 1, Label: "www", Enabled: true},
		{ID: 2, Label: "admin", Enabled: true},
	}
}

// resultSetDelegated — a domain with addresses: an apex A, a www
// CNAME→cdn with an A, an admin NXDOMAIN, a wildcard detection —
// NXDOMAIN (no wildcard).
func resultSetDelegated(f *resultFakeResolver, domain string) {
	f.setAnswer(domain, dns.TypeA,
		resultReply(dnsclient.RCodeNOERROR, apexARR(domain+".", "192.0.2.10")))
	f.setAnswer("www."+domain, dns.TypeA, resultReply(dnsclient.RCodeNOERROR,
		subdomCNAME("www."+domain+".", "cdn."+domain+"."),
		apexARR("cdn."+domain+".", "203.0.113.5")))
	f.setAnswer("admin."+domain, dns.TypeA, resultReply(dnsclient.RCodeNXDOMAIN))
	f.setWild(domain, resultReply(dnsclient.RCodeNXDOMAIN))
}

// resultSetNXDOMAIN — a non-delegated domain (an apex NXDOMAIN).
func resultSetNXDOMAIN(f *resultFakeResolver, domain string) {
	f.setAnswer(domain, dns.TypeA, resultReply(dnsclient.RCodeNXDOMAIN))
}

// resultSetWildcardZone — a domain under a wildcard: an apex A, a www
// NODATA (an NS confirms), an admin — the wildcard answer's addresses.
func resultSetWildcardZone(f *resultFakeResolver, domain string) {
	f.setAnswer(domain, dns.TypeA,
		resultReply(dnsclient.RCodeNOERROR, apexARR(domain+".", "192.0.2.1")))
	f.setAnswer(domain, dns.TypeNS, resultReply(dnsclient.RCodeNOERROR,
		apexNSRR(domain+".", "ns1."+domain+".")))
	f.setAnswer("www."+domain, dns.TypeA, resultReply(dnsclient.RCodeNOERROR))
	f.setAnswer("admin."+domain, dns.TypeA,
		resultReply(dnsclient.RCodeNOERROR, apexARR("admin."+domain+".", "198.51.100.7")))
	f.setWild(domain, resultReply(dnsclient.RCodeNOERROR, apexARR("wild."+domain+".", "198.51.100.7")))
}

// resultDelivery — a DNSBatch delivery with a task_id and domains.
func resultDelivery(id string, taskID string, domains ...queue.DNSDomain) queue.Delivery {
	payload, err := json.Marshal(queue.DNSBatch{TaskID: taskID, Domains: domains})
	if err != nil {
		panic(fmt.Sprintf("resultDelivery: marshaling DNSBatch: %v", err))
	}
	return queue.Delivery{Stream: queue.StreamDNS, ID: id, Payload: payload}
}

// resultWaitAck — waits for the first Ack in the protocol (the smoke
// sync).
func resultWaitAck(j *resultJournal, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if j.count(resultEvAck) > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// --- SQL contracts ------------------------------------------------------------

// TestResultSQLContract fixes the exact texts of the write statements
// (the dispatch_test style: the expectation is assembled independently
// of the production code; the literals are the schema codes of
// spec 6.1/6.2: done = 2, temp_error = 5, deleted = 6; the
// http_published bit = 4, the clear mask is signed ~4 = −5, T-108).
func TestResultSQLContract(t *testing.T) {
	// The result write statements — CTEs (T-178): a FOR UPDATE pre-read
	// of the old state, the transition, the zone counter deltas by the
	// old state; the outer SELECT — the count of the transitioned rows.
	// The statement is strictly "domains → zones" (T-267): the retry
	// maturity bucket increment (T-202) is moved out into a separate
	// autocommit statement, resultMatureIncSQL — the unreferenced DML
	// CTEs of the zones and of the counter in one statement execute in
	// an unpredictable order (the Nov 07 incident — the edge "counters →
	// zones", runbooks.md §2.8).
	const cteTail = "FOR UPDATE), " +
		"upd AS (UPDATE domains AS t "
	const cteShift = "FROM s WHERE t.zone_id = s.zone_id AND t.id = s.id " +
		"RETURNING s.check_state, t.zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_pending = z.domains_pending - c.n_pending, " +
		"domains_queued = z.domains_queued - c.n_queued, " +
		"domains_checked = z.domains_checked + c.n " +
		"FROM (SELECT zone_id, count(*) AS n, count(*) FILTER (WHERE check_state = 0) AS n_pending, " +
		"count(*) FILTER (WHERE check_state = 1) AS n_queued FROM upd GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id)"
	const cteEnd = " SELECT count(*) FROM upd"
	const cteHead = "WITH s AS (" +
		"SELECT zone_id, id, check_state FROM domains " +
		"WHERE zone_id = $1::int2 AND id = $2::int8 AND status <> 6 AND check_state <> 2 "

	wantStatus := cteHead + cteTail +
		"SET dns_mask = $3::int8, status = $4::int2, " +
		"temp_attempts = CASE WHEN $4::int2 = 5 THEN t.temp_attempts ELSE 0 END, " +
		"flags = (t.flags & -5)::int2, " +
		"check_state = 2, queued_at = NULL, last_checked_at = now() " +
		cteShift + cteEnd
	if resultUpdateStatusSQL != wantStatus {
		t.Errorf("resultUpdateStatusSQL = %q,\nwant %q", resultUpdateStatusSQL, wantStatus)
	}
	if strings.Contains(resultUpdateStatusSQL, "crawler_counters") {
		t.Errorf("resultUpdateStatusSQL writes counters in the zones statement (T-267 — moved to resultMatureIncSQL):\n%s",
			resultUpdateStatusSQL)
	}

	// The maturity bucket increment — a separate autocommit statement
	// after the write (T-267): the upsert pattern of the HTTP stage's
	// recordRetryMatureBumpSQL, the delta — the literal 1 (one domain
	// per call), the current_date key is computed by the DB (the time
	// zone is aligned with the write's last_checked_at).
	wantMature := "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('retry_mature:' || to_char(current_date, 'YYYY-MM-DD'), 1, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"
	if resultMatureIncSQL != wantMature {
		t.Errorf("resultMatureIncSQL = %q,\nwant %q", resultMatureIncSQL, wantMature)
	}

	wantKeep := cteHead + cteTail +
		"SET dns_mask = $3::int8, temp_attempts = 0, " +
		"flags = (t.flags & -5)::int2, " +
		"check_state = 2, queued_at = NULL, last_checked_at = now() " +
		cteShift + cteEnd
	if resultUpdateKeepStatusSQL != wantKeep {
		t.Errorf("resultUpdateKeepStatusSQL = %q,\nwant %q", resultUpdateKeepStatusSQL, wantKeep)
	}

	wantInvalid := cteHead + cteTail +
		"SET status = 7::int2, temp_attempts = 0, " +
		"flags = (t.flags & -5)::int2, " +
		"check_state = 2, queued_at = NULL, last_checked_at = now() " +
		cteShift + cteEnd
	if resultInvalidSQL != wantInvalid {
		t.Errorf("resultInvalidSQL = %q,\nwant %q", resultInvalidSQL, wantInvalid)
	}

	wantMark := "UPDATE domains AS t " +
		"SET flags = (t.flags | 4)::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> 6 AND t.check_state <> 2"
	if resultMarkPublishedSQL != wantMark {
		t.Errorf("resultMarkPublishedSQL = %q,\nwant %q", resultMarkPublishedSQL, wantMark)
	}

	wantPending := "SELECT u.zone_id, u.id, d.flags " +
		"FROM unnest($1::int2[], $2::int8[]) WITH ORDINALITY AS u(zone_id, id, ord) " +
		"JOIN domains AS d ON d.zone_id = u.zone_id AND d.id = u.id " +
		"WHERE d.status <> 6 AND d.check_state <> 2 " +
		"ORDER BY u.ord"
	if resultPendingSQL != wantPending {
		t.Errorf("resultPendingSQL = %q,\nwant %q", resultPendingSQL, wantPending)
	}

	if want := "SELECT id, label, enabled FROM subdomains ORDER BY id"; subdomainsSelectSQL != want {
		t.Errorf("subdomainsSelectSQL = %q, want %q", subdomainsSelectSQL, want)
	}

	// The text literals must follow the model codes: when the codes/bits
	// change, the test catches the desync (the flags bits — spec 6.1
	// "…").
	if int16(model.FlagHTTPPublished) != 4 || int16(^int16(model.FlagHTTPPublished)) != -5 {
		t.Errorf("the http_published bit = %d, the clear mask = %d: update the wantStatus/wantKeep/wantMark literals",
			int16(model.FlagHTTPPublished), int16(^int16(model.FlagHTTPPublished)))
	}
}

// --- DecideEpisode ------------------------------------------------------------

func TestResultDecideEpisode(t *testing.T) {
	delegated := &ApexResult{Status: ApexDelegated, Addresses: apexAddrs("192.0.2.10")}
	noAddress := &ApexResult{Status: ApexDelegatedNoAddress, NSQueried: true}

	cases := []struct {
		name     string
		apex     *ApexResult
		rep      *SubdomReport
		wild     WildcardResult
		wantSet  bool
		wantSt   model.Status
		wantMask model.DNSMask
		wantSubs []queue.HTTPSubdomain
		wantTemp bool
		wantErr  string
	}{
		{
			name:    "apex nxdomain — not_delegated, subdomains not checked",
			apex:    &ApexResult{Status: ApexNotDelegated},
			wantSet: true,
			wantSt:  model.StatusNotDelegated,
		},
		{
			name:     "apex servfail — temp_error",
			apex:     &ApexResult{Status: ApexTempError},
			wantSet:  true,
			wantSt:   model.StatusTempError,
			wantTemp: true,
		},
		{
			name: "addresses present — a q:http task, no status written",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0, 1), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www", A: apexAddrs("203.0.113.5"), Chain: []string{"cdn.example.net."}},
				{ID: 2, Label: "admin", RCode: dnsclient.RCodeNXDOMAIN},
			}},
			wantMask: mustMask(t, 0, 1),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
				{SubdomainID: 1, Label: "www", Addrs: []string{"203.0.113.5"}, CNAME: "cdn.example.net"},
			},
		},
		{
			name: "a wildcard excludes from q:http but not from dns_mask",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0, 1, 2), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www", A: apexAddrs("203.0.113.5")},
				{ID: 2, Label: "admin", A: apexAddrs("198.51.100.7")},
			}},
			wild:     WildcardResult{Mask: mustMask(t, 2)},
			wantMask: mustMask(t, 0, 1, 2),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
				{SubdomainID: 1, Label: "www", Addrs: []string{"203.0.113.5"}},
			},
		},
		{
			name: "only wildcard names resolve — delegated_no_address, an honest mask",
			apex: noAddress,
			rep: &SubdomReport{Mask: mustMask(t, 2), Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www", RCode: dnsclient.RCodeNOERROR},
				{ID: 2, Label: "admin", A: apexAddrs("198.51.100.7")},
			}},
			wild:     WildcardResult{Mask: mustMask(t, 2)},
			wantSet:  true,
			wantSt:   model.StatusDelegatedNoAddress,
			wantMask: mustMask(t, 2),
		},
		{
			name: "no addresses without failures — delegated_no_address",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www", RCode: dnsclient.RCodeNXDOMAIN},
			}},
			wantSet: true,
			wantSt:  model.StatusDelegatedNoAddress,
		},
		{
			name: "no addresses, a subdomain servfail — temp_error",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www", RCode: dnsclient.RCodeSERVFAIL},
			}},
			wantSet:  true,
			wantSt:   model.StatusTempError,
			wantTemp: true,
		},
		{
			name: "no addresses, a subdomain transport — temp_error",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www", Err: errResultNet},
			}},
			wantSet:  true,
			wantSt:   model.StatusTempError,
			wantTemp: true,
		},
		{
			// DNS-02: a zone defect is not a resolver failure: the CNAME
			// loop at www is permanent, the T-031 repeats will change
			// nothing — "no addresses".
			name: "no addresses, a subdomain CNAME loop — delegated_no_address",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www",
					Err: fmt.Errorf("%w: www.example.net. → cdn.example.net.", ErrSubdomCNAMELoop)},
			}},
			wantSet: true,
			wantSt:  model.StatusDelegatedNoAddress,
		},
		{
			name: "no addresses, a subdomain CNAME depth — delegated_no_address",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www",
					Err: fmt.Errorf("%w: 10 hops from www.example.net.", ErrSubdomCNAMEDepth)},
			}},
			wantSet: true,
			wantSt:  model.StatusDelegatedNoAddress,
		},
		{
			// A transport failure next to a zone defect is still temp:
			// "no addresses" is not derived from a silent resolver.
			name: "no addresses, a CNAME loop and transport — temp_error",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "",
					Err: fmt.Errorf("%w: a.example.net. → b.example.net.", ErrSubdomCNAMELoop)},
				{ID: 1, Label: "www", Err: errResultNet},
			}},
			wantSet:  true,
			wantSt:   model.StatusTempError,
			wantTemp: true,
		},
		{
			name: "addresses present despite one subdomain failing — a partial outcome",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www", Err: errResultNet},
			}},
			wantMask: mustMask(t, 0),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
			},
			wantTemp: true,
		},
		{
			// DNS-02: a zone defect at a subdomain without addresses does
			// not spoil the outcome of the one that found addresses and
			// does not mark the episode temp.
			name: "addresses present, another subdomain has a CNAME loop — no temp",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www",
					Err: fmt.Errorf("%w: www.example.net. → cdn.example.net.", ErrSubdomCNAMELoop)},
			}},
			wantMask: mustMask(t, 0),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
			},
		},
		{
			name: "a subdomain nxdomain — not a failure (RFC 8020)",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www", RCode: dnsclient.RCodeNXDOMAIN},
			}},
			wantMask: mustMask(t, 0),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
			},
		},
		{
			name:    "no apex result — an error",
			apex:    nil,
			wantErr: "no apex result",
		},
		{
			name:    "the apex delegated, no subdomain report — an error",
			apex:    delegated,
			rep:     nil,
			wantErr: "no subdomain report",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := DecideEpisode(tc.apex, tc.rep, tc.wild)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecideEpisode: %v", err)
			}
			if res.SetStatus != tc.wantSet {
				t.Errorf("SetStatus = %v, want %v", res.SetStatus, tc.wantSet)
			}
			if res.SetStatus && res.Status != tc.wantSt {
				t.Errorf("Status = %v, want %v", res.Status, tc.wantSt)
			}
			if res.Mask != tc.wantMask {
				t.Errorf("Mask = %d (%v), want %d (%v)", int64(res.Mask), res.Mask.IDs(), int64(tc.wantMask), tc.wantMask.IDs())
			}
			if res.TempFailed != tc.wantTemp {
				t.Errorf("TempFailed = %v, want %v", res.TempFailed, tc.wantTemp)
			}
			if res.HasHTTPTask() != (len(tc.wantSubs) > 0) {
				t.Fatalf("HasHTTPTask = %v, want %v", res.HasHTTPTask(), len(tc.wantSubs) > 0)
			}
			if !reflect.DeepEqual(res.HTTPSubdomains, tc.wantSubs) {
				t.Errorf("HTTPSubdomains = %+v, want %+v", res.HTTPSubdomains, tc.wantSubs)
			}
		})
	}
}

// mustMask — a mask with the bits (a failure is a test error).
func mustMask(t *testing.T, ids ...int) model.DNSMask {
	t.Helper()
	m, err := model.NewDNSMask(ids...)
	if err != nil {
		t.Fatalf("NewDNSMask(%v): %v", ids, err)
	}
	return m
}

// --- RecordEpisode --------------------------------------------------------------

func TestResultRecordEpisode(t *testing.T) {
	t.Run("a terminal status — a statement with status and the CASE temp_attempts", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		res := EpisodeResult{SetStatus: true, Status: model.StatusNotDelegated, Mask: mustMask(t, 1)}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42, res, nil)
		if err != nil || !recorded {
			t.Fatalf("RecordEpisode = (%v, %v), want (true, nil)", recorded, err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 1 {
			t.Fatalf("%d Exec calls, want 1", len(execs))
		}
		if execs[0].sql != resultUpdateStatusSQL {
			t.Errorf("SQL = %q, want resultUpdateStatusSQL", execs[0].sql)
		}
		wantArgs := []any{int16(7), int64(42), int64(mustMask(t, 1)), int16(model.StatusNotDelegated)}
		if !reflect.DeepEqual(execs[0].args, wantArgs) {
			t.Errorf("args = %v, want %v", execs[0].args, wantArgs)
		}
	})

	t.Run("addresses present — a statement without status (the outcome is up to the HTTP stage)", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		res := EpisodeResult{Mask: mustMask(t, 0, 1),
			HTTPSubdomains: []queue.HTTPSubdomain{{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}}}}
		if _, err := RecordEpisode(context.Background(), pool, 7, 42, res, nil); err != nil {
			t.Fatalf("RecordEpisode: %v", err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 1 || execs[0].sql != resultUpdateKeepStatusSQL {
			t.Fatalf("SQL = %v, want resultUpdateKeepStatusSQL", execs[0].sql)
		}
		wantArgs := []any{int16(7), int64(42), int64(mustMask(t, 0, 1))}
		if !reflect.DeepEqual(execs[0].args, wantArgs) {
			t.Errorf("args = %v, want %v", execs[0].args, wantArgs)
		}
	})

	t.Run("a temp_error outcome — the recording and the bucket increment as separate statements", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		res := EpisodeResult{SetStatus: true, Status: model.StatusTempError}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42, res, nil)
		if err != nil || !recorded {
			t.Fatalf("RecordEpisode = (%v, %v), want (true, nil)", recorded, err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 2 {
			t.Fatalf("%d Exec calls, want 2 (the recording + the bucket increment, T-267)", len(execs))
		}
		if execs[0].sql != resultUpdateStatusSQL {
			t.Errorf("SQL #1 = %q, want resultUpdateStatusSQL (the recording before the increment)", execs[0].sql)
		}
		if execs[1].sql != resultMatureIncSQL {
			t.Errorf("SQL #2 = %q, want resultMatureIncSQL (the increment as a separate statement)", execs[1].sql)
		}
		if len(execs[1].args) != 0 {
			t.Errorf("increment args = %v, want empty (the DB computes the current_date key)", execs[1].args)
		}
		// The order in the protocol: the increment strictly after the
		// write (T-267) — the counter waits for the recorded transition,
		// not the other way around.
		ei, ii := j.indexOf(resultEvExec, resultUpdateStatusSQL), j.indexOf(resultEvExec, resultMatureIncSQL)
		if ei < 0 || ii < 0 || ei > ii {
			t.Fatalf("the order broken: recording=%d increment=%d (the recording before the increment)", ei, ii)
		}
	})

	t.Run("an increment failure — the transition recorded, reconciliation will catch up the bucket", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, execErrs: []error{nil, errors.New("db blipped")}}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42,
			EpisodeResult{SetStatus: true, Status: model.StatusTempError}, nil)
		if err == nil || !strings.Contains(err.Error(), "retry_mature bucket increment of domain 7/42") {
			t.Fatalf("error = %v, want \"retry_mature bucket increment of domain 7/42\"", err)
		}
		if !recorded {
			t.Error("recorded = false, want true (the transition recorded before the increment failure — the ±1 window, T-267)")
		}
		if execs := pool.resultPoolExecs(); len(execs) != 2 {
			t.Fatalf("%d Exec calls, want 2", len(execs))
		}
	})

	t.Run("the guards cut off the row — success without the recording and the increment", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42,
			EpisodeResult{SetStatus: true, Status: model.StatusTempError}, nil)
		if err != nil {
			t.Fatalf("RecordEpisode: %v", err)
		}
		if recorded {
			t.Error("recorded = true, want false (already done or deleted)")
		}
		if execs := pool.resultPoolExecs(); len(execs) != 1 {
			t.Errorf("%d Exec calls, want 1 (zero translated — no increment runs)", len(execs))
		}
	})

	t.Run("a DB error — a recording error", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, execErr: errors.New("db unavailable")}
		_, err := RecordEpisode(context.Background(), pool, 7, 42, EpisodeResult{}, nil)
		if err == nil || !strings.Contains(err.Error(), "result recording of domain 7/42") ||
			!errors.Is(err, pool.execErr) {
			t.Fatalf("error = %v, want \"result recording of domain 7/42: db unavailable\"", err)
		}
	})

	t.Run("an invalid status — an error before the DB", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		_, err := RecordEpisode(context.Background(), pool, 7, 42,
			EpisodeResult{SetStatus: true, Status: model.Status(99)}, nil)
		if err == nil || !strings.Contains(err.Error(), "invalid status") {
			t.Fatalf("error = %v, want \"invalid status\"", err)
		}
		if len(pool.resultPoolExecs()) != 0 {
			t.Error("Exec must not be called on an invalid status")
		}
	})

	t.Run("a ctx cancel — no DB access", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := RecordEpisode(ctx, pool, 7, 42, EpisodeResult{}, nil); err == nil {
			t.Fatal("RecordEpisode with a canceled ctx = nil, want a context error")
		}
	})

	t.Run("the pool is not set", func(t *testing.T) {
		if _, err := RecordEpisode(context.Background(), nil, 1, 1, EpisodeResult{}, nil); err == nil ||
			!strings.Contains(err.Error(), "the pool is not set") {
			t.Fatalf("error = %v, want \"the pool is not set\"", err)
		}
	})
}

// TestResultRecordEpisodeDeadlock — a 40P01 fault injection (T-268):
// the write statement's deadlock is absorbed by repeating the
// autocommit command as is — the DNS episode is neither dropped nor
// replayed (the delivery's XACK goes through); after the attempts are
// exhausted, the error goes out (a delivery without XACK, spec 3.4).
func TestResultRecordEpisodeDeadlock(t *testing.T) {
	deadlock := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}

	t.Run("a recording deadlock then success — a command retry, a metric", func(t *testing.T) {
		reg := metrics.New("dns-worker")
		pool := &resultFakePool{j: &resultJournal{}, execErrs: []error{deadlock, nil}}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42, EpisodeResult{}, NewMetrics(reg))
		if err != nil || !recorded {
			t.Fatalf("RecordEpisode = (%v, %v), want (true, nil) — the deadlock absorbed", recorded, err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 2 || execs[0].sql != resultUpdateKeepStatusSQL || execs[1].sql != resultUpdateKeepStatusSQL {
			t.Fatalf("execs = %+v, want a retry of the same recording command", execs)
		}
		if got := dnsMetricValue(t, reg, "crawler_db_deadlock_retried_total", "dns-worker"); got != 1 {
			t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
		}
	})

	t.Run("a bucket increment deadlock then success — the episode finished", func(t *testing.T) {
		reg := metrics.New("dns-worker")
		pool := &resultFakePool{j: &resultJournal{}, execErrs: []error{nil, deadlock, nil}}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42,
			EpisodeResult{SetStatus: true, Status: model.StatusTempError}, NewMetrics(reg))
		if err != nil || !recorded {
			t.Fatalf("RecordEpisode = (%v, %v), want (true, nil) — the increment retried", recorded, err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 3 || execs[1].sql != resultMatureIncSQL || execs[2].sql != resultMatureIncSQL {
			t.Fatalf("execs = %+v, want the recording + a retry of the bucket increment", execs)
		}
		if got := dnsMetricValue(t, reg, "crawler_db_deadlock_retried_total", "dns-worker"); got != 1 {
			t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
		}
	})

	t.Run("a persistent deadlock — the error out after the retries exhausted", func(t *testing.T) {
		reg := metrics.New("dns-worker")
		pool := &resultFakePool{j: &resultJournal{}, execErr: deadlock}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42, EpisodeResult{}, NewMetrics(reg))
		if err == nil || !db.IsDeadlock(err) ||
			!strings.Contains(err.Error(), "result recording of domain 7/42") {
			t.Fatalf("error = %v, want the 40P01 \"result recording of domain 7/42\" out", err)
		}
		if recorded {
			t.Error("recorded = true, want false (the transition not recorded)")
		}
		if execs := pool.resultPoolExecs(); len(execs) != 1+db.MaxDeadlockRetries {
			t.Errorf("%d Exec calls, want %d (the first + retries)", len(execs), 1+db.MaxDeadlockRetries)
		}
		if got := dnsMetricValue(t, reg, "crawler_db_deadlock_retried_total", "dns-worker"); got != db.MaxDeadlockRetries {
			t.Errorf("crawler_db_deadlock_retried_total = %v, want %d", got, db.MaxDeadlockRetries)
		}
	})
}

// TestResultRecordInvalidDeadlock — the invalid diagnosis under a 40P01
// retry (T-268, the same resultRetry): the deadlock is absorbed by
// repeating the same command — the poison row leaves the re-publication
// loop without a delivery redelivery.
func TestResultRecordInvalidDeadlock(t *testing.T) {
	reg := metrics.New("dns-worker")
	pool := &resultFakePool{j: &resultJournal{},
		execErrs: []error{&pgconn.PgError{Code: "40P01", Message: "deadlock detected"}}}
	recorded, err := RecordInvalid(context.Background(), pool, 7, 42, NewMetrics(reg))
	if err != nil || !recorded {
		t.Fatalf("RecordInvalid = (%v, %v), want (true, nil) — the deadlock absorbed", recorded, err)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 2 || execs[0].sql != resultInvalidSQL || execs[1].sql != resultInvalidSQL {
		t.Fatalf("execs = %+v, want a retry of the same diagnosis command", execs)
	}
	if got := dnsMetricValue(t, reg, "crawler_db_deadlock_retried_total", "dns-worker"); got != 1 {
		t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
	}
}

// TestResultRunDeadlockAck — the T-268 acceptance criterion: the result
// write deadlock is absorbed by the retry — the q:dns delivery is
// acknowledged (XACK), the task does not burn in a redelivery; the
// metric grows.
func TestResultRunDeadlockAck(t *testing.T) {
	reg := metrics.New("dns-worker")
	j := &resultJournal{ackNotify: make(chan struct{}, 4)}
	// The Exec order (Concurrency 1): the publication mark → the write;
	// the write stumbles on a deadlock and repeats (the third Exec —
	// nil by default).
	pool := &resultFakePool{j: j, subdomains: resultSubs(),
		execErrs: []error{nil, &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}}}
	q := &resultFakeQueue{j: j, deliveries: []queue.Delivery{
		resultDelivery("9-1", "task-9",
			queue.DNSDomain{ZoneID: 3, DomainID: 31, Name: "delegated.example.com"}),
	}}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "delegated.example.com")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, pool, q, resolve, Options{
			Concurrency: 1, Group: "g", Consumer: "c", Metrics: reg,
		})
	}()

	if !resultWaitAck(j, 5*time.Second) {
		cancel()
		t.Fatal("XACK not awaited: the deadlock not absorbed — the delivery not acknowledged")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancel")
	}

	if got := dnsMetricValue(t, reg, "crawler_db_deadlock_retried_total", "dns-worker"); got != 1 {
		t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
	}
	if n := j.count(resultEvPublish); n != 1 {
		t.Errorf("q:http publications = %d, want 1 (no duplicate — the episode not replayed)", n)
	}
	if execs := pool.resultPoolExecs(); len(execs) != 3 ||
		execs[0].sql != resultMarkPublishedSQL || execs[1].sql != resultUpdateKeepStatusSQL ||
		execs[2].sql != resultUpdateKeepStatusSQL {
		t.Errorf("execs = %+v, want the mark + the recording with a retry of the same command", execs)
	}
}

// --- ProcessDomain ------------------------------------------------------------

// TestResultProcessDomainWithAddrs — an end-to-end episode of a domain
// with addresses: the "publish q:http → write done" order (§3.4), the
// task contents (the A/AAAA addresses, the apex and subdomain CNAMEs),
// the write statement without a status.
func TestResultProcessDomainWithAddrs(t *testing.T) {
	j := &resultJournal{}
	pool := &resultFakePool{j: j}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")
	// askAAAA: the apex and www answer AAAA (the chain's terminal
	// names).
	resolve.setAnswer("example.com", dns.TypeAAAA,
		resultReply(dnsclient.RCodeNOERROR, apexAAAARR("example.com.", "2001:db8::1")))
	resolve.setAnswer("cdn.example.com", dns.TypeAAAA,
		resultReply(dnsclient.RCodeNOERROR, apexAAAARR("cdn.example.com.", "2001:db8::2")))

	dom := PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}}
	if err := ProcessDomain(context.Background(), pool, q, resolve, dom, resultSubs(), true, nil, nil); err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	// The order: the q:http publication → the http_published mark → the
	// write (the file comment): the publication index is strictly less
	// than the first Exec.
	if pi, ei := j.indexOf(resultEvPublish, queue.StreamHTTP), j.indexOf(resultEvExec, ""); pi < 0 || ei < 0 || pi > ei {
		t.Fatalf("the order broken: publish=%d exec=%d (the publication before the mark/recording)", pi, ei)
	}
	if n := j.count(resultEvExec); n != 2 {
		t.Fatalf("%d Exec calls, want 2 (the http_published mark + the recording)", n)
	}
	execs := pool.resultPoolExecs()
	if execs[0].sql != resultMarkPublishedSQL {
		t.Errorf("SQL №1 = %q, want resultMarkPublishedSQL", execs[0].sql)
	}
	if !reflect.DeepEqual(execs[0].args, []any{int16(3), int64(11)}) {
		t.Errorf("mark args = %v, want [3 11]", execs[0].args)
	}
	if execs[1].sql != resultUpdateKeepStatusSQL {
		t.Errorf("the recording SQL = %q, want resultUpdateKeepStatusSQL", execs[1].sql)
	}
	if !reflect.DeepEqual(execs[1].args, []any{int16(3), int64(11), int64(mustMask(t, 0, 1))}) {
		t.Errorf("recording args = %v", execs[1].args)
	}

	publishes := q.resultPublishes()
	if len(publishes) != 1 {
		t.Fatalf("%d publications, want 1", len(publishes))
	}
	p := publishes[0]
	if p.stream != queue.StreamHTTP {
		t.Errorf("the stream = %q, want %q", p.stream, queue.StreamHTTP)
	}
	want := queue.HTTPTask{
		ZoneID: 3, DomainID: 11, Name: "example.com",
		Subdomains: []queue.HTTPSubdomain{
			{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10", "2001:db8::1"}},
			// CNAME — the presentation form without the root dot
			// (validated by the queue against the 253-octet limit,
			// T-024).
			{SubdomainID: 1, Label: "www", Addrs: []string{"203.0.113.5", "2001:db8::2"}, CNAME: "cdn.example.com"},
		},
	}
	if p.task.DomainID != want.DomainID || p.task.ZoneID != want.ZoneID || p.task.Name != want.Name {
		t.Errorf("the task = %+v, want %+v", p.task, want)
	}
	if !reflect.DeepEqual(p.task.Subdomains, want.Subdomains) {
		t.Errorf("the task subdomains = %+v, want %+v", p.task.Subdomains, want.Subdomains)
	}
}

// TestResultProcessDomainNXDOMAIN — a terminal apex branch: the
// subdomains and the wildcard are not polled (RFC 8020), the write is
// status=not_delegated.
func TestResultProcessDomainNXDOMAIN(t *testing.T) {
	j := &resultJournal{}
	pool := &resultFakePool{j: j}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetNXDOMAIN(resolve, "gone.example.com")

	dom := PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "gone.example.com"}}
	if err := ProcessDomain(context.Background(), pool, q, resolve, dom, resultSubs(), true, nil, nil); err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	if got := len(resolve.resultCalls()); got != 1 {
		t.Fatalf("%d exchanges, want 1 (the apex only, RFC 8020)", got)
	}
	if n := j.count(resultEvPublish); n != 0 {
		t.Errorf("%d publications, want 0 (no addresses)", n)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 1 || execs[0].sql != resultUpdateStatusSQL {
		t.Fatalf("the recording SQL = %v, want resultUpdateStatusSQL", execs)
	}
	if !reflect.DeepEqual(execs[0].args,
		[]any{int16(3), int64(12), int64(0), int16(model.StatusNotDelegated)}) {
		t.Errorf("recording args = %v", execs[0].args)
	}
}

// TestResultProcessDomainWildcard — a wildcard zone: admin is excluded
// from the task (its addresses ⊆ the wildcard answer), the apex always
// goes; a wildcard detection failure — a continuation without filtering
// (the T-038 policy).
func TestResultProcessDomainWildcard(t *testing.T) {
	t.Run("the detect fired — excluded ones are not published, the mask full", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetWildcardZone(resolve, "parked.example.com")

		if err := ProcessDomain(context.Background(), pool, q, resolve,
			PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 5, DomainID: 21, Name: "parked.example.com"}},
			resultSubs(), false, nil, nil); err != nil {
			t.Fatalf("ProcessDomain: %v", err)
		}

		publishes := q.resultPublishes()
		if len(publishes) != 1 {
			t.Fatalf("%d publications, want 1", len(publishes))
		}
		subs := publishes[0].task.Subdomains
		if len(subs) != 1 || subs[0].SubdomainID != 0 || subs[0].Label != "" {
			t.Fatalf("the task subdomains = %+v, want the apex only (admin — a wildcard, www without addresses)", subs)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 2 || execs[0].sql != resultMarkPublishedSQL {
			t.Fatalf("execs = %+v, want the http_published mark + the recording", execs)
		}
		if !reflect.DeepEqual(execs[1].args,
			[]any{int16(5), int64(21), int64(mustMask(t, 0, 2))}) {
			t.Errorf("recording args = %v, want a mask with the apex and admin bits", execs[1].args)
		}
	})

	t.Run("the detect failed — no filtering, the episode does not crash", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "flaky.example.com")
		// The detection question (a random label) — a transport failure.
		resolve.setWild("flaky.example.com", resultAnswer{err: errResultNet})

		if err := ProcessDomain(context.Background(), pool, q, resolve,
			PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 5, DomainID: 22, Name: "flaky.example.com"}},
			resultSubs(), false, nil, nil); err != nil {
			t.Fatalf("ProcessDomain: %v (the T-038 policy — continue)", err)
		}
		publishes := q.resultPublishes()
		if len(publishes) != 1 || len(publishes[0].task.Subdomains) != 2 {
			t.Fatalf("the task subdomains = %+v, want the apex and www without filtering", publishes)
		}
		if n := j.count(resultEvExec); n != 2 {
			t.Errorf("%d Exec calls, want 2 (the http_published mark + the recording)", n)
		}
	})
}

// TestResultProcessDomainErrors — step errors: a publication failure —
// without a write (the domain comes back via redelivery); a write
// failure — an error after the publication (a bounded task duplicate,
// §3.4); a ctx cancellation — the episode did not finish, nothing is
// published or written.
func TestResultProcessDomainErrors(t *testing.T) {
	t.Run("a publication failure — no recording", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j, publishErrs: []error{errors.New("redis unavailable")}}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "example.com")

		err := ProcessDomain(context.Background(), pool, q, resolve,
			PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}},
			resultSubs(), false, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "q:http publication") {
			t.Fatalf("error = %v, want \"q:http publication\"", err)
		}
		if n := j.count(resultEvExec); n != 0 {
			t.Errorf("%d Exec calls, want 0 (the recording only after the publication)", n)
		}
	})

	t.Run("a recording failure — an episode error, the publication already happened", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j, execErr: errors.New("db unavailable")}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "example.com")

		err := ProcessDomain(context.Background(), pool, q, resolve,
			PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}},
			resultSubs(), false, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "result recording") {
			t.Fatalf("error = %v, want \"result recording\"", err)
		}
		if n := j.count(resultEvPublish); n != 1 {
			t.Errorf("%d publications, want 1 (the duplicate bounded by the delivery attempts, T-025)", n)
		}
	})

	t.Run("a ctx cancel before the start — no exchange", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "example.com")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := ProcessDomain(ctx, pool, q, resolve,
			PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}},
			resultSubs(), false, nil, nil)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if got := len(resolve.resultCalls()); got != 0 {
			t.Errorf("%d exchanges, want 0", got)
		}
		if n := j.count(resultEvExec) + j.count(resultEvPublish); n != 0 {
			t.Errorf("publications+recordings = %d, want 0", n)
		}
	})

	t.Run("a ctx cancel amid the subdomains — the episode not finished", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "example.com")
		ctx, cancel := context.WithCancel(context.Background())
		resolve.onCall = func(n int) {
			if n >= 2 { // after the apex, in the subdomains
				cancel()
			}
		}

		err := ProcessDomain(ctx, pool, q, resolve,
			PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}},
			resultSubs(), false, nil, nil)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if n := j.count(resultEvExec) + j.count(resultEvPublish); n != 0 {
			t.Errorf("publications+recordings = %d, want 0 (the episode not finished)", n)
		}
	})

	t.Run("the queue is not set", func(t *testing.T) {
		err := ProcessDomain(context.Background(), &resultFakePool{j: &resultJournal{}}, nil,
			resultNewResolver(), PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 1, DomainID: 1, Name: "example.com"}},
			resultSubs(), false, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "the queue is not set") {
			t.Fatalf("error = %v, want \"the queue is not set\"", err)
		}
	})
}

// --- the q:http publication staging mark (T-108) --------------------------------

// TestResultMarkHTTPPublished — the mark after the publication: the
// exact SQL (an OR of the bit with the others preserved), the
// done/deleted guards, the errors, the cancellation.
func TestResultMarkHTTPPublished(t *testing.T) {
	t.Run("sets the bit with one UPDATE", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		marked, err := MarkHTTPPublished(context.Background(), pool, 7, 42)
		if err != nil || !marked {
			t.Fatalf("MarkHTTPPublished = (%v, %v), want (true, nil)", marked, err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 1 || execs[0].sql != resultMarkPublishedSQL {
			t.Fatalf("execs = %+v, want one resultMarkPublishedSQL", execs)
		}
		if !reflect.DeepEqual(execs[0].args, []any{int16(7), int64(42)}) {
			t.Errorf("args = %v, want [7 42]", execs[0].args)
		}
	})

	t.Run("the guards cut off the row — success without the bit", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{},
			execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
		marked, err := MarkHTTPPublished(context.Background(), pool, 7, 42)
		if err != nil {
			t.Fatalf("MarkHTTPPublished: %v", err)
		}
		if marked {
			t.Error("marked = true, want false (done/deleted)")
		}
	})

	t.Run("a DB error", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, execErr: errors.New("db unavailable")}
		_, err := MarkHTTPPublished(context.Background(), pool, 7, 42)
		if err == nil || !strings.Contains(err.Error(), "publication mark of domain 7/42") {
			t.Fatalf("error = %v, want \"publication mark of domain 7/42\"", err)
		}
	})

	t.Run("the pool is not set", func(t *testing.T) {
		if _, err := MarkHTTPPublished(context.Background(), nil, 1, 1); err == nil ||
			!strings.Contains(err.Error(), "the pool is not set") {
			t.Fatalf("error = %v, want \"the pool is not set\"", err)
		}
	})

	t.Run("ctx cancel", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := MarkHTTPPublished(ctx, pool, 7, 42); err == nil {
			t.Fatal("a canceled ctx = nil, want a context error")
		}
	})
}

// TestResultProcessDomainPublishedSkip — a redelivery of an episode
// with an already published task (the http_published bit is set): the
// publication is skipped (no q:http duplicate), the mark is not
// repeated, the episode reaches the write — the domain receives done
// (T-108).
func TestResultProcessDomainPublishedSkip(t *testing.T) {
	j := &resultJournal{}
	pool := &resultFakePool{j: j}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")

	dom := PendingDomain{
		DNSDomain:     queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"},
		HTTPPublished: true, // the previous attempt published, the write did not go through
	}
	if err := ProcessDomain(context.Background(), pool, q, resolve, dom, resultSubs(), false, nil, nil); err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	if n := j.count(resultEvPublish); n != 0 {
		t.Errorf("%d publications, want 0 (the task already published — the duplicate excluded)", n)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 1 || execs[0].sql != resultUpdateKeepStatusSQL {
		t.Fatalf("execs = %+v, want one recording without a repeated mark", execs)
	}
	if !reflect.DeepEqual(execs[0].args, []any{int16(3), int64(11), int64(mustMask(t, 0, 1))}) {
		t.Errorf("recording args = %v", execs[0].args)
	}
}

// TestResultProcessDomainMarkFailure — an UPDATE failure of the mark
// after a successful publication is non-fatal (T-108): the episode
// finishes with the write, the price is a possible q:http duplicate on
// redelivery, not a loss.
func TestResultProcessDomainMarkFailure(t *testing.T) {
	j := &resultJournal{}
	// The first Exec (the mark) fails, the write (the second) goes
	// through.
	pool := &resultFakePool{j: j, execErrs: []error{errors.New("db blipped"), nil}}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")

	dom := PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}}
	if err := ProcessDomain(context.Background(), pool, q, resolve, dom, resultSubs(), false, nil, nil); err != nil {
		t.Fatalf("ProcessDomain: %v, want nil (the mark non-fatal)", err)
	}
	if n := j.count(resultEvPublish); n != 1 {
		t.Errorf("%d publications, want 1", n)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 2 || execs[0].sql != resultMarkPublishedSQL || execs[1].sql != resultUpdateKeepStatusSQL {
		t.Fatalf("execs = %+v, want the failed mark + the recording", execs)
	}
}

// TestResultProcessDomainMatureFailure — a failure of the retry_mature
// bucket increment AFTER the write is non-fatal (T-267): the transition
// is recorded, the episode is finished (the delivery is acknowledged),
// a redelivery is powerless (the done guard will cut off both
// statements), the monitor reconciliation will catch the bucket up by
// ±1 — a Warn, not an episode error.
func TestResultProcessDomainMatureFailure(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	// An apex SERVFAIL — a terminal temp_error without subdomains (§7).
	j := &resultJournal{}
	pool := &resultFakePool{j: j, execErrs: []error{nil, errors.New("db blipped")}}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resolve.setAnswer("flaky.example.com", dns.TypeA, resultReply(dnsclient.RCodeSERVFAIL))

	dom := PendingDomain{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "flaky.example.com"}}
	if err := ProcessDomain(context.Background(), pool, q, resolve, dom, resultSubs(), false, nil, log); err != nil {
		t.Fatalf("ProcessDomain: %v, want nil (the bucket increment non-fatal)", err)
	}
	if n := j.count(resultEvPublish); n != 0 {
		t.Errorf("%d publications, want 0 (the terminal branch without addresses)", n)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 2 || execs[0].sql != resultUpdateStatusSQL || execs[1].sql != resultMatureIncSQL {
		t.Fatalf("execs = %+v, want the temp_error recording + the failed bucket increment", execs)
	}
	if !strings.Contains(buf.String(), "the retry_mature bucket increment was not written") {
		t.Errorf("the log does not contain the bucket increment Warn:\n%s", buf.String())
	}
}

// TestResultProcessBatchRedeliveryPartial — the T-108 scenario in full:
// the first attempt of a batch of three delegated domains — the first
// is recorded, the write for the other two failed after the publication
// (the mark is set); the redelivery — the recorded one is cut off by
// the filter, the published ones are driven to the write WITHOUT a
// repeated publication (no q:http duplicates), the delivery is
// acknowledged (the XACK semantics of Processing do not change).
func TestResultProcessBatchRedeliveryPartial(t *testing.T) {
	newDelivery := func() queue.Delivery {
		return resultDelivery("7-1", "task-7",
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "a.example.com"},
			queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "b.example.com"},
			queue.DNSDomain{ZoneID: 3, DomainID: 13, Name: "c.example.com"})
	}

	// Attempt 1: Concurrency 1 — a deterministic Exec order:
	// a: mark(0), write(1); b: mark(2), write(3) — a failure;
	// c: mark(4), write(5) — a failure.
	j := &resultJournal{}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "a.example.com")
	resultSetDelegated(resolve, "b.example.com")
	resultSetDelegated(resolve, "c.example.com")

	dbDown := errors.New("db blipped")
	pool1 := &resultFakePool{j: j, subdomains: resultSubs(),
		execErrs: []error{nil, nil, nil, dbDown, nil, dbDown}}
	err := processBatch(context.Background(), pool1, q, resolve, newDelivery(),
		Options{Concurrency: 1}, nil, slog.New(slog.DiscardHandler))
	if err == nil || !errors.Is(err, dbDown) {
		t.Fatalf("processBatch (attempt 1) = %v, want a recording error", err)
	}
	if got := len(q.resultPublishes()); got != 3 {
		t.Fatalf("publications after attempt 1 = %d, want 3 (one per domain)", got)
	}

	// Redelivery: a is recorded (done — the filter cuts it off), b and c
	// are not recorded, with the http_published bit set (the publication
	// happened). The DB write is restored.
	pool2 := &resultFakePool{j: j, subdomains: resultSubs(), pending: [][3]any{
		{int16(3), int64(12), int16(model.FlagHTTPPublished)},
		{int16(3), int64(13), int16(model.FlagHTTPPublished)},
	}}
	if err := processBatch(context.Background(), pool2, q, resolve, newDelivery(),
		Options{Concurrency: 1}, nil, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("processBatch (redelivery) = %v, want nil", err)
	}

	publishes := q.resultPublishes()
	if len(publishes) != 3 {
		t.Fatalf("publications total = %d, want 3 — redelivery creates no duplicates", len(publishes))
	}
	perDomain := map[int64]int{}
	for _, p := range publishes {
		perDomain[p.task.DomainID]++
	}
	for _, id := range []int64{11, 12, 13} {
		if perDomain[id] != 1 {
			t.Errorf("domain %d: %d publications, want exactly one", id, perDomain[id])
		}
	}

	// The repeat only did the writes (without the marks — the bit is
	// already set).
	execs := pool2.resultPoolExecs()
	if len(execs) != 2 {
		t.Fatalf("redelivery Execs = %d, want 2 (recordings only)", len(execs))
	}
	for _, e := range execs {
		if e.sql != resultUpdateKeepStatusSQL {
			t.Errorf("the retry SQL = %q, want resultUpdateKeepStatusSQL (no mark)", e.sql)
		}
	}
}

// --- PendingDomains / LoadSubdomains -----------------------------------------

func TestResultPendingDomains(t *testing.T) {
	t.Run("mirror — all the batch keys in its order, the bit clean", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		batch := []queue.DNSDomain{
			{ZoneID: 3, DomainID: 11, Name: "a.example.com"},
			{ZoneID: 3, DomainID: 12, Name: "b.example.com"},
			{ZoneID: 4, DomainID: 13, Name: "c.example.org"},
		}
		out, err := PendingDomains(context.Background(), pool, batch)
		if err != nil {
			t.Fatalf("PendingDomains: %v", err)
		}
		want := []PendingDomain{
			{DNSDomain: batch[0]},
			{DNSDomain: batch[1]},
			{DNSDomain: batch[2]},
		}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("outcome = %+v, want the whole batch in order without the mark", out)
		}
		ev := j.snapshot()
		if len(ev) != 1 || ev[0].kind != resultEvQueryPend {
			t.Fatalf("events = %+v, want one query:pending", ev)
		}
		wantArgs := []any{
			[]int16{3, 3, 4},
			[]int64{11, 12, 13},
		}
		if !reflect.DeepEqual(ev[0].args, wantArgs) {
			t.Errorf("args = %v, want %v", ev[0].args, wantArgs)
		}
	})

	t.Run("a canned filter — only unprocessed ones, with the publication bit", func(t *testing.T) {
		// a: done — cut off; b: not recorded, the bit is set
		// (published); c: not recorded, the bit is clean (never reached
		// the publication).
		pool := &resultFakePool{j: &resultJournal{}, pending: [][3]any{
			{int16(3), int64(12), int16(model.FlagHTTPPublished)},
			{int16(3), int64(13), int16(0)},
		}}
		out, err := PendingDomains(context.Background(), pool, []queue.DNSDomain{
			{ZoneID: 3, DomainID: 11, Name: "a.example.com"},
			{ZoneID: 3, DomainID: 12, Name: "b.example.com"},
			{ZoneID: 3, DomainID: 13, Name: "c.example.com"},
		})
		if err != nil {
			t.Fatalf("PendingDomains: %v", err)
		}
		want := []PendingDomain{
			{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "b.example.com"}, HTTPPublished: true},
			{DNSDomain: queue.DNSDomain{ZoneID: 3, DomainID: 13, Name: "c.example.com"}},
		}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("outcome = %+v, want %+v (the name from the batch, the bit from flags)", out, want)
		}
	})

	t.Run("foreign flags bits are not read as http_published", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, pending: [][3]any{
			{int16(3), int64(11), int16(model.FlagIDN | model.FlagRenderPending)},
		}}
		out, err := PendingDomains(context.Background(), pool,
			[]queue.DNSDomain{{ZoneID: 3, DomainID: 11, Name: "a.example.com"}})
		if err != nil {
			t.Fatalf("PendingDomains: %v", err)
		}
		if len(out) != 1 || out[0].HTTPPublished {
			t.Errorf("outcome = %+v, want one domain without the publication mark", out)
		}
	})

	t.Run("an empty batch — no DB access", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		out, err := PendingDomains(context.Background(), pool, nil)
		if err != nil || out != nil {
			t.Fatalf("PendingDomains = (%v, %v), want (nil, nil)", out, err)
		}
		if len(j.snapshot()) != 0 {
			t.Error("an empty batch must not touch the DB")
		}
	})

	t.Run("a DB error", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, pendingErr: errors.New("db unavailable")}
		_, err := PendingDomains(context.Background(), pool,
			[]queue.DNSDomain{{ZoneID: 3, DomainID: 11, Name: "a.example.com"}})
		if err == nil || !strings.Contains(err.Error(), "the batch filter") {
			t.Fatalf("error = %v, want \"the batch filter\"", err)
		}
	})

	t.Run("ctx cancel", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := PendingDomains(ctx, pool,
			[]queue.DNSDomain{{ZoneID: 3, DomainID: 11, Name: "a.example.com"}}); err == nil {
			t.Fatal("PendingDomains with a canceled ctx = nil, want an error")
		}
	})
}

func TestResultLoadSubdomains(t *testing.T) {
	t.Run("reads the reference table with disabled rows", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, subdomains: []model.Subdomain{
			{ID: 0, Label: "", Enabled: true},
			{ID: 1, Label: "www", Enabled: true},
			{ID: 2, Label: "admin", Enabled: false},
		}}
		subs, err := LoadSubdomains(context.Background(), pool)
		if err != nil {
			t.Fatalf("LoadSubdomains: %v", err)
		}
		if len(subs) != 3 || subs[2].Label != "admin" || subs[2].Enabled {
			t.Errorf("the reference table = %+v", subs)
		}
	})

	t.Run("an empty reference table — an error", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		_, err := LoadSubdomains(context.Background(), pool)
		if err == nil || !errors.Is(err, ErrSubdomainsEmpty) {
			t.Fatalf("error = %v, want ErrSubdomainsEmpty", err)
		}
	})

	t.Run("a DB error", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, subsErr: errors.New("db unavailable")}
		if _, err := LoadSubdomains(context.Background(), pool); err == nil ||
			!strings.Contains(err.Error(), "the subdomain reference table") {
			t.Fatalf("error = %v, want \"the subdomain reference table\"", err)
		}
	})
}

// --- Options ------------------------------------------------------------------

func TestResultOptionsDefaults(t *testing.T) {
	o := Options{}.withDefaults()
	if o.Concurrency != DefaultConcurrency || o.Group != DefaultGroup ||
		o.Consume != DefaultConsume || !strings.HasPrefix(o.Consumer, "dns-worker-") {
		t.Errorf("defaults = %+v", o)
	}
	custom := Options{Concurrency: 8, Group: "g", Consumer: "c1", Consume: 4, AskAAAA: true}.
		withDefaults()
	if custom.Concurrency != 8 || custom.Group != "g" || custom.Consumer != "c1" ||
		custom.Consume != 4 || !custom.AskAAAA {
		t.Errorf("custom values overwritten: %+v", custom)
	}
}

func TestResultRunValidation(t *testing.T) {
	opts := Options{Concurrency: 1}
	if err := Run(context.Background(), nil, &resultFakeQueue{j: &resultJournal{}}, resultNewResolver(), opts); err == nil ||
		!strings.Contains(err.Error(), "the pool is not set") {
		t.Fatalf("error = %v, want \"the pool is not set\"", err)
	}
	if err := Run(context.Background(), &resultFakePool{j: &resultJournal{}}, nil, resultNewResolver(), opts); err == nil ||
		!strings.Contains(err.Error(), "the queue is not set") {
		t.Fatalf("error = %v, want \"the queue is not set\"", err)
	}
	if err := Run(context.Background(), &resultFakePool{j: &resultJournal{}}, &resultFakeQueue{j: &resultJournal{}}, nil, opts); err == nil ||
		!strings.Contains(err.Error(), "the resolver is not set") {
		t.Fatalf("error = %v, want \"the resolver is not set\"", err)
	}
}

// --- the deterministic batch states (DNS-03) ------------------------------------

// TestResultProcessBatchDictStates — an empty, entirely disabled or
// malformed subdomains reference table is configuration, not poison:
// the batch is acknowledged without episodes, a redelivery does not
// spin it.
func TestResultProcessBatchDictStates(t *testing.T) {
	dom := queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}
	cases := []struct {
		name string
		subs []model.Subdomain
	}{
		{"an empty reference table", nil},
		{"all subdomains disabled", []model.Subdomain{
			{ID: 0, Label: "", Enabled: false},
			{ID: 1, Label: "www", Enabled: false},
		}},
		{"a malformed reference row", []model.Subdomain{
			{ID: 0, Label: "", Enabled: true},
			{ID: 99, Label: "bad", Enabled: true}, // an id outside 0..62
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := &resultJournal{}
			pool := &resultFakePool{j: j, subdomains: tc.subs}
			q := &resultFakeQueue{j: j}
			resolve := resultNewResolver()
			err := processBatch(context.Background(), pool, q, resolve,
				resultDelivery("9-1", "task-9", dom), Options{Concurrency: 1},
				nil, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatalf("processBatch = %v, want nil (the batch acknowledged with a Warn)", err)
			}
			if got := len(resolve.resultCalls()); got != 0 {
				t.Errorf("%d exchanges, want 0 (no episodes start)", got)
			}
			if n := j.count(resultEvExec) + j.count(resultEvPublish); n != 0 {
				t.Errorf("recordings+publications = %d, want 0", n)
			}
		})
	}
}

// TestResultProcessBatchPoisonDomain — a domain with an empty name (a
// deterministic episode error) does not drop the batch: the terminal
// invalid diagnosis (T-109) — a separate statement for the poison row;
// the other domains are processed normally, the delivery is
// acknowledged. The episodes run in parallel (Concurrency 2): the order
// of the statements is not deterministic — the writes are matched by
// domain_id.
func TestResultProcessBatchPoisonDomain(t *testing.T) {
	j := &resultJournal{}
	pool := &resultFakePool{j: j, subdomains: resultSubs()}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")

	err := processBatch(context.Background(), pool, q, resolve,
		resultDelivery("9-2", "task-10",
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"},
			queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "   "}),
		Options{Concurrency: 2}, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("processBatch = %v, want nil (a poison domain does not crash the batch)", err)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 3 {
		t.Fatalf("recordings = %+v, want 3: the healthy one's mark + recording, the poison one's diagnosis", execs)
	}
	byDomain := map[int64][]resultExec{}
	for _, e := range execs {
		byDomain[e.args[1].(int64)] = append(byDomain[e.args[1].(int64)], e)
	}
	healthy := byDomain[11]
	if len(healthy) != 2 || healthy[0].sql != resultMarkPublishedSQL || healthy[1].sql != resultUpdateKeepStatusSQL {
		t.Fatalf("domain 11 recordings = %+v, want the mark + the recording (the \"publication → recording\" order)", healthy)
	}
	poison := byDomain[12]
	if len(poison) != 1 || poison[0].sql != resultInvalidSQL {
		t.Fatalf("domain 12 recordings = %+v, want one invalid-diagnosis statement", poison)
	}
	if poison[0].args[0].(int16) != 3 {
		t.Errorf("diagnosis args = %v, want zone_id 3 as a literal", poison[0].args)
	}
	if got := len(resolve.resultCalls()); got == 0 {
		t.Fatal("the healthy domain not processed")
	}
}

// TestResultProcessBatchPoisonDomainRecordError — a failure of the
// invalid diagnosis write is an episode error: the batch is not
// acknowledged (no XACK), a redelivery will finish the fixation (the
// guard idempotency of T-109).
func TestResultProcessBatchPoisonDomainRecordError(t *testing.T) {
	j := &resultJournal{}
	pool := &resultFakePool{j: j, subdomains: resultSubs()}
	pool.execErr = errors.New("db unavailable")
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()

	err := processBatch(context.Background(), pool, q, resolve,
		resultDelivery("9-3", "task-11",
			queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "   "}),
		Options{Concurrency: 1}, nil, slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "invalid diagnosis") {
		t.Fatalf("processBatch = %v, want an invalid-diagnosis recording error", err)
	}
}

// TestResultRecordInvalid — the poison row diagnosis statement: the
// exact SQL text and parameters, the guards (an UPDATE 0 — success),
// the errors.
func TestResultRecordInvalid(t *testing.T) {
	t.Run("a statement with the invalid literal and guards", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		recorded, err := RecordInvalid(context.Background(), pool, 3, 12, nil)
		if err != nil || !recorded {
			t.Fatalf("RecordInvalid = (%v, %v), want (true, nil)", recorded, err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 1 {
			t.Fatalf("%d Exec calls, want 1", len(execs))
		}
		if execs[0].sql != resultInvalidSQL {
			t.Errorf("SQL = %q, want resultInvalidSQL", execs[0].sql)
		}
		wantArgs := []any{int16(3), int64(12)}
		if !reflect.DeepEqual(execs[0].args, wantArgs) {
			t.Errorf("args = %v, want %v", execs[0].args, wantArgs)
		}
	})

	t.Run("the guards cut off the row — success without the recording", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
		recorded, err := RecordInvalid(context.Background(), pool, 3, 12, nil)
		if err != nil {
			t.Fatalf("RecordInvalid: %v", err)
		}
		if recorded {
			t.Error("recorded = true, want false (already done or deleted)")
		}
	})

	t.Run("a DB error — a recording error", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, execErr: errors.New("db unavailable")}
		_, err := RecordInvalid(context.Background(), pool, 3, 12, nil)
		if err == nil || !strings.Contains(err.Error(), "invalid diagnosis of domain 3/12") ||
			!errors.Is(err, pool.execErr) {
			t.Fatalf("error = %v, want \"invalid diagnosis of domain 3/12: db unavailable\"", err)
		}
	})

	t.Run("the pool is not set", func(t *testing.T) {
		if _, err := RecordInvalid(context.Background(), nil, 1, 1, nil); err == nil ||
			!strings.Contains(err.Error(), "the pool is not set") {
			t.Fatalf("error = %v, want \"the pool is not set\"", err)
		}
	})
}

// --- the smoke test of the Run loop ----------------------------------------------

// TestResultRunSmoke — an end-to-end loop: a q:dns read → the reference
// table → the filter → the episodes → the q:http publication → the write
// → XACK; a clean exit on a ctx cancellation.
func TestResultRunSmoke(t *testing.T) {
	j := &resultJournal{ackNotify: make(chan struct{}, 4)}
	pool := &resultFakePool{j: j, subdomains: resultSubs()}
	q := &resultFakeQueue{j: j, deliveries: []queue.Delivery{
		resultDelivery("1-1", "task-1",
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"},
			queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "gone.example.com"}),
	}}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")
	resultSetNXDOMAIN(resolve, "gone.example.com")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, pool, q, resolve, Options{Concurrency: 2, Group: "g", Consumer: "c"})
	}()

	if !resultWaitAck(j, 5*time.Second) {
		cancel()
		t.Fatal("XACK not awaited: the delivery not processed")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v, want nil (a clean exit on cancel)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancel")
	}

	// The publication — only to the domain with addresses; the write —
	// to both; the mark — only to the published one (example.com).
	publishes := q.resultPublishes()
	if len(publishes) != 1 || publishes[0].task.DomainID != 11 ||
		publishes[0].task.Name != "example.com" {
		t.Fatalf("publications = %+v, want one q:http for example.com", publishes)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 3 {
		t.Fatalf("%d recordings, want 3 (the example.com mark + recording, the gone recording)", len(execs))
	}
	records := map[int64]resultExec{}
	for _, e := range execs {
		if e.sql == resultMarkPublishedSQL {
			if e.args[1].(int64) != 11 {
				t.Errorf("the http_published mark = %+v, want domain 11", e.args)
			}
			continue
		}
		records[e.args[1].(int64)] = e
	}
	if sql := records[11].sql; sql != resultUpdateKeepStatusSQL {
		t.Errorf("example.com: SQL = %q, want no status (addresses present)", sql)
	}
	if sql := records[12].sql; sql != resultUpdateStatusSQL {
		t.Errorf("gone.example.com: SQL = %q, want with status (not_delegated)", sql)
	}
	acks := q.resultAcks()
	if len(acks) != 1 || len(acks[0].ids) != 1 || acks[0].ids[0] != "1-1" ||
		acks[0].group != "g" || acks[0].stream != queue.StreamDNS {
		t.Fatalf("XACK = %+v, want entry 1-1 of group g", acks)
	}
}

// TestResultRunIdempotentRedelivery — a redelivery of an already
// recorded batch: the filter cuts off the done rows — no DNS exchanges
// and no q:http duplicates, the delivery is acknowledged.
func TestResultRunIdempotentRedelivery(t *testing.T) {
	j := &resultJournal{ackNotify: make(chan struct{}, 4)}
	pool := &resultFakePool{j: j, subdomains: resultSubs(), pending: [][3]any{}} // everything is done/deleted
	q := &resultFakeQueue{j: j, deliveries: []queue.Delivery{
		resultDelivery("2-1", "task-2",
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}),
	}}
	resolve := resultNewResolver() // no scenarios set: an exchange = a scenario error

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, pool, q, resolve, Options{Concurrency: 1, Group: "g", Consumer: "c"})
	}()

	if !resultWaitAck(j, 5*time.Second) {
		cancel()
		t.Fatal("XACK not awaited")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancel")
	}

	if got := len(resolve.resultCalls()); got != 0 {
		t.Errorf("%d exchanges, want 0 (done domains are not re-checked)", got)
	}
	if n := j.count(resultEvPublish); n != 0 {
		t.Errorf("%d publications, want 0 (the q:http duplicate excluded)", n)
	}
	if n := j.count(resultEvExec); n != 0 {
		t.Errorf("%d recordings, want 0", n)
	}
	if len(q.resultAcks()) != 1 {
		t.Errorf("XACK = %+v, want one", q.resultAcks())
	}
}

// TestResultRunSurvivesErrors — a processing error (a publication
// failure) does not stop the loop and does not acknowledge the delivery;
// the next one is processed; a read error is survived.
func TestResultRunSurvivesErrors(t *testing.T) {
	j := &resultJournal{ackNotify: make(chan struct{}, 4)}
	pool := &resultFakePool{j: j, subdomains: resultSubs()}
	q := &resultFakeQueue{j: j,
		deliveries: []queue.Delivery{
			resultDelivery("3-1", "task-3",
				queue.DNSDomain{ZoneID: 3, DomainID: 31, Name: "fail.example.com"}),
			resultDelivery("3-2", "task-4",
				queue.DNSDomain{ZoneID: 3, DomainID: 32, Name: "gone.example.com"}),
		},
		// The first q:http publication fails — the first delivery is not
		// acknowledged.
		publishErrs: []error{errors.New("redis unavailable")},
	}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "fail.example.com")
	resultSetNXDOMAIN(resolve, "gone.example.com")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, pool, q, resolve, Options{Concurrency: 1, Group: "g", Consumer: "c"})
	}()

	if !resultWaitAck(j, 5*time.Second) {
		cancel()
		t.Fatal("the second delivery's XACK not awaited")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after the ctx cancel")
	}

	acks := q.resultAcks()
	if len(acks) != 1 || acks[0].ids[0] != "3-2" {
		t.Fatalf("XACK = %+v, want only 3-2 (3-1 without an XACK — it returns via redelivery)", acks)
	}
	// Only the domain of the second delivery was recorded; the
	// publication was attempted — once (unsuccessfully;
	// gone.example.com has no addresses — nothing to publish).
	if n := j.count(resultEvExec); n != 1 {
		t.Errorf("%d recordings, want 1", n)
	}
	if n := j.count(resultEvPublish); n != 1 {
		t.Errorf("%d publication attempts, want 1 (unsuccessful)", n)
	}
}

// Integration tests of the http_published bit automaton (T-257, TZ-fsm spec
// tasks/TZ-fsm.md §3 FR-3) against live PostgreSQL: a scenario chain of all
// specification transitions (internal/dnsworker/result.go, the table at
// resultMarkPublishedSQL — the single source of truth) by real writer
// statements on one domain: B1 setting after XADD q:http, B4 skipping
// publication on redelivery, B2 clearing by the result write, B3 clearing
// by scheduler enqueueing — including the "XADD without the bit" window (a
// redelivery publishes an ALLOWED duplicate) and the bit surviving on
// pending (the lease returned the row unwritten — outside an episode the
// bit does not matter); the survival of "other" bits (is_idn) is checked on
// every transition. A separate scenario — the non-fatal B1 failure (the
// episode is carried to the write, the duplicate is excluded by the
// done row). The episodes are executed by ProcessDomain with a
// deterministic resolver (the network and classification — dns_test.go on
// the bench); the write failure is emulated by a pool wrapper (the
// countersMatureFailPool style). The zone counter deltas after each episode
// are checked by the monitor reconciliation (T-178). The call level is
// library-level, no services started; isolation — withTestSchema.
package integration

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
)

// httpBitQueue — the scenario's queue.Queue: publishing always succeeds
// (the transport is not the test's subject), q:http is counted (the unit of
// observation of duplicates/skips), scheduler enqueueing q:dns is not.
type httpBitQueue struct{ http int }

func (q *httpBitQueue) Publish(_ context.Context, stream string, _ queue.Message) (string, error) {
	if stream == queue.StreamHTTP {
		q.http++
	}
	return "id-1", nil
}

func (q *httpBitQueue) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	panic("httpBitQueue: Consume не используется")
}

func (q *httpBitQueue) Ack(context.Context, string, string, ...string) error {
	panic("httpBitQueue: Ack не используется")
}

// httpBitResolver — a deterministic dnsclient.Resolver: the domain's apex
// and www answer with an A record (the episode is always "there are
// addresses" → a q:http task, the write is the keep-status branch), other
// names (the random label of the T-038 wildcard detection) — NXDOMAIN.
type httpBitResolver struct{ domain string }

var _ dnsclient.Resolver = httpBitResolver{}

func (r httpBitResolver) Exchange(_ context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	fqdn := strings.ToLower(dns.Fqdn(r.domain))
	name := strings.ToLower(dns.Fqdn(q.Name))
	if q.Type == dns.TypeA && (name == fqdn || name == "www."+fqdn) {
		return &dnsclient.Result{RCode: dnsclient.RCodeNOERROR, Answer: []dns.RR{&dns.A{
			Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
			A:   net.ParseIP("192.0.2.10"),
		}}}, nil
	}
	return &dnsclient.Result{RCode: dnsclient.RCodeNXDOMAIN}, nil
}

// httpBitSubs — the scenario's reference table: the apex and www (both
// resolve — the 0b11 mask, the "there are addresses" episode, the write is
// the keep-status branch).
func httpBitSubs() []model.Subdomain {
	return []model.Subdomain{
		{ID: 0, Label: "", Enabled: true}, // the apex
		{ID: 1, Label: "www", Enabled: true},
	}
}

// httpBitFlags — the flags mask of the domain row: checking the survival of
// "other" bits (is_idn) under the set/clear statements (B1–B3 — "others
// preserved").
func httpBitFlags(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, id int64) int16 {
	t.Helper()
	var flags int16
	if err := pool.QueryRow(ctx,
		"SELECT flags FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
	).Scan(&flags); err != nil {
		t.Fatalf("чтение flags: %v", err)
	}
	return flags
}

// httpBitRecordFailMark — the identifier of the "there are addresses"
// result write (dnsworker resultUpdateKeepStatusSQL, R3) in the SQL text;
// duplicating the literal is the countersMatureIncMark style (the constant
// is not exported).
const httpBitRecordFailMark = "SET dns_mask = $3::int8, temp_attempts = 0"

// httpBitRecordFailPool — a db.DB wrapper of the pool: the DNS result
// write Query fails (the "worker died after XADD and the B1 mark, before
// the B2 write" scenario — the delivery is not acknowledged, the
// redelivery will finish the episode); everything else (the batch filter,
// the mark) is delegated to the pool.
type httpBitRecordFailPool struct{ *pgxpool.Pool }

func (p *httpBitRecordFailPool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return p.Pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *httpBitRecordFailPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.Pool.Exec(ctx, sql, args...)
}

func (p *httpBitRecordFailPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.Contains(sql, httpBitRecordFailMark) {
		return nil, errors.New("сбой записи результата (сценарий)")
	}
	return p.Pool.Query(ctx, sql, args...)
}

// httpBitMarkFailMark — the identifier of the bit setting (dnsworker
// resultMarkPublishedSQL) in the SQL text; duplicating the literal is the
// countersMatureIncMark style (the constant is not exported).
const httpBitMarkFailMark = "SET flags = (t.flags | 4)::int2"

// httpBitMarkFailPool — a db.DB wrapper of the pool: the bit-setting Exec
// fails (the "mark failure after XADD — non-fatal B1" scenario: the
// episode must reach the write); the rest is delegated to the pool.
type httpBitMarkFailPool struct{ *pgxpool.Pool }

func (p *httpBitMarkFailPool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return p.Pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *httpBitMarkFailPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, httpBitMarkFailMark) {
		return pgconn.CommandTag{}, errors.New("сбой постановки бита (сценарий)")
	}
	return p.Pool.Exec(ctx, sql, args...)
}

func (p *httpBitMarkFailPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.Pool.Query(ctx, sql, args...)
}

// httpBitFilter — the q:dns batch filter (resultPendingSQL) for exactly the
// one scenario domain: returns it with the bit state (B4).
func httpBitFilter(ctx context.Context, t *testing.T, pool *pgxpool.Pool, batch []queue.DNSDomain) dnsworker.PendingDomain {
	t.Helper()
	pend, err := dnsworker.PendingDomains(ctx, pool, batch)
	if err != nil {
		t.Fatalf("фильтр пачки: %v", err)
	}
	if len(pend) != 1 {
		t.Fatalf("фильтр пачки: доменов %d, хочу 1", len(pend))
	}
	return pend[0]
}

// TestHTTPPublishedBit_SpecTransitions — the specification transition chain
// on one domain: R1 → (the "XADD without the bit" window) → B1 (+the
// window duplicate) → B4+B2 → R7+R1 → B1 → R5 (the bit survived pending)
// → B3 → publishing a fresh episode without suppression. The q:http
// publication count grows 1 → 2 (the window duplicate) → 2 (B4: no
// duplicate) → 3 (B1 of episode 2) → 4 (a fresh episode after B3).
func TestHTTPPublishedBit_SpecTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "fsm-bit.test")
	// countersSeedDomains numbers the labels: prefix "spec" → label "spec0".
	ids := countersSeedDomains(t, pool, zone, "spec", 1, model.CheckStatePending, model.StatusNew)
	id := ids[0]
	const name = "spec0.fsm-bit.test"
	batch := []queue.DNSDomain{{ZoneID: zone, DomainID: id, Name: name}}
	subs := httpBitSubs()
	resolve := httpBitResolver{domain: name}
	q := &httpBitQueue{}
	fail := &httpBitRecordFailPool{Pool: pool}
	enqueue := func() {
		t.Helper()
		eres, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
			{ZoneID: zone, DomainID: id, Name: name},
		})
		if err != nil || eres.Queued != 1 || eres.Published != 1 {
			t.Fatalf("EnqueueBatch = %+v err=%v, хочу Queued=Published=1 (Р1)", eres, err)
		}
	}

	// The "other bit" is_idn (unrelated to the counters, unlike
	// render_pending): the B1–B3 specification requires preserving it —
	// the OR/AND ~bit statements do not touch foreign bits.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET flags = flags | $1 WHERE zone_id = $2 AND id = $3",
		int16(model.FlagIDN), zone, id); err != nil {
		t.Fatalf("сеем is_idn: %v", err)
	}

	// A manual domain insert bypassing the incremental deltas — the
	// reconciliation is the authority (the baseline of the zone counters,
	// the TestCheckState_LeaseNullRepair style).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	// R1: scheduler enqueueing — queued, the bit is clean (no B1 yet),
	// is_idn survived the enqueueing.
	enqueue()
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) || r.httpPublished {
		t.Fatalf("после постановки = %+v, хочу queued/бит чист (Р1)", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags после постановки = %b, хочу только is_idn (прочие биты сохранены)", f)
	}
	countersVerifyEmpty(t, pool, "после постановки (Р1)")

	// Delivery 1 — the "XADD without the bit" window: the q:http
	// publication went through (XADD succeeded), the worker died BEFORE the
	// mark and the write. The previous attempt is emulated by hand (the
	// TestDNSWorker_RedeliveryPublishedMarker style): neither the mark
	// Exec nor the write happened.
	if _, err := q.Publish(ctx, queue.StreamHTTP, queue.HTTPTask{
		ZoneID: zone, DomainID: id, Name: name,
		Subdomains: []queue.HTTPSubdomain{{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}}},
	}); err != nil {
		t.Fatalf("публикация q:http доставки 1: %v", err)
	}
	if q.http != 1 {
		t.Fatalf("публикаций q:http = %d, хочу 1", q.http)
	}

	// Delivery 2 (redelivery): the bit is CLEAN — the window is not
	// closed, the filter does not see the mark, the episode publishes a
	// DUPLICATE q:http (allowed — the price of the window, not a loss) and
	// sets the bit (B1); the write fails via the wrapper — the delivery is
	// not acknowledged.
	dom := httpBitFilter(ctx, t, pool, batch)
	if dom.HTTPPublished {
		t.Fatal("бит стоит в окне «XADD без бита» — дубль был бы подавлен (нарушение спецификации)")
	}
	if err := dnsworker.ProcessDomain(ctx, fail, q, resolve, dom, subs, false, nil, nil); err == nil {
		t.Fatal("ProcessDomain доставки 2 = nil, хочу ошибку записи (сценарий сбоя до Б2)")
	}
	if q.http != 2 {
		t.Fatalf("публикаций q:http = %d, хочу 2 (дубль окна допустим)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) || !r.httpPublished {
		t.Fatalf("после Б1 = %+v, хочу queued/бит установлен (постановка после XADD)", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN|model.FlagHTTPPublished) {
		t.Fatalf("flags после Б1 = %b, хочу is_idn|http_published (OR сохраняет прочие)", f)
	}
	countersVerifyEmpty(t, pool, "после Б1 (запись не состоялась — дельт нет)")

	// Delivery 3 (redelivery): the bit is set ("published, not written") —
	// the publication is SKIPPED (B4), the episode reaches the write:
	// done, the bit is cleared (B2), the status is untouched (the "there
	// are addresses" branch — the verdict belongs to the HTTP stage), the
	// episode mask is written.
	dom = httpBitFilter(ctx, t, pool, batch)
	if !dom.HTTPPublished {
		t.Fatal("бит http_published не прочитан фильтром пачки (Б4)")
	}
	if err := dnsworker.ProcessDomain(ctx, pool, q, resolve, dom, subs, false, nil, nil); err != nil {
		t.Fatalf("ProcessDomain доставки 3: %v", err)
	}
	if q.http != 2 {
		t.Fatalf("публикаций q:http = %d, хочу 2 (Б4: дубль исключён)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) ||
		r.httpPublished || !r.lastChecked || r.status != int16(model.StatusNew) {
		t.Fatalf("после Б2 = %+v, хочу done/бит снят/отметка есть/статус не тронут", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags после Б2 = %b, хочу только is_idn (маска ~бита сохраняет прочие)", f)
	}
	var mask int64
	if err := pool.QueryRow(ctx,
		"SELECT dns_mask FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
	).Scan(&mask); err != nil {
		t.Fatalf("чтение dns_mask: %v", err)
	}
	if mask != 0b11 {
		t.Fatalf("dns_mask = %b, хочу 0b11 (апекс и www резолвятся)", mask)
	}
	countersVerifyEmpty(t, pool, "после записи результата (Б2)")

	// Episode 2: the daily recheck (R7) returns the domain, enqueueing
	// (R1) gives a fresh queued with a clean bit.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 400 WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст проверки: %v", err)
	}
	rk, err := scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Interval: 300 * 24 * time.Hour})
	if err != nil || rk.Domains != 1 {
		t.Fatalf("RecheckOnce = %+v err=%v, хочу 1 (Р7)", rk, err)
	}
	enqueue()
	countersVerifyEmpty(t, pool, "после Р7+Р1 эпизода 2")

	// Delivery 4: the episode publishes q:http and sets the bit (B1), the
	// write fails (the wrapper), the lease expires — the lease return (R5)
	// leaves the row pending with the SURVIVING bit: outside an episode
	// the bit does not matter, only the batch filter reads it.
	dom = httpBitFilter(ctx, t, pool, batch)
	if dom.HTTPPublished {
		t.Fatal("бит стоит до эпизода 2 — постановка не сняла (нарушение Б3)")
	}
	if err := dnsworker.ProcessDomain(ctx, fail, q, resolve, dom, subs, false, nil, nil); err == nil {
		t.Fatal("ProcessDomain доставки 4 = nil, хочу ошибку записи (сценарий сбоя до Б2)")
	}
	if q.http != 3 {
		t.Fatalf("публикаций q:http = %d, хочу 3 (Б1 эпизода 2)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) || !r.httpPublished {
		t.Fatalf("после Б1 эпизода 2 = %+v, хочу queued/бит установлен", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN|model.FlagHTTPPublished) {
		t.Fatalf("flags после Б1 эпизода 2 = %b, хочу is_idn|http_published", f)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET queued_at = now() - interval '2 hours' WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст аренды: %v", err)
	}
	lres, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil || lres.Domains != 1 {
		t.Fatalf("LeaseOnce = %+v err=%v, хочу 1 (Р5)", lres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) || !r.httpPublished {
		t.Fatalf("после lease-возврата = %+v, хочу pending/бит пережил (вне эпизода)", r)
	}
	countersVerifyEmpty(t, pool, "после lease-возврата (Р5)")

	// B3: enqueueing a new episode clears the bit — otherwise the previous
	// episode's mark would suppress publishing the fresh one (a lost
	// q:http).
	enqueue()
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) || r.httpPublished {
		t.Fatalf("после Б3 = %+v, хочу queued/бит снят постановкой планировщика", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags после Б3 = %b, хочу только is_idn (маска ~бита сохраняет прочие)", f)
	}
	countersVerifyEmpty(t, pool, "после Б3")

	// Delivery 5: a fresh episode with a clean bit publishes q:http WITHOUT
	// suppression (the point of B3) and finishes with the write (B2).
	dom = httpBitFilter(ctx, t, pool, batch)
	if dom.HTTPPublished {
		t.Fatal("бит стоит после постановки — публикация эпизода была бы подавлена (нарушение Б3)")
	}
	if err := dnsworker.ProcessDomain(ctx, pool, q, resolve, dom, subs, false, nil, nil); err != nil {
		t.Fatalf("ProcessDomain доставки 5: %v", err)
	}
	if q.http != 4 {
		t.Fatalf("публикаций q:http = %d, хочу 4 (свежий эпизод публикует после Б3)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) || r.httpPublished {
		t.Fatalf("после завершения эпизода 2 = %+v, хочу done/бит снят", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags после эпизода 2 = %b, хочу только is_idn", f)
	}
	countersVerifyEmpty(t, pool, "после завершения эпизода 2")
}

// TestHTTPPublishedBit_MarkFailureNonFatal — the B1 bit-setting failure
// does not kill the episode: the publication already happened (XADD went
// through), the result write carries the domain to done (the B1
// specification — non-fatal, a Warn instead of an error); a q:http
// duplicate is impossible — done is cut off by the batch filter, the "XADD
// without the bit" window is closed by the write; other bits are
// preserved.
func TestHTTPPublishedBit_MarkFailureNonFatal(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "fsm-bit-nf.test")
	// countersSeedDomains numbers the labels: prefix "spec" → label "spec0".
	ids := countersSeedDomains(t, pool, zone, "spec", 1, model.CheckStateQueued, model.StatusNew)
	id := ids[0]
	const name = "spec0.fsm-bit-nf.test"
	// The row as after R1 (queued, the lease started) with the "other bit"
	// is_idn.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET flags = flags | $1, queued_at = now() WHERE zone_id = $2 AND id = $3",
		int16(model.FlagIDN), zone, id); err != nil {
		t.Fatalf("сеем is_idn/queued_at: %v", err)
	}
	// A manual insert bypassing the deltas — the reconciliation is the
	// authority (the baseline).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	resolve := httpBitResolver{domain: name}
	q := &httpBitQueue{}
	fail := &httpBitMarkFailPool{Pool: pool}
	dom := httpBitFilter(ctx, t, pool, []queue.DNSDomain{{ZoneID: zone, DomainID: id, Name: name}})
	if err := dnsworker.ProcessDomain(ctx, fail, q, resolve, dom, httpBitSubs(), false, nil, nil); err != nil {
		t.Fatalf("ProcessDomain = %v, хочу nil (пометка non-fatal — эпизод доведён до записи)", err)
	}
	if q.http != 1 {
		t.Fatalf("публикаций q:http = %d, хочу 1 (XADD прошёл до отказа пометки)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) ||
		r.httpPublished || !r.lastChecked {
		t.Fatalf("после эпизода = %+v, хочу done/бит чист/отметка есть", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags = %b, хочу только is_idn (прочие биты сохранены)", f)
	}
	countersVerifyEmpty(t, pool, "после эпизода с отказом пометки")
}

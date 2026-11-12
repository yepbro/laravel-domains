// Integration tests of the pipeline's incremental counters (T-178,
// incremental-counters spec §12.2) against live PostgreSQL (an isolated
// schema with migrations, the e2e_test.go style): transitions by the real
// package functions (scheduler/httpworker/ingest) maintain the deltas; the
// ready verify logic of monitor cross-checks the counters against live
// aggregates after each episode; the record duplicate-delivery scenario
// (writing one domain again) — the deltas are not double-counted;
// deleting a queued domain decrements the right column; an artificial
// drift (a manual UPDATE zones) is caught by verify and fixed by recount.
// Run: make test-integration.
package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/analyzer"
	"crawler/internal/content"
	"crawler/internal/dnsworker"
	"crawler/internal/httpworker"
	"crawler/internal/ingest"
	"crawler/internal/migrate"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/renderworker"
	"crawler/internal/rules"
	"crawler/internal/scheduler"
	"crawler/internal/status"
)

// countersSchema — an isolated schema with migrations (no pg_stat_activity
// label: connection drops are e2e territory).
func countersSchema(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// countersStubQueue — a queue.Queue for enqueueing: publishing always
// succeeds (the DB side of the transitions is tested, not Redis).
type countersStubQueue struct{ published int }

func (q *countersStubQueue) Publish(context.Context, string, queue.Message) (string, error) {
	q.published++
	return "id-1", nil
}

func (q *countersStubQueue) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	panic("countersStubQueue: Consume не используется")
}

func (q *countersStubQueue) Ack(context.Context, string, string, ...string) error {
	panic("countersStubQueue: Ack не используется")
}

// countersSeedZone — a reference table zone; returns the id.
func countersSeedZone(t *testing.T, pool *pgxpool.Pool, name string) int16 {
	t.Helper()
	var id int16
	if err := pool.QueryRow(context.Background(),
		"INSERT INTO zones (name, is_active) VALUES ($1, true) RETURNING id", name,
	).Scan(&id); err != nil {
		t.Fatalf("зона %s: %v", name, err)
	}
	return id
}

// countersSeedDomains — inserts domains with a state/status; the prefix
// distinguishes batches of one zone (UNIQUE (zone_id, label), review R2
// P2-1); returns the ids in order.
func countersSeedDomains(t *testing.T, pool *pgxpool.Pool, zone int16, prefix string, n int, state model.CheckState, status model.Status) []int64 {
	t.Helper()
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		var id int64
		if err := pool.QueryRow(context.Background(),
			"INSERT INTO domains (zone_id, label, check_state, status) VALUES ($1, $2, $3, $4) RETURNING id",
			zone, fmt.Sprintf("%s%d", prefix, i), int16(state), int16(status),
		).Scan(&id); err != nil {
			t.Fatalf("домен %s%d: %v", prefix, i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// countersVerifyEmpty — the counters vs live aggregates cross-check must
// converge (tolerance 0): any position is a test error with the episode
// name.
func countersVerifyEmpty(t *testing.T, pool *pgxpool.Pool, what string) {
	t.Helper()
	mismatch, err := monitor.VerifyCounters(context.Background(), pool, monitor.VerifyOptions{})
	if err != nil {
		t.Fatalf("%s: VerifyCounters: %v", what, err)
	}
	if len(mismatch) > 0 {
		t.Fatalf("%s: дрейф счётчиков после эпизода: %+v", what, mismatch)
	}
}

// TestCounters_PipelineTransitions — transitions by the real package
// statements (enqueue pending→queued, lease queued→pending, recheck
// done→pending) maintain the zone deltas; after each episode the counters
// converge with the live aggregates. The first reconciliation pass fills
// the zeros, a repeat one has zero drift (idempotence), the global
// counters have the full model.Statuses legend.
func TestCounters_PipelineTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()
	q := &countersStubQueue{}

	zone := countersSeedZone(t, pool, "counters.test")
	pending := countersSeedDomains(t, pool, zone, "p", 5, model.CheckStatePending, model.StatusNew)
	countersSeedDomains(t, pool, zone, "d", 3, model.CheckStateDone, model.StatusAlive)

	// The first reconciliation pass fills the zeros; a repeat one has zero
	// drift (idempotence).
	first, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("первый проход: %v", err)
	}
	if first.Zones != 1 {
		t.Fatalf("первый проход: зон %d, хочу 1", first.Zones)
	}
	second, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("повторный проход: %v", err)
	}
	for _, p := range second.Positions {
		if p.Drift() != 0 {
			t.Fatalf("повторный проход: дрейф %s = %d→%d (проход идемпотентен)", p.Counter, p.Old, p.New)
		}
	}
	// The global counters legend is in the crawler_counters rows (the
	// drift positions do not include the status rows: they are written by
	// one reconciliation, R1).
	{
		rows, err := pool.Query(ctx, "SELECT name FROM crawler_counters")
		if err != nil {
			t.Fatalf("чтение счётчиков: %v", err)
		}
		names := map[string]bool{}
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatalf("чтение счётчиков: строка: %v", err)
			}
			names[n] = true
		}
		rows.Close()
		for _, want := range monitor.CounterNames() {
			if !names[want] {
				t.Fatalf("легенда глобальных счётчиков без %s", want)
			}
		}
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")

	// enqueue: pending → queued (the deltas pending −5 / queued +5).
	batch := make([]scheduler.DispatchRow, 0, len(pending))
	for i, id := range pending {
		batch = append(batch, scheduler.DispatchRow{ZoneID: zone, DomainID: id, Name: fmt.Sprintf("p%d.counters.test", i)})
	}
	res, err := scheduler.EnqueueBatch(ctx, pool, q, batch)
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if res.Queued != len(pending) {
		t.Fatalf("EnqueueBatch: переведено %d, хочу %d", res.Queued, len(pending))
	}
	countersVerifyEmpty(t, pool, "после enqueue")

	// lease: an expired lease returns the changed rows to pending
	// (the default Timeout is 30 minutes — shift queued_at into the past).
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET queued_at = now() - interval '2 hours' WHERE zone_id = $1 AND check_state = $2",
		zone, int16(model.CheckStateQueued)); err != nil {
		t.Fatalf("возраст аренды: %v", err)
	}
	leaseRes, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if leaseRes.Domains != int64(len(pending)) {
		t.Fatalf("LeaseOnce: возвращено %d, хочу %d", leaseRes.Domains, len(pending))
	}
	countersVerifyEmpty(t, pool, "после lease")

	// recheck (the age-based recheck): the done rows are old — the change
	// checked −n / pending +n.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 400 WHERE zone_id = $1 AND check_state = $2",
		zone, int16(model.CheckStateDone)); err != nil {
		t.Fatalf("возраст done-строк: %v", err)
	}
	rk, err := scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Interval: 300 * 24 * time.Hour})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}
	if rk.Domains != 3 {
		t.Fatalf("RecheckOnce: переведено %d, хочу 3", rk.Domains)
	}
	countersVerifyEmpty(t, pool, "после recheck")
}

// TestCounters_RecordDoubleDelivery — writing the result of an HTTP
// episode over done (the DNS stage already moved the domain, T-178: DNS is
// the main queued → done transition) and its redelivery: neither write has
// zone deltas (the row's old state is done, incremental-counters spec
// §6.4); the daily checks counter (T-179) counts write events — +1 per
// delivery; the reconciliation brings the row to the live window (one
// domain), the daily:checks drift is observable in the pass positions; the
// cross-check converges (the daily row is not part of verify — an accepted
// semantic divergence, TZ_slow_done_today §5.1).
func TestCounters_RecordDoubleDelivery(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "record.test")
	ids := countersSeedDomains(t, pool, zone, "q", 1, model.CheckStateQueued, model.StatusNew)
	if _, err := pool.Exec(ctx,
		"UPDATE zones SET domains_total = 1, domains_queued = 1 WHERE id = $1", zone); err != nil {
		t.Fatalf("счётчики зоны: %v", err)
	}

	// A DNS episode moves queued → done (no terminal status needed: the
	// "there are addresses" branch — the verdict belongs to the HTTP
	// stage).
	ok, err := dnsworker.RecordEpisode(ctx, pool, zone, ids[0], dnsworker.EpisodeResult{Mask: 1}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode: ok=%v err=%v", ok, err)
	}
	countersVerifyEmpty(t, pool, "после DNS-перехода")

	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	res := &httpworker.DomainResult{ZoneID: zone, DomainID: ids[0], Name: "q0.record.test"}
	for i := 0; i < 2; i++ { // a duplicate delivery over done — no zone deltas
		ok, err := rec.RecordAndPublish(ctx, res)
		if err != nil || !ok {
			t.Fatalf("запись %d: ok=%v err=%v", i+1, ok, err)
		}
	}
	var queued, checked int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_queued, domains_checked FROM zones WHERE id = $1", zone,
	).Scan(&queued, &checked); err != nil {
		t.Fatalf("чтение зоны: %v", err)
	}
	if queued != 0 || checked != 1 {
		t.Fatalf("после record×2 queued=%d checked=%d, хочу 0/1 (дельта один раз — на DNS-переходе)", queued, checked)
	}

	// The daily counter (T-179): two result writes — two events (the delta
	// is not doubled per delivery: one delivery is one increment);
	// the live window — one domain with last_checked_at = current_date.
	if got := countersChecksToday(t, pool); got != 2 {
		t.Fatalf("после record×2 checks:<день> = %d, хочу 2 (по одному событию на запись)", got)
	}
	countersVerifyEmpty(t, pool, "после record×2 поверх done (суточная строка вне verify)")

	// The reconciliation is the authority: the day row := the live window
	// (1), the |2 − 1| drift is visible in the pass's daily:checks
	// position.
	pass, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if got := countersChecksToday(t, pool); got != 1 {
		t.Fatalf("после реконсиляции checks:<день> = %d, хочу 1 (живое окно: один домен сегодня)", got)
	}
	drifted := false
	for _, p := range pass.Positions {
		if p.Counter != "daily:checks" {
			continue
		}
		drifted = true
		if p.Old != 2 || p.New != 1 || p.Drift() != 1 {
			t.Fatalf("daily:checks = %+v, хочу old=2 new=1 drift=1", p)
		}
	}
	if !drifted {
		t.Fatal("позиция daily:checks отсутствует в итогах прохода")
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")
}

// countersChecksToday — the value of the current day's daily checks counter
// row (the name is computed by the DB — the same current_date as the
// increment and the reconciliation window); a missing row is a test error.
func countersChecksToday(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	return countersDailyToday(t, pool, "checks")
}

// countersTempToday — the value of the current day's daily temp_error
// numerator row (T-182); a missing row is a test error.
func countersTempToday(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	return countersDailyToday(t, pool, "temp_error")
}

// countersRetryMatureToday — the value of today's retry maturity bucket
// (T-202); a missing row is a test error.
func countersRetryMatureToday(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	return countersDailyToday(t, pool, "retry_mature")
}

// countersDailyToday — the value of the daily <prefix>:YYYY-MM-DD row.
func countersDailyToday(t *testing.T, pool *pgxpool.Pool, prefix string) int64 {
	t.Helper()
	var v int64
	err := pool.QueryRow(context.Background(),
		"SELECT value FROM crawler_counters WHERE name = $1 || to_char(current_date, 'YYYY-MM-DD')",
		prefix+":",
	).Scan(&v)
	if err != nil {
		t.Fatalf("чтение %s:<день>: %v", prefix, err)
	}
	return v
}

// TestCounters_DNSTempErrorShare — the daily temp_error numerator (T-182,
// TZ_slow_dns_share §8.2): a temp_error episode grows the numerator and the
// denominator together; a repeat episode without temp_error — the
// denominator only; a temp_error episode duplicate — both again (the share
// is robust to duplicates); the reconciliation brings both rows to the
// live window (the share is computed over rows — the day-window scan from
// the alert is gone); the daily:checks/daily:temp_error positions observe
// the semantic difference; verify does not cross-check the daily rows.
func TestCounters_DNSTempErrorShare(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "share.test")
	ids := countersSeedDomains(t, pool, zone, "s", 2, model.CheckStateQueued, model.StatusNew)
	// The starting pass — the zone columns from the live aggregate (2
	// queued), the day's daily rows as zeros (no domains checked today).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("стартовый проход: %v", err)
	}
	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	// A temp_error episode: no records, all checks are timeouts (the
	// temp_error verdict, §5.5 item 9).
	tempRes := &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[0], Name: "s0.share.test",
		Subdomains: []httpworker.SubdomainResult{{
			SubdomainID: 0,
			HTTPS:       httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
			HTTP:        httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
		}},
	}
	if ok, err := rec.RecordAndPublish(ctx, tempRes); err != nil || !ok {
		t.Fatalf("temp_error-эпизод: ok=%v err=%v", ok, err)
	}
	if got := countersChecksToday(t, pool); got != 1 {
		t.Fatalf("после temp_error-эпизода checks:<день> = %d, хочу 1", got)
	}
	if got := countersTempToday(t, pool); got != 1 {
		t.Fatalf("после temp_error-эпизода temp_error:<день> = %d, хочу 1", got)
	}

	// An episode without temp_error (permanently silent — no_http): the
	// denominator grows, the numerator untouched.
	silentRes := &httpworker.DomainResult{ZoneID: zone, DomainID: ids[1], Name: "s1.share.test"}
	if ok, err := rec.RecordAndPublish(ctx, silentRes); err != nil || !ok {
		t.Fatalf("no_http-эпизод: ok=%v err=%v", ok, err)
	}
	if got := countersChecksToday(t, pool); got != 2 {
		t.Fatalf("после no_http-эпизода checks:<день> = %d, хочу 2", got)
	}
	if got := countersTempToday(t, pool); got != 1 {
		t.Fatalf("после no_http-эпизода temp_error:<день> = %d, хочу 1 (не temp_error-итог не инкрементирует)", got)
	}

	// A duplicate delivery of the temp_error episode: +1 to the numerator
	// and the denominator at once — the share would not change, the write
	// events are honest.
	if ok, err := rec.RecordAndPublish(ctx, tempRes); err != nil || !ok {
		t.Fatalf("дубль temp_error-эпизода: ok=%v err=%v", ok, err)
	}
	if got := countersChecksToday(t, pool); got != 3 {
		t.Fatalf("после дубля checks:<день> = %d, хочу 3", got)
	}
	if got := countersTempToday(t, pool); got != 2 {
		t.Fatalf("после дубля temp_error:<день> = %d, хочу 2", got)
	}
	countersVerifyEmpty(t, pool, "после эпизодов доли (суточные строки вне verify)")

	// The reconciliation is the authority: the live window is 2 domains of
	// the day (one has the temp_error status), the share 1/2; the
	// increments gave 3/2 (the duplicate) — the daily:* positions observe
	// the difference.
	pass, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if got := countersChecksToday(t, pool); got != 2 {
		t.Fatalf("после реконсиляции checks:<день> = %d, хочу 2 (живое окно: два домена сегодня)", got)
	}
	if got := countersTempToday(t, pool); got != 1 {
		t.Fatalf("после реконсиляции temp_error:<день> = %d, хочу 1 (один temp_error-домен дня)", got)
	}
	byName := map[string]monitor.CounterPosition{}
	for _, p := range pass.Positions {
		byName[p.Counter] = p
	}
	if p := byName["daily:checks"]; p.Old != 3 || p.New != 2 || p.Drift() != 1 {
		t.Fatalf("daily:checks = %+v, хочу old=3 new=2 drift=1", p)
	}
	if p := byName["daily:temp_error"]; p.Old != 2 || p.New != 1 || p.Drift() != 1 {
		t.Fatalf("daily:temp_error = %+v, хочу old=2 new=1 drift=1 (дубль события)", p)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции доли")
}

// TestCounters_RetryMatureBaskets — the retry maturity buckets (T-202):
// temp_error episodes of BOTH pipeline branches (the HTTP verdict and the
// terminal DNS-stage temp_error branch — a bulk DNS incident arrives
// through it) increment today's bucket with the same key; verdicts without
// temp_error do not touch the bucket; a duplicate delivery is one more
// event (the streaming semantics, like the daily rows); the reconciliation
// writes the bucket from the live window (temp_error × done × date), the
// retry_mature:today position observes the semantic difference. The guard
// reads only the buckets of past days — today's does not open the heavy
// selection.
func TestCounters_RetryMatureBaskets(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "mature.test")
	ids := countersSeedDomains(t, pool, zone, "m", 3, model.CheckStateQueued, model.StatusNew)
	// The starting pass creates the buckets as zeros (no temp_error
	// episodes today yet).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("стартовый проход: %v", err)
	}
	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	// An HTTP temp_error episode (all checks are timeouts): the day bucket +1.
	tempRes := &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[0], Name: "m0.mature.test",
		Subdomains: []httpworker.SubdomainResult{{
			SubdomainID: 0,
			HTTPS:       httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
			HTTP:        httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
		}},
	}
	if ok, err := rec.RecordAndPublish(ctx, tempRes); err != nil || !ok {
		t.Fatalf("temp_error-эпизод HTTP: ok=%v err=%v", ok, err)
	}
	if got := countersRetryMatureToday(t, pool); got != 1 {
		t.Fatalf("после temp_error-эпизода HTTP retry_mature:<день> = %d, хочу 1", got)
	}

	// A no_http episode: does not touch the bucket — only temp_error verdicts ripen.
	silentRes := &httpworker.DomainResult{ZoneID: zone, DomainID: ids[1], Name: "m1.mature.test"}
	if ok, err := rec.RecordAndPublish(ctx, silentRes); err != nil || !ok {
		t.Fatalf("no_http-эпизод: ok=%v err=%v", ok, err)
	}
	if got := countersRetryMatureToday(t, pool); got != 1 {
		t.Fatalf("после no_http-эпизода retry_mature:<день> = %d, хочу 1", got)
	}

	// The terminal DNS-stage temp_error branch (an apex SERVFAIL): the same
	// bucket key — a separate autocommit statement AFTER the write
	// (T-267; formerly — a CTE of the same statement, T-202).
	if ok, err := dnsworker.RecordEpisode(ctx, pool, zone, ids[2], dnsworker.EpisodeResult{
		SetStatus: true,
		Status:    model.StatusTempError,
	}, nil); err != nil || !ok {
		t.Fatalf("temp_error-эпизод DNS: ok=%v err=%v", ok, err)
	}
	if got := countersRetryMatureToday(t, pool); got != 2 {
		t.Fatalf("после temp_error-эпизода DNS retry_mature:<день> = %d, хочу 2", got)
	}

	// A duplicate delivery of the HTTP temp_error episode: one more event
	// (the live window will count 2 domains — the reconciliation brings
	// the semantic difference together).
	if ok, err := rec.RecordAndPublish(ctx, tempRes); err != nil || !ok {
		t.Fatalf("дубль temp_error-эпизода: ok=%v err=%v", ok, err)
	}
	if got := countersRetryMatureToday(t, pool); got != 3 {
		t.Fatalf("после дубля retry_mature:<день> = %d, хочу 3", got)
	}
	countersVerifyEmpty(t, pool, "после эпизодов (корзины вне verify)")

	// The retry heavy-selection guard: today's bucket is NOT in the sum
	// (episodes ripen by midnight) — nothing mature, the sum is 0.
	var mature int64
	if err := pool.QueryRow(ctx,
		"SELECT coalesce(sum(value), 0) FROM crawler_counters "+
			"WHERE name >= 'retry_mature:' AND name < 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')",
	).Scan(&mature); err != nil {
		t.Fatalf("guard-сумма корзин: %v", err)
	}
	if mature != 0 {
		t.Fatalf("сумма зрелых корзин = %d, хочу 0 (сегодняшние эпизоды незрелые)", mature)
	}

	// The reconciliation is the authority: the day bucket := the live
	// window (temp_error × done × the current date — two domains), the
	// retry_mature:today position observes the difference (a duplicate
	// event).
	pass, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if got := countersRetryMatureToday(t, pool); got != 2 {
		t.Fatalf("после реконсиляции retry_mature:<день> = %d, хочу 2 (живое окно: два temp_error-домена сегодня)", got)
	}
	byName := map[string]monitor.CounterPosition{}
	for _, p := range pass.Positions {
		byName[p.Counter] = p
	}
	if p := byName["retry_mature:today"]; p.Old != 3 || p.New != 2 || p.Drift() != 1 {
		t.Fatalf("retry_mature:today = %+v, хочу old=3 new=2 drift=1 (дубль события)", p)
	}
	if p := byName["retry_mature:past"]; p.Old != 0 || p.New != 0 || p.Drift() != 0 {
		t.Fatalf("retry_mature:past = %+v, хочу нули (зрелой когорты нет)", p)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции корзин")
}

// countersMatureFailPool — a db.DB wrapper of the pool: the retry_mature
// bucket increment Exec (dnsworker resultMatureIncSQL, T-267) fails,
// everything else is delegated to the pool — a failure BETWEEN the result
// write statements (the transition is already committed, the increment is
// not).
type countersMatureFailPool struct {
	*pgxpool.Pool
}

// countersMatureIncMark — the identifier of the bucket increment in the SQL
// text (a literal duplication — the schema_test.go style: the constant is
// not exported).
const countersMatureIncMark = "VALUES ('retry_mature:' || to_char(current_date, 'YYYY-MM-DD'), 1, now())"

func (p *countersMatureFailPool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return p.Pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *countersMatureFailPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, countersMatureIncMark) {
		return pgconn.CommandTag{}, errors.New("сбой инкремента корзины (сценарий)")
	}
	return p.Pool.Exec(ctx, sql, args...)
}

func (p *countersMatureFailPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.Pool.Query(ctx, sql, args...)
}

// TestCounters_RetryMatureIncrementFailure — the ±1 non-atomicity window
// (T-267): the retry_mature:today bucket increment is a separate
// autocommit statement AFTER the transition write; its failure leaves the
// domain written (done + temp_error) and the bucket undercounted until the
// nearest reconciliation — the monitor pass writes the bucket from the
// live window (temp_error × done × date) and evens it out; the retry guard
// is not affected (it reads only the buckets of past days).
func TestCounters_RetryMatureIncrementFailure(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "maturefail.test")
	ids := countersSeedDomains(t, pool, zone, "mf", 1, model.CheckStateQueued, model.StatusNew)

	fail := &countersMatureFailPool{Pool: pool}
	recorded, err := dnsworker.RecordEpisode(ctx, fail, zone, ids[0], dnsworker.EpisodeResult{
		SetStatus: true,
		Status:    model.StatusTempError,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "retry_mature bucket increment") {
		t.Fatalf("RecordEpisode = (%v, %v), want (true, ошибка инкремента)", recorded, err)
	}
	if !recorded {
		t.Fatal("recorded = false, want true: переход зафиксирован до сбоя инкремента")
	}

	// The transition is written: the domain done + temp_error, the zone deltas honest.
	var status, state int16
	if err := pool.QueryRow(ctx,
		"SELECT status, check_state FROM domains WHERE zone_id = $1 AND id = $2", zone, ids[0],
	).Scan(&status, &state); err != nil {
		t.Fatalf("чтение домена: %v", err)
	}
	if model.Status(status) != model.StatusTempError || model.CheckState(state) != model.CheckStateDone {
		t.Fatalf("домен = статус %d состояние %d, want temp_error/done (переход пережил сбой инкремента)",
			status, state)
	}

	// The bucket is undercounted by 1 (the increment failed) — the live
	// window sees one temp_error today. The bucket row may not exist at
	// all: resultMatureIncSQL is an upsert and the row's creator, and the
	// fail pool intercepted it — ErrNoRows is the bucket 0 (the
	// undercount until the reconciliation, in terms of a missing
	// increment).
	var basket int64
	err = pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters "+
			"WHERE name = 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')",
	).Scan(&basket)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		basket = 0 // no row — the bucket was not created (the increment failed)
	case err != nil:
		t.Fatalf("чтение корзины суток: %v", err)
	}
	if basket != 0 {
		t.Fatalf("корзина суток = %d, want 0 (инкремент упал — окно ±1 до реконсиляции)", basket)
	}

	// The reconciliation is the authority: the bucket := the live window
	// (one temp_error × done today), the undercount is caught up.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters "+
			"WHERE name = 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')",
	).Scan(&basket); err != nil {
		t.Fatalf("чтение корзины суток после прохода: %v", err)
	}
	if basket != 1 {
		t.Fatalf("корзина суток после реконсиляции = %d, want 1 (живое окно догнало сбой инкремента)", basket)
	}
}

// TestCounters_DailyChecksDayChange — the day rollover (T-179, spec §9.2):
// emulating a date change with a fixed connection time zone (current_date
// is derived from the session TimeZone): writes "on different days"
// maintain different checks: rows, yesterday's stays as throughput history,
// the live window of each day counts only its writes. A pool with
// MaxConns=1 — SET TimeZone affects the single connection, the write
// transactions see it.
func TestCounters_DailyChecksDayChange(t *testing.T) {
	skipUnlessIntegration(t)
	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("миграции: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("разбор DSN: %v", err)
	}
	cfg.MaxConns = 1 // the TimeZone is session-level: all statements — one connection
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("пул: %v", err)
	}
	t.Cleanup(pool.Close)
	ctx := context.Background()

	// Pacific/Midway (UTC−11) and Pacific/Kiritimati (UTC+14) —
	// spread current_date by at least a day at any moment of the run
	// (25 hours between the zones; no DST in either). set_config is the
	// parameterizable analog of SET TIME ZONE (SET itself takes no
	// parameters), is_local=false — the session level.
	const (
		tzDay1 = "Pacific/Midway"
		tzDay2 = "Pacific/Kiritimati"
	)
	setTZ := func(tz string) {
		t.Helper()
		if _, err := pool.Exec(ctx, "SELECT set_config('TimeZone', $1, false)", tz); err != nil {
			t.Fatalf("таймзона %s: %v", tz, err)
		}
	}
	var day1, day2 string
	readDate := func(tz string) string {
		setTZ(tz)
		var d string
		if err := pool.QueryRow(ctx, "SELECT to_char(current_date, 'YYYY-MM-DD')").Scan(&d); err != nil {
			t.Fatalf("current_date (%s): %v", tz, err)
		}
		return d
	}
	day1 = readDate(tzDay1)
	day2 = readDate(tzDay2)
	if day1 == day2 {
		t.Fatalf("таймзоны не развели сутки: %s == %s (прогон в окне ±14ч вокруг полуночи — повторить)", day1, day2)
	}

	zone := countersSeedZone(t, pool, "days.test")
	ids := countersSeedDomains(t, pool, zone, "d", 2, model.CheckStateDone, model.StatusNew)
	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	checksRow := func(day string) (int64, bool) {
		var v int64
		err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = $1", "checks:"+day).Scan(&v)
		if err != nil {
			return 0, false
		}
		return v, true
	}

	// Day 1: the first domain is written — the day row is created by the increment.
	setTZ(tzDay1)
	if ok, err := rec.RecordAndPublish(ctx, &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[0], Name: "d0.days.test",
	}); err != nil || !ok {
		t.Fatalf("запись дня 1: ok=%v err=%v", ok, err)
	}
	if v, ok := checksRow(day1); !ok || v != 1 {
		t.Fatalf("checks:%s = (%d, %v), хочу (1, true) — строку создаёт первый инкремент суток", day1, v, ok)
	}

	// Day 2 (for the connection): the second domain — a new row,
	// yesterday's untouched.
	setTZ(tzDay2)
	if ok, err := rec.RecordAndPublish(ctx, &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[1], Name: "d1.days.test",
	}); err != nil || !ok {
		t.Fatalf("запись дня 2: ok=%v err=%v", ok, err)
	}
	if v, ok := checksRow(day2); !ok || v != 1 {
		t.Fatalf("checks:%s = (%d, %v), хочу (1, true) — новая строка нового дня", day2, v, ok)
	}
	if v, ok := checksRow(day1); !ok || v != 1 {
		t.Fatalf("checks:%s = (%d, %v) после записи дня 2, хочу (1, true) — история не тронута", day1, v, ok)
	}

	// The "day 2" reconciliation: the current_date window sees only the day
	// 2 domain (the day 1 last_checked_at is outside the window), the day 2
	// row converges, the pass does not overwrite day 1.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if v, _ := checksRow(day2); v != 1 {
		t.Fatalf("после реконсиляции checks:%s = %d, хочу 1", day2, v)
	}
	if v, _ := checksRow(day1); v != 1 {
		t.Fatalf("реконсиляция тронула вчерашнюю строку checks:%s = %d, хочу 1 (история)", day1, v)
	}
}

// TestCounters_DNSStageTransitions — the pipeline's main transition
// queued/pending → done maintains the deltas in all DNS-stage branches
// (a terminal status, "there are addresses", the invalid diagnosis),
// including a domain with a lease expired in the middle of the episode
// (pending) and a redelivery (done — an idempotent no-op, review P1-1).
func TestCounters_DNSStageTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "dns.test")
	ids := countersSeedDomains(t, pool, zone, "p", 4, model.CheckStatePending, model.StatusNew)
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	// Enqueueing: 4 × pending → queued.
	q := &countersStubQueue{}
	batch := make([]scheduler.DispatchRow, len(ids))
	for i, id := range ids {
		batch[i] = scheduler.DispatchRow{ZoneID: zone, DomainID: id, Name: fmt.Sprintf("p%d.dns.test", i)}
	}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	countersVerifyEmpty(t, pool, "после enqueue")

	// d0 "returned by the lease" in the middle of the episode: pending →
	// done — the delta pending −1/checked +1 (pre-reading the old state,
	// incremental-counters spec §6.1.2).
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET check_state = 0, queued_at = NULL WHERE zone_id = $1 AND id = $2",
		zone, ids[0]); err != nil {
		t.Fatalf("возврат аренды: %v", err)
	}
	if err := countersShiftForManual(ctx, pool, zone, 0, 1, -1, 0); err != nil {
		t.Fatalf("дельта возврата: %v", err)
	}

	// A terminal status (not_delegated): queued → done.
	ok, err := dnsworker.RecordEpisode(ctx, pool, zone, ids[1], dnsworker.EpisodeResult{
		SetStatus: true, Status: model.StatusNotDelegated, Mask: 1,
	}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode terminal: ok=%v err=%v", ok, err)
	}
	countersVerifyEmpty(t, pool, "после терминального статуса")

	// The "there are addresses" branch: pending → done.
	ok, err = dnsworker.RecordEpisode(ctx, pool, zone, ids[0], dnsworker.EpisodeResult{Mask: 3}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode keep-status: ok=%v err=%v", ok, err)
	}
	countersVerifyEmpty(t, pool, "после ветки «есть адреса»")

	// A redelivery of an already committed episode — a no-op.
	ok, err = dnsworker.RecordEpisode(ctx, pool, zone, ids[1], dnsworker.EpisodeResult{
		SetStatus: true, Status: model.StatusNotDelegated, Mask: 1,
	}, nil)
	if err != nil || ok {
		t.Fatalf("RecordEpisode дубль: ok=%v err=%v, хочу (false, nil)", ok, err)
	}
	countersVerifyEmpty(t, pool, "после дубля доставки")

	// The invalid diagnosis: queued → done, the row leaves the cycle.
	ok, err = dnsworker.RecordInvalid(ctx, pool, zone, ids[2], nil)
	if err != nil || !ok {
		t.Fatalf("RecordInvalid: ok=%v err=%v", ok, err)
	}
	countersVerifyEmpty(t, pool, "после диагноза invalid")

	var queued, checked int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_queued, domains_checked FROM zones WHERE id = $1", zone,
	).Scan(&queued, &checked); err != nil {
		t.Fatalf("чтение зоны: %v", err)
	}
	if queued != 1 || checked != 3 {
		t.Fatalf("после DNS-переходов queued=%d checked=%d, хочу 1/3", queued, checked)
	}

	// The daily checks counter (T-179): the increment is an httpworker
	// result-write event, the DNS transitions do not produce it (terminal
	// branches have no HTTP episode) — the day row stayed at the starting
	// pass's zero; the live window grew to 3 (the DNS stage also sets
	// last_checked_at = current_date). The pass is the authority: it
	// overwrites the row with the window, the daily:checks drift position
	// observes the difference of the flows (3 DNS-only completions), not a
	// "zero anomaly".
	if got := countersChecksToday(t, pool); got != 0 {
		t.Fatalf("после DNS-переходов checks:<день> = %d, хочу 0 (инкремент — только запись результата HTTP)", got)
	}
	pass, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if got := countersChecksToday(t, pool); got != 3 {
		t.Fatalf("после реконсиляции checks:<день> = %d, хочу 3 (живое окно всех веток конвейера)", got)
	}
	dailySeen := false
	for _, p := range pass.Positions {
		if p.Counter != "daily:checks" {
			continue
		}
		dailySeen = true
		if p.Old != 0 || p.New != 3 || p.Drift() != 3 {
			t.Fatalf("daily:checks = %+v, хочу old=0 new=3 drift=3 (DNS-only завершения окна)", p)
		}
	}
	if !dailySeen {
		t.Fatal("позиция daily:checks отсутствует в итогах прохода")
	}
}

// countersShiftForManual — the zone counter delta for a manual row change
// bypassing the code (supporting the "the lease expired in the middle of
// the episode" scenario: total, pending, queued, checked).
func countersShiftForManual(ctx context.Context, pool *pgxpool.Pool, zone int16, total, pending, queued, checked int64) error {
	_, err := pool.Exec(ctx,
		"UPDATE zones SET domains_total = domains_total + $2, domains_pending = domains_pending + $3, "+
			"domains_queued = domains_queued + $4, domains_checked = domains_checked + $5 WHERE id = $1",
		zone, total, pending, queued, checked)
	return err
}

// TestCounters_DeleteWhileQueued — deleting queued domains (a delete file
// through staging): the queued column is decremented (the rows'
// check_state returned by the statement), total too; the cross-check
// converges (incremental-counters spec §12.2).
func TestCounters_DeleteWhileQueued(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "delete.test")
	countersSeedDomains(t, pool, zone, "q", 2, model.CheckStateQueued, model.StatusNew)
	if _, err := pool.Exec(ctx,
		"UPDATE zones SET domains_total = 2, domains_queued = 2 WHERE id = $1", zone); err != nil {
		t.Fatalf("счётчики зоны: %v", err)
	}

	// A delete batch into staging (StageTable — a direct INSERT: the batch
	// columns, line_no is filled by DEFAULT).
	batch, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	for i, label := range []string{"q0", "q1"} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, $5)",
			batch[:], zone, label, int16(model.ActionDelete), int16(i+1)); err != nil {
			t.Fatalf("staging %s: %v", label, err)
		}
	}
	dres, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch, ZoneID: zone, Action: model.ActionDelete})
	if err != nil {
		t.Fatalf("ApplyDelete: %v", err)
	}
	if dres.Deleted != 2 || dres.NotFound != 0 {
		t.Fatalf("итог delete = %+v, хочу {2 0}", dres)
	}
	var total, queued int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_total, domains_queued FROM zones WHERE id = $1", zone,
	).Scan(&total, &queued); err != nil {
		t.Fatalf("чтение зоны: %v", err)
	}
	if total != 0 || queued != 0 {
		t.Fatalf("после delete total=%d queued=%d, хочу 0/0", total, queued)
	}
	countersVerifyEmpty(t, pool, "после delete в queued")
}

// TestCounters_DeleteWithRenderPending — deleting a domain with the
// render_pending bit (T-186): applying delete clears the bit in the same
// transaction and decrements the render debt counter (the T-175 invariant
// "the counter = the number of rows with the bit"); other bits are
// preserved; reapplying the batch (at-least-once, spec 3.4) does not
// decrement twice; the cross-check converges.
func TestCounters_DeleteWithRenderPending(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "render-del.test")
	ids := countersSeedDomains(t, pool, zone, "r", 2, model.CheckStateDone, model.StatusAlive)
	// The render_pending bit (2) over is_idn (1) — as flag setting in the
	// result write (renderq.go); the debt counter — the same transitions
	// (the 000015 backfill would count 2).
	for _, id := range ids {
		if _, err := pool.Exec(ctx,
			"UPDATE domains SET flags = 3 WHERE zone_id = $1 AND id = $2", zone, id); err != nil {
			t.Fatalf("бит render_pending домена %d: %v", id, err)
		}
	}
	// The counter row exists after migration 000015 (the backfill of an
	// empty schema — 0); we seed the value the way the bit-setting
	// transitions would.
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) VALUES ('render_pending', 2) "+
			"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value"); err != nil {
		t.Fatalf("счётчик долга: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE zones SET domains_total = 2, domains_checked = 2 WHERE id = $1", zone); err != nil {
		t.Fatalf("счётчики зоны: %v", err)
	}

	apply := func(batch ingest.BatchID) ingest.DeleteResult {
		t.Helper()
		for i, label := range []string{"r0", "r1"} {
			if _, err := pool.Exec(ctx,
				"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, $5)",
				batch[:], zone, label, int16(model.ActionDelete), int16(i+1)); err != nil {
				t.Fatalf("staging %s: %v", label, err)
			}
		}
		res, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch, ZoneID: zone, Action: model.ActionDelete})
		if err != nil {
			t.Fatalf("ApplyDelete: %v", err)
		}
		return res
	}

	if res := apply(mustBatchID(t)); res.Deleted != 2 || res.NotFound != 0 {
		t.Fatalf("итог delete = %+v, хочу {2 0}", res)
	}

	// The bit is cleared, is_idn preserved; the debt counter decremented by 2.
	for i, id := range ids {
		var flags int16
		if err := pool.QueryRow(ctx,
			"SELECT flags FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
		).Scan(&flags); err != nil {
			t.Fatalf("домен r%d: %v", i, err)
		}
		if flags != 1 {
			t.Fatalf("домен r%d: flags = %d, хочу 1 (бит render_pending снят, is_idn сохранён)", i, flags)
		}
	}
	var debt int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'").Scan(&debt); err != nil {
		t.Fatalf("счётчик долга: %v", err)
	}
	if debt != 0 {
		t.Fatalf("счётчик рендер-долга = %d, хочу 0 (декремент на 2)", debt)
	}
	countersVerifyEmpty(t, pool, "после delete с битом")

	// A redelivery of the same delete content (a new batch,
	// at-least-once, spec 3.4): the domains are already deleted — the
	// status guard does not decrement the counter twice and does not touch
	// the bits; the report counts the rows as "deleted" (the T-019
	// semantics).
	if res := apply(mustBatchID(t)); res.Deleted != 2 || res.NotFound != 0 {
		t.Fatalf("повтор = %+v, хочу {2 0} (уже удалён — тоже «удалён»)", res)
	}
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'").Scan(&debt); err != nil {
		t.Fatalf("счётчик долга (повтор): %v", err)
	}
	if debt != 0 {
		t.Fatalf("счётчик рендер-долга после повтора = %d, хочу 0 (идемпотентность)", debt)
	}
	countersVerifyEmpty(t, pool, "после повтора delete")
}

// mustBatchID — a new batch for reapplying in the T-186 scenario.
func mustBatchID(t *testing.T) ingest.BatchID {
	t.Helper()
	id, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestCounters_DriftCaughtAndFixed — an artificial drift (a manual UPDATE
// zones bypassing the code): verify catches the position (for the CLI it
// is exit code 1), recount fixes it, a repeat verify is clean (the
// incremental-counters spec §12.3 criterion).
func TestCounters_DriftCaughtAndFixed(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "drift.test")
	countersSeedDomains(t, pool, zone, "p", 4, model.CheckStatePending, model.StatusNew)
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	// The drift: manual SQL bypassing the transition code.
	if _, err := pool.Exec(ctx,
		"UPDATE zones SET domains_pending = domains_pending + 5 WHERE id = $1", zone); err != nil {
		t.Fatalf("дрейф: %v", err)
	}
	mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(mismatch) != 1 || mismatch[0].Counter != fmt.Sprintf("zone:%d:pending", zone) {
		t.Fatalf("verify = %+v, хочу единственную зону:%d:pending", mismatch, zone)
	}

	// recount fixes it; a repeat verify is clean.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("recount: %v", err)
	}
	countersVerifyEmpty(t, pool, "после recount")
}

// countersHourRenderer — a Renderer for the hourly buckets (T-203): a live
// render result without Chromium (the DB side is tested — the rendered
// flag and the bucket, not the browser).
type countersHourRenderer struct{}

func (countersHourRenderer) Run(_ context.Context, _ render.RenderTask) (render.RenderOutput, error) {
	return render.RenderOutput{
		FinalURL: "https://h0.hour.test/rendered",
		DOM:      "<html><body>dom</body></html>",
	}, nil
}

// countersHourlyBucket — the value of the <prefix>:YYYY-MM-DD"T"HH24
// bucket (boundary — the SQL expression of the hour, computed by the DB);
// a missing row is (0, false).
func countersHourlyBucket(t *testing.T, pool *pgxpool.Pool, prefix, boundary string) (int64, bool) {
	t.Helper()
	var v int64
	err := pool.QueryRow(context.Background(),
		"SELECT value FROM crawler_counters WHERE name = $1 || to_char("+boundary+", 'YYYY-MM-DD\"T\"HH24')",
		prefix+":",
	).Scan(&v)
	if err != nil {
		return 0, false
	}
	return v, true
}

// TestCounters_HourlyThroughputBaskets — the hourly throughput buckets
// (T-203): a result-write episode maintains the current hour's results
// bucket with the delta of the inserted rows (a silent episode does not
// touch the bucket); a render maintains the rendered bucket of the row's
// checked_at hour (the flag and the bucket — one transaction); the
// reconciliation brings the buckets to the live count by the calendar hour
// (it creates the prev bucket from a row seeded bypassing the increments)
// and deletes stale buckets; the throughput reader
// (status.FetchThroughput) assembles the window from the two live buckets.
func TestCounters_HourlyThroughputBaskets(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "hour.test")
	ids := countersSeedDomains(t, pool, zone, "h", 3, model.CheckStateDone, model.StatusNew)
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("стартовый проход: %v", err)
	}
	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	// A row of the past hour, seeded bypassing the increments (a recheck
	// "before the deploy"): the pass must create the prev bucket from the
	// live window. The moment — the first minute of the previous calendar
	// hour (the boundary comes from the DB: the row is guaranteed to be
	// inside the prev hour at any moment of the run; racing the hour
	// boundary in the middle of the test leaves a window of milliseconds).
	// The domain is ids[2], WITHOUT write episodes: every RecordAndPublish
	// overwrites the domain's result entirely (recordDeleteSQL, §3.5 "no
	// check history is stored") — a row under h0/h1 would be wiped by the
	// episodes below rather than survive to the reconciliation.
	if _, err := pool.Exec(ctx,
		"INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, final_url, checked_at) "+
			"VALUES ($1, $2, 1, 2, 'https://old.hour.test/', "+
			"date_trunc('hour', now() - interval '1 hour') + interval '1 minute')",
		zone, ids[2]); err != nil {
		t.Fatalf("строка прошлого часа: %v", err)
	}

	// An episode with two records (two subdomains × one record each): the
	// current hour bucket +2 — the delta equals the inserted rows.
	rich := &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[0], Name: "h0.hour.test",
		Subdomains: []httpworker.SubdomainResult{
			{
				SubdomainID: 0,
				Records: []httpworker.Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					FinalURL: "https://h0.hour.test/", HTTPStatus: 200,
				}},
			},
			{
				SubdomainID: 2,
				Records: []httpworker.Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					FinalURL: "https://www.hour.test/", HTTPStatus: 200,
				}},
			},
		},
	}
	if ok, err := rec.RecordAndPublish(ctx, rich); err != nil || !ok {
		t.Fatalf("эпизод с записями: ok=%v err=%v", ok, err)
	}
	if v, ok := countersHourlyBucket(t, pool, "results", "date_trunc('hour', now())"); !ok || v != 2 {
		t.Fatalf("после эпизода results:<час> = (%d, %v), хочу (2, true) — дельта вставленных строк", v, ok)
	}

	// A silent temp_error episode: no records — it does not touch the bucket.
	silent := &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[1], Name: "h1.hour.test",
		Subdomains: []httpworker.SubdomainResult{{
			SubdomainID: 0,
			HTTPS:       httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
			HTTP:        httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
		}},
	}
	if ok, err := rec.RecordAndPublish(ctx, silent); err != nil || !ok {
		t.Fatalf("молчаливый эпизод: ok=%v err=%v", ok, err)
	}
	if v, _ := countersHourlyBucket(t, pool, "results", "date_trunc('hour', now())"); v != 2 {
		t.Fatalf("после молчаливого эпизода results:<час> = %d, хочу 2 (эпизод без строк корзину не трогает)", v)
	}

	// Rendering the episode's row: the rendered bucket of the row's
	// checked_at hour +1 (the flag and the bucket — one content-replacement
	// transaction).
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rw, err := renderworker.New(pool, &countersStubQueue{}, store, countersHourRenderer{}, renderworker.Options{})
	if err != nil {
		t.Fatalf("renderworker.New: %v", err)
	}
	rendered, err := rw.ProcessResult(ctx, queue.Result{
		ZoneID: zone, DomainID: ids[0], SubdomainID: 0, Variant: model.VariantMain, Name: "h0.hour.test",
	})
	if err != nil || !rendered {
		t.Fatalf("ProcessResult: ok=%v err=%v", rendered, err)
	}
	if v, ok := countersHourlyBucket(t, pool, "rendered", "date_trunc('hour', now())"); !ok || v != 1 {
		t.Fatalf("после рендера rendered:<час> = (%d, %v), хочу (1, true)", v, ok)
	}

	// Stale buckets (older than the previous hour): seeded by a direct
	// INSERT — the pass must delete them (the live throughput window, not
	// history).
	for _, name := range []string{"results:2000-01-01T00", "rendered:2000-01-01T00"} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO crawler_counters (name, value, updated_at) VALUES ($1, 999, now())", name,
		); err != nil {
			t.Fatalf("устаревшая корзина %s: %v", name, err)
		}
	}

	// The reconciliation is the authority: the prev buckets are created
	// from the live window (results 1 — the seeded row, rendered 0), cur
	// converges with the increments (2/1, no semantic divergences — zero
	// drift); the stale ones are deleted.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if v, ok := countersHourlyBucket(t, pool, "results", "date_trunc('hour', now() - interval '1 hour')"); !ok || v != 1 {
		t.Fatalf("после реконсиляции results:<prev час> = (%d, %v), хочу (1, true) — живое окно прошлого часа", v, ok)
	}
	if v, ok := countersHourlyBucket(t, pool, "rendered", "date_trunc('hour', now() - interval '1 hour')"); !ok || v != 0 {
		t.Fatalf("после реконсиляции rendered:<prev час> = (%d, %v), хочу (0, true) — пустой prev-час создан нулём", v, ok)
	}
	if v, _ := countersHourlyBucket(t, pool, "results", "date_trunc('hour', now())"); v != 2 {
		t.Fatalf("после реконсиляции results:<час> = %d, хочу 2 (корзина сходится с живым count)", v)
	}
	if v, _ := countersHourlyBucket(t, pool, "rendered", "date_trunc('hour', now())"); v != 1 {
		t.Fatalf("после реконсиляции rendered:<час> = %d, хочу 1", v)
	}
	for _, name := range []string{"results:2000-01-01T00", "rendered:2000-01-01T00"} {
		var n int
		if err := pool.QueryRow(ctx,
			"SELECT count(*) FROM crawler_counters WHERE name = $1", name,
		).Scan(&n); err != nil || n != 0 {
			t.Fatalf("устаревшая корзина %s пережила проход (count=%d err=%v)", name, n, err)
		}
	}

	// The throughput reader: a window of the two live buckets (prev 1 +
	// cur 2), the rendered share 1, the coverage window of 1–2 h — the
	// divisor of the honest rate.
	thr, err := status.FetchThroughput(ctx, pool)
	if err != nil {
		t.Fatalf("FetchThroughput: %v", err)
	}
	if thr.ResultsHour != 3 || thr.RenderedHour != 1 {
		t.Fatalf("throughput = %+v, хочу results_hour=3 rendered_hour=1 (prev+cur корзины)", thr)
	}
	if thr.HourWindowSec < 3600 || thr.HourWindowSec >= 7200 {
		t.Fatalf("hour_window_sec = %v, хочу [3600, 7200) — окно prev+cur корзин", thr.HourWindowSec)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции корзин темпа (корзины вне verify)")
}

// tailContentStub — an analyzer.ContentGetter: no content (ErrNotFound —
// the seeded row has no content_hash, spec 5.8: the analysis is by headers
// and cookies).
type tailContentStub struct{}

func (tailContentStub) Get(_ context.Context, _ content.Hash) ([]byte, error) {
	return nil, content.ErrNotFound
}

// tailSetsStub — an analyzer.SetSource: an empty set of the given version
// (no rules — no hits; the stamp is set on the "empty" verdict too,
// T-071).
type tailSetsStub struct{ version int32 }

func (s tailSetsStub) Set() *rules.Set { return &rules.Set{Version: s.version} }

// TestCounters_ReanalyzeTailBaskets — the re-analysis unanalyzed tail
// buckets (T-204): a lost publication (a NULL row of the past hour, seeded
// bypassing the increments — like a recheck "before the deploy") is
// materialized by the monitor pass into the prev bucket from the live
// window; the tick's guard opens — the tick publishes the tail; the
// analyzer's stamp decrements the bucket of the EPISODE's hour (a
// redelivery is a no-op by the version guard, the delta is not
// double-counted); the guard closes — an immature row of the current hour
// (in-flight) is not read by the tick at all; stale buckets are deleted by
// the pass.
func TestCounters_ReanalyzeTailBaskets(t *testing.T) {
	skipUnlessIntegration(t)
	reanSkipHourBoundary(t) // the seed and the reconciliation window — "the previous hour" from now()
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "tail.test")
	ids := countersSeedDomains(t, pool, zone, "t", 2, model.CheckStateDone, model.StatusNew)
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("стартовый проход: %v", err)
	}

	// The tail of the past hour: a NULL row (a lost q:analyze publication).
	// The domain ids[1] — WITHOUT write episodes (recordDeleteSQL wipes all
	// of a domain's rows — the lesson of the T-203 wave); the moment — the
	// first minute of the previous calendar hour (the boundary comes from
	// the DB; racing the hour boundary — a window of milliseconds).
	if _, err := pool.Exec(ctx,
		"INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, final_url, checked_at) "+
			"VALUES ($1, $2, 1, 2, 'https://lost.tail.test/', "+
			"date_trunc('hour', now() - interval '1 hour') + interval '1 minute')",
		zone, ids[1]); err != nil {
		t.Fatalf("хвост прошлого часа: %v", err)
	}

	// The reconciliation is the authority: the tails' prev bucket is
	// created from the live window (1 NULL row of the past hour), cur —
	// zero.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if v, ok := countersHourlyBucket(t, pool, "reanalyze", "date_trunc('hour', now() - interval '1 hour')"); !ok || v != 1 {
		t.Fatalf("после реконсиляции reanalyze:<prev час> = (%d, %v), хочу (1, true) — живое окно прошлого часа", v, ok)
	}
	if v, ok := countersHourlyBucket(t, pool, "reanalyze", "date_trunc('hour', now())"); !ok || v != 0 {
		t.Fatalf("после реконсиляции reanalyze:<час> = (%d, %v), хочу (0, true) — пустой час создан нулём", v, ok)
	}

	// The guard is open (a mature tail): the tick reads the window with the
	// time shape and publishes exactly the tail.
	q := &reanQ{}
	var cursor analyzer.ReanalyzeCursor
	res, err := analyzer.ReanalyzeTick(ctx, pool, q, 1001, &cursor, analyzer.ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeTick: %v", err)
	}
	if res.Selected != 1 || res.Published != 1 {
		t.Fatalf("тик = %+v, хочу 1/1 (хвост прошлого часа)", res)
	}
	if got := q.publishes[0]; got.DomainID != ids[1] || got.SubdomainID != 1 {
		t.Fatalf("публикация = %+v, хочу ключ хвоста", got)
	}

	// The analyzer's stamp — a decrement of the episode hour's bucket (in
	// the same transaction); a redelivery of the same version is a no-op by
	// the mark guard (T-204): the delta is not double-counted.
	an, err := analyzer.New(pool, &countersStubQueue{}, tailContentStub{}, tailSetsStub{version: 1001}, analyzer.Options{})
	if err != nil {
		t.Fatalf("analyzer.New: %v", err)
	}
	task := queue.Result{
		ZoneID: zone, DomainID: ids[1], SubdomainID: 1,
		Variant: model.VariantMain, Name: "lost.tail.test",
	}
	for i := 0; i < 2; i++ {
		if _, err := an.ProcessResult(ctx, task); err != nil {
			t.Fatalf("ProcessResult №%d: %v", i+1, err)
		}
	}
	if v, _ := countersHourlyBucket(t, pool, "reanalyze", "date_trunc('hour', now() - interval '1 hour')"); v != 0 {
		t.Fatalf("после штампа reanalyze:<prev час> = %d, хочу 0 (декремент без дубля)", v)
	}
	var stamped int32
	if err := pool.QueryRow(ctx,
		"SELECT rules_version FROM subdomain_results WHERE zone_id = $1 AND domain_id = $2 AND subdomain_id = 1",
		zone, ids[1]).Scan(&stamped); err != nil || stamped != 1001 {
		t.Fatalf("rules_version хвоста = (%d, %v), хочу штамп 1001", stamped, err)
	}

	// The guard is closed: a fresh NULL row of the CURRENT hour (in-flight
	// — the main write path → q:analyze maintains it, the hour bucket is
	// still being finished off by decrements) is not mature — the tick does
	// not read the window.
	if _, err := pool.Exec(ctx,
		"INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, final_url, checked_at) "+
			"VALUES ($1, $2, 2, 2, 'https://inflight.tail.test/', now())",
		zone, ids[1]); err != nil {
		t.Fatalf("in-flight строка: %v", err)
	}
	res2, err := analyzer.ReanalyzeTick(ctx, pool, q, 1001, &cursor, analyzer.ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeTick (guard закрыт): %v", err)
	}
	if res2 != (analyzer.ReanalyzeResult{}) || len(q.publishes) != 1 {
		t.Fatalf("тик при закрытом guard = %+v (публикаций %d), хочу 0/0/1 — незрелая строка не вычитывается",
			res2, len(q.publishes))
	}

	// A stale tails bucket (older than the previous hour) — the pass
	// deletes it: the buckets are the guard's live window, not history.
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value, updated_at) VALUES ('reanalyze:2000-01-01T00', 999, now())",
	); err != nil {
		t.Fatalf("устаревшая корзина: %v", err)
	}
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("финальная реконсиляция: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM crawler_counters WHERE name = 'reanalyze:2000-01-01T00'",
	).Scan(&n); err != nil || n != 0 {
		t.Fatalf("устаревшая корзина пережила проход (count=%d err=%v)", n, err)
	}
	countersVerifyEmpty(t, pool, "после сценария корзин хвостов (корзины вне verify)")
}

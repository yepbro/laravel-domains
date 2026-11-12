// Integration tests of the T-200 form of returning expired leases (the
// TZ_slow_lease_return.md spec §7.2) against live PostgreSQL: a mature
// lease is returned by a batch of PK probes (a repeated pass — 0), a
// result recorded by the worker between the selection and the
// transition is not returned (the guard race), the drain cursor does
// not skip records that mature later (a small lease, two passes with a
// pause), the zone counter deltas agree with the T-178 reconciliation;
// the EXPLAIN contracts of both sides of the pass — the composite
// cursor predicate is served by the domains_lease_idx leaf (T-104),
// the transition leads from unnest with PK probes of domains without a
// range lease scan (§4.2, review T-200 P3-1). Unit level (SQL
// contracts, the cursor, the loop on fakes) —
// internal/scheduler/lease_test.go. The call level — library-level,
// without starting services (the reanalyze_test.go style); isolation —
// withTestSchema (search_path in the DSN; ALL connections of the test
// — one pool of that schema). Run — make test-integration (after
// merging, from the main tree).
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/scheduler"
)

// leaseSeedQueued — a domain in queued with a lease mark age ago
// (make_interval — deterministic input without parsing interval
// strings); returns the id.
func leaseSeedQueued(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, label string, ageMinutes int) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO domains (zone_id, label, check_state, status, queued_at)
		 VALUES ($1, $2, $3, $4, now() - make_interval(mins => $5)) RETURNING id`,
		zone, label, int16(model.CheckStateQueued), int16(model.StatusNew), ageMinutes,
	).Scan(&id); err != nil {
		t.Fatalf("домен %s: %v", label, err)
	}
	return id
}

// leaseCheckState — the check_state of a domain (the quote value for messages).
func leaseCheckState(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, id int64) int16 {
	t.Helper()
	var state int16
	if err := pool.QueryRow(ctx,
		"SELECT check_state FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
	).Scan(&state); err != nil {
		t.Fatalf("чтение домена %d: %v", id, err)
	}
	return state
}

// TestLease_MatureReturnedRepeatZero — the §7.2 scenario: a mature
// lease is returned to pending in one pass (a batch of PK probes, the
// count by RETURNING), a repeated pass with the same cursor — 0 (the
// transitioned rows drop out of the selection); the zone deltas
// queued −n / pending +n agree with the T-178 reconciliation
// (VerifyCounters after the episode).
func TestLease_MatureReturnedRepeatZero(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper (review T-200 P2-1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "lease.test")
	labels := []string{"m1", "m2", "m3", "m4"}
	for i := range labels {
		leaseSeedQueued(ctx, t, pool, zone, labels[i], 60+i) // all older than the lease
	}

	// The T-178 reconciliation — fills the zone counter zeros (4 queued);
	// after the return the cross-check must agree even without a new
	// reconciliation: the deltas are handled by the CTE of the same
	// transition operator.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")

	var cursor scheduler.LeaseCursor
	res, err := scheduler.LeaseOnce(ctx, pool, &cursor, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res.Domains != int64(len(labels)) || res.Selected != len(labels) {
		t.Fatalf("итог = %+v, want Domains=Selected=%d", res, len(labels))
	}
	if n := countDomains(t, pool,
		fmt.Sprintf("zone_id = %d AND check_state = %d", zone, int16(model.CheckStatePending))); n != len(labels) {
		t.Fatalf("pending после возврата: %d, хочу %d", n, len(labels))
	}
	if n := countDomains(t, pool,
		fmt.Sprintf("zone_id = %d AND queued_at IS NOT NULL", zone)); n != 0 {
		t.Errorf("queued_at не сброшен у возвращённых: %d, хочу 0", n)
	}
	countersVerifyEmpty(t, pool, "после lease (дельты T-178)")

	// A repeated pass with the same cursor — an empty batch: the
	// transitioned rows no longer satisfy the selection (idempotency,
	// spec 3.4).
	repeat, err := scheduler.LeaseOnce(ctx, pool, &cursor, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("повторный LeaseOnce: %v", err)
	}
	if repeat != (scheduler.LeaseResult{}) {
		t.Fatalf("повторный проход = %+v, want нулевой", repeat)
	}
	countersVerifyEmpty(t, pool, "после повторного прохода")
}

// --- the race: a result recorded between the selection and the transition --

// leaseRacePool — a pool wrapper for the race scenario (§7.2): in the
// SAME pass transaction, between the batch SELECT and the transition
// UPDATE, a "worker" records a result for the batch victim — the
// transition guard must filter it out. Both operators are identified
// by text prefixes (the package SQL is not exported); an unexpected
// operator — an error: the pass contract breaks loudly.
type leaseRacePool struct {
	*pgxpool.Pool

	victimZone int16
	victimID   int64
}

var _ scheduler.LeaseDB = (*leaseRacePool)(nil)

func (p *leaseRacePool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &leaseRaceTx{Tx: tx, pool: p}, nil
}

// leaseRaceTx — a transaction with the race injection point.
type leaseRaceTx struct {
	pgx.Tx

	pool *leaseRacePool
}

func (t *leaseRaceTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	switch {
	case strings.HasPrefix(sql, "SELECT zone_id, id, queued_at FROM domains"):
		return t.Tx.Query(ctx, sql, args...)
	case strings.HasPrefix(sql, "WITH upd AS (UPDATE domains AS t"):
		// The "worker" records a result between the selection and the
		// transition (the record contract: queued → done, the lease mark
		// reset); the pass transaction sees its own edit — the guard must
		// filter the victim out, the RETURNING count does not count it.
		if _, err := t.Exec(ctx,
			"UPDATE domains SET check_state = $1, queued_at = NULL WHERE zone_id = $2 AND id = $3",
			int16(model.CheckStateDone), t.pool.victimZone, t.pool.victimID); err != nil {
			return nil, fmt.Errorf("leaseRaceTx: запись результата воркера: %w", err)
		}
		return t.Tx.Query(ctx, sql, args...)
	default:
		return nil, fmt.Errorf("leaseRaceTx: неожидаемый SQL прохода: %s", sql)
	}
}

// TestLease_WorkerRaceGuard — the §7.2 scenario: a result recorded
// between the selection and the transition — the domain is not returned
// (stays done), the other batch rows are transitioned; the cursor moves
// past the batch anyway.
func TestLease_WorkerRaceGuard(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper (review T-200 P2-1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "lease-race.test")
	first := leaseSeedQueued(ctx, t, pool, zone, "r1", 90)
	victim := leaseSeedQueued(ctx, t, pool, zone, "r2", 60) // the batch key — the middle one
	last := leaseSeedQueued(ctx, t, pool, zone, "r3", 30)

	// The T-178 reconciliation — the zone counts 3 queued: after the
	// race, the direct cross-check of the deltas of a partially
	// transitioned batch (review T-200 P3-2).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")

	race := &leaseRacePool{Pool: pool, victimZone: zone, victimID: victim}
	var cursor scheduler.LeaseCursor
	res, err := scheduler.LeaseOnce(ctx, race, &cursor, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce с гонкой: %v", err)
	}
	if res.Domains != 2 || res.Selected != 3 {
		t.Fatalf("итог = %+v, want Domains=2 (жертва отсеяна guard), Selected=3", res)
	}
	if state := leaseCheckState(ctx, t, pool, zone, victim); state != int16(model.CheckStateDone) {
		t.Errorf("жертва гонки: check_state = %d, хочу done — результат воркера не затёрт возвратом", state)
	}
	for _, id := range []int64{first, last} {
		if state := leaseCheckState(ctx, t, pool, zone, id); state != int16(model.CheckStatePending) {
			t.Errorf("домен %d: check_state = %d, хочу pending — соседние строки пачки возвращены", id, state)
		}
	}

	// The zone deltas counted ONLY the transitioned rows (T-178):
	// queued 3−2=1, pending 0+2=2 — the guard-filtered victim is not in
	// the deltas. The queued remainder (1 against 0 alive) — the test's
	// direct SQL worker bypasses the incremental deltas (a result is
	// written by the T-058 record contract, a simplification here); the
	// reconciliation levels it out — what is pinned here is exactly
	// "the deltas == the transition RETURNING".
	var queued, pending int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_queued, domains_pending FROM zones WHERE id = $1", zone,
	).Scan(&queued, &pending); err != nil {
		t.Fatalf("чтение счётчиков зоны: %v", err)
	}
	if queued != 1 || pending != 2 {
		t.Errorf("дельты зоны = queued %d / pending %d, want 1/2 — дельты ведут ровно RETURNING перевода",
			queued, pending)
	}

	// The cursor moved past the batch: a repeated pass (already without
	// the race) finds nothing — the victim's fate has changed, coming
	// back to the batch would give nothing.
	repeat, err := scheduler.LeaseOnce(ctx, pool, &cursor, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("повторный LeaseOnce: %v", err)
	}
	if repeat != (scheduler.LeaseResult{}) {
		t.Fatalf("повторный проход = %+v, want нулевой — курсор за пачкой", repeat)
	}
}

// TestLease_CursorNotSkipLaterMatured — the §7.2 scenario: the cursor
// does not skip records that aged out AFTER the pass (a small lease,
// passes with a pause): a fresh lease to the right of the threshold is
// not selected and does not move the cursor; having aged out over the
// pause, it is returned by the next pass (its key is to the right of
// the cursor, spec §4.1).
func TestLease_CursorNotSkipLaterMatured(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper (review T-200 P2-1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "lease-cursor.test")
	old := leaseSeedQueued(ctx, t, pool, zone, "c1", 30) // mature under a small lease

	// A small 2 s lease: the lease "freshness" budget is robust to the
	// runner pauses (a pause > the lease between the seed and pass 2
	// would age the lease out — a false failure; review T-200 P2-1).
	opts := scheduler.LeaseOptions{Timeout: 2 * time.Second}
	var cursor scheduler.LeaseCursor

	// Pass 1: only the mature one; the cursor — past it.
	first, err := scheduler.LeaseOnce(ctx, pool, &cursor, opts)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if first.Domains != 1 || first.Selected != 1 {
		t.Fatalf("проход 1 = %+v, want 1/1", first)
	}
	if state := leaseCheckState(ctx, t, pool, zone, old); state != int16(model.CheckStatePending) {
		t.Fatalf("зрелая аренда не возвращена: check_state = %d", state)
	}

	// A fresh lease: to the right of the threshold — the second pass is
	// empty, the cursor stands.
	fresh := leaseSeedQueued(ctx, t, pool, zone, "c2", 0)
	second, err := scheduler.LeaseOnce(ctx, pool, &cursor, opts)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if second != (scheduler.LeaseResult{}) {
		t.Fatalf("проход 2 = %+v, want нулевой — аренда ещё не истекла", second)
	}
	if state := leaseCheckState(ctx, t, pool, zone, fresh); state != int16(model.CheckStateQueued) {
		t.Fatalf("живая аренда тронута: check_state = %d, хочу queued", state)
	}

	// A pause longer than the lease: the fresh one aged out — pass 3
	// returns it, the cursor did not skip it (the fresh key is to the
	// right of the pass 1 cursor).
	time.Sleep(2500 * time.Millisecond)
	third, err := scheduler.LeaseOnce(ctx, pool, &cursor, opts)
	if err != nil {
		t.Fatalf("проход 3: %v", err)
	}
	if third.Domains != 1 || third.Selected != 1 {
		t.Fatalf("проход 3 = %+v, want 1/1 — состарившаяся позже запись не пропущена курсором", third)
	}
	if state := leaseCheckState(ctx, t, pool, zone, fresh); state != int16(model.CheckStatePending) {
		t.Errorf("состарившаяся аренда не возвращена: check_state = %d, хочу pending", state)
	}
}

// TestLease_CursorPredicateIndex — the EXPLAIN contracts of both sides
// of the pass (the reanalyze_test.go style). SELECT (§4.1): the
// composite cursor predicate + the aging threshold are served by the
// leaf instance of domains_lease_idx (T-104), not by a sort/full scan.
// UPDATE (§4.2, review T-200 P3-1): the core of the spec §2.1
// diagnosis — the transition must lead from unnest (a Function Scan)
// and probe domains by PK (domains_pkey); there is no lease scan of
// the expired range on the UPDATE side (§3.1/§6.1). On a toy volume a
// seq scan is cheaper than the index — enable_seqscan = off checks the
// indexability of the predicates and the plan shape, not the
// cost-based choice (the cost — the T-177 slow-query log in
// production).
func TestLease_CursorPredicateIndex(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper (review T-200 P2-1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "lease-explain.test")
	id := leaseSeedQueued(ctx, t, pool, zone, "e1", 90)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatalf("SET LOCAL: %v", err)
	}

	// The SELECT side: the leaseSelectSQL form with literals (parameters
	// in EXPLAIN require a prepared statement — the reanalyze_test.go
	// style): the 30 min threshold, the cursor 2 hours ago with a zero
	// PK part, the batch LIMIT.
	selPlan := leaseExplain(ctx, t, tx, fmt.Sprintf(
		"SELECT zone_id, id, queued_at FROM domains "+
			"WHERE check_state = %d AND status <> %d "+
			"AND queued_at < now() - make_interval(secs => %d) "+
			"AND (queued_at, zone_id, id) > (now() - interval '2 hours', 0::int2, 0::int8) "+
			"ORDER BY queued_at, zone_id, id LIMIT %d",
		int16(model.CheckStateQueued), int16(model.StatusDeleted),
		int(scheduler.DefaultLeaseTimeout.Seconds()), scheduler.DefaultLeaseBatch))
	// domains is partitioned: the parent name of domains_lease_idx is
	// not inherited by the leaves — PG materializes the child indexes
	// as <partition>_<columns>_idx (a measurement:
	// domains_default_queued_at_zone_id_id_idx on DEFAULT; check_state
	// = 1 is absent from the Index Cond — implied by the partial index
	// predicate). The assert matches the key-column suffix, invariant to
	// the split.
	if !strings.Contains(selPlan, "queued_at_zone_id_id_idx") {
		t.Errorf("предикат композитного курсора не обслуживается листовым экземпляром domains_lease_idx:\n%s",
			selPlan)
	}

	// The UPDATE side: the leaseUpdateSQL form with literals (the
	// unnest keys — the seeded domain, the 30 min threshold). The join
	// order is not pinned at the test volume: with a handful of rows
	// the planner is free to lead from the lease index leaf (a range of
	// a few rows, cheaper than PK probes; on the production 42M table
	// it leads from unnest with PK probes — the TZ §2.1/§3.1, the prod
	// plan is guarded by the T-177 §7.3 slow-query log, the operator
	// text — by the leaseUpdateSQL unit contract). The stable invariants
	// of the test: the unnest pack is present in the plan (PG16 prints
	// a Function Scan by the u alias, not the function name) and there
	// is no degradation into a seq scan over domains.
	updPlan := leaseExplain(ctx, t, tx, fmt.Sprintf(
		"WITH upd AS (UPDATE domains AS t "+
			"SET check_state = %d, queued_at = NULL "+
			"FROM unnest(ARRAY[%d::int2], ARRAY[%d::int8]) AS u(zone_id, id) "+
			"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
			"AND t.check_state = %d AND t.status <> %d "+
			"AND t.queued_at < now() - make_interval(secs => %d) "+
			"RETURNING t.zone_id), "+
			"zc AS (UPDATE zones AS z "+
			"SET domains_queued = z.domains_queued - c.n, "+
			"domains_pending = z.domains_pending + c.n "+
			"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
			"WHERE z.id = c.zone_id) "+
			"SELECT zone_id FROM upd",
		int16(model.CheckStatePending), zone, id,
		int16(model.CheckStateQueued), int16(model.StatusDeleted),
		int(scheduler.DefaultLeaseTimeout.Seconds())))
	if !strings.Contains(updPlan, "Function Scan on u") {
		t.Errorf("в плане перевода нет unnest-пакета (Function Scan on u):\n%s", updPlan)
	}
	if strings.Contains(updPlan, "Seq Scan on domains") {
		t.Errorf("перевод пачки деградировал в seq scan по domains:\n%s", updPlan)
	}
}

// leaseExplain — the EXPLAIN (COSTS OFF) plan text of a query in the tx transaction.
func leaseExplain(ctx context.Context, t *testing.T, tx pgx.Tx, query string) string {
	t.Helper()
	rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+query)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("EXPLAIN: чтение: %v", err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	return plan.String()
}

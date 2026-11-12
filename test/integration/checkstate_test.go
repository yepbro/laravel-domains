// Integration tests of the check_state automaton (T-255, TZ-fsm spec
// tasks/TZ-fsm.md §3 FR-1) against live PostgreSQL: a scenario chain of
// all specification transitions (internal/scheduler/checkstate.go — the
// single source of truth) by real writer statements — insert via an add
// file (R10), scheduler enqueueing (R1: queued_at + clearing
// http_published), DNS result recording (R3: done, lease reset), daily
// recheck (R7), lease return (R5), the terminal DNS temp_error branch
// (R2) and temp_error retry (R8), deletion (R14) and revival (R9) — after
// each episode the zone counter deltas converge with the T-178
// reconciliation. Separately — the terminal FR-1.2 guard: a queued row
// with NULL queued_at (a broken I1 invariant) is invisible to the main
// lease pass, returned by the LeaseNullOnce repair pass (the
// crawler_scheduler_lease_null_returned_total counter grows), a repeat
// pass is 0; the EXPLAIN contract of the guard selection — the IS NULL
// predicate is served by the leaf instance of domains_lease_idx (T-104),
// the shape of the main lease query does not change. Manual recheck (R11)
// and bulk subdomain recheck (R12) — cmd/crawlerctl (package main): the
// integration test recheck_integration_test.go, run by make
// test-integration with the same package list. The invalid (R13)
// diagnosis on a live DB is covered by the existing
// TestCounters_DNSStageTransitions (counters_test.go, RecordInvalid) —
// not duplicated here. The call level is library-level, no services
// started; isolation — withTestSchema.
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/dnsworker"
	"crawler/internal/ingest"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/scheduler"
)

// checkStateRow — the observed state of a domains row for transition
// checks: check_state, status, the lease mark, the temp_error counter,
// the http_published bit.
type checkStateRow struct {
	state         int16
	status        int16
	queuedAtNull  bool
	lastChecked   bool
	tempAttempts  int16
	httpPublished bool
}

// checkStateOf — a domain row by key.
func checkStateOf(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, id int64) checkStateRow {
	t.Helper()
	var r checkStateRow
	var queuedAt, lastChecked *time.Time
	var flags int16
	if err := pool.QueryRow(ctx,
		"SELECT check_state, status, queued_at, last_checked_at, temp_attempts, flags "+
			"FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
	).Scan(&r.state, &r.status, &queuedAt, &lastChecked, &r.tempAttempts, &flags); err != nil {
		t.Fatalf("чтение домена %d: %v", id, err)
	}
	r.queuedAtNull = queuedAt == nil
	r.lastChecked = lastChecked != nil
	r.httpPublished = flags&int16(model.FlagHTTPPublished) != 0
	return r
}

// checkStateStageAdd — an add batch into staging (labels) and the
// ApplyAdd application: insert (R10) or revival (R9) by a real statement.
func checkStateStageAdd(ctx context.Context, t *testing.T, pool ingest.ApplyDB, zone int16, labels ...string) ingest.AddResult {
	t.Helper()
	batch, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	for i, label := range labels {
		if _, err := pool.Exec(ctx,
			"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, $5)",
			batch[:], zone, label, int16(model.ActionAdd), int16(i+1)); err != nil {
			t.Fatalf("staging add %s: %v", label, err)
		}
	}
	res, err := ingest.ApplyAdd(ctx, pool, ingest.Batch{ID: batch, ZoneID: zone, Action: model.ActionAdd}, 100)
	if err != nil {
		t.Fatalf("ApplyAdd: %v", err)
	}
	return res
}

// TestCheckState_SpecTransitions — the specification transition chain in
// the order R10 → R1 → R3 → R7 → R1+R5 → R2 → R8 → R14 → R9: each
// episode is a real writer; side effects (queued_at, the http_published
// bit, temp_attempts, zone deltas) are checked on the row.
func TestCheckState_SpecTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "fsm.test")
	q := &countersStubQueue{}

	// R10: inserting a new domain via an add file — status=new, pending, no lease.
	add := checkStateStageAdd(ctx, t, pool, zone, "spec")
	if add.Inserted != 1 || add.Revived != 0 {
		t.Fatalf("ApplyAdd = %+v, хочу Inserted=1", add)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"SELECT id FROM domains WHERE zone_id = $1 AND label = 'spec'", zone,
	).Scan(&id); err != nil {
		t.Fatalf("id вставленного: %v", err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		r.status != int16(model.StatusNew) || !r.queuedAtNull {
		t.Fatalf("после вставки = %+v, хочу new/pending/queued_at NULL (Р10)", r)
	}
	countersVerifyEmpty(t, pool, "после вставки (Р10)")

	// R1: scheduler enqueueing — queued, the lease started, the
	// http_published bit of the previous episode cleared, the q:dns
	// publication via RETURNING.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET flags = flags | $1 WHERE zone_id = $2 AND id = $3",
		int16(model.FlagHTTPPublished), zone, id); err != nil {
		t.Fatalf("сеем бит http_published: %v", err)
	}
	eres, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "spec.fsm.test"},
	})
	if err != nil || eres.Queued != 1 || eres.Published != 1 {
		t.Fatalf("EnqueueBatch = %+v err=%v, хочу Queued=Published=1 (Р1)", eres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) ||
		r.queuedAtNull || r.httpPublished {
		t.Fatalf("после постановки = %+v, хочу queued/queued_at NOT NULL/бит снят (Р1)", r)
	}
	countersVerifyEmpty(t, pool, "после постановки (Р1)")

	// R3: recording the DNS result "there are addresses" — done, the
	// lease is finished, the status verdict belongs to the HTTP stage (not
	// written), the check mark is present.
	ok, err := dnsworker.RecordEpisode(ctx, pool, zone, id, dnsworker.EpisodeResult{Mask: 1}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode: ok=%v err=%v (Р3)", ok, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) ||
		!r.queuedAtNull || !r.lastChecked || r.status != int16(model.StatusNew) {
		t.Fatalf("после записи DNS = %+v, хочу done/queued_at NULL/отметка есть/статус не тронут (Р3)", r)
	}
	countersVerifyEmpty(t, pool, "после записи DNS (Р3)")

	// R7: the daily age-based recheck — done → pending, the status is
	// kept, no lease.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 400 WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст проверки: %v", err)
	}
	rk, err := scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Interval: 300 * 24 * time.Hour})
	if err != nil || rk.Domains != 1 {
		t.Fatalf("RecheckOnce = %+v err=%v, хочу 1 (Р7)", rk, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		!r.queuedAtNull || r.status != int16(model.StatusNew) {
		t.Fatalf("после перепроверки = %+v, хочу pending/queued_at NULL (Р7)", r)
	}
	countersVerifyEmpty(t, pool, "после перепроверки (Р7)")

	// R1 + R5: re-enqueueing and an expired lease — return to pending.
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "spec.fsm.test"},
	}); err != nil {
		t.Fatalf("EnqueueBatch (Р1 повторно): %v", err)
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
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) || !r.queuedAtNull {
		t.Fatalf("после lease-возврата = %+v, хочу pending/queued_at NULL (Р5)", r)
	}
	countersVerifyEmpty(t, pool, "после lease-возврата (Р5)")

	// R2: the terminal DNS temp_error branch — done + temp_error, the
	// attempt counter is kept (a new episode starts from scratch), the
	// maturity bucket is today.
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "spec.fsm.test"},
	}); err != nil {
		t.Fatalf("EnqueueBatch (перед Р2): %v", err)
	}
	ok, err = dnsworker.RecordEpisode(ctx, pool, zone, id, dnsworker.EpisodeResult{
		SetStatus: true,
		Status:    model.StatusTempError,
	}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode temp_error: ok=%v err=%v (Р2)", ok, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) ||
		r.status != int16(model.StatusTempError) || !r.queuedAtNull {
		t.Fatalf("после temp_error-ветки DNS = %+v, хочу done/temp_error/queued_at NULL (Р2)", r)
	}
	countersVerifyEmpty(t, pool, "после temp_error-ветки DNS (Р2)")

	// R8: the temp_error retry — done → pending, temp_attempts + 1 (the
	// maturity bucket of the past day is placed by hand: today's episodes
	// do not ripen, the 24 h window is by date — the check age is outside
	// the window).
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 2 WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст temp_error: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) "+
			"VALUES ('retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD'), 1)"); err != nil {
		t.Fatalf("корзина зрелости: %v", err)
	}
	tr, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
	if err != nil || tr.Requeued != 1 || tr.Finalized != 0 {
		t.Fatalf("RetryOnce = %+v err=%v, хочу Requeued=1 (Р8)", tr, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		r.status != int16(model.StatusTempError) || r.tempAttempts != 1 {
		t.Fatalf("после повтора temp_error = %+v, хочу pending/temp_error/attempts=1 (Р8)", r)
	}
	countersVerifyEmpty(t, pool, "после повтора temp_error (Р8)")

	// R14: a delete file — status=deleted, check_state does NOT change (pending).
	dbatch, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, 'spec', $3, 1)",
		dbatch[:], zone, int16(model.ActionDelete)); err != nil {
		t.Fatalf("staging delete: %v", err)
	}
	dres, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: dbatch, ZoneID: zone, Action: model.ActionDelete})
	if err != nil || dres.Deleted != 1 {
		t.Fatalf("ApplyDelete = %+v err=%v, хочу Deleted=1 (Р14)", dres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		r.status != int16(model.StatusDeleted) {
		t.Fatalf("после delete = %+v, хочу pending/deleted — check_state не меняется (Р14)", r)
	}
	countersVerifyEmpty(t, pool, "после delete (Р14)")

	// R9: revival via an add file — new/pending, no lease.
	radd := checkStateStageAdd(ctx, t, pool, zone, "spec")
	if radd.Revived != 1 || radd.Inserted != 0 {
		t.Fatalf("ApplyAdd воскрешение = %+v, хочу Revived=1 (Р9)", radd)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) ||
		r.status != int16(model.StatusNew) || !r.queuedAtNull {
		t.Fatalf("после воскрешения = %+v, хочу new/pending/queued_at NULL (Р9)", r)
	}
	countersVerifyEmpty(t, pool, "после воскрешения (Р9)")
}

// TestCheckState_LeaseNullRepair — the terminal FR-1.2 guard: a queued
// row with NULL queued_at (a broken I1 invariant) is invisible to the
// main lease pass (NULL is filtered out by the age comparison — an
// eternal hang before the guard), returned by the LeaseNullOnce repair
// pass with the crawler_scheduler_lease_null_returned_total counter; a
// repeat pass is 0 (idempotence); the zone deltas are maintained by the
// transition statement.
func TestCheckState_LeaseNullRepair(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "fsm-null.test")
	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO domains (zone_id, label, check_state, status, queued_at)
		 VALUES ($1, 'broken', $2, $3, NULL) RETURNING id`,
		zone, int16(model.CheckStateQueued), int16(model.StatusNew),
	).Scan(&id); err != nil {
		t.Fatalf("домен queued/NULL: %v", err)
	}
	// A manual INSERT bypassing the incremental deltas: reconciliation is
	// the authority, after it the zone counters are true (1 queued).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")

	// The main lease pass does NOT see the row: NULL is filtered out by the comparison.
	lres, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if lres.Domains != 0 {
		t.Fatalf("основной lease-проход вернул NULL-строку: %+v, хочу 0 — NULL отсеивается возрастом", lres)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) {
		t.Fatalf("строка изменилась основным проходом: %+v, хочу queued", r)
	}

	// The FR-1.2 repair pass: return to pending, the counter grows.
	reg := metrics.New("scheduler")
	nres, err := scheduler.LeaseNullOnce(ctx, pool, scheduler.LeaseOptions{Metrics: reg})
	if err != nil || nres.Domains != 1 || nres.Selected != 1 {
		t.Fatalf("LeaseNullOnce = %+v err=%v, хочу Domains=Selected=1", nres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) || !r.queuedAtNull {
		t.Fatalf("после страховки = %+v, хочу pending/queued_at NULL (Р6)", r)
	}
	countersVerifyEmpty(t, pool, "после страховки (Р6) — дельты вёл оператор перевода")
	if got := checkStateNullReturned(t, reg); got != 1 {
		t.Fatalf("счётчик страховки = %v, хочу 1", got)
	}

	// A repeat pass — empty (the changed rows are no longer selected).
	repeat, err := scheduler.LeaseNullOnce(ctx, pool, scheduler.LeaseOptions{Metrics: reg})
	if err != nil || repeat != (scheduler.LeaseNullResult{}) {
		t.Fatalf("повторный LeaseNullOnce = %+v err=%v, хочу нулевой (идемпотентность)", repeat, err)
	}
	if got := checkStateNullReturned(t, reg); got != 1 {
		t.Fatalf("счётчик после повтора = %v, хочу 1 (пустой проход не считается)", got)
	}

	// The EXPLAIN contract of the guard selection (the
	// TestLease_CursorPredicateIndex style): the queued_at IS NULL
	// predicate + ORDER BY of the key are served by the leaf instance of
	// domains_lease_idx (T-104) — index usability is preserved, no seq
	// scan over domains.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatalf("SET LOCAL: %v", err)
	}
	plan := checkStateExplain(ctx, t, tx, fmt.Sprintf(
		"SELECT zone_id, id FROM domains "+
			"WHERE check_state = %d AND status <> %d AND queued_at IS NULL "+
			"ORDER BY queued_at, zone_id, id LIMIT %d",
		int16(model.CheckStateQueued), int16(model.StatusDeleted), scheduler.DefaultLeaseBatch))
	if !strings.Contains(plan, "queued_at_zone_id_id_idx") {
		t.Errorf("выборка страховки не обслуживается листовым экземпляром domains_lease_idx:\n%s", plan)
	}
	if strings.Contains(plan, "Seq Scan on domains") {
		t.Errorf("выборка страховки деградировала в seq scan по domains:\n%s", plan)
	}
}

// checkStateNullReturned — the guard counter value in the registry.
func checkStateNullReturned(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_scheduler_lease_null_returned_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// checkStateExplain — the EXPLAIN (COSTS OFF) plan text of a query.
func checkStateExplain(ctx context.Context, t *testing.T, tx pgx.Tx, query string) string {
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

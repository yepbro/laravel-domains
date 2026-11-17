// Integration test of the enqueue cycle's repair of an unenqueueable row
// (T-309, the R16 transition of the check_state spec) against live
// PostgreSQL (an isolated schema with migrations — the counters_test.go
// style): a pending row whose label was stretched by manual SQL beyond the
// spec 5.2 limit (the штатный ingest rejects such names — manual SQL is the
// source of the violation, the same trust boundary as checkstate.go I1)
// does not block the batch — the valid rows are moved and published, the
// poison one gets the terminal diagnosis (status=invalid, check_state=done,
// queued_at NULL, the http_published bit cleared, last_checked_at today),
// the zone counters converge (pending −1 / checked +1, T-178); a repeated
// batch is an idempotent no-op for the diagnosed row; the dispatcher no
// longer selects it. The unit tests (internal/scheduler) pin the SQL text;
// this file pins that the statement actually executes against the schema of
// migrations 000001/000016 (the cast of the typed arrays, the correlated
// zone probe, the CTE deltas).
package integration

import (
	"context"
	"strings"
	"testing"

	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/scheduler"
)

// TestEnqueueInvalidDiagnosis — the T-309 scenario on a live schema.
func TestEnqueueInvalidDiagnosis(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "invalid.test")
	ids := countersSeedDomains(t, pool, zone, "v", 2, model.CheckStatePending, model.StatusNew)

	// The poison row: the label is stretched by manual SQL beyond the spec
	// 5.2 limit — 250 + 1 + octet_length('invalid.test') = 262 > 253 (the
	// full name is assembled exactly this way by dispatch.go). The staging
	// http_published bit (4) is set together with is_idn (1): the diagnosis
	// must clear the bit and preserve the others (the R13 form).
	long := strings.Repeat("a", 250)
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET label = $3, flags = 5 WHERE zone_id = $1 AND id = $2",
		zone, ids[0], long); err != nil {
		t.Fatalf("the manual SQL corruption of the label: %v", err)
	}
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("reconciliation (the baseline): %v", err)
	}

	// The batch as the dispatcher would select it: the poison row at the
	// head of the zone queue, then the valid one. Before T-309 this batch
	// was rejected whole; now only the valid row is moved and published.
	q := &countersStubQueue{}
	batch := []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: ids[0], Name: long + ".invalid.test"}, // 262 octets
		{ZoneID: zone, DomainID: ids[1], Name: "v1.invalid.test"},
	}
	res, err := scheduler.EnqueueBatch(ctx, pool, q, batch)
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if res != (scheduler.EnqueueResult{Queued: 1, Published: 1, Skipped: 0, Invalid: 1, Diagnosed: 1}) {
		t.Fatalf("EnqueueBatch = %+v, want {Queued:1 Published:1 Invalid:1 Diagnosed:1}", res)
	}
	if q.published != 1 {
		t.Fatalf("publications = %d, want 1 — only the valid row is published", q.published)
	}

	// The terminal state of the poison row (R16): invalid/done, no lease
	// mark, the staging bit cleared (is_idn preserved), checked today.
	var status, checkState, flags int16
	var queuedNull, checkedToday bool
	if err := pool.QueryRow(ctx,
		"SELECT status, check_state, queued_at IS NULL, last_checked_at = CURRENT_DATE, flags "+
			"FROM domains WHERE zone_id = $1 AND id = $2", zone, ids[0],
	).Scan(&status, &checkState, &queuedNull, &checkedToday, &flags); err != nil {
		t.Fatalf("the poison row after the diagnosis: %v", err)
	}
	if status != int16(model.StatusInvalid) || checkState != int16(model.CheckStateDone) ||
		!queuedNull || !checkedToday || flags != 1 {
		t.Fatalf("the poison row: status=%d check_state=%d queued_at_NULL=%v checked_today=%v flags=%d, "+
			"want invalid/done/true/true/1 (the http_published bit cleared, is_idn kept)",
			status, checkState, queuedNull, checkedToday, flags)
	}

	// The zone counters: pending −1 / checked +1 by the diagnosis statement
	// (T-178), queued +1 by the move — the cross-check converges.
	var pending, queued, checked int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_pending, domains_queued, domains_checked FROM zones WHERE id = $1", zone,
	).Scan(&pending, &queued, &checked); err != nil {
		t.Fatalf("the zone counters: %v", err)
	}
	if pending != 0 || queued != 1 || checked != 1 {
		t.Fatalf("zone counters: pending=%d queued=%d checked=%d, want 0/1/1", pending, queued, checked)
	}
	countersVerifyEmpty(t, pool, "after the diagnosis")

	// The cycle is unblocked: pending of the zone is empty — the dispatcher
	// (which selects exactly pending) returns an empty batch instead of
	// re-selecting the poison head forever.
	got, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("DispatchBatch after the diagnosis: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("the selection after the diagnosis = %d rows, want 0 (the diagnosed row left pending)", len(got))
	}

	// A repeated batch is an idempotent no-op: the poison is filtered again
	// by the name (Invalid=1), but the diagnosis guard finds nothing to move
	// (Diagnosed=0 — the row is done); the valid row is queued — the move's
	// pending guard skips it (Skipped=1); nothing is published.
	res2, err := scheduler.EnqueueBatch(ctx, pool, q, batch)
	if err != nil {
		t.Fatalf("a repeated EnqueueBatch: %v", err)
	}
	if res2 != (scheduler.EnqueueResult{Queued: 0, Published: 0, Skipped: 1, Invalid: 1, Diagnosed: 0}) {
		t.Fatalf("a repeated EnqueueBatch = %+v, want {Skipped:1 Invalid:1 Diagnosed:0}", res2)
	}
	if q.published != 1 {
		t.Fatalf("publications after the repeat = %d, want still 1", q.published)
	}
	countersVerifyEmpty(t, pool, "after the repeated batch")
}

// Unit tests of the terminal queued/NULL queued_at guard (T-255, the
// TZ-fsm spec §3 FR-1.2) on a fake pgx pool with transactions (an extension
// of leaseFakePool — lease_test.go): the exact SQL texts of both statements and
// parameters (the batch LIMIT, the queued + IS NULL guard, the queued_at reset, the
// check_state/status codes as literals), the pair of statements in one transaction,
// counting by RETURNING, an empty batch — a no-op, the guard filtered out changed
// rows, step errors and a nil pool; the
// crawler_scheduler_lease_null_returned_total counter grows by the moves,
// the LeaseWatch cycle drain in batches until exhaustion, surviving an error.
// Behavior against a live DB (including the usability of domains_lease_idx) —
// integration tests (test/integration/checkstate_test.go).
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// --- stubs -------------------------------------------------------------

// leaseNullSeedBatch — a guard batch of n rows: one zone, ids in order.
func leaseNullSeedBatch(n int, translated int64) leaseNullBatch {
	rows := make([]leaseNullRow, n)
	for i := range rows {
		rows[i] = leaseNullRow{zone: 1, id: int64(i + 1)}
	}
	return leaseNullBatch{rows: rows, translated: translated}
}

// --- expected SQL texts (the FR-1.2 contract) --------------------------------

const wantLeaseNullSelect = "SELECT zone_id, id FROM domains " +
	"WHERE check_state = 1 AND status <> 6 AND queued_at IS NULL " +
	"ORDER BY queued_at, zone_id, id LIMIT $1"

const wantLeaseNullUpdate = "WITH upd AS (UPDATE domains AS t " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) " +
	"WHERE t.zone_id = u.zone_id AND t.id = u.id " +
	"AND t.check_state = 1 AND t.status <> 6 AND t.queued_at IS NULL " +
	"RETURNING t.zone_id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_queued = z.domains_queued - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT zone_id FROM upd"

// --- LeaseNullOnce ---------------------------------------------------------

// TestLeaseNullOnceSQL — the contract of both guard statements: the batch SELECT
// of queued/NULL without deleted by the index key, UPDATE … FROM unnest with
// guard re-checks (IS NULL, the queued_at reset, zone deltas); exact
// parameters, the operation order of one transaction, counting by RETURNING.
func TestLeaseNullOnceSQL(t *testing.T) {
	pool := &leaseFakePool{nullBatches: []leaseNullBatch{leaseNullSeedBatch(3, 2)}}

	res, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{BatchSize: 7000})
	if err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}

	ops := pool.opsSnapshot()
	want := []string{"begin", "null_select", "null_update", "commit"}
	if len(ops) != len(want) {
		t.Fatalf("порядок операций = %v, want %v — пара операторов в одной транзакции", ops, want)
	}
	for i := range want {
		if ops[i] != want[i] {
			t.Fatalf("порядок операций = %v, want %v", ops, want)
		}
	}

	sel := pool.nullSelectSnapshot()
	if len(sel) != 1 {
		t.Fatalf("SELECT-вызовов страховки = %d, want 1", len(sel))
	}
	if sel[0].sql != wantLeaseNullSelect {
		t.Errorf("SQL выборки страховки:\n%s\nwant:\n%s", sel[0].sql, wantLeaseNullSelect)
	}
	if len(sel[0].args) != 1 || sel[0].args[0] != 7000 {
		t.Errorf("аргументы выборки = %v, want [7000] (LIMIT пачки)", sel[0].args)
	}

	upd := pool.nullUpdateSnapshot()
	if len(upd) != 1 {
		t.Fatalf("UPDATE-вызовов страховки = %d, want 1", len(upd))
	}
	if upd[0].sql != wantLeaseNullUpdate {
		t.Errorf("SQL перевода страховки:\n%s\nwant:\n%s", upd[0].sql, wantLeaseNullUpdate)
	}
	wantUpdArgs := []any{
		int16(model.CheckStatePending), // SET check_state
		[]int16{1, 1, 1},               // the zones of the selected batch
		[]int64{1, 2, 3},               // the domains of the selected batch
	}
	if !reflect.DeepEqual(upd[0].args, wantUpdArgs) {
		t.Errorf("аргументы перевода = %v, want %v", upd[0].args, wantUpdArgs)
	}

	// Counting by RETURNING: 2 of the selected 3 moved (the guard filtered out one).
	if res.Domains != 2 || res.Selected != 3 {
		t.Errorf("итог = %+v, want Domains=2 (RETURNING), Selected=3", res)
	}
}

// TestLeaseNullOnceDefaults — zero options: the LIMIT is the default batch
// of the main pass (one size for the LeaseWatch cycle).
func TestLeaseNullOnceDefaults(t *testing.T) {
	pool := &leaseFakePool{nullBatches: []leaseNullBatch{leaseNullSeedBatch(1, 1)}}
	if _, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{}); err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	sel := pool.nullSelectSnapshot()
	if len(sel) != 1 || sel[0].args[0] != DefaultLeaseBatch {
		t.Fatalf("LIMIT = %v, want дефолт %d", sel[0].args, DefaultLeaseBatch)
	}
}

// TestLeaseNullOnceEmpty — an exhausted set: an empty batch — only
// the SELECT; the move and commit are not executed, the outcome is zero.
func TestLeaseNullOnceEmpty(t *testing.T) {
	pool := &leaseFakePool{} // the guard batch queue is empty

	res, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	if res != (LeaseNullResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	ops := pool.opsSnapshot()
	if len(ops) != 2 || ops[0] != "begin" || ops[1] != "null_select" {
		t.Errorf("порядок операций = %v, want [begin null_select] — пустая пачка без перевода", ops)
	}
}

// TestLeaseNullOnceGuardFiltered — the move's guard filtered out part of the batch
// (the rows' fate changed between the selection and the lock): the count is by
// RETURNING; the unnest hits the entire selected batch.
func TestLeaseNullOnceGuardFiltered(t *testing.T) {
	pool := &leaseFakePool{nullBatches: []leaseNullBatch{leaseNullSeedBatch(5, 1)}}

	res, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	if res.Domains != 1 || res.Selected != 5 {
		t.Fatalf("итог = %+v, want Domains=1 (RETURNING после guard), Selected=5", res)
	}
	upd := pool.nullUpdateSnapshot()
	if got := len(upd[0].args[1].([]int16)); got != 5 {
		t.Errorf("unnest-ключей перевода = %d, want 5 — пробивается вся выбранная пачка", got)
	}
}

// TestLeaseNullOnceError — pass step errors are wrapped with the guard
// context; a nil pool — the "pool not set" error.
func TestLeaseNullOnceError(t *testing.T) {
	selectPool := &leaseFakePool{
		nullBatches:    []leaseNullBatch{leaseNullSeedBatch(2, 2)},
		nullSelectErrs: []error{errors.New("бд недоступна")},
	}
	_, err := LeaseNullOnce(context.Background(), selectPool, LeaseOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "scheduler: страховка queued без queued_at") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка выборки = %v, want контекст страховки и исходная ошибка", err)
	}

	updatePool := &leaseFakePool{
		nullBatches:    []leaseNullBatch{leaseNullSeedBatch(2, 2)},
		nullUpdateErrs: []error{context.Canceled},
	}
	if _, err := LeaseNullOnce(context.Background(), updatePool, LeaseOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка перевода = %v, want context.Canceled", err)
	}

	commitPool := &leaseFakePool{
		nullBatches: []leaseNullBatch{leaseNullSeedBatch(2, 2)},
		commitErrs:  []error{errors.New("connection refused")},
	}
	if _, err := LeaseNullOnce(context.Background(), commitPool, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "фиксация") {
		t.Errorf("ошибка Commit = %v, want контекст фиксации", err)
	}

	beginPool := &leaseFakePool{beginErr: errors.New("нет соединений")}
	if _, err := LeaseNullOnce(context.Background(), beginPool, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "транзакция") {
		t.Errorf("ошибка Begin = %v, want контекст транзакции", err)
	}

	if _, err := LeaseNullOnce(context.Background(), nil, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// TestLeaseNullOnceMetrics — the crawler_scheduler_lease_null_returned_total counter
// grows by the rows actually moved (by RETURNING, not by the selection):
// two passes 2 + 0 — the outcome 2; without a registry (nil Metrics) the pass works.
func TestLeaseNullOnceMetrics(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &leaseFakePool{nullBatches: []leaseNullBatch{
		leaseNullSeedBatch(3, 2), // 2 moved (the guard filtered out one)
		leaseNullSeedBatch(0, 0), // the set is exhausted
	}}

	first, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	second, err := LeaseNullOnce(context.Background(), pool, LeaseOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("повторный LeaseNullOnce: %v", err)
	}
	if first.Domains != 2 || second.Domains != 0 {
		t.Fatalf("проходы = %+v, %+v; want Domains 2, 0", first, second)
	}

	if got := leaseNullReturnedValue(t, reg); got != 2 {
		t.Errorf("счётчик после проходов = %v, want 2 (по RETURNING, пустой проход не считается)", got)
	}

	// Without a registry — a no-op; the pass runs.
	noop := &leaseFakePool{nullBatches: []leaseNullBatch{leaseNullSeedBatch(1, 1)}}
	if _, err := LeaseNullOnce(context.Background(), noop, LeaseOptions{}); err != nil {
		t.Fatalf("LeaseNullOnce без метрик: %v", err)
	}
}

// leaseNullReturnedValue — the value of the guard counter in the registry
// (the driftGaugeValue style of the monitor tests).
func leaseNullReturnedValue(t *testing.T, reg *metrics.Registry) float64 {
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

// --- the LeaseWatch cycle: the repair pass -------------------------------------

// TestLeaseWatchNullDrain — the LeaseWatch cycle performs the repair
// guard pass on the same tick (after the main set's drain): a full
// batch continues the drain, an incomplete one stops it; the counter grows by
// the outcome; moved rows are not returned (a repeated pass — an empty batch).
func TestLeaseWatchNullDrain(t *testing.T) {
	reg := metrics.New("scheduler")
	// A full batch of DefaultLeaseBatch + an incomplete one of 2 — the drain in two
	// passes; the main set is empty (no batches).
	pool := &leaseFakePool{
		nullBatches: []leaseNullBatch{
			leaseNullSeedBatch(DefaultLeaseBatch, int64(DefaultLeaseBatch)),
			leaseNullSeedBatch(2, 2),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Interval: time.Hour, // the tick never comes: only the starting drain
			Metrics:  reg,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		sel := len(pool.nullSelectSnapshot())
		upd := len(pool.nullUpdateSnapshot())
		if sel >= 2 && upd >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("дренаж страховки не дошёл до второй пачки: selects=%d updates=%d", sel, upd)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if got := leaseNullReturnedValue(t, reg); got != float64(DefaultLeaseBatch+2) {
		t.Errorf("счётчик дренажа = %v, want %d", got, DefaultLeaseBatch+2)
	}
	// Both passes — by the single start tick: without a tick there are no new selections.
	if sel := len(pool.nullSelectSnapshot()); sel != 2 {
		t.Errorf("SELECT-вызовов страховки = %d, want 2 (неполная пачка останавливает)", sel)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}
}

// TestLeaseWatchNullDrainError — a guard batch error interrupts only
// its drain: the loop lives, the next tick retries (the OnRun contract); a Warn
// in the log.
func TestLeaseWatchNullDrainError(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &leaseFakePool{
		nullBatches: []leaseNullBatch{
			leaseNullSeedBatch(2, 2),
			leaseNullSeedBatch(1, 1),
		},
		nullSelectErrs: []error{errors.New("бд недоступна")}, // the first pass fails
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Timeout: 40 * time.Millisecond, // the interval default: 20 ms
			Log:     logger,
		})
	}()

	// The first drain fails; the tick retries and finishes both batches.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if upd := len(pool.nullUpdateSnapshot()); upd >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("очередной тик не повторил дренаж страховки: updates=%d", len(pool.nullUpdateSnapshot()))
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}

	log := buf.String()
	for _, want := range []string{
		"level=WARN",
		"бд недоступна",
		"строки queued без queued_at возвращены", // the drain outcome — a Warn (a signal to the operator)
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

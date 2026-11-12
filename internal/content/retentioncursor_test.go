// Unit tests of the persistent retention drain cursor (T-274,
// retentioncursor.go + the wiring in retentionPass/planCircle/RetentionLoop):
// the round trip (the upsert in the batch's TRANSACTION — the atomicity of
// "batch ⟺ cursor"), the main scenario "a restart continues from the
// cursor, not from zero" (RetentionLoop reads the state at start), the DB
// position is not moved past rows deferred by the guards (the loss of the
// retry list is compensated by re-reading the window), the circle plan
// clears the state at the moment of the plan and suppresses saves until
// the apply, invalid state → start from zero, missing table (42P01) —
// operation without persistence.
// The fake pool lives in retention_test.go (the cursor row is cursorRow,
// applied in Commit).
package content

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/model"
)

// runLoopPass — running RetentionLoop until the first completed pass (the
// starting drain) and stopping it: the unit of the "restart" scenarios —
// the cycle reads the persistent cursor BEFORE the first pass.
func runLoopPass(t *testing.T, pool *retentionFakePool, opts RetentionOptions) RetentionStats {
	t.Helper()
	passed := make(chan RetentionStats, 1)
	opts.OnPass = func(s RetentionStats, _ time.Duration) { passed <- s }
	opts.Retention = DefaultRetention
	opts.Interval = time.Hour // no ticker wait — the starting pass only
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()
	var stats RetentionStats
	select {
	case stats = <-passed:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("the loop's starting pass did not finish")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancel")
	}
	return stats
}

// TestRetentionPass_CursorSavedInBatchTx — the round trip: a committed
// batch commits the cursor with its OWN transaction (the upsert is the
// last statement of the purge tx); the position is the last row of the
// pass.
func TestRetentionPass_CursorSavedInBatchTx(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass: %v", err)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != want {
		t.Fatalf("cursor row = %+v, want %+v (the pass's last row)", pool.cursorRow, want)
	}
	if pool.cursorSaves != 1 || pool.begins != 2 {
		t.Fatalf("upserts = %d (transactions %d), want 1 upsert in the batch tx (2 transactions: wrapper + purge)",
			pool.cursorSaves, pool.begins)
	}
	// Atomicity: the upsert is in the PURGE TRANSACTION, as the last
	// statement (after purge/decref/delete, before Commit).
	tx := pool.txs[1]
	n := len(tx.calls)
	if n < 2 || tx.calls[n-1].sql != retentionCursorSaveSQL {
		t.Fatalf("the purge tx last statement = %v, want the cursor upsert", tx.calls[n-1].sql)
	}
	if !tx.committed {
		t.Fatal("the batch tx not committed")
	}
}

// TestRetentionPass_CursorSaveFailsWithBatch — an upsert failure rolls
// back the WHOLE BATCH (atomicity in both directions): neither the rows
// nor the cursor; a pass retry is idempotent from the previous position.
func TestRetentionPass_CursorSaveFailsWithBatch(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.cursorSaveErr = errors.New("db unavailable")
	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err == nil ||
		!strings.Contains(err.Error(), "saving the drain cursor") {
		t.Fatalf("error = %v, want the cursor save failure", err)
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("cursor = %+v (saves=%d), want not committed", pool.cursorRow, pool.cursorSaves)
	}
	for _, r := range pool.results {
		if r.hash == nil || !r.jsonb {
			t.Fatalf("row %v purged with a rolled-back batch", r.key)
		}
	}
	if st.cursor != (retentionCursor{}) {
		t.Fatalf("in-memory cursor = %+v, want zero (the batch not committed)", st.cursor)
	}
}

// TestRetentionPass_DeferredBatchKeepsCursor — a batch with rows deferred
// by the queue guards does NOT move the position in the DB (review of
// T-274, P1): the loss of the retry list on a restart is compensated by
// re-reading the guard window — the keys will be deferred again. Full and
// partial deferral.
func TestRetentionPass_DeferredBatchKeepsCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 1, 0))},
		analyze: []map[RetentionPendingKey]struct{}{{}},
	}
	st := &retentionState{persist: true}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("Rows = %d, want 0 (the whole batch deferred)", stats.Rows)
	}
	// The persistent position is in place (no state — as before the pass):
	// a restart will re-read the window and defer the keys again. The
	// in-memory cursor moves as before — the probe returns the keys within
	// the process.
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("cursor row = %+v (saves=%d), want no commit beyond the deferred batch",
			pool.cursorRow, pool.cursorSaves)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if st.cursor != want {
		t.Fatalf("in-memory cursor = %+v, want %+v (it advances even without purging)", st.cursor, want)
	}

	// Partial deferral: the purge proceeds but the position in the DB does
	// not move — the deferred key is not jumped over persistently.
	pool2 := drainPool(now)
	opts2 := retentionDefaultOpts()
	opts2.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0), {}},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}
	st2 := &retentionState{persist: true}
	stats2, err := retentionPass(context.Background(), pool2, opts2, st2)
	if err != nil {
		t.Fatalf("pass (a partial deferral): %v", err)
	}
	if stats2.Rows != 1 {
		t.Fatalf("Rows = %d, want 1 (sub=1 purged, sub=0 deferred)", stats2.Rows)
	}
	if pool2.cursorRow != nil || pool2.cursorSaves != 0 {
		t.Fatalf("cursor row = %+v (saves=%d), want no commit: the batch stepped over a deferred key",
			pool2.cursorRow, pool2.cursorSaves)
	}

	// Recovery "by a restart": the guard is off, a new process reads the
	// previous DB position (there is none — the zero cursor) and trims
	// both rows; now the batch has no deferrals — the position is
	// committed.
	pool.queries = nil
	statsRestart, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), &retentionState{persist: true})
	if err != nil {
		t.Fatalf("the recovery pass: %v", err)
	}
	if statsRestart.Rows != 2 {
		t.Fatalf("Rows = %d, want 2 (both deferred rows purged by the \"restart\")", statsRestart.Rows)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("the deferred record not purged after the \"restart\" — a loss behind the cursor")
	}
	if pool.cursorRow == nil || pool.cursorSaves != 1 {
		t.Fatalf("cursor row = %+v (saves=%d), want the batch commit with no deferrals",
			pool.cursorRow, pool.cursorSaves)
	}
}

// TestRetentionPass_CircleResetsPersistedCursor — a forced catch-up circle
// (the render-debt counter reset to zero) clears the persistent state
// BEFORE traversing from zero: a restart in the middle of the circle will
// continue the circle rather than return to the old cursor; circle batches
// write the position anew.
func TestRetentionPass_CircleResetsPersistedCursor(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	// 10 — under render debt (the flag): the record is protected, the
	// cursor will pass it; 20 — a regular non-find.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{flags: int16(model.FlagRenderPending)}
	pool.domains[retentionDomKey{zone: 1, id: 20}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	pool.counters["render_pending"] = 5

	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 1 (the debt epoch): %v", err)
	}
	if pool.cursorRow == nil {
		t.Fatal("pass 1 did not commit the cursor")
	}

	// The debt is lifted: the counter reset — pass 2 applies the catch-up
	// circle.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.counters["render_pending"] = 0
	pool.queries = nil
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("pass 2 (the debt lift): %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("pass 2: Rows = %d, want 1 (caught up with the debt-epoch record)", stats.Rows)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("pass 2 args = %v, want a catch-up circle from the zero cursor", pool.queries[0].args)
	}
	// The state was reset (DELETE) and rewritten by the circle batch: the
	// circle went from zero; the last visited row is the domain 10 record
	// (domain 20 is already clean from pass 1, outside the set).
	if pool.cursorDelete != 1 {
		t.Fatalf("cursor row deletes = %d, want 1 (the circle cleans the persistent state)", pool.cursorDelete)
	}
	want := retentionCursor{checkedAt: old, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != want {
		t.Fatalf("cursor row after the circle = %+v, want %+v (the circle batch position)", pool.cursorRow, want)
	}
}

// TestRetentionLoop_ResumesFromPersistedCursor — THE main test of T-274:
// a "restart" (a new RetentionLoop) reads the saved cursor and continues
// the drain from it, not from zero — the first selection of the starting
// pass starts from the saved position; records behind the cursor are not
// visited.
func TestRetentionLoop_ResumesFromPersistedCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	// The "previous process" stopped at the first row.
	saved := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &saved

	stats := runLoopPass(t, pool, RetentionOptions{BatchSize: 100})
	if stats.Rows != 1 {
		t.Fatalf("the starting pass = %+v, want 1 row (only sub=1 ahead of the cursor)", stats)
	}
	wantArgs := []any{DefaultRetention.String(), saved.checkedAt, saved.key.zone, saved.key.domain, saved.key.sub, saved.key.variant, 100}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("the first selection = %v, want continuing from the saved cursor %v", pool.queries[0].args, wantArgs)
	}
	// sub=0 is behind the cursor — untouched; after the pass the cursor is
	// on sub=1.
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("a record behind the cursor purged despite continuing from the cursor")
	}
	after := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != after {
		t.Fatalf("cursor row after the pass = %+v, want %+v", pool.cursorRow, after)
	}
}

// TestRetentionLoop_InvalidPersistedCursor — invalid state (a stamp in the
// future beyond the clock allowance): a Warn and the previous behavior —
// start from the zero cursor, a full catch-up.
func TestRetentionLoop_InvalidPersistedCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	bad := retentionCursor{checkedAt: time.Now().Add(time.Hour), key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &bad

	var buf bytes.Buffer
	opts := RetentionOptions{BatchSize: 100, Log: retentionTestLogger(&buf)}
	stats := runLoopPass(t, pool, opts)
	if stats.Rows != 2 {
		t.Fatalf("the starting pass = %+v, want 2 rows (a full catch-up from zero)", stats)
	}
	if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("the first selection = %v, want the zero cursor", pool.queries[0].args)
	}
	if !strings.Contains(buf.String(), "malformed") {
		t.Errorf("log = %q, want the malformed-state Warn", buf.String())
	}
	// The invalid row is overwritten by the very first batch commit.
	if pool.cursorRow == nil || pool.cursorRow.checkedAt.Equal(bad.checkedAt) {
		t.Fatalf("cursor row after the pass = %+v, want overwritten by the batch", pool.cursorRow)
	}
}

// TestRetentionLoop_NoCursorTable — the DB is before migration 000020
// (42P01): an Info, persistence is disabled for the process — the drain
// works, there are no saves (a degradation to the T-273 behavior, not a
// breakage).
func TestRetentionLoop_NoCursorTable(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.cursorErr = &pgconn.PgError{Code: "42P01"} // undefined_table

	var buf bytes.Buffer
	opts := RetentionOptions{BatchSize: 100, Log: retentionTestLogger(&buf)}
	stats := runLoopPass(t, pool, opts)
	if stats.Rows != 2 {
		t.Fatalf("the starting pass = %+v, want 2 rows (the drain works without the table)", stats)
	}
	if pool.cursorSaves != 0 {
		t.Fatalf("cursor upserts = %d, want 0 (persistence disabled)", pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "the persistent cursor table is missing") {
		t.Errorf("log = %q, want the missing-table Info", buf.String())
	}
}

// TestRetentionLoop_NoCursorTableMidRun — the table disappeared after the
// start (a reset in the middle of operation): the first batch failure
// disables persistence (an Info), the pass completes without an error, and
// the purge is retried by the next one.
func TestRetentionLoop_NoCursorTableMidRun(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.cursorSaveErr = &pgconn.PgError{Code: "42P01"} // the upsert fails in the batch tx

	var buf bytes.Buffer
	opts := RetentionOptions{BatchSize: 100, Log: retentionTestLogger(&buf)}
	stats := runLoopPass(t, pool, opts)
	if stats.Rows != 0 {
		t.Fatalf("the starting pass = %+v, want 0 rows (the batch rolled back entirely)", stats)
	}
	if pool.cursorSaves != 0 {
		t.Fatalf("upserts = %d, want 0", pool.cursorSaves)
	}
	// Persistence is off, the rows are intact — a repeated pass purges.
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), &retentionState{}); err != nil {
		t.Fatalf("a repeated pass: %v", err)
	}
	if !strings.Contains(buf.String(), "the persistent cursor table is missing") {
		t.Errorf("log = %q, want the missing-table Info", buf.String())
	}
}

// TestRetentionOnce_DoesNotTouchCursor — the one-off pass (the runbooks
// T-167 path A, isolated builds) neither reads nor writes the persistent
// state of the cycle: the isolated drain does not move the owner's
// position.
func TestRetentionOnce_DoesNotTouchCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	saved := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &saved

	if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if pool.cursorSaves != 0 || pool.cursorDelete != 0 {
		t.Fatalf("upserts=%d deletes=%d, want 0/0 (a one-off pass without persistence)",
			pool.cursorSaves, pool.cursorDelete)
	}
	if pool.cursorRow == nil || *pool.cursorRow != saved {
		t.Fatalf("cursor row = %+v, want untouched %+v", pool.cursorRow, saved)
	}
}

// TestRetentionCursorLoadContract — the SQL texts of the persistent cursor
// are pinned by anchors (the contract of migration 000020: a single row,
// an upsert by PK, a DELETE on circles).
func TestRetentionCursorLoadContract(t *testing.T) {
	if retentionCursorLoadSQL != "SELECT checked_at, zone_id, domain_id, subdomain_id, variant FROM retention_drain_cursor WHERE id = 1" {
		t.Fatalf("retentionCursorLoadSQL diverged from the migration 000020 contract:\n%s", retentionCursorLoadSQL)
	}
	for _, want := range []string{
		"INSERT INTO retention_drain_cursor",
		"ON CONFLICT (id) DO UPDATE SET",
		"updated_at = EXCLUDED.updated_at",
	} {
		if !strings.Contains(retentionCursorSaveSQL, want) {
			t.Errorf("retentionCursorSaveSQL lost the fragment %q:\n%s", want, retentionCursorSaveSQL)
		}
	}
	if retentionCursorDeleteSQL != "DELETE FROM retention_drain_cursor WHERE id = 1" {
		t.Fatalf("retentionCursorDeleteSQL diverged from the contract:\n%s", retentionCursorDeleteSQL)
	}
}

// TestRetentionLoadCursor_ZeroRow — an all-zero row (manual SQL) is
// treated as "no state", not as a year-old position.
func TestRetentionLoadCursor_ZeroRow(t *testing.T) {
	pool := drainPool(retentionTestNow())
	pool.cursorRow = &retentionCursor{}
	cur, ok, err := retentionLoadCursor(context.Background(), pool)
	if err != nil || ok || cur != (retentionCursor{}) {
		t.Fatalf("load = (%+v, %v, %v), want an empty state without an error", cur, ok, err)
	}
}

// TestRetentionPass_OverflowPlanResetsCursorAndSuppressesSaves — a circle
// plan in the middle of a pass (the retry list overflow): the cursor row
// is deleted AT THE MOMENT OF THE PLAN (a restart in the window between
// the plan and the apply lands in a full catch-up, review of T-274 P2),
// and the subsequent batches of the same pass do not resurrect the
// position (saving is suppressed until the circle apply); the next pass
// applies the circle from zero.
func TestRetentionPass_OverflowPlanResetsCursorAndSuppressesSaves(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	// A, B — under protection (they overflow the limit of 1), C — a clean
	// candidate AFTER the overflow: its batch must not resurrect the
	// cursor row.
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 0}, checkedAt: old.Add(2 * time.Hour), hash: retentionHash16(0x33), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}
	protected := mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 1, 0), pendingKey(1, 10, 2, 0))
	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // a batch = a row: A → the limit, B → the plan, C → the purge
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{protected, protected, protected, protected},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}, {}, {}},
	}

	var buf bytes.Buffer
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{persist: true, retryLimit: 1}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	// A is deferred (the list), B overflowed (the circle plan), C is also
	// protected in the snapshot: Rows=0, the plan is set, applying — the
	// next pass.
	if stats.Rows != 0 || !st.circle {
		t.Fatalf("pass = %+v (circle=%v), want 0 rows and a circle plan", stats, st.circle)
	}
	if pool.cursorDelete != 1 {
		t.Fatalf("cursor row deletes = %d, want 1 (the circle plan cleans the DB at plan time)", pool.cursorDelete)
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("cursor row = %+v (saves=%d), want suppressed until the circle applies", pool.cursorRow, pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "overflowed") {
		t.Errorf("log = %q, want the overflow Warn", buf.String())
	}

	// The next pass applies the circle from the zero cursor: all three
	// rows are candidates again (the protection is still on — the keys are
	// deferred anew; the circle rebuilds the set).
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("pass 2 (the circle): %v", err)
	}
	if len(pool.queries) == 0 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(1)) {
		t.Fatalf("pass 2 args = %v, want a catch-up circle from the zero cursor", pool.queries[0].args)
	}
}

// TestRetentionPass_PlanResetFailureWarns — a failure to reflect the
// circle plan in the DB is a Warn without a pass error (the best effort:
// the circle still happens in memory; a restart in the window degrades to
// continuing from the old position — costlier, but without data loss).
// The circle plan is not consumed, the row... remains, and the saving is
// suppressed anyway.
func TestRetentionPass_PlanResetFailureWarns(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	protected := mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 1, 0))
	opts := retentionDefaultOpts()
	opts.BatchSize = 1
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{protected, protected, protected},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}, {}},
	}
	// The commit of the reset fails (commitErr hits non-select
	// transactions — the circle plan's reset tx).
	pool.commitErr = errors.New("db unavailable")

	var buf bytes.Buffer
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{persist: true, retryLimit: 1}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("the plan reflection failure is not a pass error: %v", err)
	}
	if !st.circle {
		t.Fatal("the circle plan lost on the DB reflection failure")
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("cursor row = %+v (saves=%d), want no resurrection (the save suppressed)", pool.cursorRow, pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "not reflected in the DB") {
		t.Errorf("log = %q, want the unreflected-plan Warn", buf.String())
	}
}

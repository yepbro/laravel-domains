// Unit tests of the persistent retention drain cursor (T-274,
// retentioncursor.go + the wiring in retentionPass/planCircle/RetentionLoop):
// the round trip (the upsert in the batch's TRANSACTION — the atomicity of
// "batch ⟺ cursor"), the main scenario "a restart continues from the
// cursor, not from zero" (RetentionLoop reads the state at start), the DB
// position is not moved past rows deferred by the guards (the loss of the
// retry list is compensated by re-reading the window — including the
// multi-batch case of 09#P2-1), the circle plan clears the state at the
// moment of the plan and suppresses saves until the apply, the debt-era
// marker of T-295 (09#P2-2: the restart restores the era; NULL — an
// unknown state — circles from zero), invalid state → start from zero,
// missing table (42P01) — operation without persistence.
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
	"crawler/internal/queue"
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

// TestRetentionPass_DeferredThenCleanBatchesKeepCursor — the multi-batch
// regression of 09#P2-1: the len(batch) == selected check guarded only the
// batch with the deferrals themselves; a CLEAN batch of the same pass after
// a deferred one saved the DB position past the deferred key — a restart
// (the retry list is memory-only) lost the key forever. The save condition
// must hold while ANY deferred key is unresolved (the retry list is the
// floor of the position): clean batches keep the old row until the probe
// resolves the list; the first save-eligible batch after that commits.
func TestRetentionPass_DeferredThenCleanBatchesKeepCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	// sub=0 — deferred by the snapshot of pass 1; sub=1, sub=2 — clean
	// batches after it (BatchSize = 1).
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		checkedAt: pool.results[1].checkedAt.Add(time.Hour), hash: retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}
	opts := retentionDefaultOpts()
	opts.BatchSize = 1
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0), {}, {}},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}, {}},
	}

	st := &retentionState{persist: true}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if stats.Rows != 2 || stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("pass 1 = %+v, want sub=1/sub=2 purged, sub=0 deferred", stats)
	}
	if len(st.retries) != 1 {
		t.Fatalf("the retry list = %d keys, want 1 (sub=0)", len(st.retries))
	}
	// The position must stay behind the deferred key despite the two clean
	// batches after it (the T-274 guarantee across batches).
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("cursor row = %+v (saves=%d), want none: a clean batch moved the position past the deferred key",
			pool.cursorRow, pool.cursorSaves)
	}
	want := retentionCursor{checkedAt: pool.results[2].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 0}}
	if st.cursor != want {
		t.Fatalf("in-memory cursor = %+v, want %+v (it advances; the probe owns the deferred key)", st.cursor, want)
	}

	// Pass 2: the protection is gone — the probe purges sub=0 in place and
	// the list resolves; the selection is empty (everything is behind the
	// in-memory cursor), so no save happens either: the DB position never
	// passed the deferred key while it was unresolved.
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("pass 2: Rows = %d, want 1 (the probe cleared sub=0)", stats.Rows)
	}
	if len(st.retries) != 0 {
		t.Fatalf("the retry list = %d, want 0 (resolved)", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("the deferred record not purged by the probe")
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("cursor row = %+v (saves=%d), want none: the position jumped over the deferred key's window",
			pool.cursorRow, pool.cursorSaves)
	}

	// A new aged row ahead — the first save-eligible batch commits the
	// position: the floor lifts with the resolved list.
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 3, variant: 0},
		checkedAt: pool.results[2].checkedAt.Add(time.Hour), hash: retentionHash16(0x44), jsonb: true,
	})
	pool.contents[retentionHash(0x44)] = retentionFakeContent{refc: 1, clen: 4000}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	after := retentionCursor{checkedAt: pool.results[3].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 3, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != after || pool.cursorSaves != 1 {
		t.Fatalf("cursor row = %+v (saves=%d), want %+v committed by the clean batch",
			pool.cursorRow, pool.cursorSaves, after)
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
	// The "previous process" stopped at the first row. Its row carries a
	// definite debt state (a T-295 binary always writes one): false — no
	// era was observed; NULL would mean a pre-T-295 row — an unknown
	// state, a circle (TestRetentionLoop_UnknownDebtStatePlansCircle).
	saved := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &saved
	noDebt := false
	pool.cursorDebt = &noDebt

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
	// The mid-run latch message covers both schemas (000020/000021).
	if !strings.Contains(buf.String(), "the persistent cursor state is unavailable") {
		t.Errorf("log = %q, want the unavailable-state Info", buf.String())
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
// are pinned by anchors (the contract of migrations 000020/000021: a
// single row, an upsert by PK with the debt state, a marker upsert, a
// DELETE on circles).
func TestRetentionCursorLoadContract(t *testing.T) {
	if retentionCursorLoadSQL != "SELECT checked_at, zone_id, domain_id, subdomain_id, variant, debt_seen FROM retention_drain_cursor WHERE id = 1" {
		t.Fatalf("retentionCursorLoadSQL diverged from the migration 000020/000021 contract:\n%s", retentionCursorLoadSQL)
	}
	for _, want := range []string{
		"INSERT INTO retention_drain_cursor",
		"ON CONFLICT (id) DO UPDATE SET",
		"debt_seen = EXCLUDED.debt_seen",
		"updated_at = EXCLUDED.updated_at",
	} {
		if !strings.Contains(retentionCursorSaveSQL, want) {
			t.Errorf("retentionCursorSaveSQL lost the fragment %q:\n%s", want, retentionCursorSaveSQL)
		}
	}
	for _, want := range []string{
		"INSERT INTO retention_drain_cursor",
		"ON CONFLICT (id) DO UPDATE SET debt_seen = true",
		// The zero-time literal (review of T-295, P3-1): the Go zero
		// stamp scans into time.Time{} — the marker-only row loads as
		// "no state"; '1970-01-01' would scan into time.Unix(0,0) and
		// load as a 1970 position.
		"VALUES (1, '0001-01-01 00:00:00+00', 0, 0, 0, 0, true, now())",
	} {
		if !strings.Contains(retentionDebtSeenSQL, want) {
			t.Errorf("retentionDebtSeenSQL lost the fragment %q:\n%s", want, retentionDebtSeenSQL)
		}
	}
	if retentionCursorDeleteSQL != "DELETE FROM retention_drain_cursor WHERE id = 1" {
		t.Fatalf("retentionCursorDeleteSQL diverged from the contract:\n%s", retentionCursorDeleteSQL)
	}
}

// TestRetentionLoadCursor_ZeroRow — an all-zero row (manual SQL) is
// treated as "no state", not as a year-old position; the debt marker of
// such a row (a marker-only insert of T-295) is still read.
func TestRetentionLoadCursor_ZeroRow(t *testing.T) {
	pool := drainPool(retentionTestNow())
	debt := true
	pool.cursorRow = &retentionCursor{}
	pool.cursorDebt = &debt
	cur, savedDebt, ok, err := retentionLoadCursor(context.Background(), pool)
	if err != nil || ok || cur != (retentionCursor{}) {
		t.Fatalf("load = (%+v, %v, %v), want an empty state without an error", cur, ok, err)
	}
	if savedDebt == nil || !*savedDebt {
		t.Fatalf("the debt marker of the zero row = %v, want true (a marker-only row)", savedDebt)
	}
}

// TestRetentionLoop_RestartsIntoDebtEra — the restart regression of
// 09#P2-2: the debt-era marker (debt_seen, migration 000021) restores
// the machine's debtEra state at the loop start. The finding's scenario:
// the render-debt era ENDED while the process was down — the first
// counter read of the new process already sees 0, the ">0 → 0"
// transition is never observed, and without the marker the records
// filtered out during the era stay behind the cursor forever; with the
// marker restored, the first observed zero applies the catch-up circle.
func TestRetentionLoop_RestartsIntoDebtEra(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	// The "previous process" stopped inside the era at the first row and
	// persisted the marker with the position (T-295).
	saved := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &saved
	debt := true
	pool.cursorDebt = &debt
	// The era ended in the downtime window: the counter is already 0
	// (the pool's default — the row exists, migration 000015).

	stats := runLoopPass(t, pool, RetentionOptions{BatchSize: 100})
	if stats.Rows != 2 {
		t.Fatalf("the starting pass = %+v, want 2 rows (the catch-up circle after the restored era)", stats)
	}
	if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("the first selection = %v, want the zero cursor (the circle)", pool.queries[0].args)
	}
	// The circle plan deleted the marker together with the position (the
	// plan-time DELETE), and the circle's batch wrote a fresh row without
	// the era.
	if pool.cursorDelete != 1 {
		t.Fatalf("cursor row deletes = %d, want 1 (the circle plan at the start)", pool.cursorDelete)
	}
	after := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != after || pool.cursorDebt == nil || *pool.cursorDebt {
		t.Fatalf("cursor row after the circle = %+v (debt=%v), want %+v with the era closed",
			pool.cursorRow, pool.cursorDebt, after)
	}
}

// TestRetentionLoop_UnknownDebtStatePlansCircle — a row written by a
// pre-T-295 binary (debt_seen IS NULL): the debt state is unknown, and
// the safe reading is a full catch-up circle from zero, not a guess — a
// debt era that ended while the process was down must not stay behind
// the cursor (09#P2-2). The circle heals the row: the first batch writes
// a definite state.
func TestRetentionLoop_UnknownDebtStatePlansCircle(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	// The row of the OLD binary: a position, no debt state (NULL).
	saved := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &saved
	pool.cursorDebt = nil

	var buf bytes.Buffer
	stats := runLoopPass(t, pool, RetentionOptions{BatchSize: 100, Log: retentionTestLogger(&buf)})
	if stats.Rows != 2 {
		t.Fatalf("the starting pass = %+v, want 2 rows (a full catch-up from zero)", stats)
	}
	if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("the first selection = %v, want the zero cursor (the unknown-state circle)", pool.queries[0].args)
	}
	if !strings.Contains(buf.String(), "no debt state") {
		t.Errorf("log = %q, want the unknown debt state Info", buf.String())
	}
	// The plan dropped the NULL row; the circle's batch wrote a definite
	// state (the NULL heals on the first committed batch).
	if pool.cursorDelete != 1 {
		t.Fatalf("cursor row deletes = %d, want 1 (the plan-time DELETE of the NULL row)", pool.cursorDelete)
	}
	after := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != after || pool.cursorDebt == nil {
		t.Fatalf("cursor row = %+v (debt=%v), want %+v with a definite debt state",
			pool.cursorRow, pool.cursorDebt, after)
	}
}

// TestRetentionPass_DebtEraMarkerPersisted — the era observation is
// persisted at the MOMENT it is made (retentionMarkDebtSeen, a separate
// short transaction — a standstill pass commits no batch, but a restart
// in the era must restore it); a repeated observation of the same era is
// not a transition — the marker is not re-written on every pass; the
// batch upserts of the era stamp the state by the same statement (a NULL
// row heals on the first committed batch).
func TestRetentionPass_DebtEraMarkerPersisted(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.counters["render_pending"] = 3 // the era is running

	// Pass 1 — a standstill: the selection is empty (no rows at all), no
	// batch is committed, but the marker must reach the DB.
	pool.results = nil
	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 1 (the standstill): %v", err)
	}
	if pool.debtMarks != 1 {
		t.Fatalf("debt marker upserts = %d, want 1 (the era persisted at the observation moment)", pool.debtMarks)
	}
	// The marker-only row: the zero position (a fresh insert) with the
	// era flag — the load treats the zero stamp as "no state" but reads
	// the marker (TestRetentionLoadCursor_ZeroRow).
	if pool.cursorRow == nil || *pool.cursorRow != (retentionCursor{}) || pool.cursorDebt == nil || !*pool.cursorDebt {
		t.Fatalf("cursor row = %+v (debt=%v), want the zero position with the era marker",
			pool.cursorRow, pool.cursorDebt)
	}

	// Pass 2 — the era continues: not a transition, no second marker.
	pool.results = nil
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if pool.debtMarks != 1 {
		t.Fatalf("debt marker upserts = %d, want still 1 (the era is idempotent)", pool.debtMarks)
	}

	// Pass 3 — rows appear ahead (a standstill pass cannot purge them,
	// but the batch upsert must stamp the era state with the position).
	old := now.Add(-2 * DefaultRetention)
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 5, variant: 0}, checkedAt: old, hash: retentionHash16(0x55), jsonb: true},
	}
	pool.contents[retentionHash(0x55)] = retentionFakeContent{refc: 1, clen: 5000}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	want := retentionCursor{checkedAt: old, key: retentionKey{zone: 1, domain: 10, sub: 5, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != want || pool.cursorDebt == nil || !*pool.cursorDebt {
		t.Fatalf("cursor row = %+v (debt=%v), want %+v stamped with the era by the batch upsert",
			pool.cursorRow, pool.cursorDebt, want)
	}
}

// TestRetentionPass_DebtMarkerRetriedAfterFailure — the write-once era
// entry lost the marker for the whole era when the single write hit a
// transient failure (review of T-295, P3-3): while the era runs
// unconfirmed, every debtActive observation retries the idempotent
// marker upsert until it reaches the DB; a confirmed era writes nothing.
func TestRetentionPass_DebtMarkerRetriedAfterFailure(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.counters["render_pending"] = 3 // the era is running
	pool.results = nil                  // a standstill: no batches, the marker tx is the only path

	// Pass 1 — the era entry: the marker write fails (a transient, not a
	// missing table/column).
	pool.debtMarkErr = errors.New("connection loss: unexpected EOF")
	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 1 (the failed marker): %v", err)
	}
	if st.mode != debtEraMode {
		t.Fatalf("mode = %v, want debtEra (a write failure does not cancel the era)", st.mode)
	}
	if st.debtMarked {
		t.Fatal("the era is confirmed after a FAILED marker write")
	}

	// Pass 2 — the same era (not a transition): the retry reaches the DB.
	pool.debtMarkErr = nil
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 2 (the marker retry): %v", err)
	}
	if !st.debtMarked {
		t.Fatal("the era is not persisted after the retry")
	}
	if pool.debtMarks != 1 {
		t.Fatalf("debt marker upserts = %d, want 1 (the retried entry)", pool.debtMarks)
	}
	// The marker-only row: the zero position with the era flag
	// (TestRetentionLoadCursor_ZeroRow).
	if pool.cursorRow == nil || *pool.cursorRow != (retentionCursor{}) || pool.cursorDebt == nil || !*pool.cursorDebt {
		t.Fatalf("cursor row = %+v (debt=%v), want the zero position with the era marker",
			pool.cursorRow, pool.cursorDebt)
	}

	// Pass 3 — the era continues confirmed: no further marker writes.
	pool.results = nil
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	if pool.debtMarks != 1 {
		t.Fatalf("debt marker upserts = %d, want still 1 (a confirmed era writes nothing)", pool.debtMarks)
	}
}

// TestRetentionPass_DebtMarkerAckedByBatch — the second confirmation
// path (review of T-295, P3-3): a committed batch upsert of the era
// stamps debt_seen=true with the position (the same statement), which
// acknowledges the marker even while the point-upsert keeps failing —
// and suppresses the retries after it.
func TestRetentionPass_DebtMarkerAckedByBatch(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.counters["render_pending"] = 3 // the era is running
	pool.results = nil                  // a standstill: the marker tx is the only path

	// Pass 1 — the era entry: the marker write fails and KEEPS failing.
	pool.debtMarkErr = errors.New("connection loss: unexpected EOF")
	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if st.debtMarked {
		t.Fatal("the era is confirmed after a FAILED marker write")
	}

	// Pass 2 — a row aged ahead: the batch upsert commits with the era
	// stamp (the batch upserts do not share the marker's failure) — the
	// ack comes from the batch, debtMarks stays at zero.
	old := now.Add(-2 * DefaultRetention)
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 5, variant: 0}, checkedAt: old, hash: retentionHash16(0x55), jsonb: true},
	}
	pool.contents[retentionHash(0x55)] = retentionFakeContent{refc: 1, clen: 5000}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 2 (the batch of the era): %v", err)
	}
	if !st.debtMarked {
		t.Fatal("the era is not confirmed by the batch upsert stamp")
	}
	if pool.debtMarks != 0 {
		t.Fatalf("debt marker upserts = %d, want 0 (the point-upsert keeps failing; the stamp rode the batch)",
			pool.debtMarks)
	}
	want := retentionCursor{checkedAt: old, key: retentionKey{zone: 1, domain: 10, sub: 5, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != want || pool.cursorDebt == nil || !*pool.cursorDebt {
		t.Fatalf("cursor row = %+v (debt=%v), want %+v stamped with the era by the batch upsert",
			pool.cursorRow, pool.cursorDebt, want)
	}

	// Pass 3 — a standstill again: the era is confirmed by the batch
	// stamp, no marker retries (the point-upsert would still fail — and
	// must not even be attempted).
	pool.results = nil
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	if pool.debtMarks != 0 {
		t.Fatalf("debt marker upserts = %d, want still 0 (the batch stamp is the ack)", pool.debtMarks)
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
	if stats.Rows != 0 || !st.circlePlanned() {
		t.Fatalf("pass = %+v (mode=%v), want 0 rows and a circle plan", stats, st.mode)
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
	if !st.circlePlanned() {
		t.Fatal("the circle plan lost on the DB reflection failure")
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("cursor row = %+v (saves=%d), want no resurrection (the save suppressed)", pool.cursorRow, pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "not reflected in the DB") {
		t.Errorf("log = %q, want the unreflected-plan Warn", buf.String())
	}
}

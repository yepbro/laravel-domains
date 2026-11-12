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
		t.Fatal("стартовый проход цикла не завершился")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
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
		t.Fatalf("проход: %v", err)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != want {
		t.Fatalf("строка курсора = %+v, want %+v (последняя строка прохода)", pool.cursorRow, want)
	}
	if pool.cursorSaves != 1 || pool.begins != 2 {
		t.Fatalf("upsert'ов = %d (транзакций %d), want 1 upsert в tx пачки (2 транзакции: оболочка + чистка)",
			pool.cursorSaves, pool.begins)
	}
	// Atomicity: the upsert is in the PURGE TRANSACTION, as the last
	// statement (after purge/decref/delete, before Commit).
	tx := pool.txs[1]
	n := len(tx.calls)
	if n < 2 || tx.calls[n-1].sql != retentionCursorSaveSQL {
		t.Fatalf("последний оператор tx чистки = %v, want upsert курсора", tx.calls[n-1].sql)
	}
	if !tx.committed {
		t.Fatal("tx пачки не зафиксирована")
	}
}

// TestRetentionPass_CursorSaveFailsWithBatch — an upsert failure rolls
// back the WHOLE BATCH (atomicity in both directions): neither the rows
// nor the cursor; a pass retry is idempotent from the previous position.
func TestRetentionPass_CursorSaveFailsWithBatch(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.cursorSaveErr = errors.New("бд недоступна")
	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err == nil ||
		!strings.Contains(err.Error(), "сохранение курсора") {
		t.Fatalf("ошибка = %v, want сбой сохранения курсора", err)
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("курсор = %+v (saves=%d), want не зафиксирован", pool.cursorRow, pool.cursorSaves)
	}
	for _, r := range pool.results {
		if r.hash == nil || !r.jsonb {
			t.Fatalf("строка %v очищена при откаченной пачке", r.key)
		}
	}
	if st.cursor != (retentionCursor{}) {
		t.Fatalf("in-memory курсор = %+v, want нулевой (пачка не зафиксирована)", st.cursor)
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
		t.Fatalf("проход: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("Rows = %d, want 0 (вся пачка отложена)", stats.Rows)
	}
	// The persistent position is in place (no state — as before the pass):
	// a restart will re-read the window and defer the keys again. The
	// in-memory cursor moves as before — the probe returns the keys within
	// the process.
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("строка курсора = %+v (saves=%d), want нет фиксации за отложенной пачкой",
			pool.cursorRow, pool.cursorSaves)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if st.cursor != want {
		t.Fatalf("in-memory курсор = %+v, want %+v (двигается и без чистки)", st.cursor, want)
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
		t.Fatalf("проход (частичная отложка): %v", err)
	}
	if stats2.Rows != 1 {
		t.Fatalf("Rows = %d, want 1 (sub=1 очищена, sub=0 отложена)", stats2.Rows)
	}
	if pool2.cursorRow != nil || pool2.cursorSaves != 0 {
		t.Fatalf("строка курсора = %+v (saves=%d), want нет фиксации: пачка перешагнула отложенный ключ",
			pool2.cursorRow, pool2.cursorSaves)
	}

	// Recovery "by a restart": the guard is off, a new process reads the
	// previous DB position (there is none — the zero cursor) and trims
	// both rows; now the batch has no deferrals — the position is
	// committed.
	pool.queries = nil
	statsRestart, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), &retentionState{persist: true})
	if err != nil {
		t.Fatalf("проход восстановления: %v", err)
	}
	if statsRestart.Rows != 2 {
		t.Fatalf("Rows = %d, want 2 (обе отложенные строки вычищены «рестартом»)", statsRestart.Rows)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("отложенная запись не вычищена после «рестарта» — потеря за курсором")
	}
	if pool.cursorRow == nil || pool.cursorSaves != 1 {
		t.Fatalf("строка курсора = %+v (saves=%d), want фиксация пачки без отложек",
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
		t.Fatalf("проход 1 (эпоха долга): %v", err)
	}
	if pool.cursorRow == nil {
		t.Fatal("проход 1 не зафиксировал курсор")
	}

	// The debt is lifted: the counter reset — pass 2 applies the catch-up
	// circle.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.counters["render_pending"] = 0
	pool.queries = nil
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2 (снятие долга): %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("проход 2: Rows = %d, want 1 (догнали запись эпохи долга)", stats.Rows)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("args прохода 2 = %v, want catch-up-круг с нулевого курсора", pool.queries[0].args)
	}
	// The state was reset (DELETE) and rewritten by the circle batch: the
	// circle went from zero; the last visited row is the domain 10 record
	// (domain 20 is already clean from pass 1, outside the set).
	if pool.cursorDelete != 1 {
		t.Fatalf("удалений строки курсора = %d, want 1 (круг чистит персистентное состояние)", pool.cursorDelete)
	}
	want := retentionCursor{checkedAt: old, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != want {
		t.Fatalf("строка курсора после круга = %+v, want %+v (позиция круговой пачки)", pool.cursorRow, want)
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
		t.Fatalf("стартовый проход = %+v, want 1 строка (только sub=1 впереди курсора)", stats)
	}
	wantArgs := []any{DefaultRetention.String(), saved.checkedAt, saved.key.zone, saved.key.domain, saved.key.sub, saved.key.variant, 100}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("первая выборка = %v, want продолжение от сохранённого курсора %v", pool.queries[0].args, wantArgs)
	}
	// sub=0 is behind the cursor — untouched; after the pass the cursor is
	// on sub=1.
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("запись позади курсора очищена вопреки продолжению с курсора")
	}
	after := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != after {
		t.Fatalf("строка курсора после прохода = %+v, want %+v", pool.cursorRow, after)
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
		t.Fatalf("стартовый проход = %+v, want 2 строки (полный catch-up с нуля)", stats)
	}
	if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("первая выборка = %v, want нулевой курсор", pool.queries[0].args)
	}
	if !strings.Contains(buf.String(), "некорректен") {
		t.Errorf("лог = %q, want Warn о некорректном состоянии", buf.String())
	}
	// The invalid row is overwritten by the very first batch commit.
	if pool.cursorRow == nil || pool.cursorRow.checkedAt.Equal(bad.checkedAt) {
		t.Fatalf("строка курсора после прохода = %+v, want перезаписана пачкой", pool.cursorRow)
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
		t.Fatalf("стартовый проход = %+v, want 2 строки (дренаж работает без таблицы)", stats)
	}
	if pool.cursorSaves != 0 {
		t.Fatalf("upsert'ов курсора = %d, want 0 (персистентность выключена)", pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "таблицы персистентного курсора нет") {
		t.Errorf("лог = %q, want Info об отсутствующей таблице", buf.String())
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
		t.Fatalf("стартовый проход = %+v, want 0 строк (пачка откатана целиком)", stats)
	}
	if pool.cursorSaves != 0 {
		t.Fatalf("upsert'ов = %d, want 0", pool.cursorSaves)
	}
	// Persistence is off, the rows are intact — a repeated pass purges.
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), &retentionState{}); err != nil {
		t.Fatalf("повторный проход: %v", err)
	}
	if !strings.Contains(buf.String(), "таблицы персистентного курсора нет") {
		t.Errorf("лог = %q, want Info об отсутствующей таблице", buf.String())
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
		t.Fatalf("upsert'ов=%d удалений=%d, want 0/0 (разовый проход без персистентности)",
			pool.cursorSaves, pool.cursorDelete)
	}
	if pool.cursorRow == nil || *pool.cursorRow != saved {
		t.Fatalf("строка курсора = %+v, want нетронутой %+v", pool.cursorRow, saved)
	}
}

// TestRetentionCursorLoadContract — the SQL texts of the persistent cursor
// are pinned by anchors (the contract of migration 000020: a single row,
// an upsert by PK, a DELETE on circles).
func TestRetentionCursorLoadContract(t *testing.T) {
	if retentionCursorLoadSQL != "SELECT checked_at, zone_id, domain_id, subdomain_id, variant FROM retention_drain_cursor WHERE id = 1" {
		t.Fatalf("retentionCursorLoadSQL разошёлся с контрактом миграции 000020:\n%s", retentionCursorLoadSQL)
	}
	for _, want := range []string{
		"INSERT INTO retention_drain_cursor",
		"ON CONFLICT (id) DO UPDATE SET",
		"updated_at = EXCLUDED.updated_at",
	} {
		if !strings.Contains(retentionCursorSaveSQL, want) {
			t.Errorf("retentionCursorSaveSQL потерял фрагмент %q:\n%s", want, retentionCursorSaveSQL)
		}
	}
	if retentionCursorDeleteSQL != "DELETE FROM retention_drain_cursor WHERE id = 1" {
		t.Fatalf("retentionCursorDeleteSQL разошёлся с контрактом:\n%s", retentionCursorDeleteSQL)
	}
}

// TestRetentionLoadCursor_ZeroRow — an all-zero row (manual SQL) is
// treated as "no state", not as a year-old position.
func TestRetentionLoadCursor_ZeroRow(t *testing.T) {
	pool := drainPool(retentionTestNow())
	pool.cursorRow = &retentionCursor{}
	cur, ok, err := retentionLoadCursor(context.Background(), pool)
	if err != nil || ok || cur != (retentionCursor{}) {
		t.Fatalf("load = (%+v, %v, %v), want пустое состояние без ошибки", cur, ok, err)
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
		t.Fatalf("проход: %v", err)
	}
	// A is deferred (the list), B overflowed (the circle plan), C is also
	// protected in the snapshot: Rows=0, the plan is set, applying — the
	// next pass.
	if stats.Rows != 0 || !st.circle {
		t.Fatalf("проход = %+v (circle=%v), want 0 строк и план круга", stats, st.circle)
	}
	if pool.cursorDelete != 1 {
		t.Fatalf("удалений строки курсора = %d, want 1 (план круга чистит БД в момент плана)", pool.cursorDelete)
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("строка курсора = %+v (saves=%d), want подавлено до применения круга", pool.cursorRow, pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "переполнен") {
		t.Errorf("лог = %q, want Warn о переполнении", buf.String())
	}

	// The next pass applies the circle from the zero cursor: all three
	// rows are candidates again (the protection is still on — the keys are
	// deferred anew; the circle rebuilds the set).
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 2 (круг): %v", err)
	}
	if len(pool.queries) == 0 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(1)) {
		t.Fatalf("args прохода 2 = %v, want catch-up-круг с нулевого курсора", pool.queries[0].args)
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
	pool.commitErr = errors.New("бд недоступна")

	var buf bytes.Buffer
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{persist: true, retryLimit: 1}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("сбой отражения плана — не ошибка прохода: %v", err)
	}
	if !st.circle {
		t.Fatal("план круга потерян при сбое отражения в БД")
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("строка курсора = %+v (saves=%d), want без воскресения (сохранение подавлено)", pool.cursorRow, pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "не отражён в БД") {
		t.Errorf("лог = %q, want Warn о неотражённом плане", buf.String())
	}
}

// Unit tests of the retention drain state (T-185, retentiondrain.go):
// the time cursor survives passes (a row is visited once in its lifetime,
// the next pass continues from the stop point), point offers of the
// queue-guard retries (the PK probe, eviction of non-candidates, the list
// bound), catch-up circles (the T-175 render-debt counter reset to zero,
// the fallback day on a missing row, the list overflow), and the return of
// a re-checked row to the drain after aging.
// The fake pool lives in retention_test.go (including the crawler_counters
// counter).
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

// drainPool — a pool with one live domain zone=1 (no flags) and two aged
// candidates sub=0/sub=1: the base of the drain scenarios.
func drainPool(now time.Time) *retentionFakePool {
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	return pool
}

// zeroArgs — the expected arguments of a selection from the zero cursor.
func zeroArgs(limit int) []any {
	return []any{DefaultRetention.String(), time.Time{}, int16(0), int64(0), int16(0), int16(0), limit}
}

// --- the cycle cursor (spec §4.2) ---------------------------------------------

// TestRetentionPass_CursorAcrossPasses — the cursor survives passes:
// the first drain stops at the last row of the window; the hourly pass
// continues from the stop point and does not re-read what has been passed;
// a record that aged over the hour is picked up without a full circle.
func TestRetentionPass_CursorAcrossPasses(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	// The third record is fresh (5.5 h < the 6 h window) — not a candidate
	// of the first pass.
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		checkedAt: now.Add(-5*time.Hour - 30*time.Minute), hash: retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}

	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("проход 1: Rows = %d, want 2 (sub=0, sub=1)", stats.Rows)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if st.cursor != want {
		t.Fatalf("курсор после прохода 1 = %+v, want %+v (последняя строка окна)", st.cursor, want)
	}

	// An hour later sub=2 has aged (5.5 h > 6 h − 1 h of the window) — the
	// pass continues from the cursor, without a full circle.
	pool.now = now.Add(time.Hour)
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("проход 2: Rows = %d, want 1 (только состаревшаяся sub=2)", stats.Rows)
	}
	if len(pool.queries) != 1 {
		t.Fatalf("SELECT'ов прохода 2 = %d, want 1", len(pool.queries))
	}
	wantArgs := []any{DefaultRetention.String(), want.checkedAt, want.key.zone, want.key.domain, want.key.sub, want.key.variant, 100}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("args прохода 2 = %v, want курсор прохода 1 %v", pool.queries[0].args, wantArgs)
	}
}

// TestRetentionPass_InterlockKeepsCursor — an interlock stop in the middle
// of a drain keeps the progress: the committed batches advanced the
// cursor; the next pass continues after them, not from zero.
func TestRetentionPass_InterlockKeepsCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		checkedAt: pool.results[1].checkedAt.Add(time.Hour), hash: retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}

	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // a batch = a row
	depth := &fakeDepth{n: 0}
	opts.Depth = depth
	opts.AnalyzeWatermark = 100
	var selects int
	pool.onSelect = func() {
		selects++
		if selects >= 1 {
			depth.mu.Lock()
			depth.n = 150 // the analyzer stopped keeping up after the first batch
			depth.mu.Unlock()
		}
	}

	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 1 || !stats.Skipped {
		t.Fatalf("проход 1 = %+v, want одна пачка и пропуск интерлоком", stats)
	}
	want := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	if st.cursor != want {
		t.Fatalf("курсор после остановки = %+v, want %+v (первая строка)", st.cursor, want)
	}

	// The depth dipped: the drain continues from the cursor — rows 1 and 2.
	depth.mu.Lock()
	depth.n = 0
	depth.mu.Unlock()
	pool.onSelect = nil // the depth hour has passed — no more race
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 2 || stats.Skipped {
		t.Fatalf("проход 2 = %+v, want 2 строки без пропуска", stats)
	}
	wantArgs := []any{DefaultRetention.String(), want.checkedAt, want.key.zone, want.key.domain, want.key.sub, want.key.variant, 1}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("args прохода 2 = %v, want продолжение от курсора %v", pool.queries[0].args, wantArgs)
	}
}

// TestRetentionPass_RewrittenReEnters — a recheck rewrite (DELETE+INSERT,
// a fresh checked_at and new content, T-058) is skipped by the guard in the
// current window and enters the drain again when it ages: a fresh stamp is
// always ahead of the cursor (spec §3.2).
func TestRetentionPass_RewrittenReEnters(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	// A rewrite between the selection and the purge: a fresh stamp, new
	// content.
	pool.onSelect = func() {
		r := pool.results[0]
		r.checkedAt, r.hash = now.Add(-time.Hour), retentionHash16(0x99)
		pool.results[0] = r
	}
	pool.contents[retentionHash(0x99)] = retentionFakeContent{refc: 1, clen: 1500}

	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("проход 1: Rows = %d, want 0 (охрана возраста)", stats.Rows)
	}

	// Six hours later the rewrite has aged — the drain picks it up.
	pool.now = now.Add(6*time.Hour + time.Minute)
	pool.onSelect = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 1 || stats.FreedBytes != 1500 {
		t.Fatalf("проход 2 = %+v, want чистка перезаписанной строки (новый контент 0x99)", stats)
	}
}

// --- queue guard retries (spec §4.3) ------------------------------------------

// TestRetentionPass_RetryOffer — a key deferred by the q:render snapshot is
// offered by the point probe in the next pass: the protection is gone —
// it is purged in place and the list empties; the forward drain meanwhile
// continues from its own cursor.
func TestRetentionPass_RetryOffer(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render: []map[RetentionPendingKey]struct{}{
			pendingKey(1, 10, 0, 0), // snapshot 1: sub=0 waits on q:render
			{},                      // snapshot 2: the protection is gone
		},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 1 || stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("проход 1 = %+v, want sub=1 очищена, sub=0 отложена", stats)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d ключей, want 1", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("защищённая запись очищена в проходе 1")
	}

	// Pass 2: the probe offered the key, the protection is gone — purged
	// in place.
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("проход 2: Rows = %d, want 1 (зонд очистил sub=0)", stats.Rows)
	}
	if len(pool.queries) != 2 || pool.queries[0].sql != retentionProbeSQL || pool.queries[1].sql != retentionSelectSQL {
		t.Fatalf("обращения прохода 2: %d (первый = %q), want зонд THEN выборка", len(pool.queries), pool.queries[0].sql)
	}
	probe := pool.queries[0]
	if !reflect.DeepEqual(probe.args[:4], []any{
		[]int16{1}, []int64{10}, []int16{0}, []int16{0},
	}) || probe.args[4] != DefaultRetention.String() {
		t.Errorf("args зонда = %v, want ключ sub=0 с границей старения", probe.args)
	}
	if len(st.retries) != 0 {
		t.Fatalf("список ретраев после прохода 2 = %d ключей, want 0", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("запись не очищена зондом прохода 2")
	}
}

// TestRetentionPass_RetryStillProtected — a key still waiting for
// consumers is not purged by the probe and stays in the list for the next
// pass (until the protection is lifted); the exclusion is observable in
// Deferred.
func TestRetentionPass_RetryStillProtected(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	protected := pendingKey(1, 10, 0, 0)
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{protected, protected}, // snapshots 1 and 2
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("проход 2: Rows = %d, want 0 (ключ всё ещё защищён)", stats.Rows)
	}
	if stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("Deferred = %v, want повторное исключение ключа зонда", stats.Deferred)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d, want 1 (ключ держится до снятия защиты)", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("защищённая запись очищена вопреки защите")
	}
}

// TestRetentionPass_RetryEviction — a key that stopped being a candidate
// (the domain became a Laravel find) is not returned by the probe — the key
// falls out of the list, no purge (spec §4.3: "excluding from the offers
// those whose protection is gone and the purge has happened" — and the
// symmetric cases).
func TestRetentionPass_RetryEviction(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0), {}},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d, want 1 после прохода 1", len(st.retries))
	}

	// The domain became a find: the probe will not return the key —
	// eviction without a purge.
	pool.laravel[retentionDomKey{zone: 1, id: 10}] = true
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Fatalf("проход 2 = %+v, want пусто (находка — не кандидат)", stats)
	}
	if len(st.retries) != 0 {
		t.Fatalf("список ретраев = %d, want 0 (эвикция не-кандидата)", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil || !r.jsonb {
		t.Fatal("запись находки затронута чисткой")
	}
}

// TestRetentionPass_RetryBound — the retry list overflow: a Warn and a
// conservative catch-up circle on the next pass (the circle will rebuild
// the list itself); the new key list does not grow beyond the limit.
func TestRetentionPass_RetryBound(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	for sub := int16(0); sub < 3; sub++ {
		pool.results = append(pool.results, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old.Add(time.Duration(sub) * time.Hour), hash: retentionHash16(byte(0x10 + sub)), jsonb: true,
		})
		pool.contents[retentionHash(byte(0x10+sub))] = retentionFakeContent{refc: 1, clen: 1000}
	}
	allProtected := mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 1, 0), pendingKey(1, 10, 2, 0))
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{allProtected, allProtected},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	var buf bytes.Buffer
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{retryLimit: 1}
	opts.BatchSize = 1 // a batch = a row: the overflow is met across several batches
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if !st.circle || len(st.retries) != 1 {
		t.Fatalf("после прохода 1: circle=%v, retries=%d (want запланированный круг, предел 1)", st.circle, len(st.retries))
	}
	// The Warn — exactly one per overflow episode (review of T-185, P3-1
	// R2): the second and third batches with waiting keys stay silent, the
	// plan is already set. Counted is the Warn text: the circle reason in
	// the Info line of pass 2 quotes the same overflow word (see
	// warnOverflow below) — that is not a Warn.
	const warnOverflow = "переполнен — следующий проход catch-up-кругом"
	if got := strings.Count(buf.String(), warnOverflow); got != 1 {
		t.Fatalf("Warn о переполнении = %d за проход 1, want 1", got)
	}

	// The next pass — a catch-up circle from the zero cursor: the list is
	// cleared, the selection starts from the beginning again; the standstill
	// holds — the keys are deferred anew (the regression of full circles is
	// limited to standstill episodes at the watermarks); a new episode — a
	// new Warn.
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if len(pool.queries) < 1 || pool.queries[0].sql != retentionSelectSQL {
		t.Fatalf("обращения прохода 2 = %d, want выборки круга (зонд не звать: список очищен)", len(pool.queries))
	}
	if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(1)) {
		t.Fatalf("args прохода 2 = %v, want нулевой курсор catch-up-круга", pool.queries[0].args)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев круга = %d, want 1 (переполнение на прежнем пределе)", len(st.retries))
	}
	if got := strings.Count(buf.String(), warnOverflow); got != 2 {
		t.Fatalf("Warn о переполнении = %d за два прохода, want 2 (по одному на эпизод)", got)
	}
}

// --- render-debt catch-up (spec §4.4) -----------------------------------------

// TestRetentionPass_DebtCatchUp — records under the render_pending flag are
// silently filtered out by the selection (the cursor passes them): the
// T-175 debt counter's transition to 0 plans a catch-up circle from the
// zero cursor, catching up everything that accumulated over the debt era.
func TestRetentionPass_DebtCatchUp(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	// 10 — under render debt (the flag, not rendered): the record is
	// protected; 20 — a regular non-find.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{flags: int16(model.FlagRenderPending)}
	pool.domains[retentionDomKey{zone: 1, id: 20}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	pool.counters["render_pending"] = 5

	var buf bytes.Buffer
	opts := retentionDefaultOpts()
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 1 (эпоха долга): %v", err)
	}
	if stats.Rows != 1 || !st.debtSeen || st.circle {
		t.Fatalf("проход 1 = %+v (debtSeen=%v): только домен 20, круг не планируется", stats, st.debtSeen)
	}

	// The debt has been drained: the flag is lifted, the counter reset to
	// zero.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.counters["render_pending"] = 0
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2 (снятие долга): %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("проход 2: Rows = %d, want 1 (догнали запись домена 10)", stats.Rows)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("args прохода 2 = %v, want catch-up-круг с нулевого курсора", pool.queries[0].args)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("запись эпохи долга не очищена catch-up-кругом")
	}
	if !strings.Contains(buf.String(), "обнулился") {
		t.Errorf("лог = %q, want причина круга — обнуление счётчика", buf.String())
	}
}

// TestRetentionPass_DebtFallback — the counter row is missing (the DB
// before the T-175 deploy): the fallback — a circle once per day from the
// last circle; in between, the passes continue from the cursor; the anchor
// is the starting circle of the process.
func TestRetentionPass_DebtFallback(t *testing.T) {
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
	delete(pool.counters, "render_pending")

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if st.lastCircle.IsZero() {
		t.Fatal("стартовый круг не зафиксирован якорем fallback-суток")
	}

	// An hour later: there is no new record, a circle is NOT planned (the
	// day has not elapsed) — the pass is one empty SELECT from the cursor.
	pool.now = now.Add(time.Hour)
	pool.results = append(pool.results, retentionRow{
		key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		// Aged by this moment (11.5 h > the 5 h boundary), but sits BEHIND
		// the pass 1 cursor (11 h): not touched at all until the circle.
		checkedAt: now.Add(-11*time.Hour - 30*time.Minute), hash: retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}
	pool.queries = nil
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 0 || len(pool.queries) != 1 {
		t.Fatalf("проход 2 = %+v (%d SELECT'ов), want пустой проход от курсора без круга", stats, len(pool.queries))
	}
	if reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatal("проход 2 пошёл с нулевого курсора вопреки неистёкшим суткам fallback")
	}

	// A day later — the fallback circle from the zero cursor picks up the
	// tail.
	st.lastCircle = time.Now().Add(-DefaultRetentionFallbackCircle - time.Hour)
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 3: %v", err)
	}
	if stats.Rows != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("проход 3 = %+v, args=%v: want fallback-круг, подбирающий sub=2", stats, pool.queries[0].args)
	}
}

// TestRetentionPass_DebtReadError — a counter read error is not a pass
// error: a Warn; the debt state is kept (the fallback anchor is fresh — the
// circle does not fire), and the drain proceeds normally.
func TestRetentionPass_DebtReadError(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.counters["render_pending"] = 5
	pool.debtErr = errors.New("бд недоступна")

	var buf bytes.Buffer
	opts := retentionDefaultOpts()
	opts.Log = retentionTestLogger(&buf)
	// The anchor is not set manually: the state initialization in
	// retentionPass must set it — otherwise (the "remove the block"
	// mutation) the fallback would immediately apply a circle and reset
	// debtSeen (review of T-185, P3-1 R3).
	st := &retentionState{debtSeen: true}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("stats = %+v, want штатный дренаж при ошибке счётчика", stats)
	}
	if !st.debtSeen || st.circle {
		t.Fatalf("состояние долга: debtSeen=%v circle=%v, want держится без круга", st.debtSeen, st.circle)
	}
	if st.lastCircle.IsZero() {
		t.Fatal("якорь fallback-суток не поставлен при создании состояния")
	}
	if !strings.Contains(buf.String(), "не прочитан") {
		t.Errorf("лог = %q, want Warn о нечитаемом счётчике", buf.String())
	}
}

// TestRetentionPass_DebtAnchorRefresh — the fallback-day anchor is moved by
// successful counter reads: the day "since the last circle" has elapsed,
// but the read succeeded — no circle is raised, the cursor is not reset;
// a transient failure of the next pass does not either (the anchor is
// fresh) — fallback means "unavailable for a day IN A ROW" (review of
// T-185, P3-2 R3).
func TestRetentionPass_DebtAnchorRefresh(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	anchor := retentionCursor{checkedAt: now.Add(-time.Hour), key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	st := &retentionState{
		cursor:     anchor,                                                      // the drain has already passed this point
		lastCircle: time.Now().Add(-DefaultRetentionFallbackCircle - time.Hour), // "the day has elapsed"
	}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if st.circle || st.cursor != anchor {
		t.Fatalf("после прохода 1: circle=%v cursor=%+v — успешное чтение обязано обновить якорь, круг не поднимается", st.circle, st.cursor)
	}

	// The next hour — a read failure: the anchor is fresh, no circle is
	// raised.
	pool.debtErr = errors.New("бд недоступна")
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if st.circle || st.cursor != anchor {
		t.Fatalf("транзиентный сбой чтения поднял круг: circle=%v cursor=%+v", st.circle, st.cursor)
	}
}

// TestRetentionPass_DebtReadErrorFallback — a persistently unavailable
// counter (a read failure) engages the fallback-day guard: the anchor has
// expired — a circle from the zero cursor picks up also the row lost behind
// the cursor (review of T-185, P3-3/P2-1 R2).
func TestRetentionPass_DebtReadErrorFallback(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if st.cursor == (retentionCursor{}) {
		t.Fatal("проход 1 не продвинул курсор")
	}

	// A row behind the cursor (the P1-1 leak class): the guard circle must
	// trim it.
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		checkedAt: now.Add(-11*time.Hour - 30*time.Minute), // behind the pass 1 cursor
		hash:      retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}

	pool.debtErr = errors.New("бд недоступна")
	st.lastCircle = time.Now().Add(-DefaultRetentionFallbackCircle - time.Hour)
	pool.queries = nil
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 1 || stats.FreedBytes != 3000 {
		t.Fatalf("проход 2 = %+v, want вычищена утраченная за курсором строка", stats)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("args прохода 2 = %v, want выборка fallback-круга с нулевого курсора", pool.queries[0].args)
	}
	// The cursor after the circle — on the picked-up row (the drain
	// continued normally from the zero point).
	want := retentionCursor{checkedAt: pool.results[2].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 0}}
	if st.cursor != want {
		t.Fatalf("курсор после прохода 2 = %+v, want %+v (подобранная строка круга)", st.cursor, want)
	}
}

// TestRetentionPass_DebtTableMissing — the DB before the migration 000015
// deploy: undefined_table (42P01) — not a read failure but "no counter" —
// the fallback branch without a Warn, one Info per process (review of
// T-185, P3-2/P3-4).
func TestRetentionPass_DebtTableMissing(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.debtErr = &pgconn.PgError{Code: "42P01"} // undefined_table

	var buf bytes.Buffer
	opts := retentionDefaultOpts()
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("проход 1: stats = %+v, want штатный дренаж", stats)
	}
	// The second pass of the same bench — the Info is not repeated.
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if strings.Contains(buf.String(), "не прочитан") {
		t.Errorf("лог = %q: absent-table обязан молчать (Warn только для сбоев)", buf.String())
	}
	if got := strings.Count(buf.String(), "таблицы счётчика рендер-долга нет"); got != 1 {
		t.Errorf("Info об отсутствующей таблице = %d, want 1 (один раз на процесс)", got)
	}
}

// --- contracts -----------------------------------------------------------------

// TestRetentionDebtSQLContract — the point read of the counter is the same
// statement as the render rework pass guard (renderDebtCounterSQL,
// T-181/T-175): the literal is duplicated with an anchor; a contract
// divergence is caught here.
func TestRetentionDebtSQLContract(t *testing.T) {
	if retentionDebtSQL != "SELECT value FROM crawler_counters WHERE name = 'render_pending'" {
		t.Fatalf("retentionDebtSQL разошёлся с контрактом T-175/T-181:\n%s", retentionDebtSQL)
	}
}

// TestRetentionOnce_EachFromZero — the one-off RetentionOnce keeps no
// state: every call starts from the zero cursor (the first selection of
// every pass is zero; the runbooks T-167 path A, isolated builds).
func TestRetentionOnce_EachFromZero(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	for pass := 1; pass <= 2; pass++ {
		if _, err := RetentionOnce(context.Background(), pool, opts); err != nil {
			t.Fatalf("проход %d: %v", pass, err)
		}
		if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
			t.Fatalf("проход %d: первая выборка = %v, want нулевой курсор (разовый проход)", pass, pool.queries[0].args)
		}
		pool.queries = nil
	}
}

// TestRetentionSQLShape — a regression anchor of the drain SQL shape
// (spec §5.1 "the retentionSelectSQL text"; review of T-185, P3-4): the
// cursor is a row-value over TIME with a PK tail, ORDER BY by the same
// key, LIMIT last; the probe — LATERAL by PK with LIMIT 1 (pinning the
// point plan). A return to PK order (the diagnosis of entry #13) or a loss
// of determinism is caught here.
func TestRetentionSQLShape(t *testing.T) {
	for _, want := range []string{
		"WHERE r.checked_at < now() - $1::interval",
		"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) " +
			"> ($2::timestamptz, $3::int2, $4::int8, $5::int2, $6::int2)",
		"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant",
		"LIMIT $7",
	} {
		if !strings.Contains(retentionSelectSQL, want) {
			t.Errorf("retentionSelectSQL потерял фрагмент дренажа по времени:\n%s\n--- нет:\n%s", retentionSelectSQL, want)
		}
	}
	// The PK cursor (the old order) must be gone.
	if strings.Contains(retentionSelectSQL, "ORDER BY r.zone_id, r.domain_id") {
		t.Errorf("retentionSelectSQL вернул PK-порядок (диагноз запроса №13):\n%s", retentionSelectSQL)
	}
	for _, want := range []string{
		"JOIN LATERAL (",
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id",
		"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant",
		"LIMIT 1) AS r ON true",
	} {
		if !strings.Contains(retentionProbeSQL, want) {
			t.Errorf("retentionProbeSQL потерял фрагмент точечного зонда:\n%s\n--- нет:\n%s", retentionProbeSQL, want)
		}
	}
	if strings.Contains(retentionProbeSQL, "ORDER BY") {
		t.Errorf("retentionProbeSQL не нуждается в порядке:\n%s", retentionProbeSQL)
	}
}

// TestRetentionPass_EqualTimestampsBoundary — the main risk of losing rows
// to pagination: batches of rows of one transaction share the checked_at
// stamp (T-058), and the batch boundary cuts such a group — the PK tail of
// the cursor must return the rest of the group with the next batch
// (review of T-185, P3-4).
func TestRetentionPass_EqualTimestampsBoundary(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	for sub := int16(0); sub < 3; sub++ {
		pool.results = append(pool.results, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old, hash: retentionHash16(byte(0x10 + sub)), jsonb: true, // one stamp for all
		})
		pool.contents[retentionHash(byte(0x10+sub))] = retentionFakeContent{refc: 1, clen: 1000}
	}

	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // a batch = a row: the group is cut twice
	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход: %v", err)
	}
	if stats.Rows != 3 {
		t.Fatalf("Rows = %d, want 3 (все строки равной отметки подобраны)", stats.Rows)
	}
	if len(pool.queries) != 4 { // three batches + the empty end selection
		t.Fatalf("SELECT'ов = %d, want 4", len(pool.queries))
	}
	// The cursors of batches 2 and 3 — the same stamp, the key strictly
	// grows: the cursor of selection i+1 is the last row of batch i
	// (sub = 0, then 1).
	for i := 1; i < 3; i++ {
		args := pool.queries[i].args
		if !args[1].(time.Time).Equal(old) {
			t.Fatalf("пачка %d: курсор-время = %v, want общая отметка %v", i+1, args[1], old)
		}
		if args[4].(int16) != int16(i-1) {
			t.Fatalf("пачка %d: курсор sub = %v, want %d (PK-хвост за последней строкой предыдущей)", i+1, args[4], i-1)
		}
	}
}

// TestRetentionPass_RetryPurgeFailureKeepsList — a probe purge failure
// keeps the previous retry list: unpurged keys are not lost behind the
// cursor, and the next pass repeats the probe idempotently (the assertion
// of the commit message; review of T-185, P3-4).
func TestRetentionPass_RetryPurgeFailureKeepsList(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0), {}},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d, want 1 после прохода 1", len(st.retries))
	}

	// The probe purge fails: the list must survive until the next pass.
	pool.purgeErr = errors.New("бд недоступна")
	if _, err := retentionPass(context.Background(), pool, opts, st); err == nil {
		t.Fatal("проход 2 обязан упасть ошибкой чистки")
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев после сбоя чистки = %d, want 1 (ключи не теряются)", len(st.retries))
	}
}

// TestRetentionPass_NoMidPassProbe — the probe is offered strictly at the
// start of a pass: keys deferred by the current pass are offered by the
// next one (review of T-185, P3-2 — the contract of the retentionPass
// comment).
func TestRetentionPass_NoMidPassProbe(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0)},
		analyze: []map[RetentionPendingKey]struct{}{{}},
	}

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d, want 1 (ключ отложен текущим проходом)", len(st.retries))
	}
	for i, q := range pool.queries {
		if q.sql == retentionProbeSQL {
			t.Fatalf("зонд выполнен среди дренажа (обращение %d) вопреки «один раз за проход, до дренажа»", i)
		}
	}
}

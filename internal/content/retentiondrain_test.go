// Unit tests of the retention drain state (T-185, retentiondrain.go):
// the time cursor survives passes (a row is visited once in its lifetime,
// the next pass continues from the stop point), point offers of the
// queue-guard retries (the PK probe, eviction of non-candidates, the list
// bound), catch-up circles (the T-175 render-debt counter reset to zero,
// the fallback day on a missing row, the list overflow), the drain
// machine itself (the transition table and observation sequences of
// 09#P2-3), and the return of a re-checked row to the drain after aging.
// The fake pool lives in retention_test.go (including the crawler_counters
// counter).
package content

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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
		t.Fatalf("pass 1: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("pass 1: Rows = %d, want 2 (sub=0, sub=1)", stats.Rows)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if st.cursor != want {
		t.Fatalf("cursor after pass 1 = %+v, want %+v (the last row of the window)", st.cursor, want)
	}

	// An hour later sub=2 has aged (5.5 h > 6 h − 1 h of the window) — the
	// pass continues from the cursor, without a full circle.
	pool.now = now.Add(time.Hour)
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("pass 2: Rows = %d, want 1 (only the aged-out sub=2)", stats.Rows)
	}
	if len(pool.queries) != 1 {
		t.Fatalf("pass 2 SELECTs = %d, want 1", len(pool.queries))
	}
	wantArgs := []any{DefaultRetention.String(), want.checkedAt, want.key.zone, want.key.domain, want.key.sub, want.key.variant, 100}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("pass 2 args = %v, want the pass 1 cursor %v", pool.queries[0].args, wantArgs)
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
		t.Fatalf("pass 1: %v", err)
	}
	if stats.Rows != 1 || !stats.Skipped {
		t.Fatalf("pass 1 = %+v, want one batch and an interlock skip", stats)
	}
	want := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	if st.cursor != want {
		t.Fatalf("cursor after the stop = %+v, want %+v (the first row)", st.cursor, want)
	}

	// The depth dipped: the drain continues from the cursor — rows 1 and 2.
	depth.mu.Lock()
	depth.n = 0
	depth.mu.Unlock()
	pool.onSelect = nil // the depth hour has passed — no more race
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 2 || stats.Skipped {
		t.Fatalf("pass 2 = %+v, want 2 rows without a skip", stats)
	}
	wantArgs := []any{DefaultRetention.String(), want.checkedAt, want.key.zone, want.key.domain, want.key.sub, want.key.variant, 1}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("pass 2 args = %v, want continuing from the cursor %v", pool.queries[0].args, wantArgs)
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
		t.Fatalf("pass 1: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("pass 1: Rows = %d, want 0 (the age guard)", stats.Rows)
	}

	// Six hours later the rewrite has aged — the drain picks it up.
	pool.now = now.Add(6*time.Hour + time.Minute)
	pool.onSelect = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 1 || stats.FreedBytes != 1500 {
		t.Fatalf("pass 2 = %+v, want the overwritten row purged (new content 0x99)", stats)
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
		t.Fatalf("pass 1: %v", err)
	}
	if stats.Rows != 1 || stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("pass 1 = %+v, want sub=1 purged, sub=0 deferred", stats)
	}
	if len(st.retries) != 1 {
		t.Fatalf("the retry list = %d keys, want 1", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("a protected record purged in pass 1")
	}

	// Pass 2: the probe offered the key, the protection is gone — purged
	// in place.
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("pass 2: Rows = %d, want 1 (the probe cleared sub=0)", stats.Rows)
	}
	if len(pool.queries) != 2 || pool.queries[0].sql != retentionProbeSQL || pool.queries[1].sql != retentionSelectSQL {
		t.Fatalf("pass 2 calls: %d (the first = %q), want probe THEN selection", len(pool.queries), pool.queries[0].sql)
	}
	probe := pool.queries[0]
	if !reflect.DeepEqual(probe.args[:4], []any{
		[]int16{1}, []int64{10}, []int16{0}, []int16{0},
	}) || probe.args[4] != DefaultRetention.String() {
		t.Errorf("probe args = %v, want the sub=0 key with the aging boundary", probe.args)
	}
	if len(st.retries) != 0 {
		t.Fatalf("the retry list after pass 2 = %d keys, want 0", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("the record not purged by the pass 2 probe")
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
		t.Fatalf("pass 1: %v", err)
	}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("pass 2: Rows = %d, want 0 (the key still protected)", stats.Rows)
	}
	if stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("Deferred = %v, want the probe key excluded again", stats.Deferred)
	}
	if len(st.retries) != 1 {
		t.Fatalf("the retry list = %d, want 1 (the key held until the protection lifts)", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("a protected record purged despite the protection")
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
		t.Fatalf("pass 1: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("the retry list = %d, want 1 after pass 1", len(st.retries))
	}

	// The domain became a find: the probe will not return the key —
	// eviction without a purge.
	pool.laravel[retentionDomKey{zone: 1, id: 10}] = true
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Fatalf("pass 2 = %+v, want empty (a find is not a candidate)", stats)
	}
	if len(st.retries) != 0 {
		t.Fatalf("the retry list = %d, want 0 (eviction of a non-candidate)", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil || !r.jsonb {
		t.Fatal("a find record touched by the purge")
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
		t.Fatalf("pass 1: %v", err)
	}
	if !st.circlePlanned() || len(st.retries) != 1 {
		t.Fatalf("after pass 1: mode=%v, retries=%d (want the scheduled circle, the limit 1)", st.mode, len(st.retries))
	}
	// The Warn — exactly one per overflow episode (review of T-185, P3-1
	// R2): the second and third batches with waiting keys stay silent, the
	// plan is already set. Counted is the Warn text: the circle reason in
	// the Info line of pass 2 quotes the same overflow word (see
	// warnOverflow below) — that is not a Warn.
	const warnOverflow = "overflowed — the next pass runs as a catch-up circle"
	if got := strings.Count(buf.String(), warnOverflow); got != 1 {
		t.Fatalf("the overflow Warn = %d over pass 1, want 1", got)
	}

	// The next pass — a catch-up circle from the zero cursor: the list is
	// cleared, the selection starts from the beginning again; the standstill
	// holds — the keys are deferred anew (the regression of full circles is
	// limited to standstill episodes at the watermarks); a new episode — a
	// new Warn.
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if len(pool.queries) < 1 || pool.queries[0].sql != retentionSelectSQL {
		t.Fatalf("pass 2 calls = %d, want the circle selection (no probe: the list cleared)", len(pool.queries))
	}
	if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(1)) {
		t.Fatalf("pass 2 args = %v, want the catch-up circle zero cursor", pool.queries[0].args)
	}
	if len(st.retries) != 1 {
		t.Fatalf("the circle retry list = %d, want 1 (an overflow at the previous limit)", len(st.retries))
	}
	if got := strings.Count(buf.String(), warnOverflow); got != 2 {
		t.Fatalf("the overflow Warn = %d over two passes, want 2 (one per episode)", got)
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
		t.Fatalf("pass 1 (the debt epoch): %v", err)
	}
	if stats.Rows != 1 || st.mode != debtEraMode || st.circlePlanned() {
		t.Fatalf("pass 1 = %+v (mode=%v): only domain 20, no circle planned", stats, st.mode)
	}

	// The debt has been drained: the flag is lifted, the counter reset to
	// zero.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.counters["render_pending"] = 0
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass 2 (the debt lift): %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("pass 2: Rows = %d, want 1 (caught up with the domain 10 record)", stats.Rows)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("pass 2 args = %v, want a catch-up circle from the zero cursor", pool.queries[0].args)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("the debt-epoch record not purged by the catch-up circle")
	}
	if !strings.Contains(buf.String(), "dropped to zero") {
		t.Errorf("log = %q, want the circle reason — the counter dropping to zero", buf.String())
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
		t.Fatalf("pass 1: %v", err)
	}
	if st.lastCircle.IsZero() {
		t.Fatal("the starting circle not anchored by the fallback day")
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
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 0 || len(pool.queries) != 1 {
		t.Fatalf("pass 2 = %+v (%d SELECTs), want an empty pass from the cursor without a circle", stats, len(pool.queries))
	}
	if reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatal("pass 2 went from the zero cursor despite the unexpired fallback day")
	}

	// A day later — the fallback circle from the zero cursor picks up the
	// tail.
	st.lastCircle = time.Now().Add(-DefaultRetentionFallbackCircle - time.Hour)
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	if stats.Rows != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("pass 3 = %+v, args=%v: want the fallback circle picking up sub=2", stats, pool.queries[0].args)
	}
}

// TestRetentionPass_DebtReadError — a counter read error is not a pass
// error: a Warn; the debt state is kept (the fallback anchor is fresh — the
// circle does not fire), and the drain proceeds normally.
func TestRetentionPass_DebtReadError(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.counters["render_pending"] = 5
	pool.debtErr = errors.New("db unavailable")

	var buf bytes.Buffer
	opts := retentionDefaultOpts()
	opts.Log = retentionTestLogger(&buf)
	// The anchor is not set manually: the state initialization in
	// retentionPass must set it — otherwise (the "remove the block"
	// mutation) the fallback would immediately apply a circle and reset
	// debtSeen (review of T-185, P3-1 R3).
	st := &retentionState{mode: debtEraMode}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("stats = %+v, want a regular drain with the counter error", stats)
	}
	if st.mode != debtEraMode || st.circlePlanned() {
		t.Fatalf("the debt state: mode=%v, want debtEra kept without a circle", st.mode)
	}
	if st.lastCircle.IsZero() {
		t.Fatal("the fallback-day anchor not set at state creation")
	}
	if !strings.Contains(buf.String(), "not read") {
		t.Errorf("log = %q, want the unreadable-counter Warn", buf.String())
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
		t.Fatalf("pass 1: %v", err)
	}
	if st.circlePlanned() || st.cursor != anchor {
		t.Fatalf("after pass 1: mode=%v cursor=%+v — a successful read must refresh the anchor, no circle raised", st.mode, st.cursor)
	}

	// The next hour — a read failure: the anchor is fresh, no circle is
	// raised.
	pool.debtErr = errors.New("db unavailable")
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if st.circlePlanned() || st.cursor != anchor {
		t.Fatalf("a transient read failure raised the circle: mode=%v cursor=%+v", st.mode, st.cursor)
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
		t.Fatalf("pass 1: %v", err)
	}
	if st.cursor == (retentionCursor{}) {
		t.Fatal("pass 1 did not advance the cursor")
	}

	// A row behind the cursor (the P1-1 leak class): the guard circle must
	// trim it.
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		checkedAt: now.Add(-11*time.Hour - 30*time.Minute), // behind the pass 1 cursor
		hash:      retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}

	pool.debtErr = errors.New("db unavailable")
	st.lastCircle = time.Now().Add(-DefaultRetentionFallbackCircle - time.Hour)
	pool.queries = nil
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if stats.Rows != 1 || stats.FreedBytes != 3000 {
		t.Fatalf("pass 2 = %+v, want the row lost behind the cursor purged", stats)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("pass 2 args = %v, want the fallback circle selection from the zero cursor", pool.queries[0].args)
	}
	// The cursor after the circle — on the picked-up row (the drain
	// continued normally from the zero point).
	want := retentionCursor{checkedAt: pool.results[2].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 0}}
	if st.cursor != want {
		t.Fatalf("cursor after pass 2 = %+v, want %+v (the circle's picked-up row)", st.cursor, want)
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
		t.Fatalf("pass 1: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("pass 1: stats = %+v, want a regular drain", stats)
	}
	// The second pass of the same bench — the Info is not repeated.
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if strings.Contains(buf.String(), "not read") {
		t.Errorf("log = %q: the absent table must stay silent (Warn only for failures)", buf.String())
	}
	if got := strings.Count(buf.String(), "the render debt counter table is missing"); got != 1 {
		t.Errorf("the missing-table Info = %d, want 1 (once per process)", got)
	}
}

// --- contracts -----------------------------------------------------------------

// TestRetentionDebtSQLContract — the point read of the counter is the same
// statement as the render rework pass guard (renderDebtCounterSQL,
// T-181/T-175): the literal is duplicated with an anchor; a contract
// divergence is caught here.
func TestRetentionDebtSQLContract(t *testing.T) {
	if retentionDebtSQL != "SELECT value FROM crawler_counters WHERE name = 'render_pending'" {
		t.Fatalf("retentionDebtSQL diverged from the T-175/T-181 contract:\n%s", retentionDebtSQL)
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
			t.Fatalf("pass %d: %v", pass, err)
		}
		if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
			t.Fatalf("pass %d: the first selection = %v, want the zero cursor (a one-off pass)", pass, pool.queries[0].args)
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
			t.Errorf("retentionSelectSQL lost the time-based drain fragment:\n%s\n--- missing:\n%s", retentionSelectSQL, want)
		}
	}
	// The PK cursor (the old order) must be gone.
	if strings.Contains(retentionSelectSQL, "ORDER BY r.zone_id, r.domain_id") {
		t.Errorf("retentionSelectSQL returned a PK order (query diagnosis #13):\n%s", retentionSelectSQL)
	}
	for _, want := range []string{
		"JOIN LATERAL (",
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id",
		"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant",
		"LIMIT 1) AS r ON true",
	} {
		if !strings.Contains(retentionProbeSQL, want) {
			t.Errorf("retentionProbeSQL lost the point-probe fragment:\n%s\n--- missing:\n%s", retentionProbeSQL, want)
		}
	}
	if strings.Contains(retentionProbeSQL, "ORDER BY") {
		t.Errorf("retentionProbeSQL needs no order:\n%s", retentionProbeSQL)
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
		t.Fatalf("pass: %v", err)
	}
	if stats.Rows != 3 {
		t.Fatalf("Rows = %d, want 3 (all rows of equal stamp picked up)", stats.Rows)
	}
	if len(pool.queries) != 4 { // three batches + the empty end selection
		t.Fatalf("SELECTs = %d, want 4", len(pool.queries))
	}
	// The cursors of batches 2 and 3 — the same stamp, the key strictly
	// grows: the cursor of selection i+1 is the last row of batch i
	// (sub = 0, then 1).
	for i := 1; i < 3; i++ {
		args := pool.queries[i].args
		if !args[1].(time.Time).Equal(old) {
			t.Fatalf("batch %d: the time cursor = %v, want the common stamp %v", i+1, args[1], old)
		}
		if args[4].(int16) != int16(i-1) {
			t.Fatalf("batch %d: the sub cursor = %v, want %d (the PK tail past the previous batch's last row)", i+1, args[4], i-1)
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
		t.Fatalf("pass 1: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("the retry list = %d, want 1 after pass 1", len(st.retries))
	}

	// The probe purge fails: the list must survive until the next pass.
	pool.purgeErr = errors.New("db unavailable")
	if _, err := retentionPass(context.Background(), pool, opts, st); err == nil {
		t.Fatal("pass 2 must fail with a purge error")
	}
	if len(st.retries) != 1 {
		t.Fatalf("the retry list after the purge failure = %d, want 1 (keys are not lost)", len(st.retries))
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
		t.Fatalf("pass: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("the retry list = %d, want 1 (the key deferred by the current pass)", len(st.retries))
	}
	for i, q := range pool.queries {
		if q.sql == retentionProbeSQL {
			t.Fatalf("the probe ran amid the drain (call %d) despite \"once per pass, before the drain\"", i)
		}
	}
}

// --- the drain machine (09#P2-3, tasks/TZ-fsm.md) -------------------------------

// TestNextDrainModeTable — every cell of the transition table pinned in
// the doc comment of nextDrainMode: the mode × the observation, with the
// fallback-day guard (it weighs only in the debtUnavailable column). The
// circlePlanned row of the table is the apply order, not a table cell —
// a waiting circle is applied before the transition (catchUp); that order
// is pinned by TestDrainMachineSequences below.
func TestNextDrainModeTable(t *testing.T) {
	cases := []struct {
		name        string
		from        retentionDrainMode
		obs         retentionDebtObs
		fallbackDue bool
		want        retentionDrainMode
		wantCircle  bool // the transition plans a circle (a why reason)
	}{
		{"drain/active", drainMode, debtActive, false, debtEraMode, false},
		{"drain/lifted", drainMode, debtLifted, false, drainMode, false},
		{"drain/unavailable", drainMode, debtUnavailable, false, drainMode, false},
		{"drain/unavailable/fallback", drainMode, debtUnavailable, true, circlePlannedMode, true},
		{"debtEra/active", debtEraMode, debtActive, false, debtEraMode, false},
		{"debtEra/lifted", debtEraMode, debtLifted, false, circlePlannedMode, true},
		{"debtEra/unavailable", debtEraMode, debtUnavailable, false, debtEraMode, false},
		{"debtEra/unavailable/fallback", debtEraMode, debtUnavailable, true, circlePlannedMode, true},
		{"circlePlanned/active", circlePlannedMode, debtActive, false, debtEraMode, false},
		{"circlePlanned/lifted", circlePlannedMode, debtLifted, false, drainMode, false},
		{"circlePlanned/unavailable", circlePlannedMode, debtUnavailable, false, circlePlannedMode, false},
		{"circlePlanned/unavailable/fallback", circlePlannedMode, debtUnavailable, true, circlePlannedMode, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, why := nextDrainMode(tc.from, tc.obs, tc.fallbackDue)
			if got != tc.want {
				t.Fatalf("nextDrainMode(%v, %v, fallback=%v) = %v, want %v",
					tc.from, tc.obs, tc.fallbackDue, got, tc.want)
			}
			if (why != "") != tc.wantCircle {
				t.Fatalf("the circle reason = %q, want a non-empty verdict: %v", why, tc.wantCircle)
			}
		})
	}
}

// drainEvent — one step of the machine's life for the sequence tests: a
// debt observation at a pass start, or the mid-pass retry list overflow
// (deferRetries → planCircle; the apply waits for the next pass start).
type drainEvent struct {
	obs      retentionDebtObs
	overflow bool
	// fallbackAged — age the fallback-day anchor beyond the day before
	// the step (the debtUnavailable row of the table).
	fallbackAged bool
}

// TestDrainMachineSequences — observation sequences → state sequences:
// the machine is driven through the PRODUCTION transition application
// (observe/planCircle) with a non-persistent state (no DB), and after
// every step the mode and the cursor position are pinned. The cursor is
// the circle's observable: applyCircle resets it to zero.
func TestDrainMachineSequences(t *testing.T) {
	moved := retentionCursor{checkedAt: retentionTestNow(), key: retentionKey{zone: 1, domain: 10, sub: 7, variant: 0}}
	cases := []struct {
		name   string
		events []drainEvent
		// want — after every event: the mode and whether the cursor was
		// reset by a circle apply in that step.
		want []struct {
			mode   retentionDrainMode
			circle bool
		}
	}{
		{
			// The era → the lift: one full circle (§4.4).
			name:   "era then lift",
			events: []drainEvent{{obs: debtActive}, {obs: debtLifted}},
			want: []struct {
				mode   retentionDrainMode
				circle bool
			}{{debtEraMode, false}, {drainMode, true}},
		},
		{
			// A zero counter without an era is no event (the hour
			// discretization — the era that fit between reads is picked
			// up by other mechanisms, the catchUp comment).
			name:   "lift without era",
			events: []drainEvent{{obs: debtLifted}, {obs: debtLifted}},
			want: []struct {
				mode   retentionDrainMode
				circle bool
			}{{drainMode, false}, {drainMode, false}},
		},
		{
			// An era OBSERVED while a circle was waiting is not erased by
			// the apply (the former flags lost it — an edge of 09#P2-2):
			// overflow (pass N) → the apply and a fresh era (pass N+1) →
			// the lift circles (pass N+2).
			name:   "era survives the waiting circle",
			events: []drainEvent{{overflow: true}, {obs: debtActive}, {obs: debtLifted}},
			want: []struct {
				mode   retentionDrainMode
				circle bool
			}{{circlePlannedMode, false}, {debtEraMode, true}, {drainMode, true}},
		},
		{
			// The lift while a circle is already waiting: one circle, not
			// two (the plan is idempotent).
			name:   "lift with a waiting circle",
			events: []drainEvent{{overflow: true}, {obs: debtLifted}},
			want: []struct {
				mode   retentionDrainMode
				circle bool
			}{{circlePlannedMode, false}, {drainMode, true}},
		},
		{
			// The era holds through unavailable reads (the state is kept,
			// review of T-185, P3-1 R3); the aged anchor is the fallback
			// day — a circle from debtEra too (it resets the era state).
			name:   "era holds, then the fallback day",
			events: []drainEvent{{obs: debtActive}, {obs: debtUnavailable}, {obs: debtUnavailable, fallbackAged: true}},
			want: []struct {
				mode   retentionDrainMode
				circle bool
			}{{debtEraMode, false}, {debtEraMode, false}, {drainMode, true}},
		},
		{
			// A repeated active observation is not a transition (the
			// marker is written once per era, not per pass).
			name:   "era is idempotent",
			events: []drainEvent{{obs: debtActive}, {obs: debtActive}, {obs: debtLifted}},
			want: []struct {
				mode   retentionDrainMode
				circle bool
			}{{debtEraMode, false}, {debtEraMode, false}, {drainMode, true}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := slog.New(slog.DiscardHandler)
			// The production starting state of a pass: the anchor set
			// (retentionPass), the cursor moved by the previous passes.
			st := &retentionState{persist: false, lastCircle: time.Now(), cursor: moved}
			if len(tc.events) != len(tc.want) {
				t.Fatalf("the case is malformed: %d events, %d verdicts", len(tc.events), len(tc.want))
			}
			for i, ev := range tc.events {
				var circled bool
				if ev.fallbackAged {
					st.lastCircle = time.Now().Add(-DefaultRetentionFallbackCircle - time.Minute)
				}
				if ev.overflow {
					st.planCircle(context.Background(), nil, log, "the queue-guard retry list overflowed — the circle will rebuild the set")
				} else {
					st.observe(context.Background(), nil, log, ev.obs)
				}
				circled = st.cursor == (retentionCursor{})
				if st.mode != tc.want[i].mode || circled != tc.want[i].circle {
					t.Fatalf("step %d (%+v): mode=%v circle=%v, want mode=%v circle=%v",
						i, ev, st.mode, circled, tc.want[i].mode, tc.want[i].circle)
				}
			}
		})
	}
}

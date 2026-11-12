// Unit tests of the exact retention protection — the exclusion from
// batches of records whose messages wait on q:render/q:analyze consumers
// (T-166/T-167, retentionpending.go): a fake key source over the fake pool
// of retention_test.go. Checked: excluding the keys of both streams
// (including when the SQL has already returned the records as candidates),
// skipping a whole batch without a transaction, snapshot refresh during a
// long drain, and the conservative skip of the pass on a stream read error
// (the first snapshot and the refresh).
package content

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"crawler/internal/queue"
)

// retentionTestNow — the fixed "now" of the passes (the retention_test.go
// style).
func retentionTestNow() time.Time {
	return time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
}

// fakePending — a source of unconsumed keys with pre-prepared snapshots:
// render/fallback/analyze — sequences of sets per snapshot (the last one
// repeats); errFrom — from which snapshot (1-based; the snapshot is counted
// by the q:render access — the first stream read) to answer with a Redis
// error; 0 — never. calls — the order of accesses by stream.
type fakePending struct {
	mu       sync.Mutex
	render   []map[RetentionPendingKey]struct{}
	fallback []map[RetentionPendingKey]struct{}
	analyze  []map[RetentionPendingKey]struct{}
	errFrom  int
	snap     int
	calls    []string
}

// compile-time: the fake is a protection source for tests without Redis.
var _ RetentionPending = (*fakePending)(nil)

func (f *fakePending) PendingKeys(_ context.Context, stream string) (map[RetentionPendingKey]struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, stream)
	if stream == queue.StreamRender {
		f.snap++
	}
	if f.errFrom > 0 && f.snap >= f.errFrom {
		return nil, errors.New("redis unavailable")
	}
	seq := &f.analyze
	switch stream {
	case queue.StreamRender:
		seq = &f.render
	case queue.StreamRenderFallback:
		seq = &f.fallback
	}
	if len(*seq) == 0 {
		return map[RetentionPendingKey]struct{}{}, nil
	}
	m := (*seq)[0]
	if len(*seq) > 1 {
		*seq = (*seq)[1:]
	}
	return m, nil
}

// pendingKey — a protection key from the record's coordinates.
func pendingKey(zone int16, domain int64, sub, variant int16) map[RetentionPendingKey]struct{} {
	return map[RetentionPendingKey]struct{}{{
		Zone: zone, Domain: domain, Sub: sub, Variant: variant,
	}: {}}
}

// TestRetentionOnce_PendingExclude — records whose messages wait on the
// q:render/q:render-fallback/q:analyze consumers are excluded from the
// purge even though the selection returned them as candidates: the
// content, jsonb, and refcount are untouched; a record waiting on several
// streams at once is counted in each (the observability of "who holds");
// non-waiting records of the batch are purged normally.
func TestRetentionOnce_PendingExclude(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	old := now.Add(-2 * DefaultRetention)
	// Three more candidates: (10,1,0) waits on q:render and q:analyze,
	// (10,2,1) — on q:analyze only, (10,3,0) — on q:render-fallback only
	// (the record went to the render fallback, T-238: worker B's pace is
	// limited, raw content is needed at finalization after the fallback is
	// exhausted); the clean seed candidates — (10,0,0) and the rendered
	// record of domain 40.
	pool.results = append(pool.results,
		retentionRow{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		retentionRow{key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 1}, checkedAt: old, hash: retentionHash16(0x22), jsonb: true},
		retentionRow{key: retentionKey{zone: 1, domain: 10, sub: 3, variant: 0}, checkedAt: old, hash: retentionHash16(0x33), jsonb: true},
	)
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 1000}

	opts := retentionDefaultOpts()
	// Snapshot 1 (the only one): (10,1,0) waits on q:render and q:analyze,
	// (10,2,1) — on q:analyze, (10,3,0) — on q:render-fallback.
	opts.Pending = &fakePending{
		render:   []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 1, 0)},
		fallback: []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 3, 0)},
		analyze:  []map[RetentionPendingKey]struct{}{mergePending(pendingKey(1, 10, 1, 0), pendingKey(1, 10, 2, 1))},
	}
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 2 || stats.Refs != 2 {
		t.Fatalf("stats = %+v, want Rows=Refs=2 (only the non-waiting: 10 and 40)", stats)
	}
	if stats.Deferred[queue.StreamRender] != 1 || stats.Deferred[queue.StreamRenderFallback] != 1 ||
		stats.Deferred[queue.StreamAnalyze] != 2 {
		t.Fatalf("Deferred = %v, want q:render=1 (10/1/0), q:render-fallback=1 (10/3/0), q:analyze=2 (10/1/0 and 10/2/1)", stats.Deferred)
	}
	if stats.Skipped || stats.SkippedPending {
		t.Fatalf("stats = %+v, want a pass without skips", stats)
	}
	// Records waiting on the queues are untouched: the content and jsonb
	// are in place, the refcount of the live rows did not change.
	for _, key := range []retentionKey{
		{zone: 1, domain: 10, sub: 1, variant: 0},
		{zone: 1, domain: 10, sub: 2, variant: 1},
		{zone: 1, domain: 10, sub: 3, variant: 0},
	} {
		if r := pool.row(key); r == nil || r.hash == nil || !r.jsonb {
			t.Errorf("record %v waiting for the queues purged: %+v", key, r)
		}
	}
	if _, ok := pool.contents[retentionHash(0x11)]; !ok {
		t.Error("contents 0x11 (waiting for the queues) removed")
	}
	if _, ok := pool.contents[retentionHash(0x22)]; !ok {
		t.Error("contents 0x22 (waiting for the queues) removed")
	}
	if _, ok := pool.contents[retentionHash(0x33)]; !ok {
		t.Error("contents 0x33 (waiting for q:render-fallback) removed")
	}
	// The SQL returned the waiting records as candidates — the exclusion
	// was done by the protection.
	if len(pool.queries) == 0 {
		t.Fatal("no selections — the test did not check candidate exclusion")
	}
}

// TestRetentionOnce_PendingAllExcluded — the whole batch waits on the
// queues: the purge transaction is not opened (the only transaction is the
// selection pinning wrapper tx, T-271), the drain completes normally (the
// cursor went past the selection), the records are untouched.
func TestRetentionOnce_PendingAllExcluded(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	// One candidate, and its message waits on q:render.
	pool.results = pool.results[:1]
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render: []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0)},
	}
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Fatalf("stats = %+v, want an empty drain without transactions", stats)
	}
	if stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("Deferred = %v, want q:render=1", stats.Deferred)
	}
	// The only transaction is the selection pinning wrapper tx (T-271);
	// there is no purge transaction: nothing to purge.
	if pool.begins != 1 {
		t.Fatalf("transactions = %d, want 1 (the selection tx wrapper only, no purge)", pool.begins)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil || !r.jsonb {
		t.Fatalf("a waiting record touched: %+v", r)
	}
}

// TestRetentionOnce_PendingRefresh — the snapshot is refreshed during a
// long drain (DefaultRetentionPendingRefresh selections): a record whose
// message entered the stream after the first snapshot is protected by the
// refresh — without it, it would have been purged (the T-066
// re-publication race, retentionpending.go).
func TestRetentionOnce_PendingRefresh(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	var rows []retentionRow
	for sub := int16(0); sub < 26; sub++ {
		rows = append(rows, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old, hash: retentionHash16(byte(sub)), jsonb: true,
		})
		pool.contents[retentionHash(byte(sub))] = retentionFakeContent{refc: 1, clen: 100}
	}
	pool.results = rows

	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // a drain in row-sized batches: 26 selections
	second := mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 25, 0))
	opts.Pending = &fakePending{
		render: []map[RetentionPendingKey]struct{}{
			pendingKey(1, 10, 0, 0), // snapshot 1: the first record waits
			second,                  // snapshot 2 (the refresh): the last one too — its message was published after the first snapshot
		},
	}
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	// 24 records are purged: sub 1..24; sub 0 — the first snapshot, sub 25
	// — the refreshed one (its message was published after the first
	// snapshot).
	if stats.Rows != 24 {
		t.Fatalf("Rows = %d, want 24 (sub=0 and sub=25 excluded)", stats.Rows)
	}
	for _, sub := range []int16{0, 25} {
		if r := pool.row(retentionKey{zone: 1, domain: 10, sub: sub, variant: 0}); r.hash == nil || !r.jsonb {
			t.Errorf("record sub=%d protected by the snapshot refresh purged", sub)
		}
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 12, variant: 0}); r.hash != nil {
		t.Errorf("record sub=12 without protection not purged")
	}
	p := opts.Pending.(*fakePending)
	if len(p.calls) != 6 {
		t.Fatalf("stream calls = %v, want 6 (two snapshots × q:render+q:render-fallback+q:analyze)", p.calls)
	}
}

// TestRetentionOnce_PendingAfterInterlock — the order of the checks: the
// q:analyze interlock comes before the first streams snapshot — with the
// interlock closed, the heavy tail reading is not performed at all (and its
// timeout does not yield a false SkippedPending instead of an honest
// Skipped).
func TestRetentionOnce_PendingAfterInterlock(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	opts := retentionDefaultOpts()
	opts.AnalyzeWatermark = 100
	depth := &fakeDepth{n: 150}
	opts.Depth = depth
	pending := &fakePending{errFrom: 1} // even an unavailable source is not read
	opts.Pending = pending

	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if !stats.Skipped || stats.SkippedPending {
		t.Fatalf("stats = %+v, want the interlock Skipped (not SkippedPending)", stats)
	}
	if len(pending.calls) != 0 {
		t.Fatalf("the snapshot read with the interlock closed: %v", pending.calls)
	}
	if len(pool.queries) != 0 {
		t.Fatalf("SELECTs = %d, want 0", len(pool.queries))
	}
}

// TestRetentionOnce_PendingReadError — a stream read error BEFORE the
// first selection: a conservative skip of the pass — not an error, the DB
// is untouched, the outcome goes to OnPass (SkippedPending — the
// passes{skipped_pending} metric and pending_errors_total, a Warn in the
// cycle).
func TestRetentionOnce_PendingReadError(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{errFrom: 1} // the very first snapshot — an error

	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v, want a conservative skip without an error", err)
	}
	if !stats.SkippedPending || stats.Rows != 0 {
		t.Fatalf("stats = %+v, want SkippedPending with no work", stats)
	}
	if len(pool.queries) != 0 || pool.begins != 0 {
		t.Fatalf("a skipped pass must leave the DB alone: %d SELECTs, %d transactions", len(pool.queries), pool.begins)
	}
}

// TestRetentionOnce_PendingRefreshError — a snapshot refresh error in the
// middle of the drain: the batches already made stand (batches are atomic
// in themselves); the remainder of the pass is skipped conservatively.
func TestRetentionOnce_PendingRefreshError(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	var rows []retentionRow
	for sub := int16(0); sub < 26; sub++ {
		rows = append(rows, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old, hash: retentionHash16(byte(sub)), jsonb: true,
		})
		pool.contents[retentionHash(byte(sub))] = retentionFakeContent{refc: 1, clen: 100}
	}
	pool.results = rows

	opts := retentionDefaultOpts()
	opts.BatchSize = 1
	opts.Pending = &fakePending{errFrom: 2} // snapshot 1 empty, the refresh — an error

	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v, want a deferred remainder without an error", err)
	}
	if stats.Rows != 25 || !stats.SkippedPending {
		t.Fatalf("stats = %+v, want 25 committed batches and SkippedPending", stats)
	}
	// The 25th record (sub=24) was purged; the 26th (sub=25) is untouched —
	// the pass was stopped before its batch.
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 25, variant: 0}); r.hash == nil {
		t.Error("a record past the error point purged — the remainder must wait for the next pass")
	}
}

// TestRetentionOnce_PendingDisabled — a nil source: the protection is off,
// the pass behavior is the previous one (all candidates are purged).
func TestRetentionOnce_PendingDisabled(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 2 || len(stats.Deferred) != 0 {
		t.Fatalf("stats = %+v, want the previous 2 rows without Deferred", stats)
	}
}

// mergePending merges key sets (the fake's snapshot).
func mergePending(ms ...map[RetentionPendingKey]struct{}) map[RetentionPendingKey]struct{} {
	out := make(map[RetentionPendingKey]struct{})
	for _, m := range ms {
		for k := range m {
			out[k] = struct{}{}
		}
	}
	return out
}

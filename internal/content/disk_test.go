// Unit tests of the free-space control of the content directory (T-046; spec 8):
// a stubbable statfs (SpaceChecker), the write-pause threshold, dedup and reads
// keep working, recovery after reclamation, the alert event,
// the DiskWatchLoop periodic check (Warn/Info and transitions). The
// pack_test.go/compact_test.go style: a fake DB pool, real files in t.TempDir.
package content

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSpace — a stubbable SpaceChecker: a space snapshot and the call count
// (the Put pause and DiskWatchLoop must ask for the space exactly once
// per operation).
type fakeSpace struct {
	mu    sync.Mutex
	stat  SpaceStat
	err   error
	calls int
}

func (f *fakeSpace) Space(string) (SpaceStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.stat, f.err
}

func (f *fakeSpace) set(avail, total uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stat = SpaceStat{Avail: avail, Total: total}
}

func (f *fakeSpace) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeSpace) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fullSink — a collector of alert events (the hook is called from Put and the
// DiskWatchLoop goroutine — synchronization is mandatory).
type fullSink struct {
	mu     sync.Mutex
	events []StoreFullEvent
}

func (s *fullSink) hook(ev StoreFullEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
}

func (s *fullSink) snapshot() []StoreFullEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]StoreFullEvent(nil), s.events...)
}

// newDiskTestStore — a store with a stubbed statfs and a 5% threshold
// (space 50/100 = 50% — "plenty", 4/100 = 4% — "below the threshold").
func newDiskTestStore(t *testing.T, mutate func(*PackOptions)) (*PackStore, *packFakePool, *fakeSpace) {
	t.Helper()
	st, pool := newPackTestStore(t, func(o *PackOptions) {
		o.DiskFreePercent = 5
		if mutate != nil {
			mutate(o)
		}
	})
	fake := &fakeSpace{}
	fake.set(50, 100)
	st.stat = fake
	return st, pool, fake
}

// TestOSStatfs — a real statfs of a temporary directory: the capacity is known,
// the available space is at most the capacity; a missing directory is an error.
func TestOSStatfs(t *testing.T) {
	var osStat OSStatfs
	sp, err := osStat.Space(t.TempDir())
	if err != nil {
		t.Fatalf("Space: %v", err)
	}
	if sp.Total == 0 {
		t.Error("Total = 0 — statfs did not see the file system")
	}
	if sp.Avail > sp.Total {
		t.Errorf("Avail %d > Total %d", sp.Avail, sp.Total)
	}
	if _, err := osStat.Space(filepath.Join(t.TempDir(), "no-such")); err == nil {
		t.Error("Space of an absent directory: an error expected")
	}
}

// TestPutPauseOnLowDisk — the content write pause (spec 8): space below the
// threshold — Put of new content returns ErrStoreFull, no block or metadata
// row appears; the alert event goes to the hook with a space snapshot.
func TestPutPauseOnLowDisk(t *testing.T) {
	var sink *fullSink
	st, pool, fake := newDiskTestStore(t, func(o *PackOptions) {
		sink = &fullSink{}
		o.OnStoreFull = sink.hook
	})
	ctx := context.Background()

	good := []byte("<html>content before the overflow</html>")
	res, err := st.Put(ctx, good)
	if err != nil {
		t.Fatalf("Put with space: %v", err)
	}

	fake.set(4, 100) // 4% < the 5% threshold
	big := []byte("<html>new content into a full disk</html>")
	packBefore := compactPackSize(t, st.opts.Dir, 1)
	_, err = st.Put(ctx, big)
	if !errors.Is(err, ErrStoreFull) {
		t.Fatalf("Put without space: %v, want ErrStoreFull", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "4.0%") || !strings.Contains(msg, "5.0%") {
		t.Errorf("an error without the share and the threshold: %q", msg)
	}
	if _, ok := pool.row(HashOf(big)); ok {
		t.Error("the metadata row appeared despite ErrStoreFull")
	}
	if got := compactPackSize(t, st.opts.Dir, 1); got != packBefore {
		t.Errorf("the pack grew despite ErrStoreFull: %d → %d bytes", packBefore, got)
	}

	events := sink.snapshot()
	if len(events) != 1 {
		t.Fatalf("%d alert events, want 1 (every stopped Put counts)", len(events))
	}
	ev := events[0]
	if ev.Source != StoreFullPut || ev.Recovered {
		t.Errorf("event %+v: want Source=%s, Recovered=false", ev, StoreFullPut)
	}
	if ev.Space.Avail != 4 || ev.Space.Total != 100 {
		t.Errorf("an event without the space snapshot: %+v", ev.Space)
	}

	// Dedup keeps working: no bytes consumed, a reference is added.
	dup, err := st.Put(ctx, good)
	if err != nil {
		t.Fatalf("dedup-Put into a full disk: %v (content writing is paused, references are not)", err)
	}
	if dup.Created || dup.Hash != res.Hash {
		t.Errorf("dedup-Put: %+v, want Created=false with the same hash", dup)
	}
	if row, ok := pool.row(res.Hash); !ok || row.refc != 2 {
		t.Errorf("the refcount after the dedup during the pause: %+v, want 2", row)
	}
	// Reading does not stop: the analyzer and re-checks need the content.
	got, err := st.Get(ctx, res.Hash)
	if err != nil || !bytes.Equal(got, good) {
		t.Errorf("Get during the pause: %v, match=%v", err, bytes.Equal(got, good))
	}
}

// TestPutResumeAfterFree — recovery after reclamation (spec 8):
// the space returned above the threshold — writing continues; there is no separate
// used-space counter, statfs sees the current state (after compaction
// T-045 it is the same: the packs are removed, the space is free).
func TestPutResumeAfterFree(t *testing.T) {
	st, pool, fake := newDiskTestStore(t, nil)
	ctx := context.Background()

	fake.set(4, 100)
	if _, err := st.Put(ctx, []byte("into a full disk")); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("Put into a full disk: %v, want ErrStoreFull", err)
	}
	fake.set(50, 100) // the space was reclaimed (compaction, manual cleanup)
	res, err := st.Put(ctx, []byte("after the release"))
	if err != nil {
		t.Fatalf("Put after the release: %v", err)
	}
	if !res.Created {
		t.Error("Put after the release did not write the content")
	}
	if _, ok := pool.row(res.Hash); !ok {
		t.Error("the metadata row did not appear after the release")
	}
}

// TestPutDiskCheckDisabled — a zero threshold disables the control: statfs
// is not called at all (and does not fail on the stub with a zero snapshot).
func TestPutDiskCheckDisabled(t *testing.T) {
	st, _, fake := newDiskTestStore(t, func(o *PackOptions) { o.DiskFreePercent = 0 })
	ctx := context.Background()
	if _, err := st.Put(ctx, []byte("without the disk check")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n := fake.count(); n != 0 {
		t.Errorf("statfs called %d times with the check disabled, want 0", n)
	}
}

// TestPutDiskStatError — a statfs error is an infrastructure failure, not the
// verdict "out of space": Put returns it as is (not ErrStoreFull).
func TestPutDiskStatError(t *testing.T) {
	st, _, fake := newDiskTestStore(t, nil)
	fake.setErr(errors.New("statfs unavailable"))
	_, err := st.Put(context.Background(), []byte("content"))
	if err == nil || errors.Is(err, ErrStoreFull) {
		t.Fatalf("the statfs error masked: %v, want a plain error", err)
	}
}

// TestPutDiskZeroTotal — an exotic FS without capacity: the share is
// undefinable, the pause does not trigger (a false write stop is worse than a
// missed threshold).
func TestPutDiskZeroTotal(t *testing.T) {
	st, _, fake := newDiskTestStore(t, nil)
	fake.set(0, 0)
	res, err := st.Put(context.Background(), []byte("zero capacity"))
	if err != nil {
		t.Fatalf("Put at Total=0: %v", err)
	}
	if !res.Created {
		t.Error("the content not written at Total=0")
	}
}

// TestPutNoHook — no hook set: the pause works, no panic.
func TestPutNoHook(t *testing.T) {
	st, _, fake := newDiskTestStore(t, func(o *PackOptions) { o.OnStoreFull = nil })
	fake.set(4, 100)
	if _, err := st.Put(context.Background(), []byte("without the hook")); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("Put: %v, want ErrStoreFull", err)
	}
}

// TestDiskWatchLoop — the periodic check (spec 8): a Warn when entering
// the pause and an Info on recovery, events only on transitions;
// ctx cancellation — a clean nil.
func TestDiskWatchLoop(t *testing.T) {
	var (
		logBuf lockedBuffer
		sink   *fullSink
	)
	st, _, fake := newDiskTestStore(t, func(o *PackOptions) {
		sink = &fullSink{}
		o.OnStoreFull = sink.hook
	})
	log := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	lctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- DiskWatchLoop(lctx, st, DiskWatchOptions{Interval: 10 * time.Millisecond, Log: log})
	}()

	waitFor(t, func() bool { return fake.count() > 0 }, "the first DiskWatchLoop pass")
	// The first pass with space available (50%): no events, no Warn.
	if evs := sink.snapshot(); len(evs) != 0 {
		t.Fatalf("%d events with space, want 0", len(evs))
	}

	fake.set(4, 100) // the space fell below the threshold
	waitFor(t, func() bool { return logBuf.contains("content writing paused") }, "the pause Warn")
	evs := sink.snapshot()
	if len(evs) == 0 || evs[0].Source != StoreFullWatch || evs[0].Recovered {
		t.Fatalf("the pause event: %+v, want Source=%s Recovered=false", evs, StoreFullWatch)
	}

	fake.set(50, 100) // the space was reclaimed
	waitFor(t, func() bool { return logBuf.contains("content writing resumed") }, "the recovery Info")
	evs = sink.snapshot()
	if len(evs) < 2 || !evs[len(evs)-1].Recovered || evs[len(evs)-1].Source != StoreFullWatch {
		t.Errorf("the recovery event: %+v, want the last Recovered=true", evs)
	}
	// While the state does not change, there are no repeated events (an
	// edge-triggered signal).
	n := len(sink.snapshot())
	time.Sleep(30 * time.Millisecond)
	if got := len(sink.snapshot()); got != n {
		t.Errorf("events without a state change: %d → %d, want no change", n, got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("DiskWatchLoop after the cancel: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DiskWatchLoop did not finish after the ctx cancel")
	}
}

// TestDiskWatchLoopStatError — a statfs error: a Warn without a state change,
// the loop stays alive, the next tick retries.
func TestDiskWatchLoopStatError(t *testing.T) {
	var (
		logBuf lockedBuffer
		sink   *fullSink
	)
	st, _, fake := newDiskTestStore(t, func(o *PackOptions) {
		sink = &fullSink{}
		o.OnStoreFull = sink.hook
	})
	log := slog.New(slog.NewTextHandler(&logBuf, nil))
	fake.setErr(errors.New("statfs unavailable"))

	lctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- DiskWatchLoop(lctx, st, DiskWatchOptions{Interval: 10 * time.Millisecond, Log: log})
	}()
	waitFor(t, func() bool { return logBuf.contains("free space check") }, "the statfs error Warn")
	if evs := sink.snapshot(); len(evs) != 0 {
		t.Errorf("events on the statfs error: %+v, want none (the state does not change)", evs)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("DiskWatchLoop after the cancel: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DiskWatchLoop did not finish after the ctx cancel")
	}
}

// TestDiskWatchNilStore — a programming error, not a panic.
func TestDiskWatchNilStore(t *testing.T) {
	if err := DiskWatchLoop(context.Background(), nil, DiskWatchOptions{}); err == nil {
		t.Error("DiskWatchLoop(nil): an error expected")
	}
}

// TestHumanBytes — size formatting in messages.
func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    uint64
		want string
	}{
		{0, "0B"},
		{512, "512B"},
		{2048, "2.0KiB"},
		{5 << 20, "5.0MiB"},
		{3 << 30, "3.0GiB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.n); got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

// waitFor — polling a condition with a deadline (quiet retries of the loop's
// asynchronous transitions; the compactWaitFor style).
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("did not wait for: %s", what)
}

// lockedBuffer — a thread-safe buffer for a slog handler.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) contains(s string) bool {
	return strings.Contains(b.String(), s)
}

package watch

import (
	"testing"
	"time"
)

// fakeClock — deterministic time for the tracker tests.
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(0, 0)} }

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) Add(d time.Duration) { c.t = c.t.Add(d) }

func TestTrackerStableAfterWindow(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	if !tr.observe("f.txt", 42, mtime, false) {
		t.Fatal("observe of a new file must make it a candidate")
	}

	clock.Add(10*time.Second - time.Nanosecond)
	if _, ok := tr.ready("f.txt", 42, mtime); ok {
		t.Error("the file was delivered before the stability window expired")
	}

	clock.Add(time.Nanosecond)
	f, ok := tr.ready("f.txt", 42, mtime)
	if !ok {
		t.Fatal("the file was not delivered after the stability window")
	}
	if f.Path != "f.txt" || f.Size != 42 || !f.ModTime.Equal(mtime) {
		t.Errorf("File = %+v, want path f.txt, size 42, mtime %v", f, mtime)
	}

	// Redelivery of the same file is excluded.
	if _, ok := tr.ready("f.txt", 42, mtime); ok {
		t.Error("the file was delivered a second time")
	}
}

func TestTrackerEventResetsWindow(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)

	// A write event with the same signature (a coarse mtime on the FS):
	// the window must start anew.
	if !tr.observe("f.txt", 42, mtime, true) {
		t.Fatal("the write event did not return the file to the candidates")
	}
	if _, ok := tr.ready("f.txt", 42, mtime); ok {
		t.Error("the write event did not reset the stability window")
	}
	clock.Add(10 * time.Second)
	if _, ok := tr.ready("f.txt", 42, mtime); !ok {
		t.Error("the file was not delivered after the new window")
	}
}

func TestTrackerScanDoesNotResetUnchanged(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)

	// A scheduled pass with an unchanged signature does not reset the
	// window.
	tr.observe("f.txt", 42, mtime, false)
	if _, ok := tr.ready("f.txt", 42, mtime); !ok {
		t.Error("an unchanged-on-traversal file must not be delivered")
	}
}

func TestTrackerChangedSignatureResets(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	tr.observe("f.txt", 42, time.Unix(1000, 0), false)
	clock.Add(10 * time.Second)

	// An append: the size and mtime changed — a new window.
	mtime2 := time.Unix(1005, 0)
	if !tr.observe("f.txt", 100, mtime2, false) {
		t.Fatal("a changed file did not stay a candidate")
	}
	if _, ok := tr.ready("f.txt", 100, mtime2); ok {
		t.Error("a changed signature delivered the file without a new window")
	}
	if _, ok := tr.ready("f.txt", 42, time.Unix(1000, 0)); ok {
		t.Error("a stale candidate signature was delivered")
	}
	clock.Add(10 * time.Second)
	if f, ok := tr.ready("f.txt", 100, mtime2); !ok || f.Size != 100 {
		t.Errorf("after the new window the file was not delivered or the size is wrong: %+v ok=%v", f, ok)
	}
}

func TestTrackerReadyRequiresExactCurrentState(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	tr.observe("f.txt", 42, time.Unix(1000, 0), false)
	clock.Add(10 * time.Second)

	// The current state differs from the observed one (the file is
	// growing) — no delivery; the window will start anew at the next
	// observe.
	if _, ok := tr.ready("f.txt", 50, time.Unix(1000, 0)); ok {
		t.Error("a file with a changed size was delivered as stable")
	}
}

func TestTrackerForget(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)
	tr.forget("f.txt")

	if _, ok := tr.ready("f.txt", 42, mtime); ok {
		t.Error("a forgotten (deleted) file was delivered")
	}
	if got := tr.paths(); len(got) != 0 {
		t.Errorf("paths after forget = %v, want empty", got)
	}
}

func TestTrackerDeliveredSameSignatureNotTrackedAgain(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)
	if _, ok := tr.ready("f.txt", 42, mtime); !ok {
		t.Fatal("the file was not delivered")
	}

	// The same file seen again by a pass — not a candidate.
	if tr.observe("f.txt", 42, mtime, false) {
		t.Error("a delivered file with the same signature became a candidate again")
	}

	// A new file at the same path (a different signature) — a candidate.
	if !tr.observe("f.txt", 43, mtime.Add(time.Second), false) {
		t.Error("a new file at a known path did not become a candidate")
	}
	clock.Add(10 * time.Second)
	if _, ok := tr.ready("f.txt", 43, mtime.Add(time.Second)); !ok {
		t.Error("a new file at the same path was not delivered")
	}
}

// TestTrackerForgetDropsDelivered — the memory of the delivered is bounded
// by the existing paths: the path is gone (Remove/Rename — including a
// move of the processed file to processed/) → the delivered entry is
// removed, otherwise the delivered map would grow unboundedly with the
// number of distinct paths of a long-lived service (ING-P3-3). A file
// re-appearing with the same signature becomes a candidate again; the
// reprocessing will end with a duplicate by sha256 in the DB (T-021).
func TestTrackerForgetDropsDelivered(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)
	if _, ok := tr.ready("f.txt", 42, mtime); !ok {
		t.Fatal("the file was not delivered")
	}
	if tr.observe("f.txt", 42, mtime, false) {
		t.Fatal("a delivered file must not become a candidate again")
	}

	tr.forget("f.txt") // the file left the watched directory
	if len(tr.delivered) != 0 {
		t.Errorf("delivered = %v, want empty after forget", tr.delivered)
	}
	if !tr.observe("f.txt", 42, mtime, false) {
		t.Error("a re-appearing file with the same signature must become a candidate")
	}
	clock.Add(10 * time.Second)
	if _, ok := tr.ready("f.txt", 42, mtime); !ok {
		t.Error("a re-appearing file was not delivered after the stability window")
	}
}

func TestTrackerPathsSorted(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(time.Second, clock.Now)
	mtime := time.Unix(1000, 0)
	for _, p := range []string{"c.txt", "a.txt", "b.txt"} {
		tr.observe(p, 1, mtime, false)
	}
	got := tr.paths()
	want := []string{"a.txt", "b.txt", "c.txt"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paths = %v, want %v", got, want)
		}
	}
}

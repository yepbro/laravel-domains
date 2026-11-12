// Slow queries loop unit tests (T-177): the message format (the header,
// the lines with the service label and pid, the text truncation, the
// numeral agreement, the "…and more" tail), the pid+query_start
// debounce (a new statement — one message, a continuing one — silence,
// a finished one leaves the base, a new start of the same pid — a
// message again, a source failure does not drop the base), disabling
// by an interval of 0. A live pg_stat_activity is outside unit tests
// (hermeticity, the load_test.go / zonestats_test.go style).
package monitor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSlowSource — a source with scheduled snapshots per pass (the
// fakeLoadSource style); having exhausted the plan, it returns empty
// snapshots. The counter is under a mutex: the loop goroutine writes
// it, the test goroutine reads it (waitSlowPasses) — without the lock
// it is a race under -race (T-190; the zonestatsFakePool style). The
// passes/errs plan is filled before the loop starts and afterwards is
// only read — it needs no lock.
type fakeSlowSource struct {
	mu     sync.Mutex
	passes [][]SlowQuery
	errs   []error
	calls  int
}

func (s *fakeSlowSource) SlowQueries(context.Context, time.Duration) ([]SlowQuery, error) {
	s.mu.Lock()
	i := s.calls
	s.calls++
	s.mu.Unlock()
	if i < len(s.errs) && s.errs[i] != nil {
		return nil, s.errs[i]
	}
	if i < len(s.passes) {
		return s.passes[i], nil
	}
	return nil, nil
}

// count — the number of completed passes (a thread-safe read of the counter).
func (s *fakeSlowSource) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// slowQueryAt — a statement with the debounce key (pid, start).
func slowQueryAt(pid int, app string, sec float64, started time.Time, query string) SlowQuery {
	return SlowQuery{PID: pid, App: app, Seconds: sec, StartedAt: started, Query: query}
}

// waitSlowPasses waits until the source returns at least n snapshots
// (the TestZoneStatsLoopTicker style) and cancels the loop: after the
// plan is exhausted the snapshots are empty — no more deliveries
// appear, the message count checks are deterministic. The counter is
// read through count() under the fake's mutex: the loop goroutine
// writes it (a race under -race, T-190); the report itself (reports) is
// read only after <-done — the channel provides happens-before for all
// the loop deliveries.
func waitSlowPasses(t *testing.T, src *fakeSlowSource, n int, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for src.count() < n {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the loop did not reach pass %d: %d", n, src.count())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the loop: %v", err)
	}
}

func TestSlowQueryKey(t *testing.T) {
	base := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	a := slowQueryAt(7, "scheduler", 5, base, "SELECT 1")
	b := slowQueryAt(7, "scheduler", 9, base.Add(time.Second), "SELECT 1")
	if a.key() == b.key() {
		t.Error("the key: a new start of the same pid must differ")
	}
	if a.key() != slowQueryAt(7, "http-worker", 5, base, "SELECT 2").key() {
		t.Error("the key: pid+start matched — the same statement")
	}
}

// TestSlowQueryLoop_Passes — a full run of passes: the pid+query_start
// debounce: a continuing statement is silent, a new start of the same
// pid is an event again, a finished one leaves the base.
func TestSlowQueryLoop_Passes(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	long := slowQueryAt(11, "scheduler", 42.3, t0, "UPDATE domains SET …")
	other := slowQueryAt(12, "http-worker", 7.8, t0.Add(2*time.Second), "SELECT …")
	restart := slowQueryAt(11, "scheduler", 4.0, t0.Add(time.Minute), "SELECT count(*) …")

	var reports []string
	src := &fakeSlowSource{passes: [][]SlowQuery{
		{long},          // start: 1 new one
		{long, other},   // the continuing one is silent, the other is new
		{long, other},   // both continue — silence
		{},              // all finished
		{long, restart}, // pid 11: a new statement start — an event
		{long, restart}, // silence
	}}
	opts := SlowQueryOptions{
		Interval: time.Millisecond,
		Notify:   func(text string) { reports = append(reports, text) },
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, len(src.passes), cancel, done)

	if len(reports) != 3 {
		t.Fatalf("messages = %d, want 3 (a new statement + another statement + a new start)", len(reports))
	}
	// The header counts the new ones (events), those continuing from
	// previous passes — by the "total in flight" addition. Pass 5: after
	// the empty pass 4 both statements are "new" again (the key evicted —
	// in practice a finished statement does not return).
	for i, want := range []string{
		"1 slow query (threshold 3s)",
		"1 slow query (threshold 3s), total in flight 2",
		"2 slow queries (threshold 3s)",
	} {
		if !strings.Contains(reports[i], want) {
			t.Errorf("message %d without %q:\n%s", i, want, reports[i])
		}
	}
}

// TestSlowQueryLoop_ErrorKeepsDebounce — a source failure does not drop
// the debounce base: a statement observed before the error and back
// after it (the same pid+start — it did not finish) does not produce a
// repeated message.
func TestSlowQueryLoop_ErrorKeepsDebounce(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	long := slowQueryAt(21, "analyzer", 31.5, t0, "SELECT …")
	var reports []string
	src := &fakeSlowSource{passes: [][]SlowQuery{
		{long}, // start: 1 message
		{long}, // continuing: silence
		{long}, // a source error: a Warn without a delivery, the base lives
		{long}, // the snapshot returns: the same key — silence
	}}
	src.errs = []error{nil, nil, errors.New("DB unavailable"), nil}

	opts := SlowQueryOptions{Interval: time.Millisecond, Notify: func(text string) { reports = append(reports, text) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, len(src.passes), cancel, done)

	if len(reports) != 1 {
		t.Fatalf("messages = %d, want 1 (a failure does not drop the debounce base)", len(reports))
	}
}

// TestSlowQueryLoop_FinishedEvicted — an empty snapshot without an
// error — an honest "the statement finished": the key leaves the base,
// a re-observation of the same key (a degenerate case — in practice
// query_start is unique) counts as an event again.
func TestSlowQueryLoop_FinishedEvicted(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	long := slowQueryAt(21, "analyzer", 31.5, t0, "SELECT …")
	var reports []string
	src := &fakeSlowSource{passes: [][]SlowQuery{
		{long}, // start: 1 message
		{},     // finished: the key left the base
		{long}, // the same observation again — the key is no longer in the base: a message
	}}
	opts := SlowQueryOptions{Interval: time.Millisecond, Notify: func(text string) { reports = append(reports, text) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, len(src.passes), cancel, done)

	if len(reports) != 2 {
		t.Fatalf("messages = %d, want 2 (a new observation after completion)", len(reports))
	}
}

func TestSlowQueryLoop_Disabled(t *testing.T) {
	src := &fakeSlowSource{}
	err := SlowQueryLoop(context.Background(), src, SlowQueryOptions{Interval: 0})
	if err != nil {
		t.Fatalf("the disabled loop: %v", err)
	}
	if src.count() != 0 {
		t.Errorf("the disabled loop: passes = %d, want 0", src.count())
	}
	if err := SlowQueryLoop(context.Background(), nil, SlowQueryOptions{Interval: time.Minute}); err == nil {
		t.Error("the source is not set: an error is expected")
	}
}

// TestSlowQueryLoop_SourceError — an error of each pass does not kill
// the loop and does not deliver empty messages.
func TestSlowQueryLoop_SourceError(t *testing.T) {
	var reports []string
	src := &fakeSlowSource{errs: []error{errors.New("DB unavailable")}}
	opts := SlowQueryOptions{Interval: time.Millisecond, Notify: func(text string) { reports = append(reports, text) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, 3, cancel, done)

	if len(reports) != 0 {
		t.Fatalf("messages = %d, want 0 (a source error is not mailed out)", len(reports))
	}
}

func TestFormatSlowQueryReport(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	fresh := []SlowQuery{
		slowQueryAt(101, "scheduler", 42.34, t0, "UPDATE \"domains\"\n  SET  check_state = 1,\n      queued_at = now()"),
		slowQueryAt(202, "http-worker", 7.8, t0, "SELECT * FROM domains WHERE id = 42"),
		slowQueryAt(203, "", 4.2, t0, strings.Repeat("x", 300)),
		slowQueryAt(204, "web", 3.4, t0, "SELECT 1"),
		slowQueryAt(205, "web", 3.1, t0, "SELECT 2"),
	}
	r := formatSlowQueryReport(fresh, len(fresh), 3*time.Second)
	for _, want := range []string{
		"🐌 PostgreSQL: 5 slow queries (threshold 3s)",
		"• 42.3s [scheduler] pid 101: UPDATE \"domains\" SET check_state = 1, queued_at = now()", // spaces collapsed
		"• 7.8s [http-worker] pid 202: SELECT * FROM domains WHERE id = 42",
		"• 4.2s pid 203: " + strings.Repeat("x", slowQueryMaxChars) + "…", // no label, truncation
		"…and 2 more",
	} {
		if !strings.Contains(r, want) {
			t.Errorf("the message does not contain %q:\n%s", want, r)
		}
	}
	if n := strings.Count(r, "pid "); n != 3 {
		t.Errorf("query lines = %d, want 3 (the slowQueryTopLines cap)", n)
	}
	if s := ", total in flight"; strings.Contains(r, s) {
		t.Errorf("the total equals the new count — the %q addition is not needed:\n%s", s, r)
	}

	// Continuing from previous passes: 2 in flight, 1 new.
	r = formatSlowQueryReport(fresh[:1], 2, 3*time.Second)
	if !strings.Contains(r, "1 slow query (threshold 3s), total in flight 2") {
		t.Errorf("the header without \"total in flight 2\":\n%s", r)
	}

	// One query — the singular; a non-whole threshold — as is;
	// hidden text — the marker.
	r = formatSlowQueryReport([]SlowQuery{slowQueryAt(7, "monitor-cli", 9.1, t0, "")}, 1, 2500*time.Millisecond)
	for _, want := range []string{
		"1 slow query (threshold 2.5s",
		"• 9.1s [monitor-cli] pid 7: (text unavailable — no pg_read_all_stats)",
	} {
		if !strings.Contains(r, want) {
			t.Errorf("the message does not contain %q:\n%s", want, r)
		}
	}

	// The numeral agreement: 23 — the paucal form (2–4), 11 and 12 —
	// the genitive plural (the tens exception), 21 — the singular.
	for n, want := range map[int]string{
		2: "slow queries", 5: "slow queries",
		11: "slow queries", 21: "slow query", 23: "slow queries",
	} {
		if got := slowQueriesPhrase(n); got != want {
			t.Errorf("slowQueriesPhrase(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestSlowQueryText(t *testing.T) {
	if got := slowQueryText("  SELECT\t1\n  FROM\tx  "); got != "SELECT 1 FROM x" {
		t.Errorf("the spaces are not collapsed: %q", got)
	}
	if got := slowQueryText(strings.Repeat("x", slowQueryMaxChars+10)); len([]rune(got)) != slowQueryMaxChars+1 {
		t.Errorf("truncation: %d runes, want %d (+the ellipsis)", len([]rune(got)), slowQueryMaxChars)
	}
	if exact := strings.Repeat("x", slowQueryMaxChars); slowQueryText(exact) != exact {
		t.Errorf("the text exactly at the limit is not truncated: %q", slowQueryText(exact))
	}
	if got := slowQueryText("  "); !strings.Contains(got, "text unavailable") {
		t.Errorf("an empty text: %q", got)
	}
}

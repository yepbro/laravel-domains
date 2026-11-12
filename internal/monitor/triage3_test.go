// The third triage finish (the monitor run of 0.970): the exact texts of
// read errors (a missing file — the "no such file" text, a missing
// field — the missing-field text, an empty answer — the empty-answer
// text), return cut-outs in OSLoad (broken loadavg/meminfo fail the
// pass, not a zero snapshot), a malformed PSI some at a valid full,
// negative top checks (the fifth line is not printed), cpu-line
// boundaries (exactly 5 fields), MemTotal 0 with a zero pin, a disk
// Total of 1 byte, a negative busy in the gauge, a nil loop src,
// newDoneStale with value=1, notify without getenv, the render debt
// source call counter, the VerifyCounters row closing on every branch,
// UnitEvents with a ctx cancellation (errors.Is) and a 1ns timeout,
// parseMemoryEvents with a valueless counter key line.
package monitor

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeCgroup/fakeSystemctl — the unit events fixtures (unitevents_test.go).

// TestOSLoadSourceErrorTexts — broken required files: the error carries
// the original read text (return cut-out mutants would lead to a zero
// snapshot or a re-error with a foreign text).
func TestOSLoadSourceErrorTexts(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "loadavg", "x 1 2\n") // loadavg is not a number
	_, err := (OSLoad{Root: root}).Load(context.Background())
	if err == nil || !strings.Contains(err.Error(), "loadavg") {
		t.Errorf("a broken loadavg: %v, want a loadavg error", err)
	}

	root2 := fullCPUFixtures(t)
	writeFix(t, root2, "meminfo", "MemTotal: x kB\nMemAvailable: 1 kB\n")
	_, err = (OSLoad{Root: root2}).Load(context.Background())
	if err == nil || !strings.Contains(err.Error(), "meminfo") {
		t.Errorf("a broken meminfo: %v, want a meminfo error", err)
	}

	// A missing file — the "no such file or directory" text (a read
	// error, not a derived form).
	for _, miss := range []string{"loadavg", "stat", "meminfo"} {
		root3 := fullCPUFixtures(t)
		if err := removeFix(root3, miss); err != nil {
			t.Fatal(err)
		}
		_, err := (OSLoad{Root: root3}).Load(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no such file") {
			t.Errorf("no %s: %v, want the original file read error", miss, err)
		}
	}
}

// TestOSLoadBrokenSomeValidFull — a malformed some line at a valid full
// (and vice versa): an error of the respective kind — the pass is not
// silent and does not substitute the kinds.
func TestOSLoadBrokenSomeValidFull(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "pressure/memory",
		"some avg10=x avg60=1 avg300=1\nfull avg10=1 avg60=1 avg300=1\n")
	// A malformed some line fails the pass even at a valid full (a
	// return cut-out mutant swallowed the error and returned a snapshot).
	if _, err := (OSLoad{Root: root}).Load(context.Background()); err == nil {
		t.Error("a broken some with a valid full: want a pass error")
	}
	root2 := fullCPUFixtures(t)
	writeFix(t, root2, "pressure/memory",
		"some avg10=1 avg60=1 avg300=1\nfull avg10=x avg60=1 avg300=1\n")
	if _, err := (OSLoad{Root: root2}).Load(context.Background()); err == nil {
		t.Error("a broken full: want an error")
	}
}

// TestReadMemInfoNoFileText — a missing meminfo: the "no such file"
// text, zero values.
func TestReadMemInfoNoFileText(t *testing.T) {
	tt, a, st, sf, err := readMemInfo(filepath.Join(t.TempDir(), "missing"))
	if err == nil || !strings.Contains(err.Error(), "no such file") ||
		tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("no file: (%v/%v/%v/%v, %v), want zeros and the read text", tt, a, st, sf, err)
	}
}

// TestReadMemInfoMissingFieldPins — missing MemTotal/MemAvailable: zero
// values (a pin) and the requiredness error.
func TestReadMemInfoMissingFieldPins(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "meminfo")
	if err := writeFileForTest(p, "Cached: 1 kB\n"); err != nil {
		t.Fatal(err)
	}
	tt, a, st, sf, err := readMemInfo(p)
	if err == nil || tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("no required fields: (%v/%v/%v/%v, %v), want zeros and an error", tt, a, st, sf, err)
	}
	// MemTotal 0 — the same error, zeros.
	if err := writeFileForTest(p, "MemTotal: 0 kB\nMemAvailable: 1 kB\n"); err != nil {
		t.Fatal(err)
	}
	tt, a, st, sf, err = readMemInfo(p)
	if err == nil || tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("MemTotal 0: (%v/%v/%v/%v, %v), want zeros and an error", tt, a, st, sf, err)
	}
	// A value without a unit (one field): a parse error, not a panic on
	// the index of the neighboring field.
	if err := writeFileForTest(p, "MemTotal: y\n"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := readMemInfo(p); err == nil {
		t.Error("a value without a unit: want a parse error")
	}
}

// TestReadCPUStatFieldsBoundary — exactly five fields of the cpu line is
// valid, four is an error, six is valid (the < 5 boundaries).
func TestReadCPUStatFieldsBoundary(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "stat")
	if err := writeFileForTest(p, "cpu  1 2 3\ncpu0 1 1 1 1\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := readCPUStat(p); err == nil {
		t.Error("three numbers: want an error (the line is shorter than five fields)")
	}
	// Four numbers (five fields with "cpu") are valid: missing fields are
	// read as zeros.
	if err := writeFileForTest(p, "cpu  1 2 3 4\ncpu0 1 1 1 1\n"); err != nil {
		t.Fatal(err)
	}
	st4, err := readCPUStat(p)
	if err != nil || st4.Idle != 4 || st4.Iowait != 0 {
		t.Errorf("four numbers: (%+v, %v), want idle 4 iowait 0", st4, err)
	}
	if err := writeFileForTest(p, "cpu  1 2 3 4 5\ncpu0 1 1 1 1\n"); err != nil {
		t.Fatal(err)
	}
	st, err := readCPUStat(p)
	if err != nil || st.Idle != 4 || st.Iowait != 5 {
		t.Errorf("five fields: (%+v, %v), want idle 4 iowait 5", st, err)
	}
	if err := writeFileForTest(p, "cpu  1 2 3 4 5 6\ncpu0 1 1 1 1\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := readCPUStat(p); err != nil {
		t.Errorf("six fields: %v, want valid", err)
	}
}

// TestReadProcsNoEmptyComm — a contribution without a name does not get
// into the aggregate (a PID without stat / a malformed line): the ""
// key is absent.
func TestReadProcsNoEmptyComm(t *testing.T) {
	root := fullCPUFixtures(t)
	mkdirFixRaw(t, root, "400") // a PID without stat — no contribution
	mkdirFix(t, root, "401", "(broken) no parens here", "1 1\n")
	procs, err := readProcs(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := procs[""]; ok {
		t.Errorf("an empty name in the aggregate: %+v", procs[""])
	}
}

// TestLoadReportLoopNilSource — the source not set: an error, not a panic.
func TestLoadReportLoopNilSource(t *testing.T) {
	if err := LoadReportLoop(context.Background(), nil, LoadReportOptions{Interval: time.Minute}); err == nil ||
		!strings.Contains(err.Error(), "source is not set") {
		t.Errorf("a nil source: %v, want an error", err)
	}
}

// TestFormatLoadReportTopNotExtended — the fifth top CPU candidate is
// not printed (the slice is exactly topProcs).
func TestFormatLoadReportTopNotExtended(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1,
	}
	procCPU := map[string]float64{"a": 2, "b": 1.7, "c": 1.3, "d": 0.9, "e": 0.7}
	r := formatLoadReport(st, nil, nil, procCPU, nil, nil, false, nil, nil, nil)
	if strings.Contains(r, "e 0.7c") {
		t.Errorf("the fifth top candidate was printed:\n%s", r)
	}
	if want := "🧮 CPU top: a 2.0c, b 1.7c, c 1.3c, d 0.9c"; topCPULine(r) != want {
		t.Errorf("the CPU top = %q, want %q", topCPULine(r), want)
	}
}

// TestCPUDeltaClampFraction — busy in (−1, 0) is clamped to zero too
// (a fractional idle overflow, a clamp boundary weakening mutant).
func TestCPUDeltaClampFraction(t *testing.T) {
	prev := CPUStat{Total: 100, Cores: 1}
	cur := CPUStat{Idle: 300, Total: 300, Cores: 1} // d=200, idle 1.5
	busy, _, _, _, ok := cpuDelta(prev, cur)
	if !ok {
		t.Fatal("the delta was not computed")
	}
	if busy != 0 {
		t.Errorf("busy = %v, want 0 (the fractional overflow clamp)", busy)
	}
}

// TestFormatLoadReportTinyDisk — a disk Total of 1 byte: the percent is
// computed (not suppressed), 100% at a full avail.
func TestFormatLoadReportTinyDisk(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1,
	}
	disks := []DiskLine{{Dir: "/tiny", Avail: 1, Total: 1}}
	r := formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, disks, nil)
	if !strings.Contains(r, "📀 Disk /tiny: free 0 of 0 GiB (100%)") {
		t.Errorf("the disk Total 1: %q, want 100%%", r)
	}
}

// TestLoadReportLoopSpaceErrorSecondDir — a statfs failure of the first
// directory does not break the section: the second directory is printed.
func TestLoadReportLoopSpaceErrorSecondDir(t *testing.T) {
	st := LoadStat{Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1}
	src := &fakeLoadSource{states: []LoadStat{st}}
	space := &switchSpaceSource{
		errs:  []error{errors.New("statfs: permission denied"), nil},
		stats: []SpaceStat{{}, {Avail: 1 << 30, Total: 2 << 30}},
	}
	reports := make(chan string, 4)
	opts := LoadReportOptions{
		Interval: time.Hour,
		Space:    space,
		Dirs:     []string{"/bad", "/good"},
		Notify:   func(text string) { reports <- text },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, src, opts) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the loop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not exit")
	}
	select {
	case r := <-reports:
		if !strings.Contains(r, "📀 Disk /good:") {
			t.Errorf("the second directory vanished after the first one failed:\n%s", r)
		}
		if strings.Contains(r, "/bad") {
			t.Errorf("the failing directory is in the report:\n%s", r)
		}
	default:
		t.Fatal("the report did not arrive")
	}
}

// switchSpaceSource — a SpaceChecker with per-directory results in order
// (the first is an error).
type switchSpaceSource struct {
	stats []SpaceStat
	errs  []error
	i     int
}

func (s *switchSpaceSource) Space(string) (SpaceStat, error) {
	if s.i < len(s.errs) && s.errs[s.i] != nil {
		s.i++
		return SpaceStat{}, s.errs[s.i-1]
	}
	if s.i >= len(s.stats) {
		return SpaceStat{}, nil
	}
	st := s.stats[s.i]
	s.i++
	return st, nil
}

// TestNewDoneStaleValueOne — a value of 1 is a full-fledged snapshot
// (the series grows, the n > 0 boundary).
func TestNewDoneStaleValueOne(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tr := &newDoneStaleTrack{minPasses: 2}
	row := func(v int64, at time.Time) staleRow {
		return staleRow{value: v, snap: sql.NullTime{Time: at, Valid: true}}
	}
	if s := tr.decide("", time.Time{}, row(1, t0)); !strings.Contains(s.detail, "1/2") {
		t.Errorf("value=1: %s, want the streak 1/2", s.detail)
	}
	if s := tr.decide("", time.Time{}, row(1, t0.Add(time.Minute))); s.status != AlertStatusFiring {
		t.Errorf("value=1, the second snapshot: %s, want firing", s.status)
	}
}

// TestTelegramNotifierNoGetenv — the constructor without Getenv uses
// os.Getenv (no panic on a nil function).
func TestTelegramNotifierNoGetenv(t *testing.T) {
	// There are no secrets in the test environment: the constructor
	// honestly disables the delivery, but getenv was called (not a nil
	// function).
	n := NewTelegramNotifier(TelegramOptions{Enabled: true, Script: "/bin/true"})
	if n == nil {
		t.Fatal("the constructor without Getenv returned nil")
	}
}

// TestRedisUptimeMissingFieldText — a missing INFO server field is the
// missing-field error (not a silent zero).
func TestRedisUptimeMissingFieldText(t *testing.T) {
	if _, err := (RedisInfo{RDB: &fakeInfoRedis{info: "# Server\nrun_id:x\n"}}).RedisUptime(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "uptime_in_seconds field is missing") {
		t.Errorf("no field: %v, want \"the field is missing\"", err)
	}
}

// TestRedisUptimeCmdErrorUnderlying — an INFO command error carries the
// original reason (a return cut-out mutant would fall into parsing an
// empty answer — the text would be the missing-field one, without the
// command reason).
func TestRedisUptimeCmdErrorUnderlying(t *testing.T) {
	failing := &fakeInfoRedis{err: errors.New("connection reset")}
	if _, err := (RedisInfo{RDB: failing}).RedisUptime(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "connection reset") {
		t.Errorf("a command error: %v, want the original command reason", err)
	}
}

// TestRenderDebtIterErrorEmptyText — an iteration break at ZERO rows
// (Next false right away, rows.Err ≠ nil): the reason is the break, not
// the missing-row one (a return-muting mutant substituted the
// missing-row text, sending diagnostics to the runbooks — the T-175
// review, P3-2).
func TestRenderDebtIterErrorEmptyText(t *testing.T) {
	pool := &alertsPool{debtEmpty: true, debtRowsErr: errors.New("iteration drop")}
	_, err := (RenderDebtReader{Pool: pool, Redis: &fakeLener{}}).RenderDebt(context.Background())
	if err == nil || !strings.Contains(err.Error(), "iteration drop") {
		t.Errorf("an iteration drop without rows: %v, want the drop reason (not \"the row is missing\")", err)
	}
}

// TestRedisMemoryMissingFieldText — a missing INFO memory field is the
// missing-field error.
func TestRedisMemoryMissingFieldText(t *testing.T) {
	if _, err := (RedisInfo{RDB: &fakeInfoRedis{info: "# Memory\nused_memory_human:1K\n"}}).RedisMemory(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "is missing") {
		t.Errorf("no field: %v, want \"the field is missing\"", err)
	}
}

// TestParsePSIMissingFieldText — a missing PSI field is the
// missing-field error (a mutant swallowed it and returned a partial
// snapshot).
func TestParsePSIMissingFieldText(t *testing.T) {
	if _, err := parsePSI("some avg60=1 avg300=1\n"); err == nil ||
		!strings.Contains(err.Error(), "avg10 field is missing") {
		t.Errorf("no avg10: %v, want \"the avg10 field is missing\"", err)
	}
}

// TestStreamGroupsClosedFlags — the group fake: empty groups do not
// request XLEN (a call counter).
func TestStreamGroupsClosedFlags(t *testing.T) {
	inner := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{alertStreams[0]: {}},
		xlen:   map[string]int64{},
	}
	rdb := &countingGroupsRedis{fakeGroupsRedis: *inner}
	_, err := (RedisStreamGroups{RDB: rdb}).StreamGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rdb.xlenCalls != 0 {
		t.Errorf("XLEN was called %d times with empty groups, want 0", rdb.xlenCalls)
	}
}

// countingGroupsRedis — a fakeGroupsRedis with an XLEN counter.
type countingGroupsRedis struct {
	fakeGroupsRedis
	xlenCalls int
}

func (r *countingGroupsRedis) XLen(ctx context.Context, stream string) *redis.IntCmd {
	r.xlenCalls++
	return r.fakeGroupsRedis.XLen(ctx, stream)
}

// TestRenderDebtLoopSourceCalls — a 1ns Interval: the ticker hammers,
// the source is called many times (a "disabled" mutant would call only
// the starting one).
func TestRenderDebtLoopSourceCalls(t *testing.T) {
	src := &countingDebtSource{debt: RenderDebt{Flags: 1, Lag: 1}}
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- RenderDebtLoop(ctx, src, RenderDebtOptions{Interval: 1}) }()
	deadline := time.Now().Add(1 * time.Second)
	for src.calls() < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the loop: %v", err)
	}
	if src.calls() < 5 {
		t.Errorf("source calls = %d, want ≥5 (the 1ns ticker is alive)", src.calls())
	}
}

// countingDebtSource — a render debt source with a call counter;
// the counter is atomic: RenderDebt is called from the loop goroutine,
// calls() from the test before the loop stops (race-detector).
type countingDebtSource struct {
	debt RenderDebt
	n    atomic.Int64
}

func (s *countingDebtSource) RenderDebt(context.Context) (RenderDebt, error) {
	s.n.Add(1)
	return s.debt, nil
}

func (s *countingDebtSource) calls() int { return int(s.n.Load()) }

// TestVerifyCountersClosedEveryBranch — the cross-check rows are Closed
// on every error branch (a Scan error and an empty history answer).
func TestVerifyCountersClosedEveryBranch(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	aggRow := aggRowOf(7, 10, 4, 2, 3, statuses, 0)
	// A history Scan error: hrows.Close was called.
	steps := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow),
		&zonestatsFakeRows{rows: [][]any{{int64(9)}}, scanErr: errors.New("boom")},
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{}); err == nil {
		t.Fatal("a history Scan error: want an error")
	}
	if !steps[2].rows.closed {
		t.Error("hrows are not closed on a Scan error")
	}
	// An empty history answer: hrows.Close was called.
	steps2 := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow),
		zonestatsRows(),
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps2}, VerifyOptions{}); err == nil {
		t.Fatal("an empty history: want an error")
	}
	if !steps2[2].rows.closed {
		t.Error("hrows are not closed on an empty response")
	}
	// A history rows.Err: closed.
	steps3 := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow),
		&zonestatsFakeRows{rows: [][]any{{int64(0), int64(0)}}, err: errors.New("drop")},
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps3}, VerifyOptions{}); err == nil {
		t.Fatal("a history rows.Err: want an error")
	}
	if !steps3[2].rows.closed {
		t.Error("hrows are not closed on rows.Err")
	}
	// A zones rows.Err: closed.
	steps4 := verifySteps(
		&zonestatsFakeRows{rows: [][]any{verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})}, err: errors.New("drop")},
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps4}, VerifyOptions{}); err == nil {
		t.Fatal("a zones rows.Err: want an error")
	}
	if !steps4[0].rows.closed {
		t.Error("the zone rows are not closed on rows.Err")
	}
	// A zones Scan error: closed.
	steps5 := verifySteps(
		&zonestatsFakeRows{rows: [][]any{{int16(7)}}, scanErr: errors.New("boom")},
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps5}, VerifyOptions{}); err == nil {
		t.Fatal("a zones Scan error: want an error")
	}
	if !steps5[0].rows.closed {
		t.Error("the zone rows are not closed on a Scan error")
	}
	// A stored rows.Err: closed.
	steps6 := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		&zonestatsFakeRows{rows: [][]any{counterRowOf("render_pending", 0)}, err: errors.New("drop")})
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps6}, VerifyOptions{}); err == nil {
		t.Fatal("a stored rows.Err: want an error")
	}
	if !steps6[3].rows.closed {
		t.Error("the stored rows are not closed on rows.Err")
	}
	// An aggregate rows.Err (read by readZoneAgg with a defer Close) — closed.
	steps7 := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		&zonestatsFakeRows{rows: [][]any{aggRow}, err: errors.New("drop")},
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps7}, VerifyOptions{}); err == nil {
		t.Fatal("an aggregate rows.Err: want an error")
	}
	if !steps7[1].rows.closed {
		t.Error("the aggregate rows are not closed on rows.Err")
	}
}

// TestZoneStatsWriteDailyEmptyText — an empty upsert answer is the
// empty-upsert-answer error (not a derived Scan).
func TestZoneStatsWriteDailyEmptyText(t *testing.T) {
	p := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	tx, _ := p.Begin(context.Background())
	_, err := writeDailyCounter(context.Background(), tx, recountDailyChecksSQL, 5, time.Unix(0, 0), "the daily checks row")
	if err == nil || !strings.Contains(err.Error(), "empty upsert response") {
		t.Errorf("an empty upsert: %v, want \"empty upsert response\"", err)
	}
}

// TestOSUnitEventsCtxErrors — a ctx cancellation before and after the
// timeout wrapper: the errors are context ones (errors.Is), not an
// empty snapshot.
func TestOSUnitEventsCtxErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (OSUnitEvents{}).UnitEvents(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancellation before the start: %v, want context.Canceled", err)
	}
	// A 1ns Timeout — an expired wrapper: a context error (not the window
	// default).
	if _, err := (OSUnitEvents{Timeout: 1}).UnitEvents(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("Timeout 1ns: %v, want deadline exceeded", err)
	}
}

// TestParseMemoryEventsBareKey — a key line without a value does not
// break the parsing and does not turn into a number error (foreign keys
// do not pass).
func TestParseMemoryEventsBareKey(t *testing.T) {
	if _, err := parseMemoryEvents("oom\n"); err != nil {
		t.Errorf("a key without a value: %v, want a skip", err)
	}
	if ev, err := parseMemoryEvents("oom\noom 4\n"); err != nil || ev.OOM != 4 {
		t.Errorf("a key without a value + a valid line: (%+v, %v)", ev, err)
	}
}

// TestAlertDescKnownAndUnknown — a known alert gets the legend
// description (not the name), an unknown one — the name.
func TestAlertDescKnownAndUnknown(t *testing.T) {
	for name := range alertDescs {
		if got := alertDesc(name); got == name {
			t.Errorf("alertDesc(%s) = the name, want the legend description", name)
		}
		break // one known one is enough
	}
	if got := alertDesc("no-such-alert"); got != "no-such-alert" {
		t.Errorf("alertDesc(unknown) = %q, want the name as is", got)
	}
}

// TestOSUnitEventsSingleBrokenEvents — one unit with a malformed
// memory.events (readable but not a number): the snapshot is not
// returned (an unread counter is not substituted with zeros), the error
// names the unit.
func TestOSUnitEventsSingleBrokenEvents(t *testing.T) {
	root := t.TempDir()
	cg := filepath.Join(root, "cg")
	show := "Id=crawler-http-worker.service\nNRestarts=2\n"
	fakeCgroup(t, cg, "crawler-http-worker.service", "oom x\n")
	_, err := (OSUnitEvents{
		CgroupFS:  cg,
		Systemctl: fakeSystemctl(t, root, show, ""),
	}).UnitEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), "crawler-http-worker.service") {
		t.Errorf("a broken memory.events: %v, want an error with the unit name", err)
	}
}

// Unit tests of the system load report (T-173): the parsing of the /proc files
// (loadavg, stat, meminfo, the process lines, PSI some/full), the utilization
// and process tick deltas, the tops, the report format (sections,
// "n/a" degradation), the OSLoad source on a fixture directory, the loop on
// fakes with one–two passes (a name restart between passes — the CPU top
// window is skipped without underflow junk, T-189); the queue source
// RedisQueues — the consumer group backlog on a fake redisGroupsCmd
// (T-188). Live /proc and Redis — outside the unit tests (hermeticity,
// the psi_test.go style).
package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"crawler/internal/queue"
)

// writeFix — a fixture file in the temporary root (the parent directory
// is created — the fixtures live in pressure/ and PID directories).
func writeFix(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("the fixture %s: the directory: %v", name, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("the fixture %s: %v", name, err)
	}
}

// loadFixtures — the /proc fixture directory: 4 cores, a load of 24/22/20, 2 GiB
// of used memory, a 200 MiB swap, PSI, three processes (two headless-shell).
func loadFixtures(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFix(t, root, "loadavg", "24.82 21.96 20.67 3/943 1588621\n")
	// 4 cores × 100 ticks: busy 97/100 by the sum of the cores (user 70, sys 24,
	// idle 3, iowait 0 … per core) — the cpu line is aggregated.
	writeFix(t, root, "stat",
		"cpu  28000 0 9600 1200 0 200 0 0\n"+
			"cpu0 7000 0 2400 300 0 50 0 0\n"+
			"cpu1 7000 0 2400 300 0 50 0 0\n"+
			"cpu2 7000 0 2400 300 0 50 0 0\n"+
			"cpu3 7000 0 2400 300 0 50 0 0\n")
	writeFix(t, root, "meminfo",
		"MemTotal:        7973728 kB\n"+
			"MemFree:          430080 kB\n"+
			"MemAvailable:    2359296 kB\n"+
			"Cached:          3801088 kB\n"+
			"SwapTotal:       3932160 kB\n"+
			"SwapFree:        3729408 kB\n")
	writeFix(t, root, "pressure/cpu",
		"some avg10=91.59 avg60=90.37 avg300=88.90 total=115180632759\n")
	writeFix(t, root, "pressure/memory",
		"some avg10=7.85 avg60=7.57 avg300=5.97 total=19881877618\n"+
			"full avg10=2.00 avg60=1.87 avg300=1.76 total=8010557278\n")

	// The processes: utime+stime — the fields 12/13 after the name; the statm resident —
	// field 2 (4 KiB pages do not matter in the test — the host page multiplier).
	// 1588621 (monitor) 20 0 ... utime=300 stime=100 → 400 ticks.
	mkdirFix(t, root, "1588621", "(monitor) R 1 0 0 0 -1 4194560 0 0 0 0 "+
		"300 100 0 0 20 0 1 0", "3000 500 200 10 0 400 0\n")
	// Two headless-shell with 1000 ticks and 262144 pages (1 GiB) each.
	mkdirFix(t, root, "201", "(headless-shell) S 1 0 0 0 -1 4194560 0 0 0 0 "+
		"700 300 0 0 20 0 1 0", "262144 262144 100 10 0 262144 0\n")
	mkdirFix(t, root, "202", "(headless-shell) S 1 0 0 0 -1 4194560 0 0 0 0 "+
		"600 400 0 0 20 0 2 0", "262144 262144 100 10 0 262144 0\n")
	// postgres with a name containing spaces and parentheses.
	mkdirFix(t, root, "203", "(postgres: 1[2] writer) S 1 0 0 0 -1 0 0 0 0 0 "+
		"500 500 0 0 20 0 3 0", "1000 900 50 5 0 900 0\n")
	// A non-PID directory and a file — ignored.
	if err := os.Mkdir(filepath.Join(root, "acpi"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFix(t, root, "uptime", "12345.67 89012.34\n")
	return root
}

// mkdirFix — a PID fixture directory with stat and statm.
func mkdirFix(t *testing.T, root, pid, stat, statm string) {
	t.Helper()
	dir := filepath.Join(root, pid)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFix(t, dir, "stat", pid+" "+stat+"\n")
	writeFix(t, dir, "statm", statm)
}

func TestOSLoad_Fixtures(t *testing.T) {
	st, err := OSLoad{Root: loadFixtures(t)}.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if st.Load1 != 24.82 || st.Load5 != 21.96 || st.Load15 != 20.67 {
		t.Errorf("loadavg = %v/%v/%v", st.Load1, st.Load5, st.Load15)
	}
	if st.CPU.Cores != 4 {
		t.Errorf("cores = %d, want 4", st.CPU.Cores)
	}
	if want := uint64(28000 + 9600 + 1200 + 200); st.CPU.Total != want {
		t.Errorf("the Total ticks = %d, want %d", st.CPU.Total, want)
	}
	if !st.PSIPresent || st.CPUPSI.Avg10 != 91.59 {
		t.Errorf("CPU PSI = %+v present=%v", st.CPUPSI, st.PSIPresent)
	}
	if st.MemPSISome.Avg10 != 7.85 || st.MemPSIFull.Avg10 != 2.00 {
		t.Errorf("mem PSI some/full = %+v / %+v", st.MemPSISome, st.MemPSIFull)
	}
	if st.MemTotal != 7973728*1024 || st.MemAvailable != 2359296*1024 {
		t.Errorf("meminfo total/available = %d/%d", st.MemTotal, st.MemAvailable)
	}
	if st.SwapTotal != 3932160*1024 || st.SwapFree != 3729408*1024 {
		t.Errorf("swap total/free = %d/%d", st.SwapTotal, st.SwapFree)
	}

	// The processes: the aggregates by name — headless-shell merged (2 processes),
	// the corrupt postgres name with spaces/parentheses parsed whole.
	if got := st.Procs["monitor"].Ticks; got != 400 {
		t.Errorf("monitor ticks = %d, want 400", got)
	}
	if got := st.Procs["headless-shell"]; got.Ticks != 2000 || got.Count != 2 {
		t.Errorf("headless-shell = %+v, want ticks 2000, 2 processes", got)
	}
	if got := st.Procs["postgres: 1[2] writer"].Ticks; got != 1000 {
		t.Errorf("postgres ticks = %d, want 1000", got)
	}
	if _, ok := st.Procs["acpi"]; ok {
		t.Error("a non-PID directory got into the processes")
	}
}

func TestOSLoad_NoPressureFiles(t *testing.T) {
	root := loadFixtures(t)
	// A kernel without PSI: the pressure files are missing — the snapshot lives, PSI is "n/a".
	if err := os.RemoveAll(filepath.Join(root, "pressure")); err != nil {
		t.Fatal(err)
	}
	st, err := OSLoad{Root: root}.Load(context.Background())
	if err != nil {
		t.Fatalf("Load without PSI: %v", err)
	}
	if st.PSIPresent {
		t.Error("PSIPresent = true without the pressure files")
	}
}

func TestOSLoad_BrokenMandatory(t *testing.T) {
	root := loadFixtures(t)
	if err := os.Remove(filepath.Join(root, "meminfo")); err != nil {
		t.Fatal(err)
	}
	if _, err := (OSLoad{Root: root}).Load(context.Background()); err == nil {
		t.Error("Load without meminfo: want a pass error")
	}
}

func TestCPUDelta(t *testing.T) {
	prev := CPUStat{User: 1000, System: 500, Idle: 500, Total: 2000, Cores: 4}
	cur := CPUStat{User: 1200, System: 700, Idle: 600, Total: 2500, Cores: 4}
	busy, user, sys, io, ok := cpuDelta(prev, cur)
	if !ok {
		t.Fatal("the delta was not computed on growing counters")
	}
	// dTotal=500: user 200/500=0.4, sys 200/500=0.4, idle 100/500=0.2,
	// iowait 0 → busy 0.8.
	if busy != 0.8 || user != 0.4 || sys != 0.4 || io != 0 {
		t.Errorf("delta = busy %v user %v sys %v io %v, want 0.8/0.4/0.4/0", busy, user, sys, io)
	}
	if _, _, _, _, ok := cpuDelta(CPUStat{}, cur); ok {
		t.Error("the first pass (prev.Total=0) is not \"n/a\"")
	}
	if _, _, _, _, ok := cpuDelta(cur, cur); ok {
		t.Error("a zero delta is not \"n/a\"")
	}
}

// fakeLoadSource — a LoadSource with a snapshot queue (exhausted — the last one).
type fakeLoadSource struct {
	states []LoadStat
}

func (s *fakeLoadSource) Load(context.Context) (LoadStat, error) {
	if len(s.states) == 0 {
		return LoadStat{}, errors.New("source exhausted")
	}
	st := s.states[0]
	if len(s.states) > 1 {
		s.states = s.states[1:]
	}
	return st, nil
}

// fakeLoadQueues — a QueueSource with a prepared snapshot.
type fakeLoadQueues struct {
	queues map[string]QueueStat
	err    error
}

func (s fakeLoadQueues) Queues(context.Context) (map[string]QueueStat, error) {
	return s.queues, s.err
}

// fakeLoadSpace — a SpaceChecker with a single answer.
type fakeLoadSpace struct {
	stat SpaceStat
	err  error
}

func (s fakeLoadSpace) Space(string) (SpaceStat, error) { return s.stat, s.err }

func TestLoadReportLoop_OnePass(t *testing.T) {
	st, err := OSLoad{Root: loadFixtures(t)}.Load(context.Background())
	if err != nil {
		t.Fatalf("the snapshot: %v", err)
	}
	src := &fakeLoadSource{states: []LoadStat{st}}
	// The Len of the fixtures — backlogs (T-188): the line format does not change.
	queues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 121, Read: 500},
		queue.StreamRender: {Len: 116320, Read: 1200},
	}
	var reports []string
	opts := LoadReportOptions{
		Interval: time.Minute,
		Queues:   fakeLoadQueues{queues: queues},
		Space:    fakeLoadSpace{stat: SpaceStat{Avail: 44 << 30, Total: 79 << 30}},
		Dirs:     []string{"/var/lib/crawler/content"},
		Notify:   func(text string) { reports = append(reports, text) },
	}

	// A cancellation BEFORE the start: the starting pass runs (a snapshot after a restart
	// — right away), the loop exits by the already closed ctx — exactly one report.
	// The deltas (the utilization, the rates) are not checked by this pass —
	// the formatLoadReport test below counts them.
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
		t.Fatal("the loop did not exit on cancellation")
	}
	if len(reports) != 1 {
		t.Fatalf("reports = %d, want 1 (the starting pass)", len(reports))
	}
	r := reports[0]
	for _, want := range []string{
		"📊 Load: CPU n/a",           // no delta — the base
		"🧮 CPU top: n/a",                // the process rates are also a delta
		"headless-shell 2.0GiB (2 procs)", // the memory top is full right away
		"💾 RAM: used 5.4 of 7.6 GiB",  // 7973728−2359296 KiB
		"swap 198 MiB",
		"📀 Disk /var/lib/crawler/content: free 44 of 79 GiB (56%)",
		"📬 Queues: q:dns 121 · q:render 116320", // backlogs without deltas
	} {
		if !strings.Contains(r, want) {
			t.Errorf("the report does not contain %q:\n%s", want, r)
		}
	}
}

// TestLoadReportLoop_ProcRestart — a restart of a name's processes between
// passes (T-189, production of Oct 25: postgres 18 → 16 processes — the name's
// tick sum fell): the uint64 subtraction of a smaller sum from a bigger one underflows
// to ~2⁶⁴ and after dividing by perCore prints junk "10¹⁴ cores"
// ("postgres 310613997334638.2 cores"). The non-monotonic name is skipped for the
// window — an honest absence, not zero and not junk; the other names are correct,
// the memory top of the restarted name is alive (the name does not vanish from the report).
func TestLoadReportLoop_ProcRestart(t *testing.T) {
	// A window of 40,000 ticks on 4 cores → perCore 10,000. The base: postgres
	// 100,000 ticks; the second pass: postgres restarted (the sum fell
	// to 1,000); headless-shell and render-worker grow monotonically.
	base := LoadStat{
		Load1: 4, Load5: 4, Load15: 4,
		CPU:      CPUStat{Total: 1_000_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{
			"postgres":       {Ticks: 100_000, RSS: 2 << 30, Count: 18},
			"headless-shell": {Ticks: 50_000, RSS: 1 << 30, Count: 2},
			"render-worker":  {Ticks: 20_000},
		},
	}
	cur := LoadStat{
		Load1: 4, Load5: 4, Load15: 4,
		CPU:      CPUStat{Total: 1_040_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{
			"postgres":       {Ticks: 1_000, RSS: 2 << 30, Count: 16},
			"headless-shell": {Ticks: 60_000, RSS: 1 << 30, Count: 2},
			"render-worker":  {Ticks: 21_000},
			// A new name of the window: it was not in the base — the full tick sum
			// is the delta (the ticks started inside the window): 5,000/10,000.
			"cron": {Ticks: 5_000},
		},
	}
	src := &fakeLoadSource{states: []LoadStat{base, cur}}
	reports := make(chan string, 8) // a buffer: the tail passes before the cancellation
	opts := LoadReportOptions{
		Interval: 20 * time.Millisecond,
		Notify:   func(text string) { reports <- text },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, src, opts) }()

	recv := func(step string) string {
		select {
		case r := <-reports:
			return r
		case <-time.After(5 * time.Second):
			t.Fatalf("the pass %s: the report did not arrive", step)
		}
		return ""
	}
	if r := recv("base"); !strings.Contains(r, "🧮 CPU top: n/a") {
		t.Errorf("the base: the CPU top is not \"n/a\" (no delta):\n%s", r)
	}
	r := recv("window")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the loop: %v", err)
	}

	// The restarted name skipped, the others correct: headless-shell
	// 10,000/10,000 = 1.0 (cores); the new cron name — 5,000/10,000 = 0.5
	// (the full sum: it was not in the base); render-worker 1,000/10,000 = 0.1.
	if want := "🧮 CPU top: headless-shell 1.0c, cron 0.5c, render-worker 0.1c"; !strings.Contains(r, want) {
		t.Errorf("the CPU top = %q, want %q:\n%s", topCPULine(r), want, r)
	}
	// The memory top of the name is alive — the restart cures only the CPU delta.
	if want := "postgres 2.0GiB (16 procs)"; !strings.Contains(r, want) {
		t.Errorf("postgres vanished from the memory top (it must stay):\n%s", r)
	}

	// No junk values: a name's share is bounded by the cores from above (the tick
	// sum of all names ≤ the system ticks); everything above is an underflow; the
	// core × 8 threshold — headroom for counting anomalies; the junk symptom is ~10¹⁴.
	for _, item := range strings.Split(strings.TrimPrefix(topCPULine(r), "🧮 CPU top: "), ", ") {
		name, v, ok := strings.Cut(item, " ")
		if !ok {
			t.Fatalf("the top line does not parse: %q", item)
		}
		share, err := strconv.ParseFloat(strings.TrimSuffix(v, "c"), 64)
		if err != nil {
			t.Fatalf("the top share %q is not a number: %v", v, err)
		}
		if name == "postgres" {
			t.Errorf("a restarted name in the top: %s %.1fc:\n%s", name, share, r)
		}
		if share > 4*8 {
			t.Errorf("a junk top share (an underflow): %s %.1fc:\n%s", name, share, r)
		}
	}
}

// topCPULine — the "CPU top" line of the report (empty — none).
func topCPULine(report string) string {
	for _, line := range strings.Split(report, "\n") {
		if strings.HasPrefix(line, "🧮 CPU top: ") {
			return line
		}
	}
	return ""
}

func TestFormatLoadReport_Deltas(t *testing.T) {
	st, err := OSLoad{Root: loadFixtures(t)}.Load(context.Background())
	if err != nil {
		t.Fatalf("the snapshot: %v", err)
	}
	busy, io := 0.975, 0.0 // the user/sys shares are not printed by the report — only busy/io
	procCPU := map[string]float64{
		"postgres":       1.9,
		"headless-shell": 1.15,
		"http-worker":    0.5,
		"analyzer":       0.31,
		"unbound":        0.01, // noise: below the threshold
	}
	// The Len of the fixtures — backlogs (T-188): a delta of ±N — a backlog growth/drain
	// (−1200 — the q:render drain); a rate of N/s — deliveries.
	queues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 121, Read: 2100},
		queue.StreamRender: {Len: 115120, Read: 1200},
	}
	rates := map[string]float64{
		queue.StreamDNS:    3.5,
		queue.StreamRender: 2.0,
	}
	prevQueues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 121, Read: 0},
		queue.StreamRender: {Len: 116320, Read: 0},
	}

	debt := RenderDebt{Flags: 443000, Lag: 99120}
	r := formatLoadReport(st, &busy, &io, procCPU, queues, rates, true, prevQueues, nil, &debt)
	for _, want := range []string{
		"CPU 98% (io 0%)",
		"PSI 92% [5m 90%]",
		"load 24.8/22.0/20.7 of 4 cores",
		"CPU top: postgres 1.9c, headless-shell 1.1c, http-worker 0.5c, analyzer 0.3c",
		"Memory top: headless-shell 2.0GiB (2 procs)",
		"PSI mem 8% / full 2%",
		"q:dns 121 (±0, 3.5/s)",
		"q:render 115120 (−1200, 2.0/s)",
		"📬 Render debt: flags 443000 + lag 99120 = 542120",
	} {
		if !strings.Contains(r, want) {
			t.Errorf("the report does not contain %q:\n%s", want, r)
		}
	}
	if strings.Contains(r, "unbound") {
		t.Errorf("CPU top noise (unbound 0.01c) got into the report:\n%s", r)
	}
}

func TestFormatLoadReport_Degraded(t *testing.T) {
	// No queues and no disk — "n/a" sections; PSI missing — the pressure
	// lines are simply shorter.
	st := LoadStat{
		Load1: 1.0, Load5: 1.0, Load15: 1.0,
		CPU:      CPUStat{Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {RSS: 1 << 30}},
	}
	r := formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, nil, nil)
	for _, want := range []string{"CPU n/a", "CPU top: n/a", "📬 Queues: n/a", "📀 Disk: n/a", "📬 Render debt: n/a"} {
		if !strings.Contains(r, want) {
			t.Errorf("the degraded report does not contain %q:\n%s", want, r)
		}
	}
	if strings.Contains(r, "PSI") {
		t.Errorf("PSI got into the report without data:\n%s", r)
	}

	// The debt source is independent of the queues (the T-175 review, P3-1): with
	// unavailable queues the debt line is printed anyway.
	debt := RenderDebt{Flags: 443000, Lag: 99120}
	r = formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, nil, &debt)
	if !strings.Contains(r, "📬 Render debt: flags 443000 + lag 99120 = 542120") {
		t.Errorf("the debt was not printed with n/a queues:\n%s", r)
	}
}

func TestParseProcStatLine(t *testing.T) {
	// A name with parentheses and spaces; utime/stime — the 12/13 fields after the name.
	line := "203 (postgres: 1[2] writer) S 1 0 0 0 -1 0 0 0 0 0 500 500 0 0 20 0"
	if comm, ticks := parseProcStatLine([]byte(line)); comm != "postgres: 1[2] writer" || ticks != 1000 {
		t.Errorf("parseProcStatLine = %q %d", comm, ticks)
	}
	if comm, _ := parseProcStatLine([]byte("1 (short) R 1 2 3")); comm != "" {
		t.Errorf("a short line = %q, want an empty contribution", comm)
	}
}

// TestRedisQueues_NoClient — a nil client: a source error.
func TestRedisQueues_NoClient(t *testing.T) {
	if _, err := (RedisQueues{}).Queues(context.Background()); err == nil {
		t.Error("a nil client: want a source error")
	}
}

// TestRedisQueues_Backlog — the depth of a report stream — the backlog of the
// consumer groups (T-188): the history in XLEN does not overstate the depth
// (production of Oct 25: q:render XLEN 136,500 at a backlog of 27,529); several
// groups — the sum of the contributions (queue.GroupDepth); lag −1 — pending only; a stream
// without groups and the q:dead quarantine — XLEN; Read — the maximum of the groups' entries-read.
// The fake — fakeGroupsRedis (streamgroups_test.go), the same interface.
func TestRedisQueues_Backlog(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			// Production of Oct 25 (spec §2): XLEN 136,500 — 17h of untrimmed
			// history; the group backlog min(27409, 136500) + 120 = 27,529.
			queue.StreamRender: {{
				Name: "render-worker", Lag: 27409, Pending: 120, EntriesRead: 1097613,
			}},
			// Two groups: min(98, 50)+2 = 52 and 10+1 = 11 → 63; Read —
			// the maximum over the groups.
			queue.StreamHTTP: {
				{Name: "http-worker", Lag: 98, Pending: 2, EntriesRead: 500},
				{Name: "retry", Lag: 10, Pending: 1, EntriesRead: 700},
			},
			// lag −1 (a trim of undelivered, T-168) — pending only.
			queue.StreamAnalyze: {{
				Name: "analyzer", Lag: -1, Pending: 1, EntriesRead: 8_000_000,
			}},
			// The stream exists, no groups — nobody delivered: XLEN.
			queue.StreamDNS: {},
			// q:render-fallback — the worker B group: min(40, 300)+1 = 41
			// (S-P3-1: the stream in the report; the backlog is computed by the shared formula).
			queue.StreamRenderFallback: {{
				Name: "render-fallback-worker", Lag: 40, Pending: 1, EntriesRead: 77,
			}},
			// q:dead — always XLEN: the quarantine depth is the
			// operator's inventory, not a backlog (it has no groups by construction).
			queue.StreamDead: {{Name: "triage", Lag: 100, Pending: 5, EntriesRead: 30}},
		},
		xlen: map[string]int64{
			queue.StreamRender:         136500,
			queue.StreamHTTP:           50,
			queue.StreamAnalyze:        71784,
			queue.StreamDNS:            2,
			queue.StreamRenderFallback: 300,
			queue.StreamDead:           1441,
		},
	}
	q, err := RedisQueues{RDB: rdb}.Queues(context.Background())
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	want := map[string]QueueStat{
		queue.StreamDNS:            {Len: 2},
		queue.StreamHTTP:           {Len: 63, Read: 700},
		queue.StreamRender:         {Len: 27529, Read: 1097613},
		queue.StreamAnalyze:        {Len: 1, Read: 8_000_000},
		queue.StreamRenderFallback: {Len: 41, Read: 77},
		queue.StreamDead:           {Len: 1441, Read: 30},
	}
	if len(q) != len(want) {
		t.Fatalf("streams = %d, want %d: %+v", len(q), len(want), q)
	}
	for s, w := range want {
		if q[s] != w {
			t.Errorf("%s = %+v, want %+v", s, q[s], w)
		}
	}

	// No streams at all (a queue empty from birth) — {0, 0} without an error.
	rdb2 := &fakeGroupsRedis{groups: map[string][]redis.XInfoGroup{}, xlen: map[string]int64{}}
	q2, err := RedisQueues{RDB: rdb2}.Queues(context.Background())
	if err != nil {
		t.Fatalf("Queues (empty): %v", err)
	}
	for _, s := range reportStreams {
		if q2[s] != (QueueStat{}) {
			t.Errorf("%s = %+v, want {0 0}", s, q2[s])
		}
	}
}

// TestRedisQueues_GroupsError — an XINFO GROUPS error (not "no key")
// drops the section with the stream name: a backlog without group states is
// not counted; a raw XLEN instead of it would overstate the depth with history (T-188).
// "No key" from XLEN — a {0, 0} contribution; the section is alive.
func TestRedisQueues_GroupsError(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups:    map[string][]redis.XInfoGroup{},
		xlen:      map[string]int64{queue.StreamDNS: 5},
		groupErrs: map[string]error{queue.StreamDNS: errors.New("connection refused")},
	}
	if _, err := (RedisQueues{RDB: rdb}).Queues(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "q:dns") {
		t.Errorf("the groups error = %v, want an error with the q:dns name", err)
	}

	rdb2 := &fakeGroupsRedis{
		groups:   map[string][]redis.XInfoGroup{},
		xlen:     map[string]int64{},
		lensErrs: map[string]error{queue.StreamDNS: redis.Nil},
	}
	q, err := RedisQueues{RDB: rdb2}.Queues(context.Background())
	if err != nil {
		t.Fatalf("Queues (no XLEN key): %v", err)
	}
	if q[queue.StreamDNS] != (QueueStat{}) {
		t.Errorf("q:dns = %+v, want {0 0}", q[queue.StreamDNS])
	}
}

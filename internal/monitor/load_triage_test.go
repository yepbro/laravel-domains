// The mutant triage tests of the CI run of Nov 09, 2026 (half A, score 0.758):
// the finishing of the load.go coverage — exact /proc parser values (all
// CPU fields non-zero — the Total arithmetic observable), condition
// boundaries (len(fields), n < 0, indexes), PSI degradations per file, source
// errors with zero values, the observe metrics (gauges ×100),
// the exact report text, the fmtMem/round/top boundaries, the queue source
// on the "no such key" text error, the rate calculator and the disabled
// loop interval. Live /proc and Redis — outside the unit tests (hermeticity).
package monitor

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// fullCPUFixtures — a fixture directory where ALL eight fields of the
// aggregated cpu line are non-zero and distinguishable: the arithmetic of st.Total and the
// index carries of c[i] are observable (the zero fields of the loadFixtures fixtures hid
// the sign and index mutants).
func fullCPUFixtures(t *testing.T) string {
	t.Helper()
	root := loadFixtures(t)
	writeFix(t, root, "loadavg", "1.5 2.5 3.5 1/100 500\n")
	writeFix(t, root, "stat",
		"cpu  11 22 33 44 55 66 77 88 99 100\n"+ // 10 fields: the guest tail must be ignored
			"cpu0 11 22 33 44 55 66 77 88\n"+
			"cpu1 11 22 33 44 55 66 77 88\n")
	writeFix(t, root, "meminfo",
		"MemTotal:        16777216 kB\n"+
			"MemAvailable:     8388608 kB\n"+
			"SwapTotal:        2097152 kB\n"+
			"SwapFree:         1048576 kB\n")
	return root
}

// TestReadCPUStat_AllFieldsNonZero — Total is the sum of all eight fields
// (the guest tail outside the sum); every field is read by its position.
func TestReadCPUStat_AllFieldsNonZero(t *testing.T) {
	st, err := readCPUStat(fullCPUFixtures(t) + "/stat")
	if err != nil {
		t.Fatalf("readCPUStat: %v", err)
	}
	if st.User != 11 || st.Nice != 22 || st.System != 33 || st.Idle != 44 ||
		st.Iowait != 55 || st.Irq != 66 || st.Softirq != 77 || st.Steal != 88 {
		t.Errorf("поля cpu = %+v, want 11/22/33/44/55/66/77/88", st)
	}
	if want := uint64(11 + 22 + 33 + 44 + 55 + 66 + 77 + 88); st.Total != want {
		t.Errorf("Total = %d, want %d (сумма всех полей)", st.Total, want)
	}
	if st.Cores != 2 {
		t.Errorf("Cores = %d, want 2", st.Cores)
	}
}

// TestReadCPUStat_MissingAggLine — cpuN lines without the aggregated "cpu ":
// a Total of 0 — an error (a first-disjunct-weakening mutant would stay silent).
func TestReadCPUStat_MissingAggLine(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "stat", "cpu0 11 22 33 44 55 66 77 88\n")
	if _, err := readCPUStat(root + "/stat"); err == nil {
		t.Error("нет строки cpu: хочу ошибку (Total 0)")
	}
	// An aggregated line without cpuN lines — also an error (Cores 0).
	writeFix(t, root, "stat", "cpu  11 22 33 44 55 66 77 88\n")
	if _, err := readCPUStat(root + "/stat"); err == nil {
		t.Error("нет строк cpuN: хочу ошибку (Cores 0)")
	}
}

// TestReadCPUStat_BrokenLines — a short cpu line and a non-numeric tick.
func TestReadCPUStat_BrokenLines(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "stat", "cpu  11 22 33\n"+"cpu0 1 1 1 1 1 1 1 1\n")
	if _, err := readCPUStat(root + "/stat"); err == nil {
		t.Error("строка cpu короче пяти полей: хочу ошибку")
	}
	writeFix(t, root, "stat", "cpu  11 22 x 44 55 66 77 88\n"+"cpu0 1 1 1 1 1 1 1 1\n")
	if _, err := readCPUStat(root + "/stat"); err == nil {
		t.Error("нечисловой тик: хочу ошибку")
	}
	// A tick of 2^63 is legal for uint64 (a bitSize 63 mutant must accept it).
	writeFix(t, root, "stat", "cpu  9223372036854775808 22 33 44 55 66 77 88\n"+"cpu0 1 1 1 1 1 1 1 1\n")
	if _, err := readCPUStat(root + "/stat"); err != nil {
		t.Errorf("тик 2^63: %v (uint64 принимает)", err)
	}
	// An unavailable file — an error with a zero structure (the zeros pin).
	st, err := readCPUStat(root + "/нет")
	if err == nil || st != (CPUStat{}) {
		t.Errorf("нет файла: (%+v, %v), want нули и ошибка", st, err)
	}
}

// TestReadLoadAvg_Boundaries — exactly three fields are valid (the < 3 boundary),
// two — an error; a zero load is valid, a negative one — an error;
// the values on error — zeros (a pin).
func TestReadLoadAvg_Boundaries(t *testing.T) {
	dir := t.TempDir()
	file := dir + "/loadavg"
	writeFix(t, dir, "loadavg", "0 0.5 1\n")
	if l1, l5, l15, err := readLoadAvg(file); err != nil || l1 != 0 || l5 != 0.5 || l15 != 1 {
		t.Errorf("ровно три поля/нули: (%v/%v/%v, %v)", l1, l5, l15, err)
	}
	writeFix(t, dir, "loadavg", "1 2\n")
	if l1, l5, l15, err := readLoadAvg(file); err == nil || l1 != 0 || l5 != 0 || l15 != 0 {
		t.Errorf("два поля: (%v/%v/%v, %v), want ошибка и нули", l1, l5, l15, err)
	}
	writeFix(t, dir, "loadavg", "1 2 3 4\n")
	if _, _, _, err := readLoadAvg(file); err != nil {
		t.Errorf("четыре поля: %v (лишние не мешают)", err)
	}
	writeFix(t, dir, "loadavg", "-1 2 3\n")
	if l1, _, _, err := readLoadAvg(file); err == nil || l1 != 0 {
		t.Errorf("отрицательная загрузка: (%v, %v), want ошибка и нули", l1, err)
	}
	writeFix(t, dir, "loadavg", "x 2 3\n")
	if l1, l5, l15, err := readLoadAvg(file); err == nil || l1 != 0 || l5 != 0 || l15 != 0 {
		t.Errorf("нечисловая загрузка: (%v/%v/%v, %v), want ошибка и нули", l1, l5, l15, err)
	}
	if _, _, _, err := readLoadAvg(dir + "/нет"); err == nil {
		t.Error("нет файла: хочу ошибку")
	}
}

// TestReadMemInfo_Boundaries — a corrupt value and an empty value field —
// errors; a missing swap — zeros without an error; on error — zeros.
func TestReadMemInfo_Boundaries(t *testing.T) {
	dir := t.TempDir()
	writeFix(t, dir, "meminfo", "MemTotal: 100 kB\nMemAvailable: x kB\n")
	if tt, a, st, sf, err := readMemInfo(dir + "/meminfo"); err == nil ||
		tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("нечисловое значение: (%v/%v/%v/%v, %v), want ошибка и нули", tt, a, st, sf, err)
	}
	writeFix(t, dir, "meminfo", "MemTotal: 100 kB\nMemAvailable:\n")
	if _, _, _, _, err := readMemInfo(dir + "/meminfo"); err == nil {
		t.Error("пустое значение: хочу ошибку (поле без числа)")
	}
	// kB → bytes exactly; without swap — zeros.
	writeFix(t, dir, "meminfo", "MemTotal: 1 kB\nMemAvailable: 1 kB\n")
	if tt, a, st, sf, err := readMemInfo(dir + "/meminfo"); err != nil ||
		tt != 1024 || a != 1024 || st != 0 || sf != 0 {
		t.Errorf("без свопа: (%v/%v/%v/%v, %v), want 1024/1024/0/0", tt, a, st, sf, err)
	}
}

// TestOSLoad_PSIDegradations — PSI is present when at least one pressure
// file is available (each PSIPresent assignment is observable).
func TestOSLoad_PSIDegradations(t *testing.T) {
	root := fullCPUFixtures(t)
	// Only cpu pressure: the memory file removed.
	if err := removeFix(root, "pressure/memory"); err != nil {
		t.Fatal(err)
	}
	st, err := OSLoad{Root: root}.Load(context.Background())
	if err != nil {
		t.Fatalf("только pressure/cpu: %v", err)
	}
	if !st.PSIPresent || st.CPUPSI.Avg10 != 91.59 {
		t.Errorf("PSI cpu = present=%v %+v, want present и 91.59", st.PSIPresent, st.CPUPSI)
	}
	// Only memory: the cpu file removed.
	root2 := fullCPUFixtures(t)
	if err := removeFix(root2, "pressure/cpu"); err != nil {
		t.Fatal(err)
	}
	st, err = OSLoad{Root: root2}.Load(context.Background())
	if err != nil {
		t.Fatalf("только pressure/memory: %v", err)
	}
	if !st.PSIPresent || st.MemPSISome.Avg10 != 7.85 {
		t.Errorf("PSI mem = present=%v some=%+v, want present и 7.85", st.PSIPresent, st.MemPSISome)
	}
	// A corrupt cpu file — a pass error (the values do not stay silent).
	root3 := fullCPUFixtures(t)
	writeFix(t, root3, "pressure/cpu", "some avg10=999 avg60=1 avg300=1 total=1\n")
	if _, err := (OSLoad{Root: root3}).Load(context.Background()); err == nil {
		t.Error("кривой pressure/cpu: хочу ошибку")
	}
	root4 := fullCPUFixtures(t)
	writeFix(t, root4, "pressure/memory", "some avg10=x\n")
	if _, err := (OSLoad{Root: root4}).Load(context.Background()); err == nil {
		t.Error("кривой pressure/memory: хочу ошибку")
	}
}

// removeFix — the deletion of a fixture file (the PSI degradations).
func removeFix(root, name string) error {
	return os.Remove(root + "/" + name)
}

// TestOSLoad_MissingSources — the mandatory sources: a missing
// loadavg/stat/meminfo and a process failure drop the pass (return-cut
// mutants would swallow the error and reach success).
func TestOSLoad_MissingSources(t *testing.T) {
	for _, miss := range []string{"loadavg", "stat", "meminfo"} {
		root := fullCPUFixtures(t)
		if err := removeFix(root, miss); err != nil {
			t.Fatal(err)
		}
		if _, err := (OSLoad{Root: root}).Load(context.Background()); err == nil {
			t.Errorf("нет %s: хочу ошибку прохода", miss)
		}
	}
}

// TestReadProcs_ContinueBoundaries — a non-PID directory and a corrupt stat between
// valid PIDs: continue skips only the failing contribution; break must
// not cut the walk (the subsequent PIDs are aggregated). Non-PID directories
// with their own stat file (".cfg", "zzz") do not enter the aggregate.
func TestReadProcs_ContinueBoundaries(t *testing.T) {
	root := fullCPUFixtures(t)
	// A non-PID directory in the middle + a PID without a stat file + a PID with a corrupt stat,
	// then two valid ones: all valid ones must enter the aggregate.
	mkdirFixRaw(t, root, "zcfg")
	mkdirFixRaw(t, root, "300") // an empty PID directory (no stat)
	mkdirFix(t, root, "301", "(broken) no parens here", "1 1\n")
	mkdirFix(t, root, "302", "(alpha) S 1 0 0 0 -1 0 0 0 0 0 7 3 0 0 20 0 1", "1000 500 200 10 0 400 0\n")
	mkdirFix(t, root, "033", "(beta) S 1 0 0 0 -1 0 0 0 0 0 5 9 0 0 20 0 2", "2000 800 900 5 0 700 0\n")
	// Non-PID directories with their OWN stat (dir[0] outside '0'..'9' on both
	// sides of the boundary): they need not be read at all.
	mkdirFix(t, root, ".cfg", "(ghost) S 1 0 0 0 -1 0 0 0 0 0 1 1 0 0 20 0 3", "1 1\n")
	mkdirFix(t, root, "zzz", "(spook) S 1 0 0 0 -1 0 0 0 0 0 1 1 0 0 20 0 4", "1 1\n")
	// PID directories with digit boundaries: '9xx' and '0xx' — read.
	mkdirFix(t, root, "9xx", "(nine) S 1 0 0 0 -1 0 0 0 0 0 2 2 0 0 20 0 5", "1 1\n")
	procs, err := readProcs(root)
	if err != nil {
		t.Fatalf("readProcs: %v", err)
	}
	if got := procs["alpha"]; got.Ticks != 10 || got.Count != 1 {
		t.Errorf("alpha = %+v, want тики 10 (7+3)", got)
	}
	if got := procs["beta"]; got.Ticks != 14 || got.Count != 1 {
		t.Errorf("beta = %+v, want тики 14 (5+9) — PID с ведущим нулём читается", got)
	}
	if got := procs["nine"]; got.Ticks != 4 {
		t.Errorf("nine = %+v, want тики 4 (PID-каталог на границе '9')", got)
	}
	for _, ghost := range []string{"broken", "zcfg", "ghost", "spook"} {
		if _, ok := procs[ghost]; ok {
			t.Errorf("не-PID/кривой вклад %q попал в агрегат", ghost)
		}
	}
}

// mkdirFixRaw — an empty fixture directory (non-PID / a PID without stat).
func mkdirFixRaw(t *testing.T, root, name string) {
	t.Helper()
	if err := os.Mkdir(root+"/"+name, 0o755); err != nil {
		t.Fatal(err)
	}
}

// TestParseProcStatLine_Boundaries — exactly 13 fields after the name,
// the name at the start of the line, close == open, an empty name — an empty contribution.
func TestParseProcStatLine_Boundaries(t *testing.T) {
	// Exactly 13 fields: state + 12 (utime — the 12th, stime — the 13th) — valid.
	line := "7 (ok) R 1 2 3 4 5 6 7 8 9 10 3 4"
	if comm, ticks := parseProcStatLine([]byte(line)); comm != "ok" || ticks != 7 {
		t.Errorf("13 полей = %q %d, want ok и 7 (3+4)", comm, ticks)
	}
	// 12 fields — short.
	if comm, ticks := parseProcStatLine([]byte("7 (short) R 1 2 3 4 5 6 7 8 9 10 3")); comm != "" || ticks != 0 {
		t.Errorf("12 полей = %q %d, want пустой вклад", comm, ticks)
	}
	// Counter-oriented parentheses (")(") — an empty contribution.
	if comm, _ := parseProcStatLine([]byte("1 )( R 1 2 3 4 5 6 7 8 9 10 3 4")); comm != "" {
		t.Errorf("встречные скобки = %q, want пустой вклад", comm)
	}
	// An empty name "()" — an empty contribution (and zero ticks).
	if comm, ticks := parseProcStatLine([]byte("1 () R 1 2 3 4 5 6 7 8 9 10 3 4")); comm != "" || ticks != 0 {
		t.Errorf("пустое имя = (%q, %d), want пустой вклад с нулём", comm, ticks)
	}
	// A short/corrupt line — zero ticks (the value pin on failure).
	for _, bad := range []string{
		"7 (short) R 1 2 3 4 5 6 7 8 9 10 3",
		"1 )( R 1 2 3 4 5 6 7 8 9 10 3 4",
		"1 (x) R 1 2 3 4 5 6 7 8 9 10 p q",
	} {
		if _, ticks := parseProcStatLine([]byte(bad)); ticks != 0 {
			t.Errorf("%q: тики = %d, want 0", bad, ticks)
		}
	}
	// utime of 2^63 — valid for uint64 (a bitSize 63 mutant must accept it).
	if comm, ticks := parseProcStatLine([]byte(
		"9 (huge) R 1 2 3 4 5 6 7 8 9 10 9223372036854775808 5")); comm != "huge" || ticks != 9223372036854775813 {
		t.Errorf("utime 2^63 = (%q, %d), want huge и 2^63+5", comm, ticks)
	}
	// Non-numeric utime/stime — an empty contribution.
	if comm, _ := parseProcStatLine([]byte("1 (x) R 1 2 3 4 5 6 7 8 9 10 p q")); comm != "" {
		t.Errorf("нечисловые тики = %q, want пустой вклад", comm)
	}
}

// TestParseStatmRSS_Boundaries — resident is the SECOND field (not the first),
// one field — zero; a value of 2^63 is read.
func TestParseStatmRSS_Boundaries(t *testing.T) {
	if got := parseStatmRSS([]byte("100 900 1 1 1 1 1\n")); got != 900 {
		t.Errorf("resident = %d, want 900 (второе поле)", got)
	}
	if got := parseStatmRSS([]byte("100\n")); got != 0 {
		t.Errorf("одно поле = %d, want 0", got)
	}
	if got := parseStatmRSS([]byte("x y\n")); got != 0 {
		t.Errorf("нечисловое = %d, want 0", got)
	}
	if got := parseStatmRSS([]byte("5 9223372036854775808\n")); got != 9223372036854775808 {
		t.Errorf("2^63 = %d, want как есть (uint64)", got)
	}
}

// TestCPUDelta_IowaitAndClamp — a non-zero iowait delta (the busy formula),
// a zero prev.Total + a unit delta and a negative busy (the clamp of 0).
func TestCPUDelta_IowaitAndClamp(t *testing.T) {
	prev := CPUStat{User: 1000, System: 500, Idle: 500, Iowait: 100, Total: 2100, Cores: 4}
	cur := CPUStat{User: 1200, System: 700, Idle: 600, Iowait: 300, Total: 2800, Cores: 4}
	busy, user, sys, io, ok := cpuDelta(prev, cur)
	if !ok {
		t.Fatal("дельта не посчитана")
	}
	// dTotal=700: user 200/700, sys 200/700, io 200/700, idle 100/700 →
	// busy = 1 − 100/700 − 200/700 = 400/700 (a comparison with a tolerance —
	// because of the operation order).
	const eps = 1e-12
	if diff(busy, 400.0/700) > eps || diff(user, 200.0/700) > eps ||
		diff(sys, 200.0/700) > eps || diff(io, 200.0/700) > eps {
		t.Errorf("delta = busy %v user %v sys %v io %v", busy, user, sys, io)
	}
	// prev.Total = 1, d = 1 — a delta is possible (the zero boundaries).
	_, _, _, _, ok = cpuDelta(CPUStat{Total: 1}, CPUStat{Total: 2})
	if !ok {
		t.Error("prev.Total=1, d=1: хочу ok (нулевые границы — только точные нули)")
	}
	// A negative busy (the idle delta above the total) — clamped to zero.
	busy, _, _, _, ok = cpuDelta(CPUStat{Total: 100}, CPUStat{Total: 200, Idle: 300})
	if !ok {
		t.Fatal("кламп: дельта посчитана")
	}
	if busy != 0 {
		t.Errorf("отрицательный busy не прижат: %v, want 0", busy)
	}
	// No delta — zero values (the zeros pin at "not measured").
	if b, u2, s2, i2, ok := cpuDelta(cur, cur); ok || b != 0 || u2 != 0 || s2 != 0 || i2 != 0 {
		t.Errorf("нулевая дельта = (%v/%v/%v/%v, %v), want нули", b, u2, s2, i2, ok)
	}
	if b, u2, s2, i2, ok := cpuDelta(CPUStat{}, cur); ok || b != 0 || u2 != 0 || s2 != 0 || i2 != 0 {
		t.Errorf("первый проход = (%v/%v/%v/%v, %v), want нули", b, u2, s2, i2, ok)
	}
}

// diff — |a − b| for the tolerances of fraction comparisons.
func diff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}

// TestLoadMetricsObserve — all gauges get exact values:
// loadavg ×3, cpu ×4 (shares ×100), PSI ×4, memory ×3 (used = total −
// available), swap ×2 (used = total − free).
func TestLoadMetricsObserve(t *testing.T) {
	reg := metrics.New("monitor")
	m := NewLoadMetrics(reg)
	st := LoadStat{
		Load1: 1.5, Load5: 2.5, Load15: 3.5,
		PSIPresent: true,
		CPUPSI:     PSIStat{Avg10: 10, Avg60: 20, Avg300: 30},
		MemPSISome: PSIStat{Avg10: 4, Avg60: 5, Avg300: 6},
		MemPSIFull: PSIStat{Avg10: 7, Avg60: 8, Avg300: 9},
		MemTotal:   8 << 30, MemAvailable: 6 << 30,
		SwapTotal: 2 << 30, SwapFree: 1 << 30,
	}
	busy, user, sys, io := 0.37, 0.25, 0.11, 0.01
	m.observe(st, &busy, &user, &sys, &io)

	for kind, want := range map[string]float64{
		"1m": 1.5, "5m": 2.5, "15m": 3.5,
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_loadavg", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("loadavg{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
	for kind, want := range map[string]float64{
		"busy": 37, "user": 25, "system": 11, "iowait": 1,
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_cpu_percent", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("cpu{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
	for kind, want := range map[string]float64{
		"some_avg10": 4, "some_avg300": 6, "full_avg10": 7, "full_avg300": 9,
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_mem_psi_percent", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("mem_psi{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
	for kind, want := range map[string]float64{
		"total": float64(8 << 30), "available": float64(6 << 30), "used": float64(2 << 30),
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_mem_bytes", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("mem{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
	for kind, want := range map[string]float64{
		"total": float64(2 << 30), "used": float64(1 << 30),
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_swap_bytes", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("swap{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
}

// TestLoadConsts — the pins of the report constants (the period, the top sizes, the noise
// threshold) and the interval default.
func TestLoadConsts(t *testing.T) {
	if DefaultLoadReportInterval != 10*time.Minute {
		t.Errorf("DefaultLoadReportInterval = %v, want 10m", DefaultLoadReportInterval)
	}
	if topProcs != 4 {
		t.Errorf("topProcs = %d, want 4", topProcs)
	}
	if minProcCPUShare != 0.05 {
		t.Errorf("minProcCPUShare = %v, want 0.05", minProcCPUShare)
	}
}

// TestFormatLoadReport_Exact — the full report text on a compact snapshot:
// any format edit (prefixes, separators, disk percentages, queue deltas
// of ±N and the rate) is observable by a whole-string comparison.
func TestFormatLoadReport_Exact(t *testing.T) {
	st := LoadStat{
		Load1: 24.82, Load5: 21.96, Load15: 20.67,
		CPU:        CPUStat{Cores: 4},
		PSIPresent: true,
		CPUPSI:     PSIStat{Avg10: 91.59, Avg60: 90.37},
		MemPSISome: PSIStat{Avg10: 7.85},
		MemPSIFull: PSIStat{Avg10: 2},
		MemTotal:   7973728 << 10, MemAvailable: 2359296 << 10,
		SwapTotal: 3932160 << 10, SwapFree: 3729408 << 10,
		Procs: map[string]ProcAgg{
			"postgres":       {RSS: 2 << 30, Count: 3},
			"headless-shell": {RSS: 1 << 30, Count: 2},
		},
	}
	// busy/io are chosen sensitive to the multiplier mutations (×99/×101
	// change the percentage rounding).
	busy, io := 0.997, 0.996
	procCPU := map[string]float64{"postgres": 1.4, "headless-shell": 0.05}
	queues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 121, Read: 2100},
		queue.StreamRender: {Len: 116319, Read: 1200},
	}
	rates := map[string]float64{queue.StreamDNS: 3.5, queue.StreamRender: 0.5}
	prevQueues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 120, Read: 0},
		queue.StreamRender: {Len: 116320, Read: 1100},
	}
	disks := []DiskLine{{Dir: "/var/lib/crawler/content", Avail: 44 << 30, Total: 80 << 30}}
	got := formatLoadReport(st, &busy, &io, procCPU, queues, rates, true, prevQueues, disks, nil)
	want := "📊 Нагрузка: CPU 100% (io 100%), PSI 92% [5м 90%]; load 24.8/22.0/20.7 из 4 ядер\n" +
		"🧮 Топ CPU: postgres 1.4я, headless-shell 0.1я\n" +
		"🧮 Топ память: postgres 2.0ГиБ (3 пр.), headless-shell 1.0ГиБ (2 пр.)\n" +
		"💾 RAM: занято 5.4 из 7.6 ГиБ (доступно 2.2), swap 198 МиБ; PSI mem 8% / full 2%\n" +
		"📀 Диск /var/lib/crawler/content: свободно 44 из 80 ГиБ (55%)\n" +
		"📬 Очереди: q:dns 121 (+1, 3.5/с) · q:render 116319 (−1, 0.5/с)\n" +
		"📬 Рендер-долг: н/д"
	if got != want {
		t.Errorf("отчёт =\n%s\nwant:\n%s", got, want)
	}
}

// TestFormatLoadReport_EdgeSections — the section boundaries: a rate of 0 and < 1 are
// not printed; a delta of ±0; a disk with Total 0 (a percentage without division by zero);
// a memory top of only zero RSS — "n/a"; prevQueues without
// prevStarted — without deltas; exactly topProcs lines and the noise threshold by
// value (0.05 passes, below — no).
func TestFormatLoadReport_EdgeSections(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1,
		CPU:      CPUStat{Cores: 2},
		MemTotal: 4 << 30, MemAvailable: 2 << 30,
		Procs: map[string]ProcAgg{"ghost": {Count: 2}}, // RSS 0 — "n/a"
	}
	queues := map[string]QueueStat{queue.StreamDNS: {Len: 5, Read: 10}}
	// Rates: zero (not printed), a fractional < 1 is printed — with a delta
	// of ±0 (prevStarted=true, the depth did not change).
	rates := map[string]float64{queue.StreamDNS: 0}
	prevQueues := map[string]QueueStat{queue.StreamDNS: {Len: 5}}
	r := formatLoadReport(st, nil, nil, nil, queues, rates, true, prevQueues, nil, nil)
	if !strings.Contains(r, "q:dns 5 (±0)") {
		t.Errorf("дельта ±0 не напечатана:\n%s", r)
	}
	if strings.Contains(r, "/с") {
		t.Errorf("нулевой темп напечатан:\n%s", r)
	}
	// A fractional rate of 0.5 is printed; prevQueues without prevStarted — no deltas
	// (a direct call of the formatter: in the loop the pair always goes together; here
	// we separate them).
	rates[queue.StreamDNS] = 0.5
	r = formatLoadReport(st, nil, nil, nil, queues, rates, true, prevQueues, nil, nil)
	if !strings.Contains(r, "q:dns 5 (±0, 0.5/с)") {
		t.Errorf("дробный темп не напечатан:\n%s", r)
	}
	r = formatLoadReport(st, nil, nil, nil, queues, rates, false, prevQueues, nil, nil)
	if strings.Contains(r, "(±0") || strings.Contains(r, "/с") {
		t.Errorf("дельта/темп без prevStarted напечатаны:\n%s", r)
	}

	// A disk with a zero Total — a percentage of 0 without a panic.
	disks := []DiskLine{{Dir: "/x", Avail: 100, Total: 0}}
	r = formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, disks, nil)
	if !strings.Contains(r, "📀 Диск /x: свободно 0 из 0 ГиБ (0%)") {
		t.Errorf("диск с Total 0 = %q", r)
	}

	// A memory top of zero RSS — "n/a".
	if !strings.Contains(r, "🧮 Топ память: н/д") {
		t.Errorf("топ памяти без RSS не «н/д»:\n%s", r)
	}

	// The CPU top: five names above the threshold — exactly four are
	// printed (topProcs); a name exactly at the 0.05 threshold passes, below — no.
	procCPU := map[string]float64{
		"a": 2, "b": 1.7, "c": 1.3, "d": 0.9, "e": 0.7, "f": 0.05, "g": 0.04,
	}
	r = formatLoadReport(st, nil, nil, procCPU, nil, nil, false, nil, nil, nil)
	if want := "🧮 Топ CPU: a 2.0я, b 1.7я, c 1.3я, d 0.9я"; !strings.Contains(r, want) {
		t.Errorf("топ CPU = %q, want %q (ровно 4 строки)", topCPULine(r), want)
	}
	if strings.Contains(r, "g 0.0я") {
		t.Errorf("шум ниже порога в топе:\n%s", r)
	}
	// The threshold boundary: a name exactly at 0.05 — passes (a "≤" mutant would
	// lose it), below the threshold — no (two names, the top is not cut).
	r = formatLoadReport(st, nil, nil,
		map[string]float64{"f": 0.05, "g": 0.04}, nil, nil, false, nil, nil, nil)
	if !strings.Contains(r, "🧮 Топ CPU: f 0.1я") {
		t.Errorf("имя ровно в пороге 0.05 исчезло:\n%s", r)
	}
	if strings.Contains(topCPULine(r), "g") {
		t.Errorf("шум 0.04 в топе: %q", topCPULine(r))
	}
}

// TestSortTopOrder — equal values: the order by name ascending
// (determinism); different ones — by value descending.
func TestSortTopOrder(t *testing.T) {
	items := topByCPU(map[string]float64{"b": 1, "a": 1, "c": 2})
	if len(items) != 3 || items[0].name != "c" || items[1].name != "a" || items[2].name != "b" {
		t.Errorf("порядок = %+v, want c,a,b (значение ↓, имя ↑)", items)
	}
	// RSS = 1 byte — the name enters the top (the zero boundary).
	items = topByRSS(map[string]ProcAgg{"tiny": {RSS: 1, Count: 1}})
	if len(items) != 1 || items[0].name != "tiny" {
		t.Errorf("RSS 1 байт исчез из топа: %+v", items)
	}
	// Count = 1 — without the process suffix; Count = 2 — with it (the
	// "> 1" boundary is observed by the memory top text).
	st := LoadStat{Procs: map[string]ProcAgg{
		"one": {RSS: 1 << 20, Count: 1},
		"two": {RSS: 2 << 20, Count: 2},
	}, MemTotal: 1, MemAvailable: 1}
	r := formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, nil, nil)
	if !strings.Contains(r, "two 2МиБ (2 пр.)") || strings.Contains(r, "one 1МиБ (1") {
		t.Errorf("аннотация числа процессов: %q", r)
	}
}

// TestFmtMemAndRound — the boundaries of the human-readable sizes and rounding.
func TestFmtMemAndRound(t *testing.T) {
	cases := map[uint64]string{
		0:             "0МиБ",
		1 << 20:       "1МиБ",
		(1 << 30) - 1: "1024МиБ",
		1 << 30:       "1.0ГиБ",
		5<<30 + 1<<29: "5.5ГиБ",
	}
	for b, want := range cases {
		if got := fmtMem(b); got != want {
			t.Errorf("fmtMem(%d) = %q, want %q", b, got, want)
		}
	}
	for v, want := range map[float64]int{0.4: 0, 0.5: 1, 1.6: 2, -0.2: 0, -1.7: 0, 0: 0} {
		if got := round(v); got != want {
			t.Errorf("round(%v) = %d, want %d", v, got, want)
		}
	}
	if gib(2<<30) != 2 || mib(2<<20) != 2 {
		t.Errorf("gib/mib = %v/%v, want 2/2", gib(2<<30), mib(2<<20))
	}
}

// TestRedisQueues_TextNoSuchKey — the "no such key" text error (not
// redis.Nil — a proxy/cluster) at XLEN and XINFO GROUPS is read as a missing
// stream; the section stays alive.
func TestRedisQueues_TextNoSuchKey(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups:    map[string][]redis.XInfoGroup{},
		xlen:      map[string]int64{queue.StreamDNS: 5},
		lensErrs:  map[string]error{queue.StreamDNS: errors.New("MOVED 1234: no such key")},
		groupErrs: map[string]error{queue.StreamDNS: errors.New("MOVED 1234: no such key")},
	}
	q, err := RedisQueues{RDB: rdb}.Queues(context.Background())
	if err != nil {
		t.Fatalf("текстовый no such key: %v (секция обязана жить)", err)
	}
	if q[queue.StreamDNS] != (QueueStat{}) {
		t.Errorf("q:dns = %+v, want {0 0}", q[queue.StreamDNS])
	}
	// A full cut of the XLEN branch: the original swallows "no such key" and
	// lives ({0 0}); a mutant would reach XINFO GROUPS with its full-fledged
	// error and drop the section — silence is required here.
	rdb2 := &fakeGroupsRedis{
		groups:    map[string][]redis.XInfoGroup{},
		xlen:      map[string]int64{queue.StreamDNS: 5},
		lensErrs:  map[string]error{queue.StreamDNS: errors.New("no such key")},
		groupErrs: map[string]error{queue.StreamDNS: errors.New("connection refused")},
	}
	q2, err := (RedisQueues{RDB: rdb2}).Queues(context.Background())
	if err != nil {
		t.Fatalf("ветка «нет ключа» вырезана молча: %v", err)
	}
	if q2[queue.StreamDNS] != (QueueStat{}) {
		t.Errorf("q:dns = %+v, want {0 0}", q2[queue.StreamDNS])
	}
}

// TestRedisQueues_ReadEqualGroups — groups with equal entries-read:
// the maximum is correct at equality too (the > boundary).
func TestRedisQueues_ReadEqualGroups(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			queue.StreamHTTP: {
				{Name: "a", Lag: 0, Pending: 0, EntriesRead: 500},
				{Name: "b", Lag: 0, Pending: 0, EntriesRead: 500},
			},
		},
		xlen: map[string]int64{queue.StreamHTTP: 0},
	}
	q, err := RedisQueues{RDB: rdb}.Queues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := q[queue.StreamHTTP].Read; got != 500 {
		t.Errorf("Read = %d, want 500", got)
	}
}

// TestLoadReportLoop_DisabledAndRates — an interval of 0 turns the loop off without
// snapshots; the delivery rate between passes — the Read delta over the window (not
// a product and not a sum with the past).
func TestLoadReportLoop_DisabledAndRates(t *testing.T) {
	src := &countingLoadSource{}
	if err := LoadReportLoop(context.Background(), src, LoadReportOptions{Interval: 0}); err != nil {
		t.Fatalf("выключенный цикл: %v", err)
	}
	if src.count != 0 {
		t.Errorf("выключенный цикл: снимков = %d, want 0", src.count)
	}

	// The rate: a base snapshot + a window with a delivery delta of 100 at a past
	// Read of 5000 — an "N/s" of the order of hundreds is printed (dividing by a window of ~20 ms;
	// the multiplication/addition mutants give units or hundreds of thousands).
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 1_000_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 5000}},
	}
	st2 := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 1_040_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 14_000}},
	}
	two := &fakeLoadSource{states: []LoadStat{st, st2}}
	queues := []map[string]QueueStat{
		{queue.StreamDNS: {Len: 1, Read: 5000}},
		{queue.StreamDNS: {Len: 2, Read: 5100}},
	}
	fq := &switchQueuesSource{passes: queues}
	reports := make(chan string, 8)
	opts := LoadReportOptions{
		Interval: 20 * time.Millisecond,
		Queues:   fq,
		Notify:   func(text string) { reports <- text },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, two, opts) }()
	<-reports // the base
	var r string
	select {
	case r = <-reports:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("второй отчёт не пришёл")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	line := ""
	for _, l := range strings.Split(r, "\n") {
		if strings.HasPrefix(l, "📬 Очереди: ") {
			line = l
		}
	}
	if !strings.Contains(line, "/с") {
		t.Fatalf("темп не напечатан: %q", line)
	}
	// Extract the "N.N/s" rate from the tail "q:dns 2 (+1, N.N/s)".
	rate := 0.0
	if i := strings.Index(line, "/с"); i >= 0 {
		j := i
		for j > 0 && (line[j-1] >= '0' && line[j-1] <= '9' || line[j-1] == '.') {
			j--
		}
		if v, err := strconv.ParseFloat(line[j:i], 64); err == nil {
			rate = v
		}
	}
	// The window — a 20 ms ticker plus jitter: a correct rate of 100/0.02..0.3 =
	// 333..5000; the mutants: *window ≈ 100*0.02..0.3 = 2..30; +p.Read
	// ≈ 10100/window ≥ 33666. The corridor [150, 20000] separates them with headroom.
	if rate < 150 || rate > 20000 {
		t.Errorf("темп доставок = %v (окно ~20мс, дельта 100), want 150..20000", rate)
	}
	if !strings.Contains(line, "(+1") {
		t.Errorf("дельта бэклога не напечатана: %q", line)
	}
}

// TestLoadReportLoop_TinyWindowTopCPU — a CPU tick window of one unit:
// the process core share is computed anyway (perCore 0.25 at 4 cores →
// a delta of 1 tick = 4 cores — the window scale does not mute the top).
func TestLoadReportLoop_TinyWindowTopCPU(t *testing.T) {
	base := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 100, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 100}},
	}
	cur := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 101, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 101}},
	}
	src := &fakeLoadSource{states: []LoadStat{base, cur}}
	reports := make(chan string, 8)
	opts := LoadReportOptions{
		Interval: 20 * time.Millisecond,
		Notify:   func(text string) { reports <- text },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, src, opts) }()
	<-reports // the base
	var r string
	select {
	case r = <-reports:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("второй отчёт не пришёл")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	// A window of d=1, perCore=0.25: a's share = 1/0.25 = 4.0 cores.
	if want := "🧮 Топ CPU: a 4.0я"; !strings.Contains(r, want) {
		t.Errorf("топ CPU окна в 1 тик = %q, want %q", topCPULine(r), want)
	}
}

// TestLoadReportLoop_SourceErrorNoReport — a system snapshot failure drops
// the pass ENTIRELY: neither a report nor delta bases (a return-cut
// mutant would build a report from a zero snapshot).
func TestLoadReportLoop_SourceErrorNoReport(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 1000, Cores: 1},
		MemTotal: 1, MemAvailable: 1,
		Procs: map[string]ProcAgg{"a": {Ticks: 10}},
	}
	// The first pass — a snapshot, then — a persistent source error.
	src := &flakyLoadSource{first: st, err: errors.New("источник недоступен")}
	// reports is written from the loop goroutine (Notify) and polled here
	// until the loop stops — access only under a mutex (the race detector).
	var mu sync.Mutex
	var reports []string
	opts := LoadReportOptions{
		Interval: 10 * time.Millisecond,
		Notify:   func(text string) { mu.Lock(); reports = append(reports, text); mu.Unlock() },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, src, opts) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(reports)
		mu.Unlock()
		if n >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	n := len(reports)
	mu.Unlock()
	if n != 1 {
		cancel()
		t.Fatalf("отчётов = %d, want 1 (успешный старт)", n)
	}
	// Wait for a couple of failing passes and cancel: no new reports.
	time.Sleep(60 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	mu.Lock()
	n = len(reports)
	mu.Unlock()
	if n != 1 {
		t.Errorf("отчётов после сбоев источника = %d, want 1 (сбой не строит отчёт)", n)
	}
}

// flakyLoadSource — a successful first snapshot, then a persistent error.
type flakyLoadSource struct {
	first LoadStat
	err   error
	done  bool
}

func (s *flakyLoadSource) Load(context.Context) (LoadStat, error) {
	if s.done {
		return LoadStat{}, s.err
	}
	s.done = true
	return s.first, nil
}

// TestLoadReportLoop_SpaceErrorNoDiskLine — a statfs failure of a directory does not
// produce a disk line with zeros (continue, not "0 of 0").
func TestLoadReportLoop_SpaceErrorNoDiskLine(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1,
	}
	src := &fakeLoadSource{states: []LoadStat{st}}
	var reports []string
	opts := LoadReportOptions{
		Interval: time.Hour, // the only starting pass
		Space:    fakeLoadSpace{err: errors.New("statfs: отказано")},
		Dirs:     []string{"/var/lib/crawler/content"},
		Notify:   func(text string) { reports = append(reports, text) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, src, opts) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не вышел")
	}
	if len(reports) != 1 {
		t.Fatalf("отчётов = %d, want 1", len(reports))
	}
	if strings.Contains(reports[0], "Диск /var/lib/crawler/content: свободно 0 из 0") {
		t.Errorf("сбой statfs породил нулевую строку диска:\n%s", reports[0])
	}
	if !strings.Contains(reports[0], "📀 Диск: н/д") {
		t.Errorf("секция диска не «н/д» при единственном сбойном каталоге:\n%s", reports[0])
	}
}

// switchQueuesSource — a QueueSource with a snapshot queue per pass.
type switchQueuesSource struct {
	passes []map[string]QueueStat
	i      int
}

func (s *switchQueuesSource) Queues(context.Context) (map[string]QueueStat, error) {
	if s.i >= len(s.passes) {
		return nil, nil
	}
	q := s.passes[s.i]
	s.i++
	return q, nil
}

// countingLoadSource — a LoadSource with a call counter (a disabled
// loop makes no snapshots at all).
type countingLoadSource struct {
	count int
}

func (s *countingLoadSource) Load(context.Context) (LoadStat, error) {
	s.count++
	return LoadStat{}, nil
}

// --- the post-triage of the CI run of Nov 10, 2026 (the remains after T-270/T-271/T-273) ---

// TestOSLoadCanceledContext — a ctx cancellation before the pass: an error, not a
// snapshot (a return-cut mutant would read the fixtures and return success).
func TestOSLoadCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (OSLoad{Root: fullCPUFixtures(t)}).Load(ctx); err == nil {
		t.Error("отменённый ctx: хочу ошибку прохода, не снимок")
	}
}

// TestReadCPUStat_GuestTailGarbage — the fields after eight (the guest tail)
// are not parsed at all: junk in the tail is not an error (a [9]-capacity
// mutant would reach the tail and drop the parsing of long kernel lines).
func TestReadCPUStat_GuestTailGarbage(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "stat",
		"cpu  11 22 33 44 55 66 77 88 x 100\n"+"cpu0 1 1 1 1 1 1 1 1\n")
	if _, err := readCPUStat(root + "/stat"); err != nil {
		t.Errorf("мусор в guest-хвосте: %v (поля после восьми не парсятся)", err)
	}
}

// TestReadProcs_NonDirBreaksScan — a non-directory with a digit head
// ("0note" sorts BEFORE all PID directories): continue skips only
// it; break (a mutant) would cut the walk and lose every PID below.
func TestReadProcs_NonDirBreaksScan(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "0note", "не каталог")
	mkdirFix(t, root, "400", "(afterfile) S 1 0 0 0 -1 0 0 0 0 0 7 3 0 0 20 0 9", "1 1\n")
	procs, err := readProcs(root)
	if err != nil {
		t.Fatalf("readProcs: %v", err)
	}
	if got := procs["afterfile"]; got.Ticks != 10 {
		t.Errorf("PID за не-каталогом потерян: %+v, want тики 10 (continue, не break)", got)
	}
}

// TestReadProcs_SymlinkNotDir — a symlink "PID" is not read: IsDir is part
// of the selection contract (a continue-cut mutant would read the stat
// of the symlink's target and aggregate a foreign contribution).
func TestReadProcs_SymlinkNotDir(t *testing.T) {
	root := t.TempDir()
	mkdirFix(t, root, "ghostpkg", "(ghost) S 1 0 0 0 -1 0 0 0 0 0 7 3 0 0 20 0 1", "1 1\n")
	if err := os.Symlink(root+"/ghostpkg", root+"/500"); err != nil {
		t.Fatal(err)
	}
	procs, err := readProcs(root)
	if err != nil {
		t.Fatalf("readProcs: %v", err)
	}
	if _, ok := procs["ghost"]; ok {
		t.Error("симлинк-«PID» прочитан: отбор — только настоящие каталоги (IsDir)")
	}
}

// TestParseProcStatLine_NoOpenParen — ")" without "(": an empty contribution, not
// a prefix name up to the parenthesis (the open < 0 weakening mutants let
// lines without an opening parenthesis through).
func TestParseProcStatLine_NoOpenParen(t *testing.T) {
	if comm, ticks := parseProcStatLine([]byte("1 x) R 1 2 3 4 5 6 7 8 9 10 3 4")); comm != "" || ticks != 0 {
		t.Errorf("скобка закрытия без открытия = (%q, %d), want пустой вклад", comm, ticks)
	}
}

// TestParseProcStatLine_HugeStime — stime of 2^63 is legal for uint64
// (a bitSize 63 mutant must accept it; the utime case is covered above).
func TestParseProcStatLine_HugeStime(t *testing.T) {
	if comm, ticks := parseProcStatLine([]byte(
		"9 (huge) R 1 2 3 4 5 6 7 8 9 10 5 9223372036854775808")); comm != "huge" || ticks != 9223372036854775813 {
		t.Errorf("stime 2^63 = (%q, %d), want huge и 2^63+5 (uint64)", comm, ticks)
	}
}

// TestRedisQueues_GroupsTextNoSuchKey — the stream vanished between XLEN and XINFO
// (DEL/a trim to 0): the "no such key" text form at XINFO GROUPS —
// "no groups"; the depth stays XLEN; the section is alive (a cut of the
// second disjunct would drop it; the XLEN branch is covered by the test above).
func TestRedisQueues_GroupsTextNoSuchKey(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups:    map[string][]redis.XInfoGroup{},
		xlen:      map[string]int64{queue.StreamDNS: 5},
		groupErrs: map[string]error{queue.StreamDNS: errors.New("MOVED 1234: no such key")},
	}
	q, err := RedisQueues{RDB: rdb}.Queues(context.Background())
	if err != nil {
		t.Fatalf("XINFO GROUPS «no such key»: %v (секция обязана жить)", err)
	}
	if q[queue.StreamDNS].Len != 5 {
		t.Errorf("q:dns = %+v, want глубина XLEN 5 (групп нет — XLEN)", q[queue.StreamDNS])
	}
}

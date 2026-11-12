package render

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestParseProcStat(t *testing.T) {
	// The fields after ")" start with state: ppid is the 2nd, rss
	// (pages) is the 22nd; vsize (the 21st) must not get into rss. The
	// process name has parentheses and spaces (parsed by the last ")").
	line := fmt.Sprintf("42 (chromium (renderer)) R 7 %s999999 128 0 0 0 0 0 0 0 0 0",
		strings.Repeat("0 ", 18))
	st, ok := parseProcStat([]byte(line))
	if !ok {
		t.Fatal("the stat line was not parsed")
	}
	if st.ppid != 7 {
		t.Errorf("ppid = %d, want 7", st.ppid)
	}
	if want := int64(128 * os.Getpagesize()); st.rss != want {
		t.Errorf("rss = %d, want %d (128 pages)", st.rss, want)
	}

	// The boundary of the field minimum: exactly 22 fields after ")"
	// (state .. rss) parse, 21 — a failure.
	exact := "1 (p) R 7 " + strings.Repeat("0 ", 19) + "128"
	if st, ok := parseProcStat([]byte(exact)); !ok || st.ppid != 7 ||
		st.rss != int64(128*os.Getpagesize()) {
		t.Errorf("exactly 22 fields: %+v ok=%v, want ppid 7 and an rss of 128 pages", st, ok)
	}
	short := "1 (p) R 7 " + strings.Repeat("0 ", 18) + "128"
	if _, ok := parseProcStat([]byte(short)); ok {
		t.Errorf("a 21-field line accepted: %q", short)
	}

	// ")" as the first character (i == 0): parsing goes from the last
	// ")", the fields strictly from i+2 (after the mandatory space):
	// ppid here is 0, not 7.
	lead := ")7 R " + strings.Repeat("0 ", 20) + "128"
	if st, ok := parseProcStat([]byte(lead)); !ok || st.ppid != 0 {
		t.Errorf("the lead with ')' as the first character: %+v ok=%v, want ok with ppid 0", st, ok)
	}

	// rss of 2^62 pages: the 64-bit parse is mandatory (63 bits — a
	// range error).
	huge := "1 (p) R 7 " + strings.Repeat("0 ", 19) + "4611686018427387904"
	if _, ok := parseProcStat([]byte(huge)); !ok {
		t.Errorf("a 64-bit rss rejected: %q", huge)
	}

	// Truncated and broken lines are rejected, not panicking.
	for name, bad := range map[string]string{
		"without parentheses":                "1 2 3 4",
		"without parentheses, enough fields": "a 7 " + strings.Repeat("0 ", 20) + "128",
		"a tail shorter than i+2":          "1 ()",
		"an empty name":                "1 () R 7",
		"too few fields":                "1 (p) R 7 0",
		"ppid is not a number":             "1 (p) R x " + strings.Repeat("0 ", 20),
		"rss is not a number":              "1 (p) R 7 " + strings.Repeat("0 ", 19) + "y",
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := parseProcStat([]byte(bad)); ok {
				t.Fatalf("a broken line accepted: %q", bad)
			}
		})
	}
}

func TestTreeRSS(t *testing.T) {
	procs := map[int]procStat{
		1: {ppid: 0, rss: 100},
		2: {ppid: 1, rss: 200},
		3: {ppid: 1, rss: 50},
		4: {ppid: 2, rss: 10},  // an indirect descendant (zygote/renderer)
		9: {ppid: 8, rss: 999}, // a foreign tree
	}
	if got := treeRSS(procs, 1); got != 360 {
		t.Errorf("treeRSS(1) = %d, want 360 (the tree 1+2+3+4)", got)
	}
	if got := treeRSS(procs, 2); got != 210 {
		t.Errorf("treeRSS(2) = %d, want 210", got)
	}
	if got := treeRSS(procs, 42); got != 0 {
		t.Errorf("treeRSS(a non-existent root) = %d, want 0", got)
	}
	// The root disappeared from the snapshot — 0, even if a descendant
	// with its ppid remained (the process died between listing and
	// reading; there is no reparenting in the snapshot).
	if got := treeRSS(map[int]procStat{6: {ppid: 5, rss: 99}}, 5); got != 0 {
		t.Errorf("treeRSS(a root outside the snapshot, a child exists) = %d, want 0", got)
	}
	// A cycle in ppid does not loop the traversal.
	cycle := map[int]procStat{1: {ppid: 2, rss: 1}, 2: {ppid: 1, rss: 2}}
	if got := treeRSS(cycle, 1); got != 3 {
		t.Errorf("treeRSS(a cycle) = %d, want 3", got)
	}
}

func TestReadProcStatsSkipsNonNumeric(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("/proc is unavailable")
	}
	procs, err := readProcStats()
	if err != nil {
		t.Fatalf("readProcStats: %v", err)
	}
	if len(procs) == 0 {
		t.Fatal("the /proc snapshot is empty")
	}
	// The non-numeric /proc entries (the self/thread-self symlinks) are
	// not read as processes: without continue Atoi zeroes the pid, and
	// the /proc/<name>/stat symlink is readable — a non-existent pid 0
	// would appear in the snapshot.
	if _, ok := procs[0]; ok {
		t.Error("pid 0 in the snapshot: a non-numeric /proc entry read as a process")
	}
}

func TestProcTreeRSSSelf(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("/proc is unavailable")
	}
	if got := procTreeRSS(0); got != -1 {
		t.Errorf("procTreeRSS(0) = %d, want -1 (no root)", got)
	}
	// pid 1 always exists (in a container — the container itself): a
	// valid root, the tree is non-empty (the root <= 0 boundary: 1 is a
	// working pid).
	if got := procTreeRSS(1); got <= 0 {
		t.Errorf("procTreeRSS(1) = %d, want > 0", got)
	}
	// A live process — the test itself: the RSS is positive, the tree
	// includes descendants.
	if got := procTreeRSS(os.Getpid()); got <= 0 {
		t.Fatalf("the own process's RSS %d, want > 0", got)
	}
}

func TestProcTreeRSSRealChild(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("/proc is unavailable")
	}
	// A live child process: parsing the real /proc/<pid>/stat of a
	// fresh process (not a cache of the test process).
	cmd := exec.Command("sleep", "5")
	if err := cmd.Start(); err != nil {
		t.Skipf("sleep did not start: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	var rss int64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rss = procTreeRSS(cmd.Process.Pid); rss > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rss <= 0 {
		t.Fatalf("the sleep process tree's RSS %d, want > 0", rss)
	}
}

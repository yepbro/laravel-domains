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
		t.Fatal("строка stat не разобрана")
	}
	if st.ppid != 7 {
		t.Errorf("ppid = %d, хочу 7", st.ppid)
	}
	if want := int64(128 * os.Getpagesize()); st.rss != want {
		t.Errorf("rss = %d, хочу %d (128 страниц)", st.rss, want)
	}

	// The boundary of the field minimum: exactly 22 fields after ")"
	// (state .. rss) parse, 21 — a failure.
	exact := "1 (p) R 7 " + strings.Repeat("0 ", 19) + "128"
	if st, ok := parseProcStat([]byte(exact)); !ok || st.ppid != 7 ||
		st.rss != int64(128*os.Getpagesize()) {
		t.Errorf("ровно 22 поля: %+v ok=%v, хочу ppid 7 и rss 128 страниц", st, ok)
	}
	short := "1 (p) R 7 " + strings.Repeat("0 ", 18) + "128"
	if _, ok := parseProcStat([]byte(short)); ok {
		t.Errorf("21 поле принят: %q", short)
	}

	// ")" as the first character (i == 0): parsing goes from the last
	// ")", the fields strictly from i+2 (after the mandatory space):
	// ppid here is 0, not 7.
	lead := ")7 R " + strings.Repeat("0 ", 20) + "128"
	if st, ok := parseProcStat([]byte(lead)); !ok || st.ppid != 0 {
		t.Errorf("«)» первым символом: %+v ok=%v, хочу ok с ppid 0", st, ok)
	}

	// rss of 2^62 pages: the 64-bit parse is mandatory (63 bits — a
	// range error).
	huge := "1 (p) R 7 " + strings.Repeat("0 ", 19) + "4611686018427387904"
	if _, ok := parseProcStat([]byte(huge)); !ok {
		t.Errorf("64-битный rss отвергнут: %q", huge)
	}

	// Truncated and broken lines are rejected, not panicking.
	for name, bad := range map[string]string{
		"без скобок":                "1 2 3 4",
		"без скобок, полей хватает": "a 7 " + strings.Repeat("0 ", 20) + "128",
		"хвост короче i+2":          "1 ()",
		"пустое имя":                "1 () R 7",
		"мало полей":                "1 (p) R 7 0",
		"ppid не число":             "1 (p) R x " + strings.Repeat("0 ", 20),
		"rss не число":              "1 (p) R 7 " + strings.Repeat("0 ", 19) + "y",
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := parseProcStat([]byte(bad)); ok {
				t.Fatalf("битая строка принята: %q", bad)
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
		t.Errorf("treeRSS(1) = %d, хочу 360 (дерево 1+2+3+4)", got)
	}
	if got := treeRSS(procs, 2); got != 210 {
		t.Errorf("treeRSS(2) = %d, хочу 210", got)
	}
	if got := treeRSS(procs, 42); got != 0 {
		t.Errorf("treeRSS(несуществующий корень) = %d, хочу 0", got)
	}
	// The root disappeared from the snapshot — 0, even if a descendant
	// with its ppid remained (the process died between listing and
	// reading; there is no reparenting in the snapshot).
	if got := treeRSS(map[int]procStat{6: {ppid: 5, rss: 99}}, 5); got != 0 {
		t.Errorf("treeRSS(корень вне снимка, потомок есть) = %d, хочу 0", got)
	}
	// A cycle in ppid does not loop the traversal.
	cycle := map[int]procStat{1: {ppid: 2, rss: 1}, 2: {ppid: 1, rss: 2}}
	if got := treeRSS(cycle, 1); got != 3 {
		t.Errorf("treeRSS(цикл) = %d, хочу 3", got)
	}
}

func TestReadProcStatsSkipsNonNumeric(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("/proc недоступен")
	}
	procs, err := readProcStats()
	if err != nil {
		t.Fatalf("readProcStats: %v", err)
	}
	if len(procs) == 0 {
		t.Fatal("снимок /proc пуст")
	}
	// The non-numeric /proc entries (the self/thread-self symlinks) are
	// not read as processes: without continue Atoi zeroes the pid, and
	// the /proc/<name>/stat symlink is readable — a non-existent pid 0
	// would appear in the snapshot.
	if _, ok := procs[0]; ok {
		t.Error("pid 0 в снимке: нечисловая запись /proc прочитана как процесс")
	}
}

func TestProcTreeRSSSelf(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("/proc недоступен")
	}
	if got := procTreeRSS(0); got != -1 {
		t.Errorf("procTreeRSS(0) = %d, хочу -1 (нет корня)", got)
	}
	// pid 1 always exists (in a container — the container itself): a
	// valid root, the tree is non-empty (the root <= 0 boundary: 1 is a
	// working pid).
	if got := procTreeRSS(1); got <= 0 {
		t.Errorf("procTreeRSS(1) = %d, хочу > 0", got)
	}
	// A live process — the test itself: the RSS is positive, the tree
	// includes descendants.
	if got := procTreeRSS(os.Getpid()); got <= 0 {
		t.Fatalf("RSS собственного процесса %d, хочу > 0", got)
	}
}

func TestProcTreeRSSRealChild(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("/proc недоступен")
	}
	// A live child process: parsing the real /proc/<pid>/stat of a
	// fresh process (not a cache of the test process).
	cmd := exec.Command("sleep", "5")
	if err := cmd.Start(); err != nil {
		t.Skipf("sleep не запустился: %v", err)
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
		t.Fatalf("RSS дерева процесса sleep = %d, хочу > 0", rss)
	}
}

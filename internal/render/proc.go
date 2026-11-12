package render

import (
	"os"
	"strconv"
	"strings"
)

// Chromium memory control (T-065, render spec 5.6 "restart … when the
// memory threshold is exceeded"): CDP does not expose the process RSS,
// so the threshold is computed over /proc — the total RSS of the
// Chromium process tree (the main process plus all descendants:
// renderers, GPU, crashpad — all participate in the render spec 3.2
// budget). Linux-only by construction; the project deploys to Linux
// (systemd/cgroups, render spec 10.1); in other environments the read
// returns "unknown" (-1) and the memory-triggered restart does not fire
// (Browser.RSS).

// procStat — the /proc/<pid>/stat fields needed for the tree and memory.
type procStat struct {
	ppid int   // PPid — the parent; the tree is built over it
	rss  int64 // RSS in bytes (the stat field "rss", pages × the page size)
}

// procTreeRSS — the total RSS of the process tree rooted at root, bytes.
// -1 — reading /proc failed; a root missing from the snapshot (the
// process has already died) yields 0.
func procTreeRSS(root int) int64 {
	if root <= 0 {
		return -1
	}
	procs, err := readProcStats()
	// Branch mutants (T-M07): a /proc read failure is unreachable in
	// CI — the container always has a readable /proc (TestProcTreeRSSSelf
	// and TestReadProcStatsSkipsNonNumeric read it), and it cannot be
	// hidden or unmounted from a test. Cutting the return out
	// (branch/if) is the same unreachable branch. The value -1 is the
	// "unknown" contract, pinned by tests via rootPID <= 0 above.
	// mutator-disable-next-line branch/if
	if err != nil {
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer
		return -1
	}
	return treeRSS(procs, root)
}

// readProcStats — a single pass over /proc: a snapshot {pid → ppid,
// rss}. Processes that disappeared between listing and reading are
// skipped: the snapshot is consistent by construction, the lags between
// entries are insignificant for the RSS threshold (the check runs
// before every task, the pass itself no more often than the rssCacheTTL
// cache window, T-229; render spec 5.6).
func readProcStats() (map[int]procStat, error) {
	entries, err := os.ReadDir("/proc")
	// Unreachable in CI (T-M07): /proc is always listable in the
	// container; a ReadDir failure requires a hidden/unmounted /proc.
	// Cutting the return out (branch/if) is the same unreachable branch.
	// mutator-disable-next-line branch/if
	if err != nil {
		return nil, err
	}
	procs := make(map[int]procStat, len(entries))
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			// The loop/break mutant is equivalent (T-M07): os.ReadDir
			// returns entries sorted by name, and the non-numeric /proc
			// entries ("acpi", "bus", "cmdline"…) are alphabetical and
			// come after all pids — break on the first non-numeric entry
			// equals continue (there are no more pids after it). The
			// skip itself is pinned by the test
			// TestReadProcStatsSkipsNonNumeric.
			// mutator-disable-next-line loop/break
			continue // a non-numeric name is not a process
		}
		data, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		// Cutting the continue out (branch/if) is equivalent (T-M07):
		// without continue, falling through to parseProcStat(data) with
		// data == nil is safe — the empty string is rejected by the
		// parse (LastIndex = -1), the entry is not added. loop/break is
		// nondeterministic: firing it requires the process to die between
		// listing and reading — a race, not reproducible on demand in CI.
		// mutator-disable-next-line branch/if
		if err != nil {
			// mutator-disable-next-line loop/break
			continue // the process went away between listing and reading
		}
		if st, ok := parseProcStat(data); ok {
			procs[pid] = st
		}
	}
	// Unreachable in CI (T-M07): an empty snapshot requires that /proc
	// contain not a single readable process — the test process itself
	// is always visible; exactly one (the `== 1` mutant) is also
	// unreachable in the go test container (at minimum PID 1 and the
	// test binary itself).
	// Cutting the return out (branch/if) is the same unreachable branch.
	// mutator-disable-next-line numbers/decrementer, numbers/incrementer, branch/if
	if len(procs) == 0 {
		return nil, os.ErrNotExist
	}
	return procs, nil
}

// parseProcStat parses a /proc/<pid>/stat line. The process name is
// taken by the last ")" — comm may contain parentheses and spaces; the
// fields then start from state: ppid is field 4, rss is field 24 (in
// pages, man proc).
func parseProcStat(data []byte) (procStat, bool) {
	s := string(data)
	i := strings.LastIndexByte(s, ')')
	// Equivalent mutants (T-M07): `i+2 >= len(s)` and `i+3 > len(s)`
	// differ from the original only at i+2 == len(s) — and there both
	// branches fail (Fields(s[len:]) is empty → fewer than 22 fields).
	// The annotations also suppress the killable `i <= 0` and `i < 1`
	// (')' as the first character) — the trade-off of inline annotations
	// (§4 docs/mutation-testing.md); the behavior is pinned by
	// TestParseProcStat.
	// mutator-disable-next-line expression/comparison, numbers/incrementer
	if i < 0 || i+2 > len(s) {
		return procStat{}, false
	}
	f := strings.Fields(s[i+2:])
	if len(f) < 22 { // through rss inclusive
		return procStat{}, false
	}
	ppid, err := strconv.Atoi(f[1])
	if err != nil {
		return procStat{}, false
	}
	pages, err := strconv.ParseInt(f[21], 10, 64)
	if err != nil {
		return procStat{}, false
	}
	return procStat{ppid: ppid, rss: pages * int64(os.Getpagesize())}, true
}

// treeRSS sums the RSS of the tree from root over the process snapshot.
// Closed ppid loops (a corrupted snapshot) are physically impossible,
// but "seen" guarantees termination on any input.
func treeRSS(procs map[int]procStat, root int) int64 {
	children := make(map[int][]int, len(procs))
	for pid, st := range procs {
		children[st.ppid] = append(children[st.ppid], pid)
	}
	var total int64
	seen := map[int]bool{root: true}
	queue := []int{root}
	for len(queue) > 0 {
		pid := queue[0]
		queue = queue[1:]
		st, ok := procs[pid]
		if !ok {
			// loop/break is equivalent (T-M07): !ok is possible only for
			// the root — the first queue element (descendants get into
			// the queue only from children, built over the snapshot
			// keys); after the root is dequeued the queue is empty, and
			// break and continue are indistinguishable. The skip itself
			// (including with the root's descendants in the snapshot) is
			// pinned by TestTreeRSS.
			// mutator-disable-next-line loop/break
			continue
		}
		total += st.rss
		for _, c := range children[pid] {
			if !seen[c] {
				// Equivalent mutant (T-M07): each pid enters exactly one
				// children list (the ppid is unique); only the root can
				// get into the queue twice, and it is marked at
				// initialization (seen = {root: true}) — cutting the
				// marking out is unobservable.
				// mutator-disable-next-line statement/remove
				seen[c] = true
				queue = append(queue, c)
			}
		}
	}
	return total
}

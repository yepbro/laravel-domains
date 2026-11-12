package render

import "fmt"

// The browser generation FSM (T-263, the spec tasks/TZ-fsm.md §3 FR-9): the single
// source of truth on the states and transitions of the "browser + pool" generation
// (the Manager.b/Manager.p slot). Before T-263 the generation's liveness was folded
// from five independent checks inside swapReasonLocked; now the state is the one
// predicate genStateLocked, used by swapReasonLocked and Alive.
// The state is encoded by the variables Manager.b/p/closed ×
// Browser.lost/ctx (Browser.Alive) × Pool.closed — there is no separate field
// (the formalization does not rewrite the transitions onto an FSM framework,
// TZ-fsm §1):
// the predicate is a derivative, the transitions are performed by the same
// operators as before T-263, the behavior did not change.
//
// # States
//
//	G0 "absent"    the slot is empty (m.b == nil, the manager is not closed): before
//	               the first start or after a failed start (T4).
//	G1 "starting"  transient: restartLocked under m.mu calls
//	               StartBrowser. It exists only for the owner
//	               of the mutex: concurrent acquire/Alive/Start wait on
//	               m.mu and observe the result instead (G2/G0), so
//	               genStateLocked does not return it.
//	G2 "active"    the browser is alive (Browser.Alive), the pool is open — tasks
//	               run. The page/memory thresholds are NOT states,
//	               but reasons for the planned replacement of a live generation
//	               (swapReasonLocked).
//	G3 "draining"  the pool is closed (retire): new tasks — ErrClosed,
//	               in-flight tasks are finished by a background goroutine,
//	               the browser is killed by its Close. Normally REPLACED
//	               generations live in this state (with no references from the slot);
//	               in the slot itself — the guard of the race "the generation was
//	               replaced right after acquire" and of the desync "a browser without
//	               a pool" (unreachable normally, b and p are set together).
//	G4 "dead"      the generation's browser is unavailable (!Browser.Alive):
//	               a context cancellation (LostConnection, process death,
//	               Close) or the probe's sticky lost mark
//	               (T-218/T-226, probeConn).
//	G5 "closed"    the manager is closed (Manager.Close): terminal —
//	               new generations are not started, tasks — ErrClosed,
//	               the probe — false.
//
// # Transitions: state × event × guards × target × side effects
//
//	| # | From | Event | Guards | Target | Side effects | Executor |
//	|---|------|-------|--------|--------|--------------|----------|
//	| T1 | G0, G4 | a generation is needed: a task (Run→acquire), the probe (Alive), a start (Start) | the manager is not G5 | G1 | — (entering restartLocked; G4 — restoring a dead slot, the reason "browser unavailable") | manager.go |
//	| T2 | G1 | StartBrowser succeeds | the slot had no pool (m.p == nil: the first start or a start after T4) | G2 | the "chromium started" log; the Restarts counter does not grow | restartLocked |
//	| T3 | G1 | StartBrowser succeeds | the slot had a pool (m.p != nil — a replacement) | G2 | a synchronous retire of the old pool; a background Close of the old generation (T7); Restarts+1; the "chromium restarted" log with the reason | restartLocked |
//	| T4 | G1 | StartBrowser fails | — | G0 | the old generation, if there was one (m.p != nil), goes to background closing (T7); the error goes to the caller (the task/probe will retry the start) | restartLocked |
//	| T5 | G2 | a planned replacement: the page threshold (Browser.NeedsRestart) or the memory threshold (RSS > 0 and RSS ≥ MemoryThreshold) — checked before a task/probe | — | G1 | the change as in T3, the reason — "page threshold"/"memory threshold" | swapReasonLocked → restartLocked |
//	| T6 | G2 | browser death: a context cancellation, LostConnection (a WebSocket drop/process death) or the probe's lost (T-218/T-226) | — | G4 | task errors — the browser class (the retry — Manager.Run); the replacement — by the nearest T1 | browser.go Alive/probeConn |
//	| T7 | G2/G4 | the generation was replaced (T3/T5/T4) | — | G3 → gone | draining in-flight tasks (each ≤ Options.Timeout, spec 3.4), then closing the browser — in the background (go Close); the slot does not wait for the new generation | pool.go retire/Close |
//	| T8 | G3 (in the slot) | acquire/the probe sees a closed pool | the slot's browser is alive | G1 | the change as in T3, the reason "pool closed": the tasks' ErrClosed errors do not accumulate until attempts are exhausted | swapReasonLocked |
//	| T9 | any except G5 | Manager.Close | — | G5 | the slot is detached (b, p = nil); the generation is closed synchronously outside m.mu (Pool.Close: draining tasks ≤ Options.Timeout, then the browser); a repeated Close is harmless (CAS) | manager.go Close |
//
// Non-transitions: acquire on G2 without thresholds hands the task to the current pool;
// Browser()/Restarts() read the slot/counter under m.mu; draining the replaced
// generation (T7) does not touch the slot's state.
//
// # Replacement reasons (swapReasonLocked: the slot's state + the G2 thresholds)
//
//	"first start"                     G0
//	"browser unavailable"             G4 (priority over the pool: a dead
//	                                  browser in a closed pool — G4, I1)
//	"pool closed"                     G3 in the slot (m.p == nil or
//	                                  Pool.closed)
//	"page threshold N (rendered M)"   G2, Pages ≥ RestartEveryPages
//	"memory threshold N MiB (RSS M MiB)"   G2, MemoryThreshold > 0 and
//	                                  RSS ≥ MemoryThreshold (RSS ≤ 0 —
//	                                  unknown, does not trigger)
//
// # Invariants
//
//	I1  The classification is deterministic by the priority G5 > G0 > G4 > G3:
//	    several conditions at once yield one reason line.
//	I2  G1 exists only under m.mu: the generation change is single, there is no
//	    double start (R-07); readers are serialized by the mutex.
//	I3  From G5 there are no transitions: acquire — ErrClosed, Alive — false, a new
//	    generation is not started.
//	I4  The replaced generation (G3) finishes its tasks and closes in the background:
//	    starting a new one does not wait for the drain (spec 3.4, T-065).

// genState — the state of the manager's browser generation slot: the values are
// G0…G5 of the FSM table above.
type genState int

const (
	genAbsent   genState = iota // G0 "absent"
	genStarting                 // G1 "starting" — only inside restartLocked under m.mu (I2)
	genActive                   // G2 "active"
	genDraining                 // G3 "draining"
	genDead                     // G4 "dead"
	genClosed                   // G5 "closed"
)

// String — the FSM state name (diagnostics, tests).
func (g genState) String() string {
	switch g {
	case genAbsent:
		return "отсутствует"
	case genStarting:
		return "стартующий"
	case genActive:
		return "активный"
	case genDraining:
		return "draining"
	case genDead:
		return "мёртв"
	case genClosed:
		return "закрыт"
	default:
		return fmt.Sprintf("состояние(%d)", int(g))
	}
}

// genStateLocked — the liveness/state of the current generation as one value
// (the T-263 FSM predicate, replacing five independent checks). Under m.mu;
// the branch priority — invariant I1. The replacement thresholds of a live
// generation are not included here: those are reasons, not states
// (swapReasonLocked).
func (m *Manager) genStateLocked() genState {
	if m.closed.Load() {
		return genClosed
	}
	// Equivalent mutant (T-M08): cutting out the m.b == nil check falls
	// through to !m.b.Alive() — the nil-guard of Alive returns false,
	// the generation change decision is the same (G4 instead of G0 — both change
	// the generation); only the reason line differs, and it goes
	// exclusively into the slog.Info text of the restart (logging,
	// §4 docs/mutation-testing.md).
	// mutator-disable-next-line branch/if
	if m.b == nil {
		return genAbsent
	}
	if !m.b.Alive() {
		return genDead
	}
	if m.p == nil || m.p.closed.Load() {
		return genDraining
	}
	return genActive
}

// swapReasonLocked — the generation change reason, if one is needed:
// the state classification — by the FSM predicate (genStateLocked),
// on top of a live generation — the thresholds (activeSwapReasonLocked). The
// memory threshold is checked by the RSS of the process tree (Browser.RSS,
// proc.go); an unknown RSS (the dev stack's remote mode) does not trigger — there the hard
// cap is held by the Chromium container's mem_limit (spec 3.2/10.1). A closed
// pool — the guard against the race "the generation was replaced right after
// acquire": the closed generation is replaced, and the ErrClosed errors do not
// accumulate until the task's attempts are exhausted.
func (m *Manager) swapReasonLocked() (string, bool) {
	switch m.genStateLocked() {
	case genAbsent:
		return "первый старт", true
	case genDead:
		return "браузер недоступен", true
	case genDraining:
		return "пул закрыт", true
	case genActive:
		return m.activeSwapReasonLocked()
	default: // genClosed — not classified here: acquire/Alive/Start
		// check for closure earlier (I3); genStarting — only under
		// m.mu inside restartLocked (I2), it does not reach here.
		return "", false
	}
}

// activeSwapReasonLocked — the reasons for the planned replacement of a live
// generation (G2 "active": the change is prevention via resource thresholds, not a
// reaction to death). The memory threshold: the RSS is read in one /proc pass — the
// value goes both into the check and into the message (R-06); control disabled — /proc is
// not read at all. An unknown RSS (remote mode) does not trigger.
func (m *Manager) activeSwapReasonLocked() (string, bool) {
	if m.b.NeedsRestart() {
		return fmt.Sprintf("порог страниц %d (отрендерено %d)",
			m.b.opts.RestartEveryPages, m.b.Pages()), true
	}
	if m.b.opts.MemoryThreshold > 0 {
		// Only the mutants of the left half of the condition are equivalent
		// (T-M08; the same conclusion as for Browser.OverMemory in browser.go): at a
		// threshold >= 1 (the guard above) `rss > 0` is indistinguishable from `>= 0`/`> -1`/`true`
		// — rss == 0 and rss == -1 are discarded by the right half (0/-1 >=
		// threshold is false), rss == 1 is unreachable (RSS from /proc is a
		// multiple of the page). The line granularity of the annotation also kills the
		// two killable mutants of the right half — the trade-off of §4
		// docs/mutation-testing.md: cutting out `rss >= threshold` (there would
		// remain `rss > 0` — a restart at any positive RSS) and
		// the `>= -> >` boundary (the equality RSS == threshold through a live /proc
		// is nondeterministic). Cutting out the right half is pinned by
		// TestManagerMemoryRestartBoundaries (a threshold above the live RSS —
		// no restart); the `>= -> >` boundary has no deterministic
		// cover at all (the equality RSS == threshold through a live
		// /proc is unreachable) — it is covered by tests only for the copy
		// of the expression in Browser.OverMemory (TestBrowserOverMemory,
		// the SIGSTOP-pinned RSS).
		// mutator-disable-next-line expression/comparison, expression/remove, numbers/decrementer, numbers/incrementer
		if rss := m.b.RSS(); rss > 0 && rss >= m.b.opts.MemoryThreshold {
			return fmt.Sprintf("порог памяти %d MiB (RSS %d MiB)",
				// Logging (§4 docs/mutation-testing.md): shifts and ±1
				// of the arguments change only the text of the restart
				// reason message, not the decision to restart.
				// mutator-disable-next-line arithmetic/bitwise, numbers/decrementer, numbers/incrementer
				m.b.opts.MemoryThreshold>>20, rss>>20), true
		}
	}
	return "", false
}

// The worker reader's mode FSM (T-262, spec tasks/TZ-fsm.md §3 FR-8):
// the single source of truth about the discrete mode of the stream-reading
// goroutine of the q:http Run loop. Before T-262 the mode was smeared
// over the reader's if order (failed/infraDown/failStreak/infraStreak —
// part of the six variables encoding five modes, audit §2 # 8): now the mode
// is a readerMode value chosen by the single readerDecide function from
// the pass's observations; the pause priority order is fixed by the table
// below, not by the if order. The full mode table (including the browser pause
// and the rate pause) — internal/renderworker/readerstate.go: the automaton
// is one and the same in both workers (FR-8); here — its q:http projection.
//
// # The q:http sensors
//
// The HTTP stage mounts two of the four sensors:
//
//	infrastructure  a sustained InfraError of publishing the result
//	                (OOM/the Redis connection, T-152): the flag is set
//	                by the dispatcher (queue.IsInfra), extinguished by the
//	                write probe (Reliab.Writable) or a processing success
//	fuse            deliveries fail in a row (failed ≥ Consume —
//	                a systematic processing failure, T-139)
//
// The browser (Options.Alive — the renderworker probe) and the rate cap
// (Options.Rate, T-240 — worker B's batch mode) are absent in q:http: the B
// and R modes enter the enum for the automaton's parity (FR-8: one enum in each
// worker) and are structurally unreachable — browser=false and rate=false
// are read as zero values, readerDecide does not choose them (the unit test
// fixes the unreachability).
//
// # Transitions: the pass's observations → a mode (priority top-down)
//
// The row numbers — of the full table in renderworker/readerstate.go (T1
// the browser pause and T4 the rate pause are not projected into q:http: the
// sensors are absent); "—" — the value is not observed (the sensor is not mounted) and
// is not read by the decision.
//
//	| # | Infra (infra × writable) | failed | Mode | Pass actions |
//	|---|--------------------------|--------|-------|------------------|
//	| T2 | the flag is lit, the probe fails (writable=false) | — | I | Warn, wait workerFailBackoff(infraStreak), re-observation |
//	| T3 | no flag or the probe passed | > 0 and ≥ batch | F | Warn, wait workerFailBackoff(failStreak), then a read |
//	| T5 | no flag or the probe passed | < batch | C | XREADGROUP |
//
// The semantics against the original reader do not change: the infra pause does
// not read the stream (attempts are not burned), the fuse reads after the pause
// (throttles the rate, does not block reading until a zero counter).
//
// # Panic parity (FR-8, fixed by T-262)
//
// The http-worker delivery goroutine gets recover accounting on the model of
// renderworker (worker.go, invariant I3 of its automaton): a processing panic
// (the load/Options.OnResult) — the same unacknowledged delivery: failed.Add(1),
// an Error log with the stack, a delivery without XACK comes back with a
// re-delivery (XAUTOCLAIM, T-025); the in-flight neighbors and the graceful
// shutdown are preserved — a delivery panic stopped killing the unit
// (before T-262 q:http had no recover). A panic series is throttled by
// the fuse (T3) as an error series — delivery attempts are not burned
// at full speed.
//
// The coverage — the delivery goroutine (the XREADGROUP dispatcher): the claim path
// RecoverLoop (the same processDelivery on behalf of the recoverer) is not
// covered by the recover — exactly as in the renderworker model (the FR-8 parity);
// a deterministic task panic on a claim (via claim_min_idle) still brings the unit
// down — extending the coverage beyond the parity would change the unmarked spec
// behavior (TZ-fsm spec §4) and is left as a candidate
// for a separate task.

package httpworker

import "fmt"

// readerMode — the reader's discrete mode (FR-8): the automaton's value,
// chosen by readerDecide from the pass's observations. The mode set is common
// with renderworker (browser/rate); the constant order — the pause priority of
// the full table (internal/renderworker/readerstate.go).
type readerMode int8

const (
	modeConsume      readerMode = iota // C: a chunk of deliveries is read
	modePauseBrowser                   // B: the browser sensor — renderworker only, unreachable here
	modePauseInfra                     // I: a publishing InfraError, the stream is not read
	modePauseFail                      // F: the fuse, after the pause it reads
	modePauseRate                      // R: the rate cap — renderworker only, unreachable here
)

// String — the mode name (the transition table's terms) for
// test messages.
func (m readerMode) String() string {
	switch m {
	case modeConsume:
		return "reading"
	case modePauseBrowser:
		return "browser pause"
	case modePauseInfra:
		return "infra pause"
	case modePauseFail:
		return "fuse pause"
	case modePauseRate:
		return "rate pause"
	}
	return fmt.Sprintf("readerMode(%d)", int8(m))
}

// readerObs — the observations of one reader pass (FR-8): the input of
// readerDecide. Fields go in "sensor × value" pairs: an unmounted sensor
// (browser — the renderworker browser probe, rate — the rate cap T-240)
// is not polled, its value is not read by the decision; writable is observed
// only with the infra flag lit (the write probe — one SET, T-152: a healthy
// Redis loop does not poke it).
type readerObs struct {
	browser   bool  // the browser probe is mounted (q:http — always false)
	alive     bool  // the browser is alive (observed with browser)
	infra     bool  // the sustained publishing infra-failure flag is lit (T-152)
	writable  bool  // the Redis write probe passed (observed with infra)
	failed    int64 // deliveries in a row without an acknowledgment (the dispatcher's counter)
	batch     int   // the read chunk (Options.Consume) — the fuse threshold
	rate      bool  // the rate cap is mounted (q:http — always false)
	allowance int   // the free slots of the rate window (observed with rate)
}

// readerDecide — the single function of the mode automaton's transitions
// (FR-8): the pass's observations → a mode — the priority of the full transition
// table (internal/renderworker/readerstate.go; in q:http — its rows T2/T3/T5),
// not the if order of the loop. Without memory: the pause series (the durations)
// and the failed counter live in the loop. The fuse condition requires failed > 0:
// a zero counter — not a pause at any batch (the original reader's
// series-reset branch).
func readerDecide(o readerObs) readerMode {
	switch {
	case o.browser && !o.alive:
		return modePauseBrowser
	case o.infra && !o.writable:
		return modePauseInfra
	case o.failed > 0 && o.batch > 0 && o.failed >= int64(o.batch):
		return modePauseFail
	case o.rate && o.allowance <= 0:
		return modePauseRate
	}
	return modeConsume
}

// The worker reader's mode FSM (T-262, spec tasks/TZ-fsm.md §3 FR-8):
// the single source of truth about the discrete mode of the stream-reading
// goroutine of the Run consumption loop (q:render / q:render-fallback). Before T-262 the mode
// was smeared over the reader's if order (failed/infraDown/failStreak/
// infraStreak/rateLim/Alive — six variables encoded five
// modes, audit §2 # 8): now the mode is a readerMode value,
// chosen by the single readerDecide function from the pass's
// observations; the pause priority order is fixed by the table below, not by the
// if order. The formalization does not rewrite the loop on the FSM framework
// (TZ-fsm §1): pauses and reading remain the reader's steps, exactly one mode
// corresponds to the set of observations. Related FSMs:
// the delivery outcome policy — outcomestate.go (T-261), the delivery
// of an entry — internal/queue/deliverystate.go (T-260); the same FSM
// of the q:http reader — internal/httpworker/readerstate.go (with shared
// modes; the recover parity of delivery panics lives there too).
//
// # Modes
//
//	C  reading         a chunk of deliveries is read by XREADGROUP (trimmed
//	                   to the rate window's slots); delivery attempts
//	                   are spent by the read (T1 of deliverystate.go)
//	B  browser pause   Chromium is dead (Options.Alive): the stream is NOT
//	                   read — deliveries stay in the stream (not in the
//	                   PEL), attempts are not burned; the restart — the manager
//	                   (T-065), the reader only waits
//	I  infra pause     a sustained InfraError of publishing (OOM/
//	                   the Redis connection, T-152): the stream is NOT read —
//	                   attempts are not burned; the exit — a write probe
//	                   (Reliab.Writable) or a processing success
//	F  fuse            deliveries fail in a row (failed ≥ Consume —
//	   pause           a systematic processing failure): after the pause
//	                   the pass READS — the fuse throttles the read
//	                   rate but does not block it until a zero
//	                   counter (otherwise a stuck processing failure
//	                   would stop the stage forever); the pause series
//	                   is reset by the observation of a zero counter
//	                   (an acknowledged delivery)
//	R  rate pause      the consumption cap is exhausted (the T-240 window,
//	                   FR-13 — worker B's batch mode): the stream is NOT
//	                   read until a window slot frees up
//
// # Transitions: the pass's observations → a mode (priority top-down)
//
// Each row assumes the observations of the rows above did not hold;
// "—" — the value is not observed (the sensor is not mounted) and is not read
// by the decision.
//
//	| # | Browser (browser × alive) | Infra (infra × writable) | failed | Rate (rate × allowance) | Mode | Pass actions |
//	|---|---------------------------|--------------------------|--------|-------------------------|------|--------------|
//	| T1 | dead (alive=false) | — | — | — | B | Warn, wait DefaultErrorWait, re-observation |
//	| T2 | alive or no sensor | the flag is lit, the probe fails (writable=false) | — | — | I | Warn, wait renderFailBackoff(infraStreak), re-observation |
//	| T3 | alive or no sensor | no flag or the probe passed | > 0 and ≥ batch | — | F | Warn, wait renderFailBackoff(failStreak), then a read |
//	| T4 | alive or no sensor | no flag or the probe passed | < batch | the window has no free slots (allowance ≤ 0) | R | Debug, wait rateWindow.retry, re-observation |
//	| T5 | alive or no sensor | no flag or the probe passed | < batch | slots are available (or no sensor) | C | a chunk ≤ allowance, XREADGROUP, record into the rate window |
//
// The T1→T4 order is the priority itself: a dead browser matters more than an infra failure
// (without Chromium there is no processing at all), an infra failure matters more than the
// fuse (drawing chunks between pauses is pointless — publications fail,
// T-152), the fuse matters more than the rate (a systematic processing failure
// overrides the planned limitation), the rate is the last limitation
// before a read. Re-observation after the fuse pause: during the
// wait the rate window could have been exhausted — the rate step before a read
// is re-observed (the same R pause, attempts are not spent).
//
// # Memory and its resets
//
// The mode is a function WITHOUT memory: repeated observations of the same set
// give the same mode. Only the durations and the counter hold memory:
//
//	failed       deliveries in a row without an acknowledgment (dispatcher:
//	             an error/panic +1, an acknowledgment — reset to 0)
//	infraStreak  I pauses endured in a row (the duration — doubling
//	             renderFailBackoff); reset by resumption:
//	             the infra-failure flag is extinguished by a write probe or a processing
//	             success
//	failStreak   F pauses endured in a row; reset by the observation
//	             of a zero failed counter
//
// # Invariants
//
//	I1  B and I do not read the stream: deliveries stay in the stream (not in the PEL)
//	    — delivery attempts are not burned (an attempt is spent only
//	    by a delivery: the T1 read / T4–T7 claim of deliverystate.go).
//	I2  F reads after the pause: the fuse limits the read
//	    rate but does not stop it — reads continue and
//	    are throttled by doubling (TestRenderWorker_RunBackoffOnFailures).
//	I3  A delivery processing panic is the same unacknowledged delivery:
//	    the recover in the dispatcher goroutine counts it in failed (+1),
//	    a delivery without XACK will come back with a re-delivery (XAUTOCLAIM,
//	    T-025); a series of panics is throttled by the fuse as a series of errors
//	    (TestRenderWorker_RunPanicBackoff), the process does not crash.
//	I4  Only readerDecide chooses the pass's mode: the
//	    priority order exists in one place (the table above and its
//	    unit test), the loop contains no own conditions on pauses
//	    duplicating the table. The single wait point outside the
//	    B/I/F branches — the single rate step before a read: it stops
//	    the pass with mode R and re-observes the window after the F pause (during
//	    the wait the window could have been exhausted) — as before T-262, when
//	    the rate check ran after the fuse; the series
//	    resets run before it (the order is unchanged).

package renderworker

import "fmt"

// readerMode — the reader's discrete mode (FR-8): the FSM value,
// chosen by readerDecide from the pass's observations. The constant order —
// the pause priority of the transition table (the file's doc comment).
type readerMode int8

const (
	modeConsume      readerMode = iota // C: a chunk of deliveries is read
	modePauseBrowser                   // B: dead Chromium, the stream is not read
	modePauseInfra                     // I: an InfraError of publishing, the stream is not read
	modePauseFail                      // F: the fuse, after the pause it reads
	modePauseRate                      // R: the rate cap, the stream is not read
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
// readerDecide. Fields go in "sensor × value" pairs: an unmounted
// sensor (browser — Options.Alive, rate — the rate window T-240) is not
// polled, its value is not read by the decision; writable
// is observed only with the infra flag lit (the write probe is one SET,
// T-152: a healthy Redis loop does not trigger it).
type readerObs struct {
	browser   bool  // the browser probe is mounted (Options.Alive)
	alive     bool  // the browser is alive (observed with browser)
	infra     bool  // the sustained infra failure flag of publishing is lit (T-152)
	writable  bool  // the Redis write probe passed (observed with infra)
	failed    int64 // deliveries in a row without an ack (the dispatcher's counter)
	batch     int   // the read chunk (Options.Consume) — the fuse threshold
	rate      bool  // the rate cap is mounted (Options.Rate > 0)
	allowance int   // the free slots of the rate window (observed with rate)
}

// readerDecide — the single function of the mode FSM's
// transitions (FR-8): the pass's observations → a mode, the priority T1→T4 of the
// transition table (the file's doc comment), not the if order of the loop. Without
// memory: the pause series (durations) and the failed counter live in the loop.
// The fuse condition requires failed > 0: a zero counter — not
// a pause with any batch (the branch resetting the series of the original reader).
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

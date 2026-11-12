// The worker's consumption rate (T-240, render spec stage 35 §4.5 item 1, FR-13): the cap
// of "no more than N entries per sliding window" — the "collect and rework" batch mode
// of worker B without scaling up chromium capacity. The fallback queue
// q:render-fallback accumulates as much as needed (MAXLEN T-237), worker B
// drains it at the configurable rate render.fallback.worker_rate (T-233):
// production chromium capacity is kept at the fallback-rate volume, not the peak
// debt (spec §3 item 3).
//
// The rate parameter is shared, with no binding to an engine or a stream (FR-4: there are no
// engine branches): Options.Rate is applied by the Run loop whenever it is set;
// production wiring in cmd/render-worker sets it on the worker B build
// (the q:render-fallback stream), the main q:render build reads without a cap.
//
// It is the consumption (XREADGROUP) that is limited, not the processing: the
// RecoverLoop claim is not touched by the cap — the return rate of stuck deliveries
// is configured by redis.recover_* (T-153), there is no reason to mix the two knobs.

package renderworker

import "time"

// RateWindow — the sliding window of the consumption rate cap (T-240, FR-13):
// a minute. The default cap of render.fallback.worker_rate is 100 entries (T-233):
// 100/min ≈ 1.7 entries/s — the full capacity of one chromium (8 contexts ×
// ~5 s/page), values below the default cut the rate intentionally — worker B
// reworks the tail without taking capacity from the main pipeline.
const RateWindow = time.Minute

// renderRateWindow — the window of the Run loop's rate: a variable, not a constant —
// unit tests shrink the window to hundreds of milliseconds without waiting out a minute
// (an example is renderFailWaitBase).
var renderRateWindow = RateWindow

// rateMark — a chunk of entries consumed by one read: the window stores marks
// in chunks (one read — one timestamp), not individual entries —
// the window's memory at the worker_rate validation cap (up to 1 million entries, T-233)
// stays in the thousands of marks, not millions.
type rateMark struct {
	at time.Time
	n  int
}

// rateWindow — the sliding window of the consumption rate (T-240, FR-13): no more than
// limit entries per window. Maintained by the reader of the Run loop — a single goroutine, without
// a mutex (the style of the reader's local counters failStreak/infraStreak).
// Protocol: before a read — allowance (how many entries may be taken now;
// the Consume read chunk is trimmed to it), after — record (the read
// entries enter the window). The window is sliding: a mark leaves it after window
// from the read, freeing a rate slot; an empty read (the queue is empty,
// XREADGROUP blocks) does not enter the window — idleness does not spend
// the cap, after entries appear the worker immediately takes the full budget.
type rateWindow struct {
	limit  int
	window time.Duration
	marks  []rateMark
}

// newRateWindow builds a window with the cap limit per window; limit must be
// > 0 (Run builds the window only for a set Options.Rate).
func newRateWindow(limit int, window time.Duration) *rateWindow {
	return &rateWindow{limit: limit, window: window}
}

// count drops the marks that left the window (older than window relative to
// now) and returns the number of entries in the live window.
func (w *rateWindow) count(now time.Time) int {
	live := w.marks[:0]
	n := 0
	for _, m := range w.marks {
		if now.Sub(m.at) < w.window {
			live = append(live, m)
			n += m.n
		}
	}
	w.marks = live
	return n
}

// allowance — the free rate slots at the moment now: limit minus the entries
// read during the last window. ≤ 0 — the cap is exhausted, the read must
// wait for retry; a positive value trims the read chunk so as not to
// take more than the cap at once.
func (w *rateWindow) allowance(now time.Time) int {
	return w.limit - w.count(now)
}

// retry — the pause until a rate slot frees up: the window's oldest mark + window
// − now. Reliable with the cap exhausted (the window is non-empty — the limit
// is positive); a minimum of 1 ns — protection against a zero pause on rounding
// (otherwise the reader loop would spin idle, an example is limitBucket
// dnsclient).
func (w *rateWindow) retry(now time.Time) time.Duration {
	if len(w.marks) == 0 {
		return 0
	}
	d := w.marks[0].at.Add(w.window).Sub(now)
	if d < 1 {
		d = 1
	}
	return d
}

// record records n entries read at the moment now. n ≤ 0 — a no-op:
// an empty read (the queue is empty) does not spend the cap — the window counts
// consumption, not queue requests.
func (w *rateWindow) record(now time.Time, n int) {
	if n <= 0 {
		return
	}
	w.marks = append(w.marks, rateMark{at: now, n: n})
}

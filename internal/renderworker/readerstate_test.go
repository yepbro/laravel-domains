package renderworker

import "testing"

// TestRenderWorker_ReaderDecide — the transition table of the reader mode
// FSM (T-262, TZ-fsm §3 FR-8): the full set of pass observations →
// a mode. The rows follow the T1→T4 priority order of the
// readerstate.go specification: each row assumes the observations of the rows
// above did not hold; an unmounted sensor is encoded by the zero
// values of its fields (the decision does not read the value) — these rows
// also fix the unreachability of pauses without a sensor.
func TestRenderWorker_ReaderDecide(t *testing.T) {
	const batch = 4
	for _, c := range []struct {
		name string
		obs  readerObs
		want readerMode
	}{
		{"normal reading: everything is alive",
			readerObs{browser: true, alive: true, failed: 0, batch: batch, rate: true, allowance: batch}, modeConsume},
		{"T1: a dead browser overrides all pauses",
			readerObs{browser: true, alive: false, infra: true, writable: false, failed: 100, batch: batch, rate: true, allowance: 0}, modePauseBrowser},
		{"no browser sensor — alive is not observed",
			readerObs{browser: false, alive: false, failed: 0, batch: batch}, modeConsume},
		{"T2: the infra pause overrides the fuse and the rate",
			readerObs{browser: true, alive: true, infra: true, writable: false, failed: 100, batch: batch, rate: true, allowance: 0}, modePauseInfra},
		{"the infra failure flag is lit, the write probe passed",
			readerObs{browser: true, alive: true, infra: true, writable: true, failed: 0, batch: batch}, modeConsume},
		{"the flag is not lit — writable is not observed (failed below the threshold)",
			readerObs{infra: false, writable: false, failed: 3, batch: batch}, modeConsume},
		{"T3: the fuse overrides the rate",
			readerObs{alive: true, infra: false, failed: batch, batch: batch, rate: true, allowance: 0}, modePauseFail},
		{"failures below the read chunk — the fuse is silent",
			readerObs{failed: batch - 1, batch: batch}, modeConsume},
		{"a zero counter — not a pause (the series reset branch)",
			readerObs{failed: 0, batch: batch}, modeConsume},
		{"a zero counter with a degenerate chunk — not a pause",
			readerObs{failed: 0, batch: 0}, modeConsume},
		{"T4: the rate window is exhausted",
			readerObs{failed: 0, batch: batch, rate: true, allowance: 0}, modePauseRate},
		{"T4: the rate window in debt (allowance < 0)",
			readerObs{failed: 0, batch: batch, rate: true, allowance: -2}, modePauseRate},
		{"no rate sensor — allowance is not observed",
			readerObs{failed: 0, batch: batch, rate: false, allowance: 0}, modeConsume},
		{"free rate slots trim the chunk — it is a read, not a pause",
			readerObs{failed: 0, batch: batch, rate: true, allowance: 2}, modeConsume},
	} {
		if got := readerDecide(c.obs); got != c.want {
			t.Errorf("%s: mode %s, want %s (observations: %+v)", c.name, got, c.want, c.obs)
		}
	}
}

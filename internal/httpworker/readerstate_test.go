package httpworker

import "testing"

// TestWorkerReaderDecide — the mode automaton's transition table of the q:http
// reader (T-262, TZ-fsm spec §3 FR-8): the pass's observations → a mode. The full
// mode table (browser/rate) — renderworker/readerstate.go; here —
// its q:http projection: there are no browser and rate sensors, the B/R modes
// are structurally unreachable (browser=false, rate=false) — the rows fix
// this, and the last two confirm the totality of the function on all
// five modes of the enum (the workers' automaton parity).
func TestWorkerReaderDecide(t *testing.T) {
	const batch = 4
	for _, c := range []struct {
		name string
		obs  readerObs
		want readerMode
	}{
		{"T5: normal reading",
			readerObs{failed: 0, batch: batch}, modeConsume},
		{"T2: the infra pause overrides the fuse",
			readerObs{infra: true, writable: false, failed: 100, batch: batch}, modePauseInfra},
		{"the infra failure flag is lit, the write probe passed — the fuse is next",
			readerObs{infra: true, writable: true, failed: batch, batch: batch}, modePauseFail},
		{"the flag is not lit — writable is not observed, the fuse works",
			readerObs{infra: false, writable: false, failed: batch, batch: batch}, modePauseFail},
		{"T3: the fuse at the chunk threshold",
			readerObs{failed: batch, batch: batch}, modePauseFail},
		{"failures below the read chunk — the fuse is silent",
			readerObs{failed: batch - 1, batch: batch}, modeConsume},
		{"a zero counter — not a pause (the series reset branch)",
			readerObs{failed: 0, batch: batch}, modeConsume},
		{"a zero counter with a degenerate chunk — not a pause",
			readerObs{failed: 0, batch: 0}, modeConsume},
		{"the unreachable browser pause: no sensor in q:http",
			readerObs{browser: false, alive: false, failed: 0, batch: batch}, modeConsume},
		{"the unreachable rate pause: no sensor in q:http",
			readerObs{rate: false, allowance: 0, failed: 0, batch: batch}, modeConsume},
		{"the enum totality: a mounted browser sensor would choose B",
			readerObs{browser: true, alive: false, failed: 0, batch: batch}, modePauseBrowser},
		{"the enum totality: a mounted rate sensor would choose R",
			readerObs{failed: 0, batch: batch, rate: true, allowance: 0}, modePauseRate},
	} {
		if got := readerDecide(c.obs); got != c.want {
			t.Errorf("%s: mode %s, want %s (observations: %+v)", c.name, got, c.want, c.obs)
		}
	}
}

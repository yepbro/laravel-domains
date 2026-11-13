// Tests of the triage of the surviving mutants of the CI run of
// 11/10/2026 (part B, score 0.857): the status coverage supplements —
// the cache clock by default (nil Now — time.Now, the call does not
// panic), the defensive copy of the cached map is complete (a cut-short
// copy would spoil the cache on the very first call), a single-entry TTL
// is a legal explicit value; a status name outside the legend does not
// interrupt the walk of the response (the later legend rows are
// counted); a one-second throughput window is a real window, not the
// default.
package status

import (
	"context"
	"testing"
	"time"

	"crawler/internal/model"
)

// TestCacheTriage — the TTL cache mechanics: the clock by default
// (nil Now — time.Now), a full defensive copy of the map, a single-entry
// TTL.
func TestCacheTriage(t *testing.T) {
	// A TTL of 1ns is a positive explicit value; the default does not
	// replace it.
	if o := (CacheOptions{TTL: time.Nanosecond}).withDefaults(); o.TTL != time.Nanosecond {
		t.Errorf("TTL 1ns replaced by the default: %+v (the default is only for ≤ 0)", o)
	}

	// NewCache without a clock: a nil Now must be replaced with
	// time.Now — the very first call does not panic (the mutant kept the
	// nil function).
	c := NewCache(CacheOptions{})
	got, err := cached(c, &c.statuses, func() (map[string]int64, error) {
		return map[string]int64{"new": 1, "alive": 2}, nil
	})
	if err != nil {
		t.Fatalf("cached without explicit clocks: %v", err)
	}
	// The defensive copy is COMPLETE: a cut-short/skipped record in
	// cloneOf would return a trimmed map (a mutation of the result by the
	// caller would tear the cache).
	if len(got) != 2 || got["new"] != 1 || got["alive"] != 2 {
		t.Errorf("a copy of the cache value = %v, want both entries (a full copy)", got)
	}
}

// TestFetchDomainStatusesTriageUnknownFirst — a name outside the
// model.Statuses legend does NOT interrupt the walk of the response
// (continue, not break): the legend rows after it are counted.
func TestFetchDomainStatusesTriageUnknownFirst(t *testing.T) {
	pool := newStatusPool()
	pool.domStatuses = []statusStatusSeed{
		{"unknown-code", 7}, // outside the legend — the first row of the response
		{model.StatusAlive.String(), 40},
		{model.StatusNew.String(), 1000},
	}
	got, err := FetchDomainStatuses(context.Background(), pool)
	if err != nil {
		t.Fatalf("FetchDomainStatuses: %v", err)
	}
	if got["alive"] != 40 || got["new"] != 1000 {
		t.Errorf("statuses after an unknown name = %v: live rows must be counted", got)
	}
}

// TestComputeRatesTriageWindowOne — a one-second bucket window is a real
// window (dividing by 1), not the speedWindowSec default: a manual
// assembly of the values with a narrow window does not understate the
// throughput.
func TestComputeRatesTriageWindowOne(t *testing.T) {
	r := ComputeRates(Throughput{HourWindowSec: 1, ResultsHour: 3600, RenderedHour: 60}, 0, 0)
	if r.ResultsPerSec != 3600 || r.RenderPerSec != 60 {
		t.Errorf("the 1s-window rate = %v/%v, want 3600/60 (dividing by the actual window length)",
			r.ResultsPerSec, r.RenderPerSec)
	}
}

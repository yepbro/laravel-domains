// Structural shutdown invariant of render-worker (T-232): the background
// loops of run (DiskWatchLoop — store, the collector of the q:render
// depth — rdb via the gate) are stopped by the derived cancellation and
// joined by the WaitGroup before the defer-closes of rdb/store/the
// Chromium manager on any exit of run — a graceful ctx cancellation, an
// early return from renderworker.New, a panic. Before T-232 both loops
// lived fire-and-forget: the defer-closes could catch the middle of a
// DiskWatchLoop pass or a queue depth request. main.go is checked by the
// shared helper internal/bgjoin (the sample and progenitor —
// TestRunBackgroundJoin of http-worker, T-223).
package main

import (
	"testing"

	"crawler/internal/bgjoin"
)

func TestRunBackgroundJoin(t *testing.T) {
	bgjoin.Check(t, "main.go")
}

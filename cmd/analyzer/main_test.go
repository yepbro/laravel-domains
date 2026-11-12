// Structural shutdown invariant of analyzer (T-232): the background loops of
// run (ReanalyzeLoop — the DB pool and publishing q:analyze via rdb, the
// collector of the q:analyze depth — rdb via the gate) are stopped by the
// derived cancellation and joined by the WaitGroup before the defer-closes
// of store/rdb on any exit of run — a graceful ctx cancellation, an early
// return from analyzer.New, a panic. Before T-232 both loops lived
// fire-and-forget: the defer-closes could catch the middle of a reanalysis
// pass. main.go is checked by the shared helper internal/bgjoin (the
// sample and progenitor — TestRunBackgroundJoin of http-worker, T-223).
package main

import (
	"testing"

	"crawler/internal/bgjoin"
)

func TestRunBackgroundJoin(t *testing.T) {
	bgjoin.Check(t, "main.go")
}

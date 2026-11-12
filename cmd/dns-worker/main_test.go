// Structural shutdown invariant of dns-worker (T-232): the background
// collectors of run (the q:dns depth — rdb via the gate, the state of the
// limiter T-035, the switchovers to the reserve T-107 — metrics) are
// stopped by the derived cancellation and joined by the WaitGroup before
// the defer-closes of rdb and the clients of the resolver chain on any
// exit of run — a graceful ctx cancellation, a panic. Before T-232 all
// three lived fire-and-forget: the defer-closes could catch a queue depth
// request. main.go is checked by the shared helper internal/bgjoin
// (the sample and progenitor — TestRunBackgroundJoin of http-worker,
// T-223).
package main

import (
	"testing"

	"crawler/internal/bgjoin"
)

func TestRunBackgroundJoin(t *testing.T) {
	bgjoin.Check(t, "main.go")
}

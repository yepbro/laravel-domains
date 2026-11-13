package db

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestScanGate — the heavy-scan uniqueness semaphore (T-271): mutual
// exclusion of two holders (the 08–09.11.2026 incident — the retention ↔
// render-late scans coinciding at ≈1 GiB each brought PostgreSQL down with
// an OOM kill), ctx cancellation interrupts the wait, a nil gate is a
// no-op.
func TestScanGate(t *testing.T) {
	// nil — a no-op: Acquire/Release with no effect and no panic.
	if err := ScanGate(nil).Acquire(context.Background()); err != nil {
		t.Fatalf("the nil gate Acquire: %v", err)
	}
	ScanGate(nil).Release()

	gate := make(ScanGate, 1)
	var (
		inFlight atomic.Int32
		maxSeen  atomic.Int32
		wg       sync.WaitGroup
	)
	hold := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := gate.Acquire(ctx); err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			defer gate.Release()
			n := inFlight.Add(1)
			for {
				m := maxSeen.Load()
				if n <= m || maxSeen.CompareAndSwap(m, n) {
					break
				}
			}
			<-hold // hold the gate longer than the goroutine scheduler
			inFlight.Add(-1)
		}()
	}
	// Both holders must line up: the concurrency peak is 1.
	time.Sleep(50 * time.Millisecond)
	close(hold)
	wg.Wait()
	if max := maxSeen.Load(); max != 1 {
		t.Fatalf("the peak of concurrent holders = %d, want 1 (mutual exclusion)", max)
	}

	// ctx cancellation while waiting for the gate: a busy gate — a ctx
	// error, not a hang.
	if err := gate.Acquire(context.Background()); err != nil {
		t.Fatalf("acquiring a free gate: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() { got <- gate.Acquire(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-got:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting for the gate under cancellation = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire did not return on ctx cancellation")
	}
	gate.Release()
}

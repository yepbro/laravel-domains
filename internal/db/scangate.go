package db

import "context"

// ScanGate — a uniqueness semaphore for a service's heavy background scans
// (T-271): a buffered channel of capacity 1, shared by independent
// background loops of one service (http-worker: retention drain ↔ the
// render late re-publisher). Incident 08–09.11.2026: the degradation of
// two cursor-selection plans to "seq scan + hash join + Sort" held ≈1 GiB
// of working structures per scan; two scans coinciding on top of 2 GiB of
// shared_buffers meant five PostgreSQL OOM kills overnight. Each
// selection's degradation source is closed in its own way (T-273):
// retention — pinning the plan with SET LOCAL on the cursor SELECT,
// render-late — returning to the unpinned form (the T-271 pinning itself
// unwound the join from domains — an endless loop past the deadline); the
// gate is a guard against residual heavy scans coinciding (retention's
// wide catch-up circle, the snapshot's Sort): while one loop is between
// Acquire and Release, the other waits.
//
// Capacity 1 — "one heavy scan per service": point statements (PK,
// unnest chunks) do not go under the gate and carry no delays. A nil gate
// is a no-op (isolated builds, unit tests without the bus); the service
// assembly creates the channel: make(db.ScanGate, 1).
type ScanGate chan struct{}

// Acquire takes the gate; the wait is interrupted by ctx cancellation — a
// service shutdown does not wait for someone else's background pass. The
// paired Release must come strictly after a successful Acquire (defer at
// the call site): a Release without Acquire would block forever on an
// empty channel.
func (g ScanGate) Acquire(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case g <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release releases the gate; a nil gate is a no-op.
func (g ScanGate) Release() {
	if g == nil {
		return
	}
	<-g
}

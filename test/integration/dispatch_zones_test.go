// Integration tests for the dispatcher selection filter "only zones with
// work" (T-183, spec tasks/TZ_slow_dispatch_zones.md §5.2): the source of
// the round 1 zone set becomes the zones.domains_pending counter (T-178),
// not the mere presence of pending rows — live semantics tested against
// PostgreSQL/migrations. Run: make test-integration.
package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crawler/internal/ingest"
	"crawler/internal/scheduler"
)

// TestScheduler_DispatchOnlyZonesWithWork — the dispatcher selects only
// zones with domains_pending > 0: a zone with pending is selected; a zone
// with pending rows but a zero counter (drift — inserts bypassing the
// increments, the filter's trust boundary until the nearest
// reconciliation) is not selected; exhausting the zone's pending (moving
// all its domains to queued by the same EnqueueBatch) closes it until a
// refill; an add-file into a new zone opens it right away — the apply_add
// increment (T-178) without waiting for reconciliation.
func TestScheduler_DispatchOnlyZonesWithWork(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Zone with work: the counter is maintained by the seed (equivalent
	// of apply_add deltas).
	busy := insertPendingDomains(t, pool, "busy.test", []string{"b1", "b2"})

	// Drift zone: pending rows exist, counter is 0 — manual SQL bypassing
	// the increments (incremental-counters spec §4.1: reconciliation fixes
	// drift; here we are before it).
	drift := accInsertZone(t, pool, "drift.test", "")
	if _, err := pool.Exec(ctx,
		"INSERT INTO domains (zone_id, label) VALUES ($1, 'stale1'), ($1, 'stale2')", drift,
	); err != nil {
		t.Fatalf("inserting the drift zone's domains: %v", err)
	}

	// Round 1 sees only the zone with work: drift zone domains are not selected.
	batch, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("dispatch batch: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("dispatched domains: %d, want 2 (only the zone with work)", len(batch))
	}
	got := idSet(func() []int64 {
		ids := make([]int64, 0, len(batch))
		for _, d := range batch {
			ids = append(ids, d.DomainID)
		}
		return ids
	}())
	for _, id := range busy {
		if !got[id] {
			t.Fatalf("domain %d of busy.test not dispatched (batch %+v)", id, batch)
		}
	}

	// Exhaustion: moving all pending of the zone to queued decrements the
	// counter with the same operator (enqueue, T-178) — the next selection
	// is empty, the zone is closed until a refill (no probes by "tail").
	q := &countersStubQueue{}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch); err != nil {
		t.Fatalf("enqueue batch: %v", err)
	}
	after, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Fatalf("after the exhaustion dispatched %d, want 0 (the zone outside the selection)", len(after))
	}

	// Refill: an add-file into a new zone through the ingest pipeline (the
	// same build as cmd/ingest — newIngestEnv) — apply_add increments
	// domains_pending (T-178), the zone is visible to the dispatcher
	// immediately.
	dir := t.TempDir()
	pipe, err := ingest.NewPipeline(
		ingest.NewZoneStore(pool),
		ingest.NewFileStore(pool),
		pool,
		ingest.PipelineOptions{Dir: dir, BatchSize: 4, AllowedZonePaths: []string{"fresh.test"}},
	)
	if err != nil {
		t.Fatalf("ingest.NewPipeline: %v", err)
	}
	file := filepath.Join(dir, "fresh.test", "add", "fresh.txt")
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(fileContent("f1.fresh.test", "f2.fresh.test")), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := pipe.Process(ctx, file); err != nil || out.Kind != ingest.OutcomeApplied {
		t.Fatalf("the ingest pipeline: out=%+v err=%v, want applied", out, err)
	}
	refilled, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(refilled) != 2 {
		t.Fatalf("after the ingest dispatched %d, want 2 (apply_add opened the zone immediately)", len(refilled))
	}
	for _, d := range refilled {
		if !strings.HasSuffix(d.Name, ".fresh.test") {
			t.Fatalf("a foreign domain %q in the batch — want only fresh.test", d.Name)
		}
	}
}

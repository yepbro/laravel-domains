// Integration tests for the background cleanup of orphaned staging
// batches (T-098, ING-P2-3 guard): the batch_id < threshold range
// semantics against live PostgreSQL — a batch older than the threshold
// is removed, fresh ones stay untouched, a repeated pass is a no-op.
// Unit level (identifier encoding and ordering, the SQL/argument
// contract, errors and the loop) — internal/ingest/cleanup_test.go
// and stage_test.go.
package integration

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"crawler/internal/ingest"
)

// stageBatchID builds a batch identifier with the given creation time:
// the high 8 bytes are Unix microseconds big-endian, the low ones are
// random (encoding of internal/ingest.NewBatchID, T-098; the
// integration package test assembles it itself — the generator only
// accepts the current time).
func stageBatchID(t *testing.T, at time.Time) []byte {
	t.Helper()
	id := make([]byte, ingest.BatchIDLen)
	binary.BigEndian.PutUint64(id[:8], uint64(at.UnixMicro())) // time prefix
	if _, err := rand.Read(id[8:]); err != nil {               // random tail
		t.Fatalf("случайный хвост batch_id: %v", err)
	}
	return id
}

// stageOrphanRows inserts batch rows directly into ingest_stage —
// a model of an orphaned batch: the pipeline that created it died
// without a discard (kill -9, DBMS crash), the batch_id will never
// appear again.
func (e *ingestEnv) stageOrphanRows(t *testing.T, batchID []byte, zoneID int16, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := e.pool.Exec(context.Background(),
			"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) "+
				"VALUES ($1, $2, $3, 1, $4)",
			batchID, zoneID, fmt.Sprintf("orphan-%d", i), int64(i)); err != nil {
			t.Fatalf("вставка строки осиротевшей партии: %v", err)
		}
	}
}

// TestIngest_CleanupStaleBatches — the batch older than the threshold
// is removed; the one at the threshold (but younger) and the fresh one
// survive; a repeated pass is a no-op.
func TestIngest_CleanupStaleBatches(t *testing.T) {
	skipUnlessIntegration(t)
	e := newIngestEnv(t)
	ctx := context.Background()

	// The zone is created by the normal pipeline: staging references zone_id.
	if out := e.process(t, "com/add/seed.txt", fileContent("seed.com")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("посевной файл: итог = %s, want applied", out.Kind)
	}
	z, ok := e.zone(t, "com")
	if !ok {
		t.Fatal("зона com не найдена")
	}

	now := time.Now()
	oldID := stageBatchID(t, now.Add(-ingest.StaleBatchAge-time.Hour))  // a day and an hour ago
	edgeID := stageBatchID(t, now.Add(-ingest.StaleBatchAge+time.Hour)) // an hour younger than the threshold
	freshID := stageBatchID(t, now)                                     // fresh batch
	e.stageOrphanRows(t, oldID, z.id, 3)
	e.stageOrphanRows(t, edgeID, z.id, 2)
	e.stageOrphanRows(t, freshID, z.id, 2)

	rows, err := ingest.CleanupStaleBatches(ctx, e.pool, now)
	if err != nil {
		t.Fatalf("CleanupStaleBatches: %v", err)
	}
	if rows != 3 {
		t.Errorf("удалено строк = %d, want 3 (только партия старше порога)", rows)
	}
	if n := e.countOf(t, "ingest_stage"); n != 4 {
		t.Errorf("строк в ingest_stage = %d, want 4: партии у порога и свежая не задеваются", n)
	}

	// A repeated pass at the same point in time is a no-op (the cleaned
	// rows are not selected again; spec 3.4 idempotency).
	rows, err = ingest.CleanupStaleBatches(ctx, e.pool, now)
	if err != nil {
		t.Fatalf("повторная CleanupStaleBatches: %v", err)
	}
	if rows != 0 {
		t.Errorf("повторная зачистка удалила %d, want 0", rows)
	}
	if n := e.countOf(t, "ingest_stage"); n != 4 {
		t.Errorf("строк в ingest_stage после повтора = %d, want 4", n)
	}

	// Fresh batches leave staging the normal way — ClearBatch of their own batch.
	for _, id := range [][]byte{edgeID, freshID} {
		var bid ingest.BatchID
		copy(bid[:], id)
		if _, err := ingest.ClearBatch(ctx, e.pool, bid); err != nil {
			t.Fatalf("ClearBatch: %v", err)
		}
	}
	if n := e.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage после ClearBatch = %d, want 0", n)
	}
}

// Интеграционные тесты фоновой зачистки осиротевших партий staging
// (T-098, страховка ING-P2-3): семантика диапазона batch_id < границы
// против живой PostgreSQL — партия старше порога удаляется, свежие не
// задеваются, повторный проход — no-op. Юнит-уровень (кодировка
// идентификатора и порядок, контракт SQL/аргумента, ошибки и цикл) —
// internal/ingest/cleanup_test.go и stage_test.go.
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

// stageBatchID — идентификатор партии с заданным временем создания:
// старшие 8 байт — микросекунды Unix big-endian, младшие — случайные
// (кодировка internal/ingest.NewBatchID, T-098; тест пакета integration
// собирает его сам — генератор принимает только текущее время).
func stageBatchID(t *testing.T, at time.Time) []byte {
	t.Helper()
	id := make([]byte, ingest.BatchIDLen)
	binary.BigEndian.PutUint64(id[:8], uint64(at.UnixMicro())) // префикс времени
	if _, err := rand.Read(id[8:]); err != nil {               // случайный хвост
		t.Fatalf("случайный хвост batch_id: %v", err)
	}
	return id
}

// stageOrphanRows вставляет строки партии напрямую в ingest_stage —
// модель осиротевшей партии: конвейер, создавший её, погиб без discard
// (kill -9, падение СУБД), batch_id больше не встретится.
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

// TestIngest_CleanupStaleBatches — старше порога удаляется, у порога
// (но моложе) и свежая живут; повторный проход — no-op.
func TestIngest_CleanupStaleBatches(t *testing.T) {
	skipUnlessIntegration(t)
	e := newIngestEnv(t)
	ctx := context.Background()

	// Зона создаётся штатным конвейером: staging ссылается на zone_id.
	if out := e.process(t, "com/add/seed.txt", fileContent("seed.com")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("посевной файл: итог = %s, want applied", out.Kind)
	}
	z, ok := e.zone(t, "com")
	if !ok {
		t.Fatal("зона com не найдена")
	}

	now := time.Now()
	oldID := stageBatchID(t, now.Add(-ingest.StaleBatchAge-time.Hour))  // сутки и час назад
	edgeID := stageBatchID(t, now.Add(-ingest.StaleBatchAge+time.Hour)) // на час моложе порога
	freshID := stageBatchID(t, now)                                     // свежая партия
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

	// Повторный проход по тому же моменту — no-op (вычищенное не
	// выбирается, идемпотентность ТЗ 3.4).
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

	// Свежие партии покидают staging штатно — ClearBatch своей партии.
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

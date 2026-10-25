// Интеграционные тесты фильтра выборки диспетчера «только зоны с
// работой» (T-183, ТЗ tasks/TZ_slow_dispatch_zones.md §5.2): источником
// множества зон круга 1 становится счётчик zones.domains_pending (T-178),
// а не сам факт наличия pending-строк — живая семантика против
// PostgreSQL/миграций. Прогон — make test-integration.
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

// TestScheduler_DispatchOnlyZonesWithWork — диспетчер выбирает только
// зоны с domains_pending > 0: зона с pending выбирается; зона с
// pending-строками, но нулевым счётчиком (дрейф — вставка мимо
// инкрементов, граница доверия фильтра до ближайшей реконсиляции) не
// пробируется; исчерпание pending зоны (перевод всех доменов в queued
// тем же EnqueueBatch) закрывает её до пополнения; add-файл в новую зону
// открывает её сразу — инкремент apply_add (T-178) без ожидания
// реконсиляции.
func TestScheduler_DispatchOnlyZonesWithWork(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Зона с работой: счётчик ведёт посев (эквивалент дельт apply_add).
	busy := insertPendingDomains(t, pool, "busy.test", []string{"b1", "b2"})

	// Зона-дрейф: pending-строки есть, счётчик 0 — ручной SQL мимо
	// инкрементов (ТЗИ §4.1: дрейф чинит реконсиляция, здесь — до неё).
	drift := accInsertZone(t, pool, "drift.test", "")
	if _, err := pool.Exec(ctx,
		"INSERT INTO domains (zone_id, label) VALUES ($1, 'stale1'), ($1, 'stale2')", drift,
	); err != nil {
		t.Fatalf("вставка доменов зоны-дрейфа: %v", err)
	}

	// Круг 1 видит только зону с работой: домены зоны-дрейфа не выбраны.
	batch, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("выборка пачки: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("выбрано доменов: %d, хочу 2 (только зона с работой)", len(batch))
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
			t.Fatalf("домен %d зоны busy.test не выбран (пачка %+v)", id, batch)
		}
	}

	// Исчерпание: перевод всех pending зоны в queued декрементирует
	// счётчик тем же оператором (enqueue, T-178) — следующая выборка
	// пуста, зона закрыта до пополнения (проб по «хвосту» нет).
	q := &countersStubQueue{}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch); err != nil {
		t.Fatalf("постановка пачки: %v", err)
	}
	after, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Fatalf("после исчерпания выбрано %d, хочу 0 (зона вне выборки)", len(after))
	}

	// Пополнение: add-файл в новую зону через конвейер ingest (та же
	// сборка, что cmd/ingest — newIngestEnv) — apply_add инкрементирует
	// domains_pending (T-178), зона видна диспетчеру сразу.
	dir := t.TempDir()
	pipe, err := ingest.NewPipeline(
		ingest.NewZoneStore(pool),
		ingest.NewFileStore(pool),
		pool,
		ingest.PipelineOptions{Dir: dir, BatchSize: 4},
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
		t.Fatalf("конвейер ingest: out=%+v err=%v, хочу applied", out, err)
	}
	refilled, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(refilled) != 2 {
		t.Fatalf("после ingest выбрано %d, хочу 2 (apply_add открыл зону сразу)", len(refilled))
	}
	for _, d := range refilled {
		if !strings.HasSuffix(d.Name, ".fresh.test") {
			t.Fatalf("в пачке чужой домен %q — хочу только fresh.test", d.Name)
		}
	}
}

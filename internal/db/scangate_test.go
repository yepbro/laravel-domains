package db

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestScanGate — семафор единственности тяжёлых сканов (T-271):
// взаимное исключение двух держателей (инцидент 08–09.11.2026 —
// совпадение сканов retention ↔ render-late по ≈1 ГиБ клало PostgreSQL
// OOM-киллом), отмена ctx прерывает ожидание, nil-гейт — no-op.
func TestScanGate(t *testing.T) {
	// nil — no-op: Acquire/Release без эффекта и без паники.
	if err := ScanGate(nil).Acquire(context.Background()); err != nil {
		t.Fatalf("nil-гейт Acquire: %v", err)
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
			<-hold // держать гейт дольше планировщика горутин
			inFlight.Add(-1)
		}()
	}
	// Оба держателя обязаны выстроиться: пик одновременности — 1.
	time.Sleep(50 * time.Millisecond)
	close(hold)
	wg.Wait()
	if max := maxSeen.Load(); max != 1 {
		t.Fatalf("пик одновременных держателей = %d, want 1 (взаимное исключение)", max)
	}

	// Отмена ctx в ожидании гейта: занятый гейт — ошибка ctx, не висение.
	if err := gate.Acquire(context.Background()); err != nil {
		t.Fatalf("занятие свободного гейта: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() { got <- gate.Acquire(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-got:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ожидание гейта под отменой = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire не вернулся по отмене ctx")
	}
	gate.Release()
}

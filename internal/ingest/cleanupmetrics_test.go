// Юнит-тесты метрики фоновой очистки (T-099): экспорт BadHashes проходов
// в счётчик crawler_ingest_bad_hashes_total, нулевой итог не меняет
// значение, no-op без реестра, идемпотентность конструктора (T-076) и
// доставка итогов проходов циклом через CleanupOptions.OnPass.
package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// cleanupCounterValue — значение счётчика name (без меток, кроме
// сервисной): первый ряд семейства; -1, если метрика не экспортирована
// (стиль fallbackCounterValue в dnsworker).
func cleanupCounterValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// TestCleanupMetricsObserve — итоги проходов накапливаются в счётчик:
// нулевой BadHashes значение не меняет, положительные складываются
// (Prometheus-семантика _total сохраняется).
func TestCleanupMetricsObserve(t *testing.T) {
	reg := metrics.New("ingest")
	m := NewCleanupMetrics(reg)

	m.Observe(CleanupResult{Domains: 3, Results: 10}) // BadHashes 0 — нет роста
	if got := cleanupCounterValue(t, reg, "crawler_ingest_bad_hashes_total"); got != 0 {
		t.Fatalf("после нулевого BadHashes счётчик = %v, want 0", got)
	}

	m.Observe(CleanupResult{BadHashes: 3})
	m.Observe(CleanupResult{BadHashes: 0})
	m.Observe(CleanupResult{BadHashes: 2})
	if got := cleanupCounterValue(t, reg, "crawler_ingest_bad_hashes_total"); got != 5 {
		t.Errorf("счётчик = %v, want 5 (3 + 2, нулевые итоги не меняют)", got)
	}
}

// TestCleanupMetricsNoRegistry — без реестра коллектора нет (изолированные
// сборки), вызовы Observe — no-op без паники.
func TestCleanupMetricsNoRegistry(t *testing.T) {
	if m := NewCleanupMetrics(nil); m != nil {
		t.Fatalf("NewCleanupMetrics(nil) = %v, want nil", m)
	}
	NewCleanupMetrics(nil).Observe(CleanupResult{BadHashes: 7}) // не паникует

	reg := metrics.New("ingest")
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == "crawler_ingest_bad_hashes_total" {
			t.Fatalf("прибор зарегистрирован без реестра: %s", mf.GetName())
		}
	}
}

// TestCleanupMetricsIdempotent — повторная регистрация делит тот же
// счётчик (T-076): вызовы через второй экземпляр продолжают ряд.
func TestCleanupMetricsIdempotent(t *testing.T) {
	reg := metrics.New("ingest")
	first := NewCleanupMetrics(reg)
	second := NewCleanupMetrics(reg) // дубль-регистрация — не паника

	first.Observe(CleanupResult{BadHashes: 4})
	second.Observe(CleanupResult{BadHashes: 6})
	if got := cleanupCounterValue(t, reg, "crawler_ingest_bad_hashes_total"); got != 10 {
		t.Errorf("счётчик = %v, want 10 (один прибор на оба экземпляра)", got)
	}
}

// TestCleanupDeletedOnPass — цикл отдаёт хуку итог каждого успешного
// прохода (включая нулевой) и не отдаёт ошибочные: Warn уже записан,
// метрика по несостоявшемуся проходу не врёт.
func TestCleanupDeletedOnPass(t *testing.T) {
	var mu sync.Mutex
	var passes []CleanupResult

	pool := &cleanupFakePool{
		// Проход 1 (сразу) — работа с мусорным хэшем; проход 2 — ошибка
		// БД; проход 3 — пустой набор; дальше — пустые.
		selects: []*cleanupFakeRows{
			cleanupRows([]any{int16(1), int64(10)}),
			cleanupRows(),
		},
		selectErrs: []error{nil, errors.New("бд недоступна")},
		deletes: []*cleanupFakeRows{cleanupRows(
			[]any{int16(1), int64(10), cleanupHash16(0xaa)},
			[]any{int16(1), int64(10), make([]byte, 9)},
		)},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- CleanupDeleted(ctx, pool, CleanupOptions{
			Interval: 2 * time.Millisecond,
			OnPass: func(res CleanupResult) {
				mu.Lock()
				defer mu.Unlock()
				passes = append(passes, res)
			},
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats().selects < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до третьего прохода: %+v", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("CleanupDeleted: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// После отмены возможен лишний пустой проход (тир между опросом
	// счётчика и cancel) — детерминированы первый итог и отсутствие
	// данных ошибочного прохода: все проходы после первого нулевые.
	if len(passes) < 2 {
		t.Fatalf("проходов в хуке = %d (%+v), want ≥2", len(passes), passes)
	}
	wantFirst := CleanupResult{Domains: 1, Results: 2, Contents: 1, BadHashes: 1}
	if passes[0] != wantFirst {
		t.Errorf("итог первого прохода = %+v, want %+v", passes[0], wantFirst)
	}
	for i, p := range passes[1:] {
		if p != (CleanupResult{}) {
			t.Errorf("проход %d после первого = %+v, want нулевой (ошибочный проход хуку не отдаётся)", i+1, p)
		}
	}
}

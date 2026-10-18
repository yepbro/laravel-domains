// Юнит-тесты коллектора переключений DNS на резерв (T-107): no-op на
// nil-зависимостях, экспорт первого снимка в счётчики, накопление
// дельтами между проходами и перезапуск источника (отрицательная дельта).
// Источник состояния — замыкание над мутируемым снимком (FallbackStatsFunc).

package dnsworker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
)

// fallbackCounterValue — значение counter-метрики name (без меток, кроме
// сервисной): первый ряд семейства.
func fallbackCounterValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
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
	return -1 // метрика не найдена — тест провалит сравнением
}

// mutateStats — мутируемый источник снимков: коллектор в фоне, тест
// меняет значения между проходами.
type mutateStats struct {
	mu  sync.Mutex
	val dnsclient.FallbackStats
}

func (m *mutateStats) set(v dnsclient.FallbackStats) {
	m.mu.Lock()
	m.val = v
	m.mu.Unlock()
}

func (m *mutateStats) get() dnsclient.FallbackStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.val
}

func TestCollectFallbackLoopNoop(t *testing.T) {
	// Без реестра или источника — no-op, не паникует и не регистрирует.
	if err := CollectFallbackLoop(context.Background(), nil, func() dnsclient.FallbackStats { return dnsclient.FallbackStats{} }, time.Millisecond, nil); err != nil {
		t.Fatalf("nil-реестр: %v", err)
	}
	reg := metrics.New("dns-worker")
	if err := CollectFallbackLoop(context.Background(), reg, nil, time.Millisecond, nil); err != nil {
		t.Fatalf("nil-источник: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if strings.HasPrefix(mf.GetName(), "crawler_dns_fallback_") {
			t.Fatalf("приборы переключений зарегистрированы без источника: %s", mf.GetName())
		}
	}
}

func TestCollectFallbackLoopExportsAndDeltas(t *testing.T) {
	reg := metrics.New("dns-worker")
	src := &mutateStats{val: dnsclient.FallbackStats{Switches: 5, Exhausted: 2}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- CollectFallbackLoop(ctx, reg, src.get, time.Millisecond, nil)
	}()

	// Первый проход — сразу при старте: снимок {5, 2}.
	waitFor(t, func() bool {
		return fallbackCounterValue(t, reg, "crawler_dns_fallback_switches_total") == 5 &&
			fallbackCounterValue(t, reg, "crawler_dns_fallback_exhausted_total") == 2
	})

	// Источник накопил ещё: счётчики растут дельтой, а не переприсваиваются.
	src.set(dnsclient.FallbackStats{Switches: 8, Exhausted: 3})
	waitFor(t, func() bool {
		return fallbackCounterValue(t, reg, "crawler_dns_fallback_switches_total") == 8 &&
			fallbackCounterValue(t, reg, "crawler_dns_fallback_exhausted_total") == 3
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CollectFallbackLoop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("коллектор не завершился после отмены ctx")
	}
}

// TestCollectFallbackLoopSourceRestart — счётчик источника «уменьшился»
// (цепь пересоздали): прометеевский счётчик монотонен, добавляется
// значение нового снимка целиком (5 → 3 даёт 5+3).
func TestCollectFallbackLoopSourceRestart(t *testing.T) {
	reg := metrics.New("dns-worker")
	src := &mutateStats{}
	src.set(dnsclient.FallbackStats{Switches: 5, Exhausted: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- CollectFallbackLoop(ctx, reg, src.get, time.Millisecond, nil)
	}()

	waitFor(t, func() bool {
		return fallbackCounterValue(t, reg, "crawler_dns_fallback_switches_total") == 5
	})
	src.set(dnsclient.FallbackStats{Switches: 3, Exhausted: 1})
	waitFor(t, func() bool {
		return fallbackCounterValue(t, reg, "crawler_dns_fallback_switches_total") == 8 &&
			fallbackCounterValue(t, reg, "crawler_dns_fallback_exhausted_total") == 3
	})

	cancel()
	<-done
}

// waitFor — опрос условия до 2 с (коллектор тикает в фоне).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("условие коллектора не выполнено за 2 с")
	}
}

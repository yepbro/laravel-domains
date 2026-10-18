// Юнит-тесты коллектора состояния лимитёра DNS (T-077): no-op на
// nil-зависимостях, экспорт снимка LimitStats в gauges по метке kind,
// чистый выход по отмене ctx. Источник состояния — замыкание над
// заготовленным снимком (LimitStatsFunc).
package dnsworker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
)

// limitGaugeValue — значение gauge-метрики name без меток (кроме
// сервисной): первый ряд семейства.
func limitGaugeValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
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
			if ga := m.GetGauge(); ga != nil {
				return ga.GetValue()
			}
		}
	}
	return -1 // метрика не найдена — тест провалит сравнением
}

func TestCollectLimitLoopNoop(t *testing.T) {
	// Без реестра или источника — no-op, не паникует и не регистрирует.
	if err := CollectLimitLoop(context.Background(), nil, func() dnsclient.LimitStats { return dnsclient.LimitStats{} }, time.Millisecond, nil); err != nil {
		t.Fatalf("nil-реестр: %v", err)
	}
	reg := metrics.New("dns-worker")
	if err := CollectLimitLoop(context.Background(), reg, nil, time.Millisecond, nil); err != nil {
		t.Fatalf("nil-источник: %v", err)
	}
	// Базовые метрики каркаса (service_up, build_info, Go/runtime) в
	// реестре есть, приборов лимитёра быть не должно.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if strings.HasPrefix(mf.GetName(), "crawler_dns_") {
			t.Fatalf("приборы лимитёра зарегистрированы без источника: %s", mf.GetName())
		}
	}
}

func TestCollectLimitLoopExportsStats(t *testing.T) {
	reg := metrics.New("dns-worker")
	// Снимок «адаптив на дне»: 2/2000, 100/10000, окно 150/200 таймаутов.
	snap := dnsclient.LimitStats{
		Concurrency:    2,
		ConcurrencyCfg: 2000,
		QPS:            100,
		QPSCfg:         10000,
		InFlight:       7,
		WindowFilled:   true,
		WindowSize:     200,
		WindowTimeouts: 150,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- CollectLimitLoop(ctx, reg, func() dnsclient.LimitStats { return snap }, time.Millisecond, nil)
	}()

	// Первый проход — сразу при старте: ждём появления значений.
	deadline := time.Now().Add(2 * time.Second)
	for dnsMetricValue(t, reg, "crawler_dns_limit_concurrency", limitKindEffective) < 0 &&
		time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CollectLimitLoop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("коллектор не завершился после отмены ctx")
	}

	if got := dnsMetricValue(t, reg, "crawler_dns_limit_concurrency", limitKindEffective); got != 2 {
		t.Errorf("crawler_dns_limit_concurrency{effective} = %v, want 2", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_dns_limit_concurrency", limitKindConfigured); got != 2000 {
		t.Errorf("crawler_dns_limit_concurrency{configured} = %v, want 2000", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_dns_limit_qps", limitKindEffective); got != 100 {
		t.Errorf("crawler_dns_limit_qps{effective} = %v, want 100", got)
	}
	if got := dnsMetricValue(t, reg, "crawler_dns_limit_qps", limitKindConfigured); got != 10000 {
		t.Errorf("crawler_dns_limit_qps{configured} = %v, want 10000", got)
	}
	if got := limitGaugeValue(t, reg, "crawler_dns_limit_timeout_ratio"); got != 0.75 {
		t.Errorf("crawler_dns_limit_timeout_ratio = %v, want 0.75", got)
	}
	if got := limitGaugeValue(t, reg, "crawler_dns_inflight"); got != 7 {
		t.Errorf("crawler_dns_inflight = %v, want 7", got)
	}
}

// TestCollectLimitLoopRatioEmptyWindow — окно ещё наполняется: доля 0,
// а не паника деления на ноль.
func TestCollectLimitLoopRatioEmptyWindow(t *testing.T) {
	reg := metrics.New("dns-worker")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- CollectLimitLoop(ctx, reg, func() dnsclient.LimitStats {
			return dnsclient.LimitStats{Concurrency: 2000, ConcurrencyCfg: 2000}
		}, time.Hour, nil) // первый проход — сразу, тики не нужны
	}()

	deadline := time.Now().Add(2 * time.Second)
	for limitGaugeValue(t, reg, "crawler_dns_limit_timeout_ratio") < 0 &&
		time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if got := limitGaugeValue(t, reg, "crawler_dns_limit_timeout_ratio"); got != 0 {
		t.Errorf("crawler_dns_limit_timeout_ratio пустого окна = %v, want 0", got)
	}
}

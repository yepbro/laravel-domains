// Unit tests of the queue length collector (T-077, spec 5.9) on a fake
// XLEN source: the gauge value export through Gather, the independence of
// the stream errors, the interval defaults, a no-op without a registry,
// the loop (an immediate first pass, surviving an error, a clean exit on
// the ctx cancellation).
package metrics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeLen — a LenFunc over a map of streams; errLen — the streams
// answering with an error. The map changes on the live loop — the access
// is under the lock.
type fakeLen struct {
	mu     sync.Mutex
	lens   map[string]int64
	errLen map[string]error
	calls  []string
}

func (f *fakeLen) Len(_ context.Context, stream string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, stream)
	if err, ok := f.errLen[stream]; ok {
		return 0, err
	}
	return f.lens[stream], nil
}

// set — the stream value (a mutation under the lock: the loop reads in a
// goroutine).
func (f *fakeLen) set(stream string, n int64) {
	f.mu.Lock()
	f.lens[stream] = n
	f.mu.Unlock()
}

// fail — the stream answers with an error; an empty error — success.
func (f *fakeLen) fail(stream string, err error) {
	f.mu.Lock()
	if err == nil {
		delete(f.errLen, stream)
	} else {
		f.errLen[stream] = err
	}
	f.mu.Unlock()
}

// gaugeValue — the current value of the gauge series (no series —
// ok=false).
func gaugeValue(t *testing.T, g prometheus.Gatherer, name, queue string) (float64, bool) {
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
			for _, l := range m.GetLabel() {
				if l.GetName() == LabelQueue && l.GetValue() == queue {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func TestCollectQueueDepths(t *testing.T) {
	reg := New("dns-worker")
	gauge := NewQueueGauge(reg)
	lenOf := &fakeLen{
		lens:   map[string]int64{"q:dns": 42, "q:http": 7},
		errLen: map[string]error{"q:render": errors.New("redis недоступен")},
	}

	err := CollectQueueDepths(context.Background(), gauge, lenOf.Len,
		[]string{"q:dns", "q:http", "q:render"})
	if err == nil || !strings.Contains(err.Error(), "q:render") {
		t.Fatalf("ошибка = %v, want сбой q:render", err)
	}

	// The successful streams were recorded despite the failure of the
	// third.
	for queue, want := range map[string]float64{"q:dns": 42, "q:http": 7} {
		if v, ok := gaugeValue(t, reg, "crawler_queue_depth", queue); !ok || v != want {
			t.Errorf("crawler_queue_depth{queue=%q} = (%v, %v), want (%v, true)", queue, v, ok, want)
		}
	}
	if _, ok := gaugeValue(t, reg, "crawler_queue_depth", "q:render"); ok {
		t.Error("сбойный стрим не должен получать значение")
	}
	// The service label is substituted by the factory.
	if v, ok := gaugeValue(t, reg, "crawler_queue_depth", "q:dns"); ok && v == 42 {
		families, _ := reg.Gather()
		for _, mf := range families {
			if mf.GetName() == "crawler_queue_depth" {
				for _, m := range mf.GetMetric() {
					found := false
					for _, l := range m.GetLabel() {
						if l.GetName() == LabelService && l.GetValue() == "dns-worker" {
							found = true
						}
					}
					if !found {
						t.Error("метка service не подставлена фабрикой")
					}
				}
			}
		}
	}

	// A repeated pass with a new value overwrites the gauge.
	lenOf.set("q:dns", 43)
	if err := CollectQueueDepths(context.Background(), gauge, lenOf.Len, []string{"q:dns"}); err != nil {
		t.Fatal(err)
	}
	if v, _ := gaugeValue(t, reg, "crawler_queue_depth", "q:dns"); v != 43 {
		t.Errorf("повторный проход: значение = %v, want 43", v)
	}
}

func TestCollectQueueDepthsValidation(t *testing.T) {
	reg := New("test-svc")
	if err := CollectQueueDepths(context.Background(), nil, func(context.Context, string) (int64, error) { return 0, nil }, nil); err == nil {
		t.Error("gauge nil: ожидалась ошибка")
	}
	if err := CollectQueueDepths(context.Background(), NewQueueGauge(reg), nil, nil); err == nil {
		t.Error("lenOf nil: ожидалась ошибка")
	}
}

// waitForGauge — waiting for a value to appear (the loop runs in a
// goroutine).
func waitForGauge(t *testing.T, g prometheus.Gatherer, queue string, want float64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := gaugeValue(t, g, "crawler_queue_depth", queue); ok && v == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("crawler_queue_depth{queue=%q} не достигла %v за 2 с", queue, want)
}

func TestCollectQueueLoop(t *testing.T) {
	reg := New("http-worker")
	lenOf := &fakeLen{lens: map[string]int64{"q:http": 5}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = CollectQueueLoop(ctx, reg, lenOf.Len, []string{"q:http"}, time.Millisecond, nil)
		close(done)
	}()

	// The first pass — right at the start, without waiting for a tick.
	waitForGauge(t, reg, "q:http", 5)

	// A pass on a tick updates the value.
	lenOf.set("q:http", 6)
	waitForGauge(t, reg, "q:http", 6)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
}

func TestCollectQueueLoopNoop(t *testing.T) {
	// A nil registry, a nil source and an empty stream set — a no-op
	// without a panic and without calls to the source (isolated builds).
	lenOf := &fakeLen{lens: map[string]int64{"q:dns": 1}}
	for _, tc := range []struct {
		name    string
		reg     *Registry
		lenOf   LenFunc
		streams []string
	}{
		{"реестр nil", nil, lenOf.Len, []string{"q:dns"}},
		{"источник nil", New("test-svc"), nil, []string{"q:dns"}},
		{"стримы пусты", New("test-svc"), lenOf.Len, nil},
	} {
		if err := CollectQueueLoop(context.Background(), tc.reg, tc.lenOf, tc.streams, time.Millisecond, nil); err != nil {
			t.Errorf("%s: no-op вернул ошибку %v", tc.name, err)
		}
	}
	if len(lenOf.calls) != 0 {
		t.Errorf("обращения к источнику = %v, want 0", lenOf.calls)
	}
}

// Surviving an error: the loop lives, the gauge keeps the last value.
func TestCollectQueueLoopSurvivesError(t *testing.T) {
	reg := New("render-worker")
	boom := errors.New("redis недоступен")
	lenOf := &fakeLen{lens: map[string]int64{"q:render": 9}, errLen: map[string]error{}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = CollectQueueLoop(ctx, reg, lenOf.Len, []string{"q:render"}, time.Millisecond, nil)
		close(done)
	}()
	waitForGauge(t, reg, "q:render", 9)

	// Several failing passes: the value stays the same.
	lenOf.fail("q:render", boom)
	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if v, _ := gaugeValue(t, reg, "crawler_queue_depth", "q:render"); v != 9 {
		t.Errorf("при сбоях XLEN значение = %v, want последнее известное 9", v)
	}

	// The source recovers — the value is updated.
	lenOf.fail("q:render", nil)
	lenOf.set("q:render", 11)
	waitForGauge(t, reg, "q:render", 11)
}

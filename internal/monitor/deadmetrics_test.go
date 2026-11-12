// Unit tests of the q:dead depth metric (T-154): the wrapper mirrors only
// q:dead into crawler_queue_depth{queue}; the working streams — pass-through without
// a mirror (their depth is published by the workers, T-077); a read error
// does not touch the metric; a nil registry — pure pass-through.
package monitor

import (
	"context"
	"errors"
	"testing"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

func TestDeadDepthMetrics(t *testing.T) {
	reg := metrics.New("monitor")
	src := &fakeLener{lens: map[string]int64{queue.StreamDead: 7, queue.StreamHTTP: 3}}
	m := NewDeadDepthMetrics(reg, src)

	// A working stream — pass-through: the value goes through, no row appears
	// (the q:http depth is published by http-worker; a duplicate from monitor would
	// double the PromQL aggregates).
	n, err := m.Len(context.Background(), queue.StreamHTTP)
	if n != 3 || err != nil {
		t.Fatalf("Len(q:http) = (%v, %v), want транзит (3, nil)", n, err)
	}
	if _, ok := gaugeLabels(t, reg, "crawler_queue_depth", map[string]string{"queue": queue.StreamHTTP}); ok {
		t.Error("рабочий стрим зеркалится в crawler_queue_depth")
	}

	// q:dead — pass-through and mirror.
	if n, err := m.Len(context.Background(), queue.StreamDead); n != 7 || err != nil {
		t.Fatalf("Len(q:dead) = (%v, %v), want (7, nil)", n, err)
	}
	if v, ok := gaugeLabels(t, reg, "crawler_queue_depth", map[string]string{"queue": queue.StreamDead}); !ok || v != 7 {
		t.Errorf("crawler_queue_depth{queue=q:dead} = (%v, %v), want 7 (monitor — наблюдатель карантина)", v, ok)
	}

	// A new value overwrites the gauge; a read error — holds.
	src.lens[queue.StreamDead] = 9
	if _, err := m.Len(context.Background(), queue.StreamDead); err != nil {
		t.Fatal(err)
	}
	src.errs = []error{errors.New("connection refused")}
	if _, err := m.Len(context.Background(), queue.StreamDead); err == nil {
		t.Fatal("ошибка источника не прокинута")
	}
	if v, ok := gaugeLabels(t, reg, "crawler_queue_depth", map[string]string{"queue": queue.StreamDead}); !ok || v != 9 {
		t.Errorf("после ошибки глубина = (%v, %v), want последняя известная 9", v, ok)
	}

	// A nil registry — pure pass-through for all streams.
	plain := NewDeadDepthMetrics(nil, src)
	src.errs = nil
	if n, err := plain.Len(context.Background(), queue.StreamDead); n != 9 || err != nil {
		t.Errorf("транзит без реестра = (%v, %v), want источник как есть", n, err)
	}
}

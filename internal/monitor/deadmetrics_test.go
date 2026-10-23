// Юнит-тесты метрики глубины q:dead (T-154): обёртка зеркалит только
// q:dead в crawler_queue_depth{queue}, рабочие стримы — транзит без
// зеркала (их глубину публикуют воркеры, T-077), ошибка чтения метрику
// не трогает, nil-реестр — чистый транзит.
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

	// Рабочий стрим — транзит: значение проходит, ряд не появляется
	// (глубину q:http публикует http-worker, дубль из monitor удваивал
	// бы PromQL-агрегаты).
	n, err := m.Len(context.Background(), queue.StreamHTTP)
	if n != 3 || err != nil {
		t.Fatalf("Len(q:http) = (%v, %v), want транзит (3, nil)", n, err)
	}
	if _, ok := gaugeLabels(t, reg, "crawler_queue_depth", map[string]string{"queue": queue.StreamHTTP}); ok {
		t.Error("рабочий стрим зеркалится в crawler_queue_depth")
	}

	// q:dead — транзит и зеркало.
	if n, err := m.Len(context.Background(), queue.StreamDead); n != 7 || err != nil {
		t.Fatalf("Len(q:dead) = (%v, %v), want (7, nil)", n, err)
	}
	if v, ok := gaugeLabels(t, reg, "crawler_queue_depth", map[string]string{"queue": queue.StreamDead}); !ok || v != 7 {
		t.Errorf("crawler_queue_depth{queue=q:dead} = (%v, %v), want 7 (monitor — наблюдатель карантина)", v, ok)
	}

	// Новое значение перезаписывает gauge, ошибка чтения — держит.
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

	// nil-реестр — чистый транзит для всех стримов.
	plain := NewDeadDepthMetrics(nil, src)
	src.errs = nil
	if n, err := plain.Len(context.Background(), queue.StreamDead); n != 9 || err != nil {
		t.Errorf("транзит без реестра = (%v, %v), want источник как есть", n, err)
	}
}

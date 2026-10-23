// Юнит-тесты метрик цикла retention (T-166): накопление итогов проходов
// в счётчики (строки/ссылки/contents/байты), пропуски интерлока меткой
// passes{result}, ошибки чтения защиты очередей (passes{skipped_pending}
// + pending_errors), исключённые защитой записи по стримам
// (deferred{stream}), аномалии учёта, no-op без реестра, идемпотентность
// конструктора (T-076).
package content

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// retentionCounterValue — значение счётчика name (без меток, кроме
// сервисной): первый ряд семейства; -1, если метрика не экспортирована
// (стиль cleanupCounterValue в ingest).
func retentionCounterValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
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

// retentionCounterByLabels — значение счётчика name с точным набором
// меток (кроме сервисной); -1, если ряд не найден.
func retentionCounterByLabels(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	nextMetric:
		for _, m := range mf.GetMetric() {
			got := make(map[string]string)
			for _, lp := range m.GetLabel() {
				if lp.GetName() == metrics.LabelService {
					continue
				}
				got[lp.GetName()] = lp.GetValue()
			}
			if len(got) != len(labels) {
				continue
			}
			for k, v := range labels {
				if got[k] != v {
					continue nextMetric
				}
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// TestRetentionMetricsObserve — итоги проходов накапливаются в счётчики,
// нулевые значения не меняют, интерлок считается отдельно (skips +
// passes{result="skipped"}), рабочие проходы — passes{result="ok"}.
func TestRetentionMetricsObserve(t *testing.T) {
	reg := metrics.New("http-worker")
	m := NewRetentionMetrics(reg)

	m.Observe(RetentionStats{}, time.Second) // пустой проход: счётчики не растут
	for _, name := range []string{
		"crawler_content_retention_rows_total",
		"crawler_content_retention_refs_total",
		"crawler_content_retention_contents_total",
		"crawler_content_retention_freed_bytes_total",
		"crawler_content_retention_bad_hashes_total",
		"crawler_content_retention_missing_refs_total",
	} {
		if got := retentionCounterValue(t, reg, name); got != 0 {
			t.Fatalf("%s после нулевого итога = %v, want 0", name, got)
		}
	}

	m.Observe(RetentionStats{Rows: 10, Refs: 8, Contents: 6, FreedBytes: 42000}, 2*time.Second)
	m.Observe(RetentionStats{Rows: 5, Refs: 4, Contents: 4, FreedBytes: 21000, BadHashes: 1, Missing: 2}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 15 {
		t.Errorf("rows = %v, want 15", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_refs_total"); got != 12 {
		t.Errorf("refs = %v, want 12", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_contents_total"); got != 10 {
		t.Errorf("contents = %v, want 10", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_freed_bytes_total"); got != 63000 {
		t.Errorf("freed = %v, want 63000", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_bad_hashes_total"); got != 1 {
		t.Errorf("bad_hashes = %v, want 1 (растёт только при аномалии)", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_missing_refs_total"); got != 2 {
		t.Errorf("missing = %v, want 2", got)
	}
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_passes_total", map[string]string{"result": "ok"}); got != 3 {
		t.Errorf("passes{ok} = %v, want 3 (включая пустой)", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_skips_total"); got != 0 {
		t.Errorf("skips = %v, want 0", got)
	}

	// Пропуск интерлока: счётчики работы не растут (работы не было),
	// skips и метка skipped.
	m.Observe(RetentionStats{Skipped: true, Depth: 150}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 15 {
		t.Errorf("rows после пропуска = %v, want прежние 15", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_skips_total"); got != 1 {
		t.Errorf("skips = %v, want 1", got)
	}
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_passes_total", map[string]string{"result": "skipped"}); got != 1 {
		t.Errorf("passes{skipped} = %v, want 1", got)
	}

	// Пропуск посреди дренажа НЕ теряет сделанной работы (fix ревью:
	// пачки-то зафиксированы в БД): интерлок после части пачек —
	// счётчики работы зачитываются и в пропущенном проходе.
	m.Observe(RetentionStats{Skipped: true, Depth: 150, Rows: 7, Refs: 7, Contents: 5, FreedBytes: 1000}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 22 {
		t.Errorf("rows = %v, want 22 (15 + 7 из пропущенного посреди дренажа)", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_contents_total"); got != 15 {
		t.Errorf("contents = %v, want 15 (10 + 5)", got)
	}

	// Пропуск по ошибке чтения стримов защиты (T-166/T-167): отдельная
	// метка passes{skipped_pending} и счётчик ошибок; skips интерлока
	// не растёт — причины разные, алерты раздельные.
	m.Observe(RetentionStats{SkippedPending: true}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_pending_errors_total"); got != 1 {
		t.Errorf("pending_errors = %v, want 1", got)
	}
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_passes_total", map[string]string{"result": "skipped_pending"}); got != 1 {
		t.Errorf("passes{skipped_pending} = %v, want 1", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_skips_total"); got != 2 {
		t.Errorf("skips = %v, want прежние 2 (интерлок ни при чём)", got)
	}

	// Исключённые защитой записи — по стримам: запись, ждущая оба стрима,
	// считается в каждом; работающие счётчики не трогают.
	m.Observe(RetentionStats{
		Rows:     2,
		Deferred: map[string]int64{"q:render": 1, "q:analyze": 2},
	}, time.Second)
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_deferred_total", map[string]string{"stream": "q:render"}); got != 1 {
		t.Errorf("deferred{q:render} = %v, want 1", got)
	}
	if got := retentionCounterByLabels(t, reg, "crawler_content_retention_deferred_total", map[string]string{"stream": "q:analyze"}); got != 2 {
		t.Errorf("deferred{q:analyze} = %v, want 2", got)
	}
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 24 {
		t.Errorf("rows = %v, want 24 (чистка шла вместе с исключениями)", got)
	}
}

// TestRetentionMetricsNoRegistry — без реестра коллектора нет (изолированные
// сборки), вызовы Observe — no-op без паники.
func TestRetentionMetricsNoRegistry(t *testing.T) {
	if m := NewRetentionMetrics(nil); m != nil {
		t.Fatalf("NewRetentionMetrics(nil) = %v, want nil", m)
	}
	NewRetentionMetrics(nil).Observe(RetentionStats{Rows: 7}, time.Second) // не паникует

	families, err := metrics.New("http-worker").Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == "crawler_content_retention_rows_total" {
			t.Fatalf("прибор зарегистрирован без реестра: %s", mf.GetName())
		}
	}
}

// TestRetentionMetricsIdempotent — повторная регистрация делит те же
// коллекторы (T-076): конструктор можно вызывать на каждом цикле.
func TestRetentionMetricsIdempotent(t *testing.T) {
	reg := metrics.New("http-worker")
	first := NewRetentionMetrics(reg)
	second := NewRetentionMetrics(reg)
	first.Observe(RetentionStats{Rows: 3}, time.Second)
	second.Observe(RetentionStats{Rows: 4}, time.Second)
	if got := retentionCounterValue(t, reg, "crawler_content_retention_rows_total"); got != 7 {
		t.Errorf("rows = %v, want 7 (один коллектор на два конструктора)", got)
	}
}

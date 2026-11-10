// Тесты триажа мутантов CI-прогона 09.11.2026 (половина A): добивки
// slowqueries.go — источник PGSlowQueries (пул не задан, SQL-контракт и
// параметры, дефолт лимита, ошибки Query/Scan/Err), согласование
// числительных по всему диапазону, ключ дебаунса, порог целыми
// секундами, метрики (метка unknown без application_name) и пины
// констант. Фейк пула — zonestatsFakePool (тот же интерфейс db.DB).
package monitor

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"crawler/internal/metrics"
)

func TestPGSlowQueries_NoPool(t *testing.T) {
	if _, err := (PGSlowQueries{}).SlowQueries(context.Background(), time.Second); err == nil {
		t.Error("пул не задан: хочу ошибку источника")
	}
}

// slowScanRow — строка снимка pg_stat_activity правильной ширины.
func slowScanRow(pid int, app string, sec float64, query string) []any {
	return []any{pid, app, sec, time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC), query}
}

// TestPGSlowQueries_Contract — точный SQL-текст и параметры: исключение
// своего application_name, порог секундами (float), лимит строк;
// нулевой/отрицательный лимит — дефолт.
func TestPGSlowQueries_Contract(t *testing.T) {
	pool := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	src := PGSlowQueries{Pool: pool, ExcludeApp: "monitor"}
	if _, err := src.SlowQueries(context.Background(), 3*time.Second); err != nil {
		t.Fatalf("SlowQueries: %v", err)
	}
	calls := pool.calls()
	if len(calls) != 1 || calls[0].sql != slowQuerySQL {
		t.Fatalf("запросов = %d, want 1 с текстом slowQuerySQL", len(calls))
	}
	want := []any{"monitor", 3.0, 20}
	got := calls[0].args
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("аргументы = %#v, want %#v (дефолт лимита 20)", got, want)
	}

	pool2 := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	if _, err := (PGSlowQueries{Pool: pool2, Limit: 7}).SlowQueries(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	if got := pool2.calls()[0].args[2]; got != 7 {
		t.Errorf("явный лимит = %v, want 7", got)
	}
	for _, lim := range []int{0, -1} {
		pool3 := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
		if _, err := (PGSlowQueries{Pool: pool3, Limit: lim}).SlowQueries(context.Background(), time.Second); err != nil {
			t.Fatal(err)
		}
		if got := pool3.calls()[0].args[2]; got != 20 {
			t.Errorf("лимит %d → %v, want дефолт 20", lim, got)
		}
	}
}

// TestPGSlowQueries_Errors — ошибки Query/Scan/Err роняют источник.
func TestPGSlowQueries_Errors(t *testing.T) {
	src := PGSlowQueries{Pool: &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}}
	if q, err := src.SlowQueries(context.Background(), time.Second); err == nil || q != nil {
		t.Errorf("Query-ошибка: (%v, %v), want nil и ошибка", q, err)
	}
	// Строка не той ширины — Scan-ошибка с контекстом «чтение».
	narrow := &zonestatsFakePool{steps: []zonestatsStep{
		{rows: zonestatsRows([]any{7, "app"})}}}
	if _, err := (PGSlowQueries{Pool: narrow}).SlowQueries(context.Background(), time.Second); err == nil ||
		!strings.Contains(err.Error(), "чтение") {
		t.Errorf("Scan-ошибка = %v, want контекст чтения", err)
	}
	// Ошибка итерации после строки — ошибка источника.
	r := &zonestatsFakeRows{rows: [][]any{slowScanRow(11, "scheduler", 42.3, "SELECT 1")}, err: errors.New("обрыв")}
	if _, err := (PGSlowQueries{Pool: &zonestatsFakePool{steps: []zonestatsStep{{rows: r}}}}).
		SlowQueries(context.Background(), time.Second); err == nil ||
		!strings.Contains(err.Error(), "обрыв") {
		t.Errorf("rows.Err = %v, want исходная ошибка", err)
	}
}

// TestSlowQueriesPluralAll — согласование числительных по диапазону
// (дифференциальный прогон против эталона русской множественной формы).
func TestSlowQueriesPluralAll(t *testing.T) {
	plural := func(n int) string {
		switch {
		case n%10 == 1 && n%100 != 11:
			return "медленный запрос"
		case n%10 >= 2 && n%10 <= 4 && (n%100 < 10 || n%100 >= 20):
			return "медленных запроса"
		}
		return "медленных запросов"
	}
	for n := 0; n <= 300; n++ {
		if got := slowQueriesPhrase(n); got != plural(n) {
			t.Errorf("slowQueriesPhrase(%d) = %q, want %q", n, got, plural(n))
			return
		}
	}
}

// TestSlowQueryKeyExact — ключ дебаунса — pid и наносекунды старта
// десятичной записью.
func TestSlowQueryKeyExact(t *testing.T) {
	at := time.Date(2026, 10, 24, 12, 0, 0, 123456789, time.UTC)
	q := SlowQuery{PID: 42, StartedAt: at}
	want := "42@" + strconv.FormatInt(at.UnixNano(), 10)
	if got := q.key(); got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}

// TestSlowDurStringExact — целые секунды — без дробного хвоста, даже
// гигантские (мутант FormatFloat дал бы экспоненту).
func TestSlowDurStringExact(t *testing.T) {
	for d, want := range map[time.Duration]string{
		3 * time.Second:          "3",
		12 * time.Second:         "12",
		2500 * time.Millisecond:  "2.5",
		9200000000 * time.Second: "9200000000",
	} {
		if got := slowDurString(d); got != want {
			t.Errorf("slowDurString(%v) = %q, want %q", d, got, want)
		}
	}
}

// TestSlowQueryConsts — пины констант цикла.
func TestSlowQueryConsts(t *testing.T) {
	if DefaultSlowQueryInterval != 10*time.Second {
		t.Errorf("DefaultSlowQueryInterval = %v, want 10s", DefaultSlowQueryInterval)
	}
	if DefaultSlowQueryMinDuration != 3*time.Second {
		t.Errorf("DefaultSlowQueryMinDuration = %v, want 3s", DefaultSlowQueryMinDuration)
	}
	if slowQueryLimit != 20 {
		t.Errorf("slowQueryLimit = %d, want 20", slowQueryLimit)
	}
	if slowQueryTopLines != 3 {
		t.Errorf("slowQueryTopLines = %d, want 3", slowQueryTopLines)
	}
	if slowQueryMaxChars != 160 {
		t.Errorf("slowQueryMaxChars = %d, want 160", slowQueryMaxChars)
	}
}

// slowCounterValue — текущее значение счётчика с метками.
func slowCounterValue(t *testing.T, reg *metrics.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := make(map[string]string, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			equal := true
			for k, v := range labels {
				if got[k] != v {
					equal = false
				}
			}
			if equal {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestSlowQueryMetricsObserve — gauge текущих и счётчик по метке
// сервиса; пустая метка — unknown.
func TestSlowQueryMetricsObserve(t *testing.T) {
	reg := metrics.New("monitor")
	m := newSlowQueryMetrics(reg)
	m.observe(5, []SlowQuery{
		{App: "scheduler"},
		{App: ""},
	})
	if v, ok := gaugeLabels(t, reg, "crawler_pg_slow_queries", nil); !ok || v != 5 {
		t.Errorf("crawler_pg_slow_queries = (%v, %v), want 5", v, ok)
	}
	for app, want := range map[string]float64{"scheduler": 1, "unknown": 1} {
		if v, ok := slowCounterValue(t, reg, "crawler_pg_slow_queries_total", map[string]string{"app": app}); !ok || v != want {
			t.Errorf("total{app=%s} = (%v, %v), want %v", app, v, ok, want)
		}
	}
	// nil-приёмник — no-op без паники.
	var nilm *slowQueryMetrics
	nilm.observe(0, nil)
}

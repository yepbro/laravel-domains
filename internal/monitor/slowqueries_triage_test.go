// Triage tests of the mutants of the CI run of Nov 09 2026 (half A):
// the finishing work on slowqueries.go — the PGSlowQueries source
// (the pool not set, the SQL contract and the parameters, the limit
// default, the Query/Scan/Err errors), the numeral agreement over the
// whole range, the debounce key, the threshold as whole seconds, the
// metrics (the unknown label without an application_name) and the
// constant pins. The pool fake is zonestatsFakePool (the same db.DB
// interface).
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

// slowScanRow — a pg_stat_activity snapshot row of the right width.
func slowScanRow(pid int, app string, sec float64, query string) []any {
	return []any{pid, app, sec, time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC), query}
}

// TestPGSlowQueries_Contract — the exact SQL text and parameters: the
// exclusion of its own application_name, the threshold in seconds
// (a float), the row limit; a zero/negative limit — the default.
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

// TestPGSlowQueries_Errors — Query/Scan/Err errors fail the source.
func TestPGSlowQueries_Errors(t *testing.T) {
	src := PGSlowQueries{Pool: &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}}
	if q, err := src.SlowQueries(context.Background(), time.Second); err == nil || q != nil {
		t.Errorf("Query-ошибка: (%v, %v), want nil и ошибка", q, err)
	}
	// A row of the wrong width — a Scan error with the "reading" context.
	narrow := &zonestatsFakePool{steps: []zonestatsStep{
		{rows: zonestatsRows([]any{7, "app"})}}}
	if _, err := (PGSlowQueries{Pool: narrow}).SlowQueries(context.Background(), time.Second); err == nil ||
		!strings.Contains(err.Error(), "чтение") {
		t.Errorf("Scan-ошибка = %v, want контекст чтения", err)
	}
	// An iteration error after a row — a source error.
	r := &zonestatsFakeRows{rows: [][]any{slowScanRow(11, "scheduler", 42.3, "SELECT 1")}, err: errors.New("обрыв")}
	if _, err := (PGSlowQueries{Pool: &zonestatsFakePool{steps: []zonestatsStep{{rows: r}}}}).
		SlowQueries(context.Background(), time.Second); err == nil ||
		!strings.Contains(err.Error(), "обрыв") {
		t.Errorf("rows.Err = %v, want исходная ошибка", err)
	}
}

// TestSlowQueriesPluralAll — the numeral agreement over the whole range
// (a differential run against the reference Russian plural form).
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

// TestSlowQueryKeyExact — the debounce key — the pid and the start's
// nanoseconds in decimal notation.
func TestSlowQueryKeyExact(t *testing.T) {
	at := time.Date(2026, 10, 24, 12, 0, 0, 123456789, time.UTC)
	q := SlowQuery{PID: 42, StartedAt: at}
	want := "42@" + strconv.FormatInt(at.UnixNano(), 10)
	if got := q.key(); got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}

// TestSlowDurStringExact — whole seconds — without a fractional tail,
// even gigantic ones (a FormatFloat mutant would give an exponent).
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

// TestSlowQueryConsts — the pins of the loop constants.
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

// slowCounterValue — the current counter value with labels.
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

// TestSlowQueryMetricsObserve — the current gauge and the counter by
// the service label; an empty label — unknown.
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
	// A nil receiver — a no-op without a panic.
	var nilm *slowQueryMetrics
	nilm.observe(0, nil)
}

// Unit tests for the crawlerctl status output (T-079, spec 5.9): the table
// and JSON formats (null for "n/a"), routing/exit codes and the help text.
// The SQL contracts, the aggregate semantics and the calculations —
// internal/status package tests (T-126, dashboard spec §4.5); behavior
// against live PostgreSQL/Redis — the dev environment (make status).
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/status"
)

// --- shared pgx.Rows fake -------------------------------------------------------

// statusRows — pgx.Rows with pre-loaded rows (positional Scan, the
// int16/string/bool/int64/float32/float64/[]byte types). The shared fake
// of the crawlerctl command tests (zones, subdomains, near, recheck, …);
// born with status (T-079), the logic moved to internal/status (T-126) —
// the fake stays here, with the CLI tests.
type statusRows struct {
	rows [][]any
	i    int
}

func statusRowsOf(rows ...[]any) *statusRows { return &statusRows{rows: rows} }

func (r *statusRows) Close()                                       {}
func (r *statusRows) Err() error                                   { return nil }
func (r *statusRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *statusRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *statusRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *statusRows) RawValues() [][]byte                          { return nil }
func (r *statusRows) Conn() *pgx.Conn                              { return nil }
func (r *statusRows) TypeMap() *pgtype.Map                         { return nil }

func (r *statusRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *statusRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("statusRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("statusRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *int16:
			*dst = v.(int16)
		case *string:
			*dst = v.(string)
		case *bool:
			*dst = v.(bool)
		case *int64:
			*dst = v.(int64)
		case *float32:
			*dst = v.(float32)
		case *float64:
			*dst = v.(float64)
		case *[]byte:
			*dst = v.([]byte)
		default:
			return fmt.Errorf("statusRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// --- output ----------------------------------------------------------------------

// TestPercentMark — the checked share; an empty zone — "—".
func TestPercentMark(t *testing.T) {
	cases := []struct {
		checked, total int64
		want           string
	}{
		{40, 100, "40%"},
		{1, 3, "33%"}, // share rounded down to whole percents
		{0, 0, "—"},
		{5, 0, "—"},
	}
	for _, tc := range cases {
		if got := percentMark(tc.checked, tc.total); got != tc.want {
			t.Errorf("percentMark(%d, %d) = %q, want %q", tc.checked, tc.total, got, tc.want)
		}
	}
}

// statusTestSnapshot — a consistent dataset for the output tests:
// two zones (one without domains), measurements and rates from TestComputeRates.
func statusTestSnapshot() (zones []status.ZoneStats, tot status.Totals, thr status.Throughput, rates status.Rates) {
	zones = []status.ZoneStats{
		{ID: 1, Name: "crawl.test", IsActive: true, DomainsTotal: 100, DomainsChecked: 40, Pending: 60, Queued: 2, Done: 38},
		// A zone without domains (LEFT JOIN): the monitor counter is left
		// over from a previous filling — it still enters the totals.
		{ID: 3, Name: "empty.test", IsActive: false, DomainsTotal: 10, DomainsChecked: 0},
	}
	tot = status.ComputeTotals(zones)
	thr = status.Throughput{DoneToday: 3600, ElapsedSec: 3600, ResultsHour: 7200, RenderedHour: 3600, HourWindowSec: 3600}
	rates = status.ComputeRates(thr, tot.Pending, tot.Queued) // 1 domain/s → ETA 62s
	return zones, tot, thr, rates
}

// TestWriteStatusTable — the format: the zone header and columns, totals,
// queues across all streams, the rate with windows, the cycle with the ETA;
// right-aligned numbers do not break the totals columns.
func TestWriteStatusTable(t *testing.T) {
	zones, tot, thr, rates := statusTestSnapshot()
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:analyze": 3, "q:dead": 1}

	var b bytes.Buffer
	writeStatusTable(&b, zones, tot, thr, rates, lens)
	got := b.String()

	for _, want := range []string{
		"зона", "pending", "queued", "done", // header
		"crawl.test +", "empty.test -",
		"60", "38", "итого", "40%", // aggregates and totals
		"q:dns 5", "q:render 12", "q:dead 1", // queues
		"доменов/с          1.0   (3600 проверено с начала суток, прошло 1h 00m)",
		"HTTP-результатов/с 2.0   (7200 за окно темпа 1h 00m)",
		"рендеров/с         1.0   (3600 с итоговым DOM за то же окно)",
		"цикл: осталось 62 (pending+queued); ETA ≈ 1м (по 1.0 дом./с)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("таблица не содержит %q:\n%s", want, got)
		}
	}

	// The totals row is right-aligned with the numeric columns: the width
	// of the total column is at least the longest value (100); the
	// totals (110) widen it; the totals row ends with the done numbers.
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "итого") {
			if !strings.HasSuffix(line, "38") {
				t.Errorf("строка итога не кончается done: %q", line)
			}
			if !strings.Contains(line, "110") {
				t.Errorf("строка итога без суммы всего: %q", line)
			}
		}
	}
}

// TestWriteStatusTableDegraded — Redis is unavailable: queues are "n/a",
// the DB summary is printed; an empty cycle is marked as completed.
func TestWriteStatusTableDegraded(t *testing.T) {
	_, tot, thr, _ := statusTestSnapshot()
	rates := status.ComputeRates(thr, 0, 0) // the cycle is empty

	var b bytes.Buffer
	writeStatusTable(&b, nil, tot, thr, rates, nil)
	got := b.String()
	for _, want := range []string{
		"н/д (Redis недоступен)",
		"итого",
		"доменов/с          1.0",
		"цикл: осталось 0 (pending и queued пусты) — проверять нечего",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("деградированный вывод не содержит %q:\n%s", want, got)
		}
	}

	// A short day window — the rate and the ETA honestly "n/a".
	thr = status.Throughput{ElapsedSec: 300, ResultsHour: 0}
	rates = status.ComputeRates(thr, 10, 0)
	b.Reset()
	writeStatusTable(&b, nil, status.Totals{}, thr, rates, nil)
	got = b.String()
	for _, want := range []string{
		"н/д   (окно с начала суток короче 10м)",
		"ETA н/д (скорость неизвестна или 0)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("вывод без скорости не содержит %q:\n%s", want, got)
		}
	}
}

// TestWriteStatusJSON — machine output: the zone/totals/queues/throughput
// fields, rate rounding, null for "n/a" (a nil queue map, rates without
// a window, an ETA without a rate).
func TestWriteStatusJSON(t *testing.T) {
	zones, tot, thr, rates := statusTestSnapshot()
	lens := map[string]int64{"q:dns": 5, "q:http": 0, "q:render": 12, "q:analyze": 3, "q:dead": 1}

	var b bytes.Buffer
	if err := writeStatusJSON(&b, zones, tot, thr, rates, lens); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}

	type zoneJSON struct {
		Name           string `json:"name"`
		NameUnicode    string `json:"name_unicode"`
		Active         bool   `json:"active"`
		DomainsTotal   int64  `json:"domains_total"`
		DomainsChecked int64  `json:"domains_checked"`
		Pending        int64  `json:"pending"`
		Queued         int64  `json:"queued"`
		Done           int64  `json:"done"`
	}
	var doc struct {
		Zones      []zoneJSON       `json:"zones"`
		Totals     map[string]int64 `json:"totals"`
		Queues     map[string]int64 `json:"queues"`
		Throughput struct {
			DoneToday     int64   `json:"done_today"`
			ElapsedSec    float64 `json:"elapsed_sec"`
			ResultsHour   int64   `json:"results_hour"`
			HourWindowSec float64 `json:"hour_window_sec"`
		} `json:"throughput"`
		Rates struct {
			DomainsPerSec *float64 `json:"domains_per_sec"`
			ResultsPerSec float64  `json:"results_per_sec"`
			Remaining     int64    `json:"remaining"`
			ETASec        *int64   `json:"eta_sec"`
		} `json:"rates"`
	}
	if err := json.Unmarshal(b.Bytes(), &doc); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, b.String())
	}
	if len(doc.Zones) != 2 || doc.Zones[0].Name != "crawl.test" || !doc.Zones[0].Active {
		t.Errorf("zones = %+v", doc.Zones)
	}
	if doc.Zones[1].NameUnicode != "" {
		t.Errorf("name_unicode пустой зоны = %q, хочу \"\" (omitempty вне теста)", doc.Zones[1].NameUnicode)
	}
	if doc.Totals["pending"] != 60 || doc.Totals["domains_total"] != 110 {
		t.Errorf("totals = %v", doc.Totals)
	}
	if doc.Queues["q:render"] != 12 || len(doc.Queues) != 5 {
		t.Errorf("queues = %v", doc.Queues)
	}
	if doc.Throughput.DoneToday != 3600 || doc.Throughput.ElapsedSec != 3600 {
		t.Errorf("throughput = %+v", doc.Throughput)
	}
	// The length of the rate bucket window is in the wire format (T-203):
	// a machine consumer sees what the results_hour sum is a multiple of.
	if doc.Throughput.HourWindowSec != 3600 {
		t.Errorf("hour_window_sec = %v, хочу 3600", doc.Throughput.HourWindowSec)
	}
	if doc.Rates.DomainsPerSec == nil || *doc.Rates.DomainsPerSec != 1 {
		t.Errorf("domains_per_sec = %v, хочу 1", doc.Rates.DomainsPerSec)
	}
	if doc.Rates.ResultsPerSec != 2 {
		t.Errorf("results_per_sec = %v, хочу 2", doc.Rates.ResultsPerSec)
	}
	if doc.Rates.ETASec == nil || *doc.Rates.ETASec != 62 {
		t.Errorf("eta_sec = %v, хочу 62", doc.Rates.ETASec)
	}

	// Degradation: a short window + unavailable Redis → null fields, the
	// remainder without an ETA; the json stays valid.
	b.Reset()
	thr = status.Throughput{ElapsedSec: 300}
	rates = status.ComputeRates(thr, 10, 0)
	if err := writeStatusJSON(&b, nil, status.Totals{}, thr, rates, nil); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}
	var deg struct {
		Zones  []struct{}       `json:"zones"`
		Queues map[string]int64 `json:"queues"`
		Rates  struct {
			DomainsPerSec *float64 `json:"domains_per_sec"`
			Remaining     int64    `json:"remaining"`
			ETASec        *int64   `json:"eta_sec"`
		} `json:"rates"`
	}
	if err := json.Unmarshal(b.Bytes(), &deg); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, b.String())
	}
	if len(deg.Zones) != 0 {
		t.Errorf("zones = %d, хочу пустой массив", len(deg.Zones))
	}
	if deg.Queues != nil {
		t.Errorf("queues = %v, хочу null", deg.Queues)
	}
	if deg.Rates.DomainsPerSec != nil || deg.Rates.ETASec != nil {
		t.Errorf("rates = %+v, хочу null/null", deg.Rates)
	}
	if deg.Rates.Remaining != 10 {
		t.Errorf("remaining = %d, хочу 10", deg.Rates.Remaining)
	}
}

// --- routing/exit codes --------------------------------------------------------------

// TestRunStatus — the execution path without infrastructure: the command
// needs a config (the PostgreSQL DSN, Redis) — without one it is an
// execution error (code 1) before connections (the TestRunReanalyze
// style); --json does not break the path.
func TestRunStatus(t *testing.T) {
	// An explicitly missing config file: a deterministic failure before
	// any connections (in the tools container the CRAWLER_* variables
	// could assemble a configuration from the environment — the test must
	// not touch the live stack).
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	var stdout, stderr bytes.Buffer
	code := run([]string{"status"}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitError, stderr.String())
	}
	if !strings.Contains(stderr.String(), "status: config:") {
		t.Errorf("stderr не содержит контекст конфигурации:\n%s", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout до соединения не пуст:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"status", "--json"}, &stdout, &stderr)
	if code != exitError || !strings.Contains(stderr.String(), "status: config:") {
		t.Errorf("--json: код = %d, stderr:\n%s; хочу ошибку конфигурации (код 1)", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("--json: stdout до соединения не пуст:\n%s", stdout.String())
	}
}

// TestStatusHelpMentionsStreams — the help lists all streams and the ETA
// (TestRunHelpPerCommand in main_test.go checks the full content); the
// throughput source is the hour buckets (T-203): the help must not promise
// a live "last hour" over subdomain_results (an outdated text — the T-203
// P2-1 review finding).
func TestStatusHelpMentionsStreams(t *testing.T) {
	for _, s := range status.Streams {
		if !strings.Contains(statusHelp, s) {
			t.Errorf("statusHelp не упоминает стрим %q", s)
		}
	}
	if !strings.Contains(statusHelp, "ETA") {
		t.Error("statusHelp не описывает ETA цикла")
	}
	if !strings.Contains(statusHelp, "часовые корзины crawler_counters") {
		t.Error("statusHelp не описывает источник темпа (часовые корзины, T-203)")
	}
	if strings.Contains(statusHelp, "последний час") {
		t.Error("statusHelp обещает «последний час» — окно темпа 1–2 ч из корзин (T-203)")
	}
}

// Tests of the canonical JSON format of the summary (dashboard spec
// §6.2, T-129): the wire contract is shared by the CLI crawlerctl status
// --json and the dashboard snapshot — the null rules (the queues without
// Redis, a rate without a window, an ETA without a rate), rounding to
// three digits, empty zones — "[]", not null. The full form of the
// dashboard snapshot (the embedded sections + the new §5.5–5.8) is
// checked by the internal/dashboard tests.
package status

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestBuildSummaryJSON — the form of the CLI --json: all the sections in
// place, a nil queue map — null, the rates without a window — null
// fields, rounding to three digits.
func TestBuildSummaryJSON(t *testing.T) {
	zones := []ZoneStats{
		{ID: 1, Name: "crawl.test", IsActive: true, DomainsTotal: 100, DomainsChecked: 40, Pending: 60, Queued: 2, Done: 38},
		{ID: 2, Name: "xn--80adjurfhd.test", NameUnicode: "проверка.test", DomainsTotal: 10},
	}
	tot := ComputeTotals(zones)
	thr := Throughput{DoneToday: 3599, ElapsedSec: 3600, ResultsHour: 7199, RenderedHour: 1, HourWindowSec: 3600}
	rates := ComputeRates(thr, tot.Pending, tot.Queued) // ≈0.9997 domains/s

	doc := BuildSummaryJSON(zones, tot, thr, rates, nil)
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := string(b)

	// The field names — literally the CLI contract.
	for _, want := range []string{
		`"zones":[{`,
		`"name":"crawl.test"`,
		`"name_unicode":"проверка.test"`,
		`"domains_total":10`,
		`"totals":{"domains_total":110,"domains_checked":40,"pending":60,"queued":2,"done":38}`,
		`"queues":null`, // Redis unavailable
		`"throughput":{"done_today":3599,"elapsed_sec":3600,"results_hour":7199,"rendered_hour":1,"hour_window_sec":3600}`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the JSON does not contain %s:\n%s", want, got)
		}
	}

	// Rounding to three digits: 3599/3600 = 0.99972… → 1.0; the hourly
	// rates — 7199/3600 = 1.99972… → 2; 1/3600 = 0.00027… → 0.
	r := doc.Rates
	if r.DomainsPerSec == nil || *r.DomainsPerSec != 1 {
		t.Errorf("domains_per_sec = %v, want 1 (3599/3600 rounds to 1)", r.DomainsPerSec)
	}
	if r.ResultsPerSec != 2 || r.RendersPerSec != 0 {
		t.Errorf("the hour rates = %v/%v, want 2/0", r.ResultsPerSec, r.RendersPerSec)
	}
	if r.Remaining != 62 {
		t.Errorf("remaining = %d, want 62", r.Remaining)
	}
	if r.ETASec == nil || *r.ETASec != 62 {
		t.Errorf("eta_sec = %v, want 62", r.ETASec)
	}

	// Empty zones — "[]", not null; the unicode of an empty zone is
	// omitted.
	empty := BuildSummaryJSON(nil, Totals{}, Throughput{ElapsedSec: 300}, ComputeRates(Throughput{ElapsedSec: 300}, 10, 0), nil)
	if empty.Zones == nil || len(empty.Zones) != 0 {
		t.Errorf("zones = %v, want an empty slice", empty.Zones)
	}
	if empty.Rates.DomainsPerSec != nil || empty.Rates.ETASec != nil {
		t.Errorf("a short window: rates = %+v, want null/null", *empty.Rates)
	}
	if empty.Rates.Remaining != 10 {
		t.Errorf("remaining = %d, want 10", empty.Rates.Remaining)
	}
}

// TestSummaryJSONFieldNames — the field names of the sections are pinned
// (the machine consumers of the CLI and the dashboard must not diverge,
// dashboard spec §6.2): the exact JSON keys of each section.
func TestSummaryJSONFieldNames(t *testing.T) {
	cases := []struct {
		typ  any
		want string
	}{
		{ZoneJSON{}, `{"name":"","active":false,"domains_total":0,"domains_checked":0,"pending":0,"queued":0,"done":0}`},
		{TotalsJSON{}, `{"domains_total":0,"domains_checked":0,"pending":0,"queued":0,"done":0}`},
		{ThroughputJSON{}, `{"done_today":0,"elapsed_sec":0,"results_hour":0,"rendered_hour":0,"hour_window_sec":0}`},
		{RatesJSON{}, `{"domains_per_sec":null,"results_per_sec":0,"renders_per_sec":0,"remaining":0,"eta_sec":null}`},
	}
	for _, tc := range cases {
		b, err := json.Marshal(tc.typ)
		if err != nil {
			t.Fatalf("Marshal(%T): %v", tc.typ, err)
		}
		if string(b) != tc.want {
			t.Errorf("%T = %s, want %s", tc.typ, b, tc.want)
		}
	}
}

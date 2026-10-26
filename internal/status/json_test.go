// Тесты канонического JSON-формата сводки (ТЗД §6.2, T-129): wire-контракт
// общий для CLI crawlerctl status --json и снимка дашборда — правила
// null (очереди без Redis, скорость без окна, ETA без скорости),
// округление до трёх знаков, пустые зоны — «[]», а не null. Полную форму
// снимка дашборда (встроенные секции + новые §5.5–5.8) проверяют тесты
// internal/dashboard.
package status

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestBuildSummaryJSON — форма CLI --json: все секции на месте, nil-карта
// очередей — null, скорости без окна — null-поля, округление до трёх
// знаков.
func TestBuildSummaryJSON(t *testing.T) {
	zones := []ZoneStats{
		{ID: 1, Name: "crawl.test", IsActive: true, DomainsTotal: 100, DomainsChecked: 40, Pending: 60, Queued: 2, Done: 38},
		{ID: 2, Name: "xn--80adjurfhd.test", NameUnicode: "проверка.test", DomainsTotal: 10},
	}
	tot := ComputeTotals(zones)
	thr := Throughput{DoneToday: 3599, ElapsedSec: 3600, ResultsHour: 7199, RenderedHour: 1, HourWindowSec: 3600}
	rates := ComputeRates(thr, tot.Pending, tot.Queued) // ≈0.9997 дом./с

	doc := BuildSummaryJSON(zones, tot, thr, rates, nil)
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := string(b)

	// Имена полей — дословно контракт CLI.
	for _, want := range []string{
		`"zones":[{`,
		`"name":"crawl.test"`,
		`"name_unicode":"проверка.test"`,
		`"domains_total":10`,
		`"totals":{"domains_total":110,"domains_checked":40,"pending":60,"queued":2,"done":38}`,
		`"queues":null`, // Redis недоступен
		`"throughput":{"done_today":3599,"elapsed_sec":3600,"results_hour":7199,"rendered_hour":1,"hour_window_sec":3600}`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("JSON не содержит %s:\n%s", want, got)
		}
	}

	// Округление до трёх знаков: 3599/3600 = 0.99972… → 1.0; часовые
	// скорости — 7199/3600 = 1.99972… → 2; 1/3600 = 0.00027… → 0.
	r := doc.Rates
	if r.DomainsPerSec == nil || *r.DomainsPerSec != 1 {
		t.Errorf("domains_per_sec = %v, хочу 1 (3599/3600 округляется до 1)", r.DomainsPerSec)
	}
	if r.ResultsPerSec != 2 || r.RendersPerSec != 0 {
		t.Errorf("скорости часа = %v/%v, хочу 2/0", r.ResultsPerSec, r.RendersPerSec)
	}
	if r.Remaining != 62 {
		t.Errorf("remaining = %d, хочу 62", r.Remaining)
	}
	if r.ETASec == nil || *r.ETASec != 62 {
		t.Errorf("eta_sec = %v, хочу 62", r.ETASec)
	}

	// Пустые зоны — «[]», не null; unicode пустой зоны опускается.
	empty := BuildSummaryJSON(nil, Totals{}, Throughput{ElapsedSec: 300}, ComputeRates(Throughput{ElapsedSec: 300}, 10, 0), nil)
	if empty.Zones == nil || len(empty.Zones) != 0 {
		t.Errorf("zones = %v, хочу пустой срез", empty.Zones)
	}
	if empty.Rates.DomainsPerSec != nil || empty.Rates.ETASec != nil {
		t.Errorf("короткое окно: rates = %+v, хочу null/null", *empty.Rates)
	}
	if empty.Rates.Remaining != 10 {
		t.Errorf("remaining = %d, хочу 10", empty.Rates.Remaining)
	}
}

// TestSummaryJSONFieldNames — имена полей секций закреплены (машинные
// потребители CLI и дашборда не должны расходиться, ТЗД §6.2): точные
// ключи JSON каждой секции.
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

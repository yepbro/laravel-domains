// Unit tests of the landing data (T-122) on the fake pool of
// web_test.go: the SQL contracts of the selects, the live fake
// semantics (the stats, the cloud with a log scale, the table with
// shares, the latest finds with mask badges), errors are not hidden.
package web

import (
	"context"
	"strings"
	"testing"
)

// TestLandingSQLContracts — the select texts are pinned: the T-120
// caches, the cloud/table LIMIT, the ORDER BY updated_at by the T-120
// index.
func TestLandingSQLContracts(t *testing.T) {
	for sql, want := range map[string]string{
		statsSQL:      "sum(domains_laravel)",
		last24hSQL:    "FROM laravel_history",
		cloudSQL:      "LIMIT 50",
		tableSQL:      "LIMIT 10",
		latestSQL:     "ORDER BY ld.updated_at DESC LIMIT 5",
		subdomainsSQL: "WHERE enabled",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("the SQL does not contain %q:\n%s", want, sql)
		}
	}
}

// TestFetchLandingData — the semantics: the stats sums, the cloud log
// scale (the rank preserved, the maximum — the top of the range), the
// table percentages, the found_on_mask badges and the find versions.
func TestFetchLandingData(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	data, err := FetchLandingData(context.Background(), db)
	if err != nil {
		t.Fatalf("FetchLandingData: %v", err)
	}

	if data.Stats.Zones != 12 || data.Stats.Total != 1_000_000 ||
		data.Stats.Checked != 800_000 || data.Stats.Laravel != 4_242 || data.Stats.Last24h != 17 {
		t.Errorf("the statistics = %+v", data.Stats)
	}

	if len(data.Cloud) != 3 {
		t.Fatalf("the cloud = %d zones, want 3", len(data.Cloud))
	}
	if data.Cloud[0].Size < 1.49 || data.Cloud[0].Size > 1.5 {
		t.Errorf("the cloud size maximum = %v, want ~1.5 (the top bound)", data.Cloud[0].Size)
	}
	if data.Cloud[2].Size <= 0.875 {
		t.Errorf("the cloud size minimum = %v, want > 0.875", data.Cloud[2].Size)
	}
	if !(data.Cloud[0].Size > data.Cloud[1].Size && data.Cloud[1].Size > data.Cloud[2].Size) {
		t.Errorf("the size rank is broken: %v", data.Cloud)
	}

	if len(data.Table) != 2 {
		t.Fatalf("the table = %d zones, want 2", len(data.Table))
	}
	if p := data.Table[0].Percent; p < 0.24 && p > 0.26 {
		t.Errorf("the com zone percentage = %v, want 0.25", p)
	}
	if data.Table[0].Checked != 800_000 {
		t.Errorf("checked = %d", data.Table[0].Checked)
	}

	if len(data.Latest) != 1 {
		t.Fatalf("the finds = %d, want 1", len(data.Latest))
	}
	it := data.Latest[0]
	if it.Name != "shop.example.com" || it.VerText != "9.19–12" {
		t.Errorf("the find = %+v", it)
	}
	// found_on_mask 0b11 — the apex (bit 0, no badge) and www (bit 1).
	if len(it.Subdomains) != 1 || it.Subdomains[0].Label != "www" ||
		it.Subdomains[0].Host != "www.shop.example.com" {
		t.Errorf("the badges = %+v, want www only", it.Subdomains)
	}
	if it.UpdatedText != "Oct 18, 2026" {
		t.Errorf("the date = %q", it.UpdatedText)
	}
}

// TestFetchLandingNilPool — no pool — an error.
func TestFetchLandingNilPool(t *testing.T) {
	if _, err := FetchLandingData(context.Background(), nil); err == nil {
		t.Fatal("a nil pool = nil error, want an error")
	}
}

// TestFetchLandingQueryError — a query error surfaces (a page without
// data does not render half-built).
func TestFetchLandingQueryError(t *testing.T) {
	if _, err := FetchLandingData(context.Background(), brokenDB{}); err == nil {
		t.Fatal("the DB layer unavailable = nil error, want an error")
	}
}

// TestDisplayUnicode — punycode → Unicode, ASCII as is.
func TestDisplayUnicode(t *testing.T) {
	if got := displayUnicode("xn--80ak6aa92e.com"); got == "xn--80ak6aa92e.com" {
		t.Errorf("the IDN is not decoded: %q", got)
	}
	if got := displayUnicode("example.com"); got != "example.com" {
		t.Errorf("the ASCII changed: %q", got)
	}
	if got := displayUnicode("!!"); got != "!!" {
		t.Errorf("the junk changed: %q", got)
	}
}

// Unit tests of the landing stats API (L-03): the JSON schema of the
// response (decoded as a generic document — the wire contract, not the
// Go structs), the empty-DB arrays (never null), a DB failure — a 500
// without details, non-GET methods — 405, the caching header; the data
// pass is single (the subdomain reference is not queried twice).
package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// statsDoc — the response body as a generic JSON document.
func statsDoc(t *testing.T, body string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("the response is not a JSON object: %v\n%s", err, body)
	}
	return doc
}

// statsObj — a nested object of the document.
func statsObj(t *testing.T, v any, key string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("%s is not a JSON object: %#v", key, v)
	}
	return m
}

// statsArr — an array of the document (null fails: the collections of
// the contract are [], never null).
func statsArr(t *testing.T, doc map[string]any, key string) []any {
	t.Helper()
	v, ok := doc[key]
	if !ok {
		t.Fatalf("the response without %q:\n%v", key, doc)
	}
	arr, ok := v.([]any)
	if !ok {
		t.Fatalf("%s is not a JSON array (null?): %#v", key, v)
	}
	return arr
}

// statsInt — a number field as int64.
func statsInt(t *testing.T, m map[string]any, key string) int64 {
	t.Helper()
	f, ok := m[key].(float64)
	if !ok {
		t.Fatalf("%q is not a number: %#v", key, m[key])
	}
	return int64(f)
}

// TestLandingStatsE2E — the happy path on the fake pool: the counters,
// the cloud (with the log-scale size and the Unicode zone name), the
// table with the share, the latest find with the badges and the
// versions, the subdomain reference — plus the response headers.
func TestLandingStatsE2E(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	srv := newTestServer(t, db)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/landing/stats")
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200:\n%s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "public, max-age=60" {
		t.Errorf("Cache-Control = %q, want public, max-age=60", cc)
	}
	doc := statsDoc(t, body)

	stats := statsObj(t, doc["stats"], "stats")
	for key, want := range map[string]int64{
		"zones": 12, "total": 1_000_000, "checked": 800_000,
		"laravel": 4_242, "laravel_last_24h": 17,
	} {
		if got := statsInt(t, stats, key); got != want {
			t.Errorf("stats.%s = %d, want %d", key, got, want)
		}
	}

	cloud := statsArr(t, doc, "cloud")
	if len(cloud) != 3 {
		t.Fatalf("the cloud = %d zones, want 3", len(cloud))
	}
	com := statsObj(t, cloud[0], "cloud[0]")
	if com["name"] != "com" || statsInt(t, com, "laravel") != 2000 {
		t.Errorf("cloud[0] = %v", com)
	}
	if _, ok := com["name_unicode"]; ok {
		t.Errorf("cloud[0] holds an empty name_unicode (omitempty): %v", com)
	}
	if got := com["size"].(float64); got != 1.5 { // the top of the log scale, %.3f
		t.Errorf("cloud[0].size = %v, want 1.5", got)
	}
	idn := statsObj(t, cloud[2], "cloud[2]")
	if idn["name_unicode"] != "xn--" {
		t.Errorf("cloud[2].name_unicode = %v, want the Unicode form", idn["name_unicode"])
	}

	table := statsArr(t, doc, "table")
	if len(table) != 2 {
		t.Fatalf("the table = %d zones, want 2", len(table))
	}
	row := statsObj(t, table[0], "table[0]")
	if row["name"] != "com" || statsInt(t, row, "checked") != 800_000 ||
		statsInt(t, row, "laravel") != 2000 || row["percent"].(float64) != 0.25 {
		t.Errorf("table[0] = %v", row)
	}

	latest := statsArr(t, doc, "latest")
	if len(latest) != 1 {
		t.Fatalf("the finds = %d, want 1", len(latest))
	}
	find := statsObj(t, latest[0], "latest[0]")
	if find["name"] != "shop.example.com" || find["versions"] != "9.19–12" ||
		find["updated"] != "Oct 18, 2026" {
		t.Errorf("latest[0] = %v", find)
	}
	// found_on_mask 0b11 — the apex (no badge) and www. The badges are
	// named `badges` ("subdomains" is the top-level reference table).
	badges := statsArr(t, find, "badges")
	if len(badges) != 1 {
		t.Fatalf("the find badges = %d, want 1 (www)", len(badges))
	}
	if b := statsObj(t, badges[0], "badges[0]"); b["label"] != "www" ||
		b["host"] != "www.shop.example.com" {
		t.Errorf("the badge = %v, want www", b)
	}

	subs := statsArr(t, doc, "subdomains")
	if len(subs) != 3 {
		t.Fatalf("the subdomain reference = %d entries, want 3", len(subs))
	}
	if s := statsObj(t, subs[1], "subdomains[1]"); statsInt(t, s, "id") != 1 || s["label"] != "www" {
		t.Errorf("subdomains[1] = %v, want {1, www}", s)
	}

	// The data pass is the one of the SSR page: every landing SQL once,
	// the subdomain reference is shared with the mask decoding — not
	// queried twice for the API.
	db.mu.Lock()
	defer db.mu.Unlock()
	if n := strings.Count(strings.Join(db.queries, "\n"), subdomainsSQL); n != 1 {
		t.Errorf("the subdomain reference SQL ran %d times, want 1 (a single data pass)", n)
	}
}

// TestLandingStatsEmptyDB — a fresh database: zeros in the counters and
// empty arrays (not null) — the static landing renders an empty state.
func TestLandingStatsEmptyDB(t *testing.T) {
	srv := newTestServer(t, &fakeDB{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/landing/stats")
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	doc := statsDoc(t, body)
	stats := statsObj(t, doc["stats"], "stats")
	for _, key := range []string{"zones", "total", "checked", "laravel", "laravel_last_24h"} {
		if got := statsInt(t, stats, key); got != 0 {
			t.Errorf("stats.%s = %d on an empty DB, want 0", key, got)
		}
	}
	for _, key := range []string{"cloud", "table", "latest", "subdomains"} {
		if arr := statsArr(t, doc, key); len(arr) != 0 {
			t.Errorf("%s holds %d items on an empty DB, want 0", key, len(arr))
		}
	}
}

// TestLandingStatsDBError — a data failure: a 500 without internal
// details, in the uniform JSON error format of the /api/* surface
// (checkWriteError, T-227) — the consumer is the TypeScript client of
// the static landing, not a browser page.
func TestLandingStatsDBError(t *testing.T) {
	srv := newTestServer(t, brokenDB{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/landing/stats")
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("the DB failure: status = %d, want 500", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("the 500 Content-Type = %q, want application/json", ct)
	}
	if !strings.Contains(body, `"unavailable"`) {
		t.Errorf("the 500 body without the error code: %s", body)
	}
	if strings.Contains(body, "the db is unavailable") {
		t.Errorf("the 500 body reveals the internal error: %s", body)
	}
}

// TestLandingStatsMethodNotAllowed — non-GET methods — an honest 405
// (the path patterns of the router are GET-only).
func TestLandingStatsMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req, err := http.NewRequest(method, ts.URL+"/api/v1/landing/stats", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		_ = readBody(t, resp)
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s = %d, want 405", method, resp.StatusCode)
		}
	}
}

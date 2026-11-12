// Unit tests for crawlerctl domain (T-081, spec 5.11) on a fake pool
// (db.DB) and a fake content store — no DB or pack files. Covered: the
// exact SQL text (the contract with the domain_info function of
// migration 000006 — a single source of truth, the status_test.go
// style), jsonb response parsing, name form normalization
// (case/trailing dot/unicode/punycode — before touching the DB), the
// --content record choice (the rendered-DOM priority), the summary
// format (sections, the dns_mask breakdown, the results table, Laravel
// signs with evidence fragments and weak rule hits), machine output
// (--json — the raw jsonb; found:false for an unknown domain) and clear
// "what exactly was not found" messages. Behavior against the live
// stack — the dev environment (make domain NAME=…).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/idn"
)

// --- SQL contracts ----------------------------------------------------------

// wantDomainInfoSQL / wantZoneLongestSQL — the expected query texts; the
// fake matches SQL exactly, an unexpected query fails the test loudly.
const wantDomainInfoSQL = "SELECT domain_info($1) AS info"

const wantZoneLongestSQL = "SELECT z.name FROM zones AS z " +
	"WHERE z.name = ANY($1::text[]) " +
	"ORDER BY length(z.name) DESC, z.name LIMIT 1"

// domainFixtureJSON — a typical domain_info response: a live domain with
// two DNS-alive subdomains, three results (apex DOM, a www alias, an api
// error), the Laravel aggregate and a couple of rule hits — one
// confirmed and one weak. The format is the migration 000006 contract
// (T-082).
const domainFixtureJSON = `{
  "zone": {"id": 1, "name": "crawl.test", "name_unicode": null},
  "domain": {
    "id": 42, "label": "laravel-11", "name": "laravel-11.crawl.test",
    "status": 4, "status_name": "alive",
    "check_state": 2, "check_state_name": "done",
    "is_idn": false, "render_pending": false,
    "dns_mask": 3, "temp_attempts": 0,
    "queued_at": "2026-10-16T09:00:00.5+00:00",
    "last_checked_at": "2026-10-16"
  },
  "dns_alive": [{"id": 0, "label": ""}, {"id": 1, "label": "www"}],
  "results": [
    {"subdomain_id": 0, "subdomain": "", "variant": 0, "scheme": 2, "scheme_name": "https",
     "http_status": 200, "final_url": "https://laravel-11.crawl.test/",
     "alias_kind": 0, "alias_kind_name": "none", "alias": null,
     "rendered": true, "error": null, "content_hash": "00112233445566778899aabbccddeeff",
     "checked_at": "2026-10-16T09:00:05.123456+00:00"},
    {"subdomain_id": 1, "subdomain": "www", "variant": 0, "scheme": 2, "scheme_name": "https",
     "http_status": 200, "final_url": "https://laravel-11.crawl.test/",
     "alias_kind": 1, "alias_kind_name": "identical", "alias": "",
     "rendered": false, "error": null, "content_hash": "00112233445566778899aabbccddeeff",
     "checked_at": "2026-10-16T09:00:07+00:00"},
    {"subdomain_id": 2, "subdomain": "api", "variant": 1, "scheme": 1, "scheme_name": "http",
     "http_status": null, "final_url": null,
     "alias_kind": 0, "alias_kind_name": "none", "alias": null,
     "rendered": false, "error": 1, "content_hash": "",
     "checked_at": "2026-10-16T09:00:08+00:00"}
  ],
  "laravel": {
    "score": 1.25, "ver_min": "11.0", "ver_max": "11.4",
    "found_on_mask": 1, "found_on": [{"id": 0, "label": ""}],
    "rules_version": 42, "updated_at": "2026-10-16T09:01:00+00:00",
    "findings": [
      {"subdomain_id": 0, "subdomain": "", "variant": 0, "score": 1.25,
       "ver_min": "11.0", "ver_max": "11.4", "is_laravel": true,
       "signals": [
         {"rule": "cookie_xsrf", "weight": 0.5, "evidence": "Set-Cookie: XSRF-TOKEN=eyJ…"},
         {"rule": "header_laravel", "weight": 0.75, "evidence": "X-Laravel-Version: 11"}]},
      {"subdomain_id": 1, "subdomain": "www", "variant": 0, "score": 0.25,
       "ver_min": null, "ver_max": null, "is_laravel": false,
       "signals": [{"rule": "meta_generator", "weight": 0.25, "evidence": "Laravel"}]}
    ]
  }
}`

// domainFixtureInfo — the parsed domainFixtureJSON.
func domainFixtureInfo(t *testing.T) domainInfo {
	t.Helper()
	var info domainInfo
	if err := json.Unmarshal([]byte(domainFixtureJSON), &info); err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	return info
}

// --- fake pool (db.DB) ----------------------------------------------------

// domainPool — db.DB without a DB: infos (punycode name → domain_info
// json; an empty value or a missing key — NULL "not found"), zones —
// the reference table for zoneLongestSQL.
type domainPool struct {
	infos map[string]string
	zones []string

	mu        sync.Mutex
	queries   []string
	queryArgs [][]any
	queryErr  error
}

var _ db.DB = (*domainPool)(nil)

func newDomainPool() *domainPool {
	return &domainPool{infos: map[string]string{}}
}

func (p *domainPool) seedInfo(name, json string) { p.infos[name] = json }

func (p *domainPool) setQueryErr(err error) {
	p.mu.Lock()
	p.queryErr = err
	p.mu.Unlock()
}

func (p *domainPool) calls() ([]string, [][]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.queries...), append([][]any(nil), p.queryArgs...)
}

func (p *domainPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("domainPool: CopyFrom must not be called")
}

func (p *domainPool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("domainPool: unexpected Exec: %s", sql)
}

func (p *domainPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, sql)
	p.queryArgs = append(p.queryArgs, append([]any(nil), args...))
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	switch sql {
	case domainInfoSQL:
		name, _ := args[0].(string)
		if js, ok := p.infos[name]; ok && js != "" {
			return domainRowsOf([]any{[]byte(js)}), nil
		}
		return domainRowsOf([]any{nil}), nil // NULL: not found
	case zoneLongestSQL:
		cands, _ := args[0].([]string)
		best := ""
		for _, z := range p.zones { // ORDER BY length DESC, name
			for _, c := range cands {
				if z != c {
					continue
				}
				if len(z) > len(best) || (len(z) == len(best) && z < best) {
					best = z
				}
			}
		}
		if best == "" {
			return domainRowsOf([]any{nil}), nil
		}
		return domainRowsOf([]any{[]byte(best)}), nil
	default:
		return nil, fmt.Errorf("domainPool: unexpected SQL Query: %s", sql)
	}
}

// domainRows — pgx.Rows with prepared rows (positional Scan; a nil
// value — SQL NULL). Column values: []byte, string.
type domainRows struct {
	rows [][]any
	i    int
}

func domainRowsOf(rows ...[]any) *domainRows { return &domainRows{rows: rows} }

func (r *domainRows) Close()                                       {}
func (r *domainRows) Err() error                                   { return nil }
func (r *domainRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *domainRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *domainRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *domainRows) RawValues() [][]byte                          { return nil }
func (r *domainRows) Conn() *pgx.Conn                              { return nil }
func (r *domainRows) TypeMap() *pgtype.Map                         { return nil }

func (r *domainRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *domainRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("domainRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("domainRows: %d destinations, %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *[]byte:
			if v == nil {
				*dst = nil
				continue
			}
			*dst = v.([]byte)
		case *string:
			if v == nil {
				*dst = ""
				continue
			}
			*dst = v.(string)
		default:
			return fmt.Errorf("domainRows: the destination %T is not supported", d)
		}
	}
	return nil
}

// --- fake content store ---------------------------------------------

// fakeContentStore — domainContentStore: bytes by hex hash or an error.
type fakeContentStore struct {
	data map[string][]byte
	err  error
}

func (s *fakeContentStore) Get(_ context.Context, h content.Hash) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	d, ok := s.data[h.String()]
	if !ok {
		return nil, content.ErrNotFound
	}
	return d, nil
}

// --- SQL and query semantics ---------------------------------------------------

func TestDomainSQLContract(t *testing.T) {
	if domainInfoSQL != wantDomainInfoSQL {
		t.Errorf("domainInfoSQL:\n%s\nwant:\n%s", domainInfoSQL, wantDomainInfoSQL)
	}
	if zoneLongestSQL != wantZoneLongestSQL {
		t.Errorf("zoneLongestSQL:\n%s\nwant:\n%s", zoneLongestSQL, wantZoneLongestSQL)
	}
}

// TestFetchDomainInfo — calling the function with a punycode name,
// parsing the jsonb (zone/statuses/dns_alive/results/laravel with
// findings), NULL → found=false without an error; pool errors carry the
// command context.
func TestFetchDomainInfo(t *testing.T) {
	pool := newDomainPool()
	pool.seedInfo("laravel-11.crawl.test", domainFixtureJSON)

	info, raw, found, err := fetchDomainInfo(context.Background(), pool, "laravel-11.crawl.test")
	if err != nil {
		t.Fatalf("fetchDomainInfo: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if string(raw) != domainFixtureJSON {
		t.Errorf("raw does not match the function response:\n%s", raw)
	}
	queries, args := pool.calls()
	if len(queries) != 1 || queries[0] != wantDomainInfoSQL {
		t.Fatalf("queries = %q, want one domainInfoSQL", queries)
	}
	if len(args) != 1 || args[0][0] != "laravel-11.crawl.test" {
		t.Errorf("args = %v, want the punycode name as the first argument", args)
	}

	if info.Zone.ID != 1 || info.Zone.Name != "crawl.test" || info.Zone.NameUnicode != "" {
		t.Errorf("zone = %+v", info.Zone)
	}
	d := info.Domain
	if d.ID != 42 || d.Label != "laravel-11" || d.Name != "laravel-11.crawl.test" ||
		d.StatusName != "alive" || d.CheckStateName != "done" ||
		d.DNSMask != 3 || d.TempAttempts != 0 || d.QueuedAt == "" || d.LastCheckedAt != "2026-10-16" {
		t.Errorf("domain = %+v", d)
	}
	if len(info.DNSAlive) != 2 || info.DNSAlive[0].Label != "" || info.DNSAlive[1].Label != "www" {
		t.Errorf("dns_alive = %+v", info.DNSAlive)
	}
	if len(info.Results) != 3 {
		t.Fatalf("results = %d, want 3", len(info.Results))
	}
	first := info.Results[0]
	if first.Subdomain != "" || first.SchemeName != "https" || first.HTTPStatus != 200 ||
		!first.Rendered || first.ContentHash != "00112233445566778899aabbccddeeff" {
		t.Errorf("results[0] = %+v", first)
	}
	if info.Laravel == nil {
		t.Fatal("laravel = nil, want the aggregate")
	}
	if info.Laravel.Score != 1.25 || info.Laravel.VerMin != "11.0" || info.Laravel.VerMax != "11.4" ||
		info.Laravel.RulesVersion != 42 {
		t.Errorf("laravel = %+v", info.Laravel)
	}
	if len(info.Laravel.Findings) != 2 || !info.Laravel.Findings[0].IsLaravel || info.Laravel.Findings[1].IsLaravel {
		t.Errorf("findings = %+v, want a confirmed + a weak one", info.Laravel.Findings)
	}
	if sigs := info.Laravel.Findings[0].Signals; len(sigs) != 2 || sigs[0].Rule != "cookie_xsrf" || sigs[1].Evidence != "X-Laravel-Version: 11" {
		t.Errorf("signals = %+v", sigs)
	}

	// NULL "not found" — not an error.
	pool = newDomainPool()
	_, raw, found, err = fetchDomainInfo(context.Background(), pool, "no-such.crawl.test")
	if err != nil || found || raw != nil {
		t.Errorf("NULL: raw=%v found=%v err=%v, want nil/false/nil", raw, found, err)
	}

	// Pool errors carry context; a nil pool is an error, not a panic.
	boom := errors.New("boom")
	pool = newDomainPool()
	pool.setQueryErr(boom)
	if _, _, _, err := fetchDomainInfo(context.Background(), pool, "x.crawl.test"); err == nil || !strings.Contains(err.Error(), "domain: domain_info:") {
		t.Errorf("err = %v, want the \"domain: domain_info:\" context", err)
	}
	if _, _, _, err := fetchDomainInfo(context.Background(), nil, "x.crawl.test"); err == nil {
		t.Error("fetchDomainInfo(nil) = nil, want an error")
	}

	// Broken json is a parse error, not silence.
	pool = newDomainPool()
	pool.seedInfo("bad.crawl.test", "{not json")
	if _, _, _, err := fetchDomainInfo(context.Background(), pool, "bad.crawl.test"); err == nil || !strings.Contains(err.Error(), "json parsing") {
		t.Errorf("broken json: err = %v, want a parse error", err)
	}
}

// TestDomainNormalizeForms — operator input normalization (spec 5.2):
// case, a trailing dot, spaces, unicode and punycode all yield one
// punycode name — it is what goes into domain_info (the query
// argument).
func TestDomainNormalizeForms(t *testing.T) {
	forms := map[string]string{
		"laravel-11.crawl.test":   "laravel-11.crawl.test",
		"Laravel-11.CRAWL.Test.":  "laravel-11.crawl.test",
		" laravel-11.crawl.test ": "laravel-11.crawl.test",
		"пример.crawl.test":       "xn--e1afmkfd.crawl.test",
		"xn--e1afmkfd.crawl.test": "xn--e1afmkfd.crawl.test",
		"ПРИМЕР.crawl.TEST.":      "xn--e1afmkfd.crawl.test",
	}
	pool := newDomainPool()
	for in, want := range forms {
		res, err := idn.Normalize(in)
		if err != nil {
			t.Errorf("Normalize(%q): %v", in, err)
			continue
		}
		if res.ASCII != want {
			t.Errorf("Normalize(%q).ASCII = %q, want %q", in, res.ASCII, want)
		}
		pool.seedInfo(want, domainFixtureJSON)
		if _, _, found, err := fetchDomainInfo(context.Background(), pool, res.ASCII); err != nil || !found {
			t.Errorf("fetchDomainInfo(%q → %q): found=%v err=%v", in, res.ASCII, found, err)
		}
	}
}

// TestZoneSuffixCandidates — zone candidates: all suffixes except the
// full name, longest first (as domain_info splits them).
func TestZoneSuffixCandidates(t *testing.T) {
	cases := []struct {
		name string
		want []string
	}{
		{"example.crawl.test", []string{"crawl.test", "test"}},
		{"a.b.crawl.test", []string{"b.crawl.test", "crawl.test", "test"}},
		{"x.test", []string{"test"}},
		{"пример.crawl.test", []string{"crawl.test", "test"}},
	}
	for _, tc := range cases {
		if got := zoneSuffixCandidates(tc.name); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("zoneSuffixCandidates(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestFetchCatalogZone — the longest suffix in zones (co.uk is preferred
// over uk); not found — best = the longest candidate, found=false.
func TestFetchCatalogZone(t *testing.T) {
	pool := newDomainPool()
	pool.zones = []string{"uk", "co.uk", "test"}

	zone, found, err := fetchCatalogZone(context.Background(), pool, "shop.example.co.uk")
	if err != nil || !found || zone != "co.uk" {
		t.Errorf("co.uk: zone=%q found=%v err=%v, want co.uk/true", zone, found, err)
	}
	_, args := pool.calls()
	if len(args) != 1 || !reflect.DeepEqual(args[0][0], []string{"example.co.uk", "co.uk", "uk"}) {
		t.Errorf("args = %v, want the suffix candidates", args)
	}

	zone, found, err = fetchCatalogZone(context.Background(), pool, "free.test")
	if err != nil || !found || zone != "test" {
		t.Errorf("test: zone=%q found=%v err=%v, want test/true", zone, found, err)
	}

	zone, found, err = fetchCatalogZone(context.Background(), pool, "x.unknown.tld")
	if err != nil || found || zone != "unknown.tld" {
		t.Errorf("no zone: zone=%q found=%v err=%v, want unknown.tld/false", zone, found, err)
	}

	// Errors and a nil pool.
	pool.setQueryErr(errors.New("boom"))
	if _, _, err := fetchCatalogZone(context.Background(), pool, "x.test"); err == nil || !strings.Contains(err.Error(), "domain: zones:") {
		t.Errorf("err = %v, want the \"domain: zones:\" context", err)
	}
	if _, _, err := fetchCatalogZone(context.Background(), nil, "x.test"); err == nil {
		t.Error("fetchCatalogZone(nil) = nil, want an error")
	}
}

// --- content choice ------------------------------------------------------------

// TestSelectContentResult — the rendered record has priority (DOM is the
// pipeline's final result), otherwise the first record with content in
// subdomain×variant order; no content — false.
func TestSelectContentResult(t *testing.T) {
	hash := "00112233445566778899aabbccddeeff"
	cases := []struct {
		name    string
		results []domainResult
		wantIdx int
		wantOK  bool
	}{
		{
			name: "the render outranks the first one",
			results: []domainResult{
				{Subdomain: "", ContentHash: hash, Rendered: false},
				{Subdomain: "www", ContentHash: hash, Rendered: true},
			},
			wantIdx: 1, wantOK: true,
		},
		{
			name: "no render — the first one with content",
			results: []domainResult{
				{Subdomain: "", ContentHash: ""},
				{Subdomain: "www", ContentHash: hash},
				{Subdomain: "api", ContentHash: hash},
			},
			wantIdx: 1, wantOK: true,
		},
		{
			name: "the apex DOM before the www DOM (the function order)",
			results: []domainResult{
				{Subdomain: "", ContentHash: hash, Rendered: true},
				{Subdomain: "www", ContentHash: hash, Rendered: true},
			},
			wantIdx: 0, wantOK: true,
		},
		{
			name: "no content at all",
			results: []domainResult{
				{Subdomain: "", ContentHash: ""},
				{Subdomain: "api", ContentHash: "", Error: 1},
			},
			wantIdx: 0, wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := selectContentResult(tc.results)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && got != tc.results[tc.wantIdx] {
				t.Errorf("the record selected %+v, want %+v", got, tc.results[tc.wantIdx])
			}
		})
	}
	if _, ok := selectContentResult(nil); ok {
		t.Error("selectContentResult(nil) = true, want false")
	}
}

// --- formatting ---------------------------------------------------------------

func TestSubLabel(t *testing.T) {
	if got := subLabel(""); got != "(apex)" {
		t.Errorf("subLabel(\"\") = %q, want (apex)", got)
	}
	if got := subLabel("www"); got != "www" {
		t.Errorf("subLabel(www) = %q", got)
	}
}

func TestVerRange(t *testing.T) {
	cases := []struct {
		min, max, want string
	}{
		{"11.0", "11.4", "11.0 – 11.4"},
		{"11.0", "11.0", "11.0"},
		{"11.0", "", "≥ 11.0"},
		{"", "11.4", "≤ 11.4"},
		{"", "", "—"},
	}
	for _, tc := range cases {
		if got := verRange(tc.min, tc.max); got != tc.want {
			t.Errorf("verRange(%q, %q) = %q, want %q", tc.min, tc.max, got, tc.want)
		}
	}
}

func TestHTTPErrorName(t *testing.T) {
	cases := []struct {
		code int16
		want string
	}{
		{0, "none"}, {1, "timeout"}, {2, "redirects"},
		{3, "tls"}, {4, "body"}, {5, "network"}, {9, "code 9"},
	}
	for _, tc := range cases {
		if got := httpErrorName(tc.code); got != tc.want {
			t.Errorf("httpErrorName(%d) = %q, want %q", tc.code, got, tc.want)
		}
	}
}

func TestFormatDomainTS(t *testing.T) {
	// timestamptz (ISO 8601 from the jsonb) → date-time; date and junk stay as-is.
	cases := []struct {
		in, want string
	}{
		{"2026-10-16T09:00:00.5+00:00", "2026-10-16 09:00:00"},
		{"2026-10-16T09:00:08+00:00", "2026-10-16 09:00:08"},
		{"2026-10-16", "2026-10-16"},
		{"not a time", "not a time"},
	}
	for _, tc := range cases {
		if got := formatDomainTS(tc.in); got != tc.want {
			t.Errorf("formatDomainTS(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := formatDomainTSShort("2026-10-16T09:00:08+00:00"); got != "2026-10-16 09:00" {
		t.Errorf("formatDomainTSShort = %q", got)
	}
}

// TestResultCells — results table cells: status/http 0 and an empty URL
// without an error — "—", content DOM/HTML/—, aliases, an error with its
// class.
func TestResultCells(t *testing.T) {
	cases := []struct {
		name string
		r    domainResult
		want string
	}{
		{"http 0 — a dash", domainResult{}, "—"},
		{"a status", domainResult{HTTPStatus: 200}, "200"},
	}
	for _, tc := range cases {
		if got := httpStatusDesc(tc.r); got != tc.want {
			t.Errorf("%s: httpStatusDesc = %q, want %q", tc.name, got, tc.want)
		}
	}

	content := []struct {
		name string
		r    domainResult
		want string
	}{
		{"no content", domainResult{}, "—"},
		{"raw HTML", domainResult{ContentHash: "ab"}, "HTML"},
		{"the render DOM", domainResult{ContentHash: "ab", Rendered: true}, "DOM"},
	}
	for _, tc := range content {
		if got := contentKind(tc.r); got != tc.want {
			t.Errorf("%s: contentKind = %q, want %q", tc.name, got, tc.want)
		}
	}

	aliases := []struct {
		name string
		r    domainResult
		want string
	}{
		{"no relation", domainResult{AliasKind: 0}, "—"},
		{"identical to the apex", domainResult{AliasKind: 1, Alias: ""}, "≡ (apex)"},
		{"a redirect to www", domainResult{AliasKind: 2, Alias: "www"}, "→ www"},
	}
	for _, tc := range aliases {
		if got := aliasDesc(tc.r); got != tc.want {
			t.Errorf("%s: aliasDesc = %q, want %q", tc.name, got, tc.want)
		}
	}

	finals := []struct {
		name string
		r    domainResult
		want string
	}{
		{"a URL only", domainResult{FinalURL: "https://x.test/"}, "https://x.test/"},
		{"an error only", domainResult{Error: 1}, "error 1 (timeout)"},
		{"a URL and an error", domainResult{FinalURL: "http://x.test/", Error: 3}, "http://x.test/ — error 3 (tls)"},
		{"empty", domainResult{}, "—"},
	}
	for _, tc := range finals {
		if got := resultFinalDesc(tc.r); got != tc.want {
			t.Errorf("%s: resultFinalDesc = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// --- human-readable output ---------------------------------------------------------

// TestWriteDomainText — summary sections against the fixture: a header
// with the zone and statuses, the dns_mask breakdown, the results table
// (scheme/status/content/alias/URL/error), Laravel signs with versions,
// rules and evidence fragments, weak rule hits as a separate section.
func TestWriteDomainText(t *testing.T) {
	info := domainFixtureInfo(t)

	var b strings.Builder
	writeDomainText(&b, info, 0.6)
	got := b.String()

	for _, want := range []string{
		"domain:        laravel-11.crawl.test\n",
		"zone:          crawl.test (id 1)\n",
		"status:        alive\n",
		"check:         done\n",
		"checked:       2026-10-16\n",
		"queued since:  2026-10-16 09:00:00\n",
		"DNS-alive subdomains (dns_mask = 3): (apex), www\n",
		"results by subdomain and variant:\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the summary does not contain %q:\n%s", want, got)
		}
	}

	// Table rows: padding is not checked — only the cells within a line.
	var apexLine, wwwLine, apiLine string
	for _, line := range strings.Split(got, "\n") {
		switch {
		case strings.HasPrefix(line, "(apex)"):
			apexLine = line
		case strings.HasPrefix(line, "www"):
			wwwLine = line
		case strings.HasPrefix(line, "api"):
			apiLine = line
		}
	}

	// Column widths are in runes (CLI-11): the Cyrillic header cell
	// (7 runes, 14 bytes) — the widest cell of the column, followed
	// immediately by the separator, not byte-based slack; data rows are
	// padded to the same width (www → 3 + 4 spaces).
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "subdomain ") {
			if !strings.HasPrefix(line, "subdomain var.") {
				t.Errorf("the table header with byte padding: %q", line)
			}
		}
	}
	if !strings.Contains(got, "\nwww       main ") {
		t.Errorf("the www line is not rune-aligned (\"subdomain\" = 9 runes: 9−3=6 spaces + the separator):\n%q", wwwLine)
	}
	for _, cell := range []string{"main", "https", "200", "DOM", "—", "2026-10-16 09:00", "https://laravel-11.crawl.test/"} {
		if !strings.Contains(apexLine, cell) {
			t.Errorf("the apex line without %q: %q", cell, apexLine)
		}
	}
	for _, cell := range []string{"www", "HTML", "≡ (apex)"} {
		if !strings.Contains(wwwLine, cell) {
			t.Errorf("the www line without %q: %q", cell, wwwLine)
		}
	}
	for _, cell := range []string{"api", "http", "—", "error 1 (timeout)"} {
		if !strings.Contains(apiLine, cell) {
			t.Errorf("the api line without %q: %q", cell, apiLine)
		}
	}

	for _, want := range []string{
		"Laravel:\n",
		"  score:       1.250 (threshold 0.600)\n",
		"  versions:    11.0 – 11.4\n",
		"  subdomains:  (apex)\n",
		"  rules:       version 42, updated 2026-10-16 09:01:00\n",
		"  firings (is_laravel = true):\n",
		"    (apex) main  score 1.250, versions 11.0 – 11.4:\n",
		"      cookie_xsrf (+0.500): \"Set-Cookie: XSRF-TOKEN=eyJ...\"\n",
		"      header_laravel (+0.750): \"X-Laravel-Version: 11\"\n",
		"  weak firings (is_laravel = false, score < 0.600):\n",
		"    www main  score 0.250:\n",
		"      meta_generator (+0.250): \"Laravel\"\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the Laravel section without %q:\n%s", want, got)
		}
	}
}

// TestWriteDomainTextVariants — summary edge cases: an IDN domain (the
// unicode string and flags), empty results, no Laravel signs, temp
// attempts.
func TestWriteDomainTextVariants(t *testing.T) {
	var info domainInfo
	info.Zone.Name = "xn--p1ai"
	info.Zone.NameUnicode = "рф"
	info.Zone.ID = 7
	info.Domain.Name = "xn--e1afmkfd.xn--p1ai"
	info.Domain.Label = "xn--e1afmkfd"
	info.Domain.StatusName = "temp_error"
	info.Domain.CheckStateName = "queued"
	info.Domain.TempAttempts = 2
	info.Domain.IsIDN = true
	info.Domain.RenderPending = true

	var b strings.Builder
	writeDomainText(&b, info, 0.6)
	got := b.String()
	for _, want := range []string{
		"domain:        xn--e1afmkfd.xn--p1ai\n",
		"unicode:       пример.рф\n",
		"zone:          xn--p1ai (unicode: рф, id 7)\n",
		"status:        temp_error\n",
		"check:         queued\n",
		"checked:       —\n",
		"flags:         is_idn, render_pending\n",
		"temp attempts: 2\n",
		"DNS-alive subdomains (dns_mask = 0): none\n",
		"  — none (the domain was not checked or the DNS is not delegated)\n",
		"  — no signs found (no row in laravel_domains;\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the summary (variant) does not contain %q:\n%s", want, got)
		}
	}
}

// --- machine output -------------------------------------------------------------

// TestWriteDomainJSON — raw jsonb pretty-print: the content is unchanged,
// a 2-space indent, a trailing newline.
func TestWriteDomainJSON(t *testing.T) {
	var b strings.Builder
	if err := writeDomainJSON(&b, []byte(domainFixtureJSON)); err != nil {
		t.Fatalf("writeDomainJSON: %v", err)
	}
	got := b.String()
	if !strings.HasSuffix(got, "\n") {
		t.Error("the json does not end with a newline")
	}
	if !strings.Contains(got, "\n  \"zone\"") {
		t.Errorf("the json without the 2-space indent:\n%s", got)
	}
	var have, want map[string]any
	if err := json.Unmarshal([]byte(got), &have); err != nil {
		t.Fatalf("the output is not json: %v;\n%s", err, got)
	}
	if err := json.Unmarshal([]byte(domainFixtureJSON), &want); err != nil {
		t.Fatalf("the fixture is not json: %v", err)
	}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("the pretty-print changed the data:\n%s", got)
	}
}

// TestWriteDomainNotFoundJSON — found:false with the zone clarification.
func TestWriteDomainNotFoundJSON(t *testing.T) {
	var b bytes.Buffer
	if err := writeDomainNotFoundJSON(&b, "example.crawl.test", "crawl.test", false); err != nil {
		t.Fatalf("writeDomainNotFoundJSON: %v", err)
	}
	var doc struct {
		Found     bool   `json:"found"`
		Name      string `json:"name"`
		Zone      string `json:"zone"`
		ZoneFound bool   `json:"zone_found"`
	}
	if err := json.Unmarshal(b.Bytes(), &doc); err != nil {
		t.Fatalf("the output is not json: %v;\n%s", err, b.String())
	}
	if doc.Found || doc.Name != "example.crawl.test" || doc.Zone != "crawl.test" || doc.ZoneFound {
		t.Errorf("doc = %+v, want found=false, the zone crawl.test without a row", doc)
	}
}

// --- not found -------------------------------------------------------------------

// TestDomainNotFound — what exactly was not found (spec 5.11): the zone
// exists — "no domain with the label"; the zone does not exist — "the
// zone is missing from the reference table"; --json prints found:false
// and returns an error (code 1 via run).
func TestDomainNotFound(t *testing.T) {
	pool := newDomainPool()
	pool.zones = []string{"crawl.test"}

	err := domainNotFound(context.Background(), pool, "example.crawl.test", false, &strings.Builder{})
	if err == nil || !strings.Contains(err.Error(), "the zone crawl.test has no domain with the label example") {
		t.Errorf("err = %v, want \"the zone crawl.test has no domain with the label example\"", err)
	}

	var out bytes.Buffer
	err = domainNotFound(context.Background(), pool, "example.crawl.test", true, &out)
	if err == nil || !strings.Contains(out.String(), `"found":false`) || !strings.Contains(out.String(), `"zone_found":true`) {
		t.Errorf("--json: err=%v out=%s; want found:false + zone_found:true + an error", err, out.String())
	}

	pool = newDomainPool() // no zones at all
	err = domainNotFound(context.Background(), pool, "example.unknown.tld", false, &strings.Builder{})
	if err == nil || !strings.Contains(err.Error(), "zone unknown.tld is missing from the zones catalog") {
		t.Errorf("err = %v, want \"zone unknown.tld is missing from the zones catalog\"", err)
	}

	// A clarification failure is a command error, not "not found".
	pool = newDomainPool()
	pool.setQueryErr(errors.New("boom"))
	if err := domainNotFound(context.Background(), pool, "x.crawl.test", false, &strings.Builder{}); err == nil || !strings.Contains(err.Error(), "domain: zones:") {
		t.Errorf("err = %v, want a zone clarification error", err)
	}
}

// --- content ----------------------------------------------------------------------

// TestWriteDomainContent — stdout receives exactly the bytes of the
// selected record, stderr — the summary and the note (record, kind,
// size, hash); missing content and store errors are clear errors.
func TestWriteDomainContent(t *testing.T) {
	info := domainFixtureInfo(t)
	page := []byte("<!DOCTYPE html><html><body>Laravel 11</body></html>\n")
	store := &fakeContentStore{data: map[string][]byte{
		"00112233445566778899aabbccddeeff": page,
	}}

	var stdout, stderr bytes.Buffer
	if err := writeDomainContent(context.Background(), store, info, 0.6, &stdout, &stderr); err != nil {
		t.Fatalf("writeDomainContent: %v", err)
	}
	if stdout.String() != string(page) {
		t.Errorf("stdout = %q, want only the content bytes", stdout.String())
	}
	for _, want := range []string{
		"domain:        laravel-11.crawl.test", // the summary goes to stderr
		"content (--content): subdomain (apex), variant main, DOM (render),",
		fmt.Sprintf(" %d bytes, hash 00112233445566778899aabbccddeeff", len(page)),
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr does not contain %q:\n%s", want, stderr.String())
		}
	}

	// A record with no content — a clear error.
	noContent := info
	noContent.Results = []domainResult{{Subdomain: "api", Error: 1}}
	if err := writeDomainContent(context.Background(), store, noContent, 0.6, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "no stored content") {
		t.Errorf("err = %v, want \"no stored content\"", err)
	}

	// A broken hash from the function (not 16 hex bytes) — a data error,
	// not a panic. The apex is shown with the apex label (a Russian
	// literal), not an empty raw string.
	badHash := info
	badHash.Results = []domainResult{{Subdomain: "", ContentHash: "abc", Rendered: true}}
	if err := writeDomainContent(context.Background(), store, badHash, 0.6, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "invalid content_hash") ||
		!strings.Contains(err.Error(), "(subdomain (apex),") {
		t.Errorf("err = %v, want \"invalid content_hash\" with the (apex) label", err)
	}

	// The store did not find the bytes (the reference exists, the block
	// does not) — an error carrying the hash.
	store = &fakeContentStore{err: content.ErrNotFound}
	if err := writeDomainContent(context.Background(), store, info, 0.6, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "00112233445566778899aabbccddeeff") {
		t.Errorf("err = %v, want an error with the content hash", err)
	}
}

// --- routing/codes -------------------------------------------------------------------

// TestRunDomain — the execution path without infrastructure: the command
// needs a config (the PostgreSQL DSN) — without one, an execution error
// (code 1) before any connections (the TestRunStatus style). Input
// validation comes first: a bad name and an incompatible flag
// combination are code 2 even without configuration; normalized name
// forms pass validation (they reach the config).
func TestRunDomain(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	runCase := func(args ...string) (int, string, string) {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}

	code, stdout, stderr := runCase("domain", "laravel-11.crawl.test")
	if code != exitError {
		t.Fatalf("code = %d, want %d; stderr:\n%s", code, exitError, stderr)
	}
	if !strings.Contains(stderr, "domain: config:") {
		t.Errorf("stderr without the configuration context:\n%s", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout before the connection is not empty:\n%s", stdout)
	}

	// Name forms (case/trailing dot/unicode) pass normalization — the same
	// configuration error, not a bad-name usage error.
	for _, name := range []string{"Laravel-11.CRAWL.Test.", "пример.crawl.test", "xn--e1afmkfd.crawl.test"} {
		code, _, stderr = runCase("domain", "--json", name)
		if code != exitError || !strings.Contains(stderr, "domain: config:") {
			t.Errorf("domain --json %q: code %d, stderr:\n%s; want a configuration error", name, code, stderr)
		}
	}

	// Input validation comes before the configuration.
	code, _, stderr = runCase("domain", "bad!!name.test")
	if code != exitUsage || !strings.Contains(stderr, "invalid domain name") {
		t.Errorf("an invalid name: code %d, stderr:\n%s; want code 2", code, stderr)
	}
	code, _, stderr = runCase("domain", "--json", "--content", "x.crawl.test")
	if code != exitUsage || !strings.Contains(stderr, "incompatible") {
		t.Errorf("--json --content: code %d, stderr:\n%s; want code 2", code, stderr)
	}
}

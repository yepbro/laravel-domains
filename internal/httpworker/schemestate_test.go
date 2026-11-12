package httpworker

import (
	"net/url"
	"strings"
	"testing"

	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
)

// Matrix tests of the scheme check state FSM (T-266, TZ-fsm spec §3
// FR-12): all the legal combinations are listed explicitly and accepted
// by the validators; the illegal ones are rejected. The matrices and the row numbering —
// the schemestate.go doc comment.

// matrixURL — a URL of the matrix fixture (valid by construction).
func matrixURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

// TestSchemeCheckValidateMatrix — all 14 legal check states
// (S1–S14): every matrix row is accepted by the validator.
func TestSchemeCheckValidateMatrix(t *testing.T) {
	var (
		h   = content.HashOf([]byte("matrix body"))
		spa = httpfetch.SPAVerdict{}
	)
	cases := []struct {
		name  string
		check SchemeCheck
	}{
		{
			"S1 alive, the early Put passed",
			SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, FinalURL: matrixURL("https://x.example/"),
				Status: 200, ResponseMS: 40, ContentHash: &h, ContentStored: true, SPA: &spa},
		},
		{
			"S2 alive, the content write pause",
			SchemeCheck{Scheme: model.SchemeHTTP, Available: true, FinalURL: matrixURL("http://x.example/"),
				Status: 200, ResponseMS: 40, ContentHash: &h, SPA: &spa},
		},
		{
			"S3 alive, the body truncated",
			SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, FinalURL: matrixURL("https://x.example/"),
				Status: 200, ResponseMS: 40, ContentHash: &h, Truncated: true,
				ErrClass: ErrBody, ErrText: "the body is truncated by the max_body_bytes cap (the data is partial)", SPA: &spa},
		},
		{
			"S4 the cache hit of page S1",
			SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, Cached: true,
				//				// FinalURL — the check's starting URL: a deliberate
				//				// distortion on a cache hit (schemestate.go).
				FinalURL: matrixURL("https://x.example/"), Status: 200, ResponseMS: 40,
				ContentHash: &h, ContentStored: true, SPA: &spa,
				Redirects: []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 200}}},
		},
		{
			"S5 the cache hit of page S2 (the write pause is inherited)",
			SchemeCheck{Scheme: model.SchemeHTTP, Available: true, Cached: true,
				FinalURL: matrixURL("http://x.example/"), Status: 200, ResponseMS: 40,
				ContentHash: &h, SPA: &spa,
				Redirects: []httpfetch.RedirectStep{{URL: "http://x.example/", Status: 200}}},
		},
		{
			"S6 the cache hit of page S3 (the truncation carries over)",
			SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, Cached: true,
				FinalURL: matrixURL("https://x.example/"), Status: 200, ResponseMS: 40,
				ContentHash: &h, Truncated: true, ErrClass: ErrBody,
				ErrText: "the body is truncated by the max_body_bytes cap (the data is partial)", SPA: &spa,
				Redirects: []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 200}}},
		},
		{
			"S7 dead dial: connection refused",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrNetwork, ErrText: "dial tcp: connection refused", DeadDial: true},
		},
		{
			"S8 dead dial: connect timeout",
			SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrTimeout, ErrText: "dial tcp: i/o timeout", DeadDial: true},
		},
		{
			"S9 a body read timeout with a partial outcome",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrTimeout, ErrText: "timeout awaiting response body",
				FinalURL: matrixURL("https://x.example/"), Status: 200,
				Redirects: []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 200}}},
		},
		{
			"S10 a TLS handshake failure",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrTLS, ErrText: "tls: handshake failure"},
		},
		{
			"S11 a redirect hop failure (the chain is non-empty — not dead-host)",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrNetwork, ErrText: "dial tcp: lookup hop.example: no such host",
				Redirects: []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 302}}},
		},
		{
			"S12 the redirect limit",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrRedirects, ErrText: httpfetch.ErrTooManyRedirects.Error(),
				Redirects: []httpfetch.RedirectStep{
					{URL: "https://x.example/", Status: 301},
					{URL: "https://x.example/a", Status: 301},
				}},
		},
		{
			"S13 a body read error after the headers",
			SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrBody, ErrText: "gzip: the data exceeds the limit",
				FinalURL: matrixURL("https://x.example/"), Status: 200},
		},
		{
			"S14 the dead-host cutoff synthetic",
			SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrCutoff,
				ErrText: "dead-host cutoff (T-164): apex and www failed the connect phase — the subdomain was not checked"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.check.validate(); err != nil {
				t.Fatalf("a legal combination rejected: %v", err)
			}
		})
	}
}

// TestSchemeCheckValidateIllegal — the illegal combinations are rejected
// (the label — the invariant whose branch the violation ignites; the check order in
// validate() is chosen so that every branch is reachable).
func TestSchemeCheckValidateIllegal(t *testing.T) {
	var (
		h   = content.HashOf([]byte("matrix body"))
		spa = httpfetch.SPAVerdict{}
	)
	//	// live — the base live check S1; dead — the base dead dial S7.
	live := func() SchemeCheck {
		return SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, FinalURL: matrixURL("https://x.example/"),
			Status: 200, ResponseMS: 40, ContentHash: &h, ContentStored: true, SPA: &spa}
	}
	cases := []struct {
		name   string
		mutate func(*SchemeCheck)
	}{
		{"I1: the class outside the enum", func(c *SchemeCheck) { c.ErrClass = ErrorClass(9) }},
		{"I1: the scheme outside the enum", func(c *SchemeCheck) { c.Scheme = model.Scheme(9) }},
		{"I8: a success with an error text", func(c *SchemeCheck) { c.ErrText = "an extra text" }},
		{"I8: an error without a text", func(c *SchemeCheck) {
			c.ErrClass, c.ErrText = ErrTimeout, ""
			c.Available, c.ContentHash, c.ContentStored, c.SPA = false, nil, false, nil
		}},
		{"I5: unavailable with a hash", func(c *SchemeCheck) {
			c.Available, c.ContentStored, c.SPA, c.FinalURL = false, false, nil, nil
		}},
		{"I5: unavailable with a truncation", func(c *SchemeCheck) {
			c.Available, c.ContentStored, c.SPA, c.FinalURL = false, false, nil, nil
			c.Truncated, c.ErrClass, c.ErrText = true, ErrBody, "the body is truncated"
		}},
		{"I5: unavailable with a stored body", func(c *SchemeCheck) {
			c.Available, c.SPA, c.FinalURL = false, nil, nil
		}},
		{"I4: available without a hash", func(c *SchemeCheck) { c.ContentHash = nil }},
		{"I4: available without a final URL", func(c *SchemeCheck) { c.FinalURL = nil }},
		{"I4: available without an SPA verdict", func(c *SchemeCheck) { c.SPA = nil }},
		{"I4: ErrNone at a truncation", func(c *SchemeCheck) { c.Truncated = true }},
		{"I4: ErrBody without a truncation", func(c *SchemeCheck) { c.ErrClass, c.ErrText = ErrBody, "a broken encoding" }},
		{"I3: a cache hit without a hash", func(c *SchemeCheck) { c.Cached, c.ContentHash = true, nil }},
		{"I3: a cache hit of an unavailable check", func(c *SchemeCheck) {
			c.Cached = true
			c.Available, c.ContentStored, c.SPA, c.FinalURL = false, false, nil, nil
		}},
		{"I6: stored at a truncation", func(c *SchemeCheck) {
			c.Truncated, c.ErrClass, c.ErrText = true, ErrBody, "the body is truncated"
		}},
		{"I7: ErrCutoff on an available check", func(c *SchemeCheck) {
			c.ErrClass, c.ErrText = ErrCutoff, "dead-host cutoff"
		}},
		{"I2: DeadDial on an available check", func(c *SchemeCheck) { c.DeadDial = true }},
		{"I2: DeadDial with a non-empty chain", func(c *SchemeCheck) {
			c.ErrClass, c.ErrText, c.DeadDial = ErrNetwork, "dial tcp: connection refused", true
			c.Available, c.ContentHash, c.ContentStored, c.SPA, c.FinalURL = false, nil, false, nil, nil
			c.Redirects = []httpfetch.RedirectStep{{URL: "https://x.example/", Status: 302}}
		}},
		{"I2: DeadDial with the TLS class", func(c *SchemeCheck) {
			c.ErrClass, c.ErrText, c.DeadDial = ErrTLS, "tls: handshake failure", true
			c.Available, c.ContentHash, c.ContentStored, c.SPA, c.FinalURL = false, nil, false, nil, nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := live()
			tc.mutate(&c)
			if err := c.validate(); err == nil {
				t.Fatal("an illegal combination accepted")
			}
		})
	}
}

// TestRecordValidateMatrix — the record's content part: all the matrix rows
// R1–R8 at their boundaries (before ContentSaver / after the normalization) plus
// a minimal direct newRecordBatch call record.
func TestRecordValidateMatrix(t *testing.T) {
	var (
		h       = content.HashOf([]byte("record body"))
		aliasTo = int16(1)
	)
	cases := []struct {
		name string
		rec  Record
	}{
		{
			"R1 an original with content (both boundaries)",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentHash: &h, ContentStored: true, StoreContent: true},
		},
		{
			"R2 before Save: an original in the content write pause",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentHash: &h, StoreContent: true},
		},
		{
			"R3 before Save: an original with a truncation",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentHash: &h, Truncated: true,
				ErrClass: ErrBody, StoreContent: true},
		},
		{
			"R4 before Save: an alias of a stored original",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", ContentHash: &h, ContentStored: true,
				AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo},
		},
		{
			"R4 before Save: an alias of an original in the write pause",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", ContentHash: &h,
				AliasKind: model.AliasIdentical, AliasSubdomainID: &aliasTo},
		},
		{
			"R5 before Save: an alias of a truncated page",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", ContentHash: &h, Truncated: true, ErrClass: ErrBody,
				AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo},
		},
		{
			"R6 after Save: the content is not stored",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentNotStored: true, StoreContent: true},
		},
		{
			"R7 after Save: a truncated original without a hash",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
				FinalURL: "https://x.example/", ContentNotStored: true, Truncated: true,
				ErrClass: ErrBody, StoreContent: true},
		},
		{
			"R8 after Save: an alias (Truncated/ContentStored — a copy of src)",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", ContentStored: true,
				AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo},
		},
		{
			"R8 after Save: an alias of a truncated page",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
				FinalURL: "https://x.example/", Truncated: true, ErrClass: ErrBody,
				AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo},
		},
		{
			"a minimal direct-call record (partial fixtures)",
			Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.rec.validate(); err != nil {
				t.Fatalf("a legal combination rejected: %v", err)
			}
		})
	}
}

// TestRecordValidateIllegal — the illegal combinations of the content part
// are rejected (by the violation of a P1–P7 invariant).
func TestRecordValidateIllegal(t *testing.T) {
	var (
		h       = content.HashOf([]byte("record body"))
		aliasTo = int16(1)
	)
	//	// origin — R1 (an original with content); alias — R4 (an alias before Save).
	origin := func() Record {
		return Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
			FinalURL: "https://x.example/", ContentHash: &h, ContentStored: true, StoreContent: true}
	}
	alias := func() Record {
		return Record{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 1,
			FinalURL: "https://x.example/", ContentHash: &h,
			AliasKind: model.AliasRedirect, AliasSubdomainID: &aliasTo}
	}
	cases := []struct {
		name   string
		base   func() Record
		mutate func(*Record)
	}{
		{"P1: the variant outside the enum", origin, func(r *Record) { r.Variant = model.Variant(7) }},
		{"P1: the scheme outside the enum", origin, func(r *Record) { r.Scheme = model.Scheme(9) }},
		{"P1: the alias kind outside the enum", origin, func(r *Record) { r.AliasKind = model.AliasKind(5) }},
		{"P2: AliasNone with a reference", origin, func(r *Record) { r.AliasSubdomainID = &aliasTo }},
		{"P2: an alias without a reference", alias, func(r *Record) { r.AliasSubdomainID = nil }},
		{"P3: StoreContent on an alias", alias, func(r *Record) { r.StoreContent = true }},
		{"P4: ContentNotStored with a hash", origin, func(r *Record) { r.ContentNotStored = true }},
		{"P4: ContentNotStored with a stored body", origin, func(r *Record) {
			r.ContentHash, r.ContentNotStored, r.ContentStored = nil, true, true
		}},
		{"P4: ContentNotStored on an alias", alias, func(r *Record) {
			r.ContentHash, r.ContentNotStored = nil, true
		}},
		{"P5: an original without a hash and the sign", origin, func(r *Record) { r.ContentHash = nil }},
		{"P6: a truncation with storing", origin, func(r *Record) {
			r.Truncated, r.ErrClass = true, ErrBody
		}},
		{"P7: a record class outside an available check", origin, func(r *Record) { r.ErrClass = ErrTimeout }},
		{"P7: ErrBody without a truncation", origin, func(r *Record) { r.ErrClass = ErrBody }},
		{"P7: a truncation without ErrBody", alias, func(r *Record) { r.Truncated = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.base()
			tc.mutate(&r)
			if err := r.validate(); err == nil {
				t.Fatal("an illegal combination accepted")
			}
		})
	}
}

// TestValidateResult — the "before ContentSaver" boundary (the end of ProcessDomain,
// FR-12): the domain result validator accepts a legal result
// (a live check + an R1 record + the S14 cutoff synthetics) and rejects
// an illegal check/record with the subdomain context.
func TestValidateResult(t *testing.T) {
	var (
		h   = content.HashOf([]byte("result body"))
		spa = httpfetch.SPAVerdict{}
	)
	legal := &DomainResult{
		ZoneID: 7, DomainID: 91, Name: "ok.example",
		Subdomains: []SubdomainResult{
			{
				SubdomainID: 0, Label: "", Hostname: "ok.example",
				HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, Available: true, FinalURL: matrixURL("https://ok.example/"),
					Status: 200, ContentHash: &h, ContentStored: true, SPA: &spa},
				HTTP: SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrNetwork, ErrText: "dial tcp: connection refused"},
				Records: []Record{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS, SubdomainID: 0,
					FinalURL: "https://ok.example/", ContentHash: &h, ContentStored: true, StoreContent: true}},
			},
			{
				//			// The dead-host cutoff synthetics (S14): no records.
				SubdomainID: 2, Label: "blog", Hostname: "blog.ok.example",
				HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: ErrCutoff, ErrText: "dead-host cutoff (T-164)"},
				HTTP:  SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: ErrCutoff, ErrText: "dead-host cutoff (T-164)"},
			},
		},
	}
	//	// clone — a copy of the result with its own slices (the cases' mutations
	//	// must not leak into the base legal).
	clone := func(res *DomainResult) *DomainResult {
		out := *res
		out.Subdomains = append([]SubdomainResult(nil), res.Subdomains...)
		for i := range out.Subdomains {
			out.Subdomains[i].Records = append([]Record(nil), res.Subdomains[i].Records...)
		}
		return &out
	}

	if err := validateResult(legal); err != nil {
		t.Fatalf("a legal result rejected: %v", err)
	}

	badCheck := clone(legal)
	badCheck.Subdomains[0].HTTPS.ContentHash = nil // I4: Available without a hash
	if err := validateResult(badCheck); err == nil || !strings.Contains(err.Error(), "ok.example") {
		t.Fatalf("a check validation error with the subdomain context expected: %v", err)
	}

	badRecord := clone(legal)
	badRecord.Subdomains[0].Records[0].ContentNotStored = true // P4: with a live hash
	if err := validateResult(badRecord); err == nil || !strings.Contains(err.Error(), "T-056") {
		t.Fatalf("a record validation error expected: %v", err)
	}
}

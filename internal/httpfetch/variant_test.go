// Unit tests of the http/https variant logic (T-054; spec 5.5 item 5,
// §7). The scenarios of §11.1 ("http → https, http leads to another site")
// and the T-059 list: bare→www is the T-055 aliases' territory (different
// subdomains); here the variants of one subdomain over both protocols are
// checked.

package httpfetch

import (
	"net/url"
	"testing"

	"crawler/internal/content"
	"crawler/internal/model"
)

// variantCheck is an available check outcome with a final host+path.
func variantCheck(scheme model.Scheme, host, path string, hash *content.Hash) VariantInput {
	return VariantInput{Scheme: scheme, Available: true, Host: host, Path: path, ContentHash: hash}
}

// variantDown is an unavailable check outcome (a network error/timeout/
// the redirect limit — the classification belongs to T-058; only
// availability matters here).
func variantDown(scheme model.Scheme) VariantInput {
	return VariantInput{Scheme: scheme}
}

// variantHash is a content hash for the input; the nil representation of
// "content not stored" matters, the value is a comparison identifier.
func variantHash(s string) *content.Hash {
	h := content.HashOf([]byte(s))
	return &h
}

// variantRecordsEqual is a comparison without reflect: records are
// comparable in full.
func variantRecordsEqual(got, want []VariantRecord) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestDecideVariants is the decision table per §5.5 item 5: both
// unavailable, one protocol, one site (host+path or hash), different
// sites. Each case runs twice with swapped arguments — the pair is
// distinguished by the Scheme field, not by position.
func TestDecideVariants(t *testing.T) {
	cases := []struct {
		name  string
		https VariantInput
		http  VariantInput
		want  []VariantRecord
	}{
		{
			name:  "both unavailable — no records (the domain status is decided by T-058)",
			https: variantDown(model.SchemeHTTPS),
			http:  variantDown(model.SchemeHTTP),
			want:  nil,
		},
		{
			name:  "only https works — one record with its scheme",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantDown(model.SchemeHTTP),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "only http works — one record with its scheme",
			https: variantDown(model.SchemeHTTPS),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/", nil),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTP}},
		},
		{
			name: "http→https: the same final host+path — one site",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/",
				variantHash("страница")),
			http: variantCheck(model.SchemeHTTP, "example.com", "/",
				variantHash("страница")),
			want: []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name: "the same host+path with different content — one site (the OR criterion)",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/",
				variantHash("https-версия")),
			http: variantCheck(model.SchemeHTTP, "example.com", "/",
				variantHash("http-версия")),
			want: []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "http leads to another site (another host) — two variants",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "parking.example.net", "/parked.html", nil),
			want: []VariantRecord{
				{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
				{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
			},
		},
		{
			name:  "the same host, different paths — two variants",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/en", nil),
			want: []VariantRecord{
				{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
				{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
			},
		},
		{
			name: "a parking: different paths, an equal hash — one site (the OR criterion)",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/",
				variantHash("заглушка парковки")),
			http: variantCheck(model.SchemeHTTP, "example.com", "/index2.html",
				variantHash("заглушка парковки")),
			want: []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "the hash of empty content is not nil: equal empty pages — one site",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", variantHash("")),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/empty", variantHash("")),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "both hashes nil: the criterion degenerates to host+path — one site",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/", nil),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
		{
			name:  "both hashes nil: different paths — two variants",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "example.com", "/en", nil),
			want: []VariantRecord{
				{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
				{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
			},
		},
		{
			name:  "one hash nil (content not stored): the hash branch is unprovable — two variants",
			https: variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			http: variantCheck(model.SchemeHTTP, "example.com", "/en",
				variantHash("страница")),
			want: []VariantRecord{
				{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
				{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
			},
		},
		{
			name:  "the host case does not split a site (punycode is already lower-case — a guard)",
			https: variantCheck(model.SchemeHTTPS, "www.example.com", "/", nil),
			http:  variantCheck(model.SchemeHTTP, "WWW.EXAMPLE.COM", "/", nil),
			want:  []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, args := range [][2]VariantInput{{tc.https, tc.http}, {tc.http, tc.https}} {
				got, err := DecideVariants(args[0], args[1])
				if err != nil {
					t.Fatalf("DecideVariants(%v, %v): %v", args[0].Scheme, args[1].Scheme, err)
				}
				if !variantRecordsEqual(got, tc.want) {
					t.Errorf("DecideVariants(%v, %v) = %v, want %v",
						args[0].Scheme, args[1].Scheme, got, tc.want)
				}
			}
		})
	}
}

// TestDecideVariantsOneProtocolFinalURL — the record's scheme is the
// CHECK's scheme: an http check may have ended on an https page, and the
// record still carries scheme=http (the data from the http check).
func TestDecideVariantsOneProtocolFinalURL(t *testing.T) {
	https, http := variantDown(model.SchemeHTTPS), variantCheck(model.SchemeHTTP,
		"example.com", "/", nil)
	got, err := DecideVariants(http, https)
	if err != nil {
		t.Fatal(err)
	}
	want := []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTP}}
	if !variantRecordsEqual(got, want) {
		t.Errorf("DecideVariants = %v, want %v", got, want)
	}
}

// TestDecideVariantsBadPair — the arguments must form an http+https pair
// by the Scheme field: equal or incorrect schemes are an error (silently
// mixed-up outcomes would give wrong variant/scheme in the DB).
func TestDecideVariantsBadPair(t *testing.T) {
	cases := []struct {
		name string
		a, b VariantInput
	}{
		{
			name: "https twice",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			b:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
		},
		{
			name: "http twice",
			a:    variantCheck(model.SchemeHTTP, "example.com", "/", nil),
			b:    variantDown(model.SchemeHTTP),
		},
		{
			name: "a zero scheme code",
			a:    variantDown(model.SchemeHTTPS),
			b:    VariantInput{Scheme: 0, Available: true, Host: "example.com", Path: "/"},
		},
		{
			name: "an unknown scheme code",
			a:    VariantInput{Scheme: model.Scheme(9), Available: true, Host: "a.example"},
			b:    variantDown(model.SchemeHTTP),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecideVariants(tc.a, tc.b)
			if err == nil {
				t.Fatalf("DecideVariants(%v, %v) = %v, want an error", tc.a.Scheme, tc.b.Scheme, got)
			}
			if got != nil {
				t.Errorf("DecideVariants: on an error the records = %v, want nil", got)
			}
		})
	}
}

// TestVariantSameSite — the sameness criterion: both OR branches on
// their own and the mandatory availability of the outcomes.
func TestVariantSameSite(t *testing.T) {
	cases := []struct {
		name string
		a, b VariantInput
		want bool
	}{
		{
			name: "unavailable outcomes — not the same site",
			a:    variantDown(model.SchemeHTTP),
			b:    variantDown(model.SchemeHTTPS),
			want: false,
		},
		{
			name: "an available and an unavailable one — not one site",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			b:    variantDown(model.SchemeHTTP),
			want: false,
		},
		{
			name: "host+path match — the hash is not needed",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/", nil),
			want: true,
		},
		{
			name: "a different path, an equal hash — a parking, one site",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", variantHash("p")),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/x", variantHash("p")),
			want: true,
		},
		{
			name: "a different host, an equal hash — one site",
			a:    variantCheck(model.SchemeHTTPS, "a.example.com", "/", variantHash("p")),
			b:    variantCheck(model.SchemeHTTP, "b.example.com", "/", variantHash("p")),
			want: true,
		},
		{
			name: "a different path, different hashes — different",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", variantHash("1")),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/en", variantHash("2")),
			want: false,
		},
		{
			name: "one hash nil — the hash branch is unprovable",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", variantHash("1")),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/en", nil),
			want: false,
		},
		{
			name: "both hashes nil, different paths — different",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/", nil),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/en", nil),
			want: false,
		},
		{
			name: "the host case is forgiven, the path is byte-exact",
			a:    variantCheck(model.SchemeHTTPS, "EXAMPLE.com", "/a b", nil),
			b:    variantCheck(model.SchemeHTTP, "example.COM", "/a b", nil),
			want: true,
		},
		{
			name: "the encoded and decoded path forms — different bytes",
			a:    variantCheck(model.SchemeHTTPS, "example.com", "/a%20b", nil),
			b:    variantCheck(model.SchemeHTTP, "example.com", "/a b", nil),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := VariantSameSite(tc.a, tc.b); got != tc.want {
				t.Errorf("VariantSameSite(%q%q, %q%q) = %v, want %v",
					tc.a.Host, tc.a.Path, tc.b.Host, tc.b.Path, got, tc.want)
			}
		})
	}
}

// TestNewVariantInput — the representation of the final URL in the
// variant logic: the host without the port, the path without the query
// and fragment, the percent-decoding of url.URL.Path, and the nil URL of
// an unavailable check.
func TestNewVariantInput(t *testing.T) {
	parse := func(t *testing.T, raw string) *url.URL {
		t.Helper()
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("url.Parse(%q): %v", raw, err)
		}
		return u
	}

	t.Run("the port is stripped, the query and fragment are dropped", func(t *testing.T) {
		in := NewVariantInput(model.SchemeHTTPS, true,
			parse(t, "https://www.example.com:443/a%20b?i=1#frag"), nil)
		if in.Host != "www.example.com" {
			t.Errorf("Host = %q, want www.example.com (no port)", in.Host)
		}
		if in.Path != "/a b" {
			t.Errorf("Path = %q, want the decoded %q without query/fragment", in.Path, "/a b")
		}
	})

	t.Run("a non-standard port does not split one site", func(t *testing.T) {
		https := NewVariantInput(model.SchemeHTTPS, true,
			parse(t, "https://example.com:8443/"), nil)
		http := NewVariantInput(model.SchemeHTTP, true,
			parse(t, "http://example.com:8080/"), nil)
		if !VariantSameSite(https, http) {
			t.Errorf("the outcomes of example.com:8443 and example.com:8080 — not one site, want one")
		}
	})

	t.Run("the query is outside the criterion: counter parameters — one site", func(t *testing.T) {
		https := NewVariantInput(model.SchemeHTTPS, true,
			parse(t, "https://example.com/"), nil)
		http := NewVariantInput(model.SchemeHTTP, true,
			parse(t, "http://example.com/?i=2"), nil)
		if !VariantSameSite(https, http) {
			t.Error("the paths / and /?i=2 counted as different sites, want one (the query is outside the criterion)")
		}
	})

	t.Run("an IPv6 literal without brackets and a port", func(t *testing.T) {
		in := NewVariantInput(model.SchemeHTTP, true,
			parse(t, "http://[2001:db8::1]:8080/"), nil)
		if in.Host != "2001:db8::1" {
			t.Errorf("Host = %q, want 2001:db8::1", in.Host)
		}
	})

	t.Run("a nil URL of an unavailable check — zero Host/Path", func(t *testing.T) {
		in := NewVariantInput(model.SchemeHTTP, false, nil, nil)
		if in.Host != "" || in.Path != "" || in.Available {
			t.Errorf("NewVariantInput(nil) = %+v, want zero Host/Path", in)
		}
		if VariantSameSite(in, in) {
			t.Error("an unavailable outcome called the same site, want false")
		}
	})
}

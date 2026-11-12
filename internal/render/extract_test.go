package render

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/chromedp/cdproto/network"
)

// The T-064 checks without a live Chromium: the transformations are pure (CDP cookies →
// the render record, CDP headers → http.Header), the capture orchestration runs on
// the extractCDP fakes (the Session's probeFn trick). The live capture checks on
// a real Chromium — extract_live_test.go.

func TestExtractCookieFields(t *testing.T) {
	// The full attribute set: the expiry is absolute → RFC 3339 UTC, the flags and
	// SameSite are preserved.
	got := extractCookieFields(&network.Cookie{
		Name:     "XSRF-TOKEN",
		Value:    "eyJpdiI6",
		Domain:   ".example.com",
		Path:     "/",
		Expires:  1792567680, // 2026-10-21T07:28:00Z
		HTTPOnly: true,
		Secure:   true,
		SameSite: network.CookieSameSiteLax,
	})
	want := ExtractCookie{
		Name: "XSRF-TOKEN", Value: "eyJpdiI6", Domain: ".example.com",
		Path: "/", Expires: "2026-10-21T07:28:00Z",
		HTTPOnly: true, Secure: true, SameSite: "lax",
	}
	if got != want {
		t.Fatalf("cookie:\n got %+v\nwant %+v", got, want)
	}

	cases := []struct {
		name string
		in   network.Cookie
		want ExtractCookie
	}{
		{
			name: "session-cookie без срока",
			in:   network.Cookie{Name: "a", Value: "1", Session: true, Expires: -1},
			want: ExtractCookie{Name: "a", Value: "1"},
		},
		{
			name: "отрицательный срок — тоже без expires",
			in:   network.Cookie{Name: "a", Value: "1", Expires: -1},
			want: ExtractCookie{Name: "a", Value: "1"},
		},
		{
			name: "samesite strict",
			in:   network.Cookie{Name: "a", SameSite: network.CookieSameSiteStrict},
			want: ExtractCookie{Name: "a", SameSite: "strict"},
		},
		{
			name: "samesite none",
			in:   network.Cookie{Name: "a", SameSite: network.CookieSameSiteNone},
			want: ExtractCookie{Name: "a", SameSite: "none"},
		},
		{
			name: "samesite не выставлен",
			in:   network.Cookie{Name: "a"},
			want: ExtractCookie{Name: "a"},
		},
		{
			// The Session flag overrides a positive expiry: expires is not
			// set even with the field filled in.
			name: "session-cookie со сроком — без expires",
			in:   network.Cookie{Name: "a", Value: "1", Session: true, Expires: 1792567680},
			want: ExtractCookie{Name: "a", Value: "1"},
		},
		{
			// The minimal positive expiry: a Unix second reads into RFC
			// 3339 (the "> 0" boundary — one included).
			name: "срок 1 — 1970-01-01T00:00:01Z",
			in:   network.Cookie{Name: "a", Value: "1", Expires: 1},
			want: ExtractCookie{Name: "a", Value: "1", Expires: "1970-01-01T00:00:01Z"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractCookieFields(&tc.in); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestExtractCookiesFromCDP(t *testing.T) {
	if got := extractCookiesFromCDP(nil); got != nil {
		t.Fatalf("пустой набор: %+v, хочу nil (NULL в jsonb)", got)
	}
	// The order is deterministic (name, domain, path) regardless of the arrival order.
	got := extractCookiesFromCDP([]*network.Cookie{
		{Name: "b", Domain: ".example.com", Path: "/"},
		{Name: "a", Domain: ".other.com", Path: "/"},
		{Name: "a", Domain: ".example.com", Path: "/deep"},
		{Name: "a", Domain: ".example.com", Path: "/"},
	})
	want := []ExtractCookie{
		{Name: "a", Domain: ".example.com", Path: "/"},
		{Name: "a", Domain: ".example.com", Path: "/deep"},
		{Name: "a", Domain: ".other.com", Path: "/"},
		{Name: "b", Domain: ".example.com", Path: "/"},
	}
	if len(got) != len(want) {
		t.Fatalf("размер %d, хочу %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("позиция %d: got %+v, want %+v", i, got[i], want[i])
		}
	}

	// A full duplicate of the sort key (name+domain+path) with different
	// values: the final Path comparison gets equal strings — the strict
	// `<` keeps the pair in the original order (sort.Slice on two elements is
	// an insertion sort, Less(1,0) is called exactly once and does not swap
	// on false); a non-strict `<=` would swap the elements.
	got = extractCookiesFromCDP([]*network.Cookie{
		{Name: "a", Domain: ".example.com", Path: "/", Value: "второй"},
		{Name: "a", Domain: ".example.com", Path: "/", Value: "первый"},
	})
	if len(got) != 2 || got[0].Value != "второй" || got[1].Value != "первый" {
		t.Errorf("порядок равных ключей: %+v", got)
	}
}

// TestExtractCookieJSONContract — the jsonb record shape matches the HTTP
// stage's (recordCookie, T-058): snake_case field names, the record's field
// order, omitempty for empty attributes.
func TestExtractCookieJSONContract(t *testing.T) {
	full := []ExtractCookie{{
		Name: "XSRF-TOKEN", Value: "eyJpdiI6", Domain: ".example.com",
		Path: "/", Expires: "2026-10-21T07:28:00Z",
		HTTPOnly: true, Secure: true, SameSite: "lax",
	}}
	got, err := json.Marshal(full)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `[{"name":"XSRF-TOKEN","value":"eyJpdiI6","path":"/",` +
		`"domain":".example.com","expires":"2026-10-21T07:28:00Z","secure":true,` +
		`"http_only":true,"same_site":"lax"}]`
	if string(got) != want {
		t.Fatalf("json:\n got  %s\nwant %s", got, want)
	}

	minimal := []ExtractCookie{{Name: "a", Value: "1"}}
	got, err = json.Marshal(minimal)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(got) != `[{"name":"a","value":"1"}]` {
		t.Fatalf("минимальная запись: %s", got)
	}
}

// TestExtractDOMExprLimited — the DOM capture expression with a length cap (R-02):
// the limit is moved into the browser itself, a heavy page does not pull a
// string over the limit through CDP; 0 — the expression without truncation.
func TestExtractDOMExprLimited(t *testing.T) {
	if got := extractDOMExprLimited(0); got != extractDOMExpr {
		t.Errorf("лимит 0: %q, хочу базовое выражение %q", got, extractDOMExpr)
	}
	if got := extractDOMExprLimited(-5); got != extractDOMExpr {
		t.Errorf("отрицательный лимит: %q, хочу базовое выражение %q", got, extractDOMExpr)
	}
	// The boundary: a limit of 1 — the minimal valid cap (does not degrade
	// into the unlimited expression).
	if got := extractDOMExprLimited(1); got != extractDOMExpr+".substring(0, 1)" {
		t.Errorf("лимит 1: %q, хочу %q", got, extractDOMExpr+".substring(0, 1)")
	}
	got := extractDOMExprLimited(2048)
	want := extractDOMExpr + ".substring(0, 2048)"
	if got != want {
		t.Errorf("выражение:\n got  %s\nwant %s", got, want)
	}
}

// TestTruncateDOM — the hard byte limit of the DOM in the worker's memory (R-02):
// over the limit — a cut with the truncation marker, the boundary does not tear a
// multibyte symbol; within the limit and without a limit — unchanged.
func TestTruncateDOM(t *testing.T) {
	if got := truncateDOM("<html/>", 0); got != "<html/>" {
		t.Errorf("без лимита: %q", got)
	}
	if got := truncateDOM("<html/>", 100); got != "<html/>" {
		t.Errorf("в лимите: %q", got)
	}

	got := truncateDOM(strings.Repeat("a", 100), 50)
	if len(got) != 50 || !strings.HasSuffix(got, extractDOMTruncated) {
		t.Errorf("обрезка: %d байт, суффикс %q", len(got), got[len(got)-len(extractDOMTruncated):])
	}

	// The cut boundary lands inside a multibyte symbol — a rollback to its
	// start: the result is valid UTF-8. The truncation marker is 31 bytes, cut =
	// 100-31 = 69 — an odd position inside the two-byte "é".
	multi := strings.Repeat("é", 200) // 2 bytes per symbol
	got = truncateDOM(multi, 100)
	if !utf8.ValidString(got) {
		t.Errorf("срез рвёт многобайтный символ: %q (валид: %v)", got, utf8.ValidString(got))
	}
	if !strings.HasSuffix(got, extractDOMTruncated) {
		t.Error("признак обрезки потерян при откате границы")
	}
	if want := strings.Repeat("é", 34); strings.TrimSuffix(got, extractDOMTruncated) != want {
		t.Errorf("откат границы: префикс %q, хочу %q", strings.TrimSuffix(got, extractDOMTruncated), want)
	}
}

// TestTruncateDOMBoundaries — the boundary cases of the hard limit: the length exactly
// at the limit, the minimal valid limit, a limit exactly the length of the
// truncation marker (31) and one byte more (32), the boundary rollback to zero on an invalid
// UTF-8. Each case pins the exact cut value.
func TestTruncateDOMBoundaries(t *testing.T) {
	// The length exactly at the limit — unchanged (the strict <= boundary).
	if got := truncateDOM(strings.Repeat("a", 50), 50); got != strings.Repeat("a", 50) {
		t.Errorf("ровно в лимите: %q, хочу без изменений", got)
	}
	// A limit of 1 — the minimal valid cap (smaller than the truncation
	// marker): the degenerate cut dom[:1].
	if got := truncateDOM("abc", 1); got != "a" {
		t.Errorf("лимит 1: %q, хочу %q", got, "a")
	}
	// A limit of 31 — exactly the marker length: cut = 0, the degenerate return dom[:31]
	// without the marker.
	if got := truncateDOM(strings.Repeat("a", 100), 31); got != strings.Repeat("a", 31) {
		t.Errorf("лимит 31: %q, хочу %q", got, strings.Repeat("a", 31))
	}
	// A limit of 32 over a single-byte string: cut = 1 lands on the symbol
	// start — the cut dom[:1] with the truncation marker.
	if got := truncateDOM(strings.Repeat("a", 100), 32); got != "a"+extractDOMTruncated {
		t.Errorf("лимит 32: %q, хочу %q", got, "a"+extractDOMTruncated)
	}
	// A limit of 32, dom[1] is a continuation byte: the boundary rolls back
	// to the rune start (cut down to 0) — the degenerate return of dom[:32]
	// in full, without the marker.
	dom := "a\x80" + strings.Repeat("a", 99)
	if got := truncateDOM(dom, 32); got != dom[:32] {
		t.Errorf("лимит 32 с продолжением на dom[1]: %q, хочу %q", got, dom[:32])
	}
	// An invalid UTF-8 of continuation bytes only: the boundary rollback
	// reaches zero — the degenerate return of dom[:50]; an out-of-bounds
	// access (dom[-1]) is impermissible.
	bad := strings.Repeat("\x80", 100)
	if got := truncateDOM(bad, 50); got != bad[:50] {
		t.Errorf("невалидный UTF-8: %q, хочу %q", got, bad[:50])
	}
}

func TestExtractResponseHeaders(t *testing.T) {
	if got := extractResponseHeaders(nil); got != nil {
		t.Fatalf("nil-заголовки: %+v, хочу nil", got)
	}
	if got := extractResponseHeaders(network.Headers{}); got != nil {
		t.Fatalf("пустые заголовки: %+v, хочу nil", got)
	}

	got := extractResponseHeaders(network.Headers{
		"content-type":   "text/html; charset=utf-8",
		"X-Powered-By":   "PHP/8.3",
		"content-length": float64(1234),
		"set-cookie":     []any{"a=1", "b=2"},
		// A big integer in a float64: the FormatInt branch (an integral value) must
		// give the decimal form, not the exponential "1e+18" of FormatFloat.
		"x-big-int": float64(1e18),
		// A fractional value — the FormatFloat branch: the shortest form "1.25"
		// (no integer rounding, no exponent).
		"x-fraction": float64(1.25),
	})
	want := http.Header{
		"Content-Type":   []string{"text/html; charset=utf-8"},
		"X-Powered-By":   []string{"PHP/8.3"},
		"Content-Length": []string{"1234"},
		// Multi-value headers — element by element, as in the HTTP stage (T-058):
		// a ", " glue is irreversible (commas in a cookie's Expires).
		"Set-Cookie": []string{"a=1", "b=2"},
		"X-Big-Int":  []string{"1000000000000000000"},
		"X-Fraction": []string{"1.25"},
	}
	if len(got) != len(want) {
		t.Fatalf("размер %d, хочу %d: %+v", len(got), len(want), got)
	}
	for k, vs := range want {
		g := got[k]
		if len(g) != len(vs) {
			t.Fatalf("%s = %v, хочу %v", k, g, vs)
		}
		for i := range vs {
			if g[i] != vs[i] {
				t.Errorf("%s[%d] = %q, хочу %q", k, i, g[i], vs[i])
			}
		}
	}
}

// fakeExtractCDP — extractCDP with faked commands and call accounting.
type fakeExtractCDP struct {
	dom     string
	domErr  error
	domSeen bool
	// domLimit — the fake's DOM cap (truncateDOM in extractResult).
	domLimit int

	cookies    []*network.Cookie
	cookiesErr error
	cookieSeen bool
}

func (f *fakeExtractCDP) cdp() extractCDP {
	return extractCDP{
		domLimit: f.domLimit,
		dom: func(context.Context) (string, error) {
			f.domSeen = true
			return f.dom, f.domErr
		},
		cookies: func(_ context.Context, _ string) ([]*network.Cookie, error) {
			f.cookieSeen = true
			return f.cookies, f.cookiesErr
		},
	}
}

func TestExtractResult(t *testing.T) {
	t.Run("DOM и cookies итоговой страницы", func(t *testing.T) {
		f := &fakeExtractCDP{
			dom:     "<html><body>итог</body></html>",
			cookies: []*network.Cookie{{Name: "js", Value: "1", HTTPOnly: true}},
		}
		out := RenderOutput{FinalURL: "https://example.com/final"}
		if err := f.cdp().extractResult(context.Background(), &out); err != nil {
			t.Fatalf("extractResult: %v", err)
		}
		if out.DOM != f.dom {
			t.Errorf("DOM %q", out.DOM)
		}
		if len(out.Cookies) != 1 || out.Cookies[0] != (ExtractCookie{Name: "js", Value: "1", HTTPOnly: true}) {
			t.Errorf("cookies %+v", out.Cookies)
		}
	})

	t.Run("пустая страница — пустой DOM без ошибки", func(t *testing.T) {
		f := &fakeExtractCDP{}
		out := RenderOutput{FinalURL: "https://example.com/"}
		if err := f.cdp().extractResult(context.Background(), &out); err != nil {
			t.Fatalf("extractResult: %v", err)
		}
		if out.DOM != "" {
			t.Errorf("DOM %q, хочу пустой", out.DOM)
		}
	})

	t.Run("ошибка evaluate", func(t *testing.T) {
		f := &fakeExtractCDP{domErr: errors.New("сбой вкладки")}
		out := RenderOutput{FinalURL: "https://example.com/"}
		err := f.cdp().extractResult(context.Background(), &out)
		if err == nil || !strings.Contains(err.Error(), "итоговый DOM") {
			t.Fatalf("ошибка: %v", err)
		}
		if !errors.Is(err, f.domErr) {
			t.Errorf("исходная ошибка не пробита: %v", err)
		}
		if f.cookieSeen {
			t.Error("после сбоя DOM cookies не запрашиваются")
		}
	})

	t.Run("ошибка cookies", func(t *testing.T) {
		f := &fakeExtractCDP{cookiesErr: errors.New("cookie store недоступен")}
		out := RenderOutput{FinalURL: "https://example.com/"}
		err := f.cdp().extractResult(context.Background(), &out)
		if err == nil || !strings.Contains(err.Error(), "cookies") {
			t.Fatalf("ошибка: %v", err)
		}
	})

	t.Run("DOM сверх лимита обрезается с признаком", func(t *testing.T) {
		// R-02: a heavy page does not keep a DOM over the limit in the worker's
		// memory — a cut by the byte limit + the truncation marker.
		f := &fakeExtractCDP{dom: strings.Repeat("x", 3000), domLimit: 1024}
		out := RenderOutput{FinalURL: "https://example.com/"}
		if err := f.cdp().extractResult(context.Background(), &out); err != nil {
			t.Fatalf("extractResult: %v", err)
		}
		if len(out.DOM) != 1024 {
			t.Fatalf("длина DOM %d, хочу лимит 1024", len(out.DOM))
		}
		if !strings.HasSuffix(out.DOM, extractDOMTruncated) {
			t.Fatalf("DOM без признака обрезки: …%q", out.DOM[len(out.DOM)-40:])
		}
	})

	t.Run("DOM в лимите — без обрезки и признака", func(t *testing.T) {
		f := &fakeExtractCDP{dom: "<html>ok</html>", domLimit: 1024}
		out := RenderOutput{FinalURL: "https://example.com/"}
		if err := f.cdp().extractResult(context.Background(), &out); err != nil {
			t.Fatalf("extractResult: %v", err)
		}
		if out.DOM != f.dom || strings.Contains(out.DOM, "truncated") {
			t.Fatalf("DOM %q, хочу без изменений", out.DOM)
		}
	})

	t.Run("без http(s) cookies не запрашиваются", func(t *testing.T) {
		for _, final := range []string{"data:text/html,hello", "about:blank"} {
			f := &fakeExtractCDP{cookies: []*network.Cookie{{Name: "x"}}}
			out := RenderOutput{FinalURL: final}
			if err := f.cdp().extractResult(context.Background(), &out); err != nil {
				t.Fatalf("%s: extractResult: %v", final, err)
			}
			if f.cookieSeen {
				t.Errorf("%s: cookies запрошены для схемы без хранилища", final)
			}
			if out.Cookies != nil {
				t.Errorf("%s: cookies %+v, хочу nil", final, out.Cookies)
			}
		}
	})

	t.Run("http-схема — cookies запрашиваются", func(t *testing.T) {
		// Both allowed schemes lead into the cookie store: not https only.
		f := &fakeExtractCDP{cookies: []*network.Cookie{{Name: "h", Value: "1"}}}
		out := RenderOutput{FinalURL: "http://example.com/page"}
		if err := f.cdp().extractResult(context.Background(), &out); err != nil {
			t.Fatalf("extractResult: %v", err)
		}
		if !f.cookieSeen {
			t.Fatal("cookies не запрошены для http-схемы")
		}
		if len(out.Cookies) != 1 || out.Cookies[0] != (ExtractCookie{Name: "h", Value: "1"}) {
			t.Fatalf("cookies %+v", out.Cookies)
		}
	})
}

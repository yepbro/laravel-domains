// Capture of the render result (T-064; spec 5.6 "The result: the final DOM
// (document.documentElement.outerHTML) and all cookies (including those set
// by JS) via CDP", acceptance criterion §11.4 item 3) and the response headers of the main
// navigation. The headers are a requirement of spec 5.8 (header rules): for a
// rendered page the final document is not obliged to match what the
// HTTP stage captured, so the render must hand over its own.
//
// The capture methods chosen:
//
//   - DOM — chromedp.Evaluate(`document.documentElement.outerHTML`):
//     serialization of the live tree after JS finishing touches (elements, attributes and
//     texts introduced by scripts are part of the result), the format is the same
//     HTML serializer of Chrome that the browser uses when saving a page,
//     i.e. textual HTML. The result goes straight into the content
//     store (replacing the raw HTML, T-067) and into the rule matcher over HTML/meta
//     (T-068) without transformations. The DOM.getFlattenedDocument alternative —
//     a node tree with nodeId that would have to be
//     serialized back (DOM.getOuterHTML per node) — more complex and gives no
//     ready HTML. The outerHTML limitation — shadow trees are not
//     expanded, but the Laravel rules work over the light DOM.
//
//   - cookies — Network.getCookies (CDP) with the URL of the final page: the command
//     reads the cookie store of the isolated context, not
//     document.cookie — httpOnly and the ones set by the page via JS are visible
//     (spec 5.6 "all cookies"; document.cookie does not return httpOnly).
//     The Network.getAllCookies mentioned in the decomposition was removed from the current
//     protocol version (the command is absent in cdproto v0.157.8); Network.getCookies
//     over the tab's session reads the same context store, and the explicit URL
//     of the final page yields the "this page's cookies" set without subframe noise.
//
//   - headers — the response of the main navigation from Network.responseReceived:
//     chromedp.NavigateResponse (instead of the expectation-equivalent Navigate)
//     tracks the loader/request chain across HTTP redirects and returns
//     the response of the final document. A separate event subscription is not needed.
//     We mean the response of the navigation itself: a JS redirect after the load is no
//     longer tracked (its outcome is visible in FinalURL, and the headers are those of the
//     document whose content is rendered). The headers from responseReceivedExtraInfo
//     (the report-only set) are not requested: for the spec 5.8 rules the
//     main set is enough.
package render

import (
	"context"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// extractDOMExpr — the final DOM capture expression (spec 5.6): the outerHTML
// of the root element — the full tree after the JS finishing touches.
const extractDOMExpr = `document.documentElement.outerHTML`

// extractDOMTruncated — the marker of truncating an over-limit DOM (R-02):
// an HTML comment at the end of the document — the analyzer (the HTML rules)
// and the operator see that the tree was not captured in full.
const extractDOMTruncated = "<!-- crawler: dom truncated -->"

// extractDOMExprLimited — the capture expression with a length cap: a heavy or
// hostile page (an SPA monster, bloated JS) must not pull a string over the
// limit through CDP — the truncation happens inside the browser itself. substring counts
// UTF-16 units, so the exact byte limit is tightened in Go
// (truncateDOM).
func extractDOMExprLimited(limit int) string {
	if limit <= 0 {
		return extractDOMExpr
	}
	return extractDOMExpr + fmt.Sprintf(".substring(0, %d)", limit)
}

// truncateDOM — the hard byte limit of the final DOM in the worker's memory:
// at MemoryMax=128M and render.contexts=4 an unlimited DOM (plus
// the chromedp transient buffer and the hash/Put copies) knocks out the cgroup budget
// with an OOM kill. Over the limit the DOM is truncated and gets the truncation
// marker; the cut boundary is rolled back to the start of a multibyte symbol.
func truncateDOM(dom string, limit int) string {
	if limit <= 0 || len(dom) <= limit {
		return dom
	}
	cut := limit - len(extractDOMTruncated)
	// Equivalent mutants of the loop line (T-M09): `cut >= 0` and `cut > -1`
	// differ from `cut > 0` only by the iteration at cut == 0 — there
	// !utf8.RuneStart(dom[0]) is checked; for a valid UTF-8 dom[0] is
	// a rune start, the loop exits with the same cut == 0; for an invalid
	// one (dom[0] is a continuation byte) cut goes to -1, and the cut <= 0 branch below
	// returns the same dom[:limit]. The incrementer (`cut > 1`) and cutting out
	// the left half (`true` — with an invalid UTF-8 it goes into dom[-1]) are
	// not killed here: they are dead via the boundary cases of TestTruncateDOMBoundaries.
	// mutator-disable-next-line expression/comparison, numbers/decrementer
	for cut > 0 && !utf8.RuneStart(dom[cut]) {
		cut-- // do not tear a multibyte symbol in the middle
	}
	if cut <= 0 {
		return dom[:limit] // a degenerate limit smaller than the truncation marker
	}
	return dom[:cut] + extractDOMTruncated
}

// ExtractCookie — a cookie of the final page in the format of the
// subdomain_results.cookies column (jsonb). The shape mirrors the HTTP stage's
// record (recordCookie, T-058) — a single storage contract: name/value are the
// targets of the analyzer rules (spec 5.8), the attributes are the same JSON field
// names. max_age is not filled in by the render: the browser store (which CDP reads
// from) keeps only the absolute expiry, a relative Max-Age is lost when the cookie is
// saved; the field is kept so that the record shape matches the HTTP stage fully.
type ExtractCookie struct {
	Name     string `json:"name"`
	Value    string `json:"value,omitempty"`
	Path     string `json:"path,omitempty"`
	Domain   string `json:"domain,omitempty"`
	Expires  string `json:"expires,omitempty"` // RFC 3339, UTC
	MaxAge   int    `json:"max_age,omitempty"`
	Secure   bool   `json:"secure,omitempty"`
	HTTPOnly bool   `json:"http_only,omitempty"`
	SameSite string `json:"same_site,omitempty"`
}

// extractSameSite — the SameSite attribute name in the HTTP stage's encoding
// (recordSameSite, T-058): not set — an empty string (omitempty).
func extractSameSite(s network.CookieSameSite) string {
	switch s {
	case network.CookieSameSiteLax:
		return "lax"
	case network.CookieSameSiteStrict:
		return "strict"
	case network.CookieSameSiteNone:
		return "none"
	default:
		return ""
	}
}

// extractCookieFields — a CDP cookie into an ExtractCookie. The CDP expiry is
// absolute (unix seconds): a session cookie (no expiry) stays without expires,
// a negative value is the same absence of an expiry.
func extractCookieFields(c *network.Cookie) ExtractCookie {
	ec := ExtractCookie{
		Name:     c.Name,
		Value:    c.Value,
		Domain:   c.Domain,
		Path:     c.Path,
		Secure:   c.Secure,
		HTTPOnly: c.HTTPOnly,
		SameSite: extractSameSite(c.SameSite),
	}
	if !c.Session && c.Expires > 0 {
		// Equivalent mutant (T-M09): the nanoseconds in time.Unix are unobservable
		// — RFC 3339 of second granularity discards them (Unix(sec, 0) and
		// Unix(sec, 1) format the same). The `Expires > 0` boundary,
		// cutting out `!c.Session` and the seconds are pinned by TestExtractCookieFields.
		// mutator-disable-next-line numbers/incrementer
		ec.Expires = time.Unix(int64(c.Expires), 0).UTC().Format(time.RFC3339)
	}
	return ec
}

// extractCookiesFromCDP — CDP cookies into the render record with a deterministic
// order (name, domain, path): the jsonb set is stable regardless of
// the cookie order in the store's response. An empty set — nil (NULL in jsonb).
func extractCookiesFromCDP(cs []*network.Cookie) []ExtractCookie {
	if len(cs) == 0 {
		return nil
	}
	out := make([]ExtractCookie, 0, len(cs))
	for _, c := range cs {
		out = append(out, extractCookieFields(c))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			// Equivalent mutant (T-M09): the line is reached only with
			// different Names (the inequality guard above) — for different strings `<` and
			// `<=` are indistinguishable. The final one (Path below) is not killed:
			// equal paths of indistinguishable cookies are pinned by the
			// TestExtractCookiesFromCDP test.
			// mutator-disable-next-line expression/comparison
			return out[i].Name < out[j].Name
		}
		if out[i].Domain != out[j].Domain {
			// Equivalent mutant (T-M09): the same argument — the
			// Domain inequality guard above leaves only different strings.
			// mutator-disable-next-line expression/comparison
			return out[i].Domain < out[j].Domain
		}
		return out[i].Path < out[j].Path
	})
	return out
}

// extractResponseHeaders — the response headers of the main navigation into
// http.Header: a single contract with the HTTP stage (subdomain_results.headers,
// T-058 — canonical key names, the values as a list). CDP hands over the
// headers as a name→value object (the values are almost always strings, numbers
// are rare); repeated headers (Set-Cookie first of all) arrive
// as an array — the array is stored element by element, as in the HTTP stage: gluing
// with a comma is irreversible (cookie values contain commas —
// Expires=Wed, 21 Oct 2025 07:28:00 GMT) and diverges from the jsonb shape
// the analyzer reads (map[string][]string, T-058/T-069).
func extractResponseHeaders(h network.Headers) http.Header {
	if len(h) == 0 {
		return nil
	}
	out := make(http.Header, len(h))
	for k, v := range h {
		out[textproto.CanonicalMIMEHeaderKey(k)] = extractHeaderValues(v)
	}
	return out
}

// extractHeaderValues — header values from CDP as a list: a single
// value (string/number) — a slice of one element, an array — element by element
// (the "values as a list" contract of T-058; the render mirrors the HTTP stage's
// record, where repeated headers are separate elements).
func extractHeaderValues(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return []string{extractHeaderString(v)}
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		out = append(out, extractHeaderString(item))
	}
	return out
}

// extractHeaderString — a header value from CDP into a string (the json decoder
// gives string/float64; CDP does not hand over other shapes — a fallback Sprintf).
func extractHeaderString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		// Equivalent mutant (T-M09): a precision of -2, like -1, is negative
		// — the Go implementation treats any negative precision as
		// the shortest form (ftoa.go: shortest := prec < 0; the documentation
		// of strconv specifically mentions only -1). The line granularity
		// also kills the bitSize 64 → 65 incrementer (an illegal bitSize panic) —
		// the trade-off of §4 docs/mutation-testing.md: the defect class is covered
		// by the 64 → 63 decrementer, which is not killed and is dead via the fractional
		// value in TestExtractResponseHeaders (together with the
		// -1 → 0 precision: "1" instead of "1.25").
		// mutator-disable-next-line numbers/incrementer
		return strconv.FormatFloat(x, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// extractCDP — the CDP commands of the result capture; a struct instead of a package so
// that the unit tests substitute the fields with fakes (the same trick as the Session's
// probeFn).
type extractCDP struct {
	// dom — evaluating the final DOM expression in the tab's context.
	dom func(ctx context.Context) (string, error)
	// cookies — the cookies of the context store applicable to the URL.
	cookies func(ctx context.Context, pageURL string) ([]*network.Cookie, error)
	// domLimit — the DOM length cap in bytes, 0 — no limit: the capture
	// expression and truncateDOM (R-02).
	domLimit int
}

// newExtractCDP — the real CDP commands (run over the tab's context of the
// session); domLimit — the final DOM cap (Options.MaxDOMBytes).
func newExtractCDP(domLimit int) extractCDP {
	return extractCDP{
		domLimit: domLimit,
		dom: func(ctx context.Context) (string, error) {
			return chromedp.Run(ctx, chromedp.Evaluate[string](extractDOMExprLimited(domLimit)))
		},
		cookies: func(ctx context.Context, pageURL string) ([]*network.Cookie, error) {
			var out []*network.Cookie
			err := chromedp.Do(ctx, chromedp.Func(func(c context.Context, t *chromedp.Target) error {
				res, err := cdp.Call(c, t, network.GetCookies, network.GetCookiesParams{
					URLs: []string{pageURL},
				})
				out = res.Cookies
				return err
			}))
			return out, err
		},
	}
}

// extractResult — the final stage of Session.Run (T-064): the final DOM and all
// cookies of the final page into out. The DOM is mandatory (without it the result is
// pointless) and bounded by the Options.MaxDOMBytes cap (truncation with the marker,
// R-02); the cookies are captured by FinalURL; tasks are http/https only
// (validateTaskURL, R-09); a non-http final URL is a defensive case:
// no cookie set exists, the step is skipped without an error.
func (e extractCDP) extractResult(ctx context.Context, out *RenderOutput) error {
	dom, err := e.dom(ctx)
	if err != nil {
		return fmt.Errorf("the final DOM (%s): %w", extractDOMExpr, err)
	}
	out.DOM = truncateDOM(dom, e.domLimit)

	if u, perr := url.Parse(out.FinalURL); perr == nil &&
		(u.Scheme == "http" || u.Scheme == "https") {
		cs, err := e.cookies(ctx, out.FinalURL)
		if err != nil {
			return fmt.Errorf("cookies of the final page %s: %w", out.FinalURL, err)
		}
		out.Cookies = extractCookiesFromCDP(cs)
	}
	return nil
}

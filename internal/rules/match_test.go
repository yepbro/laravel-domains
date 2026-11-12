package rules

import (
	"context"
	"strings"
	"testing"
)

func bgCtx() context.Context { return context.Background() }

func TestMatchRuleCookieName(t *testing.T) {
	cases := []struct {
		name string
		rule Rule
		page Page
		want bool
		ev   string
	}{
		{
			"an exact match",
			Rule{Target: TargetCookieName, Match: "XSRF-TOKEN"},
			Page{Cookies: []Cookie{{Name: "XSRF-TOKEN", Value: "x"}}},
			true, "cookie XSRF-TOKEN",
		},
		{
			"the exact match did not fire",
			Rule{Target: TargetCookieName, Match: "XSRF-TOKEN"},
			Page{Cookies: []Cookie{{Name: "xsrf-token", Value: "x"}}}, // case matters
			false, "",
		},
		{
			"a regex over a name",
			Rule{Target: TargetCookieName, Regex: `^[a-z0-9_\-]+_session$`},
			Page{Cookies: []Cookie{
				{Name: "PHPSESSID", Value: "1"},
				{Name: "laravel_session", Value: "2"},
			}},
			true, "cookie laravel_session",
		},
		{
			"the first match in slice order",
			Rule{Target: TargetCookieName, Match: "b_session"},
			Page{Cookies: []Cookie{
				{Name: "b_session", Value: "1"},
				{Name: "a_session", Value: "2"},
			}},
			true, "cookie b_session",
		},
	}
	for _, c := range cases {
		ev, ok := matchRule(bgCtx(), c.rule, c.page, DefaultLimits())
		if ok != c.want {
			t.Fatalf("%s: ok=%v, want %v", c.name, ok, c.want)
		}
		if ok && ev != c.ev {
			t.Errorf("%s: evidence=%q, want %q", c.name, ev, c.ev)
		}
	}
}

func TestMatchRuleCookieValue(t *testing.T) {
	// regex over the value: evidence with the name and the value prefix
	r := Rule{Target: TargetCookieValue, Regex: `^eyJpdiI6`}
	ev, ok := matchRule(bgCtx(), r, Page{Cookies: []Cookie{
		{Name: "PHPSESSID", Value: "plain"},
		{Name: "laravel_session", Value: "eyJpdiI6" + strings.Repeat("x", 200)},
	}}, DefaultLimits())
	if !ok {
		t.Fatal("a hit on the encrypted value is expected")
	}
	if ev != "cookie laravel_session=eyJpdiI6"+strings.Repeat("x", 33)+"…" {
		t.Errorf("an unexpected evidence: %q", ev)
	}

	// exact value match (match) is supported too
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetCookieValue, Match: "exact"}, Page{Cookies: []Cookie{
		{Name: "c", Value: "exact"},
	}}, DefaultLimits())
	if !ok || ev != "cookie c=exact" {
		t.Errorf("match over a value: ok=%v ev=%q", ok, ev)
	}
}

func TestMatchRuleHeader(t *testing.T) {
	page := Page{Headers: map[string][]string{
		"Content-Type":  {"text/html"},
		"X-Powered-By":  {"PHP/8.2.8"},
		"Set-Cookie":    {"a=1", "b=2"},
		"X-Laravel":     {"yes"},
		"Cache-Control": {"no-cache"},
	}}

	// exact name match
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetHeaderName, Match: "X-Laravel"}, page, DefaultLimits())
	if !ok || ev != "header X-Laravel" {
		t.Errorf("a name by match: ok=%v ev=%q", ok, ev)
	}

	// regex over the name
	_, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderName, Regex: `^X-Powered`}, page, DefaultLimits())
	if !ok {
		t.Error("the regex over a header name did not fire")
	}

	// regex over the value: evidence with the header name
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `^PHP/`}, page, DefaultLimits())
	if !ok || ev != "header X-Powered-By: PHP/8.2.8" {
		t.Errorf("a value by regex: ok=%v ev=%q", ok, ev)
	}

	// several values of one header: the first one in slice order
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `^[ab]=`}, page, DefaultLimits())
	if !ok || ev != "header Set-Cookie: a=1" {
		t.Errorf("a multi-value header: ok=%v ev=%q", ok, ev)
	}

	// exact value match
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Match: "no-cache"}, page, DefaultLimits())
	if !ok || ev != "header Cache-Control: no-cache" {
		t.Errorf("a value by match: ok=%v ev=%q", ok, ev)
	}
}

func TestMatchRuleHTML(t *testing.T) {
	page := Page{HTML: []byte(`<html><head><script src="/build/assets/app-DJIrOSj4.js"></script></head></html>`)}

	// the evidence is the matched fragment itself
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: `/build/assets/[^"']+\.js`}, page, DefaultLimits())
	if !ok || ev != "/build/assets/app-DJIrOSj4.js" {
		t.Errorf("html: ok=%v ev=%q", ok, ev)
	}

	// an html target with match instead of regex does not hit (spec 5.8: regex over HTML)
	_, ok = matchRule(bgCtx(), Rule{Target: TargetHTML, Match: "/build/"}, page, DefaultLimits())
	if ok {
		t.Error("html + match must not fire")
	}
}

func TestMatchRuleMeta(t *testing.T) {
	page := Page{HTML: []byte(
		`<html><head>
		<meta charset="utf-8">
		<meta name="viewport" content="width=device-width">
		<meta name="csrf-token" content="eyJ0eXAiOiJKV1Qi">
		</head><body>csrf-token в тексте не считается meta-целью</body></html>`)}

	// the evidence is the whole meta tag (before the evidence limit truncation)
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `csrf-token`}, page, DefaultLimits())
	if !ok {
		t.Fatal("the meta tag was not found")
	}
	if ev != `<meta name="csrf-token" content="eyJ0eXAiOiJKV1Qi">` {
		t.Errorf("an unexpected evidence: %q", ev)
	}

	// meta + match does not hit (regex only, like html)
	_, ok = matchRule(bgCtx(), Rule{Target: TargetMeta, Match: "csrf-token"}, page, DefaultLimits())
	if ok {
		t.Error("meta + match must not fire")
	}

	// ">" inside the quoted attribute value does not terminate the tag
	// (AN-08): the regex over the name after a value containing ">" hits,
	// the evidence is the whole tag
	quoted := Page{HTML: []byte(`<meta content="a>b" name="csrf-token">`)}
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `csrf-token`}, quoted, DefaultLimits())
	if !ok || ev != `<meta content="a>b" name="csrf-token">` {
		t.Errorf("meta with «>» in quotes: ok=%v ev=%q", ok, ev)
	}
	// single quotes — the same semantics
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `csrf-token`},
		Page{HTML: []byte(`<meta content='a>b' name='csrf-token'>`)}, DefaultLimits())
	if !ok || ev != `<meta content='a>b' name='csrf-token'>` {
		t.Errorf("meta with «>» in single quotes: ok=%v ev=%q", ok, ev)
	}
	// an unterminated quote — no tag is extracted at all (no cut at ">")
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `unterminated`},
		Page{HTML: []byte(`<meta content="unterminated>`)}, DefaultLimits()); ok {
		t.Error("an unclosed quote must not yield a truncated meta tag")
	}
}

func TestMatchRuleEvidenceFromCappedInput(t *testing.T) {
	lim := DefaultLimits()
	longName := strings.Repeat("n", 100) // > evidenceNameBytes

	// name-only: the name is capped to 32 bytes inside the evidence too
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetCookieName, Match: longName},
		Page{Cookies: []Cookie{{Name: longName}}}, lim)
	if !ok {
		t.Fatal("an exact match over a long name")
	}
	if want := "cookie " + truncateUTF8(longName, evidenceNameBytes); ev != want {
		t.Errorf("the name in the evidence is not truncated: %q, want %q", ev, want)
	}
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderName, Match: longName},
		Page{Headers: map[string][]string{longName: {"v"}}}, lim)
	if !ok {
		t.Fatal("an exact match over a long header name")
	}
	if want := "header " + truncateUTF8(longName, evidenceNameBytes); ev != want {
		t.Errorf("the header name is not truncated: %q, want %q", ev, want)
	}

	// value: the evidence is built from the MaxValueBytes-capped value (the
	// same string the matching ran on), not from the full c.Value: a value
	// longer than the limit still hits via the anchor at the start and
	// yields evidence within the truncation window.
	huge := "eyJpdiI6" + strings.Repeat("z", lim.MaxValueBytes)
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetCookieValue, Regex: `^eyJpdiI6`},
		Page{Cookies: []Cookie{{Name: "c", Value: huge}}}, lim)
	if !ok {
		t.Fatal("a value longer than MaxValueBytes was not matched by the prefix")
	}
	// the truncation window: the form prefix + the start of the value up to the evidence limit
	zCut := func(prefix string) string {
		return prefix + strings.Repeat("z", lim.snippetCap()-len(prefix)) + "…"
	}
	if ev != zCut("cookie c=eyJpdiI6") {
		t.Errorf("the value evidence: %q", ev)
	}
	if ev2, ok := matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `^eyJpdiI6`},
		Page{Headers: map[string][]string{"X-Long": {huge}}}, lim); !ok ||
		ev2 != zCut("header X-Long: eyJpdiI6") {
		t.Errorf("the header value evidence: ok=%v %q", ok, ev2)
	}
}

func TestMatchRuleSkipsBadRules(t *testing.T) {
	page := Page{HTML: []byte("x"), Cookies: []Cookie{{Name: "a", Value: "b"}}}

	// a non-compilable regex — the rule is skipped without a panic
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: `[unclosed`}, page, DefaultLimits()); ok {
		t.Error("a non-compilable regex must not fire")
	}

	// a too long pattern — the MaxPatternBytes limit (spec §9)
	long := strings.Repeat("a", 4097)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: long}, page, DefaultLimits()); ok {
		t.Error("a pattern over the limit must not fire")
	}

	// a too long pattern that WOULD match: the limit cuts it off before
	// compilation — a bloated pattern must not match even when there is a
	// match
	longMatch := strings.Repeat("a", DefaultLimits().MaxPatternBytes+1)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: longMatch},
		Page{HTML: []byte(strings.Repeat("a", 5000))}, DefaultLimits()); ok {
		t.Error("a pattern over the limit must not fire even on a match")
	}

	// match + an invalid regex: the rule is spoiled as a whole; degrading
	// to partial application (the exact match only) is not allowed
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieName, Match: "XSRF-TOKEN", Regex: `[unclosed`},
		Page{Cookies: []Cookie{{Name: "XSRF-TOKEN"}}}, DefaultLimits()); ok {
		t.Error("a rule with an invalid regex must not fire by match")
	}

	// an unknown target — no panic and no hit
	if _, ok := matchRule(bgCtx(), Rule{Target: "body", Regex: "x"}, page, DefaultLimits()); ok {
		t.Error("an unknown target must not fire")
	}

	// a rule without match and regex simply does not match; cookie targets
	// also exercise the nil-regex branch in matches (the html target never
	// calls it)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML}, page, DefaultLimits()); ok {
		t.Error("an empty rule must not fire")
	}
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieName},
		Page{Cookies: []Cookie{{Name: "a"}}}, DefaultLimits()); ok {
		t.Error("an empty rule must not fire on a cookie target")
	}
}

func TestMatchRuleHTMLSizeCap(t *testing.T) {
	// a match beyond the limit is not looked up; at the boundary — it is
	head := strings.Repeat(" ", 100)
	html := []byte(head + "NEEDLE")
	lim := DefaultLimits()
	lim.MaxHTMLBytes = 100

	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: "NEEDLE"}, Page{HTML: html}, lim); ok {
		t.Error("a match beyond the HTML limit was found")
	}
	lim.MaxHTMLBytes = 106 // NEEDLE fits entirely
	if ev, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: "NEEDLE"}, Page{HTML: html}, lim); !ok || ev != "NEEDLE" {
		t.Errorf("a match within the limit was not found: ok=%v ev=%q", ok, ev)
	}

	// meta beyond the limit is not examined either
	metaHTML := []byte(head + `<meta name="csrf-token">`)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: "csrf"}, Page{HTML: metaHTML}, lim); ok {
		t.Error("meta beyond the HTML limit was found")
	}
}

func TestMatchRuleValueSizeCap(t *testing.T) {
	lim := DefaultLimits()
	lim.MaxValueBytes = 8

	// an anchor at the start: the truncation is invisible
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieValue, Regex: `^eyJ`}, Page{Cookies: []Cookie{
		{Name: "c", Value: "eyJ" + strings.Repeat("z", 100)},
	}}, lim); !ok {
		t.Error("a value prefix within the limit was not found")
	}

	// an anchor at the end beyond the limit does not hit — the documented
	// trade of completeness for the time guarantee (the TAIL tail is capped
	// and invisible)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieValue, Regex: `TAIL$`}, Page{Cookies: []Cookie{
		{Name: "c", Value: strings.Repeat("z", 100) + "TAIL"},
	}}, lim); ok {
		t.Error("a value tail beyond the limit must not be found")
	}

	// headers are bounded by the same limit
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `END$`}, Page{Headers: map[string][]string{
		"X-Long": {strings.Repeat("t", 100) + "END"},
	}}, lim); ok {
		t.Error("a header value tail beyond the limit must not be found")
	}
}

func TestMatchRuleContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	page := Page{
		HTML:    []byte(`<meta name="csrf-token">`),
		Cookies: []Cookie{{Name: "XSRF-TOKEN"}},
		Headers: map[string][]string{"X-Laravel": {"yes"}},
	}
	// cancellation before the start: no target is examined
	for _, r := range []Rule{
		{Target: TargetCookieName, Match: "XSRF-TOKEN"},
		{Target: TargetCookieValue, Regex: "XSRF"},
		{Target: TargetHeaderName, Match: "X-Laravel"},
		{Target: TargetHeaderValue, Regex: "yes"},
		{Target: TargetHTML, Regex: "csrf"},
		{Target: TargetMeta, Regex: "csrf"},
	} {
		if _, ok := matchRule(ctx, r, page, DefaultLimits()); ok {
			t.Errorf("a cancelled context: the target %s fired", r.Target)
		}
	}

	// cancellation inside the cookie scan: a check on every iteration, the
	// matching cookie is not examined
	partial := Page{Cookies: []Cookie{
		{Name: "first", Value: "1"},
		{Name: "XSRF-TOKEN"},
	}}
	if _, ok := matchRule(ctx, Rule{Target: TargetCookieName, Match: "XSRF-TOKEN"}, partial, DefaultLimits()); ok {
		t.Error("the cookie iteration must stop on the context")
	}
}

func TestCompileCache(t *testing.T) {
	pattern := `^eyJpdiI6$`
	re1, err1 := compileCached(pattern)
	if err1 != nil || re1 == nil {
		t.Fatalf("the first compile: %v", err1)
	}
	re2, err2 := compileCached(pattern)
	if err2 != nil || re2 != re1 {
		t.Error("a repeated call must return the same compiled pattern")
	}

	// a compilation error is cached: the same pattern yields the error without a panic
	bad := `[unclosed`
	if _, err := compileCached(bad); err == nil {
		t.Error("a compile error is expected")
	}
	if _, err := compileCached(bad); err == nil {
		t.Error("the cached error must reproduce")
	}
	if v, ok := compileCache.Load(bad); !ok || v.(compileResult).err == nil {
		t.Error("the error must sit in the cache")
	}
}

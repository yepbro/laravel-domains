package rules

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// evalSet builds a set from a rule yaml body (the engine does not need
// LoadDir validation — we test matching, not loading).
func evalSet(t *testing.T, body string) *Set {
	t.Helper()
	set, err := LoadDir(writeRules(t, body))
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestEvaluateAllTargets(t *testing.T) {
	set := evalSet(t, `
rules:
  - id: cookie_xsrf_token
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
  - id: cookie_encrypted_payload
    target: cookie_value
    regex: "^eyJpdiI6"
    weight: 0.45
  - id: header_laravel
    target: header_name
    match: "X-Laravel"
    weight: 0.9
  - id: header_php
    target: header_value
    regex: "^PHP/8"
    weight: 0.3
  - id: meta_csrf_token
    target: meta
    regex: 'csrf-token'
    weight: 0.2
  - id: vite_assets
    target: html
    regex: '/build/assets/[^"]+\.js'
    weight: 0.15
    ver_min: "9.19"
`)
	page := Page{
		HTML: []byte(`<html><head><meta name="csrf-token" content="tok">
<script src="/build/assets/app-DJIrOSj4.js"></script></head></html>`),
		Cookies: []Cookie{{Name: "XSRF-TOKEN", Value: "eyJpdiI6ZmFrZQ=="}},
		Headers: map[string][]string{
			"X-Laravel":    {"yes"},
			"X-Powered-By": {"PHP/8.2.8"},
		},
	}
	res := set.Evaluate(page)

	if len(res.Signals) != 6 {
		t.Fatalf("%d rules fired, want 6: %+v", len(res.Signals), res.Signals)
	}
	want := ScoreFixed([]float64{0.25, 0.45, 0.9, 0.3, 0.2, 0.15})
	if diff(res.Score, want) > 1e-9 {
		t.Errorf("score = %v, want %v", res.Score, want)
	}
	byRule := map[string]Evidence{}
	for _, s := range res.Signals {
		byRule[s.Rule] = s
	}
	// evidence per target
	for rule, ev := range map[string]string{
		"cookie_xsrf_token":        "cookie XSRF-TOKEN",
		"cookie_encrypted_payload": "cookie XSRF-TOKEN=eyJpdiI6ZmFrZQ==",
		"header_laravel":           "header X-Laravel",
		"header_php":               "header X-Powered-By: PHP/8.2.8",
		"meta_csrf_token":          `<meta name="csrf-token" content="tok">`,
		"vite_assets":              "/build/assets/app-DJIrOSj4.js",
	} {
		if got := byRule[rule].Evidence; got != ev {
			t.Errorf("%s: evidence=%q, want %q", rule, got, ev)
		}
	}
	if res.VerMin != "9.19" || res.VerMax != "" || res.Conflict {
		t.Errorf("the versions: [%s..%s] conflict=%v", res.VerMin, res.VerMax, res.Conflict)
	}
}

func TestEvaluateHeaderOrderDeterministic(t *testing.T) {
	// the rule matches any header name: map iteration is random, the engine
	// must choose deterministically (name sorting) — re-analysis is
	// reproducible
	set := evalSet(t, `
rules:
  - id: any_header
    target: header_name
    regex: ".+"
    weight: 0.1
`)
	page := Page{Headers: map[string][]string{
		"Zeta":     {"1"},
		"Alpha":    {"2"},
		"Middle":   {"3"},
		"Beta":     {"4"},
		"Omega":    {"5"},
		"Aa-first": {"6"},
	}}
	first := set.Evaluate(page).Signals[0].Evidence
	for i := 0; i < 50; i++ {
		if ev := set.Evaluate(page).Signals[0].Evidence; ev != first {
			t.Fatalf("a non-deterministic evidence: %q against %q", ev, first)
		}
	}
	if first != "header Aa-first" {
		t.Errorf("the first name by sort order was expected, got %q", first)
	}
}

// countingCtx reports a context error after N checks — a deterministic
// cancellation in the middle of the rule scan (EvalTimeout=0, so that
// EvaluateCtx does not wrap the context in a timer).
type countingCtx struct{ left int }

func (c *countingCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *countingCtx) Done() <-chan struct{}       { return nil }
func (c *countingCtx) Err() error {
	if c.left > 0 {
		c.left--
		return nil
	}
	return context.Canceled
}
func (c *countingCtx) Value(any) any { return nil }

func TestEvaluateCtxStopsBetweenRules(t *testing.T) {
	set := evalSet(t, `
rules:
  - id: r1
    target: html
    regex: "MARK"
    weight: 0.1
  - id: r2
    target: html
    regex: "MARK"
    weight: 0.1
  - id: r3
    target: html
    regex: "MARK"
    weight: 0.1
`)
	lim := DefaultLimits()
	lim.EvalTimeout = 0 // no timer wrapper: Err() is called directly

	// budget: r1 and r2 each pass two ctx checks (the EvaluateCtx loop +
	// the matchRule entry), r3 gets the cancellation; what hit before the
	// cancellation is kept and marked Partial (the outcome must not be
	// treated as complete, AN-09)
	res := set.EvaluateCtx(&countingCtx{left: 4}, Page{HTML: []byte("MARK MARK MARK")}, lim)
	if len(res.Signals) != 2 || res.Signals[1].Rule != "r2" {
		t.Fatalf("r1,r2 were expected before the cancellation, got %+v", res.Signals)
	}
	if res.Score == 0 {
		t.Error("the score must count what fired before the cancellation")
	}
	if !res.Partial {
		t.Error("an interrupted scan must be marked Partial")
	}
}

func TestEvaluateCtxCanceledUpfront(t *testing.T) {
	set := evalSet(t, `
rules:
  - id: r1
    target: html
    regex: "MARK"
    weight: 0.9
`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := set.EvaluateCtx(ctx, Page{HTML: []byte("MARK")}, DefaultLimits())
	if len(res.Signals) != 0 || res.Score != 0 {
		t.Errorf("a cancelled context: an empty result was expected, got %+v", res)
	}
	if !res.Partial {
		t.Error("a cancelled context: the result is incomplete (Partial)")
	}
}

func TestEvaluateZeroLimitsMeansDefault(t *testing.T) {
	// zero Limits as a whole — DefaultLimits: a match beyond 2 MiB is invisible
	set := evalSet(t, `
rules:
  - id: deep
    target: html
    regex: "NEEDLE"
    weight: 0.5
`)
	html := append(make([]byte, DefaultLimits().MaxHTMLBytes), []byte("NEEDLE")...)
	res := set.EvaluateCtx(context.Background(), Page{HTML: html}, Limits{})
	if len(res.Signals) != 0 {
		t.Error("zero Limits must include DefaultLimits (the 2 MiB bound)")
	}
}

// TestEvaluateCtxNanoBudget — a positive EvalTimeout of any size (here
// 1 ns) must enable the WithTimeout wrapper: by the time the delay is
// computed in WithDeadline the deadline has already passed (between
// time.Now().Add and time.Until — hundreds of ns), the context is
// canceled synchronously (the dur <= 0 branch), and the scan is
// interrupted with Partial on the very first check — deterministically,
// without a race with the timer. The "> 1" mutant skips the wrapper: a
// 1 ns budget would be treated as disabled, the scan would run to the end
// without Partial.
func TestEvaluateCtxNanoBudget(t *testing.T) {
	set := &Set{Rules: []Rule{
		{ID: "r1", Target: TargetHTML, Regex: "MARK", Weight: 0.1},
		{ID: "r2", Target: TargetHTML, Regex: "MARK", Weight: 0.1},
		{ID: "r3", Target: TargetHTML, Regex: "MARK", Weight: 0.1},
	}}
	lim := DefaultLimits()
	lim.EvalTimeout = 1 // ns
	res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("MARK")}, lim)
	if !res.Partial {
		t.Error("a 1 ns budget must interrupt the scan (Partial)")
	}
}

// TestMatchRuleCtxStopsMidScan — cancellation between elements: the ctx
// budget is checked both at the matchRule entry and in every scan (cookies,
// header names and values, meta tags); a matching element AFTER the budget
// is exhausted is not examined. countingCtx: the entry + the first element
// exhaust the stock, the second (matching) one is already under
// cancellation.
func TestMatchRuleCtxStopsMidScan(t *testing.T) {
	t.Run("cookie_name", func(t *testing.T) {
		page := Page{Cookies: []Cookie{{Name: "a"}, {Name: "hit"}}}
		if _, ok := matchRule(&countingCtx{left: 2}, Rule{Target: TargetCookieName, Match: "hit"}, page, DefaultLimits()); ok {
			t.Error("the cookie iteration must stop before the matching cookie")
		}
	})
	t.Run("cookie_value", func(t *testing.T) {
		page := Page{Cookies: []Cookie{{Name: "a", Value: "1"}, {Name: "b", Value: "hit"}}}
		if _, ok := matchRule(&countingCtx{left: 2}, Rule{Target: TargetCookieValue, Match: "hit"}, page, DefaultLimits()); ok {
			t.Error("the cookie value iteration must stop before the matching one")
		}
	})
	t.Run("header_name", func(t *testing.T) {
		page := Page{Headers: map[string][]string{"A": {"1"}, "B": {"2"}}}
		if _, ok := matchRule(&countingCtx{left: 2}, Rule{Target: TargetHeaderName, Match: "B"}, page, DefaultLimits()); ok {
			t.Error("the header name iteration must stop before the matching one")
		}
	})
	t.Run("header_value", func(t *testing.T) {
		page := Page{Headers: map[string][]string{"H": {"1", "hit"}}}
		if _, ok := matchRule(&countingCtx{left: 2}, Rule{Target: TargetHeaderValue, Match: "hit"}, page, DefaultLimits()); ok {
			t.Error("the header value iteration must stop before the matching one")
		}
	})
	t.Run("meta", func(t *testing.T) {
		page := Page{HTML: []byte(`x<meta a="1"><meta b="1">`)}
		if _, ok := matchRule(&countingCtx{left: 2}, Rule{Target: TargetMeta, Regex: `b=`}, page, DefaultLimits()); ok {
			t.Error("the meta tag scan must stop before the matching tag")
		}
	})
}

// TestMatchRuleMetaAdvance — the sliding meta search moves the position
// past the end of the scanned tag: the third tag is found and becomes the
// evidence. The "pos = loc[1]" mutant (without accumulating the offset)
// loops over the first two tags — the test kills it by the mutant's
// timeout.
func TestMatchRuleMetaAdvance(t *testing.T) {
	page := Page{HTML: []byte(`xx<meta a="1"><meta b="1"><meta c="1">`)}
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `c="1"`}, page, DefaultLimits())
	if !ok {
		t.Fatal("the third meta tag must be found")
	}
	if ev != `<meta c="1">` {
		t.Errorf("the evidence = %q, want the third tag", ev)
	}
}

func TestEvaluateEmptyPage(t *testing.T) {
	set := evalSet(t, `
rules:
  - id: r
    target: html
    regex: "."
    weight: 0.5
`)
	res := set.Evaluate(Page{}) // nil HTML, nil cookies, nil headers
	if len(res.Signals) != 0 || res.Score != 0 || res.Conflict {
		t.Errorf("an empty page: an unexpected result %+v", res)
	}
}

func TestEvaluateEvidenceTrimmedAndSafe(t *testing.T) {
	set := evalSet(t, `
rules:
  - id: long
    target: html
    regex: "LONG-[a-z]+"
    weight: 0.1
  - id: utf8
    target: html
    regex: "ЗДЕСЬ-[а-яё]+"
    weight: 0.1
  - id: ctrl
    target: html
    regex: '(?s)CTRL-.+'
    weight: 0.1
`)
	// a long ASCII fragment: truncation to 64 bytes + "…"
	long := []byte("LONG-" + strings.Repeat("a", 300))
	// a multibyte fragment: truncation does not split a rune
	utf8Frag := []byte("ЗДЕСЬ-" + strings.Repeat("тест", 100))
	// control characters and broken UTF-8 are sanitized: (?s).+ captures
	// everything, including \x00, ESC and the invalid byte \xff
	ctrl := []byte("CTRL-a\x00b\x1bc\xffd")

	lim := DefaultLimits()
	for name, html := range map[string][]byte{"long": long, "utf8": utf8Frag, "ctrl": ctrl} {
		res := set.EvaluateCtx(context.Background(), Page{HTML: html}, lim)
		for _, s := range res.Signals {
			ev := s.Evidence
			if len(ev) > lim.MaxSnippetBytes+len("…") {
				t.Errorf("%s: the evidence is longer than the limit: %d bytes", name, len(ev))
			}
			if !utf8.ValidString(ev) {
				t.Errorf("%s: the evidence is not valid UTF-8: %q", name, ev)
			}
			if strings.ContainsAny(ev, "\x00\x01\x1b\x7f") {
				t.Errorf("%s: control characters are not sanitized: %q", name, ev)
			}
			if len(ev) == lim.MaxSnippetBytes+len("…") && !strings.HasSuffix(ev, "…") {
				t.Errorf("%s: a truncated evidence without the ellipsis: %q", name, ev)
			}
		}
	}

	// exactly 64 bytes — no ellipsis
	res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("LONG-" + strings.Repeat("a", 59))}, lim)
	if ev := res.Signals[0].Evidence; len(ev) != 64 || strings.HasSuffix(ev, "…") {
		t.Errorf("an evidence exactly at the limit must stay whole: %q (%d)", ev, len(ev))
	}
}

func TestEvaluateMultibyteRuneNotSplit(t *testing.T) {
	// the truncation boundary falls in the middle of a multibyte rune
	s := strings.Repeat("тест", 100)
	got := truncateUTF8(s, 5) // 5 bytes = "te" (2+2) + the first byte of the next rune
	if got != "те" {
		t.Errorf("truncateUTF8 tears a rune: %q", got)
	}
	if got := truncateUTF8("abcd", 4); got != "abcd" {
		t.Errorf("a string within the limit must be returned whole: %q", got)
	}
	if got := truncateUTF8("abcd", 0); got != "" {
		t.Errorf("a zero limit: %q", got)
	}
}

func TestEvaluateReDoSHugeInput(t *testing.T) {
	// an expression catastrophic in PCRE ((a+)+b against a…a) is linear in
	// RE2; in addition the input is capped by the limit — beyond 2 MiB
	// there is no match. The test pins the property "the engine terminates
	// in finite time" (hanging would kill the test by the package timeout).
	set := evalSet(t, `
rules:
  - id: redos
    target: html
    regex: "(a+)+b"
    weight: 0.3
  - id: tail
    target: html
    regex: "TAIL"
    weight: 0.3
`)
	html := append([]byte(strings.Repeat("a", 8<<20)), []byte("TAIL")...) // 8 MiB
	res := set.Evaluate(Page{HTML: html})
	byRule := map[string]bool{}
	for _, s := range res.Signals {
		byRule[s.Rule] = true
	}
	if byRule["redos"] {
		t.Error("(a+)+b must not match on a…aTAIL (no b)")
	}
	if byRule["tail"] {
		t.Error("a match beyond the HTML limit was found")
	}
}

func TestDefaultLimitsGuard(t *testing.T) {
	// the guard values are the spec §9 contract; changing them must be deliberate
	lim := DefaultLimits()
	if lim.MaxHTMLBytes != 2<<20 || lim.MaxValueBytes != 64<<10 || lim.MaxPatternBytes != 4<<10 {
		t.Errorf("the size limits changed: %+v", lim)
	}
	if lim.MaxSnippetBytes != 64 || lim.EvalTimeout != 5*time.Second {
		t.Errorf("the evidence/timeout changed: %+v", lim)
	}
}

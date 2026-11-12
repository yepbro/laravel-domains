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
			"точное совпадение",
			Rule{Target: TargetCookieName, Match: "XSRF-TOKEN"},
			Page{Cookies: []Cookie{{Name: "XSRF-TOKEN", Value: "x"}}},
			true, "cookie XSRF-TOKEN",
		},
		{
			"точное совпадение не сработало",
			Rule{Target: TargetCookieName, Match: "XSRF-TOKEN"},
			Page{Cookies: []Cookie{{Name: "xsrf-token", Value: "x"}}}, // case matters
			false, "",
		},
		{
			"regex по имени",
			Rule{Target: TargetCookieName, Regex: `^[a-z0-9_\-]+_session$`},
			Page{Cookies: []Cookie{
				{Name: "PHPSESSID", Value: "1"},
				{Name: "laravel_session", Value: "2"},
			}},
			true, "cookie laravel_session",
		},
		{
			"первое совпадение по порядку среза",
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
		t.Fatal("ожидалось срабатывание по зашифрованному значению")
	}
	if ev != "cookie laravel_session=eyJpdiI6"+strings.Repeat("x", 33)+"…" {
		t.Errorf("неожиданное свидетельство: %q", ev)
	}

	// exact value match (match) is supported too
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetCookieValue, Match: "exact"}, Page{Cookies: []Cookie{
		{Name: "c", Value: "exact"},
	}}, DefaultLimits())
	if !ok || ev != "cookie c=exact" {
		t.Errorf("match по значению: ok=%v ev=%q", ok, ev)
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
		t.Errorf("имя по match: ok=%v ev=%q", ok, ev)
	}

	// regex over the name
	_, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderName, Regex: `^X-Powered`}, page, DefaultLimits())
	if !ok {
		t.Error("regex по имени заголовка не сработал")
	}

	// regex over the value: evidence with the header name
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `^PHP/`}, page, DefaultLimits())
	if !ok || ev != "header X-Powered-By: PHP/8.2.8" {
		t.Errorf("значение по regex: ok=%v ev=%q", ok, ev)
	}

	// several values of one header: the first one in slice order
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `^[ab]=`}, page, DefaultLimits())
	if !ok || ev != "header Set-Cookie: a=1" {
		t.Errorf("многозначный заголовок: ok=%v ev=%q", ok, ev)
	}

	// exact value match
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Match: "no-cache"}, page, DefaultLimits())
	if !ok || ev != "header Cache-Control: no-cache" {
		t.Errorf("значение по match: ok=%v ev=%q", ok, ev)
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
		t.Error("html + match не должен срабатывать")
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
		t.Fatal("meta-тег не найден")
	}
	if ev != `<meta name="csrf-token" content="eyJ0eXAiOiJKV1Qi">` {
		t.Errorf("неожиданное свидетельство: %q", ev)
	}

	// meta + match does not hit (regex only, like html)
	_, ok = matchRule(bgCtx(), Rule{Target: TargetMeta, Match: "csrf-token"}, page, DefaultLimits())
	if ok {
		t.Error("meta + match не должен срабатывать")
	}

	// ">" inside the quoted attribute value does not terminate the tag
	// (AN-08): the regex over the name after a value containing ">" hits,
	// the evidence is the whole tag
	quoted := Page{HTML: []byte(`<meta content="a>b" name="csrf-token">`)}
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `csrf-token`}, quoted, DefaultLimits())
	if !ok || ev != `<meta content="a>b" name="csrf-token">` {
		t.Errorf("meta с «>» в кавычках: ok=%v ev=%q", ok, ev)
	}
	// single quotes — the same semantics
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `csrf-token`},
		Page{HTML: []byte(`<meta content='a>b' name='csrf-token'>`)}, DefaultLimits())
	if !ok || ev != `<meta content='a>b' name='csrf-token'>` {
		t.Errorf("meta с «>» в одинарных кавычках: ok=%v ev=%q", ok, ev)
	}
	// an unterminated quote — no tag is extracted at all (no cut at ">")
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `unterminated`},
		Page{HTML: []byte(`<meta content="unterminated>`)}, DefaultLimits()); ok {
		t.Error("незакрытая кавычка не должна давать усечённый meta-тег")
	}
}

func TestMatchRuleEvidenceFromCappedInput(t *testing.T) {
	lim := DefaultLimits()
	longName := strings.Repeat("n", 100) // > evidenceNameBytes

	// name-only: the name is capped to 32 bytes inside the evidence too
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetCookieName, Match: longName},
		Page{Cookies: []Cookie{{Name: longName}}}, lim)
	if !ok {
		t.Fatal("точное совпадение по длинному имени")
	}
	if want := "cookie " + truncateUTF8(longName, evidenceNameBytes); ev != want {
		t.Errorf("имя в свидетельстве не ужимается: %q, want %q", ev, want)
	}
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderName, Match: longName},
		Page{Headers: map[string][]string{longName: {"v"}}}, lim)
	if !ok {
		t.Fatal("точное совпадение по длинному имени заголовка")
	}
	if want := "header " + truncateUTF8(longName, evidenceNameBytes); ev != want {
		t.Errorf("имя заголовка не ужимается: %q, want %q", ev, want)
	}

	// value: the evidence is built from the MaxValueBytes-capped value (the
	// same string the matching ran on), not from the full c.Value: a value
	// longer than the limit still hits via the anchor at the start and
	// yields evidence within the truncation window.
	huge := "eyJpdiI6" + strings.Repeat("z", lim.MaxValueBytes)
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetCookieValue, Regex: `^eyJpdiI6`},
		Page{Cookies: []Cookie{{Name: "c", Value: huge}}}, lim)
	if !ok {
		t.Fatal("значение длиннее MaxValueBytes не сопоставлено по префиксу")
	}
	// the truncation window: the form prefix + the start of the value up to the evidence limit
	zCut := func(prefix string) string {
		return prefix + strings.Repeat("z", lim.snippetCap()-len(prefix)) + "…"
	}
	if ev != zCut("cookie c=eyJpdiI6") {
		t.Errorf("свидетельство значения: %q", ev)
	}
	if ev2, ok := matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `^eyJpdiI6`},
		Page{Headers: map[string][]string{"X-Long": {huge}}}, lim); !ok ||
		ev2 != zCut("header X-Long: eyJpdiI6") {
		t.Errorf("свидетельство значения заголовка: ok=%v %q", ok, ev2)
	}
}

func TestMatchRuleSkipsBadRules(t *testing.T) {
	page := Page{HTML: []byte("x"), Cookies: []Cookie{{Name: "a", Value: "b"}}}

	// a non-compilable regex — the rule is skipped without a panic
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: `[unclosed`}, page, DefaultLimits()); ok {
		t.Error("некомпилируемый regex не должен срабатывать")
	}

	// a too long pattern — the MaxPatternBytes limit (spec §9)
	long := strings.Repeat("a", 4097)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: long}, page, DefaultLimits()); ok {
		t.Error("паттерн сверх лимита не должен срабатывать")
	}

	// a too long pattern that WOULD match: the limit cuts it off before
	// compilation — a bloated pattern must not match even when there is a
	// match
	longMatch := strings.Repeat("a", DefaultLimits().MaxPatternBytes+1)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: longMatch},
		Page{HTML: []byte(strings.Repeat("a", 5000))}, DefaultLimits()); ok {
		t.Error("паттерн сверх лимита не должен срабатывать даже при совпадении")
	}

	// match + an invalid regex: the rule is spoiled as a whole; degrading
	// to partial application (the exact match only) is not allowed
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieName, Match: "XSRF-TOKEN", Regex: `[unclosed`},
		Page{Cookies: []Cookie{{Name: "XSRF-TOKEN"}}}, DefaultLimits()); ok {
		t.Error("правило с некорректным regex не должно срабатывать по match")
	}

	// an unknown target — no panic and no hit
	if _, ok := matchRule(bgCtx(), Rule{Target: "body", Regex: "x"}, page, DefaultLimits()); ok {
		t.Error("неизвестная цель не должна срабатывать")
	}

	// a rule without match and regex simply does not match; cookie targets
	// also exercise the nil-regex branch in matches (the html target never
	// calls it)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML}, page, DefaultLimits()); ok {
		t.Error("пустое правило не должно срабатывать")
	}
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieName},
		Page{Cookies: []Cookie{{Name: "a"}}}, DefaultLimits()); ok {
		t.Error("пустое правило не должно срабатывать по cookie-цели")
	}
}

func TestMatchRuleHTMLSizeCap(t *testing.T) {
	// a match beyond the limit is not looked up; at the boundary — it is
	head := strings.Repeat(" ", 100)
	html := []byte(head + "NEEDLE")
	lim := DefaultLimits()
	lim.MaxHTMLBytes = 100

	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: "NEEDLE"}, Page{HTML: html}, lim); ok {
		t.Error("совпадение за пределами лимита HTML найдено")
	}
	lim.MaxHTMLBytes = 106 // NEEDLE fits entirely
	if ev, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: "NEEDLE"}, Page{HTML: html}, lim); !ok || ev != "NEEDLE" {
		t.Errorf("совпадение в пределах лимита не найдено: ok=%v ev=%q", ok, ev)
	}

	// meta beyond the limit is not examined either
	metaHTML := []byte(head + `<meta name="csrf-token">`)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: "csrf"}, Page{HTML: metaHTML}, lim); ok {
		t.Error("meta за пределами лимита HTML найдено")
	}
}

func TestMatchRuleValueSizeCap(t *testing.T) {
	lim := DefaultLimits()
	lim.MaxValueBytes = 8

	// an anchor at the start: the truncation is invisible
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieValue, Regex: `^eyJ`}, Page{Cookies: []Cookie{
		{Name: "c", Value: "eyJ" + strings.Repeat("z", 100)},
	}}, lim); !ok {
		t.Error("приставка в пределах лимита значения не найдена")
	}

	// an anchor at the end beyond the limit does not hit — the documented
	// trade of completeness for the time guarantee (the TAIL tail is capped
	// and invisible)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieValue, Regex: `TAIL$`}, Page{Cookies: []Cookie{
		{Name: "c", Value: strings.Repeat("z", 100) + "TAIL"},
	}}, lim); ok {
		t.Error("хвост за пределами лимита значения не должен находиться")
	}

	// headers are bounded by the same limit
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `END$`}, Page{Headers: map[string][]string{
		"X-Long": {strings.Repeat("t", 100) + "END"},
	}}, lim); ok {
		t.Error("хвост значения заголовка за пределами лимита не должен находиться")
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
			t.Errorf("отменённый контекст: цель %s сработала", r.Target)
		}
	}

	// cancellation inside the cookie scan: a check on every iteration, the
	// matching cookie is not examined
	partial := Page{Cookies: []Cookie{
		{Name: "first", Value: "1"},
		{Name: "XSRF-TOKEN"},
	}}
	if _, ok := matchRule(ctx, Rule{Target: TargetCookieName, Match: "XSRF-TOKEN"}, partial, DefaultLimits()); ok {
		t.Error("перебор cookies должен останавливаться по контексту")
	}
}

func TestCompileCache(t *testing.T) {
	pattern := `^eyJpdiI6$`
	re1, err1 := compileCached(pattern)
	if err1 != nil || re1 == nil {
		t.Fatalf("первая компиляция: %v", err1)
	}
	re2, err2 := compileCached(pattern)
	if err2 != nil || re2 != re1 {
		t.Error("повторный вызов должен вернуть тот же скомпилированный паттерн")
	}

	// a compilation error is cached: the same pattern yields the error without a panic
	bad := `[unclosed`
	if _, err := compileCached(bad); err == nil {
		t.Error("ожидалась ошибка компиляции")
	}
	if _, err := compileCached(bad); err == nil {
		t.Error("кэшированная ошибка должна воспроизводиться")
	}
	if v, ok := compileCache.Load(bad); !ok || v.(compileResult).err == nil {
		t.Error("ошибка должна лежать в кэше")
	}
}

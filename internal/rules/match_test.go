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
			Page{Cookies: []Cookie{{Name: "xsrf-token", Value: "x"}}}, // регистр важен
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
	// regex по значению: свидетельство с именем и префиксом значения
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

	// точное совпадение по значению (match) тоже поддерживается
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

	// точное совпадение имени
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetHeaderName, Match: "X-Laravel"}, page, DefaultLimits())
	if !ok || ev != "header X-Laravel" {
		t.Errorf("имя по match: ok=%v ev=%q", ok, ev)
	}

	// regex по имени
	_, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderName, Regex: `^X-Powered`}, page, DefaultLimits())
	if !ok {
		t.Error("regex по имени заголовка не сработал")
	}

	// regex по значению: свидетельство с именем заголовка
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `^PHP/`}, page, DefaultLimits())
	if !ok || ev != "header X-Powered-By: PHP/8.2.8" {
		t.Errorf("значение по regex: ok=%v ev=%q", ok, ev)
	}

	// несколько значений одного заголовка: первое по порядку среза
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Regex: `^[ab]=`}, page, DefaultLimits())
	if !ok || ev != "header Set-Cookie: a=1" {
		t.Errorf("многозначный заголовок: ok=%v ev=%q", ok, ev)
	}

	// точное совпадение значения
	ev, ok = matchRule(bgCtx(), Rule{Target: TargetHeaderValue, Match: "no-cache"}, page, DefaultLimits())
	if !ok || ev != "header Cache-Control: no-cache" {
		t.Errorf("значение по match: ok=%v ev=%q", ok, ev)
	}
}

func TestMatchRuleHTML(t *testing.T) {
	page := Page{HTML: []byte(`<html><head><script src="/build/assets/app-DJIrOSj4.js"></script></head></html>`)}

	// свидетельство — сам совпавший фрагмент
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: `/build/assets/[^"']+\.js`}, page, DefaultLimits())
	if !ok || ev != "/build/assets/app-DJIrOSj4.js" {
		t.Errorf("html: ok=%v ev=%q", ok, ev)
	}

	// цель html с match вместо regex не срабатывает (ТЗ 5.8: по HTML — regex)
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

	// свидетельство — весь meta-тег (до обрезки лимитом свидетельства)
	ev, ok := matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: `csrf-token`}, page, DefaultLimits())
	if !ok {
		t.Fatal("meta-тег не найден")
	}
	if ev != `<meta name="csrf-token" content="eyJ0eXAiOiJKV1Qi">` {
		t.Errorf("неожиданное свидетельство: %q", ev)
	}

	// meta + match не срабатывает (только regex, как html)
	_, ok = matchRule(bgCtx(), Rule{Target: TargetMeta, Match: "csrf-token"}, page, DefaultLimits())
	if ok {
		t.Error("meta + match не должен срабатывать")
	}
}

func TestMatchRuleSkipsBadRules(t *testing.T) {
	page := Page{HTML: []byte("x"), Cookies: []Cookie{{Name: "a", Value: "b"}}}

	// некомпилируемый regex — правило пропускается без паники
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: `[unclosed`}, page, DefaultLimits()); ok {
		t.Error("некомпилируемый regex не должен срабатывать")
	}

	// слишком длинный паттерн — лимит MaxPatternBytes (ТЗ §9)
	long := strings.Repeat("a", 4097)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: long}, page, DefaultLimits()); ok {
		t.Error("паттерн сверх лимита не должен срабатывать")
	}

	// неизвестная цель — без паники и без срабатывания
	if _, ok := matchRule(bgCtx(), Rule{Target: "body", Regex: "x"}, page, DefaultLimits()); ok {
		t.Error("неизвестная цель не должна срабатывать")
	}

	// правило без match и regex — просто не матчится
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML}, page, DefaultLimits()); ok {
		t.Error("пустое правило не должно срабатывать")
	}
}

func TestMatchRuleHTMLSizeCap(t *testing.T) {
	// совпадение за пределами лимита не ищется; на границе — ищется
	head := strings.Repeat(" ", 100)
	html := []byte(head + "NEEDLE")
	lim := DefaultLimits()
	lim.MaxHTMLBytes = 100

	if _, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: "NEEDLE"}, Page{HTML: html}, lim); ok {
		t.Error("совпадение за пределами лимита HTML найдено")
	}
	lim.MaxHTMLBytes = 106 // NEEDLE целиком в пределах
	if ev, ok := matchRule(bgCtx(), Rule{Target: TargetHTML, Regex: "NEEDLE"}, Page{HTML: html}, lim); !ok || ev != "NEEDLE" {
		t.Errorf("совпадение в пределах лимита не найдено: ok=%v ev=%q", ok, ev)
	}

	// meta за пределами лимита тоже не смотрится
	metaHTML := []byte(head + `<meta name="csrf-token">`)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetMeta, Regex: "csrf"}, Page{HTML: metaHTML}, lim); ok {
		t.Error("meta за пределами лимита HTML найдено")
	}
}

func TestMatchRuleValueSizeCap(t *testing.T) {
	lim := DefaultLimits()
	lim.MaxValueBytes = 8

	// якорь по началу: обрезка невидима
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieValue, Regex: `^eyJ`}, Page{Cookies: []Cookie{
		{Name: "c", Value: "eyJ" + strings.Repeat("z", 100)},
	}}, lim); !ok {
		t.Error("приставка в пределах лимита значения не найдена")
	}

	// якорь по концу за пределами лимита не срабатывает — задокументированный
	// размен полноты на гарантию времени (хвост TAIL обрезан и невидим)
	if _, ok := matchRule(bgCtx(), Rule{Target: TargetCookieValue, Regex: `TAIL$`}, Page{Cookies: []Cookie{
		{Name: "c", Value: strings.Repeat("z", 100) + "TAIL"},
	}}, lim); ok {
		t.Error("хвост за пределами лимита значения не должен находиться")
	}

	// заголовки ограничиваются тем же лимитом
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
	// отмена до начала: ни одна цель не смотрится
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

	// отмена внутри перебора cookies: проверка на каждой итерации,
	// совпадающая кука не смотрится
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

	// ошибка компиляции кэшируется: тот же паттерн даёт ошибку без паники
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

package rules

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// evalSet строит набор из yaml-тела правила (валидация LoadDir не нужна
// движку — тестируем матчинг, а не загрузку).
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
		t.Fatalf("сработало %d правил, want 6: %+v", len(res.Signals), res.Signals)
	}
	want := ScoreFixed([]float64{0.25, 0.45, 0.9, 0.3, 0.2, 0.15})
	if diff(res.Score, want) > 1e-9 {
		t.Errorf("score = %v, want %v", res.Score, want)
	}
	byRule := map[string]Evidence{}
	for _, s := range res.Signals {
		byRule[s.Rule] = s
	}
	// свидетельства по целям
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
		t.Errorf("версии: [%s..%s] conflict=%v", res.VerMin, res.VerMax, res.Conflict)
	}
}

func TestEvaluateHeaderOrderDeterministic(t *testing.T) {
	// правило матчит любое имя заголовка: обход map случайный, движок
	// обязан выбирать детерминированно (сортировка имён) — переанализ
	// воспроизводим
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
			t.Fatalf("недетерминированное свидетельство: %q против %q", ev, first)
		}
	}
	if first != "header Aa-first" {
		t.Errorf("ожидалось первое имя по сортировке, получено %q", first)
	}
}

// countingCtx отчитывается ошибкой контекста после N проверок —
// детерминированная отмена посреди перебора правил (EvalTimeout=0,
// чтобы EvaluateCtx не оборачивал контекст таймером).
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
	lim.EvalTimeout = 0 // без обёртки таймера: Err() зовётся напрямую

	// бюджет: r1 и r2 проходят по две проверки ctx (цикл EvaluateCtx +
	// вход matchRule), на r3 — отмена; сработавшие до отмены сохранены
	// и помечены Partial (полным итог считать нельзя, AN-09)
	res := set.EvaluateCtx(&countingCtx{left: 4}, Page{HTML: []byte("MARK MARK MARK")}, lim)
	if len(res.Signals) != 2 || res.Signals[1].Rule != "r2" {
		t.Fatalf("ожидались r1,r2 до отмены, получено %+v", res.Signals)
	}
	if res.Score == 0 {
		t.Error("score должен учитывать сработавшее до отмены")
	}
	if !res.Partial {
		t.Error("прерванный просмотр обязан помечаться Partial")
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
		t.Errorf("отменённый контекст: ожидался пустой результат, получено %+v", res)
	}
	if !res.Partial {
		t.Error("отменённый контекст: результат неполон (Partial)")
	}
}

func TestEvaluateZeroLimitsMeansDefault(t *testing.T) {
	// нулевые Limits целиком — DefaultLimits: совпадение за 2 MiB невидимо
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
		t.Error("нулевые Limits должны включать DefaultLimits (обрезка до 2 MiB)")
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
		t.Errorf("пустая страница: неожиданный результат %+v", res)
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
	// длинный ASCII-фрагмент: обрезка 64 байта + «…»
	long := []byte("LONG-" + strings.Repeat("a", 300))
	// многоугольный фрагмент: обрезка не рвёт руну
	utf8Frag := []byte("ЗДЕСЬ-" + strings.Repeat("тест", 100))
	// управляющие символы и битый UTF-8 sanitируются: (?s).+ захватывает
	// всё, включая \x00, ESC и невалидный байт \xff
	ctrl := []byte("CTRL-a\x00b\x1bc\xffd")

	lim := DefaultLimits()
	for name, html := range map[string][]byte{"long": long, "utf8": utf8Frag, "ctrl": ctrl} {
		res := set.EvaluateCtx(context.Background(), Page{HTML: html}, lim)
		for _, s := range res.Signals {
			ev := s.Evidence
			if len(ev) > lim.MaxSnippetBytes+len("…") {
				t.Errorf("%s: свидетельство длиннее лимита: %d байт", name, len(ev))
			}
			if !utf8.ValidString(ev) {
				t.Errorf("%s: свидетельство не является валидным UTF-8: %q", name, ev)
			}
			if strings.ContainsAny(ev, "\x00\x01\x1b\x7f") {
				t.Errorf("%s: управляющие символы не sanitized: %q", name, ev)
			}
			if len(ev) == lim.MaxSnippetBytes+len("…") && !strings.HasSuffix(ev, "…") {
				t.Errorf("%s: обрезанное свидетельство без многоточия: %q", name, ev)
			}
		}
	}

	// ровно 64 байта — без многоточия
	res := set.EvaluateCtx(context.Background(), Page{HTML: []byte("LONG-" + strings.Repeat("a", 59))}, lim)
	if ev := res.Signals[0].Evidence; len(ev) != 64 || strings.HasSuffix(ev, "…") {
		t.Errorf("свидетельство ровно в лимит должно оставаться целиком: %q (%d)", ev, len(ev))
	}
}

func TestEvaluateMultibyteRuneNotSplit(t *testing.T) {
	// граница обрезки приходится на середину многобайтной руны
	s := strings.Repeat("тест", 100)
	got := truncateUTF8(s, 5) // «те» (2+2 байта) + середина «с»
	if got != "те" {
		t.Errorf("truncateUTF8 рвёт руну: %q", got)
	}
	if got := truncateUTF8("abcd", 4); got != "abcd" {
		t.Errorf("строка в лимите должна возвращаться целиком: %q", got)
	}
	if got := truncateUTF8("abcd", 0); got != "" {
		t.Errorf("нулевой лимит: %q", got)
	}
}

func TestEvaluateReDoSHugeInput(t *testing.T) {
	// выражение, катастрофическое в PCRE ((a+)+b против a…a), в RE2
	// линейно; вдобавок вход обрезан лимитом — за пределами 2 MiB совпадения
	// нет. Тест фиксирует свойство «движок завершается за конечное время»
	// (зависание уронит тест по таймауту пакета).
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
		t.Error("(a+)+b не должен находить совпадение на a…aTAIL (нет b)")
	}
	if byRule["tail"] {
		t.Error("совпадение за пределами лимита HTML найдено")
	}
}

func TestDefaultLimitsGuard(t *testing.T) {
	// защитные значения — контракт ТЗ §9; смена должна быть осознанной
	lim := DefaultLimits()
	if lim.MaxHTMLBytes != 2<<20 || lim.MaxValueBytes != 64<<10 || lim.MaxPatternBytes != 4<<10 {
		t.Errorf("размерные лимиты изменились: %+v", lim)
	}
	if lim.MaxSnippetBytes != 64 || lim.EvalTimeout != 5*time.Second {
		t.Errorf("свидетельство/таймаут изменились: %+v", lim)
	}
}

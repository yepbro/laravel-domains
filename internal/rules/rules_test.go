package rules

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeRules(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestEvaluateCookiesAndVersionRange(t *testing.T) {
	dir := writeRules(t, `
rules:
  - id: cookie_xsrf_token
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
  - id: cookie_encrypted_payload
    target: cookie_value
    regex: "^eyJpdiI6"
    weight: 0.45
  - id: vite_assets
    target: html
    regex: '/build/assets/[^"]+-[A-Za-z0-9_-]{8}\.(js|css)'
    weight: 0.15
    ver_min: "9.19"
  - id: mix_assets
    target: html
    regex: '\.(css|js)\?id=[0-9a-f]{20}'
    weight: 0.15
    ver_max: "9.x"
`)
	set, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if set.Version == 0 {
		t.Fatal("Version не рассчитан")
	}

	page := Page{
		HTML: []byte(`<html><link href="/css/app.css?id=2d0e2f1a4b6c8d0e2f1a"><script src="/build/assets/app-DJIrOSj4.js"></script></html>`),
		Cookies: []Cookie{
			{Name: "XSRF-TOKEN", Value: "eyJpdiI6ZmFrZQ=="},
			{Name: "PHPSESSID", Value: "abc"},
		},
	}
	res := set.Evaluate(page)

	wantScore := ScoreFixed([]float64{0.25, 0.45, 0.15, 0.15})
	if diff(res.Score, wantScore) > 1e-9 {
		t.Errorf("score = %v, want %v", res.Score, wantScore)
	}
	if res.Partial {
		t.Error("полный просмотр не должен помечаться Partial")
	}
	// пересечение [9.19, ∞) и (-∞, 9.x] = [9.19, 9.x]
	if res.VerMin != "9.19" || res.VerMax != "9.x" {
		t.Errorf("версии = [%s, %s], want [9.19, 9.x]", res.VerMin, res.VerMax)
	}
	if res.Conflict {
		t.Error("неожиданный конфликт версий")
	}
	if len(res.Signals) != 4 {
		t.Errorf("сработало %d правил, want 4", len(res.Signals))
	}
}

func TestEvaluateVersionConflict(t *testing.T) {
	dir := writeRules(t, `
rules:
  - id: new_only
    target: html
    regex: "NEW-MARKER"
    weight: 0.3
    ver_min: "10"
  - id: old_only
    target: html
    regex: "OLD-MARKER"
    weight: 0.3
    ver_max: "9.x"
`)
	set, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	res := set.Evaluate(Page{HTML: []byte("NEW-MARKER OLD-MARKER")})
	if !res.Conflict {
		t.Error("ожидался конфликт диапазонов версий")
	}
	if res.VerMin != "" || res.VerMax != "" {
		t.Error("при конфликте диапазон должен сбрасываться")
	}
}

func TestEvaluateNegativeSample(t *testing.T) {
	dir := writeRules(t, `
rules:
  - id: cookie_xsrf_token
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
`)
	set, _ := LoadDir(dir)
	res := set.Evaluate(Page{
		HTML:    []byte("<html></html>"),
		Cookies: []Cookie{{Name: "csrftoken", Value: "django"}},
	})
	if len(res.Signals) != 0 || res.Score != 0 {
		t.Errorf("ложное срабатывание: score=%v signals=%v", res.Score, res.Signals)
	}
}

// TestEvaluateMatchValueRules — контракт «match по значению cookie/
// заголовка поддерживается» (движок matchRule + docs/analyzer.md
// «Цели и режимы матчинга»): валидатор загрузчика не бракует такие
// правила, набор грузится и срабатывает.
func TestEvaluateMatchValueRules(t *testing.T) {
	dir := writeRules(t, `
rules:
  - id: cookie_exact
    target: cookie_value
    match: "exact-value"
    weight: 0.3
  - id: header_exact
    target: header_value
    match: "PHP/8.2.8"
    weight: 0.4
`)
	set, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("правила с match по значениям обязаны загружаться: %v", err)
	}
	res := set.Evaluate(Page{
		Cookies: []Cookie{{Name: "c", Value: "exact-value"}},
		Headers: map[string][]string{"X-Powered-By": {"PHP/8.2.8"}},
	})
	if len(res.Signals) != 2 {
		t.Fatalf("сработало %d правил, want 2: %+v", len(res.Signals), res.Signals)
	}
}

func TestLoadDirValidation(t *testing.T) {
	valid := `
rules:
  - id: ok_match
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
  - id: ok_regex
    target: header_name
    regex: "^X-"
    weight: 0.2
  - id: ok_match_value
    target: cookie_value
    match: "eyJpdiI6"
    weight: 0.3
  - id: ok_match_header_value
    target: header_value
    match: "PHP/8.2.8"
    weight: 0.3
  - id: ok_ver_eq
    target: html
    regex: "MARKER"
    weight: 0.3
    ver_min: "9"
    ver_max: "9"
`
	cases := []struct {
		name string
		body string
	}{
		{"weight больше 1", `
rules:
  - id: bad_weight
    target: html
    regex: "x"
    weight: 1.5
`},
		{"weight ноль", `
rules:
  - id: zero_weight
    target: html
    regex: "x"
    weight: 0
`},
		{"weight NaN", `
rules:
  - id: nan_weight
    target: html
    regex: "x"
    weight: .nan
`},
		{"повтор id", `
rules:
  - id: dup
    target: html
    regex: "x"
    weight: 0.5
  - id: dup
    target: html
    regex: "y"
    weight: 0.5
`},
		{"правило без id", `
rules:
  - target: html
    regex: "x"
    weight: 0.5
`},
		{"неизвестный target", `
rules:
  - id: bad_target
    target: dns_name
    regex: "x"
    weight: 0.5
`},
		{"match и regex одновременно", `
rules:
  - id: both
    target: cookie_name
    match: "XSRF-TOKEN"
    regex: "^XSRF"
    weight: 0.5
`},
		{"ни match, ни regex", `
rules:
  - id: neither
    target: cookie_name
    weight: 0.5
`},
		{"html без regex", `
rules:
  - id: html_match
    target: html
    match: "<html>"
    weight: 0.5
`},
		{"meta без regex", `
rules:
  - id: meta_match
    target: meta
    match: "generator"
    weight: 0.5
`},
		{"некорректный regex", `
rules:
  - id: bad_regex
    target: html
    regex: "["
    weight: 0.5
`},
		{"regex длиннее MaxPatternBytes", "rules:\n  - id: long_regex\n    target: html\n    regex: \"" +
			strings.Repeat("a", DefaultLimits().MaxPatternBytes+1) + "\"\n    weight: 0.5\n"},
		{"regex матчит пустую строку (якорь)", `
rules:
  - id: empty_anchor
    target: html
    regex: "^"
    weight: 0.5
`},
		{"regex матчит пустую строку (звёздочка)", `
rules:
  - id: empty_star
    target: html
    regex: "x*"
    weight: 0.5
`},
		{"regex матчит пустую строку (пустая группа)", `
rules:
  - id: empty_group
    target: html
    regex: "(?:)"
    weight: 0.5
`},
		{"ver_min больше ver_max", `
rules:
  - id: ver_swap
    target: html
    regex: "x"
    weight: 0.5
    ver_min: "10"
    ver_max: "9.x"
`},
		{"некорректный ver_min", `
rules:
  - id: ver_bad
    target: html
    regex: "x"
    weight: 0.5
    ver_min: "девять"
`},
		{"пустой набор", `
rules: []
`},
		{"опечатка в поле (строгий разбор)", `
rules:
  - id: typo
    target: html
    regex: "x"
    wight: 0.5
`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := LoadDir(writeRules(t, c.body)); err == nil {
				t.Error("ожидалась ошибка валидации")
			}
		})
	}

	t.Run("валидный набор", func(t *testing.T) {
		if _, err := LoadDir(writeRules(t, valid)); err != nil {
			t.Errorf("валидный набор отклонён: %v", err)
		}
	})
}

func TestLoadDirErrors(t *testing.T) {
	t.Run("несуществующий каталог", func(t *testing.T) {
		if _, err := LoadDir(filepath.Join(t.TempDir(), "нет-такого")); err == nil {
			t.Error("ожидалась ошибка чтения каталога")
		}
	})
	t.Run("каталог без yaml", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadDir(dir); err == nil {
			t.Error("ожидалась ошибка «нет yaml-файлов»")
		}
	})
	t.Run("битый yaml", func(t *testing.T) {
		if _, err := LoadDir(writeRules(t, "rules: [")); err == nil {
			t.Error("ожидалась ошибка разбора yaml")
		}
	})
	t.Run("файл из комментариев с валидным файлом", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "00-comments.yaml"), []byte("# только комментарии\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte("rules:\n  - id: r\n    target: html\n    regex: x\n    weight: 0.5\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadDir(dir); err != nil {
			t.Errorf("файл из комментариев должен пропускаться: %v", err)
		}
	})
	t.Run("многодокументный файл читается целиком", func(t *testing.T) {
		// Второй документ (---) не должен теряться молча: правила
		// обоих документов входят в набор, версия совпадает с разбиением
		// по файлам.
		multi := writeRules(t, partA+"---\n"+partB)
		set, err := LoadDir(multi)
		if err != nil {
			t.Fatalf("многодокументный файл: %v", err)
		}
		if len(set.Rules) != 2 || set.Rules[0].ID != "a_cookie" || set.Rules[1].ID != "b_html" {
			t.Fatalf("правила второго документа потеряны: %+v", set.Rules)
		}
		split := writeRulesFiles(t, map[string]string{"b.yaml": partB, "a.yaml": partA})
		other, err := LoadDir(split)
		if err != nil {
			t.Fatal(err)
		}
		if set.Version != other.Version {
			t.Errorf("версия зависит от разбиения на документы: %d != %d", set.Version, other.Version)
		}
	})
	t.Run("опечатка во втором документе — ошибка", func(t *testing.T) {
		body := partA + "---\nrules:\n  - id: typo2\n    target: html\n    regex: x\n    wight: 0.5\n"
		if _, err := LoadDir(writeRules(t, body)); err == nil {
			t.Error("строгий разбор обязан охватывать все документы файла")
		}
	})
}

// writeRulesFiles — каталог с несколькими yaml-файлами (порядок записей
// в map не важен — имена задаются явно).
func writeRulesFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const (
	partA = `
rules:
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
`
	partB = `
rules:
  - id: b_html
    target: html
    regex: "MARKER"
    weight: 0.3
    ver_min: "9.19"
`
	// combined — те же правила одним документом (в обратном порядке).
	combined = `
rules:
  - id: b_html
    target: html
    regex: "MARKER"
    weight: 0.3
    ver_min: "9.19"
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
`
)

func TestLoadDirVersionStable(t *testing.T) {
	one := writeRulesFiles(t, map[string]string{"rules.yaml": combined})
	two := writeRulesFiles(t, map[string]string{"b.yaml": partB, "a.yaml": partA})

	s1, err := LoadDir(one)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := LoadDir(two)
	if err != nil {
		t.Fatal(err)
	}
	if s1.Version != s2.Version {
		t.Errorf("version зависит от разбиения на файлы: %d != %d", s1.Version, s2.Version)
	}
	if !reflect.DeepEqual(s1.Rules, s2.Rules) {
		t.Errorf("наборы различаются: %+v != %+v", s1.Rules, s2.Rules)
	}

	// Правка поведения (вес) меняет версию.
	dir := writeRules(t, partA)
	before, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte(`
rules:
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.3
`), 0o644); err != nil {
		t.Fatal(err)
	}
	after, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if before.Version == after.Version {
		t.Error("смена weight должна менять rules_version")
	}

	// Правка описания — поведение то же, версия не меняется.
	dirDesc := writeRules(t, `
rules:
  - id: a_cookie
    description: было
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
`)
	d1, err := LoadDir(dirDesc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirDesc, "rules.yaml"), []byte(`
rules:
  - id: a_cookie
    description: стало
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
`), 0o644); err != nil {
		t.Fatal(err)
	}
	d2, err := LoadDir(dirDesc)
	if err != nil {
		t.Fatal(err)
	}
	if d1.Version != d2.Version {
		t.Error("правка description не должна менять rules_version (поведение то же)")
	}
}

func TestLoadDirRulesSortedByID(t *testing.T) {
	// Порядок в файле обратный — набор канонизируется по id: детерминированы
	// и применение правил, и хэш.
	dir := writeRules(t, combined)
	set, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Rules) != 2 || set.Rules[0].ID != "a_cookie" || set.Rules[1].ID != "b_html" {
		t.Errorf("правила не отсортированы по id: %+v", set.Rules)
	}
}

func TestCanonicalHashDisambiguation(t *testing.T) {
	// Поля могут содержать любые байты (YAML-строки): представление с
	// префиксами длин не позволяет разным наборам давать один хэш из-за
	// совпадения байтового потока после сдвига границ полей.
	a := []Rule{{ID: "a\x00b", Target: "c"}}
	b := []Rule{{ID: "a", Target: "b\x00c"}}
	if canonicalHash(a) == canonicalHash(b) {
		t.Error("разные наборы дали одинаковый канонический хэш")
	}
}

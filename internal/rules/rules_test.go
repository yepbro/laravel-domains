package rules

import (
	"os"
	"path/filepath"
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
	if set.Version == "" {
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

func TestLoadDirValidation(t *testing.T) {
	dir := writeRules(t, `
rules:
  - id: bad_weight
    target: html
    regex: "x"
    weight: 1.5
`)
	if _, err := LoadDir(dir); err == nil {
		t.Error("ожидалась ошибка валидации weight")
	}
}

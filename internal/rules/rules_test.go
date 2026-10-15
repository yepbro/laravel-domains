package rules

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNoisyOR(t *testing.T) {
	cases := []struct {
		ws   []float64
		want float64
	}{
		{[]float64{0.25}, 0.25},
		{[]float64{0.25, 0.45}, 1 - 0.75*0.55},
		{[]float64{0.2, 0.2, 0.2, 0.2, 0.2}, 1 - 0.8*0.8*0.8*0.8*0.8},
	}
	for _, c := range cases {
		if got := ScoreFixed(c.ws); diff(got, c.want) > 1e-9 {
			t.Errorf("noisyOR(%v) = %v, want %v", c.ws, got, c.want)
		}
	}
}

func diff(a, b float64) float64 {
	if a-b < 0 {
		return b - a
	}
	return a - b
}

func TestVersionCompare(t *testing.T) {
	cases := []struct {
		a, b string
		less bool
	}{
		{"9.19", "11", true},
		{"11", "9.19", false},
		{"9.19", "9.x", true}, // 9.19 < всё девятое
		{"9.x", "10", true},   // бесконечность только в последнем компоненте
		{"9", "9.1", true},    // отсутствующий компонент = 0
		{"9.19", "9.19", false},
	}
	for _, c := range cases {
		va, vb := parseVersion(c.a), parseVersion(c.b)
		if va == nil || vb == nil {
			t.Fatalf("parseVersion(%q или %q) вернул nil", c.a, c.b)
		}
		if got := va.less(vb); got != c.less {
			t.Errorf("%s < %s = %v, want %v", c.a, c.b, got, c.less)
		}
	}
}

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

func TestResultContains(t *testing.T) {
	res := Result{VerMin: "9.19", VerMax: ""}
	if !res.Contains("9.19", "") {
		t.Error("[9.19,∞) должен включать 9.19")
	}
	if !res.Contains("10", "11") {
		t.Error("[9.19,∞) должен включать [10,11]")
	}
	if res.Contains("9.18", "") {
		t.Error("[9.19,∞) не должен включать 9.18")
	}
	res2 := Result{VerMin: "9.19", VerMax: "11.x"}
	if res2.Contains("11", "12") {
		t.Error("[9.19,11.x] не должен включать 12")
	}
}

package rules

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
		t.Fatal("Version is not computed")
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
		t.Error("a full scan must not be marked Partial")
	}
	// the intersection of [9.19, ∞) and (-∞, 9.x] = [9.19, 9.x]
	if res.VerMin != "9.19" || res.VerMax != "9.x" {
		t.Errorf("the versions = [%s, %s], want [9.19, 9.x]", res.VerMin, res.VerMax)
	}
	if res.Conflict {
		t.Error("an unexpected version conflict")
	}
	if len(res.Signals) != 4 {
		t.Errorf("%d rules fired, want 4", len(res.Signals))
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
		t.Error("a version range conflict is expected")
	}
	if res.VerMin != "" || res.VerMax != "" {
		t.Error("on a conflict the range must be reset")
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
		t.Errorf("a false hit: score=%v signals=%v", res.Score, res.Signals)
	}
}

// TestEvaluateMatchValueRules — the "match over a cookie/header value is
// supported" contract (the matchRule engine + docs/analyzer.md "Targets
// and matching modes"): the loader's validator does not reject such rules,
// the set loads and hits.
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
		t.Fatalf("rules with value matches must load: %v", err)
	}
	res := set.Evaluate(Page{
		Cookies: []Cookie{{Name: "c", Value: "exact-value"}},
		Headers: map[string][]string{"X-Powered-By": {"PHP/8.2.8"}},
	})
	if len(res.Signals) != 2 {
		t.Fatalf("%d rules fired, want 2: %+v", len(res.Signals), res.Signals)
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
		{"a weight above 1", `
rules:
  - id: bad_weight
    target: html
    regex: "x"
    weight: 1.5
`},
		{"a zero weight", `
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
		{"duplicate id", `
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
		{"a rule without id", `
rules:
  - target: html
    regex: "x"
    weight: 0.5
`},
		{"an unknown target", `
rules:
  - id: bad_target
    target: dns_name
    regex: "x"
    weight: 0.5
`},
		{"match and regex together", `
rules:
  - id: both
    target: cookie_name
    match: "XSRF-TOKEN"
    regex: "^XSRF"
    weight: 0.5
`},
		{"neither match nor regex", `
rules:
  - id: neither
    target: cookie_name
    weight: 0.5
`},
		{"html without regex", `
rules:
  - id: html_match
    target: html
    match: "<html>"
    weight: 0.5
`},
		{"meta without regex", `
rules:
  - id: meta_match
    target: meta
    match: "generator"
    weight: 0.5
`},
		{"an invalid regex", `
rules:
  - id: bad_regex
    target: html
    regex: "["
    weight: 0.5
`},
		{"a regex longer than MaxPatternBytes", "rules:\n  - id: long_regex\n    target: html\n    regex: \"" +
			strings.Repeat("a", DefaultLimits().MaxPatternBytes+1) + "\"\n    weight: 0.5\n"},
		{"a regex matching the empty string (an anchor)", `
rules:
  - id: empty_anchor
    target: html
    regex: "^"
    weight: 0.5
`},
		{"a regex matching the empty string (a star)", `
rules:
  - id: empty_star
    target: html
    regex: "x*"
    weight: 0.5
`},
		{"a regex matching the empty string (an empty group)", `
rules:
  - id: empty_group
    target: html
    regex: "(?:)"
    weight: 0.5
`},
		{"ver_min above ver_max", `
rules:
  - id: ver_swap
    target: html
    regex: "x"
    weight: 0.5
    ver_min: "10"
    ver_max: "9.x"
`},
		{"an invalid ver_min", `
rules:
  - id: ver_bad
    target: html
    regex: "x"
    weight: 0.5
    ver_min: "девять"
`},
		{"an invalid ver_max", `
rules:
  - id: ver_bad_max
    target: html
    regex: "x"
    weight: 0.5
    ver_max: "ten"
`},
		{"a typo in a foreign field (strict parsing)", `
rules:
  - id: typo_descr
    target: html
    regex: "x"
    weight: 0.5
    descrption: "опечатка"
`},
		{"an empty set", `
rules: []
`},
		{"a typo in a field (strict parsing)", `
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
				t.Error("a validation error is expected")
			}
		})
	}

	t.Run("a valid set", func(t *testing.T) {
		if _, err := LoadDir(writeRules(t, valid)); err != nil {
			t.Errorf("the valid set was rejected: %v", err)
		}
	})
}

func TestLoadDirErrors(t *testing.T) {
	t.Run("a missing directory", func(t *testing.T) {
		_, err := LoadDir(filepath.Join(t.TempDir(), "нет-такого"))
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("err = %v, want the os.ReadDir error wrap (opening the directory)", err)
		}
	})
	t.Run("a directory without yaml", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := LoadDir(dir)
		if err == nil || !strings.Contains(err.Error(), "no yaml files in directory") {
			t.Errorf("err = %v, want the exact text «no yaml files in directory» (not «rule set is empty»)", err)
		}
	})
	t.Run("a subdirectory with a yaml extension is skipped", func(t *testing.T) {
		// a directory is not a rule file: a name extension must not drag it
		// into the set (reading a directory as a file fails)
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "pkg.yaml"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "rules.yaml"),
			[]byte("rules:\n  - id: r\n    target: html\n    regex: x\n    weight: 0.5\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		set, err := LoadDir(dir)
		if err != nil {
			t.Fatalf("a subdirectory with the .yaml extension must be skipped: %v", err)
		}
		if len(set.Rules) != 1 || set.Rules[0].ID != "r" {
			t.Errorf("the set = %+v, want a single rule r", set.Rules)
		}
	})
	t.Run("a directory among the entries does not interrupt the file selection", func(t *testing.T) {
		// "0sub.dir" sorts before the yaml files: an early exit from the
		// entry scan would lose all the rule files
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "0sub.dir"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(partA), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadDir(dir); err != nil {
			t.Fatalf("a directory at the start of the listing must not break the load: %v", err)
		}
	})
	t.Run("a broken symlink — a load error", func(t *testing.T) {
		// an entry with a .yaml extension but ReadFile fails: a silent loss
		// of a rule file is not allowed (EOF on empty data would look like
		// "a file without rules")
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "rules.yaml"),
			[]byte("rules:\n  - id: r\n    target: html\n    regex: x\n    weight: 0.5\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("нет-такой-цели", filepath.Join(dir, "broken.yaml")); err != nil {
			t.Fatal(err)
		}
		_, err := LoadDir(dir)
		if err == nil || !strings.Contains(err.Error(), "broken.yaml") {
			t.Errorf("err = %v, want the broken.yaml read error", err)
		}
	})
	t.Run(".yml loads on par with .yaml", func(t *testing.T) {
		set, err := LoadDir(writeRulesFiles(t, map[string]string{"rules.yml": partA}))
		if err != nil {
			t.Fatalf(".yml must load the same way as .yaml: %v", err)
		}
		if len(set.Rules) != 1 {
			t.Errorf("the .yml rules are lost: %+v", set.Rules)
		}
	})
	t.Run("broken yaml", func(t *testing.T) {
		if _, err := LoadDir(writeRules(t, "rules: [")); err == nil {
			t.Error("a yaml parse error is expected")
		}
	})
	t.Run("a comments-only file with a valid file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "00-comments.yaml"), []byte("# только комментарии\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte("rules:\n  - id: r\n    target: html\n    regex: x\n    weight: 0.5\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadDir(dir); err != nil {
			t.Errorf("a comments-only file must be skipped: %v", err)
		}
	})
	t.Run("a multi-document file is read whole", func(t *testing.T) {
		// The second document (---) must not be lost silently: the rules of
		// both documents enter the set, the version matches the per-file
		// splitting.
		multi := writeRules(t, partA+"---\n"+partB)
		set, err := LoadDir(multi)
		if err != nil {
			t.Fatalf("the multi-document file: %v", err)
		}
		if len(set.Rules) != 2 || set.Rules[0].ID != "a_cookie" || set.Rules[1].ID != "b_html" {
			t.Fatalf("the second document's rules are lost: %+v", set.Rules)
		}
		split := writeRulesFiles(t, map[string]string{"b.yaml": partB, "a.yaml": partA})
		other, err := LoadDir(split)
		if err != nil {
			t.Fatal(err)
		}
		if set.Version != other.Version {
			t.Errorf("the version depends on the document splitting: %d != %d", set.Version, other.Version)
		}
	})
	t.Run("a typo in the second document — an error", func(t *testing.T) {
		body := partA + "---\nrules:\n  - id: typo2\n    target: html\n    regex: x\n    wight: 0.5\n"
		if _, err := LoadDir(writeRules(t, body)); err == nil {
			t.Error("strict parsing must cover all of the file's documents")
		}
	})
}

// writeRulesFiles — a directory with several yaml files (the map entry
// order does not matter — the names are given explicitly).
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
	// combined — the same rules in one document (in reverse order).
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
		t.Errorf("the version depends on the file splitting: %d != %d", s1.Version, s2.Version)
	}
	if !reflect.DeepEqual(s1.Rules, s2.Rules) {
		t.Errorf("the sets differ: %+v != %+v", s1.Rules, s2.Rules)
	}

	// A behavior change (the weight) changes the version.
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
		t.Error("a weight change must change rules_version")
	}

	// A description change — the behavior is the same, the version does not
	// change.
	dirDesc := writeRules(t, `
rules:
  - id: a_cookie
    description: before
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
    description: after
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
		t.Error("a description edit must not change rules_version (the behavior is the same)")
	}
}

func TestLoadDirRulesSortedByID(t *testing.T) {
	// The order in the file is reversed — the set is canonicalized by id:
	// both rule application and the hash are deterministic.
	dir := writeRules(t, combined)
	set, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Rules) != 2 || set.Rules[0].ID != "a_cookie" || set.Rules[1].ID != "b_html" {
		t.Errorf("the rules are not sorted by id: %+v", set.Rules)
	}
}

func TestCanonicalHashDisambiguation(t *testing.T) {
	// Fields may contain arbitrary bytes (YAML strings): the
	// length-prefixed representation does not let different sets produce
	// one hash due to the byte streams matching after a field boundary
	// shift.
	a := []Rule{{ID: "a\x00b", Target: "c"}}
	b := []Rule{{ID: "a", Target: "b\x00c"}}
	if canonicalHash(a) == canonicalHash(b) {
		t.Error("different sets produced the same canonical hash")
	}
	// The ":" separator without a length prefix is ambiguous: is the stream
	// ":a::b" the fields ("a", ":b") or ("a:", "b")?
	c := []Rule{{ID: "a", Target: ":b"}}
	d := []Rule{{ID: "a:", Target: "b"}}
	if canonicalHash(c) == canonicalHash(d) {
		t.Error("the length-less encoding allows a field boundary shift")
	}
	// A length prefix without ":" is ambiguous on digit values: the
	// concatenation of the fields "1", "AAAAAAAAA", "", "9BBBBBBBBB0", …
	// also reads as the first field "9AAAAAAAAA0" (length 11) with the rest
	// shifted — the byte streams without ":" match, with ":" they differ.
	e := []Rule{{ID: "1", Target: "AAAAAAAAA", Regex: "9BBBBBBBBB0", Weight: 0.5}}
	f := []Rule{{ID: "9AAAAAAAAA0", Target: "1", Match: "BBBBBBBBB", Weight: 0.5}}
	if canonicalHash(e) == canonicalHash(f) {
		t.Error("the separator-less encoding allows a field boundary shift")
	}
}

// TestCanonicalHashFieldCoverage — every field that changes the detection
// behavior enters rules_version: changing any of them must change the hash
// (removing the writeField of any field would make different sets
// indistinguishable).
func TestCanonicalHashFieldCoverage(t *testing.T) {
	base := Rule{ID: "r1", Target: "html", Regex: "MARK", Weight: 0.5, VerMin: "9", VerMax: "11"}
	variants := map[string]Rule{
		"id":      {ID: "r2", Target: "html", Regex: "MARK", Weight: 0.5, VerMin: "9", VerMax: "11"},
		"target":  {ID: "r1", Target: "meta", Regex: "MARK", Weight: 0.5, VerMin: "9", VerMax: "11"},
		"match":   {ID: "r1", Target: "html", Match: "X", Regex: "MARK", Weight: 0.5, VerMin: "9", VerMax: "11"},
		"regex":   {ID: "r1", Target: "html", Regex: "OTHER", Weight: 0.5, VerMin: "9", VerMax: "11"},
		"weight":  {ID: "r1", Target: "html", Regex: "MARK", Weight: 0.75, VerMin: "9", VerMax: "11"},
		"ver_min": {ID: "r1", Target: "html", Regex: "MARK", Weight: 0.5, VerMin: "10", VerMax: "11"},
		"ver_max": {ID: "r1", Target: "html", Regex: "MARK", Weight: 0.5, VerMin: "9", VerMax: "10.x"},
	}
	baseHash := canonicalHash([]Rule{base})
	for name, r := range variants {
		if canonicalHash([]Rule{r}) == baseHash {
			t.Errorf("the %s edit does not change rules_version", name)
		}
	}
}

// TestCanonicalHashWeightPrecision — the weight is written to
// rules_version in the shortest form (prec −1): 0.26 and 0.27 are
// different rules. At prec 0 ("one significant digit", the decrementer
// mutant) both would be formatted as "0.3" and the sets would become
// indistinguishable.
func TestCanonicalHashWeightPrecision(t *testing.T) {
	light := []Rule{{ID: "w", Weight: 0.26}}
	heavy := []Rule{{ID: "w", Weight: 0.27}}
	if canonicalHash(light) == canonicalHash(heavy) {
		t.Error("the weights 0.26 and 0.27 give one rules_version (the weight format is not shortest)")
	}
}

// TestLoadDirNegativeVersionComponents — a negative numeric version
// component ("-1") formally passes parseVersion; without a second boundary
// the range does not conflict with itself: the comparison against a missing
// left boundary (vmin == nil) must not be computed.
func TestLoadDirNegativeVersionComponents(t *testing.T) {
	if _, err := LoadDir(writeRules(t, `
rules:
  - id: neg_ver_max
    target: html
    regex: "x"
    weight: 0.5
    ver_max: "-1"
`)); err != nil {
		t.Errorf("ver_max without ver_min conflicts with nothing: %v", err)
	}
}

// TestLoadDirPatternBoundary — the regex length limit boundary: a pattern
// of exactly MaxPatternBytes bytes is valid (one byte longer is an error,
// see the TestLoadDirValidation table); the ">=" mutant would reject the
// exactly-at-limit case.
func TestLoadDirPatternBoundary(t *testing.T) {
	body := "rules:\n  - id: exact\n    target: html\n    regex: \"" +
		strings.Repeat("a", DefaultLimits().MaxPatternBytes) + "\"\n    weight: 0.5\n"
	if _, err := LoadDir(writeRules(t, body)); err != nil {
		t.Errorf("a pattern exactly at MaxPatternBytes is valid: %v", err)
	}
}

// TestLoadDirTrimsIDs — whitespace around the id is trimmed for every
// rule: a missed trim would leave the second and further rules with
// "  id  ".
func TestLoadDirTrimsIDs(t *testing.T) {
	set, err := LoadDir(writeRules(t, `
rules:
  - id: first
    target: html
    regex: "x"
    weight: 0.5
  - id: "  padded  "
    target: html
    regex: "y"
    weight: 0.5
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Rules) != 2 || set.Rules[0].ID != "first" || set.Rules[1].ID != "padded" {
		t.Errorf("ids are not trimmed: %+v", set.Rules)
	}
}

// TestLoadDirDuplicateIDReported — when a duplicate id comes with an extra
// error in the second rule, it is the duplicate that is reported: the
// canonical sort comparator is strict, the order of equal ids is stable
// (insertion sort), and the invalid rule does not jump to the front,
// replacing the "duplicate id" diagnosis with its own error.
func TestLoadDirDuplicateIDReported(t *testing.T) {
	_, err := LoadDir(writeRules(t, `
rules:
  - id: dup
    target: html
    regex: "x"
    weight: 0.5
  - id: dup
    target: bogus
    regex: "x"
    weight: 0.5
`))
	if err == nil || !strings.Contains(err.Error(), "duplicate id") {
		t.Errorf("err = %v, want the duplicate id diagnosis (not the second rule's error)", err)
	}
}

// TestTargetNamesDeterministic — the list of allowed targets for error
// messages: complete and strictly sorted (map iteration order is random —
// sorting makes the text deterministic). The loop covers a random
// coincidence of the iteration order with the sorted one.
func TestTargetNamesDeterministic(t *testing.T) {
	want := []string{"cookie_name", "cookie_value", "header_name", "header_value", "html", "meta"}
	for range 50 {
		if got := targetNames(); !slices.Equal(got, want) {
			t.Fatalf("targetNames() = %v, want %v", got, want)
		}
	}
	// The validation error text names the complete list of allowed targets.
	_, err := LoadDir(writeRules(t, `
rules:
  - id: bad_target
    target: body
    regex: "x"
    weight: 0.5
`))
	if err == nil || !strings.Contains(err.Error(),
		"(allowed: cookie_name, cookie_value, header_name, header_value, html, meta)") {
		t.Errorf("err = %v, want the full list of allowed targets", err)
	}
}

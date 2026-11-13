package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// repoRoot — the repository root relative to the package directory (the
// tests run from cmd/testbedgen).
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// loadScenarios — the real scenarios.yaml of the bench as a Config.
func loadScenarios(t *testing.T) Config {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy/docker/testbed/scenarios.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// ---------------------------------------------------------------- §11.2

// TestScenariosCoverTZ — an audit of the bench completeness against the
// mandatory composition of spec 11.2: every scenario from the T-090 task
// list is present and described correctly (a check by a table; "the
// Laravel pages of different versions" — a separate
// TestSamplesCoverLaravelVersions).
func TestScenariosCoverTZ(t *testing.T) {
	cfg := loadScenarios(t)
	byName := make(map[string]Scenario, len(cfg.Scenarios))
	for _, sc := range cfg.Scenarios {
		byName[sc.Name] = sc
	}

	mockAt := func(path string) func(DNSCfg, SiteCfg) bool {
		return func(d DNSCfg, s SiteCfg) bool {
			return d.A == "mockserver" && s.Template == "mock" && s.MockPath == path
		}
	}

	cases := []struct {
		req      string // the requirement of §11.2
		scenario string
		ok       func(d DNSCfg, s SiteCfg) bool
	}{
		{"NXDOMAIN", "nxdomain-case", func(d DNSCfg, _ SiteCfg) bool { return d == (DNSCfg{}) }},
		{"wildcard", "wildcard", func(d DNSCfg, _ SiteCfg) bool { return d.Wildcard && d.A == "testweb" }},
		{"tarpit", "tarpit", mockAt("/tarpit")},
		{"infinite redirect", "infinite-redirect", mockAt("/redirect/infinite")},
		{"huge body", "huge-body", mockAt("/huge")},
		{"SPA page", "spa-shell", func(_ DNSCfg, s SiteCfg) bool { return s.Template == "spa" }},
	}
	for _, tc := range cases {
		t.Run(tc.req, func(t *testing.T) {
			sc, found := byName[tc.scenario]
			if !found {
				t.Fatalf("the scenario %q (spec 11.2 \"%s\") is missing from scenarios.yaml", tc.scenario, tc.req)
			}
			if !tc.ok(sc.DNS, sc.Site) {
				t.Errorf("the scenario %q does not match spec 11.2 \"%s\": dns=%+v site=%+v",
					tc.scenario, tc.req, sc.DNS, sc.Site)
			}
		})
	}

	// The contract of the mock scenarios: each one has a mock_path — the
	// route map cmd/mockserver.hostRoutes must serve "/" by the scenario
	// Host.
	for _, sc := range cfg.Scenarios {
		if sc.Site.Template == "mock" && sc.Site.MockPath == "" {
			t.Errorf("the scenario %s: template=mock without mock_path", sc.Name)
		}
	}
}

// sampleMeta — the meta.yaml fields of a sample needed by the audit.
type sampleMeta struct {
	IsLaravel bool   `yaml:"is_laravel"`
	VerMin    string `yaml:"ver_min"`
}

// TestSamplesCoverLaravelVersions — "the Laravel pages of different
// versions" from spec 11.2: the testdata/laravel-samples corpus (T-073)
// covers the 5.x–12 line and is served by the bench with cookies
// (headers.json → Set-Cookie in the Caddyfile), so that the cookie-based
// analysis is possible on e2e, not only by the HTML.
func TestSamplesCoverLaravelVersions(t *testing.T) {
	samplesDir := filepath.Join(repoRoot(t), "testdata/laravel-samples")
	entries, err := os.ReadDir(samplesDir)
	if err != nil {
		t.Fatal(err)
	}

	positives := 0
	minMajor, maxMajor := -1, -1
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(samplesDir, e.Name(), "meta.yaml"))
		if err != nil {
			continue // not a sample (the generator does not wire in a directory without meta.yaml)
		}
		var meta sampleMeta
		if err := yaml.Unmarshal(raw, &meta); err != nil {
			t.Errorf("%s: meta.yaml: %v", e.Name(), err)
			continue
		}
		if !meta.IsLaravel {
			continue
		}
		positives++

		var hdrs map[string][]string
		raw, err = os.ReadFile(filepath.Join(samplesDir, e.Name(), "headers.json"))
		if err != nil {
			t.Errorf("%s: a positive sample without headers.json: %v", e.Name(), err)
			continue
		}
		if err := yaml.Unmarshal(raw, &hdrs); err != nil {
			t.Errorf("%s: headers.json: %v", e.Name(), err)
			continue
		}
		if len(hdrs["Set-Cookie"]) == 0 {
			t.Errorf("%s: the positive sample has no Set-Cookie — the cookie-based analysis is impossible on e2e", e.Name())
			continue
		}
		hasLaravelCookie := false
		for _, c := range hdrs["Set-Cookie"] {
			// The Laravel cookies: XSRF-TOKEN, laravel_session,
			// remember_<guard> or a _session suffix with an encrypted
			// value (the rules of T-073).
			if strings.HasPrefix(c, "XSRF-TOKEN=") || strings.HasPrefix(c, "laravel_session=") ||
				strings.HasPrefix(c, "remember_") || strings.Contains(c, "_session=") {
				hasLaravelCookie = true
			}
		}
		if !hasLaravelCookie {
			t.Errorf("%s: the Set-Cookie has no Laravel cookies (XSRF-TOKEN/_session/remember_)", e.Name())
		}

		if major := majorOf(meta.VerMin); minMajor < 0 || major < minMajor {
			minMajor = major
		}
		if major := majorOf(meta.VerMin); major > maxMajor {
			maxMajor = major
		}
	}

	if positives < 7 {
		t.Errorf("Laravel positive samples = %d, want >= 7 (a range of versions and mix/vite/inertia/livewire stacks)", positives)
	}
	// The version line: from the legacy 5.x to the current 12 (spec 11.2
	// "different versions").
	if minMajor > 6 || maxMajor < 11 {
		t.Errorf("the sample major-version range [%d..%d], want 5.x–12 coverage", minMajor, maxMajor)
	}
}

// majorOf — the major part of a version string ("5.8" → 5, "11" → 11).
func majorOf(ver string) int {
	n := 0
	for _, r := range ver {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}

// ---------------------------------------------------------------- generate sync

// TestGeneratedArtifactsInSync — the committed generated artifacts (the
// zones, zones.conf, the Caddyfile, the web/sites fixtures) match
// scenarios.yaml and the testdata/laravel-samples corpus. It catches a
// desync like T-073: the samples were added but make testbed-gen was not
// run after the edit — the sample domains do not resolve and the bench
// silently loses the scenarios (the CI of the zones depends on the commit
// of the artifacts). Regeneration: an isolated root with a copy of
// scenarios.yaml and a symlink to the real samples directory.
func TestGeneratedArtifactsInSync(t *testing.T) {
	root := repoRoot(t)
	tmp := t.TempDir()

	if err := os.MkdirAll(filepath.Join(tmp, "deploy/docker/testbed"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmp, "testdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "deploy/docker/testbed/scenarios.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "deploy/docker/testbed/scenarios.yaml"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "testdata/laravel-samples"), filepath.Join(tmp, "testdata/laravel-samples")); err != nil {
		t.Fatal(err)
	}

	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	g := &generator{cfg: cfg, zones: map[string]*zoneData{}, baseDir: tmp}
	g.run()

	for _, rel := range []string{
		"deploy/docker/testbed/coredns/zones.conf",
		"deploy/docker/testbed/web/Caddyfile",
		"deploy/docker/testbed/web/sites",
		"deploy/docker/testbed/coredns/zones",
	} {
		assertTreesEqual(t, filepath.Join(tmp, rel), filepath.Join(root, rel))
	}
}

// assertTreesEqual — the file trees are identical (the file set and the
// contents).
func assertTreesEqual(t *testing.T, got, want string) {
	t.Helper()
	snapshot := func(root string) map[string]string {
		t.Helper()
		out := map[string]string{}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(raw)
			out[filepath.ToSlash(rel)] = fmt.Sprintf("%x", sum)
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root, err)
		}
		return out
	}

	gotFiles, wantFiles := snapshot(got), snapshot(want)
	for rel, sum := range wantFiles {
		if gotSum, ok := gotFiles[rel]; !ok {
			t.Errorf("%s: the file is missing from the fresh generate (stale/extra): %s", filepath.Base(got), rel)
		} else if gotSum != sum {
			t.Errorf("%s: %s diverges from scenarios.yaml — run make testbed-gen and commit the generated artifacts", filepath.Base(got), rel)
		}
	}
	for rel := range gotFiles {
		if _, ok := wantFiles[rel]; !ok {
			t.Errorf("%s: the fresh generate has a file missing from the repository: %s (run make testbed-gen)", filepath.Base(got), rel)
		}
	}
}

// ---------------------------------------------------------------- addSamples

// TestAddSamples — the wiring of the samples corpus: a domain + an A
// record for each directory with a meta.yaml, the headers from
// headers.json (including several Set-Cookie) land in the Caddy snippet,
// a directory without a meta.yaml is skipped.
func TestAddSamples(t *testing.T) {
	tmp := t.TempDir()
	samples := map[string]string{
		"laravel-ex": `is_laravel: true
ver_min: "11"
`,
		"plain-ex": `is_laravel: false
`,
		"not-a-sample": "", // no meta.yaml — not wired in
	}
	for id, meta := range samples {
		dir := filepath.Join(tmp, "testdata/laravel-samples", id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if meta != "" {
			if err := os.WriteFile(filepath.Join(dir, "meta.yaml"), []byte(meta), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	noHeaders := filepath.Join(tmp, "testdata/laravel-samples/plain-ex")
	if err := os.WriteFile(filepath.Join(noHeaders, "index.html"), []byte("<html></html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	withHeaders := filepath.Join(tmp, "testdata/laravel-samples/laravel-ex")
	if err := os.WriteFile(filepath.Join(withHeaders, "index.html"), []byte("<html></html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(withHeaders, "headers.json"), []byte(`{
  "Set-Cookie": [
    "XSRF-TOKEN=eyJpdiI6ZXhhbXBsZToxNjp7fQ==; path=/; secure",
    "laravel_session=eyJpdiI6ZXhhbXBsZToxNjp7fQ==; path=/; httponly"
  ],
  "X-Frame-Options": ["SAMEORIGIN"]
}`), 0o644); err != nil {
		t.Fatal(err)
	}

	g := &generator{
		cfg:     Config{Network: Network{TestwebIP: "172.28.100.12"}},
		zones:   map[string]*zoneData{},
		baseDir: tmp,
	}
	g.zone("crawl.test")
	g.addSamples("crawl.test")

	var aRecords bytes.Buffer
	for _, r := range g.zones["crawl.test"].records {
		aRecords.WriteString(r + "\n")
	}
	caddy := g.caddy.String()

	for _, id := range []string{"laravel-ex", "plain-ex"} {
		if !strings.Contains(aRecords.String(), id+" IN A 172.28.100.12") {
			t.Errorf("the sample %s got no A record in the base zone", id)
		}
		if !strings.Contains(caddy, id+".crawl.test") {
			t.Errorf("the sample %s got no virtual host in the Caddyfile", id)
		}
	}
	if strings.Contains(caddy, "not-a-sample") || strings.Contains(aRecords.String(), "not-a-sample") {
		t.Error("a directory without meta.yaml must not be picked up as a sample")
	}

	// Headers: both cookies and an ordinary header; the names are sorted
	// (Set-Cookie before X-Frame-Options) — the sorting gives the
	// determinism of the generated artifacts regardless of the key order
	// in headers.json.
	for _, want := range []string{
		`header +Set-Cookie "XSRF-TOKEN=`,
		`header +Set-Cookie "laravel_session=`,
		`header +X-Frame-Options "SAMEORIGIN"`,
		"root * /srv/samples/laravel-ex",
	} {
		if !strings.Contains(caddy, want) {
			t.Errorf("the sample snippet is missing the fragment %q", want)
		}
	}
}

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

// repoRoot — корень репозитория относительно каталога пакета (тесты
// запускаются из cmd/testbedgen).
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// loadScenarios — реальный scenarios.yaml стенда как Config.
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

// TestScenariosCoverTZ — аудит полноты стенда против обязательного состава
// ТЗ 11.2: каждый сценарий из списка задачи T-090 присутствует и описан
// корректно (сверка таблицей; «страницы Laravel разных версий» — отдельный
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
		req      string // требование §11.2
		scenario string
		ok       func(d DNSCfg, s SiteCfg) bool
	}{
		{"NXDOMAIN", "nxdomain-case", func(d DNSCfg, _ SiteCfg) bool { return d == (DNSCfg{}) }},
		{"wildcard", "wildcard", func(d DNSCfg, _ SiteCfg) bool { return d.Wildcard && d.A == "testweb" }},
		{"тарпит", "tarpit", mockAt("/tarpit")},
		{"бесконечный редирект", "infinite-redirect", mockAt("/redirect/infinite")},
		{"гигантское тело", "huge-body", mockAt("/huge")},
		{"SPA-страница", "spa-shell", func(_ DNSCfg, s SiteCfg) bool { return s.Template == "spa" }},
	}
	for _, tc := range cases {
		t.Run(tc.req, func(t *testing.T) {
			sc, found := byName[tc.scenario]
			if !found {
				t.Fatalf("сценарий %q (ТЗ 11.2 «%s») отсутствует в scenarios.yaml", tc.scenario, tc.req)
			}
			if !tc.ok(sc.DNS, sc.Site) {
				t.Errorf("сценарий %q не соответствует ТЗ 11.2 «%s»: dns=%+v site=%+v",
					tc.scenario, tc.req, sc.DNS, sc.Site)
			}
		})
	}

	// Контракт mock-сценариев: у каждого указан mock_path — маршрут
	// cmd/mockserver.hostRoutes обязан обслуживать «/» по Host сценария.
	for _, sc := range cfg.Scenarios {
		if sc.Site.Template == "mock" && sc.Site.MockPath == "" {
			t.Errorf("сценарий %s: template=mock без mock_path", sc.Name)
		}
	}
}

// sampleMeta — поля meta.yaml образца, нужные аудиту.
type sampleMeta struct {
	IsLaravel bool   `yaml:"is_laravel"`
	VerMin    string `yaml:"ver_min"`
}

// TestSamplesCoverLaravelVersions — «страницы Laravel разных версий» из
// ТЗ 11.2: корпус testdata/laravel-samples (T-073) покрывает линейку 5.x–12
// и отдаётся стендом с куками (headers.json → Set-Cookie в Caddyfile),
// чтобы анализ по cookies был возможен на e2e, а не только по HTML.
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
			continue // не образец (каталог без meta.yaml генератор не подключает)
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
			t.Errorf("%s: позитивный образец без headers.json: %v", e.Name(), err)
			continue
		}
		if err := yaml.Unmarshal(raw, &hdrs); err != nil {
			t.Errorf("%s: headers.json: %v", e.Name(), err)
			continue
		}
		if len(hdrs["Set-Cookie"]) == 0 {
			t.Errorf("%s: у позитивного образца нет Set-Cookie — анализ по кукам невозможен на e2e", e.Name())
			continue
		}
		hasLaravelCookie := false
		for _, c := range hdrs["Set-Cookie"] {
			// Куки Laravel: XSRF-TOKEN, laravel_session, remember_<guard> или
			// суффикс _session с зашифрованным значением (правила T-073).
			if strings.HasPrefix(c, "XSRF-TOKEN=") || strings.HasPrefix(c, "laravel_session=") ||
				strings.HasPrefix(c, "remember_") || strings.Contains(c, "_session=") {
				hasLaravelCookie = true
			}
		}
		if !hasLaravelCookie {
			t.Errorf("%s: в Set-Cookie нет кук Laravel (XSRF-TOKEN/_session/remember_)", e.Name())
		}

		if major := majorOf(meta.VerMin); minMajor < 0 || major < minMajor {
			minMajor = major
		}
		if major := majorOf(meta.VerMin); major > maxMajor {
			maxMajor = major
		}
	}

	if positives < 7 {
		t.Errorf("позитивных образцов Laravel = %d, хочу >= 7 (спектр версий и стеков mix/vite/inertia/livewire)", positives)
	}
	// Линейка версий: от legacy 5.x до актуальной 12 (ТЗ 11.2 «разных версий»).
	if minMajor > 6 || maxMajor < 11 {
		t.Errorf("спектр мажорных версий образцов [%d..%d], хочу покрытие 5.x–12", minMajor, maxMajor)
	}
}

// majorOf — мажорная часть строки версии («5.8» → 5, «11» → 11).
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

// ---------------------------------------------------------------- синхронность генерата

// TestGeneratedArtifactsInSync — закоммиченный генерат (зоны, zones.conf,
// Caddyfile, фикстуры web/sites) соответствует scenarios.yaml и корпусу
// testdata/laravel-samples. Ловит рассинхрон типа T-073: образцы добавлены,
// а make testbed-gen после правки не выполнен — домены образцов не
// резолвятся и стенд молча теряет сценарии (CI зон зависит от коммита
// генерата). Регенерация: изолированный корень с копией scenarios.yaml и
// symlink на реальный каталог образцов.
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

// assertTreesEqual — файловые деревья идентичны (набор файлов и содержимое).
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
			t.Fatalf("обход %s: %v", root, err)
		}
		return out
	}

	gotFiles, wantFiles := snapshot(got), snapshot(want)
	for rel, sum := range wantFiles {
		if gotSum, ok := gotFiles[rel]; !ok {
			t.Errorf("%s: файла нет в свежем генерате (устарел/лишний): %s", filepath.Base(got), rel)
		} else if gotSum != sum {
			t.Errorf("%s: %s расходится с scenarios.yaml — выполните make testbed-gen и закоммитьте генерат", filepath.Base(got), rel)
		}
	}
	for rel := range gotFiles {
		if _, ok := wantFiles[rel]; !ok {
			t.Errorf("%s: в свежем генерате появился файл, отсутствующий в репозитории: %s (выполните make testbed-gen)", filepath.Base(got), rel)
		}
	}
}

// ---------------------------------------------------------------- addSamples

// TestAddSamples — подключение корпуса образцов: домен + A-запись для
// каждого каталога с meta.yaml, заголовки из headers.json (в т.ч. несколько
// Set-Cookie) попадают в сниппет Caddy, каталог без meta.yaml пропускается.
func TestAddSamples(t *testing.T) {
	tmp := t.TempDir()
	samples := map[string]string{
		"laravel-ex": `is_laravel: true
ver_min: "11"
`,
		"plain-ex": `is_laravel: false
`,
		"not-a-sample": "", // без meta.yaml — не подключается
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
			t.Errorf("образец %s не получил A-запись в базовой зоне", id)
		}
		if !strings.Contains(caddy, id+".crawl.test") {
			t.Errorf("образец %s не получил виртуальный хост в Caddyfile", id)
		}
	}
	if strings.Contains(caddy, "not-a-sample") || strings.Contains(aRecords.String(), "not-a-sample") {
		t.Error("каталог без meta.yaml не должен подключаться как образец")
	}

	// Заголовки: обе куки и обычный заголовок; имена отсортированы
	// (Set-Cookie раньше X-Frame-Options) — сортировка даёт детерминизм
	// генерата независимо от порядка ключей в headers.json.
	for _, want := range []string{
		`header +Set-Cookie "XSRF-TOKEN=`,
		`header +Set-Cookie "laravel_session=`,
		`header +X-Frame-Options "SAMEORIGIN"`,
		"root * /srv/samples/laravel-ex",
	} {
		if !strings.Contains(caddy, want) {
			t.Errorf("сниппет образца без фрагмента %q", want)
		}
	}
}

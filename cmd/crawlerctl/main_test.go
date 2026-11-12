package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crawler/internal/content"
)

// runCase — a TestRun scenario: the CLI arguments, the expected exit
// code and the substrings in stdout/stderr (an empty string — the output
// is not checked).
type runCase struct {
	name  string
	args  []string
	code  int
	inOut string
	inErr string
	noOut string // substrings that must not be in stdout
}

func TestRun(t *testing.T) {
	cases := []runCase{
		// Top-level routing.
		{name: "без аргументов — справка и код 2", args: nil, code: exitUsage, inErr: "crawlerctl — CLI Domain Crawler"},
		{name: "help", args: []string{"help"}, code: exitOK, inOut: "crawlerctl — CLI Domain Crawler"},
		{name: "--help", args: []string{"--help"}, code: exitOK, inOut: "crawlerctl — CLI"},
		{name: "неизвестная команда", args: []string{"nope"}, code: exitUsage, inErr: `неизвестная команда "nope"`},

		// status — a functional command (T-079): it needs configuration
		// (DB/Redis); the execution path without infrastructure is
		// TestRunStatus in status_test.go; here — usage and help.
		{name: "status лишний аргумент", args: []string{"status", "foo"}, code: exitUsage, inErr: "лишние аргументы"},
		{name: "status неизвестный флаг", args: []string{"status", "--nope"}, code: exitUsage, inErr: "status: "},
		{name: "status --help", args: []string{"status", "--help"}, code: exitOK, inOut: "сводка по зонам"},

		// domain — a functional command (T-081): it needs configuration
		// (PostgreSQL, and the content directory for --content); the
		// execution path without infrastructure is TestRunDomain in
		// domain_test.go; here — usage and help.
		{name: "domain без имени", args: []string{"domain"}, code: exitUsage, inErr: "укажите имя домена"},
		{name: "domain два имени", args: []string{"domain", "a.test", "b.test"}, code: exitUsage, inErr: "укажите имя домена"},
		{name: "domain --json --content несовместимы", args: []string{"domain", "--json", "--content", "x.crawl.test"},
			code: exitUsage, inErr: "несовместимы"},
		{name: "domain невалидное имя (до конфигурации)", args: []string{"domain", "bad!!name.test"}, code: exitUsage,
			inErr: "некорректное имя домена"},
		{name: "domain help первым аргументом", args: []string{"domain", "help"}, code: exitOK, inOut: "crawlerctl domain"},
		{name: "domain help после флага — позиционный аргумент", args: []string{"domain", "--json", "help"},
			code: exitUsage, inErr: "некорректное имя домена"},
		{name: "domain неизвестный флаг", args: []string{"domain", "--nope", "x.test"}, code: exitUsage,
			inErr: "domain: "},
		{name: "domain --help", args: []string{"domain", "--help"}, code: exitOK, inOut: "ТЗ 5.11"},

		// subdomains — a functional command (T-083): it needs
		// configuration (the DB) — the execution path without
		// infrastructure is checked by the separate
		// TestRunSubdomainsExecution and the subdomains_test.go subtests;
		// here — usage and help.
		{name: "subdomains без действия", args: []string{"subdomains"}, code: exitUsage, inErr: "укажите действие"},
		{name: "subdomains неизвестное действие", args: []string{"subdomains", "fly"}, code: exitUsage, inErr: `допустимы: list, add, disable, recheck`},
		{name: "subdomains add без метки", args: []string{"subdomains", "add"}, code: exitUsage, inErr: "укажите метку"},
		{name: "subdomains add пустая метка", args: []string{"subdomains", "add", ""}, code: exitUsage, inErr: "апекс"},
		{name: "subdomains add дефис по краям (после --)", args: []string{"subdomains", "add", "--", "-bad-"}, code: exitUsage, inErr: "некорректная метка субдомена"},
		{name: "subdomains add метка 64", args: []string{"subdomains", "add", strings.Repeat("a", 64)}, code: exitUsage, inErr: "некорректная метка субдомена"},
		{name: "subdomains add метка с точкой", args: []string{"subdomains", "add", "a.b"}, code: exitUsage, inErr: "некорректная метка субдомена"},
		{name: "subdomains disable без метки", args: []string{"subdomains", "disable"}, code: exitUsage, inErr: "укажите субдомен"},
		{name: "subdomains --help", args: []string{"subdomains", "--help"}, code: exitOK, inOut: "subdomains list"},
		{name: "subdomains add --help", args: []string{"subdomains", "add", "--help"}, code: exitOK, inOut: "управление проверяемыми субдоменами"},

		// zones — a functional command (T-084): it needs configuration
		// (the DB) — the execution path without infrastructure is checked
		// by the separate TestRunZonesExecution and the zones_test.go
		// subtests; here — usage and help.
		{name: "zones без действия", args: []string{"zones"}, code: exitUsage, inErr: "укажите действие"},
		{name: "zones неизвестное действие", args: []string{"zones", "fly"}, code: exitUsage, inErr: `допустимы: list, enable, disable, sync, collapse`},
		{name: "zones enable без зоны", args: []string{"zones", "enable"}, code: exitUsage, inErr: "укажите зону"},
		{name: "zones enable невалидная зона", args: []string{"zones", "enable", "bad!!zone"}, code: exitUsage, inErr: "некорректное имя зоны"},
		{name: "zones --help", args: []string{"zones", "--help"}, code: exitOK, inOut: "zones list"},
		{name: "zones enable --help", args: []string{"zones", "enable", "--help"}, code: exitOK, inOut: "управление зонами проверки"},
		{name: "zones sync --help", args: []string{"zones", "sync", "--help"}, code: exitOK, inOut: "tlds-alpha-by-domain.txt"},

		// reanalyze — a functional command (T-074): it needs configuration
		// (DB/Redis) — the execution path with infrastructure is checked
		// by the separate TestRunReanalyze; here — usage and help.
		{name: "reanalyze --zone невалидная", args: []string{"reanalyze", "--zone", "bad!!zone"}, code: exitUsage, inErr: "некорректное имя зоны"},
		{name: "reanalyze --zone без значения", args: []string{"reanalyze", "--zone"}, code: exitUsage, inErr: "reanalyze: "},
		{name: "reanalyze лишний аргумент", args: []string{"reanalyze", "foo"}, code: exitUsage, inErr: "лишние аргументы"},
		{name: "reanalyze --help", args: []string{"reanalyze", "--help"}, code: exitOK, inOut: "переанализ сохранённых результатов"},

		// recheck — a functional command (T-084): it needs configuration
		// (the DB) — the execution path without infrastructure is checked
		// by the separate TestRunRecheckExecution and the recheck_test.go
		// subtests; here — usage and help.
		{name: "recheck без домена", args: []string{"recheck"}, code: exitUsage, inErr: "укажите домен"},
		{name: "recheck невалидное имя", args: []string{"recheck", "ok.crawl.test", "bad!!name"}, code: exitUsage, inErr: "некорректное имя домена"},
		{name: "recheck --help", args: []string{"recheck", "--help"}, code: exitOK, inOut: "принудительная постановка"},

		// dict — zstd dictionary training (T-043).
		{name: "dict без действия", args: []string{"dict"}, code: exitUsage, inErr: "укажите действие"},
		{name: "dict неизвестное действие", args: []string{"dict", "fly"}, code: exitUsage, inErr: `допустимы: train`},
		{name: "dict --help", args: []string{"dict", "--help"}, code: exitOK, inOut: "словарём сжатия zstd"},
		{name: "dict train --help", args: []string{"dict", "train", "--help"}, code: exitOK, inOut: "crawlerctl dict"},
		{name: "dict train без файла", args: []string{"dict", "train"}, code: exitUsage, inErr: "укажите выходной файл"},
		{name: "dict train два позиционных", args: []string{"dict", "train", "a.zst", "b.zst"}, code: exitUsage,
			inErr: "укажите выходной файл"},
		{name: "dict train несуществующие образцы", args: []string{"dict", "train", "d.zst", "--samples", "/nonexistent"},
			code: exitError, inErr: "dict train: образцы /nonexistent:"},
		{name: "dict train --help значением --samples — не справка, а путь образцов",
			args: []string{"dict", "train", "d.zst", "--samples", "--help"},
			code: exitError, inErr: "dict train: образцы --help:", noOut: "словарём сжатия zstd"},
		{name: "dict train неизвестный флаг", args: []string{"dict", "train", "--nope", "d.zst"}, code: exitUsage,
			inErr: "dict train: "},

		// dashboard-secret — dashboard password hash generation (dashboard
		// spec §7.3): it runs without infrastructure; the functional tests
		// and the roundtrip — TestRunDashboardSecret* in
		// dashboardsecret_test.go.
		{name: "dashboard-secret пустой аргумент", args: []string{"dashboard-secret", ""}, code: exitUsage, inErr: "пустой пароль"},
		{name: "dashboard-secret два аргумента", args: []string{"dashboard-secret", "a", "b"}, code: exitUsage, inErr: "лишние аргументы"},
		{name: "dashboard-secret неизвестный флаг", args: []string{"dashboard-secret", "--nope"}, code: exitUsage,
			inErr: "dashboard-secret: "},
		{name: "dashboard-secret --help", args: []string{"dashboard-secret", "--help"}, code: exitOK,
			inOut: "истории shell"},

		// dev commands: routing and help (the functional tests live with
		// the commands themselves).
		{name: "seed --help", args: []string{"seed", "--help"}, code: exitOK, inOut: "crawlerctl seed"},
		{name: "seed несуществующий сценарий", args: []string{"seed", "--scenarios", "/nonexistent/scenarios.yaml"},
			code: exitError, inErr: "seed:"},
		{name: "test-rules --help", args: []string{"test-rules", "--help"}, code: exitOK, inOut: "crawlerctl test-rules"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) код = %d, хочу %d", tc.args, code, tc.code)
			}
			if tc.inOut != "" && !strings.Contains(stdout.String(), tc.inOut) {
				t.Errorf("run(%q) stdout не содержит %q;\nstdout:\n%s", tc.args, tc.inOut, stdout.String())
			}
			if tc.noOut != "" && strings.Contains(stdout.String(), tc.noOut) {
				t.Errorf("run(%q) stdout содержит %q, чего не ждали;\nstdout:\n%s", tc.args, tc.noOut, stdout.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr не содержит %q;\nstderr:\n%s", tc.args, tc.inErr, stderr.String())
			}
		})
	}
}

// TestRunReanalyze — the reanalyze execution path (T-074) without
// infrastructure: the command needs configuration (the PostgreSQL DSN,
// Redis, the rules directory) — without it, it ends with an execution
// error (code 1) carrying the task context, without touching the
// network. Input validation comes first: a bad zone is code 2 even
// without configuration (the TestRun table). Working against live
// DB/Redis — the dev environment (make reanalyze).
func TestRunReanalyze(t *testing.T) {
	// An explicitly missing config file: a deterministic refusal before
	// any connections (in the tools container the APP_* variables could
	// assemble a configuration from the environment — the test must not
	// touch the live stack).
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	var stdout, stderr bytes.Buffer
	code := run([]string{"reanalyze"}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitError, stderr.String())
	}
	if !strings.Contains(stderr.String(), "reanalyze: config:") {
		t.Errorf("stderr не содержит контекст конфигурации:\n%s", stderr.String())
	}

	// A valid zone — the same configuration error (the input check
	// passed).
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"reanalyze", "--zone", "Crawl.Test."}, &stdout, &stderr)
	if code != exitError || !strings.Contains(stderr.String(), "reanalyze: config:") {
		t.Errorf("код = %d, stderr:\n%s; хочу ошибку конфигурации (код 1)", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout до соединения не пуст:\n%s", stdout.String())
	}
}

// TestUsageTextListsCommands — the help mentions every command of the
// table (a forgotten command is a skeleton's usual bug).
func TestUsageTextListsCommands(t *testing.T) {
	for name := range commands {
		if !strings.Contains(usageText, name) {
			t.Errorf("usageText не упоминает команду %q", name)
		}
	}
	for _, want := range []string{"--json", "--content", "--zone", "--bulk", "--min-score", "--samples", "list|add|disable|recheck", "list|enable|disable"} {
		if !strings.Contains(usageText, want) {
			t.Errorf("usageText не упоминает %q", want)
		}
	}
}

// TestRunHelpPerCommand — every command's --help gives code 0 and a
// human-readable help (not empty and not the default flag.PrintDefaults).
func TestRunHelpPerCommand(t *testing.T) {
	for name := range commands {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run([]string{name, "--help"}, &stdout, &stderr)
			if code != exitOK {
				t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitOK, stderr.String())
			}
			out := stdout.String()
			if !strings.HasPrefix(out, "crawlerctl "+name) {
				t.Errorf("справка не начинается с имени команды:\n%s", out)
			}
			if !strings.Contains(out, "Использование") {
				t.Errorf("в справке нет раздела «Использование»:\n%s", out)
			}
		})
	}
}

// TestParseFlags — the parseFlags contract: an explicit help → ErrHelp;
// a parsing error → usageErr; success — the flags are parsed, the
// positionals are available.
func TestParseFlags(t *testing.T) {
	var stdout bytes.Buffer

	t.Run("help", func(t *testing.T) {
		fs := newFlagSet("x")
		fs.Bool("json", false, "")
		handled, err := parseFlags(fs, "СПРАВКА x", []string{"--json", "--help"}, &stdout)
		if !handled || err != flag.ErrHelp {
			t.Fatalf("handled=%v err=%v, хочу (true, flag.ErrHelp)", handled, err)
		}
		if !strings.Contains(stdout.String(), "СПРАВКА x") {
			t.Errorf("справка не напечатана:\n%s", stdout.String())
		}
	})

	// The value of a flag that takes a value is not a help request, even
	// when it is the --help token itself (CLI-03).
	t.Run("help значением флага", func(t *testing.T) {
		fs := newFlagSet("x")
		fs.String("incoming", "", "")
		stdout.Reset()
		handled, err := parseFlags(fs, "СПРАВКА x", []string{"--incoming", "--help"}, &stdout)
		if handled || err != nil {
			t.Fatalf("handled=%v err=%v, хочу успех без справки", handled, err)
		}
		if stdout.Len() != 0 {
			t.Errorf("справка напечатана:\n%s", stdout.String())
		}
		if got := fs.Lookup("incoming").Value.String(); got != "--help" {
			t.Errorf("--incoming = %q, хочу значение %q", got, "--help")
		}
	})

	t.Run("help после терминатора и позиционного", func(t *testing.T) {
		for _, args := range [][]string{{"--", "--help"}, {"pos", "--help"}} {
			fs := newFlagSet("x")
			stdout.Reset()
			handled, err := parseFlags(fs, "СПРАВКА x", args, &stdout)
			if handled || err != nil {
				t.Fatalf("parseFlags(%q): handled=%v err=%v, хочу успех без справки", args, handled, err)
			}
			if stdout.Len() != 0 {
				t.Errorf("parseFlags(%q): справка напечатана:\n%s", args, stdout.String())
			}
		}
	})

	t.Run("ошибка парсинга — usage", func(t *testing.T) {
		fs := newFlagSet("x")
		fs.Bool("json", false, "")
		handled, err := parseFlags(fs, "", []string{"--nope"}, &stdout)
		if !handled || err == nil {
			t.Fatalf("handled=%v err=%v, хочу ошибка использования", handled, err)
		}
		if !isUsageErr(err) {
			t.Errorf("err = %T, хочу usageErr", err)
		}
	})

	t.Run("позиционные аргументы", func(t *testing.T) {
		fs := newFlagSet("x")
		fs.String("zone", "", "")
		handled, err := parseFlags(fs, "", []string{"--zone", "z", "pos1", "pos2"}, &stdout)
		if handled || err != nil {
			t.Fatalf("handled=%v err=%v, хочу успех", handled, err)
		}
		if fs.NArg() != 2 || fs.Arg(0) != "pos1" {
			t.Errorf("позиционные: NArg=%d Arg(0)=%q", fs.NArg(), fs.Arg(0))
		}
	})
}

// TestNormalizeSubdomainLabel — subdomain label validation: punycode,
// case, edges, length.
func TestNormalizeSubdomainLabel(t *testing.T) {
	ok := map[string]string{
		"www":          "www",
		"Admin":        "admin",
		"api-2":        "api-2",
		"москва":       "xn--80adxhks",
		"xn--80adxhks": "xn--80adxhks",
		"a":            "a",
	}
	for in, want := range ok {
		if got, err := normalizeSubdomainLabel(in); err != nil || got != want {
			t.Errorf("normalizeSubdomainLabel(%q) = %q, %v; хочу %q", in, got, err, want)
		}
	}
	bad := []string{"", "   ", "-lead", "trail-", "a.b", "bad!!", strings.Repeat("a", 64), "ап..рель"}
	for _, in := range bad {
		if got, err := normalizeSubdomainLabel(in); err == nil {
			t.Errorf("normalizeSubdomainLabel(%q) = %q, хочу ошибку", in, got)
		}
	}
}

// dictTrainSamplePages — deterministic HTML samples for the command's
// functional test: a shared template + unique inserts (as in
// internal/content).
func dictTrainSamplePages(n int) []string {
	pages := make([]string, n)
	for i := 0; i < n; i++ {
		pages[i] = fmt.Sprintf(
			"<!DOCTYPE html><html><head><title>site %d</title></head>"+
				"<body><header class=\"site-header\">шапка парковки</header>"+
				"<main><p>уникальный текст страницы %d</p></main>"+
				"<footer class=\"site-footer\">продаётся: admin@example.invalid</footer>"+
				"</body></html>", i, i)
	}
	return pages
}

// dictTrainSamplePagesAlt — a second template (a hosting suspension
// page): a different dictionary content — a different ID (the
// retraining scenario, T-103).
func dictTrainSamplePagesAlt(n int) []string {
	pages := make([]string, n)
	for i := range pages {
		pages[i] = fmt.Sprintf(
			"<!DOCTYPE html><html><head><title>suspended %d</title></head>"+
				"<body><header class=\"hdr\">account suspended</header>"+
				"<main><p>unique page text %d</p></main>"+
				"<footer class=\"ftr\">support@hosting.invalid</footer>"+
				"</body></html>", i, i)
	}
	return pages
}

// writeSampleDir — a directory with HTML page samples.
func writeSampleDir(t *testing.T, dir string, pages []string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, page := range pages {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("page%02d.html", i)), []byte(page), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRunDictTrainRetiresPrevious — retraining archives the previous
// dictionary into the registry (dict-<id>.zstd next to the new file):
// frames compressed by the previous dictionary stay readable after the
// active one is replaced (T-103).
func TestRunDictTrainRetiresPrevious(t *testing.T) {
	dir := t.TempDir()
	samplesA := filepath.Join(dir, "a")
	samplesB := filepath.Join(dir, "b")
	writeSampleDir(t, samplesA, dictTrainSamplePages(10))
	writeSampleDir(t, samplesB, dictTrainSamplePagesAlt(10))
	dictPath := filepath.Join(dir, "dict.zst")

	// The first training.
	var stdout, stderr bytes.Buffer
	if code := run([]string{"dict", "train", dictPath, "--samples", samplesA}, &stdout, &stderr); code != exitOK {
		t.Fatalf("первый train: код = %d, stderr:\n%s", code, stderr.String())
	}
	first, err := content.NewDict(mustReadFile(t, dictPath))
	if err != nil {
		t.Fatalf("словарь первого train: %v", err)
	}
	// A frame compressed by the first dictionary (content "before
	// retraining").
	page := dictTrainSamplePages(101)[100]
	frame := first.Compress([]byte(page))

	// Retraining: the previous dictionary — in the registry.
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"dict", "train", dictPath, "--samples", samplesB}, &stdout, &stderr); code != exitOK {
		t.Fatalf("второй train: код = %d, stderr:\n%s", code, stderr.String())
	}
	outStr := stdout.String()
	if !strings.Contains(outStr, "в реестре") || !strings.Contains(outStr, fmt.Sprintf("%08x", first.ID())) {
		t.Errorf("stdout не сообщает об архивации прежнего словаря:\n%s", outStr)
	}
	regPath := content.DictRegistryPath(dir, first.ID())
	retired, err := content.NewDict(mustReadFile(t, regPath))
	if err != nil {
		t.Fatalf("файл реестра %s: %v", regPath, err)
	}
	if retired.ID() != first.ID() {
		t.Errorf("словарь в реестре ID %08x, хочу %08x", retired.ID(), first.ID())
	}
	second, err := content.NewDict(mustReadFile(t, dictPath))
	if err != nil {
		t.Fatalf("новый активный словарь: %v", err)
	}
	if second.ID() == first.ID() {
		t.Fatal("переобучение не сменило ID словаря — тест ничего не проверяет")
	}

	// The old frame is readable after retraining: the registry is loaded
	// through the same configuration path as the active file.
	if ok, err := content.SetDictFile(dictPath, nil); err != nil || !ok {
		t.Fatalf("SetDictFile: ok=%v err=%v", ok, err)
	}
	defer func() { _, _ = content.SetDictFile("", nil) }()
	got, err := content.DecompressWithDict(frame)
	if err != nil || !strings.Contains(string(got), "site 100") {
		t.Errorf("кадр прежнего словаря после переобучения: err=%v, содержит=%q", err, string(got))
	}
}

// mustReadFile — a file's contents or a fatal test error.
func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение %s: %v", path, err)
	}
	return raw
}

// TestRunDictTrain — an end-to-end run of the command: samples → a
// dictionary in a file, code 0; the dictionary is valid and decompresses
// (content.NewDict).
func TestRunDictTrain(t *testing.T) {
	dir := t.TempDir()
	samples := filepath.Join(dir, "samples")
	if err := os.MkdirAll(samples, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, page := range dictTrainSamplePages(10) {
		if err := os.WriteFile(filepath.Join(samples, fmt.Sprintf("page%02d.html", i)), []byte(page), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A non-HTML file — ignored by the sample collector.
	if err := os.WriteFile(filepath.Join(samples, "meta.yaml"), []byte("id: x"), 0o644); err != nil {
		t.Fatal(err)
	}

	dictPath := filepath.Join(dir, "dict.zst")
	var stdout, stderr bytes.Buffer
	code := run([]string{"dict", "train", dictPath, "--samples", samples}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitOK, stderr.String())
	}
	outStr := stdout.String()
	// meta.yaml is not included: the collector takes only *.html/*.htm.
	for _, want := range []string{"файлов-образцов 10 из 10", "словарь обучен", "словарь записан"} {
		if !strings.Contains(outStr, want) {
			t.Errorf("stdout не содержит %q:\n%s", want, outStr)
		}
	}

	raw, err := os.ReadFile(dictPath)
	if err != nil {
		t.Fatalf("файл словаря не создан: %v", err)
	}
	d, err := content.NewDict(raw)
	if err != nil {
		t.Fatalf("словарь невалиден: %v", err)
	}
	page := dictTrainSamplePages(101)[100] // a fresh page, not from the training set
	if _, err := d.Decompress(d.Compress([]byte(page))); err != nil {
		t.Errorf("roundtrip со словарём команды: %v", err)
	}

	// A directory without HTML — an execution error (code 1), not usage.
	empty := filepath.Join(dir, "empty")
	if err := os.MkdirAll(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"dict", "train", dictPath, "--samples", empty}, &stdout, &stderr)
	if code != exitError || !strings.Contains(stderr.String(), "нет HTML-образцов") {
		t.Errorf("пустой каталог: код = %d, stderr:\n%s", code, stderr.String())
	}
}

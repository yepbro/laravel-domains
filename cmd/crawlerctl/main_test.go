package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crawler/internal/content"
)

// runCase — сценарий TestRun: аргументы CLI, ожидаемый код возврата и
// подстроки в stdout/stderr (пустая строка — вывод не проверяется).
type runCase struct {
	name  string
	args  []string
	code  int
	inOut string
	inErr string
	noOut string // подстроки, которых в stdout быть не должно
}

func TestRun(t *testing.T) {
	cases := []runCase{
		// Роутинг верхнего уровня.
		{name: "без аргументов — справка и код 2", args: nil, code: exitUsage, inErr: "crawlerctl — CLI Domain Crawler"},
		{name: "help", args: []string{"help"}, code: exitOK, inOut: "crawlerctl — CLI Domain Crawler"},
		{name: "--help", args: []string{"--help"}, code: exitOK, inOut: "crawlerctl — CLI"},
		{name: "неизвестная команда", args: []string{"nope"}, code: exitUsage, inErr: `неизвестная команда "nope"`},

		// status — информационная заглушка, код 0 (как раньше).
		{name: "status", args: []string{"status"}, code: exitOK, inOut: "make ps"},
		{name: "status --help", args: []string{"status", "--help"}, code: exitOK, inOut: "T-079"},

		// domain — нормализация работает, код 0.
		{name: "domain без имени", args: []string{"domain"}, code: exitUsage, inErr: "укажите имя домена"},
		{name: "domain два имени", args: []string{"domain", "a.test", "b.test"}, code: exitUsage, inErr: "укажите имя домена"},
		{name: "domain", args: []string{"domain", "example.crawl.test"}, code: exitOK,
			inOut: "punycode:   example.crawl.test", noOut: `{"ascii"`},
		{name: "domain нормализация входа", args: []string{"domain", "Example.CRAWL.TEST."}, code: exitOK,
			inOut: "punycode:   example.crawl.test"},
		{name: "domain зона и метка", args: []string{"domain", "пример.crawl.test"}, code: exitOK,
			inOut: "punycode:   xn--e1afmkfd.crawl.test"},
		{name: "domain --json", args: []string{"domain", "--json", "Example.CRAWL.test."}, code: exitOK, inOut: `"zone":"test"`}, // полный разбор полей — TestRunDomainJSON
		{name: "domain --content", args: []string{"domain", "--content", "example.crawl.test"}, code: exitOK,
			inOut: "контент (--content)", noOut: `"ascii"`},
		{name: "domain невалидное имя", args: []string{"domain", "bad!!name.test"}, code: exitUsage,
			inErr: "некорректное имя домена"},
		{name: "domain help первым аргументом", args: []string{"domain", "help"}, code: exitOK, inOut: "crawlerctl domain"},
		{name: "domain help после флага — позиционный аргумент", args: []string{"domain", "--json", "help"},
			code: exitUsage, inErr: "некорректное имя домена"},
		{name: "domain неизвестный флаг", args: []string{"domain", "--nope", "x.test"}, code: exitUsage,
			inErr: "domain: "},
		{name: "domain --help", args: []string{"domain", "--help"}, code: exitOK, inOut: "ТЗ 5.11"},

		// subdomains — заглушки, код 1 с номером задачи.
		{name: "subdomains без действия", args: []string{"subdomains"}, code: exitUsage, inErr: "укажите действие"},
		{name: "subdomains неизвестное действие", args: []string{"subdomains", "fly"}, code: exitUsage, inErr: `допустимы: list, add, disable`},
		{name: "subdomains list", args: []string{"subdomains", "list"}, code: exitError, inErr: "subdomains list: не реализовано (задача T-083)"},
		{name: "subdomains add без метки", args: []string{"subdomains", "add"}, code: exitUsage, inErr: "укажите метку"},
		{name: "subdomains add", args: []string{"subdomains", "add", "www"}, code: exitError, inErr: "subdomains add www: не реализовано (задача T-083)"},
		{name: "subdomains add unicode-метка", args: []string{"subdomains", "add", "МосквА"}, code: exitError, inErr: "subdomains add xn--80adxhks: не реализовано"},
		{name: "subdomains add пустая метка", args: []string{"subdomains", "add", ""}, code: exitUsage, inErr: "апекс"},
		{name: "subdomains add дефис по краям (после --)", args: []string{"subdomains", "add", "--", "-bad-"}, code: exitUsage, inErr: "некорректная метка субдомена"},
		{name: "subdomains add метка 64", args: []string{"subdomains", "add", strings.Repeat("a", 64)}, code: exitUsage, inErr: "некорректная метка субдомена"},
		{name: "subdomains add метка с точкой", args: []string{"subdomains", "add", "a.b"}, code: exitUsage, inErr: "некорректная метка субдомена"},
		{name: "subdomains disable без метки", args: []string{"subdomains", "disable"}, code: exitUsage, inErr: "укажите метку"},
		{name: "subdomains disable", args: []string{"subdomains", "disable", "www"}, code: exitError, inErr: "subdomains disable www: не реализовано (задача T-083)"},
		{name: "subdomains --help", args: []string{"subdomains", "--help"}, code: exitOK, inOut: "subdomains list"},
		{name: "subdomains add --help", args: []string{"subdomains", "add", "--help"}, code: exitOK, inOut: "управление проверяемыми субдоменами"},

		// zones — заглушки, код 1 с номером задачи.
		{name: "zones без действия", args: []string{"zones"}, code: exitUsage, inErr: "укажите действие"},
		{name: "zones неизвестное действие", args: []string{"zones", "fly"}, code: exitUsage, inErr: `допустимы: list, enable, disable`},
		{name: "zones list", args: []string{"zones", "list"}, code: exitError, inErr: "zones list: не реализовано (задача T-084)"},
		{name: "zones enable без зоны", args: []string{"zones", "enable"}, code: exitUsage, inErr: "укажите зону"},
		{name: "zones enable", args: []string{"zones", "enable", "Crawl.TEST."}, code: exitError, inErr: "zones enable crawl.test: не реализовано (задача T-084)"},
		{name: "zones disable punycode", args: []string{"zones", "disable", "xn--80adxhks"}, code: exitError, inErr: "zones disable xn--80adxhks: не реализовано (задача T-084)"},
		{name: "zones enable невалидная зона", args: []string{"zones", "enable", "bad!!zone"}, code: exitUsage, inErr: "некорректное имя зоны"},
		{name: "zones --help", args: []string{"zones", "--help"}, code: exitOK, inOut: "zones list"},

		// reanalyze — информационная заглушка, код 0 (как раньше).
		{name: "reanalyze", args: []string{"reanalyze"}, code: exitOK, inOut: "T-074"},
		{name: "reanalyze --zone", args: []string{"reanalyze", "--zone", "Crawl.Test."}, code: exitOK, inOut: "T-074"},
		{name: "reanalyze --zone невалидная", args: []string{"reanalyze", "--zone", "bad!!zone"}, code: exitUsage, inErr: "некорректное имя зоны"},
		{name: "reanalyze --help", args: []string{"reanalyze", "--help"}, code: exitOK, inOut: "crawlerctl reanalyze"},

		// recheck — заглушка, код 1 с номером задачи.
		{name: "recheck без домена", args: []string{"recheck"}, code: exitUsage, inErr: "укажите домен"},
		{name: "recheck", args: []string{"recheck", "example.crawl.test"}, code: exitError, inErr: "recheck example.crawl.test: не реализовано (задача T-084)"},
		{name: "recheck несколько доменов", args: []string{"recheck", "a.crawl.test", "b.crawl.test"}, code: exitError,
			inErr: "recheck a.crawl.test b.crawl.test: не реализовано"},
		{name: "recheck список сжимается", args: []string{"recheck", "a.crawl.test", "b.crawl.test", "c.crawl.test", "d.crawl.test"},
			code: exitError, inErr: "recheck a.crawl.test b.crawl.test c.crawl.test (+1): не реализовано"},
		{name: "recheck невалидное имя", args: []string{"recheck", "ok.crawl.test", "bad!!name"}, code: exitUsage, inErr: "некорректное имя домена"},
		{name: "recheck --help", args: []string{"recheck", "--help"}, code: exitOK, inOut: "принудительная постановка"},

		// dict — обучение словаря zstd (T-043).
		{name: "dict без действия", args: []string{"dict"}, code: exitUsage, inErr: "укажите действие"},
		{name: "dict неизвестное действие", args: []string{"dict", "fly"}, code: exitUsage, inErr: `допустимы: train`},
		{name: "dict --help", args: []string{"dict", "--help"}, code: exitOK, inOut: "словарём сжатия zstd"},
		{name: "dict train --help", args: []string{"dict", "train", "--help"}, code: exitOK, inOut: "crawlerctl dict"},
		{name: "dict train без файла", args: []string{"dict", "train"}, code: exitUsage, inErr: "укажите выходной файл"},
		{name: "dict train два позиционных", args: []string{"dict", "train", "a.zst", "b.zst"}, code: exitUsage,
			inErr: "укажите выходной файл"},
		{name: "dict train несуществующие образцы", args: []string{"dict", "train", "d.zst", "--samples", "/nonexistent"},
			code: exitError, inErr: "dict train:"},
		{name: "dict train неизвестный флаг", args: []string{"dict", "train", "--nope", "d.zst"}, code: exitUsage,
			inErr: "dict train: "},

		// dev-команды: роутинг и справка (функциональные тесты — у самих команд).
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

// TestRunDomainJSON — машинный вывод domain: json разбирается и содержит
// поля нормализации (заготовка T-081).
func TestRunDomainJSON(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"domain", "--json", "Example.CRAWL.test."}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitOK, stderr.String())
	}
	var got struct {
		ASCII   string `json:"ascii"`
		Unicode string `json:"unicode"`
		Zone    string `json:"zone"`
		Label   string `json:"label"`
		IsIDN   bool   `json:"is_idn"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("вывод не json: %v;\n%s", err, stdout.String())
	}
	if got.ASCII != "example.crawl.test" || got.Zone != "test" || got.Label != "example.crawl" {
		t.Errorf("поля нормализации: %+v", got)
	}
}

// TestUsageTextListsCommands — справка упоминает все команды таблицы
// (забытая команда — частый баг каркаса).
func TestUsageTextListsCommands(t *testing.T) {
	for name := range commands {
		if !strings.Contains(usageText, name) {
			t.Errorf("usageText не упоминает команду %q", name)
		}
	}
	for _, want := range []string{"--json", "--content", "--zone", "--bulk", "--min-score", "--samples", "list|add|disable", "list|enable|disable"} {
		if !strings.Contains(usageText, want) {
			t.Errorf("usageText не упоминает %q", want)
		}
	}
}

// TestRunHelpPerCommand — у каждой команды --help даёт код 0 и человекочитаемую
// справку (не пустую и не дефолтную flag.PrintDefaults).
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

// TestParseFlags — контракт parseFlags: явный help → ErrHelp; ошибка
// парсинга → usageErr; успех — флаги разобраны, позиционные доступны.
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

// TestNormalizeSubdomainLabel — валидация метки субдомена: punycode,
// регистр, края, длина.
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

// TestSummarizeNames — сжатие списка имён в сообщении-заглушке recheck.
func TestSummarizeNames(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{[]string{"a"}, "a"},
		{[]string{"a", "b", "c"}, "a b c"},
		{[]string{"a", "b", "c", "d"}, "a b c (+1)"},
		{[]string{"a", "b", "c", "d", "e"}, "a b c (+2)"},
	}
	for _, tc := range cases {
		if got := summarizeNames(tc.in); got != tc.want {
			t.Errorf("summarizeNames(%q) = %q, хочу %q", tc.in, got, tc.want)
		}
	}
}

// dictTrainSamplePages — детерминированные HTML-образцы для функционального
// теста команды: общий шаблон + уникальные вставки (как в internal/content).
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

// TestRunDictTrain — сквозной прогон команды: образцы → словарь в файле,
// код 0; словарь валиден и распаковывается (content.NewDict).
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
	// Не-HTML файл — игнорируется сборщиком образцов.
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
	// meta.yaml не входит: сборщик берёт только *.html/*.htm.
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
	page := dictTrainSamplePages(101)[100] // новая страница, не из обучающих
	if _, err := d.Decompress(d.Compress([]byte(page))); err != nil {
		t.Errorf("roundtrip со словарём команды: %v", err)
	}

	// Каталог без HTML — ошибка выполнения (код 1), не использования.
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

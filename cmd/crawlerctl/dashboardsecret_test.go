// Тест crawlerctl dashboard-secret (ТЗД §7.3, T-127): пароль из аргумента
// или stdin (первая строка, перевод строки отбрасывается), вывод — строка
// password_hash формата ТЗД §7.2 с параметрами генерации (600 000
// итераций, соль 16 байт, ключ 32 байта), раундтрип сворачивается
// пакетом internal/dashboard/auth (его же использует cmd/dashboard,
// T-128). Пустой пароль — ошибка использования (код 2). Полная сверка
// KDF (600 000 итераций ≈ 0.4 с) — по одному разу на источник пароля;
// разбор неверного пароля и вариации параметров — в secret_test.go пакета
// auth на быстрых итерациях.
package main

import (
	"bytes"
	"os"
	"regexp"
	"strings"
	"testing"

	"crawler/internal/dashboard/auth"
)

// hashRe — форма вывода: pbkdf2-sha256$600000$<b64 соли 16 байт>$<b64 ключа 32 байт>.
var hashRe = regexp.MustCompile(`^pbkdf2-sha256\$600000\$[A-Za-z0-9+/]{22}==\$[A-Za-z0-9+/]{43}=\n$`)

// setStdin — подмена источника stdin команды (шов dashboardsecret.go).
func setStdin(t *testing.T, r *strings.Reader) {
	t.Helper()
	old := stdin
	stdin = r
	t.Cleanup(func() { stdin = old })
}

// parseOutput — вывод команды как разобранный хэш (600 000 итераций
// заявлены генерацией, ТЗД §7.2).
func parseOutput(t *testing.T, stdout *bytes.Buffer) auth.PasswordHash {
	t.Helper()
	if !hashRe.MatchString(stdout.String()) {
		t.Fatalf("вывод не похож на password_hash (ТЗД §7.2): %q", stdout.String())
	}
	ph, err := auth.Parse(strings.TrimRight(stdout.String(), "\n"))
	if err != nil {
		t.Fatalf("Parse(вывод): %v", err)
	}
	return ph
}

// Аргумент: строка печатается и сворачивается обратно со своим паролем.
func TestRunDashboardSecretArgument(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"dashboard-secret", "s3cret-пароль"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitOK, stderr.String())
	}
	if ph := parseOutput(t, &stdout); !ph.Verify("s3cret-пароль") {
		t.Errorf("раундтрип: хэш не сверяется со своим паролем")
	}
}

// stdin: первая строка (перевод строки и хвост отбрасываются); пустой
// ввод — ошибка использования.
func TestRunDashboardSecretStdin(t *testing.T) {
	ok := []struct {
		name string
		in   string
	}{
		{"с переводом строки", "stdin-пароль\n"},
		{"без перевода строки", "stdin-пароль"},
		{"лишние строки не читаются", "stdin-пароль\nвторая-строка\n"},
		{"перевод строки CRLF", "stdin-пароль\r\n"},
	}
	for _, tt := range ok {
		t.Run(tt.name, func(t *testing.T) {
			setStdin(t, strings.NewReader(tt.in))
			var stdout, stderr bytes.Buffer
			code := run([]string{"dashboard-secret"}, &stdout, &stderr)
			if code != exitOK {
				t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitOK, stderr.String())
			}
			// Полная сверка (600 000 итераций ≈ 0.4 с) — на кейсах,
			// где поведение «первая строка» реально различимо: хвост
			// перевода строки, лишние строки, CRLF.
			ph := parseOutput(t, &stdout)
			deep := tt.name != "без перевода строки"
			if deep && !ph.Verify("stdin-пароль") {
				t.Errorf("хэш из stdin не сверяется с паролем первой строки")
			}
		})
	}

	empty := []struct {
		name string
		in   string
	}{
		{"пустой stdin (EOF)", ""},
		{"одна пустая строка", "\n"},
		{"пустая строка CRLF", "\r\n"},
	}
	for _, tt := range empty {
		t.Run(tt.name, func(t *testing.T) {
			setStdin(t, strings.NewReader(tt.in))
			var stdout, stderr bytes.Buffer
			code := run([]string{"dashboard-secret"}, &stdout, &stderr)
			if code != exitUsage {
				t.Fatalf("код = %d, хочу %d (пустой пароль — ошибка использования)", code, exitUsage)
			}
			if !strings.Contains(stderr.String(), "пустой пароль") {
				t.Errorf("stderr не объясняет пустой пароль:\n%s", stderr.String())
			}
		})
	}
}

// Аргумент важнее stdin (документированный порядок источника); пустой
// аргумент — ошибка использования и не подменяется stdin молча. Ошибка
// чтения stdin (не EOF) — ошибка выполнения (код 1).
func TestRunDashboardSecretArgumentPrecedence(t *testing.T) {
	setStdin(t, strings.NewReader("из-stdin\n"))
	var stdout, stderr bytes.Buffer
	code := run([]string{"dashboard-secret", "из-аргумента"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitOK, stderr.String())
	}
	if ph := parseOutput(t, &stdout); !ph.Verify("из-аргумента") {
		t.Errorf("аргумент обязан быть важнее stdin")
	}

	setStdin(t, strings.NewReader("непустой-stdin\n"))
	stdout, stderr = bytes.Buffer{}, bytes.Buffer{}
	code = run([]string{"dashboard-secret", ""}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("пустой аргумент: код = %d, хочу %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "пустой пароль") {
		t.Errorf("stderr:\n%s", stderr.String())
	}
}

// Справка: предупреждает про историю shell, рекомендует stdin, называет
// формат и параметры.
func TestRunDashboardSecretHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"dashboard-secret", "--help"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("код = %d, хочу %d", code, exitOK)
	}
	help := stdout.String()
	for _, want := range []string{"crawlerctl dashboard-secret", "истории shell", "600000", "pbkdf2-sha256"} {
		if !strings.Contains(help, want) {
			t.Errorf("справка не содержит %q:\n%s", want, help)
		}
	}
}

// errReader — stdin, падающий до каких-либо данных (не EOF): команда
// обязана отличать ошибку чтения (код 1) от пустого ввода (код 2).
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, os.ErrClosed }

func TestRunDashboardSecretStdinReadError(t *testing.T) {
	old := stdin
	stdin = errReader{}
	t.Cleanup(func() { stdin = old })
	var stdout, stderr bytes.Buffer
	code := run([]string{"dashboard-secret"}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("код = %d, хочу %d (ошибка чтения stdin — ошибка выполнения)", code, exitError)
	}
	if !strings.Contains(stderr.String(), "чтение пароля") {
		t.Errorf("stderr не объясняет отказ чтения:\n%s", stderr.String())
	}
}

// stdin процесса остаётся источником по умолчанию (шов вернули).
func TestStdinRestored(t *testing.T) {
	if stdin != os.Stdin {
		t.Errorf("после тестов stdin обязан вернуться к os.Stdin, получено %v", stdin)
	}
}

// Test for crawlerctl dashboard-secret (dashboard spec §7.3, T-127): the
// password from an argument or stdin (first line, newline stripped), the
// output — a password_hash string in the dashboard spec §7.2 format with
// the generation parameters (600,000 iterations, 16-byte salt, 32-byte
// key), the round trip is verified by the internal/dashboard/auth package
// (cmd/dashboard uses it too, T-128). An empty password is a usage error
// (code 2). The full KDF verification (600,000 iterations ≈ 0.4 s) runs
// once per password source; parsing a wrong password and parameter
// variations live in the auth package's secret_test.go on fast
// iterations.
package main

import (
	"bytes"
	"os"
	"regexp"
	"strings"
	"testing"

	"crawler/internal/dashboard/auth"
)

// hashRe — output shape: pbkdf2-sha256$600000$<b64 of the 16-byte salt>$<b64 of the 32-byte key>.
var hashRe = regexp.MustCompile(`^pbkdf2-sha256\$600000\$[A-Za-z0-9+/]{22}==\$[A-Za-z0-9+/]{43}=\n$`)

// setStdin — replaces the command's stdin source (the dashboardsecret.go seam).
func setStdin(t *testing.T, r *strings.Reader) {
	t.Helper()
	old := stdin
	stdin = r
	t.Cleanup(func() { stdin = old })
}

// parseOutput — the command output as a parsed hash (600,000 iterations
// declared by the generation, dashboard spec §7.2).
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

// Argument: the string is printed and verifies back against its own password.
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

// stdin: the first line (newline and the rest are stripped); empty
// input — a usage error.
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
			// Full verification (600,000 iterations ≈ 0.4 s) runs on the
			// cases where the "first line" behavior is actually
			// distinguishable: trailing newline, extra lines, CRLF.
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

// The argument takes precedence over stdin (the documented source order);
// an empty argument is a usage error and is not silently replaced by
// stdin. A stdin read error (not EOF) is an execution error (code 1).
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

// Help: warns about shell history, recommends stdin, names the format
// and parameters.
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

// errReader — stdin that fails before any data (not EOF): the command
// must distinguish a read error (code 1) from empty input (code 2).
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

// The process stdin remains the default source (the seam is restored).
func TestStdinRestored(t *testing.T) {
	if stdin != os.Stdin {
		t.Errorf("после тестов stdin обязан вернуться к os.Stdin, получено %v", stdin)
	}
}

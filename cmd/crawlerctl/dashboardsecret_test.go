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
		t.Fatalf("the output does not look like a password_hash (the dashboard spec §7.2): %q", stdout.String())
	}
	ph, err := auth.Parse(strings.TrimRight(stdout.String(), "\n"))
	if err != nil {
		t.Fatalf("Parse(output): %v", err)
	}
	return ph
}

// Argument: the string is printed and verifies back against its own password.
func TestRunDashboardSecretArgument(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"dashboard-secret", "s3cret-password"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("code = %d, want %d; stderr:\n%s", code, exitOK, stderr.String())
	}
	if ph := parseOutput(t, &stdout); !ph.Verify("s3cret-password") {
		t.Errorf("roundtrip: the hash does not verify against its own password")
	}
}

// stdin: the first line (newline and the rest are stripped); empty
// input — a usage error.
func TestRunDashboardSecretStdin(t *testing.T) {
	ok := []struct {
		name string
		in   string
	}{
		{"with a newline", "stdin-password\n"},
		{"without a newline", "stdin-password"},
		{"extra lines are not read", "stdin-password\nsecond-line\n"},
		{"a CRLF newline", "stdin-password\r\n"},
	}
	for _, tt := range ok {
		t.Run(tt.name, func(t *testing.T) {
			setStdin(t, strings.NewReader(tt.in))
			var stdout, stderr bytes.Buffer
			code := run([]string{"dashboard-secret"}, &stdout, &stderr)
			if code != exitOK {
				t.Fatalf("code = %d, want %d; stderr:\n%s", code, exitOK, stderr.String())
			}
			// Full verification (600,000 iterations ≈ 0.4 s) runs on the
			// cases where the "first line" behavior is actually
			// distinguishable: trailing newline, extra lines, CRLF.
			ph := parseOutput(t, &stdout)
			deep := tt.name != "without a newline"
			if deep && !ph.Verify("stdin-password") {
				t.Errorf("the hash from stdin does not verify against the first line's password")
			}
		})
	}

	empty := []struct {
		name string
		in   string
	}{
		{"empty stdin (EOF)", ""},
		{"one empty line", "\n"},
		{"an empty CRLF line", "\r\n"},
	}
	for _, tt := range empty {
		t.Run(tt.name, func(t *testing.T) {
			setStdin(t, strings.NewReader(tt.in))
			var stdout, stderr bytes.Buffer
			code := run([]string{"dashboard-secret"}, &stdout, &stderr)
			if code != exitUsage {
				t.Fatalf("code = %d, want %d (an empty password — a usage error)", code, exitUsage)
			}
			if !strings.Contains(stderr.String(), "empty password") {
				t.Errorf("stderr does not explain the empty password:\n%s", stderr.String())
			}
		})
	}
}

// The argument takes precedence over stdin (the documented source order);
// an empty argument is a usage error and is not silently replaced by
// stdin. A stdin read error (not EOF) is an execution error (code 1).
func TestRunDashboardSecretArgumentPrecedence(t *testing.T) {
	setStdin(t, strings.NewReader("from-stdin\n"))
	var stdout, stderr bytes.Buffer
	code := run([]string{"dashboard-secret", "from-argument"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("code = %d, want %d; stderr:\n%s", code, exitOK, stderr.String())
	}
	if ph := parseOutput(t, &stdout); !ph.Verify("from-argument") {
		t.Errorf("the argument must take precedence over stdin")
	}

	setStdin(t, strings.NewReader("nonempty-stdin\n"))
	stdout, stderr = bytes.Buffer{}, bytes.Buffer{}
	code = run([]string{"dashboard-secret", ""}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("an empty argument: code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "empty password") {
		t.Errorf("stderr:\n%s", stderr.String())
	}
}

// Help: warns about shell history, recommends stdin, names the format
// and parameters.
func TestRunDashboardSecretHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"dashboard-secret", "--help"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("code = %d, want %d", code, exitOK)
	}
	help := stdout.String()
	for _, want := range []string{"crawlerctl dashboard-secret", "shell history", "600000", "pbkdf2-sha256"} {
		if !strings.Contains(help, want) {
			t.Errorf("the help does not contain %q:\n%s", want, help)
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
		t.Fatalf("code = %d, want %d (a stdin reading error — an execution error)", code, exitError)
	}
	if !strings.Contains(stderr.String(), "reading the password") {
		t.Errorf("stderr does not explain the reading refusal:\n%s", stderr.String())
	}
}

// The process stdin remains the default source (the seam is restored).
func TestStdinRestored(t *testing.T) {
	if stdin != os.Stdin {
		t.Errorf("after the tests stdin must be back to os.Stdin, got %v", stdin)
	}
}

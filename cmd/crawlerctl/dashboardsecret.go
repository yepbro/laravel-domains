// ---------------------------------------------------------------- dashboard-secret
//
// Command for generating the dashboard operator password hash
// (dashboard spec §7.3, T-127): prints a ready-made
// dashboard.password_hash string in the dashboard spec §7.2 format.
// KDF, parsing and format verification live in the
// internal/dashboard/auth package (cmd/dashboard uses it too, T-128).
// The command runs without infrastructure (no DB/Redis needed).

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"crawler/internal/dashboard/auth"
)

// dashboardSecretHelp — help text for crawlerctl dashboard-secret --help.
const dashboardSecretHelp = `crawlerctl dashboard-secret — generating the dashboard password hash

Usage:
  crawlerctl dashboard-secret [PASSWORD]

Prints the string for dashboard.password_hash of the operator dashboard
(the dashboard spec §7.2):

  pbkdf2-sha256$<iterations>$<salt base64>$<key base64>

PBKDF2-HMAC-SHA256, 600000 iterations (OWASP), a 16-byte salt, a 32-byte
key; the parsing and verification — cmd/dashboard (T-128).

The password — the only positional argument or stdin. stdin is recommended:
an argument stays in the shell history and is visible in the process list.

  crawlerctl dashboard-secret < password.txt
  printf 'password' | crawlerctl dashboard-secret

The first line is read from stdin, the newline is discarded. An empty
password — a usage error (exit code 2).
`

// stdin — the command's password source (test seam; in-process — os.Stdin).
var stdin io.Reader = os.Stdin

func cmdDashboardSecret(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("dashboard-secret")
	if handled, err := parseFlags(fs, dashboardSecretHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 1 {
		return usagef("dashboard-secret: extra arguments — the password is given by one argument or stdin")
	}
	var password string
	if fs.NArg() == 1 {
		password = fs.Arg(0)
	} else {
		line, err := bufio.NewReader(stdin).ReadString('\n')
		// EOF is not an error: a piped password may lack a trailing
		// newline; empty input (EOF right away) becomes an empty
		// password below.
		if err != nil && err != io.EOF {
			return fmt.Errorf("dashboard-secret: reading the password from stdin: %w", err)
		}
		password = strings.TrimRight(line, "\r\n")
	}
	if password == "" {
		return usagef("dashboard-secret: empty password — supply the string via stdin or as an argument")
	}
	hash, err := auth.Generate(password, 0)
	if err != nil {
		return err
	}
	out(stdout, "%s\n", hash)
	return nil
}

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
const dashboardSecretHelp = `crawlerctl dashboard-secret — генерация хэша пароля дашборда

Использование:
  crawlerctl dashboard-secret [ПАРОЛЬ]

Печатает строку для dashboard.password_hash операторского дашборда
(ТЗД §7.2):

  pbkdf2-sha256$<итерации>$<соль base64>$<ключ base64>

PBKDF2-HMAC-SHA256, 600000 итераций (OWASP), соль 16 байт, ключ 32 байта;
разбор и сверка — cmd/dashboard (T-128).

Пароль — единственный позиционный аргумент или stdin. Рекомендуется stdin:
аргумент остаётся в истории shell и виден в списке процессов.

  crawlerctl dashboard-secret < password.txt
  printf 'пароль' | crawlerctl dashboard-secret

Из stdin читается первая строка, перевод строки отбрасывается. Пустой
пароль — ошибка использования (код 2).
`

// stdin — the command's password source (test seam; in-process — os.Stdin).
var stdin io.Reader = os.Stdin

func cmdDashboardSecret(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("dashboard-secret")
	if handled, err := parseFlags(fs, dashboardSecretHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 1 {
		return usagef("dashboard-secret: лишние аргументы — пароль задаётся одним аргументом или stdin")
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
			return fmt.Errorf("dashboard-secret: чтение пароля из stdin: %w", err)
		}
		password = strings.TrimRight(line, "\r\n")
	}
	if password == "" {
		return usagef("dashboard-secret: пустой пароль — подайте строку в stdin или аргументом")
	}
	hash, err := auth.Generate(password, 0)
	if err != nil {
		return err
	}
	out(stdout, "%s\n", hash)
	return nil
}

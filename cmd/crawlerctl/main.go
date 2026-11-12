// crawlerctl — the Domain Crawler management CLI (spec 4.1 of the main
// spec).
//
// Management commands (status, domain, subdomains, zones, reanalyze,
// recheck): status/domain/reanalyze work against the dev stack
// (PostgreSQL, Redis, the content store), the rest are skeletons: the
// commands parse arguments and flags, validate the input (for domain and
// zone names — the internal/idn package, the same pipeline as for
// incoming rows) and report which task will deliver the functionality.
// The dev environment commands seed and test-rules are fully
// implemented.
//
// Exit codes: 0 — success, including --help; 1 — an execution error,
// including "not implemented" (the calling script must not count the
// action as done); 2 — a usage error (bad arguments, flags or command
// input).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"unicode/utf8"
)

// The crawlerctl exit codes (the full contract — in the package comment).
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// commandFunc — a command handler: the arguments after the command name
// and the output streams. The error contract: usageErr → code 2,
// flag.ErrHelp (the help has been printed) → code 0, nil → code 0,
// anything else → code 1.
type commandFunc func(args []string, stdout, stderr io.Writer) error

// commands — the command table: routing in run and the list in usageText.
var commands = map[string]commandFunc{
	"status":           cmdStatus,
	"domain":           cmdDomain,
	"subdomains":       cmdSubdomains,
	"zones":            cmdZones,
	"reanalyze":        cmdReanalyze,
	"recheck":          cmdRecheck,
	"dict":             cmdDict,
	"dashboard-secret": cmdDashboardSecret,
	"seed":             cmdSeed,
	"test-rules":       cmdTestRules,
	"near":             cmdNear,
	"check":            cmdCheck,
	"counters":         cmdCounters,
	"fixtures":         cmdFixtures,
}

// usageText — the top-level help: no arguments, help, an unknown
// command. Commands to be delivered by future tasks are marked with
// their numbers.
const usageText = `crawlerctl — CLI Domain Crawler

Команды управления (основное ТЗ, 4.1):
  status                        сводка по зонам, очередям, скорости (T-079)
  domain <имя> [--json] [--content]
                                информация о домене: статус, признаки
                                Laravel, контент (ТЗ 5.11)
  subdomains list|add|disable|recheck
                                 управление проверяемыми субдоменами
                                 (T-083)
  zones list|enable|disable|sync|collapse
                                  управление зонами (T-084), наполнение
                                  справочника из реестра IANA (T-125),
                                  сворачивание 2LD-зон в TLD (T-196)
  reanalyze [--zone ИМЯ]        переанализ результатов по новым правилам (T-074)
  recheck <домен> [домен …]     принудительная постановка домена на
                                проверку (T-084)
  dict train <файл> [--samples ПУТЬ]…
                                обучение/переобучение словаря сжатия zstd
                                по HTML-образцам (ТЗ 5.7)
  dashboard-secret [ПАРОЛЬ]     хэш пароля операторского дашборда
                                (dashboard.password_hash, дашборд ТЗД);
                                пароль — аргумент или stdin

Команды разбора:
  near [--zone ИМЯ] [--min-score F]
                                 находки Laravel в серой зоне порога —
                                 материал калибровки правил (T-115)
  check <домен>                  живая проверка домена конвейером:
                                 DNS → HTTP (+рендер SPA) → анализ,
                                 read-only (T-119)

Команды счётчиков (T-178):
  counters list|recount|verify
                                 инкрементальные счётчики конвейера:
                                 значения, ручная реконсиляция (--zone),
                                 сверка с живыми агрегатами (код 0/1)

Команды dev-окружения:
  seed [--incoming DIR] [--scenarios FILE] [--bulk N]
                                тестовые списки доменов в incoming
  test-rules [--rules DIR] [--samples DIR] [--min-score F]
                                прогон правил Laravel по корпусу образцов
  fixtures export --out КАТАЛОГ [--ok N] [--failed N] [--heavy N]
                                 [--smoke N] [--weeks N] [--seed N]
                                 выгрузка фикстуры рендера (zones/domains/
                                 subdomain_results без контента) для
                                 стенда движков (ТЗ рендера §6.1, T-242)
  fixtures import --dir КАТАЛОГ [--force] [--publish all|smoke]
                                 накат дампа фикстуры в тестовую БД
                                 (идемпотентно по PK; без --force —
                                 dry-run: цель из DSN и план без записи)
                                 и публикация задач
                                 прямо в q:render без DNS/HTTP (ТЗ
                                 рендера §6.1, T-243)
  fixtures snapshot --dir КАТАЛОГ --label ИМЯ --engine ИМЯ
                                  фиксация результатов прогона стенда
                                  (строки БД по фикстуре, темп по q:analyze,
                                  метрики этапа) в JSON-снимок (T-244)
  fixtures report --baseline ФАЙЛ --candidate ФАЙЛ
                                  отчёт сравнения движков по ТЗ §5.1:
                                  метрики п.3 и явные вердикты против
                                  порогов п.4 (T-244)

Справка по команде: crawlerctl <команда> --help
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run — command routing; returns the exit code. Extracted from main for
// tests: codes and stdout/stderr output are checked without launching a
// process.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		out(stderr, "%s", usageText)
		return exitUsage
	}
	switch args[0] {
	case "help", "-h", "--help":
		out(stdout, "%s", usageText)
		return exitOK
	}
	cmd, ok := commands[args[0]]
	if !ok {
		out(stderr, "crawlerctl: неизвестная команда %q\n\n%s", args[0], usageText)
		return exitUsage
	}
	err := cmd(args[1:], stdout, stderr)
	switch {
	case err == nil:
		return exitOK
	case errors.Is(err, flag.ErrHelp):
		return exitOK // the help was already printed by the command
	case isUsageErr(err):
		out(stderr, "crawlerctl: %v\n", err)
		return exitUsage
	default:
		out(stderr, "crawlerctl: %v\n", err)
		return exitError
	}
}

// ---------------------------------------------------------------- errors

// usageErr — a usage error: bad arguments, flags or command input. run
// returns code 2 for it (as well as for the no-argument help).
type usageErr struct{ err error }

func (e *usageErr) Error() string { return e.err.Error() }
func (e *usageErr) Unwrap() error { return e.err }

func isUsageErr(err error) bool {
	var u *usageErr
	return errors.As(err, &u)
}

// usagef — a usage error with formatting.
func usagef(format string, a ...any) error {
	return &usageErr{fmt.Errorf(format, a...)}
}

// ---------------------------------------------------------------- output

// out — printing into the command's stream. An output error is not
// checked deliberately: the stream is the process's os.Stdout/os.Stderr,
// there is no reaction to a write failure (an explicit ignore — instead
// of suppressing errcheck across the whole package).
func out(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...)
}

// cellWidth — the width of a monospace table cell in runes: fmt pads %s
// by runes, while Cyrillic headers and values (the subdomain header, the
// apex label, the em dash) are wider in bytes — a len()-based width
// would inflate the columns by the bytes−runes difference. Full-width
// CJK characters are wider than one cell — the CLI output has none of
// them.
func cellWidth(s string) int { return utf8.RuneCountInString(s) }

// signalCtx — the cancellable context of a one-off CLI: Ctrl-C/SIGTERM
// interrupt the command — active DB queries finish via context
// cancellation, not a signal-caused connection tear-down. The single
// helper of all commands (it first appeared at reanalyze/recheck).
func signalCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// ---------------------------------------------------------------- flags

// newFlagSet — a FlagSet in the single crawlerctl style: parsing errors
// are returned instead of terminating the process and write nothing
// extra to the output (we print the help ourselves — only on an explicit
// request, see parseFlags).
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// parseFlags parses the command's flags; help is its human-readable
// help text. An explicit help request prints it and returns
// (true, flag.ErrHelp) — run turns that into code 0. The -h/--help
// flags are only recognized where the flag package itself would count
// them as flags (flagHelpRequested): a flag's value token (--samples
// --help — the path "--help") and everything after the "--" terminator
// or the first positional argument is not a help request but an ordinary
// command argument. A bare help word is only the command's first
// argument; otherwise it is a positional argument (crawlerctl domain
// --json help — a domain name). A parsing error — (true, usageErr),
// i.e. code 2.
func parseFlags(fs *flag.FlagSet, help string, args []string, stdout io.Writer) (bool, error) {
	if len(args) > 0 && args[0] == "help" {
		out(stdout, "%s", help)
		return true, flag.ErrHelp
	}
	if flagHelpRequested(fs, args) {
		out(stdout, "%s", help)
		return true, flag.ErrHelp
	}
	if err := fs.Parse(args); err != nil {
		return true, usagef("%s: %v", fs.Name(), err)
	}
	return false, nil
}

// flagHelpRequested — whether -h/--help appears in the arguments as a
// flag. The scan repeats the flag package's rules: at the first
// positional argument (and at the "--" terminator) flag parsing ends;
// after a flag that requires a separate value (non-boolean, without
// "=") the next token is the value, even if it looks like --help
// (crawlerctl seed --incoming --help — the directory "--help", not
// help).
func flagHelpRequested(fs *flag.FlagSet, args []string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" || len(a) < 2 || a[0] != '-' {
			return false // a terminator/positional: no flags beyond
		}
		if a == "-h" || a == "--help" {
			return true
		}
		if flagTakesValue(fs, a) {
			i++ // the next token is the flag's value
		}
	}
	return false
}

// flagTakesValue — whether the flag token requires a separate value:
// boolean flags (flag.Bool) take none, the rest (--zone, --samples,
// including fs.Func) do; the -name=value form is self-sufficient.
func flagTakesValue(fs *flag.FlagSet, a string) bool {
	if strings.Contains(a, "=") {
		return false
	}
	f := fs.Lookup(strings.TrimLeft(a, "-"))
	if f == nil {
		return false // an unknown flag — a parsing error in fs.Parse
	}
	type boolFlag interface{ IsBoolFlag() bool }
	bf, ok := f.Value.(boolFlag)
	return !ok || !bf.IsBoolFlag()
}

// flagExplicit — whether the flag was set explicitly on the command line
// (fs.Visit walks only the flags parsed by Parse). It tells "passed"
// from "default" — a default can be complemented with a configuration
// source (test-rules).
func flagExplicit(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

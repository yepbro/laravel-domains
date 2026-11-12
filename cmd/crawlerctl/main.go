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

Management commands (the main spec, 4.1):
  status                        a summary over zones, queues, rates (T-079)
  domain <name> [--json] [--content]
                                the domain info: the status, the Laravel
                                findings, the content (spec 5.11)
  subdomains list|add|disable|recheck
                                 management of the checked subdomains
                                 (T-083)
  zones list|enable|disable|sync|collapse
                                  zone management (T-084), populating
                                  the catalog from the IANA registry (T-125),
                                  collapsing 2LD zones into TLD (T-196)
  reanalyze [--zone NAME]        re-analysis of the results under the new rules (T-074)
  recheck <domain> [domain …]    force-scheduling a domain
                                for a check (T-084)
  dict train <file> [--samples PATH]…
                                training/retraining the zstd compression
                                dictionary over HTML samples (spec 5.7)
  dashboard-secret [PASSWORD]   the operator dashboard's password hash
                                (dashboard.password_hash, the dashboard spec);
                                the password — an argument or stdin

Triage commands:
  near [--zone NAME] [--min-score F]
                                 Laravel finds in the threshold gray zone —
                                 rule calibration material (T-115)
  check <domain>                 a live domain check by the pipeline:
                                 DNS → HTTP (+SPA render) → analysis,
                                 read-only (T-119)

Counter commands (T-178):
  counters list|recount|verify
                                 the pipeline's incremental counters:
                                 values, a manual reconciliation (--zone),
                                 a cross-check against live aggregates (code 0/1)

Dev-environment commands:
  seed [--incoming DIR] [--scenarios FILE] [--bulk N]
                                test domain lists into incoming
  test-rules [--rules DIR] [--samples DIR] [--min-score F]
                                a run of the Laravel rules over the sample corpus
  fixtures export --out DIR [--ok N] [--failed N] [--heavy N]
                                 [--smoke N] [--weeks N] [--seed N]
                                 the render fixture export (zones/domains/
                                 subdomain_results without content) for
                                 the engine bench (the render spec §6.1, T-242)
  fixtures import --dir DIR [--force] [--publish all|smoke]
                                  the fixture dump import into a test DB
                                  (idempotent by PK; without --force —
                                  dry-run: the target from the DSN and the
                                  plan without writes)
                                  and publishing the tasks
                                  straight into q:render without DNS/HTTP (the
                                  render spec §6.1, T-243)
  fixtures snapshot --dir DIR --label NAME --engine NAME
                                   capturing the bench run results
                                   (the DB rows per fixture, the pace over q:analyze,
                                   the stage metrics) into a JSON snapshot (T-244)
  fixtures report --baseline FILE --candidate FILE
                                   the engine comparison report per spec §5.1:
                                   the item 3 metrics and explicit verdicts against
                                   the item 4 thresholds (T-244)

Command help: crawlerctl <command> --help
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
		out(stderr, "crawlerctl: unknown command %q\n\n%s", args[0], usageText)
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

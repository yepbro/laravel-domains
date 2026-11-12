// ---------------------------------------------------------------- check
//
// Live check of a single domain outside the scheduler loop (T-119): the
// pipeline lives in the library package internal/checker (T-123 — shared
// by the CLI and the landing form), the command prints the result.
// Read-only with respect to the DB and queues; UA/timeouts come from the
// configuration; no active probes (ethics §5.8/§9).

package main

import (
	"fmt"
	"io"

	"crawler/internal/checker"
	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/idn"
)

// checkHelp — help text for crawlerctl check --help.
const checkHelp = `crawlerctl check — a live domain check by the pipeline (T-119)

Usage:
  crawlerctl check <domain>

The full cycle for one name: DNS (the subdomains catalog from the DB) →
HTTP of both schemes of the DNS-alive subdomains (an SPA page render with
render.remote_url configured) → passive analysis with the Laravel rules.
The per-subdomain output: the status, the redirect chain, the TLS summary,
the fired rules with evidence, the score, the version range.

Read-only: nothing is written into the DB and nothing is published into
the queues — scheduling a domain for the regular check is a separate
command (crawlerctl recheck). The User-Agent, timeouts and limits come
from the configuration (the dns/http/render sections); the analysis — the
current analyzer.rules_dir rule set and the analyzer.laravel_min_score
threshold. No active probes: only GET / of the alive subdomains (the
passive ethics, spec §5.8/§9).
Exit codes: 0 — the check was performed, 1 — error, 2 — usage.
`

func cmdCheck(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("check")
	if handled, err := parseFlags(fs, checkHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("specify exactly one domain: crawlerctl check example.com")
	}
	if _, err := idn.Normalize(fs.Arg(0)); err != nil {
		return usagef("check: invalid domain name %q: %v", fs.Arg(0), err)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	ctx, stop := signalCtx()
	defer stop()

	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // connection budget per spec 6.3 — same as the services
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	defer pool.Close()

	res, err := checker.Run(ctx, cfg, pool, fs.Arg(0), checkLog{out: stderr})
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	checkPrintResult(stdout, res, cfg.Analyzer.LaravelMinScore, cfg.Analyzer.RulesDir)
	return nil
}

// checkLog — minimal logger for the checker package, writing to the command's stderr.
type checkLog struct{ out io.Writer }

func (l checkLog) Warn(msg string, args ...any) {
	out(l.out, "check: %s", msg)
	for i := 0; i+1 < len(args); i += 2 {
		out(l.out, " %v=%v", args[i], args[i+1])
	}
	out(l.out, "\n")
}
func (l checkLog) Info(msg string, args ...any) {}

// checkPrintResult — prints the check result.
func checkPrintResult(w io.Writer, res *checker.Result, minScore float64, rulesDir string) {
	out(w, "check: %s (zone %s", res.Domain, res.Zone)
	if res.Unicode != res.Domain {
		out(w, ", unicode %s", res.Unicode)
	}
	out(w, ") — rules %s, threshold %.2f, DNS %s\n\n", rulesDir, minScore, res.Apex)

	if res.Terminal != "" {
		out(w, "result: the DNS stage terminal status — %s (no addresses)\n", res.Terminal)
		return
	}

	for i := range res.Records {
		r := &res.Records[i]
		label := r.Label
		if label == "" {
			label = "(apex)"
		}
		out(w, "%s [%s] record %s/%s: %s %s\n", r.Host, label, r.Variant, r.Scheme, r.Status, r.FinalURL)
		if len(r.Redirects) > 1 {
			out(w, "    redirects:")
			for j, step := range r.Redirects {
				if j == len(r.Redirects)-1 {
					break
				}
				out(w, " %d→%s", step.Status, step.URL)
			}
			out(w, "\n")
		}
		if r.HasTLS {
			verdict := "valid"
			if !r.TLSValid {
				verdict = "PROBLEMS"
			}
			out(w, "    TLS: %s, expires %s\n", verdict, r.TLSNotAfter.Format("2006-01-02"))
		}
		src := "raw HTML"
		if r.Rendered {
			src = "DOM after render"
		}
		out(w, "    laravel: score=%.3f ver=[%s..%s] rules=%d (%s)\n",
			r.Score, orDash(r.VerMin), orDash(r.VerMax), len(r.Signals), src)
		for _, ev := range r.Signals {
			out(w, "      - %-28s w=%.2f  \"%s\"\n", ev.Rule, ev.Weight, ev.Evidence)
		}
	}
	out(w, "\ncheck: %s — final score=%.3f found=%v, DNS %d ms, total %d ms (nothing written)\n",
		res.Domain, res.Score, res.Found, res.DNSMS, res.TotalMS)
}

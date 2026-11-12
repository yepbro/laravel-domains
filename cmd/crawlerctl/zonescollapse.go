// ---------------------------------------------------------------- zones collapse
//
// crawlerctl zones collapse (T-196, the tld-only-zones spec §7): moves
// 2LD zone data into TLD zones — maintenance of the downtime window after
// the switch to TLD zone semantics (internal/idn, T-194). A dry run is
// the default: inventory and plan without changes; --apply performs the
// move in batches (§7.3). The logic lives in internal/collapse (reused by
// integration tests against the live schema); this file holds the flags,
// the configuration (the DSN and the ingest.allowed_zone_paths allowlist —
// its zones are not collapsed) and the pool.

package main

import (
	"fmt"
	"io"
	"log/slog"

	"crawler/internal/collapse"
	"crawler/internal/config"
	"crawler/internal/db"
)

// zonesCollapse — crawlerctl zones collapse: dry-run by default,
// --apply/--skip-reconcile/--force (§7.1). The pool is as in the
// neighboring management commands (openManagePool, but the config is
// also needed for the allowlist — read here, counters.go style).
func zonesCollapse(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones collapse")
	apply := fs.Bool("apply", false, "perform the move (without the flag — dry-run: inventory and plan)")
	skipReconcile := fs.Bool("skip-reconcile", false, "skip the counter reconciliation after the move (only with --apply)")
	force := fs.Bool("force", false, "ignore the domains in the queue (check_state = 1) in the affected zones")
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("zones collapse: extra arguments %v (the inventory selects the zones, the command takes no positional arguments)", fs.Args())
	}
	if *skipReconcile && !*apply {
		return usagef("zones collapse: --skip-reconcile complements --apply (dry-run moves and reconciles nothing)")
	}

	// A one-off CLI command: Ctrl-C/SIGTERM interrupt the move — completed
	// batches stay, a repeated command run continues from where it stopped.
	ctx, stop := signalCtx()
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("zones collapse: %w", err)
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // connection budget of spec 6.3 — as the services
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("zones collapse: %w", err)
	}
	defer pool.Close()

	opts := collapse.Options{
		Allowlist:     cfg.Ingest.AllowedZonePaths, // normalization — internal/config (T-195)
		Apply:         *apply,
		Force:         *force,
		SkipReconcile: *skipReconcile,
		// The warn about a failed final moving_ids cleanup (T-229) goes to
		// stderr, manage.go style (stdout carries only the command report).
		Log: slog.New(slog.NewTextHandler(stderr, nil)),
	}
	if err := collapse.Run(ctx, pool, opts, stdout); err != nil {
		return fmt.Errorf("zones collapse: %w", err)
	}
	return nil
}

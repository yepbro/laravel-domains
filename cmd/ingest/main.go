// The ingest service (spec 5.1): watches the incoming/ directory
// (internal/watch), runs the ready files through the ingest pipeline
// (internal/ingest: path parsing → zone → file parsing with sha256 in a
// single pass → validation → staging → applying add/delete → the
// ingest_files report → processed/failed) and keeps background cleanups:
// deleted domains and orphaned staging batches (CleanupDeleted,
// T-020/T-098).
// The framework is internal/service: migrations, the DB pool (OnDBPool),
// the /livez + /healthz probes and /metrics, graceful shutdown (spec 3.4).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/ingest"
	"crawler/internal/metrics"
	"crawler/internal/service"
	"crawler/internal/watch"
)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[ingest] configuration: %v", err)
	}
	logger := service.NewLogger("ingest", cfg)

	// The metrics registry — right here (T-077): the junk content_hash
	// counter of the cleanup (T-099) receives it before OnRun starts, the
	// framework serves the same registry via /metrics.
	reg := metrics.New("ingest")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "ingest",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9101"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — the working loop of ingest (spec 5.1): watching incoming
// (fsnotify + a safety-net crawl, the size/mtime stability —
// internal/watch, T-013), the processing pipeline of each ready file
// (internal/ingest, T-014…T-021) and the background cleanup of deleted
// domains in a separate goroutine (T-020; the interval and the chunk are
// the package defaults; the pass totals go into the junk content_hash
// counter, T-099). Shutdown — ctx cancellation (graceful shutdown,
// spec 3.4): the channel of ready files is closed, the pipeline finishes
// the current file, the cleanup exits on the cancellation.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("ingest: the DB pool is not provided (postgres.dsn is required)")
	}

	pipe, err := ingest.NewPipeline(
		ingest.NewZoneStore(pool),
		ingest.NewFileStore(pool),
		pool,
		ingest.PipelineOptions{
			Dir:           cfg.Ingest.Dir,
			BatchSize:     cfg.Ingest.BatchSize,
			MaxErrorRatio: cfg.Ingest.MaxErrorRatio,
			// Archive limits (T-133): the uncompressed size and the record count.
			ArchiveMaxUncompressed: cfg.Ingest.ArchiveMaxUncompressed.B(),
			ArchiveMaxFiles:        cfg.Ingest.ArchiveMaxFiles,
			// The allowlist of the multi-level zone directories
			// (the tld-only-zones spec §5.1): the test zones of the
			// testbed; the production value is empty.
			AllowedZonePaths: cfg.Ingest.AllowedZonePaths,
			// The rejected log goes to the service's common stderr
			// (spec 5.10 does not set paths): the records are
			// distinguishable by the service=rejected attribute (T-016).
			RejectLog: ingest.NewRejectedLogger(os.Stderr, slog.LevelInfo),
			Log:       logger,
		},
	)
	if err != nil {
		return err
	}

	files, err := watch.Dir(ctx, watch.Options{
		Dir:          cfg.Ingest.Dir,
		StableAfter:  time.Duration(cfg.Ingest.StableSeconds) * time.Second,
		ScanInterval: cfg.Ingest.ScanInterval.D(),
		Log:          logger,
	})
	if err != nil {
		return err
	}

	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- ingest.CleanupDeleted(ctx, pool, ingest.CleanupOptions{
			Log: logger,
			// The pass total — into the junk content_hash counter (T-099):
			// a zero BadHashes does not change the value, a nil registry — no-op.
			OnPass: ingest.NewCleanupMetrics(reg).Observe,
		})
	}()

	for f := range files {
		// The processing totals (the processed/failed moves, duplicates,
		// the error threshold) are logged by the Pipeline itself; a return
		// error means only the files left in incoming because of an
		// infrastructure failure: the loop continues with the next files,
		// the rest will be reprocessed by a restart (at-least-once,
		// spec 3.4).
		if _, err := pipe.Process(ctx, f.Path); err != nil {
			logger.Error("file not processed — stays in incoming",
				"path", f.Path, "err", err)
		}
	}
	// The channel is closed (the ctx cancellation) — the pipeline is
	// finished; we wait for the cleanup (it exits on the same
	// cancellation).
	return <-cleanupDone
}

// The end-to-end pipeline for processing one input file (T-021;
// spec 5.1): an internal/watch event → ParsePath (T-014) → EnsureZone
// (T-014) → one pass over the file: streaming parsing (T-015) and the
// content sha256 via io.TeeReader — exactly one read, there is no
// second pass over the file (spec 3.5) → line validation with the
// rejected log (T-016) → loading into staging (T-017) → applying
// add/delete (T-018/T-019) → the report into ingest_files (report.go)
// → moving the file to processed/ or failed/. Archives
// (.zip/.tar/.tar.gz/.tgz/.gz) branch by extension into streaming
// unpacking — archive.go (T-133): members go through the same
// Parse/Validator/Stage, the unit of accounting is the archive.
//
// Idempotency (spec 3.4, 5.1; T-113): protection against reprocessing
// — exact redelivery, a match of (zone, action, sha256) in
// ingest_files; the pipeline checks the hash after the staging load
// (the hash is only known by the end of reading) and before applying.
// The outcome classes and the "does the file stay or go to the
// archive" decision — Outcome.
package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"crawler/internal/model"
)

// DefaultBatchSize — the default COPY chunk size of the staging load
// (ingest.batch_size, spec 5.10). Duplicates config.Default so the
// pipeline works without configuration too (the DefaultMaxErrorRatio
// style, T-016).
const DefaultBatchSize = 50000

// Ingest archive directories — the top level of incoming (spec 5.1):
// processed/ — successfully processed files, failed/ — files with an
// error and a log report nearby. internal/watch does not watch them
// (T-013).
const (
	DirProcessed = "processed"
	DirFailed    = "failed"
)

// stageChanSize — the buffer of the line channel between file parsing
// and the staging load: the parser does not wait for the DB on every
// line, and memory is bounded (spec 3.5).
const stageChanSize = 1024

// errStopped — the line producer was stopped by the consumer (the load
// was interrupted by a DB error): the parser finishes early.
var errStopped = errors.New("ingest: the batch load was stopped")

// PipelineOptions — pipeline parameters; zero values are replaced by
// defaults (the watch.Options style, T-013).
type PipelineOptions struct {
	// Dir — the watched incoming directory (ingest.dir): files arrive
	// with paths inside it, and the processed/failed archive is placed
	// there too (spec 5.1).
	Dir string
	// BatchSize — rows per COPY chunk into staging (ingest.batch_size).
	BatchSize int
	// MaxErrorRatio — the threshold for the share of rejected file lines
	// (ingest.max_error_ratio); 0 — DefaultMaxErrorRatio. For archives
	// it acts on a member (T-133): a member above the threshold fails
	// the archive as a whole.
	MaxErrorRatio float64
	// ArchiveMaxUncompressed — the cap on the total uncompressed size of
	// archive members (ingest.archive_max_uncompressed, T-133); 0 — the default.
	ArchiveMaxUncompressed int64
	// ArchiveMaxFiles — the cap on the number of archive entries
	// (ingest.archive_max_files, T-133); 0 — the default.
	ArchiveMaxFiles int
	// AllowedZonePaths — multi-level zones allowed to be incoming
	// directories (ingest.allowed_zone_paths, the tld-only-zones spec
	// §5.1): testbed test zones (crawl.test); nil/empty — working
	// directories are TLD-only. A file of a forbidden multi-level
	// directory is a file-level error (failed/, T-021).
	AllowedZonePaths []string
	// RejectLog — the rejected logger (NewRejectedLogger, T-016); nil —
	// records suppressed (counters are kept anyway).
	RejectLog *slog.Logger
	// Log — the service logger for processing outcomes; nil — suppressed.
	Log *slog.Logger
	// Now — the time source for processed_at (a stub in tests);
	// nil — time.Now.
	Now func() time.Time
}

// Pipeline — end-to-end processing of a ready input file (spec 5.1)
// from the ready stages T-014…T-020. It keeps no state of its own
// between files: idempotency — via the (zone, action, sha256) triple
// in ingest_files, not in memory.
type Pipeline struct {
	zones ZoneStore
	files FileStore
	pool  ApplyDB
	opts  PipelineOptions
	log   *slog.Logger
	now   func() time.Time
}

// NewPipeline — a pipeline over the stores: ZoneStore (T-014),
// FileStore (the ingest_files log, report.go) and an ApplyDB pool for
// staging and applying (the add apply is in one transaction, T-205).
func NewPipeline(zones ZoneStore, files FileStore, pool ApplyDB, opts PipelineOptions) (*Pipeline, error) {
	if zones == nil || files == nil || pool == nil {
		return nil, errors.New("ingest: pipeline: ZoneStore, FileStore and a DB pool are required")
	}
	if opts.Dir == "" {
		return nil, errors.New("ingest: pipeline: the incoming directory is not set (PipelineOptions.Dir)")
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBatchSize
	}
	if opts.MaxErrorRatio == 0 {
		opts.MaxErrorRatio = DefaultMaxErrorRatio
	}
	if opts.ArchiveMaxUncompressed <= 0 {
		opts.ArchiveMaxUncompressed = DefaultArchiveMaxUncompressed
	}
	if opts.ArchiveMaxFiles <= 0 {
		opts.ArchiveMaxFiles = DefaultArchiveMaxFiles
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Pipeline{zones: zones, files: files, pool: pool, opts: opts, log: log, now: now}, nil
}

// OutcomeKind — how the file processing ended.
type OutcomeKind string

const (
	// OutcomeApplied — the file was applied to the DB, the report was
	// written to ingest_files, the file was moved to processed/.
	OutcomeApplied OutcomeKind = "applied"
	// OutcomeDuplicate — exact redelivery: the same (zone, action,
	// sha256) triple is already in the ingest_files log (spec 5.1,
	// T-113): the load was discarded, the file was moved to processed/
	// without a new report.
	OutcomeDuplicate OutcomeKind = "duplicate"
	// OutcomeFailed — the file is invalid in itself (the path structure,
	// the zone name, an error share above the threshold): moved to
	// failed/ with the log report nearby; no report is written to
	// ingest_files — the same hash does not occupy the log, a corrected
	// file can be loaded again.
	OutcomeFailed OutcomeKind = "failed"
	// OutcomeLeft — an infrastructure failure (the DB, file reading):
	// the file stays in incoming and will be reprocessed after a
	// service restart or a file change (at-least-once delivery,
	// spec 3.4); processing of the following files continues.
	OutcomeLeft OutcomeKind = "left"
)

// Outcome — the outcome of file processing: the class, the counters
// (filled in as far as they arrived) and the reason (for failed/left
// — into the log).
type Outcome struct {
	Kind   OutcomeKind
	Report FileReport
	Err    error
}

// Process processes a ready file (an internal/watch event, T-013): the
// full spec 5.1 pipeline and moving the file by the outcome. The
// returned error is not nil only for OutcomeLeft (an infrastructure
// failure — the file stayed in incoming): that is not a "file"
// problem, the caller sees it; applied/duplicate/failed are planned
// outcomes, the returned error is nil, the failed reason is in
// Outcome.Err.
func (p *Pipeline) Process(ctx context.Context, abs string) (Outcome, error) {
	o := p.process(ctx, abs)
	if o.Kind == OutcomeLeft {
		return o, o.Err
	}
	return o, nil
}

// process — the Process implementation without return branching.
func (p *Pipeline) process(ctx context.Context, abs string) Outcome {
	rel := filepath.ToSlash(abs)
	if r, err := filepath.Rel(p.opts.Dir, abs); err == nil {
		rel = filepath.ToSlash(r)
	}
	// A path outside incoming / Rel errors go into ParsePath as is — it
	// rejects an invalid structure itself (T-014).

	f, err := ParsePath(rel)
	if err != nil {
		// The path structure and the zone name will not change on a
		// retry — the file is invalid (spec 5.1: failed/ with the log
		// report).
		return p.failFile(abs, FileReport{Path: rel}, err)
	}
	rep := FileReport{Zone: f.Zone, Action: f.Action, Path: f.Path}

	// A multi-level zone directory — only from the allowlist (the
	// tld-only-zones spec §5.1): the check runs before EnsureZone and
	// reading the file — a forbidden directory neither creates a zone in
	// the reference table nor loads a batch, the file is invalid right
	// away (failed/ with the reason, lines are not parsed, T-021).
	// The same check stands in NewValidator (a guard for direct
	// validator calls outside the pipeline).
	if err := checkZonePath(f.Zone, p.opts.AllowedZonePaths); err != nil {
		return p.failFile(abs, rep, fmt.Errorf("ingest: the file %s: %w", f.Path, err))
	}

	// The zone of a zone directory is found or created automatically on
	// the first file (spec 5.1; EnsureZone, T-014) and immediately
	// warms the line zone cache. A zone-less file (f.Zone == "") has no
	// upfront zones: they are created lazily by the first lines
	// (emitRow), the cache is per file. A failure is an infrastructure
	// one: the file stays (at-least-once, spec 3.4).
	zoneIDs := make(map[string]int16)
	if f.Zone != "" {
		z, zerr := EnsureZone(ctx, p.zones, f.Zone)
		if zerr != nil {
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: zerr}
		}
		rep.ZoneID = z.ID
		zoneIDs[f.Zone] = z.ID
	}
	zoneID := rep.ZoneID // 0 — a zone-less file (the report: zone_id NULL)

	id, err := NewBatchID()
	if err != nil {
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: err}
	}
	batch := Batch{ID: id, ZoneID: zoneID, Action: f.Action}

	fh, err := os.Open(abs)
	if err != nil {
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: fmt.Errorf("ingest: reading the file %s: %w", abs, err)}
	}
	// The file is opened read-only: a close error is harmless.
	defer func() { _ = fh.Close() }()

	// One pass over the file (spec 3.5): bytes go simultaneously into
	// sha256 (TeeReader) and into the parser; valid lines are loaded
	// into staging as parsing proceeds. The hash is known by the end of
	// reading — the duplicate check is below. Archives (T-133) branch by
	// extension: tar/gz — the same single pass (TeeReader under the
	// unpackers), zip — a hash pass plus reading members by offsets
	// (ReaderAt: a random-access format).
	h := sha256.New()
	var (
		st   Stats
		vres FileResult
		arch archiveResult
	)
	if kind := archiveKindOf(f.Path); kind == archiveNone {
		st, vres, _, err = p.streamStage(ctx, io.TeeReader(fh, h), f, batch, zoneIDs)
		rep.SHA256 = h.Sum(nil)
	} else {
		arch, err = p.streamArchive(ctx, fh, f, kind, batch, zoneIDs)
		st, vres = arch.Stats, arch.Result
		rep.SHA256 = arch.SHA256
		rep.Members = arch.Members
	}
	rep.LinesTotal = int64(st.Total)
	rep.Skipped = int64(st.Skipped)
	rep.Rejected = int64(st.Rejected + vres.Rejected)
	rep.Accepted = int64(vres.Accepted)
	rep.Reasons = vres.Reasons
	if err != nil {
		if errors.Is(err, errArchiveRejected) || errors.Is(err, errZoneNotAllowed) {
			// An error in the substance of the file: the archive is
			// invalid (broken, limits, a member above the threshold —
			// T-133) or a multi-level directory zone outside the
			// allowlist (the NewValidator guard branch — the working
			// check stands at the beginning of process; this is reached
			// only on option desync, we classify the file, not an eternal
			// retry): like a txt file with an error share above the
			// threshold — lines are not applied, the batch is discarded
			// (spec 5.1), the file as a whole goes to failed/ with the log
			// report; partially accepted lines were not applied,
			// resubmission is idempotent (ON CONFLICT DO NOTHING, spec 5.1).
			if derr := p.clearBatch(ctx, id); derr != nil {
				p.discard(id)
				return Outcome{Kind: OutcomeLeft, Report: rep, Err: derr}
			}
			return p.failFile(abs, rep, err)
		}
		p.discard(id)
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: err}
	}

	if vres.Failed {
		// The error share is above the threshold (T-016): the file is
		// invalid as a whole, lines are not applied — the batch is
		// discarded (spec 5.1).
		if derr := p.clearBatch(ctx, id); derr != nil {
			// The clearing did not go through (including ctx cancellation
			// during graceful shutdown): retried with its own context —
			// the batch lines must not stay in staging (the batch_id will
			// never be seen again).
			p.discard(id)
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: derr}
		}
		// Numerator/denominator — as in the streamStage verdict: parser
		// rejections (invalid_utf8, line_len) plus validator rejections.
		rejectedAll := int64(st.Rejected) + int64(vres.Rejected)
		contentAll := int64(vres.Total) + int64(st.Rejected)
		return p.failFile(abs, rep, fmt.Errorf(
			"the share of rejected lines %d of %d is above ingest.max_error_ratio (%g)",
			rejectedAll, contentAll, p.opts.MaxErrorRatio))
	}

	// Idempotency (spec 5.1; T-113): only exact redelivery is skipped —
	// a match of (zone, action, sha256); for a zone-less file the log
	// "zone" is NULL (a file has no zones). A duplicate with another
	// action (an undo by the same list: add/, then delete/) and a
	// duplicate in another zone are processed: the harmlessness of
	// repeated applying is guaranteed by the domain operations (T-097).
	known, err := p.files.FilesBySHA256(ctx, rep.SHA256)
	if err != nil {
		p.discard(id)
		return Outcome{Kind: OutcomeLeft, Report: rep,
			Err: fmt.Errorf("ingest: the file %s duplicate check: %w", f.Path, err)}
	}
	var dup, crossZone, crossAction *model.IngestFile
	for i := range known {
		k := &known[i]
		sameZone := (zoneID != 0 && k.ZoneID != nil && *k.ZoneID == zoneID) ||
			(zoneID == 0 && k.ZoneID == nil)
		sameAction := k.Action != nil && *k.Action == f.Action
		switch {
		case sameZone && sameAction:
			dup = k // exact redelivery
		case sameAction:
			if crossZone == nil {
				crossZone = k // the same action in another zone
			}
		default:
			if crossAction == nil {
				crossAction = k // another action, any zone
			}
		}
	}

	if dup != nil {
		if derr := p.clearBatch(ctx, id); derr != nil {
			// The duplicate is not applied, but the batch is already
			// loaded: a clearing failure (including ctx cancellation) is
			// retried by discard — as in the error threshold branch above.
			p.discard(id)
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: derr}
		}
		if _, merr := p.moveFile(DirProcessed, f.Path, abs); merr != nil {
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: merr}
		}
		p.log.Info("the file is already processed (a duplicate — the same zone, action and sha256) — moved to processed",
			"path", f.Path, "sha256", fmt.Sprintf("%x", rep.SHA256),
			"first_path", derefString(dup.Path),
			"first_processed_at", derefTime(dup.ProcessedAt))
		return Outcome{Kind: OutcomeDuplicate, Report: rep}
	}

	// No exact redelivery — the file is applied. The same content was
	// already delivered differently: reprocessing in another zone with
	// the same action is legitimate (the first delivery did not touch
	// that zone's domains), but it is useful for the operator to see it
	// (Warn, ING-P2-5); another action — a planned undo (Info).
	if crossZone != nil {
		p.log.Warn("a sha256 duplicate in another zone under the same action — processing it",
			"path", f.Path, "action", f.Action.String(), "zone_id", zoneID,
			"first_path", derefString(crossZone.Path),
			"first_zone_id", derefInt16(crossZone.ZoneID),
			"first_processed_at", derefTime(crossZone.ProcessedAt))
	} else if crossAction != nil {
		p.log.Info("the same content was already delivered under another action — processing it (undo)",
			"path", f.Path, "action", f.Action.String(), "zone_id", zoneID,
			"first_path", derefString(crossAction.Path),
			"first_action", derefAction(crossAction.Action),
			"first_processed_at", derefTime(crossAction.ProcessedAt))
	}

	// Applying by the file action (spec 5.1): add → ApplyAdd,
	// delete → ApplyDelete. Both are idempotent (spec 3.4); a failure —
	// the file stays in incoming, a retry converges to the same outcome.
	switch f.Action {
	case model.ActionAdd:
		// Apply chunks — the same ingest.batch_size as the COPY chunks
		// into staging (T-205).
		res, aerr := ApplyAdd(ctx, p.pool, batch, p.opts.BatchSize)
		if aerr != nil {
			p.discard(id)
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: aerr}
		}
		rep.Added = res.Inserted + res.Revived
		rep.Revived = res.Revived
		rep.Existed = res.Existing
	case model.ActionDelete:
		res, derr := ApplyDelete(ctx, p.pool, batch)
		if derr != nil {
			p.discard(id)
			return Outcome{Kind: OutcomeLeft, Report: rep, Err: derr}
		}
		rep.Deleted = res.Deleted
		rep.NotFound = res.NotFound
	}

	// The report — after processing the file (spec 5.1: "after
	// processing the file a report is written"). The (zone_id, action,
	// sha256) uniqueness is the second line of defense against
	// duplicates: a concurrent report is rejected by the schema
	// (sqlFileRecord).
	if rerr := p.files.RecordFile(ctx, rep.record(p.now())); rerr != nil {
		return Outcome{Kind: OutcomeLeft, Report: rep,
			Err: fmt.Errorf("ingest: the file %s report: %w", f.Path, rerr)}
	}

	if _, merr := p.moveFile(DirProcessed, f.Path, abs); merr != nil {
		// The report is already in the log: reprocessing will find the
		// duplicate and only move the file — self-healing without
		// re-applying.
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: merr}
	}

	p.log.Info("the file is processed",
		"path", f.Path, "zone", f.Zone, "zones", len(zoneIDs),
		"action", f.Action.String(),
		"lines", rep.LinesTotal, "added", rep.Added, "revived", rep.Revived,
		"existed", rep.Existed, "deleted", rep.Deleted, "not_found", rep.NotFound,
		"rejected", rep.Rejected, "sha256", fmt.Sprintf("%x", rep.SHA256))
	return Outcome{Kind: OutcomeApplied, Report: rep}
}

// emitRow — a file line into a staging row: the record zone
// (Entry.Zone — the path zone or, for a zone-less file, the one
// derived from the name) is resolved into zones.id via the file cache;
// a missing zone is created automatically (EnsureZone, spec 5.1) — by
// the first line of a zone-less file or the first line of a new zone.
// The cache keeps one EnsureZone check (the zone is unfrozen) per file
// zone: a zone-directory file — one entry, a zone-less one — as many
// as the file's zones.
func (p *Pipeline) emitRow(ctx context.Context, cache map[string]int16, emit func(Row) error, e Entry, lineNo int64) error {
	id, ok := cache[e.Zone]
	if !ok {
		z, err := EnsureZone(ctx, p.zones, e.Zone)
		if err != nil {
			return fmt.Errorf("ingest: the line zone %s: %w", e.Zone, err)
		}
		id = z.ID
		cache[e.Zone] = id
	}
	return emit(Row{ZoneID: id, Label: e.Label, LineNo: lineNo})
}

// streamStage — one pass over the file (no re-reading, spec 3.5): the
// stream r goes into the parser (T-015), meaningful lines are
// validated (T-016) and loaded into staging (T-017) as parsing
// proceeds. Returns the parser counters, the validation outcome
// (Reasons — already merged with the per-line parser rejections;
// Failed — by the error share of the validator and the parser
// together) and the number of loaded lines. An error is a file reading
// or loading failure; the counters match what was yielded.
//
// The parser is pushing (OnLine), Stage is pulling (RowIter); the link
// is stageStream (a producer goroutine and a buffered channel).
func (p *Pipeline) streamStage(ctx context.Context, r io.Reader, f File, batch Batch, zoneIDs map[string]int16) (Stats, FileResult, int64, error) {
	v, err := NewValidator(ValidatorOptions{
		File:             f.Path,
		Zone:             f.Zone,
		AllowedZonePaths: p.opts.AllowedZonePaths,
		MaxErrorRatio:    p.opts.MaxErrorRatio,
		RejectLog:        p.opts.RejectLog,
	})
	if err != nil {
		return Stats{}, FileResult{}, 0, fmt.Errorf("ingest: the file %s: %w", f.Path, err)
	}
	// Per-line parser rejections are written to the same rejected log
	// as the validator rejections (the format — Validator.reject):
	// Reject.Text was already truncated by the parser (maxRejectText),
	// a nil logger — records suppressed.
	rejectLog := p.opts.RejectLog
	if rejectLog == nil {
		rejectLog = slog.New(slog.DiscardHandler)
	}

	var (
		st       Stats
		prejects = make(map[Reason]int) // per-line parser rejections
	)
	staged, serr, perr := p.stageStream(ctx, batch, func(emit func(Row) error) error {
		var perr error
		st, perr = Parse(r, Options{
			OnLine: func(l Line) error {
				e, _, ok := v.Line(l.Num, l.Text)
				if !ok {
					return nil // the rejection is counted by the validator (the rejected log)
				}
				return p.emitRow(ctx, zoneIDs, emit, e, int64(l.Num))
			},
			OnReject: func(rj Reject) error {
				prejects[rj.Reason]++
				rejectLog.Warn("line rejected",
					"file", f.Path,
					"line", rj.Num,
					"text", rj.Text,
					"reason", string(rj.Reason),
				)
				return nil
			},
		})
		return perr
	})

	if serr != nil {
		return st, FileResult{}, staged, serr
	}
	if perr != nil && !errors.Is(perr, errStopped) {
		return st, FileResult{}, staged, fmt.Errorf("ingest: parsing the file %s: %w", f.Path, perr)
	}
	res := v.Finish()
	res.Reasons = mergeReasons(prejects, res.Reasons)
	// Per-line parser rejections (invalid_utf8, line_len) never reach
	// v.Line and are therefore not in the Finish counters — we add them
	// into the threshold ourselves: the error share is counted over all
	// meaningful lines of the file (a broken dump is exactly non-UTF-8,
	// docs/incoming-files.md §4), otherwise a file of pure junk
	// (Data = 0) would pass as successful (ING-P1-2).
	res.Failed = overErrorRatio(p.opts.MaxErrorRatio, res.Total, res.Rejected, st.Rejected)
	return st, res, staged, nil
}

// stageStream — the link between the line producer and the staging
// load (spec 3.5): produce pushes valid lines through emit, Stage
// pulls them from the buffered channel in COPY chunks. Memory is
// bounded by stageChanSize and the chunk size; on a load error the
// consumer closes stop, the producer finishes without a leak. Load and
// producer errors are returned separately: a producer stopped by the
// consumer (errStopped) is not an error, the cause is in stageErr.
// The producer counters stay in its closures (by the time of the
// return the goroutine has finished — reading is safe).
func (p *Pipeline) stageStream(ctx context.Context, batch Batch, produce func(emit func(Row) error) error) (staged int64, stageErr, produceErr error) {
	rows := make(chan Row, stageChanSize)
	stop := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- produce(func(r Row) error {
			select {
			case rows <- r:
				return nil
			case <-stop:
				return errStopped
			}
		})
		close(rows)
	}()

	staged, serr := Stage(ctx, p.pool, batch, p.opts.BatchSize, func() (Row, error) {
		select {
		case row, ok := <-rows:
			if !ok {
				return Row{}, io.EOF
			}
			return row, nil
		case <-ctx.Done():
			return Row{}, ctx.Err()
		}
	})

	close(stop)
	perr := <-done

	if serr != nil {
		return staged, serr, nil
	}
	return staged, nil, perr
}

// clearBatch — clearing the batch in staging; the error is wrapped
// with context (after a processing error or for a non-applicable
// batch).
func (p *Pipeline) clearBatch(ctx context.Context, id BatchID) error {
	if _, err := ClearBatch(ctx, p.pool, id); err != nil {
		return fmt.Errorf("ingest: discarding the batch in %s: %w", StageTable, err)
	}
	return nil
}

// discardTimeout — the margin for clearing a batch after a failure: a
// separate context with a timeout so that best-effort clearing does
// not hang forever.
const discardTimeout = 5 * time.Second

// discard — drop the batch after an infrastructure failure (best
// effort): every processing attempt takes a new batch_id, the rows of
// the failed batch are dead — we clean them up so staging does not
// grow (T-017). Also called with an already cancelled ctx (graceful
// shutdown at the moment of loading): the clearing must survive
// cancellation — otherwise the batch rows would stay in staging
// forever (the batch_id will never be seen again) and the table would
// grow from restart to restart. The context is its own, with a
// timeout; a clearing error does not shadow the main one: it is
// logged.
func (p *Pipeline) discard(id BatchID) {
	ctx, cancel := context.WithTimeout(context.Background(), discardTimeout)
	defer cancel()
	if err := p.clearBatch(ctx, id); err != nil {
		p.log.Warn("the discarded batch cleanup", "err", err)
	}
}

// failFile — the file is invalid: moving to failed/ and the log
// report nearby (spec 5.1). The rel of the destination is the parsed
// path (it mirrors the zone/action); the path from an unparsed report
// is not trusted (Process is a public API; an absolute path or ".."
// in the components would let filepath.Join escape failed/): the file
// is placed by its base name. The report name is <file>.report.
func (p *Pipeline) failFile(abs string, rep FileReport, reason error) Outcome {
	rel := archiveRel(rep.Path, abs)
	dst, err := p.moveFile(DirFailed, rel, abs)
	if err != nil {
		return Outcome{Kind: OutcomeLeft, Report: rep, Err: err}
	}
	// The file is already out of incoming: a report write failure does
	// not bring it back — the error reason is preserved in the service
	// log.
	if werr := os.WriteFile(dst+".report", []byte(renderReport(rep, "bad — "+reason.Error())), 0o644); werr != nil {
		p.log.Warn("the log report next to the file was not written",
			"path", dst+".report", "err", werr)
	}
	p.log.Warn("the file is bad — moved to failed",
		"path", rep.Path, "failed", dst, "err", reason)
	return Outcome{Kind: OutcomeFailed, Report: rep, Err: reason}
}

// moveFile moves the file to the processed/failed archive preserving
// the zone and action structure (incoming/com/add/x.txt →
// processed/com/add/x.txt): the file origin is visible in the archive
// without the log. rel — the destination path inside the archive
// directory. os.Rename on Unix silently replaces the target, so a
// taken name is checked explicitly; a collision (another instance of
// a file with the same name) gets a numeric suffix: x.txt → x-2.txt,
// x-3.txt, …
func (p *Pipeline) moveFile(archive, rel, abs string) (string, error) {
	dst := filepath.Join(p.opts.Dir, archive, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", fmt.Errorf("ingest: the archive directory %s: %w", filepath.Dir(dst), err)
	}
	ext := filepath.Ext(dst)
	stem := strings.TrimSuffix(dst, ext)
	for i := 1; ; i++ {
		target := dst
		if i > 1 {
			target = fmt.Sprintf("%s-%d%s", stem, i, ext)
		}
		if _, err := os.Lstat(target); err == nil {
			continue // the name is taken by another instance
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("ingest: the archive %s: %w", target, err)
		}
		if err := os.Rename(abs, target); err != nil {
			return "", fmt.Errorf("ingest: moving %s → %s: %w", abs, target, err)
		}
		return target, nil
	}
}

// archiveRel — a safe relative destination path inside the archive
// directory. A path parsed by ParsePath is safe by construction
// (path.Clean, no ".." — T-014); an unparsed rep.Path is not trusted:
// an absolute path or ".." in the components would escape
// processed/failed after filepath.Join (ING-P3-1) — the file's base
// name is substituted.
func archiveRel(rel, abs string) string {
	if rel != "" && !path.IsAbs(rel) {
		if c := path.Clean(rel); c != "." && c != ".." && !strings.HasPrefix(c, "../") {
			return c
		}
	}
	return filepath.Base(abs)
}

// derefString — the value of a NULL log column for the log (an empty string).
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// derefTime — the value of a NULL log column for the log (the zero moment).
func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// derefAction — the value of a NULL log column for the log (an empty name).
func derefAction(a *model.Action) string {
	if a == nil {
		return ""
	}
	return a.String()
}

// derefInt16 — the value of a NULL log column for the log (zero).
func derefInt16(v *int16) int16 {
	if v == nil {
		return 0
	}
	return *v
}

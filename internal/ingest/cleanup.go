// Background cleanup of deleted domains (T-020; spec 5.1 delete branch,
// 13.1, 8).
//
// A deleted domain stays in the DB with the deleted status (spec 13.1),
// and a background task removes its results and content references: for
// domains with status = deleted the subdomain_results rows are deleted,
// and contents.refcount is decreased for the content_hash of each
// deleted row (spec 6.1: content_hash is a reference to contents;
// deduplication — spec 5.7, 8).
//
// The boundary with stage 4: content with refcount → 0 is NOT physically
// removed — pack files and garbage collection (pack compaction) stay
// with the content store (T-042/T-045, spec 5.7); a zero refcount only
// marks the data dead for a future compaction. The task does not touch
// laravel_* finds: the T-020 plan limits the cleanup to results and
// content references, and the domain stays in the DB (spec 13.1).
//
// Tracking what was processed without a schema change: the "mark in the
// row" is the remaining results themselves. A pass candidate is a
// deleted domain that still has subdomain_results rows (EXISTS); a fully
// cleaned domain is not selected at all, repeated passes do no extra
// work, and a failure or restart loses nothing — there is no external
// state. The candidate selection is served by the partial index
// domains_deleted_idx (migration 0013, T-170): (zone_id, id)
// WHERE status = 6 — the key matches the PK and the selection's ORDER BY
// (chunks without sorting, early stop by LIMIT), and while there are no
// deleted domains the pass does not read the domains table at all
// (production measurement Oct 23–24: without the index the planner
// scanned all subdomain_results — 17 s of DB CPU per pass, idle).
// The status code is baked into the SQL text as a literal via
// model.StatusDeleted (scheduler/dispatch.go style): the planner cannot
// prove a parameter in the partial index predicate, and a generic plan
// of the prepared statement would lose the index.
//
// The same loop cleans up orphaned ingest_stage batches (T-098, the
// ING-P2-3 guard): the rows of a batch whose pipeline died without a
// discard (kill -9 of the service, a DB crash) are removed by the
// batch_id age — the creation time is baked into the identifier's high
// bytes (stage.go), see CleanupStaleBatches.
//
// The start moment is on a schedule (a ticker in CleanupDeleted), not an
// event after applying a delete file: the application (T-019) is not
// coupled with the cleanup by API, and repeatable passes are guaranteed
// to pick up everything — including results appended by workers after
// the domain was deleted (spec 8: "…or is removed by the background
// cleanup"). Wiring into the ingest service — T-021.
package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
)

const (
	// DefaultCleanupBatch — the chunk limit: domains per pass.
	// Comparable to the scheduler chunks (spec 5.3: 1,000–10,000): the
	// pass transaction stays short — subdomain_results row locks are held
	// for the minimum time — and the drain rate at the default interval
	// (1,000 domains a minute) is up to 60,000 domains per hour.
	DefaultCleanupBatch = 1000

	// DefaultCleanupInterval — the interval of CleanupDeleted loop
	// passes. The spec does not set a cleanup rate: the background task
	// is in no hurry, the data is not served to clients anymore; T-021
	// can move the value into config.
	DefaultCleanupInterval = time.Minute
)

// contentHashLen — the size of content_hash → contents.hash: 16 bytes,
// 128 bits (spec 6.1, 5.7). Checked when reading RETURNING — a cheap
// invariant against result-writer bugs (T-056), like MaxLabelLen in
// Stage. A wrong length does not crash the pass (poison pill,
// ING-P2-4): the row is already deleted, its refcount was not changed.
const contentHashLen = 16

// Pass SQL. unnest chunks — the style of the batched UPDATEs of
// internal/db (spec 6: "writing in chunks … UPDATE … FROM unnest(...)").
const (
	// cleanupUpdateSQL — decreasing the refcount by the aggregated hashes:
	// one content can be referenced from many deleted results
	// (deduplication, spec 5.7). GREATEST(..., 0) keeps the counter
	// non-negative on desync (before migration 000019 there was no CHECK
	// in the schema; now the invariant duplicates contents_refcount_check,
	// and the clamp is the first line of defense keeping a mass pass from
	// triggering it — the internal/content/pack.go spec, I1): a negative
	// would break the "0 = dead content" invariant for compaction
	// (T-042/T-045). refcount = 0 is not a row deletion: the storage is
	// cleaned physically by pack compaction (stage 4, T-045).
	cleanupUpdateSQL = "UPDATE contents SET refcount = GREATEST(refcount - h.n, 0) " +
		"FROM unnest($1::bytea[], $2::int4[]) AS h(hash, n) " +
		"WHERE contents.hash = h.hash"
)

// cleanupSelectSQL — the candidates: deleted domains with remaining
// results. EXISTS is that very "mark": cleaned domains are not selected.
// ORDER BY the domain key — deterministic chunks without sorting
// (matches the PK and the domains_deleted_idx key, spec 6.1/T-170). The
// status code is a literal in the text (fmt.Sprintf via model,
// dispatch.go style): the planner proves the partial index predicate
// only by a literal.
var cleanupSelectSQL = fmt.Sprintf(
	"SELECT d.zone_id, d.id FROM domains d "+
		"WHERE d.status = %d "+
		"AND EXISTS (SELECT 1 FROM subdomain_results r "+
		"WHERE r.zone_id = d.zone_id AND r.domain_id = d.id) "+
		"ORDER BY d.zone_id, d.id LIMIT $1",
	int16(model.StatusDeleted))

// cleanupDeleteSQL — deleting the results of a chunk of domains. The
// EXISTS guard over domains.status — against a race with applying an
// add file (T-018): a domain revived from deleted to new/pending between
// the candidate selection and the deletion keeps its results (spec 5.1:
// a returning domain "is not changed"); a domain deleted again will go
// through the loop once more. RETURNING yields only the rows actually
// deleted — the refcount decrease below goes strictly by them: a second
// parallel cleanup that deleted the same rows earlier will not decrease
// the refcount again. The status literal — the provability of the
// domains_deleted_idx predicate (T-170): the guard is an index probe,
// not a table read.
var cleanupDeleteSQL = fmt.Sprintf(
	"DELETE FROM subdomain_results r "+
		"USING unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) "+
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id "+
		"AND EXISTS (SELECT 1 FROM domains d "+
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id AND d.status = %d) "+
		"RETURNING r.zone_id, r.domain_id, r.content_hash",
	int16(model.StatusDeleted))

// staleStageSQL — the cleanup of orphaned staging batches (T-098): the
// batch_id range below the "now − StaleBatchAge" bound — a scan over the
// leading primary key column, like the cleanup of the pipeline's own
// batch. The table name comes from StageTable (identifier quoting, as in
// internal/db), a single source for SQL and COPY — hence a var, like
// clearStageSQL.
var staleStageSQL = "DELETE FROM " + StageTable.Sanitize() + " WHERE batch_id < $1"

// CleanupDB — a pool for the cleanup: a db.DB from which a transaction
// can be started. A pass is atomic (deleting the results and decreasing
// the refcount is one transaction: a failure between them would leave
// the refcount inflated forever), so db.DB is not enough — Begin is
// needed; *pgxpool.Pool satisfies it.
type CleanupDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool satisfies the interface.
var _ CleanupDB = (*pgxpool.Pool)(nil)

// CleanupOptions — the cleanup parameters; zero values are replaced
// with the defaults (withDefaults, watch.Options style).
type CleanupOptions struct {
	// BatchSize — the maximum domains in one pass (DefaultCleanupBatch);
	// the chunk limit is set by the candidate SELECT.
	BatchSize int
	// Interval — the period of the CleanupDeleted loop's repeated passes
	// (DefaultCleanupInterval); a one-off CleanupOnce is not affected.
	Interval time.Duration
	// Log — the CleanupDeleted loop logger; nil — records suppressed
	// (slog.DiscardHandler). CleanupOnce does not log — the result is
	// returned as CleanupResult.
	Log *slog.Logger
	// OnPass — the hook of a successful pass's result: a snapshot of
	// CleanupResult right after the transaction commit (a failed pass is
	// not handed to the hook — Warn has already recorded it). The wiring
	// point of metric export (T-099) without the loop depending on
	// internal/metrics; the AlertsOptions.OnPass style (log + field).
	// Must be fast and must not panic — called in the pass loop.
	OnPass func(CleanupResult)
}

// withDefaults replaces non-positive values with the defaults.
func (o CleanupOptions) withDefaults() CleanupOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultCleanupBatch
	}
	if o.Interval <= 0 {
		o.Interval = DefaultCleanupInterval
	}
	return o
}

// CleanupResult — the result of one CleanupOnce pass.
type CleanupResult struct {
	// Domains — the domains whose results were deleted (after the guard
	// against ones revived by an add file; no more than the chunk size).
	Domains int
	// Results — the deleted subdomain_results rows.
	Results int64
	// Contents — the contents rows with a decreased refcount (distinct
	// hashes).
	Contents int64
	// BadHashes — the deleted results with a wrong-length content_hash
	// (the contents contract — 16 octets, spec 6.1): the pass does not
	// crash, their refcount was not changed; the CleanupDeleted loop
	// marks this with a Warn.
	BadHashes int64
}

// cleanupKey — the domain key: partition + identifier, like the domains
// PK.
type cleanupKey struct {
	zoneID int16
	id     int64
}

// cleanupHash — the 128-bit content_hash (spec 6.1); the reference
// aggregation key.
type cleanupHash [contentHashLen]byte

// CleanupOnce performs one cleanup pass (spec 5.1, 13.1): up to
// opts.BatchSize domains with the deleted status that still have
// results — their subdomain_results rows are deleted in one transaction,
// and contents.refcount is decreased for the content_hash of each
// deleted row. A repeated pass is safe (spec 3.4): cleaned domains are
// not selected, and the refcount is decreased strictly by the references
// actually deleted.
func CleanupOnce(ctx context.Context, pool CleanupDB, opts CleanupOptions) (CleanupResult, error) {
	if pool == nil {
		return CleanupResult{}, errors.New("ingest: очистка удалённых доменов: пул не задан")
	}
	o := opts.withDefaults()

	zoneIDs, domainIDs, err := cleanupCandidates(ctx, pool, o.BatchSize)
	if err != nil {
		return CleanupResult{}, err
	}
	if len(zoneIDs) == 0 {
		// An empty set — nothing to clean; no transaction is opened.
		return CleanupResult{}, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return CleanupResult{}, fmt.Errorf("ingest: очистка удалённых доменов: транзакция: %w", err)
	}
	// Rollback unconditionally: after Commit it is a no-op. An interrupted
	// pass leaves no half-done changes (spec 3.4).
	defer func() { _ = tx.Rollback(ctx) }()

	res, refs, err := cleanupResults(ctx, tx, zoneIDs, domainIDs)
	if err != nil {
		return CleanupResult{}, err
	}
	if len(refs) > 0 {
		n, err := cleanupRefcounts(ctx, tx, refs)
		if err != nil {
			return CleanupResult{}, err
		}
		res.Contents = n
	}
	if err := tx.Commit(ctx); err != nil {
		return CleanupResult{}, fmt.Errorf("ingest: очистка удалённых доменов: фиксация: %w", err)
	}
	return res, nil
}

// cleanupCandidates selects up to limit deleted domains with remaining
// results (cleanupSelectSQL): parallel arrays of zone and domain keys
// for the unnest chunk DELETE.
func cleanupCandidates(ctx context.Context, pool CleanupDB, limit int) ([]int16, []int64, error) {
	rows, err := pool.Query(ctx, cleanupSelectSQL, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("ingest: очистка удалённых доменов: выборка кандидатов: %w", err)
	}
	defer rows.Close()

	var zoneIDs []int16
	var domainIDs []int64
	for rows.Next() {
		var (
			zoneID   int16
			domainID int64
		)
		if err := rows.Scan(&zoneID, &domainID); err != nil {
			return nil, nil, fmt.Errorf("ingest: очистка удалённых доменов: чтение кандидатов: %w", err)
		}
		zoneIDs = append(zoneIDs, zoneID)
		domainIDs = append(domainIDs, domainID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("ingest: очистка удалённых доменов: выборка кандидатов: %w", err)
	}
	return zoneIDs, domainIDs, nil
}

// cleanupResults deletes the subdomain_results rows of a chunk of
// domains (cleanupDeleteSQL) and returns the pass counters together with
// the number of deleted references to each content hash.
func cleanupResults(ctx context.Context, tx pgx.Tx, zoneIDs []int16, domainIDs []int64) (CleanupResult, map[cleanupHash]int32, error) {
	rows, err := tx.Query(ctx, cleanupDeleteSQL, zoneIDs, domainIDs)
	if err != nil {
		return CleanupResult{}, nil, fmt.Errorf("ingest: очистка удалённых доменов: удаление результатов: %w", err)
	}
	defer rows.Close()

	domains := make(map[cleanupKey]struct{}, len(zoneIDs))
	refs := make(map[cleanupHash]int32)
	var res CleanupResult
	for rows.Next() {
		var (
			zoneID   int16
			domainID int64
			hash     []byte
		)
		if err := rows.Scan(&zoneID, &domainID, &hash); err != nil {
			return CleanupResult{}, nil, fmt.Errorf("ingest: очистка удалённых доменов: чтение удалённых результатов: %w", err)
		}
		domains[cleanupKey{zoneID: zoneID, id: domainID}] = struct{}{}
		res.Results++
		switch {
		case hash == nil:
			// A result without content (NULL content_hash) — holds no
			// references.
		case len(hash) != contentHashLen:
			// A result-writer bug (T-056): the row is already deleted,
			// crashing the pass is not allowed — one bad hash would block
			// the cleanup forever (the same candidates are reselected,
			// ING-P2-4). The refcount of such a reference is not touched;
			// the result is BadHashes.
			res.BadHashes++
		default:
			var h cleanupHash
			copy(h[:], hash)
			refs[h]++
		}
	}
	if err := rows.Err(); err != nil {
		return CleanupResult{}, nil, fmt.Errorf("ingest: очистка удалённых доменов: удаление результатов: %w", err)
	}
	res.Domains = len(domains)
	return res, refs, nil
}

// cleanupRefcounts decreases contents.refcount by the number of deleted
// references (cleanupUpdateSQL) and returns the number of contents rows
// affected.
func cleanupRefcounts(ctx context.Context, tx pgx.Tx, refs map[cleanupHash]int32) (int64, error) {
	list := make([]cleanupHash, 0, len(refs))
	for h := range refs {
		list = append(list, h)
	}
	// A deterministic argument order — reproducible chunks.
	sort.Slice(list, func(i, j int) bool {
		return bytes.Compare(list[i][:], list[j][:]) < 0
	})
	hashes := make([][]byte, len(list))
	counts := make([]int32, len(list))
	for i, h := range list {
		hashes[i] = h[:]
		counts[i] = refs[h]
	}

	tag, err := tx.Exec(ctx, cleanupUpdateSQL, hashes, counts)
	if err != nil {
		return 0, fmt.Errorf("ingest: очистка удалённых доменов: уменьшение refcount contents: %w", err)
	}
	return tag.RowsAffected(), nil
}

// StaleBatchAge — the orphaned staging batch threshold (T-098). After
// the ING-P2-3 fix (614e690) a failed batch is cleaned by a discard with
// its own context, but a kill -9 of the service or a DB crash leaves the
// rows forever: the UNLOGGED table survives a service restart and is
// truncated only by PostgreSQL crash recovery — a graceful DB restart
// keeps the rows. A batch lives exactly as long as one file is
// processed — the pipeline processes files sequentially, and COPY
// chunks deliver ~10⁵–10⁶ rows/s, so even a huge file fits in minutes:
// a day is a deliberate margin over the lifetime of any live batch; the
// cleanup cannot cut into the batch of a running Process (the race is
// excluded by age, not by locking). Orphaned rows do not affect
// correctness (a batch_id will not be encountered again) — only disk
// space, so a large margin is cheaper than the risk; no config key is
// introduced (the export is for the integration test of the boundary).
//
// The contract boundary is the system clock (age and bounds are wall
// time): a time step forward larger than the threshold during live
// processing (a VM pause longer than a day, a manual date edit, a start
// before time synchronization) prematurely ages a batch. The most common
// complication of a long VM pause — a lost DB connection — heals by
// itself: Process will return OutcomeLeft, and the file is reprocessed
// with a fresh batch_id. Excluding active batches from the cleanup (a
// registry shared by the pipeline and the loop) is deliberately not
// done: the cost of the state is out of proportion to the scenario, and
// node clocks must be consistent (NTP/chrony in slew mode).
//
// Rows left over from the versions before T-098 (a fully random
// batch_id) almost never fall into the time range and are not deleted at
// all — until a PostgreSQL crash recovery truncates the UNLOGGED table;
// this does not affect correctness, only disk space.
const StaleBatchAge = 24 * time.Hour

// CleanupStaleBatches deletes from staging the rows of batches older
// than StaleBatchAge (T-098) and returns their count. The age is read
// from the batch_id itself — the creation time in the high bytes
// (stage.go): staging has no timestamp column, and the range over the
// identifier prefix is served by the primary key. Live batches younger
// than the threshold are not touched; an empty staging — DELETE 0.
func CleanupStaleBatches(ctx context.Context, pool db.DB, now time.Time) (int64, error) {
	if pool == nil {
		return 0, errors.New("ingest: зачистка осиротевших партий staging: пул не задан")
	}
	// The age bound must be expressible as positive microseconds: before
	// 1970 (a clock on the Unix epoch — a dead RTC battery, a start
	// before synchronization) the uint64 cast in batchIDBefore would make
	// the bound huge and the cleanup would cut away the whole staging. A
	// failure (Warn in the loop) instead of deleting everything — it is
	// the now−StaleBatchAge bound that is checked, not now itself: a
	// clock on the 1970-01-01 epoch gives a negative bound.
	if cutoff := now.Add(-StaleBatchAge); cutoff.UnixMicro() < 0 {
		return 0, errors.New("ingest: зачистка осиротевших партий staging: граница возраста до 1970 года — системные часы не синхронизированы")
	}
	cutoff := batchIDBefore(now.Add(-StaleBatchAge))
	tag, err := pool.Exec(ctx, staleStageSQL, cutoff[:])
	if err != nil {
		return 0, fmt.Errorf("ingest: зачистка осиротевших партий %s: %w", StageTable, err)
	}
	return tag.RowsAffected(), nil
}

// CleanupDeleted — the background cleanup task (spec 5.1, 13.1):
// repeats CleanupOnce on a ticker (the first pass — immediately: the
// drain of what accumulated after a restart) until ctx is cancelled and
// returns nil — the OnRun contract of the service framework (T-012: a
// clean shutdown on SIGTERM). The same passes clean up orphaned staging
// batches (CleanupStaleBatches, T-098). A pass error does not stop the
// task or the service: it is logged (Warn), the next pass will retry —
// passes are idempotent (spec 3.4).
func CleanupDeleted(ctx context.Context, pool CleanupDB, opts CleanupOptions) error {
	if pool == nil {
		return errors.New("ingest: очистка удалённых доменов: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("фоновая очистка удалённых доменов",
		"interval", o.Interval.String(), "batch", o.BatchSize)

	pass := func() {
		res, err := CleanupOnce(ctx, pool, o)
		switch {
		case err != nil && ctx.Err() != nil:
			// A cancel during the pass is part of the shutdown, not an
			// alarm.
		case err != nil:
			log.Warn("проход очистки удалённых доменов", "err", err)
		default:
			if res.BadHashes > 0 {
				// The poison pill is defused (ING-P2-4): the anomaly is
				// visible to the operator, the cleanup continues.
				log.Warn("удалены результаты с некорректным content_hash — refcount не менялся",
					"bad_hashes", res.BadHashes)
			}
			if res.Domains > 0 {
				log.Info("проход очистки удалённых доменов",
					"domains", res.Domains, "results", res.Results, "contents", res.Contents)
			}
			// The pass result — to the consumers (the T-099 metric):
			// after the log, before the staging cleanup — strictly the
			// CleanupOnce result.
			if o.OnPass != nil {
				o.OnPass(res)
			}
		}

		// The cleanup of orphaned staging batches (T-098) — in the same
		// loop pass: the first pass right after the start picks up the
		// rows orphaned by the death of the previous run (kill -9, a DB
		// crash). An error does not stop the loop — the next pass will
		// retry; a zero result is not logged.
		if rows, err := CleanupStaleBatches(ctx, pool, time.Now()); err != nil {
			if ctx.Err() == nil {
				log.Warn("зачистка осиротевших партий staging", "err", err)
			}
		} else if rows > 0 {
			log.Warn("удалены осиротевшие строки staging",
				"rows", rows, "older_than", StaleBatchAge.String())
		}
	}

	pass()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

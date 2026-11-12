// Pack files — the physical layer of the content store (spec 5.7, task
// T-042).
//
// The content.dir directory holds pack files pack-NNNNNN.pack: append-only
// containers of zstd-compressed blocks (the dictionary is T-043,
// CompressWithDict). Block metadata — PostgreSQL rows in
// contents(hash, pack_id, off, len, raw_len, refcount) (spec 6.1). The returned
// implementation satisfies ContentStore (T-041); the format and contracts
// are pinned in docs/content.md.
//
// pack_id is the ordinal number of the file, the pack registry is the set
// of files itself (each pack's header duplicates its id): there is no packs
// table in spec 6.1, and no migration introduces one. The newest pack is the
// maximum id in the directory.
//
// Write transactionality (spec 3.4): a block is appended to the pack and
// synced to disk (fsync) BEFORE the metadata row is committed. A crash
// between the steps leaves a garbage block without a row in the pack — it is
// invisible (Get goes through the metadata only) and is cleaned up by
// compaction (T-045); the reverse window "the row is durable, the bytes are
// gone" is closed: after a power loss a written row always has its bytes on
// disk (PostgreSQL in the production config commits asynchronously —
// synchronous_commit = off, the pack's dirty pages would sit in cache for
// minutes).
package content

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"crawler/internal/db"
)

// Pack file parameters. Rotation follows content.pack_size (the spec 5.7
// recommendation: 512 MB–1 GB; the config validates the bounds for the ops
// layer, T-006).
const (
	// DefaultPackSize — the rotation threshold with a zero PackOptions.PackSize:
	// 512 MiB, like the content.pack_size default.
	DefaultPackSize = 512 << 20

	// packMagic/packVersion — the pack file header (format v1).
	packMagic   = "CDPK"
	packVersion = 1

	// packHeaderLen — the v1 header length: magic[4] version[1] hdrLen[2]
	// packID[4] maxSize[8] dictID[4], all integers little-endian.
	packHeaderLen = 4 + 1 + 2 + 4 + 8 + 4

	// packLockFile — the inter-process lock file of the packs directory:
	// the http-worker and render-worker append content concurrently (spec 4.1).
	packLockFile = ".packs.lock"
)

// errPackClosed — write operations after Close: a programming error,
// not part of the ContentStore contract.
var errPackClosed = errors.New("content: хранилище паков закрыто")

// errReadOnly — writing to a store opened with PackOptions.ReadOnly:
// a read-only reader (crawlerctl domain --content) must change neither the
// FS nor the metadata; a programming error of the caller.
var errReadOnly = errors.New("content: хранилище паков открыто только для чтения")

// The refcount automaton of content (T-265, spec tasks/TZ-fsm.md §3 FR-11): the
// single source of truth about the states and transitions of a contents row —
// the reference counter of a content block in a pack. The formalization does
// not rewrite the transitions onto an FSM framework (TZ-fsm §1): they remain
// SQL statements with guard predicates (the project's proven model); this
// table is what all refcount writers (listed below) must be checked against.
// The adjacent automaton of a row's render state is
// internal/renderworker/worker.go (T-259); the domain state is
// internal/scheduler/checkstate.go (T-255).
//
// # States
//
//	S0 "absent"           there is no contents row (the content was never
//	                      written to the store); indistinguishable from S3/S4 by
//	                      observation — the difference is only in the history and
//	                      the garbage bytes of packs (S0 did not produce any);
//	S1 "live(n)", n ≥ 1   the row is live: the block is placed (pack_id, off, len,
//	                      raw_len), visible to consumers (Get, Put dedup,
//	                      the T-044 swap);
//	S2 "dead·tombstone"   reading of "dead" # 1: the row exists,
//	                      refcount = 0 — a tombstone until compaction
//	                      (T-045); Get/Delete/dedup do not see it (I2);
//	                      the block's bytes are dead, the space is held;
//	S3 "dead·removed"     reading of "dead" # 2: no row, the dead
//	                      bytes of the block remain in packs until a compaction
//	                      pass; arises straight from live —
//	                      retention (decref to 0 → DELETE in the same
//	                      transaction) — or from S2 — by the purge
//	                      of compaction;
//	S4 "deleted"          the terminal "clean" state: no row AND no dead
//	                      bytes (compaction removed the pack);
//	                      the only transition is to S1 (a Put of new content).
//
// The table of differences between the two readings of "dead" is in
// docs/content.md ("Two readings of dead", T-265); for consumers both mean
// "no content" (ErrNotFound / UPDATE 0).
//
// # Transitions: state × event × guards × target × side effects
//
//	| # | From | Event | Guards / statement | Target | Side effects | Writer |
//	|---|------|-------|--------------------|--------|--------------|--------|
//	| T1 | S0 | Put of new content (block spool → row commit) | INSERT without conflict | S1(1) | the block is appended and fsync'ed BEFORE the row (spec 3.4); Created = true; a batch — refcount = n copies (T-228) | pack.go sqlMetaPut (appendAndMeta), batch.go sqlMetaPutBatch |
//	| T2 | S1(n) | Put/swap of live content (dedup) | sqlMetaAddRef: refcount > 0 | S1(n+1) | no bytes written; Created = false; a batch — +n copies (sqlMetaAddRefBatch) | pack.go sqlMetaAddRef (Put dedup, refcount.go SwapRefTx +1), batch.go |
//	| T3 | S1(n) | race of concurrent Puts of one hash | ON CONFLICT DO UPDATE: refcount + 1 (+ EXCLUDED.refcount for a batch) | S1(n+1) | the loser's block is garbage for T-045; RETURNING (xmax = 0) = false | pack.go sqlMetaPut, batch.go sqlMetaPutBatch |
//	| T4 | S1(n), n ≥ 2 | Delete/swap removes a reference | sqlMetaDelRef: refcount > 0 | S1(n−1) | RowsAffected = 1; idempotency of redeliveries is the stages' transactions (spec 3.4), not repeated −1s | pack.go sqlMetaDelRef (Delete, refcount.go SwapRefTx −1) |
//	| T5 | S1(1) | Delete of the last reference | sqlMetaDelRef: refcount > 0 | S2 | the row remains with refcount = 0 (the block's space is cleaned by T-045); a repeated Delete — UPDATE 0 → ErrNotFound (I4) | pack.go sqlMetaDelRef |
//	| T6 | S2 | Put resurrection | dedup missed (UPDATE 0), the DO UPDATE conflict branch: refcount 0+1, EXCLUDED placement | S1(1) | a fresh block is appended and fsync'ed BEFORE the row; the placement is moved to it (EXCLUDED's bytes are guaranteed in place, the old one is dead bytes); Created = false (xmax ≠ 0) — a known discrepancy of the dedup share metric (spec 11.4 item 3) | pack.go sqlMetaPut, batch.go sqlMetaPutBatch |
//	| T7 | S0/S3/S4 | Put of content without a row | the INSERT branch (no conflict) | S1(1) | Created = true: resurrecting the "removed" reading is indistinguishable from new content (I3) | pack.go sqlMetaPut, batch.go sqlMetaPutBatch |
//	| T8 | S1(n) | mass removal by deleted-domain cleanup (aggregated by hashes) | cleanupUpdateSQL: GREATEST(refcount − h.n, 0) | S1(n−k); k = n → S2 | the row is NOT deleted (a tombstone — the space is cleaned by T-045); the clamp: a desync of k > n is forgiven with zero, a minus is impossible (I1) | ingest/cleanup.go (T-020) |
//	| T9 | S1(n) | reference removal by retention (aggregated) | retentionDecrefSQL GREATEST + retentionDeleteSQL (refcount <= 0) in one transaction | S1(n−k); k ≥ n → S3 directly, bypassing S2 | retention is a mass source of dead blocks: rows do not accumulate until pack compaction; the clamp as in T8 | retention.go (T-166) |
//	| T10 | S1(n) | a block is unreadable during compaction | sqlCompactCorrupt: SET refcount = 0, the refcount > 0 guard | S2 | the content cannot be removed otherwise (dedup sees a live row and writes no bytes); the integrity discrepancy goes into the pass error | compact.go (T-045) |
//	| T11 | S2 | compaction purges the pack's dead rows | sqlCompactDead (the batch list) / sqlCompactDeadPack (the final sweep of orphans): DELETE … WHERE pack_id = N AND refcount = 0 | S3; removing the pack file — S4 | the block dies together with the row; the file is removed after the live check (sqlCompactLive); a row resurrected during the pass is untouched (its placement was already moved by T6) | compact.go (T-045) |
//	| T12 | S1(n) without holders | the bench fixture-import utility | fixturesSweepContentsSQL: DELETE WHERE NOT EXISTS of references | S3 | the test bench only (T-243), production does not use it | cmd/crawlerctl/fixtures_import.go |
//
// Non-transitions (refcount unchanged): Get (sqlMetaGet — reading live rows);
// retention/compaction selections; moving live blocks by
// compaction (sqlCompactMove: S1 → S1, the placement changes —
// pack_id/off, not the state).
//
// # Invariants
//
//	I1  refcount ≥ 0 always, including a desync. Point statements use the
//	    refcount > 0 guard predicate (a removal only sees a live
//	    row: UPDATE 0 → ErrNotFound for Delete, ErrRefIntegrity for
//	    the T-044 swap — better to fail than to silently corrupt the counter);
//	    mass passes (retention, T-020 cleanup) use the
//	    GREATEST(refcount − n, 0) clamp: a desync is forgiven, the
//	    "0 = dead" invariant holds. Since migration 000019 (T-265) the invariant
//	    is duplicated by the schema CHECK contents_refcount_check (NOT VALID;
//	    VALIDATE — per the docs/schema.md regulation "CHECK invariants"):
//	    a minus is impossible for a writer or manual SQL alike.
//	I2  "0 = dead": all consumers of live rows filter refcount > 0
//	    (sqlMetaGet, sqlMetaAddRef); refcount = 0 and a missing row
//	    are indistinguishable to them — "dead" lives in two readings
//	    (S2/S3), the differences are in docs/content.md.
//	I3  a resurrection always writes a fresh block and moves the placement
//	    onto it: Put appends and fsyncs the block BEFORE committing the row
//	    (spec 3.4 "spool → commit"), EXCLUDED points at guaranteed-live
//	    bytes; the "removed" reading simply has no rows — the INSERT (T7)
//	    recreates it. The only reading difference observable by Put:
//	    Created = false (T6) versus true (T7).
//	I4  a double removal is forbidden: a repeated Delete of dead or missing
//	    content — UPDATE 0 → ErrNotFound (a symptom of an accounting error,
//	    not a normal path).
//	I5  removing a reference and the −1 of the counter are atomic in one
//	    transaction (the result write of SwapRefTx T-044/T-058, retention
//	    T-166, the T-020 cleanup — spec 3.4): a crash leaves neither a reference
//	    without a counter nor a counter without a reference.
//	I6  "+1 without an owner" is allowed (a safe leak): a redelivered Put
//	    after a failure adds an extra reference — the block stays
//	    live, the data is intact (the same choice as the statement order of
//	    SwapRef: a space leak is better than data loss).

// Metadata SQL of contents (spec 6.1). The exact texts are pinned by tests
// (pack_test.go, the ingest-test style on a fake pgx pool).
const (
	// Put deduplication: the live content is already in the store — a reference
	// only, no bytes written. A dead row (refcount = 0) is not resurrected by
	// this path: its block may have been compacted, a new Put writes
	// fresh bytes (sqlMetaPut).
	sqlMetaAddRef = "UPDATE contents SET refcount = refcount + 1 " +
		"WHERE hash = $1 AND refcount > 0"

	// Metadata of a new block. A hash conflict — a race of concurrent Puts of
	// one content or a dead row (refcount = 0): the loser adds a reference,
	// the placement is moved to the just-appended
	// block (EXCLUDED — its bytes are guaranteed in place, the block at the old
	// placement may have been compacted), the loser's block is
	// garbage for T-045. RETURNING (xmax = 0) tells an insert from an
	// update: an inserted row has no xmax set (0), a row updated by the
	// conflict has the xid of the current transaction; CommandTag for
	// INSERT … ON CONFLICT DO UPDATE is always "INSERT 0 1" and does not
	// tell the branches apart.
	sqlMetaPut = "INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) " +
		"VALUES ($1, $2, $3, $4, $5, 1) " +
		"ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + 1, " +
		"pack_id = EXCLUDED.pack_id, off = EXCLUDED.off, " +
		"len = EXCLUDED.len, raw_len = EXCLUDED.raw_len " +
		"RETURNING (xmax = 0) AS created"

	// Metadata for reading: dead rows (refcount = 0) are invisible —
	// the Delete contract (T-041): after the last reference the content is
	// unavailable.
	sqlMetaGet = "SELECT pack_id, off, len, raw_len FROM contents " +
		"WHERE hash = $1 AND refcount > 0"

	// Removing a reference by Delete: with refcount = 0 the row stays "dead" —
	// physical space reclamation is compaction's job (T-045).
	sqlMetaDelRef = "UPDATE contents SET refcount = refcount - 1 " +
		"WHERE hash = $1 AND refcount > 0"
)

// PackOptions — PackStore parameters from the content section of the
// configuration (T-006). A zero PackSize means DefaultPackSize; a zero
// MaxStoreBytes means no limit (the config always sets it, T-046).
type PackOptions struct {
	Dir           string // the pack-file directory (content.dir)
	PackSize      int64  // the rotation threshold, bytes (content.pack_size)
	MaxStoreBytes int64  // the limit of one content, bytes (content.max_store_bytes)
	// DiskFreePercent — the free-space threshold of the content directory, %
	// (content.disk_free_percent, spec 8): below the threshold Put returns
	// ErrStoreFull — a content write pause; 0 — the control is disabled
	// (the config always sets it, T-046).
	DiskFreePercent float64
	// OnStoreFull — the T-080 alert hook: typed events "space below the
	// threshold" from Put, ENOSPC, and DiskWatchLoop. nil — not set.
	OnStoreFull OnStoreFullFunc
	// ReadOnly — open the store read-only (readers without writes:
	// crawlerctl domain --content): neither the directory nor the lock file
	// is created — it works even without write permissions on content.dir,
	// and a typo in the directory does not create stray paths; Put/Delete/
	// beginAppend refuse with errReadOnly, Get reads via the existing packs
	// and metadata.
	ReadOnly bool
}

// PackStore — a ContentStore on pack files: blocks in pack-NNNNNN.pack,
// metadata and reference counters in PostgreSQL (contents). Put is serialized
// (the write-position mutex + the directory flock across processes), Get reads
// committed offsets in parallel without locks: packs are append-only.
type PackStore struct {
	pool db.DB
	opts PackOptions

	mu       sync.Mutex   // the write position and rotation (within the process)
	active   *packFile    // the pack to append to — always the newest (appendBlockLocked)
	lock     *os.File     // the packs directory flock (across processes)
	dirCache packDirCache // the newest pack with an mtime-based directory cache
	stat     SpaceChecker // the statfs source for space control (nil — OSStatfs; tests stub it)
	closed   bool
}

// compile-time: PackStore implements ContentStore (T-041).
var _ ContentStore = (*PackStore)(nil)

// packFile — a pack open for appending.
type packFile struct {
	id   int32
	f    packWriter
	size int64 // refreshed by Stat under the lock
}

// packWriter — a pack file as the writer needs it: a real *os.File or a
// test stub (a wrapper counting the group-commit fsyncs,
// T-102; the SpaceChecker style in disk.go).
type packWriter interface {
	Write(p []byte) (int, error)
	ReadAt(p []byte, off int64) (int, error)
	Stat() (fs.FileInfo, error)
	Sync() error
	Close() error
}

// packHeader — pack file metadata (v1).
type packHeader struct {
	id      int32  // the pack number = the ordinal number in the file name
	maxSize int64  // the rotation threshold at creation time (content.pack_size)
	dictID  uint32 // the zstd dictionary ID at creation time (0 — no dictionary)
}

// OpenPackStore opens the store in opts.Dir: the directory is created,
// and so is the lock file (fail-fast on a directory that is not writable);
// the packs themselves are scanned lazily on the first write. pool is the
// pgx pool (internal/db) holding the contents rows. PackOptions.ReadOnly opens
// a reader: without creating a directory or a lock (see the field).
func OpenPackStore(pool db.DB, opts PackOptions) (*PackStore, error) {
	if pool == nil {
		return nil, errors.New("content: пул БД не задан")
	}
	if opts.Dir == "" {
		return nil, errors.New("content: каталог паков не задан (content.dir)")
	}
	if opts.PackSize < 0 {
		return nil, fmt.Errorf("content: pack_size %d отрицателен", opts.PackSize)
	}
	if opts.PackSize == 0 {
		opts.PackSize = DefaultPackSize
	}
	if opts.MaxStoreBytes < 0 {
		return nil, fmt.Errorf("content: max_store_bytes %d отрицателен", opts.MaxStoreBytes)
	}
	if opts.ReadOnly {
		// A reader needs neither the directory nor the lock: the FS is not
		// modified, no write permissions are required. A missing directory is an
		// error: a typo in content.dir must not silently look like an empty
		// store.
		st, err := os.Stat(opts.Dir)
		if err != nil {
			return nil, fmt.Errorf("content: каталог паков %s: %w", opts.Dir, err)
		}
		if !st.IsDir() {
			return nil, fmt.Errorf("content: каталог паков %s: не каталог", opts.Dir)
		}
		return &PackStore{pool: pool, opts: opts, stat: OSStatfs{}}, nil
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("content: каталог паков %s: %w", opts.Dir, err)
	}
	// 0o660: the lock and the packs are opened for writing by several units of
	// the crawler group (http-worker, render-worker, analyzer — flock;
	// docs/deploy.md §3, "pack-file permissions 0660"); 0644 on the first
	// creator would cut off the others' write access in the systemd deploy
	// (a separate user per service).
	lock, err := os.OpenFile(filepath.Join(opts.Dir, packLockFile), os.O_CREATE|os.O_RDWR, 0o660)
	if err != nil {
		return nil, fmt.Errorf("content: файл блокировки %s: %w", filepath.Join(opts.Dir, packLockFile), err)
	}
	return &PackStore{pool: pool, opts: opts, lock: lock, stat: OSStatfs{}}, nil
}

// Dir — the store's pack-file directory (diagnostics, tests, services).
func (s *PackStore) Dir() string { return s.opts.Dir }

// Close closes the store's files; Get after Close keeps working
// (packs are opened for reading on demand), writing is an error.
func (s *PackStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.active != nil {
		if cerr := s.active.f.Close(); cerr != nil && err == nil {
			err = cerr
		}
		s.active = nil
	}
	// A ReadOnly store has no lock (s.lock == nil).
	if s.lock != nil {
		if cerr := s.lock.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// Put stores data (the contract is in store.go). Deduplication: the hash is
// already among the live contents rows — a reference is added, no bytes are
// written. New content: the block is appended to the pack (spooled), then the
// metadata is committed — a crash between the steps leaves an invisible
// garbage block.
func (s *PackStore) Put(ctx context.Context, data []byte) (PutResult, error) {
	if err := ctx.Err(); err != nil {
		return PutResult{}, err
	}
	if s.opts.ReadOnly {
		return PutResult{}, errReadOnly
	}
	if s.opts.MaxStoreBytes > 0 && int64(len(data)) > s.opts.MaxStoreBytes {
		return PutResult{}, fmt.Errorf("%w: %d байт при лимите %d",
			ErrTooLarge, len(data), s.opts.MaxStoreBytes)
	}
	h := HashOf(data)

	// 1) dedup: live content is a reference only.
	tag, err := s.pool.Exec(ctx, sqlMetaAddRef, h[:])
	if err != nil {
		return PutResult{}, fmt.Errorf("content: метаданные %s: %w", h, err)
	}
	if tag.RowsAffected() == 1 {
		return PutResult{Hash: h}, nil
	}

	// 1.5) the content write pause (spec 8, T-046): the directory's free space
	// below the threshold — the new block is not written, ErrStoreFull + an
	// alert event. The dedup above has already returned: references to live
	// content and reads keep working — no bytes are consumed.
	if err := s.pauseIfDiskFull(); err != nil {
		return PutResult{}, err
	}

	// 2) spooling the block into the pack and 3) committing the metadata —
	// under one acquisition of the mutex and flock (appendAndMeta): the block
	// and its row appear in the store atomically with respect to compaction
	// (T-045), which removes old packs under the same lock. The INSERT conflict
	// commentary is in appendAndMeta.
	block := CompressWithDict(data)
	created, err := s.appendAndMeta(ctx, h, block, len(data))
	if err != nil {
		// ENOSPC on a physical write — the last line of defense after the
		// threshold (wrapPackWriteErr): also an alert signal (spec 8).
		if errors.Is(err, ErrStoreFull) {
			s.fireStoreFull(s.enospcEvent())
		}
		return PutResult{}, err
	}
	return PutResult{Hash: h, Created: created}, nil
}

// appendAndMeta appends the block to the newest pack and commits the
// metadata row under one acquisition of the mutex and the directory flock
// (T-045): the row appears in the DB no later than the lock held during the
// block's append is released — compaction, taking flock next, cannot deem the
// block garbage and remove the pack before the row is committed. An error at
// any step leaves the block as garbage without a row — invisible, purged by
// compaction (the T-042 crash-window semantics preserved); the lock is held in
// the critical section for the duration of the DB query too — the price of a
// correct inter-process protocol, content writing is not a hot path (pages
// arrive from the network slower than a commit).
//
// The INSERT branch of sqlMetaPut — new content; the UPDATE branch of a
// conflict — a concurrent writer got there first (this call's block is
// garbage) or resurrection of a dead row (a fresh block appended, the
// placement moved onto it). Created comes from RETURNING (xmax = 0): true —
// an actual insert, false — an update. The resurrection branch yields false
// although bytes were written: for the dedup share metric (spec 11.4 item 3)
// this is a rare discrepancy, pinned in docs/content.md.
func (s *PackStore) appendAndMeta(ctx context.Context, h Hash, block []byte, rawLen int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, errPackClosed
	}
	if err := flockDir(s.lock); err != nil {
		return false, err
	}
	defer func() { unlockDir(s.lock) }()

	packID, off, err := s.appendBlockLocked(block)
	if err != nil {
		return false, err
	}
	// The block must survive to disk before the metadata row (spec 3.4,
	// "spool → commit"): fsync within the same flock acquisition closes the
	// "row durable, bytes gone" window on power loss (a repeated Put does not
	// fix such a record — dedup sees a live row). Content writing
	// is not a hot path (docs/content.md): the price is one fsync per block.
	if err := s.syncActiveLocked(); err != nil {
		return false, err
	}
	rows, err := s.pool.Query(ctx, sqlMetaPut, h[:], packID, off, int32(len(block)), int32(rawLen))
	if err != nil {
		return false, fmt.Errorf("content: метаданные %s: %w", h, err)
	}
	defer rows.Close()
	if !rows.Next() {
		err := rows.Err()
		if err == nil {
			err = errors.New("upsert не вернул строки RETURNING")
		}
		return false, fmt.Errorf("content: метаданные %s: %w", h, err)
	}
	var created bool
	if err := rows.Scan(&created); err != nil {
		return false, fmt.Errorf("content: метаданные %s: %w", h, err)
	}
	return created, nil
}

// Get returns content by the contents metadata: the block (pack_id, off, len)
// is read from the pack and decompressed. Dead rows (refcount = 0) and
// missing ones — ErrNotFound. An unreadable block with a live row —
// ErrCorrupted (see store.go).
func (s *PackStore) Get(ctx context.Context, hash Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	packID, off, clen, rawLen, err := s.meta(ctx, hash)
	if err != nil {
		return nil, err
	}
	block, err := readPackBlock(s.opts.Dir, packID, off, clen)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		// The pack is gone (ENOENT) although the row is live: the file was
		// removed under compaction between reading the row and opening the
		// pack — the row has already moved to a new place. We re-read the
		// metadata (at most once) and read via the fresh placement; without
		// the retry the reader would see a transient error out of the blue.
		if packID, off, clen, rawLen, err = s.meta(ctx, hash); err != nil {
			return nil, err
		}
		block, err = readPackBlock(s.opts.Dir, packID, off, clen)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrCorrupted, hash, err)
	}
	raw, err := DecompressWithDict(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: распаковка блока пака %d: %w", ErrCorrupted, hash, packID, err)
	}
	if int32(len(raw)) != rawLen {
		return nil, fmt.Errorf("%w: %s: блок пака %d повреждён: распаковано %d байт, в метаданных %d",
			ErrCorrupted, hash, packID, len(raw), rawLen)
	}
	return raw, nil
}

// meta — the metadata row of live content (sqlMetaGet): the block's
// placement. Missing/dead content — ErrNotFound.
func (s *PackStore) meta(ctx context.Context, hash Hash) (int32, int64, int32, int32, error) {
	rows, err := s.pool.Query(ctx, sqlMetaGet, hash[:])
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("content: метаданные %s: %w", hash, err)
	}
	defer rows.Close()
	var packID int32
	var off int64
	var clen, rawLen int32
	switch {
	case rows.Next():
		if err := rows.Scan(&packID, &off, &clen, &rawLen); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("content: метаданные %s: %w", hash, err)
		}
	case rows.Err() != nil:
		return 0, 0, 0, 0, fmt.Errorf("content: метаданные %s: %w", hash, rows.Err())
	default:
		return 0, 0, 0, 0, ErrNotFound
	}
	return packID, off, clen, rawLen, nil
}

// Delete removes one reference: refcount − 1, the row remains (at zero —
// "dead", invisible to Get). Nothing to remove — ErrNotFound.
func (s *PackStore) Delete(ctx context.Context, hash Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.opts.ReadOnly {
		return errReadOnly
	}
	tag, err := s.pool.Exec(ctx, sqlMetaDelRef, hash[:])
	if err != nil {
		return fmt.Errorf("content: метаданные %s: %w", hash, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// appendBatch — a batch of appends under one acquisition of the mutex and
// flock (C-11): compaction appends thousands of blocks of a pass —
// reopening files and a pair of flock syscalls per block would needlessly
// drive up the cost of a background task whose job is to spare the disk. One
// fsync per batch (finish) preserves the C-1 invariant: the bytes are on disk
// before the row-move transaction.
type appendBatch struct {
	s *PackStore
}

// beginAppend takes the batch's locks. Used by compaction
// (T-045); Put goes through appendAndMeta, which holds the same lock until the
// metadata is committed.
func (s *PackStore) beginAppend() (*appendBatch, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errPackClosed
	}
	if s.opts.ReadOnly {
		s.mu.Unlock()
		return nil, errReadOnly
	}
	if err := flockDir(s.lock); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	return &appendBatch{s: s}, nil
}

// append appends a block to the newest pack (the batch's mutex and flock are
// already held). An append error is not fatal for the batch: the caller must
// finish it with finish.
func (b *appendBatch) append(block []byte) (int32, int64, error) {
	return b.s.appendBlockLocked(block)
}

// finish syncs the active pack to disk (before the rows are committed — C-1)
// and releases the batch's locks.
func (b *appendBatch) finish() error {
	err := b.s.syncActiveLocked()
	unlockDir(b.s.lock)
	b.s.mu.Unlock()
	return err
}

// appendBlockLocked — the append core, called under the mutex and
// flock. It performs NO fsync: a batch of appends is synced by one
// syncActiveLocked (Put — before the row is committed, compaction — before
// the move transaction; syncActiveLocked), rotation — closeActiveLocked.
// The active pack is synchronized with the newest in the directory: writers
// append ONLY to the newest pack, all the others are closed for writing —
// the compaction precondition (T-045), which removes old packs: the set of
// rows of a non-newest pack is frozen the moment the next pack appears, late
// appends do not happen (in the previous scheme the active pack could lag
// behind the newest and the old pack stayed open for writing).
func (s *PackStore) appendBlockLocked(block []byte) (int32, int64, error) {
	maxID, err := s.newestPackIDLocked()
	if err != nil {
		return 0, 0, err
	}
	if maxID == 0 {
		maxID = 1 // an empty directory — pack number 1 is created
	}
	if s.active == nil || s.active.id != maxID {
		pf, err := s.openPackForWrite(maxID)
		if err != nil {
			return 0, 0, err
		}
		if s.active != nil {
			_ = s.active.f.Close()
		}
		s.active = pf
	} else {
		// Another process may have appended to the pack since the last call.
		st, err := s.active.f.Stat()
		if err != nil {
			return 0, 0, fmt.Errorf("content: пак %d: %w", s.active.id, err)
		}
		s.active.size = st.Size()
	}

	recLen := int64(4 + len(block))
	if s.active.size > packHeaderLen && s.active.size+recLen > s.opts.PackSize {
		if err := s.rotateLocked(recLen); err != nil {
			return 0, 0, err
		}
	}

	off := s.active.size + 4 // a record: [len u32 LE][block]; off — the start of the block
	buf := make([]byte, 4+len(block))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(block)))
	copy(buf[4:], block)
	if _, err := s.active.f.Write(buf); err != nil {
		return 0, 0, wrapPackWriteErr(err, s.active.id)
	}
	s.active.size += recLen
	return s.active.id, off, nil
}

// syncActiveLocked — fsync of the active pack: the bytes of appended blocks
// must survive to disk BEFORE the metadata rows are committed (C-1, spec 3.4
// "spool → commit"); called under the mutex and flock (Put — per block,
// compaction — one per batch, C-11).
func (s *PackStore) syncActiveLocked() error {
	if s.active == nil {
		return nil
	}
	if err := s.active.f.Sync(); err != nil {
		return wrapPackWriteErr(err, s.active.id)
	}
	return nil
}

// closeActiveLocked closes the active pack: unsynced bytes go to disk
// before Close (a rotation inside a compaction batch could otherwise close a
// pack with unsynced blocks), otherwise the batch would lose the C-1
// guarantee.
func (s *PackStore) closeActiveLocked() error {
	if err := s.syncActiveLocked(); err != nil {
		return err
	}
	return s.active.f.Close()
}

// packDirCache — the pack registry cache: the directory's mtime changes when
// pack files are created and removed but not when they are appended to, so
// cache freshness is checked with one os.Stat instead of a ReadDir on every
// write.
type packDirCache struct {
	valid   bool
	modTime time.Time
	maxID   int32
}

// newestPackIDLocked — the maximum pack id in the directory (0 — no packs)
// with an mtime-based directory cache; called under the mutex. Equal mtimes do
// not prove the absence of a new pack: the kernel updates mtime on a coarse
// timer (1–4 ms, coarser on some FSes), a pack created by another process may
// fit into the same granularity. So with a fresh cache the existence of the
// next id is checked (one stat is cheaper than a ReadDir): appends must go
// only to the newest pack — the compaction precondition (T-045).
func (s *PackStore) newestPackIDLocked() (int32, error) {
	// stat before ReadDir: events in between just invalidate the cache once more.
	st, err := os.Stat(s.opts.Dir)
	if err != nil {
		return 0, fmt.Errorf("content: каталог паков %s: %w", s.opts.Dir, err)
	}
	if s.dirCache.valid && !s.dirCache.modTime.Equal(st.ModTime()) {
		s.dirCache.valid = false // the pack registry changed — rebuild
	}
	if s.dirCache.valid {
		_, err := os.Stat(filepath.Join(s.opts.Dir, packFileName(s.dirCache.maxID+1)))
		switch {
		case err == nil:
			s.dirCache.valid = false // a pack newer than the cache was found — rebuild
		case errors.Is(err, fs.ErrNotExist):
			return s.dirCache.maxID, nil
		default:
			return 0, fmt.Errorf("content: каталог паков %s: %w", s.opts.Dir, err)
		}
	}
	maxID, err := s.scanPackIDs()
	if err != nil {
		return 0, err
	}
	s.dirCache = packDirCache{valid: true, modTime: st.ModTime(), maxID: maxID}
	return maxID, nil
}

// rotateLocked runs when the current pack is full: switch to the directory's
// newest pack (another process may have rotated earlier); if that one is
// full too — create the next one: pack_id grows monotonically, the name is
// allocated from the directory under the lock. The first block of a pack is
// always written, even over the threshold — rotation must make progress
// (content ≤ 16 MiB by the max_store_bytes validation, the threshold ≥ 1 MiB).
func (s *PackStore) rotateLocked(recLen int64) error {
	maxID, err := s.newestPackIDLocked()
	if err != nil {
		return err
	}
	if s.active.id != maxID {
		pf, err := s.openPackForWrite(maxID)
		if err != nil {
			return err
		}
		if err := s.closeActiveLocked(); err != nil {
			_ = pf.f.Close()
			return err
		}
		s.active = pf
	}
	if s.active.size <= packHeaderLen || s.active.size+recLen <= s.opts.PackSize {
		return nil // the newest pack fit the record
	}
	pf, err := s.createPackLocked(maxID + 1)
	if err != nil {
		return err
	}
	s.dirCache.valid = false // a new pack was created — the registry changed
	if err := s.closeActiveLocked(); err != nil {
		_ = pf.f.Close()
		return err
	}
	s.active = pf
	return nil
}

// createPackLocked creates a pack with a header. The file may exist empty
// (a crash after create before the header write) — the header is appended;
// incompatible contents are an error (the open file is closed: an fd leak on
// every rotation retry would feed the process EMFILE).
func (s *PackStore) createPackLocked(id int32) (*packFile, error) {
	pf, err := s.openPackForWrite(id)
	if err != nil {
		return nil, err
	}
	if pf.size != packHeaderLen {
		_ = pf.f.Close()
		return nil, fmt.Errorf("content: пак %d: файл уже содержит %d байт, ожидался только заголовок",
			id, pf.size)
	}
	return pf, nil
}

// openPackForWrite opens (or creates) a pack for appending and puts the
// header in order: an empty file gets a v1 header, a non-empty one is
// validated (the version and the id matching the file name).
func (s *PackStore) openPackForWrite(id int32) (*packFile, error) {
	path := filepath.Join(s.opts.Dir, packFileName(id))
	// O_APPEND: appends always go to the end of the file regardless of the fd
	// position — another process's size does not distort the write.
	// 0o660 — see the lock above: the writer units of the crawler group append
	// to the active pack (docs/deploy.md §3).
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o660)
	if err != nil {
		return nil, fmt.Errorf("content: пак %d: %w", id, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("content: пак %d: %w", id, err)
	}
	hdr := packHeader{id: id, maxSize: s.opts.PackSize, dictID: activeDictID()}
	if st.Size() < packHeaderLen {
		// An empty file (just created or a crash before the header) or a torn
		// header write on power loss/ENOSPC (C-7): the file cannot contain
		// records — the tail is truncated and the header is written from
		// scratch. Otherwise a "short" newest pack jams the writing of the whole
		// store, and a non-newest one — an eternal scan error. The end of the
		// file is offset 0, a regular Write is the header write (WriteAt on an
		// O_APPEND descriptor is forbidden in Go).
		if st.Size() > 0 {
			if err := f.Truncate(0); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("content: пак %d: %w", id, err)
			}
		}
		if _, err := f.Write(hdr.bytes()); err != nil {
			_ = f.Close()
			return nil, wrapPackWriteErr(err, id)
		}
		// The header and the directory record of the file must be on disk
		// before any metadata rows of this pack: without a directory sync a
		// created file may not survive a power loss together with its first
		// rows (C-1).
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, wrapPackWriteErr(err, id)
		}
		if err := syncDir(s.opts.Dir); err != nil {
			_ = f.Close()
			return nil, wrapPackWriteErr(err, id)
		}
		return &packFile{id: id, f: f, size: packHeaderLen}, nil
	}
	got, err := readPackHeader(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("content: пак %d: %w", id, err)
	}
	if got.id != id {
		_ = f.Close()
		return nil, fmt.Errorf("content: пак %d: заголовок называет пак %d", id, got.id)
	}
	return &packFile{id: id, f: f, size: st.Size()}, nil
}

// scanPackIDs returns the maximum pack id in the directory (0 — no packs).
func (s *PackStore) scanPackIDs() (int32, error) {
	ents, err := os.ReadDir(s.opts.Dir)
	if err != nil {
		return 0, fmt.Errorf("content: каталог паков %s: %w", s.opts.Dir, err)
	}
	var max int32
	for _, e := range ents {
		if id, ok := packIDFromName(e.Name()); ok && id > max {
			max = id
		}
	}
	return max, nil
}

// packFileName — the pack file name: pack-NNNNNN.pack (the ordinal number is
// the pack_id, the pack registry is the set of names plus the file header).
func packFileName(id int32) string {
	return fmt.Sprintf("pack-%06d.pack", id)
}

// packIDFromName parses a pack file name; foreign files (the dictionary, the
// lock, temporary ones) are rejected. Only the canonical name
// pack-NNNNNN.pack (packFileName) is accepted: "pack-1.pack" is a foreign
// file, otherwise one pack_id would get two files (the writer creates the
// canonical name next to a non-canonical one), and removal with rotation
// would confuse them.
func packIDFromName(name string) (int32, bool) {
	const prefix, suffix = "pack-", ".pack"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	n, err := strconv.Atoi(name[len(prefix) : len(name)-len(suffix)])
	if err != nil || n < 1 || int64(n) > 1<<31-1 {
		return 0, false
	}
	if name != packFileName(int32(n)) {
		return 0, false // a name without padding is not our pack
	}
	return int32(n), true
}

// bytes encodes the v1 header (little-endian).
func (h packHeader) bytes() []byte {
	b := make([]byte, 0, packHeaderLen)
	b = append(b, packMagic...)
	b = append(b, packVersion)
	b = binary.LittleEndian.AppendUint16(b, packHeaderLen)
	b = binary.LittleEndian.AppendUint32(b, uint32(h.id))
	b = binary.LittleEndian.AppendUint64(b, uint64(h.maxSize))
	b = binary.LittleEndian.AppendUint32(b, h.dictID)
	return b
}

// readPackHeader reads and parses a pack header.
func readPackHeader(f *os.File) (packHeader, error) {
	buf := make([]byte, packHeaderLen)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return packHeader{}, err
	}
	return parsePackHeader(buf)
}

// parsePackHeader parses a v1 header: the first packHeaderLen bytes with
// the v1 fields. hdrLen may exceed packHeaderLen — future versions
// append fields at the end, v1 does not read them (documented in
// docs/content.md); an hdrLen smaller than the mandatory v1 fields is
// corruption.
func parsePackHeader(b []byte) (packHeader, error) {
	bad := errors.New("заголовок пака нечитаем")
	if len(b) < packHeaderLen || string(b[:len(packMagic)]) != packMagic {
		return packHeader{}, bad
	}
	if b[4] != packVersion {
		return packHeader{}, fmt.Errorf("версия пака %d не поддерживается (поддерживается %d)", b[4], packVersion)
	}
	if hdrLen := binary.LittleEndian.Uint16(b[5:7]); hdrLen < packHeaderLen {
		return packHeader{}, bad
	}
	return packHeader{
		id:      int32(binary.LittleEndian.Uint32(b[7:11])),
		maxSize: int64(binary.LittleEndian.Uint64(b[11:19])),
		dictID:  binary.LittleEndian.Uint32(b[19:23]),
	}, nil
}

// readPackBlock opens the pack and reads clen bytes of the block at offset
// off (Get's point reads). Compaction reads the pack once per pass —
// readPackBlockAt on the open file (C-11).
func readPackBlock(dir string, packID int32, off int64, clen int32) ([]byte, error) {
	f, err := os.Open(filepath.Join(dir, packFileName(packID)))
	if err != nil {
		return nil, fmt.Errorf("content: пак %d: %w", packID, err)
	}
	defer func() { _ = f.Close() }() // read-only
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("content: пак %d: %w", packID, err)
	}
	return readPackBlockAt(f, st.Size(), packID, off, clen)
}

// readPackBlockAt reads clen bytes of a block from an open pack at offset
// off; size is the file size for validating the placement. The placement is
// checked before the allocation: len comes from the DB (contents.len), a
// corrupted row must not order a buffer of up to 2 GiB (C-14) — the same
// check as in the CompactScan scan.
func readPackBlockAt(f *os.File, size int64, packID int32, off int64, clen int32) ([]byte, error) {
	if clen < 0 || off < packHeaderLen+4 || off+int64(clen) > size {
		return nil, fmt.Errorf("content: пак %d, смещение %d, длина %d: блок вне файла (%d байт)",
			packID, off, clen, size)
	}
	buf := make([]byte, clen)
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("content: пак %d, смещение %d: %w", packID, off, err)
	}
	return buf, nil
}

// syncDir syncs the directory: the record of a file's creation/removal
// (a new pack's header, compaction's unlink) must survive to disk
// together with the file's contents.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// wrapPackWriteErr classifies pack write errors: out of disk space —
// ErrStoreFull (spec 8; the proactive threshold and write pause — T-046),
// the original error is kept for errors.Is.
func wrapPackWriteErr(err error, packID int32) error {
	if errors.Is(err, syscall.ENOSPC) {
		return fmt.Errorf("%w: пак %d: %w", ErrStoreFull, packID, err)
	}
	return fmt.Errorf("content: запись пака %d: %w", packID, err)
}

// flockDir/unlockDir — acquiring/releasing the inter-process lock of the
// packs directory: writers (T-042) and compaction (T-045).
func flockDir(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("content: блокировка каталога паков: %w", err)
	}
	return nil
}

func unlockDir(f *os.File) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// activeDictID — the ID of the package's active dictionary (0 — no
// dictionary) for the header of a pack being created.
func activeDictID() uint32 {
	dictMu.RLock()
	defer dictMu.RUnlock()
	if activeD == nil {
		return 0
	}
	return activeD.ID()
}

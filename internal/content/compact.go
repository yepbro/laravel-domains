// Background compaction of pack files (T-045; spec 5.7, 13.2).
//
// A pack is append-only: Delete removes references (refcount → 0, a "dead"
// row, T-042), rewrites move placements to fresh blocks, and a crash
// between appending a block and committing metadata leaves a garbage
// block without a row. All of this accumulates dead bytes in packs;
// compaction rewrites the live blocks into new packs and removes the old
// file when the live data share falls below the threshold (spec 5.7:
// "background compaction of packs when the live data share is < 50%";
// the threshold and interval are up to the developers, spec 13.2).
//
// The repair procedure for one pack (spec 3.4, "append → commit →
// delete"):
//
//   - only packs closed for writing are compacted — all but the
//     newest: writers append exclusively to the newest pack and commit
//     the metadata row without releasing flock (T-042, refined here),
//     so the set of rows of a non-newest pack is frozen;
//   - live blocks are rewritten into the newest pack via the regular write
//     path (the appendBatch batch: mutex + flock, O_APPEND, one fsync per
//     batch — C-11) — the old pack is not touched;
//   - a transaction (in batches, short ones — the T-020 style) moves the
//     placements of live rows to the new blocks and deletes the pack's dead
//     rows. Before the commit the old pack is intact — a crash leaves the
//     copied blocks as garbage, reads go through the old placements;
//   - after the commit the old pack's file is removed (under flock, after
//     the control checks). A crash between the commit and the removal leaves
//     a file of pure garbage — the next pass sees a live share of 0 and
//     removes it.
//
// A repeated pass is safe: what was moved is not moved twice (the
// pack_id guard in UPDATE), dead rows are deleted with the refcount = 0
// guard. The space taken by the store is the sum of the pack-file sizes of
// the directory, there is no separate counter: after the old pack is
// removed it honestly decreases (CompactResult.FreedBytes; the proactive
// fill threshold — T-046).
package content

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
)

const (
	// DefaultCompactThreshold — the live data share threshold: a pack is
	// compacted when the share is strictly below (spec 5.7 — 50%).
	DefaultCompactThreshold = 0.5

	// DefaultCompactInterval — the interval of CompactLoop background
	// passes. The spec does not set the pace (13.2): compaction is in no
	// hurry, space is returned in the background; an hour keeps disk I/O from
	// running after every deletion batch and still catches up with the tails
	// overnight.
	DefaultCompactInterval = time.Hour

	// compactChunk — metadata rows per repair transaction: short
	// transactions and bounded memory (spec 3.5); a crash is replayed from
	// the next batch.
	compactChunk = 1000
)

// Compaction SQL. The exact texts are pinned by tests
// (compact_test.go, the pack_test/ingest-test style).
const (
	// The rows of the pack — live and dead together; ordered by offset —
	// a linear read of the source pack while copying.
	sqlCompactRows = "SELECT hash, off, len, refcount FROM contents " +
		"WHERE pack_id = $1 ORDER BY off"

	// Moving the placements of live rows to the new blocks (len/raw_len are
	// not changed — the blocks are copied verbatim). The pack_id guard and
	// refcount > 0: a row that died or moved since it was read is not
	// moved — a dead one stays "dead" (invisible), its block will be
	// reclaimed by the next pass.
	sqlCompactMove = "UPDATE contents SET pack_id = v.pack_id, off = v.off " +
		"FROM unnest($1::bytea[], $2::int4[], $3::int8[]) AS v(hash, pack_id, off) " +
		"WHERE contents.hash = v.hash AND contents.pack_id = $4 " +
		"AND contents.refcount > 0"

	// Deleting dead rows pointing into the compacted pack:
	// the space under them is freed only here (docs/content.md).
	// The refcount = 0 guard — a row resurrected during the pass keeps
	// living with its fresh block.
	sqlCompactDead = "DELETE FROM contents " +
		"USING unnest($1::bytea[]) AS h(hash) " +
		"WHERE contents.hash = h.hash AND contents.pack_id = $2 " +
		"AND contents.refcount = 0"

	// Final sweep of the pack's dead rows before the file is removed:
	// a row that died between the read and the repair transaction got into
	// neither the move (the refcount > 0 guard) nor the deletion by the
	// batch list (it was read as live) — after the pack's unlink it would
	// stay in contents forever, and the next pass would no longer find its
	// pack. Here all of the pack's rows are dead (the live ones were moved in
	// batches above): the refcount = 0 guard will not touch a resurrected
	// row — its placement was already moved to the newest pack (sqlMetaPut).
	sqlCompactDeadPack = "DELETE FROM contents " +
		"WHERE pack_id = $1 AND refcount = 0"

	// Control before removing the file: no live rows left for the pack.
	// The T-042/T-045 invariant (rows appear only under flock together with
	// the block, and blocks only into the newest pack) makes the answer
	// immutable for a non-newest pack; the query is a guard against invariant
	// violations.
	sqlCompactLive = "SELECT count(*) FROM contents " +
		"WHERE pack_id = $1 AND refcount > 0"

	// The number of all rows of the pack (live and dead) — telling an
	// "empty" pack without a header (no rows — a garbage file) apart from
	// corruption (rows pointing into a file without a header).
	sqlCompactAll = "SELECT count(*) FROM contents WHERE pack_id = $1"

	// Marking a dead row with an unreadable block (loss/corruption of bytes):
	// a repeated Put does not fix it (dedup sees a live row), and compaction
	// without the marker could neither move the block nor remove the pack — a
	// "poisonous" pack would block space reclamation forever. The pack_id
	// guard (the row has not moved) and refcount > 0 (an already dead one is
	// not needed).
	sqlCompactCorrupt = "UPDATE contents SET refcount = 0 " +
		"WHERE hash = $1 AND pack_id = $2 AND refcount > 0"
)

// errCompactNewest — an attempt to compact the newest pack: it is open for
// writing (T-042), only closed packs are compacted.
var errCompactNewest = errors.New("content: новейший пак не компактифицируется")

// CompactDB — a pool for compaction: a db.DB from which a transaction can be
// started (moving placements and deleting dead rows are atomic).
// *pgxpool.Pool satisfies it; the ingest.CleanupDB style (T-020).
type CompactDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool fits the interface.
var _ CompactDB = (*pgxpool.Pool)(nil)

// CompactOptions — compaction parameters; zero values are replaced with
// defaults (withDefaults, the T-020 style).
type CompactOptions struct {
	// Interval — the period of CompactLoop passes (DefaultCompactInterval);
	// a one-off CompactOnce is not affected.
	Interval time.Duration
	// Threshold — the live data share threshold: a pack is compacted when
	// the share is strictly below (DefaultCompactThreshold, spec 5.7).
	Threshold float64
	// Log — the logger of the CompactLoop cycle; nil — records suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
}

// withDefaults replaces invalid zeros with the default values;
// a Threshold outside (0, 1] makes no sense as a share — also a default.
func (o CompactOptions) withDefaults() CompactOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultCompactInterval
	}
	if o.Threshold <= 0 || o.Threshold > 1 {
		o.Threshold = DefaultCompactThreshold
	}
	return o
}

// CompactInfo — the result of the CompactScan pack analysis.
type CompactInfo struct {
	PackID int32
	// Size — the size of the pack's file; DataBytes — the space of records
	// without the header.
	Size      int64
	DataBytes int64
	// LiveBytes — the space of live blocks (rows with refcount > 0, length
	// prefixes included): what compaction will rewrite.
	LiveBytes int64
	// DeadBytes — the space of the blocks of dead rows (refcount = 0).
	DeadBytes int64
	// GarbageBytes — the remaining space: blocks without a metadata row
	// (the T-042 crash window, losers of concurrent Puts) and the unterminated
	// tail.
	GarbageBytes int64
	LiveBlocks   int64
	DeadBlocks   int64
	// GarbageBlocks — whole records without a row; the bytes of an
	// unterminated tail count into GarbageBytes but do not count as blocks.
	GarbageBlocks int64
	// Truncated — the traversal stopped at an unterminated record in the tail
	// (a write crash, docs/content.md).
	Truncated bool
	// Ratio — the live data share: LiveBytes / DataBytes (0 when empty).
	Ratio float64
}

// CompactResult — the result of compacting one pack by CompactPack.
type CompactResult struct {
	PackID       int32
	CopiedBlocks int   // rewritten live blocks
	CopiedBytes  int64 // their space in the new packs (with length prefixes)
	Tombstones   int64 // deleted dead rows of contents
	Removed      bool  // the old pack's file was removed
	FreedBytes   int64 // space freed (the size of the removed file)
	// Corrupted — live rows with unreadable blocks: marked dead,
	// the pack's compaction continued; the integrity discrepancy is in the
	// CompactPack error (the pass's Warn), the details are in its text.
	Corrupted int64
}

// CompactStats — the result of one CompactOnce pass.
type CompactStats struct {
	Packs      int   // packs examined (except the newest)
	Compacted  int   // rewritten and removed
	FreedBytes int64 // space freed in total
}

// CompactScan analyzes a pack: live/dead/garbage bytes by the contents rows
// (pack_id, refcount) and a traversal of the file's records — the T-042
// format allows walking a pack by length prefixes. The rows are read from
// the DB before the file traversal: a block's bytes are always older than
// its row ("spool → commit", T-042), so there are no contradictions, and
// blocks without a row are garbage. The scan changes nothing and holds no
// locks: packs are append-only, and the rows of a non-newest pack are frozen
// (T-042).
func (s *PackStore) CompactScan(ctx context.Context, packID int32) (CompactInfo, error) {
	info := CompactInfo{PackID: packID}
	path := filepath.Join(s.opts.Dir, packFileName(packID))
	st, err := os.Stat(path)
	if err != nil {
		return info, fmt.Errorf("content: скан пака %d: %w", packID, err)
	}
	if st.Size() < packHeaderLen {
		// A file shorter than the header (a torn header write on failure, C-7):
		// it cannot contain a single whole record. There should be no contents
		// rows for such a pack — an "empty" pack (live share 0) is removed by
		// the pass instead of an eternal scan error; rows on the pack mean
		// corruption.
		n, err := s.allContents(ctx, packID)
		if err != nil {
			return info, fmt.Errorf("content: скан пака %d: %w", packID, err)
		}
		if n > 0 {
			return info, fmt.Errorf("content: скан пака %d: файл %d байт короче заголовка при %d строках contents",
				packID, st.Size(), n)
		}
		info.Size = st.Size()
		return info, nil
	}
	info.Size = st.Size()
	info.DataBytes = st.Size() - packHeaderLen

	f, err := os.Open(path)
	if err != nil {
		return info, fmt.Errorf("content: скан пака %d: %w", packID, err)
	}
	defer func() { _ = f.Close() }() // read-only

	rows, err := s.pool.Query(ctx, sqlCompactRows, packID)
	if err != nil {
		return info, fmt.Errorf("content: скан пака %d: метаданные: %w", packID, err)
	}
	defer rows.Close()
	var (
		liveBytes, deadBytes   int64
		liveBlocks, deadBlocks int64
		prefix                 [4]byte
	)
	for rows.Next() {
		var (
			hash       []byte
			off        int64
			clen, refc int32
		)
		if err := rows.Scan(&hash, &off, &clen, &refc); err != nil {
			return info, fmt.Errorf("content: скан пака %d: чтение метаданных: %w", packID, err)
		}
		if len(hash) != HashLen {
			return info, fmt.Errorf("content: скан пака %d: хэш %x: %d октетов, ожидается %d",
				packID, hash, len(hash), HashLen)
		}
		// The row must point at a whole record of the file: after the
		// header, within the file, with a matching length prefix.
		if off < packHeaderLen+4 || off+int64(clen) > st.Size() {
			return info, fmt.Errorf("content: скан пака %d: %x: блок [%d, %d) вне файла (%d байт)",
				packID, hash, off, off+int64(clen), st.Size())
		}
		if _, err := f.ReadAt(prefix[:], off-4); err != nil {
			return info, fmt.Errorf("content: скан пака %d: %x: префикс длины: %w", packID, hash, err)
		}
		if got := int32(binary.LittleEndian.Uint32(prefix[:])); got != clen {
			return info, fmt.Errorf("content: скан пака %d: %x: префикс длины %d не сходится с len %d",
				packID, hash, got, clen)
		}
		if refc > 0 {
			liveBlocks++
			liveBytes += 4 + int64(clen)
		} else {
			deadBlocks++
			deadBytes += 4 + int64(clen)
		}
	}
	if err := rows.Err(); err != nil {
		return info, fmt.Errorf("content: скан пака %d: метаданные: %w", packID, err)
	}

	// Traversing the records: the header, then [u32 len][block] until the
	// end. A tail that does not add up to a whole record is garbage bytes (a
	// write crash), not counted as a block.
	var (
		walked int64
		pos    = int64(packHeaderLen)
	)
	for pos < st.Size() {
		if _, err := f.ReadAt(prefix[:], pos); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return info, fmt.Errorf("content: скан пака %d: обход записей: %w", packID, err)
			}
			info.Truncated = true
			break
		}
		recLen := int64(binary.LittleEndian.Uint32(prefix[:]))
		if pos+4+recLen > st.Size() {
			info.Truncated = true
			break
		}
		pos += 4 + recLen
		walked++
	}

	info.LiveBytes = liveBytes
	info.DeadBytes = deadBytes
	info.LiveBlocks = liveBlocks
	info.DeadBlocks = deadBlocks
	info.GarbageBytes = info.DataBytes - liveBytes - deadBytes
	info.GarbageBlocks = walked - liveBlocks - deadBlocks
	// Negative garbage — the rows overlap each other: the metadata is
	// corrupted, such a pack cannot be compacted.
	if info.GarbageBytes < 0 || info.GarbageBlocks < 0 {
		return CompactInfo{PackID: packID}, fmt.Errorf(
			"content: скан пака %d: строки перекрывают друг друга (живые %d, мёртвые %d байт при данных %d)",
			packID, liveBytes, deadBytes, info.DataBytes)
	}
	if info.DataBytes > 0 {
		info.Ratio = float64(liveBytes) / float64(info.DataBytes)
	}
	return info, nil
}

// compactLiveRow — a live row of the pack for rewriting.
type compactLiveRow struct {
	hash Hash
	off  int64
	clen int32
}

// CompactPack compacts one pack: live blocks are rewritten into the newest
// pack (the old file is not touched), transactions in compactChunk batches
// move the placements of live rows and delete dead rows, after which the
// pack's file is removed (the control and unlink under flock).
// The details and crash safety are in the file comment.
func (s *PackStore) CompactPack(ctx context.Context, packID int32) (CompactResult, error) {
	cdb, ok := s.pool.(CompactDB)
	if !ok {
		return CompactResult{}, errors.New("content: компактификация: пул БД не поддерживает транзакции")
	}
	if err := s.checkNotNewest(packID); err != nil {
		return CompactResult{}, err
	}
	path := filepath.Join(s.opts.Dir, packFileName(packID))
	st, err := os.Stat(path)
	if err != nil {
		return CompactResult{}, fmt.Errorf("content: компактификация пака %d: %w", packID, err)
	}
	size := st.Size()
	// A pack shorter than the header is legitimate to remove (the C-7 scan
	// sees it as "empty"): it cannot have rows (the scan would fail), the
	// flush goes through empty, the file is removed. No explicit error is
	// needed here. The source pack is opened once per pass (C-11): blocks are
	// read with ReadAt by offsets — no open/close per block.
	src, err := os.Open(path)
	if err != nil {
		return CompactResult{}, fmt.Errorf("content: компактификация пака %d: %w", packID, err)
	}
	defer func() { _ = src.Close() }() // read-only

	rows, err := s.pool.Query(ctx, sqlCompactRows, packID)
	if err != nil {
		return CompactResult{}, fmt.Errorf("content: компактификация пака %d: метаданные: %w", packID, err)
	}
	defer rows.Close()

	res := CompactResult{PackID: packID}
	var (
		live    []compactLiveRow
		dead    [][]byte
		corrupt []error
	)
	// flush rewrites the accumulated batch of live blocks into the newest
	// pack and commits the transaction. A failure before Commit leaves the
	// copied blocks as garbage, the rows are untouched — a repeated pass
	// replays the batch. Appending happens as one batch under one acquisition
	// of the mutex and flock and one fsync (C-11); the batch's bytes are on
	// disk before the transaction (C-1).
	flush := func() error {
		if len(live) == 0 && len(dead) == 0 {
			return nil
		}
		hashes := make([][]byte, 0, len(live))
		packs := make([]int32, 0, len(live))
		offs := make([]int64, 0, len(live))
		var corruptHashes [][]byte
		if len(live) > 0 {
			batch, err := s.beginAppend()
			if err != nil {
				return err
			}
			for _, r := range live {
				block, err := readPackBlockAt(src, size, packID, r.off, r.clen)
				if err != nil {
					// The block is unavailable: neither reading nor a repeated
					// Put will restore this content (dedup sees a live row).
					// The row is marked dead in the same transaction — the
					// pack stays compactable, the space is freed; the
					// integrity discrepancy goes into the pass result (the
					// cycle's Warn).
					corrupt = append(corrupt, fmt.Errorf(
						"content: компактификация пака %d: %x: блок нечитаем, строка помечена мёртвой: %w",
						packID, r.hash, err))
					corruptHashes = append(corruptHashes, r.hash[:])
					res.Corrupted++
					continue
				}
				newPack, newOff, err := batch.append(block)
				if err != nil {
					if ferr := batch.finish(); ferr != nil {
						err = errors.Join(err, ferr)
					}
					return err
				}
				hashes = append(hashes, r.hash[:])
				packs = append(packs, newPack)
				offs = append(offs, newOff)
				res.CopiedBlocks++
				res.CopiedBytes += 4 + int64(r.clen)
			}
			if err := batch.finish(); err != nil {
				return err
			}
		}
		tx, err := cdb.Begin(ctx)
		if err != nil {
			return fmt.Errorf("content: компактификация пака %d: транзакция: %w", packID, err)
		}
		// Rollback unconditionally: after Commit — a no-op (the T-020 style).
		defer func() { _ = tx.Rollback(ctx) }()
		if len(corruptHashes) > 0 {
			// Before sqlCompactDead: the marked row is deleted by the same
			// transaction (the refcount = 0 guard will converge after the
			// marking).
			for _, hb := range corruptHashes {
				if _, err := tx.Exec(ctx, sqlCompactCorrupt, hb, packID); err != nil {
					return fmt.Errorf("content: компактификация пака %d: пометка нечитаемого блока: %w", packID, err)
				}
			}
			dead = append(dead, corruptHashes...)
		}
		if len(hashes) > 0 {
			if _, err := tx.Exec(ctx, sqlCompactMove, hashes, packs, offs, packID); err != nil {
				return fmt.Errorf("content: компактификация пака %d: перенос размещений: %w", packID, err)
			}
		}
		if len(dead) > 0 {
			tag, err := tx.Exec(ctx, sqlCompactDead, dead, packID)
			if err != nil {
				return fmt.Errorf("content: компактификация пака %d: удаление мёртвых строк: %w", packID, err)
			}
			res.Tombstones += tag.RowsAffected()
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("content: компактификация пака %d: фиксация: %w", packID, err)
		}
		live, dead = live[:0], dead[:0]
		return nil
	}

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		var (
			hash       []byte
			off        int64
			clen, refc int32
		)
		if err := rows.Scan(&hash, &off, &clen, &refc); err != nil {
			return res, fmt.Errorf("content: компактификация пака %d: чтение метаданных: %w", packID, err)
		}
		if len(hash) != HashLen {
			return res, fmt.Errorf("content: компактификация пака %d: хэш %x: %d октетов, ожидается %d",
				packID, hash, len(hash), HashLen)
		}
		var h Hash
		copy(h[:], hash)
		if refc > 0 {
			live = append(live, compactLiveRow{hash: h, off: off, clen: clen})
		} else {
			dead = append(dead, h[:])
		}
		if len(live)+len(dead) >= compactChunk {
			if err := flush(); err != nil {
				return res, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("content: компактификация пака %d: метаданные: %w", packID, err)
	}
	if err := flush(); err != nil {
		return res, err
	}

	// Final sweep of the pack's dead rows (C-13): see sqlCompactDeadPack.
	tag, err := s.pool.Exec(ctx, sqlCompactDeadPack, packID)
	if err != nil {
		return res, fmt.Errorf("content: компактификация пака %d: зачистка мёртвых строк: %w", packID, err)
	}
	res.Tombstones += tag.RowsAffected()

	// Control before the removal: no live rows left for the pack. A
	// non-newest pack gets no new rows (T-042/T-045), the query is a guard;
	// it runs before the locks are taken so as not to hold flock during the
	// DB query.
	n, err := s.liveContents(ctx, packID)
	if err != nil {
		return res, err
	}
	if n > 0 {
		return res, fmt.Errorf("content: компактификация пака %d: живых строк %d — файл не удаляется",
			packID, n)
	}

	// Removing the file: the size must not have changed (nobody writes into
	// the pack), the unlink happens under the mutex and flock, like all file
	// work.
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := flockDir(s.lock); err != nil {
		return res, err
	}
	defer func() { unlockDir(s.lock) }()
	st, err = os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Another pass got to remove the file first (double compaction).
			res.Removed = true
			res.FreedBytes = size
			return res, nil
		}
		return res, fmt.Errorf("content: компактификация пака %d: %w", packID, err)
	}
	if st.Size() != size {
		return res, fmt.Errorf(
			"content: компактификация пака %d: файл изменился с момента скана (%d → %d байт) — удаление пропущено",
			packID, size, st.Size())
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return res, fmt.Errorf("content: компактификация пака %d: удаление: %w", packID, err)
	}
	res.Removed = true
	res.FreedBytes = size
	// The pack is compacted and removed; integrity discrepancies (unreadable
	// blocks) go into the resulting error, the pass does not lose them.
	return res, errors.Join(corrupt...)
}

// checkNotNewest — the compaction precondition: the pack is closed for
// writing. The newest is determined from the directory under the mutex and
// flock — the same acquisition in which writers allocate ids of new packs.
func (s *PackStore) checkNotNewest(packID int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errPackClosed
	}
	if err := flockDir(s.lock); err != nil {
		return err
	}
	defer func() { unlockDir(s.lock) }()
	maxID, err := s.scanPackIDs()
	if err != nil {
		return err
	}
	switch {
	case packID == maxID:
		return errCompactNewest
	case packID > maxID:
		return fmt.Errorf("content: компактификация: пак %d не существует (новейший %d)", packID, maxID)
	}
	return nil
}

// liveContents — the number of live rows pointing into the pack (the control
// before removing the file).
func (s *PackStore) liveContents(ctx context.Context, packID int32) (int64, error) {
	rows, err := s.pool.Query(ctx, sqlCompactLive, packID)
	if err != nil {
		return 0, fmt.Errorf("content: контроль живых строк пака %d: %w", packID, err)
	}
	defer rows.Close()
	var n int64
	switch {
	case rows.Next():
		if err := rows.Scan(&n); err != nil {
			return 0, fmt.Errorf("content: контроль живых строк пака %d: %w", packID, err)
		}
	case rows.Err() != nil:
		return 0, fmt.Errorf("content: контроль живых строк пака %d: %w", packID, rows.Err())
	}
	return n, nil
}

// allContents — the number of all rows (live and dead) pointing into the
// pack: the scan of an "empty" pack without a header (C-7).
func (s *PackStore) allContents(ctx context.Context, packID int32) (int64, error) {
	rows, err := s.pool.Query(ctx, sqlCompactAll, packID)
	if err != nil {
		return 0, fmt.Errorf("content: контроль строк пака %d: %w", packID, err)
	}
	defer rows.Close()
	var n int64
	switch {
	case rows.Next():
		if err := rows.Scan(&n); err != nil {
			return 0, fmt.Errorf("content: контроль строк пака %d: %w", packID, err)
		}
	case rows.Err() != nil:
		return 0, fmt.Errorf("content: контроль строк пака %d: %w", packID, rows.Err())
	}
	return n, nil
}

// compactPackList — the ids of the directory's packs in ascending order and
// the newest (0 — no packs). The pack registry is the set of files itself
// (T-042).
func compactPackList(dir string) ([]int32, int32, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("content: каталог паков %s: %w", dir, err)
	}
	var ids []int32
	for _, e := range ents {
		if id, ok := packIDFromName(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	var newest int32
	if len(ids) > 0 {
		newest = ids[len(ids)-1]
	}
	return ids, newest, nil
}

// CompactOnce — one pass: scan all packs except the newest, repair those
// whose live data share is below the threshold (strictly, spec 5.7). An
// error of one pack does not stop the pass — it is collected into the
// resulting error (the calling cycle logs a Warn), the other packs are
// processed.
func CompactOnce(ctx context.Context, st *PackStore, opts CompactOptions) (CompactStats, error) {
	if st == nil {
		return CompactStats{}, errors.New("content: компактификация: хранилище не задано")
	}
	o := opts.withDefaults()
	ids, newest, err := compactPackList(st.opts.Dir)
	if err != nil {
		return CompactStats{}, err
	}
	var (
		stats CompactStats
		errs  []error
	)
	for _, id := range ids {
		if id == newest {
			break // the newest is closed to compaction
		}
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		stats.Packs++
		info, err := st.CompactScan(ctx, id)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if info.Ratio >= o.Threshold {
			continue
		}
		if info.LiveBytes > 0 {
			// The "old + new" space peak (C-17): copying live blocks takes
			// space on top of the not-yet-removed pack — on a volume below
			// the write-pause threshold the pass would run into ENOSPC
			// halfway. The postponed pack will be taken by the next pass when
			// the space returns; a pack without live blocks copies nothing
			// and is processed on any disk — removing garbage is itself the
			// path to freeing space.
			if _, full, derr := st.diskFull(); derr != nil {
				errs = append(errs, derr)
				continue
			} else if full {
				errs = append(errs, fmt.Errorf(
					"content: компактификация пака %d отложена: свободное место ниже порога, живые байты (%d) копировались бы поверх старого пака",
					id, info.LiveBytes))
				continue
			}
		}
		res, err := st.CompactPack(ctx, id)
		// A pack can be compacted and removed even on error (integrity
		// discrepancies of unreadable blocks are collected into it) — the
		// freed space and the repair are accounted for either way.
		if res.Removed {
			stats.Compacted++
			stats.FreedBytes += res.FreedBytes
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}
	return stats, errors.Join(errs...)
}

// CompactLoop — background compaction (spec 5.7): CompactOnce passes on a
// ticker (the first one immediately: draining what accumulated during idle)
// until ctx is cancelled; returning nil is the OnRun contract of the service
// framework (T-012; the RecheckDaily and ingest.CleanupDeleted style). A pass
// error does not stop the cycle: Warn, the next pass will retry — the pass is
// idempotent (spec 3.4).
func CompactLoop(ctx context.Context, st *PackStore, opts CompactOptions) error {
	if st == nil {
		return errors.New("content: компактификация: хранилище не задано")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("фоновая компактификация паков",
		"interval", o.Interval.String(),
		"threshold", o.Threshold)

	pass := func() {
		stats, err := CompactOnce(ctx, st, o)
		switch {
		case err != nil && ctx.Err() != nil:
			// Cancellation during a pass is part of shutdown, not an alarm.
		case err != nil:
			log.Warn("проход компактификации паков", "err", err)
		case stats.Compacted > 0:
			log.Info("проход компактификации паков",
				"packs", stats.Compacted, "freed_bytes", stats.FreedBytes)
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

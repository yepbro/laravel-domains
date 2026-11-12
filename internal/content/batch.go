// Group commit of content records (T-102; deferred finding C-15).
//
// Put pays a separate fsync and a separate DB query under flock for every
// block — the price of the C-1 invariants. A consumer storing several
// pages at once (up to 126 records) commits them with one group commit:
// PutBatch appends the batch's blocks and commits the metadata of all
// rows with one group commit: one fsync and one batched INSERT per batch
// instead of per-block ones (since T-147 the http-worker stores pages with
// early Puts one at a time — slot memory is more expensive; batches are
// written by the internal compaction passes and future batch holders).
// The C-1 invariant is not weakened:
//
//   - bytes before rows: the batch's single fsync runs BEFORE the
//     batched INSERT — a row cannot be committed before its bytes are on
//     disk (packs closed by rotation inside the batch are synced at
//     rotation — closeActiveLocked, as with compaction batches C-11);
//   - a pack is not removed before its live rows are committed: the
//     batched INSERT runs under flock taken before the first block of the
//     batch is appended. Compaction removes packs under the same lock
//     (T-045) — the "rotation → unlink → INSERT" window is closed by
//     mutual exclusion, not by timing: no "pack being written" markers or
//     grace periods are introduced into compaction (an inter-process
//     marker would require a file with its own crash semantics; a failure
//     there is the same class of loss as C-1, and the gain over one INSERT
//     per batch is minimal).
//
// A batch failure is safe and idempotent on retry (spec 3.4): the batched
// INSERT is atomic — either all the rows are missing or all are in place;
// a crash before the INSERT leaves the blocks as garbage without rows
// (invisible, compaction T-045 will purge them); dedup references added
// before the failure remain — a leak on the safe side (the same choice as
// Put and SwapRef).
package content

import (
	"context"
	"errors"
	"fmt"
)

// Group commit SQL. The exact texts are pinned by tests (batch_test.go,
// the pack_test.go style).
const (
	// Dedup batch: the live content of the batch is references only, no bytes
	// are written (the batched analog of sqlMetaAddRef: one command per batch
	// instead of per-row UPDATEs). The increment is the count of the hash's
	// copies in the batch (the unnest pair hash → n, as in sqlMetaPutBatch):
	// every Put of an item adds a reference, the semantics of sequential Puts
	// (T-228: earlier a batch of duplicates of live content yielded exactly +1
	// — references were undercounted, and compaction would have removed a
	// still-needed block). Dead rows (refcount = 0) are not resurrected:
	// their blocks may have been compacted, new Puts write fresh bytes.
	sqlMetaAddRefBatch = "UPDATE contents SET refcount = refcount + v.c " +
		"FROM unnest($1::bytea[], $2::int4[]) AS v(h, c) " +
		"WHERE hash = v.h AND refcount > 0 RETURNING hash"

	// Batched commit of the new blocks of a batch (the batched analog of
	// sqlMetaPut): unnest arrays; hashes in the batch are unique (duplicates
	// of one hash are folded into one row with the counter n — ON CONFLICT
	// cannot touch one row twice, and refcount = n preserves the semantics
	// "every Put adds a reference"). A conflict means a concurrent writer got
	// there first (the batch's blocks are garbage) or resurrection of a dead
	// row: the placement is moved to the batch's blocks (EXCLUDED), as in the
	// per-row upsert. RETURNING hash + (xmax = 0): the row order of RETURNING
	// is undefined, creation is told apart by hash.
	sqlMetaPutBatch = "INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) " +
		"SELECT hash, pack_id, off, len, raw_len, n " +
		"FROM unnest($1::bytea[], $2::int4[], $3::int8[], $4::int4[], $5::int4[], $6::int4[]) " +
		"AS v(hash, pack_id, off, len, raw_len, n) " +
		"ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + EXCLUDED.refcount, " +
		"pack_id = EXCLUDED.pack_id, off = EXCLUDED.off, " +
		"len = EXCLUDED.len, raw_len = EXCLUDED.raw_len " +
		"RETURNING hash, (xmax = 0) AS created"
)

// BatchStore — an optional extension of ContentStore with group commit
// (T-102): a consumer storing several contents at once commits them as a
// batch — one fsync and one batched metadata INSERT per batch instead of
// per-block ones (see the file comment). The implementation is
// (*PackStore).PutBatch; consumers discover it via a type assertion
// and must correctly handle stores without batching (per-row
// Put). Since T-147 the http-worker stores pages with early Puts one at a
// time (slot memory is more expensive than group commit — docs/content.md,
// "Writing from the http-worker"); the batch is a contract for consumers
// holding several contents by commit time (compaction C-11).
type BatchStore interface {
	ContentStore

	// PutBatch stores the items with one group commit and returns the
	// per-item results in items order. Contract:
	//
	//   - errs[i] != nil ⟺ item i is not stored individually:
	//     ErrTooLarge (over max_store_bytes, spec 5.5) or ErrStoreFull
	//     (content write pause, spec 8: the item's bytes were not written);
	//     the other items of the batch are still stored — dedup references
	//     to live content pass even when full;
	//   - err != nil ⟺ a batch failure (a DB/FS error or ctx cancellation):
	//     no new rows committed, results/errs are undefined; redelivery
	//     replays the batch idempotently by hash (dedup and upsert,
	//     spec 3.4);
	//   - with err == nil len(results) = len(errs) = len(items), and
	//     results[i].Hash = HashOf(items[i]) — the T-041 contract;
	//     PutResult.Created — "new bytes were written", false for
	//     deduplications and hash repeats within the batch.
	//
	// Duplicates of one content within the batch are allowed: the bytes are
	// written once, each copy adds a reference (like sequential Puts).
	// An empty batch and a batch of only deduplications take no locks and
	// do no fsync.
	PutBatch(ctx context.Context, items [][]byte) (results []PutResult, errs []error, err error)
}

// compile-time: PackStore implements group commit (T-102).
var _ BatchStore = (*PackStore)(nil)

// batchItem — a batch item after the filters: the hash, the indexes of the
// items of the original items with this hash (the first one carries the
// bytes), the counter of Puts of this hash in the batch (the refcount of
// the inserted row), and the size of the uncompressed bytes
// (contents.raw_len).
type batchItem struct {
	hash   Hash
	idxs   []int
	count  int32
	rawLen int32
}

// PutBatch — group commit; the contract is in BatchStore. Steps:
// the dedup batch (one UPDATE) → the disk pause → compression → appending
// all blocks, one fsync and one batched INSERT under a single acquisition
// of the mutex and flock (the C-1 invariants, the file comment).
func (s *PackStore) PutBatch(ctx context.Context, items [][]byte) ([]PutResult, []error, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if s.opts.ReadOnly {
		return nil, nil, errReadOnly
	}
	results := make([]PutResult, len(items))
	errs := make([]error, len(items))

	// 0) hashes, limits, in-batch duplicates: one hash — one row with a
	// reference counter (the bytes are written once, every Put of the item
	// adds a reference — the semantics of sequential Puts).
	all := make([]*batchItem, 0, len(items))
	byHash := make(map[Hash]*batchItem, len(items))
	for i, data := range items {
		if s.opts.MaxStoreBytes > 0 && int64(len(data)) > s.opts.MaxStoreBytes {
			errs[i] = fmt.Errorf("%w: %d байт при лимите %d",
				ErrTooLarge, len(data), s.opts.MaxStoreBytes)
			continue
		}
		h := HashOf(data)
		if it, ok := byHash[h]; ok {
			it.idxs = append(it.idxs, i)
			it.count++
			continue
		}
		it := &batchItem{hash: h, idxs: []int{i}, count: 1, rawLen: int32(len(data))}
		byHash[h] = it
		all = append(all, it)
	}
	if len(all) == 0 {
		return results, errs, nil
	}

	// 1) dedup batch: live content is references only (one DB call per
	// batch), no bytes written (sqlMetaAddRefBatch).
	live, err := s.addRefBatch(ctx, all)
	if err != nil {
		return nil, nil, err
	}
	remain := make([]*batchItem, 0, len(all))
	for _, it := range all {
		if live[it.hash] {
			for _, i := range it.idxs {
				results[i] = PutResult{Hash: it.hash} // Created=false: dedup
			}
			continue
		}
		remain = append(remain, it)
	}
	if len(remain) == 0 {
		return results, errs, nil
	}

	// 2) content write pause (spec 8, T-046): the batch's bytes are not
	// written, items get a per-item ErrStoreFull (the dedups above have
	// already returned: references to live content pass even when full).
	if err := s.pauseIfDiskFull(); err != nil {
		for _, it := range remain {
			for _, i := range it.idxs {
				errs[i] = err
			}
		}
		return results, errs, nil
	}

	// 3) compression before taking the locks: CPU work must not lengthen the
	// critical section (flock is held from appending until the rows are
	// committed).
	blocks := make([][]byte, len(remain))
	for i, it := range remain {
		blocks[i] = CompressWithDict(items[it.idxs[0]])
	}

	// 4–6) appending all blocks, one fsync and a batched INSERT — under one
	// acquisition of the mutex and flock (putBatchCommit, the C-1 invariants).
	created, err := s.putBatchCommit(ctx, remain, blocks)
	if err != nil {
		// ENOSPC on a physical write — an alert event (spec 8), as in Put.
		if errors.Is(err, ErrStoreFull) {
			s.fireStoreFull(s.enospcEvent())
		}
		return nil, nil, err
	}
	for _, it := range remain {
		results[it.idxs[0]] = PutResult{Hash: it.hash, Created: created[it.hash]}
		for _, i := range it.idxs[1:] {
			results[i] = PutResult{Hash: it.hash} // hash repeat within the batch: the bytes are already there
		}
	}
	return results, errs, nil
}

// addRefBatch — the dedup batch (step 1): references are added for live
// content without writing bytes, one command per batch. Every hash is
// paired with the number of copies in the batch — the increment is n, not
// 1 (the contract "every Put of an item adds a reference"). Invariant: the
// hashes of items are unique — guaranteed by the byHash dedup of step 0 of
// PutBatch (with a duplicate hash in unnest PostgreSQL would update the row
// once via one of the join rows, not sum the pairs; the fake pool in the
// duplicate test must fail, not silently diverge from the server — review
// T-228, P3-2). Returns the set of hashes that got references.
func (s *PackStore) addRefBatch(ctx context.Context, items []*batchItem) (map[Hash]bool, error) {
	hashes := make([][]byte, 0, len(items))
	counts := make([]int32, 0, len(items))
	for _, it := range items {
		hashes = append(hashes, it.hash[:])
		counts = append(counts, it.count)
	}
	rows, err := s.pool.Query(ctx, sqlMetaAddRefBatch, hashes, counts)
	if err != nil {
		return nil, fmt.Errorf("content: метаданные пачки (%d хэшей): %w", len(hashes), err)
	}
	defer rows.Close()
	live := make(map[Hash]bool, len(items))
	for rows.Next() {
		h, err := scanHashRow(rows)
		if err != nil {
			return nil, fmt.Errorf("content: метаданные пачки: %w", err)
		}
		live[h] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: метаданные пачки: %w", err)
	}
	return live, nil
}

// scanHashRow — the first column of a row as a Hash (RETURNING hash of the
// batched statements): a length check against corrupted data.
func scanHashRow(rows rowScanner) (Hash, error) {
	var hb []byte
	if err := rows.Scan(&hb); err != nil {
		return Hash{}, err
	}
	if len(hb) != HashLen {
		return Hash{}, fmt.Errorf("хэш %x: %d октетов, ожидается %d", hb, len(hb), HashLen)
	}
	var h Hash
	copy(h[:], hb)
	return h, nil
}

// rowScanner — the minimal row-reading interface (pgx.Rows and its test
// stand-ins).
type rowScanner interface {
	Scan(dest ...any) error
}

// putBatchCommit — steps 4–6 of the group commit: appends the blocks to
// the newest pack, performs the batch's single fsync and commits all rows
// with one batched INSERT — under one acquisition of the mutex and flock
// from appending to commit (the invariant "a pack is not removed before
// its live rows are committed"; fsync before INSERT — "bytes before rows").
// Returns the map "hash → whether created" (RETURNING xmax = 0).
func (s *PackStore) putBatchCommit(ctx context.Context, items []*batchItem, blocks [][]byte) (map[Hash]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errPackClosed
	}
	if err := flockDir(s.lock); err != nil {
		return nil, err
	}
	defer func() { unlockDir(s.lock) }()

	type placed struct {
		it     *batchItem
		packID int32
		off    int64
		clen   int32
	}
	written := make([]placed, 0, len(items))
	for i, it := range items {
		packID, off, err := s.appendBlockLocked(blocks[i])
		if err != nil {
			return nil, err
		}
		written = append(written, placed{it: it, packID: packID, off: off, clen: int32(len(blocks[i]))})
	}
	// The batch's single fsync: the bytes of all blocks are on disk before
	// any row of the batch is committed (C-1); packs closed by rotation
	// inside the batch were synced by closeActiveLocked at rotation.
	if err := s.syncActiveLocked(); err != nil {
		return nil, err
	}

	hashes := make([][]byte, 0, len(written))
	packs := make([]int32, 0, len(written))
	offs := make([]int64, 0, len(written))
	clens := make([]int32, 0, len(written))
	rawLens := make([]int32, 0, len(written))
	counts := make([]int32, 0, len(written))
	for _, w := range written {
		hashes = append(hashes, w.it.hash[:])
		packs = append(packs, w.packID)
		offs = append(offs, w.off)
		clens = append(clens, w.clen)
		rawLens = append(rawLens, w.it.rawLen)
		counts = append(counts, w.it.count)
	}
	rows, err := s.pool.Query(ctx, sqlMetaPutBatch, hashes, packs, offs, clens, rawLens, counts)
	if err != nil {
		return nil, fmt.Errorf("content: метаданные пачки (%d строк): %w", len(written), err)
	}
	defer rows.Close()
	created := make(map[Hash]bool, len(written))
	for rows.Next() {
		var hb []byte
		var cr bool
		if err := rows.Scan(&hb, &cr); err != nil {
			return nil, fmt.Errorf("content: метаданные пачки: %w", err)
		}
		if len(hb) != HashLen {
			return nil, fmt.Errorf("content: метаданные пачки: хэш %x: %d октетов, ожидается %d",
				hb, len(hb), HashLen)
		}
		var h Hash
		copy(h[:], hb)
		created[h] = cr
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: метаданные пачки: %w", err)
	}
	if len(created) != len(written) {
		return nil, fmt.Errorf("content: метаданные пачки: RETURNING дал %d строк, ожидалось %d",
			len(created), len(written))
	}
	return created, nil
}

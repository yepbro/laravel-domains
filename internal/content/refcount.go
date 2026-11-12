package content

// Rewriting a content reference when a result is rewritten (T-044; spec 5.7, 8).
//
// Spec 5.7: "when a result is rewritten, the reference counter of the old
// content is decremented". Put deduplication and Delete reference removal
// already live in the store (T-042); this is the missing accounting
// operation for the results layer: a subdomain/variant got new content →
// a new reference is added, the old one is removed. Writing the
// subdomain_results rows themselves is the http-worker's territory
// (T-058): below are only pure swap functions to call from there;
// the recommended place is one transaction with the result write
// (SwapRefTx).

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
)

// refNoHash — the zero Hash: "no reference", NULL content_hash in
// subdomain_results (spec 6.1). The probability of a real content hash
// equal to zero (the first 16 bytes of SHA-256) is 2^-128, negligible;
// the semantics are fixed in one place so the layers do not diverge
// in their reading of NULL.
var refNoHash Hash

// ErrRefIntegrity — the contents reference accounting diverges from what
// the swap expects: the content was not written by a Put (or is dead —
// refcount = 0) where a live reference is needed. This is an integrity
// error, not ErrNotFound: idempotent result writing (spec 3.4) must not
// get into this branch; silently going negative or skipping a step is
// worse than failing — the transaction is rolled back in full, and the
// redelivery is handled by the caller.
var ErrRefIntegrity = errors.New("content: content reference accounting violated")

// RefDB — a pool for reference swaps: a db.DB from which a transaction
// can be started (the ingest.CleanupDB, scheduler.RetryDB style). Both
// swap statements are atomic in one transaction (spec 3.4);
// *pgxpool.Pool satisfies the interface.
type RefDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool fits the interface.
var _ RefDB = (*pgxpool.Pool)(nil)

// SwapRef rewrites the content reference of one result (spec 5.7,
// 8): newHash gains a reference (refcount + 1), oldHash loses it
// (refcount − 1) — atomically in one DB transaction, on the same
// contents table and with the same statements the store's Put/Delete use
// (T-042). Called by the http-worker results-writing layer (T-058) when
// rewriting subdomain_results with a content_hash change; the function
// does not touch the result rows themselves.
//
// Scenarios (a zero Hash — "no reference", NULL content_hash, spec 6.1):
//   - old == new — the content did not change: a no-op without a transaction
//     (idempotency of redelivery, spec 3.4);
//   - the old one is zero — the result's first content: only +1 to the new one;
//   - the new one is zero — a result without content: only −1 to the old one;
//   - both zero — a no-op;
//   - the old one's refcount reaches zero after −1 — the row stays "dead"
//     (the T-042 contract): Get/Delete do not see it, the space is freed by
//     pack compaction (T-045).
//
// Statement order — first +1 to the new one, then −1 to the old one. Inside one
// transaction there is no intermediate state; the order is pinned for
// embedding the swap into the result-write transaction (SwapRefTx, T-058) and
// in case the steps are run separately: a crash "after +1, before −1" leaves
// an extra reference on the old content — safe, the block merely stays live
// (a space leak, the data is intact); a crash in the reverse order loses a live
// reference: the refcount of content still referenced by a result
// could reach zero — compaction T-045 would free the block, Get would return
// ErrNotFound for a live result, and re-applying the analysis rules
// (spec 5.7: content is stored while rules need to be re-applied) would become
// impossible. Per spec 3.4 we choose the safe side: a space leak is better
// than data loss.
//
// The refcount > 0 guard in both statements is not only a ban on going
// negative: RowsAffected = 0 means the new content was not written by
// a Put (or is dead), or the old one's reference was already removed — a discrepancy
// between the result accounting and contents. Such a case is ErrRefIntegrity with a
// transaction rollback: better to fail than to silently corrupt the counter.
func SwapRef(ctx context.Context, pool RefDB, oldHash, newHash Hash) error {
	if pool == nil {
		return errors.New("content: reference swap: the pool is not set")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if oldHash == newHash {
		// The content did not change (including both references zero): the counters
		// are not touched, no transaction is opened.
		return nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("content: reference swap %s → %s: transaction: %w", oldHash, newHash, err)
	}
	// Rollback unconditionally: after Commit — a no-op. An interrupted swap leaves
	// no half-applied changes (spec 3.4).
	defer func() { _ = tx.Rollback(ctx) }()
	if err := SwapRefTx(ctx, tx, oldHash, newHash); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("content: reference swap %s → %s: commit: %w", oldHash, newHash, err)
	}
	return nil
}

// SwapRefTx — the SwapRef primitive for the caller's transaction: T-058 writes
// subdomain_results and changes references in ONE transaction — separate
// transactions are dangerous in both directions (a reference without a row or a row without
// a reference, spec 3.4). The semantics, scenarios, and statement order are the same as
// SwapRef's; Begin/Commit are the caller's responsibility.
func SwapRefTx(ctx context.Context, tx pgx.Tx, oldHash, newHash Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil {
		return errors.New("content: reference swap: the transaction is not set")
	}
	if oldHash == newHash {
		return nil // the content did not change (including both references zero)
	}
	if newHash != refNoHash {
		tag, err := tx.Exec(ctx, sqlMetaAddRef, newHash[:])
		if err != nil {
			return fmt.Errorf("content: reference swap %s → %s: referencing the new content: %w", oldHash, newHash, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: %s is not stored (Put) or is dead — Put first, then the result",
				ErrRefIntegrity, newHash)
		}
	}
	if oldHash != refNoHash {
		tag, err := tx.Exec(ctx, sqlMetaDelRef, oldHash[:])
		if err != nil {
			return fmt.Errorf("content: reference swap %s → %s: removing the old content reference: %w", oldHash, newHash, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: %s has no live reference — the result accounting diverges from contents",
				ErrRefIntegrity, oldHash)
		}
	}
	return nil
}

// SwapRef rewrites the content reference through this store's pool:
// the same contents table and the same statements as Put/Delete (T-042).
// Convenient for a worker already holding a *PackStore (T-058). OpenPackStore
// accepts a db.DB, so the pool's ability to open transactions
// (RefDB, e.g. *pgxpool.Pool) is checked at call time.
// The semantics and statement order are SwapRef's.
func (s *PackStore) SwapRef(ctx context.Context, oldHash, newHash Hash) error {
	pool, ok := s.pool.(RefDB)
	if !ok {
		return fmt.Errorf("content: reference swap: the %T pool does not open transactions (a RefDB is needed, e.g. *pgxpool.Pool)", s.pool)
	}
	return SwapRef(ctx, pool, oldHash, newHash)
}

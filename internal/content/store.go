package content

// Contract of the external content store (spec 5.7: "access through the
// ContentStore interface (Put, Get, Delete) so that the implementation can be
// replaced later (for example, with S3)"). This file pins the thin byte
// contract and content identity; the implementation on pack files with
// metadata in PostgreSQL is T-042...T-046.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

// HashLen is the content identifier size: 16 bytes (128 bits), same as
// contents.hash and subdomain_results.content_hash in the schema (spec 6.1).
const HashLen = 16

// Hash is the content identifier: HashLen bytes of SHA-256 over the normalized
// contents, truncated to 128 bits (spec 5.7 "deduplication by a 128-bit
// hash"). A fixed-size array cannot be "almost the right" size, and it works
// as a map key.
type Hash [HashLen]byte

// String returns the hex representation of the identifier for logs and errors.
func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

// HashOf computes the content identifier: SHA-256 over data, truncated to
// the first HashLen bytes. The algorithm from spec 5.7/6.1 is pinned in a
// single place: Put must return HashOf(data), and consumers compare content
// by hash without touching the store (the variant logic of T-054 and the
// aliases of T-055 — "same content hash", spec 5.5 items 5-6).
//
// "Normalized contents" (spec 5.7) are bytes already brought to their final
// form by the pipeline: UTF-8 encoding and the page at the end of the
// redirect chain (spec 5.5 items 4, 8). The store itself does not normalize:
// Put accepts and Get returns exactly the bytes passed — otherwise the
// roundtrip Get(Put(x)) == x and deduplication would be undefinable at the
// contract level.
func HashOf(data []byte) Hash {
	sum := sha256.Sum256(data)
	var h Hash
	copy(h[:], sum[:HashLen])
	return h
}

// Contract errors: implementations return them directly or wrapped
// (reachable via errors.Is); workers tell branches apart by sentinels,
// not by message text.
var (
	// ErrNotFound — Get/Delete: no content with this Hash exists — it was
	// never written, or all references have already been removed by Delete.
	ErrNotFound = errors.New("content: контент не найден")

	// ErrStoreFull — Put: the store has reached its limit (disk overflow,
	// spec 8). The data is neither written nor corrupted: the worker pauses
	// content writing and raises an alert (T-046, T-080), retrying later.
	ErrStoreFull = errors.New("content: хранилище переполнено")

	// ErrTooLarge — Put: content larger than the max_store_bytes limit —
	// "max stored HTML size" (spec 5.5, 5.10). Rejected as a whole;
	// the worker keeps the result without content (T-056).
	ErrTooLarge = errors.New("content: контент больше лимита хранения")

	// ErrCorrupted — Get: the metadata row is alive but its block is
	// unreadable (loss/corruption of pack bytes: disk failure, interrupted
	// write, the window before fsync). Distinguishable from ErrNotFound:
	// reading does not mask corruption as "never written", and compaction
	// purges such blocks (the row is marked dead, the pack is freed) —
	// after that Get answers ErrNotFound.
	ErrCorrupted = errors.New("content: контент повреждён")
)

// PutResult is the outcome of Put: the key for subdomain_results.content_hash
// (T-056, T-058) and the write fact for the deduplication share metrics
// (spec 11.4 item 3).
type PutResult struct {
	Hash    Hash // content identifier — always HashOf(data)
	Created bool // true — new bytes were written; false — deduplication: the bytes already existed, another reference was added
}

// ContentStore is the external store of final page contents (spec 5.7):
// raw HTML is written by the http-worker (T-056), the final render DOM by the
// render-worker (T-067); the analyzer and crawlerctl domain --content read it
// (T-074, T-081); references are removed on result overwrite and deletion
// (T-020, T-044).
//
// The contract boundary is a thin layer of bytes:
//   - zstd compression with a trained dictionary (spec 5.7, T-043) is an
//     implementation detail: Put accepts and Get returns uncompressed bytes;
//   - the contents.refcount reference counter is not exposed by methods: the
//     pair "Put added a reference — Delete removed one" is pinned by the
//     method semantics; the counter is maintained by the implementation
//     (T-044). The consumer does not need the counter — the worker knows only
//     its own contents; the rule "overwriting a result decreases the refcount
//     of the old one" is the behavior of the results layer above the DB
//     (T-044, T-058), not a store operation. Physical freeing of bytes is also
//     an implementation detail: pack files are append-only, space is returned
//     by compaction (T-045).
//
// All methods are ready for context cancellation: cancellation returns a
// context error (errors.Is), the operation is considered not to have
// happened — no reference is added or removed.
type ContentStore interface {
	// Put stores data and returns the content identifier. The content is
	// treated as normalized (see HashOf); empty data is valid content.
	// Idempotent by contents: the same bytes yield the same Hash; writing
	// again neither duplicates bytes on disk (Created = false) nor adds
	// anything but one more reference — a pair that Delete will remove.
	// This is how T-044 deduplication works: parking pages and hosting
	// stubs are stored once (spec 5.7, 8).
	//
	// Either Put adds a reference and returns PutResult, or it returns an
	// error: ErrTooLarge — larger than the max_store_bytes limit;
	// ErrStoreFull — the store is full, retry later; context error —
	// cancellation.
	Put(ctx context.Context, data []byte) (PutResult, error)

	// Get returns the bytes passed to Put (decompression happens inside
	// the implementation) as an independent copy: mutating the result does
	// not affect the store. Missing content — ErrNotFound.
	Get(ctx context.Context, hash Hash) ([]byte, error)

	// Delete removes one reference added by Put: while references remain,
	// Get keeps returning the content; after the last one the content is
	// unavailable (ErrNotFound), and the implementation frees the space
	// (compaction T-045). Removing a nonexistent reference is ErrNotFound,
	// not silence: in a pipeline with redelivery (spec 3.4) a double
	// removal is a symptom of a bookkeeping bug; better to see it right
	// away than silently ruin someone else's refcount.
	Delete(ctx context.Context, hash Hash) error
}

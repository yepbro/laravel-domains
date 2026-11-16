// Package content — the content storage framework (spec 5.7).
//
// The ContentStore interface (Put/Get/Delete), append-only pack files,
// deduplication by a 128-bit hash, refcounts, and compaction are the
// subject of tasks T-041…T-046. For now the package provides minimal zstd
// compression wrappers on top of which pack-file writing is built
// (the trainable dictionary is T-043).
package content

import (
	"github.com/klauspost/compress/zstd"
)

// maxDecodeSize — the cap on one decompressed frame: 32 MiB, a 2× headroom
// over the largest legitimate content (content.max_store_bytes ≤ 16 MiB by
// the config validation; if that limit is ever raised, this constant must
// move with it — and the old packs must stay under the new value). Every
// decoder of the package is created with it: pack blocks are bytes from
// disk, not data from a trusted writer, and a corrupted or hostile frame
// expanding beyond the cap is rejected by the decoder itself
// (zstd.ErrDecoderSizeExceeded → ErrCorrupted in Get) — with
// the klauspost/compress default of 64 GiB a valid-but-oversized frame
// would be materialized in full first, OOM-killing the worker long before
// the raw_len check runs (bulk review 08#P2-1).
const maxDecodeSize = 32 << 20

// The encoder and decoder are created once: EncodeAll/DecodeAll are safe
// for concurrent use from multiple goroutines.
var (
	encoder, _ = zstd.NewWriter(nil) // nil — for EncodeAll only
	// The decoder is bounded by maxDecodeSize: the input of DecodeAll is
	// a pack block from disk (see the constant).
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxDecodeSize)) // nil — for DecodeAll only
)

// Compress compresses src with zstd (no dictionary).
func Compress(src []byte) []byte {
	return encoder.EncodeAll(src, nil)
}

// Decompress decompresses data compressed by Compress.
func Decompress(src []byte) ([]byte, error) {
	return decoder.DecodeAll(src, nil)
}

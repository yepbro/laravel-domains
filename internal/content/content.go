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

// The encoder and decoder are created once: EncodeAll/DecodeAll are safe
// for concurrent use from multiple goroutines.
var (
	encoder, _ = zstd.NewWriter(nil) // nil — for EncodeAll only
	decoder, _ = zstd.NewReader(nil) // nil — for DecodeAll only
)

// Compress compresses src with zstd (no dictionary).
func Compress(src []byte) []byte {
	return encoder.EncodeAll(src, nil)
}

// Decompress decompresses data compressed by Compress.
func Decompress(src []byte) ([]byte, error) {
	return decoder.DecodeAll(src, nil)
}

package content

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestCompressRoundTrip(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		[]byte("<html><body>hello, world</body></html>"),
		[]byte(strings.Repeat("<div class=\"row\">hosting parking</div>\n", 1000)),
	}
	for _, src := range cases {
		compressed := Compress(src)
		got, err := Decompress(compressed)
		if err != nil {
			t.Fatalf("Decompress: %v", err)
		}
		if !bytes.Equal(got, src) {
			t.Errorf("round-trip changed the data: got %d bytes, want %d", len(got), len(src))
		}
	}
}

func TestCompressReducesRepetitive(t *testing.T) {
	src := []byte(strings.Repeat("laravel", 10000))
	if compressed := Compress(src); len(compressed) >= len(src) {
		t.Errorf("zstd did not compress the repetitive data: %d -> %d bytes", len(src), len(compressed))
	}
}

func TestDecompressGarbage(t *testing.T) {
	if _, err := Decompress([]byte("this is not a zstd stream")); err == nil {
		t.Error("expected a garbage-decompression error")
	}
}

// TestDecompressSizeCap — decompression is bounded (bulk review 08#P2-1):
// a block comes from disk, not from a trusted writer, so the decoder must
// refuse a frame expanding beyond the cap instead of materializing it in
// full — the klauspost/compress default cap is 64 GiB, an order of magnitude
// above the server's memory. The cap is 32 MiB, a 2× headroom over the
// largest legitimate content (max_store_bytes ≤ 16 MiB), which must still
// decompress.
func TestDecompressSizeCap(t *testing.T) {
	// A "bomb": a structurally valid frame expanding far beyond the cap;
	// zeros keep the compressed form tiny.
	bomb := Compress(bytes.Repeat([]byte{0}, 40<<20))
	if _, err := Decompress(bomb); !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Errorf("a frame beyond the cap: err = %v, want zstd.ErrDecoderSizeExceeded", err)
	}

	// The bomb is a valid frame, not garbage: a decoder with a raised cap
	// reads it in full — the test rejects the frame for its size only.
	big, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		t.Fatalf("a reference decoder: %v", err)
	}
	defer big.Close()
	if got, err := big.DecodeAll(bomb, nil); err != nil || len(got) != 40<<20 {
		t.Errorf("the bomb frame is not a valid zstd frame: %v, %d bytes", err, len(got))
	}

	// The largest legitimate content (max_store_bytes ≤ 16 MiB) still
	// decompresses.
	src := bytes.Repeat([]byte("<div class=pad>"), 1<<20) // 15 MiB
	if got, err := Decompress(Compress(src)); err != nil || len(got) != len(src) {
		t.Errorf("a frame under the cap: %v, %d bytes", err, len(got))
	}
}

package content

import (
	"bytes"
	"strings"
	"testing"
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

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
		[]byte("<html><body>привет, мир</body></html>"),
		[]byte(strings.Repeat("<div class=\"row\">парковка хостинга</div>\n", 1000)),
	}
	for _, src := range cases {
		compressed := Compress(src)
		got, err := Decompress(compressed)
		if err != nil {
			t.Fatalf("Decompress: %v", err)
		}
		if !bytes.Equal(got, src) {
			t.Errorf("round-trip изменил данные: got %d байт, want %d", len(got), len(src))
		}
	}
}

func TestCompressReducesRepetitive(t *testing.T) {
	src := []byte(strings.Repeat("laravel", 10000))
	if compressed := Compress(src); len(compressed) >= len(src) {
		t.Errorf("zstd не сжал повторяющиеся данные: %d -> %d байт", len(src), len(compressed))
	}
}

func TestDecompressGarbage(t *testing.T) {
	if _, err := Decompress([]byte("это не zstd-поток")); err == nil {
		t.Error("ожидалась ошибка распаковки мусора")
	}
}

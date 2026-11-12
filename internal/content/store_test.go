package content

// Pinning of the content identification algorithm (HashOf, spec 5.7) and
// the hex representation for logs. The reference in-memory implementation and
// running the StoreContract contract suite against it live in the contenttest
// package (T-228): the suite imports testing and must not end up in the
// non-test build of the content package (analogous to stdlib testing/fstest).

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestHashOf pins the identification algorithm (spec 5.7): SHA-256,
// truncated to the first HashLen bytes. The expectation is computed directly
// with crypto/sha256, so that changing the algorithm in HashOf cannot pass
// unnoticed.
func TestHashOf(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		[]byte("<html><body>итоговая страница</body></html>"),
		[]byte("bin\x00\x01\xff content \x00"),
	}
	for i, d := range cases {
		h := HashOf(d)
		if len(h) != HashLen {
			t.Fatalf("HashOf(%d): длина %d, want %d", i, len(h), HashLen)
		}
		sum := sha256.Sum256(d)
		var want Hash
		copy(want[:], sum[:HashLen])
		if h != want {
			t.Errorf("HashOf(%d) = %s, want SHA-256(data)[:16] = %s", i, h, want)
		}
	}
	if HashOf(nil) != HashOf([]byte{}) {
		t.Error("nil и пустой срез — один и тот же контент, хэши должны совпасть")
	}
	if HashOf([]byte("http://example.com/")) == HashOf([]byte("https://example.com/")) {
		t.Error("разный контент дал одинаковый хэш")
	}
}

// TestHashString — the hex representation for logs and error messages.
func TestHashString(t *testing.T) {
	h := HashOf([]byte("контент"))
	want := hex.EncodeToString(h[:])
	if got := h.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if len(h.String()) != HashLen*2 {
		t.Errorf("String(): %d символов, want %d (байт на hex-цифру)", len(h.String()), HashLen*2)
	}
}

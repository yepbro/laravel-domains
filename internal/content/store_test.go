package content

// Фиксация алгоритма идентификации контента (HashOf, ТЗ 5.7) и
// hex-представления для логов. Эталонная in-memory реализация и прогон
// на ней контрактного набора StoreContract — в пакете contenttest
// (T-228): набор импортирует testing и не должен попадать в нетестовую
// сборку пакета content (аналог stdlib testing/fstest).

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestHashOf — фиксация алгоритма идентификации (ТЗ 5.7): SHA-256,
// усечённый до первых HashLen байт. Ожидание считается напрямую
// crypto/sha256, чтобы смена алгоритма в HashOf не прошла незамеченной.
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

// TestHashString — hex-представление для логов и сообщений об ошибках.
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

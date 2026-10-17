package content

// Эталонная in-memory реализация контракта и сам прогон набора
// StoreContract на ней. Набор переиспользуется тестами реализаций:
// T-042 (пак-файлы поверх фейкового pgx-пула) и T-046 (живая БД,
// test/integration) вызывают StoreContract со своим конструктором
// хранилища; сам набор живёт в store_contract.go.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"
)

// storeFake — эталонная реализация контракта для прогона самого набора
// StoreContract: карта «Hash → байты + счётчик ссылок», без сжатия и
// внешних хранилищ. Ссылки ведутся по правилам контракта: Put добавляет,
// Delete снимает, исчерпание — удаление.
type storeFake struct {
	mu      sync.Mutex
	entries map[Hash]*storeFakeEntry
}

// storeFakeEntry — контент и число живых ссылок на него (contents.refcount).
type storeFakeEntry struct {
	data []byte
	refc int
}

// compile-time: storeFake реализует ContentStore.
var _ ContentStore = (*storeFake)(nil)

func newStoreFake() *storeFake {
	return &storeFake{entries: make(map[Hash]*storeFakeEntry)}
}

func (s *storeFake) Put(ctx context.Context, data []byte) (PutResult, error) {
	if err := ctx.Err(); err != nil {
		return PutResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := HashOf(data)
	e, ok := s.entries[h]
	if !ok {
		s.entries[h] = &storeFakeEntry{data: bytes.Clone(data), refc: 1}
		return PutResult{Hash: h, Created: true}, nil
	}
	e.refc++
	return PutResult{Hash: h}, nil
}

func (s *storeFake) Get(ctx context.Context, hash Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[hash]
	if !ok {
		return nil, ErrNotFound
	}
	return bytes.Clone(e.data), nil
}

func (s *storeFake) Delete(ctx context.Context, hash Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[hash]
	if !ok {
		return ErrNotFound
	}
	e.refc--
	if e.refc <= 0 {
		delete(s.entries, hash)
	}
	return nil
}

// TestStoreContractOnFake — эталонная реализация проходит собственный
// контрактный набор: набор корректен и готов к переиспользованию
// реализациями (T-042, T-046).
func TestStoreContractOnFake(t *testing.T) {
	StoreContract(t, func(*testing.T) ContentStore { return newStoreFake() })
}

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

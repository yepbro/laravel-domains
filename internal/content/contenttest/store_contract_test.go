// Эталонная in-memory реализация контракта и прогон на ней самого
// набора StoreContract (переехали из content/store_test.go, T-228):
// набор корректен и готов к переиспользованию тестами реализаций —
// T-042 (PackStore, внешний тест-пакет content_test) и T-046 (живая
// БД, test/integration) вызывают StoreContract со своим конструктором.
package contenttest

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"crawler/internal/content"
)

// storeFake — эталонная реализация контракта для прогона самого набора
// StoreContract: карта «Hash → байты + счётчик ссылок», без сжатия и
// внешних хранилищ. Ссылки ведутся по правилам контракта: Put добавляет,
// Delete снимает, исчерпание — удаление.
type storeFake struct {
	mu      sync.Mutex
	entries map[content.Hash]*storeFakeEntry
}

// storeFakeEntry — контент и число живых ссылок на него (contents.refcount).
type storeFakeEntry struct {
	data []byte
	refc int
}

// compile-time: storeFake реализует ContentStore.
var _ content.ContentStore = (*storeFake)(nil)

func newStoreFake() *storeFake {
	return &storeFake{entries: make(map[content.Hash]*storeFakeEntry)}
}

func (s *storeFake) Put(ctx context.Context, data []byte) (content.PutResult, error) {
	if err := ctx.Err(); err != nil {
		return content.PutResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := content.HashOf(data)
	e, ok := s.entries[h]
	if !ok {
		s.entries[h] = &storeFakeEntry{data: bytes.Clone(data), refc: 1}
		return content.PutResult{Hash: h, Created: true}, nil
	}
	e.refc++
	return content.PutResult{Hash: h}, nil
}

func (s *storeFake) Get(ctx context.Context, hash content.Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[hash]
	if !ok {
		return nil, content.ErrNotFound
	}
	return bytes.Clone(e.data), nil
}

func (s *storeFake) Delete(ctx context.Context, hash content.Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[hash]
	if !ok {
		return content.ErrNotFound
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
	StoreContract(t, func(*testing.T) content.ContentStore { return newStoreFake() })
}

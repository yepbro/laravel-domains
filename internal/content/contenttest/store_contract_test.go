// The reference in-memory implementation of the contract and a run of the
// StoreContract rule set itself on it (moved from content/store_test.go, T-228):
// the rule set is correct and ready for reuse by implementation tests —
// T-042 (PackStore, the external test package content_test) and T-046 (a
// live DB, test/integration) call StoreContract with their own constructor.
package contenttest

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"crawler/internal/content"
)

// storeFake — a reference implementation of the contract for running the
// StoreContract rule set itself: a map "Hash → bytes + a reference counter",
// without compression or external stores. References follow the contract rules:
// Put adds, Delete removes, exhaustion — deletion.
type storeFake struct {
	mu      sync.Mutex
	entries map[content.Hash]*storeFakeEntry
}

// storeFakeEntry — the content and the number of live references to it (contents.refcount).
type storeFakeEntry struct {
	data []byte
	refc int
}

// compile-time: storeFake implements ContentStore.
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

// TestStoreContractOnFake — the reference implementation passes its own
// contract rule set: the rule set is correct and ready for reuse by
// implementations (T-042, T-046).
func TestStoreContractOnFake(t *testing.T) {
	StoreContract(t, func(*testing.T) content.ContentStore { return newStoreFake() })
}

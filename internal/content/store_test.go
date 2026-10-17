package content

// Контрактный набор ContentStore (ТЗ 5.7) и его прогон на эталонной
// in-memory реализации. Набор переиспользуется тестами реализаций:
// T-042 (пак-файлы поверх PostgreSQL) и T-046 (лимиты хранилища) вызывают
// StoreContract со своим конструктором хранилища.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

// StoreContract прогоняет контракт ContentStore (ТЗ 5.7) по реализации,
// создаваемой newStore для каждого подтеста (изоляция состояний; освобождение
// ресурсов реализации — t.Cleanup внутри конструктора).
//
// Набор проверяет только контракт байтов, общий для всех реализаций:
// roundtrip, идемпотентность Put, парность ссылок Put/Delete, ошибки
// ErrNotFound, отмену контекста, конкурентные Put. Поведение, зависящее от
// конфигурации конкретной реализации — лимит max_store_bytes (ErrTooLarge)
// и переполнение диска (ErrStoreFull, ТЗ 8) — набор не моделирует: его
// проверяют собственные тесты реализаций (T-046).
func StoreContract(t *testing.T, newStore func(t *testing.T) ContentStore) {
	t.Helper()

	// Корпус: пустой контент (nil и пустой срез — один и тот же контент,
	// TestHashOf, поэтому в корпусе одно пустое значение), текст, бинарные
	// байты с нулями (нечувствительность к содержимому) и крупный
	// повторяющийся фрагмент (сжимаемость — зона ответственности zstd-слоя,
	// для контракта это просто байты).
	data := [][]byte{
		nil,
		[]byte("<html><body>итоговая страница</body></html>"),
		[]byte("bin\x00\x01\xff content \x00"),
		bytes.Repeat([]byte("<div>парковка хостинга</div>"), 8192), // ~168 КиБ
	}
	ctx := context.Background()

	t.Run("roundtrip", func(t *testing.T) {
		store := newStore(t)
		for i, d := range data {
			res, err := store.Put(ctx, d)
			if err != nil {
				t.Fatalf("Put(%d): %v", i, err)
			}
			if !res.Created {
				t.Errorf("Put(%d): первый Put должен записать новый контент, Created = false", i)
			}
			if want := HashOf(d); res.Hash != want {
				t.Errorf("Put(%d): hash = %s, want HashOf(data) = %s", i, res.Hash, want)
			}
			got, err := store.Get(ctx, res.Hash)
			if err != nil {
				t.Fatalf("Get(%d): %v", i, err)
			}
			if !bytes.Equal(got, d) {
				t.Errorf("Get(%d): байты не совпали: got %d, want %d байт", i, len(got), len(d))
			}
			// Get возвращает независимую копию (контракт метода).
			if len(got) > 0 {
				got[0] ^= 0xff
				again, err := store.Get(ctx, res.Hash)
				if err != nil {
					t.Fatalf("повторный Get(%d): %v", i, err)
				}
				if !bytes.Equal(again, d) {
					t.Errorf("Get(%d): правка результата первого Get изменила хранилище", i)
				}
			}
		}
	})

	t.Run("Put не изменяет данные", func(t *testing.T) {
		store := newStore(t)
		d := []byte("изменяемые данные")
		before := bytes.Clone(d)
		if _, err := store.Put(ctx, d); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if !bytes.Equal(d, before) {
			t.Error("Put изменил переданные данные")
		}
	})

	t.Run("идемпотентность Put", func(t *testing.T) {
		store := newStore(t)
		first, err := store.Put(ctx, data[1])
		if err != nil {
			t.Fatalf("первый Put: %v", err)
		}
		second, err := store.Put(ctx, bytes.Clone(data[1]))
		if err != nil {
			t.Fatalf("повторный Put: %v", err)
		}
		if first.Hash != second.Hash {
			t.Errorf("тот же контент дал разные хэши: %s и %s", first.Hash, second.Hash)
		}
		if second.Created {
			t.Error("повторный Put записал байты заново: Created = true, want false (дедупликация)")
		}
		if got, err := store.Get(ctx, second.Hash); err != nil || !bytes.Equal(got, data[1]) {
			t.Errorf("Get после повторного Put: %v, совпадение = %v", err, bytes.Equal(got, data[1]))
		}
	})

	t.Run("разный контент — разный Hash", func(t *testing.T) {
		store := newStore(t)
		a, err := store.Put(ctx, data[1])
		if err != nil {
			t.Fatalf("Put(a): %v", err)
		}
		b, err := store.Put(ctx, data[2])
		if err != nil {
			t.Fatalf("Put(b): %v", err)
		}
		if a.Hash == b.Hash {
			t.Errorf("разный контент дал один хэш %s", a.Hash)
		}
	})

	t.Run("ссылки: Put ×N — Delete ×N", func(t *testing.T) {
		store := newStore(t)
		const n = 3
		res, err := store.Put(ctx, data[3])
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		for i := 1; i < n; i++ { // n ссылок всего
			if _, err := store.Put(ctx, data[3]); err != nil {
				t.Fatalf("Put-ссылка %d: %v", i, err)
			}
		}
		for i := 1; i < n; i++ { // пока есть ссылки, контент жив
			if err := store.Delete(ctx, res.Hash); err != nil {
				t.Fatalf("Delete %d: %v", i, err)
			}
			if _, err := store.Get(ctx, res.Hash); err != nil {
				t.Fatalf("Get после Delete %d из %d ссылок: %v (контент ещё нужен)", i, n, err)
			}
		}
		if err := store.Delete(ctx, res.Hash); err != nil { // последняя ссылка
			t.Fatalf("Delete последней ссылки: %v", err)
		}
		if _, err := store.Get(ctx, res.Hash); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get после исчерпания ссылок: %v, want ErrNotFound", err)
		}
		if err := store.Delete(ctx, res.Hash); !errors.Is(err, ErrNotFound) {
			t.Errorf("Delete без ссылок: %v, want ErrNotFound", err)
		}
	})

	t.Run("Get несуществующего", func(t *testing.T) {
		store := newStore(t)
		for _, h := range []Hash{{}, HashOf([]byte("такого контента нет"))} {
			if _, err := store.Get(ctx, h); !errors.Is(err, ErrNotFound) {
				t.Errorf("Get(%s): %v, want ErrNotFound", h, err)
			}
		}
	})

	t.Run("Delete несуществующего", func(t *testing.T) {
		store := newStore(t)
		h := HashOf([]byte("такого контента нет"))
		if err := store.Delete(ctx, h); !errors.Is(err, ErrNotFound) {
			t.Errorf("Delete(%s): %v, want ErrNotFound", h, err)
		}
	})

	t.Run("отмена контекста", func(t *testing.T) {
		store := newStore(t)
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		d := []byte("данные отменённой записи")
		if _, err := store.Put(canceled, d); !errors.Is(err, context.Canceled) {
			t.Errorf("Put с отменённым ctx: %v, want context.Canceled", err)
		}
		h := HashOf(d)
		if _, err := store.Get(canceled, h); !errors.Is(err, context.Canceled) {
			t.Errorf("Get с отменённым ctx: %v, want context.Canceled", err)
		}
		if err := store.Delete(canceled, h); !errors.Is(err, context.Canceled) {
			t.Errorf("Delete с отменённым ctx: %v, want context.Canceled", err)
		}
		// Отменённые операции не меняют хранилище: контент не появился.
		if _, err := store.Get(ctx, h); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get после отменённого Put: %v, want ErrNotFound", err)
		}
	})

	t.Run("параллельные Put одного контента", func(t *testing.T) {
		store := newStore(t)
		const workers = 8
		var wg sync.WaitGroup
		hashes := make([]Hash, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				res, err := store.Put(ctx, data[3])
				if err != nil {
					t.Errorf("Put горутины %d: %v", i, err)
					return
				}
				hashes[i] = res.Hash
			}(i)
		}
		wg.Wait()
		want := HashOf(data[3])
		for i, h := range hashes {
			if h != want {
				t.Fatalf("горутина %d: hash = %s, want %s", i, h, want)
			}
		}
		// Каждый Put добавил свою ссылку: workers удалений исчерпывают контент.
		for i := 0; i < workers; i++ {
			if err := store.Delete(ctx, want); err != nil {
				t.Fatalf("Delete %d из %d: %v", i+1, workers, err)
			}
		}
		if _, err := store.Get(ctx, want); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get после %d Delete: %v, want ErrNotFound", workers, err)
		}
	})
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

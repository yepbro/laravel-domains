// Пакет contenttest — контрактный набор проверок хранилища контента
// (T-041; вынесен сюда из content/store_contract.go в T-228). Набор
// оперирует *testing.T и потому живёт отдельным пакетом — аналогом
// stdlib testing/fstest, — импортируемым только тестами: обычный файл
// в пакете content тянул testing в нетестовую сборку (в прод-бинарник).
// Набор оперирует только экспортируемым API пакета content и
// переиспользуется тестами реализаций: эталонный прогон на in-memory
// фейке (store_contract_test.go), PackStore (внешний тест-пакет
// content_test, T-042) и интеграционные тесты против живой БД
// (test/integration, T-046).
package contenttest

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"crawler/internal/content"
)

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
func StoreContract(t *testing.T, newStore func(t *testing.T) content.ContentStore) {
	t.Helper()

	// Корпус: пустой контент (nil и пустой срез — один и тот же контент,
	// TestHashOf, поэтому в корпусе одно пустое значение), текст, бинарные
	// байты с нулями (нечувствительность к содержимому) и крупный
	// повторяющийся фрагмент (сжимаемость — зона ответственности zstd-слоя,
	// для контракта это просто байты).
	data := [][]byte{
		nil,
		[]byte("<html><body>final page</body></html>"),
		[]byte("bin\x00\x01\xff content \x00"),
		bytes.Repeat([]byte("<div>hosting parking</div>"), 8192), // ~168 KiB
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
				t.Errorf("Put(%d): the first Put must write new content, Created = false", i)
			}
			if want := content.HashOf(d); res.Hash != want {
				t.Errorf("Put(%d): hash = %s, want HashOf(data) = %s", i, res.Hash, want)
			}
			got, err := store.Get(ctx, res.Hash)
			if err != nil {
				t.Fatalf("Get(%d): %v", i, err)
			}
			if !bytes.Equal(got, d) {
				t.Errorf("Get(%d): the bytes do not match: got %d, want %d bytes", i, len(got), len(d))
			}
			// Get возвращает независимую копию (контракт метода).
			if len(got) > 0 {
				got[0] ^= 0xff
				again, err := store.Get(ctx, res.Hash)
				if err != nil {
					t.Fatalf("the repeated Get(%d): %v", i, err)
				}
				if !bytes.Equal(again, d) {
					t.Errorf("Get(%d): editing the first Get's result changed the store", i)
				}
			}
		}
	})

	t.Run("Put does not modify the data", func(t *testing.T) {
		store := newStore(t)
		d := []byte("mutable data")
		before := bytes.Clone(d)
		if _, err := store.Put(ctx, d); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if !bytes.Equal(d, before) {
			t.Error("Put modified the passed data")
		}
	})

	t.Run("Put idempotency", func(t *testing.T) {
		store := newStore(t)
		first, err := store.Put(ctx, data[1])
		if err != nil {
			t.Fatalf("the first Put: %v", err)
		}
		second, err := store.Put(ctx, bytes.Clone(data[1]))
		if err != nil {
			t.Fatalf("the repeated Put: %v", err)
		}
		if first.Hash != second.Hash {
			t.Errorf("the same content produced different hashes: %s and %s", first.Hash, second.Hash)
		}
		if second.Created {
			t.Error("the repeated Put wrote the bytes again: Created = true, want false (deduplication)")
		}
		if got, err := store.Get(ctx, second.Hash); err != nil || !bytes.Equal(got, data[1]) {
			t.Errorf("Get after the repeated Put: %v, match = %v", err, bytes.Equal(got, data[1]))
		}
	})

	t.Run("different content — a different Hash", func(t *testing.T) {
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
			t.Errorf("different content produced one hash %s", a.Hash)
		}
	})

	t.Run("references: Put ×N — Delete ×N", func(t *testing.T) {
		store := newStore(t)
		const n = 3
		res, err := store.Put(ctx, data[3])
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		for i := 1; i < n; i++ { // n ссылок всего
			if _, err := store.Put(ctx, data[3]); err != nil {
				t.Fatalf("the Put reference %d: %v", i, err)
			}
		}
		for i := 1; i < n; i++ { // пока есть ссылки, контент жив
			if err := store.Delete(ctx, res.Hash); err != nil {
				t.Fatalf("Delete %d: %v", i, err)
			}
			if _, err := store.Get(ctx, res.Hash); err != nil {
				t.Fatalf("Get after Delete %d of %d references: %v (the content is still needed)", i, n, err)
			}
		}
		if err := store.Delete(ctx, res.Hash); err != nil { // последняя ссылка
			t.Fatalf("Delete of the last reference: %v", err)
		}
		if _, err := store.Get(ctx, res.Hash); !errors.Is(err, content.ErrNotFound) {
			t.Errorf("Get after the references are exhausted: %v, want ErrNotFound", err)
		}
		if err := store.Delete(ctx, res.Hash); !errors.Is(err, content.ErrNotFound) {
			t.Errorf("Delete without references: %v, want ErrNotFound", err)
		}
	})

	t.Run("Get of a nonexistent one", func(t *testing.T) {
		store := newStore(t)
		for _, h := range []content.Hash{{}, content.HashOf([]byte("no such content"))} {
			if _, err := store.Get(ctx, h); !errors.Is(err, content.ErrNotFound) {
				t.Errorf("Get(%s): %v, want ErrNotFound", h, err)
			}
		}
	})

	t.Run("Delete of a nonexistent one", func(t *testing.T) {
		store := newStore(t)
		h := content.HashOf([]byte("no such content"))
		if err := store.Delete(ctx, h); !errors.Is(err, content.ErrNotFound) {
			t.Errorf("Delete(%s): %v, want ErrNotFound", h, err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		store := newStore(t)
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		d := []byte("data of a cancelled write")
		if _, err := store.Put(canceled, d); !errors.Is(err, context.Canceled) {
			t.Errorf("Put with a cancelled ctx: %v, want context.Canceled", err)
		}
		h := content.HashOf(d)
		if _, err := store.Get(canceled, h); !errors.Is(err, context.Canceled) {
			t.Errorf("Get with a cancelled ctx: %v, want context.Canceled", err)
		}
		if err := store.Delete(canceled, h); !errors.Is(err, context.Canceled) {
			t.Errorf("Delete with a cancelled ctx: %v, want context.Canceled", err)
		}
		// Отменённые операции не меняют хранилище: контент не появился.
		if _, err := store.Get(ctx, h); !errors.Is(err, content.ErrNotFound) {
			t.Errorf("Get after a cancelled Put: %v, want ErrNotFound", err)
		}
	})

	t.Run("parallel Puts of one content", func(t *testing.T) {
		store := newStore(t)
		const workers = 8
		var wg sync.WaitGroup
		hashes := make([]content.Hash, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				res, err := store.Put(ctx, data[3])
				if err != nil {
					t.Errorf("the goroutine's Put %d: %v", i, err)
					return
				}
				hashes[i] = res.Hash
			}(i)
		}
		wg.Wait()
		want := content.HashOf(data[3])
		for i, h := range hashes {
			if h != want {
				t.Fatalf("goroutine %d: hash = %s, want %s", i, h, want)
			}
		}
		// Каждый Put добавил свою ссылку: workers удалений исчерпывают контент.
		for i := 0; i < workers; i++ {
			if err := store.Delete(ctx, want); err != nil {
				t.Fatalf("Delete %d of %d: %v", i+1, workers, err)
			}
		}
		if _, err := store.Get(ctx, want); !errors.Is(err, content.ErrNotFound) {
			t.Errorf("Get after %d Deletes: %v, want ErrNotFound", workers, err)
		}
	})
}

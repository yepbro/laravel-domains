package content

// Контрактный набор StoreContract (T-041) — тестовый каркас проверок
// контракта ContentStore. Вынесен из store_test.go в обычный файл пакета:
// набор переиспользуется не только внутри пакета (pack_test.go, T-042),
// но и внешними тестами реализаций — интеграционными тестами хранилища
// против живой БД (test/integration, T-046); из _test-файла импорт
// внешнему пакету недоступен. Аналог stdlib-пакетов вида testing/fstest,
// но без отдельного пакета — набор оперирует только экспортируемым API.

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
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

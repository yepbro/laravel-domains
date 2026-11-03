// Прогон контрактного набора StoreContract на PackStore — во внешнем
// тест-пакете с T-228: набор живёт в contenttest (импортирует content),
// внутренние _test-файлы пакета импортировать его не могут — цикл
// импортов в тесте. Фабрика хранилища — экспорт internal-фабрики
// newPackTestStore через export_test.go (паттерн stdlib).
package content_test

import (
	"testing"

	"crawler/internal/content"
	"crawler/internal/content/contenttest"
)

// TestStoreContractOnPack — контракт ContentStore (T-041) на реализации
// пак-файлов: главная проверка T-042.
func TestStoreContractOnPack(t *testing.T) {
	contenttest.StoreContract(t, func(t *testing.T) content.ContentStore {
		return content.NewPackStoreForTest(t)
	})
}

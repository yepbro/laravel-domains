// Экспорт тестовых фабрик внешнему тест-пакету content_test — паттерн
// stdlib export_test.go. Нужен с T-228: контрактный набор StoreContract
// переехал в пакет contenttest, который импортирует content, поэтому
// внутренние _test-файлы пакета импортировать contenttest не могут
// (цикл импортов в тесте: content+tests → contenttest → content), а
// внешний content_test может (go перепривязывает его зависимость от
// content к тестовому варианту пакета). Файл существует только в
// тестовой сборке (суффикс _test.go) и в прод-бинарник не попадает.
package content

import "testing"

// NewPackStoreForTest — PackStore на фейковом pgx-пуле с реальными
// пак-файлами во временном каталоге (newPackTestStore, pack_test.go):
// для contenttest.StoreContract во внешнем тест-пакете
// (pack_contract_test.go).
func NewPackStoreForTest(t *testing.T) *PackStore {
	st, _ := newPackTestStore(t, nil)
	return st
}

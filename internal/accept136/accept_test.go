// Package accept136 — временный пакет приёмки T-136 (извещения Telegram),
// удаляется после проверки канала «Ошибка тестов».
package accept136

import "testing"

func TestAcceptanceT136IntentionalFail(t *testing.T) {
	t.Fatal("приёмка T-136: намеренный провал — канал «Ошибка тестов»")
}

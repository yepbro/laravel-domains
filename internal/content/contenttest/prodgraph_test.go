// Инвариант размещения набора (T-228, ревью P3-3): contenttest импортирует
// testing и потому обязан быть только в тестовой сборке. Нетестовой импорт
// contenttest тихо вернул бы testing (и сам contenttest) в прод-граф
// бинарников cmd — lint, vet и тесты при этом остались бы зелёными. Guard —
// единственная автоматическая проверка критерия «testing вне нетестовой
// сборки»; ручная проверка — go list -deps ./cmd/... | grep testing.
package contenttest

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNotInProductionGraph — прод-граф всех cmd-бинарников не содержит
// ни testing, ни contenttest (import-path-паттерн работает из любого
// каталога внутри модуля — тестовый бинарник запускается в каталоге
// пакета).
func TestNotInProductionGraph(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skipf("toolchain Go недоступна, проверка прод-графа пропущена: %v", err)
	}
	out, err := exec.Command("go", "list", "-deps", "crawler/cmd/...").Output()
	if err != nil {
		t.Fatalf("go list -deps crawler/cmd/...: %v", err)
	}
	var bad []string
	for _, pkg := range strings.Split(string(out), "\n") {
		switch pkg {
		case "testing", "crawler/internal/content/contenttest":
			bad = append(bad, pkg)
		}
	}
	if len(bad) > 0 {
		t.Errorf("прод-граф cmd содержит тестовые пакеты %v — contenttest "+
			"обязан импортироваться только из _test-файлов (T-228)", bad)
	}
}

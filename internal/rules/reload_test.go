// Тесты релоадера набора правил (T-068): начальная загрузка, замена набора
// при перезагрузке, сохранение прежнего набора при ошибке, подписка на
// смену, точка входа SIGHUP. «Фейковая ФС» — t.TempDir с перезаписываемыми
// файлами плюс шов load для подмены без диска.

package rules

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const reloadRules = `
rules:
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.25
`

const reloadRulesV2 = `
rules:
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.4
`

func rewriteRules(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReloaderInitialLoad(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil) // nil-логгер — допустимо (тихий)
	if err != nil {
		t.Fatal(err)
	}
	set := r.Set()
	if set == nil || len(set.Rules) != 1 || set.Rules[0].ID != "a_cookie" {
		t.Fatalf("начальный набор не загружен: %+v", set)
	}
	if r.Set() != set {
		t.Error("Set должен возвращать один и тот же снапшот между перезагрузками")
	}
}

func TestReloaderNewError(t *testing.T) {
	if _, err := NewReloader(filepath.Join(t.TempDir(), "нет-такого"), nil); err == nil {
		t.Error("ожидалась ошибка начальной загрузки")
	}
}

func TestReloaderReloadReplacesSet(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	got := r.Set()
	if got == old {
		t.Fatal("набор не заменён")
	}
	if got.Rules[0].Weight != 0.4 {
		t.Errorf("после перезагрузки weight = %v, want 0.4", got.Rules[0].Weight)
	}
	if got.Version == old.Version {
		t.Error("смена набора должна менять rules_version")
	}
}

func TestReloaderReloadFailureKeepsOldSet(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()

	// Битый набор: перезагрузка падает, сервис работает на прежнем.
	rewriteRules(t, dir, "rules: [\n")
	if err := r.Reload(); err == nil {
		t.Error("ожидалась ошибка перезагрузки")
	}
	if r.Set() != old {
		t.Error("при ошибке перезагрузки набор должен сохраниться")
	}

	// Каталог вовсе исчез — то же самое.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := r.Reload(); err == nil {
		t.Error("ожидалась ошибка перезагрузки (нет каталога)")
	}
	if r.Set() != old {
		t.Error("при ошибке перезагрузки набор должен сохраниться")
	}
}

func TestReloaderHandleSIGHUP(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()

	// Сигнальная точка входа не возвращает ошибку и не паникует —
	// ошибка уже залогирована, сервис продолжает работу.
	rewriteRules(t, dir, reloadRulesV2)
	r.HandleSIGHUP()
	if r.Set() == old {
		t.Error("HandleSIGHUP должен перезагружать набор")
	}
	rewriteRules(t, dir, "rules: [\n")
	r.HandleSIGHUP()
	if r.Set().Rules[0].Weight != 0.4 {
		t.Error("ошибка в HandleSIGHUP не должна трогать рабочий набор")
	}
}

// TestReloaderReloadClearsCompileCache — кэш компиляции не переживает
// смену набора: паттерны ушедших правил (и кэшированные ошибки) не
// накапливаются весь срок жизни процесса; неизменившийся набор кэш
// не трогает.
func TestReloaderReloadClearsCompileCache(t *testing.T) {
	const probe = `CACHE-PROBE-V1-` // уникальный паттерн, кроме этого теста не встречается
	dir := writeRules(t, `
rules:
  - id: r_html
    target: html
    regex: "`+probe+`"
    weight: 0.3
`)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := compileCache.Load(probe); ok {
		t.Fatalf("кэш загрязнён до теста: %s", probe)
	}
	r.Set().Evaluate(Page{HTML: []byte("x " + probe)}) // компиляция и кэширование
	if _, ok := compileCache.Load(probe); !ok {
		t.Fatal("паттерн не попал в кэш компиляции")
	}

	// Тот же набор (no-op по версии) — кэш не чистится.
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, ok := compileCache.Load(probe); !ok {
		t.Fatal("no-op перезагрузка не должна чистить кэш")
	}

	rewriteRules(t, dir, `
rules:
  - id: r_html
    target: html
    regex: "OTHER"
    weight: 0.4
`)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, ok := compileCache.Load(probe); ok {
		t.Error("смена rules_version должна очищать кэш компиляции")
	}
}

func TestReloaderReloadSameSetIsNoop(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()
	sub, cancel := r.Subscribe()
	defer cancel()

	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	if r.Set() != old {
		t.Error("перечтение того же набора — no-op, снапшот менять не нужно")
	}
	select {
	case <-sub:
		t.Error("уведомление при неизменившемся rules_version не нужно")
	default:
	}
}

func TestReloaderSubscribe(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	sub, cancel := r.Subscribe()
	defer cancel()

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-sub:
		if got != r.Set() {
			t.Error("подписчик должен получить новый снапшот")
		}
	case <-time.After(time.Second):
		t.Fatal("уведомление о смене набора не пришло")
	}
}

func TestReloaderSubscribeCancel(t *testing.T) {
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	sub, cancel := r.Subscribe()
	cancel()

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub:
		t.Error("после cancel уведомления не приходят")
	default:
	}
}

func TestReloaderSubscribeSlowConsumerGetsLatest(t *testing.T) {
	// Подписчик не читает канал две перезагрузки подряд: Reload не
	// блокируется, в канале остаётся самое свежее уведомление.
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	sub, _ := r.Subscribe()

	rewriteRules(t, dir, reloadRulesV2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}
	const third = `
rules:
  - id: a_cookie
    target: cookie_name
    match: "XSRF-TOKEN"
    weight: 0.6
`
	rewriteRules(t, dir, third)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-sub:
		if got.Rules[0].Weight != 0.6 {
			t.Errorf("в канале должно быть свежее уведомление, weight = %v", got.Rules[0].Weight)
		}
	default:
		t.Error("уведомление потеряно")
	}
}

func TestReloaderLoadSeam(t *testing.T) {
	// Шов load: подмена источника набора без файловой системы; ошибка
	// шва ведёт себя как ошибка загрузки — набор не меняется.
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := r.Set()

	boom := errors.New("сбой диска")
	r.load = func(string) (*Set, error) { return nil, boom }
	if err := r.Reload(); !errors.Is(err, boom) {
		t.Errorf("Reload должен вернуть ошибку шва, got %v", err)
	}
	if r.Set() != old {
		t.Error("ошибка шва не должна менять набор")
	}
}

func TestReloaderConcurrent(t *testing.T) {
	// Читатели Set параллельно с перезагрузками: снапшоты консистентны
	// (запускается и под -race: гонка между заменой набора и чтением).
	dir := writeRules(t, reloadRules)
	r, err := NewReloader(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if s := r.Set(); s == nil || len(s.Rules) != 1 {
					t.Error("неконсистентный снапшот")
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 10 {
			_ = r.Reload()
		}
	}()
	wg.Wait()
}

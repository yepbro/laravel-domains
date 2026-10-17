// Тесты обучаемого словаря zstd (T-043): обучение на детерминированном
// синтетическом корпусе, факт улучшения сжатия (без хрупких процентов),
// roundtrip, детерминизм, фолбэк без словаря, загрузка из файла.

package content

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dictCorpus — детерминированный корпус «парковочных страниц»: общий
// шаблон (шапка, навигация, футер) и несжимаемая случайная середина.
// Именно такие страницы — целевой сценарий словаря (ТЗ 5.7: парковки
// и заглушки). Генератор с фиксированным seed — корпус стабилен между
// прогонами, как требует ТЗ 11.1 к тестовым данным.
func dictCorpus(n, pageSize int) [][]byte {
	head := `<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width, initial-scale=1">` +
		`<title>Домен свободен</title><link rel="stylesheet" href="/parking.css">` +
		`<link rel="stylesheet" href="/vendor/bootstrap.min.css">` +
		`<script src="/vendor/analytics.js" defer></script></head><body>` +
		`<header class="site-header"><nav><ul class="nav-menu">` +
		`<li><a href="/">Главная</a></li><li><a href="/about">О компании</a></li>` +
		`<li><a href="/contacts">Контакты</a></li><li><a href="/order">Заказать</a></li>` +
		`</ul></nav></header><main class="container"><div class="row">` +
		`<div class="col-md-8"><section class="content"><h1>Домен продаётся</h1>`
	foot := `</section></div><aside class="col-md-4"><div class="banner">` +
		`Этот домен продаётся: admin@example.invalid</div></aside></div></main>` +
		`<footer class="site-footer"><p>&copy; Parking Ltd. Все права защищены.</p>` +
		`<p><a href="/terms">Условия</a> · <a href="/privacy">Конфиденциальность</a></p>` +
		`</footer></body></html>`
	rng := rand.New(rand.NewSource(1))
	midSize := pageSize - len(head) - len(foot)
	if midSize < 1 {
		midSize = 1
	}
	pages := make([][]byte, n)
	for i := 0; i < n; i++ {
		m := make([]byte, midSize)
		rng.Read(m)
		pages[i] = append(append([]byte(head), m...), foot...)
	}
	return pages
}

func TestTrainDictImprovesCompression(t *testing.T) {
	corpus := dictCorpus(24, 2048)
	d, err := TrainDict(corpus, TrainOptions{DictSize: 32 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}

	// Оценка на новой странице того же шаблона (не из обучающего корпуса).
	page := dictCorpus(1, 2048)[0]
	plain := Compress(page)
	withDict := d.Compress(page)
	if len(withDict) >= len(plain) {
		t.Errorf("словарь не улучшил сжатие: без словаря %d байт, со словарём %d",
			len(plain), len(withDict))
	}
	t.Logf("страница %d байт: без словаря %d, со словарём %d (%.1f%% исходного)",
		len(page), len(plain), len(withDict), 100*float64(len(withDict))/float64(len(page)))

	got, err := d.Decompress(withDict)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Error("roundtrip со словарём изменил данные")
	}

	// Кадры, сжатые без словаря, декодер со словарём тоже читает.
	got, err = d.Decompress(plain)
	if err != nil {
		t.Fatalf("Decompress беззоварного кадра: %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Error("roundtrip беззоварного кадра изменил данные")
	}
}

func TestTrainDictDeterministic(t *testing.T) {
	corpus := dictCorpus(12, 2048)
	a, err := TrainDict(corpus, TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	b, err := TrainDict(corpus, TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict (повтор): %v", err)
	}
	if !bytes.Equal(a.Raw(), b.Raw()) {
		t.Error("один и тот же корпус дал разные словари")
	}
	if a.ID() == 0 {
		t.Error("ID словаря 0 — форматом запрещён")
	}
}

func TestTrainDictValidation(t *testing.T) {
	if _, err := TrainDict(nil, TrainOptions{}); err == nil {
		t.Error("пустой корпус: ожидалась ошибка")
	}
	// Нет повторов: случайные данные, каждый сегмент уникален.
	random := make([][]byte, 8)
	for i := range random {
		b := make([]byte, 512)
		for j := range b {
			b[j] = byte(i*131 + j*37 + j*j%251)
		}
		random[i] = b
	}
	if _, err := TrainDict(random, TrainOptions{DictSize: 1 << 10}); err == nil {
		t.Error("корпус без повторов: ожидалась ошибка")
	}
	for _, size := range []int{MinDictSize - 1, MaxDictSize + 1} {
		if _, err := TrainDict(dictCorpus(4, 1024), TrainOptions{DictSize: size}); err == nil {
			t.Errorf("DictSize=%d: ожидалась ошибка", size)
		}
	}
	// Нулевой DictSize — дефолт, ошибка только если корпус не тянет на минимум.
	if _, err := TrainDict(dictCorpus(16, 4096), TrainOptions{}); err != nil {
		t.Errorf("DictSize по умолчанию: %v", err)
	}
}

func TestDictSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dict.zst")

	d, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	if err := d.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := LoadDict(path)
	if err != nil {
		t.Fatalf("LoadDict: %v", err)
	}
	if loaded.ID() != d.ID() {
		t.Errorf("ID после загрузки %d, хочу %d", loaded.ID(), d.ID())
	}
	page := dictCorpus(1, 2048)[0]
	got, err := loaded.Decompress(loaded.Compress(page))
	if err != nil || !bytes.Equal(got, page) {
		t.Errorf("roundtrip загруженного словаря: %v", err)
	}

	// Временных файлов после Save не осталось.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("в каталоге кроме словаря что-то осталось: %v", names)
	}

	// Отсутствующий файл — fs.ErrNotExist, битый — ошибка формата.
	if _, err := LoadDict(filepath.Join(dir, "nope.zst")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("отсутствующий файл: err = %v, хочу fs.ErrNotExist", err)
	}
	bad := filepath.Join(dir, "bad.zst")
	if err := os.WriteFile(bad, []byte("это не словарь"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDict(bad); err == nil {
		t.Error("битый файл словаря: ожидалась ошибка")
	}
}

func TestWithDictPackageFallback(t *testing.T) {
	t.Cleanup(func() { SetDict(nil) })

	// Без словаря — то же сжатие, что каркасное.
	src := []byte("<html>парковка домена</html>")
	SetDict(nil)
	if got := CompressWithDict(src); !bytes.Equal(got, Compress(src)) {
		t.Error("без установленного словаря CompressWithDict должен совпадать с Compress")
	}
	got, err := DecompressWithDict(Compress(src))
	if err != nil || !bytes.Equal(got, src) {
		t.Errorf("фолбэк-распаковка: %v", err)
	}

	// Со словарём — сжатие словарное, распаковка понимает оба вида кадров.
	d, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	SetDict(d)
	page := dictCorpus(1, 2048)[0]
	got, err = DecompressWithDict(CompressWithDict(page))
	if err != nil || !bytes.Equal(got, page) {
		t.Errorf("roundtrip через активный словарь: %v", err)
	}
	got, err = DecompressWithDict(Compress(page))
	if err != nil || !bytes.Equal(got, page) {
		t.Errorf("активный словарь не читает беззоварный кадр: %v", err)
	}
}

func TestSetDictFile(t *testing.T) {
	t.Cleanup(func() { SetDict(nil) })
	var logBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, nil))

	dir := t.TempDir()
	path := filepath.Join(dir, "dict.zst")
	d, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	if err := d.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	ok, err := SetDictFile(path, log)
	if err != nil || !ok {
		t.Fatalf("SetDictFile: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(logBuf.String(), "словарь zstd загружен") {
		t.Errorf("загрузка не отражена в логе:\n%s", logBuf.String())
	}

	// Отсутствующий файл — не ошибка, работа без словаря.
	logBuf.Reset()
	ok, err = SetDictFile(filepath.Join(dir, "absent.zst"), log)
	if err != nil || ok {
		t.Fatalf("отсутствующий файл: ok=%v err=%v, хочу (false, nil)", ok, err)
	}
	if !strings.Contains(logBuf.String(), "без словаря") {
		t.Errorf("фолбэк не отражён в логе:\n%s", logBuf.String())
	}
	if got := CompressWithDict(nil); !bytes.Equal(got, Compress(nil)) {
		t.Error("после фолбэка CompressWithDict должен совпадать с Compress")
	}

	// Пустой путь — просто отключение, без записи в лог.
	logBuf.Reset()
	if ok, err := SetDictFile("", log); err != nil || ok {
		t.Fatalf("пустой путь: ok=%v err=%v", ok, err)
	}
	if logBuf.Len() != 0 {
		t.Errorf("пустой путь не должен логироваться:\n%s", logBuf.String())
	}

	// Битый файл — ошибка (молчаливый фолбэк скрыл бы проблему).
	bad := filepath.Join(dir, "bad.zst")
	if err := os.WriteFile(bad, []byte("мусор"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(bad, log); err == nil {
		t.Error("битый файл словаря: ожидалась ошибка")
	}
}

func TestTrainDictSyntheticScanLimit(t *testing.T) {
	// Корпус больше trainScanLimit: обучение берёт префикс и работает.
	corpus := dictCorpus(64, 128<<10)
	if _, err := TrainDict(corpus, TrainOptions{DictSize: 16 << 10}); err != nil {
		t.Fatalf("TrainDict на корпусе сверх лимита сканирования: %v", err)
	}
}

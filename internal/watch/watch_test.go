package watch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Параметры наблюдателя в тестах: миллисекундные интервалы, чтобы проверка
// стабильности и обход были быстрыми, но детерминизм обеспечивают запасы
// между таймаутами (окно стабильности заметно больше периодов проверки).
const (
	testStable = 120 * time.Millisecond
	testScan   = 200 * time.Millisecond
	testQuiet  = 60 * time.Millisecond // < testStable - интервал проверки
)

// start запускает наблюдателя над dir и возвращает канал готовых файлов;
// наблюдатель останавливается с тестом.
func start(t *testing.T, dir string, mutate func(*Options)) <-chan File {
	t.Helper()
	opts := Options{
		Dir:          dir,
		StableAfter:  testStable,
		ScanInterval: testScan,
		QueueSize:    8,
	}
	if mutate != nil {
		mutate(&opts)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	files, err := Dir(ctx, opts)
	if err != nil {
		t.Fatalf("Dir: %v", err)
	}
	return files
}

// waitFor читает следующий готовый файл или падает по таймауту.
func waitFor(t *testing.T, files <-chan File) File {
	t.Helper()
	select {
	case f, ok := <-files:
		if !ok {
			t.Fatal("канал готовых файлов закрыт")
		}
		return f
	case <-time.After(3 * time.Second):
		t.Fatal("готовый файл не получен за 3 с")
		return File{}
	}
}

// expectEmpty проверяет, что за quiet ничего не выдано.
func expectEmpty(t *testing.T, files <-chan File, quiet time.Duration) {
	t.Helper()
	select {
	case f := <-files:
		t.Errorf("неожижиданная выдача файла %q", f.Path)
	case <-time.After(quiet):
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDir_StableFileDelivered(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	name := filepath.Join(dir, "crawl.test", "add", "seed.txt")
	writeFile(t, name, "example.com\n")

	f := waitFor(t, files)
	if f.Path != name {
		t.Errorf("выдан путь %q, want %q", f.Path, name)
	}
	if f.Size != int64(len("example.com\n")) {
		t.Errorf("размер = %d, want %d", f.Size, len("example.com\n"))
	}
}

func TestDir_GrowingFileNotDeliveredUntilQuiet(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	name := filepath.Join(dir, "crawl.test", "add", "growing.txt")
	writeFile(t, name, "example.com\n")

	// Дозапись каждые 30 мс в течение ~360 мс: окна стабильности 120 мс
	// не возникает, файл не выдаётся.
	stop := time.After(360 * time.Millisecond)
	for {
		select {
		case <-stop:
		default:
			f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = f.WriteString("example.com\n")
			_ = f.Close()
			time.Sleep(30 * time.Millisecond)
			continue
		}
		break
	}
	expectEmpty(t, files, testQuiet) // запись только что шла — выдачи быть не должно
	waitFor(t, files)                // после затишья файл готов
}

func TestDir_TmpPartHiddenIgnored(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	sub := filepath.Join(dir, "crawl.test", "add")
	writeFile(t, filepath.Join(sub, "a.txt.tmp"), "x")
	writeFile(t, filepath.Join(sub, "b.part"), "x")
	writeFile(t, filepath.Join(sub, ".hidden.txt"), "x")
	expectEmpty(t, files, 3*testScan)

	// Переименование *.tmp → *.txt (ТЗ 5.1) — файл готов после окна
	// стабильности.
	if err := os.Rename(filepath.Join(sub, "a.txt.tmp"), filepath.Join(sub, "a.txt")); err != nil {
		t.Fatal(err)
	}
	if f := waitFor(t, files); filepath.Base(f.Path) != "a.txt" {
		t.Errorf("выдан %q, want a.txt", f.Path)
	}
}

func TestDir_ScanPicksPreExistingFile(t *testing.T) {
	dir := t.TempDir()
	// Файл существует до старта наблюдателя: событий fsnotify нет,
	// выдаёт его только обход-страховка.
	name := filepath.Join(dir, "crawl.test", "add", "old.txt")
	writeFile(t, name, "example.com\n")

	files := start(t, dir, nil)
	if f := waitFor(t, files); f.Path != name {
		t.Errorf("выдан путь %q, want %q", f.Path, name)
	}
}

func TestDir_NewNestedDirsWatched(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	// Каталоги зоны создаются после старта наблюдателя: watch добавляется
	// на лету, файл глубине <зона>/add/ подхватывается.
	name := filepath.Join(dir, "новая-зона.test", "add", "seed.txt")
	writeFile(t, name, "example.com\n")
	if f := waitFor(t, files); f.Path != name {
		t.Errorf("выдан путь %q, want %q", f.Path, name)
	}
}

func TestDir_NoDuplicateButReplacedFileRedelivered(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	name := filepath.Join(dir, "crawl.test", "add", "seed.txt")
	writeFile(t, name, "one\n")
	waitFor(t, files)
	expectEmpty(t, files, 3*testScan) // повторная выдача исключена

	// Тот же путь, новое содержимое — новый файл, выдаётся снова.
	writeFile(t, name, "one\ntwo\n")
	f := waitFor(t, files)
	if f.Size != int64(len("one\ntwo\n")) {
		t.Errorf("размер перезаписанного файла = %d, want %d", f.Size, len("one\ntwo\n"))
	}
}

func TestDir_DeletedBeforeStableNotDelivered(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	name := filepath.Join(dir, "crawl.test", "add", "gone.txt")
	writeFile(t, name, "example.com\n")
	time.Sleep(testStable / 3)
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	expectEmpty(t, files, 3*testScan)
}

func TestDir_ProcessedFailedSkipped(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	writeFile(t, filepath.Join(dir, "processed", "done.txt"), "x")
	writeFile(t, filepath.Join(dir, "failed", "bad.txt"), "x")
	expectEmpty(t, files, 3*testScan)

	// Контроль: файл в зоне после этого выдаётся — наблюдатель жив.
	writeFile(t, filepath.Join(dir, "crawl.test", "add", "ok.txt"), "x")
	waitFor(t, files)
}

func TestDir_BackpressureNoLoss(t *testing.T) {
	dir := t.TempDir()
	// Канал на 2 файла, файлов 5: наблюдатель обязан заблокироваться,
	// но не потерять файлы.
	files := start(t, dir, func(o *Options) { o.QueueSize = 2 })

	var want []string
	for i := 0; i < 5; i++ {
		name := filepath.Join(dir, "crawl.test", "add", string(rune('a'+i))+".txt")
		writeFile(t, name, "example.com\n")
		want = append(want, name)
	}
	time.Sleep(3 * testStable) // буфер заполнен и send заблокирован

	got := make(map[string]bool)
	for i := 0; i < 5; i++ {
		f := waitFor(t, files)
		got[f.Path] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("файл %q потерян при backpressure", name)
		}
	}

	// Цикл наблюдения жив после разблокировки.
	name := filepath.Join(dir, "crawl.test", "add", "z.txt")
	writeFile(t, name, "example.com\n")
	if f := waitFor(t, files); f.Path != name {
		t.Errorf("после backpressure выдан %q, want %q", f.Path, name)
	}
}

func TestDir_CancelClosesChannel(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	files, err := Dir(ctx, Options{
		Dir:          dir,
		StableAfter:  testStable,
		ScanInterval: testScan,
	})
	if err != nil {
		t.Fatalf("Dir: %v", err)
	}

	cancel()
	select {
	case _, ok := <-files:
		if ok {
			t.Error("после cancel в канале осталось значение")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("канал не закрылся после cancel")
	}
}

func TestDir_MissingDirCreated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "incoming")
	files := start(t, dir, nil)

	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		t.Fatalf("отсутствующий каталог не создан: %v", err)
	}

	name := filepath.Join(dir, "crawl.test", "add", "seed.txt")
	writeFile(t, name, "example.com\n")
	if f := waitFor(t, files); f.Path != name {
		t.Errorf("выдан путь %q, want %q", f.Path, name)
	}
}

func TestDir_RequiresDir(t *testing.T) {
	if _, err := Dir(context.Background(), Options{}); err == nil {
		t.Error("ожидалась ошибка при пустом Options.Dir")
	}
}

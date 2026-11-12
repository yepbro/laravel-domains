package watch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The watcher parameters in the tests: millisecond intervals so the
// stability check and the pass are fast; the determinism is provided by
// the margins between the timeouts (the stability window is noticeably
// larger than the check periods).
const (
	testStable = 120 * time.Millisecond
	testScan   = 200 * time.Millisecond
	testQuiet  = 60 * time.Millisecond // < testStable - the check interval
)

// start starts the watcher over dir and returns the channel of ready
// files; the watcher stops with the test.
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

// waitFor reads the next ready file or fails by timeout.
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

// expectEmpty checks that nothing was delivered within quiet.
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

	// Appends every 30 ms for ~360 ms: the 120 ms stability window never
	// arises, the file is not delivered.
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
	expectEmpty(t, files, testQuiet) // the writing just happened — there must be no delivery
	waitFor(t, files)                // after the quiet the file is ready
}

func TestDir_TmpPartHiddenIgnored(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	sub := filepath.Join(dir, "crawl.test", "add")
	writeFile(t, filepath.Join(sub, "a.txt.tmp"), "x")
	writeFile(t, filepath.Join(sub, "b.part"), "x")
	writeFile(t, filepath.Join(sub, ".hidden.txt"), "x")
	expectEmpty(t, files, 3*testScan)

	// A rename *.tmp → *.txt (spec 5.1) — the file is ready after the
	// stability window.
	if err := os.Rename(filepath.Join(sub, "a.txt.tmp"), filepath.Join(sub, "a.txt")); err != nil {
		t.Fatal(err)
	}
	if f := waitFor(t, files); filepath.Base(f.Path) != "a.txt" {
		t.Errorf("выдан %q, want a.txt", f.Path)
	}
}

// TestDir_ArchiveExtensions — archives of supported formats (T-133) are
// tracked on a par with *.txt: the same readiness rules; unsupported
// formats, temporary and hidden names are not delivered.
func TestDir_ArchiveExtensions(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	sub := filepath.Join(dir, "crawl.test", "add")
	want := []string{"a.zip", "b.tar", "c.tar.gz", "d.tgz", "e.gz", "F.ZIP"}
	for _, name := range want {
		writeFile(t, filepath.Join(sub, name), "example.com\n")
	}
	writeFile(t, filepath.Join(sub, "g.rar"), "x")     // not supported
	writeFile(t, filepath.Join(sub, "h.7z"), "x")      // not supported
	writeFile(t, filepath.Join(sub, "i.zip.tmp"), "x") // a temporary name
	writeFile(t, filepath.Join(sub, ".j.zip"), "x")    // hidden

	got := make(map[string]bool, len(want))
	for range want {
		f := waitFor(t, files)
		got[filepath.Base(f.Path)] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("архив %s не выдан (выданы %v)", name, got)
		}
	}
	// Nothing extra (rar/7z/tmp/hidden).
	expectEmpty(t, files, 3*testScan)
}

// TestDir_GrowingArchiveNotDeliveredUntilQuiet — an archive being appended
// (T-133) is not delivered until the stability window: the same readiness
// rules as for txt.
func TestDir_GrowingArchiveNotDeliveredUntilQuiet(t *testing.T) {
	dir := t.TempDir()
	files := start(t, dir, nil)

	name := filepath.Join(dir, "crawl.test", "add", "growing.gz")
	writeFile(t, name, "example.com\n")

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
	expectEmpty(t, files, testQuiet) // the writing just happened — there must be no delivery
	waitFor(t, files)                // after the quiet the archive is ready
}

func TestDir_ScanPicksPreExistingFile(t *testing.T) {
	dir := t.TempDir()
	// The file existed before the watcher started: there are no fsnotify
	// events; only the safety-net pass delivers it.
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

	// The zone directories are created after the watcher start: the watch
	// is added on the fly, a file at the depth of <zone>/add/ is picked
	// up.
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
	expectEmpty(t, files, 3*testScan) // redelivery is excluded

	// The same path, new content — a new file, delivered again.
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

	// The control: a file in a zone is delivered after this — the watcher
	// is alive.
	writeFile(t, filepath.Join(dir, "crawl.test", "add", "ok.txt"), "x")
	waitFor(t, files)
}

func TestDir_BackpressureNoLoss(t *testing.T) {
	dir := t.TempDir()
	// A channel of 2 files, 5 files: the watcher must block but not lose
	// the files.
	files := start(t, dir, func(o *Options) { o.QueueSize = 2 })

	var want []string
	for i := 0; i < 5; i++ {
		name := filepath.Join(dir, "crawl.test", "add", string(rune('a'+i))+".txt")
		writeFile(t, name, "example.com\n")
		want = append(want, name)
	}
	time.Sleep(3 * testStable) // the buffer is full and the send is blocked

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

	// The watch loop is alive after the unblocking.
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

// TestDir_HiddenRootWatched — a hidden ingest.dir (/data/.incoming) is a
// valid configuration (config.Validate only requires non-emptiness): the
// hiddenness of the root does not disable the watch (ING-P3-4); hidden
// subdirectories inside the root are still ignored.
func TestDir_HiddenRootWatched(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".incoming")
	files := start(t, dir, nil)

	name := filepath.Join(dir, "crawl.test", "add", "seed.txt")
	writeFile(t, name, "example.com\n")
	if f := waitFor(t, files); f.Path != name {
		t.Errorf("выдан путь %q, want %q (скрытый корень наблюдается)", f.Path, name)
	}

	writeFile(t, filepath.Join(dir, ".hidden", "add", "x.txt"), "x")
	expectEmpty(t, files, 3*testScan)
}

func TestDir_RequiresDir(t *testing.T) {
	if _, err := Dir(context.Background(), Options{}); err == nil {
		t.Error("ожидалась ошибка при пустом Options.Dir")
	}
}

package watch

import (
	"testing"
	"time"
)

// fakeClock — детерминированное время для тестов трекера.
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(0, 0)} }

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) Add(d time.Duration) { c.t = c.t.Add(d) }

func TestTrackerStableAfterWindow(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	if !tr.observe("f.txt", 42, mtime, false) {
		t.Fatal("observe нового файла должен делать его кандидатом")
	}

	clock.Add(10*time.Second - time.Nanosecond)
	if _, ok := tr.ready("f.txt", 42, mtime); ok {
		t.Error("файл выдан раньше истечения окна стабильности")
	}

	clock.Add(time.Nanosecond)
	f, ok := tr.ready("f.txt", 42, mtime)
	if !ok {
		t.Fatal("файл не выдан после окна стабильности")
	}
	if f.Path != "f.txt" || f.Size != 42 || !f.ModTime.Equal(mtime) {
		t.Errorf("File = %+v, want path f.txt, size 42, mtime %v", f, mtime)
	}

	// Повторная выдача того же файла исключена.
	if _, ok := tr.ready("f.txt", 42, mtime); ok {
		t.Error("файл выдан второй раз")
	}
}

func TestTrackerEventResetsWindow(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)

	// Событие записи при той же сигнатуре (грубый mtime на ФС): окно
	// обязано начаться заново.
	if !tr.observe("f.txt", 42, mtime, true) {
		t.Fatal("событие записи не вернуло файл в кандидаты")
	}
	if _, ok := tr.ready("f.txt", 42, mtime); ok {
		t.Error("событие записи не сбросило окно стабильности")
	}
	clock.Add(10 * time.Second)
	if _, ok := tr.ready("f.txt", 42, mtime); !ok {
		t.Error("файл не выдан после нового окна")
	}
}

func TestTrackerScanDoesNotResetUnchanged(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)

	// Плановый обход с неизменной сигнатурой окно не сбрасывает.
	tr.observe("f.txt", 42, mtime, false)
	if _, ok := tr.ready("f.txt", 42, mtime); !ok {
		t.Error("неизменный при обходе файл не выдан")
	}
}

func TestTrackerChangedSignatureResets(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	tr.observe("f.txt", 42, time.Unix(1000, 0), false)
	clock.Add(10 * time.Second)

	// Дозапись: изменились размер и mtime — новое окно.
	mtime2 := time.Unix(1005, 0)
	if !tr.observe("f.txt", 100, mtime2, false) {
		t.Fatal("изменённый файл не остался кандидатом")
	}
	if _, ok := tr.ready("f.txt", 100, mtime2); ok {
		t.Error("изменённая сигнатура выдала файл без нового окна")
	}
	if _, ok := tr.ready("f.txt", 42, time.Unix(1000, 0)); ok {
		t.Error("выдана устаревшая сигнатура кандидата")
	}
	clock.Add(10 * time.Second)
	if f, ok := tr.ready("f.txt", 100, mtime2); !ok || f.Size != 100 {
		t.Errorf("после нового окна файл не выдан или размер неверен: %+v ok=%v", f, ok)
	}
}

func TestTrackerReadyRequiresExactCurrentState(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	tr.observe("f.txt", 42, time.Unix(1000, 0), false)
	clock.Add(10 * time.Second)

	// Текущее состояние отличается от наблюдавшегося (файл растёт) —
	// выдачи нет, окно начнётся заново при следующем observe.
	if _, ok := tr.ready("f.txt", 50, time.Unix(1000, 0)); ok {
		t.Error("файл с изменившимся размером выдан как стабильный")
	}
}

func TestTrackerForget(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)
	tr.forget("f.txt")

	if _, ok := tr.ready("f.txt", 42, mtime); ok {
		t.Error("забытый (удалённый) файл выдан")
	}
	if got := tr.paths(); len(got) != 0 {
		t.Errorf("paths после forget = %v, want пусто", got)
	}
}

func TestTrackerDeliveredSameSignatureNotTrackedAgain(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(10*time.Second, clock.Now)

	mtime := time.Unix(1000, 0)
	tr.observe("f.txt", 42, mtime, false)
	clock.Add(10 * time.Second)
	if _, ok := tr.ready("f.txt", 42, mtime); !ok {
		t.Fatal("файл не выдан")
	}

	// Тот же файл снова замечен обходом — не кандидат.
	if tr.observe("f.txt", 42, mtime, false) {
		t.Error("выданный файл с той же сигнатурой снова стал кандидатом")
	}

	// Новый файл по тому же пути (другая сигнатура) — кандидат.
	if !tr.observe("f.txt", 43, mtime.Add(time.Second), false) {
		t.Error("новый файл по известному пути не стал кандидатом")
	}
	clock.Add(10 * time.Second)
	if _, ok := tr.ready("f.txt", 43, mtime.Add(time.Second)); !ok {
		t.Error("новый файл по тому же пути не выдан")
	}
}

func TestTrackerPathsSorted(t *testing.T) {
	clock := newFakeClock()
	tr := newTracker(time.Second, clock.Now)
	mtime := time.Unix(1000, 0)
	for _, p := range []string{"c.txt", "a.txt", "b.txt"} {
		tr.observe(p, 1, mtime, false)
	}
	got := tr.paths()
	want := []string{"a.txt", "b.txt", "c.txt"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paths = %v, want %v", got, want)
		}
	}
}

// Юнит-тесты контроля свободного места каталога контента (T-046; ТЗ 8):
// заглушаемый statfs (SpaceChecker), порог паузы записи, дедуп и чтение
// продолжают работать, восстановление после освобождения, событие алерта,
// периодическая проверка DiskWatchLoop (Warn/Info и переходы). Стиль
// pack_test.go/compact_test.go: фейковый пул БД, реальные файлы в t.TempDir.
package content

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSpace — заглушаемый SpaceChecker: снимок места и число вызовов
// (пауза Put и DiskWatchLoop должны спрашивать место по одному разу
// на операцию).
type fakeSpace struct {
	mu    sync.Mutex
	stat  SpaceStat
	err   error
	calls int
}

func (f *fakeSpace) Space(string) (SpaceStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.stat, f.err
}

func (f *fakeSpace) set(avail, total uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stat = SpaceStat{Avail: avail, Total: total}
}

func (f *fakeSpace) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeSpace) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fullSink — сборщик событий алерта (хук вызывается из Put и горутины
// DiskWatchLoop — синхронизация обязательна).
type fullSink struct {
	mu     sync.Mutex
	events []StoreFullEvent
}

func (s *fullSink) hook(ev StoreFullEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
}

func (s *fullSink) snapshot() []StoreFullEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]StoreFullEvent(nil), s.events...)
}

// newDiskTestStore — хранилище с подменённым statfs и порогом 5%
// (место 50/100 = 50% — «много», 4/100 = 4% — «ниже порога»).
func newDiskTestStore(t *testing.T, mutate func(*PackOptions)) (*PackStore, *packFakePool, *fakeSpace) {
	t.Helper()
	st, pool := newPackTestStore(t, func(o *PackOptions) {
		o.DiskFreePercent = 5
		if mutate != nil {
			mutate(o)
		}
	})
	fake := &fakeSpace{}
	fake.set(50, 100)
	st.stat = fake
	return st, pool, fake
}

// TestOSStatfs — реальный statfs временного каталога: ёмкость известна,
// доступно не больше ёмкости; несуществующий каталог — ошибка.
func TestOSStatfs(t *testing.T) {
	var osStat OSStatfs
	sp, err := osStat.Space(t.TempDir())
	if err != nil {
		t.Fatalf("Space: %v", err)
	}
	if sp.Total == 0 {
		t.Error("Total = 0 — statfs не разглядел файловую систему")
	}
	if sp.Avail > sp.Total {
		t.Errorf("Avail %d > Total %d", sp.Avail, sp.Total)
	}
	if _, err := osStat.Space(filepath.Join(t.TempDir(), "нет-такого")); err == nil {
		t.Error("Space несуществующего каталога: ожидалась ошибка")
	}
}

// TestPutPauseOnLowDisk — пауза записи контента (ТЗ 8): места ниже порога —
// Put нового контента возвращает ErrStoreFull, блок и строка метаданных
// не появляются; событие алерта уходит в хук со снимком места.
func TestPutPauseOnLowDisk(t *testing.T) {
	var sink *fullSink
	st, pool, fake := newDiskTestStore(t, func(o *PackOptions) {
		sink = &fullSink{}
		o.OnStoreFull = sink.hook
	})
	ctx := context.Background()

	good := []byte("<html>контент до переполнения</html>")
	res, err := st.Put(ctx, good)
	if err != nil {
		t.Fatalf("Put при месте: %v", err)
	}

	fake.set(4, 100) // 4% < порога 5%
	big := []byte("<html>новый контент в полный диск</html>")
	packBefore := compactPackSize(t, st.opts.Dir, 1)
	_, err = st.Put(ctx, big)
	if !errors.Is(err, ErrStoreFull) {
		t.Fatalf("Put при нехватке места: %v, want ErrStoreFull", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "4.0%") || !strings.Contains(msg, "5.0%") {
		t.Errorf("ошибка без доли и порога: %q", msg)
	}
	if _, ok := pool.row(HashOf(big)); ok {
		t.Error("строка метаданных появилась несмотря на ErrStoreFull")
	}
	if got := compactPackSize(t, st.opts.Dir, 1); got != packBefore {
		t.Errorf("пак вырос несмотря на ErrStoreFull: %d → %d байт", packBefore, got)
	}

	events := sink.snapshot()
	if len(events) != 1 {
		t.Fatalf("событий алерта %d, want 1 (каждый остановленный Put — счётчик)", len(events))
	}
	ev := events[0]
	if ev.Source != StoreFullPut || ev.Recovered {
		t.Errorf("событие %+v: want Source=%s, Recovered=false", ev, StoreFullPut)
	}
	if ev.Space.Avail != 4 || ev.Space.Total != 100 {
		t.Errorf("событие без снимка места: %+v", ev.Space)
	}

	// Дедуп продолжает работать: байты не расходуются, ссылка добавляется.
	dup, err := st.Put(ctx, good)
	if err != nil {
		t.Fatalf("дедуп-Put в полный диск: %v (запись контента на паузе, ссылки — нет)", err)
	}
	if dup.Created || dup.Hash != res.Hash {
		t.Errorf("дедуп-Put: %+v, want Created=false с тем же хэшем", dup)
	}
	if row, ok := pool.row(res.Hash); !ok || row.refc != 2 {
		t.Errorf("refcount после дедупа в паузе: %+v, want 2", row)
	}
	// Чтение не останавливается: контент нужен анализатору и перепроверкам.
	got, err := st.Get(ctx, res.Hash)
	if err != nil || !bytes.Equal(got, good) {
		t.Errorf("Get в паузе: %v, совпадение=%v", err, bytes.Equal(got, good))
	}
}

// TestPutResumeAfterFree — восстановление после освобождения (ТЗ 8):
// место вернулось за порог — запись продолжается, отдельного счётчика
// занятого нет, statfs видит актуальное состояние (после компактификации
// T-045 — то же самое: паки удалены, место свободно).
func TestPutResumeAfterFree(t *testing.T) {
	st, pool, fake := newDiskTestStore(t, nil)
	ctx := context.Background()

	fake.set(4, 100)
	if _, err := st.Put(ctx, []byte("в полный диск")); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("Put в полный диск: %v, want ErrStoreFull", err)
	}
	fake.set(50, 100) // место освободили (компактификация, ручная чистка)
	res, err := st.Put(ctx, []byte("после освобождения"))
	if err != nil {
		t.Fatalf("Put после освобождения: %v", err)
	}
	if !res.Created {
		t.Error("Put после освобождения не записал контент")
	}
	if _, ok := pool.row(res.Hash); !ok {
		t.Error("строка метаданных не появилась после освобождения")
	}
}

// TestPutDiskCheckDisabled — нулевой порог выключает контроль: statfs
// не вызывается вовсе (и не падает на заглушке с нулевым снимком).
func TestPutDiskCheckDisabled(t *testing.T) {
	st, _, fake := newDiskTestStore(t, func(o *PackOptions) { o.DiskFreePercent = 0 })
	ctx := context.Background()
	if _, err := st.Put(ctx, []byte("без контроля диска")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n := fake.count(); n != 0 {
		t.Errorf("statfs вызван %d раз при выключенном контроле, want 0", n)
	}
}

// TestPutDiskStatError — ошибка statfs — инфраструктурная, а не вердикт
// «место кончилось»: Put возвращает её как есть (не ErrStoreFull).
func TestPutDiskStatError(t *testing.T) {
	st, _, fake := newDiskTestStore(t, nil)
	fake.setErr(errors.New("statfs недоступен"))
	_, err := st.Put(context.Background(), []byte("контент"))
	if err == nil || errors.Is(err, ErrStoreFull) {
		t.Fatalf("ошибка statfs замаскирована: %v, want обычная ошибка", err)
	}
}

// TestPutDiskZeroTotal — экзотическая ФС без ёмкости: доля неопределима,
// пауза не срабатывает (ложная остановка записи хуже пропущенного порога).
func TestPutDiskZeroTotal(t *testing.T) {
	st, _, fake := newDiskTestStore(t, nil)
	fake.set(0, 0)
	res, err := st.Put(context.Background(), []byte("нулевая ёмкость"))
	if err != nil {
		t.Fatalf("Put при Total=0: %v", err)
	}
	if !res.Created {
		t.Error("контент не записан при Total=0")
	}
}

// TestPutNoHook — хук не задан: пауза работает, паники нет.
func TestPutNoHook(t *testing.T) {
	st, _, fake := newDiskTestStore(t, func(o *PackOptions) { o.OnStoreFull = nil })
	fake.set(4, 100)
	if _, err := st.Put(context.Background(), []byte("без хука")); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("Put: %v, want ErrStoreFull", err)
	}
}

// TestDiskWatchLoop — периодическая проверка (ТЗ 8): Warn при входе
// в паузу и Info при восстановлении, события только на переходах;
// отмена ctx — чистый nil.
func TestDiskWatchLoop(t *testing.T) {
	var (
		logBuf lockedBuffer
		sink   *fullSink
	)
	st, _, fake := newDiskTestStore(t, func(o *PackOptions) {
		sink = &fullSink{}
		o.OnStoreFull = sink.hook
	})
	log := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	lctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- DiskWatchLoop(lctx, st, DiskWatchOptions{Interval: 10 * time.Millisecond, Log: log})
	}()

	waitFor(t, func() bool { return fake.count() > 0 }, "первый проход DiskWatchLoop")
	// Первый проход при месте (50%): ни события, ни Warn.
	if evs := sink.snapshot(); len(evs) != 0 {
		t.Fatalf("событий при месте %d, want 0", len(evs))
	}

	fake.set(4, 100) // место упало ниже порога
	waitFor(t, func() bool { return logBuf.contains("пауза записи контента") }, "Warn о паузе")
	evs := sink.snapshot()
	if len(evs) == 0 || evs[0].Source != StoreFullWatch || evs[0].Recovered {
		t.Fatalf("событие паузы: %+v, want Source=%s Recovered=false", evs, StoreFullWatch)
	}

	fake.set(50, 100) // место освободили
	waitFor(t, func() bool { return logBuf.contains("запись контента возобновлена") }, "Info о восстановлении")
	evs = sink.snapshot()
	if len(evs) < 2 || !evs[len(evs)-1].Recovered || evs[len(evs)-1].Source != StoreFullWatch {
		t.Errorf("событие восстановления: %+v, want последнее Recovered=true", evs)
	}
	// Пока состояние не меняется, повторных событий нет (фронтовой сигнал).
	n := len(sink.snapshot())
	time.Sleep(30 * time.Millisecond)
	if got := len(sink.snapshot()); got != n {
		t.Errorf("события без смены состояния: %d → %d, want без изменений", n, got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("DiskWatchLoop после отмены: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DiskWatchLoop не завершился после отмены ctx")
	}
}

// TestDiskWatchLoopStatError — ошибка statfs: Warn без смены состояния,
// цикл жив, следующий тик повторяет.
func TestDiskWatchLoopStatError(t *testing.T) {
	var (
		logBuf lockedBuffer
		sink   *fullSink
	)
	st, _, fake := newDiskTestStore(t, func(o *PackOptions) {
		sink = &fullSink{}
		o.OnStoreFull = sink.hook
	})
	log := slog.New(slog.NewTextHandler(&logBuf, nil))
	fake.setErr(errors.New("statfs недоступен"))

	lctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- DiskWatchLoop(lctx, st, DiskWatchOptions{Interval: 10 * time.Millisecond, Log: log})
	}()
	waitFor(t, func() bool { return logBuf.contains("контроль свободного места") }, "Warn об ошибке statfs")
	if evs := sink.snapshot(); len(evs) != 0 {
		t.Errorf("события при ошибке statfs: %+v, want нет (состояние не меняется)", evs)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("DiskWatchLoop после отмены: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DiskWatchLoop не завершился после отмены ctx")
	}
}

// TestDiskWatchNilStore — программная ошибка, а не паника.
func TestDiskWatchNilStore(t *testing.T) {
	if err := DiskWatchLoop(context.Background(), nil, DiskWatchOptions{}); err == nil {
		t.Error("DiskWatchLoop(nil): ожидалась ошибка")
	}
}

// TestHumanBytes — форматирование размеров в сообщениях.
func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    uint64
		want string
	}{
		{0, "0B"},
		{512, "512B"},
		{2048, "2.0KiB"},
		{5 << 20, "5.0MiB"},
		{3 << 30, "3.0GiB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.n); got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

// waitFor — опрос условия с дедлайном (тихие ретраи асинхронных переходов
// цикла; стиль compactWaitFor).
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("не дождались: %s", what)
}

// lockedBuffer — потокобезопасный буфер для slog-хендлера.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) contains(s string) bool {
	return strings.Contains(b.String(), s)
}

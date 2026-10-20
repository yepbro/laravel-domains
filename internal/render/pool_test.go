package render

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// fakeSession — сессия по контракту renderSession с настраиваемым Run.
type fakeSession struct {
	run    func(ctx context.Context, task RenderTask) (RenderOutput, error)
	closes atomic.Int32
}

func (f *fakeSession) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	if f.run != nil {
		return f.run(ctx, task)
	}
	return RenderOutput{FinalURL: task.URL, Title: "ok"}, nil
}

func (f *fakeSession) Close() { f.closes.Add(1) }

// recorder — фабрика сессий для пула: помнит созданные сессии и может
// уронить первые создания с заданной ошибкой (по номеру вызова).
type recorder struct {
	mu       sync.Mutex
	sessions []*fakeSession
	fail     map[int]error // номер создания (с 0) → ошибка фабрики
	calls    int
}

func (r *recorder) factory(_ context.Context) (renderSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.calls
	r.calls++
	if err, ok := r.fail[i]; ok {
		return nil, err
	}
	s := &fakeSession{}
	r.sessions = append(r.sessions, s)
	return s, nil
}

// n — сессия i (0-based); все создания происходят синхронно внутри Run,
// поэтому после его возвала достаточно прочитать.
func (r *recorder) session(t *testing.T, i int) *fakeSession {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.sessions) {
		t.Fatalf("сессия %d не создана (создано %d)", i, len(r.sessions))
	}
	return r.sessions[i]
}

func ctxErr(class Class) error {
	return &Error{Class: class, Err: errors.New("сбой")}
}

func TestPoolRunSuccess(t *testing.T) {
	b := testBrowser(2)
	p := NewPool(b)
	rec := &recorder{}
	p.newSession = rec.factory

	out, err := p.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.FinalURL != "https://a.example/" || out.Title != "ok" {
		t.Fatalf("результат %+v", out)
	}
	s := rec.session(t, 0)
	if s.closes.Load() != 1 {
		t.Errorf("сессия закрыта %d раз, хочу 1 (профиль выкинут)", s.closes.Load())
	}
	if b.Pages() != 1 {
		t.Errorf("счётчик страниц %d, хочу 1", b.Pages())
	}
}

func TestPoolRetryOnDeadContext(t *testing.T) {
	b := testBrowser(1)
	p := NewPool(b)
	rec := &recorder{}
	p.newSession = rec.factory

	var first atomic.Bool
	p.newSession = func(ctx context.Context) (renderSession, error) {
		s, err := rec.factory(ctx)
		if err != nil {
			return nil, err
		}
		if first.CompareAndSwap(false, true) {
			fs := s.(*fakeSession)
			fs.run = func(context.Context, RenderTask) (RenderOutput, error) {
				return RenderOutput{}, ctxErr(ClassContext)
			}
		}
		return s, nil
	}

	out, err := p.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if err != nil {
		t.Fatalf("повтор не вылечил: %v", err)
	}
	if out.Title != "ok" {
		t.Fatalf("результат %+v", out)
	}
	for i := range 2 {
		if s := rec.session(t, i); s.closes.Load() != 1 {
			t.Errorf("сессия %d закрыта %d раз", i, s.closes.Load())
		}
	}
	// Одна задача — одна страница в счётчике перезапуска.
	if b.Pages() != 1 {
		t.Errorf("счётчик страниц %d, хочу 1", b.Pages())
	}
}

func TestPoolContextErrorAfterRetry(t *testing.T) {
	b := testBrowser(1)
	p := NewPool(b)
	rec := &recorder{}
	p.newSession = func(ctx context.Context) (renderSession, error) {
		s, err := rec.factory(ctx)
		if err != nil {
			return nil, err
		}
		fs := s.(*fakeSession)
		fs.run = func(context.Context, RenderTask) (RenderOutput, error) {
			return RenderOutput{}, ctxErr(ClassContext)
		}
		return s, nil
	}

	_, err := p.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if ClassOf(err) != ClassContext {
		t.Fatalf("класс %v (%v), хочу context", ClassOf(err), err)
	}
	if got := b.Pages(); got != 0 {
		t.Errorf("счётчик страниц %d, хочу 0", got)
	}
	if n := len(rec.sessions); n != 2 {
		t.Errorf("создано сессий %d, хочу 2 (ровно один повтор)", n)
	}
}

func TestPoolNoRetryOnTaskOrBrowserError(t *testing.T) {
	for _, class := range []Class{ClassTask, ClassBrowser} {
		t.Run(class.String(), func(t *testing.T) {
			b := testBrowser(1)
			p := NewPool(b)
			rec := &recorder{}
			p.newSession = func(ctx context.Context) (renderSession, error) {
				s, err := rec.factory(ctx)
				if err != nil {
					return nil, err
				}
				fs := s.(*fakeSession)
				fs.run = func(context.Context, RenderTask) (RenderOutput, error) {
					return RenderOutput{}, ctxErr(class)
				}
				return s, nil
			}

			_, err := p.Run(context.Background(), RenderTask{URL: "https://a.example/"})
			if ClassOf(err) != class {
				t.Fatalf("класс %v, хочу %v", ClassOf(err), class)
			}
			if n := len(rec.sessions); n != 1 {
				t.Errorf("создано сессий %d, хочу 1 (без повторов)", n)
			}
		})
	}
}

func TestPoolSessionCreateError(t *testing.T) {
	t.Run("контекст не поднялся — повтор на новом", func(t *testing.T) {
		b := testBrowser(1)
		p := NewPool(b)
		rec := &recorder{fail: map[int]error{0: ctxErr(ClassContext)}}
		p.newSession = rec.factory

		if _, err := p.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
			t.Fatalf("повтор не вылечил: %v", err)
		}
		if b.Pages() != 1 {
			t.Errorf("счётчик страниц %d, хочу 1", b.Pages())
		}
	})
	t.Run("браузер недоступен — без повторов", func(t *testing.T) {
		b := testBrowser(1)
		p := NewPool(b)
		rec := &recorder{fail: map[int]error{0: ctxErr(ClassBrowser)}}
		p.newSession = rec.factory

		_, err := p.Run(context.Background(), RenderTask{URL: "u"})
		if ClassOf(err) != ClassBrowser {
			t.Fatalf("класс %v, хочу browser", ClassOf(err))
		}
		if rec.calls != 1 {
			t.Errorf("попыток создания %d, хочу 1", rec.calls)
		}
	})
}

func TestPoolConcurrencyLimit(t *testing.T) {
	b := testBrowser(2)
	p := NewPool(b)
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	p.newSession = func(ctx context.Context) (renderSession, error) {
		s, err := (&recorder{}).factory(ctx)
		if err != nil {
			return nil, err
		}
		fs := s.(*fakeSession)
		fs.run = func(context.Context, RenderTask) (RenderOutput, error) {
			entered <- struct{}{}
			<-release
			return RenderOutput{}, nil
		}
		return fs, nil
	}

	const tasks = 3
	var wg sync.WaitGroup
	for range tasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.Run(context.Background(), RenderTask{URL: "u"})
		}()
	}

	// Две задачи занимают оба слота, третья ждёт.
	for range 2 {
		<-entered
	}
	select {
	case <-entered:
		t.Fatal("третья задача вошла при занятых слотах")
	default:
	}
	select {
	case p.slots <- struct{}{}:
		t.Fatal("слот свободен при двух задачах в полёте")
	default:
	}

	close(release)
	wg.Wait()
	// Все слоты вернулись.
	select {
	case p.slots <- struct{}{}:
		<-p.slots
	default:
		t.Fatal("слоты не освободились после задач")
	}
}

func TestPoolWaitCanceled(t *testing.T) {
	b := testBrowser(1)
	p := NewPool(b)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	p.newSession = func(ctx context.Context) (renderSession, error) {
		s, err := (&recorder{}).factory(ctx)
		if err != nil {
			return nil, err
		}
		fs := s.(*fakeSession)
		fs.run = func(context.Context, RenderTask) (RenderOutput, error) {
			entered <- struct{}{}
			<-release
			return RenderOutput{}, nil
		}
		return fs, nil
	}

	go func() {
		defer close(done)
		_, _ = p.Run(context.Background(), RenderTask{URL: "holder"})
	}()
	<-entered

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.Run(ctx, RenderTask{URL: "waiter"})
	if ClassOf(err) != ClassTask || !errors.Is(err, context.Canceled) {
		t.Fatalf("ошибка ожидания слота: %v", err)
	}

	close(release)
	<-done
}

// TestPoolNoRetryAfterRetire — закрытый пул не повторяет даже контекстную
// ошибку: поколение уже выводится из работы, повтор — дело нового поколения.
func TestPoolNoRetryAfterRetire(t *testing.T) {
	b := testBrowser(1)
	p := NewPool(b)
	rec := &recorder{}
	p.newSession = func(ctx context.Context) (renderSession, error) {
		s, err := rec.factory(ctx)
		if err != nil {
			return nil, err
		}
		fs := s.(*fakeSession)
		fs.run = func(context.Context, RenderTask) (RenderOutput, error) {
			p.retire() // поколение сменилось по ходу первой попытки
			return RenderOutput{}, ctxErr(ClassContext)
		}
		return s, nil
	}
	_, err := p.Run(context.Background(), RenderTask{URL: "u"})
	if ClassOf(err) != ClassContext {
		t.Fatalf("класс %v (%v), хочу context", ClassOf(err), err)
	}
	if got := len(rec.sessions); got != 1 {
		t.Errorf("создано сессий %d, хочу 1 (retired пул не повторяет)", got)
	}
}

// TestPoolCloseStopsBrowser — Close пула останавливает и браузер поколения
// (cancel), а не только слоты: следующее поколение стартует чистым.
func TestPoolCloseStopsBrowser(t *testing.T) {
	b := testBrowser(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.ctx, b.cancel = ctx, cancel
	p := NewPool(b)
	p.Close()
	if b.Alive() {
		t.Error("браузер не остановлен Close пула")
	}
}

func TestPoolClosed(t *testing.T) {
	b := testBrowser(1)
	p := NewPool(b)
	rec := &recorder{}
	p.newSession = rec.factory
	p.Close()

	_, err := p.Run(context.Background(), RenderTask{URL: "u"})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("ошибка %v, хочу ErrClosed", err)
	}
	if ClassOf(err) != ClassTask {
		t.Errorf("класс %v, хочу task", ClassOf(err))
	}
	if rec.calls != 0 {
		t.Errorf("после Close создано сессий: %d", rec.calls)
	}
	// Повторный Close безвреден.
	p.Close()
}

func TestPoolCloseWaitsInflight(t *testing.T) {
	b := testBrowser(1)
	p := NewPool(b)
	started := make(chan struct{})
	release := make(chan struct{})
	p.newSession = func(ctx context.Context) (renderSession, error) {
		s, err := (&recorder{}).factory(ctx)
		if err != nil {
			return nil, err
		}
		fs := s.(*fakeSession)
		fs.run = func(context.Context, RenderTask) (RenderOutput, error) {
			close(started)
			<-release
			return RenderOutput{}, nil
		}
		return fs, nil
	}

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_, _ = p.Run(context.Background(), RenderTask{URL: "u"})
	}()
	<-started

	closed := make(chan struct{})
	go func() {
		p.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close не дождался задачи в полёте")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-runDone
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close не завершился после задач")
	}
	if b.Alive() {
		t.Error("браузер не остановлен Close")
	}
}

func TestPoolParallelStress(t *testing.T) {
	const contexts = 3
	b := testBrowser(contexts)
	p := NewPool(b)
	rec := &recorder{}
	var inflight, max atomic.Int32
	p.newSession = func(ctx context.Context) (renderSession, error) {
		s, err := rec.factory(ctx)
		if err != nil {
			return nil, err
		}
		fs := s.(*fakeSession)
		fs.run = func(context.Context, RenderTask) (RenderOutput, error) {
			cur := inflight.Add(1)
			for {
				old := max.Load()
				if cur <= old || max.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inflight.Add(-1)
			return RenderOutput{}, nil
		}
		return fs, nil
	}

	const tasks = 20
	var wg sync.WaitGroup
	for range tasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
				t.Errorf("Run: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := max.Load(); got > contexts {
		t.Errorf("максимум параллельных задач %d > слотов %d", got, contexts)
	}
	if got := b.Pages(); got != tasks {
		t.Errorf("счётчик страниц %d, хочу %d", got, tasks)
	}
	if got := len(rec.sessions); got != tasks {
		t.Errorf("создано сессий %d, хочу %d (свежий профиль на задачу)", got, tasks)
	}
}

func TestBrowserNeedsRestart(t *testing.T) {
	b := testBrowser(1)
	b.opts.RestartEveryPages = 3
	b.pages.Add(2)
	if b.NeedsRestart() {
		t.Error("порог 3 не достигнут (2 страницы)")
	}
	b.pages.Add(1)
	if !b.NeedsRestart() {
		t.Error("порог 3 достигнут")
	}
}

func TestBrowserRSSUnknown(t *testing.T) {
	// rootPID не определён (remote-режим, процесс не найден) — RSS
	// неизвестен, порог памяти не триггерует.
	b := testBrowser(1)
	b.opts.MemoryThreshold = 1
	if got := b.RSS(); got != -1 {
		t.Errorf("RSS без процесса = %d, хочу -1", got)
	}
	if b.OverMemory() {
		t.Error("неизвестный RSS не должен триггерить перезапуск")
	}
	var nilB *Browser
	if nilB.OverMemory() {
		t.Error("nil-браузер не должен триггерить перезапуск")
	}
	if got := nilB.RSS(); got != -1 {
		t.Errorf("RSS(nil) = %d, хочу -1", got)
	}
	// rootPID 1 валиден (граница «не определён» — ровно 0): дерево pid 1
	// в контейнере непусто.
	if _, err := os.Stat("/proc"); err == nil {
		b1 := testBrowser(1)
		b1.rootPID = 1
		if got := b1.RSS(); got <= 0 {
			t.Errorf("RSS(pid 1) = %d, хочу > 0", got)
		}
	}
}

func TestBrowserOverMemory(t *testing.T) {
	// RSS живого процесса с замороженной памятью: спящий потомок
	// останавливается (SIGSTOP) — память между чтениями не «дышит»
	// (RSS самого теста меняется — точные сравнения флакают), граница
	// порога проверяется на равенстве. Страницы потомка faulted-in
	// асинхронно (sleep контейнера — busybox): SIGSTOP сразу после
	// Start заморозил бы нулевой RSS — сперва прогрев до ненулевого.
	b := testBrowser(1)
	cmd := exec.Command("sleep", "5")
	if err := cmd.Start(); err != nil {
		t.Skipf("sleep не запустился: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	b.rootPID = cmd.Process.Pid

	var rss int64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r := b.RSS(); r > 0 {
			rss = r
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rss <= 0 {
		t.Skip("RSS недоступен (нет /proc)")
	}
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Skipf("SIGSTOP не удался: %v", err)
	}
	// Заморозили — дождаться стабильного значения (два подряд
	// одинаковых чтения) на случай последних проживлённых страниц.
	stable := time.Now().Add(2 * time.Second)
	for time.Now().Before(stable) {
		r := b.RSS()
		if r == rss {
			break
		}
		if r > 0 {
			rss = r
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.opts.MemoryThreshold = rss // ровно порог — превышен (>=)
	if !b.OverMemory() {
		t.Error("RSS равен порогу: перезапуск должен срабатывать")
	}
	b.opts.MemoryThreshold = 1 // байт: любая живая память — превышение
	if !b.OverMemory() {
		t.Error("минимальный порог 1 байт: положительный RSS — превышение")
	}
	b.opts.MemoryThreshold = rss + 1<<30 // заведомо выше живого RSS
	if b.OverMemory() {
		t.Error("RSS ниже порога — без перезапуска")
	}
	b.opts.MemoryThreshold = 0 // контроль отключён
	if b.OverMemory() {
		t.Error("нулевой порог отключает контроль")
	}

	// RSS == 0 (корень исчез из снимка) — не превышение.
	if pid := missingPID(); pid != 0 {
		b.rootPID = pid
		b.opts.MemoryThreshold = 1
		if got := b.RSS(); got != 0 {
			t.Fatalf("RSS исчезнувшего корня = %d, хочу 0", got)
		}
		if b.OverMemory() {
			t.Error("нулевой RSS не должен триггерить перезапуск")
		}
	}
}

// missingPID — числовой pid, отсутствующий в /proc (0, если не нашёлся):
// RSS такого корня — 0 (снимок есть, корня в нём нет).
func missingPID() int {
	for p := 1 << 18; p < 1<<18+4096; p++ {
		if _, err := os.Stat("/proc/" + strconv.Itoa(p)); err != nil {
			return p
		}
	}
	return 0
}

package render

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// liveBrowser — «живой» браузер без Chromium: контекст без chromedp-данных
// Alive считает живым (проверяются только отмена контекста и потеря
// соединения, которого нет). cancel позволяет убить контекст посреди теста.
func liveBrowser(contexts int) (*Browser, context.CancelFunc) {
	b := testBrowser(contexts)
	ctx, cancel := context.WithCancel(context.Background())
	b.ctx = ctx
	return b, cancel
}

// deadBrowser — браузер с отменённым контекстом (Alive = false).
func deadBrowser(contexts int) *Browser {
	b, cancel := liveBrowser(contexts)
	cancel()
	return b
}

// poolOver — пул над b с фейковыми сессиями (стиль pool_test.go); run nil —
// успешная сессия.
func poolOver(b *Browser, run func(context.Context, RenderTask) (RenderOutput, error)) *Pool {
	p := NewPool(b)
	p.newSession = func(context.Context) (renderSession, error) {
		fs := &fakeSession{}
		if run != nil {
			fs.run = run
		}
		return fs, nil
	}
	return p
}

// managerFor — менеджер с фейковыми фабриками: браузеры из слайса browsers
// (по числу вызовов), пул — poolOver; считает старты и помнит пулы.
func managerFor(t *testing.T, opts Options, browsers ...*Browser) (*Manager, *atomic.Int32, *[]*Pool) {
	t.Helper()
	m := NewManager(context.Background(), opts)
	var calls atomic.Int32
	var pools []*Pool
	var mu sync.Mutex
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		i := int(calls.Add(1)) - 1
		if i >= len(browsers) {
			return nil, &Error{Class: ClassBrowser, Err: errors.New("нет очередного браузера")}
		}
		return browsers[i], nil
	}
	m.newPool = func(b *Browser) *Pool {
		p := poolOver(b, nil)
		mu.Lock()
		pools = append(pools, p)
		mu.Unlock()
		return p
	}
	return m, &calls, &pools
}

func managerOpts() Options {
	return Options{
		Contexts:          1,
		Timeout:           5 * time.Second,
		RestartEveryPages: 100,
	}.withDefaults()
}

func TestManagerRunStartsBrowserOnce(t *testing.T) {
	b, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b)

	for range 2 {
		out, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.FinalURL != "https://a.example/" {
			t.Fatalf("результат %+v", out)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("стартов браузера %d, хочу 1 (один браузер переиспользуется)", got)
	}
	if got := m.Restarts(); got != 0 {
		t.Errorf("перезапусков %d, хочу 0", got)
	}
	m.Close()
	if m.Browser() != nil {
		t.Error("после Close браузера быть не должно")
	}
}

func TestManagerStart(t *testing.T) {
	// Start поднимает первое поколение немедленно (боевая сборка падает на
	// старте, а не перебирает попытки доставок); живое поколение — no-op.
	b, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b)
	if m.Browser() != nil {
		t.Fatal("до Start поколения нет")
	}
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("стартов %d, хочу 1", got)
	}
	if m.Browser() != b {
		t.Fatal("Start не поднял поколение")
	}
	if err := m.Start(); err != nil {
		t.Fatalf("повторный Start: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("стартов %d, хочу 1 (живое поколение не трогается)", got)
	}
	m.Close()
	if err := m.Start(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start после Close: %v, хочу ErrClosed", err)
	}
}

func TestManagerAlive(t *testing.T) {
	// Зонд потребления: живое поколение — true без лишних стартов; после
	// смерти поколения (рестарт Chromium хостом) зонд заменяет его сам —
	// реконнект, пауза потребления не зависает навсегда.
	b1, cancel1 := liveBrowser(1)
	b2, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b1, b2)
	if !m.Alive() {
		t.Fatal("до старта зонд сам поднимает первое поколение (true)")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("стартов %d, хочу 1 (зонд стартовал первое поколение)", got)
	}
	for range 2 {
		if !m.Alive() {
			t.Fatal("живое поколение: Alive = true")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("стартов %d, хочу 1 (живое поколение не перезапускается)", got)
	}

	cancel1() // хост перезапустил Chromium: соединение потеряно навсегда
	if !m.Alive() {
		t.Fatal("зонд обязан заменить мёртвое поколение (true)")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("стартов %d, хочу 2 (мертвое поколение заменено зондаом)", got)
	}
	if !m.Browser().Alive() {
		t.Error("новое поколение живо")
	}

	// Пул первого поколения закрыт фоновым Close: задачи в него не идут.
	if !m.Alive() {
		t.Error("после замены зонд видит живое поколение")
	}
	m.Close()
	if m.Alive() {
		t.Error("после Close зонд false")
	}
}

func TestManagerRestartOnPagesThreshold(t *testing.T) {
	opts := managerOpts()
	opts.RestartEveryPages = 1 // каждая успешная страница — порог
	b1, _ := liveBrowser(1)
	b1.opts.RestartEveryPages = 1
	b2, _ := liveBrowser(1)
	b2.opts.RestartEveryPages = 1
	m, calls, pools := managerFor(t, opts, b1, b2, nil)

	if _, err := m.Run(context.Background(), RenderTask{URL: "u1"}); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u2"}); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("стартов %d, хочу 2 (смена поколения по порогу страниц)", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("перезапусков %d, хочу 1", got)
	}
	// Счётчик страниц нового поколения с нуля: порог снова не достигнут.
	if got := m.Browser().Pages(); got != 1 {
		t.Errorf("страниц нового поколения %d, хочу 1", got)
	}
	// Старый пул закрыт: новые задачи в него не идут.
	_, err := (*pools)[0].Run(context.Background(), RenderTask{URL: "u"})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("ошибка старого пула %v, хочу ErrClosed", err)
	}
}

func TestManagerRestartOnMemoryThreshold(t *testing.T) {
	b1, _ := liveBrowser(1)
	b1.rootPID = os.Getpid()
	if b1.RSS() <= 0 {
		t.Skip("RSS недоступен (нет /proc)")
	}
	b1.opts.MemoryThreshold = 1 // байт: любая живая память — превышение
	b2, _ := liveBrowser(1)     // порог 0: контроль отключён, не перезапустится
	b3, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b1, b2, b3)

	if _, err := m.Run(context.Background(), RenderTask{URL: "u1"}); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u2"}); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("стартов %d, хочу 2 (смена поколения по порогу памяти)", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("перезапусков %d, хочу 1", got)
	}
}

func TestManagerRestartOnDeadBrowserBeforeTask(t *testing.T) {
	// Браузер поколения умер до задачи (потеря соединения/процесса):
	// acquire заменяет поколение до того, как задача встретится с мёртвым
	// браузером. Поколение подложено белым ящиком — как если бы браузер
	// умер сразу после прошлого перезапуска.
	m := NewManager(context.Background(), managerOpts())
	dead := deadBrowser(1)
	m.b, m.p = dead, poolOver(dead, nil)
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		calls.Add(1)
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("стартов %d, хочу 1 (мертвое поколение заменено)", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("перезапусков %d, хочу 1", got)
	}
}

func TestManagerRetryAfterBrowserDeathInTask(t *testing.T) {
	opts := managerOpts()
	b1, cancel1 := liveBrowser(1)
	b2, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, opts, b1, b2)

	// Пул первого поколения: сессия убивает браузер и возвращает ClassBrowser.
	inner, first := m.newPool, true
	m.newPool = func(b *Browser) *Pool {
		p := inner(b)
		if first {
			first = false
			p.newSession = func(context.Context) (renderSession, error) {
				return &fakeSession{run: func(context.Context, RenderTask) (RenderOutput, error) {
					cancel1()
					return RenderOutput{}, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
				}}, nil
			}
		}
		return p
	}

	out, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if err != nil {
		t.Fatalf("повтор после смерти браузера не вылечил: %v", err)
	}
	if out.Title != "ok" {
		t.Fatalf("результат %+v", out)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("стартов %d, хочу 2", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("перезапусков %d, хочу 1", got)
	}
}

func TestManagerBoundedRestarts(t *testing.T) {
	// Сессии падают классом browser при живом браузере: попытки ограничены,
	// последняя ошибка уходит наверх (повторная доставка — уровень очереди).
	m := NewManager(context.Background(), managerOpts())
	var sessions atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b := testBrowser(1)
		b.ctx = context.Background()
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool {
		p := NewPool(b)
		p.newSession = func(context.Context) (renderSession, error) {
			sessions.Add(1)
			return &fakeSession{run: func(context.Context, RenderTask) (RenderOutput, error) {
				return RenderOutput{}, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
			}}, nil
		}
		return p
	}

	_, err := m.Run(context.Background(), RenderTask{URL: "u"})
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("класс %v (%v), хочу browser", ClassOf(err), err)
	}
	// Пин литерала (мутанты ±1 константы): попытка + ровно 2 перезапуска.
	// Сравнение попыток с 1+maxRestartsPerTask маскировало бы мутацию —
	// ожидание менялось бы вместе с константой.
	if maxRestartsPerTask != 2 {
		t.Fatalf("maxRestartsPerTask = %d, хочу 2", maxRestartsPerTask)
	}
	if got := sessions.Load(); got != 1+maxRestartsPerTask {
		t.Errorf("попыток %d, хочу %d (попытка + перезапуски)", got, 1+maxRestartsPerTask)
	}
}

func TestManagerStartFailure(t *testing.T) {
	m := NewManager(context.Background(), managerOpts())
	var fail atomic.Bool
	fail.Store(true)
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		if fail.Load() {
			return nil, &Error{Class: ClassBrowser, Err: errors.New("нет бинарка")}
		}
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	_, err := m.Run(context.Background(), RenderTask{URL: "u"})
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("ошибка старта: класс %v (%v), хочу browser", ClassOf(err), err)
	}
	// Старт не откладывается: следующая задача пробует снова.
	fail.Store(false)
	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run после починки: %v", err)
	}
}

func TestManagerClosed(t *testing.T) {
	b, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b)
	m.Close()

	_, err := m.Run(context.Background(), RenderTask{URL: "u"})
	if !errors.Is(err, ErrClosed) || ClassOf(err) != ClassTask {
		t.Fatalf("ошибка %v (%v), хочу ErrClosed класса task", err, ClassOf(err))
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("стартов после Close: %d", got)
	}
	m.Close() // повторный вызов безвреден
	// И не завешивает менеджер: мьютекс освобождён, задачи по-прежнему
	// получают ErrClosed сразу. Вырезание Unlock оставляло бы мьютекс
	// захваченным (Run вис) — проверяем с таймаутом, а не до упора.
	errCh := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u2"})
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("ошибка после повторного Close: %v, хочу ErrClosed", err)
		}
		if ClassOf(err) != ClassTask {
			t.Errorf("класс %v, хочу task", ClassOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run завис после повторного Close: мьютекс не освобождён")
	}
}

func TestManagerRunRetryOnClosedPool(t *testing.T) {
	// Гонка «поколение сменилось сразу после acquire»: задача получает
	// ErrClosed из уже закрываемого пула — менеджер меняет поколение и
	// повторяет задачу, а не копит ошибки до исчерпания попыток. Пул
	// закрывается фабрикой сразу при создании — детерминированная имитация
	// закрытия между acquire и p.Run.
	m := NewManager(context.Background(), managerOpts())
	var poolCalls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool {
		p := poolOver(b, nil)
		if poolCalls.Add(1) == 1 {
			p.Close()
		}
		return p
	}

	out, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if err != nil {
		t.Fatalf("Run после закрытия поколения: %v", err)
	}
	if out.Title != "ok" {
		t.Fatalf("результат %+v", out)
	}
	if got := poolCalls.Load(); got != 2 {
		t.Errorf("поколений %d, хочу 2 (закрытое заменено)", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("перезапусков %d, хочу 1", got)
	}
}

func TestManagerCloseWaitsInflight(t *testing.T) {
	opts := managerOpts()
	m := NewManager(context.Background(), opts)
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b, _ := liveBrowser(1)
		return b, nil
	}
	started := make(chan struct{})
	release := make(chan struct{})
	m.newPool = func(b *Browser) *Pool {
		return poolOver(b, func(context.Context, RenderTask) (RenderOutput, error) {
			close(started)
			<-release
			return RenderOutput{}, nil
		})
	}

	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u"})
		runDone <- err
	}()
	<-started

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close не дождался задачи в полёте")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("задача в полёте: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close не завершился после задач")
	}
}

func TestManagerCloseRejectsTasksWhileDraining(t *testing.T) {
	// R-07: пока Close дорабатывает задачи поколения (Pool.Close ждёт wg),
	// новые задачи получают ErrClosed сразу, а не ждут дрена — мьютекс
	// не держится на всё ожидание.
	m := NewManager(context.Background(), managerOpts())
	started := make(chan struct{})
	release := make(chan struct{})
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool {
		return poolOver(b, func(context.Context, RenderTask) (RenderOutput, error) {
			close(started)
			<-release
			return RenderOutput{}, nil
		})
	}

	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u"})
		runDone <- err
	}()
	<-started

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	// Close входит в дрен (критическая секция короткая), задача держит его.
	select {
	case <-closed:
		t.Fatal("Close не должен завершиться до конца задач поколения")
	case <-time.After(100 * time.Millisecond):
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u2"})
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("ошибка %v, хочу ErrClosed сразу, без ожидания дрена", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("новая задача блокируется на дрена Close (мьютекс удерживается?)")
	}

	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("задача в полёте: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close не завершился после задач")
	}
}

func TestManagerParallelStress(t *testing.T) {
	// Каждая успешная страница достигает порог: задачи постоянно меняют
	// поколение, ждут слоты, попадают в закрываемые пулы — все обязаны
	// завершиться результатом, без взаимоблокировок.
	opts := managerOpts()
	opts.RestartEveryPages = 1
	opts.Contexts = 2
	m := NewManager(context.Background(), opts)
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		calls.Add(1)
		b, _ := liveBrowser(2)
		b.opts.RestartEveryPages = 1
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	const tasks = 20
	var wg sync.WaitGroup
	errs := make(chan error, tasks)
	for range tasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Run: %v", err)
	}
	if got := m.Restarts(); got == 0 {
		t.Error("перезапусков не было при пороге 1 страница")
	}
	// Каждый старт после первого — перезапуск: инвариант смены поколения.
	if got, want := calls.Load(), int32(m.Restarts())+1; got != want {
		t.Errorf("стартов %d, хочу перезапусков+1 = %d", got, want)
	}
	m.Close()
}

// wiredBrowser — браузер с подключённой отменой контекста: остановку
// видно через Alive (стиль TestPoolCloseStopsBrowser), в отличие от
// liveBrowser, чей Close не отменяет контекст.
func wiredBrowser(contexts int) (*Browser, context.CancelFunc) {
	b := testBrowser(contexts)
	ctx, cancel := context.WithCancel(context.Background())
	b.ctx, b.cancel = ctx, cancel
	return b, cancel
}

func TestManagerDefaultFactories(t *testing.T) {
	// Фабрики NewManager: newBrowser = StartBrowser, newPool = NewPool.
	// Вырезание первого — nil-вызов на первом старте (паника), второго —
	// паника после успешного старта браузера; оба наблюдаемы без подмен.
	t.Run("newBrowser — StartBrowser", func(t *testing.T) {
		// Герметичный отказ старта: закрытый порт, соединение
		// отказывается мгновенно (TestStartBrowserRemoteUnreachable).
		opts := managerOpts()
		opts.RemoteURL = "ws://127.0.0.1:1"
		opts.DialTimeout = 500 * time.Millisecond
		m := NewManager(context.Background(), opts)
		err := m.Start()
		var e *Error
		if !errors.As(err, &e) || e.Class != ClassBrowser {
			t.Fatalf("хочу ошибку класса browser от StartBrowser, получено: %v", err)
		}
		m.Close()
	})
	t.Run("newPool — NewPool", func(t *testing.T) {
		// Браузер фейком, пул — настоящий: вырезание присвоения
		// оставляет nil-фабрику — паника на первом старте. Запуск задач
		// через настоящий пул не нужен: сессии требуют живой chromedp-
		// контекст, создания пула достаточно.
		m := NewManager(context.Background(), managerOpts())
		b, _ := liveBrowser(1)
		m.newBrowser = func(context.Context, Options) (*Browser, error) {
			return b, nil
		}
		if err := m.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if m.Browser() != b {
			t.Fatal("Start не поднял поколение с настоящим NewPool")
		}
		m.Close()
	})
}

func TestManagerRunReturnsBrowserErrorAfterClose(t *testing.T) {
	// Закрытие менеджера посреди задачи: ошибка браузера уходит наверх
	// как есть, а не подменяется ErrClosed повторного acquire (ветка
	// m.closed.Load() в решении о повторе Run).
	m := NewManager(context.Background(), managerOpts())
	started := make(chan struct{})
	release := make(chan struct{})
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool {
		return poolOver(b, func(context.Context, RenderTask) (RenderOutput, error) {
			close(started)
			<-release
			return RenderOutput{}, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
		})
	}

	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u"})
		runDone <- err
	}()
	<-started

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	// Дождаться критической секции Close (closed установлен, поколение
	// откреплено) до освобождения задачи — детерминизм проверяемой ветки.
	deadline := time.Now().Add(5 * time.Second)
	for m.Browser() != nil {
		if time.Now().After(deadline) {
			t.Fatal("Close не вошёл в критическую секцию")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)

	err := <-runDone
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("класс %v (%v), хочу browser: ошибка задачи не подменяется ErrClosed", ClassOf(err), err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close не завершился после задачи")
	}
}

func TestManagerRunNoRetryOnTaskError(t *testing.T) {
	// Ошибка страницы — обычный результат этапа: менеджер не повторяет
	// задачу и не перезапускает браузер (повторная доставка — уровень
	// очереди, T-066).
	b, _ := liveBrowser(1)
	m := NewManager(context.Background(), managerOpts())
	boom := &Error{Class: ClassTask, Err: errors.New("страница не загрузилась")}
	var runs atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) { return b, nil }
	m.newPool = func(b *Browser) *Pool {
		return poolOver(b, func(context.Context, RenderTask) (RenderOutput, error) {
			runs.Add(1)
			return RenderOutput{}, boom
		})
	}

	_, err := m.Run(context.Background(), RenderTask{URL: "u"})
	if !errors.Is(err, boom) {
		t.Fatalf("ошибка %v, хочу исходную ошибку страницы", err)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("выполнений задачи %d, хочу 1 (ошибка страницы — без повторов менеджера)", got)
	}
	if got := m.Restarts(); got != 0 {
		t.Errorf("перезапусков %d, хочу 0", got)
	}
}

func TestManagerAliveAfterCloseNoRestart(t *testing.T) {
	// После Close зонд окончательно мёртв: false без попытки поднять
	// новое поколение (проверка closed идёт раньше решения о смене;
	// фабрика здесь не исчерпывается — успех рестарта виден).
	m := NewManager(context.Background(), managerOpts())
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		calls.Add(1)
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	if !m.Alive() {
		t.Fatal("зонд поднял первое поколение (true)")
	}
	m.Close()
	if m.Alive() {
		t.Error("после Close зонд false")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("стартов %d, хочу 1 (закрытый менеджер не поднимает поколения)", got)
	}
}

func TestManagerAcquireSwapsMissingPool(t *testing.T) {
	// Страховка nil-пула: браузер поколения есть, пула нет — acquire
	// меняет поколение («пул закрыт»), а не падает. Осиротевший браузер
	// при этом не гасится (old == nil — закрывать нечего): состояние
	// недостижимо штатными путями (b и p ставятся вместе под m.mu),
	// guard защищает от паники, а не от утечки.
	m := NewManager(context.Background(), managerOpts())
	b1, _ := liveBrowser(1)
	m.b = b1 // пул не подложен — рассинхрон поколений
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		calls.Add(1)
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("стартов %d, хочу 1 (поколение без пула заменено)", got)
	}
}

func TestManagerMemoryRestartBoundaries(t *testing.T) {
	// Границы контроля памяти: порог ровно 0 — контроль отключён; порог
	// выше живого RSS — не достигнут. Оба без перезапуска при
	// положительном RSS процесса (второй кейс закрепляет и погашенный по
	// гранулярности аннотации trade-off §4 — вырезание правой половины
	// условия в swapReasonLocked: `rss > 0 && true` перезапускало бы
	// браузер после каждой задачи).
	probe, _ := liveBrowser(1)
	probe.rootPID = os.Getpid()
	if probe.RSS() <= 0 {
		t.Skip("RSS недоступен (нет /proc)")
	}
	above := probe.RSS() + 1<<30 // заведомо выше RSS живого дерева
	newCase := func(t *testing.T, threshold int64) {
		t.Helper()
		b1, _ := liveBrowser(1)
		b1.rootPID = os.Getpid()
		b1.opts.MemoryThreshold = threshold
		b2, _ := liveBrowser(1)
		m, calls, _ := managerFor(t, managerOpts(), b1, b2)

		for range 2 {
			if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
				t.Fatalf("Run: %v", err)
			}
		}
		if got := calls.Load(); got != 1 {
			t.Errorf("стартов %d, хочу 1 (порог %d не триггерует перезапуск)", got, threshold)
		}
		if got := m.Restarts(); got != 0 {
			t.Errorf("перезапусков %d, хочу 0", got)
		}
	}
	t.Run("нулевой порог — контроль отключён", func(t *testing.T) { newCase(t, 0) })
	t.Run("порог выше живого RSS", func(t *testing.T) { newCase(t, above) })
}

func TestManagerRestartFailureDetachesGeneration(t *testing.T) {
	// Неудачный перезапуск открепляет старое поколение сразу:
	// Browser() nil, без ссылки на мёртвый браузер; старт не
	// откладывается — следующая задача пробует снова.
	b1, cancel1 := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b1) // второй старт — ошибка

	if _, err := m.Run(context.Background(), RenderTask{URL: "u1"}); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if m.Browser() != b1 {
		t.Fatal("живое поколение не закреплено")
	}
	cancel1() // браузер поколения умер
	_, err := m.Run(context.Background(), RenderTask{URL: "u2"})
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("ошибка перезапуска: класс %v (%v), хочу browser", ClassOf(err), err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("стартов %d, хочу 2", got)
	}
	if m.Browser() != nil {
		t.Error("после неудачного перезапуска поколение откреплено (Browser nil)")
	}
}

func TestManagerRestartClosesOldGeneration(t *testing.T) {
	// Старое поколение уходит на фоновое закрытие при смене — и при
	// удачной, и при неудачном старте нового: браузер поколения гасится
	// (отмена контекста), а не остаётся висеть.
	run := func(t *testing.T, restartFails bool) {
		opts := managerOpts() // порог страниц — в opts браузеров ниже:
		// NeedsRestart читает именно b.opts, опции менеджера фабрика
		// подменённых браузеров не использует.
		b1, _ := wiredBrowser(1)
		b1.opts.RestartEveryPages = 1
		browsers := []*Browser{b1}
		if !restartFails {
			b2, _ := wiredBrowser(1)
			b2.opts.RestartEveryPages = 1
			browsers = append(browsers, b2)
		}
		m, calls, _ := managerFor(t, opts, browsers...)

		if _, err := m.Run(context.Background(), RenderTask{URL: "u1"}); err != nil {
			t.Fatalf("Run 1: %v", err)
		}
		_, err := m.Run(context.Background(), RenderTask{URL: "u2"}) // порог страниц
		if restartFails && ClassOf(err) != ClassBrowser {
			t.Fatalf("Run 2: %v, хочу ошибку класса browser", err)
		}
		if !restartFails && err != nil {
			t.Fatalf("Run 2: %v", err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("стартов %d, хочу 2", got)
		}
		// Фоновое закрытие старого поколения — дождаться отмены.
		deadline := time.Now().Add(2 * time.Second)
		for b1.Alive() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if b1.Alive() {
			t.Error("браузер старого поколения не остановлен при смене поколения")
		}
	}
	t.Run("удачная смена", func(t *testing.T) { run(t, false) })
	t.Run("неудачный старт нового", func(t *testing.T) { run(t, true) })
}

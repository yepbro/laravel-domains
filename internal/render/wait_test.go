package render

// Юнит-тесты стратегии ожидания (wait.go, ТЗ 5.6 / T-063): фазы движутся
// фейковыми событиями (каналы waitEvents) и фейковыми таймерами — реальное
// время не используется, порядок шагов сценария детерминирован (на каждом
// шаге select готов ровно один кейс). Живое поведение против Chromium —
// wait_live_test.go.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
)

// fakeWaitTimer — таймер на фейковых часах: срабатывает только явным fire()
// из теста. Reset дренирует недоставленное значение (семантика Timer
// Go ≥ 1.23), счётчики Reset/Stop видны тесту.
type fakeWaitTimer struct {
	ch      chan time.Time
	resets  atomic.Int32
	stopped atomic.Bool
}

func (f *fakeWaitTimer) C() <-chan time.Time { return f.ch }
func (f *fakeWaitTimer) Reset(d time.Duration) {
	select {
	case <-f.ch:
	default:
	}
	f.resets.Add(1)
}
func (f *fakeWaitTimer) Stop() { f.stopped.Store(true) }
func (f *fakeWaitTimer) fire() {
	select {
	case f.ch <- time.Time{}:
	default:
	}
}

// fakeWaitClock — фабрика таймеров: помнит созданные таймеры по
// длительности. Тесты обязаны пользоваться планами с неповторяющимися
// длительностями фаз (loadCap, quiet, settleCap различимы).
type fakeWaitClock struct {
	mu     sync.Mutex
	timers map[time.Duration]*fakeWaitTimer
	total  int
}

func (c *fakeWaitClock) timer(d time.Duration) waitTimer {
	t := &fakeWaitTimer{ch: make(chan time.Time, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.timers == nil {
		c.timers = make(map[time.Duration]*fakeWaitTimer)
	}
	c.timers[d] = t
	c.total++
	return t
}

// byDur — таймер фазы с заданной длительностью (nil, если ещё не создан).
func (c *fakeWaitClock) byDur(d time.Duration) *fakeWaitTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timers[d]
}

// nTimers — сколько таймеров создано.
func (c *fakeWaitClock) nTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// waitTestPlan — план с различимыми длительностями фаз.
func waitTestPlan() WaitPlan {
	return WaitPlan{Load: 1 * time.Second, Quiet: 2 * time.Second, Settle: 3 * time.Second}
}

// waitResult — исход waitLoop для сценария.
type waitResult struct {
	out WaitOutcome
	err error
}

// startWaitLoop запускает waitLoop в горутине; шаги сценария выполняет сам
// тест, результат собирается через waitDone.
func startWaitLoop(ctx context.Context, plan WaitPlan, evs waitEvents, clock *fakeWaitClock) <-chan waitResult {
	done := make(chan waitResult, 1)
	go func() {
		out, err := waitLoop(ctx, plan, evs, clock.timer)
		done <- waitResult{out: out, err: err}
	}()
	return done
}

func waitDone(t *testing.T, done <-chan waitResult) waitResult {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("waitLoop не завершился")
		return waitResult{}
	}
}

// waitPhase2 дожидается входа в фазу затишья: созданы все три таймера.
func waitPhase2(t *testing.T, clock *fakeWaitClock) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if clock.nTimers() >= 3 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("фаза затишья не началась: таймеров %d, хочу 3", clock.nTimers())
}

// waitResets дожидается, пока окно затишья сбросят n раз.
func waitResets(t *testing.T, clock *fakeWaitClock, quiet time.Duration, n int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if q := clock.byDur(quiet); q != nil && q.resets.Load() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("окно затишья не сброшено %d раз", n)
}

// waitTimerByDur дожидается создания таймера фазы с заданной длительностью.
func waitTimerByDur(t *testing.T, clock *fakeWaitClock, d time.Duration) *fakeWaitTimer {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if q := clock.byDur(d); q != nil {
			return q
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("таймер фазы %s не создан", d)
	return nil
}

func TestDefaultWaitPlan(t *testing.T) {
	got := DefaultWaitPlan(25 * time.Second)
	want := WaitPlan{Load: 12500 * time.Millisecond, Quiet: 2 * time.Second, Settle: 5 * time.Second}
	if got != want {
		t.Fatalf("DefaultWaitPlan(25s) = %+v, хочу %+v", got, want)
	}
	// Потолки — доли бюджета; окно затишья — константа темпа запросов, но
	// не больше половины потолка фазы (иначе не подтверждается никогда).
	got = DefaultWaitPlan(10 * time.Second)
	want = WaitPlan{Load: 5 * time.Second, Quiet: 1 * time.Second, Settle: 2 * time.Second}
	if got != want {
		t.Fatalf("DefaultWaitPlan(10s) = %+v, хочу %+v", got, want)
	}
	// Стандартный бюджет выше порога — окно не сжимается.
	if q := DefaultWaitPlan(15 * time.Second).Quiet; q != 1500*time.Millisecond {
		t.Errorf("DefaultWaitPlan(15s).Quiet = %s, хочу 1.5s (половина потолка)", q)
	}
	// Вырожденный крошечный бюджет: фазы положительны, окно не длиннее
	// половины потолка — подтверждение затишья остаётся возможным.
	got = DefaultWaitPlan(time.Second)
	if got.Load <= 0 || got.Quiet <= 0 || got.Settle <= 0 {
		t.Fatalf("DefaultWaitPlan(1s) с неположительными фазами: %+v", got)
	}
	if got.Quiet > got.Settle/2 {
		t.Fatalf("DefaultWaitPlan(1s): окно %s > половины потолка %s", got.Quiet, got.Settle/2)
	}
}

func TestWaitLoopLoadThenQuiet(t *testing.T) {
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	close(evs.load) // событие загрузки уже пришло

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock) // фаза загрузки пройдена мгновенно

	// Тишина: окно истекает без единого запроса.
	clock.byDur(plan.Quiet).fire()

	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v", r.err)
	}
	if !r.out.LoadFired || !r.out.Quiet {
		t.Fatalf("исход %+v, хочу {LoadFired:true Quiet:true}", r.out)
	}
	if q := clock.byDur(plan.Quiet); q.resets.Load() != 0 {
		t.Errorf("окно сброшено %d раз, хочу 0 (запросов не было)", q.resets.Load())
	}
	if c := clock.byDur(plan.Load); !c.stopped.Load() {
		t.Error("потолок фазы загрузки не остановлен после события load")
	}
	if c := clock.byDur(plan.Settle); !c.stopped.Load() {
		t.Error("потолок фазы затишья не остановлен при подтверждении окна")
	}
}

func TestWaitLoopLoadCapProceeds(t *testing.T) {
	// load не пришёл: потолок фазы мягкий — ожидание продолжается затишьем.
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}

	done := startWaitLoop(context.Background(), plan, evs, clock)
	// Потолок фазы загрузки истекает без события load.
	waitTimerByDur(t, clock, plan.Load).fire()
	waitPhase2(t, clock)
	clock.byDur(plan.Quiet).fire()

	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v", r.err)
	}
	if !r.out.Quiet {
		t.Fatalf("исход %+v: окно затишья выдержано, Quiet должен быть true", r.out)
	}
}

func TestWaitLoopRequestsMoveWindow(t *testing.T) {
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	close(evs.load)

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock)

	// Каждый новый запрос запускает окно заново.
	evs.reqs <- struct{}{}
	waitResets(t, clock, plan.Quiet, 1)
	evs.reqs <- struct{}{}
	waitResets(t, clock, plan.Quiet, 2)

	// После последнего запроса окно выдержано — готово.
	clock.byDur(plan.Quiet).fire()
	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v", r.err)
	}
	if !r.out.Quiet {
		t.Fatalf("исход %+v: окно выдержано, Quiet должен быть true", r.out)
	}
	if q := clock.byDur(plan.Quiet); q.resets.Load() != 2 {
		t.Errorf("сбросов окна %d, хочу 2", q.resets.Load())
	}
}

func TestWaitLoopSettleCap(t *testing.T) {
	// Непрерывный трафик: окно не сходится, фазу обрывает потолок — итог
	// снимается как есть (не ошибка).
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	close(evs.load)

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock)
	evs.reqs <- struct{}{}
	waitResets(t, clock, plan.Quiet, 1)

	clock.byDur(plan.Settle).fire()
	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v (потолок затишья — не ошибка)", r.err)
	}
	if r.out.Quiet {
		t.Fatalf("исход %+v: сеть не затихла, Quiet должен быть false", r.out)
	}
	if q := clock.byDur(plan.Quiet); !q.stopped.Load() {
		t.Error("окно затишья не остановлено при обрыве по потолку")
	}
}

func TestWaitLoopSettleCapBelowWindow(t *testing.T) {
	// Вырожденный план (потолок фазы короче окна): обрыв по потолку, окно
	// не успевает сработать никогда — поведение то же, «ограниченное
	// ожидание» остаётся ограниченным.
	plan := WaitPlan{Load: 1 * time.Second, Quiet: 10 * time.Second, Settle: 100 * time.Millisecond}
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	close(evs.load)

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock)
	clock.byDur(plan.Settle).fire()

	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v", r.err)
	}
	if r.out.Quiet {
		t.Fatalf("исход %+v: потолок короче окна, Quiet должен быть false", r.out)
	}
}

func TestWaitLoopStaleRequestsDrained(t *testing.T) {
	// Запросы, ушедшие до входа в фазу затишья, к новому окну не
	// относятся: окно отсчитывается от входа в фазу, старые сигналы сливы.
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	evs.reqs <- struct{}{}
	evs.reqs <- struct{}{}
	close(evs.load)

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock)
	// Дать циклу выбрать готовые кейсы: кроме слива старых сигналов,
	// готовых нет; если бы сигналы двигали окно — счётчик бы вырос.
	time.Sleep(50 * time.Millisecond)
	if q := clock.byDur(plan.Quiet); q != nil && q.resets.Load() != 0 {
		t.Fatalf("старые сигналы двигают окно: сбросов %d, хочу 0", q.resets.Load())
	}

	clock.byDur(plan.Quiet).fire()
	r := waitDone(t, done)
	if r.err != nil || !r.out.Quiet {
		t.Fatalf("исход %+v, err %v: хочу {Quiet:true} без ошибки", r.out, r.err)
	}
}

func TestWaitLoopContextCanceled(t *testing.T) {
	t.Run("фаза загрузки", func(t *testing.T) {
		plan := waitTestPlan()
		evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
		clock := &fakeWaitClock{}
		ctx, cancel := context.WithCancel(context.Background())

		done := startWaitLoop(ctx, plan, evs, clock)
		cancel()

		r := waitDone(t, done)
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("ошибка %v, хочу context.Canceled", r.err)
		}
		if r.out.LoadFired || r.out.Quiet {
			t.Fatalf("исход %+v при отмене, хочу нулевой", r.out)
		}
	})

	t.Run("фаза затишья", func(t *testing.T) {
		plan := waitTestPlan()
		evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
		clock := &fakeWaitClock{}
		ctx, cancel := context.WithCancel(context.Background())
		close(evs.load)

		done := startWaitLoop(ctx, plan, evs, clock)
		waitPhase2(t, clock)
		cancel()

		r := waitDone(t, done)
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("ошибка %v, хочу context.Canceled", r.err)
		}
	})
}

func TestWaitLoopOverallDeadline(t *testing.T) {
	// Общий таймаут (жёсткий предел визита) режет ожидание изнутри —
	// реальный дедлайн контекста, фейковые таймеры не срабатывают.
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()

	done := startWaitLoop(ctx, plan, evs, clock)
	r := waitDone(t, done)
	if !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("ошибка %v, хочу context.DeadlineExceeded", r.err)
	}
	if r.out.Quiet {
		t.Fatalf("исход %+v: окно не подтверждено", r.out)
	}
}

// TestWaitSubscribe — подписки и насосы waitSubscribe против in-memory
// транспорта chromedp (cdpfake_test.go; живой браузер не нужен): событие
// загрузки закрывает load однократно, сетевые запросы сигнализируются в
// reqs с буфером ровно waitReqsBuffer, отказ Network.enable возвращается
// ошибкой, отмена контекста не порождает ни закрытия load, ни ложного
// сигнала reqs.
func TestWaitSubscribe(t *testing.T) {
	t.Run("событие загрузки закрывает load однократно", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		evs, err := waitSubscribe(wctx)
		if err != nil {
			t.Fatalf("waitSubscribe: %v", err)
		}
		tr.fireSession("Page.loadEventFired", `{}`)
		select {
		case <-evs.load:
		case <-time.After(2 * time.Second):
			t.Fatal("событие загрузки не закрыло канал load")
		}
		// Повторное событие — без паники повторного close (sync.Once).
		tr.fireSession("Page.loadEventFired", `{}`)
		select {
		case <-evs.load:
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("сетевой запрос сигнализируется в reqs", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		evs, err := waitSubscribe(wctx)
		if err != nil {
			t.Fatalf("waitSubscribe: %v", err)
		}
		tr.fireSession("Network.requestWillBeSent", `{"requestId":"r-1"}`)
		select {
		case <-evs.reqs:
		case <-time.After(2 * time.Second):
			t.Fatal("сетевой запрос не сигнализирован в reqs")
		}
	})

	t.Run("отказ Network.enable — ошибка подписки", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		tr.setError("Network.enable", &cdproto.Error{Code: -32000, Message: "сеть недоступна"})
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		_, err := waitSubscribe(wctx)
		if err == nil || !strings.Contains(err.Error(), "сеть недоступна") {
			t.Fatalf("ошибка %v, хочу отказ Network.enable", err)
		}
	})

	t.Run("отмена контекста тихо останавливает насосы", func(t *testing.T) {
		ctx, _ := newFakeCDPContext(t)
		wctx, cancel := context.WithCancel(ctx)
		evs, err := waitSubscribe(wctx)
		if err != nil {
			t.Fatalf("waitSubscribe: %v", err)
		}
		cancel()
		// Ошибка итератора — терминальная: насосы выходят молча, без
		// закрытия load и без ложного сигнала reqs.
		select {
		case <-evs.load:
			t.Fatal("отмена контекста закрыла канал load")
		case <-time.After(100 * time.Millisecond):
		}
		n := 0
		draining := true
		for draining {
			select {
			case <-evs.reqs:
				n++
			default:
				draining = false
			}
		}
		if n != 0 {
			t.Errorf("отмена контекста дала %d ложных сигналов reqs, хочу 0", n)
		}
	})
}

// TestWaitSubscribeReqsBuffer — буфер канала reqs вмещает ровно 64 сигнала
// (waitReqsBuffer): насос не блокирует поток событий вкладки, лишние
// сигналы слипаются (один и тот же сброс окна затишья). Литералы 64/65
// запиновы сознательно: символ waitReqsBuffer в тесте следовал бы за
// мутантом константы и ничего бы не отличал. Детерминизм: 65 событий с
// интервалом (каждое успевает пройти цепочку доставки до следующего),
// затем положительное ожидание заполнения буфера до 64 и ещё один
// интервал — 65-е событие к этому моменту обработано (у оригинала
// слипнуто, у мутанта 65 — влезло); слив фиксирует итог.
func TestWaitSubscribeReqsBuffer(t *testing.T) {
	ctx, tr := newFakeCDPContext(t)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	evs, err := waitSubscribe(wctx)
	if err != nil {
		t.Fatalf("waitSubscribe: %v", err)
	}
	for i := 0; i < 65; i++ {
		tr.fireSession("Network.requestWillBeSent", `{"requestId":"r"}`)
		time.Sleep(2 * time.Millisecond)
	}
	// Буфер наполнился (64 у оригинала и мутанта alike)…
	deadline := time.Now().Add(2 * time.Second)
	for len(evs.reqs) < 64 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(evs.reqs) < 64 {
		t.Fatalf("буфер reqs не наполнился: %d сигналов из 64", len(evs.reqs))
	}
	// …и последнему событию дан тот же интервал, что и между событиями:
	// у оригинала его посылка слипнута (буфер полон), у мутанта 65 — влезла.
	time.Sleep(50 * time.Millisecond)

	n := 0
	draining := true
	for draining {
		select {
		case <-evs.reqs:
			n++
		default:
			draining = false
		}
	}
	if n != 64 {
		t.Errorf("буфер reqs вместил %d сигналов, хочу ровно 64 (65-й слипнут)", n)
	}
}

// TestWaitRealTimer — обёртка реального таймера: Reset перезапускает отсчёт
// (срабатывание переносится на новый срок), Stop гасит несработавший.
// Реальное время — миллисекунды, без пауз ожидания фаз.
func TestWaitRealTimer(t *testing.T) {
	t.Run("Reset перезапускает отсчёт", func(t *testing.T) {
		timer := waitRealTimerFactory(10 * time.Second)
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C():
		case <-time.After(2 * time.Second):
			t.Fatal("после Reset таймер не сработал по новому сроку")
		}
	})
	t.Run("Stop гасит несработавший", func(t *testing.T) {
		timer := waitRealTimerFactory(50 * time.Millisecond)
		timer.Stop()
		select {
		case <-timer.C():
			t.Fatal("таймер сработал после Stop")
		case <-time.After(300 * time.Millisecond):
		}
	})
}

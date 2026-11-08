package render

// Юнит-тесты автомата поколения браузера (T-263, genstate.go): таблица
// состояний genStateLocked на cdpfake-браузерах (фейк транспорта
// cdpfake_test.go: настоящий bootstrap chromedp, живость/смерть видны
// Browser.Alive), таблица решений swapReasonLocked по всем состояниям и
// порогам, обход переходов жизненного цикла с побочками (рестарт, дрен,
// терминальность Close) и изоляция Г1 «стартующий» мьютексом менеджера.

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// genCDPBrowser — поколение поверх fakeCDP: полный bootstrap chromedp на
// in-memory транспорте, Alive видит живой браузерный контекст. Смерть
// поколения в сценариях — b.Close() (отмена контекста: та же ветка
// Browser.Alive, что LostConnection реального соединения).
//
// Освобождение — t.Cleanup(b.Close), не сырые cancel-функции chromedp:
// обёртка cancelWait (NewContext) для НЕ-attacher аллокатора
// (fakeCDPAllocator) вычитывает токен семафора c.allocated ровно один
// раз и никем не закрывается — второй вызов cancel виснет навсегда;
// Browser.Close идемпотентен (флаг closed) и гасит оба контекста
// (браузерный и аллокатора) первым вызовом.
func genCDPBrowser(t *testing.T, opts Options) *Browser {
	t.Helper()
	tr := newFakeCDP()
	allocCtx, cancelAlloc := chromedp.NewAllocatorContext(context.Background(),
		&fakeCDPAllocator{tr: tr})
	bctx, cancelBrowser := chromedp.NewContext(allocCtx)
	b := &Browser{opts: opts, ctx: bctx, cancel: cancelBrowser, cancelAlloc: cancelAlloc,
		probeTimeout: testProbeTimeout}
	t.Cleanup(b.Close)
	if err := chromedp.Do(bctx); err != nil {
		t.Fatalf("bootstrap браузера поверх fakeCDP: %v", err)
	}
	return b
}

// genStateOf — чтение состояния слота под мьютексом менеджера (как
// читают acquire/Alive/Start).
func genStateOf(m *Manager) genState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.genStateLocked()
}

// genStartedWith — менеджер с поднятым cdpfake-поколением (браузер
// возвращается сценариям для смерти/порогов).
func genStartedWith(t *testing.T) (*Manager, *Browser) {
	t.Helper()
	b := genCDPBrowser(t, managerOpts())
	m, _, _ := managerFor(t, managerOpts(), b)
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return m, b
}

func TestGenStateNames(t *testing.T) {
	// Шесть состояний автомата (T-263) с именами для диагностики; Г1
	// «стартующий» входит в перечень, хотя предикат его не возвращает
	// (существует только под m.mu, инвариант И2).
	names := map[genState]string{
		genAbsent:   "отсутствует",
		genStarting: "стартующий",
		genActive:   "активный",
		genDraining: "draining",
		genDead:     "мёртв",
		genClosed:   "закрыт",
	}
	for g, want := range names {
		if got := g.String(); got != want {
			t.Errorf("String(%d) = %q, хочу %q", int(g), got, want)
		}
	}
	for i := 0; i <= int(genClosed); i++ {
		if _, ok := names[genState(i)]; !ok {
			t.Errorf("состояние %d вне перечня автомата (лишнее/незадокументированное)", i)
		}
	}
}

func TestGenStateTable(t *testing.T) {
	// Таблица состояний (ТЗ-fsm §4): все наблюдаемые извне m.mu
	// состояния слота — на cdpfake-браузерах; Г1 — отдельный тест.
	cases := []struct {
		name  string
		setup func(t *testing.T) *Manager
		want  genState
	}{
		{"Г0 отсутствует — до первого старта", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts())
			return m
		}, genAbsent},
		{"Г0 отсутствует — после неудачного старта (П4)", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts()) // фабрика без браузеров — старт падает
			if err := m.Start(); ClassOf(err) != ClassBrowser {
				t.Fatalf("Start: %v, хочу ошибку класса browser", err)
			}
			return m
		}, genAbsent},
		{"Г2 активный — живое поколение (П2)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			return m
		}, genActive},
		{"Г4 мёртв — браузер поколения погашен (П6)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			b.Close()
			return m
		}, genDead},
		{"Г3 draining — пул поколения retired (П8)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			return m
		}, genDraining},
		{"Г3 draining — слот без пула (страховка рассинхрона)", func(t *testing.T) *Manager {
			b := genCDPBrowser(t, managerOpts())
			m := NewManager(context.Background(), managerOpts())
			m.mu.Lock()
			m.b = b // пул не подложен — недостижимо штатно (b и p ставятся вместе)
			m.mu.Unlock()
			return m
		}, genDraining},
		{"Г4 мёртв приоритетнее Г3 draining (И1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			b.Close()
			return m
		}, genDead},
		{"Г5 закрыт — Close при живом поколении (П9)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.Close()
			return m
		}, genClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.setup(t)
			if got := genStateOf(m); got != tc.want {
				t.Fatalf("состояние поколения %s, хочу %s", got, tc.want)
			}
		})
	}
}

func TestSwapReasonTable(t *testing.T) {
	// Таблица решений swapReasonLocked: причины замены по состояниям
	// слота + пороги живого поколения (genstate.go, «Причины замены»).
	cases := []struct {
		name       string
		setup      func(t *testing.T) *Manager
		wantSubstr string
		wantSwap   bool
	}{
		{"первый старт (Г0)", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts())
			return m
		}, "первый старт", true},
		{"браузер недоступен (Г4)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			b.Close()
			return m
		}, "браузер недоступен", true},
		{"мёртвый браузер в закрытом пуле — причина Г4 (И1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			b.Close()
			return m
		}, "браузер недоступен", true},
		{"пул закрыт (Г3 в слоте, П8)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			return m
		}, "пул закрыт", true},
		{"без замены (Г2, пороги не достигнуты)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			return m
		}, "", false},
		{"порог страниц (Г2, П5)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1) // одна успешная страница — порог
			m.mu.Unlock()
			return m
		}, "порог страниц", true},
		{"порог страниц приоритетнее порога памяти", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.rootPID = os.Getpid()
			m.mu.Unlock()
			if b.RSS() <= 0 {
				t.Skip("RSS недоступен (нет /proc)")
			}
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1)
			b.opts.MemoryThreshold = 1 // оба порога достигнуты — решает первый
			m.mu.Unlock()
			return m
		}, "порог страниц", true},
		{"порог памяти (Г2, П5)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.rootPID = os.Getpid()
			m.mu.Unlock()
			if b.RSS() <= 0 {
				t.Skip("RSS недоступен (нет /proc)")
			}
			m.mu.Lock()
			b.opts.MemoryThreshold = 1 // байт: любая живая память — превышение
			m.mu.Unlock()
			return m
		}, "порог памяти", true},
		{"мёртвый браузер с достигнутым порогом — состояние важнее порога (И1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1)
			m.mu.Unlock()
			b.Close()
			return m
		}, "браузер недоступен", true},
		{"закрыт — решений нет (И3)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.Close()
			return m
		}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.setup(t)
			m.mu.Lock()
			reason, swap := m.swapReasonLocked()
			m.mu.Unlock()
			if swap != tc.wantSwap {
				t.Fatalf("решение о смене %v (%q), хочу %v", swap, reason, tc.wantSwap)
			}
			if tc.wantSubstr != "" && !strings.Contains(reason, tc.wantSubstr) {
				t.Fatalf("причина %q не содержит %q", reason, tc.wantSubstr)
			}
		})
	}
}

func TestGenStateLifecycle(t *testing.T) {
	// Обход переходов: П1→П2 (старт), П6 (смерть) → П1→П3 (замена зонда,
	// П7 дрен старого), П8 (закрытый пул в слоте) → П3, П9 (Close).
	// Побочки: Restarts, retire старого пула (ErrClosed задач), запрет
	// новых поколений из Г5.
	b1 := genCDPBrowser(t, managerOpts())
	b2 := genCDPBrowser(t, managerOpts())
	b3 := genCDPBrowser(t, managerOpts())
	m, calls, pools := managerFor(t, managerOpts(), b1, b2, b3)

	// П1→П2: первое поколение — активный, без перезапусков.
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := genStateOf(m); got != genActive {
		t.Fatalf("после старта состояние %s, хочу активный", got)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("стартов %d, хочу %d", got, want)
	}
	if got := m.Restarts(); got != 0 {
		t.Errorf("перезапусков %d, хочу 0 (первый старт — не перезапуск)", got)
	}

	// П6: смерть браузера поколения — мёртв.
	b1.Close()
	if got := genStateOf(m); got != genDead {
		t.Fatalf("после смерти браузера состояние %s, хочу мёртв", got)
	}

	// П1→П3: зонд заменяет мёртвое поколение; старый пул дренирован (П7).
	if !m.Alive() {
		t.Fatal("зонд обязан заменить мёртвое поколение")
	}
	if got := genStateOf(m); got != genActive {
		t.Fatalf("после замены состояние %s, хочу активный", got)
	}
	if got, want := calls.Load(), int32(2); got != want {
		t.Fatalf("стартов %d, хочу %d (замена мёртвого поколения)", got, want)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("перезапусков %d, хочу 1", got)
	}
	if !(*pools)[0].closed.Load() {
		t.Error("пул мёртвого поколения не выведен из работы (retire)")
	}
	if _, err := (*pools)[0].Run(context.Background(), RenderTask{URL: "u"}); !errors.Is(err, ErrClosed) {
		t.Errorf("задача в дренируемый пул: %v, хочу ErrClosed", err)
	}

	// П8: закрытый пул в слоте при живом браузере — замена «пул закрыт».
	m.mu.Lock()
	m.p.retire()
	m.mu.Unlock()
	if got := genStateOf(m); got != genDraining {
		t.Fatalf("после retire пула состояние %s, хочу draining", got)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run после закрытия пула слота: %v", err)
	}
	if got, want := calls.Load(), int32(3); got != want {
		t.Fatalf("стартов %d, хочу %d (замена поколения с закрытым пулом)", got, want)
	}
	if got := m.Restarts(); got != 2 {
		t.Errorf("перезапусков %d, хочу 2", got)
	}

	// П9: Close — терминальное: задачи ErrClosed, зонд false, стартов нет.
	m.Close()
	if got := genStateOf(m); got != genClosed {
		t.Fatalf("после Close состояние %s, хочу закрыт", got)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); !errors.Is(err, ErrClosed) || ClassOf(err) != ClassTask {
		t.Fatalf("Run после Close: %v (%v), хочу ErrClosed класса task", err, ClassOf(err))
	}
	if m.Alive() {
		t.Error("зонд после Close: true, хочу false")
	}
	if got, want := calls.Load(), int32(3); got != want {
		t.Errorf("стартов после Close %d, хочу %d (из Г5 поколений нет)", got, want)
	}
}

func TestGenStateStartingSerialized(t *testing.T) {
	// И2/Г1 «стартующий»: старт поколения держит m.mu всё время
	// StartBrowser — конкурентный зонд ждёт исхода, не наблюдает пустой
	// слот и не поднимает второй браузер.
	b := genCDPBrowser(t, managerOpts())
	entered := make(chan struct{})
	release := make(chan struct{})
	m := NewManager(context.Background(), managerOpts())
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		if calls.Add(1) > 1 {
			t.Error("двойной старт поколения — Г1 не изолирован мьютексом")
			return b, nil
		}
		close(entered)
		<-release
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u"})
		runDone <- err
	}()
	<-entered // Г1 «стартующий»: m.mu удержан restartLocked

	probeDone := make(chan bool, 1)
	go func() { probeDone <- m.Alive() }()
	select {
	case alive := <-probeDone:
		t.Fatalf("зонд вернулся (%v) во время старта: стартующий не изолирован мьютексом", alive)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !<-probeDone {
		t.Fatal("после старта зонд видит живое поколение")
	}
	if got := genStateOf(m); got != genActive {
		t.Fatalf("после старта состояние %s, хочу активный", got)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Errorf("стартов %d, хочу %d (зонд ждал исхода старта)", got, want)
	}
}

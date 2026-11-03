package render

// Воспроизведение инцидента 02.11 (T-218): после рестарта внешнего
// Chromium (systemctl restart crawler-chromium, таймер T-208) воркеры
// копили вебсокеты к :9222 (284 за ~6 ч), Target.createBrowserContext
// висел до SessionStartTimeout («таймаут создания контекста 15s» ×
// 11 520 WARN «контекст рендера умер» за 6 ч), рендер деградировал ~5,5 ч
// и к 07:25 встал; свежий Chromium был здоров — ломалось состояние
// воркеров, лечилось рестартом воркеров.
//
// Механизм (по коду chromedp v0.21.0): чтение WebSocket
// (remote.Conn.Read) не реагирует ни на контекст, ни на таймауты —
// о смерти соединения библиотека узнаёт только от ошибок сокета.
// Полузакрытое/подвисшее соединение (без FIN/RST: NAT/conntrack, браузер
// подвис при остановке) выглядит живым бесконечно: Browser.Alive() =
// true, Manager не меняет поколение, каждая задача висит в создании
// контекста и умирает таймаутом (класс context → бесполезный повтор на
// новом контексте того же мёртвого сокета). Запись WebSocket
// (remote.Conn.Write) тоже без дедлайна: насос chromedp висит в записи
// до исчерпания TCP-ретрансмиссий, upstream-аллокатор закрывает сокет
// только после насосов — старые соединения не освобождаются.
//
// Здесь: switchableCDP — управляемый транспорт («чёрная дыра»: чтение
// висит, записи принимаются молча); проверяются обе половины фикса —
// детектор мёртвого соединения (probeConn: ClassBrowser → смена
// поколения менеджером, восстановление без рестарта воркера) и
// принудительное закрытие сокета аллокатором (соединения не копятся).

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/chromedp"
)

// switchableCDP — транспорт CDP с переключением жизни: как fakeCDP
// отвечает на bootstrap-команды chromedp (уникальные sessionId/targetId —
// вкладки сессий не конфликтуют со служебной вкладкой браузера), по
// kill() становится «чёрной дырой» — Write принимает команды молча (как
// запись в буфер полузакрытого сокета), Read висит до Close()
// (remote.Conn.Read контекст игнорирует — фidelity важна: зонд и
// принудительное закрытие проверяются именно против такого поведения).
type switchableCDP struct {
	mu     sync.Mutex
	alive  bool
	closed bool
	seq    int
	hang   map[string]bool
	fail   map[string]bool
	inbox  chan *cdproto.Message
	cmds   []fakeCDPCmd
	closeC chan struct{}
}

func newSwitchableCDP() *switchableCDP {
	return &switchableCDP{
		alive:  true,
		hang:   make(map[string]bool),
		fail:   make(map[string]bool),
		inbox:  make(chan *cdproto.Message, 1024),
		closeC: make(chan struct{}),
	}
}

// Write удовлетворяет chromedp.Transport. Ответ синтезируется синхронно;
// в режиме «чёрной дыры» и для методов из hang команда принимается
// молча — ответа не будет никогда (имитация мёртвого браузера/висящей
// команды при живом соединении).
func (f *switchableCDP) Write(ctx context.Context, m *cdproto.Message) error {
	f.mu.Lock()
	f.cmds = append(f.cmds, fakeCDPCmd{
		ID:        m.ID,
		Method:    string(m.Method),
		SessionID: string(m.SessionID),
		Params:    string(m.Params),
	})
	method := string(m.Method)
	dead := !f.alive || f.hang[method]
	cmdErr := f.fail[method]
	f.seq++
	seq := f.seq
	f.mu.Unlock()
	if dead {
		return nil
	}

	reply := &cdproto.Message{ID: m.ID, SessionID: m.SessionID}
	switch {
	case cmdErr:
		// Ответ-ошибка протокола при живом соединении (браузер ответил —
		// смерть транспорта это не подтверждает).
		reply.Error = &cdproto.Error{Code: -32000, Message: "команда сломана"}
	case method == "Target.attachToTarget":
		reply.Result = jsonv2.Value(fmt.Sprintf(`{"sessionId":"sw-session-%d"}`, seq))
	case method == "Target.createTarget":
		reply.Result = jsonv2.Value(fmt.Sprintf(`{"targetId":"sw-target-%d"}`, seq))
	case method == "Target.createBrowserContext":
		reply.Result = jsonv2.Value(fmt.Sprintf(`{"browserContextId":"sw-bc-%d"}`, seq))
	case method == "Runtime.evaluate":
		reply.Result = jsonv2.Value(`{"result":{"type":"object","className":"Object"}}`)
	case method == "Target.getTargets":
		reply.Result = jsonv2.Value(`{"targetInfos":[]}`)
	default:
		reply.Result = jsonv2.Value("{}")
	}
	select {
	case f.inbox <- reply:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Read удовлетворяет chromedp.Transport: ждёт сообщение до закрытия
// транспорта. Контекст НЕ наблюдается — как у remote.Conn.Read; без
// этого нельзя доказать, что принудительный Close действительно нужен.
func (f *switchableCDP) Read(_ context.Context, m *cdproto.Message) error {
	select {
	case <-f.closeC:
		return errors.New("transport closed")
	case msg := <-f.inbox:
		*m = *msg
		return nil
	}
}

// Close удовлетворяет chromedp.Transport: разблокирует зависшее чтение
// (аналог закрытия сокета — «use of closed network connection»).
func (f *switchableCDP) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	close(f.closeC)
	return nil
}

// kill — «смерть браузера» на глазах воркера: соединение полузакрыто,
// ответы (в том числе накопленные) не приходят никогда.
func (f *switchableCDP) kill() {
	f.mu.Lock()
	f.alive = false
	f.mu.Unlock()
	for {
		select {
		case <-f.inbox:
		default:
			return
		}
	}
}

// hangMethod — не отвечать на метод (висящая команда при живом
// соединении: например, createBrowserContext на перегруженном браузере).
func (f *switchableCDP) hangMethod(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hang[method] = true
}

// failMethod — отвечать на метод ошибкой протокола при живом соединении
// (браузер прислал ответ-ошибку — транспорт работает).
func (f *switchableCDP) failMethod(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[method] = true
}

// unhangMethod — снять подвисание метода (снова отвечает).
func (f *switchableCDP) unhangMethod(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.hang, method)
}

func (f *switchableCDP) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// countOf — число команд с данным методом.
func (f *switchableCDP) countOf(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.cmds {
		if c.Method == method {
			n++
		}
	}
	return n
}

// dialRecorder — подмена remoteDial: каждое подключение — новый
// switchableCDP, журнал транспортов = журнал соединений воркера к :9222.
type dialRecorder struct {
	mu         sync.Mutex
	transports []*switchableCDP
}

func (r *dialRecorder) dial(_ context.Context, _ string, _ time.Duration) (chromedp.Transport, error) {
	tr := newSwitchableCDP()
	r.mu.Lock()
	r.transports = append(r.transports, tr)
	r.mu.Unlock()
	return tr, nil
}

func (r *dialRecorder) list() []*switchableCDP {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*switchableCDP(nil), r.transports...)
}

// overrideRemoteDial — подмена шва подключения на время теста.
func overrideRemoteDial(t *testing.T, f func(ctx context.Context, wsURL string, timeout time.Duration) (chromedp.Transport, error)) {
	t.Helper()
	old := remoteDial
	remoteDial = f
	t.Cleanup(func() { remoteDial = old })
}

// waitForFor — как waitFor (block_test.go), с настраиваемым сроком:
// фоновые закрытия соединений идут до ~5,5 с (внутренние таймауты
// chromedp: closeTargetTimeout 5 с + waitTargetGone 0,5 с).
func waitForFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("условие не наступило за %s", d)
}

// bootSession — сессия пула с реальным созданием контекста (NewSession:
// зонд соединения + createBrowserContext + вкладка) и мгновенной
// задачей: воспроизведение инцидента — механика обрыва в создании
// контекста, а не в рендере страницы.
type bootSession struct {
	real renderSession
}

func (b bootSession) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	return RenderOutput{FinalURL: task.URL, Title: "ok"}, nil
}

func (b bootSession) Close() { b.real.Close() }

// remoteOpts — параметры remote-режима для тестов реконнекта: адрес с
// готовым путём /devtools/browser/ (без запроса /json/version —
// транспорт подменяется швом remoteDial), короткие таймауты.
func remoteOpts() Options {
	return Options{
		RemoteURL:           "ws://127.0.0.1:9222/devtools/browser/test",
		Contexts:            1,
		Timeout:             2 * time.Second,
		RestartEveryPages:   1000,
		DialTimeout:         time.Second,
		SessionStartTimeout: 300 * time.Millisecond,
	}
}

// testProbeTimeout — тестовый бюджет зонда соединения (T-226): серия
// зондов в тестах идёт в реальном времени; полный продуктовый probeTimeout
// (5 с × ~10 зондов) вытолкнул бы пакет за бюджет мутационного прогона
// (MUTATE_TIMEOUT=60 с). Короткий бюджет не меняет проверяемую семантику:
// «чёрная дыра»/висящий метод не отвечают и за 100 мс.
const testProbeTimeout = 100 * time.Millisecond

// switchableBrowser — браузер поверх управляемого транспорта: полный
// bootstrap chromedp (StartBrowser не подходит — он строит браузер из
// Options сам), здесь контекст уже поднят вызывающим для тонких сценариев
// (зонд, таймаут создания контекста). Освобождение — Cleanup теста.
func switchableBrowser(t *testing.T, tr *switchableCDP, opts Options) *Browser {
	t.Helper()
	allocCtx, cancelAlloc := chromedp.NewAllocatorContext(context.Background(),
		newRemoteAllocator(opts))
	bctx, cancelBrowser := chromedp.NewContext(allocCtx)
	t.Cleanup(func() {
		cancelBrowser()
		cancelAlloc()
	})
	if err := initBrowserSession(bctx, opts); err != nil {
		t.Fatalf("bootstrap браузера поверх фейка: %v", err)
	}
	return &Browser{opts: opts, ctx: bctx, cancel: cancelBrowser, cancelAlloc: cancelAlloc,
		probeTimeout: testProbeTimeout}
}

// TestManagerRecoversAfterBrowserDeath — приёмник задачи T-218: смерть
// «браузера» (полузакрытое соединение) → попытки контекстов не копятся
// (класс browser, а не контекстный ретраи на мёртвом сокете) → менеджер
// сам меняет поколение → после «воскрешения» браузера (новое соединение)
// рендер продолжается; восстановление ≤ минуты; соединения не растут:
// старый сокет закрыт принудительно, живой — ровно один.
func TestManagerRecoversAfterBrowserDeath(t *testing.T) {
	rec := &dialRecorder{}
	overrideRemoteDial(t, rec.dial)

	m := NewManager(context.Background(), remoteOpts())
	// Старт — реальный StartBrowser (полный bootstrap поверх фейка), но
	// с тестовым бюджетом зонда: сценарий дважды ждёт неудачный зонд,
	// продуктовые 5 с × 2 несовместимы с бюджетом прогона (MUTATE_TIMEOUT).
	realBrowser := m.newBrowser
	m.newBrowser = func(ctx context.Context, opts Options) (*Browser, error) {
		b, err := realBrowser(ctx, opts)
		if b != nil {
			b.probeTimeout = testProbeTimeout
		}
		return b, err
	}
	// Пул с реальным созданием сессий (зонд + контекст) и мгновенной
	// задачей — см. bootSession.
	m.newPool = func(b *Browser) *Pool {
		p := NewPool(b)
		inner := p.newSession
		p.newSession = func(ctx context.Context) (renderSession, error) {
			s, err := inner(ctx)
			if err != nil {
				return nil, err
			}
			return bootSession{real: s}, nil
		}
		return p
	}
	t.Cleanup(m.Close)

	// Живой браузер: задача проходит, одно соединение.
	if _, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"}); err != nil {
		t.Fatalf("Run на живом браузере: %v", err)
	}
	if conns := rec.list(); len(conns) != 1 {
		t.Fatalf("соединений %d, хочу 1", len(conns))
	}

	// Смерть браузера: ресайкл хоста, соединение полузакрыто.
	started := time.Now()
	dead := rec.list()[0]
	createdBefore := dead.countOf("Target.createBrowserContext") // живой рендер создал контекст

	dead.kill()

	// Воркер восстанавливается сам — без рестарта сервиса: зонд
	// соединения классифицирует смерть как browser, менеджер меняет
	// поколение, задача проходит на новом соединении.
	if _, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"}); err != nil {
		t.Fatalf("воркер не восстановился после смерти браузера: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Minute {
		t.Errorf("восстановление заняло %s, хочу ≤ минуты", elapsed)
	}

	conns := rec.list()
	if len(conns) != 2 {
		t.Fatalf("соединений %d, хочу 2 (реконнект без накопления)", len(conns))
	}
	// После смерти соединения контексты на нём не создаются вовсе
	// (до фикса — зависший createBrowserContext на каждую попытку,
	// инцидент 02.11: ×11 520 WARN за 6 ч).
	if n := conns[0].countOf("Target.createBrowserContext"); n != createdBefore {
		t.Errorf("createBrowserContext на мёртвом соединении +%d после смерти, хочу 0", n-createdBefore)
	}
	// Старый сокет закрыт принудительно (фон — после штатной процедуры
	// chromedp, ≤ её таймаутов), новый живой — ровно один.
	waitForFor(t, 10*time.Second, conns[0].isClosed)
	if conns[1].isClosed() {
		t.Error("живое соединение закрыто")
	}
	// Поколение стабильно: дальнейшие задачи не плодят соединений.
	for range 3 {
		if _, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"}); err != nil {
			t.Fatalf("Run после восстановления: %v", err)
		}
	}
	if got := len(rec.list()); got != 2 {
		t.Errorf("соединений %d после серии задач, хочу 2", got)
	}
}

// TestRemoteAllocatorClosesConnectionOnCancel — принудительное закрытие
// сокета при отмене контекста жизненного цикла: «чёрная дыра» не может
// удержать соединение (насосы chromedp зависли бы в записи/чтении —
// upstream-аллокатор ждал бы их неограниченно).
func TestRemoteAllocatorClosesConnectionOnCancel(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})

	opts := remoteOpts()
	a := newRemoteAllocator(opts)
	allocCtx, cancelAlloc := chromedp.NewAllocatorContext(context.Background(), a)
	bctx, cancelBrowser := chromedp.NewContext(allocCtx)
	if err := initBrowserSession(bctx, opts); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	tr.kill()
	// Отмена контекста жизненного цикла — контракт аллокатора («canceling
	// the provided context frees all resources»): сокет обязан закрыться
	// принудительно, не дожидаясь зависших насосов chromedp.
	cancelAlloc()

	waitForFor(t, 10*time.Second, tr.isClosed)
	a.Wait() // фоновое закрытие соединения завершено
	cancelBrowser()
}

// TestBrowserProbeConnDeadConnection — зонд соединения против «чёрной
// дыры» T-218 (полузакрытый сокет: транспорт «жив», ответов нет): живое
// отвечает; после смерти первая неудача поколение ещё не гасит —
// одиночный таймаут не отличает дыру от перегруженного Chromium (T-226),
// вторая подряд гасит залипающе, Alive это видит даже при живом (по
// мнению chromedp) контексте — состояние инцидента 02.11.
func TestBrowserProbeConnDeadConnection(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	if !b.Alive() {
		t.Fatal("живое соединение: Alive")
	}
	if !b.probeConn() {
		t.Fatal("зонд живого соединения не прошёл")
	}

	tr.kill()
	// Первая неудача: класс browser (зонд не прошёл), но поколение живо.
	if b.probeConn() {
		t.Fatal("зонд мёртвого соединения прошёл")
	}
	if !b.Alive() {
		t.Fatal("одиночный таймаут «чёрной дыры» погасил поколение (T-226: требуется серия)")
	}
	// Вторая подряд — lost залипающе, Manager увидит смену поколения.
	if b.probeConn() {
		t.Fatal("зонд мёртвого соединения прошёл")
	}
	if b.Alive() {
		t.Fatal("мёртвое соединение: Alive обязан быть false (для Manager — смена поколения)")
	}
	// Залипание: повторный зонд не ждёт probeTimeout и не «оживляет».
	started := time.Now()
	if b.probeConn() {
		t.Fatal("повторный зонд мёртвого соединения прошёл")
	}
	if elapsed := time.Since(started); elapsed > probeTimeout/2 {
		t.Errorf("повторный зонд на залипающем lost занял %s, хочу мгновенно", elapsed)
	}
}

// TestProbeConnTimeoutTwoFailures — приёмник T-226: одиночный таймаут
// зонда не гасит поколение. Сценарий — «перегруженный Chromium»: соединение
// живо (транспорт отвечает на прочие команды), Target.getTargets не успевает
// за probeTimeout. Обратная сторона контрмеры T-218: зонд ходит перед
// каждой задачей, и разовый лаг занятого браузера не должен стоить
// поколения (рестарт, разрыв чужих задач) — lost ставится со второй
// неудачи подряд, успех между неудачами сбрасывает счётчик.
func TestProbeConnTimeoutTwoFailures(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	if !b.probeConn() {
		t.Fatal("зонд живого соединения не прошёл")
	}

	// Одиночный таймаут: неудача без смерти поколения.
	tr.hangMethod("Target.getTargets")
	started := time.Now()
	if b.probeConn() {
		t.Fatal("зонд висящего getTargets прошёл")
	}
	// Пин подмены бюджета: продуктовый probeTimeout зонд ждать не должен
	// (без подмены поля эта проверка падает на ~5 с).
	if elapsed := time.Since(started); elapsed >= probeTimeout {
		t.Errorf("зонд ждал полный продуктовый probeTimeout (%s) — тестовый бюджет не подменён", elapsed)
	}
	if !b.Alive() {
		t.Fatal("одиночный таймаут зонда погасил поколение (T-226: ложная смерть на нагруженном воркере)")
	}

	// Успех между неудачами сбрасывает счётчик: следующий одиночный
	// таймаут — всё ещё не смерть.
	tr.unhangMethod("Target.getTargets")
	if !b.probeConn() {
		t.Fatal("зонд живого соединения после сброса не прошёл")
	}
	tr.hangMethod("Target.getTargets")
	if b.probeConn() {
		t.Fatal("зонд висящего getTargets прошёл")
	}
	if !b.Alive() {
		t.Fatal("таймаут сразу после успеха (сброс счётчика) погасил поколение")
	}

	// Вторая подряд неудача: поколение гасится залипающе.
	if b.probeConn() {
		t.Fatal("зонд висящего getTargets прошёл")
	}
	if b.Alive() {
		t.Fatal("двойной таймаут зонда не погасил поколение")
	}
	// Залипание: повторный зонд не ждёт probeTimeout и не «оживляет».
	started = time.Now()
	if b.probeConn() {
		t.Fatal("повторный зонд потерянного браузера прошёл")
	}
	if elapsed := time.Since(started); elapsed > probeTimeout/2 {
		t.Errorf("повторный зонд на залипающем lost занял %s, хочу мгновенно", elapsed)
	}
}

// TestProbeConnTransportDeathInstantLost — доказанная смерть транспорта
// (закрытый сокет: чтение падает, chromedp закрывает LostConnection, ошибки
// вызова оборачивают context.Canceled) не ослаблена контрмерой T-226:
// lost ставится первой же неудачей, без серии, мгновенно.
func TestProbeConnTransportDeathInstantLost(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	// Закрытие сокета (аналог FIN/RST — «use of closed network
	// connection»): chromedp узнаёт от чтения и закрывает LostConnection.
	_ = tr.Close()
	c := chromedp.FromContext(b.ctx)
	waitForFor(t, 5*time.Second, func() bool {
		select {
		case <-c.Browser.LostConnection:
			return true
		default:
			return false
		}
	})

	started := time.Now()
	if b.probeConn() {
		t.Fatal("зонд закрытого соединения прошёл")
	}
	if elapsed := time.Since(started); elapsed > probeTimeout/2 {
		t.Errorf("зонд закрытого соединения ждал %s — смерть транспорта детектится мгновенно, без probeTimeout", elapsed)
	}
	if !b.lost.Load() {
		t.Fatal("транспортная смерть не залипнула с первой неудачи (ослаблен детект T-218)")
	}
	if b.Alive() {
		t.Fatal("потерянное соединение: Alive обязан быть false")
	}
}

// TestProbeConnProtocolErrorSeries — ответ-ошибка протокола на getTargets
// при живом соединении (браузер ответил — транспорт работает) относится к
// «необъяснённым» неудачам T-226, а не к доказанной смерти: одиночный
// ошибочный ответ не гасит поколение (класс ложных смертей), серия из
// двух — гасит.
func TestProbeConnProtocolErrorSeries(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	tr.failMethod("Target.getTargets")
	if b.probeConn() {
		t.Fatal("зонд с ответом-ошибкой прошёл")
	}
	if !b.Alive() {
		t.Fatal("одиночный ошибочный ответ протокола погасил поколение (T-226: соединение живо — браузер ответил)")
	}
	if b.probeConn() {
		t.Fatal("зонд с ответом-ошибкой прошёл")
	}
	if b.Alive() {
		t.Fatal("серия ошибочных ответов не погасила поколение")
	}
}

// TestSessionStartTimeoutDeadConnection — классификация таймаута
// создания контекста: мёртвое соединение — класс browser (Manager
// меняет поколение), висящая команда при живом соединении — класс
// context (существующий повтор на новом контексте).
func TestSessionStartTimeoutDeadConnection(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	// Мёртвое соединение: createBrowserContext висит, зонд не отвечает.
	tr.kill()
	sctx, scancel := chromedp.NewContext(b.ctx, chromedp.WithNewBrowserContext())
	defer scancel()
	s := &Session{b: b, ctx: sctx, cancel: scancel}
	err := s.start(context.Background())
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassBrowser {
		t.Fatalf("таймаут создания контекста на мёртвом соединении: класс %v (%v), хочу browser", ClassOf(err), err)
	}
}

// TestSessionStartTimeoutSlowBrowser — висящий createBrowserContext при
// живом соединении (зонд проходит) остаётся классом context: повтор на
// новом контексте, как до T-218.
func TestSessionStartTimeoutSlowBrowser(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	tr.hangMethod("Target.createBrowserContext")
	sctx, scancel := chromedp.NewContext(b.ctx, chromedp.WithNewBrowserContext())
	defer scancel()
	s := &Session{b: b, ctx: sctx, cancel: scancel}
	err := s.start(context.Background())
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassContext {
		t.Fatalf("таймаут создания контекста при живом соединении: класс %v (%v), хочу context", ClassOf(err), err)
	}
}

// TestStartBrowserBootstrapTimeout — потолок инициализации
// (initBrowserSession, T-218): bootstrap-команды chromedp не ограничены
// изнутри — подвисший при старте Chromium держал бы старт поколения (а с
// ним и мьютекс менеджера) бесконечно. Подвисшая командаbootstrap'а →
// StartBrowser отказывает классом browser за потолок DialTimeout +
// SessionStartTimeout, сокет закрыт; после «выздоровления» браузера
// следующий старт проходит.
func TestStartBrowserBootstrapTimeout(t *testing.T) {
	tr := newSwitchableCDP()
	var dials int
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		dials++
		if dials == 1 {
			return tr, nil
		}
		return newSwitchableCDP(), nil
	})

	// Служебная вкладка браузерной сессии не создаётся: соединение есть,
	// ответов нет (подвисший при старте Chromium).
	tr.hangMethod("Target.createTarget")

	opts := remoteOpts() // потолок инициализации = 1 с + 300 мс
	started := time.Now()
	_, err := StartBrowser(context.Background(), opts)
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassBrowser {
		t.Fatalf("таймаут инициализации: класс %v (%v), хочу browser", ClassOf(err), err)
	}
	if !strings.Contains(err.Error(), "таймаут инициализации") {
		t.Errorf("ошибка не называет причину: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Errorf("старт висел %s — потолок не работает", elapsed)
	}
	// Сокет подвисшего соединения закрыт принудительно (гигиена отмен
	// StartBrowser + аллокатор), не ждёт «выздоровления» браузера.
	waitForFor(t, 10*time.Second, tr.isClosed)

	// Браузер «выздоровел» (hang снят — новый транспорт отвечает):
	// следующий старт поколения проходит, соединение живо.
	b, err := StartBrowser(context.Background(), opts)
	if err != nil {
		t.Fatalf("старт после восстановления: %v", err)
	}
	defer b.Close()
	if !b.Alive() {
		t.Error("новое поколение живо")
	}
}

// TestSessionRunPageTimeoutDeadConnection — обрыв соединения посреди
// страницы (T-218, ревью P2): таймаут страницы на полузакрытом сокете —
// не тарпит, а инфраструктурный отказ: зонд в Session.wrap реклассифицирует
// его в browser, Manager сменит поколение и повторит задачу.
func TestSessionRunPageTimeoutDeadConnection(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	s, err := b.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession на живом соединении: %v", err)
	}
	defer s.Close()

	// Обрыв посреди задачи: команды уходят, ответов нет — первый же этап
	// (startBlock/Fetch.enable) висит до таймаута страницы.
	tr.kill()
	_, err = s.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassBrowser {
		t.Fatalf("таймаут страницы на мёртвом соединении: класс %v (%v), хочу browser", ClassOf(err), err)
	}
}

// TestResolveRemoteWSURL — уточнение WebSocket-адреса: готовый адрес
// /devtools/browser/ используется как есть (без сети), иначе — запрос
// /json/version; ответ без webSocketDebuggerUrl — ошибка.
func TestResolveRemoteWSURL(t *testing.T) {
	ctx := context.Background()

	ready := "ws://127.0.0.1:9222/devtools/browser/abc"
	ws, err := resolveRemoteWSURL(ctx, ready, time.Second)
	if err != nil {
		t.Fatalf("готовый адрес: %v", err)
	}
	if ws != ready {
		t.Errorf("готовый адрес изменён: %s", ws)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/json/version" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, `{"Browser":"x","webSocketDebuggerUrl":"ws://127.0.0.1:9222/devtools/browser/xyz"}`)
	}))
	defer srv.Close()
	ws, err = resolveRemoteWSURL(ctx, srv.URL, time.Second)
	if err != nil {
		t.Fatalf("уточнение адреса: %v", err)
	}
	if ws != "ws://127.0.0.1:9222/devtools/browser/xyz" {
		t.Errorf("webSocketDebuggerUrl: %s", ws)
	}

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{}`)
	}))
	defer empty.Close()
	if _, err := resolveRemoteWSURL(ctx, empty.URL, time.Second); err == nil {
		t.Error("ответ без webSocketDebuggerUrl обязан ошибкой")
	} else if !strings.Contains(err.Error(), "webSocketDebuggerUrl") {
		t.Errorf("ошибка не называет причину: %v", err)
	}
}

// overrideRemoteResolve — подмена шва резолва на время теста (стиль
// overrideRemoteDial).
func overrideRemoteResolve(t *testing.T, f func(context.Context, string) ([]net.IPAddr, error)) {
	t.Helper()
	old := remoteResolve
	remoteResolve = f
	t.Cleanup(func() { remoteResolve = old })
}

// TestResolveRemoteWSURLPreferIPv4 — выбор адреса из ответа DNS (T-229):
// IPv6-first порядок не должен отдавать v6-адрес Chromium, слушающему
// только IPv4 (подключение пошло бы вторым адресом); v4-first и v6-only
// ответы работают первым адресом. Путь /devtools/browser/ — без сети,
// адрес проверяется сразу после резолва.
func TestResolveRemoteWSURLPreferIPv4(t *testing.T) {
	ctx := context.Background()
	v6 := net.ParseIP("2001:db8::1")
	v4 := net.ParseIP("192.0.2.10")
	const withPath = "ws://chromium.example:9222/devtools/browser/abc"
	const noPort = "ws://chromium.example/devtools/browser/abc"

	for _, tc := range []struct {
		name  string
		raw   string
		addrs []net.IPAddr
		want  string
	}{
		{"IPv6-first — выбран IPv4", withPath, []net.IPAddr{{IP: v6}, {IP: v4}}, "ws://192.0.2.10:9222/devtools/browser/abc"},
		{"IPv4-first — он же", withPath, []net.IPAddr{{IP: v4}, {IP: v6}}, "ws://192.0.2.10:9222/devtools/browser/abc"},
		{"только IPv6 — первый адрес", withPath, []net.IPAddr{{IP: v6}}, "ws://[2001:db8::1]:9222/devtools/browser/abc"},
		// Без порта: v4 — как есть, v6 — в скобках (голые двоеточия
		// host перепарсились бы портом, ревью P3-7).
		{"IPv4 без порта", noPort, []net.IPAddr{{IP: v6}, {IP: v4}}, "ws://192.0.2.10/devtools/browser/abc"},
		{"IPv6 без порта — в скобках", noPort, []net.IPAddr{{IP: v6}}, "ws://[2001:db8::1]/devtools/browser/abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			overrideRemoteResolve(t, func(_ context.Context, _ string) ([]net.IPAddr, error) {
				return tc.addrs, nil
			})
			ws, err := resolveRemoteWSURL(ctx, tc.raw, time.Second)
			if err != nil {
				t.Fatalf("резолв: %v", err)
			}
			if ws != tc.want {
				t.Errorf("адрес %s, хочу %s", ws, tc.want)
			}
		})
	}
}

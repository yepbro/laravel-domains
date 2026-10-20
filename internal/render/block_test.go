package render

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// fakeCDPSession — сессия CDP для тестов блокировки: помнит команды
// (Fetch.enable/failRequest/continueRequest/disable) и может валить их
// по имени метода. Subscribe не используется — события подаются в насос
// собственным итератором (см. pumpEvents).
type fakeCDPSession struct {
	mu    sync.Mutex
	calls []fakeCDPCall
	fail  map[string]error
	// gate — необязательные шлюзы по методам: Call ждёт закрытия канала
	// (см. комментарий в Call).
	gate map[string]chan struct{}
}

// fakeCDPCall — выполненная команда: метод и параметры как есть, дедлайн
// контекста вызова (нулевой — вызов без дедлайна).
type fakeCDPCall struct {
	Method   string
	Params   any
	Deadline time.Time
}

func (f *fakeCDPSession) Call(ctx context.Context, method string, params, _ any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	f.mu.Lock()
	call := fakeCDPCall{Method: method, Params: params}
	if dl, ok := ctx.Deadline(); ok {
		call.Deadline = dl
	}
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	if err, ok := f.fail[method]; ok {
		return err
	}
	// gate — управляемая блокировка: Call с методом из gate не возвращается,
	// пока шлюз не закроют — безусловно, вопреки отмене контекста: тесту
	// нужна задержка слушателя ровно внутри blockRespond (отмена контекста
	// слушателя и есть штатный путь выхода, его и проверяем).
	if g, ok := f.gate[method]; ok {
		<-g
	}
	return nil
}

func (f *fakeCDPSession) Subscribe(string) (<-chan jsonv2.Value, func()) {
	panic("тесты подают события собственным итератором, не через Subscribe")
}

// calls — копия журнала команд.
func (f *fakeCDPSession) callsOf() []fakeCDPCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCDPCall(nil), f.calls...)
}

// countOf — число команд с данным методом.
func (f *fakeCDPSession) countOf(method string) int {
	n := 0
	for _, c := range f.callsOf() {
		if c.Method == method {
			n++
		}
	}
	return n
}

// waitFor — пока cond не истинна или не истечёт срок (детерминизм
// асинхронного насоса).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("условие не наступило за 2 с")
}

// TestBlockResourceType — перечень блокируемых и пропускаемых типов.
// Блокируются ровно Image/Media/Font (ТЗ 5.6); полный список типов
// протокола перечислен явно: новый тип в cdproto по умолчанию проходит
// (консервативное поведение, не ломает загрузку страницы).
func TestBlockResourceType(t *testing.T) {
	all := []network.ResourceType{
		network.ResourceTypeDocument,
		network.ResourceTypeStylesheet,
		network.ResourceTypeImage,
		network.ResourceTypeMedia,
		network.ResourceTypeFont,
		network.ResourceTypeScript,
		network.ResourceTypeTextTrack,
		network.ResourceTypeXHR,
		network.ResourceTypeFetch,
		network.ResourceTypePrefetch,
		network.ResourceTypeEventSource,
		network.ResourceTypeWebSocket,
		network.ResourceTypeManifest,
		network.ResourceTypeSignedExchange,
		network.ResourceTypePing,
		network.ResourceTypeCSPViolationReport,
		network.ResourceTypePreflight,
		network.ResourceTypeFedCM,
		network.ResourceTypeOther,
	}
	want := map[network.ResourceType]bool{
		network.ResourceTypeImage: true,
		network.ResourceTypeMedia: true,
		network.ResourceTypeFont:  true,
	}
	for _, rt := range all {
		if got := blockResourceType(rt); got != want[rt] {
			t.Errorf("blockResourceType(%s) = %v, хочу %v", rt, got, want[rt])
		}
	}
	// Неизвестный тип — проходит.
	if blockResourceType(network.ResourceType("SomethingNew")) {
		t.Error("неизвестный тип должен проходить")
	}
	// Пустой тип (Chrome его не присылает) — проходит.
	if blockResourceType(network.ResourceType("")) {
		t.Error("пустой тип должен проходить")
	}
}

// TestBlockPatterns — паттерны Fetch.enable: по одному на блокируемый
// тип, стадия Request (отказ до отправки запроса), без URL-фильтра.
func TestBlockPatterns(t *testing.T) {
	patterns := blockPatterns()
	if len(patterns) != len(blockBlockedTypes) {
		t.Fatalf("паттернов %d, хочу %d (по числу блокируемых типов)",
			len(patterns), len(blockBlockedTypes))
	}
	seen := make(map[network.ResourceType]bool)
	for _, p := range patterns {
		if !blockResourceType(p.ResourceType) {
			t.Errorf("паттерн с незаблокированным типом %s", p.ResourceType)
		}
		if seen[p.ResourceType] {
			t.Errorf("дублирующийся тип паттерна %s", p.ResourceType)
		}
		seen[p.ResourceType] = true
		if p.RequestStage != fetch.RequestStageRequest {
			t.Errorf("стадия паттерна %s: %s, хочу Request",
				p.ResourceType, p.RequestStage)
		}
		if p.URLPattern != "" {
			t.Errorf("URL-фильтр паттерна %s: %q, хочу пусто (только по типу)",
				p.ResourceType, p.URLPattern)
		}
	}
}

// TestBlockEnable — включение перехвата командой Fetch.enable с паттернами
// по блокируемым типам; ошибка команды возвращается вызывающему.
func TestBlockEnable(t *testing.T) {
	t.Run("команда и параметры", func(t *testing.T) {
		fake := &fakeCDPSession{}
		if err := blockEnable(context.Background(), fake); err != nil {
			t.Fatalf("blockEnable: %v", err)
		}
		calls := fake.callsOf()
		if len(calls) != 1 || calls[0].Method != "Fetch.enable" {
			t.Fatalf("команды %v, хочу одну Fetch.enable", calls)
		}
		params, ok := calls[0].Params.(fetch.EnableParams)
		if !ok {
			t.Fatalf("параметры %T, хочу fetch.EnableParams", calls[0].Params)
		}
		if len(params.Patterns) != len(blockBlockedTypes) {
			t.Fatalf("паттернов %d, хочу %d", len(params.Patterns), len(blockBlockedTypes))
		}
		for _, p := range params.Patterns {
			if !blockResourceType(p.ResourceType) || p.RequestStage != fetch.RequestStageRequest {
				t.Errorf("паттерн %+v вне блокируемого набора", p)
			}
		}
	})

	t.Run("ошибка CDP прокладывается", func(t *testing.T) {
		boom := errors.New("session gone")
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.enable": boom}}
		err := blockEnable(context.Background(), fake)
		if !errors.Is(err, boom) {
			t.Fatalf("ошибка %v, хочу %v", err, boom)
		}
	})
}

// TestBlockRespond — судьба приостановленного запроса по типу:
// блокируемые — Fetch.failRequest(BlockedByClient) с тем же RequestID,
// остальные — Fetch.continueRequest; ошибка команды не панике.
func TestBlockRespond(t *testing.T) {
	cases := []struct {
		typ  network.ResourceType
		fail bool
	}{
		{network.ResourceTypeImage, true},
		{network.ResourceTypeMedia, true},
		{network.ResourceTypeFont, true},
		{network.ResourceTypeDocument, false},
		{network.ResourceTypeStylesheet, false},
		{network.ResourceTypeScript, false},
		{network.ResourceTypeXHR, false},
		{network.ResourceTypeFetch, false},
		{network.ResourceTypeWebSocket, false},
		{network.ResourceTypeManifest, false},
		{network.ResourceTypeOther, false},
	}
	for _, tc := range cases {
		name := tc.typ.String() + "→" + map[bool]string{true: "fail", false: "continue"}[tc.fail]
		t.Run(name, func(t *testing.T) {
			fake := &fakeCDPSession{}
			ev := fetch.EventRequestPaused{RequestID: "req-1", ResourceType: tc.typ}
			blockRespond(context.Background(), fake, ev)

			calls := fake.callsOf()
			if len(calls) != 1 {
				t.Fatalf("команд %d, хочу одну: %v", len(calls), calls)
			}
			want := "Fetch.continueRequest"
			if tc.fail {
				want = "Fetch.failRequest"
			}
			if calls[0].Method != want {
				t.Fatalf("команда %s, хочу %s", calls[0].Method, want)
			}
			if tc.fail {
				params, ok := calls[0].Params.(fetch.FailRequestParams)
				if !ok {
					t.Fatalf("параметры %T, хочу fetch.FailRequestParams", calls[0].Params)
				}
				if params.RequestID != ev.RequestID {
					t.Errorf("RequestID %s, хочу %s", params.RequestID, ev.RequestID)
				}
				if params.ErrorReason != network.ErrorReasonBlockedByClient {
					t.Errorf("ErrorReason %s, хочу BlockedByClient", params.ErrorReason)
				}
			} else {
				params, ok := calls[0].Params.(fetch.ContinueRequestParams)
				if !ok {
					t.Fatalf("параметры %T, хочу fetch.ContinueRequestParams", calls[0].Params)
				}
				if params.RequestID != ev.RequestID {
					t.Errorf("RequestID %s, хочу %s", params.RequestID, ev.RequestID)
				}
			}
		})
	}

	t.Run("ошибка команды проглатывается", func(t *testing.T) {
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.failRequest": errors.New("dead")}}
		ev := fetch.EventRequestPaused{RequestID: "r", ResourceType: network.ResourceTypeImage}
		blockRespond(context.Background(), fake, ev) // не паникует
	})
}

// TestBlockDisable — выключение перехвата: одна команда Fetch.disable под
// собственным коротким таймаутом blockDisableTimeout (обычный вызов CDP —
// миллисекунды, дольше ждать незачем), ошибка best-effort не возвращается.
func TestBlockDisable(t *testing.T) {
	t.Run("команда и таймаут", func(t *testing.T) {
		fake := &fakeCDPSession{}
		blockDisable(context.Background(), fake)
		calls := fake.callsOf()
		if len(calls) != 1 || calls[0].Method != "Fetch.disable" {
			t.Fatalf("команды %v, хочу одну Fetch.disable", calls)
		}
		rem := time.Until(calls[0].Deadline)
		if rem <= 1500*time.Millisecond || rem > 2500*time.Millisecond {
			t.Errorf("таймаут Fetch.disable: осталось %s, хочу ≈2 с (границы 1.5–2.5 с)", rem)
		}
	})

	t.Run("ошибка проглатывается", func(t *testing.T) {
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.disable": errors.New("вкладка умерла")}}
		blockDisable(context.Background(), fake) // не паникует
		if fake.countOf("Fetch.disable") != 1 {
			t.Error("команда Fetch.disable не выполнена")
		}
	})
}

// pumpEvents — итератор событий в духе cdp.Events: значения из канала,
// ошибка при отмене ctx (терминально), конец при закрытии канала. Даёт
// тесту контроль над потоком событий без реальной подписки CDP; ctx
// передаёт сам blockPump (внутренний контекст слушателя).
func pumpEvents(events chan fetch.EventRequestPaused,
) func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
	return func(ctx context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return func(yield func(fetch.EventRequestPaused, error) bool) {
			for {
				select {
				case <-ctx.Done():
					yield(fetch.EventRequestPaused{}, ctx.Err())
					return
				case ev, ok := <-events:
					if !ok {
						return
					}
					if !yield(ev, nil) {
						return
					}
				}
			}
		}
	}
}

// TestBlockPumpLifecycle — насос событий: отвечает на события обоих
// категорий, завершается по stop без утечки горутины, не трогает события
// после остановки, а при внезапном закрытии канала выключает перехват
// (fail-open).
func TestBlockPumpLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake)

	// События обоих видов получают ответы.
	events <- fetch.EventRequestPaused{RequestID: "img", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return fake.countOf("Fetch.failRequest") == 1 })
	events <- fetch.EventRequestPaused{RequestID: "js", ResourceType: network.ResourceTypeScript}
	waitFor(t, func() bool { return fake.countOf("Fetch.continueRequest") == 1 })

	// stop возвращает управление — горутина закончилась.
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop не дождался выхода слушателя")
	}

	// После остановки события не обрабатываются.
	before := len(fake.callsOf())
	select {
	case events <- fetch.EventRequestPaused{RequestID: "late", ResourceType: network.ResourceTypeMedia}:
		t.Fatal("событие принято после stop")
	default:
	}
	if got := len(fake.callsOf()); got != before {
		t.Errorf("команд после stop: %d (было %d)", got, before)
	}
	if fake.countOf("Fetch.disable") != 0 {
		t.Error("обычный stop не делает fail-open disable — его делает startBlock")
	}
}

// TestBlockPumpCtxDone — отмена родительского контекста (конец страницы)
// завершает насос без fail-open disable (задача закончилась, сессию
// закрывает пул).
func TestBlockPumpCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake)

	events <- fetch.EventRequestPaused{RequestID: "i", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return fake.countOf("Fetch.failRequest") == 1 })

	cancel()
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("насос не вышел по отмене контекста")
	}
	if fake.countOf("Fetch.disable") != 0 {
		t.Error("отмена контекста не должна звать disable (сессию закрывает пул)")
	}
	// Ошибка итератора не проваливается в blockRespond: нулевое событие
	// ошибки не получает ответа (не было бы и лишнего continueRequest).
	if got := fake.countOf("Fetch.continueRequest"); got != 0 {
		t.Errorf("после отмены отвечено на нулевое событие: continueRequest %d, хочу 0", got)
	}
}

// TestBlockPumpEventsClosed — закрытие канала событий при живом контексте
// (вкладка умерла): насос выходит и выключает перехват, чтобы
// блокируемые запросы не зависли до таймаута страницы.
func TestBlockPumpEventsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake)

	close(events)
	waitFor(t, func() bool { return fake.countOf("Fetch.disable") == 1 })
	stop() // после выхода насоса возвращает сразу
}

// TestBlockPumpEventError — ошибка потока событий при живом контексте
// (неразбираемое событие — yield ошибки терминален, контекст не отменен):
// насос выходит молча — без ответа на нулевое событие и без fail-open
// disable (жизненный цикл перехвата держит startBlock, а не мусор в
// потоке). Детерминизм — синхронность yield: тело итерации насоса
// отрабатывает ДО возврата из yield, поэтому зонд yieldDone гарантирует,
// что ответ мутанта был бы записан в фейк при ещё живом контексте.
func TestBlockPumpEventError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &fakeCDPSession{}
	probe := &errPumpProbe{
		entered:   make(chan struct{}),
		yieldDone: make(chan struct{}),
	}
	stop := blockPump(ctx, probe.events(errors.New("событие не разбирается")), fake)

	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("насос не вошёл в итератор событий")
	}
	select {
	case <-probe.yieldDone:
	case <-time.After(2 * time.Second):
		t.Fatal("итерация ошибки не завершена (тело насоса не отработало)")
	}
	stop()
	if calls := fake.callsOf(); len(calls) != 0 {
		t.Fatalf("команды %v, хочу ноль: ошибка потока не отвечает на "+
			"событие и не выключает перехват", calls)
	}
}

// errPumpProbe — итератор в духе cdp.Events: одна терминальная ошибка при
// живом контексте (как decoding-ошибка полезной нагрузки) с зондами
// синхронности — entered при входе в итератор, yieldDone по возврату из
// yield (тело итерации потребителя к этому моменту отработало).
type errPumpProbe struct {
	entered   chan struct{}
	yieldDone chan struct{}
}

func (p *errPumpProbe) events(err error) func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
	return func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return func(yield func(fetch.EventRequestPaused, error) bool) {
			close(p.entered)
			yield(fetch.EventRequestPaused{}, err)
			close(p.yieldDone)
		}
	}
}

// TestBlockPumpRespondError — ошибка ответа не останавливает насос:
// следующие события обрабатываются (fail-open не нужен, страница жива).
func TestBlockPumpRespondError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused, 2)
	fake := &fakeCDPSession{fail: map[string]error{"Fetch.failRequest": errors.New("boom")}}
	stop := blockPump(ctx, pumpEvents(events), fake)

	events <- fetch.EventRequestPaused{RequestID: "a", ResourceType: network.ResourceTypeImage}
	events <- fetch.EventRequestPaused{RequestID: "b", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return len(fake.callsOf()) >= 2 }) // обе попытки

	stop()
}

// TestBlockPumpStopWaitsForListener — stop дожидается выхода слушателя
// (контракт blockPump: «горутина не переживает вызов»): пока насос сидит в
// blockRespond за шлюзом, stop не возвращается; после освобождения —
// возвращается быстро.
func TestBlockPumpStopWaitsForListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	gate := make(chan struct{})
	fake := &fakeCDPSession{gate: map[string]chan struct{}{"Fetch.continueRequest": gate}}
	stop := blockPump(ctx, pumpEvents(events), fake)

	// Слушатель вошёл в blockRespond и ждёт освобождения шлюза: команда
	// записана в журнал до ожидания.
	events <- fetch.EventRequestPaused{RequestID: "gated", ResourceType: network.ResourceTypeScript}
	waitFor(t, func() bool { return len(fake.callsOf()) == 1 })

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("stop вернулся до выхода слушателя из blockRespond")
	case <-time.After(100 * time.Millisecond):
	}

	close(gate) // отпустить blockRespond — насос завершается по отмене
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop не дождался выхода слушателя после освобождения шлюза")
	}
}

// TestStartBlock — включение блокировки в конвейере сессии: отказы контекста
// вкладки и Fetch.enable, штатный stop (гасит слушателя и выключает
// перехват). События перехвата подаются через fakeCDP — in-memory транспорт
// chromedp (cdpfake_test.go), живой браузер не нужен.
func TestStartBlock(t *testing.T) {
	t.Run("контекст сессии — не chromedp", func(t *testing.T) {
		s := &Session{ctx: context.Background()}
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "контекст вкладки не инициализирован") {
			t.Fatalf("ошибка %v, хочу «контекст вкладки не инициализирован»", err)
		}
	})

	t.Run("chromedp-контекст без вкладки", func(t *testing.T) {
		tr := newFakeCDP()
		allocCtx, cancelAlloc := chromedp.NewAllocatorContext(
			context.Background(), &fakeCDPAllocator{tr: tr})
		defer cancelAlloc()
		sctx, cancel := chromedp.NewContext(allocCtx) // без Do: Target не создан
		defer cancel()
		s := &Session{ctx: sctx}
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "контекст вкладки не инициализирован") {
			t.Fatalf("ошибка %v, хочу «контекст вкладки не инициализирован»", err)
		}
	})

	t.Run("ошибка Fetch.enable", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		tr.setError("Fetch.enable", &cdproto.Error{Code: -32000, Message: "домен Fetch сломан"})
		s := &Session{ctx: ctx}
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "включение перехвата Fetch") {
			t.Fatalf("ошибка %v, хочу wrap «включение перехвата Fetch»", err)
		}
		if !strings.Contains(err.Error(), "домен Fetch сломан") {
			t.Errorf("исходная ошибка не пробита: %v", err)
		}
		// Слушатель погашен ещё на пути ошибки: события перехвата не
		// получают ответов (мутант вырезания stopListen оставил бы насос
		// живым — событие получило бы failRequest/continueRequest).
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"late","resourceType":"Image"}`)
		assertNoNewCDPCalls(t, tr, "Fetch.requestPaused-после-ошибки")
	})

	t.Run("stop гасит слушателя и выключает перехват", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := &Session{ctx: ctx}
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		if tr.countOf("Fetch.enable") != 1 {
			t.Fatalf("Fetch.enable выполнен %d раз, хочу 1", tr.countOf("Fetch.enable"))
		}

		// Событие перехвата получает ответ: слушатель работает.
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"img-1","resourceType":"Image"}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 1 })

		stop()
		waitFor(t, func() bool { return tr.countOf("Fetch.disable") == 1 })

		// После stop события не отвечаются (слушатель погашен) и новых
		// команд нет.
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"late","resourceType":"Media"}`)
		assertNoNewCDPCalls(t, tr, "Fetch.requestPaused-после-stop")
	})
}

// assertNoNewCDPCalls — негативная проверка: с момента вызова новые команды
// через транспорт не проходят (окно 100 мс — порядок запаса против
// микросекунд цепочки доставки; в стиле пакета — негативные паузы
// TestWaitLoopStaleRequestsDrained).
func assertNoNewCDPCalls(t *testing.T, tr *fakeCDP, what string) {
	t.Helper()
	before := len(tr.callsOf())
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) && len(tr.callsOf()) == before {
		time.Sleep(time.Millisecond)
	}
	if got := len(tr.callsOf()); got != before {
		t.Errorf("%s: прошли новые команды %+v", what, tr.callsOf()[before:])
	}
}

package render

import (
	"context"
	"errors"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
)

// fakeCDPSession — сессия CDP для тестов блокировки: помнит команды
// (Fetch.enable/failRequest/continueRequest/disable) и может валить их
// по имени метода. Subscribe не используется — события подаются в насос
// собственным итератором (см. pumpEvents).
type fakeCDPSession struct {
	mu    sync.Mutex
	calls []fakeCDPCall
	fail  map[string]error
}

// fakeCDPCall — выполненная команда: метод и параметры как есть.
type fakeCDPCall struct {
	Method string
	Params any
}

func (f *fakeCDPSession) Call(ctx context.Context, method string, params, _ any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCDPCall{Method: method, Params: params})
	if err, ok := f.fail[method]; ok {
		return err
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

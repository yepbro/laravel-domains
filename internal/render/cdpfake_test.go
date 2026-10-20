package render

// Герметичная замена Chromium для юнит-тестов подписок и команд вкладки:
// in-memory chromedp.Transport. Живой браузер не нужен — команды и события
// ходят каналами, авто-ответы транспорта обслуживают весь bootstrap chromedp
// (SetDiscoverTargets → Target.targetCreated → attachToTarget → цепочка
// Runtime/Page/Network/… enable). Приём — экспортированные срезы chromedp:
// Transport (интерфейс чтения/записи сообщений CDP), NewBrowserTransport и
// NewAllocatorContext; удалённое соединение (remote.Conn) подменяется
// транспортным фейком, как в прочих тестах пакета подменяются поля структур
// (probeFn, eventsOf у blockPump, fakeExtractCDP).
//
// О чём фейк говорит тестам:
//   - команды — журнал cmds (метод, сессия, параметры) и счётчики по методам;
//   - ошибки команд — setError по методу (после bootstrap'а: например, отказ
//     Network.enable для waitSubscribe);
//   - события — fireSession/fireBrowser: доставляются подпискам
//     chromedp.Events ровно как события реальной вкладки.

import (
	"context"
	"sync"
	"testing"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
)

// fakeCDPSessionID/fakeCDPTargetID — фиксированные идентификаторы вкладки
// фейка: ответ attachToTarget и sessionId событий должны совпадать.
const (
	fakeCDPSessionID = "fake-session"
	fakeCDPTargetID  = "fake-target"
)

// fakeCDPCmd — команда, прошедшая через транспорт: идентификатор, метод,
// сессия и параметры (json-текст) как есть.
type fakeCDPCmd struct {
	ID        int64
	Method    string
	SessionID string
	Params    string
}

// fakeCDPReplies — результаты bootstrap-команд chromedp: attachToTarget
// (идентификатор сессии), Runtime.evaluate «self» (не Worker — ветка
// isWorker), getTargets (пустой список — waitTargetGone при закрытии
// заканчивается сразу). Всё прочее получает пустой результат {}.
var fakeCDPReplies = map[string]string{
	"Target.attachToTarget": `{"sessionId":"` + fakeCDPSessionID + `"}`,
	"Runtime.evaluate":      `{"result":{"type":"object","className":"Object"}}`,
	"Target.getTargets":     `{"targetInfos":[]}`,
}

// fakeCDP — in-memory транспорт CDP: Write принимает команды и отвечает на
// них (авто-ответ или сконфигурированная ошибка), Read раздаёт ответы и
// события; inbox общий, порядок доставки FIFO.
type fakeCDP struct {
	mu     sync.Mutex
	cmds   []fakeCDPCmd
	errors map[string]*cdproto.Error

	inbox chan *cdproto.Message
}

func newFakeCDP() *fakeCDP {
	return &fakeCDP{
		errors: make(map[string]*cdproto.Error),
		inbox:  make(chan *cdproto.Message, 1024),
	}
}

// Write удовлетворяет chromedp.Transport: запись команды. Ответ синтезируется
// синхронно (без реального браузера): результат из fakeCDPReplies либо {},
// либо ошибка из errors. Посылка в inbox учитывает ctx транспорта: если
// читатель уже вышел (жизненный цикл браузера погашен), write-loop chromedp
// не подвисает на полном буфере. На браузерный setDiscoverTargets (первый
// шаг bootstrap'а вкладки) дополнительно объявляется страница — chromedp
// ждёт именно этого события.
func (f *fakeCDP) Write(ctx context.Context, m *cdproto.Message) error {
	f.mu.Lock()
	f.cmds = append(f.cmds, fakeCDPCmd{
		ID:        m.ID,
		Method:    string(m.Method),
		SessionID: string(m.SessionID),
		Params:    string(m.Params),
	})
	cmdErr := f.errors[string(m.Method)]
	f.mu.Unlock()

	reply := &cdproto.Message{ID: m.ID, SessionID: m.SessionID}
	if cmdErr != nil {
		reply.Error = cmdErr
	} else {
		res, ok := fakeCDPReplies[string(m.Method)]
		if !ok {
			res = "{}"
		}
		reply.Result = jsonv2.Value(res)
	}
	select {
	case f.inbox <- reply:
	case <-ctx.Done():
		return ctx.Err()
	}

	if m.SessionID == "" && string(m.Method) == "Target.setDiscoverTargets" {
		f.fireBrowser("Target.targetCreated",
			`{"targetInfo":{"targetId":"`+fakeCDPTargetID+
				`","type":"page","url":"about:blank","attached":false}}`)
	}
	return nil
}

// Read удовлетворяет chromedp.Transport: ожидание сообщения (ответ/событие)
// до отмены контекста браузера.
func (f *fakeCDP) Read(ctx context.Context, m *cdproto.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case msg := <-f.inbox:
		*m = *msg
		return nil
	}
}

// Close удовлетворяет chromedp.Transport.
func (f *fakeCDP) Close() error { return nil }

// fireSession — событие сессии вкладки: доставляется подпискам
// chromedp.Events (параметры — валидный json целевого события).
func (f *fakeCDP) fireSession(method, params string) {
	f.inbox <- &cdproto.Message{
		Method:    cdproto.MethodType(method),
		SessionID: target.SessionID(fakeCDPSessionID),
		Params:    jsonv2.Value(params),
	}
}

// fireBrowser — событие браузерного уровня (без сессии).
func (f *fakeCDP) fireBrowser(method, params string) {
	f.inbox <- &cdproto.Message{
		Method: cdproto.MethodType(method),
		Params: jsonv2.Value(params),
	}
}

// setError — отвечать ошибкой на метод (до следующего сброса).
func (f *fakeCDP) setError(method string, e *cdproto.Error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors[method] = e
}

// callsOf — копия журнала команд.
func (f *fakeCDP) callsOf() []fakeCDPCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCDPCmd(nil), f.cmds...)
}

// countOf — число команд с данным методом.
func (f *fakeCDP) countOf(method string) int {
	n := 0
	for _, c := range f.callsOf() {
		if c.Method == method {
			n++
		}
	}
	return n
}

// fakeCDPAllocator — аллокатор поверх fakeCDP: браузер живёт, пока жив
// контекст аллокатора (как remote.Allocator, но транспорт in-memory).
type fakeCDPAllocator struct{ tr *fakeCDP }

func (a *fakeCDPAllocator) Allocate(ctx context.Context, opts ...chromedp.BrowserOption) (*chromedp.Browser, error) {
	wctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return chromedp.NewBrowserTransport(wctx, a.tr, opts...)
}

func (a *fakeCDPAllocator) Wait() {}

// newFakeCDPContext — chromedp-контекст с поднятой вкладкой поверх fakeCDP:
// chromedp.Do проводит полный bootstrap (браузер → targetCreated → attach →
// домены), все его команды обслуживаются авто-ответами. Контекст
// освобождается Cleanup теста.
func newFakeCDPContext(t *testing.T) (context.Context, *fakeCDP) {
	t.Helper()
	tr := newFakeCDP()
	allocCtx, cancelAlloc := chromedp.NewAllocatorContext(context.Background(), &fakeCDPAllocator{tr: tr})
	tctx, cancel := chromedp.NewContext(allocCtx)
	t.Cleanup(func() {
		cancel()
		cancelAlloc()
	})
	if err := chromedp.Do(tctx); err != nil {
		t.Fatalf("bootstrap chromedp-контекста поверх фейка: %v", err)
	}
	return tctx, tr
}

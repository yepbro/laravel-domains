package render

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// Блокировка загрузки изображений, шрифтов и медиа в Chromium — ТЗ 5.6
// («экономия ресурсов»), задача T-062.
//
// Механика: перехват сетевых запросов вкладки доменом CDP Fetch
// (Network.setRequestInterception из протокола удалён; в chromedp v0.21
// перехват — события fetch.RequestPaused). Порядок включения в
// Session.Run: подписка на события → Fetch.enable → Navigate. Подписка
// обязана идти раньше включения: requestPaused, пришедший до подписки,
// остался бы без ответа и держал бы запрос приостановленным до таймаута
// страницы (подписка cdp.Events буферизует события с момента вызова —
// правило «subscribe, trigger, range» из докомментария chromedp.Events).
//
// Fetch.enable получает паттерны только по блокируемым типам ресурсов:
// приостанавливается лишь то, что будет отклонено, — Fetch.failRequest
// (BlockedByClient → net::ERR_BLOCKED_BY_CLIENT) на стадии Request:
// запрос не покидает браузер, не тратится ни трафик, ни декодирование.
// Остальные запросы (Document, Stylesheet, Script, XHR/Fetch, WebSocket,
// …) браузер не приостанавливает вовсе: они проходят сетевым стеком сами,
// и сбой или задержка слушателя не могут повесить страницу (fail-open —
// блокировка экономит ресурсы, но не обеспечивает корректность рендера).
// Скрипты и XHR обязаны грузиться: SPA-оболочки дорисовываются после
// загрузки — рендер и существует ради JS (ТЗ 5.6).

// blockDisableTimeout — предел Fetch.disable при остановке перехвата:
// обычный вызов CDP занимает миллисекунды, дольше ждать незачем.
const blockDisableTimeout = 2 * time.Second

// blockBlockedTypes — блокируемые типы ресурсов (ТЗ 5.6: изображения,
// шрифты, медиа). Полный перечень network.ResourceType см. в тесте
// TestBlockResourceType: всё, чего нет здесь, проходит.
//
// Favicon причислен к Image: иконка — изображение, и запрошенные
// страницей (link rel=icon) Chromium классифицирует как Image — они
// блокируются вместе с остальными картинками. Запрос /favicon.ico,
// инициированный самим браузером вне рендера, приходит в CDP типом Other;
// в headless-shell без UI он не выполняется, а выделять его URL-эвристикой
// (рискуя задеть чужие запросы) из-за одного мелкого файла не стоит.
var blockBlockedTypes = []network.ResourceType{
	network.ResourceTypeImage,
	network.ResourceTypeMedia,
	network.ResourceTypeFont,
}

// blockResourceType — блокируется ли тип ресурса. Неизвестные типы
// (появятся в будущих версиях протокола) проходят: консервативное
// поведение не ломает загрузку страницы.
func blockResourceType(t network.ResourceType) bool {
	for _, b := range blockBlockedTypes {
		if t == b {
			return true
		}
	}
	return false
}

// blockPatterns — паттерны Fetch.enable: по одному на блокируемый тип,
// стадия Request (отказ до отправки — экономится и исходящий трафик).
func blockPatterns() []*fetch.RequestPattern {
	patterns := make([]*fetch.RequestPattern, len(blockBlockedTypes))
	for i, t := range blockBlockedTypes {
		patterns[i] = &fetch.RequestPattern{
			ResourceType: t,
			RequestStage: fetch.RequestStageRequest,
		}
	}
	return patterns
}

// blockEnable включает перехват на сессии CDP вкладки.
func blockEnable(ctx context.Context, sess cdp.Session) error {
	_, err := cdp.Call(ctx, sess, fetch.Enable, fetch.EnableParams{
		Patterns: blockPatterns(),
	})
	return err
}

// blockDisable выключает перехват best-effort: контекст задачи к моменту
// остановки мог уже отмениться (таймаут страницы), поэтому вызов идёт без
// его отмены и с коротким таймаутом; ошибка (умершая вкладка и т. п.) не
// важна — сессия одноразовая.
func blockDisable(ctx context.Context, sess cdp.Session) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blockDisableTimeout)
	defer cancel()
	_, _ = cdp.Call(dctx, sess, fetch.Disable, cdp.Empty{})
}

// blockRespond отвечает на приостановленный запрос: блокируемый тип —
// failRequest, любой другой — continueRequest (страховка: паттерны не
// должны приостанавливать чужие типы, но расхождение классификации не
// должно вешать запрос). Ошибки ответа не фатальны: перехват — экономия,
// страница дорабатывает и без него (fail-open).
func blockRespond(ctx context.Context, sess cdp.Session, ev fetch.EventRequestPaused) {
	var err error
	if blockResourceType(ev.ResourceType) {
		_, err = cdp.Call(ctx, sess, fetch.FailRequest, fetch.FailRequestParams{
			RequestID:   ev.RequestID,
			ErrorReason: network.ErrorReasonBlockedByClient,
		})
	} else {
		_, err = cdp.Call(ctx, sess, fetch.ContinueRequest,
			fetch.ContinueRequestParams{RequestID: ev.RequestID})
	}
	// Логирование (§4 docs/mutation-testing.md): Debug-запись о неудачном
	// ответе не влияет на судьбу запроса (fail-open самой blockRespond);
	// мутанты ветки и вырезания оператора меняют только текст отладочного
	// лога. Аннотации две: branch/if мутатор заходит на узле IfStmt (гасится
	// на строке if), statement/remove — через BlockStmt тела и гасится на
	// строке оператора; оба дают неразличимый дифф (noop тела), одного
	// гашения мало — второй генератор выживает.
	// mutator-disable-next-line branch/if
	if err != nil {
		// mutator-disable-next-line statement/remove
		slog.Debug("рендер: ответ на перехваченный запрос не прошёл",
			"type", ev.ResourceType.String(), "err", err.Error())
	}
}

// blockListen — production-обвязка насоса: подписка на requestPaused
// сессии вкладки (Target реализует cdp.Session).
func blockListen(ctx context.Context, sess cdp.Session) (stop func()) {
	return blockPump(ctx, func(lctx context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return cdp.Events(lctx, sess, fetch.RequestPaused)
	}, sess)
}

// blockPump качает события перехвата и отвечает на каждый; завершается
// при отмене ctx (конец задачи/страницы, stop) или закрытии канала
// событий (вкладка умерла). stop отменяет слушателя и ДОЖИДАЕТСЯ его
// выхода — горутина не переживает вызов (утечек нет, проверено -race).
//
// eventsOf — фабрика итератора в духе cdp.Events; вызывается синхронно с
// внутренним контекстом слушателя ДО запуска горутины: подписка
// фиксируется раньше Fetch.enable (startBlock), а stop, отменяя этот же
// контекст, детерминированно завершает и обход итератора. Тесты подают
// свою фабрику вместо реальной подписки.
func blockPump(ctx context.Context,
	eventsOf func(context.Context) iter.Seq2[fetch.EventRequestPaused, error],
	sess cdp.Session) (stop func()) {
	lctx, cancel := context.WithCancel(ctx)
	events := eventsOf(lctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev, err := range events {
			if err != nil {
				// Отмена контекста или неразобранное событие — обход
				// закончен (yield ошибки у итератора терминальный).
				return
			}
			blockRespond(lctx, sess, ev)
		}
		// Канал событий закрылся при живом контексте — подписка кончилась
		// неожиданно: выключить перехват, чтобы блокируемые запросы не
		// зависли до таймаута страницы (fail-open).
		if lctx.Err() == nil {
			blockDisable(lctx, sess)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// startBlock включает блокировку ресурсов в конвейере сессии (T-062);
// вызывается из Session.Run ДО перехода на страницу. Ошибка включения —
// ошибка конвейера (вкладка/браузер), Run классифицирует её обычным wrap.
// Возвращённый stop гасит слушателя и выключает перехват; сессия
// одноразовая, но прямой пользователь Session не наследует блокировку.
func (s *Session) startBlock(ctx context.Context) (stop func(), err error) {
	c := chromedp.FromContext(s.ctx)
	if c == nil || c.Target == nil {
		return nil, errors.New("контекст вкладки не инициализирован")
	}
	tgt := c.Target

	// Подписка строго до Fetch.enable (см. докомментарий файла).
	stopListen := blockListen(ctx, tgt)
	if err := blockEnable(ctx, tgt); err != nil {
		stopListen()
		return nil, fmt.Errorf("включение перехвата Fetch: %w", err)
	}
	return func() {
		stopListen()
		blockDisable(ctx, tgt)
	}, nil
}

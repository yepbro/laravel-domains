package render

import (
	"context"
	"fmt"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// Session — один изолированный контекст Chromium (CDP
// Target.createBrowserContext, chromedp.WithNewBrowserContext):
// инкогнито-профиль со своими cookies, кэшем и хранилищами плюс одна
// вкладка about:blank. Живёт ровно одну задачу: после страницы пул закрывает
// сессию и следующая задача получает новый профиль — состояние между
// страницами не сохраняется (ТЗ 5.6, §9; обоснование пересоздания вместо
// очистки — в докомментарии пакета).
type Session struct {
	b      *Browser
	ctx    context.Context    // chromedp-контекст вкладки (дочерний браузерному)
	cancel context.CancelFunc // закрывает вкладку и уничтожает профиль

	// probeFn подменяет зонд вкладки в юнит-тестах; nil — реальный зонд.
	probeFn func() bool
}

// NewSession создаёт изолированный контекст с вкладкой (инкогнито-профиль:
// собственные cookies/кэш/хранилища, изоляция от других сессий и от
// служебной вкладки браузера). Если задан Options.UserAgent, применяется к
// вкладке (ТЗ §9). После задачи сессию обязательно закрыть Close — профиль
// выкидывается целиком.
func (b *Browser) NewSession(ctx context.Context) (*Session, error) {
	if !b.Alive() {
		return nil, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
	}
	sctx, cancel := chromedp.NewContext(b.ctx, chromedp.WithNewBrowserContext())
	s := &Session{b: b, ctx: sctx, cancel: cancel}
	if err := s.start(ctx); err != nil {
		// Закрыть недоинициализированную вкладку (если создаётся) и
		// выбросить частично созданный профиль.
		cancel()
		return nil, err
	}
	return s, nil
}

// start инициализирует вкладку (createBrowserContext + about:blank) и
// применяет User-Agent. Первый вызов обязан идти на самом контексте вкладки
// s.ctx: в chromedp насос событий вкладки (Target.run) привязан к контексту
// первого вызова — короткоживущий потомок с таймаутом отменился бы сразу
// после инициализации и все дальнейшие команды вкладки зависли бы
// (воспроизводится на живом Chromium). Ожидание ограничено
// SessionStartTimeout обгонной гонкой: при превышении или отмене
// вызывающего контекста сессия целиком закрывается (NewSession отменяет
// вкладку), а не «долечивается».
func (s *Session) start(ctx context.Context) error {
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		err := chromedp.Do(s.ctx)
		if err == nil && s.b.opts.UserAgent != "" {
			err = chromedp.Do(s.ctx, chromedp.Func(func(c context.Context, t *chromedp.Target) error {
				return setUserAgent(c, t, s.b.opts.UserAgent)
			}))
		}
		done <- result{err}
	}()

	timer := time.NewTimer(s.b.opts.SessionStartTimeout)
	defer timer.Stop()
	select {
	case r := <-done:
		if r.err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return &Error{Class: ClassTask,
				Err: fmt.Errorf("задача отменена: %w: %w", ctx.Err(), r.err)}
		}
		// Классификация без зонда вкладки (сессия не поднята): умерший
		// браузер против не сложившегося контекста.
		return s.b.classifyCtx(r.err, "", nil)
	case <-timer.C:
		return &Error{Class: ClassContext,
			Err: fmt.Errorf("таймаут создания контекста %s", s.b.opts.SessionStartTimeout)}
	case <-ctx.Done():
		return &Error{Class: ClassTask,
			Err: fmt.Errorf("задача отменена: %w", ctx.Err())}
	}
}

// Run выполняет задачу: переход на task.URL (NavigateResponse — с ответом
// основной навигации, T-064), ожидание готовности страницы (T-063: событие
// загрузки с потолком + ограниченное «затишье» сети) и съём результата —
// итоговый URL и заголовок (T-061), итоговый DOM, все cookies итоговой
// страницы и заголовки ответа основной навигации (T-064, extract.go).
// Общий таймаут страницы — Options.Timeout (ТЗ 5.6: 25 с по умолчанию); он
// ограничивает действия задачи, а не жизнь профиля. Истечение общего
// таймаута — ошибка задачи (ClassTask), а не частичный результат: по ТЗ §8
// прерывание по лимиту фиксируется кодом ошибки, «частичного» DOM в схеме
// §6.1 нет — T-067 заменил бы полный сырой HTML недорисованным DOM без
// маркера; сырой HTML уже сохранён HTTP-этапом и остаётся результатом до
// успешного рендера (§7: рендер — асинхронное дополнение, неудача —
// повторная доставка уровнем очереди, T-066). Мягкие потолки фаз ожидания —
// не ошибка, после них результат снимается как есть (wait.go).
//
// Цепочка контекстов идёт от вкладки (s.ctx): chromedp ищет сессию по
// значению в контексте, производный от вызывающего контекст её не видит.
// Отмена и дедлайн вызывающего ctx привносятся AfterFunc: останавливают
// действия задачи, не трогая профиль.
//
// Блокировка изображений/шрифтов/медиа (T-062, ТЗ 5.6) включается здесь,
// до перехода (startBlock, перед waitSubscribe): перехват видит запросы
// страницы с первого. Этапы — chromedp.Run/Do над s.Context().
func (s *Session) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	tctx, stop := context.WithTimeout(s.ctx, s.b.opts.Timeout)
	defer stop()
	stopCaller := context.AfterFunc(ctx, stop)
	defer stopCaller()

	stopBlock, err := s.startBlock(tctx)
	if err != nil {
		return RenderOutput{}, s.wrap(err, ctx, tctx, task.URL)
	}
	defer stopBlock()

	var out RenderOutput
	// T-063: подписки на события вкладки — до перехода и до Network.enable:
	// подписка начинает буферизацию с момента вызова; событие загрузки,
	// случившееся за время NavigateResponse (своё ожидание load), waitLoop
	// возьмёт из буфера мгновенно и перейдёт к «затишью» сети.
	evs, err := waitSubscribe(tctx)
	var resp *network.Response
	if err == nil {
		// NavigateResponse — переход с ожиданием загрузки (то же ожидание,
		// что у Navigate) плюс ответ основной навигации: chromedp отслеживает
		// цепочку loader/request через редиректы и возвращает итоговый
		// документ (T-064 — заголовки для правил ТЗ 5.8). У навигаций без
		// сетевого запроса Chromium отдаёт псевдо-ответ (data: —
		// Content-Type из URL), возможен и nil — оба случая обрабатываются
		// ниже проверкой resp != nil.
		resp, err = chromedp.Run(tctx, chromedp.NavigateResponse(task.URL))
	}
	if err == nil {
		// T-063: событие загрузки (из буфера — мгновенно) и «затишье» сети
		// (окно без новых запросов) с потолками WaitPlan.
		_, err = waitLoop(tctx, DefaultWaitPlan(s.b.opts.Timeout), evs, waitRealTimerFactory)
	}
	if err == nil {
		out.Title, err = chromedp.Run(tctx, chromedp.Title())
	}
	if err == nil {
		out.FinalURL, err = chromedp.Run(tctx, chromedp.Location())
	}
	if err == nil {
		err = newExtractCDP().extractResult(tctx, &out) // T-064: DOM + cookies
	}
	if err == nil && resp != nil {
		out.ResponseHeaders = extractResponseHeaders(resp.Headers) // T-064
	}
	if err != nil {
		return out, s.wrap(err, ctx, tctx, task.URL)
	}
	return out, nil
}

// Close закрывает вкладку и уничтожает изолированный профиль
// (Target.disposeBrowserContext): cookies, кэш и хранилища страницы
// выбрасываются — состояние страниц не переживает (ТЗ §9). Ждёт фактического
// закрытия (гарантия chromedp), переиспользовать сессию после Close нельзя.
func (s *Session) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

// Context — chromedp-контекст вкладки сессии. Точка расширения T-062…T-064:
// этапы выполняются chromedp.Run/Do над этим контекстом.
func (s *Session) Context() context.Context { return s.ctx }

// wrap классифицирует ошибку этапа страницы: отменённый вызывающий контекст
// и истёкший таймаут страницы — ClassTask (повтор на новом контексте
// бессмысленен), остальное — classifyCtx по состоянию браузера/вкладки.
func (s *Session) wrap(err error, caller, opCtx context.Context, url string) *Error {
	switch {
	case caller.Err() != nil:
		return &Error{Class: ClassTask, URL: url,
			Err: fmt.Errorf("задача отменена: %w: %w", caller.Err(), err)}
	case opCtx.Err() != nil:
		return &Error{Class: ClassTask, URL: url,
			Err: fmt.Errorf("таймаут страницы %s: %w: %w", s.b.opts.Timeout, opCtx.Err(), err)}
	default:
		return s.b.classifyCtx(err, url, s)
	}
}

// probe — зонд вкладки для классификации ошибок: chromedp не отменяет
// контекст вкладки при её падении (тексты ошибок при этом неинформативны:
// «Session not found» и т. п.), поэтому неответчивость проверяется живым
// запросом с коротким таймаутом.
func (s *Session) probe() bool {
	if s.ctx == nil || s.ctx.Err() != nil {
		return false
	}
	if s.probeFn != nil {
		return s.probeFn()
	}
	ctx, stop := context.WithTimeout(s.ctx, probeTimeout)
	defer stop()
	_, err := chromedp.Run(ctx, chromedp.Evaluate[int]("1"))
	return err == nil
}

package render

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/remote"
)

// probeTimeout — таймаут живого зонда вкладки при классификации ошибок:
// короткий, чтобы решение «повторить на новом контексте» не тянуло за собой
// ожидание уровня таймаута страницы.
const probeTimeout = 2 * time.Second

// Browser — один Chromium воркера (ТЗ 5.6: «один браузер»): одно
// WebSocket-соединение (remote) или один дочерний процесс (local) и одна
// служебная вкладка about:blank, поверх которой открываются изолированные
// контексты задач (Session). Браузер переиспользуется всеми задачами;
// перезапуск по счётчику страниц/памяти — T-065.
type Browser struct {
	opts Options

	// ctx — chromedp-контекст браузерной сессии: owns соединение и
	// служебную вкладку. Отмена (Close или потеря соединения) гасит и все
	// контексты задач — они дочерние.
	ctx    context.Context
	cancel context.CancelFunc
	// cancelAlloc останавливает аллокатор: remote — закрывает соединение
	// после закрытия вкладок, local — ждёт выхода процесса.
	cancelAlloc context.CancelFunc

	pages atomic.Int64 // успешно отрендеренные страницы (T-065)

	mu     sync.Mutex
	closed bool
}

// StartBrowser подключается к внешнему Chromium (Options.RemoteURL — remote
// через chromedp/remote.NewAllocator) или запускает собственный процесс
// headless-shell (пустой RemoteURL — chromedp.NewExecAllocator, боевой
// render-образ) и инициализирует браузерную сессию. Профиль браузерной
// сессии — сам headless-shell без пользовательского каталога: он служебный,
// страницы задач живут в изолированных контекстах и его не касаются.
//
// ctx — контекст жизненного цикла: в chromedp браузер живёт, пока жив
// контекст первого вызова, поэтому отмена ctx останавливает браузер
// (включая инициализацию); deadline на ctx не накладывать.
//
// Ошибка подключения — *Error класса ClassBrowser: воркер может повторять
// старт (T-065/T-066), не разбирая тексты ошибок.
func StartBrowser(ctx context.Context, opts Options) (*Browser, error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	var allocCtx context.Context
	var cancelAlloc context.CancelFunc
	mode := opts.RemoteURL
	if mode != "" {
		// Dev-стек: внешний headless-shell, compose-сервис chromium
		// (ws://host:9222; аллокатор сам уточнит WebSocket-адрес браузера
		// запросом /json/version).
		allocCtx, cancelAlloc = remote.NewAllocator(ctx, opts.RemoteURL,
			remote.WithDialTimeout(opts.DialTimeout))
	} else {
		// Боевой режим: собственный процесс. --no-sandbox обязателен в
		// контейнере без привилегий (deploy/docker/Dockerfile.render,
		// USER nobody); остальное — головной набор chromedp: headless, без
		// фоновых сервисов браузера.
		mode = "локальный процесс"
		execOpts := append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.Flag("no-sandbox", true))
		if opts.ExecPath != "" {
			execOpts = append(execOpts, chromedp.ExecPath(opts.ExecPath))
		}
		allocCtx, cancelAlloc = chromedp.NewExecAllocator(ctx, execOpts...)
	}

	bctx, cancel := chromedp.NewContext(allocCtx)
	b := &Browser{opts: opts, ctx: bctx, cancel: cancel, cancelAlloc: cancelAlloc}

	// Инициализация соединения и служебной вкладки. Первый Run обязан идти
	// на самом bctx: в chromedp браузер живёт, пока жив контекст первого
	// вызова (правило из докомментария chromedp.Run) — дочерний контекст с
	// таймаутом отменился бы после инициализации и погасил браузер.
	// Длительность попытки ограничена самим chromedp: remote —
	// Options.DialTimeout (WebSocket) и уточнение адреса /json/version,
	// local — таймаут чтения адреса от процесса (20 с в chromedp);
	// внешняя граница — отмена ctx вызывающим.
	if err := chromedp.Do(bctx); err != nil {
		cancel()
		cancelAlloc()
		return nil, &Error{
			Class: ClassBrowser,
			Err:   fmt.Errorf("инициализация Chromium (%s): %w", mode, err),
		}
	}
	return b, nil
}

// Alive — жива ли браузерная сессия: контекст не отменён и соединение с
// Chromium не потеряно. При потере соединения chromedp закрывает
// LostConnection в обоих режимах (WebSocket-обрыв remote, смерть процесса
// local) и отменяет браузерный контекст.
func (b *Browser) Alive() bool {
	if b == nil || b.ctx == nil || b.ctx.Err() != nil {
		return false
	}
	if c := chromedp.FromContext(b.ctx); c != nil && c.Browser != nil {
		select {
		case <-c.Browser.LostConnection:
			return false
		default:
		}
	}
	return true
}

// Pages — число успешно отрендеренных страниц с запуска браузера: счётчик
// перезапуска «каждые N страниц» (ТЗ 5.6; порог Options.RestartEveryPages,
// сам перезапуск — T-065).
func (b *Browser) Pages() int64 { return b.pages.Load() }

// NeedsRestart — достигнут ли порог перезапуска по числу страниц (T-065).
func (b *Browser) NeedsRestart() bool {
	return b.Pages() >= int64(b.opts.RestartEveryPages)
}

// Close останавливает браузер: закрываются вкладки и изолированные
// контексты, затем соединение (remote) или процесс (local). Повторный вызов
// и вызов на неинициализированный браузер (юнит-тесты) безвредны.
func (b *Browser) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	if b.cancel != nil {
		b.cancel()
	}
	if b.cancelAlloc != nil {
		b.cancelAlloc()
	}
}

// classifyCtx классифицирует сбой операции над живым по контекстам браузером
// (отмену контекста задачи и таймаут страницы проверяет вызывающий —
// Session.wrapTask/sessionStart). Порядок без разбора текстов ошибок:
// смерть браузера — по состоянию соединения; смерть контекста вкладки — по
// его контексту; неответчивую вкладку (chromedp не отменляет контекст
// вкладки при её падении) — живым зондом.
func (b *Browser) classifyCtx(err error, url string, s *Session) *Error {
	switch {
	case !b.Alive():
		return &Error{Class: ClassBrowser, URL: url,
			Err: fmt.Errorf("браузер Chromium недоступен: %w", err)}
	case s != nil && s.ctx != nil && s.ctx.Err() != nil:
		return &Error{Class: ClassContext, URL: url,
			Err: fmt.Errorf("контекст вкладки отменён: %w", err)}
	case errors.Is(err, chromedp.ErrPageLoad):
		// Страница не загрузилась (net::ERR_*): вкладка жива, профиль цел.
		return &Error{Class: ClassTask, URL: url, Err: err}
	case s == nil || !s.probe():
		return &Error{Class: ClassContext, URL: url,
			Err: fmt.Errorf("контекст вкладки не отвечает: %w", err)}
	default:
		return &Error{Class: ClassTask, URL: url, Err: err}
	}
}

// setUserAgent применяет идентифицируемый UA сессии (ТЗ §9) поверх вкладки.
func setUserAgent(ctx context.Context, t *chromedp.Target, ua string) error {
	_, err := cdp.Call(ctx, t, emulation.SetUserAgentOverride,
		emulation.SetUserAgentOverrideParams{UserAgent: ua})
	return err
}

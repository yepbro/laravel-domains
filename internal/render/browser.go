package render

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
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

	// rootPID — PID главного процесса Chromium в локальном режиме
	// (0 — remote-режим или процесс не определён): корень дерева процессов
	// для контроля памяти (Browser.RSS, T-065). Неизменен после старта.
	rootPID int

	// lost — залипающая отметка «соединение мёртво» (T-218): её ставит
	// probeConn, когда живой зонд браузера не получил ответа — chromedp
	// в такой ситуации ничего не знает (чтение WebSocket не реагирует ни
	// на контекст, ни на таймауты). Полузакрытый сокет не воскресает,
	// поэтому отметка не снимается: поколение меняет Manager.
	lost atomic.Bool

	mu     sync.Mutex
	closed bool
}

// StartBrowser подключается к внешнему Chromium (Options.RemoteURL — remote
// через собственный аллокатор поверх chromedp/remote, remotealloc.go —
// детерминированное закрытие соединения, T-218) или запускает собственный
// процесс headless-shell (пустой RemoteURL — chromedp.NewExecAllocator,
// боевой render-образ) и инициализирует браузерную сессию. Профиль
// браузерной сессии — сам headless-shell без пользовательского каталога:
// он служебный, страницы задач живут в изолированных контекстах и его
// не касаются.
//
// ctx — контекст жизненного цикла: в chromedp браузер живёт, пока жив
// контекст первого вызова, поэтому отмена ctx останавливает браузер
// (включая инициализацию); deadline на ctx не накладывать.
//
// Ошибка подключения — *Error класса ClassBrowser: воркер может повторять
// старт (T-065/T-066), не разбирая тексты ошибок.
func StartBrowser(ctx context.Context, opts Options) (*Browser, error) {
	// Мутант ненаблюдаем в CI (T-M07): производные таймауты
	// (DialTimeout/SessionStartTimeout) видны только у успешно
	// стартовавшего браузера (b.opts сессий, таймаут remote-диала);
	// герметичные старты падают раньше — connection refused мгновенен,
	// локального бинарка нет. Живой Chromium — live-тесты за
	// CRAWLER_RENDER_TEST=1, в мутационный прогон не входят.
	// mutator-disable-next-line statement/remove
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	var allocCtx context.Context
	var cancelAlloc context.CancelFunc
	mode := opts.RemoteURL
	if mode != "" {
		// Внешний Chromium (dev-стек: compose-сервис chromium; боевой
		// systemd-юнит crawler-chromium, render.remote_url): собственный
		// аллокатор поверх chromedp/remote — соединение WebSocket с
		// детерминированным закрытием сокета при смене поколения
		// (remotealloc.go, T-218), адрес при необходимости уточняется
		// запросом /json/version.
		allocCtx, cancelAlloc = chromedp.NewAllocatorContext(ctx,
			newRemoteAllocator(opts))
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
	// таймаутом отменился бы после инициализации и погасил браузер,
	// поэтому потолок — обгонная гонка initBrowserSession, а не дедлайн
	// на самом bctx. Подключение ограничено Options.DialTimeout
	// (remoteDial, включая уточнение адреса /json/version); дальнейший
	// bootstrap (создание/подключение служебной вкладки, включение
	// доменов) самой chromedp не ограничен — на подвисшем, но принимающем
	// соединения Chromium он висит бесконечно, удерживая мьютекс
	// менеджера (restartLocked, T-218), — поэтому попытка гонится с
	// внешним потолком DialTimeout + SessionStartTimeout (масштаб того же
	// bootstrap'а у сессии). Локальный режим — чтение адреса процесса
	// (20 с в chromedp), потолок гонки покрывает и его; внешняя граница —
	// отмена ctx вызывающим.
	if err := initBrowserSession(bctx, opts); err != nil {
		// Отмены при неудачной инициализации — гигиена ресурсов chromedp
		// (горутины/соединения/процесс); герметичные отказы старта
		// (connection refused, fork/exec несуществующего бинарка) ничего
		// не успевают создать — эффект отмен ненаблюдаем без
		// полуподнятого браузера, который в CI не воспроизводится (T-M07).
		// mutator-disable-next-line statement/remove
		cancel()
		// mutator-disable-next-line statement/remove
		cancelAlloc()
		return nil, &Error{
			Class: ClassBrowser,
			Err:   fmt.Errorf("инициализация Chromium (%s): %w", mode, err),
		}
	}

	// Локальный режим: запомнить процесс Chromium — корень дерева для
	// контроля памяти (T-065). chromedp отдаёт его явно (Browser.Process
	// задокументирован как раз для сбора метрик процесса); в remote-режиме
	// процесса рядом нет — контроль памяти отключён (RSS = -1).
	//
	// Блок ниже исполняется только после успешной инициализации локального
	// Chromium (chromedp.Do выше), т. е. с живым процессом браузера — в CI
	// старт всегда падает, а live-тесты за CRAWLER_RENDER_TEST=1 в
	// мутационный прогон не входят (прецедент — NVMe, T-M01). Мутанты
	// блока гасятся: nil-половки проверки FromContext после успешного Do
	// (expression/remove), вырезание условия Process() и присвоения
	// rootPID из реального процесса (branch/if на строке if,
	// statement/remove на операторе); граница «не определён» 0 против
	// -1/1 (numbers) меняет лишь Warn — rootPID остаётся 0 и RSS как был
	// -1; сам Warn — логирование (§4 docs/mutation-testing.md).
	if opts.RemoteURL == "" {
		// mutator-disable-next-line expression/remove
		if c := chromedp.FromContext(bctx); c != nil && c.Browser != nil {
			// mutator-disable-next-line branch/if
			if p := c.Browser.Process(); p != nil {
				// mutator-disable-next-line statement/remove
				b.rootPID = p.Pid
			}
		}
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer, branch/if
		if b.rootPID == 0 {
			// mutator-disable-next-line statement/remove
			slog.Warn("процесс Chromium не определён: перезапуск по памяти отключён")
		}
	}
	return b, nil
}

// initBrowserSession — инициализация браузерной сессии (соединение +
// служебная вкладка) с внешним потолком: bootstrap chromedp не
// ограничен изнутри (см. StartBrowser), а старт идёт под мьютексом
// менеджера — обгонная гонка, как у Session.start. Превышение — ошибка
// (старт поколения повторится следующей задачей/зондом), отмены всего
// созданного делает вызывающий StartBrowser.
func initBrowserSession(bctx context.Context, opts Options) error {
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		done <- result{chromedp.Do(bctx)}
	}()
	limit := opts.DialTimeout + opts.SessionStartTimeout
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.err
	case <-timer.C:
		return fmt.Errorf("таймаут инициализации %s", limit)
	}
}

// Alive — жива ли браузерная сессия: контекст не отменён, соединение с
// Chromium не потеряно и зонд соединения не признал его мёртвым (lost,
// T-218). При потере соединения chromedp закрывает LostConnection в
// обоих режимах (WebSocket-обрыв remote, смерть процесса local) и
// отменяет браузерный контекст; полузакрытое соединение (без FIN/RST —
// NAT/conntrack, подвисший при остановке браузер) библиотека не видит
// вовсе — его ловит probeConn.
func (b *Browser) Alive() bool {
	if b == nil || b.ctx == nil || b.ctx.Err() != nil {
		return false
	}
	if b.lost.Load() {
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

// probeConn — зонд соединения с браузером: живая команда уровня браузера
// (Target.getTargets) с коротким таймаутом. Нужен помимо Alive: chromedp
// узнаёт о смерти соединения только от ошибок сокета, а чтение
// WebSocket (remote.Conn.Read) не реагирует ни на контекст, ни на
// таймауты — полузакрытое соединение выглядит живым бесконечно
// (инцидент 02.11: сессии висели в «таймауте создания контекста 15s» ×
// 11 520 за 6 ч, поколение не сменялось). Нет ответа — соединение
// мертво: браузер помечается lost (залипающе — полузакрытый сокет не
// воскресает), Alive() возвращает false и Manager меняет поколение.
//
// Одна попытка, и lost ставится на любую ошибку вызова — осознанно:
// подтверждающий повтор удвоил бы латентность детекции (инцидент
// требует восстановления за минуты), а цена ложного срабатывания — один
// реконнект к тому же Chromium; персистентная ошибка зонда без
// залипания не меняла бы поколение вовсе (Alive её не видит) — тише, но
// не дешевле.
func (b *Browser) probeConn() bool {
	if b == nil {
		return false
	}
	if b.lost.Load() {
		return false
	}
	if b.ctx == nil {
		// Без контекста chromedp (юнит-фейки пакета) — зонду нечего
		// проверять; Alive для таких браузеров уже вёл себя так же.
		return true
	}
	c := chromedp.FromContext(b.ctx)
	if c == nil || c.Browser == nil {
		return true
	}
	ctx, stop := context.WithTimeout(b.ctx, probeTimeout)
	defer stop()
	if _, err := cdp.Call(ctx, c.Browser, target.GetTargets, target.GetTargetsParams{}); err != nil {
		b.lost.Store(true)
		return false
	}
	return true
}

// Pages — число успешно отрендеренных страниц с запуска браузера: счётчик
// перезапуска «каждые N страниц» (ТЗ 5.6; порог Options.RestartEveryPages,
// сам перезапуск — T-065).
func (b *Browser) Pages() int64 { return b.pages.Load() }

// NeedsRestart — достигнут ли порог перезапуска по числу страниц (T-065):
// счётчик Pages обнуляется сменой поколения (Manager).
func (b *Browser) NeedsRestart() bool {
	return b.Pages() >= int64(b.opts.RestartEveryPages)
}

// RSS — суммарный RSS дерева процессов Chromium (главный процесс и все
// потомки — рендереры, GPU: в бюджет ТЗ 3.2 вкладывается каждый), байт;
// корень — PID процесса локального режима. -1 — неизвестно: remote-режим
// (браузер в чужом контейнере, там потолок держит mem_limit контейнера)
// или /proc недоступен.
func (b *Browser) RSS() int64 {
	// Эквивалентные мутанты (T-M07): `rootPID < 0` и `<= -1` при
	// rootPID == 0 ведут в procTreeRSS, чей собственный guard root <= 0
	// возвращает то же -1; вырезание `rootPID <= 0` (expression/remove)
	// эквивалентно по той же причине (nil-половина остаётся) и попутно
	// гасит убиваемого «b == nil → false» — trade-off строчных аннотаций
	// (§4 docs/mutation-testing.md, прецедент T-M02); nil-приёмник и
	// валидный pid 1 закреплены тестами (TestBrowserRSSUnknown).
	// mutator-disable-next-line expression/comparison, numbers/decrementer, expression/remove
	if b == nil || b.rootPID <= 0 {
		return -1
	}
	return procTreeRSS(b.rootPID)
}

// OverMemory — превышен ли порог памяти перезапуска
// Options.MemoryThreshold (ТЗ 5.6). Неизвестный RSS и нулевой порог
// (контроль отключён) не триггеруют: перезапуск — мягкая профилактика
// роста памяти, жёсткий потолок в любом режиме держит cgroup контейнера
// (mem_limit — docker-аналог MemoryMax, ТЗ 3.2/10.1).
func (b *Browser) OverMemory() bool {
	if b == nil || b.opts.MemoryThreshold <= 0 {
		return false
	}
	rss := b.RSS()
	// Эквивалентные мутанты (T-M07): при threshold >= 1 (guard выше) все
	// варианты левой половины (`>= 0`, `true`, `> -1`, `> 1`) неотличимы
	// от `rss > 0`: rss == 0 и rss == -1 отбрасываются правой половиной
	// (0/-1 >= threshold неверно), а rss == 1 недостижим — RSS с /proc
	// кратен странице (>= 4096). Аннотация expression/comparison попутно
	// гасит и границу `>= → >` справа (trade-off §4); сама граница «RSS
	// равен порогу — превышение» закреплена TestBrowserOverMemory.
	// mutator-disable-next-line expression/comparison, expression/remove, numbers/decrementer, numbers/incrementer
	return rss > 0 && rss >= b.opts.MemoryThreshold
}

// Close останавливает браузер: закрываются вкладки и изолированные
// контексты, затем соединение (remote) или процесс (local). Повторный вызов
// и вызов на неинициализированный браузер (юнит-тесты) безвредны — nil-guard
// здесь согласован с Alive/RSS/OverMemory: все методы Browser терпимы к
// нулевому значению.
func (b *Browser) Close() {
	if b == nil {
		return
	}
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
		// Неответчивая вкладка: отличить умершее соединение с браузером
		// (полузакрытый сокет — зонд probeConn, T-218) от умершей вкладки
		// при живом соединении: первое — класс browser (повтор на новом
		// контексте того же мёртвого сокета бессмысленен, поколение
		// сменит Manager), второе — класс context (существующий повтор).
		if !b.probeConn() {
			return &Error{Class: ClassBrowser, URL: url,
				Err: fmt.Errorf("браузер Chromium недоступен: %w", err)}
		}
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

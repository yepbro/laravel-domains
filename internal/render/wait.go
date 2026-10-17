package render

// Стратегия ожидания готовности страницы после перехода (ТЗ 5.6, T-063):
// событие загрузки с собственным потолком, затем ограниченное ожидание
// «затишья» сети. Комбинированный алгоритм Session.Run:
//
//	переход (Page.navigate, без ожидания) → событие загрузки (потолок
//	Load) → окно затишья Quiet без новых запросов (потолок Settle) → съём
//	результата
//
// Все ожидания — производные контекста вкладки (урок T-061: контекст
// действий обязан производиться от контекста вкладки, chromedp ищет
// сессию по значению в контексте).
//
// Потолки фаз Load и Settle — мягкие: истёк потолок → идём дальше и
// снимаем результат как есть («ограниченное ожидание» по ТЗ 5.6 — именно
// ограниченное, не бесконечное). Жёсткий предел — общий таймаут страницы
// render.timeout (обёртка всего визита в Session.Run): его истечение —
// ошибка задачи (см. докомментарий Session.Run), а не частичный результат.
//
// Значения по умолчанию выводятся из общего таймаута (DefaultWaitPlan);
// обоснования — у констант и полей WaitPlan.

import (
	"context"
	"sync"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
)

const (
	// WaitQuietWindow — окно «затишья» сети по умолчанию: страница считается
	// догруженной, когда столько времени не было НОВЫХ сетевых запросов
	// (Network.requestWillBeSent). 2 с — верх рекомендованного диапазона
	// (1.5–2 с): цепочка XHR SPA (запрос → ответ → порождённый запрос) на
	// медленном хосте укладывается в ~1 с, окно берётся с запасом; плата —
	// разовая добавка к каждой странице после последнего запроса, на фоне
	// общего таймаута 25 с и 2–4 параллельных контекстов пропускную
	// способность не ограничивает. Константа, а не доля бюджета: окно
	// характеризует темп запросов сайта, а не общий таймаут; вырожденные
	// малые таймауты сжимают окно DefaultWaitPlan'ом (не больше половины
	// потолка фазы), иначе окно длиннее потолка не подтверждается никогда.
	WaitQuietWindow = 2 * time.Second

	// WaitLoadShare — доля общего таймаута (1/N) на ожидание события
	// загрузки: половина, 12.5 с при 25 с. Типичная загрузка укладывается
	// в единицы секунд; если Page.loadEventFired не пришёл за половину
	// бюджета, страница зависла на подресурсе (load ждёт всех), и
	// дальнейшее ожидание малополезно: DOMContentLoaded обычно уже
	// случился, DOM пригоден к съёму, а остатка бюджета хватает на затишье
	// и съём результата.
	WaitLoadShare = 2

	// WaitSettleShare — доля общего таймаута (1/N) на фазу затишья: пятая
	// часть, 5 с при 25 с. Если за это время сеть ни разу не была тихой,
	// страница генерирует постоянный фон (опрос, аналитика, рекламные
	// биддеры) и окно не сойдётся — дальнейшее ожидание лишь съедает
	// бюджет, итог снимается как есть.
	WaitSettleShare = 5
)

// WaitPlan — потолки фаз ожидания готовности (ТЗ 5.6). Нулевые значения не
// поддерживаются: план строится DefaultWaitPlan из общего таймаута.
type WaitPlan struct {
	// Load — потолок ожидания события загрузки (Page.loadEventFired) после
	// перехода. У SPA load наступает до дорисовки (скрипты догружают
	// данные после) — фаза фиксирует базовую готовность документа, дорисовку
	// ждёт фаза затишья.
	Load time.Duration
	// Quiet — окно «затишья»: столько времени не должно быть новых сетевых
	// запросов. Каждый Network.requestWillBeSent запускает окно заново.
	Quiet time.Duration
	// Settle — потолок фазы затишья, считая от её старта (не от последнего
	// запроса): страховка от страниц с непрерывным фоновым трафиком.
	Settle time.Duration
}

// DefaultWaitPlan выводит план из общего таймаута страницы render.timeout:
// при 25 с — load 12.5 с, окно затишья 2 с, потолок затишья 5 с. Доли, а
// не константы: маленькие таймауты (тесты, тесные окружения) пропорционально
// сжимают фазы, сохраняя запас бюджета на съём результата
// (Load + Settle = 0.7 таймаута). Окно затишья — константа WaitQuietWindow,
// но не больше половины потолка фазы: окно длиннее потолка не подтвердилось
// бы ни разу — сжатые бюджеты получают пропорционально сжатое окно вместо
// неработающего.
func DefaultWaitPlan(total time.Duration) WaitPlan {
	return WaitPlan{
		Load:   total / WaitLoadShare,
		Quiet:  min(WaitQuietWindow, total/(WaitSettleShare*2)),
		Settle: total / WaitSettleShare,
	}
}

// WaitOutcome — чем закончилось ожидание (диагностика; оба исхода — успех).
type WaitOutcome struct {
	// LoadFired — событие загрузки пришло до потолка фазы. false — потолок
	// истёк (страница зависла на подресурсе), ожидание продолжено по затишью.
	LoadFired bool
	// Quiet — окно затишья выдержано. false — фаза оборвана потолком Settle
	// (непрерывный фоновый трафик); итог всё равно снимается как есть.
	Quiet bool
}

// waitReqsBuffer — буфер канала сетевых событий: насос-горутина не должна
// блокировать поток событий вкладки. Переполнение лишь слипает сигналы —
// для окна затишья это один и тот же сброс, потери смысла нет.
const waitReqsBuffer = 64

// waitEvents — события вкладки, приведённые к каналам для select в waitLoop.
type waitEvents struct {
	// load закрывается однократно по Page.loadEventFired.
	load chan struct{}
	// reqs сигнализируется по каждому Network.requestWillBeSent
	// (с возможным слипанием, см. waitReqsBuffer).
	reqs chan struct{}
}

// waitSubscribe подписывается на события вкладки (load, сетевые запросы) и
// включает домен Network. Вызывать ДО перехода: подписка chromedp.Events
// начинает буферизацию с момента вызова, поэтому события между подпиской и
// Page.navigate не теряются; Network.enable обязателен, чтобы приходили
// Network.requestWillBeSent.
//
// События приводятся к каналам насос-горутинами: типизированный итератор
// нельзя выбрать в select. Насосы заканчиваются вместе с ctx — подписки
// chromedp.Events привязаны к нему (endingSession), нечитаемая подписка не
// держит очередь. load закрывается однократно (sync.Once); в reqs —
// неблокирующая посылка: окно затишья отсчитывается от «любого нового
// запроса», слипшиеся сигналы дают тот же сброс окна.
func waitSubscribe(ctx context.Context) (waitEvents, error) {
	load := chromedp.Events(ctx, page.LoadEventFired)
	reqs := chromedp.Events(ctx, network.RequestWillBeSent)
	enable := chromedp.Func(func(ctx context.Context, t *chromedp.Target) error {
		_, err := cdp.Call(ctx, t, network.Enable, network.EnableParams{})
		return err
	})
	if err := chromedp.Do(ctx, enable); err != nil {
		return waitEvents{}, err
	}
	evs := waitEvents{
		load: make(chan struct{}),
		reqs: make(chan struct{}, waitReqsBuffer),
	}
	var once sync.Once
	go func() {
		for _, err := range load {
			if err != nil {
				return // ctx отменён или вкладка умерла
			}
			once.Do(func() { close(evs.load) })
		}
	}()
	go func() {
		for _, err := range reqs {
			if err != nil {
				return
			}
			select {
			case evs.reqs <- struct{}{}:
			default:
			}
		}
	}()
	return evs, nil
}

// waitNavigate стартует переход командой Page.navigate, НЕ ожидая события
// загрузки. Почему не chromedp.Navigate: он объединяет Page.navigate с
// ожиданием конца загрузки (chromedp v0.21, nav.go: Navigate =
// waitLoad(navigate)) без отдельного потолка — висящая страница съела бы
// весь бюджет ещё до фазы затишья. Событие загрузки и затишье ждёт waitLoop
// с потолками WaitPlan. Повторяет внутренний navigate chromedp, включая
// разбор ответа команды: errorText (net::ERR_*) → *chromedp.LoadError —
// classifyCtx распознаёт его через errors.Is(ErrPageLoad) как ошибку
// страницы, семантика T-061 сохранена.
func waitNavigate(ctx context.Context, url string) error {
	return chromedp.Do(ctx, chromedp.Func(func(ctx context.Context, t *chromedp.Target) error {
		res, err := cdp.Call(ctx, t, page.Navigate, page.NavigateParams{URL: url})
		if err != nil {
			return err
		}
		if res.ErrorText != "" {
			return &chromedp.LoadError{ErrorText: res.ErrorText}
		}
		return nil
	}))
}

// waitLoop — две фазы ожидания готовности:
//
//  1. событие загрузки (evs.load) с потолком plan.Load;
//  2. окно затишья plan.Quiet без новых запросов (evs.reqs сбрасывает окно)
//     с потолком plan.Settle.
//
// Оба потолка — мягкие (по истечении идём дальше); жёсткий предел — ctx
// (общий таймаут страницы): его отмена возвращает ошибку. Таймеры — через
// фабрику newTimer: реальный ход времени в юнит-тестах подменяется фейками
// (wait_test.go).
func waitLoop(ctx context.Context, plan WaitPlan, evs waitEvents, newTimer waitTimerFunc) (WaitOutcome, error) {
	var out WaitOutcome

	// Фаза 1: событие загрузки с потолком.
	loadCap := newTimer(plan.Load)
	select {
	case <-evs.load:
		out.LoadFired = true
		loadCap.Stop()
	case <-loadCap.C():
		// Потолок мягкий: load ждёт всех подресурсов, зависший подресурс
		// не должен съесть бюджет — DOMContentLoaded обычно уже случился,
		// продолжаем к затишью.
	case <-ctx.Done():
		return out, ctx.Err()
	}

	// Окно затишья начинается с входа в фазу: запросы, ушедшие во время
	// фазы загрузки, к новому окну не относятся — сливаются.
drain:
	for {
		select {
		case <-evs.reqs:
		default:
			break drain
		}
	}

	// Фаза 2: окно затишья с потолком.
	quiet := newTimer(plan.Quiet)
	settleCap := newTimer(plan.Settle)
	for {
		select {
		case <-evs.reqs:
			// Новый запрос — окно заново.
			quiet.Reset(plan.Quiet)
		case <-quiet.C():
			out.Quiet = true
			settleCap.Stop()
			return out, nil
		case <-settleCap.C():
			// Потолок мягкий: сеть не затихает — итог снимается как есть.
			quiet.Stop()
			return out, nil
		case <-ctx.Done():
			return out, ctx.Err()
		}
	}
}

// waitTimer — таймер фазы; абстракция ради фейковых часов в юнит-тестах.
type waitTimer interface {
	C() <-chan time.Time
	Reset(d time.Duration)
	Stop()
}

// waitTimerFunc — фабрика таймеров фаз.
type waitTimerFunc func(d time.Duration) waitTimer

// waitRealTimer — реальный таймер. Go ≥ 1.23: канал Timer небуферизован,
// Reset и Stop дренируют недоставленное значение сами — безопасны в
// select-цикле выше.
type waitRealTimer struct{ timer *time.Timer }

// waitRealTimerFactory — фабрика реальных таймеров для Session.Run.
func waitRealTimerFactory(d time.Duration) waitTimer {
	return &waitRealTimer{timer: time.NewTimer(d)}
}

func (t *waitRealTimer) C() <-chan time.Time   { return t.timer.C }
func (t *waitRealTimer) Reset(d time.Duration) { t.timer.Reset(d) }
func (t *waitRealTimer) Stop()                 { t.timer.Stop() }

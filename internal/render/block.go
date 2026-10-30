package render

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// Блокировка загрузки изображений, шрифтов и медиа в Chromium — ТЗ 5.6
// («экономия ресурсов»), задача T-062; опционально и CSS (T-213), а с
// T-214 — и запросов к доменам веб-аналитики.
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
// Fetch.enable получает паттерны по блокируемым типам ресурсов и — при
// включённой доменной блокировке трекеров (T-214) — по доменам
// аналитики (urlPattern без resourceType, ловит любой тип):
// приостанавливается лишь то, что слушатель собирается отклонить, —
// Fetch.failRequest (BlockedByClient → net::ERR_BLOCKED_BY_CLIENT) на
// стадии Request: запрос не покидает браузер, не тратится ни трафик,
// ни декодирование. Остальные запросы (Document, Script, XHR/Fetch,
// WebSocket, …, а без опции T-213 ниже — и Stylesheet) браузер не
// приостанавливает вовсе: они проходят сетевым стеком сами, и сбой или
// задержка слушателя не могут повесить страницу (fail-open — блокировка
// экономит ресурсы, но не обеспечивает корректность рендера).
// Скрипты и XHR обязаны грузиться: SPA-оболочки дорисовываются после
// загрузки — рендер и существует ради JS (ТЗ 5.6).
//
// Намеренное исключение — подстрока urlPattern доменных паттернов
// (T-214) шире точной хостовой проверки blockTrackerURL: запрос чужого
// хоста с доменом трекера в пути (/js/google-analytics.com.js)
// приостанавливается и отпускается слушателем continueRequest'ом.
// Окно паузы — один цикл слушателя, а его смерть страхует fail-open
// disable (blockPump): инвариант «неблокируемое не висит» держит не
// паттерн, а пара «широкий паттерн → точный вердикт».
//
// Stylesheet при включённой опции render.block_stylesheet (T-213)
// присоединяется к блокируемым тем же механизмом (blockTypes); дефолт
// false — риск computed styles, обоснование у опции.

// blockDisableTimeout — предел Fetch.disable при остановке перехвата:
// обычный вызов CDP занимает миллисекунды, дольше ждать незачем.
const blockDisableTimeout = 2 * time.Second

// blockBlockedTypes — базовый перечень блокируемых типов ресурсов
// (ТЗ 5.6: изображения, шрифты, медиа — T-062; 91% веса типичной
// страницы). Полный перечень network.ResourceType см. в тесте
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

// blockTypes — блокируемые типы ресурсов сессии: базовый перечень
// blockBlockedTypes плюс Stylesheet при включённой опции
// render.block_stylesheet (T-213). CSS — оставшиеся 5–10% байт веса
// страницы и заметная часть style/layout CPU (ресёрч 31.10: блок
// CSS+картинок — ещё −треть ко времени загрузки), но отсечён от
// базового перечня риском: у редких сайтов JS читает computed styles и
// media queries — на не-применённых стилях их логика (и итоговый DOM)
// могла бы отличаться, поэтому Stylesheet блокируется опцией с дефолтом
// false, а включение на проде — только после A/B (доля расхождений
// детекции <1%, темп не хуже). Список с опцией — копия: базовый
// перечень общий для всех сессий и мутировать его нельзя.
func blockTypes(blockStylesheet bool) []network.ResourceType {
	if !blockStylesheet {
		return blockBlockedTypes
	}
	return append(slices.Clone(blockBlockedTypes), network.ResourceTypeStylesheet)
}

// blockTrackerDomains — домены специализированной веб-аналитики и
// телеметрии для блокировки по URL (T-214), отдельно от блокировки по
// типам: аналитика приходит Script/XHR/Image/Ping-запросами — типом её
// не отрезать (SPA-скрипты сайта обязаны грузиться), а прямые потери от
// неё — 5–15 запросов и 100–500 КБ на страницу (замеры источников,
// ресёрч 31.10) плюс CPU таймеров трекера, дорисовывающих «затишье»
// сети (T-063).
//
// Принцип выбора: только домены, чьё единственное назначение —
// аналитика/телеметрия/рекламные биды. НЕ общий CDN и не
// инфраструктура сайта — с неё грузится и полезный код: yastatic.net,
// googleapis.com, jsdelivr и пр. общие статики; www.facebook.com —
// наравне с пикселем кнопки «поделиться» и редиректы (в списке только
// SDK-домен connect.facebook.net); segment.io/segment.com —
// customer-data платформа, для сайта это инфраструктура, cdn.segment.com
// раздаёт не только аналитику. Список обновляемый: региональные
// mc.yandex.* и legacy-хосты (mxpnl.com) добавляются по мере
// надобности; self-hosted Matomo/Sentry на домене самого сайта списком
// не ловятся принципиально — свой домен сайта блокировать нельзя.
var blockTrackerDomains = []string{
	"google-analytics.com",  // Google Analytics: gtag/js, /g/collect
	"googletagmanager.com",  // Google Tag Manager: контейнер gtm.js
	"doubleclick.net",       // реклама/ремаркетинг Google: fls/stat
	"googleadservices.com",  // конверсии Google Ads: pageadconversion
	"googlesyndication.com", // AdSense/DFP: pagead2, биды
	"connect.facebook.net",  // Facebook SDK + Pixel
	"hotjar.com",            // записи сессий: static/in/vars.hotjar.com
	"mc.yandex.ru",          // Яндекс Метрика: tag.js, /watch
	"mc.yandex.com",         // Яндекс Метрика (международная)
	"matomo.cloud",          // Matomo SaaS (self-hosted — домен сайта)
	"clarity.ms",            // Microsoft Clarity
	"mixpanel.com",          // Mixpanel
	"fullstory.com",         // FullStory
	"amplitude.com",         // Amplitude
	"sentry.io",             // Sentry-агент: *.ingest.sentry.io
}

// blockRules — правила блокировки одной сессии рендера: блокируемые
// типы (blockTypes — единая точка опций T-062/T-213) и включённость
// доменной блокировки трекеров (T-214). Единая точка вычисления —
// blockRulesOf: startBlock собирает правила из Options один раз, дальше
// один неизменяемый набор разъезжается по всем точкам решения — паттерны
// Fetch.enable (blockEnable) и вердикты blockRespond видят одну и ту же
// конфигурацию, расхождение списков исключено в принципе (стиль
// единой точки blockTypes у T-213).
type blockRules struct {
	types    []network.ResourceType
	trackers bool
}

// blockRulesOf — правила сессии из опций браузера.
func blockRulesOf(opts Options) blockRules {
	return blockRules{
		types:    blockTypes(opts.BlockStylesheet),
		trackers: opts.BlockTrackerDomains,
	}
}

// blockTrackerURL — точная проверка URL против blockTrackerDomains:
// хост равен домену списка или является его поддоменом. Точнее
// подстроки urlPattern Fetch.enable (см. blockPatterns): путь сайта
// вида /js/google-analytics.com.js подстроку содержит, но хост не
// трекер — блокировать нельзя; то же с userinfo
// (https://google-analytics.com@site.example — хост site.example) и
// чужим суффиксом (google-analytics.com.evil.example). Не разобранный
// url.Parse адрес трекером не считается (fail-open: блокировка не
// обязана ловить всё, но не имеет права резать чужое).
func blockTrackerURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, d := range blockTrackerDomains {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

// blockTracker — приостановленный запрос идёт к трекеру (T-214):
// доменная блокировка включена и хост URL — домен списка. Событие без
// сведений о запросе (аномалия протокола; на практике Request
// присутствует всегда) трекером не считается.
func (r blockRules) blockTracker(ev fetch.EventRequestPaused) bool {
	return r.trackers && ev.Request != nil && blockTrackerURL(ev.Request.URL)
}

// blockResourceType — блокируется ли тип ресурса. Неизвестные типы
// (появятся в будущих версиях протокола) проходят: консервативное
// поведение не ломает загрузку страницы.
func blockResourceType(types []network.ResourceType, t network.ResourceType) bool {
	for _, b := range types {
		if t == b {
			return true
		}
	}
	return false
}

// blockPatterns — паттерны Fetch.enable: по одному на блокируемый тип
// (стадия Request — отказ до отправки запроса) и, при включённой
// доменной блокировке трекеров (T-214), по одному на домен списка —
// без resourceType: аналитика ходит любыми типами (Script, XHR, Ping,
// Image), паттерн обязан ловить их все. urlPattern — glob с полным
// совпадением (Chromium: base::MatchPattern; '*' — любая
// последовательность, '?' — ровно один символ), поэтому домен
// обрамляется звёздками: "*domain*" равно подстроке URL; паттерн без
// звёздок совпал бы только с адресом, равным домену целиком. Подстрока
// шире хостовой проверки blockTrackerURL — нарочно: приостанавливается
// и путь /js/google-analytics.com.js, судьбу решает точная проверка в
// blockRespond (страховка от ложных срабатываний сидит в слушателе, а
// не в паттерне).
func blockPatterns(types []network.ResourceType, trackers bool) []*fetch.RequestPattern {
	patterns := make([]*fetch.RequestPattern, 0, len(types))
	for _, t := range types {
		patterns = append(patterns, &fetch.RequestPattern{
			ResourceType: t,
			RequestStage: fetch.RequestStageRequest,
		})
	}
	if trackers {
		for _, d := range blockTrackerDomains {
			patterns = append(patterns, &fetch.RequestPattern{
				URLPattern:   "*" + d + "*",
				RequestStage: fetch.RequestStageRequest,
			})
		}
	}
	return patterns
}

// blockEnable включает перехват на сессии CDP вкладки.
func blockEnable(ctx context.Context, sess cdp.Session, rules blockRules) error {
	_, err := cdp.Call(ctx, sess, fetch.Enable, fetch.EnableParams{
		Patterns: blockPatterns(rules.types, rules.trackers),
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

// blockRespond отвечает на приостановленный запрос: блокируемый тип или
// (T-214) трекер-домен — failRequest, любой другой — continueRequest
// (страховка: паттерны не должны приостанавливать чужие запросы, но
// подстрока urlPattern доменных паттернов шире хостовой проверки —
// расхождение классификации не должно вешать запрос). Ошибки ответа не
// фатальны: перехват — экономия, страница дорабатывает и без него
// (fail-open).
func blockRespond(ctx context.Context, sess cdp.Session, ev fetch.EventRequestPaused, rules blockRules) {
	var err error
	if blockResourceType(rules.types, ev.ResourceType) || rules.blockTracker(ev) {
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
func blockListen(ctx context.Context, sess cdp.Session, rules blockRules) (stop func()) {
	return blockPump(ctx, func(lctx context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return cdp.Events(lctx, sess, fetch.RequestPaused)
	}, sess, rules)
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
//
// rules — правила сессии (blockRules, T-213/T-214): судьбу каждого
// запроса решает blockRespond.
func blockPump(ctx context.Context,
	eventsOf func(context.Context) iter.Seq2[fetch.EventRequestPaused, error],
	sess cdp.Session, rules blockRules) (stop func()) {
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
			blockRespond(lctx, sess, ev, rules)
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

// startBlock включает блокировку ресурсов в конвейере сессии (T-062;
// с опцией Options.BlockStylesheet — и CSS, T-213; с
// Options.BlockTrackerDomains — и домены аналитики, T-214); вызывается
// из Session.Run ДО перехода на страницу. Ошибка включения —
// ошибка конвейера (вкладка/браузер), Run классифицирует её обычным wrap.
// Возвращённый stop гасит слушателя и выключает перехват; сессия
// одноразовая, но прямой пользователь Session не наследует блокировку.
func (s *Session) startBlock(ctx context.Context) (stop func(), err error) {
	c := chromedp.FromContext(s.ctx)
	if c == nil || c.Target == nil {
		return nil, errors.New("контекст вкладки не инициализирован")
	}
	tgt := c.Target
	rules := blockRulesOf(s.b.opts)

	// Подписка строго до Fetch.enable (см. докомментарий файла).
	stopListen := blockListen(ctx, tgt, rules)
	if err := blockEnable(ctx, tgt, rules); err != nil {
		stopListen()
		return nil, fmt.Errorf("включение перехвата Fetch: %w", err)
	}
	return func() {
		stopListen()
		blockDisable(ctx, tgt)
	}, nil
}

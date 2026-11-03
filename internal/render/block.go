package render

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"

	"crawler/internal/httpfetch"
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
//
// SSRF-контур (T-219, http.block_internal_addrs) — исключение из
// fail-open: паттерн-«всё» приостанавливает каждый запрос сессии
// (включая Document, Script, XHR/Fetch, WebSocket — навигация по
// HTTP/JS-редиректам и подресурсы идут сетевым стеком Chromium мимо
// воронки httpfetch), и запрос с внутренним хостом отклоняется
// (blockInternal, renderHostInternal). Цена — CDP-цикл на каждый
// запрос страницы, поэтому паттерн есть только при включённом флаге
// (Internet-профиль); дефолт false его не ставит. Контур проверяет
// IP-литералы и loopback-имена; с T-230 произвольные имена резолвятся
// в самом перехвате (dnsCache ниже): имя, публичное на HTTP-этапе и
// внутреннее к моменту рендера (смена адреса между этапами —
// DNS-rebinding во времени), больше не проходит. Остаточный вектор —
// атакующий, владеющий зоной, различает сами резолвы (перехвата и
// Chromium — два отдельных DNS-обмена) и отвечает им разными
// адресами; полное закрытие остаётся за egress-политикой контейнера
// Chromium (docs/render.md).

// blockDisableTimeout — предел Fetch.disable при остановке перехвата:
// обычный вызов CDP занимает миллисекунды, дольше ждать незачем.
const blockDisableTimeout = 2 * time.Second

// dnsCacheTTL — время жизни кэша резолвов SSRF-контура (T-230):
// повторные запросы того же хоста (подресурсы страницы) не множат
// DNS-обмены, но и не фиксируют вердикт дольше окна — TTL 30 с на
// порядок больше типичного обмена с локальным unbound и на порядок
// меньше эпизода перепроверки домена.
const dnsCacheTTL = 30 * time.Second

// dnsLookupTimeout — потолок ОДНОГО резолва контура (T-230, по ревью):
// обмен идёт в горутине насоса перехвата (пока он длится, приос-
// тановленные запросы страницы ждут ответа), поэтому зависший резолвер
// не должен выедать бюджет страницы без границы — обмен длиннее потолка
// обрезается, вердикт fail-open (см. hostInternal). 3 с — выше холодной
// рекурсии unbound за чужим доменом (типовой обмен — миллисекунды),
// ниже таймаута страницы (render.timeout 15–25 с).
const dnsLookupTimeout = 3 * time.Second

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
// типы (blockTypes — единая точка опций T-062/T-213), включённость
// доменной блокировки трекеров (T-214), SSRF-контур внутренних
// адресов (T-219) и кэш резолвов имён этого контура (T-230).
// Единая точка вычисления — blockRulesOf: startBlock собирает правила
// из Options один раз, дальше один неизменяемый набор разъезжается по
// всем точкам решения — паттерны Fetch.enable (blockEnable) и вердикты
// blockRespond видят одну и ту же конфигурацию, расхождение списков
// исключено в принципе (стиль единой точки blockTypes у T-213).
type blockRules struct {
	types    []network.ResourceType
	trackers bool
	internal bool
	// dns — кэш резолвов имён SSRF-контура (T-230); создаётся на сессию
	// только при включённом контуре. Нил (контур выключен / правила,
	// собранные без него) — резолв не вызывается вовсе.
	dns *dnsCache
}

// blockRulesOf — правила сессии из опций браузера.
func blockRulesOf(opts Options) blockRules {
	r := blockRules{
		types:    blockTypes(opts.BlockStylesheet),
		trackers: opts.BlockTrackerDomains,
		internal: opts.BlockInternalAddrs,
	}
	if opts.BlockInternalAddrs {
		r.dns = newDNSCache(opts.LookupHost)
	}
	return r
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
//
// С включённым SSRF-контуром (T-219) добавляется паттерн-«всё»
// (urlPattern "*", без типа): приостанавливается каждый запрос сессии,
// включая Document (навигации по HTTP/JS-редиректам) и Script/XHR/
// WebSocket (подресурсы), — внутренние хосты отклоняет blockInternal,
// остальные отпускаются continueRequest. Один лишний CDP-цикл на
// запрос — цена замыкания контура в самом Chromium; при выключенном
// флаге паттерна нет вовсе.
func blockPatterns(types []network.ResourceType, trackers, internal bool) []*fetch.RequestPattern {
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
	if internal {
		patterns = append(patterns, &fetch.RequestPattern{
			URLPattern:   "*",
			RequestStage: fetch.RequestStageRequest,
		})
	}
	return patterns
}

// blockEnable включает перехват на сессии CDP вкладки.
func blockEnable(ctx context.Context, sess cdp.Session, rules blockRules) error {
	_, err := cdp.Call(ctx, sess, fetch.Enable, fetch.EnableParams{
		Patterns: blockPatterns(rules.types, rules.trackers, rules.internal),
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

// blockInternal — приостановленный запрос идёт на внутренний адрес
// (SSRF-контур T-219/T-230, http.block_internal_addrs): фильтр включён
// и хост URL — внутренний. Литеральные формы (IP-литерал любого вида,
// включая IPv4-в-IPv6 и числоподобные WHATWG) и loopback-имена ловит
// синхронная проверка renderHostInternal — путь T-219; произвольное
// имя резолвится с кэшем на сессию (dnsCache, T-230): DNS-rebinding
// (имя публичное на HTTP-этапе, внутреннее — к моменту рендера)
// отклоняется в самом Chromium. URL в событии канонизирован браузером,
// но вход здесь — та же единая проверка, что у задач рендера:
// расхождения форм хоста исключены. Событие без сведений о запросе
// (аномалия протокола) внутренним не считается — как у трекеров.
func (r blockRules) blockInternal(ctx context.Context, ev fetch.EventRequestPaused) bool {
	return r.internal && ev.Request != nil && requestURLInternal(ctx, ev.Request.URL, r.dns)
}

// requestURLInternal — хост URL запроса внутренний; неразобранный URL —
// нет (fail-open, как у blockTrackerURL: блокировка не имеет права
// резать чужое, контур задачи держит validateTaskURLInternal).
// Литеральные и числоподобные формы — renderHostInternal без резолва;
// прочие имена — резолв через кэш (T-230): внешний литерал тоже не
// резолвится (адрес уже известен).
func requestURLInternal(ctx context.Context, rawURL string, dns *dnsCache) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if renderHostInternal(host) {
		return true
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false // внешний IP-литерал — адрес известен, резолв не нужен
	}
	if _, ok := whatwgParseIPv4(host); ok {
		return false // внешняя числоподобная форма WHATWG — то же
	}
	return dns.hostInternal(ctx, host)
}

// dnsCache — кэш резолвов имён SSRF-контура перехвата (T-230):
// положительные (внутренний адрес), отрицательные (внешний) и ошибочные
// вердикты живут dnsCacheTTL — подресурсы страницы ходят по одному хосту
// десятками запросов, без кэша каждый обменялся бы с DNS отдельно.
// Кэш принадлежит правилам одной сессии (blockRulesOf в startBlock):
// обращения идут только из горутины насоса перехвата этой сессии
// (blockPump → blockRespond — события сериализованы), мьютекс не нужен;
// сессия живёт одну задачу (≤ render.timeout), межстраничное
// переселение вердиктов исключено самим сроком жизни (карту записей не
// ограничиваем сознательно: её размер ограничен числом хостов одной
// страницы — сотни записей по десяткам байт; долгоживущий кэш
// httpfetch.CachingResolver с MaxEntries — другой масштаб, сессия
// умирает вместе с задачей).
//
// Fail-семантика контура разделена (T-230): ошибка резолва — fail-open
// (запрос проходит: NXDOMAIN рекламных и битых подресурсов — норма
// страницы, резолв-ошибка ≠ внутренний адрес; fail-closed здесь резал
// бы легитимные страницы пачками), найденный внутренний адрес —
// fail-closed (failRequest): это ровно то, ради чего контур существует.
// Обмен ограничен собственным потолком timeout (dnsLookupTimeout):
// зависший резолвер обрезается, а не выедает бюджет страницы в горутине
// насоса; обрыв по дедлайну/отмене НЕ кэшируется (обрыв — не ответ DNS,
// следующий запрос того же хоста повторит обмен), детерминированные
// ошибки (NXDOMAIN) — кэшируются как «не внутренний» на общих TTL.
type dnsCache struct {
	// lookup — шов резолва (Options.LookupHost; nil — системный
	// net.DefaultResolver: headless-shell резолвит системным же,
	// перехват видит то же представление об адресах, что браузер).
	lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	// now — шов времени для тестов TTL; nil — time.Now.
	now func() time.Time
	// timeout — потолок одного обмена (dnsLookupTimeout; шов тестов).
	timeout time.Duration
	// entries — вердикты по хостам; ключ — канонический хост (нижний
	// регистр без хвостовой точки, нормализует requestURLInternal).
	entries map[string]dnsCacheEntry
}

// dnsCacheEntry — закэшированный вердикт резолва.
type dnsCacheEntry struct {
	internal bool
	at       time.Time
}

// newDNSCache — кэш на сессию с швом резолва.
func newDNSCache(lookup func(context.Context, string) ([]netip.Addr, error)) *dnsCache {
	if lookup == nil {
		lookup = lookupHostSystem
	}
	return &dnsCache{lookup: lookup, now: time.Now, timeout: dnsLookupTimeout}
}

// lookupHostSystem — системный резолвер (дефолт шва Options.LookupHost):
// тот же /etc/resolv.conf, которым резолвит сам Chromium, — вердикт
// контура не расходится с адресами, по которым реально идёт загрузка.
func lookupHostSystem(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

// hostInternal — имя резолвится во внутренний адрес (хотя бы один из
// ответа: список отдаёт атакующий, fail-closed по найденному). Ошибка
// резолва и пустой ответ — не внутренний (fail-open, см. докомментарий
// dnsCache); детерминированные исходы (ответ, пустой ответ, NXDOMAIN)
// кэшируются на dnsCacheTTL, обрыв по дедлайну/отмене — нет.
// Нил-кэш (правила без контура — страховка вызывающих) — не внутренний.
func (c *dnsCache) hostInternal(ctx context.Context, host string) bool {
	if c == nil {
		return false
	}
	if c.now == nil {
		c.now = time.Now
	}
	if e, ok := c.entries[host]; ok && c.now().Sub(e.at) < dnsCacheTTL {
		return e.internal
	}
	internal := false
	definitive := true
	if addrs, err := c.exchange(ctx, host); err == nil {
		for _, a := range addrs {
			if httpfetch.AddrIsInternal(a) {
				internal = true
				break
			}
		}
	} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// Обрыв обмена (потолок timeout или конец страницы) — не ответ
		// DNS: вердикт fail-open, но не кэшируем — повтор запроса того
		// же хоста обменяется заново.
		definitive = false
	} // прочие ошибки (NXDOMAIN, SERVFAIL-ответ) — детерминированный
	// «не внутренний» с кэшированием на общих основаниях.
	// Логирование (§4 docs/mutation-testing.md): Debug-запись не влияет
	// на судьбу запроса (вердикт уже вычислен), мутанты ветки и
	// вырезания оператора меняют только текст отладочного лога (образец
	// — лог blockRespond ниже); аннотации две — branch/if гасится на
	// строке if, statement/remove — на строке вызова, оба мутанта
	// дают неразличимый дифф (noop тела).
	// mutator-disable-next-line branch/if
	if internal {
		// mutator-disable-next-line statement/remove
		slog.Debug("рендер: имя резолвится во внутренний адрес — запрос отклоняется (SSRF)",
			"host", host)
	}
	if definitive {
		if c.entries == nil {
			c.entries = make(map[string]dnsCacheEntry)
		}
		c.entries[host] = dnsCacheEntry{internal: internal, at: c.now()}
	}
	return internal
}

// exchange — один обмен с потолком timeout: зависший резолвер не
// выедает бюджет страницы в горутине насоса (потолок ниже
// render.timeout, стоп-вызов stop дожидается обмена не дольше потолка).
func (c *dnsCache) exchange(ctx context.Context, host string) ([]netip.Addr, error) {
	if c.timeout <= 0 {
		return c.lookup(ctx, host)
	}
	rctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.lookup(rctx, host)
}

// blockRespond отвечает на приостановленный запрос: внутренний адрес
// (T-219 — литералы, T-230 — имена по резолву), блокируемый тип или
// (T-214) трекер-домен — failRequest, любой другой — continueRequest
// (страховка: паттерны не должны приостанавливать чужие запросы, но
// подстрока urlPattern доменных паттернов и паттерн-«всё» SSRF-контура
// шире точных проверок — расхождение классификации не должно вешать
// запрос). Порядок условий — сначала дешёвые вердикты без сети (тип,
// трекер), контур с резолвом последним: запрос, который всё равно
// отклоняется по типу/домену, не должен тратить DNS-обмен (по ревью
// T-230). Ошибки ответа не фатальны: перехват — экономия, страница
// дорабатывает и без него (fail-open).
func blockRespond(ctx context.Context, sess cdp.Session, ev fetch.EventRequestPaused, rules blockRules) {
	var err error
	if blockResourceType(rules.types, ev.ResourceType) || rules.blockTracker(ev) || rules.blockInternal(ctx, ev) {
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

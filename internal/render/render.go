// Package render — управление Chromium для рендера страниц (ТЗ 5.6, §9).
//
// Модель пакета:
//
//   - Browser — один Chromium на воркер (ТЗ 5.6: «один браузер»). Два
//     режима: внешнее подключение по CDP (Options.RemoteURL —
//     render.remote_url, контейнер chromium dev-стека) или собственный
//     процесс headless-shell (боевой render-образ,
//     deploy/docker/Dockerfile.render). В обоих режимах — одно соединение
//     с браузером (WebSocket) и одна служебная вкладка about:blank.
//   - Session — изолированный контекст Chromium (CDP
//     Target.createBrowserContext, опция chromedp.WithNewBrowserContext):
//     инкогнито-профиль со своими cookies, кэшем и хранилищами плюс одна
//     вкладка. Сессия живёт ровно одну задачу: после страницы пул её
//     закрывает (Target.disposeBrowserContext), следующая задача получает
//     новый профиль — состояние между страницами не сохраняется (ТЗ 5.6,
//     §9). Пересоздание контекста вместо выборочной очистки: dispose
//     выкидывает все хранилища профиля атомарно и без перечисления
//     origin'ов (иначе пришлось бы чистить cookies/Storage по каждому
//     встреченному origin, и кэш это не покрывает), а стоимость — один
//     CreateBrowserContext + CreateTarget, на фоне таймаута страницы в
//     секунды это незаметно.
//   - Pool — до Options.Contexts (рекомендация ТЗ: 2–4) задач параллельно,
//     каждая в своём изолированном контексте.
//
// Ошибки классифицируются (Class): упавший контекст — повтор на новом,
// недоступный браузер — наверх, ошибка страницы — обычный результат этапа
// (повторная доставка — уровень очереди, T-066).
//
// Развитие T-062…T-067 поверх текущего API: блокировка ресурсов (T-062) и
// ожидание «затишья» сети (T-063) встраиваются этапами в Session.Run (до
// перехода и до съёма результата соответственно); итоговый DOM, cookies и
// заголовки ответа (T-064) — поля RenderOutput (extract.go); Manager (T-065)
// владеет поколениями «браузер + пул» — перезапуск каждые N страниц
// (счётчик Browser.Pages) и по порогу RSS; цикл очередей — T-066 (публикация
// q:render) и T-067 (потребитель — internal/renderworker, Manager.Run).
package render

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/httpfetch"
)

// RenderTask — одна задача рендера: открыть страницу и снять результат.
//
// URL приходит из результата HTTP-этапа (сообщение q:render, T-066); схема
// контролируется перед переходом (validateTaskURL): рендер открывает только
// http/https, прочие адреса (file:, data:, ftp:) отвергаются классом task —
// браузер открыл бы и локальный файл (R-09). Формальный контроль входа —
// уровень воркера, остальное отвергает сам браузер (chromedp.ErrPageLoad).
type RenderTask struct {
	URL string
}

// RenderOutput — результат задачи: итоговый URL и заголовок (T-061),
// итоговый DOM, все cookies итоговой страницы и заголовки ответа основной
// навигации (T-064; extract.go — обоснование способов съёма).
type RenderOutput struct {
	// FinalURL — document.location после загрузки страницы: включает
	// JS-редиректы, которых HTTP-этап не видит (T-048 отслеживает только
	// HTTP-переходы и meta refresh).
	FinalURL string
	// Title — содержимое document.title (диагностика и live-проверки).
	Title string
	// DOM — итоговый DOM документа (document.documentElement.outerHTML,
	// ТЗ 5.6): полное дерево после дорисовки JS; формат HTML — замена
	// сырого HTML в хранилище (T-067) и вход правил по HTML/meta (T-068).
	DOM string
	// Cookies — все cookies итоговой страницы, включая httpOnly и
	// выставленные JS (ТЗ 5.6, §11.4 п. 3): CDP читает хранилище
	// изолированного контекста, document.cookie их не видит. Формат —
	// единый контракт subdomain_results.cookies (T-058). Для страниц без
	// cookie-хранилища — nil.
	Cookies []ExtractCookie
	// ResponseHeaders — заголовки ответа основной навигации (ТЗ 5.8 —
	// правила по заголовкам): http.Header, как у HTTP-этапа (T-058).
	// Задачи — только http/https (validateTaskURL, R-09): у навигации
	// есть реальный сетевой ответ; nil (ответ не получен) обрабатывается
	// защитно в Session.Run.
	ResponseHeaders http.Header
}

// Options — параметры Browser и Pool. Собираются из конфигурации NewOptions;
// производные таймауты (DialTimeout, SessionStartTimeout) — внутренние
// режимы работы, в ТЗ 5.10 не параметризуются.
type Options struct {
	// RemoteURL — адрес внешнего Chromium по CDP (ws://host:9222):
	// dev-стек, compose-сервис chromium. Пусто — воркер запускает
	// собственный процесс headless-shell (боевой render-образ).
	RemoteURL string
	// Contexts — максимальное число параллельных изолированных контекстов
	// (ТЗ 5.6: «по умолчанию 2–4 параллельно»).
	Contexts int
	// Timeout — общий таймаут одной страницы (ТЗ 5.6: по умолчанию 25 с).
	Timeout time.Duration
	// RestartEveryPages — порог перезапуска браузера по числу страниц
	// (ТЗ 5.6): Browser ведёт счётчик Pages, смена поколения — Manager
	// (T-065).
	RestartEveryPages int
	// MemoryThreshold — порог перезапуска браузера по памяти, байт:
	// суммарный RSS дерева процессов Chromium ≥ порога → перезапуск
	// (ТЗ 5.6). Бюджет Chromium ≈ 2 ГБ (ТЗ 3.2, cgroup MemoryMax), порог
	// обязан быть ниже лимита контейнера: перезапуск — профилактика до
	// срабатывания cgroup-OOM. 0 — контроль отключён; в remote-режиме
	// процесс Chromium рядом нет (RSS неизвестен) и контроль не работает —
	// потолок там держит mem_limit контейнера Chromium.
	MemoryThreshold int64
	// UserAgent — идентифицируемый User-Agent страниц рендера (ТЗ §9):
	// подменяет UA браузера в сессиях (Emulation.setUserAgentOverride).
	// В секции render ТЗ 5.10 отдельного поля нет, краулер представлен
	// одним UA — http.user_agent. Пусто — UA браузера без изменений.
	UserAgent string
	// ExecPath — путь к Chromium в локальном режиме; пусто — стандартный
	// поиск chromedp (в списке есть headless-shell по имени из PATH).
	ExecPath string
	// DialTimeout — таймаут WebSocket-подключения к внешнему Chromium
	// (0 — 10 с, как в chromedp). Ограничивает и уточнение адреса
	// /json/version; сами bootstrap-команды сессии (вкладка, домены)
	// ограничивает initBrowserSession суммой DialTimeout +
	// SessionStartTimeout — потолок действует в обоих режимах, включая
	// холодный старт локального процесса (T-218). Контекст StartBrowser
	// при этом обязан жить столько, сколько браузер (жизненный цикл
	// воркера) — отмена извне гасит и инициализацию; потолок — обгонная
	// гонка, а не дедлайн на контексте.
	DialTimeout time.Duration
	// SessionStartTimeout — таймаут создания изолированного контекста с
	// вкладкой (0 — 15 с).
	SessionStartTimeout time.Duration
	// MaxDOMBytes — потолок итогового DOM в байтах (R-02, гигиена памяти
	// воркера: render.contexts съёмов параллельно при MemoryMax=128M у
	// systemd-профиля): DOM сверх лимита обрезается в самом браузере и
	// получает признак обрезки (extract.go). 0 — без лимита. Источник в
	// сборке — content.max_store_bytes: контент сверх лимита хранилище не
	// принимает (ErrTooLarge), держать его в памяти бессмысленно.
	MaxDOMBytes int64
	// BlockStylesheet — блокировать ли CSS в рендер-контексте (T-213):
	// при true перечень блокируемых типов (block.go, T-062:
	// Image/Media/Font) пополняется Stylesheet. Дефолт false — риск
	// редких сайтов, чей JS читает computed styles/media queries:
	// включение на проде — только после A/B (доля расхождений детекции
	// <1%, темп не хуже). Источник — render.block_stylesheet.
	BlockStylesheet bool
	// BlockTrackerDomains — блокировать ли в рендер-контексте запросы к
	// доменам веб-аналитики/телеметрии (T-214): Fetch.enable получает
	// urlPattern-паттерны по доменам списка blockTrackerDomains
	// (block.go), вердикт по каждому приостановленному запросу — точная
	// хостовая проверка (свои скрипты сайта и пути с похожей подстрокой
	// не задеты). Дефолт true — трекеры не участвуют в детекции,
	// экономия трафика и CPU прямая; остаточный риск и обоснование
	// дефолта — у поля BlockTrackerDomains internal/config. Источник —
	// render.block_tracker_domains.
	BlockTrackerDomains bool
	// BlockInternalAddrs — SSRF-контур рендера (T-219): тот же флаг
	// http.block_internal_addrs, что у HTTP-этапа. При true задача с
	// IP-литеральным хостом внутреннего диапазона
	// (httpfetch.AddrIsInternal, включая IPv4-в-IPv6 —
	// [::ffff:10.0.0.1]) отвергается классом task до перехода —
	// Chromium не открывает внутренние адреса: final_url приходит из
	// БД, и аномалия пайплайна или компрометация БД не должна
	// превращаться в обход контура. Имена-хосты задачи отдельно не
	// резолвятся: их адреса фильтруются HTTP-этапом под той же воронкой
	// httpfetch, второй резолвер в валидации задачи расширял бы контур;
	// произвольные имена навигаций и подресурсов внутри страницы
	// резолвирует перехват запросов Chromium (T-230, block.go —
	// DNS-rebinding). false — прежнее поведение: dev/testbed рендерит
	// внутренние стенды.
	BlockInternalAddrs bool
	// LookupHost — шов резолва имён-хостов для SSRF-контура перехвата
	// рендера (T-230): при включённом BlockInternalAddrs перехват Fetch
	// резолвит произвольные имена (литеральные и числоподобные формы
	// ловит renderHostInternal без резолва) и отклоняет запросы, чей
	// хост резолвится в httpfetch.AddrIsInternal, — DNS-rebinding во
	// времени (имя публичное на HTTP-этапе, внутреннее — к моменту
	// рендера) закрывается в самом Chromium. Остаточный вектор —
	// атакующий с контролем зоны различает резолв перехвата и свой
	// резолв Chromium (два отдельных DNS-обмена) и отвечает им разными
	// адресами; полное закрытие — egress-политика контейнера Chromium
	// (docs/render.md). nil — системный резолвер (net.DefaultResolver):
	// headless-shell резолвит системным же, перехват видит то же
	// представление об адресах, что и браузер. Резолвы кэшируются на
	// сессию с TTL и ограничены потолком на обмен (block.go, dnsCache).
	// Сборки из конфигурации поле не заполняют (работает дефолт),
	// подменяют тесты.
	LookupHost func(ctx context.Context, host string) ([]netip.Addr, error)
}

// NewOptions собирает Options из конфигурации сервиса (ТЗ 5.10): секция
// render плюс User-Agent HTTP-этапа (ТЗ §9 — единый идентифицируемый UA
// краулера), потолок DOM из секции content (R-02: итоговый DOM не больше
// того, что хранилище контента принимает) и флаг SSRF-контура
// http.block_internal_addrs (T-219 — общий с HTTP-этапом).
func NewOptions(cfg config.Config) Options {
	return Options{
		RemoteURL:           cfg.Render.RemoteURL,
		Contexts:            cfg.Render.Contexts,
		Timeout:             time.Duration(cfg.Render.Timeout),
		RestartEveryPages:   cfg.Render.RestartEveryPages,
		MemoryThreshold:     cfg.Render.RestartMemoryBytes.B(),
		UserAgent:           cfg.HTTP.UserAgent,
		MaxDOMBytes:         cfg.Content.MaxStoreBytes.B(),
		BlockStylesheet:     cfg.Render.BlockStylesheet,
		BlockTrackerDomains: cfg.Render.BlockTrackerDomains,
		BlockInternalAddrs:  cfg.HTTP.BlockInternalAddrs,
	}
}

// withDefaults заполняет производные таймауты. Contexts не дефолтится
// сознательно: 0 — ошибка конфигурации, а не «как-нибудь».
func (o Options) withDefaults() Options {
	c := o
	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.SessionStartTimeout == 0 {
		c.SessionStartTimeout = 15 * time.Second
	}
	return c
}

// validate проверяет значения до передачи в chromedp: невалидные параметры
// отклоняются здесь, а не паниками глубоко в библиотеке. Диапазоны
// повторяют секцию render internal/config (ТЗ 5.10).
func (o Options) validate() error {
	var problems []string
	if o.RemoteURL != "" {
		u, err := url.Parse(o.RemoteURL)
		if err != nil || u.Host == "" ||
			(u.Scheme != "ws" && u.Scheme != "wss" && u.Scheme != "http" && u.Scheme != "https") {
			problems = append(problems,
				fmt.Sprintf("remote_url: ожидается ws://host:9222, задано %q", o.RemoteURL))
		}
	}
	if o.Contexts < 1 || o.Contexts > 16 {
		problems = append(problems,
			fmt.Sprintf("contexts: ожидается 1..16 (рекомендация ТЗ 5.6: 2–4), задано %d", o.Contexts))
	}
	if o.Timeout <= 0 {
		problems = append(problems, fmt.Sprintf("timeout: ожидается положительная длительность, задано %s", o.Timeout))
	}
	if o.RestartEveryPages < 1 {
		problems = append(problems,
			fmt.Sprintf("restart_every_pages: ожидается >= 1, задано %d", o.RestartEveryPages))
	}
	// Диапазон порога памяти повторяет render.restart_memory_bytes
	// internal/config: 0 — контроль отключён, ниже 256 MiB — ниже холостого
	// RSS headless-shell с контекстами (краш-луп перезапусков), выше
	// 16 GiB — за пределами разумного бюджета одного воркера (ТЗ 3.2).
	// Эквивалентные мутанты (T-M08): `< 0` — страховка, отрицательный
	// порог ловится и диапазонной половиной ниже (!= 0 и < 256<<20),
	// тем же сообщением; `< -1` отличается от `< 0` только на значении
	// -1 — и там тем же путём. Гранулярность строки гасит и убиваемое
	// вырезание всего правого дизъюнкта (trade-off §4
	// docs/mutation-testing.md) — диапазонная половина закреплена
	// TestOptionsValidate (границы 255<<20/256<<20/16<<30/17<<30 и
	// точный текст ошибки). Инкрементер (`< 1`) не гасим: границу «0 —
	// контроль отключён» держит тот же тест.
	// mutator-disable-next-line expression/remove, numbers/decrementer
	if o.MemoryThreshold < 0 ||
		(o.MemoryThreshold != 0 && (o.MemoryThreshold < 256<<20 || o.MemoryThreshold > 16<<30)) {
		problems = append(problems, fmt.Sprintf(
			"memory_threshold: ожидается 0 (отключено) или %d..%d байт, задано %d",
			256<<20, 16<<30, o.MemoryThreshold))
	}
	if o.MaxDOMBytes < 0 {
		problems = append(problems,
			fmt.Sprintf("max_dom_bytes: ожидается 0 (без лимита) или положительное число, задано %d", o.MaxDOMBytes))
	}
	if len(problems) > 0 {
		return errors.New("render: " + strings.Join(problems, "; "))
	}
	return nil
}

// validateTaskURL — контроль схемы задачи рендера: открываются только
// http/https. URL приходит из subdomain_results.final_url — в штатном
// потоке это адрес из HTTP-редиректов, но при аномалии пайплайна или
// компрометации БД он может нести file:///data: — браузер открыл бы
// локальный файл, а съём DOM унёс бы его содержимое в хранилище контента
// (R-09). Отказ — класс task: страница не рендерится, повторная доставка
// уровня очереди (T-066).
func validateTaskURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return &Error{Class: ClassTask, URL: raw,
			Err: fmt.Errorf("рендер открывает только http/https, задано %q", raw)}
	}
	return nil
}

// validateTaskURLInternal — контроль адреса задачи рендера при
// включённом SSRF-контуре (http.block_internal_addrs, T-219): хост
// внутреннего диапазона (renderHostInternal — IP-литералы, включая
// IPv4-в-IPv6 и числоподобные формы WHATWG, loopback-имена) не
// открывается, класс task — повторная доставка уровня очереди (T-066),
// как у проверки схемы. Имя-хост проходит без резолва: его адреса уже
// отфильтрованы HTTP-этапом под той же воронкой httpfetch, а резолв в
// валидации задачи означал бы второй резолвер в контуре и гонку адресов с
// DNS-этапом. Той же проверкой плюс резолвом имён (T-230, dnsCache)
// живёт перехват запросов Chromium (blockInternal): навигации по
// HTTP/JS-редиректам и подресурсы — включая произвольные имена,
// резолвящиеся во внутренние адреса уже после HTTP-этапа
// (DNS-rebinding), — блокируются в самом браузере.
func validateTaskURLInternal(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil // схема/хост уже отвергнуты validateTaskURL
	}
	if renderHostInternal(u.Hostname()) {
		return &Error{Class: ClassTask, URL: raw,
			Err: fmt.Errorf("рендер внутренних адресов запрещён (SSRF, http.block_internal_addrs): %q", raw)}
	}
	return nil
}

// renderHostInternal — хост внутренний для SSRF-контура рендера
// (T-219): loopback-имена Chromium (localhost и поддомены — браузер
// резолвит их в петлю сам), IP-литералы внутренних диапазонов
// (httpfetch.AddrIsInternal — единое определение контура, включая
// IPv4-в-IPv6) и «числоподобные» хосты, которые Chromium канонизирует
// в IPv4 по алгоритму WHATWG («127.1», «2130706433», «0x7f.1» — все
// 127.0.0.1; netip такие формы не разбирает, а браузер открывал бы).
// Прочие имена проходят без резолва (их адреса на этапе задачи
// фильтрует HTTP-этап, если имя пришло через конвейер); сами имена в
// навигациях и подресурсах страницы резолвит и проверяет перехват
// запросов (T-230, block.go — dnsCache), закрывая DNS-rebinding во
// времени (смена адреса между HTTP-этапом и рендером) в самом
// Chromium; остаточный вектор — различение атакующим двух резолвов
// (перехвата и браузера) — держит egress-политика контейнера
// (docs/render.md).
func renderHostInternal(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return httpfetch.AddrIsInternal(ip)
	}
	if ip, ok := whatwgParseIPv4(host); ok {
		return httpfetch.AddrIsInternal(ip)
	}
	return false
}

// whatwgParseIPv4 — канонизация числоподобного хоста по алгоритму
// IPv4-парсера WHATWG URL (им руководствуется Chromium): хост из 1..4
// точечных частей, каждая — десятичное/восьмеричное (ведущий 0)/
// шестнадцатеричное (0x) число, сворачивается в IPv4; хост, чья
// последняя часть не число — имя, не IPv4 («0.com» проходит как имя).
// Точная копия порогов WHATWG: не-последние части ≤ 255, последняя <
// 256^(5−n). ok=false — хост не числоподобный (имя или мусор).
func whatwgParseIPv4(host string) (netip.Addr, bool) {
	parts := strings.Split(host, ".")
	if len(parts) == 0 || len(parts) > 4 {
		return netip.Addr{}, false
	}
	if _, ok := whatwgIPv4Number(parts[len(parts)-1]); !ok {
		return netip.Addr{}, false // не заканчивается числом — имя
	}
	nums := make([]uint64, len(parts))
	for i, p := range parts {
		v, ok := whatwgIPv4Number(p)
		if !ok {
			return netip.Addr{}, false
		}
		nums[i] = v
	}
	for _, v := range nums[:len(nums)-1] {
		if v > 255 {
			return netip.Addr{}, false
		}
	}
	if nums[len(nums)-1] >= uint64(1)<<(8*(5-len(nums))) {
		return netip.Addr{}, false
	}
	var v uint64
	for i, n := range nums[:len(nums)-1] {
		v += n << (8 * (3 - i)) // части, кроме последней, — старшие байты
	}
	v += nums[len(nums)-1]
	return netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}), true
}

// whatwgIPv4Number — число одной части IPv4-хоста WHATWG: 0x/0X —
// шестнадцатеричное (пустая мантисса — 0), ведущий 0 — восьмеричное,
// иначе десятичное; пустая строка и посторонние символы — не число.
func whatwgIPv4Number(s string) (uint64, bool) {
	switch {
	case len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X'):
		m := s[2:]
		if m == "" {
			return 0, true
		}
		v, err := strconv.ParseUint(m, 16, 64)
		return v, err == nil
	case len(s) > 1 && s[0] == '0':
		v, err := strconv.ParseUint(s[1:], 8, 64)
		return v, err == nil
	default:
		if s == "" {
			return 0, false
		}
		v, err := strconv.ParseUint(s, 10, 64)
		return v, err == nil
	}
}

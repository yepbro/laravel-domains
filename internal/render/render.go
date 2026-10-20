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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"crawler/internal/config"
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
	// cookie-хранилища (data:, about:) — nil.
	Cookies []ExtractCookie
	// ResponseHeaders — заголовки ответа основной навигации (ТЗ 5.8 —
	// правила по заголовкам): http.Header, как у HTTP-этапа (T-058).
	// У навигаций без сетевого запроса — псевдо-ответ Chromium (data: —
	// Content-Type из URL) либо nil.
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
	// /json/version; на сам запуск браузера отдельного таймаута нет —
	// контекст StartBrowser обязан жить столько, сколько браузер (жизненный
	// цикл воркера), отмена извне гасит и инициализацию.
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
}

// NewOptions собирает Options из конфигурации сервиса (ТЗ 5.10): секция
// render плюс User-Agent HTTP-этапа (ТЗ §9 — единый идентифицируемый UA
// краулера) и потолок DOM из секции content (R-02: итоговый DOM не больше
// того, что хранилище контента принимает).
func NewOptions(cfg config.Config) Options {
	return Options{
		RemoteURL:         cfg.Render.RemoteURL,
		Contexts:          cfg.Render.Contexts,
		Timeout:           time.Duration(cfg.Render.Timeout),
		RestartEveryPages: cfg.Render.RestartEveryPages,
		MemoryThreshold:   cfg.Render.RestartMemoryBytes.B(),
		UserAgent:         cfg.HTTP.UserAgent,
		MaxDOMBytes:       cfg.Content.MaxStoreBytes.B(),
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

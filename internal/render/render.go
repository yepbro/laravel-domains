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
// недоступный браузер — наверх (перезапуск — T-065), ошибка страницы —
// обычный результат этапа (повторная доставка — уровень очереди, T-066).
//
// Развитие T-062…T-067 поверх текущего API: блокировка ресурсов (T-062) и
// ожидание «затишья» сети (T-063) встраиваются этапами в Session.Run (между
// Navigate и съёмом результата), итоговый DOM и cookies (T-064) — полями
// RenderOutput; счётчик Browser.Pages обслуживает перезапуск каждые N
// страниц (T-065); цикл очередей (T-066/T-067) вызовет Pool.Run.
package render

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"crawler/internal/config"
)

// RenderTask — одна задача рендера: открыть страницу и снять результат.
//
// URL приходит из результата HTTP-этапа (сообщение q:render, T-066) и здесь
// дополнительно не валидируется: формальный контроль входа — уровень
// воркера, некорректный адрес отвергнёт сам браузер (chromedp.ErrPageLoad).
type RenderTask struct {
	URL string
}

// RenderOutput — минимальный результат T-061: итоговый URL документа (после
// редиректов загрузки) и заголовок. T-064 дополнит его итоговым DOM
// (document.documentElement.outerHTML) и всеми cookies: поля добавляются,
// интерфейс не меняется.
type RenderOutput struct {
	// FinalURL — document.location после загрузки страницы.
	FinalURL string
	// Title — содержимое document.title (диагностика и live-проверки).
	Title string
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
	// (ТЗ 5.6): Browser ведёт счётчик Pages, сам перезапуск — T-065.
	RestartEveryPages int
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
}

// NewOptions собирает Options из конфигурации сервиса (ТЗ 5.10): секция
// render плюс User-Agent HTTP-этапа (ТЗ §9 — единый идентифицируемый UA
// краулера).
func NewOptions(cfg config.Config) Options {
	return Options{
		RemoteURL:         cfg.Render.RemoteURL,
		Contexts:          cfg.Render.Contexts,
		Timeout:           time.Duration(cfg.Render.Timeout),
		RestartEveryPages: cfg.Render.RestartEveryPages,
		UserAgent:         cfg.HTTP.UserAgent,
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
	if len(problems) > 0 {
		return errors.New("render: " + strings.Join(problems, "; "))
	}
	return nil
}

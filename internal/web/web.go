// Package web — публичный сервис лендинга (этап 13, T-121/T-122;
// продуктовое расширение вне основного ТЗ — перенос из larasites):
// предподсчитанные счётчики конвейера, форма ручной проверки домена,
// i18n ru/en.
//
// Каркас (T-121): публичный слушатель — собственный http.Server внутри
// OnRun с graceful stop по отмене ctx. Каркас internal/service даёт один
// порт HTTP на сервис и вешает туда /metrics с пробными эндпоинтами —
// выставлять его наружу нельзя (метрики — внутренняя поверхность,
// ТЗ §9), поэтому MetricsAddr остаётся на loopback (:9107, cmd/web), а
// посетителя обслуживает web.listen. Прецедент управления жизненным
// циклом внутри OnRun — клиенты воркеров (Redis-очереди).
//
// Роуты:
//
//   - GET /            → редирект на дефолтную локаль /ru;
//   - GET /{locale}    — лендинг локали ru|en (T-122: перенос разметки
//     larasites — статистика/облако/таблица зон, последние находки,
//     форма, SEO canonical/hreflang/OG/Twitter);
//   - POST /api/check-domain — живая проверка (T-123, заглушка 501);
//   - GET /healthz     — readiness: ping PostgreSQL (503 при отказе);
//   - GET /static/*    — статика лендинга (go:embed; app.css —
//     генерат Tailwind, placeholder до make web-css/T-124).
package web

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/checker"
	"crawler/internal/config"
)

// Pinger — readiness-часть зависимости БД.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Querier — выборки данных лендинга (*pgxpool.Pool удовлетворяет вместе
// с Pinger). Выделен для тестов без PostgreSQL.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// DB — зависимость сервиса: readiness ping + данные страницы.
type DB interface {
	Pinger
	Querier
}

// Locales — поддерживаемые локали лендинга (зафиксированное решение
// этапа 13: i18n ru+en; ru — дефолт).
var Locales = []string{"ru", "en"}

// DefaultLocale — куда редиректит GET /.
const DefaultLocale = "ru"

// Options — параметры сервиса.
type Options struct {
	// Log — логгер сервиса; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
}

// Server — публичный HTTP-сервер лендинга.
type Server struct {
	db      DB
	opt     Options
	tpl     *template.Template
	cfg     *config.Config
	limiter *tokenBucketLimiter

	// liveCheck — шов живой проверки (T-123): production — checker.Run;
	// тесты подменяют фейком без сети (в стиле ContentGetter воркеров).
	liveCheck checker.LiveFunc
}

// landingVM — данные рендера лендинга: локаль, список локалей,
// данные страницы и словарь локали (методы Msg/MsgRaw/ToJSON вызываются
// из шаблона как {{$.Msg "ключ"}} — функции данных, а не FuncMap:
// шаблон парсится один раз без локали).
type landingVM struct {
	Locale  string
	Locales []string
	Data    LandingData
	msgs    messages
}

// Msg — строка локали, безопасно экранированная.
func (v landingVM) Msg(key string) template.HTML {
	return template.HTML(template.HTMLEscapeString(v.msgs[key]))
}

// MsgRaw — строка локали без экранирования (для JSON-вставок).
func (v landingVM) MsgRaw(key string) string { return v.msgs[key] }

// ToJSON — JSON-литерал строки для <script type="application/json">.
func (v landingVM) ToJSON(s string) template.HTML {
	b, err := json.Marshal(s)
	if err != nil {
		return template.HTML(`""`)
	}
	return template.HTML(b)
}

// New собирает сервер: db — пул БД (ping + данные страницы + журнал
// формы). cfg — конфигурация секции web (рейт-лимит, timeout живой
// проверки, T-123). Шаблон парсится один раз: локаль приходит данными
// (landingVM), словарь — метод рендера.
func New(db DB, cfg *config.Config, opts Options) (*Server, error) {
	if db == nil {
		return nil, errors.New("web: пул БД не задан (postgres.dsn обязателен)")
	}
	if cfg == nil {
		return nil, errors.New("web: конфигурация не задана")
	}
	tpl, err := template.ParseFS(templates, "templates/landing.html")
	if err != nil {
		return nil, fmt.Errorf("web: шаблоны: %w", err)
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	return &Server{
		db:        db,
		opt:       opts,
		tpl:       tpl,
		cfg:       cfg,
		limiter:   newLimiter(cfg.Web.RateLimit, time.Minute),
		liveCheck: checker.Run,
	}, nil
}

//go:embed templates
var templates embed.FS

//go:embed static
var staticFS embed.FS

// Handler — маршрутизатор публичной поверхности (тесты — httptest).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("POST /api/check-domain", s.handleCheckDomainLive)
	// GET на API-эндпоинт перехватывался бы префиксом лендинга (GET /)
	// с вводящим в заблуждение 404 — честный 405 без раскрытия деталей.
	mux.HandleFunc("GET /api/check-domain", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})
	mux.HandleFunc("GET /static/", s.handleStatic)
	mux.HandleFunc("GET /{$}", s.handleRoot)
	mux.HandleFunc("GET /", s.handleLanding)
	return mux
}

// Run — жизненный цикл: слушатель web.listen, graceful stop по отмене
// ctx (бюджет остановки каркаса — 10 с: Shutdown с меньшим окном,
// начатые ответы дорабатывают). Вызывается из OnRun cmd/web.
func (s *Server) Run(ctx context.Context, listen string) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("web: слушатель %s: %w", listen, err)
	}
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second, // slowloris-потолок (§9)
	}
	done := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		// Serve закрывает слушатель сам; ErrServerClosed — штатный выход
		// после Shutdown, не ошибка.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			done <- fmt.Errorf("web: сервер %s: %w", listen, err)
			return
		}
		done <- nil
	}()
	s.opt.Log.Info("публичный сервис лендинга запущен", "listen", listen)

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Graceful shutdown (ТЗ 3.4): начатые ответы дорабатывают в окне
		// меньше бюджета остановки каркаса.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("web: остановка: %w", err)
		}
		return nil
	}
}

// handleRoot — GET / — редирект на дефолтную локаль (выбор локали
// префиксом пути, не cookie — канонические адреса для SEO).
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/"+DefaultLocale, http.StatusFound)
}

// handleLanding — GET /{locale}: данные страницы из кэшей (T-120) и
// рендер шаблона локали. Сбой БД — 500 без раскрытия деталей: страница
// без данных бессмысленна (деградация секций — паттерн внутреннего
// дашборда T-129, не публичной страницы).
func (s *Server) handleLanding(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	locale, _, _ := strings.Cut(path, "/")
	if _, ok := localeMessagesMap[locale]; !ok {
		http.NotFound(w, r)
		return
	}
	data, err := FetchLandingData(r.Context(), s.db)
	if err != nil {
		s.opt.Log.Warn("данные лендинга", "locale", locale, "err", err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tpl.ExecuteTemplate(w, "landing.html", landingVM{
		Locale:  locale,
		Locales: Locales,
		Data:    data,
		msgs:    localeMessages(locale),
	}); err != nil {
		// Заголовок уже мог уйти — статус не сменить; логируем и рвём.
		s.opt.Log.Warn("рендер лендинга", "locale", locale, "err", err)
	}
}

// handleStatic — статика из embed: собирается в бинарий, отдаётся
// immutable-заголовком (имена без хэшей — кэш по общему правилу).
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	// app.css — генерат Tailwind (make web-css / стадия css Dockerfile,
	// T-124); в git — placeholder, чтобы go:embed не падал на свежем
	// клоне (страница откроется без стилей, не 500).
	http.StripPrefix("/static/", http.FileServer(http.FS(sub))).ServeHTTP(w, r)
}

// handleCheckDomain удалён (T-123): форму обслуживает
// handleCheckDomainLive (api.go) — гибрид stored/live с рейт-лимитом.

// handleHealthz — readiness: ping PostgreSQL с коротким таймаутом
// (посетитель не ждёт восстановления БД дольше секунды — 503 быстрее).
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

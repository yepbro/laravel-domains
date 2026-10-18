// Package web — публичный сервис лендинга (этап 13, T-121; продуктовое
// расширение вне основного ТЗ — перенос из larasites): предподсчитанные
// счётчики конвейера, форма ручной проверки домена, i18n ru/en.
//
// Каркас (T-121): публичный слушатель — собственный http.Server внутри
// OnRun с graceful stop по отмене ctx. Каркас internal/service даёт один
// порт HTTP на сервис и вешает туда /metrics с пробыми эндпоинтами —
// выставлять его наружу нельзя (метрики — внутренняя поверхность,
// ТЗ §9), поэтому MetricsAddr остаётся на loopback (:9107, cmd/web), а
// посетителя обслуживает web.listen. Прецедент управления жизненным
// циклом внутри OnRun — клиенты воркеров (Redis-очереди).
//
// Роуты (наполнение лендинга — T-122, форма — T-123):
//
//   - GET /            → редирект на дефолтную локаль /ru;
//   - GET /{locale}    — лендинг локали ru|en (T-122);
//   - POST /api/check-domain — живая проверка (T-123, заглушка 501);
//   - GET /healthz     — readiness: ping PostgreSQL (503 при отказе) —
//     отдельный от metrics-порта эндпоинт;
//   - GET /static/*    — статика лендинга (T-122).
//
// Методы: не GET/HEAD на страницах и не POST на API — 405. Шаблоны и
// статика — html/template + go:embed (прецедент migrations/embed.go),
// новых зависимостей нет.
package web

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// Pinger — зависимость readiness-проб: пул БД (*pgxpool.Pool
// удовлетворяет). Выделен для тестов каркаса без PostgreSQL.
type Pinger interface {
	Ping(ctx context.Context) error
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
	ping Pinger
	opt  Options
	tpl  *template.Template
}

// New собирает сервер: ping — источник readiness-проверки /healthz.
func New(ping Pinger, opts Options) (*Server, error) {
	if ping == nil {
		return nil, errors.New("web: пул БД не задан (postgres.dsn обязателен)")
	}
	tpl, err := template.ParseFS(templates, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("web: шаблоны: %w", err)
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	return &Server{ping: ping, opt: opts, tpl: tpl}, nil
}

//go:embed templates
var templates embed.FS

// Handler — маршрутизатор публичной поверхности (тесты — httptest).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("POST /api/check-domain", s.handleCheckDomain)
	// GET на API-эндпоинт перехватывался бы префиксом лендинга (GET /)
	// с вводящим в заблуждение 404 — честный 405 без раскрытия деталей.
	mux.HandleFunc("GET /api/check-domain", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})
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

// handleRoot — GET / — редирект на дефолтную локаль (i18n ru+en, ru —
// дефолт; выбор локали префиксом пути, не cookie — канонические адреса
// для SEO).
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/"+DefaultLocale, http.StatusFound)
}

// handleLanding — GET /{locale} и статика: лендинг локали (T-122
// наполняет разметкой и данными; каркас рендерит шаблон с локалью и
// закрывает чужие префиксы 404).
func (s *Server) handleLanding(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	locale, _, _ := strings.Cut(path, "/")
	if !localeSupported(locale) {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tpl.ExecuteTemplate(w, "landing.html", map[string]any{
		"Locale": locale,
	}); err != nil {
		// Заголовок уже мог уйти — статус не сменить; логируем и рвём.
		s.opt.Log.Warn("рендер лендинга", "locale", locale, "err", err)
	}
}

// handleCheckDomain — POST /api/check-domain: живая проверка домена из
// формы (T-123: гибрид сохранённых данных и живого конвейера). До T-123
// — честная заглушка 501 (зависимости формы ещё не собраны).
func (s *Server) handleCheckDomain(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "not implemented yet (T-123)", http.StatusNotImplemented)
}

// handleHealthz — readiness: ping PostgreSQL с коротким таймаутом
// (посетитель не ждёт восстановления БД дольше секунды — 503 быстрее).
// nil-зависимости каркас не допускает (New).
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.ping.Ping(ctx); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

func localeSupported(locale string) bool {
	return locale == "ru" || locale == "en"
}

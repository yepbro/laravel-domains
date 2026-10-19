// Package dashboard — операторский веб-дашборд конвейера (ТЗД; этап 15):
// единая страница состояния конвейера за паролем, строго read-only
// (ТЗД §1.3). Каркас T-128 — публичный http.Server на dashboard.listen
// с graceful stop по отмене ctx (прецедент жизненного цикла —
// internal/web, T-121), авторизация Basic + cookie-сессия и защита от
// подбора (ТЗД §7). Сборка снимков /api/status — T-129 (snapshot.go —
// данные, collector.go — фоновый цикл), фронт — assets.go (T-130),
// развёртывание — T-131.
//
// Каркас internal/service даёт один порт HTTP на сервис и вешает туда
// /metrics с пробными эндпоинтами — выставлять его наружу нельзя
// (метрики — внутренняя поверхность, ТЗ §9), поэтому MetricsAddr
// остаётся на loopback (127.0.0.1:9108, ТЗД §4.2), а оператора
// обслуживает dashboard.listen.
//
// Роуты (ТЗД §6.1):
//
//   - GET /            → HTML-страница дашборда (assets/page.html);
//   - GET /assets/*    — статика фронта из embed (app.js, style.css);
//   - GET /api/status  — JSON последнего снимка (T-129): до первого
//     успешного сбора — 503 (ТЗД §5.9);
//   - GET /healthz     — readiness: ping PostgreSQL (503 при отказе);
//   - GET /livez       — liveness;
//   - GET /robots.txt  — `Disallow: /` (внутренний сервис).
//
// Первые три — за авторизацией, служебные — без. Не GET/HEAD — 405
// (методные шаблоны ServeMux); security-заголовки всех ответов и
// таймауты сервера — ТЗД §6.3.
package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/dashboard/auth"
)

// Pinger — readiness-зависимость БД (*pgxpool.Pool удовлетворяет): пул
// нужен пробе /healthz (ТЗД §6.1) и сборщику снимков (collector.go).
type Pinger interface {
	Ping(ctx context.Context) error
}

// Options — параметры сервиса.
type Options struct {
	// Log — логгер сервиса; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// Now — часы: выдача/проверка cookie-сессий (§7.5) и окно защиты от
	// подбора (§7.6); nil — time.Now. Шов тестов.
	Now func() time.Time
	// Snapshots — источник снимков /api/status (цикл сборки T-129,
	// *Collector); nil — снимков нет: 503 «snapshot_not_ready» (семантика
	// §5.9 до первого сбора — прецедент заглушки T-128).
	Snapshots SnapshotSource
}

// Server — публичный HTTP-сервер дашборда.
type Server struct {
	db       Pinger
	snaps    SnapshotSource
	opt      Options
	username string
	hash     auth.PasswordHash
	limiter  *loginLimiter
}

// ParseCredentials — стартовая проверка кредов (ТЗД §7.7): пустые
// username/password_hash или неразборимая строка хэша — ошибка запуска.
// Вызывается из cmd/dashboard до service.Run (отказ до миграций и
// слушателей) и повторно New-ом (тесты); общий config.Validate креды
// сознательно не проверяет — иначе требование коснулось бы всех
// сервисов проекта.
func ParseCredentials(cfg *config.Config) (auth.PasswordHash, error) {
	if cfg.Dashboard.Username == "" {
		return auth.PasswordHash{}, errors.New("dashboard: username пуст — креды оператора обязательны (ТЗД §7.7)")
	}
	// Basic-схема разделяет креды по первому «:» (r.BasicAuth): имя с
	// двоеточием не сможет совпасть никогда — отказываем сразу, с внятной
	// ошибкой, а не «молчаливым» отказом входа.
	if strings.Contains(cfg.Dashboard.Username, ":") {
		return auth.PasswordHash{}, errors.New("dashboard: username не может содержать «:» (синтаксис Basic user:pass)")
	}
	if cfg.Dashboard.PasswordHash == "" {
		return auth.PasswordHash{}, errors.New("dashboard: password_hash пуст — креды оператора обязательны (ТЗД §7.7)")
	}
	hash, err := auth.Parse(cfg.Dashboard.PasswordHash)
	if err != nil {
		return auth.PasswordHash{}, fmt.Errorf("dashboard: password_hash: %w", err)
	}
	return hash, nil
}

// New собирает сервер: db — пул БД (проба /healthz), cfg — конфигурация
// секции dashboard; креды проходят стартовую проверку §7.7 (см.
// ParseCredentials).
func New(db Pinger, cfg *config.Config, opts Options) (*Server, error) {
	if db == nil {
		return nil, errors.New("dashboard: пул БД не задан (postgres.dsn обязателен)")
	}
	if cfg == nil {
		return nil, errors.New("dashboard: конфигурация не задана")
	}
	hash, err := ParseCredentials(cfg)
	if err != nil {
		return nil, err
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Server{
		db:       db,
		snaps:    opts.Snapshots,
		opt:      opts,
		username: cfg.Dashboard.Username,
		hash:     hash,
		limiter:  newLoginLimiter(maxLoginFailures, loginWindow),
	}, nil
}

// contentSecurityPolicy — CSP всех ответов дашборда (ТЗД §6.3,
// дословно): внешний мир закрыт, скрипты/стили/соединения — только
// свои, data:-картинки — инлайн-SVG фронта (T-130).
const contentSecurityPolicy = `default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:`

// Handler — маршрутизатор публичной поверхности (тесты — httptest).
// Методные шаблоны ServeMux сами отвечают 405 на не-GET/HEAD (§6.3),
// HEAD обслуживается GET-шаблонами.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Без авторизации (ТЗД §6.1).
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /robots.txt", s.handleRobots)
	// За авторизацией (ТЗД §6.1, §7).
	mux.Handle("GET /{$}", s.auth(s.handleIndex))
	mux.Handle("GET /assets/", s.auth(s.handleAssets))
	mux.Handle("GET /api/status", s.auth(s.handleAPIStatus))
	return s.secureHeaders(mux)
}

// secureHeaders — security-заголовки всех ответов, включая 401/429/405
// (ТЗД §6.3).
func (s *Server) secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Content-Security-Policy", contentSecurityPolicy)
		next.ServeHTTP(w, r)
	})
}

// Run — жизненный цикл: слушатель dashboard.listen ([host]:port,
// ТЗД §8.3), graceful stop по отмене ctx (бюджет остановки каркаса —
// 10 с: Shutdown с меньшим окном, начатые ответы дорабатывают).
// Вызывается из OnRun cmd/dashboard. Таймауты сервера — ТЗД §6.3:
// ReadHeader 5 с, Read 10 с, Write 15 с, Idle 60 с.
func (s *Server) Run(ctx context.Context, listen string) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("dashboard: слушатель %s: %w", listen, err)
	}
	srv := &http.Server{
		Handler: s.Handler(),
		// Таймауты сервера (ТЗД §6.3).
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	done := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		// Serve закрывает слушатель сам; ErrServerClosed — штатный выход
		// после Shutdown, не ошибка.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			done <- fmt.Errorf("dashboard: сервер %s: %w", listen, err)
			return
		}
		done <- nil
	}()
	s.opt.Log.Info("публичный сервис дашборда запущен", "listen", listen)

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Graceful shutdown (ТЗ 3.4): начатые ответы дорабатывают в окне
		// меньше бюджета остановки каркаса.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("dashboard: остановка: %w", err)
		}
		return nil
	}
}

// handleIndex и handleAssets (страница и статика из embed) — assets.go
// (T-130).

// handleAPIStatus — GET /api/status: JSON последнего снимка (ТЗД §6.2):
// секции зон/итогов/темпа/скоростей — как crawlerctl status --json
// (status.SummaryJSON, T-129), плюс статусы/Laravel/ingest/здоровье и
// карта ошибок секций; null для недоступных значений. До первого
// успешного сбора — 503 (ТЗД §5.9); Cache-Control: no-store —
// периодический опрос не кэширует ни снимок, ни отказ.
func (s *Server) handleAPIStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	var snap *Snapshot
	if s.snaps != nil {
		snap = s.snaps.Current()
	}
	if snap == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"snapshot_not_ready"}`+"\n")
		return
	}
	// Снимок публикуется атомарной заменой целиком — маршалинг
	// консистентен одному моменту сборки; EscapeHTML off — стиль CLI.
	// Кодирование в буфер: ошибка (гипотетический NaN из real-колонок)
	// не должна отдавать частичный JSON со статусом 200.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(snap); err != nil {
		http.Error(w, "snapshot encode failed", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(buf.Bytes())
}

// handleHealthz — readiness без авторизации (ТЗД §6.1): ping PostgreSQL
// коротким таймаутом (оператор не ждёт восстановления БД дольше
// секунды — 503 быстрее; стиль internal/web).
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

// handleLivez — liveness без авторизации (ТЗД §6.1): процесс и его
// HTTP-цикл живы; зависимости сознательно не проверяются — семантика
// пробы каркаса internal/service.
func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

// handleRobots — GET /robots.txt без авторизации (ТЗД §6.1): дашборд —
// внутренний сервис, полный disallow.
func (s *Server) handleRobots(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "User-agent: *\nDisallow: /\n")
}

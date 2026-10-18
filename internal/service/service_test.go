package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/config"
	"crawler/internal/metrics"
)

// ---- Логирование ----

func TestLoggerServiceAttrAndAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "test-svc", slog.LevelDebug)
	logger.Info("запуск", "port", 9101)

	out := buf.String()
	for _, want := range []string{"level=INFO", "service=test-svc", "msg=запуск", "port=9101"} {
		if !strings.Contains(out, want) {
			t.Errorf("лог не содержит %q:\n%s", want, out)
		}
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	newLogger(&buf, "svc", slog.LevelInfo).Debug("отладка")
	if buf.Len() != 0 {
		t.Errorf("debug при уровне info не должен попадать в лог:\n%s", buf.String())
	}

	buf.Reset()
	logger := newLogger(&buf, "svc", logLevel("warn")) // уровень из значения конфига
	logger.Info("скрыто")
	logger.Warn("видно")
	if strings.Contains(buf.String(), "скрыто") {
		t.Errorf("info при уровне warn не должен попадать в лог:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "видно") {
		t.Errorf("warn при уровне warn должен попадать в лог:\n%s", buf.String())
	}
}

func TestNewLoggerDefaultsToInfo(t *testing.T) {
	// nil-конфиг (сервис без config.Load) — уровень info.
	if got := NewLogger("svc", nil).Enabled(context.Background(), slog.LevelInfo); !got {
		t.Error("NewLogger(nil-конфиг) должен включать уровень info")
	}

	// Уровень берётся из конфигурации: warn пропускает Warn и режет Info.
	cfg := &config.Config{}
	cfg.Log.Level = "warn"
	var buf bytes.Buffer
	logger := NewLoggerForTest(&buf, cfg)
	logger.Info("скрыто")
	logger.Warn("видно")
	if strings.Contains(buf.String(), "скрыто") {
		t.Errorf("info при уровне warn из конфига не должен попадать в лог:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "видно") {
		t.Errorf("warn при уровне warn из конфига должен попадать в лог:\n%s", buf.String())
	}
}

// NewLoggerForTest — NewLogger с подменой приёмника (stderr → буфер).
func NewLoggerForTest(w io.Writer, cfg *config.Config) *slog.Logger {
	return newLogger(w, "svc", logLevel(cfg.Log.Level))
}

func TestLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":            slog.LevelInfo,
		"info":        slog.LevelInfo,
		"debug":       slog.LevelDebug,
		"warn":        slog.LevelWarn,
		"error":       slog.LevelError,
		"WARN":        slog.LevelWarn, // регистр нормализован в config.normalize
		" debug ":     slog.LevelDebug,
		"трассировка": slog.LevelInfo, // недостижимо после валидации config
	}
	for in, want := range cases {
		if got := logLevel(in); got != want {
			t.Errorf("logLevel(%q) = %v, хочу %v", in, got, want)
		}
	}
}

// ---- Жизненный цикл (serve) ----

// recorder — потокобезопасный журнал событий для проверки порядка
// graceful shutdown (ТЗ 3.4: рабочая логика → HTTP метрик → пул БД).
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *recorder) has(event string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e == event {
			return true
		}
	}
	return false
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// waitFor ждёт появления события: serve работает в горутинах.
func waitFor(t *testing.T, r *recorder, event string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.has(event) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("событие %q не наступило за 2 с; события: %v", event, r.snapshot())
}

// lifecycle — один прогон serve: каналы сигналов, результат, события.
type lifecycle struct {
	rec      *recorder
	finished chan struct{} // закрытие = serve вернулся; результат — в err
	err      error
	term     chan os.Signal
	hup      chan os.Signal
	addr     string       // адрес HTTP-сервера метрик (127.0.0.1:порт)
	ln       net.Listener // для имитации падения HTTP-сервера
}

// failHTTP имитирует падение HTTP-сервера: закрытие listener из-под Serve.
func (lc *lifecycle) failHTTP(t *testing.T) {
	t.Helper()
	if err := lc.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("закрытие listener: %v", err)
	}
}

// startServe запускает serve с тестовым Options; closePool фиксирует
// закрытие пула БД, handler — хендлеры HTTP-сервера метрик теста,
// rec — журнал событий теста (nil — свой).
func startServe(t *testing.T, opts Options, handler http.Handler, rec *recorder) *lifecycle {
	t.Helper()
	if rec == nil {
		rec = &recorder{}
	}
	if opts.Logger == nil {
		opts.Logger = newLogger(io.Discard, "test", slog.LevelError)
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 5 * time.Second
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	lc := &lifecycle{
		rec:      rec,
		finished: make(chan struct{}),
		term:     make(chan os.Signal, 2),
		hup:      make(chan os.Signal, 1),
		addr:     ln.Addr().String(),
		ln:       ln,
	}
	go func() {
		lc.err = serve(opts, srv, ln, func() { lc.rec.add("pool-closed") }, lc.term, lc.hup)
		close(lc.finished)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		select {
		case <-lc.finished:
		case <-time.After(2 * time.Second):
			t.Error("serve не завершился после закрытия listener")
		}
	})
	return lc
}

func (lc *lifecycle) waitDone(t *testing.T) error {
	t.Helper()
	select {
	case <-lc.finished:
		return lc.err
	case <-time.After(3 * time.Second):
		t.Fatal("serve не завершился за 3 с")
		return nil
	}
}

// TestServeShutdownOrder — §3.4: сигнал отменяет контекст OnRun, serve
// ждёт завершения рабочей логики, HTTP-сервер дожидается начатого запроса,
// пул БД закрывается последним.
func TestServeShutdownOrder(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, _ *http.Request) {
		rec.add("http-start")
		time.Sleep(400 * time.Millisecond) // запрос в полёте в момент сигнала
		rec.add("http-done")
		_, _ = w.Write([]byte("ok"))
	})
	lc := startServe(t, Options{
		Name: "test",
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done() // «доработка текущей задачи» по отмене контекста
			rec.add("onrun-done")
			return nil
		},
	}, mux, rec)

	<-ready
	go func() {
		resp, err := http.Get("http://" + lc.addr + "/slow")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	waitFor(t, rec, "http-start")
	lc.term <- syscall.SIGTERM

	if err := lc.waitDone(t); err != nil {
		t.Fatalf("serve: %v", err)
	}
	// http-start случился до сигнала; далее — строгий порядок §3.4.
	want := []string{"http-start", "onrun-done", "http-done", "pool-closed"}
	got := rec.snapshot()
	if len(got) != len(want) {
		t.Fatalf("события = %v, хочу %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("порядок остановки = %v, хочу %v (ТЗ 3.4: OnRun → HTTP → пул)", got, want)
		}
	}
}

// TestServeOnRunCleanExit — самостоятельное завершение OnRun без сигнала
// останавливает сервис (одноразовая задача), resources закрываются.
func TestServeOnRunCleanExit(t *testing.T) {
	done := make(chan struct{})
	lc := startServe(t, Options{
		Name: "test",
		OnRun: func(context.Context) error {
			close(done)
			return nil
		},
	}, http.NewServeMux(), nil)

	<-done
	if err := lc.waitDone(t); err != nil {
		t.Fatalf("serve: %v (чистое завершение — nil)", err)
	}
	if !lc.rec.has("pool-closed") {
		t.Errorf("пул должен закрываться и при чистом завершении; события: %v", lc.rec.snapshot())
	}
}

// TestServeOnRunError — ошибка рабочей логики останавливает сервис
// и возвращается вызывающему (код выхода 1).
func TestServeOnRunError(t *testing.T) {
	boom := errors.New("воркер сломался")
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name: "test",
		OnRun: func(context.Context) error {
			close(ready)
			return boom
		},
	}, http.NewServeMux(), nil)

	<-ready
	err := lc.waitDone(t)
	if !errors.Is(err, boom) {
		t.Fatalf("serve = %v, хочу исходная ошибка OnRun", err)
	}
	if !lc.rec.has("pool-closed") {
		t.Errorf("пул должен закрываться и при ошибке; события: %v", lc.rec.snapshot())
	}
}

// TestServeShutdownTimeout — не остановившаяся за таймаут рабочая логика
// не блокирует остановку навсегда: HTTP и пул закрываются, serve возвращает
// ошибку таймаута.
func TestServeShutdownTimeout(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name:            "test",
		ShutdownTimeout: 60 * time.Millisecond,
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done()
			time.Sleep(500 * time.Millisecond) // игнорирует отмену
			rec.add("late")
			return nil
		},
	}, http.NewServeMux(), rec)

	<-ready
	start := time.Now()
	lc.term <- syscall.SIGTERM
	err := lc.waitDone(t)
	if err == nil || !strings.Contains(err.Error(), "не остановилась") {
		t.Fatalf("serve = %v, хочу ошибку таймаута остановки", err)
	}
	if elapsed := time.Since(start); elapsed > 450*time.Millisecond {
		t.Fatalf("остановка заняла %s — таймаут не сработал", elapsed)
	}
	if !rec.has("pool-closed") {
		t.Errorf("пул должен закрываться и после таймаута; события: %v", rec.snapshot())
	}
}

// TestServeSecondSignalForcesExit — повторный сигнал в фазе ожидания OnRun
// прерывает graceful shutdown (errForcedExit) без закрытия ресурсов.
func TestServeSecondSignalForcesExit(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name:            "test",
		ShutdownTimeout: 10 * time.Second, // долго ждём OnRun — перебьём сигналом
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done()
			rec.add("stopping")          // serve вошёл в фазу остановки
			block := make(chan struct{}) // «завис»: не завершается по отмене
			<-block
			return nil
		},
	}, http.NewServeMux(), rec)

	<-ready
	lc.term <- syscall.SIGTERM
	waitFor(t, rec, "stopping")

	start := time.Now()
	lc.term <- syscall.SIGTERM
	err := lc.waitDone(t)
	if !errors.Is(err, errForcedExit) {
		t.Fatalf("serve = %v, хочу errForcedExit", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("принудительный выход занял %s", elapsed)
	}
	if rec.has("pool-closed") {
		t.Errorf("принудительный выход не должен закрывать пул (немедленный выход)")
	}
}

// TestServeSIGHUP — SIGHUP до сигнала завершения доходит до OnSIGHUP.
func TestServeSIGHUP(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name: "test",
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done()
			return nil
		},
		OnSIGHUP: func() { rec.add("sighup") },
	}, http.NewServeMux(), rec)

	<-ready
	lc.hup <- syscall.SIGHUP
	waitFor(t, rec, "sighup")

	lc.term <- syscall.SIGTERM
	if err := lc.waitDone(t); err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// TestServeHTTPServerError — падение HTTP-сервера останавливает сервис
// с ошибкой; рабочая логика завершается по отмене контекста, ресурсы
// закрываются (compose перезапустит сервис по restart-политике).
func TestServeHTTPServerError(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name: "test",
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done() // завершаемся по отмене после ошибки HTTP
			rec.add("onrun-done")
			return nil
		},
	}, http.NewServeMux(), rec)

	<-ready
	lc.failHTTP(t) // «падение сервера»: закрытие listener из-под Serve

	err := lc.waitDone(t)
	if err == nil || !strings.Contains(err.Error(), "HTTP-сервер метрик") {
		t.Fatalf("serve = %v, хочу ошибку HTTP-сервера", err)
	}
	want := []string{"onrun-done", "pool-closed"}
	got := rec.snapshot()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("события = %v, хочу %v", got, want)
	}
}

// TestServeOnRunPanic — паника рабочей логики не роняет процесс: serve
// возвращает её как ошибку, ресурсы закрываются (graceful путь).
func TestServeOnRunPanic(t *testing.T) {
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name: "test",
		OnRun: func(context.Context) error {
			close(ready)
			panic("битое сообщение из очереди")
		},
	}, http.NewServeMux(), nil)

	<-ready
	err := lc.waitDone(t)
	if err == nil || !strings.Contains(err.Error(), "паника") {
		t.Fatalf("serve = %v, хочу ошибку паники OnRun", err)
	}
	if !lc.rec.has("pool-closed") {
		t.Errorf("пул должен закрываться и при панике; события: %v", lc.rec.snapshot())
	}
}

// ---- Run ----

// TestRunValidation — имя сервиса обязательно.
func TestRunValidation(t *testing.T) {
	if err := Run(Options{}); err == nil || !strings.Contains(err.Error(), "имя сервиса") {
		t.Fatalf("Run без Name = %v, хочу ошибку об обязательном имени", err)
	}
}

// TestHealthzHandler — /healthz отражает готовность зависимостей:
// без пула БД (сервис без OnDBPool/DSN) — 200; с пулом на недоступной
// БД — 503, healthcheck оркестратора видит падение PostgreSQL.
func TestHealthzHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	healthzHandler(nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Errorf("без пула: %d %q, want 200 %q", rec.Code, rec.Body.String(), "ok\n")
	}

	// Пул на гарантированно закрытый порт: Ping внутри пробы падает
	// быстрее секундного таймаута хендлера.
	cfg, err := pgxpool.ParseConfig("postgres://nobody@127.0.0.1:1/crawler?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.ConnectTimeout = 300 * time.Millisecond
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	rec = httptest.NewRecorder()
	healthzHandler(pool).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("с недоступной БД: %d %q, want 503", rec.Code, rec.Body.String())
	}
}

// withMigrateUp подменяет применение миграций (шов migrateUp) и
// возвращает функцию восстановления.
func withMigrateUp(f func(context.Context, string) error) func() {
	old := migrateUp
	migrateUp = f
	return func() { migrateUp = old }
}

// TestRunSignalDuringMigrationWait — SIGTERM в фазе ожиданий миграций
// (подключение к БД, advisory-блокировка) прерывает ожидание по
// контексту и завершает сервис штатно: Run = nil, OnRun не запускался.
// Обработчик обязан стоять раньше миграций: дефолтная реакция на сигнал
// убила бы процесс здесь.
func TestRunSignalDuringMigrationWait(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	started := make(chan struct{})
	restore := withMigrateUp(func(ctx context.Context, _ string) error {
		close(started)
		<-ctx.Done() // фаза ожидания: прерывается отменой migCtx
		return ctx.Err()
	})
	defer restore()

	done := make(chan error, 1)
	go func() {
		done <- Run(Options{
			Name:        "run-test",
			PGDSN:       "postgres://crawler@127.0.0.1:1/crawler", // маршрутизируется в заглушку
			MetricsAddr: "127.0.0.1:0",
			Logger:      newLogger(io.Discard, "run-test", slog.LevelError),
			OnRun: func(context.Context) error {
				t.Error("OnRun не должен запускаться: сигнал пришёл во время миграций")
				return nil
			},
		})
	}()

	<-started
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, хочу nil (штатная остановка по сигналу во время миграций)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не завершился после SIGTERM во время миграций")
	}
}

// TestRunSignalDuringMigrationApply — сигнал в фазе применения не
// прерывает миграцию (golang-migrate не принимает контекст): она
// дорабатывает, затем сервис останавливается штатно, не запуская
// рабочую логику.
func TestRunSignalDuringMigrationApply(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	applied := make(chan struct{})
	restore := withMigrateUp(func(context.Context, string) error {
		if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
			t.Error(err)
		}
		time.Sleep(150 * time.Millisecond) // «применение» продолжается после сигнала
		close(applied)
		return nil
	})
	defer restore()

	done := make(chan error, 1)
	go func() {
		done <- Run(Options{
			Name:        "run-test",
			PGDSN:       "postgres://crawler@127.0.0.1:1/crawler",
			MetricsAddr: "127.0.0.1:0",
			Logger:      newLogger(io.Discard, "run-test", slog.LevelError),
			OnRun: func(context.Context) error {
				t.Error("OnRun не должен запускаться: сигнал пришёл во время миграций")
				return nil
			},
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, хочу nil (миграция доработана, штатная остановка)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не завершился после SIGTERM во время применения миграций")
	}
	select { // миграция реально доработала после сигнала, а не до него
	case <-applied:
	default:
		t.Error("миграция прервана сигналом (должна дорабатывать)")
	}
}

// TestRunSecondSignalForcedExit — повторный сигнал в фазе ожидания OnRun
// возвращает из Run ошибку принудительного выхода (раньше — os.Exit(1)
// прямо в библиотеке: обход defer'ов, ветка не покрывалась тестами).
func TestRunSecondSignalForcedExit(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	started := make(chan struct{})
	stopping := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Run(Options{
			Name:            "run-test",
			MetricsAddr:     "127.0.0.1:0",
			Logger:          newLogger(io.Discard, "run-test", slog.LevelError),
			ShutdownTimeout: 10 * time.Second, // фазу ожидания прерывает второй сигнал
			OnRun: func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				close(stopping)
				block := make(chan struct{}) // «зависает» в остановке
				<-block
				return nil
			},
		})
	}()

	<-started
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	<-stopping
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, errForcedExit) {
			t.Fatalf("Run = %v, хочу errForcedExit", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не завершился после повторного сигнала (os.Exit в библиотеке?)")
	}
}

// TestNewRegistry — выбор реестра метрик: Options.Metrics (создан main-ом
// под приборы рабочей логики, T-077) используется как есть, nil — каркас
// строит собственный по имени сервиса.
func TestNewRegistry(t *testing.T) {
	own := metrics.New("workers-svc")
	if got := newRegistry(Options{Metrics: own}); got != own {
		t.Errorf("newRegistry с Options.Metrics = %v, хочу тот же реестр", got)
	}

	reg := newRegistry(Options{Name: "plain-svc"})
	if reg == nil {
		t.Fatal("newRegistry без Metrics = nil, хочу собственный реестр каркаса")
	}
	// Собственный реестр каркаса инициализирован как metrics.New:
	// прибор рабочей логики регистрируется фабрикой и виден в Gather.
	tasks := reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}, metrics.LabelStage)
	tasks.WithLabelValues(metrics.StageDNS).Inc()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	found := false
	for _, mf := range families {
		if mf.GetName() == "crawler_stage_tasks_total" {
			found = true
		}
	}
	if !found {
		t.Error("прибор рабочей логики не попал в Gather реестра каркаса")
	}
}

// TestRunWithOwnMetrics — Run принимает готовый реестр Options.Metrics
// (T-077): сервис поднимается и завершается чисто, реестр остаётся
// пригодным для приборов рабочей логики.
func TestRunWithOwnMetrics(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	reg := metrics.New("run-test")
	reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_stage_tasks_total",
		Help: "Обработанные домены по этапам конвейера.",
	}, metrics.LabelStage).WithLabelValues(metrics.StageDNS).Inc()

	err := Run(Options{
		Name:        "run-test",
		MetricsAddr: "127.0.0.1:0",
		Logger:      newLogger(io.Discard, "run-test", slog.LevelError),
		Metrics:     reg,
		OnRun:       func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("Run с Options.Metrics = %v, хочу nil", err)
	}
}

// TestRunLifecycle — полный путь Run: инициализация, HTTP на свободном
// порту, завершение OnRun без ошибки — nil; с ошибкой — она же наружу.
func TestRunLifecycle(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)
	logger := newLogger(io.Discard, "run-test", slog.LevelError)

	err := Run(Options{
		Name:        "run-test",
		MetricsAddr: "127.0.0.1:0",
		Logger:      logger,
		OnRun:       func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("Run (чистое завершение OnRun) = %v, хочу nil", err)
	}

	boom := errors.New("сбой воркера")
	err = Run(Options{
		Name:        "run-test",
		MetricsAddr: "127.0.0.1:0",
		Logger:      logger,
		OnRun:       func(context.Context) error { return boom },
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run (ошибка OnRun) = %v, хочу исходную ошибку", err)
	}
}

// TestRunExtraMux — дополнительные хендлеры регистрируются до старта
// рабочей логики (OnRun видит готовый mux-сервер).
func TestRunExtraMux(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	registered := make(chan struct{}, 1)
	err := Run(Options{
		Name:        "run-test",
		MetricsAddr: "127.0.0.1:0",
		Logger:      newLogger(io.Discard, "run-test", slog.LevelError),
		ExtraMux: func(mux *http.ServeMux) {
			mux.HandleFunc("/extra", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("extra"))
			})
			registered <- struct{}{}
		},
		OnRun: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("Run с ExtraMux = %v, хочу nil", err)
	}
	select {
	case <-registered:
	default:
		t.Error("ExtraMux не вызван при инициализации HTTP-сервера")
	}
}

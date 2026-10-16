package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"crawler/internal/config"
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

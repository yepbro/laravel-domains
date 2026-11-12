package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	"crawler/internal/migrate"
)

// ---- Logging ----

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
	logger := newLogger(&buf, "svc", logLevel("warn")) // the level from the config value
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
	// A nil config (a service without config.Load) — the info level.
	if got := NewLogger("svc", nil).Enabled(context.Background(), slog.LevelInfo); !got {
		t.Error("NewLogger(nil-конфиг) должен включать уровень info")
	}

	// The level comes from the configuration: warn lets Warn through and
	// cuts Info.
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

// NewLoggerForTest — NewLogger with a substituted sink (stderr → a buffer).
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
		"WARN":        slog.LevelWarn, // the case is normalized in config.normalize
		" debug ":     slog.LevelDebug,
		"трассировка": slog.LevelInfo, // unreachable after config validation
	}
	for in, want := range cases {
		if got := logLevel(in); got != want {
			t.Errorf("logLevel(%q) = %v, хочу %v", in, got, want)
		}
	}
}

// ---- Lifecycle (serve) ----

// recorder — a thread-safe event journal for checking the order of the
// graceful shutdown (spec 3.4: the working logic → the HTTP metrics → the
// DB pool).
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

// waitFor waits for the event to appear: serve works in goroutines.
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

// lifecycle — one serve run: the signal channels, the result, the events.
type lifecycle struct {
	rec      *recorder
	finished chan struct{} // closing = serve returned; the result is in err
	err      error
	term     chan os.Signal
	hup      chan os.Signal
	addr     string       // the address of the metrics HTTP server (127.0.0.1:port)
	ln       net.Listener // for simulating an HTTP server failure
}

// failHTTP simulates an HTTP server failure: closing the listener from
// under Serve.
func (lc *lifecycle) failHTTP(t *testing.T) {
	t.Helper()
	if err := lc.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("закрытие listener: %v", err)
	}
}

// startServe starts serve with the test Options; closePool records the
// closing of the DB pool, handler — the metrics HTTP handlers of the
// test, rec — the event journal of the test (nil — its own).
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

// TestServeShutdownOrder — §3.4: the signal cancels the OnRun context,
// serve waits for the working logic to finish, the HTTP server waits for
// the started request, the DB pool is closed last.
func TestServeShutdownOrder(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, _ *http.Request) {
		rec.add("http-start")
		time.Sleep(400 * time.Millisecond) // the request is in flight at the signal moment
		rec.add("http-done")
		_, _ = w.Write([]byte("ok"))
	})
	lc := startServe(t, Options{
		Name: "test",
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done() // "finishing the current task" on the context cancellation
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
	// http-start happened before the signal; then — the strict order §3.4.
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

// TestServeOnRunCleanExit — an independent OnRun completion without a
// signal stops the service (a one-off task); the resources are closed.
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

// TestServeOnRunError — a working logic error stops the service
// and is returned to the caller (the exit code 1).
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

// TestServeShutdownTimeout — the working logic that did not stop within
// the timeout does not block the shutdown forever: the HTTP and the pool
// are closed; serve returns the timeout error.
func TestServeShutdownTimeout(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name:            "test",
		ShutdownTimeout: 60 * time.Millisecond,
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done()
			time.Sleep(500 * time.Millisecond) // ignores the cancellation
			rec.add("late")
			return nil
		},
	}, http.NewServeMux(), rec)

	<-ready
	start := time.Now()
	lc.term <- syscall.SIGTERM
	err := lc.waitDone(t)
	if err == nil || !strings.Contains(err.Error(), "did not stop") {
		t.Fatalf("serve = %v, хочу ошибку таймаута остановки", err)
	}
	if elapsed := time.Since(start); elapsed > 450*time.Millisecond {
		t.Fatalf("остановка заняла %s — таймаут не сработал", elapsed)
	}
	if !rec.has("pool-closed") {
		t.Errorf("пул должен закрываться и после таймаута; события: %v", rec.snapshot())
	}
}

// TestServeSecondSignalForcesExit — a repeat signal in the OnRun waiting
// phase interrupts the graceful shutdown (errForcedExit) without closing
// the resources.
func TestServeSecondSignalForcesExit(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name:            "test",
		ShutdownTimeout: 10 * time.Second, // we wait for OnRun long — interrupt with a signal
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done()
			rec.add("stopping")          // serve entered the shutdown phase
			block := make(chan struct{}) // "stuck": does not finish on cancellation
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

// TestServeSIGHUP — SIGHUP before the shutdown signal reaches OnSIGHUP.
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

// TestServeHTTPServerError — an HTTP server failure stops the service
// with an error; the working logic finishes by the context cancellation;
// the resources are closed (compose will restart the service per the
// restart policy).
func TestServeHTTPServerError(t *testing.T) {
	rec := &recorder{}
	ready := make(chan struct{})
	lc := startServe(t, Options{
		Name: "test",
		OnRun: func(ctx context.Context) error {
			close(ready)
			<-ctx.Done() // finishing on the cancellation after the HTTP error
			rec.add("onrun-done")
			return nil
		},
	}, http.NewServeMux(), rec)

	<-ready
	lc.failHTTP(t) // a "server failure": closing the listener from under Serve

	err := lc.waitDone(t)
	if err == nil || !strings.Contains(err.Error(), "metrics HTTP server") {
		t.Fatalf("serve = %v, хочу ошибку HTTP-сервера", err)
	}
	want := []string{"onrun-done", "pool-closed"}
	got := rec.snapshot()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("события = %v, хочу %v", got, want)
	}
}

// TestServeOnRunPanic — a panic of the working logic does not bring the
// process down: serve returns it as an error, the resources are closed
// (the graceful path).
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
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("serve = %v, хочу ошибку паники OnRun", err)
	}
	if !lc.rec.has("pool-closed") {
		t.Errorf("пул должен закрываться и при панике; события: %v", lc.rec.snapshot())
	}
}

// ---- Run ----

// TestRunValidation — the service name is mandatory.
func TestRunValidation(t *testing.T) {
	if err := Run(Options{}); err == nil || !strings.Contains(err.Error(), "service name") {
		t.Fatalf("Run без Name = %v, хочу ошибку об обязательном имени", err)
	}
}

// newDeadPool — a pool to a guaranteed closed port for the readiness
// checks: the Ping inside the probe fails faster than its one-second
// timeout.
func newDeadPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://nobody@127.0.0.1:1/crawler?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.ConnectTimeout = 300 * time.Millisecond
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestHealthzHandler — /healthz reflects the dependency readiness:
// without a DB pool (a service without OnDBPool/DSN) — 200; with a pool to
// an unavailable DB — 503: the PostgreSQL failure is visible to the
// readiness probe (the consumer is the operator/monitoring; the compose
// healthchecks of the application services since T-110 check /livez).
func TestHealthzHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	healthzHandler(nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Errorf("без пула: %d %q, want 200 %q", rec.Code, rec.Body.String(), "ok\n")
	}

	pool := newDeadPool(t)

	rec = httptest.NewRecorder()
	healthzHandler(pool).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("с недоступной БД: %d %q, want 503", rec.Code, rec.Body.String())
	}
}

// TestLivezHandler — the liveness probe always answers 200 while the
// process is alive: it does not touch the dependencies either structurally
// (a handler without parameters) or by meaning — an unavailable DB/Redis
// is not a reason to consider the process dead and demand its restart.
func TestLivezHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	livezHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Errorf("livez: %d %q, want 200 %q", rec.Code, rec.Body.String(), "ok\n")
	}
}

// TestRegisterProbes — the probe routing on the shared mux (the Run path):
// the key T-110 separation — a live process with a dead DB answers /livez
// 200 (liveness) and /healthz 503 (readiness); the probes do not occupy
// foreign routes.
func TestRegisterProbes(t *testing.T) {
	pool := newDeadPool(t)

	mux := http.NewServeMux()
	registerProbes(mux, pool)

	get := func(path string) int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	if got := get("/livez"); got != http.StatusOK {
		t.Errorf("/livez = %d, want 200 (liveness не зависит от БД)", got)
	}
	if got := get("/healthz"); got != http.StatusServiceUnavailable {
		t.Errorf("/healthz = %d, want 503 (readiness: БД недоступна)", got)
	}
	if got := get("/metrics"); got != http.StatusNotFound {
		t.Errorf("/metrics = %d, want 404 (the probes do not occupy foreign routes)", got)
	}
	if got := get("/nope"); got != http.StatusNotFound {
		t.Errorf("/nope = %d, want 404", got)
	}
}

// withMigrateUp substitutes the migration application (the migrateUp
// seam) and returns the restore function.
func withMigrateUp(f func(context.Context, string) error) func() {
	old := migrateUp
	migrateUp = f
	return func() { migrateUp = old }
}

// TestRunSignalDuringMigrationWait — SIGTERM in the migration waiting
// phase (the DB connection, the advisory lock) interrupts the wait by the
// context and finishes the service normally: Run = nil, OnRun was not
// started. The handler must be installed before the migrations: the
// default signal reaction would kill the process here.
func TestRunSignalDuringMigrationWait(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	started := make(chan struct{})
	restore := withMigrateUp(func(ctx context.Context, _ string) error {
		close(started)
		<-ctx.Done() // the waiting phase: interrupted by the migCtx cancellation
		return ctx.Err()
	})
	defer restore()

	done := make(chan error, 1)
	go func() {
		done <- Run(Options{
			Name:        "run-test",
			PGDSN:       "postgres://crawler@127.0.0.1:1/crawler", // routed into the stub
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

// TestRunSignalDuringMigrationInterrupted — a signal in the application
// phase interrupts the migration on the PostgreSQL side (T-112):
// migrate.Up returns ErrInterrupted (the migration rolled back, the
// version is the last applied one); the service finishes normally (nil)
// without starting the working logic.
func TestRunSignalDuringMigrationInterrupted(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	started := make(chan struct{})
	restore := withMigrateUp(func(ctx context.Context, _ string) error {
		close(started)
		<-ctx.Done() // "the application": cancelled by the signal via pg_cancel_backend
		return fmt.Errorf("%w: миграция 8 отменена и откатана: %w",
			migrate.ErrInterrupted, errors.New("canceling statement due to user request"))
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

	<-started
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, хочу nil (миграция прервана и откатана — штатная остановка)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не завершился после SIGTERM во время применения миграций")
	}
}

// TestRunSignalDuringMigrationApply — the fallback path when cancelling
// the in-flight migration is impossible (a non-URL DSN, the DB
// unavailable): the migration runs to the end (C-04), then the service
// stops normally, without starting the working logic.
func TestRunSignalDuringMigrationApply(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	applied := make(chan struct{})
	restore := withMigrateUp(func(context.Context, string) error {
		if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
			t.Error(err)
		}
		time.Sleep(150 * time.Millisecond) // "the application" continues after the signal
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
	select { // the migration really ran to the end after the signal, not before it
	case <-applied:
	default:
		t.Error("миграция прервана сигналом (должна дорабатывать)")
	}
}

// TestRunSecondSignalForcedExit — a repeat signal in the OnRun waiting
// phase returns the forced exit error from Run (previously — os.Exit(1)
// right in the library: bypassing the defers, the branch was not covered
// by tests).
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
			ShutdownTimeout: 10 * time.Second, // the waiting phase is interrupted by the second signal
			OnRun: func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				close(stopping)
				block := make(chan struct{}) // "hangs" in the shutdown
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

// TestNewRegistry — the metrics registry choice: Options.Metrics (created
// by main for the instruments of the working logic, T-077) is used as is;
// nil — the framework builds its own by the service name.
func TestNewRegistry(t *testing.T) {
	own := metrics.New("workers-svc")
	if got := newRegistry(Options{Metrics: own}); got != own {
		t.Errorf("newRegistry с Options.Metrics = %v, хочу тот же реестр", got)
	}

	reg := newRegistry(Options{Name: "plain-svc"})
	if reg == nil {
		t.Fatal("newRegistry без Metrics = nil, хочу собственный реестр каркаса")
	}
	// The framework's own registry is initialized as metrics.New: the
	// instrument of the working logic is registered by the factory and
	// visible in Gather.
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

// TestRunWithOwnMetrics — Run accepts the ready Options.Metrics registry
// (T-077): the service comes up and finishes cleanly, the registry stays
// fit for the instruments of the working logic.
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

// TestRunLifecycle — the full Run path: the initialization, the HTTP on a
// free port, an OnRun completion without an error — nil; with an error —
// it comes out as is.
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

// TestRunExtraMux — the additional handlers are registered before the
// working logic starts (OnRun sees the ready mux server).
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

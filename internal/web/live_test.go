// Tests of the live check circuit of the form (T-220): the newLiveCircuit
// seam is substituted with a fake — it verifies that concurrent form
// checks assemble the circuit exactly once per service (the invariant
// "fds do not grow with the number of requests": before T-220 each
// request built its own resolver chain with udp_pool_size pools and its
// own Fetcher), and that the web.live_concurrency semaphore cuts
// concurrent pipelines with a fast 503 with Retry-After, not a queue up
// to web.timeout. A direct fd count is not done (nondeterministic in a
// common test runner) — the circuit assembly counter is enough.

package web

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crawler/internal/checker"
	"crawler/internal/config"
)

// fakeCircuit — a fake circuit: Run delegates to a programmable
// function, Close is counted (how many times the service closed the
// circuit).
type fakeCircuit struct {
	run    checker.LiveFunc
	closes atomic.Int32
}

func (f *fakeCircuit) Run(ctx context.Context, cfg *config.Config, pool checker.Pool, domain string, log checker.Logger) (*checker.Result, error) {
	return f.run(ctx, cfg, pool, domain, log)
}

func (f *fakeCircuit) Close() error { f.closes.Add(1); return nil }

// installCircuitSeam — substitutes the circuit assembly seam, returns
// the assembly counter, and restores the production function at the end
// of the test.
func installCircuitSeam(t *testing.T, factory func() checker.LiveCircuit) *atomic.Int32 {
	t.Helper()
	var builds atomic.Int32
	prev := newLiveCircuit
	newLiveCircuit = func(context.Context, *config.Config, checker.Logger) (checker.LiveCircuit, error) {
		builds.Add(1)
		return factory(), nil
	}
	t.Cleanup(func() { newLiveCircuit = prev })
	return &builds
}

// postCheck — a concurrent POST of the form; returns the status and the
// body.
func postCheck(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Post(url+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"example.com"}`))
	if err != nil {
		t.Fatalf("POST формы: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	return resp.StatusCode, string(body[:n])
}

// TestLiveCircuitSingleUnderLoad — the load criterion of T-220: 10
// concurrent checks on the production path (liveCheck not substituted,
// the circuit assembled lazily by the seam) build the circuit exactly
// once — the sockets of the resolver chain and the Fetcher are opened
// once per service, not a chain per request. live_concurrency is raised
// to 10 so that the semaphore does not interfere with the measurement
// (the semaphore mechanism — a separate test below); along the way,
// /healthz answers 200 while the form is busy checking.
func TestLiveCircuitSingleUnderLoad(t *testing.T) {
	cfg := config.Default()
	cfg.Web.RateLimit = 100
	cfg.Web.LiveConcurrency = 10 // the test seam: no semaphore rejections
	db := &fakeDB{seed: seedLanding()}

	builds := installCircuitSeam(t, func() checker.LiveCircuit {
		return &fakeCircuit{run: func(_ context.Context, _ *config.Config, _ checker.Pool, domain string, _ checker.Logger) (*checker.Result, error) {
			time.Sleep(50 * time.Millisecond) // the concurrency window of the checks
			return &checker.Result{Domain: domain, Zone: "com", Apex: "delegated"}, nil
		}}
	})

	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	postsDone := make(chan struct{})
	var (
		wg       sync.WaitGroup
		statuses = make([]int, 10)
		healthz  atomic.Int32 // probes OK under load
	)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			statuses[n], _ = postCheck(t, ts.URL)
		}(i)
	}
	go func() {
		for {
			select {
			case <-postsDone:
				return
			default:
			}
			resp, err := http.Get(ts.URL + "/healthz")
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					healthz.Add(1)
				} else {
					t.Errorf("healthz под нагрузкой: %d, хочу 200", resp.StatusCode)
				}
			}
		}
	}()
	wg.Wait()
	close(postsDone)

	for i, st := range statuses {
		if st != http.StatusOK {
			t.Errorf("запрос %d: статус %d, хочу 200 (семфор 10, контур жив)", i, st)
		}
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("контур собран %d раз на 10 параллельных проверок, хочу 1", got)
	}
	if healthz.Load() == 0 {
		t.Error("пробы /healthz не успели отработать за окно нагрузки — тест не доказал живость healthz")
	}

	// Stopping the service closes the circuit exactly once and forever.
	srv.closeLive()
	if got := builds.Load(); got != 1 {
		t.Fatalf("после closeLive контур пересобран (%d сборок)", got)
	}
}

// TestLiveSemaphoreOverload — the semaphore of concurrent live checks
// (deterministically, without timing windows): two slots are taken by
// blocked pipelines (the entered signal), the next five requests must
// get a fast 503 {"error":"busy"} with Retry-After — not wait for a
// slot up to web.timeout; after the release, the slot holders answer
// 200.
func TestLiveSemaphoreOverload(t *testing.T) {
	cfg := config.Default()
	cfg.Web.RateLimit = 100
	cfg.Web.LiveConcurrency = 2
	db := &fakeDB{seed: seedLanding()} // no domainInfo: no stored part either

	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	var inFlight, maxInFlight atomic.Int32
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		entered <- struct{}{}
		cur := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		return &checker.Result{Domain: "example.com"}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Two pipelines take both slots and wait for the release.
	slow := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			st, _ := postCheck(t, ts.URL)
			slow <- st
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("конвейер не занял слот за 3 с")
		}
	}

	// Five requests on top of the busy slots — each gets a fast 503 busy.
	var extra sync.WaitGroup
	extraStatuses := make([]int, 5)
	extraBodies := make([]string, 5)
	for i := 0; i < 5; i++ {
		extra.Add(1)
		go func(n int) {
			defer extra.Done()
			extraStatuses[n], extraBodies[n] = postCheck(t, ts.URL)
		}(i)
	}
	extra.Wait()
	for i, st := range extraStatuses {
		if st != http.StatusServiceUnavailable {
			t.Errorf("запрос поверх занятых слотов: %d, хочу 503", st)
			continue
		}
		if !strings.Contains(extraBodies[i], `"busy"`) {
			t.Errorf("тело 503 без error:busy: %s", extraBodies[i])
		}
	}

	// Release: the slot holders finish with 200, the cap is not breached.
	close(release)
	for i := 0; i < 2; i++ {
		if st := <-slow; st != http.StatusOK {
			t.Errorf("занявший слот запрос: %d, хочу 200", st)
		}
	}
	if got := maxInFlight.Load(); got > 2 {
		t.Fatalf("одновременно внутри конвейера %d, лимит 2", got)
	}
}

// TestLiveSemaphoreRetryAfter — the overload 503 carries Retry-After
// (the client knows when to come back), the 200 does not.
func TestLiveSemaphoreRetryAfter(t *testing.T) {
	cfg := config.Default()
	cfg.Web.RateLimit = 100
	cfg.Web.LiveConcurrency = 1
	srv, err := New(&fakeDB{seed: seedLanding()}, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		entered <- struct{}{}
		<-release
		return &checker.Result{Domain: "example.com"}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	first := make(chan int, 1)
	go func() {
		st, _ := postCheck(t, ts.URL)
		first <- st
	}()
	// The first request takes the slot for sure (the entered signal) —
	// only after that the second one must get a fast 503, without
	// hanging in the queue up to web.timeout and without grabbing the
	// slot itself.
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("первый запрос не вошёл в слот за 3 с")
	}
	resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"example.com"}`))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 256)
	n, _ := resp.Body.Read(body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("второй запрос при занятом слоте: %d, хочу 503", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra == "" {
		t.Error("503 перегрузки без Retry-After")
	}
	// A JSON body with a correct Content-Type (T-227, the unification
	// with 422).
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("503 Content-Type = %q, хочу application/json", ct)
	}
	if !strings.Contains(string(body[:n]), `"busy"`) {
		t.Errorf("тело 503: %s", string(body[:n]))
	}
	close(release)
	if st := <-first; st != http.StatusOK {
		t.Fatalf("первый (в слоте) запрос: %d, хочу 200", st)
	}
}

// TestLiveCircuitBuildRetriedAfterFailure — a circuit assembly error is
// not remembered (web.go liveFunc): the first request gets a 502
// check_failed, the next one assembles the circuit again — rules/
// sockets may have appeared.
func TestLiveCircuitBuildRetriedAfterFailure(t *testing.T) {
	cfg := config.Default()
	cfg.Web.RateLimit = 100
	srv, err := New(&fakeDB{seed: seedLanding()}, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var builds atomic.Int32
	prev := newLiveCircuit
	newLiveCircuit = func(context.Context, *config.Config, checker.Logger) (checker.LiveCircuit, error) {
		if builds.Add(1) == 1 {
			return nil, errors.New("правила ещё не разложены")
		}
		return &fakeCircuit{run: func(_ context.Context, _ *config.Config, _ checker.Pool, domain string, _ checker.Logger) (*checker.Result, error) {
			return &checker.Result{Domain: domain}, nil
		}}, nil
	}
	t.Cleanup(func() { newLiveCircuit = prev })

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// The first request (the assembly fails) — a 502 check_failed with a
	// JSON body and a correct Content-Type (T-227): http.Error used to
	// serve JSON with text/plain.
	resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"example.com"}`))
	if err != nil {
		t.Fatal(err)
	}
	firstBody := readBody(t, resp)
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(firstBody, "check_failed") {
		t.Fatalf("первый запрос (сборка упала): %d %s, хочу 502 check_failed", resp.StatusCode, firstBody)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("502 Content-Type = %q, хочу application/json", ct)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("попыток сборки %d после первого запроса, хочу 1", got)
	}
	if st, _ := postCheck(t, ts.URL); st != http.StatusOK {
		t.Fatalf("повторный запрос: %d, хочу 200 (сборка повторена)", st)
	}
	if got := builds.Load(); got != 2 {
		t.Fatalf("попыток сборки %d, хочу 2 (ошибка не запомнена)", got)
	}
}

// TestLiveCircuitClosedNoRebuild — after closeLive (the Run exit, the
// service stop) the circuit is not assembled again: the request gets a
// live check rejection, not a new circuit nobody would close (P3-2 of
// the review).
func TestLiveCircuitClosedNoRebuild(t *testing.T) {
	cfg := config.Default()
	cfg.Web.RateLimit = 100
	srv, err := New(&fakeDB{seed: seedLanding()}, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	builds := installCircuitSeam(t, func() checker.LiveCircuit {
		return &fakeCircuit{run: func(_ context.Context, _ *config.Config, _ checker.Pool, domain string, _ checker.Logger) (*checker.Result, error) {
			return &checker.Result{Domain: domain}, nil
		}}
	})

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if st, _ := postCheck(t, ts.URL); st != http.StatusOK {
		t.Fatalf("первый запрос: %d, хочу 200", st)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("контур собран %d раз, хочу 1", got)
	}

	srv.closeLive()
	if st, body := postCheck(t, ts.URL); st != http.StatusBadGateway || !strings.Contains(body, "check_failed") {
		t.Fatalf("запрос после остановки: %d %s, хочу 502 check_failed (без пересборки)", st, body)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("после closeLive контур собран заново (%d сборок), хочу без пересборки", got)
	}
}

// TestRunClosesLiveCircuit — the Run exit (the service stop) closes the
// live check circuit: the sockets of the resolver chain and the render
// do not outlive the service.
func TestRunClosesLiveCircuit(t *testing.T) {
	cfg := config.Default()
	cfg.Web.RateLimit = 100
	srv, err := New(&fakeDB{seed: seedLanding()}, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	circ := &fakeCircuit{run: func(_ context.Context, _ *config.Config, _ checker.Pool, domain string, _ checker.Logger) (*checker.Result, error) {
		return &checker.Result{Domain: domain}, nil
	}}
	builds := installCircuitSeam(t, func() checker.LiveCircuit { return circ })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, addr) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("сервер не поднялся за 2 с")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if st, _ := postCheck(t, "http://"+addr); st != http.StatusOK {
		t.Fatalf("проверка формы через Run: %d, хочу 200", st)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("контур собран %d раз, хочу 1", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run после отмены: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("сервер не остановился после отмены ctx")
	}
	if got := circ.closes.Load(); got != 1 {
		t.Fatalf("контур закрыт %d раз на остановке сервиса, хочу 1", got)
	}
}

// Tests of the long-lived live check circuit (runtime.go, T-220).
// No network or DB: the resolver chain is substituted by the
// newResolverChainFn seam (counting the builds and the pool sizes without
// sockets), the DB pool — by a fake with one row of the subdomains
// reference table. Checked: the circuit assembles the chain exactly once
// with a small UDP pool, Run reuses the circuit (parallel checks build no
// new chains), closing the circuit closes the chain exactly once and
// idempotently, the background rules reload stops on Close, the one-off
// Run (CLI) assembles and closes the circuit per call. A direct fd count
// is not done (nondeterministic in a shared test runner): "the fds do not
// grow with the request count" follows from the invariant "only the
// circuit assembly opens sockets; the circuit is one per service".

package checker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/config"
	"crawler/internal/dnsclient"
)

// chainSpy — the chain assembly seam: counts the calls, remembers the
// pool sizes, returns a fake resolver; close counts the chain closings.
type chainSpy struct {
	builds    atomic.Int32
	closes    atomic.Int32
	poolSizes []int
	exchanges atomic.Int32
}

func (s *chainSpy) install(t *testing.T) {
	t.Helper()
	prev := newResolverChainFn
	newResolverChainFn = func(ctx context.Context, cfg config.DNS, udpPoolSize int) (dnsclient.Resolver, func() error, error) {
		s.builds.Add(1)
		s.poolSizes = append(s.poolSizes, udpPoolSize)
		return s, func() error { s.closes.Add(1); return nil }, nil
	}
	t.Cleanup(func() { newResolverChainFn = prev })
}

// Exchange — "no such domain": the apex is NXDOMAIN right away (the
// terminal branch of the pipeline, no HTTP stage); every call is counted.
func (s *chainSpy) Exchange(context.Context, dnsclient.Question) (*dnsclient.Result, error) {
	s.exchanges.Add(1)
	return &dnsclient.Result{RCode: dnsclient.RCodeNXDOMAIN}, nil
}

// --- fake DB pool (the subdomains reference table) -----------------------------

// rowsMini — pgx.Rows with pre-prepared rows (a positional Scan; the
// style of webRows in internal/web).
type rowsMini struct {
	rows [][]any
	i    int
}

func (r *rowsMini) Close()                        {}
func (r *rowsMini) Err() error                    { return nil }
func (r *rowsMini) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *rowsMini) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *rowsMini) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *rowsMini) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) || len(dest) != len(r.rows[r.i-1]) {
		return fmt.Errorf("rowsMini: Scan без Next или арность %d ≠ колонок", len(dest))
	}
	for i, d := range dest {
		switch dst := d.(type) {
		case *int16:
			*dst = r.rows[r.i-1][i].(int16)
		case *string:
			*dst = r.rows[r.i-1][i].(string)
		case *bool:
			*dst = r.rows[r.i-1][i].(bool)
		default:
			return fmt.Errorf("rowsMini: приёмник[%d] %T", i, d)
		}
	}
	return nil
}

func (r *rowsMini) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *rowsMini) RawValues() [][]byte    { return nil }
func (r *rowsMini) Conn() *pgx.Conn        { return nil }
func (r *rowsMini) TypeMap() *pgtype.Map   { return nil }

// poolFake — a db.DB for LoadSubdomains: one row (the apex, enabled).
type poolFake struct{}

func (poolFake) Ping(context.Context) error { return nil }
func (poolFake) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не используется живой проверкой")
}
func (poolFake) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не используется живой проверкой")
}
func (poolFake) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &rowsMini{rows: [][]any{{int16(0), "", true}}}, nil
}

// testCfg — a circuit config without a network: the rules in a temp
// directory, the render disabled.
func testCfg(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Analyzer.RulesDir = writeRules(t, "rule_one")
	return cfg
}

// writeRules — a minimal valid rule set (a changed id changes
// rules_version — for the reload test).
func writeRules(t *testing.T, id string) string {
	t.Helper()
	dir := t.TempDir()
	yaml := "rules:\n  - id: " + id + "\n    target: cookie_name\n    match: laravel_session\n    weight: 0.3\n"
	if err := os.WriteFile(filepath.Join(dir, "live.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestNewRuntimeSmallPoolOnce — the circuit assembles the chain exactly
// once with a small UDP pool (LiveUDPPoolSize by default, an explicit
// value as is); the render is not created without render.remote_url.
func TestNewRuntimeSmallPoolOnce(t *testing.T) {
	spy := &chainSpy{}
	spy.install(t)

	rt, err := NewRuntime(context.Background(), testCfg(t), nil, RuntimeOptions{RulesReload: -1})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	defer func() { _ = rt.Close() }()
	if got := spy.builds.Load(); got != 1 {
		t.Fatalf("цепь собрана %d раз, ожидалась 1 (на NewRuntime)", got)
	}
	if len(spy.poolSizes) != 1 || spy.poolSizes[0] != LiveUDPPoolSize {
		t.Fatalf("пул цепи = %v, ожидался [%d] (малый пул живой проверки)", spy.poolSizes, LiveUDPPoolSize)
	}
	if rt.renderer != nil {
		t.Error("render.Manager создан при пустом render.remote_url")
	}
}

// TestNewRuntimeExplicitPool — an explicit UDPPoolSize reaches the clients
// of the chain (the owner may narrow the pool further than the norm).
func TestNewRuntimeExplicitPool(t *testing.T) {
	spy := &chainSpy{}
	spy.install(t)

	rt, err := NewRuntime(context.Background(), testCfg(t), nil, RuntimeOptions{RulesReload: -1, UDPPoolSize: 8})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	defer func() { _ = rt.Close() }()
	if len(spy.poolSizes) != 1 || spy.poolSizes[0] != 8 {
		t.Fatalf("пул цепи = %v, ожидался [8]", spy.poolSizes)
	}
}

// TestRuntimeRunReusesCircuit — the load invariant (T-220): 10 parallel
// checks on one circuit build no new chains — all the network assembly
// (the UDP sockets) is done once at NewRuntime; Run only runs the
// pipeline. Every Run reaches the DNS exchange of the fake (an NXDOMAIN
// apex — the terminal branch without HTTP) and finishes without an
// error.
func TestRuntimeRunReusesCircuit(t *testing.T) {
	spy := &chainSpy{}
	spy.install(t)

	rt, err := NewRuntime(context.Background(), testCfg(t), nil, RuntimeOptions{RulesReload: -1})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	defer func() { _ = rt.Close() }()

	cfg := testCfg(t)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			res, err := rt.Run(context.Background(), cfg, poolFake{}, fmt.Sprintf("example%d.com", n), nil)
			if err != nil {
				t.Errorf("Run %d: %v", n, err)
				return
			}
			if res.Terminal == "" {
				t.Errorf("Run %d: ожидалась терминальная ветка NXDOMAIN, итог %+v", n, res)
			}
		}(i)
	}
	wg.Wait()

	if got := spy.builds.Load(); got != 1 {
		t.Fatalf("после 10 параллельных Run цепь собрана %d раз, ожидалась 1", got)
	}
	if got := spy.exchanges.Load(); got != 10 {
		t.Fatalf("DNS-обменов %d, ожидалось 10 (по одному апексу на Run)", got)
	}
}

// TestRuntimeCloseClosesChainOnce — Close closes the chain exactly once
// and is idempotent (protection against a double Close by the owner and
// a delayed Run).
func TestRuntimeCloseClosesChainOnce(t *testing.T) {
	spy := &chainSpy{}
	spy.install(t)

	rt, err := NewRuntime(context.Background(), testCfg(t), nil, RuntimeOptions{RulesReload: -1})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("повторный Close: %v", err)
	}
	if got := spy.closes.Load(); got != 1 {
		t.Fatalf("цепь закрыта %d раз, ожидался 1", got)
	}
}

// TestRunOneShotBuildsAndCloses — the free Run function (the CLI
// crawlerctl check): a circuit for one call — the chain assembled and
// closed once each, without the background rules reload.
func TestRunOneShotBuildsAndCloses(t *testing.T) {
	spy := &chainSpy{}
	spy.install(t)

	if _, err := Run(context.Background(), testCfg(t), poolFake{}, "example.com", nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := spy.builds.Load(); got != 1 {
		t.Fatalf("цепь собрана %d раз, ожидалась 1", got)
	}
	if got := spy.closes.Load(); got != 1 {
		t.Fatalf("цепь закрыта %d раз, ожидался 1 (defer Close)", got)
	}
}

// TestRuntimeRulesReload — the background reload (T-220): a set change on
// the disk is picked up by the timer without rebuilding the circuit;
// Close stops the loop (tickDone closed).
func TestRuntimeRulesReload(t *testing.T) {
	spy := &chainSpy{}
	spy.install(t)

	dir := writeRules(t, "rule_one")
	cfg := config.Default()
	cfg.Analyzer.RulesDir = dir

	rt, err := NewRuntime(context.Background(), cfg, nil, RuntimeOptions{RulesReload: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if got := rt.rules.Set().Rules[0].ID; got != "rule_one" {
		t.Fatalf("начальный набор: %q, ожидался rule_one", got)
	}

	// A set change: another id → another rules_version.
	yaml := "rules:\n  - id: rule_two\n    target: cookie_name\n    match: app_session\n    weight: 0.2\n"
	if err := os.WriteFile(filepath.Join(dir, "live.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for rt.rules.Set().Rules[0].ID == "rule_one" {
		if time.Now().After(deadline) {
			t.Fatal("фоновая перезагрузка не подхватила новый набор за 3 с")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rt.rules.Set().Rules[0].ID != "rule_two" {
		t.Fatalf("после перезагрузки набор %q, ожидался rule_two", rt.rules.Set().Rules[0].ID)
	}
	if got := spy.builds.Load(); got != 1 {
		t.Fatalf("перезагрузка правил пересобрала цепь (%d сборок)", got)
	}

	// Close stops the reload loop.
	if err := rt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-rt.tickDone:
	default:
		t.Fatal("фоновый цикл перезагрузки не остановился после Close")
	}
}

// TestNewRuntimeRendererConfigured — render.remote_url configures the
// manager on the circuit (no Start: the browser generation is raised
// lazily by the first render — the start does not touch the network and
// does not degrade the service).
func TestNewRuntimeRendererConfigured(t *testing.T) {
	spy := &chainSpy{}
	spy.install(t)

	cfg := testCfg(t)
	cfg.Render.RemoteURL = "ws://127.0.0.1:19222"
	rt, err := NewRuntime(context.Background(), cfg, nil, RuntimeOptions{RulesReload: -1})
	if err != nil {
		t.Fatalf("NewRuntime с render.remote_url: %v", err)
	}
	defer func() { _ = rt.Close() }()
	if rt.renderer == nil {
		t.Fatal("render.Manager не создан при настроенном render.remote_url")
	}
}

// TestNewRuntimeBadRules — a failure of the initial rules load: the
// circuit is not created, the partially assembled resources (the chain)
// are closed — no leak.
func TestNewRuntimeBadRules(t *testing.T) {
	spy := &chainSpy{}
	spy.install(t)

	cfg := config.Default()
	cfg.Analyzer.RulesDir = filepath.Join(t.TempDir(), "нет-такого")
	if _, err := NewRuntime(context.Background(), cfg, nil, RuntimeOptions{RulesReload: -1}); err == nil {
		t.Fatal("NewRuntime без каталога правил = nil error, хочу ошибку")
	}
	if got := spy.closes.Load(); got != 1 {
		t.Fatalf("цепь закрыта %d раз при сбое сборки, ожидался 1 (без утечки)", got)
	}
}

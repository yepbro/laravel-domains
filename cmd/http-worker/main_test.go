// The unit tests of the retention guard adapter (retentionPending,
// T-166/T-167): reading the unconsumed tail of the streams without a live
// Redis (a fake with the prepared XINFO GROUPS/XRANGE, the fakeBackpressure
// style in internal/queue): the lower edge of XRANGE — the last-delivered-id
// of the group exclusively, no groups — the whole stream from the
// beginning, no stream — an empty set, the pagination of the tail, the
// errors, the foreign bodies.
// Right here — the effective capacity threshold of q:render
// (renderQueueWatermark, T-174): the fallback "0 — the common
// scheduler.queue_high_watermark" and the MAXLEN/2 clamp
// (queue.StreamWatermark); and the wiring of the AfterConnect hooks of
// the service pool (afterConnectHooks, T-201): NoParallelGather — the
// seriality of the retention drain + LockTimeout (T-192); and the gate of
// the heavy background scans (scanGate, T-271): retention ↔ render-late
// must not coincide in PostgreSQL; and the structural shutdown invariant
// of run (TestRunBackgroundJoin, T-223): the derived cancellation + a
// WaitGroup — every background loop Add/Done, the join-defer executes
// before the defer-closes of the resources (the checker — the shared
// internal/bgjoin, T-232).
package main

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"crawler/internal/bgjoin"
	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/queue"
)

// fakePendingRedis — the window of the adapter with the stubs: groups —
// the response of XINFO GROUPS, xrange — the XRANGE pages by the lower
// edge (nil — the tail is empty).
type fakePendingRedis struct {
	groups    []redis.XInfoGroup
	groupsErr error
	xrange    func(start string) ([]redis.XMessage, error)

	froms []string // the lower edges of XRANGE in order
}

// compile-time: the fake is interchangeable with the real client in the
// adapter window.
var _ retentionPendingRedis = (*fakePendingRedis)(nil)

func (f *fakePendingRedis) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	cmd := redis.NewXInfoGroupsCmd(ctx, key)
	if f.groupsErr != nil {
		cmd.SetErr(f.groupsErr)
	} else {
		cmd.SetVal(f.groups)
	}
	return cmd
}

func (f *fakePendingRedis) XRangeN(ctx context.Context, key, start, stop string, count int64) *redis.XMessageSliceCmd {
	f.froms = append(f.froms, start)
	cmd := redis.NewXMessageSliceCmd(ctx, key, start, stop, count)
	if f.xrange == nil {
		cmd.SetVal(nil)
		return cmd
	}
	msgs, err := f.xrange(start)
	if err != nil {
		cmd.SetErr(err)
	} else {
		cmd.SetVal(msgs)
	}
	return cmd
}

// pendingMsg — a stream record with a Result message (the format of
// T-024).
func pendingMsg(id string, zone int16, domain int64, sub, variant int16) redis.XMessage {
	return redis.XMessage{
		ID: id,
		Values: map[string]any{"data": fmt.Sprintf(
			`{"task_id":"t","zone_id":%d,"domain_id":%d,"subdomain_id":%d,"variant":%d}`,
			zone, domain, sub, variant)},
	}
}

// wantKey — the expected guard key.
func wantKey(zone int16, domain int64, sub, variant int16) content.RetentionPendingKey {
	return content.RetentionPendingKey{Zone: zone, Domain: domain, Sub: sub, Variant: variant}
}

// TestPendingKeys_LastDelivered — the lower edge of XRANGE — the
// last-delivered-id of the group exclusively ("(id"): the records after
// it are the keys of the set.
func TestPendingKeys_LastDelivered(t *testing.T) {
	f := &fakePendingRedis{
		groups: []redis.XInfoGroup{{Name: "render-worker", LastDeliveredID: "5-0"}},
		xrange: func(string) ([]redis.XMessage, error) {
			return []redis.XMessage{
				pendingMsg("6-0", 1, 10, 2, 1),
				pendingMsg("7-0", 2, 20, 0, 0),
			}, nil
		},
	}
	keys, err := retentionPending{rdb: f}.PendingKeys(context.Background(), "q:render")
	if err != nil {
		t.Fatalf("PendingKeys: %v", err)
	}
	if len(f.froms) != 1 || f.froms[0] != "(5-0" {
		t.Fatalf("нижние края XRANGE = %v, want [(5-0]", f.froms)
	}
	want := map[content.RetentionPendingKey]struct{}{
		wantKey(1, 10, 2, 1): {},
		wantKey(2, 20, 0, 0): {},
	}
	if len(keys) != len(want) {
		t.Fatalf("ключей = %v, want %v", keys, want)
	}
	for k := range want {
		if _, ok := keys[k]; !ok {
			t.Errorf("ключ %v отсутствует во множестве %v", k, keys)
		}
	}
}

// TestPendingKeys_MinDelivered — several groups: the tail is read by one
// XRANGE from the minimal last-delivered-id (numerically: "999-0" <
// "1000-0", the lexicographic comparison is wrong) — the union of the
// undelivered intervals of all the groups.
func TestPendingKeys_MinDelivered(t *testing.T) {
	f := &fakePendingRedis{
		groups: []redis.XInfoGroup{
			{Name: "legacy", LastDeliveredID: "999-0"},
			{Name: "render-worker", LastDeliveredID: "1000-0"},
		},
		xrange: func(start string) ([]redis.XMessage, error) {
			if start != "(999-0" {
				return nil, fmt.Errorf("неожиданный нижний край %q", start)
			}
			return []redis.XMessage{pendingMsg("1001-0", 1, 10, 0, 0)}, nil
		},
	}
	keys, err := retentionPending{rdb: f}.PendingKeys(context.Background(), "q:render")
	if err != nil {
		t.Fatalf("PendingKeys: %v", err)
	}
	if len(f.froms) != 1 || f.froms[0] != "(999-0" {
		t.Fatalf("нижние края XRANGE = %v, want один [(999-0] — минимум численный", f.froms)
	}
	if _, ok := keys[wantKey(1, 10, 0, 0)]; !ok {
		t.Fatalf("ключи = %v, want ключ записи отставшей группы", keys)
	}
}

// TestPendingKeys_NoGroups — the stream has no groups: everything is
// undelivered, XRANGE from the beginning ("-").
func TestPendingKeys_NoGroups(t *testing.T) {
	f := &fakePendingRedis{
		xrange: func(string) ([]redis.XMessage, error) {
			return []redis.XMessage{pendingMsg("1-0", 1, 10, 0, 0)}, nil
		},
	}
	keys, err := retentionPending{rdb: f}.PendingKeys(context.Background(), "q:analyze")
	if err != nil {
		t.Fatalf("PendingKeys: %v", err)
	}
	if len(f.froms) != 1 || f.froms[0] != "-" {
		t.Fatalf("нижние края XRANGE = %v, want [-]", f.froms)
	}
	if _, ok := keys[wantKey(1, 10, 0, 0)]; !ok {
		t.Fatalf("ключи = %v, want ключ записи 1/10/0/0", keys)
	}
}

// TestPendingKeys_NoStream — the stream does not exist ("no such key" on
// XINFO GROUPS): there is nothing to wait for consumers — an empty set
// without an error.
func TestPendingKeys_NoStream(t *testing.T) {
	f := &fakePendingRedis{groupsErr: redis.Nil}
	keys, err := retentionPending{rdb: f}.PendingKeys(context.Background(), "q:render")
	if err != nil {
		t.Fatalf("PendingKeys: %v, want пустое множество без ошибки", err)
	}
	if len(keys) != 0 || len(f.froms) != 0 {
		t.Fatalf("keys = %v, froms = %v, want пусто (XRANGE не нужен)", keys, f.froms)
	}

	f = &fakePendingRedis{groupsErr: errors.New("ERR no such key")}
	if _, err := (retentionPending{rdb: f}).PendingKeys(context.Background(), "q:render"); err != nil {
		t.Fatalf("текстовая форма no such key: %v", err)
	}
}

// TestPendingKeys_Pagination — the tail is longer than a page: the next
// one is read from the last record of the previous one (exclusively),
// until an empty page.
func TestPendingKeys_Pagination(t *testing.T) {
	page1 := make([]redis.XMessage, pendingScanBatch)
	for i := range page1 {
		page1[i] = pendingMsg(fmt.Sprintf("1-%d", i), 1, 10, int16(i), 0)
	}
	tail := []redis.XMessage{pendingMsg("2-0", 1, 10, pendingScanBatch, 0)}
	f := &fakePendingRedis{
		groups: []redis.XInfoGroup{{Name: "analyzer", LastDeliveredID: "0-0"}},
		xrange: func(start string) ([]redis.XMessage, error) {
			if start == "(0-0" {
				return page1, nil
			}
			return tail, nil // the second page is shorter, the reading will finish
		},
	}
	keys, err := retentionPending{rdb: f}.PendingKeys(context.Background(), "q:analyze")
	if err != nil {
		t.Fatalf("PendingKeys: %v", err)
	}
	if len(f.froms) != 2 || f.froms[0] != "(0-0" || f.froms[1] != "(1-"+fmt.Sprint(pendingScanBatch-1) {
		t.Fatalf("нижние края XRANGE = %v, want [(0-0, (1-%d]", f.froms, pendingScanBatch-1)
	}
	if int64(len(keys)) != pendingScanBatch+1 {
		t.Fatalf("ключей = %d, want %d (весь хвост)", len(keys), pendingScanBatch+1)
	}
	if _, ok := keys[wantKey(1, 10, pendingScanBatch, 0)]; !ok {
		t.Error("запись второй страницы не попала во множество")
	}
}

// TestPendingKeys_Errors — an unavailability of Redis (the groups,
// XRANGE) — an error: the guard is conservative, skipping the pass is
// decided by the loop.
func TestPendingKeys_Errors(t *testing.T) {
	f := &fakePendingRedis{groupsErr: errors.New("connection refused")}
	if _, err := (retentionPending{rdb: f}).PendingKeys(context.Background(), "q:render"); err == nil {
		t.Fatal("ошибка XINFO GROUPS не проброшена")
	}

	f = &fakePendingRedis{
		groups: []redis.XInfoGroup{{Name: "analyzer", LastDeliveredID: "0-0"}},
		xrange: func(string) ([]redis.XMessage, error) { return nil, errors.New("OOM") },
	}
	if _, err := (retentionPending{rdb: f}).PendingKeys(context.Background(), "q:analyze"); err == nil {
		t.Fatal("ошибка XRANGE не проброшена")
	}
}

// TestPendingKeys_ForeignPayload — the bodies without the data field, an
// unparsable JSON and the zero identifiers (a valid JSON `{}` — not a
// Result: validate requires zone_id/domain_id ≥ 1) are skipped: only
// marshalMessage writes the queue streams (the validated formats), the
// foreign records are outside the contract.
func TestPendingKeys_ForeignPayload(t *testing.T) {
	f := &fakePendingRedis{
		xrange: func(string) ([]redis.XMessage, error) {
			return []redis.XMessage{
				pendingMsg("1-0", 1, 10, 0, 0),
				{ID: "2-0", Values: map[string]any{"not-data": "x"}},
				{ID: "3-0", Values: map[string]any{"data": "не json"}},
				{ID: "4-0", Values: map[string]any{"data": `{"task_id":"t","zone_id":0,"domain_id":0,"subdomain_id":0,"variant":0}`}},
			}, nil
		},
	}
	keys, err := retentionPending{rdb: f}.PendingKeys(context.Background(), "q:render")
	if err != nil {
		t.Fatalf("PendingKeys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("ключей = %v, want только валидное сообщение", keys)
	}
	if _, ok := keys[wantKey(1, 10, 0, 0)]; !ok {
		t.Error("валидное сообщение потерялось среди чужих тел")
	}
}

// TestAfterConnectHooks — the wiring of the AfterConnect hooks of the
// http-worker pool (T-201): the retention drain (RetentionLoop) on the
// service pool assembled a Gather Merge — 2 parallel workers on the CPUs
// of the HTTP slots (the EXPLAIN ANALYZE of 26.10) — NoParallelGather
// must be part of it (the extension of T-171 to the fourth service), next
// to it LockTimeout (T-192). Before T-201 the hook wiring in main was not
// pinned by any test (the commit of T-192: "the Options are assembled in
// main without a seam") — the named list afterConnectHooks is that seam:
// dropping or replacing a hook fails the test loudly. The coverage
// boundary (the review of T-201, P3-1): the composition is pinned, but
// not the last link — the DBAfterConnect line in main (the project style:
// the Options are inline; extracting the assembly — a separate task).
// The SETs themselves — the contracts of
// TestLockTimeout_HookSQL/TestJoinHooks in internal/db (executing a hook
// needs a live connection — a unit test has nothing to substitute in
// *pgx.Conn); here only the composition is pinned.
func TestAfterConnectHooks(t *testing.T) {
	if len(afterConnectHooks) != 2 {
		t.Fatalf("AfterConnect-хуков пула = %d, want 2 (NoParallelGather T-201 + LockTimeout T-192)", len(afterConnectHooks))
	}
	// NoParallelGather — a named function of the db package: a code
	// pointer identifies it unambiguously among the hooks (the LockTimeout
	// closure — a different code); the regression of T-201 — a return to
	// LockTimeout-without-seriality — is caught exactly here.
	if reflect.ValueOf(afterConnectHooks[0]).Pointer() != reflect.ValueOf(db.NoParallelGather).Pointer() {
		t.Error("первый AfterConnect-хук не db.NoParallelGather — retention-дренаж снова соберёт Gather Merge (T-201)")
	}
	// LockTimeout — a closure: a code pointer cannot be compared — the
	// compiler specializes the closure per call-site (init/main — different
	// codes, verified on the project toolchain), and the symbolization of
	// runtime.FuncForPC keeps the base name of the literal
	// (…LockTimeout.func1) under any inlining. The path in the name — the
	// package of the call-site (…cmd/http-worker.init.LockTimeout.func1),
	// not the db package: comparing the full name is impossible, the
	// suffix — the right instrument (a substring would also match
	// hypothetical func10/func12).
	// The argument of 30 s — the contract of TestLockTimeout_HookSQL in
	// internal/db.
	if name := runtime.FuncForPC(reflect.ValueOf(afterConnectHooks[1]).Pointer()).Name(); !strings.HasSuffix(name, "LockTimeout.func1") {
		t.Errorf("второй AfterConnect-хук — %s, want замыкание db.LockTimeout (T-192)", name)
	}
	for i, h := range afterConnectHooks {
		if h == nil {
			t.Errorf("AfterConnect-хук #%d — nil: настройка сессии потеряна", i)
		}
	}
}

// TestScanGate — the wiring of the common semaphore of the heavy
// background scans (scanGate, T-271): the retention drain and the late
// re-publisher must share ONE gate of capacity 1 — a coincidence of their
// scans in PostgreSQL was exactly the incident of 08–09.11.2026 (five
// OOM-kills overnight: two degraded cursor SELECTs of ≈1 GiB on top of
// 2 GiB shared_buffers). The coverage boundary — as by TestAfterConnectHooks
// (the review of T-201, P3-1): the gate itself is pinned (capacity 1,
// not nil); the last link — the Gate lines in the options of the run
// loops — TestScanGateLoopWiring (the review of T-271, P3-4). The mutual
// exclusion of the holders — TestScanGate in internal/db; the behavior
// under the gate — TestRetentionSelectGate/TestRenderLateSelectGate in
// the loop packages.
func TestScanGate(t *testing.T) {
	if scanGate == nil {
		t.Fatal("scanGate = nil: гейт тяжёлых сканов не создан — циклы уйдут без сериализации (T-271)")
	}
	if cap(scanGate) != 1 {
		t.Fatalf("ёмкость scanGate = %d, want 1 (один тяжёлый скан на сервис)", cap(scanGate))
	}
}

// TestScanGateLoopWiring — the last link of the scanGate wiring: the Gate
// field in the options of the run loops (the review of T-271, P3-4). A
// dropped Gate line would be silent in all the other checks: TestScanGate
// pins the gate itself, the tests of the loop packages — the behavior
// under the passed gate, but nobody checked that run passes it. Running
// run in a unit test is impossible (live Postgres/Redis), extracting the
// options assembly into a seam function — a separate task (recognized by
// the review of T-201, P3-1: the project style — the Options are inline),
// so the invariant is pinned by parsing main.go via go/ast — the
// TestRunBackgroundJoin/bgjoin precedent (T-223). The invariants: the
// LateOptions/RetentionOptions literals exist exactly one each (each loop
// is started) and contain Gate: scanGate.
func TestScanGateLoopWiring(t *testing.T) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("разбор main.go: %v", err)
	}
	literals := map[string]int{ // the literals of the loop options by the types
		"httpworker.LateOptions":   0,
		"content.RetentionOptions": 0,
	}
	ast.Inspect(parsed, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		typ := pkg.Name + "." + sel.Sel.Name
		if _, known := literals[typ]; !known {
			return true
		}
		literals[typ]++
		for _, el := range lit.Elts {
			kv, ok := el.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Gate" {
				if val, ok := kv.Value.(*ast.Ident); !ok || val.Name != "scanGate" {
					t.Errorf("%s: Gate = %s, want scanGate — цикл уйдёт без гейта (T-271)", typ, types.ExprString(kv.Value))
				}
				return true
			}
		}
		t.Errorf("%s{...}: поле Gate отсутствует — тяжёлый скан цикла без single-flight (T-271)", typ)
		return true
	})
	for typ, n := range literals {
		if n != 1 {
			t.Errorf("литералов %s в main.go = %d, want 1 (цикл запускается ровно один раз)", typ, n)
		}
	}
}

// TestRenderQueueWatermark — the effective capacity threshold of
// q:render (renderQueueWatermark, T-174): the fallback "0 — the common
// scheduler.queue_high_watermark" (the semantics of
// retention_analyze_watermark) and the clamp min(watermark, MAXLEN/2) by
// the same single calculation queue.StreamWatermark as by the publishing
// gate (T-026): the gate must close before the MAXLEN trim — a threshold
// above the limit would silently lose the undelivered render tasks by
// the trim (they have no render_pending flag, the publishing went
// through).
func TestRenderQueueWatermark(t *testing.T) {
	cases := []struct {
		name      string
		render    int
		scheduler int
		redisLen  int
		want      int64
	}{
		// The configuration defaults: 10k < 75k (half of
		// DefaultStreamMaxLen(q:render) = 150k, T-025) — the clamp is not
		// active, the gate fires at the configured value.
		{"дефолт: кламп не активен", 10000, 100000, 0, 10000},
		{"явное значение ниже клампа", 7000, 100000, 0, 7000},
		// 0 — the common threshold; the clamp caps it to 75k: the raw
		// comparison of T-066 (without the clamp) kept the gate open at a
		// lag of 98.4k out of 100k — the equilibrium of 24.10.
		{"0 — общий порог с клампом", 0, 100000, 0, 75000},
		{"явное значение выше клампа ограничивается", 200000, 100000, 0, 75000},
		// An explicit redis.maxlen cuts the clamp ceiling for all the
		// cases.
		{"0 — общий при явном redis.maxlen", 0, 100000, 10000, 5000},
		{"явное при явном redis.maxlen", 10000, 100000, 10000, 5000},
		// The negative ones (past the validation) — StreamWatermark gives
		// 0: the gate is off, the documented semantics of ≤ 0.
		{"отрицательные — гейт отключён", -5, 0, 0, 0},
	}
	for _, tc := range cases {
		cfg := &config.Config{}
		cfg.Render.QueueHighWatermark = tc.render
		cfg.Scheduler.QueueHighWatermark = tc.scheduler
		cfg.Redis.MaxLen = tc.redisLen
		if got := renderQueueWatermark(cfg); got != tc.want {
			t.Errorf("%s: renderQueueWatermark = %d, хочу %d", tc.name, got, tc.want)
		}
	}

	// The default invariant (the task criterion): the threshold is not
	// above half of the MAXLEN limit of q:render — otherwise the clamp
	// would cut it silently, and the gate at a depth near the limit would
	// not make it in time to close before the trim.
	def := renderQueueWatermark(config.Default())
	if def != 10000 {
		t.Errorf("дефолт эффективного порога q:render = %d, хочу 10000 (T-174)", def)
	}
	if half := queue.DefaultStreamMaxLen(queue.StreamRender) / 2; def > half {
		t.Errorf("дефолт %d выше DefaultStreamMaxLen(q:render)/2 = %d — ворота не сработают до обрезки", def, half)
	}
}

// TestRunBackgroundJoin — the structural shutdown invariant of
// http-worker (T-223): all the background loops of run are stopped and
// joined before the defer-closes of store/rdb and the transport on any
// exit — a graceful ctx cancellation, an early return from a
// constructor, a panic. Before T-223 CompactLoop and RetentionLoop were
// started fire-and-forget: run after the three receptions of the bg
// channel returned and closed the PackStore/the Redis pool in the middle
// of a compaction/retention pass, and a fourth sender would hang on the
// channel of capacity 3. The scheme — the derived cancellation + a
// WaitGroup (by the review of T-223: a channel of a capacity does not
// cover the early exits and a panic, and the "capacity = senders =
// receptions" coupling is fragile by hand). Running run in a unit test
// is impossible (live Postgres/Redis/DNS), the invariant is pinned by
// parsing main.go via go/ast (the precedent of reading the sources by
// tests — compose_test.go, postgresql_conf_test.go); the checked
// invariants:
//
//   - run derives the ctx with a derived cancellation (context.WithCancel)
//     and passes it to every loop and the main loop: the framework cancels
//     the ctx of OnRun only after it returns, an early exit/a panic must
//     stop the loops themselves;
//   - the join-defer (the only defer with bg.Wait) — a func literal with
//     an unconditional cancel() before bg.Wait(), registered last of the
//     defers of run → it executes first (LIFO), before the closes of
//     rdb/store/the transport, and before the first start of the loops;
//   - every go-start in run is wrapped by the pair "bg.Add(1); go
//     func(){ defer bg.Done(); … }()": an Add without a pair to a go —
//     an unjoined loop (the regression of T-223), an orphan Add without a
//     go and a Wait-before-cancel hang the Wait (added by the review of
//     T-232), the defer bg.Done must be an unconditional top-level
//     statement of the literal — a conditional Done (inside an if) would
//     leave the Wait hanging.
//
// The checker — the shared internal/bgjoin (extracted from this test by
// the task T-232 for render-worker/analyzer/dns-worker and strengthened
// there by the review: the cancel-before-Wait order, the binding of the
// derived ctx, an orphan Add, a defer in nested blocks) — here and in the
// other services one implementation of the invariant, the strengthening
// reaches all of them.
func TestRunBackgroundJoin(t *testing.T) {
	bgjoin.Check(t, "main.go")
}

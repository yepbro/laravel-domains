// Integration test of the renderworker consumer stream parameter
// (T-236, spec §4.1 item 3, FR-12) against the live Redis/PostgreSQL
// dev environment: a worker built with Options.Stream = the alternative
// stream and a separate consumer group reads exactly that stream —
// XREADGROUP lazily creates the group on the alternative stream, the
// delivery is processed to completion and acknowledged (PEL empty),
// and the default q:render is not read at all (no group is created,
// the control record stays undelivered).
//
// The alternative stream is q:analyze: the queue transport accepts
// only known streams (knownStream), and the Result format is valid in
// q:render and q:analyze alike (message.go); the production
// alternative stream q:render-fallback will appear in internal/queue
// with task T-237 — the worker wiring does not care which of the known
// Result streams is plugged in. The task addresses a non-existent
// subdomain_results row: ProcessResult answers with the normal
// rejection (false, nil) without rendering or publishing — the
// delivery is acknowledged and the loop does not feed itself with
// publications to the same stream (a successful render publishes to
// q:analyze — the reader would see its own result again). The call
// level matches the neighbors (render_requeue_test.go): the library
// Run of the worker without starting the service; isolation —
// withTestSchema + Redis DB 15.
package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/content"
	"crawler/internal/migrate"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/renderworker"
)

// rsRenderer — a render seam stub: test task processing never reaches
// the render (the row is missing — normal rejection), the call is
// recorded by a counter and fails the check.
type rsRenderer struct {
	calls atomic.Int32
}

func (r *rsRenderer) Run(_ context.Context, task render.RenderTask) (render.RenderOutput, error) {
	r.calls.Add(1)
	return render.RenderOutput{}, fmt.Errorf("renderworker-stream: рендер не должен вызываться: %s", task.URL)
}

// TestRenderWorkerAltStream — consumption of the alternative stream
// by a separate group (T-236, FR-12): a fallback worker build — the
// same binary with Options{Stream, Group} — reads and acknowledges the
// records of its own stream; the default q:render stays untouched
// (records and groups).
func TestRenderWorkerAltStream(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(pool.Close)

	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// Fallback worker build (FR-12): a different stream and a separate
	// group, everything else — normal dependencies (DB, queue, store,
	// render).
	const altGroup = "render-alt"
	renderer := &rsRenderer{}
	opts := renderworker.Options{
		Stream:   queue.StreamAnalyze,
		Group:    altGroup,
		Consumer: "alt-w1",
	}
	w, err := renderworker.New(pool, q, store, renderer, opts)
	if err != nil {
		t.Fatalf("renderworker.New: %v", err)
	}

	// The task goes to the alternative stream (the key is missing in
	// the DB — normal rejection); the control one goes to the default
	// q:render: the main worker build would read exactly that one, the
	// build with Options.Stream must ignore it.
	altTask := queue.Result{
		TaskID: queue.NewTaskID(), ZoneID: 5, DomainID: 501,
		SubdomainID: 1, Name: "alt.stream.test",
	}
	altID, err := q.Publish(ctx, queue.StreamAnalyze, altTask)
	if err != nil {
		t.Fatalf("публикация в %s: %v", queue.StreamAnalyze, err)
	}
	if _, err := q.Publish(ctx, queue.StreamRender, queue.Result{
		TaskID: queue.NewTaskID(), ZoneID: 5, DomainID: 502,
		SubdomainID: 1, Name: "default.stream.test",
	}); err != nil {
		t.Fatalf("публикация контрольной в %s: %v", queue.StreamRender, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- renderworker.Run(runCtx, w, opts) }()

	// The alternative stream was consumed and carried through to XACK:
	// the group is created, its last-delivered-id is the task record,
	// the PEL is empty (processing — a normal rejection without an
	// error, the delivery is acknowledged).
	deadline := time.Now().Add(10 * time.Second)
	for {
		groups, err := rdb.XInfoGroups(ctx, queue.StreamAnalyze).Result()
		if err != nil {
			t.Fatalf("XINFO GROUPS %s: %v", queue.StreamAnalyze, err)
		}
		consumed := false
		for _, g := range groups {
			if g.Name == altGroup && g.LastDeliveredID == altID && g.Pending == 0 {
				consumed = true
			}
		}
		if consumed {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("группа %s не потребила запись %s стрима %s (groups=%+v)",
				altGroup, altID, queue.StreamAnalyze, groups)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}

	// The default q:render was not read: no group was created at all,
	// the control record stayed undelivered.
	renderGroups, err := rdb.XInfoGroups(ctx, queue.StreamRender).Result()
	if err != nil {
		t.Fatalf("XINFO GROUPS %s: %v", queue.StreamRender, err)
	}
	if len(renderGroups) != 0 {
		t.Errorf("группы %s = %+v — дефолтный стрим читался (Options.Stream = %s обязан заменить точку потребления)",
			queue.StreamRender, renderGroups, queue.StreamAnalyze)
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
		t.Errorf("записей q:render = %d, хочу 1 (контрольная на месте, потерь и дублей нет)", n)
	}
	// The alternative stream holds only the original record: processing
	// without publications (no self-feeding).
	if n := streamLen(t, rdb, queue.StreamAnalyze); n != 1 {
		t.Errorf("записей q:analyze = %d, хочу 1 (штатный отказ ничего не публикует)", n)
	}
	if n := renderer.calls.Load(); n != 0 {
		t.Errorf("рендер вызван %d раз — задача без строки не должна до него доходить", n)
	}
}

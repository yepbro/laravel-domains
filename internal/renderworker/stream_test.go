// Unit tests of the worker's consumption stream parameter (T-236; spec §4.1 item 3,
// FR-12): Options.Stream — the consumption wiring point — the Run loop's read
// (XREADGROUP) and the RecoverLoop claim (XAUTOCLAIM) go by the field's value,
// the default — q:render (zero Options reproduce today's behavior
// of the main build, zero changes). The override — the fallback worker
// build (spec §4.5 item 1): the same binary, a different stream and a separate group.
// The override value — the existing q:analyze stream: the queue
// transport lets only known streams through (knownStream), and the
// Result contract allows both format streams — q:render and q:analyze (message.go);
// the live consumption of an alternative stream by a separate group —
// the integration test test/integration/render_stream_test.go.
package renderworker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"crawler/internal/queue"
)

// rwConsumeCall — the arguments of one Consume call (stream/group/
// consumer): what the Run loop passes to the queue transport.
type rwConsumeCall struct {
	stream   string
	group    string
	consumer string
}

// rwStreamQueue — a queue.Queue recording the arguments of every Consume and
// empty on deliveries: the real queue with no entries
// blocks in XREADGROUP until ctx is cancelled — the fake repeats this, the Run loop
// stays in the reader, no processing happens.
type rwStreamQueue struct {
	mu    sync.Mutex
	calls []rwConsumeCall
}

var _ queue.Queue = (*rwStreamQueue)(nil)

func (q *rwStreamQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", nil // no publication: deliveries are not processed
}

func (q *rwStreamQueue) Consume(ctx context.Context, stream, group, consumer string, _ int) ([]queue.Delivery, error) {
	q.mu.Lock()
	q.calls = append(q.calls, rwConsumeCall{stream: stream, group: group, consumer: consumer})
	q.mu.Unlock()
	<-ctx.Done() // an empty read: like XREADGROUP BLOCK without entries
	return nil, ctx.Err()
}

func (q *rwStreamQueue) Ack(context.Context, string, string, ...string) error {
	return nil
}

// firstConsume — the arguments of the first read (zero, if there was none).
func (q *rwStreamQueue) firstConsume() rwConsumeCall {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.calls) == 0 {
		return rwConsumeCall{}
	}
	return q.calls[0]
}

// rwClaimSpy — a go-redis window over the reliability layer (Reliab): records the
// stream of every XAUTOCLAIM claim, answers with an empty PEL — RecoverLoop makes
// the first claim immediately (before the first tick), the field's stream is visible without
// waits. The write probe (T-152) is not mounted — the gate is transparent, like in
// the queue's unit fixtures.
type rwClaimSpy struct {
	mu      sync.Mutex
	streams []string
}

func (f *rwClaimSpy) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	f.mu.Lock()
	f.streams = append(f.streams, a.Stream)
	f.mu.Unlock()
	cmd := redis.NewXAutoClaimCmd(ctx, "xautoclaim")
	cmd.SetVal(nil, "0-0")
	return cmd
}

func (f *rwClaimSpy) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return redis.NewXPendingExtCmd(ctx, "xpending")
}

// firstClaimStream — the first claim's stream ("" — there was no claim).
func (f *rwClaimSpy) firstClaimStream() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.streams) == 0 {
		return ""
	}
	return f.streams[0]
}

// TestRenderWorker_StreamDefaults — the consumption stream default (T-236,
// FR-12): DefaultStream — q:render (zero Options reproduce
// today's build), the override is not wiped by the field.
func TestRenderWorker_StreamDefaults(t *testing.T) {
	if DefaultStream != queue.StreamRender {
		t.Errorf("DefaultStream = %q, хочу %q (дефолт — q:render: ноль изменений поведения)",
			DefaultStream, queue.StreamRender)
	}
	if o := (Options{}).withDefaults(); o.Stream != DefaultStream {
		t.Errorf("withDefaults: Stream = %q, хочу дефолт %q", o.Stream, DefaultStream)
	}
	if o := (Options{Stream: queue.StreamAnalyze}).withDefaults(); o.Stream != queue.StreamAnalyze {
		t.Errorf("withDefaults: переопределение Stream = %q затёрто (хочу %q)",
			o.Stream, queue.StreamAnalyze)
	}
}

// TestRenderWorker_RunConsumesConfiguredStream — the consumption wiring
// through Options.Stream (T-236, FR-12): the Run loop's reader opens
// the read by the field's stream/group/consumer, RecoverLoop claims the same
// stream (the consumption points — worker.go: Run/Consume and RecoverOptions);
// default Options read q:render with the DefaultGroup group — today's
// behavior of the main build.
func TestRenderWorker_RunConsumesConfiguredStream(t *testing.T) {
	// newStreamWorker — a worker over the recording queue (no processing:
	// rows/render/storage — the fixture's fakes, the case never gets there).
	newStreamWorker := func(t *testing.T, q queue.Queue) *Worker {
		t.Helper()
		pool := newRWFakePool()
		store := &rwFakeStore{j: pool.j, p: pool}
		renderer := &rwFakeRenderer{j: pool.j}
		w, err := New(pool, q, store, renderer, Options{})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return w
	}
	// waitConsume — the first fact of a read (deterministically: the Run
	// reader calls the queue right on the first iteration).
	waitConsume := func(t *testing.T, q *rwStreamQueue) rwConsumeCall {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			if c := q.firstConsume(); c.stream != "" {
				return c
			}
			select {
			case <-deadline:
				t.Fatal("цикл Run не прочитал стрим (первый Consume не состоялся)")
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	waitClaim := func(t *testing.T, spy *rwClaimSpy) string {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			if s := spy.firstClaimStream(); s != "" {
				return s
			}
			select {
			case <-deadline:
				t.Fatal("RecoverLoop не сделал ни одного клейма")
			case <-time.After(2 * time.Millisecond):
			}
		}
	}

	t.Run("переопределение — стрим и группа сборки", func(t *testing.T) {
		q := &rwStreamQueue{}
		spy := &rwClaimSpy{}
		reliab := queue.NewReliab(spy, q, queue.ReliabOptions{MinIdle: time.Hour})
		w := newStreamWorker(t, q)
		opts := Options{
			Stream:   queue.StreamAnalyze,
			Group:    "render-fallback-test",
			Consumer: "fb-w1",
			Recover:  reliab,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- Run(ctx, w, opts) }()

		got := waitConsume(t, q)
		if got.stream != opts.Stream {
			t.Errorf("Consume: стрим %q, хочу %q (Options.Stream — точка проводки потребления)",
				got.stream, opts.Stream)
		}
		if got.group != opts.Group || got.consumer != opts.Consumer {
			t.Errorf("Consume: группа/консьюмер %q/%q, хочу %q/%q (Options.Group/Consumer)",
				got.group, got.consumer, opts.Group, opts.Consumer)
		}
		// The RecoverLoop claim — over the same stream (the first claim before the tick).
		if s := waitClaim(t, spy); s != opts.Stream {
			t.Errorf("клейм RecoverLoop: стрим %q, хочу %q (Options.Stream)", s, opts.Stream)
		}

		rwStopLoop(t, cancel, done)
	})

	t.Run("дефолт — q:render группой DefaultGroup", func(t *testing.T) {
		q := &rwStreamQueue{}
		w := newStreamWorker(t, q)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- Run(ctx, w, Options{}) }()

		got := waitConsume(t, q)
		if got.stream != queue.StreamRender {
			t.Errorf("Consume: стрим %q, хочу дефолт %q (нулевые Options = сегодняшняя сборка)",
				got.stream, queue.StreamRender)
		}
		if got.group != DefaultGroup {
			t.Errorf("Consume: группа %q, хочу DefaultGroup %q", got.group, DefaultGroup)
		}
		if !strings.HasPrefix(got.consumer, "render-worker-") {
			t.Errorf("Consume: консьюмер %q, хочу дефолтный render-worker-<pid>", got.consumer)
		}

		rwStopLoop(t, cancel, done)
	})
}

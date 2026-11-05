// Юнит-тесты параметра стрима потребления воркера (T-236; ТЗ §4.1 п.3,
// ФТ-12): Options.Stream — точка проводки потребления — чтение цикла Run
// (XREADGROUP) и клейм RecoverLoop (XAUTOCLAIM) идут по значению поля,
// дефолт — q:render (нулевые Options воспроизводят сегодняшнее поведение
// основной сборки, ноль изменений). Переопределение — сборка воркера
// фолбэка (ТЗ §4.5 п.1): тот же бинарь, другой стрим и отдельная группа.
// Значение переопределения — существующий стрим q:analyze: транспорт
// очереди пускает только известные стримы (knownStream), а контракт
// Result допускает оба стрима формата — q:render и q:analyze (message.go);
// живое потребление альтернативного стрима отдельной группой —
// интеграционный тест test/integration/render_stream_test.go.
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

// rwConsumeCall — аргументы одного вызова Consume (стрим/группа/
// консьюмер): то, что цикл Run передаёт транспортy очереди.
type rwConsumeCall struct {
	stream   string
	group    string
	consumer string
}

// rwStreamQueue — queue.Queue, записывающий аргументы каждого Consume и
// пустой на доставки: реальная очередь при отсутствии записей
// блокируется в XREADGROUP до отмены ctx — фейк повторяет это, цикл Run
// остаётся в читателе, обработки не происходит.
type rwStreamQueue struct {
	mu    sync.Mutex
	calls []rwConsumeCall
}

var _ queue.Queue = (*rwStreamQueue)(nil)

func (q *rwStreamQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", nil // публикации нет: доставки не обрабатываются
}

func (q *rwStreamQueue) Consume(ctx context.Context, stream, group, consumer string, _ int) ([]queue.Delivery, error) {
	q.mu.Lock()
	q.calls = append(q.calls, rwConsumeCall{stream: stream, group: group, consumer: consumer})
	q.mu.Unlock()
	<-ctx.Done() // пустое чтение: как XREADGROUP BLOCK без записей
	return nil, ctx.Err()
}

func (q *rwStreamQueue) Ack(context.Context, string, string, ...string) error {
	return nil
}

// firstConsume — аргументы первого чтения (нулевые, если чтения не было).
func (q *rwStreamQueue) firstConsume() rwConsumeCall {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.calls) == 0 {
		return rwConsumeCall{}
	}
	return q.calls[0]
}

// rwClaimSpy — окно go-redis слоя надёжности (Reliab): записывает стрим
// каждого клейма XAUTOCLAIM, отвечает пустым PEL — RecoverLoop делает
// первый клейм немедленно (до первого тика), стрим поля виден без
// ожиданий. Проба записи (T-152) не монтируется — гейт прозрачен, как у
// юнит-фиков очереди.
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

// firstClaimStream — стрим первого клейма ("" — клейма не было).
func (f *rwClaimSpy) firstClaimStream() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.streams) == 0 {
		return ""
	}
	return f.streams[0]
}

// TestRenderWorker_StreamDefaults — дефолт стрима потребления (T-236,
// ФТ-12): DefaultStream — q:render (нулевые Options воспроизводят
// сегодняшнюю сборку), переопределение полем не затирается.
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

// TestRenderWorker_RunConsumesConfiguredStream — проводка потребления
// через Options.Stream (T-236, ФТ-12): читатель цикла Run открывает
// чтение по стриму/группе/консьюмеру поля, RecoverLoop клеймит тот же
// стрим (точки потребления — worker.go: Run/Consume и RecoverOptions);
// дефолтные Options читают q:render группой DefaultGroup — сегодняшнее
// поведение основной сборки.
func TestRenderWorker_RunConsumesConfiguredStream(t *testing.T) {
	// newStreamWorker — воркер на записывающей очереди (обработки нет:
	// строки/рендер/хранилище — фейки фикстуры, до них дело не доходит).
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
	// waitConsume — первый факт чтения (детерминированно: читатель Run
	// обращается к очереди сразу на первой итерации).
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
		// Клейм RecoverLoop — по тому же стриму (первый клейм до тика).
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

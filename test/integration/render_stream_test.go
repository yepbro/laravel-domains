// Интеграционный тест параметра стрима потребления renderworker (T-236,
// ТЗ §4.1 п.3, ФТ-12) против живых Redis/PostgreSQL dev-окружения:
// воркер, собранный с Options.Stream = альтернативный стрим и отдельной
// группой потребителей, читает именно его — XREADGROUP лениво создаёт
// группу на альтернативном стриме, доставка дорабатывается и
// подтверждается (PEL пуст), дефолтный q:render не читается вовсе
// (группа не создаётся, контрольная запись остаётся недоставленной).
//
// Альтернативный стрим — q:analyze: транспорт очереди принимает только
// известные стримы (knownStream), а формат Result допустим в q:render и
// q:analyze одинаково (message.go); боевой альтернативный стрим
// q:render-fallback появится в internal/queue задачей T-237 — для
// проводки воркера безразлично, какой из известных Result-стримов
// подставлен. Задача адресует несуществующую строку subdomain_results:
// ProcessResult отвечает штатным отказом (false, nil) без рендера и
// публикации — доставка подтверждается, а цикл не самоперекармливается
// публикациями в тот же стрим (успешный рендер публикует q:analyze —
// читатель увидел бы собственный результат снова). Уровень вызовов — как
// у соседей (render_requeue_test.go): библиотечный Run воркера без
// запуска сервиса; изоляция — withTestSchema + Redis БД 15.
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

// rsRenderer — заглушка шва рендера: обработка задачи теста до рендера
// не доходит (строки нет — штатный отказ), вызов фиксируется счётчиком
// и роняет проверку.
type rsRenderer struct {
	calls atomic.Int32
}

func (r *rsRenderer) Run(_ context.Context, task render.RenderTask) (render.RenderOutput, error) {
	r.calls.Add(1)
	return render.RenderOutput{}, fmt.Errorf("renderworker-stream: рендер не должен вызываться: %s", task.URL)
}

// TestRenderWorkerAltStream — потребление альтернативного стрима
// отдельной группой (T-236, ФТ-12): сборка воркера фолбэка — тот же
// бинарь с Options{Stream, Group} — читает и подтверждает записи своего
// стрима; дефолтный q:render остаётся нетронутым (записи и группы).
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

	// Сборка воркера фолбэка (ФТ-12): другой стрим и отдельная группа,
	// остальное — штатные зависимости (БД, очередь, хранилище, рендер).
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

	// Задача — в альтернативный стрим (ключа в БД нет — штатный отказ);
	// контрольная — в дефолтный q:render: основная сборка воркера читала
	// бы именно её, сборка с Options.Stream обязана игнорировать.
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

	// Потребление альтернативного стрима состоялось и доведено до XACK:
	// группа создана, её last-delivered-id — запись задачи, PEL пуст
	// (обработка — штатный отказ без ошибки, доставка подтверждается).
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

	// Дефолтный q:render не читался: группа не создавалась вовсе,
	// контрольная запись осталась недоставленной.
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
	// Альтернативный стрим — только исходная запись: обработка без
	// публикаций (самоперекармливание отсутствует).
	if n := streamLen(t, rdb, queue.StreamAnalyze); n != 1 {
		t.Errorf("записей q:analyze = %d, хочу 1 (штатный отказ ничего не публикует)", n)
	}
	if n := renderer.calls.Load(); n != 0 {
		t.Errorf("рендер вызван %d раз — задача без строки не должна до него доходить", n)
	}
}

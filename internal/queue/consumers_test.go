// Тесты гигиены консьюмеров групп (T-155) без живого Redis (стиль
// trim_test.go): фейк с заготовленными ответами XINFO CONSUMERS /
// XPENDING EXTENDED / XGROUP DELCONSUMER. Проверяются инварианты выбора
// жертв (консьюмер с pending не удаляется независимо от простоя; пустой
// неактивный удаляется; активный не трогается), перепроверка pending
// перед удалением (гонка оживления), реакция на отсутствие стрима и
// группы, частичный сбой (валидные группы чистятся), детектор унесённого
// PEL, дефолты опций и завершение цикла по отмене ctx. Реальная
// семантика DELCONSUMER — интеграционные тесты T-032.

package queue

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeConsumers — окно consumerGroups с заготовленными ответами: список
// консьюмеров группы, ответ XPENDING по кандидату («stream|group|name»)
// и число PEL-записей, которое «унесло» удаление. Вызовы записываются
// для проверок аргументов; методы дергаются и из фонового цикла — доступ
// к журналам под мьютексом (стиль fakeTrim, корректно под -race).
type fakeConsumers struct {
	lists    map[string][]redis.XInfoConsumer // ключ "stream|group"
	listsErr map[string]error
	pendRes  map[string][]redis.XPendingExt // ответ XPENDING по кандидату
	pendErr  map[string]error
	delPEL   int64 // ответ DELCONSUMER: PEL-записей, унесённых с консьюмером

	mu       sync.Mutex
	pendArgs []redis.XPendingExtArgs
	dels     []string // "stream|group|consumer" по порядку удаления
}

func (f *fakeConsumers) XInfoConsumers(ctx context.Context, key, group string) *redis.XInfoConsumersCmd {
	cmd := redis.NewXInfoConsumersCmd(ctx, key, group)
	cmd.SetVal(f.lists[key+"|"+group])
	if err, ok := f.listsErr[key+"|"+group]; ok {
		cmd.SetErr(err)
	}
	return cmd
}

func (f *fakeConsumers) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	f.mu.Lock()
	f.pendArgs = append(f.pendArgs, *a)
	f.mu.Unlock()
	cmd := redis.NewXPendingExtCmd(ctx, "xpending")
	key := a.Stream + "|" + a.Group + "|" + a.Consumer
	cmd.SetVal(f.pendRes[key])
	if err, ok := f.pendErr[key]; ok {
		cmd.SetErr(err)
	}
	return cmd
}

func (f *fakeConsumers) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	f.mu.Lock()
	f.dels = append(f.dels, stream+"|"+group+"|"+consumer)
	f.mu.Unlock()
	return redis.NewIntResult(f.delPEL, nil)
}

// delCalls — снимок удалений (копия — для проверок вне мьютекса).
func (f *fakeConsumers) delCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.dels))
	copy(out, f.dels)
	return out
}

// Компиляционные проверки: фейк взаимозаменяем с реальным клиентом
// в окне гигиены.
var _ consumerGroups = (*redis.Client)(nil)
var _ consumerGroups = (*fakeConsumers)(nil)

// TestCleanupConsumersOnceSelection — инварианты выбора жертв одного
// прохода: консьюмер с pending не удаляется независимо от простоя
// (DELCONSUMER с pending теряет PEL — недоставка навсегда); пустой,
// неактивный дольше порога — удаляется; активный (idle меньше порога) —
// не трогается; перед удалением подтверждается пустота PEL кандидата.
func TestCleanupConsumersOnceSelection(t *testing.T) {
	idle := 2 * time.Hour
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-455745", Pending: 430, Idle: 72 * time.Hour}, // мёртвый с висящим pending
				{Name: "http-worker-461002", Pending: 0, Idle: 26 * time.Hour},   // мёртвый пустой
				{Name: "http-worker-461003", Pending: 0, Idle: 30 * time.Second}, // живой активный
			},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamHTTP: "http-worker"},
		Idle:   idle,
	})
	if err != nil {
		t.Fatalf("CleanupConsumersOnce: %v", err)
	}
	if removed != 1 {
		t.Errorf("удалено %d, хочу 1 (только пустой неактивный)", removed)
	}
	dels := f.delCalls()
	if len(dels) != 1 || dels[0] != StreamHTTP+"|http-worker|http-worker-461002" {
		t.Fatalf("DELCONSUMER %v, хочу только пустой неактивный http-worker-461002", dels)
	}
	if len(f.pendArgs) != 1 || f.pendArgs[0].Consumer != "http-worker-461002" {
		t.Errorf("перепроверка pending: %+v, хочу одну по кандидату http-worker-461002", f.pendArgs)
	}
}

// TestCleanupConsumersOnceRaceyPEL — перепроверка пустоты PEL перед
// удалением: консьюмер, получивший доставки между XINFO CONSUMERS
// (пуст) и XPENDING (уже держит записи), не удаляется — гонка
// оживления не теряет PEL.
func TestCleanupConsumersOnceRaceyPEL(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamRender + "|render-worker": {
				{Name: "render-worker-101", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		// «Оживление» между XINFO и перепроверкой: PEL уже не пуст.
		pendRes: map[string][]redis.XPendingExt{
			StreamRender + "|render-worker|render-worker-101": {{ID: "5-0"}},
		},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamRender: "render-worker"},
		Idle:   2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("CleanupConsumersOnce: %v", err)
	}
	if removed != 0 {
		t.Errorf("удалено %d, хочу 0 (перепроверка увидела pending)", removed)
	}
	if dels := f.delCalls(); len(dels) != 0 {
		t.Errorf("DELCONSUMER при непустой перепроверке: %v", dels)
	}
}

// TestCleanupConsumersOnceCarriedPEL — детектор сработавшей гонки:
// DELCONSUMER ответил числом унесённых PEL-записей (> 0) — проход
// возвращает ошибку для Warn-лога цикла, консьюмер при этом удалён.
func TestCleanupConsumersOnceCarriedPEL(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {
				{Name: "dns-worker-9", Pending: 0, Idle: 5 * time.Hour},
			},
		},
		pendRes: map[string][]redis.XPendingExt{},
		delPEL:  2,
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   2 * time.Hour,
	})
	if err == nil {
		t.Fatal("унесённый DELCONSUMER-ом pending не возвращён ошибкой")
	}
	if removed != 1 {
		t.Errorf("удалено %d, хочу 1 (консьюмер удалён, ошибка — сигнал)", removed)
	}
}

// TestCleanupConsumersOnceNoStreamAndNoGroup — стрим без ключа (ещё не
// создавался) и группа без ключа (ленивое создание при первом чтении):
// пустой проход без ошибки, DELCONSUMER не выполняется.
func TestCleanupConsumersOnceNoStreamAndNoGroup(t *testing.T) {
	f := &fakeConsumers{
		listsErr: map[string]error{
			StreamHTTP + "|http-worker": redis.Nil,
			StreamDNS + "|dns-worker":   errors.New("NOGROUP No such consumer group 'dns-worker' for key name 'q:dns'"),
		},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{
			StreamHTTP: "http-worker",
			StreamDNS:  "dns-worker",
		},
	})
	if err != nil {
		t.Fatalf("CleanupConsumersOnce: %v (нет стрима/группы — не ошибка)", err)
	}
	if removed != 0 {
		t.Errorf("удалено %d, хочу 0", removed)
	}
	if dels := f.delCalls(); len(dels) != 0 {
		t.Errorf("DELCONSUMER без стрима/группы: %v", dels)
	}
}

// TestCleanupConsumersOncePartialFailure — ошибка одной группы (чтение
// списка, перепроверка pending, удаление) не отменяет остальные:
// валидные чистятся, ошибки собираются в возврат.
func TestCleanupConsumersOncePartialFailure(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-1", Pending: 0, Idle: 3 * time.Hour},
			},
			StreamRender + "|render-worker": {
				{Name: "render-worker-1", Pending: 0, Idle: 3 * time.Hour},
			},
			StreamAnalyze + "|analyzer": {
				{Name: "analyzer-1", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		listsErr: map[string]error{
			StreamDNS + "|dns-worker": errors.New("connection reset"),
		},
		pendErr: map[string]error{
			StreamRender + "|render-worker|render-worker-1": errors.New("LOADING Redis is loading the dataset in memory"),
		},
		delPEL: 0,
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{})
	if err == nil {
		t.Fatal("частичный сбой не возвращён ошибкой")
	}
	if removed != 2 {
		t.Errorf("удалено %d, хочу 2 (q:http и q:analyze; q:dns сбой, q:render не подтверждён)", removed)
	}
	dels := f.delCalls()
	want := []string{
		StreamAnalyze + "|analyzer|analyzer-1",
		StreamHTTP + "|http-worker|http-worker-1",
	}
	if !reflect.DeepEqual(dels, want) {
		t.Fatalf("DELCONSUMER %v, хочу %v (сортировка стримов, сбойные пропущены)", dels, want)
	}
}

// TestCleanupConsumersOnceUnknownStreamAndNilClient — неизвестное имя
// стрима в карте групп и пустая группа — ошибки; nil-клиент — ошибка.
func TestCleanupConsumersOnceUnknownStreamAndNilClient(t *testing.T) {
	f := &fakeConsumers{}
	if _, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{"q:unknown": "g"},
	}); err == nil {
		t.Error("неизвестный стрим не отвергнут")
	}
	if _, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: ""},
	}); err == nil {
		t.Error("пустая группа не отвергнута")
	}
	if _, err := CleanupConsumersOnce(context.Background(), nil, ConsumerOptions{}); err == nil {
		t.Error("nil-клиент не отвергнут")
	}
	if dels := f.delCalls(); len(dels) != 0 {
		t.Errorf("DELCONSUMER при ошибках аргументов: %v", dels)
	}
}

// TestConsumerOptionsDefaults — нулевые Groups/Idle/Interval заменяются
// дефолтами: рабочие пары задач (WorkingGroups — все четыре рабочих
// стрима, без q:dead), DefaultConsumerIdle и DefaultConsumerInterval.
// Пин-тест: новый рабочий стрим обязан попасть в WorkingGroups, иначе
// его группы молча не чистятся; соответствие имён групп константам
// воркеров сверяет consumers_pin_test.go.
func TestConsumerOptionsDefaults(t *testing.T) {
	o := ConsumerOptions{}.withDefaults()
	if !reflect.DeepEqual(o.Groups, WorkingGroups()) {
		t.Errorf("Groups по умолчанию = %v, хочу %v (WorkingGroups)", o.Groups, WorkingGroups())
	}
	if len(o.Groups) != len(workingStreams()) {
		t.Errorf("WorkingGroups покрывает %d стримов, рабочих — %d: новый стрим добавьте в WorkingGroups", len(o.Groups), len(workingStreams()))
	}
	for s := range o.Groups {
		if !knownStream(s) || s == StreamDead {
			t.Errorf("WorkingGroups содержит неподходящий стрим %q", s)
		}
	}
	if o.Idle != DefaultConsumerIdle {
		t.Errorf("Idle по умолчанию = %s, хочу %s", o.Idle, DefaultConsumerIdle)
	}
	if o.Interval != DefaultConsumerInterval {
		t.Errorf("Interval по умолчанию = %s, хочу %s", o.Interval, DefaultConsumerInterval)
	}
	o = ConsumerOptions{
		Groups:   map[string]string{StreamHTTP: "http-worker"},
		Idle:     time.Minute,
		Interval: time.Hour,
	}.withDefaults()
	if o.Idle != time.Minute || o.Interval != time.Hour || !reflect.DeepEqual(o.Groups, map[string]string{StreamHTTP: "http-worker"}) {
		t.Errorf("явные значения заменены дефолтами: %+v", o)
	}
}

// TestCleanupConsumersLoopCancel — цикл делает первый проход сразу
// (удаление фиксируется журналом фейка) и завершается nil по отмене ctx
// (контракт OnRun, T-012), не дожидаясь тика.
func TestCleanupConsumersLoopCancel(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {
				{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- CleanupConsumersLoop(ctx, f, ConsumerOptions{
			Groups:   map[string]string{StreamDNS: "dns-worker"},
			Idle:     2 * time.Hour,
			Interval: time.Hour,
		})
	}()
	// Первый проход асинхронного цикла: фиксируется вызовом DELCONSUMER.
	deadline := time.After(2 * time.Second)
	for len(f.delCalls()) == 0 {
		select {
		case <-deadline:
			t.Fatal("первый проход CleanupConsumersLoop не выполнен")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CleanupConsumersLoop после отмены: %v, хочу nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CleanupConsumersLoop не завершился после отмены ctx")
	}
}

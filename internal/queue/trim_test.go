// Тесты освобождения подтверждённого префикса стримов (T-141) без живого
// Redis (стиль redisq_test.go): фейк с заготовленными ответами XINFO
// GROUPS / XPENDING / XTRIM. Проверяются выбор точки обрезки (старейшая
// pending группы; при пустом pending — last-delivered-id; минимум по
// нескольким группам с численным сравнением id), пропуск стрима без
// групп и без ключа (q:dead-случай), ошибки чтения (небезопасный проход
// пропускается целиком, остальные стримы освобождаются), сумма удалённого
// и дефолты опций. Реальная семантика XTRIM MINID и деградация памяти —
// интеграционный тест TestResilience_TrimConfirmedPrefix (T-141).

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

// fakeTrim — окно trimStreams с заготовленными ответами: группы стрима,
// сводка XPENDING по группе (старейшая pending) и результат XTRIM.
// Вызовы записываются для проверок аргументов; методы дергаются и из
// фонового цикла TrimLoop — доступ к журналам вызовов под мьютексом
// (стиль recover_test.go, корректно под -race).
type fakeTrim struct {
	groups    map[string][]redis.XInfoGroup
	groupsErr map[string]error
	pend      map[string]redis.XPending // ключ "stream|group"
	pendErr   map[string]error
	xlen      map[string]int64 // длина стрима (сигнал застрявшего префикса)
	trimCount int64            // ответ XTRIM (число удалённых)

	mu    sync.Mutex
	infos []string            // ключи XINFO GROUPS по порядку
	pends []string            // ключи XPENDING по порядку
	trims []map[string]string // XTRIM: {"stream": …, "minid": …}
}

func (f *fakeTrim) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	f.mu.Lock()
	f.infos = append(f.infos, key)
	f.mu.Unlock()
	cmd := redis.NewXInfoGroupsCmd(ctx, key)
	cmd.SetVal(f.groups[key])
	if err, ok := f.groupsErr[key]; ok {
		cmd.SetErr(err)
	}
	return cmd
}

func (f *fakeTrim) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	f.mu.Lock()
	f.pends = append(f.pends, stream+"|"+group)
	f.mu.Unlock()
	cmd := redis.NewXPendingCmd(ctx, "xpending", stream, group)
	pend := f.pend[stream+"|"+group]
	cmd.SetVal(&pend)
	if err, ok := f.pendErr[stream+"|"+group]; ok {
		cmd.SetErr(err)
	}
	return cmd
}

func (f *fakeTrim) XTrimMinID(ctx context.Context, key, minID string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.trims == nil {
		f.trims = []map[string]string{}
	}
	f.trims = append(f.trims, map[string]string{"stream": key, "minid": minID})
	return redis.NewIntResult(f.trimCount, nil)
}

// trimCalls — снимок вызовов XTRIM (копия — для проверок вне мьютекса).
func (f *fakeTrim) trimCalls() []map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]string, len(f.trims))
	copy(out, f.trims)
	return out
}

func (f *fakeTrim) XLen(ctx context.Context, key string) *redis.IntCmd {
	return redis.NewIntResult(f.xlen[key], nil)
}

// Компиляционные проверки: фейк взаимозаменяем с реальным клиентом
// в окне освобождения.
var _ trimStreams = (*redis.Client)(nil)
var _ trimStreams = (*fakeTrim)(nil)

// TestTrimOncePendingKept — точка обрезки по группе с зависшими записями:
// XTRIM MINID — старейшая pending (всё ниже доставлено и подтверждено,
// сама pending и хвост выше остаются).
func TestTrimOncePendingKept(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamHTTP: {{Name: "http", Pending: 3, LastDeliveredID: "105-0"}},
		},
		pend: map[string]redis.XPending{
			StreamHTTP + "|http": {Count: 3, Lower: "101-0", Higher: "103-0"},
		},
		trimCount: 7,
	}
	n, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamHTTP}})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if n != 7 {
		t.Errorf("удалено %d, хочу 7 (ответ XTRIM)", n)
	}
	if len(f.trims) != 1 || f.trims[0]["stream"] != StreamHTTP || f.trims[0]["minid"] != "101-0" {
		t.Fatalf("XTRIM %+v, хочу MINID 101-0 в %s (старейшая pending)", f.trims, StreamHTTP)
	}
}

// TestTrimOnceEmptyPEL — пустой pending группы: точка — last-delivered-id
// (всё ниже доставлено и подтверждено, недоставленный хвост выше остаётся).
func TestTrimOnceEmptyPEL(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}},
		},
	}
	if _, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamDNS}}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "42-1" {
		t.Fatalf("XTRIM %+v, хочу MINID 42-1 (last-delivered-id)", f.trims)
	}
	if len(f.pends) != 0 {
		t.Errorf("XPENDING при пустом pending группы не нужен: %v", f.pends)
	}
}

// TestTrimOnceMinAcrossGroups — несколько групп: точка — численный минимум
// по группам ("999-0" < "1000-0", лексикографический порядок обманчив).
// Свежая группа со стартом "0" (last-delivered-id "0-0", ещё не читала)
// держит точку на "0-0" — консервативно: обрезка выше была бы потерей
// истории для неё (XTRIM MINID 0-0 не удаляет ничего, TrimOnce просто
// не освобождает такой стрим до прогресса группы).
func TestTrimOnceMinAcrossGroups(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamAnalyze: {
				{Name: "a", Pending: 1, LastDeliveredID: "1000-0"},
				{Name: "b", Pending: 0, LastDeliveredID: "999-0"},
			},
		},
		pend: map[string]redis.XPending{
			StreamAnalyze + "|a": {Count: 1, Lower: "1000-0"},
		},
	}
	if _, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamAnalyze}}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "999-0" {
		t.Fatalf("XTRIM %+v, хочу MINID 999-0 (минимум по группам)", f.trims)
	}

	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamAnalyze: {
				{Name: "a", Pending: 0, LastDeliveredID: "50-0"},
				{Name: "fresh", Pending: 0, LastDeliveredID: "0-0"},
			},
		},
	}
	if _, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamAnalyze}}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "0-0" {
		t.Fatalf("XTRIM %+v, хочу MINID 0-0 (свежая группа держит точку — история ей ещё не доставлена)", f.trims)
	}
}

// TestTrimOnceNoGroupsAndNoKey — стрим без групп (q:dead-случай: групп
// нет — подтверждённого префикса не бывает) и несуществующий ключ:
// пустой проход без ошибки, XTRIM не выполняется.
func TestTrimOnceNoGroupsAndNoKey(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDead: {}},
		groupsErr: map[string]error{
			StreamRender: redis.Nil,
			StreamDNS:    errors.New("ERR no such key"),
		},
	}
	n, stuck, err := TrimOnce(context.Background(), f, TrimOptions{
		Streams: []string{StreamDead, StreamRender, StreamDNS},
	})
	if err != nil {
		t.Fatalf("TrimOnce: %v (стрим без групп/ключа — не ошибка)", err)
	}
	if n != 0 {
		t.Errorf("удалено %d, хочу 0", n)
	}
	if len(stuck) != 0 {
		t.Errorf("застрявшие без групп/ключа: %v", stuck)
	}
	if len(f.trims) != 0 {
		t.Errorf("XTRIM по стриму без групп/ключа: %+v", f.trims)
	}
}

// TestTrimOncePendingErrorSkipsStream — ошибка чтения PEL делает обрезку
// по last-delivered-id небезопасной (pending может лежать ниже): стрим
// пропускается с ошибкой, остальные освобождаются.
func TestTrimOncePendingErrorSkipsStream(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamHTTP: {{Name: "http", Pending: 2, LastDeliveredID: "10-0"}},
			StreamAnalyze: {
				{Name: "an", Pending: 0, LastDeliveredID: "5-0"},
			},
		},
		pendErr: map[string]error{
			StreamHTTP + "|http": errors.New("connection reset"),
		},
	}
	n, _, err := TrimOnce(context.Background(), f, TrimOptions{
		Streams: []string{StreamHTTP, StreamAnalyze},
	})
	if err == nil {
		t.Fatal("ошибка XPENDING не возвращена")
	}
	if n != 0 {
		t.Errorf("удалено %d, хочу 0 (сбойный стрим пропущен)", n)
	}
	if len(f.trims) != 1 || f.trims[0]["stream"] != StreamAnalyze {
		t.Fatalf("XTRIM %+v, хочу только %s (валидная часть отрабатывает)", f.trims, StreamAnalyze)
	}
}

// TestTrimOnceUnknownStreamAndNilClient — неизвестное имя стрима и nil-
// клиент — ошибки; XTRIM не выполняется.
func TestTrimOnceUnknownStreamAndNilClient(t *testing.T) {
	f := &fakeTrim{}
	if _, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{"q:unknown"}}); err == nil {
		t.Error("неизвестный стрим не отвергнут")
	}
	if _, _, err := TrimOnce(context.Background(), nil, TrimOptions{}); err == nil {
		t.Error("nil-клиент не отвергнут")
	}
	if len(f.trims) != 0 {
		t.Errorf("XTRIM при ошибках аргументов: %+v", f.trims)
	}
}

// TestTrimOnceStuckSignal — сигнал застрявшего префикса: длина стрима
// (XLEN) ≥ ¾ предела при живых группах — точка обрезки заморожена
// (минимум по группам держит осиротевшая группа), подтверждённый
// префикс копится, а глубина lag+pending молчит; ниже порога и явный
// MaxLen из опций — сигнала нет / считается от явного предела.
func TestTrimOnceStuckSignal(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamAnalyze: {{Name: "an", Pending: 0, LastDeliveredID: "5-0"}},
		},
		pend: map[string]redis.XPending{},
		xlen: map[string]int64{StreamAnalyze: 120_000}, // ¾ от 150 000 = 112 500
	}
	_, stuck, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamAnalyze}})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(stuck) != 1 || stuck[0] != StreamAnalyze {
		t.Fatalf("застрявшие = %v, хочу [%s] (XLEN 120 000 ≥ ¾ предела 150 000)", stuck, StreamAnalyze)
	}

	// Ниже порога — сигнала нет.
	f.xlen[StreamAnalyze] = 100_000
	if _, stuck, _ := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamAnalyze}}); len(stuck) != 0 {
		t.Errorf("застрявшие при XLEN 100 000 < 112 500: %v", stuck)
	}

	// Явный предел из опций: порог считается от него (гейт-подобно
	// BackpressureOptions.MaxLen).
	if _, stuck, _ := TrimOnce(context.Background(), f, TrimOptions{
		Streams: []string{StreamAnalyze}, MaxLen: 120_000,
	}); len(stuck) != 1 || stuck[0] != StreamAnalyze {
		t.Errorf("застрявшие при явном MaxLen 120 000 и XLEN 100 000: %v, хочу [%s]", stuck, StreamAnalyze)
	}
}

// TestTrimOptionsDefaults — нулевые Streams/Interval заменяются дефолтами:
// рабочие стримы задач (workingStreams — всё известное knownStream, кроме
// q:dead) и DefaultTrimInterval. Пин-тест: новый рабочий стрим обязан
// попасть в workingStreams, иначе молча не освобождается.
func TestTrimOptionsDefaults(t *testing.T) {
	o := TrimOptions{}.withDefaults()
	want := workingStreams()
	if !reflect.DeepEqual(o.Streams, want) {
		t.Errorf("Streams по умолчанию = %v, хочу %v (без q:dead)", o.Streams, want)
	}
	for _, s := range want {
		if !knownStream(s) {
			t.Errorf("workingStreams содержит неизвестный стрим %q", s)
		}
	}
	if len(want) != 4 {
		t.Errorf("рабочих стримов задач %d (%v), хочу 4 — новый стрим добавьте в workingStreams", len(want), want)
	}
	if o.Interval != DefaultTrimInterval {
		t.Errorf("Interval по умолчанию = %s, хочу %s", o.Interval, DefaultTrimInterval)
	}
	o = TrimOptions{Streams: []string{StreamDead}, Interval: time.Nanosecond}.withDefaults()
	if !reflect.DeepEqual(o.Streams, []string{StreamDead}) || o.Interval != time.Nanosecond {
		t.Errorf("явные значения заменены дефолтами: %+v", o)
	}
}

// TestTrimLoopCancel — цикл делает первый проход сразу и завершается
// nil по отмене ctx (контракт OnRun, T-012), не дожидаясь тика. Журнал
// вызовов читается через снимок fakeTrim.trimCalls — цикл живёт в другой
// горутине (доступ под мьютексом фейка).
func TestTrimLoopCancel(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "1-0"}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- TrimLoop(ctx, f, TrimOptions{Interval: time.Hour}) }()
	// Первый проход асинхронного цикла: точка фиксируется вызовом XTRIM.
	deadline := time.After(2 * time.Second)
	for len(f.trimCalls()) == 0 {
		select {
		case <-deadline:
			t.Fatal("первый проход TrimLoop не выполнен")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("TrimLoop после отмены: %v, хочу nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TrimLoop не завершился после отмены ctx")
	}
}

// Юнит-тесты адаптера защиты retention (retentionPending, T-166/T-167):
// чтение непотреблённого хвоста стримов без живого Redis (фейк с
// заготовленными XINFO GROUPS/XRANGE, стиль fakeBackpressure в
// internal/queue): нижний край XRANGE — last-delivered-id группы
// эксклюзивно, отсутствие групп — весь стрим с начала, отсутствие
// стрима — пустое множество, пагинация хвоста, ошибки, чужие тела.
// Здесь же — эффективный порог ёмкости q:render (renderQueueWatermark,
// T-174): фолбэк «0 — общий scheduler.queue_high_watermark» и кламп
// MAXLEN/2 (queue.StreamWatermark); и проводка AfterConnect-хуков пула
// сервиса (afterConnectHooks, T-201): NoParallelGather — сериальность
// retention-дренажа + LockTimeout (T-192).
package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/queue"
)

// fakePendingRedis — окно адаптера с заготовками: groups — ответ XINFO
// GROUPS, xrange — страницы XRANGE по нижнему краю (nil — хвост пуст).
type fakePendingRedis struct {
	groups    []redis.XInfoGroup
	groupsErr error
	xrange    func(start string) ([]redis.XMessage, error)

	froms []string // нижние края XRANGE по порядку
}

// compile-time: фейк взаимозаменяем с реальным клиентом в окне адаптера.
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

// pendingMsg — запись стрима с Result-сообщением (формат T-024).
func pendingMsg(id string, zone int16, domain int64, sub, variant int16) redis.XMessage {
	return redis.XMessage{
		ID: id,
		Values: map[string]any{"data": fmt.Sprintf(
			`{"task_id":"t","zone_id":%d,"domain_id":%d,"subdomain_id":%d,"variant":%d}`,
			zone, domain, sub, variant)},
	}
}

// wantKey — ожидаемый ключ защиты.
func wantKey(zone int16, domain int64, sub, variant int16) content.RetentionPendingKey {
	return content.RetentionPendingKey{Zone: zone, Domain: domain, Sub: sub, Variant: variant}
}

// TestPendingKeys_LastDelivered — нижний край XRANGE — last-delivered-id
// группы эксклюзивно («(id»): записи после него — ключи множества.
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

// TestPendingKeys_MinDelivered — несколько групп: хвост читается одним
// XRANGE от минимального last-delivered-id (численно: "999-0" < "1000-0",
// лексикографическое сравнение ошибочно) — объединение недоставленных
// интервалов всех групп.
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

// TestPendingKeys_NoGroups — групп у стрима нет: недоставлено всё
// содержимое, XRANGE с начала («-»).
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

// TestPendingKeys_NoStream — стрима нет («no such key» на XINFO GROUPS):
// ждать потребителей нечего — пустое множество без ошибки.
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

// TestPendingKeys_Pagination — хвост длиннее страницы: следующая читается
// от последней записи предыдущей (эксклюзивно), до пустой страницы.
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
			return tail, nil // вторая страница — короче, чтение закончится
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

// TestPendingKeys_Errors — недоступность Redis (группы, XRANGE) — ошибка:
// защита консервативна, пропуск прохода решает цикл.
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

// TestPendingKeys_ForeignPayload — тела без поля data, неразбираемый
// JSON и нулевые идентификаторы (валидный JSON `{}` — не Result:
// validate требует zone_id/domain_id ≥ 1) пропускаются: стримы очереди
// пишет только marshalMessage (валидированные форматы), чужие записи —
// вне контракта.
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

// TestAfterConnectHooks — проводка AfterConnect-хуков пула http-worker
// (T-201): retention-дренаж (RetentionLoop) на пуле сервиса собирал
// Gather Merge — 2 параллельных воркера на CPU HTTP-слотов (EXPLAIN
// ANALYZE 26.10) — NoParallelGather обязан входить в состав (продление
// T-171 на четвёртый сервис), рядом LockTimeout (T-192). До T-201
// проводка хуков в main тестом не закреплялась (коммит T-192: «Options
// собираются в main без шва») — именованный список afterConnectHooks и
// есть шов: выпадение или подмена хука валит тест громко. Граница
// покрытия (ревью T-201, P3-1): закреплён состав, но не последнее звено
// — строку DBAfterConnect в main (стиль проекта: Options инлайн; вынос
// сборки — отдельная задача). Сами SET — контракты
// TestLockTimeout_HookSQL/TestJoinHooks в internal/db (исполнение хука
// требует живого соединения — юнит-тесту нечего подменять в *pgx.Conn),
// здесь закреплён только состав.
func TestAfterConnectHooks(t *testing.T) {
	if len(afterConnectHooks) != 2 {
		t.Fatalf("AfterConnect-хуков пула = %d, want 2 (NoParallelGather T-201 + LockTimeout T-192)", len(afterConnectHooks))
	}
	// NoParallelGather — именованная функция пакета db: кодовый указатель
	// однозначно идентифицирует её среди хуков (замыкание LockTimeout —
	// другой код); регрессия T-201 — возврат к LockTimeout-без-параллельности
	// — ловится именно здесь.
	if reflect.ValueOf(afterConnectHooks[0]).Pointer() != reflect.ValueOf(db.NoParallelGather).Pointer() {
		t.Error("первый AfterConnect-хук не db.NoParallelGather — retention-дренаж снова соберёт Gather Merge (T-201)")
	}
	// LockTimeout — замыкание: кодовый указатель сравнить нельзя —
	// компилятор специализирует замыкание под call-site (init/main —
	// разные коды, проверено на инструментальной цепочке проекта), а
	// символизация runtime.FuncForPC сохраняет базовое имя литерала
	// (…LockTimeout.func1) при любом инлайнинге. Путь в имени — пакета
	// call-site (…cmd/http-worker.init.LockTimeout.func1), не пакета
	// db: сверять полное имя нельзя, суффикс — правильный инструмент
	// (подстрока совпала бы и с гипотетическими func10/func12).
	// Аргумент 30 с — контракт TestLockTimeout_HookSQL в internal/db.
	if name := runtime.FuncForPC(reflect.ValueOf(afterConnectHooks[1]).Pointer()).Name(); !strings.HasSuffix(name, "LockTimeout.func1") {
		t.Errorf("второй AfterConnect-хук — %s, want замыкание db.LockTimeout (T-192)", name)
	}
	for i, h := range afterConnectHooks {
		if h == nil {
			t.Errorf("AfterConnect-хук #%d — nil: настройка сессии потеряна", i)
		}
	}
}

// TestRenderQueueWatermark — эффективный порог ёмкости q:render
// (renderQueueWatermark, T-174): фолбэк «0 — общий
// scheduler.queue_high_watermark» (семантика retention_analyze_watermark)
// и кламп min(watermark, MAXLEN/2) тем же единым расчётом
// queue.StreamWatermark, что у гейта публикации (T-026): ворота обязаны
// закрываться до обрезки MAXLEN — порог выше предела молча терял бы
// недоставленные рендер-задачи обрезкой (флага render_pending у них нет,
// публикация прошла).
func TestRenderQueueWatermark(t *testing.T) {
	cases := []struct {
		name      string
		render    int
		scheduler int
		redisLen  int
		want      int64
	}{
		// Дефолты конфигурации: 10 тыс. < 75 тыс. (половина
		// DefaultStreamMaxLen(q:render) = 150 тыс., T-025) — кламп
		// не активен, ворота срабатывают на настроенном значении.
		{"дефолт: кламп не активен", 10000, 100000, 0, 10000},
		{"явное значение ниже клампа", 7000, 100000, 0, 7000},
		// 0 — общий порог; кламп ограничивает его до 75 тыс.: сырое
		// сравнение T-066 (без клампа) держало ворота открытыми при
		// лаге 98,4 тыс. из 100 тыс. — равновесие 24.10.
		{"0 — общий порог с клампом", 0, 100000, 0, 75000},
		{"явное значение выше клампа ограничивается", 200000, 100000, 0, 75000},
		// Явный redis.maxlen режет потолок клампа для всех случаев.
		{"0 — общий при явном redis.maxlen", 0, 100000, 10000, 5000},
		{"явное при явном redis.maxlen", 10000, 100000, 10000, 5000},
		// Отрицательные (мимо валидации) — StreamWatermark даёт 0:
		// гейт отключён, документированная семантика ≤ 0.
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

	// Инвариант дефолта (критерий задачи): порог не выше половины
	// предела MAXLEN q:render — иначе кламп резал бы его молча, а
	// ворота при глубине у предела не успевали закрываться до обрезки.
	def := renderQueueWatermark(config.Default())
	if def != 10000 {
		t.Errorf("дефолт эффективного порога q:render = %d, хочу 10000 (T-174)", def)
	}
	if half := queue.DefaultStreamMaxLen(queue.StreamRender) / 2; def > half {
		t.Errorf("дефолт %d выше DefaultStreamMaxLen(q:render)/2 = %d — ворота не сработают до обрезки", def, half)
	}
}

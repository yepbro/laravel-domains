// Юнит-тесты адаптера защиты retention (retentionPending, T-166/T-167):
// чтение непотреблённого хвоста стримов без живого Redis (фейк с
// заготовленными XINFO GROUPS/XRANGE, стиль fakeBackpressure в
// internal/queue): нижний край XRANGE — last-delivered-id группы
// эксклюзивно, отсутствие групп — весь стрим с начала, отсутствие
// стрима — пустое множество, пагинация хвоста, ошибки, чужие тела.
package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"

	"crawler/internal/content"
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

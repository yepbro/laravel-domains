// Stream group source tests (T-162): XINFO GROUPS + XLEN over the
// worker streams, the group depth — the gate formula (min(lag, XLEN) +
// pending, the T-168 clamp, an undefined lag of −1), last-delivered-id
// passes through as is; a stream without a key — no contribution
// (not an error), a read error — a pass error.
package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"crawler/internal/queue"
)

// fakeGroupsRedis — a go-redis window: XINFO GROUPS and XLEN from
// presets (the fakeBackpressure style in internal/queue).
type fakeGroupsRedis struct {
	groups map[string][]redis.XInfoGroup
	xlen   map[string]int64
	// groupErrs/lensErrs — per-stream errors (a one-element queue).
	groupErrs map[string]error
	lensErrs  map[string]error
}

func (f *fakeGroupsRedis) XInfoGroups(_ context.Context, key string) *redis.XInfoGroupsCmd {
	cmd := redis.NewXInfoGroupsCmd(context.Background(), key)
	if err, ok := f.groupErrs[key]; ok {
		cmd.SetErr(err)
		return cmd
	}
	if grp, ok := f.groups[key]; ok {
		cmd.SetVal(grp)
		return cmd
	}
	// the stream does not exist — like Redis: a no such key error.
	cmd.SetErr(redis.Nil)
	return cmd
}

func (f *fakeGroupsRedis) XLen(_ context.Context, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(context.Background())
	if err, ok := f.lensErrs[key]; ok {
		cmd.SetErr(err)
		return cmd
	}
	cmd.SetVal(f.xlen[key])
	return cmd
}

// TestRedisStreamGroups — the group depth and last-delivered-id for each
// worker stream group; the lag clamp by XLEN; not all streams have
// groups.
func TestRedisStreamGroups(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			// lag 98 at XLEN 50 — the clamp: min(98, 50) + pending 2 = 52.
			queue.StreamRender: {{
				Name: "render-worker", Lag: 98, Pending: 2, LastDeliveredID: "1761280800000-0",
			}},
			// An undefined lag (−1) — pending only (T-168).
			queue.StreamAnalyze: {{
				Name: "analyzer", Lag: -1, Pending: 5, LastDeliveredID: "0-0",
			}},
		},
		xlen: map[string]int64{queue.StreamRender: 50, queue.StreamAnalyze: 1000},
	}
	src := RedisStreamGroups{RDB: rdb}

	groups, err := src.StreamGroups(context.Background())
	if err != nil {
		t.Fatalf("StreamGroups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("групп = %d, want 2 (q:dns/q:http без ключа — пустой вклад): %+v", len(groups), groups)
	}
	byKey := make(map[string]StreamGroupState, len(groups))
	for _, g := range groups {
		byKey[g.Stream+"/"+g.Group] = g
	}
	rend := byKey["q:render/render-worker"]
	if rend.Depth != 52 || rend.LastDeliveredID != "1761280800000-0" {
		t.Errorf("q:render/render-worker = %+v, want depth=52 (кламп 98→50 + 2) и ID как есть", rend)
	}
	ana := byKey["q:analyze/analyzer"]
	if ana.Depth != 5 || ana.LastDeliveredID != "0-0" {
		t.Errorf("q:analyze/analyzer = %+v, want depth=5 (lag −1: только pending) и ID 0-0", ana)
	}

	// A stream with a key but without groups (the consumers were never
	// created) — no contribution: there is no one whose deliveries
	// could be measured.
	rdb2 := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{queue.StreamDNS: {}},
		xlen:   map[string]int64{queue.StreamDNS: 10},
	}
	if groups, err := (RedisStreamGroups{RDB: rdb2}).StreamGroups(context.Background()); err != nil || len(groups) != 0 {
		t.Errorf("стрим без групп = (%v, %v), want пусто без ошибки", groups, err)
	}
}

// TestRedisStreamGroupsErrors — a read error (not "no key") of any
// stream — a pass error with the stream name; a nil client — an error.
func TestRedisStreamGroupsErrors(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups:    map[string][]redis.XInfoGroup{},
		groupErrs: map[string]error{queue.StreamHTTP: errors.New("connection refused")},
		lensErrs:  map[string]error{queue.StreamDNS: errors.New("контекст истёк")},
	}
	_, err := (RedisStreamGroups{RDB: rdb}).StreamGroups(context.Background())
	if err == nil || !strings.Contains(err.Error(), "q:http") {
		t.Errorf("ошибка XINFO = %v, want с именем q:http", err)
	}

	rdb2 := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			// the group exists, XLEN fails — also a pass error.
			queue.StreamDNS: {{Name: "dns-worker", Lag: 1, Pending: 0}},
		},
		xlen:     map[string]int64{},
		lensErrs: map[string]error{queue.StreamDNS: errors.New("контекст истёк")},
	}
	_, err = (RedisStreamGroups{RDB: rdb2}).StreamGroups(context.Background())
	if err == nil || !strings.Contains(err.Error(), "q:dns") || !strings.Contains(err.Error(), "XLEN") {
		t.Errorf("ошибка XLEN = %v, want с именем q:dns и XLEN", err)
	}

	if _, err := (RedisStreamGroups{}).StreamGroups(context.Background()); err == nil {
		t.Error("nil-клиент: ошибки нет")
	}
}

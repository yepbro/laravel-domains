// Тесты стрима очереди фолбэка рендера q:render-fallback (T-237, ТЗ 4.3,
// ФТ-6) без живого Redis: пин allStreams-инварианта (стрим известен всей
// очереди — knownStream/workingStreams/WorkingGroups), потолок MAXLEN
// политики (свой, не штучной сотни тысяч — бюджет, docs/redis.md §3.1),
// освобождение подтверждённого префикса (TrimOnce, trim_test.go) и
// возврат зависших с переводом в q:dead (Reliab.Claim, reliab_test.go).
// Формат сообщения — существующий контракт Result, отдельной таблицы не
// нужно: roundtrip q:render-fallback — в message_test.go.

package queue

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRenderFallbackAllStreamsInvariant — allStreams-инвариант: состав
// стримов очереди — ровно константы пакета, q:render-fallback включён
// (ТЗ 4.1 п.3: попадание в allStreams даёт trim/claim/надёжность
// автоматически). Новый стрим без allStreams молча выпадает из
// освобождения и клейма — пин ловит.
func TestRenderFallbackAllStreamsInvariant(t *testing.T) {
	want := []string{StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze, StreamDead}
	if !reflect.DeepEqual(allStreams, want) {
		t.Errorf("allStreams = %v, хочу %v (новый стрим добавьте в allStreams — queue.go)", allStreams, want)
	}
	if !knownStream(StreamRenderFallback) {
		t.Errorf("knownStream(%q) = false: стрим вне allStreams — Publish/Consume/Claim его отвергнут", StreamRenderFallback)
	}
	// Рабочий стрим (без q:dead): освобождается TrimLoop и чистится
	// по группам — группа фолбэка есть (воркер B, ФТ-12).
	working := false
	for _, s := range workingStreams() {
		if s == StreamRenderFallback {
			working = true
		}
	}
	if !working {
		t.Errorf("workingStreams = %v: нет %q — подтверждённый префикс не освобождается", workingStreams(), StreamRenderFallback)
	}
	if _, ok := WorkingGroups()[StreamRenderFallback]; !ok {
		t.Errorf("WorkingGroups: нет пары для %q — консьюмеры группы не чистятся (T-155)", StreamRenderFallback)
	}
}

// TestRenderFallbackMaxLen — потолок политики T-025 для фолбэк-стрима:
// собственный (бюджетная доля штучного потока, docs/redis.md §3.1), а не
// штучные 150 000 (пробивал бы бюджет Redis при худшей пачке q:dns) и не
// потолок q:dead; Publish усекает стрим им (XADD MAXLEN ~).
func TestRenderFallbackMaxLen(t *testing.T) {
	if got := DefaultStreamMaxLen(StreamRenderFallback); got != defaultMaxLenRenderFallback {
		t.Errorf("DefaultStreamMaxLen(%s) = %d, хочу %d (политика T-025)", StreamRenderFallback, got, defaultMaxLenRenderFallback)
	}
	if defaultMaxLenRenderFallback >= defaultMaxLenUnit {
		t.Errorf("потолок фолбэка %d ≥ штучному %d — фолбэк обязан быть долей потока этапа (бюджет, ТЗ 3.2)",
			defaultMaxLenRenderFallback, defaultMaxLenUnit)
	}

	fake := &fakeRedis{}
	q := NewRedisQueue(fake, RedisOptions{})
	res := Result{ZoneID: 3, DomainID: 42, SubdomainID: 1, Variant: 1, Name: "example.com"}
	if _, err := q.Publish(context.Background(), StreamRenderFallback, res); err != nil {
		t.Fatalf("Publish Result в %s: %v", StreamRenderFallback, err)
	}
	add := fake.adds[0]
	if add.Stream != StreamRenderFallback {
		t.Errorf("XADD стрим %q, хочу %q", add.Stream, StreamRenderFallback)
	}
	if want := DefaultStreamMaxLen(StreamRenderFallback); add.MaxLen != want || !add.Approx {
		t.Errorf("XADD MAXLEN ~ %d не применён: MaxLen=%d Approx=%v, хочу %d (политика T-025)",
			want, add.MaxLen, add.Approx, want)
	}
}

// TestRenderFallbackTrim — освобождение подтверждённого префикса
// фолбэк-стрима (по образцу TestTrimOncePendingKept): точка обрезки —
// старейшая pending группы (зависший PEL не удаляется), дефолтные Streams
// TrimOptions стрим покрывают (workingStreams).
func TestRenderFallbackTrim(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamRenderFallback: {{Name: "render-fallback-worker", Pending: 2, LastDeliveredID: "205-0"}},
		},
		pend: map[string]redis.XPending{
			StreamRenderFallback + "|render-fallback-worker": {Count: 2, Lower: "201-0", Higher: "204-0"},
		},
		trimCount: 4,
	}
	n, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamRenderFallback}})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if n != 4 {
		t.Errorf("удалено %d, хочу 4 (ответ XTRIM)", n)
	}
	if len(f.trims) != 1 || f.trims[0]["stream"] != StreamRenderFallback || f.trims[0]["minid"] != "201-0" {
		t.Fatalf("XTRIM %+v, хочу MINID 201-0 в %s (старейшая pending группы)", f.trims, StreamRenderFallback)
	}

	covered := false
	for _, s := range (TrimOptions{}).withDefaults().Streams {
		if s == StreamRenderFallback {
			covered = true
		}
	}
	if !covered {
		t.Errorf("дефолтные Streams TrimOptions не покрывают %s (workingStreams)", StreamRenderFallback)
	}
}

// TestRenderFallbackClaim — возврат зависших фолбэк-записей (по образцу
// TestReliabClaim/TestReliabClaimDead): счётчик попыток из PEL
// возвращается в работу; исчерпавшая попытки запись уходит в q:dead —
// Dead валиден с источником q:render-fallback и идентификаторами
// предмета, выжатыми из тела Result (deadSubjectOf), исходная запись
// подтверждается.
func TestRenderFallbackClaim(t *testing.T) {
	fresh := `{"task_id":"t1"}`
	subject := `{"task_id":"t2","zone_id":3,"domain_id":42,"subdomain_id":1,"variant":1,"name":"www.example.com"}`
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "301-0", Values: map[string]any{fieldData: []byte(fresh)}},
			{ID: "302-0", Values: map[string]any{fieldData: []byte(subject)}},
		},
		claimNext: "303-0",
		pendRes: map[string][]redis.XPendingExt{
			"301-0": {{ID: "301-0", RetryCount: 2}}, // попытки остаются — в работу
			"302-0": {{ID: "302-0", RetryCount: 4}}, // лимит 3 исчерпан — q:dead
		},
	}
	r := newTestReliab(fake, ReliabOptions{})

	got, _, next, err := r.Claim(context.Background(), StreamRenderFallback,
		"render-fallback-worker", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if next != "303-0" {
		t.Errorf("курсор = %q, хочу 303-0", next)
	}
	want := []Claimed{
		{Delivery{StreamRenderFallback, "301-0", []byte(fresh)}, 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Claim:\n получил  %+v\n хочу     %+v", got, want)
	}

	// Исчерпавшая — q:dead: источник фолбэк-стрима, предмет из тела Result.
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("публикаций в q:dead: %+v, хочу одну", fake.adds)
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	wantDead := Dead{
		TaskID:      "t2",
		Stream:      StreamRenderFallback,
		EntryID:     "302-0",
		Attempts:    4,
		Reason:      "исчерпан лимит попыток 3: доставок без подтверждения — 4",
		ZoneID:      3,
		DomainID:    42,
		SubdomainID: 1,
		Variant:     1,
		Name:        "www.example.com",
	}
	if !reflect.DeepEqual(dead, wantDead) {
		t.Errorf("Dead:\n получил  %+v\n хочу     %+v", dead, wantDead)
	}

	// Исходная запись покидает pending: XACK из фолбэк-стрима.
	if want := []ackCall{{StreamRenderFallback, "render-fallback-worker", []string{"302-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
	}
}

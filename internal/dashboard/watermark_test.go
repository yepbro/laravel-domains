// Юнит-тесты эффективных порогов очередей снимка (T-144, ТЗД §5.4):
// пороги подсветки дашборда — queue.StreamWatermark по каждому рабочему
// стриму, тот же единый расчёт, что у алерта переполнения очередей
// монитора (T-080: wm[s] = queue.StreamWatermark(QueueWatermark,
// QueueMaxLen, s)) и гейта публикации (T-026) — второй копии формулы
// нет, порог дашборда обязан равняться порогу монитора при любых
// связках конфиг×MAXLEN: дефолты (кламп по T-141: q:dns — 125 пачек,
// штучные — 75 000), явно заниженный watermark, нестандартный MaxLen,
// отключённый гейт. Пороги — данные конфигурации: деградация Redis
// (секция очередей) их не трогает.
package dashboard

import (
	"context"
	"errors"
	"testing"

	"crawler/internal/queue"
	"crawler/internal/status"
)

// TestQueueWatermarksSync — порог дашборда == порогу монитора: для
// каждой связки QueueWatermark×QueueMaxLen и каждого рабочего стрима
// значение снимка равно queue.StreamWatermark с теми же опциями
// (ровно так считает алерт монитора, T-080); точные числа ключевых
// связок — против ожиданий, чтобы связка с политикой T-141 не молчала
// при регрессии. q:dead порога не имеет (гейтом не ограничивается).
func TestQueueWatermarksSync(t *testing.T) {
	cases := []struct {
		name      string
		watermark int64
		maxLen    int64
		want      map[string]int64
	}{
		{
			"дефолты конфигурации: кламп по пределам T-141",
			100000, 0,
			map[string]int64{
				queue.StreamDNS: 125, queue.StreamHTTP: 75000,
				queue.StreamRender: 75000, queue.StreamAnalyze: 75000,
			},
		},
		{
			"явно заниженный watermark ниже всех пределов",
			50, 0,
			map[string]int64{
				queue.StreamDNS: 50, queue.StreamHTTP: 50,
				queue.StreamRender: 50, queue.StreamAnalyze: 50,
			},
		},
		{
			"нестандартный MaxLen: кламп по его половине",
			100000, 300,
			map[string]int64{
				queue.StreamDNS: 150, queue.StreamHTTP: 150,
				queue.StreamRender: 150, queue.StreamAnalyze: 150,
			},
		},
		{
			"MaxLen выше watermark: без клампа",
			100000, 250000,
			map[string]int64{
				queue.StreamDNS: 100000, queue.StreamHTTP: 100000,
				queue.StreamRender: 100000, queue.StreamAnalyze: 100000,
			},
		},
		{
			"гейт отключён (watermark ≤ 0): пороги 0",
			0, 0,
			map[string]int64{
				queue.StreamDNS: 0, queue.StreamHTTP: 0,
				queue.StreamRender: 0, queue.StreamAnalyze: 0,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			coll := newSnapCollector(t, healthyPool(), &fakeLenner{}, func(o *CollectorOptions) {
				o.QueueWatermark = tc.watermark
				o.QueueMaxLen = tc.maxLen
			})
			snap := coll.CollectOnce(context.Background())
			for _, s := range status.Streams {
				if s == queue.StreamDead {
					if _, ok := snap.QueueWatermarks[s]; ok {
						t.Errorf("queue_watermarks[%s] есть, хочу отсутствие (q:dead гейтом не ограничивается)", s)
					}
					continue
				}
				// Порог монитора: queue.StreamWatermark с теми же
				// опциями (alerts.go evalQueues, T-080).
				monitorWM := queue.StreamWatermark(tc.watermark, tc.maxLen, s)
				if got := snap.QueueWatermarks[s]; got != monitorWM {
					t.Errorf("queue_watermarks[%s] = %d, хочу %d (порог монитора)", s, got, monitorWM)
				}
				if got := snap.QueueWatermarks[s]; got != tc.want[s] {
					t.Errorf("queue_watermarks[%s] = %d, хочу %d (точное значение связки)", s, got, tc.want[s])
				}
			}
		})
	}
}

// TestQueueWatermarksNotSection — пороги подсветки не секция: отказ
// Redis (деградация очередей) не трогает их — подсказка секции остаётся
// оператору и при «н/д» глубин (§5.9: данные конфигурации, деградировать
// не могут).
func TestQueueWatermarksNotSection(t *testing.T) {
	lens := &fakeLenner{}
	lens.err.Store(errors.New("redis: timeout"))
	coll := newSnapCollector(t, healthyPool(), lens, func(o *CollectorOptions) {
		o.QueueWatermark = 100000
	})

	snap := coll.CollectOnce(context.Background())
	if snap.Queues != nil {
		t.Fatal("queues не null при отказе Redis (предусловие теста)")
	}
	want := map[string]int64{
		queue.StreamDNS: 125, queue.StreamHTTP: 75000,
		queue.StreamRender: 75000, queue.StreamAnalyze: 75000,
	}
	for s, wm := range want {
		if got := snap.QueueWatermarks[s]; got != wm {
			t.Errorf("queue_watermarks[%s] = %d при лежащем Redis, хочу %d", s, got, wm)
		}
	}
}

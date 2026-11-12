// Unit tests of the snapshot's effective queue thresholds (T-144,
// dashboard spec §5.4): the dashboard highlight thresholds —
// queue.StreamWatermark per each working stream, the same single
// computation as the monitor queue overflow alert (T-080:
// wm[s] = queue.StreamWatermark(QueueWatermark, QueueMaxLen, s)) and
// the publication gate (T-026) — there is no second copy of the formula,
// the dashboard threshold must equal the monitor threshold for any
// config×MAXLEN combination: defaults (the T-141 clamp: q:dns — 125
// chunks, single items — 75,000), an explicitly lowered watermark, a
// non-standard MaxLen, a disabled gate. The thresholds are
// configuration data: a Redis degradation (the queues section) does
// not touch them.
package dashboard

import (
	"context"
	"errors"
	"testing"

	"crawler/internal/queue"
	"crawler/internal/status"
)

// TestQueueWatermarksSync — the dashboard threshold == the monitor
// threshold: for every QueueWatermark×QueueMaxLen combination and each
// working stream the snapshot value equals queue.StreamWatermark with
// the same options (exactly how the monitor alert computes it, T-080);
// the exact numbers of the key combinations are asserted against, so
// that the combination with the T-141 policy does not stay silent on
// regression. q:dead has no threshold (not limited by the gate).
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
				// q:render-fallback: MAXLEN 15,000 (the T-237 policy) → 7,500.
				queue.StreamRenderFallback: 7500,
			},
		},
		{
			"явно заниженный watermark ниже всех пределов",
			50, 0,
			map[string]int64{
				queue.StreamDNS: 50, queue.StreamHTTP: 50,
				queue.StreamRender: 50, queue.StreamAnalyze: 50,
				queue.StreamRenderFallback: 50,
			},
		},
		{
			"нестандартный MaxLen: кламп по его половине",
			100000, 300,
			map[string]int64{
				queue.StreamDNS: 150, queue.StreamHTTP: 150,
				queue.StreamRender: 150, queue.StreamAnalyze: 150,
				queue.StreamRenderFallback: 150,
			},
		},
		{
			"MaxLen выше watermark: без клампа",
			100000, 250000,
			map[string]int64{
				queue.StreamDNS: 100000, queue.StreamHTTP: 100000,
				queue.StreamRender: 100000, queue.StreamAnalyze: 100000,
				queue.StreamRenderFallback: 100000,
			},
		},
		{
			"гейт отключён (watermark ≤ 0): пороги 0",
			0, 0,
			map[string]int64{
				queue.StreamDNS: 0, queue.StreamHTTP: 0,
				queue.StreamRender: 0, queue.StreamAnalyze: 0,
				queue.StreamRenderFallback: 0,
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
				// The monitor threshold: queue.StreamWatermark with the
				// same options (alerts.go, the queue_overflow machine,
				// T-080).
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

// TestQueueWatermarksNotSection — highlight thresholds are not a
// section: a Redis refusal (queue degradation) does not touch them —
// the section hint stays with the operator even at "n/a" depths (§5.9:
// configuration data, cannot degrade).
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

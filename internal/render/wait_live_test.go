package render

// Live-тесты стратегии ожидания (T-063) — против реального Chromium
// dev-стека. Включение и адрес — как в browser_live_test.go:
// CRAWLER_RENDER_TEST=1 (адрес: CRAWLER_RENDER_REMOTE_URL, затем
// CHROMIUM_WS_URL, по умолчанию ws://172.28.100.14:9222).
//
//	внешней сети не требуется: страницы — data: URL; «шумная» страница
//	порождает сетевые запросы fetch на TEST-NET (192.0.2.1, RFC 5737) —
//	событие Network.requestWillBeSent приходит в момент инициирования
//	запроса, ответ не нужен.

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"
)

// waitLiveNoisyURL — data:-страница с фоновым трафиком: запрос каждые
// 300 мс. Запросы на TEST-NET не отвечают (сеть тестbed их уронит или
// молча уронит маршрутизатор) — для окна затишья важно лишь событие
// инициирования Network.requestWillBeSent.
func waitLiveNoisyURL() string {
	body := `<title>noisy</title><script>setInterval(function(){fetch("http://192.0.2.1/ping").catch(function(){})},300)</script>`
	return "data:text/html," + url.QueryEscape(body)
}

// waitLiveStuckURL — data:-страница с вечным парсер-блокирующим скриптом:
// событие load не наступает (скрипт не finishes), а съём результата
// (Runtime.evaluate заголовка) висит на занятом рендерере — общий таймаут
// режет визит целиком.
func waitLiveStuckURL() string {
	body := `<title>stuck</title><script>for(;;);</script>`
	return "data:text/html," + url.QueryEscape(body)
}

func TestWaitLiveSettleQuiet(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	opts := liveOptions()
	// Бюджет liveOptions — 20 с: план — load 10 с, окно 2 с, потолок
	// затишья 4 с. Тихая страница обязана закончиться окном (≈2 с), не
	// потолком и не общим таймаутом.
	b, err := StartBrowser(ctx, opts)
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	p := NewPool(b)
	defer p.Close()

	want := dataURL("settle-quiet")
	start := time.Now()
	out, err := p.Run(ctx, RenderTask{URL: want})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Title != "settle-quiet" || out.FinalURL != want {
		t.Fatalf("результат %+v", out)
	}
	plan := DefaultWaitPlan(opts.Timeout)
	if elapsed < plan.Quiet-100*time.Millisecond {
		t.Errorf("визит %s: окно затишья %s не выдержано (раньше окна)", elapsed, plan.Quiet)
	}
	if elapsed >= plan.Settle {
		t.Errorf("визит %s дошёл до потолка затишья %s — тихая страница должна заканчиваться окном", elapsed, plan.Settle)
	}
	if elapsed >= opts.Timeout {
		t.Errorf("визит %s дотянул до общего таймаута %s — тихая страница должна заканчиваться окном", elapsed, opts.Timeout)
	}
}

func TestWaitLiveSettleNoisy(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	opts := liveOptions()
	// Бюджет liveOptions — 20 с: план — load 10 с, окно 2 с, потолок
	// затишья 4 с. Непрерывные запросы двигают окно, и визит обязан
	// закончиться потолком затишья (≈4 с): раньше — окно «схлопнулось» бы
	// (стратегия не видит запросы), позже — общий таймаут.
	b, err := StartBrowser(ctx, opts)
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	p := NewPool(b)
	defer p.Close()

	want := waitLiveNoisyURL()
	start := time.Now()
	out, err := p.Run(ctx, RenderTask{URL: want})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v (потолок затишья — не ошибка)", err)
	}
	if out.Title != "noisy" || out.FinalURL != want {
		t.Fatalf("результат %+v", out)
	}
	plan := DefaultWaitPlan(opts.Timeout)
	if elapsed < plan.Settle-100*time.Millisecond {
		t.Errorf("визит %s закончился раньше потолка затишья %s — окно подтвердилось при непрерывных запросах", elapsed, plan.Settle)
	}
	if elapsed >= opts.Timeout {
		t.Errorf("визит %s дотянул до общего таймаута %s", elapsed, opts.Timeout)
	}
}

func TestWaitLiveOverallTimeout(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	opts := liveOptions()
	// Крошечный общий таймаут: план — load 1 с (не наступит), окно 200 мс,
	// потолок затишья 400 мс; съём результата висит на занятом рендерере —
	// общий таймаут обязан оборвать визит ошибкой задачи.
	opts.Timeout = 2 * time.Second
	b, err := StartBrowser(ctx, opts)
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	p := NewPool(b)
	defer p.Close()

	start := time.Now()
	_, err = p.Run(ctx, RenderTask{URL: waitLiveStuckURL()})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("ожидана ошибка общего таймаута, получен успех")
	}
	if ClassOf(err) != ClassTask {
		t.Errorf("класс %v (%v), хочу task — таймаут страницы без повторов контекста", ClassOf(err), err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка %v не содержит context.DeadlineExceeded", err)
	}
	if elapsed < opts.Timeout {
		t.Errorf("визит %s оборван раньше общего таймаута %s", elapsed, opts.Timeout)
	}
	// Ошибка задачи — счётчик страниц не растёт, повторной попытки нет.
	if got := b.Pages(); got != 0 {
		t.Errorf("счётчик страниц %d, хочу 0 (неуспешная задача)", got)
	}
}

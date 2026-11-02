package render

// Live-тесты стратегии ожидания (T-063) — против реального Chromium
// dev-стека. Включение и адрес — как в browser_live_test.go:
// CRAWLER_RENDER_TEST=1 (адрес: CRAWLER_RENDER_REMOTE_URL, затем
// CHROMIUM_WS_URL, по умолчанию ws://172.28.100.14:9222).
//
//	Страницы — HTTP-фикстуры (liveServe в browser_live_test.go): внешней
//	сети не требуется, «шумная» страница порождает сетевые запросы fetch
//	на TEST-NET (192.0.2.1, RFC 5737) — событие Network.requestWillBeSent
//	приходит в момент инициирования запроса, ответ не нужен. data:-страницы
//	(герметичность без сервера) невозможны после R-09: validateTaskURL
//	отвергает не-http схемы (T-216).

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// waitLiveServe — страницы стратегий ожидания на локальном сервере.
// quiet — тихая страница (load и ни одного запроса после); noisy — фоновый
// трафик: запрос каждые 300 мс на TEST-NET, ответы не приходят (сеть
// testbed их уронит или молча уронит маршрутизатор) — для окна затишья
// важно лишь событие инициирования Network.requestWillBeSent; stuck —
// вечный парсер-блокирующий скрипт: событие load не наступает (скрипт не
// завершается), а съём результата (Runtime.evaluate заголовка) висит на
// занятом рендерере — общий таймаут режет визит целиком.
func waitLiveServe(t *testing.T) (quiet, noisy, stuck string) {
	t.Helper()
	page := func(body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = fmt.Fprint(w, body)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/quiet", page(`<title>settle-quiet</title>hello`))
	mux.HandleFunc("/noisy", page(`<title>noisy</title><script>`+
		`setInterval(function(){fetch("http://192.0.2.1/ping").catch(function(){})},300)`+
		`</script>`))
	mux.HandleFunc("/stuck", page(`<title>stuck</title><script>for(;;);</script>`))
	base := liveServe(t, mux)
	return base + "/quiet", base + "/noisy", base + "/stuck"
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

	want, _, _ := waitLiveServe(t)
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

	_, want, _ := waitLiveServe(t)
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

	_, _, stuck := waitLiveServe(t)
	start := time.Now()
	_, err = p.Run(ctx, RenderTask{URL: stuck})
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

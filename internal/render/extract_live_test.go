package render

// Live-проверки съёма результата T-064 против реального Chromium dev-стека
// (включение и адрес — как в browser_live_test.go: CRAWLER_RENDER_TEST=1,
// compose-сервис chromium). Два уровня:
//
//   - статическая страница HTTP-фикстуры (liveServe): точный outerHTML
//     после исполнения скрипта (дерево дорисовано JS), cookies нет,
//     заголовки ответа основной навигации сняты с реального сетевого
//     ответа (data:-страницы с псевдо-ответом невозможны после R-09:
//     validateTaskURL отвергает не-http схемы, T-216);
//   - внутренняя страница spa-shell.crawl.test (testbed): JS-cookie,
//     выставленная скриптом синхронно при разборе (document.cookie — до
//     события load), видна в CDP-наборе; заголовки ответа основной
//     навигации сняты.
//
// Асинхронный контент spa-shell (setTimeout 150 мс) здесь не проверяется:
// ожидание «затишья» сети — T-063, без него съём идёт сразу после load.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// TestExtractLiveStaticPage — съём результата на статической странице
// фикстуры: скрипт меняет текст синхронно — дорисовка обязана попасть в
// итоговый DOM к моменту съёма (после события load); cookies нет (чистый
// изолированный контекст, страница их не выставляет); заголовки — с
// реального ответа сервера фикстуры.
func TestExtractLiveStaticPage(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/dom", func(w http.ResponseWriter, _ *http.Request) {
		// X-Live-Fixture — фирменный заголовок фикстуры: его наличие в
		// результате доказывает, что заголовки сняты с реального ответа
		// сервера (равно как Content-Type мог бы дать и псевдо-ответ).
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("X-Live-Fixture", "1")
		_, _ = fmt.Fprint(w, "<html><head><title>ext</title></head><body>"+
			`<div id="d">before</div>`+
			`<script>document.getElementById("d").textContent="after-js";</script>`+
			"</body></html>")
	})
	page := liveServe(t, mux) + "/dom"

	b, err := StartBrowser(ctx, liveOptions())
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	defer b.Close()

	s, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()

	out, err := s.Run(ctx, RenderTask{URL: page})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.FinalURL != page {
		t.Errorf("итоговый URL %q, хочу %q", out.FinalURL, page)
	}
	want := "<html><head><title>ext</title></head><body>" +
		`<div id="d">after-js</div>` +
		`<script>document.getElementById("d").textContent="after-js";</script>` +
		"</body></html>"
	if out.DOM != want {
		t.Fatalf("итоговый DOM:\n got  %q\nwant %q", out.DOM, want)
	}
	if len(out.Cookies) != 0 {
		t.Errorf("cookies у страницы без них: %+v, хочу пусто", out.Cookies)
	}
	// Заголовки реального ответа основной навигации: Content-Type сервера
	// фикстуры и фирменный X-Live-Fixture (ключи канонические, как у
	// HTTP-этапа; полный набор заголовков сетевого ответа не фиксирован —
	// Date и пр. добавляет сам сервер).
	if ct := out.ResponseHeaders.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type %q, хочу text/html…", ct)
	}
	if got := out.ResponseHeaders.Get("X-Live-Fixture"); got != "1" {
		t.Errorf("X-Live-Fixture %q, хочу 1 — заголовки не с реального ответа фикстуры", got)
	}
}

func TestExtractLiveInternalPage(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	b, err := StartBrowser(ctx, liveOptions())
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	defer b.Close()

	s, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()

	// spa-shell.crawl.test: сценарий testbed «DOM и cookies от JS» — скрипт
	// выставляет document.cookie синхронно (до load) и рисует контент в
	// #root по таймеру (после load — территория ожидания T-063).
	out, err := s.Run(ctx, RenderTask{URL: "http://spa-shell.crawl.test/"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.FinalURL != "http://spa-shell.crawl.test/" {
		t.Errorf("итоговый URL %q", out.FinalURL)
	}

	// JS-cookie видна в CDP-наборе: хранилище контекста читается командой,
	// а не document.cookie.
	var js *ExtractCookie
	for i := range out.Cookies {
		if out.Cookies[i].Name == "js_session" {
			js = &out.Cookies[i]
		}
	}
	if js == nil {
		t.Fatalf("JS-cookie js_session не снята: %+v", out.Cookies)
	}
	if js.Value != "rendered-by-js" || js.Path != "/" || js.Domain != "spa-shell.crawl.test" {
		t.Errorf("js_session: %+v", js)
	}

	// Дерево страницы после загрузки: каркас SPA на месте.
	if !strings.Contains(out.DOM, `<div id="root">`) {
		t.Errorf("итоговый DOM без каркаса SPA: %q", out.DOM)
	}
	if !strings.HasPrefix(out.DOM, "<html") {
		t.Errorf("итоговый DOM не с корневого элемента: %q", out.DOM)
	}

	// Заголовки ответа основной навигации (ТЗ 5.8): ключи канонические,
	// как у HTTP-этапа.
	if ct := out.ResponseHeaders.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type %q, хочу text/html…", ct)
	}
	if out.ResponseHeaders.Get("Server") == "" {
		t.Errorf("заголовок Server не снят: %v", out.ResponseHeaders)
	}
}

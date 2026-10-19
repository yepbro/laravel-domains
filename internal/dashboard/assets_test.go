// Юнит-тесты фронта дашборда (T-130, ТЗД §6.1, §6.4–6.5): рендер
// страницы за авторизацией (секции ru-разметки §5.1–5.9, подключение
// статики, период опроса), роуты статики из embed (Content-Type,
// ненулевые тела, 404 вне карты файлов), отсутствие внешних источников
// (§3.2.2). Заголовки всех ответов — §6.3 (TestSecurityHeaders,
// dashboard_test.go).
package dashboard

import (
	"net/http"
	"strings"
	"testing"
)

// pageWants — обязательные строки разметки страницы: карточки итогов и
// скоростей (§5.1–5.2), заголовки секций (§5.3–5.8), «н/д» до первого
// снимка (§5.9), подключение статики и период опроса (§6.4).
var pageWants = []string{
	`<html lang="ru">`, // §6.5: русский язык интерфейса
	"Дашборд Domain Crawler",
	// Карточки §5.1–5.2.
	"Всего доменов",
	"Проверено",
	"В работе",
	"Дом./с",
	"HTTP-рез./с",
	"Ренд./с",
	"ETA цикла",
	"Laravel-находок",
	// Секции §5.3–5.8.
	"Очереди",
	"Зоны",
	"Статусы доменов",
	"Последние находки Laravel",
	"Последние ingest-файлы",
	"Здоровье сервисов",
	// §5.9: плейсхолдеры деградации до первого снимка/секции.
	"н/д",
	// §6.4: конфигурация опроса и стили/скрипт только свои.
	`<meta name="dashboard-refresh" content="5">`,
	`<link rel="stylesheet" href="/assets/style.css">`,
	`<script src="/assets/app.js" defer></script>`,
}

// TestIndexPage — GET / за авторизацией: страница дашборда из embed —
// 200, text/html, все секции ru-разметки на месте.
func TestIndexPage(t *testing.T) {
	h := newTestServer(t, nil).Handler()

	rec := doReq(h, http.MethodGet, "/", "10.0.0.30:3030", authed())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / = %d, хочу 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("GET /: Content-Type = %q, хочу text/html; charset=utf-8", ct)
	}
	if rec.Body.Len() == 0 {
		t.Fatal("GET /: пустое тело")
	}
	body := rec.Body.String()
	for _, want := range pageWants {
		if !strings.Contains(body, want) {
			t.Errorf("GET /: разметка без %q:\n%s", want, body)
		}
	}
	// Спарклайны скоростей — SVG polyline (§6.4).
	if !strings.Contains(body, "<polyline") {
		t.Error("GET /: нет SVG polyline спарклайнов (§6.4)")
	}
}

// TestIndexNoExternalSources — ТЗД §3.2.2: без внешних CDN, шрифтов
// и счётчиков — в разметке нет ни одного внешнего адреса (свои
// артефакты и API — относительные пути).
func TestIndexNoExternalSources(t *testing.T) {
	h := newTestServer(t, nil).Handler()
	body := doReq(h, http.MethodGet, "/", "10.0.0.30:3031", authed()).Body.String()
	for _, bad := range []string{"http://", "https://", "integrity=", "crossorigin"} {
		if strings.Contains(body, bad) {
			t.Errorf("GET /: разметка ссылается на внешний источник (%q), §3.2.2", bad)
		}
	}
}

// TestAssets — GET /assets/{app.js,style.css} за авторизацией: 200,
// явные Content-Type (карта assetTypes, не системный mime-файл) и
// ненулевые тела; имена вне карты и каталог — 404 без листинга.
func TestAssets(t *testing.T) {
	h := newTestServer(t, nil).Handler()

	for _, tc := range []struct {
		path        string
		contentType string
	}{
		{"/assets/app.js", "application/javascript; charset=utf-8"},
		{"/assets/style.css", "text/css; charset=utf-8"},
	} {
		rec := doReq(h, http.MethodGet, tc.path, "10.0.0.31:3131", authed())
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, хочу 200", tc.path, rec.Code)
			continue
		}
		if ct := rec.Header().Get("Content-Type"); ct != tc.contentType {
			t.Errorf("GET %s: Content-Type = %q, хочу %q", tc.path, ct, tc.contentType)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
			t.Errorf("GET %s: Cache-Control = %q, хочу no-cache", tc.path, cc)
		}
		if rec.Body.Len() == 0 {
			t.Errorf("GET %s: пустое тело", tc.path)
		}
	}

	for _, path := range []string{"/assets/nope.js", "/assets/", "/assets/page.html"} {
		rec := doReq(h, http.MethodGet, path, "10.0.0.31:3132", authed())
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, хочу 404 (файл вне карты отдачи)", path, rec.Code)
		}
	}
}

// Unit tests of the dashboard front end (T-130, dashboard spec §6.1,
// §6.4–6.5): rendering of the page behind authorization (the sections
// of the ru markup §5.1–5.9, statics wiring, the polling period), the
// statics routes from embed (Content-Type, non-empty bodies, 404
// outside the file map), the absence of external sources (§3.2.2).
// The headers of all responses — §6.3 (TestSecurityHeaders,
// dashboard_test.go).
package dashboard

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// pageWants — the mandatory strings of the page markup: totals and
// rates cards (§5.1–5.2), section headings (§5.3–5.8), "n/a" before
// the first snapshot (§5.9), statics wiring and the polling period
// (§6.4).
var pageWants = []string{
	`<html lang="ru">`, // §6.5: Russian interface language
	"Дашборд Domain Crawler",
	// §5.1–5.2 cards.
	"Всего доменов",
	"Проверено",
	"В работе",
	"Дом./с",
	"HTTP-рез./с",
	"Ренд./с",
	"ETA цикла",
	"Laravel-находок",
	// §5.3–5.8 sections.
	"Очереди",
	"Зоны",
	"Статусы доменов",
	"Последние находки Laravel",
	"Последние ingest-файлы",
	"Здоровье сервисов",
	// §5.9: degradation placeholders before the first snapshot/section.
	"н/д",
	// The render engines panel (T-246).
	"Движки рендера",
	// §6.4: polling configuration and styles/script of our own only.
	`<meta name="dashboard-refresh" content="5">`,
	`<link rel="stylesheet" href="/assets/style.css">`,
	`<script src="/assets/app.js" defer></script>`,
}

// TestIndexPage — GET / behind authorization: the dashboard page from
// embed — 200, text/html, all ru markup sections in place.
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
	// Rate sparklines — an SVG polyline (§6.4).
	if !strings.Contains(body, "<polyline") {
		t.Error("GET /: нет SVG polyline спарклайнов (§6.4)")
	}
}

// TestIndexNoExternalSources — dashboard spec §3.2.2: no external
// CDNs, fonts or counters — the markup has not a single external
// address (own artifacts and the API — relative paths).
func TestIndexNoExternalSources(t *testing.T) {
	h := newTestServer(t, nil).Handler()
	body := doReq(h, http.MethodGet, "/", "10.0.0.30:3031", authed()).Body.String()
	for _, bad := range []string{"http://", "https://", "integrity=", "crossorigin"} {
		if strings.Contains(body, bad) {
			t.Errorf("GET /: разметка ссылается на внешний источник (%q), §3.2.2", bad)
		}
	}
}

// TestAssets — GET /assets/{app.js,style.css} behind authorization:
// 200, explicit Content-Type (the assetTypes map, not the system mime
// file) and non-empty bodies; names outside the map and a directory —
// 404 without listing.
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

// TestServeAssetMissing — an artifact outside embed (a build error,
// not a request error): an honest 500 "asset unavailable" and an
// Error in the log with the artifact name. White box: the /assets/*
// branch cannot reach it — names are filtered by the assetTypes map
// (404 earlier), so serveAsset is called directly with a name that is
// not in embed.
func TestServeAssetMissing(t *testing.T) {
	var logs syncBuf
	srv := newTestServer(t, func(o *Options) { o.Log = slog.New(slog.NewTextHandler(&logs, nil)) })

	rec := httptest.NewRecorder()
	srv.serveAsset(rec, "missing.bin", "text/plain; charset=utf-8")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("serveAsset(нет в embed) = %d, хочу 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "asset unavailable") {
		t.Errorf("тело = %q, хочу диагноз asset unavailable", rec.Body.String())
	}
	if s := logs.String(); !strings.Contains(s, "артефакт фронта отсутствует в embed") || !strings.Contains(s, "missing.bin") {
		t.Errorf("Error без имени артефакта:\n%s", s)
	}
}

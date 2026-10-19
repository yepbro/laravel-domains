// Фронт дашборда (ТЗД §5, §6.4–6.5, T-130): единая русскоязычная
// страница (assets/page.html), скрипт автообновления (assets/app.js) и
// стили (assets/style.css) — только артефакты go:embed (прецеденты —
// migrations/embed.go и static internal/web), внешних CDN и шрифтов нет
// (ТЗД §3.2.2: окружение без интернета; CSP §6.3 их и не пустил бы).
// Рендер полностью клиентский: страница статична и одинакова для всех
// вкладок, данные приходят опросом /api/status (T-129) — сервер ничего
// не шаблонизирует и не знает о состоянии при отдаче HTML.
//
// Content-Type задан явной картой, а не распознаванием http.FileServer:
// mime.TypeByExtension зависит от системного /etc/mime.types (например,
// дебиановский отдаёт .js как text/javascript) — контракт роута должен
// быть детерминированным; заодно нет листинга каталога и ходов по
// дереву файловой системы: имя вне карты — честный 404.
package dashboard

import (
	"embed"
	"net/http"
	"strings"
)

//go:embed assets
var assetsFS embed.FS

// assetTypes — файлы, отдаваемые под /assets/*: имя → Content-Type.
var assetTypes = map[string]string{
	"app.js":    "application/javascript; charset=utf-8",
	"style.css": "text/css; charset=utf-8",
}

// handleIndex — GET /: страница дашборда из embed (ТЗД §6.1).
// Cache-Control: no-cache — имена артефактов без хэшей, переезд на новую
// версию должен подхватываться сразу (API рядом и так no-store).
func (s *Server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	s.serveAsset(w, "page.html", "text/html; charset=utf-8")
}

// handleAssets — GET /assets/*: статику фронта из embed (ТЗД §6.1);
// имя вне assetTypes (в том числе каталог и «..») — 404.
func (s *Server) handleAssets(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/assets/")
	contentType, ok := assetTypes[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.serveAsset(w, name, contentType)
}

// serveAsset — файл каталога assets/ с явным Content-Type и no-cache.
// Отсутствие файла в embed — ошибка сборки, а не запроса: 500 и Error
// в лог (стиль web.handleRobots).
func (s *Server) serveAsset(w http.ResponseWriter, name, contentType string) {
	b, err := assetsFS.ReadFile("assets/" + name)
	if err != nil {
		s.opt.Log.Error("артефакт фронта отсутствует в embed", "asset", name, "err", err)
		http.Error(w, "asset unavailable", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Cache-Control", "no-cache")
	_, _ = w.Write(b)
}

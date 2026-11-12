// Dashboard front end (dashboard spec §5, §6.4–6.5, T-130): a single
// Russian-language page (assets/page.html), an auto-refresh script
// (assets/app.js) and styles (assets/style.css) — go:embed artifacts
// only (precedents: migrations/embed.go and static internal/web), with
// no external CDNs or fonts (dashboard spec §3.2.2: an offline
// environment; the §6.3 CSP would have blocked them anyway).
// Rendering is fully client-side: the page is static and identical for
// all tabs, data arrives via /api/status polling (T-129) — the server
// does no templating and knows nothing about the state when serving
// the HTML.
//
// Content-Type comes from an explicit map rather than http.FileServer
// sniffing: mime.TypeByExtension depends on the system
// /etc/mime.types (Debian's, for example, serves .js as
// text/javascript) — the route contract must be deterministic; as a
// bonus there is no directory listing and no walking the filesystem
// tree: a name outside the map is an honest 404.
package dashboard

import (
	"embed"
	"net/http"
	"strings"
)

//go:embed assets
var assetsFS embed.FS

// assetTypes — files served under /assets/*: name → Content-Type.
var assetTypes = map[string]string{
	"app.js":    "application/javascript; charset=utf-8",
	"style.css": "text/css; charset=utf-8",
}

// handleIndex — GET /: the dashboard page from embed (dashboard spec §6.1).
// Cache-Control: no-cache — artifact names carry no hashes, so a move to a
// new version must be picked up immediately (the API next to it is
// no-store anyway).
func (s *Server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	s.serveAsset(w, "page.html", "text/html; charset=utf-8")
}

// handleAssets — GET /assets/*: front-end statics from embed (dashboard
// spec §6.1); a name outside assetTypes (including a directory and "..")
// is a 404.
func (s *Server) handleAssets(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/assets/")
	contentType, ok := assetTypes[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.serveAsset(w, name, contentType)
}

// serveAsset — a file from the assets/ directory with an explicit
// Content-Type and no-cache. A missing file in embed is a build error,
// not a request error: 500 and Error in the log (web.handleRobots
// style).
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

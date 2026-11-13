// The CORS allowlist of the /api/* surface (L-04, TZ-ld-landing §7.2):
// the static landings of the separate ld-landing repo live on their own
// domains (sites.laravel.city, sites.laracity.ru) while the API stays on
// its own (api9.laravel.city) — a browser fetch across origins needs the
// server's blessing, and both endpoints of the landing client are
// cross-origin: the form POST /api/check-domain (a JSON body sends a
// preflight OPTIONS first) and the stats GET /api/v1/landing/stats.
//
// The policy is an exact allowlist (web.cors.allowed_origins): the
// browser's Origin is compared byte-for-byte, and the winner is echoed
// into Access-Control-Allow-Origin — never "*", so the API blessing is
// the explicit list of the operator's own domains, not everyone's. A
// foreign or absent Origin changes nothing in the response: no CORS
// headers, the request passes to the handler as is (the browser blocks
// the script's read itself; CORS is the browser's policy — the server's
// control of the form is the rate limit). An empty allowlist — CORS is
// off entirely: no headers and no OPTIONS special-casing, the exact
// behavior before L-04 (an OPTIONS without a route still gets the mux's
// honest 405).
package web

import (
	"net/http"
	"strings"
)

// The fixed preflight answer (the fetch spec's exact values, no
// configuration surface: the two API routes are GET and POST, the only
// non-simple request header of the landing client is Content-Type).
const (
	corsAllowMethods = "GET, POST, OPTIONS"
	corsAllowHeaders = "Content-Type"
	corsMaxAge       = "600"
)

// apiCORS wraps the router with the CORS policy of the /api/* surface
// (L-04). The path gate is a prefix check, not a route list: everything
// under /api/ — both landing endpoints, their 405/404 answers, and any
// future API route — answers with the same policy, while /healthz and
// the 404s of the removed landing routes (L-08) are untouched. The
// preflight short-circuit lives
// here, before the mux: the mux has no OPTIONS routes (registering them
// would change the routing table for the CORS-off mode too).
func (s *Server) apiCORS(h http.Handler) http.Handler {
	if len(s.corsOrigins) == 0 {
		return h // CORS off (L-04): the exact handler stack of before
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			h.ServeHTTP(w, r)
			return
		}
		// The answer depends on Origin (the echo vs no headers): a cache
		// between the browser and the service must key on it — the stats
		// endpoint is publicly cacheable (max-age=60), and without Vary
		// a shared cache would serve one origin's Allow-Origin to a
		// request of another. Add, not Set: the wrapped handler may have
		// its own Vary (the union of the fields is the correct cache
		// key).
		w.Header().Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		if !s.corsOrigins[origin] {
			// A foreign origin, or none (same-origin fetch, curl): no
			// CORS headers; the request proceeds — the rate limit and
			// the live check budget are the server's controls, CORS is
			// not one.
			h.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		if r.Method == http.MethodOptions {
			// A preflight of an allowed origin: 204 with the method/
			// header list and the caching of the answer (the form sends
			// one per visitor session — without Max-Age every check
			// would double into OPTIONS+POST). The handler is not
			// called: a preflight is not a request to the endpoint, and
			// the rate limit is not spent on the browser's handshake.
			w.Header().Set("Access-Control-Allow-Methods", corsAllowMethods)
			w.Header().Set("Access-Control-Allow-Headers", corsAllowHeaders)
			w.Header().Set("Access-Control-Max-Age", corsMaxAge)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

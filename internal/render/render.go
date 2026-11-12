// Package render — Chromium control for page rendering (spec 5.6, §9).
//
// The package model:
//
//   - Browser — one Chromium per worker (spec 5.6: "one browser"). Two
//     modes: an external connection over CDP (Options.RemoteURL —
//     render.remote_url, the chromium container of the dev stack) or an
//     own headless-shell process (the production render image,
//     deploy/docker/Dockerfile.render). In both modes — one browser
//     connection (WebSocket) and one service about:blank tab.
//   - Session — an isolated Chromium context (CDP
//     Target.createBrowserContext, the chromedp.WithNewBrowserContext
//     option): an incognito profile with its own cookies, cache and
//     storages plus one tab. The session lives for exactly one task:
//     after a page the pool closes it (Target.disposeBrowserContext),
//     the next task gets a new profile — no state survives between
//     pages (spec 5.6, §9). Recreating the context instead of selective
//     clearing: dispose discards all the profile storages atomically
//     and without enumerating origins (otherwise cookies/Storage would
//     have to be cleared per each encountered origin, and the cache
//     would not be covered anyway), while the cost is one
//     CreateBrowserContext + CreateTarget — against a page timeout in
//     seconds this is imperceptible.
//   - Pool — up to Options.Contexts (the spec recommendation: 2–4)
//     tasks in parallel, each in its own isolated context.
//
// Errors are classified (Class): a dead context — retry on a new one,
// an unavailable browser — upward, a page error — a regular stage
// result (re-delivery is the queue level, T-066).
//
// The T-062…T-067 development on top of the current API: resource
// blocking (T-062) and the network quiet wait (T-063) are built in as
// stages into Session.Run (before navigation and before result capture
// respectively); the final DOM, cookies and response headers (T-064)
// are RenderOutput fields (extract.go); Manager (T-065) owns the
// "browser + pool" generations — a restart every N pages (the
// Browser.Pages counter) and by the RSS threshold; the queue cycle is
// T-066 (the q:render publication) and T-067 (the consumer —
// internal/renderworker, Manager.Run).
package render

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/httpfetch"
)

// RenderTask — one render task: open a page and capture the result.
//
// The URL comes from the HTTP stage result (a q:render message, T-066);
// the scheme is checked before navigation (validateTaskURL): render
// opens only http/https, other addresses (file:, data:, ftp:) are
// rejected with the task class — the browser would open a local file
// too (R-09). The formal input check is the worker level; the rest is
// rejected by the browser itself (chromedp.ErrPageLoad).
type RenderTask struct {
	URL string
}

// RenderOutput — the task result: the final URL and title (T-061), the
// final DOM, all cookies of the final page and the headers of the main
// navigation's response (T-064; extract.go — the rationale for the
// capture methods).
type RenderOutput struct {
	// FinalURL — document.location after the page loads: includes the
	// JS redirects the HTTP stage does not see (T-048 tracks only HTTP
	// transitions and meta refresh).
	FinalURL string
	// Title — the document.title content (diagnostics and live checks).
	Title string
	// DOM — the final DOM of the document
	// (document.documentElement.outerHTML, spec 5.6): the full tree
	// after the JS rendering; the HTML format — the replacement of the
	// raw HTML in the store (T-067) and the input of the HTML/meta
	// rules (T-068).
	DOM string
	// Cookies — all cookies of the final page, including httpOnly and
	// the ones set by JS (spec 5.6, §11.4 item 3): CDP reads the
	// isolated context's storage, document.cookie does not see them.
	// The format — the unified contract subdomain_results.cookies
	// (T-058). For pages without a cookie storage — nil.
	Cookies []ExtractCookie
	// ResponseHeaders — the headers of the main navigation's response
	// (spec 5.8 — the header rules): http.Header, as at the HTTP stage
	// (T-058). The tasks are only http/https (validateTaskURL, R-09):
	// the navigation has a real network response; nil (no response
	// received) is handled defensively in Session.Run.
	ResponseHeaders http.Header
}

// Options — the parameters of Browser and Pool. Assembled from the
// configuration by NewOptions; the derived timeouts (DialTimeout,
// SessionStartTimeout) are internal modes, not parameterized in
// spec 5.10.
type Options struct {
	// Engine — the render engine (the engines render spec §4.2,
	// FR-1/3/4): "" or config.EngineChromium — the current mode
	// (external CDP when RemoteURL is set, an own process when empty —
	// here and below "chromium" covers both); config.EngineMoli and
	// config.EngineLightpanda — external alternative CDP servers:
	// RemoteURL is mandatory for them (the ws/wss scheme),
	// MemoryThreshold — 0 (the combination is checked by validate), the
	// allocator address mode is by engine (remotealloc.go, wsURLMode).
	// Only the validation and the allocator see the engine: the package
	// core (Session/Pool/Manager/extract/block/wait) does not branch on
	// it (FR-4).
	Engine string
	// RemoteURL — the external Chromium address over CDP
	// (ws://host:9222): the dev stack, the chromium compose service.
	// Empty — the worker starts its own headless-shell process (the
	// production render image).
	RemoteURL string
	// Contexts — the maximum number of parallel isolated contexts
	// (spec 5.6: "2–4 in parallel by default").
	Contexts int
	// Timeout — the overall timeout of one page (spec 5.6: 25 s by
	// default).
	Timeout time.Duration
	// RestartEveryPages — the browser restart threshold by page count
	// (spec 5.6): Browser keeps the Pages counter, the generation swap
	// is Manager (T-065).
	RestartEveryPages int
	// MemoryThreshold — the browser restart threshold by memory, bytes:
	// the total RSS of the Chromium process tree ≥ the threshold →
	// restart (spec 5.6). The Chromium budget ≈ 2 GB (spec 3.2, cgroup
	// MemoryMax), the threshold must be below the container limit: the
	// restart is a prophylactic before the cgroup-OOM fires. 0 — the
	// control is off; in the remote mode there is no local Chromium
	// process (the RSS is unknown) and the control does not work — the
	// cap there is held by the mem_limit of the Chromium container.
	MemoryThreshold int64
	// UserAgent — an identifiable User-Agent of the rendered pages
	// (spec §9): overrides the browser UA in sessions
	// (Emulation.setUserAgentOverride). The render section of spec 5.10
	// has no separate field; the crawler is represented by a single UA
	// — http.user_agent. Empty — the browser UA unchanged.
	UserAgent string
	// ExecPath — the Chromium path in the local mode; empty — the
	// standard chromedp search (the list includes headless-shell by
	// name from PATH).
	ExecPath string
	// DialTimeout — the WebSocket connection timeout to the external
	// Chromium (0 — 10 s, as in chromedp). It bounds the /json/version
	// address refinement too; the session's bootstrap commands (the
	// tab, the domains) are bounded by initBrowserSession with the sum
	// DialTimeout + SessionStartTimeout — the cap applies in both
	// modes, including the cold start of the local process (T-218). The
	// StartBrowser context must live as long as the browser (the worker
	// lifecycle) — an external cancellation kills the initialization
	// too; the cap is a racing select, not a deadline on the context.
	DialTimeout time.Duration
	// SessionStartTimeout — the timeout of creating an isolated context
	// with a tab (0 — 15 s).
	SessionStartTimeout time.Duration
	// MaxDOMBytes — the cap of the final DOM in bytes (R-02, worker
	// memory hygiene: render.contexts captures in parallel at
	// MemoryMax=128M of the systemd profile): the DOM above the limit
	// is truncated in the browser itself and gets a truncation marker
	// (extract.go). 0 — no limit. The source in the build is
	// content.max_store_bytes: the store does not accept content above
	// the limit (ErrTooLarge), keeping it in memory is pointless.
	MaxDOMBytes int64
	// BlockStylesheet — whether to block CSS in the render context
	// (T-213): when true, the list of blocked types (block.go, T-062:
	// Image/Media/Font) is extended with Stylesheet. The default false
	// — the risk of rare sites whose JS reads computed styles/media
	// queries: enabling in production — only after an A/B (the share of
	// detection discrepancies <1%, the rate no worse). The source is
	// render.block_stylesheet.
	BlockStylesheet bool
	// BlockTrackerDomains — whether to block requests to web
	// analytics/telemetry domains in the render context (T-214):
	// Fetch.enable receives urlPattern patterns over the domains of the
	// blockTrackerDomains list (block.go), the verdict on each paused
	// request is an exact host check (the site's own scripts and paths
	// with a similar substring are untouched). The default true —
	// trackers do not participate in detection, the traffic and CPU
	// savings are direct; the residual risk and the rationale for the
	// default are at the BlockTrackerDomains field of internal/config.
	// The source is render.block_tracker_domains.
	BlockTrackerDomains bool
	// BlockInternalAddrs — the render SSRF guard (T-219): the same
	// http.block_internal_addrs flag as at the HTTP stage. When true, a
	// task with an internal-range IP-literal host
	// (httpfetch.AddrIsInternal, including IPv4-in-IPv6 —
	// [::ffff:10.0.0.1]) is rejected with the task class before
	// navigation — Chromium does not open internal addresses: final_url
	// comes from the DB, and a pipeline anomaly or a DB compromise must
	// not turn into a bypass of the guard. The task's hostnames are not
	// resolved separately: their addresses are filtered by the HTTP
	// stage under the same httpfetch funnel; a second resolver in the
	// task validation would widen the guard; the arbitrary names of
	// navigations and subresources inside the page are resolved and
	// checked by the Chromium request interception (T-230, block.go —
	// DNS-rebinding). false — the previous behavior: dev/testbed render
	// internal benches.
	BlockInternalAddrs bool
	// LookupHost — the hostname resolve seam for the SSRF guard of the
	// render interception (T-230): when BlockInternalAddrs is on, the
	// Fetch interception resolves arbitrary names (literal and
	// numeric-like forms are caught by renderHostInternal without a
	// resolve) and rejects requests whose host resolves into
	// httpfetch.AddrIsInternal — a time-based DNS-rebinding (the name
	// is public at the HTTP stage, internal by the render time) is
	// closed inside Chromium itself. The residual vector — an attacker
	// controlling the zone distinguishes the interception's resolve
	// from its own Chromium resolve (two separate DNS exchanges) and
	// answers them with different addresses; the full closure is the
	// egress policy of the Chromium container (docs/render.md). nil —
	// the system resolver (net.DefaultResolver): headless-shell
	// resolves with the system one too, the interception sees the same
	// view of the addresses as the browser. The resolves are cached per
	// session with a TTL and bounded by a cap per exchange (block.go,
	// dnsCache). Builds from the configuration do not set the field
	// (the default works), tests replace it.
	LookupHost func(ctx context.Context, host string) ([]netip.Addr, error)
}

// altEngine — an external alternative render engine (the engines render
// spec §4.2 item 1, FR-4): moli or lightpanda. The only derivative over
// Engine next to Options — read by the validation (the combination
// "engine × RemoteURL × MemoryThreshold" and the RemoteURL scheme) and
// by the remote allocator wiring; deeper into the package the engine
// does not pass.
func (o Options) altEngine() bool {
	return o.Engine == config.EngineMoli || o.Engine == config.EngineLightpanda
}

// NewOptions assembles Options from the service configuration
// (spec 5.10): the render section plus the HTTP stage User-Agent
// (spec §9 — a single identifiable UA of the crawler), the DOM cap from
// the content section (R-02: the final DOM is no larger than what the
// content store accepts) and the SSRF guard flag
// http.block_internal_addrs (T-219 — shared with the HTTP stage).
func NewOptions(cfg config.Config) Options {
	return Options{
		Engine:              cfg.Render.Engine,
		RemoteURL:           cfg.Render.RemoteURL,
		Contexts:            cfg.Render.Contexts,
		Timeout:             time.Duration(cfg.Render.Timeout),
		RestartEveryPages:   cfg.Render.RestartEveryPages,
		MemoryThreshold:     cfg.Render.RestartMemoryBytes.B(),
		UserAgent:           cfg.HTTP.UserAgent,
		MaxDOMBytes:         cfg.Content.MaxStoreBytes.B(),
		BlockStylesheet:     cfg.Render.BlockStylesheet,
		BlockTrackerDomains: cfg.Render.BlockTrackerDomains,
		BlockInternalAddrs:  cfg.HTTP.BlockInternalAddrs,
	}
}

// withDefaults fills the derived timeouts. Contexts is not defaulted
// deliberately: 0 — a configuration error, not "whatever".
func (o Options) withDefaults() Options {
	c := o
	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.SessionStartTimeout == 0 {
		c.SessionStartTimeout = 15 * time.Second
	}
	return c
}

// validate checks the values before passing them to chromedp: invalid
// parameters are rejected here, not by panics deep in the library. The
// ranges repeat the render section of internal/config (spec 5.10).
func (o Options) validate() error {
	var problems []string
	// The render engine (the engines render spec §4.2, FR-3): the
	// enumeration and the combination "engine × remote_url ×
	// memory_threshold" repeat the validation of the render section of
	// internal/config (T-233) — Options is assembled not only from the
	// configuration, the boundary check here holds the allocator
	// contract: moli|lightpanda — external CDP servers, remote_url is
	// mandatory (an own browser process — chromium only, FR-2), the RSS
	// of a foreign process is unavailable — memory_threshold must be 0.
	// Chromium allows both connection modes; the memory threshold with
	// an external chromium does not work but is not forbidden — as
	// today.
	switch o.Engine {
	case "", config.EngineChromium, config.EngineMoli, config.EngineLightpanda: // "" — as chromium (a config without Default())
	default:
		problems = append(problems, fmt.Sprintf("engine: expected %s|%s|%s, got %q",
			config.EngineChromium, config.EngineMoli, config.EngineLightpanda, o.Engine))
	}
	if o.altEngine() {
		if o.RemoteURL == "" {
			problems = append(problems, fmt.Sprintf(
				"engine: %s requires remote_url (an external CDP server ws://host:9222; a browser process of our own — only %s)",
				o.Engine, config.EngineChromium))
		} else if u, err := url.Parse(o.RemoteURL); err != nil || u.Host == "" ||
			(u.Scheme != "ws" && u.Scheme != "wss") {
			// The alternative engines' allocator dials the configuration
			// address (lightpanda) or its authority (moli) straight as a
			// WebSocket: http(s) has no /json/version refinement path
			// with a ready ws address, and the chromedp websocket client
			// accepts only ws/wss — a rejection at assembly time, not an
			// obscure handshake.
			problems = append(problems, fmt.Sprintf(
				"remote_url: for the %s engine ws://host:9222 is expected, got %q", o.Engine, o.RemoteURL))
		}
		if o.MemoryThreshold != 0 {
			problems = append(problems, fmt.Sprintf(
				"memory_threshold: for the %s engine 0 is expected (the RSS of a foreign process is unavailable), got %d",
				o.Engine, o.MemoryThreshold))
		}
	}
	if o.RemoteURL != "" {
		u, err := url.Parse(o.RemoteURL)
		if err != nil || u.Host == "" ||
			(u.Scheme != "ws" && u.Scheme != "wss" && u.Scheme != "http" && u.Scheme != "https") {
			problems = append(problems,
				fmt.Sprintf("remote_url: ws://host:9222 is expected, got %q", o.RemoteURL))
		}
	}
	if o.Contexts < 1 || o.Contexts > 16 {
		problems = append(problems,
			fmt.Sprintf("contexts: 1..16 expected (the spec 5.6 recommendation: 2–4), got %d", o.Contexts))
	}
	if o.Timeout <= 0 {
		problems = append(problems, fmt.Sprintf("timeout: a positive duration is expected, got %s", o.Timeout))
	}
	if o.RestartEveryPages < 1 {
		problems = append(problems,
			fmt.Sprintf("restart_every_pages: >= 1 expected, got %d", o.RestartEveryPages))
	}
	// The memory threshold range repeats render.restart_memory_bytes of
	// internal/config: 0 — the control is off; below 256 MiB — below
	// the idle RSS of headless-shell with contexts (a restart crash
	// loop); above 16 GiB — beyond a sane budget of a single worker
	// (spec 3.2). Equivalent mutants (T-M08): `< 0` is a guard, a
	// negative threshold is caught by the range half below as well
	// (!= 0 and < 256<<20), with the same message; `< -1` differs from
	// `< 0` only at the value -1 — and there by the same path. The
	// string granularity also suppresses the killable cut of the whole
	// right disjunct (the trade-off of §4
	// docs/mutation-testing.md) — the range half is pinned by
	// TestOptionsValidate (the boundaries
	// 255<<20/256<<20/16<<30/17<<30 and the exact error text). The
	// incrementer (`< 1`) is not suppressed: the "0 — the control is
	// off" boundary is held by the same test.
	// mutator-disable-next-line expression/remove, numbers/decrementer
	if o.MemoryThreshold < 0 ||
		(o.MemoryThreshold != 0 && (o.MemoryThreshold < 256<<20 || o.MemoryThreshold > 16<<30)) {
		problems = append(problems, fmt.Sprintf(
			"memory_threshold: 0 (disabled) or %d..%d bytes expected, got %d",
			256<<20, 16<<30, o.MemoryThreshold))
	}
	if o.MaxDOMBytes < 0 {
		problems = append(problems,
			fmt.Sprintf("max_dom_bytes: 0 (no limit) or a positive number expected, got %d", o.MaxDOMBytes))
	}
	if len(problems) > 0 {
		return errors.New("render: " + strings.Join(problems, "; "))
	}
	return nil
}

// validateTaskURL — the scheme check of the render task: only
// http/https are opened. The URL comes from subdomain_results.final_url
// — in the regular flow it is an address from HTTP redirects, but with
// a pipeline anomaly or a DB compromise it may carry file:///data: —
// the browser would open a local file and the DOM capture would carry
// its contents into the content store (R-09). The failure is the task
// class: the page is not rendered, re-delivery is the queue level
// (T-066).
func validateTaskURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return &Error{Class: ClassTask, URL: raw,
			Err: fmt.Errorf("render opens only http/https, got %q", raw)}
	}
	return nil
}

// validateTaskURLInternal — the render task address check with the SSRF
// guard enabled (http.block_internal_addrs, T-219): an internal-range
// host (renderHostInternal — IP literals, including IPv4-in-IPv6 and
// the WHATWG numeric-like forms, loopback names) is not opened, the
// task class — re-delivery at the queue level (T-066), as with the
// scheme check. A hostname passes without a resolve: its addresses are
// already filtered by the HTTP stage under the same httpfetch funnel,
// and a resolve in the task validation would mean a second resolver in
// the guard and an address race with the DNS stage. The same check plus
// a name resolve (T-230, dnsCache) lives in the Chromium request
// interception (blockInternal): navigations over HTTP/JS redirects and
// subresources — including arbitrary names resolving into internal
// addresses already after the HTTP stage (DNS-rebinding) — are blocked
// in the browser itself.
func validateTaskURLInternal(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil // the scheme/host are already rejected by validateTaskURL
	}
	if renderHostInternal(u.Hostname()) {
		return &Error{Class: ClassTask, URL: raw,
			Err: fmt.Errorf("rendering internal addresses is forbidden (SSRF, http.block_internal_addrs): %q", raw)}
	}
	return nil
}

// renderHostInternal — whether the host is internal for the render SSRF
// guard (T-219): Chromium loopback names (localhost and subdomains —
// the browser resolves them to the loopback itself), IP literals of
// internal ranges (httpfetch.AddrIsInternal — the unified guard
// definition, including IPv4-in-IPv6) and "numeric-like" hosts that
// Chromium canonicalizes into IPv4 by the WHATWG algorithm ("127.1",
// "2130706433", "0x7f.1" — all 127.0.0.1; netip does not parse such
// forms, but the browser would open them). Other names pass without a
// resolve (their addresses are filtered at the task stage by the HTTP
// stage if the name came through the pipeline); the names themselves
// in the page's navigations and subresources are resolved and checked
// by the request interception (T-230, block.go — dnsCache), closing
// the time-based DNS-rebinding (an address change between the HTTP
// stage and the render) in Chromium itself; the residual vector — the
// attacker distinguishing the two resolves (the interception's and the
// browser's) — is held by the egress policy of the container
// (docs/render.md).
func renderHostInternal(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return httpfetch.AddrIsInternal(ip)
	}
	if ip, ok := whatwgParseIPv4(host); ok {
		return httpfetch.AddrIsInternal(ip)
	}
	return false
}

// whatwgParseIPv4 — the canonicalization of a numeric-like host by the
// IPv4 parser algorithm of WHATWG URL (the one Chromium follows): a
// host of 1..4 dotted parts, each a decimal/octal (leading 0)/
// hexadecimal (0x) number, folds into an IPv4; a host whose last part
// is not a number is a name, not an IPv4 ("0.com" passes as a name).
// An exact copy of the WHATWG thresholds: non-last parts ≤ 255, the
// last < 256^(5−n). ok=false — the host is not numeric-like (a name or
// junk).
func whatwgParseIPv4(host string) (netip.Addr, bool) {
	parts := strings.Split(host, ".")
	if len(parts) == 0 || len(parts) > 4 {
		return netip.Addr{}, false
	}
	if _, ok := whatwgIPv4Number(parts[len(parts)-1]); !ok {
		return netip.Addr{}, false // does not end with a number — a name
	}
	nums := make([]uint64, len(parts))
	for i, p := range parts {
		v, ok := whatwgIPv4Number(p)
		if !ok {
			return netip.Addr{}, false
		}
		nums[i] = v
	}
	for _, v := range nums[:len(nums)-1] {
		if v > 255 {
			return netip.Addr{}, false
		}
	}
	if nums[len(nums)-1] >= uint64(1)<<(8*(5-len(nums))) {
		return netip.Addr{}, false
	}
	var v uint64
	for i, n := range nums[:len(nums)-1] {
		v += n << (8 * (3 - i)) // the parts except the last are the high bytes
	}
	v += nums[len(nums)-1]
	return netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}), true
}

// whatwgIPv4Number — the number of one part of a WHATWG IPv4 host:
// 0x/0X — hexadecimal (an empty mantissa — 0), a leading 0 — octal,
// otherwise decimal; an empty string and foreign characters are not a
// number.
func whatwgIPv4Number(s string) (uint64, bool) {
	switch {
	case len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X'):
		m := s[2:]
		if m == "" {
			return 0, true
		}
		v, err := strconv.ParseUint(m, 16, 64)
		return v, err == nil
	case len(s) > 1 && s[0] == '0':
		v, err := strconv.ParseUint(s[1:], 8, 64)
		return v, err == nil
	default:
		if s == "" {
			return 0, false
		}
		v, err := strconv.ParseUint(s, 10, 64)
		return v, err == nil
	}
}

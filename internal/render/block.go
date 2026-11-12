package render

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"

	"crawler/internal/httpfetch"
)

// Blocking the loading of images, fonts and media in Chromium — spec 5.6
// ("resource saving"), task T-062; optionally CSS too (T-213), and since
// T-214 — requests to web analytics domains as well.
//
// Mechanics: the interception of the tab's network requests by the CDP Fetch
// domain (Network.setRequestInterception was removed from the protocol; in chromedp
// v0.21 the interception is the fetch.RequestPaused events). The order of enabling in
// Session.Run: subscribing to the events → Fetch.enable → Navigate. The subscription
// must come before enabling: a requestPaused arriving before the subscription
// would be left unanswered and would hold the request suspended until the page
// timeout (the cdp.Events subscription buffers events from the moment of the call —
// the "subscribe, trigger, range" rule from the chromedp.Events doc comment).
//
// Fetch.enable receives patterns by the blocked resource types and — with
// the domain blocking of trackers enabled (T-214) — by the analytics
// domains (urlPattern without resourceType, it catches any type):
// only what the listener is going to reject is suspended —
// Fetch.failRequest (BlockedByClient → net::ERR_BLOCKED_BY_CLIENT) at
// the Request stage: the request does not leave the browser, neither traffic nor
// decoding is spent. The other requests (Document, Script, XHR/Fetch,
// WebSocket, …, and without the T-213 option below — Stylesheet too) the browser
// does not suspend at all: they pass through the network stack by themselves, and a
// failure or delay of the listener cannot hang the page (fail-open — blocking
// saves resources but does not ensure render correctness).
// Scripts and XHR must load: SPA shells finish rendering after
// the load — the render exists for the sake of JS (spec 5.6).
//
// The deliberate exception — the urlPattern substring of the domain patterns
// (T-214) is wider than the exact host check of blockTrackerURL: a request to a foreign
// host with a tracker domain in the path (/js/google-analytics.com.js)
// is suspended and released by the listener's continueRequest.
// The pause window is one listener cycle, and its death is guarded by the fail-open
// disable (blockPump): the "non-blocked does not hang" invariant is held not
// by the pattern but by the pair "a wide pattern → an exact verdict".
//
// Stylesheet with the render.block_stylesheet option enabled (T-213)
// joins the blocked ones by the same mechanism (blockTypes); the default
// is false — the computed styles risk, the rationale is at the option.
//
// The SSRF contour (T-219, http.block_internal_addrs) — the exception from
// fail-open: the "all" pattern suspends every request of the session
// (including Document, Script, XHR/Fetch, WebSocket — navigations via
// HTTP/JS redirects and subresources go through Chromium's network stack past
// the httpfetch funnel), and a request with an internal host is rejected
// (blockInternal, renderHostInternal). The price is a CDP cycle per
// page request, so the pattern exists only with the flag enabled
// (the Internet profile); the default false does not set it. The contour checks
// IP literals and loopback names; since T-230 arbitrary names are resolved
// in the interception itself (the dnsCache below): a name public at the HTTP stage and
// internal by the render moment (an address change between the stages —
// DNS rebinding in time) no longer passes. The residual vector —
// an attacker owning the zone distinguishes the resolves themselves (the interception's
// and Chromium's — two separate DNS exchanges) and answers them with different
// addresses; the complete closure remains with the egress policy of the Chromium
// container (docs/render.md).

// blockDisableTimeout — the limit of Fetch.disable when stopping the interception:
// a regular CDP call takes milliseconds, there is no reason to wait longer.
const blockDisableTimeout = 2 * time.Second

// dnsCacheTTL — the lifetime of the SSRF contour's resolve cache (T-230):
// repeated requests of the same host (page subresources) do not multiply
// the DNS exchanges but also do not pin the verdict longer than the window — the TTL of 30s is
// an order of magnitude larger than a typical exchange with the local unbound and an order of
// magnitude smaller than a domain recheck episode.
const dnsCacheTTL = 30 * time.Second

// dnsLookupTimeout — the cap of ONE contour resolve (T-230, per review):
// the exchange runs in the interception pump's goroutine (while it lasts, the sus-
// pended requests of the page wait for the reply), so a hung resolver
// must not eat the page's budget without a boundary — an exchange longer than the cap
// is cut off, the verdict is fail-open (see hostInternal). 3s — above the cold
// unbound recursion for a foreign domain (a typical exchange is milliseconds),
// below the page timeout (render.timeout 15–25s).
const dnsLookupTimeout = 3 * time.Second

// blockBlockedTypes — the base list of blocked resource types
// (spec 5.6: images, fonts, media — T-062; 91% of the weight of a typical
// page). For the full list of network.ResourceType see the
// TestBlockResourceType test: everything not here passes.
//
// Favicon is classed with Image: an icon is an image, and the ones requested
// by the page (link rel=icon) are classified by Chromium as Image — they
// are blocked together with the other pictures. The /favicon.ico request,
// initiated by the browser itself outside the render, arrives in CDP as the Other type;
// in headless-shell without a UI it is not executed, and singling it out with a URL
// heuristic (risking touching foreign requests) for the sake of one small file is not
// worth it.
var blockBlockedTypes = []network.ResourceType{
	network.ResourceTypeImage,
	network.ResourceTypeMedia,
	network.ResourceTypeFont,
}

// blockTypes — the session's blocked resource types: the base list of
// blockBlockedTypes plus Stylesheet with the option
// render.block_stylesheet enabled (T-213). CSS — the remaining 5–10% of the weight
// bytes of the page and a noticeable part of the style/layout CPU (the research of
// Oct 31: blocking CSS+images is another third off the load time), but it is cut off from
// the base list by the risk: on rare sites JS reads computed styles and
// media queries — on unapplied styles their logic (and the final DOM)
// could differ, so Stylesheet is blocked by an option with the default
// false, and enabling it in production — only after an A/B (the divergence share
// of detection <1%, the pace no worse). The list with the option is a copy: the base
// list is shared by all sessions and must not be mutated.
func blockTypes(blockStylesheet bool) []network.ResourceType {
	if !blockStylesheet {
		return blockBlockedTypes
	}
	return append(slices.Clone(blockBlockedTypes), network.ResourceTypeStylesheet)
}

// blockTrackerDomains — the domains of specialized web analytics and
// telemetry for blocking by URL (T-214), separately from blocking by
// types: analytics arrives as Script/XHR/Image/Ping requests — it cannot be cut off by
// type (the site's SPA scripts must load), and the direct losses from
// it are 5–15 requests and 100–500 KB per page (the source measurements,
// the research of Oct 31) plus the CPU of the tracker timers finishing the "quiet"
// of the network (T-063).
//
// The selection principle: only domains whose sole purpose is
// analytics/telemetry/ad bids. NOT a common CDN and not
// the site's infrastructure — useful code loads from it too: yastatic.net,
// googleapis.com, jsdelivr and other common statics; www.facebook.com —
// on par with the pixel of the "share" button and the redirects (only the
// SDK domain connect.facebook.net is in the list); segment.io/segment.com —
// a customer-data platform, for a site this is infrastructure, cdn.segment.com
// serves not only analytics. The list is maintainable: the regional
// mc.yandex.* and legacy hosts (mxpnl.com) are added as
// needed; self-hosted Matomo/Sentry on the site's own domain are not caught by the
// list on principle — the site's own domain must not be blocked.
var blockTrackerDomains = []string{
	"google-analytics.com",  // Google Analytics: gtag/js, /g/collect
	"googletagmanager.com",  // Google Tag Manager: the gtm.js container
	"doubleclick.net",       // Google ads/remarketing: fls/stat
	"googleadservices.com",  // Google Ads conversions: pageadconversion
	"googlesyndication.com", // AdSense/DFP: pagead2, bids
	"connect.facebook.net",  // Facebook SDK + Pixel
	"hotjar.com",            // session recordings: static/in/vars.hotjar.com
	"mc.yandex.ru",          // Yandex Metrica: tag.js, /watch
	"mc.yandex.com",         // Yandex Metrica (international)
	"matomo.cloud",          // Matomo SaaS (self-hosted — the site's domain)
	"clarity.ms",            // Microsoft Clarity
	"mixpanel.com",          // Mixpanel
	"fullstory.com",         // FullStory
	"amplitude.com",         // Amplitude
	"sentry.io",             // Sentry agent: *.ingest.sentry.io
}

// blockRules — the blocking rules of one render session: the blocked
// types (blockTypes — the single point of the T-062/T-213 options), whether the
// domain blocking of trackers is enabled (T-214), the SSRF contour of internal
// addresses (T-219) and the resolve cache of that contour's names (T-230).
// The single computation point — blockRulesOf: startBlock assembles the rules
// from Options once, then one immutable set travels to
// all the decision points — the Fetch.enable patterns (blockEnable) and the
// blockRespond verdicts see the same configuration, a list divergence
// is excluded in principle (the single-point style of blockTypes at T-213).
type blockRules struct {
	types    []network.ResourceType
	trackers bool
	internal bool
	// dns — the resolve cache of the SSRF contour's names (T-230); created per
	// session only when the contour is enabled. A nil (the contour disabled /
	// rules assembled without it) — the resolve is not called at all.
	dns *dnsCache
}

// blockRulesOf — the session rules from the browser options.
func blockRulesOf(opts Options) blockRules {
	r := blockRules{
		types:    blockTypes(opts.BlockStylesheet),
		trackers: opts.BlockTrackerDomains,
		internal: opts.BlockInternalAddrs,
	}
	if opts.BlockInternalAddrs {
		r.dns = newDNSCache(opts.LookupHost)
	}
	return r
}

// blockTrackerURL — the exact URL check against blockTrackerDomains:
// the host equals a list domain or is its subdomain. More precise than
// the urlPattern substring of Fetch.enable (see blockPatterns): a site path
// like /js/google-analytics.com.js contains the substring but the host is not
// a tracker — blocking it is impermissible; the same with userinfo
// (https://google-analytics.com@site.example — the host is site.example) and
// a foreign suffix (google-analytics.com.evil.example). A URL not parsed by
// url.Parse is not considered a tracker (fail-open: blocking is not
// obliged to catch everything but has no right to cut foreign things).
func blockTrackerURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, d := range blockTrackerDomains {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

// blockTracker — the suspended request goes to a tracker (T-214):
// the domain blocking is enabled and the URL's host is a list domain. An event without
// request info (a protocol anomaly; in practice Request
// is always present) is not considered a tracker.
func (r blockRules) blockTracker(ev fetch.EventRequestPaused) bool {
	return r.trackers && ev.Request != nil && blockTrackerURL(ev.Request.URL)
}

// blockResourceType — whether the resource type is blocked. Unknown types
// (they will appear in future protocol versions) pass: the conservative
// behavior does not break the page load.
func blockResourceType(types []network.ResourceType, t network.ResourceType) bool {
	for _, b := range types {
		if t == b {
			return true
		}
	}
	return false
}

// blockPatterns — the Fetch.enable patterns: one per blocked type
// (the Request stage — the refusal before the request is sent) and, with the
// domain blocking of trackers enabled (T-214), one per list domain —
// without resourceType: analytics travels in any types (Script, XHR, Ping,
// Image), the pattern must catch them all. urlPattern is a glob with a full
// match (Chromium: base::MatchPattern; '*' — any
// sequence, '?' — exactly one symbol), so the domain
// is framed with stars: "*domain*" equals a URL substring; a pattern without
// stars would match only an address equal to the domain in full. The substring
// is wider than the host check of blockTrackerURL — on purpose: the
// /js/google-analytics.com.js path is suspended too, its fate is decided by the exact check in
// blockRespond (the guard against false positives sits in the listener, not
// in the pattern).
//
// With the SSRF contour enabled (T-219) the "all" pattern is added
// (urlPattern "*", without a type): every request of the session is suspended,
// including Document (navigations via HTTP/JS redirects) and Script/XHR/
// WebSocket (subresources), — internal hosts are rejected by blockInternal,
// the rest are released by continueRequest. One extra CDP cycle per
// request — the price of closing the contour in Chromium itself; with the
// flag off there is no pattern at all.
func blockPatterns(types []network.ResourceType, trackers, internal bool) []*fetch.RequestPattern {
	patterns := make([]*fetch.RequestPattern, 0, len(types))
	for _, t := range types {
		patterns = append(patterns, &fetch.RequestPattern{
			ResourceType: t,
			RequestStage: fetch.RequestStageRequest,
		})
	}
	if trackers {
		for _, d := range blockTrackerDomains {
			patterns = append(patterns, &fetch.RequestPattern{
				URLPattern:   "*" + d + "*",
				RequestStage: fetch.RequestStageRequest,
			})
		}
	}
	if internal {
		patterns = append(patterns, &fetch.RequestPattern{
			URLPattern:   "*",
			RequestStage: fetch.RequestStageRequest,
		})
	}
	return patterns
}

// blockEnable enables the interception on the tab's CDP session.
func blockEnable(ctx context.Context, sess cdp.Session, rules blockRules) error {
	_, err := cdp.Call(ctx, sess, fetch.Enable, fetch.EnableParams{
		Patterns: blockPatterns(rules.types, rules.trackers, rules.internal),
	})
	return err
}

// blockDisable disables the interception best-effort: the task's context by the
// moment of stopping may already be canceled (the page timeout), so the call goes
// without its cancellation and with a short timeout; an error (a dead tab etc.) is
// not important — the session is one-off.
func blockDisable(ctx context.Context, sess cdp.Session) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blockDisableTimeout)
	defer cancel()
	_, _ = cdp.Call(dctx, sess, fetch.Disable, cdp.Empty{})
}

// blockInternal — the suspended request goes to an internal address
// (the SSRF contour of T-219/T-230, http.block_internal_addrs): the filter is
// enabled and the URL's host is internal. The literal forms (an IP literal of any kind,
// including IPv4-in-IPv6 and the WHATWG number-like ones) and loopback names are caught by
// the synchronous check of renderHostInternal — the T-219 path; an arbitrary
// name is resolved with the per-session cache (dnsCache, T-230): DNS rebinding
// (the name public at the HTTP stage, internal — by the render moment)
// is rejected in Chromium itself. The URL in the event is canonicalized by the browser,
// but the input here is the same single check as for the render tasks:
// host form divergences are excluded. An event without request info
// (a protocol anomaly) is not considered internal — as with trackers.
func (r blockRules) blockInternal(ctx context.Context, ev fetch.EventRequestPaused) bool {
	return r.internal && ev.Request != nil && requestURLInternal(ctx, ev.Request.URL, r.dns)
}

// requestURLInternal — the request URL's host is internal; an unparsed URL —
// not (fail-open, as in blockTrackerURL: blocking has no right
// to cut foreign things, the task contour is held by validateTaskURLInternal).
// Literal and number-like forms — renderHostInternal without a resolve;
// other names — a resolve through the cache (T-230): an external literal is not
// resolved either (the address is already known).
func requestURLInternal(ctx context.Context, rawURL string, dns *dnsCache) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if renderHostInternal(host) {
		return true
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false // an external IP literal — the address is known, no resolve needed
	}
	if _, ok := whatwgParseIPv4(host); ok {
		return false // an external WHATWG number-like form — the same
	}
	return dns.hostInternal(ctx, host)
}

// dnsCache — the resolve cache of the interception's SSRF contour names (T-230):
// positive (an internal address), negative (external) and errored
// verdicts live dnsCacheTTL — the page's subresources hit one host
// by dozens of requests; without the cache each would exchange with DNS separately.
// The cache belongs to the rules of one session (blockRulesOf in startBlock):
// the calls come only from this session's interception pump goroutine
// (blockPump → blockRespond — the events are serialized), no mutex is needed;
// the session lives one task (≤ render.timeout), a cross-page
// migration of verdicts is excluded by the lifetime itself (we do not bound the
// entries map deliberately: its size is bounded by the number of hosts of one
// page — hundreds of entries by tens of bytes; the long-lived cache
// httpfetch.CachingResolver with MaxEntries is another scale, the session
// dies with the task).
//
// The fail semantics of the contour are split (T-230): a resolve error — fail-open
// (the request passes: the NXDOMAIN of ad and broken subresources is the norm
// of a page, a resolve error ≠ an internal address; fail-closed here would cut
// legitimate pages by batches), a found internal address —
// fail-closed (failRequest): this is exactly what the contour exists for.
// The exchange is bounded by its own timeout cap (dnsLookupTimeout):
// a hung resolver is cut off rather than eating the page's budget in the
// pump's goroutine; a cut by deadline/cancellation is NOT cached (a cut is not a DNS
// answer, the next request of the same host will exchange again), deterministic
// errors (NXDOMAIN) are cached as "not internal" on the common TTL.
type dnsCache struct {
	// lookup — the resolve seam (Options.LookupHost; nil — the system
	// net.DefaultResolver: headless-shell resolves with the system one too,
	// the interception sees the same view of addresses as the browser).
	lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	// now — the time seam for the TTL tests; nil — time.Now.
	now func() time.Time
	// timeout — the cap of one exchange (dnsLookupTimeout; the tests' seam).
	timeout time.Duration
	// entries — the verdicts by hosts; the key is the canonical host (lowercase
	// without the trailing dot, normalized by requestURLInternal).
	entries map[string]dnsCacheEntry
}

// dnsCacheEntry — a cached resolve verdict.
type dnsCacheEntry struct {
	internal bool
	at       time.Time
}

// newDNSCache — a per-session cache with the resolve seam.
func newDNSCache(lookup func(context.Context, string) ([]netip.Addr, error)) *dnsCache {
	if lookup == nil {
		lookup = lookupHostSystem
	}
	return &dnsCache{lookup: lookup, now: time.Now, timeout: dnsLookupTimeout}
}

// lookupHostSystem — the system resolver (the Options.LookupHost seam default):
// the same /etc/resolv.conf Chromium itself resolves with — the
// contour's verdict does not diverge from the addresses the loading actually goes over.
func lookupHostSystem(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

// hostInternal — the name resolves into an internal address (at least one of
// the reply: the list is handed by the attacker, fail-closed on a found one). A resolve
// error and an empty reply — not internal (fail-open, see the doc comment
// of dnsCache); deterministic outcomes (a reply, an empty reply, NXDOMAIN)
// are cached for dnsCacheTTL, a cut by deadline/cancellation — not.
// A nil cache (rules without the contour — the callers' guard) — not internal.
func (c *dnsCache) hostInternal(ctx context.Context, host string) bool {
	if c == nil {
		return false
	}
	if c.now == nil {
		c.now = time.Now
	}
	if e, ok := c.entries[host]; ok && c.now().Sub(e.at) < dnsCacheTTL {
		return e.internal
	}
	internal := false
	definitive := true
	if addrs, err := c.exchange(ctx, host); err == nil {
		for _, a := range addrs {
			if httpfetch.AddrIsInternal(a) {
				internal = true
				break
			}
		}
	} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// A cut of the exchange (the timeout cap or the end of the page) — not a
		// DNS answer: the verdict is fail-open but not cached — a repeated request of the
		// same host will exchange anew.
		definitive = false
	} // other errors (an NXDOMAIN, a SERVFAIL reply) — a deterministic
	// "not internal" with caching on the common grounds.
	// Logging (§4 docs/mutation-testing.md): a Debug record does not affect
	// the request's fate (the verdict is already computed), the branch and
	// statement-removal mutants change only the debug log text (the
	// sample is the blockRespond log below); there are two annotations — branch/if is killed on
	// the if line, statement/remove — on the call line, both mutants
	// give an indistinguishable diff (a body noop).
	// mutator-disable-next-line branch/if
	if internal {
		// mutator-disable-next-line statement/remove
		slog.Debug("рендер: имя резолвится во внутренний адрес — запрос отклоняется (SSRF)",
			"host", host)
	}
	if definitive {
		if c.entries == nil {
			c.entries = make(map[string]dnsCacheEntry)
		}
		c.entries[host] = dnsCacheEntry{internal: internal, at: c.now()}
	}
	return internal
}

// exchange — one exchange with the timeout cap: a hung resolver does not
// eat the page's budget in the pump's goroutine (the cap is below
// render.timeout, the stop call waits for the exchange no longer than the cap).
func (c *dnsCache) exchange(ctx context.Context, host string) ([]netip.Addr, error) {
	if c.timeout <= 0 {
		return c.lookup(ctx, host)
	}
	rctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.lookup(rctx, host)
}

// blockRespond answers a suspended request: an internal address
// (T-219 — literals, T-230 — names by resolve), a blocked type or
// (T-214) a tracker domain — failRequest, anything else — continueRequest
// (the guard: the patterns must not suspend foreign requests, but
// the urlPattern substring of the domain patterns and the SSRF contour's "all"
// pattern are wider than the exact checks — a classification divergence must not hang
// a request). The order of conditions — the cheap networkless verdicts first (type,
// tracker), the contour with the resolve last: a request that would be
// rejected by type/domain anyway must not spend a DNS exchange (per the
// T-230 review). Reply errors are not fatal: the interception is a saving, the page
// finishes without it too (fail-open).
func blockRespond(ctx context.Context, sess cdp.Session, ev fetch.EventRequestPaused, rules blockRules) {
	var err error
	if blockResourceType(rules.types, ev.ResourceType) || rules.blockTracker(ev) || rules.blockInternal(ctx, ev) {
		_, err = cdp.Call(ctx, sess, fetch.FailRequest, fetch.FailRequestParams{
			RequestID:   ev.RequestID,
			ErrorReason: network.ErrorReasonBlockedByClient,
		})
	} else {
		_, err = cdp.Call(ctx, sess, fetch.ContinueRequest,
			fetch.ContinueRequestParams{RequestID: ev.RequestID})
	}
	// Logging (§4 docs/mutation-testing.md): a Debug record about a failed
	// reply does not affect the request's fate (the fail-open of blockRespond itself);
	// the branch and statement-removal mutants change only the debug
	// log text. There are two annotations: the branch/if mutator enters at the IfStmt
	// node (killed
	// on the if line), statement/remove — via the body's BlockStmt and is killed on
	// the statement line; both give an indistinguishable diff (a body noop), one
	// kill is not enough — the second generator survives.
	// mutator-disable-next-line branch/if
	if err != nil {
		// mutator-disable-next-line statement/remove
		slog.Debug("рендер: ответ на перехваченный запрос не прошёл",
			"type", ev.ResourceType.String(), "err", err.Error())
	}
}

// blockListen — the production wiring of the pump: subscribing to the
// requestPaused of the tab's session (Target implements cdp.Session).
func blockListen(ctx context.Context, sess cdp.Session, rules blockRules) (stop func()) {
	return blockPump(ctx, func(lctx context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return cdp.Events(lctx, sess, fetch.RequestPaused)
	}, sess, rules)
}

// blockPump pumps the interception events and answers each; it finishes
// on ctx cancellation (the end of the task/page, stop) or on the closing of the
// events channel (the tab died). stop cancels the listener and WAITS for its
// exit — the goroutine does not survive the call (no leaks, checked with -race).
//
// eventsOf — the iterator factory in the spirit of cdp.Events; it is called synchronously
// with the listener's internal context BEFORE the goroutine starts: the
// subscription is pinned earlier than Fetch.enable (startBlock), and stop, canceling this same
// context, deterministically finishes both the traversal and the iterator. The tests feed
// their own factory instead of the real subscription.
//
// rules — the session rules (blockRules, T-213/T-214): the fate of each
// request is decided by blockRespond.
func blockPump(ctx context.Context,
	eventsOf func(context.Context) iter.Seq2[fetch.EventRequestPaused, error],
	sess cdp.Session, rules blockRules) (stop func()) {
	lctx, cancel := context.WithCancel(ctx)
	events := eventsOf(lctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev, err := range events {
			if err != nil {
				// A context cancellation or an unparsed event — the
				// traversal is over (an error yield in the iterator is terminal).
				return
			}
			blockRespond(lctx, sess, ev, rules)
		}
		// The events channel closed with the context alive — the subscription ended
		// unexpectedly: disable the interception so that the blockable requests do
		// not hang until the page timeout (fail-open).
		if lctx.Err() == nil {
			blockDisable(lctx, sess)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// startBlock enables resource blocking in the session's pipeline (T-062;
// with the Options.BlockStylesheet option — CSS too, T-213; with
// Options.BlockTrackerDomains — the analytics domains too, T-214); it is called
// from Session.Run BEFORE navigating to the page. An enabling error —
// a pipeline error (tab/browser), Run classifies it with the regular wrap.
// The returned stop kills the listener and disables the interception; the
// session is one-off, but a direct Session user does not inherit the blocking.
func (s *Session) startBlock(ctx context.Context) (stop func(), err error) {
	c := chromedp.FromContext(s.ctx)
	if c == nil || c.Target == nil {
		return nil, errors.New("контекст вкладки не инициализирован")
	}
	tgt := c.Target
	rules := blockRulesOf(s.b.opts)

	// The subscription strictly before Fetch.enable (see the file's doc comment).
	stopListen := blockListen(ctx, tgt, rules)
	if err := blockEnable(ctx, tgt, rules); err != nil {
		stopListen()
		return nil, fmt.Errorf("включение перехвата Fetch: %w", err)
	}
	return func() {
		stopListen()
		blockDisable(ctx, tgt)
	}, nil
}

package render

import (
	"context"
	"errors"
	"iter"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// fakeCDPSession — a CDP session for the blocking tests: it remembers the commands
// (Fetch.enable/failRequest/continueRequest/disable) and can crash them
// by the method name. Subscribe is not used — the events are fed to the pump
// by an own iterator (see pumpEvents).
type fakeCDPSession struct {
	mu    sync.Mutex
	calls []fakeCDPCall
	fail  map[string]error
	// gate — optional per-method gates: Call waits for the channel to be
	// closed (see the comment in Call).
	gate map[string]chan struct{}
}

// fakeCDPCall — an executed command: the method and the params as-is, the deadline
// of the call's context (zero — a call without a deadline).
type fakeCDPCall struct {
	Method   string
	Params   any
	Deadline time.Time
}

func (f *fakeCDPSession) Call(ctx context.Context, method string, params, _ any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	f.mu.Lock()
	call := fakeCDPCall{Method: method, Params: params}
	if dl, ok := ctx.Deadline(); ok {
		call.Deadline = dl
	}
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	if err, ok := f.fail[method]; ok {
		return err
	}
	// gate — a controlled block: Call with a method from gate does not return
	// until the gate is closed — unconditionally, against the context cancellation: the
	// test needs the listener's delay exactly inside blockRespond (the context
	// cancellation of the listener is the regular exit path, and that is what we check).
	if g, ok := f.gate[method]; ok {
		<-g
	}
	return nil
}

func (f *fakeCDPSession) Subscribe(string) (<-chan jsonv2.Value, func()) {
	panic("the tests feed events with their own iterator, not via Subscribe")
}

// calls — a copy of the command journal.
func (f *fakeCDPSession) callsOf() []fakeCDPCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCDPCall(nil), f.calls...)
}

// countOf — the number of commands with the given method.
func (f *fakeCDPSession) countOf(method string) int {
	n := 0
	for _, c := range f.callsOf() {
		if c.Method == method {
			n++
		}
	}
	return n
}

// waitFor — until cond becomes true or the term expires (the determinism
// of the async pump).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the condition was not reached in 2 s")
}

// TestBlockResourceType — the list of the blocked and passing types.
// Exactly Image/Media/Font are blocked (spec 5.6); with the block_stylesheet
// option (T-213) — Stylesheet too; the full list of the protocol's types is enumerated
// explicitly: a new type in cdproto passes by default (the conservative behavior,
// it does not break the page load).
func TestBlockResourceType(t *testing.T) {
	all := []network.ResourceType{
		network.ResourceTypeDocument,
		network.ResourceTypeStylesheet,
		network.ResourceTypeImage,
		network.ResourceTypeMedia,
		network.ResourceTypeFont,
		network.ResourceTypeScript,
		network.ResourceTypeTextTrack,
		network.ResourceTypeXHR,
		network.ResourceTypeFetch,
		network.ResourceTypePrefetch,
		network.ResourceTypeEventSource,
		network.ResourceTypeWebSocket,
		network.ResourceTypeManifest,
		network.ResourceTypeSignedExchange,
		network.ResourceTypePing,
		network.ResourceTypeCSPViolationReport,
		network.ResourceTypePreflight,
		network.ResourceTypeFedCM,
		network.ResourceTypeOther,
	}
	baseWant := map[network.ResourceType]bool{
		network.ResourceTypeImage: true,
		network.ResourceTypeMedia: true,
		network.ResourceTypeFont:  true,
	}
	cssWant := maps.Clone(baseWant)
	cssWant[network.ResourceTypeStylesheet] = true
	for name, tc := range map[string]struct {
		types []network.ResourceType
		want  map[network.ResourceType]bool
	}{
		"without the option (T-062)": {blockTypes(false), baseWant},
		"block_stylesheet (T-213)":   {blockTypes(true), cssWant},
	} {
		t.Run(name, func(t *testing.T) {
			for _, rt := range all {
				if got := blockResourceType(tc.types, rt); got != tc.want[rt] {
					t.Errorf("blockResourceType(%s) = %v, want %v", rt, got, tc.want[rt])
				}
			}
			// An unknown type — it passes.
			if blockResourceType(tc.types, network.ResourceType("SomethingNew")) {
				t.Error("an unknown type must pass")
			}
			// An empty type (Chrome does not send it) — it passes.
			if blockResourceType(tc.types, network.ResourceType("")) {
				t.Error("an empty type must pass")
			}
		})
	}
}

// TestBlockTypes — the session's type list (T-213): without the option — the
// T-062 base set (Image/Media/Font, Stylesheet passes), with the option — plus
// Stylesheet; the copy does not touch the shared base list (otherwise
// the off option would pull CSS along with the on one).
func TestBlockTypes(t *testing.T) {
	base := blockTypes(false)
	if !slices.Equal(base, []network.ResourceType{
		network.ResourceTypeImage, network.ResourceTypeMedia, network.ResourceTypeFont,
	}) {
		t.Fatalf("without the option %v are blocked, want Image/Media/Font (T-062)", base)
	}
	css := blockTypes(true)
	if len(css) != len(base)+1 || css[len(css)-1] != network.ResourceTypeStylesheet {
		t.Fatalf("with the option %v are blocked, want the base set + Stylesheet (T-213)", css)
	}
	if !slices.Equal(css[:len(base)], base) {
		t.Errorf("with the option the base part %v changed, want %v", css[:len(base)], base)
	}
	// A copy, not an append into the shared slice: the base list has not grown.
	if len(blockBlockedTypes) != 3 {
		t.Errorf("the base list grew to %d after blockTypes(true) — a shared mutation", len(blockBlockedTypes))
	}
}

// TestBlockPatterns — the Fetch.enable patterns: one per blocked
// type, the Request stage (the refusal before the request is sent), without a URL
// filter.
// The tracker domain patterns (T-214) — a separate TestBlockPatternsTrackers.
func TestBlockPatterns(t *testing.T) {
	for name, types := range map[string][]network.ResourceType{
		"without the option (T-062)": blockTypes(false),
		"block_stylesheet (T-213)":   blockTypes(true),
	} {
		t.Run(name, func(t *testing.T) {
			patterns := blockPatterns(types, false, false)
			if len(patterns) != len(types) {
				t.Fatalf("patterns %d, want %d (one per blocked type)",
					len(patterns), len(types))
			}
			seen := make(map[network.ResourceType]bool)
			for _, p := range patterns {
				if !blockResourceType(types, p.ResourceType) {
					t.Errorf("a pattern with an unblocked type %s", p.ResourceType)
				}
				if seen[p.ResourceType] {
					t.Errorf("a duplicate pattern type %s", p.ResourceType)
				}
				seen[p.ResourceType] = true
				if p.RequestStage != fetch.RequestStageRequest {
					t.Errorf("the pattern stage %s: %s, want Request",
						p.ResourceType, p.RequestStage)
				}
				if p.URLPattern != "" {
					t.Errorf("the pattern URL filter %s: %q, want empty (by type only)",
						p.ResourceType, p.URLPattern)
				}
			}
		})
	}
}

// TestBlockPatternsTrackers — the Fetch.enable domain patterns (T-214):
// with the option on, one per list domain — the "domain in
// stars" urlPattern (the Fetch.enable glob with a full match: without the stars
// the pattern would match only an address equal to the domain), without resourceType
// (analytics travels in any types), the Request stage; with it off —
// no domain patterns at all.
func TestBlockPatternsTrackers(t *testing.T) {
	on := blockPatterns(blockTypes(false), true, false)
	urlPatterns := make(map[string]bool)
	typePatterns := 0
	for _, p := range on {
		if p.URLPattern == "" {
			typePatterns++
			continue
		}
		urlPatterns[p.URLPattern] = true
		if p.ResourceType != "" {
			t.Errorf("the domain pattern %q with the type %s — the analytics is not caught by type",
				p.URLPattern, p.ResourceType)
		}
		if p.RequestStage != fetch.RequestStageRequest {
			t.Errorf("the domain pattern stage %q: %s, want Request",
				p.URLPattern, p.RequestStage)
		}
	}
	if typePatterns != len(blockTypes(false)) {
		t.Errorf("type patterns %d, want %d (domain ones on top of types)",
			typePatterns, len(blockTypes(false)))
	}
	if len(urlPatterns) != len(blockTrackerDomains) {
		t.Errorf("domain patterns %d, want %d (one per list domain)",
			len(urlPatterns), len(blockTrackerDomains))
	}
	for _, d := range blockTrackerDomains {
		if !urlPatterns["*"+d+"*"] {
			t.Errorf("the domain %s without the pattern *%s*", d, d)
		}
	}

	for name, types := range map[string][]network.ResourceType{
		"without the option (T-062)": blockTypes(false),
		"block_stylesheet (T-213)":   blockTypes(true),
	} {
		t.Run("off: "+name, func(t *testing.T) {
			off := blockPatterns(types, false, false)
			if len(off) != len(types) {
				t.Fatalf("patterns %d, want %d (types only)",
					len(off), len(types))
			}
			for _, p := range off {
				if p.URLPattern != "" {
					t.Errorf("with the option off there is a domain pattern %q", p.URLPattern)
				}
			}
		})
	}
}

// TestBlockPatternsInternal — the SSRF contour's "all" pattern (T-219): with
// http.block_internal_addrs enabled Fetch.enable receives exactly one
// extra pattern without a type and URL (it suspends every request
// of the session — Document, Script, XHR, WebSocket), the Request stage; with
// the flag off there is no pattern at all.
func TestBlockPatternsInternal(t *testing.T) {
	on := blockPatterns(blockTypes(false), false, true)
	if len(on) != len(blockTypes(false))+1 {
		t.Fatalf("patterns %d, want %d+1 (types + the catch-all)", len(on), len(blockTypes(false)))
	}
	catchAll := 0
	for _, p := range on {
		if p.URLPattern == "*" {
			catchAll++
			if p.ResourceType != "" {
				t.Errorf("the catch-all pattern with the type %s — the contour does not catch it by type", p.ResourceType)
			}
			if p.RequestStage != fetch.RequestStageRequest {
				t.Errorf("the catch-all pattern stage: %s, want Request", p.RequestStage)
			}
		}
	}
	if catchAll != 1 {
		t.Errorf("catch-all patterns %d, want 1", catchAll)
	}

	off := blockPatterns(blockTypes(false), false, false)
	for _, p := range off {
		if p.URLPattern == "*" {
			t.Errorf("with the contour off there is a catch-all pattern: %+v", p)
		}
	}
}

// TestBlockRespondInternal — the SSRF contour (T-219): with the flag on,
// a request of any type to an internal host (loopback, RFC1918, cloud
// metadata, IPv4-in-IPv6, a loopback name, a number-like form) —
// failRequest(BlockedByClient); an external host and an event without request —
// continueRequest; with the flag off an internal host passes.
// The resolver is a fake (T-230: name hosts go into the resolve; a real DNS
// in a unit test would make the "public name → continue" case dependent on the
// environment — filtering resolvers answer 0.0.0.0, which is internal).
func TestBlockRespondInternal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		url     string
		fail    bool
		lookups int // resolver exchanges: literals/loopback — 0, a name — 1
	}{
		{"Document→127.0.0.1 fail", "http://127.0.0.1/admin", true, 0},
		{"Document→169.254.169.254 fail", "http://169.254.169.254/latest/meta-data/", true, 0},
		{"Document→[::ffff:169.254.169.254] fail", "http://[::ffff:169.254.169.254]/latest/meta-data/", true, 0},
		{"Script→10.0.0.1 fail", "http://10.0.0.1/app.js", true, 0},
		{"XHR→localhost fail", "http://localhost:6379/stats", true, 0},
		{"WebSocket→127.1 fail (the WHATWG form)", "ws://127.1/socket", true, 0},
		{"Document→site.example continue", "http://site.example/page", false, 1},
		{"Script→203.0.113.7 continue", "http://203.0.113.7/app.js", false, 0},
		{"a path with the 127.0.0.1 substring continue", "http://site.example/js/127.0.0.1.js", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookup := &fakeLookupHost{addrs: map[string][]netip.Addr{
				"site.example": {netip.MustParseAddr("203.0.113.7")},
			}}
			on := blockRulesOf(Options{BlockInternalAddrs: true, LookupHost: lookup.lookup})
			fake := &fakeCDPSession{}
			blockRespond(context.Background(), fake, fetch.EventRequestPaused{
				RequestID: "req-1", ResourceType: network.ResourceTypeDocument,
				Request: &network.Request{URL: tc.url},
			}, on)
			calls := fake.callsOf()
			want := "Fetch.continueRequest"
			if tc.fail {
				want = "Fetch.failRequest"
			}
			if len(calls) != 1 || calls[0].Method != want {
				t.Fatalf("commands %v, want one %s", calls, want)
			}
			if n := len(lookup.callsOf()); n != tc.lookups {
				t.Errorf("resolver exchanges %d, want %d (calls %v)",
					n, tc.lookups, lookup.callsOf())
			}
			if tc.fail {
				params, ok := calls[0].Params.(fetch.FailRequestParams)
				if !ok {
					t.Fatalf("params %T, want fetch.FailRequestParams", calls[0].Params)
				}
				if params.ErrorReason != network.ErrorReasonBlockedByClient {
					t.Errorf("failRequest %+v, want BlockedByClient", params)
				}
			}
		})
	}

	// An event without request — continue (a protocol anomaly; as with trackers).
	fake := &fakeCDPSession{}
	blockRespond(context.Background(), fake, fetch.EventRequestPaused{
		RequestID: "anomaly", ResourceType: network.ResourceTypeScript,
	}, blockRulesOf(Options{BlockInternalAddrs: true}))
	if calls := fake.callsOf(); len(calls) != 1 || calls[0].Method != "Fetch.continueRequest" {
		t.Fatalf("commands %v, want one Fetch.continueRequest (an event without request)", calls)
	}

	// The flag is off — an internal host passes (dev/testbed), no resolve.
	lookup := &fakeLookupHost{}
	fake = &fakeCDPSession{}
	blockRespond(context.Background(), fake, fetch.EventRequestPaused{
		RequestID: "dev-1", ResourceType: network.ResourceTypeDocument,
		Request: &network.Request{URL: "http://127.0.0.1/"},
	}, blockRulesOf(Options{LookupHost: lookup.lookup}))
	if calls := fake.callsOf(); len(calls) != 1 || calls[0].Method != "Fetch.continueRequest" {
		t.Fatalf("commands %v, want one Fetch.continueRequest (the flag is off)", calls)
	}
	if n := len(lookup.callsOf()); n != 0 {
		t.Errorf("resolver exchanges %d, want 0 (the contour is off)", n)
	}
}

// TestBlockTrackerURL — the exact host check against the blockTrackerDomains
// list (T-214): a host domain and its subdomains are trackers;
// a domain substring in the path/query/userinfo and a tracker suffix of a foreign host —
// not; an unparsed URL — not (fail-open).
func TestBlockTrackerURL(t *testing.T) {
	cases := []struct {
		name string
		url  string
		want bool
	}{
		{"a tracker host", "https://www.google-analytics.com/g/collect?cid=1", true},
		{"the tracker apex", "https://google-analytics.com/analytics.js", true},
		{"a tracker subdomain", "https://cdn4.mixpanel.com/lib.min.js", true},
		{"a RU metric", "https://mc.yandex.ru/metrika/tag.js", true},
		{"an international metric", "https://mc.yandex.com/watch/12345678", true},
		{"the sentry ingest subdomain", "https://o123456.ingest.sentry.io/api/1/envelope/", true},
		{"a path with the domain substring", "https://site.example/js/google-analytics.com.js", false},
		{"a query with the domain substring", "https://site.example/search?q=doubleclick.net", false},
		{"userinfo with a domain", "https://google-analytics.com@site.example/collect", false},
		{"a tracker suffix of a foreign host", "https://google-analytics.com.evil.example/ga.js", false},
		{"the domain boundary (.co ≠ .com)", "https://api.mixpanel.co/track", false},
		{"a regular site", "https://laravel.example/js/app.js", false},
		{"an unparseable URL", "://no-scheme", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := blockTrackerURL(tc.url); got != tc.want {
				t.Errorf("blockTrackerURL(%q) = %v, want %v", tc.url, got, tc.want)
			}
		})
	}
}

// TestBlockTrackerDomainsList — the hygiene of the domain list (T-214): each
// element is a lowercase host without a scheme/wildcard characters,
// no duplicates; the list covers both the apex and the subdomains of each domain
// (the invariant of the pair "a wide *domain* pattern → the host check").
func TestBlockTrackerDomainsList(t *testing.T) {
	if n := len(blockTrackerDomains); n < 12 || n > 16 {
		t.Errorf("%d domains in the list — the intended range is 12–16", n)
	}
	seen := make(map[string]bool, len(blockTrackerDomains))
	for _, d := range blockTrackerDomains {
		if d == "" || d != strings.ToLower(d) || !strings.Contains(d, ".") ||
			strings.ContainsAny(d, "/:?*") {
			t.Errorf("the domain %q is not a lower-case host without a scheme and asterisks", d)
		}
		if seen[d] {
			t.Errorf("a duplicate of the domain %q", d)
		}
		seen[d] = true
		if !blockTrackerURL("https://" + d + "/tag.js") {
			t.Errorf("the apex %s is not recognized by the list", d)
		}
		if !blockTrackerURL("https://static." + d + "/tag.js") {
			t.Errorf("the static.%s subdomain is not recognized by the list", d)
		}
	}
}

// TestBlockEnable — enabling the interception with the Fetch.enable command and the
// patterns by the blocked types; a command error is returned to the caller.
func TestBlockEnable(t *testing.T) {
	t.Run("the command and params", func(t *testing.T) {
		fake := &fakeCDPSession{}
		rules := blockRulesOf(Options{BlockStylesheet: true}) // with the option: one more pattern
		if err := blockEnable(context.Background(), fake, rules); err != nil {
			t.Fatalf("blockEnable: %v", err)
		}
		calls := fake.callsOf()
		if len(calls) != 1 || calls[0].Method != "Fetch.enable" {
			t.Fatalf("commands %v, want one Fetch.enable", calls)
		}
		params, ok := calls[0].Params.(fetch.EnableParams)
		if !ok {
			t.Fatalf("params %T, want fetch.EnableParams", calls[0].Params)
		}
		if len(params.Patterns) != len(rules.types) {
			t.Fatalf("patterns %d, want %d", len(params.Patterns), len(rules.types))
		}
		for _, p := range params.Patterns {
			if !blockResourceType(rules.types, p.ResourceType) || p.RequestStage != fetch.RequestStageRequest {
				t.Errorf("the pattern %+v is outside the blocked set", p)
			}
		}
	})

	t.Run("the CDP error is propagated", func(t *testing.T) {
		boom := errors.New("session gone")
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.enable": boom}}
		err := blockEnable(context.Background(), fake, blockRulesOf(Options{}))
		if !errors.Is(err, boom) {
			t.Fatalf("the error %v, want %v", err, boom)
		}
	})
}

// TestBlockRespond — the fate of a suspended request by type:
// the blocked ones — Fetch.failRequest(BlockedByClient) with the same RequestID,
// the rest — Fetch.continueRequest; Stylesheet is blocked only with the
// option (T-213); a command error is not a panic.
func TestBlockRespond(t *testing.T) {
	cases := []struct {
		typ  network.ResourceType
		fail bool
	}{
		{network.ResourceTypeImage, true},
		{network.ResourceTypeMedia, true},
		{network.ResourceTypeFont, true},
		{network.ResourceTypeDocument, false},
		{network.ResourceTypeStylesheet, false},
		{network.ResourceTypeScript, false},
		{network.ResourceTypeXHR, false},
		{network.ResourceTypeFetch, false},
		{network.ResourceTypeWebSocket, false},
		{network.ResourceTypeManifest, false},
		{network.ResourceTypeOther, false},
	}
	for _, tc := range cases {
		name := tc.typ.String() + "→" + map[bool]string{true: "fail", false: "continue"}[tc.fail]
		t.Run(name, func(t *testing.T) {
			fake := &fakeCDPSession{}
			ev := fetch.EventRequestPaused{RequestID: "req-1", ResourceType: tc.typ}
			blockRespond(context.Background(), fake, ev, blockRulesOf(Options{}))

			calls := fake.callsOf()
			if len(calls) != 1 {
				t.Fatalf("commands %d, want one: %v", len(calls), calls)
			}
			want := "Fetch.continueRequest"
			if tc.fail {
				want = "Fetch.failRequest"
			}
			if calls[0].Method != want {
				t.Fatalf("the command %s, want %s", calls[0].Method, want)
			}
			if tc.fail {
				params, ok := calls[0].Params.(fetch.FailRequestParams)
				if !ok {
					t.Fatalf("params %T, want fetch.FailRequestParams", calls[0].Params)
				}
				if params.RequestID != ev.RequestID {
					t.Errorf("RequestID %s, want %s", params.RequestID, ev.RequestID)
				}
				if params.ErrorReason != network.ErrorReasonBlockedByClient {
					t.Errorf("ErrorReason %s, want BlockedByClient", params.ErrorReason)
				}
			} else {
				params, ok := calls[0].Params.(fetch.ContinueRequestParams)
				if !ok {
					t.Fatalf("params %T, want fetch.ContinueRequestParams", calls[0].Params)
				}
				if params.RequestID != ev.RequestID {
					t.Errorf("RequestID %s, want %s", params.RequestID, ev.RequestID)
				}
			}
		})
	}

	// Stylesheet with the block_stylesheet option (T-213) — failRequest, as
	// the base blocked types.
	t.Run("Stylesheet→fail with the option (T-213)", func(t *testing.T) {
		fake := &fakeCDPSession{}
		ev := fetch.EventRequestPaused{RequestID: "css-1", ResourceType: network.ResourceTypeStylesheet}
		blockRespond(context.Background(), fake, ev, blockRulesOf(Options{BlockStylesheet: true}))
		calls := fake.callsOf()
		if len(calls) != 1 || calls[0].Method != "Fetch.failRequest" {
			t.Fatalf("commands %v, want one Fetch.failRequest", calls)
		}
		params, ok := calls[0].Params.(fetch.FailRequestParams)
		if !ok {
			t.Fatalf("params %T, want fetch.FailRequestParams", calls[0].Params)
		}
		if params.RequestID != ev.RequestID {
			t.Errorf("RequestID %s, want %s", params.RequestID, ev.RequestID)
		}
		if params.ErrorReason != network.ErrorReasonBlockedByClient {
			t.Errorf("ErrorReason %s, want BlockedByClient", params.ErrorReason)
		}
	})

	// Tracker domains (T-214): with the option on, a request of ANY type to
	// a tracker host — failRequest (analytics travels as Script/XHR — it is not caught by
	// type), a path suspended by the substring with a foreign host —
	// continueRequest (the precision sits in the listener, not in the pattern);
	// with the option off, a tracker request of a non-blocked type passes.
	t.Run("tracker domains (T-214)", func(t *testing.T) {
		on := blockRulesOf(Options{BlockTrackerDomains: true})
		for _, tc := range []struct {
			name string
			ev   fetch.EventRequestPaused
			fail bool
		}{
			{"Script→fail", fetch.EventRequestPaused{
				RequestID: "ga-js", ResourceType: network.ResourceTypeScript,
				Request: &network.Request{URL: "https://www.google-analytics.com/analytics.js"},
			}, true},
			{"XHR→fail", fetch.EventRequestPaused{
				RequestID: "mc-xhr", ResourceType: network.ResourceTypeXHR,
				Request: &network.Request{URL: "https://mc.yandex.ru/watch/12345678"},
			}, true},
			{"Document→fail", fetch.EventRequestPaused{
				RequestID: "dc-doc", ResourceType: network.ResourceTypeDocument,
				Request: &network.Request{URL: "https://stats.g.doubleclick.net/r/collect"},
			}, true},
			{"Ping→fail", fetch.EventRequestPaused{
				RequestID: "cl-ping", ResourceType: network.ResourceTypePing,
				Request: &network.Request{URL: "https://www.clarity.ms/tag/xyz"},
			}, true},
			{"a tracker Image→fail (the type and the domain)", fetch.EventRequestPaused{
				RequestID: "ga-img", ResourceType: network.ResourceTypeImage,
				Request: &network.Request{URL: "https://www.google-analytics.com/collect.gif"},
			}, true},
			{"a Script with the domain in the path→continue", fetch.EventRequestPaused{
				RequestID: "own-js", ResourceType: network.ResourceTypeScript,
				Request: &network.Request{URL: "https://site.example/js/google-analytics.com.js"},
			}, false},
			{"an event without request→continue", fetch.EventRequestPaused{
				RequestID: "anomaly", ResourceType: network.ResourceTypeScript,
			}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fake := &fakeCDPSession{}
				blockRespond(context.Background(), fake, tc.ev, on)
				calls := fake.callsOf()
				want := "Fetch.continueRequest"
				if tc.fail {
					want = "Fetch.failRequest"
				}
				if len(calls) != 1 || calls[0].Method != want {
					t.Fatalf("commands %v, want one %s", calls, want)
				}
				if tc.fail {
					params, ok := calls[0].Params.(fetch.FailRequestParams)
					if !ok {
						t.Fatalf("params %T, want fetch.FailRequestParams", calls[0].Params)
					}
					if params.RequestID != tc.ev.RequestID {
						t.Errorf("RequestID %s, want %s", params.RequestID, tc.ev.RequestID)
					}
					if params.ErrorReason != network.ErrorReasonBlockedByClient {
						t.Errorf("ErrorReason %s, want BlockedByClient", params.ErrorReason)
					}
				}
			})
		}

		// The option is off — a tracker script passes: the domain patterns not
		// only do not get into Fetch.enable but also do not affect the verdicts.
		fake := &fakeCDPSession{}
		blockRespond(context.Background(), fake, fetch.EventRequestPaused{
			RequestID: "ga-off", ResourceType: network.ResourceTypeScript,
			Request: &network.Request{URL: "https://www.google-analytics.com/analytics.js"},
		}, blockRulesOf(Options{}))
		if calls := fake.callsOf(); len(calls) != 1 || calls[0].Method != "Fetch.continueRequest" {
			t.Fatalf("commands %v, want one Fetch.continueRequest (the option is off)", calls)
		}
	})

	t.Run("the command error is swallowed", func(t *testing.T) {
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.failRequest": errors.New("dead")}}
		ev := fetch.EventRequestPaused{RequestID: "r", ResourceType: network.ResourceTypeImage}
		blockRespond(context.Background(), fake, ev, blockRulesOf(Options{})) // does not panic
	})
}

// TestBlockDisable — disabling the interception: one Fetch.disable command under
// the own short blockDisableTimeout (a regular CDP call is
// milliseconds, there is no reason to wait longer), a best-effort error is not returned.
func TestBlockDisable(t *testing.T) {
	t.Run("the command and the timeout", func(t *testing.T) {
		fake := &fakeCDPSession{}
		blockDisable(context.Background(), fake)
		calls := fake.callsOf()
		if len(calls) != 1 || calls[0].Method != "Fetch.disable" {
			t.Fatalf("commands %v, want one Fetch.disable", calls)
		}
		rem := time.Until(calls[0].Deadline)
		if rem <= 1500*time.Millisecond || rem > 2500*time.Millisecond {
			t.Errorf("the Fetch.disable timeout: %s left, want ≈2 s (the 1.5–2.5 s bounds)", rem)
		}
	})

	t.Run("the error is swallowed", func(t *testing.T) {
		fake := &fakeCDPSession{fail: map[string]error{"Fetch.disable": errors.New("the tab died")}}
		blockDisable(context.Background(), fake) // does not panic
		if fake.countOf("Fetch.disable") != 1 {
			t.Error("the Fetch.disable command did not run")
		}
	})
}

// pumpEvents — an events iterator in the spirit of cdp.Events: the values from the
// channel, an error on ctx cancellation (terminal), the end on the channel
// closing. It gives the test control over the event flow without a real CDP
// subscription; the ctx is passed by blockPump itself (the listener's internal
// context).
func pumpEvents(events chan fetch.EventRequestPaused,
) func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
	return func(ctx context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return func(yield func(fetch.EventRequestPaused, error) bool) {
			for {
				select {
				case <-ctx.Done():
					yield(fetch.EventRequestPaused{}, ctx.Err())
					return
				case ev, ok := <-events:
					if !ok {
						return
					}
					if !yield(ev, nil) {
						return
					}
				}
			}
		}
	}
}

// TestBlockPumpLifecycle — the events pump: it answers the events of both
// categories, finishes by stop without a goroutine leak, does not touch events
// after stopping, and on a sudden channel closing disables the
// interception (fail-open).
func TestBlockPumpLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	// The events of both kinds get replies.
	events <- fetch.EventRequestPaused{RequestID: "img", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return fake.countOf("Fetch.failRequest") == 1 })
	events <- fetch.EventRequestPaused{RequestID: "js", ResourceType: network.ResourceTypeScript}
	waitFor(t, func() bool { return fake.countOf("Fetch.continueRequest") == 1 })

	// stop returns control — the goroutine has finished.
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not wait out the listener's exit")
	}

	// After the stop the events are not processed.
	before := len(fake.callsOf())
	select {
	case events <- fetch.EventRequestPaused{RequestID: "late", ResourceType: network.ResourceTypeMedia}:
		t.Fatal("an event was accepted after stop")
	default:
	}
	if got := len(fake.callsOf()); got != before {
		t.Errorf("commands after stop: %d (was %d)", got, before)
	}
	if fake.countOf("Fetch.disable") != 0 {
		t.Error("a regular stop does not do the fail-open disable — startBlock does")
	}
}

// TestBlockPumpCtxDone — the parent context cancellation (the end of the page)
// finishes the pump without a fail-open disable (the task is over, the pool
// closes the session).
func TestBlockPumpCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	events <- fetch.EventRequestPaused{RequestID: "i", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return fake.countOf("Fetch.failRequest") == 1 })

	cancel()
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("the pump did not exit on the context cancellation")
	}
	if fake.countOf("Fetch.disable") != 0 {
		t.Error("the context cancellation must not call disable (the pool closes the session)")
	}
	// The iterator's error does not fall through to blockRespond: the zero event
	// of an error gets no reply (there would be no extra continueRequest either).
	if got := fake.countOf("Fetch.continueRequest"); got != 0 {
		t.Errorf("after the cancellation zero events were answered: continueRequest %d, want 0", got)
	}
}

// TestBlockPumpEventsClosed — the events channel closing with the context
// alive (the tab died): the pump exits and disables the interception so that
// the blockable requests do not hang until the page timeout.
func TestBlockPumpEventsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	fake := &fakeCDPSession{}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	close(events)
	waitFor(t, func() bool { return fake.countOf("Fetch.disable") == 1 })
	stop() // after the pump's exit it returns at once
}

// TestBlockPumpEventError — an event flow error with the context alive
// (an unparsable event — an error yield is terminal, the context is not canceled):
// the pump exits silently — without a reply to the zero event and without a
// fail-open disable (the interception lifecycle is held by startBlock, not by
// garbage in the flow). The determinism — the synchrony of yield: the pump's
// iteration body finishes BEFORE the return from yield, so the yieldDone probe
// guarantees that a mutant's reply would have been written into the fake while the
// context was still alive.
func TestBlockPumpEventError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &fakeCDPSession{}
	probe := &errPumpProbe{
		entered:   make(chan struct{}),
		yieldDone: make(chan struct{}),
	}
	stop := blockPump(ctx, probe.events(errors.New("an event is not parsed")), fake, blockRulesOf(Options{}))

	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the pump did not enter the event iterator")
	}
	select {
	case <-probe.yieldDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the error iteration did not complete (the pump body did not run)")
	}
	stop()
	if calls := fake.callsOf(); len(calls) != 0 {
		t.Fatalf("commands %v, want zero: a stream error does not answer the "+
			"event and does not turn off the interception", calls)
	}
}

// errPumpProbe — an iterator in the spirit of cdp.Events: a single terminal error
// with the context alive (like a payload decoding error) with the probes
// of synchrony — entered on entering the iterator, yieldDone on the return from
// yield (the consumer's iteration body has finished by then).
type errPumpProbe struct {
	entered   chan struct{}
	yieldDone chan struct{}
}

func (p *errPumpProbe) events(err error) func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
	return func(context.Context) iter.Seq2[fetch.EventRequestPaused, error] {
		return func(yield func(fetch.EventRequestPaused, error) bool) {
			close(p.entered)
			yield(fetch.EventRequestPaused{}, err)
			close(p.yieldDone)
		}
	}
}

// TestBlockPumpRespondError — a reply error does not stop the pump:
// the following events are processed (no fail-open needed, the page is alive).
func TestBlockPumpRespondError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused, 2)
	fake := &fakeCDPSession{fail: map[string]error{"Fetch.failRequest": errors.New("boom")}}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	events <- fetch.EventRequestPaused{RequestID: "a", ResourceType: network.ResourceTypeImage}
	events <- fetch.EventRequestPaused{RequestID: "b", ResourceType: network.ResourceTypeImage}
	waitFor(t, func() bool { return len(fake.callsOf()) >= 2 }) // both attempts

	stop()
}

// TestBlockPumpStopWaitsForListener — stop waits for the listener's exit
// (the blockPump contract: "the goroutine does not survive the call"): while the
// pump sits in blockRespond behind the gate, stop does not return; after the release —
// it returns quickly.
func TestBlockPumpStopWaitsForListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan fetch.EventRequestPaused)
	gate := make(chan struct{})
	fake := &fakeCDPSession{gate: map[string]chan struct{}{"Fetch.continueRequest": gate}}
	stop := blockPump(ctx, pumpEvents(events), fake, blockRulesOf(Options{}))

	// The listener has entered blockRespond and waits for the gate's release: the
	// command is written to the journal before the wait.
	events <- fetch.EventRequestPaused{RequestID: "gated", ResourceType: network.ResourceTypeScript}
	waitFor(t, func() bool { return len(fake.callsOf()) == 1 })

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("stop returned before the listener exited blockRespond")
	case <-time.After(100 * time.Millisecond):
	}

	close(gate) // release blockRespond — the pump finishes by the cancellation
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not wait out the listener's exit after the gate was released")
	}
}

// TestStartBlock — enabling blocking in the session's pipeline: the refusals of the
// tab's context and of Fetch.enable, the regular stop (it kills the listener and
// disables the interception), the extension of the patterns by Stylesheet with the
// Options.BlockStylesheet option
// (T-213) and by the tracker domains with the Options.BlockTrackerDomains option
// (T-214). The interception events are fed via fakeCDP — the in-memory
// chromedp transport (cdpfake_test.go), no live browser needed.
func TestStartBlock(t *testing.T) {
	// newSession — a session with a browser whose options startBlock reads
	// (BlockStylesheet); the browser as such is not needed — only opts.
	newSession := func(ctx context.Context, opts Options) *Session {
		return &Session{b: &Browser{opts: opts}, ctx: ctx}
	}

	t.Run("the session context is not chromedp", func(t *testing.T) {
		s := newSession(context.Background(), Options{})
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "the tab context is not initialized") {
			t.Fatalf("the error %v, want \"the tab context is not initialized\"", err)
		}
	})

	t.Run("a chromedp context without a tab", func(t *testing.T) {
		tr := newFakeCDP()
		allocCtx, cancelAlloc := chromedp.NewAllocatorContext(
			context.Background(), &fakeCDPAllocator{tr: tr})
		defer cancelAlloc()
		sctx, cancel := chromedp.NewContext(allocCtx) // without Do: Target not created
		defer cancel()
		s := newSession(sctx, Options{})
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "the tab context is not initialized") {
			t.Fatalf("the error %v, want \"the tab context is not initialized\"", err)
		}
	})

	t.Run("a Fetch.enable error", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		tr.setError("Fetch.enable", &cdproto.Error{Code: -32000, Message: "the Fetch domain is broken"})
		s := newSession(ctx, Options{})
		_, err := s.startBlock(context.Background())
		if err == nil || !strings.Contains(err.Error(), "enabling the Fetch interception") {
			t.Fatalf("the error %v, want the \"enabling the Fetch interception\" wrap", err)
		}
		if !strings.Contains(err.Error(), "the Fetch domain is broken") {
			t.Errorf("the original error is not punched through: %v", err)
		}
		// The listener is killed already on the error path: the interception events get
		// no replies (a stopListen-removal mutant would leave the pump
		// alive — an event would get a failRequest/continueRequest).
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"late","resourceType":"Image"}`)
		assertNoNewCDPCalls(t, tr, "Fetch.requestPaused-after-error")
	})

	t.Run("stop kills the listener and turns off the interception", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		if tr.countOf("Fetch.enable") != 1 {
			t.Fatalf("Fetch.enable ran %d times, want 1", tr.countOf("Fetch.enable"))
		}

		// An interception event gets a reply: the listener works.
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"img-1","resourceType":"Image"}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 1 })

		stop()
		waitFor(t, func() bool { return tr.countOf("Fetch.disable") == 1 })

		// After the stop the events are not answered (the listener is killed) and there
		// are no new commands.
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"late","resourceType":"Media"}`)
		assertNoNewCDPCalls(t, tr, "Fetch.requestPaused-after-stop")
	})

	// The BlockStylesheet option (T-213): the Fetch.enable patterns are extended
	// with Stylesheet, and a suspended Stylesheet gets a failRequest.
	t.Run("block_stylesheet extends the interception (T-213)", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{BlockStylesheet: true})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		defer stop()

		enable := tr.callsOf()
		n, params := 0, ""
		for _, c := range enable {
			if c.Method == "Fetch.enable" {
				n++
				params = c.Params
			}
		}
		if n != 1 {
			t.Fatalf("Fetch.enable ran %d times, want 1", n)
		}
		if !strings.Contains(params, `"Stylesheet"`) {
			t.Errorf("the Fetch.enable patterns without Stylesheet: %s", params)
		}
		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"css-1","resourceType":"Stylesheet"}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 1 })
	})

	// The BlockTrackerDomains option (T-214): the Fetch.enable patterns are extended
	// with the domain ones; a suspended Script to a tracker — failRequest, a Script
	// with a tracker domain in the path of a foreign host — continueRequest.
	t.Run("block_tracker_domains extends the interception (T-214)", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{BlockTrackerDomains: true})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		defer stop()

		enable := tr.callsOf()
		n, params := 0, ""
		for _, c := range enable {
			if c.Method == "Fetch.enable" {
				n++
				params = c.Params
			}
		}
		if n != 1 {
			t.Fatalf("Fetch.enable ran %d times, want 1", n)
		}
		if !strings.Contains(params, `"*google-analytics.com*"`) {
			t.Errorf("the Fetch.enable patterns without the tracker domain: %s", params)
		}
		if !strings.Contains(params, `"*mc.yandex.ru*"`) {
			t.Errorf("the Fetch.enable patterns without mc.yandex.ru: %s", params)
		}

		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"ga-1","resourceType":"Script",`+
				`"request":{"url":"https://www.google-analytics.com/analytics.js"}}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 1 })

		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"own-1","resourceType":"Script",`+
				`"request":{"url":"https://site.example/js/google-analytics.com.js"}}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.continueRequest") == 1 })
	})

	// The BlockInternalAddrs option + the name resolve (T-230): the "all"
	// pattern (TestBlockPatternsInternal) suspends every request, and
	// the interception verdict for an arbitrary name is by the resolve: a name that
	// at the render moment resolves into an internal address (DNS rebinding) —
	// failRequest, an external name — continueRequest; a repeated name takes the
	// verdict from the per-session cache without a new DNS exchange.
	t.Run("block_internal_addrs resolves names in the interception (T-230)", func(t *testing.T) {
		lookup := &fakeLookupHost{addrs: map[string][]netip.Addr{
			"rebind.example.net": {netip.MustParseAddr("169.254.169.254")},
			"ok.example.net":     {netip.MustParseAddr("203.0.113.7")},
		}}
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{BlockInternalAddrs: true, LookupHost: lookup.lookup})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		defer stop()

		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"rb-1","resourceType":"Document",`+
				`"request":{"url":"http://rebind.example.net/"}}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 1 })

		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"rb-ok","resourceType":"Script",`+
				`"request":{"url":"http://ok.example.net/app.js"}}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.continueRequest") == 1 })

		tr.fireSession("Fetch.requestPaused",
			`{"requestId":"rb-2","resourceType":"XHR",`+
				`"request":{"url":"http://rebind.example.net/2"}}`)
		waitFor(t, func() bool { return tr.countOf("Fetch.failRequest") == 2 })
		if n := len(lookup.callsOf()); n != 2 {
			t.Errorf("resolver exchanges %d, want 2 (the per-session cache — one per host; calls %v)",
				n, lookup.callsOf())
		}
	})

	// Without the option there are no domain patterns at all — Fetch.enable carries only
	// the type ones (T-062/T-213).
	t.Run("without block_tracker_domains there are no domain patterns (T-214)", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		s := newSession(ctx, Options{})
		stop, err := s.startBlock(context.Background())
		if err != nil {
			t.Fatalf("startBlock: %v", err)
		}
		defer stop()

		for _, c := range tr.callsOf() {
			if c.Method != "Fetch.enable" {
				continue
			}
			if strings.Contains(c.Params, "urlPattern") {
				t.Errorf("the Fetch.enable patterns contain urlPattern: %s", c.Params)
			}
		}
	})
}

// TestDNSCacheResolveTTL — the per-session resolve cache (T-230): repeated
// requests of the same host do not multiply the DNS exchanges, an entry lives
// dnsCacheTTL and on expiry is refreshed by a fresh exchange (the verdict is not
// pinned forever — DNS may change the answer between the exchanges); negative
// (an external address) and errored verdicts are cached the same way; a nil cache —
// not internal (the guard of the rules without the contour).
func TestDNSCacheResolveTTL(t *testing.T) {
	lookup := &fakeLookupHost{addrs: map[string][]netip.Addr{
		"rebind.example.net": {netip.MustParseAddr("169.254.169.254")},
		"ok.example.net":     {netip.MustParseAddr("203.0.113.7")},
	}, errs: map[string]error{
		"nx.example.net": errors.New("no such host"),
	}}
	c := newDNSCache(lookup.lookup)
	now := time.Now()
	c.now = func() time.Time { return now }

	// The first request — an exchange; the "internal" verdict is cached, a repeat
	// within the TTL window — without an exchange.
	if !c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("the first resolution: want internal (169.254.169.254)")
	}
	if !c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("the cached verdict: want internal")
	}
	if n := len(lookup.callsOf()); n != 1 {
		t.Fatalf("exchanges %d, want 1 (a repeat within the TTL window — from the cache)", n)
	}

	// The TTL expiry — a fresh exchange; DNS has changed the answer to a public
	// one (the rebinding contour does not hold the verdict longer than the window).
	lookup.addrs["rebind.example.net"] = []netip.Addr{netip.MustParseAddr("203.0.113.7")}
	now = now.Add(dnsCacheTTL + time.Second)
	if c.hostInternal(context.Background(), "rebind.example.net") {
		t.Fatal("after the TTL expiry: want external by the new answer")
	}
	if n := len(lookup.callsOf()); n != 2 {
		t.Fatalf("exchanges %d, want 2 (an expired entry is refreshed by an exchange)", n)
	}

	// The negative and errored verdicts are cached the same way: one
	// exchange per two calls.
	for _, host := range []string{"ok.example.net", "nx.example.net"} {
		if c.hostInternal(context.Background(), host) {
			t.Fatalf("%s: want not-internal", host)
		}
		if c.hostInternal(context.Background(), host) {
			t.Fatalf("%s: the cached verdict is also not-internal", host)
		}
	}
	if n := len(lookup.callsOf()); n != 4 {
		t.Fatalf("exchanges %d, want 4 (one per host: the negative and the error are cached)", n)
	}

	// A nil cache (the rules without the contour — the guard) — not internal.
	var nilCache *dnsCache
	if nilCache.hostInternal(context.Background(), "any.example.net") {
		t.Error("a nil cache: want not-internal")
	}
}

// TestDNSCacheLookupTimeout — the cap of one resolve (T-230, per the
// P1 review): a hung exchange is cut off by the cache's own timeout (not the
// page's context), the verdict is fail-open (not internal) but is NOT
// cached: a cut is not a DNS answer, a repeated request of the same host
// exchanges again (the exchanges are bound by one cap, no waiting
// accumulation). A deterministic error (NXDOMAIN) is cached, as
// before.
func TestDNSCacheLookupTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, 4)
	c := newDNSCache(func(ctx context.Context, host string) ([]netip.Addr, error) {
		started <- struct{}{}
		select {
		case <-release: // not closed until the end of the test — the exit only via rctx
			return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
		case <-ctx.Done():
			return nil, ctx.Err() // the DeadlineExceeded of the exchange cap
		}
	})
	c.timeout = 20 * time.Millisecond

	for i := 0; i < 2; i++ {
		if c.hostInternal(context.Background(), "blackhole.example.net") {
			t.Fatalf("exchange %d: want fail-open (not internal)", i+1)
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("the exchange did not start")
		}
	}
	if n := len(c.entries); n != 0 {
		t.Errorf("the cache after hangups: %d entries, want 0 (a hangup is not cached)", n)
	}

	// A deterministic resolve error — cached (fail-open).
	errLookup := &fakeLookupHost{errs: map[string]error{
		"nx.example.net": errors.New("no such host"),
	}}
	e := newDNSCache(errLookup.lookup)
	if e.hostInternal(context.Background(), "nx.example.net") {
		t.Error("NXDOMAIN: want not-internal")
	}
	if e.hostInternal(context.Background(), "nx.example.net") {
		t.Error("NXDOMAIN from the cache: want not-internal")
	}
	if n := len(errLookup.callsOf()); n != 1 {
		t.Errorf("exchanges %d, want 1 (the error is cached)", n)
	}
}

// assertNoNewCDPCalls — a negative check: from the moment of the call, new commands
// do not pass through the transport (the window of 100 ms is an order of magnitude of headroom
// against the microseconds of the delivery chain; in the package's style — the negative
// pauses of TestWaitLoopStaleRequestsDrained).
func assertNoNewCDPCalls(t *testing.T, tr *fakeCDP, what string) {
	t.Helper()
	before := len(tr.callsOf())
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) && len(tr.callsOf()) == before {
		time.Sleep(time.Millisecond)
	}
	if got := len(tr.callsOf()); got != before {
		t.Errorf("%s: unexpected new commands went through %+v", what, tr.callsOf()[before:])
	}
}

// fakeLookupHost — a fake of the Options.LookupHost seam for the resolve tests of
// the interception's SSRF contour (T-230): a map of host → addresses and host → error,
// the journal of exchanges (the per-session cache must not multiply the resolves of one
// host).
type fakeLookupHost struct {
	mu    sync.Mutex
	addrs map[string][]netip.Addr
	errs  map[string]error
	calls []string
}

func (f *fakeLookupHost) lookup(_ context.Context, host string) ([]netip.Addr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, host)
	if err, ok := f.errs[host]; ok {
		return nil, err
	}
	return f.addrs[host], nil
}

func (f *fakeLookupHost) callsOf() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// TestBlockRespondInternalResolve — the name resolve in the SSRF contour's
// interception
// (T-230, the task's criterion): with the flag on, an arbitrary name that
// at the render moment resolves into an internal address (the
// DNS rebinding scenario: the name public at the HTTP stage, internal — by the
// render moment) is rejected by the interception with a failRequest; with the
// flag off the same name passes; a name with a public resolve and a resolve error
// pass (fail-open: the NXDOMAIN of subresources is common, a resolve error ≠
// an internal address); IP literals (including the number-like forms) go
// the former T-219 way without a resolve at all.
func TestBlockRespondInternalResolve(t *testing.T) {
	for _, tc := range []struct {
		name     string
		internal bool
		url      string
		fail     bool
		lookups  int // the expected number of resolver exchanges
	}{
		{"on: a name → 169.254.169.254 — fail (rebinding)", true,
			"http://rebind.example.net/latest/meta-data/", true, 1},
		{"on: a name → RFC1918 — fail", true,
			"http://intranet.example.net/", true, 1},
		{"on: a name → a public address — continue", true,
			"http://ok.example.net/app.js", false, 1},
		{"on: a resolution error — continue (fail-open)", true,
			"http://nx.example.net/pixel.gif", false, 1},
		{"on: an internal IP literal — fail without a resolution (T-219)", true,
			"http://10.0.0.1/admin", true, 0},
		{"on: an external IP literal — continue without a resolution", true,
			"http://203.0.113.7/", false, 0},
		{"on: a numeric-looking form — fail without a resolution (WHATWG)", true,
			"http://127.1/x", true, 0},
		{"on: a loopback name — fail without a resolution", true,
			"http://localhost:6379/stats", true, 0},
		{"off: a name → 169.254.169.254 — continue", false,
			"http://rebind.example.net/latest/meta-data/", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookup := &fakeLookupHost{
				addrs: map[string][]netip.Addr{
					// A mixed reply: a found internal address — the
					// "internal" verdict (fail-closed on a found address: the list
					// is handed by the attacker, the browser would pick any).
					"rebind.example.net": {
						netip.MustParseAddr("203.0.113.7"),
						netip.MustParseAddr("169.254.169.254"),
					},
					"intranet.example.net": {netip.MustParseAddr("10.0.0.5")},
					"ok.example.net":       {netip.MustParseAddr("203.0.113.7")},
				},
				errs: map[string]error{
					"nx.example.net": errors.New("no such host"),
				},
			}
			fake := &fakeCDPSession{}
			blockRespond(context.Background(), fake, fetch.EventRequestPaused{
				RequestID: "req-1", ResourceType: network.ResourceTypeDocument,
				Request: &network.Request{URL: tc.url},
			}, blockRulesOf(Options{BlockInternalAddrs: tc.internal, LookupHost: lookup.lookup}))
			calls := fake.callsOf()
			want := "Fetch.continueRequest"
			if tc.fail {
				want = "Fetch.failRequest"
			}
			if len(calls) != 1 || calls[0].Method != want {
				t.Fatalf("commands %v, want one %s", calls, want)
			}
			if n := len(lookup.callsOf()); n != tc.lookups {
				t.Errorf("resolver exchanges %d, want %d (calls %v)",
					n, tc.lookups, lookup.callsOf())
			}
		})
	}
}

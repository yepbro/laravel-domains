// Package httpfetch is the HTTP transport of the site checking stage
// (spec 5.5, items 1–2 and the limits table).
//
// The transport solves three things unavailable to the default net/http:
//
//   - connecting to a pre-resolved IP: the addresses of the primary host
//     come from the DNS stage in the q:http task (spec 5.5 item 2); the
//     request specifies the host (the Host header, SNI) and the
//     connection addresses separately;
//   - per-layer timeouts from the spec 5.5 limits table: connect 5 s,
//     TLS handshake 5 s, waiting for headers 10 s, the total request
//     timeout 20 s — from the http section of the configuration (T-006);
//   - resolving HTTP redirect hosts "via the same resolver with the
//     cache" (spec 5.5 item 2): connecting to a host that has no DNS
//     stage addresses (a redirect to another host) goes through the
//     transport's Resolver; the cache wrapper is CachingResolver;
//   - the limits and protections of T-049 (the spec 5.5 limits table):
//     the body cap with the truncation flag, the anti-tarpit (a min speed
//     after the first byte), connections per IP, global connections and
//     RPS — limits.go, enabled by Options.Limits.
//
// Ethics and the minimum of requests (spec 5.5, §9): the crawler requests
// only the main page "/" — a site check's FetchRequest is built from the
// scheme and host without a path (or with the path "/", it is the same
// request). The package code does not touch /robots.txt and invents no
// paths itself: other paths in the redirect chain (T-048) are set by the
// server's responses — Location and meta refresh, that is, the site's
// initiative, not the crawler's. The User-Agent of every request is an
// honest one with the operator's contacts: the http.user_agent of the
// configuration, the default config.DefaultUserAgent (T-052). "One request
// per (subdomain, variant) with no repeats" is the worker's shared cache
// of final URLs (T-053); the transport does not retry: one Fetch is one
// outbound request (plus the chain hops over the server's Location).
//
// The transport does not handle redirects: one request — one response,
// Location stays a response header; following the chain (max_redirects,
// the cycle detect, meta refresh, punycode normalization of hosts) is
// built by T-048 on top of Fetch or on top of the RoundTripper interface:
// an http.Client with this transport resolves the redirect hops through
// the Resolver automatically (the original ctx does not carry the DNS
// stage addresses to the new host — see dialContext).
package httpfetch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"crawler/internal/config"
)

// The defaults — the spec 5.5 limits table ("all are configurable").
const (
	// DefaultConnectTimeout is establishing a TCP connection (connect 5 s).
	DefaultConnectTimeout = 5 * time.Second
	// DefaultTLSHandshakeTimeout is the TLS handshake (5 s).
	DefaultTLSHandshakeTimeout = 5 * time.Second
	// DefaultHeaderTimeout is waiting for the response headers (10 s);
	// in the transport this is ResponseHeaderTimeout, and the body is not
	// bounded by it.
	DefaultHeaderTimeout = 10 * time.Second
	// DefaultTotalTimeout is the total request timeout (20 s): the request
	// context covers establishing the connection, TLS, sending and reading
	// the body; it ends with closing the result's Body.
	DefaultTotalTimeout = 20 * time.Second
	// DefaultFallbackDelay is the pause before starting the connection
	// attempt to the next address (RFC 8305 connection attempt delay;
	// 300 ms — like net.Dialer's FallbackDelay).
	DefaultFallbackDelay = 300 * time.Millisecond
)

// Options are the transport parameters. Zero and negative values are
// replaced with the defaults of the spec 5.5 limits table (the
// dnsclient.Options repo style: "0 means the default").
type Options struct {
	// ConnectTimeout is the timeout for establishing a TCP connection to a
	// single address.
	ConnectTimeout time.Duration
	// TLSHandshakeTimeout is the TLS handshake timeout.
	TLSHandshakeTimeout time.Duration
	// HeaderTimeout is waiting for the response headers after the request
	// is sent.
	HeaderTimeout time.Duration
	// TotalTimeout is the total Fetch timeout: the connection, TLS,
	// headers and reading the body (in effect until the result's Body is
	// closed).
	TotalTimeout time.Duration
	// FallbackDelay is the pause between connection attempts to the next
	// of several addresses (see dialAddrs).
	FallbackDelay time.Duration
	// MaxBodyBytes is the readable body limit ("max readable body size |
	// 2 MB", spec 5.5). It is applied by the T-049 limiter (Options.Limits:
	// truncation with the BodyLimiter.Truncated flag); without the limiter
	// the value is carried from the configuration to the consumer as is.
	// With Limits enabled, the body cap is one for the transport and the
	// limiter — Limits.MaxBodyBytes (bodyCeiling); this field does not
	// affect decompression.
	MaxBodyBytes int64
	// Limits is the limits and protections of T-049 (the spec 5.5 limits
	// table): the body cap, the anti-tarpit, connections per IP, global
	// connections and RPS. nil — a transport without limits (the isolated
	// layer tests of T-047); NewOptions enables the limits from the http
	// config section.
	Limits *LimitOptions
	// UserAgent is the User-Agent of all Fetch requests: an honest one
	// with the operator's contacts (spec 5.5, §9; the http.user_agent
	// field of the configuration, the default is config.DefaultUserAgent,
	// formalization — T-052). Empty — not set, and the transport sends
	// Go's default UA: a loophole only for the isolated layer tests;
	// NewOptions from the http section substitutes the default when the
	// field is empty — the production transport always identifies itself.
	UserAgent string
	// Resolver resolves hosts without DNS stage addresses (redirects to
	// other hosts). nil — the system resolver (SystemResolver). The cache
	// is the NewCachingResolver wrapper over the stage's resolver; the
	// "same resolver" of spec 5.5 item 2 — the worker's wiring will
	// assemble the dnsclient adapter.
	Resolver Resolver
	// BlockInternalAddrs — do not connect to internal addresses (loopback,
	// RFC1918/ULA, link-local, unspecified, multicast — AddrIsInternal,
	// including the IPv4-in-IPv6 form) via any connection path (the SSRF
	// guard, T-219): both the DNS stage addresses (FetchRequest.Addrs of
	// the starting host) and the redirect hosts' literals/resolves
	// (lookup) are filtered — the single funnel filterInternalAddrs. An
	// untrusted site could steer the crawler to 127.0.0.1,
	// 169.254.169.254 — cloud metadata — or an address of the network
	// where the DB/Redis live. The same flag enables the address control
	// of the render (render.Options.BlockInternalAddrs). false — the
	// previous behavior: dev/testbed benches resolve their scenarios into
	// internal networks (172.28.100.0/24); for production on the Internet
	// enabling it is a must (http.block_internal_addrs).
	BlockInternalAddrs bool
	// TLSClientConfig is the TLS client configuration. SNI and the
	// certificate check run against the URL's host (DialContext connects
	// to the pre-resolved IP without changing the target address). nil —
	// the usual check; InsecureSkipVerify and storing the certificate
	// check result (issuer, validity, SAN) — T-050.
	TLSClientConfig *tls.Config
}

// NewOptions assembles the parameters from the http config section
// (T-006); the T-049 limits are always enabled — the production transport
// never comes without the limits of the spec 5.5 table. FallbackDelay,
// Resolver and TLSClientConfig are not set as numbers in the spec: the
// address race pause is a transport detail; the stage resolver and TLS are
// wired at worker assembly (T-050/T-053). An empty UserAgent (a config
// assembled by hand, without Default/Validate) gets the
// config.DefaultUserAgent default — a transport built from the
// configuration never comes without an honest UA (spec §9, T-052).
func NewOptions(cfg config.HTTP) Options {
	limits := NewLimitOptions(cfg)
	ua := cfg.UserAgent
	if strings.TrimSpace(ua) == "" {
		ua = config.DefaultUserAgent
	}
	return Options{
		ConnectTimeout:      cfg.ConnectTimeout.D(),
		TLSHandshakeTimeout: cfg.TLSHandshakeTimeout.D(),
		HeaderTimeout:       cfg.HeaderTimeout.D(),
		TotalTimeout:        cfg.TotalTimeout.D(),
		MaxBodyBytes:        cfg.MaxBodyBytes.B(),
		UserAgent:           ua,
		Limits:              &limits,
		BlockInternalAddrs:  cfg.BlockInternalAddrs,
	}
}

// withDefaults replaces zero and negative values with the defaults of the
// spec 5.5 limits table.
func (o Options) withDefaults() Options {
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = DefaultConnectTimeout
	}
	if o.TLSHandshakeTimeout <= 0 {
		o.TLSHandshakeTimeout = DefaultTLSHandshakeTimeout
	}
	if o.HeaderTimeout <= 0 {
		o.HeaderTimeout = DefaultHeaderTimeout
	}
	if o.TotalTimeout <= 0 {
		o.TotalTimeout = DefaultTotalTimeout
	}
	if o.FallbackDelay <= 0 {
		o.FallbackDelay = DefaultFallbackDelay
	}
	return o
}

// FetchRequest is a single HTTP request: the URL and the host's
// pre-resolved addresses from the DNS stage (spec 5.5 item 2; the q:http
// task format — T-024). For a site check the URL is built from the scheme
// and host with the path "/" or no path at all — it is the same request
// (RequestURI yields "/"); other paths are not needed by the transport
// (spec 5.5 item 4, §9: only the main page, /robots.txt is not requested),
// the paths inside the redirect chain are set by the server. Addrs are
// mandatory: the transport does not resolve the primary host itself —
// that is exactly how it differs from an ordinary client; the request
// specifies the host (the Host header, SNI) and the connection addresses
// separately.
type FetchRequest struct {
	URL   *url.URL
	Addrs []netip.Addr
}

// FetchResult is the outcome of a single request for the stage's
// consumers. The minimal T-047 set: subsequent tasks extend it (the body
// limit and tarpits — T-049, certificate attributes — T-050, charsets —
// T-051, the redirect chain — T-048/T-058).
type FetchResult struct {
	// StatusCode is the response status code (3xx with Location — data
	// for the T-048 redirect loop, not an error).
	StatusCode int
	// Status is the whole status line ("200 OK").
	Status string
	// Header is the response header set.
	Header http.Header
	// Body is the response body. It must be closed by the caller: Close
	// releases the connection and stops the request's total timeout. With
	// the T-049 limits (Options.Limits) the body is additionally wrapped
	// in a BodyLimiter: the MaxBodyBytes cap and the anti-tarpit
	// (spec 5.5).
	Body io.ReadCloser
	// BodyLimiter is the T-049 body limiter; nil — the limits are off and
	// Body has no wrapper. Truncated is the "body truncated by the
	// MaxBodyBytes cap" flag: the data is partial, not a network error
	// (spec 8); the decision belongs to the consumer (T-058). The flag is
	// set by reading the Body by the time of io.EOF.
	BodyLimiter *BodyLimiter
	// FinalURL is the URL of this response: for a single request it
	// equals the requested one; in the T-048 redirect chain it will hold
	// the last response's address.
	FinalURL *url.URL
	// Elapsed is the time until the response headers (the response time
	// of spec 5.5 item 7).
	Elapsed time.Duration
	// TLS is the parameters of the established TLS connection (nil for
	// http).
	TLS *tls.ConnectionState
	// TLSInfo is the parsing of the connection's certificate and the
	// result of the own verification (spec 5.5 item 3): issuer, subject,
	// serial number, validity, SAN, chain, valid+reasons. It is filled by
	// Fetch — the URL's host (SNI/Host) needed for the check is known
	// there, not in ConnectionState; nil for http (not an error, §8).
	// Direct RoundTrip users assemble it themselves (NewTLSInfo).
	TLSInfo *TLSInfo
}

// cancelBody is a response body that stops the total timeout on Close:
// the TotalTimeout context lives through the whole body read, not only
// the Fetch (a premature cancellation would kill the connection on the
// very first byte).
type cancelBody struct {
	rc     io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Read(p []byte) (int, error) { return b.rc.Read(p) }

func (b *cancelBody) Close() error {
	err := b.rc.Close()
	b.cancel()
	return err
}

// dialFunc is one TCP connection attempt to an address. A separate type is
// the seam for swapping the transport in network-free unit tests (the
// dnsclient.exchangeFunc style); the production implementation is
// dialNet.
type dialFunc func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error)

// Fetcher is the stage's HTTP transport. It implements http.RoundTripper
// (a single request-response) and adds Fetch — the wrapper of one request
// with DNS stage addresses and a total timeout. Safe for concurrent use
// from goroutines: the shared state is http.Transport and Resolver, both
// thread-safe.
type Fetcher struct {
	opt  Options
	rt   *http.Transport
	res  Resolver
	dial dialFunc
	// lim is the active T-049 limits (Options.Limits); nil — disabled.
	lim *limitState
}

// compile-time check: *Fetcher is an http.RoundTripper.
var _ http.RoundTripper = (*Fetcher)(nil)

// NewFetcher creates a transport from the options (zero values — the
// defaults of the spec 5.5 limits table). A nil Resolver means the system
// resolver: local development and unit tests without the DNS stage.
func NewFetcher(opts Options) *Fetcher {
	opts = opts.withDefaults()
	if opts.Resolver == nil {
		opts.Resolver = SystemResolver
	}
	f := &Fetcher{
		opt: opts,
		res: opts.Resolver,
	}
	f.dial = f.dialNet
	if opts.Limits != nil {
		f.lim = newLimitState(opts.Limits.withDefaults())
	}
	f.rt = &http.Transport{
		// Direct connections: the outbound connections of one crawler IP
		// (spec 10.2); no proxies are configured.
		Proxy: nil,
		// Connecting to a pre-resolved IP: the primary host's addresses
		// arrive in the Fetch request's ctx; redirect hosts are resolved
		// by dialContext via the Resolver.
		DialContext: f.dialContext,
		// Spec 5.5: keep-alive is "disabled or a very short idle".
		// "Disabled" was chosen — pinned in T-049: only one main page per
		// site is requested (the minimum of requests), the redirect chain
		// goes across different hosts, and a pool keyed by "host" would
		// ride out an address change on re-resolve and hold thousands of
		// idle connections at concurrency 2000. IdleConnTimeout has no
		// effect with DisableKeepAlives (connections do not live longer
		// than the response) — the "short idle" option is not configured.
		DisableKeepAlives: true,
		// Per-layer timeouts (the spec 5.5 limits table): the TLS
		// handshake and waiting for the headers; connect and total live in
		// the dial and Fetch.
		TLSHandshakeTimeout:   opts.TLSHandshakeTimeout,
		ResponseHeaderTimeout: opts.HeaderTimeout,
		TLSClientConfig:       opts.TLSClientConfig,
	}
	return f
}

// CloseIdleConnections closes the transport's idle connections. With
// DisableKeepAlives there are none; the method is the shutdown hygiene
// contract for the worker assembly and tests.
func (f *Fetcher) CloseIdleConnections() { f.rt.CloseIdleConnections() }

// RoundTrip implements http.RoundTripper: a single request-response with
// no redirect following (that is the http.Client level; the chain is
// T-048). A request to a host whose ctx has no DNS stage addresses (a
// redirect) connects through the transport's Resolver — an http.Client
// over the Fetcher gets redirect host resolving with no extra code.
//
// Compression also lives here (T-051, spec 5.5): the request gets
// Accept-Encoding (gzip, br), and the response is decompressed with a cap.
// The hook sits in RoundTrip, not only in Fetch: the path of an
// http.Client over the Fetcher (the T-048 redirect chain) goes through
// this method and gets the same behavior. The request is cloned — the
// RoundTrip contract forbids modifying a foreign request, and reusable
// requests race over headers concurrently.
func (f *Fetcher) RoundTrip(req *http.Request) (*http.Response, error) {
	// The request rate token (spec 5.5: RPS): both paths go through this
	// RoundTrip — Fetch and the http.Client over the Fetcher (the T-048
	// redirect chain) — the token is taken once per outbound request.
	if f.lim != nil {
		if err := f.lim.rps.take(req.Context()); err != nil {
			return nil, fmt.Errorf("httpfetch: %s %s: %w", req.Method, req.URL, err)
		}
	}
	req = req.Clone(req.Context())
	if req.Header.Get("Accept-Encoding") == "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	resp, err := f.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if err := f.decodeResponseBody(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// bodyCeiling is the single source of the body cap for decompression and
// the read limit: with the T-049 limits enabled — Limits.MaxBodyBytes
// (the same number BodyLimiter cuts by); without limits —
// Options.MaxBodyBytes or the limits table default. The two separate
// fields (Options and Limits) are filled with the same cfg.MaxBodyBytes
// in the production NewOptions assembly, but a manual Options assembly
// diverged — decompression and reading saw different caps.
func (f *Fetcher) bodyCeiling() int64 {
	if f.lim != nil && f.lim.opt.MaxBodyBytes > 0 {
		return f.lim.opt.MaxBodyBytes
	}
	if f.opt.MaxBodyBytes > 0 {
		return f.opt.MaxBodyBytes
	}
	return defaultDecompressLimit
}

// decodeResponseBody decompresses the response body by Content-Encoding
// with a cap on the decompressed size (protection against zip bombs,
// spec 5.5) and fits into the reading chain: network → decompression (a
// single coding or an RFC 9110 layer chain for a compound "gzip, br", the
// bodyCeiling cap per layer) → the T-049 readable body limit → the
// consumer. The cap equals the body limit: the decompressed stream is the
// body, otherwise reading to the end (the dedup hash) would inflate the
// bomb to the limit but with gigabytes of decompression. The tail beyond
// the cap is classified by decompressTailMargin (an honest truncation vs a
// bomb — see decompressCap.classifyTail) on the final — last decoded —
// stream. The Content-Encoding/Content-Length headers are stripped: the
// body is already decompressed in hand, the coding headers would lie (as
// with net/http's transparent decompression). An unsupported coding is a
// request error: silently handing compressed bytes as the body is worse
// than an honest rejection.
func (f *Fetcher) decodeResponseBody(resp *http.Response) error {
	// RFC 9110 §5.3 allows spreading a list header over several lines —
	// the semantics equal a comma-separated list; Values+Join assembles
	// the whole value (Get would take only the first line, and the
	// compound packaging of "gzip" + "br" on two lines would read as a
	// single gzip).
	ce := strings.Join(resp.Header.Values("Content-Encoding"), ",")
	if ce == "" {
		return nil // no compression — the body as is
	}
	limit := f.bodyCeiling()
	// A BodyLimiter with the same cap may sit above the decompression: its
	// probe byte beyond the limit falls into decompressCap's tail
	// classification — a tail within decompressTailMargin gives a
	// truncation with Truncated (a live site slightly over the limit,
	// H-2), a continuing stream gives ErrDecompressTooLarge without
	// spending the bomb (T-051).
	body, err := decodeContentEncoding(resp.Body, ce, limit)
	if err != nil {
		_ = resp.Body.Close()
		return err
	}
	resp.Body = body
	if !identityOnlyContentEncoding(ce) {
		// An effective coding: the body is already decompressed in hand,
		// the coding headers would lie — strip them (like net/http's
		// transparent decompression); without effective codings (identity,
		// empty) both stay truthful.
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
	}
	return nil
}

// Fetch performs the GET of one URL with a connection to pre-resolved
// addresses (spec 5.5 items 1–2): the URL's host sets the Host header and
// SNI, the connection goes to Addrs. The total timeout (TotalTimeout, 20 s
// by default) covers the request in full, including reading the body by
// the caller — until Body is closed. An error response status is not a
// Fetch error (the code is in FetchResult); an error is only transport
// (the network, timeouts, a ctx cancellation, an unsupported
// Content-Encoding — see RoundTrip).
func (f *Fetcher) Fetch(ctx context.Context, r FetchRequest) (*FetchResult, error) {
	if r.URL == nil {
		return nil, errors.New("httpfetch: a request without a URL")
	}
	if r.URL.Scheme != "http" && r.URL.Scheme != "https" {
		return nil, fmt.Errorf("httpfetch: unsupported scheme %q (http/https expected)", r.URL.Scheme)
	}
	host := strings.ToLower(r.URL.Hostname())
	if host == "" {
		return nil, fmt.Errorf("httpfetch: the URL %s without a host", r.URL)
	}
	if len(r.Addrs) == 0 {
		return nil, fmt.Errorf("httpfetch: %s: no DNS-stage addresses (spec 5.5 item 2)", r.URL)
	}

	tctx, cancel := context.WithTimeout(ctx, f.opt.TotalTimeout)
	tctx = context.WithValue(tctx, addrsKey{}, &addrsValue{host: host, addrs: r.Addrs})
	req, err := http.NewRequestWithContext(tctx, http.MethodGet, r.URL.String(), nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("httpfetch: %s: %w", r.URL, err)
	}
	if f.opt.UserAgent != "" {
		req.Header.Set("User-Agent", f.opt.UserAgent)
	}

	// Via its own RoundTrip: Accept-Encoding (gzip, br — spec 5.5),
	// decompression with a cap and the RPS token are received by this path
	// as well as by the http.Client over the Fetcher (the T-048 redirect
	// chain) — one code for both.
	start := time.Now()
	resp, err := f.RoundTrip(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("httpfetch: GET %s: %w", r.URL, err)
	}

	body := io.ReadCloser(&cancelBody{rc: resp.Body, cancel: cancel})
	var limiter *BodyLimiter
	if f.lim != nil {
		// The T-049 body limits: truncation at MaxBodyBytes (the flag is
		// Truncated, not a network error) and the anti-tarpit on the min
		// speed after the first byte (a violation is a timeout, the
		// connection closes). The total timeout cancellation is the same
		// cancel.
		limiter = newBodyLimiter(body, cancel, f.lim.opt)
		body = limiter
	}
	return &FetchResult{
		StatusCode:  resp.StatusCode,
		Status:      resp.Status,
		Header:      resp.Header,
		Body:        body,
		BodyLimiter: limiter,
		FinalURL:    resp.Request.URL,
		Elapsed:     time.Since(start),
		TLS:         resp.TLS,
		TLSInfo:     NewTLSInfo(resp.TLS, host),
	}, nil
}

// IsTimeout reports whether a Fetch error was caused by the timeout of one
// of the layers (connect, TLS, headers, total), not by a protocol
// refusal. The flag is needed by the consumer's classification (a timeout
// is temp_error, not "no site"; spec 5.5, §8) — the dnsclient.IsTimeout
// style.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// IsConnectFailure classifies a fetch error as a connect-phase failure —
// establishing the TCP connection (the dead-host cutoff, T-164): a
// connection refused (an RST on SYN — the port is closed) or a connect
// timeout (a SYN with no answer, including the request context's deadline
// expiring during the dial phase). Other dial errors (network unreachable
// and the like) are more often a property of the crawler's route than of
// the host and do not enter the strict dead-host signature. Not a connect
// phase: the TLS handshake (IsTLSCertError), waiting for the headers,
// reading the body, the total timeout after the connection is established,
// resolving redirect hosts, a limits refusal (RPS, connection slots) —
// such errors have no *net.OpError{Op:"dial"} in the chain. The dial
// errors of redirect hosts vs the starting host's failure are told apart
// by the consumer via the completed part of the chain (T-048: for the
// first hop's failure it is empty).
func IsConnectFailure(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		return false
	}
	return errors.Is(opErr.Err, syscall.ECONNREFUSED) || opErr.Timeout()
}

// addrsKey is the ctx key type for the DNS stage addresses (not a string:
// name collisions with external code are unacceptable).
type addrsKey struct{}

// addrsValue is the DNS stage addresses of one Fetch request: host is the
// normalized URL host (for the comparison in dialContext), addrs are the
// connection addresses in the DNS response order.
type addrsValue struct {
	host  string
	addrs []netip.Addr
}

// addrsFromContext extracts the DNS stage addresses from the request ctx.
func addrsFromContext(ctx context.Context) *addrsValue {
	v, _ := ctx.Value(addrsKey{}).(*addrsValue)
	return v
}

// dialContext is the connection point to a pre-resolved IP: net/http
// passes the request context and the target "host:port" address from the
// URL here, while we connect to the DNS stage or redirect-resolve
// addresses.
func (f *Fetcher) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("httpfetch: an unsupported network %q", network)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("httpfetch: the connect address %q: %w", addr, err)
	}
	host = strings.ToLower(host)

	// The request's primary host: the addresses came from the DNS stage
	// together with the q:http task (spec 5.5 item 2) — no resolving. With
	// the SSRF circuit enabled the addresses pass the same funnel as
	// redirect hosts (T-219): a name that resolves into an internal
	// network with no DNS stage control is no better than an internal
	// host from Location.
	if info := addrsFromContext(ctx); info != nil && info.host == host {
		addrs, err := f.filterInternalAddrs(fmt.Sprintf("the host %q", info.host), info.addrs)
		if err != nil {
			return nil, err
		}
		return f.dialAddrs(ctx, addrs, port)
	}

	// A host without DNS stage addresses — a redirect to another host
	// (spec 5.5 item 2: "resolve via the same resolver with the cache").
	// A request via RoundTrip without addresses (an http.Client over the
	// transport) also lands here: the original ctx does not carry the
	// addresses to the new host — the comparison above.
	addrs, err := f.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	return f.dialAddrs(ctx, addrs, port)
}

// lookup returns the host's addresses: an IP literal from a redirect URL —
// by itself (the resolver does not need the name), a name — via the
// transport's Resolver. Internal addresses are rejected by the
// filterInternalAddrs funnel (the SSRF guard, T-219) — see its comment.
func (f *Fetcher) lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	var addrs []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		addrs = []netip.Addr{ip}
	} else {
		var err error
		addrs, err = f.res.LookupNetIP(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("httpfetch: resolve %q: %w", host, err)
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("httpfetch: resolve %q: an empty answer", host)
		}
	}
	return f.filterInternalAddrs(fmt.Sprintf("resolve %q", host), addrs)
}

// filterInternalAddrs is the single funnel of the SSRF circuit (T-219):
// with Options.BlockInternalAddrs, internal addresses (AddrIsInternal —
// loopback, RFC1918/ULA, link-local, unspecified, multicast, including
// IPv4-in-IPv6) are rejected from the connection address list. Both dial
// paths go through it: the DNS stage addresses (FetchRequest.Addrs,
// dialContext) and the lookup results (Location literals, redirect host
// resolves). All addresses of the list being internal is a connection
// refusal; a mixed answer leaves the external ones. The flag off — the
// list unchanged (dev/testbed resolve their scenarios into internal
// networks).
func (f *Fetcher) filterInternalAddrs(subject string, addrs []netip.Addr) ([]netip.Addr, error) {
	if !f.opt.BlockInternalAddrs {
		return addrs, nil
	}
	external := make([]netip.Addr, 0, len(addrs))
	for _, a := range addrs {
		if !AddrIsInternal(a) {
			external = append(external, a)
		}
	}
	if len(external) == 0 {
		return nil, fmt.Errorf("httpfetch: %s: internal-only addresses %v — blocked (SSRF, http.block_internal_addrs)", subject, addrs)
	}
	return external, nil
}

// AddrIsInternal is an address of an internal/special range that an
// Internet crawler has no business reaching: loopback, private (RFC1918
// and ULA — netip merges them into IsPrivate), link-local (including the
// cloud metadata 169.254.169.254), unspecified and multicast. The address
// is unmapped (Unmap): the netip predicates treat the IPv4-in-IPv6 form
// unequally (IsLoopback/IsPrivate/IsLinkLocalUnicast unwrap 4-in-6,
// IsUnspecified does not: ::ffff:0.0.0.0 would pass as "external"), and
// there is no reason to rely on each predicate's implicit behavior — Unmap
// gives one semantics to any 4-in-6 literal from Location (T-219; the same
// trick is limitIPMap.limitFor). Exported: it is the single definition of
// an "internal address" of the http.block_internal_addrs SSRF circuit;
// the render (render) uses it too.
func AddrIsInternal(a netip.Addr) bool {
	a = a.Unmap()
	return a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() ||
		a.IsLinkLocalMulticast() || a.IsUnspecified() || a.IsMulticast()
}

// dialNet is the production dial (the dial field is the swap seam in
// tests): TCP to the address; the connect timeout and cancellation come
// from the attempt's ctx (dialTimed).
func (f *Fetcher) dialNet(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", net.JoinHostPort(addr.String(), port))
}

// dialTimed is one connection attempt with the connect timeout (the
// spec 5.5 limits table): on top of the request context both the
// cancellation and the total timeout act (the runtime takes the nearest
// deadline).
func (f *Fetcher) dialTimed(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
	tctx, cancel := context.WithTimeout(ctx, f.opt.ConnectTimeout)
	defer cancel()
	return f.dial(tctx, addr, port)
}

// dialAddrs connects to one of the DNS stage addresses. The strategy with
// several addresses is a happy-eyeballs-like race with the FallbackDelay
// step (RFC 8305 connection attempt delay), generalized from the two
// address families to the whole list: the first attempt starts at once,
// the next one — after a pause, and so on; the first successful connection
// wins, the losing attempts are interrupted by canceling the derived
// context, and late connections are closed by their attempt goroutines
// themselves.
//
// Against a sequential scan: a dead address silent on SYN waits out the
// full connect timeout (5 s), and a domain with four A records where only
// the last one is alive would spend 20 s — the whole request budget
// (spec 5.5). The race bounds the wait by the first successful connection
// plus one pause per skipped address; "sequential fallback" remains a
// special case — the next attempt starts immediately when all launched
// ones have already failed, without waiting for the pause. The attempt
// order is the DNS response order (unbound serves the zone's records as
// they are; family prioritization is a job for address prioritization,
// not the transport).
func (f *Fetcher) dialAddrs(ctx context.Context, addrs []netip.Addr, port string) (net.Conn, error) {
	switch len(addrs) {
	case 0:
		return nil, errors.New("httpfetch: no addresses to connect to")
	case 1:
		return f.dialLimited(ctx, addrs[0], port)
	}

	// The derived context stops the hanging attempts after the winner has
	// returned (a dial that ended with a connection no longer suffers from
	// the cancellation — it only acts on a dial in progress).
	stop, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	won := false
	gaveUp := false // the caller left by cancellation: the winner is no longer needed
	var winner net.Conn
	failed := 0
	var lastErr error
	event := make(chan struct{}, 1) // wakes the wait loop; a buffer of 1 — the signal is not lost

	attempt := func(i int) {
		conn, err := f.dialLimited(stop, addrs[i], port)
		mu.Lock()
		defer mu.Unlock()
		if won || gaveUp {
			if conn != nil {
				_ = conn.Close() // the race was lost after the success already
			}
			return
		}
		if conn != nil {
			won, winner = true, conn
			signal(event)
			return
		}
		failed++
		lastErr = err
		signal(event)
	}

	started := 1
	go attempt(0)
	timer := time.NewTimer(f.opt.FallbackDelay)
	defer timer.Stop()
	for {
		select {
		case <-event:
			mu.Lock()
			conn, ok, fails, err := winner, won, failed, lastErr
			mu.Unlock()
			if ok {
				return conn, nil
			}
			if fails == len(addrs) {
				return nil, fmt.Errorf("httpfetch: all %d addresses failed: %w", len(addrs), err)
			}
			if fails == started && started < len(addrs) {
				// all launched attempts failed — the next address without
				// a pause
				go attempt(started)
				started++
				timer.Reset(f.opt.FallbackDelay)
			}
		case <-timer.C:
			if started < len(addrs) {
				go attempt(started)
				started++
				if started < len(addrs) {
					timer.Reset(f.opt.FallbackDelay)
				}
			}
		case <-ctx.Done():
			// The cancellation may win the select after an attempt has
			// already won: the established connection (with the limitConn
			// limit slots) must be released here, not leak away with the
			// losing exit. Late attempts are seen by the gaveUp flag.
			mu.Lock()
			gaveUp = true
			conn := winner
			winner = nil
			mu.Unlock()
			if conn != nil {
				_ = conn.Close()
			}
			return nil, ctx.Err()
		}
	}
}

// signal sends a non-blocking notification of an attempt event.
func signal(event chan<- struct{}) {
	select {
	case event <- struct{}{}:
	default:
	}
}

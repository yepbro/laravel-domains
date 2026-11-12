// The redirect chain pass of the HTTP stage (T-048; spec 5.5 items 1, 4
// and spec 5.2 item 8). The T-047 transport does not follow redirects
// (RoundTrip without http.Client, CheckRedirect is inapplicable) — this
// file drives the chain: HTTP hops 301/302/303/307/308 and
// <meta http-equiv="refresh"> of 200 responses count together against one
// max_redirects and one cycle detect; the final page is the last response
// of the chain.

package httpfetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"crawler/internal/idn"
)

// DefaultMaxRedirects is the maximum number of chain hops (the spec 5.5
// limits table: "max redirects 8"). FetchChain substitutes it for empty
// options; an explicit value comes from http.max_redirects (T-006).
const DefaultMaxRedirects = 8

// DefaultChainTimeout is the cap on the total budget of a redirect chain.
// Spec 5.5 does not set it as a number: TotalTimeout (20 s) applies to a
// single hop, and a chain without a total budget would drag on (max_redirects
// hops × the hop timeout, the config allows up to 100 hops) for tens of
// minutes, holding a worker concurrency slot and a per-IP semaphore. The
// budget default is the sum of the hop budgets, but no more than this cap:
// slow/hostile sites cannot hold a domain check longer than two minutes.
const DefaultChainTimeout = 2 * time.Minute

// Body scan limits for meta refresh. They are not in the configuration:
// spec 5.5 sets no numbers — an implementation detail.
const (
	// redirectMetaLimit — how many bytes of the HTML body start to scan:
	// <head> with meta tags fits in tens of kilobytes even with inline
	// styles, reading further is pointless (HTML 4.2.5: meta refresh only
	// works in head). §9 — untrusted content is bounded in size and time;
	// both are finite here.
	redirectMetaLimit = 256 << 10
	// redirectMetaChunk is a single read while scanning.
	redirectMetaChunk = 8 << 10
	// redirectMetaOverlap is the overlap between chunks: a marker
	// ("<meta", "</head") cut by a read boundary must still be found.
	redirectMetaOverlap = 16
)

// Scanner patterns (byte-based — for EqualFold without conversions).
var (
	redirectMetaOpen = []byte("<meta")
	redirectHeadEnd  = []byte("</head")
)

// ErrTooManyRedirects — the chain was interrupted: max_redirects exceeded
// or a cycle detected (spec 5.5 item 1). One classification for both
// causes: the limit and a cycle alike are a permanent property of the
// site, not a transient failure; repeating the check would not change the
// outcome, so this is NOT temp_error (temp_error is reserved for
// SERVFAIL/timeouts, spec 6.2): per §8, "endless redirects are interrupted
// by the 5.5 limits, an error code is recorded" — the consumer records the
// error code in the result, while the final domain status (no_http if no
// subdomain answered) is decided by the episode aggregator (T-058, the §7
// algorithm).
var ErrTooManyRedirects = errors.New("httpfetch: too many redirects")

// RedirectStep is a chain step: the request URL and the status of the
// received response (the format of the subdomain_results.redirects
// record, spec 5.5 item 7 "redirect chain"; jsonb T-058). The last element
// is the final page.
type RedirectStep struct {
	URL    string `json:"url"`
	Status int    `json:"status"`
}

// ChainResult is the outcome of the chain pass. Result is the final
// response (the end of the chain, spec 5.5 item 4), nil on an error
// (limit/cycle/transport): Chain then holds the completed part.
type ChainResult struct {
	Result *FetchResult
	Chain  []RedirectStep
}

// ChainOptions are the chain pass parameters.
type ChainOptions struct {
	// MaxRedirects is the maximum number of chain hops: HTTP redirects and
	// meta refresh count together (spec 5.5 item 1 "accounting"). The
	// semantics of http.max_redirects (T-006): 0 — do not follow at all
	// (the first response is final), negative values reduce to 0. A
	// convenient default is DefaultMaxRedirects; FetchChain substitutes
	// it.
	MaxRedirects int
	// TotalTimeout is the total chain budget: the parent context for the
	// timeout of each hop and for reading the final body. 0 — default: the
	// sum of the hop budgets (1+MaxRedirects)×TotalTimeout, no more than
	// DefaultChainTimeout. An explicit value is accepted as is.
	TotalTimeout time.Duration
}

// FetchChain walks the redirect chain from the starting request to the
// final page with the DefaultMaxRedirects limit (8, the spec 5.5 limits
// table). An explicit limit (http.max_redirects) — FetchChainOpts.
func FetchChain(ctx context.Context, f *Fetcher, req FetchRequest) (*ChainResult, error) {
	return FetchChainOpts(ctx, f, req, ChainOptions{MaxRedirects: DefaultMaxRedirects})
}

// FetchChainOpts performs the starting request and walks the chain to the
// final page — "at the end of the chain" (spec 5.5 item 4): after HTTP
// redirects (301/302/303/307/308 with Location; 303 semantically requires
// GET — all chain requests are GET anyway) and the meta refresh of 200
// responses; the Location and the meta tag's url are resolved relative to
// the current response (net/url), hosts are converted to punycode and
// lower case (spec 5.2 item 8). An http↔https scheme switch is an
// ordinary chain step; non-standard ports are preserved.
//
// The outcome is the last response: its status and body (Body is a
// reader; the read limit and tarpit protection are T-049) go into
// ChainResult.Result, the history — Chain ([]RedirectStep: the request URL
// and the status of each response).
//
// Errors: a transport error of any step is returned as is (the
// classification IsTimeout → temp_error belongs to the consumer,
// spec 6.2); exceeding the limit and a cycle are ErrTooManyRedirects
// wrappers (Result nil, Chain the completed part); resolving a new host is
// a resolver error. The DNS stage addresses apply only to the starting
// host (its reappearance in the chain reuses them, spec 5.5 item 2
// "connecting to a previously resolved IP"); other hosts are resolved by
// the transport's resolver — "via the same resolver with the cache" (the
// CachingResolver wrapper of T-047 in the worker wiring).
func FetchChainOpts(ctx context.Context, f *Fetcher, req FetchRequest, opts ChainOptions) (*ChainResult, error) {
	if req.URL == nil {
		return nil, errors.New("httpfetch: a request without a URL")
	}
	max := opts.MaxRedirects
	if max < 0 {
		max = 0
	}

	// The starting URL goes through the same pipeline as the hops: a
	// punycode host, an explicit path. The q:http task contract provides
	// punycode; normalization guards against unexpected unicode.
	start, ok := redirectResolve(req.URL, req.URL.String())
	if !ok {
		return nil, fmt.Errorf("httpfetch: the start URL %s: the scheme or host does not normalize", req.URL)
	}
	startHost := start.Hostname()
	visited := map[string]struct{}{redirectKey(start): {}}
	cur := FetchRequest{URL: start, Addrs: req.Addrs}

	// The total chain budget: the parent context for all hops (each
	// Fetch's own TotalTimeout stays inside). Canceling the budget mid
	// flight closes the hop connection — a timeout-class error (temp_error
	// at the consumer). The final body is read by the consumer after the
	// return already: the budget lives until the body is closed
	// (chainBody); on error paths it is canceled here.
	budget := opts.TotalTimeout
	if budget <= 0 {
		budget = time.Duration(1+max) * f.opt.TotalTimeout
		if budget > DefaultChainTimeout {
			budget = DefaultChainTimeout
		}
	}
	ctx, cancelBudget := context.WithTimeout(ctx, budget)

	var chain []RedirectStep
	redirects := 0
	for {
		res, err := f.Fetch(ctx, cur)
		if err != nil {
			cancelBudget()
			return &ChainResult{Chain: chain}, err
		}
		chain = append(chain, RedirectStep{URL: cur.URL.String(), Status: res.StatusCode})

		if max == 0 {
			// The "do not follow" mode (T-006): the first response is the
			// final one. Before redirectNext — the meta scan is not
			// performed at all: without a hop, reading up to 256 KB of
			// the body for meta refresh is needless I/O.
			res.Body = &chainBody{rc: res.Body, cancel: cancelBudget}
			return &ChainResult{Result: res, Chain: chain}, nil
		}
		next, follow := redirectNext(res)
		if !follow {
			// The end of the chain: this response is the outcome.
			res.Body = &chainBody{rc: res.Body, cancel: cancelBudget}
			return &ChainResult{Result: res, Chain: chain}, nil
		}
		_ = res.Body.Close()
		if redirects >= max {
			cancelBudget()
			return &ChainResult{Chain: chain}, fmt.Errorf(
				"%w: the limit %d exceeded on the hop to %s", ErrTooManyRedirects, max, next)
		}
		if _, dup := visited[redirectKey(next)]; dup {
			cancelBudget()
			return &ChainResult{Chain: chain}, fmt.Errorf("%w: a cycle at %s", ErrTooManyRedirects, next)
		}
		visited[redirectKey(next)] = struct{}{}
		redirects++

		addrs := req.Addrs
		if host := next.Hostname(); host != startHost {
			// A new host: the DNS stage addresses do not apply to it (they
			// were issued to the starting host) — resolve with the
			// transport's resolver.
			addrs, err = f.lookup(ctx, host)
			if err != nil {
				cancelBudget()
				return &ChainResult{Chain: chain}, err
			}
		}
		cur = FetchRequest{URL: next, Addrs: addrs}
	}
}

// redirectIsHTTPStatus — the codes that are followed (spec 5.5 item 1);
// other 3xx (300, 304, …) do not count as hops.
func redirectIsHTTPStatus(code int) bool {
	switch code {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	}
	return false
}

// redirectNext determines the next chain URL from the response. A second
// value of false means the response is final. A response with a broken or
// foreign Location (not a URL, a non-http/https scheme, an invalid host)
// is not followed: the response itself is the outcome, like a browser
// without a hop. For a 200 HTML response the body is scanned (and wrapped
// in redirectBody — no bytes are lost).
func redirectNext(res *FetchResult) (*url.URL, bool) {
	if redirectIsHTTPStatus(res.StatusCode) {
		raw := strings.TrimSpace(res.Header.Get("Location"))
		if raw == "" {
			return nil, false
		}
		return redirectResolve(res.FinalURL, raw)
	}
	if res.StatusCode == http.StatusOK {
		ct := strings.ToLower(res.Header.Get("Content-Type"))
		if !strings.Contains(ct, "html") {
			return nil, false // meta refresh is HTML-document behavior
		}
		raw, ok := redirectScanMeta(res)
		if !ok {
			return nil, false
		}
		return redirectResolve(res.FinalURL, raw)
	}
	return nil, false
}

// redirectResolve builds the next request URL from the raw
// Location/meta refresh string: relative links are resolved against the
// current response as the base (net/url), the host is converted to
// punycode (spec 5.2 item 8 — "hosts from Location and <meta refresh>
// containing unicode are converted to punycode before the request") and
// lower case, an empty path is normalized to "/". IP literals (IPv4/IPv6)
// do not need HostToASCII and IPv6 fails on it (T-009) — they pass as is:
// url.URL.Hostname() yields the literal without brackets, ready for
// dialing. false — the string cannot be followed: not a URL, a foreign
// scheme, no host, or an invalid name (broken punycode, UTS-46 forbidden
// characters).
func redirectResolve(base *url.URL, raw string) (*url.URL, bool) {
	ref, err := url.Parse(raw)
	if err != nil {
		return nil, false
	}
	u := base.ResolveReference(ref)
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, false
	}
	host := u.Hostname()
	if host == "" {
		return nil, false
	}
	if _, err := netip.ParseAddr(host); err != nil {
		// A name (not an IP literal): punycode and lower case.
		ascii, err := idn.HostToASCII(host)
		if err != nil {
			return nil, false
		}
		if port := u.Port(); port != "" {
			u.Host = net.JoinHostPort(ascii, port)
		} else {
			u.Host = ascii
		}
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u, true
}

// redirectKey is the cycle detection key (spec 5.5 item 1): the
// normalized URL without the fragment — a fragment is not sent to the
// server and is not a hop. The query counts: redirects with a counter
// (?i=1, ?i=2, …) are legitimate distinct pages, not a cycle; an exact
// repetition of a URL with the query is a cycle; "endless" parametric
// chains are stopped by max_redirects.
func redirectKey(u *url.URL) string {
	k := *u
	k.Fragment, k.RawFragment = "", ""
	return k.String()
}

// redirectScanMeta scans the start of the 200 response's HTML body for
// meta refresh (spec 5.5 item 4). It returns the hop URL if found (the
// first valid tag with url; a refresh of itself without url is ignored).
// The body prefix read during the scan is returned to the consumer by the
// redirectBody wrapper — a no-hop outcome loses no bytes. The scan is
// finite: redirectMetaLimit bytes and the closing </head> (a meta refresh
// after the end of head is invalid, HTML 4.2.5). The parser is minimal and
// string-based — a full HTML parser for one tag is redundant and heavy
// (§9: untrusted content — only bounded parsers).
func redirectScanMeta(res *FetchResult) (string, bool) {
	var buf bytes.Buffer
	chunk := make([]byte, redirectMetaChunk)
	pos, total := 0, 0
	for total < redirectMetaLimit {
		n, rerr := res.Body.Read(chunk)
		if n > 0 {
			total += n
			buf.Write(chunk[:n])
			var raw string
			var stop bool
			pos, raw, stop = redirectScanBuffer(buf.Bytes(), pos)
			if raw != "" {
				// A hop was found but may not happen: a url of a foreign
				// scheme (redirectResolve will return false) or the
				// caller's "do not follow" mode. The prefix read by the
				// scan is returned to the consumer by the wrapper — even
				// on this path a no-hop outcome loses no bytes.
				if buf.Len() > 0 {
					res.Body = &redirectBody{prefix: bytes.NewReader(buf.Bytes()), rc: res.Body}
				}
				return raw, true // a hop: the caller will close the body
			}
			if stop {
				break
			}
		}
		if rerr != nil {
			break // EOF (the body is shorter than the limit) or a read error — the scan is over
		}
	}
	if buf.Len() > 0 {
		res.Body = &redirectBody{prefix: bytes.NewReader(buf.Bytes()), rc: res.Body}
	}
	return "", false
}

// redirectScanBuffer is a scanner pass over the accumulated buffer from
// position pos: it looks for <meta tags (case-insensitive) until the
// closing </head>. It returns the new position, the hop URL ("" — not
// found) and the "stop looking" flag (</head> before the next <meta>).
func redirectScanBuffer(b []byte, pos int) (int, string, bool) {
	for {
		m := redirectIndexFold(b, pos, redirectMetaOpen)
		h := redirectIndexFold(b, pos, redirectHeadEnd)
		if h >= 0 && (m < 0 || h < m) {
			return len(b), "", true
		}
		if m < 0 {
			// The marker may be cut by a read boundary — the overlap.
			if p := len(b) - redirectMetaOverlap; p > pos {
				return p, "", false
			}
			return pos, "", false
		}
		end := redirectTagEnd(b, m)
		if end < 0 {
			return pos, "", false // the tag is not fully read — waiting for more bytes
		}
		if raw, ok := redirectMetaURL(b, m, end); ok {
			return end + 1, raw, false
		}
		pos = end + 1
	}
}

// redirectIndexFold is the index of the first occurrence of pat
// (case-insensitive) in b[from:], or -1.
func redirectIndexFold(b []byte, from int, pat []byte) int {
	for i := from; i+len(pat) <= len(b); i++ {
		if bytes.EqualFold(b[i:i+len(pat)], pat) {
			return i
		}
	}
	return -1
}

// redirectTagEnd is the index of the '>' of the tag starting at b[start]
// (accounting for value quotes: a '>' inside quotes does not close the
// tag), or -1.
func redirectTagEnd(b []byte, start int) int {
	var quote byte
	for i := start + 1; i < len(b); i++ {
		c := b[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '>':
			return i
		}
	}
	return -1
}

// redirectMetaURL extracts the hop URL from the tag in the range
// [tagStart, tagEnd) — the name with attributes, no angle brackets. The
// case of attribute names and http-equiv does not matter; values are
// quoted ('…' or "…") or bare. false — not a refresh, no content, or
// content without url (a self-refresh is ignored).
func redirectMetaURL(b []byte, tagStart, tagEnd int) (string, bool) {
	p := tagStart + len(redirectMetaOpen)
	if p >= tagEnd {
		return "", false // "<meta>" without attributes
	}
	if c := b[p]; !redirectIsSpace(c) && c != '/' {
		return "", false // "<metadata" and the like — not a meta tag
	}
	equiv, content := redirectTagAttrs(b, p, tagEnd)
	if !strings.EqualFold(equiv, "refresh") {
		return "", false
	}
	return redirectMetaContent(content)
}

// redirectTagAttrs parses the tag attributes (the [p, end) range) with a
// simple scan of "name[=value]" separated by spaces; the value is quoted
// or runs until a space. It returns the http-equiv and content values
// (empty strings — the attribute is absent).
func redirectTagAttrs(b []byte, p, end int) (equiv, content string) {
	for p < end {
		for p < end && (redirectIsSpace(b[p]) || b[p] == '/') {
			p++
		}
		if p >= end {
			break
		}
		ns := p
		for p < end && b[p] != '=' && !redirectIsSpace(b[p]) {
			p++
		}
		name := strings.ToLower(string(b[ns:p]))
		vs, ve := p, p
		if p < end && b[p] == '=' {
			p++
			for p < end && redirectIsSpace(b[p]) {
				p++
			}
			if p < end && (b[p] == '\'' || b[p] == '"') {
				q := b[p]
				p++
				vs = p
				for p < end && b[p] != q {
					p++
				}
				ve = p
				if p < end {
					p++ // the closing quote
				}
			} else {
				vs = p
				for p < end && !redirectIsSpace(b[p]) {
					p++
				}
				ve = p
			}
		}
		switch name {
		case "http-equiv":
			equiv = string(b[vs:ve])
		case "content":
			content = string(b[vs:ve])
		}
	}
	return equiv, content
}

// redirectIsSpace — markup whitespace characters.
func redirectIsSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\r', '\n', '\f':
		return true
	}
	return false
}

// redirectMetaContent parses the content value: "N; url=X" (N is an
// integer number of delay seconds). The crawler does not wait out the
// delay: both 0 (an immediate hop) and N > 0 lead to the same final
// address at the end of the chain (spec 5.5 item 4 — the crawler is not a
// browser and cannot "wait out" the counter). Without the url part — a
// self-refresh: ignore (there is no X). Forms without a number ("url=/x")
// and with a broken/negative number are invalid per HTML and are not
// handled.
func redirectMetaContent(content string) (string, bool) {
	left, rest, hasSemi := strings.Cut(content, ";")
	left = strings.TrimSpace(left)
	if left == "" || strings.HasPrefix(left, "-") {
		return "", false
	}
	if _, err := strconv.Atoi(left); err != nil {
		return "", false
	}
	if !hasSemi {
		return "", false // "5" — a refresh of itself
	}
	eq := strings.Index(rest, "=")
	if eq < 0 {
		return "", false
	}
	if name := strings.TrimSpace(rest[:eq]); !strings.EqualFold(name, "url") {
		return "", false
	}
	v := strings.TrimSpace(rest[eq+1:])
	if len(v) >= 2 && (v[0] == '\'' || v[0] == '"') && v[len(v)-1] == v[0] {
		v = strings.TrimSpace(v[1 : len(v)-1]) // paired quotes around the URL
	}
	if v == "" {
		return "", false
	}
	return v, true
}

// redirectBody is the final response body after the meta refresh scan:
// first the read prefix, then the original stream; Close closes the
// original stream (with its TotalTimeout timer — the cancelBody of
// T-047).
type redirectBody struct {
	prefix *bytes.Reader
	rc     io.ReadCloser
}

func (b *redirectBody) Read(p []byte) (int, error) {
	if b.prefix.Len() > 0 {
		return b.prefix.Read(p)
	}
	return b.rc.Read(p)
}

func (b *redirectBody) Close() error { return b.rc.Close() }

// chainBody is the final response body under the chain budget: closing
// the body stops the budget (the chain's shared ctx), just as cancelBody
// stops the total timeout of a single request (T-047). The final body is
// read after FetchChainOpts has already returned — the budget lives until
// the body is closed.
type chainBody struct {
	rc     io.ReadCloser
	cancel context.CancelFunc
}

func (b *chainBody) Read(p []byte) (int, error) { return b.rc.Read(p) }

func (b *chainBody) Close() error {
	err := b.rc.Close()
	b.cancel()
	return err
}

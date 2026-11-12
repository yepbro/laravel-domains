// Package checker — the library pipeline of the live check of one domain
// (T-123, a refactoring of the crawlerctl check from T-119): the DNS of
// all the subdomains → the HTTP of both schemes of the live ones (+ an SPA
// render with render.remote_url configured) → a passive analysis by the
// rules. Read-only to the DB and the queues: it writes and publishes
// nothing (the publication belongs to the scheduler); only the subdomains
// reference table is read from the DB. The consumers — the crawlerctl
// check CLI (debugging the rules) and the public landing form
// (internal/web, T-123); the UA/timeouts/limits — from the configuration,
// no active probes (the passive ethics of the spec §5.8/§9).
package checker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/dnsworker"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/idn"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/rules"
)

// SubRecord — the outcome of one record (subdomain×variant): what both
// the CLI and the landing form show.
type SubRecord struct {
	SubdomainID int16
	Label       string // "" is the apex
	Host        string // label.domain
	Variant     string // main | http
	Scheme      string // http | https
	Status      string // the HTTP code or "error <class>"
	FinalURL    string
	Redirects   []httpfetch.RedirectStep
	TLSValid    bool
	TLSNotAfter time.Time
	HasTLS      bool
	Signals     []rules.Evidence
	Score       float64
	VerMin      string
	VerMax      string
	Rendered    bool // the analysis by the render DOM
}

// Result — the outcome of the domain check.
type Result struct {
	Domain   string // punycode
	Unicode  string
	Zone     string
	Apex     string // the apex branch of the DNS (delegated/not_delegated/…)
	Terminal string // the terminal status of the DNS stage ("" — the HTTP ran)
	Records  []SubRecord
	Found    bool // at least one record with is_laravel
	Score    float64
	VerMin   string
	VerMax   string
	DNSMS    int64 // the duration of the DNS phase, ms
	TotalMS  int64 // the duration of the whole check, ms
}

// Logger — the minimal pipeline logger (satisfied by *slog.Logger).
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

// LiveFunc — the pipeline seam for the consumers (the landing form T-123
// substitutes it in tests without a network; the production implementation
// is the Run method of the long-lived Runtime circuit, T-220, and the
// one-off Run function for the CLI).
type LiveFunc func(ctx context.Context, cfg *config.Config, pool Pool, domain string, log Logger) (*Result, error)

// Pool — the pipeline dependency: the subdomains reference table (db.DB).
type Pool = db.DB

// Run executes the full pipeline of one name on the ready circuit (T-220):
// the method matches LiveFunc by signature — web passes it from the
// circuit assembled for the lifetime of the service; the cfg parameter is
// ignored (the circuit was assembled from its own configuration at
// NewRuntime, including the analyzer.laravel_min_score threshold). The
// pool is used only for reading the subdomains reference table
// (LoadSubdomains). Network/analysis errors are not returned as a check
// failure — they are reflected in the records (Status/Signals); returning
// an error is only for infrastructure failures (the reference DB, a ctx
// cancellation). A one-off call without its own circuit — the free Run
// function (the CLI).
func (rt *Runtime) Run(ctx context.Context, _ *config.Config, pool Pool, domain string, log Logger) (*Result, error) {
	if log == nil {
		// NewRuntime allows a nil logger — the pipeline must not panic
		// on Warn (P3-1 of the T-220 review).
		log = rt.log
	}
	start := time.Now()
	norm, err := idn.Normalize(domain)
	if err != nil {
		return nil, fmt.Errorf("checker: invalid domain name %q: %w", domain, err)
	}
	res := &Result{Domain: norm.ASCII, Unicode: norm.Unicode, Zone: norm.Zone}

	subs, err := dnsworker.LoadSubdomains(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("checker: %w", err)
	}

	// The DNS stage as a library: the manual check chain (resolve.go)
	// under the T-035 limits — the chain and the limiter are assembled on
	// the circuit once (NewRuntime); here only the pass itself.
	resolve := rt.resolve

	dnsStart := time.Now()
	apex, err := dnsworker.CheckApex(ctx, resolve, norm.ASCII)
	if err != nil {
		return nil, fmt.Errorf("checker: %s: apex: %w", norm.ASCII, err)
	}
	res.Apex = apex.Status.String()
	if apex.Status.Stop() {
		if st, ok := apex.Status.DomainStatus(); ok {
			res.Terminal = st.String()
		}
		res.TotalMS = time.Since(start).Milliseconds()
		return res, nil
	}

	rep, err := dnsworker.CheckSubdomains(ctx, resolve, norm.ASCII, subs, rt.cfg.DNS.IPv6)
	if err != nil {
		return nil, fmt.Errorf("checker: %s: subdomains: %w", norm.ASCII, err)
	}
	var wild dnsworker.WildcardResult
	if info, werr := dnsworker.DetectWildcard(ctx, resolve, norm.ASCII); werr != nil {
		log.Warn("checker: wildcard detection failed — continuing without filtering", "err", werr)
	} else if info.Detected {
		if wild, err = dnsworker.MarkWildcardSubdomains(info.Addrs, wildSubs(rep)); err != nil {
			return nil, fmt.Errorf("checker: %s: wildcard marking: %w", norm.ASCII, err)
		}
	}
	episode, err := dnsworker.DecideEpisode(apex, rep, wild)
	if err != nil {
		return nil, fmt.Errorf("checker: %s: episode decision: %w", norm.ASCII, err)
	}
	if !episode.HasHTTPTask() {
		res.Terminal = episode.Status.String()
		res.DNSMS = time.Since(dnsStart).Milliseconds()
		res.TotalMS = time.Since(start).Milliseconds()
		return res, nil
	}
	res.DNSMS = time.Since(dnsStart).Milliseconds()

	// The transport like the production http-worker (T-049/T-050/T-047) —
	// one per circuit (NewRuntime) with the shared limitState; here only
	// the pass itself.
	fetcher := rt.fetcher

	// The live check has nowhere (and no reason) to write production
	// content: since T-147 the worker saves the bodies with an early Put
	// and requires a ContentStore — a one-off in-memory store accepts the
	// pages, the analysis reads them back by the record hash (the render
	// will substitute its DOM for the HTML for SPA below).
	mem := newCheckerStore()
	worker, err := httpworker.New(httpworker.ChainFetch(fetcher), httpworker.Options{
		MaxRedirects: rt.cfg.HTTP.MaxRedirects,
		Store:        mem,
	})
	if err != nil {
		return nil, fmt.Errorf("checker: %w", err)
	}
	// The identifiers are synthetic: ProcessDomain does not validate them.
	task := queue.HTTPTask{ZoneID: 1, DomainID: 1, Name: norm.ASCII, Subdomains: episode.HTTPSubdomains}
	dom, err := worker.ProcessDomain(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("checker: %s: HTTP: %w", norm.ASCII, err)
	}

	// The SPA render — an external CDP browser (render.remote_url); the
	// manager is assembled on the circuit, the browser generation is
	// raised lazily by the first render (an unavailable Chromium — the
	// analysis by the original HTML, as with a record render error too).
	// The first failure of the "browser" class damps the render until the
	// end of this check (the analog of renderer=nil of the one-off Run
	// before T-220): retries on the shared manager would start the
	// generation under its mutex and pay Dial+SessionStart timeouts on
	// every SPA record (P2-3 of the T-220 review); the next check will
	// try again.
	renderer := rt.renderer
	renderDown := false

	// The rules snapshot — from the Reloader of the circuit: the set is
	// read once at assembly and kept fresh by the background reload,
	// rather than from the disk per each request (T-220).
	set := rt.rules.Set()

	for i := range dom.Subdomains {
		sub := &dom.Subdomains[i]
		for j := range sub.Records {
			rec := &sub.Records[j]
			sr := SubRecord{
				SubdomainID: rec.SubdomainID,
				Label:       sub.Label,
				Host:        sub.Hostname,
				FinalURL:    rec.FinalURL,
				Redirects:   rec.Redirects,
				HasTLS:      rec.TLS != nil && rec.Scheme == model.SchemeHTTPS,
			}
			if sr.HasTLS {
				sr.TLSValid = rec.TLS.Valid
				sr.TLSNotAfter = rec.TLS.NotAfter
			}
			switch rec.Variant {
			case model.VariantHTTP:
				sr.Variant = "http"
			default:
				sr.Variant = "main"
			}
			if rec.Scheme == model.SchemeHTTPS {
				sr.Scheme = "https"
			} else {
				sr.Scheme = "http"
			}
			if rec.ErrClass != httpworker.ErrNone {
				sr.Status = "error " + rec.ErrClass.String()
			} else {
				sr.Status = fmt.Sprintf("%d", rec.HTTPStatus)
			}

			page := rules.Page{
				HTML:    mem.body(rec.ContentHash),
				Cookies: ruleCookies(rec.Cookies),
				Headers: map[string][]string(rec.Header),
			}
			if rec.SPA != nil && rec.SPA.SPA && renderer != nil && !renderDown {
				if ro, rerr := renderer.Run(ctx, render.RenderTask{URL: rec.FinalURL}); rerr == nil {
					page.HTML = []byte(ro.DOM)
					for _, c := range ro.Cookies {
						page.Cookies = append(page.Cookies, rules.Cookie{Name: c.Name, Value: c.Value})
					}
					page.Headers = map[string][]string(ro.ResponseHeaders)
					sr.Rendered = true
				} else if render.ClassOf(rerr) == render.ClassBrowser {
					log.Warn("checker: render unavailable — SPA on the raw HTML for the rest of the check", "err", rerr)
					renderDown = true
				}
			}
			eval := set.EvaluateCtx(ctx, page, rules.Limits{})
			sr.Signals = eval.Signals
			sr.Score = eval.Score
			sr.VerMin, sr.VerMax = eval.VerMin, eval.VerMax
			if eval.IsLaravel(rt.cfg.Analyzer.LaravelMinScore) {
				sr.Score = eval.Score
				if eval.Score > res.Score {
					res.Score = eval.Score
					res.VerMin, res.VerMax = eval.VerMin, eval.VerMax
				}
				res.Found = true
			}
			res.Records = append(res.Records, sr)
		}
	}
	res.TotalMS = time.Since(start).Milliseconds()
	return res, nil
}

// wildSubs — the input of the wildcard marking of T-038 (a copy of
// resultWildSubs of dnsworker).
func wildSubs(rep *dnsworker.SubdomReport) []dnsworker.WildSubdomain {
	out := make([]dnsworker.WildSubdomain, 0, len(rep.Results))
	for _, r := range rep.Results {
		addrs := make([]net.IP, 0, len(r.A)+len(r.AAAA))
		for _, a := range r.A {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		for _, a := range r.AAAA {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		out = append(out, dnsworker.WildSubdomain{ID: r.ID, Label: r.Label, Addrs: addrs})
	}
	return out
}

func ruleCookies(cs []*http.Cookie) []rules.Cookie {
	out := make([]rules.Cookie, 0, len(cs))
	for _, c := range cs {
		out = append(out, rules.Cookie{Name: c.Name, Value: c.Value})
	}
	return out
}

// checkerStore — the one-off in-memory content store of the live check
// (T-147): the http-worker saves the page bodies with an early Put and
// works without retaining bytes in memory; the live check has nowhere (and
// no reason) to write the production packs — the pages of one check settle
// here and are read back by the record hash for the passive analysis (the
// SPA render substitutes its DOM for the HTML). It implements
// content.ContentStore (Put/Get/Delete, the contract of T-041: a dedup by
// hash, Created, a refcount); the memory is bounded by the task of one
// domain (pages ≤ 2 × subdomains, a body ≤ max_body_bytes).
type checkerStore struct {
	mu     sync.Mutex
	blocks map[content.Hash][]byte
	refs   map[content.Hash]int
}

func newCheckerStore() *checkerStore {
	return &checkerStore{
		blocks: make(map[content.Hash][]byte),
		refs:   make(map[content.Hash]int),
	}
}

func (s *checkerStore) Put(_ context.Context, data []byte) (content.PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := content.HashOf(data)
	if s.refs[h] > 0 {
		s.refs[h]++
		return content.PutResult{Hash: h}, nil
	}
	s.blocks[h] = append([]byte(nil), data...)
	s.refs[h] = 1
	return content.PutResult{Hash: h, Created: true}, nil
}

func (s *checkerStore) Get(_ context.Context, h content.Hash) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refs[h] <= 0 {
		return nil, content.ErrNotFound
	}
	return append([]byte(nil), s.blocks[h]...), nil
}

func (s *checkerStore) Delete(_ context.Context, h content.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refs[h] <= 0 {
		return content.ErrNotFound
	}
	s.refs[h]--
	if s.refs[h] == 0 {
		delete(s.blocks, h)
	}
	return nil
}

// body returns the body by the record hash (nil — no content: a
// truncation, an unsaved page, an alias without a reference). For the
// live check the alias content is available as its own (the T-055 class
// is one) — the rules see the real bytes of the record page.
func (s *checkerStore) body(h *content.Hash) []byte {
	if h == nil {
		return nil
	}
	b, err := s.Get(context.Background(), *h)
	if err != nil {
		return nil
	}
	return b
}

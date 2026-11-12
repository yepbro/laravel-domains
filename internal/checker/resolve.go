// The resolver chain of the live check of one domain (T-119/T-123) — its
// own failure semantics, different from the mass pipeline.
// FallbackResolver (internal/dnsclient, T-107) switches only on a
// transport failure: an RCODE answer of the live server (SERVFAIL) stays
// the consumer's decision — with unbound degraded, the mass crawl must
// not shoot the same questions at the public DNS. A manual check is a
// single human question: the human waits for an answer, not a failure
// classification, so the chain here escalates on RCODE failures too
// (SERVFAIL/REFUSED): the dev-testbed unbound answers SERVFAIL for
// everything outside the test zones (a deliberate hermeticity of the
// environment); the production unbound does not answer like that; the
// reserves are dns.fallback_resolvers (the dev form — public resolvers,
// production — per configuration). NXDOMAIN is a legitimate "no such
// name" outcome and needs no escalation; the mass pipeline (the
// dns-worker) does not use this chain.
package checker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"crawler/internal/config"
	"crawler/internal/dnsclient"
)

// escalateTarget — an element of the chain: the resolver and its address
// (only for the diagnostics of error messages).
type escalateTarget struct {
	addr string
	res  dnsclient.Resolver
}

// escalateResolver — a resolver chain with escalation on failure (see the
// package comment). The first element is the primary (the local unbound,
// dns.resolvers[0]), the rest are dns.fallback_resolvers in configuration
// order. Safe for concurrent use from goroutines (no state).
type escalateResolver struct {
	targets []escalateTarget
}

// compile-time check: *escalateResolver is a dnsclient.Resolver.
var _ dnsclient.Resolver = (*escalateResolver)(nil)

// Exchange walks the chain until the first useful outcome. A useful
// outcome is an answer with any RCODE except SERVFAIL and REFUSED
// (NOERROR, NXDOMAIN, …): the consumer interprets its semantics
// (T-036/T-039), the chain does not decide. SERVFAIL/REFUSED and a
// transport failure — moving to the next resolver; a ctx cancellation
// interrupts the chain right away (shutdown does not shoot the reserves).
//
// If nobody gave a useful outcome — the last RCODE answer is returned
// (an honest classification: SERVFAIL → temp_error at the consumer), and
// with purely transport failures — an error listing the chain and
// wrapping the last cause (errors.Is/As work through it).
func (r *escalateResolver) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	var (
		lastRes *dnsclient.Result
		lastErr error
		fails   []string
	)
	for _, t := range r.targets {
		res, err := t.res.Exchange(ctx, q)
		if err == nil && res.RCode != dnsclient.RCodeSERVFAIL && res.RCode != dnsclient.RCodeREFUSED {
			return res, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		switch {
		case err != nil:
			lastRes, lastErr = nil, err
			fails = append(fails, t.desc(err))
		default:
			lastRes, lastErr = res, nil
			fails = append(fails, t.desc(res.RCode))
		}
	}
	if lastRes != nil {
		return lastRes, nil
	}
	return nil, fmt.Errorf("checker: %s: no resolver in the chain responded (%d): %s; last: %w",
		q.Name, len(fails), strings.Join(fails, "; "), lastErr)
}

// desc — a diagnostic description of the failure of one chain element.
func (t escalateTarget) desc(v any) string {
	if t.addr == "" {
		return fmt.Sprint(v)
	}
	return t.addr + ": " + fmt.Sprint(v)
}

// newResolverChain assembles the chain for the live check: the primary
// client (dns.resolvers[0] — the local unbound) plus one client per
// dns.fallback_resolvers entry. udpPoolSize is the size of the UDP socket
// pool of each client (0 — the udp_pool_size of the dns section, the mass
// pipeline): the long-lived circuit of the form (T-220) passes the small
// LiveUDPPoolSize — the parallelism of the live check is limited by the
// web semaphore; a production-sized socket fund is not needed. An empty
// reserve list degenerates into the single primary client — the behavior
// before the chain (T-119). The returned close closes all the clients of
// the chain (one call, the errors joined).
func newResolverChain(ctx context.Context, cfg config.DNS, udpPoolSize int) (dnsclient.Resolver, func() error, error) {
	primaryOpts := dnsclient.NewOptions(cfg)
	if udpPoolSize > 0 {
		primaryOpts.UDPPoolSize = udpPoolSize
	}
	primary, err := dnsclient.NewClient(ctx, primaryOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("checker: primary resolver %s: %w", primaryOpts.Addr, err)
	}
	clients := []*dnsclient.Client{primary}
	closeAll := func() error {
		var errs []error
		for _, c := range clients {
			if err := c.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	targets := []escalateTarget{{addr: primaryOpts.Addr, res: primary}}
	for _, addr := range cfg.FallbackResolvers {
		opts := dnsclient.NewOptions(cfg)
		opts.Addr = addr
		if udpPoolSize > 0 {
			opts.UDPPoolSize = udpPoolSize
		}
		c, err := dnsclient.NewClient(ctx, opts)
		if err != nil {
			_ = closeAll()
			return nil, nil, fmt.Errorf("checker: fallback resolver %s: %w", addr, err)
		}
		clients = append(clients, c)
		targets = append(targets, escalateTarget{addr: addr, res: c})
	}
	return &escalateResolver{targets: targets}, closeAll, nil
}

// Switching the DNS exchange to backup resolvers (T-107, the remainder of
// the DNS-12 find of the bulk review; spec 5.4: "The resolver is a local
// Unbound … Public resolvers are only a fallback (the list is in the
// config)") — the FallbackResolver decorator over the Resolver interface,
// in the same policy layer as the T-035 limiter: the T-034 transport stays
// "one client — one resolver", the chain is the decorator's concern above it.
//
// The switch condition is a transport failure: the lower resolver returned
// an error after its own retries were exhausted (in the T-034 contract an
// error is only the network: timeout, refusal, disconnect; that is exactly
// "the local one ran out of attempts" and "unreachability" — from the
// exchange's point of view they are the same thing). Any RCODE is the
// answer of a live server, not a refusal: the response semantics do not
// change, NXDOMAIN → not_delegated and SERVFAIL → temp_error remain the
// consumer's decision (T-036/T-039); the decorator does not re-ask the
// backup because of a SERVFAIL. A ctx cancellation is not a resolver
// failure: the cancellation error is returned without polling the backups.
//
// The switch happens per exchange, without remembering state (a "dead"
// resolver is not short-circuited): the local unbound coming back needs no
// re-probe/cool-down, and a state race between thousands of concurrent
// exchanges cannot arise at all. The price — while the local one is dead,
// every exchange pays for its retry loop (dns.timeout × (dns.retries+1))
// before going to the backup: the switch metric and the growing latency
// show it, but the pipeline keeps running and does not pile domains into temp_error.
//
// Place in the decorator stack: under LimitResolver (cmd/dns-worker
// assembles LimitResolver(FallbackResolver(Client…))). The concurrency
// limit and the QPS token are taken once for the whole "local → backups"
// cycle, so switching does not spend the limits again; the T-035 adaptive
// sees the exchange outcome — a success via the backup does not lower the
// limits (the pipeline runs); a full exhaustion of the chain lands in the adaptive window as an ordinary error of the lower resolver.

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// FallbackTarget is an element of the resolver chain: the Resolver itself
// (usually *Client) and its address. The address is only for diagnostics —
// error messages and startup probe logs (probe.go); it does not affect switching.
type FallbackTarget struct {
	// Addr is the resolver address host:port for error messages and logs.
	Addr string
	// Resolver is a chain resolver (not nil).
	Resolver Resolver
}

// FallbackStats is a snapshot of the switching state for monitoring
// (metric export — the internal/dnsworker collector, T-107; only data
// here, no dependency on internal/metrics — like LimitStats).
type FallbackStats struct {
	// Switches is the exchanges answered by a backup resolver (the local
	// one failed on transport, the backup answered). A growing rate signals
	// degradation of the local unbound.
	Switches int64
	// Exhausted is the exchanges that failed across the whole chain (nobody
	// answered); each such exchange is a future consumer temp_error (T-036/T-039).
	Exhausted int64
}

// FallbackResolver is a resolver chain with backup switching (T-107):
// the first element is the primary (the local unbound), the rest are the
// backups (dns.fallback_resolvers) in config order. Safe for concurrent
// use from goroutines (the shared state is only the counters).
type FallbackResolver struct {
	primary   FallbackTarget
	fallbacks []FallbackTarget

	switches  atomic.Int64
	exhausted atomic.Int64
}

// compile-time check: *FallbackResolver — Resolver.
var _ Resolver = (*FallbackResolver)(nil)

// NewFallbackResolver assembles the chain: primary is the primary resolver,
// fallbacks are the backups in polling order. An empty backup list is
// allowed (isolated environments, dev): the decorator degenerates into
// the primary resolver, the counters and the contract do not change. A nil
// resolver is an error (a programming one, caught at service startup).
func NewFallbackResolver(primary FallbackTarget, fallbacks ...FallbackTarget) (*FallbackResolver, error) {
	if primary.Resolver == nil {
		return nil, errors.New("dnsclient: NewFallbackResolver: primary resolver not set")
	}
	for i, f := range fallbacks {
		if f.Resolver == nil {
			return nil, fmt.Errorf("dnsclient: NewFallbackResolver: fallback resolver %d not set", i+1)
		}
	}
	// A copy: the caller's slice must not affect the chain after assembly.
	fbs := make([]FallbackTarget, len(fallbacks))
	copy(fbs, fallbacks)
	return &FallbackResolver{primary: primary, fallbacks: fbs}, nil
}

// Exchange performs the question along the resolver chain (spec 5.4): the
// primary first; on a transport failure — the backups in order until the
// first answer. The outcome (a Result with any RCODE) of the first
// responder is returned as is — response classification does not change.
//
// If nobody answered, the error lists every resolver of the chain and
// wraps the cause of the last one (errors.Is/As and IsTimeout work on it):
// the final error is classified by the last resolver polled.
//
// The outcome Elapsed is the whole Exchange time, including the retry
// loops of the failed resolvers; Attempts is the exchanges of the responder
// (Client does not report the number of failed exchanges, inventing one would be a metric lie).
func (r *FallbackResolver) Exchange(ctx context.Context, q Question) (*Result, error) {
	start := time.Now()
	res, err := r.primary.Resolver.Exchange(ctx, q)
	if err == nil {
		return res, nil
	}
	// A cancellation of the call itself is not a resolver refusal: the
	// backups are not polled (a shutdown must not shell the public DNS with
	// the same question).
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	fails := []string{failDesc(r.primary, err)}
	lastErr := err
	for _, fb := range r.fallbacks {
		res, err := fb.Resolver.Exchange(ctx, q)
		if err == nil {
			r.switches.Add(1)
			res.Elapsed = time.Since(start)
			return res, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		fails = append(fails, failDesc(fb, err))
		lastErr = err
	}
	r.exhausted.Add(1)
	return nil, fmt.Errorf("dnsclient: %s %s: no resolver in the chain responded (%d): %s; last: %w",
		qTypeName(q.Type), q.Name, len(fails), strings.Join(fails, "; "), lastErr)
}

// Stats returns a snapshot of the switching counters (monitoring points
// for T-107; export — internal/dnsworker).
func (r *FallbackResolver) Stats() FallbackStats {
	return FallbackStats{
		Switches:  r.switches.Load(),
		Exhausted: r.exhausted.Load(),
	}
}

// failDesc is a diagnostic description of one chain resolver's failure.
func failDesc(t FallbackTarget, err error) string {
	if t.Addr == "" {
		return err.Error()
	}
	return t.Addr + ": " + err.Error()
}

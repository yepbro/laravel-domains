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
// consumer's decision (T-036/T-039). A ctx cancellation is not a resolver
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
// T-300 (the bulk review 04#P2-1/P2-2) makes the two deliberate compromises
// of this policy opt-in via the dns.fallback config subsection:
//
//   - escalate_rcodes — a SERVFAIL/REFUSED answer of the primary is not
//     final either: the question is re-asked along the reserves until a
//     useful answer (any other RCODE). Motivation — the incident 24.10: a
//     local unbound answering SERVFAIL to everything pushed 491K domains
//     into temp_error while the public reserves were alive; the manual
//     check chain (internal/checker/resolve.go) has always escalated. The
//     counterweight — the mass crawl must not unload its full dns.max_qps
//     onto the public DNS in such a storm — is the EscalateQPS budget: an
//     exchange without a token keeps the primary's answer (the default
//     behavior, visible by the primary rcode counters). The "SERVFAIL of
//     the primary" counters work in BOTH modes — the incident is visible
//     without the enabling.
//   - fast_switch (mapped onto the client Options.FastFail by
//     dnsclient.NewOptions) — the first transport error of a resolver moves
//     the question to the next one instead of the full internal retry loop:
//     the retry budget belongs to the chain, not to one resolver.
//
// Place in the decorator stack: under LimitResolver (cmd/dns-worker
// assembles LimitResolver(FallbackResolver(Client…))). The concurrency
// limit and the QPS token are taken once for the whole "local → backups"
// cycle, so switching does not spend the limits again; the T-035 adaptive
// sees the exchange outcome — a success via the backup does not lower the
// limits (the pipeline runs); a full exhaustion of the chain lands in the adaptive window as an ordinary error of the lower resolver. The time
// paid to the failed primary (transport or escalation) accumulates in
// PrimaryFailNanos — the degradation is visible in the latency metric too
// (04#P2-2), not only in the switch counters.

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crawler/internal/config"
)

// DefaultEscalateQPS is the RCODE-escalation budget (dns.fallback.escalate_qps
// omitted/0 with escalate_rcodes on): 100 ≈ 1% of the planned dns.max_qps —
// the reserve drains a SERVFAIL backlog slowly but surely, without knocking
// on the public DNS with the whole crawl flow.
const DefaultEscalateQPS = 100

// FallbackOptions is the switching policy of the chain (the dns.fallback
// subsection, T-300). The zero value is the behavior before T-300: only a
// transport failure switches, without a budget.
type FallbackOptions struct {
	// EscalateRCodes — also switch when the primary ANSWERS with
	// SERVFAIL/REFUSED: the question is re-asked along the reserves until
	// a useful answer (any other RCODE; the semantics of the manual check
	// chain, internal/checker/resolve.go). See the package comment for the
	// budget and the motivation.
	EscalateRCodes bool
	// EscalateQPS is the ceiling of the reserve QUERIES per second during
	// RCODE escalations (the avalanche guard): one token per request to a
	// fallback resolver (a walk of N reserves is N tokens). Applied only
	// together with EscalateRCodes; <= 0 is replaced with
	// DefaultEscalateQPS by NewFallbackResolver — the guard cannot be
	// disabled by a zero.
	EscalateQPS float64
}

// NewFallbackOptions maps the dns section onto the switching policy
// (the NewLimitOptions style): the raw values, the defaults are applied by
// NewFallbackResolver.
func NewFallbackOptions(cfg config.DNS) FallbackOptions {
	return FallbackOptions{
		EscalateRCodes: cfg.Fallback.EscalateRCodes,
		EscalateQPS:    cfg.Fallback.EscalateQPS,
	}
}

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
// (metric export — the internal/dnsworker collector, T-107/T-300; only data
// here, no dependency on internal/metrics — like LimitStats).
type FallbackStats struct {
	// Switches is the exchanges answered by a backup resolver (the local
	// one failed on transport — or, with dns.fallback.escalate_rcodes, was
	// escalated over a SERVFAIL/REFUSED answer). A growing rate signals
	// degradation of the local unbound.
	Switches int64
	// Exhausted is the exchanges that failed across the whole chain on
	// transport (nobody answered); each such exchange is a future consumer
	// temp_error (T-036/T-039). An abandoned RCODE escalation is NOT here:
	// the primary did answer.
	Exhausted int64
	// PrimaryServFail is the primary answers with SERVFAIL — counted in
	// BOTH modes (T-300, 04#P2-1): the incident metric works without the
	// escalation enabling; its steady growth is the alert to enable
	// dns.fallback.escalate_rcodes or to fix the local resolver.
	PrimaryServFail int64
	// PrimaryRefused is the primary answers with REFUSED (the second
	// escalatable code; see PrimaryServFail).
	PrimaryRefused int64
	// Escalated is the reserve QUERIES granted by the budget — one token
	// per request to a fallback resolver, not per escalation (a walk of N
	// reserves is N queries): the observable avalanche guard, 04#P2-1; the
	// ceiling of EscalateQPS applies to exactly these queries.
	Escalated int64
	// PrimaryFailNanos is the total time spent in primary exchanges that
	// did not produce the final answer (a transport failure — switched or
	// exhausted — or a "bad" RCODE answer escalated over, successfully or
	// not): the degradation of the local resolver in the latency metric,
	// not only in the switch counters (04#P2-2) — the reserve answers, the
	// limiter stays "healthy", but every such exchange pays this time.
	PrimaryFailNanos int64
}

// FallbackResolver is a resolver chain with backup switching (T-107):
// the first element is the primary (the local unbound), the rest are the
// backups (dns.fallback_resolvers) in config order. Safe for concurrent
// use from goroutines (the shared state is the counters and the escalation
// budget).
type FallbackResolver struct {
	primary   FallbackTarget
	fallbacks []FallbackTarget
	opt       FallbackOptions
	// budget is the escalation rate guard; nil unless opt.EscalateRCodes
	// (built once in NewFallbackResolver).
	budget *escalateBudget

	switches        atomic.Int64
	exhausted       atomic.Int64
	primaryServFail atomic.Int64
	primaryRefused  atomic.Int64
	escalated       atomic.Int64
	primaryFail     atomic.Int64
}

// compile-time check: *FallbackResolver — Resolver.
var _ Resolver = (*FallbackResolver)(nil)

// NewFallbackResolver assembles the chain: primary is the primary resolver,
// fallbacks are the backups in polling order, opts is the switching policy
// (the zero value — the behavior before T-300). An empty backup list is
// allowed (isolated environments, dev): the decorator degenerates into
// the primary resolver, the counters and the contract do not change. A nil
// resolver or a NaN escalation budget is an error (a programming one,
// caught at service startup).
func NewFallbackResolver(primary FallbackTarget, opts FallbackOptions, fallbacks ...FallbackTarget) (*FallbackResolver, error) {
	if primary.Resolver == nil {
		return nil, errors.New("dnsclient: NewFallbackResolver: primary resolver not set")
	}
	for i, f := range fallbacks {
		if f.Resolver == nil {
			return nil, fmt.Errorf("dnsclient: NewFallbackResolver: fallback resolver %d not set", i+1)
		}
	}
	// The budget defaults/defense (the config validation is the first
	// line, T-298; this is the package's own guard against a direct
	// construction). NaN first: NaN <= 0 is false, a NaN rate would make
	// tryTake silently refuse everything.
	if math.IsNaN(opts.EscalateQPS) {
		return nil, errors.New("dnsclient: NewFallbackResolver: escalate_qps is NaN")
	}
	r := &FallbackResolver{primary: primary, opt: opts}
	// An escalation without reserves is a no-op walk of an empty list: the
	// policy is forced off so that the budget and the Escalated counter do
	// not grow without a single query to the reserves (the config
	// validation rejects the combination — this is the guard against a
	// direct construction).
	if opts.EscalateRCodes && len(fallbacks) > 0 {
		if opts.EscalateQPS <= 0 {
			r.opt.EscalateQPS = DefaultEscalateQPS
		}
		r.budget = newEscalateBudget(r.opt.EscalateQPS, time.Now)
	} else {
		r.opt.EscalateRCodes = false
	}
	// A copy: the caller's slice must not affect the chain after assembly.
	fbs := make([]FallbackTarget, len(fallbacks))
	copy(fbs, fallbacks)
	r.fallbacks = fbs
	return r, nil
}

// Exchange performs the question along the resolver chain (spec 5.4): the
// primary first; on a transport failure — the backups in order until the
// first answer; with dns.fallback.escalate_rcodes (T-300) — also on a
// SERVFAIL/REFUSED answer of the primary, within the EscalateQPS budget.
// The outcome (a Result with any RCODE) of the first responder is returned
// as is — response classification does not change.
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
	primaryElapsed := time.Since(start)
	if err == nil {
		if res.RCode != RCodeSERVFAIL && res.RCode != RCodeREFUSED {
			return res, nil
		}
		// The primary answered with a "bad" RCODE. Counted in both modes
		// (04#P2-1): the incident is visible without the escalation.
		r.countPrimaryRCode(res.RCode)
		if !r.opt.EscalateRCodes || r.budget == nil {
			// Off by config: the primary's answer is final — the current
			// behavior.
			return res, nil
		}
		// Escalation (T-300, 04#P2-1): re-ask the reserves until a useful
		// answer — any RCODE except SERVFAIL/REFUSED (the manual check
		// chain semantics; a SERVFAIL of a reserve is walked over). Every
		// reserve QUERY spends its own budget token (the ceiling is on the
		// requests to the public resolvers, not on escalations: a walk of
		// N reserves is N queries); without a token the walk stops and the
		// primary's answer is final — no waiting for a token, the exchange
		// holds the T-035 slot.
		for _, fb := range r.fallbacks {
			if !r.budget.tryTake() {
				break
			}
			r.escalated.Add(1)
			fbRes, fbErr := fb.Resolver.Exchange(ctx, q)
			if fbErr == nil && fbRes.RCode != RCodeSERVFAIL && fbRes.RCode != RCodeREFUSED {
				r.switches.Add(1)
				r.primaryFail.Add(int64(primaryElapsed))
				fbRes.Elapsed = time.Since(start)
				return fbRes, nil
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
		}
		// Nobody gave a useful answer (or the budget ran out mid-walk): the
		// primary's own answer is the honest outcome (its rcode → the
		// consumer's classification), the cost of the walk is reflected in
		// Elapsed and in the primary fail time. NOT exhausted: the primary
		// did answer.
		r.primaryFail.Add(int64(primaryElapsed))
		res.Elapsed = time.Since(start)
		return res, nil
	}
	// A cancellation of the call itself is not a resolver refusal: the
	// backups are not polled (a shutdown must not shell the public DNS with
	// the same question).
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	// The time of the failed primary is observable even when the chain
	// then dies completely (04#P2-2).
	r.primaryFail.Add(int64(primaryElapsed))
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

// countPrimaryRCode bumps the "SERVFAIL/REFUSED of the primary" counter of
// the given code (the metric in both modes, T-300).
func (r *FallbackResolver) countPrimaryRCode(rc RCode) {
	if rc == RCodeSERVFAIL {
		r.primaryServFail.Add(1)
		return
	}
	r.primaryRefused.Add(1)
}

// Stats returns a snapshot of the switching counters (monitoring points
// for T-107/T-300; export — internal/dnsworker).
func (r *FallbackResolver) Stats() FallbackStats {
	return FallbackStats{
		Switches:         r.switches.Load(),
		Exhausted:        r.exhausted.Load(),
		PrimaryServFail:  r.primaryServFail.Load(),
		PrimaryRefused:   r.primaryRefused.Load(),
		Escalated:        r.escalated.Load(),
		PrimaryFailNanos: r.primaryFail.Load(),
	}
}

// failDesc is a diagnostic description of one chain resolver's failure.
func failDesc(t FallbackTarget, err error) string {
	if t.Addr == "" {
		return err.Error()
	}
	return t.Addr + ": " + err.Error()
}

// escalateBudget is the avalanche guard of the RCODE escalation (T-300,
// 04#P2-1): a token bucket with a lazy refill — tokens are accrued at
// access time for (now−last)×rate, no background goroutines (the
// limitBucket mechanics, but without waiting: an escalation without a
// token must not hold the exchange — the T-035 slot is already held).
// The burst capacity is one second of the rate. Safe for concurrent use.
type escalateBudget struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64 // capacity (a second of the rate, at least 1)
	tokens float64
	last   time.Time        // the moment of the last accrual
	now    func() time.Time // injectable clock (tests)
}

// newEscalateBudget assembles the bucket: qps tokens per second, the
// initial fill is the full burst (the storm starts from a clean bucket —
// the first second is the most valuable for the escalation).
func newEscalateBudget(qps float64, now func() time.Time) *escalateBudget {
	burst := math.Max(1, math.Ceil(qps))
	return &escalateBudget{
		rate:   qps,
		burst:  burst,
		tokens: burst,
		last:   now(),
		now:    now,
	}
}

// tryTake spends one token if the bucket has one; false — above the
// budget, without waiting (the caller keeps the primary's answer).
func (b *escalateBudget) tryTake() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := b.now(); n.After(b.last) {
		b.tokens = min(b.burst, b.tokens+n.Sub(b.last).Seconds()*b.rate)
		b.last = n
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

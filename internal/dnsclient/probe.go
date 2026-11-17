// Startup synthetic probe of a resolver (T-107, closing the DNS-12 find:
// "fail-fast on a dead resolver does not work"). The UDP socket pool
// is connected via connect(2) without packets (T-034) — population
// only catches a bad address/route, but not a dead server: the failure
// shows up on the first production exchange (ECONNREFUSED/timeout →
// temp_error), when domains are already moving down the pipeline. The
// probe closes the gap with one cheap question at dns-worker startup.
//
// The question is the root NS ("."): a recursive resolver always answers
// it (root cache/hints); the external meaning of the question has not
// changed in decades. The liveness criterion is a USABLE answer: NOERROR
// with a non-empty answer section (T-300, the bulk review 04#P3-1). A
// resolver that answers the root NS with SERVFAIL/REFUSED (or an empty
// NODATA) responds on transport but cannot resolve anything — the shape of
// the 24.10 incident (a local unbound SERVFAILing everything put 491K
// domains into temp_error while the service kept running); counting such a
// server "alive" defeats the probe's purpose. Only a transport failure
// counts as an error in the working exchanges (the T-034 contract — an
// RCODE is never an Exchange error); here, at startup, the stricter bar is
// the point. The probe goes to the chain clients directly, bypassing the
// T-035 limiter: the service exchange takes no concurrency slot, spends no
// QPS token, and does not dilute the adaptive window with a bogus success.
//
// Level policy (cmd/dns-worker): a resolver with a usable answer — Info;
// a degraded one (a bad RCODE or an empty answer — the server responds but
// cannot resolve) and a transport-silent one — a Warn for each; nobody
// usable — a startup error: a worker without a single resolving resolver
// turns every episode into temp_error (retried after
// temp_error_retry_after, T-031) — falling at startup (systemd restarts
// with Restart=on-failure) is more visible and cheaper than silently
// poisoning domains with statuses. A DEGRADED PRIMARY is a startup error
// unless the escalation policy covers it (opts.EscalateRCodes, T-300):
// the default switching does not bypass an answering resolver — a
// SERVFAILing local unbound with transport-alive reserves is exactly the
// 24.10 incident composition, the pipeline would dump every episode into
// temp_error while "a part of the chain responds"; the error message
// names the lever (enable dns.fallback.escalate_rcodes or fix the
// resolver). Side effect: isolated environments without a full resolver
// must teach their unbound the root NS locally (the dev testbed does it
// with a transparent local-data record,
// deploy/docker/unbound/unbound.testbed.conf) or accept the startup error.

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/miekg/dns"
)

// probeQuestion is the startup probe question: the root NS (see the package comment).
var probeQuestion = Question{Name: ".", Type: dns.TypeNS}

// Probe performs a single synthetic probe of a resolver: nil means the
// resolver answered the root NS with NOERROR and a non-empty answer
// section (a usable recursive resolver, T-300); an error means either a
// transport failure (after the retries of the lower client, the cause
// wrapped) or an unusable answer (a bad RCODE / an empty NOERROR — the
// server responds but cannot resolve). A ctx cancellation error is
// returned as is (a probe at shutdown does not sentence the resolver).
func Probe(ctx context.Context, r Resolver) error {
	_, err := probeOnce(ctx, r)
	return err
}

// probeOnce is the probe with the outcome kind: degraded=true — the
// server RESPONDS but the answer is unusable (a bad RCODE or an empty
// NOERROR); the transport failures and cancellations come with
// degraded=false. The kind matters to the chain policy: a degraded
// resolver is not bypassed by the default switching (an answer is final),
// a silent one is (T-300).
func probeOnce(ctx context.Context, r Resolver) (degraded bool, err error) {
	if r == nil {
		return false, errors.New("dnsclient: Probe: resolver not set")
	}
	res, err := r.Exchange(ctx, probeQuestion)
	if err != nil {
		return false, err
	}
	if res.RCode != RCodeNOERROR {
		return true, fmt.Errorf("dnsclient: probe: the root NS answer is %s (want NOERROR with records): the resolver responds but cannot resolve", res.RCode)
	}
	if len(res.Answer) == 0 {
		return true, errors.New("dnsclient: probe: the root NS answer is empty NOERROR (want records): the resolver responds but cannot resolve")
	}
	return false, nil
}

// ProbeChain polls the resolver chain with the startup probe (the order
// matches the failover chain: primary first, then backups) and logs the
// result of each: a usable answer — Info, a degraded or silent resolver —
// Warn with the reason. opts is the switching policy of the chain
// (NewFallbackOptions): with EscalateRCodes a degraded primary is
// workable (the reserves are asked over its head), without it — not (the
// default policy switches only on transport failures). It returns an
// error if nobody answered with a usable answer (dead or degraded — a
// startup error of the hosting service, see the package comment), or the
// primary is degraded while the escalation is off (the incident 24.10
// composition), or ctx is cancelled (returned as is). An empty chain is a
// programming error (the config guarantees a primary resolver, spec 5.10).
func ProbeChain(ctx context.Context, targets []FallbackTarget, opts FallbackOptions, log *slog.Logger) error {
	if len(targets) == 0 {
		return errors.New("dnsclient: ProbeChain: resolver chain is empty")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	alive := 0
	primaryDegraded := false
	fails := make([]string, 0, len(targets))
	for i, t := range targets {
		degraded, err := probeOnce(ctx, t.Resolver)
		if err == nil {
			alive++
			log.Info("resolver startup probe: usable root NS answer", "resolver", t.Addr)
			continue
		}
		// A ctx cancellation is not "all dead": the probe aborts with that error.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		log.Warn("resolver startup probe: no usable answer", "resolver", t.Addr, "err", err)
		if i == 0 && degraded {
			primaryDegraded = true
		}
		if t.Addr == "" {
			fails = append(fails, err.Error())
			continue
		}
		fails = append(fails, t.Addr+": "+err.Error())
	}
	if alive == 0 {
		return fmt.Errorf("dnsclient: startup probe: no resolver in the chain answers the root NS with NOERROR records (%d): %s",
			len(targets), strings.Join(fails, "; "))
	}
	if primaryDegraded && !opts.EscalateRCodes {
		return fmt.Errorf("dnsclient: startup probe: the primary resolver responds but cannot resolve the root NS (%s): "+
			"the default switching does not bypass an answering resolver — every episode would become temp_error; "+
			"fix the resolver or enable dns.fallback.escalate_rcodes", fails[0])
	}
	return nil
}

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
// changed in decades. The liveness criterion is any response: an RCODE
// (including SERVFAIL) means the server responds, and that is not a
// probe error; only a transport failure counts as an error (in the
// terms of the T-034 contract — an Exchange error after retries). The
// probe goes to the chain clients directly, bypassing the T-035 limiter:
// the service exchange takes no concurrency slot, spends no QPS token,
// and does not dilute the adaptive window with a bogus success.
//
// Level policy (cmd/dns-worker): part of the chain responds — a Warn for
// each silent one (the pipeline keeps running by switching to the backup,
// but the degradation is visible from the first log line); nobody
// responds — a startup error: a worker without a single resolver turns
// every episode into temp_error (retried after temp_error_retry_after,
// T-031) — falling at startup (systemd restarts with Restart=on-failure)
// is more visible and cheaper than silently poisoning domains with
// statuses. Side effect: isolated environments without outbound traffic
// must keep dns.fallback_resolvers empty (the dev config does just that),
// otherwise the backup probe will honestly report Warn.

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
// resolver responds (any RCODE), an error means a transport failure. A ctx
// cancellation error is returned as is (a probe at shutdown does not
// sentence the resolver).
func Probe(ctx context.Context, r Resolver) error {
	if r == nil {
		return errors.New("dnsclient: Probe: resolver not set")
	}
	_, err := r.Exchange(ctx, probeQuestion)
	return err
}

// ProbeChain polls the resolver chain with the startup probe (the order
// matches the failover chain: primary first, then backups) and logs the
// result of each: success — Info, failure — Warn with the reason. It
// returns an error only if nobody responded (a startup error of the
// hosting service, see the package comment) or ctx is cancelled
// (returned as is). An empty chain is a programming error (the config
// guarantees a primary resolver, spec 5.10).
func ProbeChain(ctx context.Context, targets []FallbackTarget, log *slog.Logger) error {
	if len(targets) == 0 {
		return errors.New("dnsclient: ProbeChain: resolver chain is empty")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	alive := 0
	for _, t := range targets {
		err := Probe(ctx, t.Resolver)
		if err == nil {
			alive++
			log.Info("resolver startup probe: responds", "resolver", t.Addr)
			continue
		}
		// A ctx cancellation is not "all dead": the probe aborts with that error.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		log.Warn("resolver startup probe: no response", "resolver", t.Addr, "err", err)
	}
	if alive == 0 {
		addrs := make([]string, len(targets))
		for i, t := range targets {
			addrs[i] = t.Addr
		}
		return fmt.Errorf("dnsclient: startup probe: no resolver in the chain responds (%d): %s",
			len(targets), strings.Join(addrs, ", "))
	}
	return nil
}

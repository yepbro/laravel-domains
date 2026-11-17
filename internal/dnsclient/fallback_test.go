// Tests of switching to backup resolvers (T-107; the escalation policy and
// the budget — T-300). No network: chain elements are Resolver fakes with
// call-order recording and a programmable outcome. Checked: the switch
// condition (only a transport failure by default; SERVFAIL/REFUSED of the
// primary — with dns.fallback.escalate_rcodes), the chain order, the Stats
// counters, the escalation budget (the avalanche guard), the aggregated
// exhaustion error, ctx cancellation, and concurrency (the -race run catches races).

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/config"
)

// --- helper fakes ----------------------------------------------------------

// fbCalls is a shared record of the chain call order (thread-safe:
// the concurrent test runs Exchange from goroutines).
type fbCalls struct {
	mu    sync.Mutex
	order []string
}

func (c *fbCalls) add(name string) {
	c.mu.Lock()
	c.order = append(c.order, name)
	c.mu.Unlock()
}

func (c *fbCalls) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.order...)
}

// fbFake is a chain element: it counts calls and returns the given
// outcome; fn (when set) fully replaces the behavior (ctx cancellation, pauses).
type fbFake struct {
	name  string
	calls *fbCalls
	res   *Result
	err   error
	fn    func(ctx context.Context, q Question) (*Result, error)
}

func (f *fbFake) Exchange(ctx context.Context, q Question) (*Result, error) {
	if f.calls != nil { // fakes outside the chain (probes) are not recorded
		f.calls.add(f.name)
	}
	if f.fn != nil {
		return f.fn(ctx, q)
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.res != nil {
		return f.res, nil
	}
	return &Result{RCode: RCodeNOERROR, Answer: []dns.RR{aRR(dns.Fqdn(q.Name), "192.0.2.1")}}, nil
}

// newFallback is a chain of fakes with a shared call recorder (the default
// switching policy — the behavior before T-300).
func newFallback(t *testing.T, fakes ...*fbFake) (*FallbackResolver, *fbCalls) {
	return newFallbackOpts(t, FallbackOptions{}, fakes...)
}

// newFallbackOpts is a chain of fakes with the given switching policy
// (dns.fallback.* of the config, T-300).
func newFallbackOpts(t *testing.T, opts FallbackOptions, fakes ...*fbFake) (*FallbackResolver, *fbCalls) {
	t.Helper()
	if len(fakes) == 0 {
		t.Fatal("newFallback: at least a primary target is needed")
	}
	calls := &fbCalls{}
	targets := make([]FallbackTarget, len(fakes))
	for i, f := range fakes {
		f.name = fmt.Sprintf("10.0.0.%d:53", i+1)
		f.calls = calls
		targets[i] = FallbackTarget{Addr: f.name, Resolver: f}
	}
	r, err := NewFallbackResolver(targets[0], opts, targets[1:]...)
	if err != nil {
		t.Fatalf("NewFallbackResolver: %v", err)
	}
	return r, calls
}

// --- switch condition ------------------------------------------------------

// TestFallbackPrimaryAnswers — the primary answered: the backups are not
// polled, the outcome is the primary's result, the counters stay silent.
func TestFallbackPrimaryAnswers(t *testing.T) {
	primary := &fbFake{res: &Result{RCode: RCodeNOERROR, Answer: []dns.RR{aRR("example.com.", "192.0.2.7")}}}
	r, calls := newFallback(t, primary, &fbFake{}, &fbFake{})

	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if got := res.RecordsOf(dns.TypeA); len(got) != 1 || got[0].(*dns.A).A.String() != "192.0.2.7" {
		t.Fatalf("the result is not from the primary resolver: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 1 {
		t.Fatalf("the fallbacks must not be polled, calls: %v", order)
	}
	if st := r.Stats(); st.Switches != 0 || st.Exhausted != 0 {
		t.Fatalf("the counters with a live primary: %+v", st)
	}
}

// TestFallbackRCODEIsNotASwitch — by default (dns.fallback.escalate_rcodes
// off, T-300) an RCODE (including SERVFAIL/NXDOMAIN) is a server answer, not
// a transport failure: no switching, the response classification semantics
// (T-036/T-039) do not change. The "SERVFAIL/REFUSED of the primary" counters
// grow in this mode too — the incident metric works without the escalation
// enabling (04#P2-1).
func TestFallbackRCODEIsNotASwitch(t *testing.T) {
	for _, rcode := range []RCode{RCodeSERVFAIL, RCodeNXDOMAIN, RCodeREFUSED} {
		t.Run(rcode.String(), func(t *testing.T) {
			primary := &fbFake{res: &Result{RCode: rcode}}
			r, calls := newFallback(t, primary, &fbFake{})
			res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
			if err != nil {
				t.Fatalf("an RCODE is not an error: %v", err)
			}
			if res.RCode != rcode {
				t.Fatalf("the result rcode %v, want %v without a switch", res.RCode, rcode)
			}
			if order := calls.snapshot(); len(order) != 1 {
				t.Fatalf("an RCODE is not a reason to poll the fallbacks, calls: %v", order)
			}
			st := r.Stats()
			if st.Switches != 0 || st.Exhausted != 0 || st.Escalated != 0 {
				t.Fatalf("the degradation counters on an RCODE answer: %+v", st)
			}
			wantServFail, wantRefused := int64(0), int64(0)
			switch rcode {
			case RCodeSERVFAIL:
				wantServFail = 1
			case RCodeREFUSED:
				wantRefused = 1
			}
			if st.PrimaryServFail != wantServFail || st.PrimaryRefused != wantRefused {
				t.Fatalf("the primary rcode counters: ServFail=%d Refused=%d, want %d/%d (the metric in both modes)",
					st.PrimaryServFail, st.PrimaryRefused, wantServFail, wantRefused)
			}
		})
	}
}

// TestFallbackSwitchOnPrimaryFailure — the primary ran out of attempts on
// a network error: the question goes to the backup, the outcome is the
// backup's result (any RCODE), Switches grows, Elapsed is the whole
// exchange time (including the failed one's loop).
func TestFallbackSwitchOnPrimaryFailure(t *testing.T) {
	primary := &fbFake{fn: func(ctx context.Context, q Question) (*Result, error) {
		time.Sleep(25 * time.Millisecond) // the dead primary's retry loop
		return nil, fakeNetErr{timeout: true}
	}}
	reserve := &fbFake{res: &Result{RCode: RCodeNXDOMAIN, Ns: []dns.RR{nsRR("example.com.", "ns1.example.com.")}}}
	r, calls := newFallback(t, primary, reserve)

	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("the fallback must answer: %v", err)
	}
	if res.RCode != RCodeNXDOMAIN || len(res.Ns) != 1 {
		t.Fatalf("the result is not from the fallback: %+v", res)
	}
	if min := 25 * time.Millisecond; res.Elapsed < min {
		t.Fatalf("Elapsed %s does not include the failed primary's cycle (< %s)", res.Elapsed, min)
	}
	if order := calls.snapshot(); len(order) != 2 || order[0] != "10.0.0.1:53" || order[1] != "10.0.0.2:53" {
		t.Fatalf("the chain order: %v", order)
	}
	if st := r.Stats(); st.Switches != 1 || st.Exhausted != 0 {
		t.Fatalf("the counters after the switch: %+v", st)
	}
	// The time paid to the failed primary is observable (04#P2-2): without
	// it a working reserve hides the degradation from the latency metrics.
	if st := r.Stats(); time.Duration(st.PrimaryFailNanos) < 20*time.Millisecond {
		t.Fatalf("PrimaryFailNanos %s does not include the failed primary's cycle", time.Duration(st.PrimaryFailNanos))
	}
}

// TestFallbackSecondReserveAnswers — the backups are polled in order
// until the first answer.
func TestFallbackSecondReserveAnswers(t *testing.T) {
	r, calls := newFallback(t,
		&fbFake{err: fakeNetErr{timeout: true}},
		&fbFake{err: errors.New("connection refused")},
		&fbFake{},
	)

	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RCode != RCodeNOERROR {
		t.Fatalf("the result is not from the second fallback: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 3 {
		t.Fatalf("all must be polled until the first answer, calls: %v", order)
	}
	if st := r.Stats(); st.Switches != 1 || st.Exhausted != 0 {
		t.Fatalf("the counters: %+v", st)
	}
}

// --- chain exhaustion ------------------------------------------------------

// TestFallbackAllDead — nobody answered: the error lists all chain
// addresses, wraps the last one's cause (errors.Is, IsTimeout) and bumps
// Exhausted — each such exchange becomes a consumer temp_error.
func TestFallbackAllDead(t *testing.T) {
	first := errors.New("wire cut")
	last := fmt.Errorf("udp: %w", fakeNetErr{timeout: true})
	r, _ := newFallback(t,
		&fbFake{err: first},
		&fbFake{err: errors.New("no route")},
		&fbFake{err: last},
	)

	_, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err == nil {
		t.Fatal("chain exhaustion — an error")
	}
	for _, want := range []string{"10.0.0.1:53", "10.0.0.2:53", "10.0.0.3:53", "no resolver in the chain responded", "example.com"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the message without %q: %v", want, err)
		}
	}
	if !errors.Is(err, last) || errors.Is(err, first) {
		t.Fatalf("the error must wrap the last resolver's cause and only it: %v", err)
	}
	if !IsTimeout(err) {
		t.Fatalf("the last resolver's timeout classification is lost: %v", err)
	}
	if st := r.Stats(); st.Exhausted != 1 || st.Switches != 0 {
		t.Fatalf("the counters after exhaustion: %+v", st)
	}
}

// TestFallbackNoReserves — a chain with the primary only (an empty
// dns.fallback_resolvers): it degrades into an ordinary resolver.
func TestFallbackNoReserves(t *testing.T) {
	r, _ := newFallback(t, &fbFake{err: errors.New("down")})
	if _, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA}); err == nil {
		t.Fatal("without fallbacks a primary failure is an error")
	}
	if st := r.Stats(); st.Exhausted != 1 || st.Switches != 0 {
		t.Fatalf("the counters of a chain without fallbacks: %+v", st)
	}
}

// --- ctx cancellation ------------------------------------------------------

// TestFallbackCancelledNoReserves — a ctx cancellation (shutdown) is not
// a resolver refusal: the backups are not polled, the cancellation error
// is returned, the counters stay clean (not counted as degradation).
func TestFallbackCancelledNoReserves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	primary := &fbFake{fn: func(ctx context.Context, q Question) (*Result, error) {
		cancel() // Client returns the raw ctx error on cancellation
		return nil, ctx.Err()
	}}
	r, calls := newFallback(t, primary, &fbFake{})

	_, err := r.Exchange(ctx, Question{Name: "example.com", Type: dns.TypeA})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if order := calls.snapshot(); len(order) != 1 {
		t.Fatalf("cancellation must not push the question through the fallbacks, calls: %v", order)
	}
	if st := r.Stats(); st.Switches != 0 || st.Exhausted != 0 {
		t.Fatalf("the counters on cancellation: %+v", st)
	}
}

// TestFallbackCancelledMidChain — the cancellation arrived at a backup:
// the chain aborts at once, without polling the rest.
func TestFallbackCancelledMidChain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r, calls := newFallback(t,
		&fbFake{err: fakeNetErr{timeout: true}},
		&fbFake{fn: func(ctx context.Context, q Question) (*Result, error) {
			cancel()
			return nil, ctx.Err()
		}},
		&fbFake{},
	)

	_, err := r.Exchange(ctx, Question{Name: "example.com", Type: dns.TypeA})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if order := calls.snapshot(); len(order) != 2 {
		t.Fatalf("the chain must break on cancellation, calls: %v", order)
	}
}

// --- assembly and concurrency ----------------------------------------------

// TestNewFallbackResolverValidation — nil chain elements are a
// programming error of the assembly.
func TestNewFallbackResolverValidation(t *testing.T) {
	if _, err := NewFallbackResolver(FallbackTarget{Addr: "a"}, FallbackOptions{}); err == nil {
		t.Fatal("a nil primary: want an error")
	}
	if _, err := NewFallbackResolver(
		FallbackTarget{Addr: "a", Resolver: &fbFake{}},
		FallbackOptions{},
		FallbackTarget{Addr: "b"},
	); err == nil {
		t.Fatal("a nil fallback: want an error")
	}
}

// TestFallbackConcurrentStats — concurrent exchanges through one chain
// (the primary fails, the backup answers) give consistent counters; the
// call order is not deterministic — the sums matter (the -race run catches races).
func TestFallbackConcurrentStats(t *testing.T) {
	r, calls := newFallback(t,
		&fbFake{err: fakeNetErr{timeout: true}},
		&fbFake{},
	)
	const total = 40
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := r.Exchange(context.Background(), Question{Name: fmt.Sprintf("h%d.example.com", i), Type: dns.TypeA}); err != nil {
				t.Errorf("Exchange(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if st := r.Stats(); st.Switches != int64(total) || st.Exhausted != 0 {
		t.Fatalf("the counters: Switches=%d Exhausted=%d, want %d/0", st.Switches, st.Exhausted, total)
	}
	if n := len(calls.snapshot()); n != total*2 {
		t.Fatalf("calls %d, want %d (the primary + a fallback per exchange)", n, total*2)
	}
}

// --- RCODE escalation (dns.fallback.escalate_rcodes, T-300 / 04#P2-1) -------

// TestFallbackEscalateOnBadRCode — with the escalation on, a SERVFAIL/REFUSED
// answer of the primary is not final: the question goes to the reserves, the
// first useful answer (any RCODE except SERVFAIL/REFUSED) wins — the policy
// of the manual check chain (internal/checker/resolve.go), now opt-in for
// the mass pipeline.
func TestFallbackEscalateOnBadRCode(t *testing.T) {
	for _, rcode := range []RCode{RCodeSERVFAIL, RCodeREFUSED} {
		t.Run(rcode.String(), func(t *testing.T) {
			primary := &fbFake{res: &Result{RCode: rcode}}
			reserve := &fbFake{res: &Result{RCode: RCodeNOERROR, Answer: []dns.RR{aRR("example.com.", "192.0.2.9")}}}
			r, calls := newFallbackOpts(t, FallbackOptions{EscalateRCodes: true, EscalateQPS: 10}, primary, reserve)

			res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
			if err != nil {
				t.Fatalf("the escalation must answer: %v", err)
			}
			if got := res.RecordsOf(dns.TypeA); len(got) != 1 || got[0].(*dns.A).A.String() != "192.0.2.9" {
				t.Fatalf("the result is not from the reserve: %+v", res)
			}
			if order := calls.snapshot(); len(order) != 2 || order[0] != "10.0.0.1:53" || order[1] != "10.0.0.2:53" {
				t.Fatalf("the chain order: %v", order)
			}
			st := r.Stats()
			if st.Switches != 1 || st.Escalated != 1 || st.Exhausted != 0 {
				t.Fatalf("the counters after the escalation: %+v", st)
			}
			if rcode == RCodeSERVFAIL && st.PrimaryServFail != 1 || rcode == RCodeREFUSED && st.PrimaryRefused != 1 {
				t.Fatalf("the primary rcode counter after the escalation: %+v", st)
			}
		})
	}
}

// TestFallbackEscalateWalkUntilUseful — a reserve answering SERVFAIL too is
// not a useful outcome: the walk continues to the next one (the checker
// escalateResolver semantics).
func TestFallbackEscalateWalkUntilUseful(t *testing.T) {
	r, calls := newFallbackOpts(t, FallbackOptions{EscalateRCodes: true, EscalateQPS: 10},
		&fbFake{res: &Result{RCode: RCodeSERVFAIL}},
		&fbFake{res: &Result{RCode: RCodeSERVFAIL}},
		&fbFake{res: &Result{RCode: RCodeNXDOMAIN, Ns: []dns.RR{nsRR("example.com.", "ns1.example.com.")}}},
	)

	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RCode != RCodeNXDOMAIN || len(res.Ns) != 1 {
		t.Fatalf("the result is not the useful answer of the last reserve: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 3 {
		t.Fatalf("the walk must reach the third reserve, calls: %v", order)
	}
	// Two reserve queries — two budget tokens: Escalated counts the
	// requests to the reserves, not the escalations (the ceiling of
	// EscalateQPS applies to the queries, T-300 review P2-2).
	if st := r.Stats(); st.Switches != 1 || st.Escalated != 2 || st.Exhausted != 0 {
		t.Fatalf("the counters: %+v", st)
	}
}

// TestFallbackEscalateAbandonedReturnsPrimaryAnswer — nobody in the chain
// gave a useful answer: the primary's own answer is the honest outcome (its
// SERVFAIL → temp_error is the consumer's decision); the chain is NOT
// exhausted (the primary answered — Exhausted counts only transport deaths
// of the whole chain).
func TestFallbackEscalateAbandonedReturnsPrimaryAnswer(t *testing.T) {
	r, calls := newFallbackOpts(t, FallbackOptions{EscalateRCodes: true, EscalateQPS: 10},
		&fbFake{res: &Result{RCode: RCodeSERVFAIL}},
		&fbFake{err: fakeNetErr{timeout: true}},
	)

	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("an abandoned escalation returns the primary's answer, not an error: %v", err)
	}
	if res.RCode != RCodeSERVFAIL {
		t.Fatalf("the result rcode %v, want the primary's SERVFAIL", res.RCode)
	}
	if order := calls.snapshot(); len(order) != 2 {
		t.Fatalf("the reserve must be polled once, calls: %v", order)
	}
	st := r.Stats()
	if st.Switches != 0 || st.Exhausted != 0 || st.Escalated != 1 || st.PrimaryServFail != 1 {
		t.Fatalf("the counters of the abandoned escalation: %+v", st)
	}
}

// TestFallbackEscalateBudgetExhausted — the avalanche guard (04#P2-1): above
// the EscalateQPS budget the primary's answer stays the result — the mass
// crawl must not unload its full dns.max_qps onto the public reserves in a
// SERVFAIL storm. No waiting for a token: the exchange does not hold the
// limiter slot any longer.
func TestFallbackEscalateBudgetExhausted(t *testing.T) {
	primary := &fbFake{res: &Result{RCode: RCodeSERVFAIL}}
	reserve := &fbFake{}
	// 0.001/s: the burst is one token, the refill of the next one takes
	// 1000 s — the second exchange cannot accidentally get a token even
	// under a stalled CI runner.
	r, calls := newFallbackOpts(t, FallbackOptions{EscalateRCodes: true, EscalateQPS: 0.001}, primary, reserve)

	// The burst of the bucket is one exchange: the first escalation passes.
	if res, err := r.Exchange(context.Background(), Question{Name: "a.example.com", Type: dns.TypeA}); err != nil || res.RCode != RCodeNOERROR {
		t.Fatalf("the first escalation must pass: %v %+v", err, res)
	}
	// The token has not accrued yet (≪ 1 s): no escalation, no reserve call —
	// the second exchange polls only the primary.
	res, err := r.Exchange(context.Background(), Question{Name: "b.example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RCode != RCodeSERVFAIL {
		t.Fatalf("above the budget the primary's answer is final: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 3 || order[2] != "10.0.0.1:53" {
		t.Fatalf("the reserve must not be polled above the budget, calls: %v", order)
	}
	st := r.Stats()
	if st.Switches != 1 || st.Escalated != 1 || st.PrimaryServFail != 2 {
		t.Fatalf("the counters: %+v", st)
	}
}

// TestFallbackEscalateDefaultBudget — a zero EscalateQPS with the escalation
// on is the package default (DefaultEscalateQPS), not "no budget": the guard
// cannot be disabled by a zero.
func TestFallbackEscalateDefaultBudget(t *testing.T) {
	r, calls := newFallbackOpts(t, FallbackOptions{EscalateRCodes: true},
		&fbFake{res: &Result{RCode: RCodeSERVFAIL}},
		&fbFake{},
	)
	if res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA}); err != nil || res.RCode != RCodeNOERROR {
		t.Fatalf("the escalation with the default budget: %v %+v", err, res)
	}
	if order := calls.snapshot(); len(order) != 2 {
		t.Fatalf("calls: %v", order)
	}
}

// TestFallbackEscalateWithoutReservesForcedOff — escalation with an empty
// reserve list is forced off in the constructor (T-300 review P3-2): the
// walk of an empty chain would burn budget tokens and grow Escalated
// without a single query — the counters must not lie. The config
// validation rejects the combination; this is the package guard.
func TestFallbackEscalateWithoutReservesForcedOff(t *testing.T) {
	r, calls := newFallbackOpts(t, FallbackOptions{EscalateRCodes: true, EscalateQPS: 10},
		&fbFake{res: &Result{RCode: RCodeSERVFAIL}},
	)
	res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RCode != RCodeSERVFAIL {
		t.Fatalf("the primary's answer stays the result: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 1 {
		t.Fatalf("calls: %v", order)
	}
	if st := r.Stats(); st.Escalated != 0 || st.Switches != 0 || st.PrimaryServFail != 1 {
		t.Fatalf("the counters must not grow without reserves: %+v", st)
	}
}

// TestFallbackEscalateNotForUsefulRCodes — NXDOMAIN and NOERROR of the
// primary are never escalated (even with the option on): "no such name" and
// a normal answer are final classifications, not degradation.
func TestFallbackEscalateNotForUsefulRCodes(t *testing.T) {
	for _, rcode := range []RCode{RCodeNXDOMAIN, RCodeNOERROR} {
		t.Run(rcode.String(), func(t *testing.T) {
			r, calls := newFallbackOpts(t, FallbackOptions{EscalateRCodes: true, EscalateQPS: 10},
				&fbFake{res: &Result{RCode: rcode}},
				&fbFake{},
			)
			res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
			if err != nil {
				t.Fatalf("Exchange: %v", err)
			}
			if res.RCode != rcode {
				t.Fatalf("the primary's answer %v must stay final, got %v", rcode, res.RCode)
			}
			if order := calls.snapshot(); len(order) != 1 {
				t.Fatalf("a useful RCODE is not a reason to poll the reserves, calls: %v", order)
			}
			if st := r.Stats(); st.Escalated != 0 || st.Switches != 0 || st.PrimaryServFail != 0 || st.PrimaryRefused != 0 {
				t.Fatalf("the counters on a useful RCODE: %+v", st)
			}
		})
	}
}

// TestEscalateBudget — the token bucket of the guard itself: a lazy refill
// by a fake clock, a burst passes, above the burst tryTake fails without
// waiting, the accrued tokens come back after a pause.
func TestEscalateBudget(t *testing.T) {
	now := time.Now()
	b := newEscalateBudget(2, func() time.Time { return now }) // 2/s, burst 2
	if first, second := b.tryTake(), b.tryTake(); !first || !second {
		t.Fatal("the initial burst must pass")
	}
	if b.tryTake() {
		t.Fatal("above the burst tryTake must fail without waiting")
	}
	now = now.Add(time.Second)
	if first, second := b.tryTake(), b.tryTake(); !first || !second {
		t.Fatal("a second later the budget is two tokens again")
	}
	if b.tryTake() {
		t.Fatal("exactly two tokens per second, not more")
	}
}

// TestNewFallbackOptionsFromConfig — the dns.fallback subsection maps onto
// the switching policy (T-300): the raw values only, the defaults are
// applied by NewFallbackResolver (the client Options style).
func TestNewFallbackOptionsFromConfig(t *testing.T) {
	cfg := config.DNS{
		Resolvers:         []string{"10.0.0.1:53"},
		FallbackResolvers: []string{"1.1.1.1:53"},
		Fallback:          config.DNSFallback{EscalateRCodes: true, EscalateQPS: 42.5, FastSwitch: true},
	}
	if got := NewFallbackOptions(cfg); got != (FallbackOptions{EscalateRCodes: true, EscalateQPS: 42.5}) {
		t.Fatalf("NewFallbackOptions: %+v", got)
	}
	if got := NewFallbackOptions(config.DNS{}); got != (FallbackOptions{}) {
		t.Fatalf("the zero config is the zero policy (the behavior before T-300): %+v", got)
	}
}

// Tests of switching to backup resolvers (T-107). No network: chain
// elements are Resolver fakes with call-order recording and a
// programmable outcome. Checked: the switch condition (only a transport
// failure, not an RCODE), the chain order, the Stats counters, the
// aggregated exhaustion error, ctx cancellation, and concurrency (the -race run catches races).

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

// newFallback is a chain of fakes with a shared call recorder.
func newFallback(t *testing.T, fakes ...*fbFake) (*FallbackResolver, *fbCalls) {
	t.Helper()
	if len(fakes) == 0 {
		t.Fatal("newFallback: нужна хотя бы основная цель")
	}
	calls := &fbCalls{}
	targets := make([]FallbackTarget, len(fakes))
	for i, f := range fakes {
		f.name = fmt.Sprintf("10.0.0.%d:53", i+1)
		f.calls = calls
		targets[i] = FallbackTarget{Addr: f.name, Resolver: f}
	}
	r, err := NewFallbackResolver(targets[0], targets[1:]...)
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
		t.Fatalf("итог — не результат основного резолвера: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 1 {
		t.Fatalf("резервы не должны опрашиваться, вызовы: %v", order)
	}
	if st := r.Stats(); st.Switches != 0 || st.Exhausted != 0 {
		t.Fatalf("счётчики при живом основном: %+v", st)
	}
}

// TestFallbackRCODEIsNotASwitch — an RCODE (including SERVFAIL/NXDOMAIN) is
// a server answer, not a transport failure: no switching, the response
// classification semantics (T-036/T-039) do not change.
func TestFallbackRCODEIsNotASwitch(t *testing.T) {
	for _, rcode := range []RCode{RCodeSERVFAIL, RCodeNXDOMAIN, RCodeREFUSED} {
		t.Run(rcode.String(), func(t *testing.T) {
			primary := &fbFake{res: &Result{RCode: rcode}}
			r, calls := newFallback(t, primary, &fbFake{})
			res, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
			if err != nil {
				t.Fatalf("RCODE — не ошибка: %v", err)
			}
			if res.RCode != rcode {
				t.Fatalf("rcode итога %v, ожидался %v без переключения", res.RCode, rcode)
			}
			if order := calls.snapshot(); len(order) != 1 {
				t.Fatalf("RCODE не повод опрашивать резервы, вызовы: %v", order)
			}
			if st := r.Stats(); st.Switches != 0 || st.Exhausted != 0 {
				t.Fatalf("счётчики при RCODE-ответе: %+v", st)
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
		t.Fatalf("резерв обязан ответить: %v", err)
	}
	if res.RCode != RCodeNXDOMAIN || len(res.Ns) != 1 {
		t.Fatalf("итог — не результат резерва: %+v", res)
	}
	if min := 25 * time.Millisecond; res.Elapsed < min {
		t.Fatalf("Elapsed %s не включает цикл отказавшего основного (< %s)", res.Elapsed, min)
	}
	if order := calls.snapshot(); len(order) != 2 || order[0] != "10.0.0.1:53" || order[1] != "10.0.0.2:53" {
		t.Fatalf("порядок цепи: %v", order)
	}
	if st := r.Stats(); st.Switches != 1 || st.Exhausted != 0 {
		t.Fatalf("счётчики после переключения: %+v", st)
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
		t.Fatalf("итог не от второго резерва: %+v", res)
	}
	if order := calls.snapshot(); len(order) != 3 {
		t.Fatalf("должны быть опрошены все до первого ответа, вызовы: %v", order)
	}
	if st := r.Stats(); st.Switches != 1 || st.Exhausted != 0 {
		t.Fatalf("счётчики: %+v", st)
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
		t.Fatal("исчерпание цепи — ошибка")
	}
	for _, want := range []string{"10.0.0.1:53", "10.0.0.2:53", "10.0.0.3:53", "no resolver in the chain responded", "example.com"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("сообщение без %q: %v", want, err)
		}
	}
	if !errors.Is(err, last) || errors.Is(err, first) {
		t.Fatalf("ошибка обязана оборачивать причину последнего резолвера и только её: %v", err)
	}
	if !IsTimeout(err) {
		t.Fatalf("классификация таймаута последнего резолвера потеряна: %v", err)
	}
	if st := r.Stats(); st.Exhausted != 1 || st.Switches != 0 {
		t.Fatalf("счётчики после исчерпания: %+v", st)
	}
}

// TestFallbackNoReserves — a chain with the primary only (an empty
// dns.fallback_resolvers): it degrades into an ordinary resolver.
func TestFallbackNoReserves(t *testing.T) {
	r, _ := newFallback(t, &fbFake{err: errors.New("down")})
	if _, err := r.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA}); err == nil {
		t.Fatal("без резервов отказ основного — ошибка")
	}
	if st := r.Stats(); st.Exhausted != 1 || st.Switches != 0 {
		t.Fatalf("счётчики цепи без резервов: %+v", st)
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
		t.Fatalf("ожидается context.Canceled, получено %v", err)
	}
	if order := calls.snapshot(); len(order) != 1 {
		t.Fatalf("отмена не должна гнать вопрос по резервам, вызовы: %v", order)
	}
	if st := r.Stats(); st.Switches != 0 || st.Exhausted != 0 {
		t.Fatalf("счётчики при отмене: %+v", st)
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
		t.Fatalf("ожидается context.Canceled, получено %v", err)
	}
	if order := calls.snapshot(); len(order) != 2 {
		t.Fatalf("цепь обязана прерваться на отмене, вызовы: %v", order)
	}
}

// --- assembly and concurrency ----------------------------------------------

// TestNewFallbackResolverValidation — nil chain elements are a
// programming error of the assembly.
func TestNewFallbackResolverValidation(t *testing.T) {
	if _, err := NewFallbackResolver(FallbackTarget{Addr: "a"}); err == nil {
		t.Fatal("nil-основной: ожидается ошибка")
	}
	if _, err := NewFallbackResolver(
		FallbackTarget{Addr: "a", Resolver: &fbFake{}},
		FallbackTarget{Addr: "b"},
	); err == nil {
		t.Fatal("nil-резерв: ожидается ошибка")
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
		t.Fatalf("счётчики: Switches=%d Exhausted=%d, ожидалось %d/0", st.Switches, st.Exhausted, total)
	}
	if n := len(calls.snapshot()); n != total*2 {
		t.Fatalf("вызовов %d, ожидалось %d (основной + резерв на каждый обмен)", n, total*2)
	}
}

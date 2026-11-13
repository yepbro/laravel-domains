// Tests of the live check resolver chain (see resolve.go): no network —
// the chain elements, like in fallback_test.go (dnsclient), are fake
// Resolvers with a programmable outcome and call counting. Checked: the
// escalation condition (SERVFAIL/REFUSED and a transport failure — yes;
// NOERROR and NXDOMAIN — no), the chain order, the "nobody answered
// usefully" outcome (the last RCODE or the aggregated error), the ctx
// cancellation and concurrency (-race catches the races).

package checker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
)

// resFake — a chain element: returns the given RCODE/error; calls
// (if set) counts the calls by name.
type resFake struct {
	name  string
	calls *sync.Map
	rcode dnsclient.RCode
	err   error
}

func (f *resFake) Exchange(context.Context, dnsclient.Question) (*dnsclient.Result, error) {
	if f.calls != nil {
		if v, ok := f.calls.Load(f.name); ok {
			p := v.(*int)
			*p++
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &dnsclient.Result{RCode: f.rcode}, nil
}

func newCalls(names ...string) *sync.Map {
	m := &sync.Map{}
	for _, n := range names {
		zero := 0
		m.Store(n, &zero)
	}
	return m
}

func called(m *sync.Map, name string) int {
	v, ok := m.Load(name)
	if !ok {
		return -1
	}
	return *v.(*int)
}

func chainOf(targets ...escalateTarget) *escalateResolver {
	return &escalateResolver{targets: targets}
}

func q() dnsclient.Question {
	return dnsclient.Question{Name: "example.com.", Type: dns.TypeA}
}

// TestEscalateUsefulFirstAnswer — a useful outcome of the first element
// is returned as is, the reserves are not polled: NOERROR (including an
// empty NODATA) and NXDOMAIN (no such name — a legitimate answer, not a
// server failure).
func TestEscalateUsefulFirstAnswer(t *testing.T) {
	cases := []struct {
		name  string
		rcode dnsclient.RCode
	}{
		{"NOERROR", dnsclient.RCodeNOERROR},
		{"NXDOMAIN", dnsclient.RCodeNXDOMAIN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := newCalls("p", "f")
			r := chainOf(
				escalateTarget{addr: "p", res: &resFake{name: "p", calls: calls, rcode: tc.rcode}},
				escalateTarget{addr: "f", res: &resFake{name: "f", calls: calls, rcode: dnsclient.RCodeNOERROR}},
			)
			res, err := r.Exchange(context.Background(), q())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.RCode != tc.rcode {
				t.Fatalf("result = %s, want %s", res.RCode, tc.rcode)
			}
			if n := called(calls, "f"); n != 0 {
				t.Fatalf("the fallback was polled (%d times) with a useful result from the primary", n)
			}
		})
	}
}

// TestEscalateOnRCODEFailure — SERVFAIL and REFUSED of the primary are a
// server failure: the chain moves to the reserve, its useful outcome is
// returned.
func TestEscalateOnRCODEFailure(t *testing.T) {
	cases := []struct {
		name     string
		primary  dnsclient.RCode
		expected dnsclient.RCode
	}{
		{"SERVFAIL→NOERROR", dnsclient.RCodeSERVFAIL, dnsclient.RCodeNOERROR},
		{"REFUSED→NXDOMAIN", dnsclient.RCodeREFUSED, dnsclient.RCodeNXDOMAIN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := newCalls("p", "f")
			r := chainOf(
				escalateTarget{addr: "p", res: &resFake{name: "p", calls: calls, rcode: tc.primary}},
				escalateTarget{addr: "f", res: &resFake{name: "f", calls: calls, rcode: tc.expected}},
			)
			res, err := r.Exchange(context.Background(), q())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.RCode != tc.expected {
				t.Fatalf("result = %s, want %s", res.RCode, tc.expected)
			}
			if n := called(calls, "f"); n != 1 {
				t.Fatalf("the fallback was polled %d times, want 1", n)
			}
		})
	}
}

// TestEscalateOnTransportError — a transport failure of the primary (an
// error after the retries) — the same switching as FallbackResolver
// T-107.
func TestEscalateOnTransportError(t *testing.T) {
	calls := newCalls("p", "f")
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", calls: calls, err: errors.New("i/o timeout")}},
		escalateTarget{addr: "f", res: &resFake{name: "f", calls: calls, rcode: dnsclient.RCodeNOERROR}},
	)
	res, err := r.Exchange(context.Background(), q())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RCode != dnsclient.RCodeNOERROR {
		t.Fatalf("result = %s, want NOERROR", res.RCode)
	}
	if n := called(calls, "f"); n != 1 {
		t.Fatalf("the fallback was polled %d times, want 1", n)
	}
}

// TestEscalateAllFailedRCODE — all the elements answered with an RCODE
// failure: without an error the last RCODE answer is returned (the
// consumer honestly classifies SERVFAIL → temp_error).
func TestEscalateAllFailedRCODE(t *testing.T) {
	calls := newCalls("p", "f1", "f2")
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", calls: calls, rcode: dnsclient.RCodeSERVFAIL}},
		escalateTarget{addr: "f1", res: &resFake{name: "f1", calls: calls, rcode: dnsclient.RCodeSERVFAIL}},
		escalateTarget{addr: "f2", res: &resFake{name: "f2", calls: calls, rcode: dnsclient.RCodeREFUSED}},
	)
	res, err := r.Exchange(context.Background(), q())
	if err != nil {
		t.Fatalf("an RCODE refusal is not a chain error: %v", err)
	}
	if res.RCode != dnsclient.RCodeREFUSED {
		t.Fatalf("result = %s, want the last REFUSED", res.RCode)
	}
	for _, name := range []string{"p", "f1", "f2"} {
		if n := called(calls, name); n != 1 {
			t.Fatalf("%s was polled %d times, want 1", name, n)
		}
	}
}

// TestEscalateAllTransportFailed — solid transport failures: the error
// lists the whole chain and wraps the last cause.
func TestEscalateAllTransportFailed(t *testing.T) {
	boom := errors.New("connection refused")
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", err: errors.New("i/o timeout")}},
		escalateTarget{addr: "f", res: &resFake{name: "f", err: boom}},
	)
	_, err := r.Exchange(context.Background(), q())
	if err == nil {
		t.Fatal("want a chain exhaustion error")
	}
	for _, sub := range []string{"p:", "f:", "connection refused", "i/o timeout"} {
		if !strings.Contains(err.Error(), sub) {
			t.Fatalf("the message does not contain %q: %v", sub, err)
		}
	}
	if !errors.Is(err, boom) {
		t.Fatal("errors.Is does not find the last resolver's cause")
	}
}

// TestEscalateContextCancelled — a ctx cancellation between the chain
// elements interrupts it right away (shutdown does not shoot the
// reserves).
func TestEscalateContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", err: errors.New("i/o timeout")}},
		escalateTarget{addr: "f", res: &resFake{name: "f", rcode: dnsclient.RCodeNOERROR}},
	)
	_, err := r.Exchange(ctx, q())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want a cancellation error, got: %v", err)
	}
}

// TestEscalateConcurrent — the chain is stateless: parallel Exchanges do
// not interfere with each other (-race catches the races).
func TestEscalateConcurrent(t *testing.T) {
	r := chainOf(
		escalateTarget{addr: "p", res: &resFake{name: "p", rcode: dnsclient.RCodeSERVFAIL}},
		escalateTarget{addr: "f", res: &resFake{name: "f", rcode: dnsclient.RCodeNOERROR}},
	)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := r.Exchange(context.Background(), q())
			if err != nil {
				t.Errorf("parallel exchange: %v", err)
				return
			}
			if res.RCode != dnsclient.RCodeNOERROR {
				t.Errorf("parallel exchange: result %s, want NOERROR", res.RCode)
			}
		}()
	}
	wg.Wait()
}

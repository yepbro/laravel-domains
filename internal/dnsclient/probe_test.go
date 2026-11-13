// Tests of the resolver startup synthetic probe (T-107): the liveness
// criterion (any RCODE), the probe question, the ProbeChain log levels
// (a Warn for silent ones while the chain is alive, an error only when
// all are dead), ctx cancellation, and the empty chain guard. The log is
// intercepted by a recording slog handler.

package dnsclient

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/miekg/dns"
)

// probeRecorder is a recording slog handler: record levels and messages.
type probeRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *probeRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *probeRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *probeRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *probeRecorder) WithGroup(string) slog.Handler      { return h }

// entries is a snapshot of the records (level, message).
func (h *probeRecorder) entries() []struct {
	level   slog.Level
	message string
	attr    string
} {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]struct {
		level   slog.Level
		message string
		attr    string
	}, 0, len(h.records))
	for _, r := range h.records {
		var addr string
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "resolver" {
				addr = a.Value.String()
			}
			return true
		})
		out = append(out, struct {
			level   slog.Level
			message string
			attr    string
		}{r.Level, r.Message, addr})
	}
	return out
}

// --- Probe -------------------------------------------------------------------

// TestProbeAliveOnAnyRCode — the liveness criterion is any response: NOERROR
// and SERVFAIL alike mean "the resolver responds" (an RCODE is not a probe error).
func TestProbeAliveOnAnyRCode(t *testing.T) {
	for _, rcode := range []RCode{RCodeNOERROR, RCodeSERVFAIL, RCodeNXDOMAIN} {
		t.Run(rcode.String(), func(t *testing.T) {
			r := &fbFake{res: &Result{RCode: rcode}}
			if err := Probe(context.Background(), r); err != nil {
				t.Fatalf("Probe on the answer %v: %v", rcode, err)
			}
		})
	}
}

// TestProbeDeadOnTransportError — a transport failure (after the retries of
// the lower client) fails the probe, the cause is wrapped.
func TestProbeDeadOnTransportError(t *testing.T) {
	sentinel := errors.New("udp: connection refused")
	if err := Probe(context.Background(), &fbFake{err: sentinel}); !errors.Is(err, sentinel) {
		t.Fatalf("Probe: %v, want the cause %v", err, sentinel)
	}
}

// TestProbeQuestionIsRootNS — the probe asks one simple well-known
// question: the root NS; a recursive resolver always answers it (cache/hints).
func TestProbeQuestionIsRootNS(t *testing.T) {
	var got Question
	r := &fbFake{fn: func(_ context.Context, q Question) (*Result, error) {
		got = q
		return &Result{RCode: RCodeNOERROR}, nil
	}}
	if err := Probe(context.Background(), r); err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if got.Name != "." || got.Type != dns.TypeNS {
		t.Fatalf("the probe question: %+v, want NS \".\"", got)
	}
}

// TestProbeNilResolver — a guard against a programming error.
func TestProbeNilResolver(t *testing.T) {
	if err := Probe(context.Background(), nil); err == nil {
		t.Fatal("a nil resolver: want an error")
	}
}

// --- ProbeChain --------------------------------------------------------------

// TestProbeChainAllAlive — everyone answers: no error, every target is Info.
func TestProbeChainAllAlive(t *testing.T) {
	rec := &probeRecorder{}
	log := slog.New(rec)
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: &fbFake{}},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{}},
	}
	if err := ProbeChain(context.Background(), targets, log); err != nil {
		t.Fatalf("ProbeChain: %v", err)
	}
	entries := rec.entries()
	if len(entries) != 2 {
		t.Fatalf("log entries %d, want 2", len(entries))
	}
	for _, e := range entries {
		if e.level != slog.LevelInfo {
			t.Fatalf("the live resolver level: %v (%s)", e.level, e.message)
		}
	}
}

// TestProbeChainPrimaryDeadReserveAlive — the local one failing while the
// backup is alive is a Warn (the pipeline works via switching), no startup error.
func TestProbeChainPrimaryDeadReserveAlive(t *testing.T) {
	rec := &probeRecorder{}
	log := slog.New(rec)
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: &fbFake{err: fakeNetErr{timeout: true}}},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{}},
	}
	if err := ProbeChain(context.Background(), targets, log); err != nil {
		t.Fatalf("ProbeChain: %v", err)
	}
	warns := 0
	for _, e := range rec.entries() {
		switch {
		case e.attr == "127.0.0.1:53" && e.level == slog.LevelWarn:
			warns++
		case e.attr == "127.0.0.1:53":
			t.Fatalf("a dead local — Warn, got %v", e.level)
		case e.attr == "1.1.1.1:53" && e.level != slog.LevelInfo:
			t.Fatalf("a live fallback — Info, got %v", e.level)
		}
	}
	if warns != 1 {
		t.Fatalf("Warn about the dead local: %d, want 1", warns)
	}
}

// TestProbeChainPrimaryAliveReserveDead — a dead backup while the local one
// is alive is also a Warn (lost redundancy), not a startup error.
func TestProbeChainPrimaryAliveReserveDead(t *testing.T) {
	rec := &probeRecorder{}
	log := slog.New(rec)
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: &fbFake{}},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{err: errors.New("no route")}},
	}
	if err := ProbeChain(context.Background(), targets, log); err != nil {
		t.Fatalf("ProbeChain: %v", err)
	}
	for _, e := range rec.entries() {
		if e.attr == "1.1.1.1:53" && e.level != slog.LevelWarn {
			t.Fatalf("a dead fallback — Warn, got %v", e.level)
		}
	}
}

// TestProbeChainAllDead — nobody responds: a startup error listing the
// chain addresses (fail-fast, DNS-12/T-107).
func TestProbeChainAllDead(t *testing.T) {
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: &fbFake{err: errors.New("down")}},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{err: errors.New("down")}},
	}
	err := ProbeChain(context.Background(), targets, slog.New(&probeRecorder{}))
	if err == nil {
		t.Fatal("all dead — a start error")
	}
	for _, want := range []string{"127.0.0.1:53", "1.1.1.1:53"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error without the address %q: %v", want, err)
		}
	}
}

// TestProbeChainEmpty — an empty chain is a programming error (the config
// guarantees a primary resolver, spec 5.10).
func TestProbeChainEmpty(t *testing.T) {
	if err := ProbeChain(context.Background(), nil, nil); err == nil {
		t.Fatal("an empty chain: want an error")
	}
}

// TestProbeChainNilLog — a nil logger is not a panic (records are dropped).
func TestProbeChainNilLog(t *testing.T) {
	targets := []FallbackTarget{{Addr: "127.0.0.1:53", Resolver: &fbFake{}}}
	if err := ProbeChain(context.Background(), targets, nil); err != nil {
		t.Fatalf("ProbeChain: %v", err)
	}
}

// TestProbeChainCancelled — a ctx cancellation mid-probe is the
// cancellation error, not "all dead" (a probe at shutdown does not sentence resolvers).
func TestProbeChainCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dead := &fbFake{fn: func(ctx context.Context, q Question) (*Result, error) {
		cancel()
		return nil, ctx.Err()
	}}
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: dead},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{}},
	}
	if err := ProbeChain(ctx, targets, slog.New(&probeRecorder{})); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

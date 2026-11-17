// Tests of the resolver startup synthetic probe (T-107; the strict root NS
// liveness criterion — T-300, the bulk review 04#P3-1): alive is NOERROR
// with a non-empty answer (a recursive resolver always serves the root NS
// from hints/cache — anything else is a functionally broken server, the
// 24.10 incident shape), a bad RCODE or an empty answer is a Warn-level
// degradation, a transport failure is dead. The probe question, the
// ProbeChain log levels (a Warn for silent or degraded ones while the chain
// is alive, an error only when all are dead or degraded), ctx cancellation,
// and the empty chain guard. The log is intercepted by a recording slog handler.

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

// TestProbeAliveOnUsableRootNS — the liveness criterion is a usable answer
// of the root NS question (T-300): NOERROR with records. A bad RCODE
// (SERVFAIL/REFUSED/NXDOMAIN) or an empty answer means the server responds
// but cannot resolve — not alive for the probe (a Warn in ProbeChain, the
// fail-fast of the whole chain — the incident 24.10 shape: a service that
// dumps every episode into temp_error).
func TestProbeAliveOnUsableRootNS(t *testing.T) {
	alive := []struct {
		name string
		res  *Result
	}{
		{"NOERROR with records", &Result{RCode: RCodeNOERROR, Answer: []dns.RR{nsRR(".", "a.root-servers.net.")}}},
		{"NOERROR NODATA is not usable", &Result{RCode: RCodeNOERROR}},
		{"SERVFAIL", &Result{RCode: RCodeSERVFAIL}},
		{"REFUSED", &Result{RCode: RCodeREFUSED}},
		{"NXDOMAIN", &Result{RCode: RCodeNXDOMAIN}},
	}
	for i, tc := range alive {
		t.Run(tc.name, func(t *testing.T) {
			r := &fbFake{res: tc.res}
			err := Probe(context.Background(), r)
			if i == 0 {
				if err != nil {
					t.Fatalf("Probe on a usable root NS answer: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("an unusable root NS answer is a probe error")
			}
			if !strings.Contains(err.Error(), tc.res.RCode.String()) {
				t.Fatalf("the probe error must name the rcode %v: %v", tc.res.RCode, err)
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
		return &Result{RCode: RCodeNOERROR, Answer: []dns.RR{nsRR(".", "a.root-servers.net.")}}, nil
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
	if err := ProbeChain(context.Background(), targets, FallbackOptions{}, log); err != nil {
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
	if err := ProbeChain(context.Background(), targets, FallbackOptions{}, log); err != nil {
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
	if err := ProbeChain(context.Background(), targets, FallbackOptions{}, log); err != nil {
		t.Fatalf("ProbeChain: %v", err)
	}
	for _, e := range rec.entries() {
		if e.attr == "1.1.1.1:53" && e.level != slog.LevelWarn {
			t.Fatalf("a dead fallback — Warn, got %v", e.level)
		}
	}
}

// TestProbeChainDegradedPrimaryFailsWithoutEscalation — a primary that
// answers the root NS with SERVFAIL and a transport-alive reserve is the
// exact 24.10 incident composition: the default policy does not bypass an
// answering resolver, every episode would become temp_error — a startup
// error (T-300, the review P2-3), with the lever named in the message.
func TestProbeChainDegradedPrimaryFailsWithoutEscalation(t *testing.T) {
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: &fbFake{res: &Result{RCode: RCodeSERVFAIL}}},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{}},
	}
	err := ProbeChain(context.Background(), targets, FallbackOptions{}, slog.New(&probeRecorder{}))
	if err == nil {
		t.Fatal("a degraded primary without the escalation policy — a start error")
	}
	if !strings.Contains(err.Error(), "escalate_rcodes") || !strings.Contains(err.Error(), "127.0.0.1:53") {
		t.Fatalf("the error must name the lever and the primary: %v", err)
	}
}

// TestProbeChainDegradedPrimaryPassesWithEscalation — the same composition
// with dns.fallback.escalate_rcodes on is workable (the reserves are asked
// over the degraded primary's head): a Warn about the primary, an Info for
// the reserve, no startup error.
func TestProbeChainDegradedPrimaryPassesWithEscalation(t *testing.T) {
	rec := &probeRecorder{}
	log := slog.New(rec)
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: &fbFake{res: &Result{RCode: RCodeSERVFAIL}}},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{}},
	}
	if err := ProbeChain(context.Background(), targets, FallbackOptions{EscalateRCodes: true}, log); err != nil {
		t.Fatalf("ProbeChain with the escalation on: %v", err)
	}
	for _, e := range rec.entries() {
		switch {
		case e.attr == "127.0.0.1:53" && e.level == slog.LevelWarn:
			// the degraded primary — Warn
		case e.attr == "127.0.0.1:53":
			t.Fatalf("a SERVFAIL-answering local — Warn, got %v", e.level)
		case e.attr == "1.1.1.1:53" && e.level != slog.LevelInfo:
			t.Fatalf("a live fallback — Info, got %v", e.level)
		}
	}
}

// TestProbeChainAllDegradedFailsFast — everyone answers but nobody can
// resolve the root NS: a startup error (fail-fast instead of rising a
// service that dumps every episode into temp_error, the 24.10 incident).
func TestProbeChainAllDegradedFailsFast(t *testing.T) {
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: &fbFake{res: &Result{RCode: RCodeSERVFAIL}}},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{res: &Result{RCode: RCodeREFUSED}}},
	}
	err := ProbeChain(context.Background(), targets, FallbackOptions{}, slog.New(&probeRecorder{}))
	if err == nil {
		t.Fatal("all degraded — a start error")
	}
	for _, want := range []string{"127.0.0.1:53", "1.1.1.1:53"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error without the address %q: %v", want, err)
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
	err := ProbeChain(context.Background(), targets, FallbackOptions{}, slog.New(&probeRecorder{}))
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
	if err := ProbeChain(context.Background(), nil, FallbackOptions{}, nil); err == nil {
		t.Fatal("an empty chain: want an error")
	}
}

// TestProbeChainNilLog — a nil logger is not a panic (records are dropped).
func TestProbeChainNilLog(t *testing.T) {
	targets := []FallbackTarget{{Addr: "127.0.0.1:53", Resolver: &fbFake{}}}
	if err := ProbeChain(context.Background(), targets, FallbackOptions{}, nil); err != nil {
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
	if err := ProbeChain(ctx, targets, FallbackOptions{}, slog.New(&probeRecorder{})); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// Тесты стартовой синтетической пробы резолвера (T-107): критерий жизни
// (любой RCODE), вопрос пробы, уровни лога ProbeChain (Warn по молчащим
// при живой цепи, ошибка только когда мертвы все), отмена ctx и защита
// от пустой цепи. Лог перехватывается записывающим slog-хендлером.

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

// probeRecorder — записывающий slog-хендлер: уровни и сообщения записей.
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

// entries — снимок записей (уровень, сообщение).
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

// TestProbeAliveOnAnyRCode — критерий жизни — любой ответ: NOERROR и
// SERVFAIL одинаково «резолвер отвечает» (RCODE — не ошибка пробы).
func TestProbeAliveOnAnyRCode(t *testing.T) {
	for _, rcode := range []RCode{RCodeNOERROR, RCodeSERVFAIL, RCodeNXDOMAIN} {
		t.Run(rcode.String(), func(t *testing.T) {
			r := &fbFake{res: &Result{RCode: rcode}}
			if err := Probe(context.Background(), r); err != nil {
				t.Fatalf("Probe при ответе %v: %v", rcode, err)
			}
		})
	}
}

// TestProbeDeadOnTransportError — транспортный отказ (после ретраев
// нижнего клиента) — провал пробы, причина оборачивается.
func TestProbeDeadOnTransportError(t *testing.T) {
	sentinel := errors.New("udp: connection refused")
	if err := Probe(context.Background(), &fbFake{err: sentinel}); !errors.Is(err, sentinel) {
		t.Fatalf("Probe: %v, ожидалась причина %v", err, sentinel)
	}
}

// TestProbeQuestionIsRootNS — проба задаёт простой известный вопрос:
// NS корня; рекурсивный резолвер отвечает на него всегда (кэш/хинты).
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
		t.Fatalf("вопрос пробы: %+v, ожидался NS \".\"", got)
	}
}

// TestProbeNilResolver — защита от программной ошибки.
func TestProbeNilResolver(t *testing.T) {
	if err := Probe(context.Background(), nil); err == nil {
		t.Fatal("nil-резолвер: ожидается ошибка")
	}
}

// --- ProbeChain --------------------------------------------------------------

// TestProbeChainAllAlive — все отвечают: ошибки нет, каждая цель — Info.
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
		t.Fatalf("записей лога %d, ожидалось 2", len(entries))
	}
	for _, e := range entries {
		if e.level != slog.LevelInfo {
			t.Fatalf("уровень живого резолвера: %v (%s)", e.level, e.message)
		}
	}
}

// TestProbeChainPrimaryDeadReserveAlive — провал локального при живом
// резерве — Warn (конвейер работает переключением), ошибки старта нет.
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
			t.Fatalf("мёртвый локальный — Warn, получен %v", e.level)
		case e.attr == "1.1.1.1:53" && e.level != slog.LevelInfo:
			t.Fatalf("живой резерв — Info, получен %v", e.level)
		}
	}
	if warns != 1 {
		t.Fatalf("Warn по мёртвому локальному: %d, ожидался 1", warns)
	}
}

// TestProbeChainPrimaryAliveReserveDead — мёртвый резерв при живом
// локальном — тоже Warn (потерянная избыточность), не ошибка старта.
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
			t.Fatalf("мёртвый резерв — Warn, получен %v", e.level)
		}
	}
}

// TestProbeChainAllDead — не отвечает никто: ошибка старта со списком
// адресов цепи (fail-fast, DNS-12/T-107).
func TestProbeChainAllDead(t *testing.T) {
	targets := []FallbackTarget{
		{Addr: "127.0.0.1:53", Resolver: &fbFake{err: errors.New("down")}},
		{Addr: "1.1.1.1:53", Resolver: &fbFake{err: errors.New("down")}},
	}
	err := ProbeChain(context.Background(), targets, slog.New(&probeRecorder{}))
	if err == nil {
		t.Fatal("все мертвы — ошибка старта")
	}
	for _, want := range []string{"127.0.0.1:53", "1.1.1.1:53"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ошибка без адреса %q: %v", want, err)
		}
	}
}

// TestProbeChainEmpty — пустая цепь — программная ошибка (конфиг
// гарантирует основной резолвер, ТЗ 5.10).
func TestProbeChainEmpty(t *testing.T) {
	if err := ProbeChain(context.Background(), nil, nil); err == nil {
		t.Fatal("пустая цепь: ожидается ошибка")
	}
}

// TestProbeChainNilLog — nil-логгер — не паника (записи подавлены).
func TestProbeChainNilLog(t *testing.T) {
	targets := []FallbackTarget{{Addr: "127.0.0.1:53", Resolver: &fbFake{}}}
	if err := ProbeChain(context.Background(), targets, nil); err != nil {
		t.Fatalf("ProbeChain: %v", err)
	}
}

// TestProbeChainCancelled — отмена ctx среди пробы — ошибка отмены,
// а не «все мертвы» (проба на shutdown не приговор резолверам).
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
		t.Fatalf("ожидается context.Canceled, получено %v", err)
	}
}

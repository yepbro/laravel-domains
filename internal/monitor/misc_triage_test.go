// The mutant triage tests of the CI run of Nov 09, 2026 (half A): the finishing
// of the small monitor sources — unitevents (the hot boundaries of events,
// the systemctl/memory.events parsers, read errors with zeros),
// redisuptime/redismem (the INFO numeric fields: a missing field,
// a negative one, 2^63, zeros on error), psi (the ParseFloat boundaries
// of values), streamgroups (skipping empty streams, "no such key"),
// notify (a constructor without a panic on a nil logger, the Warn reasons
// of disabling, the Send delivery) and renderdebt (the withDefaults boundaries,
// a disabled interval, a nil loop logger, source errors).
package monitor

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- unitevents -------------------------------------------------------------

func TestUnitEventsHot(t *testing.T) {
	cases := []struct {
		e    UnitEvents
		want bool
	}{
		{UnitEvents{}, false},
		{UnitEvents{OOM: 1}, true},
		{UnitEvents{OOMKill: 1}, true},
		{UnitEvents{NRestarts: 1}, true},
		{UnitEvents{High: 1000}, false},
	}
	for _, c := range cases {
		if got := c.e.hot(); got != c.want {
			t.Errorf("hot(%+v) = %v, want %v", c.e, got, c.want)
		}
	}
}

func TestParseSystemctlShowBoundaries(t *testing.T) {
	// NRestarts before the first Id is discarded; several blocks; "[not
	// set]" — zero; the values are distinguishable (base 10, not 9/11).
	out := "NRestarts=99\n" +
		"Id=crawler-http-worker.service\nNRestarts=123\n" +
		"\n" +
		"Id=crawler-render-worker.service\nNRestarts=[not set]\n"
	units := parseSystemctlShow(out)
	if len(units) != 2 {
		t.Fatalf("units = %d, want 2", len(units))
	}
	if units[0].Unit != "crawler-http-worker.service" || units[0].NRestarts != 123 {
		t.Errorf("http-worker = %+v, want NRestarts=123 (dec 123, not 102)", units[0])
	}
	if units[1].NRestarts != 0 {
		t.Errorf("[not set] = %d, want 0", units[1].NRestarts)
	}
	// Empty output — an empty snapshot.
	if got := parseSystemctlShow(""); len(got) != 0 {
		t.Errorf("an empty output = %+v", got)
	}
}

func TestParseMemoryEventsBoundaries(t *testing.T) {
	ev, err := parseMemoryEvents("low 1\nhigh 42\nmax 3\noom 4\noom_kill 5\n")
	if err != nil {
		t.Fatal(err)
	}
	if ev.OOM != 4 || ev.OOMKill != 5 || ev.High != 42 {
		t.Errorf("memory.events = %+v, want oom 4 kill 5 high 42", ev)
	}
	// A value of 2^63 — valid for uint64.
	ev, err = parseMemoryEvents("oom 9223372036854775808\n")
	if err != nil || ev.OOM != 9223372036854775808 {
		t.Errorf("oom 2^63 = (%+v, %v)", ev, err)
	}
	// A non-numeric value — an error.
	if _, err := parseMemoryEvents("oom x\n"); err == nil {
		t.Error("a non-numeric value: want an error")
	}
	// Missing keys — zeros.
	ev, err = parseMemoryEvents("low 7\n")
	if err != nil || ev != (memoryEvents{}) {
		t.Errorf("no keys = (%+v, %v), want zeros", ev, err)
	}
}

func TestReadMemoryEventsFileClasses(t *testing.T) {
	dir := t.TempDir()
	// No file — zeros without an error (a unit without a cgroup).
	ev, err := readMemoryEvents(filepath.Join(dir, "missing"))
	if err != nil || ev != (memoryEvents{}) {
		t.Errorf("no file = (%+v, %v), want zeros", ev, err)
	}
	// A directory instead of a file — a read error (not ErrNotExist).
	if _, err := readMemoryEvents(dir); err == nil {
		t.Error("a directory instead of the file: want an error")
	}
	// A corrupt file — a format error.
	p := filepath.Join(dir, "memory.events")
	if err := os.WriteFile(p, []byte("oom x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readMemoryEvents(p); err == nil {
		t.Error("a broken file: want an error")
	}
}

// --- redisuptime / redismem ---------------------------------------------------

func TestInfoSecondsBoundaries(t *testing.T) {
	if v, err := infoSeconds(map[string]string{"uptime_in_seconds": "125"}, "uptime_in_seconds"); err != nil || v != 125 {
		t.Errorf("125 = (%v, %v)", v, err)
	}
	// A missing field — an error and zero.
	if v, err := infoSeconds(map[string]string{}, "uptime_in_seconds"); err == nil || v != 0 {
		t.Errorf("no field = (%v, %v), want 0 and an error", v, err)
	}
	// Zero is a valid value (the n < 0 boundary).
	if v, err := infoSeconds(map[string]string{"k": "0"}, "k"); err != nil || v != 0 {
		t.Errorf("zero = (%v, %v), want 0 without an error", v, err)
	}
	// A negative — an error and zero.
	if v, err := infoSeconds(map[string]string{"k": "-5"}, "k"); err == nil || v != 0 {
		t.Errorf("negative = (%v, %v), want 0 and an error", v, err)
	}
	// Non-numeric — an error and zero.
	if v, err := infoSeconds(map[string]string{"k": "x"}, "k"); err == nil || v != 0 {
		t.Errorf("non-numeric = (%v, %v), want 0 and an error", v, err)
	}
	// 2^63 — valid for int64 (the bitSize boundary).
	if v, err := infoSeconds(map[string]string{"k": "4611686018427387904"}, "k"); err != nil || v != 4611686018427387904 {
		t.Errorf("2^62 = (%v, %v)", v, err)
	}
}

func TestInfoBytesBoundaries(t *testing.T) {
	if v, err := infoBytes(map[string]string{"maxmemory": "1024"}, "maxmemory"); err != nil || v != 1024 {
		t.Errorf("1024 = (%v, %v)", v, err)
	}
	if v, err := infoBytes(map[string]string{}, "maxmemory"); err == nil || v != 0 {
		t.Errorf("no field = (%v, %v), want 0 and an error", v, err)
	}
	if v, err := infoBytes(map[string]string{"k": "0"}, "k"); err != nil || v != 0 {
		t.Errorf("zero = (%v, %v), want 0 without an error", v, err)
	}
	if v, err := infoBytes(map[string]string{"k": "-1"}, "k"); err == nil || v != 0 {
		t.Errorf("negative = (%v, %v), want 0 and an error", v, err)
	}
}

// --- psi -----------------------------------------------------------------------

func TestParsePSIValueBoundaries(t *testing.T) {
	// Zero and 100 are valid shares (the 0..100 boundaries inclusive).
	for _, v := range []string{"0.00", "100.00"} {
		if _, err := parsePSI("some avg10=" + v + " avg60=1 avg300=1 total=0\n"); err != nil {
			t.Errorf("avg10=%s: %v, want valid", v, err)
		}
	}
	// 1e308 is a valid number for ParseFloat (the parser does not cut the range
	// before the 0..100 check — a "not a share" format error, not "not a number").
	if _, err := parsePSI("some avg10=1e308 avg60=1 avg300=1 total=0\n"); err == nil ||
		!strings.Contains(err.Error(), "not a percent share") {
		t.Errorf("1e308: %v, want \"is not a percent share\"", err)
	}
	// A missing avg300 field — an error.
	if _, err := parsePSI("some avg10=1 avg60=1 total=0\n"); err == nil ||
		!strings.Contains(err.Error(), "avg300") {
		t.Errorf("no avg300: %v", err)
	}
}

// --- streamgroups ----------------------------------------------------------------

func TestStreamGroupsEmptyAndMissing(t *testing.T) {
	// Both streams without groups — an empty snapshot; groups with a zero count
	// are skipped (continue, not break — the subsequent streams are read).
	rdb := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			"q:dns": {}, // an empty list
		},
		xlen: map[string]int64{},
	}
	st, err := (RedisStreamGroups{RDB: rdb}).StreamGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st) != 0 {
		t.Errorf("the snapshot = %+v, want empty (no groups)", st)
	}
}

// --- notify ------------------------------------------------------------------------

func TestTelegramNotifierNilLogNoPanic(t *testing.T) {
	// A nil logger and a nil Getenv are substituted by defaults — the constructor
	// does not panic in any disabling branch.
	for _, opts := range []TelegramOptions{
		{Enabled: false},
		{Enabled: true, Getenv: func(string) string { return "t" }},
		{Enabled: true, Getenv: func(string) string { return "t" }, Script: "/missing"},
	} {
		if n := NewTelegramNotifier(opts); n == nil {
			t.Errorf("%+v: the constructor returned nil", opts)
		}
	}
}

func TestTelegramNotifierOffReasons(t *testing.T) {
	// Enabled, the secrets are present, the script not set — a Warn "the script
	// path is not set", the delivery is off (not the "script unavailable" branch).
	var buf syncedBuffer
	logger := newNotifyLogger(&buf)
	n := NewTelegramNotifier(TelegramOptions{
		Enabled: true,
		Log:     logger,
		Getenv:  func(string) string { return "x" },
	})
	if n.script != "" {
		t.Fatalf("script = %q, want off", n.script)
	}
	log := buf.String()
	if !strings.Contains(log, "script path is not set") {
		t.Errorf("a Warn without the \"script path is not set\" reason:\n%s", log)
	}
	if strings.Contains(log, "unavailable or not executable") {
		t.Errorf("the reason is mixed up with \"the script is unavailable\":\n%s", log)
	}
}

// newNotifyLogger — a text logger into a synchronized buffer.
func newNotifyLogger(buf *syncedBuffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestTelegramNotifierDisabledNoSend — the delivery disabled (no
// secrets): OnPass/Send make no send attempts (the mock journal and
// the log are clean of "telegram:"), the episode state is not mailed out.
func TestTelegramNotifierDisabledNoSend(t *testing.T) {
	buf := &syncedBuffer{}
	n := NewTelegramNotifier(TelegramOptions{
		Enabled: true,
		Log:     newNotifyLogger(buf),
		Getenv:  func(string) string { return "" }, // no secrets
	})
	res := AlertsResult{Alerts: []AlertSnapshot{{
		Name: AlertQueueDead, Status: AlertStatusFiring, Detail: "depth 5",
	}}}
	n.OnPass(res)
	n.Send("report")
	time.Sleep(50 * time.Millisecond) // the background send goroutines
	if log := buf.String(); strings.Contains(log, "send") {
		t.Errorf("the disabled delivery tries to send:\n%s", log)
	}
}

// TestTelegramNotifierNotMeasuredEpisode — "not measured" does not break
// the episode: the return of measurements to ok closes it with exactly one ✅.
func TestTelegramNotifierNotMeasuredEpisode(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal.log")
	script := fakeNotifyScript(t, journal, false)
	n, _ := notifierEnv(t, script, "token", "chat")

	pass := func(status, detail string) AlertsResult {
		return AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueDead, Status: status, Detail: detail}}}
	}
	n.OnPass(pass(AlertStatusNotMeasured, "Redis not set")) // the "not measured" base
	n.OnPass(pass(AlertStatusFiring, "depth 5"))            // 🚨
	n.OnPass(pass(AlertStatusNotMeasured, "drop"))          // an interruption — without a message
	n.OnPass(pass(AlertStatusOK, "depth 0"))                // ✅
	n.OnPass(pass(AlertStatusOK, "depth 0"))                // silence
	waitForJournal(t, journal, "🚨")
	waitForJournal(t, journal, "✅")
	b, err := os.ReadFile(journal)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), "🚨"); n != 1 {
		t.Errorf("🚨 = %d, want 1", n)
	}
	if n := strings.Count(string(b), "✅"); n != 1 {
		t.Errorf("✅ = %d, want 1 (a repeated ok stays silent)", n)
	}
}

// TestTelegramNotifierNotMeasuredCold — "not measured" without an open
// episode — complete silence (no message, not even in the log: the note is only
// for a fired alert).
func TestTelegramNotifierNotMeasuredCold(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal.log")
	script := fakeNotifyScript(t, journal, false)
	n, buf := notifierEnv(t, script, "token", "chat")
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
		{Name: AlertQueueDead, Status: AlertStatusNotMeasured, Detail: "Redis not set"},
	}})
	time.Sleep(50 * time.Millisecond)
	if strings.Contains(buf.String(), "no longer measured") {
		t.Errorf("a cold \"not measured\" is noted in the log:\n%s", buf.String())
	}
	if b, _ := os.ReadFile(journal); len(b) != 0 {
		t.Errorf("a cold \"not measured\" mailed out a message: %s", b)
	}
}

func TestTelegramConstPin(t *testing.T) {
	if telegramNotifyTimeout != 40*time.Second {
		t.Errorf("telegramNotifyTimeout = %v, want 40s", telegramNotifyTimeout)
	}
}

func TestTelegramNotifierSendDelivers(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal.log")
	script := fakeNotifyScript(t, journal, false)
	n, _ := notifierEnv(t, script, "token", "chat")
	n.Send("arbitrary report")
	waitForJournal(t, journal, "arbitrary report")
}

// waitForJournal — waits for a line to appear in the mock journal (a
// background send goroutine).
func waitForJournal(t *testing.T, journal, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(journal)
		if err == nil && strings.Contains(string(b), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the line %q was not delivered to the journal", want)
}

// --- renderdebt ---------------------------------------------------------------------

type fakeDebtSource struct {
	debt RenderDebt
	err  error
}

func (s *fakeDebtSource) RenderDebt(context.Context) (RenderDebt, error) {
	return s.debt, s.err
}

func TestRenderDebtOptionsBoundaries(t *testing.T) {
	if DefaultRenderDebtInterval != 5*time.Minute {
		t.Errorf("DefaultRenderDebtInterval = %v, want 5m", DefaultRenderDebtInterval)
	}
	if got := (RenderDebtOptions{}).withDefaults().Interval; got != DefaultRenderDebtInterval {
		t.Errorf("a zero interval = %v, want the default", got)
	}
	if got := (RenderDebtOptions{Interval: -time.Second}).withDefaults().Interval; got != DefaultRenderDebtInterval {
		t.Errorf("a negative interval = %v, want the default", got)
	}
	if got := (RenderDebtOptions{Interval: time.Second}).withDefaults().Interval; got != time.Second {
		t.Errorf("a one-second interval = %v, want as set", got)
	}
}

func TestRenderDebtLoopNilLogAndSourceErrors(t *testing.T) {
	// A nil logger does not panic: the starting pass runs and exits.
	src := &fakeDebtSource{debt: RenderDebt{Flags: 3, Lag: 1}}
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	go func() { done <- RenderDebtLoop(ctx, src, RenderDebtOptions{Interval: time.Hour}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the loop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not exit")
	}

	// The source not set — an error.
	if err := RenderDebtLoop(context.Background(), nil, RenderDebtOptions{Interval: time.Minute}); err == nil {
		t.Error("the source is not set: want an error")
	}
}

func TestRenderDebtSourceErrors(t *testing.T) {
	// The pool/Redis not set — source errors (return-cut
	// mutants would panic on nil).
	if _, err := renderDebtOf(context.Background(), nil, nil); err == nil {
		t.Error("the pool is not set: want an error")
	}
	if _, err := renderDebtOf(context.Background(), &zonestatsFakePool{}, nil); err == nil {
		t.Error("Redis not set: want an error")
	}
	// A Query error — a source error.
	pool := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("db unavailable")}}}
	if _, err := renderDebtOf(context.Background(), pool, &renderLensSource{}); err == nil {
		t.Error("a Query error: want an error")
	}
}

// renderLensSource — a StreamLener with zero depths.
type renderLensSource struct{}

func (s *renderLensSource) Len(context.Context, string) (int64, error) { return 0, nil }

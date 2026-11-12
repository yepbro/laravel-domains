// The tests of alert delivery to Telegram (T-162): a mock script (an exec via
// a temporary file — the real telegram-notify.sh goes to the network) writes
// the arguments into a journal; the transitions are checked (firing 🚨, clearing ✅,
// debouncing without duplicates, the first pass — the base, going to "not measured" —
// without a message), the disabling (config, secrets, script) and the send failure.
package monitor

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNotifyScript — an executable mock of telegram-notify.sh: it appends the
// message text (--message) to the journal, one line per call (the newlines
// of the text become tabs: a journal line = a send; a single-write record —
// the background sends of consecutive passes do not interleave). fail=true
// — exit 1 with noise in stderr (a send failure).
func fakeNotifyScript(t *testing.T, journal string, fail bool) string {
	t.Helper()
	dir := t.TempDir()
	script := filepath.Join(dir, "telegram-notify.sh")
	oneLine := "line=$(printf '%s' \"$2\" | tr '\\n' '\\t'); printf '%s\\n' \"$line\" >> " + journal + "\n"
	body := "#!/bin/sh\n" + oneLine
	if fail {
		body += "echo 'telegram: ошибка отправки (HTTP 400)' >&2\nexit 1\n"
	}
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return script
}

// syncedBuffer — a bytes.Buffer under a lock: the background send
// goroutine writes a Warn into the logger at the same time as the test reads the buffer (the
// logger serializes writes, but not the buffer read from another goroutine).
type syncedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// notifierEnv — a notifier on the mock with a log buffer and a secret getter.
func notifierEnv(t *testing.T, script string, token, chat string) (*TelegramNotifier, *syncedBuffer) {
	t.Helper()
	buf := &syncedBuffer{}
	getenv := func(key string) string {
		switch key {
		case EnvTelegramBotToken:
			return token
		case EnvTelegramChatID:
			return chat
		}
		return ""
	}
	n := NewTelegramNotifier(TelegramOptions{
		Enabled: true,
		Script:  script,
		Log:     slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Getenv:  getenv,
	})
	return n, buf
}

// journalLines — the lines of the mock's send journal.
func journalLines(t *testing.T, journal string) []string {
	t.Helper()
	b, err := os.ReadFile(journal)
	if err != nil {
		return nil
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// waitLines — waits for n journal lines to appear (the send is asynchronous).
func waitLines(t *testing.T, journal string, n int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		lines := journalLines(t, journal)
		if len(lines) >= n {
			return lines
		}
		if time.Now().After(deadline) {
			t.Fatalf("отправок = %d, want ≥ %d (журнал: %q)", len(lines), n, journalLines(t, journal))
		}
		time.Sleep(time.Millisecond)
	}
}

// TestTelegramNotifierTransitions — firing and clearing as separate
// messages, no duplicates (debouncing — a state change only),
// the first pass — the base without mailing, a fired alert going to "not measured"
// — without a message.
func TestTelegramNotifierTransitions(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, false)
	n, _ := notifierEnv(t, script, "token", "chat")

	pass := func(status string) {
		n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
			{Name: AlertDiskMin, Status: status, Detail: "свободно 12.1GiB из 79.0GiB, порог 15.0GiB"},
		}})
	}

	pass(AlertStatusFiring) // the first pass — the base: a pre-start firing is not mailed
	if lines := journalLines(t, journal); len(lines) != 0 {
		t.Fatalf("после базы отправок = %d, want 0: %q", len(lines), lines)
	}
	pass(AlertStatusFiring) // a repeated firing — silence
	pass(AlertStatusFiring)
	if lines := journalLines(t, journal); len(lines) != 0 {
		t.Fatalf("повторные срабатывания дали дубли: %q", lines)
	}

	pass(AlertStatusOK) // clearing — a separate message
	lines := waitLines(t, journal, 1)
	if !strings.Contains(lines[0], "✅ Восстановлено") || !strings.Contains(lines[0], AlertDiskMin) ||
		!strings.Contains(lines[0], "12.1GiB") {
		t.Errorf("снятие = %q, want «✅ Восстановлено» с именем и деталью", lines[0])
	}

	pass(AlertStatusFiring) // a new episode — a new firing
	lines = waitLines(t, journal, 2)
	if !strings.Contains(lines[1], "🚨") || !strings.Contains(lines[1], AlertDiskMin) ||
		!strings.Contains(lines[1], alertDesc(AlertDiskMin)) {
		t.Errorf("срабатывание = %q, want «🚨» с именем и описанием", lines[1])
	}

	// A source failure at a fired alert — without a message ("recovered" would
	// be a lie); the episode stays open; a repeated firing after the
	// source pause does not give a second 🚨 (the alert was never cleared).
	pass(AlertStatusNotMeasured)
	pass(AlertStatusFiring)
	if lines := journalLines(t, journal); len(lines) != 2 {
		t.Errorf("пауза источника дала сообщение: %q", lines)
	}

	// A recovery of measurements straight to ok — closes the episode with ✅.
	pass(AlertStatusOK)
	lines = waitLines(t, journal, 3)
	if !strings.Contains(lines[2], "✅ Восстановлено") {
		t.Errorf("восстановление после «не измеряется» = %q, want ✅", lines[2])
	}

	// A new episode — a new 🚨.
	pass(AlertStatusFiring)
	lines = waitLines(t, journal, 4)
	if !strings.Contains(lines[3], "🚨") {
		t.Errorf("новый эпизод = %q, want 🚨", lines[3])
	}

	// An unfired alert going to "not measured" and back — silence.
	n2, _ := notifierEnv(t, script, "token", "chat")
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusOK}}})
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusNotMeasured}}})
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusOK}}})
	if got := len(journalLines(t, journal)); got != 4 {
		t.Errorf("шум несработавшего алерта: отправок %d, want 4", got)
	}
}

// TestTelegramNotifierDisabled — all the disabling reasons: config, no
// secrets (either of the two missing), the script not existing/not executable —
// not a single exec; the reason — in the journal.
func TestTelegramNotifierDisabled(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, false)
	fire := AlertsResult{Alerts: []AlertSnapshot{{Name: AlertDiskMin, Status: AlertStatusFiring, Detail: "d"}}}

	send := func(n *TelegramNotifier) {
		n.OnPass(fire)
		n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertDiskMin, Status: AlertStatusOK}}})
		n.OnPass(fire)
	}

	// Explicitly disabled by config.
	var buf bytes.Buffer
	n := NewTelegramNotifier(TelegramOptions{
		Enabled: false, Script: script,
		Log:    slog.New(slog.NewTextHandler(&buf, nil)),
		Getenv: func(string) string { return "x" },
	})
	send(n)
	if lines := journalLines(t, journal); len(lines) != 0 {
		t.Errorf("telegram=false: отправки %q", lines)
	}
	if !strings.Contains(buf.String(), "выключена") {
		t.Errorf("выключение конфигом без записи:\n%s", buf.String())
	}

	// No token / no chat / neither.
	for _, c := range []struct {
		name  string
		token string
		chat  string
	}{
		{"нет токена", "", "чат"},
		{"нет чата", "токен", ""},
		{"нет обоих", "", ""},
	} {
		n, buf := notifierEnv(t, script, c.token, c.chat)
		send(n)
		if lines := journalLines(t, journal); len(lines) != 0 {
			t.Errorf("%s: отправки %q", c.name, lines)
		}
		if !strings.Contains(buf.String(), EnvTelegramBotToken) {
			t.Errorf("%s: причина без имени секрета:\n%s", c.name, buf.String())
		}
	}

	// No script / a directory instead of the script / not executable.
	dir := t.TempDir()
	noExec := filepath.Join(dir, "plain")
	if err := os.WriteFile(noExec, []byte("#!/bin/sh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		path string
	}{
		{"скрипта нет", filepath.Join(dir, "нет")},
		{"каталог", dir},
		{"не исполняем", noExec},
	} {
		n, buf := notifierEnv(t, c.path, "token", "chat")
		send(n)
		if lines := journalLines(t, journal); len(lines) != 0 {
			t.Errorf("%s: отправки %q", c.name, lines)
		}
		if !strings.Contains(buf.String(), "level=WARN") {
			t.Errorf("%s: сломанная настройка без Warn:\n%s", c.name, buf.String())
		}
	}
}

// TestTelegramNotifierScriptFailure — a send failure (exit code 1): a Warn
// with the script output; the delivery stays alive (the next transition tries again).
func TestTelegramNotifierScriptFailure(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, true)
	n, buf := notifierEnv(t, script, "token", "chat")

	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusOK, Detail: "d"}}}) // the base
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusFiring, Detail: "d"}}})
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusOK, Detail: "d"}}})

	// Both calls reached the script (the mock journal is written before exit 1).
	waitLines(t, journal, 2)
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "level=WARN") {
		if time.Now().After(deadline) {
			t.Fatalf("сбой отправки без Warn:\n%s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}
	if !strings.Contains(buf.String(), "HTTP 400") {
		t.Errorf("Warn без вывода скрипта:\n%s", buf.String())
	}
}

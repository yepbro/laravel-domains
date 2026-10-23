// Тесты доставки алертов в Telegram (T-162): мок-скрипт (exec через
// временный файл — реальный telegram-notify.sh ходит в сеть) пишет
// аргументы в журнал; проверяются переходы (срабатывание 🚨, снятие ✅,
// дебаунс без дублей, первый проход — база, уход в «не измеряется» —
// без сообщения), выключения (конфиг, секреты, скрипт) и сбой отправки.
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

// fakeNotifyScript — исполняемый мок telegram-notify.sh: дописывает текст
// сообщения (--message) в journal одной строкой на вызов (переводы строк
// текста — в табы: строка журнала = отправка; запись одним write —
// фоновые отправки последовательных проходов не интерлетятся). fail=true
// — выход 1 с шумом в stderr (сбой отправки).
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

// syncedBuffer — bytes.Buffer под замком: фоновая горутина отправки
// пишет Warn в логгер одновременно с чтением буфера тестом (логгер
// сериализует записи, но не чтение буфера из другой горутины).
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

// notifierEnv — нотификатор на моке с буфером лога и геттером секрета.
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

// journalLines — строки журнала отправок мока.
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

// waitLines — ждёт появления n строк журнала (отправка асинхронна).
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

// TestTelegramNotifierTransitions — срабатывание и снятие отдельными
// сообщениями, дубли отсутствуют (дебаунс — только изменение состояния),
// первый проход — база без рассылки, уход сработавшего в «не измеряется»
// — без сообщения.
func TestTelegramNotifierTransitions(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, false)
	n, _ := notifierEnv(t, script, "token", "chat")

	pass := func(status string) {
		n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
			{Name: AlertDiskMin, Status: status, Detail: "свободно 12.1GiB из 79.0GiB, порог 15.0GiB"},
		}})
	}

	pass(AlertStatusFiring) // первый проход — база: сработавший до старта не рассылается
	if lines := journalLines(t, journal); len(lines) != 0 {
		t.Fatalf("после базы отправок = %d, want 0: %q", len(lines), lines)
	}
	pass(AlertStatusFiring) // повторно сработавший — тишина
	pass(AlertStatusFiring)
	if lines := journalLines(t, journal); len(lines) != 0 {
		t.Fatalf("повторные срабатывания дали дубли: %q", lines)
	}

	pass(AlertStatusOK) // снятие — отдельное сообщение
	lines := waitLines(t, journal, 1)
	if !strings.Contains(lines[0], "✅ Восстановлено") || !strings.Contains(lines[0], AlertDiskMin) ||
		!strings.Contains(lines[0], "12.1GiB") {
		t.Errorf("снятие = %q, want «✅ Восстановлено» с именем и деталью", lines[0])
	}

	pass(AlertStatusFiring) // новый эпизод — новое срабатывание
	lines = waitLines(t, journal, 2)
	if !strings.Contains(lines[1], "🚨") || !strings.Contains(lines[1], AlertDiskMin) ||
		!strings.Contains(lines[1], alertDesc(AlertDiskMin)) {
		t.Errorf("срабатывание = %q, want «🚨» с именем и описанием", lines[1])
	}

	// Сбой источника у сработавшего — без сообщения («восстановлено» было
	// бы ложью), эпизод открыт; повторное срабатывание после паузы
	// источника второго 🚨 не даёт (алерт и не снимался).
	pass(AlertStatusNotMeasured)
	pass(AlertStatusFiring)
	if lines := journalLines(t, journal); len(lines) != 2 {
		t.Errorf("пауза источника дала сообщение: %q", lines)
	}

	// Восстановление измерений сразу в ok — закрывает эпизод ✅.
	pass(AlertStatusOK)
	lines = waitLines(t, journal, 3)
	if !strings.Contains(lines[2], "✅ Восстановлено") {
		t.Errorf("восстановление после «не измеряется» = %q, want ✅", lines[2])
	}

	// Новый эпизод — новый 🚨.
	pass(AlertStatusFiring)
	lines = waitLines(t, journal, 4)
	if !strings.Contains(lines[3], "🚨") {
		t.Errorf("новый эпизод = %q, want 🚨", lines[3])
	}

	// Несработавший алерт в «не измеряется» и обратно — тишина.
	n2, _ := notifierEnv(t, script, "token", "chat")
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusOK}}})
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusNotMeasured}}})
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusOK}}})
	if got := len(journalLines(t, journal)); got != 4 {
		t.Errorf("шум несработавшего алерта: отправок %d, want 4", got)
	}
}

// TestTelegramNotifierDisabled — все причины выключения: конфиг, нет
// секретов (нет любого из двух), скрипт не существует/не исполняем —
// ни одного exec, причина — в журнале.
func TestTelegramNotifierDisabled(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, false)
	fire := AlertsResult{Alerts: []AlertSnapshot{{Name: AlertDiskMin, Status: AlertStatusFiring, Detail: "d"}}}

	send := func(n *TelegramNotifier) {
		n.OnPass(fire)
		n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertDiskMin, Status: AlertStatusOK}}})
		n.OnPass(fire)
	}

	// Явно выключено конфигом.
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

	// Нет токена / нет чата / нет обоих.
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

	// Скрипта нет / каталог вместо скрипта / не исполняем.
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

// TestTelegramNotifierScriptFailure — сбой отправки (код выхода 1): Warn
// с выводом скрипта, доставка живёт (следующий переход пытается снова).
func TestTelegramNotifierScriptFailure(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, true)
	n, buf := notifierEnv(t, script, "token", "chat")

	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusOK, Detail: "d"}}}) // база
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusFiring, Detail: "d"}}})
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusOK, Detail: "d"}}})

	// Оба вызова дошли до скрипта (журнал мока пишется до exit 1).
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

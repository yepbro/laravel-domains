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
	"strconv"
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
		body += "echo 'telegram: send error (HTTP 400)' >&2\nexit 1\n"
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
// The delivery pace is test-fast (the real token bucket — one message per
// 3 s — would stretch the multi-message tests to minutes); the knobs travel
// through TelegramOptions and are applied by the constructor before the
// worker starts (29#P3-9). The optional tweaks override the defaults (a
// small queue, a short drain budget — the overflow/truncation tests).
func notifierEnv(t *testing.T, script string, token, chat string, tweaks ...func(*TelegramOptions)) (*TelegramNotifier, *syncedBuffer) {
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
	opts := TelegramOptions{
		Enabled:      true,
		Script:       script,
		Log:          slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Getenv:       getenv,
		paceBurst:    1000,
		paceEvery:    time.Millisecond,
		retryBackoff: 20 * time.Millisecond,
	}
	for _, tweak := range tweaks {
		tweak(&opts)
	}
	n := NewTelegramNotifier(opts)
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
			t.Fatalf("sends = %d, want ≥ %d (the journal: %q)", len(lines), n, journalLines(t, journal))
		}
		time.Sleep(time.Millisecond)
	}
}

// fakeNotifyControlledScript — a programmable mock of telegram-notify.sh:
// every call appends to the attempts counter; the first failN calls print
// the script's error format for the given HTTP code and exit 1, the rest
// succeed (the journal line); with release set (a file path) every call
// first blocks until the file appears (the in-flight send of the Close
// test). The knobs travel through the FAKE_NOTIFY_* environment — the exec
// inherits the test process env (the notify tests are not parallel; each
// test sets its own paths).
func fakeNotifyControlledScript(t *testing.T, journal string, failN int, code, release string) (script, attempts string) {
	t.Helper()
	dir := t.TempDir()
	attempts = filepath.Join(dir, "attempts")
	script = filepath.Join(dir, "telegram-notify.sh")
	t.Setenv("FAKE_NOTIFY_JOURNAL", journal)
	t.Setenv("FAKE_NOTIFY_ATTEMPTS", attempts)
	t.Setenv("FAKE_NOTIFY_FAILS", strconv.Itoa(failN))
	t.Setenv("FAKE_NOTIFY_CODE", code)
	if release != "" {
		t.Setenv("FAKE_NOTIFY_BLOCK", release)
	}
	body := `#!/bin/sh
n=$(($(cat "$FAKE_NOTIFY_ATTEMPTS" 2>/dev/null || echo 0) + 1))
printf '%s' "$n" > "$FAKE_NOTIFY_ATTEMPTS"
if [ -n "${FAKE_NOTIFY_BLOCK:-}" ]; then
	while [ ! -f "$FAKE_NOTIFY_BLOCK" ]; do sleep 0.01; done
fi
if [ "$n" -le "$FAKE_NOTIFY_FAILS" ]; then
	echo "telegram: send error (HTTP $FAKE_NOTIFY_CODE); the run is not failed by it — the step has continue-on-error" >&2
	echo "telegram: API: $FAKE_NOTIFY_CODE: Too Many Requests: retry after 1" >&2
	exit 1
fi
line=$(printf '%s' "$2" | tr '\n' '\t'); printf '%s\n' "$line" >> "$FAKE_NOTIFY_JOURNAL"
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return script, attempts
}

// attemptsOf — the current value of the mock's attempt counter (-1 — none).
func attemptsOf(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return -1
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return -1
	}
	return v
}

// waitAttempts — waits for the mock's attempt counter to reach n.
func waitAttempts(t *testing.T, path string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if attemptsOf(t, path) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the script attempts did not reach %d (now %d)", n, attemptsOf(t, path))
}

// TestTelegramNotifierTransitions — firing and clearing as separate
// messages, no duplicates (debouncing — a state change only), the base
// pass of a fired alert — the informational ⏳ "still firing" (12#P2-2:
// a monitor restart on a deploy does not mail out healthy states, but an
// inherited incident is announced — the later ✅ stays paired), a fired
// alert going to "not measured" — without a message.
func TestTelegramNotifierTransitions(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, false)
	n, _ := notifierEnv(t, script, "token", "chat")

	pass := func(status string) {
		n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
			{Name: AlertDiskMin, Status: status, Detail: "free 12.1GiB of 79.0GiB, threshold 15.0GiB"},
		}})
	}

	// The base pass of a fired alert — ⏳, not a fresh 🚨: the episode
	// predates the monitor start; the inherited incident stays visible.
	pass(AlertStatusFiring)
	lines := waitLines(t, journal, 1)
	if !strings.Contains(lines[0], "⏳") || !strings.Contains(lines[0], AlertDiskMin) ||
		!strings.Contains(lines[0], alertDesc(AlertDiskMin)) || !strings.Contains(lines[0], "12.1GiB") {
		t.Errorf("the inherited base = %q, want \"⏳ Still firing\" with the name, the description and the detail", lines[0])
	}
	pass(AlertStatusFiring) // repeated firings — silence
	pass(AlertStatusFiring)
	if lines := journalLines(t, journal); len(lines) != 1 {
		t.Fatalf("repeated firings produced duplicates: %q", lines)
	}

	pass(AlertStatusOK) // clearing — a separate message, paired with the ⏳
	lines = waitLines(t, journal, 2)
	if !strings.Contains(lines[1], "✅ Recovered") || !strings.Contains(lines[1], AlertDiskMin) ||
		!strings.Contains(lines[1], "12.1GiB") {
		t.Errorf("the clearing = %q, want \"✅ Recovered\" with the name and the detail", lines[1])
	}

	pass(AlertStatusFiring) // a new episode — a new firing
	lines = waitLines(t, journal, 3)
	if !strings.Contains(lines[2], "🚨") || !strings.Contains(lines[2], AlertDiskMin) ||
		!strings.Contains(lines[2], alertDesc(AlertDiskMin)) {
		t.Errorf("the firing = %q, want \"🚨\" with the name and the description", lines[2])
	}

	// A source failure at a fired alert — without a message ("recovered" would
	// be a lie); the episode stays open; a repeated firing after the
	// source pause does not give a second 🚨 (the alert was never cleared).
	pass(AlertStatusNotMeasured)
	pass(AlertStatusFiring)
	if lines := journalLines(t, journal); len(lines) != 3 {
		t.Errorf("a source pause produced a message: %q", lines)
	}

	// A recovery of measurements straight to ok — closes the episode with ✅.
	pass(AlertStatusOK)
	lines = waitLines(t, journal, 4)
	if !strings.Contains(lines[3], "✅ Recovered") {
		t.Errorf("a recovery after \"not measured\" = %q, want ✅", lines[3])
	}

	// A new episode — a new 🚨.
	pass(AlertStatusFiring)
	lines = waitLines(t, journal, 5)
	if !strings.Contains(lines[4], "🚨") {
		t.Errorf("a new episode = %q, want 🚨", lines[4])
	}

	// An unfired alert going to "not measured" and back — silence.
	n2, _ := notifierEnv(t, script, "token", "chat")
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusOK}}})
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusNotMeasured}}})
	n2.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertPSICPU, Status: AlertStatusOK}}})
	if got := len(journalLines(t, journal)); got != 5 {
		t.Errorf("the noise of an unfired alert: %d sends, want 5", got)
	}
}

// TestEpisodeNextTable — the pure transition table of the notification
// episode FSM (12#P2-4): the full state × pass-status matrix and the
// delivery-failure event.
func TestEpisodeNextTable(t *testing.T) {
	cases := []struct {
		name   string
		st     episodeState
		status string
		want   episodeState
		act    episodeAction
	}{
		{"base ok", episodeBase, AlertStatusOK, episodeIdle, episodeActNone},
		{"base not measured", episodeBase, AlertStatusNotMeasured, episodeIdle, episodeActNone},
		{"base firing — inherited ⏳", episodeBase, AlertStatusFiring, episodeFiring, episodeActInherit},
		{"idle ok", episodeIdle, AlertStatusOK, episodeIdle, episodeActNone},
		{"idle not measured", episodeIdle, AlertStatusNotMeasured, episodeIdle, episodeActNone},
		{"idle firing — open 🚨", episodeIdle, AlertStatusFiring, episodeFiring, episodeActOpen},
		{"firing holds", episodeFiring, AlertStatusFiring, episodeFiring, episodeActNone},
		{"firing ok — close ✅", episodeFiring, AlertStatusOK, episodeIdle, episodeActClose},
		{"firing not measured — the edge note", episodeFiring, AlertStatusNotMeasured, episodeFiringUnmeasured, episodeActNoteUnmeasured},
		{"unmeasured holds — in silence", episodeFiringUnmeasured, AlertStatusNotMeasured, episodeFiringUnmeasured, episodeActNone},
		{"unmeasured back to firing — no second 🚨", episodeFiringUnmeasured, AlertStatusFiring, episodeFiring, episodeActNone},
		{"unmeasured ok — the honest ✅", episodeFiringUnmeasured, AlertStatusOK, episodeIdle, episodeActClose},
		{"undelivered firing — re-open 🚨", episodeFiringUndelivered, AlertStatusFiring, episodeFiring, episodeActOpen},
		{"undelivered ok — the silent close (no unpaired ✅)", episodeFiringUndelivered, AlertStatusOK, episodeIdle, episodeActNone},
		{"undelivered not measured — holds in silence", episodeFiringUndelivered, AlertStatusNotMeasured, episodeFiringUndelivered, episodeActNone},
		{"an unknown status — no change", episodeFiring, "weird", episodeFiring, episodeActNone},
	}
	for _, c := range cases {
		got, act := nextEpisode(c.st, c.status)
		if got != c.want || act != c.act {
			t.Errorf("%s: next(%d, %s) = (%d, %d), want (%d, %d)",
				c.name, c.st, c.status, got, act, c.want, c.act)
		}
	}
	for _, c := range []struct {
		st   episodeState
		want episodeState
	}{
		{episodeBase, episodeBase},
		{episodeIdle, episodeIdle},
		{episodeFiring, episodeFiringUndelivered},
		{episodeFiringUnmeasured, episodeFiringUndelivered},
		{episodeFiringUndelivered, episodeFiringUndelivered},
	} {
		if got := onEpisodeSendFailed(c.st); got != c.want {
			t.Errorf("sendFailed(%d) = %d, want %d", c.st, got, c.want)
		}
	}
}

// TestTelegramNotifierSendRetries429 — a transient failure (HTTP 429 with
// Retry-After 1): the delivery waits the parsed pause, retries and
// succeeds; the message is delivered once (12#P2-3).
func TestTelegramNotifierSendRetries429(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script, attempts := fakeNotifyControlledScript(t, journal, 1, "429", "")
	n, buf := notifierEnv(t, script, "token", "chat")

	n.Send("retry me")
	waitAttempts(t, attempts, 2) // the retry happened…
	lines := waitLines(t, journal, 1)
	if !strings.Contains(lines[0], "retry me") {
		t.Errorf("the retried message = %q, want the text", lines[0])
	}
	if !strings.Contains(buf.String(), "retrying") {
		t.Errorf("a retry without a journal entry:\n%s", buf.String())
	}
}

// TestTelegramNotifierPermanentNoRetry — a permanent failure (HTTP 400):
// exactly one attempt (no retry), a Warn with the script output (12#P2-3);
// the Retry-After line in a non-429 output is not honored.
func TestTelegramNotifierPermanentNoRetry(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script, attempts := fakeNotifyControlledScript(t, journal, 1000, "400", "")
	n, buf := notifierEnv(t, script, "token", "chat")

	n.Send("arbitrary")
	waitAttempts(t, attempts, 1)
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "delivery failed") {
		if time.Now().After(deadline) {
			t.Fatalf("a send failure without a Warn:\n%s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}
	// The test backoff is 20 ms — a wrongly retried message would have hit
	// the script again well within this pause.
	time.Sleep(150 * time.Millisecond)
	if got := attemptsOf(t, attempts); got != 1 {
		t.Errorf("attempts = %d, want 1 (a permanent failure is not retried)", got)
	}
}

// TestTelegramNotifierFiringRedelivered — the 🚨 delivery failed
// permanently: the episode does not silently close — the delivery failure
// is an FSM event, and the next firing pass re-attempts the message
// (12#P2-3/P2-4; a lost opening must not surface only as a late ✅).
func TestTelegramNotifierFiringRedelivered(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script, attempts := fakeNotifyControlledScript(t, journal, 1000, "400", "")
	n, _ := notifierEnv(t, script, "token", "chat")

	pass := func(status string) {
		n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
			{Name: AlertQueueFreeze, Status: status, Detail: "d"},
		}})
	}
	pass(AlertStatusOK)     // the base — silence
	pass(AlertStatusFiring) // 🚨 enqueued → the single attempt fails
	waitAttempts(t, attempts, 1)
	time.Sleep(50 * time.Millisecond) // the failure event reaches failedC
	pass(AlertStatusFiring)           // the FSM re-attempts the opening
	waitAttempts(t, attempts, 2)
}

// TestTelegramNotifierCloseDrains — Close waits for the in-flight send
// (12#P2-3): a message picked up by the worker is delivered before Close
// returns; the intake after Close is a Warn and a drop, without a panic;
// a second Close and a disabled notifier's Close — no-ops.
func TestTelegramNotifierCloseDrains(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	release := filepath.Join(t.TempDir(), "release")
	script, attempts := fakeNotifyControlledScript(t, journal, 0, "429", release)
	n, buf := notifierEnv(t, script, "token", "chat")

	n.Send("in flight")
	waitAttempts(t, attempts, 1) // the worker picked the message up; the script runs
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := os.WriteFile(release, []byte("go"), 0o644); err != nil {
			t.Error(err)
		}
	}()
	n.Close() // returns only after the in-flight send has completed
	if lines := journalLines(t, journal); len(lines) != 1 || !strings.Contains(lines[0], "in flight") {
		t.Fatalf("after Close the in-flight message must be delivered: %q", lines)
	}

	n.Send("too late") // the closed intake — a Warn and a drop
	if !strings.Contains(buf.String(), "shutting down") {
		t.Errorf("the post-Close drop without a Warn:\n%s", buf.String())
	}
	time.Sleep(50 * time.Millisecond)
	if got := attemptsOf(t, attempts); got != 1 {
		t.Errorf("attempts after Close = %d, want 1 (the dropped message is not sent)", got)
	}
	n.Close() // idempotent

	// A disabled delivery — Close is a no-op without a panic.
	n2 := NewTelegramNotifier(TelegramOptions{Enabled: false})
	n2.Close()
}

// TestTelegramNotifierDetailTruncated — a detail over the message budget
// (sendMessage rejects texts over 4096 characters) is cut with a pointer to
// the monitor journal; the truncation is noted in the log (12#P3-1).
func TestTelegramNotifierDetailTruncated(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, false)
	n, buf := notifierEnv(t, script, "token", "chat")

	detail := strings.Repeat("x", 4000) + "|THE TAIL|"
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
		{Name: AlertQueueFreeze, Status: AlertStatusFiring, Detail: detail},
	}})
	lines := waitLines(t, journal, 1)
	if strings.Contains(lines[0], "|THE TAIL|") {
		t.Error("the tail of the over-budget detail leaked into the message")
	}
	if !strings.Contains(lines[0], "full text in the monitor journal") {
		t.Errorf("the truncated message has no journal pointer: %q", lines[0])
	}
	if got := len([]rune(lines[0])); got > 3700 {
		t.Errorf("the truncated message is %d runes, want ≤ 3700 (the sendMessage cap is 4096)", got)
	}
	if !strings.Contains(buf.String(), "truncated") {
		t.Errorf("the truncation without a log note:\n%s", buf.String())
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
		t.Errorf("telegram=false: sends %q", lines)
	}
	if !strings.Contains(buf.String(), "delivery off") {
		t.Errorf("a config switch-off without a record:\n%s", buf.String())
	}

	// No token / no chat / neither.
	for _, c := range []struct {
		name  string
		token string
		chat  string
	}{
		{"no token", "", "chat"},
		{"no chat", "token", ""},
		{"neither", "", ""},
	} {
		n, buf := notifierEnv(t, script, c.token, c.chat)
		send(n)
		if lines := journalLines(t, journal); len(lines) != 0 {
			t.Errorf("%s: sends %q", c.name, lines)
		}
		if !strings.Contains(buf.String(), EnvTelegramBotToken) {
			t.Errorf("%s: the reason without the secret name:\n%s", c.name, buf.String())
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
		{"no script", filepath.Join(dir, "missing")},
		{"a directory", dir},
		{"not executable", noExec},
	} {
		n, buf := notifierEnv(t, c.path, "token", "chat")
		send(n)
		if lines := journalLines(t, journal); len(lines) != 0 {
			t.Errorf("%s: sends %q", c.name, lines)
		}
		if !strings.Contains(buf.String(), "level=WARN") {
			t.Errorf("%s: a broken setup without a Warn:\n%s", c.name, buf.String())
		}
	}
}

// TestTelegramNotifierScriptFailure — a send failure (exit code 1): a Warn
// with the script output; the delivery stays alive (the next transition tries
// again). An episode whose 🚨 failed closes silently — no unpaired ✅ for an
// episode the operator has never seen (29#P2-2).
func TestTelegramNotifierScriptFailure(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, true)
	n, buf := notifierEnv(t, script, "token", "chat")

	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusOK, Detail: "d"}}}) // the base
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusFiring, Detail: "d"}}})
	// The delivery failure must reach failedC before the next pass (in
	// production the pass cadence is minutes, the failure — seconds).
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "level=WARN") {
		if time.Now().After(deadline) {
			t.Fatalf("a send failure without a Warn:\n%s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)                                                                             // reportFailure → failedC
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueFreeze, Status: AlertStatusOK, Detail: "d"}}}) // the silent close

	// The failed 🚨 reached the script (the mock journals the text before exit 1).
	waitLines(t, journal, 1)
	if !strings.Contains(buf.String(), "HTTP 400") {
		t.Errorf("a Warn without the script output:\n%s", buf.String())
	}
	// No unpaired ✅ even asynchronously: the failed opening is the only send.
	time.Sleep(150 * time.Millisecond)
	if got := len(journalLines(t, journal)); got != 1 {
		t.Errorf("sends = %d, want 1 (no unpaired ✅ after a failed 🚨):\n%s", got, buf.String())
	}
}

// TestTelegramNotifierQueueOverflow — a full queue must not lose an episode
// opening forever (29#P2-1): the drop returns to the FSM as
// firing-undelivered, and the next firing pass re-attempts the 🚨.
func TestTelegramNotifierQueueOverflow(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	release := filepath.Join(t.TempDir(), "release")
	script, attempts := fakeNotifyControlledScript(t, journal, 0, "429", release)
	n, buf := notifierEnv(t, script, "token", "chat", func(o *TelegramOptions) { o.queueCap = 1 })

	// Occupy the queue: one in-flight send (blocked on the release file),
	// one queued — the cap is full, the overflow is deterministic.
	n.Send("filler 1")
	waitAttempts(t, attempts, 1)
	n.Send("filler 2")

	// A fresh episode (a base OK pass first): the 🚨 hits the full queue.
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
		{Name: AlertQueueFreeze, Status: AlertStatusOK, Detail: "d"},
	}})
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
		{Name: AlertQueueFreeze, Status: AlertStatusFiring, Detail: "d"},
	}})
	if !strings.Contains(buf.String(), "the delivery queue is full") {
		t.Fatalf("the overflow drop without a Warn:\n%s", buf.String())
	}

	// Release the transport: the fillers drain; the episode re-attempt —
	// the next firing pass re-enqueues and delivers the 🚨.
	if err := os.WriteFile(release, []byte("go"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitLines(t, journal, 2)
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
		{Name: AlertQueueFreeze, Status: AlertStatusFiring, Detail: "d"},
	}})
	lines := waitLines(t, journal, 3)
	if !strings.Contains(lines[2], "🚨") || !strings.Contains(lines[2], AlertQueueFreeze) {
		t.Errorf("the re-attempted opening = %q, want 🚨 with the alert name", lines[2])
	}
}

// TestTelegramNotifierInheritedRedelivered — the re-attempt of an
// undelivered inherited opening keeps its kind: ⏳ is re-sent as ⏳, not as
// a fresh 🚨 announcing a firing the monitor never saw (29#P3-1).
func TestTelegramNotifierInheritedRedelivered(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	// The first call fails permanently (400), the rest succeed.
	script, attempts := fakeNotifyControlledScript(t, journal, 1, "400", "")
	n, _ := notifierEnv(t, script, "token", "chat")

	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
		{Name: AlertQueueFreeze, Status: AlertStatusFiring, Detail: "d"},
	}}) // the base pass — ⏳; the single attempt fails permanently
	waitAttempts(t, attempts, 1)
	time.Sleep(50 * time.Millisecond) // the failure event reaches failedC

	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
		{Name: AlertQueueFreeze, Status: AlertStatusFiring, Detail: "d"},
	}}) // the re-attempt — the same kind
	lines := waitLines(t, journal, 1)
	if !strings.Contains(lines[0], "⏳ Still firing") || strings.Contains(lines[0], "🚨") {
		t.Errorf("the re-attempted inherited opening = %q, want ⏳ Still firing", lines[0])
	}
}

// TestTelegramNotifierStaleFailureIgnored — a failure event carries its
// episode's generation: by the time it arrives, the episode may have closed
// and a new one opened — the stale event must not mark the new episode
// undelivered (a duplicate 🚨, 29#P3-2); a fresh event of the current
// generation does.
func TestTelegramNotifierStaleFailureIgnored(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, false)
	n, _ := notifierEnv(t, script, "token", "chat")
	pass := func(status string) {
		n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
			{Name: AlertQueueFreeze, Status: status, Detail: "d"},
		}})
	}

	// Episode 1 (generation 1): opened and closed normally.
	pass(AlertStatusOK)
	pass(AlertStatusFiring)
	waitLines(t, journal, 1)
	pass(AlertStatusOK)
	waitLines(t, journal, 2)

	// Episode 2 (generation 2) is firing; a stale failure of episode 1 arrives.
	pass(AlertStatusFiring)
	waitLines(t, journal, 3)
	n.failedC <- telegramMessage{alert: AlertQueueFreeze, kind: telegramMsgFiring, gen: 1}
	pass(AlertStatusFiring) // applies the events: the stale one is ignored
	time.Sleep(100 * time.Millisecond)
	if got := len(journalLines(t, journal)); got != 3 {
		t.Errorf("sends = %d, want 3 (a stale failure event re-opened the episode:\n%s)", got, journalLines(t, journal))
	}

	// A fresh event of the current generation does re-open the delivery.
	n.failedC <- telegramMessage{alert: AlertQueueFreeze, kind: telegramMsgFiring, gen: 2}
	pass(AlertStatusFiring)
	lines := waitLines(t, journal, 4)
	if !strings.Contains(lines[3], "🚨") {
		t.Errorf("the re-attempted opening = %q, want 🚨", lines[3])
	}
}

// TestTelegramNotifierCloseDropsQueuedTail — the queued-but-unsent tail is
// dropped at Close with a Warn; only the in-flight send is awaited
// (29#P3-8).
func TestTelegramNotifierCloseDropsQueuedTail(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	release := filepath.Join(t.TempDir(), "release")
	script, attempts := fakeNotifyControlledScript(t, journal, 0, "429", release)
	n, buf := notifierEnv(t, script, "token", "chat")

	n.Send("in flight")
	waitAttempts(t, attempts, 1) // the worker picked it up; the script is blocked
	n.Send("queued 1")
	n.Send("queued 2")
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := os.WriteFile(release, []byte("go"), 0o644); err != nil {
			t.Error(err)
		}
	}()
	n.Close()
	if lines := journalLines(t, journal); len(lines) != 1 || !strings.Contains(lines[0], "in flight") {
		t.Fatalf("after Close the in-flight message must be delivered, the tail dropped: %q", lines)
	}
	if !strings.Contains(buf.String(), "queued messages dropped at shutdown") {
		t.Errorf("the dropped tail without a Warn:\n%s", buf.String())
	}
}

// TestTelegramNotifierDrainTruncated — Close does not wait past its drain
// budget (29#P2-3): a hung in-flight send truncates the drain with an
// explicit Warn; the orphaned script child still finishes on its own (no
// kill, no deadlock) — the message is delivered after all.
func TestTelegramNotifierDrainTruncated(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	release := filepath.Join(t.TempDir(), "release")
	script, attempts := fakeNotifyControlledScript(t, journal, 0, "429", release)
	n, buf := notifierEnv(t, script, "token", "chat",
		func(o *TelegramOptions) { o.drainBudget = 50 * time.Millisecond })

	n.Send("slow")
	waitAttempts(t, attempts, 1) // the script is blocked on the release file

	start := time.Now()
	n.Close()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Close blocked for %s despite the drain budget", elapsed)
	}
	if !strings.Contains(buf.String(), "drain truncated") {
		t.Errorf("the truncated drain without a Warn:\n%s", buf.String())
	}

	// The worker is still inside the exec; releasing lets the child finish.
	if err := os.WriteFile(release, []byte("go"), 0o644); err != nil {
		t.Fatal(err)
	}
	lines := waitLines(t, journal, 1)
	if !strings.Contains(lines[0], "slow") {
		t.Errorf("the orphaned send = %q, want the text", lines[0])
	}
}

// TestTelegramPacerDelay — the arithmetic of the token bucket (29#P3-8):
// the burst tokens are immediate, the next token is borrowed (the wait is
// paid before the send), the idle time refills up to the burst.
func TestTelegramPacerDelay(t *testing.T) {
	const eps = 10 * time.Millisecond
	check := func(got, want time.Duration, step string) {
		if d := got - want; d < -eps || d > eps {
			t.Errorf("%s: delay = %s, want ≈ %s", step, got, want)
		}
	}
	t0 := time.Unix(1700000000, 0)

	// burst 2: two immediate sends, the third borrows the next token.
	p := telegramPacer{burst: 2, every: 3 * time.Second}
	check(p.delay(t0), 0, "burst: the first")
	check(p.delay(t0.Add(time.Second)), 0, "burst: the second")
	check(p.delay(t0.Add(time.Second)), 2*time.Second, "burst: the third borrows")
	// At t0+3.5 s: half a token refilled since the borrowed last (t0+3 s)
	// — the wait is the missing half of the interval.
	check(p.delay(t0.Add(3500*time.Millisecond)), 2500*time.Millisecond, "burst: after a pause")
	// A long idle refills up to the burst.
	check(p.delay(t0.Add(2*time.Minute)), 0, "burst: refilled after an idle")
	check(p.delay(t0.Add(2*time.Minute)), 0, "burst: the second refill token")

	// burst 1 — the production shape (29#P3-5): the first immediate, then
	// one per interval, never above the pace on its own.
	p1 := telegramPacer{burst: 1, every: 3 * time.Second}
	check(p1.delay(t0), 0, "burst 1: the first")
	check(p1.delay(t0), 3*time.Second, "burst 1: the second borrows the full interval")
	check(p1.delay(t0.Add(100*time.Second)), 0, "burst 1: refilled after an idle")
	check(p1.delay(t0.Add(100*time.Second)), 3*time.Second, "burst 1: capped at one token")
}

// TestTelegramNotifierUnmeasuredNotedOnce — the "no longer measured" note
// of an open episode is an edge, not every pass (the I3 discipline,
// 29#P3-4): a multi-pass degradation produces exactly one Info.
func TestTelegramNotifierUnmeasuredNotedOnce(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal")
	script := fakeNotifyScript(t, journal, false)
	n, buf := notifierEnv(t, script, "token", "chat")
	pass := func(status string) {
		n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
			{Name: AlertQueueFreeze, Status: status, Detail: "d"},
		}})
	}

	pass(AlertStatusOK)     // the base
	pass(AlertStatusFiring) // 🚨
	waitLines(t, journal, 1)
	pass(AlertStatusNotMeasured)
	pass(AlertStatusNotMeasured)
	pass(AlertStatusNotMeasured)
	if got := strings.Count(buf.String(), "no longer measured"); got != 1 {
		t.Errorf("the unmeasured note fired %d times, want 1 (an edge, not every pass):\n%s", got, buf.String())
	}
}

// TestClassifyTelegramFailure — the classification against the script's
// error format: the transients (429/5xx/transport), the permanents, the
// parsed and the unparsed Retry-After (29#P3-3).
func TestClassifyTelegramFailure(t *testing.T) {
	cases := []struct {
		name       string
		out        string
		transient  bool
		retryAfter time.Duration
	}{
		{"429 with Retry-After", "telegram: send error (HTTP 429); the run is not failed by it — the step has continue-on-error\n" +
			"telegram: API: 429: Too Many Requests: retry after 7", true, 7 * time.Second},
		{"429 without Retry-After", "telegram: send error (HTTP 429); a proxy HTML body", true, telegramRetryAfterUnparsed},
		{"429 with an over-cap Retry-After", "telegram: send error (HTTP 429)\ntelegram: API: 429: Too Many Requests: retry after 9999", true, telegramRetryAfterMax},
		{"5xx", "telegram: send error (HTTP 502)", true, 0},
		{"a curl transport error", "telegram: send error (HTTP 000)", true, 0},
		{"a bad request", "telegram: send error (HTTP 400)", false, 0},
		{"a revoked token", "telegram: API: 403: bot was blocked by the user", false, 0},
	}
	for _, c := range cases {
		f := classifyTelegramFailure(c.out)
		if f.transient != c.transient || f.retryAfter != c.retryAfter {
			t.Errorf("%s: classify(%q) = %+v, want transient=%v retryAfter=%s",
				c.name, c.out, f, c.transient, c.retryAfter)
		}
	}
}

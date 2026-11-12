// Delivering alerts to the operator in Telegram (T-162, spec 5.9): fired and
// cleared monitor alerts are sent as messages to the same dedicated group
// as the CI/CD notifications (spec 10.4) — by calling deploy/telegram-notify.sh
// (T-136) with the --message mode: an exec of the external script, as the
// pipeline calls it; no Telegram HTTP client is created in Go (a single point of
// truth about sending: the markers, secret masking, behavior without them).
//
// The secrets TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are taken by the script from the environment:
// monitor inherits them from the unit's EnvironmentFile (/etc/crawler/crawler.env
// — crawler-monitor.service); they do not get into the YAML. Without any secret
// or the script — the delivery is off; the alerts stay a Warn in the monitoring
// journal (like a CI step staying silent without secrets, §10.4).
//
// Debouncing — an alert episode (opening…closing) is delivered by exactly two
// messages: 🚨 at the first firing, "✅ Recovered" at the
// clearing; repeated passes of a fired alert produce no messages.
// The first pass — the base without mailing (a monitor restart on a deploy does not
// mail out the states of all alerts). A source interruption ("not measured")
// does not break the episode: a return of measurements straight to ok closes the episode with an honest
// ✅ (the alert stopped firing — the operator needs the point); a return to
// firing does not make a second 🚨 (the alert was never cleared — source flapping
// does not spam); the transition to "not measured" itself is not marked by a message:
// "recovered" would be a lie; the transition is visible in the journal (Info).
package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"
)

// The Telegram secret environment variables (the monitor unit's EnvironmentFile,
// deploy/systemd/crawler.env.example — the same values as the CI/CD
// notifications, docs/ci-cd.md §"Telegram notifications").
const (
	EnvTelegramBotToken = "TELEGRAM_BOT_TOKEN"
	EnvTelegramChatID   = "TELEGRAM_CHAT_ID"
)

// telegramNotifyTimeout — the budget of one script call: curl inside with
// --max-time 30 plus a bash/jq start — with headroom, but shorter than the
// alert loop period.
const telegramNotifyTimeout = 40 * time.Second

// TelegramOptions — the alert delivery parameters (NewTelegramNotifier).
type TelegramOptions struct {
	// Enabled — the delivery switch (monitor.alerts.telegram): false —
	// the delivery is explicitly off.
	Enabled bool
	// Script — the telegram-notify.sh path on the host
	// (monitor.alerts.telegram_script).
	Script string
	// Log — the logger (nil — entries suppressed, slog.DiscardHandler).
	Log *slog.Logger
	// Getenv — reading the secret environment (nil — os.Getenv); the tests
	// substitute it (t.Setenv is unsuitable: an empty value is checked too).
	Getenv func(string) string
}

// TelegramNotifier — the delivery of alert transitions to Telegram: connected
// by the hook in AlertsOptions.OnPass (the pass loop calls it from one thread —
// no state locking is needed). Sending is asynchronous: an alert pass does
// not wait for curl (a hung script is cut off by the timeout); a failure — a Warn: the alert is
// already in the journal and metrics; a retry — the next state transition.
type TelegramNotifier struct {
	// script — the script path; "" — the delivery is off (all reasons —
	// configuration, secrets, script availability — are decided by the constructor).
	script string
	log    *slog.Logger
	// state — the episode tracking by alert names: an episode is from the
	// first firing to the clearing; within it exactly one 🚨 and one ✅.
	state map[string]notifyEpisode
}

// notifyEpisode — the state of one alert for delivery.
type notifyEpisode struct {
	// seen — the alert pass has already been observed (the starting base).
	seen bool
	// wasFiring — the alert fired since the last clearing (the episode is open):
	// source interruptions ("not measured") do not close the episode —
	// a recovery of measurements to ok closes it honestly with ✅; a return to
	// firing does not make a second 🚨 (the alert was never cleared).
	wasFiring bool
}

// NewTelegramNotifier decides whether the delivery is on: explicitly off,
// no environment secrets, the script unavailable/not executable — no message
// recipient appears; every reason is a journal entry (Info for
// expected states, a Warn for a broken production host setup).
func NewTelegramNotifier(opts TelegramOptions) *TelegramNotifier {
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	n := &TelegramNotifier{script: "", log: log, state: make(map[string]notifyEpisode)}
	if !opts.Enabled {
		log.Info("telegram: доставка алертов выключена (monitor.alerts.telegram=false)")
		return n
	}
	if getenv(EnvTelegramBotToken) == "" || getenv(EnvTelegramChatID) == "" {
		log.Info("telegram: доставка алертов выключена — не задан TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID " +
			"(EnvironmentFile юнита, deploy/systemd/crawler.env.example); алерты остаются Warn в журнале monitor")
		return n
	}
	script := opts.Script
	if script == "" {
		log.Warn("telegram: доставка алертов выключена — не задан путь скрипта (monitor.alerts.telegram_script)")
		return n
	}
	if st, err := os.Stat(script); err != nil || st.IsDir() || st.Mode()&0o111 == 0 {
		log.Warn("telegram: доставка алертов выключена — скрипт не доступен или не исполняем",
			"script", script, "err", err)
		return n
	}
	n.script = script
	log.Info("telegram: доставка алертов включена", "script", script)
	return n
}

// OnPass — the AlertsOptions.OnPass hook: an alert episode (firing … clearing)
// is delivered by exactly two messages — 🚨 at the opening and ✅ at
// the closing. Debouncing: repeated passes of a fired alert, a return to
// firing after a source interruption and the noise of unfired alerts — silence.
// The first pass — the starting base without mailing: a monitor restart on a deploy
// does not mail out the states of all alerts.
func (n *TelegramNotifier) OnPass(res AlertsResult) {
	if n.script == "" {
		return
	}
	for _, a := range res.Alerts {
		st := n.state[a.Name]
		firstPass := !st.seen
		st.seen = true
		switch {
		case a.Status == AlertStatusFiring:
			if !st.wasFiring && !firstPass {
				n.send(fmt.Sprintf("🚨 Алерт %s: %s\n%s", a.Name, alertDesc(a.Name), a.Detail))
			}
			st.wasFiring = true
		case a.Status == AlertStatusOK:
			if st.wasFiring {
				n.send(fmt.Sprintf("✅ Восстановлено: %s (%s)\n%s", a.Name, alertDesc(a.Name), a.Detail))
			}
			st.wasFiring = false
		case a.Status == AlertStatusNotMeasured && st.wasFiring:
			// Without a message — see the file header: a recovery is not proven,
			// and source flapping must neither spam nor close the episode.
			n.log.Info("telegram: сработавший алерт больше не измеряется — без сообщения",
				"alert", a.Name, "причина", a.Detail)
		}
		n.state[a.Name] = st
	}
}

// alertDesc — the alert description for the message text (alertDescs).
func alertDesc(name string) string {
	if d, ok := alertDescs[name]; ok {
		return d
	}
	return name
}

// Send — an arbitrary message to the operator (the periodic load report
// of T-173): the same script, secrets and timeout as the alerts; the delivery
// off (no secrets/script — script == "") — a no-op. Safe
// for a concurrent call (send — its own goroutine per message).
func (n *TelegramNotifier) Send(text string) {
	if n.script == "" {
		return
	}
	n.send(text)
}

// send runs the script in a background goroutine: the text is the only
// --message argument (an exec without a shell: the text is not interpreted,
// quotes/newlines are preserved; the script takes the secrets from the inherited
// monitor environment and does not print them into the output).
func (n *TelegramNotifier) send(text string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), telegramNotifyTimeout)
		defer cancel()
		out, err := exec.CommandContext(ctx, n.script, "--message", text).CombinedOutput()
		switch {
		case ctx.Err() != nil:
			n.log.Warn("telegram: отправка алерта прервана таймаутом",
				"script", n.script, "timeout", telegramNotifyTimeout.String())
		case err != nil:
			n.log.Warn("telegram: отправка алерта не удалась",
				"script", n.script, "err", err, "вывод", strings.TrimSpace(string(out)))
		default:
			n.log.Debug("telegram: алерт отправлен", "вывод", strings.TrimSpace(string(out)))
		}
	}()
}

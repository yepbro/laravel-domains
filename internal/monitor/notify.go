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
// # The notification episode FSM (T-290, the bulk review 12#P2-2/P2-4)
//
// Debouncing — an alert episode (opening…closing) is delivered by at most two
// messages: 🚨 at the first firing, "✅ Recovered" at the clearing; repeated
// passes of a fired alert produce no messages. The lifecycle is an explicit
// finite-state machine (the TZ-fsm project rule): episodeState × the pass
// status → nextEpisode — the pure transition table below. The states:
//
//	base               the alert has not been observed yet (a fresh notifier
//	                   — the monitor start). The first pass is the base: a
//	                   restart on a deploy does not mail out the states of
//	                   all alerts, but an inherited incident stays visible
//	                   and paired — an already fired alert yields the
//	                   informational "⏳ Still firing at the monitor start"
//	                   (12#P2-2: formerly the later ✅ arrived unpaired, for
//	                   an episode the operator had never seen a 🚨 for);
//	                   healthy and degraded alerts start silently;
//	idle               no open episode;
//	firing             an open episode, the opening message enqueued;
//	firing-undelivered an open episode whose opening message was not
//	                   delivered (the transport exhausted its retries, or
//	                   the intake queue was full — 29#P2-1): the next
//	                   firing pass re-attempts it — a 🚨 lost to a
//	                   429/5xx outage is not lost forever (12#P2-3); the
//	  	           delivery failure is an event of this same automaton
//	                   (onEpisodeSendFailed), not a transport-internal flag;
//	                   on ok the episode closes silently — a "recovered"
//	                   for an episode the operator has never seen misleads
//	                   (the unpaired-✅ defect of 12#P2-2 by another door,
//	                   29#P2-2; the failed delivery is already a Warn in
//	                   the journal);
//	firing-unmeasured  an open episode, the source degraded: the transition
//	                   itself is not marked by a message — "recovered" would
//	                   be a lie, and source flapping must neither spam nor
//	                   close the episode; the degradation is noted once, at
//	                   the edge — repeated passes hold in silence (29#P3-4);
//	                   a return of measurements straight
//	                   to ok closes the episode with an honest ✅ (the alert
//	                   stopped firing — the operator needs the point); a
//	                   return to firing does not make a second 🚨 (the alert
//	                   was never cleared).
//
// # The serialized delivery (T-290, the bulk review 12#P2-3)
//
// One worker goroutine, a bounded queue and a token bucket instead of a
// goroutine per message: a common incident firing dozens of alerts at once
// does not stampede the API into 429, the message order is kept, the memory
// is bounded, and the shutdown is a bounded drain (Close: the in-flight
// message finishes — awaited via a WaitGroup within telegramDrainBudget,
// smaller than the service's own graceful window; the queued-but-unsent are
// dropped with a Warn). One script call is retried on transient failures —
// HTTP 429 (with the API's Retry-After), 5xx and transport errors — with an
// exponential backoff; a 🚨 that still fails returns to the episode FSM and
// is re-attempted on the next alert pass. An opening dropped at the intake
// (a full queue) returns to the episode FSM the same way (29#P2-1). Close
// is the shutdown drain of the service — the caller (cmd/monitor) invokes
// it after its loops, once every producer of messages has stopped.
package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
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

// The delivery parameters (T-290): the queue, the pace and the attempts of
// the serialized sender.
const (
	// telegramQueueCap — the bounded delivery queue: a common incident fires
	// up to ~20 alerts per pass plus the load report; an overflow — a Warn
	// and a drop, and an episode opening returns to the FSM as
	// firing-undelivered (the next pass re-attempts it, 29#P2-1); the
	// alert loop must never block on the delivery.
	telegramQueueCap = 64
	// telegramSendBurst/telegramSendEvery — the token bucket of the send
	// rate: the first message immediate, then one per 3 s — exactly the
	// pace of the Telegram group cap (20/min), never above it on the
	// monitor's own; a residual 429 (the group is shared with the CI/CD
	// posts) is retried with the API's Retry-After
	// (classifyTelegramFailure).
	telegramSendBurst = 1
	telegramSendEvery = 3 * time.Second
	// telegramSendAttempts — the attempts per message: the first try plus
	// two retries with an exponential backoff; a longer outage is the
	// episode FSM's business — the next firing pass re-attempts the
	// opening message of an open episode (onEpisodeSendFailed).
	telegramSendAttempts = 3
	// telegramRetryBackoff — the base pause between the attempts
	// (2 s, then 4 s); a 429's Retry-After replaces it.
	telegramRetryBackoff = 2 * time.Second
	// telegramRetryAfterMax — the ceiling of the parsed Retry-After: the
	// API named its pause — honor it up to this bound.
	telegramRetryAfterMax = 5 * time.Minute
	// telegramRetryAfterUnparsed — the wait of a 429 whose output carries
	// no parsable Retry-After (a proxy/HTML body): the queue is
	// serialized, and a long default pause would starve every message
	// behind it for no API-mandated reason (29#P3-3).
	telegramRetryAfterUnparsed = 30 * time.Second
	// telegramDrainBudget — the budget of the shutdown drain (Close):
	// deliberately smaller than the service framework's graceful window
	// (service.DefaultShutdownTimeout, 10 s) and the unit's
	// TimeoutStopSec (30 s), so a hung in-flight send truncates the drain
	// with an explicit Warn instead of stretching the stop into the
	// framework's false "did not stop" alarm and a SIGKILL (29#P2-3); the
	// orphaned script child finishes on its own (curl --max-time 30).
	telegramDrainBudget = 8 * time.Second
	// telegramDetailLimit — the detail budget inside a message:
	// sendMessage rejects texts over 4096 characters; with the message
	// header and the truncation suffix a 3500-rune detail stays under the
	// cap (the details are ASCII-dominated — stream names, numbers, unit
	// names; astral-plane symbols count double in Telegram's UTF-16 — the
	// ~600-character headroom covers them).
	telegramDetailLimit = 3500
)

// The message kinds — for the journal entries and the episode binding of
// delivery failures (reportFailure).
const (
	telegramMsgFiring    = "firing"    // 🚨 the opening of an episode
	telegramMsgInherited = "inherited" // ⏳ an episode inherited at the monitor start
	telegramMsgRecovered = "recovered" // ✅ the closing of an episode
	telegramMsgArbitrary = "message"   // an arbitrary text (Send), not episode-bound
)

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

	// The test knobs (unexported — not a configuration surface; zero — the
	// production values above). They are applied by the constructor BEFORE
	// the worker goroutine starts, so the pacer/backoff/budget fields are
	// written exactly once, before any read (29#P3-9 — formerly the tests
	// patched them on a running worker).
	queueCap     int
	paceBurst    int
	paceEvery    time.Duration
	retryBackoff time.Duration
	drainBudget  time.Duration
}

// TelegramNotifier — the delivery of alert transitions to Telegram: connected
// by the hook in AlertsOptions.OnPass (the pass loop calls it from one thread —
// the episode state map has a single writer and needs no locking) and by Send
// (the load report, the slow queries — safe for concurrent calls: the intake
// is a guarded bounded queue). The sending is serialized (T-290): a single
// worker goroutine paces the script calls with a token bucket and retries
// transient failures; Close — the shutdown drain (see the file header).
type TelegramNotifier struct {
	// script — the script path; "" — the delivery is off (all reasons —
	// configuration, secrets, script availability — are decided by the constructor).
	script string
	log    *slog.Logger
	// state — the episode FSM state by alert name (the single writer —
	// OnPass; the worker's delivery-failure events arrive through failedC
	// and are applied at the start of the next pass).
	state map[string]episodeState
	// epGen — the episode generation by alert name (incremented on every
	// fresh opening): delivery-failure events carry the generation of
	// their message, so a stale event of an already closed/reopened
	// episode does not mark the new one undelivered (29#P3-2).
	epGen map[string]int
	// undeliveredKind — the kind (⏳/🚨) of an undelivered opening, for its
	// re-attempt: an inherited episode is re-announced as ⏳, not as a
	// fresh 🚨 (29#P3-1). An entry exists exactly while the state is
	// firing-undelivered.
	undeliveredKind map[string]string

	// The serialized delivery: the queue, the worker's failure events and
	// its lifetime (done + WaitGroup), the pace.
	queue   chan telegramMessage
	failedC chan telegramMessage
	done    chan struct{}
	wg      sync.WaitGroup
	pacer   telegramPacer
	// retryBackoff — the base pause between the attempts.
	retryBackoff time.Duration
	// drainBudget — the shutdown-drain budget of Close (29#P2-3).
	drainBudget time.Duration

	// mu/closed — the intake guard: after Close the enqueue is a Warn and
	// a drop (the channel itself stays open — the worker leaves it alone).
	mu     sync.Mutex
	closed bool
}

// episodeState — the state of one alert's notification episode (the FSM
// specification — the file header; the transition table — nextEpisode).
type episodeState uint8

const (
	// episodeBase — no pass of the alert observed yet (the monitor start).
	episodeBase episodeState = iota
	// episodeIdle — observed, no open episode.
	episodeIdle
	// episodeFiring — an open episode (the opening message enqueued).
	episodeFiring
	// episodeFiringUndelivered — an open episode, the opening message was
	// not delivered — the next firing pass re-attempts it.
	episodeFiringUndelivered
	// episodeFiringUnmeasured — an open episode, the source degraded.
	episodeFiringUnmeasured
)

// episodeAction — what OnPass does besides moving to the next state.
type episodeAction uint8

const (
	episodeActNone           episodeAction = iota
	episodeActOpen                         // 🚨 — the opening message
	episodeActInherit                      // ⏳ — an episode inherited at the base pass
	episodeActClose                        // ✅ — the closing message
	episodeActNoteUnmeasured               // the Info note, without a message
)

// nextEpisode — the pure transition table of the notification episode FSM:
// the state and the pass status → the next state and the delivery action.
// Unknown statuses (never produced by the machines) change nothing.
func nextEpisode(st episodeState, status string) (episodeState, episodeAction) {
	switch st {
	case episodeBase:
		// The base pass: an inherited incident is announced with ⏳ — not a
		// fresh 🚨 (the monitor has not seen the opening, 12#P2-2); healthy
		// and degraded alerts start silently.
		if status == AlertStatusFiring {
			return episodeFiring, episodeActInherit
		}
		return episodeIdle, episodeActNone
	case episodeIdle:
		if status == AlertStatusFiring {
			return episodeFiring, episodeActOpen
		}
		return episodeIdle, episodeActNone
	case episodeFiring, episodeFiringUndelivered:
		// The undelivered opening re-attempts on the next firing pass
		// (the re-attempt keeps the episode — its generation and ⏳/🚨
		// kind — see OnPass); an undelivered episode closes silently on
		// ok: a "recovered" for an episode the operator has never seen
		// misleads exactly as the unpaired ✅ of 12#P2-2 did (29#P2-2) —
		// the failed delivery is already a Warn in the journal.
		switch status {
		case AlertStatusFiring:
			if st == episodeFiringUndelivered {
				return episodeFiring, episodeActOpen
			}
			return episodeFiring, episodeActNone
		case AlertStatusOK:
			if st == episodeFiringUndelivered {
				return episodeIdle, episodeActNone
			}
			return episodeIdle, episodeActClose
		case AlertStatusNotMeasured:
			// The degradation is noted once, at the edge (I3 — the
			// front-edge discipline, 29#P3-4); an undelivered episode
			// holds its state — the delivery failure dominates.
			if st == episodeFiringUndelivered {
				return episodeFiringUndelivered, episodeActNone
			}
			return episodeFiringUnmeasured, episodeActNoteUnmeasured
		}
	case episodeFiringUnmeasured:
		switch status {
		case AlertStatusFiring:
			// The alert was never cleared — no second 🚨 (source flapping
			// does not spam).
			return episodeFiring, episodeActNone
		case AlertStatusOK:
			// The measurements recovered straight to ok — the honest ✅.
			return episodeIdle, episodeActClose
		case AlertStatusNotMeasured:
			// The hold — the edge was noted on entering this state
			// (29#P3-4).
			return episodeFiringUnmeasured, episodeActNone
		}
	}
	return st, episodeActNone
}

// onEpisodeSendFailed — the delivery-failure event of the FSM: the opening
// message of an open episode was not delivered — the episode re-opens the
// delivery (the next firing pass enqueues the 🚨 again); the closed/idle
// states ignore it (a lost ✅ is only a Warn: the episode is closed, and a
// recovery reported hours later would mislead — the transition itself is in
// the journal by logFront).
func onEpisodeSendFailed(st episodeState) episodeState {
	switch st {
	case episodeFiring, episodeFiringUnmeasured:
		return episodeFiringUndelivered
	}
	return st
}

// telegramMessage — one delivery item: the text and, for episode-bound
// messages, the alert name, the kind and the episode generation — the
// delivery failure of the opening message must return to the FSM as its
// event, and only to the episode it belongs to (a stale event of an
// already closed episode is ignored, 29#P3-2).
type telegramMessage struct {
	alert string // "" — an arbitrary text (Send), not episode-bound
	kind  string
	gen   int // the episode generation (epGen) at the enqueue time
	text  string
}

// telegramPacer — the token bucket of the send rate (a single consumer —
// the worker; no locking): burst tokens immediately, then one token per
// every; the idle time refills the bucket up to burst.
type telegramPacer struct {
	burst  int
	every  time.Duration
	tokens float64
	last   time.Time
}

// delay — the pause before the next send (a token is consumed; 0 — now).
func (p *telegramPacer) delay(now time.Time) time.Duration {
	if p.last.IsZero() {
		p.last, p.tokens = now, float64(p.burst)
	} else if elapsed := now.Sub(p.last); elapsed > 0 {
		if refill := elapsed.Seconds() / p.every.Seconds(); p.tokens+refill < float64(p.burst) {
			p.tokens += refill
		} else {
			p.tokens = float64(p.burst)
		}
		p.last = now
	}
	if p.tokens >= 1 {
		p.tokens--
		return 0
	}
	wait := (1 - p.tokens) * p.every.Seconds()
	p.tokens = 0
	// The borrowed token: the next refill counts from now+wait — the send
	// that sleeps the wait consumes exactly it.
	p.last = now.Add(time.Duration(wait * float64(time.Second)))
	return time.Duration(wait * float64(time.Second))
}

// telegramFailure — the classification of one failed script call.
type telegramFailure struct {
	transient  bool
	retryAfter time.Duration // >0 — the parsed Retry-After (HTTP 429)
}

// telegramRetryAfterRe — the Retry-After of the Telegram API in the script's
// error output ("telegram: API: 429: Too Many Requests: retry after 7" —
// deploy/telegram-notify.sh prints the API description on stderr).
var telegramRetryAfterRe = regexp.MustCompile(`retry after (\d+)`)

// classifyTelegramFailure — the transience of one failed script call by its
// combined output (the error format of deploy/telegram-notify.sh): HTTP 429
// (the rate limit; the pause — the API's Retry-After, or
// telegramRetryAfterUnparsed when the output carries none), 5xx and HTTP 000
// (a curl transport error: the proxy is down, a connect timeout) —
// transient, retried with a backoff; the rest (400/403/… — a bad request, a
// revoked token) — permanent, a single attempt.
func classifyTelegramFailure(out string) telegramFailure {
	if strings.Contains(out, "HTTP 429") {
		f := telegramFailure{transient: true, retryAfter: telegramRetryAfterUnparsed}
		if m := telegramRetryAfterRe.FindStringSubmatch(out); m != nil {
			if sec, err := strconv.Atoi(m[1]); err == nil && sec > 0 {
				d := time.Duration(sec) * time.Second
				if d > telegramRetryAfterMax {
					d = telegramRetryAfterMax // the API named a longer pause than the ceiling — clamp to it
				}
				f.retryAfter = d
			}
		}
		return f
	}
	if strings.Contains(out, "HTTP 5") || strings.Contains(out, "HTTP 000") {
		return telegramFailure{transient: true}
	}
	return telegramFailure{}
}

// NewTelegramNotifier decides whether the delivery is on: explicitly off,
// no environment secrets, the script unavailable/not executable — no message
// recipient appears; every reason is a journal entry (Info for
// expected states, a Warn for a broken production host setup). An enabled
// delivery starts the serialized worker (the file header, T-290).
func NewTelegramNotifier(opts TelegramOptions) *TelegramNotifier {
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	n := &TelegramNotifier{script: "", log: log, state: make(map[string]episodeState)}
	if !opts.Enabled {
		log.Info("telegram: alert delivery off (monitor.alerts.telegram=false)")
		return n
	}
	if getenv(EnvTelegramBotToken) == "" || getenv(EnvTelegramChatID) == "" {
		log.Info("telegram: alert delivery off — TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set " +
			"(the unit's EnvironmentFile, deploy/systemd/crawler.env.example); alerts stay Warn in the monitor journal")
		return n
	}
	script := opts.Script
	if script == "" {
		log.Warn("telegram: alert delivery off — the script path is not set (monitor.alerts.telegram_script)")
		return n
	}
	if st, err := os.Stat(script); err != nil || st.IsDir() || st.Mode()&0o111 == 0 {
		log.Warn("telegram: alert delivery off — the script is unavailable or not executable",
			"script", script, "err", err)
		return n
	}
	n.script = script
	n.epGen = make(map[string]int)
	n.undeliveredKind = make(map[string]string)
	// The options knobs (the test hooks, 29#P3-9) override the production
	// values before the worker starts; zero values — the defaults.
	burst, every := telegramSendBurst, telegramSendEvery
	if opts.paceBurst > 0 {
		burst = opts.paceBurst
	}
	if opts.paceEvery > 0 {
		every = opts.paceEvery
	}
	backoff := telegramRetryBackoff
	if opts.retryBackoff > 0 {
		backoff = opts.retryBackoff
	}
	drain := telegramDrainBudget
	if opts.drainBudget > 0 {
		drain = opts.drainBudget
	}
	queueCap := telegramQueueCap
	if opts.queueCap > 0 {
		queueCap = opts.queueCap
	}
	n.queue = make(chan telegramMessage, queueCap)
	n.failedC = make(chan telegramMessage, queueCap)
	n.done = make(chan struct{})
	n.pacer = telegramPacer{burst: burst, every: every}
	n.retryBackoff = backoff
	n.drainBudget = drain
	n.wg.Add(1)
	go n.work()
	log.Info("telegram: alert delivery on", "script", script)
	return n
}

// OnPass — the AlertsOptions.OnPass hook: an alert episode (firing … clearing)
// is delivered by at most two messages — 🚨 at the opening and ✅ at the
// closing; the base pass of an inherited incident — the informational ⏳
// (12#P2-2). Debouncing: repeated passes of a fired alert, a return to
// firing after a source interruption and the noise of unfired alerts —
// silence. An opening that was not delivered — the transport exhausted its
// retries, or the intake queue was full (29#P2-1) — moves the episode to
// firing-undelivered: the next firing pass re-attempts it, with the same
// generation and the same kind (an inherited ⏳ is re-sent as ⏳, 29#P3-1).
func (n *TelegramNotifier) OnPass(res AlertsResult) {
	if n.script == "" {
		return
	}
	// The delivery-failure events of the previous passes first: an open
	// episode whose opening message was not delivered re-attempts it on
	// this pass (the worker reports, the pass applies — the state map
	// keeps its single writer).
	n.applyFailures()
	for _, a := range res.Alerts {
		prev := n.state[a.Name]
		st, act := nextEpisode(prev, a.Status)
		n.state[a.Name] = st
		switch act {
		case episodeActOpen, episodeActInherit:
			kind := telegramMsgFiring
			if act == episodeActInherit {
				kind = telegramMsgInherited
			}
			if prev == episodeFiringUndelivered {
				// The re-attempt of an undelivered opening: the same
				// episode (the generation stays), the same kind.
				if k, ok := n.undeliveredKind[a.Name]; ok {
					kind = k
				}
			} else {
				n.epGen[a.Name]++
			}
			if n.enqueue(telegramMessage{alert: a.Name, kind: kind, gen: n.epGen[a.Name],
				text: n.openingText(kind, a)}) {
				delete(n.undeliveredKind, a.Name)
			} else {
				// A full/closed queue must not eat the episode (29#P2-1):
				// back to firing-undelivered — the next pass re-attempts.
				n.state[a.Name] = episodeFiringUndelivered
				n.undeliveredKind[a.Name] = kind
			}
		case episodeActClose:
			n.enqueue(telegramMessage{alert: a.Name, kind: telegramMsgRecovered,
				text: fmt.Sprintf("✅ Recovered: %s (%s)\n%s", a.Name, alertDesc(a.Name), n.detail(a.Name, a.Detail))})
			delete(n.undeliveredKind, a.Name) // hygiene: the episode is over
		case episodeActNoteUnmeasured:
			// Without a message — see the file header: a recovery is not proven,
			// and source flapping must neither spam nor close the episode.
			n.log.Info("telegram: firing alert no longer measured — no message",
				"alert", a.Name, "reason", a.Detail)
		}
		if prev == episodeFiringUndelivered && st != episodeFiringUndelivered && act != episodeActOpen {
			// The episode left the undelivered state without a re-attempt
			// (the silent close of 29#P2-2) — the pending kind is void.
			delete(n.undeliveredKind, a.Name)
		}
	}
}

// openingText — the text of an episode opening: 🚨 for a fresh firing, ⏳
// for an episode inherited at the monitor start (the re-attempt of an
// undelivered opening keeps the original kind, 29#P3-1).
func (n *TelegramNotifier) openingText(kind string, a AlertSnapshot) string {
	if kind == telegramMsgInherited {
		return fmt.Sprintf("⏳ Still firing at the monitor start: %s (%s)\n%s",
			a.Name, alertDesc(a.Name), n.detail(a.Name, a.Detail))
	}
	return fmt.Sprintf("🚨 Alert %s: %s\n%s", a.Name, alertDesc(a.Name), n.detail(a.Name, a.Detail))
}

// applyFailures — drains the worker's delivery-failure events into the
// episode states (the start of OnPass: the state map keeps a single
// writer). An event carries its episode's generation: by the time a long
// Retry-After exhausts, the episode may have closed and a new one opened —
// a stale event must not mark the new episode undelivered (a duplicate 🚨,
// 29#P3-2); a fresh event also records the opening kind for the re-attempt
// (29#P3-1).
func (n *TelegramNotifier) applyFailures() {
	for {
		select {
		case m := <-n.failedC:
			if m.gen != n.epGen[m.alert] {
				continue // a stale event of an already closed/reopened episode
			}
			st := onEpisodeSendFailed(n.state[m.alert])
			n.state[m.alert] = st
			if st == episodeFiringUndelivered {
				n.undeliveredKind[m.alert] = m.kind
			}
		default:
			return
		}
	}
}

// detail — the alert detail for the message text, truncated to the budget
// with a pointer to the journal (12#P3-1): the full text stays in the
// monitor log — logFront writes it on every transition; the truncation
// itself is noted at Debug.
func (n *TelegramNotifier) detail(alert, d string) string {
	if utf8.RuneCountInString(d) <= telegramDetailLimit {
		return d
	}
	n.log.Debug("telegram: the detail is over the message budget — truncated; the full text is in the monitor journal",
		"alert", alert, "runes", utf8.RuneCountInString(d))
	return string([]rune(d)[:telegramDetailLimit]) + "… (full text in the monitor journal)"
}

// alertDesc — the alert description for the message text (alertDescs).
func alertDesc(name string) string {
	if d, ok := alertDescs[name]; ok {
		return d
	}
	return name
}

// Send — an arbitrary message to the operator (the periodic load report
// of T-173, the slow-query notifications of T-177): the same serialized
// queue, secrets and timeout as the alerts; the delivery off (no
// secrets/script — script == "") — a no-op. Safe for a concurrent call
// (the guarded bounded intake); the text is the caller's responsibility
// (the reports are bounded by construction).
func (n *TelegramNotifier) Send(text string) {
	if n.script == "" {
		return
	}
	n.enqueue(telegramMessage{kind: telegramMsgArbitrary, text: text})
}

// enqueue — the bounded intake: under the cap the message is queued (the
// worker paces the actual sends); a full queue or a closed delivery — a Warn
// and a drop (the alert loop must never block on the delivery). The return
// reports the drop: the caller returns an episode opening to the FSM
// (firing-undelivered) — a lost 🚨 is re-attempted, not lost (29#P2-1).
func (n *TelegramNotifier) enqueue(m telegramMessage) bool {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		n.log.Warn("telegram: message dropped — the delivery is shutting down", "kind", m.kind, "alert", m.alert)
		return false
	}
	select {
	case n.queue <- m:
		n.mu.Unlock()
		return true
	default:
		n.mu.Unlock()
		n.log.Warn("telegram: the delivery queue is full — the message is dropped",
			"kind", m.kind, "alert", m.alert, "queue", cap(n.queue))
		return false
	}
}

// work — the single delivery worker: the messages leave strictly in the
// enqueue order, paced by the token bucket; the shutdown (Close) stops the
// intake, lets the in-flight message finish and drops the rest.
func (n *TelegramNotifier) work() {
	defer n.wg.Done()
	for {
		select {
		case <-n.done:
			n.dropQueued()
			return
		default:
		}
		select {
		case m := <-n.queue:
			n.deliver(m)
		case <-n.done:
			n.dropQueued()
			return
		}
	}
}

// dropQueued — the shutdown tail: the messages not yet picked up are
// dropped (draining the whole queue with its retries would stretch the
// service stop to minutes; the alert transitions stay in the journal).
func (n *TelegramNotifier) dropQueued() {
	dropped := 0
	for {
		select {
		case <-n.queue:
			dropped++
		default:
			if dropped > 0 {
				n.log.Warn("telegram: queued messages dropped at shutdown", "dropped", dropped)
			}
			return
		}
	}
}

// deliver — one message: the pacer pause, then the script attempts with the
// transient-error backoff (12#P2-3). The text is the only --message argument
// (an exec without a shell: the text is not interpreted, quotes/newlines are
// preserved; the script takes the secrets from the inherited monitor
// environment and does not print them into the output). A definitive failure
// of an episode opening (🚨/⏳) is reported to the FSM (reportFailure).
func (n *TelegramNotifier) deliver(m telegramMessage) {
	if d := n.pacer.delay(time.Now()); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-n.done:
			t.Stop()
			n.log.Warn("telegram: send skipped — shutting down", "kind", m.kind, "alert", m.alert)
			n.reportFailure(m)
			return
		}
	}
	backoff := n.retryBackoff
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), telegramNotifyTimeout)
		out, err := exec.CommandContext(ctx, n.script, "--message", m.text).CombinedOutput()
		// The timeout verdict is taken BEFORE cancel(): our own cancel
		// would make ctx.Err() non-nil on every path.
		timedOut := ctx.Err() != nil
		cancel()
		if err == nil {
			n.log.Debug("telegram: message sent", "kind", m.kind, "alert", m.alert,
				"attempt", attempt, "output", strings.TrimSpace(string(out)))
			return
		}
		f := classifyTelegramFailure(string(out))
		transient := f.transient || timedOut // a hung script — the same transport class
		if !transient || attempt >= telegramSendAttempts {
			n.log.Warn("telegram: message delivery failed",
				"kind", m.kind, "alert", m.alert, "attempts", attempt,
				"err", err, "output", strings.TrimSpace(string(out)))
			n.reportFailure(m)
			return
		}
		wait := backoff
		if f.retryAfter > 0 {
			wait = f.retryAfter // the API named its pause — honor it
		} else {
			backoff *= 2
		}
		n.log.Warn("telegram: a transient send failure — retrying",
			"kind", m.kind, "alert", m.alert, "attempt", attempt, "in", wait.String(),
			"err", err, "output", strings.TrimSpace(string(out)))
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-n.done:
			t.Stop()
			n.log.Warn("telegram: send interrupted — shutting down", "kind", m.kind, "alert", m.alert)
			n.reportFailure(m)
			return
		}
	}
}

// reportFailure — the ultimate failure of a message: an episode-bound
// OPENING message (🚨/⏳) returns to the FSM as its event — with its
// generation and kind (the next firing pass re-attempts the delivery the
// same way it was opened); a lost ✅ is only a Warn — see
// onEpisodeSendFailed.
func (n *TelegramNotifier) reportFailure(m telegramMessage) {
	if m.alert == "" || (m.kind != telegramMsgFiring && m.kind != telegramMsgInherited) {
		return
	}
	select {
	case n.failedC <- m:
	default: // full — bounded: the next failed pass repeats the event
	}
}

// Close — the shutdown drain (12#P2-3): the intake stops, the in-flight
// send finishes (awaited via the WaitGroup), the queued-but-unsent are
// dropped with a Warn — all within n.drainBudget: the budget is smaller
// than the service framework's graceful window
// (service.DefaultShutdownTimeout, 10 s), so a hung script truncates the
// drain with an explicit Warn instead of stretching the service stop past
// its own deadline and into systemd's TimeoutStopSec (29#P2-3); the
// orphaned script child finishes on its own (curl --max-time 30 inside).
// Idempotent; a no-op for a disabled delivery. The service calls it at its
// own shutdown (cmd/monitor, after its loops) — binding the sender's
// lifetime to the service ctx.
func (n *TelegramNotifier) Close() {
	if n.script == "" {
		return
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	n.mu.Unlock()
	close(n.done)
	drained := make(chan struct{})
	go func() {
		n.wg.Wait()
		close(drained)
	}()
	timer := time.NewTimer(n.drainBudget)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
		n.log.Warn("telegram: drain truncated — the in-flight send did not finish within the budget",
			"budget", n.drainBudget.String())
	}
}

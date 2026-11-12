// Reloader — reloading the Laravel rule set on SIGHUP (spec 5.8):
// "the rules are reloaded without a restart". The signal is caught by the
// internal/service framework (Options.OnSIGHUP, make rules-reload), which
// calls HandleSIGHUP; the Reloader itself is signal-agnostic — the reload
// entry point is injected by the analyzer service (T-071).

package rules

import (
	"log/slog"
	"sync"
)

// subBuf — the subscription channel capacity: a subscriber receives the
// freshest set; if it has not taken the previous one, that one is replaced
// (a slow consumer still reads the current set via Set).
const subBuf = 1

// Reloader holds the current rule set from the directory and notifies
// subscribers about changes. Safe for concurrent use: consumers (analyzer
// workers) read the snapshot via Set, a reload replaces it wholesale —
// the rules inside a Set never mutate.
type Reloader struct {
	dir    string
	logger *slog.Logger
	// load — a seam for tests (errors/substitutions without a file
	// system); production — LoadDir.
	load func(dir string) (*Set, error)

	mu   sync.Mutex
	set  *Set
	subs map[chan *Set]struct{}
}

// NewReloader loads the initial set from dir. An initial load error is
// returned to the caller: a service without a valid rule set does not
// start (otherwise the analyzer would silently miss Laravel).
func NewReloader(dir string, logger *slog.Logger) (*Reloader, error) {
	r := &Reloader{
		dir:    dir,
		logger: logger,
		load:   LoadDir,
		subs:   make(map[chan *Set]struct{}),
	}
	if r.logger == nil {
		r.logger = slog.New(slog.DiscardHandler)
	}
	set, err := r.load(dir)
	if err != nil {
		return nil, err
	}
	r.set = set
	r.logger.Info("rule set loaded", "rules_dir", dir, "rules", len(set.Rules), "rules_version", set.Version)
	return r, nil
}

// Set — the current snapshot of the set (immutable; the reference can be
// held without locks, including after a reload).
func (r *Reloader) Set() *Set {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.set
}

// Reload re-reads the directory. A load error (no directory, broken YAML,
// invalid set) does NOT change the current set and does not stop the
// service — the previous rules keep working (a Warn in the log); the error
// is returned for tests and callers that need it. A successful reload with
// the same rules_version is a no-op: the set has not changed behaviorally.
func (r *Reloader) Reload() error {
	set, err := r.load(r.dir)
	if err != nil {
		r.logger.Warn("rule reload failed — the previous set stays active",
			"rules_dir", r.dir, "err", err)
		return err
	}

	r.mu.Lock()
	if set.Version == r.set.Version {
		r.mu.Unlock()
		r.logger.Info("rule set unchanged", "rules_version", set.Version)
		return nil
	}
	old := r.set
	r.set = set
	chans := make([]chan *Set, 0, len(r.subs))
	for ch := range r.subs {
		chans = append(chans, ch)
	}
	r.mu.Unlock()

	// The compilation cache lives together with the set: it is cleared on a
	// rules_version change — entries of removed rules' patterns (and the
	// settled compilation errors of broken revisions) do not accumulate in
	// the process.
	resetCompileCache()

	r.logger.Info("rule set reloaded",
		"rules_dir", r.dir, "rules", len(set.Rules),
		"rules_version", set.Version, "prev_version", old.Version)
	for _, ch := range chans {
		notify(ch, set)
	}
	return nil
}

// HandleSIGHUP — the entry point for service.Options.OnSIGHUP: a reload on
// signal without a service restart. The error is already logged by Reload —
// the signal is not an error source, the framework keeps running.
func (r *Reloader) HandleSIGHUP() {
	_ = r.Reload()
}

// Subscribe — a subscription to set changes (for example, the re-analysis
// scheduler of T-074 starts a crawl when rules_version changes). The
// channel is buffered: if a notification is missed, the current set is
// still available via Set. cancel unsubscribes; after it returns, the
// channel no longer receives messages.
func (r *Reloader) Subscribe() (<-chan *Set, func()) {
	ch := make(chan *Set, subBuf)
	r.mu.Lock()
	r.subs[ch] = struct{}{}
	r.mu.Unlock()
	cancel := func() {
		r.mu.Lock()
		delete(r.subs, ch)
		r.mu.Unlock()
	}
	return ch, cancel
}

// notify delivers the set to a subscriber without blocking: if the channel
// is full, the subscriber has not taken the previous notification yet and
// it is replaced with the fresh one.
func notify(ch chan *Set, set *Set) {
	select {
	case ch <- set:
	default:
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- set:
		default:
		}
	}
}

package render

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Pool — parallel processing of render tasks (render spec 5.6: "one browser,
// several isolated contexts, 2-4 in parallel by default"): at most
// Options.Contexts tasks at a time, each in a fresh isolated profile.
// Sessions are not reused: the context is recreated for every task and no
// state is carried across pages (render spec §9).
type Pool struct {
	b *Browser

	// slots — the concurrency semaphore: capacity = Options.Contexts.
	slots chan struct{}
	// done is closed on Close: tasks waiting for a slot bail out at once.
	done chan struct{}
	// wg tracks tasks in flight; Close lets them finish (render spec 3.4).
	// Invariant against the WaitGroup race: wg.Add in Run happens only under
	// mu and only while closed=false, and Wait in Close starts AFTER retire —
	// that is, after mu has guaranteed there will be no new Adds (an Add on
	// a zero counter concurrent with Wait is forbidden by the sync.WaitGroup
	// docs; the CI incident of Nov 11, 2026 — a race in
	// TestGenStateLifecycle).
	mu     sync.Mutex
	wg     sync.WaitGroup
	closed atomic.Bool

	// newSession — the session factory; replaced by fakes in unit tests.
	newSession func(ctx context.Context) (renderSession, error)
}

// renderSession — the minimal session contract for the pool (*Session
// satisfies it).
type renderSession interface {
	Run(ctx context.Context, task RenderTask) (RenderOutput, error)
	Close()
}

// NewPool — a pool over the running browser b; parallelism is taken from
// b.opts.Contexts. One pool per browser: a browser restart (T-065) means
// a new browser and a new pool over it.
func NewPool(b *Browser) *Pool {
	p := &Pool{
		b:     b,
		slots: make(chan struct{}, b.opts.Contexts),
		done:  make(chan struct{}),
	}
	p.newSession = func(ctx context.Context) (renderSession, error) {
		return b.NewSession(ctx)
	}
	return p
}

// Browser returns the pool's browser (the page counter and Alive feed the
// worker loop of T-067 and the restarts of T-065).
func (p *Pool) Browser() *Browser { return p.b }

// Run executes the task in a free isolated context, blocking until a slot
// frees up or ctx is cancelled. A ClassContext error (a dead context) is
// retried once on a fresh context — unless the pool was retired meanwhile
// (a generation swap): then the error is wrapped in ErrClosed and handed
// to the manager, which retries the task on the new generation (a retry
// inside the retiring pool is pointless — its browser closes after the
// drain). Page errors and an unavailable browser are returned to the
// caller — redelivery stays with the queue level (T-066), browser
// restart is T-065.
func (p *Pool) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	// In-flight registration happens under mu and only for a live pool:
	// serialized with retire (see the wg invariant above) — Add never
	// races with Wait.
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return RenderOutput{}, &Error{Class: ClassTask, URL: task.URL, Err: ErrClosed}
	}
	p.wg.Add(1)
	p.mu.Unlock()
	defer p.wg.Done()
	select {
	case p.slots <- struct{}{}:
		// Close may have happened while racing with the select (select
		// picks a random ready case): recheck after taking the slot.
		select {
		case <-p.done:
			<-p.slots
			return RenderOutput{}, &Error{Class: ClassTask, URL: task.URL, Err: ErrClosed}
		default:
		}
	case <-p.done:
		return RenderOutput{}, &Error{Class: ClassTask, URL: task.URL, Err: ErrClosed}
	case <-ctx.Done():
		return RenderOutput{}, &Error{Class: ClassTask, URL: task.URL, Err: ctx.Err()}
	}
	defer func() { <-p.slots }()

	out, err := p.attempt(ctx, task)
	if err == nil {
		p.b.pages.Add(1)
		return out, nil
	}
	if ClassOf(err) == ClassContext {
		if p.closed.Load() {
			// The generation was swapped between the failure and the
			// retry decision: retrying inside the retiring pool is
			// pointless, the error goes to the manager (05#P2-1).
			return out, retiredContextErr(task.URL, err)
		}
		// Logging mutant (T-M07): the retry Warn does not change the
		// observable behavior — killed per §4 of docs/mutation-testing.md
		// (logging is not tested).
		// mutator-disable-next-line statement/remove
		slog.Warn("render context died, retrying the task on a new context",
			"url", task.URL, "err", err.Error())
		if out, err = p.attempt(ctx, task); err == nil {
			p.b.pages.Add(1)
			return out, nil
		}
		if ClassOf(err) == ClassContext && p.closed.Load() {
			// The same race, one window later: the generation was
			// retired during the pool's own retry attempt.
			return out, retiredContextErr(task.URL, err)
		}
	}
	return out, err
}

// retiredContextErr — the ClassContext error of a task whose pool was
// retired (a generation swap by the manager, T-065) between the failure
// and the retry decision. The class is kept — the failure itself is
// transient (a dead tab heals on a fresh context); the ErrClosed wrap is
// what Manager.Run's retry filter looks for: without it a raw
// ClassContext escaped upward (the manager retries only ClassBrowser and
// ErrClosed), and a healable error burned one of the queue's
// redis.max_attempts deliveries (05#P2-1 — the contract "a pool closure
// by a generation swap means a restart and a retry" now holds for a task
// mid-flight too, not only for one that did not start). The wrap keeps
// the cause in the chain (%w: %w — the review P3-1: %v would cut
// errors.Is/As of the original, e.g. context.Canceled) and wraps the
// inner error's cause, not its *Error shell — the shell's class and URL
// are already re-stated by this wrap.
func retiredContextErr(url string, err error) error {
	cause := err
	var e *Error
	if errors.As(err, &e) && e.Err != nil {
		cause = e.Err
	}
	return &Error{Class: ClassContext, URL: url,
		Err: fmt.Errorf("%w: %w", ErrClosed, cause)}
}

// attempt — a single attempt: a fresh isolated context, the task, closing
// the profile regardless of the outcome (no state outlives the page).
func (p *Pool) attempt(ctx context.Context, task RenderTask) (RenderOutput, error) {
	sess, err := p.newSession(ctx)
	if err != nil {
		return RenderOutput{}, err
	}
	defer sess.Close()
	return sess.Run(ctx, task)
}

// Close stops the pool: new tasks are rejected (ErrClosed), current ones
// finish — each bounded by Options.Timeout (render spec 3.4) — and then the
// browser is stopped. Calling it again is harmless.
func (p *Pool) Close() {
	p.retire()
	p.wg.Wait()
	p.b.Close()
}

// retire closes the pool to new tasks (ErrClosed) without waiting for the
// current ones: the manager (T-065) takes the replaceable generation out of
// service synchronously with the restart decision, leaving task completion
// and the browser stop to the background Close (the "task landed in the
// pool being closed after acquire" races are covered by the retry in
// Manager.Run; since T-291 a context failure raced by the retire is
// wrapped in ErrClosed there too — the retry on the new generation,
// 05#P2-1).
func (p *Pool) retire() {
	// Under mu: atomicity of CAS + close(done) against wg.Add in Run (see
	// the wg invariant at the pool fields); Wait in Close starts after this
	// returns — no concurrent Add on a zero counter can happen anymore.
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.done)
}

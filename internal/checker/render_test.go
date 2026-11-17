// Tests of the SPA render degradation policy of the live check
// (renderSPA, 16#P3-4 of the bulk review): a render error of ANY class
// leaves a trace — a Warn with the class and the class marker for the
// record; only the browser class damps the render until the end of the
// check (T-220), the page/context classes do not. The render call is the
// spaRenderer seam: no Chromium, no network — the errors of every class
// are constructed directly (the classification lives in the render
// package and is pinned by its own tests).

package checker

import (
	"context"
	"errors"
	"testing"

	"crawler/internal/render"
)

// spaFake — the render seam stub: one answer of Run (an output or an
// error), the calls are counted.
type spaFake struct {
	out   render.RenderOutput
	err   error
	calls int
}

func (f *spaFake) Run(context.Context, render.RenderTask) (render.RenderOutput, error) {
	f.calls++
	return f.out, f.err
}

// warnLog — the Logger capture: the Warn messages (the Info is dropped).
type warnLog struct {
	warns []string
}

func (l *warnLog) Info(string, ...any) {}
func (l *warnLog) Warn(msg string, _ ...any) {
	l.warns = append(l.warns, msg)
}

// TestRenderSPA — the policy table: a success passes the output without
// a marker and without damping; the browser class marks "browser",
// warns and damps; the page (task) and context classes mark and warn
// WITHOUT damping — one bad page must not turn off the render for the
// rest of the records (16#P3-4: before the fix these classes were
// completely silent); a foreign error (not *render.Error) is task by
// ClassOf.
func TestRenderSPA(t *testing.T) {
	ctx := context.Background()

	// A success.
	fake := &spaFake{out: render.RenderOutput{DOM: "<html>dom</html>"}}
	log := &warnLog{}
	out, ok, down, errClass := renderSPA(ctx, fake, "https://x/", log)
	if !ok || down || errClass != "" {
		t.Fatalf("a success: (ok, down, errClass) = (%v, %v, %q), want (true, false, \"\")", ok, down, errClass)
	}
	if out.DOM != "<html>dom</html>" {
		t.Errorf("a success: the output DOM = %q, want the render output", out.DOM)
	}
	if len(log.warns) != 0 {
		t.Errorf("a success: Warn = %v, want none", log.warns)
	}

	// The classes: the browser one damps, the task/context ones do not;
	// every class warns and marks the record.
	for _, tc := range []struct {
		name     string
		err      error
		wantDown bool
	}{
		{"browser", &render.Error{Class: render.ClassBrowser, Err: errors.New("no chromium")}, true},
		{"task", &render.Error{Class: render.ClassTask, Err: errors.New("page timeout")}, false},
		{"context", &render.Error{Class: render.ClassContext, Err: errors.New("tab crashed")}, false},
		{"a foreign error is task", errors.New("not a render error"), false},
	} {
		fake := &spaFake{err: tc.err}
		log := &warnLog{}
		out, ok, down, errClass := renderSPA(ctx, fake, "https://x/", log)
		if ok {
			t.Fatalf("%s: ok = true, want false", tc.name)
		}
		if down != tc.wantDown {
			t.Errorf("%s: down = %v, want %v", tc.name, down, tc.wantDown)
		}
		wantClass := render.ClassOf(tc.err).String()
		if errClass != wantClass {
			t.Errorf("%s: errClass = %q, want %q", tc.name, errClass, wantClass)
		}
		if out.DOM != "" {
			t.Errorf("%s: the output DOM = %q, want empty (no output on an error)", tc.name, out.DOM)
		}
		if len(log.warns) != 1 {
			t.Fatalf("%s: Warn calls = %d (%v), want 1 — an error of ANY class leaves a trace (16#P3-4)",
				tc.name, len(log.warns), log.warns)
		}
	}
}

// TestRenderSPAStillRunsAfterPageError — the policy consequence in the
// records loop: after a page-class failure the render is not damped, so
// the loop's condition (!renderDown) still lets the next record render.
// Pinned at the helper level: a second call after a task-class error
// still reaches the renderer (the browser class would have returned
// down=true and the loop would have skipped the rest).
func TestRenderSPAStillRunsAfterPageError(t *testing.T) {
	fake := &spaFake{err: &render.Error{Class: render.ClassTask, Err: errors.New("net::ERR_TIMED_OUT")}}
	_, _, down1, _ := renderSPA(context.Background(), fake, "https://a/", &warnLog{})
	if down1 {
		t.Fatal("a task-class error damped the render, want false (only the browser class damps)")
	}
	if _, _, _, _ = renderSPA(context.Background(), fake, "https://b/", &warnLog{}); fake.calls != 2 {
		t.Fatalf("the renderer calls = %d, want 2 (a page error does not stop the next record)", fake.calls)
	}
}

// Unit tests of the "render impossible" marker subdomain_results.render_failed_at
// (T-210): the finalizations without a render (the tarpit T-148 / the cert class T-159 /
// the task class T-172) stamp the row — until the content changes the rework
// pass (httpworker/renderq.go) does not re-publish the entry. The reset — by a
// content change: an episode rewrite (DELETE + INSERT of the result record,
// T-058 — in the httpworker unit tests the column is not in the INSERT) and
// a successful render (renderUpdateSQL). The stamp guards — the same principles
// as renderUpdateSQL's: the optimistic one on content_hash (the row slipped
// out from under the task — no marker) and rendered = false (a concurrent
// render settled the question). The fake environment — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"testing"

	"crawler/internal/queue"
	"crawler/internal/render"
)

// rwMarkErr — a retryable render error of the task class (not a tarpit/cert —
// processDelivery decides the retries, only the finalization matters here).
func rwMarkErr(url string) error {
	return &render.Error{Class: render.ClassTask, URL: url,
		Err: errors.New("net::ERR_CONNECTION_REFUSED")}
}

// TestRenderWorker_FinalizeMarksRenderFailed — all three finalization
// policies set the marker: rendered = false, the content_hash is the old one,
// the render_failed_at stamp is not nil, q:analyze is published, no
// transaction is opened (the marker — an autocommit UPDATE, T-210).
func TestRenderWorker_FinalizeMarksRenderFailed(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		attempt int
	}{
		{"a tarpit (T-148)", rwTarpitErr("https://spa.example/"), 2},
		{"the cert class (T-159)", rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID"), 1},
		{"the task class (T-172)", rwMarkErr("https://spa.example/"), 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			f.renderer.err = c.err

			if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), c.attempt); err != nil {
				t.Fatalf("finalization: %v", err)
			}
			row := rwRowSnapshot(f.pool, rwKey)
			if row.failedAt == nil {
				t.Error("render_failed_at must be set by the finalization — without the marker the rework pass republishes the record (T-210)")
			}
			if row.rendered {
				t.Error("rendered must stay false — the record outcome is the raw HTML of the HTTP phase")
			}
			if row.hash == nil || string(row.hash) != string(raw[:]) {
				t.Errorf("content_hash = %v, the previous raw HTML reference is needed %s", row.hash, raw)
			}
			if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
				t.Errorf("the publications %+v, one q:analyze is needed (the pipeline continues with analysis)", pubs)
			}
			if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
				t.Errorf("a transaction must not open — the marker is an autocommit (an event on %d)", i)
			}
		})
	}
}

// TestRenderWorker_FinalizeMarkerStaleRow — the stamp's content guard:
// the recheck rewrote the row between the target SELECT and the marker — the stamp
// is NOT set (the new episode has the right to a render), the finalization
// publishes q:analyze of the current row and acknowledges the delivery.
func TestRenderWorker_FinalizeMarkerStaleRow(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
	fresh := rwTestHash(2)
	f.pool.mutateBeforeMark = func(p *rwFakePool) {
		if r, ok := p.rows[rwKey]; ok {
			p.refc[raw]-- // DELETE of the episode's old rows (SwapRef, T-058)
			r.hash = fresh[:]
			p.refc[fresh]++ // INSERT of the new rows
		}
	}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("finalization with a gone row is not an error (like the render guard): %v", err)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt != nil {
		t.Error("the marker is not set for a row of new content — the content_hash guard (T-210)")
	}
	if row.hash == nil || string(row.hash) != string(fresh[:]) {
		t.Errorf("content_hash = %v, the content of the new episode is needed %s", row.hash, fresh)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
		t.Errorf("q:analyze publications = %d, 1 is needed (the analysis of the current row is harmless)", len(pubs))
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — processDelivery does not acknowledge itself (the XACK is Processings)", acked)
	}
}

// TestRenderWorker_FinalizeMarkerRenderedRow — a concurrent re-delivery
// has already rendered the row: it needs no stamp (the
// rendered = false guard), the finalization publishes q:analyze — an analysis
// duplicate with the same content is harmless (at-least-once, spec 3.4).
func TestRenderWorker_FinalizeMarkerRenderedRow(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	f.pool.mutateBeforeMark = func(p *rwFakePool) {
		p.rows[rwKey].rendered = true // a concurrent delivery wrote the DOM
	}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("finalization of a rendered row is not an error: %v", err)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt != nil {
		t.Error("render_failed_at is not set on a rendered row — the render question is closed (the invariant: marker ⇒ rendered = false)")
	}
	if !row.rendered {
		t.Error("the rendered of a concurrent delivery must be preserved")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
		t.Errorf("q:analyze publications = %d, 1 is needed", len(pubs))
	}
}

// TestRenderWorker_FinalizeMarkerExecError — the marker UPDATE failure: an
// error without an XACK — the delivery will return and the finalization will repeat (the
// stamp will be re-set idempotently).
func TestRenderWorker_FinalizeMarkerExecError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
	f.pool.markErr = errors.New("no connection")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
		t.Fatal("a marker failure must be returned (a re-delivery will complete the finalization)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications before the marker: %+v", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the marker did not take (an UPDATE failure)")
	}
}

// TestRenderWorker_FinalizeMarkerRepeatIdempotent — a publication failure
// after the stamp: the delivery returns, the repeated finalization
// re-sets the stamp (an UPDATE of the same row) and publishes again —
// the duplicate is bounded by the attempts, the analyzer is idempotent by the key.
func TestRenderWorker_FinalizeMarkerRepeatIdempotent(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
	f.q.publishErrs = []error{errors.New("redis unavailable")}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
		t.Fatal("a publication failure — a re-delivery error")
	}
	first := rwRowSnapshot(f.pool, rwKey)
	if first.failedAt == nil {
		t.Fatal("the marker is set before the publication (a publication failure does not roll the stamp back)")
	}
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("the repeated finalization: %v", err)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt == nil {
		t.Error("the marker must be in place after the repeated finalization")
	}
	if !row.failedAt.Equal(*first.failedAt) {
		t.Error("coalesce: the stamp of the FIRST finalization is not moved by a repeat (the how-long-ago-marked diagnostics, T-210)")
	}
	// Two publication records: the first (the failed one) also lands in the
	// fake's journal — the delivery is acknowledged only by the successful one.
	if pubs := f.q.publishesSnapshot(); len(pubs) != 2 {
		t.Errorf("q:analyze publications = %d, 2 needed (the failed one + the delivered one)", len(pubs))
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — processDelivery does not acknowledge itself (the XACK is Processings)", acked)
	}
}

// TestRenderWorker_RenderClearsMarker — a successful render of a marked
// row (a concurrent finalization delivery): renderUpdateSQL removes
// render_failed_at together with rendered = true — the rework pass
// selection sees the entry as rendered regardless of the marker,
// the invariant "marker ⇒ rendered = false" holds.
func TestRenderWorker_RenderClearsMarker(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRowMarked(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html><body>rendered anyway</body></html>")

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil || !written {
		t.Fatalf("ProcessResult: written=%v err=%v", written, err)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if !row.rendered {
		t.Error("rendered must be true (a successful render)")
	}
	if row.failedAt != nil {
		t.Error("render_failed_at must be cleared by a successful render (renderUpdateSQL, T-210)")
	}
}

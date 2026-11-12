// The worker group match pin test (T-155): WorkingGroups mirrors the
// DefaultGroup of the worker packages — the queue package cannot import
// them directly (workers are built on top of queue, an import cycle), so
// the names are duplicated as strings and checked here. The external
// test package queue_test imports both queue and the workers without a
// cycle.

package queue_test

import (
	"testing"

	"crawler/internal/analyzer"
	"crawler/internal/dnsworker"
	"crawler/internal/httpworker"
	"crawler/internal/queue"
	"crawler/internal/renderworker"
)

// TestWorkingGroupsMatchWorkerDefaults — a change of a worker's
// DefaultGroup (or of a stage's stream) must get into
// queue.WorkingGroups, otherwise that stage's groups are silently not
// cleaned. The q:render-fallback pair is checked by a literal: the
// render-fallback-worker group — the worker B profile (FR-12/T-240: the
// same renderworker binary, a separate group), not the render worker's
// DefaultGroup.
func TestWorkingGroupsMatchWorkerDefaults(t *testing.T) {
	want := map[string]string{
		queue.StreamDNS:     dnsworker.DefaultGroup,
		queue.StreamHTTP:    httpworker.DefaultGroup,
		queue.StreamRender:  renderworker.DefaultGroup,
		queue.StreamAnalyze: analyzer.DefaultGroup,
	}
	got := queue.WorkingGroups()
	if len(got) != len(want)+1 { // + the q:render-fallback pair (the worker B profile)
		t.Fatalf("WorkingGroups = %v, want the pairs %v + q:render-fallback=render-fallback-worker (an extra/missing stream — update the queue and the workers in sync)", got, want)
	}
	for stream, group := range want {
		if got[stream] != group {
			t.Errorf("WorkingGroups[%s] = %q, want %q (the %s worker's DefaultGroup)", stream, got[stream], group, stream)
		}
	}
	if got[queue.StreamRenderFallback] != "render-fallback-worker" {
		t.Errorf("WorkingGroups[%s] = %q, want %q (the worker-B profile group, T-240/FR-12)",
			queue.StreamRenderFallback, got[queue.StreamRenderFallback], "render-fallback-worker")
	}
}

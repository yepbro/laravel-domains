// Structural shutdown invariant of render-worker (T-232): the background
// loops of run (DiskWatchLoop — store, the collector of the q:render
// depth — rdb via the gate) are stopped by the derived cancellation and
// joined by the WaitGroup before the defer-closes of rdb/store/the
// Chromium manager on any exit of run — a graceful ctx cancellation, an
// early return from renderworker.New, a panic. Before T-232 both loops
// lived fire-and-forget: the defer-closes could catch the middle of a
// DiskWatchLoop pass or a queue depth request. main.go is checked by the
// shared helper internal/bgjoin (the sample and progenitor —
// TestRunBackgroundJoin of http-worker, T-223).
package main

import (
	"reflect"
	"testing"

	"crawler/internal/bgjoin"
	"crawler/internal/config"
)

func TestRunBackgroundJoin(t *testing.T) {
	bgjoin.Check(t, "main.go")
}

// TestEngineSkipPatternsBinding — the per-engine binding of the URL
// skip-list (T-285): the worker receives ONLY the patterns of its build's
// render.engine — the list of another engine is not looked at even when
// configured (the acceptance criterion "the patterns are inactive with
// engine=chromium or another engine"); a chromium/empty engine gets none
// (worker B and a rollback build never skip — chromium is the terminal
// rework engine).
func TestEngineSkipPatternsBinding(t *testing.T) {
	moli := []string{"hh.ru"}
	lightpanda := []string{"slow.example"}
	cases := []struct {
		name   string
		engine string
		lists  map[string][]string
		want   []string
	}{
		{"the moli build gets the moli list", config.EngineMoli,
			map[string][]string{"moli": moli, "lightpanda": lightpanda}, moli},
		{"the lightpanda build gets its own, not moli's", config.EngineLightpanda,
			map[string][]string{"moli": moli, "lightpanda": lightpanda}, lightpanda},
		{"a chromium build gets none (worker B, a rollback)", config.EngineChromium,
			map[string][]string{"moli": moli}, nil},
		{"an engine without a key (a foreign list only)", config.EngineMoli,
			map[string][]string{"lightpanda": lightpanda}, nil},
		{"an empty engine (a config without Default) gets none", "",
			map[string][]string{"moli": moli}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Render.Engine = c.engine
			cfg.Render.EngineSkipPatterns = c.lists
			if got := engineSkipPatterns(cfg); !reflect.DeepEqual(got, c.want) {
				t.Errorf("engineSkipPatterns(engine=%q) = %v, want %v", c.engine, got, c.want)
			}
		})
	}
}

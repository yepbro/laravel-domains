// Integration test of the dashboard snapshot collector (T-129, dashboard
// spec §12.2): a full snapshot of all sections against the live
// PostgreSQL and Redis of the testbed — SQL aggregates of internal/status
// on the real schema (zones/throughput/statuses/Laravel/ingest), stream
// depths via BackpressureGate.Len, /livez probes (a live and an
// unreachable service), /api/status serves the collected snapshot behind
// authentication. The unit level (SQL contracts, degradation, JSON shape,
// loop) — internal/status and internal/dashboard.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/config"
	"crawler/internal/dashboard"
	"crawler/internal/dashboard/auth"
	"crawler/internal/monitor"
	"crawler/internal/queue"
)

// dashSeed — the data of all sections inserted into the isolated schema.
type dashSeed struct {
	zoneID  int16
	domains [3]int64 // ids of the inserted domains
}

// seedDashboardData — a coherent set: a zone, three domains (alive+done,
// new+pending, temp_error+queued), two Laravel finds (a fresh one and one
// older than 24 h), two ingest files (a fresh add and an old delete), and
// a row with NULL processed_at (the schema allows it — sorted "last",
// columns zeroed).
// sha256 — a built-in PG 11+ function (bytes for the column without extensions).
func seedDashboardData(t *testing.T, pool *pgxpool.Pool) dashSeed {
	t.Helper()
	ctx := context.Background()
	var s dashSeed

	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name, is_active, domains_total, domains_checked) "+
			"VALUES ('snap.test', true, 3, 1) RETURNING id").Scan(&s.zoneID); err != nil {
		t.Fatalf("зона: %v", err)
	}
	domains := []struct {
		label  string
		status int16
		state  int16
	}{{"alpha", 4, 2}, {"beta", 0, 0}, {"gamma", 5, 1}}
	for i, d := range domains {
		if err := pool.QueryRow(ctx,
			"INSERT INTO domains (zone_id, label, status, check_state) "+
				"VALUES ($1, $2, $3, $4) RETURNING id",
			s.zoneID, d.label, d.status, d.state).Scan(&s.domains[i]); err != nil {
			t.Fatalf("домен %s: %v", d.label, err)
		}
	}

	// Laravel finds: a fresh one (the 24 h window) and a day old.
	if _, err := pool.Exec(ctx,
		"INSERT INTO laravel_domains (zone_id, domain_id, score, ver_min, ver_max, found_on_mask, rules_version, updated_at) "+
			"VALUES ($1, $2, 0.9, '9.19', '11', 1, 1, now())",
		s.zoneID, s.domains[0]); err != nil {
		t.Fatalf("свежая находка: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO laravel_domains (zone_id, domain_id, score, found_on_mask, rules_version, updated_at) "+
			"VALUES ($1, $2, 0.7, 1, 1, now() - interval '30 hours')",
		s.zoneID, s.domains[1]); err != nil {
		t.Fatalf("старая находка: %v", err)
	}

	// The ingest journal: a fresh add, a day-old delete, and a row without processed_at.
	if _, err := pool.Exec(ctx,
		"INSERT INTO ingest_files (zone_id, action, path, sha256, lines_total, added, existed, rejected, processed_at) "+
			"VALUES ($1, 1, 'snap.test/add/fresh.txt', sha256('1'::bytea), 100, 98, 2, 0, now())",
		s.zoneID); err != nil {
		t.Fatalf("свежий файл: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO ingest_files (zone_id, action, path, sha256, lines_total, deleted, rejected, processed_at) "+
			"VALUES ($1, 2, 'snap.test/delete/stale.txt', sha256('2'::bytea), 50, 49, 1, now() - interval '26 hours')",
		s.zoneID); err != nil {
		t.Fatalf("старый файл: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO ingest_files (sha256) VALUES (sha256('3'::bytea))"); err != nil {
		t.Fatalf("файл без processed_at: %v", err)
	}
	return s
}

// TestDashboard_Snapshot — a full snapshot on the testbed: all sections
// are alive, the numbers match the seed, window freshness (24 h / the
// latest), /api/status behind authentication serves the same snapshot.
func TestDashboard_Snapshot(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seed := seedDashboardData(t, pool)
	if seed.zoneID == 0 || seed.domains[0] == 0 {
		t.Fatal("посев данных не вернул идентификаторы")
	}

	// Counter reconciliation (T-178): zones / the status distribution /
	// history cumulatives are read by the dashboard from zones.domains_*
	// and crawler_counters — the combined monitor pass fills them (the
	// seed is below the transition level, direct SQL). The same order as
	// in production: migration → the first monitor pass → readers.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция счётчиков после посева: %v", err)
	}

	// Stream depths: entries without consumer groups — Len gives XLEN.
	for stream, n := range map[string]int{"q:http": 2, "q:dead": 1} {
		for i := 0; i < n; i++ {
			if err := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]any{"x": fmt.Sprintf("%s-%d", stream, i)},
			}).Err(); err != nil {
				t.Fatalf("XAdd %s: %v", stream, err)
			}
		}
	}

	// Probes: a live service and a connection failure (a closed port).
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer alive.Close()
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	cfg := config.Default()
	coll, err := dashboard.NewCollector(pool, gate, cfg.Dashboard, dashboard.CollectorOptions{
		Probes: map[string]string{"alive": alive.URL, "dead": deadURL},
		// The engines section: an empty NON-nil slice — "n/a" without
		// errors (the operator way of switching it off, S-P2-5): nil would
		// pull the config.Default() default with the render-worker:9105
		// address, absent in the test environment, and the snapshot would
		// fail with a render section error.
		RenderMetrics: []string{},
		// Queue highlighting thresholds — as in cmd/dashboard: NewCollector
		// does not read the scheduler/redis sections itself (§5.4); MaxLen —
		// for the effective stream threshold (clamped by MAXLEN).
		QueueWatermark: int64(cfg.Scheduler.QueueHighWatermark),
		QueueMaxLen:    int64(cfg.Redis.MaxLen),
	})
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// The loop: the first pass right away — we wait for readiness (until
	// then Current() is nil, the 503 semantics of §5.9), then stop and
	// check what was captured.
	loopCtx, stop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() { loopDone <- coll.Loop(loopCtx) }()
	deadline := time.Now().Add(10 * time.Second)
	for coll.Current() == nil {
		if time.Now().After(deadline) {
			stop()
			t.Fatal("первый успешный сбор не завершился за 10 с")
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap := coll.Current()
	stop()
	if err := <-loopDone; err != nil {
		t.Fatalf("Loop: %v", err)
	}

	if len(snap.Errors) != 0 {
		t.Fatalf("снимок с ошибками секций: %v", snap.Errors)
	}

	// §5.1/§5.3: the zone and the totals.
	if len(snap.Zones) != 1 || snap.Zones[0].Name != "snap.test" {
		t.Fatalf("zones = %+v", snap.Zones)
	}
	z := snap.Zones[0]
	if z.Pending != 1 || z.Queued != 1 || z.Done != 1 {
		t.Errorf("агрегат зоны = pending %d queued %d done %d, хочу 1/1/1", z.Pending, z.Queued, z.Done)
	}
	if snap.Totals == nil || snap.Totals.DomainsTotal != 3 || snap.Totals.DomainsChecked != 1 {
		t.Errorf("totals = %+v, хочу всего 3/проверено 1 (счётчики zones)", snap.Totals)
	}

	// §5.2: throughput and rates (no checks today — the rate is zero but
	// known; the hourly windows are empty).
	if snap.Throughput == nil || snap.Throughput.ElapsedSec <= 0 {
		t.Errorf("throughput = %+v, хочу живой elapsed", snap.Throughput)
	}
	if snap.Rates == nil || snap.Rates.Remaining != 2 {
		t.Errorf("rates = %+v, хочу remaining 2 (pending+queued)", snap.Rates)
	}

	// §5.4: stream depths and effective highlighting thresholds — clamped
	// to half of the stream MAXLEN (T-141; configuration defaults: a
	// watermark of 100,000 → q:dns 125 batches, individual 75,000), like
	// the monitor alert.
	if snap.QueueWatermarks["q:dns"] != 125 || snap.QueueWatermarks["q:http"] != 75000 {
		t.Errorf("queue_watermarks = %v, хочу q:dns 125 / q:http 75000 (эффективные пороги дефолтов)",
			snap.QueueWatermarks)
	}
	if got := snap.Queues["q:http"]; got != 2 {
		t.Errorf("q:http = %d, хочу 2 (XLEN без групп)", got)
	}
	if got := snap.Queues["q:dead"]; got != 1 {
		t.Errorf("q:dead = %d, хочу 1", got)
	}
	if got := snap.Queues["q:dns"]; got != 0 {
		t.Errorf("q:dns = %d, хочу 0", got)
	}

	// §5.5: the status distribution (the full legend, counters per the seed).
	if snap.Statuses["alive"] != 1 || snap.Statuses["new"] != 1 || snap.Statuses["temp_error"] != 1 {
		t.Errorf("statuses = %v, хочу alive/new/temp_error по 1", snap.Statuses)
	}

	// §5.6: 2 in total, 1 in the last 24 h, the latest is fresh with a version range.
	if snap.Laravel == nil || snap.Laravel.Total != 2 || snap.Laravel.Last24h != 1 {
		t.Errorf("laravel = %+v, хочу всего 2 / за 24 ч 1", snap.Laravel)
	}
	if len(snap.Laravel.Recent) != 2 || snap.Laravel.Recent[0].Domain != "alpha.snap.test" {
		t.Fatalf("laravel.recent = %+v, хочу свежую alpha.snap.test первой", snap.Laravel.Recent)
	}
	first := snap.Laravel.Recent[0]
	if first.VerMin != "9.19" || first.VerMax != "11" || first.Score < 0.89 {
		t.Errorf("свежая находка = %+v, хочу score ≈0.9 и версии 9.19–11", first)
	}

	// §5.7: the last file is the fresh add; NULL processed_at sorts last.
	if len(snap.Ingest) != 3 {
		t.Fatalf("ingest = %+v, хочу 3 записи", snap.Ingest)
	}
	if snap.Ingest[0].Path != "snap.test/add/fresh.txt" || snap.Ingest[0].Action != "add" ||
		snap.Ingest[0].Added != 98 || snap.Ingest[0].Existed != 2 {
		t.Errorf("свежий файл = %+v", snap.Ingest[0])
	}
	if snap.Ingest[1].Action != "delete" || snap.Ingest[1].Deleted != 49 {
		t.Errorf("старый файл = %+v", snap.Ingest[1])
	}
	if snap.Ingest[2].ProcessedAt != "" || snap.Ingest[2].Path != "" {
		t.Errorf("файл без processed_at = %+v, хочу нулевые поля и пустое время", snap.Ingest[2])
	}

	// §5.8: the live probe is up, the closed port is down with a reason.
	if len(snap.Health) != 2 {
		t.Fatalf("health = %+v, хочу 2 пробы", snap.Health)
	}
	if !snap.Health[0].Up || snap.Health[0].Name != "alive" {
		t.Errorf("живая проба = %+v", snap.Health[0])
	}
	if snap.Health[1].Up || snap.Health[1].Error == "" {
		t.Errorf("мертвая проба = %+v, хочу down с причиной", snap.Health[1])
	}

	// Acceptance §12.3 (the response structure): a server with a real pool
	// and collector serves the snapshot behind Basic authentication; the
	// errors map is empty.
	cfg.Dashboard.Username = "operator"
	hash, err := auth.Generate("integration-pass", 1000)
	if err != nil {
		t.Fatalf("auth.Generate: %v", err)
	}
	cfg.Dashboard.PasswordHash = hash
	srv, err := dashboard.New(pool, cfg, dashboard.Options{Snapshots: coll})
	if err != nil {
		t.Fatalf("dashboard.New: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/status", nil)
	req.SetBasicAuth("operator", "integration-pass")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("GET /api/status = %d: %s", resp.StatusCode, body)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, хочу no-store", cc)
	}
	var doc map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("ответ не JSON: %v", err)
	}
	for _, key := range []string{"zones", "totals", "queues", "throughput", "rates",
		"statuses", "laravel", "ingest", "health", "errors", "built_at", "queue_watermarks"} {
		if _, ok := doc[key]; !ok {
			t.Errorf("ответ без секции %q (§6.2)", key)
		}
	}
	if string(doc["errors"]) != "{}" {
		t.Errorf("errors = %s, хочу пустую карту (все секции живы)", doc["errors"])
	}
	if !strings.Contains(string(doc["laravel"]), `"last_24h":1`) {
		t.Errorf("laravel в ответе: %s", doc["laravel"])
	}
}

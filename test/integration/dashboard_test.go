// Интеграционный тест сборщика снимков дашборда (T-129, ТЗД §12.2):
// полный снимок всех секций против живых PostgreSQL и Redis testbed —
// SQL-агрегаты internal/status на реальной схеме (зоны/темп/статусы/
// Laravel/ingest), глубины стримов через BackpressureGate.Len, пробы
// /livez (живой и недоступный сервис), /api/status отдаёт собранный
// снимок за авторизацией. Юнит-уровень (SQL-контракты, деградация,
// JSON-форма, цикл) — internal/status и internal/dashboard.
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
	"crawler/internal/queue"
)

// dashSeed — данные всех секций, вставленные в изолированную схему.
type dashSeed struct {
	zoneID  int16
	domains [3]int64 // id вставленных доменов
}

// seedDashboardData — согласованный набор: зона, три домена (alive+done,
// new+pending, temp_error+queued), две Laravel-находки (свежая и старше
// 24 ч), два ingest-файла (свежий add и старый delete) и строка с NULL
// processed_at (схема допускает — «последним», колонки в нули).
// sha256 — встроенная функция PG 11+ (байты для колонки без расширений).
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

	// Laravel-находки: свежая (окно 24 ч) и суточная давность.
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

	// Журнал ingest: свежий add, суточный delete и строка без processed_at.
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

// TestDashboard_Snapshot — полный снимок на testbed: все секции живы,
// числа совпадают с посевом, свежесть окон (24 ч / последние),
// /api/status за авторизацией отдаёт тот же снимок.
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

	// Глубины стримов: записи без групп потребителей — Len даёт XLEN.
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

	// Пробы: живой сервис и отказ соединения (закрытый порт).
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
		// Порог подсветки очередей — как в cmd/dashboard: NewCollector не
		// читает секцию scheduler сам (§5.4).
		QueueWatermark: int64(cfg.Scheduler.QueueHighWatermark),
	})
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	// Цикл: первый проход сразу — ждём готовности (до него Current() nil,
	// семантика 503 §5.9), затем останавливаем и проверяем снятое.
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

	// §5.1/§5.3: зона и итоги.
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

	// §5.2: темп и скорости (проверок сегодня нет — скорость нулевая,
	// но известная; часовые окна пусты).
	if snap.Throughput == nil || snap.Throughput.ElapsedSec <= 0 {
		t.Errorf("throughput = %+v, хочу живой elapsed", snap.Throughput)
	}
	if snap.Rates == nil || snap.Rates.Remaining != 2 {
		t.Errorf("rates = %+v, хочу remaining 2 (pending+queued)", snap.Rates)
	}

	// §5.4: глубины стримов и порог подсветки (значение по умолчанию
	// конфигурации — 100000).
	if snap.QueueWatermark != 100000 {
		t.Errorf("queue_watermark = %d, хочу 100000 (дефолт конфигурации)", snap.QueueWatermark)
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

	// §5.5: распределение статусов (легенда полная, счётчики по посеву).
	if snap.Statuses["alive"] != 1 || snap.Statuses["new"] != 1 || snap.Statuses["temp_error"] != 1 {
		t.Errorf("statuses = %v, хочу alive/new/temp_error по 1", snap.Statuses)
	}

	// §5.6: всего 2, за 24 ч — 1, последняя — свежая с диапазоном версий.
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

	// §5.7: последний файл — свежий add, NULL processed_at — последним.
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

	// §5.8: живая проба up, закрытый порт down с причиной.
	if len(snap.Health) != 2 {
		t.Fatalf("health = %+v, хочу 2 пробы", snap.Health)
	}
	if !snap.Health[0].Up || snap.Health[0].Name != "alive" {
		t.Errorf("живая проба = %+v", snap.Health[0])
	}
	if snap.Health[1].Up || snap.Health[1].Error == "" {
		t.Errorf("мертвая проба = %+v, хочу down с причиной", snap.Health[1])
	}

	// Приёмка §12.3 (структура ответа): сервер с реальным пулом и сборщиком
	// отдаёт снимок за Basic-авторизацией; карта errors пуста.
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
		"statuses", "laravel", "ingest", "health", "errors", "built_at", "queue_watermark"} {
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

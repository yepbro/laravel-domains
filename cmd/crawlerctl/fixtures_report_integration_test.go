// Интеграционный тест снапшота и отчёта сравнения (T-244) против живого
// PostgreSQL дев-окружения: критерий задачи — «на двух прогонах одной
// конфигурации диф детекции ≈0, на испорченном прогоне отчёт ловит
// расхождение» — проверяется на реальном SQL (юнит-тесты гоняют чистые
// структуры). Изоляция — одноразовая схема с миграциями в БД дев-стека
// (общий helper живых тестов фикстур t243Schema, T-243). Запуск:
// make test-integration (CRAWLER_INTEGRATION=1, БД — APP_PG_DSN
// tools-контейнера); без переменной тест пропускается.
package main

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestFixturesSnapshotReportLive — два прогона одной выборки на живой
// схеме: снимки через fixturesSnapshotReadRows (SQL снапшота), отчёт
// fixturesReportBuild — детекция ≈0, DOM-пары/идентичность, успешность;
// испорченный прогон (перевёрнутые вердикты) ловится вердиктом
// НЕ ПРОЙДЕН.
func TestFixturesSnapshotReportLive(t *testing.T) {
	if os.Getenv("CRAWLER_INTEGRATION") != "1" {
		t.Skip("интеграционный тест снапшота/отчёта: запустите make test-integration")
	}
	ctx := context.Background()
	pool, cleanup := t243Schema(t)
	defer cleanup()

	// Дамп: 6 задач одного домена (ok-страта, базовый предикат final_url).
	ts := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	url := "https://fixture.example/"
	dump := &fixturesDump{
		Manifest: fixturesManifest{FormatVersion: fixturesFormatVersion, Seed: 242},
		Zones:    []fixturesZone{{ID: 9, Name: "fixture.test", IsActive: true, DomainsTotal: 1}},
		Domains:  []fixturesDomain{{ZoneID: 9, ID: 77, Label: "spa", Status: 2, CheckState: 2, DNSMask: 1}},
	}
	for i := int64(1); i <= 6; i++ {
		dump.Results = append(dump.Results, fixturesResult{
			ZoneID: 9, DomainID: 77, SubdomainID: int16(i), Variant: 0,
			Scheme: 2, FinalURL: &url, CheckedAt: ts, Stratum: "ok",
		})
	}
	if _, err := fixturesImportRun(ctx, pool, dump, os.Stdout); err != nil {
		t.Fatalf("накат: %v", err)
	}

	// simulateRun — прогон: success/finalize/вердикты. Контракт воркера
	// (renderUpdateSQL): rendered=true вместе с content_hash и
	// rules_version=NULL — анализатор затем штампует строку заново;
	// findings пишутся только при срабатываниях (findingUpsertSQL), при
	// нуле — DELETE (findingDeleteSQL, P1-1). laravel[sub]==nil —
	// анализатор по строке ещё не прошёл (rules_version остаётся NULL).
	simulateRun := func(finalized map[int16]bool, laravel map[int16]*bool, domHash map[int16]string, domLen map[int16]int32) {
		t.Helper()
		for sub := int16(1); sub <= 6; sub++ {
			if finalized[sub] {
				if _, err := pool.Exec(ctx,
					"UPDATE subdomain_results SET render_failed_at = now() WHERE zone_id = 9 AND domain_id = 77 AND subdomain_id = $1 AND variant = 0", sub); err != nil {
					t.Fatalf("финализация %d: %v", sub, err)
				}
				continue
			}
			hash := []byte(domHash[sub])
			if _, err := pool.Exec(ctx,
				"INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) VALUES ($1, 0, 0, $2, $2, 1) ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + 1",
				hash, domLen[sub]); err != nil {
				t.Fatalf("сид контента %d: %v", sub, err)
			}
			if _, err := pool.Exec(ctx,
				"UPDATE subdomain_results SET rendered = true, content_hash = $1, rules_version = NULL WHERE zone_id = 9 AND domain_id = 77 AND subdomain_id = $2 AND variant = 0",
				hash, sub); err != nil {
				t.Fatalf("рендер %d: %v", sub, err)
			}
			isLaravel, ok := laravel[sub]
			if !ok {
				continue // хвост q:analyze: строка без штампа анализатора
			}
			if _, err := pool.Exec(ctx,
				"UPDATE subdomain_results SET rules_version = 7 WHERE zone_id = 9 AND domain_id = 77 AND subdomain_id = $1 AND variant = 0", sub); err != nil {
				t.Fatalf("штамп анализатора %d: %v", sub, err)
			}
			if *isLaravel {
				if _, err := pool.Exec(ctx,
					"INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, is_laravel, signals, rules_version) "+
						"VALUES (9, 77, $1, 0, 0.9, true, '[]'::jsonb, 7) ON CONFLICT (zone_id, domain_id, subdomain_id, variant) "+
						"DO UPDATE SET is_laravel = true, rules_version = 7",
					sub); err != nil {
					t.Fatalf("вердикт анализатора %d: %v", sub, err)
				}
			} else if _, err := pool.Exec(ctx,
				"DELETE FROM laravel_findings WHERE zone_id = 9 AND domain_id = 77 AND subdomain_id = $1 AND variant = 0", sub); err != nil {
				t.Fatalf("удаление findings %d: %v", sub, err)
			}
		}
	}

	keys := make([]fixturesKey, 0, len(dump.Results))
	for _, r := range dump.Results {
		keys = append(keys, fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant})
	}
	take := func(label, engine string) *fixturesSnapshot {
		t.Helper()
		rows, err := fixturesSnapshotReadRows(ctx, pool, keys)
		if err != nil {
			t.Fatalf("снапшот %s: %v", label, err)
		}
		return &fixturesSnapshot{
			FormatVersion: fixturesSnapshotVersion,
			Label:         label, Engine: engine,
			CreatedAt: time.Now().UTC(),
			Dump:      fixturesSnapshotDump{Seed: 242, Keys: len(keys)},
			Rows:      rows,
		}
	}

	// Прогон 1 (chromium): 5 успехов (sub 1..5), 1 финализация; вердикты
	// на всех успехах, Laravel на 1 и 4.
	reset := func() {
		if _, err := fixturesImportRun(ctx, pool, dump, os.Stdout); err != nil {
			t.Fatalf("повторный накат: %v", err)
		}
	}
	reset()
	simulateRun(
		map[int16]bool{6: true},
		map[int16]*bool{1: bp(true), 2: bp(false), 3: bp(false), 4: bp(true), 5: bp(false)},
		map[int16]string{1: "01010101010101010101010101010101", 2: "02020202020202020202020202020202",
			3: "03030303030303030303030303030303", 4: "04040404040404040404040404040404", 5: "05050505050505050505050505050505"},
		map[int16]int32{1: 1000, 2: 2000, 3: 3000, 4: 4000, 5: 5000},
	)
	snapChromium := take("chromium-1", "chromium")

	// Прогон 2 (moli): те же задачи; 5 успехов, у sub 3 — другой DOM
	// (движок собрал другую страницу), вердикты идентичны.
	reset()
	simulateRun(
		map[int16]bool{6: true},
		map[int16]*bool{1: bp(true), 2: bp(false), 3: bp(false), 4: bp(true), 5: bp(false)},
		map[int16]string{1: "01010101010101010101010101010101", 2: "02020202020202020202020202020202",
			3: "33333333333333333333333333333333", 4: "04040404040404040404040404040404", 5: "05050505050505050505050505050505"},
		map[int16]int32{1: 1000, 2: 2000, 3: 3100, 4: 4000, 5: 5000},
	)
	snapMoli := take("moli-1", "moli")

	report, err := fixturesReportBuild(snapChromium, snapMoli, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	if report.SuccessRenderedB != 5 || report.SuccessRenderedC != 5 {
		t.Fatalf("успешность прогонов = %d/%d, хочу 5/5", report.SuccessRenderedB, report.SuccessRenderedC)
	}
	if report.DetectionComparable != 5 || report.DetectionDiff != 0 {
		t.Fatalf("детекция = %d сравнимых / %d расхождений, хочу 5/0", report.DetectionComparable, report.DetectionDiff)
	}
	if v := verdictOf(t, report, "детекция"); v == nil || v.Status != verdictPass {
		t.Fatalf("вердикт детекции = %+v, хочу ПРОЙДЕН", v)
	}
	if report.DOMPairs != 5 || report.DOMIdentical != 4 {
		t.Fatalf("DOM-пары = %d, идентичных %d, хочу 5/4", report.DOMPairs, report.DOMIdentical)
	}
	if report.DOMMedLenRatio == nil || *report.DOMMedLenRatio != 1 {
		t.Fatalf("медиана длины DOM = %v, хочу 1", report.DOMMedLenRatio)
	}

	// Испорченный прогон: у субдомена 1 сигнал ПРОПАЛ вовсе (анализатор
	// удалил findings — ни одного срабатывания, P1-1), у субдомена 2 —
	// появился; 2 из 5 сравнимых (40%) — отчёт обязан поймать оба.
	reset()
	simulateRun(
		map[int16]bool{6: true},
		map[int16]*bool{1: bp(false), 2: bp(true), 3: bp(false), 4: bp(true), 5: bp(false)},
		map[int16]string{1: "01010101010101010101010101010101", 2: "02020202020202020202020202020202",
			3: "03030303030303030303030303030303", 4: "04040404040404040404040404040404", 5: "05050505050505050505050505050505"},
		map[int16]int32{1: 1000, 2: 2000, 3: 3000, 4: 4000, 5: 5000},
	)
	snapCorrupted := take("moli-2", "moli")
	report, err = fixturesReportBuild(snapChromium, snapCorrupted, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт (испорченный): %v", err)
	}
	if report.DetectionDiff != 2 || report.DetectionRatePct != 40 {
		t.Fatalf("расхождения = %d (%.2f%%), хочу 2 (40%%)", report.DetectionDiff, report.DetectionRatePct)
	}
	if v := verdictOf(t, report, "детекция"); v == nil || v.Status != verdictFail {
		t.Fatalf("вердикт детекции = %+v, хочу НЕ ПРОЙДЕН", v)
	}

	// Хвост q:analyze: у субдомена 4 DOM записан, но анализатор не прошёл
	// (rules_version NULL, findings в БД — от прошлого прогона): строка не
	// сравнима (P1-2) — findings прошлого прогона не дают ложный ноль.
	reset()
	simulateRun(
		map[int16]bool{6: true},
		map[int16]*bool{1: bp(true), 2: bp(false), 3: bp(false), 5: bp(false)}, // 4 — без штампа
		map[int16]string{1: "01010101010101010101010101010101", 2: "02020202020202020202020202020202",
			3: "03030303030303030303030303030303", 4: "04040404040404040404040404040404", 5: "05050505050505050505050505050505"},
		map[int16]int32{1: 1000, 2: 2000, 3: 3000, 4: 4000, 5: 5000},
	)
	snapTail := take("moli-3", "moli")
	report, err = fixturesReportBuild(snapChromium, snapTail, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт (хвост analyzer): %v", err)
	}
	if report.DetectionComparable != 4 {
		t.Fatalf("сравнимых %d, хочу 4 (строка без штампа анализатора исключена)", report.DetectionComparable)
	}
	if report.DetectionDiff != 0 {
		t.Fatalf("расхождений %d, хочу 0", report.DetectionDiff)
	}
}

// bp — *bool литерал для карт срабатываний (Go не имеет литерала указателя).
func bp(v bool) *bool { return &v }

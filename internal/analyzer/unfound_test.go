// Юнит-тесты точечного дренажа контента домена при снятии находки
// переанализом (T-187): сигнал aggregateDomain (строка laravel_domains
// существовала и удалена доставкой без fresh) → после фиксации доставки
// воркер зовёт шов DrainDomain с ключом домена и окном retention —
// закрывается остаточный класс потери за курсором дренажа T-185
// (docs/content.md). Контрольный сценарий, границы события (fresh,
// отсутствие перехода, оставшаяся находка) и отказоустойчивость (сбой
// дренажа не роняет доставку). Шов подменяется рекордером — контентная
// семантика DrainDomain покрыта тестами internal/content
// (retentiondomain_test.go).
package analyzer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"crawler/internal/content"
)

// anDrainCall — зафиксированный вызов шва дренажа (T-187).
type anDrainCall struct {
	pool      content.RetentionDB
	retention time.Duration
	zone      int16
	domain    int64
}

// anDrainRecorder — подмена Options.DrainDomain: пишет вызовы в журнал
// фикстуры (событие anEvDrain — порядок против commit) и отвечает
// заготовленным итогом/ошибкой.
type anDrainRecorder struct {
	j     *anJournal
	stats content.RetentionStats
	err   error

	mu    sync.Mutex
	calls []anDrainCall
}

func (r *anDrainRecorder) drain(ctx context.Context, pool content.RetentionDB,
	retention time.Duration, zoneID int16, domainID int64) (content.RetentionStats, error) {
	r.mu.Lock()
	r.calls = append(r.calls, anDrainCall{pool: pool, retention: retention, zone: zoneID, domain: domainID})
	r.mu.Unlock()
	r.j.record(anEvDrain)
	return r.stats, r.err
}

func (r *anDrainRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *anDrainRecorder) last() anDrainCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[len(r.calls)-1]
}

// TestUnfoundDrain_ControlScenario — переанализ (fresh=false) снял
// находку: сигнал агрегации становится точечным дренажом домена — шов
// вызван ПОСЛЕ фиксации транзакции (findings и агрегат уже в БД) с
// ключом домена и окном retention воркера.
func TestUnfoundDrain_ControlScenario(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	// Прошлый эпизод находил признаки — строка laravel_domains есть;
	// доставка разбирает страницу без признаков (fresh=false — переанализ).
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
	f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
		score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
	}
	rec := &anDrainRecorder{j: f.pool.j, stats: content.RetentionStats{Rows: 3, Refs: 2}}
	f.w.opt.DrainDomain = rec.drain
	f.w.opt.Retention = 6 * time.Hour

	if wrote, err := f.w.ProcessResult(context.Background(), anTask); err != nil || !wrote {
		t.Fatalf("ProcessResult = %v, %v; нужен успех с записью", wrote, err)
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Fatal("строка laravel_domains не снята — события нет")
	}
	if rec.count() != 1 {
		t.Fatalf("вызовов дренажа = %d, want 1", rec.count())
	}
	call := rec.last()
	if call.zone != anKey.zone || call.domain != anKey.domain {
		t.Errorf("ключ дренажа = %d/%d, want %d/%d", call.zone, call.domain, anKey.zone, anKey.domain)
	}
	if call.retention != 6*time.Hour {
		t.Errorf("окно дренажа = %v, want 6h (retention воркера)", call.retention)
	}
	if p, ok := call.pool.(*anFakePool); !ok || p != f.pool {
		t.Error("дренажу передан не пул воркера")
	}
	// Порядок: дренаж строго после фиксации — событие уже состоялось,
	// сбой дренажа не должен ронять доставку (TestUnfoundDrain_Error).
	if f.pool.j.indexOf(anEvCommit) > f.pool.j.indexOf(anEvDrain) {
		t.Fatalf("дренаж до фиксации: %v", f.pool.j.snapshot())
	}
}

// TestUnfoundDrain_EventBounds — дренаж только на переходе без fresh:
// свежая потеря находки переписывает записи впереди курсора (T-058),
// отсутствие перехода (строки не было — DELETE 0) и оставшаяся находка
// дренажа не требуют.
func TestUnfoundDrain_EventBounds(t *testing.T) {
	cases := []struct {
		name      string
		fresh     bool
		laravel   bool // сеять ли строку laravel_domains (был ли переход)
		confident bool // доставка находит уверенные признаки
		want      int
	}{
		{"свежая потеря находки — впереди курсора", true, true, false, 0},
		{"перехода нет (строки не было)", false, false, false, 0},
		{"находка осталась (уверенные признаки)", false, true, true, 0},
		{"повтор пустого переанализа после снятия", false, false, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
			rec := &anDrainRecorder{j: f.pool.j}
			f.w.opt.DrainDomain, f.w.opt.Retention = rec.drain, 6*time.Hour

			if tc.confident {
				hash := anTestHash(7)
				f.store.blocks[hash] = anPageHTML()
				f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
			} else {
				f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
			}
			if tc.laravel {
				f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
					score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
				}
			}

			task := anTask
			task.Fresh = tc.fresh
			if _, err := f.w.ProcessResult(context.Background(), task); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			if rec.count() != tc.want {
				t.Fatalf("вызовов дренажа = %d, want %d (событие = переход без fresh)", rec.count(), tc.want)
			}
		})
	}
}

// TestUnfoundDrain_Error — сбой дренажа не роняет доставку: findings,
// агрегат и снятие строки зафиксированы, ProcessResult отвечает успехом
// (XACK состоится; записи домена остаются позади курсора — класс до
// T-187, Warn в логе воркера).
func TestUnfoundDrain_Error(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6, Retention: 6 * time.Hour})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
	f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
		score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
	}
	rec := &anDrainRecorder{j: f.pool.j, err: errors.New("drain boom")}
	f.w.opt.DrainDomain = rec.drain

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil || !wrote {
		t.Fatalf("ProcessResult = %v, %v; доставка обязана удаться (анализ зафиксирован)", wrote, err)
	}
	if rec.count() != 1 {
		t.Fatalf("вызовов дренажа = %d, want 1 (попытка была)", rec.count())
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Fatal("строка laravel_domains не снята при сбое дренажа")
	}
}

// TestUnfoundDrain_Disabled — выключенный дренаж (retention = 0 — контент
// хранится всегда; шов не задан — изолированная сборка): событие
// происходит, вызова нет, доставка не падает.
func TestUnfoundDrain_Disabled(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"retention выключен", Options{MinScore: 0.6, Retention: 0, DrainDomain: (&anDrainRecorder{}).drain}},
		{"шов не задан", Options{MinScore: 0.6, Retention: 6 * time.Hour}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), tc.opts)
			f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
			f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
				score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
			}
			if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
				t.Fatal("строка laravel_domains не снята")
			}
		})
	}
}

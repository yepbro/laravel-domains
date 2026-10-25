// Юнит-тесты точечного дренажа контента домена (T-187, retentiondomain.go):
// закрытие остаточного класса потери за курсором — снятие Laravel-находки
// переанализом возвращает остаревшие записи домена в кандидаты retention
// позади курсора (T-185), дренаж чистит их по префиксу PK
// (zone_id, domain_id) штатной purge-механикой T-166. Фейковый пул —
// retention_test.go. Контрольный сценарий задачи: после дренажа у домена
// не остаётся кандидатов (позади курсора и вообще).
package content

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/model"
)

// --- SQL-контракт --------------------------------------------------------------

// TestDrainDomain_SQLContract — точный текст выборки дренажа домена:
// адресация префиксом PK (без курсора и LIMIT — строки домена
// ограничены субдоменами 0..62 × варианты), те же фильтры отбора, что у
// retentionSelectSQL (возраст, контент/jsonb, рендер-флаг, deleted,
// не-находка), детерминизм пачки порядком PK.
func TestDrainDomain_SQLContract(t *testing.T) {
	renderPending := fmt.Sprintf("AND (r.rendered OR (d.flags & %d) = 0)", int16(model.FlagRenderPending))
	deleted := fmt.Sprintf("AND d.status <> %d", int16(model.StatusDeleted))
	for _, frag := range []string{
		"FROM subdomain_results AS r",
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id",
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8",
		"AND r.checked_at < now() - $3::interval",
		"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
			"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL",
		renderPending,
		deleted,
		"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld",
		"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant",
	} {
		if !strings.Contains(retentionDomainSQL, frag) {
			t.Errorf("выборка дренажа домена: нет фрагмента %q (фильтры отбора T-166)", frag)
		}
	}
	for _, bad := range []string{"LIMIT", "> ($2::timestamptz"} {
		if strings.Contains(retentionDomainSQL, bad) {
			t.Errorf("выборка дренажа домена: лишний фрагмент %q (точечная по PK, без курсора/лимита)", bad)
		}
	}
}

// --- контрольный сценарий задачи ----------------------------------------------

// TestDrainDomain_ControlScenario — переанализ снял находку домена: его
// остаревшие записи (позади курсора дренажа — курсор цикла их не
// вернёт) чистятся точечно — ссылка и jsonb срезаны, refcount уменьшен,
// contents без ссылок удалён; чужие домены не тронуты.
func TestDrainDomain_ControlScenario(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	// Домен 10 — бывшая находка (строку laravel_domains снял переанализ),
	// три остаревшие записи: две с контентом (делят один — дедупликация),
	// одна только с jsonb.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{status: int16(model.StatusAlive)}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 1}, checkedAt: old.Add(2 * time.Hour), jsonb: true},
		// Соседний домен 20 той же зоны — не трогается.
		{key: retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x33), jsonb: true},
	}
	pool.domains[retentionDomKey{zone: 1, id: 20}] = retentionFakeDomain{status: int16(model.StatusAlive)}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 2, clen: 5000}
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 7000}

	stats, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10)
	if err != nil {
		t.Fatalf("DrainDomain: %v", err)
	}
	if stats.Rows != 3 || stats.Refs != 2 || stats.Contents != 1 || stats.FreedBytes != 5000 {
		t.Fatalf("stats = %+v, want Rows=3 Refs=2 Contents=1 FreedBytes=5000", stats)
	}
	for _, sub := range []int16{0, 1, 2} {
		r := pool.row(retentionKey{zone: 1, domain: 10, sub: sub, variant: map[int16]int16{0: 0, 1: 0, 2: 1}[sub]})
		if r == nil || r.hash != nil || r.jsonb {
			t.Errorf("домен 10, sub=%d: запись не очищена: %+v", sub, r)
		}
	}
	if _, ok := pool.contents[retentionHash(0x11)]; ok {
		t.Error("contents 0x11 (обе ссылки срезаны) обязан быть удалён")
	}
	if r := pool.row(retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}); r == nil || r.hash == nil || !r.jsonb {
		t.Errorf("чужой домен 20 тронут: %+v", r)
	}
	if c, ok := pool.contents[retentionHash(0x33)]; !ok || c.refc != 1 {
		t.Errorf("contents 0x33 (чужой домен) тронут: %+v", c)
	}
	// Контрольный критерий: у домена не осталось кандидатов ВООБЩЕ —
	// повторный дренаж пуст, транзакция не открывается.
	pool.queries, pool.txCalls = nil, nil
	stats, err = DrainDomain(context.Background(), pool, DefaultRetention, 1, 10)
	if err != nil {
		t.Fatalf("повторный DrainDomain: %v", err)
	}
	if stats.Rows != 0 || pool.begins != 1 {
		t.Fatalf("повтор = %+v (транзакций %d), want пустой итог без новой транзакции", stats, pool.begins)
	}
}

// --- фильтры отбора -------------------------------------------------------------

// TestDrainDomain_Filters — критерии T-166 в точечной выборке: находка
// хранит контент всегда, render_pending без rendered ждёт рендера,
// deleted — территория T-020, свежие и уже очищенные записи не кандидаты.
func TestDrainDomain_Filters(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		domain int64
	}{
		{"домен снова находка (переанализ вернул)", 20},
		{"render_pending, не rendered", 30},
		{"deleted — чистит T-020", 50},
		{"свежая запись — не остарела", 60},
		{"уже очищенная — чистить нечего", 70},
	}
	pool := retentionSeedPool(now)
	// 70: остаревшая запись без ссылки и jsonb (например, прошлый дренаж).
	pool.domains[retentionDomKey{zone: 1, id: 70}] = retentionFakeDomain{status: int16(model.StatusAlive)}
	pool.results = append(pool.results, retentionRow{
		key: retentionKey{zone: 1, domain: 70, sub: 0, variant: 0}, checkedAt: now.Add(-2 * DefaultRetention),
	})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stats, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, tc.domain)
			if err != nil {
				t.Fatalf("DrainDomain: %v", err)
			}
			if stats.Rows != 0 || stats.Batches != 0 {
				t.Fatalf("stats = %+v, want пустой итог (записи защищены фильтрами отбора)", stats)
			}
		})
	}
}

// TestDrainDomain_ArgsAndNoCursor — параметры выборки: ключи домена и
// граница старения интервалом; курсор дренажа цикла не участвует.
func TestDrainDomain_ArgsAndNoCursor(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	pool := drainPool(now) // домен 1/10, два остаревших кандидата

	if _, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10); err != nil {
		t.Fatalf("DrainDomain: %v", err)
	}
	if len(pool.queries) != 1 {
		t.Fatalf("SELECT'ов = %d, want 1", len(pool.queries))
	}
	want := []any{int16(1), int64(10), DefaultRetention.String()}
	if !reflect.DeepEqual(pool.queries[0].args, want) {
		t.Fatalf("args = %v, want %v (зона, домен, окно)", pool.queries[0].args, want)
	}
	// Пачка purge — ключи домена в порядке PK, граница та же.
	if len(pool.txCalls) == 0 || pool.txCalls[0].sql != retentionPurgeSQL {
		t.Fatalf("первый оператор транзакции = %+v, want purge", pool.txCalls[0])
	}
	purge := pool.txCalls[0].args
	if !reflect.DeepEqual(purge[:4], []any{[]int16{1, 1}, []int64{10, 10}, []int16{0, 1}, []int16{0, 0}}) {
		t.Errorf("ключи purge = %v, want строки домена в порядке PK", purge[:4])
	}
	if purge[5] != DefaultRetention.String() {
		t.Errorf("граница purge = %v, want %s", purge[5], DefaultRetention)
	}
}

// --- пустой итог и сбои --------------------------------------------------------

// TestDrainDomain_EmptyNoTx — кандидатов нет: один SELECT, транзакция не
// открывается (чистить нечего).
func TestDrainDomain_EmptyNoTx(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.results = nil
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{status: int16(model.StatusAlive)}

	stats, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10)
	if err != nil {
		t.Fatalf("DrainDomain: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Fatalf("stats = %+v, want пустой итог", stats)
	}
	if len(pool.queries) != 1 || pool.begins != 0 {
		t.Fatalf("обращений = %d (транзакций %d), want один SELECT без транзакции", len(pool.queries), pool.begins)
	}
}

// TestDrainDomain_Errors — сбои шагов: ошибка выборки и чистки
// возвращается (вызывающий логирует, не роняя доставку — T-187),
// выключенное окно и пустой пул — громкая ошибка использования.
func TestDrainDomain_Errors(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	if _, err := DrainDomain(context.Background(), nil, DefaultRetention, 1, 10); err == nil {
		t.Error("nil-пул: ожидана ошибка")
	}
	if _, err := DrainDomain(context.Background(), retentionSeedPool(now), 0, 1, 10); err == nil {
		t.Error("retention = 0: ожидана ошибка (контент хранится всегда)")
	}

	pool := retentionSeedPool(now)
	pool.selectErr = errors.New("select boom")
	if _, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10); err == nil ||
		!strings.Contains(err.Error(), "дренаж домена") {
		t.Fatalf("ошибка выборки = %v, want контекст дренажа", err)
	}

	pool = retentionSeedPool(now)
	pool.purgeErr = errors.New("purge boom")
	_, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10)
	if err == nil || !strings.Contains(err.Error(), "дренаж домена 1/10") {
		t.Fatalf("ошибка чистки = %v, want контекст с ключом домена", err)
	}
	// Охраны T-044: сбой чистки откатил транзакцию — строки не тронуты.
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r == nil || r.hash == nil {
		t.Error("сбой пачки оставил изменения в строках (транзакция не откатилась)")
	}
}

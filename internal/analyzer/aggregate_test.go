// Юнит-тесты агрегации на уровне домена (T-072; ТЗ 5.8 «на уровне домена
// — агрегат (максимальный score, объединённый диапазон, маска
// субдоменов)», §6.1 laravel_domains, §13.1, 3.4, 3.5) — на фейковом
// пуле worker_test.go, расширенном живой семантикой laravel_domains
// (SELECT уверенных срабатываний домена, upsert/DELETE по PK). Проверяются
// SQL-контракты и типы параметров, максимальный score, объединение
// диапазонов версий (NULL-границы и конфликт T-070, числовой порядок
// версий), маска битов субдоменов, upsert/удаление строки домена,
// идемпотентность повторной доставки, deleted-домен, сбои шагов и отмена
// ctx, «нет уверенных срабатываний — нет строки».
package analyzer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- фейковый пул: laravel_domains (расширение anFakePool) ----------------------

// anDomainKey — PK laravel_domains в фейке.
type anDomainKey struct {
	zone   int16
	domain int64
}

// anDomain — строка laravel_domains, которую пишет aggregateUpsertSQL.
type anDomain struct {
	score        float32
	verMin       *string
	verMax       *string
	foundOnMask  model.DNSMask
	rulesVersion int32
	updatedAt    time.Time
}

// Query реализует aggregateSelectSQL в транзакции фейка: уверенные
// срабатывания (is_laravel) домена в порядке PK — детерминированно, как
// лист партиции PostgreSQL; собственные записи транзакции (findings этой
// доставки) видны — Exec фейка меняет состояние напрямую, Rollback
// восстанавливает снапшот. Инъекции aggScanErr/aggRowsErr и onClose —
// мутационные тесты T-M10 (сбой Scan/Err строк, контроль Close).
func (t *anFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != aggregateSelectSQL {
		return nil, fmt.Errorf("anFakeTx: неожидаемый SQL: %s", sql)
	}
	if t.pool.aggSelectErr != nil {
		return nil, t.pool.aggSelectErr
	}
	key := anDomainKey{zone: args[0].(int16), domain: args[1].(int64)}
	keys := make([]anRowKey, 0, len(t.pool.findings))
	for k, f := range t.pool.findings {
		if k.zone == key.zone && k.domain == key.domain && f.isLaravel {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].subdomain != keys[j].subdomain {
			return keys[i].subdomain < keys[j].subdomain
		}
		return keys[i].variant < keys[j].variant
	})
	rows := make([][]any, 0, len(keys))
	for _, k := range keys {
		f := t.pool.findings[k]
		rows = append(rows, []any{k.subdomain, f.score, f.verMin, f.verMax})
	}
	t.pool.j.record(anEvAggSelect)
	return &anAggRows{rows: rows, scanErr: t.pool.aggScanErr, err: t.pool.aggRowsErr,
		onClose: func() { t.pool.j.record(anEvAggClose) }}, nil
}

// anAggRows — pgx.Rows с уверенными срабатываниями домена (subdomain_id,
// score, ver_min, ver_max); scanErr/err/onClose — инъекции T-M10.
type anAggRows struct {
	rows    [][]any
	i       int
	scanErr error
	err     error
	onClose func()
}

func (r *anAggRows) Close() {
	if r.onClose != nil {
		r.onClose()
	}
}
func (r *anAggRows) Err() error                    { return r.err }
func (r *anAggRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *anAggRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *anAggRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *anAggRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.i == 0 || r.i > len(r.rows) || len(dest) != 4 {
		return fmt.Errorf("anAggRows: Scan без Next или приёмников %d (нужно 4)", len(dest))
	}
	row := r.rows[r.i-1]
	// NULL → nil-приёмник остаётся нетронутым (семантика pgx: *string
	// для границ остаётся nil).
	for i, v := range row {
		if v == nil {
			continue
		}
		switch d := dest[i].(type) {
		case *int16:
			*d = v.(int16)
		case *float32:
			*d = v.(float32)
		case **string:
			*d = v.(*string)
		default:
			return fmt.Errorf("anAggRows: приёмник[%d] %T", i, dest[i])
		}
	}
	return nil
}

func (r *anAggRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *anAggRows) RawValues() [][]byte    { return nil }
func (r *anAggRows) Conn() *pgx.Conn        { return nil }
func (r *anAggRows) TypeMap() *pgtype.Map   { return nil }

// anDomainSnapshot — снапшот строки laravel_domains.
func anDomainSnapshot(p *anFakePool, zone int16, domain int64) (anDomain, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d, ok := p.domains[anDomainKey{zone, domain}]; ok {
		return *d, true
	}
	return anDomain{}, false
}

// anSeedFinding — готовое срабатывание чужого (уже зафиксированного)
// эпизода: у старых findings своей строки subdomain_results не требуется.
func anSeedFinding(p *anFakePool, key anRowKey, score float32, verMin, verMax *string, isLaravel bool) {
	p.findings[key] = &anFinding{score: score, verMin: verMin, verMax: verMax, isLaravel: isLaravel, rulesVersion: 999}
}

// anStr — указатель на строку (граница диапазона фикстуры).
func anStr(s string) *string { return &s }

// --- SQL-контракт --------------------------------------------------------------

// TestAggregate_SQLContract — точный текст SQL агрегации против миграции
// 000001 (laravel_domains: PK (zone_id, domain_id), score real, ver_min/
// ver_max text, found_on_mask bigint, rules_version integer NOT NULL,
// updated_at timestamptz NOT NULL) и §13.1 (порог is_laravel).
func TestAggregate_SQLContract(t *testing.T) {
	if !strings.Contains(aggregateLockSQL, "pg_advisory_xact_lock($1::int8)") {
		t.Error("lock: нет advisory-блокировки домена до конца транзакции")
	}
	if !strings.Contains(aggregateSelectSQL, "FROM laravel_findings") {
		t.Error("select: агрегат читается из laravel_findings")
	}
	if !strings.Contains(aggregateSelectSQL, "AND is_laravel") {
		t.Error("select: нет фильтра уверенных срабатываний (§13.1 порог laravel_domains)")
	}
	if !strings.Contains(aggregateUpsertSQL,
		"ON CONFLICT (zone_id, domain_id) DO UPDATE") {
		t.Error("upsert: нет конфликта по PK laravel_domains (§3.5 перезапись)")
	}
	for _, col := range []string{"score", "ver_min", "ver_max", "found_on_mask", "rules_version", "updated_at"} {
		if !strings.Contains(aggregateUpsertSQL, col) {
			t.Errorf("upsert: нет колонки %s (§6.1)", col)
		}
	}
	if !strings.Contains(aggregateUpsertSQL, "$3::float4") {
		t.Error("upsert: score не приводится к real (float4)")
	}
	if !strings.Contains(aggregateUpsertSQL, "$6::int8") {
		t.Error("upsert: found_on_mask не приводится к bigint (маска DNSMask)")
	}
	if !strings.Contains(aggregateUpsertSQL, "now()") {
		t.Error("upsert: updated_at не из времени транзакции сервера (стиль record.go)")
	}
	if !strings.Contains(aggregateDeleteSQL, "DELETE FROM laravel_domains") {
		t.Error("delete: не удаляет строку laravel_domains")
	}
}

// --- scanAggregate: объединение диапазонов, маска, максимум ---------------------

func TestAggregate_ScanSemantics(t *testing.T) {
	cases := []struct {
		name string
		rows [][]any // (subdomain_id, score, ver_min, ver_max)
		want domainAggregate
	}{
		{
			name: "единственное срабатывание — диапазон как есть",
			rows: [][]any{{int16(1), float32(0.7), anStr("5.3"), anStr("7.x")}},
			want: domainAggregate{score: 0.7, verMin: anStr("5.3"), verMax: anStr("7.x"),
				foundOnMask: mustMask(t, 1)},
		},
		{
			name: "числовой порядок версий: min(10,5.3)=5.3, max(9.x,11)=11",
			rows: [][]any{
				{int16(1), float32(0.7), anStr("10"), anStr("9.x")},
				{int16(2), float32(0.6), anStr("5.3"), anStr("11")},
			},
			want: domainAggregate{score: 0.7, verMin: anStr("5.3"), verMax: anStr("11"),
				foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "максимальный score не у нижней границы диапазона",
			rows: [][]any{
				{int16(1), float32(0.9), anStr("9.19"), anStr("11")},
				{int16(2), float32(0.95), anStr("5.3"), anStr("7.x")},
			},
			want: domainAggregate{score: 0.95, verMin: anStr("5.3"), verMax: anStr("11"),
				foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "NULL верхней границы распространяется: объединение неограничено сверху",
			rows: [][]any{
				{int16(1), float32(0.7), anStr("9.19"), nil},
				{int16(2), float32(0.6), anStr("5.3"), anStr("7.x")},
			},
			want: domainAggregate{score: 0.7, verMin: anStr("5.3"),
				foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "конфликт отдельного срабатывания (обе NULL, T-070) — диапазон домена неопределён",
			rows: [][]any{
				{int16(1), float32(0.7), nil, nil},
				{int16(2), float32(0.6), anStr("9.19"), anStr("11")},
			},
			want: domainAggregate{score: 0.7, foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "все срабатывания без границ — обе границы NULL",
			rows: [][]any{
				{int16(1), float32(0.7), nil, nil},
				{int16(2), float32(0.6), nil, anStr("7.x")},
			},
			want: domainAggregate{score: 0.7, foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "равные границы в разной записи — первая по PK",
			rows: [][]any{
				{int16(1), float32(0.7), anStr("9.19"), anStr("9.19.0")},
			},
			want: domainAggregate{score: 0.7, verMin: anStr("9.19"), verMax: anStr("9.19.0"),
				foundOnMask: mustMask(t, 1)},
		},
		{
			name: "численно равные границы соседних строк — первая по PK (min и max)",
			rows: [][]any{
				{int16(1), float32(0.7), anStr("9.19"), anStr("9.19.0")},
				{int16(2), float32(0.6), anStr("9.19.0"), anStr("9.19")},
			},
			want: domainAggregate{score: 0.7, verMin: anStr("9.19"), verMax: anStr("9.19.0"),
				foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "отрицательный score (мусор в БД) — первая строка задаёт максимум",
			rows: [][]any{
				{int16(1), float32(-0.5), nil, nil},
			},
			want: domainAggregate{score: -0.5, foundOnMask: mustMask(t, 1)},
		},
		{
			name: "варианты одного субдомена схлопываются в бит; бит 62 допустим",
			rows: [][]any{
				{int16(5), float32(0.61), anStr("10"), anStr("11")},
				{int16(5), float32(0.62), anStr("10"), anStr("11")},
				{int16(62), float32(0.6), anStr("10"), anStr("11")},
			},
			want: domainAggregate{score: 0.62, verMin: anStr("10"), verMax: anStr("11"),
				foundOnMask: mustMask(t, 5, 62)},
		},
		{
			name: "пустой набор — пустой агрегат",
			rows: nil,
			want: domainAggregate{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scanAggregate(&anAggRows{rows: tc.rows})
			if err != nil {
				t.Fatalf("scanAggregate: %v", err)
			}
			if got.findings != len(tc.rows) {
				t.Fatalf("findings = %d, нужно %d", got.findings, len(tc.rows))
			}
			if got.score != tc.want.score {
				t.Errorf("score = %v, нужно %v (максимум)", got.score, tc.want.score)
			}
			if got.foundOnMask != tc.want.foundOnMask {
				t.Errorf("found_on_mask = %d (%v), нужно %d (%v)",
					int64(got.foundOnMask), got.foundOnMask.IDs(),
					int64(tc.want.foundOnMask), tc.want.foundOnMask.IDs())
			}
			if !anStrEq(got.verMin, tc.want.verMin) {
				t.Errorf("ver_min = %v, нужно %v", anStrDeref(got.verMin), anStrDeref(tc.want.verMin))
			}
			if !anStrEq(got.verMax, tc.want.verMax) {
				t.Errorf("ver_max = %v, нужно %v", anStrDeref(got.verMax), anStrDeref(tc.want.verMax))
			}
		})
	}
}

// TestAggregate_InvalidSubdomainID — номер бита вне 0..62 (мусор в БД)
// — ошибка, маска не искажается молча (model.DNSMask).
func TestAggregate_InvalidSubdomainID(t *testing.T) {
	_, err := scanAggregate(&anAggRows{rows: [][]any{{int16(63), float32(0.7), nil, nil}}})
	if err == nil {
		t.Fatal("subdomain_id 63 (бит 63 знаковый) — ожидана ошибка")
	}
}

// --- через воркер: транзакция, upsert/удаление, идемпотентность -----------------

// TestAggregate_ThroughWorker — сквозной контракт: findings домена
// (написанные доставкой + зафиксированные раньше) → advisory-блокировка →
// SELECT уверенных срабатываний → upsert laravel_domains (максимальный
// score, объединённый диапазон, маска, версия набора) → COMMIT; порядок —
// по журналу. Чужой домен и слабые срабатывания в агрегат не входят.
func TestAggregate_ThroughWorker(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	// Доставка: субдомен 1 (XSRF + session + welcome) → score ≈ 0.7231,
	// диапазон [5.3, 7.x], is_laravel.
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])

	// Зафиксированное раньше уверенное срабатывание субдомена 2: верхняя
	// граница неопределена — объединение неограничено сверху.
	anSeedFinding(f.pool, anRowKey{3, 42, 2, 0}, 0.7, anStr("9.19"), nil, true)
	// Слабое срабатывание субдомена 3 — ниже порога, вне агрегата (§13.1).
	anSeedFinding(f.pool, anRowKey{3, 42, 3, 0}, 0.3, anStr("4.0"), anStr("4.1"), false)
	// Уверенное срабатывание чужого домена — не входит.
	anSeedFinding(f.pool, anRowKey{3, 43, 1, 0}, 0.99, anStr("1.0"), anStr("2.0"), true)

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}

	// Порядок: findings → блокировка → чтение → агрегат → фиксация;
	// строки агрегации закрываются после чтения (мутант вырезания
	// rows.Close, T-M10).
	for _, pair := range [][2]anEventKind{
		{anEvUpsert, anEvLock}, {anEvLock, anEvAggSelect},
		{anEvAggSelect, anEvAggClose}, {anEvAggClose, anEvAggUpsert},
		{anEvAggUpsert, anEvCommit},
	} {
		if f.pool.j.indexOf(pair[0]) > f.pool.j.indexOf(pair[1]) {
			t.Fatalf("порядок %v после %v нарушен: %v", pair[1], pair[0], f.pool.j.snapshot())
		}
	}

	row, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain)
	if !ok {
		t.Fatal("строка laravel_domains не записана")
	}
	// Максимальный score: noisy-OR(0.25, 0.35, 0.45) ≈ 0.7231 > 0.7.
	wantScore := float32(1 - (1-0.25)*(1-0.35)*(1-0.45))
	if row.score != wantScore {
		t.Errorf("score = %v, нужно максимум %v", row.score, wantScore)
	}
	// Объединение [5.3, 7.x] ∪ [9.19, NULL) = [5.3, NULL).
	if row.verMin == nil || *row.verMin != "5.3" {
		t.Errorf("ver_min = %v, нужно 5.3 (min нижних границ)", anStrDeref(row.verMin))
	}
	if row.verMax != nil {
		t.Errorf("ver_max = %v, нужно NULL (объединение неограничено сверху)", *row.verMax)
	}
	// Маска: субдомены 1 (доставка) и 2 (раньше); 3 — слабое, 43 — чужой.
	if want := mustMask(t, 1, 2); row.foundOnMask != want {
		t.Errorf("found_on_mask = %d (%v), нужно %d (%v)",
			int64(row.foundOnMask), row.foundOnMask.IDs(), int64(want), want.IDs())
	}
	if row.rulesVersion != 1001 {
		t.Errorf("rules_version = %d, нужно 1001 (версия набора пересчёта)", row.rulesVersion)
	}
	if row.updatedAt.IsZero() {
		t.Error("updated_at не проставлен")
	}
}

// TestAggregate_NoConfidentDeletesRow — уверенных срабатываний нет:
// строки домена не должно быть (порог §13.1), существующая удаляется
// (§3.5) — и от слабого итога доставки, и от пустого (finding удалён).
func TestAggregate_NoConfidentDeletesRow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cookies *string
		content []byte
	}{
		{"слабое срабатывание ниже порога", anCookies(t, "XSRF-TOKEN"), nil}, // 0.25 < 0.6
		{"ни одного срабатывания", anCookies(t, "PHPSESSID"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
			f.pool.seedResult(anKey, anHeaders(t), tc.cookies, nil)
			// Прежний эпизод находил Laravel — строка домена существует.
			f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
				score: 0.9, verMin: anStr("5.3"), verMax: anStr("7.x"),
				foundOnMask: mustMask(t, 1, 2), rulesVersion: 999, updatedAt: time.Now(),
			}

			if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
				t.Fatal("строка laravel_domains не удалена (нет уверенных срабатываний)")
			}
			if f.pool.j.indexOf(anEvAggDelete) == -1 {
				t.Fatal("DELETE laravel_domains не выполнялся")
			}
		})
	}
}

// TestAggregate_RepeatDelivery — повторная доставка идемпотентна (ТЗ
// 3.4): upsert перезаписывает ту же строку теми же значениями.
func TestAggregate_RepeatDelivery(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	anSeedFinding(f.pool, anRowKey{3, 42, 2, 0}, 0.7, anStr("9.19"), anStr("11"), true)

	for i := 0; i < 2; i++ {
		if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
			t.Fatalf("доставка №%d: %v", i+1, err)
		}
	}
	row, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain)
	if !ok {
		t.Fatal("строка laravel_domains не записана")
	}
	if want := mustMask(t, 1, 2); row.foundOnMask != want {
		t.Fatalf("found_on_mask = %d, нужно %d", int64(row.foundOnMask), int64(want))
	}
	if row.verMin == nil || *row.verMin != "5.3" || row.verMax == nil || *row.verMax != "11" {
		t.Fatalf("диапазон = [%v, %v], нужно [5.3, 11]",
			anStrDeref(row.verMin), anStrDeref(row.verMax))
	}
	if n := countEvents(f.pool.j.snapshot(), anEvAggUpsert); n != 2 {
		t.Fatalf("agg upsert выполнен %d раз (нужно 2 — по одному на доставку)", n)
	}
}

// TestAggregate_DomainDeleted — домен удалён (§8/§13.1): охрана штампа →
// 0 строк → без записи; агрегация не выполняется вовсе, существующая
// строка домена не трогается (вычистит фоновая очистка laravel_*,
// план после T-020).
func TestAggregate_DomainDeleted(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	stale := anDomain{score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now()}
	f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &stale
	f.pool.mutateBeforeTx = func(p *anFakePool) {
		p.statuses[anKey] = int16(model.StatusDeleted)
	}

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil || wrote {
		t.Fatalf("ProcessResult = %v, %v; нужен успех без записи", wrote, err)
	}
	if f.pool.j.indexOf(anEvLock) != -1 || f.pool.j.indexOf(anEvAggUpsert) != -1 || f.pool.j.indexOf(anEvAggDelete) != -1 {
		t.Fatal("агрегация выполнялась для удалённого домена")
	}
	if row, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); !ok || row.rulesVersion != 999 {
		t.Fatal("существующая строка laravel_domains затронута при удалённом домене")
	}
}

// TestAggregate_Errors — сбой каждого шага агрегации: ошибка доставки
// (без XACK), состояние откатывается — findings и строка домена;
// want — точная подстрока обёртки (пины текстов против мутантов
// вырезания return, T-M10), fresh — доставка свежего эпизода HTTP
// (ветки истории appeared/lost, T-118).
func TestAggregate_Errors(t *testing.T) {
	seed := func(t *testing.T) *anFixture {
		f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
		hash := anTestHash(7)
		f.store.blocks[hash] = anPageHTML()
		f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
		stale := anDomain{score: 0.5, foundOnMask: mustMask(t, 2), rulesVersion: 999, updatedAt: time.Now()}
		f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &stale
		return f
	}
	cases := []struct {
		name   string
		inj    func(f *anFixture)
		want   string
		fresh  bool
		hadRow bool // была ли строка домена до сбоя (appeared-ветка стартует без неё)
	}{
		{"блокировка домена", func(f *anFixture) { f.pool.lockErr = errors.New("lock boom") }, "", false, true},
		{"чтение срабатываний", func(f *anFixture) { f.pool.aggSelectErr = errors.New("select boom") }, "", false, true},
		{"чтение срабатываний (Scan строк)", func(f *anFixture) { f.pool.aggScanErr = errors.New("scan boom") },
			"чтение срабатываний", false, true},
		{"чтение срабатываний (Err строк)", func(f *anFixture) { f.pool.aggRowsErr = errors.New("rows boom") },
			"чтение срабатываний", false, true},
		{"мусорный субдомен в findings", func(f *anFixture) {
			anSeedFinding(f.pool, anRowKey{3, 42, 63, 0}, 0.7, nil, nil, true)
		}, "чтение срабатываний", false, true},
		{"чтение строки домена", func(f *anFixture) { f.pool.existsErr = errors.New("exists boom") },
			"чтение строки домена", false, true},
		{"запись строки домена", func(f *anFixture) { f.pool.aggUpsertErr = errors.New("agg boom") },
			"запись строки домена: agg boom", false, true},
		{"запись строки домена: RowsAffected", func(f *anFixture) { f.pool.aggUpsertRowsAff = 2 },
			"запись строки домена: затронуто 2 строк", false, true},
		{"удаление строки домена", func(f *anFixture) {
			// Итог доставки без срабатываний → ветка удаления.
			f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
			f.pool.aggUpsertErr = errors.New("не должен вызываться")
			f.pool.aggDeleteErr = errors.New("agg del boom")
		}, "", false, true},
		{"запись истории (lost)", func(f *anFixture) {
			f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil) // признаков нет
			f.pool.aggHistErr = errors.New("hist boom")
		}, "запись истории (lost)", true, true},
		{"запись истории (appeared)", func(f *anFixture) {
			delete(f.pool.domains, anDomainKey{anKey.zone, anKey.domain}) // строки не было
			f.pool.aggHistErr = errors.New("hist boom")
		}, "запись истории (appeared)", true, false},
		{"фиксация", func(f *anFixture) { f.pool.commitErr = errors.New("commit boom") }, "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := seed(t)
			tc.inj(f)
			task := anTask
			task.Fresh = tc.fresh
			if _, err := f.w.ProcessResult(context.Background(), task); err == nil {
				t.Fatal("ожидана ошибка доставки")
			} else if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ошибка = %v, нужна подстрока %q", err, tc.want)
			}
			// Откат: прежнее состояние строки домена сохранилось.
			row, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain)
			if tc.hadRow != ok || (ok && row.rulesVersion != 999) {
				t.Fatalf("строка домена изменилась при сбое: %+v (ok=%v)", row, ok)
			}
			if _, ok := anFindingSnapshot(f.pool, anKey); ok {
				t.Fatal("findings записаны при сбое агрегации")
			}
		})
	}
}

// TestAggregate_CtxCancelled — отмена ctx прерывает агрегацию: ошибка,
// ничего не пишется (graceful shutdown, ТЗ 3.4).
func TestAggregate_CtxCancelled(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	tx, err := f.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := aggregateDomain(ctx, tx, anKey.zone, anKey.domain, 1001, false); err == nil {
		t.Fatal("отменённый ctx — ошибка агрегации")
	}
}

// mustMask — маска фикстуры (падение — ошибка теста, не бизнеса).
func mustMask(t *testing.T, ids ...int) model.DNSMask {
	t.Helper()
	m, err := model.NewDNSMask(ids...)
	if err != nil {
		t.Fatalf("NewDNSMask: %v", err)
	}
	return m
}

// --- история детекции appeared/lost (T-118) -----------------------------------

// anHistorySnapshot — снапшот журнала переходов фикстуры.
func anHistorySnapshot(p *anFakePool) []anHistoryRec {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]anHistoryRec(nil), p.history...)
}

// TestAggregate_History — семантика журнала переходов laravel_history
// (T-118): запись только при свежем эпизоде HTTP (queue.Result.Fresh) —
// появление строки → appeared=true, исчезновение → false; повторный
// пересчёт без смены наличия строки историю не пишет; переанализ
// (fresh=false) не пишет ничего — смена правил не факт о сайте.
func TestAggregate_History(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	ctx := context.Background()

	runTask := func(task queue.Result) {
		t.Helper()
		if _, err := f.w.ProcessResult(ctx, task); err != nil {
			t.Fatalf("ProcessResult: %v", err)
		}
	}

	// Эпизод 1 (fresh): страница с признаками → строка появилась.
	laravelHash := anTestHash(7)
	f.store.blocks[laravelHash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), laravelHash[:])
	task := anTask
	task.Fresh = true
	runTask(task)

	hist := anHistorySnapshot(f.pool)
	if len(hist) != 1 || !hist[0].appeared || hist[0].domain != anKey.domain {
		t.Fatalf("история после появления = %+v, хочу одну запись appeared=true", hist)
	}

	// Эпизод 2 (fresh): те же признаки — строка есть, перехода нет.
	runTask(task)
	if hist = anHistorySnapshot(f.pool); len(hist) != 1 {
		t.Fatalf("повторный пересчёт без смены наличия: история = %+v, хочу без изменений", hist)
	}

	// Эпизод 3 (fresh): страница без признаков — findings пусты, строка
	// удалена → appeared=false.
	plainHash := anTestHash(8)
	f.store.blocks[plainHash] = []byte(`<html><body>nothing here</body></html>`)
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t), plainHash[:])
	runTask(task)

	hist = anHistorySnapshot(f.pool)
	if len(hist) != 2 || hist[1].appeared {
		t.Fatalf("история после исчезновения = %+v, хочу вторую запись appeared=false", hist)
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Fatal("строка laravel_domains не удалена")
	}

	// Переанализ появления (fresh=false): findings появятся, строка
	// запишется — история не растёт (смена правил не факт о сайте).
	rKey := anRowKey{zone: 3, domain: 43, subdomain: 1, variant: 0}
	f.pool.seedResult(rKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), laravelHash[:])
	rTask := queue.Result{
		ZoneID: rKey.zone, DomainID: rKey.domain,
		SubdomainID: rKey.subdomain, Variant: 0, Name: "reanalyzed.example",
	}
	runTask(rTask)
	if _, ok := anDomainSnapshot(f.pool, rKey.zone, rKey.domain); !ok {
		t.Fatal("строка переанализированного домена не записана")
	}
	if hist = anHistorySnapshot(f.pool); len(hist) != 2 {
		t.Fatalf("переанализ-появление пишeт историю: %+v, хочу без изменений (fresh=false)", hist)
	}

	// Переанализ исчезновения (fresh=false): findings пусты, строка
	// удалена — история не растёт.
	f.pool.seedResult(rKey, anHeaders(t), anCookies(t), plainHash[:])
	runTask(rTask)
	if _, ok := anDomainSnapshot(f.pool, rKey.zone, rKey.domain); ok {
		t.Fatal("строка переанализированного домена не удалена")
	}
	if hist = anHistorySnapshot(f.pool); len(hist) != 2 {
		t.Fatalf("переанализ-исчезновение пишет историю: %+v, хочу без изменений (fresh=false)", hist)
	}

	// Пустой fresh-эпизод без строки (DELETE 0): перехода нет — записи нет.
	runTask(task)
	if hist = anHistorySnapshot(f.pool); len(hist) != 2 {
		t.Fatalf("удаление отсутствующей строки пишет историю: %+v, хочу без изменений", hist)
	}
}

// anStrEq / anStrDeref — сравнение и печать NULL-границ.
func anStrEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func anStrDeref(s *string) any {
	if s == nil {
		return "NULL"
	}
	return *s
}

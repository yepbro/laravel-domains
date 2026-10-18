// Юнит-тесты crawlerctl near (T-115) на фейковом pgx-пуле: точный
// SQL-текст выборки (контракт, стиль status_test.go), живая семантика
// фейка (диапазон серой зоны, фильтр зоны, сортировка score DESC,
// расшифровка signals), вывод строк и сигналов; run() без конфигурации —
// ошибка выполнения до соединений. Поведение против живой БД —
// dev-окружение (crawlerctl near на dev-стеке).
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
)

// wantNearSelectSQL — ожидаемый текст выборки серой зоны.
const wantNearSelectSQL = "SELECT f.zone_id, f.domain_id, d.label || '.' || z.name AS name, " +
	"f.subdomain_id, COALESCE(s.label, '') AS subdomain, f.variant, " +
	"f.score, COALESCE(f.ver_min, ''), COALESCE(f.ver_max, ''), f.is_laravel, f.signals " +
	"FROM laravel_findings AS f " +
	"JOIN domains AS d ON d.zone_id = f.zone_id AND d.id = f.domain_id " +
	"JOIN zones AS z ON z.id = f.zone_id " +
	"LEFT JOIN subdomains AS s ON s.id = f.subdomain_id " +
	"WHERE ($1::int2 IS NULL OR f.zone_id = $1) " +
	"AND f.score >= $2::real AND f.score < $3::real " +
	"ORDER BY f.score DESC, f.zone_id, f.domain_id, f.subdomain_id, f.variant"

// --- фейковый пул (db.DB) ----------------------------------------------------

// nearSeed — находка в фейке: домен + score + signals.
type nearSeed struct {
	zone       int16
	domain     int64
	name       string
	subID      int16
	sub        string
	variant    int16
	score      float32
	verMin     string
	verMax     string
	isLaravel  bool
	signalsJxn string // JSON массива signals
}

// nearPool — db.DB без БД: живая семантика nearSelectSQL (фильтры зоны и
// диапазона, ORDER BY score DESC; доменный ключ в хвосте сортировки
// фейком не моделируется — score сидов в тестах уникальны).
type nearPool struct {
	seeds []nearSeed

	mu       sync.Mutex
	queries  []string
	args     [][]any
	queryErr error
}

var _ db.DB = (*nearPool)(nil)

func (p *nearPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("nearPool: CopyFrom не должен вызываться")
}

func (p *nearPool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("nearPool: неожидаемый Exec: %s", sql)
}

func (p *nearPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, sql)
	p.args = append(p.args, args)
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	if sql != nearSelectSQL {
		return nil, fmt.Errorf("nearPool: неожидаемый SQL Query: %s", sql)
	}
	return p.selectRows(args), nil
}

// selectRows применяет семантику nearSelectSQL к сидам.
func (p *nearPool) selectRows(args []any) pgx.Rows {
	var zoneArg *int16
	if args[0] != nil {
		z := args[0].(int16)
		zoneArg = &z
	}
	lo := args[1].(float32)
	hi := args[2].(float32)

	var rows [][]any
	for _, s := range p.seeds {
		if zoneArg != nil && s.zone != *zoneArg {
			continue
		}
		if s.score < lo || s.score >= hi {
			continue
		}
		rows = append(rows, []any{
			s.zone, s.domain, s.name, s.subID, s.sub, s.variant,
			s.score, s.verMin, s.verMax, s.isLaravel, []byte(s.signalsJxn),
		})
	}
	sort.Slice(rows, func(i, j int) bool { // ORDER BY score DESC
		return rows[i][6].(float32) > rows[j][6].(float32)
	})
	return statusRowsOf(rows...)
}

// --- тесты --------------------------------------------------------------------

// TestNearSQLContract — текст выборки зафиксирован: стартовые столбцы,
// JOIN-ы, фильтры диапазона и зоны, порядок.
func TestNearSQLContract(t *testing.T) {
	if nearSelectSQL != wantNearSelectSQL {
		t.Errorf("nearSelectSQL:\n%s\nwant:\n%s", nearSelectSQL, wantNearSelectSQL)
	}
}

// TestNearFetch — живая семантика фейка: диапазон серой зоны, фильтр
// зоны, сортировка score DESC, разбор signals.
func TestNearFetch(t *testing.T) {
	pool := &nearPool{
		seeds: []nearSeed{
			{zone: 3, domain: 11, name: "alpha.com", subID: 0, sub: "", score: 0.5875,
				signalsJxn: `[{"rule":"cookie_xsrf_token","weight":0.25,"evidence":"XSRF-TOKEN"},{"rule":"cookie_encrypted_payload","weight":0.45,"evidence":"eyJpdiI6"}]`},
			{zone: 3, domain: 12, name: "beta.com", subID: 1, sub: "www", variant: 1, score: 0.30,
				signalsJxn: `[{"rule":"meta_generator","weight":0.3,"evidence":"Laravel"}]`},
			{zone: 5, domain: 13, name: "gamma.org", subID: 1, sub: "www", score: 0.40,
				signalsJxn: `[{"rule":"welcome_page","weight":0.4,"evidence":"5.x"}]`},
			// Вне диапазона: слишком слабая и подтверждённая.
			{zone: 3, domain: 14, name: "weak.com", score: 0.25, signalsJxn: `[]`},
			{zone: 3, domain: 15, name: "strong.com", score: 0.65, signalsJxn: `[]`},
		},
	}

	rows, err := nearFetch(context.Background(), pool, nil, 0.6)
	if err != nil {
		t.Fatalf("nearFetch: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("строк = %d, хочу 3 (диапазон [0.3 .. 0.6))", len(rows))
	}
	if rows[0].Name != "alpha.com" || rows[0].Score != 0.5875 {
		t.Errorf("первая строка = %s %.4f, хочу alpha.com 0.5875 (score DESC)", rows[0].Name, rows[0].Score)
	}
	if len(rows[0].Signals) != 2 || rows[0].Signals[0].Rule != "cookie_xsrf_token" {
		t.Errorf("signals alpha.com = %+v, хочу два правила с разбором JSON", rows[0].Signals)
	}

	// Фильтр зоны: только com.
	zoneID := int16(3)
	rows, err = nearFetch(context.Background(), pool, &zoneID, 0.6)
	if err != nil {
		t.Fatalf("nearFetch зоны: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("строк зоны com = %d, хочу 2", len(rows))
	}
	for _, r := range rows {
		if r.ZoneID != 3 {
			t.Errorf("строка вне зоны: %+v", r)
		}
	}

	// Параметры первого вызова — зона nil (все) и границы диапазона.
	first := pool.args[0]
	if first[0] != nil {
		t.Errorf("args[0] = %v, хочу nil для зоны без фильтра", first[0])
	}
	if lo := first[1].(float32); lo != 0.3 {
		t.Errorf("нижняя граница = %v, хочу 0.3 (min/2)", lo)
	}
	if hi := first[2].(float32); hi != 0.6 {
		t.Errorf("верхняя граница = %v, хочу 0.6 (min)", hi)
	}
}

// TestNearFetchErrors — ошибки БД и пустой справочник зон не роняют
// корректный путь, но завершают команду.
func TestNearFetchErrors(t *testing.T) {
	t.Run("ошибка БД", func(t *testing.T) {
		pool := &nearPool{queryErr: errors.New("бд недоступна")}
		if _, err := nearFetch(context.Background(), pool, nil, 0.6); err == nil {
			t.Fatal("nearFetch с ошибкой БД = nil, хочу ошибку")
		}
	})
	t.Run("мусор в signals", func(t *testing.T) {
		pool := &nearPool{seeds: []nearSeed{
			{zone: 3, domain: 1, name: "x.com", score: 0.4, signalsJxn: `{не-json`},
		}}
		if _, err := nearFetch(context.Background(), pool, nil, 0.6); err == nil ||
			!strings.Contains(err.Error(), "signals") {
			t.Fatalf("ошибка = %v, хочу отказ разбора signals", err)
		}
	})
}

// TestNearPrintRow — формат вывода: домен, субдомен/вариант, score,
// версии, сигналы с evidence; пустая метка — апекс.
func TestNearPrintRow(t *testing.T) {
	var buf bytes.Buffer
	nearPrintRow(&buf, nearRow{
		Name: "example.com", Subdomain: "www", Variant: 1, Score: 0.5,
		VerMin: "9.19", VerMax: "12", Signals: []domainSignal{
			{Rule: "cookie_xsrf_token", Weight: 0.25, Evidence: "XSRF-TOKEN"},
		},
	})
	got := buf.String()
	for _, want := range []string{
		"example.com  www/http  score=0.500  ver=[9.19..12]  правил=1",
		"cookie_xsrf_token", "XSRF-TOKEN",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("вывод не содержит %q:\n%s", want, got)
		}
	}

	buf.Reset()
	nearPrintRow(&buf, nearRow{Name: "a.com", Subdomain: "", Score: 0.35})
	if !strings.Contains(buf.String(), "a.com  (апекс)/main") {
		t.Errorf("пустая метка не отображена как апекс:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "ver=-") {
		t.Errorf("пустые версии не отображены прочерком:\n%s", buf.String())
	}
}

// TestRunNear — без конфигурации (нет YAML и окружения) команда завершается
// кодом 1 до соединений (стиль TestRunStatus).
func TestRunNear(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	var stdout, stderr bytes.Buffer
	code := run([]string{"near"}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("код = %d, хочу %d; stderr:\n%s", code, exitError, stderr.String())
	}
	if !strings.Contains(stderr.String(), "near: config:") {
		t.Errorf("stderr не содержит контекста конфигурации:\n%s", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout до соединения не пуст:\n%s", stdout.String())
	}
}

// TestRunNearUsage — порог вне (0, 1) и лишние аргументы — код 2.
func TestRunNearUsage(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	for _, args := range [][]string{
		{"near", "--min-score", "1.5"},
		{"near", "--min-score", "0"},
		{"near", "лишний"},
	} {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		if code != exitUsage {
			t.Errorf("run(%v) = %d, хочу %d; stderr:\n%s", args, code, exitUsage, stderr.String())
		}
	}
}

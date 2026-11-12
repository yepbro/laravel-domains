// Unit tests for crawlerctl near (T-115) on a fake pgx pool: the exact
// SQL text of the selection (a contract, the status_test.go style), the
// fake's live semantics (the gray-zone range, the zone filter, the
// score DESC sort, the signals decoding), the row and signal output;
// run() without a configuration — an execution error before connections.
// Behavior against the live DB — the dev environment (crawlerctl near on
// the dev stack).
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

// wantNearSelectSQL — the expected gray-zone selection text.
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

// --- fake pool (db.DB) -------------------------------------------------------

// nearSeed — a finding in the fake: domain + score + signals.
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
	signalsJxn string // JSON of the signals array
}

// nearPool — a db.DB without a DB: the live semantics of nearSelectSQL
// (the zone and range filters, ORDER BY score DESC; the domain key at the
// sort tail is not modeled by the fake — seed scores in the tests are
// unique).
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

// selectRows applies the nearSelectSQL semantics to the seeds.
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

// --- tests --------------------------------------------------------------------

// TestNearSQLContract — the selection text is pinned: the leading columns,
// the JOINs, the range and zone filters, the order.
func TestNearSQLContract(t *testing.T) {
	if nearSelectSQL != wantNearSelectSQL {
		t.Errorf("nearSelectSQL:\n%s\nwant:\n%s", nearSelectSQL, wantNearSelectSQL)
	}
}

// TestNearFetch — the fake's live semantics: the gray-zone range, the
// zone filter, the score DESC sort, the signals parsing.
func TestNearFetch(t *testing.T) {
	pool := &nearPool{
		seeds: []nearSeed{
			{zone: 3, domain: 11, name: "alpha.com", subID: 0, sub: "", score: 0.5875,
				signalsJxn: `[{"rule":"cookie_xsrf_token","weight":0.25,"evidence":"XSRF-TOKEN"},{"rule":"cookie_encrypted_payload","weight":0.45,"evidence":"eyJpdiI6"}]`},
			{zone: 3, domain: 12, name: "beta.com", subID: 1, sub: "www", variant: 1, score: 0.30,
				signalsJxn: `[{"rule":"meta_generator","weight":0.3,"evidence":"Laravel"}]`},
			{zone: 5, domain: 13, name: "gamma.org", subID: 1, sub: "www", score: 0.40,
				signalsJxn: `[{"rule":"welcome_page","weight":0.4,"evidence":"5.x"}]`},
			// Out of range: too weak and already confirmed.
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

	// Zone filter: com only.
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

	// The parameters of the first call — a nil zone (all) and the range bounds.
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

// TestNearFetchErrors — DB errors and a garbage signals payload do not
// crash a correct path but finish the command.
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

// TestNearPrintRow — the output format: domain, subdomain/variant, score,
// versions, signals with evidence; an empty label — the apex.
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

// TestRunNear — without a configuration (no YAML and no environment) the
// command exits with code 1 before connections (the TestRunStatus style).
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

// TestRunNearUsage — a threshold outside (0, 1) and extra arguments — exit code 2.
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

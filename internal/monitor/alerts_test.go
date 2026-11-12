// Unit tests of the monitoring alerts (T-080, spec 5.9, 8, 10.3) on fake
// sources: a pgx pool (the temp_error share, zones.updated_at), stream XLENs,
// the statfs of the content directory, the NVMe wear. Covered: SQL contracts, thresholds and
// hysteresis at exact boundaries (firing/clearing, the sample minimum,
// the stall factor, the junk hash window, the bytes format), degradations
// of sources in all forms (a query error, an empty answer, a read error,
// an iteration error — a Warn, the loop keeps living), the SMART degradation in a container
// (without a Warn and without a false firing), the front log entries, the hook
// OnPass, the AlertsLoop cycle. Real SMART reading — behind the devices;
// behavior against a live DB — test/integration.
package monitor

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/queue"
)

// --- fake sources ------------------------------------------------------------

// alertDNSDaily — the answer of dnsDailyCountersSQL: the numerator (the temp_error events
// of the day) and the denominator (the write events) of the temp_error share (T-182).
type alertDNSDaily struct {
	temp  int64
	total int64
}

// alertsPool — a db.DB without a DB: the routing of the three alert SQLs, the
// queues of results and errors, the call protocol. Under a lock: AlertsLoop runs
// in a goroutine, the tests wait for progress via the pass counter.
type alertsPool struct {
	mu    sync.Mutex
	daily []alertDNSDaily  // the queue of answers of dnsDailyCountersSQL (exhausted — zeros)
	fresh []sql.NullTime   // the queue of answers of zoneFreshSQL (exhausted — zero)
	debt  []int64          // the queue of answers of renderDebtFlagsSQL (exhausted — zeros)
	stale []alertStaleSnap // the queue of answers of newDoneStaleSQL (exhausted — zeros)

	dailyErrs []error
	freshErrs []error
	debtErrs  []error
	staleErrs []error

	// Answer-shape degradations: no rows (rows.Next false), a row with
	// unreadable types (a Scan error), an iteration error (rows.Err).
	dailyEmpty, dailyBadRow    bool
	freshEmpty, freshBadRow    bool
	debtEmpty, debtBadRow      bool
	staleEmpty, staleBadRow    bool
	dailyRowsErr, freshRowsErr error
	debtRowsErr                error
	staleRowsErr               error

	queries []zonestatsCall // reused: sql + args
}

// alertStaleSnap — the answer of newDoneStaleSQL: the value of the new_done_stale row
// and the snapshot stamp (updated_at — the moment of the reconciliation pass that wrote it).
type alertStaleSnap struct {
	value int64
	at    time.Time
}

func (p *alertsPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *alertsPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

func (p *alertsPool) Query(_ context.Context, sqlQ string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, zonestatsCall{sql: sqlQ, args: args})
	switch sqlQ {
	case dnsDailyCountersSQL:
		if err := popErr(&p.dailyErrs); err != nil {
			return nil, err
		}
		if p.dailyEmpty {
			return &alertRows{err: p.dailyRowsErr}, nil
		}
		if p.dailyBadRow {
			return &alertRows{rows: [][]any{{"байты", "байты"}}, err: p.dailyRowsErr}, nil
		}
		d := alertDNSDaily{}
		if len(p.daily) > 0 {
			d = p.daily[0]
			p.daily = p.daily[1:]
		}
		return &alertRows{rows: [][]any{{d.temp, d.total}}, err: p.dailyRowsErr}, nil
	case zoneFreshSQL:
		if err := popErr(&p.freshErrs); err != nil {
			return nil, err
		}
		if p.freshEmpty {
			return &alertRows{err: p.freshRowsErr}, nil
		}
		if p.freshBadRow {
			return &alertRows{rows: [][]any{{"байты"}}, err: p.freshRowsErr}, nil
		}
		t := sql.NullTime{}
		if len(p.fresh) > 0 {
			t = p.fresh[0]
			p.fresh = p.fresh[1:]
		}
		return &alertRows{rows: [][]any{{t}}, err: p.freshRowsErr}, nil
	case renderDebtFlagsSQL:
		if err := popErr(&p.debtErrs); err != nil {
			return nil, err
		}
		if p.debtEmpty {
			return &alertRows{err: p.debtRowsErr}, nil
		}
		if p.debtBadRow {
			return &alertRows{rows: [][]any{{"байты"}}, err: p.debtRowsErr}, nil
		}
		var v int64
		if len(p.debt) > 0 {
			v = p.debt[0]
			p.debt = p.debt[1:]
		}
		return &alertRows{rows: [][]any{{v}}, err: p.debtRowsErr}, nil
	case newDoneStaleSQL:
		if err := popErr(&p.staleErrs); err != nil {
			return nil, err
		}
		if p.staleEmpty {
			return &alertRows{err: p.staleRowsErr}, nil
		}
		if p.staleBadRow {
			return &alertRows{rows: [][]any{{"байты", "байты"}}, err: p.staleRowsErr}, nil
		}
		s := alertStaleSnap{}
		if len(p.stale) > 0 {
			s = p.stale[0]
			p.stale = p.stale[1:]
		}
		return &alertRows{rows: [][]any{{s.value, sql.NullTime{Time: s.at, Valid: !s.at.IsZero()}}}, err: p.staleRowsErr}, nil
	default:
		return nil, fmt.Errorf("alertsPool: неожидаемый SQL Query: %s", sqlQ)
	}
}

// popErr pops the head of the error queue (nil — no error); called under
// the owner's lock.
func popErr(q *[]error) error {
	if len(*q) == 0 {
		return nil
	}
	err := (*q)[0]
	*q = (*q)[1:]
	return err
}

// calls — a snapshot of the Query protocol under the lock.
func (p *alertsPool) calls() []zonestatsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]zonestatsCall(nil), p.queries...)
}

// alertRows — pgx.Rows with prepared rows (Scan is positional; the types
// int64/sql.NullTime — everything the alerts read). The statusRows
// style (cmd/crawlerctl/status_test.go). err — the rows.Err answer (nil by default).
type alertRows struct {
	rows [][]any
	i    int
	err  error
}

func (r *alertRows) Close()                                       {}
func (r *alertRows) Err() error                                   { return r.err }
func (r *alertRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *alertRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *alertRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *alertRows) RawValues() [][]byte                          { return nil }
func (r *alertRows) Conn() *pgx.Conn                              { return nil }
func (r *alertRows) TypeMap() *pgtype.Map                         { return nil }

func (r *alertRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *alertRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("alertRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("alertRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *int64:
			n, ok := v.(int64)
			if !ok {
				return fmt.Errorf("alertRows: колонка %d: %T не int64", i, v)
			}
			*dst = n
		case *sql.NullTime:
			t, ok := v.(sql.NullTime)
			if !ok {
				return fmt.Errorf("alertRows: колонка %d: %T не sql.NullTime", i, v)
			}
			*dst = t
		default:
			return fmt.Errorf("alertRows: приёмник %d: %T не поддерживается", i, d)
		}
	}
	return nil
}

// fakeLener — XLEN via a map (no stream — 0), an error queue, a protocol.
type fakeLener struct {
	mu    sync.Mutex
	lens  map[string]int64
	errs  []error
	calls []string
}

func (f *fakeLener) Len(_ context.Context, stream string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, stream)
	if err := popErr(&f.errs); err != nil {
		return 0, err
	}
	return f.lens[stream], nil
}

func (f *fakeLener) streams() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// fakeSpace — the statfs of the content directory per a stub.
type fakeSpace struct {
	mu    sync.Mutex
	stat  SpaceStat
	err   error
	calls int
}

func (f *fakeSpace) Space(string) (SpaceStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.stat, f.err
}

// fakeWear — the NVMe wear per a stub.
type fakeWear struct {
	mu    sync.Mutex
	wears []NVMeWear
	err   error
	calls int
}

func (f *fakeWear) Wear(context.Context) ([]NVMeWear, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.wears, f.err
}

// fakeUnitEvents is declared in unitevents_test.go (the error queue errs,
// the calls protocol) — reused by the memory_high alert too (T-162).

// fakeStreamGroups — the working stream groups per a stub (T-162).
type fakeStreamGroups struct {
	mu     sync.Mutex
	groups []StreamGroupState
	err    error
	calls  int
}

func (f *fakeStreamGroups) StreamGroups(context.Context) ([]StreamGroupState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.groups, f.err
}

// fakePSI — the CPU pressure per a stub (T-162).
type fakePSI struct {
	mu    sync.Mutex
	psi   PSIStat
	err   error
	calls int
}

func (f *fakePSI) PSI(context.Context) (PSIStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.psi, f.err
}

// --- assembling the test evaluator -------------------------------------------

// alertsTestEnv — an evaluator on fakes with a log buffer; the defaults are healthy:
// a 0% share, fresh zones, empty queues and untracked groups, enough
// space (also for the absolute minimum), low wear, units without events,
// no CPU pressure.
type alertsTestEnv struct {
	*Alerts
	pool     *alertsPool
	lener    *fakeLener
	space    *fakeSpace
	wear     *fakeWear
	units    *fakeUnitEvents
	groups   *fakeStreamGroups
	psi      *fakePSI
	redisMem *fakeRedisMem
	buf      *bytes.Buffer
}

// alertsFixedClock — the same fixed clock as the zonestats tests.
func alertsFixedClock() (time.Time, func() time.Time) {
	return zonestatsFixedClock()
}

// alertHealthyShares — a queue of healthy dnsDailyCountersSQL answers
// (a 0% share: no temp_error events) for n passes: the tests not busy
// with the error share do not hit an empty queue.
func alertHealthyShares(n int) []alertDNSDaily {
	daily := make([]alertDNSDaily, n)
	for i := range daily {
		daily[i] = alertDNSDaily{temp: 0, total: 1000}
	}
	return daily
}

// alertHealthyFresh — a queue of fresh zoneFreshSQL answers (a minute ago)
// for n passes.
func alertHealthyFresh(now time.Time, n int) []sql.NullTime {
	fresh := make([]sql.NullTime, n)
	for i := range fresh {
		fresh[i] = sql.NullTime{Time: now.Add(-time.Minute), Valid: true}
	}
	return fresh
}

func newAlertsTestEnv(t *testing.T, mod func(o *AlertsOptions)) *alertsTestEnv {
	t.Helper()
	now, clock := alertsFixedClock()
	pool := &alertsPool{
		daily: alertHealthyShares(10),
		fresh: alertHealthyFresh(now, 10),
	}
	lener := &fakeLener{lens: map[string]int64{}}
	space := &fakeSpace{stat: SpaceStat{Avail: 100 << 30, Total: 200 << 30}}
	wear := &fakeWear{wears: []NVMeWear{{Device: "nvme0", PercentUsed: 10}}}
	units := &fakeUnitEvents{units: []UnitEvents{{Unit: "crawler-http-worker.service"}}}
	groups := &fakeStreamGroups{}
	psi := &fakePSI{}
	redisMem := &fakeRedisMem{mem: RedisMemory{UsedBytes: 100 << 20, MaxBytes: 512 << 20, Policy: "noeviction"}}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	opts := AlertsOptions{
		ZoneStatsInterval: time.Minute,
		QueueWatermark:    1000,
		ContentDir:        "/var/content",
		DiskFreePercent:   5,
		Now:               clock,
		Log:               logger,
	}
	if mod != nil {
		mod(&opts)
	}
	return &alertsTestEnv{
		Alerts: NewAlerts(AlertsDeps{Pool: pool, Redis: lener, Space: space, Wear: wear,
			UnitEvents: units, StreamGroups: groups, PSI: psi, RedisMemory: redisMem}, opts),
		pool: pool, lener: lener, space: space, wear: wear,
		units: units, groups: groups, psi: psi, redisMem: redisMem, buf: &buf,
	}
}

// snapshotOf — the alert state by name from the pass snapshot.
func snapshotOf(t *testing.T, res AlertsResult, name string) AlertSnapshot {
	t.Helper()
	for _, s := range res.Alerts {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("алерт %s отсутствует в снапшоте %+v", name, res.Alerts)
	return AlertSnapshot{}
}

// --- the DNS error share ------------------------------------------------------

// TestAlertsDNSContract — the exact share SQL: two point reads of the daily
// crawler_counters rows (the day names are computed by the DB, coalesce outside
// the subqueries), no parameters and no scan of domains (T-182 — the former
// dnsShareSQL aggregated the whole day window); a share ≥ the threshold — an alert.
func TestAlertsDNSContract(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.daily = []alertDNSDaily{{temp: 350, total: 1000}}

	res := env.Pass(context.Background())
	s := snapshotOf(t, res, AlertDNSErrors)
	if s.Status != AlertStatusFiring {
		t.Fatalf("статус = %s (%s), want firing: доля 35%% ≥ 30%%", s.Status, s.Detail)
	}
	for _, want := range []string{"35.0%", "350", "1000"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail %q не содержит %s", s.Detail, want)
		}
	}

	calls := env.pool.calls()
	var dns []zonestatsCall
	for _, c := range calls {
		if c.sql == dnsDailyCountersSQL {
			dns = append(dns, c)
		}
	}
	if len(dns) != 1 {
		t.Fatalf("запросов dnsDailyCountersSQL = %d, want 1", len(dns))
	}
	if len(dns[0].args) != 0 {
		t.Errorf("аргументы = %v, want пусто (имена дней вычисляет БД)", dns[0].args)
	}
	// The names of the daily rows — DB expressions (current_date); the outer
	// coalesce gives an honest zero before the first write of the day; there is no
	// domains window in the SQL.
	for _, want := range []string{
		"'temp_error:' || to_char(current_date, 'YYYY-MM-DD')",
		"'checks:' || to_char(current_date, 'YYYY-MM-DD')",
		"coalesce((SELECT value FROM crawler_counters",
	} {
		if !strings.Contains(dnsDailyCountersSQL, want) {
			t.Errorf("dnsDailyCountersSQL без %q:\n%s", want, dnsDailyCountersSQL)
		}
	}
	if strings.Contains(dnsDailyCountersSQL, "FROM domains") {
		t.Errorf("dnsDailyCountersSQL читает domains — скан окна суток обязан исчезнуть (T-182):\n%s", dnsDailyCountersSQL)
	}
}

// TestAlertsDNSHysteresis — firing at 30%, holding in the corridor
// 20–30%, clearing below 20%; before firing a share in the corridor does not fire
// the alert.
func TestAlertsDNSHysteresis(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newDNSMachine()
	steps := []struct {
		daily alertDNSDaily
		want  string
	}{
		{alertDNSDaily{300, 1000}, AlertStatusFiring}, // 30.0% — firing
		{alertDNSDaily{250, 1000}, AlertStatusFiring}, // 25.0% — the hysteresis corridor
		{alertDNSDaily{200, 1000}, AlertStatusFiring}, // exactly 20.0% — hold (the clearing boundary)
		{alertDNSDaily{199, 1000}, AlertStatusOK},     // 19.9% — clearing
		{alertDNSDaily{250, 1000}, AlertStatusOK},     // 25% without a fired alert — does not fire
	}
	for i, s := range steps {
		got := m.transition(env.Alerts, ctx, obs(dnsShare{temp: s.daily.temp, total: s.daily.total}))
		if got.Status != s.want {
			t.Errorf("шаг %d (доля %d/%d): статус = %s (%s), want %s",
				i, s.daily.temp, s.daily.total, got.Status, got.Detail, s.want)
		}
	}
}

// TestAlertsDNSMinSamples — an empty day counter (no rows yet — an honest
// zero) and a small window (fewer than 100 denominator events): the share is
// not indicative — "not measured", not a firing. Exactly 100 is already
// indicative: the boundary is strict (less than, not "no more than").
func TestAlertsDNSMinSamples(t *testing.T) {
	env := newAlertsTestEnv(t, nil)

	// Before the first write of the day both rows are absent: coalesce gives zeros
	// — "not measured", not a false firing (TZ_slow_dns_share §7).
	env.pool.daily = []alertDNSDaily{{temp: 0, total: 0}}
	s := snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("пустой счётчик: статус = %s (%s), want not_measured", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "0") || !strings.Contains(s.Detail, "100") {
		t.Errorf("detail %q без чисел выборки", s.Detail)
	}

	env.pool.daily = []alertDNSDaily{{temp: 2, total: 50}}
	s = snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("статус = %s (%s), want not_measured", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "50") || !strings.Contains(s.Detail, "100") {
		t.Errorf("detail %q без чисел выборки", s.Detail)
	}

	// Exactly the sample threshold — the share is computed (0 of 100 — a correct zero).
	env.pool.daily = []alertDNSDaily{{temp: 0, total: 100}}
	s = snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "из 100") {
		t.Errorf("ровно 100 проверок: статус = %s (%s), want ok с долей по 100", s.Status, s.Detail)
	}
}

// TestAlertsDNSSourceError — a query error: a Warn, the alert "not measured",
// the next pass works (the loop keeps living).
func TestAlertsDNSSourceError(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.dailyErrs = []error{errors.New("бд недоступна")}

	res := env.Pass(context.Background())
	s := snapshotOf(t, res, AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("статус = %s, want not_measured", s.Status)
	}
	if !strings.Contains(s.Detail, "бд недоступна") {
		t.Errorf("detail = %q, want причина ошибки источника", s.Detail)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "бд недоступна") {
		t.Errorf("ошибка источника без Warn:\n%s", env.buf.String())
	}

	s = snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusOK {
		t.Errorf("после ошибки статус = %s, want ok (цикл живёт)", s.Status)
	}
}

// TestAlertsDNSSourceErrors — the answer-shape degradations: an empty result
// (a scalar SELECT must return a row — an empty set is a protocol
// failure), an unreadable row (a Scan error), an iteration error (rows.Err)
// — source errors with a Warn and the reason in detail, not a silent "too
// little data" via zeros.
func TestAlertsDNSSourceErrors(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.dailyEmpty = true
	s := snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "пустой ответ") {
		t.Errorf("пустой ответ: статус = %s (%s), want not_measured «пустой ответ»", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("пустой ответ без Warn:\n%s", env.buf.String())
	}

	env = newAlertsTestEnv(t, nil)
	env.pool.dailyBadRow = true
	s = snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "чтение") {
		t.Errorf("ошибка Scan: статус = %s (%s), want not_measured «чтение»", s.Status, s.Detail)
	}

	env = newAlertsTestEnv(t, nil)
	env.pool.dailyRowsErr = errors.New("соединение сброшено")
	s = snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "соединение сброшено") {
		t.Errorf("rows.Err: статус = %s (%s), want not_measured с ошибкой итерации", s.Status, s.Detail)
	}
}

// --- queue overflow -----------------------------------------------------------

// TestAlertsQueuesThresholds — firing at the gate threshold (len ≥
// watermark), holding up to 90% of the threshold, clearing below; the streams of the
// queue alert are only the working ones, it does not poll q:dead (the
// quarantine depth is read by the separate queue_dead alert, T-154).
func TestAlertsQueuesThresholds(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.lener.lens[queue.StreamDead] = 1 << 40 // not counted by the queue alert
	ctx := context.Background()
	m := env.newQueuesMachine()
	steps := []struct {
		lens map[string]int64
		want string
	}{
		{map[string]int64{}, AlertStatusOK},                          // empty queues: the detail names a stream, not ""
		{map[string]int64{queue.StreamDNS: 124}, AlertStatusOK},      // below the q:dns threshold (capped at MAXLEN/2 = 125, T-141)
		{map[string]int64{queue.StreamDNS: 125}, AlertStatusFiring},  // the q:dns gate threshold
		{map[string]int64{queue.StreamHTTP: 950}, AlertStatusFiring}, // the hysteresis corridor
		{map[string]int64{queue.StreamHTTP: 900}, AlertStatusFiring}, // exactly 90% of the threshold — hold
		{map[string]int64{queue.StreamHTTP: 899}, AlertStatusOK},     // below 90%
	}
	for i, s := range steps {
		got := m.transition(env.Alerts, ctx, obs(s.lens))
		if got.Status != s.want {
			t.Errorf("шаг %d (%v): статус = %s (%s), want %s", i, s.lens, got.Status, got.Detail, s.want)
		}
		// The hysteresis corridor (no hot stream): the detail names the
		// holding stream and the depth, not an empty threshold string.
		if i == 3 && !strings.Contains(got.Detail, "q:http=950") {
			t.Errorf("коридор гистерезиса: detail = %q, want q:http=950", got.Detail)
		}
		if i == 0 && !strings.Contains(got.Detail, "q:dns=0") {
			t.Errorf("пустые очереди: detail = %q, want макс q:dns=0 (имя стрима, не пустота)", got.Detail)
		}
	}

	// The "ok" detail — the maximum among the working streams: the update in the loop
	// (the leader is not the first stream) and the starting value (the leader being the first stream
	// is read from the map, not as the zero of a neighbor).
	if s := m.transition(env.Alerts, ctx, obs(map[string]int64{queue.StreamDNS: 3})); s.Status != AlertStatusOK || !strings.Contains(s.Detail, "макс q:dns=3") {
		t.Errorf("максимум q:dns: %s (%s), want ok макс q:dns=3", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, obs(map[string]int64{queue.StreamHTTP: 5})); s.Status != AlertStatusOK || !strings.Contains(s.Detail, "макс q:http=5") {
		t.Errorf("максимум q:http: %s (%s), want ok макс q:http=5", s.Status, s.Detail)
	}

	// The queue alert does not poll q:dead even at a huge quarantine
	// depth: a live pass reads only the working streams; q:dead polls
	// in the protocol — only the queue_dead alert.
	_ = snapshotOf(t, env.Pass(ctx), AlertQueueOverflow)
	deadPolls := 0
	for _, s := range env.lener.streams() {
		if s == queue.StreamDead {
			deadPolls++
		}
	}
	if deadPolls == 0 {
		t.Error("алерт queue_dead не опрашивает q:dead (протокол пуст)")
	}
	// The detail of the fired step names the stream and the length.
	if s := m.transition(env.Alerts, ctx, obs(map[string]int64{queue.StreamRender: 4242})); s.Status != AlertStatusFiring || !strings.Contains(s.Detail, "q:render=4242") {
		t.Errorf("сработавший detail = %q (%s), want q:render=4242 firing", s.Detail, s.Status)
	}
}

// TestAlertsQueuesNotConfigured — without Redis and without a threshold the queue alert
// is "not measured", it never fires falsely.
func TestAlertsQueuesNotConfigured(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 0 })
	if s := snapshotOf(t, env.Pass(context.Background()), AlertQueueOverflow); s.Status != AlertStatusNotMeasured {
		t.Errorf("watermark 0: статус = %s, want not_measured", s.Status)
	}

	env = newAlertsTestEnv(t, nil)
	env.deps.Redis = nil
	if s := snapshotOf(t, env.Pass(context.Background()), AlertQueueOverflow); s.Status != AlertStatusNotMeasured {
		t.Errorf("без Redis: статус = %s, want not_measured", s.Status)
	}

	// A threshold of 1 — the minimal valid watermark (a stream of length 1), not
	// "not set": the alert is measured.
	env = newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 1 })
	if s := snapshotOf(t, env.Pass(context.Background()), AlertQueueOverflow); s.Status != AlertStatusOK {
		t.Errorf("watermark 1: статус = %s (%s), want ok — порог валиден", s.Status, s.Detail)
	}
}

// TestAlertsQueuesSourceError — an unavailable Redis: a Warn, "not measured",
// the loop keeps living.
func TestAlertsQueuesSourceError(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.lener.errs = []error{errors.New("connection refused")}

	s := snapshotOf(t, env.Pass(context.Background()), AlertQueueOverflow)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("статус = %s, want not_measured", s.Status)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "connection refused") {
		t.Errorf("ошибка Redis без Warn:\n%s", env.buf.String())
	}
}

// --- untriaged q:dead (T-154) ---------------------------------------------------

// TestAlertsDeadEpisode — the queue_dead episode: firing on the third
// pass with a non-zero depth (not on the first — "persistently non-zero"),
// holding by any non-zero depth, clearing by an empty q:dead; one Warn
// per episode; the alert reads exactly q:dead.
func TestAlertsDeadEpisode(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newDeadMachine()

	steps := []struct {
		dead int64
		want string
	}{
		{0, AlertStatusOK},     // an empty quarantine
		{5, AlertStatusOK},     // the first death — observation 1/3
		{5, AlertStatusOK},     // 2/3
		{5, AlertStatusFiring}, // 3/3 — persistently non-zero
		{1, AlertStatusFiring}, // hold: any non-zero depth
		{0, AlertStatusOK},     // triage (XTRIM) — clearing
		{7, AlertStatusOK},     // a new death after clearing — a new episode, 1/3
	}
	for i, s := range steps {
		got := m.transition(env.Alerts, ctx, obs(s.dead))
		if got.Status != s.want {
			t.Errorf("шаг %d (q:dead=%d): статус = %s (%s), want %s", i, s.dead, got.Status, got.Detail, s.want)
		}
	}
	// The detail with the depth and the pass counter (a cross-check with the bury counter):
	// after the steps above the series is 1 observation; this pass is the second.
	got := m.transition(env.Alerts, ctx, obs(int64(12)))
	for _, want := range []string{"q:dead=12", "2/3", "crawler_queue_dead_buried_total"} {
		if !strings.Contains(got.Detail, want) {
			t.Errorf("detail %q не содержит %q", got.Detail, want)
		}
	}

	// One Warn per episode: the firing Warn "ALERT", a sustained
	// firing and the clearing after — without a repeated Warn (a full pass).
	env = newAlertsTestEnv(t, nil)
	for range 3 {
		env.lener.lens = map[string]int64{queue.StreamDead: 9}
		env.Pass(ctx)
	}
	if n := strings.Count(env.buf.String(), "АЛЕРТ: мертвые задачи в q:dead"); n != 1 {
		t.Errorf("Warn срабатывания = %d, want 1:\n%s", n, env.buf.String())
	}
	env.lener.lens = map[string]int64{queue.StreamDead: 9}
	if got := snapshotOf(t, env.Pass(ctx), AlertQueueDead); got.Status != AlertStatusFiring {
		t.Fatalf("устойчивое срабатывание: %s, want firing", got.Status)
	}
	before := env.buf.String()
	env.lener.lens = map[string]int64{}
	s := snapshotOf(t, env.Pass(ctx), AlertQueueDead)
	if s.Status != AlertStatusOK {
		t.Fatalf("пустой карантин: %s, want ok", s.Status)
	}
	if delta := env.buf.String()[len(before):]; strings.Contains(delta, "level=WARN") {
		t.Errorf("снятие дало Warn:\n%s", delta)
	} else if !strings.Contains(delta, "level=INFO") || !strings.Contains(delta, "алерт снят") {
		t.Errorf("снятие без Info:\n%s", delta)
	}
}

// TestAlertsDeadDegradation — without Redis and at a read error queue_dead
// is "not measured" (an error — with a Warn); the series of observations is not reset
// by an error: an unknown observation is not a zero one.
func TestAlertsDeadDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.deps.Redis = nil
	if s := snapshotOf(t, env.Pass(context.Background()), AlertQueueDead); s.Status != AlertStatusNotMeasured {
		t.Errorf("без Redis: статус = %s, want not_measured", s.Status)
	}

	ctx := context.Background()
	env = newAlertsTestEnv(t, nil)
	env.lener.lens = map[string]int64{queue.StreamDead: 4}
	// Errors — for all the pass reads: the queue alert reads 4 working
	// streams, then q:dead, q:render-fallback (T-246) and the q:render
	// of render debt — each read gets its own error.
	env.lener.errs = []error{
		errors.New("connection refused"), errors.New("connection refused"),
		errors.New("connection refused"), errors.New("connection refused"),
		errors.New("connection refused"),
	}
	s := snapshotOf(t, env.Pass(ctx), AlertQueueDead)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("ошибка чтения: статус = %s, want not_measured", s.Status)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "connection refused") {
		t.Errorf("ошибка чтения q:dead без Warn:\n%s", env.buf.String())
	}

	// An error between observations does not zero the series: two observations,
	// a failure in the middle, a third live one — firing (3 of 3 observations).
	env.lener.errs = nil
	if s := snapshotOf(t, env.Pass(ctx), AlertQueueDead); s.Status != AlertStatusOK {
		t.Fatalf("первое живое наблюдение: %s, want ok (1/3)", s.Status)
	}
	if s := snapshotOf(t, env.Pass(ctx), AlertQueueDead); s.Status != AlertStatusOK {
		t.Fatalf("второе живое наблюдение: %s, want ok (2/3)", s.Status)
	}
	env.lener.errs = []error{
		errors.New("connection refused"), errors.New("connection refused"),
		errors.New("connection refused"), errors.New("connection refused"),
		errors.New("connection refused"),
	}
	if s := snapshotOf(t, env.Pass(ctx), AlertQueueDead); s.Status != AlertStatusNotMeasured {
		t.Fatalf("сбой посередине серии: %s, want not_measured", s.Status)
	}
	env.lener.errs = nil
	if s := snapshotOf(t, env.Pass(ctx), AlertQueueDead); s.Status != AlertStatusFiring {
		t.Fatalf("третье наблюдение после сбоя: %s, want firing (серия сохранена)", s.Status)
	}
}

// --- Redis memory (T-154) --------------------------------------------------------

// TestAlertsRedisMemoryThreshold — the 80% threshold (exactly — firing),
// the hysteresis corridor 75–80% (holding the fired state), below 75% —
// clearing; after clearing the corridor does not fire the alert again.
func TestAlertsRedisMemoryThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRedisMemoryMachine()
	const max = int64(1000)
	steps := []struct {
		used int64
		want string
	}{
		{799, AlertStatusOK},     // 79.9% — below the threshold
		{800, AlertStatusFiring}, // exactly 80% — firing
		{760, AlertStatusFiring}, // 76% — the hysteresis corridor
		{750, AlertStatusFiring}, // exactly 75% — hold (the boundary)
		{749, AlertStatusOK},     // 74.9% — clearing
		{760, AlertStatusOK},     // the corridor without a fired alert — does not fire
	}
	for i, s := range steps {
		got := m.transition(env.Alerts, ctx, obs(RedisMemory{UsedBytes: s.used, MaxBytes: max, Policy: "noeviction"}))
		if got.Status != s.want {
			t.Errorf("шаг %d (used %d из %d): статус = %s (%s), want %s", i, s.used, max, got.Status, got.Detail, s.want)
		}
	}
	// The detail with absolutes, the share and the policy.
	s := m.transition(env.Alerts, ctx, obs(RedisMemory{UsedBytes: 800, MaxBytes: 1000, Policy: "noeviction"}))
	if s.Status != AlertStatusFiring ||
		!strings.Contains(s.Detail, "80.0%") || !strings.Contains(s.Detail, "noeviction") {
		t.Errorf("detail = %q (%s), want firing с долей 80.0%% и политикой", s.Detail, s.Status)
	}
}

// TestAlertsRedisMemoryDegradation — without a source and at maxmemory 0
// (no cap) the alert is "not measured"; a source error — "not
// measured" with a Warn, the loop keeps living.
func TestAlertsRedisMemoryDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.deps.RedisMemory = nil
	if s := snapshotOf(t, env.Pass(context.Background()), AlertRedisMemory); s.Status != AlertStatusNotMeasured {
		t.Errorf("без источника: статус = %s, want not_measured", s.Status)
	}

	env = newAlertsTestEnv(t, nil)
	env.redisMem.mem = RedisMemory{UsedBytes: 1 << 20, MaxBytes: 0, Policy: "noeviction"}
	s := snapshotOf(t, env.Pass(context.Background()), AlertRedisMemory)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "maxmemory=0") {
		t.Errorf("maxmemory 0: %s (%s), want not_measured «без потолка»", s.Status, s.Detail)
	}

	env = newAlertsTestEnv(t, nil)
	env.redisMem.errs = []error{errors.New("connection refused")}
	s = snapshotOf(t, env.Pass(context.Background()), AlertRedisMemory)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("ошибка INFO: статус = %s, want not_measured", s.Status)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "connection refused") {
		t.Errorf("ошибка INFO без Warn:\n%s", env.buf.String())
	}
}

// --- progress stall ----------------------------------------------------------

// TestAlertsStallThreshold — the freshness of zones.updated_at in recount
// periods: 60 s at a 5×60 s threshold — ok, 301 s — a stall; NULL — no
// zones, not measured.
func TestAlertsStallThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil) // ZoneStatsInterval 60s → a 300s threshold
	now, _ := alertsFixedClock()
	ctx := context.Background()
	m := env.newStallMachine()
	at := func(age time.Duration) observation[time.Time] { return obs(now.Add(-age)) }

	if s := m.transition(env.Alerts, ctx, at(60*time.Second)); s.Status != AlertStatusOK {
		t.Errorf("60с: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	// Between 4 and 5 periods (250 s at a 5×60 s threshold) — ok: it pins the
	// threshold factor, not "one of the neighboring ones".
	if s := m.transition(env.Alerts, ctx, at(250*time.Second)); s.Status != AlertStatusOK {
		t.Errorf("250с: статус = %s (%s), want ok (порог 5, не 4 периода)", s.Status, s.Detail)
	}

	// Exactly the threshold (300 s = 5 × 60 s) — not a stall yet: strictly greater.
	if s := m.transition(env.Alerts, ctx, at(5*time.Minute)); s.Status != AlertStatusOK {
		t.Errorf("ровно 300с: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	s := m.transition(env.Alerts, ctx, at(301*time.Second))
	if s.Status != AlertStatusFiring {
		t.Fatalf("301с: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"5m1s", "5", "1m0s"} { // age, factor, period
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail %q не содержит %s", s.Detail, want)
		}
	}

	// No zones (NULL) — not measured: an observation with a reason.
	if s := m.transition(env.Alerts, ctx,
		obsReason[time.Time]("справочник зон пуст — прогресс не отслеживается")); s.Status != AlertStatusNotMeasured {
		t.Errorf("без зон: статус = %s, want not_measured", s.Status)
	}
}

// TestAlertsStallSourceErrors — the freshness source degradations: a query
// error, an empty answer, an unreadable row, an iteration error — a Warn and
// "not measured" with a reason, the alert loop keeps living.
func TestAlertsStallSourceErrors(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.freshErrs = []error{errors.New("бд недоступна")}
	s := snapshotOf(t, env.Pass(context.Background()), AlertProgressStall)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "бд недоступна") {
		t.Errorf("ошибка запроса: статус = %s (%s), want not_measured с ошибкой", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("ошибка запроса без Warn:\n%s", env.buf.String())
	}

	env = newAlertsTestEnv(t, nil)
	env.pool.freshEmpty = true
	s = snapshotOf(t, env.Pass(context.Background()), AlertProgressStall)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "пустой ответ") {
		t.Errorf("пустой ответ: статус = %s (%s), want not_measured «пустой ответ»", s.Status, s.Detail)
	}

	env = newAlertsTestEnv(t, nil)
	env.pool.freshBadRow = true
	s = snapshotOf(t, env.Pass(context.Background()), AlertProgressStall)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "чтение") {
		t.Errorf("ошибка Scan: статус = %s (%s), want not_measured «чтение»", s.Status, s.Detail)
	}

	env = newAlertsTestEnv(t, nil)
	env.pool.freshRowsErr = errors.New("соединение сброшено")
	s = snapshotOf(t, env.Pass(context.Background()), AlertProgressStall)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "соединение сброшено") {
		t.Errorf("rows.Err: статус = %s (%s), want not_measured с ошибкой итерации", s.Status, s.Detail)
	}
}

// TestAlertsStallDefaultInterval — a zero ZoneStatsInterval — the default
// (DefaultZoneStatsInterval, 30 minutes, T-199): a threshold of 2.5h, not zero
// (2h is behind the threshold — still ok; at a 5 min default it would converge to firing).
func TestAlertsStallDefaultInterval(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.ZoneStatsInterval = 0 })
	now, _ := alertsFixedClock()
	env.pool.fresh = []sql.NullTime{{Time: now.Add(-2 * time.Hour), Valid: true}}

	if s := snapshotOf(t, env.Pass(context.Background()), AlertProgressStall); s.Status != AlertStatusOK {
		t.Errorf("2ч при дефолтных 5×30м: статус = %s (%s), want ok", s.Status, s.Detail)
	}
}

// TestAlertsDivergedIntervals — T-206: the alert loop interval (Interval,
// monitor.alerts_interval) is decoupled from the reconciliation period
// (ZoneStatsInterval, monitor.zone_stats_interval — the defaults are 5 min and
// 30 min). The progress stall threshold is counted from reconciliation
// (5 × 30 min = 2.5h: zones.updated_at is written by ZoneStatsLoop — a threshold
// shorter than its period would fire forever, including 5 × alerts_interval =
// 25 min); the bad_hashes window is counted from the alert interval (the detail names
// 15 × 5 min = 1h15m, not 15 × 30 min = 7.5h).
func TestAlertsDivergedIntervals(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.Interval = 5 * time.Minute
		o.ZoneStatsInterval = 30 * time.Minute
	})
	now, _ := alertsFixedClock()
	ctx := context.Background()

	// A stamp age of 2h — ok at a 5 × 30 min threshold (counting from
	// the 5 min alerts_interval would give a 25 min threshold and a false firing).
	env.pool.fresh = []sql.NullTime{{Time: now.Add(-2 * time.Hour), Valid: true}}
	s := snapshotOf(t, env.Pass(ctx), AlertProgressStall)
	if s.Status != AlertStatusOK {
		t.Errorf("2ч при реконсиляции 30м: статус = %s (%s), want ok", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "2h30m0s") { // a threshold of 5 × 30m
		t.Errorf("detail %q не содержит порог 2h30m0s (5 × zone_stats_interval)", s.Detail)
	}

	// 2h 31 min — behind the 2.5h threshold.
	env.pool.fresh = []sql.NullTime{{Time: now.Add(-151 * time.Minute), Valid: true}}
	if s := snapshotOf(t, env.Pass(ctx), AlertProgressStall); s.Status != AlertStatusFiring {
		t.Errorf("151м при реконсиляции 30м: статус = %s (%s), want firing", s.Status, s.Detail)
	}

	// The bad_hashes window — from the alert interval: 15 × 5 min = 1h15m.
	src := &fakeBadHashes{}
	env.deps.BadHashes = src
	src.set(0)
	snapshotOf(t, env.Pass(ctx), AlertBadHashes) // the base
	src.set(10)                                  // a single delta of exactly 10
	s = snapshotOf(t, env.Pass(ctx), AlertBadHashes)
	if s.Status != AlertStatusFiring {
		t.Fatalf("сумма 10: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "1h15m0s") {
		t.Errorf("detail %q не содержит окно 1h15m0s (15 × alerts_interval 5м)", s.Detail)
	}
	if strings.Contains(s.Detail, "7h") {
		t.Errorf("detail %q содержит окно от 30-мин интервала (7h30m) — окно не отвязано", s.Detail)
	}
}

// --- disk space ---------------------------------------------------------------

// TestAlertsDiskThreshold — the same formula as the Put pause (strictly below
// the threshold): 4% at a 5% threshold — an alert; exactly 5% — ok.
func TestAlertsDiskThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newDiskMachine()

	s := m.transition(env.Alerts, ctx, obs(SpaceStat{Avail: 4 << 30, Total: 100 << 30})) // 4.0 %
	if s.Status != AlertStatusFiring {
		t.Fatalf("4%%: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "4.0%") || !strings.Contains(s.Detail, "5.0%") {
		t.Errorf("detail %q без доли/порога", s.Detail)
	}

	// Exactly the threshold — not less.
	if s := m.transition(env.Alerts, ctx, obs(SpaceStat{Avail: 5 << 30, Total: 100 << 30})); s.Status != AlertStatusOK {
		t.Errorf("ровно порог: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	// A threshold of 1% — the minimal valid check, not "off".
	env = newAlertsTestEnv(t, func(o *AlertsOptions) { o.DiskFreePercent = 1 })
	m = env.newDiskMachine()
	if s := m.transition(env.Alerts, ctx, obs(SpaceStat{Avail: 100 << 30, Total: 200 << 30})); s.Status != AlertStatusOK {
		t.Errorf("порог 1%%: статус = %s (%s), want ok — контроль включён", s.Status, s.Detail)
	}

	// The factor 100: 100 free of 10100 bytes — 0.99…% < 1 — an alert;
	// 101·Avail/Total would give exactly 1.0% and clear the alert.
	if s := m.transition(env.Alerts, ctx, obs(SpaceStat{Avail: 100, Total: 10100})); s.Status != AlertStatusFiring {
		t.Errorf("100 из 10100 байт: статус = %s (%s), want firing (0.99%% < 1%%)", s.Status, s.Detail)
	}
}

// TestAlertsDiskDegradation — the check off, the directory not set, a FS without
// capacity, a statfs error (a Warn) — "not measured", not a false firing.
func TestAlertsDiskDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.DiskFreePercent = 0 })
	if s := snapshotOf(t, env.Pass(context.Background()), AlertDiskFree); s.Status != AlertStatusNotMeasured {
		t.Errorf("контроль выключен: статус = %s, want not_measured", s.Status)
	}

	env = newAlertsTestEnv(t, func(o *AlertsOptions) { o.ContentDir = "" })
	if s := snapshotOf(t, env.Pass(context.Background()), AlertDiskFree); s.Status != AlertStatusNotMeasured {
		t.Errorf("без каталога: статус = %s, want not_measured", s.Status)
	}

	env = newAlertsTestEnv(t, nil)
	env.space.stat = SpaceStat{Avail: 1 << 30, Total: 0}
	if s := snapshotOf(t, env.Pass(context.Background()), AlertDiskFree); s.Status != AlertStatusNotMeasured {
		t.Errorf("total=0: статус = %s, want not_measured", s.Status)
	}

	env = newAlertsTestEnv(t, nil)
	env.space.err = errors.New("no such file or directory")
	s := snapshotOf(t, env.Pass(context.Background()), AlertDiskFree)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("ошибка statfs: статус = %s, want not_measured", s.Status)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("ошибка statfs без Warn:\n%s", env.buf.String())
	}
}

// --- NVMe wear ----------------------------------------------------------------

// TestAlertsNVMeWear — the alert at 90% of the lifespan; several devices — by
// the worst; the detail with TBW.
func TestAlertsNVMeWear(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newWearMachine()

	s := m.transition(env.Alerts, ctx, obs([]NVMeWear{{Device: "nvme0", PercentUsed: 90, DataUnitsWritten: 200_000_000}}))
	if s.Status != AlertStatusFiring {
		t.Fatalf("90%%: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "nvme0=90%") || !strings.Contains(s.Detail, "ТБ") {
		t.Errorf("detail %q без устройства/TBW", s.Detail)
	}

	s = m.transition(env.Alerts, ctx, obs([]NVMeWear{
		{Device: "nvme0", PercentUsed: 41},
		{Device: "nvme1", PercentUsed: 89}, // the worst, but below the threshold
	}))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "nvme1") || !strings.Contains(s.Detail, "89%") {
		t.Errorf("89%%: статус = %s (%s), want ok по худшему nvme1", s.Status, s.Detail)
	}

	// A new drive (0%): the starting value is the first device;
	// the detail names the name, not an empty string.
	s = m.transition(env.Alerts, ctx, obs([]NVMeWear{{Device: "nvme0", PercentUsed: 0}}))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "nvme0") || !strings.Contains(s.Detail, "0%") {
		t.Errorf("0%%: статус = %s (%s), want ok с именем nvme0", s.Status, s.Detail)
	}

	// An empty list (the source is alive, no devices) — "not measured",
	// not a panic on the first element.
	s = m.transition(env.Alerts, ctx, obsReason[[]NVMeWear]("NVMe-устройства не найдены"))
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "не найдены") {
		t.Errorf("без устройств: статус = %s (%s), want not_measured", s.Status, s.Detail)
	}

	// Equal wear of two devices: the worst is the first (the replacement is strictly on
	// "greater", not "not less").
	s = m.transition(env.Alerts, ctx, obs([]NVMeWear{
		{Device: "nvme0", PercentUsed: 50},
		{Device: "nvme1", PercentUsed: 50},
	}))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "nvme0: использовано 50%") {
		t.Errorf("равный износ: %s (%s), want ok по первому nvme0", s.Status, s.Detail)
	}

	// An empty list via a full pass (observeWear: the source is alive,
	// no devices) — the same "not measured" without a panic.
	env.wear.wears = nil
	if s := snapshotOf(t, env.Pass(ctx), AlertNVMeWear); s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "не найдены") {
		t.Errorf("без устройств (полный проход): статус = %s (%s), want not_measured «не найдены»", s.Status, s.Detail)
	}
}

// TestAlertsNVMeDegradationQuiet — SMART unavailable (a container): "not
// measured" without a Warn — an expected state, not an incident; the transition — Debug.
func TestAlertsNVMeDegradationQuiet(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.wear.err = errors.New("SMART недоступен: nvme0: permission denied")

	s := snapshotOf(t, env.Pass(context.Background()), AlertNVMeWear)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("статус = %s, want not_measured", s.Status)
	}
	if strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("деградация SMART дала Warn:\n%s", env.buf.String())
	}
	if !strings.Contains(env.buf.String(), "не измеряется") { // the Debug transition
		t.Errorf("переход в «не измеряется» не залогирован:\n%s", env.buf.String())
	}
}

// --- transition logging, the snapshot, the defaults -----------------------------

// TestAlertsTransitionsLogging — the front entries: firing — a Warn
// "ALERT", clearing — Info, a source error — a Warn.
func TestAlertsTransitionsLogging(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()

	// Four passes: firing, a sustained firing (silent),
	// clearing, a sustained "ok" (silent).
	env.pool.daily = []alertDNSDaily{{temp: 500, total: 1000}, {temp: 500, total: 1000}, {temp: 0, total: 1000}, {temp: 0, total: 1000}}
	env.Pass(ctx) // firing
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "АЛЕРТ: рост доли ошибок DNS") {
		t.Errorf("срабатывание без Warn-записи:\n%s", env.buf.String())
	}

	// A sustained firing — silent: the signal is front-loaded; a repeated Warn
	// in the same state is not written.
	beforeHold := env.buf.String()
	env.Pass(ctx)
	if delta := env.buf.String()[len(beforeHold):]; strings.Contains(delta, "level=WARN") || strings.Contains(delta, "АЛЕРТ") {
		t.Errorf("устойчивое срабатывание повторило Warn:\n%s", delta)
	}

	env.Pass(ctx) // clearing
	if !strings.Contains(env.buf.String(), "level=INFO") || !strings.Contains(env.buf.String(), "алерт снят") {
		t.Errorf("снятие без Info-записи:\n%s", env.buf.String())
	}

	// A sustained state — silent: a repeated pass without a status change does not
	// write a Warn/Info again (the signal is front-loaded); the Debug result of the pass —
	// not a signal, it is allowed.
	before := env.buf.String()
	env.Pass(ctx)
	if delta := env.buf.String()[len(before):]; strings.Contains(delta, "level=WARN") || strings.Contains(delta, "level=INFO") {
		t.Errorf("повторный тихий проход добавил Warn/Info:\n%s", delta)
	}

	// A fired alert going to "not measured" (a source failure):
	// the loss of an active incident is recorded with Info, not a silent Debug.
	// Before the failure the alert fires again — the transition is exactly from firing.
	env.pool.daily = []alertDNSDaily{{temp: 500, total: 1000}}
	env.Pass(ctx)
	if !strings.Contains(env.buf.String(), "АЛЕРТ: рост доли ошибок DNS") {
		t.Fatalf("повторное срабатывание не залогировано:\n%s", env.buf.String())
	}
	before2 := env.buf.String()
	env.pool.dailyErrs = []error{errors.New("бд недоступна")}
	env.Pass(ctx)
	delta2 := env.buf.String()[len(before2):]
	if !strings.Contains(delta2, "сработавший алерт больше не измеряется") || !strings.Contains(delta2, "level=INFO") {
		t.Errorf("firing→not_measured без Info:\n%s", delta2)
	}
}

// TestAlertsOnPassSnapshot — the hook receives the snapshot of all alerts
// in a fixed order; Firing() selects the fired ones.
func TestAlertsOnPassSnapshot(t *testing.T) {
	var mu sync.Mutex
	var passes []AlertsResult
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.OnPass = func(r AlertsResult) {
			mu.Lock()
			defer mu.Unlock()
			passes = append(passes, r)
		}
	})
	env.pool.daily = []alertDNSDaily{{temp: 500, total: 1000}}

	res := env.Pass(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if len(passes) != 1 {
		t.Fatalf("OnPass вызовов = %d, want 1", len(passes))
	}
	wantNames := []string{AlertDNSErrors, AlertQueueOverflow, AlertProgressStall, AlertDiskFree,
		AlertDiskMin, AlertDiskCrit, AlertNVMeWear, AlertBadHashes, AlertUnitOOM, AlertMemHigh,
		AlertQueueDead, AlertQueueFreeze, AlertRedisMemory, AlertPSICPU, AlertRenderDebt,
		AlertRenderFbDepth, AlertRenderFbBudget, AlertRenderBrowserErr, AlertDomainsStale,
		AlertRedisRestart}
	if len(res.Alerts) != len(wantNames) {
		t.Fatalf("алертов в снапшоте = %d, want %d", len(res.Alerts), len(wantNames))
	}
	for i, name := range wantNames {
		if res.Alerts[i].Name != name {
			t.Errorf("снапшот[%d] = %s, want %s", i, res.Alerts[i].Name, name)
		}
	}
	if firing := res.Firing(); len(firing) != 1 || firing[0].Name != AlertDNSErrors {
		t.Errorf("Firing() = %+v, want один dns_temp_error_share", firing)
	}
}

// TestAlertsOptionsDefaults — zero and negative periods — the defaults;
// unit values (1ns) — valid values, not overwritten (the "≤ 0" boundary);
// NewAlerts substitutes the OS implementations for empty Space/Wear and the default
// clock.
func TestAlertsOptionsDefaults(t *testing.T) {
	o := (AlertsOptions{}).withDefaults()
	// The literal 5m, not DefaultAlertsInterval: a comparison with the
	// constant mutates together with it (the threshold drains must be caught here).
	if o.Interval != 5*time.Minute {
		t.Errorf("Interval = %v, want 5m", o.Interval)
	}
	if o.ZoneStatsInterval != DefaultZoneStatsInterval {
		t.Errorf("ZoneStatsInterval = %v, want %v", o.ZoneStatsInterval, DefaultZoneStatsInterval)
	}

	neg := AlertsOptions{Interval: -time.Second, ZoneStatsInterval: -time.Second}.withDefaults()
	if neg.Interval != 5*time.Minute || neg.ZoneStatsInterval != DefaultZoneStatsInterval {
		t.Errorf("отрицательные периоды: %+v, want дефолты", neg)
	}

	one := AlertsOptions{Interval: 1, ZoneStatsInterval: 1}.withDefaults()
	if one.Interval != 1 || one.ZoneStatsInterval != 1 {
		t.Errorf("единичные периоды затёрты: %+v, want как заданы", one)
	}

	got := AlertsOptions{Interval: 30 * time.Second, ZoneStatsInterval: 45 * time.Second}.withDefaults()
	if got.Interval != 30*time.Second || got.ZoneStatsInterval != 45*time.Second {
		t.Errorf("withDefaults затирает заданные: %+v", got)
	}

	a := NewAlerts(AlertsDeps{Pool: &alertsPool{}}, AlertsOptions{})
	if _, ok := a.deps.Space.(OSStatfs); !ok {
		t.Errorf("Space = %T, want OSStatfs", a.deps.Space)
	}
	if _, ok := a.deps.Wear.(OSWear); !ok {
		t.Errorf("Wear = %T, want OSWear", a.deps.Wear)
	}
	if a.now == nil || a.log == nil {
		t.Error("NewAlerts оставил nil часов/логгера")
	}
}

// --- OS sources -----------------------------------------------------------------

// TestOSStatfsSpace — the real statfs of a temporary directory: the capacity is visible
// in bytes (multiplying the blocks by Bsize — a division would give fractions of a megabyte
// even on the multi-gigabyte CI volumes); a non-existent directory — an error
// (no panic).
func TestOSStatfsSpace(t *testing.T) {
	sp, err := OSStatfs{}.Space(t.TempDir())
	if err != nil {
		t.Fatalf("Space: %v", err)
	}
	if sp.Total < 1<<20 || sp.Avail < 1<<20 {
		t.Errorf("статистика без умножения на Bsize (блоки вместо байт): %+v", sp)
	}
	if _, err := (OSStatfs{}).Space(filepath.Join(t.TempDir(), "нет-такого")); err == nil {
		t.Error("несуществующий каталог: ошибки нет")
	}
}

// TestParseHealthLog — the health page offsets (the uapi/linux/nvme.h layout,
// struct nvme_smart_log): percentage_used — byte 5, data units written —
// bytes 48–63 LE; a short buffer — an error.
func TestParseHealthLog(t *testing.T) {
	page := make([]byte, nvmeHealthLogSize)
	page[5] = 42                                            // percentage_used
	binary.LittleEndian.PutUint64(page[48:56], 195_312_500) // ≈ 100 TB

	percent, units, err := parseHealthLog(page)
	if err != nil {
		t.Fatalf("parseHealthLog: %v", err)
	}
	if percent != 42 {
		t.Errorf("percentage_used = %d, want 42 (байт 5)", percent)
	}
	if units != 195_312_500 {
		t.Errorf("data units written = %d, want 195312500 (байты 48–63 LE)", units)
	}
	// Exactly 100.0 TB: the product and the division are exact in float64 (195312500 ×
	// 512000 = 1e14 < 2^53; the quotient 100 is exactly representable) — mutations
	// of the coefficients (±1 per data unit, ±1 per trillion) are caught
	// by the exact equality.
	if tbw := nvmeTBW(NVMeWear{DataUnitsWritten: units}); tbw != 100 {
		t.Errorf("TBW = %v, want ровно 100 ТБ", tbw)
	}

	// A new drive: zero wear decodes as is.
	zero := make([]byte, nvmeHealthLogSize)
	if percent, _, _ := parseHealthLog(zero); percent != 0 {
		t.Errorf("нулевой износ = %d, want 0", percent)
	}

	// Values >100% are valid (the lifespan is exhausted) and are not truncated.
	over := make([]byte, nvmeHealthLogSize)
	over[5] = 253
	if percent, _, _ := parseHealthLog(over); percent != 253 {
		t.Errorf("износ >100%% = %d, want 253", percent)
	}

	// The page size is fixed by the protocol — 512 bytes: literals (not the
	// constant — it mutates together with the check): 512 parses,
	// 511 is already a short page.
	full := make([]byte, 512)
	full[5] = 7
	binary.LittleEndian.PutUint64(full[48:56], 9)
	if percent, u, err := parseHealthLog(full); err != nil || percent != 7 || u != 9 {
		t.Errorf("страница 512 байт: (%d, %d, %v), want (7, 9, nil)", percent, u, err)
	}
	if _, _, err := parseHealthLog(make([]byte, 511)); err == nil {
		t.Error("страница 511 байт: ошибки нет, want короткая страница")
	}

	// A short buffer — an error and zero return values.
	pct, u, err := parseHealthLog(page[:nvmeHealthLogSize-1])
	if err == nil {
		t.Error("короткий буфер: ошибки нет")
	}
	if pct != 0 || u != 0 {
		t.Errorf("короткий буфер: (%d, %d), want нули при ошибке", pct, u)
	}
}

// TestOSWearDegradation — an empty sysfs and an inaccessible /dev node — the
// degradation errors with a reason (a container), not a panic and not an empty result.
func TestOSWearDegradation(t *testing.T) {
	root := t.TempDir()

	// sysfs without class/nvme — no NVMe at all.
	_, err := OSWear{SysFS: root, DevFS: root}.Wear(context.Background())
	if err == nil || !strings.Contains(err.Error(), "не обнаружены") {
		t.Errorf("пустой sysfs: ошибка = %v, want «не обнаружены»", err)
	}

	// sysfs sees the controller (as in a container: the host sysfs), but the /dev node
	// is not passed through — SMART is unavailable, the reason names the device.
	if err := os.MkdirAll(filepath.Join(root, "class", "nvme", "nvme0"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = OSWear{SysFS: root, DevFS: filepath.Join(root, "dev")}.Wear(context.Background())
	if err == nil || !strings.Contains(err.Error(), "SMART недоступен") || !strings.Contains(err.Error(), "nvme0") {
		t.Errorf("недоступный /dev: ошибка = %v, want деградация с nvme0", err)
	}

	// Several controllers: all are walked (one error does not stop the
	// loop) — the reason names every device.
	if err := os.MkdirAll(filepath.Join(root, "class", "nvme", "nvme1"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = OSWear{SysFS: root, DevFS: filepath.Join(root, "dev")}.Wear(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nvme0") || !strings.Contains(err.Error(), "nvme1") {
		t.Errorf("два недоступных контроллера: ошибка = %v, want причина с nvme0 и nvme1", err)
	}

	// A ctx cancellation — a context error before touching the devices.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (OSWear{SysFS: root}).Wear(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("отмена ctx: ошибка = %v, want context.Canceled", err)
	}
}

// --- the absolute disk minimum (T-162) -------------------------------------------

// TestAlertsDiskMinThreshold — the Warn (15 GiB) and escalation (10 GiB) thresholds
// at exact boundaries: firing is strict (<), recovery by returning
// to the threshold; the escalation is a separate alert with its own pair of states.
func TestAlertsDiskMinThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	minM, critM := env.newDiskMinMachine(), env.newDiskCritMachine()
	steps := []struct {
		avail     uint64
		min, crit string
	}{
		{16 << 30, AlertStatusOK, AlertStatusOK},           // headroom
		{15 << 30, AlertStatusOK, AlertStatusOK},           // exactly the minimum — not fired (strict <)
		{15<<30 - 1, AlertStatusFiring, AlertStatusOK},     // a byte below — Warn
		{10 << 30, AlertStatusFiring, AlertStatusOK},       // exactly the escalation — still Warn
		{10<<30 - 1, AlertStatusFiring, AlertStatusFiring}, // a byte below — escalation
		{11 << 30, AlertStatusFiring, AlertStatusOK},       // only the escalation recovered
		{15 << 30, AlertStatusOK, AlertStatusOK},           // the Warn minimum recovered
	}
	for i, s := range steps {
		sp := obs(SpaceStat{Avail: s.avail, Total: 79 << 30})
		mn := minM.transition(env.Alerts, ctx, sp)
		cr := critM.transition(env.Alerts, ctx, sp)
		if mn.Status != s.min {
			t.Errorf("шаг %d (avail %d): disk_free_min = %s (%s), want %s",
				i, s.avail, mn.Status, mn.Detail, s.min)
		}
		if cr.Status != s.crit {
			t.Errorf("шаг %d (avail %d): disk_free_crit = %s (%s), want %s",
				i, s.avail, cr.Status, cr.Detail, s.crit)
		}
	}
	// The detail names the threshold in readable units.
	s := critM.transition(env.Alerts, ctx, obs(SpaceStat{Avail: 9 << 30, Total: 79 << 30}))
	for _, want := range []string{"9.0GiB", "79.0GiB", "10.0GiB"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
}

// TestAlertsDiskMinCustomAndDegradation — own thresholds from options
// (config), degradations: the directory not set and a statfs error — "not
// measured" (an error with a Warn); total=0 — a quiet "not measured".
func TestAlertsDiskMinCustomAndDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.DiskFreeMinBytes = 2 << 30
		o.DiskFreeCritBytes = 1 << 30
	})
	env.space.stat = SpaceStat{Avail: 1<<30 + 1, Total: 10 << 30}
	s := snapshotOf(t, env.Pass(context.Background()), AlertDiskMin)
	if s.Status != AlertStatusFiring {
		t.Fatalf("свой минимум 2GiB: %s (%s), want firing при 1GiB+1", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "2.0GiB") {
		t.Errorf("detail = %q, want порог 2.0GiB из опций", s.Detail)
	}
	c := snapshotOf(t, env.Pass(context.Background()), AlertDiskCrit)
	if c.Status != AlertStatusOK || !strings.Contains(c.Detail, "1.0GiB") {
		t.Errorf("эскалация 1GiB: %s (%s), want ok с порогом 1.0GiB", c.Status, c.Detail)
	}

	// A statfs error — a Warn and "not measured", the loop keeps living.
	env.space.err = errors.New("отказано в доступе")
	s = snapshotOf(t, env.Pass(context.Background()), AlertDiskMin)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "отказано") {
		t.Errorf("ошибка statfs: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("ошибка statfs без Warn:\n%s", env.buf.String())
	}

	env2 := newAlertsTestEnv(t, func(o *AlertsOptions) { o.ContentDir = "" })
	if s := snapshotOf(t, env2.Pass(context.Background()), AlertDiskMin); s.Status != AlertStatusNotMeasured {
		t.Errorf("без каталога контента: %s, want not_measured", s.Status)
	}
	env3 := newAlertsTestEnv(t, nil)
	env3.space.stat = SpaceStat{Avail: 1 << 30, Total: 0}
	if s := snapshotOf(t, env3.Pass(context.Background()), AlertDiskMin); s.Status != AlertStatusNotMeasured {
		t.Errorf("total=0: %s, want not_measured", s.Status)
	}

	// A volume smaller than the minimum — the threshold is inapplicable (a 10 GB dev volume against 15 GiB):
	// an eternal firing on a small volume is false; the percentage one covers it.
	env5 := newAlertsTestEnv(t, nil)
	env5.space.stat = SpaceStat{Avail: 9 << 30, Total: 10 << 30}
	s = snapshotOf(t, env5.Pass(context.Background()), AlertDiskMin)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "неприменим") {
		t.Errorf("малый том: %s (%s), want not_measured «неприменим»", s.Status, s.Detail)
	}
}

// --- MemoryHigh throttling (T-162) ------------------------------------------------

// TestAlertsMemHighStreak — firing on the growth of the high counter for N passes
// in a row, clearing by the first pass without growth; a unit restart (the counter to zero)
// zeroes the series without a false spike.
func TestAlertsMemHighStreak(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.MemHighMinPasses = 2 })
	ctx := context.Background()
	m := env.newMemHighMachine()
	set := func(high uint64) observation[[]UnitEvents] {
		return obs([]UnitEvents{{Unit: "crawler-http-worker.service", High: high}})
	}

	// The first pass — the base (without it the delta cannot be computed).
	if s := m.transition(env.Alerts, ctx, set(120000)); s.Status != AlertStatusOK {
		t.Fatalf("база: %s (%s), want ok", s.Status, s.Detail)
	}
	// Growing for one pass — the series is not enough yet.
	s := m.transition(env.Alerts, ctx, set(120500))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "рост") {
		t.Fatalf("один проход роста: %s (%s), want ok", s.Status, s.Detail)
	}
	// The second growth pass — firing; the detail names the unit and the gain.
	s = m.transition(env.Alerts, ctx, set(121300))
	if s.Status != AlertStatusFiring {
		t.Fatalf("второй проход роста: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"crawler-http-worker.service", "800"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// Growth stopped — clearing by the same pass.
	if s := m.transition(env.Alerts, ctx, set(121300)); s.Status != AlertStatusOK {
		t.Errorf("без роста: %s (%s), want ok (снятие)", s.Status, s.Detail)
	}
	// A unit restart: the counter at zero — a decrease is not a spike; the series starts from scratch.
	_ = m.transition(env.Alerts, ctx, set(121300))
	if s := m.transition(env.Alerts, ctx, set(5)); s.Status != AlertStatusOK {
		t.Errorf("рестарт юнита: %s (%s), want ok без ложного всплеска", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, set(50)); s.Status != AlertStatusOK {
		t.Errorf("первый проход роста после рестарта: %s (%s), want ok (серия 1 < 2)", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, set(55)); s.Status != AlertStatusFiring {
		t.Errorf("рост после рестарта двумя проходами: %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsMemHighDegradation — the source not set and errNoSystemd — a quiet
// "not measured"; any other error — a Warn. The unit tracking memory survives
// a pass with an error (the evaluation continues on the actual counters).
func TestAlertsMemHighDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	if s := snapshotOf(t, env.Pass(context.Background()), AlertMemHigh); s.Status != AlertStatusOK {
		t.Fatalf("здоровые юниты: %s (%s), want ok", s.Status, s.Detail)
	}

	// An empty unit list — a VALID memory_high measurement (the asymmetry
	// with unit_oom is deliberate and old: the master evalMemHigh had no "not
	// found" branch): ok with the "0 units" detail; not_measured would be a false
	// degradation (the T-264 review, P1); the front log is not written in this case.
	env.units.units = nil
	s := snapshotOf(t, env.Pass(context.Background()), AlertMemHigh)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "0 юнитов") {
		t.Fatalf("пустой список: %s (%s), want ok «0 юнитов» (не not_measured)", s.Status, s.Detail)
	}
	// The neighboring unit_oom treats an empty list as "not found" — the
	// machines diverge deliberately.
	if u := snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM); u.Status != AlertStatusNotMeasured {
		t.Fatalf("unit_oom на пустом списке: %s, want not_measured (асимметрия машин)", u.Status)
	}

	env2 := newAlertsTestEnv(t, nil)
	// The error queue: unit_oom and memory_high read the source independently
	// (two calls per pass) — each gets its own error.
	noSystemd := fmt.Errorf("monitor: юниты: %w: systemctl show: упал", errNoSystemd)
	env2.units.errs = []error{noSystemd, noSystemd}
	s = snapshotOf(t, env2.Pass(context.Background()), AlertMemHigh)
	if s.Status != AlertStatusNotMeasured || strings.Contains(env2.buf.String(), "level=WARN") {
		t.Errorf("без systemd: %s, want тихое not_measured (лог:\n%s)", s.Status, env2.buf.String())
	}

	env3 := newAlertsTestEnv(t, nil)
	hardErr := errors.New("monitor: юниты: cgroup v2 недоступен")
	env3.units.errs = []error{hardErr, hardErr}
	s = snapshotOf(t, env3.Pass(context.Background()), AlertMemHigh)
	if s.Status != AlertStatusNotMeasured {
		t.Errorf("боевая ошибка: %s, want not_measured", s.Status)
	}
	if !strings.Contains(env3.buf.String(), "level=WARN") {
		t.Errorf("боевая ошибка без Warn:\n%s", env3.buf.String())
	}

	env4 := newAlertsTestEnv(t, nil)
	n := NewAlerts(AlertsDeps{Pool: env4.pool}, env4.opts)
	res := n.Pass(context.Background())
	if s := snapshotOf(t, res, AlertMemHigh); s.Status != AlertStatusNotMeasured {
		t.Errorf("источник не задан: %s, want not_measured", s.Status)
	}
}

// --- frozen queue (T-162) ---------------------------------------------------------

// TestAlertsQueueFreeze — the immobility window of the last-delivered-id at
// depth > 0: an ID movement and an emptied queue do not accumulate time; the first
// entry after an idle bases at the current moment (no instant
// false firing on the accumulated idle).
func TestAlertsQueueFreeze(t *testing.T) {
	now := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.QueueFreeze = 15 * time.Minute
		o.Now = func() time.Time { return now }
	})
	ctx := context.Background()
	m := env.newQueueFreezeMachine()
	g := func(id string, depth int64) observation[[]StreamGroupState] {
		return obs([]StreamGroupState{
			{Stream: queue.StreamRender, Group: "render-worker", Depth: depth, LastDeliveredID: id},
		})
	}

	// The base: a queue with work, a fresh ID.
	if s := m.transition(env.Alerts, ctx, g("1761280800000-0", 40)); s.Status != AlertStatusOK {
		t.Fatalf("база: %s (%s), want ok", s.Status, s.Detail)
	}
	// An hour without movement at a depth — frozen.
	now = now.Add(time.Hour)
	s := m.transition(env.Alerts, ctx, g("1761280800000-0", 40))
	if s.Status != AlertStatusFiring {
		t.Fatalf("час без доставок: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"q:render/render-worker", "глубине 40", "1761280800000-0"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// The queue emptied — clearing; the accumulated time is reset.
	now = now.Add(time.Minute)
	if s := m.transition(env.Alerts, ctx, g("1761280800000-0", 0)); s.Status != AlertStatusOK {
		t.Errorf("пустая очередь: %s (%s), want ok", s.Status, s.Detail)
	}
	// Depth again with the same ID: the idle from the previous round does not count —
	// the window ticks anew; 14 minutes since the depth returned is not enough.
	now = now.Add(14 * time.Minute)
	if s := m.transition(env.Alerts, ctx, g("1761280800000-0", 7)); s.Status != AlertStatusOK {
		t.Errorf("14 мин новой глубины: %s (%s), want ok (окно сброшено пустой очередью)", s.Status, s.Detail)
	}
	now = now.Add(15 * time.Minute)
	if s := m.transition(env.Alerts, ctx, g("1761280800000-0", 7)); s.Status != AlertStatusFiring {
		t.Errorf("15 мин без движения: %s (%s), want firing", s.Status, s.Detail)
	}
	// A delivery went through — clearing; the moment updated.
	if s := m.transition(env.Alerts, ctx, g("1761284400000-0", 7)); s.Status != AlertStatusOK {
		t.Errorf("движение ID: %s (%s), want ok", s.Status, s.Detail)
	}
}

// TestAlertsQueueFreezeDegradation — the source not set/an error — "not
// measured", an error with a Warn; a group absent from the answer — no tracking accumulation.
func TestAlertsQueueFreezeDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	deps := AlertsDeps{Pool: env.pool}
	a := NewAlerts(deps, env.opts)
	if s := snapshotOf(t, a.Pass(context.Background()), AlertQueueFreeze); s.Status != AlertStatusNotMeasured {
		t.Errorf("источник не задан: %s, want not_measured", s.Status)
	}

	env.groups.err = errors.New("monitor: группы q:http: соединение сброшено")
	s := snapshotOf(t, env.Pass(context.Background()), AlertQueueFreeze)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "сброшено") {
		t.Errorf("ошибка источника: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("ошибка источника без Warn:\n%s", env.buf.String())
	}

	// A group vanished from the answer (the stream deleted) — the tracking entry cleaned:
	// a returning group accumulates the window from scratch, not from the past observation.
	now := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	env2 := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.QueueFreeze = time.Minute
		o.Now = func() time.Time { return now }
	})
	env2.groups.groups = []StreamGroupState{
		{Stream: queue.StreamDNS, Group: "dns-worker", Depth: 3, LastDeliveredID: "1-0"},
	}
	_ = snapshotOf(t, env2.Pass(context.Background()), AlertQueueFreeze) // the entry since=t0
	now = now.Add(5 * time.Minute)
	env2.groups.groups = nil // the stream vanished: the pass cleans the tracking
	_ = snapshotOf(t, env2.Pass(context.Background()), AlertQueueFreeze)
	env2.groups.groups = []StreamGroupState{
		{Stream: queue.StreamDNS, Group: "dns-worker", Depth: 3, LastDeliveredID: "1-0"},
	}
	_ = snapshotOf(t, env2.Pass(context.Background()), AlertQueueFreeze) // the entry anew, since=t0+5m
	now = now.Add(30 * time.Second)
	if s := snapshotOf(t, env2.Pass(context.Background()), AlertQueueFreeze); s.Status != AlertStatusOK {
		t.Errorf("30с после возвращения группы: %s, want ok (окно с нуля; выжившая запись дала бы firing)",
			s.Status)
	}
}

// --- PSI CPU (T-162) -------------------------------------------------------------

// TestAlertsPSICPU — window accumulation: above the threshold — accumulating,
// firing on the window close; a dip below the threshold resets the window (a spike does
// not extend it); clearing by returning below the threshold.
func TestAlertsPSICPU(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.PSICPUPercent = 90
		o.PSIWindow = 30 * time.Minute
		o.Now = func() time.Time { return now }
	})
	ctx := context.Background()
	m := env.newPSIMachine()
	set := func(avg300 float64) observation[PSIStat] {
		return obs(PSIStat{Avg10: avg300, Avg60: avg300, Avg300: avg300})
	}

	if s := m.transition(env.Alerts, ctx, set(50)); s.Status != AlertStatusOK {
		t.Fatalf("без давления: %s (%s), want ok", s.Status, s.Detail)
	}
	now = now.Add(10 * time.Minute)
	s := m.transition(env.Alerts, ctx, set(95))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "накопление") {
		t.Fatalf("10 мин из 30: %s (%s), want ok с накоплением", s.Status, s.Detail)
	}
	// A brief dip (avg300 below the threshold) — the window anew: the next
	// 30 minutes accumulate from scratch, not continuing the accumulated.
	_ = m.transition(env.Alerts, ctx, set(85)) // the dip: the window reset
	_ = m.transition(env.Alerts, ctx, set(95)) // the window started anew (held=0)
	now = now.Add(29 * time.Minute)
	if s := m.transition(env.Alerts, ctx, set(95)); s.Status != AlertStatusOK {
		t.Fatalf("29 мин после провала: %s (%s), want ok (окно перезапущено)", s.Status, s.Detail)
	}
	now = now.Add(time.Minute)
	s = m.transition(env.Alerts, ctx, set(95))
	if s.Status != AlertStatusFiring {
		t.Fatalf("30 мин выше порога: %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "95.00%") || !strings.Contains(s.Detail, "30m0s") {
		t.Errorf("detail = %q, want avg300=95.00%% и удержание 30m0s", s.Detail)
	}
	// Clearing by returning below the threshold.
	if s := m.transition(env.Alerts, ctx, set(70)); s.Status != AlertStatusOK {
		t.Errorf("под порогом: %s (%s), want ok", s.Status, s.Detail)
	}
}

// TestAlertsPSIDegradation — a kernel without PSI — a quiet "not measured"
// (errNoPSI, like SMART in a container); other errors — a Warn.
func TestAlertsPSIDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.psi.err = fmt.Errorf("monitor: PSI: %w", errNoPSI)
	s := snapshotOf(t, env.Pass(context.Background()), AlertPSICPU)
	if s.Status != AlertStatusNotMeasured || strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("ядро без PSI: %s, want тихое not_measured (лог:\n%s)", s.Status, env.buf.String())
	}

	env2 := newAlertsTestEnv(t, nil)
	env2.psi.err = errors.New("monitor: PSI: отказано в доступе")
	s = snapshotOf(t, env2.Pass(context.Background()), AlertPSICPU)
	if s.Status != AlertStatusNotMeasured {
		t.Errorf("ошибка чтения: %s, want not_measured", s.Status)
	}
	if !strings.Contains(env2.buf.String(), "level=WARN") {
		t.Errorf("ошибка чтения без Warn:\n%s", env2.buf.String())
	}
}

// --- render debt accumulation (T-175) ---------------------------------------------

// setRenderDebt — a stub of the pass debt: the flag counter in the pool, the q:render
// lag in the lener (the answer queues line up by passes).
func setRenderDebt(env *alertsTestEnv, flags, lag int64) {
	env.pool.debt = append(env.pool.debt, flags)
	env.lener.lens[queue.StreamRender] = lag
}

// TestAlertsRenderDebtGrowth — firing on a debt above the threshold AND
// growth for N passes in a row; the fired alert HOLDS while the debt is above
// 90% of the threshold (stagnation and lag swings do not swing the episode 🚨/✅),
// clearing — by a drain below the clearing threshold; the first pass — the base.
func TestAlertsRenderDebtGrowth(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.RenderDebtDomains = 50000
		o.RenderDebtPasses = 2
	})
	ctx := context.Background()
	m := env.newRenderDebtMachine()
	debt := func(flags, lag int64) observation[RenderDebt] { return obs(RenderDebt{Flags: flags, Lag: lag}) }

	// The base: the debt is already above the threshold, but no growth yet.
	s := m.transition(env.Alerts, ctx, debt(443000, 99000))
	if s.Status != AlertStatusOK {
		t.Fatalf("база: %s (%s), want ok", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "флаги 443000 + лаг 99000 = 542000") {
		t.Errorf("detail = %q, want сводка долга одной строкой", s.Detail)
	}
	if !strings.Contains(s.Detail, "снятие < 45000") {
		t.Errorf("detail = %q, want порог снятия (гистерезис 90%%)", s.Detail)
	}
	// Growing for one pass — the series is short.
	s = m.transition(env.Alerts, ctx, debt(445000, 99000))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Fatalf("один проход роста: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	// The second growth pass — firing.
	s = m.transition(env.Alerts, ctx, debt(447000, 100000))
	if s.Status != AlertStatusFiring {
		t.Fatalf("второй проход роста: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"547000", "+3000", "накопление"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// Stagnation (growth stopped) — the alert HOLDS: a clearing "by
	// no-growth" would swing the episode on the lag swings (the T-175 review, P2-1).
	if s := m.transition(env.Alerts, ctx, debt(447000, 100000)); s.Status != AlertStatusFiring {
		t.Errorf("стагнация: %s (%s), want firing (удержание выше порога снятия)", s.Status, s.Detail)
	}
	// Lag swings around a stationary high debt — also a
	// hold, not a clearing.
	if s := m.transition(env.Alerts, ctx, debt(446500, 100300)); s.Status != AlertStatusFiring {
		t.Errorf("колебание лага: %s (%s), want firing (удержание)", s.Status, s.Detail)
	}
	// A drain below the clearing threshold (90% of 50k) — clearing; the
	// hysteresis corridor (45000..50000) would hold the episode.
	if s := m.transition(env.Alerts, ctx, debt(44680, 300)); s.Status != AlertStatusOK {
		t.Errorf("ниже порога снятия: %s (%s), want ok", s.Status, s.Detail)
	}
	// In the hysteresis corridor (without a fired alert) — ok: the firing
	// threshold is strict.
	if s := m.transition(env.Alerts, ctx, debt(44680, 400)); s.Status != AlertStatusOK {
		t.Errorf("коридор гистерезиса без алерта: %s (%s), want ok", s.Status, s.Detail)
	}
	// Re-accumulation: the series was reset by no-growth; two growth passes
	// above the threshold — both passes are checked (the first — the series is still short).
	_ = m.transition(env.Alerts, ctx, debt(44650, 0)) // streak 0
	s = m.transition(env.Alerts, ctx, debt(49800, 600))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Errorf("первый проход повторного роста: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, debt(49900, 800)); s.Status != AlertStatusFiring {
		t.Errorf("второй проход повторного роста: %s (%s), want firing", s.Status, s.Detail)
	}
	// The hold corridor (45–50k at a fired alert): a clearing "below
	// the firing threshold" does not fire — it holds until 45k.
	if s := m.transition(env.Alerts, ctx, debt(30000, 18000)); s.Status != AlertStatusFiring {
		t.Errorf("коридор удержания 48000: %s (%s), want firing (снятие < 45000, не < 50000)", s.Status, s.Detail)
	}
	// Clearing — only by a drain below the clearing threshold (90% of 50k);
	// 40k < 45k — recovery.
	if s := m.transition(env.Alerts, ctx, debt(20000, 20000)); s.Status != AlertStatusOK {
		t.Errorf("ниже порога снятия: %s (%s), want ok", s.Status, s.Detail)
	}
}

// TestAlertsRenderDebtJitter — a level jitter above the threshold without
// accumulation (a series of positive deltas does not build up) — no false
// firing; a fired alert is not cleared by the jitter (no false
// "recoveries") — the scenario of the T-175 P2-1 review.
func TestAlertsRenderDebtJitter(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.RenderDebtDomains = 50000
		o.RenderDebtPasses = 2
	})
	ctx := context.Background()
	m := env.newRenderDebtMachine()
	debt := func(flags int64) observation[RenderDebt] { return obs(RenderDebt{Flags: flags}) }

	// Two positive deltas in a row do not build up — the alert is silent.
	for _, d := range []RenderDebt{
		{Flags: 60000}, {Flags: 60050}, {Flags: 59980},
		{Flags: 60040}, {Flags: 60020},
	} {
		if s := m.transition(env.Alerts, ctx, obs(d)); s.Status != AlertStatusOK {
			t.Fatalf("дрожание без серии роста: %s (%s), want ok", s.Status, s.Detail)
		}
	}

	// A fired (by two growth passes) alert is not cleared by the jitter:
	// 60000 → 60100 → 60200 (firing) → 60150 → 60180 → 60120 — all above
	// the clearing threshold; one episode.
	_ = m.transition(env.Alerts, ctx, debt(60000))
	_ = m.transition(env.Alerts, ctx, debt(60100))
	if s := m.transition(env.Alerts, ctx, debt(60200)); s.Status != AlertStatusFiring {
		t.Fatalf("рост двумя проходами: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, flags := range []int64{60150, 60180, 60120} {
		if s := m.transition(env.Alerts, ctx, debt(flags)); s.Status != AlertStatusFiring {
			t.Errorf("дрожание @%d: %s (%s), want firing (снятие — только дренаж)", flags, s.Status, s.Detail)
		}
	}
}

// TestAlertsRenderDebtBoundary — the threshold is strict (>): a debt equal to the
// threshold does not fire even with growth; threshold+1 fires with a ready
// series. Growth below the threshold builds the series (crossing the threshold after
// sustained growth is the same accumulation).
func TestAlertsRenderDebtBoundary(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.RenderDebtDomains = 100000
		o.RenderDebtPasses = 2
	})
	ctx := context.Background()
	m := env.newRenderDebtMachine()
	debt := func(flags int64) observation[RenderDebt] { return obs(RenderDebt{Flags: flags}) }

	_ = m.transition(env.Alerts, ctx, debt(99000))   // the base
	s := m.transition(env.Alerts, ctx, debt(100000)) // == the threshold: growth is there, the threshold is not
	if s.Status != AlertStatusOK {
		t.Fatalf("долг == порогу: %s (%s), want ok (строгое сравнение)", s.Status, s.Detail)
	}
	s = m.transition(env.Alerts, ctx, debt(100001)) // threshold+1 with a growth series of 2
	if s.Status != AlertStatusFiring {
		t.Fatalf("порог+1 с серией 2: %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsRenderDebtDegradation — Redis not set / a counter error /
// the counter row missing (a manual edit) / an XLEN error — "not
// measured" with a Warn; the tracking memory is not touched by an error (recovery
// will finish the job on the actual debt).
func TestAlertsRenderDebtDegradation(t *testing.T) {
	// Redis not set — like the queue alerts.
	env := newAlertsTestEnv(t, nil)
	a := NewAlerts(AlertsDeps{Pool: env.pool}, env.opts)
	if s := snapshotOf(t, a.Pass(context.Background()), AlertRenderDebt); s.Status != AlertStatusNotMeasured {
		t.Errorf("Redis не задан: %s, want not_measured", s.Status)
	}

	// A counter query error — a Warn.
	env2 := newAlertsTestEnv(t, nil)
	env2.pool.debtErrs = []error{errors.New("бд недоступна")}
	s := snapshotOf(t, env2.Pass(context.Background()), AlertRenderDebt)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "бд недоступна") {
		t.Errorf("ошибка счётчика: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env2.buf.String(), "level=WARN") {
		t.Errorf("ошибка счётчика без Warn:\n%s", env2.buf.String())
	}

	// No counter row (the migration not applied / a manual edit) — an error,
	// not a silent zero: the debt gauge must not lie.
	env3 := newAlertsTestEnv(t, nil)
	env3.pool.debtEmpty = true
	s = snapshotOf(t, env3.Pass(context.Background()), AlertRenderDebt)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "render_pending") {
		t.Errorf("строки счётчика нет: %s (%s), want not_measured про строку", s.Status, s.Detail)
	}

	// An XLEN error — a Warn. Before render debt, Len is called 5 more times (4
	// streams of the queue alert + q:dead) — the error is placed sixth in the queue
	// (the T-246 fallback depth alert goes after render debt and does not
	// shift the order).
	env4 := newAlertsTestEnv(t, nil)
	env4.lener.errs = []error{nil, nil, nil, nil, nil, errors.New("redis: xlen")}
	s = snapshotOf(t, env4.Pass(context.Background()), AlertRenderDebt)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "q:render") {
		t.Errorf("ошибка XLEN: %s (%s), want not_measured про q:render", s.Status, s.Detail)
	}
	if !strings.Contains(env4.buf.String(), "level=WARN") {
		t.Errorf("ошибка XLEN без Warn:\n%s", env4.buf.String())
	}

	// The tracking memory survives an error: after the failure growth is counted from
	// the last successful snapshot.
	env5 := newAlertsTestEnv(t, func(o *AlertsOptions) { o.RenderDebtPasses = 2 })
	setRenderDebt(env5, 60000, 0)
	_ = snapshotOf(t, env5.Pass(context.Background()), AlertRenderDebt) // the base: prev=60000
	env5.pool.debtErrs = []error{errors.New("бд недоступна")}
	_ = snapshotOf(t, env5.Pass(context.Background()), AlertRenderDebt) // the failure: the memory untouched
	setRenderDebt(env5, 61000, 0)
	if s := snapshotOf(t, env5.Pass(context.Background()), AlertRenderDebt); s.Status != AlertStatusOK ||
		!strings.Contains(s.Detail, "серия роста 1/2") {
		t.Errorf("рост после сбоя: %s (%s), want ok с серией 1/2 от последнего снимка", s.Status, s.Detail)
	}

	// The fired episode survives a source failure: the hold relies
	// on the tracking latch, not the past status ("not measured"
	// overwrites the state but does not break the episode) — review round 2, P2-1.
	env6 := newAlertsTestEnv(t, func(o *AlertsOptions) { o.RenderDebtPasses = 2 })
	ctx6 := context.Background()
	setRenderDebt(env6, 60000, 0)
	_ = snapshotOf(t, env6.Pass(ctx6), AlertRenderDebt) // the base
	setRenderDebt(env6, 61000, 0)
	_ = snapshotOf(t, env6.Pass(ctx6), AlertRenderDebt) // growth
	setRenderDebt(env6, 62000, 0)
	if s := snapshotOf(t, env6.Pass(ctx6), AlertRenderDebt); s.Status != AlertStatusFiring {
		t.Fatalf("до сбоя: %s (%s), want firing", s.Status, s.Detail)
	}
	env6.pool.debtErrs = []error{errors.New("бд недоступна")}
	if s := snapshotOf(t, env6.Pass(ctx6), AlertRenderDebt); s.Status != AlertStatusNotMeasured {
		t.Fatalf("сбой источника: %s, want not_measured", s.Status)
	}
	setRenderDebt(env6, 61500, 0) // a jitter around the same debt, above the clearing threshold
	if s := snapshotOf(t, env6.Pass(ctx6), AlertRenderDebt); s.Status != AlertStatusFiring {
		t.Errorf("после сбоя, долг 61500: %s (%s), want firing (удержание пережило сбой — без ложного «восстановлено»)", s.Status, s.Detail)
	}
	setRenderDebt(env6, 20000, 0) // a drain below the clearing threshold
	if s := snapshotOf(t, env6.Pass(ctx6), AlertRenderDebt); s.Status != AlertStatusOK {
		t.Errorf("дренаж после сбоя: %s (%s), want ok (эпизод закрыт честно)", s.Status, s.Detail)
	}
}

// TestAlertsRenderDebtTinyThreshold — a threshold of 1 (the edge of the config corridor):
// the clearing threshold — ceil(90%) = 1; clearing is reachable by a zero debt
// (review round 2, P3-3: the truncation int64(0.9) = 0 would make the alert sticky).
func TestAlertsRenderDebtTinyThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.RenderDebtDomains = 1
		o.RenderDebtPasses = 1
	})
	ctx := context.Background()
	m := env.newRenderDebtMachine()
	debt := func(flags int64) observation[RenderDebt] { return obs(RenderDebt{Flags: flags}) }

	_ = m.transition(env.Alerts, ctx, debt(0))  // the base
	s := m.transition(env.Alerts, ctx, debt(2)) // a growth of +2, above the threshold of 1
	if s.Status != AlertStatusFiring {
		t.Fatalf("порог 1: %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "снятие < 1") {
		t.Errorf("detail = %q, want «снятие < 1» (ceil 90%% от 1)", s.Detail)
	}
	// Exactly the clearing threshold — a hold.
	if s := m.transition(env.Alerts, ctx, debt(1)); s.Status != AlertStatusFiring {
		t.Errorf("долг 1 при пороге снятия 1: %s (%s), want firing (удержание)", s.Status, s.Detail)
	}
	// Zero — clearing.
	if s := m.transition(env.Alerts, ctx, debt(0)); s.Status != AlertStatusOK {
		t.Errorf("нулевой долг: %s (%s), want ok (снятие достижимо)", s.Status, s.Detail)
	}
}

// --- "status hung" domains (T-256, FR-2.2) --------------------------------------

// staleSnap — a snapshot of the new_done_stale row: minutes from the fixed
// base (the reconciliation pass stamps differ — the series grows only on a
// new snapshot, the T-256 P2-1 review).
func staleSnap(v int64, min int) alertStaleSnap {
	now, _ := alertsFixedClock()
	return alertStaleSnap{value: v, at: now.Add(time.Duration(min) * time.Minute)}
}

// TestAlertsNewDoneStaleEpisode — the domains_new_done_stale episode: zero in
// the norm, the series grows only on NEW reconciliation snapshots (the alert
// passes are more frequent than the source — repeated reads of one row do not
// count as observations), firing on the second non-zero snapshot, holding
// by any non-zero row, clearing by zero; one Warn per episode.
func TestAlertsNewDoneStaleEpisode(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newDomainsStaleMachine()
	snap := func(v, min int64) observation[staleRow] {
		at, _ := alertsFixedClock()
		return obs(staleRow{
			value: v,
			snap:  sql.NullTime{Time: at.Add(time.Duration(min) * time.Minute), Valid: true},
		})
	}

	steps := []struct {
		value, min int64
		want       string
	}{
		{0, 0, AlertStatusOK},      // the norm: no hung domains
		{3, 30, AlertStatusOK},     // the first non-zero snapshot 1/2
		{3, 30, AlertStatusOK},     // the same snapshot — the series does not grow (the alert polls more often than the source)
		{3, 60, AlertStatusFiring}, // the second non-zero snapshot 2/2 — persistent
		{1, 60, AlertStatusFiring}, // hold: any non-zero row
		{0, 90, AlertStatusOK},     // the T-030 recheck closed it — clearing
		{7, 120, AlertStatusOK},    // a new episode after clearing — 1/2
	}
	for i, s := range steps {
		got := m.transition(env.Alerts, ctx, snap(s.value, s.min))
		if got.Status != s.want {
			t.Errorf("шаг %d (new_done_stale=%d@+%dм): статус = %s (%s), want %s",
				i, s.value, s.min, got.Status, got.Detail, s.want)
		}
	}
	// The day-boundary edge case (the P2-1 review): a single non-zero
	// snapshot (midnight aged yesterday's in-flight episodes) is overwritten
	// by zero by the next pass — no alert at all.
	env2 := newAlertsTestEnv(t, nil)
	m2 := env2.newDomainsStaleMachine()
	got := runSequence(env2.Alerts, m2, snap(2, 0), snap(0, 30))
	wantSequence(t, "пограничный снапшот", got, []string{AlertStatusOK, AlertStatusOK})
	if strings.Contains(env2.buf.String(), "АЛЕРТ") {
		t.Errorf("пограничный снапшот дал Warn:\n%s", env2.buf.String())
	}

	// The detail with the level, the snapshot counter and the treatment path: the
	// series grows on every new snapshot (3/2 — over-fulfilled, a hold).
	env3 := newAlertsTestEnv(t, nil)
	m3 := env3.newDomainsStaleMachine()
	_ = m3.transition(env3.Alerts, ctx, snap(9, 0))  // 1/2
	_ = m3.transition(env3.Alerts, ctx, snap(9, 30)) // 2/2 — firing
	g := m3.transition(env3.Alerts, ctx, snap(12, 60))
	for _, want := range []string{"12", "3/2", "crawler_domains_new_done_stale"} {
		if !strings.Contains(g.Detail, want) {
			t.Errorf("detail %q не содержит %q", g.Detail, want)
		}
	}

	// One Warn per episode: the firing Warn "ALERT", the hold and
	// the clearing — without a repeated Warn (a full pass).
	env4 := newAlertsTestEnv(t, nil)
	env4.pool.stale = []alertStaleSnap{staleSnap(9, 0), staleSnap(9, 30)}
	for range 2 {
		env4.Pass(ctx)
	}
	if n := strings.Count(env4.buf.String(), "АЛЕРТ: домены «статус повис»"); n != 1 {
		t.Errorf("Warn срабатывания = %d, want 1:\n%s", n, env4.buf.String())
	}
	env4.pool.stale = []alertStaleSnap{staleSnap(9, 90)}
	if got := snapshotOf(t, env4.Pass(ctx), AlertDomainsStale); got.Status != AlertStatusFiring {
		t.Fatalf("устойчивое срабатывание: %s, want firing", got.Status)
	}
	before := env4.buf.String()
	env4.pool.stale = nil // zeros by default
	s := snapshotOf(t, env4.Pass(ctx), AlertDomainsStale)
	if s.Status != AlertStatusOK {
		t.Fatalf("нулевая строка: %s, want ok", s.Status)
	}
	if delta := env4.buf.String()[len(before):]; strings.Contains(delta, "level=WARN") {
		t.Errorf("снятие дало Warn:\n%s", delta)
	} else if !strings.Contains(delta, "level=INFO") || !strings.Contains(delta, "алерт снят") {
		t.Errorf("снятие без Info:\n%s", delta)
	}
}

// TestAlertsNewDoneStaleDegradation — the new_done_stale row not yet
// created by the first reconciliation pass — a quiet "not measured" (without
// a Warn and without a false zero); a read error — "not measured" with a Warn,
// the series of observations is not reset by an error: an unknown observation is
// not a zero one.
func TestAlertsNewDoneStaleDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.staleEmpty = true
	s := snapshotOf(t, env.Pass(context.Background()), AlertDomainsStale)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("строки нет: статус = %s, want not_measured", s.Status)
	}
	if strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("гонка старта циклов дала Warn:\n%s", env.buf.String())
	}

	ctx := context.Background()
	env = newAlertsTestEnv(t, nil)
	env.pool.stale = []alertStaleSnap{staleSnap(4, 0)}
	if s := snapshotOf(t, env.Pass(ctx), AlertDomainsStale); s.Status != AlertStatusOK {
		t.Fatalf("первое наблюдение: %s, want ok (1/2)", s.Status)
	}
	env.pool.staleErrs = []error{errors.New("connection refused")}
	if s := snapshotOf(t, env.Pass(ctx), AlertDomainsStale); s.Status != AlertStatusNotMeasured {
		t.Fatalf("ошибка чтения: %s, want not_measured", s.Status)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "connection refused") {
		t.Errorf("ошибка чтения строки без Warn:\n%s", env.buf.String())
	}
	// An error does not touch the tracking memory: the same snapshot after the failure —
	// the series is still 1 (not "the first observation"); a new non-zero
	// snapshot is the second — firing.
	env.pool.staleErrs = nil
	env.pool.stale = []alertStaleSnap{staleSnap(4, 0)}
	if s := snapshotOf(t, env.Pass(ctx), AlertDomainsStale); s.Status != AlertStatusOK || !strings.Contains(s.Detail, "1/2") {
		t.Fatalf("тот же снапшот после сбоя: %s (%s), want ok 1/2 (серия сохранена)", s.Status, s.Detail)
	}
	env.pool.stale = []alertStaleSnap{staleSnap(4, 30)}
	if s := snapshotOf(t, env.Pass(ctx), AlertDomainsStale); s.Status != AlertStatusFiring {
		t.Fatalf("новый снапшот после сбоя: %s, want firing (серия сохранена)", s.Status)
	}

	// An unreadable row and an iteration error — "not measured" with a Warn.
	env = newAlertsTestEnv(t, nil)
	env.pool.staleBadRow = true
	if s := snapshotOf(t, env.Pass(ctx), AlertDomainsStale); s.Status != AlertStatusNotMeasured {
		t.Errorf("нечитаемая строка: %s, want not_measured", s.Status)
	}
	env = newAlertsTestEnv(t, nil)
	env.pool.staleRowsErr = errors.New("iteration failed")
	if s := snapshotOf(t, env.Pass(ctx), AlertDomainsStale); s.Status != AlertStatusNotMeasured {
		t.Errorf("ошибка итерации: %s, want not_measured", s.Status)
	}
}

// --- the AlertsLoop cycle --------------------------------------------------------

// TestAlertsLoopPasses — the first pass right at start; a source error
// does not kill the loop (the second pass on the tick); a ctx cancellation — a clean nil.
func TestAlertsLoopPasses(t *testing.T) {
	var mu sync.Mutex
	passes := 0
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.Interval = 2 * time.Millisecond
		o.OnPass = func(AlertsResult) { mu.Lock(); passes++; mu.Unlock() }
	})
	env.pool.dailyErrs = []error{errors.New("бд недоступна")} // the starting pass — an error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- AlertsLoop(ctx, AlertsDeps{
			Pool: env.pool, Redis: env.lener, Space: env.space, Wear: env.wear,
		}, env.opts)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := passes
		mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("проходов = %d, want ≥ 3 (цикл живёт после ошибки)", n)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("AlertsLoop: %v, want nil при отмене ctx", err)
	}
}

// TestAlertsLoopCancelDuringPass — a ctx cancellation during the pass: without a Warn
// (part of the shutdown), a clean nil — the OnRun contract (the ZoneStatsLoop style).
func TestAlertsLoopCancelDuringPass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &alertsPool{dailyErrs: []error{context.Canceled}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before the start: the first pass query will fail

	done := make(chan error, 1)
	go func() {
		done <- AlertsLoop(ctx, AlertsDeps{Pool: pool}, AlertsOptions{Log: logger})
	}()

	if err := <-done; err != nil {
		t.Fatalf("AlertsLoop: %v, want nil при отмене ctx", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("отмена во время прохода — Warn:\n%s", buf.String())
	}
}

// TestAlertsLoopFirstPassImmediate — the first pass runs right at
// start, without waiting for the tick: the loop interval is an hour; the only way to see
// OnPass within the test seconds is the starting pass.
func TestAlertsLoopFirstPassImmediate(t *testing.T) {
	passed := make(chan struct{}, 1)
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.Interval = time.Hour
		o.OnPass = func(AlertsResult) {
			select {
			case passed <- struct{}{}:
			default:
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- AlertsLoop(ctx, AlertsDeps{
			Pool: env.pool, Redis: env.lener, Space: env.space, Wear: env.wear,
		}, env.opts)
	}()

	select {
	case <-passed: // the starting pass — right away
	case <-time.After(5 * time.Second):
		t.Fatal("первый проход не выполнен при старте (цикл ждёт тика)")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("AlertsLoop: %v, want nil при отмене ctx", err)
	}
}

func TestAlertsLoopNilPool(t *testing.T) {
	err := AlertsLoop(context.Background(), AlertsDeps{}, AlertsOptions{})
	if err == nil || !strings.Contains(err.Error(), "пул БД не задан") {
		t.Errorf("ошибка = %v, want «пул БД не задан»", err)
	}
}

// TestAlertBytes — the boundaries of the formatting steps: exactly 1<<30/1<<20/1<<10
// fall into the senior step (inclusive boundaries), a byte less — into the junior one;
// the half and one-and-a-half values pin the divisor of each step.
func TestAlertBytes(t *testing.T) {
	tests := []struct {
		name string
		n    uint64
		want string
	}{
		{"ноль", 0, "0B"},
		{"байты", 1, "1B"},
		{"килобайт без кибибайта", 1023, "1023B"},
		{"ровно кибибайт", 1 << 10, "1.0KiB"},
		{"полтора кибибайта", 1536, "1.5KiB"},
		{"кибибайт до мибибайта", 1<<20 - 1, "1024.0KiB"},
		{"ровно мибибайт", 1 << 20, "1.0MiB"},
		{"полтора мибибайта", 3 << 19, "1.5MiB"},
		{"мибибайт до гибибайта", 1<<30 - 1, "1024.0MiB"},
		{"ровно гибибайт", 1 << 30, "1.0GiB"},
		{"полтора гибибайта", 3 << 29, "1.5GiB"},
		{"много гибибайт", 5 << 30, "5.0GiB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := alertBytes(tt.n); got != tt.want {
				t.Errorf("alertBytes(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}

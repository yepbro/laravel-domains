// Юнит-тесты алертов мониторинга (T-080, ТЗ 5.9, 8, 10.3) на фейковых
// источниках: пул pgx (доля temp_error, zones.updated_at), XLEN стримов,
// statfs каталога контента, износ NVMe. Покрыты: контракты SQL, пороги и
// гистерезис на точных границах (включение/снятие, минимум выборки,
// фактор остановки, окно мусорных хешей, формат байтов), деградации
// источников всех форм (ошибка запроса, пустой ответ, ошибка чтения,
// ошибка итерации — Warn, цикл живёт), деградация SMART в контейнере
// (без Warn и без ложного срабатывания), фронтовые записи лога, хук
// OnPass, цикл AlertsLoop. Реальное чтение SMART — за устройствами,
// поведение против живой БД — test/integration.
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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- фейковые источники -----------------------------------------------------

// alertShare — ответ dnsShareSQL.
type alertShare struct {
	temp  int64
	total int64
}

// alertsPool — db.DB без БД: маршрутизация двух SQL алертов, очереди
// результатов и ошибок, протокол вызовов. Под замком: AlertsLoop работает
// в горутине, тесты ждут прогресса по счётчику проходов.
type alertsPool struct {
	mu    sync.Mutex
	share []alertShare   // очередь ответов dnsShareSQL (кончилась — нули)
	fresh []sql.NullTime // очередь ответов zoneFreshSQL (кончилась — нулевая)

	shareErrs []error
	freshErrs []error

	// Деградации формы ответа: без строк (rows.Next ложен), строка с
	// нечитаемыми типами (ошибка Scan), ошибка итерации (rows.Err).
	shareEmpty, shareBadRow    bool
	freshEmpty, freshBadRow    bool
	shareRowsErr, freshRowsErr error

	queries []zonestatsCall // переиспользован: sql + args
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
	case dnsShareSQL:
		if err := popErr(&p.shareErrs); err != nil {
			return nil, err
		}
		if p.shareEmpty {
			return &alertRows{err: p.shareRowsErr}, nil
		}
		if p.shareBadRow {
			return &alertRows{rows: [][]any{{"байты", "байты"}}, err: p.shareRowsErr}, nil
		}
		s := alertShare{}
		if len(p.share) > 0 {
			s = p.share[0]
			p.share = p.share[1:]
		}
		return &alertRows{rows: [][]any{{s.temp, s.total}}, err: p.shareRowsErr}, nil
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
	default:
		return nil, fmt.Errorf("alertsPool: неожидаемый SQL Query: %s", sqlQ)
	}
}

// popErr достаёт голову очереди ошибок (nil — без ошибки); вызов под
// замком владельца.
func popErr(q *[]error) error {
	if len(*q) == 0 {
		return nil
	}
	err := (*q)[0]
	*q = (*q)[1:]
	return err
}

// calls — снимок протокола Query под замком.
func (p *alertsPool) calls() []zonestatsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]zonestatsCall(nil), p.queries...)
}

// alertRows — pgx.Rows с заготовленными строками (Scan позиционный; типы
// int64/sql.NullTime — всё, что читают алерты). Стиль statusRows
// (cmd/crawlerctl/status_test.go). err — ответ rows.Err (nil по умолчанию).
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

// fakeLener — XLEN по карте (нет стрима — 0), очередь ошибок, протокол.
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

// fakeSpace — statfs каталога контента по заготовке.
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

// fakeWear — износ NVMe по заготовке.
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

// --- сборка тестового оценщика ----------------------------------------------

// alertsTestEnv — оценщик на фейках с журналом лога; дефолты здоровы:
// доля 0 %, зоны свежие, очереди пустые, места достаточно, износ мал.
type alertsTestEnv struct {
	*Alerts
	pool  *alertsPool
	lener *fakeLener
	space *fakeSpace
	wear  *fakeWear
	buf   *bytes.Buffer
}

// alertsFixedClock — те же фиксированные часы, что у тестов zonestats.
func alertsFixedClock() (time.Time, func() time.Time) {
	return zonestatsFixedClock()
}

// alertHealthyShares — очередь здоровых ответов dnsShareSQL (доля 0 %)
// на n проходов: тесты, не занятые долей ошибок, не упираются в пустую
// очередь.
func alertHealthyShares(n int) []alertShare {
	shares := make([]alertShare, n)
	for i := range shares {
		shares[i] = alertShare{temp: 0, total: 1000}
	}
	return shares
}

// alertHealthyFresh — очередь свежих ответов zoneFreshSQL (минута назад)
// на n проходов.
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
		share: alertHealthyShares(10),
		fresh: alertHealthyFresh(now, 10),
	}
	lener := &fakeLener{lens: map[string]int64{}}
	space := &fakeSpace{stat: SpaceStat{Avail: 100 << 30, Total: 200 << 30}}
	wear := &fakeWear{wears: []NVMeWear{{Device: "nvme0", PercentUsed: 10}}}

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
		Alerts: NewAlerts(AlertsDeps{Pool: pool, Redis: lener, Space: space, Wear: wear}, opts),
		pool:   pool, lener: lener, space: space, wear: wear, buf: &buf,
	}
}

// snapshot — состояние алерта по имени из снапшота прохода.
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

// --- доля ошибок DNS ---------------------------------------------------------

// TestAlertsDNSContract — точный SQL (окно — текущие сутки по date-колонке
// last_checked_at) и параметр-статус temp_error; доля ≥ порога — алерт.
func TestAlertsDNSContract(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.share = []alertShare{{temp: 350, total: 1000}}

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
		if c.sql == dnsShareSQL {
			dns = append(dns, c)
		}
	}
	if len(dns) != 1 {
		t.Fatalf("запросов dnsShareSQL = %d, want 1", len(dns))
	}
	if !reflect.DeepEqual(dns[0].args, []any{int16(model.StatusTempError)}) {
		t.Errorf("аргументы = %v, want [temp_error]", dns[0].args)
	}
}

// TestAlertsDNSHysteresis — включение на 30 %, удержание в коридоре
// 20–30 %, снятие ниже 20 %; до срабатывания доля в коридоре не включает
// алерт.
func TestAlertsDNSHysteresis(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	steps := []struct {
		share alertShare
		want  string
	}{
		{alertShare{300, 1000}, AlertStatusFiring}, // 30.0 % — включение
		{alertShare{250, 1000}, AlertStatusFiring}, // 25.0 % — коридор гистерезиса
		{alertShare{200, 1000}, AlertStatusFiring}, // ровно 20.0 % — удержание (граница снятия)
		{alertShare{199, 1000}, AlertStatusOK},     // 19.9 % — снятие
		{alertShare{250, 1000}, AlertStatusOK},     // 25 % без сработавшего алерта — не включается
	}
	env.pool.share = make([]alertShare, len(steps))
	for i, s := range steps {
		env.pool.share[i] = s.share
	}
	for i, s := range steps {
		got := snapshotOf(t, env.Pass(ctx), AlertDNSErrors)
		if got.Status != s.want {
			t.Errorf("шаг %d (доля %d/%d): статус = %s (%s), want %s",
				i, s.share.temp, s.share.total, got.Status, got.Detail, s.want)
		}
	}
}

// TestAlertsDNSMinSamples — пустое окно (меньше 100 проверок за сутки):
// доля не показательна — «не измеряется», не срабатывание. Ровно 100 —
// уже показательна: граница строгая (меньше, не «не больше»).
func TestAlertsDNSMinSamples(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.share = []alertShare{{temp: 2, total: 50}}

	s := snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("статус = %s (%s), want not_measured", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "50") || !strings.Contains(s.Detail, "100") {
		t.Errorf("detail %q без чисел выборки", s.Detail)
	}

	// Ровно порог выборки — доля считается (0 из 100 — корректный ноль).
	env.pool.share = []alertShare{{temp: 0, total: 100}}
	s = snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "из 100") {
		t.Errorf("ровно 100 проверок: статус = %s (%s), want ok с долей по 100", s.Status, s.Detail)
	}
}

// TestAlertsDNSSourceError — ошибка запроса: Warn, алерт «не измеряется»,
// следующий проход работает (цикл живёт).
func TestAlertsDNSSourceError(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.shareErrs = []error{errors.New("бд недоступна")}

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

// TestAlertsDNSSourceErrors — деградации формы ответа: пустой результат
// (нет строк), нечитаемая строка (ошибка Scan), ошибка итерации (rows.Err)
// — ошибки источника с Warn и причиной в detail, не молчаливое «мало
// данных» по нулям.
func TestAlertsDNSSourceErrors(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.pool.shareEmpty = true
	s := snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "пустой ответ") {
		t.Errorf("пустой ответ: статус = %s (%s), want not_measured «пустой ответ»", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("пустой ответ без Warn:\n%s", env.buf.String())
	}

	env = newAlertsTestEnv(t, nil)
	env.pool.shareBadRow = true
	s = snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "чтение") {
		t.Errorf("ошибка Scan: статус = %s (%s), want not_measured «чтение»", s.Status, s.Detail)
	}

	env = newAlertsTestEnv(t, nil)
	env.pool.shareRowsErr = errors.New("соединение сброшено")
	s = snapshotOf(t, env.Pass(context.Background()), AlertDNSErrors)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "соединение сброшено") {
		t.Errorf("rows.Err: статус = %s (%s), want not_measured с ошибкой итерации", s.Status, s.Detail)
	}
}

// --- переполнение очередей ---------------------------------------------------

// TestAlertsQueuesThresholds — срабатывание на пороге гейта (len ≥
// watermark), удержание до 90 % порога, снятие ниже; стримы — только
// рабочие, q:dead гейтом не ограничивается и не опрашивается.
func TestAlertsQueuesThresholds(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.lener.lens[queue.StreamDead] = 1 << 40 // никогда не опрашивается
	ctx := context.Background()
	steps := []struct {
		lens map[string]int64
		want string
	}{
		{map[string]int64{}, AlertStatusOK},                          // пустые очереди: detail называет стрим, не «»
		{map[string]int64{queue.StreamDNS: 999}, AlertStatusOK},      // ниже порога
		{map[string]int64{queue.StreamDNS: 1000}, AlertStatusFiring}, // порог гейта
		{map[string]int64{queue.StreamHTTP: 950}, AlertStatusFiring}, // коридор гистерезиса
		{map[string]int64{queue.StreamHTTP: 900}, AlertStatusFiring}, // ровно 90 % порога — удержание
		{map[string]int64{queue.StreamHTTP: 899}, AlertStatusOK},     // ниже 90 %
	}
	for i, s := range steps {
		env.lener.lens = s.lens
		got := snapshotOf(t, env.Pass(ctx), AlertQueueOverflow)
		if got.Status != s.want {
			t.Errorf("шаг %d (%v): статус = %s (%s), want %s", i, s.lens, got.Status, got.Detail, s.want)
		}
		// Коридор гистерезиса (нет горячего стрима): деталь называет
		// удерживающий стрим и глубину, а не пустую строку порога.
		if i == 3 && !strings.Contains(got.Detail, "q:http=950") {
			t.Errorf("коридор гистерезиса: detail = %q, want q:http=950", got.Detail)
		}
		if i == 0 && !strings.Contains(got.Detail, "q:dns=0") {
			t.Errorf("пустые очереди: detail = %q, want макс q:dns=0 (имя стрима, не пустота)", got.Detail)
		}
	}

	// Деталь «ok» — максимум среди рабочих стримов: обновление в цикле
	// (лидер не первый стрим) и стартовое значение (лидер первый стрим
	// читается из карты, а не нулём соседа).
	env.lener.lens = map[string]int64{queue.StreamDNS: 3}
	if s := snapshotOf(t, env.Pass(ctx), AlertQueueOverflow); s.Status != AlertStatusOK || !strings.Contains(s.Detail, "макс q:dns=3") {
		t.Errorf("максимум q:dns: %s (%s), want ok макс q:dns=3", s.Status, s.Detail)
	}
	env.lener.lens = map[string]int64{queue.StreamHTTP: 5}
	if s := snapshotOf(t, env.Pass(ctx), AlertQueueOverflow); s.Status != AlertStatusOK || !strings.Contains(s.Detail, "макс q:http=5") {
		t.Errorf("максимум q:http: %s (%s), want ok макс q:http=5", s.Status, s.Detail)
	}

	for _, s := range env.lener.streams() {
		if s == queue.StreamDead {
			t.Error("q:dead опрошен алертом очередей")
		}
	}
	// detail сработавшего шага называет стрим и длину.
	env.lener.lens = map[string]int64{queue.StreamRender: 4242}
	s := snapshotOf(t, env.Pass(ctx), AlertQueueOverflow)
	if s.Status != AlertStatusFiring || !strings.Contains(s.Detail, "q:render=4242") {
		t.Errorf("сработавший detail = %q (%s), want q:render=4242 firing", s.Detail, s.Status)
	}
}

// TestAlertsQueuesNotConfigured — без Redis и без порога алерт очередей
// «не измеряется», не срабатывает ложно.
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

	// Порог 1 — минимальный валидный watermark (стрим длиной 1), а не
	// «не задан»: алерт измеряется.
	env = newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 1 })
	if s := snapshotOf(t, env.Pass(context.Background()), AlertQueueOverflow); s.Status != AlertStatusOK {
		t.Errorf("watermark 1: статус = %s (%s), want ok — порог валиден", s.Status, s.Detail)
	}
}

// TestAlertsQueuesSourceError — недоступный Redis: Warn, «не измеряется»,
// цикл живёт.
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

// --- остановка прогресса ------------------------------------------------------

// TestAlertsStallThreshold — свежесть zones.updated_at в периодах
// пересчёта: 60 с при пороге 5×60 с — ок, 301 с — остановка; NULL — зон
// нет, не измеряется.
func TestAlertsStallThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil) // ZoneStatsInterval 60s → порог 300s
	now, _ := alertsFixedClock()
	ctx := context.Background()

	env.pool.fresh = []sql.NullTime{{Time: now.Add(-60 * time.Second), Valid: true}}
	if s := snapshotOf(t, env.Pass(ctx), AlertProgressStall); s.Status != AlertStatusOK {
		t.Errorf("60с: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	// Между 4 и 5 периодами (250с при пороге 5×60с) — ок: фиксирует
	// фактор порога, а не «какой-то из соседних».
	env.pool.fresh = []sql.NullTime{{Time: now.Add(-250 * time.Second), Valid: true}}
	if s := snapshotOf(t, env.Pass(ctx), AlertProgressStall); s.Status != AlertStatusOK {
		t.Errorf("250с: статус = %s (%s), want ok (порог 5, не 4 периода)", s.Status, s.Detail)
	}

	// Ровно порог (300с = 5 × 60с) — ещё не остановка: строго больше.
	env.pool.fresh = []sql.NullTime{{Time: now.Add(-5 * time.Minute), Valid: true}}
	if s := snapshotOf(t, env.Pass(ctx), AlertProgressStall); s.Status != AlertStatusOK {
		t.Errorf("ровно 300с: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	env.pool.fresh = []sql.NullTime{{Time: now.Add(-301 * time.Second), Valid: true}}
	s := snapshotOf(t, env.Pass(ctx), AlertProgressStall)
	if s.Status != AlertStatusFiring {
		t.Fatalf("301с: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"5m1s", "5", "1m0s"} { // возраст, фактор, период
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail %q не содержит %s", s.Detail, want)
		}
	}

	env.pool.fresh = []sql.NullTime{{}}
	if s := snapshotOf(t, env.Pass(ctx), AlertProgressStall); s.Status != AlertStatusNotMeasured {
		t.Errorf("без зон: статус = %s, want not_measured", s.Status)
	}
}

// TestAlertsStallSourceErrors — деградации источника свежести: ошибка
// запроса, пустой ответ, нечитаемая строка, ошибка итерации — Warn и
// «не измеряется» с причиной, цикл алертов жив.
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

// TestAlertsStallDefaultInterval — нулевой ZoneStatsInterval — дефолт 60 с
// (DefaultZoneStatsInterval): порог 5 минут, а не ноль.
func TestAlertsStallDefaultInterval(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.ZoneStatsInterval = 0 })
	now, _ := alertsFixedClock()
	env.pool.fresh = []sql.NullTime{{Time: now.Add(-4 * time.Minute), Valid: true}}

	if s := snapshotOf(t, env.Pass(context.Background()), AlertProgressStall); s.Status != AlertStatusOK {
		t.Errorf("4мин при дефолтных 5×60с: статус = %s (%s), want ok", s.Status, s.Detail)
	}
}

// --- место на диске -----------------------------------------------------------

// TestAlertsDiskThreshold — та же формула, что пауза Put (strictly ниже
// порога): 4 % при пороге 5 % — алерт, ровно 5 % — ок.
func TestAlertsDiskThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()

	env.space.stat = SpaceStat{Avail: 4 << 30, Total: 100 << 30} // 4.0 %
	s := snapshotOf(t, env.Pass(ctx), AlertDiskFree)
	if s.Status != AlertStatusFiring {
		t.Fatalf("4%%: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "4.0%") || !strings.Contains(s.Detail, "5.0%") {
		t.Errorf("detail %q без доли/порога", s.Detail)
	}

	env.space.stat = SpaceStat{Avail: 5 << 30, Total: 100 << 30} // ровно порог — не меньше
	if s := snapshotOf(t, env.Pass(ctx), AlertDiskFree); s.Status != AlertStatusOK {
		t.Errorf("ровно порог: статус = %s (%s), want ok", s.Status, s.Detail)
	}

	// Порог 1 % — минимальный валидный контроль, не «выключен».
	env = newAlertsTestEnv(t, func(o *AlertsOptions) { o.DiskFreePercent = 1 })
	if s := snapshotOf(t, env.Pass(ctx), AlertDiskFree); s.Status != AlertStatusOK {
		t.Errorf("порог 1%%: статус = %s (%s), want ok — контроль включён", s.Status, s.Detail)
	}

	// Множитель 100: свободно 100 из 10100 байт — 0.99… % < 1 — алерт;
	// 101·Avail/Total дало бы ровно 1.0 % и сняло алерт.
	env = newAlertsTestEnv(t, func(o *AlertsOptions) { o.DiskFreePercent = 1 })
	env.space.stat = SpaceStat{Avail: 100, Total: 10100}
	if s := snapshotOf(t, env.Pass(ctx), AlertDiskFree); s.Status != AlertStatusFiring {
		t.Errorf("100 из 10100 байт: статус = %s (%s), want firing (0.99%% < 1%%)", s.Status, s.Detail)
	}
}

// TestAlertsDiskDegradation — контроль выключен, каталог не задан, ФС без
// ёмкости, ошибка statfs (Warn) — «не измеряется», не ложное срабатывание.
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

// --- износ NVMe ---------------------------------------------------------------

// TestAlertsNVMeWear — алерт на 90 % ресурса; несколько устройств — по
// худшему; detail с TBW.
func TestAlertsNVMeWear(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.wear.wears = []NVMeWear{{Device: "nvme0", PercentUsed: 90, DataUnitsWritten: 200_000_000}}

	s := snapshotOf(t, env.Pass(context.Background()), AlertNVMeWear)
	if s.Status != AlertStatusFiring {
		t.Fatalf("90%%: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "nvme0=90%") || !strings.Contains(s.Detail, "ТБ") {
		t.Errorf("detail %q без устройства/TBW", s.Detail)
	}

	env.wear.wears = []NVMeWear{
		{Device: "nvme0", PercentUsed: 41},
		{Device: "nvme1", PercentUsed: 89}, // худший, но ниже порога
	}
	s = snapshotOf(t, env.Pass(context.Background()), AlertNVMeWear)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "nvme1") || !strings.Contains(s.Detail, "89%") {
		t.Errorf("89%%: статус = %s (%s), want ok по худшему nvme1", s.Status, s.Detail)
	}

	// Новый накопитель (0 %): стартовое значение — первое устройство,
	// деталь называет имя, а не пустую строку.
	env.wear.wears = []NVMeWear{{Device: "nvme0", PercentUsed: 0}}
	s = snapshotOf(t, env.Pass(context.Background()), AlertNVMeWear)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "nvme0") || !strings.Contains(s.Detail, "0%") {
		t.Errorf("0%%: статус = %s (%s), want ok с именем nvme0", s.Status, s.Detail)
	}

	// Пустой список (источник жив, устройств нет) — «не измеряется»,
	// не паника на первом элементе.
	env.wear.wears = nil
	s = snapshotOf(t, env.Pass(context.Background()), AlertNVMeWear)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "не найдены") {
		t.Errorf("без устройств: статус = %s (%s), want not_measured", s.Status, s.Detail)
	}

	// Равный износ двух устройств: худший — первое (замена строго на
	// «больше», не «не меньше»).
	env.wear.wears = []NVMeWear{
		{Device: "nvme0", PercentUsed: 50},
		{Device: "nvme1", PercentUsed: 50},
	}
	s = snapshotOf(t, env.Pass(context.Background()), AlertNVMeWear)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "nvme0: использовано 50%") {
		t.Errorf("равный износ: %s (%s), want ok по первому nvme0", s.Status, s.Detail)
	}
}

// TestAlertsNVMeDegradationQuiet — SMART недоступен (контейнер): «не
// измеряется» без Warn — ожидаемое состояние, не авария; переход — Debug.
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
	if !strings.Contains(env.buf.String(), "не измеряется") { // Debug-переход
		t.Errorf("переход в «не измеряется» не залогирован:\n%s", env.buf.String())
	}
}

// --- лог переходов, снапшот, дефолты ------------------------------------------

// TestAlertsTransitionsLogging — фронтовые записи: срабатывание — Warn
// «АЛЕРТ», снятие — Info, ошибка источника — Warn.
func TestAlertsTransitionsLogging(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()

	// Четыре прохода: срабатывание, устойчивое срабатывание (тихо),
	// снятие, устойчивое «ok» (тихо).
	env.pool.share = []alertShare{{temp: 500, total: 1000}, {temp: 500, total: 1000}, {temp: 0, total: 1000}, {temp: 0, total: 1000}}
	env.Pass(ctx) // срабатывание
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "АЛЕРТ: рост доли ошибок DNS") {
		t.Errorf("срабатывание без Warn-записи:\n%s", env.buf.String())
	}

	// Устойчивое срабатывание — тихо: сигнал фронтовой, повторный Warn
	// в том же состоянии не пишется.
	beforeHold := env.buf.String()
	env.Pass(ctx)
	if delta := env.buf.String()[len(beforeHold):]; strings.Contains(delta, "level=WARN") || strings.Contains(delta, "АЛЕРТ") {
		t.Errorf("устойчивое срабатывание повторило Warn:\n%s", delta)
	}

	env.Pass(ctx) // снятие
	if !strings.Contains(env.buf.String(), "level=INFO") || !strings.Contains(env.buf.String(), "алерт снят") {
		t.Errorf("снятие без Info-записи:\n%s", env.buf.String())
	}

	// Устойчивое состояние — тихо: повторный проход без смены статуса не
	// пишет Warn/Info заново (сигнал фронтовой); Debug-итог прохода —
	// не сигнал, ему можно.
	before := env.buf.String()
	env.Pass(ctx)
	if delta := env.buf.String()[len(before):]; strings.Contains(delta, "level=WARN") || strings.Contains(delta, "level=INFO") {
		t.Errorf("повторный тихий проход добавил Warn/Info:\n%s", delta)
	}

	// Уход сработавшего алерта в «не измеряется» (сбой источника):
	// потеря активной аварии фиксируется Info, не тихим Debug.
	// Перед сбоем алерт снова срабатывает — переход именно из firing.
	env.pool.share = []alertShare{{temp: 500, total: 1000}}
	env.Pass(ctx)
	if !strings.Contains(env.buf.String(), "АЛЕРТ: рост доли ошибок DNS") {
		t.Fatalf("повторное срабатывание не залогировано:\n%s", env.buf.String())
	}
	before2 := env.buf.String()
	env.pool.shareErrs = []error{errors.New("бд недоступна")}
	env.Pass(ctx)
	delta2 := env.buf.String()[len(before2):]
	if !strings.Contains(delta2, "сработавший алерт больше не измеряется") || !strings.Contains(delta2, "level=INFO") {
		t.Errorf("firing→not_measured без Info:\n%s", delta2)
	}
}

// TestAlertsOnPassSnapshot — хук получает снапшот всех шести алертов
// в фиксированном порядке; Firing() отбирает сработавшие.
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
	env.pool.share = []alertShare{{temp: 500, total: 1000}}

	res := env.Pass(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if len(passes) != 1 {
		t.Fatalf("OnPass вызовов = %d, want 1", len(passes))
	}
	wantNames := []string{AlertDNSErrors, AlertQueueOverflow, AlertProgressStall, AlertDiskFree, AlertNVMeWear, AlertBadHashes}
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

// TestAlertsOptionsDefaults — нулевые и отрицательные периоды — дефолты;
// единичные (1нс) — валидные значения, не затираются (граница «≤ 0»);
// NewAlerts подставляет реализации ОС для пустых Space/Wear и дефолтные
// часы.
func TestAlertsOptionsDefaults(t *testing.T) {
	o := (AlertsOptions{}).withDefaults()
	// Литерал 60s, а не DefaultAlertsInterval: сравнение с константой
	// мутируется вместе с ней (59/61 с должны ловиться здесь).
	if o.Interval != 60*time.Second {
		t.Errorf("Interval = %v, want 60s", o.Interval)
	}
	if o.ZoneStatsInterval != DefaultZoneStatsInterval {
		t.Errorf("ZoneStatsInterval = %v, want %v", o.ZoneStatsInterval, DefaultZoneStatsInterval)
	}

	neg := AlertsOptions{Interval: -time.Second, ZoneStatsInterval: -time.Second}.withDefaults()
	if neg.Interval != 60*time.Second || neg.ZoneStatsInterval != DefaultZoneStatsInterval {
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

// --- источники ОС ---------------------------------------------------------------

// TestOSStatfsSpace — реальный statfs временного каталога: ёмкость видна
// в байтах (умножение блоков на Bsize — деление давало бы доли мегабайта
// даже на многогигабайтных томах CI); несуществующий каталог — ошибка
// (не паника).
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

// TestParseHealthLog — смещения страницы health (layout uapi/linux/nvme.h
// struct nvme_smart_log): percentage_used — байт 5, data units written —
// байты 48–63 LE; короткий буфер — ошибка.
func TestParseHealthLog(t *testing.T) {
	page := make([]byte, nvmeHealthLogSize)
	page[5] = 42                                            // percentage_used
	binary.LittleEndian.PutUint64(page[48:56], 195_312_500) // ≈ 100 ТБ

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
	// ТБ ровно 100.0: произведение и деление точны в float64 (195312500 ×
	// 512000 = 1e14 < 2^53, частное 100 представимо точно) — мутации
	// коэффициентов (±1 на единицу записи, ±1 на триллион) ловятся
	// точным равенством.
	if tbw := nvmeTBW(NVMeWear{DataUnitsWritten: units}); tbw != 100 {
		t.Errorf("TBW = %v, want ровно 100 ТБ", tbw)
	}

	// Новый накопитель: нулевой износ декодируется как есть.
	zero := make([]byte, nvmeHealthLogSize)
	if percent, _, _ := parseHealthLog(zero); percent != 0 {
		t.Errorf("нулевой износ = %d, want 0", percent)
	}

	// Значения >100 % валидны (ресурс исчерпан) и не усекаются.
	over := make([]byte, nvmeHealthLogSize)
	over[5] = 253
	if percent, _, _ := parseHealthLog(over); percent != 253 {
		t.Errorf("износ >100%% = %d, want 253", percent)
	}

	// Размер страницы фиксирован протоколом — 512 байт: литералы (не
	// константа — она мутируется вместе с проверкой): 512 разбирается,
	// 511 — уже короткая страница.
	full := make([]byte, 512)
	full[5] = 7
	binary.LittleEndian.PutUint64(full[48:56], 9)
	if percent, u, err := parseHealthLog(full); err != nil || percent != 7 || u != 9 {
		t.Errorf("страница 512 байт: (%d, %d, %v), want (7, 9, nil)", percent, u, err)
	}
	if _, _, err := parseHealthLog(make([]byte, 511)); err == nil {
		t.Error("страница 511 байт: ошибки нет, want короткая страница")
	}

	// Короткий буфер — ошибка и нулевые значения возврата.
	pct, u, err := parseHealthLog(page[:nvmeHealthLogSize-1])
	if err == nil {
		t.Error("короткий буфер: ошибки нет")
	}
	if pct != 0 || u != 0 {
		t.Errorf("короткий буфер: (%d, %d), want нули при ошибке", pct, u)
	}
}

// TestOSWearDegradation — пустой sysfs и недоступный /dev-узел — ошибки
// деградации с причиной (контейнер), не паника и не пустой результат.
func TestOSWearDegradation(t *testing.T) {
	root := t.TempDir()

	// sysfs без class/nvme — NVMe нет вовсе.
	_, err := OSWear{SysFS: root, DevFS: root}.Wear(context.Background())
	if err == nil || !strings.Contains(err.Error(), "не обнаружены") {
		t.Errorf("пустой sysfs: ошибка = %v, want «не обнаружены»", err)
	}

	// sysfs видит контроллер (как в контейнере: sysfs хоста), но /dev-узел
	// не проброшен — SMART недоступен, причина называет устройство.
	if err := os.MkdirAll(filepath.Join(root, "class", "nvme", "nvme0"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = OSWear{SysFS: root, DevFS: filepath.Join(root, "dev")}.Wear(context.Background())
	if err == nil || !strings.Contains(err.Error(), "SMART недоступен") || !strings.Contains(err.Error(), "nvme0") {
		t.Errorf("недоступный /dev: ошибка = %v, want деградация с nvme0", err)
	}

	// Несколько контроллеров: обзор всех (ошибка одного не останавливает
	// цикл) — причина называет каждое устройство.
	if err := os.MkdirAll(filepath.Join(root, "class", "nvme", "nvme1"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = OSWear{SysFS: root, DevFS: filepath.Join(root, "dev")}.Wear(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nvme0") || !strings.Contains(err.Error(), "nvme1") {
		t.Errorf("два недоступных контроллера: ошибка = %v, want причина с nvme0 и nvme1", err)
	}

	// Отмена ctx — ошибка контекста до обращения к устройствам.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (OSWear{SysFS: root}).Wear(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("отмена ctx: ошибка = %v, want context.Canceled", err)
	}
}

// --- цикл AlertsLoop ------------------------------------------------------------

// TestAlertsLoopPasses — первый проход сразу при старте; ошибка источника
// не убивает цикл (второй проход по тику); отмена ctx — чистый nil.
func TestAlertsLoopPasses(t *testing.T) {
	var mu sync.Mutex
	passes := 0
	env := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.Interval = 2 * time.Millisecond
		o.OnPass = func(AlertsResult) { mu.Lock(); passes++; mu.Unlock() }
	})
	env.pool.shareErrs = []error{errors.New("бд недоступна")} // стартовый проход — ошибка

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

// TestAlertsLoopCancelDuringPass — отмена ctx во время прохода: без Warn
// (часть остановки), чистый nil — контракт OnRun (стиль ZoneStatsLoop).
func TestAlertsLoopCancelDuringPass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &alertsPool{shareErrs: []error{context.Canceled}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до старта: первый же запрос прохода провалится

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

// TestAlertsLoopFirstPassImmediate — первый проход выполняется сразу при
// старте, не ждёт тика: интервал цикла — час, единственный способ увидеть
// OnPass за секунды теста — стартовый проход.
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
	case <-passed: // стартовый проход — сразу
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

// TestAlertBytes — границы ступеней форматирования: ровно 1<<30/1<<20/1<<10
// попадают в старшую ступень (границы включающие), минус байт — в младшую;
// половинные и полуторные значения фиксируют делитель каждой ступени.
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

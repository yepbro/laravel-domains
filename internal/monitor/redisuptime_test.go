// Redis uptime source and redis_restart alert unit tests (T-270,
// FR-2.2): INFO server parsing (a table: sections/CRLF, missing and
// malformed fields), RedisInfo against a fake go-redis client (a nil
// client, a command error, the requested section), the restart window
// threshold (the boundary exactly at the window — no longer "recent"),
// degradations (the source not set / an error — "not measured").
// A live Redis is outside the unit tests.
package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeRedisUptime — uptime from a preset, an error queue; the
// fakeRedisMem style.
type fakeRedisUptime struct {
	errs []error
	up   RedisUptime
}

// compile-time: the fake is a RedisUptimeSource.
var _ RedisUptimeSource = (*fakeRedisUptime)(nil)

func (f *fakeRedisUptime) RedisUptime(context.Context) (RedisUptime, error) {
	if err := popErr(&f.errs); err != nil {
		return RedisUptime{}, err
	}
	return f.up, nil
}

// --- INFO parsing -------------------------------------------------------------

// TestParseRedisUptimeINFO — the "key:value" lines, section headers
// and CRLF; a missing field, a non-numeric and a negative seconds value
// are errors (the format is fixed by the server).
func TestParseRedisUptimeINFO(t *testing.T) {
	raw := "# Server\r\n" +
		"redis_version:7.0.15\r\n" +
		"redis_mode:standalone\r\n" +
		"os:Linux 6.6.0 x86_64\r\n" +
		"uptime_in_seconds:900\r\n" +
		"uptime_in_days:0\r\n"
	up, err := parseRedisUptimeINFO(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if up != (RedisUptime{Seconds: 900}) {
		t.Errorf("parse = %+v, want {900}", up)
	}

	// Without \r (unix output) — the same result.
	if up, err = parseRedisUptimeINFO(strings.ReplaceAll(raw, "\r", "")); err != nil || up.Seconds != 900 {
		t.Errorf("без CR: (%+v, %v), want ({900}, nil)", up, err)
	}

	for name, bad := range map[string]string{
		"поля нет":      "# Server\nredis_version:7.0.15\n",
		"не число":      "# Server\nuptime_in_seconds:сутки\n",
		"отрицательное": "# Server\nuptime_in_seconds:-1\n",
	} {
		if _, err := parseRedisUptimeINFO(bad); err == nil {
			t.Errorf("%s: parse прошёл, want ошибка", name)
		}
	}
}

// TestRedisInfoUptimeClient — RedisInfo reads the server section with
// the same client as memory (redismem_test.go): a nil client and a
// command error — source errors.
func TestRedisInfoUptimeClient(t *testing.T) {
	if _, err := (RedisInfo{}).RedisUptime(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "клиент не задан") {
		t.Errorf("nil-клиент: %v, want «клиент не задан»", err)
	}

	failing := &fakeInfoRedis{err: errors.New("connection reset")}
	if _, err := (RedisInfo{RDB: failing}).RedisUptime(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "INFO server") {
		t.Errorf("ошибка команды: %v, want контекст INFO server", err)
	}

	ok := &fakeInfoRedis{info: "# Server\nuptime_in_seconds:42\n"}
	up, err := (RedisInfo{RDB: ok}).RedisUptime(context.Background())
	if err != nil || up.Seconds != 42 {
		t.Fatalf("чтение = (%+v, %v), want ({42}, nil)", up, err)
	}
	if len(ok.sections) != 1 || ok.sections[0][0] != "server" {
		t.Errorf("секции запроса = %v, want [[server]]", ok.sections)
	}
}

// --- the redis_restart alert ------------------------------------------------------

// TestAlertsRedisRestartThreshold — the 15 min window: uptime below the
// window — a "recent" restart (firing), exactly at the window — already
// not (clearing by itself when leaving the window: the level is
// monotonic, no machine memory needed).
func TestAlertsRedisRestartThreshold(t *testing.T) {
	if AlertRedisRestartWindow != 15*time.Minute {
		t.Fatalf("AlertRedisRestartWindow = %v, want 15 мин (3 прохода алертов)", AlertRedisRestartWindow)
	}
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRedisRestartMachine()
	steps := []struct {
		uptime int64
		want   string
	}{
		{30, AlertStatusFiring},  // half a minute ago — a restart
		{899, AlertStatusFiring}, // 14 min 59 s — a restart
		{900, AlertStatusOK},     // exactly the window — no longer "recent"
		{3600, AlertStatusOK},    // an hour — the norm
		{5, AlertStatusFiring},   // a new restart reset the uptime
	}
	for i, s := range steps {
		got := m.transition(env.Alerts, ctx, obs(int64(s.uptime)))
		if got.Status != s.want {
			t.Errorf("шаг %d (uptime %d с): статус = %s (%s), want %s", i, s.uptime, got.Status, got.Detail, s.want)
		}
	}
	// The detail — the uptime and the stream loss warning.
	hot := m.transition(env.Alerts, ctx, obs(int64(30)))
	if hot.Status != AlertStatusFiring ||
		!strings.Contains(hot.Detail, "30s") ||
		!strings.Contains(hot.Detail, "T-270") {
		t.Errorf("detail = %q (%s), want firing с аптаймом и отсылкой к компенсатору", hot.Detail, hot.Status)
	}
}

// TestAlertsRedisRestartDegradation — without a source and on its error
// the alert is "not measured" (the source commands in cmd/monitor: a
// live Redis is absent in unit tests — the norm, not a false firing).
func TestAlertsRedisRestartDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil) // the source is not set (no wiring was done)
	if s := snapshotOf(t, env.Pass(context.Background()), AlertRedisRestart); s.Status != AlertStatusNotMeasured {
		t.Errorf("без источника: статус = %s, want not_measured", s.Status)
	}

	env.deps.RedisUptime = &fakeRedisUptime{errs: []error{errors.New("redis закрыт")}}
	s := snapshotOf(t, env.Pass(context.Background()), AlertRedisRestart)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "redis закрыт") {
		t.Errorf("ошибка источника: %+v, want not_measured с причиной", s)
	}

	env.deps.RedisUptime = &fakeRedisUptime{up: RedisUptime{Seconds: 86400}}
	if s := snapshotOf(t, env.Pass(context.Background()), AlertRedisRestart); s.Status != AlertStatusOK {
		t.Errorf("суточный аптайм: статус = %s, want ok", s.Status)
	}
}

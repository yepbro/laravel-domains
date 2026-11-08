// Юнит-тесты источника аптайма Redis и алерта redis_restart (T-270,
// ФТ-2.2): разбор INFO server (таблица: секции/CRLF, отсутствующие и
// кривые поля), RedisInfo против фейкового клиента go-redis (nil-клиент,
// ошибка команды, секция запроса), порог окна рестарта (граница ровно
// на окне — уже не «недавно»), деградации (источник не задан / ошибка —
// «не измеряется»). Живой Redis — за границами юнит-тестов.
package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeRedisUptime — аптайм по заготовке, очередь ошибок; стиль
// fakeRedisMem.
type fakeRedisUptime struct {
	errs []error
	up   RedisUptime
}

// compile-time: фейк — RedisUptimeSource.
var _ RedisUptimeSource = (*fakeRedisUptime)(nil)

func (f *fakeRedisUptime) RedisUptime(context.Context) (RedisUptime, error) {
	if err := popErr(&f.errs); err != nil {
		return RedisUptime{}, err
	}
	return f.up, nil
}

// --- разбор INFO -------------------------------------------------------------

// TestParseRedisUptimeINFO — строки «ключ:значение», заголовки секций
// и CRLF; отсутствующее поле, нечисловое и отрицательное секунды —
// ошибки (формат фиксирован сервером).
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

	// Без \r (юникс-вывод) — тот же результат.
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

// TestRedisInfoUptimeClient — RedisInfo читает секцию server тем же
// клиентом, что память (redismem_test.go): nil-клиент и ошибка команды
// — ошибки источника.
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

// --- алерт redis_restart ------------------------------------------------------

// TestAlertsRedisRestartThreshold — окно 15 мин: аптайп меньше окна —
// рестарт «недавно» (срабатывание), ровно на окне — уже нет (снятие сам
// выход из окна: уровень монотонен, память машины не нужна).
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
		{30, AlertStatusFiring},  // полминуты назад — рестарт
		{899, AlertStatusFiring}, // 14 мин 59 с — рестарт
		{900, AlertStatusOK},     // ровно окно — уже не «недавно»
		{3600, AlertStatusOK},    // час — норма
		{5, AlertStatusFiring},   // новый рестарт сбросил аптайм
	}
	for i, s := range steps {
		got := m.transition(env.Alerts, ctx, obs(int64(s.uptime)))
		if got.Status != s.want {
			t.Errorf("шаг %d (uptime %d с): статус = %s (%s), want %s", i, s.uptime, got.Status, got.Detail, s.want)
		}
	}
	// Detail — аптайм и предупреждение о потерях стримов.
	hot := m.transition(env.Alerts, ctx, obs(int64(30)))
	if hot.Status != AlertStatusFiring ||
		!strings.Contains(hot.Detail, "30s") ||
		!strings.Contains(hot.Detail, "T-270") {
		t.Errorf("detail = %q (%s), want firing с аптаймом и отсылкой к компенсатору", hot.Detail, hot.Status)
	}
}

// TestAlertsRedisRestartDegradation — без источника и при его ошибке
// алерт «не измеряется» (источник командует cmd/monitor: живой Redis в
// юнит-тестах отсутствует — норма, не ложное срабатывание).
func TestAlertsRedisRestartDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil) // источник не задан (не проводился wiring)
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

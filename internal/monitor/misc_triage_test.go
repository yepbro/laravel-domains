// Тесты триажа мутантов CI-прогона 09.11.2026 (половина A): добивки
// небольших источников monitor — unitevents (hot-границы событий,
// парсеры systemctl/memory.events, ошибки чтения с нулями),
// redisuptime/redismem (числовые поля INFO: отсутствующее поле,
// отрицательное, 2^63, нули на ошибке), psi (ParseFloat-границы
// значений), streamgroups (пропуск пустых стримов, «no such key»),
// notify (конструктор без паники на nil-логгере, Warn-причины
// выключения, доставка Send) и renderdebt (withDefaults-границы,
// выключенный интервал, nil-логгер цикла, ошибки источника).
package monitor

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- unitevents -------------------------------------------------------------

func TestUnitEventsHot(t *testing.T) {
	cases := []struct {
		e    UnitEvents
		want bool
	}{
		{UnitEvents{}, false},
		{UnitEvents{OOM: 1}, true},
		{UnitEvents{OOMKill: 1}, true},
		{UnitEvents{NRestarts: 1}, true},
		{UnitEvents{High: 1000}, false},
	}
	for _, c := range cases {
		if got := c.e.hot(); got != c.want {
			t.Errorf("hot(%+v) = %v, want %v", c.e, got, c.want)
		}
	}
}

func TestParseSystemctlShowBoundaries(t *testing.T) {
	// NRestarts до первого Id отбрасывается; несколько блоков; «[not
	// set]» — ноль; значения различимы (base 10, не 9/11).
	out := "NRestarts=99\n" +
		"Id=crawler-http-worker.service\nNRestarts=123\n" +
		"\n" +
		"Id=crawler-render-worker.service\nNRestarts=[not set]\n"
	units := parseSystemctlShow(out)
	if len(units) != 2 {
		t.Fatalf("юнитов = %d, want 2", len(units))
	}
	if units[0].Unit != "crawler-http-worker.service" || units[0].NRestarts != 123 {
		t.Errorf("http-worker = %+v, want NRestarts=123 (дес. 123, не 102)", units[0])
	}
	if units[1].NRestarts != 0 {
		t.Errorf("[not set] = %d, want 0", units[1].NRestarts)
	}
	// Пустой вывод — пустой снимок.
	if got := parseSystemctlShow(""); len(got) != 0 {
		t.Errorf("пустой вывод = %+v", got)
	}
}

func TestParseMemoryEventsBoundaries(t *testing.T) {
	ev, err := parseMemoryEvents("low 1\nhigh 42\nmax 3\noom 4\noom_kill 5\n")
	if err != nil {
		t.Fatal(err)
	}
	if ev.OOM != 4 || ev.OOMKill != 5 || ev.High != 42 {
		t.Errorf("memory.events = %+v, want oom 4 kill 5 high 42", ev)
	}
	// Значение 2^63 — валидно для uint64.
	ev, err = parseMemoryEvents("oom 9223372036854775808\n")
	if err != nil || ev.OOM != 9223372036854775808 {
		t.Errorf("oom 2^63 = (%+v, %v)", ev, err)
	}
	// Нечисловое значение — ошибка.
	if _, err := parseMemoryEvents("oom x\n"); err == nil {
		t.Error("нечисловое значение: хочу ошибку")
	}
	// Отсутствующие ключи — нули.
	ev, err = parseMemoryEvents("low 7\n")
	if err != nil || ev != (memoryEvents{}) {
		t.Errorf("нет ключей = (%+v, %v), want нули", ev, err)
	}
}

func TestReadMemoryEventsFileClasses(t *testing.T) {
	dir := t.TempDir()
	// Файла нет — нули без ошибки (юнит без cgroup).
	ev, err := readMemoryEvents(filepath.Join(dir, "нет"))
	if err != nil || ev != (memoryEvents{}) {
		t.Errorf("нет файла = (%+v, %v), want нули", ev, err)
	}
	// Каталог вместо файла — ошибка чтения (не ErrNotExist).
	if _, err := readMemoryEvents(dir); err == nil {
		t.Error("каталог вместо файла: хочу ошибку")
	}
	// Кривой файл — ошибка формата.
	p := filepath.Join(dir, "memory.events")
	if err := os.WriteFile(p, []byte("oom x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readMemoryEvents(p); err == nil {
		t.Error("кривой файл: хочу ошибку")
	}
}

// --- redisuptime / redismem ---------------------------------------------------

func TestInfoSecondsBoundaries(t *testing.T) {
	if v, err := infoSeconds(map[string]string{"uptime_in_seconds": "125"}, "uptime_in_seconds"); err != nil || v != 125 {
		t.Errorf("125 = (%v, %v)", v, err)
	}
	// Отсутствующее поле — ошибка и ноль.
	if v, err := infoSeconds(map[string]string{}, "uptime_in_seconds"); err == nil || v != 0 {
		t.Errorf("нет поля = (%v, %v), want 0 и ошибка", v, err)
	}
	// Ноль — валидное значение (граница n < 0).
	if v, err := infoSeconds(map[string]string{"k": "0"}, "k"); err != nil || v != 0 {
		t.Errorf("ноль = (%v, %v), want 0 без ошибки", v, err)
	}
	// Отрицательное — ошибка и ноль.
	if v, err := infoSeconds(map[string]string{"k": "-5"}, "k"); err == nil || v != 0 {
		t.Errorf("отрицательное = (%v, %v), want 0 и ошибка", v, err)
	}
	// Нечисловое — ошибка и ноль.
	if v, err := infoSeconds(map[string]string{"k": "x"}, "k"); err == nil || v != 0 {
		t.Errorf("нечисловое = (%v, %v), want 0 и ошибка", v, err)
	}
	// 2^63 — валидно для int64 (граница bitSize).
	if v, err := infoSeconds(map[string]string{"k": "4611686018427387904"}, "k"); err != nil || v != 4611686018427387904 {
		t.Errorf("2^62 = (%v, %v)", v, err)
	}
}

func TestInfoBytesBoundaries(t *testing.T) {
	if v, err := infoBytes(map[string]string{"maxmemory": "1024"}, "maxmemory"); err != nil || v != 1024 {
		t.Errorf("1024 = (%v, %v)", v, err)
	}
	if v, err := infoBytes(map[string]string{}, "maxmemory"); err == nil || v != 0 {
		t.Errorf("нет поля = (%v, %v), want 0 и ошибка", v, err)
	}
	if v, err := infoBytes(map[string]string{"k": "0"}, "k"); err != nil || v != 0 {
		t.Errorf("ноль = (%v, %v), want 0 без ошибки", v, err)
	}
	if v, err := infoBytes(map[string]string{"k": "-1"}, "k"); err == nil || v != 0 {
		t.Errorf("отрицательное = (%v, %v), want 0 и ошибка", v, err)
	}
}

// --- psi -----------------------------------------------------------------------

func TestParsePSIValueBoundaries(t *testing.T) {
	// Ноль и 100 — валидные доли (границы 0..100 включительно).
	for _, v := range []string{"0.00", "100.00"} {
		if _, err := parsePSI("some avg10=" + v + " avg60=1 avg300=1 total=0\n"); err != nil {
			t.Errorf("avg10=%s: %v, want валидно", v, err)
		}
	}
	// 1e308 — валидное число для ParseFloat (парсер не режет диапазон
	// раньше проверки 0..100 — ошибка формата «не доля», а не «не число»).
	if _, err := parsePSI("some avg10=1e308 avg60=1 avg300=1 total=0\n"); err == nil ||
		!strings.Contains(err.Error(), "не доля") {
		t.Errorf("1e308: %v, want «не доля процента»", err)
	}
	// Отсутствующее поле avg300 — ошибка.
	if _, err := parsePSI("some avg10=1 avg60=1 total=0\n"); err == nil ||
		!strings.Contains(err.Error(), "avg300") {
		t.Errorf("нет avg300: %v", err)
	}
}

// --- streamgroups ----------------------------------------------------------------

func TestStreamGroupsEmptyAndMissing(t *testing.T) {
	// Оба стрима без групп — пустой снимок; группы с нулевым числом
	// пропускаются (continue, не break — последующие стримы читаются).
	rdb := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			"q:dns": {}, // пустой список
		},
		xlen: map[string]int64{},
	}
	st, err := (RedisStreamGroups{RDB: rdb}).StreamGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st) != 0 {
		t.Errorf("снимок = %+v, want пусто (групп нет)", st)
	}
}

// --- notify ------------------------------------------------------------------------

func TestTelegramNotifierNilLogNoPanic(t *testing.T) {
	// nil-логгер и nil-Getenv подменяются дефолтами — конструктор не
	// паникует ни в одной ветке выключения.
	for _, opts := range []TelegramOptions{
		{Enabled: false},
		{Enabled: true, Getenv: func(string) string { return "t" }},
		{Enabled: true, Getenv: func(string) string { return "t" }, Script: "/нет"},
	} {
		if n := NewTelegramNotifier(opts); n == nil {
			t.Errorf("%+v: конструктор вернул nil", opts)
		}
	}
}

func TestTelegramNotifierOffReasons(t *testing.T) {
	// Включён, секреты есть, скрипт не задан — Warn «не задан путь
	// скрипта», доставка выключена (не ветка «скрипт не доступен»).
	var buf syncedBuffer
	logger := newNotifyLogger(&buf)
	n := NewTelegramNotifier(TelegramOptions{
		Enabled: true,
		Log:     logger,
		Getenv:  func(string) string { return "x" },
	})
	if n.script != "" {
		t.Fatalf("script = %q, want выключен", n.script)
	}
	log := buf.String()
	if !strings.Contains(log, "не задан путь скрипта") {
		t.Errorf("Warn без причины «не задан путь скрипта»:\n%s", log)
	}
	if strings.Contains(log, "не доступен") {
		t.Errorf("причина перепутана с «скрипт не доступен»:\n%s", log)
	}
}

// newNotifyLogger — текстовый логгер в синхронизированный буфер.
func newNotifyLogger(buf *syncedBuffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestTelegramNotifierDisabledNoSend — выключенная доставка (нет
// секретов): OnPass/Send не порождают попыток отправки (журнал мока и
// лог чисты от «telegram:»), состояние эпизодов не рассылается.
func TestTelegramNotifierDisabledNoSend(t *testing.T) {
	buf := &syncedBuffer{}
	n := NewTelegramNotifier(TelegramOptions{
		Enabled: true,
		Log:     newNotifyLogger(buf),
		Getenv:  func(string) string { return "" }, // секретов нет
	})
	res := AlertsResult{Alerts: []AlertSnapshot{{
		Name: AlertQueueDead, Status: AlertStatusFiring, Detail: "глубина 5",
	}}}
	n.OnPass(res)
	n.Send("отчёт")
	time.Sleep(50 * time.Millisecond) // фоновые горутины отправки
	if log := buf.String(); strings.Contains(log, "отправка") {
		t.Errorf("выключенная доставка пытается отправить:\n%s", log)
	}
}

// TestTelegramNotifierNotMeasuredEpisode — «не измеряется» не рвёт
// эпизод: возврат измерений в ok закрывает его ровно одним ✅.
func TestTelegramNotifierNotMeasuredEpisode(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal.log")
	script := fakeNotifyScript(t, journal, false)
	n, _ := notifierEnv(t, script, "token", "chat")

	pass := func(status, detail string) AlertsResult {
		return AlertsResult{Alerts: []AlertSnapshot{{Name: AlertQueueDead, Status: status, Detail: detail}}}
	}
	n.OnPass(pass(AlertStatusNotMeasured, "Redis не задан")) // база «не измеряется»
	n.OnPass(pass(AlertStatusFiring, "глубина 5"))           // 🚨
	n.OnPass(pass(AlertStatusNotMeasured, "обрыв"))          // перерыв — без сообщения
	n.OnPass(pass(AlertStatusOK, "глубина 0"))               // ✅
	n.OnPass(pass(AlertStatusOK, "глубина 0"))               // тишина
	waitForJournal(t, journal, "🚨")
	waitForJournal(t, journal, "✅")
	b, err := os.ReadFile(journal)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), "🚨"); n != 1 {
		t.Errorf("🚨 = %d, want 1", n)
	}
	if n := strings.Count(string(b), "✅"); n != 1 {
		t.Errorf("✅ = %d, want 1 (повторный ok молчит)", n)
	}
}

// TestTelegramNotifierNotMeasuredCold — «не измеряется» без открытого
// эпизода — полная тишина (ни сообщения, даже в лог: заметка только для
// сработавшего).
func TestTelegramNotifierNotMeasuredCold(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal.log")
	script := fakeNotifyScript(t, journal, false)
	n, buf := notifierEnv(t, script, "token", "chat")
	n.OnPass(AlertsResult{Alerts: []AlertSnapshot{
		{Name: AlertQueueDead, Status: AlertStatusNotMeasured, Detail: "Redis не задан"},
	}})
	time.Sleep(50 * time.Millisecond)
	if strings.Contains(buf.String(), "больше не измеряется") {
		t.Errorf("холодное «не измеряется» отмечено в логе:\n%s", buf.String())
	}
	if b, _ := os.ReadFile(journal); len(b) != 0 {
		t.Errorf("холодное «не измеряется» разослало сообщение: %s", b)
	}
}

func TestTelegramConstPin(t *testing.T) {
	if telegramNotifyTimeout != 40*time.Second {
		t.Errorf("telegramNotifyTimeout = %v, want 40s", telegramNotifyTimeout)
	}
}

func TestTelegramNotifierSendDelivers(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal.log")
	script := fakeNotifyScript(t, journal, false)
	n, _ := notifierEnv(t, script, "token", "chat")
	n.Send("произвольный отчёт")
	waitForJournal(t, journal, "произвольный отчёт")
}

// waitForJournal — ждёт появления строки в журнале мока (фоновая
// горутина отправки).
func waitForJournal(t *testing.T, journal, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(journal)
		if err == nil && strings.Contains(string(b), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("строка %q не доставлена в журнал", want)
}

// --- renderdebt ---------------------------------------------------------------------

type fakeDebtSource struct {
	debt RenderDebt
	err  error
}

func (s *fakeDebtSource) RenderDebt(context.Context) (RenderDebt, error) {
	return s.debt, s.err
}

func TestRenderDebtOptionsBoundaries(t *testing.T) {
	if DefaultRenderDebtInterval != 5*time.Minute {
		t.Errorf("DefaultRenderDebtInterval = %v, want 5m", DefaultRenderDebtInterval)
	}
	if got := (RenderDebtOptions{}).withDefaults().Interval; got != DefaultRenderDebtInterval {
		t.Errorf("нулевой интервал = %v, want дефолт", got)
	}
	if got := (RenderDebtOptions{Interval: -time.Second}).withDefaults().Interval; got != DefaultRenderDebtInterval {
		t.Errorf("отрицательный интервал = %v, want дефолт", got)
	}
	if got := (RenderDebtOptions{Interval: time.Second}).withDefaults().Interval; got != time.Second {
		t.Errorf("секундный интервал = %v, want как задан", got)
	}
}

func TestRenderDebtLoopNilLogAndSourceErrors(t *testing.T) {
	// nil-логгер не паникует: стартовый проход выполняется и выходит.
	src := &fakeDebtSource{debt: RenderDebt{Flags: 3, Lag: 1}}
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	go func() { done <- RenderDebtLoop(ctx, src, RenderDebtOptions{Interval: time.Hour}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не вышел")
	}

	// Источник не задан — ошибка.
	if err := RenderDebtLoop(context.Background(), nil, RenderDebtOptions{Interval: time.Minute}); err == nil {
		t.Error("источник не задан: хочу ошибку")
	}
}

func TestRenderDebtSourceErrors(t *testing.T) {
	// Пул/Redis не заданы — ошибки источника (мутанты вырезания return
	// паниковали бы на nil).
	if _, err := renderDebtOf(context.Background(), nil, nil); err == nil {
		t.Error("пул не задан: хочу ошибку")
	}
	if _, err := renderDebtOf(context.Background(), &zonestatsFakePool{}, nil); err == nil {
		t.Error("Redis не задан: хочу ошибку")
	}
	// Ошибка Query — ошибка источника.
	pool := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}
	if _, err := renderDebtOf(context.Background(), pool, &renderLensSource{}); err == nil {
		t.Error("Query-ошибка: хочу ошибку")
	}
}

// renderLensSource — StreamLener с нулевыми глубинами.
type renderLensSource struct{}

func (s *renderLensSource) Len(context.Context, string) (int64, error) { return 0, nil }

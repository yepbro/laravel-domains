// Юнит-тесты алертов очереди фолбэка рендера (T-246, ТЗ рендера §4.6
// п.3): рост глубины q:render-fallback (порог — эффективный watermark
// стрима, механика render_debt: уровень + серия роста/у порога +
// защёлка), превышение бюджета доли (окно дельт счётчика событий
// suspended{reason="budget"} зонда /metrics рендер-воркера, suspends.go
// — парсер и деградации) и рост browser-ошибок этапа (S-P2-6,
// rate-детектор по счётчику
// crawler_stage_errors_total{stage="render",type="browser"} того же
// зонда). Фейковые источники, стиль renderdebt/badhashes тестов.
package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"crawler/internal/queue"
)

// fakeBudgetEvents — источник счётчика событий бюджета доли по заготовке
// (стиль fakeBadHashes): значение меняется между проходами, очередь
// ошибок пустая — ответ без ошибки.
type fakeBudgetEvents struct {
	mu    sync.Mutex
	val   int64
	errs  []error
	calls int
}

func (f *fakeBudgetEvents) set(v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.val = v
}

func (f *fakeBudgetEvents) FallbackBudget(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := popErr(&f.errs); err != nil {
		return 0, err
	}
	return f.val, nil
}

// --- рост глубины q:render-fallback -------------------------------------------

// setFallbackDepth — заготовка глубины прохода: Len(q:render-fallback)
// читает карту ленера (одинаково для всех последующих проходов, тест
// задаёт значение на шаг).
func setFallbackDepth(env *alertsTestEnv, depth int64) {
	env.lener.lens[queue.StreamRenderFallback] = depth
}

// TestAlertsRenderFallbackDepthGrowth — срабатывание по росту: глубина
// растёт 2 прохода подряд, пересекая порог; удержание стагнацией
// (защёлка), снятие дренажем ниже 90 % порога; разовый всплеск публикаций
// (скачок, рассосавшийся до следующего прохода) оператора не будит.
// Стационарная глубина у порога — второй триггер, отдельный тест ниже
// (TestAlertsRenderFallbackDepthStationary).
func TestAlertsRenderFallbackDepthGrowth(t *testing.T) {
	// Порог 1000 (watermark из дефолта тестового окружения; предел
	// MAXLEN стрима выше — без клампа), снятие < 900.
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRenderFbDepthMachine()

	// База и рост ниже порога: серия роста копится, порог ещё не
	// задействован.
	s := m.transition(env.Alerts, ctx, obs(int64(500)))
	if s.Status != AlertStatusOK {
		t.Fatalf("база: %s (%s), want ok", s.Status, s.Detail)
	}
	for _, want := range []string{"q:render-fallback=500", "порог 1000", "снятие < 900", "у порога 0/2"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// Один проход роста — серии мало (разовый всплеск публикаций —
	// штатная работа фолбэка «собрать и доработать»).
	s = m.transition(env.Alerts, ctx, obs(int64(800)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Fatalf("один проход роста: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	// Второй проход роста с пересечением порога — срабатывание.
	s = m.transition(env.Alerts, ctx, obs(int64(1420)))
	if s.Status != AlertStatusFiring {
		t.Fatalf("второй проход роста: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"q:render-fallback=1420", "+620", "воркера B"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// Стагнация выше порога снятия — удержание (не повторный Warn,
	// эпизод один).
	if s := m.transition(env.Alerts, ctx, obs(int64(1410))); s.Status != AlertStatusFiring {
		t.Errorf("стагнация: %s (%s), want firing (удержание)", s.Status, s.Detail)
	}
	// Дренаж ниже 90 % порога — снятие.
	if s := m.transition(env.Alerts, ctx, obs(int64(890))); s.Status != AlertStatusOK {
		t.Errorf("дренаж: %s (%s), want ok", s.Status, s.Detail)
	}
	// Коридор гистерезиса (900..1000) без сработавшего алерта — ok:
	// порог срабатывания строгий, рост в коридоре серию копит.
	if s := m.transition(env.Alerts, ctx, obs(int64(950))); s.Status != AlertStatusOK {
		t.Errorf("коридор без алерта: %s (%s), want ok", s.Status, s.Detail)
	}
	// Просадка сбрасывает серию; повторное накопление — снова два
	// прохода роста (рост через порог — то же накопление).
	_ = m.transition(env.Alerts, ctx, obs(int64(800))) // streak 0
	s = m.transition(env.Alerts, ctx, obs(int64(1010)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Errorf("первый проход повторного роста: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, obs(int64(1060))); s.Status != AlertStatusFiring {
		t.Errorf("второй проход повторного роста: %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsRenderFallbackDepthStationary — второй триггер (S-P2-1,
// решение оператора): глубина ≥ порога 2 прохода подряд БЕЗ роста —
// firing со 2-го прохода (серия роста базируется нулём и стационарную
// глубину не видит — рестарт монитора посреди эпизода больше не глушит
// алерт); упор в потолок MAXLEN — firing и удержание (XADD вытесняет
// старейшие недоставленные записи, глубина стоит); одиночный всплеск,
// рассосавшийся между проходами, — тихо (анти-спайк); защёлка/снятие
// прежние.
func TestAlertsRenderFallbackDepthStationary(t *testing.T) {
	// Порог — кламп по половине MAXLEN q:render-fallback (15 000/2 =
	// 7 500), как на дефолтных watermark 100 тыс.
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 100000 })
	ctx := context.Background()
	m := env.newRenderFbDepthMachine()

	// Стационарная глубина 9000 без роста: первый проход — база
	// (у порога 1/2), второй — firing.
	s := m.transition(env.Alerts, ctx, obs(int64(9000)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "у порога 1/2") {
		t.Fatalf("стационарная база: %s (%s), want ok с серией у порога 1/2", s.Status, s.Detail)
	}
	s = m.transition(env.Alerts, ctx, obs(int64(9000)))
	if s.Status != AlertStatusFiring {
		t.Fatalf("стационарная 9000: %s (%s), want firing со 2-го прохода", s.Status, s.Detail)
	}
	for _, want := range []string{"q:render-fallback=9000", "порог 7500", "снятие < 6750", "серия роста 0/2", "у порога 2/2", "стационарно"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}

	// Упор в потолок MAXLEN: глубина не растёт (обрезка вытесняет
	// недоставленное) — firing и удержание.
	if s := m.transition(env.Alerts, ctx, obs(int64(15000))); s.Status != AlertStatusFiring {
		t.Errorf("упор в MAXLEN: %s (%s), want firing", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, obs(int64(15000))); s.Status != AlertStatusFiring {
		t.Errorf("упор в MAXLEN, второй проход: %s (%s), want firing (удержание)", s.Status, s.Detail)
	}

	// Дренаж ниже 90 % порога — снятие (защёлка не вечна).
	if s := m.transition(env.Alerts, ctx, obs(int64(6000))); s.Status != AlertStatusOK {
		t.Errorf("дренаж: %s (%s), want ok", s.Status, s.Detail)
	}

	// Одиночный всплеск между проходами (рассасывается) — тихо: серия
	// у порога не набирается, рост одного прохода — не триггер.
	_ = m.transition(env.Alerts, ctx, obs(int64(9000))) // у порога 1/2, ok
	_ = m.transition(env.Alerts, ctx, obs(int64(100)))  // рассосался, ok
	s = m.transition(env.Alerts, ctx, obs(int64(9000)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "у порога 1/2") {
		t.Errorf("одиночный всплеск: %s (%s), want ok с серией у порога 1/2 (анти-спайк)", s.Status, s.Detail)
	}
}

// TestAlertsRenderFallbackDepthWatermark — порог из конфигурации по
// образцу q:render: эффективный watermark стрима
// queue.StreamWatermark(scheduler.queue_high_watermark, redis.maxlen,
// q:render-fallback) — кламп по половине предела MAXLEN (политика T-025:
// q:render-fallback — 15 000 → 7 500 на дефолтных 100 тыс.).
func TestAlertsRenderFallbackDepthWatermark(t *testing.T) {
	cases := []struct {
		name      string
		watermark int64
		maxLen    int64
		want      int64
	}{
		{"дефолт конфигурации: кламп по MAXLEN/2", 100000, 0, 7500},
		{"заниженный watermark ниже предела", 5000, 0, 5000},
		{"нестандартный MaxLen: кламп по его половине", 100000, 300, 150},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newAlertsTestEnv(t, func(o *AlertsOptions) {
				o.QueueWatermark = tc.watermark
				o.QueueMaxLen = tc.maxLen
			})
			ctx := context.Background()
			m := env.newRenderFbDepthMachine()
			// Серия роста двумя проходами на глубине ровно порога.
			_ = m.transition(env.Alerts, ctx, obs(tc.want+10)) // база
			_ = m.transition(env.Alerts, ctx, obs(tc.want+20)) // рост 1
			s := m.transition(env.Alerts, ctx, obs(tc.want+30))
			if s.Status != AlertStatusFiring {
				t.Fatalf("глубина %d > порога %d: %s (%s), want firing",
					tc.want+30, tc.want, s.Status, s.Detail)
			}
			if got := queue.StreamWatermark(tc.watermark, tc.maxLen, queue.StreamRenderFallback); got != tc.want {
				t.Errorf("StreamWatermark = %d, want %d (порог алерта — единый расчёт)", got, tc.want)
			}
			if !strings.Contains(s.Detail, "порог ") || !strings.Contains(s.Detail, "серия роста 2/2") {
				t.Errorf("detail = %q, want порог и готовую серию роста", s.Detail)
			}
		})
	}
	// Глубина ровно порога: одиночный проход — не firing (анти-спайк:
	// всплеск обязан пережить второй проход), стационарно — firing со
	// второго прохода (S-P2-1: равенство — уже «у порога», серия у
	// порога без роста — полноценный триггер); рост с пересечением
	// порога — срабатывание.
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 1000 })
	ctx := context.Background()
	m := env.newRenderFbDepthMachine()
	_ = m.transition(env.Alerts, ctx, obs(int64(1000))) // база (у порога 1/2)
	if s := m.transition(env.Alerts, ctx, obs(int64(1000))); s.Status != AlertStatusFiring {
		t.Errorf("глубина == порогу без роста два прохода: %s (%s), want firing (стационарно у порога, S-P2-1)", s.Status, s.Detail)
	}
	_ = m.transition(env.Alerts, ctx, obs(int64(1001))) // рост 1 (удержание)
	if s := m.transition(env.Alerts, ctx, obs(int64(1002))); s.Status != AlertStatusFiring {
		t.Errorf("рост через порог: %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsRenderFallbackDepthDegradation — Redis не задан, порог не
// задан, ошибка Len — «не измеряется» (ошибка — с Warn); память трекинга
// и защёлка сработавшего эпизода переживают сбой источника (стиль
// render_debt).
func TestAlertsRenderFallbackDepthDegradation(t *testing.T) {
	// Redis не задан — как у алертов очередей.
	env := newAlertsTestEnv(t, nil)
	a := NewAlerts(AlertsDeps{Pool: env.pool}, env.opts)
	if s := snapshotOf(t, a.Pass(context.Background()), AlertRenderFbDepth); s.Status != AlertStatusNotMeasured {
		t.Errorf("Redis не задан: %s, want not_measured", s.Status)
	}

	// Порог не задан (гейт выключен) — не измеряется.
	env2 := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 0 })
	if s := snapshotOf(t, env2.Pass(context.Background()), AlertRenderFbDepth); s.Status != AlertStatusNotMeasured {
		t.Errorf("watermark 0: %s, want not_measured", s.Status)
	}

	// Ошибка Len — Warn; очередь ошибок: до этого алерта Len зовут
	// 6 раз (4 стрима алерта очередей + q:dead + q:render рендер-долга
	// — он раньше по порядку машин NewAlerts), ошибка — седьмая.
	env3 := newAlertsTestEnv(t, nil)
	env3.lener.errs = []error{nil, nil, nil, nil, nil, nil, errors.New("redis: xinfo")}
	s := snapshotOf(t, env3.Pass(context.Background()), AlertRenderFbDepth)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "redis: xinfo") {
		t.Errorf("ошибка Len: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env3.buf.String(), "level=WARN") {
		t.Errorf("ошибка Len без Warn:\n%s", env3.buf.String())
	}

	// Память трекинга переживает сбой: рост считается от последнего
	// успешного снимка; защёлка сработавшего эпизода сбой не рвёт.
	env4 := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	setFallbackDepth(env4, 1200)
	_ = snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth) // база
	setFallbackDepth(env4, 1300)
	_ = snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth) // рост
	setFallbackDepth(env4, 1400)
	if s := snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
		t.Fatalf("до сбоя: %s, want firing", s.Status)
	}
	env4.lener.errs = []error{nil, nil, nil, nil, nil, nil, errors.New("redis: xinfo")}
	if s := snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusNotMeasured {
		t.Fatalf("сбой источника: %s, want not_measured", s.Status)
	}
	setFallbackDepth(env4, 1350) // выше порога снятия 900
	if s := snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
		t.Errorf("после сбоя, глубина 1350: %s, want firing (эпизод пережил сбой)", s.Status)
	}
	setFallbackDepth(env4, 100)
	if s := snapshotOf(t, env4.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusOK {
		t.Errorf("дренаж после сбоя: %s, want ok", s.Status)
	}
}

// --- превышение бюджета доли ---------------------------------------------------

// TestAlertsRenderFallbackBudgetEpisode — событие приостановки (рост
// счётчика) — срабатывание; окно держит алерт до старения дельты
// (6 проходов), поток событий продлевает; рестарт воркера (убыль
// счётчика) — без ложного всплеска.
func TestAlertsRenderFallbackBudgetEpisode(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRenderFbBudgetMachine()

	// База: накопленное до старта мониторинга не событие.
	s := m.transition(env.Alerts, ctx, obs(int64(3)))
	if s.Status != AlertStatusOK {
		t.Fatalf("база: %s (%s), want ok", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "события бюджета 3") {
		t.Errorf("detail = %q, want значение счётчика", s.Detail)
	}
	// Событие — срабатывание (порог 1: каждое защёлкивание — аномалия).
	s = m.transition(env.Alerts, ctx, obs(int64(4)))
	if s.Status != AlertStatusFiring {
		t.Fatalf("событие: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"новых за окно", "1 (порог 1)", "share_percent"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// Окно 6 проходов держит алерт старением: 5 проходов без событий —
	// ещё держит, шестой вытесняет дельту — снятие.
	for range 5 {
		if got := m.transition(env.Alerts, ctx, obs(int64(4))); got.Status != AlertStatusFiring {
			t.Fatalf("окно держит событие: %s (%s), want firing", got.Status, got.Detail)
		}
	}
	if got := m.transition(env.Alerts, ctx, obs(int64(4))); got.Status != AlertStatusOK {
		t.Fatalf("после старения окна: %s (%s), want ok", got.Status, got.Detail)
	}
	// Рестарт воркера (счётчик обнулился) — убыль не всплеск.
	if got := m.transition(env.Alerts, ctx, obs(int64(0))); got.Status != AlertStatusOK {
		t.Errorf("рестарт воркера: %s (%s), want ok без всплеска", got.Status, got.Detail)
	}
	// Поток событий продлевает алерт: событие в каждом проходе.
	for i := int64(1); i <= 3; i++ {
		if got := m.transition(env.Alerts, ctx, obs(i)); got.Status != AlertStatusFiring {
			t.Fatalf("поток событий, проход %d: %s, want firing", i, got.Status)
		}
	}
}

// TestAlertsRenderFallbackBudgetPrecreatedSeries — S-P1-4 (мониторная
// часть) поверх предсозданных серий T-250: первое событие бюджета 0→1 —
// firing (первый проход зонда читает 0 и базирует prev нулём — событие
// больше не теряется); рестарт воркера (счётчик процессный, обнулился) —
// повторное первое событие снова firing, убыль — без ложного всплеска.
func TestAlertsRenderFallbackBudgetPrecreatedSeries(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRenderFbBudgetMachine()

	// Предсозданная серия: зонд читает 0 (не «не измеряется»), prev
	// базируется нулём.
	if s := m.transition(env.Alerts, ctx, obs(int64(0))); s.Status != AlertStatusOK {
		t.Fatalf("нулевая серия: %s (%s), want ok (предсоздание T-250)", s.Status, s.Detail)
	}
	// Первое событие 0→1 — firing: дельта видна, окно её держит.
	if s := m.transition(env.Alerts, ctx, obs(int64(1))); s.Status != AlertStatusFiring {
		t.Fatalf("первое событие 0→1: %s (%s), want firing", s.Status, s.Detail)
	}
	// Окно стареет: 5 проходов держат событие, шестой вытесняет — снятие.
	for range 5 {
		if got := m.transition(env.Alerts, ctx, obs(int64(1))); got.Status != AlertStatusFiring {
			t.Fatalf("окно держит событие: %s (%s), want firing", got.Status, got.Detail)
		}
	}
	if got := m.transition(env.Alerts, ctx, obs(int64(1))); got.Status != AlertStatusOK {
		t.Fatalf("после старения окна: %s (%s), want ok", got.Status, got.Detail)
	}
	// Рестарт воркера: счётчик процессный — обнулился; убыль не всплеск.
	if got := m.transition(env.Alerts, ctx, obs(int64(0))); got.Status != AlertStatusOK {
		t.Fatalf("рестарт воркера: %s (%s), want ok без всплеска", got.Status, got.Detail)
	}
	// Повторное первое событие (0→1 нового процесса) — снова firing.
	if got := m.transition(env.Alerts, ctx, obs(int64(1))); got.Status != AlertStatusFiring {
		t.Fatalf("первое событие после рестарта: %s (%s), want firing", got.Status, got.Detail)
	}
}

// TestAlertsRenderFallbackBudgetDegradation — источник не задан — тихое
// «не измеряется» (адрес не настроен), ошибка зонда — «не измеряется»
// с Warn (метрики нет в экспозиции — ошибка разбора, тоже Warn).
func TestAlertsRenderFallbackBudgetDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	s := snapshotOf(t, env.Pass(context.Background()), AlertRenderFbBudget)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("источник не задан: %s, want not_measured", s.Status)
	}
	if !strings.Contains(s.Detail, "monitor.render_metrics_url") {
		t.Errorf("detail = %q, want подсказка адреса", s.Detail)
	}
	if strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("источник не задан дал Warn:\n%s", env.buf.String())
	}

	env2 := newAlertsTestEnv(t, nil)
	src := &fakeBudgetEvents{errs: []error{errors.New("connection refused")}}
	env2.deps.FallbackBudget = src
	s = snapshotOf(t, env2.Pass(context.Background()), AlertRenderFbBudget)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "connection refused") {
		t.Errorf("ошибка зонда: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env2.buf.String(), "level=WARN") {
		t.Errorf("ошибка зонда без Warn:\n%s", env2.buf.String())
	}

	// Очередь ошибок опустела — следующий проход живого зонда работает
	// (успешный путь observeRenderFallbackBudget полным стеком): база
	// кольца без всплеска.
	src.set(5)
	if s := snapshotOf(t, env2.Pass(context.Background()), AlertRenderFbBudget); s.Status != AlertStatusOK ||
		!strings.Contains(s.Detail, "события бюджета 5") {
		t.Errorf("живой зонд после ошибки: %s (%s), want ok с базой кольца", s.Status, s.Detail)
	}
}

// --- browser-ошибки этапа рендера (S-P2-6) --------------------------------------

// fakeBrowserErrors — источник счётчика browser-ошибок по заготовке
// (стиль fakeBudgetEvents).
type fakeBrowserErrors struct {
	mu   sync.Mutex
	val  int64
	errs []error
}

func (f *fakeBrowserErrors) RenderBrowserErrors(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := popErr(&f.errs); err != nil {
		return 0, err
	}
	return f.val, nil
}

// TestAlertsRenderBrowserErrors — rate-детектор (S-P2-6): серия
// browser-ошибок (рост счётчика 2 прохода подряд) — firing; одиночная
// ошибка — тихо; счётчик перестал расти — снятие (алерт события, не
// состояния); рестарт воркера (убыль) — без всплеска, новое накопление
// с нуля.
func TestAlertsRenderBrowserErrors(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newRenderBrowserErrMachine()

	// База: ленивое семейство — 0 у живого воркера без browser-ошибок.
	s := m.transition(env.Alerts, ctx, obs(int64(0)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "browser-ошибки рендера 0") {
		t.Fatalf("база: %s (%s), want ok с нулём", s.Status, s.Detail)
	}
	// Одиночная ошибка — серия 1/2, тихо (штатный перезапуск сессии).
	s = m.transition(env.Alerts, ctx, obs(int64(1)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Fatalf("одиночная: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	// Серия — второй растущий проход: firing.
	s = m.transition(env.Alerts, ctx, obs(int64(3)))
	if s.Status != AlertStatusFiring {
		t.Fatalf("серия: %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"browser-ошибки рендера 3", "+2", "движок"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail = %q, want содержит %s", s.Detail, want)
		}
	}
	// Счётчик стоит — серия сброшена, алерт снят (восстановление движка
	// или удержание гейта: новых ошибок нет).
	if s := m.transition(env.Alerts, ctx, obs(int64(3))); s.Status != AlertStatusOK {
		t.Errorf("стационарно: %s (%s), want ok", s.Status, s.Detail)
	}
	// Рестарт воркера (счётчик процессный, обнулился) — убыль не всплеск.
	if s := m.transition(env.Alerts, ctx, obs(int64(0))); s.Status != AlertStatusOK {
		t.Errorf("рестарт воркера: %s (%s), want ok без всплеска", s.Status, s.Detail)
	}
	// Новое накопление — снова два растущих прохода.
	s = m.transition(env.Alerts, ctx, obs(int64(2)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Errorf("первый проход новой серии: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	if s := m.transition(env.Alerts, ctx, obs(int64(5))); s.Status != AlertStatusFiring {
		t.Errorf("второй проход новой серии: %s (%s), want firing", s.Status, s.Detail)
	}
}

// TestAlertsRenderBrowserErrorsDegradation — источник не задан — тихое
// «не измеряется» с подсказкой адреса; чужая экспозиция (адрес настроен
// не на рендер-воркер) — тихое «не измеряется» без Warn (S-P2-6:
// конфигурация, не авария зонда); сетевая ошибка — «не измеряется»
// с Warn.
func TestAlertsRenderBrowserErrorsDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	s := snapshotOf(t, env.Pass(context.Background()), AlertRenderBrowserErr)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "monitor.render_metrics_url") {
		t.Fatalf("источник не задан: %s (%s), want not_measured с подсказкой", s.Status, s.Detail)
	}
	if strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("источник не задан дал Warn:\n%s", env.buf.String())
	}

	env2 := newAlertsTestEnv(t, nil)
	env2.deps.RenderBrowserErrors = &fakeBrowserErrors{errs: []error{errNotRenderExposition}}
	s = snapshotOf(t, env2.Pass(context.Background()), AlertRenderBrowserErr)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "не рендер-воркера") {
		t.Fatalf("чужая экспозиция: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if strings.Contains(env2.buf.String(), "level=WARN") {
		t.Errorf("чужая экспозиция дала Warn:\n%s", env2.buf.String())
	}

	env3 := newAlertsTestEnv(t, nil)
	src := &fakeBrowserErrors{errs: []error{errors.New("connection refused")}}
	env3.deps.RenderBrowserErrors = src
	s = snapshotOf(t, env3.Pass(context.Background()), AlertRenderBrowserErr)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "connection refused") {
		t.Fatalf("сетевая ошибка: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env3.buf.String(), "level=WARN") {
		t.Errorf("сетевая ошибка без Warn:\n%s", env3.buf.String())
	}

	// Очередь ошибок опустела — живой зонд читает ленивую серию (успех
	// observeRenderBrowserErrors полным стеком): 0 без ошибок — ok.
	if s := snapshotOf(t, env3.Pass(context.Background()), AlertRenderBrowserErr); s.Status != AlertStatusOK ||
		!strings.Contains(s.Detail, "browser-ошибки рендера 0") {
		t.Errorf("живой зонд после ошибки: %s (%s), want ok с нулём", s.Status, s.Detail)
	}
}

// TestParseBrowserErrorsExposition — табличный разбор ветки
// stage="render",type="browser": соседние классы (task/context) и чужой
// stage не мешают; ленивое семейство (серии нет у рендер-воркера) — 0;
// чужая экспозиция без маркера — errNotRenderExposition.
func TestParseBrowserErrorsExposition(t *testing.T) {
	cases := []struct {
		name    string
		expo    string
		want    int64
		wantErr string
	}{
		{"ветка browser", `crawler_stage_errors_total{stage="render",type="browser"} 7`, 7, ""},
		{"соседние классы этапа не мешают",
			"crawler_stage_errors_total{stage=\"render\",type=\"task\"} 3\n" +
				"crawler_stage_errors_total{stage=\"render\",type=\"context\"} 2\n" +
				"crawler_stage_errors_total{stage=\"render\",type=\"browser\"} 1\n", 1, ""},
		{"чужой stage (http-воркер) — чужая экспозиция",
			"crawler_stage_errors_total{stage=\"http\",type=\"timeout\"} 99", 0, "не рендер-воркера"},
		{"ленивое семейство у рендер-воркера — 0",
			"crawler_stage_tasks_total{stage=\"render\"} 5\n", 0, ""},
		{"без метрик вовсе — чужая экспозиция", "# HELP x y\n", 0, "не рендер-воркера"},
		{"не-число", `crawler_stage_errors_total{stage="render",type="browser"} много`, 0, "не счётчик"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseBrowserErrorsExposition(strings.NewReader(tc.expo))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("разбор: %v", err)
			}
			if got != tc.want {
				t.Errorf("значение = %d, want %d", got, tc.want)
			}
		})
	}
}

// --- зонд /metrics рендер-воркера (suspends.go) ----------------------------------

// TestRenderSuspendsHTTP — зонд читает ветку reason="budget" и счётчик
// browser-ошибок из живой экспозиции (httptest): gate-события и прочие
// приборы не мешают, путь 404 — ошибка с адресом.
func TestRenderSuspendsHTTP(t *testing.T) {
	const expo = `# HELP crawler_render_fallback_suspended_total События приостановки
# TYPE crawler_render_fallback_suspended_total counter
crawler_render_fallback_suspended_total{reason="gate"} 2
crawler_render_fallback_suspended_total{reason="budget"} 7
crawler_stage_errors_total{stage="render",type="browser"} 4
crawler_stage_errors_total{stage="render",type="task"} 9
crawler_stage_tasks_total{stage="render"} 123
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(expo))
	}))
	defer srv.Close()

	got, err := RenderSuspends{URL: srv.URL}.FallbackBudget(context.Background())
	if err != nil {
		t.Fatalf("FallbackBudget: %v", err)
	}
	if got != 7 {
		t.Errorf("счётчик = %d, want 7 (только ветка budget)", got)
	}

	browser, err := RenderSuspends{URL: srv.URL}.RenderBrowserErrors(context.Background())
	if err != nil {
		t.Fatalf("RenderBrowserErrors: %v", err)
	}
	if browser != 4 {
		t.Errorf("browser-ошибки = %d, want 4 (ветка browser, соседние классы не мешают)", browser)
	}

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer broken.Close()
	if _, err := (RenderSuspends{URL: broken.URL}).FallbackBudget(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "500") {
		t.Errorf("не-200: %v, want ошибка со статусом", err)
	}
	if _, err := (RenderSuspends{URL: broken.URL}).RenderBrowserErrors(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "500") {
		t.Errorf("не-200 (browser): %v, want ошибка со статусом", err)
	}
}

// TestParseSuspendExposition — табличный разбор: точная пара меток,
// пропуск HELP/TYPE и чужих метрик, предсозданная серия T-250 (0 с
// первого старта), отсутствие метрики/ветки — 0 при маркере
// рендер-воркера (S-P1-4) и ошибка для чужой экспозиции, мусорное
// значение — ошибка.
func TestParseSuspendExposition(t *testing.T) {
	cases := []struct {
		name    string
		expo    string
		want    int64
		wantErr string
	}{
		{"ветка бюджета", `crawler_render_fallback_suspended_total{reason="budget"} 7`, 7, ""},
		{"предсозданная серия T-250 — 0 с первого старта",
			"crawler_stage_tasks_total{stage=\"render\"} 123\n" +
				"crawler_render_fallback_suspended_total{reason=\"budget\"} 0\n", 0, ""},
		{"семейство есть, ветки budget нет, маркер рендер-воркера — 0 (S-P1-4)",
			"crawler_render_fallback_suspended_total{reason=\"gate\"} 2\n" +
				"crawler_stage_tasks_total{stage=\"render\"} 123\n", 0, ""},
		{"старая сборка: маркер есть, семейства нет — 0",
			"crawler_stage_tasks_total{stage=\"render\"} 123\n", 0, ""},
		{"только gate, маркера нет — чужая экспозиция", `crawler_render_fallback_suspended_total{reason="gate"} 2`, 0, "отсутствует"},
		{"без метрик вовсе (чужая экспозиция)", "# HELP x y\n", 0, "отсутствует"},
		{"не-число", `crawler_render_fallback_suspended_total{reason="budget"} много`, 0, "не счётчик"},
		{"отрицательное", `crawler_render_fallback_suspended_total{reason="budget"} -1`, 0, "не счётчик"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSuspendExposition(strings.NewReader(tc.expo))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("разбор: %v", err)
			}
			if got != tc.want {
				t.Errorf("значение = %d, want %d", got, tc.want)
			}
		})
	}
}

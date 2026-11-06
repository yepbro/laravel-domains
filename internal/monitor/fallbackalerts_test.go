// Юнит-тесты алертов очереди фолбэка рендера (T-246, ТЗ рендера §4.6
// п.3): рост глубины q:render-fallback (порог — эффективный watermark
// стрима, механика render_debt: уровень + серия роста + защёлка) и
// превышение бюджета доли (окно дельт счётчика событий suspended{reason=
// "budget"} зонда /metrics рендер-воркера, suspends.go — парсер и
// деградации). Фейковые источники, стиль renderdebt/badhashes тестов.
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

func (f *fakeBudgetEvents) FallbackBudget(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := popErr(&f.errs); err != nil {
		return 0, err
	}
	return f.val, nil
}

func (f *fakeBudgetEvents) set(v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.val = v
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

	// База и рост ниже порога: серия роста копится, порог ещё не
	// задействован.
	setFallbackDepth(env, 500)
	s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth)
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
	setFallbackDepth(env, 800)
	s = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Fatalf("один проход роста: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	// Второй проход роста с пересечением порога — срабатывание.
	setFallbackDepth(env, 1420)
	s = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth)
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
	setFallbackDepth(env, 1410)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
		t.Errorf("стагнация: %s (%s), want firing (удержание)", s.Status, s.Detail)
	}
	// Дренаж ниже 90 % порога — снятие.
	setFallbackDepth(env, 890)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusOK {
		t.Errorf("дренаж: %s (%s), want ok", s.Status, s.Detail)
	}
	// Коридор гистерезиса (900..1000) без сработавшего алерта — ok:
	// порог срабатывания строгий, рост в коридоре серию копит.
	setFallbackDepth(env, 950)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusOK {
		t.Errorf("коридор без алерта: %s (%s), want ok", s.Status, s.Detail)
	}
	// Просадка сбрасывает серию; повторное накопление — снова два
	// прохода роста (рост через порог — то же накопление).
	setFallbackDepth(env, 800)
	_ = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth) // streak 0
	setFallbackDepth(env, 1010)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusOK || !strings.Contains(s.Detail, "серия роста 1/2") {
		t.Errorf("первый проход повторного роста: %s (%s), want ok с серией 1/2", s.Status, s.Detail)
	}
	setFallbackDepth(env, 1060)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
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

	// Стационарная глубина 9000 без роста: первый проход — база
	// (у порога 1/2), второй — firing.
	setFallbackDepth(env, 9000)
	s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "у порога 1/2") {
		t.Fatalf("стационарная база: %s (%s), want ok с серией у порога 1/2", s.Status, s.Detail)
	}
	setFallbackDepth(env, 9000)
	s = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth)
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
	setFallbackDepth(env, 15000)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
		t.Errorf("упор в MAXLEN: %s (%s), want firing", s.Status, s.Detail)
	}
	setFallbackDepth(env, 15000)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
		t.Errorf("упор в MAXLEN, второй проход: %s (%s), want firing (удержание)", s.Status, s.Detail)
	}

	// Дренаж ниже 90 % порога — снятие (защёлка не вечна).
	setFallbackDepth(env, 6000)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusOK {
		t.Errorf("дренаж: %s (%s), want ok", s.Status, s.Detail)
	}

	// Одиночный всплеск между проходами (рассасывается) — тихо: серия
	// у порога не набирается, рост одного прохода — не триггер.
	setFallbackDepth(env, 9000)
	_ = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth) // у порога 1/2, ok
	setFallbackDepth(env, 100)
	_ = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth) // рассосался, ok
	setFallbackDepth(env, 9000)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusOK || !strings.Contains(s.Detail, "у порога 1/2") {
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
			// Серия роста двумя проходами на глубине ровно порога.
			setFallbackDepth(env, tc.want+10)
			_ = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth) // база
			setFallbackDepth(env, tc.want+20)
			_ = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth) // рост 1
			setFallbackDepth(env, tc.want+30)
			s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth)
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
	setFallbackDepth(env, 1000)
	_ = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth) // база (у порога 1/2)
	setFallbackDepth(env, 1000)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
		t.Errorf("глубина == порогу без роста два прохода: %s (%s), want firing (стационарно у порога, S-P2-1)", s.Status, s.Detail)
	}
	setFallbackDepth(env, 1001)
	_ = snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth) // рост 1 (удержание)
	setFallbackDepth(env, 1002)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbDepth); s.Status != AlertStatusFiring {
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
	// — он раньше по порядку evaluators), ошибка — седьмая.
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
	src := &fakeBudgetEvents{}
	env.deps.FallbackBudget = src
	ctx := context.Background()

	// База: накопленное до старта мониторинга не событие.
	src.set(3)
	s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget)
	if s.Status != AlertStatusOK {
		t.Fatalf("база: %s (%s), want ok", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "события бюджета 3") {
		t.Errorf("detail = %q, want значение счётчика", s.Detail)
	}
	// Событие — срабатывание (порог 1: каждое защёлкивание — аномалия).
	src.set(4)
	s = snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget)
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
		if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusFiring {
			t.Fatalf("окно держит событие: %s (%s), want firing", s.Status, s.Detail)
		}
	}
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusOK {
		t.Fatalf("после старения окна: %s (%s), want ok", s.Status, s.Detail)
	}
	// Рестарт воркера (счётчик обнулился) — убыль не всплеск.
	src.set(0)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusOK {
		t.Errorf("рестарт воркера: %s (%s), want ok без всплеска", s.Status, s.Detail)
	}
	// Поток событий продлевает алерт: событие в каждом проходе.
	for i := range 3 {
		src.set(int64(i + 1))
		if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusFiring {
			t.Fatalf("поток событий, проход %d: %s, want firing", i, s.Status)
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
	src := &fakeBudgetEvents{}
	env.deps.FallbackBudget = src
	ctx := context.Background()

	// Предсозданная серия: зонд читает 0 (не «не измеряется»), prev
	// базируется нулём.
	src.set(0)
	s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget)
	if s.Status != AlertStatusOK {
		t.Fatalf("нулевая серия: %s (%s), want ok (предсоздание T-250)", s.Status, s.Detail)
	}
	// Первое событие 0→1 — firing: дельта видна, окно её держит.
	src.set(1)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusFiring {
		t.Fatalf("первое событие 0→1: %s (%s), want firing", s.Status, s.Detail)
	}
	// Окно стареет: 5 проходов держат событие, шестой вытесняет — снятие.
	for range 5 {
		if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusFiring {
			t.Fatalf("окно держит событие: %s (%s), want firing", s.Status, s.Detail)
		}
	}
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusOK {
		t.Fatalf("после старения окна: %s (%s), want ok", s.Status, s.Detail)
	}
	// Рестарт воркера: счётчик процессный — обнулился; убыль не всплеск.
	src.set(0)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusOK {
		t.Fatalf("рестарт воркера: %s (%s), want ok без всплеска", s.Status, s.Detail)
	}
	// Повторное первое событие (0→1 нового процесса) — снова firing.
	src.set(1)
	if s := snapshotOf(t, env.Pass(ctx), AlertRenderFbBudget); s.Status != AlertStatusFiring {
		t.Fatalf("первое событие после рестарта: %s (%s), want firing", s.Status, s.Detail)
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
}

// --- зонд /metrics рендер-воркера (suspends.go) ----------------------------------

// TestRenderSuspendsHTTP — зонд читает ветку reason="budget" из живой
// экспозиции (httptest): gate-события и прочие приборы не мешают, путь
// 404 — ошибка с адресом.
func TestRenderSuspendsHTTP(t *testing.T) {
	const expo = `# HELP crawler_render_fallback_suspended_total События приостановки
# TYPE crawler_render_fallback_suspended_total counter
crawler_render_fallback_suspended_total{reason="gate"} 2
crawler_render_fallback_suspended_total{reason="budget"} 7
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

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer broken.Close()
	if _, err := (RenderSuspends{URL: broken.URL}).FallbackBudget(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "500") {
		t.Errorf("не-200: %v, want ошибка со статусом", err)
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

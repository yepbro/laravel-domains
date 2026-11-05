// Юнит-тесты health-гейта и бюджета доли фолбэка (T-239, ТЗ §4.4
// п.3–4, ФТ-9): стрик инфра-ошибок класса браузера (основной движок
// недоступен) числом render.fallback.health_gate_streak приостанавливает
// фолбэк-публикацию до восстановления движка — кандидат финализируется
// по-сегодняшнему (маркер render_failed_at + q:analyze), без потерь;
// доля публикаций за окно сверх render.fallback.share_percent
// приостанавливает публикации до границы окна (маркер ставится);
// события приостановок — crawler_render_fallback_suspended_total
// {gate|budget}. Сценарий «движок лёг» — юнит-уровень через фейки:
// живой прогон требует ПАДАЮЩЕГО движка по расписанию (детерминированный
// стрик ClassBrowser), что даёт только фейк рендера — интеграционная
// приёмка фолбэка (TestAccept03) использует тот же приём для воркера A.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// newRWGateFixture — фикстура с вооружённым фолбэком (Enabled + стрим
// q:render) и подключёнными состояниями гейта/бюджета (T-239):
// threshold — порог стрика инфра-ошибок (render.fallback
// .health_gate_streak), ceiling — потолок доли публикаций за окно
// (render.fallback.share_percent; 0 — без потолка). Реестр метрик —
// для проверки событий приостановки.
func newRWGateFixture(t *testing.T, threshold int, ceiling float64) (*rwFixture, *metrics.Registry) {
	t.Helper()
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{
			Enabled: true,
			Gate:    FallbackGate{Threshold: threshold},
			Budget:  FallbackBudget{Ceiling: ceiling},
		},
		Metrics: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f, reg
}

// rwBrowserErr — ошибка рендера класса браузера (основной движок
// недоступен): как ClassOf классифицирует сбои подключения/зонда
// (T-065) — инфра-ошибка для стрика health-гейта (T-239).
func rwBrowserErr(url string) error {
	return &render.Error{Class: render.ClassBrowser, URL: url,
		Err: errors.New("браузер недоступен: соединение потеряно")}
}

// rwGateSuspended — значение crawler_render_fallback_suspended_total
// {reason=gate} (−1 — серии нет).
func rwGateSuspended(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	return rwMetricValue(t, reg, "crawler_render_fallback_suspended_total",
		map[string]string{"reason": fbReasonGate})
}

// rwBudgetSuspended — значение crawler_render_fallback_suspended_total
// {reason=budget} (−1 — серии нет).
func rwBudgetSuspended(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	return rwMetricValue(t, reg, "crawler_render_fallback_suspended_total",
		map[string]string{"reason": fbReasonBudget})
}

// rwFBCount — записей в q:render-fallback (фикстура без очистки
// публикаций — счётчик растёт по фазам теста).
func rwFBCount(t *testing.T, f *rwFixture) int {
	t.Helper()
	return len(rwFallbackPublishes(t, f))
}

// TestRenderWorker_FallbackDecision — таблица состояний функции-решения
// (кандидат × гейт × бюджет, T-239; замена стаб-теста always-publish
// T-238): здоровый движок публикует, стрик на/за порогом и приостановка
// бюджета отказывают; нулевые пороги (сборки без конфигурации) — гейт и
// бюджет выключены, поведение заглушки T-238.
func TestRenderWorker_FallbackDecision(t *testing.T) {
	cases := []struct {
		name   string
		gate   FallbackGate
		budget FallbackBudget
		want   bool
	}{
		{"здоров (нулевые состояния)", FallbackGate{}, FallbackBudget{}, true},
		{"гейт выключен (порог 0), стрик есть", FallbackGate{Threshold: 0, streak: 5}, FallbackBudget{}, true},
		{"стрик ниже порога", FallbackGate{Threshold: 3, streak: 2}, FallbackBudget{}, true},
		{"стрик на пороге", FallbackGate{Threshold: 3, streak: 3}, FallbackBudget{}, false},
		{"стрик за порогом", FallbackGate{Threshold: 3, streak: 7}, FallbackBudget{}, false},
		{"бюджет без потолка, окно тратилось", FallbackGate{}, FallbackBudget{Ceiling: 0, tasks: 999, published: 999}, true},
		{"доля окна на потолке — не сверх", FallbackGate{}, FallbackBudget{Ceiling: 30, tasks: 1000, published: 300}, true},
		{"окно бюджета приостановлено", FallbackGate{}, FallbackBudget{Ceiling: 30, hold: true}, false},
		{"гейт и бюджет одновременно", FallbackGate{Threshold: 1, streak: 1}, FallbackBudget{Ceiling: 30, hold: true}, false},
	}
	cand := fallbackCandidate{task: rwTask, reason: "timeout"}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := fallbackPublishDecision(cand, c.gate, c.budget); got != c.want {
				t.Errorf("fallbackPublishDecision(%+v, %+v) = %v, want %v", c.gate, c.budget, got, c.want)
			}
		})
	}
}

// TestRenderWorker_FallbackGateBrowserStreakSuspends — стрик инфра-ошибок
// ClassBrowser закрывает гейт (ФТ-9а): ниже порога публикации идут
// (единичная инфра-ошибка — шум живого потока), ошибка класса задачи
// стрик НЕ сбрасывает (свойство страницы, не движка — тарпиты есть и у
// здорового), порог достигнут — событие приостановки, кандидат
// финализируется по-сегодняшнему (маркер + q:analyze) БЕЗ публикации в
// q:render-fallback; событие не плодится на инфра-ошибках за порогом.
func TestRenderWorker_FallbackGateBrowserStreakSuspends(t *testing.T) {
	f, reg := newRWGateFixture(t, 3, 30)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwBrowserErr("https://spa.example/")

	// Две инфра-ошибки: доставки возвращаются ошибкой (redelivery,
	// browser-класс финализацией не покрывается), события нет.
	for i := 1; i <= 2; i++ {
		if err := f.w.processDelivery(context.Background(), rwDelivery(fmt.Sprintf("e%d", i)), 1); err == nil {
			t.Fatalf("инфра-ошибка класса браузера — ошибка доставки (redelivery), доставка %d", i)
		}
	}
	if got := rwGateSuspended(t, reg); got != -1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} до порога = %v, want -1 (серии нет)", got)
	}

	// Кандидат между инфра-ошибками (стрик 2 < 3): публикация идёт,
	// ошибка класса задачи стрик не сбрасывает — иначе тарпиты живого
	// потока открывали бы гейт лежачего движка.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("кандидат при открытом гейте — публикация, не ошибка: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Fatalf("публикаций q:render-fallback = %d, нужна 1 (гейт открыт)", n)
	}

	// Третья инфра-ошибка — порог (дефолт конфига 3): событие
	// приостановки, одно на переход «открыт → закрыт».
	f.renderer.err = rwBrowserErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("e3"), 1); err == nil {
		t.Fatal("третья инфра-ошибка — ошибка доставки (redelivery)")
	}
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1", got)
	}

	// Кандидат при закрытом гейте — ДРУГАЯ запись (не дубль уже
	// опубликованной: дубль узнаётся до решения, см.
	// TestRenderWorker_FallbackDuplicateDuringSuspension): финализация
	// по-сегодняшнему — маркер
	// и q:analyze, БЕЗ публикации (перелив потока на дорогой движок при
	// нездоровом основном исключён, ФТ-9а), задача без потерь.
	key2 := rwRowKey{zone: 3, domain: rwKey.domain + 100, subdomain: 1, variant: 0}
	f.pool.seedRow(key2, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "t2", key2, "spa.example"), 2); err != nil {
		t.Fatalf("кандидат при закрытом гейте — штатная финализация, не ошибка: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Errorf("публикаций q:render-fallback = %d, want 1 (гейт закрыт — новой быть не должно)", n)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 || pubs[1].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна q:analyze финализации (как при выключенном фолбэке)", pubs)
	}
	if row := rwRowSnapshot(f.pool, key2); row.failedAt == nil {
		t.Error("маркер render_failed_at обязан ставиться (приостановка = сегодняшняя финализация, ТЗ §4.4 п.3)")
	}

	// Инфра-ошибка за порогом: событие не плодится (стрик 4).
	f.renderer.err = rwBrowserErr("https://spa.example/")
	_ = f.w.processDelivery(context.Background(), rwDelivery("e4"), 1)
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (одно событие на защёлкивание)", got)
	}
}

// TestRenderWorker_FallbackGateRecoversOnSuccess — восстановление движка
// (ФТ-9а): успешный рендер обнуляет стрик (движок подал признак жизни,
// образец renderFailBackoff — обнуление успехом), гейт открывается и
// следующие кандидаты публикуются; события восстановления нет (метрика —
// только приостановки).
func TestRenderWorker_FallbackGateRecoversOnSuccess(t *testing.T) {
	f, reg := newRWGateFixture(t, 3, 30)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwBrowserErr("https://spa.example/")

	// Порог: гейт закрыт.
	for i := 1; i <= 3; i++ {
		_ = f.w.processDelivery(context.Background(), rwDelivery(fmt.Sprintf("e%d", i)), 1)
	}
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Fatalf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (порог достигнут)", got)
	}

	// Восстановление: успешный рендер другой записи (менеджер
	// перезапустил движок, T-065 — страница отрисовалась).
	key2 := rwRowKey{zone: 3, domain: rwKey.domain + 1, subdomain: 1, variant: 0}
	f.pool.seedRow(key2, "https://ok.example/", &raw)
	f.renderer.err = nil
	f.renderer.output = rwOutput("<html><body>dom</body></html>")
	if err := f.w.processDelivery(context.Background(), rwKeyDelivery(t, "ok1", key2, "ok.example"), 1); err != nil {
		t.Fatalf("успешный рендер после восстановления: %v", err)
	}
	if row := rwRowSnapshot(f.pool, key2); !row.rendered {
		t.Fatal("успешный рендер обязан записать DOM (движок жив)")
	}

	// Гейт открыт: кандидат публикуется в фолбэк, маркера нет.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("кандидат после восстановления — публикация, не ошибка: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Errorf("публикаций q:render-fallback = %d, нужна 1 (гейт открыт восстановлением)", n)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("маркер не ставится — запись ждёт фолбэка (ФТ-10)")
	}
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (события восстановления нет)", got)
	}
}

// TestRenderWorker_FallbackBudgetWindowSuspension — окно бюджета доли
// (ФТ-9б): публикации копятся числителем, подтверждённые доставки —
// знаменателем; заполненное окно с долей сверх потолка приостанавливает
// публикации следующего окна — кандидат получает маркер (критерий
// «после исчерпания бюджета — маркер ставится»), событие {budget};
// приостановленное окно публикаций не набирает — следующая оценка
// возвращает фолбэк (самовозврат).
func TestRenderWorker_FallbackBudgetWindowSuspension(t *testing.T) {
	// Потолок 10%: 101 публикация на 1000 доставок окна (10,1%) —
	// перерасход; 100 (ровно 10%) — нет («сверх потолка»).
	f, reg := newRWGateFixture(t, 0, 10)
	raw := rwTestHash(1)

	// Фаза A — 101 кандидат: каждый публикуется (гейт выключен
	// порогом 0, бюджет до оценки окна не мешает).
	f.renderer.err = rwTarpitErr("https://spa.example/")
	pubs := 0
	for i := 0; i < 101; i++ {
		key := rwRowKey{zone: 3, domain: int64(2000 + i), subdomain: 1, variant: 0}
		f.pool.seedRow(key, "https://spa.example/", &raw)
		if err := f.w.processDelivery(context.Background(),
			rwKeyDelivery(t, fmt.Sprintf("a%d", i), key, "spa.example"), 2); err != nil {
			t.Fatalf("кандидат %d: публикация: %v", i, err)
		}
		pubs++
	}
	if n := rwFBCount(t, f); n != pubs {
		t.Fatalf("публикаций q:render-fallback = %d, want %d", n, pubs)
	}
	if got := rwBudgetSuspended(t, reg); got != -1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} до закрытия окна = %v, want -1", got)
	}

	// Фаза B — наполнитель до 1000 подтверждённых доставок: доставки
	// без строк подтверждаются штатным отказом. На тысячной окно
	// закрывается: 10,1% > 10% — приостановка следующего окна.
	filler := func(from, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			key := rwRowKey{zone: 9, domain: int64(50000 + from + i), subdomain: 1, variant: 0}
			if err := f.w.processDelivery(context.Background(),
				rwKeyDelivery(t, fmt.Sprintf("b%d", from+i), key, "missing.example"), 1); err != nil {
				t.Fatalf("наполнитель %d: строки нет — не ошибка: %v", from+i, err)
			}
		}
	}
	filler(0, 899)
	if got := rwBudgetSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want 1 (окно перерасходовало потолок)", got)
	}

	// Фаза C — кандидат в приостановленном окне: финализация с маркером
	// (как при выключенном фолбэке), публикации нет.
	key := rwRowKey{zone: 3, domain: 3000, subdomain: 1, variant: 0}
	f.pool.seedRow(key, "https://spa.example/", &raw)
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "c1", key, "spa.example"), 2); err != nil {
		t.Fatalf("кандидат в приостановленном окне — штатная финализация, не ошибка: %v", err)
	}
	if n := rwFBCount(t, f); n != pubs {
		t.Errorf("публикаций q:render-fallback = %d, want %d (бюджет исчерпан — окно держит)", n, pubs)
	}
	if row := rwRowSnapshot(f.pool, key); row.failedAt == nil {
		t.Error("после исчерпания бюджета маркер render_failed_at ставится (ТЗ §4.4 п.4)")
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "timeout"}); got != float64(pubs) {
		t.Errorf("crawler_render_fallback_published_total{timeout} = %v, want %d", got, pubs)
	}

	// Фаза D — приостановленное окно добирает 999 подтверждённых
	// доставок (кандидат фазы C уже посчитан) и закрывается без
	// публикаций: оценка возвращает фолбэк.
	filler(899, fallbackBudgetWindow-1)
	if got := rwBudgetSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want 1 (одно событие на защёлкивание)", got)
	}

	// Фаза E — следующее окно публикует снова (самовозврат).
	key2 := rwRowKey{zone: 3, domain: 3001, subdomain: 1, variant: 0}
	f.pool.seedRow(key2, "https://spa.example/", &raw)
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "e1", key2, "spa.example"), 2); err != nil {
		t.Fatalf("кандидат после окна — публикация, не ошибка: %v", err)
	}
	if n := rwFBCount(t, f); n != pubs+1 {
		t.Errorf("публикаций q:render-fallback = %d, want %d (окно возобновило публикации)", n, pubs+1)
	}
	if got := rwBudgetSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want 1 (возобновление события не рождает)", got)
	}
}

// TestRenderWorker_FallbackBudgetExactCeilingNotSuspended — доля ровно
// на потолке — не «сверх»: окно закрывается без приостановки (порог
// отклонения ТЗ §5.3 — выше 30% — обнуляет экономику, ровно 30% —
// рабочая точка).
func TestRenderWorker_FallbackBudgetExactCeilingNotSuspended(t *testing.T) {
	f, reg := newRWGateFixture(t, 0, 10)
	raw := rwTestHash(1)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	// 100 публикаций из 1000 доставок = ровно 10%.
	for i := 0; i < 100; i++ {
		key := rwRowKey{zone: 3, domain: int64(30000 + i), subdomain: 1, variant: 0}
		f.pool.seedRow(key, "https://spa.example/", &raw)
		if err := f.w.processDelivery(context.Background(),
			rwKeyDelivery(t, fmt.Sprintf("a%d", i), key, "spa.example"), 2); err != nil {
			t.Fatalf("кандидат %d: публикация: %v", i, err)
		}
	}
	for i := 0; i < 900; i++ {
		key := rwRowKey{zone: 9, domain: int64(80000 + i), subdomain: 1, variant: 0}
		if err := f.w.processDelivery(context.Background(),
			rwKeyDelivery(t, fmt.Sprintf("b%d", i), key, "missing.example"), 1); err != nil {
			t.Fatalf("наполнитель %d: %v", i, err)
		}
	}
	if got := rwBudgetSuspended(t, reg); got != -1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want -1 (доля на потолке, не сверх)", got)
	}
	// Следующий кандидат публикуется: окно не приостанавливалось.
	key := rwRowKey{zone: 3, domain: 31000, subdomain: 1, variant: 0}
	f.pool.seedRow(key, "https://spa.example/", &raw)
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "c1", key, "spa.example"), 2); err != nil {
		t.Fatalf("кандидат после окна на потолке: %v", err)
	}
	if n := rwFBCount(t, f); n != 101 {
		t.Errorf("публикаций q:render-fallback = %d, want 101", n)
	}
}

// TestRenderWorker_FallbackGateNoOpDoesNotReset — сброс стрика только
// успешным рендером (ревью T-239, P1): штатный отказ без рендера
// (строки нет / ушла из-под задачи — ProcessResult вернул nil, не
// выполнив рендер) признаком жизни движка не считается — no-op между
// инфра-ошибками стрик не обнуляет, гейт защёлкивается сквозь него.
func TestRenderWorker_FallbackGateNoOpDoesNotReset(t *testing.T) {
	f, reg := newRWGateFixture(t, 3, 30)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)

	// Две инфра-ошибки движка (доставка возвращается ошибкой — ред.)
	f.renderer.err = rwBrowserErr("https://spa.example/")
	_ = f.w.processDelivery(context.Background(), rwDelivery("e1"), 1)
	_ = f.w.processDelivery(context.Background(), rwDelivery("e2"), 1)

	// No-op между ними: строки нет — успех без рендера; рендер не
	// выполнялся, движок признака жизни не подал.
	missing := rwRowKey{zone: 9, domain: 99999, subdomain: 1, variant: 0}
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "noop", missing, "missing.example"), 1); err != nil {
		t.Fatalf("no-op — не ошибка: %v", err)
	}

	// Третья инфра-ошибка — порог достигнут (no-op стрик не сбросил).
	_ = f.w.processDelivery(context.Background(), rwDelivery("e3"), 1)
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Fatalf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (no-op без рендера стрик не сбрасывает)", got)
	}

	// Кандидат при закрытом гейте — маркер, без публикации.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("кандидат при закрытом гейте — штатная финализация: %v", err)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Errorf("публикаций q:render-fallback = %d, хочу 0", n)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("маркер render_failed_at ставится (приостановка = сегодняшняя финализация)")
	}
}

// TestRenderWorker_FallbackDuplicateDuringSuspension — дубль доставки
// опубликованной записи узнаётся ДО решения гейта/бюджета (ревью T-239,
// P3): запись уже ждёт воркера B в q:render-fallback — приостановка
// состоявшуюся публикацию не отменяет, дубль подтверждается без второй
// записи в стриме, без маркера и без q:analyze (ФТ-11).
func TestRenderWorker_FallbackDuplicateDuringSuspension(t *testing.T) {
	f, _ := newRWGateFixture(t, 3, 30)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)

	// Публикация состоялась; XACK потерян — доставка вернётся дублем.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("фолбэк-публикация: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Fatalf("публикаций q:render-fallback = %d, нужна 1", n)
	}

	// Движок лёг: гейт закрыт стриком.
	f.renderer.err = rwBrowserErr("https://spa.example/")
	for i := 0; i < 3; i++ {
		_ = f.w.processDelivery(context.Background(), rwDelivery(fmt.Sprintf("e%d", i)), 1)
	}

	// Дубль при закрытом гейте: без второй публикации, без маркера,
	// без q:analyze — запись дорабатывает воркер B.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1dup"), 2); err != nil {
		t.Fatalf("дубль в приостановке подтверждается без ошибки: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Errorf("публикаций q:render-fallback = %d, want 1 (дубль не дублирует стрим)", n)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("дублю маркер не нужен — запись уже ждёт воркера B (ФТ-10)")
	}
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamAnalyze {
			t.Errorf("q:analyze дублю не положен: %+v", p)
		}
	}
}

// TestRenderWorker_FallbackStatesParallel — параллельные доставки
// (ревью T-239, P3: заявленная потокобезопасность fbMu): N кандидатов
// одновременно публикуются — учёт числителя/знаменателя окна без потерь,
// дедлоков нет; все доставки завершаются (Timeout через wait group).
func TestRenderWorker_FallbackStatesParallel(t *testing.T) {
	f, reg := newRWGateFixture(t, 1000, 30) // порог недостижим — гейт открыт
	raw := rwTestHash(1)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	const n = 64
	dels := make([]queue.Delivery, n)
	for i := 0; i < n; i++ {
		key := rwRowKey{zone: 3, domain: int64(40000 + i), subdomain: 1, variant: 0}
		f.pool.seedRow(key, "https://spa.example/", &raw)
		dels[i] = rwKeyDelivery(t, fmt.Sprintf("p%d", i), key, "spa.example")
	}
	var wg sync.WaitGroup
	for _, del := range dels {
		wg.Add(1)
		go func(del queue.Delivery) {
			defer wg.Done()
			_ = f.w.processDelivery(context.Background(), del, 2)
		}(del)
	}
	wg.Wait()

	// Все публикации прошли: числитель = n (учёт без гонок), доля
	// окна n/n ниже потолка 30% не защёлкивалась — окно ещё не закрыто.
	if got := rwFBCount(t, f); got != n {
		t.Errorf("публикаций q:render-fallback = %d, want %d", got, n)
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "timeout"}); got != float64(n) {
		t.Errorf("crawler_render_fallback_published_total{timeout} = %v, want %d", got, n)
	}
	if got := rwBudgetSuspended(t, reg); got != -1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want -1 (окно не закрывалось)", got)
	}
	_, budget := f.w.fallbackStates()
	if budget.tasks != n || budget.published != n {
		t.Errorf("окно бюджета: tasks=%d published=%d, want %d/%d (учёт без гонок)", budget.tasks, budget.published, n, n)
	}
}

// TestRenderWorker_FallbackWorkerBNoState — изоляция воркера B (ревью
// T-239, P3): тот же бинарь на стриме q:render-fallback наблюдений
// гейта/бюджета не ведёт (fallbackArmed) и событий приостановки не
// поднимает — его движок (chromium) не основной, алерты фолбэка не
// его забота.
func TestRenderWorker_FallbackWorkerBNoState(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{
			Enabled: true,
			Gate:    FallbackGate{Threshold: 1}, // порог 1: любая инфра-ошибка закрыла бы
			Budget:  FallbackBudget{Ceiling: 30},
		},
		Stream:  queue.StreamRenderFallback, // сборка воркера B (T-236)
		Metrics: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwBrowserErr("https://spa.example/")

	// Инфра-ошибка и кандидат на воркере B: состояний нет, метрика
	// приостановок не растёт.
	_ = f.w.processDelivery(context.Background(), rwDelivery("e1"), 1)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("воркер B: кандидат — финализация (рекурсия запрещена): %v", err)
	}
	gate, budget := f.w.fallbackStates()
	if gate.streak != 0 || budget.tasks != 0 || budget.published != 0 || budget.hold {
		t.Errorf("состояния воркера B: gate=%+v budget=%+v, хочу нулевые (наблюдения только в вооружённой сборке)", gate, budget)
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_suspended_total", nil); got != -1 {
		t.Errorf("crawler_render_fallback_suspended_total = %v, want -1 (серий нет)", got)
	}
	if got := rwGateSuspended(t, reg); got != -1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want -1", got)
	}
}

// TestRenderWorker_FallbackEngineDownScenario — сценарий «движок лёг»
// (ТЗ §4.4 п.3, критерий T-239): основной движок отдаёт инфра-ошибки
// класса браузера — публикации в q:render-fallback НЕТ (browser-класс —
// не кандидат, инфраструктура), каждая доставка возвращается ошибкой
// без XACK (задачи в redelivery q:render — клейм вернёт их после
// восстановления); стрик закрывает гейт — смешанный кандидат
// (движко-зависимая ошибка класса задачи) финализируется с маркером,
// доставки без потерь. Живой движок, падающий по расписанию, даёт
// только фейк (см. заголовок файла), проводка redelivery —
// queue.ProcessingAttempt: ошибка ⇒ без XACK, nil ⇒ XACK.
func TestRenderWorker_FallbackEngineDownScenario(t *testing.T) {
	f, reg := newRWGateFixture(t, 3, 30) // дефолты конфигурации T-233
	raw := rwTestHash(1)

	// Несколько записей живого потока; движок лёг — ClassBrowser на
	// каждой задаче.
	keys := []rwRowKey{
		rwKey,
		{zone: 3, domain: rwKey.domain + 1, subdomain: 1, variant: 0},
		{zone: 3, domain: rwKey.domain + 2, subdomain: 1, variant: 0},
		{zone: 3, domain: rwKey.domain + 3, subdomain: 1, variant: 0},
		{zone: 3, domain: rwKey.domain + 4, subdomain: 1, variant: 0},
	}
	for _, k := range keys {
		f.pool.seedRow(k, "https://spa.example/", &raw)
	}
	f.renderer.err = rwBrowserErr("https://spa.example/")

	// Все доставки — ошибки без XACK: задачи в redelivery, публикаций
	// в фолбэк нет (класс браузера кандидатом не является, ФТ-7).
	for i, k := range keys {
		del := rwKeyDelivery(t, fmt.Sprintf("d%d", i), k, "spa.example")
		if err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", del, 1, f.w.processDelivery); err == nil {
			t.Fatalf("движок лёг: доставка %d обязана вернуться ошибкой (redelivery)", i)
		}
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v, хочу пусто (задачи в redelivery, попытки не жгутся)", acked)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Fatalf("публикаций q:render-fallback = %d, хочу 0 (движок лёг — перелива нет)", n)
	}
	// Стрик достиг порога — событие гейта (алерт деградации движка).
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (стрик инфра-ошибок)", got)
	}

	// Смешанный кандидат при лежачем движке: движко-зависимая ошибка
	// класса задачи дошла бы до фолбэка, но гейт закрыт — финализация
	// по-сегодняшнему (маркер + q:analyze), доставка подтверждается:
	// задач без потерь (redelivery либо штатная финализация).
	f.renderer.err = rwTarpitErr("https://spa.example/")
	del := rwKeyDelivery(t, "c1", keys[0], "spa.example")
	if err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", del, 2, f.w.processDelivery); err != nil {
		t.Fatalf("кандидат при лежачем движке — штатная финализация, не ошибка: %v", err)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Errorf("публикаций q:render-fallback = %d, хочу 0 (гейт закрыт)", n)
	}
	if row := rwRowSnapshot(f.pool, keys[0]); row.failedAt == nil {
		t.Error("маркер render_failed_at ставится — приостановка = сегодняшняя финализация")
	}
	acked := f.q.ackedSnapshot()
	if len(acked) != 1 || acked[0] != "c1" {
		t.Errorf("XACK = %v, хочу [c1] (финализация подтверждает доставку)", acked)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze (конвейер доходит до анализа)", pubs)
	}
}

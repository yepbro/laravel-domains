// Юнит-тесты публикации фолбэка из путей финализации без рендера
// (T-238, ТЗ §4.4 п.1–2/п.5, ФТ-7/8/10/11/14): тарпит T-148 и класс
// задачи T-172 на последней попытке при включённом render.fallback
// .enabled публикуют задачу в q:render-fallback вместо маркера
// render_failed_at (маркер и q:analyze — воркеру B после исчерпания
// фолбэка, ФТ-10); DNS-класс T-211 и cert-класс T-159 фолбэка не
// получают (движко-независимые причины, ФТ-7); выключенный фолбэк —
// вчерашнее поведение (маркер + q:analyze); сбой публикации — ошибка
// без маркера, доставка в redelivery; повторная доставка уже
// опубликованной записи не дублирует стрим (фильтр по идентификатору
// записи результата). Фейковое окружение — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// newRWFallbackFixture — фикстура с включённым фолбэком
// (FallbackOptions.Enabled, T-238): остальные опции — дефолты
// (TimeoutAttempts 2, MaxAttempts 3, Stream q:render); нулевые пороги
// гейта/бюджета (T-239) — состояния выключены, решение публикует без
// потолков (поведение заглушки always-publish для тестов T-238).
func newRWFallbackFixture(t *testing.T) *rwFixture {
	t.Helper()
	f := newRWFixture(t)
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Fallback: FallbackOptions{Enabled: true}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

// rwFallbackPublishes — публикации в q:render-fallback из всех
// зафиксированных (подсобник проверок).
func rwFallbackPublishes(t *testing.T, f *rwFixture) []rwPublishRec {
	t.Helper()
	var out []rwPublishRec
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamRenderFallback {
			out = append(out, p)
		}
	}
	return out
}

// TestRenderWorker_TarpitFallbackPublish — тарпит на исчерпании
// лимита с включённым фолбэком: вместо маркера и q:analyze задача
// публикуется в q:render-fallback (Fresh=true, ключ записи), строка не
// тронута (rendered=false, прежний content_hash, render_failed_at
// NULL — «ждёт фолбэка», ТЗ §4.4 п.1), доставка подтверждается,
// Put/транзакция не выполняются, метрика
// crawler_render_fallback_published_total{timeout} растёт, счётчики
// финализаций — нет.
func TestRenderWorker_TarpitFallbackPublish(t *testing.T) {
	f := newRWFallbackFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Fallback: FallbackOptions{Enabled: true}, Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// Доставка №2 из 2 (DefaultTimeoutAttempts).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("тарпит с фолбэком — публикация, не ошибка: %v", err)
	}
	pubs := rwFallbackPublishes(t, f)
	if len(pubs) != 1 {
		t.Fatalf("публикации q:render-fallback: %d, нужна 1 (всего %+v)", len(pubs), f.q.publishesSnapshot())
	}
	if !pubs[0].msg.Fresh {
		t.Error("фолбэк-сообщение Fresh=true — доработка текущего эпизода, как у финализации")
	}
	if pubs[0].msg.ZoneID != rwTask.ZoneID || pubs[0].msg.DomainID != rwTask.DomainID ||
		pubs[0].msg.SubdomainID != rwTask.SubdomainID || pubs[0].msg.Variant != rwTask.Variant {
		t.Errorf("фолбэк-ключ %+v не совпадает с задачей", pubs[0].msg)
	}
	// Ни q:analyze, ни маркера: запись ждёт воркера B (ФТ-10).
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamAnalyze {
			t.Errorf("q:analyze публикует воркер B после фолбэка, не первичный: %+v", p)
		}
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt != nil {
		t.Error("маркер render_failed_at ставить нельзя — запись ждёт фолбэка (ФТ-10), маркер поставит воркер B")
	}
	if row.rendered {
		t.Error("rendered должен остаться false")
	}
	if row.hash == nil || string(row.hash) != string(raw[:]) {
		t.Errorf("content_hash = %v, нужна прежняя ссылка на сырой HTML %s", row.hash, raw)
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не должен вызываться: %d", len(puts))
	}
	if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
		t.Errorf("транзакция не должна открываться (событие на %d)", i)
	}
	if refc := rwRefc(f.pool)[raw]; refc != 1 {
		t.Errorf("refcount сырого HTML = %d, нужен 1 (ссылка жива)", refc)
	}
	// Метрики: публикация с причиной timeout, финализаций нет.
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "timeout"}); got != 1 {
		t.Errorf("crawler_render_fallback_published_total{timeout} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 0 (не финализация)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total не должен иметь серий: %v", got)
	}
}

// TestRenderWorker_TaskFallbackPublish — класс задачи на последней
// попытке с включённым фолбэком: та же публикация в q:render-fallback
// вместо маркера, метка причины — код net::ERR_* (словарь
// taskFinalizeReason, ТЗ §4.6 п.2).
func TestRenderWorker_TaskFallbackPublish(t *testing.T) {
	f := newRWFallbackFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Fallback: FallbackOptions{Enabled: true}, Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

	// Доставка №3 из 3 (DefaultMaxAttempts): после тарпит-ветки —
	// ветка последней попытки T-172.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("класс задачи на последней попытке с фолбэком — публикация, не ошибка: %v", err)
	}
	pubs := rwFallbackPublishes(t, f)
	if len(pubs) != 1 {
		t.Fatalf("публикации q:render-fallback: %d, нужна 1 (всего %+v)", len(pubs), f.q.publishesSnapshot())
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("маркер render_failed_at ставить нельзя — запись ждёт фолбэка (ФТ-10)")
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "net::ERR_CONNECTION_REFUSED"}); got != 1 {
		t.Errorf("crawler_render_fallback_published_total{net::ERR_CONNECTION_REFUSED} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total не должен иметь серий: %v", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 0 (не cert-финализация)", got)
	}
}

// TestRenderWorker_FallbackDisabledTodayBehavior — выключенный фолбэк
// (дефолт, ФТ-14): финализации работают как сегодня — маркер
// render_failed_at, публикация q:analyze, ни одной записи в
// q:render-fallback.
func TestRenderWorker_FallbackDisabledTodayBehavior(t *testing.T) {
	f := newRWFixture(t) // Options{} — фолбэк выключен
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("тарпит без фолбэка — финализация: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze (поведение до T-238)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("маркер render_failed_at обязан ставиться (T-210) при выключенном фолбэке")
	}
	if fb := rwFallbackPublishes(t, f); len(fb) != 0 {
		t.Errorf("публикаций q:render-fallback быть не должно: %+v", fb)
	}
}

// TestRenderWorker_DNSCertNoFallback — движко-независимые причины
// (ФТ-7): DNS-класс T-211 и cert-класс T-159 финализируются как сегодня
// — маркер + q:analyze, — фолбэка не получают и при включённом флаге:
// Chromium DNS-отказ и ошибку сертификата хоста не исправит (ТЗ §4.4
// п.2).
func TestRenderWorker_DNSCertNoFallback(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"DNS-класс (T-211)", rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")},
		{"cert-класс (T-159)", rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFallbackFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			f.renderer.err = c.err

			// Оба класса финализируются с первой же доставки.
			if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
				t.Fatalf("движко-независимый класс — финализация с первой доставки: %v", err)
			}
			if fb := rwFallbackPublishes(t, f); len(fb) != 0 {
				t.Errorf("фолбэк не положен: %+v", fb)
			}
			pubs := f.q.publishesSnapshot()
			if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
				t.Fatalf("публикации %+v, нужна одна q:analyze (финализация как сегодня)", pubs)
			}
			if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
				t.Error("маркер render_failed_at обязан ставиться (T-210)")
			}
		})
	}
}

// TestRenderWorker_FallbackNoRecursionWorkerB — запрет рекурсии
// (ФТ-10, ТЗ §4.5 п.2): воркер B — тот же бинарь со стримом потребления
// q:render-fallback; его движко-зависимый сбой на исчерпании попыток
// финализируется маркером как сегодня, фолбэк фолбэка не публикуется —
// стрим всегда один, движок всегда chromium.
func TestRenderWorker_FallbackNoRecursionWorkerB(t *testing.T) {
	f := newRWFallbackFixture(t)
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{Enabled: true},
		Stream:   queue.StreamRenderFallback, // сборка воркера B (T-236)
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("воркер B: тарпит на исчерпании — финализация (не рекурсивный фолбэк): %v", err)
	}
	if fb := rwFallbackPublishes(t, f); len(fb) != 0 {
		t.Errorf("рекурсивный фолбэк запрещён: %+v", fb)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze (финализация воркера B, ТЗ §4.5 п.2)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("воркер B ставит маркер после исчерпания фолбэка (ФТ-10)")
	}
}

// TestRenderWorker_FallbackPublishErrorNoMarker — транзакционность
// композиции «публикация → подтверждение» (ТЗ §4.4 п.1, ФТ-10): сбой
// публикации q:render-fallback — ошибка без XACK (доставка в
// redelivery), маркер НЕ стоит, строка не менялась; повторная доставка
// публикует (фиксация дубля снимается провалом — задача не теряется
// между фильтром и стримом).
func TestRenderWorker_FallbackPublishErrorNoMarker(t *testing.T) {
	f := newRWFallbackFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	// Сбой публикации: ошибка, доставка вернётся (redelivery).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("сбой публикации q:render-fallback — ошибка (повторная доставка)")
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("маркер render_failed_at не ставится: запись не финализирована — публикация не состоялась")
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
		t.Error("rendered должен остаться false")
	}

	// Повторная доставка: публикация проходит, маркера по-прежнему нет.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("повтор после сбоя публикации обязан опубликовать фолбэк: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	fb := 0
	for _, p := range pubs {
		switch p.stream {
		case queue.StreamRenderFallback:
			fb++
		case queue.StreamAnalyze:
			t.Error("q:analyze публикует воркер B, не первичный воркер")
		}
	}
	if fb != 2 {
		// Первая (неудачная) попытка тоже зафиксирована фейком: успешная
		// публикация одна — фильтр дубля не съел повтор после сбоя.
		t.Errorf("попыток публикации q:render-fallback = %d, нужно 2 (сбой + успех), в стриме — одна", fb)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("маркер не ставится и после успешной фолбэк-публикации (ФТ-10)")
	}
}

// TestRenderWorker_FallbackDuplicateDelivery — идемпотентность
// публикации (ТЗ §4.4 п.5, ФТ-11): повторная доставка уже опубликованной
// записи (сбой XACK после публикации) не даёт вторую запись в стриме —
// фильтр по идентификатору записи результата; доставка подтверждается
// без маркера и без q:analyze, метрика публикаций не растёт (публикации
// не было).
func TestRenderWorker_FallbackDuplicateDelivery(t *testing.T) {
	f := newRWFallbackFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Fallback: FallbackOptions{Enabled: true}, Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// Первая доставка: публикация в фолбэк.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("фолбэк-публикация: %v", err)
	}
	// Дубль: тот же ключ, тот же контент (XACK потерян после публикации).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("дубль доставки подтверждается без ошибки: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	fb := 0
	for _, p := range pubs {
		if p.stream == queue.StreamRenderFallback {
			fb++
		}
	}
	if fb != 1 {
		t.Errorf("записей q:render-fallback = %d, нужна 1 (дубль доставки не дублирует стрим)", fb)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("маркер не ставится ни на публикации, ни на дубле (ФТ-10)")
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "timeout"}); got != 1 {
		t.Errorf("метрика публикаций = %v, want 1 (дубль публикацией не считается)", got)
	}
}

// TestRenderWorker_FallbackNewEpisodeNotFiltered — идентификатор
// фильтра — ключ строки + checked_at эпизода: новый эпизод (перепроверка
// T-058 перезаписала строку) — легитимный новый кандидат, фильтр его не
// знает; в частности контентно-адресный content_hash НЕ идентифицирует
// эпизод — новый эпизод с побайтово тем же контентом (хэш совпал,
// ревью T-238 P1) обязан проходить фолбэк заново.
func TestRenderWorker_FallbackNewEpisodeNotFiltered(t *testing.T) {
	f := newRWFallbackFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("фолбэк-публикация (эпизод 1): %v", err)
	}
	// Новый эпизод: перезапись строки T-058 — новый checked_at; контент
	// побайтово тот же (content_hash совпал — контентный дедуп T-056):
	// старый ключ фильтра обязан не узнать запись.
	fresh := time.Now().Add(time.Hour)
	f.pool.rows[rwKey].checkedAt = fresh

	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("фолбэк-публикация (эпизод 2, тот же контент): %v", err)
	}
	fb := 0
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamRenderFallback {
			fb++
		}
	}
	if fb != 2 {
		t.Errorf("записей q:render-fallback = %d, нужно 2 (эпизоды различаются checked_at, не хэшем)", fb)
	}
}

// TestRenderWorker_FallbackRenderedRowSkipsPublish — строка уже
// отрендерена (воркер B доработал её между публикацией и поздним
// дублем доставки; конкурентный рендер): публикация не нужна — доставка
// подтверждается без маркера, без q:analyze и без второй записи в
// фолбэке (окно лишнего повторного рендера закрыто, ревью T-238 P3).
func TestRenderWorker_FallbackRenderedRowSkipsPublish(t *testing.T) {
	f := newRWFallbackFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.pool.rows[rwKey].rendered = true // доработал воркер B
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("отрендеренная строка — не ошибка: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно (строка готова): %+v", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("маркер отрендеренной строке не нужен")
	}
}

// TestRenderWorker_FallbackNoRowNoURL — строки нет или у неё нет
// итогового URL: фолбэку нечего дорабатывать — штатный отказ без
// публикаций (как у финализации), доставка подтверждается. Оба уровня:
// композит processDelivery (ранний отказ ProcessResult) и прямые
// guard'ы publishFallback (строка исчезла между рендером и публикацией).
func TestRenderWorker_FallbackNoRowNoURL(t *testing.T) {
	f := newRWFallbackFixture(t)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// Композит: ProcessResult выходит до рендера — не ошибка.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("строки нет — не ошибка: %v", err)
	}
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("без итогового URL — не ошибка: %v", err)
	}
	// Прямые guard'ы publishFallback (строку убрала перепроверка после
	// ошибки рендера): (false, nil) — вызывающий финализирует, там тот
	// же отказ.
	delete(f.pool.rows, rwKey)
	handled, err := f.w.publishFallback(context.Background(), rwTask, errors.New("тарпит"))
	if err != nil || handled {
		t.Fatalf("publishFallback без строки = (%v, %v), хочу (false, nil)", handled, err)
	}
	f.pool.seedRow(rwKey, "", nil)
	handled, err = f.w.publishFallback(context.Background(), rwTask, errors.New("тарпит"))
	if err != nil || handled {
		t.Fatalf("publishFallback без итогового URL = (%v, %v), хочу (false, nil)", handled, err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// Таблица состояний функции-решения (кандидат × гейт × бюджет) —
// TestRenderWorker_FallbackDecision, fallbackgate_test.go (T-239):
// стаб-тест always-publish T-238 заменён реализацией гейта/бюджета.

// TestRenderWorker_FallbackDedupEviction — ёмкость фильтра: при
// превышении вытесняется старейший ключ (его дубль становится
// допустимым — гасится консьюмером B, ФТ-11), недавние остаются.
func TestRenderWorker_FallbackDedupEviction(t *testing.T) {
	d := newFallbackDedup()
	if !d.claim("a") {
		t.Fatal("первая фиксация «a» обязана проходить")
	}
	for i := 0; i < fallbackDedupCap; i++ {
		if !d.claim(fmt.Sprintf("k%d", i)) {
			t.Fatalf("фиксация k%d не должна вытеснять соседей", i)
		}
	}
	if !d.claim("a") {
		t.Error("«a» вытеснена (старейшая) — повторная фиксация проходит, дубль становится допустимым (ФТ-11)")
	}
	if !d.claim("b") {
		t.Fatal("первая фиксация «b» обязана проходить")
	}
	if d.claim("b") {
		t.Fatal("повторная фиксация «b» — дубль, обязана быть отклонена")
	}
}

// TestRenderWorker_FallbackDedupRelease — release снимает фиксацию
// неудавшейся публикации: повторная доставка сохраняет право на
// публикацию (иначе задача терялась бы между фильтром и стримом);
// незнакомый ключ — no-op.
func TestRenderWorker_FallbackDedupRelease(t *testing.T) {
	d := newFallbackDedup()
	d.release("unknown") // no-op, не паникует
	if !d.claim("k") {
		t.Fatal("первая фиксация обязана проходить")
	}
	d.release("k")
	if !d.claim("k") {
		t.Fatal("после release повторная публикация обязана проходить")
	}
	if d.claim("k") {
		t.Fatal("без release повторная фиксация — дубль")
	}
}

// TestRenderWorker_FallbackDedupKey — идентификатор записи результата:
// ключ строки + checked_at эпизода (стабилен в жизни эпизода, меняется
// перезаписью): разные записи, разные эпизоды — разные идентификаторы.
func TestRenderWorker_FallbackDedupKey(t *testing.T) {
	ts := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	other := rwTask
	other.DomainID++
	if fallbackDedupKey(rwTask, ts) == fallbackDedupKey(other, ts) {
		t.Error("разные записи — разные идентификаторы")
	}
	if fallbackDedupKey(rwTask, ts) == fallbackDedupKey(rwTask, ts.Add(time.Second)) {
		t.Error("разные эпизоды (checked_at) — разные идентификаторы")
	}
	if fallbackDedupKey(rwTask, ts) == fallbackDedupKey(rwTask, time.Time{}) {
		t.Error("нулевой checked_at — тоже значение идентификатора (не совпадает)")
	}
}

// TestRenderWorker_FallbackConsumedMetrics — учёт потребления фолбэка
// воркером B (T-245, ТЗ §4.6 п.2, ФТ-15): счётчик
// crawler_render_fallback_consumed_total растёт только у сборки со
// стримом q:render-fallback (Options.Stream, ФТ-12) и только на
// подтверждаемых доставках — по исходам: rendered (успешная доработка:
// DOM записан, q:analyze), finalized (финализация с маркером —
// исчерпание фолбэка, ФТ-10), skipped (строки нет — без побочных
// эффектов). Основная сборка (q:render) счётчик не трогает: серий нет —
// потребление фолбэка учитывает только дорабатывающий воркер. Баланс
// published = consumed + остаток q:render-fallback — шаг 4 приёмки
// ТЗ §6.2 (интеграционная сверка — TestAccept03_RenderFallback).
func TestRenderWorker_FallbackConsumedMetrics(t *testing.T) {
	// consumedSeries — значение серии исхода (-1 — серии нет).
	consumedSeries := func(t *testing.T, g prometheus.Gatherer, result string) float64 {
		t.Helper()
		return rwMetricValue(t, g, "crawler_render_fallback_consumed_total", map[string]string{"result": result})
	}

	t.Run("успех доработки — rendered", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-fallback-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			Stream:  queue.StreamRenderFallback, // сборка воркера B (T-236)
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.output = rwOutput("<html><body>dom</body></html>")

		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		if got := consumedSeries(t, reg, fbResultRendered); got != 1 {
			t.Errorf("crawler_render_fallback_consumed_total{rendered} = %v, want 1", got)
		}
		for _, result := range []string{fbResultFinalized, fbResultSkipped} {
			if got := consumedSeries(t, reg, result); got != -1 {
				t.Errorf("crawler_render_fallback_consumed_total{%s} = %v, want -1 (серии нет)", result, got)
			}
		}
	})

	t.Run("исчерпание фолбэка — finalized", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-fallback-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			Stream:  queue.StreamRenderFallback, // сборка воркера B (T-236)
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwTarpitErr("https://spa.example/")

		// Тарпит на исчерпании: воркер B не публикует рекурсивный фолбэк
		// (ФТ-10) — финализация с маркером (TestRenderWorker_
		// FallbackNoRecursionWorkerB) = потребление finalized.
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		// Cert-класс — другой вызов fbConsume в processDelivery (та же
		// семантика finalized, путь T-159 с первой доставки).
		f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
		if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 1); err != nil {
			t.Fatalf("processDelivery (cert): %v", err)
		}
		if got := consumedSeries(t, reg, fbResultFinalized); got != 2 {
			t.Errorf("crawler_render_fallback_consumed_total{finalized} = %v, want 2 (тарпит + cert)", got)
		}
		if got := consumedSeries(t, reg, fbResultRendered); got != -1 {
			t.Errorf("crawler_render_fallback_consumed_total{rendered} = %v, want -1 (серии нет)", got)
		}
	})

	t.Run("ошибка доставки не считается — серия не создаётся", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-fallback-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			Stream:  queue.StreamRenderFallback, // сборка воркера B (T-236)
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		// Класс браузера (движок недоступен) — инфраструктурная ошибка:
		// доставка без XACK остаётся в PEL (клейм/bury — очередь),
		// потреблением не считается — ключевой инвариант баланса
		// published = consumed + остаток (шаг 4 приёмки ТЗ §6.2).
		f.renderer.err = &render.Error{Class: render.ClassBrowser, URL: "https://spa.example/",
			Err: errors.New("chromium недоступен")}

		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
			t.Fatal("ошибка класса браузера — ошибка доставки (повторная доставка), не успех")
		}
		for _, result := range []string{fbResultRendered, fbResultFinalized, fbResultSkipped} {
			if got := consumedSeries(t, reg, result); got != -1 {
				t.Errorf("crawler_render_fallback_consumed_total{%s} = %v, want -1 (ошибочная доставка не считается)", result, got)
			}
		}
	})

	t.Run("строки нет — skipped", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-fallback-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			Stream:  queue.StreamRenderFallback, // сборка воркера B (T-236)
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		// Строку не сеем: перепроверка переписала эпизод между публикацией
		// и потреблением — доставка подтверждается без рендера и маркера,
		// но потреблением считается (баланс не расходится).
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		if got := consumedSeries(t, reg, fbResultSkipped); got != 1 {
			t.Errorf("crawler_render_fallback_consumed_total{skipped} = %v, want 1", got)
		}
	})

	t.Run("основная сборка (q:render) потребление не считает", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			// Stream — дефолт q:render (основная сборка).
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.output = rwOutput("<html><body>dom</body></html>")

		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		for _, result := range []string{fbResultRendered, fbResultFinalized, fbResultSkipped} {
			if got := consumedSeries(t, reg, result); got != -1 {
				t.Errorf("crawler_render_fallback_consumed_total{%s} = %v, want -1 (считает только воркер B)", result, got)
			}
		}
	})
}

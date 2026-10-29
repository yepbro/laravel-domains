// Юнит-тесты политики последней попытки × класс задачи (T-172): ошибка
// рендера класса task (отказ соединения, load error — свойство хоста, не
// инфраструктуры) на доставке с номером redis.max_attempts финализирует
// запись без рендера по образцу тарпитов T-148 вместо захоронения в
// q:dead: итог — честный сырой HTML HTTP-фазы (rendered = false),
// публикация q:analyze (Fresh=true), подтверждение доставки. Ниже
// последней попытки ошибка повторяема (восстановление хостов наблюдается:
// замер T-172 — 6.6% ретраев ERR_CONNECTION_REFUSED успешны);
// инфраструктурный класс (БД, хранилище, публикация, браузер, отмена ctx)
// умирает прежним путём — q:dead остаётся настоящим инфраструктурным
// ошибкам. Фейковое окружение — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// TestRenderWorker_TaskFinalizedOnLastAttempt — суть T-172, сценарий
// инцидента 23.10 (смешанная серия): таймаут на первой доставке (тарпит
// ниже лимита T-148 — повторяемая ошибка), отказ соединения на второй
// (ранняя попытка — повторяемая), отказ на третьей = последней
// (redis.max_attempts) — финализация без рендера, не q:dead: обработка
// успешна (Processing подтверждает доставку), строка остаётся с сырым
// HTML (rendered = false, content_hash не меняется), публикуется
// q:analyze (Fresh=true), Put/транзакция не выполняются.
func TestRenderWorker_TaskFinalizedOnLastAttempt(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)

	// Доставка 1: таймаут страницы — тарпит ниже TimeoutAttempts (2),
	// территория T-148: ошибка повторной доставки.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
		t.Fatal("таймаут ниже тарпит-лимита — ошибка повторной доставки (T-148)")
	}

	// Доставка 2: отказ соединения — ранняя попытка, хост может подняться
	// (замер: восстановление наблюдается) — ошибка повторной доставки.
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("отказ соединения до последней попытки — ошибка (хосту даётся шанс подняться)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("публикаций до финализации быть не должно: %+v", pubs)
	}

	// Доставка 3 = MaxAttempts: та же ошибка — финализация без рендера.
	err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 3, f.w.processDelivery)
	if err != nil {
		t.Fatalf("ошибка класса задачи на последней попытке обязана финализироваться без ошибки (не q:dead): %v", err)
	}

	acked := f.q.ackedSnapshot()
	if len(acked) != 1 || acked[0] != "e1" {
		t.Errorf("XACK = %v, нужна e1 (финализация подтверждает доставку)", acked)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.rendered {
		t.Error("rendered должен остаться false — итог записи сырой HTML HTTP-фазы")
	}
	if row.failedAt == nil {
		t.Error("render_failed_at обязан ставиться финализацией (T-210) — иначе проход доработки переиздаёт запись")
	}
	if row.hash == nil || string(row.hash) != string(raw[:]) {
		t.Errorf("content_hash = %v, нужна прежняя ссылка на сырой HTML %s", row.hash, raw)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze (конвейер продолжается анализом)", pubs)
	}
	if !pubs[0].msg.Fresh {
		t.Error("q:analyze Fresh=true — финализация дорабатывает текущий эпизод, как успешный рендер")
	}
	if pubs[0].msg.ZoneID != rwTask.ZoneID || pubs[0].msg.DomainID != rwTask.DomainID ||
		pubs[0].msg.SubdomainID != rwTask.SubdomainID || pubs[0].msg.Variant != rwTask.Variant {
		t.Errorf("q:analyze ключ %+v не совпадает с задачей", pubs[0].msg)
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не должен вызываться: %d", len(puts))
	}
	if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
		t.Errorf("транзакция не должна открываться (событие на %d)", i)
	}
	// Refcount не тронут: сырой HTML остаётся итоговым контентом записи.
	if refc := rwRefc(f.pool)[raw]; refc != 1 {
		t.Errorf("refcount сырого HTML = %d, нужен 1 (ссылка жива)", refc)
	}
}

// TestRenderWorker_TaskBelowLastAttemptRetries — ретраи на ранних
// попытках не тронуты: ошибка класса задачи ниже MaxAttempts — обычная
// повторимая ошибка (без XACK, вернётся клеймом), как до T-172.
func TestRenderWorker_TaskBelowLastAttemptRetries(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

	for attempt := 1; attempt <= 2; attempt++ {
		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), attempt, f.w.processDelivery)
		if err == nil {
			t.Fatalf("attempts=%d < MaxAttempts=3: ошибка повторяема (путь q:dead не заменён раньше времени)", attempt)
		}
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — до последней попытки подтверждения нет", acked)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_TaskFinalizeBoundary — граница настраивается полем
// Options.MaxAttempts (боевая сборка передаёт redis.max_attempts): при 1
// финализация уже на первой доставке, при 5 — четвёртая ещё повторяема.
func TestRenderWorker_TaskFinalizeBoundary(t *testing.T) {
	newFix := func(t *testing.T, max int) *rwFixture {
		t.Helper()
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{MaxAttempts: max})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return f
	}
	t.Run("лимит 1 — финализация на первой доставке", func(t *testing.T) {
		f := newFix(t, 1)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("лимит 1: первая доставка — задача-класс — финализация: %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("публикаций q:analyze = %d, нужна 1", len(pubs))
		}
	})
	t.Run("лимит 5 — четвёртая доставка ещё повторяема", func(t *testing.T) {
		f := newFix(t, 5)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 4); err == nil {
			t.Fatal("лимит 5: четвёртая доставка ниже лимита — ошибка повторной доставки")
		}
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 5); err != nil {
			t.Fatalf("лимит 5: пятая доставка — финализация: %v", err)
		}
	})
}

// TestRenderWorker_TaskFinalizeInfraStillDies — инфраструктурный класс
// не финализируется и на последней попытке: умирает прежним путём
// (без XACK → q:dead) — браузер, контекст, отмена вызывающего контекста
// (не свойство хоста), ошибки БД/хранилища/публикации (не ошибки
// рендера — render.Error в цепочке отсутствует), пустой результат
// (нарушение контракта T-064 — на разбор оператору).
func TestRenderWorker_TaskFinalizeInfraStillDies(t *testing.T) {
	cases := []struct {
		name string
		set  func(f *rwFixture)
	}{
		{"браузер", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassBrowser, URL: "https://spa.example/", Err: errors.New("canceled")}
		}},
		{"контекст", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassContext, URL: "https://spa.example/", Err: errors.New("Session not found")}
		}},
		{"отмена вызывающего контекста (класс task, но не свойство хоста)", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassTask, URL: "https://spa.example/",
				Err: fmt.Errorf("задача отменена: %w: %w", context.Canceled, errors.New("canceled"))}
		}},
		{"сбой БД (не ошибка рендера)", func(f *rwFixture) {
			f.pool.queryErr = errors.New("база недоступна")
		}},
		{"сбой хранилища (не ошибка рендера)", func(f *rwFixture) {
			f.renderer.output = rwOutput("<html>dom</html>")
			f.store.putErr = errors.New("диск полон")
		}},
		{"пустой результат рендера (контракт T-064)", func(f *rwFixture) {
			f.renderer.output = render.RenderOutput{FinalURL: "https://spa.example/x"} // DOM пуст
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			c.set(f)

			// Последняя доставка (3 = MaxAttempts): инфраструктурный класс
			// обязан возвращаться — путь q:dead прежний.
			err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 3, f.w.processDelivery)
			if err == nil {
				t.Fatal("инфраструктурная ошибка обязана возвращаться и на последней попытке (путь q:dead)")
			}
			if acked := f.q.ackedSnapshot(); len(acked) != 0 {
				t.Errorf("XACK = %v — инфраструктурная ошибка не подтверждается", acked)
			}
			if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
				t.Errorf("публикаций быть не должно: %+v", pubs)
			}
		})
	}
}

// TestRenderWorker_TaskFinalizeNoRow — строки нет (перепроверка
// перезаписала) или у неё нет итогового URL: финализация — штатный отказ
// без публикации, доставка подтверждается (как ProcessResult).
func TestRenderWorker_TaskFinalizeNoRow(t *testing.T) {
	f := newRWFixture(t)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

	// Строки нет вовсе.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("строки нет — не ошибка: %v", err)
	}
	// Строка без итогового URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 3); err != nil {
		t.Fatalf("без итогового URL — не ошибка: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_TaskFinalizePublishError — сбой публикации q:analyze
// при финализации: ошибка без XACK — доставка вернётся и финализация
// повторится (at-least-once, дубль публикации ограничен попытками,
// анализатор идемпотентен по ключу).
func TestRenderWorker_TaskFinalizePublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err == nil {
		t.Fatal("сбой публикации q:analyze при финализации — ошибка (повторная доставка)")
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — подтверждения быть не должно", acked)
	}
	// Повтор идемпотентен: та же финализация, публикация проходит.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("повторная доставка после сбоя публикации обязана финализироваться: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 || pubs[0].stream != queue.StreamAnalyze || pubs[1].stream != queue.StreamAnalyze {
		t.Errorf("публикации %+v, нужны две q:analyze (до сбоя и на повторе)", pubs)
	}
	if !pubs[1].msg.Fresh {
		t.Error("q:analyze Fresh=true — доработка текущего эпизода")
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
		t.Error("rendered должен остаться false")
	}
}

// TestRenderWorker_TaskFinalizeMetrics — финализация на последней
// попытке считается задачей этапа, ошибкой класса task и отдельным
// счётчиком с меткой причины (код net::ERR_*); счётчики тарпитов и
// сертификатов не задеты.
func TestRenderWorker_TaskFinalizeMetrics(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("финализация: %v", err)
	}
	stage := map[string]string{"stage": "render"}
	if got := rwMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
		t.Errorf("crawler_stage_tasks_total = %v, want 1 (финализация — обработанная задача)", got)
	}
	labels := map[string]string{"stage": "render", "type": render.ClassTask.String()}
	if got := rwMetricValue(t, reg, "crawler_stage_errors_total", labels); got != 1 {
		t.Errorf("crawler_stage_errors_total{task} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": "net::ERR_CONNECTION_REFUSED"}); got != 1 {
		t.Errorf("crawler_render_task_finalized_total{reason=net::ERR_CONNECTION_REFUSED} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": "net::ERR_NAME_NOT_RESOLVED"}); got != -1 {
		t.Errorf("чужая метка причины = %v, want −1 (нет серии)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 0 (не тарпит)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 0 (не cert)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (DOM не записан)", got)
	}
}

// TestRenderWorker_TaskFinalizeReason — метка причины: код ошибки
// загрузки net::ERR_*, «other» для задачи без кода (сбой страницы),
// «timeout» для таймаута, не пойманного тарпит-лимитом (страховка сборок
// с TimeoutAttempts выше MaxAttempts — конфиг это запрещает).
func TestRenderWorker_TaskFinalizeReason(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED"), "net::ERR_CONNECTION_REFUSED"},
		{rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED"), "net::ERR_NAME_NOT_RESOLVED"},
		{&render.Error{Class: render.ClassTask, URL: "https://spa.example/", Err: errors.New("session crashed")}, "other"},
		{rwTarpitErr("https://spa.example/"), "timeout"},
		{&render.Error{Class: render.ClassContext, URL: "https://spa.example/", Err: errors.New("x")}, "other"},
	}
	for _, c := range cases {
		if got := taskFinalizeReason(c.err); got != c.want {
			t.Errorf("taskFinalizeReason(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

// TestRenderWorker_TaskFinalizeTarpitPriority — тарпит на последней
// попытке финализируется тарпит-веткой T-148 (счётчик тарпитов), а не
// политикой T-172: порядок веток processDelivery сохраняется.
func TestRenderWorker_TaskFinalizeTarpitPriority(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("финализация: %v", err)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 1 (таймаут — территория T-148)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total имеет серии без причины: %v", got)
	}
}

// TestRenderWorker_DateInvalidCertFromFirstDelivery — просроченный
// сертификат (расширение T-172): финализируется cert-политикой с первой
// доставки (замер: успешных ретраев 0 из 321), а не политикой последней
// попытки и не q:dead.
func TestRenderWorker_DateInvalidCertFromFirstDelivery(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_DATE_INVALID")

	if err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery); err != nil {
		t.Fatalf("DATE_INVALID первой доставки — cert-финализация (не q:dead): %v", err)
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 1 || acked[0] != "e1" {
		t.Errorf("XACK = %v, нужна e1", acked)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Errorf("публикации %+v, нужна одна q:analyze", pubs)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 1 (cert-класс)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total не должен расти: %v", got)
	}
}

// TestRenderWorker_MaxAttemptsDefault — дефолт воркера согласован с
// очередью (queue.DefaultMaxAttempts): боевая сборка передаёт
// redis.max_attempts, расхождение дефолтов молча меняло бы границу
// финализации против захоронения Reliab.
func TestRenderWorker_MaxAttemptsDefault(t *testing.T) {
	o := Options{}.withDefaults()
	if o.MaxAttempts != queue.DefaultMaxAttempts {
		t.Errorf("withDefaults: max_attempts=%d, дефолт очереди %d", o.MaxAttempts, queue.DefaultMaxAttempts)
	}
}

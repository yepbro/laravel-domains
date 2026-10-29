// Юнит-тесты политики DNS-отказа на рендере (T-211, по итогам T-207):
// ERR_NAME_NOT_RESOLVED — смерть хоста на уровне DNS. Запись попала в
// q:render через живой HTTP-эпизод (HTTP-фаза сама резолвила имя), а
// браузер резолвит тем же системным unbound — отказ на рендере значит,
// DNS хоста умер между эпизодом и рендером; возврат DNS — масштаб дней,
// окно ретраев (3 доставки, ~20 мин в PEL) его не покрывает (замер
// T-207: 252 финализации за ~7 ч, каждая съедала все 3 доставки).
// DNS-класс финализируется без рендера с первой же доставки — по
// образцу cert-класса T-159 (без тарпит-лимита T-148): итог — сырой
// HTML HTTP-фазы (rendered = false), публикация q:analyze,
// подтверждение доставки — q:dead остаётся инфраструктурным ошибкам.
// ERR_CONNECTION_REFUSED в класс не входит: восстановление наблюдается
// (замер T-172: 6.6% ретраев успешны) — повторяем до последней доставки
// (T-172). Фейковое окружение — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"testing"

	"github.com/chromedp/chromedp"

	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// TestRenderWorker_DNSFinalizedWithoutRender — суть T-211: DNS-отказ на
// ПЕРВОЙ доставке финализирует запись без рендера (мёртвый хост не ест
// 3 доставки и ~20 мин PEL): обработка успешна (Processing подтверждает
// доставку), строка остаётся с сырым HTML (rendered = false,
// content_hash не меняется), публикуется q:analyze (Fresh=true —
// доработка текущего эпизода), Put/транзакция не выполняются; маркер
// render_failed_at (T-210) закрывает переиздания прохода до смены
// контента.
func TestRenderWorker_DNSFinalizedWithoutRender(t *testing.T) {
	for _, code := range dnsErrorCodes {
		t.Run(code, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			f.renderer.err = rwCertErr("https://spa.example/", code)

			// Первая доставка (attempts=1) — финализация сразу, а не ошибка
			// повторной доставки: ProcessingAttempt подтверждает после
			// nil-возврата.
			err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery)
			if err != nil {
				t.Fatalf("DNS-отказ первой доставки обязан финализироваться без ошибки (не q:dead): %v", err)
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
		})
	}
}

// TestRenderWorker_DNSFinalizeAnyAttempt — финализация не зависит от
// номера доставки (как у cert-класса): клейм с большим attempts (поздний
// деплой фикса, смешанная история доставок) тоже финализирует, а не
// уводит в q:dead.
func TestRenderWorker_DNSFinalizeAnyAttempt(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")

	for attempt := 1; attempt <= 3; attempt++ {
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), attempt); err != nil {
			t.Fatalf("attempts=%d: DNS-класс финализируется на любом номере доставки: %v", attempt, err)
		}
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 3 {
		t.Errorf("публикаций q:analyze = %d, нужно 3 (по одной на доставку)", len(pubs))
	}
	if acks := len(f.q.ackedSnapshot()); acks != 0 {
		t.Errorf("XACK = %d — processDelivery не подтверждает сам (XACK — Processing)", acks)
	}
}

// TestRenderWorker_DNSFinalizeNoRow — строки нет (перепроверка
// перезаписала) или у неё нет итогового URL: финализация — штатный отказ
// без публикации, доставка подтверждается (как ProcessResult).
func TestRenderWorker_DNSFinalizeNoRow(t *testing.T) {
	f := newRWFixture(t)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")

	// Строки нет вовсе.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("строки нет — не ошибка: %v", err)
	}
	// Строка без итогового URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 1); err != nil {
		t.Fatalf("без итогового URL — не ошибка: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_DNSFinalizePublishError — сбой публикации q:analyze
// при финализации: ошибка без XACK — доставка вернётся и финализация
// повторится (at-least-once, дубль публикации ограничен попытками,
// анализатор идемпотентен по ключу).
func TestRenderWorker_DNSFinalizePublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	// Повторная доставка после сбоя публикации (клейм, attempts вырос).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
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

// TestRenderWorker_DNSNotClassified — прежний путь сохраняется для
// ошибок вне DNS-класса: отказ соединения (решение T-211 по замеру
// T-172: 6.6% ретраев успешны — до последней доставки повторяем, на
// последней финализирует политика T-172, не DNS), код нерезолва в
// чужом классе (контекст умер — инфраструктура), таймаут страницы
// (территория T-148).
func TestRenderWorker_DNSNotClassified(t *testing.T) {
	// Отказ соединения ниже последней попытки — повторяемая ошибка:
	// именно решение T-211 — CONNECTION_REFUSED в DNS-класс не входит.
	t.Run("отказ соединения ниже последней попытки (решение T-211)", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 2, f.w.processDelivery)
		if err == nil {
			t.Fatal("отказ соединения до последней попытки — ошибка (хосту даётся шанс подняться, замер T-172)")
		}
		if acked := f.q.ackedSnapshot(); len(acked) != 0 {
			t.Errorf("XACK = %v — ошибка вне DNS-класса не подтверждается", acked)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("публикаций быть не должно: %+v", pubs)
		}
		if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
			t.Error("rendered должен остаться false")
		}
	})

	// Отказ соединения на последней попытке — финализирует политика
	// T-172 (класс задачи), не DNS: счётчики различают политики.
	t.Run("отказ соединения на последней попытке — T-172, не DNS", func(t *testing.T) {
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

		if err := f.w.processDelivery(context.Background(), rwDelivery("e9"), 3); err != nil {
			t.Fatalf("последняя попытка — финализация T-172: %v", err)
		}
		if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
			map[string]string{"reason": "net::ERR_CONNECTION_REFUSED"}); got != 1 {
			t.Errorf("crawler_render_task_finalized_total{refused} = %v, want 1 (политика T-172)", got)
		}
		if got := rwMetricValue(t, reg, "crawler_render_dns_finalized_total", nil); got != 0 {
			t.Errorf("crawler_render_dns_finalized_total = %v, want 0 (refused — не DNS-класс)", got)
		}
	})

	// Код нерезолва в чужом классе не финализируется и на последней
	// доставке: контекстный класс — инфраструктура (повтор осмыслен).
	t.Run("код нерезолва в чужом классе", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = &render.Error{
			Class: render.ClassContext, URL: "https://spa.example/",
			Err: &chromedp.LoadError{ErrorText: "net::ERR_NAME_NOT_RESOLVED"}}

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 2, f.w.processDelivery)
		if err == nil {
			t.Fatal("код DNS в чужом классе обязан возвращаться (повтор на новом контексте осмыслен)")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("публикаций быть не должно: %+v", pubs)
		}
	})

	// Таймаут страницы остаётся территорией T-148: DNS-распознавание
	// (ProcessResult проверяет его после cert, до тарпита) не должно
	// глотать таймауты — ниже тарпит-лимита это повторяемая ошибка.
	t.Run("таймаут ниже тарпит-лимита (территория T-148)", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwTarpitErr("https://spa.example/")

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery)
		if err == nil {
			t.Fatal("таймаут ниже тарпит-лимита — ошибка повторной доставки (T-148), не DNS-финализация")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("публикаций быть не должно: %+v", pubs)
		}
	})
}

// TestRenderWorker_DNSMetrics — финализация по DNS-отказу считается
// задачей этапа, ошибкой класса task и отдельным счётчиком DNS-класса;
// счётчики тарпитов, сертификатов и task-класса не задеты.
func TestRenderWorker_DNSMetrics(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
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
	if got := rwMetricValue(t, reg, "crawler_render_dns_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_dns_finalized_total = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 0 (DNS ≠ cert)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 0 (DNS ≠ тарпит)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total не должен иметь серий: %v", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (DOM не записан)", got)
	}
}

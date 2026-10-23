// Юнит-тесты политики постоянных ошибок сертификата хоста (T-159,
// расширение T-172): ERR_CERT_AUTHORITY_INVALID /
// ERR_CERT_COMMON_NAME_INVALID — свойства хоста, а не сети или
// инфраструктуры, ретрай бессмысленен; ERR_CERT_DATE_INVALID добавлен по
// замеру T-172 (0 успешных ретраев из 321 первых неудач за сутки — окно
// продления сертификата много шире окна ретраев). Cert-класс
// финализируется без рендера с первой же доставки (без тарпит-лимита
// T-148, которому лимит даёт странице шанс в изменившихся условиях):
// итог — сырой HTML HTTP-фазы (rendered = false), публикация q:analyze,
// подтверждение доставки — q:dead остаётся инфраструктурным ошибкам,
// очередь q:render не копит попытки cert-хостов. Фейковое окружение —
// worker_test.go.
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

// rwCertErr — ошибка рендера, неотличимая от боевой по классификации
// (classifyCtx: сбои загрузки страницы net::ERR_* — класс task, исходная
// ошибка chromedp.LoadError с текстом браузера сохраняется в цепочке).
func rwCertErr(url, code string) error {
	return &render.Error{Class: render.ClassTask, URL: url,
		Err: &chromedp.LoadError{ErrorText: code}}
}

// TestRenderWorker_CertFinalizedWithoutRender — суть T-159: cert-ошибка
// на ПЕРВОЙ доставке финализирует запись без рендера (тарпитам T-148 для
// этого нужен номер ≥ TimeoutAttempts — здесь ожидание бессмысленно):
// обработка успешна (Processing подтверждает доставку), строка остаётся
// с сырым HTML (rendered = false, content_hash не меняется),
// публикуется q:analyze (Fresh=true — доработка текущего эпизода),
// Put/транзакция не выполняются.
func TestRenderWorker_CertFinalizedWithoutRender(t *testing.T) {
	for _, code := range certErrorCodes {
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
				t.Fatalf("cert-ошибка первой доставки обязана финализироваться без ошибки (не q:dead): %v", err)
			}

			acked := f.q.ackedSnapshot()
			if len(acked) != 1 || acked[0] != "e1" {
				t.Errorf("XACK = %v, нужна e1 (финализация подтверждает доставку)", acked)
			}
			row := rwRowSnapshot(f.pool, rwKey)
			if row.rendered {
				t.Error("rendered должен остаться false — итог записи сырой HTML HTTP-фазы")
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

// TestRenderWorker_CertFinalizeAnyAttempt — финализация не зависит от
// номера доставки (противоположность тарпитам T-148): клейм с большим
// attempts (поздний деплой фикса, смешанная история доставок) тоже
// финализирует, а не уводит в q:dead.
func TestRenderWorker_CertFinalizeAnyAttempt(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_COMMON_NAME_INVALID")

	for attempt := 1; attempt <= 3; attempt++ {
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), attempt); err != nil {
			t.Fatalf("attempts=%d: cert-класс финализируется на любом номере доставки: %v", attempt, err)
		}
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 3 {
		t.Errorf("публикаций q:analyze = %d, нужно 3 (по одной на доставку)", len(pubs))
	}
	if acks := len(f.q.ackedSnapshot()); acks != 0 {
		t.Errorf("XACK = %d — processDelivery не подтверждает сам (XACK — Processing)", acks)
	}
}

// TestRenderWorker_CertFinalizeNoRow — строки нет (перепроверка
// перезаписала) или у неё нет итогового URL: финализация — штатный отказ
// без публикации, доставка подтверждается (как ProcessResult).
func TestRenderWorker_CertFinalizeNoRow(t *testing.T) {
	f := newRWFixture(t)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")

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

// TestRenderWorker_CertFinalizePublishError — сбой публикации q:analyze
// при финализации: ошибка без XACK — доставка вернётся и финализация
// повторится (at-least-once, дубль публикации ограничен попытками,
// анализатор идемпотентен по ключу).
func TestRenderWorker_CertFinalizePublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
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

// TestRenderWorker_CertNotClassified — прежний путь сохраняется для
// ошибок вне cert-класса (ошибка без XACK → повторная доставка →
// redis.max_attempts → q:dead, T-025): сетевые коды (refused может быть
// временным — восстановление наблюдается, замер T-172: 6.6% ретраев
// успешны; на последней доставке их финализирует политика T-172),
// обычные net::ERR_*, ошибки других классов. Проверка на любой попытке:
// финализация — только cert-класс.
func TestRenderWorker_CertNotClassified(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"отказ соединения (может быть временным)", rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")},
		{"резолв (может быть временным)", rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")},
		{"код сертификата в чужом классе (контекст умер)", &render.Error{
			Class: render.ClassContext, URL: "https://spa.example/",
			Err: &chromedp.LoadError{ErrorText: "net::ERR_CERT_AUTHORITY_INVALID"}}},
		{"обычная ошибка страницы без кода загрузки", &render.Error{
			Class: render.ClassTask, URL: "https://spa.example/", Err: errors.New("session crashed")}},
		{"браузер", &render.Error{
			Class: render.ClassBrowser, URL: "https://spa.example/", Err: errors.New("canceled")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			f.renderer.err = c.err

			// Номер доставки ниже последней (MaxAttempts 3): не-cert
			// ошибки не финализируются cert-политикой никогда — путь
			// повторной доставки прежний (что с ними на последней
			// доставке — политика T-172, taskfinal_test.go).
			err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 2, f.w.processDelivery)
			if err == nil {
				t.Fatal("ошибка вне cert-класса обязана возвращаться (путь q:dead прежний)")
			}
			if acked := f.q.ackedSnapshot(); len(acked) != 0 {
				t.Errorf("XACK = %v — ошибка вне cert-класса не подтверждается", acked)
			}
			if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
				t.Errorf("публикаций быть не должно: %+v", pubs)
			}
			if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
				t.Error("rendered должен остаться false")
			}
		})
	}

	// Код сертификата в чужом классе не финализируется и на последней
	// доставке: контекстный класс — инфраструктура (T-172 финализирует
	// только класс task).
	t.Run("код сертификата в чужом классе на последней доставке", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = &render.Error{
			Class: render.ClassContext, URL: "https://spa.example/",
			Err: &chromedp.LoadError{ErrorText: "net::ERR_CERT_AUTHORITY_INVALID"}}

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 3, f.w.processDelivery)
		if err == nil {
			t.Fatal("ошибка чужого класса обязана возвращаться и на последней доставке")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("публикаций быть не должно: %+v", pubs)
		}
	})

	// Таймаут страницы остаётся территорией T-148: cert-распознавание
	// (ProcessResult проверяет его первым) не должно глотать таймауты —
	// ниже тарпит-лимита это повторяемая ошибка.
	t.Run("таймаут ниже тарпит-лимита (территория T-148)", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwTarpitErr("https://spa.example/")

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery)
		if err == nil {
			t.Fatal("таймаут ниже тарпит-лимита — ошибка повторной доставки (T-148), не cert-финализация")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("публикаций быть не должно: %+v", pubs)
		}
	})
}

// TestRenderWorker_CertMetrics — финализация по сертификату считается
// задачей этапа, ошибкой класса task и отдельным счётчиком cert-класса;
// тарпит-счётчик не задет.
func TestRenderWorker_CertMetrics(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")

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
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 0 (cert ≠ тарпит)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (DOM не записан)", got)
	}
}

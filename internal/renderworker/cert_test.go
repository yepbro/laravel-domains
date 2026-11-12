// Unit tests of the permanent host certificate error policy (T-159,
// the T-172 extension): ERR_CERT_AUTHORITY_INVALID /
// ERR_CERT_COMMON_NAME_INVALID — host properties, not network or
// infrastructure ones, a retry is pointless; ERR_CERT_DATE_INVALID was added by the
// T-172 measurement (0 successful retries of 321 first failures per day — the
// certificate renewal window is much wider than the retry window). The cert class
// is finalized without a render from the very first delivery (without the T-148 tarpit
// limit, which gives the page a chance in changed conditions):
// the result is the raw HTML of the HTTP phase (rendered = false), a q:analyze publication,
// the delivery acknowledgment — q:dead stays for infrastructure errors,
// the q:render queue does not accumulate cert hosts' attempts. The fake environment —
// worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"testing"

	"github.com/chromedp/chromedp"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// rwCertErr — a render error indistinguishable from the production one by classification
// (classifyCtx: page load failures net::ERR_* — the task class, the original
// chromedp.LoadError error with the browser text is kept in the chain).
func rwCertErr(url, code string) error {
	return &render.Error{Class: render.ClassTask, URL: url,
		Err: &chromedp.LoadError{ErrorText: code}}
}

// TestRenderWorker_CertFinalizedWithoutRender — the essence of T-159: a cert error
// on the FIRST delivery finalizes the entry without a render (the tarpits T-148 need
// a number ≥ TimeoutAttempts for that — here waiting is pointless):
// the processing is successful (Processing acknowledges the delivery), the row stays
// with the raw HTML (rendered = false, content_hash unchanged),
// q:analyze is published (Fresh=true — reworking the current episode),
// Put/the transaction are not executed.
func TestRenderWorker_CertFinalizedWithoutRender(t *testing.T) {
	for _, code := range certErrorCodes {
		t.Run(code, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			f.renderer.err = rwCertErr("https://spa.example/", code)

			// The first delivery (attempts=1) — the finalization right away, not an
			// error of a re-delivery: ProcessingAttempt acknowledges after
			// a nil return.
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
			// The refcount is untouched: the raw HTML stays the entry's final content.
			if refc := rwRefc(f.pool)[raw]; refc != 1 {
				t.Errorf("refcount сырого HTML = %d, нужен 1 (ссылка жива)", refc)
			}
		})
	}
}

// TestRenderWorker_CertFinalizeAnyAttempt — the finalization does not depend on the
// delivery number (the opposite of the tarpits T-148): a claim with a high
// attempts (a late fix deploy, a mixed delivery history) also
// finalizes instead of taking to q:dead.
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

// TestRenderWorker_CertFinalizeNoRow — there is no row (the recheck
// rewrote it) or it has no final URL: the finalization — a regular
// rejection without a publication, the delivery is acknowledged (like ProcessResult).
func TestRenderWorker_CertFinalizeNoRow(t *testing.T) {
	f := newRWFixture(t)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")

	// There is no row at all.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("строки нет — не ошибка: %v", err)
	}
	// A row without a final URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 1); err != nil {
		t.Fatalf("без итогового URL — не ошибка: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_CertFinalizePublishError — a q:analyze publication
// failure during finalization: an error without an XACK — the delivery will return and the
// finalization will repeat (at-least-once, a publication duplicate is bounded by the attempts,
// the analyzer is idempotent by the key).
func TestRenderWorker_CertFinalizePublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	// A re-delivery after the publication failure (a claim, attempts grew).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("сбой публикации q:analyze при финализации — ошибка (повторная доставка)")
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — подтверждения быть не должно", acked)
	}
	// The repeat is idempotent: the same finalization, the publication goes through.
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

// TestRenderWorker_CertNotClassified — the former path is preserved for
// errors outside the cert class (an error without an XACK → a re-delivery →
// redis.max_attempts → q:dead, T-025): network codes (refused may be
// temporary — recovery is observed, the T-172 measurement: 6.6% of retries
// succeed; on the last delivery the T-172 policy finalizes them),
// ordinary net::ERR_*, errors of other classes. The check on any attempt:
// only the cert class is finalized.
func TestRenderWorker_CertNotClassified(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"отказ соединения (может быть временным)", rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")},
		{"таймаут соединения (может быть временным)", rwCertErr("https://spa.example/", "net::ERR_CONNECTION_TIMED_OUT")},
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

			// The delivery number is below the last one (MaxAttempts 3): non-cert
			// errors are never finalized by the cert policy — the
			// re-delivery path is the former one (what happens to them on the last
			// delivery is the T-172 policy, taskfinal_test.go).
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

	// A certificate code in a foreign class is not finalized on the last
	// delivery either: the context class — infrastructure (T-172 finalizes
	// only the task class).
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

	// A page timeout stays the territory of T-148: the cert recognition
	// (ProcessResult checks it first) must not swallow timeouts —
	// below the tarpit limit it is a retryable error.
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

// TestRenderWorker_CertMetrics — a certificate finalization is counted
// as a stage task, a task class error and a separate cert class counter;
// the tarpit counter is not touched.
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
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total",
		map[string]string{"engine": config.EngineChromium}); got != -1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want −1 (cert ≠ тарпит, серия не создаётся)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (DOM не записан)", got)
	}
}

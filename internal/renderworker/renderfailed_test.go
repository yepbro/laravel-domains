// Юнит-тесты маркера «рендер невозможен» subdomain_results.render_failed_at
// (T-210): финализации без рендера (тарпит T-148 / cert-класс T-159 /
// класс задачи T-172) штампуют строку — до смены контента проход
// доработки (httpworker/renderq.go) запись не переиздаёт. Сброс — сменой
// контента: перезапись эпизода (DELETE + INSERT записи результата,
// T-058 — в юнит-тестах httpworker колонка не входит в INSERT) и
// успешный рендер (renderUpdateSQL). Охраны штампа — те же принципы,
// что у renderUpdateSQL: оптимистическая по content_hash (строка ушла
// из-под задачи — маркер не ставится) и rendered = false (конкурентный
// рендер закрыл вопрос). Фейковое окружение — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"testing"

	"crawler/internal/queue"
	"crawler/internal/render"
)

// rwMarkErr — повторимая ошибка рендера класса task (не тарпит/cert —
// ретраи решает processDelivery, здесь важна только финализация).
func rwMarkErr(url string) error {
	return &render.Error{Class: render.ClassTask, URL: url,
		Err: errors.New("net::ERR_CONNECTION_REFUSED")}
}

// TestRenderWorker_FinalizeMarksRenderFailed — все три политики
// финализации ставят маркер: rendered = false, content_hash прежний,
// штамп render_failed_at не nil, q:analyze опубликована, транзакция не
// открывается (маркер — автокоммит-UPDATE, T-210).
func TestRenderWorker_FinalizeMarksRenderFailed(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		attempt int
	}{
		{"тарпит (T-148)", rwTarpitErr("https://spa.example/"), 2},
		{"cert-класс (T-159)", rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID"), 1},
		{"класс задачи (T-172)", rwMarkErr("https://spa.example/"), 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			f.renderer.err = c.err

			if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), c.attempt); err != nil {
				t.Fatalf("финализация: %v", err)
			}
			row := rwRowSnapshot(f.pool, rwKey)
			if row.failedAt == nil {
				t.Error("render_failed_at обязан ставиться финализацией — без маркера проход доработки переиздаёт запись (T-210)")
			}
			if row.rendered {
				t.Error("rendered должен остаться false — итог записи сырой HTML HTTP-фазы")
			}
			if row.hash == nil || string(row.hash) != string(raw[:]) {
				t.Errorf("content_hash = %v, нужна прежняя ссылка на сырой HTML %s", row.hash, raw)
			}
			if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
				t.Errorf("публикации %+v, нужна одна q:analyze (конвейер продолжается анализом)", pubs)
			}
			if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
				t.Errorf("транзакция не должна открываться — маркер автокоммитом (событие на %d)", i)
			}
		})
	}
}

// TestRenderWorker_FinalizeMarkerStaleRow — охрана контента штампа:
// перепроверка перезаписала строку между SELECT цели и маркером — штамп
// НЕ ставится (новый эпизод имеет право на рендер), финализация
// публикует q:analyze текущей строки и подтверждает доставку.
func TestRenderWorker_FinalizeMarkerStaleRow(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
	fresh := rwTestHash(2)
	f.pool.mutateBeforeMark = func(p *rwFakePool) {
		if r, ok := p.rows[rwKey]; ok {
			p.refc[raw]-- // DELETE старых строк эпизода (SwapRef, T-058)
			r.hash = fresh[:]
			p.refc[fresh]++ // INSERT новых строк
		}
	}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("финализация с ушедшей строкой — не ошибка (как охрана рендера): %v", err)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt != nil {
		t.Error("маркер не ставится строке нового контента — охрана content_hash (T-210)")
	}
	if row.hash == nil || string(row.hash) != string(fresh[:]) {
		t.Errorf("content_hash = %v, нужен контент нового эпизода %s", row.hash, fresh)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
		t.Errorf("публикаций q:analyze = %d, нужна 1 (анализ текущей строки безвреден)", len(pubs))
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — processDelivery не подтверждает сам (XACK — Processing)", acked)
	}
}

// TestRenderWorker_FinalizeMarkerRenderedRow — конкурентная повторная
// доставка уже отрендерила строку: штамп ей не нужен (охрана
// rendered = false), финализация публикует q:analyze — дубль анализа
// тем же контентом безвреден (at-least-once, ТЗ 3.4).
func TestRenderWorker_FinalizeMarkerRenderedRow(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	f.pool.mutateBeforeMark = func(p *rwFakePool) {
		p.rows[rwKey].rendered = true // конкурентная доставка записала DOM
	}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("финализация отрендеренной строки — не ошибка: %v", err)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt != nil {
		t.Error("render_failed_at не ставится rendered-строке — вопрос рендера закрыт (инвариант: маркер ⇒ rendered = false)")
	}
	if !row.rendered {
		t.Error("rendered конкурентной доставки должен сохраниться")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
		t.Errorf("публикаций q:analyze = %d, нужна 1", len(pubs))
	}
}

// TestRenderWorker_FinalizeMarkerExecError — сбой UPDATE маркера: ошибка
// без XACK — доставка вернётся и финализация повторится (штамп
// переставится идемпотентно).
func TestRenderWorker_FinalizeMarkerExecError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
	f.pool.markErr = errors.New("нет соединения")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
		t.Fatal("сбой маркера обязан возвращаться (повторная доставка доведёт финализацию)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций до маркера быть не должно: %+v", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("маркер не встал (сбой UPDATE)")
	}
}

// TestRenderWorker_FinalizeMarkerRepeatIdempotent — сбой публикации
// после штампа: доставка возвращается, повторная финализация
// переставляет штамп (UPDATE той же строки) и публикует снова —
// дубль ограничен попытками, анализатор идемпотентен по ключу.
func TestRenderWorker_FinalizeMarkerRepeatIdempotent(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
		t.Fatal("сбой публикации — ошибка повторной доставки")
	}
	first := rwRowSnapshot(f.pool, rwKey)
	if first.failedAt == nil {
		t.Fatal("маркер ставится до публикации (сбой публикации штамп не откатывает)")
	}
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("повторная финализация: %v", err)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt == nil {
		t.Error("маркер обязан стоять после повторной финализации")
	}
	if !row.failedAt.Equal(*first.failedAt) {
		t.Error("coalesce: штамп ПЕРВОЙ финализации не переставляется повтором (диагностика «как давно помечено», T-210)")
	}
	// Две записи публикации: первая (упавшая) тоже попадает в журнал
	// фейка — доставка подтверждается только успешной.
	if pubs := f.q.publishesSnapshot(); len(pubs) != 2 {
		t.Errorf("публикаций q:analyze = %d, нужно 2 (сбойная + доставленная)", len(pubs))
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — processDelivery не подтверждает сам (XACK — Processing)", acked)
	}
}

// TestRenderWorker_RenderClearsMarker — успешный рендер строки с
// маркером (конкурентная доставка финализации): renderUpdateSQL снимает
// render_failed_at вместе с rendered = true — выборка прохода
// доработки видит запись отрендеренной независимо от маркера,
// инвариант «маркер ⇒ rendered = false» сохраняется.
func TestRenderWorker_RenderClearsMarker(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRowMarked(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html><body>rendered anyway</body></html>")

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil || !written {
		t.Fatalf("ProcessResult: written=%v err=%v", written, err)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if !row.rendered {
		t.Error("rendered должен быть true (успешный рендер)")
	}
	if row.failedAt != nil {
		t.Error("render_failed_at обязан сниматься успешным рендером (renderUpdateSQL, T-210)")
	}
}

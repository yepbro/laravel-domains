// Юнит-тесты панели движков рендера (T-246, renders.go): разбор
// exposition text format (метки, кавычки/запятые в значениях, деградации
// формы), агрегация экспозиций двух сборок воркера по движкам и исходам,
// темпы между проходами (часы CollectorOptions.Now, рестарт воркера —
// нулевая дельта), глубина q:render-fallback, деградации секции
// (адреса не заданы — «н/д» без ошибки; экспозиция недоступна — ошибка).
package dashboard

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"crawler/internal/queue"
)

// --- разбор exposition -----------------------------------------------------------

// TestParsePromExposition — минимальный парсер: сэмплы с метками и без,
// HELP/TYPE пропускаются, экраны значений, табличные ошибки формы.
func TestParsePromExposition(t *testing.T) {
	expo := `# HELP crawler_render_records_total Записи по признаку rendered
# TYPE crawler_render_records_total counter
crawler_render_records_total{rendered="true",engine="moli"} 1000
crawler_render_records_total{rendered="false"} 42
crawler_render_tarpit_finalized_total 7
weird_metric{a="x,y",b="q\"r"} 1.5
`
	samples, err := parsePromExposition(strings.NewReader(expo))
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if len(samples) != 4 {
		t.Fatalf("сэмплов = %d, want 4 (HELP/TYPE пропущены):\n%+v", len(samples), samples)
	}
	byName := map[string]promSample{}
	for _, s := range samples {
		byName[s.Name+"|"+s.Labels["rendered"]] = s
	}
	rec := byName["crawler_render_records_total|true"]
	if rec.Value != 1000 || rec.Labels["engine"] != "moli" {
		t.Errorf("запись с метками = %+v, want engine=moli 1000", rec)
	}
	if s := byName["crawler_render_records_total|false"]; s.Value != 42 || s.Labels["engine"] != "" {
		t.Errorf("запись rendered=false = %+v, want 42 без engine", s)
	}
	if s := byName["crawler_render_tarpit_finalized_total|"]; s.Value != 7 {
		t.Errorf("сэмпл без меток = %+v, want 7", s)
	}
	// Запятая внутри кавычек — часть значения, экранированная кавычка —
	// раскрывается.
	w := byName["weird_metric|"]
	if w.Labels["a"] != "x,y" || w.Labels["b"] != `q"r` || w.Value != 1.5 {
		t.Errorf("метки с запятой/кавычкой = %+v", w.Labels)
	}

	for _, tc := range []struct{ name, expo, wantErr string }{
		{"метки без закрывающей", `m{x="1" 5`, "закрывающей"},
		{"нет значения", `m{x="1"}`, "нет значения"},
		{"значение не число", `m{x="1"} abc`, "значение"},
		{"метка без кавычек", `m{x=1} 5`, "кавычках"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parsePromExposition(strings.NewReader(tc.expo)); err == nil ||
				!strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
			}
		})
	}
}

// --- сборка секции ---------------------------------------------------------------

// renderLens — StreamLener по карте глубин (глубина q:render-fallback
// панели не обязана следовать seq fakeLenner секции очередей).
type renderLens struct {
	lens map[string]int64
	err  error
}

func (l *renderLens) Len(_ context.Context, stream string) (int64, error) {
	if l.err != nil {
		return 0, l.err
	}
	return l.lens[stream], nil
}

// renderWorkerExpo — маленькая управляемая экспозиция /metrics рендер-
// воркера: значения меняются между проходами (темп), рестарт — сброс.
type renderWorkerExpo struct {
	engine            string
	rendered          float64
	finalized         float64
	published         float64
	consumedRendered  float64
	consumedFinalized float64
	consumedSkipped   float64
}

func (w renderWorkerExpo) exposition() string {
	var b strings.Builder
	b.WriteString("# TYPE crawler_render_records_total counter\n")
	b.WriteString("crawler_render_records_total{rendered=\"true\",engine=\"" + w.engine + "\"} " +
		formatFloat(w.rendered) + "\n")
	b.WriteString("crawler_render_records_total{rendered=\"false\"} 999\n")
	b.WriteString("crawler_render_task_finalized_total{reason=\"timeout\",engine=\"" + w.engine + "\"} " +
		formatFloat(w.finalized) + "\n")
	if w.published > 0 {
		b.WriteString("crawler_render_fallback_published_total{reason=\"timeout\"} " +
			formatFloat(w.published) + "\n")
	}
	if w.consumedRendered+w.consumedFinalized+w.consumedSkipped > 0 {
		b.WriteString("crawler_render_fallback_consumed_total{result=\"rendered\"} " +
			formatFloat(w.consumedRendered) + "\n")
		b.WriteString("crawler_render_fallback_consumed_total{result=\"finalized\"} " +
			formatFloat(w.consumedFinalized) + "\n")
		b.WriteString("crawler_render_fallback_consumed_total{result=\"skipped\"} " +
			formatFloat(w.consumedSkipped) + "\n")
	}
	return b.String()
}

// formatFloat — компактная запись числа для экспозиции (целые без
// хвоста, дроби как есть).
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// newRenderCollector — сборщик с управляемыми часами (темпы считаются
// между проходами) и картой глубин; set — перевод часов.
func newRenderCollector(t *testing.T, lens *renderLens, urls []string) (*Collector, func(time.Time)) {
	t.Helper()
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	set := func(t time.Time) { now = t }
	coll := newSnapCollector(t, healthyPool(), lens, func(o *CollectorOptions) {
		o.RenderMetrics = urls
		o.Now = clock
	})
	return coll, set
}

// TestCollectRenderPanel — агрегация двух сборок воркера: движки по
// метке engine (сортировка по имени), успешность по атрибутированным
// завершениям, фолбэк published/consumed/глубина, темпы — дельта между
// проходами (первый проход — база, темпы 0).
func TestCollectRenderPanel(t *testing.T) {
	// Экспозиции читаются горутинами сервера, тест мутирует значения
	// между проходами — замок (гонка под -race).
	var mu sync.Mutex
	workerA := renderWorkerExpo{engine: "moli", rendered: 1000, finalized: 100, published: 10}
	setA := func(w renderWorkerExpo) {
		mu.Lock()
		defer mu.Unlock()
		workerA = w
	}
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		expo := workerA.exposition()
		mu.Unlock()
		_, _ = w.Write([]byte(expo))
	}))
	defer srvA.Close()
	workerB := renderWorkerExpo{engine: "chromium", rendered: 50,
		consumedRendered: 5, consumedFinalized: 1, consumedSkipped: 2}
	setB := func(w renderWorkerExpo) {
		mu.Lock()
		defer mu.Unlock()
		workerB = w
	}
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		expo := workerB.exposition()
		mu.Unlock()
		_, _ = w.Write([]byte(expo))
	}))
	defer srvB.Close()

	lens := &renderLens{lens: map[string]int64{queue.StreamRenderFallback: 3}}
	coll, setClock := newRenderCollector(t, lens, []string{srvA.URL, srvB.URL})
	ctx := context.Background()

	// Первый проход: база темпов (0), движки отсортированы, успешность
	// moli = 1000/1100.
	snap := coll.CollectOnce(ctx)
	if snap.Render == nil {
		t.Fatal("render = nil, want секция собрана")
	}
	if len(snap.Errors) != 0 {
		t.Fatalf("errors = %v, want пусто", snap.Errors)
	}
	if len(snap.Render.Engines) != 2 ||
		snap.Render.Engines[0].Engine != "chromium" ||
		snap.Render.Engines[1].Engine != "moli" {
		t.Fatalf("engines = %+v, want chromium,moli по алфавиту", snap.Render.Engines)
	}
	moli := snap.Render.Engines[1]
	if moli.RenderedTotal != 1000 || moli.FinalizedTotal != 100 {
		t.Errorf("moli кумулятивы = %+v, want rendered 1000 finalized 100", moli)
	}
	if moli.SuccessPct == nil || math.Abs(*moli.SuccessPct-1000.0/1100*100) > 1e-9 {
		t.Errorf("moli success = %v, want 1000/1100", moli.SuccessPct)
	}
	chromium := snap.Render.Engines[0]
	if chromium.RenderedTotal != 50 || chromium.FinalizedTotal != 0 || chromium.SuccessPct == nil || *chromium.SuccessPct != 100 {
		t.Errorf("chromium = %+v, want 50/0 и 100%%", chromium)
	}
	for _, e := range snap.Render.Engines {
		if e.RenderedPerSec != 0 {
			t.Errorf("%s: темп первого прохода = %v, want 0 (база)", e.Engine, e.RenderedPerSec)
		}
	}
	fb := snap.Render.Fallback
	if fb.PublishedTotal != 10 || fb.PublishedPerSec != 0 {
		t.Errorf("published = %+v, want 10 и темп 0 (база)", fb)
	}
	if fb.ConsumedTotal != 8 || fb.ConsumedPerSec != 0 {
		t.Errorf("consumed = %+v, want 8 и темп 0 (база)", fb)
	}
	if fb.ConsumedByResult["rendered"] != 5 ||
		fb.ConsumedByResult["finalized"] != 1 || fb.ConsumedByResult["skipped"] != 2 {
		t.Errorf("consumed по исходам = %v, want 5/1/2", fb.ConsumedByResult)
	}
	if fb.Depth == nil || *fb.Depth != 3 {
		t.Errorf("depth = %v, want 3", fb.Depth)
	}

	// Второй проход через 10 с: moli +100 рендеров и +2 публикации —
	// темпы 10/с и 0.2/с; воркер B доработал 2 записи — темп потребления
	// 0.2/с (пара темпов — диагностика «воркер B не успевает», ревью
	// T-246); rendered движков без движения — 0.
	setA(renderWorkerExpo{engine: "moli", rendered: 1100, finalized: 100, published: 12})
	setB(renderWorkerExpo{engine: "chromium", rendered: 50,
		consumedRendered: 7, consumedFinalized: 1, consumedSkipped: 2})
	setClock(time.Date(2026, 11, 6, 12, 0, 10, 0, time.UTC))
	snap = coll.CollectOnce(ctx)
	if snap.Render == nil {
		t.Fatal("второй проход: render = nil")
	}
	moli = snap.Render.Engines[1]
	if moli.RenderedPerSec != 10 {
		t.Errorf("moli темп = %v, want 10 (100 рендеров за 10 с)", moli.RenderedPerSec)
	}
	if snap.Render.Engines[0].RenderedPerSec != 0 {
		t.Errorf("chromium темп = %v, want 0", snap.Render.Engines[0].RenderedPerSec)
	}
	if snap.Render.Fallback.PublishedPerSec != 0.2 {
		t.Errorf("published темп = %v, want 0.2 (2 за 10 с)", snap.Render.Fallback.PublishedPerSec)
	}
	if snap.Render.Fallback.ConsumedPerSec != 0.2 {
		t.Errorf("consumed темп = %v, want 0.2 (2 доработки за 10 с)", snap.Render.Fallback.ConsumedPerSec)
	}
	if snap.Render.Fallback.ConsumedTotal != 10 {
		t.Errorf("consumed = %d, want 10 (7+1+2)", snap.Render.Fallback.ConsumedTotal)
	}

	// Рестарт воркера A (кумулятив упал) — нулевая дельта, не отрицательная;
	// B без движения — темп потребления тоже 0.
	setA(renderWorkerExpo{engine: "moli", rendered: 5})
	setClock(time.Date(2026, 11, 6, 12, 0, 20, 0, time.UTC))
	snap = coll.CollectOnce(ctx)
	if got := snap.Render.Engines[1].RenderedPerSec; got != 0 {
		t.Errorf("темп после рестарта = %v, want 0 (сброс базы)", got)
	}
	if pct := snap.Render.Engines[1].SuccessPct; pct == nil || *pct != 100 {
		t.Errorf("успешность после рестарта = %v, want 100 (5/5)", pct)
	}
	if got := snap.Render.Fallback.ConsumedPerSec; got != 0 {
		t.Errorf("consumed темп без движения = %v, want 0", got)
	}
}

// TestCollectRenderDegradation — адреса не заданы — секция «н/д» без
// ошибки (стиль health); недоступная экспозиция — секция деградирована
// с причиной, остальные секции живы; битая экспозиция — ошибка разбора.
func TestCollectRenderDegradation(t *testing.T) {
	// Не настроено — не ошибка.
	coll, _ := newRenderCollector(t, &renderLens{}, nil)
	snap := coll.CollectOnce(context.Background())
	if snap.Render != nil {
		t.Error("render без адресов не nil, want «н/д»")
	}
	if _, ok := snap.Errors["render"]; ok {
		t.Errorf("адреса не заданы — ошибка секции: %v", snap.Errors)
	}

	// Экспозиция недоступна (закрытый сервер) — деградация секции,
	// очереди/зоны живы (§5.9).
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	coll, _ = newRenderCollector(t, &renderLens{}, []string{deadURL})
	snap = coll.CollectOnce(context.Background())
	if snap.Render != nil {
		t.Error("render при недоступной экспозиции не nil")
	}
	if snap.Errors["render"] == "" {
		t.Errorf("errors без причины render: %v", snap.Errors)
	}
	if len(snap.Errors) != 1 || snap.Zones == nil || snap.Queues == nil {
		t.Errorf("деградация задела чужие секции: %v", snap.Errors)
	}

	// Битая экспозиция (метки не закрыты) — ошибка разбора с причиной.
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("m{x=\"1\" 5\n"))
	}))
	defer broken.Close()
	coll, _ = newRenderCollector(t, &renderLens{}, []string{broken.URL})
	snap = coll.CollectOnce(context.Background())
	if snap.Render != nil || snap.Errors["render"] == "" {
		t.Errorf("битая экспозиция: render=%v errors=%v, want деградация", snap.Render, snap.Errors)
	}

	// Глубина недоступна (Redis лежит) — Depth nil, секция жива: панель
	// без остатка очереди лучше отсутствия панели.
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(renderWorkerExpo{engine: "chromium", rendered: 10}.exposition()))
	}))
	defer alive.Close()
	coll, _ = newRenderCollector(t, &renderLens{err: errors.New("redis: timeout")}, []string{alive.URL})
	snap = coll.CollectOnce(context.Background())
	if snap.Render == nil || snap.Render.Fallback.Depth != nil {
		t.Errorf("render=%v depth=%v, want секция жива с Depth nil", snap.Render, snap.Render.Fallback.Depth)
	}
	if len(snap.Errors) != 1 || snap.Errors["queues"] == "" {
		t.Errorf("отказ Redis не отражён секцией очередей: %v", snap.Errors)
	}
}

// TestCollectRenderNotConfiguredByDefault — сборщик без опций (конфиг
// по умолчанию: адресов нет) — render всегда nil, /api/status отдаёт
// null; JSON-поле присутствует (контракт фронта).
func TestCollectRenderNotConfiguredByDefault(t *testing.T) {
	coll := newSnapCollector(t, healthyPool(), &fakeLenner{}, nil)
	snap := coll.CollectOnce(context.Background())
	if snap.Render != nil {
		t.Fatal("render при дефолтном конфиге не nil")
	}
}
